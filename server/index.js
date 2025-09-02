const express = require('express');
const cors = require('cors');
const https = require('https');
const fs = require('fs');
require('dotenv').config();
const WalletMonitoringService = require('./src/services/monitoringService');
const Database = require('./src/database/connection');
const SolanaWebSocketService = require('./src/services/solanaWebSocketService');
const AuthMiddleware = require('./middleware/authMiddleware');
const PriceService = require('./src/services/priceService');
const { redis } = require('./src/services/tokenService');
const authRoutes = require('./routes/authRoutes');
const adminRoutes = require('./routes/adminRoutes');
const walletRoutes = require('./routes/walletsRoutes');
const transactionRoutes = require('./routes/transactionsRoutes');
const miscRoutes = require('./routes/miscRoutes');
const groupRoutes = require('./routes/groupsRoutes');
const errorHandler = require('./middleware/errorHandler');
const { startWebSocketService } = require('./utils/websocketStarter');
const { startSessionCleaner } = require('./utils/sessionCleaner');

const app = express();
const port = process.env.PORT || 5001;

const monitoringService = new WalletMonitoringService();
const solanaWebSocketService = new SolanaWebSocketService();
const db = new Database();
const auth = new AuthMiddleware(db);
const priceService = new PriceService();
const sseClients = new Set();

const sslOptions = {
  key: fs.readFileSync('/etc/letsencrypt/live/wallet-monitor.ddns.net/privkey.pem'),
  cert: fs.readFileSync('/etc/letsencrypt/live/wallet-monitor.ddns.net/fullchain.pem'),
};

app.use(express.json({ 
  limit: '50mb',
  verify: (req, res, buf) => {
    req.rawBody = buf;
  }
}));
app.use(express.urlencoded({ 
  limit: '50mb', 
  extended: true,
  parameterLimit: 50000
}));
app.use(cors({
  origin: [
    'http://localhost:3000',
    'http://localhost:3001',
    'https://wallet-monitor.ddns.net',
    'http://wallet-monitor.ddns.net',
    'https://wallet-monitor.ddns.net:3000',
    'http://wallet-monitor.ddns.net:3000'  
  ],
  optionsSuccessStatus: 200,
}));
app.use((req, res, next) => {
  req.setTimeout(300000); 
  res.setTimeout(300000);
  next();
});

app.get('/api/init', auth.authRequired, async (req, res) => {
  try {
    const groupId = req.query.groupId || null;
    const hours = parseInt(req.query.hours) || 24;
    const transactionType = req.query.type;
    
    console.log(`[${new Date().toISOString()}] 🚀 App initialization${groupId ? ` for group ${groupId}` : ''}`);
    const startTime = Date.now();
    
    const [walletCounts, transactions, monitoringStatus, groups] = await Promise.all([
      db.getWalletCount(groupId),
      db.getRecentTransactionsOptimized(hours, 400, transactionType, groupId),
      db.getMonitoringStatus(groupId),
      db.getGroups()
    ]);
    
    const websocketStatus = solanaWebSocketService.getStatus();
    
    const duration = Date.now() - startTime;
    console.log(`[${new Date().toISOString()}] ⚡ Global initialization completed in ${duration}ms`);
    
    res.json({
      success: true,
      duration,
      data: {
        wallets: {
          totalCount: walletCounts.totalWallets,
          groups: walletCounts.groups,
          selectedGroup: walletCounts.selectedGroup
        },
        transactions,
        monitoring: {
          isMonitoring: websocketStatus.isConnected,
          processedSignatures: websocketStatus.messageCount,
          activeWallets: parseInt(monitoringStatus.active_wallets) || 0,
          activeGroupId: websocketStatus.activeGroupId,
          todayStats: {
            buyTransactions: parseInt(monitoringStatus.buy_transactions_today) || 0,
            sellTransactions: parseInt(monitoringStatus.sell_transactions_today) || 0,
            solSpent: Number(monitoringStatus.sol_spent_today || 0).toFixed(6),
            solReceived: Number(monitoringStatus.sol_received_today || 0).toFixed(6),
            uniqueTokens: parseInt(monitoringStatus.unique_tokens_today) || 0
          }
        },
        groups,
        performance: {
          loadTime: duration,
          optimizationLevel: 'GLOBAL'
        }
      }
    });
    
  } catch (error) {
    console.error(`[${new Date().toISOString()}] ❌ Error in global initialization:`, error);
    res.status(500).json({ error: 'Failed to initialize application data' });
  }
});

app.use('/api/auth', authRoutes(auth, db));
app.use('/api/admin', adminRoutes(auth, db));
app.use('/api/wallets', walletRoutes(auth, db, solanaWebSocketService));
app.use('/api/transactions', transactionRoutes(auth, db, redis, sseClients));
app.use('/api', miscRoutes(auth, db, priceService));
app.use('/api/groups', groupRoutes(auth, db, solanaWebSocketService));

app.use(errorHandler);

process.on('SIGINT', async () => {
  console.log(`[${new Date().toISOString()}] 🛑 Shutting down server...`);
  await monitoringService.close();
  await solanaWebSocketService.shutdown();
  await priceService.close();
  await redis.quit();
  sseClients.forEach((client) => client.end());
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log(`[${new Date().toISOString()}] 🛑 Shutting down server...`);
  await monitoringService.close();
  await solanaWebSocketService.shutdown(); 
  await redis.quit();
  sseClients.forEach((client) => client.end());
  process.exit(0);
});

setTimeout(startWebSocketService(solanaWebSocketService), 2000);
startSessionCleaner(auth);

https.createServer(sslOptions, app).listen(port, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] 🚀 Global wallet monitoring server running on https://0.0.0.0:${port}`);
});
