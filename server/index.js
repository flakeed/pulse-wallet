const express = require('express');
const cors = require('cors');
const https = require('https');
const fs = require('fs');
require('dotenv').config();

const Database = require('./src/database/connection');
const SolanaGrpcService = require('./src/services/solanaGrpcService');
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

const { startGrpcService } = require('./utils/grpcStarter');
const { startSessionCleaner } = require('./utils/sessionCleaner');

const app = express();
const port = process.env.PORT || 5001;

const solanaGrpcService = new SolanaGrpcService();
const db = new Database();
const auth = new AuthMiddleware(db);
const priceService = new PriceService();
const sseClients = new Set();

const sslOptions = {
  key: fs.readFileSync('/etc/letsencrypt/live/degenlogs.com/privkey.pem'),
  cert: fs.readFileSync('/etc/letsencrypt/live/degenlogs.com/fullchain.pem'),
};

app.use(express.json({ 
  limit: '100mb', 
  verify: (req, res, buf) => {
    req.rawBody = buf;
  }
}));

app.use(express.urlencoded({ 
  limit: '100mb', 
  extended: true,
  parameterLimit: 100000 
}));

app.use(cors({
  origin: [
    'http://localhost:3000',
    'http://localhost:3001',
    'https://degenlogs.com',
    'http://degenlogs.com',
    'https://degenlogs.com:3000',
    'http://degenlogs.com:3000',
  ],
  optionsSuccessStatus: 200,
}));

app.use((req, res, next) => {
  req.setTimeout(600000);
  res.setTimeout(600000);
  next();
});

app.get('/api/init', auth.authRequired, async (req, res) => {
  try {
    const groupId = req.query.groupId || null;
    const hours = parseInt(req.query.hours) || 24;
    const transactionType = req.query.type;
    
    console.log(`[${new Date().toISOString()}] 🚀 Optimized app initialization${groupId ? ` for group ${groupId}` : ''} by user ${req.user.username || req.user.id}`);
    const startTime = Date.now();
    
    const [walletCounts, transactions, groups] = await Promise.all([
      db.getWalletCount(groupId),
      db.getRecentTransactionsOptimized(hours, 4000, transactionType, groupId),
      db.getGroups()
    ]);
    
    const grpcStatus = solanaGrpcService.getStatus();
    const performanceStats = solanaGrpcService.getPerformanceStats();
    
    const duration = Date.now() - startTime;
    console.log(`[${new Date().toISOString()}] ⚡ Optimized initialization completed in ${duration}ms - ${transactions.length} transactions, ${walletCounts.totalWallets} wallets`);
    
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
          isMonitoring: grpcStatus.isConnected && grpcStatus.isStarted,
          processedSignatures: grpcStatus.messageCount,
          activeWallets: performanceStats.monitoredWallets,
          activeGroupId: grpcStatus.activeGroupId,
          mode: 'optimized_grpc',
          performance: {
            messagesProcessed: performanceStats.messagesProcessed,
            cacheStats: performanceStats.solPriceCache,
            batchSize: performanceStats.currentBatchSize,
            isHealthy: performanceStats.isHealthy
          }
        },
        groups,
        performance: {
          loadTime: duration,
          optimizationLevel: 'OPTIMIZED_GRPC_V2',
          cacheHits: {
            solPrice: performanceStats.solPriceCache.ageMs < 60000,
            processedTransactions: performanceStats.processedTransactionsCache,
            recentlyProcessed: performanceStats.recentlyProcessedCache
          }
        }
      }
    });
    
  } catch (error) {
    console.error(`[${new Date().toISOString()}] ❌ Error in optimized initialization:`, error);
    res.status(500).json({ 
      error: 'Failed to initialize application data',
      details: error.message,
      optimization: 'OPTIMIZED_GRPC_V2'
    });
  }
});

app.get('/api/health', (req, res) => {
  const grpcStatus = solanaGrpcService.getStatus();
  const performanceStats = solanaGrpcService.getPerformanceStats();
  
  res.json({ 
    status: 'ok', 
    message: 'Optimized backend running with gRPC v2',
    timestamp: new Date().toISOString(),
    grpc: {
      connected: grpcStatus.isConnected,
      started: grpcStatus.isStarted,
      activeGroup: grpcStatus.activeGroupId,
      monitoredWallets: grpcStatus.subscriptions,
      messageCount: grpcStatus.messageCount,
      reconnectAttempts: grpcStatus.reconnectAttempts,
      mode: grpcStatus.mode
    },
    performance: {
      messagesProcessed: performanceStats.messagesProcessed,
      caches: {
        processedTransactions: performanceStats.processedTransactionsCache,
        recentlyProcessed: performanceStats.recentlyProcessedCache,
        solPrice: {
          cached: performanceStats.solPriceCache.lastUpdated > 0,
          price: performanceStats.solPriceCache.price,
          ageMs: performanceStats.solPriceCache.ageMs
        }
      },
      batch: {
        currentSize: performanceStats.currentBatchSize,
        isHealthy: performanceStats.isHealthy
      }
    },
    optimization: 'GRPC_V2_WITH_CACHING'
  });
});

app.get('/api/performance', auth.authRequired, auth.adminRequired, (req, res) => {
  const performanceStats = solanaGrpcService.getPerformanceStats();
  const grpcStatus = solanaGrpcService.getStatus();
  
  res.json({
    timestamp: new Date().toISOString(),
    grpc: grpcStatus,
    performance: performanceStats,
    system: {
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      pid: process.pid,
      version: process.version
    },
    optimization: {
      level: 'OPTIMIZED_GRPC_V2',
      features: [
        'Redis caching',
        'Transaction batching',
        'Memory optimization',
        'Duplicate prevention',
        'Automatic cache cleanup'
      ]
    }
  });
});

app.post('/api/cache/clear', auth.authRequired, auth.adminRequired, (req, res) => {
  try {
    solanaGrpcService.clearCaches();
    res.json({
      success: true,
      message: 'Caches cleared successfully',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error(`[${new Date().toISOString()}] ❌ Error clearing caches:`, error);
    res.status(500).json({
      success: false,
      error: 'Failed to clear caches',
      details: error.message
    });
  }
});

app.use('/api/auth', authRoutes(auth, db));
app.use('/api/admin', adminRoutes(auth, db));
app.use('/api/wallets', walletRoutes(auth, db, solanaGrpcService));
app.use('/api/transactions', transactionRoutes(auth, db, redis, sseClients));
app.use('/api', miscRoutes(auth, db, priceService, solanaGrpcService));
app.use('/api/groups', groupRoutes(auth, db, solanaGrpcService));

app.use(errorHandler);

const gracefulShutdown = async (signal) => {
  console.log(`[${new Date().toISOString()}] 🛑 Received ${signal}, shutting down gracefully...`);
  
  try {
    console.log(`[${new Date().toISOString()}] 🔄 Stopping optimized gRPC service...`);
    await solanaGrpcService.shutdown();
    
    console.log(`[${new Date().toISOString()}] 🔄 Stopping other services...`);
    await Promise.all([
      priceService.close(),
      redis.quit()
    ]);
    
    console.log(`[${new Date().toISOString()}] 🔄 Closing SSE connections...`);
    sseClients.forEach((client) => {
      try {
        client.end();
      } catch (error) {
        console.warn(`[${new Date().toISOString()}] ⚠️ Error closing SSE client:`, error.message);
      }
    });
    sseClients.clear();
    
    console.log(`[${new Date().toISOString()}] ✅ Graceful shutdown completed`);
    process.exit(0);
    
  } catch (error) {
    console.error(`[${new Date().toISOString()}] ❌ Error during shutdown:`, error);
    process.exit(1);
  }
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

process.on('unhandledRejection', (reason, promise) => {
  console.error(`[${new Date().toISOString()}] ❌ Unhandled rejection at:`, promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
  console.error(`[${new Date().toISOString()}] ❌ Uncaught exception:`, error);
  gracefulShutdown('UNCAUGHT_EXCEPTION');
});

console.log(`[${new Date().toISOString()}] 🚀 Starting optimized wallet monitoring server...`);

setTimeout(() => {
  startGrpcService(solanaGrpcService)();
}, 3000);

startSessionCleaner(auth);

https.createServer(sslOptions, app).listen(port, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] 🚀 Optimized global wallet monitoring server running on https://0.0.0.0:${port}`);
  console.log(`[${new Date().toISOString()}] 🔧 Optimizations enabled: Redis caching, transaction batching, memory management`);
  console.log(`[${new Date().toISOString()}] 📊 Ready to handle 500k+ wallets with gRPC v2`);
});