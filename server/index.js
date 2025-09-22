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
    
    console.log(`[${new Date().toISOString()}] 🚀 App initialization${groupId ? ` for group ${groupId}` : ''} by user ${req.user.username || req.user.id}`);
    const startTime = Date.now();
    
    const [walletCounts, transactions, groups] = await Promise.all([
      db.getWalletCount(groupId),
      db.getRecentTransactionsOptimized(hours, 4000, transactionType, groupId),
      db.getGroups()
    ]);
    
    const grpcStatus = solanaGrpcService.getStatus();
    const performanceStats = solanaGrpcService.getPerformanceStats();
    
    const duration = Date.now() - startTime;
    console.log(`[${new Date().toISOString()}] ⚡ Initialization completed in ${duration}ms - ${transactions.length} transactions, ${walletCounts.totalWallets} wallets`);
    
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
          activeWallets: performanceStats.totalMonitoredWallets,
          activeGroupId: grpcStatus.activeGroupId,
          mode: 'full_stream_optimized',
          performance: {
            messagesReceived: performanceStats.messagesReceived,
            messagesFiltered: performanceStats.messagesFiltered,
            filterEfficiency: performanceStats.filterEfficiency,
            avgFilterTime: performanceStats.avgFilterTimeMs,
            isHealthy: performanceStats.isHealthy
          }
        },
        groups,
        performance: {
          loadTime: duration,
          optimizationLevel: 'FULL_STREAM_OPTIMIZED_V3',
          cacheHits: {
            solPrice: performanceStats.solPriceCache.ageMs < 60000,
            processedTransactions: performanceStats.caches.processedTransactions,
            walletMetadata: performanceStats.caches.walletMetadata
          },
          streamingMode: 'full_solana_with_client_filtering'
        }
      }
    });
    
  } catch (error) {
    console.error(`[${new Date().toISOString()}] ❌ Error in optimized initialization:`, error);
    res.status(500).json({ 
      error: 'Failed to initialize application data',
      details: error.message,
      optimization: 'FULL_STREAM_OPTIMIZED_V3'
    });
  }
});

app.get('/api/health', (req, res) => {
  const grpcStatus = solanaGrpcService.getStatus();
  const performanceStats = solanaGrpcService.getPerformanceStats();
  
  res.json({ 
    status: 'ok', 
    message: 'Optimized backend running with Full Stream gRPC',
    timestamp: new Date().toISOString(),
    grpc: {
      connected: grpcStatus.isConnected,
      started: grpcStatus.isStarted,
      activeGroup: grpcStatus.activeGroupId,
      monitoredWallets: grpcStatus.totalSubscriptions,
      messageCount: grpcStatus.messageCount,
      filteredCount: grpcStatus.filteredCount,
      reconnectAttempts: grpcStatus.reconnectAttempts,
      mode: grpcStatus.mode
    },
    performance: {
      streamType: 'full_solana_stream',
      totalReceived: performanceStats.messagesReceived,
      totalFiltered: performanceStats.messagesFiltered,
      totalProcessed: performanceStats.messagesProcessed,
      filterEfficiency: `${performanceStats.filterEfficiency}%`,
      avgFilterTime: `${performanceStats.avgFilterTimeMs.toFixed(3)}ms`,
      caches: {
        processedTransactions: performanceStats.caches.processedTransactions,
        recentlyProcessed: performanceStats.caches.recentlyProcessed,
        walletMetadata: performanceStats.caches.walletMetadata,
        walletToGroup: performanceStats.caches.walletToGroup,
        solPrice: {
          cached: performanceStats.solPriceCache.lastUpdated > 0,
          price: performanceStats.solPriceCache.price,
          ageMs: performanceStats.solPriceCache.ageMs
        }
      },
      isHealthy: performanceStats.isHealthy
    },
    optimization: 'FULL_STREAM_WITH_CLIENT_FILTERING_V3'
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
      level: 'FULL_STREAM_OPTIMIZED_V3',
      features: [
        'Full Solana transaction stream',
        'Client-side wallet filtering',
        'O(1) wallet lookup with Set/Map',
        'Batched transaction processing',
        'Smart cache management',
        'Real-time filter efficiency monitoring',
        'Automatic cache cleanup',
        'Single connection resilience'
      ],
      advantages: [
        'Scales to millions of wallets',
        'No node subscription limits',
        'Better reliability (1 connection vs many)',
        'Real-time performance monitoring',
        'Efficient memory usage'
      ],
      metrics: {
        efficiency: `${performanceStats.filterEfficiency}% of transactions filtered out`,
        avgFilterTime: `${performanceStats.avgFilterTimeMs.toFixed(3)}ms per transaction`,
        monitoredWallets: performanceStats.totalMonitoredWallets.toLocaleString(),
        streamMode: 'Full Solana blockchain streaming'
      }
    }
  });
});

app.post('/api/cache/clear', auth.authRequired, auth.adminRequired, (req, res) => {
  try {
    const { force = false } = req.body;
    
    let result;
    if (force) {
      result = solanaGrpcService.forceCleanupCaches();
    } else {
      result = solanaGrpcService.clearCaches();
    }
    
    res.json({
      success: true,
      message: force ? 'Force cache cleanup completed' : 'Manual cache cleanup completed',
      result,
      timestamp: new Date().toISOString(),
      cacheType: 'full_stream_optimized'
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

app.get('/api/filter-stats', auth.authRequired, (req, res) => {
  const stats = solanaGrpcService.getPerformanceStats();
  const status = solanaGrpcService.getStatus();
  
  res.json({
    timestamp: new Date().toISOString(),
    filteringPerformance: {
      efficiency: `${stats.filterEfficiency}%`,
      avgFilterTime: `${stats.avgFilterTimeMs.toFixed(3)}ms`,
      totalReceived: stats.messagesReceived,
      totalFiltered: stats.messagesFiltered,
      totalProcessed: stats.messagesProcessed,
      monitoredWallets: stats.totalMonitoredWallets
    },
    streamHealth: {
      connected: status.isConnected,
      started: status.isStarted,
      reconnectAttempts: status.reconnectAttempts,
      activeGroup: status.activeGroupId,
      streamMode: status.mode
    },
    recommendations: stats.filterEfficiency < 95 ? [
      'Filter efficiency below 95% - consider optimizing wallet data structures',
      'Check if too many irrelevant transactions are being processed'
    ] : [
      'Filter performance is optimal'
    ]
  });
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
    console.log(`[${new Date().toISOString()}] 🔄 Stopping full stream gRPC service...`);
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

console.log(`[${new Date().toISOString()}] 🚀 Starting wallet monitoring server with Full Stream optimization...`);

setTimeout(() => {
  startGrpcService(solanaGrpcService)();
}, 3000);

startSessionCleaner(auth);

https.createServer(sslOptions, app).listen(port, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] 🚀 Full Stream wallet monitoring server running on https://0.0.0.0:${port}`);
  console.log(`[${new Date().toISOString()}] 📊 Ready to handle unlimited wallets with optimized filtering`);
});