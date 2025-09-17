module.exports = (auth, db, solanaGrpcService) => {
  const express = require('express');
  const router = express.Router();

  router.get('/', auth.authRequired, async (req, res) => {
    try {
      const groupId = req.query.groupId || null;
      const includeStats = req.query.includeStats === 'true';
      const limit = parseInt(req.query.limit) || 50;
      const offset = parseInt(req.query.offset) || 0;
      
      console.log(`[${new Date().toISOString()}] 📋 Global wallets request${groupId ? ` for group ${groupId}` : ''}, stats: ${includeStats}, limit: ${limit}`);
      
      if (includeStats) {
        const wallets = await db.getActiveWallets(groupId);
        const walletsWithStats = await Promise.all(
          wallets.slice(offset, offset + limit).map(async (wallet) => {
            const stats = await db.getWalletStats(wallet.id);
            return {
              ...wallet,
              stats: {
                totalBuyTransactions: stats.total_buy_transactions || 0,
                totalSellTransactions: stats.total_sell_transactions || 0,
                totalTransactions: (stats.total_buy_transactions || 0) + (stats.total_sell_transactions || 0),
                totalSpentSOL: Number(stats.total_sol_spent || 0).toFixed(6),
                totalReceivedSOL: Number(stats.total_sol_received || 0).toFixed(6),
                netSOL: (Number(stats.total_sol_received || 0) - Number(stats.total_sol_spent || 0)).toFixed(6),
                lastTransactionAt: stats.last_transaction_at,
              },
            };
          })
        );
        res.json(walletsWithStats);
      } else {
        let query = `
          SELECT w.id, w.address, w.name, w.group_id, w.created_at,
                 g.name as group_name, u.username as added_by_username,
                 COUNT(*) OVER() as total_count
          FROM wallets w
          LEFT JOIN groups g ON w.group_id = g.id
          LEFT JOIN users u ON w.added_by = u.id
          WHERE w.is_active = TRUE
        `;
        const params = [];
        
        if (groupId) {
          query += ` AND w.group_id = $1`;
          params.push(groupId);
          query += ` ORDER BY w.created_at DESC LIMIT $2 OFFSET $3`;
          params.push(limit, offset);
        } else {
          query += ` ORDER BY w.created_at DESC LIMIT $1 OFFSET $2`;
          params.push(limit, offset);
        }
        
        const result = await db.pool.query(query, params);
        
        const wallets = result.rows.map(row => ({
          id: row.id,
          address: row.address,
          name: row.name,
          group_id: row.group_id,
          group_name: row.group_name,
          added_by_username: row.added_by_username,
          created_at: row.created_at,
          stats: {
            totalTransactions: 0,
            totalSpentSOL: "0.000000",
            totalReceivedSOL: "0.000000", 
            netSOL: "0.000000"
          }
        }));
        
        res.json({
          wallets,
          totalCount: result.rows.length > 0 ? parseInt(result.rows[0].total_count) : 0,
          hasMore: result.rows.length === limit,
          limit,
          offset
        });
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error fetching wallets:`, error);
      res.status(500).json({ error: 'Failed to fetch wallets' });
    }
  });

  router.delete('/', auth.authRequired, async (req, res) => {
    try {
      const groupId = req.query.groupId || null;
      
      console.log(`[${new Date().toISOString()}] 🗑️ Starting complete wallet removal${groupId ? ` for group ${groupId}` : ' (ALL GROUPS)'} by user ${req.user.username}`);
      
      const startTime = Date.now();
      
      const walletsToRemove = await db.getActiveWallets(groupId);
      const addressesToRemove = walletsToRemove.map(w => w.address);
      
      console.log(`[${new Date().toISOString()}] 📊 Found ${walletsToRemove.length} wallets to remove${groupId ? ` from group ${groupId}` : ''}`);
      
      if (addressesToRemove.length > 0) {
        try {
          await solanaGrpcService.removeAllWallets(groupId);
          console.log(`[${new Date().toISOString()}] ✅ Successfully removed wallets from gRPC monitoring`);
        } catch (grpcError) {
          console.warn(`[${new Date().toISOString()}] ⚠️ gRPC removal warning: ${grpcError.message}`);
        }
      }
      
      console.log(`[${new Date().toISOString()}] 🗄️ Starting database deletion...`);
      const deletionResult = await db.removeAllWallets(groupId);
      
      const newCounts = await db.getWalletCount(groupId);
      
      const duration = Date.now() - startTime;
      
      const responseData = {
        success: true,
        message: `Successfully removed ${deletionResult.deletedCount} wallets and all associated data${groupId ? ` from group ${groupId}` : ' from all groups'}`,
        details: {
          walletsRemoved: deletionResult.deletedCount,
          groupId: groupId,
          grpcRemoved: addressesToRemove.length,
          cascadeDeleted: {
            transactions: deletionResult.details?.transactions || 0,
            tokenOperations: deletionResult.details?.tokenOperations || 0,
            walletStats: deletionResult.details?.walletStats || 0
          },
          processingTime: duration,
          removedAddresses: addressesToRemove.slice(0, 5),
          totalRemoved: addressesToRemove.length
        },
        newCounts: {
          totalWallets: newCounts.totalWallets,
          groups: newCounts.groups,
          selectedGroup: newCounts.selectedGroup
        }
      };
      
      console.log(`[${new Date().toISOString()}] 🎉 Complete wallet removal completed in ${duration}ms:`, {
        deleted: deletionResult.deletedCount,
        grpcRemoved: addressesToRemove.length,
        newTotal: newCounts.totalWallets,
        groupAffected: groupId || 'ALL_GROUPS'
      });
      
      res.json(responseData);
      
    } catch (error) {
      const duration = Date.now() - (Date.now() - 1000); 
      console.error(`[${new Date().toISOString()}] ❌ Complete wallet removal failed after ${duration}ms:`, error);
      
      res.status(500).json({ 
        success: false,
        error: 'Failed to remove wallets completely',
        message: error.message,
        details: {
          stage: 'unknown',
          groupId: req.query.groupId || null,
          timestamp: new Date().toISOString()
        }
      });
    }
  });

  router.post('/bulk-optimized', auth.authRequired, async (req, res) => {
    const startTime = Date.now();
    
    try {
      const { wallets, groupId } = req.body;
      const addedBy = req.user.id;

      console.log(`[${new Date().toISOString()}] 🚀 Bulk import: ${wallets?.length || 0} wallets by user ${req.user.username || req.user.id}`);

      if (!wallets || !Array.isArray(wallets) || wallets.length === 0) {
        return res.status(400).json({ 
          success: false,
          error: 'Non-empty wallets array is required' 
        });
      }

      if (wallets.length > 1000) {
        return res.status(400).json({ 
          success: false,
          error: 'Maximum 1,000 wallets allowed per batch' 
        });
      }

      if (groupId) {
        const groupQuery = `SELECT id FROM groups WHERE id = $1`;
        const groupResult = await db.pool.query(groupQuery, [groupId]);
        if (groupResult.rows.length === 0) {
          return res.status(400).json({ success: false, error: 'Invalid group ID' });
        }
      }

      const results = {
        total: wallets.length,
        successful: 0,
        failed: 0,
        skipped: 0,
        errors: [],
        successfulWallets: [],
        newCounts: null
      };

      const validationStart = Date.now();

      const validWallets = [];
      const solanaAddressRegex = /^[1-9A-HJ-NP-Za-km-z]+$/;
      const seenAddresses = new Set();

      for (const wallet of wallets) {
        if (!wallet || !wallet.address) {
          results.failed++;
          results.errors.push({
            address: 'unknown',
            name: wallet?.name || null,
            error: 'Missing wallet address'
          });
          continue;
        }

        const address = wallet.address.trim();

        if (address.length < 32 || address.length > 44 || !solanaAddressRegex.test(address)) {
          results.failed++;
          results.errors.push({
            address: address,
            name: wallet.name || null,
            error: 'Invalid Solana address format'
          });
          continue;
        }

        if (seenAddresses.has(address)) {
          results.failed++;
          results.errors.push({
            address: address,
            name: wallet.name || null,
            error: 'Duplicate address in current batch'
          });
          continue;
        }

        seenAddresses.add(address);
        validWallets.push({
          address: address,
          name: wallet.name?.trim() || null,
          addedBy,
          groupId: groupId || null
        });
      }

      const validationTime = Date.now() - validationStart;
      console.log(`[${new Date().toISOString()}] ⚡ Global validation completed in ${validationTime}ms: ${validWallets.length}/${wallets.length} valid`);

      if (validWallets.length === 0) {
        return res.json({
          success: false,
          message: 'No valid wallets to import after validation',
          results,
          duration: Date.now() - startTime
        });
      }

      const dbStart = Date.now();

      try {
        const insertedWallets = await db.addWalletsBatchOptimized(validWallets);
        
        const dbTime = Date.now() - dbStart;

        results.successful = insertedWallets.length;
        results.failed += (validWallets.length - insertedWallets.length);
        results.successfulWallets = insertedWallets.map(wallet => ({
          address: wallet.address,
          name: wallet.name,
          id: wallet.id,
          groupId: wallet.group_id,
          addedBy: wallet.added_by
        }));

        const newCounts = await db.getWalletCount(groupId);
        results.newCounts = newCounts;

      } catch (dbError) {
        console.error(`[${new Date().toISOString()}] ❌ Global DB error:`, dbError.message);
        throw new Error(`Database operation failed: ${dbError.message}`);
      }

      if (results.successful > 0) {
        console.log(`[${new Date().toISOString()}] 🔗 Starting global gRPC subscriptions...`);
        
        setImmediate(async () => {
          try {
            const addressesToSubscribe = results.successfulWallets.map(w => w.address);
            
            const relevantAddresses = results.successfulWallets
              .filter(wallet => !solanaGrpcService.activeGroupId || wallet.groupId === solanaGrpcService.activeGroupId)
              .map(w => w.address);

            if (relevantAddresses.length > 0 && solanaGrpcService.isStarted) {
              await solanaGrpcService.subscribeToWalletsBatch(relevantAddresses, 200);
              console.log(`[${new Date().toISOString()}] ✅ Global gRPC subscriptions completed: ${relevantAddresses.length} wallets`);
            } else {
              console.log(`[${new Date().toISOString()}] ⏭️ Skipping gRPC subscriptions: ${relevantAddresses.length} relevant, gRPC started: ${solanaGrpcService.isStarted}`);
            }
          } catch (grpcError) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Global gRPC subscription failed:`, grpcError.message);
          }
        });
      }

      const duration = Date.now() - startTime;
      const walletsPerSecond = Math.round((results.successful / duration) * 1000);

      console.log(`[${new Date().toISOString()}] 🎉 Bulk import completed in ${duration}ms: ${results.successful}/${results.total} successful (${walletsPerSecond} wallets/sec)`);

      res.json({
        success: results.successful > 0,
        message: `Global import: ${results.successful} successful, ${results.failed} failed out of ${results.total} total`,
        results,
        duration,
        performance: {
          walletsPerSecond,
          totalTime: duration,
          averageTimePerWallet: Math.round(duration / results.total),
          optimizationLevel: 'GLOBAL_GRPC'
        }
      });

    } catch (error) {
      const duration = Date.now() - startTime;
      console.error(`[${new Date().toISOString()}] ❌ Global bulk import failed after ${duration}ms:`, error);
      
      res.status(500).json({ 
        success: false,
        error: 'Internal server error during global bulk import',
        details: error.message,
        duration
      });
    }
  });

  router.post('/toggle', auth.authRequired, async (req, res) => {
    try {
      const { action, groupId } = req.body;

      if (action === 'start') {
        await solanaGrpcService.start(groupId);
        res.json({ success: true, message: `Global gRPC monitoring started${groupId ? ` for group ${groupId}` : ''}` });
      } else if (action === 'stop') {
        await solanaGrpcService.stop();
        res.json({ success: true, message: 'Global gRPC monitoring stopped' });
      } else {
        res.status(400).json({ error: 'Invalid action. Use "start" or "stop"' });
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error toggling global gRPC monitoring:`, error);
      res.status(500).json({ error: error.message });
    }
  });

  return router;
};