module.exports = (auth, db, priceService, solanaGrpcService) => { 
  const express = require('express');
  const router = express.Router();

  router.get('/solana/price', auth.authRequired, async (req, res) => {
    try {
      
      const priceData = await Promise.race([
        priceService.getSolPrice(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('SOL price timeout')), 10000))
      ]);
      
      if (!priceData || !priceData.success) {
        console.warn(`[${new Date().toISOString()}] ⚠️ Invalid SOL price data:`, priceData);
        return res.json({
          success: true,
          price: 150,
          source: 'fallback',
          error: 'Could not fetch current SOL price, using fallback'
        });
      }
      
      res.json(priceData);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error in SOL price endpoint:`, error.message);
      res.json({ 
        success: true, 
        price: 150,
        source: 'fallback_error',
        error: 'SOL price service unavailable'
      });
    }
  });

  router.post('/tokens/batch-data', auth.authRequired, async (req, res) => {
    try {
      const { mints } = req.body;

      if (!mints || !Array.isArray(mints)) {
        console.log(`[${new Date().toISOString()}] ❌ Missing mints array in request body`);
        return res.status(400).json({ success: false, error: 'Missing mints array in request body', details: 'Request must include a "mints" array' });
      }

      if (mints.length === 0) {
        console.log(`[${new Date().toISOString()}] ⚠️ Empty mints array`);
        return res.json({ success: true, data: {}, meta: { totalRequested: 0, successfulResponses: 0, failedResponses: 0, processingTime: 0, dataSource: 'empty_request' } });
      }

      const MAX_BATCH_SIZE = process.env.MAX_TOKEN_BATCH_SIZE || 50;
      if (mints.length > MAX_BATCH_SIZE) {
        console.log(`[${new Date().toISOString()}] ❌ Batch too large: ${mints.length} > ${MAX_BATCH_SIZE}`);
        return res.status(400).json({ success: false, error: `Too many mints requested. Maximum ${MAX_BATCH_SIZE} allowed, got ${mints.length}`, limit: MAX_BATCH_SIZE, received: mints.length, suggestion: `Split your request into smaller batches of ${MAX_BATCH_SIZE} or fewer` });
      }

      const validMints = [];
      const invalidMints = [];
      const solanaAddressRegex = /^[1-9A-HJ-NP-Za-km-z]+$/;
      mints.forEach(mint => {
        if (!mint || typeof mint !== 'string') invalidMints.push({ mint, reason: 'not a string' });
        else if (mint.length < 32 || mint.length > 44) invalidMints.push({ mint, reason: 'invalid length' });
        else if (!solanaAddressRegex.test(mint)) invalidMints.push({ mint, reason: 'invalid characters' });
        else validMints.push(mint);
      });

      if (invalidMints.length > 0) {
        console.log(`[${new Date().toISOString()}] ❌ Found ${invalidMints.length} invalid mint addresses`);
        return res.status(400).json({ success: false, error: `Invalid mint addresses found: ${invalidMints.length} invalid out of ${mints.length}`, invalidMints: invalidMints.slice(0, 5), validCount: validMints.length });
      }

      const uniqueMints = [...new Set(validMints)];
      if (uniqueMints.length !== validMints.length) {
        console.log(`[${new Date().toISOString()}] ℹ️ Removed ${validMints.length - uniqueMints.length} duplicate mints`);
      }

      const startTime = Date.now();

      const result = {};
      const processedCount = { successful: 0, failed: 0, cached: 0 };
      const SUB_BATCH_SIZE = 10;

      for (let i = 0; i < uniqueMints.length; i += SUB_BATCH_SIZE) {
        const subBatch = uniqueMints.slice(i, i + SUB_BATCH_SIZE);
        const batchNum = Math.floor(i / SUB_BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(uniqueMints.length / SUB_BATCH_SIZE);

        const subBatchPromises = subBatch.map(async (mint) => {
          try {
            const tokenData = await Promise.race([
              priceService.getTokenData(mint),
              new Promise((_, reject) => setTimeout(() => reject(new Error(`Timeout for token ${mint.slice(0, 8)}...`)), 10000))
            ]);

            const cached = tokenData.lastUpdated && (Date.now() - new Date(tokenData.lastUpdated).getTime()) < priceService.CACHE_TTL * 1000;
            if (cached) processedCount.cached++;

            if (tokenData && typeof tokenData.price === 'number') {
              processedCount.successful++;
              return {
                mint,
                data: {
                  price: tokenData.price || 0,
                  priceInSol: tokenData.priceInSol || 0,
                  marketCap: tokenData.marketCap || 0,
                  liquidity: tokenData.liquidity || 0,
                  volume24h: tokenData.volume24h || 0,
                  pools: tokenData.pools || 0,
                  bestPool: tokenData.bestPool || null,
                  token: {
                    symbol: tokenData.symbol || 'UNK',
                    name: tokenData.name || 'Unknown Token',
                    supply: tokenData.supply || 0,
                    decimals: tokenData.decimals || 6
                  },
                  age: {
                    createdAt: tokenData.age?.createdAt || null,
                    ageInHours: tokenData.age?.ageInHours || null,
                    isNew: (tokenData.age?.ageInHours || 999) < 24
                  },
                  lastUpdated: tokenData.lastUpdated || new Date().toISOString(),
                  source: tokenData.source || 'price_service',
                  cached
                }
              };
            } else {
              console.warn(`[${new Date().toISOString()}] ⚠️ No valid data for ${mint.slice(0, 8)}...`);
              processedCount.failed++;
              return { mint, data: null };
            }
          } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing ${mint.slice(0, 8)}...:`, error.message);
            processedCount.failed++;
            return { mint, data: null };
          }
        });

        try {
          const subBatchResults = await Promise.all(subBatchPromises);
          subBatchResults.forEach(({ mint, data }) => {
            result[mint] = data;
          });
          if (i + SUB_BATCH_SIZE < uniqueMints.length) {
            await new Promise(resolve => setTimeout(resolve, 200));
          }
        } catch (error) {
          console.error(`[${new Date().toISOString()}] ❌ Sub-batch ${batchNum} failed:`, error.message);
          subBatch.forEach(mint => {
            result[mint] = null;
            processedCount.failed++;
          });
        }
      }

      const duration = Date.now() - startTime;
      const successRate = uniqueMints.length > 0 ? ((processedCount.successful / uniqueMints.length) * 100).toFixed(1) : '0';

      const warnings = [];
      if (duration > 15000) warnings.push('Processing took longer than expected');
      if (processedCount.failed > processedCount.successful) warnings.push('More tokens failed than succeeded - check external API status');

      res.json({
        success: true,
        data: result,
        meta: {
          totalRequested: mints.length,
          uniqueRequested: uniqueMints.length,
          successfulResponses: processedCount.successful,
          failedResponses: processedCount.failed,
          cachedResponses: processedCount.cached,
          successRate: `${successRate}%`,
          processingTime: duration,
          subBatches: Math.ceil(uniqueMints.length / SUB_BATCH_SIZE),
          warnings: warnings.length > 0 ? warnings : undefined,
          dataSource: 'enhanced_price_service_v2'
        }
      });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Fatal error in batch token data endpoint:`, error);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error during batch token data processing',
        details: process.env.NODE_ENV === 'development' ? error.message : 'Server error',
        timestamp: new Date().toISOString()
      });
    }
  });

  router.get('/health', (req, res) => {
    const grpcStatus = solanaGrpcService.getStatus(); 
    res.json({ 
      status: 'ok', 
      message: 'Backend is running with gRPC',
      grpc: {
        connected: grpcStatus.isConnected,
        activeGroup: grpcStatus.activeGroupId,
        monitoredWallets: grpcStatus.subscriptions,
        messageCount: grpcStatus.messageCount
      }
    });
  });

  router.post('/monitoring/toggle', auth.authRequired, async (req, res) => {
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
      console.error(`[${new Date().toISOString()}] ❌ Error toggling global monitoring:`, error);
      res.status(500).json({ error: error.message });
    }
  });

router.post('/tokens/deployment-time', auth.authRequired, async (req, res) => {
  try {
    const { mints } = req.body;

    if (!mints || !Array.isArray(mints)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Missing mints array in request body' 
      });
    }

    if (mints.length === 0) {
      return res.json({ 
        success: true, 
        data: {},
        meta: { 
          totalRequested: 0, 
          processingTime: 0,
          dataSource: 'deployment_time_service'
        } 
      });
    }

    const MAX_BATCH_SIZE = 50;
    if (mints.length > MAX_BATCH_SIZE) {
      return res.status(400).json({ 
        success: false, 
        error: `Too many mints requested. Maximum ${MAX_BATCH_SIZE} allowed, got ${mints.length}` 
      });
    }

    const validMints = [];
    const invalidMints = [];
    const solanaAddressRegex = /^[1-9A-HJ-NP-Za-km-z]+$/;
    
    mints.forEach(mint => {
      if (!mint || typeof mint !== 'string') {
        invalidMints.push({ mint, reason: 'not a string' });
      } else if (mint.length < 32 || mint.length > 44) {
        invalidMints.push({ mint, reason: 'invalid length' });
      } else if (!solanaAddressRegex.test(mint)) {
        invalidMints.push({ mint, reason: 'invalid characters' });
      } else {
        validMints.push(mint);
      }
    });

    if (invalidMints.length > 0) {
      return res.status(400).json({ 
        success: false, 
        error: `Invalid mint addresses found: ${invalidMints.length} invalid out of ${mints.length}`,
        invalidMints: invalidMints.slice(0, 5)
      });
    }

    const uniqueMints = [...new Set(validMints)];
    const startTime = Date.now();

    const result = {};
    const uncachedMints = [];

    try {
      const dbQuery = `
        SELECT mint, deployment_time, symbol, name, decimals 
        FROM tokens 
        WHERE mint = ANY($1) AND deployment_time IS NOT NULL
      `;
      const dbResult = await db.pool.query(dbQuery, [uniqueMints]);
      
      const dbTokens = new Map();
      dbResult.rows.forEach(row => {
        dbTokens.set(row.mint, {
          deploymentTime: row.deployment_time,
          symbol: row.symbol,
          name: row.name,
          decimals: row.decimals
        });
      });

      uniqueMints.forEach(mint => {
        if (dbTokens.has(mint)) {
          result[mint] = dbTokens.get(mint);
        } else {
          uncachedMints.push(mint);
        }
      });

    } catch (dbError) {
      console.error(`[${new Date().toISOString()}] ❌ DB query error:`, dbError.message);
      uncachedMints.push(...uniqueMints);
    }

    if (uncachedMints.length > 0) {
      console.log(`[${new Date().toISOString()}] 🔍 Fetching deployment time for ${uncachedMints.length} tokens`);
      
      const connection = solanaGrpcService.connection || new Connection(
        process.env.SOLANA_RPC_URL || '',
        { commitment: 'confirmed' }
      );

      const BATCH_SIZE = 10;
      for (let i = 0; i < uncachedMints.length; i += BATCH_SIZE) {
        const batch = uncachedMints.slice(i, i + BATCH_SIZE);
        
        const batchPromises = batch.map(async (mint) => {
          try {
            const mintPubkey = new PublicKey(mint);
            
            const signatures = await connection.getSignaturesForAddress(
              mintPubkey, 
              { limit: 100 },
              'confirmed'
            );

            let deploymentTime = null;
            let symbol = mint.slice(0, 4).toUpperCase();
            let name = `Token ${mint.slice(0, 8)}...`;
            let decimals = 6;

            if (signatures.length > 0) {
              const firstSignature = signatures[signatures.length - 1];
              
              if (firstSignature.blockTime) {
                deploymentTime = new Date(firstSignature.blockTime * 1000).toISOString();
              }
            }

            try {
              const mintAccount = await connection.getParsedAccountInfo(mintPubkey);
              if (mintAccount.value && mintAccount.value.data.parsed) {
                decimals = mintAccount.value.data.parsed.info.decimals || 6;
              }
            } catch (mintError) {
            }

            return {
              mint,
              data: deploymentTime ? {
                deploymentTime,
                symbol,
                name,
                decimals
              } : null
            };

          } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error getting deployment time for ${mint}:`, error.message);
            return { mint, data: null };
          }
        });

        const batchResults = await Promise.allSettled(batchPromises);
        
        const tokensToCache = [];
        batchResults.forEach((promiseResult, index) => {
          if (promiseResult.status === 'fulfilled') {
            const { mint, data } = promiseResult.value;
            result[mint] = data;
            
            if (data && data.deploymentTime) {
              tokensToCache.push({
                mint,
                symbol: data.symbol,
                name: data.name,
                decimals: data.decimals,
                deploymentTime: data.deploymentTime
              });
            }
          } else {
            const mint = batch[index];
            result[mint] = null;
          }
        });

        if (tokensToCache.length > 0) {
          try {
            const values = [];
            const placeholders = [];
            
            tokensToCache.forEach((token, index) => {
              const offset = index * 5;
              placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}::timestamp)`);
              values.push(
                token.mint,
                token.symbol,
                token.name,
                token.decimals,
                token.deploymentTime
              );
            });

            const insertQuery = `
              INSERT INTO tokens (mint, symbol, name, decimals, deployment_time)
              VALUES ${placeholders.join(', ')}
              ON CONFLICT (mint) DO UPDATE SET
                deployment_time = COALESCE(tokens.deployment_time, EXCLUDED.deployment_time),
                symbol = COALESCE(NULLIF(tokens.symbol, ''), EXCLUDED.symbol),
                name = COALESCE(NULLIF(tokens.name, ''), EXCLUDED.name),
                decimals = COALESCE(tokens.decimals, EXCLUDED.decimals),
                updated_at = CURRENT_TIMESTAMP
            `;
            
            await db.pool.query(insertQuery, values);
            console.log(`[${new Date().toISOString()}] ✅ Cached ${tokensToCache.length} token deployment times`);
            
          } catch (cacheError) {
            console.error(`[${new Date().toISOString()}] ⚠️ Failed to cache deployment times:`, cacheError.message);
          }
        }

        if (i + BATCH_SIZE < uncachedMints.length) {
          await new Promise(resolve => setTimeout(resolve, 200));
        }
      }
    }

    const duration = Date.now() - startTime;
    const successfulResponses = Object.values(result).filter(r => r !== null).length;
    const failedResponses = uniqueMints.length - successfulResponses;

    console.log(`[${new Date().toISOString()}] ✅ Deployment time fetch completed in ${duration}ms: ${successfulResponses}/${uniqueMints.length} successful`);

    res.json({
      success: true,
      data: result,
      meta: {
        totalRequested: mints.length,
        uniqueRequested: uniqueMints.length,
        successfulResponses,
        failedResponses,
        processingTime: duration,
        fromDatabase: uniqueMints.length - uncachedMints.length,
        fromRpc: uncachedMints.length,
        dataSource: 'deployment_time_service'
      }
    });

  } catch (error) {
    console.error(`[${new Date().toISOString()}] ❌ Fatal error in deployment time endpoint:`, error);
    res.status(500).json({ 
      success: false, 
      error: 'Internal server error during deployment time processing',
      details: process.env.NODE_ENV === 'development' ? error.message : 'Server error',
      timestamp: new Date().toISOString()
    });
  }
});

  return router;
};