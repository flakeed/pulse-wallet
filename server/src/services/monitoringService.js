const { Connection, PublicKey } = require('@solana/web3.js');
const { fetchTokenMetadata } = require('./tokenService');
const Database = require('../database/connection');
const PriceService = require('./priceService');
const { redis } = require('./tokenService');

class WalletMonitoringService {
    constructor() {
        this.db = new Database();
        this.connection = new Connection(process.env.SOLANA_RPC_URL || '', {
            commitment: 'confirmed',
            httpHeaders: { 'Connection': 'keep-alive' }
        });
        this.priceService = new PriceService();
        this.isMonitoring = false;
        this.processedSignatures = new Set();
        this.recentlyProcessed = new Set();
        this.stats = {
            totalScans: 0,
            totalWallets: 0,
            totalBuyTransactions: 0,
            totalSellTransactions: 0,
            errors: 0,
            lastScanDuration: 0,
            startTime: Date.now(),
        };
        this.isProcessingQueue = false;
        this.queueKey = 'webhook:queue';
        this.batchSize = 400;
        this.solPriceCache = {
            price: 150,
            lastUpdated: 0,
            cacheTimeout: 60000 
        };
    }

    stopMonitoring() {
        this.isMonitoring = false;
        console.log('⏹️ Legacy monitoring stopped');
    }

    async processQueue() {
        if (this.isProcessingQueue) return;
        this.isProcessingQueue = true;
    
        while (true) {
            const requestData = await redis.lpop(this.queueKey, this.batchSize);
            if (!requestData || requestData.length === 0) break;
    
            const requests = requestData.map((data) => {
                try {
                    return JSON.parse(data);
                } catch (error) {
                    console.error(`[${new Date().toISOString()}] ❌ Invalid queue entry:`, error.message);
                    return null;
                }
            }).filter((req) => req !== null);
    
            if (requests.length === 0) continue;
    
            const batchResults = await Promise.all(
                requests.map(async (request) => {
                    const { signature, walletAddress, blockTime } = request;
                    try {
                        const wallet = await this.db.getWalletByAddress(walletAddress);
                        if (!wallet) {
                            console.warn(`[${new Date().toISOString()}] ⚠️ Wallet ${walletAddress} not found`);
                            return null;
                        }
    
                        const txData = await this.processTransaction({ signature, blockTime }, wallet);
                        if (txData) {
                            console.log(`[${new Date().toISOString()}] ✅ Processed transaction ${signature}`);
                            return {
                                signature,
                                walletAddress,
                                walletName: wallet.name,
                                groupId: wallet.group_id,
                                groupName: wallet.group_name,
                                transactionType: txData.type,
                                solAmount: txData.solAmount,
                                tokens: txData.tokensChanged.map((tc) => ({
                                    mint: tc.mint,
                                    amount: tc.rawChange / Math.pow(10, tc.decimals),
                                    symbol: tc.symbol,
                                    name: tc.name,
                                })),
                                timestamp: new Date(blockTime * 1000).toISOString(),
                            };
                        }
                        return null;
                    } catch (error) {
                        console.error(`[${new Date().toISOString()}] ❌ Error processing signature ${signature}:`, error.message);
                        return null;
                    }
                })
            );
    
            const successfulTxs = batchResults.filter((tx) => tx !== null);
            if (successfulTxs.length > 0) {
                const pipeline = redis.pipeline();
                successfulTxs.forEach((tx) => {
                    pipeline.publish('transactions', JSON.stringify(tx));
                });
                await pipeline.exec();
            }
        }
    
        this.isProcessingQueue = false;
        const queueLength = await redis.llen(this.queueKey);
        if (queueLength > 0) {
            setImmediate(() => this.processQueue());
        }
    }

    async processWebhookMessage(message) {
        const { signature, walletAddress, blockTime } = message;
        const requestId = require('uuid').v4();
        await redis.lpush(this.queueKey, JSON.stringify({
            requestId,
            signature,
            walletAddress,
            blockTime,
            timestamp: Date.now(),
        }));

        if (!this.isProcessingQueue) {
            setImmediate(() => this.processQueue());
        }
    }

    async fetchTransactionWithRetry(signature, maxRetries = 3) {
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const options = {
                    maxSupportedTransactionVersion: 0, 
                    commitment: 'confirmed',
                };
    
                const tx = await this.connection.getParsedTransaction(signature, options);
                
                if (!tx) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ Transaction ${signature} not found (attempt ${attempt})`);
                    if (attempt < maxRetries) {
                        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
                        continue;
                    }
                    return null;
                }
    
                if (tx.meta?.err) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ Transaction ${signature} failed:`, tx.meta.err);
                    return null;
                }
    
                return tx;
            } catch (error) {
                console.error(`[${new Date().toISOString()}] ❌ Error fetching transaction ${signature} (attempt ${attempt}):`, error.message);
                
                if (attempt < maxRetries) {
                    console.log(`[${new Date().toISOString()}] ⏳ Waiting before retry...`);
                    await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
                }
            }
        }
        
        console.error(`[${new Date().toISOString()}] ❌ Failed to fetch transaction ${signature} after ${maxRetries} attempts`);
        return null;
    }

    async fetchSolPrice() {
        const now = Date.now();
        
        if (now - this.solPriceCache.lastUpdated < this.solPriceCache.cacheTimeout) {
            return this.solPriceCache.price;
        }

        try {
            const priceData = await this.priceService.getSolPrice();
            const newPrice = priceData.price || 150;

            this.solPriceCache = {
                price: newPrice,
                lastUpdated: now,
                cacheTimeout: 60000
            };
            
            return newPrice;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error fetching SOL price:`, error.message);
            return this.solPriceCache.price; 
        }
    }

    async processTransaction(sig, wallet) {
        try {
            if (!sig.signature || !sig.blockTime) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Invalid signature object:`, sig);
                return null;
            }

            const existingTx = await this.db.pool.query(
                'SELECT id FROM transactions WHERE signature = $1 AND wallet_id = $2',
                [sig.signature, wallet.id]
            );
            if (existingTx.rows.length > 0) {
                return null;
            }

            const processedKey = `${sig.signature}-${wallet.id}`;
            if (this.recentlyProcessed.has(processedKey)) {
                return null;
            }
            this.recentlyProcessed.add(processedKey);

            if (this.recentlyProcessed.size > 1000) {
                const toDelete = Array.from(this.recentlyProcessed).slice(0, 500);
                toDelete.forEach(key => this.recentlyProcessed.delete(key));
            }

            const tx = await this.fetchTransactionWithRetry(sig.signature);
            if (!tx || !tx.meta || !tx.meta.preBalances || !tx.meta.postBalances) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Invalid transaction ${sig.signature} - missing metadata`);
                return null;
            }

            const walletPubkey = wallet.address;
            let walletIndex = -1;
            if (tx.transaction.message.accountKeys) {
                if (Array.isArray(tx.transaction.message.accountKeys)) {
                    walletIndex = tx.transaction.message.accountKeys.findIndex(
                        (key) => key.pubkey ? key.pubkey.toString() === walletPubkey : key.toString() === walletPubkey
                    );
                } else if (tx.transaction.message.staticAccountKeys) {
                    walletIndex = tx.transaction.message.staticAccountKeys.findIndex(
                        (key) => key.toString() === walletPubkey
                    );
                }
                if (walletIndex === -1 && tx.transaction.message.addressTableLookups) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ Versioned transaction with address table lookups not fully supported yet`);
                    return null;
                }
            }

            if (walletIndex === -1) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Wallet ${walletPubkey} not found in transaction ${sig.signature}`);
                return null;
            }

            const preBalance = tx.meta.preBalances[walletIndex] || 0;
            const postBalance = tx.meta.postBalances[walletIndex] || 0;
            const solChange = (postBalance - preBalance) / 1e9;

            const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
            let transactionType, totalSolAmount = 0, usdcAmount = 0;
            const FEE_THRESHOLD = 0.01;
            let tokenChanges = [];

            const solPrice = await this.fetchSolPrice();

            let usdcChange = 0;
            const usdcPreBalance = (tx.meta.preTokenBalances || []).find(b => b.mint === USDC_MINT && b.owner === walletPubkey);
            const usdcPostBalance = (tx.meta.postTokenBalances || []).find(b => b.mint === USDC_MINT && b.owner === walletPubkey);
            
            if (usdcPreBalance && usdcPostBalance) {
                usdcChange = (Number(usdcPostBalance.uiTokenAmount.amount) - Number(usdcPreBalance.uiTokenAmount.amount)) / 1e6;
            } else if (usdcPostBalance) {
                usdcChange = Number(usdcPostBalance.uiTokenAmount.uiAmount || 0);
            } else if (usdcPreBalance) {
                usdcChange = -Number(usdcPreBalance.uiTokenAmount.uiAmount || 0);
            }

            if (usdcChange !== 0) {
                usdcAmount = Math.abs(usdcChange);
                const usdcSolEquivalent = usdcAmount / solPrice;
                if (usdcChange < 0) {
                    transactionType = 'buy';
                    totalSolAmount = usdcSolEquivalent;
                } else if (usdcChange > 0) {
                    transactionType = 'sell';
                    totalSolAmount = usdcSolEquivalent;
                }
                tokenChanges = await this.analyzeTokenChanges(tx.meta, transactionType, walletPubkey);
            } else if (solChange < -FEE_THRESHOLD) {
                transactionType = 'buy';
                totalSolAmount = Math.abs(solChange);
                tokenChanges = await this.analyzeTokenChanges(tx.meta, transactionType, walletPubkey);
            } else if (solChange > 0.001) {
                transactionType = 'sell';
                totalSolAmount = solChange;
                tokenChanges = await this.analyzeTokenChanges(tx.meta, transactionType, walletPubkey);
            } else {
                console.log(`[${new Date().toISOString()}] ℹ️ Transaction ${sig.signature} - SOL change too small: ${solChange.toFixed(6)} (likely just fees)`);
                return null;
            }

            if (tokenChanges.length === 0) {
                console.log(`[${new Date().toISOString()}] ℹ️ Transaction ${sig.signature} - no token changes detected`);
                return null;
            }

            const mints = tokenChanges.map(tc => tc.mint);
            const tokenInfos = await this.priceService.getTokenPrices(mints);

            const enrichedTokenChanges = tokenChanges.map(tc => {
                const tokenInfo = tokenInfos.get(tc.mint) || {
                    price: 0,
                    marketCap: 0,
                    deploymentTime: null,
                    ageInHours: null,
                    symbol: tc.symbol,
                    name: tc.name,
                    decimals: tc.decimals,
                };
                return {
                    mint: tc.mint,
                    rawChange: tc.rawChange,
                    decimals: tc.decimals,
                    symbol: tc.symbol,
                    name: tc.name,
                    token_price_usd: tokenInfo.price,
                    usd_value: (tc.rawChange / Math.pow(10, tc.decimals)) * tokenInfo.price,
                    sol_amount: (tc.rawChange / Math.pow(10, tc.decimals)) * (tokenInfo.price / solPrice),
                    market_cap: tokenInfo.marketCap,
                    deployment_time: tokenInfo.deploymentTime,
                    ageInHours: tokenInfo.ageInHours,
                };
            });

            return await this.db.withTransaction(async (client) => {
                const finalCheck = await client.query(
                    'SELECT id FROM transactions WHERE signature = $1 AND wallet_id = $2',
                    [sig.signature, wallet.id]
                );
                if (finalCheck.rows.length > 0) {
                    return null;
                }

                const query = `
                    INSERT INTO transactions (
                        wallet_id, signature, block_time, transaction_type,
                        sol_spent, sol_received, usd_spent, usd_received
                    ) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id, signature, transaction_type
                `;
                const result = await client.query(query, [
                    wallet.id,
                    sig.signature,
                    new Date(sig.blockTime * 1000).toISOString(),
                    transactionType,
                    transactionType === 'buy' ? totalSolAmount : 0,
                    transactionType === 'sell' ? totalSolAmount : 0,
                    transactionType === 'buy' && usdcAmount ? usdcAmount : 0,
                    transactionType === 'sell' && usdcAmount ? usdcAmount : 0,
                ]);

                if (result.rows.length === 0) {
                    return null;
                }

                const transaction = result.rows[0];
                const tokenSavePromises = enrichedTokenChanges.map((tokenChange) =>
                    this.saveTokenOperationInTransaction(client, transaction.id, tokenChange, transactionType)
                );
                await Promise.all(tokenSavePromises);

                return {
                    signature: sig.signature,
                    type: transactionType,
                    solAmount: totalSolAmount,
                    usdcAmount,
                    tokensChanged: enrichedTokenChanges,
                };
            });
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing transaction ${sig.signature}:`, error.message);
            return null;
        }
    }

    async analyzeTokenChanges(meta, transactionType, walletAddress) {
        const WRAPPED_SOL_MINT = 'So11111111111111111111111111111111111111112';
        const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
        const tokenChanges = [];

        const allBalanceChanges = new Map();
        for (const pre of meta.preTokenBalances || []) {
            const key = `${pre.mint}-${pre.accountIndex}`;
            allBalanceChanges.set(key, {
                mint: pre.mint,
                accountIndex: pre.accountIndex,
                owner: pre.owner,
                preAmount: pre.uiTokenAmount.amount,
                preUiAmount: pre.uiTokenAmount.uiAmount,
                postAmount: '0',
                postUiAmount: 0,
                decimals: pre.uiTokenAmount.decimals
            });
        }

        for (const post of meta.postTokenBalances || []) {
            const key = `${post.mint}-${post.accountIndex}`;
            if (allBalanceChanges.has(key)) {
                const existing = allBalanceChanges.get(key);
                existing.postAmount = post.uiTokenAmount.amount;
                existing.postUiAmount = post.uiTokenAmount.uiAmount;
            } else {
                allBalanceChanges.set(key, {
                    mint: post.mint,
                    accountIndex: post.accountIndex,
                    owner: post.owner,
                    preAmount: '0',
                    preUiAmount: 0,
                    postAmount: post.uiTokenAmount.amount,
                    postUiAmount: post.uiTokenAmount.uiAmount,
                    decimals: post.uiTokenAmount.decimals
                });
            }
        }

        const mintChanges = new Map();
        for (const [key, change] of allBalanceChanges) {
            if (change.mint === WRAPPED_SOL_MINT || change.mint === USDC_MINT) {
                continue;
            }

            if (change.owner !== walletAddress) {
                continue;
            }

            const rawChange = Number(change.postAmount) - Number(change.preAmount);

            let isValidChange = false;
            if (transactionType === 'buy' && rawChange > 0) {
                isValidChange = true;
            } else if (transactionType === 'sell' && rawChange < 0) {
                isValidChange = true;
            } else {
                console.log(`[${new Date().toISOString()}] ⏭️ Skipping token ${change.mint} - balance change doesn't match transaction type`);
                continue;
            }

            if (isValidChange) {
                if (mintChanges.has(change.mint)) {
                    const existing = mintChanges.get(change.mint);
                    existing.totalRawChange += Math.abs(rawChange);
                } else {
                    mintChanges.set(change.mint, {
                        mint: change.mint,
                        decimals: change.decimals,
                        totalRawChange: Math.abs(rawChange)
                    });
                    console.log(`[${new Date().toISOString()}] 🆕 New mint change: ${change.mint} = ${Math.abs(rawChange)}`);
                }
            }
        }

        if (mintChanges.size === 0) {
            return [];
        }

        const mints = Array.from(mintChanges.keys());
        const tokenInfos = await this.batchFetchTokenMetadata(mints);

        for (const [mint, aggregatedChange] of mintChanges) {
            const tokenInfo = tokenInfos.get(mint) || {
                symbol: 'Unknown',
                name: 'Unknown Token',
                decimals: aggregatedChange.decimals,
            };

            tokenChanges.push({
                mint: mint,
                rawChange: aggregatedChange.totalRawChange,
                decimals: aggregatedChange.decimals,
                symbol: tokenInfo.symbol,
                name: tokenInfo.name,
            });

        }

        return tokenChanges;
    }

    async batchFetchTokenMetadata(mints) {
        const tokenInfos = new Map();
        const uncachedMints = [];
        const pipeline = redis.pipeline();

        for (const mint of mints) {
            pipeline.get(`token:${mint}`);
        }
        const results = await pipeline.exec();

        results.forEach(([err, cachedToken], index) => {
            if (!err && cachedToken) {
                tokenInfos.set(mints[index], JSON.parse(cachedToken));
            } else {
                uncachedMints.push(mints[index]);
            }
        });

        if (uncachedMints.length > 0) {
            const batchSize = 10;
            for (let i = 0; i < uncachedMints.length; i += batchSize) {
                const batch = uncachedMints.slice(i, i + batchSize);
                const batchResults = await Promise.all(
                    batch.map(async (mint) => {
                        const tokenInfo = await fetchTokenMetadata(mint, this.connection);
                        return { mint, tokenInfo };
                    })
                );
                const pipeline = redis.pipeline();
                batchResults.forEach(({ mint, tokenInfo }) => {
                    if (tokenInfo) {
                        tokenInfos.set(mint, tokenInfo);
                        pipeline.set(`token:${mint}`, JSON.stringify(tokenInfo), 'EX', 24 * 60 * 60);
                    }
                });
                await pipeline.exec();
            }
        }

        return tokenInfos;
    }

    async saveTokenOperationInTransaction(client, transactionId, tokenChange, transactionType) {
        try {
            const tokenInfo = await fetchTokenMetadata(tokenChange.mint, this.connection);
            if (!tokenInfo) {
                console.warn(`[${new Date().toISOString()}] ⚠️ No metadata for token ${tokenChange.mint}`);
                return;
            }

            const tokenUpsertQuery = `
                INSERT INTO tokens (mint, symbol, name, decimals) 
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (mint) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    decimals = EXCLUDED.decimals,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            `;
            const tokenResult = await client.query(tokenUpsertQuery, [
                tokenChange.mint,
                tokenInfo.symbol,
                tokenInfo.name,
                tokenInfo.decimals,
            ]);

            const tokenId = tokenResult.rows[0].id;
            const amount = tokenChange.rawChange / Math.pow(10, tokenChange.decimals);

            const operationQuery = `
                INSERT INTO token_operations (transaction_id, token_id, amount, operation_type) 
                VALUES ($1, $2, $3, $4)
            `;
            await client.query(operationQuery, [transactionId, tokenId, amount, transactionType]);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error saving token operation:`, error.message);
            throw error;
        }
    }

    async removeAllWallets(groupId = null) {
        try {
            console.log(`[${new Date().toISOString()}] 🗑️ Removing all wallets from monitoring service${groupId ? ` for group ${groupId}` : ''}`);
            
            let query = `SELECT signature FROM transactions t JOIN wallets w ON t.wallet_id = w.id WHERE 1=1`;
            const params = [];
            let paramIndex = 1;
            
            if (groupId) {
                query += ` AND w.group_id = ${paramIndex}`;
                params.push(groupId);
            }
            
            const transactions = await this.db.pool.query(query, params);
            const allSignatures = transactions.rows.map((tx) => tx.signature);
            allSignatures.forEach((sig) => this.processedSignatures.delete(sig));
            
            if (!groupId) {
                this.processedSignatures.clear();
                this.recentlyProcessed.clear();
            }
            
            const result = await this.db.removeAllWallets(groupId);
            
            console.log(`[${new Date().toISOString()}] ✅ All wallets removed from monitoring service${groupId ? ` for group ${groupId}` : ''} (${result.deletedCount} wallets)`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error removing all wallets from monitoring service:`, error.message);
            throw error;
        }
    }

    async close() {
        this.stopMonitoring();
        if (this.priceService) {
            await this.priceService.close();
        }
        await redis.quit();
        await this.db.close();
        console.log(`[${new Date().toISOString()}] ✅ Monitoring service closed`);
    }
}

module.exports = WalletMonitoringService;