const { default: Client, CommitmentLevel } = require('@triton-one/yellowstone-grpc');
const Database = require('../database/connection');
const { PublicKey } = require('@solana/web3.js');
const bs58 = require('bs58');
const { redis } = require('./tokenService');
const { batchFetchTokenMetadata } = require('./tokenService');

class SolanaGrpcService {
    constructor() {
        this.grpcEndpoint = process.env.GRPC_ENDPOINT || 'http://45.134.108.254:10000';
        this.client = null;
        this.stream = null;
        this.db = new Database();
        this.isStarted = false;
        this.isConnecting = false;
        this.reconnectInterval = 3000;
        this.maxReconnectAttempts = 50;
        this.reconnectAttempts = 0;
        this.messageCount = 0;
        this.filteredCount = 0;
        this.activeGroupId = null;

        this.monitoredWallets = new Set();
        this.walletToGroup = new Map();
        this.walletMetadata = new Map();

        this.processedTransactions = new Set();
        this.recentlyProcessed = new Map(); 
        this.duplicateCache = new Set(); 

        this.solPriceCache = {
            price: 150,
            lastUpdated: 0,
            cacheTimeout: 30000 
        };
        this.priceUpdateInProgress = false;

        this.realtimeMode = process.env.REALTIME_MODE !== 'false'; 
        this.batchSize = this.realtimeMode ? 1 : 100; 
        this.batchTimeout = this.realtimeMode ? 0 : 25; 

        if (!this.realtimeMode) {
            this.transactionBatch = new Map();
            this.batchTimer = null;
        }

        this.BUY_THRESHOLD = 0; 
        this.SELL_THRESHOLD = 0; 
        this.MIN_TOKEN_CHANGE = parseFloat(process.env.MIN_TOKEN_CHANGE) || 0; 

        this.stats = {
            totalReceived: 0,
            totalFiltered: 0,
            totalProcessed: 0,
            totalSkipped: 0,
            filterEfficiency: 0,
            avgFilterTime: 0,
            lastStatsUpdate: Date.now(),
            walletHits: 0,
            duplicatesSkipped: 0
        };

        this.PROCESSED_CLEANUP_INTERVAL = 12 * 60 * 60 * 1000; 
        this.RECENTLY_PROCESSED_CLEANUP_INTERVAL = 30 * 60 * 1000; 
        this.DUPLICATE_CACHE_CLEANUP_INTERVAL = 10 * 60 * 1000; 
        this.lastProcessedCleanup = Date.now();
        this.lastRecentlyProcessedCleanup = Date.now();
        this.lastDuplicateCacheCleanup = Date.now();

        this.processingQueue = [];
        this.queueProcessing = false;
        this.maxQueueSize = 10000;

        this.setupOptimizedCacheCleanup();
        this.setupEnhancedStatsReporting();

        console.log(`[${new Date().toISOString()}] 🚀 OPTIMIZED Stream Service initialized for 300k+ wallets in ${this.realtimeMode ? 'REALTIME' : 'BATCH'} mode`);
        console.log(`[${new Date().toISOString()}] 💰 Enhanced SOL thresholds: buy>${this.BUY_THRESHOLD}, sell>${this.SELL_THRESHOLD}`);
        console.log(`[${new Date().toISOString()}] ⚡ Performance optimizations: aggressive filtering, enhanced caching, optimized processing`);
    }

    setupEnhancedStatsReporting() {
        setInterval(() => {
            const now = Date.now();
            const timeDiff = (now - this.stats.lastStatsUpdate) / 1000;

            if (this.stats.totalReceived > 0) {
                this.stats.filterEfficiency = ((this.stats.totalFiltered / this.stats.totalReceived) * 100).toFixed(2);
            }

            console.log(`[${new Date().toISOString()}] 📊 OPTIMIZED Performance Stats (${this.realtimeMode ? 'REALTIME' : 'BATCH'} mode):`);
            console.log(`  📥 Total received: ${this.stats.totalReceived} tx (${(this.stats.totalReceived / timeDiff).toFixed(1)} tx/s)`);
            console.log(`  🎯 Filtered out: ${this.stats.totalFiltered} (${this.stats.filterEfficiency}%)`);
            console.log(`  ✅ Processed: ${this.stats.totalProcessed}`);
            console.log(`  🚫 Skipped: ${this.stats.totalSkipped} (duplicates: ${this.stats.duplicatesSkipped})`);
            console.log(`  👥 Monitored wallets: ${this.monitoredWallets.size.toLocaleString()}`);
            console.log(`  🔍 Wallet hits: ${this.stats.walletHits}`);
            console.log(`  🔄 Active group: ${this.activeGroupId || 'all'}`);
            console.log(`  🧠 Cache sizes: proc=${this.processedTransactions.size}, recent=${this.recentlyProcessed.size}, dup=${this.duplicateCache.size}`);
            console.log(`  📦 Queue: ${this.processingQueue.length}/${this.maxQueueSize}`);

            this.stats.totalReceived = 0;
            this.stats.totalFiltered = 0;
            this.stats.totalProcessed = 0;
            this.stats.totalSkipped = 0;
            this.stats.walletHits = 0;
            this.stats.duplicatesSkipped = 0;
            this.stats.lastStatsUpdate = now;
        }, 15000); 
    }

    setupOptimizedCacheCleanup() {
        setInterval(() => {
            const now = Date.now();

            if (now - this.lastProcessedCleanup >= this.PROCESSED_CLEANUP_INTERVAL) {
                if (this.processedTransactions.size > 100000) {
                    const toDelete = Array.from(this.processedTransactions).slice(0, 50000);
                    toDelete.forEach(sig => this.processedTransactions.delete(sig));
                    console.log(`[${new Date().toISOString()}] 🧹 Processed cleanup: removed ${toDelete.length} signatures`);
                }
                this.lastProcessedCleanup = now;
            }

            if (now - this.lastRecentlyProcessedCleanup >= this.RECENTLY_PROCESSED_CLEANUP_INTERVAL) {
                let cleanupCount = 0;
                for (const [key, timestamp] of this.recentlyProcessed.entries()) {
                    if (now - timestamp > 3600000) { 
                        this.recentlyProcessed.delete(key);
                        cleanupCount++;
                    }
                }
                if (cleanupCount > 0) {
                    console.log(`[${new Date().toISOString()}] 🧹 Recent cleanup: removed ${cleanupCount} entries`);
                }
                this.lastRecentlyProcessedCleanup = now;
            }

            if (now - this.lastDuplicateCacheCleanup >= this.DUPLICATE_CACHE_CLEANUP_INTERVAL) {
                if (this.duplicateCache.size > 50000) {
                    const toDelete = Array.from(this.duplicateCache).slice(0, 25000);
                    toDelete.forEach(key => this.duplicateCache.delete(key));
                    console.log(`[${new Date().toISOString()}] 🧹 Duplicate cleanup: removed ${toDelete.length} entries`);
                }
                this.lastDuplicateCacheCleanup = now;
            }
        }, 120000); 
    }

    async loadMonitoredWallets(groupId = null) {
        const startTime = Date.now();
        console.log(`[${new Date().toISOString()}] 📋 Loading monitored wallets${groupId ? ` for group ${groupId}` : ' (all groups)'}`);

        try {
            const wallets = await this.db.getActiveWallets(groupId);

            this.monitoredWallets.clear();
            this.walletToGroup.clear();
            this.walletMetadata.clear();

            const batchSize = 10000;
            for (let i = 0; i < wallets.length; i += batchSize) {
                const batch = wallets.slice(i, i + batchSize);

                for (const wallet of batch) {
                    this.monitoredWallets.add(wallet.address);
                    this.walletToGroup.set(wallet.address, wallet.group_id);
                    this.walletMetadata.set(wallet.address, {
                        id: wallet.id,
                        name: wallet.name,
                        group_id: wallet.group_id,
                        group_name: wallet.group_name
                    });
                }

                if (i > 0 && i % 50000 === 0) {
                    await new Promise(resolve => setImmediate(resolve));
                    console.log(`[${new Date().toISOString()}] 📊 Loaded ${i} wallets...`);
                }
            }

            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] ✅ Loaded ${this.monitoredWallets.size.toLocaleString()} wallets in ${duration}ms (${Math.round(this.monitoredWallets.size / (duration / 1000))} wallets/sec)`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error loading wallets:`, error.message);
            throw error;
        }
    }

    async start(groupId = null) {
        if (this.isStarted && this.activeGroupId === groupId) {
            console.log(`[${new Date().toISOString()}] ℹ️ Stream service already running for group ${groupId || 'all'}`);
            return;
        }

        console.log(`[${new Date().toISOString()}] 🚀 Starting OPTIMIZED Solana stream for group ${groupId || 'all'}`);

        this.isStarted = true;
        this.activeGroupId = groupId;

        try {
            await this.loadMonitoredWallets(groupId);
            await this.createOptimizedFullStream();

            console.log(`[${new Date().toISOString()}] ✅ OPTIMIZED stream service started successfully`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to start optimized stream service:`, error.message);
            this.isStarted = false;
            throw error;
        }
    }

    async createOptimizedFullStream() {
        await this.endStream();

        try {
            console.log(`[${new Date().toISOString()}] 🔗 Connecting to OPTIMIZED Solana stream...`);

            this.client = new Client(this.grpcEndpoint, undefined, {
                'grpc.keepalive_time_ms': 15000, 
                'grpc.keepalive_timeout_ms': 3000,
                'grpc.keepalive_permit_without_calls': true,
                'grpc.http2.max_pings_without_data': 0,
                'grpc.http2.min_time_between_pings_ms': 5000,
                'grpc.http2.min_ping_interval_without_data_ms': 150000,
                'grpc.max_receive_message_length': 512 * 1024 * 1024, 
                'grpc.max_send_message_length': 512 * 1024 * 1024,
                'grpc.http2.max_concurrent_streams': 2000, 
                'grpc.keepalive_without_calls': true,
                'grpc.so_reuseaddr': 1,
                'grpc.tcp_user_timeout_ms': 30000
            });

            this.stream = await this.client.subscribe();

            this.stream.on('data', data => {
                this.messageCount++;
                this.stats.totalReceived++;
                this.handleOptimizedStreamMessage(data);
            });

            this.stream.on('error', error => {
                console.error(`[${new Date().toISOString()}] ❌ Optimized stream error:`, error.message);
                this.handleOptimizedReconnect();
            });

            this.stream.on('end', () => {
                console.log(`[${new Date().toISOString()}] 📡 Optimized stream ended`);
                if (this.isStarted) {
                    setTimeout(() => this.handleOptimizedReconnect(), 1000);
                }
            });

            const request = {
                accounts: {},
                slots: {},
                transactions: {
                    [""]: {
                        vote: false,
                        failed: false,
                        signature: undefined,
                        accountInclude: [],
                        accountExclude: [],
                        accountRequired: []
                    }
                },
                transactionsStatus: {},
                entry: {},
                blocks: {},
                blocksMeta: {},
                commitment: CommitmentLevel.CONFIRMED,
                accountsDataSlice: []
            };

            console.log(`[${new Date().toISOString()}] 📡 Subscribing to OPTIMIZED full Solana stream for ${this.monitoredWallets.size.toLocaleString()} wallets...`);

            await new Promise((resolve, reject) => {
                this.stream.write(request, err => {
                    if (err) {
                        console.error(`[${new Date().toISOString()}] ❌ Optimized stream subscription failed:`, err.message);
                        reject(err);
                    } else {
                        console.log(`[${new Date().toISOString()}] ✅ OPTIMIZED Solana stream active`);
                        resolve();
                    }
                });
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to create optimized stream:`, error.message);
            throw error;
        }
    }

    handleOptimizedStreamMessage(data) {
        try {
            if (!data.transaction) return;

            const signature = this.extractSignature(data.transaction);
            if (!signature) return;

            const duplicateKey = `${signature}`;
            if (this.duplicateCache.has(duplicateKey)) {
                this.stats.duplicatesSkipped++;
                this.stats.totalSkipped++;
                return;
            }

            if (this.processedTransactions.has(signature)) {
                this.stats.totalFiltered++;
                return;
            }

            const filterStart = process.hrtime.bigint();
            const walletMatch = this.optimizedWalletFilter(data.transaction);
            const filterTime = Number(process.hrtime.bigint() - filterStart) / 1000000;

            if (!walletMatch) {
                this.stats.totalFiltered++;
                this.stats.avgFilterTime = (this.stats.avgFilterTime + filterTime) / 2;
                return;
            }

            this.stats.walletHits++;
            this.duplicateCache.add(duplicateKey);

            if (this.realtimeMode) {
                this.processTransactionOptimized(data, walletMatch);
            } else {
                this.addToProcessingQueue(data, walletMatch);
            }

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error handling optimized stream message:`, error.message);
        }
    }

    optimizedWalletFilter(transactionData) {
        try {
            const accountKeys = this.extractAllAccountKeys(transactionData);

            for (const accountKey of accountKeys) {
                if (this.monitoredWallets.has(accountKey)) {
                    const walletGroup = this.walletToGroup.get(accountKey);

                    if (this.activeGroupId && walletGroup !== this.activeGroupId) {
                        continue;
                    }

                    return {
                        address: accountKey,
                        group_id: walletGroup,
                        metadata: this.walletMetadata.get(accountKey)
                    };
                }
            }

            return null;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in optimized wallet filter:`, error.message);
            return null;
        }
    }

    addToProcessingQueue(data, walletMatch) {
        if (this.processingQueue.length >= this.maxQueueSize) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Processing queue full, dropping transaction`);
            return;
        }

        this.processingQueue.push({ data, walletMatch });

        if (!this.queueProcessing) {
            setImmediate(() => this.processQueueBatch());
        }
    }

    async processQueueBatch() {
        if (this.queueProcessing || this.processingQueue.length === 0) return;

        this.queueProcessing = true;
        const batch = this.processingQueue.splice(0, this.batchSize);

        try {
            const promises = batch.map(({ data, walletMatch }) =>
                this.processTransactionOptimized(data, walletMatch).catch(error => {
                    console.error(`[${new Date().toISOString()}] ❌ Failed queue processing:`, error.message);
                    return null;
                })
            );

            const results = await Promise.allSettled(promises);
            const successful = results.filter(r => r.status === 'fulfilled' && r.value !== null).length;
            this.stats.totalProcessed += successful;

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Batch processing error:`, error.message);
        } finally {
            this.queueProcessing = false;

            if (this.processingQueue.length > 0) {
                setImmediate(() => this.processQueueBatch());
            }
        }
    }

    async processTransactionOptimized(data, walletMatch) {
        try {
            const signature = this.extractSignature(data.transaction);
            if (!signature) return null;

            const processedKey = `${signature}-${walletMatch.address}`;
            const now = Date.now();

            if (this.recentlyProcessed.has(processedKey)) {
                this.stats.totalSkipped++;
                return null;
            }

            this.recentlyProcessed.set(processedKey, now);
            this.processedTransactions.add(signature);

            const existingTx = await this.db.pool.query(
                'SELECT id FROM transactions WHERE signature = $1 AND wallet_id = $2 LIMIT 1',
                [signature, walletMatch.metadata?.id]
            );

            if (existingTx.rows.length > 0) {
                this.stats.totalSkipped++;
                return null;
            }

            const blockTime = Number(data.blockTime) || Math.floor(Date.now() / 1000);

            return await this.processTransactionFromOptimizedData({
                signature,
                transaction: data.transaction.transaction || data.transaction,
                meta: data.transaction.meta || data.meta,
                blockTime,
                wallet: {
                    address: walletMatch.address,
                    id: walletMatch.metadata?.id,
                    name: walletMatch.metadata?.name,
                    group_id: walletMatch.group_id,
                    group_name: walletMatch.metadata?.group_name
                }
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing optimized transaction:`, error.message);
            return null;
        }
    }

    async processTransactionFromOptimizedData({ signature, transaction, meta, blockTime, wallet }) {
        try {
            if (!transaction || !meta || meta.err) {
                return null;
            }

            const accountKeys = this.extractAllAccountKeys({ transaction, meta });
            const walletIndex = accountKeys.indexOf(wallet.address);

            if (walletIndex === -1) return null;

            const preBalance = meta.preBalances?.[walletIndex] || 0;
            const postBalance = meta.postBalances?.[walletIndex] || 0;
            const solChange = (postBalance - preBalance) / 1e9;

            const solPrice = await this.fetchSolPrice();

            const analysis = await this.enhancedTransactionAnalysis({
                meta,
                solChange,
                walletAddress: wallet.address,
                solPrice,
                signature
            });

            if (!analysis.transactionType || analysis.tokenChanges.length === 0 || analysis.totalSolAmount < 0.01) {
                this.stats.totalSkipped++;
                return null;
            }

            const transactionMessage = {
                signature,
                walletAddress: wallet.address,
                walletName: wallet.name,
                groupId: wallet.group_id,
                groupName: wallet.group_name,
                transactionType: analysis.transactionType,
                solAmount: analysis.totalSolAmount,
                tokens: analysis.tokenChanges.map(tc => ({
                    mint: tc.mint,
                    amount: tc.amount,
                    symbol: tc.symbol,
                    name: tc.name
                })),
                timestamp: new Date(blockTime * 1000).toISOString()
            };

            this.publishToSSE(transactionMessage, wallet.group_id);

            if (this.realtimeMode) {
                setImmediate(() => this.saveTransactionAsync({
                    wallet,
                    signature,
                    blockTime,
                    transactionType: analysis.transactionType,
                    totalSolAmount: analysis.totalSolAmount,
                    tokenChanges: analysis.tokenChanges,
                    solPrice
                }));
            } else {
                await this.saveTransactionSync({
                    wallet,
                    signature,
                    blockTime,
                    transactionType: analysis.transactionType,
                    totalSolAmount: analysis.totalSolAmount,
                    tokenChanges: analysis.tokenChanges,
                    solPrice
                });
            }

            console.log(`[${new Date().toISOString()}] ⚡ OPTIMIZED: ${signature.slice(0, 8)}... (${analysis.transactionType}) ${wallet.address.slice(0, 8)}...`);

            return {
                signature,
                type: analysis.transactionType,
                solAmount: analysis.totalSolAmount,
                tokensChanged: analysis.tokenChanges
            };

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing optimized transaction data:`, error.message);
            return null;
        }
    }

    async enhancedTransactionAnalysis({ meta, solChange, walletAddress, solPrice, signature }) {
        const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
        const USDT_MINT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';

        let transactionType = null;
        let totalSolAmount = 0;

        const stablecoins = [USDC_MINT, USDT_MINT];
        let stablecoinChange = 0;
        let stablecoinUsed = null;

        for (const stablecoin of stablecoins) {
            const preBalance = (meta.preTokenBalances || []).find(b => 
                b.mint === stablecoin && b.owner === walletAddress
            );
            const postBalance = (meta.postTokenBalances || []).find(b => 
                b.mint === stablecoin && b.owner === walletAddress
            );

            let change = 0;
            if (preBalance && postBalance) {
                const decimals = stablecoin === USDC_MINT ? 6 : 6; 
                change = (Number(postBalance.uiTokenAmount.amount) - Number(preBalance.uiTokenAmount.amount)) / Math.pow(10, decimals);
            } else if (postBalance) {
                change = Number(postBalance.uiTokenAmount.uiAmount || 0);
            } else if (preBalance) {
                change = -Number(preBalance.uiTokenAmount.uiAmount || 0);
            }

            if (Math.abs(change) > Math.abs(stablecoinChange)) {
                stablecoinChange = change;
                stablecoinUsed = stablecoin;
            }
        }

        if (Math.abs(stablecoinChange) > 0) { 
            if (stablecoinChange < 0) {
                transactionType = 'buy';
                totalSolAmount = Math.abs(stablecoinChange) / solPrice;
            } else {
                transactionType = 'sell';
                totalSolAmount = stablecoinChange / solPrice;
            }
        } else if (solChange < 0) { 
            transactionType = 'buy';
            totalSolAmount = Math.abs(solChange);
        } else if (solChange > 0) { 
            transactionType = 'sell';
            totalSolAmount = solChange;
        } else {

            const tokenChanges = await this.analyzeTokenChangesOptimized(meta, null, walletAddress);
            if (tokenChanges.length > 0) {

                const hasIncrease = tokenChanges.some(tc => tc.amount > 0);
                const hasDecrease = tokenChanges.some(tc => tc.amount < 0);

                if (hasIncrease && !hasDecrease) {
                    transactionType = 'buy';
                    totalSolAmount = Math.abs(solChange); 
                } else if (hasDecrease && !hasIncrease) {
                    transactionType = 'sell';
                    totalSolAmount = Math.abs(solChange); 
                } else if (hasIncrease && hasDecrease) {

                    transactionType = solChange < 0 ? 'buy' : 'sell';
                    totalSolAmount = Math.abs(solChange);
                }
            }
        }

        if (!transactionType) {
            return { transactionType: null, totalSolAmount: 0, tokenChanges: [] };
        }

        const tokenChanges = await this.analyzeTokenChangesOptimized(meta, transactionType, walletAddress);

        if (tokenChanges.length === 0) {
            console.log(`[${new Date().toISOString()}] ⚠️ ${signature}: No token changes detected despite ${transactionType} classification`);
            return { transactionType: null, totalSolAmount: 0, tokenChanges: [] };
        }

        return { transactionType, totalSolAmount, tokenChanges };
    }

    async analyzeTokenChangesOptimized(meta, transactionType, walletAddress) {
        const EXCLUDED_MINTS = new Set([
            'So11111111111111111111111111111111111111112', 
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'  
        ]);

        const tokenChanges = [];
        const allBalanceChanges = new Map();

        for (const pre of meta.preTokenBalances || []) {
            const key = `${pre.mint}-${pre.owner}`;
            allBalanceChanges.set(key, {
                mint: pre.mint,
                owner: pre.owner,
                preAmount: pre.uiTokenAmount.amount,
                postAmount: '0',
                decimals: pre.uiTokenAmount.decimals
            });
        }

        for (const post of meta.postTokenBalances || []) {
            const key = `${post.mint}-${post.owner}`;
            if (allBalanceChanges.has(key)) {
                const existing = allBalanceChanges.get(key);
                existing.postAmount = post.uiTokenAmount.amount;
            } else {
                allBalanceChanges.set(key, {
                    mint: post.mint,
                    owner: post.owner,
                    preAmount: '0',
                    postAmount: post.uiTokenAmount.amount,
                    decimals: post.uiTokenAmount.decimals
                });
            }
        }

        const mintChanges = new Map();
        for (const [key, change] of allBalanceChanges) {
            if (EXCLUDED_MINTS.has(change.mint) || change.owner !== walletAddress) {
                continue;
            }

            const rawChange = Number(change.postAmount) - Number(change.preAmount);
            if (rawChange === 0) continue; 

            let isValidChange = false;
            if (!transactionType) {

                isValidChange = Math.abs(rawChange) > 0;
            } else if (transactionType === 'buy' && rawChange > 0) {
                isValidChange = true;
            } else if (transactionType === 'sell' && rawChange < 0) {
                isValidChange = true;
            } else if (Math.abs(rawChange) > this.MIN_TOKEN_CHANGE) {

                isValidChange = true;
            }

            if (isValidChange) {
                if (mintChanges.has(change.mint)) {
                    const existing = mintChanges.get(change.mint);
                    existing.totalRawChange += rawChange; 
                } else {
                    mintChanges.set(change.mint, {
                        mint: change.mint,
                        decimals: change.decimals,
                        totalRawChange: rawChange
                    });
                }
            }
        }

        for (const [mint, aggregatedChange] of mintChanges) {
            if (aggregatedChange.totalRawChange === 0) continue; 

            const amount = aggregatedChange.totalRawChange / Math.pow(10, aggregatedChange.decimals);

            tokenChanges.push({
                mint: mint,
                amount: amount,
                rawChange: aggregatedChange.totalRawChange,
                decimals: aggregatedChange.decimals,
                symbol: mint.slice(0, 4).toUpperCase(),
                name: `Token ${mint.slice(0, 8)}...`
            });
        }

        return tokenChanges;
    }

    publishToSSE(transactionMessage, groupId) {
        try {
            const pipeline = redis.pipeline();
            pipeline.publish('transactions', JSON.stringify(transactionMessage));

            if (groupId) {
                pipeline.publish(`transactions:group:${groupId}`, JSON.stringify(transactionMessage));
            }

            pipeline.exec().catch(error => {
                console.error(`[${new Date().toISOString()}] ❌ Redis publish error:`, error.message);
            });
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ SSE publish error:`, error.message);
        }
    }

    async saveTransactionAsync(transactionData) {
        try {
            await this.saveTransactionToDatabase(transactionData);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Async DB save error:`, error.message);
        }
    }

    async saveTransactionSync(transactionData) {
        return await this.saveTransactionToDatabase(transactionData);
    }

    async saveTransactionToDatabase({ wallet, signature, blockTime, transactionType, totalSolAmount, tokenChanges, solPrice }) {
        try {
            return await this.db.withTransaction(async (client) => {

                const finalCheck = await client.query(
                    'SELECT id FROM transactions WHERE signature = $1 AND wallet_id = $2 LIMIT 1',
                    [signature, wallet.id]
                );
                if (finalCheck.rows.length > 0) {
                    return null;
                }

                const transactionQuery = `
                    INSERT INTO transactions (
                        wallet_id, signature, block_time, transaction_type,
                        sol_spent, sol_received, usd_spent, usd_received
                    ) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id, signature, transaction_type
                `;

                const transactionResult = await client.query(transactionQuery, [
                    wallet.id,
                    signature,
                    new Date(blockTime * 1000).toISOString(),
                    transactionType,
                    transactionType === 'buy' ? totalSolAmount : 0,
                    transactionType === 'sell' ? totalSolAmount : 0,
                    0, 
                    0
                ]);

                if (transactionResult.rows.length === 0) {
                    return null;
                }

                const transaction = transactionResult.rows[0];

                const tokenPromises = tokenChanges.map(tokenChange =>
                    this.saveTokenOperationOptimized(client, transaction.id, tokenChange, transactionType)
                );
                await Promise.all(tokenPromises);

                return {
                    signature: signature,
                    type: transactionType,
                    solAmount: totalSolAmount,
                    tokensChanged: tokenChanges
                };
            });
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error saving transaction to database:`, error.message);
            throw error;
        }
    }

    async saveTokenOperationOptimized(client, transactionId, tokenChange, transactionType) {
        try {

            const tokenUpsertQuery = `
                INSERT INTO tokens (mint, symbol, name, decimals) 
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (mint) DO UPDATE SET
                    symbol = CASE WHEN tokens.symbol = mint THEN EXCLUDED.symbol ELSE tokens.symbol END,
                    name = CASE WHEN tokens.name LIKE 'Token %...' THEN EXCLUDED.name ELSE tokens.name END,
                    decimals = EXCLUDED.decimals,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            `;

            const tokenResult = await client.query(tokenUpsertQuery, [
                tokenChange.mint,
                tokenChange.symbol,
                tokenChange.name,
                tokenChange.decimals
            ]);

            const tokenId = tokenResult.rows[0].id;

            const operationQuery = `
                INSERT INTO token_operations (transaction_id, token_id, amount, operation_type) 
                VALUES ($1, $2, $3, $4)
            `;

            await client.query(operationQuery, [
                transactionId,
                tokenId,
                Math.abs(tokenChange.amount), 
                tokenChange.amount > 0 ? 'buy' : 'sell' 
            ]);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error saving token operation:`, error.message);
            throw error;
        }
    }

    extractAllAccountKeys(transactionData) {
        const accountKeys = [];

        try {
            let transaction = null, meta = null;

            if (transactionData.transaction?.transaction) {
                transaction = transactionData.transaction.transaction;
                meta = transactionData.transaction.meta;
            } else if (transactionData.transaction && transactionData.meta) {
                transaction = transactionData.transaction;
                meta = transactionData.meta;
            } else {
                transaction = transactionData.transaction || transactionData;
                meta = transactionData.meta || transactionData;
            }

            if (!transaction) return accountKeys;

            let mainAccountKeys = transaction.message?.accountKeys || transaction.accountKeys || [];

            if (meta) {
                if (meta.loadedWritableAddresses) {
                    mainAccountKeys = mainAccountKeys.concat(meta.loadedWritableAddresses);
                }
                if (meta.loadedReadonlyAddresses) {
                    mainAccountKeys = mainAccountKeys.concat(meta.loadedReadonlyAddresses);
                }
            }

            for (const key of mainAccountKeys) {
                try {
                    let convertedKey;

                    if (key.type === 'Buffer' && Array.isArray(key.data)) {
                        convertedKey = new PublicKey(Buffer.from(key.data)).toString();
                    } else if (Buffer.isBuffer(key)) {
                        convertedKey = new PublicKey(key).toString();
                    } else if (typeof key === 'string') {
                        convertedKey = key;
                    } else if (key && typeof key === 'object') {
                        const pubkeyBuffer = key.pubkey?.type === 'Buffer' ? Buffer.from(key.pubkey.data) :
                            key.pubkey || key.key || key.address;
                        convertedKey = pubkeyBuffer ? new PublicKey(pubkeyBuffer).toString() : key.toString();
                    } else {
                        convertedKey = new PublicKey(Buffer.from(key)).toString();
                    }

                    if (convertedKey && convertedKey.length === 44) {
                        accountKeys.push(convertedKey);
                    }
                } catch (conversionError) {

                    continue;
                }
            }

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error extracting account keys:`, error.message);
        }

        return [...new Set(accountKeys)]; 
    }

    extractSignature(transactionData) {
        try {
            const sigObj = transactionData.signature ||
                (transactionData.signatures && transactionData.signatures[0]) ||
                transactionData.transaction?.signature ||
                (transactionData.transaction?.signatures && transactionData.transaction.signatures[0]) ||
                transactionData.tx?.signature ||
                (transactionData.tx?.signatures && transactionData.tx.signatures[0]);

            if (!sigObj) return null;

            let signature;
            if (sigObj.type === 'Buffer' && Array.isArray(sigObj.data)) {
                signature = bs58.encode(Buffer.from(sigObj.data));
            } else if (Buffer.isBuffer(sigObj)) {
                signature = bs58.encode(sigObj);
            } else if (typeof sigObj === 'string') {
                signature = sigObj;
            } else {
                signature = bs58.encode(Buffer.from(sigObj));
            }

            if (signature.length < 80 || signature.length > 88) {
                return null;
            }

            return signature;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error extracting signature:`, error.message);
            return null;
        }
    }

    async fetchSolPrice() {
        const now = Date.now();
        if (now - this.solPriceCache.lastUpdated < this.solPriceCache.cacheTimeout) {
            return this.solPriceCache.price;
        }

        if (!this.priceUpdateInProgress) {
            this.priceUpdateInProgress = true;
            this.updateSolPriceOptimized();
        }

        return this.solPriceCache.price;
    }

    async updateSolPriceOptimized() {
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000);

            const response = await fetch(
                'https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112',
                {
                    headers: { 'User-Agent': 'WalletPulse/4.0' },
                    signal: controller.signal
                }
            );

            clearTimeout(timeoutId);

            if (response.ok) {
                const data = await response.json();
                if (data.pairs && data.pairs.length > 0) {
                    const bestPair = data.pairs.reduce((prev, current) =>
                        (current.volume?.h24 || 0) > (prev.volume?.h24 || 0) ? current : prev
                    );

                    const newPrice = parseFloat(bestPair.priceUsd) || 150;

                    this.solPriceCache = {
                        price: newPrice,
                        lastUpdated: Date.now(),
                        cacheTimeout: this.realtimeMode ? 30000 : 60000
                    };

                    redis.setex('sol_price_optimized', 60, JSON.stringify({
                        price: newPrice,
                        timestamp: Date.now()
                    })).catch(() => {});
                }
            }
        } catch (error) {
            if (error.name !== 'AbortError') {
                console.warn(`[${new Date().toISOString()}] ⚠️ SOL price update failed:`, error.message);
            }
        } finally {
            this.priceUpdateInProgress = false;
        }
    }

    async endStream() {
        try {
            if (this.stream) {
                this.stream.end();
                this.stream = null;
            }
            if (this.client) {
                if (typeof this.client.close === 'function') this.client.close();
                else if (typeof this.client.destroy === 'function') this.client.destroy();
                this.client = null;
            }
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Error ending stream:`, error.message);
        }
    }

    async handleOptimizedReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] 🛑 Max reconnect attempts reached, stopping service`);
            this.isStarted = false;
            return;
        }

        this.reconnectAttempts++;
        console.log(`[${new Date().toISOString()}] 🔄 Reconnecting optimized stream (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

        await this.endStream();

        this.processingQueue.length = 0;
        this.queueProcessing = false;

        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));

        try {
            await this.createOptimizedFullStream();
            console.log(`[${new Date().toISOString()}] ✅ Optimized stream reconnection successful`);
            this.reconnectAttempts = 0;
            this.reconnectInterval = 3000; 
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Optimized reconnect failed:`, error.message);
            this.reconnectInterval = Math.min(this.reconnectInterval * 1.2, 15000); 
            await this.handleOptimizedReconnect();
        }
    }

    async switchGroup(groupId) {
        console.log(`[${new Date().toISOString()}] 🔄 Switching optimized stream to group ${groupId || 'all'}`);

        const oldSize = this.monitoredWallets.size;
        this.activeGroupId = groupId;

        await this.loadMonitoredWallets(groupId);

        this.recentlyProcessed.clear();
        this.duplicateCache.clear();

        console.log(`[${new Date().toISOString()}] ✅ Switched to group ${groupId || 'all'}: ${oldSize.toLocaleString()} → ${this.monitoredWallets.size.toLocaleString()} wallets`);

        return {
            success: true,
            activeGroupId: this.activeGroupId,
            monitoredWallets: this.monitoredWallets.size,
            previousSize: oldSize
        };
    }

    async subscribeToWalletsBatch(walletAddresses, batchSize = 50000) {
        console.log(`[${new Date().toISOString()}] ➕ Adding ${walletAddresses.length} wallets to optimized monitoring`);

        await this.loadMonitoredWallets(this.activeGroupId);

        console.log(`[${new Date().toISOString()}] ✅ Optimized monitoring updated: ${this.monitoredWallets.size.toLocaleString()} total wallets`);

        return { 
            successful: walletAddresses.length, 
            failed: 0, 
            errors: [], 
            totalMonitored: this.monitoredWallets.size 
        };
    }

    async removeAllWallets(groupId = null) {
        console.log(`[${new Date().toISOString()}] 🗑️ Removing wallets from optimized monitoring${groupId ? ` for group ${groupId}` : ''}`);

        const oldSize = this.monitoredWallets.size;
        await this.loadMonitoredWallets(this.activeGroupId);

        if (!groupId) {
            this.processedTransactions.clear();
            this.recentlyProcessed.clear();
            this.duplicateCache.clear();
        } else {

            this.recentlyProcessed.clear();
            this.duplicateCache.clear();
        }

        console.log(`[${new Date().toISOString()}] ✅ Optimized monitoring updated: ${oldSize.toLocaleString()} → ${this.monitoredWallets.size.toLocaleString()} wallets`);

        return {
            success: true,
            message: 'Optimized wallet monitoring updated',
            details: {
                previousWallets: oldSize,
                remainingWallets: this.monitoredWallets.size,
                groupId
            }
        };
    }

    getStatus() {
        return {
            isConnected: this.stream !== null,
            isStarted: this.isStarted,
            activeGroupId: this.activeGroupId,
            totalSubscriptions: this.monitoredWallets.size,
            numStreams: 1,
            messageCount: this.messageCount,
            filteredCount: this.filteredCount,
            reconnectAttempts: this.reconnectAttempts,
            grpcEndpoint: this.grpcEndpoint,
            mode: 'optimized_stream_300k_plus',
            realtimeMode: this.realtimeMode,
            performance: {
                totalReceived: this.stats.totalReceived,
                totalFiltered: this.stats.totalFiltered,
                totalProcessed: this.stats.totalProcessed,
                totalSkipped: this.stats.totalSkipped,
                duplicatesSkipped: this.stats.duplicatesSkipped,
                walletHits: this.stats.walletHits,
                filterEfficiency: this.stats.filterEfficiency,
                avgFilterTime: this.stats.avgFilterTime,
                queueSize: this.processingQueue.length,
                queueProcessing: this.queueProcessing,
                cacheStats: {
                    processedTransactions: this.processedTransactions.size,
                    recentlyProcessed: this.recentlyProcessed.size,
                    duplicateCache: this.duplicateCache.size,
                    walletMetadata: this.walletMetadata.size,
                    solPriceAge: Date.now() - this.solPriceCache.lastUpdated,
                    solPrice: this.solPriceCache.price
                }
            }
        };
    }

    getPerformanceStats() {
        const now = Date.now();
        return {
            mode: 'optimized_stream_300k_plus',
            realtimeMode: this.realtimeMode,
            totalMonitoredWallets: this.monitoredWallets.size,
            messagesReceived: this.messageCount,
            messagesFiltered: this.filteredCount,
            messagesProcessed: this.stats.totalProcessed,
            messagesSkipped: this.stats.totalSkipped,
            duplicatesSkipped: this.stats.duplicatesSkipped,
            walletHits: this.stats.walletHits,
            filterEfficiency: parseFloat(this.stats.filterEfficiency),
            avgFilterTimeMs: this.stats.avgFilterTime,
            currentBatchSize: this.realtimeMode ? 0 : this.transactionBatch?.size || 0,
            processingQueue: {
                size: this.processingQueue.length,
                maxSize: this.maxQueueSize,
                processing: this.queueProcessing
            },
            caches: {
                processedTransactions: this.processedTransactions.size,
                recentlyProcessed: this.recentlyProcessed.size,
                duplicateCache: this.duplicateCache.size,
                walletMetadata: this.walletMetadata.size,
                walletToGroup: this.walletToGroup.size
            },
            solPriceCache: {
                price: this.solPriceCache.price,
                lastUpdated: this.solPriceCache.lastUpdated,
                ageMs: now - this.solPriceCache.lastUpdated,
                cacheTimeout: this.solPriceCache.cacheTimeout
            },
            thresholds: {
                buyThreshold: this.BUY_THRESHOLD,
                sellThreshold: this.SELL_THRESHOLD,
                minTokenChange: this.MIN_TOKEN_CHANGE
            },
            reconnectAttempts: this.reconnectAttempts,
            isHealthy: this.isStarted && this.stream !== null && this.monitoredWallets.size > 0
        };
    }

    forceCleanupCaches() {
        const before = {
            processedTransactions: this.processedTransactions.size,
            recentlyProcessed: this.recentlyProcessed.size,
            duplicateCache: this.duplicateCache.size,
            walletMetadata: this.walletMetadata.size
        };

        if (this.processedTransactions.size > 50000) {
            const toDeleteProcessed = Array.from(this.processedTransactions).slice(0, Math.floor(this.processedTransactions.size * 0.6));
            toDeleteProcessed.forEach(sig => this.processedTransactions.delete(sig));
        }

        if (this.recentlyProcessed.size > 25000) {
            const toDeleteRecent = Array.from(this.recentlyProcessed.keys()).slice(0, Math.floor(this.recentlyProcessed.size * 0.6));
            toDeleteRecent.forEach(key => this.recentlyProcessed.delete(key));
        }

        if (this.duplicateCache.size > 25000) {
            const toDeleteDup = Array.from(this.duplicateCache).slice(0, Math.floor(this.duplicateCache.size * 0.6));
            toDeleteDup.forEach(key => this.duplicateCache.delete(key));
        }

        this.lastProcessedCleanup = Date.now();
        this.lastRecentlyProcessedCleanup = Date.now();
        this.lastDuplicateCacheCleanup = Date.now();

        const after = {
            processedTransactions: this.processedTransactions.size,
            recentlyProcessed: this.recentlyProcessed.size,
            duplicateCache: this.duplicateCache.size,
            walletMetadata: this.walletMetadata.size
        };

        console.log(`[${new Date().toISOString()}] 🧹 OPTIMIZED force cleanup completed:`, { before, after });
        return { before, after };
    }

    clearCaches() {
        console.log(`[${new Date().toISOString()}] 🧹 OPTIMIZED cache cleanup for 300k+ wallets`);
        return this.forceCleanupCaches();
    }

    async stop() {
        console.log(`[${new Date().toISOString()}] ⏹️ Stopping OPTIMIZED stream service`);

        this.isStarted = false;

        this.queueProcessing = false;

        if (!this.realtimeMode && this.processingQueue.length > 0) {
            console.log(`[${new Date().toISOString()}] ⚡ Processing final queue of ${this.processingQueue.length} transactions`);
            await this.processQueueBatch();
        }

        await this.endStream();

        console.log(`[${new Date().toISOString()}] ✅ OPTIMIZED stream service stopped`);
    }

    async shutdown() {
        console.log(`[${new Date().toISOString()}] 🛑 Shutting down OPTIMIZED stream service`);

        await this.stop();

        this.processedTransactions.clear();
        this.recentlyProcessed.clear();
        this.duplicateCache.clear();
        this.processingQueue.length = 0;
        this.monitoredWallets.clear();
        this.walletToGroup.clear();
        this.walletMetadata.clear();

        try {
            await this.db.close();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error closing DB:`, error.message);
        }

        console.log(`[${new Date().toISOString()}] ✅ OPTIMIZED stream service shutdown complete`);
    }
}

module.exports = SolanaGrpcService;