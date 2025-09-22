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
        this.reconnectInterval = 5000;
        this.maxReconnectAttempts = 10;
        this.reconnectAttempts = 0;
        this.messageCount = 0;
        this.filteredCount = 0;
        this.activeGroupId = null;

        this.monitoredWallets = new Set(); 
        this.walletToGroup = new Map(); 
        this.walletMetadata = new Map(); 

        this.processedTransactions = new Set();
        this.recentlyProcessed = new Set();
        this.solPriceCache = {
            price: 150,
            lastUpdated: 0,
            cacheTimeout: 60000
        };

        this.transactionBatch = new Map();
        this.batchTimer = null;
        this.batchSize = 500; 
        this.batchTimeout = 50; 

        this.BUY_THRESHOLD = parseFloat(process.env.SOL_BUY_THRESHOLD) || 0.01;
        this.SELL_THRESHOLD = parseFloat(process.env.SOL_SELL_THRESHOLD) || 0.001;

        this.stats = {
            totalReceived: 0,
            totalFiltered: 0,
            totalProcessed: 0,
            filterEfficiency: 0,
            avgFilterTime: 0,
            lastStatsUpdate: Date.now()
        };

        this.PROCESSED_CLEANUP_INTERVAL = 24 * 60 * 60 * 1000;
        this.RECENTLY_PROCESSED_CLEANUP_INTERVAL = 60 * 60 * 1000;
        this.lastProcessedCleanup = Date.now();
        this.lastRecentlyProcessedCleanup = Date.now();

        this.setupCacheCleanup();
        this.setupStatsReporting();

        console.log(`[${new Date().toISOString()}] 🚀 Optimized Full Stream Service initialized`);
        console.log(`[${new Date().toISOString()}] 💰 SOL thresholds: buy>${this.BUY_THRESHOLD}, sell>${this.SELL_THRESHOLD}`);
    }

    setupStatsReporting() {

        setInterval(() => {
            const now = Date.now();
            const timeDiff = (now - this.stats.lastStatsUpdate) / 1000;

            if (this.stats.totalReceived > 0) {
                this.stats.filterEfficiency = ((this.stats.totalFiltered / this.stats.totalReceived) * 100).toFixed(2);
            }

            console.log(`[${new Date().toISOString()}] 📊 Performance Stats:`);
            console.log(`  📥 Total received: ${this.stats.totalReceived} tx (${(this.stats.totalReceived / timeDiff).toFixed(1)} tx/s)`);
            console.log(`  🎯 Filtered out: ${this.stats.totalFiltered} (${this.stats.filterEfficiency}%)`);
            console.log(`  ✅ Processed: ${this.stats.totalProcessed}`);
            console.log(`  👥 Monitored wallets: ${this.monitoredWallets.size.toLocaleString()}`);
            console.log(`  🔄 Active group: ${this.activeGroupId || 'all'}`);

            this.stats.totalReceived = 0;
            this.stats.totalFiltered = 0;
            this.stats.totalProcessed = 0;
            this.stats.lastStatsUpdate = now;
        }, 30000);
    }

    setupCacheCleanup() {
        setInterval(() => {
            const now = Date.now();

            if (now - this.lastProcessedCleanup >= this.PROCESSED_CLEANUP_INTERVAL) {
                if (this.processedTransactions.size > 100000) {
                    const toDelete = Array.from(this.processedTransactions).slice(0, 50000);
                    toDelete.forEach(sig => this.processedTransactions.delete(sig));
                    console.log(`[${new Date().toISOString()}] 🧹 Daily cleanup: removed ${toDelete.length} processed transactions`);
                }
                this.lastProcessedCleanup = now;
            }

            if (now - this.lastRecentlyProcessedCleanup >= this.RECENTLY_PROCESSED_CLEANUP_INTERVAL) {
                if (this.recentlyProcessed.size > 10000) {
                    const toDelete = Array.from(this.recentlyProcessed).slice(0, 5000);
                    toDelete.forEach(key => this.recentlyProcessed.delete(key));
                    console.log(`[${new Date().toISOString()}] 🧹 Hourly cleanup: removed ${toDelete.length} recent entries`);
                }
                this.lastRecentlyProcessedCleanup = now;
            }
        }, 300000); 
    }

    async loadMonitoredWallets(groupId = null) {
        const startTime = Date.now();
        console.log(`[${new Date().toISOString()}] 📋 Loading monitored wallets${groupId ? ` for group ${groupId}` : ' (all groups)'}`);

        try {
            const wallets = await this.db.getActiveWallets(groupId);

            this.monitoredWallets.clear();
            this.walletToGroup.clear();
            this.walletMetadata.clear();

            for (const wallet of wallets) {
                this.monitoredWallets.add(wallet.address);
                this.walletToGroup.set(wallet.address, wallet.group_id);
                this.walletMetadata.set(wallet.address, {
                    id: wallet.id,
                    name: wallet.name,
                    group_id: wallet.group_id,
                    group_name: wallet.group_name
                });
            }

            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] ✅ Loaded ${this.monitoredWallets.size.toLocaleString()} wallets in ${duration}ms`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error loading wallets:`, error.message);
            throw error;
        }
    }

    async start(groupId = null) {
        if (this.isStarted && this.activeGroupId === groupId) {
            console.log(`[${new Date().toISOString()}] ℹ️ Full stream service already running for group ${groupId || 'all'}`);
            return;
        }

        console.log(`[${new Date().toISOString()}] 🚀 Starting full Solana stream for group ${groupId || 'all'}`);

        this.isStarted = true;
        this.activeGroupId = groupId;

        try {
            await this.loadMonitoredWallets(groupId);
            await this.createFullStream();

            console.log(`[${new Date().toISOString()}] ✅ Full stream service started successfully`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to start full stream service:`, error.message);
            this.isStarted = false;
            throw error;
        }
    }

    async createFullStream() {
        await this.endStream();

        try {
            console.log(`[${new Date().toISOString()}] 🔗 Connecting to full Solana stream...`);

            this.client = new Client(this.grpcEndpoint, undefined, {
                'grpc.keepalive_time_ms': 30000,
                'grpc.keepalive_timeout_ms': 5000,
                'grpc.keepalive_permit_without_calls': true,
                'grpc.http2.max_pings_without_data': 0,
                'grpc.http2.min_time_between_pings_ms': 10000,
                'grpc.http2.min_ping_interval_without_data_ms': 300000,
                'grpc.max_receive_message_length': 256 * 1024 * 1024, 
                'grpc.max_send_message_length': 256 * 1024 * 1024,
                'grpc.http2.max_concurrent_streams': 1000,
                'grpc.keepalive_without_calls': true
            });

            this.stream = await this.client.subscribe();

            this.stream.on('data', data => {
                this.messageCount++;
                this.stats.totalReceived++;
                this.handleFullStreamMessage(data);
            });

            this.stream.on('error', error => {
                console.error(`[${new Date().toISOString()}] ❌ Full stream error:`, error.message);
                this.handleReconnect();
            });

            this.stream.on('end', () => {
                console.log(`[${new Date().toISOString()}] 📡 Full stream ended`);
                if (this.isStarted) {
                    setTimeout(() => this.handleReconnect(), 2000);
                }
            });

            const request = {
                accounts: {},
                slots: {},
                transactions: {
                    client: {
                        vote: false,           
                        failed: false,         

                    }
                },
                transactionsStatus: {},
                entry: {},
                blocks: {},
                blocksMeta: {},
                commitment: CommitmentLevel.CONFIRMED,
                accountsDataSlice: []
            };

            console.log(`[${new Date().toISOString()}] 📡 Subscribing to FULL Solana transaction stream...`);

            await new Promise((resolve, reject) => {
                this.stream.write(request, err => {
                    if (err) {
                        console.error(`[${new Date().toISOString()}] ❌ Full stream subscription failed:`, err.message);
                        reject(err);
                    } else {
                        console.log(`[${new Date().toISOString()}] ✅ Full Solana stream subscription active`);
                        resolve();
                    }
                });
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to create full stream:`, error.message);
            throw error;
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

    handleFullStreamMessage(data) {
        try {
            if (!data.transaction) return;

            const signature = this.extractSignature(data.transaction);
            if (!signature) return;

            if (this.processedTransactions.has(signature)) {
                this.stats.totalFiltered++;
                return;
            }

            const filterStart = process.hrtime.bigint();
            const isRelevant = this.quickFilterTransaction(data.transaction);
            const filterTime = Number(process.hrtime.bigint() - filterStart) / 1000000; 

            if (!isRelevant) {
                this.stats.totalFiltered++;
                this.stats.avgFilterTime = (this.stats.avgFilterTime + filterTime) / 2;
                return;
            }

            this.transactionBatch.set(signature, data);

            if (!this.batchTimer) {
                this.batchTimer = setTimeout(() => {
                    this.processBatch();
                }, this.batchTimeout);
            }

            if (this.transactionBatch.size >= this.batchSize) {
                clearTimeout(this.batchTimer);
                this.batchTimer = null;
                this.processBatch();
            }

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error handling full stream message:`, error.message);
        }
    }

    quickFilterTransaction(transactionData) {
        try {

            const accountKeys = this.extractAllAccountKeys(transactionData);

            for (const accountKey of accountKeys) {
                if (this.monitoredWallets.has(accountKey)) {

                    if (this.activeGroupId) {
                        const walletGroup = this.walletToGroup.get(accountKey);
                        if (walletGroup === this.activeGroupId) {
                            return true;
                        }
                    } else {
                        return true;
                    }
                }
            }

            return false;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in quick filter:`, error.message);
            return false;
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

        return accountKeys;
    }

    async processBatch() {
        if (this.transactionBatch.size === 0) return;

        const batch = new Map(this.transactionBatch);
        this.transactionBatch.clear();
        this.batchTimer = null;

        console.log(`[${new Date().toISOString()}] ⚡ Processing filtered batch: ${batch.size} relevant transactions`);

        const promises = Array.from(batch.entries()).map(([signature, data]) =>
            this.processTransaction(data.transaction).catch(error => {
                console.error(`[${new Date().toISOString()}] ❌ Failed to process ${signature}:`, error.message);
                return null;
            })
        );

        const results = await Promise.allSettled(promises);
        const successful = results.filter(r => r.status === 'fulfilled' && r.value !== null).length;

        this.stats.totalProcessed += successful;

        console.log(`[${new Date().toISOString()}] ✅ Batch completed: ${successful}/${batch.size} successful`);
    }

    async processTransaction(transactionData) {
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

            if (!transaction || !meta || meta.err) {
                return null;
            }

            const signature = this.extractSignature(transactionData) || transactionData.signature;
            if (!signature) return null;

            const processedKey = `${signature}`;
            if (this.processedTransactions.has(signature) || this.recentlyProcessed.has(processedKey)) {
                return null;
            }

            this.processedTransactions.add(signature);
            this.recentlyProcessed.add(processedKey);

            const existingTx = await this.db.pool.query(
                'SELECT id FROM transactions WHERE signature = $1 LIMIT 1',
                [signature]
            );
            if (existingTx.rows.length > 0) {
                return null;
            }

            const accountKeys = this.extractAllAccountKeys(transactionData);
            const relevantWallet = this.findRelevantWallet(accountKeys);

            if (!relevantWallet) return null;

            const blockTime = Number(transactionData.blockTime) || Math.floor(Date.now() / 1000);

            return await this.processTransactionFromGrpcData({
                signature,
                transaction,
                meta,
                blockTime,
                wallet: relevantWallet,
                accountKeys
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing transaction:`, error.message);
            return null;
        }
    }

    findRelevantWallet(accountKeys) {
        for (const accountKey of accountKeys) {
            if (this.monitoredWallets.has(accountKey)) {
                const walletMetadata = this.walletMetadata.get(accountKey);
                if (walletMetadata) {

                    if (this.activeGroupId && walletMetadata.group_id !== this.activeGroupId) {
                        continue;
                    }
                    return {
                        address: accountKey,
                        ...walletMetadata
                    };
                }
            }
        }
        return null;
    }

    async fetchSolPrice() {
        const now = Date.now();

        if (now - this.solPriceCache.lastUpdated < this.solPriceCache.cacheTimeout) {
            return this.solPriceCache.price;
        }

        try {
            const cachedPrice = await redis.get('sol_price_grpc');
            if (cachedPrice) {
                const priceData = JSON.parse(cachedPrice);
                this.solPriceCache = {
                    price: priceData.price,
                    lastUpdated: priceData.timestamp,
                    cacheTimeout: 60000
                };
                return priceData.price;
            }

            const response = await fetch('https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112', {
                timeout: 5000,
                headers: { 'User-Agent': 'WalletPulse/3.0' }
            });

            if (response.ok) {
                const data = await response.json();
                if (data.pairs && data.pairs.length > 0) {
                    const bestPair = data.pairs.reduce((prev, current) =>
                        (current.volume?.h24 || 0) > (prev.volume?.h24 || 0) ? current : prev
                    );
                    const newPrice = parseFloat(bestPair.priceUsd || 150);

                    this.solPriceCache = {
                        price: newPrice,
                        lastUpdated: now,
                        cacheTimeout: 60000
                    };

                    await redis.setex('sol_price_grpc', 60, JSON.stringify({
                        price: newPrice,
                        timestamp: now
                    }));

                    return newPrice;
                }
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error fetching SOL price:`, error.message);
        }

        return this.solPriceCache.price;
    }

    async handleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] 🛑 Max reconnect attempts reached, stopping service`);
            this.isStarted = false;
            return;
        }

        this.reconnectAttempts++;
        console.log(`[${new Date().toISOString()}] 🔄 Reconnecting full stream (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

        await this.endStream();

        if (this.batchTimer) {
            clearTimeout(this.batchTimer);
            this.batchTimer = null;
        }

        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));

        try {
            await this.createFullStream();
            console.log(`[${new Date().toISOString()}] ✅ Full stream reconnection successful`);
            this.reconnectAttempts = 0;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Full stream reconnect failed:`, error.message);
            this.reconnectInterval = Math.min(this.reconnectInterval * 1.5, 30000);
            await this.handleReconnect();
        }
    }

    async switchGroup(groupId) {
        console.log(`[${new Date().toISOString()}] 🔄 Switching full stream to group ${groupId || 'all'}`);

        this.activeGroupId = groupId;

        await this.loadMonitoredWallets(groupId);

        console.log(`[${new Date().toISOString()}] ✅ Switched to group ${groupId || 'all'}: monitoring ${this.monitoredWallets.size.toLocaleString()} wallets`);

        return {
            success: true,
            activeGroupId: this.activeGroupId,
            monitoredWallets: this.monitoredWallets.size
        };
    }

    async subscribeToWalletsBatch(walletAddresses, batchSize = 10000) {
        console.log(`[${new Date().toISOString()}] ➕ Adding ${walletAddresses.length} wallets to full stream monitoring`);

        const successful = walletAddresses.length;

        await this.loadMonitoredWallets(this.activeGroupId);

        console.log(`[${new Date().toISOString()}] ✅ Wallet monitoring updated: ${this.monitoredWallets.size.toLocaleString()} total wallets`);

        return { 
            successful, 
            failed: 0, 
            errors: [], 
            totalMonitored: this.monitoredWallets.size 
        };
    }

    async removeAllWallets(groupId = null) {
        console.log(`[${new Date().toISOString()}] 🗑️ Removing wallets from full stream monitoring${groupId ? ` for group ${groupId}` : ''}`);

        await this.loadMonitoredWallets(this.activeGroupId);

        return {
            success: true,
            message: 'Wallet monitoring updated for full stream',
            details: {
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
            mode: 'full_stream_with_filtering',
            performance: {
                totalReceived: this.stats.totalReceived,
                totalFiltered: this.stats.totalFiltered,
                totalProcessed: this.stats.totalProcessed,
                filterEfficiency: this.stats.filterEfficiency,
                avgFilterTime: this.stats.avgFilterTime,
                batchSize: this.batchSize,
                batchTimeout: this.batchTimeout,
                solPriceCached: this.solPriceCache.lastUpdated > 0,
                cacheStats: {
                    processedTransactions: this.processedTransactions.size,
                    recentlyProcessed: this.recentlyProcessed.size,
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
            mode: 'full_stream_optimized',
            totalMonitoredWallets: this.monitoredWallets.size,
            messagesReceived: this.messageCount,
            messagesFiltered: this.filteredCount,
            messagesProcessed: this.stats.totalProcessed,
            filterEfficiency: this.stats.filterEfficiency,
            avgFilterTimeMs: this.stats.avgFilterTime,
            currentBatchSize: this.transactionBatch.size,
            caches: {
                processedTransactions: this.processedTransactions.size,
                recentlyProcessed: this.recentlyProcessed.size,
                walletMetadata: this.walletMetadata.size,
                walletToGroup: this.walletToGroup.size
            },
            solPriceCache: {
                price: this.solPriceCache.price,
                lastUpdated: this.solPriceCache.lastUpdated,
                ageMs: now - this.solPriceCache.lastUpdated
            },
            reconnectAttempts: this.reconnectAttempts,
            isHealthy: this.isStarted && this.stream !== null
        };
    }

    async processTransactionFromGrpcData({ signature, transaction, meta, blockTime, wallet, accountKeys }) {
        try {
            const walletIndex = accountKeys.indexOf(wallet.address);
            if (walletIndex === -1) return null;

            const preBalance = meta.preBalances[walletIndex] || 0;
            const postBalance = meta.postBalances[walletIndex] || 0;
            const solChange = (postBalance - preBalance) / 1e9;

            const solPrice = await this.fetchSolPrice();

            const { transactionType, totalSolAmount, tokenChanges } = await this.analyzeTransactionFromGrpc({
                meta,
                solChange,
                walletAddress: wallet.address,
                solPrice
            });

            if (!transactionType || tokenChanges.length === 0) {
                return null;
            }

            const savedTransaction = await this.saveTransactionToDb({
                wallet,
                signature,
                blockTime,
                transactionType,
                totalSolAmount,
                tokenChanges,
                solPrice
            });

            if (savedTransaction) {
                const transactionMessage = {
                    signature,
                    walletAddress: wallet.address,
                    walletName: wallet.name,
                    groupId: wallet.group_id,
                    groupName: wallet.group_name,
                    transactionType,
                    solAmount: totalSolAmount,
                    tokens: tokenChanges.map(tc => ({
                        mint: tc.mint,
                        amount: tc.amount,
                        symbol: tc.symbol,
                        name: tc.name
                    })),
                    timestamp: new Date(blockTime * 1000).toISOString()
                };

                const pipeline = redis.pipeline();
                pipeline.publish('transactions', JSON.stringify(transactionMessage));
                if (wallet.group_id) {
                    pipeline.publish(`transactions:group:${wallet.group_id}`, JSON.stringify(transactionMessage));
                }
                await pipeline.exec();

                console.log(`[${new Date().toISOString()}] ✅ Processed transaction ${signature} (${transactionType}) for wallet ${wallet.address.slice(0, 8)}...`);
                return savedTransaction;
            }

            return null;

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing gRPC transaction:`, error.message);
            return null;
        }
    }

    async saveTransactionToDb({ wallet, signature, blockTime, transactionType, totalSolAmount, tokenChanges, solPrice }) {
        try {
            return await this.db.withTransaction(async (client) => {
                const finalCheck = await client.query(
                    'SELECT id FROM transactions WHERE signature = $1 LIMIT 1',
                    [signature]
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
                    this.saveTokenOperationInTransaction(client, transaction.id, tokenChange, transactionType)
                );
                await Promise.all(tokenPromises);

                return {
                    signature: signature,
                    type: transactionType,
                    solAmount: totalSolAmount,
                    tokensChanged: tokenChanges,
                };
            });
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error saving transaction to DB:`, error.message);
            return null;
        }
    }

    async saveTokenOperationInTransaction(client, transactionId, tokenChange, transactionType) {
        try {
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
                tokenChange.symbol,
                tokenChange.name,
                tokenChange.decimals,
            ]);

            const tokenId = tokenResult.rows[0].id;

            const operationQuery = `
                INSERT INTO token_operations (transaction_id, token_id, amount, operation_type) 
                VALUES ($1, $2, $3, $4)
            `;

            await client.query(operationQuery, [
                transactionId,
                tokenId,
                tokenChange.amount,
                transactionType
            ]);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error saving token operation:`, error.message);
            throw error;
        }
    }

    async analyzeTransactionFromGrpc({ meta, solChange, walletAddress, solPrice }) {
        const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

        let transactionType = null;
        let totalSolAmount = 0;

        const usdcPreBalance = (meta.preTokenBalances || []).find(b =>
            b.mint === USDC_MINT && b.owner === walletAddress
        );
        const usdcPostBalance = (meta.postTokenBalances || []).find(b =>
            b.mint === USDC_MINT && b.owner === walletAddress
        );

        let usdcChange = 0;
        if (usdcPreBalance && usdcPostBalance) {
            usdcChange = (Number(usdcPostBalance.uiTokenAmount.amount) -
                Number(usdcPreBalance.uiTokenAmount.amount)) / 1e6;
        } else if (usdcPostBalance) {
            usdcChange = Number(usdcPostBalance.uiTokenAmount.uiAmount || 0);
        } else if (usdcPreBalance) {
            usdcChange = -Number(usdcPreBalance.uiTokenAmount.uiAmount || 0);
        }

        if (usdcChange < 0) {
            transactionType = 'buy';
            totalSolAmount = Math.abs(usdcChange) / solPrice;
        } else if (usdcChange > 0) {
            transactionType = 'sell';
            totalSolAmount = usdcChange / solPrice;
        } else if (solChange < -this.BUY_THRESHOLD) {
            transactionType = 'buy';
            totalSolAmount = Math.abs(solChange);
        } else if (solChange > this.SELL_THRESHOLD) {
            transactionType = 'sell';
            totalSolAmount = solChange;
        } else {
            return { transactionType: null, totalSolAmount: 0, tokenChanges: [] };
        }

        const tokenChanges = await this.analyzeTokenChangesFromGrpc(
            meta,
            transactionType,
            walletAddress
        );

        return { transactionType, totalSolAmount, tokenChanges };
    }

    async analyzeTokenChangesFromGrpc(meta, transactionType, walletAddress) {
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
                }
            }
        }

        if (mintChanges.size === 0) {
            return [];
        }

        const mints = Array.from(mintChanges.keys());
        const tokenInfos = await this.batchFetchTokenMetadataCached(mints);

        for (const [mint, aggregatedChange] of mintChanges) {
            const tokenInfo = tokenInfos.get(mint) || {
                symbol: mint.slice(0, 4).toUpperCase(),
                name: `Token ${mint.slice(0, 8)}...`,
                decimals: aggregatedChange.decimals,
            };

            tokenChanges.push({
                mint: mint,
                amount: aggregatedChange.totalRawChange / Math.pow(10, aggregatedChange.decimals),
                rawChange: aggregatedChange.totalRawChange,
                decimals: aggregatedChange.decimals,
                symbol: tokenInfo.symbol,
                name: tokenInfo.name,
            });
        }

        return tokenChanges;
    }

    async batchFetchTokenMetadataCached(mints) {
        const tokenInfos = new Map();
        const uncachedMints = [];

        const pipeline = redis.pipeline();
        for (const mint of mints) {
            pipeline.get(`token:${mint}`);
        }
        const results = await pipeline.exec();

        results.forEach(([err, cachedToken], index) => {
            if (!err && cachedToken) {
                try {
                    tokenInfos.set(mints[index], JSON.parse(cachedToken));
                } catch (parseError) {
                    uncachedMints.push(mints[index]);
                }
            } else {
                uncachedMints.push(mints[index]);
            }
        });

        if (uncachedMints.length > 0) {
            try {
                const newTokenInfos = await batchFetchTokenMetadata(uncachedMints, null);

                const cachePipeline = redis.pipeline();
                for (const [mint, tokenInfo] of newTokenInfos) {
                    if (tokenInfo) {
                        tokenInfos.set(mint, tokenInfo);
                        cachePipeline.set(`token:${mint}`, JSON.stringify(tokenInfo), 'EX', 24 * 60 * 60);
                    }
                }
                await cachePipeline.exec();

            } catch (error) {
                console.error(`[${new Date().toISOString()}] ❌ Error batch fetching token metadata:`, error.message);
                for (const mint of uncachedMints) {
                    if (!tokenInfos.has(mint)) {
                        tokenInfos.set(mint, {
                            symbol: mint.slice(0, 4).toUpperCase(),
                            name: `Token ${mint.slice(0, 8)}...`,
                            decimals: 6,
                        });
                    }
                }
            }
        }

        return tokenInfos;
    }

    extractSignature(transactionData) {
        try {
            const sigObj = transactionData.signature ||
                (transactionData.signatures && transactionData.signatures[0]) ||
                transactionData.transaction?.signature ||
                (transactionData.transaction?.signatures && transactionData.transaction.signatures[0]) ||
                transactionData.tx?.signature ||
                (transactionData.tx?.signatures && transactionData.tx.signatures[0]);

            if (!sigObj) {
                return null;
            }

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

    forceCleanupCaches() {
        const before = {
            processedTransactions: this.processedTransactions.size,
            recentlyProcessed: this.recentlyProcessed.size,
            walletMetadata: this.walletMetadata.size
        };

        if (this.processedTransactions.size > 10000) {
            const toDeleteProcessed = Array.from(this.processedTransactions).slice(0, Math.floor(this.processedTransactions.size / 2));
            toDeleteProcessed.forEach(sig => this.processedTransactions.delete(sig));
        } else {
            this.processedTransactions.clear();
        }

        if (this.recentlyProcessed.size > 5000) {
            const toDeleteRecent = Array.from(this.recentlyProcessed).slice(0, Math.floor(this.recentlyProcessed.size / 2));
            toDeleteRecent.forEach(key => this.recentlyProcessed.delete(key));
        } else {
            this.recentlyProcessed.clear();
        }

        this.lastProcessedCleanup = Date.now();
        this.lastRecentlyProcessedCleanup = Date.now();

        const after = {
            processedTransactions: this.processedTransactions.size,
            recentlyProcessed: this.recentlyProcessed.size,
            walletMetadata: this.walletMetadata.size
        };

        console.log(`[${new Date().toISOString()}] 🧹 Force cleanup completed for full stream:`, { before, after });
        return { before, after };
    }

    clearCaches() {
        console.log(`[${new Date().toISOString()}] 🧹 Manual cache cleanup for full stream`);
        return this.forceCleanupCaches();
    }

    async stop() {
        console.log(`[${new Date().toISOString()}] ⏹️ Stopping full stream service`);

        this.isStarted = false;

        if (this.batchTimer) {
            clearTimeout(this.batchTimer);
            this.batchTimer = null;
        }

        if (this.transactionBatch.size > 0) {
            console.log(`[${new Date().toISOString()}] ⚡ Processing final batch of ${this.transactionBatch.size} transactions`);
            await this.processBatch();
        }

        await this.endStream();

        console.log(`[${new Date().toISOString()}] ✅ Full stream service stopped`);
    }

    async shutdown() {
        console.log(`[${new Date().toISOString()}] 🛑 Shutting down full stream service`);

        await this.stop();

        this.processedTransactions.clear();
        this.recentlyProcessed.clear();
        this.transactionBatch.clear();
        this.monitoredWallets.clear();
        this.walletToGroup.clear();
        this.walletMetadata.clear();

        try {
            await this.db.close();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error closing DB:`, error.message);
        }

        console.log(`[${new Date().toISOString()}] ✅ Full stream service shutdown complete`);
    }
}

module.exports = SolanaGrpcService;