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
        this.streams = [];
        this.db = new Database();
        this.isStarted = false;
        this.isConnecting = false;
        this.reconnectInterval = 5000;
        this.maxReconnectAttempts = 10;
        this.reconnectAttempts = 0;
        this.messageCount = 0;
        this.activeGroupId = null;
        this.allMonitoredWallets = new Set();
        this.chunkSize = parseInt(process.env.GRPC_CHUNK_SIZE) || 1000;
        this.processedTransactions = new Set();
        this.recentlyProcessed = new Set();
        this.solPriceCache = {
            price: 150,
            lastUpdated: 0,
            cacheTimeout: 60000
        };
        this.transactionBatch = new Map();
        this.batchTimer = null;
        this.batchSize = 50;
        this.batchTimeout = 200;
        this.BUY_THRESHOLD = parseFloat(process.env.SOL_BUY_THRESHOLD) || 0.01;
        this.SELL_THRESHOLD = parseFloat(process.env.SOL_SELL_THRESHOLD) || 0.001;
        this.PROCESSED_CLEANUP_INTERVAL = 24 * 60 * 60 * 1000;
        this.RECENTLY_PROCESSED_CLEANUP_INTERVAL = 60 * 60 * 1000;
        this.lastProcessedCleanup = Date.now();
        this.lastRecentlyProcessedCleanup = Date.now();
        this.setupCacheCleanup();
        console.log(`[${new Date().toISOString()}] 💰 SOL thresholds: buy>${this.BUY_THRESHOLD}, sell>${this.SELL_THRESHOLD}`);
    }

    setupCacheCleanup() {
        setInterval(() => {
            const now = Date.now();
            
            if (now - this.lastProcessedCleanup >= this.PROCESSED_CLEANUP_INTERVAL) {
                if (this.processedTransactions.size > 50000) {
                    const toDelete = Array.from(this.processedTransactions).slice(0, 25000);
                    toDelete.forEach(sig => this.processedTransactions.delete(sig));
                    console.log(`[${new Date().toISOString()}] 🧹 Daily cleanup: removed ${toDelete.length} processed transactions (total: ${this.processedTransactions.size})`);
                }
                this.lastProcessedCleanup = now;
            }
            
            if (now - this.lastRecentlyProcessedCleanup >= this.RECENTLY_PROCESSED_CLEANUP_INTERVAL) {
                if (this.recentlyProcessed.size > 5000) {
                    const toDelete = Array.from(this.recentlyProcessed).slice(0, 2500);
                    toDelete.forEach(key => this.recentlyProcessed.delete(key));
                    console.log(`[${new Date().toISOString()}] 🧹 Hourly cleanup: removed ${toDelete.length} recently processed entries (total: ${this.recentlyProcessed.size})`);
                }
                this.lastRecentlyProcessedCleanup = now;
            }
            
            if (now % (6 * 60 * 60 * 1000) < 300000) { 
                console.log(`[${new Date().toISOString()}] 📊 Cache stats: processedTransactions=${this.processedTransactions.size}, recentlyProcessed=${this.recentlyProcessed.size}`);
                console.log(`[${new Date().toISOString()}] 📊 Last cleanup: processed=${new Date(this.lastProcessedCleanup).toISOString()}, recent=${new Date(this.lastRecentlyProcessedCleanup).toISOString()}`);
            }
        }, 300000); 
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

    async start(groupId = null) {
        if (this.isStarted && this.activeGroupId === groupId) {
            console.log(`[${new Date().toISOString()}] [INFO] gRPC service already started for group ${groupId || 'all'}`);
            return;
        }
        console.log(`[${new Date().toISOString()}] [INFO] Starting gRPC service for group ${groupId || 'all'}`);
        this.isStarted = true;
        this.activeGroupId = groupId;
        try {
            if (this.allMonitoredWallets.size === 0) {
                console.log(`[${new Date().toISOString()}] [INFO] Fetching all active wallets globally`);
                const allWalletsData = await this.db.getActiveWallets();
                allWalletsData.forEach(wallet => this.allMonitoredWallets.add(wallet.address));
                console.log(`[${new Date().toISOString()}] 📊 Loaded ${this.allMonitoredWallets.size} unique wallets globally`);
            }
            await this.createAllStreams();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Failed to start gRPC service: ${error.message}`);
            this.isStarted = false;
            throw error;
        }
    }

    async createAllStreams() {
        await this.endAllStreams();

        this.streams = [];
        const walletArray = Array.from(this.allMonitoredWallets);
        const numChunks = Math.ceil(walletArray.length / this.chunkSize);
        console.log(`[${new Date().toISOString()}] [INFO] Creating ${numChunks} streams with chunk size ${this.chunkSize}`);

        for (let i = 0; i < numChunks; i++) {
            const start = i * this.chunkSize;
            const end = Math.min(start + this.chunkSize, walletArray.length);
            const chunkWallets = new Set(walletArray.slice(start, end));
            await this.createSingleStream(chunkWallets);
            if (i < numChunks - 1) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        console.log(`[${new Date().toISOString()}] [INFO] All streams created successfully. Total streams: ${this.streams.length}`);
    }

    async createSingleStream(chunkWallets) {
        if (chunkWallets.size === 0) return;

        try {
            console.log(`[${new Date().toISOString()}] [INFO] Connecting to gRPC for chunk of ${chunkWallets.size} wallets`);
            const client = new Client(this.grpcEndpoint, undefined, {
                'grpc.keepalive_time_ms': 30000,
                'grpc.keepalive_timeout_ms': 5000,
                'grpc.keepalive_permit_without_calls': true,
                'grpc.http2.max_pings_without_data': 0,
                'grpc.http2.min_time_between_pings_ms': 10000,
                'grpc.http2.min_ping_interval_without_data_ms': 300000,
                'grpc.max_receive_message_length': 64 * 1024 * 1024,
                'grpc.max_send_message_length': 64 * 1024 * 1024
            });

            const stream = await client.subscribe();
            stream.on('data', data => {
                this.messageCount++;
                this.handleGrpcMessageBatched(data);
            });
            stream.on('error', error => {
                console.error(`[${new Date().toISOString()}] [ERROR] gRPC stream error for chunk ${chunkWallets.size}: ${error.message}`);
                this.handleSingleStreamReconnect(stream, client, chunkWallets);
            });
            stream.on('end', () => {
                console.log(`[${new Date().toISOString()}] [INFO] gRPC stream ended for chunk ${chunkWallets.size}`);
                if (this.isStarted) setTimeout(() => this.handleSingleStreamReconnect(stream, client, chunkWallets), 2000);
            });

            const request = {
                accounts: {},
                slots: {},
                transactions: {
                    client: {
                        vote: false,
                        failed: false,
                        accountInclude: Array.from(chunkWallets),
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

            console.log(`[${new Date().toISOString()}] [INFO] Sending subscription for chunk of ${chunkWallets.size} wallets`);
            await new Promise((resolve, reject) => stream.write(request, err => {
                if (err) {
                    console.error(`[${new Date().toISOString()}] [ERROR] Subscription request failed for chunk ${chunkWallets.size}: ${err.message}`);
                    reject(err);
                } else {
                    console.log(`[${new Date().toISOString()}] [INFO] Subscription sent for chunk of ${chunkWallets.size} wallets`);
                    resolve();
                }
            }));

            this.streams.push({ client, stream, chunk: chunkWallets });
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Failed to create stream for chunk ${chunkWallets.size}: ${error.message}`);
            throw error;
        }
    }

    async endAllStreams() {
        for (const { stream, client } of this.streams) {
            try {
                if (stream) stream.end();
                if (client) {
                    if (typeof client.close === 'function') client.close();
                    else if (typeof client.destroy === 'function') client.destroy();
                    else if (typeof client.end === 'function') client.end();
                }
            } catch (error) {
                console.warn(`[${new Date().toISOString()}] [WARN] Error ending stream/client: ${error.message}`);
            }
        }
        this.streams = [];
    }

    async handleSingleStreamReconnect(stream, client, chunkWallets) {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] [CRITICAL] Max reconnect attempts reached for chunk ${chunkWallets.size}, stopping service`);
            this.isStarted = false;
            return;
        }

        this.reconnectAttempts++;
        console.log(`[${new Date().toISOString()}] [INFO] Reconnecting gRPC stream for chunk ${chunkWallets.size} (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

        try {
            if (stream) stream.end();
            if (client) {
                if (typeof client.close === 'function') client.close();
                else if (typeof client.destroy === 'function') client.destroy();
                else if (typeof client.end === 'function') client.end();
            }
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] [WARN] Error closing stream/client: ${error.message}`);
        }

        if (this.batchTimer) {
            clearTimeout(this.batchTimer);
            this.batchTimer = null;
        }

        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));

        try {
            await this.createSingleStream(chunkWallets);
            console.log(`[${new Date().toISOString()}] [INFO] Single stream reconnection successful for chunk ${chunkWallets.size}`);
            this.reconnectAttempts = 0;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Single stream reconnect failed for chunk ${chunkWallets.size}: ${error.message}`);
            this.reconnectInterval = Math.min(this.reconnectInterval * 1.5, 30000);
            await this.handleSingleStreamReconnect(stream, client, chunkWallets);
        }
    }

    handleGrpcMessageBatched(data) {
        try {
            if (!data.transaction) return;

            const signature = this.extractSignature(data.transaction);
            if (!signature) return;

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
            console.error(`[${new Date().toISOString()}] [ERROR] Error handling gRPC message: ${error.message}`);
        }
    }

    async processBatch() {
        if (this.transactionBatch.size === 0) return;

        const batch = new Map(this.transactionBatch);
        this.transactionBatch.clear();
        this.batchTimer = null;

        console.log(`[${new Date().toISOString()}] [INFO] Processing batch of ${batch.size} transactions`);

        const promises = Array.from(batch.entries()).map(([signature, data]) =>
            this.processTransaction(data.transaction).catch(error => {
                console.error(`[${new Date().toISOString()}] [ERROR] Failed to process transaction ${signature}: ${error.message}`);
                return null;
            })
        );

        const results = await Promise.allSettled(promises);
        const successful = results.filter(r => r.status === 'fulfilled' && r.value !== null).length;

        console.log(`[${new Date().toISOString()}] [INFO] Batch processed: ${successful}/${batch.size} successful`);
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

            let accountKeys = transaction.message?.accountKeys || transaction.accountKeys || [];
            if (meta.loadedWritableAddresses) accountKeys = accountKeys.concat(meta.loadedWritableAddresses);
            if (meta.loadedReadonlyAddresses) accountKeys = accountKeys.concat(meta.loadedReadonlyAddresses);

            const stringAccountKeys = this.convertAccountKeysToStrings(accountKeys);
            const involvedWalletAddress = Array.from(this.allMonitoredWallets).find(wallet => stringAccountKeys.includes(wallet));
            if (!involvedWalletAddress) return null;

            const walletCacheKey = `wallet:${involvedWalletAddress}`;
            let wallet = null;

            try {
                const cachedWallet = await redis.get(walletCacheKey);
                if (cachedWallet) {
                    wallet = JSON.parse(cachedWallet);
                } else {
                    wallet = await this.db.getWalletByAddress(involvedWalletAddress);
                    if (wallet) {
                        await redis.setex(walletCacheKey, 300, JSON.stringify(wallet));
                    }
                }
            } catch (error) {
                wallet = await this.db.getWalletByAddress(involvedWalletAddress);
            }

            if (!wallet) return null;

            if (this.activeGroupId && wallet.group_id !== this.activeGroupId) {
                return null;
            }

            const blockTime = Number(transactionData.blockTime) || Math.floor(Date.now() / 1000);

            return await this.processTransactionFromGrpcData({
                signature,
                transaction,
                meta,
                blockTime,
                wallet,
                accountKeys: stringAccountKeys
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Error processing transaction: ${error.message}`);
            return null;
        }
    }

    convertAccountKeysToStrings(accountKeys) {
        const stringAccountKeys = [];

        for (const key of accountKeys) {
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

                if (convertedKey.length === 44) {
                    stringAccountKeys.push(convertedKey);
                }
            } catch (error) {
                console.error(`[${new Date().toISOString()}] [ERROR] Error converting account key: ${error.message}`);
            }
        }

        return stringAccountKeys;
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

                console.log(`[${new Date().toISOString()}] ✅ Processed transaction ${signature} (${transactionType})`);
                return savedTransaction;
            }

            return null;

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing gRPC transaction: ${error.message}`);
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
            console.error(`[${new Date().toISOString()}] ❌ Error saving transaction to DB: ${error.message}`);
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
            console.error(`[${new Date().toISOString()}] ❌ Error saving token operation: ${error.message}`);
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
            console.error(`[${new Date().toISOString()}] [ERROR] Error extracting signature: ${error.message}`);
            return null;
        }
    }

    async subscribeToWalletsBatch(walletAddresses, batchSize = 10000) {
        const startTime = Date.now();
        let successful = 0;
        let failed = 0;
        const errors = [];

        for (let i = 0; i < walletAddresses.length; i += batchSize) {
            const batch = walletAddresses.slice(i, i + batchSize);

            try {
                batch.forEach(address => {
                    if (!this.allMonitoredWallets.has(address)) {
                        this.allMonitoredWallets.add(address);
                        successful++;
                    }
                });

                if (batch.length >= batchSize && i + batchSize < walletAddresses.length) {
                    await new Promise(resolve => setTimeout(resolve, 10));
                }

            } catch (error) {
                failed += batch.length;
                errors.push({
                    batch: i / batchSize + 1,
                    error: error.message,
                    addresses: batch.length
                });
            }
        }

        const duration = Date.now() - startTime;
        console.log(`[${new Date().toISOString()}] [INFO] Batch subscription completed in ${duration}ms: +${successful} wallets, total global: ${this.allMonitoredWallets.size}`);

        if (this.isStarted) {
            await this.createAllStreams();
        }

        return { successful, failed, errors, totalMonitored: this.allMonitoredWallets.size };
    }

    async unsubscribeFromWalletsBatch(walletAddresses, batchSize = 1000) {
        const startTime = Date.now();
        let successful = 0;

        for (let i = 0; i < walletAddresses.length; i += batchSize) {
            const batch = walletAddresses.slice(i, i + batchSize);

            batch.forEach(address => {
                if (this.allMonitoredWallets.has(address)) {
                    this.allMonitoredWallets.delete(address);
                    successful++;
                }
            });
        }

        const duration = Date.now() - startTime;
        console.log(`[${new Date().toISOString()}] [INFO] Batch unsubscription completed in ${duration}ms: -${successful} wallets, total global: ${this.allMonitoredWallets.size}`);

        if (this.isStarted) {
            await this.createAllStreams();
        }

        return { successful, failed: 0, errors: [], totalMonitored: this.allMonitoredWallets.size };
    }

    async subscribeToWallet(walletAddress) {
        if (!this.allMonitoredWallets.has(walletAddress)) {
            this.allMonitoredWallets.add(walletAddress);
            console.log(`[${new Date().toISOString()}] [INFO] Added wallet ${walletAddress} to global monitoring, total: ${this.allMonitoredWallets.size}`);
            if (this.isStarted) {
                await this.createAllStreams();
            }
        }
        return { success: true, totalMonitored: this.allMonitoredWallets.size };
    }

    async unsubscribeFromWallet(walletAddress) {
        if (this.allMonitoredWallets.has(walletAddress)) {
            this.allMonitoredWallets.delete(walletAddress);
            console.log(`[${new Date().toISOString()}] [INFO] Removed wallet ${walletAddress} from global monitoring, total: ${this.allMonitoredWallets.size}`);
            if (this.isStarted) {
                await this.createAllStreams();
            }
        }
        return { success: true, totalMonitored: this.allMonitoredWallets.size };
    }

    async removeAllWallets(groupId = null) {
        console.log(`[${new Date().toISOString()}] [INFO] Removing all wallets for group ${groupId || 'all'}`);

        try {
            const walletsToRemove = await this.db.getActiveWallets(groupId);
            const addressesToRemove = walletsToRemove.map(w => w.address);

            let removedCount = 0;
            addressesToRemove.forEach(address => {
                if (this.allMonitoredWallets.has(address)) {
                    this.allMonitoredWallets.delete(address);
                    removedCount++;
                }
            });

            if (addressesToRemove.length > 0) {
                const pipeline = redis.pipeline();
                addressesToRemove.forEach(address => {
                    pipeline.del(`wallet:${address}`);
                });
                await pipeline.exec();
            }

            if (!groupId || groupId === 'all') {
                this.allMonitoredWallets.clear();
                const remainingWalletsData = await this.db.getActiveWallets();
                remainingWalletsData.forEach(wallet => this.allMonitoredWallets.add(wallet.address));
                console.log(`[${new Date().toISOString()}] [INFO] Reloaded global monitoring: ${this.allMonitoredWallets.size} wallets remaining`);
            }

            if (this.isStarted) {
                await this.createAllStreams();
            }

            return {
                success: true,
                message: `Removed ${removedCount} wallets`,
                details: {
                    walletsRemoved: removedCount,
                    remainingWallets: this.allMonitoredWallets.size,
                    groupId
                }
            };

        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Error removing wallets: ${error.message}`);
            throw error;
        }
    }

    async switchGroup(groupId) {
        console.log(`[${new Date().toISOString()}] [INFO] Switching to group ${groupId || 'all'}`);

        try {
            this.activeGroupId = groupId;

            console.log(`[${new Date().toISOString()}] [INFO] Switched to group ${groupId || 'all'}: filtering ${this.allMonitoredWallets.size} global wallets`);

            return {
                success: true,
                activeGroupId: this.activeGroupId,
                monitoredWallets: this.allMonitoredWallets.size
            };

        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Error switching group: ${error.message}`);
            throw error;
        }
    }

    getStatus() {
        return {
            isConnected: this.streams.length > 0 && this.streams.every(s => s.stream !== null),
            isStarted: this.isStarted,
            activeGroupId: this.activeGroupId,
            totalSubscriptions: this.allMonitoredWallets.size,
            numStreams: this.streams.length,
            messageCount: this.messageCount,
            reconnectAttempts: this.reconnectAttempts,
            grpcEndpoint: this.grpcEndpoint,
            chunkSize: this.chunkSize,
            mode: 'optimized_grpc_sharded',
            performance: {
                processedTransactions: this.processedTransactions.size,
                recentlyProcessed: this.recentlyProcessed.size,
                batchSize: this.batchSize,
                batchTimeout: this.batchTimeout,
                solPriceCached: this.solPriceCache.lastUpdated > 0,
                cacheStats: {
                    solPriceAge: Date.now() - this.solPriceCache.lastUpdated,
                    solPrice: this.solPriceCache.price
                }
            }
        };
    }

    async stop() {
        console.log(`[${new Date().toISOString()}] [INFO] Stopping optimized gRPC service`);

        this.isStarted = false;

        if (this.batchTimer) {
            clearTimeout(this.batchTimer);
            this.batchTimer = null;
        }

        if (this.transactionBatch.size > 0) {
            console.log(`[${new Date().toISOString()}] [INFO] Processing final batch of ${this.transactionBatch.size} transactions`);
            await this.processBatch();
        }

        await this.endAllStreams();

        console.log(`[${new Date().toISOString()}] [INFO] Optimized gRPC service stopped`);
    }

    async shutdown() {
        console.log(`[${new Date().toISOString()}] [INFO] Shutting down optimized gRPC service`);

        await this.stop();

        this.processedTransactions.clear();
        this.recentlyProcessed.clear();
        this.transactionBatch.clear();
        this.allMonitoredWallets.clear();
        this.streams = [];

        try {
            await this.db.close();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Error closing DB: ${error.message}`);
        }

        console.log(`[${new Date().toISOString()}] [INFO] Optimized gRPC service shutdown complete`);
    }

    getPerformanceStats() {
        const now = Date.now();
        return {
            totalMonitoredWallets: this.allMonitoredWallets.size,
            numStreams: this.streams.length,
            messagesProcessed: this.messageCount,
            processedTransactionsCache: this.processedTransactions.size,
            recentlyProcessedCache: this.recentlyProcessed.size,
            currentBatchSize: this.transactionBatch.size,
            solPriceCache: {
                price: this.solPriceCache.price,
                lastUpdated: this.solPriceCache.lastUpdated,
                ageMs: now - this.solPriceCache.lastUpdated
            },
            cacheCleanup: {
                lastProcessedCleanup: this.lastProcessedCleanup,
                lastRecentlyProcessedCleanup: this.lastRecentlyProcessedCleanup,
                timeSinceProcessedCleanup: now - this.lastProcessedCleanup,
                timeSinceRecentlyProcessedCleanup: now - this.lastRecentlyProcessedCleanup,
                nextProcessedCleanupIn: Math.max(0, this.PROCESSED_CLEANUP_INTERVAL - (now - this.lastProcessedCleanup)),
                nextRecentlyProcessedCleanupIn: Math.max(0, this.RECENTLY_PROCESSED_CLEANUP_INTERVAL - (now - this.lastRecentlyProcessedCleanup))
            },
            reconnectAttempts: this.reconnectAttempts,
            isHealthy: this.isStarted && this.streams.length > 0 && this.streams.every(s => s.stream !== null)
        };
    }

    forceCleanupCaches() {
        const before = {
            processedTransactions: this.processedTransactions.size,
            recentlyProcessed: this.recentlyProcessed.size
        };
        
        if (this.processedTransactions.size > 1000) {
            const toDeleteProcessed = Array.from(this.processedTransactions).slice(0, Math.floor(this.processedTransactions.size / 2));
            toDeleteProcessed.forEach(sig => this.processedTransactions.delete(sig));
        } else {
            this.processedTransactions.clear();
        }
        
        if (this.recentlyProcessed.size > 1000) {
            const toDeleteRecent = Array.from(this.recentlyProcessed).slice(0, Math.floor(this.recentlyProcessed.size / 2));
            toDeleteRecent.forEach(key => this.recentlyProcessed.delete(key));
        } else {
            this.recentlyProcessed.clear();
        }
        
        this.lastProcessedCleanup = Date.now();
        this.lastRecentlyProcessedCleanup = Date.now();
        
        const after = {
            processedTransactions: this.processedTransactions.size,
            recentlyProcessed: this.recentlyProcessed.size
        };
        
        console.log(`[${new Date().toISOString()}] 🧹 Force cleanup completed:`, { before, after });
        return { before, after };
    }

    clearCaches() {
        console.log(`[${new Date().toISOString()}] 🧹 Manual cache cleanup initiated`);
        return this.forceCleanupCaches();
    }
}

module.exports = SolanaGrpcService;