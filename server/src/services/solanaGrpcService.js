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
        this.streams = [];
        this.db = new Database();
        this.isStarted = false;
        this.isConnecting = false;
        this.reconnectInterval = 5000;
        this.maxReconnectAttempts = 10;
        this.reconnectAttempts = 0;
        this.messageCount = 0;
        this.activeGroupId = null;
        this.monitoredWallets = new Set();
        this.processedTransactions = new Set();
        this.recentlyProcessed = new Set();
        this.solPriceCache = {
            price: 150,
            lastUpdated: 0,
            cacheTimeout: 60000
        };
        this.transactionBatch = new Map();
        this.batchTimer = null;
        this.batchSize = 25;
        this.batchTimeout = 100;
        this.BUY_THRESHOLD = parseFloat(process.env.SOL_BUY_THRESHOLD) || 0.01;
        this.SELL_THRESHOLD = parseFloat(process.env.SOL_SELL_THRESHOLD) || 0.001;
        this.PROCESSED_CLEANUP_INTERVAL = 24 * 60 * 60 * 1000;
        this.RECENTLY_PROCESSED_CLEANUP_INTERVAL = 60 * 60 * 1000;
        this.lastProcessedCleanup = Date.now();
        this.lastRecentlyProcessedCleanup = Date.now();
        this.chunkSize = parseInt(process.env.GRPC_CHUNK_SIZE) || 2000;
        this.maxChunks = parseInt(process.env.GRPC_MAX_CHUNKS) || 10;
        this.setupCacheCleanup();
    }

    setupCacheCleanup() {
        setInterval(() => {
            const now = Date.now();

            if (now - this.lastProcessedCleanup >= this.PROCESSED_CLEANUP_INTERVAL) {
                if (this.processedTransactions.size > 50000) {
                    const toDelete = Array.from(this.processedTransactions).slice(0, 25000);
                    toDelete.forEach(sig => this.processedTransactions.delete(sig));
                }
                this.lastProcessedCleanup = now;
            }

            if (now - this.lastRecentlyProcessedCleanup >= this.RECENTLY_PROCESSED_CLEANUP_INTERVAL) {
                if (this.recentlyProcessed.size > 5000) {
                    const toDelete = Array.from(this.recentlyProcessed).slice(0, 2500);
                    toDelete.forEach(key => this.recentlyProcessed.delete(key));
                }
                this.lastRecentlyProcessedCleanup = now;
            }
        }, 300000);
    }

    chunkArray(array, chunkSize) {
        const chunks = [];
        for (let i = 0; i < array.length; i += chunkSize) {
            chunks.push(array.slice(i, i + chunkSize));
        }
        return chunks;
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
        }

        return this.solPriceCache.price;
    }

    async start(groupId = null) {
        if (this.isStarted) {
            return;
        }
        console.log(`[${new Date().toISOString()}] [INFO] Starting chunked gRPC service`);
        this.isStarted = true;
        this.activeGroupId = groupId;
        try {
            await this.connect();
            await this.subscribeToAllWallets();
            console.log(`[${new Date().toISOString()}] 🚀 Chunked gRPC service started successfully`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Failed to start chunked gRPC: ${error.message}`);
            this.isStarted = false;
            throw error;
        }
    }

    async connect() {
        if (this.isConnecting || this.client) return;
        this.isConnecting = true;
        try {
            this.client = new Client(this.grpcEndpoint, undefined, {
                'grpc.keepalive_time_ms': 30000,
                'grpc.keepalive_timeout_ms': 5000,
                'grpc.keepalive_permit_without_calls': true,
                'grpc.http2.max_pings_without_data': 0,
                'grpc.http2.min_time_between_pings_ms': 10000,
                'grpc.http2.min_ping_interval_without_data_ms': 300000,
                'grpc.max_receive_message_length': 32 * 1024 * 1024,
                'grpc.max_send_message_length': 32 * 1024 * 1024,
                'grpc.initial_reconnect_backoff_ms': 1000,
                'grpc.max_reconnect_backoff_ms': 30000
            });
            this.reconnectAttempts = 0;
            this.isConnecting = false;
            console.log(`[${new Date().toISOString()}] [INFO] gRPC client connected successfully`);
        } catch (error) {
            this.isConnecting = false;
            throw error;
        }
    }

    async subscribeToAllWallets() {
        console.log(`[${new Date().toISOString()}] [INFO] Fetching all active wallets`);
        const wallets = await this.db.getActiveWallets(null);
        this.monitoredWallets.clear();
        wallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
        
        console.log(`[${new Date().toISOString()}] 📊 Found ${this.monitoredWallets.size} active wallets globally`);

        if (this.monitoredWallets.size === 0) {
            console.warn(`[${new Date().toISOString()}] [WARN] No wallets to monitor, skipping subscription`);
            return;
        }

        const MAX_WALLETS = parseInt(process.env.MAX_MONITORED_WALLETS) || 20000;
        if (this.monitoredWallets.size > MAX_WALLETS) {
            console.warn(`[${new Date().toISOString()}] [WARN] Wallet count (${this.monitoredWallets.size}) exceeds limit (${MAX_WALLETS}), sampling...`);
            const sampledWallets = Array.from(this.monitoredWallets)
                .sort(() => 0.5 - Math.random())
                .slice(0, MAX_WALLETS);
            this.monitoredWallets.clear();
            sampledWallets.forEach(wallet => this.monitoredWallets.add(wallet));
            console.log(`[${new Date().toISOString()}] [INFO] Sampled down to ${this.monitoredWallets.size} wallets`);
        }

        const walletChunks = this.chunkArray(Array.from(this.monitoredWallets), this.chunkSize);
        const limitedChunks = walletChunks.slice(0, this.maxChunks);
        
        console.log(`[${new Date().toISOString()}] [INFO] Creating ${limitedChunks.length} streams for ${this.monitoredWallets.size} wallets (chunk size: ${this.chunkSize})`);

        this.streams = [];
        
        for (let i = 0; i < limitedChunks.length; i++) {
            const chunk = limitedChunks[i];
            try {
                await this.createStreamForChunk(chunk, i);
                if (i < limitedChunks.length - 1) {
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            } catch (error) {
                console.error(`[${new Date().toISOString()}] [ERROR] Failed to create stream ${i}: ${error.message}`);
            }
        }

        console.log(`[${new Date().toISOString()}] [INFO] All streams created successfully: ${this.streams.length} active`);
    }

    async createStreamForChunk(walletChunk, chunkIndex) {
        const request = {
            accounts: {},
            slots: {},
            transactions: {
                client: {
                    vote: false,
                    failed: false,
                    accountInclude: walletChunk,
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

        try {
            const stream = await this.client.subscribe();
            
            stream.on('data', data => {
                this.messageCount++;
                this.handleGrpcMessageBatched(data, chunkIndex);
            });
            
            stream.on('error', error => {
                console.error(`[${new Date().toISOString()}] [ERROR] Stream ${chunkIndex} error: ${error.code} - ${error.message}`);
                this.handleStreamReconnect(chunkIndex);
            });
            
            stream.on('end', () => {
                console.log(`[${new Date().toISOString()}] [INFO] Stream ${chunkIndex} ended`);
                if (this.isStarted) setTimeout(() => this.handleStreamReconnect(chunkIndex), 2000);
            });

            await new Promise((resolve, reject) => {
                stream.write(request, err => {
                    if (err) {
                        console.error(`[${new Date().toISOString()}] [ERROR] Stream ${chunkIndex} subscription failed: ${err.message}`);
                        reject(err);
                    } else {
                        console.log(`[${new Date().toISOString()}] [INFO] Stream ${chunkIndex} subscription successful (${walletChunk.length} wallets)`);
                        resolve();
                    }
                });
            });

            this.streams[chunkIndex] = { stream, wallets: walletChunk, active: true };
            console.log(`[${new Date().toISOString()}] [INFO] Stream ${chunkIndex} active, monitoring ${walletChunk.length} wallets`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Failed to create stream ${chunkIndex}: ${error.message}`);
            throw error;
        }
    }

    handleGrpcMessageBatched(data, chunkIndex) {
        try {
            let transactionData = null;
            if (data.transaction) {
                transactionData = data;
            } else if (data.transactionStatus) {
                transactionData = data;
            }

            if (!transactionData) {
                return;
            }

            const signature = this.extractSignature(transactionData);
            if (!signature) {
                return;
            }

            this.transactionBatch.set(`${chunkIndex}:${signature}`, transactionData);

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
        }
    }

    async processBatch() {
        if (this.transactionBatch.size === 0) return;

        const batch = new Map(this.transactionBatch);
        this.transactionBatch.clear();
        this.batchTimer = null;

        console.log(`[${new Date().toISOString()}] [INFO] Processing batch of ${batch.size} transactions`);

        const promises = Array.from(batch.entries()).map(([key, data]) => {
            const [chunkIndex, signature] = key.split(':');
            return this.processTransaction(data).catch(error => {
                return null;
            });
        });

        const results = await Promise.allSettled(promises);
        const successful = results.filter(r => r.status === 'fulfilled' && r.value !== null).length;

        console.log(`[${new Date().toISOString()}] [INFO] Batch processed: ${successful}/${batch.size} successful`);
    }

    async processTransaction(transactionData) {
        try {
            let transaction = null, meta = null;

            if (transactionData.transaction) {
                transaction = transactionData.transaction;
                meta = transactionData.transaction.meta || transactionData.meta;
            } else if (transactionData.transactionStatus) {
                transaction = transactionData.transactionStatus;
                meta = transactionData.transactionStatus.meta || transactionData.meta;
            } else {
                transaction = transactionData.transaction?.transaction || transactionData;
                meta = transactionData.transaction?.meta || transactionData.meta || transactionData;
            }

            if (!transaction || !meta) {
                return null;
            }

            if (meta.err) {
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

            let accountKeys = [];
            
            if (transaction.message?.accountKeys) {
                accountKeys = transaction.message.accountKeys;
            } else if (transaction.accountKeys) {
                accountKeys = transaction.accountKeys;
            } else if (meta.accountKeys) {
                accountKeys = meta.accountKeys;
            }

            if (meta.loadedAddresses?.writable) {
                accountKeys = accountKeys.concat(meta.loadedAddresses.writable);
            }
            if (meta.loadedAddresses?.readonly) {
                accountKeys = accountKeys.concat(meta.loadedAddresses.readonly);
            }

            const stringAccountKeys = this.convertAccountKeysToStrings(accountKeys);
            const involvedWallet = Array.from(this.monitoredWallets).find(wallet => stringAccountKeys.includes(wallet));
            if (!involvedWallet) return null;

            let groupWallets = new Set();
            if (this.activeGroupId) {
                groupWallets = await this.getGroupWallets(this.activeGroupId);
                if (!groupWallets.has(involvedWallet)) {
                    return null;
                }
            }

            const walletCacheKey = `wallet:${involvedWallet}`;
            let wallet = null;

            try {
                const cachedWallet = await redis.get(walletCacheKey);
                if (cachedWallet) {
                    wallet = JSON.parse(cachedWallet);
                } else {
                    wallet = await this.db.getWalletByAddress(involvedWallet);
                    if (wallet) {
                        await redis.setex(walletCacheKey, 300, JSON.stringify(wallet));
                    }
                }
            } catch (error) {
                wallet = await this.db.getWalletByAddress(involvedWallet);
            }

            if (!wallet) return null;

            const blockTime = Number(transactionData.blockTime) || Number(meta.blockTime) || Math.floor(Date.now() / 1000);

            return await this.processTransactionFromGrpcData({
                signature,
                transaction,
                meta,
                blockTime,
                wallet,
                accountKeys: stringAccountKeys
            });

        } catch (error) {
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
            let sigObj = null;

            if (transactionData.signature) {
                sigObj = transactionData.signature;
            } else if (transactionData.transaction?.signature) {
                sigObj = transactionData.transaction.signature;
            } else if (transactionData.transactionStatus?.signature) {
                sigObj = transactionData.transactionStatus.signature;
            } else if (transactionData.transaction?.signatures?.[0]) {
                sigObj = transactionData.transaction.signatures[0];
            } else if (transactionData.signatures?.[0]) {
                sigObj = transactionData.signatures[0];
            }

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
            } else if (typeof sigObj === 'object' && sigObj.data) {
                signature = bs58.encode(Buffer.from(sigObj.data));
            } else {
                try {
                    signature = bs58.encode(Buffer.from(sigObj));
                } catch (e) {
                    return null;
                }
            }

            if (signature.length < 80 || signature.length > 88) {
                return null;
            }

            return signature;
        } catch (error) {
            return null;
        }
    }

    async handleStreamReconnect(chunkIndex) {
        if (!this.isStarted) return;

        console.log(`[${new Date().toISOString()}] [INFO] Reconnecting stream ${chunkIndex}`);
        
        try {
            if (this.streams[chunkIndex]) {
                this.streams[chunkIndex].stream?.end();
                this.streams[chunkIndex].active = false;
            }

            await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));

            const walletChunk = this.streams[chunkIndex]?.wallets || [];
            await this.createStreamForChunk(walletChunk, chunkIndex);
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Failed to reconnect stream ${chunkIndex}: ${error.message}`);
            if (this.reconnectAttempts < this.maxReconnectAttempts) {
                setTimeout(() => this.handleStreamReconnect(chunkIndex), this.reconnectInterval * 2);
            }
        }
    }

    async subscribeToWalletsBatch(walletAddresses, batchSize = 1000) {
        const startTime = Date.now();
        let successful = 0;
        let failed = 0;
        const errors = [];

        for (let i = 0; i < walletAddresses.length; i += batchSize) {
            const batch = walletAddresses.slice(i, i + batchSize);

            try {
                batch.forEach(address => {
                    if (!this.monitoredWallets.has(address)) {
                        this.monitoredWallets.add(address);
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

        return { successful, failed, errors, totalMonitored: this.monitoredWallets.size };
    }

    async unsubscribeFromWalletsBatch(walletAddresses, batchSize = 1000) {
        const startTime = Date.now();
        let successful = 0;

        for (let i = 0; i < walletAddresses.length; i += batchSize) {
            const batch = walletAddresses.slice(i, i + batchSize);

            batch.forEach(address => {
                if (this.monitoredWallets.has(address)) {
                    this.monitoredWallets.delete(address);
                    successful++;
                }
            });
        }

        const duration = Date.now() - startTime;

        return { successful, failed: 0, errors: [], totalMonitored: this.monitoredWallets.size };
    }

    async subscribeToWallet(walletAddress) {
        if (!this.monitoredWallets.has(walletAddress)) {
            this.monitoredWallets.add(walletAddress);
        }
        return { success: true, totalMonitored: this.monitoredWallets.size };
    }

    async unsubscribeFromWallet(walletAddress) {
        if (this.monitoredWallets.has(walletAddress)) {
            this.monitoredWallets.delete(walletAddress);
        }
        return { success: true, totalMonitored: this.monitoredWallets.size };
    }

    async removeAllWallets(groupId = null) {
        try {
            const walletsToRemove = await this.db.getActiveWallets(groupId);
            const addressesToRemove = walletsToRemove.map(w => w.address);

            addressesToRemove.forEach(address => this.monitoredWallets.delete(address));

            if (addressesToRemove.length > 0) {
                const pipeline = redis.pipeline();
                addressesToRemove.forEach(address => {
                    pipeline.del(`wallet:${address}`);
                });
                await pipeline.exec();
            }

            if (!groupId || groupId === this.activeGroupId) {
                const remainingWallets = await this.db.getActiveWallets(this.activeGroupId);
                this.monitoredWallets.clear();
                remainingWallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
                await this.subscribeToAllWallets();
            }

            return {
                success: true,
                message: `Removed ${addressesToRemove.length} wallets`,
                details: {
                    walletsRemoved: addressesToRemove.length,
                    remainingWallets: this.monitoredWallets.size,
                    groupId
                }
            };

        } catch (error) {
            throw error;
        }
    }

    async switchGroup(groupId) {
        try {
            this.activeGroupId = groupId;
            await this.stop();
            await this.start(groupId);
            return {
                success: true,
                activeGroupId: this.activeGroupId,
                monitoredWallets: this.monitoredWallets.size
            };
        } catch (error) {
            throw error;
        }
    }

    async getGroupWallets(groupId) {
        const cacheKey = `group_wallets:${groupId || 'all'}`;
        let groupWallets = new Set();

        try {
            const cachedWallets = await redis.get(cacheKey);
            if (cachedWallets) {
                groupWallets = new Set(JSON.parse(cachedWallets));
            } else {
                const wallets = await this.db.getActiveWallets(groupId);
                groupWallets = new Set(wallets.map(w => w.address));
                await redis.setex(cacheKey, 300, JSON.stringify(Array.from(groupWallets)));
            }
        } catch (error) {
            const wallets = await this.db.getActiveWallets(groupId);
            groupWallets = new Set(wallets.map(w => w.address));
        }

        return groupWallets;
    }

    async handleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] [CRITICAL] Max reconnect attempts reached, stopping service`);
            this.isStarted = false;
            return;
        }

        this.reconnectAttempts++;
        console.log(`[${new Date().toISOString()}] [INFO] Reconnecting gRPC service (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

        await this.stop();
        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));

        try {
            await this.connect();
            await this.subscribeToAllWallets();
            console.log(`[${new Date().toISOString()}] [INFO] Service reconnection successful`);
            this.reconnectAttempts = 0;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Reconnect failed: ${error.message}`);
            this.reconnectInterval = Math.min(this.reconnectInterval * 1.5, 30000);
            await this.handleReconnect();
        }
    }

    getStatus() {
        const activeStreams = this.streams.filter(s => s && s.active).length;
        return {
            isConnected: this.client !== null && activeStreams > 0,
            isStarted: this.isStarted,
            activeGroupId: this.activeGroupId,
            subscriptions: this.monitoredWallets.size,
            activeStreams,
            totalStreams: this.streams.length,
            messageCount: this.messageCount,
            reconnectAttempts: this.reconnectAttempts,
            grpcEndpoint: this.grpcEndpoint,
            mode: 'chunked_grpc',
            performance: {
                processedTransactions: this.processedTransactions.size,
                recentlyProcessed: this.recentlyProcessed.size,
                batchSize: this.batchSize,
                batchTimeout: this.batchTimeout,
                chunkSize: this.chunkSize,
                maxChunks: this.maxChunks,
                solPriceCached: this.solPriceCache.lastUpdated > 0,
                cacheStats: {
                    solPriceAge: Date.now() - this.solPriceCache.lastUpdated,
                    solPrice: this.solPriceCache.price
                }
            }
        };
    }

    async stop() {
        console.log(`[${new Date().toISOString()}] [INFO] Stopping chunked gRPC service`);
        this.isStarted = false;

        if (this.batchTimer) {
            clearTimeout(this.batchTimer);
            this.batchTimer = null;
        }

        if (this.transactionBatch.size > 0) {
            await this.processBatch();
        }

        for (let i = 0; i < this.streams.length; i++) {
            try {
                if (this.streams[i]?.stream) {
                    this.streams[i].stream.end();
                }
                this.streams[i] = null;
            } catch (error) {
            }
        }

        this.streams = [];
        this.monitoredWallets.clear();
        console.log(`[${new Date().toISOString()}] [INFO] Chunked gRPC service stopped`);
    }

    async shutdown() {
        console.log(`[${new Date().toISOString()}] [INFO] Shutting down chunked gRPC service`);
        await this.stop();

        this.processedTransactions.clear();
        this.recentlyProcessed.clear();
        this.transactionBatch.clear();

        try {
            if (this.client) {
                if (typeof this.client.close === 'function') this.client.close();
                else if (typeof this.client.destroy === 'function') this.client.destroy();
            }
            await this.db.close();
        } catch (error) {
        }

        console.log(`[${new Date().toISOString()}] [INFO] Chunked gRPC service shutdown complete`);
    }

    getPerformanceStats() {
        const now = Date.now();
        const activeStreams = this.streams.filter(s => s && s.active).length;
        return {
            monitoredWallets: this.monitoredWallets.size,
            messagesProcessed: this.messageCount,
            activeStreams,
            totalStreams: this.streams.length,
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
            isHealthy: this.isStarted && this.client !== null && activeStreams > 0
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

        return { before, after };
    }

    clearCaches() {
        return this.forceCleanupCaches();
    }
}

module.exports = SolanaGrpcService;