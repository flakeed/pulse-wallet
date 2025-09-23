const { default: Client, CommitmentLevel } = require('@triton-one/yellowstone-grpc');
const Database = require('../database/connection');
const { PublicKey } = require('@solana/web3.js');
const bs58 = require('bs58');
const { redis } = require('./tokenService');

class SolanaGrpcService {
    constructor() {
        this.grpcEndpoint = process.env.GRPC_ENDPOINT || 'http://45.134.108.254:10000';
        this.client = null;
        this.stream = null;
        this.db = new Database();
        this.isStarted = false;
        this.reconnectAttempts = 0;
        this.messageCount = 0;
        this.activeGroupId = null;

        this.monitoredWallets = new Set();
        this.walletToGroup = new Map();
        this.walletMetadata = new Map();

        this.processedTransactions = new Set();
        this.recentSignatures = new Set();

        this.BUY_THRESHOLD = parseFloat(process.env.SOL_BUY_THRESHOLD) || 0.001; 
        this.SELL_THRESHOLD = parseFloat(process.env.SOL_SELL_THRESHOLD) || 0.001; 

        this.transactionQueue = [];
        this.processingTimer = null;
        this.BATCH_SIZE = 50; 
        this.BATCH_TIMEOUT = 10; 

        this.solPrice = 150;
        this.solPriceLastUpdate = 0;

        this.stats = {
            received: 0,
            filtered: 0,
            processed: 0,
            errors: 0
        };

        setInterval(() => this.cleanupCaches(), 300000);

        console.log(`[${new Date().toISOString()}] 🚀 Real-Time Service initialized`);
    }

    async start(groupId = null) {
        if (this.isStarted && this.activeGroupId === groupId) {
            console.log(`[${new Date().toISOString()}] ℹ️ Service already running for group ${groupId || 'all'}`);
            return;
        }

        console.log(`[${new Date().toISOString()}] 🚀 Starting REAL-TIME stream for group ${groupId || 'all'}`);

        this.isStarted = true;
        this.activeGroupId = groupId;

        try {
            await this.loadMonitoredWallets(groupId);
            await this.updateSolPrice();
            await this.createStream();

            console.log(`[${new Date().toISOString()}] ✅ REAL-TIME service started - monitoring ${this.monitoredWallets.size} wallets`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to start:`, error.message);
            this.isStarted = false;
            throw error;
        }
    }

    async createStream() {
        await this.endStream();

        try {
            this.client = new Client(this.grpcEndpoint, undefined, {
                'grpc.keepalive_time_ms': 30000,
                'grpc.keepalive_timeout_ms': 5000,
                'grpc.max_receive_message_length': 64 * 1024 * 1024, 
                'grpc.max_send_message_length': 16 * 1024 * 1024,
            });

            this.stream = await this.client.subscribe();

            this.stream.on('data', this.handleStreamData.bind(this));
            this.stream.on('error', this.handleError.bind(this));
            this.stream.on('end', this.handleReconnect.bind(this));

            const request = {
                transactions: {
                    "": {
                        vote: false,
                        failed: false,
                        accountInclude: [],
                        accountExclude: [],
                        accountRequired: []
                    }
                },
                commitment: CommitmentLevel.CONFIRMED,
                accounts: {},
                slots: {},
                transactionsStatus: {},
                entry: {},
                blocks: {},
                blocksMeta: {},
                accountsDataSlice: []
            };

            console.log(`[${new Date().toISOString()}] 📡 Subscribing to FULL transaction stream...`);

            await new Promise((resolve, reject) => {
                this.stream.write(request, err => {
                    if (err) reject(err);
                    else {
                        console.log(`[${new Date().toISOString()}] ✅ REAL-TIME stream active`);
                        resolve();
                    }
                });
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Stream creation failed:`, error.message);
            throw error;
        }
    }

    handleStreamData(data) {
        this.stats.received++;

        if (!data.transaction) return;

        const signature = this.extractSignatureFast(data.transaction);
        if (!signature) return;

        if (this.processedTransactions.has(signature)) {
            this.stats.filtered++;
            return;
        }

        if (!this.quickWalletFilter(data.transaction)) {
            this.stats.filtered++;
            return;
        }

        this.transactionQueue.push({
            signature,
            data: data.transaction,
            timestamp: Date.now()
        });

        if (this.transactionQueue.length >= this.BATCH_SIZE) {
            this.processBatchImmediately();
        } else if (!this.processingTimer) {

            this.processingTimer = setTimeout(() => {
                this.processBatchImmediately();
            }, this.BATCH_TIMEOUT);
        }
    }

    extractSignatureFast(transactionData) {
        try {
            const sigObj = transactionData.signature || 
                          (transactionData.signatures && transactionData.signatures[0]);

            if (!sigObj) return null;

            if (typeof sigObj === 'string') return sigObj;

            if (sigObj.type === 'Buffer' && Array.isArray(sigObj.data)) {
                return bs58.encode(Buffer.from(sigObj.data));
            }

            return bs58.encode(Buffer.from(sigObj));
        } catch {
            return null;
        }
    }

    quickWalletFilter(transactionData) {
        try {
            const accountKeys = this.extractAccountKeysFast(transactionData);

            for (const key of accountKeys) {
                if (this.monitoredWallets.has(key)) {

                    if (this.activeGroupId) {
                        const walletGroup = this.walletToGroup.get(key);
                        return walletGroup === this.activeGroupId;
                    }
                    return true;
                }
            }
            return false;
        } catch {
            return false;
        }
    }

    extractAccountKeysFast(transactionData) {
        const keys = [];

        try {
            const accountKeys = transactionData.transaction?.message?.accountKeys || 
                              transactionData.message?.accountKeys || 
                              [];

            for (const key of accountKeys) {
                try {
                    if (typeof key === 'string') {
                        keys.push(key);
                    } else if (key.pubkey) {
                        keys.push(new PublicKey(key.pubkey).toString());
                    } else {
                        keys.push(new PublicKey(key).toString());
                    }
                } catch {
                    continue;
                }
            }
        } catch {

        }

        return keys;
    }

    async processBatchImmediately() {
        if (this.processingTimer) {
            clearTimeout(this.processingTimer);
            this.processingTimer = null;
        }

        if (this.transactionQueue.length === 0) return;

        const batch = this.transactionQueue.splice(0, this.BATCH_SIZE);

        setImmediate(async () => {
            try {
                const promises = batch.map(item => this.processTransactionFast(item));
                const results = await Promise.allSettled(promises);

                const successful = results.filter(r => r.status === 'fulfilled' && r.value).length;
                this.stats.processed += successful;

                if (successful > 0) {
                    console.log(`[${new Date().toISOString()}] ⚡ Processed ${successful}/${batch.length} transactions in real-time`);
                }
            } catch (error) {
                console.error(`[${new Date().toISOString()}] ❌ Batch processing error:`, error.message);
                this.stats.errors++;
            }
        });

        if (this.transactionQueue.length > 0) {
            this.processBatchImmediately();
        }
    }

    async processTransactionFast(item) {
        try {
            const { signature, data } = item;

            this.processedTransactions.add(signature);

            const relevantWallet = this.findRelevantWalletFast(data);
            if (!relevantWallet) return null;

            const analysis = await this.analyzeTransactionFast(data, relevantWallet);
            if (!analysis) return null;

            const savedTx = await this.saveTransactionFast(signature, analysis, relevantWallet);
            if (!savedTx) return null;

            const message = {
                signature,
                walletAddress: relevantWallet.address,
                walletName: relevantWallet.name,
                groupId: relevantWallet.group_id,
                groupName: relevantWallet.group_name,
                transactionType: analysis.type,
                solAmount: analysis.solAmount,
                tokens: analysis.tokens,
                timestamp: new Date().toISOString()
            };

            const pipeline = redis.pipeline();
            pipeline.publish('transactions', JSON.stringify(message));
            if (relevantWallet.group_id) {
                pipeline.publish(`transactions:group:${relevantWallet.group_id}`, JSON.stringify(message));
            }
            await pipeline.exec();

            return true;

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Fast processing error:`, error.message);
            return null;
        }
    }

    findRelevantWalletFast(transactionData) {
        const accountKeys = this.extractAccountKeysFast(transactionData);

        for (const key of accountKeys) {
            const metadata = this.walletMetadata.get(key);
            if (metadata) {
                if (this.activeGroupId && metadata.group_id !== this.activeGroupId) {
                    continue;
                }
                return {
                    address: key,
                    ...metadata
                };
            }
        }
        return null;
    }

    async analyzeTransactionFast(transactionData, wallet) {
        try {
            const meta = transactionData.meta;
            if (!meta || meta.err) return null;

            const walletIndex = this.findWalletIndex(transactionData, wallet.address);
            if (walletIndex === -1) return null;

            const solChange = this.calculateSolChange(meta, walletIndex);
            const tokenChanges = this.analyzeTokenChangesFast(meta, wallet.address);

            if (tokenChanges.length === 0) return null;

            let type, solAmount;

            if (solChange < -this.BUY_THRESHOLD) {
                type = 'buy';
                solAmount = Math.abs(solChange);
            } else if (solChange > this.SELL_THRESHOLD) {
                type = 'sell';
                solAmount = solChange;
            } else {
                return null;
            }

            return {
                type,
                solAmount,
                tokens: tokenChanges.map(tc => ({
                    mint: tc.mint,
                    symbol: tc.symbol,
                    name: tc.name,
                    amount: tc.amount
                }))
            };

        } catch (error) {
            return null;
        }
    }

    findWalletIndex(transactionData, walletAddress) {
        try {
            const accountKeys = transactionData.transaction?.message?.accountKeys || [];
            return accountKeys.findIndex(key => {
                if (typeof key === 'string') return key === walletAddress;
                if (key.pubkey) return new PublicKey(key.pubkey).toString() === walletAddress;
                return new PublicKey(key).toString() === walletAddress;
            });
        } catch {
            return -1;
        }
    }

    calculateSolChange(meta, walletIndex) {
        const preBalance = meta.preBalances?.[walletIndex] || 0;
        const postBalance = meta.postBalances?.[walletIndex] || 0;
        return (postBalance - preBalance) / 1e9;
    }

    analyzeTokenChangesFast(meta, walletAddress) {
        const changes = [];
        const EXCLUDED_MINTS = new Set([
            'So11111111111111111111111111111111111111112', 
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' 
        ]);

        try {
            const preTokens = new Map();
            const postTokens = new Map();

            for (const pre of meta.preTokenBalances || []) {
                if (pre.owner === walletAddress && !EXCLUDED_MINTS.has(pre.mint)) {
                    preTokens.set(pre.mint, {
                        amount: pre.uiTokenAmount.amount,
                        decimals: pre.uiTokenAmount.decimals
                    });
                }
            }

            for (const post of meta.postTokenBalances || []) {
                if (post.owner === walletAddress && !EXCLUDED_MINTS.has(post.mint)) {
                    postTokens.set(post.mint, {
                        amount: post.uiTokenAmount.amount,
                        decimals: post.uiTokenAmount.decimals
                    });
                }
            }

            const allMints = new Set([...preTokens.keys(), ...postTokens.keys()]);

            for (const mint of allMints) {
                const pre = preTokens.get(mint) || { amount: '0', decimals: 6 };
                const post = postTokens.get(mint) || { amount: '0', decimals: 6 };

                const change = Number(post.amount) - Number(pre.amount);

                if (Math.abs(change) > 0) {
                    changes.push({
                        mint,
                        amount: Math.abs(change) / Math.pow(10, post.decimals),
                        symbol: mint.slice(0, 4).toUpperCase(),
                        name: `Token ${mint.slice(0, 8)}...`,
                        decimals: post.decimals
                    });
                }
            }
        } catch (error) {

        }

        return changes;
    }

    async saveTransactionFast(signature, analysis, wallet) {
        try {

            const existing = await this.db.pool.query(
                'SELECT id FROM transactions WHERE signature = $1 LIMIT 1',
                [signature]
            );
            if (existing.rows.length > 0) return null;

            return await this.db.withTransaction(async (client) => {

                const txResult = await client.query(`
                    INSERT INTO transactions (wallet_id, signature, block_time, transaction_type, sol_spent, sol_received)
                    VALUES ($1, $2, NOW(), $3, $4, $5)
                    RETURNING id
                `, [
                    wallet.id,
                    signature,
                    analysis.type,
                    analysis.type === 'buy' ? analysis.solAmount : 0,
                    analysis.type === 'sell' ? analysis.solAmount : 0
                ]);

                const txId = txResult.rows[0].id;

                for (const token of analysis.tokens) {
                    await client.query(`
                        INSERT INTO tokens (mint, symbol, name, decimals) 
                        VALUES ($1, $2, $3, $4) 
                        ON CONFLICT (mint) DO UPDATE SET updated_at = NOW()
                        RETURNING id
                    `, [token.mint, token.symbol, token.name, token.decimals]);

                    await client.query(`
                        INSERT INTO token_operations (transaction_id, token_id, amount, operation_type) 
                        SELECT $1, t.id, $3, $4 FROM tokens t WHERE t.mint = $2
                    `, [txId, token.mint, token.amount, analysis.type]);
                }

                return { id: txId };
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Save error:`, error.message);
            return null;
        }
    }

    async loadMonitoredWallets(groupId = null) {
        const startTime = Date.now();
        console.log(`[${new Date().toISOString()}] 📋 Loading wallets for real-time monitoring...`);

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

            console.log(`[${new Date().toISOString()}] ✅ Loaded ${this.monitoredWallets.size} wallets in ${Date.now() - startTime}ms`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error loading wallets:`, error.message);
            throw error;
        }
    }

    async updateSolPrice() {
        try {
            if (Date.now() - this.solPriceLastUpdate < 60000) return;

            const response = await fetch('https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112', {
                timeout: 3000
            });

            if (response.ok) {
                const data = await response.json();
                if (data.pairs?.[0]?.priceUsd) {
                    this.solPrice = parseFloat(data.pairs[0].priceUsd);
                    this.solPriceLastUpdate = Date.now();
                }
            }
        } catch (error) {

        }
    }

    cleanupCaches() {

        if (this.processedTransactions.size > 50000) {
            const toDelete = Array.from(this.processedTransactions).slice(0, 25000);
            toDelete.forEach(sig => this.processedTransactions.delete(sig));
        }

        if (this.recentSignatures.size > 10000) {
            this.recentSignatures.clear();
        }

        console.log(`[${new Date().toISOString()}] 🧹 Cache cleanup: ${this.processedTransactions.size} signatures`);
    }

    handleError(error) {
        console.error(`[${new Date().toISOString()}] ❌ Stream error:`, error.message);
        this.handleReconnect();
    }

    async handleReconnect() {
        if (!this.isStarted) return;

        this.reconnectAttempts++;
        console.log(`[${new Date().toISOString()}] 🔄 Reconnecting (${this.reconnectAttempts})...`);

        await this.endStream();
        await new Promise(resolve => setTimeout(resolve, 2000));

        try {
            await this.createStream();
            this.reconnectAttempts = 0;
            console.log(`[${new Date().toISOString()}] ✅ Reconnected successfully`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Reconnect failed:`, error.message);
            setTimeout(() => this.handleReconnect(), 5000);
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
                this.client = null;
            }
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Error ending stream:`, error.message);
        }
    }

    async switchGroup(groupId) {
        this.activeGroupId = groupId;
        await this.loadMonitoredWallets(groupId);
        console.log(`[${new Date().toISOString()}] ✅ Switched to group ${groupId || 'all'}`);
        return { success: true, activeGroupId: this.activeGroupId };
    }

    async subscribeToWalletsBatch(walletAddresses) {
        await this.loadMonitoredWallets(this.activeGroupId);
        return { successful: walletAddresses.length, failed: 0, errors: [] };
    }

    async removeAllWallets(groupId = null) {
        await this.loadMonitoredWallets(this.activeGroupId);
        return { success: true, message: 'Wallets updated' };
    }

    getStatus() {
        return {
            isConnected: this.stream !== null,
            isStarted: this.isStarted,
            activeGroupId: this.activeGroupId,
            totalSubscriptions: this.monitoredWallets.size,
            messageCount: this.messageCount,
            reconnectAttempts: this.reconnectAttempts,
            mode: 'optimized_real_time',
            stats: this.stats
        };
    }

    getPerformanceStats() {
        return {
            mode: 'real_time_optimized',
            totalMonitoredWallets: this.monitoredWallets.size,
            messagesReceived: this.stats.received,
            messagesFiltered: this.stats.filtered,
            messagesProcessed: this.stats.processed,
            filterEfficiency: this.stats.received > 0 ? ((this.stats.filtered / this.stats.received) * 100).toFixed(2) : 0,
            avgFilterTimeMs: 0.1, 
            isHealthy: this.isStarted && this.stream !== null,
            queueSize: this.transactionQueue.length,
            cacheSize: this.processedTransactions.size
        };
    }

    async stop() {
        console.log(`[${new Date().toISOString()}] ⏹️ Stopping real-time service`);
        this.isStarted = false;

        if (this.processingTimer) {
            clearTimeout(this.processingTimer);
            this.processingTimer = null;
        }

        await this.endStream();
        console.log(`[${new Date().toISOString()}] ✅ Real-time service stopped`);
    }

    async shutdown() {
        await this.stop();
        this.processedTransactions.clear();
        this.transactionQueue = [];
        this.monitoredWallets.clear();
        this.walletToGroup.clear();
        this.walletMetadata.clear();
        await this.db.close();
        console.log(`[${new Date().toISOString()}] ✅ Real-time service shutdown complete`);
    }
}

module.exports = SolanaGrpcService;