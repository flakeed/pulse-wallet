const { default: Client, CommitmentLevel } = require('@triton-one/yellowstone-grpc');
const WalletMonitoringService = require('./monitoringService');
const Database = require('../database/connection');
const { PublicKey } = require('@solana/web3.js');
const bs58 = require('bs58');

class SolanaGrpcService {
    constructor() {
        this.grpcEndpoint = process.env.GRPC_ENDPOINT || 'http://45.134.108.254:10000';
        this.client = null;
        this.stream = null;
        this.monitoringService = new WalletMonitoringService();
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

        setInterval(() => {
            if (this.processedTransactions.size > 10000) {
                Array.from(this.processedTransactions).slice(0, 5000).forEach(sig => this.processedTransactions.delete(sig));
            }
        }, 300000);
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
            await this.connect();
            await this.subscribeToTransactions();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Failed to start gRPC service: ${error.message}`, error.stack);
            this.isStarted = false;
            throw error;
        }
    }

    async connect() {
        if (this.isConnecting || this.client) return;
        this.isConnecting = true;
        console.log(`[${new Date().toISOString()}] [INFO] Connecting to gRPC endpoint: ${this.grpcEndpoint}`);
        try {
            this.client = new Client(this.grpcEndpoint, undefined, {
                'grpc.keepalive_time_ms': 30000,
                'grpc.keepalive_timeout_ms': 5000,
                'grpc.keepalive_permit_without_calls': true,
                'grpc.http2.max_pings_without_data': 0,
                'grpc.http2.min_time_between_pings_ms': 10000,
                'grpc.http2.min_ping_interval_without_data_ms': 300000
            });
            this.reconnectAttempts = 0;
            console.log(`[${new Date().toISOString()}] [INFO] gRPC client connected successfully`);
            this.isConnecting = false;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Failed to connect to gRPC: ${error.message}`, error.stack);
            this.isConnecting = false;
            throw error;
        }
    }

    async subscribeToTransactions() {
        console.log(`[${new Date().toISOString()}] [INFO] Fetching active wallets for group ${this.activeGroupId || 'all'}`);
        const wallets = await this.db.getActiveWallets(this.activeGroupId);
        this.monitoredWallets.clear();
        wallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
        console.log(`[${new Date().toISOString()}] [INFO] Monitoring ${this.monitoredWallets.size} wallets`);

        if (this.monitoredWallets.size === 0) {
            console.warn(`[${new Date().toISOString()}] [WARN] No wallets to monitor, skipping subscription`);
            return;
        }

        if (this.stream) {
            console.log(`[${new Date().toISOString()}] [INFO] Ending existing stream before new subscription`);
            this.stream.end();
        }

        const request = {
            accounts: {},
            slots: {},
            transactions: { client: { vote: false, failed: false, accountInclude: Array.from(this.monitoredWallets), accountExclude: [], accountRequired: [] } },
            transactionsStatus: {},
            entry: {},
            blocks: {},
            blocksMeta: {},
            commitment: CommitmentLevel.CONFIRMED,
            accountsDataSlice: []
        };

        try {
            console.log(`[${new Date().toISOString()}] [INFO] Creating gRPC stream`);
            this.stream = await this.client.subscribe();
            this.stream.on('data', data => {
                this.messageCount++;
                this.handleGrpcMessage(data);
            });
            this.stream.on('error', error => {
                console.error(`[${new Date().toISOString()}] [ERROR] gRPC stream error: ${error.message}`, error.stack);
                if (error.message.includes('serialization failure')) {
                    console.error(`[${new Date().toISOString()}] [CRITICAL] Serialization failure, stopping service`);
                    this.isStarted = false;
                    return;
                }
                this.handleReconnect();
            });
            this.stream.on('end', () => {
                console.log(`[${new Date().toISOString()}] [INFO] gRPC stream ended`);
                if (this.isStarted) setTimeout(() => this.handleReconnect(), 2000);
            });

            console.log(`[${new Date().toISOString()}] [INFO] Sending subscription request`);
            await new Promise((resolve, reject) => this.stream.write(request, err => {
                if (err) {
                    console.error(`[${new Date().toISOString()}] [ERROR] Subscription request failed: ${err.message}`, err.stack);
                    reject(err);
                } else {
                    console.log(`[${new Date().toISOString()}] [INFO] Subscription request sent successfully`);
                    resolve();
                }
            }));
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Error in subscribeToTransactions: ${error.message}`, error.stack);
            throw error;
        }
    }

    handleGrpcMessage(data) {
        try {
            if (!data.transaction) {
                console.warn(`[${new Date().toISOString()}] [WARN] Received gRPC message without transaction data`);
                return;
            }
            const signature = this.extractSignature(data.transaction);
            if (signature) {
                console.log(`[${new Date().toISOString()}] [INFO] Transaction received: signature=${signature}`);
            }
            this.processTransaction(data.transaction);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Error handling gRPC message: ${error.message}`, error.stack);
        }
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
            console.warn(`[${new Date().toISOString()}] [WARN] Unknown transaction format, skipping`);
            return;
        }

        if (!transaction || !meta || meta.err) {
            return; 
        }

        const signature = this.extractSignature(transactionData);
        if (!signature || this.processedTransactions.has(signature)) {
            return;
        }
        this.processedTransactions.add(signature);

        let accountKeys = transaction.message?.accountKeys || transaction.accountKeys || [];
        if (meta.loadedWritableAddresses) accountKeys = accountKeys.concat(meta.loadedWritableAddresses);
        if (meta.loadedReadonlyAddresses) accountKeys = accountKeys.concat(meta.loadedReadonlyAddresses);

        const stringAccountKeys = this.convertAccountKeysToStrings(accountKeys);
        const involvedWallet = Array.from(this.monitoredWallets).find(wallet => 
            stringAccountKeys.includes(wallet)
        );

        if (!involvedWallet) return;

        const wallet = await this.db.getWalletByAddress(involvedWallet);
        if (!wallet || (this.activeGroupId && wallet.group_id !== this.activeGroupId)) {
            return;
        }

        const blockTime = Number(transactionData.blockTime) || Math.floor(Date.now() / 1000);

        await this.processTransactionFromGrpcData({
            signature,
            transaction,
            meta,
            blockTime,
            wallet,
            accountKeys: stringAccountKeys
        });

    } catch (error) {
        console.error(`[${new Date().toISOString()}] [ERROR] Error processing transaction:`, error.message);
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
            console.error(`[${new Date().toISOString()}] [ERROR] Error converting account key:`, error.message);
        }
    }
    
    return stringAccountKeys;
}

async processTransactionFromGrpcData({ signature, transaction, meta, blockTime, wallet, accountKeys }) {
    try {
        const existingTx = await this.db.pool.query(
            'SELECT id FROM transactions WHERE signature = $1 AND wallet_id = $2',
            [signature, wallet.id]
        );
        if (existingTx.rows.length > 0) {
            return null;
        }

        const walletIndex = accountKeys.indexOf(wallet.address);
        if (walletIndex === -1) return null;

        const preBalance = meta.preBalances[walletIndex] || 0;
        const postBalance = meta.postBalances[walletIndex] || 0;
        const solChange = (postBalance - preBalance) / 1e9;

        const solPrice = await this.monitoringService.fetchSolPrice();

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

            console.log(`[${new Date().toISOString()}] ✅ Processed transaction ${signature} without RPC calls`);
        }

    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error processing gRPC transaction:`, error.message);
        throw error;
    }
}

async analyzeTransactionFromGrpc({ meta, solChange, walletAddress, solPrice }) {
    const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
    const WRAPPED_SOL_MINT = 'So11111111111111111111111111111111111111112';
    
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
    } else if (solChange < -0.01) {
        transactionType = 'buy';
        totalSolAmount = Math.abs(solChange);
    } else if (solChange > 0.001) {
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

extractSignature(transactionData) {
    try {
        const sigObj = transactionData.signature || 
                      (transactionData.signatures && transactionData.signatures[0]) ||
                      transactionData.transaction?.signature || 
                      (transactionData.transaction?.signatures && transactionData.transaction.signatures[0]) ||
                      transactionData.tx?.signature || 
                      (transactionData.tx?.signatures && transactionData.tx.signatures[0]);
        
        if (!sigObj) {
            console.warn(`[${new Date().toISOString()}] [WARN] No signature found in transaction data`);
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
            console.warn(`[${new Date().toISOString()}] [WARN] Unexpected signature format: ${typeof sigObj}`);
            signature = bs58.encode(Buffer.from(sigObj));
        }

        if (signature.length < 80 || signature.length > 88) {
            console.warn(`[${new Date().toISOString()}] [WARN] Invalid signature length: ${signature.length}`);
            return null;
        }

        return signature;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] [ERROR] Error extracting signature: ${error.message}`, error.stack);
        return null;
    }
}

    convertGrpcToLegacyFormat(grpcData, accountKeys) {
        try {
            const { transaction, meta, slot, blockTime } = grpcData;
            const toBase58 = bufObj => {
                if (typeof bufObj === 'string') return bufObj;
                const buffer = bufObj.type === 'Buffer' && Array.isArray(bufObj.data) ? Buffer.from(bufObj.data) : Buffer.isBuffer(bufObj) ? bufObj : null;
                return buffer ? new PublicKey(buffer).toString() : bufObj.toString();
            };

            return {
                transaction: {
                    message: { accountKeys, instructions: transaction.message.instructions || [] },
                    signatures: transaction.signatures ? transaction.signatures.map(sig => 
                        sig.type === 'Buffer' && Array.isArray(sig.data) ? Buffer.from(sig.data).toString('base64') : sig.toString()
                    ) : []
                },
                meta: {
                    err: meta.err,
                    fee: Number(meta.fee || 0),
                    preBalances: meta.preBalances || [],
                    postBalances: meta.postBalances || [],
                    preTokenBalances: this.convertTokenBalances(meta.preTokenBalances || [], toBase58),
                    postTokenBalances: this.convertTokenBalances(meta.postTokenBalances || [], toBase58),
                    logMessages: meta.logMessages || [],
                    innerInstructions: meta.innerInstructions || []
                },
                slot,
                blockTime: Number(blockTime)
            };
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Error converting gRPC to legacy format: ${error.message}`, error.stack);
            return null;
        }
    }

    convertTokenBalances(grpcTokenBalances, toBase58) {
        return grpcTokenBalances.map(balance => {
            try {
                return {
                    accountIndex: balance.accountIndex || 0,
                    mint: toBase58(balance.mint || ''),
                    owner: toBase58(balance.owner || ''),
                    programId: toBase58(balance.programId || ''),
                    uiTokenAmount: {
                        amount: balance.uiTokenAmount?.amount || '0',
                        decimals: balance.uiTokenAmount?.decimals || 0,
                        uiAmount: balance.uiTokenAmount?.uiAmount || 0,
                        uiAmountString: balance.uiTokenAmount?.uiAmountString || '0'
                    }
                };
            } catch (error) {
                console.error(`[${new Date().toISOString()}] [ERROR] Error converting token balance: ${error.message}`, error.stack);
                return null;
            }
        }).filter(Boolean);
    }

    async subscribeToWalletsBatch(walletAddresses) {
        const addedCount = walletAddresses.filter(address => !this.monitoredWallets.has(address)).length;
        walletAddresses.forEach(address => this.monitoredWallets.add(address));
        console.log(`[${new Date().toISOString()}] [INFO] Added ${addedCount} wallets to monitoring, total: ${this.monitoredWallets.size}`);
        return { successful: walletAddresses.length, failed: 0, errors: [] };
    }

    async unsubscribeFromWalletsBatch(walletAddresses) {
        const removedCount = walletAddresses.filter(address => this.monitoredWallets.has(address)).length;
        walletAddresses.forEach(address => this.monitoredWallets.delete(address));
        console.log(`[${new Date().toISOString()}] [INFO] Removed ${removedCount} wallets from monitoring, total: ${this.monitoredWallets.size}`);
        return { successful: walletAddresses.length, failed: 0, errors: [] };
    }

    async subscribeToWallet(walletAddress) {
        this.monitoredWallets.add(walletAddress);
        console.log(`[${new Date().toISOString()}] [INFO] Added wallet ${walletAddress} to monitoring`);
        return { success: true };
    }

    async unsubscribeFromWallet(walletAddress) {
        this.monitoredWallets.delete(walletAddress);
        console.log(`[${new Date().toISOString()}] [INFO] Removed wallet ${walletAddress} from monitoring`);
    }

    async removeAllWallets(groupId = null) {
        console.log(`[${new Date().toISOString()}] [INFO] Removing all wallets for group ${groupId || 'all'}`);
        const walletsToRemove = await this.db.getActiveWallets(groupId);
        walletsToRemove.forEach(wallet => this.monitoredWallets.delete(wallet.address));
        await this.monitoringService.removeAllWallets(groupId);
        if (this.isStarted && (!groupId || groupId === this.activeGroupId)) {
            const remainingWallets = await this.db.getActiveWallets(this.activeGroupId);
            this.monitoredWallets.clear();
            remainingWallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
            console.log(`[${new Date().toISOString()}] [INFO] Reloaded monitoring: ${this.monitoredWallets.size} wallets remaining`);
        }
        return {
            success: true,
            message: `Removed ${walletsToRemove.length} wallets`,
            details: { walletsRemoved: walletsToRemove.length, remainingWallets: this.monitoredWallets.size, groupId }
        };
    }

    async switchGroup(groupId) {
        console.log(`[${new Date().toISOString()}] [INFO] Switching to group ${groupId || 'all'}`);
        this.activeGroupId = groupId;
        const wallets = await this.db.getActiveWallets(groupId);
        this.monitoredWallets.clear();
        wallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
        console.log(`[${new Date().toISOString()}] [INFO] Now monitoring ${this.monitoredWallets.size} wallets in group ${groupId || 'all'}`);
        if (this.isStarted) await this.subscribeToTransactions();
    }

    async handleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] [CRITICAL] Max reconnect attempts reached, stopping service`);
            this.isStarted = false;
            return;
        }
        this.reconnectAttempts++;
        console.log(`[${new Date().toISOString()}] [INFO] Reconnecting gRPC (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
        if (this.stream) this.stream.end();
        if (this.client) {
            if (typeof this.client.close === 'function') this.client.close();
            else if (typeof this.client.destroy === 'function') this.client.destroy();
            else if (typeof this.client.end === 'function') this.client.end();
            this.client = null;
        }
        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));
        try {
            this.isConnecting = false;
            await this.connect();
            await this.subscribeToTransactions();
            console.log(`[${new Date().toISOString()}] [INFO] Reconnection successful`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Reconnect failed: ${error.message}`, error.stack);
            await this.handleReconnect();
        }
    }

    getStatus() {
        return {
            isConnected: this.client !== null && this.stream !== null,
            isStarted: this.isStarted,
            activeGroupId: this.activeGroupId,
            subscriptions: this.monitoredWallets.size,
            messageCount: this.messageCount,
            reconnectAttempts: this.reconnectAttempts,
            grpcEndpoint: this.grpcEndpoint,
            mode: 'grpc'
        };
    }

    async stop() {
        console.log(`[${new Date().toISOString()}] [INFO] Stopping gRPC service`);
        this.isStarted = false;
        if (this.stream) this.stream.end();
        if (this.client) {
            if (typeof this.client.close === 'function') this.client.close();
            else if (typeof this.client.destroy === 'function') this.client.destroy();
            else if (typeof this.client.end === 'function') this.client.end();
            this.client = null;
        }
        this.monitoredWallets.clear();
    }

    async shutdown() {
        await this.stop();
        await this.db.close().catch(error => console.error(`[${new Date().toISOString()}] [ERROR] Error closing DB: ${error.message}`));
        console.log(`[${new Date().toISOString()}] [INFO] Shutdown complete`);
    }
}

module.exports = SolanaGrpcService;