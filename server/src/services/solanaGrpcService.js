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

    const chunkSize = 1000; 
    const walletArray = Array.from(this.monitoredWallets);
    const chunks = [];
    for (let i = 0; i < walletArray.length; i += chunkSize) {
        chunks.push(walletArray.slice(i, i + chunkSize));
    }
    console.log(`[${new Date().toISOString()}] [INFO] Split ${walletArray.length} wallets into ${chunks.length} chunks`);

    if (this.streams) {
        this.streams.forEach(stream => stream.end());
    }
    if (this.clients) {
        this.clients.forEach(client => client.close?.());
    }
    this.streams = [];
    this.clients = [];

    for (let idx = 0; idx < chunks.length; idx++) {
        const client = new Client(this.grpcEndpoint, undefined, {
            'grpc.keepalive_time_ms': 30000,
            'grpc.keepalive_timeout_ms': 5000,
            'grpc.keepalive_permit_without_calls': true,
            'grpc.http2.max_pings_without_data': 0,
            'grpc.http2.min_time_between_pings_ms': 10000,
            'grpc.http2.min_ping_interval_without_data_ms': 300000,
            'grpc.max_send_message_length': 50 * 1024 * 1024, 
            'grpc.max_receive_message_length': 50 * 1024 * 1024, 
        });
        const stream = await client.subscribe();
        stream.on('data', data => this.handleGrpcMessage(data));
        stream.on('error', error => {
            console.error(`[${new Date().toISOString()}] [ERROR] Stream ${idx} error: ${error.message}`);
            this.handleReconnect(idx);
        });
        stream.on('end', () => {
            console.log(`[${new Date().toISOString()}] [INFO] Stream ${idx} ended`);
            if (this.isStarted) setTimeout(() => this.handleReconnect(idx), 2000);
        });

        const request = {
            accounts: {},
            slots: {},
            transactions: { [`chunk${idx}`]: { vote: false, failed: false, accountInclude: chunks[idx], accountExclude: [], accountRequired: [] } },
            transactionsStatus: {},
            entry: {},
            blocks: {},
            blocksMeta: {},
            commitment: CommitmentLevel.CONFIRMED,
            accountsDataSlice: []
        };

        await new Promise((resolve, reject) => stream.write(request, err => {
            if (err) {
                console.error(`[${new Date().toISOString()}] [ERROR] Subscription ${idx} failed: ${err.message}`);
                reject(err);
            } else {
                console.log(`[${new Date().toISOString()}] [INFO] Subscription ${idx} sent`);
                resolve();
            }
        }));

        this.clients.push(client);
        this.streams.push(stream);
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
            } else if (transactionData.signatures && transactionData.message) {
                transaction = meta = transactionData;
            } else if (transactionData.tx) {
                transaction = transactionData.tx.transaction || transactionData.tx;
                meta = transactionData.tx.meta || transactionData.meta;
            } else if (transactionData.slot || transactionData.blockTime) {
                transaction = transactionData.transaction || transactionData;
                meta = transactionData.meta || transactionData;
            } else {
                console.warn(`[${new Date().toISOString()}] [WARN] Unknown transaction format, skipping`);
                return;
            }

            if (!transaction || !meta) {
                console.warn(`[${new Date().toISOString()}] [WARN] Missing transaction or meta data, skipping`);
                return;
            }

            if (meta.err) {
                console.log(`[${new Date().toISOString()}] [INFO] Skipping failed transaction with error: ${JSON.stringify(meta.err)}`);
                return;
            }

            const signature = this.extractSignature(transactionData) || transactionData.signature;
            if (!signature) {
                console.warn(`[${new Date().toISOString()}] [WARN] No signature found, skipping`);
                return;
            }

            if (this.processedTransactions.has(signature)) {
                console.log(`[${new Date().toISOString()}] [INFO] Transaction already processed: signature=${signature}`);
                return;
            }
            this.processedTransactions.add(signature);

            console.log(`[${new Date().toISOString()}] [INFO] Starting processing transaction: signature=${signature}`);

            let accountKeys = transaction.message?.accountKeys || transaction.accountKeys || [];
            if (meta.loadedWritableAddresses) accountKeys = accountKeys.concat(meta.loadedWritableAddresses);
            if (meta.loadedReadonlyAddresses) accountKeys = accountKeys.concat(meta.loadedReadonlyAddresses);

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
                        const pubkeyBuffer = key.pubkey?.type === 'Buffer' ? Buffer.from(key.pubkey.data) : key.pubkey ||
                                            key.key?.type === 'Buffer' ? Buffer.from(key.key.data) : key.key ||
                                            key.address?.type === 'Buffer' ? Buffer.from(key.address.data) : key.address;
                        convertedKey = pubkeyBuffer ? new PublicKey(pubkeyBuffer).toString() : key.toString();
                    } else {
                        convertedKey = new PublicKey(Buffer.from(key)).toString();
                    }
                    if (convertedKey.length === 44) stringAccountKeys.push(convertedKey);
                } catch (error) {
                    console.error(`[${new Date().toISOString()}] [ERROR] Error converting account key: ${error.message}`, error.stack);
                }
            }

            const involvedWallet = Array.from(this.monitoredWallets).find(wallet => stringAccountKeys.includes(wallet));
            if (!involvedWallet) {
                console.log(`[${new Date().toISOString()}] [INFO] No monitored wallet in transaction, skipping: signature=${signature}`);
                return;
            }

            const wallet = await this.db.getWalletByAddress(involvedWallet);
            if (!wallet) {
                console.warn(`[${new Date().toISOString()}] [WARN] Wallet not found in DB: ${involvedWallet}, signature=${signature}`);
                return;
            }

            if (this.activeGroupId && wallet.group_id !== this.activeGroupId) {
                console.log(`[${new Date().toISOString()}] [INFO] Wallet group mismatch, skipping: signature=${signature}`);
                return;
            }

            const blockTime = Number(transactionData.blockTime) || Math.floor(Date.now() / 1000);
            const formattedTransactionData = { transaction, meta, slot: transactionData.slot || 0, blockTime };
            const convertedTransaction = this.convertGrpcToLegacyFormat(formattedTransactionData, stringAccountKeys);

            if (!convertedTransaction) {
                console.warn(`[${new Date().toISOString()}] [WARN] Failed to convert transaction format: signature=${signature}`);
                return;
            }

            await this.monitoringService.processWebhookMessage({
                signature,
                walletAddress: involvedWallet,
                blockTime,
                groupId: wallet.group_id,
                transactionData: convertedTransaction
            });

            console.log(`[${new Date().toISOString()}] [INFO] Transaction processed and sent to webhook: signature=${signature}`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] [ERROR] Error processing transaction: ${error.message}`, error.stack);
        }
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

async handleReconnect(streamIdx = null) {
    if (streamIdx === null) {
        this.reconnectAttempts++;
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] [CRITICAL] Max reconnect attempts reached`);
            this.isStarted = false;
            return;
        }
        console.log(`[${new Date().toISOString()}] [INFO] Reconnecting all streams (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
        await this.subscribeToTransactions();
        return;
    }

    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
        console.error(`[${new Date().toISOString()}] [CRITICAL] Max reconnect attempts reached for stream ${streamIdx}`);
        this.isStarted = false;
        return;
    }
    this.reconnectAttempts++;
    console.log(`[${new Date().toISOString()}] [INFO] Reconnecting stream ${streamIdx} (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

    if (this.streams[streamIdx]) this.streams[streamIdx].end();
    if (this.clients[streamIdx]) this.clients[streamIdx].close?.();
    await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));

    try {
        const client = new Client(this.grpcEndpoint, undefined);
        const stream = await client.subscribe();
        stream.on('data', data => this.handleGrpcMessage(data));
        stream.on('error', error => this.handleReconnect(streamIdx));
        stream.on('end', () => setTimeout(() => this.handleReconnect(streamIdx), 2000));

        const walletArray = Array.from(this.monitoredWallets);
        const chunk = walletArray.slice(streamIdx * 1000, (streamIdx + 1) * 1000);
        const request = {
            transactions: { [`chunk${streamIdx}`]: { vote: false, failed: false, accountInclude: chunk, accountExclude: [], accountRequired: [] } },
            commitment: CommitmentLevel.CONFIRMED
        };
        await new Promise((resolve, reject) => stream.write(request, err => err ? reject(err) : resolve()));
        this.clients[streamIdx] = client;
        this.streams[streamIdx] = stream;
        console.log(`[${new Date().toISOString()}] [INFO] Stream ${streamIdx} reconnected`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] [ERROR] Reconnect failed for stream ${streamIdx}: ${error.message}`);
        await this.handleReconnect(streamIdx);
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
    this.streams?.forEach(stream => stream.end());
    this.clients?.forEach(client => client.close?.());
    this.streams = [];
    this.clients = [];
    this.monitoredWallets.clear();
}

async shutdown() {
    console.log(`[${new Date().toISOString()}] [INFO] Initiating gRPC service shutdown`);

    if (this.streams && this.streams.length > 0) {
        console.log(`[${new Date().toISOString()}] [INFO] Closing ${this.streams.length} gRPC streams`);
        this.streams.forEach((stream, idx) => {
            try {
                stream.end();
                console.log(`[${new Date().toISOString()}] [INFO] Stream ${idx} closed`);
            } catch (error) {
                console.error(`[${new Date().toISOString()}] [ERROR] Error closing stream ${idx}: ${error.message}`, error.stack);
            }
        });
        this.streams = [];
    }

    if (this.clients && this.clients.length > 0) {
        console.log(`[${new Date().toISOString()}] [INFO] Closing ${this.clients.length} gRPC clients`);
        this.clients.forEach((client, idx) => {
            try {
                if (typeof client.close === 'function') client.close();
                else if (typeof client.destroy === 'function') client.destroy();
                else if (typeof client.end === 'function') client.end();
                console.log(`[${new Date().toISOString()}] [INFO] Client ${idx} closed`);
            } catch (error) {
                console.error(`[${new Date().toISOString()}] [ERROR] Error closing client ${idx}: ${error.message}`, error.stack);
            }
        });
        this.clients = [];
    }

    this.monitoredWallets.clear();
    console.log(`[${new Date().toISOString()}] [INFO] Cleared monitored wallets`);

    try {
        await this.db.close();
        console.log(`[${new Date().toISOString()}] [INFO] Database connection closed`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] [ERROR] Error closing database: ${error.message}`, error.stack);
    }

    this.isStarted = false;
    console.log(`[${new Date().toISOString()}] [INFO] Shutdown complete`);
}
}

module.exports = SolanaGrpcService;