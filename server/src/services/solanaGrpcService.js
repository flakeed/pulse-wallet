const { default: Client, CommitmentLevel } = require('@triton-one/yellowstone-grpc');
const WalletMonitoringService = require('./monitoringService');
const Database = require('../database/connection');
const { PublicKey } = require('@solana/web3.js');

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
        if (this.isStarted && this.activeGroupId === groupId) return;
        this.isStarted = true;
        this.activeGroupId = groupId;
        await this.connect();
        await this.subscribeToTransactions();
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
                'grpc.http2.min_ping_interval_without_data_ms': 300000
            });
            this.reconnectAttempts = 0;
            this.isConnecting = false;
        } catch (error) {
            this.isConnecting = false;
            throw error;
        }
    }

    async subscribeToTransactions() {
        const wallets = await this.db.getActiveWallets(this.activeGroupId);
        this.monitoredWallets.clear();
        wallets.forEach(wallet => this.monitoredWallets.add(wallet.address));

        if (this.monitoredWallets.size === 0) return;

        if (this.stream) this.stream.end();

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

        this.stream = await this.client.subscribe();
        this.stream.on('data', data => {
            this.messageCount++;
            if (data.transaction) this.handleGrpcMessage(data);
        });
        this.stream.on('error', error => {
            if (error.message.includes('serialization failure')) {
                this.isStarted = false;
                return;
            }
            this.handleReconnect();
        });
        this.stream.on('end', () => {
            if (this.isStarted) setTimeout(() => this.handleReconnect(), 2000);
        });

        await new Promise((resolve, reject) => this.stream.write(request, err => err ? reject(err) : resolve()));
    }

    async handleGrpcMessage(data) {
        const timestamp = new Date().toISOString();
        try {
            let signature = null;
            let transaction = null, meta = null;
            if (data.transaction?.transaction) {
                transaction = data.transaction.transaction;
                meta = data.transaction.meta;
                signature = transaction.signature || (transaction.signatures && transaction.signatures[0])
                    ? Buffer.from((transaction.signature || transaction.signatures[0]).data || transaction.signature || transaction.signatures[0]).toString('base64')
                    : data.transaction.signature;
            } else if (data.transaction && data.meta) {
                transaction = data.transaction;
                meta = data.meta;
                signature = transaction.signature || (transaction.signatures && transaction.signatures[0])
                    ? Buffer.from((transaction.signature || transaction.signatures[0]).data || transaction.signature || transaction.signatures[0]).toString('base64')
                    : data.transaction.signature;
            } else if (data.signatures && data.message) {
                signature = data.signature || (data.signatures && data.signatures[0])
                    ? Buffer.from((data.signature || data.signatures[0]).data || data.signature || data.signatures[0]).toString('base64')
                    : data.signature;
            } else if (data.tx) {
                transaction = data.tx.transaction || data.tx;
                meta = data.tx.meta || data.meta;
                signature = transaction.signature || (transaction.signatures && transaction.signatures[0])
                    ? Buffer.from((transaction.signature || transaction.signatures[0]).data || transaction.signature || transaction.signatures[0]).toString('base64')
                    : data.signature;
            } else if (data.slot || data.blockTime) {
                transaction = data.transaction || data;
                meta = data.meta || data;
                signature = transaction.signature || (transaction.signatures && transaction.signatures[0])
                    ? Buffer.from((transaction.signature || transaction.signatures[0]).data || transaction.signature || transaction.signatures[0]).toString('base64')
                    : data.signature;
            }

            if (signature) {
                console.log(`[${timestamp}] 🔍 [SIG:${signature}] Received gRPC transaction message`);
            } else {
                console.log(`[${timestamp}] ⚠️ [NO_SIG] Received gRPC message without extractable signature`);
                return;
            }

            await this.processTransaction({ transaction, meta, slot: data.slot, blockTime: data.blockTime, signature });
        } catch (error) {
            console.error(`[${timestamp}] ❌ [NO_SIG] Error handling gRPC message:`, error.message);
        }
    }

    async processTransaction(transactionData) {
        const timestamp = new Date().toISOString();
        const signature = transactionData.signature;
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
            console.log(`[${timestamp}] 🚫 [SIG:${signature}] Skipping: Invalid transaction data structure`);
            return;
        }

        if (!transaction || !meta || meta.err) {
            console.log(`[${timestamp}] 🚫 [SIG:${signature}] Skipping: Missing transaction/meta or has error`);
            return;
        }

        if (this.processedTransactions.has(signature)) {
            console.log(`[${timestamp}] ⏭️ [SIG:${signature}] Skipping: Already processed`);
            return;
        }
        this.processedTransactions.add(signature);

        console.log(`[${timestamp}] 📝 [SIG:${signature}] Starting processing new transaction`);

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
            } catch (error) {}
        }

        console.log(`[${timestamp}] 🔑 [SIG:${signature}] Extracted ${stringAccountKeys.length} account keys`);

        const involvedWallet = Array.from(this.monitoredWallets).find(wallet => stringAccountKeys.includes(wallet));
        if (!involvedWallet) {
            console.log(`[${timestamp}] 🚫 [SIG:${signature}] No monitored wallet involved`);
            return;
        }

        console.log(`[${timestamp}] 👛 [SIG:${signature}] Involved wallet: ${involvedWallet}`);

        const wallet = await this.db.getWalletByAddress(involvedWallet);
        if (!wallet || (this.activeGroupId && wallet.group_id !== this.activeGroupId)) {
            console.log(`[${timestamp}] 🚫 [SIG:${signature}] Wallet not found in DB or group mismatch (groupId: ${this.activeGroupId})`);
            return;
        }

        console.log(`[${timestamp}] ✅ [SIG:${signature}] Wallet validated in DB (groupId: ${wallet.group_id})`);

        const blockTime = Number(transactionData.blockTime) || Math.floor(Date.now() / 1000);
        const formattedTransactionData = { transaction, meta, slot: transactionData.slot || 0, blockTime };
        const convertedTransaction = this.convertGrpcToLegacyFormat(formattedTransactionData, stringAccountKeys);

        if (!convertedTransaction) {
            console.error(`[${timestamp}] ❌ [SIG:${signature}] Failed to convert to legacy format`);
            return;
        }

        console.log(`[${timestamp}] 🔄 [SIG:${signature}] Converted to legacy format successfully`);

        try {
            console.log(`[${timestamp}] 🚀 [SIG:${signature}] Sending to monitoring service (webhook/SSE)`);
            await this.monitoringService.processWebhookMessage({
                signature,
                walletAddress: involvedWallet,
                blockTime,
                groupId: wallet.group_id,
                transactionData: convertedTransaction
            });
            console.log(`[${timestamp}] ✅ [SIG:${signature}] Successfully sent to monitoring service (webhook/SSE)`);
        } catch (error) {
            console.error(`[${timestamp}] ❌ [SIG:${signature}] Error sending to monitoring service:`, error.message);
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
            return null;
        }
    }

    convertTokenBalances(grpcTokenBalances, toBase58) {
        return grpcTokenBalances.map(balance => ({
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
        }));
    }

    async subscribeToWalletsBatch(walletAddresses) {
        walletAddresses.forEach(address => this.monitoredWallets.add(address));
        return { successful: walletAddresses.length, failed: 0, errors: [] };
    }

    async unsubscribeFromWalletsBatch(walletAddresses) {
        walletAddresses.forEach(address => this.monitoredWallets.delete(address));
        return { successful: walletAddresses.length, failed: 0, errors: [] };
    }

    async subscribeToWallet(walletAddress) {
        this.monitoredWallets.add(walletAddress);
        return { success: true };
    }

    async unsubscribeFromWallet(walletAddress) {
        this.monitoredWallets.delete(walletAddress);
    }

    async removeAllWallets(groupId = null) {
        const walletsToRemove = await this.db.getActiveWallets(groupId);
        walletsToRemove.forEach(wallet => this.monitoredWallets.delete(wallet.address));
        await this.monitoringService.removeAllWallets(groupId);
        if (this.isStarted && (!groupId || groupId === this.activeGroupId)) {
            const remainingWallets = await this.db.getActiveWallets(this.activeGroupId);
            this.monitoredWallets.clear();
            remainingWallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
        }
        return {
            success: true,
            message: `Removed ${walletsToRemove.length} wallets`,
            details: { walletsRemoved: walletsToRemove.length, remainingWallets: this.monitoredWallets.size, groupId }
        };
    }

    async switchGroup(groupId) {
        this.activeGroupId = groupId;
        const wallets = await this.db.getActiveWallets(groupId);
        this.monitoredWallets.clear();
        wallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
        if (this.isStarted) await this.subscribeToTransactions();
    }

    async handleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            this.isStarted = false;
            return;
        }
        this.reconnectAttempts++;
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
        } catch (error) {
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
        await this.db.close().catch(() => {});
    }
}

module.exports = SolanaGrpcService;