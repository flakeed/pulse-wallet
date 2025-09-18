const Client = require('@triton-one/yellowstone-grpc');
const WalletMonitoringService = require('./monitoringService');
const Database = require('../database/connection');

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
                const toDelete = Array.from(this.processedTransactions).slice(0, 5000);
                toDelete.forEach(sig => this.processedTransactions.delete(sig));
            }
        }, 300000);
    }

    async start(groupId = null) {
        if (this.isStarted && this.activeGroupId === groupId) {
            console.log(`[${new Date().toISOString()}] 🔄 gRPC service already started${groupId ? ` for group ${groupId}` : ''}`);
            return;
        }

        console.log(`[${new Date().toISOString()}] 🚀 Starting Solana gRPC client for ${this.grpcEndpoint}${groupId ? `, group ${groupId}` : ''}`);
        this.isStarted = true;
        this.activeGroupId = groupId;

        try {
            await this.connect();
            await this.subscribeToTransactions();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to start gRPC service:`, error.message);
            this.isStarted = false;
            throw error;
        }
    }

    async connect() {
        if (this.isConnecting || this.client) return;
        this.isConnecting = true;

        console.log(`[${new Date().toISOString()}] 🔌 Connecting to gRPC: ${this.grpcEndpoint}`);
        
        try {
            this.client = new Client(this.grpcEndpoint, undefined, {
                'grpc.keepalive_time_ms': 30000,
                'grpc.keepalive_timeout_ms': 5000,
                'grpc.keepalive_permit_without_calls': true,
                'grpc.http2.max_pings_without_data': 0,
                'grpc.http2.min_time_between_pings_ms': 10000,
                'grpc.http2.min_ping_interval_without_data_ms': 300000
            });

            console.log(`[${new Date().toISOString()}] ✅ Connected to Solana gRPC`);
            this.reconnectAttempts = 0;
            this.isConnecting = false;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to create gRPC client:`, error.message);
            this.isConnecting = false;
            throw error;
        }
    }

    async subscribeToTransactions() {
        try {
            const wallets = await this.db.getActiveWallets(this.activeGroupId);
            this.monitoredWallets.clear();
            wallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
            
            console.log(`[${new Date().toISOString()}] 📊 Monitoring ${this.monitoredWallets.size} wallets${this.activeGroupId ? ` in group ${this.activeGroupId}` : ' (all groups)'}`);

            if (this.stream) {
                this.stream.end();
            }

            const request = {
                slots: {},
                accounts: {},
                transactions: {
                    "all_transactions": {
                        accountInclude: [],
                        accountExclude: [],
                        accountRequired: []
                    }
                },
                blocks: {},
                blocksMeta: {},
                accountsDataSlice: [],
                commitment: 1,
                entry: {}
            };

            this.stream = await this.client.subscribe();
            
            this.stream.on('data', (data) => {
                this.messageCount++;
                this.handleGrpcMessage(data);
            });

            this.stream.on('error', (error) => {
                console.error(`[${new Date().toISOString()}] ❌ gRPC stream error:`, error.message);
                this.handleReconnect();
            });

            this.stream.on('end', () => {
                console.log(`[${new Date().toISOString()}] 🔌 gRPC stream ended`);
                if (this.isStarted) this.handleReconnect();
            });

            this.stream.write(request);
            console.log(`[${new Date().toISOString()}] ✅ Subscribed to all Solana transactions via gRPC`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error subscribing to transactions:`, error.message);
            throw error;
        }
    }

    async handleGrpcMessage(data) {
        try {
            if (data.transaction) {
                await this.processTransaction(data.transaction);
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error handling gRPC message:`, error.message);
        }
    }

    async processTransaction(transactionData) {
        try {
            const transaction = transactionData.transaction;
            const meta = transactionData.meta;
            
            if (!transaction || !meta || meta.err) {
                return;
            }

            const signature = transaction.signatures[0];
            
            if (this.processedTransactions.has(signature)) {
                return;
            }
            this.processedTransactions.add(signature);

            let accountKeys = [];
            if (transaction.message.accountKeys) {
                accountKeys = transaction.message.accountKeys.map(key => 
                    Buffer.from(key).toString('base64')
                );
            }

            let involvedWallet = null;
            for (const walletAddress of this.monitoredWallets) {
                try {
                    const { PublicKey } = require('@solana/web3.js');
                    const pubkey = new PublicKey(walletAddress);
                    const base64Key = Buffer.from(pubkey.toBytes()).toString('base64');
                    
                    if (accountKeys.includes(base64Key)) {
                        involvedWallet = walletAddress;
                        break;
                    }
                } catch (error) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ Invalid wallet address: ${walletAddress}`);
                    continue;
                }
            }

            if (!involvedWallet) {
                return;
            }

            const wallet = await this.db.getWalletByAddress(involvedWallet);
            if (!wallet) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Wallet ${involvedWallet} not found in database`);
                return;
            }

            if (this.activeGroupId && wallet.group_id !== this.activeGroupId) {
                return;
            }

            let blockTime;
            if (transactionData.slot && transactionData.blockTime) {
                blockTime = Number(transactionData.blockTime);
            } else {
                blockTime = Math.floor(Date.now() / 1000);
            }

            console.log(`[${new Date().toISOString()}] 🔍 Processing transaction ${signature.slice(0, 8)}... for wallet ${involvedWallet.slice(0, 8)}...`);

            const convertedTransaction = this.convertGrpcToLegacyFormat(transactionData, accountKeys);
            
            await this.monitoringService.processWebhookMessage({
                signature: signature,
                walletAddress: involvedWallet,
                blockTime: blockTime,
                groupId: wallet.group_id,
                transactionData: convertedTransaction
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing gRPC transaction:`, error.message);
        }
    }

    convertGrpcToLegacyFormat(grpcData, accountKeys) {
        try {
            const transaction = grpcData.transaction;
            const meta = grpcData.meta;

            const converted = {
                transaction: {
                    message: {
                        accountKeys: accountKeys.map(key => {
                            try {
                                const { PublicKey } = require('@solana/web3.js');
                                return new PublicKey(Buffer.from(key, 'base64'));
                            } catch (error) {
                                return key;
                            }
                        }),
                        instructions: transaction.message.instructions || []
                    },
                    signatures: transaction.signatures || []
                },
                meta: {
                    err: meta.err,
                    fee: meta.fee || 0,
                    preBalances: meta.preBalances || [],
                    postBalances: meta.postBalances || [],
                    preTokenBalances: this.convertTokenBalances(meta.preTokenBalances || []),
                    postTokenBalances: this.convertTokenBalances(meta.postTokenBalances || []),
                    logMessages: meta.logMessages || [],
                    innerInstructions: meta.innerInstructions || []
                },
                slot: grpcData.slot,
                blockTime: grpcData.blockTime ? Number(grpcData.blockTime) : Math.floor(Date.now() / 1000)
            };

            return converted;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error converting gRPC format:`, error.message);
            return null;
        }
    }

    convertTokenBalances(grpcTokenBalances) {
        return grpcTokenBalances.map(balance => ({
            accountIndex: balance.accountIndex || 0,
            mint: balance.mint || '',
            owner: balance.owner || '',
            programId: balance.programId || '',
            uiTokenAmount: {
                amount: balance.uiTokenAmount?.amount || '0',
                decimals: balance.uiTokenAmount?.decimals || 0,
                uiAmount: balance.uiTokenAmount?.uiAmount || 0,
                uiAmountString: balance.uiTokenAmount?.uiAmountString || '0'
            }
        }));
    }

    async subscribeToWalletsBatch(walletAddresses, batchSize = 100) {
        console.log(`[${new Date().toISOString()}] 📦 Adding ${walletAddresses.length} wallets to gRPC monitoring`);
        
        walletAddresses.forEach(address => {
            this.monitoredWallets.add(address);
        });

        console.log(`[${new Date().toISOString()}] ✅ gRPC now monitoring ${this.monitoredWallets.size} total wallets`);
        
        return {
            successful: walletAddresses.length,
            failed: 0,
            errors: []
        };
    }

    async unsubscribeFromWalletsBatch(walletAddresses, batchSize = 100) {
        console.log(`[${new Date().toISOString()}] 📦 Removing ${walletAddresses.length} wallets from gRPC monitoring`);
        
        walletAddresses.forEach(address => {
            this.monitoredWallets.delete(address);
        });

        console.log(`[${new Date().toISOString()}] ✅ gRPC now monitoring ${this.monitoredWallets.size} total wallets`);
        
        return {
            successful: walletAddresses.length,
            failed: 0,
            errors: []
        };
    }

    async subscribeToWallet(walletAddress) {
        this.monitoredWallets.add(walletAddress);
        console.log(`[${new Date().toISOString()}] ✅ Added wallet ${walletAddress.slice(0, 8)}... to gRPC monitoring`);
        return { success: true };
    }

    async unsubscribeFromWallet(walletAddress) {
        this.monitoredWallets.delete(walletAddress);
        console.log(`[${new Date().toISOString()}] ✅ Removed wallet ${walletAddress.slice(0, 8)}... from gRPC monitoring`);
    }

    async removeAllWallets(groupId = null) {
        try {
            const startTime = Date.now();
            console.log(`[${new Date().toISOString()}] 🗑️ Starting wallet removal from gRPC service${groupId ? ` for group ${groupId}` : ''}`);
            
            const walletsToRemove = await this.db.getActiveWallets(groupId);
            const addressesToRemove = walletsToRemove.map(w => w.address);
            
            console.log(`[${new Date().toISOString()}] 📊 gRPC service removing ${walletsToRemove.length} wallets from monitoring`);
            
            addressesToRemove.forEach(address => {
                this.monitoredWallets.delete(address);
            });
            
            await this.monitoringService.removeAllWallets(groupId);
            
            const shouldReload = this.isStarted && (
                (groupId && groupId === this.activeGroupId) ||
                (!groupId)
            );
            
            if (shouldReload) {
                console.log(`[${new Date().toISOString()}] 🔄 Reloading wallet list for gRPC monitoring...`);
                
                const remainingWallets = await this.db.getActiveWallets(this.activeGroupId);
                this.monitoredWallets.clear();
                remainingWallets.forEach(wallet => this.monitoredWallets.add(wallet.address));
                
                console.log(`[${new Date().toISOString()}] ✅ gRPC monitoring reloaded: ${this.monitoredWallets.size} wallets`);
            }
            
            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] 🎉 gRPC wallet removal completed in ${duration}ms`);
            
            return {
                success: true,
                message: `gRPC service: removed ${walletsToRemove.length} wallets from monitoring`,
                details: {
                    walletsRemoved: walletsToRemove.length,
                    remainingWallets: this.monitoredWallets.size,
                    groupId: groupId,
                    processingTime: `${duration}ms`
                }
            };
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in gRPC removeAllWallets:`, error.message);
            throw error;
        }
    }

    async switchGroup(groupId) {
        try {
            const startTime = Date.now();
            console.log(`[${new Date().toISOString()}] 🔄 Switching gRPC monitoring to group ${groupId || 'all'}`);

            this.activeGroupId = groupId;
            
            const wallets = await this.db.getActiveWallets(groupId);
            this.monitoredWallets.clear();
            wallets.forEach(wallet => this.monitoredWallets.add(wallet.address));

            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] ✅ gRPC group switch completed in ${duration}ms: now monitoring ${this.monitoredWallets.size} wallets`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in gRPC switchGroup:`, error.message);
            throw error;
        }
    }

    async handleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] ❌ Max reconnect attempts reached for gRPC service`);
            this.isStarted = false;
            return;
        }

        this.reconnectAttempts++;
        console.log(`[${new Date().toISOString()}] 🔄 Reconnecting gRPC service (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

        if (this.stream) {
            this.stream.end();
            this.stream = null;
        }
        if (this.client) {
            this.client.close();
            this.client = null;
        }

        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));
        
        try {
            await this.connect();
            await this.subscribeToTransactions();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ gRPC reconnect failed:`, error.message);
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
        
        if (this.stream) {
            this.stream.end();
            this.stream = null;
        }
        
        if (this.client) {
            this.client.close();
            this.client = null;
        }
        
        this.monitoredWallets.clear();
        console.log(`[${new Date().toISOString()}] ⏹️ gRPC client stopped`);
    }

    async shutdown() {
        await this.stop();
        await this.db.close().catch(() => {});
        console.log(`[${new Date().toISOString()}] ✅ gRPC service shutdown complete`);
    }
}

module.exports = SolanaGrpcService;