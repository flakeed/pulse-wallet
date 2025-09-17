const grpcPackage = require("@triton-one/yellowstone-grpc");
const { SubscribeRequest, SubscribeRequestFilterTransactions, CommitmentLevel } = grpcPackage;
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
        this.activeGroupId = null;
        this.monitoredWallets = new Set();
        this.messageCount = 0;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 20;
        this.reconnectInterval = 5000;
        this.stats = {
            totalTransactions: 0,
            processedTransactions: 0,
            filteredTransactions: 0,
            errors: 0,
            startTime: Date.now()
        };
    }

    async start(groupId = null) {
        if (this.isStarted && this.activeGroupId === groupId) {
            console.log(`[${new Date().toISOString()}] 🔄 gRPC service already started${groupId ? ` for group ${groupId}` : ''}`);
            return;
        }
        console.log(`[${new Date().toISOString()}] 🚀 Starting gRPC Solana client for ${this.grpcEndpoint}${groupId ? `, group ${groupId}` : ''}`);
        this.isStarted = true;
        this.activeGroupId = groupId;
        try {
            await this.connect();
            await this.loadMonitoredWallets();
            await this.subscribeToTransactions();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to start gRPC service:`, error.message);
            this.isStarted = false;
            throw error;
        }
    }

    async connect() {
        try {
            console.log(`[${new Date().toISOString()}] 🔌 Connecting to gRPC: ${this.grpcEndpoint}`);
            const Client = grpcPackage.default;
            if (!Client) {
                throw new Error('gRPC Client not found in default export');
            }
            this.client = new Client(this.grpcEndpoint, undefined, {
                'grpc.keepalive_time_ms': 30000,
                'grpc.keepalive_timeout_ms': 5000,
                'grpc.keepalive_permit_without_calls': true,
                'grpc.http2.max_pings_without_data': 0,
                'grpc.http2.min_time_between_pings_ms': 10000,
                'grpc.http2.min_ping_interval_without_data_ms': 30000
            });
            console.log(`[${new Date().toISOString()}] ✅ Connected to gRPC Solana stream`);
            this.reconnectAttempts = 0;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to create gRPC client:`, error.message);
            console.log(`[${new Date().toISOString()}] 🔍 Default export type:`, typeof grpcPackage.default);
            console.log(`[${new Date().toISOString()}] 🔍 Default export:`, grpcPackage.default);
            throw error;
        }
    }

    async loadMonitoredWallets() {
        try {
            const wallets = await this.db.getActiveWallets(this.activeGroupId);
            this.monitoredWallets.clear();
            wallets.forEach(wallet => {
                this.monitoredWallets.add(wallet.address);
            });
            console.log(`[${new Date().toISOString()}] 📊 Loaded ${this.monitoredWallets.size} wallets for monitoring${this.activeGroupId ? ` in group ${this.activeGroupId}` : ' (all groups)'}`);
            if (this.monitoredWallets.size > 0) {
                console.log(`[${new Date().toISOString()}] 🔍 Sample monitored wallets (group: ${this.activeGroupId || 'all'}):`);
                Array.from(this.monitoredWallets).slice(0, 5).forEach(address => {
                    console.log(`  - ${address.slice(0, 8)}...`);
                });
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error loading monitored wallets:`, error.message);
            throw error;
        }
    }

    async subscribeToTransactions() {
        try {
            console.log(`[${new Date().toISOString()}] 📡 Starting gRPC transaction subscription...`);
            const request = SubscribeRequest.fromJSON({
                accounts: {},
                slots: {},
                transactions: {
                    "client": {
                        accountInclude: [],
                        accountExclude: [],
                        accountRequired: []
                    }
                },
                blocks: {},
                blocksMeta: {},
                accountsDataSlice: [],
                commitment: CommitmentLevel.CONFIRMED,
                entry: {}
            });
            this.stream = await this.client.subscribe();
            await this.stream.write(request, { waitForReady: true });
            console.log(`[${new Date().toISOString()}] ✅ gRPC subscription request sent`);
            for await (const data of this.stream) {
                await this.handleGrpcMessage(data);
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ gRPC subscription error:`, error.message);
            console.error(`[${new Date().toISOString()}] 🔍 Error details:`, error.stack);
            if (this.isStarted) {
                await this.handleReconnect();
            }
        }
    }

    async handleGrpcMessage(data) {
        try {
            this.messageCount++;
            if (data.transaction) {
                this.stats.totalTransactions++;
                await this.processTransaction(data.transaction);
            }
        } catch (error) {
            this.stats.errors++;
            console.error(`[${new Date().toISOString()}] ❌ Error processing gRPC message:`, error.message);
        }
    }

    async processTransaction(transactionData) {
        try {
            const transaction = transactionData.transaction;
            const meta = transactionData.meta;
            if (!transaction || !meta || meta.err) {
                return;
            }
            const signature = transaction.signatures?.[0];
            if (!signature) {
                return;
            }
            const accountKeys = this.extractAccountKeys(transaction);
            const relevantWallets = accountKeys.filter(account => 
                this.monitoredWallets.has(account)
            );
            if (relevantWallets.length === 0) {
                return;
            }
            this.stats.filteredTransactions++;
            const blockTime = this.extractBlockTime(transactionData);
            console.log(`[${new Date().toISOString()}] 🎯 Relevant transaction found: ${signature.slice(0, 8)}... for ${relevantWallets.length} wallet(s)`);
            for (const walletAddress of relevantWallets) {
                const wallet = await this.db.getWalletByAddress(walletAddress);
                if (!wallet) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ Wallet ${walletAddress} not found in database`);
                    continue;
                }
                if (this.activeGroupId && wallet.group_id !== this.activeGroupId) {
                    continue;
                }
                console.log(`[${new Date().toISOString()}] 📝 Processing transaction for wallet ${walletAddress.slice(0,8)}... (group: ${wallet.group_id || 'none'})`);
                await this.monitoringService.processWebhookMessage({
                    signature: signature,
                    walletAddress: walletAddress,
                    blockTime: blockTime,
                    groupId: wallet.group_id
                });
                this.stats.processedTransactions++;
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error processing transaction:`, error.message);
            this.stats.errors++;
        }
    }

    extractAccountKeys(transaction) {
        const accountKeys = [];
        try {
            if (transaction.message?.accountKeys) {
                transaction.message.accountKeys.forEach(key => {
                    if (typeof key === 'string') {
                        accountKeys.push(key);
                    } else if (key.pubkey) {
                        accountKeys.push(key.pubkey);
                    }
                });
            }
            if (transaction.message?.addressTableLookups) {
                transaction.message.addressTableLookups.forEach(lookup => {
                    if (lookup.writableIndexes) {
                        lookup.writableIndexes.forEach(index => {
                            if (lookup.accountKey) {
                                accountKeys.push(lookup.accountKey);
                            }
                        });
                    }
                    if (lookup.readonlyIndexes) {
                        lookup.readonlyIndexes.forEach(index => {
                            if (lookup.accountKey) {
                                accountKeys.push(lookup.accountKey);
                            }
                        });
                    }
                });
            }
            if (transaction.message?.instructions) {
                transaction.message.instructions.forEach(instruction => {
                    if (instruction.accounts) {
                        instruction.accounts.forEach(accountIndex => {
                            if (transaction.message.accountKeys?.[accountIndex]) {
                                const key = transaction.message.accountKeys[accountIndex];
                                if (typeof key === 'string') {
                                    accountKeys.push(key);
                                } else if (key.pubkey) {
                                    accountKeys.push(key.pubkey);
                                }
                            }
                        });
                    }
                });
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error extracting account keys:`, error.message);
        }
        return [...new Set(accountKeys)];
    }

    extractBlockTime(transactionData) {
        try {
            if (transactionData.meta?.blockTime) {
                return transactionData.meta.blockTime;
            }
            if (transactionData.blockTime) {
                return transactionData.blockTime;
            }
            if (transactionData.transaction?.blockTime) {
                return transactionData.transaction.blockTime;
            }
            return Math.floor(Date.now() / 1000);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error extracting block time:`, error.message);
            return Math.floor(Date.now() / 1000);
        }
    }

    async subscribeToWallet(walletAddress) {
        try {
            this.monitoredWallets.add(walletAddress);
            console.log(`[${new Date().toISOString()}] ✅ Added wallet ${walletAddress.slice(0, 8)}... to gRPC monitoring (total: ${this.monitoredWallets.size})`);
            return { success: true };
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error adding wallet to gRPC monitoring:`, error.message);
            throw error;
        }
    }

    async unsubscribeFromWallet(walletAddress) {
        try {
            this.monitoredWallets.delete(walletAddress);
            console.log(`[${new Date().toISOString()}] ✅ Removed wallet ${walletAddress.slice(0, 8)}... from gRPC monitoring (total: ${this.monitoredWallets.size})`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error removing wallet from gRPC monitoring:`, error.message);
        }
    }

    async subscribeToWalletsBatch(walletAddresses, batchSize = 1000) {
        if (!walletAddresses || walletAddresses.length === 0) return { successful: 0, failed: 0 };
        const startTime = Date.now();
        let successful = 0;
        try {
            walletAddresses.forEach(address => {
                this.monitoredWallets.add(address);
                successful++;
            });
            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] ✅ gRPC batch subscription completed in ${duration}ms:`);
            console.log(`  - Added: ${successful} wallets`);
            console.log(`  - Total monitored: ${this.monitoredWallets.size}`);
            return { successful, failed: 0, errors: [] };
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in gRPC batch subscription:`, error.message);
            return { successful, failed: walletAddresses.length - successful, errors: [error.message] };
        }
    }

    async unsubscribeFromWalletsBatch(walletAddresses, batchSize = 1000) {
        if (!walletAddresses || walletAddresses.length === 0) return { successful: 0, failed: 0 };
        const startTime = Date.now();
        let successful = 0;
        try {
            walletAddresses.forEach(address => {
                this.monitoredWallets.delete(address);
                successful++;
            });
            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] ✅ gRPC batch unsubscription completed in ${duration}ms: ${successful} wallets removed`);
            console.log(`[${new Date().toISOString()}] 📊 Remaining monitored wallets: ${this.monitoredWallets.size}`);
            return { successful, failed: 0, errors: [] };
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in gRPC batch unsubscription:`, error.message);
            return { successful, failed: walletAddresses.length - successful, errors: [error.message] };
        }
    }

    async removeAllWallets(groupId = null) {
        try {
            const startTime = Date.now();
            console.log(`[${new Date().toISOString()}] 🗑️ Starting complete wallet removal from gRPC service${groupId ? ` for group ${groupId}` : ''}`);
            const walletsToRemove = await this.db.getActiveWallets(groupId);
            const addressesToRemove = walletsToRemove.map(w => w.address);
            console.log(`[${new Date().toISOString()}] 📊 gRPC service found ${walletsToRemove.length} wallets to remove`);
            addressesToRemove.forEach(address => {
                this.monitoredWallets.delete(address);
            });
            console.log(`[${new Date().toISOString()}] 🧹 Removed ${addressesToRemove.length} wallets from gRPC monitoring`);
            console.log(`[${new Date().toISOString()}] 🗄️ Starting database removal...`);
            await this.monitoringService.removeAllWallets(groupId);
            console.log(`[${new Date().toISOString()}] ✅ Database removal completed`);
            const shouldReload = this.isStarted && (
                (groupId && groupId === this.activeGroupId) ||
                (!groupId)
            );
            if (shouldReload) {
                console.log(`[${new Date().toISOString()}] 🔄 Reloading monitored wallets after group deletion...`);
                await this.loadMonitoredWallets();
                console.log(`[${new Date().toISOString()}] ✅ Reload completed: ${this.monitoredWallets.size} wallets now monitored`);
            }
            const duration = Date.now() - startTime;
            const finalReport = {
                walletsRemoved: walletsToRemove.length,
                addressesRemoved: addressesToRemove.length,
                remainingMonitored: this.monitoredWallets.size,
                groupId: groupId,
                reloaded: shouldReload,
                processingTime: `${duration}ms`
            };
            console.log(`[${new Date().toISOString()}] 🎉 Complete gRPC wallet removal finished in ${duration}ms:`, finalReport);
            return {
                success: true,
                message: `gRPC service: removed ${walletsToRemove.length} wallets, ${this.monitoredWallets.size} wallets remaining`,
                details: finalReport
            };
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in gRPC removeAllWallets:`, error.message);
            throw error;
        }
    }

    async switchGroup(groupId) {
        try {
            const startTime = Date.now();
            console.log(`[${new Date().toISOString()}] 🔄 Switching to group ${groupId || 'all'} in gRPC service`);
            this.activeGroupId = groupId;
            await this.loadMonitoredWallets();
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
        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));
        try {
            await this.stop();
            await this.connect();
            await this.loadMonitoredWallets();
            await this.subscribeToTransactions();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ gRPC reconnect failed:`, error.message);
        }
    }

    getStatus() {
        const uptime = Date.now() - this.stats.startTime;
        const transactionsPerSecond = this.stats.totalTransactions / (uptime / 1000);
        return {
            isConnected: this.client !== null && this.stream !== null,
            isStarted: this.isStarted,
            activeGroupId: this.activeGroupId,
            monitoredWallets: this.monitoredWallets.size,
            messageCount: this.messageCount,
            reconnectAttempts: this.reconnectAttempts,
            grpcEndpoint: this.grpcEndpoint,
            mode: 'grpc',
            stats: {
                ...this.stats,
                uptime: uptime,
                transactionsPerSecond: Math.round(transactionsPerSecond * 100) / 100,
                filterEfficiency: this.stats.totalTransactions > 0 ? 
                    Math.round((this.stats.filteredTransactions / this.stats.totalTransactions) * 10000) / 100 : 0
            }
        };
    }

    async stop() {
        console.log(`[${new Date().toISOString()}] ⏹️ Stopping gRPC service...`);
        this.isStarted = false;
        try {
            if (this.stream) {
                await this.stream.end();
                this.stream = null;
            }
            if (this.client) {
                this.client.close();
                this.client = null;
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error stopping gRPC service:`, error.message);
        }
        console.log(`[${new Date().toISOString()}] ✅ gRPC service stopped`);
    }

    async shutdown() {
        await this.stop();
        await this.db.close().catch(() => {});
        console.log(`[${new Date().toISOString()}] ✅ gRPC service shutdown complete`);
    }
}

module.exports = SolanaGrpcService;