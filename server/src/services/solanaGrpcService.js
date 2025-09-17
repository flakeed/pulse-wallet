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
    if (this.stream) {
        try {
            await this.stream.end();
        } catch (e) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Error ending existing stream:`, e.message);
        }
        this.stream = null;
    }

    try {
        console.log(`[${new Date().toISOString()}] 📡 Starting gRPC transaction subscription...`);
        
        const accountsToMonitor = Array.from(this.monitoredWallets);
        console.log(`[${new Date().toISOString()}] 📊 Monitoring ${accountsToMonitor.length} wallets:`, 
            accountsToMonitor.slice(0, 3).map(a => a.slice(0, 8)).join(', '));
        
        let request;
        if (accountsToMonitor.length > 0) {
            request = SubscribeRequest.fromJSON({
                accounts: {
                    "monitored_accounts": {
                        account: accountsToMonitor,
                        owner: [],
                        filters: []
                    }
                },
                slots: {},
                transactions: {
                    "monitored_transactions": {
                        accountInclude: accountsToMonitor,
                        accountExclude: [],
                        accountRequired: accountsToMonitor
                    }
                },
                blocks: {},
                blocksMeta: {},
                accountsDataSlice: [],
                commitment: CommitmentLevel.CONFIRMED,
                entry: {}
            });
            console.log(`[${new Date().toISOString()}] 📡 Subscribing to ${accountsToMonitor.length} specific accounts`);
        } else {
            request = SubscribeRequest.fromJSON({
                accounts: {},
                slots: {},
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
                commitment: CommitmentLevel.CONFIRMED,
                entry: {}
            });
            console.log(`[${new Date().toISOString()}] 📡 No wallets to monitor, subscribing to all transactions`);
        }
        
        if (!this.client) {
            await this.connect();
        }
        
        this.stream = await this.client.subscribe();
        
        await Promise.race([
            this.stream.write(request),
            new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Subscription write timeout')), 10000)
            )
        ]);
        
        console.log(`[${new Date().toISOString()}] ✅ gRPC subscription request sent`);
        
        this.startMessageProcessing();
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ gRPC subscription error:`, error.message);
        this.stream = null;
        
        if (this.isStarted) {
            await new Promise(resolve => setTimeout(resolve, 2000));
            await this.handleReconnect();
        }
        throw error;
    }
}

startMessageProcessing() {
    if (!this.stream) {
        console.error(`[${new Date().toISOString()}] ❌ Cannot start message processing - no stream`);
        return;
    }

    const processMessages = async () => {
        try {
            for await (const data of this.stream) {
                if (!this.isStarted) {
                    console.log(`[${new Date().toISOString()}] ⏹️ Service stopped, ending message processing`);
                    break;
                }
                
                await this.handleGrpcMessage(data);
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Stream processing error:`, error.message);
            
            if (this.isStarted) {
                console.log(`[${new Date().toISOString()}] 🔄 Stream ended, waiting for reconnection...`);
            }
        }
    };

    processMessages().catch(err => {
        console.error(`[${new Date().toISOString()}] ❌ Background message processing failed:`, err.message);
    });
}

    async handleGrpcMessage(data) {
        try {
            this.messageCount++;
            
            if (this.messageCount % 50 === 0) {
                console.log(`[${new Date().toISOString()}] 📊 gRPC messages processed: ${this.messageCount}`);
            }
            
            if (data.transaction && data.transaction.transaction) {
                this.stats.totalTransactions++;
                console.log(`[${new Date().toISOString()}] 📥 Received transaction data, signature: ${data.transaction.transaction.signatures?.[0]?.slice(0, 8) || 'unknown'}...`);
                await this.processTransaction(data.transaction);
            }
            
            if (data.account) {
                if (this.messageCount % 100 === 0) {
                    console.log(`[${new Date().toISOString()}] 📥 Received account update`);
                }
            }
            
        } catch (error) {
            this.stats.errors++;
            console.error(`[${new Date().toISOString()}] ❌ Error processing gRPC message:`, error.message);
        }
    }

    async processTransaction(transactionData) {
        try {
            console.log(`[${new Date().toISOString()}] 🔍 Raw transactionData:`, JSON.stringify(transactionData, null, 2).slice(0, 1000));
            const transaction = transactionData.transaction;
            const meta = transactionData.meta;
            
            if (!transaction) {
                console.log(`[${new Date().toISOString()}] ⏭️ Skipping - no transaction data`);
                return;
            }
            
            if (!meta) {
                console.log(`[${new Date().toISOString()}] ⏭️ Skipping - no meta data`);
                return;
            }
            
            if (meta.err) {
                console.log(`[${new Date().toISOString()}] ⏭️ Skipping - transaction failed with error:`, meta.err);
                return;
            }

            const signature = transaction.signatures?.[0];
            if (!signature) {
                console.log(`[${new Date().toISOString()}] ⏭️ Skipping - no signature`);
                return;
            }

            console.log(`[${new Date().toISOString()}] 🔍 Processing transaction: ${signature.slice(0, 8)}...`);

            if (!transaction.message) {
                console.log(`[${new Date().toISOString()}] ⏭️ Skipping - no message in transaction`);
                return;
            }

            const accountKeys = this.extractAccountKeys(transaction);
            console.log(`[${new Date().toISOString()}] 🔍 Extracted ${accountKeys.length} account keys from transaction`);
            
            if (accountKeys.length === 0) {
                console.log(`[${new Date().toISOString()}] ⏭️ Skipping - no account keys found`);
                return;
            }
            
            const relevantWallets = [];
            accountKeys.forEach(account => {
                const isMonitored = this.monitoredWallets.has(account);
                if (isMonitored) {
                    console.log(`[${new Date().toISOString()}] ✅ Found monitored wallet in transaction: ${account.slice(0, 8)}...`);
                    relevantWallets.push(account);
                }
            });

            if (relevantWallets.length === 0) {
                console.log(`[${new Date().toISOString()}] ⏭️ No monitored wallets found in transaction ${signature.slice(0, 8)}...`);
                if (accountKeys.length > 0) {
                    console.log(`[${new Date().toISOString()}] 🔍 Account keys in transaction: ${accountKeys.slice(0, 3).map(k => k.slice(0, 8)).join(', ')}...`);
                    console.log(`[${new Date().toISOString()}] 🔍 First few monitored wallets: ${Array.from(this.monitoredWallets).slice(0, 3).map(k => k.slice(0, 8)).join(', ')}...`);
                }
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
                    console.log(`[${new Date().toISOString()}] ⏭️ Skipping wallet ${walletAddress.slice(0, 8)}... - wrong group (${wallet.group_id} vs ${this.activeGroupId})`);
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
            console.error(`[${new Date().toISOString()}] 🔍 Transaction data structure:`, JSON.stringify(transactionData, null, 2).slice(0, 500));
            this.stats.errors++;
        }
    }

    extractAccountKeys(transaction) {
        const accountKeys = [];
        try {
            console.log(`[${new Date().toISOString()}] 🔍 Extracting account keys from transaction structure`);
            
            if (transaction.message?.accountKeys) {
                console.log(`[${new Date().toISOString()}] 🔍 Found ${transaction.message.accountKeys.length} account keys in message`);
                transaction.message.accountKeys.forEach((key, index) => {
                    let keyString;
                    try {
                        if (typeof key === 'string') {
                            keyString = key;
                        } else if (key && typeof key === 'object') {
                            keyString = key.pubkey || key.address || key.key;
                            if (!keyString && key.toString && typeof key.toString === 'function') {
                                keyString = key.toString();
                            }
                        }
                        
                        if (keyString && typeof keyString === 'string' && keyString.length >= 32) {
                            accountKeys.push(keyString);
                            if (index < 5) { 
                                console.log(`[${new Date().toISOString()}] 🔍 Account ${index}: ${keyString.slice(0, 8)}...`);
                            }
                        }
                    } catch (keyError) {
                        console.warn(`[${new Date().toISOString()}] ⚠️ Error processing account key ${index}:`, keyError.message);
                    }
                });
            }

            if (transaction.message?.staticAccountKeys) {
                console.log(`[${new Date().toISOString()}] 🔍 Found ${transaction.message.staticAccountKeys.length} static account keys`);
                transaction.message.staticAccountKeys.forEach(key => {
                    try {
                        const keyString = typeof key === 'string' ? key : (key.toString ? key.toString() : null);
                        if (keyString && typeof keyString === 'string' && keyString.length >= 32 && !accountKeys.includes(keyString)) {
                            accountKeys.push(keyString);
                        }
                    } catch (keyError) {
                        console.warn(`[${new Date().toISOString()}] ⚠️ Error processing static account key:`, keyError.message);
                    }
                });
            }

            if (transaction.message?.addressTableLookups && Array.isArray(transaction.message.addressTableLookups)) {
                console.log(`[${new Date().toISOString()}] 🔍 Found ${transaction.message.addressTableLookups.length} address table lookups`);
                transaction.message.addressTableLookups.forEach(lookup => {
                    try {
                        if (lookup.accountKey) {
                            const keyString = typeof lookup.accountKey === 'string' ? lookup.accountKey : lookup.accountKey.toString();
                            if (keyString && !accountKeys.includes(keyString)) {
                                accountKeys.push(keyString);
                            }
                        }
                    } catch (lookupError) {
                        console.warn(`[${new Date().toISOString()}] ⚠️ Error processing address table lookup:`, lookupError.message);
                    }
                });
            }

            if (transaction.message?.instructions && Array.isArray(transaction.message.instructions)) {
                transaction.message.instructions.forEach((instruction, instrIndex) => {
                    try {
                        if (instruction.accounts && Array.isArray(instruction.accounts)) {
                            instruction.accounts.forEach(accountIndex => {
                                try {
                                    if (typeof accountIndex === 'number' && transaction.message.accountKeys?.[accountIndex]) {
                                        const key = transaction.message.accountKeys[accountIndex];
                                        const keyString = typeof key === 'string' ? key : (key.pubkey || key.toString());
                                        if (keyString && !accountKeys.includes(keyString)) {
                                            accountKeys.push(keyString);
                                        }
                                    }
                                } catch (accountError) {
                                }
                            });
                        }
                    } catch (instrError) {
                        console.warn(`[${new Date().toISOString()}] ⚠️ Error processing instruction ${instrIndex}:`, instrError.message);
                    }
                });
            }

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error extracting account keys:`, error.message);
        }
        
        const uniqueKeys = [...new Set(accountKeys)];
        console.log(`[${new Date().toISOString()}] 🔍 Extracted ${uniqueKeys.length} unique account keys`);
        
        if (uniqueKeys.length > 0) {
            console.log(`[${new Date().toISOString()}] 🔍 Sample keys: ${uniqueKeys.slice(0, 3).map(k => k.slice(0, 8)).join(', ')}...`);
        }
        
        return uniqueKeys;
    }

    extractBlockTime(transactionData) {
        try {
            let blockTime = null;
            
            if (transactionData.meta?.blockTime) {
                blockTime = transactionData.meta.blockTime;
            } else if (transactionData.blockTime) {
                blockTime = transactionData.blockTime;
            } else if (transactionData.transaction?.blockTime) {
                blockTime = transactionData.transaction.blockTime;
            }
            
            if (!blockTime) {
                console.warn(`[${new Date().toISOString()}] ⚠️ No block time found, using current time`);
                blockTime = Math.floor(Date.now() / 1000);
            }
            
            const currentTime = Math.floor(Date.now() / 1000);
            const timeDiff = Math.abs(currentTime - blockTime);
            
            if (timeDiff > 3600) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Block time seems unusual: ${timeDiff}s difference from current time`);
            }
            
            return blockTime;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error extracting block time:`, error.message);
            return Math.floor(Date.now() / 1000);
        }
    }

async updateSubscription() {
    if (!this.isStarted) {
        console.log(`[${new Date().toISOString()}] ⏭️ Cannot update subscription - service not started`);
        return;
    }

    try {
        console.log(`[${new Date().toISOString()}] 🔄 Updating gRPC subscription with current wallets`);
        
        if (this.stream) {
            console.log(`[${new Date().toISOString()}] 🔌 Closing existing gRPC stream...`);
            try {
                await this.stream.end();
            } catch (endError) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Error ending stream:`, endError.message);
            }
            this.stream = null;
        }

        await new Promise(resolve => setTimeout(resolve, 500));
        
        console.log(`[${new Date().toISOString()}] 🔄 Creating new gRPC subscription...`);
        await this.subscribeToTransactions();
        
        console.log(`[${new Date().toISOString()}] ✅ gRPC subscription updated successfully`);
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error updating subscription:`, error.message);
        console.error(`[${new Date().toISOString()}] 🔍 Error details:`, error.stack);
        
        await this.handleReconnect();
    }
}

    async subscribeToWallet(walletAddress) {
        try {
            this.monitoredWallets.add(walletAddress);
            console.log(`[${new Date().toISOString()}] ✅ Added wallet ${walletAddress.slice(0, 8)}... to gRPC monitoring (total: ${this.monitoredWallets.size})`);
            
            await this.updateSubscription();
            
            return { success: true };
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error adding wallet to gRPC monitoring:`, error.message);
            throw error;
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
            
            await this.updateSubscription();
            
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

    async switchGroup(groupId) {
        try {
            const startTime = Date.now();
            console.log(`[${new Date().toISOString()}] 🔄 Switching to group ${groupId || 'all'} in gRPC service`);
            this.activeGroupId = groupId;
            await this.loadMonitoredWallets();
            
            await this.updateSubscription();
            
            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] ✅ gRPC group switch completed in ${duration}ms: now monitoring ${this.monitoredWallets.size} wallets`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in gRPC switchGroup:`, error.message);
            throw error;
        }
    }

    async unsubscribeFromWallet(walletAddress) {
        try {
            this.monitoredWallets.delete(walletAddress);
            console.log(`[${new Date().toISOString()}] ✅ Removed wallet ${walletAddress.slice(0, 8)}... from gRPC monitoring (total: ${this.monitoredWallets.size})`);
            
            await this.updateSubscription();
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error removing wallet from gRPC monitoring:`, error.message);
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
            
            await this.updateSubscription();
            
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
                
                await this.updateSubscription();
                
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
                await this.client.close();
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