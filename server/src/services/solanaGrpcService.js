const YellowstoneGrpcClient = require('@triton-one/yellowstone-grpc').default;
const { Connection, PublicKey, Message, VersionedMessage, AddressLookupTableAccount } = require('@solana/web3.js');
const WalletMonitoringService = require('./monitoringService');
const Database = require('../database/connection');
const grpc = require('@grpc/grpc-js');

class SolanaGrpcService {
    constructor() {
        this.solanaRpc = process.env.SOLANA_RPC_URL || '';
        this.grpcUrl = process.env.GRPC_URL || 'http://45.134.108.254:10000';
        this.token = process.env.GRPC_TOKEN || '';
        this.connection = new Connection(this.solanaRpc, {
            commitment: 'confirmed',
            httpHeaders: { 'Connection': 'keep-alive' }
        });
        this.client = new YellowstoneGrpcClient(this.grpcUrl, this.token);
        this.monitoringService = new WalletMonitoringService();
        this.db = new Database();
        this.stream = null;
        this.pingInterval = null;
        this.monitoredWallets = new Set();
        this.blockTimes = new Map();
        this.reconnectInterval = 3000;
        this.maxReconnectAttempts = 20;
        this.reconnectAttempts = 0;
        this.isConnecting = false;
        this.messageCount = 0;
        this.isStarted = false;
        this.maxSubscriptions = 1000000;
        this.activeGroupId = null;
    }

    async start(groupId = null) {
        if (this.isStarted && this.activeGroupId === groupId) {
            console.log(`[${new Date().toISOString()}] 🔄 Global gRPC service already started${groupId ? ` for group ${groupId}` : ''}`);
            return;
        }
        console.log(`[${new Date().toISOString()}] 🚀 Starting Global Solana gRPC client for ${this.grpcUrl}${groupId ? `, group ${groupId}` : ''}`);
        this.isStarted = true;
        this.activeGroupId = groupId;
        try {
            await this.subscribeToWallets();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to start global gRPC service:`, error.message);
            this.isStarted = false;
            throw error;
        }
    }

    async connectStream() {
        if (this.isConnecting || this.stream) return;
        this.isConnecting = true;

        console.log(`[${new Date().toISOString()}] 🔌 Connecting to gRPC: ${this.grpcUrl}`);
        
        try {
            if (this.stream) {
                this.stream.cancel();
                this.stream = null;
            }

            const request = {
                transactions: {
                    alltxs: {
                        vote: false,
                        failed: false,
                        account_include: Array.from(this.monitoredWallets),
                    }
                }
            };

            this.stream = await this.client.subscribe();

            this.stream.on('data', this.handleUpdate.bind(this));
            this.stream.on('error', this.handleError.bind(this));
            this.stream.on('end', this.handleEnd.bind(this));

            console.log(`[${new Date().toISOString()}] 📤 Sending gRPC subscription request`);
            await new Promise((resolve, reject) => {
                this.stream.write(request, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });

            console.log(`[${new Date().toISOString()}] ✅ Connected to Global Solana gRPC`);

            this.pingInterval = setInterval(() => {
                if (this.stream) {
                    console.log(`[${new Date().toISOString()}] 📤 Sending ping to keep stream alive`);
                    this.stream.write({ ping: { id: 1 } });
                }
            }, 30000);

            this.reconnectAttempts = 0;
            this.isConnecting = false;
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to create gRPC connection:`, error.message);
            this.isConnecting = false;
            throw error;
        }
    }

    async handleUpdate(update) {
        this.messageCount++;
        try {
            if (update.transaction) {
                const { transaction, meta, signature, slot } = update.transaction;
                if (meta.err) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ Skipping failed transaction: ${signature}`);
                    return;
                }

                const messageBuffer = Buffer.from(transaction.message);
                const allKeys = await this.getAllAccountKeysFromMessage(messageBuffer, slot);

                const matchingWallets = Array.from(this.monitoredWallets).filter(w => allKeys.includes(w));

                if (matchingWallets.length === 0) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ No matching wallets for transaction ${signature}`);
                    return;
                }

                let blockTime = this.blockTimes.get(slot);
                if (!blockTime) {
                    blockTime = await this.connection.getBlockTime(slot) || Math.floor(Date.now() / 1000);
                    this.blockTimes.set(slot, blockTime);
                    if (this.blockTimes.size > 1000) {
                        const oldSlots = Array.from(this.blockTimes.keys()).sort().slice(0, 500);
                        oldSlots.forEach(s => this.blockTimes.delete(s));
                    }
                }

                const parsedAccountKeys = allKeys.map(key => ({
                    pubkey: new PublicKey(key)
                }));

                const tx = {
                    transaction: {
                        message: {
                            accountKeys: parsedAccountKeys
                        }
                    },
                    meta
                };

                for (const walletAddress of matchingWallets) {
                    console.log(`[${new Date().toISOString()}] 🔍 New transaction detected: ${signature} for wallet ${walletAddress.slice(0,8)}...`);
                    
                    const wallet = await this.db.getWalletByAddress(walletAddress);
                    if (!wallet) {
                        console.warn(`[${new Date().toISOString()}] ⚠️ Wallet ${walletAddress} not found in database`);
                        continue;
                    }
                    
                    console.log(`[${new Date().toISOString()}] 📝 Processing transaction for wallet ${walletAddress.slice(0,8)}... (group: ${wallet.group_id || 'none'})`);
                    
                    await this.monitoringService.processWebhookMessage({
                        signature,
                        walletAddress,
                        blockTime,
                        tx
                    });
                }
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error handling gRPC update:`, error.message);
        }
    }

    async getAllAccountKeysFromMessage(messageBuffer, slot) {
        const version = messageBuffer[0];
        let message;
        let keys;

        if ((version & 0x80) === 0) {
            message = VersionedMessage.deserialize(messageBuffer);
            keys = message.staticAccountKeys;
            if (message.addressTableLookups.length > 0) {
                const lookedUpKeys = [];
                for (const lookup of message.addressTableLookups) {
                    const accountInfo = await this.connection.getAccountInfo(lookup.accountKey, 'confirmed');
                    if (accountInfo) {
                        const state = AddressLookupTableAccount.deserialize(accountInfo.data);
                        const writable = lookup.writableIndexes.map(idx => state.addresses[idx] || null).filter(Boolean);
                        const readonly = lookup.readonlyIndexes.map(idx => state.addresses[idx] || null).filter(Boolean);
                        lookedUpKeys.push(...writable, ...readonly);
                    }
                }
                keys = [...keys, ...lookedUpKeys];
            }
        } else {
            message = Message.from(messageBuffer);
            keys = message.accountKeys;
        }

        return keys.map(k => k.toBase58());
    }

    handleError(error) {
        console.error(`[${new Date().toISOString()}] ❌ gRPC error:`, error.message);
        this.handleReconnect();
    }

    handleEnd() {
        console.log(`[${new Date().toISOString()}] 🔌 gRPC stream ended`);
        this.isConnecting = false;
        if (this.isStarted) this.handleReconnect();
    }

async subscribeToWalletsBatch(walletAddresses) {
    if (!walletAddresses || walletAddresses.length === 0) return;
    
    const startTime = Date.now();
    const results = {
        successful: 0,
        failed: 0,
        errors: []
    };

    walletAddresses.forEach(walletAddress => {
        if (!walletAddress || typeof walletAddress !== 'string') {
            results.errors.push({ address: walletAddress, error: 'Invalid or undefined wallet address' });
            results.failed++;
            return;
        }

        try {
            new PublicKey(walletAddress); 
            if (this.monitoredWallets.has(walletAddress)) {
                results.successful++;
                return;
            }

            if (this.monitoredWallets.size >= this.maxSubscriptions) {
                results.errors.push({ address: walletAddress, error: `Maximum global subscription limit of ${this.maxSubscriptions} reached` });
                results.failed++;
                return;
            }

            this.monitoredWallets.add(walletAddress);
            results.successful++;
        } catch (error) {
            results.errors.push({ address: walletAddress, error: `Invalid Solana public key: ${error.message}` });
            results.failed++;
        }
    });

    if (results.successful > 0) {
        await this.restartStream();
    } else {
        console.log(`[${new Date().toISOString()}] ⚠️ No valid wallets to subscribe, skipping stream restart`);
    }

    const duration = Date.now() - startTime;
    console.log(`[${new Date().toISOString()}] ✅ Global batch subscription completed in ${duration}ms:`);
    console.log(`  - Successful: ${results.successful}`);
    console.log(`  - Failed: ${results.failed}`);
    console.log(`  - Total active subscriptions: ${this.monitoredWallets.size}`);
    if (results.errors.length > 0) {
        console.log(`  - Errors:`, results.errors);
    }

    return results;
}

async restartStream() {
    try {
        console.log(`[${new Date().toISOString()}] 🔄 Restarting gRPC stream`);
        if (this.stream) {
            this.stream.cancel();
            this.stream = null;
        }
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
            this.pingInterval = null;
        }
        await this.connectStream();
        console.log(`[${new Date().toISOString()}] ✅ gRPC stream restarted successfully`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Failed to restart gRPC stream:`, error.message);
        throw error;
    }
}

    async unsubscribeFromWalletsBatch(walletAddresses) {
        if (!walletAddresses || walletAddresses.length === 0) return;
    
        const startTime = Date.now();
    
        const results = {
            successful: 0,
            failed: 0,
            errors: []
        };
    
        walletAddresses.forEach(walletAddress => {
            if (this.monitoredWallets.has(walletAddress)) {
                this.monitoredWallets.delete(walletAddress);
                results.successful++;
            } else {
                results.successful++;
            }
        });
    
        await this.restartStream();
    
        const duration = Date.now() - startTime;
        console.log(`[${new Date().toISOString()}] ✅ Global batch unsubscription completed in ${duration}ms: ${results.successful} successful, ${results.failed} failed`);
        console.log(`[${new Date().toISOString()}] 📊 Remaining active subscriptions: ${this.monitoredWallets.size}`);
    
        return results;
    }

    async subscribeToWallets() {
        this.monitoredWallets.clear();
        
        const wallets = await this.db.getActiveWallets(this.activeGroupId);
        
        console.log(`[${new Date().toISOString()}] 📊 Found ${wallets.length} wallets to monitor${this.activeGroupId ? ` in group ${this.activeGroupId}` : ' (all groups)'}`);
        
        if (wallets.length === 0) {
            console.log(`[${new Date().toISOString()}] ℹ️ No wallets found for monitoring${this.activeGroupId ? ` in group ${this.activeGroupId}` : ''}`);
            return { successful: 0, failed: 0, errors: [] };
        }
        
        if (wallets.length > this.maxSubscriptions) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Wallet count (${wallets.length}) exceeds maximum (${this.maxSubscriptions}), truncating`);
            wallets.length = this.maxSubscriptions;
        }
        
        if (wallets.length > 0) {
            console.log(`[${new Date().toISOString()}] 🔍 Sample wallets to subscribe (group: ${this.activeGroupId || 'all'}):`);
            wallets.slice(0, 5).forEach(wallet => {
                console.log(`  - ${wallet.address.slice(0, 8)}... (group: ${wallet.group_id || 'none'})`);
            });
        }
    
        const walletAddresses = wallets.map(w => w.address);
        const results = await this.subscribeToWalletsBatch(walletAddresses);
    
        console.log(`[${new Date().toISOString()}] 🎉 Subscription summary for group ${this.activeGroupId || 'all'}:`);
        console.log(`  - Total wallets: ${wallets.length}`);
        console.log(`  - Successful subscriptions: ${results.successful}`);
        console.log(`  - Failed subscriptions: ${results.failed}`);
        console.log(`  - Active subscriptions: ${this.monitoredWallets.size}`);
        console.log(`  - Active group ID: ${this.activeGroupId || 'none'}`);
    
        return results;
    }

    async unsubscribeFromWallet(walletAddress) {
        if (!this.monitoredWallets.has(walletAddress)) return;

        this.monitoredWallets.delete(walletAddress);
        await this.restartStream();
        console.log(`[${new Date().toISOString()}] ✅ Unsubscribed from global logs for ${walletAddress.slice(0, 8)}...`);
    }

    async subscribeToWallet(walletAddress) {
        if (this.monitoredWallets.size >= this.maxSubscriptions) {
            throw new Error(`Maximum global subscription limit of ${this.maxSubscriptions} reached`);
        }
        
        if (this.monitoredWallets.has(walletAddress)) {
            console.log(`[${new Date().toISOString()}] ℹ️ Global wallet ${walletAddress.slice(0, 8)}... already subscribed`);
            return;
        }

        this.monitoredWallets.add(walletAddress);
        await this.restartStream();
        console.log(`[${new Date().toISOString()}] ✅ Subscribed to global wallet ${walletAddress.slice(0, 8)}...`);
        
        return { success: true };
    }

    async removeAllWallets(groupId = null) {
        try {
            const startTime = Date.now();
            console.log(`[${new Date().toISOString()}] 🗑️ Starting complete wallet removal from gRPC service${groupId ? ` for group ${groupId}` : ''}`);
            
            const walletsToRemove = await this.db.getActiveWallets(groupId);
            const addressesToUnsubscribe = walletsToRemove.map(w => w.address);
            
            console.log(`[${new Date().toISOString()}] 📊 gRPC service found ${walletsToRemove.length} wallets to remove`);
            
            if (addressesToUnsubscribe.length > 0) {
                console.log(`[${new Date().toISOString()}] 🔌 Unsubscribing ${addressesToUnsubscribe.length} wallets from Solana gRPC...`);
                
                try {
                    const unsubscribeResults = await this.unsubscribeFromWalletsBatch(addressesToUnsubscribe);
                    console.log(`[${new Date().toISOString()}] ✅ Solana gRPC unsubscription completed: ${unsubscribeResults.successful} successful, ${unsubscribeResults.failed} failed`);
                } catch (unsubError) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ Some Solana unsubscriptions failed: ${unsubError.message}`);
                    
                    addressesToUnsubscribe.forEach(address => {
                        this.monitoredWallets.delete(address);
                    });
                    console.log(`[${new Date().toISOString()}] 🧹 Manual cleanup of ${addressesToUnsubscribe.length} local subscriptions completed`);
                }
            }
            
            console.log(`[${new Date().toISOString()}] 🗄️ Starting database removal...`);
            await this.monitoringService.removeAllWallets(groupId);
            console.log(`[${new Date().toISOString()}] ✅ Database removal completed`);
            
            const shouldResubscribe = this.isStarted && (
                (groupId && groupId === this.activeGroupId) ||
                (!groupId)
            );
            
            if (shouldResubscribe) {
                console.log(`[${new Date().toISOString()}] 🔄 Resubscribing to remaining wallets after group deletion...`);
                
                try {
                    await this.subscribeToWallets();
                    console.log(`[${new Date().toISOString()}] ✅ Resubscription completed: ${this.monitoredWallets.size} active subscriptions`);
                } catch (resubError) {
                    console.error(`[${new Date().toISOString()}] ❌ Resubscription failed: ${resubError.message}`);
                }
            }
            
            const duration = Date.now() - startTime;
            const finalReport = {
                walletsRemoved: walletsToRemove.length,
                addressesUnsubscribed: addressesToUnsubscribe.length,
                remainingSubscriptions: this.monitoredWallets.size,
                groupId: groupId,
                resubscribed: shouldResubscribe,
                processingTime: `${duration}ms`
            };
            
            console.log(`[${new Date().toISOString()}] 🎉 Complete gRPC wallet removal finished in ${duration}ms:`, finalReport);
            
            return {
                success: true,
                message: `gRPC service: removed ${walletsToRemove.length} wallets, unsubscribed from Solana node, ${this.monitoredWallets.size} subscriptions remaining`,
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
            console.log(`[${new Date().toISOString()}] 🔄 Switching to global group ${groupId || 'all'}`);

            this.monitoredWallets.clear();

            this.activeGroupId = groupId;
            await this.subscribeToWallets();

            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] ✅ Global group switch completed in ${duration}ms: now monitoring ${this.monitoredWallets.size} wallets`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in global switchGroup:`, error.message);
            throw error;
        }
    }

    async handleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] ❌ Max reconnect attempts reached for global service`);
            this.isStarted = false;
            return;
        }

        this.reconnectAttempts++;
        const backoffDelay = this.reconnectInterval * Math.pow(2, this.reconnectAttempts - 1);
        console.log(`[${new Date().toISOString()}] 🔄 Reconnecting global service (${this.reconnectAttempts}/${this.maxReconnectAttempts}) in ${backoffDelay}ms`);

        await new Promise(resolve => setTimeout(resolve, backoffDelay));
        try {
            await this.connectStream();
            await this.subscribeToWallets();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Global reconnect failed:`, error.message);
        }
    }

    getStatus() {
        return {
            isConnected: !!this.stream,
            isStarted: this.isStarted,
            activeGroupId: this.activeGroupId,
            subscriptions: this.monitoredWallets.size,
            messageCount: this.messageCount,
            reconnectAttempts: this.reconnectAttempts,
            grpcUrl: this.grpcUrl,
            rpcUrl: this.solanaRpc,
            mode: 'global'
        };
    }

    async stop() {
        this.isStarted = false;
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
            this.pingInterval = null;
        }
        if (this.stream) {
            this.stream.cancel();
            this.stream = null;
        }
        this.monitoredWallets.clear();
        console.log(`[${new Date().toISOString()}] ⏹️ Global gRPC client stopped`);
    }

    async shutdown() {
        await this.stop();
        await this.db.close().catch(() => {});
        console.log(`[${new Date().toISOString()}] ✅ Global gRPC service shutdown complete`);
    }
}

module.exports = SolanaGrpcService;