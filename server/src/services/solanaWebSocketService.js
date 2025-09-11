const WebSocket = require('ws');
const { Connection } = require('@solana/web3.js');
const WalletMonitoringService = require('./monitoringService');
const Database = require('../database/connection');

const WS_READY_STATE_OPEN = 1;

class SolanaWebSocketService {
    constructor() {
        this.solanaRpc = process.env.SOLANA_RPC_URL || '';
        this.wsUrl = process.env.WEBHOOK_URL || '';
        this.connection = new Connection(this.solanaRpc, {
            commitment: 'confirmed',
            wsEndpoint: this.wsUrl,
            httpHeaders: { 'Connection': 'keep-alive' }
        });
        this.monitoringService = new WalletMonitoringService();
        this.db = new Database();
        this.ws = null;
        this.subscriptions = new Map();
        this.reconnectInterval = 3000;
        this.maxReconnectAttempts = 20;
        this.reconnectAttempts = 0;
        this.isConnecting = false;
        this.messageId = 0;
        this.pendingRequests = new Map();
        this.messageCount = 0;
        this.isStarted = false;
        this.batchSize = 400;
        this.maxSubscriptions = 1000000;
        this.activeGroupId = null;
    }

    async start(groupId = null) {
        if (this.isStarted && this.activeGroupId === groupId) {
            console.log(`[${new Date().toISOString()}] 🔄 Global WebSocket service already started${groupId ? ` for group ${groupId}` : ''}`);
            return;
        }
        console.log(`[${new Date().toISOString()}] 🚀 Starting Global Solana WebSocket client for ${this.wsUrl}${groupId ? `, group ${groupId}` : ''}`);
        this.isStarted = true;
        this.activeGroupId = groupId;
        try {
            await this.connect();
            await this.subscribeToWallets();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to start global WebSocket service:`, error.message);
            this.isStarted = false;
            throw error;
        }
    }

    async connect() {
        if (this.isConnecting || (this.ws && this.ws.readyState === WS_READY_STATE_OPEN)) return;
        this.isConnecting = true;

        console.log(`[${new Date().toISOString()}] 🔌 Connecting to WebSocket: ${this.wsUrl}`);
        
        try {
            this.ws?.close();
            this.ws = new WebSocket(this.wsUrl, {
                handshakeTimeout: 10000,
                perMessageDeflate: false,
            });

            this.ws.on('open', () => {
                console.log(`[${new Date().toISOString()}] ✅ Connected to Global Solana WebSocket`);
                this.reconnectAttempts = 0;
                this.isConnecting = false;
            });

            this.ws.on('message', async (data) => {
                this.messageCount++;
                try {
                    const message = JSON.parse(data.toString());
                    await this.handleMessage(message);
                } catch (error) {
                    console.error(`[${new Date().toISOString()}] ❌ Error parsing WebSocket message:`, error.message);
                }
            });

            this.ws.on('error', (error) => {
                console.error(`[${new Date().toISOString()}] ❌ WebSocket error:`, error.message);
                this.handleReconnect();
            });

            this.ws.on('close', (code, reason) => {
                console.log(`[${new Date().toISOString()}] 🔌 WebSocket closed. Code: ${code}, Reason: ${reason.toString()}`);
                this.isConnecting = false;
                if (this.isStarted) this.handleReconnect();
            });

            this.ws.on('ping', (data) => {
                this.ws.pong(data);
            });

            await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => reject(new Error('Connection timeout')), 10000);
                this.ws.on('open', () => { clearTimeout(timeout); resolve(); });
                this.ws.on('error', (error) => { clearTimeout(timeout); reject(error); });
            });
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to create WebSocket connection:`, error.message);
            this.isConnecting = false;
            throw error;
        }
    }

    async handleMessage(message) {
        if (!message || typeof message !== 'object') {
            console.warn(`[${new Date().toISOString()}] ⚠️ Invalid message format`);
            return;
        }

        if (message.id && this.pendingRequests.has(message.id)) {
            const { resolve, reject, type } = this.pendingRequests.get(message.id);
            this.pendingRequests.delete(message.id);
            if (message.error) {
                reject(new Error(message.error.message));
            } else {
                resolve(message.result);
            }
            return;
        }

        if (message.method === 'logsNotification') {
            await this.handleLogsNotification(message.params);
        }
    }

    async handleLogsNotification(params) {
        if (!params?.result || !params.subscription) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Invalid logs notification params`);
            return;
        }
        const { result, subscription } = params;
        const walletAddress = this.findWalletBySubscription(subscription);
        if (!walletAddress) {
            console.warn(`[${new Date().toISOString()}] ⚠️ No wallet found for subscription ${subscription}`);
            return;
        }
    
        if (result.value && result.value.signature) {
            console.log(`[${new Date().toISOString()}] 🔍 New global transaction detected: ${result.value.signature}`);
            const wallet = await this.db.getWalletByAddress(walletAddress);
            if (!wallet) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Wallet ${walletAddress} not found in global database`);
                return;
            }
            
            if (this.activeGroupId && wallet.group_id !== this.activeGroupId) {
                console.log(`[${new Date().toISOString()}] ℹ️ Skipping transaction for wallet ${walletAddress} (not in active group ${this.activeGroupId})`);
                return;
            }
            
            await this.monitoringService.processWebhookMessage({
                signature: result.value.signature,
                walletAddress,
                blockTime: result.value.timestamp || Math.floor(Date.now() / 1000),
                groupId: wallet.group_id
            });
        }
    }

    findWalletBySubscription(subscriptionId) {
        return Array.from(this.subscriptions.entries()).find(([_, subData]) => subData.logs === subscriptionId)?.[0] || null;
    }

    async subscribeToWalletsBatch(walletAddresses, batchSize = 100) {
        if (!walletAddresses || walletAddresses.length === 0) return;
        
        if (!this.ws || this.ws.readyState !== WS_READY_STATE_OPEN) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Cannot batch subscribe - WebSocket not connected`);
            return;
        }

        const startTime = Date.now();

        const results = {
            successful: 0,
            failed: 0,
            errors: []
        };

        for (let i = 0; i < walletAddresses.length; i += batchSize) {
            const batch = walletAddresses.slice(i, i + batchSize);
            
            console.log(`[${new Date().toISOString()}] 📦 Processing global batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(walletAddresses.length / batchSize)} (${batch.length} wallets)`);

            const batchPromises = batch.map(async (walletAddress) => {
                try {
                    if (this.subscriptions.has(walletAddress)) {
                        console.log(`[${new Date().toISOString()}] ⏭️ Wallet ${walletAddress.slice(0, 8)}... already subscribed globally`);
                        return { success: true, address: walletAddress, action: 'already_subscribed' };
                    }

                    if (this.subscriptions.size >= this.maxSubscriptions) {
                        throw new Error(`Maximum global subscription limit of ${this.maxSubscriptions} reached`);
                    }

                    const logsSubscriptionId = await this.sendRequest('logsSubscribe', [
                        { mentions: [walletAddress] },
                        { commitment: 'confirmed' },
                    ], 'logsSubscribe');

                    this.subscriptions.set(walletAddress, { logs: logsSubscriptionId });
                    results.successful++;
                    
                    return { success: true, address: walletAddress, subscriptionId: logsSubscriptionId };

                } catch (error) {
                    results.failed++;
                    results.errors.push({ address: walletAddress, error: error.message });
                    console.error(`[${new Date().toISOString()}] ❌ Failed to subscribe to ${walletAddress.slice(0, 8)}...: ${error.message}`);
                    return { success: false, address: walletAddress, error: error.message };
                }
            });

            await Promise.all(batchPromises);

            if (i + batchSize < walletAddresses.length) {
                await new Promise(resolve => setTimeout(resolve, 50));
            }
        }

        const duration = Date.now() - startTime;
        const walletsPerSecond = Math.round((results.successful / duration) * 1000);

        console.log(`[${new Date().toISOString()}] ✅ Global batch subscription completed in ${duration}ms:`);
        console.log(`  - Successful: ${results.successful}`);
        console.log(`  - Failed: ${results.failed}`);
        console.log(`  - Performance: ${walletsPerSecond} subscriptions/second`);
        console.log(`  - Total active subscriptions: ${this.subscriptions.size}`);

        return results;
    }

    async unsubscribeFromWalletsBatch(walletAddresses, batchSize = 100) {
        if (!walletAddresses || walletAddresses.length === 0) return;
    
        const startTime = Date.now();
    
        const results = {
            successful: 0,
            failed: 0,
            errors: []
        };
    
        for (let i = 0; i < walletAddresses.length; i += batchSize) {
            const batch = walletAddresses.slice(i, i + batchSize);
    
            const batchPromises = batch.map(async (walletAddress) => {
                try {
                    const subData = this.subscriptions.get(walletAddress);
                    
                    if (!subData?.logs) {
                        this.subscriptions.delete(walletAddress);
                        results.successful++;
                        return { success: true, address: walletAddress, action: 'not_subscribed' };
                    }
    
                    if (this.ws && this.ws.readyState === WS_READY_STATE_OPEN) {
                        try {
                            await this.sendRequest('logsUnsubscribe', [subData.logs], 'logsUnsubscribe');
                            console.log(`[${new Date().toISOString()}] ✅ Successfully unsubscribed from ${walletAddress.slice(0, 8)}... globally`);
                        } catch (wsError) {
                            console.warn(`[${new Date().toISOString()}] ⚠️ WebSocket unsubscribe failed for ${walletAddress.slice(0, 8)}...: ${wsError.message}`);
                        }
                    } else {
                        console.warn(`[${new Date().toISOString()}] ⚠️ WebSocket not connected, skipping network unsubscribe for ${walletAddress.slice(0, 8)}...`);
                    }
    
                    this.subscriptions.delete(walletAddress);
                    results.successful++;
                    
                    return { success: true, address: walletAddress };
    
                } catch (error) {
                    results.failed++;
                    results.errors.push({ address: walletAddress, error: error.message });
                    console.error(`[${new Date().toISOString()}] ❌ Failed to unsubscribe from ${walletAddress.slice(0, 8)}...: ${error.message}`);
                    
                    this.subscriptions.delete(walletAddress);
                    
                    return { success: false, address: walletAddress, error: error.message };
                }
            });
    
            await Promise.all(batchPromises);
    
            if (i + batchSize < walletAddresses.length) {
                await new Promise(resolve => setTimeout(resolve, 50));
            }
        }
    
        const duration = Date.now() - startTime;
        console.log(`[${new Date().toISOString()}] ✅ Global batch unsubscription completed in ${duration}ms: ${results.successful} successful, ${results.failed} failed`);
        console.log(`[${new Date().toISOString()}] 📊 Remaining active subscriptions: ${this.subscriptions.size}`);
    
        return results;
    }

    async subscribeToWallets() {
        this.subscriptions.clear();
        const wallets = await this.db.getActiveWallets(this.activeGroupId);
        
        if (wallets.length === 0) {
            return;
        }
        
        if (wallets.length > this.maxSubscriptions) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Wallet count (${wallets.length}) exceeds maximum (${this.maxSubscriptions}), truncating`);
            wallets.length = this.maxSubscriptions;
        }
        
        
        if (wallets.length > 0) {
            console.log(`[${new Date().toISOString()}] 🔍 Sample global wallets to subscribe:`);
            wallets.slice(0, 3).forEach(wallet => {
                console.log(`  - ${wallet.address.slice(0, 8)}... (group: ${wallet.group_id})`);
            });
        }

        const walletAddresses = wallets.map(w => w.address);
        const results = await this.subscribeToWalletsBatch(walletAddresses, 150);

        console.log(`[${new Date().toISOString()}] 🎉 Global subscription summary:`);
        console.log(`  - Total wallets: ${wallets.length}`);
        console.log(`  - Successful subscriptions: ${results.successful}`);
        console.log(`  - Failed subscriptions: ${results.failed}`);
        console.log(`  - Active subscriptions: ${this.subscriptions.size}`);

        return results;
    }

    async unsubscribeFromWallet(walletAddress) {
        const subData = this.subscriptions.get(walletAddress);
        if (!subData?.logs || !this.ws || this.ws.readyState !== WS_READY_STATE_OPEN) return;

        try {
            await this.sendRequest('logsUnsubscribe', [subData.logs], 'logsUnsubscribe');
            console.log(`[${new Date().toISOString()}] ✅ Unsubscribed from global logs for ${walletAddress.slice(0, 8)}...`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error unsubscribing from global ${walletAddress}:`, error.message);
        }
        this.subscriptions.delete(walletAddress);
    }

    async subscribeToWallet(walletAddress) {
        if (this.subscriptions.size >= this.maxSubscriptions) {
            throw new Error(`Maximum global subscription limit of ${this.maxSubscriptions} reached`);
        }
        
        if (!this.ws || this.ws.readyState !== WS_READY_STATE_OPEN) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Cannot subscribe to global wallet ${walletAddress.slice(0, 8)}... - WebSocket not connected`);
            return;
        }
        
        try {
            if (this.subscriptions.has(walletAddress)) {
                console.log(`[${new Date().toISOString()}] ℹ️ Global wallet ${walletAddress.slice(0, 8)}... already subscribed`);
                return;
            }

            const logsSubscriptionId = await this.sendRequest('logsSubscribe', [
                { mentions: [walletAddress] },
                { commitment: 'confirmed' },
            ], 'logsSubscribe');
            
            this.subscriptions.set(walletAddress, { logs: logsSubscriptionId });
            console.log(`[${new Date().toISOString()}] ✅ Subscribed to global wallet ${walletAddress.slice(0, 8)}... (logs: ${logsSubscriptionId})`);
            
            return { success: true, subscriptionId: logsSubscriptionId };
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error subscribing to global wallet ${walletAddress}:`, error.message);
            throw error;
        }
    }

    async removeAllWallets(groupId = null) {
        try {
            const startTime = Date.now();
            console.log(`[${new Date().toISOString()}] 🗑️ Starting complete wallet removal from WebSocket service${groupId ? ` for group ${groupId}` : ''}`);
            
            const walletsToRemove = await this.db.getActiveWallets(groupId);
            const addressesToUnsubscribe = walletsToRemove.map(w => w.address);
            
            console.log(`[${new Date().toISOString()}] 📊 WebSocket service found ${walletsToRemove.length} wallets to remove`);
            
            if (addressesToUnsubscribe.length > 0) {
                console.log(`[${new Date().toISOString()}] 🔌 Unsubscribing ${addressesToUnsubscribe.length} wallets from Solana node...`);
                
                try {
                    const unsubscribeResults = await this.unsubscribeFromWalletsBatch(addressesToUnsubscribe, 200);
                    console.log(`[${new Date().toISOString()}] ✅ Solana node unsubscription completed: ${unsubscribeResults.successful} successful, ${unsubscribeResults.failed} failed`);
                } catch (unsubError) {
                    console.warn(`[${new Date().toISOString()}] ⚠️ Some Solana unsubscriptions failed: ${unsubError.message}`);
                    
                    addressesToUnsubscribe.forEach(address => {
                        this.subscriptions.delete(address);
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
                    console.log(`[${new Date().toISOString()}] ✅ Resubscription completed: ${this.subscriptions.size} active subscriptions`);
                } catch (resubError) {
                    console.error(`[${new Date().toISOString()}] ❌ Resubscription failed: ${resubError.message}`);
                }
            }
            
            const duration = Date.now() - startTime;
            const finalReport = {
                walletsRemoved: walletsToRemove.length,
                addressesUnsubscribed: addressesToUnsubscribe.length,
                remainingSubscriptions: this.subscriptions.size,
                groupId: groupId,
                resubscribed: shouldResubscribe,
                processingTime: `${duration}ms`
            };
            
            console.log(`[${new Date().toISOString()}] 🎉 Complete WebSocket wallet removal finished in ${duration}ms:`, finalReport);
            
            return {
                success: true,
                message: `WebSocket service: removed ${walletsToRemove.length} wallets, unsubscribed from Solana node, ${this.subscriptions.size} subscriptions remaining`,
                details: finalReport
            };
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in WebSocket removeAllWallets:`, error.message);
            throw error;
        }
    }

    async switchGroup(groupId) {
        try {
            const startTime = Date.now();
            console.log(`[${new Date().toISOString()}] 🔄 Switching to global group ${groupId || 'all'}`);

            if (this.subscriptions.size > 0) {
                const currentAddresses = Array.from(this.subscriptions.keys());
                await this.unsubscribeFromWalletsBatch(currentAddresses);
            }

            this.activeGroupId = groupId;
            await this.subscribeToWallets();

            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] ✅ Global group switch completed in ${duration}ms: now monitoring ${this.subscriptions.size} wallets`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in global switchGroup:`, error.message);
            throw error;
        }
    }

    sendRequest(method, params, type) {
        return new Promise((resolve, reject) => {
            if (!this.ws || this.ws.readyState !== WS_READY_STATE_OPEN) {
                reject(new Error('WebSocket is not connected'));
                return;
            }

            const id = ++this.messageId;
            this.pendingRequests.set(id, { resolve, reject, type });
            this.ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));

            setTimeout(() => {
                if (this.pendingRequests.has(id)) {
                    this.pendingRequests.delete(id);
                    reject(new Error(`Request ${type} (id: ${id}) timed out`));
                }
            }, 60000);
        });
    }

    async handleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`[${new Date().toISOString()}] ❌ Max reconnect attempts reached for global service`);
            this.isStarted = false;
            return;
        }

        this.reconnectAttempts++;
        console.log(`[${new Date().toISOString()}] 🔄 Reconnecting global service (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));
        try {
            await this.connect();
            await this.subscribeToWallets();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Global reconnect failed:`, error.message);
        }
    }

    getStatus() {
        return {
            isConnected: this.ws && this.ws.readyState === WS_READY_STATE_OPEN,
            isStarted: this.isStarted,
            activeGroupId: this.activeGroupId,
            subscriptions: this.subscriptions.size,
            messageCount: this.messageCount,
            reconnectAttempts: this.reconnectAttempts,
            wsUrl: this.wsUrl,
            rpcUrl: this.solanaRpc,
            mode: 'global'
        };
    }

    async stop() {
        this.isStarted = false;
        for (const walletAddress of this.subscriptions.keys()) {
            await this.unsubscribeFromWallet(walletAddress).catch(() => {});
        }
        if (this.ws) {
            this.ws.removeAllListeners();
            this.ws.close();
            this.ws = null;
        }
        console.log(`[${new Date().toISOString()}] ⏹️ Global WebSocket client stopped`);
    }

    async shutdown() {
        await this.stop();
        await this.db.close().catch(() => {});
        console.log(`[${new Date().toISOString()}] ✅ Global WebSocket service shutdown complete`);
    }
}

module.exports = SolanaWebSocketService;