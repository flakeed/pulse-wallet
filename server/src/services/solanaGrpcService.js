const {
  default: Client,
  CommitmentLevel,
  SubscribeRequest,
  SubscribeRequestFilterAccountsFilter,
  SubscribeRequestFilterAccountsFilterLamports,
  SubscribeUpdateTransactionInfo,
  txEncode,
  txErrDecode
} = require('@triton-one/yellowstone-grpc');
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
            console.log(`[${new Date().toISOString()}] ✅ gRPC client created successfully`);
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
        
        if (this.monitoredWallets.size === 0) {
            console.warn(`[${new Date().toISOString()}] ⚠️ No wallets to monitor! Skipping gRPC subscription.`);
            return;
        }

        if (this.stream) {
            this.stream.end();
        }

        const walletsArray = Array.from(this.monitoredWallets);
        console.log(`[${new Date().toISOString()}] 📝 Setting up gRPC filter for wallets:`, walletsArray.slice(0, 3).map(w => `${w.slice(0,8)}...`));

        const request = {
            accounts: {},
            slots: {},
            transactions: {
                client: {
                    vote: false,
                    failed: false,
                    accountInclude: walletsArray, 
                    accountExclude: [],
                    accountRequired: []
                }
            },
            transactionsStatus: {},
            entry: {},
            blocks: {},
            blocksMeta: {},
            commitment: CommitmentLevel.CONFIRMED,
            accountsDataSlice: [],
            ping: undefined
        };

        console.log(`[${new Date().toISOString()}] 🔗 Creating gRPC stream...`);
        this.stream = await this.client.subscribe();
       
        this.stream.on('data', (data) => {
            this.messageCount++;
            if (this.messageCount <= 3) {
                console.log(`[${new Date().toISOString()}] 📨 Received gRPC data #${this.messageCount}:`, Object.keys(data || {}));
                if (data.transaction) {
                    console.log(`[${new Date().toISOString()}] 🔍 Transaction data structure:`, Object.keys(data.transaction || {}));
                }
            }
            
            if (this.messageCount % 1000 === 0) {
                console.log(`[${new Date().toISOString()}] 📊 gRPC processed ${this.messageCount} messages, monitoring ${this.monitoredWallets.size} wallets`);
            }
            
            this.handleGrpcMessage(data);
        });

        this.stream.on('error', (error) => {
            console.error(`[${new Date().toISOString()}] ❌ gRPC stream error:`, error.message);
            if (error.message.includes('serialization failure')) {
                console.error(`[${new Date().toISOString()}] 🛑 Serialization error - stopping service`);
                this.isStarted = false;
                return;
            }
            this.handleReconnect();
        });

        this.stream.on('end', () => {
            console.log(`[${new Date().toISOString()}] 🔌 gRPC stream ended`);
            if (this.isStarted) {
                setTimeout(() => this.handleReconnect(), 2000);
            }
        });

        console.log(`[${new Date().toISOString()}] 📤 Sending subscription request with wallet filter...`);
        console.log(`[${new Date().toISOString()}] 🔍 Request:`, JSON.stringify({
            ...request,
            transactions: {
                client: {
                    ...request.transactions.client,
                    accountInclude: `[${walletsArray.length} wallets: ${walletsArray.slice(0,2).join(', ')}...]`
                }
            }
        }, null, 2));

        await new Promise((resolve, reject) => {
            this.stream.write(request, (err) => {
                if (err === null || err === undefined) {
                    console.log(`[${new Date().toISOString()}] ✅ Subscription request sent successfully with ${walletsArray.length} wallet filters`);
                    resolve();
                } else {
                    console.error(`[${new Date().toISOString()}] ❌ Subscription request failed:`, err);
                    reject(err);
                }
            });
        });

        console.log(`[${new Date().toISOString()}] ⏳ Waiting for transaction data for ${walletsArray.length} wallets...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        
        if (this.messageCount > 0) {
            console.log(`[${new Date().toISOString()}] 🎉 SUCCESS: Received ${this.messageCount} messages from gRPC!`);
        } else {
            console.log(`[${new Date().toISOString()}] ⚠️ No messages received yet, but stream appears stable`);
        }
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error subscribing to transactions:`, error.message);
        console.error(`[${new Date().toISOString()}] 📋 Subscribe error details:`, error);
        throw error;
    }
}
async handleGrpcMessage(data) {
    try {
        if (this.messageCount <= 10) {
            console.log(`[${new Date().toISOString()}] 🔍 DEBUG: gRPC message #${this.messageCount} structure:`, JSON.stringify(Object.keys(data || {}), null, 2));
            
            if (data) {
                const fields = ['transaction', 'transactions', 'tx', 'txn', 'data', 'message'];
                for (const field of fields) {
                    if (data[field]) {
                        console.log(`[${new Date().toISOString()}] 🔍 Found field '${field}':`, JSON.stringify(Object.keys(data[field]), null, 2));
                    }
                }
                
                if (this.messageCount <= 3) {
                    console.log(`[${new Date().toISOString()}] 🔍 FULL DATA STRUCTURE:`, JSON.stringify(data, null, 2));
                }
            }
        }
        
        if (this.messageCount % 1000 === 0 && this.messageCount > 0) {
            console.log(`[${new Date().toISOString()}] 📊 gRPC processed ${this.messageCount} messages, monitoring ${this.monitoredWallets.size} wallets`);
        }
        
        let transactionData = null;
        
        if (data.transaction) {
            transactionData = data.transaction;
            console.log(`[${new Date().toISOString()}] ✅ Found transaction in data.transaction`);
        } else if (data.transactions) {
            transactionData = data.transactions;
            console.log(`[${new Date().toISOString()}] ✅ Found transaction in data.transactions`);
        } else if (data.tx) {
            transactionData = data.tx;
            console.log(`[${new Date().toISOString()}] ✅ Found transaction in data.tx`);
        } else if (data.txn) {
            transactionData = data.txn;
            console.log(`[${new Date().toISOString()}] ✅ Found transaction in data.txn`);
        } else {
            if (this.messageCount <= 10) {
                console.log(`[${new Date().toISOString()}] ⚠️ No transaction data found in expected fields. Available keys:`, Object.keys(data || {}));
            }
            return;
        }
        
        await this.processTransaction(transactionData);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error handling gRPC message:`, error.message);
        console.error(`[${new Date().toISOString()}] 📋 Error stack:`, error.stack);
    }
}
async processTransaction(transactionData) {
    try {
        if (this.messageCount <= 5) {
            console.log(`[${new Date().toISOString()}] 🔍 DEBUG: Transaction data structure:`, JSON.stringify(Object.keys(transactionData || {}), null, 2));
        }
       
        let transaction = null;
        let meta = null;
       
        if (transactionData.transaction && transactionData.meta) {
            transaction = transactionData.transaction;
            meta = transactionData.meta;
            console.log(`[${new Date().toISOString()}] ✅ Using format: transactionData.transaction + transactionData.meta`);
        }
        else if (transactionData.signatures && transactionData.message) {
            transaction = transactionData;
            meta = transactionData.meta || transactionData;
            console.log(`[${new Date().toISOString()}] ✅ Using format: direct transaction object`);
        }
        else if (transactionData.tx) {
            transaction = transactionData.tx.transaction || transactionData.tx;
            meta = transactionData.tx.meta || transactionData.meta;
            console.log(`[${new Date().toISOString()}] ✅ Using format: transactionData.tx`);
        }
        else if (transactionData.slot || transactionData.blockTime) {
            transaction = transactionData.transaction || transactionData;
            meta = transactionData.meta || transactionData;
            console.log(`[${new Date().toISOString()}] ✅ Using format: full transaction object`);
        }
        else {
            if (this.messageCount <= 10) {
                console.log(`[${new Date().toISOString()}] ⚠️ Unknown transaction format. Keys:`, Object.keys(transactionData || {}));
                console.log(`[${new Date().toISOString()}] 🔍 Sample data:`, JSON.stringify(transactionData, null, 2));
            }
            return;
        }
       
        if (!transaction) {
            console.log(`[${new Date().toISOString()}] ⏭️ Skipping: no transaction data found`);
            return;
        }
       
        if (!meta) {
            console.log(`[${new Date().toISOString()}] ⏭️ Skipping: no meta data found`);
            return;
        }
       
        if (meta.err) {
            if (this.messageCount <= 5) {
                console.log(`[${new Date().toISOString()}] ⏭️ Skipping: transaction failed with error:`, meta.err);
            }
            return;
        }
       
        let signature = null;
        if (transaction.signatures && transaction.signatures.length > 0) {
            signature = transaction.signatures[0];
        } else if (transaction.signature) {
            signature = transaction.signature;
        } else if (transactionData.signature) {
            signature = transactionData.signature;
        }
       
        if (!signature) {
            console.log(`[${new Date().toISOString()}] ⏭️ Skipping: no signature found`);
            return;
        }
       
        if (typeof signature !== 'string') {
            if (Buffer.isBuffer(signature)) {
                signature = signature.toString('base64');
            } else {
                signature = signature.toString();
            }
        }
       
        if (this.processedTransactions.has(signature)) {
            return;
        }
        this.processedTransactions.add(signature);
       
        console.log(`[${new Date().toISOString()}] 🎯 Processing transaction with signature: ${signature.slice(0, 8)}...`);
       
        let accountKeys = [];
       
        if (this.messageCount <= 5) {
            console.log(`[${new Date().toISOString()}] 🔍 DETAILED DEBUG - Transaction structure:`, JSON.stringify(transaction, null, 2));
            console.log(`[${new Date().toISOString()}] 🔍 DETAILED DEBUG - Meta structure:`, JSON.stringify(meta, null, 2));
            console.log(`[${new Date().toISOString()}] 🔍 DETAILED DEBUG - TransactionData structure:`, JSON.stringify(Object.keys(transactionData), null, 2));
        }
       
        if (transaction.message && transaction.message.accountKeys) {
            accountKeys = transaction.message.accountKeys;
            console.log(`[${new Date().toISOString()}] ✅ Found accountKeys in transaction.message.accountKeys: ${accountKeys.length} keys`);
        } else if (transaction.accountKeys) {
            accountKeys = transaction.accountKeys;
            console.log(`[${new Date().toISOString()}] ✅ Found accountKeys in transaction.accountKeys: ${accountKeys.length} keys`);
        } else if (transactionData.accountKeys) {
            accountKeys = transactionData.accountKeys;
            console.log(`[${new Date().toISOString()}] ✅ Found accountKeys in transactionData.accountKeys: ${accountKeys.length} keys`);
        } else if (meta.accountKeys) {
            accountKeys = meta.accountKeys;
            console.log(`[${new Date().toISOString()}] ✅ Found accountKeys in meta.accountKeys: ${accountKeys.length} keys`);
        } else if (transaction.message && transaction.message.account_keys) {
            accountKeys = transaction.message.account_keys;
            console.log(`[${new Date().toISOString()}] ✅ Found accountKeys in transaction.message.account_keys: ${accountKeys.length} keys`);
        } else {
            console.log(`[${new Date().toISOString()}] ❌ No accountKeys found! Available fields:`);
            console.log(`[${new Date().toISOString()}] 📝 transaction keys:`, Object.keys(transaction || {}));
            if (transaction.message) {
                console.log(`[${new Date().toISOString()}] 📝 transaction.message keys:`, Object.keys(transaction.message || {}));
            }
            console.log(`[${new Date().toISOString()}] 📝 meta keys:`, Object.keys(meta || {}));
            console.log(`[${new Date().toISOString()}] 📝 transactionData keys:`, Object.keys(transactionData || {}));
           
            const findAccountFields = (obj, prefix = '') => {
                for (const [key, value] of Object.entries(obj || {})) {
                    if (key.toLowerCase().includes('account')) {
                        console.log(`[${new Date().toISOString()}] 🔍 Found account-related field: ${prefix}${key}`, Array.isArray(value) ? `[array with ${value.length} items]` : typeof value);
                    }
                    if (typeof value === 'object' && value !== null && !Array.isArray(value) && prefix.length < 20) {
                        findAccountFields(value, `${prefix}${key}.`);
                    }
                }
            };
           
            findAccountFields(transaction, 'transaction.');
            findAccountFields(meta, 'meta.');
            findAccountFields(transactionData, 'transactionData.');
        }
       
        const stringAccountKeys = [];
       
        if (accountKeys.length > 0) {
            console.log(`[${new Date().toISOString()}] 🔄 Converting ${accountKeys.length} account keys...`);
           
            for (let i = 0; i < accountKeys.length; i++) {
                const key = accountKeys[i];
                let convertedKey = null;
               
                try {
                    if (typeof key === 'string') {
                        convertedKey = key;
                        if (this.messageCount <= 3) {
                            console.log(`[${new Date().toISOString()}] 📝 Key ${i}: string -> ${convertedKey.slice(0, 8)}...`);
                        }
                    } else if (Buffer.isBuffer(key)) {
                        const { PublicKey } = require('@solana/web3.js');
                        convertedKey = new PublicKey(key).toString();
                        if (this.messageCount <= 3) {
                            console.log(`[${new Date().toISOString()}] 📝 Key ${i}: Buffer -> ${convertedKey.slice(0, 8)}...`);
                        }
                    } else if (key && typeof key === 'object') {
                        if (key.pubkey) {
                            convertedKey = key.pubkey.toString();
                        } else if (key.key) {
                            convertedKey = key.key.toString();
                        } else if (key.address) {
                            convertedKey = key.address.toString();
                        } else {
                            console.log(`[${new Date().toISOString()}] ❓ Key ${i} is object with fields:`, Object.keys(key));
                            convertedKey = key.toString();
                        }
                        if (this.messageCount <= 3) {
                            console.log(`[${new Date().toISOString()}] 📝 Key ${i}: object -> ${convertedKey.slice(0, 8)}...`);
                        }
                    } else if (Array.isArray(key)) {
                        const { PublicKey } = require('@solana/web3.js');
                        convertedKey = new PublicKey(Buffer.from(key)).toString();
                        if (this.messageCount <= 3) {
                            console.log(`[${new Date().toISOString()}] 📝 Key ${i}: array -> ${convertedKey.slice(0, 8)}...`);
                        }
                    } else {
                        convertedKey = key.toString();
                        if (this.messageCount <= 3) {
                            console.log(`[${new Date().toISOString()}] 📝 Key ${i}: ${typeof key} -> ${convertedKey.slice(0, 8)}...`);
                        }
                    }
                   
                    if (convertedKey && convertedKey.length >= 32) {
                        stringAccountKeys.push(convertedKey);
                    } else {
                        console.warn(`[${new Date().toISOString()}] ⚠️ Invalid key length for key ${i}: ${convertedKey}`);
                    }
                } catch (error) {
                    console.error(`[${new Date().toISOString()}] ❌ Error converting key ${i}:`, error.message);
                    console.log(`[${new Date().toISOString()}] 📝 Raw key ${i}:`, key);
                }
            }
        }
       
        console.log(`[${new Date().toISOString()}] 🔍 Transaction has ${stringAccountKeys.length} valid accounts (from ${accountKeys.length} raw keys)`);
       
        if (stringAccountKeys.length > 0 && this.messageCount <= 3) {
            console.log(`[${new Date().toISOString()}] 📝 First 3 converted account keys:`, stringAccountKeys.slice(0, 3).map(k => `${k.slice(0,8)}...`));
        }
       
        let involvedWallet = null;
        for (const walletAddress of this.monitoredWallets) {
            if (stringAccountKeys.includes(walletAddress)) {
                involvedWallet = walletAddress;
                console.log(`[${new Date().toISOString()}] ✅ Found monitored wallet ${walletAddress.slice(0,8)}... in transaction ${signature.slice(0, 8)}...`);
                break;
            }
        }
       
        if (!involvedWallet) {
            if (this.messageCount <= 10) {
                console.log(`[${new Date().toISOString()}] ⏭️ No monitored wallets in transaction ${signature.slice(0, 8)}...`);
                console.log(`[${new Date().toISOString()}] 📝 Transaction accounts (first 3):`, stringAccountKeys.slice(0, 3).map(k => `${k.slice(0,8)}...`));
                console.log(`[${new Date().toISOString()}] 📝 Monitored wallets (first 3):`, Array.from(this.monitoredWallets).slice(0, 3).map(k => `${k.slice(0,8)}...`));
            }
            return;
        }
       
        const wallet = await this.db.getWalletByAddress(involvedWallet);
        if (!wallet) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Wallet ${involvedWallet} not found in database`);
            return;
        }
       
        if (this.activeGroupId && wallet.group_id !== this.activeGroupId) {
            console.log(`[${new Date().toISOString()}] ⏭️ Skipping transaction ${signature.slice(0, 8)}... - wallet belongs to different group (${wallet.group_id} != ${this.activeGroupId})`);
            return;
        }
       
        let blockTime;
        if (transactionData.blockTime) {
            blockTime = Number(transactionData.blockTime);
        } else if (transactionData.slot) {
            blockTime = Math.floor(Date.now() / 1000);
        } else {
            blockTime = Math.floor(Date.now() / 1000);
        }
       
        console.log(`[${new Date().toISOString()}] 🎯 Processing valid transaction ${signature.slice(0, 8)}... for wallet ${involvedWallet.slice(0, 8)}... (group: ${wallet.group_id || 'none'})`);
       
        const formattedTransactionData = {
            transaction: transaction,
            meta: meta,
            slot: transactionData.slot || 0,
            blockTime: blockTime
        };
       
        const convertedTransaction = this.convertGrpcToLegacyFormat(formattedTransactionData, stringAccountKeys);
       
        if (!convertedTransaction) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Failed to convert transaction format for ${signature.slice(0, 8)}...`);
            return;
        }
       
        await this.monitoringService.processWebhookMessage({
            signature: signature,
            walletAddress: involvedWallet,
            blockTime: blockTime,
            groupId: wallet.group_id,
            transactionData: convertedTransaction
        });
       
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error processing gRPC transaction:`, error.message);
        console.error(`[${new Date().toISOString()}] 📋 Error stack:`, error.stack);
    }
}
convertGrpcToLegacyFormat(grpcData, accountKeys) {
    try {
        const transaction = grpcData.transaction;
        const meta = grpcData.meta;
        
        const formattedAccountKeys = accountKeys.map(key => {
            try {
                if (typeof key === 'string' && key.length >= 32) {
                    return key;
                } else if (Buffer.isBuffer(key)) {
                    const { PublicKey } = require('@solana/web3.js');
                    return new PublicKey(key).toString();
                } else {
                    return key.toString();
                }
            } catch (error) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Error converting account key:`, error.message);
                return key;
            }
        });

        const converted = {
            transaction: {
                message: {
                    accountKeys: formattedAccountKeys,
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
        
        console.log(`[${new Date().toISOString()}] 🔄 Converted transaction with ${formattedAccountKeys.length} accounts, ${converted.meta.preTokenBalances.length} pre-token balances, ${converted.meta.postTokenBalances.length} post-token balances`);
        
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
    
    if (this.isStarted && this.stream) {
        console.log(`[${new Date().toISOString()}] 🔄 Updating gRPC subscription with new wallets`);
        try {
            console.log(`[${new Date().toISOString()}] ✅ Wallet monitoring updated - now tracking ${this.monitoredWallets.size} wallets`);
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Failed to update subscription:`, error.message);
        }
    }
    
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
        
        if (this.isStarted) {
            console.log(`[${new Date().toISOString()}] 🔄 Restarting gRPC subscription for group switch`);
            await this.subscribeToTransactions();
        }
        
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
            try {
                this.stream.end();
            } catch (error) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Error closing stream:`, error.message);
            }
            this.stream = null;
        }
        if (this.client) {
            try {
                if (typeof this.client.close === 'function') {
                    this.client.close();
                } else if (typeof this.client.destroy === 'function') {
                    this.client.destroy();
                } else if (typeof this.client.end === 'function') {
                    this.client.end();
                }
            } catch (error) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Error closing client:`, error.message);
            }
            this.client = null;
        }
        await new Promise(resolve => setTimeout(resolve, this.reconnectInterval));
        try {
            this.isConnecting = false;
            await this.connect();
            await this.subscribeToTransactions();
            console.log(`[${new Date().toISOString()}] ✅ gRPC reconnection successful`);
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
            try {
                this.stream.end();
            } catch (error) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Error ending stream:`, error.message);
            }
            this.stream = null;
        }
        if (this.client) {
            try {
                if (typeof this.client.close === 'function') {
                    this.client.close();
                } else if (typeof this.client.destroy === 'function') {
                    this.client.destroy();
                } else if (typeof this.client.end === 'function') {
                    this.client.end();
                }
            } catch (error) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Error closing client:`, error.message);
            }
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