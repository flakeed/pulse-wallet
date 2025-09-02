const { Connection, PublicKey } = require('@solana/web3.js');
const { Metaplex } = require('@metaplex-foundation/js');
const { v4: uuidv4 } = require('uuid');
const Redis = require('ioredis');

let isProcessingQueue = false;

const redis = new Redis(process.env.REDIS_URL || '');

redis.on('connect', () => {
    console.log(`[${new Date().toISOString()}] ✅ Connected to Redis`);
});
redis.on('error', (err) => {
    console.error(`[${new Date().toISOString()}] ❌ Redis connection error:`, err.message);
});

const promiseStore = new Map();
const REQUEST_DELAY = 50;
const TOKEN_CACHE_TTL = 24 * 60 * 60;
const PROMISE_TTL = 60;

async function processQueue() {
    if (isProcessingQueue) return;
    isProcessingQueue = true;

    while (true) {
        const requestData = await redis.lpop('metadata:queue', 400);
        if (!requestData || requestData.length === 0) break;

        const requests = requestData.map((data) => {
            try {
                return JSON.parse(data);
            } catch (e) {
                console.error(`[${new Date().toISOString()}] ❌ Invalid queue entry:`, e.message);
                return null;
            }
        }).filter((req) => req !== null);

        await Promise.all(
            requests.map(async (request) => {
                const { requestId, mint, connection: rpcEndpoint } = request;
                const connection = new Connection(rpcEndpoint, 'confirmed');
                console.log(`[${new Date().toISOString()}] Processing metadata request ${requestId} for mint ${mint}`);

                try {
                    const result = await processTokenMetadataRequest(mint, connection);
                    const promise = promiseStore.get(requestId);
                    if (promise) {
                        promise.resolve(result);
                    } else {
                        console.warn(`[${new Date().toISOString()}] No promise found for request ${requestId}`);
                    }
                } catch (error) {
                    console.error(`[${new Date().toISOString()}] Error processing metadata request ${requestId} for mint ${mint}:`, error.message);
                    const promise = promiseStore.get(requestId);
                    if (promise) {
                        promise.reject(error);
                    }
                }

                promiseStore.delete(requestId);
            })
        );
        await new Promise((resolve) => setTimeout(resolve, REQUEST_DELAY));
    }

    isProcessingQueue = false;
    const queueLength = await redis.llen('metadata:queue');
    if (queueLength > 0) {
        setImmediate(processQueue);
    }
}

async function processTokenMetadataRequest(mint, connection) {
    const cachedToken = await redis.get(`token:${mint}`);
    if (cachedToken) {
        return JSON.parse(cachedToken);
    }

    try {
        const onChainData = await fetchOnChainMetadata(mint, connection);
        const data = onChainData || { address: mint, symbol: 'Unknown', name: 'Unknown Token', decimals: 0, deployment_time: null };
        await redis.set(`token:${mint}`, JSON.stringify(data), 'EX', TOKEN_CACHE_TTL);
        return data;
    } catch (e) {
        console.error(`[${new Date().toISOString()}] Error fetching metadata for mint ${mint}:`, e.message);
        const data = { address: mint, symbol: 'Unknown', name: 'Unknown Token', decimals: 0, deployment_time: null };
        await redis.set(`token:${mint}`, JSON.stringify(data), 'EX', TOKEN_CACHE_TTL);
        return data;
    }
}

async function fetchTokenMetadata(mint, connection) {
    const cachedToken = await redis.get(`token:${mint}`);
    if (cachedToken) {
        return JSON.parse(cachedToken);
    }

    return new Promise((resolve, reject) => {
        const requestId = uuidv4();

        promiseStore.set(requestId, { resolve, reject });

        redis.lpush('metadata:queue', JSON.stringify({
            requestId,
            mint,
            connection: connection.rpcEndpoint,
        }), (err) => {
            if (err) {
                console.error(`[${new Date().toISOString()}] Error enqueuing request ${requestId}:`, err.message);
                promiseStore.delete(requestId);
                reject(err);
                return;
            }

            if (!isProcessingQueue) {
                setImmediate(processQueue);
            }
        });

        setTimeout(() => {
            if (promiseStore.has(requestId)) {
                console.warn(`[${new Date().toISOString()}] Cleaning up stale promise for request ${requestId}`);
                promiseStore.delete(requestId);
                reject(new Error(`Request ${requestId} timed out`));
            }
        }, PROMISE_TTL * 1000);
    });
}

async function fetchOnChainMetadata(mint, connection) {
    try {
        
        const metaplex = new Metaplex(connection);
        const mintPubkey = new PublicKey(mint);
        
        let symbol = 'Unknown';
        let name = 'Unknown Token';
        let decimals = 6;
        
        try {
            const metadataAccount = await metaplex.nfts().findByMint({ mintAddress: mintPubkey });
            if (metadataAccount) {
                symbol = metadataAccount.symbol || 'Unknown';
                name = metadataAccount.name || 'Unknown Token';
                decimals = metadataAccount.mint.decimals || 6;
            }
        } catch (metadataError) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Metaplex metadata not found for ${mint}, trying mint account...`);
            
            try {
                const mintAccount = await connection.getParsedAccountInfo(mintPubkey);
                if (mintAccount.value && mintAccount.value.data.parsed) {
                    decimals = mintAccount.value.data.parsed.info.decimals || 6;
                }
            } catch (mintError) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Could not fetch mint account for ${mint}`);
            }
        }
        
        let deploymentTime = null;
        
        try {
            
            let allSignatures = [];
            let before = undefined;
            
            for (let i = 0; i < 5; i++) {
                const signatures = await connection.getSignaturesForAddress(
                    mintPubkey,
                    { limit: 1000, before },
                    'confirmed'
                );
                
                if (signatures.length === 0) break;
                
                allSignatures.push(...signatures);
                before = signatures[signatures.length - 1].signature;
                
                if (signatures.length < 1000) break;
            }
            
            if (allSignatures.length > 0) {
                const firstSignature = allSignatures[allSignatures.length - 1];
                
                if (firstSignature.blockTime) {
                    deploymentTime = new Date(firstSignature.blockTime * 1000).toISOString();
                    console.log(`[${new Date().toISOString()}] ✅ Token ${mint.slice(0,8)}... deployed at: ${deploymentTime}`);
                } else {

                    const transaction = await connection.getParsedTransaction(
                        firstSignature.signature,
                        { maxSupportedTransactionVersion: 0 }
                    );
                    
                    if (transaction && transaction.blockTime) {
                        deploymentTime = new Date(transaction.blockTime * 1000).toISOString();
                        console.log(`[${new Date().toISOString()}] ✅ Token ${mint.slice(0,8)}... deployed at: ${deploymentTime} (from tx)`);
                    }
                }
            }
            
        } catch (deploymentError) {
            console.error(`[${new Date().toISOString()}] ❌ Error getting deployment time for ${mint}:`, deploymentError.message);
        }
        
        const result = {
            address: mint,
            symbol: symbol,
            name: name,
            decimals: decimals,
            deployment_time: deploymentTime
        };
        
        return result;
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error fetching on-chain metadata for mint ${mint}:`, error.message);
        return {
            address: mint,
            symbol: 'Unknown',
            name: 'Unknown Token',
            decimals: 6,
            deployment_time: null
        };
    }
}

async function close() {
    try {
        await redis.quit();
        console.log(`[${new Date().toISOString()}] ✅ TokenService Redis connection closed`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error closing TokenService Redis:`, error.message);
    }
}

module.exports = {
    fetchOnChainMetadata,
    fetchTokenMetadata,
    close,
    redis,
};