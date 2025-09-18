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
const metadataCache = new Map();
const REQUEST_DELAY = 50;
const TOKEN_CACHE_TTL = 24 * 60 * 60;
const PROMISE_TTL = 60;
const MEMORY_CACHE_TTL = 300000;
const BATCH_SIZE = 50;

async function processQueue() {
    if (isProcessingQueue) return;
    isProcessingQueue = true;

    while (true) {
        const requestData = await redis.lpop('metadata:queue', 200);
        if (!requestData || requestData.length === 0) break;

        const requests = requestData.map((data) => {
            try {
                return JSON.parse(data);
            } catch (e) {
                console.error(`[${new Date().toISOString()}] ❌ Invalid queue entry:`, e.message);
                return null;
            }
        }).filter((req) => req !== null);

        const batchPromises = requests.map(async (request) => {
            const { requestId, mint, connection: rpcEndpoint } = request;
            const connection = new Connection(rpcEndpoint, 'confirmed');

            try {
                const result = await processTokenMetadataRequest(mint, connection);
                const promise = promiseStore.get(requestId);
                if (promise) {
                    promise.resolve(result);
                } else {
                    console.warn(`[${new Date().toISOString()}] No promise found for request ${requestId}`);
                }
            } catch (error) {
                console.error(`[${new Date().toISOString()}] Error processing metadata request ${requestId}:`, error.message);
                const promise = promiseStore.get(requestId);
                if (promise) {
                    promise.reject(error);
                }
            }

            promiseStore.delete(requestId);
        });

        await Promise.allSettled(batchPromises);
        await new Promise((resolve) => setTimeout(resolve, REQUEST_DELAY));
    }

    isProcessingQueue = false;
    const queueLength = await redis.llen('metadata:queue');
    if (queueLength > 0) {
        setImmediate(processQueue);
    }
}

async function processTokenMetadataRequest(mint, connection) {
    const cached = metadataCache.get(mint);
    if (cached && (Date.now() - cached.timestamp) < MEMORY_CACHE_TTL) {
        return cached.data;
    }

    const cachedToken = await redis.get(`token:${mint}`);
    if (cachedToken) {
        const data = JSON.parse(cachedToken);
        metadataCache.set(mint, {
            data: data,
            timestamp: Date.now()
        });
        return data;
    }

    try {
        const onChainData = await fetchOnChainMetadata(mint, connection);
        const data = onChainData || { 
            address: mint, 
            symbol: mint.slice(0, 4).toUpperCase(), 
            name: `Token ${mint.slice(0, 8)}...`, 
            decimals: 6, 
            deployment_time: null 
        };
        
        await redis.set(`token:${mint}`, JSON.stringify(data), 'EX', TOKEN_CACHE_TTL);
        metadataCache.set(mint, {
            data: data,
            timestamp: Date.now()
        });
        
        return data;
    } catch (e) {
        const data = { 
            address: mint, 
            symbol: mint.slice(0, 4).toUpperCase(), 
            name: `Token ${mint.slice(0, 8)}...`, 
            decimals: 6, 
            deployment_time: null 
        };
        
        await redis.set(`token:${mint}`, JSON.stringify(data), 'EX', TOKEN_CACHE_TTL);
        metadataCache.set(mint, {
            data: data,
            timestamp: Date.now()
        });
        
        return data;
    }
}

async function fetchTokenMetadata(mint, connection) {
    const cached = metadataCache.get(mint);
    if (cached && (Date.now() - cached.timestamp) < MEMORY_CACHE_TTL) {
        return cached.data;
    }

    const cachedToken = await redis.get(`token:${mint}`);
    if (cachedToken) {
        const data = JSON.parse(cachedToken);
        metadataCache.set(mint, {
            data: data,
            timestamp: Date.now()
        });
        return data;
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
                promiseStore.delete(requestId);
                reject(new Error(`Request ${requestId} timed out`));
            }
        }, PROMISE_TTL * 1000);
    });
}

async function batchFetchTokenMetadata(mints, connection) {
    const results = new Map();
    const uncachedMints = [];

    for (const mint of mints) {
        const cached = metadataCache.get(mint);
        if (cached && (Date.now() - cached.timestamp) < MEMORY_CACHE_TTL) {
            results.set(mint, cached.data);
        } else {
            uncachedMints.push(mint);
        }
    }

    if (uncachedMints.length === 0) {
        return results;
    }

    const pipeline = redis.pipeline();
    uncachedMints.forEach(mint => pipeline.get(`token:${mint}`));
    const redisResults = await pipeline.exec();

    const stillUncached = [];
    redisResults.forEach(([err, cachedData], index) => {
        const mint = uncachedMints[index];
        if (!err && cachedData) {
            const tokenData = JSON.parse(cachedData);
            results.set(mint, tokenData);
            metadataCache.set(mint, {
                data: tokenData,
                timestamp: Date.now()
            });
        } else {
            stillUncached.push(mint);
        }
    });

    if (stillUncached.length === 0) {
        return results;
    }

    for (let i = 0; i < stillUncached.length; i += BATCH_SIZE) {
        const batch = stillUncached.slice(i, i + BATCH_SIZE);
        const batchResults = await Promise.allSettled(
            batch.map(async (mint) => {
                const tokenInfo = await fetchTokenMetadata(mint, connection);
                return { mint, tokenInfo };
            })
        );

        const pipeline = redis.pipeline();
        batchResults.forEach((result) => {
            if (result.status === 'fulfilled') {
                const { mint, tokenInfo } = result.value;
                if (tokenInfo) {
                    results.set(mint, tokenInfo);
                    pipeline.set(`token:${mint}`, JSON.stringify(tokenInfo), 'EX', TOKEN_CACHE_TTL);
                    metadataCache.set(mint, {
                        data: tokenInfo,
                        timestamp: Date.now()
                    });
                }
            }
        });
        await pipeline.exec();

        if (i + BATCH_SIZE < stillUncached.length) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    return results;
}

async function fetchOnChainMetadata(mint, connection) {
    try {
        let symbol = mint.slice(0, 4).toUpperCase();
        let name = `Token ${mint.slice(0, 8)}...`;
        let decimals = 6;
        
        try {
            const mintPubkey = new PublicKey(mint);
            const mintAccount = await connection.getParsedAccountInfo(mintPubkey);
            
            if (mintAccount.value && mintAccount.value.data.parsed) {
                decimals = mintAccount.value.data.parsed.info.decimals || 6;
            }

            try {
                const metaplex = new Metaplex(connection);
                const metadataAccount = await metaplex.nfts().findByMint({ mintAddress: mintPubkey });
                
                if (metadataAccount) {
                    symbol = metadataAccount.symbol || symbol;
                    name = metadataAccount.name || name;
                    decimals = metadataAccount.mint.decimals || decimals;
                }
            } catch (metadataError) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Metaplex metadata not found for ${mint}`);
            }
        } catch (mintError) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Could not fetch mint account for ${mint}`);
        }
        
        let deploymentTime = null;
        
        try {
            const mintPubkey = new PublicKey(mint);
            const signatures = await connection.getSignaturesForAddress(
                mintPubkey,
                { limit: 100 },
                'confirmed'
            );
            
            if (signatures.length > 0) {
                const firstSignature = signatures[signatures.length - 1];
                
                if (firstSignature.blockTime) {
                    deploymentTime = new Date(firstSignature.blockTime * 1000).toISOString();
                }
            }
        } catch (deploymentError) {
            console.error(`[${new Date().toISOString()}] ❌ Error getting deployment time for ${mint}:`, deploymentError.message);
        }
        
        return {
            address: mint,
            symbol: symbol,
            name: name,
            decimals: decimals,
            deployment_time: deploymentTime
        };
        
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error fetching on-chain metadata for mint ${mint}:`, error.message);
        return {
            address: mint,
            symbol: mint.slice(0, 4).toUpperCase(),
            name: `Token ${mint.slice(0, 8)}...`,
            decimals: 6,
            deployment_time: null
        };
    }
}

async function close() {
    try {
        metadataCache.clear();
        await redis.quit();
        console.log(`[${new Date().toISOString()}] ✅ TokenService closed`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error closing TokenService:`, error.message);
    }
}

module.exports = {
    fetchOnChainMetadata,
    fetchTokenMetadata,
    batchFetchTokenMetadata,
    close,
    redis,
};