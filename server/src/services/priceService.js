const { Connection, PublicKey } = require('@solana/web3.js');
const Redis = require('ioredis');

class PriceService {
    constructor() {
        this.connection = new Connection(
            process.env.SOLANA_RPC_URL || '',
            {
                commitment: 'confirmed',
                httpHeaders: {
                    'User-Agent': 'WalletPulse/3.0'
                }
            }
        );

        this.redis = new Redis(process.env.REDIS_URL || '');
        
        this.redis.on('connect', () => {
            console.log(`[${new Date().toISOString()}] ✅ PriceService Redis connected`);
        });
        
        this.redis.on('error', (err) => {
            console.error(`[${new Date().toISOString()}] ❌ PriceService Redis error:`, err.message);
        });

        this.CACHE_TTL = 30;
        this.SOL_PRICE_TTL = 60;
        this.PRICE_CACHE_TTL = 30000;
        
        this.solPriceCache = {
            price: 150,
            lastUpdated: 0
        };
        
        this.priceCache = new Map();
        this.poolCache = new Map();
    }

    async updateSolPrice() {
        try {
            const response = await fetch('https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112', {
                timeout: 8000,
                headers: {
                    'User-Agent': 'WalletPulse/3.0'
                }
            });

            if (response.ok) {
                const data = await response.json();
                if (data.pairs && data.pairs.length > 0) {
                    const bestPair = data.pairs.reduce((prev, current) =>
                        (current.volume?.h24 || 0) > (prev.volume?.h24 || 0) ? current : prev
                    );
                    const newPrice = parseFloat(bestPair.priceUsd || 150);

                    this.solPriceCache = {
                        price: newPrice,
                        lastUpdated: Date.now()
                    };

                    try {
                        await this.redis.setex('sol_price_optimized', this.SOL_PRICE_TTL, JSON.stringify(this.solPriceCache));
                    } catch (redisError) {
                        console.warn(`[${new Date().toISOString()}] ⚠️ Redis setex failed:`, redisError.message);
                    }
                    
                }
            } else {
                console.warn(`[${new Date().toISOString()}] ⚠️ DexScreener SOL price API error: ${response.status}`);
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Failed to update SOL price:`, error.message);
        }
    }

    async getSolPrice() {
        const now = Date.now();
        if (now - this.solPriceCache.lastUpdated > this.SOL_PRICE_TTL * 1000) {
            await this.updateSolPrice();
        }

        return {
            success: true,
            price: this.solPriceCache.price,
            source: 'dexscreener',
            lastUpdated: this.solPriceCache.lastUpdated
        };
    }

    async findTokenPools(tokenMint) {
        const cacheKey = `pools_opt_${tokenMint}`;

        const memCached = this.poolCache.get(tokenMint);
        if (memCached && (Date.now() - memCached.timestamp) < 300000) {
            return memCached.data;
        }

        try {
            const cached = await this.redis.get(cacheKey);
            if (cached) {
                const pools = JSON.parse(cached);
                this.poolCache.set(tokenMint, {
                    data: pools,
                    timestamp: Date.now()
                });
                return pools;
            }
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Pool cache fetch failed:`, error.message);
        }

        let pools = [];

        try {
            if (!/^[1-9A-HJ-NP-Za-km-z]+$/.test(tokenMint) || tokenMint.length < 32 || tokenMint.length > 44) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Invalid mint address: ${tokenMint}`);
                return pools;
            }

            const response = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${tokenMint}`, {
                timeout: 8000,
                headers: { 'User-Agent': 'WalletPulse/3.0' }
            });

            if (response.ok) {
                const data = await response.json();
                if (data.pairs && data.pairs.length > 0) {
                    pools = data.pairs.map(pair => ({
                        address: pair.pairAddress,
                        type: pair.dexId?.toLowerCase() || 'unknown',
                        pairedWith: pair.baseToken.address === tokenMint ? pair.quoteToken.symbol : pair.baseToken.symbol,
                        baseAmount: parseFloat(pair.liquidity?.base || 0),
                        quoteAmount: parseFloat(pair.liquidity?.quote || 0),
                        volume24h: parseFloat(pair.volume?.h24 || 0),
                        priceUsd: parseFloat(pair.priceUsd || 0),
                        pairCreatedAt: pair.pairCreatedAt || null,
                        discovered: 'dexscreener'
                    }));
                }
            }

            this.poolCache.set(tokenMint, {
                data: pools,
                timestamp: Date.now()
            });

            try {
                await this.redis.setex(cacheKey, 300, JSON.stringify(pools));
            } catch (redisError) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Redis pool cache failed:`, redisError.message);
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Pool discovery failed for ${tokenMint}:`, error.message);
        }

        return pools;
    }

    async getTokenDeploymentTime(tokenMint) {
        try {            
            const mintPubkey = new PublicKey(tokenMint);
            
            const signatures = await this.connection.getSignaturesForAddress(
                mintPubkey, 
                { limit: 100 },
                'confirmed'
            );

            if (signatures.length === 0) {
                return null;
            }

            const firstSignature = signatures[signatures.length - 1];
            
            if (firstSignature.blockTime) {
                const deploymentTime = new Date(firstSignature.blockTime * 1000);
                return deploymentTime;
            }

            return null;

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error getting deployment time for ${tokenMint}:`, error.message);
            return null;
        }
    }

    async analyzePools(tokenMint, pools) {
        if (pools.length === 0) {
            return {
                price: 0,
                priceInSol: 0,
                marketCap: 0,
                volume24h: 0,
                liquidity: 0,
                pools: 0,
                bestPool: null,
                deploymentTime: null,
                ageInHours: null
            };
        }

        const solPrice = this.solPriceCache.price;
        const bestPool = pools.reduce((best, current) => {
            const currentLiquidity = (current.baseAmount || 0) + (current.quoteAmount || 0);
            const bestLiquidity = (best.baseAmount || 0) + (best.quoteAmount || 0);
            return currentLiquidity > bestLiquidity ? current : best;
        });

        const priceUsd = bestPool.priceUsd || 0;
        const priceInSol = priceUsd / solPrice;

        const totalLiquidity = pools.reduce((sum, pool) => 
            sum + (pool.baseAmount || 0) + (pool.quoteAmount || 0), 0
        );
        
        const totalVolume24h = pools.reduce((sum, pool) => 
            sum + (pool.volume24h || 0), 0
        );

        const supplyData = await this.getTokenSupply(tokenMint);
        const marketCap = supplyData.supply * priceUsd;

        const metadata = await this.getBasicTokenMetadata(tokenMint);

        let deploymentTime = null;
        let ageInHours = null;

        const earliestPairTime = pools
            .filter(pool => pool.pairCreatedAt)
            .map(pool => new Date(pool.pairCreatedAt).getTime())
            .sort((a, b) => a - b)[0];

        if (earliestPairTime) {
            deploymentTime = new Date(earliestPairTime);
            ageInHours = (Date.now() - earliestPairTime) / (1000 * 60 * 60);
        } else {
            deploymentTime = await this.getTokenDeploymentTime(tokenMint);
            if (deploymentTime) {
                ageInHours = (Date.now() - deploymentTime.getTime()) / (1000 * 60 * 60);
            }
        }

        return {
            price: priceUsd,
            priceInSol: priceInSol,
            marketCap: marketCap,
            volume24h: totalVolume24h,
            liquidity: totalLiquidity,
            pools: pools.length,
            bestPool: bestPool ? {
                address: bestPool.address,
                type: bestPool.type,
                liquidity: (bestPool.baseAmount || 0) + (bestPool.quoteAmount || 0),
                volume24h: bestPool.volume24h || 0
            } : null,
            supply: supplyData.supply,
            decimals: supplyData.decimals,
            symbol: metadata.symbol,
            name: metadata.name,
            deploymentTime: deploymentTime ? deploymentTime.toISOString() : null,
            ageInHours: ageInHours
        };
    }

    async getTokenSupply(tokenMint) {
        try {
            const mintAccount = await this.connection.getParsedAccountInfo(new PublicKey(tokenMint));
            if (!mintAccount.value) {
                throw new Error('Mint account not found');
            }
            const data = mintAccount.value.data.parsed.info;
            return {
                supply: data.supply / Math.pow(10, data.decimals),
                decimals: data.decimals
            };
        } catch (error) {
            return { supply: 1000000, decimals: 6 };
        }
    }

    async getBasicTokenMetadata(tokenMint) {
        try {
            const cached = await this.redis.get(`metadata_opt_${tokenMint}`);
            if (cached) {
                return JSON.parse(cached);
            }

            const metadata = { 
                name: `Token ${tokenMint.slice(0, 8)}...`, 
                symbol: tokenMint.slice(0, 4).toUpperCase() 
            };

            try {
                await this.redis.setex(`metadata_opt_${tokenMint}`, 300, JSON.stringify(metadata));
            } catch (redisError) {
                console.warn(`[${new Date().toISOString()}] ⚠️ Redis metadata cache failed:`, redisError.message);
            }
            
            return metadata;
        } catch (error) {
            return { name: 'Unknown Token', symbol: 'UNK' };
        }
    }

    async getTokenData(tokenMint) {
        const cacheKey = `token_data_opt_${tokenMint}`;
        
        const memCached = this.priceCache.get(tokenMint);
        if (memCached && (Date.now() - memCached.timestamp) < this.PRICE_CACHE_TTL) {
            return memCached.data;
        }
        
        try {
            const cached = await this.redis.get(cacheKey);
            if (cached) {
                const tokenData = JSON.parse(cached);
                this.priceCache.set(tokenMint, {
                    data: tokenData,
                    timestamp: Date.now()
                });
                return tokenData;
            }
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Token data cache fetch failed:`, error.message);
        }

        const pools = await this.findTokenPools(tokenMint);
        const analysis = await this.analyzePools(tokenMint, pools);

        const tokenData = {
            mint: tokenMint,
            ...analysis,
            age: {
                createdAt: analysis.deploymentTime,
                ageInHours: analysis.ageInHours,
                isNew: analysis.ageInHours ? analysis.ageInHours < 24 : false
            },
            lastUpdated: new Date().toISOString(),
            source: 'price_service'
        };

        this.priceCache.set(tokenMint, {
            data: tokenData,
            timestamp: Date.now()
        });

        try {
            await this.redis.setex(cacheKey, this.CACHE_TTL, JSON.stringify(tokenData));
        } catch (error) {
            console.warn(`[${new Date().toISOString()}] ⚠️ Failed to cache token data:`, error.message);
        }

        return tokenData;
    }

    async getTokenPrices(tokenMints) {
        if (!tokenMints || tokenMints.length === 0) {
            return new Map();
        }

        const results = new Map();
        
        const batchSize = 5;
        for (let i = 0; i < tokenMints.length; i += batchSize) {
            const batch = tokenMints.slice(i, i + batchSize);
            
            const batchPromises = batch.map(async (mint) => {
                try {
                    const data = await this.getTokenData(mint);
                    return { mint, data };
                } catch (error) {
                    return { mint, data: null };
                }
            });
            
            const batchResults = await Promise.all(batchPromises);
            batchResults.forEach(({ mint, data }) => {
                results.set(mint, data);
            });

            if (i + batchSize < tokenMints.length) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        return results;
    }

    clearCache() {
        this.priceCache.clear();
        this.poolCache.clear();
    }

    async close() {
        try {
            this.clearCache();
            if (this.redis) {
                await this.redis.quit();
            }
            console.log(`[${new Date().toISOString()}] ✅ Price service connections closed`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error closing connections:`, error.message);
        }
    }
}

module.exports = PriceService;