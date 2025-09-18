import { useState, useEffect, useRef, useCallback } from 'react';

const globalTokenCache = new Map();
const pendingRequests = new Map();

const CACHE_TTL = 30000;
const BATCH_DELAY = 200;
const MAX_BATCH_SIZE = 25;
const REQUEST_TIMEOUT = 15000;

const getAuthHeaders = () => {
  const sessionToken = localStorage.getItem('sessionToken');
  return {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${sessionToken}`
  };
};

const logError = (context, error, details = {}) => {
  console.error(`[usePrices:${context}] Error:`, error);
  if (Object.keys(details).length > 0) {
    console.error(`[usePrices:${context}] Details:`, details);
  }
};

const formatTokenAge = (ageInHours) => {
  if (!ageInHours || ageInHours < 0) return null;
  
  if (ageInHours < 1) {
    const minutes = Math.floor(ageInHours * 60);
    return `${minutes}m`;
  }
  
  if (ageInHours < 24) {
    const hours = Math.floor(ageInHours);
    return `${hours}h`;
  }
  
  const days = Math.floor(ageInHours / 24);
  if (days < 30) {
    return `${days}d`;
  }
  
  const months = Math.floor(days / 30);
  if (months < 12) {
    return `${months}mo`;
  }
  
  const years = Math.floor(days / 365);
  return `${years}y`;
};

const isTokenNew = (ageInHours) => {
  return ageInHours !== null && ageInHours < 24;
};

let batchQueue = new Set();
let batchTimer = null;

const processBatches = async () => {
  if (batchQueue.size === 0) return;
  
  const allMints = Array.from(batchQueue);
  batchQueue.clear();
  
  const batches = [];
  for (let i = 0; i < allMints.length; i += MAX_BATCH_SIZE) {
    batches.push(allMints.slice(i, i + MAX_BATCH_SIZE));
  }
  
  for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    const batch = batches[batchIndex];
    
    try {
      await processSingleBatch(batch);
      
      if (batchIndex < batches.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (error) {
      logError('processBatches', error, { batchIndex, batchSize: batch.length });
      
      batch.forEach(mint => {
        const pending = pendingRequests.get(mint);
        if (pending) {
          pending.forEach(({ reject }) => reject(error));
          pendingRequests.delete(mint);
        }
      });
    }
  }
};

const processSingleBatch = async (mints) => {
  try {
    const validMints = mints.filter(mint => mint && typeof mint === 'string' && mint.length >= 32);
    const invalidMints = mints.filter(mint => !mint || typeof mint !== 'string' || mint.length < 32);
    
    if (invalidMints.length > 0) {
      console.warn(`[usePrices:processSingleBatch] Filtered out ${invalidMints.length} invalid mints:`, invalidMints);
      invalidMints.forEach(mint => {
        const pending = pendingRequests.get(mint);
        if (pending) {
          pending.forEach(({ resolve }) => resolve(null));
          pendingRequests.delete(mint);
        }
      });
    }
    
    if (validMints.length === 0) {
      console.warn(`[usePrices:processSingleBatch] No valid mints to process`);
      return;
    }
    
    if (validMints.length > MAX_BATCH_SIZE) {
      console.error(`[usePrices:processSingleBatch] Batch too large: ${validMints.length} > ${MAX_BATCH_SIZE}`);
      throw new Error(`Internal error: batch size ${validMints.length} exceeds limit ${MAX_BATCH_SIZE}`);
    }
    
    const requestBody = JSON.stringify({ mints: validMints });
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    
    try {
      /*
      const response = await fetch('/api/tokens/batch-data', {
        method: 'POST',
        headers: getAuthHeaders(),
        body: requestBody,
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        let errorMessage;
        try {
          const errorData = await response.json();
          errorMessage = errorData.error || `HTTP ${response.status}`;
          console.error(`[usePrices:processSingleBatch] Server error:`, errorData);
          
          if (response.status === 400 && errorMessage.includes('Too many mints')) {
            throw new Error(`Server rejected batch size ${validMints.length}. Please report this bug.`);
          }
        } catch (jsonError) {
          errorMessage = `HTTP ${response.status} ${response.statusText}`;
        }
        throw new Error(errorMessage);
      }
      
      const result = await response.json();

      if (result.success && result.data) {
        const now = Date.now();
        
        validMints.forEach(mint => {
          const tokenData = result.data[mint] || null;
          
          if (tokenData) {
            const processedTokenData = {
              ...tokenData,
              age: {
                createdAt: tokenData.age?.createdAt || null,
                ageInHours: tokenData.age?.ageInHours || null,
                isNew: tokenData.age?.isNew || isTokenNew(tokenData.age?.ageInHours),
                formattedAge: formatTokenAge(tokenData.age?.ageInHours)
              }
            };
            
            globalTokenCache.set(`token-${mint}`, {
              data: processedTokenData,
              timestamp: now
            });
          }
          
          const pending = pendingRequests.get(mint);
          if (pending) {
            pending.forEach(({ resolve }) => resolve(tokenData));
            pendingRequests.delete(mint);
          }
        });
      } else {
        throw new Error(result.error || 'Invalid response format');
      }
      */
      
      const response = await fetch('/api/tokens/batch-age', {
        method: 'POST',
        headers: getAuthHeaders(),
        body: requestBody,
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        console.warn(`[usePrices:processSingleBatch] Age endpoint not available, using mock data`);
        const now = Date.now();
        
        validMints.forEach(mint => {
          const mockAgeData = {
            age: {
              createdAt: null,
              ageInHours: null,
              isNew: false,
              formattedAge: 'Unknown'
            }
          };
          
          globalTokenCache.set(`token-${mint}`, {
            data: mockAgeData,
            timestamp: now
          });
          
          const pending = pendingRequests.get(mint);
          if (pending) {
            pending.forEach(({ resolve }) => resolve(mockAgeData));
            pendingRequests.delete(mint);
          }
        });
        return;
      }
      
      const result = await response.json();

      if (result.success && result.data) {
        const now = Date.now();
        
        validMints.forEach(mint => {
          const ageData = result.data[mint] || null;
          
          if (ageData) {
            const processedAgeData = {
              age: {
                createdAt: ageData.age?.createdAt || null,
                ageInHours: ageData.age?.ageInHours || null,
                isNew: ageData.age?.isNew || isTokenNew(ageData.age?.ageInHours),
                formattedAge: formatTokenAge(ageData.age?.ageInHours)
              }
            };
            
            globalTokenCache.set(`token-${mint}`, {
              data: processedAgeData,
              timestamp: now
            });
          }
          
          const pending = pendingRequests.get(mint);
          if (pending) {
            pending.forEach(({ resolve }) => resolve(ageData));
            pendingRequests.delete(mint);
          }
        });
      } else {
        throw new Error(result.error || 'Invalid response format');
      }
    } finally {
      clearTimeout(timeoutId);
    }
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error('Request timeout - server took too long to respond');
    }
    throw error;
  }
};

const queueTokenData = (mint) => {
  return new Promise((resolve, reject) => {
    if (!mint || typeof mint !== 'string' || mint.length < 32) {
      console.warn(`[usePrices:queueTokenData] Invalid mint: ${mint}`);
      resolve(null);
      return;
    }
    
    const cached = globalTokenCache.get(`token-${mint}`);
    if (cached && (Date.now() - cached.timestamp) < CACHE_TTL) {
      resolve(cached.data);
      return;
    }
    
    if (!pendingRequests.has(mint)) {
      pendingRequests.set(mint, []);
    }
    pendingRequests.get(mint).push({ resolve, reject });
    
    batchQueue.add(mint);
    
    if (batchTimer) {
      clearTimeout(batchTimer);
    }
    
    if (batchQueue.size >= MAX_BATCH_SIZE) {
      batchTimer = setTimeout(processBatches, 50);
    } else {
      batchTimer = setTimeout(processBatches, BATCH_DELAY);
    }
  });
};

export const useSolPrice = () => {
  const [solPrice, setSolPrice] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const isMountedRef = useRef(true);

  const fetchSolPrice = useCallback(async () => {
    const cached = globalTokenCache.get('sol-price');
    if (cached && (Date.now() - cached.timestamp) < CACHE_TTL) {
      if (isMountedRef.current) {
        setSolPrice(cached.data.price);
      }
      return cached.data.price;
    }

    if (loading) return solPrice;

    setLoading(true);
    setError(null);

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000);
      
      try {
        const response = await fetch('/api/solana/price', {
          headers: getAuthHeaders(),
          signal: controller.signal
        });
        
        clearTimeout(timeoutId);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();
        
        if (data.success || data.price) {
          const price = data.price || 150;
          
          globalTokenCache.set('sol-price', {
            data: { price, ...data },
            timestamp: Date.now()
          });

          if (isMountedRef.current) {
            setSolPrice(price);
            setError(null);
          }
          return price;
        } else {
          throw new Error(data.error || 'Failed to fetch SOL price');
        }
      } finally {
        clearTimeout(timeoutId);
      }
    } catch (err) {
      if (err.name === 'AbortError') {
        err = new Error('SOL price request timeout');
      }
      
      logError('useSolPrice', err);
      if (isMountedRef.current) {
        setError(err.message);
        setSolPrice(150);
      }
      return 150;
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [loading, solPrice]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchSolPrice();
    return () => {
      isMountedRef.current = false;
    };
  }, [fetchSolPrice]);

  return { solPrice, loading, error, refetch: fetchSolPrice };
};

export const useTokenData = (tokenMint) => {
  const [tokenData, setTokenData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const isMountedRef = useRef(true);

  const fetchTokenData = useCallback(async () => {
    if (!tokenMint) {
      if (isMountedRef.current) {
        setTokenData(null);
        setError(null);
        setLoading(false);
      }
      return null;
    }

    if (typeof tokenMint !== 'string' || tokenMint.length < 32) {
      console.warn(`[usePrices:useTokenData] Invalid mint: ${tokenMint}`);
      if (isMountedRef.current) {
        setTokenData(null);
        setError('Invalid token mint address');
        setLoading(false);
      }
      return null;
    }

    const cached = globalTokenCache.get(`token-${tokenMint}`);
    if (cached && (Date.now() - cached.timestamp) < CACHE_TTL) {
      if (isMountedRef.current) {
        setTokenData(cached.data);
        setError(null);
        setLoading(false);
      }
      return cached.data;
    }

    if (loading) return tokenData;

    setLoading(true);
    setError(null);

    try {
      const result = await queueTokenData(tokenMint);
      
      if (isMountedRef.current) {
        setTokenData(result);
        setError(null);
        
        if (result && result.age) {
          console.log(`[usePrices] Token age data loaded for ${tokenMint.slice(0, 8)}...`);
        }
      }
      return result;
    } catch (err) {
      logError('useTokenData', err, { tokenMint: tokenMint.slice(0, 8) });
      if (isMountedRef.current) {
        setError(err.message);
        setTokenData(null);
      }
      return null;
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [tokenMint, loading, tokenData]);

  useEffect(() => {
    isMountedRef.current = true;
    
    if (tokenMint) {
      const cached = globalTokenCache.get(`token-${tokenMint}`);
      if (!cached || (Date.now() - cached.timestamp) >= CACHE_TTL) {
        setTokenData(null);
        setError(null);
        setLoading(true);
        fetchTokenData();
      } else {
        setTokenData(cached.data);
        setError(null);
        setLoading(false);
      }
    } else {
      setTokenData(null);
      setError(null);
      setLoading(false);
    }
    
    return () => {
      isMountedRef.current = false;
    };
  }, [tokenMint]);

  return { tokenData, loading, error, refetch: fetchTokenData };
};

/*
export const useTokenPrice = (tokenMint) => {
  const { tokenData, loading, error, refetch } = useTokenData(tokenMint);
  
  const priceData = tokenData ? {
    price: tokenData.price,
    change24h: tokenData.change24h || 0,
    volume24h: tokenData.volume24h || 0,
    liquidity: tokenData.liquidity || 0,
    marketCap: tokenData.marketCap,
    priceInSol: tokenData.priceInSol,
    pools: tokenData.pools,
    bestPool: tokenData.bestPool,
    ageInHours: tokenData.age?.ageInHours,
    isNew: tokenData.age?.isNew,
    formattedAge: tokenData.age?.formattedAge,
    symbol: tokenData.token?.symbol,
    name: tokenData.token?.name
  } : null;

  return { priceData, loading, error, refetch };
};
*/

export const useTokenAge = (tokenMint) => {
  const { tokenData, loading, error, refetch } = useTokenData(tokenMint);
  
  const ageData = tokenData ? {
    ageInHours: tokenData.age?.ageInHours,
    isNew: tokenData.age?.isNew,
    formattedAge: tokenData.age?.formattedAge,
    createdAt: tokenData.age?.createdAt
  } : null;

  return { ageData, loading, error, refetch };
};

/*
export const usePrices = (tokenMint = null) => {
  const { solPrice, loading: solLoading, error: solError } = useSolPrice();
  const { tokenData, loading: tokenLoading, error: tokenError } = useTokenData(tokenMint);

  const tokenPrice = tokenData ? {
    price: tokenData.price,
    change24h: 0,
    volume24h: tokenData.volume24h || 0,
    liquidity: tokenData.liquidity || 0,
    marketCap: tokenData.marketCap,
    priceInSol: tokenData.priceInSol,
    pools: tokenData.pools,
    ageInHours: tokenData.age?.ageInHours,
    isNew: tokenData.age?.isNew,
    formattedAge: tokenData.age?.formattedAge,
    deploymentTime: tokenData.age?.createdAt
  } : undefined;

  return {
    solPrice,
    tokenPrice,
    enhancedTokenData: tokenData,
    loading: solLoading || tokenLoading,
    error: solError || tokenError,
    ready: solPrice !== null && (!tokenMint || tokenPrice !== undefined)
  };
};
*/

export const usePrices = (tokenMint = null) => {
  const { solPrice, loading: solLoading, error: solError } = useSolPrice();
  const { tokenData, loading: tokenLoading, error: tokenError } = useTokenData(tokenMint);

  return {
    solPrice,
    tokenAge: tokenData ? {
      ageInHours: tokenData.age?.ageInHours,
      isNew: tokenData.age?.isNew,
      formattedAge: tokenData.age?.formattedAge,
      deploymentTime: tokenData.age?.createdAt
    } : undefined,
    loading: solLoading || tokenLoading,
    error: solError || tokenError,
    ready: solPrice !== null && (!tokenMint || tokenData !== undefined)
  };
};