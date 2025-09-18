import { useState, useEffect, useRef, useCallback } from 'react';

const globalTokenCache = new Map();
const pendingRequests = new Map();

const CACHE_TTL = 300000; 
const BATCH_DELAY = 200;
const MAX_BATCH_SIZE = 25;
const REQUEST_TIMEOUT = 10000;

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
    
    const requestBody = JSON.stringify({ 
      mints: validMints,
      dataType: 'deployment-only' 
    });
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    
    try {
      const response = await fetch('/api/tokens/deployment-time', {
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
        } catch (jsonError) {
          errorMessage = `HTTP ${response.status} ${response.statusText}`;
        }
        throw new Error(errorMessage);
      }
      
      const result = await response.json();

      if (result.success && result.data) {
        const now = Date.now();
        
        validMints.forEach(mint => {
          const deploymentData = result.data[mint] || null;
          
          let processedDeploymentData = null;
          if (deploymentData && deploymentData.deploymentTime) {
            const deployTime = new Date(deploymentData.deploymentTime);
            const ageInHours = (Date.now() - deployTime.getTime()) / (1000 * 60 * 60);
            
            processedDeploymentData = {
              age: {
                createdAt: deploymentData.deploymentTime,
                ageInHours: ageInHours,
                isNew: isTokenNew(ageInHours),
                formattedAge: formatTokenAge(ageInHours)
              },
              token: {
                symbol: deploymentData.symbol || 'UNK',
                name: deploymentData.name || 'Unknown Token',
                decimals: deploymentData.decimals || 6
              },
              lastUpdated: new Date().toISOString(),
              source: 'deployment_service'
            };
            
            globalTokenCache.set(`token-${mint}`, {
              data: processedDeploymentData,
              timestamp: now
            });
          }
          
          const pending = pendingRequests.get(mint);
          if (pending) {
            pending.forEach(({ resolve }) => resolve(processedDeploymentData));
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

const queueTokenDeploymentData = (mint) => {
  return new Promise((resolve, reject) => {
    if (!mint || typeof mint !== 'string' || mint.length < 32) {
      console.warn(`[usePrices:queueTokenDeploymentData] Invalid mint: ${mint}`);
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
  const [solPrice, setSolPrice] = useState(150); 
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const isMountedRef = useRef(true);

  const fetchSolPrice = useCallback(async () => {
    if (isMountedRef.current) {
      setSolPrice(150);
      setError(null);
      setLoading(false);
    }
    return 150;
  }, []);

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
      const result = await queueTokenDeploymentData(tokenMint);
      
      if (isMountedRef.current) {
        setTokenData(result);
        setError(null);
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

export const useTokenPrice = (tokenMint) => {
  const { tokenData, loading, error, refetch } = useTokenData(tokenMint);
  
  const priceData = tokenData ? {
    price: 0,
    change24h: 0,
    volume24h: 0,
    liquidity: 0,
    marketCap: 0,
    priceInSol: 0,
    pools: null,
    bestPool: null,
    ageInHours: tokenData.age?.ageInHours,
    isNew: tokenData.age?.isNew,
    formattedAge: tokenData.age?.formattedAge,
    symbol: tokenData.token?.symbol,
    name: tokenData.token?.name
  } : null;

  return { priceData, loading, error, refetch };
};

export const usePrices = (tokenMint = null) => {
  const { solPrice, loading: solLoading, error: solError } = useSolPrice();
  const { tokenData, loading: tokenLoading, error: tokenError } = useTokenData(tokenMint);

  const tokenPrice = tokenData ? {
    price: 0,
    change24h: 0,
    volume24h: 0,
    liquidity: 0,
    marketCap: 0,
    priceInSol: 0,
    pools: null,
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