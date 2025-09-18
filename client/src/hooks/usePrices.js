import { useState, useEffect, useRef, useCallback } from 'react';

const globalTokenCache = new Map();

const CACHE_TTL = 30000;

const getAuthHeaders = () => {
  const sessionToken = localStorage.getItem('sessionToken');
  return {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${sessionToken}`
  };
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

const fetchTokenAge = async (mint) => {
  try {
    const response = await fetch(`/api/tokens/age/${mint}`, {
      method: 'GET',
      headers: getAuthHeaders(),
      signal: AbortSignal.timeout(10000)
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const result = await response.json();

    if (result.success && result.data) {
      return {
        age: {
          createdAt: result.data.createdAt || null,
          ageInHours: result.data.ageInHours || null,
          isNew: isTokenNew(result.data.ageInHours),
          formattedAge: formatTokenAge(result.data.ageInHours)
        }
      };
    }

    return null;
  } catch (error) {
    console.error(`Error fetching token age for ${mint}:`, error);
    return null;
  }
};

export const useTokenData = (tokenMint) => {
  const [tokenData, setTokenData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!tokenMint) {
      if (isMountedRef.current) {
        setTokenData(null);
        setError(null);
        setLoading(false);
      }
      return null;
    }

    if (typeof tokenMint !== 'string' || tokenMint.length < 32) {
      console.warn(`Invalid mint: ${tokenMint}`);
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
      const result = await fetchTokenAge(tokenMint);
      
      if (result) {
        globalTokenCache.set(`token-${tokenMint}`, {
          data: result,
          timestamp: Date.now()
        });
      }
      
      if (isMountedRef.current) {
        setTokenData(result);
        setError(null);
      }
      return result;
    } catch (err) {
      console.error('Token data fetch error:', err);
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
        fetchData();
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

  return { tokenData, loading, error, refetch: fetchData };
};

export const useSolPrice = () => {
  return { 
    solPrice: 150, 
    loading: false, 
    error: null, 
    refetch: () => {} 
  };
};

export const useTokenPrice = (tokenMint) => {
  return { 
    priceData: null, 
    loading: false, 
    error: null, 
    refetch: () => {} 
  };
};

export const usePrices = (tokenMint = null) => {
  const { tokenData, loading: tokenLoading, error: tokenError } = useTokenData(tokenMint);

  return {
    solPrice: 150, 
    tokenPrice: undefined,
    enhancedTokenData: tokenData,
    loading: tokenLoading,
    error: tokenError,
    ready: !tokenMint || tokenData !== undefined
  };
};