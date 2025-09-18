import React, { useState, useMemo } from 'react';
import { useTokenData } from '../hooks/usePrices';
import { formatPnL, getPnLColor } from '../utils/pnlCalculator';

function TokenCard({ token, onOpenChart }) {
  const [showDetails, setShowDetails] = useState(true);
  const [showAllWallets, setShowAllWallets] = useState(false);
  const { tokenData: data, loading, error } = useTokenData(token.mint);

  const WALLETS_DISPLAY_LIMIT = 3;

  // Simple PnL calculation without price data
  const groupPnL = useMemo(() => {
    if (!token.wallets || token.wallets.length === 0) {
      return {
        totalPnLSOL: 0,
        totalSpentSOL: 0,
        totalReceivedSOL: 0
      };
    }

    let totalSpentSOL = 0;
    let totalReceivedSOL = 0;

    token.wallets.forEach(wallet => {
      totalSpentSOL += Number(wallet.solSpent || 0);
      totalReceivedSOL += Number(wallet.solReceived || 0);
    });

    return {
      totalPnLSOL: totalReceivedSOL - totalSpentSOL,
      totalSpentSOL,
      totalReceivedSOL
    };
  }, [token.wallets]);

  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
  };

  const openGmgnChart = () => {
    if (!token.mint) return;
    const gmgnUrl = `https://gmgn.ai/sol/token/${encodeURIComponent(token.mint)}`;
    window.open(gmgnUrl, '_blank');
  };

  const openGmgnMaker = (walletAddress) => {
    if (!walletAddress || !token.mint) return;
    const gmgnUrl = `https://gmgn.ai/sol/token/${encodeURIComponent(token.mint)}?maker=${encodeURIComponent(walletAddress)}`;
    window.open(gmgnUrl, '_blank');
  };

  const formatAge = (ageData) => {
    if (!ageData || !ageData.ageInHours) return 'Unknown';
    
    const ageInHours = ageData.ageInHours;
    
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

  const handleShowAllWallets = (event) => {
    event.preventDefault(); 
    event.stopPropagation();
    setShowAllWallets(true);
  };

  const handleHideWallets = (event) => {
    event.preventDefault();
    event.stopPropagation();
    setShowAllWallets(false);
  };

  const netColor = getPnLColor(groupPnL.totalPnLSOL);
  
  const isNewToken = data?.age?.isNew || false;
  const tokenAge = data?.age || null;
  const formattedAge = tokenAge ? formatAge(tokenAge) : 'Unknown';
  const deploymentTime = tokenAge?.createdAt;

  const displayPnL = groupPnL.totalPnLSOL || 0;

  const walletsToShow = showAllWallets ? token.wallets : token.wallets.slice(0, WALLETS_DISPLAY_LIMIT);
  const hasMoreWallets = token.wallets.length > WALLETS_DISPLAY_LIMIT;

  return (
    <div className="bg-gray-900 border border-gray-700 hover:border-gray-600 transition-colors">
      <div className="flex items-center justify-between p-3 border-b border-gray-800">
        <div className="flex items-center space-x-3 min-w-0 flex-1">
          <div className="min-w-0 flex-1">
            <div className="flex items-center space-x-2">
              <span className="bg-blue-600 text-white text-xs font-bold px-2 py-0.5 rounded">
                {token.symbol || 'UNK'}
              </span>
              {isNewToken && (
                <span className="bg-red-600 text-white text-xs font-bold px-2 py-0.5 rounded animate-pulse">
                  NEW
                </span>
              )}
              <span className="text-gray-300 text-sm truncate">
                {token.name || 'Unknown Token'}
              </span>
            </div>
            <div className="flex items-center space-x-2 mt-1">
              <div className="text-gray-500 text-xs font-mono truncate max-w-32">
                {token.mint}
              </div>
              <button
                onClick={() => copyToClipboard(token.mint)}
                className="text-gray-500 hover:text-blue-400 transition-colors"
                title="Copy address"
              >
                <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                </svg>
              </button>
              <span className="text-xs text-gray-500" title={deploymentTime ? `Created: ${new Date(deploymentTime).toLocaleString()}` : 'Creation time unknown'}>
                {formattedAge}
              </span>
              {loading && (
                <div className="animate-spin rounded-full h-3 w-3 border border-gray-400 border-t-transparent"></div>
              )}
            </div>
          </div>

          <div className="text-right">
            <div className={`text-sm font-bold ${netColor} flex items-center`}>
              {formatPnL(displayPnL)}
            </div>
            
            <div className="text-xs text-gray-500">
              {token.summary.uniqueWallets}W · {token.summary.totalBuys}B · {token.summary.totalSells}S
            </div>
          </div>
        </div>

        <div className="flex items-center space-x-1 ml-3">
          <button
            onClick={() => setShowDetails(!showDetails)}
            className="p-1 text-gray-500 hover:text-gray-300 transition-colors"
            title={showDetails ? "Hide details" : "Show details"}
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d={!showDetails ? "M19 9l-7 7-7-7" : "M5 15l7-7 7 7"} />
            </svg>
          </button>
          <button
            onClick={openGmgnChart}
            className="p-1 text-gray-500 hover:text-blue-400 transition-colors"
            title="Open chart"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
            </svg>
          </button>
        </div>
      </div>

      {showDetails && (
        <div className="p-3 bg-gray-800/50">
          {data && !loading && (
            <div className="grid grid-cols-2 gap-4 mb-3 text-xs">
              <div>
                <div className="text-gray-400 mb-1">Age</div>
                <div className="text-white font-medium">
                  {formattedAge}
                  {isNewToken && (
                    <span className="text-red-400 text-xs ml-1 animate-pulse">NEW!</span>
                  )}
                  {deploymentTime && (
                    <div className="text-gray-500 text-xs">
                      {new Date(deploymentTime).toLocaleDateString()}
                    </div>
                  )}
                </div>
              </div>
              <div>
                <div className="text-gray-400 mb-1">Token Info</div>
                <div className="text-white font-medium">
                  {data.token?.symbol || token.symbol || 'UNK'}
                  <div className="text-gray-500 text-xs truncate">
                    {data.token?.name || token.name || 'Unknown Token'}
                  </div>
                </div>
              </div>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4 mb-3 text-xs">
            <div>
              <div className="text-gray-400 mb-1">Total Spent</div>
              <div className="text-white font-medium">
                {groupPnL.totalSpentSOL.toFixed(4)} SOL
              </div>
            </div>
            <div>
              <div className="text-gray-400 mb-1">Total Received</div>
              <div className="text-white font-medium">
                {groupPnL.totalReceivedSOL.toFixed(4)} SOL
              </div>
            </div>
          </div>

          <div className="space-y-1">
            <div className="flex items-center justify-between text-gray-400 text-xs mb-2">
              <span>Top Wallets</span>
              {hasMoreWallets && (
                <span className="text-gray-500">
                  {showAllWallets ? 'All' : `${WALLETS_DISPLAY_LIMIT} of ${token.wallets.length}`}
                </span>
              )}
            </div>
            
            {walletsToShow.map((wallet, index) => {
              const walletPnL = (wallet.solReceived || 0) - (wallet.solSpent || 0);
              
              return (
                <div key={wallet.address} className="flex items-center justify-between bg-gray-900/50 p-2 rounded text-xs">
                  <div className="flex items-center space-x-2">
                    <span className="text-gray-300 font-medium">
                      {wallet.name || `${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}`}
                    </span>
                    <span className="text-gray-500">
                      {wallet.txBuys}B · {wallet.txSells}S
                    </span>
                    <button
                      onClick={() => copyToClipboard(wallet.address)}
                      className="text-gray-500 hover:text-blue-400 transition-colors"
                      title="Copy wallet address"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                          d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                      </svg>
                    </button>
                    <button
                      onClick={() => openGmgnMaker(wallet.address)}
                      className="text-gray-500 hover:text-blue-400 transition-colors"
                      title="View on GMGN"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                          d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                      </svg>
                    </button>
                  </div>
                  <div className={`font-medium ${getPnLColor(walletPnL)}`}>
                    {formatPnL(walletPnL)}
                  </div>
                </div>
              );
            })}
            
            {hasMoreWallets && (
              <div className="text-center mt-2">
                {!showAllWallets ? (
                  <button
                    onClick={handleShowAllWallets} 
                    className="text-blue-400 hover:text-blue-300 text-xs underline transition-colors"
                  >
                    +{token.wallets.length - WALLETS_DISPLAY_LIMIT} more wallets
                  </button>
                ) : (
                  <button
                    onClick={handleHideWallets} 
                    className="text-gray-500 hover:text-gray-400 text-xs underline transition-colors"
                  >
                    Show less
                  </button>
                )}
              </div>
            )}
          </div>

          <div className="flex space-x-2 mt-3">
            <button
              onClick={openGmgnChart}
              className="flex-1 bg-green-600 hover:bg-green-700 text-white py-2 rounded text-sm font-medium transition-colors"
            >
              GMGN
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export default TokenCard;