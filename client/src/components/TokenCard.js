import React, { useState } from 'react';
import { useTokenData } from '../hooks/usePrices';

function TokenCard({ token }) {
  const [showDetails, setShowDetails] = useState(true);
  const { tokenData: data, loading, error } = useTokenData(token.mint);

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

  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
  };

  const openGmgnChart = () => {
    if (!token.mint) return;
    const gmgnUrl = `https://gmgn.ai/sol/token/${encodeURIComponent(token.mint)}`;
    window.open(gmgnUrl, '_blank');
  };

  const isNewToken = data?.age?.isNew || false;
  const tokenAge = data?.age || null;
  const formattedAge = tokenAge ? formatAge(tokenAge) : 'Unknown';
  const deploymentTime = tokenAge?.createdAt;
  const formattedCreationTime = deploymentTime ? new Date(deploymentTime).toLocaleString() : 'Unknown';

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
              <span className="text-xs text-gray-500" title={formattedCreationTime}>
                {formattedAge}
              </span>
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
          {loading && (
            <div className="flex items-center justify-center py-4">
              <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500 mr-3"></div>
              <span className="text-gray-400">Loading data...</span>
            </div>
          )}

          {error && !loading && (
            <div className="bg-red-900/20 border border-red-700 rounded p-2 mb-3">
              <div className="text-red-400 text-sm">Failed to load data</div>
              <div className="text-red-300 text-xs">{error}</div>
            </div>
          )}

          {data && !loading && (
            <div className="mb-3 text-xs">
              <div className="text-gray-400 mb-1">Created</div>
              <div className="text-white font-medium">
                {formattedCreationTime}
                {isNewToken && (
                  <span className="text-red-400 text-xs ml-1 animate-pulse">NEW!</span>
                )}
                <div className="text-gray-500 text-xs">
                  {formattedAge}
                </div>
              </div>
            </div>
          )}

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