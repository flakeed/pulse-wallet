import React, { useState } from 'react';

function WalletList({ walletCount = 0, groupName = null, onRemoveAllWallets }) {
  const [isRemovingAll, setIsRemovingAll] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [showDetails, setShowDetails] = useState(false);

  const handleRemoveAll = async () => {
    if (isRemovingAll) return;

    if (!window.confirm('⚠️ Remove ALL wallets from monitoring? This cannot be undone.')) {
      return;
    }

    setIsRemovingAll(true);
    setError(null);
    setSuccess(null);

    try {
      const result = await onRemoveAllWallets?.();
      setSuccess(result?.message || 'All wallets removed successfully.');
    } catch (err) {
      console.error('Error removing all wallets:', err);
      setError(err?.message || 'Failed to remove all wallets.');
    } finally {
      setIsRemovingAll(false);
    }
  };

  return (
    <div className="w-80 bg-white rounded-lg shadow-sm border">
      <div className="flex items-center justify-between p-3 border-b bg-gray-50">
        <div className="flex items-center space-x-2">
          <svg className="w-4 h-4 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M17 9V7a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2m2 4h10a2 2 0 002-2v-6a2 2 0 00-2-2H9a2 2 0 00-2 2v6a2 2 0 002 2zm7-5a2 2 0 11-4 0 2 2 0 014 0z" />
          </svg>
          <div>
            <h3 className="font-medium text-gray-900 text-sm">
              Monitored Wallets
            </h3>
            <div className="text-xs text-gray-500">
              {walletCount} wallets
              {groupName && <span className="ml-1">in {groupName}</span>}
            </div>
          </div>
        </div>

        <div className="flex items-center space-x-1">
          {walletCount > 0 && (
            <button
              onClick={handleRemoveAll}
              disabled={isRemovingAll}
              className="text-red-600 hover:text-red-700 p-1 rounded text-xs disabled:opacity-50"
              title="Remove all wallets"
            >
              {isRemovingAll ? (
                <div className="animate-spin rounded-full h-3 w-3 border border-red-600 border-t-transparent"></div>
              ) : (
                <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                </svg>
              )}
            </button>
          )}

          {walletCount > 0 && (
            <button
              onClick={() => setShowDetails(!showDetails)}
              className="text-gray-600 hover:text-gray-800 p-1 rounded"
              title={showDetails ? "Hide details" : "Show details"}
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d={!showDetails ? "M19 9l-7 7-7-7" : "M5 15l7-7 7 7"} />
              </svg>
            </button>
          )}
        </div>
      </div>

      {error && (
        <div className="p-2 m-2 bg-red-100 text-red-700 rounded text-xs">{error}</div>
      )}
      {success && (
        <div className="p-2 m-2 bg-green-100 text-green-700 rounded text-xs">{success}</div>
      )}

      {showDetails && walletCount > 0 && (
        <div className="p-3 border-b bg-gray-50">
          <h4 className="text-xs font-medium text-gray-700 mb-2">Statistics</h4>
          <div className="grid grid-cols-2 gap-2 text-xs">
            <div className="bg-white p-2 rounded">
              <div className="font-semibold text-green-600">{walletCount}</div>
              <div className="text-gray-500">Total Wallets</div>
            </div>
            <div className="bg-white p-2 rounded">
              <div className="font-semibold text-blue-600">Active</div>
              <div className="text-gray-500">Monitoring</div>
            </div>
          </div>
        </div>
      )}

      {walletCount === 0 && (
        <div className="text-center py-6 px-3">
          <svg className="w-8 h-8 mx-auto text-gray-400 mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1}
              d="M17 9V7a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2m2 4h10a2 2 0 002-2v-6a2 2 0 00-2-2H9a2 2 0 00-2 2v6a2 2 0 002 2zm7-5a2 2 0 11-4 0 2 2 0 014 0z" />
          </svg>
          <p className="text-gray-500 text-xs">No wallets monitored</p>
          <p className="text-gray-400 text-xs mt-1">Add wallets using the form above</p>
        </div>
      )}
    </div>
  );
}

export default WalletList;