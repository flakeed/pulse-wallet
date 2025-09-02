import React from 'react';

function CompactControls({ 
  groups, 
  selectedGroup, 
  onGroupChange, 
  walletCount, 
  selectedGroupInfo,
  timeframe,
  onTimeframeChange,
  sortBy,
  onSortChange
}) {
  return (
    <div className="bg-gray-800 border-b border-gray-700 px-4 py-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <select
            value={selectedGroup || ''}
            onChange={(e) => onGroupChange(e.target.value)}
            className="bg-gray-700 border border-gray-600 text-white text-sm rounded px-3 py-1 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Groups ({walletCount.toLocaleString()} wallets)</option>
            {groups.map((group) => (
              <option key={group.id} value={group.id}>
                {group.name} ({group.wallet_count.toLocaleString()} wallets)
              </option>
            ))}
          </select>
          
          {selectedGroupInfo && (
            <div className="text-blue-400 text-xs bg-blue-900/30 px-2 py-1 rounded">
              {selectedGroupInfo.groupName}: {selectedGroupInfo.walletCount.toLocaleString()} wallets
            </div>
          )}
        </div>

        <div className="flex items-center space-x-3">
          <select
            value={timeframe}
            onChange={(e) => onTimeframeChange(e.target.value)}
            className="bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="1">1h</option>
            <option value="6">6h</option>
            <option value="24">24h</option>
          </select>

          <select
            value={sortBy}
            onChange={(e) => onSortChange(e.target.value)}
            className="bg-gray-700 border border-gray-600 text-white text-sm rounded px-2 py-1 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="latest">Latest</option>
            <option value="profit">Profit</option>
            <option value="loss">Loss</option>
            <option value="volume">Volume</option>
            <option value="activity">Activity</option>
          </select>
        </div>
      </div>
    </div>
  );
}

export default CompactControls;