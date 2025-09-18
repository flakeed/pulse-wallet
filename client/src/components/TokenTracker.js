import React, { useState, useEffect, useRef } from 'react';
import TokenCard from './TokenCard';
import CompactControls from './CompactControls';
import soundManager from '../utils/soundManager';

function TokenTracker({ groupId, transactions, timeframe, onTimeframeChange, groups, selectedGroup, onGroupChange, walletCount, selectedGroupInfo }) {
  const [items, setItems] = useState([]);
  const [hours, setHours] = useState(timeframe || '24');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [sortBy, setSortBy] = useState('latest');
  const previousTokenMints = useRef(new Set());

  const aggregateTokens = (transactions, hours, groupId) => {
    const EXCLUDED_TOKENS = [
      'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
      'So11111111111111111111111111111111111111112',  
      'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 
    ];

    const byToken = new Map();
    const now = new Date();
    const filteredTransactions = transactions.filter((tx) => {
      const txTime = new Date(tx.time);
      const hoursDiff = (now - txTime) / (1000 * 60 * 60);
      const matchesTimeframe = hoursDiff <= parseInt(hours);
      const matchesGroup = !groupId || tx.wallet.group_id === groupId;
      return matchesTimeframe && matchesGroup;
    });

    filteredTransactions.forEach((tx) => {
      const tokens = tx.transactionType === 'buy' ? tx.tokensBought : tx.tokensSold;
      if (!tokens || tokens.length === 0) return;

      tokens.forEach((token) => {
        if (EXCLUDED_TOKENS.includes(token.mint)) return;

        if (!byToken.has(token.mint)) {
          byToken.set(token.mint, {
            mint: token.mint,
            symbol: token.symbol || 'Unknown',
            name: token.name || 'Unknown Token',
            decimals: token.decimals || 6,
            wallets: [],
            summary: {
              uniqueWallets: new Set(),
              totalBuys: 0,
              totalSells: 0,
              totalSpentSOL: 0,
              totalReceivedSOL: 0,
              netSOL: 0,
              latestActivity: null,
              deploymentTime: null,
              ageInHours: null,
            },
          });
        }

        const tokenData = byToken.get(token.mint);
        const walletAddress = tx.wallet.address;
        const wallet = tokenData.wallets.find((w) => w.address === walletAddress);
        const txTime = new Date(tx.time);

        if (!tokenData.summary.latestActivity || txTime > new Date(tokenData.summary.latestActivity)) {
          tokenData.summary.latestActivity = tx.time;
        }

        if (token.deployment_time) {
          if (!tokenData.summary.deploymentTime || new Date(token.deployment_time) < new Date(tokenData.summary.deploymentTime)) {
            tokenData.summary.deploymentTime = token.deployment_time;
            const deployTime = new Date(token.deployment_time);
            const currentTime = new Date();
            tokenData.summary.ageInHours = (currentTime - deployTime) / (1000 * 60 * 60);
          }
        }

        if (!wallet) {
          tokenData.wallets.push({
            address: walletAddress,
            name: tx.wallet.name || null,
            groupId: tx.wallet.group_id,
            groupName: tx.wallet.group_name,
            txBuys: tx.transactionType === 'buy' ? 1 : 0,
            txSells: tx.transactionType === 'sell' ? 1 : 0,
            solSpent: tx.transactionType === 'buy' ? parseFloat(tx.solSpent) || 0 : 0,
            solReceived: tx.transactionType === 'sell' ? parseFloat(tx.solReceived) || 0 : 0,
            tokensBought: tx.transactionType === 'buy' ? token.amount || 0 : 0,
            tokensSold: tx.transactionType === 'sell' ? token.amount || 0 : 0,
            pnlSol: (tx.transactionType === 'sell' ? parseFloat(tx.solReceived) || 0 : 0) - 
                    (tx.transactionType === 'buy' ? parseFloat(tx.solSpent) || 0 : 0),
            lastActivity: tx.time,
          });
          tokenData.summary.uniqueWallets.add(walletAddress);
        } else {
          wallet.txBuys += tx.transactionType === 'buy' ? 1 : 0;
          wallet.txSells += tx.transactionType === 'sell' ? 1 : 0;
          wallet.solSpent += tx.transactionType === 'buy' ? parseFloat(tx.solSpent) || 0 : 0;
          wallet.solReceived += tx.transactionType === 'sell' ? parseFloat(tx.solReceived) || 0 : 0;
          wallet.tokensBought += tx.transactionType === 'buy' ? token.amount || 0 : 0;
          wallet.tokensSold += tx.transactionType === 'sell' ? token.amount || 0 : 0;
          wallet.pnlSol = wallet.solReceived - wallet.solSpent;
          
          if (txTime > new Date(wallet.lastActivity)) {
            wallet.lastActivity = tx.time;
          }
        }

        tokenData.summary.totalBuys += tx.transactionType === 'buy' ? 1 : 0;
        tokenData.summary.totalSells += tx.transactionType === 'sell' ? 1 : 0;
        tokenData.summary.totalSpentSOL += tx.transactionType === 'buy' ? parseFloat(tx.solSpent) || 0 : 0;
        tokenData.summary.totalReceivedSOL += tx.transactionType === 'sell' ? parseFloat(tx.solReceived) || 0 : 0;
      });
    });

    const result = Array.from(byToken.values()).map((t) => ({
      ...t,
      summary: {
        ...t.summary,
        uniqueWallets: t.summary.uniqueWallets.size,
        netSOL: +(t.summary.totalReceivedSOL - t.summary.totalSpentSOL).toFixed(6),
      },
      wallets: t.wallets.sort((a, b) => new Date(b.lastActivity) - new Date(a.lastActivity))
    }));

    return result;
  };

  const sortTokens = (tokens, sortBy) => {
    const sortedTokens = [...tokens];
    
    switch (sortBy) {
      case 'latest':
        return sortedTokens.sort((a, b) => {
          const timeA = new Date(a.summary.latestActivity || 0);
          const timeB = new Date(b.summary.latestActivity || 0);
          return timeB - timeA;
        });
      
      case 'newest':
        return sortedTokens.sort((a, b) => {
          const deployTimeA = a.summary.deploymentTime ? new Date(a.summary.deploymentTime) : new Date(0);
          const deployTimeB = b.summary.deploymentTime ? new Date(b.summary.deploymentTime) : new Date(0);
          
          if (!a.summary.deploymentTime && b.summary.deploymentTime) return 1;
          if (a.summary.deploymentTime && !b.summary.deploymentTime) return -1;
          if (!a.summary.deploymentTime && !b.summary.deploymentTime) return 0;
          
          return deployTimeB - deployTimeA;
        });
      
      case 'most_wallets':
        return sortedTokens.sort((a, b) => b.summary.uniqueWallets - a.summary.uniqueWallets);
      
      case 'profit':
        return sortedTokens.sort((a, b) => b.summary.netSOL - a.summary.netSOL);
      
      case 'loss':
        return sortedTokens.sort((a, b) => a.summary.netSOL - b.summary.netSOL);
      
      case 'volume':
        return sortedTokens.sort((a, b) => Math.abs(b.summary.netSOL) - Math.abs(a.summary.netSOL));
      
      case 'activity':
        return sortedTokens.sort((a, b) => 
          (b.summary.totalBuys + b.summary.totalSells) - (a.summary.totalBuys + a.summary.totalSells)
        );
      
      default:
        return sortedTokens;
    }
  };

  useEffect(() => {
    setLoading(true);
    try {
      const aggregatedTokens = aggregateTokens(transactions, hours, groupId);
      const sortedTokens = sortTokens(aggregatedTokens, sortBy);
      
      const currentTokenMints = new Set(sortedTokens.map(token => token.mint));
      const newTokens = sortedTokens.filter(token => !previousTokenMints.current.has(token.mint));
      
      if (newTokens.length > 0 && previousTokenMints.current.size > 0) {
        console.log(`🔊 New tokens detected: ${newTokens.length}`);
        newTokens.forEach(token => {
          console.log(`  - ${token.symbol} (${token.mint.slice(0, 8)}...)`);
        });
        
        soundManager.playNewTokenSound();
      }
      
      previousTokenMints.current = currentTokenMints;
      
      setItems(sortedTokens);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [transactions, hours, groupId, sortBy]);

  useEffect(() => {
    setHours(timeframe);
  }, [timeframe]);

  const openGmgnChart = (mintAddress) => {
    if (!mintAddress) return;
    const gmgnUrl = `https://gmgn.ai/sol/token/${encodeURIComponent(mintAddress)}`;
    window.location.href = gmgnUrl;
  };

  return (
    <div className="h-full flex flex-col bg-gray-900">
      <CompactControls
        groups={groups}
        selectedGroup={selectedGroup}
        onGroupChange={onGroupChange}
        walletCount={walletCount}
        selectedGroupInfo={selectedGroupInfo}
        timeframe={hours}
        onTimeframeChange={(newTimeframe) => {
          setHours(newTimeframe);
          onTimeframeChange(newTimeframe);
        }}
        sortBy={sortBy}
        onSortChange={setSortBy}
      />
      
      <div className="flex-1 overflow-y-auto">
        {loading ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500 mr-3"></div>
            <span className="text-gray-400">Loading tokens...</span>
          </div>
        ) : error ? (
          <div className="bg-red-900/20 border border-red-700 rounded-lg p-4 m-4">
            <div className="text-red-400 font-medium">Error loading data</div>
            <div className="text-red-300 text-sm mt-1">{error}</div>
          </div>
        ) : items.length === 0 ? (
          <div className="text-center py-12">
            <div className="text-gray-500 mb-4">
              <svg className="w-16 h-16 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} 
                  d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
            </div>
            <p className="text-gray-400 text-lg">No token activity found</p>
            <p className="text-sm text-gray-500 mt-1">
              No token transactions detected for the selected timeframe and group
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 xl:grid-cols-2 2xl:grid-cols-3 gap-1 p-2">
            {items.map((token) => (
              <TokenCard 
                key={token.mint} 
                token={token} 
                onOpenChart={() => openGmgnChart(token.mint)} 
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default TokenTracker;