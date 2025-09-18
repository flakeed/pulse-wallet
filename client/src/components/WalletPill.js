import React from 'react';
import { formatPnL, getPnLColor } from '../utils/pnlCalculator';

function WalletPill({ wallet, tokenMint }) {
    const label = wallet.name || `${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}`;
    
    // Simple PnL calculation without price data
    const calculatedPnL = (wallet.solReceived || 0) - (wallet.solSpent || 0);

    const openGmgnTokenWithMaker = () => {
        if (!tokenMint || !wallet.address) return;
        const gmgnUrl = `https://gmgn.ai/sol/token/${encodeURIComponent(tokenMint)}?maker=${encodeURIComponent(wallet.address)}`;
        window.open(gmgnUrl, '_blank');
    };

    const copyToClipboard = () => {
        navigator.clipboard.writeText(wallet.address);
    };

    return (
        <div className="flex items-center justify-between bg-gray-800/60 hover:bg-gray-700/60 p-2 rounded text-xs transition-colors group">
            <div className="flex items-center space-x-2 min-w-0 flex-1">
                <div className="min-w-0">
                    <div className="flex items-center space-x-1">
                        <span className="text-gray-200 font-medium truncate max-w-20">
                            {label}
                        </span>
                        <button
                            onClick={copyToClipboard}
                            className="text-gray-500 hover:text-blue-400 transition-colors opacity-0 group-hover:opacity-100"
                            title="Copy address"
                        >
                            <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                    strokeWidth={2}
                                    d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                                />
                            </svg>
                        </button>
                    </div>
                    <div className="text-gray-500 text-xs">
                        {wallet.txBuys}B · {wallet.txSells}S
                    </div>
                </div>
            </div>

            <div className="flex items-center space-x-2">
                <div className="text-right">
                    <div className={`text-xs font-semibold ${getPnLColor(calculatedPnL)}`}>
                        {formatPnL(calculatedPnL)}
                    </div>
                    <div className="text-gray-500 text-xs">
                        {(wallet.solSpent || 0).toFixed(2)}→{(wallet.solReceived || 0).toFixed(2)}
                    </div>
                </div>
                
                <button
                    onClick={openGmgnTokenWithMaker}
                    className="text-gray-500 hover:text-blue-400 transition-colors p-1 rounded"
                    title="Open chart with this wallet"
                >
                    <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={2}
                            d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
                        />
                    </svg>
                </button>
            </div>
        </div>
    );
}

export default WalletPill;