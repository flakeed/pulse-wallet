export const roundToDecimals = (value, decimals = 6) => {
    if (!value || isNaN(value)) return 0;
    return Math.round(value * Math.pow(10, decimals)) / Math.pow(10, decimals);
};

export const calculateWalletPnL = (walletData, tokenPrice, solPrice) => {
    const totalTokensBought = Number(walletData.tokensBought || 0);
    const totalTokensSold = Number(walletData.tokensSold || 0);
    const totalSpentSOL = Number(walletData.solSpent || 0);
    const totalReceivedSOL = Number(walletData.solReceived || 0);

    if (totalTokensBought === 0) {
        return {
            totalPnLSOL: roundToDecimals(totalReceivedSOL - totalSpentSOL),
            realizedPnLSOL: roundToDecimals(totalReceivedSOL - totalSpentSOL),
            unrealizedPnLSOL: 0,
            currentHoldings: 0,
            avgBuyPriceSOL: 0,
            totalPnLUSD: roundToDecimals((totalReceivedSOL - totalSpentSOL) * solPrice, 2)
        };
    }

    const currentHoldings = Math.max(0, totalTokensBought - totalTokensSold);
    const soldTokens = Math.min(totalTokensSold, totalTokensBought);
    const avgBuyPriceSOL = totalSpentSOL / totalTokensBought;

    let realizedPnLSOL = 0;
    if (soldTokens > 0) {
        const soldTokensCostBasisSOL = soldTokens * avgBuyPriceSOL;
        realizedPnLSOL = totalReceivedSOL - soldTokensCostBasisSOL;
    }

    /*
    let unrealizedPnLSOL = 0;
    if (currentHoldings > 0 && tokenPrice && solPrice) {
        const currentPriceSOL = tokenPrice / solPrice;
        const currentMarketValueSOL = currentHoldings * currentPriceSOL;
        const remainingCostBasisSOL = currentHoldings * avgBuyPriceSOL;
        unrealizedPnLSOL = currentMarketValueSOL - remainingCostBasisSOL;
    }
    */

    let unrealizedPnLSOL = 0;

    const totalPnLSOL = realizedPnLSOL + unrealizedPnLSOL;

    return {
        totalPnLSOL: roundToDecimals(totalPnLSOL),
        realizedPnLSOL: roundToDecimals(realizedPnLSOL),
        unrealizedPnLSOL: roundToDecimals(unrealizedPnLSOL),
        currentHoldings: roundToDecimals(currentHoldings),
        avgBuyPriceSOL: roundToDecimals(avgBuyPriceSOL),
        totalPnLUSD: roundToDecimals(totalPnLSOL * (solPrice || 150), 2),
        soldTokens: roundToDecimals(soldTokens),
        soldPercentage: totalTokensBought > 0 ? roundToDecimals((soldTokens / totalTokensBought) * 100, 1) : 0
    };
};

export const calculateTokenPnL = (wallets, tokenPrice, solPrice) => {
    if (!wallets || wallets.length === 0) {
        return {
            totalPnLSOL: 0,
            realizedPnLSOL: 0,
            unrealizedPnLSOL: 0,
            totalTokensBought: 0,
            totalTokensSold: 0,
            currentHoldings: 0,
            totalSpentSOL: 0,
            totalReceivedSOL: 0,
            totalPnLUSD: 0,
            avgBuyPriceSOL: 0,
            soldPercentage: 0,
            walletPnLs: []
        };
    }

    let totalTokensBought = 0;
    let totalTokensSold = 0;
    let totalSpentSOL = 0;
    let totalReceivedSOL = 0;
    let totalRealizedPnLSOL = 0;
    let totalUnrealizedPnLSOL = 0;

    const walletPnLs = wallets.map(wallet => {
        // const walletPnL = calculateWalletPnL(wallet, tokenPrice, solPrice);
        
        const walletPnL = calculateWalletPnL(wallet, 0, solPrice || 150);
        
        totalTokensBought += Number(wallet.tokensBought || 0);
        totalTokensSold += Number(wallet.tokensSold || 0);
        totalSpentSOL += Number(wallet.solSpent || 0);
        totalReceivedSOL += Number(wallet.solReceived || 0);
        totalRealizedPnLSOL += walletPnL.realizedPnLSOL;
        totalUnrealizedPnLSOL += walletPnL.unrealizedPnLSOL;
        
        return {
            address: wallet.address,
            pnl: walletPnL
        };
    });

    const currentHoldings = Math.max(0, totalTokensBought - totalTokensSold);
    const avgBuyPriceSOL = totalTokensBought > 0 ? totalSpentSOL / totalTokensBought : 0;
    const totalPnLSOL = totalRealizedPnLSOL + totalUnrealizedPnLSOL; 
    const soldPercentage = totalTokensBought > 0 ? (totalTokensSold / totalTokensBought) * 100 : 0;

    return {
        totalPnLSOL: roundToDecimals(totalPnLSOL),
        realizedPnLSOL: roundToDecimals(totalRealizedPnLSOL),
        unrealizedPnLSOL: roundToDecimals(totalUnrealizedPnLSOL),
        totalTokensBought: roundToDecimals(totalTokensBought),
        totalTokensSold: roundToDecimals(totalTokensSold),
        currentHoldings: roundToDecimals(currentHoldings),
        totalSpentSOL: roundToDecimals(totalSpentSOL),
        totalReceivedSOL: roundToDecimals(totalReceivedSOL),
        totalPnLUSD: roundToDecimals(totalPnLSOL * (solPrice || 150), 2),
        avgBuyPriceSOL: roundToDecimals(avgBuyPriceSOL),
        soldPercentage: roundToDecimals(soldPercentage, 1),
        walletPnLs
    };
};

export const formatPnL = (pnlSOL, options = {}) => {
    const {
        showSign = true,
        showCurrency = true,
        decimals = 4
    } = options;

    if (!pnlSOL || isNaN(pnlSOL)) return '0.0000 SOL';

    const rounded = roundToDecimals(pnlSOL, decimals);
    const sign = showSign && rounded > 0 ? '+' : '';
    const currency = showCurrency ? ' SOL' : '';

    return `${sign}${rounded.toFixed(decimals)}${currency}`;
};

export const getPnLColor = (pnlValue) => {
    if (!pnlValue || isNaN(pnlValue)) return 'text-gray-400';
    return pnlValue > 0 ? 'text-green-400' : 
           pnlValue < 0 ? 'text-red-400' : 'text-gray-400';
};

export const formatNumber = (num, decimals = 2) => {
    if (num === null || num === undefined || isNaN(num)) return '0';
    if (Math.abs(num) >= 1e9) return `${(num / 1e9).toFixed(1)}B`;
    if (Math.abs(num) >= 1e6) return `${(num / 1e6).toFixed(1)}M`;
    if (Math.abs(num) >= 1e3) return `${(num / 1e3).toFixed(1)}K`;
    return num.toFixed(decimals);
};