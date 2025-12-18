/**
 * Summary Calculator
 * 
 * Calcula métricas agregadas y resúmenes de posiciones cerradas.
 * Incluye desglose por cuenta para soporte multi-cuenta.
 * 
 * @module services/closedPositions/summaryCalculator
 * @see docs/stories/36.story.md
 */

/**
 * Calcula el resumen de todas las posiciones cerradas
 * 
 * @param {Array} positions - Array de posiciones cerradas
 * @returns {Object} Resumen con métricas agregadas
 */
function calculateSummary(positions) {
  if (!positions || positions.length === 0) {
    return {
      totalRealizedPnL: 0,
      totalTrades: 0,
      winningTrades: 0,
      losingTrades: 0,
      winRate: 0,
      avgHoldingPeriod: 0,
      avgPnLPercent: 0,
      bestTrade: 0,
      worstTrade: 0,
      totalInvested: 0,
      totalReceived: 0,
      byAccount: []
    };
  }
  
  const totalTrades = positions.length;
  const winningTrades = positions.filter(p => p.realizedPnL > 0).length;
  const losingTrades = positions.filter(p => p.realizedPnL < 0).length;
  
  const totalRealizedPnL = positions.reduce((sum, p) => sum + p.realizedPnL, 0);
  const totalHoldingDays = positions.reduce((sum, p) => sum + p.holdingPeriodDays, 0);
  const totalPnLPercent = positions.reduce((sum, p) => sum + p.realizedPnLPercent, 0);
  const totalInvested = positions.reduce((sum, p) => sum + p.totalInvested, 0);
  const totalReceived = positions.reduce((sum, p) => sum + p.totalReceived, 0);
  
  const pnlValues = positions.map(p => p.realizedPnL);
  
  return {
    totalRealizedPnL: parseFloat(totalRealizedPnL.toFixed(2)),
    totalTrades,
    winningTrades,
    losingTrades,
    winRate: parseFloat(((winningTrades / totalTrades) * 100).toFixed(1)),
    avgHoldingPeriod: Math.round(totalHoldingDays / totalTrades),
    avgPnLPercent: parseFloat((totalPnLPercent / totalTrades).toFixed(2)),
    bestTrade: parseFloat(Math.max(...pnlValues).toFixed(2)),
    worstTrade: parseFloat(Math.min(...pnlValues).toFixed(2)),
    totalInvested: parseFloat(totalInvested.toFixed(2)),
    totalReceived: parseFloat(totalReceived.toFixed(2))
  };
}

/**
 * Calcula desglose por cuenta (para soporte multi-cuenta)
 * 
 * @param {Array} positions - Array de posiciones cerradas
 * @returns {Array} Desglose por cuenta
 */
function calculateByAccountBreakdown(positions) {
  if (!positions || positions.length === 0) {
    return [];
  }
  
  const byAccount = new Map();
  
  for (const pos of positions) {
    const accountId = pos.portfolioAccountId;
    const existing = byAccount.get(accountId) || {
      accountId,
      accountName: pos.accountName,
      trades: 0,
      winningTrades: 0,
      pnl: 0,
      totalInvested: 0
    };
    
    existing.trades += 1;
    if (pos.realizedPnL > 0) existing.winningTrades += 1;
    existing.pnl += pos.realizedPnL;
    existing.totalInvested += pos.totalInvested;
    
    byAccount.set(accountId, existing);
  }
  
  return Array.from(byAccount.values()).map(acc => ({
    accountId: acc.accountId,
    accountName: acc.accountName,
    trades: acc.trades,
    pnl: parseFloat(acc.pnl.toFixed(2)),
    winRate: parseFloat(((acc.winningTrades / acc.trades) * 100).toFixed(1)),
    pnlPercent: acc.totalInvested > 0 
      ? parseFloat(((acc.pnl / acc.totalInvested) * 100).toFixed(2))
      : 0
  }));
}

/**
 * Calcula datos acumulados de P&L para gráfico
 * 
 * @param {Array} positions - Array de posiciones cerradas (ordenadas por fecha)
 * @returns {Array} Datos de P&L acumulado
 */
function calculateCumulativePnL(positions) {
  if (!positions || positions.length === 0) {
    return [];
  }
  
  // Ordenar por fecha de venta
  const sorted = [...positions].sort((a, b) => 
    new Date(a.sellDate).getTime() - new Date(b.sellDate).getTime()
  );
  
  let cumulative = 0;
  const data = [];
  
  for (const pos of sorted) {
    cumulative += pos.realizedPnL;
    data.push({
      date: typeof pos.sellDate === 'string' 
        ? pos.sellDate.split('T')[0] 
        : pos.sellDate.toISOString().split('T')[0],
      ticker: pos.ticker,
      pnl: parseFloat(pos.realizedPnL.toFixed(2)),
      cumulativePnL: parseFloat(cumulative.toFixed(2)),
      isWin: pos.realizedPnL > 0
    });
  }
  
  return data;
}

/**
 * Calcula la distribución de trades por rango de P&L
 * 
 * @param {Array} positions - Array de posiciones cerradas
 * @returns {Array} Distribución de trades
 */
function calculateTradeDistribution(positions) {
  const buckets = [
    { label: '< -50%', min: -Infinity, max: -50, count: 0 },
    { label: '-50% a -20%', min: -50, max: -20, count: 0 },
    { label: '-20% a -10%', min: -20, max: -10, count: 0 },
    { label: '-10% a 0%', min: -10, max: 0, count: 0 },
    { label: '0% a 10%', min: 0, max: 10, count: 0 },
    { label: '10% a 20%', min: 10, max: 20, count: 0 },
    { label: '20% a 50%', min: 20, max: 50, count: 0 },
    { label: '> 50%', min: 50, max: Infinity, count: 0 }
  ];
  
  for (const pos of positions) {
    const pct = pos.realizedPnLPercent;
    for (const bucket of buckets) {
      if (pct >= bucket.min && pct < bucket.max) {
        bucket.count += 1;
        break;
      }
    }
  }
  
  return buckets.map(b => ({
    label: b.label,
    count: b.count,
    percentage: positions.length > 0 
      ? parseFloat(((b.count / positions.length) * 100).toFixed(1))
      : 0
  }));
}

/**
 * Calcula análisis de período de holding
 * 
 * @param {Array} positions - Array de posiciones cerradas
 * @returns {Object} Análisis de holding period
 */
function calculateHoldingPeriodAnalysis(positions) {
  if (!positions || positions.length === 0) {
    return {
      shortTerm: { count: 0, avgPnL: 0, winRate: 0 },
      mediumTerm: { count: 0, avgPnL: 0, winRate: 0 },
      longTerm: { count: 0, avgPnL: 0, winRate: 0 }
    };
  }
  
  const categories = {
    shortTerm: { positions: [], label: '<30 días' },
    mediumTerm: { positions: [], label: '30-365 días' },
    longTerm: { positions: [], label: '>365 días' }
  };
  
  for (const pos of positions) {
    if (pos.holdingPeriodDays < 30) {
      categories.shortTerm.positions.push(pos);
    } else if (pos.holdingPeriodDays <= 365) {
      categories.mediumTerm.positions.push(pos);
    } else {
      categories.longTerm.positions.push(pos);
    }
  }
  
  const calculateCategoryStats = (positions) => {
    if (positions.length === 0) {
      return { count: 0, avgPnL: 0, avgPnLPercent: 0, winRate: 0 };
    }
    const wins = positions.filter(p => p.realizedPnL > 0).length;
    const totalPnL = positions.reduce((sum, p) => sum + p.realizedPnL, 0);
    const totalPct = positions.reduce((sum, p) => sum + p.realizedPnLPercent, 0);
    
    return {
      count: positions.length,
      avgPnL: parseFloat((totalPnL / positions.length).toFixed(2)),
      avgPnLPercent: parseFloat((totalPct / positions.length).toFixed(2)),
      winRate: parseFloat(((wins / positions.length) * 100).toFixed(1))
    };
  };
  
  return {
    shortTerm: calculateCategoryStats(categories.shortTerm.positions),
    mediumTerm: calculateCategoryStats(categories.mediumTerm.positions),
    longTerm: calculateCategoryStats(categories.longTerm.positions)
  };
}

/**
 * Calcula resumen completo incluyendo todos los breakdowns
 * 
 * @param {Array} positions - Array de posiciones cerradas
 * @returns {Object} Resumen completo
 */
function calculateFullSummary(positions) {
  const baseSummary = calculateSummary(positions);
  const byAccount = calculateByAccountBreakdown(positions);
  
  return {
    ...baseSummary,
    byAccount
  };
}

module.exports = {
  calculateSummary,
  calculateByAccountBreakdown,
  calculateCumulativePnL,
  calculateTradeDistribution,
  calculateHoldingPeriodAnalysis,
  calculateFullSummary
};
