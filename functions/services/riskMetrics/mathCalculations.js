/**
 * Risk Metrics Math Calculations
 * 
 * Funciones matemáticas puras para cálculo de métricas de riesgo.
 * Migradas desde lib/riskMetricsUtils.ts del frontend.
 * 
 * @module services/riskMetrics/mathCalculations
 * @see docs/stories/36.story.md
 */

const { TRADING_DAYS_PER_YEAR, DEFAULT_BENCHMARKS } = require('./types');

/**
 * Calcula el retorno promedio diario
 * @param {number[]} returns - Array de retornos diarios
 * @returns {number} Retorno promedio
 */
function calculateMeanReturn(returns) {
  if (!returns || returns.length === 0) return 0;
  return returns.reduce((sum, r) => sum + r, 0) / returns.length;
}

/**
 * Calcula la varianza de una serie de retornos
 * @param {number[]} returns - Array de retornos
 * @param {number} [mean] - Media pre-calculada (opcional)
 * @returns {number} Varianza
 */
function calculateVariance(returns, mean) {
  if (!returns || returns.length < 2) return 0;
  const avg = mean !== undefined ? mean : calculateMeanReturn(returns);
  const squaredDiffs = returns.map(r => Math.pow(r - avg, 2));
  return squaredDiffs.reduce((sum, d) => sum + d, 0) / (returns.length - 1);
}

/**
 * Calcula la desviación estándar
 * @param {number[]} returns - Array de retornos
 * @param {number} [mean] - Media pre-calculada (opcional)
 * @returns {number} Desviación estándar
 */
function calculateStdDev(returns, mean) {
  return Math.sqrt(calculateVariance(returns, mean));
}

/**
 * Calcula la downside deviation (desviación de retornos negativos)
 * 
 * Fórmula estándar Sortino & Price (1994): usa TODOS los retornos,
 * aplica min(r - threshold, 0)² y divide por N_total.
 * 
 * @param {number[]} returns - Array de retornos
 * @param {number} [threshold=0] - Umbral de retorno (default: 0)
 * @returns {number} Downside deviation
 */
function calculateDownsideDeviation(returns, threshold = 0) {
  if (!returns || returns.length === 0) return 0;
  
  const squaredDiffs = returns.map(r => {
    const diff = r - threshold;
    return diff < 0 ? diff * diff : 0;
  });
  const sumSquared = squaredDiffs.reduce((sum, d) => sum + d, 0);
  if (sumSquared === 0) return 0;
  return Math.sqrt(sumSquared / returns.length);
}

/**
 * Anualiza un retorno diario promedio
 * @param {number} dailyReturn - Retorno diario como decimal
 * @returns {number} Retorno anualizado como porcentaje
 */
function annualizeReturn(dailyReturn) {
  return (Math.pow(1 + dailyReturn, TRADING_DAYS_PER_YEAR) - 1) * 100;
}

/**
 * Anualiza la volatilidad diaria
 * @param {number} dailyStdDev - Desviación estándar diaria
 * @returns {number} Volatilidad anualizada como porcentaje
 */
function annualizeVolatility(dailyStdDev) {
  return dailyStdDev * Math.sqrt(TRADING_DAYS_PER_YEAR) * 100;
}

/**
 * Calcula el Sharpe Ratio anualizado
 * @param {number} annualizedReturn - Retorno anualizado (%)
 * @param {number} annualizedVolatility - Volatilidad anualizada (%)
 * @param {number} [riskFreeRate] - Tasa libre de riesgo anualizada
 * @returns {number} Sharpe Ratio
 */
function calculateSharpeRatio(annualizedReturn, annualizedVolatility, riskFreeRate) {
  const rfr = riskFreeRate !== undefined ? riskFreeRate : DEFAULT_BENCHMARKS.RISK_FREE_RATE * 100;
  if (annualizedVolatility === 0 || annualizedVolatility < 0.01) return 0;
  return (annualizedReturn - rfr) / annualizedVolatility;
}

/**
 * Calcula el Sortino Ratio anualizado
 * @param {number} annualizedReturn - Retorno anualizado (%)
 * @param {number} annualizedDownsideDeviation - Downside deviation anualizada (%)
 * @param {number} [riskFreeRate] - Tasa libre de riesgo anualizada
 * @returns {number} Sortino Ratio
 */
function calculateSortinoRatio(annualizedReturn, annualizedDownsideDeviation, riskFreeRate) {
  const rfr = riskFreeRate !== undefined ? riskFreeRate : DEFAULT_BENCHMARKS.RISK_FREE_RATE * 100;
  if (annualizedDownsideDeviation === 0 || annualizedDownsideDeviation < 0.01) return 0;
  return (annualizedReturn - rfr) / annualizedDownsideDeviation;
}

/**
 * Calcula la covarianza entre dos series de retornos
 * @param {number[]} returns1 - Primera serie de retornos
 * @param {number[]} returns2 - Segunda serie de retornos
 * @returns {number} Covarianza
 */
function calculateCovariance(returns1, returns2) {
  const n = Math.min(returns1.length, returns2.length);
  if (n < 2) return 0;
  
  const mean1 = calculateMeanReturn(returns1.slice(0, n));
  const mean2 = calculateMeanReturn(returns2.slice(0, n));
  
  let sum = 0;
  for (let i = 0; i < n; i++) {
    sum += (returns1[i] - mean1) * (returns2[i] - mean2);
  }
  
  return sum / (n - 1);
}

/**
 * Calcula el Beta del portafolio vs el mercado
 * @param {number[]} portfolioReturns - Retornos del portafolio
 * @param {number[]} marketReturns - Retornos del mercado (benchmark)
 * @returns {number} Beta
 */
function calculateBeta(portfolioReturns, marketReturns) {
  if (!portfolioReturns || !marketReturns) return 1;
  
  const covariance = calculateCovariance(portfolioReturns, marketReturns);
  const marketVariance = calculateVariance(marketReturns);
  
  if (marketVariance === 0 || marketVariance < 0.0001) return 1;
  
  return covariance / marketVariance;
}

/**
 * Calcula la correlación entre dos series
 * @param {number[]} returns1 - Primera serie de retornos
 * @param {number[]} returns2 - Segunda serie de retornos
 * @returns {number} Correlación (-1 a 1)
 */
function calculateCorrelation(returns1, returns2) {
  const n = Math.min(returns1.length, returns2.length);
  if (n < 2) return 0;
  
  const stdDev1 = calculateStdDev(returns1.slice(0, n));
  const stdDev2 = calculateStdDev(returns2.slice(0, n));
  
  if (stdDev1 === 0 || stdDev2 === 0) return 0;
  
  const covariance = calculateCovariance(returns1, returns2);
  return covariance / (stdDev1 * stdDev2);
}

/**
 * Calcula el Value at Risk al 95% (percentil 5 de pérdidas)
 * @param {number[]} returns - Array de retornos diarios
 * @returns {number} VaR 95% como porcentaje (negativo)
 */
function calculateVaR95(returns) {
  if (!returns || returns.length === 0) return 0;
  
  const sorted = [...returns].sort((a, b) => a - b);
  const index = Math.floor(returns.length * 0.05);
  return sorted[index] * 100;
}

/**
 * Calcula el historial de drawdowns basado en rendimientos acumulados
 * 
 * Usamos los rendimientos diarios para construir un índice de rendimiento
 * en lugar del valor absoluto del portafolio. Esto asegura que el drawdown 
 * sea coherente con los rendimientos TWR, aislando el efecto de depósitos/retiros.
 * 
 * @param {Array<{date: string, dailyReturn: number}>} data - Datos de rendimiento diario
 * @returns {Array<{date: string, portfolioValue: number, peakValue: number, drawdownPercent: number, isMaxDrawdown: boolean, daysFromPeak: number}>}
 */
function calculateDrawdownHistory(data) {
  if (!data || data.length === 0) return [];
  
  const drawdowns = [];
  
  let cumulativeIndex = 100;
  let peakIndex = 100;
  let peakDate = data[0].date;
  let peakDateIndex = 0;
  let maxDrawdown = 0;
  let maxDrawdownIndex = 0;
  
  for (let i = 0; i < data.length; i++) {
    const { date, dailyReturn } = data[i];
    
    cumulativeIndex = cumulativeIndex * (1 + dailyReturn);
    
    if (cumulativeIndex > peakIndex) {
      peakIndex = cumulativeIndex;
      peakDate = date;
      peakDateIndex = i;
    }
    
    const drawdownPercent = ((cumulativeIndex - peakIndex) / peakIndex) * 100;
    
    if (drawdownPercent < maxDrawdown) {
      maxDrawdown = drawdownPercent;
      maxDrawdownIndex = i;
    }
    
    drawdowns.push({
      date,
      portfolioValue: cumulativeIndex,
      peakValue: peakIndex,
      drawdownPercent,
      isMaxDrawdown: false,
      daysFromPeak: i - peakDateIndex
    });
  }
  
  if (drawdowns[maxDrawdownIndex]) {
    drawdowns[maxDrawdownIndex].isMaxDrawdown = true;
  }
  
  return drawdowns;
}

/**
 * Encuentra el máximo drawdown de un historial
 * @param {Array<{drawdownPercent: number}>} drawdowns - Historial de drawdowns
 * @returns {number} Máximo drawdown (negativo)
 */
function findMaxDrawdown(drawdowns) {
  if (!drawdowns || drawdowns.length === 0) return 0;
  return Math.min(...drawdowns.map(d => d.drawdownPercent));
}

/**
 * Calcula el porcentaje de semanas rentables a partir de retornos diarios
 * Agrupa retornos consecutivos en semanas de 5 días de trading y cuenta las positivas
 * 
 * @param {number[]} dailyReturns - Retornos diarios como decimales
 * @returns {number} Porcentaje de semanas rentables (0-100)
 */
function calculateWeeklyProfitability(dailyReturns) {
  if (!dailyReturns || dailyReturns.length < 5) return 50;
  
  const weeklyReturns = [];
  for (let i = 0; i < dailyReturns.length; i += 5) {
    const weekSlice = dailyReturns.slice(i, i + 5);
    if (weekSlice.length >= 3) {
      const weekReturn = weekSlice.reduce((acc, r) => acc * (1 + r), 1) - 1;
      weeklyReturns.push(weekReturn);
    }
  }
  
  if (weeklyReturns.length === 0) return 50;
  
  const profitable = weeklyReturns.filter(r => r > 0).length;
  return (profitable / weeklyReturns.length) * 100;
}

/**
 * Calcula métricas de riesgo del benchmark (S&P 500) para el mismo período
 * Reutiliza las funciones de cálculo del portafolio para garantizar consistencia
 * 
 * @param {number[]} marketReturns - Retornos diarios del mercado (todos, sin alinear)
 * @param {Object} options - { riskFreeRate: decimal, ej. 0.043 }
 * @returns {Object} Métricas del benchmark
 */
function calculateBenchmarkMetrics(marketReturns, options = {}) {
  const { riskFreeRate = DEFAULT_BENCHMARKS.RISK_FREE_RATE } = options;
  
  if (!marketReturns || marketReturns.length === 0) {
    return {
      sharpeRatio: 0, sortinoRatio: 0, volatility: 0,
      annualizedReturn: 0, maxDrawdown: 0, totalReturn: 0,
      profitableWeeks: 0, dataPoints: 0
    };
  }
  
  const meanReturn = calculateMeanReturn(marketReturns);
  const stdDev = calculateStdDev(marketReturns, meanReturn);
  const downsideDev = calculateDownsideDeviation(marketReturns);
  
  const totalReturn = marketReturns.reduce((acc, r) => acc * (1 + r), 1) - 1;
  const tradingDays = marketReturns.length;
  const annualizedReturn = (Math.pow(1 + totalReturn, TRADING_DAYS_PER_YEAR / tradingDays) - 1) * 100;
  
  const annualizedVol = annualizeVolatility(stdDev);
  const annualizedDownside = annualizeVolatility(downsideDev);
  
  const sharpeRatio = calculateSharpeRatio(annualizedReturn, annualizedVol, riskFreeRate * 100);
  const sortinoRatio = calculateSortinoRatio(annualizedReturn, annualizedDownside, riskFreeRate * 100);
  
  const drawdownInput = marketReturns.map((r, i) => ({ date: `day-${i}`, dailyReturn: r }));
  const drawdownHistory = calculateDrawdownHistory(drawdownInput);
  const maxDrawdown = findMaxDrawdown(drawdownHistory);
  
  const profitableWeeks = calculateWeeklyProfitability(marketReturns);
  
  return {
    sharpeRatio: parseFloat(sharpeRatio.toFixed(2)),
    sortinoRatio: parseFloat(sortinoRatio.toFixed(2)),
    volatility: parseFloat(annualizedVol.toFixed(1)),
    annualizedReturn: parseFloat(annualizedReturn.toFixed(1)),
    maxDrawdown: parseFloat(maxDrawdown.toFixed(1)),
    totalReturn: parseFloat((totalReturn * 100).toFixed(1)),
    profitableWeeks: parseFloat(profitableWeeks.toFixed(0)),
    dataPoints: marketReturns.length
  };
}

/**
 * Calcula todas las métricas de riesgo a partir de datos de retorno
 * @param {number[]} portfolioReturns - Retornos diarios del portafolio
 * @param {number[]} marketReturns - Retornos diarios del mercado
 * @param {Object} options - Opciones adicionales
 * @returns {Object} Métricas calculadas
 */
/**
 * Calcula todas las métricas de riesgo
 * @param {number[]} allPortfolioReturns - Todos los retornos diarios del portafolio (para Sharpe, Vol, etc.)
 * @param {number[]} alignedPortfolioReturns - Retornos alineados con el mercado (para Beta, Correlación)
 * @param {number[]} marketReturns - Retornos del mercado (alineados)
 * @param {Object} options - Opciones (riskFreeRate)
 * @returns {Object} Todas las métricas calculadas
 */
function calculateAllMetrics(allPortfolioReturns, alignedPortfolioReturns, marketReturns, options = {}) {
  const { riskFreeRate = DEFAULT_BENCHMARKS.RISK_FREE_RATE } = options;
  
  if (!allPortfolioReturns || allPortfolioReturns.length === 0) {
    return {
      sharpeRatio: 0,
      sortinoRatio: 0,
      beta: 1,
      volatility: 0,
      annualizedReturn: 0,
      valueAtRisk95: 0,
      correlation: 0
    };
  }
  
  // Métricas del portafolio usando TODOS los retornos
  const meanReturn = calculateMeanReturn(allPortfolioReturns);
  const stdDev = calculateStdDev(allPortfolioReturns, meanReturn);
  const downsideDev = calculateDownsideDeviation(allPortfolioReturns);
  
  // STORY-036 FIX: Calcular retorno anualizado usando retorno compuesto total
  const totalReturn = allPortfolioReturns.reduce((acc, r) => acc * (1 + r), 1) - 1;
  const tradingDays = allPortfolioReturns.length;
  const annualizedRet = (Math.pow(1 + totalReturn, TRADING_DAYS_PER_YEAR / tradingDays) - 1) * 100;
  
  const annualizedVol = annualizeVolatility(stdDev);
  const annualizedDownside = annualizeVolatility(downsideDev);
  
  // Métricas comparativas usando retornos ALINEADOS
  const beta = calculateBeta(alignedPortfolioReturns, marketReturns);
  const correlation = calculateCorrelation(alignedPortfolioReturns, marketReturns);
  
  return {
    meanDailyReturn: meanReturn,
    totalReturn: totalReturn * 100,
    annualizedReturn: annualizedRet,
    volatility: annualizedVol,
    sharpeRatio: calculateSharpeRatio(annualizedRet, annualizedVol, riskFreeRate * 100),
    sortinoRatio: calculateSortinoRatio(annualizedRet, annualizedDownside, riskFreeRate * 100),
    beta,
    valueAtRisk95: calculateVaR95(allPortfolioReturns),
    correlation
  };
}

module.exports = {
  calculateMeanReturn,
  calculateVariance,
  calculateStdDev,
  calculateDownsideDeviation,
  annualizeReturn,
  annualizeVolatility,
  calculateSharpeRatio,
  calculateSortinoRatio,
  calculateCovariance,
  calculateBeta,
  calculateCorrelation,
  calculateVaR95,
  calculateDrawdownHistory,
  findMaxDrawdown,
  calculateWeeklyProfitability,
  calculateBenchmarkMetrics,
  calculateAllMetrics
};
