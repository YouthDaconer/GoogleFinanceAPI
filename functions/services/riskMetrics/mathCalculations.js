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
 * Calcula la desviación estándar solo de retornos negativos (downside deviation)
 * @param {number[]} returns - Array de retornos
 * @param {number} [threshold=0] - Umbral de retorno (default: 0)
 * @returns {number} Downside deviation
 */
function calculateDownsideDeviation(returns, threshold = 0) {
  if (!returns || returns.length === 0) return 0;
  
  const negativeReturns = returns.filter(r => r < threshold);
  if (negativeReturns.length === 0) return 0;
  
  const squaredDiffs = negativeReturns.map(r => Math.pow(r - threshold, 2));
  return Math.sqrt(squaredDiffs.reduce((sum, d) => sum + d, 0) / negativeReturns.length);
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
 * Calcula todas las métricas de riesgo a partir de datos de retorno
 * @param {number[]} portfolioReturns - Retornos diarios del portafolio
 * @param {number[]} marketReturns - Retornos diarios del mercado
 * @param {Object} options - Opciones adicionales
 * @returns {Object} Métricas calculadas
 */
function calculateAllMetrics(portfolioReturns, marketReturns, options = {}) {
  const { riskFreeRate = DEFAULT_BENCHMARKS.RISK_FREE_RATE } = options;
  
  if (!portfolioReturns || portfolioReturns.length === 0) {
    return {
      sharpeRatio: 0,
      sortinoRatio: 0,
      beta: 1,
      volatility: 0,
      valueAtRisk95: 0,
      correlation: 0
    };
  }
  
  const meanReturn = calculateMeanReturn(portfolioReturns);
  const stdDev = calculateStdDev(portfolioReturns, meanReturn);
  const downsideDev = calculateDownsideDeviation(portfolioReturns);
  
  const annualizedRet = annualizeReturn(meanReturn);
  const annualizedVol = annualizeVolatility(stdDev);
  const annualizedDownside = annualizeVolatility(downsideDev);
  
  return {
    meanDailyReturn: meanReturn,
    annualizedReturn: annualizedRet,
    volatility: annualizedVol,
    sharpeRatio: calculateSharpeRatio(annualizedRet, annualizedVol, riskFreeRate * 100),
    sortinoRatio: calculateSortinoRatio(annualizedRet, annualizedDownside, riskFreeRate * 100),
    beta: calculateBeta(portfolioReturns, marketReturns),
    valueAtRisk95: calculateVaR95(portfolioReturns),
    correlation: calculateCorrelation(portfolioReturns, marketReturns)
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
  calculateAllMetrics
};
