/**
 * Risk Metrics Service
 * 
 * Servicio principal que orquesta el cálculo de métricas de riesgo
 * del portafolio. Integra agregación multi-cuenta, cache de benchmark
 * y funciones matemáticas.
 * 
 * @module services/riskMetrics/riskMetricsService
 * @see docs/stories/36.story.md
 */

const { 
  calculateAllMetrics, 
  calculateDrawdownHistory, 
  findMaxDrawdown 
} = require('./mathCalculations');
const { getMarketReturns, getRiskFreeRate } = require('./benchmarkCache');
const { aggregateMultiAccountData, determineStrategy } = require('./multiAccountAggregator');
const { MIN_DAYS_FOR_METRICS, TRADING_DAYS_PER_YEAR } = require('./types');

/**
 * Calcula la fecha de inicio según el período
 * @param {string} period - Período (YTD, 1M, 3M, 6M, 1Y, 2Y, ALL)
 * @returns {string} Fecha en formato YYYY-MM-DD
 */
function getPeriodStartDate(period) {
  const now = new Date();
  let startDate;
  
  switch (period) {
    case '1M':
      startDate = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
      break;
    case '3M':
      startDate = new Date(now.getFullYear(), now.getMonth() - 3, now.getDate());
      break;
    case '6M':
      startDate = new Date(now.getFullYear(), now.getMonth() - 6, now.getDate());
      break;
    case 'YTD':
      startDate = new Date(now.getFullYear(), 0, 1);
      break;
    case '1Y':
      startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
      break;
    case '2Y':
      startDate = new Date(now.getFullYear() - 2, now.getMonth(), now.getDate());
      break;
    case 'ALL':
      startDate = new Date(2000, 0, 1);
      break;
    default:
      startDate = new Date(now.getFullYear(), 0, 1);
  }
  
  return startDate.toISOString().split('T')[0];
}

/**
 * Formatea fecha a YYYY-MM-DD
 * @param {Date} date 
 * @returns {string}
 */
function formatDateToISO(date) {
  return date.toISOString().split('T')[0];
}

/**
 * Determina la calidad de los datos según cantidad de puntos
 * @param {number} dataPoints - Número de puntos de datos
 * @returns {'excellent' | 'good' | 'limited' | 'insufficient'}
 */
function getDataQuality(dataPoints) {
  if (dataPoints >= TRADING_DAYS_PER_YEAR) return 'excellent';
  if (dataPoints >= 90) return 'good';
  if (dataPoints >= MIN_DAYS_FOR_METRICS) return 'limited';
  return 'insufficient';
}

/**
 * Obtiene las semanas rentables desde portfolioMetrics
 * @param {Object} db - Instancia de Firestore
 * @param {string} userId - ID del usuario
 * @param {string} accountId - ID de cuenta ('overall' o específico)
 * @param {string} currency - Moneda
 * @returns {Promise<number>} Porcentaje de semanas rentables
 */
async function fetchProfitableWeeks(db, userId, accountId, currency) {
  try {
    const metricsPath = accountId === 'overall'
      ? `portfolioMetrics/${userId}/weeklyMetrics/latest`
      : `portfolioMetrics/${userId}/accounts/${accountId}/weeklyMetrics/latest`;
    
    const metricsDoc = await db.doc(metricsPath).get();
    
    if (metricsDoc.exists) {
      const latestMetrics = metricsDoc.data();
      if (latestMetrics.metrics && latestMetrics.metrics[currency]) {
        return latestMetrics.metrics[currency].profitableWeeksPercentage || 50;
      }
    }
    return 50;
  } catch (error) {
    console.warn('[riskMetricsService] Error fetching profitable weeks:', error);
    return 50;
  }
}

/**
 * Calcula métricas de riesgo completas para un usuario
 * 
 * @param {string} userId - ID del usuario
 * @param {Object} options - Opciones de cálculo
 * @param {string} [options.period='YTD'] - Período de análisis
 * @param {string} [options.currency='USD'] - Moneda
 * @param {string[]} [options.accountIds=[]] - IDs de cuentas (vacío = overall)
 * @param {string} [options.requestId] - ID de request para logging
 * @returns {Promise<Object>} Resultado con métricas y metadata
 */
async function calculateRiskMetrics(userId, options = {}) {
  const {
    period = 'YTD',
    currency = 'USD',
    accountIds = [],
    requestId = 'unknown'
  } = options;
  
  const startTime = Date.now();
  console.log(`[riskMetricsService] Starting calculation`, {
    requestId,
    userId,
    period,
    currency,
    accountIds: accountIds.length || 'overall'
  });
  
  const admin = require('../firebaseAdmin');
  const db = admin.firestore();
  
  try {
    const startDate = getPeriodStartDate(period);
    const endDate = formatDateToISO(new Date());
    
    const [portfolioData, marketData, riskFreeRate] = await Promise.all([
      aggregateMultiAccountData(userId, accountIds, startDate, endDate, currency),
      getMarketReturns(startDate, endDate),
      getRiskFreeRate()
    ]);
    
    const dataPoints = portfolioData.dailyReturns.length;
    const dataQuality = getDataQuality(dataPoints);
    
    if (dataQuality === 'insufficient') {
      console.log(`[riskMetricsService] Insufficient data: ${dataPoints} points`);
      return {
        success: false,
        error: 'INSUFFICIENT_DATA',
        message: `Need at least ${MIN_DAYS_FOR_METRICS} data points, found ${dataPoints}`,
        metadata: {
          calculatedAt: new Date().toISOString(),
          period,
          currency,
          dataPointsCount: dataPoints,
          dataQuality,
          requestId,
          durationMs: Date.now() - startTime
        }
      };
    }
    
    const marketReturns = marketData.map(d => d.dailyReturn);
    
    const metrics = calculateAllMetrics(
      portfolioData.dailyReturns,
      marketReturns,
      { riskFreeRate }
    );
    
    const drawdownHistory = calculateDrawdownHistory(portfolioData.dailyData);
    const maxDrawdown = findMaxDrawdown(drawdownHistory);
    
    const strategy = determineStrategy(accountIds);
    const primaryAccountId = strategy === 'overall' ? 'overall' : 
      (strategy === 'single' ? accountIds[0] : 'overall');
    
    const profitableWeeks = await fetchProfitableWeeks(db, userId, primaryAccountId, currency);
    
    const result = {
      success: true,
      
      sharpeRatio: parseFloat(metrics.sharpeRatio.toFixed(2)),
      sortinoRatio: parseFloat(metrics.sortinoRatio.toFixed(2)),
      beta: parseFloat(metrics.beta.toFixed(2)),
      volatility: parseFloat(metrics.volatility.toFixed(1)),
      annualizedReturn: parseFloat(metrics.annualizedReturn.toFixed(1)),
      maxDrawdown: parseFloat(maxDrawdown.toFixed(1)),
      valueAtRisk95: parseFloat(metrics.valueAtRisk95.toFixed(2)),
      correlation: parseFloat(metrics.correlation.toFixed(2)),
      profitableWeeks: parseFloat(profitableWeeks.toFixed(0)),
      
      drawdownHistory: drawdownHistory.map(d => ({
        date: d.date,
        drawdown: parseFloat(d.drawdownPercent.toFixed(2)),
        value: parseFloat(d.portfolioValue.toFixed(2)),
        peak: parseFloat(d.peakValue.toFixed(2)),
        isMaxDrawdown: d.isMaxDrawdown
      })),
      
      metadata: {
        calculatedAt: new Date().toISOString(),
        period,
        currency,
        startDate,
        endDate,
        dataPointsCount: dataPoints,
        marketDataPoints: marketReturns.length,
        dataQuality,
        aggregationStrategy: portfolioData.strategy,
        aggregationMethod: portfolioData.metadata.aggregationMethod,
        accountsIncluded: portfolioData.metadata.accountsIncluded,
        accountsRequested: portfolioData.accountsRequested,
        accountsProcessed: portfolioData.accountsProcessed,
        requestId,
        durationMs: Date.now() - startTime
      }
    };
    
    console.log(`[riskMetricsService] Calculation complete`, {
      requestId,
      durationMs: result.metadata.durationMs,
      dataPoints,
      strategy: result.metadata.aggregationStrategy
    });
    
    return result;
    
  } catch (error) {
    console.error(`[riskMetricsService] Error:`, {
      requestId,
      error: error.message,
      stack: error.stack
    });
    
    return {
      success: false,
      error: error.message.includes('INSUFFICIENT_DATA') ? 'INSUFFICIENT_DATA' : 'CALCULATION_ERROR',
      message: error.message,
      metadata: {
        calculatedAt: new Date().toISOString(),
        period,
        currency,
        requestId,
        durationMs: Date.now() - startTime
      }
    };
  }
}

module.exports = {
  calculateRiskMetrics,
  getPeriodStartDate,
  formatDateToISO,
  getDataQuality
};
