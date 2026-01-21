/**
 * Attribution Service
 * 
 * Servicio principal que orquesta el cálculo de atribución del portafolio.
 * Coordina los servicios de cálculo de contribuciones, waterfall y resumen.
 * 
 * MEJORADO: Ahora usa TWR (Time-Weighted Return) del período correcto
 * en lugar de totalROI para mayor precisión.
 * 
 * @module services/attribution/attributionService
 * @see docs/architecture/portfolio-attribution-coherence-analysis.md
 */

const { calculateContributions, enrichWithCurrentPrices, findNearestPerformanceData } = require('./contributionCalculator');
const { generateWaterfallFromContributions } = require('./waterfallGenerator');
const { generateSummary } = require('./summaryGenerator');
const { getPeriodLabel, getPeriodStartDate } = require('./types');

const admin = require('../firebaseAdmin');
const db = admin.firestore();

/**
 * Calcula el TWR (Time-Weighted Return) para un período específico
 * usando los cambios diarios ajustados de portfolioPerformance
 * 
 * @param {string} userId - ID del usuario
 * @param {string} period - Período ('YTD', '1M', '3M', etc.)
 * @param {string} currency - Moneda
 * @param {string} accountId - ID de cuenta o 'overall'
 * @returns {Promise<{twr: number, hasData: boolean, docsCount: number}>}
 */
async function calculatePeriodTWR(userId, period, currency, accountId = 'overall') {
  const periodStartDate = getPeriodStartDate(period);
  const periodStartStr = periodStartDate.toISOString().split('T')[0];
  
  // Obtener todos los documentos desde el inicio del período
  const path = accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;
  
  const docsSnapshot = await db.collection(path)
    .where('date', '>=', periodStartStr)
    .orderBy('date', 'asc')
    .get();
  
  if (docsSnapshot.empty) {
    return { twr: 0, hasData: false, docsCount: 0 };
  }
  
  // Calcular TWR componiendo los cambios diarios ajustados
  let compoundFactor = 1.0;
  let validDaysCount = 0;
  
  for (const doc of docsSnapshot.docs) {
    const data = doc.data();
    const currencyData = data[currency] || data.USD || {};
    
    // Usar adjustedDailyChangePercentage (cambio diario ajustado por flujos)
    const dailyChange = currencyData.adjustedDailyChangePercentage || 0;
    
    // Componer: factor = factor * (1 + cambio/100)
    if (dailyChange !== 0) {
      compoundFactor *= (1 + dailyChange / 100);
      validDaysCount++;
    }
  }
  
  // TWR = (factor final - 1) * 100
  const twr = (compoundFactor - 1) * 100;
  
  console.log(`[Attribution] TWR ${period} (${accountId}): ${docsSnapshot.size} docs, ${validDaysCount} con cambios, TWR=${twr.toFixed(2)}%`);
  
  return { 
    twr, 
    hasData: validDaysCount > 0, 
    docsCount: docsSnapshot.size 
  };
}

/**
 * Calcula el TWR ponderado para múltiples cuentas
 * Replica la lógica de useMultiAccountHistoricalReturns del frontend
 * 
 * @param {string} userId - ID del usuario
 * @param {string} period - Período
 * @param {string} currency - Moneda
 * @param {string[]} accountIds - IDs de cuentas
 * @returns {Promise<{twr: number, hasData: boolean}>}
 */
async function calculateMultiAccountTWR(userId, period, currency, accountIds) {
  // Si es solo 'overall' o una cuenta, usar cálculo simple
  if (accountIds.length === 0 || 
      (accountIds.length === 1 && accountIds[0] === 'overall')) {
    return calculatePeriodTWR(userId, period, currency, 'overall');
  }
  
  if (accountIds.length === 1) {
    return calculatePeriodTWR(userId, period, currency, accountIds[0]);
  }
  
  // Para múltiples cuentas, calcular promedio ponderado por valor
  console.log(`[Attribution] Calculando TWR multi-cuenta para ${accountIds.length} cuentas`);
  
  const periodStartDate = getPeriodStartDate(period);
  const periodStartStr = periodStartDate.toISOString().split('T')[0];
  
  // Recolectar datos de cada cuenta
  const accountsData = [];
  
  for (const accountId of accountIds) {
    if (accountId === 'overall') continue;
    
    const path = `portfolioPerformance/${userId}/accounts/${accountId}/dates`;
    const docsSnapshot = await db.collection(path)
      .where('date', '>=', periodStartStr)
      .orderBy('date', 'asc')
      .get();
    
    if (docsSnapshot.empty) continue;
    
    // Calcular TWR de esta cuenta
    let compoundFactor = 1.0;
    let lastValue = 0;
    
    for (const doc of docsSnapshot.docs) {
      const data = doc.data();
      const currencyData = data[currency] || data.USD || {};
      const dailyChange = currencyData.adjustedDailyChangePercentage || 0;
      
      if (dailyChange !== 0) {
        compoundFactor *= (1 + dailyChange / 100);
      }
      
      // Guardar el último valor para ponderar
      lastValue = currencyData.totalValue || 0;
    }
    
    const twr = (compoundFactor - 1) * 100;
    accountsData.push({ accountId, twr, value: lastValue });
  }
  
  if (accountsData.length === 0) {
    return { twr: 0, hasData: false, docsCount: 0 };
  }
  
  // Calcular promedio ponderado por valor actual
  const totalValue = accountsData.reduce((sum, a) => sum + a.value, 0);
  
  if (totalValue === 0) {
    // Si no hay valores, usar promedio simple
    const avgTWR = accountsData.reduce((sum, a) => sum + a.twr, 0) / accountsData.length;
    return { twr: avgTWR, hasData: true, docsCount: accountsData.length };
  }
  
  const weightedTWR = accountsData.reduce((sum, a) => {
    const weight = a.value / totalValue;
    return sum + (a.twr * weight);
  }, 0);
  
  console.log(`[Attribution] TWR multi-cuenta ponderado: ${weightedTWR.toFixed(2)}%`);
  accountsData.forEach(a => {
    const weight = (a.value / totalValue * 100).toFixed(1);
    console.log(`  - ${a.accountId}: TWR=${a.twr.toFixed(2)}%, peso=${weight}%`);
  });
  
  return { twr: weightedTWR, hasData: true, docsCount: accountsData.length };
}

/**
 * Calcula la atribución completa del portafolio
 * 
 * Este es el punto de entrada principal para obtener todos los datos
 * de atribución necesarios para el dashboard.
 * 
 * @param {Object} params - Parámetros de la solicitud
 * @param {string} params.userId - ID del usuario
 * @param {string} params.period - Período de análisis ('YTD', '1M', '3M', etc.)
 * @param {string} params.currency - Moneda para los cálculos (default: 'USD')
 * @param {string[]} params.accountIds - IDs de cuentas o ['overall']
 * @param {Object} params.options - Opciones adicionales
 * @param {number} params.options.benchmarkReturn - Retorno del benchmark
 * @param {number} params.options.maxWaterfallBars - Máximo de barras en waterfall
 * @param {boolean} params.options.includeMetadata - Incluir metadata de debug
 * @param {number} params.options.portfolioReturn - TWR pre-calculado del frontend (opcional)
 * @returns {Promise<Object>} AttributionResponse completo
 */
async function getPortfolioAttribution(params) {
  const {
    userId,
    period = 'YTD',
    currency = 'USD',
    accountIds = ['overall'],
    options = {}
  } = params;
  
  const {
    benchmarkReturn = 0,
    maxWaterfallBars = 8,
    includeMetadata = true,
    portfolioReturn: frontendTWR // TWR pasado desde el frontend
  } = options;
  
  const startTime = Date.now();
  
  try {
    // =========================================================================
    // 1. VALIDACIONES
    // =========================================================================
    if (!userId) {
      throw new Error('userId is required');
    }
    
    const validPeriods = ['1M', '3M', '6M', 'YTD', '1Y', '2Y', 'ALL'];
    if (!validPeriods.includes(period)) {
      throw new Error(`Invalid period: ${period}. Valid values: ${validPeriods.join(', ')}`);
    }
    
    // =========================================================================
    // 2. OBTENER TWR DEL PERÍODO
    // =========================================================================
    // Si el frontend pasó el TWR, usarlo para consistencia
    // Si no, calcular localmente
    let periodTWR;
    let hasTWRData = false;
    
    if (frontendTWR !== undefined && !isNaN(frontendTWR)) {
      periodTWR = frontendTWR;
      hasTWRData = true;
      console.log(`[Attribution] Usando TWR del frontend: ${periodTWR.toFixed(2)}%`);
    } else {
      // Calcular TWR localmente (fallback)
      const twrResult = await calculateMultiAccountTWR(
        userId, 
        period, 
        currency, 
        accountIds
      );
      periodTWR = twrResult.twr;
      hasTWRData = twrResult.hasData;
      console.log(`[Attribution] Usando TWR calculado del período ${period}: ${periodTWR.toFixed(2)}%`);
    }
    
    // =========================================================================
    // 3. CALCULAR CONTRIBUCIONES
    // =========================================================================
    const contributionResult = await calculateContributions(
      userId,
      period,
      currency,
      accountIds
    );
    
    if (contributionResult.error) {
      return {
        success: false,
        error: contributionResult.error,
        assetAttributions: [],
        waterfallData: [],
        summary: null
      };
    }
    
    // Guardar el ROI original para diagnóstico
    contributionResult.originalPortfolioReturn = contributionResult.portfolioReturn;
    
    // IMPORTANTE: Sobrescribir portfolioReturn con el TWR del período
    // El totalROI de assetPerformance es el ROI total desde compra, NO del período
    if (hasTWRData && periodTWR !== 0) {
      console.log(`[Attribution] Reemplazando portfolioReturn: ${contributionResult.portfolioReturn.toFixed(2)}% → ${periodTWR.toFixed(2)}%`);
      contributionResult.portfolioReturn = periodTWR;
      
      // Re-normalizar SOLO contribuciones (pp) con el nuevo portfolioReturn
      // NO normalizar valores absolutos (contributionAbsolute, valueChange)
      const sumOfContributions = contributionResult.attributions.reduce((sum, a) => sum + a.contribution, 0);
      if (Math.abs(sumOfContributions) > 0.01) {
        const normalizationFactor = periodTWR / sumOfContributions;
        for (const attr of contributionResult.attributions) {
          attr.contribution *= normalizationFactor;
          // NO normalizar contributionAbsolute ni valueChange - son valores absolutos en USD
        }
        contributionResult.normalized = true;
        contributionResult.sumOfContributions = periodTWR;
      }
    }
    
    // =========================================================================
    // 4. ENRIQUECER CON DATOS DE CURRENTPRICES
    // =========================================================================
    await enrichWithCurrentPrices(contributionResult.attributions);
    
    // =========================================================================
    // 5. GENERAR WATERFALL
    // =========================================================================
    const waterfallData = generateWaterfallFromContributions(
      contributionResult,
      { maxBars: maxWaterfallBars }
    );
    
    // =========================================================================
    // 6. GENERAR RESUMEN
    // =========================================================================
    const summary = generateSummary(
      contributionResult,
      period,
      { benchmarkReturn }
    );
    
    // =========================================================================
    // 7. PREPARAR RESPUESTA
    // =========================================================================
    const response = {
      success: true,
      assetAttributions: contributionResult.attributions,
      waterfallData,
      summary
    };
    
    // Agregar metadata si se solicita
    if (includeMetadata) {
      response.metadata = {
        calculatedAt: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        dataSource: 'assetPerformance + TWR',
        portfolioDate: contributionResult.latestDate,
        periodStartDate: contributionResult.periodStartDate,
        period,
        periodLabel: getPeriodLabel(period),
        currency,
        accountIds,
        // Info de diagnóstico
        diagnostics: {
          totalAssets: contributionResult.attributions.length,
          sumOfContributions: contributionResult.sumOfContributions,
          periodTWR: hasTWRData ? periodTWR : null,
          originalTotalROI: contributionResult.originalPortfolioReturn,
          portfolioReturn: contributionResult.portfolioReturn,
          discrepancy: contributionResult.discrepancy,
          normalized: contributionResult.normalized
        }
      };
    }
    
    return response;
    
  } catch (error) {
    console.error('[AttributionService] Error:', error);
    
    return {
      success: false,
      error: error.message,
      assetAttributions: [],
      waterfallData: [],
      summary: null,
      metadata: includeMetadata ? {
        calculatedAt: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        error: error.message
      } : undefined
    };
  }
}

/**
 * Obtiene solo el top N de contribuyentes
 * 
 * Versión ligera para casos donde solo se necesitan los principales.
 * 
 * @param {Object} params - Parámetros
 * @param {string} params.userId - ID del usuario
 * @param {string} params.period - Período
 * @param {string} params.currency - Moneda
 * @param {number} params.topN - Cantidad de top contributors
 * @returns {Promise<Object>} Top y bottom contributors
 */
async function getTopContributors(params) {
  const { userId, period = 'YTD', currency = 'USD', topN = 5 } = params;
  
  const result = await getPortfolioAttribution({
    userId,
    period,
    currency,
    options: { includeMetadata: false }
  });
  
  if (!result.success) {
    return result;
  }
  
  const sorted = result.assetAttributions;
  
  return {
    success: true,
    topContributors: sorted.slice(0, topN),
    bottomContributors: sorted.slice(-topN).reverse(),
    portfolioReturn: result.summary?.portfolioReturn || 0,
    totalAssets: sorted.length
  };
}

/**
 * Valida si hay datos de atribución disponibles para un usuario
 * 
 * @param {string} userId - ID del usuario
 * @returns {Promise<Object>} Estado de disponibilidad
 */
async function checkAttributionAvailability(userId) {
  const { getLatestPerformanceData } = require('./contributionCalculator');
  
  const latestData = await getLatestPerformanceData(userId);
  
  if (!latestData) {
    return {
      available: false,
      reason: 'No portfolio performance data found',
      lastUpdate: null
    };
  }
  
  const currencies = ['USD', 'EUR', 'COP', 'MXN', 'BRL'];
  const availableCurrencies = currencies.filter(c => latestData[c]?.assetPerformance);
  
  return {
    available: availableCurrencies.length > 0,
    lastUpdate: latestData.date || latestData.id,
    availableCurrencies,
    assetCount: Object.keys(latestData.USD?.assetPerformance || {}).length
  };
}

module.exports = {
  getPortfolioAttribution,
  getTopContributors,
  checkAttributionAvailability
};
