/**
 * Attribution Service
 * 
 * Servicio principal que orquesta el cálculo de atribución del portafolio.
 * Coordina los servicios de cálculo de contribuciones, waterfall y resumen.
 * 
 * MEJORADO: Ahora usa TWR (Time-Weighted Return) del período correcto
 * en lugar de totalROI para mayor precisión.
 * 
 * INTRADAY-001: Incluye rendimiento intraday en tiempo real para
 * consistencia con PortfolioSummary del frontend.
 * 
 * @module services/attribution/attributionService
 * @see docs/architecture/portfolio-attribution-coherence-analysis.md
 */

const { calculateContributions, enrichWithCurrentPrices, findNearestPerformanceData, getLatestPerformanceData } = require('./contributionCalculator');
const { generateWaterfallFromContributions } = require('./waterfallGenerator');
const { generateSummary } = require('./summaryGenerator');
const { getPeriodLabel, getPeriodStartDate } = require('./types');
// INTRADAY-001: Importar cálculo intraday para rendimiento en tiempo real
// INTRADAY-002: calculateIntradayContributions para contribuciones por activo
const { calculateIntradayPerformance, calculateIntradayContributions, combineHistoricalWithIntraday } = require('./intradayCalculator');
const { getQuotes } = require('../financeQuery');

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
 * INTRADAY-001: Ahora incluye el rendimiento intraday en tiempo real
 * para consistencia con PortfolioSummary del frontend.
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
 * @param {boolean} params.options.includeIntraday - Incluir rendimiento intraday (default: true)
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
    portfolioReturn: frontendTWR, // TWR pasado desde el frontend
    includeIntraday = true // INTRADAY-001: Incluir rendimiento intraday por defecto
  } = options;
  
  const startTime = Date.now();
  
  // INTRADAY-001: Variables para tracking de intraday
  let intradayPerformance = null;
  let intradayError = null;
  
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
    // 2. OBTENER TWR DEL PERÍODO (HISTÓRICO) + INTRADAY EN PARALELO
    // =========================================================================
    // Calcular TWR histórico y performance intraday en paralelo para mejor performance
    
    let periodTWR = 0; // Inicializar con valor por defecto
    let hasTWRData = false;
    let historicalTWR = 0; // Inicializar con valor por defecto
    
    // INTRADAY-001: Calcular intraday en paralelo si está habilitado
    const promises = [];
    
    // Promise para TWR histórico
    if (frontendTWR !== undefined && !isNaN(frontendTWR)) {
      periodTWR = frontendTWR;
      historicalTWR = frontendTWR;
      hasTWRData = true;
      console.log(`[Attribution] Usando TWR del frontend: ${periodTWR.toFixed(2)}%`);
    } else {
      promises.push(
        calculateMultiAccountTWR(userId, period, currency, accountIds)
          .then(twrResult => {
            periodTWR = twrResult.twr || 0;
            historicalTWR = twrResult.twr || 0;
            hasTWRData = twrResult.hasData;
            console.log(`[Attribution] Usando TWR calculado del período ${period}: ${periodTWR.toFixed(2)}%`);
          })
          .catch(err => {
            console.error(`[Attribution] Error calculando TWR:`, err);
            periodTWR = 0;
            historicalTWR = 0;
            hasTWRData = false;
          })
      );
    }
    
    // Promise para intraday performance (si está habilitado)
    if (includeIntraday) {
      promises.push(
        calculateIntradayPerformance({
          userId,
          currency,
          accountIds
        })
          .then(result => {
            if (result.success) {
              intradayPerformance = result;
              console.log(`[Attribution] Intraday calculado: factor=${result.todayFactor.toFixed(6)}, cambio=${result.dailyChangePercent.toFixed(2)}%`);
            } else {
              intradayError = result.error;
              console.log(`[Attribution] Intraday falló: ${result.error}`);
            }
          })
          .catch(err => {
            intradayError = err.message;
            console.error(`[Attribution] Error calculando intraday:`, err);
          })
      );
    }
    
    // Esperar todas las promises en paralelo
    if (promises.length > 0) {
      await Promise.all(promises);
    }
    
    // INTRADAY-001: Combinar TWR histórico con factor intraday
    // Fórmula TWR: (1 + histórico) × todayFactor - 1
    // 
    // DECISIÓN DE DISEÑO (2026-01-21) - ACTUALIZADO:
    // SIEMPRE aplicar el factor intraday para mantener coherencia con PortfolioSummary.
    // 
    // El frontend combina TWR histórico + cambio desde el último día con datos,
    // sin importar cuántos días hayan pasado (festivos, fines de semana, etc.).
    // Para que Attribution muestre el mismo YTD que el Dashboard, debemos
    // replicar exactamente esa lógica.
    //
    // Semánticamente, lo que el usuario quiere ver es "rendimiento hasta hoy",
    // no "rendimiento hasta el último día bursátil".
    //
    // @see docs/architecture/portfolio-summary-vs-attribution-calculation-analysis.md
    let intradayAdjustedTWR = periodTWR;
    let intradayApplied = false;
    
    if (intradayPerformance && intradayPerformance.success && intradayPerformance.todayFactor !== 1) {
      // Calcular diferencia de días para logging/diagnóstico
      const today = new Date();
      const previousDayDate = intradayPerformance.previousDayDate 
        ? new Date(intradayPerformance.previousDayDate) 
        : null;
      
      let daysDifference = 0;
      if (previousDayDate) {
        const diffMs = today.getTime() - previousDayDate.getTime();
        daysDifference = Math.floor(diffMs / (1000 * 60 * 60 * 24));
      }
      
      // SIEMPRE aplicar el factor para mantener coherencia con PortfolioSummary
      intradayAdjustedTWR = combineHistoricalWithIntraday(periodTWR, intradayPerformance.todayFactor);
      console.log(`[Attribution] TWR ajustado: ${periodTWR.toFixed(2)}% → ${intradayAdjustedTWR.toFixed(2)}% (factor=${intradayPerformance.todayFactor.toFixed(6)}, días=${daysDifference})`);
      periodTWR = intradayAdjustedTWR;
      intradayApplied = true;
      
      // Agregar nota informativa si hay varios días de diferencia
      if (daysDifference > 1) {
        intradayPerformance.note = `Cambio de ${daysDifference} días incluido (desde ${intradayPerformance.previousDayDate})`;
      }
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
    
    // =========================================================================
    // 3.1 INTRADAY-002: Aplicar contribuciones intraday a cada activo
    // 
    // Cuando includeIntraday está habilitado, calculamos cuánto contribuye
    // cada activo al cambio intraday y lo sumamos a su contribución del período.
    // Esto permite que la suma de contribuciones coincida con el TWR ajustado.
    // =========================================================================
    let intradayContributionsApplied = false;
    let totalIntradayContribution = 0;
    
    if (intradayApplied && intradayPerformance?.success) {
      // Obtener los datos del último día para calcular contribuciones intraday
      const latestData = await getLatestPerformanceData(userId, accountIds[0] || 'overall');
      
      if (latestData) {
        const intradayContribResult = await calculateIntradayContributions({
          userId,
          currency,
          accountIds,
          latestPerformanceData: latestData
        });
        
        if (intradayContribResult.success) {
          console.log(`[Attribution] Aplicando contribuciones intraday a ${Object.keys(intradayContribResult.contributions).length} activos`);
          
          // Aplicar contribución intraday a cada activo
          for (const attr of contributionResult.attributions) {
            const intradayContrib = intradayContribResult.contributions[attr.assetKey] || 0;
            
            if (intradayContrib !== 0) {
              // Guardar contribución histórica para diagnóstico
              attr.historicalContribution = attr.contribution;
              attr.intradayContribution = intradayContrib;
              
              // Sumar contribución intraday
              attr.contribution += intradayContrib;
              totalIntradayContribution += intradayContrib;
            }
          }
          
          intradayContributionsApplied = true;
          console.log(`[Attribution] Total contribución intraday aplicada: ${totalIntradayContribution.toFixed(4)}pp`);
        } else {
          console.log(`[Attribution] No se pudieron calcular contribuciones intraday: ${intradayContribResult.error}`);
        }
      }
    }
    
    // IMPORTANTE: Sobrescribir portfolioReturn con el TWR del período (ya incluye intraday)
    // El totalROI de assetPerformance es el ROI total desde compra, NO del período
    if (hasTWRData && periodTWR !== 0) {
      console.log(`[Attribution] Reemplazando portfolioReturn: ${contributionResult.portfolioReturn.toFixed(2)}% → ${periodTWR.toFixed(2)}%`);
      contributionResult.portfolioReturn = periodTWR;
      
      // =========================================================================
      // FIX-ATTR-CONSISTENCY: NORMALIZACIÓN OBLIGATORIA
      // 
      // La suma de contribuciones DEBE ser igual al TWR para mostrar datos consistentes
      // al usuario (el TWR del PortfolioSummary debe coincidir con la suma de atribuciones).
      // 
      // Método: Distribuir el residuo (diferencia) proporcionalmente entre los activos.
      // Esto preserva la dirección (signo) de cada contribución individual mientras
      // garantiza que la suma sea exactamente igual al TWR.
      // =========================================================================
      const sumOfContributions = contributionResult.attributions.reduce((sum, a) => sum + a.contribution, 0);
      const residual = periodTWR - sumOfContributions;
      
      console.log(`[Attribution] Suma contribuciones: ${sumOfContributions.toFixed(4)}%, TWR: ${periodTWR.toFixed(4)}%, Residuo: ${residual.toFixed(4)}pp`);
      
      if (Math.abs(residual) > 0.001) {
        // Distribuir el residuo proporcionalmente al peso de cada activo
        // Usamos el valor absoluto de la contribución como peso para la distribución
        const totalAbsContribution = contributionResult.attributions.reduce((sum, a) => sum + Math.abs(a.contribution), 0);
        
        if (totalAbsContribution > 0) {
          for (const attr of contributionResult.attributions) {
            // Guardar contribución original para diagnóstico
            attr.originalContribution = attr.contribution;
            
            // Distribuir residuo proporcionalmente al peso del activo
            const weight = Math.abs(attr.contribution) / totalAbsContribution;
            const adjustment = residual * weight;
            attr.contribution += adjustment;
            attr.normalizationAdjustment = adjustment;
          }
          
          console.log(`[Attribution] Residuo distribuido entre ${contributionResult.attributions.length} activos`);
        }
        
        contributionResult.normalized = true;
        contributionResult.sumOfContributions = periodTWR;
        contributionResult.residualDistributed = residual;
      } else {
        // Residuo insignificante, no es necesario ajustar
        console.log(`[Attribution] Residuo insignificante (${residual.toFixed(4)}pp), sin ajuste necesario`);
        contributionResult.normalized = true;
        contributionResult.sumOfContributions = sumOfContributions;
      }
    }
    
    // Guardar info de contribuciones intraday en el resultado
    contributionResult.intradayContributionsApplied = intradayContributionsApplied;
    contributionResult.totalIntradayContribution = totalIntradayContribution;
    
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
        dataSource: intradayPerformance ? 'assetPerformance + TWR + intraday' : 'assetPerformance + TWR',
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
          historicalTWR: historicalTWR,
          periodTWR: hasTWRData ? periodTWR : null,
          originalTotalROI: contributionResult.originalPortfolioReturn,
          portfolioReturn: contributionResult.portfolioReturn,
          discrepancy: contributionResult.discrepancy,
          normalized: contributionResult.normalized
        },
        // INTRADAY-001: Información de rendimiento intraday
        intraday: {
          included: !!intradayPerformance,
          enabled: includeIntraday,
          applied: intradayApplied, // INTRADAY-FIX: Si realmente se aplicó el ajuste
          contributionsApplied: intradayContributionsApplied, // INTRADAY-002: Si se aplicaron contribuciones por activo
          totalIntradayContribution: totalIntradayContribution, // INTRADAY-002: Suma de contribuciones intraday
          error: intradayError,
          ...(intradayPerformance ? {
            date: intradayPerformance.date,
            todayFactor: intradayPerformance.todayFactor,
            dailyChangePercent: intradayPerformance.dailyChangePercent,
            adjustedDailyChangePercent: intradayPerformance.adjustedDailyChangePercent,
            totalValue: intradayPerformance.totalValue,
            previousDayTotalValue: intradayPerformance.previousDayTotalValue,
            previousDayDate: intradayPerformance.previousDayDate,
            assetsWithPrice: intradayPerformance.assetsWithPrice,
            symbolsRequested: intradayPerformance.symbolsRequested,
            symbolsWithPrice: intradayPerformance.symbolsWithPrice,
            note: intradayPerformance.note || null  // Info si hay varios días de diferencia
          } : {})
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
