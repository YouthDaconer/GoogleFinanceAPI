/**
 * Consolidated Returns Service - Versión Optimizada de Historical Returns
 * 
 * COST-OPT-001: Reduce lecturas de Firestore de ~1,825 a ~40 documentos
 * para consultas de rendimientos de 5 años.
 * 
 * En lugar de leer TODOS los documentos diarios, lee:
 * - Documentos de años consolidados (~5 docs para 5 años)
 * - Documentos de meses consolidados del año actual (~11 docs)
 * - Documentos diarios del mes actual (~20 docs)
 * 
 * @module consolidatedReturnsService
 * @see docs/stories/62.story.md (COST-OPT-001)
 * @see docs/architecture/cost-optimization-architectural-proposal.md
 */

const admin = require('./firebaseAdmin');
const { DateTime } = require('luxon');

// Importar funciones de consolidación
const {
  chainFactorsForPeriods,
  isMonthClosed,
  isYearClosed
} = require('../utils/periodConsolidation');

// Importar servicio original para fallback
const { calculateHistoricalReturns } = require('./historicalReturnsService');

const db = admin.firestore();

// ============================================================================
// CONSTANTES
// ============================================================================

/**
 * Configuración de rutas en Firestore
 */
const PATHS = {
  PERFORMANCE_BASE: 'portfolioPerformance',
  DATES: 'dates',
  CONSOLIDATED: 'consolidatedPeriods',
  MONTHLY: 'monthly/periods',
  YEARLY: 'yearly/periods'
};

/**
 * Configuración de la función
 */
const CONFIG = {
  TIMEZONE: 'America/New_York',
  MIN_CONSOLIDATED_DOCS: 3 // Mínimo de documentos consolidados para usar V2
};

// ============================================================================
// FUNCIONES PRINCIPALES
// ============================================================================

/**
 * Obtiene rendimientos históricos usando períodos consolidados (V2)
 * 
 * Esta función es la versión optimizada que reduce dramáticamente
 * las lecturas de Firestore al usar documentos pre-consolidados.
 * 
 * @param {string} userId - ID del usuario
 * @param {Object} params - Parámetros de la consulta
 * @param {string} params.currency - Código de moneda (USD, COP, etc.)
 * @param {string} params.accountId - ID de cuenta o "overall"
 * @param {string|null} params.ticker - Ticker específico (opcional)
 * @param {string|null} params.assetType - Tipo de asset (opcional)
 * @param {boolean} params.forceRefresh - Forzar recálculo
 * @param {boolean} params.fallbackToV1 - Permitir fallback a V1 si no hay consolidados
 * @returns {Promise<Object>} Rendimientos calculados
 * 
 * @example
 * const result = await getHistoricalReturnsV2('userId123', {
 *   currency: 'USD',
 *   accountId: 'overall'
 * });
 */
async function getHistoricalReturnsV2(userId, params = {}) {
  const {
    currency = 'USD',
    accountId = 'overall',
    ticker = null,
    assetType = null,
    forceRefresh = false,
    fallbackToV1 = true
  } = params;
  
  const startTime = Date.now();
  const now = DateTime.now().setZone(CONFIG.TIMEZONE);
  const currentMonth = now.toFormat('yyyy-MM');
  const currentYear = now.year.toString();
  
  // Determinar rango necesario (5 años atrás)
  const fiveYearsAgo = now.minus({ years: 5 });
  const startYear = fiveYearsAgo.year.toString();
  
  // COST-OPT-003 FIX: Para garantizar consistencia V1/V2, necesitamos:
  // 1. Años completos que estén ANTES del año donde inició el usuario
  // 2. TODOS los meses desde el inicio del usuario hasta el mes anterior
  // 3. Días del mes actual
  //
  // Para simplificar y evitar problemas de borde, usamos esta estrategia:
  // - Leer años desde hace 5 años hasta hace 2 años (años completamente cerrados)
  // - Leer TODOS los meses de los últimos 2 años (hasta 24 meses)
  // - Esto garantiza cobertura completa para cualquier cálculo de período
  
  const twoYearsAgo = now.minus({ years: 2 });
  const lastCompleteYearForYearly = (twoYearsAgo.year - 1).toString(); // Para 2026: 2023
  const monthsStartKey = `${twoYearsAgo.year}-01`; // Para 2026: 2024-01
  
  // Construir path base según accountId
  const basePath = buildBasePath(userId, accountId);
  
  console.log(`[consolidatedReturnsV2] Start - userId: ${userId}, currency: ${currency}, accountId: ${accountId}`);
  console.log(`[consolidatedReturnsV2] Ranges - years: ${startYear}-${lastCompleteYearForYearly}, months: ${monthsStartKey}-${currentMonth}`);
  
  try {
    // 1. Leer documentos consolidados en paralelo
    const [yearlySnapshot, monthlySnapshot, dailySnapshot] = await Promise.all([
      // Años cerrados de hace 3+ años (para usuarios con historial muy largo)
      db.collection(`${basePath}/${PATHS.CONSOLIDATED}/${PATHS.YEARLY}`)
        .where('periodKey', '>=', startYear)
        .where('periodKey', '<=', lastCompleteYearForYearly)
        .orderBy('periodKey', 'asc')
        .get(),
      
      // COST-OPT-003 FIX: Todos los meses de los últimos 2 años
      // Esto cubre completamente 1Y, 2Y y el inicio del historial para usuarios nuevos
      db.collection(`${basePath}/${PATHS.CONSOLIDATED}/${PATHS.MONTHLY}`)
        .where('periodKey', '>=', monthsStartKey)
        .where('periodKey', '<', currentMonth)
        .orderBy('periodKey', 'asc')
        .get(),
      
      // Días del mes actual (datos frescos)
      db.collection(`${basePath}/${PATHS.DATES}`)
        .where('date', '>=', `${currentMonth}-01`)
        .orderBy('date', 'asc')
        .get()
    ]);
    
    const totalDocsRead = yearlySnapshot.size + monthlySnapshot.size + dailySnapshot.size;
    
    console.log(`[consolidatedReturnsV2] Docs read - yearly: ${yearlySnapshot.size}, monthly: ${monthlySnapshot.size}, daily: ${dailySnapshot.size}, total: ${totalDocsRead}`);
    
    // 2. Verificar si tenemos suficientes documentos consolidados
    const hasConsolidatedData = yearlySnapshot.size > 0 || monthlySnapshot.size > 0;
    
    if (!hasConsolidatedData && dailySnapshot.empty) {
      console.log(`[consolidatedReturnsV2] No data found`);
      return createEmptyResult();
    }
    
    // 3. Si no hay datos consolidados pero hay diarios, usar fallback a V1
    if (!hasConsolidatedData && fallbackToV1) {
      console.log(`[consolidatedReturnsV2] No consolidated data, falling back to V1`);
      return await fallbackToV1Method(userId, basePath, currency, ticker, assetType);
    }
    
    // 4. Encadenar factores para calcular rendimientos
    const result = chainFactorsForPeriods(
      yearlySnapshot.docs,
      monthlySnapshot.docs,
      dailySnapshot.docs,
      currency,
      ticker,
      assetType,
      now
    );
    
    const duration = Date.now() - startTime;
    console.log(`[consolidatedReturnsV2] Success - duration: ${duration}ms, docsRead: ${totalDocsRead}`);
    
    // 5. Agregar metadata de versión y performance
    return {
      ...result,
      _metadata: {
        version: 'v2',
        duration,
        docsRead: totalDocsRead,
        yearlyDocs: yearlySnapshot.size,
        monthlyDocs: monthlySnapshot.size,
        dailyDocs: dailySnapshot.size,
        timestamp: now.toISO()
      }
    };
    
  } catch (error) {
    console.error(`[consolidatedReturnsV2] Error:`, error);
    
    // Si hay error y fallback está habilitado, intentar V1
    if (fallbackToV1) {
      console.log(`[consolidatedReturnsV2] Error occurred, falling back to V1`);
      return await fallbackToV1Method(userId, basePath, currency, ticker, assetType);
    }
    
    throw error;
  }
}

/**
 * Verifica si existen documentos consolidados para un usuario
 * 
 * Útil para determinar si el usuario puede usar V2 o necesita migración.
 * 
 * @param {string} userId - ID del usuario
 * @param {string} accountId - ID de cuenta o "overall"
 * @returns {Promise<Object>} Estado de los documentos consolidados
 */
async function checkConsolidatedDataStatus(userId, accountId = 'overall') {
  const basePath = buildBasePath(userId, accountId);
  
  const [yearlySnapshot, monthlySnapshot] = await Promise.all([
    db.collection(`${basePath}/${PATHS.CONSOLIDATED}/${PATHS.YEARLY}`).limit(1).get(),
    db.collection(`${basePath}/${PATHS.CONSOLIDATED}/${PATHS.MONTHLY}`).limit(1).get()
  ]);
  
  const hasYearly = !yearlySnapshot.empty;
  const hasMonthly = !monthlySnapshot.empty;
  
  return {
    hasConsolidatedData: hasYearly || hasMonthly,
    hasYearlyDocs: hasYearly,
    hasMonthlyDocs: hasMonthly,
    canUseV2: hasYearly || hasMonthly
  };
}

/**
 * Obtiene el conteo de documentos consolidados para un usuario
 * 
 * @param {string} userId - ID del usuario
 * @param {string} accountId - ID de cuenta o "overall"
 * @returns {Promise<Object>} Conteos de documentos
 */
async function getConsolidatedDataCounts(userId, accountId = 'overall') {
  const basePath = buildBasePath(userId, accountId);
  
  const [yearlySnapshot, monthlySnapshot, dailySnapshot] = await Promise.all([
    db.collection(`${basePath}/${PATHS.CONSOLIDATED}/${PATHS.YEARLY}`).get(),
    db.collection(`${basePath}/${PATHS.CONSOLIDATED}/${PATHS.MONTHLY}`).get(),
    db.collection(`${basePath}/${PATHS.DATES}`).get()
  ]);
  
  return {
    yearlyDocs: yearlySnapshot.size,
    monthlyDocs: monthlySnapshot.size,
    dailyDocs: dailySnapshot.size,
    totalDocs: yearlySnapshot.size + monthlySnapshot.size + dailySnapshot.size
  };
}

// ============================================================================
// FUNCIONES AUXILIARES
// ============================================================================

/**
 * Construye el path base de Firestore según el accountId
 * 
 * @param {string} userId - ID del usuario
 * @param {string} accountId - ID de cuenta o "overall"
 * @returns {string} Path base de Firestore
 */
function buildBasePath(userId, accountId) {
  if (accountId === 'overall' || !accountId) {
    return `${PATHS.PERFORMANCE_BASE}/${userId}`;
  }
  return `${PATHS.PERFORMANCE_BASE}/${userId}/accounts/${accountId}`;
}

/**
 * Crea un resultado vacío compatible con la estructura esperada
 * 
 * @returns {Object} Resultado vacío
 */
function createEmptyResult() {
  return {
    returns: {
      fiveYearReturn: 0,
      twoYearReturn: 0,
      oneYearReturn: 0,
      sixMonthReturn: 0,
      threeMonthReturn: 0,
      oneMonthReturn: 0,
      ytdReturn: 0,
      fiveYearPersonalReturn: 0,
      twoYearPersonalReturn: 0,
      oneYearPersonalReturn: 0,
      sixMonthPersonalReturn: 0,
      threeMonthPersonalReturn: 0,
      oneMonthPersonalReturn: 0,
      ytdPersonalReturn: 0,
      hasFiveYearData: false,
      hasTwoYearData: false,
      hasOneYearData: false,
      hasSixMonthData: false,
      hasThreeMonthData: false,
      hasOneMonthData: false,
      hasYtdData: false
    },
    validDocsCountByPeriod: {
      fiveYears: 0,
      twoYears: 0,
      oneYear: 0,
      sixMonths: 0,
      threeMonths: 0,
      oneMonth: 0,
      ytd: 0
    },
    totalValueData: {
      dates: [],
      values: [],
      percentChanges: [],
      overallPercentChange: 0
    },
    startDate: '',
    consolidatedVersion: true,
    _metadata: {
      version: 'v2',
      empty: true
    }
  };
}

/**
 * Fallback a método V1 (lectura de todos los documentos diarios)
 * 
 * Se usa cuando no hay documentos consolidados disponibles.
 * 
 * @param {string} userId - ID del usuario
 * @param {string} basePath - Path base de Firestore
 * @param {string} currency - Código de moneda
 * @param {string|null} ticker - Ticker específico
 * @param {string|null} assetType - Tipo de asset
 * @returns {Promise<Object>} Rendimientos calculados con V1
 */
async function fallbackToV1Method(userId, basePath, currency, ticker, assetType) {
  const startTime = Date.now();
  
  // Leer todos los documentos diarios (método original)
  const performanceSnapshot = await db.collection(`${basePath}/${PATHS.DATES}`)
    .orderBy('date', 'asc')
    .get();
  
  if (performanceSnapshot.empty) {
    return createEmptyResult();
  }
  
  // Usar función de cálculo original
  const result = calculateHistoricalReturns(
    performanceSnapshot.docs,
    currency,
    ticker,
    assetType
  );
  
  const duration = Date.now() - startTime;
  
  console.log(`[consolidatedReturnsV2] V1 Fallback - duration: ${duration}ms, docsRead: ${performanceSnapshot.size}`);
  
  return {
    ...result,
    consolidatedVersion: false,
    _metadata: {
      version: 'v1-fallback',
      duration,
      docsRead: performanceSnapshot.size,
      reason: 'no consolidated data available'
    }
  };
}

/**
 * Calcula el TTL dinámico para cache basado en hora del día
 * 
 * Reutiliza la lógica del servicio original.
 * 
 * @returns {Date} Fecha de expiración del cache
 */
function calculateDynamicTTL() {
  const now = DateTime.now().setZone(CONFIG.TIMEZONE);
  const hour = now.hour;
  
  // Durante horario de mercado (9:30 - 16:00), TTL corto
  if (hour >= 9 && hour < 16) {
    return now.plus({ minutes: 5 }).toJSDate();
  }
  
  // Después de cierre, TTL hasta apertura del siguiente día
  if (hour >= 16) {
    return now.plus({ days: 1 }).set({ hour: 9, minute: 30, second: 0 }).toJSDate();
  }
  
  // Antes de apertura, TTL hasta apertura
  return now.set({ hour: 9, minute: 30, second: 0 }).toJSDate();
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Función principal V2
  getHistoricalReturnsV2,
  
  // Funciones de verificación
  checkConsolidatedDataStatus,
  getConsolidatedDataCounts,
  
  // Utilidades
  buildBasePath,
  createEmptyResult,
  calculateDynamicTTL,
  
  // Constantes
  PATHS,
  CONFIG
};
