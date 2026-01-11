/**
 * Scheduled Functions para Consolidación de Períodos
 * 
 * COST-OPT-002: Ejecuta automáticamente la consolidación de rendimientos
 * para mantener actualizados los documentos consolidados.
 * 
 * Funciones:
 * - consolidateMonthlyPerformance: Día 1 de cada mes, 00:30 ET
 * - consolidateYearlyPerformance: 1 de Enero, 01:00 ET
 * 
 * Principios aplicados:
 * - SRP: Cada función tiene una responsabilidad clara
 * - DRY: Reutiliza consolidatePeriod de periodConsolidation.js
 * - KISS: Procesamiento secuencial simple con batching
 * 
 * @module periodConsolidationScheduled
 * @see docs/stories/63.story.md (COST-OPT-002)
 */

const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('./firebaseAdmin');
const { DateTime } = require('luxon');
const { consolidatePeriod, CONSOLIDATED_SCHEMA_VERSION } = require('../utils/periodConsolidation');

const db = admin.firestore();

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/**
 * Configuración común para scheduled functions
 */
const SCHEDULED_CONFIG = {
  timeZone: 'America/New_York',
  memory: '512MiB',
  timeoutSeconds: 540,
  retryCount: 3
};

/**
 * Campos que no son monedas en documentos consolidados
 */
const NON_CURRENCY_FIELDS = [
  'periodType', 'periodKey', 'startDate', 'endDate', 
  'docsCount', 'version', 'lastUpdated'
];

// ============================================================================
// SCHEDULED FUNCTIONS
// ============================================================================

/**
 * Consolida los datos de rendimiento del mes anterior
 * 
 * Se ejecuta el día 1 de cada mes a las 00:30 AM ET
 * Procesa todos los usuarios y todas sus cuentas
 * 
 * @see docs/architecture/cost-optimization-architectural-proposal.md
 */
const consolidateMonthlyPerformance = onSchedule(
  {
    schedule: '30 0 1 * *', // Día 1 de cada mes, 00:30 AM
    ...SCHEDULED_CONFIG
  },
  async (event) => {
    const startTime = Date.now();
    
    const now = DateTime.now().setZone('America/New_York');
    const previousMonth = now.minus({ months: 1 });
    const periodKey = previousMonth.toFormat('yyyy-MM');
    const periodStart = previousMonth.startOf('month').toISODate();
    const periodEnd = previousMonth.endOf('month').toISODate();
    
    console.log(`[consolidateMonthlyPerformance] Iniciando`, { periodKey, periodStart, periodEnd });
    
    // Métricas de ejecución
    const metrics = {
      usersProcessed: 0,
      accountsProcessed: 0,
      consolidationsWritten: 0,
      errors: 0,
      errorDetails: []
    };
    
    try {
      // 1. Obtener todos los usuarios con datos de performance
      const usersSnapshot = await db.collection('portfolioPerformance').get();
      
      console.log(`[consolidateMonthlyPerformance] Usuarios a procesar: ${usersSnapshot.size}`);
      
      // 2. Procesar cada usuario
      for (const userDoc of usersSnapshot.docs) {
        const userId = userDoc.id;
        
        try {
          // 2a. Consolidar nivel "overall"
          const overallResult = await consolidateUserMonth(userId, null, periodKey, periodStart, periodEnd);
          if (overallResult) {
            metrics.consolidationsWritten++;
          }
          
          // 2b. Obtener cuentas del usuario
          const accountsSnapshot = await db.collection(`portfolioPerformance/${userId}/accounts`).get();
          
          // 2c. Consolidar cada cuenta
          for (const accountDoc of accountsSnapshot.docs) {
            const accountId = accountDoc.id;
            
            try {
              const accountResult = await consolidateUserMonth(userId, accountId, periodKey, periodStart, periodEnd);
              if (accountResult) {
                metrics.consolidationsWritten++;
              }
              metrics.accountsProcessed++;
            } catch (accountError) {
              console.error(`[consolidateMonthlyPerformance] Error cuenta ${accountId}:`, accountError.message);
              metrics.errors++;
              metrics.errorDetails.push({ userId, accountId, error: accountError.message });
            }
          }
          
          metrics.usersProcessed++;
        } catch (userError) {
          console.error(`[consolidateMonthlyPerformance] Error usuario ${userId}:`, userError.message);
          metrics.errors++;
          metrics.errorDetails.push({ userId, error: userError.message });
        }
      }
      
      const duration = Date.now() - startTime;
      
      console.log(`[consolidateMonthlyPerformance] Completado`, {
        periodKey,
        ...metrics,
        durationMs: duration
      });
      
      // Guardar métricas de ejecución para monitoreo
      await saveExecutionMetrics('monthly', periodKey, metrics, duration);
      
    } catch (error) {
      console.error(`[consolidateMonthlyPerformance] Error fatal:`, error);
      throw error;
    }
  }
);

/**
 * Consolida los datos de rendimiento del año anterior
 * 
 * Se ejecuta el 1 de enero a las 01:00 AM ET
 * Agrega los documentos mensuales en un documento anual
 * 
 * @see docs/architecture/cost-optimization-architectural-proposal.md
 */
const consolidateYearlyPerformance = onSchedule(
  {
    schedule: '0 1 1 1 *', // 1 de Enero, 01:00 AM
    ...SCHEDULED_CONFIG
  },
  async (event) => {
    const startTime = Date.now();
    
    const now = DateTime.now().setZone('America/New_York');
    const previousYear = now.minus({ years: 1 });
    const yearKey = previousYear.year.toString();
    
    console.log(`[consolidateYearlyPerformance] Iniciando`, { yearKey });
    
    // Métricas de ejecución
    const metrics = {
      usersProcessed: 0,
      accountsProcessed: 0,
      consolidationsWritten: 0,
      errors: 0,
      errorDetails: []
    };
    
    try {
      // 1. Obtener todos los usuarios con datos de performance
      const usersSnapshot = await db.collection('portfolioPerformance').get();
      
      console.log(`[consolidateYearlyPerformance] Usuarios a procesar: ${usersSnapshot.size}`);
      
      // 2. Procesar cada usuario
      for (const userDoc of usersSnapshot.docs) {
        const userId = userDoc.id;
        
        try {
          // 2a. Consolidar nivel "overall" desde documentos mensuales
          const overallResult = await consolidateUserYear(userId, null, yearKey);
          if (overallResult) {
            metrics.consolidationsWritten++;
          }
          
          // 2b. Obtener cuentas del usuario
          const accountsSnapshot = await db.collection(`portfolioPerformance/${userId}/accounts`).get();
          
          // 2c. Consolidar cada cuenta
          for (const accountDoc of accountsSnapshot.docs) {
            const accountId = accountDoc.id;
            
            try {
              const accountResult = await consolidateUserYear(userId, accountId, yearKey);
              if (accountResult) {
                metrics.consolidationsWritten++;
              }
              metrics.accountsProcessed++;
            } catch (accountError) {
              console.error(`[consolidateYearlyPerformance] Error cuenta ${accountId}:`, accountError.message);
              metrics.errors++;
              metrics.errorDetails.push({ userId, accountId, error: accountError.message });
            }
          }
          
          metrics.usersProcessed++;
        } catch (userError) {
          console.error(`[consolidateYearlyPerformance] Error usuario ${userId}:`, userError.message);
          metrics.errors++;
          metrics.errorDetails.push({ userId, error: userError.message });
        }
      }
      
      const duration = Date.now() - startTime;
      
      console.log(`[consolidateYearlyPerformance] Completado`, {
        yearKey,
        ...metrics,
        durationMs: duration
      });
      
      // Guardar métricas de ejecución para monitoreo
      await saveExecutionMetrics('yearly', yearKey, metrics, duration);
      
    } catch (error) {
      console.error(`[consolidateYearlyPerformance] Error fatal:`, error);
      throw error;
    }
  }
);

// ============================================================================
// FUNCIONES DE CONSOLIDACIÓN
// ============================================================================

/**
 * Consolida un mes específico para un usuario/cuenta
 * 
 * @param {string} userId - ID del usuario
 * @param {string|null} accountId - ID de cuenta (null para overall)
 * @param {string} periodKey - Clave del período (ej: "2025-12")
 * @param {string} periodStart - Fecha de inicio ISO
 * @param {string} periodEnd - Fecha de fin ISO
 * @returns {Promise<Object|null>} Documento consolidado o null si no hay datos
 */
async function consolidateUserMonth(userId, accountId, periodKey, periodStart, periodEnd) {
  // Construir paths
  const basePath = accountId
    ? `portfolioPerformance/${userId}/accounts/${accountId}`
    : `portfolioPerformance/${userId}`;
  
  const datesPath = `${basePath}/dates`;
  const consolidatedPath = `${basePath}/consolidatedPeriods/monthly/periods/${periodKey}`;
  
  // Leer documentos diarios del mes
  const dailySnapshot = await db.collection(datesPath)
    .where('date', '>=', periodStart)
    .where('date', '<=', periodEnd)
    .orderBy('date', 'asc')
    .get();
  
  if (dailySnapshot.empty) {
    return null;
  }
  
  // Consolidar usando función de COST-OPT-001
  const consolidated = consolidatePeriod(
    dailySnapshot.docs,
    periodKey,
    'month'
  );
  
  if (!consolidated) {
    return null;
  }
  
  // Guardar documento consolidado (idempotente - usa set en lugar de update)
  await db.doc(consolidatedPath).set(consolidated);
  
  return consolidated;
}

/**
 * Consolida un año específico para un usuario/cuenta
 * Usa los documentos mensuales ya consolidados
 * 
 * @param {string} userId - ID del usuario
 * @param {string|null} accountId - ID de cuenta (null para overall)
 * @param {string} yearKey - Año a consolidar (ej: "2025")
 * @returns {Promise<Object|null>} Documento consolidado o null si no hay datos
 */
async function consolidateUserYear(userId, accountId, yearKey) {
  // Construir paths
  const basePath = accountId
    ? `portfolioPerformance/${userId}/accounts/${accountId}`
    : `portfolioPerformance/${userId}`;
  
  const monthlyPath = `${basePath}/consolidatedPeriods/monthly/periods`;
  const yearlyPath = `${basePath}/consolidatedPeriods/yearly/periods/${yearKey}`;
  
  // Leer documentos mensuales del año
  const monthlySnapshot = await db.collection(monthlyPath)
    .where('periodKey', '>=', `${yearKey}-01`)
    .where('periodKey', '<=', `${yearKey}-12`)
    .orderBy('periodKey', 'asc')
    .get();
  
  if (monthlySnapshot.empty) {
    return null;
  }
  
  // Encadenar factores de los meses para obtener el año completo
  const consolidated = consolidateMonthsToYear(monthlySnapshot.docs, yearKey);
  
  if (!consolidated) {
    return null;
  }
  
  // Guardar documento consolidado anual (idempotente)
  await db.doc(yearlyPath).set(consolidated);
  
  return consolidated;
}

/**
 * Encadena documentos mensuales consolidados en un documento anual
 * 
 * @param {Array} monthlyDocs - Documentos mensuales ordenados
 * @param {string} yearKey - Año del período
 * @returns {Object|null} Documento anual consolidado
 */
function consolidateMonthsToYear(monthlyDocs, yearKey) {
  if (!monthlyDocs || monthlyDocs.length === 0) {
    return null;
  }
  
  const firstDoc = monthlyDocs[0].data ? monthlyDocs[0].data() : monthlyDocs[0];
  const lastDoc = monthlyDocs[monthlyDocs.length - 1].data 
    ? monthlyDocs[monthlyDocs.length - 1].data() 
    : monthlyDocs[monthlyDocs.length - 1];
  
  // Obtener todas las monedas
  const currencies = new Set();
  monthlyDocs.forEach(doc => {
    const data = doc.data ? doc.data() : doc;
    Object.keys(data).forEach(key => {
      if (!NON_CURRENCY_FIELDS.includes(key)) {
        currencies.add(key);
      }
    });
  });
  
  const consolidated = {
    periodType: 'year',
    periodKey: yearKey,
    startDate: firstDoc.startDate,
    endDate: lastDoc.endDate,
    docsCount: monthlyDocs.reduce((sum, doc) => {
      const data = doc.data ? doc.data() : doc;
      return sum + (data.docsCount || 0);
    }, 0),
    version: CONSOLIDATED_SCHEMA_VERSION,
    lastUpdated: new Date().toISOString()
  };
  
  // Procesar cada moneda
  currencies.forEach(currencyCode => {
    let compoundFactor = 1;
    let startTotalValue = 0;
    let startTotalInvestment = 0;
    let endTotalValue = 0;
    let endTotalInvestment = 0;
    let totalCashFlow = 0;
    let foundFirst = false;
    
    monthlyDocs.forEach(doc => {
      const data = doc.data ? doc.data() : doc;
      const currencyData = data[currencyCode];
      
      if (!currencyData) return;
      
      // Guardar valores del primer mes
      if (!foundFirst) {
        startTotalValue = currencyData.startTotalValue || 0;
        startTotalInvestment = currencyData.startTotalInvestment || 0;
        foundFirst = true;
      }
      
      // Actualizar valores del último mes
      endTotalValue = currencyData.endTotalValue || 0;
      endTotalInvestment = currencyData.endTotalInvestment || 0;
      
      // Encadenar factores (TWR)
      if (currencyData.endFactor && currencyData.startFactor) {
        compoundFactor *= (currencyData.endFactor / currencyData.startFactor);
      }
      
      // Sumar cashflows
      totalCashFlow += currencyData.totalCashFlow || 0;
    });
    
    // Calcular MWR del año
    let personalReturn = 0;
    if (startTotalValue > 0 || totalCashFlow !== 0) {
      const netDeposits = -totalCashFlow;
      const investmentBase = startTotalValue + (netDeposits / 2);
      if (investmentBase > 0) {
        const gain = endTotalValue - startTotalValue - netDeposits;
        personalReturn = (gain / investmentBase) * 100;
      }
    }
    
    consolidated[currencyCode] = {
      startFactor: 1,
      endFactor: compoundFactor,
      periodReturn: (compoundFactor - 1) * 100,
      startTotalValue,
      endTotalValue,
      startTotalInvestment,
      endTotalInvestment,
      totalCashFlow,
      personalReturn,
      validDocsCount: monthlyDocs.length
    };
  });
  
  return consolidated;
}

// ============================================================================
// FUNCIONES DE UTILIDAD
// ============================================================================

/**
 * Guarda métricas de ejecución para monitoreo
 * 
 * @param {string} type - Tipo de consolidación ('monthly' o 'yearly')
 * @param {string} periodKey - Clave del período procesado
 * @param {Object} metrics - Métricas de ejecución
 * @param {number} duration - Duración en ms
 */
async function saveExecutionMetrics(type, periodKey, metrics, duration) {
  try {
    const metricsDoc = {
      type,
      periodKey,
      ...metrics,
      durationMs: duration,
      timestamp: new Date().toISOString()
    };
    
    await db.collection('_systemMetrics/consolidation/executions')
      .doc(`${type}_${periodKey}`)
      .set(metricsDoc);
  } catch (error) {
    console.warn(`[saveExecutionMetrics] Error guardando métricas:`, error.message);
    // No lanzar error - las métricas son opcionales
  }
}

/**
 * Consolida un mes específico para un usuario (función pública para migración)
 * 
 * Esta función es idéntica a consolidateUserMonth pero exportada para uso
 * en scripts de migración (COST-OPT-003).
 * 
 * @param {string} userId - ID del usuario
 * @param {string|null} accountId - ID de cuenta (null para overall)
 * @param {string} periodKey - Clave del período (ej: "2025-12")
 * @returns {Promise<Object|null>} Documento consolidado o null
 */
async function consolidateMonth(userId, accountId, periodKey) {
  const [year, month] = periodKey.split('-').map(Number);
  const periodDt = DateTime.fromObject({ year, month, day: 1 });
  const periodStart = periodDt.startOf('month').toISODate();
  const periodEnd = periodDt.endOf('month').toISODate();
  
  return consolidateUserMonth(userId, accountId, periodKey, periodStart, periodEnd);
}

/**
 * Consolida un año específico para un usuario (función pública para migración)
 * 
 * @param {string} userId - ID del usuario
 * @param {string|null} accountId - ID de cuenta (null para overall)
 * @param {string} yearKey - Año a consolidar (ej: "2025")
 * @returns {Promise<Object|null>} Documento consolidado o null
 */
async function consolidateYear(userId, accountId, yearKey) {
  return consolidateUserYear(userId, accountId, yearKey);
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Scheduled Functions (para index.js)
  consolidateMonthlyPerformance,
  consolidateYearlyPerformance,
  
  // Funciones públicas (para migración COST-OPT-003 y testing)
  consolidateMonth,
  consolidateYear,
  consolidateMonthsToYear,
  
  // Funciones internas exportadas para testing
  consolidateUserMonth,
  consolidateUserYear,
  saveExecutionMetrics
};
