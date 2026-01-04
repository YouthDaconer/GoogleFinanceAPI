require('dotenv').config();
const { onRequest } = require("firebase-functions/v2/https");
const httpApp = require('./httpApi');

// Nueva función unificada que combina updateCurrencyRates, updateCurrentPrices y calculateDailyPortfolioPerformance
const { unifiedMarketDataUpdate } = require('./services/unifiedMarketDataUpdate');

// Función para actualizaciones completas de datos de activos (mantener para ISINs y optionalKeys)
const { scheduledUpdatePrices } = require('./services/updateCurrentPrices');

const processDividendPayments = require('./services/processDividendPayments');
const marketStatusService = require('./services/marketStatusService');
const { saveAllIndicesAndSectorsHistoryData } = require("./services/saveAllIndicesAndSectorsHistoryData");

// Calcular semanalmente el porcentaje de semanas rentables
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { calculateProfitableWeeks } = require('./services/calculateProfitableWeeks');

// Cloud Functions Callable para operaciones de assets (REF-002)
const assetOperations = require('./services/assetOperations');

// Cloud Function Callable para precios filtrados por usuario (OPT-001)
const userPricesService = require('./services/userPricesService');

// Cloud Function Callable para rendimientos históricos pre-calculados (OPT-002)
const historicalReturnsService = require('./services/historicalReturnsService');

// Cloud Function Callable para índices históricos pre-calculados (OPT-009)
const indexHistoryService = require('./services/indexHistoryService');

// Configuración para habilitar la recolección de basura explícita
// Solo funciona cuando se ejecuta con --expose-gc
try {
  if (!global.gc) {
    console.warn('⚠️ La recolección de basura explícita no está disponible. Considera usar la flag --expose-gc al iniciar Node.');
  } else {
    console.log('✅ Recolección de basura explícita disponible.');
  }
} catch (e) {
  console.warn('⚠️ Error al verificar la recolección de basura:', e.message);
}

// Configuraciones para la función de Firebase
const runtimeOpts = {
  timeoutSeconds: 540,
  minInstances: 0,
  concurrency: 80,
  maxInstanceConcurrency: 1, // Limitar a una solicitud por instancia para evitar problemas de memoria
};

// Configuración específica para el endpoint de procesamiento de Excel
const etfProcessingOpts = {
  ...runtimeOpts,
  maxInstances: 10,
  maxRequestsPerInstance: 10,
  // Configuración importante para manejar cargas de archivos en Firebase Functions
  ingressSettings: "ALLOW_ALL",
  // Añadimos un tiempo máximo para que Firebase permita completar la carga
  timeoutSeconds: 500, // 8.3 minutos
  // Añadir ajustes específicos para subidas de archivos
  upload: {
    maxUploadSize: "30MB", // Limitar tamaño de subida a 30MB
    maxUploadTime: "7m", // Tiempo máximo para cargar un archivo
  },
  // Sin instancias mínimas para evitar cargos continuos
  minInstances: 0, 
  cpu: 1 // Asignar 1 CPU completo 
};

// Exportar la app HTTP con las configuraciones específicas
exports.app = onRequest(etfProcessingOpts, httpApp);

// Nueva función unificada que reemplaza las tres funciones individuales
exports.unifiedMarketDataUpdateV2 = unifiedMarketDataUpdate;

// Función específica para actualizaciones completas de datos (ISINs y metadatos)
exports.scheduledUpdatePricesV2 = scheduledUpdatePrices;

// Exportar las demás funciones
exports.saveAllIndicesAndSectorsHistoryDataV2 = saveAllIndicesAndSectorsHistoryData;
exports.processDividendPaymentsV2 = processDividendPayments.processDividendPayments;
exports.scheduledMarketStatusUpdateV2 = marketStatusService.scheduledMarketStatusUpdate;
exports.scheduledMarketStatusUpdateAdditionalV2 = marketStatusService.scheduledMarketStatusUpdateAdditional;
exports.updateMarketStatusHttpV2 = marketStatusService.updateMarketStatusHttp;

exports.weeklyProfitableWeeksCalculation = onSchedule({
  schedule: "every sunday 23:00",
  timeZone: "America/New_York",
  retryCount: 3,
}, async (event) => {
  try {
    console.log('Iniciando cálculo semanal de semanas rentables');
    await calculateProfitableWeeks();
    console.log('Cálculo semanal de semanas rentables completado con éxito');
    return null;
  } catch (error) {
    console.error('Error en cálculo semanal de semanas rentables:', error);
    return null;
  }
});

// ============================================================================
// Cloud Functions Callable - Operaciones de Assets (REF-002)
// ============================================================================
// DEPRECATED: Estas funciones fueron consolidadas en portfolioOperations (SCALE-CF-001)
// Mantenidas como comentario para referencia durante el período de transición
// TODO: Eliminar después de 2 semanas de uso exitoso (2025-01-07)

/*
 * Crea un nuevo asset con transacción de compra y actualización de balance atómicamente
 * @see docs/architecture/refactoring-analysis.md - Sección 1.1
 * @deprecated Use portfolioOperations({ action: 'createAsset', payload: {...} })
 */
// exports.createAsset = assetOperations.createAsset;

/*
 * Actualiza un asset existente con ajuste de balance
 * @deprecated Use portfolioOperations({ action: 'updateAsset', payload: {...} })
 */
// exports.updateAsset = assetOperations.updateAsset;

/*
 * Vende un asset existente (total o parcialmente)
 * @see docs/architecture/refactoring-analysis.md - Sección 1.2
 * @deprecated Use portfolioOperations({ action: 'sellAsset', payload: {...} })
 */
// exports.sellAsset = assetOperations.sellAsset;

/*
 * Vende unidades de múltiples lotes del mismo ticker usando FIFO
 * @see docs/architecture/refactoring-analysis.md - Sección 1.3
 * @deprecated Use portfolioOperations({ action: 'sellPartialAssetsFIFO', payload: {...} })
 */
// exports.sellPartialAssetsFIFO = assetOperations.sellPartialAssetsFIFO;

/*
 * Registra una transacción de efectivo (ingreso o egreso)
 * @see docs/architecture/refactoring-analysis.md - Sección 1.4
 * @deprecated Use portfolioOperations({ action: 'addCashTransaction', payload: {...} })
 */
// exports.addCashTransaction = assetOperations.addCashTransaction;

/*
 * Elimina un asset individual y sus transacciones asociadas
 * @deprecated Use portfolioOperations({ action: 'deleteAsset', payload: {...} })
 */
// exports.deleteAsset = assetOperations.deleteAsset;

/*
 * Elimina activos de una cuenta de portafolio (todos o por moneda)
 * @deprecated Use portfolioOperations({ action: 'deleteAssets', payload: {...} })
 */
// exports.deleteAssets = assetOperations.deleteAssets;

/*
 * Actualiza el sector de un stock en currentPrices (fallback manual)
 * @deprecated Use portfolioOperations({ action: 'updateStockSector', payload: {...} })
 */
// exports.updateStockSector = assetOperations.updateStockSector;

// ============================================================================
// Cloud Functions Callable - Operaciones de Settings (REF-005)
// ============================================================================
// DEPRECATED: Estas funciones fueron consolidadas en settingsOperations (SCALE-CF-001)
// TODO: Eliminar después de 2 semanas de uso exitoso (2025-01-07)

// Cloud Functions para currencies y userData
const settingsOperations = require('./services/settingsOperations');

/*
 * Agrega una nueva moneda al sistema
 * @see docs/stories/27.story.md (REF-005)
 * @deprecated Use settingsOperations({ action: 'addCurrency', payload: {...} })
 */
// exports.addCurrency = settingsOperations.addCurrency;

/*
 * Actualiza una moneda existente
 * @see docs/stories/27.story.md (REF-005)
 * @deprecated Use settingsOperations({ action: 'updateCurrency', payload: {...} })
 */
// exports.updateCurrency = settingsOperations.updateCurrency;

/*
 * Elimina una moneda del sistema
 * @see docs/stories/27.story.md (REF-005)
 * @deprecated Use settingsOperations({ action: 'deleteCurrency', payload: {...} })
 */
// exports.deleteCurrency = settingsOperations.deleteCurrency;

/*
 * Actualiza la moneda por defecto del usuario
 * @see docs/stories/27.story.md (REF-005)
 * @deprecated Use settingsOperations({ action: 'updateDefaultCurrency', payload: {...} })
 */
// exports.updateDefaultCurrency = settingsOperations.updateDefaultCurrency;

/*
 * Actualiza el país del usuario
 * @see docs/stories/27.story.md (REF-005)
 * @deprecated Use settingsOperations({ action: 'updateUserCountry', payload: {...} })
 */
// exports.updateUserCountry = settingsOperations.updateUserCountry;

/*
 * Actualiza el nombre para mostrar del usuario
 * @see docs/stories/27.story.md (REF-005)
 * @deprecated Use settingsOperations({ action: 'updateUserDisplayName', payload: {...} })
 */
// exports.updateUserDisplayName = settingsOperations.updateUserDisplayName;

// ============================================================================
// Cloud Functions Callable - Operaciones de Portfolio Accounts (REF-006)
// ============================================================================
// DEPRECATED: Estas funciones fueron consolidadas en accountOperations (SCALE-CF-001)
// TODO: Eliminar después de 2 semanas de uso exitoso (2025-01-07)

// Cloud Functions para portfolioAccounts
const portfolioAccountOperations = require('./services/portfolioAccountOperations');

/*
 * Crea una nueva cuenta de portafolio
 * @see docs/stories/27.story.md (REF-006)
 * @deprecated Use accountOperations({ action: 'addPortfolioAccount', payload: {...} })
 */
// exports.addPortfolioAccount = portfolioAccountOperations.addPortfolioAccount;

/*
 * Actualiza una cuenta de portafolio existente
 * @see docs/stories/27.story.md (REF-006)
 * @deprecated Use accountOperations({ action: 'updatePortfolioAccount', payload: {...} })
 */
// exports.updatePortfolioAccount = portfolioAccountOperations.updatePortfolioAccount;

/*
 * Elimina una cuenta de portafolio
 * @see docs/stories/27.story.md (REF-006)
 * @deprecated Use accountOperations({ action: 'deletePortfolioAccount', payload: {...} })
 */
// exports.deletePortfolioAccount = portfolioAccountOperations.deletePortfolioAccount;

/*
 * Actualiza el balance de una moneda en una cuenta
 * @see docs/stories/27.story.md (REF-006)
 * @deprecated Use accountOperations({ action: 'updatePortfolioAccountBalance', payload: {...} })
 */
// exports.updatePortfolioAccountBalance = portfolioAccountOperations.updatePortfolioAccountBalance;

// ============================================================================
// Cloud Functions Callable - Optimización de Consultas (OPT-001)
// ============================================================================
// DEPRECATED: Estas funciones fueron consolidadas en queryOperations (SCALE-CF-001)
// TODO: Eliminar después de 2 semanas de uso exitoso (2025-01-07)

/*
 * Obtiene precios filtrados por símbolos que el usuario posee
 * Reduce lecturas de 56 (todos) a ~25 (solo del usuario)
 * @see docs/stories/6.story.md (OPT-001)
 * @deprecated Use queryOperations({ action: 'getCurrentPricesForUser', payload: {...} })
 */
// exports.getCurrentPricesForUser = userPricesService.getCurrentPricesForUser;

// ============================================================================
// Cloud Functions Callable - Pre-cálculo de Rendimientos (OPT-002)
// ============================================================================

/*
 * Pre-calcula rendimientos históricos en el servidor con cache
 * Reduce lecturas de ~200 a 1 (cache hit) por montaje del Dashboard
 * @see docs/stories/7.story.md (OPT-002)
 * @deprecated Use queryOperations({ action: 'getHistoricalReturns', payload: {...} })
 */
// exports.getHistoricalReturns = historicalReturnsService.getHistoricalReturns;

/*
 * FEAT-CHART-001: Pre-calcula rendimientos históricos de múltiples cuentas agregados
 * Permite visualizar rendimientos combinados de un subconjunto de cuentas
 * @see docs/stories/24.story.md (FEAT-CHART-001)
 * @deprecated Use queryOperations({ action: 'getMultiAccountHistoricalReturns', payload: {...} })
 */
// exports.getMultiAccountHistoricalReturns = historicalReturnsService.getMultiAccountHistoricalReturns;

// ============================================================================
// Cloud Functions - Pre-cálculo de Índices Históricos (OPT-009)
// ============================================================================
// NOTA: getIndexHistory está consolidada en queryOperations
// Pero refreshIndexCache es scheduled y se mantiene

/*
 * Obtiene datos históricos de índices de mercado con cache global
 * Reduce lecturas de ~1,300 a 1 (cache hit) por consulta
 * @see docs/stories/14.story.md (OPT-009)
 * @deprecated Use queryOperations({ action: 'getIndexHistory', payload: {...} })
 */
// exports.getIndexHistory = indexHistoryService.getIndexHistory;

/**
 * Refresca el cache de todos los índices históricos (scheduled)
 * Se ejecuta diariamente a las 00:30 UTC
 * NOTA: Esta función scheduled NO está consolidada, se mantiene activa
 * @see docs/stories/14.story.md (OPT-009)
 */
exports.refreshIndexCache = indexHistoryService.refreshIndexCache;

// ============================================================================
// Cloud Functions Callable - Distribución de Portafolio (SCALE-OPT-001)
// ============================================================================
// DEPRECATED: Estas funciones fueron consolidadas en queryOperations (SCALE-CF-001)
// TODO: Eliminar después de 2 semanas de uso exitoso (2025-01-07)

const { onCall, HttpsError } = require('firebase-functions/v2/https');
const portfolioDistributionService = require('./services/portfolioDistributionService');
const { withRateLimit } = require('./utils/rateLimiter');

/*
 * Obtiene distribución del portafolio (sectores, países, holdings)
 * Migrado desde usePortfolioDistribution.ts y useCountriesDistribution.ts
 * Reduce lecturas Firestore de ~200 a 1 (cache hit)
 * 
 * @see docs/stories/55.story.md (SCALE-OPT-001)
 * @deprecated Use queryOperations({ action: 'getPortfolioDistribution', payload: {...} })
 */
/*
exports.getPortfolioDistribution = onCall(
  {
    cors: true,
    memory: '256MiB',
    timeoutSeconds: 60,
    minInstances: 0,
    maxInstances: 10,
  },
  withRateLimit({ functionName: 'getPortfolioDistribution', limit: 60, windowMs: 60000 })(
    async (request) => {
      console.log('[getPortfolioDistribution] Request received');
      
      if (!request.auth) {
        throw new HttpsError('unauthenticated', 'Autenticación requerida');
      }

      console.log('[getPortfolioDistribution] User:', request.auth.uid);
      const { accountIds, accountId, currency, includeHoldings } = request.data || {};
      console.log('[getPortfolioDistribution] Options:', { accountIds, accountId, currency, includeHoldings });

      try {
        const result = await portfolioDistributionService.getPortfolioDistribution(
          request.auth.uid,
          { 
            accountIds, 
            accountId, 
            currency: currency || 'USD', 
            includeHoldings: includeHoldings ?? true 
          }
        );

        console.log('[getPortfolioDistribution] Result:', {
          sectorsCount: result.sectors?.length || 0,
          holdingsCount: result.holdings?.length || 0,
          countriesCount: result.countries?.length || 0,
          fromCache: result.fromCache || false
        });

        return result;
      } catch (error) {
        console.error('[getPortfolioDistribution] Error:', error);
        throw new HttpsError('internal', 'Error calculando distribución del portafolio');
      }
    }
  )
);
*/

/*
 * Obtiene sectores disponibles en el sistema
 * @see docs/stories/55.story.md (SCALE-OPT-001)
 * @deprecated Use queryOperations({ action: 'getAvailableSectors', payload: {} })
 */
/*
exports.getAvailableSectors = onCall(
  {
    cors: true,
    memory: '256MiB',
    timeoutSeconds: 30,
    minInstances: 0,
    maxInstances: 5,
  },
  withRateLimit({ functionName: 'getAvailableSectors', limit: 30, windowMs: 60000 })(
    async (request) => {
      if (!request.auth) {
        throw new HttpsError('unauthenticated', 'Autenticación requerida');
      }

      try {
        const sectors = await portfolioDistributionService.getAvailableSectors();
        return { sectors };
      } catch (error) {
        console.error('[getAvailableSectors] Error:', error);
        throw new HttpsError('internal', 'Error obteniendo sectores disponibles');
      }
    }
  )
);
*/

// ============================================================================
// Rate Limiting Cleanup (SCALE-BE-004)
// ============================================================================

const admin = require('./services/firebaseAdmin');
const { RATE_LIMITS_COLLECTION } = require('./utils/rateLimiter');

/**
 * Limpia documentos de rate limits expirados de Firestore
 * Se ejecuta cada hora para evitar acumulación de documentos obsoletos
 * 
 * @see docs/stories/49.story.md (SCALE-BE-004)
 */
exports.cleanupRateLimits = onSchedule(
  {
    schedule: '0 * * * *',
    timeZone: 'America/New_York',
    retryCount: 1,
    memory: '256MiB',
  },
  async () => {
    const db = admin.firestore();
    const cutoff = Date.now() - (60 * 60 * 1000);
    
    const snapshot = await db.collection(RATE_LIMITS_COLLECTION)
      .where('lastUpdated', '<', cutoff)
      .limit(500)
      .get();
    
    if (snapshot.empty) {
      console.log('[cleanupRateLimits] No expired rate limit documents to clean');
      return;
    }
    
    const batch = db.batch();
    snapshot.docs.forEach(doc => batch.delete(doc.ref));
    await batch.commit();
    
    console.log(`[cleanupRateLimits] Cleaned ${snapshot.docs.length} expired rate limit docs`);
  }
);

// ============================================================================
// Health Check Endpoint - Circuit Breaker Status (SCALE-BE-003)
// ============================================================================

const { getAllCircuitStates, resetCircuit } = require('./utils/circuitBreaker');

/**
 * Health check endpoint that includes circuit breaker status
 * GET /healthCheck - Returns system health and circuit states
 * POST /healthCheck?reset=circuit-name - Resets a specific circuit
 * 
 * @see docs/stories/48.story.md (SCALE-BE-003)
 */
exports.healthCheck = onRequest({ cors: true }, async (req, res) => {
  const circuitStates = getAllCircuitStates();
  
  const openCircuits = Object.entries(circuitStates)
    .filter(([_, state]) => state.state === 'OPEN')
    .map(([name]) => name);

  const health = {
    status: openCircuits.length > 0 ? 'degraded' : 'healthy',
    timestamp: new Date().toISOString(),
    version: '2.0.0',
    circuits: circuitStates,
  };

  if (openCircuits.length > 0) {
    health.warnings = [`Circuit breakers open: ${openCircuits.join(', ')}`];
  }

  // Allow manual circuit reset via POST request
  if (req.method === 'POST' && req.query.reset) {
    const circuitName = req.query.reset;
    const wasReset = resetCircuit(circuitName);
    
    if (wasReset) {
      health.message = `Circuit '${circuitName}' was reset`;
    } else {
      health.message = `Circuit '${circuitName}' not found`;
    }
  }

  res.json(health);
});

// ============================================================================
// SCALE-CF-001: Funciones Unificadas (Cloud Functions Consolidation)
// ============================================================================

/**
 * Router unificado para operaciones de assets
 * Consolida 8 funciones: createAsset, updateAsset, sellAsset, deleteAsset,
 * deleteAssets, sellPartialAssetsFIFO, addCashTransaction, updateStockSector
 * 
 * @see docs/stories/56.story.md (SCALE-CF-001)
 */
const { portfolioOperations } = require('./services/unified/portfolioOperations');
exports.portfolioOperations = portfolioOperations;

/**
 * Router unificado para operaciones de settings
 * Consolida 6 funciones: addCurrency, updateCurrency, deleteCurrency,
 * updateDefaultCurrency, updateUserCountry, updateUserDisplayName
 * 
 * @see docs/stories/56.story.md (SCALE-CF-001)
 */
const { settingsOperations: unifiedSettingsOps } = require('./services/unified/settingsOperations');
exports.settingsOperations = unifiedSettingsOps;

/**
 * Router unificado para operaciones de cuentas
 * Consolida 4 funciones: addPortfolioAccount, updatePortfolioAccount,
 * deletePortfolioAccount, updatePortfolioAccountBalance
 * 
 * @see docs/stories/56.story.md (SCALE-CF-001)
 */
const { accountOperations } = require('./services/unified/accountOperations');
exports.accountOperations = accountOperations;

/**
 * Router unificado para operaciones de consulta
 * Consolida 6 funciones: getCurrentPricesForUser, getHistoricalReturns,
 * getMultiAccountHistoricalReturns, getIndexHistory, getPortfolioDistribution, getAvailableSectors
 * 
 * NOTA: Usa 512MiB de memoria para soportar cálculos pesados
 * 
 * @see docs/stories/56.story.md (SCALE-CF-001)
 */
const { queryOperations } = require('./services/unified/queryOperations');
exports.queryOperations = queryOperations;

/**
 * COST-OPT-002: Scheduled Functions para consolidación de períodos
 * 
 * - consolidateMonthlyPerformance: Día 1 de cada mes, 00:30 ET
 * - consolidateYearlyPerformance: 1 de Enero, 01:00 ET
 * 
 * @see docs/stories/63.story.md (COST-OPT-002)
 */
const { 
  consolidateMonthlyPerformance, 
  consolidateYearlyPerformance 
} = require('./services/periodConsolidationScheduled');

exports.consolidateMonthlyPerformance = consolidateMonthlyPerformance;
exports.consolidateYearlyPerformance = consolidateYearlyPerformance;