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

/**
 * Crea un nuevo asset con transacción de compra y actualización de balance atómicamente
 * @see docs/architecture/refactoring-analysis.md - Sección 1.1
 */
exports.createAsset = assetOperations.createAsset;

/**
 * Vende un asset existente (total o parcialmente)
 * @see docs/architecture/refactoring-analysis.md - Sección 1.2
 */
exports.sellAsset = assetOperations.sellAsset;

/**
 * Vende unidades de múltiples lotes del mismo ticker usando FIFO
 * @see docs/architecture/refactoring-analysis.md - Sección 1.3
 */
exports.sellPartialAssetsFIFO = assetOperations.sellPartialAssetsFIFO;

/**
 * Registra una transacción de efectivo (ingreso o egreso)
 * @see docs/architecture/refactoring-analysis.md - Sección 1.4
 */
exports.addCashTransaction = assetOperations.addCashTransaction;

/**
 * Elimina activos de una cuenta de portafolio
 */
exports.deleteAssets = assetOperations.deleteAssets;

/**
 * Actualiza el sector de un stock en currentPrices (fallback manual)
 */
exports.updateStockSector = assetOperations.updateStockSector;

// ============================================================================
// Cloud Functions Callable - Optimización de Consultas (OPT-001)
// ============================================================================

/**
 * Obtiene precios filtrados por símbolos que el usuario posee
 * Reduce lecturas de 56 (todos) a ~25 (solo del usuario)
 * @see docs/stories/6.story.md (OPT-001)
 */
exports.getCurrentPricesForUser = userPricesService.getCurrentPricesForUser;

// ============================================================================
// Cloud Functions Callable - Pre-cálculo de Rendimientos (OPT-002)
// ============================================================================

/**
 * Pre-calcula rendimientos históricos en el servidor con cache
 * Reduce lecturas de ~200 a 1 (cache hit) por montaje del Dashboard
 * @see docs/stories/7.story.md (OPT-002)
 */
exports.getHistoricalReturns = historicalReturnsService.getHistoricalReturns;

/**
 * FEAT-CHART-001: Pre-calcula rendimientos históricos de múltiples cuentas agregados
 * Permite visualizar rendimientos combinados de un subconjunto de cuentas
 * @see docs/stories/24.story.md (FEAT-CHART-001)
 */
exports.getMultiAccountHistoricalReturns = historicalReturnsService.getMultiAccountHistoricalReturns;

// ============================================================================
// Cloud Functions - Pre-cálculo de Índices Históricos (OPT-009)
// ============================================================================

/**
 * Obtiene datos históricos de índices de mercado con cache global
 * Reduce lecturas de ~1,300 a 1 (cache hit) por consulta
 * @see docs/stories/14.story.md (OPT-009)
 */
exports.getIndexHistory = indexHistoryService.getIndexHistory;

/**
 * Refresca el cache de todos los índices históricos (scheduled)
 * Se ejecuta diariamente a las 00:30 UTC
 * @see docs/stories/14.story.md (OPT-009)
 */
exports.refreshIndexCache = indexHistoryService.refreshIndexCache;

