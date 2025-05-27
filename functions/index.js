require('dotenv').config();
const { onRequest } = require("firebase-functions/v2/https");
const httpApp = require('./httpApi');
// Habilitar las funciones actualizadas a v2
const updateCurrencyRates = require('./services/updateCurrencyRates');
const calcDailyPortfolioPerf = require('./services/calculateDailyPortfolioPerformance');
const { scheduledUpdatePrices, scheduledUpdateQuotes } = require('./services/updateCurrentPrices');
const processDividendPayments = require('./services/processDividendPayments');
const marketStatusService = require('./services/marketStatusService');
const { saveAllIndicesAndSectorsHistoryData } = require("./services/saveAllIndicesAndSectorsHistoryData");

// Calcular semanalmente el porcentaje de semanas rentables
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { calculateProfitableWeeks } = require('./services/calculateProfitableWeeks');

// Calcular diariamente el riesgo del portafolio basado en beta
const { calculatePortfolioRisk } = require('./services/calculatePortfolioRisk');

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

// Exportar las funciones actualizadas con un nombre diferente para v2
exports.updateCurrencyRatesV2 = updateCurrencyRates.updateCurrencyRates;
exports.scheduledUpdatePricesV2 = scheduledUpdatePrices;
exports.scheduledUpdateQuotesV2 = scheduledUpdateQuotes;
exports.saveAllIndicesAndSectorsHistoryDataV2 = saveAllIndicesAndSectorsHistoryData;
exports.calcDailyPortfolioPerfV2 = calcDailyPortfolioPerf.calcDailyPortfolioPerf;
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

exports.dailyPortfolioRiskCalculation = onSchedule({
  schedule: "*/3 9-17 * * 1-5",
  timeZone: "America/New_York",
  retryCount: 3,
}, async (event) => {
  try {
    console.log('Iniciando cálculo diario de riesgo del portafolio');
    await calculatePortfolioRisk();
    console.log('Cálculo diario de riesgo del portafolio completado con éxito');
    return null;
  } catch (error) {
    console.error('Error en cálculo diario de riesgo del portafolio:', error);
    return null;
  }
});