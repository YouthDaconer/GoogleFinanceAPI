const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const admin = require('firebase-admin');
const axios = require('axios');
const { calculateAccountPerformance, convertCurrency } = require('../utils/portfolioCalculations');
const { calculatePortfolioRisk } = require('./calculatePortfolioRisk');
const { invalidatePerformanceCacheBatch } = require('./cacheInvalidationService');
const { DateTime } = require('luxon');
const pLimit = require("p-limit");

// Importar generador de logos
const { generateLogoUrl } = require('../utils/logoGenerator');

// PERF-SNAP-004: Importar generador de snapshots pre-computados
// PERF-SNAP-024: Importar generador de snapshots per-asset
const { generateAllSnapshots, generateAllAssetSnapshots, fetchLatestAssetPerformance } = require('./snapshotGenerator');

// VS-009: Importar servicio de benchmark snapshots
const { updateBenchmarkSnapshots } = require('./benchmarkSnapshotService');

/**
 * SCALE-001: Máximo de usuarios procesados en paralelo.
 * Calibrado para no exceder throughput de Firestore (~500 writes/sec).
 * 5 usuarios × ~8 writes/usuario = ~40 writes simultáneos.
 *
 * @see docs/architecture/SCALE-PERF-001-consolidation-sustainability-diagnosis.md §6.1
 */
const MAX_PARALLEL_USERS = 5;

/**
 * SEC-TOKEN-001: Secret para autenticación server-to-server con API finance-query
 * Usado por getPricesFromApi/getCurrencyRatesFromApi para obtener datos EOD.
 * 
 * @see docs/architecture/SEC-TOKEN-001-api-security-hardening-plan.md
 */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

// Importar logger estructurado (SCALE-CORE-002)
const { StructuredLogger } = require('../utils/logger');

// OPT-DEMAND-CLEANUP: Importar helper para obtener precios y currencies del API Lambda
const { getPricesFromApi, getCurrencyRatesFromApi, normalizeToUsdBase } = require('./marketDataHelper');

/**
 * End-of-Day Portfolio Update
 * 
 * OPT-DEMAND-CLEANUP: Refactorizada como el único punto de cálculos EOD.
 * 
 * CAMBIOS desde 2026-01-16:
 * - Se ejecuta 1x/día a las 17:05 ET (5 min después del cierre)
 * - Lee símbolos de `assets` (no de `currentPrices`)
 * - Obtiene precios del API Lambda (no de Firestore)
 * - NO escribe a `currentPrices` ni `currencies`
 * - Calcula performance del portafolio
 * - Calcula riesgo del portafolio
 * - Invalida cache de performance
 * 
 * Reemplaza las funciones redundantes:
 * - dailyEODSnapshot (deprecada)
 * - scheduledPortfolioCalculations (deprecada)
 * 
 * @see docs/architecture/OPT-DEMAND-CLEANUP-phase4-closure-subplan.md
 * @see docs/architecture/SEC-CF-001-cloudflare-tunnel-migration-plan.md
 * @see docs/stories/85.story.md (OPT-DEMAND-302)
 */

const { FINANCE_QUERY_API_URL } = require('./config');

// SEC-CF-001: API URL via Cloudflare Tunnel
const API_BASE_URL = FINANCE_QUERY_API_URL;

// Flag para habilitar logs detallados (puede causar mucho ruido en producción)
const ENABLE_DETAILED_LOGS = process.env.ENABLE_DETAILED_LOGS === 'true';

// Horarios de NYSE en hora local de Nueva York (no UTC)
// Esto maneja automáticamente EST/EDT gracias a Luxon
const NYSE_OPEN_HOUR = 9;
const NYSE_OPEN_MINUTE = 30;
const NYSE_CLOSE_HOUR = 16;  // 4:00 PM

// Ventana de gracia después del cierre para capturar precios finales (en minutos)
const CLOSING_GRACE_WINDOW_MINUTES = 5;

// Logger global para este módulo (se inicializa en cada ejecución)
let logger = null;

function logDebug(...args) {
  if (logger) {
    logger.debug(args[0], args.length > 1 ? { details: args.slice(1) } : {});
  }
}

function logInfo(...args) {
  if (logger) {
    logger.info(args[0], args.length > 1 ? { details: args.slice(1) } : {});
  }
}

function logWarn(...args) {
  if (logger) {
    logger.warn(args[0], args.length > 1 ? { details: args.slice(1) } : {});
  }
}

function logError(...args) {
  if (logger) {
    const error = args.find(a => a instanceof Error);
    const message = typeof args[0] === 'string' ? args[0] : 'Error';
    logger.error(message, error, { details: args.filter(a => !(a instanceof Error) && a !== message) });
  } else {
    console.error(...args);
  }
}

/**
 * Verifica si el mercado NYSE está abierto (fallback local).
 * Usa Luxon para manejar correctamente EST/EDT automáticamente.
 * NOTA: Este es un fallback - el código principal usa markets/US.isOpen de Finnhub.
 */
function isNYSEMarketOpen() {
  const nyNow = DateTime.now().setZone('America/New_York');
  const hour = nyNow.hour;
  const minute = nyNow.minute;
  const dayOfWeek = nyNow.weekday; // 1=Monday, 7=Sunday
  
  // Fin de semana: cerrado
  if (dayOfWeek === 6 || dayOfWeek === 7) {
    return false;
  }
  
  // Convertir hora actual a minutos desde medianoche
  const currentMinutes = hour * 60 + minute;
  const openMinutes = NYSE_OPEN_HOUR * 60 + NYSE_OPEN_MINUTE; // 9:30 = 570
  const closeMinutes = NYSE_CLOSE_HOUR * 60; // 16:00 = 960
  
  return currentMinutes >= openMinutes && currentMinutes < closeMinutes;
}

/**
 * Verifica si estamos en la "ventana de cierre" del mercado.
 * Esta ventana permite una última actualización justo después del cierre
 * para capturar los precios finales del día.
 * 
 * @param {number} closeHour - Hora de cierre del mercado (ej: 16 para 4:00 PM)
 * @returns {{ inWindow: boolean, reason: string }} - Si estamos en la ventana y por qué
 */
function isInClosingWindow(closeHour = NYSE_CLOSE_HOUR) {
  const nyNow = DateTime.now().setZone('America/New_York');
  const hour = nyNow.hour;
  const minute = nyNow.minute;
  const dayOfWeek = nyNow.weekday;
  
  // Fin de semana: no hay ventana de cierre
  if (dayOfWeek === 6 || dayOfWeek === 7) {
    return { inWindow: false, reason: 'weekend' };
  }
  
  const currentMinutes = hour * 60 + minute;
  const closeMinutes = closeHour * 60;
  const graceEndMinutes = closeMinutes + CLOSING_GRACE_WINDOW_MINUTES;
  
  // Estamos en la ventana si:
  // - Es exactamente la hora de cierre (16:00), O
  // - Estamos dentro de los primeros N minutos después del cierre
  if (currentMinutes >= closeMinutes && currentMinutes <= graceEndMinutes) {
    return { 
      inWindow: true, 
      reason: `closing-window (${closeHour}:00 + ${CLOSING_GRACE_WINDOW_MINUTES}min grace)`,
      minutesAfterClose: currentMinutes - closeMinutes
    };
  }
  
  return { inWindow: false, reason: 'outside-window' };
}

/**
 * OPT-DEMAND-400-FIX: Lista de festivos de NYSE como fallback
 * 
 * NOTA: Esta lista es un FALLBACK en caso de que marketHolidays no esté disponible.
 * La fuente principal de verdad es la colección marketHolidays/US sincronizada
 * desde Finnhub mediante scheduledHolidaySync.
 * 
 * @see marketStatusService.js - syncMarketHolidays()
 */
const NYSE_HOLIDAYS_FALLBACK = new Set([
  // 2025
  '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26',
  '2025-06-19', '2025-07-04', '2025-09-01', '2025-11-27', '2025-12-25',
  // 2026
  '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25',
  '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
  // 2027
  '2027-01-01', '2027-01-18', '2027-02-15', '2027-03-26', '2027-05-31',
  '2027-06-18', '2027-07-05', '2027-09-06', '2027-11-25', '2027-12-24',
]);

/**
 * OPT-DEMAND-400-FIX: Verifica si una fecha fue un día de trading válido de NYSE
 * 
 * Un día es válido para guardar en portfolioPerformance si:
 * 1. NO fue fin de semana (sábado o domingo)
 * 2. NO fue un día festivo de NYSE
 * 
 * Orden de consulta para festivos:
 * 1. marketHolidays/US (sincronizado desde Finnhub - fuente principal)
 * 2. NYSE_HOLIDAYS_FALLBACK (lista estática - respaldo)
 * 
 * @param {FirebaseFirestore.Firestore} db - Instancia de Firestore
 * @param {DateTime} date - Fecha a verificar (Luxon DateTime)
 * @returns {Promise<{isValid: boolean, reason: string, holiday?: string}>}
 */
async function isValidTradingDay(db, date) {
  const dayOfWeek = date.weekday; // 1=Monday, 7=Sunday
  const formattedDate = date.toISODate();
  
  // 1. Verificar fin de semana
  if (dayOfWeek === 6) {
    return { isValid: false, reason: 'saturday', formattedDate };
  }
  if (dayOfWeek === 7) {
    return { isValid: false, reason: 'sunday', formattedDate };
  }
  
  // 2. Verificar festivo en marketHolidays/US (fuente principal - sincronizado desde Finnhub)
  try {
    const holidaysDoc = await db.collection('marketHolidays').doc('US').get();
    
    if (holidaysDoc.exists) {
      const holidaysData = holidaysDoc.data();
      
      // El campo 'holidays' es un mapa: { "2026-01-19": "Martin Luther King Jr. Day", ... }
      if (holidaysData.holidays && holidaysData.holidays[formattedDate]) {
        const holidayName = holidaysData.holidays[formattedDate];
        logInfo(`🎄 Holiday detected from marketHolidays: ${holidayName} (${formattedDate})`);
        return { 
          isValid: false, 
          reason: 'holiday-marketHolidays', 
          holiday: holidayName,
          formattedDate 
        };
      }
      
      // Si llegamos aquí, marketHolidays existe pero la fecha no es festivo
      return { isValid: true, reason: 'trading-day', formattedDate };
    }
    
    // Si marketHolidays no existe, usar fallback estático
    logWarn('⚠️ marketHolidays/US no encontrado, usando lista estática como fallback');
    
  } catch (error) {
    logWarn(`⚠️ Error consultando marketHolidays: ${error.message}, usando fallback`);
  }
  
  // 3. Fallback: Verificar en lista estática
  if (NYSE_HOLIDAYS_FALLBACK.has(formattedDate)) {
    return { 
      isValid: false, 
      reason: 'holiday-fallback-list', 
      holiday: 'NYSE Holiday',
      formattedDate 
    };
  }
  
  // Si pasó todas las validaciones, es un día de trading válido
  return { isValid: true, reason: 'trading-day', formattedDate };
}

/**
 * 🚀 OPTIMIZACIÓN: Función unificada que obtiene todos los datos de mercado en una sola llamada
 * Combina monedas y símbolos de activos para minimizar llamadas a la API Lambda
 */
async function getAllMarketDataBatch(currencyCodes, assetSymbols) {
  try {
    // Preparar símbolos de monedas (agregar %3DX para codificación URL)
    const currencySymbols = currencyCodes.map(code => `${code}%3DX`);
    
    // Combinar todos los símbolos en una sola consulta
    const allSymbols = [...currencySymbols, ...assetSymbols];
    
    // Dividir en lotes más grandes (100 símbolos por llamada para optimizar)
    const batchSize = 100;
    const results = {
      currencies: {},
      assets: new Map()
    };
    
    logInfo(`📡 Consultando ${allSymbols.length} símbolos en ${Math.ceil(allSymbols.length / batchSize)} lotes optimizados`);
    
    for (let i = 0; i < allSymbols.length; i += batchSize) {
      const symbolBatch = allSymbols.slice(i, i + batchSize);
      const symbolsParam = symbolBatch.join(',');
      
      const url = `${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`;
      logDebug(`🔄 Lote ${Math.floor(i/batchSize) + 1}: ${symbolBatch.length} símbolos`);
      
      const { data } = await axios.get(url);
      
      if (Array.isArray(data)) {
        data.forEach(item => {
          if (item.symbol && item.regularMarketPrice) {
            // Si es una moneda (termina en =X en la respuesta)
            if (item.symbol.includes('=X')) {
              const currencyCode = item.symbol.replace('=X', '');
              if (currencyCodes.includes(currencyCode)) {
                results.currencies[currencyCode] = item.regularMarketPrice;
              }
            } 
            // Si es un activo normal
            else if (assetSymbols.includes(item.symbol)) {
              results.assets.set(item.symbol, item);
            }
          }
        });
      }
    }
    
    logInfo(`✅ Datos obtenidos: ${Object.keys(results.currencies).length} monedas, ${results.assets.size} activos`);
    return results;
  } catch (error) {
    logError(`❌ Error al obtener datos de mercado en lote:`, error.message);
    return { currencies: {}, assets: new Map() };
  }
}

// ============================================================================
// OPT-DEMAND-CLEANUP: Funciones eliminadas (2026-01-17)
// ============================================================================
// Las siguientes funciones fueron ELIMINADAS porque ya no se usan:
//
// - updateCurrencyRates(db, currencyRates)
//   Razón: Las tasas de cambio ahora vienen del API Lambda on-demand.
//   No se escriben a Firestore.
//
// - updateCurrentPrices(db, assetQuotes)
//   Razón: Los precios ahora vienen del API Lambda on-demand.
//   No se escriben a Firestore.
//
// Ver: docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
// ============================================================================

/**
 * 🚀 OPTIMIZACIÓN: Sistema de caché para datos históricos
 */
class PerformanceDataCache {
  constructor() {
    this.userLastPerformance = new Map();
    this.accountLastPerformance = new Map();
    this.buyTransactionsByAsset = new Map();
  }

  async preloadHistoricalData(db, formattedDate, userPortfolios, sellTransactions) {
    logDebug('📁 Precargando datos históricos (OPTIMIZACIÓN)...');
    
    const allUserIds = Object.keys(userPortfolios);
    const allAccountIds = Object.values(userPortfolios).flat().map(acc => acc.id);
    const assetIdsWithSells = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];

    // ✨ OPTIMIZACIÓN: Consultas paralelas masivas
    const [userPerformanceResults, accountPerformanceResults, buyTransactionsResults] = await Promise.all([
      Promise.all(allUserIds.map(async (userId) => {
        const userRef = db.collection('portfolioPerformance').doc(userId);
        const query = await userRef.collection('dates')
          .where('date', '<', formattedDate)
          .orderBy('date', 'desc')
          .limit(1)
          .get();
        return { userId, data: query.empty ? null : query.docs[0].data() };
      })),
      
      Promise.all(allAccountIds.map(async (accountId) => {
        const userId = Object.keys(userPortfolios).find(uid => 
          userPortfolios[uid].some(acc => acc.id === accountId)
        );
        const accountRef = db.collection('portfolioPerformance')
          .doc(userId)
          .collection('accounts')
          .doc(accountId);
        const query = await accountRef.collection('dates')
          .where('date', '<', formattedDate)
          .orderBy('date', 'desc')
          .limit(1)
          .get();
        return { accountId, data: query.empty ? null : query.docs[0].data() };
      })),
      
      assetIdsWithSells.length > 0 ? 
        db.collection('transactions')
          .where('type', '==', 'buy')
          .where('assetId', 'in', assetIdsWithSells)
          .get() : 
        { docs: [] }
    ]);

    // Procesar resultados
    userPerformanceResults.forEach(({ userId, data }) => {
      if (data) this.userLastPerformance.set(userId, data);
    });

    accountPerformanceResults.forEach(({ accountId, data }) => {
      if (data) this.accountLastPerformance.set(accountId, data);
    });

    if (buyTransactionsResults.docs) {
      const buyTransactions = buyTransactionsResults.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      buyTransactions.forEach(tx => {
        if (!this.buyTransactionsByAsset.has(tx.assetId)) {
          this.buyTransactionsByAsset.set(tx.assetId, []);
        }
        this.buyTransactionsByAsset.get(tx.assetId).push(tx);
      });
    }

    logInfo(`✅ Caché precargado: ${this.userLastPerformance.size} usuarios, ${this.accountLastPerformance.size} cuentas`);
  }

  /**
   * OPT-DEMAND-500: Valida consistencia entre datos de usuario y cuentas.
   * 
   * Detecta situaciones donde el overall tiene datos más recientes que las cuentas,
   * lo cual causaría cálculos incorrectos de adjustedDailyChangePercentage.
   * 
   * @param {string} userId - ID del usuario
   * @param {string[]} accountIds - IDs de las cuentas del usuario
   * @returns {{isConsistent: boolean, userDate: string|null, accountDates: Object, gap: number}}
   */
  validateDataConsistency(userId, accountIds) {
    const userData = this.userLastPerformance.get(userId);
    const userDate = userData?.date || null;
    
    const accountDates = {};
    let minAccountDate = null;
    let maxAccountDate = null;
    
    for (const accountId of accountIds) {
      const accountData = this.accountLastPerformance.get(accountId);
      const accountDate = accountData?.date || null;
      accountDates[accountId] = accountDate;
      
      if (accountDate) {
        if (!minAccountDate || accountDate < minAccountDate) {
          minAccountDate = accountDate;
        }
        if (!maxAccountDate || accountDate > maxAccountDate) {
          maxAccountDate = accountDate;
        }
      }
    }
    
    // Calcular gap en días entre overall y la cuenta más antigua
    let gap = 0;
    if (userDate && minAccountDate && userDate !== minAccountDate) {
      const userDateTime = DateTime.fromISO(userDate);
      const minAccountDateTime = DateTime.fromISO(minAccountDate);
      gap = Math.abs(userDateTime.diff(minAccountDateTime, 'days').days);
    }
    
    // Es inconsistente si:
    // 1. El overall tiene fecha más reciente que alguna cuenta
    // 2. Hay un gap de más de 1 día entre cuentas
    const isConsistent = gap <= 1 && (!userDate || !minAccountDate || userDate <= maxAccountDate);
    
    return {
      isConsistent,
      userDate,
      accountDates,
      minAccountDate,
      maxAccountDate,
      gap
    };
  }

  getUserLastPerformance(userId, currencies) {
    const data = this.userLastPerformance.get(userId);
    if (!data) {
      return currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});
    }
    
    return Object.entries(data).reduce((acc, [currency, currencyData]) => {
      if (currency !== 'date') {
        acc[currency] = {
          totalValue: currencyData.totalValue || 0,
          ...Object.entries(currencyData.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
            assetAcc[assetName] = { 
              totalValue: assetData.totalValue || 0,
              units: assetData.units || 0
            };
            return assetAcc;
          }, {})
        };
      }
      return acc;
    }, {});
  }

  getAccountLastPerformance(accountId, currencies) {
    const data = this.accountLastPerformance.get(accountId);
    if (!data) {
      return currencies.reduce((acc, cur) => ({ ...acc, [cur.code]: { totalValue: 0 } }), {});
    }
    
    return Object.entries(data).reduce((acc, [currency, currencyData]) => {
      if (currency !== 'date') {
        acc[currency] = {
          totalValue: currencyData.totalValue || 0,
          ...Object.entries(currencyData.assetPerformance || {}).reduce((assetAcc, [assetName, assetData]) => {
            assetAcc[assetName] = { 
              totalValue: assetData.totalValue || 0,
              units: assetData.units || 0
            };
            return assetAcc;
          }, {})
        };
      }
      return acc;
    }, {});
  }

  getBuyTransactionsForAsset(assetId) {
    return this.buyTransactionsByAsset.get(assetId) || [];
  }
}

/**
 * SCALE-001: Procesa el rendimiento de un solo usuario.
 * Aislado para paralelización. Atómico por WriteBatch local.
 *
 * @param {Object} params
 * @param {FirebaseFirestore.Firestore} params.db
 * @param {string} params.userId
 * @param {Array} params.accounts - Cuentas activas del usuario
 * @param {Array} params.currentPrices
 * @param {Array} params.currencies
 * @param {PerformanceDataCache} params.cache
 * @param {Array} params.allAssets
 * @param {Array} params.assetsToInclude
 * @param {Array} params.sellTransactions
 * @param {Object} params.sellTransactionsByAccount
 * @param {Array} params.inactiveAssets
 * @param {Array} params.activeAssets
 * @param {Array} params.todaysTransactions
 * @param {string} params.formattedDate
 * @returns {Promise<{userId: string, success: boolean, error?: string}>}
 * @see docs/architecture/SCALE-PERF-001-consolidation-sustainability-diagnosis.md §6.1
 */
async function processUserPerformance({
  db, userId, accounts, currentPrices, currencies,
  cache, allAssets, assetsToInclude, sellTransactions,
  sellTransactionsByAccount, inactiveAssets, activeAssets,
  todaysTransactions, formattedDate
}) {
  const startMs = Date.now();
  try {
    logDebug(`👤 Procesando usuario ${userId} con ${accounts.length} cuentas`);
    const lastOverallTotalValue = cache.getUserLastPerformance(userId, currencies);

    const allUserAssets = assetsToInclude.filter(asset =>
      accounts.some(account => account.id === asset.portfolioAccount)
    );

    const userTransactions = todaysTransactions.filter(t =>
      accounts.some(account => account.id === t.portfolioAccountId)
    );

    const overallPerformance = calculateAccountPerformance(
      allUserAssets,
      currentPrices,
      currencies,
      lastOverallTotalValue,
      userTransactions
    );

    // Calcular doneProfitAndLoss para cada moneda
    const userDoneProfitAndLossByCurrency = {};
    const userSellTransactions = sellTransactions.filter(t =>
      accounts.some(account => account.id === t.portfolioAccountId)
    );

    for (const currency of currencies) {
      let totalDoneProfitAndLoss = 0;
      const assetDoneProfitAndLoss = {};

      userSellTransactions.forEach(sellTx => {
        if (sellTx.assetId) {
          const asset = allAssets.find(a => a.id === sellTx.assetId);
          if (asset) {
            const assetKey = `${asset.name}_${asset.assetType}`;
            if (!assetDoneProfitAndLoss[assetKey]) {
              assetDoneProfitAndLoss[assetKey] = 0;
            }

            let profitAndLoss = 0;

            if (sellTx.valuePnL !== undefined && sellTx.valuePnL !== null) {
              profitAndLoss = convertCurrency(
                sellTx.valuePnL,
                sellTx.currency,
                currency.code,
                currencies,
                sellTx.defaultCurrencyForAdquisitionDollar,
                parseFloat(sellTx.dollarPriceToDate.toString())
              );
            } else {
              const sellAmountConverted = convertCurrency(
                sellTx.amount * sellTx.price,
                sellTx.currency,
                currency.code,
                currencies,
                sellTx.defaultCurrencyForAdquisitionDollar,
                parseFloat(sellTx.dollarPriceToDate.toString())
              );

              const buyTxsForAsset = cache.getBuyTransactionsForAsset(sellTx.assetId);

              if (buyTxsForAsset.length > 0) {
                let totalBuyCost = 0;
                let totalBuyUnits = 0;

                buyTxsForAsset.forEach(buyTx => {
                  totalBuyCost += buyTx.amount * buyTx.price;
                  totalBuyUnits += buyTx.amount;
                });

                const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                const costOfSoldUnits = sellTx.amount * avgCostPerUnit;

                const costOfSoldUnitsConverted = convertCurrency(
                  costOfSoldUnits,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );

                profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
              }
            }

            assetDoneProfitAndLoss[assetKey] += profitAndLoss;
            totalDoneProfitAndLoss += profitAndLoss;
          }
        }
      });

      userDoneProfitAndLossByCurrency[currency.code] = {
        doneProfitAndLoss: totalDoneProfitAndLoss,
        assetDoneProfitAndLoss
      };
    }

    // Agregar doneProfitAndLoss a overallPerformance
    for (const [currencyCode, data] of Object.entries(userDoneProfitAndLossByCurrency)) {
      if (overallPerformance[currencyCode]) {
        overallPerformance[currencyCode].doneProfitAndLoss = data.doneProfitAndLoss;

        const unrealizedProfitAndLoss = overallPerformance[currencyCode].totalValue - overallPerformance[currencyCode].totalInvestment;
        overallPerformance[currencyCode].unrealizedProfitAndLoss = unrealizedProfitAndLoss;

        if (overallPerformance[currencyCode].assetPerformance) {
          for (const [assetKey, pnl] of Object.entries(data.assetDoneProfitAndLoss)) {
            if (overallPerformance[currencyCode].assetPerformance[assetKey]) {
              overallPerformance[currencyCode].assetPerformance[assetKey].doneProfitAndLoss = pnl;
            }
          }

          for (const [assetKey, assetData] of Object.entries(overallPerformance[currencyCode].assetPerformance)) {
            const assetTotalValue = assetData.totalValue || 0;
            const assetTotalInvestment = assetData.totalInvestment || 0;
            const assetUnrealizedPnL = assetTotalValue - assetTotalInvestment;

            overallPerformance[currencyCode].assetPerformance[assetKey].unrealizedProfitAndLoss = assetUnrealizedPnL;
          }
        }
      }
    }

    // SCALE-001: WriteBatch local — atómico por usuario
    const batch = db.batch();

    const userPerformanceRef = db.collection("portfolioPerformance").doc(userId);
    batch.set(userPerformanceRef, { userId }, { merge: true });

    const userOverallPerformanceRef = userPerformanceRef.collection("dates").doc(formattedDate);
    batch.set(userOverallPerformanceRef, {
      date: formattedDate,
      ...overallPerformance
    });

    // Procesar cada cuenta del usuario
    for (const account of accounts) {
      const accountSellTransactions = sellTransactionsByAccount[account.id] || [];
      const inactiveAccountAssetsWithSells = inactiveAssets.filter(asset =>
        asset.portfolioAccount === account.id &&
        accountSellTransactions.some(t => t.assetId === asset.id)
      );

      const accountAssets = [
        ...activeAssets.filter(asset => asset.portfolioAccount === account.id),
        ...inactiveAccountAssetsWithSells
      ];

      const lastAccountTotalValue = cache.getAccountLastPerformance(account.id, currencies);

      const accountTransactions = userTransactions.filter(t => t.portfolioAccountId === account.id);
      const accountPerformance = calculateAccountPerformance(
        accountAssets,
        currentPrices,
        currencies,
        lastAccountTotalValue,
        accountTransactions
      );

      // Calcular doneProfitAndLoss para la cuenta
      const accountDoneProfitAndLossByCurrency = {};

      for (const currency of currencies) {
        let accountDoneProfitAndLoss = 0;
        const accountAssetDoneProfitAndLoss = {};

        accountSellTransactions.forEach(sellTx => {
          if (sellTx.assetId) {
            const asset = allAssets.find(a => a.id === sellTx.assetId);
            if (asset) {
              const assetKey = `${asset.name}_${asset.assetType}`;
              if (!accountAssetDoneProfitAndLoss[assetKey]) {
                accountAssetDoneProfitAndLoss[assetKey] = 0;
              }

              let profitAndLoss = 0;

              if (sellTx.valuePnL !== undefined && sellTx.valuePnL !== null) {
                profitAndLoss = convertCurrency(
                  sellTx.valuePnL,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );
              } else {
                const sellAmountConverted = convertCurrency(
                  sellTx.amount * sellTx.price,
                  sellTx.currency,
                  currency.code,
                  currencies,
                  sellTx.defaultCurrencyForAdquisitionDollar,
                  parseFloat(sellTx.dollarPriceToDate.toString())
                );

                const buyTxsForAsset = cache.getBuyTransactionsForAsset(sellTx.assetId);

                if (buyTxsForAsset.length > 0) {
                  let totalBuyCost = 0;
                  let totalBuyUnits = 0;

                  buyTxsForAsset.forEach(buyTx => {
                    totalBuyCost += buyTx.amount * buyTx.price;
                    totalBuyUnits += buyTx.amount;
                  });

                  const avgCostPerUnit = totalBuyCost / totalBuyUnits;
                  const costOfSoldUnits = sellTx.amount * avgCostPerUnit;

                  const costOfSoldUnitsConverted = convertCurrency(
                    costOfSoldUnits,
                    sellTx.currency,
                    currency.code,
                    currencies,
                    sellTx.defaultCurrencyForAdquisitionDollar,
                    parseFloat(sellTx.dollarPriceToDate.toString())
                  );

                  profitAndLoss = sellAmountConverted - costOfSoldUnitsConverted;
                }
              }

              accountAssetDoneProfitAndLoss[assetKey] += profitAndLoss;
              accountDoneProfitAndLoss += profitAndLoss;
            }
          }
        });

        accountDoneProfitAndLossByCurrency[currency.code] = {
          doneProfitAndLoss: accountDoneProfitAndLoss,
          assetDoneProfitAndLoss: accountAssetDoneProfitAndLoss
        };
      }

      // Agregar doneProfitAndLoss a accountPerformance
      for (const [currencyCode, data] of Object.entries(accountDoneProfitAndLossByCurrency)) {
        if (accountPerformance[currencyCode]) {
          accountPerformance[currencyCode].doneProfitAndLoss = data.doneProfitAndLoss;

          const accountUnrealizedPnL = accountPerformance[currencyCode].totalValue - accountPerformance[currencyCode].totalInvestment;
          accountPerformance[currencyCode].unrealizedProfitAndLoss = accountUnrealizedPnL;

          if (accountPerformance[currencyCode].assetPerformance) {
            for (const [assetKey, pnl] of Object.entries(data.assetDoneProfitAndLoss)) {
              if (accountPerformance[currencyCode].assetPerformance[assetKey]) {
                accountPerformance[currencyCode].assetPerformance[assetKey].doneProfitAndLoss = pnl;
              }
            }

            for (const [assetKey, assetData] of Object.entries(accountPerformance[currencyCode].assetPerformance)) {
              const accountAssetTotalValue = assetData.totalValue || 0;
              const accountAssetTotalInvestment = assetData.totalInvestment || 0;
              const accountAssetUnrealizedPnL = accountAssetTotalValue - accountAssetTotalInvestment;

              accountPerformance[currencyCode].assetPerformance[assetKey].unrealizedProfitAndLoss = accountAssetUnrealizedPnL;
            }
          }
        }
      }

      const accountRef = userPerformanceRef.collection("accounts").doc(account.id);
      batch.set(accountRef, { accountId: account.id }, { merge: true });

      const accountPerformanceRef = accountRef.collection("dates").doc(formattedDate);
      batch.set(accountPerformanceRef, {
        date: formattedDate,
        ...accountPerformance
      });
    }

    await batch.commit();

    // PERF-SNAP-004: Generar snapshots pre-computados (best-effort, no bloquea pipeline)
    try {
      // PERF-SNAP-026: Smart Currency — solo USD + defaultCurrency del usuario
      let snapshotCurrencies = ['USD'];
      try {
        const userDataDoc = await db.collection('userData').doc(userId).get();
        const defaultCurrency = userDataDoc.exists ? userDataDoc.data()?.defaultCurrency : null;
        if (defaultCurrency && defaultCurrency !== 'USD') {
          snapshotCurrencies.push(defaultCurrency);
        }
        logInfo(`[EOD][Snapshot] Currencies for ${userId}: [${snapshotCurrencies.join(', ')}] (default: ${defaultCurrency || 'USD'})`);
      } catch (currencyReadError) {
        logWarn(`[EOD][Snapshot] Could not read defaultCurrency for ${userId}, using USD only: ${currencyReadError.message}`);
      }
      const currencyCodes = snapshotCurrencies;
      const accountIds = accounts.map(a => a.id);
      const snapshotResult = await generateAllSnapshots(db, userId, currencyCodes, accountIds);
      logInfo(`[EOD][Snapshot] Snapshots generados para ${userId}: ${snapshotResult.success} OK, ${snapshotResult.failed} fallidos`);

      // PERF-SNAP-025: Reutilizar dailyDocs ya leídos (0 re-fetch)
      const dailyDocsOverall = snapshotResult.dailyDocsByAccount?.get('overall');

      // PERF-SNAP-024+025: Generar snapshots per-asset (best-effort)
      if (dailyDocsOverall) {
        try {
          for (const currencyCode of currencyCodes) {
            const latestAssetPerf = await fetchLatestAssetPerformance(db, userId, 'overall', currencyCode);
            if (Object.keys(latestAssetPerf).length > 0) {
              const assetResult = await generateAllAssetSnapshots(db, userId, currencyCode, latestAssetPerf, {
                dailyDocs: dailyDocsOverall,
              });
              logInfo(`[EOD][Snapshot] Asset snapshots para ${userId}/${currencyCode}: ${assetResult.success} OK, ${assetResult.failed} fallidos`);
            }
          }
        } catch (assetSnapshotError) {
          logWarn(`[EOD][Snapshot] Error generando asset snapshots para ${userId}: ${assetSnapshotError.message}`);
        }
      }
    } catch (snapshotError) {
      logWarn(`[EOD][Snapshot] Error generando snapshots para ${userId}: ${snapshotError.message}`);
    }

    // IMPORTANTE: Fuera del try/catch de snapshots para que SIEMPRE se escriba,
    // IndexedDB cache aunque los snapshots no se hayan regenerado.
    try {
      const lastSnapshotTs = new Date().toISOString();
      await db.collection("portfolioPerformance").doc(userId).set({
        lastSnapshotUpdate: lastSnapshotTs
      }, { merge: true });
      logInfo(`[EOD][Snapshot] lastSnapshotUpdate written for ${userId}: ${lastSnapshotTs}`);
    } catch (signalError) {
      logWarn(`[EOD][Snapshot] Error writing lastSnapshotUpdate for ${userId}: ${signalError.message}`);
    }

    return { userId, success: true, durationMs: Date.now() - startMs };
  } catch (error) {
    logError(`Error procesando usuario ${userId} (${Date.now() - startMs}ms)`, error);
    return { userId, success: false, error: error.message, durationMs: Date.now() - startMs };
  }
}

/**
 * SCALE-004: Marca usuarios con inconsistencias de datos (gap > 1 día) como _stale
 * para que reconcileStalePerformance los recalcule automáticamente.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Array<{userId: string, gap: number, minAccountDate: string, maxAccountDate: string, userDate: string|null}>} inconsistentUsers
 * @returns {Promise<number>} Número de usuarios marcados como _stale
 * @see docs/architecture/SCALE-PERF-001-consolidation-sustainability-diagnosis.md §6.6
 */
async function markInconsistentUsersAsStale(db, inconsistentUsers) {
  let consistencyStaleMarked = 0;

  if (inconsistentUsers.length === 0) {
    return consistencyStaleMarked;
  }

  logWarn(`⚠️ SCALE-004: Detectados ${inconsistentUsers.length} usuarios con datos inconsistentes`);

  for (const user of inconsistentUsers) {
    logWarn(`   Usuario ${user.userId}: overall=${user.userDate}, min=${user.minAccountDate}, max=${user.maxAccountDate}, gap=${user.gap} días`);

    if (user.gap <= 1) {
      continue;
    }

    try {
      const perfDoc = await db.collection("portfolioPerformance").doc(user.userId).get();
      const existingStale = perfDoc.data()?._stale;

      if (existingStale) {
        logInfo(`   ↳ ${user.userId}: Ya tiene _stale (since=${existingStale.since}), omitiendo re-marca`);
      } else {
        await db.collection("portfolioPerformance").doc(user.userId).set({
          _stale: {
            since: user.minAccountDate,
            reason: "auto-detected-inconsistency",
            source: "consistency-monitor",
            retryCount: 0,
            lastAttempt: new Date().toISOString()
          }
        }, { merge: true });
        consistencyStaleMarked++;
        logWarn(`   ↳ ${user.userId}: Marcado como _stale (since=${user.minAccountDate}) para reconciliación automática`);
      }
    } catch (staleError) {
      logError(`   ↳ Error marcando ${user.userId} como stale`, staleError);
    }
  }

  return consistencyStaleMarked;
}

/**
 * OPT-DEMAND-CLEANUP: Calcula el rendimiento diario del portafolio.
 * 
 * Modificada para recibir precios y currencies como parámetros
 * en lugar de leer de Firestore.
 * 
 * @param {FirebaseFirestore.Firestore} db - Instancia de Firestore
 * @param {Array} currentPrices - Precios actuales del API Lambda
 * @param {Array} currencies - Tasas de cambio del API Lambda
 */
async function calculateDailyPortfolioPerformance(db, currentPrices, currencies) {
  logInfo('🔄 Calculando rendimiento diario del portafolio (API Lambda)...');
  
  // OPT-DEMAND-400-FIX: Usar fecha del DÍA ANTERIOR para el cálculo
  // Esta función se ejecuta a las 00:05 ET del día siguiente,
  // por lo que los precios corresponden al día de trading anterior
  const now = DateTime.now().setZone('America/New_York');
  const yesterday = now.minus({ days: 1 });
  const formattedDate = yesterday.toISODate();
  
  // FIX-TIMESTAMP-002: Rango de fechas para soportar campo date con timestamp completo
  const dateRangeStart = `${formattedDate}T00:00:00.000Z`;
  const dateRangeEnd = `${formattedDate}T23:59:59.999Z`;
  
  logDebug(`📅 Fecha de cálculo (día anterior): ${formattedDate}`);
  logDebug(`📅 Hora actual de ejecución (NY): ${now.toISO()}`);
  
  // OPT-DEMAND-CLEANUP: Solo consultar datos que NO vienen del API
  // FIX-TIMESTAMP-002: Usar rango de fechas para soportar timestamps completos
  const [
    transactionsSnapshot,
    activeAssetsSnapshot,
    portfolioAccountsSnapshot
  ] = await Promise.all([
    db.collection('transactions')
      .where('date', '>=', dateRangeStart)
      .where('date', '<=', dateRangeEnd)
      .get(),
    db.collection('assets').where('isActive', '==', true).get(),
    db.collection('portfolioAccounts').where('isActive', '==', true).get()
  ]);
  
  const todaysTransactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const sellTransactions = todaysTransactions.filter(t => t.type === 'sell');
  const assetIdsInSellTransactions = [...new Set(sellTransactions.map(t => t.assetId).filter(id => id))];
  
  // 🚀 OPTIMIZACIÓN: Log consolidado de transacciones
  logInfo(`📊 Transacciones para ${formattedDate}: ${todaysTransactions.length} total (${sellTransactions.length} ventas)`);
  logInfo(`📊 Datos de mercado: ${currentPrices.length} precios, ${currencies.length} currencies (fuente: API Lambda)`);
  
  const activeAssets = activeAssetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  
  // Obtener activos inactivos involucrados en ventas
  let inactiveAssets = [];
  if (assetIdsInSellTransactions.length > 0) {
    const inactiveAssetsSnapshot = await db.collection('assets')
      .where('isActive', '==', false)
      .get();
    
    inactiveAssets = inactiveAssetsSnapshot.docs
      .map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(asset => assetIdsInSellTransactions.includes(asset.id));
    
    logDebug(`Obtenidos ${inactiveAssets.length} activos inactivos involucrados en ventas`);
  }
  
  const allAssets = [...activeAssets, ...inactiveAssets];
  
  // ✨ OPTIMIZACIÓN: Las transacciones de compra ya están en el caché
  
  // Agrupar transacciones de venta por cuenta
  const sellTransactionsByAccount = sellTransactions.reduce((acc, transaction) => {
    if (!acc[transaction.portfolioAccountId]) {
      acc[transaction.portfolioAccountId] = [];
    }
    acc[transaction.portfolioAccountId].push(transaction);
    return acc;
  }, {});

  // Identificar activos inactivos con ventas
  const inactiveAssetsWithSellTransactions = new Set(
    inactiveAssets.filter(asset => 
      sellTransactions.some(tx => tx.assetId === asset.id)
    ).map(asset => asset.id)
  );
  
  const assetsToInclude = [
    ...activeAssets,
    ...inactiveAssets.filter(asset => inactiveAssetsWithSellTransactions.has(asset.id))
  ];

  const userPortfolios = portfolioAccounts.reduce((acc, account) => {
    if (!acc[account.userId]) acc[account.userId] = [];
    acc[account.userId].push(account);
    return acc;
  }, {});

  // ✨ OPTIMIZACIÓN: Sistema de caché para datos históricos
  const cache = new PerformanceDataCache();
  await cache.preloadHistoricalData(db, formattedDate, userPortfolios, sellTransactions);

  // OPT-DEMAND-500: Validar consistencia de datos antes de calcular
  // Esto previene corrupciones cuando overall y cuentas tienen fechas diferentes
  const inconsistentUsers = [];
  for (const [userId, accounts] of Object.entries(userPortfolios)) {
    const accountIds = accounts.map(acc => acc.id);
    const consistency = cache.validateDataConsistency(userId, accountIds);
    
    if (!consistency.isConsistent) {
      inconsistentUsers.push({
        userId,
        ...consistency
      });
    }
  }
  
  // SCALE-004: Auto-reparación de inconsistencias detectadas
  const consistencyStaleMarked = await markInconsistentUsersAsStale(db, inconsistentUsers);

  const perfStartMs = Date.now();

  // 🚀 SCALE-001: Log consolidado de procesamiento
  const userCount = Object.keys(userPortfolios).length;
  const totalAccounts = Object.values(userPortfolios).flat().length;
  logInfo(`👥 Procesando ${userCount} usuarios con ${totalAccounts} cuentas activas (paralelo, max ${MAX_PARALLEL_USERS})`);

  // SCALE-001: Procesar usuarios en paralelo controlado
  const limit = pLimit(MAX_PARALLEL_USERS);
  const results = await Promise.allSettled(
    Object.entries(userPortfolios).map(([userId, accounts]) =>
      limit(() => processUserPerformance({
        db, userId, accounts, currentPrices, currencies,
        cache, allAssets, assetsToInclude, sellTransactions,
        sellTransactionsByAccount, inactiveAssets, activeAssets,
        todaysTransactions, formattedDate
      }))
    )
  );

  // SCALE-001: Agregar resultados
  const successResults = results.filter(r => r.status === "fulfilled" && r.value.success);
  const failedResults = results.filter(r => r.status === "rejected" || (r.status === "fulfilled" && !r.value.success));
  const successUserIds = successResults.map(r => r.value.userId);
  const failedUserIds = failedResults.map(r => {
    if (r.status === "rejected") {
      logWarn("SCALE-001: Promise rejected inesperadamente", r.reason);
      return null;
    }
    return r.value.userId;
  }).filter(Boolean);

  // SCALE-001: Marcar usuarios fallidos como _stale para reconciliación automática
  if (failedUserIds.length > 0) {
    logWarn(`⚠️ SCALE-001: ${failedUserIds.length} usuarios fallaron, marcando como _stale`);
    for (const failedUserId of failedUserIds) {
      try {
        await db.collection("portfolioPerformance").doc(failedUserId).set({
          _stale: {
            since: formattedDate,
            reason: "eod-parallel-processing-failure",
            source: "unifiedMarketDataUpdate",
            retryCount: 0,
            lastAttempt: new Date().toISOString()
          }
        }, { merge: true });
      } catch (staleError) {
        logError(`Error marcando usuario ${failedUserId} como stale`, staleError);
      }
    }
  }

  const totalDurationMs = Date.now() - perfStartMs;
  logInfo(`✅ Rendimiento calculado para ${successUserIds.length} usuarios (${failedUserIds.length} fallidos) en ${totalDurationMs}ms`);
  return {
    count: successUserIds.length,
    userIds: successUserIds,
    failedCount: failedUserIds.length,
    failedUserIds,
    consistencyStaleMarked,
    totalDurationMs
  };
}

/**
 * End-of-Day Portfolio Update
 * 
 * OPT-DEMAND-CLEANUP: Función consolidada que ejecuta 1x/día al cierre del mercado.
 * 
 * Schedule: 00:05 ET del día siguiente (Ma-Sa para cubrir L-V)
 * 
 * Flujo:
 * 1. Obtener símbolos únicos de assets activos
 * 2. Consultar precios del API Lambda (precios de cierre del día anterior)
 * 3. Consultar currencies del API Lambda
 * 4. Calcular performance del portafolio (EOD del día anterior)
 * 5. Calcular riesgo del portafolio
 * 6. Invalidar cache de performance
 * 
 * NOTA: Se ejecuta después de medianoche para garantizar precios de cierre definitivos.
 * La fecha de cálculo es el DÍA ANTERIOR (el día de trading que cerró).
 * 
 * @see docs/architecture/OPT-DEMAND-CLEANUP-phase4-closure-subplan.md
 */
exports.unifiedMarketDataUpdate = onSchedule({
  // OPT-DEMAND-400-FIX: Ejecutar a las 00:05 ET del día siguiente para precios de cierre definitivos
  // Martes-Sábado para cubrir trading days Lunes-Viernes
  schedule: '5 0 * * 2-6',  // 00:05 Ma-Sa (guarda datos de L-V)
  timeZone: 'America/New_York',
  memory: '512MiB',
  timeoutSeconds: 540,  // 9 minutos
  retryCount: 2,
  secrets: [cfServiceToken],  // SEC-TOKEN-001: Binding del secret para API auth
  labels: {
    status: 'active',
    purpose: 'eod-portfolio-calculations',
    updated: '2026-01-28'
  }
}, async (event) => {
  // Inicializar logger estructurado (SCALE-CORE-002)
  logger = StructuredLogger.forScheduled('endOfDayPortfolioUpdate');
  
  const db = admin.firestore();
  const startTime = Date.now();
  const now = DateTime.now().setZone('America/New_York');
  const yesterday = now.minus({ days: 1 });
  
  logger.info('🚀 Starting End-of-Day Portfolio Update', {
    trigger: 'scheduled',
    currentTime: now.toISO(),
    targetDate: yesterday.toISODate()
  });

  const mainOp = logger.startOperation('eodPortfolioUpdate');
  
  try {
    // =========================================================================
    // OPT-DEMAND-400-FIX: Verificar si el día anterior fue un día de trading válido
    // Solo guardamos en portfolioPerformance si NO fue fin de semana y NO fue festivo
    // =========================================================================
    const tradingDayCheck = await isValidTradingDay(db, yesterday);
    
    if (!tradingDayCheck.isValid) {
      logger.info('⏭️ Skipping EOD update - not a valid trading day', {
        date: tradingDayCheck.formattedDate,
        reason: tradingDayCheck.reason,
        holiday: tradingDayCheck.holiday || null
      });
      
      mainOp.success({
        skipped: true,
        reason: tradingDayCheck.reason,
        date: tradingDayCheck.formattedDate
      });
      
      return null;
    }
    
    logger.info('✅ Valid trading day confirmed', {
      date: tradingDayCheck.formattedDate,
      reason: tradingDayCheck.reason
    });
    
    // Paso 1: Obtener símbolos únicos de assets activos
    const assetsOp = logger.startOperation('fetchAssetSymbols');
    const assetsSnapshot = await db.collection('assets').where('isActive', '==', true).get();
    const symbols = [...new Set(assetsSnapshot.docs.map(d => d.data().name).filter(Boolean))];
    assetsOp.success({ assetCount: assetsSnapshot.size, uniqueSymbols: symbols.length });
    
    logger.info('📊 Assets fetched', { assets: assetsSnapshot.size, symbols: symbols.length });
    
    // Paso 2: Obtener precios y currencies del API Lambda
    const marketDataOp = logger.startOperation('fetchMarketData');
    const [currentPrices, currencies] = await Promise.all([
      getPricesFromApi(symbols),
      getCurrencyRatesFromApi()
    ]);
    marketDataOp.success({ pricesReceived: currentPrices.length, currenciesReceived: currencies.length });
    
    logger.info('💹 Market data fetched from API Lambda', {
      prices: currentPrices.length,
      currencies: currencies.length,
      source: 'api-lambda'
    });
    
    // SCALE-005: Persist historical exchange rates for cache
    try {
      const rateDate = yesterday.toISODate();
      const ratesMap = { USD: 1 };
      currencies.forEach(c => {
        if (c.code && c.code !== 'USD' && c.exchangeRate) {
          ratesMap[c.code] = normalizeToUsdBase(c.code, c.exchangeRate);
        }
      });
      const rateDocRef = db.doc(`historicalExchangeRates/${rateDate}`);
      const existingRateDoc = await rateDocRef.get();
      if (!existingRateDoc.exists) {
        await rateDocRef.set({
          date: rateDate,
          rates: ratesMap,
          _meta: {
            source: 'eod-pipeline',
            fetchedAt: admin.firestore.FieldValue.serverTimestamp(),
            currencyCount: Object.keys(ratesMap).length,
            version: 1
          }
        });
        logger.info('SCALE-005: Historical rates persisted', { date: rateDate, currencies: Object.keys(ratesMap).length });
      }
    } catch (ratesCacheError) {
      logger.warn('SCALE-005: Could not persist historical rates', { error: ratesCacheError.message });
    }
    
    // Paso 3: Calcular performance del portafolio
    const perfOp = logger.startOperation('calculateDailyPortfolioPerformance');
    const portfolioResult = await calculateDailyPortfolioPerformance(db, currentPrices, currencies);
    perfOp.success({ portfoliosCalculated: portfolioResult.count, failed: portfolioResult.failedCount || 0 });
    
    logger.info('📈 Portfolio performance calculated', {
      users: portfolioResult.count,
      failed: portfolioResult.failedCount || 0,
      userIds: portfolioResult.userIds?.length || 0
    });
    
    // Paso 4: Calcular riesgo del portafolio
    const riskOp = logger.startOperation('calculatePortfolioRisk');
    await calculatePortfolioRisk();
    riskOp.success();
    
    logger.info('⚠️ Portfolio risk calculated');
    
    // Paso 5: Invalidar cache de performance
    let cacheInvalidationResult = { usersProcessed: 0, cachesDeleted: 0 };
    if (portfolioResult.userIds && portfolioResult.userIds.length > 0) {
      try {
        const cacheOp = logger.startOperation('invalidatePerformanceCacheBatch');
        cacheInvalidationResult = await invalidatePerformanceCacheBatch(portfolioResult.userIds);
        cacheOp.success({ usersProcessed: cacheInvalidationResult.usersProcessed, cachesDeleted: cacheInvalidationResult.cachesDeleted });
      } catch (cacheError) {
        logger.warn('Cache invalidation failed (non-critical)', { error: cacheError.message });
      }
    }
    
    // VS-009 Paso 5.5: Generar benchmarkSnapshots (best-effort)
    try {
      const benchmarkOp = logger.startOperation('generateBenchmarkSnapshots');
      const benchmarkResult = await updateBenchmarkSnapshots(db, currentPrices, currencies, yesterday, logger);
      benchmarkOp.success({ written: benchmarkResult.success, failed: benchmarkResult.failed, skipped: benchmarkResult.skipped });
    } catch (benchmarkError) {
      logger.warn('[VS-009] benchmarkSnapshots generation failed (non-critical)', { error: benchmarkError.message });
    }

    const executionTime = (Date.now() - startTime) / 1000;
    
    // Paso 6: Actualizar systemStatus
    try {
      await db.collection('systemStatus').doc('marketData').set({
        lastCompleteUpdate: admin.firestore.FieldValue.serverTimestamp(),
        lastUpdateDate: DateTime.now().toISO(),
        source: 'api-lambda',
        performanceCalculated: portfolioResult.count,
        failedUsers: portfolioResult.failedCount || 0,
        cachesInvalidated: cacheInvalidationResult.cachesDeleted,
        executionTimeMs: Math.round(executionTime * 1000),
        marketOpen: false
      }, { merge: true });
    } catch (signalError) {
      logger.warn('SystemStatus update failed (non-critical)', { error: signalError.message });
    }
    
    mainOp.success({
      portfoliosCalculated: portfolioResult.count,
      cachesInvalidated: cacheInvalidationResult.cachesDeleted,
      executionTimeSec: executionTime,
      source: 'api-lambda'
    });
    
    logger.info('✅ End-of-Day Portfolio Update completed', {
      executionTime: `${executionTime.toFixed(2)}s`,
      portfolios: portfolioResult.count
    });
    
    return null;
    
  } catch (error) {
    mainOp.failure(error);
    logger.error('❌ End-of-Day Portfolio Update failed', error);
    throw error;
  }
});

// SCALE-001: Exportar para testing
if (process.env.NODE_ENV === "test" || process.env.FUNCTIONS_EMULATOR) {
  exports._testExports = {
    processUserPerformance,
    calculateDailyPortfolioPerformance,
    PerformanceDataCache,
    MAX_PARALLEL_USERS,
    markInconsistentUsersAsStale,
    generateAllSnapshots,
    generateAllAssetSnapshots,
    fetchLatestAssetPerformance,
  };
}