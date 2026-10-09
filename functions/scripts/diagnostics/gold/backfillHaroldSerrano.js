/**
 * Script combinado: Migración de Asset Keys + Backfill para Harold Serrano
 * 
 * USUARIO: harold.serrano.dev@gmail.com
 * USER ID: GlWtKSe53tMfcqNeFHeGf9OhowN2
 * 
 * PROBLEMA:
 * Hasta 2025-06-05 los assets se guardaron sin sufijo de tipo (ej: "VOO")
 * A partir de 2025-06-06 se guardaron correctamente (ej: "VOO_etf")
 * 
 * PLAN DE EJECUCIÓN:
 *   FASE 1: Migrar asset keys en documentos anteriores a 2025-06-06
 *   FASE 2: Backfill completo de performance 2025-01-01 → 2026-03-30
 * 
 * USO:
 *   node backfillHaroldSerrano.js --analyze         # Solo análisis
 *   node backfillHaroldSerrano.js --dry-run         # Ver cambios sin aplicar
 *   node backfillHaroldSerrano.js --fix             # Aplicar TODO
 *   node backfillHaroldSerrano.js --fix-keys-only   # Solo migrar asset keys
 *   node backfillHaroldSerrano.js --fix-backfill-only # Solo backfill (asume keys ya migrados)
 * 
 * OPCIONES:
 *   --no-consolidate    # Omitir re-consolidación de períodos
 */

const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

// Inicializar Firebase Admin
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// Utilidades de consolidación de períodos (COST-OPT-001)
const { consolidatePeriod, CONSOLIDATED_SCHEMA_VERSION, NON_CURRENCY_FIELDS } = require('../../../utils/periodConsolidation');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const CONFIG = {
  // Usuario objetivo
  USER_ID: 'GlWtKSe53tMfcqNeFHeGf9OhowN2',
  
  // Rango de backfill
  START_DATE: '2025-01-01',
  END_DATE: '2026-03-30',
  
  // Fecha límite para migración de asset keys (antes de esta fecha, keys están mal)
  KEY_MIGRATION_CUTOFF: '2025-06-06',
  
  // API de precios históricos
  HISTORICAL_API_BASE: 'https://api.portastock.net/v1',
  
  // Headers de autenticación para API
  API_HEADERS: {
    'x-service-token': '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10',
    'origin': 'https://portastock.net',
    'referer': 'https://portastock.net'
  },
  
  // Monedas activas
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  
  // Días festivos NYSE 2024
  NYSE_HOLIDAYS_2024: [
    '2024-01-01', '2024-01-15', '2024-02-19', '2024-03-29',
    '2024-05-27', '2024-06-19', '2024-07-04', '2024-09-02',
    '2024-11-28', '2024-12-25'
  ],
  
  // Días festivos NYSE 2025
  NYSE_HOLIDAYS_2025: [
    '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18',
    '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01',
    '2025-11-27', '2025-12-25'
  ],
  
  // Días festivos NYSE 2026
  NYSE_HOLIDAYS_2026: [
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
    '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
    '2026-11-26', '2026-12-25',
  ],
  
  // Rate limiting
  API_DELAY_MS: 200,
  BATCH_SIZE: 20,
};

// ============================================================================
// UTILIDADES
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'analyze', // analyze, dry-run, fix, fix-keys-only, fix-backfill-only
    noConsolidate: false,
  };

  args.forEach(arg => {
    if (arg === '--dry-run') options.mode = 'dry-run';
    else if (arg === '--fix') options.mode = 'fix';
    else if (arg === '--analyze') options.mode = 'analyze';
    else if (arg === '--fix-keys-only') options.mode = 'fix-keys-only';
    else if (arg === '--fix-backfill-only') options.mode = 'fix-backfill-only';
    else if (arg === '--no-consolidate') options.noConsolidate = true;
  });

  return options;
}

function getDatePart(dateString) {
  if (!dateString) return '';
  if (dateString.includes('T')) return dateString.substring(0, 10);
  return dateString;
}

function isDateOnOrBefore(txDate, targetDate) {
  return getDatePart(txDate) <= targetDate;
}

function isDateEqual(txDate, targetDate) {
  return getDatePart(txDate) === targetDate;
}

function isDateAfter(txDate, afterDate) {
  return getDatePart(txDate) > afterDate;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function log(level, message, data = null) {
  const timestamp = new Date().toISOString();
  const prefix = {
    'INFO': '📋',
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'DEBUG': '🔍',
    'PROGRESS': '🔄',
  }[level] || '•';
  
  console.log(`${prefix} [${timestamp}] ${message}`);
  if (data) console.log('   ', JSON.stringify(data, null, 2));
}

function generateBusinessDays(startDate, endDate) {
  const days = [];
  let current = new Date(startDate + 'T12:00:00Z');
  const end = new Date(endDate + 'T12:00:00Z');
  
  const allHolidays = [
    ...CONFIG.NYSE_HOLIDAYS_2024,
    ...CONFIG.NYSE_HOLIDAYS_2025,
    ...CONFIG.NYSE_HOLIDAYS_2026
  ];

  while (current <= end) {
    const dayOfWeek = current.getUTCDay();
    const dateStr = current.toISOString().split('T')[0];
    
    if (dayOfWeek !== 0 && dayOfWeek !== 6 && !allHolidays.includes(dateStr)) {
      days.push(dateStr);
    }
    current.setUTCDate(current.getUTCDate() + 1);
  }
  
  return days;
}

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

async function fetchHistoricalPrices(symbol, startDate = null) {
  try {
    let range = 'ytd';
    if (startDate) {
      const start = new Date(startDate);
      const now = new Date();
      const monthsAgo = (now.getFullYear() - start.getFullYear()) * 12 + (now.getMonth() - start.getMonth());
      
      if (monthsAgo > 12) {
        range = '2y';
      } else if (monthsAgo > 6) {
        range = '1y';
      }
    }
    
    const url = `${CONFIG.HISTORICAL_API_BASE}/historical?symbol=${encodeURIComponent(symbol)}&range=${range}&interval=1d`;
    const response = await fetch(url, {
      headers: CONFIG.API_HEADERS
    });
    
    if (!response.ok) {
      log('WARNING', `No se pudieron obtener precios para ${symbol}: ${response.status}`);
      return {};
    }
    
    const data = await response.json();
    
    const priceMap = {};
    Object.entries(data).forEach(([date, ohlcv]) => {
      priceMap[date] = ohlcv.close;
    });
    
    return priceMap;
  } catch (error) {
    log('ERROR', `Error obteniendo precios históricos para ${symbol}`, { error: error.message });
    return {};
  }
}

async function fetchHistoricalExchangeRate(currency, date) {
  if (currency === 'USD') return 1;
  
  try {
    let symbol;
    if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
      symbol = `${currency}USD`;
    } else {
      symbol = `USD${currency}`;
    }
    
    const timestamp = Math.floor(date.getTime() / 1000);
    const nextDay = timestamp + 86400;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}%3DX?period1=${timestamp}&period2=${nextDay}&interval=1d`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    if (data.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.[0]) {
      let rate = data.chart.result[0].indicators.quote[0].close[0];
      
      if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
        rate = 1 / rate;
      }
      
      return rate;
    }
    
    return null;
  } catch (error) {
    log('WARNING', `Error obteniendo tipo de cambio para ${currency}`, { error: error.message });
    return null;
  }
}

async function getTransactions(accountId) {
  const snapshot = await db.collection('transactions')
    .where('portfolioAccountId', '==', accountId)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function getExistingPerformance(userId, accountId, startDate, endDate) {
  const perfPath = accountId 
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates`
    : `portfolioPerformance/${userId}/dates`;
  
  const snapshot = await db.collection(perfPath)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  const existing = new Map();
  snapshot.docs.forEach(doc => {
    existing.set(doc.data().date, doc.data());
  });
  
  return existing;
}

async function getExistingPerformanceDocs(userId, accountId, startDate, endDate) {
  const collectionPath = accountId
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates`
    : `portfolioPerformance/${userId}/dates`;

  const snapshot = await db.collection(collectionPath)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();

  return snapshot.docs.map(doc => ({
    path: doc.ref.path,
    data: doc.data()
  }));
}

async function getLastPerformanceBefore(userId, accountId, beforeDate) {
  const perfPath = accountId 
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates`
    : `portfolioPerformance/${userId}/dates`;
  
  const snapshot = await db.collection(perfPath)
    .where('date', '<', beforeDate)
    .orderBy('date', 'desc')
    .limit(1)
    .get();
  
  if (snapshot.empty) return null;
  return snapshot.docs[0].data();
}

async function getUserAccounts(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

// ============================================================================
// FASE 1: MIGRACIÓN DE ASSET KEYS
// ============================================================================

/**
 * Construye dinámicamente el mapa de tipos de asset desde las transacciones del usuario.
 * Cada transacción tiene assetName y assetType, lo cual nos da el mapping correcto.
 */
async function buildAssetTypeMap(userId) {
  const accounts = await getUserAccounts(userId);
  const assetTypeMap = {};
  
  for (const account of accounts) {
    const transactions = await getTransactions(account.id);
    transactions.forEach(tx => {
      if (tx.assetName && tx.assetType) {
        assetTypeMap[tx.assetName] = tx.assetType;
      }
    });
  }
  
  return assetTypeMap;
}

/**
 * Migra los asset keys en documentos de performance anteriores a KEY_MIGRATION_CUTOFF.
 * Cambia keys como "VOO" a "VOO_etf" usando el mapa construido desde transacciones.
 */
async function migrateAssetKeys(userId, assetTypeMap, mode) {
  log('PROGRESS', '═══ FASE 1: Migración de Asset Performance Keys ═══');
  log('INFO', `Asset Type Map construido:`, assetTypeMap);
  
  const accounts = await getUserAccounts(userId);
  const accountIds = accounts.map(a => a.id);
  
  const results = { migrated: 0, skipped: 0, errors: 0, unknownKeys: [] };
  
  // Migrar OVERALL
  log('PROGRESS', 'Migrando nivel OVERALL...');
  const overallResult = await migrateCollection(
    `portfolioPerformance/${userId}/dates`,
    assetTypeMap,
    mode
  );
  results.migrated += overallResult.migrated;
  results.skipped += overallResult.skipped;
  results.errors += overallResult.errors;
  results.unknownKeys.push(...overallResult.unknownKeys);
  
  // Migrar cada cuenta
  for (const account of accounts) {
    log('PROGRESS', `Migrando cuenta: ${account.name || account.id}...`);
    const accountResult = await migrateCollection(
      `portfolioPerformance/${userId}/accounts/${account.id}/dates`,
      assetTypeMap,
      mode
    );
    results.migrated += accountResult.migrated;
    results.skipped += accountResult.skipped;
    results.errors += accountResult.errors;
    results.unknownKeys.push(...accountResult.unknownKeys);
  }
  
  log('SUCCESS', `Migración completada: ${results.migrated} docs migrados, ${results.skipped} ya correctos, ${results.errors} errores`);
  if (results.unknownKeys.length > 0) {
    const uniqueUnknown = [...new Set(results.unknownKeys)];
    log('WARNING', `Keys sin tipo encontrados (no se migraron): ${uniqueUnknown.join(', ')}`);
  }
  
  return results;
}

async function migrateCollection(collectionPath, assetTypeMap, mode) {
  const snapshot = await db.collection(collectionPath)
    .where('date', '<', CONFIG.KEY_MIGRATION_CUTOFF)
    .orderBy('date', 'asc')
    .get();
  
  log('INFO', `  Documentos anteriores a ${CONFIG.KEY_MIGRATION_CUTOFF}: ${snapshot.docs.length}`);
  
  const result = { migrated: 0, skipped: 0, errors: 0, unknownKeys: [] };
  
  for (const doc of snapshot.docs) {
    const data = doc.data();
    let needsMigration = false;
    const updates = {};

    for (const currency of CONFIG.CURRENCIES) {
      const currencyData = data[currency];
      if (!currencyData || !currencyData.assetPerformance) continue;

      const assetPerf = currencyData.assetPerformance;
      const newAssetPerf = {};
      let currencyNeedsMigration = false;

      for (const [key, value] of Object.entries(assetPerf)) {
        if (key.includes('_')) {
          // Ya tiene formato correcto
          newAssetPerf[key] = value;
        } else {
          // Necesita migración
          const assetType = assetTypeMap[key];
          if (assetType) {
            const newKey = `${key}_${assetType}`;
            newAssetPerf[newKey] = value;
            currencyNeedsMigration = true;
            
            if (mode === 'dry-run' && result.migrated < 5) {
              log('DEBUG', `   ${data.date}: ${key} → ${newKey}`);
            }
          } else {
            log('WARNING', `   ${data.date}: No se encontró tipo para "${key}", manteniendo original`);
            newAssetPerf[key] = value;
            result.unknownKeys.push(key);
          }
        }
      }

      if (currencyNeedsMigration) {
        updates[`${currency}.assetPerformance`] = newAssetPerf;
        needsMigration = true;
      }
    }

    if (needsMigration) {
      result.migrated++;
      
      if (mode === 'fix' || mode === 'fix-keys-only') {
        try {
          await doc.ref.update(updates);
          if (result.migrated % 20 === 0) {
            log('PROGRESS', `   Migrados ${result.migrated} documentos...`);
          }
        } catch (err) {
          log('ERROR', `   Error en ${data.date}: ${err.message}`);
          result.errors++;
        }
      }
    } else {
      result.skipped++;
    }
  }

  log('INFO', `  Resultado: ${result.migrated} migrados, ${result.skipped} ya correctos`);
  return result;
}

// ============================================================================
// CÁLCULO DE HOLDINGS
// ============================================================================

function calculateHoldingsAtDate(transactions, targetDate, exchangeRates = {}) {
  const holdings = new Map();
  let totalInvestmentUSD = 0;
  let totalCashFlowUSD = 0;
  
  const relevantTx = transactions.filter(tx => isDateOnOrBefore(tx.date, targetDate));
  
  const typeOrder = { 'buy': 0, 'cash_income': 1, 'dividendPay': 2, 'sell': 3, 'cash_outcome': 4 };
  relevantTx.sort((a, b) => {
    const dateA = getDatePart(a.date);
    const dateB = getDatePart(b.date);
    if (dateA !== dateB) return dateA.localeCompare(dateB);
    const orderA = typeOrder[a.type] ?? 99;
    const orderB = typeOrder[b.type] ?? 99;
    return orderA - orderB;
  });
  
  relevantTx.forEach(tx => {
    const assetKey = tx.assetName ? `${tx.assetName}_${tx.assetType || 'stock'}` : null;
    
    const txCurrency = tx.currency || 'USD';
    let conversionRate = 1;
    if (txCurrency !== 'USD' && exchangeRates[txCurrency]) {
      conversionRate = 1 / exchangeRates[txCurrency];
    }
    
    switch (tx.type) {
      case 'buy':
        if (assetKey) {
          const current = holdings.get(assetKey) || { 
            units: 0, 
            totalInvestment: 0, 
            assetType: tx.assetType || 'stock',
            symbol: tx.assetName,
            currency: txCurrency
          };
          current.units += tx.amount || 0;
          const investmentInTxCurrency = (tx.amount || 0) * (tx.price || 0);
          const investmentInUSD = investmentInTxCurrency * conversionRate;
          current.totalInvestment += investmentInUSD;
          if (!current.currency) {
            current.currency = txCurrency;
          }
          holdings.set(assetKey, current);
          totalInvestmentUSD += investmentInUSD;
        }
        break;
        
      case 'sell':
        if (assetKey) {
          const current = holdings.get(assetKey);
          if (current) {
            const soldUnits = tx.amount || 0;
            const avgCost = current.units > 0 ? current.totalInvestment / current.units : 0;
            current.units -= soldUnits;
            current.totalInvestment -= soldUnits * avgCost;
            
            if (current.units <= 0.0001) {
              holdings.delete(assetKey);
            } else {
              holdings.set(assetKey, current);
            }
            totalInvestmentUSD -= soldUnits * avgCost;
          }
        }
        break;
        
      case 'cash_income':
        totalCashFlowUSD += (tx.amount || 0) * conversionRate;
        break;
        
      case 'cash_outcome':
        totalCashFlowUSD -= (tx.amount || 0) * conversionRate;
        break;
        
      case 'dividendPay':
        break;
    }
  });
  
  return {
    holdings,
    totalInvestmentUSD: Math.max(0, totalInvestmentUSD),
    dailyCashFlow: calculateDailyCashFlow(transactions, targetDate)
  };
}

function calculateDailyDonePnL(transactions, targetDate) {
  const byAsset = new Map();
  let total = 0;
  
  transactions
    .filter(tx => isDateEqual(tx.date, targetDate) && tx.type === 'sell')
    .forEach(tx => {
      const pnl = tx.valuePnL || 0;
      total += pnl;
      const assetKey = `${tx.assetName}_${tx.assetType}`;
      byAsset.set(assetKey, (byAsset.get(assetKey) || 0) + pnl);
    });
  
  return { total, byAsset };
}

function calculateDailyCashFlow(transactions, targetDate) {
  return transactions
    .filter(tx => isDateEqual(tx.date, targetDate))
    .reduce((sum, tx) => {
      const amountInOriginalCurrency = (tx.amount || 0) * (tx.price || 0);
      
      let amountInUSD = amountInOriginalCurrency;
      if (tx.currency && tx.currency !== 'USD' && tx.dollarPriceToDate) {
        amountInUSD = amountInOriginalCurrency / parseFloat(tx.dollarPriceToDate);
      }
      
      if (tx.type === 'buy') return sum - amountInUSD;
      if (tx.type === 'sell') return sum + amountInUSD;
      return sum;
    }, 0);
}

function calculateAccumulatedCashFlow(transactions, startDateExclusive, endDateInclusive) {
  return transactions
    .filter(tx => isDateAfter(tx.date, startDateExclusive) && isDateOnOrBefore(tx.date, endDateInclusive))
    .reduce((sum, tx) => {
      const amountInOriginalCurrency = (tx.amount || 0) * (tx.price || 0);
      
      let amountInUSD = amountInOriginalCurrency;
      if (tx.currency && tx.currency !== 'USD' && tx.dollarPriceToDate) {
        amountInUSD = amountInOriginalCurrency / parseFloat(tx.dollarPriceToDate);
      }
      
      if (tx.type === 'buy') return sum - amountInUSD;
      if (tx.type === 'sell') return sum + amountInUSD;
      return sum;
    }, 0);
}

// ============================================================================
// CÁLCULO DE PERFORMANCE
// ============================================================================

function dayHasMarketPrices(pricesBySymbol, targetDate) {
  for (const [symbol, prices] of pricesBySymbol) {
    if (prices && prices[targetDate] !== undefined) {
      return true;
    }
  }
  return false;
}

function getPriceForDate(symbolPrices, targetDate) {
  if (!symbolPrices || Object.keys(symbolPrices).length === 0) return 0;
  
  if (symbolPrices[targetDate]) {
    return symbolPrices[targetDate];
  }
  
  const sortedDates = Object.keys(symbolPrices).sort().reverse();
  for (const date of sortedDates) {
    if (date < targetDate) {
      return symbolPrices[date];
    }
  }
  
  return symbolPrices[sortedDates[sortedDates.length - 1]] || 0;
}

function calculateDayPerformance(
  holdings,
  pricesBySymbol,
  targetDate,
  exchangeRates,
  totalInvestmentUSD,
  dailyCashFlowUSD,
  previousDayPerformance,
  previousDayHoldings,
  dailyDonePnL = { total: 0, byAsset: new Map() }
) {
  const result = {};
  
  CONFIG.CURRENCIES.forEach(currency => {
    const exchangeRate = exchangeRates[currency] || 1;
    
    let totalValue = 0;
    const assetPerformance = {};
    
    const dailyDonePnLForCurrency = dailyDonePnL.total * exchangeRate;
    
    holdings.forEach((holding, assetKey) => {
      const symbolPrices = pricesBySymbol.get(holding.symbol);
      const priceInAssetCurrency = getPriceForDate(symbolPrices, targetDate);
      
      let priceInUSD = priceInAssetCurrency;
      const assetCurrency = holding.currency || 'USD';
      if (assetCurrency !== 'USD' && exchangeRates[assetCurrency]) {
        priceInUSD = priceInAssetCurrency / exchangeRates[assetCurrency];
      }
      
      const valueUSD = holding.units * priceInUSD;
      const value = valueUSD * exchangeRate;
      const investment = holding.totalInvestment * exchangeRate;
      
      const assetDonePnL = (dailyDonePnL.byAsset.get(assetKey) || 0) * exchangeRate;
      
      totalValue += value;
      
      assetPerformance[assetKey] = {
        units: holding.units,
        totalValue: value,
        totalInvestment: investment,
        totalCashFlow: 0,
        unrealizedProfitAndLoss: value - investment,
        doneProfitAndLoss: assetDonePnL,
        totalROI: investment > 0 ? ((value - investment) / investment) * 100 : 0,
        dailyChangePercentage: 0,
        rawDailyChangePercentage: 0,
        adjustedDailyChangePercentage: 0,
        dailyReturn: 0,
        monthlyReturn: 0,
        annualReturn: 0,
      };
    });
    
    const totalInvestment = totalInvestmentUSD * exchangeRate;
    const totalCashFlow = dailyCashFlowUSD * exchangeRate;
    
    const prevPerf = previousDayPerformance?.[currency];
    const previousTotalValue = prevPerf?.totalValue || 0;
    
    const isNewInvestment = previousTotalValue === 0 && totalValue > 0;
    
    let rawDailyChangePercentage = 0;
    if (previousTotalValue > 0) {
      rawDailyChangePercentage = ((totalValue - previousTotalValue) / previousTotalValue) * 100;
    }
    
    let adjustedDailyChangePercentage = 0;
    if (isNewInvestment) {
      adjustedDailyChangePercentage = 0;
    } else if (previousTotalValue > 0) {
      adjustedDailyChangePercentage = ((totalValue - previousTotalValue + totalCashFlow) / previousTotalValue) * 100;
    }
    
    const dailyChangePercentage = rawDailyChangePercentage;
    const dailyReturn = adjustedDailyChangePercentage / 100;
    
    result[currency] = {
      totalValue,
      totalInvestment,
      totalCashFlow,
      doneProfitAndLoss: dailyDonePnLForCurrency,
      unrealizedProfitAndLoss: totalValue - totalInvestment,
      totalROI: totalInvestment > 0 ? ((totalValue - totalInvestment) / totalInvestment) * 100 : 0,
      dailyChangePercentage,
      rawDailyChangePercentage,
      adjustedDailyChangePercentage,
      dailyReturn,
      monthlyReturn: 0,
      annualReturn: 0,
      assetPerformance,
    };
    
    // Métricas por activo
    Object.entries(assetPerformance).forEach(([assetKey, perf]) => {
      const prevAssetPerf = prevPerf?.assetPerformance?.[assetKey];
      const prevAssetValue = prevAssetPerf?.totalValue || 0;
      const prevAssetUnits = prevAssetPerf?.units || 0;
      
      const unitsDiff = perf.units - prevAssetUnits;
      const hadAssetCashFlow = Math.abs(unitsDiff) > 0.0001;
      
      const isAssetNewInvestment = prevAssetValue < 0.01 && perf.totalValue > 0;
      
      if (isAssetNewInvestment) {
        perf.dailyChangePercentage = 0;
        perf.rawDailyChangePercentage = 0;
        perf.adjustedDailyChangePercentage = 0;
        perf.dailyReturn = 0;
      } else if (prevAssetValue >= 0.01) {
        perf.rawDailyChangePercentage = ((perf.totalValue - prevAssetValue) / prevAssetValue) * 100;
        perf.dailyChangePercentage = perf.rawDailyChangePercentage;
        
        if (hadAssetCashFlow) {
          const assetCashFlow = -(perf.totalInvestment - (prevAssetPerf?.totalInvestment || 0));
          perf.adjustedDailyChangePercentage = ((perf.totalValue - prevAssetValue + assetCashFlow) / prevAssetValue) * 100;
        } else {
          perf.adjustedDailyChangePercentage = perf.rawDailyChangePercentage;
        }
        
        if (Math.abs(perf.adjustedDailyChangePercentage) > 50) {
          perf.adjustedDailyChangePercentage = 0;
          perf.rawDailyChangePercentage = 0;
          perf.dailyChangePercentage = 0;
        }
        
        perf.dailyReturn = perf.adjustedDailyChangePercentage / 100;
      }
    });
  });
  
  return result;
}

// ============================================================================
// RE-CONSOLIDACIÓN DE PERÍODOS
// ============================================================================

function getAffectedMonths(startDate, endDate) {
  const months = new Set();
  const start = new Date(startDate + 'T12:00:00Z');
  const end = new Date(endDate + 'T12:00:00Z');
  
  let current = new Date(start);
  while (current <= end) {
    const y = current.getUTCFullYear();
    const m = String(current.getUTCMonth() + 1).padStart(2, '0');
    months.add(`${y}-${m}`);
    current.setUTCMonth(current.getUTCMonth() + 1);
    current.setUTCDate(1);
  }
  
  return [...months].sort();
}

function getAffectedYears(startDate, endDate) {
  const startYear = parseInt(startDate.substring(0, 4));
  const endYear = parseInt(endDate.substring(0, 4));
  const years = [];
  for (let y = startYear; y <= endYear; y++) {
    years.push(y.toString());
  }
  return years;
}

async function reconsolidateMonth(userId, accountId, periodKey) {
  const [year, month] = periodKey.split('-').map(Number);
  const periodStart = new Date(Date.UTC(year, month - 1, 1)).toISOString().split('T')[0];
  const periodEnd = new Date(Date.UTC(year, month, 0)).toISOString().split('T')[0];
  
  const basePath = accountId
    ? `portfolioPerformance/${userId}/accounts/${accountId}`
    : `portfolioPerformance/${userId}`;
  
  const datesPath = `${basePath}/dates`;
  const consolidatedPath = `${basePath}/consolidatedPeriods/monthly/periods/${periodKey}`;
  
  const dailySnapshot = await db.collection(datesPath)
    .where('date', '>=', periodStart)
    .where('date', '<=', periodEnd)
    .orderBy('date', 'asc')
    .get();
  
  if (dailySnapshot.empty) return null;
  
  const consolidated = consolidatePeriod(dailySnapshot.docs, periodKey, 'month');
  if (!consolidated) return null;
  
  await db.doc(consolidatedPath).set(consolidated);
  return consolidated;
}

function consolidateMonthsToYear(monthlyDocs, yearKey) {
  if (!monthlyDocs || monthlyDocs.length === 0) return null;
  
  const firstDoc = monthlyDocs[0].data ? monthlyDocs[0].data() : monthlyDocs[0];
  const lastDoc = monthlyDocs[monthlyDocs.length - 1].data
    ? monthlyDocs[monthlyDocs.length - 1].data()
    : monthlyDocs[monthlyDocs.length - 1];
  
  const currencies = new Set();
  monthlyDocs.forEach(doc => {
    const data = doc.data ? doc.data() : doc;
    Object.keys(data).forEach(key => {
      if (!NON_CURRENCY_FIELDS.includes(key)) currencies.add(key);
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
      
      if (!foundFirst) {
        startTotalValue = currencyData.startTotalValue || 0;
        startTotalInvestment = currencyData.startTotalInvestment || 0;
        foundFirst = true;
      }
      
      endTotalValue = currencyData.endTotalValue || 0;
      endTotalInvestment = currencyData.endTotalInvestment || 0;
      
      if (currencyData.endFactor && currencyData.startFactor) {
        compoundFactor *= (currencyData.endFactor / currencyData.startFactor);
      }
      
      totalCashFlow += currencyData.totalCashFlow || 0;
    });
    
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

async function reconsolidateYear(userId, accountId, yearKey) {
  const basePath = accountId
    ? `portfolioPerformance/${userId}/accounts/${accountId}`
    : `portfolioPerformance/${userId}`;
  
  const monthlyPath = `${basePath}/consolidatedPeriods/monthly/periods`;
  const yearlyPath = `${basePath}/consolidatedPeriods/yearly/periods/${yearKey}`;
  
  const monthlySnapshot = await db.collection(monthlyPath)
    .where('periodKey', '>=', `${yearKey}-01`)
    .where('periodKey', '<=', `${yearKey}-12`)
    .orderBy('periodKey', 'asc')
    .get();
  
  if (monthlySnapshot.empty) return null;
  
  const consolidated = consolidateMonthsToYear(monthlySnapshot.docs, yearKey);
  if (!consolidated) return null;
  
  await db.doc(yearlyPath).set(consolidated);
  return consolidated;
}

// ============================================================================
// FASE 2: BACKFILL DE PERFORMANCE
// ============================================================================

async function runBackfill(userId, mode, noConsolidate) {
  log('PROGRESS', '═══ FASE 2: Backfill de Portfolio Performance ═══');
  log('INFO', `Usuario: ${userId}, Rango: ${CONFIG.START_DATE} → ${CONFIG.END_DATE}`);
  
  const isWrite = (mode === 'fix' || mode === 'fix-backfill-only');
  
  // 1. Obtener cuentas del usuario
  log('PROGRESS', 'Obteniendo cuentas del usuario...');
  const accounts = await getUserAccounts(userId);
  log('SUCCESS', `Encontradas ${accounts.length} cuentas activas`);
  
  if (accounts.length === 0) {
    log('ERROR', 'No se encontraron cuentas para procesar');
    return;
  }
  
  // 2. Generar días hábiles esperados
  const expectedDays = generateBusinessDays(CONFIG.START_DATE, CONFIG.END_DATE);
  log('INFO', `Días hábiles en el período: ${expectedDays.length}`);
  
  // 3. Recopilar símbolos únicos
  log('PROGRESS', 'Analizando transacciones para identificar símbolos...');
  const allSymbols = new Set();
  const transactionsByAccount = new Map();
  
  for (const account of accounts) {
    const transactions = await getTransactions(account.id);
    transactionsByAccount.set(account.id, transactions);
    
    transactions.forEach(tx => {
      if (tx.assetName) {
        allSymbols.add(tx.assetName);
      }
    });
  }
  
  log('SUCCESS', `Símbolos únicos identificados: ${allSymbols.size}`, [...allSymbols]);
  
  // 4. Obtener precios históricos
  log('PROGRESS', 'Obteniendo precios históricos...');
  const pricesBySymbol = new Map();
  
  for (const symbol of allSymbols) {
    log('DEBUG', `  Obteniendo precios para ${symbol}...`);
    const prices = await fetchHistoricalPrices(symbol, CONFIG.START_DATE);
    pricesBySymbol.set(symbol, prices);
    await sleep(CONFIG.API_DELAY_MS);
  }
  
  log('SUCCESS', 'Precios históricos obtenidos');
  
  // 5. Obtener tipos de cambio históricos
  log('PROGRESS', 'Obteniendo tipos de cambio históricos...');
  const exchangeRatesByDate = new Map();
  
  // Procesar todos los días (overwrite mode)
  for (const date of expectedDays) {
    if (!exchangeRatesByDate.has(date)) {
      const rates = { USD: 1 };
      const dateObj = new Date(date + 'T12:00:00Z');
      
      for (const currency of CONFIG.CURRENCIES.filter(c => c !== 'USD')) {
        const rate = await fetchHistoricalExchangeRate(currency, dateObj);
        if (rate) rates[currency] = rate;
        await sleep(50);
      }
      
      exchangeRatesByDate.set(date, rates);
    }
  }
  
  log('SUCCESS', 'Tipos de cambio obtenidos');
  
  // 6. Guardar snapshot pre-backfill
  const results = {
    accountsProcessed: 0,
    daysCreated: 0,
    daysSkipped: 0,
    errors: [],
  };

  if (isWrite) {
    log('PROGRESS', 'Guardando snapshot pre-backfill...');
    try {
      const snapshotDocs = [];

      for (const account of accounts) {
        const existing = await getExistingPerformanceDocs(
          userId, account.id, CONFIG.START_DATE, CONFIG.END_DATE
        );
        snapshotDocs.push(...existing);
      }

      const existingOverall = await getExistingPerformanceDocs(
        userId, null, CONFIG.START_DATE, CONFIG.END_DATE
      );
      snapshotDocs.push(...existingOverall);

      if (snapshotDocs.length > 0) {
        const backupsDir = path.join(__dirname, 'backups');
        if (!fs.existsSync(backupsDir)) {
          fs.mkdirSync(backupsDir, { recursive: true });
        }

        const snapshotPath = path.join(
          backupsDir,
          `backfill-harold-${Date.now()}.json`
        );

        const snapshot = {
          userId,
          startDate: CONFIG.START_DATE,
          endDate: CONFIG.END_DATE,
          timestamp: new Date().toISOString(),
          documentsCount: snapshotDocs.length,
          documents: snapshotDocs
        };

        fs.writeFileSync(snapshotPath, JSON.stringify(snapshot, null, 2));
        log('SUCCESS', `Snapshot guardado: ${snapshotPath} (${snapshotDocs.length} docs)`);
      } else {
        log('INFO', 'No hay documentos existentes que respaldar (rango vacío)');
      }
    } catch (snapshotError) {
      log('ERROR', `Error guardando snapshot pre-backfill: ${snapshotError.message}`);
      log('ERROR', 'Abortando backfill — no se escribirá sin red de seguridad');
      process.exit(1);
    }
  }
  
  // 7. Procesar cada cuenta
  for (const account of accounts) {
    console.log('');
    log('PROGRESS', `═══ Procesando cuenta: ${account.name} (${account.id}) ═══`);
    
    const transactions = transactionsByAccount.get(account.id);
    
    // Overwrite: procesar todos los días
    const daysToProcess = expectedDays;
    
    log('INFO', `  [OVERWRITE] Procesando ${daysToProcess.length} días`);
    
    const documentsToWrite = [];
    const calculatedPerformance = new Map();
    let skippedNoPrices = 0;
    
    for (const date of daysToProcess.sort()) {
      if (!dayHasMarketPrices(pricesBySymbol, date)) {
        skippedNoPrices++;
        continue;
      }
      
      try {
        const allDaysSorted = [...expectedDays].sort();
        const currentIdx = allDaysSorted.indexOf(date);
        let previousDayPerformance = null;
        let previousDayDate = null;
        
        for (let i = currentIdx - 1; i >= 0; i--) {
          const prevDate = allDaysSorted[i];
          if (calculatedPerformance.has(prevDate)) {
            previousDayPerformance = calculatedPerformance.get(prevDate);
            previousDayDate = prevDate;
            break;
          }
        }
        
        if (!previousDayPerformance && currentIdx === 0) {
          const beforeRangePerf = await getLastPerformanceBefore(userId, account.id, date);
          if (beforeRangePerf) {
            previousDayPerformance = beforeRangePerf;
            previousDayDate = beforeRangePerf.date;
            log('DEBUG', `    Usando documento anterior fuera del rango: ${previousDayDate}`);
          }
        }
        
        const exchangeRates = exchangeRatesByDate.get(date) || { USD: 1 };
        const { holdings, totalInvestmentUSD } = calculateHoldingsAtDate(transactions, date, exchangeRates);
        
        const accumulatedCashFlow = previousDayDate 
          ? calculateAccumulatedCashFlow(transactions, previousDayDate, date)
          : calculateDailyCashFlow(transactions, date);
        
        const dailyDonePnL = calculateDailyDonePnL(transactions, date);
        
        const performance = calculateDayPerformance(
          holdings,
          pricesBySymbol,
          date,
          exchangeRates,
          totalInvestmentUSD,
          accumulatedCashFlow,
          previousDayPerformance,
          null,
          dailyDonePnL
        );
        
        calculatedPerformance.set(date, performance);
        
        documentsToWrite.push({
          path: `portfolioPerformance/${userId}/accounts/${account.id}/dates/${date}`,
          data: { date, ...performance },
        });
        
      } catch (error) {
        log('ERROR', `  Error procesando ${date}`, { error: error.message });
        results.errors.push({ date, account: account.id, error: error.message });
      }
    }
    
    if (skippedNoPrices > 0) {
      log('WARNING', `  Saltados ${skippedNoPrices} días sin precios de mercado`);
    }
    
    if (mode === 'dry-run') {
      log('INFO', `  [DRY-RUN] Se crearían ${documentsToWrite.length} documentos`);
      
      documentsToWrite.slice(0, 3).forEach(doc => {
        log('DEBUG', `  Documento: ${doc.path}`);
        log('DEBUG', `    USD totalValue: ${doc.data.USD?.totalValue?.toFixed(2)}`);
        log('DEBUG', `    USD totalInvestment: ${doc.data.USD?.totalInvestment?.toFixed(2)}`);
      });
      
      results.daysCreated += documentsToWrite.length;
      
    } else if (isWrite) {
      log('PROGRESS', `  Escribiendo ${documentsToWrite.length} documentos a Firestore...`);
      
      for (let i = 0; i < documentsToWrite.length; i += CONFIG.BATCH_SIZE) {
        const batch = db.batch();
        const chunk = documentsToWrite.slice(i, i + CONFIG.BATCH_SIZE);
        
        chunk.forEach(doc => {
          const ref = db.doc(doc.path);
          batch.set(ref, doc.data, { merge: true });
        });
        
        await batch.commit();
        log('SUCCESS', `  Batch ${Math.floor(i / CONFIG.BATCH_SIZE) + 1} escrito (${chunk.length} docs)`);
      }
      
      results.daysCreated += documentsToWrite.length;
      
    } else {
      log('INFO', `  [ANALYZE] ${daysToProcess.length} días a procesar`);
      results.daysSkipped += daysToProcess.length;
    }
    
    results.accountsProcessed++;
  }
  
  // 8. Actualizar nivel OVERALL
  if (mode !== 'analyze') {
    log('PROGRESS', 'Procesando nivel OVERALL...');
    
    const allActiveAccounts = await getUserAccounts(userId);
    
    if (allActiveAccounts.length > 0) {
      const allAccountDates = new Set();
      const accountPerformanceByDate = new Map();
      
      for (const account of allActiveAccounts) {
        const accountDocs = await db.collection(`portfolioPerformance/${userId}/accounts/${account.id}/dates`)
          .where('date', '>=', CONFIG.START_DATE)
          .where('date', '<=', CONFIG.END_DATE)
          .orderBy('date', 'asc')
          .get();
        
        accountDocs.docs.forEach(doc => {
          const data = doc.data();
          allAccountDates.add(data.date);
          
          if (!accountPerformanceByDate.has(data.date)) {
            accountPerformanceByDate.set(data.date, new Map());
          }
          accountPerformanceByDate.get(data.date).set(account.id, data);
        });
      }
      
      log('INFO', `  Días únicos en cuentas: ${allAccountDates.size}`);
      
      const overallDays = [...allAccountDates].sort();
      log('INFO', `  [OVERWRITE] Procesando ${overallDays.length} días OVERALL`);
      
      if (overallDays.length > 0) {
        const overallDocuments = [];
        const calculatedOverall = new Map();
        
        for (const date of overallDays) {
          const accountsData = accountPerformanceByDate.get(date);
          if (!accountsData || accountsData.size === 0) continue;
          
          const aggregatedPerformance = {};
          
          CONFIG.CURRENCIES.forEach(currency => {
            let totalValue = 0;
            let totalInvestment = 0;
            let totalCashFlow = 0;
            let totalDoneProfitAndLoss = 0;
            const combinedAssetPerformance = {};
            
            let totalPreChangeValue = 0;
            let weightedAdjustedChange = 0;
            let weightedRawChange = 0;
            
            accountsData.forEach((perfData, accountId) => {
              const currencyData = perfData[currency];
              if (currencyData) {
                const accountValue = currencyData.totalValue || 0;
                const accountAdjChange = currencyData.adjustedDailyChangePercentage || 0;
                const accountRawChange = currencyData.rawDailyChangePercentage || currencyData.dailyChangePercentage || 0;
                
                totalValue += accountValue;
                totalInvestment += currencyData.totalInvestment || 0;
                totalCashFlow += currencyData.totalCashFlow || 0;
                totalDoneProfitAndLoss += currencyData.doneProfitAndLoss || 0;
                
                if (accountValue > 0) {
                  const preChangeValue = accountAdjChange !== 0 
                    ? accountValue / (1 + accountAdjChange / 100) 
                    : accountValue;
                  
                  totalPreChangeValue += preChangeValue;
                  weightedAdjustedChange += preChangeValue * accountAdjChange;
                  weightedRawChange += preChangeValue * accountRawChange;
                }
                
                if (currencyData.assetPerformance) {
                  Object.entries(currencyData.assetPerformance).forEach(([assetKey, assetPerf]) => {
                    if (!combinedAssetPerformance[assetKey]) {
                      combinedAssetPerformance[assetKey] = {
                        units: 0,
                        totalValue: 0,
                        totalInvestment: 0,
                        totalCashFlow: 0,
                        unrealizedProfitAndLoss: 0,
                        doneProfitAndLoss: 0,
                        totalROI: 0,
                        dailyChangePercentage: 0,
                        rawDailyChangePercentage: 0,
                        adjustedDailyChangePercentage: 0,
                        dailyReturn: 0,
                        monthlyReturn: 0,
                        annualReturn: 0,
                        _preChangeValue: 0,
                        _weightedAdjChange: 0,
                        _weightedRawChange: 0,
                      };
                    }
                    const combined = combinedAssetPerformance[assetKey];
                    combined.units += assetPerf.units || 0;
                    combined.totalValue += assetPerf.totalValue || 0;
                    combined.totalInvestment += assetPerf.totalInvestment || 0;
                    combined.totalCashFlow += assetPerf.totalCashFlow || 0;
                    combined.unrealizedProfitAndLoss += assetPerf.unrealizedProfitAndLoss || 0;
                    combined.doneProfitAndLoss += assetPerf.doneProfitAndLoss || 0;
                    
                    const assetValue = assetPerf.totalValue || 0;
                    const assetAdjChange = assetPerf.adjustedDailyChangePercentage || 0;
                    const assetRawChange = assetPerf.rawDailyChangePercentage || assetPerf.dailyChangePercentage || 0;
                    
                    if (assetValue > 0) {
                      const assetPreChange = assetAdjChange !== 0 
                        ? assetValue / (1 + assetAdjChange / 100) 
                        : assetValue;
                      
                      combined._preChangeValue += assetPreChange;
                      combined._weightedAdjChange += assetPreChange * assetAdjChange;
                      combined._weightedRawChange += assetPreChange * assetRawChange;
                    }
                  });
                }
              }
            });
            
            const unrealizedProfitAndLoss = totalValue - totalInvestment;
            const totalROI = totalInvestment > 0 ? ((totalValue - totalInvestment) / totalInvestment) * 100 : 0;
            
            let rawDailyChangePercentage = 0;
            let adjustedDailyChangePercentage = 0;
            
            if (totalPreChangeValue > 0) {
              adjustedDailyChangePercentage = weightedAdjustedChange / totalPreChangeValue;
              rawDailyChangePercentage = weightedRawChange / totalPreChangeValue;
            }
            
            Object.entries(combinedAssetPerformance).forEach(([assetKey, assetPerf]) => {
              if (assetPerf.totalInvestment > 0) {
                assetPerf.totalROI = ((assetPerf.totalValue - assetPerf.totalInvestment) / assetPerf.totalInvestment) * 100;
              }
              
              if (assetPerf._preChangeValue > 0) {
                assetPerf.adjustedDailyChangePercentage = assetPerf._weightedAdjChange / assetPerf._preChangeValue;
                assetPerf.rawDailyChangePercentage = assetPerf._weightedRawChange / assetPerf._preChangeValue;
                assetPerf.dailyChangePercentage = assetPerf.rawDailyChangePercentage;
                assetPerf.dailyReturn = assetPerf.adjustedDailyChangePercentage / 100;
              }
              
              delete assetPerf._preChangeValue;
              delete assetPerf._weightedAdjChange;
              delete assetPerf._weightedRawChange;
            });
            
            aggregatedPerformance[currency] = {
              totalValue,
              totalInvestment,
              totalCashFlow,
              doneProfitAndLoss: totalDoneProfitAndLoss,
              unrealizedProfitAndLoss,
              totalROI,
              dailyChangePercentage: rawDailyChangePercentage,
              rawDailyChangePercentage,
              adjustedDailyChangePercentage,
              dailyReturn: adjustedDailyChangePercentage / 100,
              monthlyReturn: 0,
              annualReturn: 0,
              assetPerformance: combinedAssetPerformance,
            };
          });
          
          calculatedOverall.set(date, aggregatedPerformance);
          
          overallDocuments.push({
            path: `portfolioPerformance/${userId}/dates/${date}`,
            data: { date, ...aggregatedPerformance },
          });
        }
        
        if (isWrite && overallDocuments.length > 0) {
          log('PROGRESS', `  Escribiendo ${overallDocuments.length} documentos OVERALL...`);
          
          for (let i = 0; i < overallDocuments.length; i += CONFIG.BATCH_SIZE) {
            const batch = db.batch();
            const chunk = overallDocuments.slice(i, i + CONFIG.BATCH_SIZE);
            
            chunk.forEach(doc => {
              const ref = db.doc(doc.path);
              batch.set(ref, doc.data, { merge: true });
            });
            
            await batch.commit();
            log('SUCCESS', `    Batch OVERALL ${Math.floor(i / CONFIG.BATCH_SIZE) + 1} escrito (${chunk.length} docs)`);
          }
          
          results.daysCreated += overallDocuments.length;
        } else if (mode === 'dry-run') {
          log('INFO', `  [DRY-RUN] Se crearían ${overallDocuments.length} documentos OVERALL`);
        }
      } else {
        log('SUCCESS', '  OVERALL está completo, no hay días faltantes');
      }
    }
  }
  
  // 9. Re-consolidar períodos afectados
  if (isWrite && !noConsolidate) {
    console.log('');
    log('PROGRESS', '═══ Re-consolidando períodos afectados ═══');
    
    const affectedMonths = getAffectedMonths(CONFIG.START_DATE, CONFIG.END_DATE);
    const affectedYears = getAffectedYears(CONFIG.START_DATE, CONFIG.END_DATE);
    
    log('INFO', `  Meses afectados: ${affectedMonths.join(', ')}`);
    log('INFO', `  Años afectados: ${affectedYears.join(', ')}`);
    
    const consolidationResults = { monthly: 0, yearly: 0, errors: 0 };
    
    const consolidationTargets = [
      { accountId: null, label: 'OVERALL' },
      ...accounts.map(a => ({ accountId: a.id, label: a.name || a.id }))
    ];
    
    for (const target of consolidationTargets) {
      for (const monthKey of affectedMonths) {
        try {
          const result = await reconsolidateMonth(userId, target.accountId, monthKey);
          if (result) {
            consolidationResults.monthly++;
            log('SUCCESS', `    Mes ${monthKey} consolidado para ${target.label}`);
          } else {
            log('DEBUG', `    Mes ${monthKey} sin datos para ${target.label}`);
          }
        } catch (err) {
          log('ERROR', `    Error consolidando mes ${monthKey} para ${target.label}: ${err.message}`);
          consolidationResults.errors++;
        }
      }
    }
    
    for (const target of consolidationTargets) {
      for (const yearKey of affectedYears) {
        try {
          const result = await reconsolidateYear(userId, target.accountId, yearKey);
          if (result) {
            consolidationResults.yearly++;
            log('SUCCESS', `    Año ${yearKey} consolidado para ${target.label}`);
          } else {
            log('DEBUG', `    Año ${yearKey} sin datos mensuales para ${target.label}`);
          }
        } catch (err) {
          log('ERROR', `    Error consolidando año ${yearKey} para ${target.label}: ${err.message}`);
          consolidationResults.errors++;
        }
      }
    }
    
    log('SUCCESS', `  Re-consolidación completada: ${consolidationResults.monthly} mensuales, ${consolidationResults.yearly} anuales` +
      (consolidationResults.errors > 0 ? `, ${consolidationResults.errors} errores` : ''));
    
    results.consolidation = consolidationResults;
    
  } else if (isWrite && noConsolidate) {
    log('INFO', '  [--no-consolidate] Omitiendo re-consolidación de períodos');
  } else if (mode === 'dry-run') {
    const affectedMonths = getAffectedMonths(CONFIG.START_DATE, CONFIG.END_DATE);
    const affectedYears = getAffectedYears(CONFIG.START_DATE, CONFIG.END_DATE);
    log('INFO', `  [DRY-RUN] Se re-consolidarían ${affectedMonths.length} meses y ${affectedYears.length} años`);
  }
  
  // 10. Resumen
  console.log('');
  console.log('═'.repeat(80));
  console.log('  RESUMEN BACKFILL');
  console.log('═'.repeat(80));
  log('SUCCESS', `Cuentas procesadas: ${results.accountsProcessed}`);
  log('SUCCESS', `Días creados/simulados: ${results.daysCreated}`);
  if (results.daysSkipped > 0) log('INFO', `Días omitidos (analyze): ${results.daysSkipped}`);
  if (results.errors.length > 0) log('WARNING', `Errores: ${results.errors.length}`);
  if (results.consolidation) {
    log('SUCCESS', `Períodos consolidados: ${results.consolidation.monthly} mensuales, ${results.consolidation.yearly} anuales`);
  }
  
  return results;
}

// ============================================================================
// PROCESO PRINCIPAL
// ============================================================================

async function main() {
  const options = parseArgs();
  
  console.log('');
  console.log('═'.repeat(80));
  console.log('  BACKFILL + MIGRACIÓN DE ASSET KEYS - HAROLD SERRANO');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', `Usuario: ${CONFIG.USER_ID} (harold.serrano.dev@gmail.com)`);
  log('INFO', `Rango: ${CONFIG.START_DATE} → ${CONFIG.END_DATE}`);
  log('INFO', `Modo: ${options.mode}`);
  log('INFO', `Cutoff migración keys: ${CONFIG.KEY_MIGRATION_CUTOFF}`);
  console.log('');
  
  const shouldFixKeys = ['fix', 'fix-keys-only', 'dry-run', 'analyze'].includes(options.mode);
  const shouldBackfill = ['fix', 'fix-backfill-only', 'dry-run', 'analyze'].includes(options.mode);
  
  // =========================================================================
  // FASE 1: Construir Asset Type Map y migrar keys
  // =========================================================================
  if (shouldFixKeys) {
    log('PROGRESS', 'Construyendo Asset Type Map desde transacciones...');
    const assetTypeMap = await buildAssetTypeMap(CONFIG.USER_ID);
    log('SUCCESS', `Asset Type Map construido con ${Object.keys(assetTypeMap).length} assets`);
    
    await migrateAssetKeys(CONFIG.USER_ID, assetTypeMap, options.mode);
    console.log('');
  }
  
  // =========================================================================
  // FASE 2: Backfill completo
  // =========================================================================
  if (shouldBackfill) {
    await runBackfill(CONFIG.USER_ID, options.mode, options.noConsolidate);
  }
  
  console.log('');
  log('SUCCESS', '¡Proceso completado!');
  process.exit(0);
}

// Ejecutar
main().catch(error => {
  log('ERROR', `Error fatal: ${error.message}`);
  console.error(error);
  process.exit(1);
});
