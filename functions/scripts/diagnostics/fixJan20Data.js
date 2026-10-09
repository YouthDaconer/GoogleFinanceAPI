/**
 * Script de corrección para el incidente OPT-DEMAND-500
 * 
 * PROBLEMA:
 * 1. Se creó erróneamente un documento para 2026-01-17 (sábado - no es día de trading)
 * 2. Se eliminó el documento del 2026-01-20 (martes - día de trading válido)
 * 
 * SOLUCIÓN:
 * 1. Eliminar documentos del 2026-01-17 (overall y cuentas)
 * 2. Regenerar datos del 2026-01-20 con precios históricos reales
 * 
 * FUENTES DE DATOS:
 * - Precios de assets: https://354sdh5hcrnztw5vquw6sxiduu0gigku.lambda-url.us-east-1.on.aws/v1
 * - Tipos de cambio: https://query1.finance.yahoo.com/v8/finance/chart/{symbol}=X
 * 
 * USO:
 *   node fixJan20Data.js --dry-run    # Ver cambios sin aplicar
 *   node fixJan20Data.js --fix        # Aplicar cambios
 */

const admin = require('firebase-admin');
const fetch = require('node-fetch');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// Configuración
const CONFIG = {
  userId: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  targetDate: '2026-01-20',
  invalidDate: '2026-01-17',
  previousDate: '2026-01-16',
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  HISTORICAL_API_BASE: 'https://354sdh5hcrnztw5vquw6sxiduu0gigku.lambda-url.us-east-1.on.aws/v1',
  accounts: [
    { id: 'BZHvXz4QT2yqqqlFP22X', name: 'IBKR' },
    { id: 'Z3gnboYgRlTvSZNGSu8j', name: 'XTB' },
    { id: 'zHZCvwpQeA2HoYMxDtPF', name: 'Binance Cryptos' },
    { id: 'ggM52GimbLL7jwvegc9o', name: 'Trii' },
    { id: '7yOZyIh2YBRN26WyOpb7', name: 'Other' }
  ]
};

const MODE = process.argv.includes('--fix') ? 'fix' : 'dry-run';

function log(level, message, data = null) {
  const prefix = { info: '📌', warn: '⚠️', error: '❌', success: '✅' }[level] || '•';
  console.log(`${prefix} [${level.toUpperCase()}] ${message}`);
  if (data) console.log('  ', JSON.stringify(data, null, 2).split('\n').join('\n   '));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// OBTENCIÓN DE DATOS DE MERCADO
// ============================================================================

/**
 * Obtener precios históricos de un símbolo usando el endpoint correcto
 */
async function fetchHistoricalPrice(symbol, targetDate) {
  try {
    // Usar el endpoint /historical que devuelve un objeto con fechas como keys
    const url = `${CONFIG.HISTORICAL_API_BASE}/historical?symbol=${encodeURIComponent(symbol)}&range=1mo&interval=1d`;
    const response = await fetch(url);
    
    if (!response.ok) {
      log('warn', `No se pudo obtener precio para ${symbol}: ${response.status}`);
      return null;
    }
    
    const data = await response.json();
    
    // El formato es: { "2026-01-20": { open, high, low, close, adjClose, volume }, ... }
    if (data && typeof data === 'object') {
      // Buscar precio para la fecha específica
      if (data[targetDate] && data[targetDate].close) {
        return data[targetDate].close;
      }
      
      // Fallback: buscar el día anterior más cercano
      const sortedDates = Object.keys(data).sort().reverse();
      for (const date of sortedDates) {
        if (date <= targetDate && data[date]?.close) {
          log('info', `Usando precio de ${date} para ${symbol} (fallback)`);
          return data[date].close;
        }
      }
    }
    
    return null;
  } catch (error) {
    log('error', `Error obteniendo precio de ${symbol}: ${error.message}`);
    return null;
  }
}

/**
 * Obtener precios de múltiples símbolos con delay para evitar rate limiting
 */
async function fetchMultiplePrices(symbols, targetDate) {
  const priceMap = new Map();
  const uniqueSymbols = [...new Set(symbols)];
  
  log('info', `Obteniendo precios para ${uniqueSymbols.length} símbolos únicos...`);
  
  for (let i = 0; i < uniqueSymbols.length; i++) {
    const symbol = uniqueSymbols[i];
    const price = await fetchHistoricalPrice(symbol, targetDate);
    if (price) {
      priceMap.set(symbol, price);
    }
    
    // Delay entre llamadas para evitar rate limiting (300ms)
    if (i < uniqueSymbols.length - 1) {
      await sleep(300);
    }
    
    // Log de progreso cada 10 símbolos
    if ((i + 1) % 10 === 0) {
      log('info', `Progreso: ${i + 1}/${uniqueSymbols.length} símbolos procesados`);
    }
  }
  
  log('success', `Obtenidos precios para ${priceMap.size}/${uniqueSymbols.length} símbolos`);
  return priceMap;
}

/**
 * Obtener tipo de cambio histórico desde Yahoo Finance
 */
async function fetchHistoricalExchangeRate(currency, targetDate) {
  if (currency === 'USD') return 1.0;
  
  try {
    const symbol = `${currency}=X`;
    const targetDateTime = DateTime.fromISO(targetDate).setZone('America/New_York');
    const period1 = Math.floor(targetDateTime.minus({ days: 7 }).toSeconds());
    const period2 = Math.floor(targetDateTime.plus({ days: 1 }).toSeconds());
    
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?period1=${period1}&period2=${period2}&interval=1d`;
    
    const response = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    
    if (!response.ok) {
      log('warn', `No se pudo obtener tasa de cambio para ${currency}`);
      return null;
    }
    
    const data = await response.json();
    const result = data.chart?.result?.[0];
    
    if (result && result.timestamp && result.indicators?.quote?.[0]?.close) {
      const timestamps = result.timestamp;
      const closes = result.indicators.quote[0].close;
      
      // Buscar la fecha objetivo
      const targetTs = targetDateTime.startOf('day').toSeconds();
      
      for (let i = timestamps.length - 1; i >= 0; i--) {
        const ts = timestamps[i];
        const tsDate = DateTime.fromSeconds(ts).setZone('America/New_York').toISODate();
        
        if (tsDate === targetDate && closes[i]) {
          return closes[i];
        }
      }
      
      // Fallback: usar el último disponible antes de la fecha
      for (let i = timestamps.length - 1; i >= 0; i--) {
        if (closes[i]) {
          return closes[i];
        }
      }
    }
    
    return null;
  } catch (error) {
    log('error', `Error obteniendo tasa de ${currency}: ${error.message}`);
    return null;
  }
}

/**
 * Obtener todos los tipos de cambio para una fecha
 */
async function fetchAllExchangeRates(targetDate) {
  const rates = { USD: 1.0 };
  
  for (const currency of CONFIG.CURRENCIES) {
    if (currency === 'USD') continue;
    
    const rate = await fetchHistoricalExchangeRate(currency, targetDate);
    if (rate) {
      rates[currency] = rate;
    } else {
      log('warn', `No se obtuvo tasa para ${currency}, usando fallback`);
      // Fallback a tasas aproximadas
      const fallbackRates = { COP: 4150, EUR: 0.92, MXN: 20.5, BRL: 6.1, GBP: 0.79, CAD: 1.44 };
      rates[currency] = fallbackRates[currency] || 1;
    }
    
    await sleep(200);
  }
  
  return rates;
}

// ============================================================================
// CÁLCULO DE PERFORMANCE
// ============================================================================

/**
 * Obtener assets activos de una cuenta
 */
async function getAccountAssets(accountId) {
  const snapshot = await db.collection('assets')
    .where('portfolioAccount', '==', accountId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Obtener transacciones de un día
 */
async function getDayTransactions(targetDate) {
  const snapshot = await db.collection('transactions')
    .where('date', '==', targetDate)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Calcular performance de un día para una cuenta usando precios pre-cargados
 * 
 * NOTA: Los precios de la API están en la moneda nativa del activo.
 * Por ejemplo: ECOPETROL.CL está en COP, AMZN está en USD.
 * El campo asset.currency indica la moneda del activo.
 */
async function calculateAccountPerformance(accountId, accountName, previousData, exchangeRates, allTransactions, priceMap) {
  const assets = await getAccountAssets(accountId);
  
  if (assets.length === 0) {
    log('info', `  ${accountName}: Sin assets activos`);
    return null;
  }
  
  const accountTransactions = allTransactions.filter(t => t.portfolioAccountId === accountId);
  
  // Calcular performance por moneda
  const performanceByCurrency = {};
  
  for (const currency of CONFIG.CURRENCIES) {
    const targetRate = exchangeRates[currency] || 1;
    const prevCurrencyData = previousData?.[currency] || {};
    const prevTotalValue = prevCurrencyData.totalValue || 0;
    
    let totalValue = 0;
    let totalInvestment = 0;
    const assetPerformance = {};
    
    for (const asset of assets) {
      const price = priceMap.get(asset.name);
      if (!price || !asset.units) continue;
      
      // El precio está en la moneda del activo (asset.currency)
      // Primero convertimos a USD, luego a la moneda objetivo
      const assetCurrency = asset.currency || 'USD';
      const assetRate = exchangeRates[assetCurrency] || 1;
      
      // Convertir precio a USD
      // Si el activo es en USD: priceUSD = price
      // Si el activo es en COP: priceUSD = price / exchangeRates.COP
      const priceUSD = assetCurrency === 'USD' ? price : price / assetRate;
      
      // Calcular valor en USD
      const valueUSD = priceUSD * asset.units;
      
      // Convertir a la moneda objetivo
      const value = currency === 'USD' ? valueUSD : valueUSD * targetRate;
      
      // Calcular inversión (unitValue también está en la moneda del activo)
      const investmentNative = asset.unitValue * asset.units;
      const investmentUSD = assetCurrency === 'USD' ? investmentNative : investmentNative / assetRate;
      const investment = currency === 'USD' ? investmentUSD : investmentUSD * targetRate;
      
      totalValue += value;
      totalInvestment += investment;
      
      const assetKey = `${asset.name}_${asset.assetType}`;
      const prevAssetData = prevCurrencyData.assetPerformance?.[assetKey] || {};
      const prevAssetValue = prevAssetData.totalValue || 0;
      
      // Calcular cambio diario del asset
      const dailyChange = prevAssetValue > 0 ? (value - prevAssetValue) / prevAssetValue : 0;
      
      assetPerformance[assetKey] = {
        totalValue: value,
        totalInvestment: investment,
        currentPrice: price, // Precio en moneda nativa
        units: asset.units,
        dailyChangePercentage: dailyChange,
        rawDailyChangePercentage: dailyChange,
        adjustedDailyChangePercentage: dailyChange, // Sin cashflow a nivel de asset
        totalROI: investment > 0 ? ((value - investment) / investment) * 100 : 0,
        unrealizedProfitAndLoss: value - investment
      };
    }
    
    // Calcular cashflow del día para esta cuenta
    let dailyCashFlow = 0;
    accountTransactions.forEach(tx => {
      const txAmount = (tx.amount || 0) * (tx.price || 0);
      // Convertir a la moneda (asumiendo que las transacciones están en su moneda original)
      const txCurrency = tx.currency || 'USD';
      const txRate = exchangeRates[txCurrency] || 1;
      const txAmountUSD = txCurrency === 'USD' ? txAmount : txAmount / txRate;
      const txAmountConverted = currency === 'USD' ? txAmountUSD : txAmountUSD * targetRate;
      
      if (tx.type === 'buy') dailyCashFlow -= txAmountConverted;
      if (tx.type === 'sell') dailyCashFlow += txAmountConverted;
    });
    
    // Calcular cambios porcentuales
    const dailyChange = prevTotalValue > 0 ? (totalValue - prevTotalValue) / prevTotalValue : 0;
    
    // TWR: ajustado por cashflow
    const preChangeValue = prevTotalValue + dailyCashFlow;
    const adjustedChange = Math.abs(preChangeValue) > 0.01 
      ? (totalValue - preChangeValue) / Math.abs(preChangeValue) 
      : 0;
    
    performanceByCurrency[currency] = {
      totalValue,
      totalInvestment,
      totalROI: totalInvestment > 0 ? ((totalValue - totalInvestment) / totalInvestment) * 100 : 0,
      dailyChangePercentage: dailyChange,
      rawDailyChangePercentage: dailyChange,
      adjustedDailyChangePercentage: adjustedChange,
      totalCashFlow: dailyCashFlow,
      profitAndLoss: totalValue - totalInvestment,
      unrealizedProfitAndLoss: totalValue - totalInvestment,
      assetPerformance
    };
  }
  
  log('info', `  ${accountName}: $${performanceByCurrency.USD?.totalValue?.toFixed(2) || 0} USD`);
  
  return performanceByCurrency;
}

/**
 * Agregar performance de múltiples cuentas para overall
 */
function aggregateAccountsToOverall(accountPerformances) {
  const overall = {};
  
  for (const currency of CONFIG.CURRENCIES) {
    let totalValue = 0;
    let totalInvestment = 0;
    let totalCashFlow = 0;
    let weightedAdjustedChange = 0;
    let totalPreChangeValue = 0;
    const combinedAssetPerformance = {};
    
    for (const [accountId, performance] of Object.entries(accountPerformances)) {
      if (!performance || !performance[currency]) continue;
      
      const currData = performance[currency];
      totalValue += currData.totalValue || 0;
      totalInvestment += currData.totalInvestment || 0;
      totalCashFlow += currData.totalCashFlow || 0;
      
      // Para calcular el adjustedChange ponderado, usar preChangeValue
      const accountChange = currData.adjustedDailyChangePercentage || 0;
      const accountValue = currData.totalValue || 0;
      const preChangeValue = Math.abs(accountChange) > 0.0001 
        ? accountValue / (1 + accountChange)
        : accountValue;
      
      weightedAdjustedChange += preChangeValue * accountChange;
      totalPreChangeValue += preChangeValue;
      
      // Combinar assetPerformance
      for (const [assetKey, assetData] of Object.entries(currData.assetPerformance || {})) {
        if (!combinedAssetPerformance[assetKey]) {
          combinedAssetPerformance[assetKey] = { ...assetData };
        } else {
          // Sumar valores (el asset puede estar en múltiples cuentas)
          combinedAssetPerformance[assetKey].totalValue += assetData.totalValue || 0;
          combinedAssetPerformance[assetKey].totalInvestment += assetData.totalInvestment || 0;
          combinedAssetPerformance[assetKey].units += assetData.units || 0;
        }
      }
    }
    
    // Calcular cambio ajustado ponderado
    const combinedAdjustedChange = totalPreChangeValue > 0 
      ? weightedAdjustedChange / totalPreChangeValue 
      : 0;
    
    overall[currency] = {
      totalValue,
      totalInvestment,
      totalROI: totalInvestment > 0 ? ((totalValue - totalInvestment) / totalInvestment) * 100 : 0,
      dailyChangePercentage: combinedAdjustedChange, // Usar el mismo para simplicidad
      rawDailyChangePercentage: combinedAdjustedChange,
      adjustedDailyChangePercentage: combinedAdjustedChange,
      totalCashFlow,
      profitAndLoss: totalValue - totalInvestment,
      unrealizedProfitAndLoss: totalValue - totalInvestment,
      assetPerformance: combinedAssetPerformance
    };
  }
  
  return overall;
}

// ============================================================================
// PROCESO PRINCIPAL
// ============================================================================

async function main() {
  console.log('='.repeat(90));
  console.log(`CORRECCIÓN OPT-DEMAND-500 - MODO: ${MODE.toUpperCase()}`);
  console.log('='.repeat(90));
  console.log('');
  
  if (MODE === 'dry-run') {
    console.log('⚠️  MODO DRY-RUN: No se realizarán cambios.');
  }
  console.log('');
  
  // =========================================================================
  // PASO 1: Eliminar documentos erróneos del 2026-01-17
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 1: Eliminar documentos erróneos del 2026-01-17 (sábado)');
  console.log('─'.repeat(90));
  
  const docsToDelete = [];
  
  // Overall
  const overall17 = await db.doc(`portfolioPerformance/${CONFIG.userId}/dates/${CONFIG.invalidDate}`).get();
  if (overall17.exists) {
    docsToDelete.push(`portfolioPerformance/${CONFIG.userId}/dates/${CONFIG.invalidDate}`);
  }
  
  // Cuentas
  for (const account of CONFIG.accounts) {
    const acc17 = await db.doc(`portfolioPerformance/${CONFIG.userId}/accounts/${account.id}/dates/${CONFIG.invalidDate}`).get();
    if (acc17.exists) {
      docsToDelete.push(`portfolioPerformance/${CONFIG.userId}/accounts/${account.id}/dates/${CONFIG.invalidDate}`);
    }
  }
  
  if (docsToDelete.length === 0) {
    log('info', 'No hay documentos del 2026-01-17 para eliminar');
  } else {
    for (const path of docsToDelete) {
      if (MODE === 'fix') {
        await db.doc(path).delete();
        log('success', `Eliminado: ${path}`);
      } else {
        log('info', `Eliminaría: ${path}`);
      }
    }
  }
  console.log('');
  
  // =========================================================================
  // PASO 2: Obtener datos de mercado para 2026-01-20
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 2: Obtener datos de mercado para 2026-01-20');
  console.log('─'.repeat(90));
  
  log('info', 'Obteniendo tipos de cambio...');
  const exchangeRates = await fetchAllExchangeRates(CONFIG.targetDate);
  log('success', 'Tipos de cambio obtenidos', exchangeRates);
  console.log('');
  
  // =========================================================================
  // PASO 3: Obtener datos del día anterior (2026-01-16) 
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 3: Obtener datos del día anterior (2026-01-16)');
  console.log('─'.repeat(90));
  
  const previousOverall = await db.doc(`portfolioPerformance/${CONFIG.userId}/dates/${CONFIG.previousDate}`).get();
  const previousData = previousOverall.exists ? previousOverall.data() : {};
  log('info', `Overall anterior: $${previousData.USD?.totalValue?.toFixed(2) || 0} USD`);
  
  const previousAccountData = {};
  for (const account of CONFIG.accounts) {
    const accDoc = await db.doc(`portfolioPerformance/${CONFIG.userId}/accounts/${account.id}/dates/${CONFIG.previousDate}`).get();
    previousAccountData[account.id] = accDoc.exists ? accDoc.data() : {};
  }
  console.log('');
  
  // =========================================================================
  // PASO 4: Obtener transacciones del día
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 4: Obtener transacciones del día');
  console.log('─'.repeat(90));
  
  const dayTransactions = await getDayTransactions(CONFIG.targetDate);
  log('info', `Transacciones encontradas: ${dayTransactions.length}`);
  console.log('');
  
  // =========================================================================
  // PASO 5: Obtener todos los símbolos y precios
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 5: Obtener precios de todos los assets');
  console.log('─'.repeat(90));
  
  // Primero recopilar todos los símbolos únicos de todas las cuentas
  const allSymbols = new Set();
  const assetsByAccount = new Map();
  
  for (const account of CONFIG.accounts) {
    const assets = await getAccountAssets(account.id);
    assetsByAccount.set(account.id, assets);
    assets.forEach(a => allSymbols.add(a.name));
  }
  
  log('info', `Símbolos únicos encontrados: ${allSymbols.size}`);
  
  // Obtener todos los precios de una vez con delay adecuado
  const priceMap = await fetchMultiplePrices([...allSymbols], CONFIG.targetDate);
  console.log('');
  
  // =========================================================================
  // PASO 6: Calcular performance por cuenta
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 6: Calcular performance por cuenta');
  console.log('─'.repeat(90));
  
  const accountPerformances = {};
  
  for (const account of CONFIG.accounts) {
    const performance = await calculateAccountPerformance(
      account.id,
      account.name,
      previousAccountData[account.id],
      exchangeRates,
      dayTransactions,
      priceMap
    );
    
    if (performance) {
      accountPerformances[account.id] = performance;
    }
  }
  console.log('');
  
  // =========================================================================
  // PASO 7: Agregar para overall
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 7: Agregar para overall');
  console.log('─'.repeat(90));
  
  const overallPerformance = aggregateAccountsToOverall(accountPerformances);
  log('success', `Overall: $${overallPerformance.USD?.totalValue?.toFixed(2) || 0} USD`);
  log('info', `adjustedDailyChangePercentage: ${((overallPerformance.USD?.adjustedDailyChangePercentage || 0) * 100).toFixed(4)}%`);
  console.log('');
  
  // =========================================================================
  // PASO 8: Guardar en Firestore
  // =========================================================================
  console.log('─'.repeat(90));
  console.log('PASO 8: Guardar en Firestore');
  console.log('─'.repeat(90));
  
  if (MODE === 'fix') {
    // Guardar overall
    await db.doc(`portfolioPerformance/${CONFIG.userId}/dates/${CONFIG.targetDate}`).set({
      date: CONFIG.targetDate,
      source: 'fix-jan20-backfill',
      fixedAt: new Date().toISOString(),
      ...overallPerformance
    });
    log('success', `Guardado: portfolioPerformance/${CONFIG.userId}/dates/${CONFIG.targetDate}`);
    
    // Guardar cuentas
    for (const account of CONFIG.accounts) {
      if (accountPerformances[account.id]) {
        await db.doc(`portfolioPerformance/${CONFIG.userId}/accounts/${account.id}/dates/${CONFIG.targetDate}`).set({
          date: CONFIG.targetDate,
          source: 'fix-jan20-backfill',
          fixedAt: new Date().toISOString(),
          ...accountPerformances[account.id]
        });
        log('success', `Guardado: accounts/${account.id}/dates/${CONFIG.targetDate}`);
      }
    }
  } else {
    log('info', 'Modo dry-run: No se guardaron datos');
    log('info', `Guardaría overall con $${overallPerformance.USD?.totalValue?.toFixed(2) || 0} USD`);
  }
  console.log('');
  
  // =========================================================================
  // RESUMEN
  // =========================================================================
  console.log('='.repeat(90));
  console.log('RESUMEN');
  console.log('='.repeat(90));
  
  console.log(`
Acciones ${MODE === 'fix' ? 'realizadas' : 'pendientes'}:
1. ${MODE === 'fix' ? '✅' : '📝'} Eliminados ${docsToDelete.length} documentos del 2026-01-17
2. ${MODE === 'fix' ? '✅' : '📝'} Generados datos para 2026-01-20
   - Overall: $${overallPerformance.USD?.totalValue?.toFixed(2) || 0} USD
   - Cuentas: ${Object.keys(accountPerformances).length}
   - adjustedDailyChange: ${((overallPerformance.USD?.adjustedDailyChangePercentage || 0) * 100).toFixed(4)}%
  `);
  
  if (MODE === 'dry-run') {
    console.log('Para aplicar los cambios, ejecute:');
    console.log('  node fixJan20Data.js --fix');
  }
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
}).finally(() => process.exit(0));
