/**
 * Script de Corrección de Asset Performance TWR
 * 
 * PROPÓSITO:
 * Corregir los datos de rendimiento TWR de un asset específico en portfolioPerformance
 * usando precios históricos reales de la API finance-query.
 * 
 * ALCANCE:
 * - Modifica SOLO el campo assetPerformance[assetKey] dentro de cada moneda
 * - Actualiza tanto OVERALL (portfolioPerformance/{userId}/dates) como
 *   las cuentas específicas (portfolioPerformance/{userId}/accounts/{accountId}/dates)
 * - NO modifica otros campos del documento ni otros assets
 * 
 * USO:
 *   node fixAssetPerformance.js AMZN_stock --analyze     # Solo analiza discrepancias
 *   node fixAssetPerformance.js AMZN_stock --dry-run    # Muestra cambios sin aplicar
 *   node fixAssetPerformance.js AMZN_stock --fix        # Aplica los cambios
 * 
 * OPCIONES:
 *   --user=<userId>        # Usuario específico (default: DDeR8P5hYgfuN8gcU4RsQfdTJqx2)
 *   --start=YYYY-MM-DD     # Fecha inicio
 *   --end=YYYY-MM-DD       # Fecha fin
 *   --threshold=<number>   # Umbral de discrepancia en % para considerar corrección (default: 0.5)
 * 
 * ESTRUCTURA DE DATOS:
 * portfolioPerformance/
 *   {userId}/
 *     dates/                         <- OVERALL
 *       {date}/
 *         USD: { assetPerformance: { AMZN_stock: {...} } }
 *         COP: { assetPerformance: { AMZN_stock: {...} } }
 *     accounts/
 *       {accountId}/
 *         dates/
 *           {date}/
 *             USD: { assetPerformance: { AMZN_stock: {...} } }
 *             COP: { assetPerformance: { AMZN_stock: {...} } }
 * 
 * @see verifyAssetTWR.js (Script de verificación)
 * @see backfillPortfolioPerformance.js (Lógica base de cálculo)
 */

const admin = require('firebase-admin');
const fetch = require('node-fetch');
const { DateTime } = require('luxon');

// Inicializar Firebase Admin
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const CONFIG = {
  HISTORICAL_API_BASE: 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1',
  DEFAULT_USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  
  // Monedas soportadas
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  
  // Umbral de discrepancia para considerar corrección (%)
  DEFAULT_THRESHOLD: 0.5,
  
  // Batch size para escrituras
  BATCH_SIZE: 400,
  
  // API delay
  API_DELAY_MS: 100,
};

// ============================================================================
// UTILIDADES
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    assetKey: null,
    mode: 'analyze', // analyze, dry-run, fix
    userId: CONFIG.DEFAULT_USER_ID,
    startDate: '2025-01-01',
    endDate: DateTime.now().setZone('America/New_York').toISODate(),
    threshold: CONFIG.DEFAULT_THRESHOLD,
    useHistoricalFx: false, // Si true, obtiene tipos de cambio históricos de Yahoo Finance
    fixPnL: false, // Si true, también actualiza doneProfitAndLoss aunque no haya discrepancias en TWR
  };

  args.forEach(arg => {
    if (arg === '--analyze') options.mode = 'analyze';
    else if (arg === '--dry-run') options.mode = 'dry-run';
    else if (arg === '--fix') options.mode = 'fix';
    else if (arg === '--use-historical-fx') options.useHistoricalFx = true;
    else if (arg === '--fix-pnl') options.fixPnL = true;
    else if (arg.startsWith('--user=')) options.userId = arg.split('=')[1];
    else if (arg.startsWith('--start=')) options.startDate = arg.split('=')[1];
    else if (arg.startsWith('--end=')) options.endDate = arg.split('=')[1];
    else if (arg.startsWith('--threshold=')) options.threshold = parseFloat(arg.split('=')[1]);
    else if (!arg.startsWith('--')) options.assetKey = arg;
  });

  return options;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function log(level, message, data = null) {
  const prefix = {
    'INFO': '📋',
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'DEBUG': '🔍',
    'CHANGE': '🔄',
    'SKIP': '⏭️',
  }[level] || '•';
  
  console.log(`${prefix} ${message}`);
  if (data) console.log('   ', JSON.stringify(data, null, 2).split('\n').join('\n    '));
}

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

/**
 * Obtener precios históricos de un símbolo
 */
async function fetchHistoricalPrices(symbol, startDate = null) {
  try {
    let range = '1y';
    if (startDate) {
      const start = new Date(startDate);
      const now = new Date();
      const monthsAgo = (now.getFullYear() - start.getFullYear()) * 12 + (now.getMonth() - start.getMonth());
      if (monthsAgo > 12) range = '2y';
      else if (monthsAgo > 6) range = '1y';
      else range = 'ytd';
    }
    
    const url = `${CONFIG.HISTORICAL_API_BASE}/historical?symbol=${encodeURIComponent(symbol)}&range=${range}&interval=1d`;
    log('INFO', `Obteniendo precios históricos de ${symbol} (${range})...`);
    
    const response = await fetch(url);
    if (!response.ok) {
      log('ERROR', `No se pudieron obtener precios para ${symbol}: ${response.status}`);
      return {};
    }
    
    const data = await response.json();
    const priceMap = {};
    Object.entries(data).forEach(([date, ohlcv]) => {
      priceMap[date] = ohlcv.close;
    });
    
    log('SUCCESS', `Obtenidos ${Object.keys(priceMap).length} precios históricos para ${symbol}`);
    return priceMap;
  } catch (error) {
    log('ERROR', `Error obteniendo precios para ${symbol}: ${error.message}`);
    return {};
  }
}

/**
 * Obtener tipo de cambio histórico desde Yahoo Finance
 * @param {string} currency - Código de moneda (COP, EUR, etc.)
 * @param {Date} date - Fecha objetivo
 * @returns {number|null} Tipo de cambio relativo a USD
 */
async function fetchHistoricalExchangeRate(currency, date) {
  if (currency === 'USD') return 1;
  
  try {
    // Determinar el símbolo correcto del par de divisas
    let symbol;
    if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
      symbol = `${currency}USD`; // Estas monedas cotizan como XXXUSD
    } else {
      symbol = `USD${currency}`; // Las demás cotizan como USDXXX
    }
    
    const timestamp = Math.floor(date.getTime() / 1000);
    const nextDay = timestamp + 86400;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}%3DX?period1=${timestamp}&period2=${nextDay}&interval=1d`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    if (data.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.[0]) {
      let rate = data.chart.result[0].indicators.quote[0].close[0];
      
      // Para EUR y GBP, necesitamos invertir (son XXXUSD, pero queremos USD/XXX)
      if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
        rate = 1 / rate;
      }
      
      return rate;
    }
    
    return null;
  } catch (error) {
    log('WARNING', `Error obteniendo tipo de cambio para ${currency}: ${error.message}`);
    return null;
  }
}

/**
 * Obtener tipos de cambio para una fecha específica (todas las monedas)
 */
async function fetchExchangeRatesForDate(date) {
  const rates = { USD: 1 };
  const dateObj = new Date(date);
  
  for (const currency of CONFIG.CURRENCIES) {
    if (currency === 'USD') continue;
    
    const rate = await fetchHistoricalExchangeRate(currency, dateObj);
    if (rate) {
      rates[currency] = rate;
    }
    await sleep(50); // Rate limiting para Yahoo Finance
  }
  
  return rates;
}

/**
 * Obtener transacciones del usuario para un asset específico
 */
async function getAssetTransactions(userId, assetName) {
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .get();
  
  const userAccountIds = accountsSnapshot.docs.map(doc => doc.id);
  
  const snapshot = await db.collection('transactions')
    .where('assetName', '==', assetName)
    .get();
  
  const transactions = snapshot.docs
    .map(doc => ({ id: doc.id, ...doc.data() }))
    .filter(tx => userAccountIds.includes(tx.portfolioAccountId))
    .sort((a, b) => a.date.localeCompare(b.date));
  
  // Agrupar por cuenta
  const byAccount = new Map();
  transactions.forEach(tx => {
    if (!byAccount.has(tx.portfolioAccountId)) {
      byAccount.set(tx.portfolioAccountId, []);
    }
    byAccount.get(tx.portfolioAccountId).push(tx);
  });
  
  return { all: transactions, byAccount, accountIds: userAccountIds };
}

/**
 * Obtener documentos de performance existentes
 */
async function getPerformanceDocuments(userId, accountId, startDate, endDate) {
  const path = accountId 
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates`
    : `portfolioPerformance/${userId}/dates`;
  
  const snapshot = await db.collection(path)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs;
}

// ============================================================================
// CÁLCULO DE HOLDINGS Y TWR
// ============================================================================

function calculateHoldingsAtDate(transactions, targetDate) {
  let units = 0;
  let totalInvestment = 0;
  
  transactions.filter(tx => tx.date <= targetDate).forEach(tx => {
    if (tx.type === 'buy') {
      units += tx.amount || 0;
      totalInvestment += (tx.amount || 0) * (tx.price || 0);
    } else if (tx.type === 'sell') {
      const soldUnits = tx.amount || 0;
      const avgCost = units > 0 ? totalInvestment / units : 0;
      units -= soldUnits;
      totalInvestment -= soldUnits * avgCost;
    }
  });
  
  return {
    units: Math.max(0, units),
    totalInvestment: Math.max(0, totalInvestment)
  };
}

function calculateDailyCashFlow(transactions, targetDate) {
  return transactions
    .filter(tx => tx.date === targetDate)
    .reduce((sum, tx) => {
      if (tx.type === 'buy') return sum - (tx.amount || 0) * (tx.price || 0);
      if (tx.type === 'sell') return sum + (tx.amount || 0) * (tx.price || 0);
      return sum;
    }, 0);
}

/**
 * Calcular valores del asset para un día (solo USD)
 * Los cambios porcentuales son independientes de la moneda
 */
function calculateAssetPerformanceUSD(holdings, price, previousValue, dailyCashFlow) {
  const totalValueUSD = holdings.units * price;
  const totalInvestmentUSD = holdings.totalInvestment;
  const unrealizedPnLUSD = totalValueUSD - totalInvestmentUSD;
  const totalROI = totalInvestmentUSD > 0 ? (unrealizedPnLUSD / totalInvestmentUSD) * 100 : 0;
  
  // Cambios diarios (estos porcentajes son iguales para todas las monedas)
  let dailyChangePercentage = 0;
  let adjustedDailyChangePercentage = 0;
  
  if (previousValue !== null && previousValue > 0) {
    dailyChangePercentage = ((totalValueUSD - previousValue) / previousValue) * 100;
    // TWR: ajustado por cashflow
    adjustedDailyChangePercentage = ((totalValueUSD - previousValue + dailyCashFlow) / previousValue) * 100;
  }
  
  return {
    totalValueUSD,
    totalInvestmentUSD,
    unrealizedPnLUSD,
    totalROI,
    dailyChangePercentage,
    adjustedDailyChangePercentage,
    rawDailyChangePercentage: dailyChangePercentage,
    units: holdings.units,
    dailyCashFlowUSD: dailyCashFlow,
  };
}

/**
 * Derivar tipos de cambio desde un documento existente
 * Compara totalValue en USD vs otras monedas para inferir la tasa
 */
function deriveExchangeRatesFromDoc(docData) {
  const rates = { USD: 1 };
  const usdTotal = docData.USD?.totalValue || 0;
  
  if (usdTotal <= 0) return rates;
  
  CONFIG.CURRENCIES.forEach(currency => {
    if (currency === 'USD') return;
    const currencyTotal = docData[currency]?.totalValue || 0;
    if (currencyTotal > 0) {
      rates[currency] = currencyTotal / usdTotal;
    }
  });
  
  return rates;
}

/**
 * Calcular cashflow ACUMULADO entre dos fechas (exclusive startDate, inclusive endDate)
 * Esto es necesario cuando hay transacciones en días sin documento de mercado
 */
function calculateAccumulatedCashFlow(transactions, startDateExclusive, endDateInclusive) {
  return transactions
    .filter(tx => tx.date > startDateExclusive && tx.date <= endDateInclusive)
    .reduce((sum, tx) => {
      if (tx.type === 'buy') return sum - (tx.amount || 0) * (tx.price || 0);
      if (tx.type === 'sell') return sum + (tx.amount || 0) * (tx.price || 0);
      return sum;
    }, 0);
}

/**
 * Calcular el P&L Realizado (doneProfitAndLoss) de las ventas de un día
 * 
 * El cálculo se hace usando el método de costo promedio (Average Cost):
 * - Se calcula el costo promedio por unidad ANTES de la venta
 * - Se compara con el precio de venta para obtener el P&L
 * 
 * @param {Array} transactions - Todas las transacciones del asset
 * @param {string} targetDate - Fecha para calcular el P&L
 * @returns {number} P&L realizado en USD
 */
function calculateDoneProfitAndLoss(transactions, targetDate) {
  // Obtener ventas del día
  const salesOnDate = transactions.filter(tx => tx.date === targetDate && tx.type === 'sell');
  
  if (salesOnDate.length === 0) return 0;
  
  // Calcular holdings ANTES de las ventas del día (hasta el día anterior)
  let unitsBeforeSales = 0;
  let investmentBeforeSales = 0;
  
  transactions.filter(tx => tx.date < targetDate).forEach(tx => {
    if (tx.type === 'buy') {
      unitsBeforeSales += tx.amount || 0;
      investmentBeforeSales += (tx.amount || 0) * (tx.price || 0);
    } else if (tx.type === 'sell') {
      const soldUnits = tx.amount || 0;
      const avgCost = unitsBeforeSales > 0 ? investmentBeforeSales / unitsBeforeSales : 0;
      unitsBeforeSales -= soldUnits;
      investmentBeforeSales -= soldUnits * avgCost;
    }
  });
  
  // Agregar compras del mismo día (que ocurren ANTES de las ventas conceptualmente)
  transactions.filter(tx => tx.date === targetDate && tx.type === 'buy').forEach(tx => {
    unitsBeforeSales += tx.amount || 0;
    investmentBeforeSales += (tx.amount || 0) * (tx.price || 0);
  });
  
  // Calcular costo promedio por unidad
  const avgCostPerUnit = unitsBeforeSales > 0 ? investmentBeforeSales / unitsBeforeSales : 0;
  
  // Calcular P&L de cada venta
  let totalDonePnL = 0;
  
  salesOnDate.forEach(sellTx => {
    // Si la transacción tiene valuePnL precalculado, usarlo
    if (sellTx.valuePnL !== undefined && sellTx.valuePnL !== null) {
      totalDonePnL += sellTx.valuePnL;
    } else {
      // Calcular manualmente: (precio venta - costo promedio) * unidades
      const saleProceeds = (sellTx.amount || 0) * (sellTx.price || 0);
      const costBasis = (sellTx.amount || 0) * avgCostPerUnit;
      totalDonePnL += saleProceeds - costBasis;
    }
  });
  
  return totalDonePnL;
}

/**
 * Verificar si hay ventas en una fecha específica
 */
function hasSalesOnDate(transactions, targetDate) {
  return transactions.some(tx => tx.date === targetDate && tx.type === 'sell');
}

// ============================================================================
// ANÁLISIS Y CORRECCIÓN
// ============================================================================

/**
 * Analizar discrepancias para un conjunto de documentos
 * 
 * IMPORTANTE: El TWR debe calcularse usando:
 * - previousValue: del documento ANTERIOR (corregido si fue modificado, o de Firebase)
 * - cashflow: ACUMULADO desde el día siguiente al doc anterior hasta hoy
 * 
 * CORRECCIÓN DE BUG: Si el documento anterior fue corregido en esta misma ejecución,
 * usamos el valor corregido en lugar del valor de Firebase para calcular el TWR.
 * Esto evita TWR inflados artificialmente cuando hay múltiples días con datos incorrectos.
 * 
 * @param {boolean} useHistoricalFx - Si true, obtiene tipos de cambio históricos reales
 * @param {boolean} fixPnL - Si true, incluye documentos con P&L faltante aunque TWR esté correcto
 */
async function analyzeDiscrepancies(docs, transactions, priceHistory, assetKey, threshold, useHistoricalFx = false, fixPnL = false) {
  const discrepancies = [];
  
  // Cache de tipos de cambio por fecha (para no repetir llamadas)
  const fxCache = new Map();
  
  // Cache de valores corregidos por fecha (para usar en cálculos de TWR posteriores)
  // Esto es crítico: si corregimos el documento del día X, el TWR del día X+1 debe
  // calcularse usando el valor corregido, no el valor incorrecto de Firebase
  const correctedValuesCache = new Map();
  
  // Necesitamos el documento anterior para calcular TWR correctamente
  let previousDoc = null;
  let previousDocDate = null;
  
  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    const docData = doc.data();
    const date = docData.date;
    
    // Verificar si hay precio para esta fecha
    if (!priceHistory[date]) continue;
    
    const price = priceHistory[date];
    const holdings = calculateHoldingsAtDate(transactions, date);
    
    // Si no hay unidades, resetear y continuar
    if (holdings.units < 0.00001) {
      previousDoc = null;
      previousDocDate = null;
      continue;
    }
    
    // Obtener datos almacenados en USD
    const storedUSD = docData.USD?.assetPerformance?.[assetKey];
    
    if (!storedUSD) {
      // Guardar este doc como el anterior para el siguiente
      previousDoc = docData;
      previousDocDate = date;
      continue;
    }
    
    // Calcular valor actual con precio real
    const currentValueUSD = holdings.units * price;
    
    // Obtener previousValue: PRIMERO verificar si fue corregido, si no usar Firebase
    // Este es el fix del bug: si el documento anterior fue corregido, usar ese valor
    let previousValueUSD = null;
    if (previousDocDate && correctedValuesCache.has(previousDocDate)) {
      // Usar el valor corregido del documento anterior
      previousValueUSD = correctedValuesCache.get(previousDocDate);
    } else {
      // Usar el valor de Firebase del documento anterior
      previousValueUSD = previousDoc?.USD?.assetPerformance?.[assetKey]?.totalValue || null;
    }
    
    // Calcular cashflow ACUMULADO desde el documento anterior hasta hoy
    // Esto incluye transacciones de días sin documento (fines de semana, festivos)
    const accumulatedCashFlow = previousDocDate 
      ? calculateAccumulatedCashFlow(transactions, previousDocDate, date)
      : calculateDailyCashFlow(transactions, date);
    
    // Calcular métricas
    const totalInvestmentUSD = holdings.totalInvestment;
    const unrealizedPnLUSD = currentValueUSD - totalInvestmentUSD;
    const totalROI = totalInvestmentUSD > 0 ? (unrealizedPnLUSD / totalInvestmentUSD) * 100 : 0;
    
    // Calcular cambios diarios
    let dailyChangePercentage = 0;
    let adjustedDailyChangePercentage = 0;
    
    if (previousValueUSD !== null && previousValueUSD > 0) {
      // Cambio simple (sin ajuste por cashflow)
      dailyChangePercentage = ((currentValueUSD - previousValueUSD) / previousValueUSD) * 100;
      // TWR: ajustado por cashflow acumulado
      adjustedDailyChangePercentage = ((currentValueUSD - previousValueUSD + accumulatedCashFlow) / previousValueUSD) * 100;
    }
    
    // Calcular P&L realizado si hay ventas este día
    const doneProfitAndLossUSD = calculateDoneProfitAndLoss(transactions, date);
    const hasSales = hasSalesOnDate(transactions, date);
    
    const calculated = {
      totalValueUSD: currentValueUSD,
      totalInvestmentUSD,
      unrealizedPnLUSD,
      totalROI,
      dailyChangePercentage,
      adjustedDailyChangePercentage,
      rawDailyChangePercentage: dailyChangePercentage,
      units: holdings.units,
      dailyCashFlowUSD: accumulatedCashFlow,
      doneProfitAndLossUSD,
      hasSales,
    };
    
    // Comparar adjustedDailyChangePercentage (el más importante para TWR)
    const adjDiff = Math.abs((calculated.adjustedDailyChangePercentage || 0) - (storedUSD.adjustedDailyChangePercentage || 0));
    const valueDiff = Math.abs(calculated.totalValueUSD - (storedUSD.totalValue || 0));
    const valueDiffPct = storedUSD.totalValue > 0 ? (valueDiff / storedUSD.totalValue) * 100 : 0;
    
    // Verificar si falta doneProfitAndLoss cuando hay ventas
    const missingDonePnL = hasSales && storedUSD.doneProfitAndLoss === undefined;
    
    // Incluir si hay discrepancia en TWR/valor O si falta doneProfitAndLoss y fixPnL está activado
    const hasTWRDiscrepancy = adjDiff > threshold || valueDiffPct > threshold;
    const needsPnLFix = fixPnL && missingDonePnL;
    
    if (hasTWRDiscrepancy || needsPnLFix) {
      // Obtener tipos de cambio (históricos o derivados del documento)
      let exchangeRates;
      
      if (useHistoricalFx) {
        // Usar tipos de cambio históricos reales
        if (!fxCache.has(date)) {
          log('DEBUG', `Obteniendo FX histórico para ${date}...`);
          fxCache.set(date, await fetchExchangeRatesForDate(date));
        }
        exchangeRates = fxCache.get(date);
      } else {
        // Derivar tipos de cambio del documento existente
        exchangeRates = deriveExchangeRatesFromDoc(docData);
      }
      
      discrepancies.push({
        docRef: doc.ref,
        date,
        price,
        holdings,
        accumulatedCashFlow,
        calculated,
        storedUSD,
        exchangeRates,
        adjDiff,
        valueDiff,
        valueDiffPct,
        previousDocDate,
        hasTWRDiscrepancy,
        missingDonePnL,
      });
      
      // IMPORTANTE: Guardar el valor corregido para que los documentos siguientes
      // puedan calcular su TWR correctamente
      correctedValuesCache.set(date, currentValueUSD);
    } else {
      // Aunque no haya discrepancia, guardar el valor actual para referencia
      // Solo si el valor de Firebase es correcto (o muy cercano)
      // Esto asegura consistencia en la cadena de cálculos
      if (Math.abs(currentValueUSD - (storedUSD.totalValue || 0)) < 0.01) {
        correctedValuesCache.set(date, storedUSD.totalValue);
      } else {
        correctedValuesCache.set(date, currentValueUSD);
      }
    }
    
    // Guardar este doc como el anterior para el siguiente
    previousDoc = docData;
    previousDocDate = date;
  }
  
  return discrepancies;
}

/**
 * Generar actualizaciones para Firestore
 * Actualiza el asset en todas las monedas usando los tipos de cambio derivados
 */
function generateUpdates(discrepancies, assetKey) {
  const updates = [];
  
  discrepancies.forEach(d => {
    const updateData = {};
    const calc = d.calculated;
    const rates = d.exchangeRates;
    
    CONFIG.CURRENCIES.forEach(currency => {
      const rate = rates[currency] || 1;
      const isUSD = currency === 'USD';
      
      // Solo actualizar el asset específico dentro de assetPerformance
      const assetUpdate = {
        totalValue: isUSD ? calc.totalValueUSD : calc.totalValueUSD * rate,
        totalInvestment: isUSD ? calc.totalInvestmentUSD : calc.totalInvestmentUSD * rate,
        totalROI: calc.totalROI,
        // Los porcentajes son iguales para todas las monedas
        dailyChangePercentage: calc.dailyChangePercentage,
        adjustedDailyChangePercentage: calc.adjustedDailyChangePercentage,
        rawDailyChangePercentage: calc.rawDailyChangePercentage,
        totalCashFlow: isUSD ? calc.dailyCashFlowUSD : calc.dailyCashFlowUSD * rate,
        units: calc.units,
        unrealizedProfitAndLoss: isUSD ? calc.unrealizedPnLUSD : calc.unrealizedPnLUSD * rate,
        // Preservar campos existentes que requieren cálculos más complejos
        dailyReturn: d.storedUSD?.dailyReturn || 0,
        monthlyReturn: d.storedUSD?.monthlyReturn || 0,
        annualReturn: d.storedUSD?.annualReturn || 0,
      };
      
      // Agregar doneProfitAndLoss solo si hay ventas este día
      // (es un campo diario, no acumulativo)
      if (calc.hasSales) {
        assetUpdate.doneProfitAndLoss = isUSD ? calc.doneProfitAndLossUSD : calc.doneProfitAndLossUSD * rate;
      } else {
        // Si no hay ventas, establecer en 0 (o preservar el valor existente si es 0)
        assetUpdate.doneProfitAndLoss = d.storedUSD?.doneProfitAndLoss || 0;
      }
      
      updateData[`${currency}.assetPerformance.${assetKey}`] = assetUpdate;
    });
    
    if (Object.keys(updateData).length > 0) {
      updates.push({
        ref: d.docRef,
        date: d.date,
        data: updateData,
      });
    }
  });
  
  return updates;
}

/**
 * Aplicar actualizaciones a Firestore
 */
async function applyUpdates(updates) {
  const batches = [];
  let currentBatch = db.batch();
  let operationsInBatch = 0;
  
  for (const update of updates) {
    currentBatch.update(update.ref, update.data);
    operationsInBatch++;
    
    if (operationsInBatch >= CONFIG.BATCH_SIZE) {
      batches.push(currentBatch);
      currentBatch = db.batch();
      operationsInBatch = 0;
    }
  }
  
  if (operationsInBatch > 0) {
    batches.push(currentBatch);
  }
  
  log('INFO', `Aplicando ${updates.length} actualizaciones en ${batches.length} batches...`);
  
  for (let i = 0; i < batches.length; i++) {
    await batches[i].commit();
    log('SUCCESS', `Batch ${i + 1}/${batches.length} completado`);
    if (i < batches.length - 1) {
      await sleep(CONFIG.API_DELAY_MS);
    }
  }
  
  return updates.length;
}

// ============================================================================
// PROCESO PRINCIPAL
// ============================================================================

async function main() {
  const options = parseArgs();
  
  if (!options.assetKey) {
    console.log('❌ Debes proporcionar el asset key como argumento');
    console.log('');
    console.log('Uso: node fixAssetPerformance.js <ASSET_KEY> <MODO> [opciones]');
    console.log('');
    console.log('Modos:');
    console.log('  --analyze    Solo analiza discrepancias (default)');
    console.log('  --dry-run    Muestra cambios sin aplicar');
    console.log('  --fix        Aplica los cambios a Firestore');
    console.log('');
    console.log('Opciones:');
    console.log('  --user=<userId>        Usuario específico');
    console.log('  --start=YYYY-MM-DD     Fecha inicio');
    console.log('  --end=YYYY-MM-DD       Fecha fin');
    console.log('  --threshold=<number>   Umbral de discrepancia en % (default: 0.5)');
    console.log('  --use-historical-fx    Obtener tipos de cambio históricos reales de Yahoo Finance');
    console.log('                         (por defecto usa los tipos de cambio del documento existente)');
    console.log('  --fix-pnl              También corregir doneProfitAndLoss en días con ventas');
    console.log('                         (incluye documentos aunque TWR esté correcto)');
    console.log('');
    console.log('Ejemplos:');
    console.log('  node fixAssetPerformance.js AMZN_stock --analyze');
    console.log('  node fixAssetPerformance.js AMZN_stock --dry-run --start=2025-03-01');
    console.log('  node fixAssetPerformance.js AMZN_stock --fix --threshold=1.0');
    console.log('  node fixAssetPerformance.js AMZN_stock --fix --use-historical-fx');
    console.log('  node fixAssetPerformance.js AMZN_stock --fix --fix-pnl  # Corregir P&L faltante');
    process.exit(1);
  }

  const [symbol, assetType] = options.assetKey.split('_');
  
  console.log('');
  console.log('='.repeat(100));
  console.log(`CORRECCIÓN DE ASSET PERFORMANCE: ${options.assetKey}`);
  console.log('='.repeat(100));
  console.log(`Modo: ${options.mode.toUpperCase()}`);
  console.log(`Usuario: ${options.userId}`);
  console.log(`Período: ${options.startDate} a ${options.endDate}`);
  console.log(`Umbral de discrepancia: ${options.threshold}%`);
  console.log(`Tipos de cambio: ${options.useHistoricalFx ? 'Históricos (Yahoo Finance)' : 'Derivados del documento'}`);
  console.log(`Fix P&L: ${options.fixPnL ? 'Sí (incluye doneProfitAndLoss faltante)' : 'No'}`);
  console.log('');

  // 1. Obtener transacciones
  log('INFO', 'Obteniendo transacciones...');
  const txData = await getAssetTransactions(options.userId, symbol);
  log('SUCCESS', `Encontradas ${txData.all.length} transacciones en ${txData.byAccount.size} cuentas`);
  
  if (txData.all.length === 0) {
    log('WARNING', 'No hay transacciones para este asset');
    process.exit(0);
  }

  // 2. Obtener precios históricos
  const priceHistory = await fetchHistoricalPrices(symbol, options.startDate);
  
  if (Object.keys(priceHistory).length === 0) {
    log('ERROR', 'No se pudieron obtener precios históricos');
    process.exit(1);
  }

  // 3. Analizar OVERALL
  console.log('');
  console.log('-'.repeat(100));
  log('INFO', 'Analizando OVERALL...');
  
  const overallDocs = await getPerformanceDocuments(options.userId, null, options.startDate, options.endDate);
  log('INFO', `Encontrados ${overallDocs.length} documentos OVERALL`);
  
  const overallDiscrepancies = await analyzeDiscrepancies(
    overallDocs, 
    txData.all, 
    priceHistory, 
    options.assetKey, 
    options.threshold,
    options.useHistoricalFx,
    options.fixPnL
  );
  
  log('INFO', `Discrepancias encontradas en OVERALL: ${overallDiscrepancies.length}`);

  // 4. Analizar por cuenta
  const accountDiscrepancies = new Map();
  
  for (const [accountId, accountTx] of txData.byAccount) {
    console.log('');
    log('INFO', `Analizando cuenta ${accountId}...`);
    
    const accountDocs = await getPerformanceDocuments(options.userId, accountId, options.startDate, options.endDate);
    
    if (accountDocs.length === 0) {
      log('SKIP', `No hay documentos para cuenta ${accountId}`);
      continue;
    }
    
    log('INFO', `Encontrados ${accountDocs.length} documentos para cuenta ${accountId}`);
    
    const discrepancies = await analyzeDiscrepancies(
      accountDocs,
      accountTx,
      priceHistory,
      options.assetKey,
      options.threshold,
      options.useHistoricalFx,
      options.fixPnL
    );
    
    if (discrepancies.length > 0) {
      accountDiscrepancies.set(accountId, discrepancies);
      log('INFO', `Discrepancias encontradas en cuenta ${accountId}: ${discrepancies.length}`);
    } else {
      log('SUCCESS', `Sin discrepancias en cuenta ${accountId}`);
    }
  }

  // 5. Resumen
  console.log('');
  console.log('='.repeat(100));
  console.log('RESUMEN DE DISCREPANCIAS');
  console.log('='.repeat(100));
  
  const totalDiscrepancies = overallDiscrepancies.length + 
    Array.from(accountDiscrepancies.values()).reduce((sum, d) => sum + d.length, 0);
  
  console.log(`OVERALL: ${overallDiscrepancies.length} documentos con discrepancias`);
  accountDiscrepancies.forEach((discrepancies, accountId) => {
    console.log(`Cuenta ${accountId}: ${discrepancies.length} documentos con discrepancias`);
  });
  console.log(`TOTAL: ${totalDiscrepancies} documentos a corregir`);
  console.log('');

  if (totalDiscrepancies === 0) {
    log('SUCCESS', 'No se encontraron discrepancias significativas');
    process.exit(0);
  }

  // 5. Modo Analyze - solo mostrar
  if (options.mode === 'analyze') {
    log('INFO', 'Modo ANALYZE: Solo se muestran las discrepancias');
    
    // Mostrar algunas discrepancias de ejemplo
    if (overallDiscrepancies.length > 0) {
      console.log('');
      console.log('Ejemplo de discrepancias OVERALL (primeras 5):');
      overallDiscrepancies.slice(0, 5).forEach(d => {
        console.log(`  ${d.date}: price=$${d.price.toFixed(2)}, units=${d.holdings.units.toFixed(4)}`);
        console.log(`    USD: adjChange stored=${d.storedUSD?.adjustedDailyChangePercentage?.toFixed(4)}% calc=${d.calculated.adjustedDailyChangePercentage?.toFixed(4)}% (diff=${d.adjDiff.toFixed(4)}%)`);
        console.log(`    Value: stored=$${d.storedUSD?.totalValue?.toFixed(2)} calc=$${d.calculated.totalValueUSD?.toFixed(2)} (diff=${d.valueDiffPct.toFixed(2)}%)`);
      });
    }
    
    console.log('');
    log('INFO', 'Ejecuta con --dry-run para ver los cambios propuestos');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones');
    process.exit(0);
  }

  // 6. Generar actualizaciones
  log('INFO', 'Generando actualizaciones...');
  
  const allUpdates = [];
  
  // Updates para OVERALL
  const overallUpdates = generateUpdates(overallDiscrepancies, options.assetKey);
  allUpdates.push(...overallUpdates);
  
  // Updates para cada cuenta
  accountDiscrepancies.forEach((discrepancies, accountId) => {
    const accountUpdates = generateUpdates(discrepancies, options.assetKey);
    allUpdates.push(...accountUpdates);
  });
  
  log('INFO', `Total de actualizaciones generadas: ${allUpdates.length}`);

  // 7. Modo Dry-run - mostrar cambios sin aplicar
  if (options.mode === 'dry-run') {
    console.log('');
    console.log('='.repeat(100));
    console.log('MODO DRY-RUN: Cambios propuestos (no aplicados)');
    console.log('='.repeat(100));
    
    // Mostrar primeras actualizaciones de ejemplo
    const samplesToShow = Math.min(10, allUpdates.length);
    console.log(`\nMostrando ${samplesToShow} de ${allUpdates.length} actualizaciones:\n`);
    
    allUpdates.slice(0, samplesToShow).forEach((update, idx) => {
      console.log(`${idx + 1}. ${update.ref.path} (${update.date})`);
      Object.entries(update.data).forEach(([key, value]) => {
        if (typeof value === 'object') {
          console.log(`   ${key}:`);
          console.log(`     adjustedDailyChangePercentage: ${value.adjustedDailyChangePercentage?.toFixed(6)}%`);
          console.log(`     totalValue: $${value.totalValue?.toFixed(2)}`);
          console.log(`     units: ${value.units?.toFixed(4)}`);
          if (value.doneProfitAndLoss !== undefined && value.doneProfitAndLoss !== 0) {
            console.log(`     doneProfitAndLoss: $${value.doneProfitAndLoss?.toFixed(2)}`);
          }
          console.log(`     unrealizedProfitAndLoss: $${value.unrealizedProfitAndLoss?.toFixed(2)}`);
        }
      });
      console.log('');
    });
    
    if (allUpdates.length > samplesToShow) {
      console.log(`... y ${allUpdates.length - samplesToShow} actualizaciones más`);
    }
    
    console.log('');
    log('WARNING', 'Modo DRY-RUN: No se aplicaron cambios');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones');
    process.exit(0);
  }

  // 8. Modo Fix - aplicar cambios
  if (options.mode === 'fix') {
    console.log('');
    console.log('='.repeat(100));
    console.log('MODO FIX: Aplicando correcciones');
    console.log('='.repeat(100));
    
    // Confirmación de seguridad
    console.log('');
    log('WARNING', `Se van a modificar ${allUpdates.length} documentos`);
    log('WARNING', 'Esta operación solo modifica los campos del asset específico');
    log('WARNING', 'Los demás datos del documento NO serán afectados');
    console.log('');
    
    // Aplicar
    const applied = await applyUpdates(allUpdates);
    
    console.log('');
    log('SUCCESS', `✅ Se aplicaron ${applied} correcciones exitosamente`);
    
    // Resumen final
    console.log('');
    console.log('='.repeat(100));
    console.log('RESUMEN DE CORRECCIONES APLICADAS');
    console.log('='.repeat(100));
    console.log(`Asset: ${options.assetKey}`);
    console.log(`Usuario: ${options.userId}`);
    console.log(`Período: ${options.startDate} a ${options.endDate}`);
    console.log(`Documentos corregidos: ${applied}`);
    console.log(`  - OVERALL: ${overallUpdates.length}`);
    accountDiscrepancies.forEach((discrepancies, accountId) => {
      console.log(`  - Cuenta ${accountId}: ${discrepancies.length}`);
    });
    console.log('');
    log('INFO', 'Ejecuta verifyAssetTWR.js para verificar las correcciones');
  }

  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
