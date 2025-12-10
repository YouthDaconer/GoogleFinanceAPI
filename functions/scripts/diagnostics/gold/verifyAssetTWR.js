/**
 * Script de Verificación de TWR para un Asset Específico
 * 
 * PROPÓSITO:
 * Verificar si los datos de rendimiento TWR almacenados en portfolioPerformance
 * son coherentes con:
 * - Precios históricos reales de activos (API finance-query)
 * - Transacciones históricas del usuario (Firestore)
 * 
 * NO hace backfill, solo VERIFICA y compara.
 * 
 * USO:
 *   node verifyAssetTWR.js AMZN_stock
 *   node verifyAssetTWR.js AMZN_stock --start=2025-03-01 --end=2025-12-09
 *   node verifyAssetTWR.js AMZN_stock --verbose
 * 
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
  
  // Festivos NYSE 2025
  NYSE_HOLIDAYS_2025: [
    '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18',
    '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01',
    '2025-11-27', '2025-12-25'
  ],
};

// ============================================================================
// UTILIDADES
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    assetKey: null,
    userId: CONFIG.DEFAULT_USER_ID,
    startDate: '2025-01-01',
    endDate: DateTime.now().setZone('America/New_York').toISODate(),
    verbose: false,
  };

  args.forEach(arg => {
    if (arg === '--verbose') options.verbose = true;
    else if (arg.startsWith('--user=')) options.userId = arg.split('=')[1];
    else if (arg.startsWith('--start=')) options.startDate = arg.split('=')[1];
    else if (arg.startsWith('--end=')) options.endDate = arg.split('=')[1];
    else if (!arg.startsWith('--')) options.assetKey = arg;
  });

  return options;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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
    console.log(`📡 Obteniendo precios históricos de ${symbol} (${range})...`);
    
    const response = await fetch(url);
    if (!response.ok) {
      console.log(`⚠️ No se pudieron obtener precios para ${symbol}: ${response.status}`);
      return {};
    }
    
    const data = await response.json();
    const priceMap = {};
    Object.entries(data).forEach(([date, ohlcv]) => {
      priceMap[date] = ohlcv.close;
    });
    
    console.log(`✅ Obtenidos ${Object.keys(priceMap).length} precios históricos para ${symbol}`);
    return priceMap;
  } catch (error) {
    console.log(`❌ Error obteniendo precios para ${symbol}:`, error.message);
    return {};
  }
}

/**
 * Obtener transacciones del usuario para un asset específico
 * Usa portfolioAccountId para obtener transacciones de las cuentas del usuario
 */
async function getAssetTransactions(userId, assetName) {
  // Primero obtener las cuentas del usuario
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .get();
  
  const userAccountIds = accountsSnapshot.docs.map(doc => doc.id);
  console.log(`   Cuentas del usuario: ${userAccountIds.join(', ')}`);
  
  // Obtener todas las transacciones del asset
  const snapshot = await db.collection('transactions')
    .where('assetName', '==', assetName)
    .get();
  
  // Filtrar por cuentas del usuario y ordenar
  return snapshot.docs
    .map(doc => ({ id: doc.id, ...doc.data() }))
    .filter(tx => userAccountIds.includes(tx.portfolioAccountId))
    .sort((a, b) => a.date.localeCompare(b.date));
}

/**
 * Obtener datos de performance almacenados (OVERALL)
 */
async function getStoredPerformance(userId, startDate, endDate) {
  const snapshot = await db.collection(`portfolioPerformance/${userId}/dates`)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  const data = new Map();
  snapshot.docs.forEach(doc => {
    data.set(doc.data().date, doc.data());
  });
  
  return data;
}

// ============================================================================
// CÁLCULO DE HOLDINGS
// ============================================================================

/**
 * Calcular holdings hasta una fecha específica
 */
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

/**
 * Calcular cash flow del día para el asset
 */
function calculateDailyCashFlow(transactions, targetDate) {
  return transactions
    .filter(tx => tx.date === targetDate)
    .reduce((sum, tx) => {
      if (tx.type === 'buy') return sum - (tx.amount || 0) * (tx.price || 0);
      if (tx.type === 'sell') return sum + (tx.amount || 0) * (tx.price || 0);
      return sum;
    }, 0);
}

// ============================================================================
// VERIFICACIÓN PRINCIPAL
// ============================================================================

async function verify() {
  const options = parseArgs();
  
  if (!options.assetKey) {
    console.log('❌ Debes proporcionar el asset key como argumento');
    console.log('');
    console.log('Uso: node verifyAssetTWR.js <ASSET_KEY> [opciones]');
    console.log('');
    console.log('Ejemplos:');
    console.log('  node verifyAssetTWR.js AMZN_stock');
    console.log('  node verifyAssetTWR.js AMZN_stock --start=2025-03-01 --verbose');
    process.exit(1);
  }

  const [symbol, assetType] = options.assetKey.split('_');
  
  console.log('='.repeat(100));
  console.log(`VERIFICACIÓN TWR: ${options.assetKey}`);
  console.log('='.repeat(100));
  console.log(`Usuario: ${options.userId}`);
  console.log(`Período: ${options.startDate} a ${options.endDate}`);
  console.log('');

  // 1. Obtener transacciones del usuario para este asset
  console.log('📋 Obteniendo transacciones...');
  const transactions = await getAssetTransactions(options.userId, symbol);
  console.log(`   Encontradas ${transactions.length} transacciones para ${symbol}`);
  
  if (transactions.length === 0) {
    console.log('⚠️ No hay transacciones para este asset');
    process.exit(0);
  }

  // Mostrar resumen de transacciones
  console.log('');
  console.log('📊 TRANSACCIONES:');
  console.log('-'.repeat(100));
  console.log('Fecha       | Tipo  | Unidades | Precio   | Total USD  | Account ID');
  console.log('-'.repeat(100));
  transactions.forEach(tx => {
    const total = (tx.amount || 0) * (tx.price || 0);
    console.log(`${tx.date} | ${tx.type.padEnd(5)} | ${(tx.amount || 0).toFixed(4).padStart(8)} | $${(tx.price || 0).toFixed(2).padStart(7)} | $${total.toFixed(2).padStart(9)} | ${tx.portfolioAccountId}`);
  });
  console.log('');

  // 2. Obtener precios históricos
  const priceHistory = await fetchHistoricalPrices(symbol, options.startDate);
  
  if (Object.keys(priceHistory).length === 0) {
    console.log('❌ No se pudieron obtener precios históricos');
    process.exit(1);
  }

  // 3. Obtener datos de performance almacenados
  console.log('');
  console.log('📋 Obteniendo datos de performance almacenados...');
  const storedPerformance = await getStoredPerformance(options.userId, options.startDate, options.endDate);
  console.log(`   Encontrados ${storedPerformance.size} días de performance`);
  console.log('');

  // 4. Calcular y comparar TWR día a día
  console.log('='.repeat(100));
  console.log('COMPARACIÓN: CALCULADO vs ALMACENADO');
  console.log('='.repeat(100));
  console.log('');
  console.log('Fecha       | Units    | Precio   | Valor Calc | Valor Alm  | Adj% Calc | Adj% Alm  | CF        | Δ Valor  | Δ Adj%   | Estado');
  console.log('-'.repeat(150));

  let previousValue = null;
  let previousHoldings = null;
  let totalDiscrepancies = 0;
  let totalDays = 0;
  
  // Factor para TWR calculado
  let calculatedTWRFactor = 1;
  let storedTWRFactor = 1;
  
  // Obtener fechas ordenadas de los precios disponibles
  const priceDates = Object.keys(priceHistory).sort();
  
  // Fechas de períodos para cálculo
  const now = DateTime.now().setZone('America/New_York');
  const oneMonthAgo = now.minus({ months: 1 }).toISODate();
  const threeMonthsAgo = now.minus({ months: 3 }).toISODate();
  const sixMonthsAgo = now.minus({ months: 6 }).toISODate();
  const startOfYear = now.startOf('year').toISODate();

  // Factores por período
  let calcFactors = {
    ytd: { start: null, current: 1 },
    oneMonth: { start: null, current: 1 },
    threeMonths: { start: null, current: 1 },
    sixMonths: { start: null, current: 1 }
  };
  let storedFactors = {
    ytd: { start: null, current: 1 },
    oneMonth: { start: null, current: 1 },
    threeMonths: { start: null, current: 1 },
    sixMonths: { start: null, current: 1 }
  };

  for (const date of priceDates) {
    if (date < options.startDate || date > options.endDate) continue;
    
    const price = priceHistory[date];
    const holdings = calculateHoldingsAtDate(transactions, date);
    
    // Si no hay unidades, saltar
    if (holdings.units < 0.00001) {
      previousValue = null;
      previousHoldings = null;
      continue;
    }
    
    totalDays++;
    
    // Valor calculado
    const calculatedValue = holdings.units * price;
    
    // Cash flow del día
    const dailyCashFlow = calculateDailyCashFlow(transactions, date);
    
    // Cambio ajustado calculado (TWR)
    let calculatedAdjChange = 0;
    if (previousValue !== null && previousValue > 0) {
      // Fórmula TWR: (endValue - startValue + cashFlow) / startValue * 100
      // cashFlow es NEGATIVO para compras, POSITIVO para ventas
      calculatedAdjChange = ((calculatedValue - previousValue + dailyCashFlow) / previousValue) * 100;
    }
    
    // Datos almacenados
    const storedData = storedPerformance.get(date);
    const storedAssetData = storedData?.USD?.assetPerformance?.[options.assetKey];
    const storedValue = storedAssetData?.totalValue || 0;
    const storedAdjChange = storedAssetData?.adjustedDailyChangePercentage || 0;
    const storedUnits = storedAssetData?.units || 0;
    
    // Calcular diferencias
    const valueDiff = storedValue > 0 ? calculatedValue - storedValue : 0;
    const adjDiff = storedAdjChange !== 0 ? calculatedAdjChange - storedAdjChange : 0;
    
    // Actualizar factores TWR
    if (previousValue !== null) {
      calculatedTWRFactor *= (1 + calculatedAdjChange / 100);
    }
    if (storedAdjChange !== 0) {
      storedTWRFactor *= (1 + storedAdjChange / 100);
    }
    
    // Actualizar factores por período
    if (date >= startOfYear) {
      if (calcFactors.ytd.start === null) calcFactors.ytd.start = calculatedTWRFactor;
      if (storedFactors.ytd.start === null) storedFactors.ytd.start = storedTWRFactor;
    }
    if (date >= sixMonthsAgo) {
      if (calcFactors.sixMonths.start === null) calcFactors.sixMonths.start = calculatedTWRFactor;
      if (storedFactors.sixMonths.start === null) storedFactors.sixMonths.start = storedTWRFactor;
    }
    if (date >= threeMonthsAgo) {
      if (calcFactors.threeMonths.start === null) calcFactors.threeMonths.start = calculatedTWRFactor;
      if (storedFactors.threeMonths.start === null) storedFactors.threeMonths.start = storedTWRFactor;
    }
    if (date >= oneMonthAgo) {
      if (calcFactors.oneMonth.start === null) calcFactors.oneMonth.start = calculatedTWRFactor;
      if (storedFactors.oneMonth.start === null) storedFactors.oneMonth.start = storedTWRFactor;
    }
    
    // Determinar estado
    let status = '✅';
    if (Math.abs(valueDiff) > 1 || Math.abs(adjDiff) > 0.5) {
      status = '⚠️ DIFF';
      totalDiscrepancies++;
    } else if (!storedData || !storedAssetData) {
      status = '❓ NO DATA';
    }
    
    // Mostrar si hay discrepancia, es verbose, o es un día con transacciones
    const hasTx = dailyCashFlow !== 0;
    const showLine = options.verbose || status !== '✅' || hasTx;
    
    if (showLine) {
      console.log(
        `${date} | ${holdings.units.toFixed(4).padStart(8)} | $${price.toFixed(2).padStart(7)} | ` +
        `$${calculatedValue.toFixed(2).padStart(9)} | $${storedValue.toFixed(2).padStart(9)} | ` +
        `${calculatedAdjChange.toFixed(4).padStart(9)}% | ${storedAdjChange.toFixed(4).padStart(9)}% | ` +
        `$${dailyCashFlow.toFixed(2).padStart(8)} | ` +
        `${valueDiff >= 0 ? '+' : ''}${valueDiff.toFixed(2).padStart(7)} | ` +
        `${adjDiff >= 0 ? '+' : ''}${adjDiff.toFixed(4).padStart(7)}% | ${status}`
      );
    }
    
    previousValue = calculatedValue;
    previousHoldings = holdings;
  }

  // Actualizar factores finales
  calcFactors.ytd.current = calculatedTWRFactor;
  calcFactors.sixMonths.current = calculatedTWRFactor;
  calcFactors.threeMonths.current = calculatedTWRFactor;
  calcFactors.oneMonth.current = calculatedTWRFactor;
  storedFactors.ytd.current = storedTWRFactor;
  storedFactors.sixMonths.current = storedTWRFactor;
  storedFactors.threeMonths.current = storedTWRFactor;
  storedFactors.oneMonth.current = storedTWRFactor;

  // ============================================================================
  // RESUMEN DE RENDIMIENTOS
  // ============================================================================
  console.log('');
  console.log('='.repeat(100));
  console.log('RESUMEN DE RENDIMIENTOS TWR');
  console.log('='.repeat(100));
  console.log('');
  
  // Calcular rendimientos
  const calcYTD = calcFactors.ytd.start ? (calcFactors.ytd.current / calcFactors.ytd.start - 1) * 100 : 0;
  const calcSixMonth = calcFactors.sixMonths.start ? (calcFactors.sixMonths.current / calcFactors.sixMonths.start - 1) * 100 : 0;
  const calcThreeMonth = calcFactors.threeMonths.start ? (calcFactors.threeMonths.current / calcFactors.threeMonths.start - 1) * 100 : 0;
  const calcOneMonth = calcFactors.oneMonth.start ? (calcFactors.oneMonth.current / calcFactors.oneMonth.start - 1) * 100 : 0;
  
  const storedYTD = storedFactors.ytd.start ? (storedFactors.ytd.current / storedFactors.ytd.start - 1) * 100 : 0;
  const storedSixMonth = storedFactors.sixMonths.start ? (storedFactors.sixMonths.current / storedFactors.sixMonths.start - 1) * 100 : 0;
  const storedThreeMonth = storedFactors.threeMonths.start ? (storedFactors.threeMonths.current / storedFactors.threeMonths.start - 1) * 100 : 0;
  const storedOneMonth = storedFactors.oneMonth.start ? (storedFactors.oneMonth.current / storedFactors.oneMonth.start - 1) * 100 : 0;
  
  console.log('Período    | Calculado (Precios Reales) | Almacenado (Firebase) | Diferencia | Estado');
  console.log('-'.repeat(100));
  
  const periods = [
    { name: '1M', calc: calcOneMonth, stored: storedOneMonth },
    { name: '3M', calc: calcThreeMonth, stored: storedThreeMonth },
    { name: '6M', calc: calcSixMonth, stored: storedSixMonth },
    { name: 'YTD', calc: calcYTD, stored: storedYTD },
  ];
  
  periods.forEach(p => {
    const diff = p.calc - p.stored;
    const status = Math.abs(diff) < 0.5 ? '✅ OK' : (Math.abs(diff) < 2 ? '⚠️ MINOR' : '❌ MAJOR');
    console.log(
      `${p.name.padEnd(10)} | ${p.calc.toFixed(2).padStart(25)}% | ${p.stored.toFixed(2).padStart(21)}% | ` +
      `${diff >= 0 ? '+' : ''}${diff.toFixed(2).padStart(9)}% | ${status}`
    );
  });

  console.log('');
  console.log('='.repeat(100));
  console.log('DIAGNÓSTICO FINAL');
  console.log('='.repeat(100));
  console.log('');
  console.log(`📊 Total días analizados: ${totalDays}`);
  console.log(`⚠️ Días con discrepancias: ${totalDiscrepancies}`);
  console.log(`📈 Factor TWR final calculado: ${calculatedTWRFactor.toFixed(6)}`);
  console.log(`📈 Factor TWR final almacenado: ${storedTWRFactor.toFixed(6)}`);
  
  const factorDiff = ((calculatedTWRFactor / storedTWRFactor) - 1) * 100;
  console.log(`📈 Diferencia de factores: ${factorDiff >= 0 ? '+' : ''}${factorDiff.toFixed(4)}%`);
  console.log('');
  
  if (totalDiscrepancies === 0 && Math.abs(factorDiff) < 1) {
    console.log('✅ Los datos de rendimiento TWR son COHERENTES con los precios históricos');
  } else if (totalDiscrepancies < 5 && Math.abs(factorDiff) < 2) {
    console.log('⚠️ Hay PEQUEÑAS discrepancias, pero los rendimientos finales son razonablemente cercanos');
  } else {
    console.log('❌ Hay DISCREPANCIAS SIGNIFICATIVAS entre los datos calculados y almacenados');
    console.log('   Esto puede deberse a:');
    console.log('   1. Datos de precio diferentes usados en el scheduler original');
    console.log('   2. Transacciones no procesadas correctamente');
    console.log('   3. Problemas de sincronización de datos');
  }

  process.exit(0);
}

verify().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
