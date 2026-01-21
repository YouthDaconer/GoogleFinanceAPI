/**
 * Script para corregir el cashflow de un día específico
 * 
 * PROBLEMA: El 2026-01-08 se compraron 20 unidades de ECOPETROL.CL a 2005 COP cada uno,
 * pero el cashflow quedó registrado como 40100 USD en vez de 40100 COP (~$9.24 USD).
 * 
 * Este script:
 * 1. Obtiene el tipo de cambio USD/COP del día específico
 * 2. Calcula el cashflow correcto en USD
 * 3. Recalcula los valores de portfolioPerformance para ese día
 * 4. Actualiza Firestore
 * 
 * USO:
 *   node fixCashflowDay.js --analyze     # Solo muestra los datos
 *   node fixCashflowDay.js --fix         # Aplica la corrección
 */

const admin = require('firebase-admin');
const fetch = require('node-fetch');

// Inicializar Firebase Admin
const serviceAccount = require('../../key.json');

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
  // URL del Lambda de finance-query (obtenida con AWS CLI)
  LAMBDA_API_URL: 'https://354sdh5hcrnztw5vquw6sxiduu0gigku.lambda-url.us-east-1.on.aws/v1',
  
  // Usuario afectado
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  
  // Fecha a corregir
  TARGET_DATE: '2026-01-08',
  
  // Datos de la transacción incorrecta
  INCORRECT_CASHFLOW_USD: -40100, // Lo que se registró (negativo porque es compra)
  CORRECT_CASHFLOW_COP: -40100,   // El valor real en COP
  
  // Monedas a actualizar
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
};

// ============================================================================
// UTILIDADES
// ============================================================================

function log(level, message, data = null) {
  const prefix = {
    'INFO': '📋',
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'DEBUG': '🔍',
    'CHANGE': '🔄',
  }[level] || '•';
  
  console.log(`${prefix} ${message}`);
  if (data) console.log('   ', JSON.stringify(data, null, 2).split('\n').join('\n    '));
}

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    mode: args.includes('--fix') ? 'fix' : 'analyze'
  };
}

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

/**
 * Obtener tipo de cambio histórico desde Yahoo Finance
 */
async function fetchHistoricalExchangeRate(currency, dateStr) {
  if (currency === 'USD') return 1;
  
  try {
    const date = new Date(dateStr);
    const timestamp = Math.floor(date.getTime() / 1000);
    const nextDay = timestamp + 86400;
    
    // Para COP usamos USDCOP=X
    const symbol = `USD${currency}`;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}%3DX?period1=${timestamp}&period2=${nextDay}&interval=1d`;
    
    log('INFO', `Obteniendo tipo de cambio ${symbol} para ${dateStr}...`);
    
    const response = await fetch(url);
    const data = await response.json();
    
    if (data.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.[0]) {
      const rate = data.chart.result[0].indicators.quote[0].close[0];
      log('SUCCESS', `Tipo de cambio USD/${currency} el ${dateStr}: ${rate.toFixed(2)}`);
      return rate;
    }
    
    // Fallback: intentar con el API de Lambda
    log('WARNING', `No se pudo obtener de Yahoo, intentando con Lambda...`);
    const lambdaUrl = `${CONFIG.LAMBDA_API_URL}/historical-exchange-rate?base=USD&target=${currency}&date=${dateStr}`;
    const lambdaResponse = await fetch(lambdaUrl);
    const lambdaData = await lambdaResponse.json();
    
    if (lambdaData.rate) {
      log('SUCCESS', `Tipo de cambio USD/${currency} el ${dateStr} (Lambda): ${lambdaData.rate}`);
      return lambdaData.rate;
    }
    
    return null;
  } catch (error) {
    log('ERROR', `Error obteniendo tipo de cambio: ${error.message}`);
    return null;
  }
}

/**
 * Obtener documento de performance de un día
 */
async function getPerformanceDoc(userId, date, accountId = null) {
  const path = accountId 
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates/${date}`
    : `portfolioPerformance/${userId}/dates/${date}`;
  
  const doc = await db.doc(path).get();
  return doc.exists ? { ref: doc.ref, data: doc.data() } : null;
}

/**
 * Obtener todas las cuentas del usuario
 */
async function getUserAccounts(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Derivar tipos de cambio desde un documento existente
 */
function deriveExchangeRates(docData) {
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

// ============================================================================
// ANÁLISIS Y CORRECCIÓN
// ============================================================================

/**
 * Analizar el problema en un documento
 */
async function analyzeDocument(doc, usdCopRate) {
  const data = doc.data;
  
  // Cashflow incorrecto (en USD, debería ser COP convertido a USD)
  const incorrectCashflowUSD = data.USD?.dailyCashFlow || 0;
  
  // El cashflow correcto en USD
  const correctCashflowUSD = CONFIG.CORRECT_CASHFLOW_COP / usdCopRate;
  
  // Diferencia
  const difference = incorrectCashflowUSD - correctCashflowUSD;
  
  log('INFO', 'Análisis del documento:', {
    date: CONFIG.TARGET_DATE,
    incorrectCashflowUSD: incorrectCashflowUSD.toFixed(2),
    correctCashflowUSD: correctCashflowUSD.toFixed(2),
    differenceUSD: difference.toFixed(2),
    usdCopRate: usdCopRate.toFixed(2),
    totalValue: data.USD?.totalValue?.toFixed(2),
    totalInvestment: data.USD?.totalInvestment?.toFixed(2),
  });
  
  return {
    incorrectCashflowUSD,
    correctCashflowUSD,
    difference
  };
}

/**
 * Generar corrección para un documento
 */
function generateCorrection(doc, analysis, exchangeRates) {
  const data = doc.data;
  const { correctCashflowUSD, difference } = analysis;
  
  const updates = {};
  
  CONFIG.CURRENCIES.forEach(currency => {
    const rate = exchangeRates[currency] || 1;
    const currencyData = data[currency];
    
    if (!currencyData) return;
    
    // Corregir dailyCashFlow
    const oldCashFlow = currencyData.dailyCashFlow || 0;
    const newCashFlow = correctCashflowUSD * rate;
    
    // Corregir totalInvestment (restamos la diferencia)
    const oldInvestment = currencyData.totalInvestment || 0;
    const investmentCorrection = difference * rate;
    const newInvestment = oldInvestment - investmentCorrection;
    
    // Recalcular totalROI con la nueva inversión
    const totalValue = currencyData.totalValue || 0;
    const newUnrealizedPnL = totalValue - newInvestment;
    const newTotalROI = newInvestment > 0 ? (newUnrealizedPnL / newInvestment) * 100 : 0;
    
    // Recalcular adjustedDailyChangePercentage
    // Necesitamos el valor del día anterior para esto
    // Por ahora solo corregimos los campos principales
    
    updates[`${currency}.dailyCashFlow`] = newCashFlow;
    updates[`${currency}.totalInvestment`] = newInvestment;
    updates[`${currency}.totalROI`] = newTotalROI;
    updates[`${currency}.unrealizedProfitAndLoss`] = newUnrealizedPnL;
    
    log('CHANGE', `Correcciones para ${currency}:`, {
      dailyCashFlow: `${oldCashFlow.toFixed(2)} → ${newCashFlow.toFixed(2)}`,
      totalInvestment: `${oldInvestment.toFixed(2)} → ${newInvestment.toFixed(2)}`,
      totalROI: `${(currencyData.totalROI || 0).toFixed(2)}% → ${newTotalROI.toFixed(2)}%`,
    });
  });
  
  return updates;
}

/**
 * Propagar corrección a días posteriores
 * La inversión total incorrecta se propagó a todos los días siguientes
 */
async function propagateCorrection(userId, startDate, differenceUSD, exchangeRates, mode, accountId = null) {
  const path = accountId 
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates`
    : `portfolioPerformance/${userId}/dates`;
  
  log('INFO', `Propagando corrección a días posteriores (${accountId || 'OVERALL'})...`);
  
  const snapshot = await db.collection(path)
    .where('date', '>', startDate)
    .orderBy('date', 'asc')
    .get();
  
  log('INFO', `Encontrados ${snapshot.docs.length} documentos posteriores a ${startDate}`);
  
  const batch = db.batch();
  let count = 0;
  
  for (const doc of snapshot.docs) {
    const data = doc.data();
    const updates = {};
    
    CONFIG.CURRENCIES.forEach(currency => {
      const rate = exchangeRates[currency] || 1;
      const currencyData = data[currency];
      
      if (!currencyData) return;
      
      // Corregir totalInvestment
      const oldInvestment = currencyData.totalInvestment || 0;
      const correction = differenceUSD * rate;
      const newInvestment = oldInvestment - correction;
      
      // Recalcular métricas derivadas
      const totalValue = currencyData.totalValue || 0;
      const newUnrealizedPnL = totalValue - newInvestment;
      const newTotalROI = newInvestment > 0 ? (newUnrealizedPnL / newInvestment) * 100 : 0;
      
      updates[`${currency}.totalInvestment`] = newInvestment;
      updates[`${currency}.totalROI`] = newTotalROI;
      updates[`${currency}.unrealizedProfitAndLoss`] = newUnrealizedPnL;
    });
    
    if (Object.keys(updates).length > 0) {
      if (mode === 'fix') {
        batch.update(doc.ref, updates);
      }
      count++;
      
      if (count <= 3 || count === snapshot.docs.length) {
        log('DEBUG', `${data.date}: totalInvestment corregido`);
      } else if (count === 4) {
        log('DEBUG', `... (${snapshot.docs.length - 6} más)`);
      }
    }
  }
  
  if (mode === 'fix' && count > 0) {
    await batch.commit();
    log('SUCCESS', `Propagadas ${count} correcciones`);
  }
  
  return count;
}

// ============================================================================
// PROCESO PRINCIPAL
// ============================================================================

async function main() {
  const options = parseArgs();
  
  console.log('');
  console.log('='.repeat(80));
  console.log('CORRECCIÓN DE CASHFLOW - DÍA 2026-01-08');
  console.log('='.repeat(80));
  console.log(`Modo: ${options.mode.toUpperCase()}`);
  console.log(`Usuario: ${CONFIG.USER_ID}`);
  console.log(`Fecha: ${CONFIG.TARGET_DATE}`);
  console.log('');
  
  // 1. Obtener tipo de cambio USD/COP del día
  log('INFO', 'Obteniendo tipo de cambio USD/COP del día...');
  const usdCopRate = await fetchHistoricalExchangeRate('COP', CONFIG.TARGET_DATE);
  
  if (!usdCopRate) {
    log('ERROR', 'No se pudo obtener el tipo de cambio. Abortando.');
    process.exit(1);
  }
  
  // Calcular valores
  const correctCashflowUSD = CONFIG.CORRECT_CASHFLOW_COP / usdCopRate;
  const differenceUSD = CONFIG.INCORRECT_CASHFLOW_USD - correctCashflowUSD;
  
  console.log('');
  log('INFO', 'Resumen del problema:', {
    transaccion: '20 unidades ECOPETROL.CL @ 2005 COP = 40,100 COP',
    cashflowRegistradoUSD: `$${Math.abs(CONFIG.INCORRECT_CASHFLOW_USD).toFixed(2)} USD (INCORRECTO)`,
    cashflowCorrectoUSD: `$${Math.abs(correctCashflowUSD).toFixed(2)} USD (40,100 COP ÷ ${usdCopRate.toFixed(2)})`,
    diferenciaUSD: `$${Math.abs(differenceUSD).toFixed(2)} USD (exceso registrado)`,
  });
  console.log('');
  
  // 2. Obtener cuentas del usuario
  const accounts = await getUserAccounts(CONFIG.USER_ID);
  log('INFO', `Usuario tiene ${accounts.length} cuentas`);
  
  // 3. Corregir OVERALL
  console.log('');
  console.log('-'.repeat(80));
  log('INFO', 'Procesando OVERALL...');
  
  const overallDoc = await getPerformanceDoc(CONFIG.USER_ID, CONFIG.TARGET_DATE);
  
  if (!overallDoc) {
    log('ERROR', 'No se encontró documento OVERALL para la fecha');
    process.exit(1);
  }
  
  const analysis = await analyzeDocument(overallDoc, usdCopRate);
  const exchangeRates = deriveExchangeRates(overallDoc.data);
  
  log('INFO', 'Tipos de cambio derivados del documento:', exchangeRates);
  
  const overallUpdates = generateCorrection(overallDoc, analysis, exchangeRates);
  
  if (options.mode === 'fix') {
    await overallDoc.ref.update(overallUpdates);
    log('SUCCESS', 'Documento OVERALL corregido');
  }
  
  // 4. Propagar a días posteriores (OVERALL)
  const overallPropagated = await propagateCorrection(
    CONFIG.USER_ID, 
    CONFIG.TARGET_DATE, 
    differenceUSD, 
    exchangeRates,
    options.mode
  );
  
  // 5. Corregir por cuenta
  for (const account of accounts) {
    console.log('');
    console.log('-'.repeat(80));
    log('INFO', `Procesando cuenta: ${account.name} (${account.id})`);
    
    const accountDoc = await getPerformanceDoc(CONFIG.USER_ID, CONFIG.TARGET_DATE, account.id);
    
    if (!accountDoc) {
      log('WARNING', `No hay documento para esta cuenta en ${CONFIG.TARGET_DATE}`);
      continue;
    }
    
    // Verificar si esta cuenta tiene el cashflow incorrecto
    const accountCashflow = accountDoc.data.USD?.dailyCashFlow || 0;
    
    if (Math.abs(accountCashflow - CONFIG.INCORRECT_CASHFLOW_USD) < 1) {
      log('INFO', 'Esta cuenta tiene el cashflow incorrecto, corrigiendo...');
      
      const accountAnalysis = await analyzeDocument(accountDoc, usdCopRate);
      const accountExchangeRates = deriveExchangeRates(accountDoc.data);
      const accountUpdates = generateCorrection(accountDoc, accountAnalysis, accountExchangeRates);
      
      if (options.mode === 'fix') {
        await accountDoc.ref.update(accountUpdates);
        log('SUCCESS', 'Documento de cuenta corregido');
      }
      
      // Propagar a días posteriores de esta cuenta
      await propagateCorrection(
        CONFIG.USER_ID,
        CONFIG.TARGET_DATE,
        differenceUSD,
        accountExchangeRates,
        options.mode,
        account.id
      );
    } else {
      log('INFO', `Cashflow de esta cuenta: $${accountCashflow.toFixed(2)} (no afectada)`);
    }
  }
  
  // 6. Resumen final
  console.log('');
  console.log('='.repeat(80));
  console.log('RESUMEN');
  console.log('='.repeat(80));
  
  if (options.mode === 'analyze') {
    log('WARNING', 'Modo ANALYZE: No se aplicaron cambios');
    log('INFO', 'Ejecuta con --fix para aplicar las correcciones');
  } else {
    log('SUCCESS', '✅ Correcciones aplicadas exitosamente');
    log('INFO', `Documento ${CONFIG.TARGET_DATE} corregido`);
    log('INFO', `${overallPropagated} documentos posteriores actualizados`);
    console.log('');
    log('INFO', 'Próximos pasos:');
    log('INFO', '1. Verificar que los valores YTD son correctos');
    log('INFO', '2. Ejecutar backfill si es necesario para recalcular TWR');
  }
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
