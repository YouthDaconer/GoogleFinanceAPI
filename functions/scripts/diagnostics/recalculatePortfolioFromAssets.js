/**
 * Script para recalcular métricas de portfolio basándose en assetPerformance
 * 
 * Este script recalcula las métricas a nivel de portfolio (totalValue, totalInvestment,
 * adjustedDailyChangePercentage, etc.) sumando los valores de todos los assets individuales.
 * 
 * Esto asegura consistencia entre assetPerformance y las métricas del portfolio.
 * 
 * USO:
 *   node recalculatePortfolioFromAssets.js --dry-run    # Ver cambios sin aplicar
 *   node recalculatePortfolioFromAssets.js --fix        # Aplicar cambios
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  ACCOUNTS: ['BZHvXz4QT2yqqqlFP22X', 'Z3gnboYgRlTvSZNGSu8j'],
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  START_DATE: '2024-01-01',
  END_DATE: '2025-12-31',
  BATCH_SIZE: 15,
};

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    mode: args.includes('--fix') ? 'fix' : 'dry-run'
  };
}

function log(level, message) {
  const colors = {
    INFO: '\x1b[36m',
    SUCCESS: '\x1b[32m',
    WARN: '\x1b[33m',
    PROGRESS: '\x1b[35m',
  };
  const reset = '\x1b[0m';
  console.log(`${colors[level] || ''}[${level}]${reset} ${message}`);
}

/**
 * Recalcular métricas de portfolio sumando assetPerformance
 * 
 * Para adjustedDailyChangePercentage: usar la fórmula TWR correcta:
 *   adj% = (EndValue - StartValue + CashFlow) / StartValue * 100
 * 
 * Donde CashFlow es negativo para compras (sale dinero) y positivo para ventas.
 * Esto neutraliza las inyecciones/retiros de capital.
 */
function recalculatePortfolioMetrics(currencyData, prevCurrencyData) {
  const assetPerf = currencyData.assetPerformance || {};
  const prevAssetPerf = prevCurrencyData?.assetPerformance || {};
  
  // Sumar valores de todos los assets
  let totalValue = 0;
  let totalInvestment = 0;
  let totalCashFlow = 0;
  let doneProfitAndLoss = 0;
  
  // Calcular prevTotalValue sumando los valores del día anterior
  let prevTotalValue = 0;
  
  Object.entries(assetPerf).forEach(([assetKey, asset]) => {
    totalValue += asset.totalValue || 0;
    totalInvestment += asset.totalInvestment || 0;
    totalCashFlow += asset.totalCashFlow || 0;
    doneProfitAndLoss += asset.doneProfitAndLoss || 0;
  });
  
  // Calcular prevTotalValue de los assets que existían ayer
  Object.entries(prevAssetPerf).forEach(([assetKey, asset]) => {
    prevTotalValue += asset.totalValue || 0;
  });
  
  // Calcular métricas derivadas
  const unrealizedProfitAndLoss = totalValue - totalInvestment;
  const totalROI = totalInvestment > 0 ? ((totalValue - totalInvestment) / totalInvestment) * 100 : 0;
  
  // Calcular cambio bruto (raw): simple comparación de valores
  let rawDailyChangePercentage = 0;
  if (prevTotalValue > 0) {
    rawDailyChangePercentage = ((totalValue - prevTotalValue) / prevTotalValue) * 100;
  }
  
  // Calcular adj% usando fórmula TWR correcta:
  // adj% = (EndValue - StartValue + CashFlow) / StartValue * 100
  // CashFlow es negativo para compras, positivo para ventas
  let adjustedDailyChangePercentage = 0;
  if (prevTotalValue > 0) {
    adjustedDailyChangePercentage = ((totalValue - prevTotalValue + totalCashFlow) / prevTotalValue) * 100;
  }
  
  const dailyReturn = adjustedDailyChangePercentage / 100;
  
  return {
    totalValue,
    totalInvestment,
    totalCashFlow,
    doneProfitAndLoss,
    unrealizedProfitAndLoss,
    totalROI,
    dailyChangePercentage: rawDailyChangePercentage,
    rawDailyChangePercentage,
    adjustedDailyChangePercentage,
    dailyReturn,
    monthlyReturn: currencyData.monthlyReturn || 0,
    annualReturn: currencyData.annualReturn || 0,
    assetPerformance: assetPerf, // Mantener assetPerformance sin cambios
  };
}

async function recalculateAll() {
  const options = parseArgs();
  
  console.log('═'.repeat(80));
  console.log('  RECÁLCULO DE MÉTRICAS DE PORTFOLIO DESDE assetPerformance');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', `Modo: ${options.mode.toUpperCase()}`);
  console.log('');
  
  let totalDocsFixed = 0;
  let totalChanges = 0;
  
  // =========================================================================
  // PASO 1: Procesar cuentas individuales
  // =========================================================================
  log('PROGRESS', 'Procesando cuentas individuales...');
  
  for (const accountId of CONFIG.ACCOUNTS) {
    const allDocs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`)
      .where('date', '>=', CONFIG.START_DATE)
      .where('date', '<=', CONFIG.END_DATE)
      .orderBy('date', 'asc')
      .get();
    
    const docsByDate = new Map();
    allDocs.docs.forEach(doc => {
      const data = doc.data();
      docsByDate.set(data.date, { ref: doc.ref, data });
    });
    
    const allDates = [...docsByDate.keys()].sort();
    const documentsToFix = [];
    
    for (let i = 0; i < allDates.length; i++) {
      const date = allDates[i];
      const { ref, data } = docsByDate.get(date);
      const previousData = i > 0 ? docsByDate.get(allDates[i - 1])?.data : null;
      
      const fixes = {};
      let hasChanges = false;
      
      CONFIG.CURRENCIES.forEach(currency => {
        const currencyData = data[currency];
        if (!currencyData || !currencyData.assetPerformance) return;
        
        const prevCurrencyData = previousData?.[currency];
        const recalculated = recalculatePortfolioMetrics(currencyData, prevCurrencyData);
        
        // Verificar si hay diferencias significativas
        const oldTotal = currencyData.totalValue || 0;
        const newTotal = recalculated.totalValue;
        const oldAdj = currencyData.adjustedDailyChangePercentage || 0;
        const newAdj = recalculated.adjustedDailyChangePercentage;
        
        if (Math.abs(oldTotal - newTotal) > 0.01 || Math.abs(oldAdj - newAdj) > 0.01) {
          fixes[currency] = recalculated;
          hasChanges = true;
          totalChanges++;
        }
      });
      
      if (hasChanges) {
        documentsToFix.push({ ref, date, fixes });
      }
    }
    
    // Aplicar correcciones
    if (options.mode === 'fix' && documentsToFix.length > 0) {
      for (let i = 0; i < documentsToFix.length; i += CONFIG.BATCH_SIZE) {
        const batch = db.batch();
        const chunk = documentsToFix.slice(i, i + CONFIG.BATCH_SIZE);
        chunk.forEach(({ ref, fixes }) => batch.update(ref, fixes));
        await batch.commit();
      }
    }
    
    log('INFO', `  ${accountId}: ${documentsToFix.length} docs a corregir`);
    totalDocsFixed += documentsToFix.length;
  }
  
  // =========================================================================
  // PASO 2: Procesar OVERALL
  // =========================================================================
  log('PROGRESS', 'Procesando OVERALL...');
  
  const overallDocs = await db.collection(`portfolioPerformance/${CONFIG.USER_ID}/dates`)
    .where('date', '>=', CONFIG.START_DATE)
    .where('date', '<=', CONFIG.END_DATE)
    .orderBy('date', 'asc')
    .get();
  
  const overallByDate = new Map();
  overallDocs.docs.forEach(doc => {
    const data = doc.data();
    overallByDate.set(data.date, { ref: doc.ref, data });
  });
  
  const allDates = [...overallByDate.keys()].sort();
  const overallToFix = [];
  
  for (let i = 0; i < allDates.length; i++) {
    const date = allDates[i];
    const { ref, data } = overallByDate.get(date);
    const previousData = i > 0 ? overallByDate.get(allDates[i - 1])?.data : null;
    
    const fixes = {};
    let hasChanges = false;
    
    CONFIG.CURRENCIES.forEach(currency => {
      const currencyData = data[currency];
      if (!currencyData || !currencyData.assetPerformance) return;
      
      const prevCurrencyData = previousData?.[currency];
      const recalculated = recalculatePortfolioMetrics(currencyData, prevCurrencyData);
      
      const oldTotal = currencyData.totalValue || 0;
      const newTotal = recalculated.totalValue;
      const oldAdj = currencyData.adjustedDailyChangePercentage || 0;
      const newAdj = recalculated.adjustedDailyChangePercentage;
      
      if (Math.abs(oldTotal - newTotal) > 0.01 || Math.abs(oldAdj - newAdj) > 0.01) {
        fixes[currency] = recalculated;
        hasChanges = true;
        totalChanges++;
      }
    });
    
    if (hasChanges) {
      overallToFix.push({ ref, date, fixes });
    }
  }
  
  // Aplicar correcciones a OVERALL
  if (options.mode === 'fix' && overallToFix.length > 0) {
    for (let i = 0; i < overallToFix.length; i += CONFIG.BATCH_SIZE) {
      const batch = db.batch();
      const chunk = overallToFix.slice(i, i + CONFIG.BATCH_SIZE);
      chunk.forEach(({ ref, fixes }) => batch.update(ref, fixes));
      await batch.commit();
    }
  }
  
  log('INFO', `  OVERALL: ${overallToFix.length} docs a corregir`);
  totalDocsFixed += overallToFix.length;
  
  // =========================================================================
  // RESUMEN
  // =========================================================================
  console.log('');
  console.log('═'.repeat(80));
  log('INFO', `Total documentos a corregir: ${totalDocsFixed}`);
  log('INFO', `Total cambios en monedas: ${totalChanges}`);
  
  if (options.mode === 'fix') {
    log('SUCCESS', `✅ Corrección completada`);
    
    // Invalidar cache
    const cache = await db.collection(`userData/${CONFIG.USER_ID}/performanceCache`).get();
    if (cache.docs.length > 0) {
      const batch = db.batch();
      cache.docs.forEach(doc => batch.delete(doc.ref));
      await batch.commit();
      log('SUCCESS', `Cache invalidado: ${cache.docs.length} docs`);
    }
  } else {
    log('WARN', 'Modo dry-run: No se aplicaron cambios');
    log('INFO', 'Ejecuta con --fix para aplicar');
  }
  
  process.exit(0);
}

recalculateAll().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
