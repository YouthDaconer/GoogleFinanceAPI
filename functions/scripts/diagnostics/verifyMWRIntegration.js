/**
 * Verificación integral de MWR (Personal Return) con datos reales de Firestore
 * 
 * Historia 25: Este script valida que las funciones implementadas calculan
 * correctamente el MWR comparándolo con:
 * 1. ROI Simple (valorización / inversión)
 * 2. TWR existente
 * 3. Datos manuales conocidos
 * 
 * Ejecutar: node scripts/diagnostics/verifyMWRIntegration.js
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

// Importar las funciones implementadas
const { 
  calculateSimplePersonalReturn, 
  calculateModifiedDietzReturn,
  calculateAllPersonalReturns 
} = require('../../utils/mwrCalculations');

const {
  getPeriodBoundaries,
  sortDocumentsByDate,
  extractDocumentData,
  initializePeriods,
  MIN_DOCS
} = require('../../utils/periodCalculations');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

// Configuración de cuentas a verificar
const ACCOUNTS = [
  {
    name: 'XTB',
    userId: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
    accountId: 'Z3gnboYgRlTvSZNGSu8j'
  },
  {
    name: 'IBKR',
    userId: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
    accountId: 'BZHvXz4QT2yqqqlFP22X'
  },
  {
    name: 'Binance Cryptos',
    userId: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
    accountId: 'zHZCvwpQeA2HoYMxDtPF'
  }
];

const CURRENCY = 'USD';

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

function formatPercent(value) {
  if (value === null || value === undefined || isNaN(value)) return 'N/A';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

function formatMoney(value) {
  if (value === null || value === undefined) return 'N/A';
  return `$${value.toFixed(2)}`;
}

// ============================================================================
// MAIN VERIFICATION
// ============================================================================

async function verifyMWRIntegration() {
  console.log('='.repeat(120));
  console.log('🔍 VERIFICACIÓN INTEGRAL DE MWR (Personal Return) - Historia 25');
  console.log('='.repeat(120));
  console.log();
  console.log(`📅 Fecha de ejecución: ${DateTime.now().toISO()}`);
  console.log();

  for (const account of ACCOUNTS) {
    await verifyAccount(account);
  }

  console.log('='.repeat(120));
  console.log('✅ VERIFICACIÓN COMPLETADA');
  console.log('='.repeat(120));
}

async function verifyAccount(account) {
  console.log('='.repeat(120));
  console.log(`📊 VERIFICANDO CUENTA: ${account.name}`);
  console.log('='.repeat(120));
  console.log();

  // 1. Obtener documentos históricos
  const performancePath = `portfolioPerformance/${account.userId}/accounts/${account.accountId}/dates`;
  const snapshot = await db.collection(performancePath).orderBy('date', 'asc').get();

  if (snapshot.empty) {
    console.log('❌ No hay datos de performance para esta cuenta');
    return;
  }

  console.log(`📄 Documentos encontrados: ${snapshot.docs.length}`);
  console.log();

  // 2. Obtener datos actuales de assets para ROI Simple
  const assetsSnapshot = await db.collection('assets')
    .where('portfolioAccount', '==', account.accountId)
    .where('isActive', '==', true)
    .get();

  const pricesSnapshot = await db.collection('currentPrices').get();
  const prices = {};
  pricesSnapshot.docs.forEach(d => {
    const data = d.data();
    prices[data.symbol] = data.price;
  });

  let totalInvestmentFromAssets = 0;
  let totalCurrentValueFromAssets = 0;

  console.log('📦 Assets activos:');
  for (const doc of assetsSnapshot.docs) {
    const asset = doc.data();
    const investment = asset.unitValue * asset.units;
    const currentPrice = prices[asset.name] || 0;
    const currentValue = currentPrice * asset.units;
    
    totalInvestmentFromAssets += investment;
    totalCurrentValueFromAssets += currentValue;
    
    const pnl = currentValue - investment;
    const pnlPct = ((currentValue / investment - 1) * 100);
    
    console.log(`   ${asset.name.padEnd(10)} | Inv: ${formatMoney(investment).padStart(12)} | Val: ${formatMoney(currentValue).padStart(12)} | P&L: ${formatPercent(pnlPct).padStart(10)}`);
  }

  const roiSimple = ((totalCurrentValueFromAssets / totalInvestmentFromAssets) - 1) * 100;
  
  console.log();
  console.log('📊 Resumen de assets:');
  console.log(`   Total Inversión: ${formatMoney(totalInvestmentFromAssets)}`);
  console.log(`   Valor Actual: ${formatMoney(totalCurrentValueFromAssets)}`);
  console.log(`   Valorización: ${formatMoney(totalCurrentValueFromAssets - totalInvestmentFromAssets)}`);
  console.log(`   ROI Simple: ${formatPercent(roiSimple)}`);
  console.log();

  // 3. Calcular MWR usando las funciones implementadas
  console.log('-'.repeat(120));
  console.log('🧮 CÁLCULO DE MWR CON FUNCIONES IMPLEMENTADAS');
  console.log('-'.repeat(120));
  console.log();

  // Convertir snapshots a formato que esperan las funciones
  const docsForCalculation = snapshot.docs.map(doc => ({
    ...doc.data(),
    data: () => doc.data()
  }));

  const mwrResults = calculateAllPersonalReturns(docsForCalculation, CURRENCY, null, null);
  
  console.log('📈 Resultados de calculateAllPersonalReturns():');
  console.log();
  console.log('   Período    | MWR (Personal)  | Tiene Datos');
  console.log('   ' + '-'.repeat(50));
  console.log(`   YTD        | ${formatPercent(mwrResults.ytdPersonalReturn).padStart(15)} | ${mwrResults.hasYtdPersonalData ? '✅' : '❌'}`);
  console.log(`   1M         | ${formatPercent(mwrResults.oneMonthPersonalReturn).padStart(15)} | ${mwrResults.hasOneMonthPersonalData ? '✅' : '❌'}`);
  console.log(`   3M         | ${formatPercent(mwrResults.threeMonthPersonalReturn).padStart(15)} | ${mwrResults.hasThreeMonthPersonalData ? '✅' : '❌'}`);
  console.log(`   6M         | ${formatPercent(mwrResults.sixMonthPersonalReturn).padStart(15)} | ${mwrResults.hasSixMonthPersonalData ? '✅' : '❌'}`);
  console.log(`   1Y         | ${formatPercent(mwrResults.oneYearPersonalReturn).padStart(15)} | ${mwrResults.hasOneYearPersonalData ? '✅' : '❌'}`);
  console.log(`   2Y         | ${formatPercent(mwrResults.twoYearPersonalReturn).padStart(15)} | ${mwrResults.hasTwoYearPersonalData ? '✅' : '❌'}`);
  console.log(`   5Y         | ${formatPercent(mwrResults.fiveYearPersonalReturn).padStart(15)} | ${mwrResults.hasFiveYearPersonalData ? '✅' : '❌'}`);
  console.log();

  // 4. Calcular TWR manualmente para comparar
  console.log('-'.repeat(120));
  console.log('📊 COMPARACIÓN TWR vs MWR');
  console.log('-'.repeat(120));
  console.log();

  const boundaries = getPeriodBoundaries();
  const sortedDocs = sortDocumentsByDate(docsForCalculation);
  
  // Calcular TWR manualmente para YTD
  let currentFactor = 1;
  let ytdStartFactor = null;
  let ytdStartValue = null;
  let ytdEndValue = null;
  let ytdTotalCashFlow = 0;
  let ytdCashFlows = [];

  for (const doc of sortedDocs) {
    const data = extractDocumentData(doc, CURRENCY);
    if (!data || !data.hasData) continue;

    const docDate = data.date;
    const adjChange = data.adjustedDailyChangePercentage || 0;

    // TWR: Actualizar factor compuesto
    currentFactor = currentFactor * (1 + adjChange / 100);

    // YTD period
    if (docDate >= boundaries.periods.ytd.startDate) {
      if (ytdStartFactor === null) {
        ytdStartFactor = currentFactor;
        ytdStartValue = data.totalValue;
      }
      
      ytdEndValue = data.totalValue;
      
      if (data.totalCashFlow !== 0) {
        ytdCashFlows.push({ date: docDate, amount: data.totalCashFlow });
      }
      ytdTotalCashFlow += data.totalCashFlow || 0;
    }
  }

  const ytdTWR = ytdStartFactor ? ((currentFactor / ytdStartFactor) - 1) * 100 : 0;
  
  // MWR manual usando Modified Dietz
  const ytdMWRManual = ytdCashFlows.length > 0 
    ? calculateModifiedDietzReturn(ytdStartValue, ytdEndValue, ytdCashFlows, boundaries.periods.ytd.startDate, boundaries.todayISO)
    : calculateSimplePersonalReturn(ytdStartValue, ytdEndValue, ytdTotalCashFlow);

  console.log('📈 YTD Comparación:');
  console.log();
  console.log(`   Métrica              | Valor          | Descripción`);
  console.log('   ' + '-'.repeat(70));
  console.log(`   TWR (calculado)      | ${formatPercent(ytdTWR).padStart(14)} | Rendimiento eliminando efecto de flujos`);
  console.log(`   MWR (función)        | ${formatPercent(mwrResults.ytdPersonalReturn).padStart(14)} | calculateAllPersonalReturns()`);
  console.log(`   MWR (manual)         | ${formatPercent(ytdMWRManual).padStart(14)} | Cálculo directo con Modified Dietz`);
  console.log(`   ROI Simple           | ${formatPercent(roiSimple).padStart(14)} | (Valor - Inversión) / Inversión`);
  console.log();
  
  // 5. Validar consistencia
  console.log('-'.repeat(120));
  console.log('✅ VALIDACIONES');
  console.log('-'.repeat(120));
  console.log();

  let validations = [];

  // Validación 1: MWR función == MWR manual
  const mwrDiff = Math.abs(mwrResults.ytdPersonalReturn - ytdMWRManual);
  validations.push({
    name: 'MWR función vs MWR manual',
    passed: mwrDiff < 0.1,
    message: mwrDiff < 0.1 
      ? `Diferencia: ${mwrDiff.toFixed(4)}% (< 0.1%)` 
      : `⚠️ Diferencia: ${mwrDiff.toFixed(4)}% (> 0.1%)`
  });

  // Validación 2: MWR != 0 cuando hay cashflows
  validations.push({
    name: 'MWR calcula correctamente',
    passed: Math.abs(mwrResults.ytdPersonalReturn) > 0 || ytdTotalCashFlow === 0,
    message: Math.abs(mwrResults.ytdPersonalReturn) > 0 
      ? `MWR: ${formatPercent(mwrResults.ytdPersonalReturn)}`
      : ytdTotalCashFlow === 0 ? 'No hay cashflows (MWR = 0 es válido)' : '⚠️ MWR = 0 pero hay cashflows'
  });

  // Validación 3: TWR y MWR difieren cuando hay cashflows significativos
  const twrMwrDiff = Math.abs(ytdTWR - mwrResults.ytdPersonalReturn);
  if (Math.abs(ytdTotalCashFlow) > 100) {
    validations.push({
      name: 'TWR y MWR difieren (esperado con cashflows)',
      passed: twrMwrDiff > 0.5,
      message: twrMwrDiff > 0.5 
        ? `Diferencia: ${twrMwrDiff.toFixed(2)}% (esperado por cashflows de ${formatMoney(ytdTotalCashFlow)})`
        : `Diferencia pequeña: ${twrMwrDiff.toFixed(2)}%`
    });
  }

  // Validación 4: MWR está en rango razonable vs ROI Simple
  // Nota: Cuando los cashflows son altos respecto al valor inicial, 
  // la diferencia puede ser grande y aún ser correcta matemáticamente.
  const mwrVsRoi = Math.abs(mwrResults.ytdPersonalReturn - roiSimple);
  const cashflowRatio = Math.abs(ytdTotalCashFlow) / (ytdStartValue || 1);
  const acceptableDiff = cashflowRatio > 2 ? 50 : 20; // Mayor tolerancia si cashflows >> valor inicial
  
  validations.push({
    name: 'MWR en rango razonable vs ROI',
    passed: mwrVsRoi < acceptableDiff,
    message: mwrVsRoi < acceptableDiff 
      ? `Diferencia MWR-ROI: ${mwrVsRoi.toFixed(2)}% (razonable, ratio CF/inicial: ${cashflowRatio.toFixed(1)}x)`
      : `⚠️ Diferencia MWR-ROI: ${mwrVsRoi.toFixed(2)}% (ratio CF/inicial: ${cashflowRatio.toFixed(1)}x, revisar)`
  });

  // Validación 5: Flags de datos son correctos
  validations.push({
    name: 'Flags hasData correctos',
    passed: mwrResults.hasYtdPersonalData === (ytdStartValue !== null),
    message: mwrResults.hasYtdPersonalData ? 'hasYtdPersonalData: true ✅' : 'hasYtdPersonalData: false'
  });

  // Mostrar resultados de validación
  let passedCount = 0;
  let failedCount = 0;

  for (const v of validations) {
    const icon = v.passed ? '✅' : '❌';
    console.log(`   ${icon} ${v.name}`);
    console.log(`      ${v.message}`);
    console.log();
    
    if (v.passed) passedCount++;
    else failedCount++;
  }

  console.log('-'.repeat(120));
  console.log(`📊 RESUMEN: ${passedCount} validaciones pasadas, ${failedCount} fallidas`);
  console.log('-'.repeat(120));
  console.log();

  // 6. Datos crudos para debugging
  console.log('-'.repeat(120));
  console.log('📋 DATOS CRUDOS PARA DEBUGGING');
  console.log('-'.repeat(120));
  console.log();
  console.log(`   YTD Start Date: ${boundaries.periods.ytd.startDate}`);
  console.log(`   YTD Start Value: ${formatMoney(ytdStartValue)}`);
  console.log(`   YTD End Value: ${formatMoney(ytdEndValue)}`);
  console.log(`   YTD Total CashFlow: ${formatMoney(ytdTotalCashFlow)}`);
  console.log(`   YTD CashFlows count: ${ytdCashFlows.length}`);
  console.log(`   YTD Start Factor (TWR): ${ytdStartFactor?.toFixed(6)}`);
  console.log(`   YTD Current Factor (TWR): ${currentFactor.toFixed(6)}`);
  console.log();

  // Mostrar últimos 5 cashflows
  if (ytdCashFlows.length > 0) {
    console.log('   Últimos 5 cashflows:');
    const lastCashflows = ytdCashFlows.slice(-5);
    for (const cf of lastCashflows) {
      console.log(`      ${cf.date}: ${formatMoney(cf.amount)}`);
    }
  }
  console.log();
}

// ============================================================================
// RUN
// ============================================================================

verifyMWRIntegration()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('Error:', err);
    process.exit(1);
  });
