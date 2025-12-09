/**
 * Script de Diagnóstico: Comparación Multi-Cuenta vs Overall
 * 
 * Este script verifica:
 * 1. Qué retorna getMultiAccountHistoricalReturns con [IBKR, XTB]
 * 2. Qué retorna getHistoricalReturns con "overall"
 * 3. Diferencias entre ambos
 * 
 * HIPÓTESIS: El problema es que cuando selecciono "2 cuentas" en el UI,
 * podría estar usando "overall" en lugar de hacer la agregación real.
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

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
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  ACCOUNTS: {
    IBKR: 'BZHvXz4QT2yqqqlFP22X',
    XTB: 'Z3gnboYgRlTvSZNGSu8j',
    BINANCE: 'zHZCvwpQeA2HoYMxDtPF'
  },
  CURRENCY: 'USD'
};

// ============================================================================
// UTILIDADES
// ============================================================================

/**
 * Calcular rendimiento compuesto desde documentos diarios
 */
function calculateCompoundReturn(docs, currency, startDateISO, endDateISO) {
  // Filtrar documentos en el rango
  const filtered = docs.filter(d => d.date >= startDateISO && d.date <= endDateISO);
  filtered.sort((a, b) => a.date.localeCompare(b.date));
  
  let compoundFactor = 1;
  let validDays = 0;
  
  filtered.forEach(doc => {
    const currencyData = doc[currency];
    if (currencyData && currencyData.adjustedDailyChangePercentage !== undefined) {
      const dailyChange = currencyData.adjustedDailyChangePercentage / 100;
      compoundFactor *= (1 + dailyChange);
      validDays++;
    }
  });
  
  return {
    return: (compoundFactor - 1) * 100,
    validDays,
    compoundFactor
  };
}

/**
 * Agregar datos de múltiples cuentas (simula getMultiAccountHistoricalReturns)
 */
function aggregateAccountData(accountDocs, currency) {
  // Agrupar por fecha
  const byDate = new Map();
  
  accountDocs.forEach(({ accountId, docs }) => {
    docs.forEach(doc => {
      if (!byDate.has(doc.date)) {
        byDate.set(doc.date, { 
          date: doc.date, 
          accounts: {},
          totalValue: 0 
        });
      }
      
      const dateEntry = byDate.get(doc.date);
      const currencyData = doc[currency];
      
      if (currencyData) {
        dateEntry.accounts[accountId] = {
          totalValue: currencyData.totalValue || 0,
          adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage || 0
        };
        dateEntry.totalValue += currencyData.totalValue || 0;
      }
    });
  });
  
  // Calcular cambio diario ponderado para cada fecha
  const result = [];
  const sortedDates = Array.from(byDate.keys()).sort();
  
  sortedDates.forEach(date => {
    const dateEntry = byDate.get(date);
    const accounts = Object.values(dateEntry.accounts);
    
    // Calcular cambio ponderado por valor
    const totalValue = accounts.reduce((sum, acc) => sum + acc.totalValue, 0);
    let weightedChange = 0;
    
    if (totalValue > 0) {
      accounts.forEach(acc => {
        const weight = acc.totalValue / totalValue;
        weightedChange += acc.adjustedDailyChangePercentage * weight;
      });
    }
    
    result.push({
      date,
      [currency]: {
        totalValue,
        adjustedDailyChangePercentage: weightedChange
      }
    });
  });
  
  return result;
}

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

async function getAccountDocs(accountId) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function getOverallDocs() {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

// ============================================================================
// DIAGNÓSTICO PRINCIPAL
// ============================================================================

async function main() {
  console.log('');
  console.log('═'.repeat(80));
  console.log('  DIAGNÓSTICO: Multi-Cuenta vs Overall');
  console.log('  Comparando agregación de IBKR+XTB vs datos OVERALL');
  console.log('═'.repeat(80));
  console.log('');

  // Obtener datos
  console.log('📊 Obteniendo datos de Firestore...');
  const [ibkrDocs, xtbDocs, binanceDocs, overallDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB),
    getAccountDocs(CONFIG.ACCOUNTS.BINANCE),
    getOverallDocs()
  ]);
  
  console.log(`   IBKR: ${ibkrDocs.length} documentos`);
  console.log(`   XTB: ${xtbDocs.length} documentos`);
  console.log(`   Binance: ${binanceDocs.length} documentos`);
  console.log(`   OVERALL: ${overallDocs.length} documentos`);
  
  // Calcular agregación manual IBKR + XTB
  const aggregatedIbkrXtb = aggregateAccountData([
    { accountId: 'IBKR', docs: ibkrDocs },
    { accountId: 'XTB', docs: xtbDocs }
  ], CONFIG.CURRENCY);
  
  // Calcular agregación IBKR + XTB + Binance
  const aggregatedAll = aggregateAccountData([
    { accountId: 'IBKR', docs: ibkrDocs },
    { accountId: 'XTB', docs: xtbDocs },
    { accountId: 'BINANCE', docs: binanceDocs }
  ], CONFIG.CURRENCY);
  
  console.log(`   Agregado IBKR+XTB: ${aggregatedIbkrXtb.length} fechas`);
  console.log(`   Agregado ALL (IBKR+XTB+Binance): ${aggregatedAll.length} fechas`);
  
  // Definir períodos para análisis
  const now = DateTime.now().setZone("America/New_York");
  const periods = {
    'Marzo 2025': { start: '2025-03-01', end: '2025-03-31' },
    'YTD 2025': { start: '2025-01-01', end: now.toISODate() },
    '1M': { start: now.minus({ months: 1 }).toISODate(), end: now.toISODate() },
    '3M': { start: now.minus({ months: 3 }).toISODate(), end: now.toISODate() }
  };
  
  console.log('');
  console.log('━'.repeat(80));
  console.log('  COMPARACIÓN DE RENDIMIENTOS POR PERÍODO');
  console.log('━'.repeat(80));
  
  console.log('');
  console.log('Período      | IBKR+XTB (Agg) | OVERALL (DB) | ALL (Agg)    | Diff IBKR+XTB vs OVERALL');
  console.log('-'.repeat(95));
  
  for (const [period, { start, end }] of Object.entries(periods)) {
    // Calcular con agregación IBKR+XTB
    const ibkrXtbReturn = calculateCompoundReturn(aggregatedIbkrXtb, CONFIG.CURRENCY, start, end);
    
    // Calcular con datos OVERALL de Firestore
    const overallReturn = calculateCompoundReturn(overallDocs, CONFIG.CURRENCY, start, end);
    
    // Calcular con agregación de TODAS las cuentas
    const allReturn = calculateCompoundReturn(aggregatedAll, CONFIG.CURRENCY, start, end);
    
    const diff = overallReturn.return - ibkrXtbReturn.return;
    const diffFlag = Math.abs(diff) > 0.5 ? ' ⚠️' : '';
    
    console.log(
      `${period.padEnd(12)} | ` +
      `${ibkrXtbReturn.return.toFixed(2).padStart(12)}% | ` +
      `${overallReturn.return.toFixed(2).padStart(10)}% | ` +
      `${allReturn.return.toFixed(2).padStart(10)}% | ` +
      `${diff.toFixed(2).padStart(8)}%${diffFlag}`
    );
  }
  
  console.log('');
  console.log('━'.repeat(80));
  console.log('  CONCLUSIONES');
  console.log('━'.repeat(80));
  console.log('');
  console.log('  📋 EXPLICACIÓN DE COLUMNAS:');
  console.log('     - IBKR+XTB (Agg): Agregación calculada solo con IBKR y XTB');
  console.log('     - OVERALL (DB): Datos almacenados en Firestore (incluye TODAS las cuentas)');
  console.log('     - ALL (Agg): Agregación calculada con IBKR + XTB + Binance');
  console.log('');
  console.log('  🔍 ANÁLISIS:');
  console.log('     Si OVERALL ≈ ALL, entonces los datos en Firestore incluyen Binance correctamente.');
  console.log('     Si IBKR+XTB ≠ OVERALL, el problema es que el UI debería mostrar IBKR+XTB');
  console.log('     cuando seleccionas "2 cuentas", pero está mostrando OVERALL.');
  console.log('');
  
  // Verificar valores actuales
  console.log('━'.repeat(80));
  console.log('  VALORES ACTUALES (última fecha)');
  console.log('━'.repeat(80));
  
  const latestIbkr = ibkrDocs[ibkrDocs.length - 1];
  const latestXtb = xtbDocs[xtbDocs.length - 1];
  const latestBinance = binanceDocs[binanceDocs.length - 1];
  const latestOverall = overallDocs[overallDocs.length - 1];
  
  console.log('');
  console.log(`  IBKR (${latestIbkr?.date}):`);
  console.log(`     Valor: $${latestIbkr?.USD?.totalValue?.toFixed(2) || 0}`);
  console.log(`     Inversión: $${latestIbkr?.USD?.totalInvestment?.toFixed(2) || 0}`);
  
  console.log('');
  console.log(`  XTB (${latestXtb?.date}):`);
  console.log(`     Valor: $${latestXtb?.USD?.totalValue?.toFixed(2) || 0}`);
  console.log(`     Inversión: $${latestXtb?.USD?.totalInvestment?.toFixed(2) || 0}`);
  
  console.log('');
  console.log(`  Binance (${latestBinance?.date}):`);
  console.log(`     Valor: $${latestBinance?.USD?.totalValue?.toFixed(2) || 0}`);
  console.log(`     Inversión: $${latestBinance?.USD?.totalInvestment?.toFixed(2) || 0}`);
  
  console.log('');
  console.log(`  OVERALL (${latestOverall?.date}):`);
  console.log(`     Valor: $${latestOverall?.USD?.totalValue?.toFixed(2) || 0}`);
  console.log(`     Inversión: $${latestOverall?.USD?.totalInvestment?.toFixed(2) || 0}`);
  
  const sumValue = (latestIbkr?.USD?.totalValue || 0) + (latestXtb?.USD?.totalValue || 0) + (latestBinance?.USD?.totalValue || 0);
  const sumInvestment = (latestIbkr?.USD?.totalInvestment || 0) + (latestXtb?.USD?.totalInvestment || 0) + (latestBinance?.USD?.totalInvestment || 0);
  
  console.log('');
  console.log(`  SUMA IBKR+XTB+Binance:`);
  console.log(`     Valor: $${sumValue.toFixed(2)}`);
  console.log(`     Inversión: $${sumInvestment.toFixed(2)}`);
  
  console.log('');
  console.log(`  DIFERENCIA (OVERALL - SUMA):`);
  console.log(`     Valor: $${(latestOverall?.USD?.totalValue - sumValue).toFixed(2)}`);
  console.log(`     Inversión: $${(latestOverall?.USD?.totalInvestment - sumInvestment).toFixed(2)}`);
  
  // Verificar datos de marzo en detalle
  console.log('');
  console.log('━'.repeat(80));
  console.log('  DETALLE DE MARZO 2025 (Anomalía reportada: -4.25% vs esperado -5% a -7%)');
  console.log('━'.repeat(80));
  
  const marchIbkrXtb = aggregatedIbkrXtb.filter(d => d.date.startsWith('2025-03'));
  
  console.log('');
  console.log('  Valores UI reportados por el usuario:');
  console.log('     IBKR: -6.75%');
  console.log('     XTB: -5.13%');
  console.log('     Consolidado (UI): -4.25%');
  console.log('');
  console.log('  Valores calculados desde Firestore:');
  
  const marchIbkrReturn = calculateCompoundReturn(ibkrDocs, CONFIG.CURRENCY, '2025-03-01', '2025-03-31');
  const marchXtbReturn = calculateCompoundReturn(xtbDocs, CONFIG.CURRENCY, '2025-03-01', '2025-03-31');
  const marchBinanceReturn = calculateCompoundReturn(binanceDocs, CONFIG.CURRENCY, '2025-03-01', '2025-03-31');
  const marchOverallReturn = calculateCompoundReturn(overallDocs, CONFIG.CURRENCY, '2025-03-01', '2025-03-31');
  const marchAggIbkrXtb = calculateCompoundReturn(aggregatedIbkrXtb, CONFIG.CURRENCY, '2025-03-01', '2025-03-31');
  
  console.log(`     IBKR: ${marchIbkrReturn.return.toFixed(2)}%`);
  console.log(`     XTB: ${marchXtbReturn.return.toFixed(2)}%`);
  console.log(`     Binance: ${marchBinanceReturn.return.toFixed(2)}%`);
  console.log(`     OVERALL (DB): ${marchOverallReturn.return.toFixed(2)}%`);
  console.log(`     IBKR+XTB (Agregado): ${marchAggIbkrXtb.return.toFixed(2)}%`);
  
  console.log('');
  if (marchBinanceReturn.return > 0 && marchIbkrReturn.return < 0 && marchXtbReturn.return < 0) {
    console.log('  🔍 ¡BINANCE tuvo rendimiento POSITIVO mientras IBKR y XTB fueron negativos!');
    console.log('     Esto explica por qué OVERALL muestra menos pérdida que IBKR+XTB');
  }
  
  console.log('');
  console.log('═'.repeat(80));
  console.log('  ✅ DIAGNÓSTICO COMPLETO');
  console.log('═'.repeat(80));
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
