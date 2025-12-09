/**
 * DIAGNÓSTICO: Verificar inconsistencia entre OVERALL y multi-cuenta
 * 
 * Problema reportado:
 * - "2 cuentas" (IBKR+XTB) Enero: 3.17%
 * - "Todas las cuentas" (OVERALL) Enero: 1.67%
 * - Binance no existía en Enero, debería ser igual
 * 
 * Hipótesis:
 * 1. OVERALL usa portfolioPerformance/{userId}/dates (datos pre-calculados)
 * 2. Multi-cuenta usa getMultiAccountHistoricalReturns (agregación en tiempo real)
 * 3. El OVERALL puede tener datos de Binance desde antes (con valor 0?)
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

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  ACCOUNTS: {
    IBKR: 'BZHvXz4QT2yqqqlFP22X',
    XTB: 'Z3gnboYgRlTvSZNGSu8j',
    BINANCE: 'zHZCvwpQeA2HoYMxDtPF'
  },
  CURRENCY: 'USD'
};

async function getOverallDocs() {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function getAccountDocs(accountId) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

function calculateMonthReturn(docs, currency, monthPrefix) {
  const monthDocs = docs.filter(d => d.date.startsWith(monthPrefix));
  if (monthDocs.length === 0) return { return: 0, days: 0, hasData: false };
  
  let factor = 1;
  monthDocs.forEach(d => {
    const change = d[currency]?.adjustedDailyChangePercentage || 0;
    factor *= (1 + change / 100);
  });
  
  return {
    return: (factor - 1) * 100,
    days: monthDocs.length,
    hasData: true
  };
}

async function main() {
  console.log('');
  console.log('═'.repeat(100));
  console.log('  DIAGNÓSTICO: OVERALL vs Multi-Cuenta - ¿Por qué difieren?');
  console.log('═'.repeat(100));
  console.log('');

  // Obtener todos los datos
  const [overallDocs, ibkrDocs, xtbDocs, binanceDocs] = await Promise.all([
    getOverallDocs(),
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB),
    getAccountDocs(CONFIG.ACCOUNTS.BINANCE)
  ]);

  console.log('Documentos encontrados:');
  console.log(`  OVERALL: ${overallDocs.length}`);
  console.log(`  IBKR:    ${ibkrDocs.length}`);
  console.log(`  XTB:     ${xtbDocs.length}`);
  console.log(`  Binance: ${binanceDocs.length}`);

  // Verificar fechas de cada colección
  console.log('');
  console.log('Rango de fechas:');
  console.log(`  OVERALL: ${overallDocs[0]?.date} a ${overallDocs[overallDocs.length-1]?.date}`);
  console.log(`  IBKR:    ${ibkrDocs[0]?.date} a ${ibkrDocs[ibkrDocs.length-1]?.date}`);
  console.log(`  XTB:     ${xtbDocs[0]?.date} a ${xtbDocs[xtbDocs.length-1]?.date}`);
  console.log(`  Binance: ${binanceDocs[0]?.date} a ${binanceDocs[binanceDocs.length-1]?.date}`);

  // =========================================================================
  // COMPARACIÓN: OVERALL vs suma de cuentas individuales
  // =========================================================================
  console.log('');
  console.log('━'.repeat(100));
  console.log('  COMPARACIÓN ENERO 2025: ¿Por qué OVERALL ≠ IBKR+XTB?');
  console.log('━'.repeat(100));
  console.log('');

  // Enero 2025
  const eneroOverall = overallDocs.filter(d => d.date.startsWith('2025-01'));
  const eneroIbkr = ibkrDocs.filter(d => d.date.startsWith('2025-01'));
  const eneroXtb = xtbDocs.filter(d => d.date.startsWith('2025-01'));
  const eneroBinance = binanceDocs.filter(d => d.date.startsWith('2025-01'));

  console.log(`  Documentos en Enero 2025:`);
  console.log(`    OVERALL: ${eneroOverall.length} días`);
  console.log(`    IBKR:    ${eneroIbkr.length} días`);
  console.log(`    XTB:     ${eneroXtb.length} días`);
  console.log(`    Binance: ${eneroBinance.length} días`);
  console.log('');

  // Calcular rendimiento de cada fuente
  const overallEnero = calculateMonthReturn(overallDocs, CONFIG.CURRENCY, '2025-01');
  const ibkrEnero = calculateMonthReturn(ibkrDocs, CONFIG.CURRENCY, '2025-01');
  const xtbEnero = calculateMonthReturn(xtbDocs, CONFIG.CURRENCY, '2025-01');
  const binanceEnero = calculateMonthReturn(binanceDocs, CONFIG.CURRENCY, '2025-01');

  console.log(`  Rendimiento Enero:`);
  console.log(`    OVERALL (desde dates):     ${overallEnero.return.toFixed(2)}%`);
  console.log(`    IBKR individual:           ${ibkrEnero.return.toFixed(2)}%`);
  console.log(`    XTB individual:            ${xtbEnero.return.toFixed(2)}%`);
  console.log(`    Binance individual:        ${binanceEnero.hasData ? binanceEnero.return.toFixed(2) + '%' : 'N/A (sin datos)'}`);

  // Mostrar día a día de enero para entender la diferencia
  console.log('');
  console.log('━'.repeat(100));
  console.log('  DETALLE DÍA A DÍA: Enero 2025');
  console.log('━'.repeat(100));
  console.log('');

  console.log('  Fecha       | OVERALL Val | IBKR Val   | XTB Val    | OVERALL % | IBKR %    | XTB %');
  console.log('  ' + '-'.repeat(95));

  const overallByDate = new Map(eneroOverall.map(d => [d.date, d]));
  const ibkrByDate = new Map(eneroIbkr.map(d => [d.date, d]));
  const xtbByDate = new Map(eneroXtb.map(d => [d.date, d]));

  const allEneroDatesSorted = [...new Set([...overallByDate.keys(), ...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();

  allEneroDatesSorted.slice(0, 10).forEach(date => {
    const o = overallByDate.get(date);
    const i = ibkrByDate.get(date);
    const x = xtbByDate.get(date);

    const oVal = o?.USD?.totalValue || 0;
    const iVal = i?.USD?.totalValue || 0;
    const xVal = x?.USD?.totalValue || 0;

    const oChg = o?.USD?.adjustedDailyChangePercentage || 0;
    const iChg = i?.USD?.adjustedDailyChangePercentage || 0;
    const xChg = x?.USD?.adjustedDailyChangePercentage || 0;

    const sumVal = iVal + xVal;

    console.log(
      `  ${date} | ` +
      `$${oVal.toFixed(2).padStart(9)} | ` +
      `$${iVal.toFixed(2).padStart(8)} | ` +
      `$${xVal.toFixed(2).padStart(8)} | ` +
      `${oChg.toFixed(2).padStart(8)}% | ` +
      `${iChg.toFixed(2).padStart(8)}% | ` +
      `${xChg.toFixed(2).padStart(8)}%`
    );

    // Verificar si OVERALL.totalValue = IBKR.totalValue + XTB.totalValue
    if (Math.abs(oVal - sumVal) > 1) {
      console.log(`             ⚠️ OVERALL ($${oVal.toFixed(2)}) ≠ IBKR+XTB ($${sumVal.toFixed(2)}), diff: $${(oVal - sumVal).toFixed(2)}`);
    }
  });

  // =========================================================================
  // VERIFICAR: ¿El adjustedDailyChangePercentage de OVERALL es correcto?
  // =========================================================================
  console.log('');
  console.log('━'.repeat(100));
  console.log('  ANÁLISIS: ¿El adjustedDailyChangePercentage de OVERALL es correcto?');
  console.log('━'.repeat(100));
  console.log('');

  // Verificar si el cambio de OVERALL está siendo calculado correctamente
  // El cambio ponderado debería ser: (IBKR_val * IBKR_chg + XTB_val * XTB_chg) / (IBKR_val + XTB_val)
  console.log('  Verificando si OVERALL.change es el promedio ponderado de las cuentas...');
  console.log('');

  let discrepancies = 0;
  allEneroDatesSorted.forEach(date => {
    const o = overallByDate.get(date);
    const i = ibkrByDate.get(date);
    const x = xtbByDate.get(date);

    if (!o || !i) return;

    const oChg = o?.USD?.adjustedDailyChangePercentage || 0;
    const iVal = i?.USD?.totalValue || 0;
    const xVal = x?.USD?.totalValue || 0;
    const iChg = i?.USD?.adjustedDailyChangePercentage || 0;
    const xChg = x?.USD?.adjustedDailyChangePercentage || 0;

    // Calcular valor pre-cambio para ponderación correcta
    const iPreChange = iChg !== 0 ? iVal / (1 + iChg / 100) : iVal;
    const xPreChange = xChg !== 0 ? xVal / (1 + xChg / 100) : xVal;
    const totalPreChange = iPreChange + xPreChange;

    // Cambio ponderado esperado (usando valor pre-cambio)
    const expectedChg = totalPreChange > 0 
      ? (iPreChange * iChg + xPreChange * xChg) / totalPreChange 
      : 0;

    const diff = Math.abs(oChg - expectedChg);
    if (diff > 0.1) {
      discrepancies++;
      console.log(`  ⚠️ ${date}: OVERALL=${oChg.toFixed(2)}%, Esperado=${expectedChg.toFixed(2)}%, Diff=${diff.toFixed(2)}%`);
    }
  });

  if (discrepancies === 0) {
    console.log('  ✅ El adjustedDailyChangePercentage de OVERALL coincide con el promedio ponderado');
    console.log('');
    console.log('  El problema NO está en los datos guardados.');
    console.log('  El problema podría estar en cómo se calcula el rendimiento mensual.');
  } else {
    console.log(`  ❌ Se encontraron ${discrepancies} discrepancias en el adjustedDailyChangePercentage`);
  }

  // =========================================================================
  // CALCULAR: ¿Qué debería dar el rendimiento de Enero según OVERALL?
  // =========================================================================
  console.log('');
  console.log('━'.repeat(100));
  console.log('  RENDIMIENTO MENSUAL: Cálculo paso a paso');
  console.log('━'.repeat(100));
  console.log('');

  // Calcular mes de enero usando los datos de OVERALL
  let factorOverall = 1;
  let factorRecalculado = 1;

  console.log('  Fecha       | OVERALL %  | Factor OVERALL | Recalculado % | Factor Recalc');
  console.log('  ' + '-'.repeat(80));

  allEneroDatesSorted.forEach(date => {
    const o = overallByDate.get(date);
    const i = ibkrByDate.get(date);
    const x = xtbByDate.get(date);

    const oChg = o?.USD?.adjustedDailyChangePercentage || 0;
    
    // Recalcular con valor pre-cambio
    const iVal = i?.USD?.totalValue || 0;
    const xVal = x?.USD?.totalValue || 0;
    const iChg = i?.USD?.adjustedDailyChangePercentage || 0;
    const xChg = x?.USD?.adjustedDailyChangePercentage || 0;

    const iPreChange = iChg !== 0 ? iVal / (1 + iChg / 100) : iVal;
    const xPreChange = xChg !== 0 ? xVal / (1 + xChg / 100) : xVal;
    const totalPreChange = iPreChange + xPreChange;
    const recalcChg = totalPreChange > 0 ? (iPreChange * iChg + xPreChange * xChg) / totalPreChange : 0;

    factorOverall *= (1 + oChg / 100);
    factorRecalculado *= (1 + recalcChg / 100);

    console.log(
      `  ${date} | ` +
      `${oChg.toFixed(2).padStart(9)}% | ` +
      `${factorOverall.toFixed(6).padStart(14)} | ` +
      `${recalcChg.toFixed(2).padStart(12)}% | ` +
      `${factorRecalculado.toFixed(6)}`
    );
  });

  const rendimientoOverall = (factorOverall - 1) * 100;
  const rendimientoRecalculado = (factorRecalculado - 1) * 100;

  console.log('');
  console.log(`  Rendimiento Enero usando OVERALL.adjustedDailyChangePercentage: ${rendimientoOverall.toFixed(2)}%`);
  console.log(`  Rendimiento Enero recalculado (IBKR+XTB con pre-change):        ${rendimientoRecalculado.toFixed(2)}%`);
  console.log(`  UI muestra "Todas las cuentas" Enero: 1.67%`);
  console.log(`  UI muestra "2 cuentas" Enero: 3.17%`);

  console.log('');
  console.log('═'.repeat(100));
  console.log('  📋 CONCLUSIÓN');
  console.log('═'.repeat(100));
  console.log('');

  if (Math.abs(rendimientoOverall - 1.67) < 0.5) {
    console.log('  El cálculo de OVERALL coincide con el UI (1.67%).');
    console.log('  El problema está en que OVERALL usa datos diferentes a IBKR+XTB agregados.');
    console.log('');
    console.log('  Posibles causas:');
    console.log('  1. OVERALL incluye más cuentas (aunque Binance no tenía datos en enero)');
    console.log('  2. Los adjustedDailyChangePercentage de OVERALL fueron calculados diferente');
  } else {
    console.log('  El cálculo de OVERALL NO coincide con el UI.');
    console.log('  Hay algo más pasando en el frontend o en los cálculos.');
  }

  console.log('');
  console.log('═'.repeat(100));

  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
