/**
 * Script de Diagnóstico: Fechas de Inicio por Cuenta
 * 
 * Verifica cuándo empezó cada cuenta y cómo esto afecta
 * la agregación multi-cuenta.
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

async function getAccountDocs(accountId) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
  const snapshot = await db.collection(path).orderBy('date', 'asc').get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function main() {
  console.log('');
  console.log('═'.repeat(80));
  console.log('  ANÁLISIS DE FECHAS DE INICIO POR CUENTA');
  console.log('═'.repeat(80));
  console.log('');

  // Obtener datos de cada cuenta
  const [ibkrDocs, xtbDocs, binanceDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB),
    getAccountDocs(CONFIG.ACCOUNTS.BINANCE)
  ]);

  console.log('📊 Rangos de fechas por cuenta:');
  console.log('');
  console.log(`   IBKR: ${ibkrDocs[0]?.date || 'N/A'} → ${ibkrDocs[ibkrDocs.length-1]?.date || 'N/A'} (${ibkrDocs.length} docs)`);
  console.log(`   XTB:  ${xtbDocs[0]?.date || 'N/A'} → ${xtbDocs[xtbDocs.length-1]?.date || 'N/A'} (${xtbDocs.length} docs)`);
  console.log(`   Binance: ${binanceDocs[0]?.date || 'N/A'} → ${binanceDocs[binanceDocs.length-1]?.date || 'N/A'} (${binanceDocs.length} docs)`);
  
  // Verificar marzo 2025 específicamente
  console.log('');
  console.log('━'.repeat(80));
  console.log('  MARZO 2025 - ANÁLISIS DETALLADO');
  console.log('━'.repeat(80));
  
  const marchIbkr = ibkrDocs.filter(d => d.date.startsWith('2025-03'));
  const marchXtb = xtbDocs.filter(d => d.date.startsWith('2025-03'));
  
  console.log('');
  console.log(`   IBKR en marzo: ${marchIbkr.length} días`);
  console.log(`   XTB en marzo: ${marchXtb.length} días`);
  
  // Verificar si hay días donde solo una cuenta tiene datos
  const ibkrDates = new Set(marchIbkr.map(d => d.date));
  const xtbDates = new Set(marchXtb.map(d => d.date));
  
  const onlyIbkr = marchIbkr.filter(d => !xtbDates.has(d.date));
  const onlyXtb = marchXtb.filter(d => !ibkrDates.has(d.date));
  
  console.log('');
  console.log(`   Días SOLO con IBKR: ${onlyIbkr.length}`);
  console.log(`   Días SOLO con XTB: ${onlyXtb.length}`);
  
  if (onlyIbkr.length > 0) {
    console.log('');
    console.log('   📅 Fechas solo con IBKR:');
    onlyIbkr.forEach(d => console.log(`      ${d.date}`));
  }
  
  if (onlyXtb.length > 0) {
    console.log('');
    console.log('   📅 Fechas solo con XTB:');
    onlyXtb.forEach(d => console.log(`      ${d.date}`));
  }
  
  // Calcular rendimiento de marzo con diferentes métodos
  console.log('');
  console.log('━'.repeat(80));
  console.log('  CÁLCULO DE RENDIMIENTO DE MARZO 2025');
  console.log('━'.repeat(80));
  
  // Método 1: Compuesto IBKR
  let ibkrFactor = 1;
  marchIbkr.forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    ibkrFactor *= (1 + change / 100);
  });
  console.log(`\n   IBKR (compuesto directo): ${((ibkrFactor - 1) * 100).toFixed(2)}%`);
  
  // Método 2: Compuesto XTB  
  let xtbFactor = 1;
  marchXtb.forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    xtbFactor *= (1 + change / 100);
  });
  console.log(`   XTB (compuesto directo): ${((xtbFactor - 1) * 100).toFixed(2)}%`);
  
  // Método 3: Agregación ponderada día a día
  const allMarchDates = [...new Set([...ibkrDates, ...xtbDates])].sort();
  let aggregatedFactor = 1;
  
  const ibkrByDate = new Map(marchIbkr.map(d => [d.date, d]));
  const xtbByDate = new Map(marchXtb.map(d => [d.date, d]));
  
  console.log('');
  console.log('   Fecha       | IBKR Val | XTB Val  | IBKR %   | XTB %    | Weighted % | Factor');
  console.log('   ' + '-'.repeat(85));
  
  allMarchDates.forEach(date => {
    const ibkr = ibkrByDate.get(date);
    const xtb = xtbByDate.get(date);
    
    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;
    
    const ibkrChange = ibkr?.USD?.adjustedDailyChangePercentage || 0;
    const xtbChange = xtb?.USD?.adjustedDailyChangePercentage || 0;
    
    let weightedChange = 0;
    if (totalVal > 0) {
      weightedChange = (ibkrVal * ibkrChange + xtbVal * xtbChange) / totalVal;
    }
    
    aggregatedFactor *= (1 + weightedChange / 100);
    
    console.log(
      `   ${date} | ` +
      `$${ibkrVal.toFixed(2).padStart(7)} | ` +
      `$${xtbVal.toFixed(2).padStart(7)} | ` +
      `${ibkrChange.toFixed(2).padStart(7)}% | ` +
      `${xtbChange.toFixed(2).padStart(7)}% | ` +
      `${weightedChange.toFixed(2).padStart(9)}% | ` +
      `${aggregatedFactor.toFixed(4)}`
    );
  });
  
  console.log('');
  console.log(`   Agregado IBKR+XTB (ponderado): ${((aggregatedFactor - 1) * 100).toFixed(2)}%`);
  
  // Verificar valores al inicio y fin de marzo
  console.log('');
  console.log('━'.repeat(80));
  console.log('  VALORES AL INICIO Y FIN DE MARZO');
  console.log('━'.repeat(80));
  
  const firstMarchDate = allMarchDates[0];
  const lastMarchDate = allMarchDates[allMarchDates.length - 1];
  
  const firstIbkr = ibkrByDate.get(firstMarchDate);
  const firstXtb = xtbByDate.get(firstMarchDate);
  const lastIbkr = ibkrByDate.get(lastMarchDate);
  const lastXtb = xtbByDate.get(lastMarchDate);
  
  console.log('');
  console.log(`   Primer día (${firstMarchDate}):`);
  console.log(`      IBKR: $${firstIbkr?.USD?.totalValue?.toFixed(2) || 0}`);
  console.log(`      XTB:  $${firstXtb?.USD?.totalValue?.toFixed(2) || 0}`);
  console.log(`      Total: $${((firstIbkr?.USD?.totalValue || 0) + (firstXtb?.USD?.totalValue || 0)).toFixed(2)}`);
  
  console.log('');
  console.log(`   Último día (${lastMarchDate}):`);
  console.log(`      IBKR: $${lastIbkr?.USD?.totalValue?.toFixed(2) || 0}`);
  console.log(`      XTB:  $${lastXtb?.USD?.totalValue?.toFixed(2) || 0}`);
  console.log(`      Total: $${((lastIbkr?.USD?.totalValue || 0) + (lastXtb?.USD?.totalValue || 0)).toFixed(2)}`);
  
  // Calcular cambio simple (sin considerar cashflows)
  const totalStart = (firstIbkr?.USD?.totalValue || 0) + (firstXtb?.USD?.totalValue || 0);
  const totalEnd = (lastIbkr?.USD?.totalValue || 0) + (lastXtb?.USD?.totalValue || 0);
  const simpleChange = totalStart > 0 ? ((totalEnd - totalStart) / totalStart) * 100 : 0;
  
  console.log('');
  console.log(`   Cambio simple (sin cashflows): ${simpleChange.toFixed(2)}%`);
  
  // Verificar cashflows del mes
  console.log('');
  console.log('━'.repeat(80));
  console.log('  CASHFLOWS EN MARZO 2025');
  console.log('━'.repeat(80));
  
  let ibkrCashflow = 0;
  let xtbCashflow = 0;
  
  marchIbkr.forEach(d => {
    const cf = d.USD?.totalCashFlow || 0;
    if (cf !== 0) {
      ibkrCashflow += cf;
      console.log(`   ${d.date} IBKR: $${cf.toFixed(2)}`);
    }
  });
  
  marchXtb.forEach(d => {
    const cf = d.USD?.totalCashFlow || 0;
    if (cf !== 0) {
      xtbCashflow += cf;
      console.log(`   ${d.date} XTB: $${cf.toFixed(2)}`);
    }
  });
  
  console.log('');
  console.log(`   Total cashflow IBKR: $${ibkrCashflow.toFixed(2)}`);
  console.log(`   Total cashflow XTB: $${xtbCashflow.toFixed(2)}`);
  
  console.log('');
  console.log('═'.repeat(80));
  console.log('  ✅ ANÁLISIS COMPLETO');
  console.log('═'.repeat(80));
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
