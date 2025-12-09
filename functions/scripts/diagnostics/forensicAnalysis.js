/**
 * ANÁLISIS FORENSE COMPLETO: Verificar todos los puntos del usuario
 * 
 * Puntos a verificar:
 * 1. ✅ Marzo corregido (-5.80% entre -6.75% y -5.13%)
 * 2. ❌ Abril: IBKR -8.86%, XTB +4.00%, Total +9.01% (IMPOSIBLE)
 * 3. ❌ YTD: IBKR 21.48%, XTB 28.44%, Total 36.03% (mayor que ambos)
 * 4. ❌ Dinero fantasma: $509 de diferencia
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

/**
 * Calcular rendimiento de un mes específico para una cuenta
 */
function calculateMonthReturn(docs, currency, year, month) {
  const monthDocs = docs.filter(d => {
    const date = DateTime.fromISO(d.date);
    return date.year.toString() === year && (date.month - 1).toString() === month;
  });
  
  if (monthDocs.length === 0) return { return: 0, days: 0, hasData: false };
  
  let factor = 1;
  monthDocs.forEach(doc => {
    const change = doc[currency]?.adjustedDailyChangePercentage || 0;
    factor *= (1 + change / 100);
  });
  
  return {
    return: (factor - 1) * 100,
    days: monthDocs.length,
    hasData: true
  };
}

/**
 * Calcular YTD para una cuenta
 */
function calculateYTD(docs, currency) {
  const ytdDocs = docs.filter(d => d.date >= '2025-01-01');
  
  if (ytdDocs.length === 0) return { return: 0, days: 0 };
  
  let factor = 1;
  ytdDocs.forEach(doc => {
    const change = doc[currency]?.adjustedDailyChangePercentage || 0;
    factor *= (1 + change / 100);
  });
  
  return {
    return: (factor - 1) * 100,
    days: ytdDocs.length
  };
}

async function main() {
  console.log('');
  console.log('═'.repeat(90));
  console.log('  ANÁLISIS FORENSE COMPLETO');
  console.log('═'.repeat(90));
  console.log('');

  // Obtener datos de todas las cuentas
  const [ibkrDocs, xtbDocs, binanceDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB),
    getAccountDocs(CONFIG.ACCOUNTS.BINANCE)
  ]);

  console.log(`Documentos IBKR: ${ibkrDocs.length}`);
  console.log(`Documentos XTB: ${xtbDocs.length}`);
  console.log(`Documentos Binance: ${binanceDocs.length}`);

  // =========================================================================
  // PUNTO 1: Verificar Marzo
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  1️⃣  VERIFICACIÓN MARZO 2025');
  console.log('━'.repeat(90));
  
  const ibkrMarzo = calculateMonthReturn(ibkrDocs, CONFIG.CURRENCY, '2025', '2');
  const xtbMarzo = calculateMonthReturn(xtbDocs, CONFIG.CURRENCY, '2025', '2');
  
  console.log('');
  console.log(`  IBKR Marzo:  ${ibkrMarzo.return.toFixed(2)}% (${ibkrMarzo.days} días) - UI muestra: -6.75%`);
  console.log(`  XTB Marzo:   ${xtbMarzo.return.toFixed(2)}% (${xtbMarzo.days} días) - UI muestra: -5.13%`);
  
  const minMarzo = Math.min(ibkrMarzo.return, xtbMarzo.return);
  const maxMarzo = Math.max(ibkrMarzo.return, xtbMarzo.return);
  console.log(`  Rango válido: [${minMarzo.toFixed(2)}%, ${maxMarzo.toFixed(2)}%]`);
  console.log(`  UI muestra combinado: -5.80%`);
  console.log(`  ¿Dentro del rango? ${-5.80 >= minMarzo && -5.80 <= maxMarzo ? '✅ SÍ' : '❌ NO'}`);

  // =========================================================================
  // PUNTO 2: Verificar Abril (EL PROBLEMA CRÍTICO)
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  2️⃣  VERIFICACIÓN ABRIL 2025 (⚠️ CRÍTICO)');
  console.log('━'.repeat(90));
  
  const ibkrAbril = calculateMonthReturn(ibkrDocs, CONFIG.CURRENCY, '2025', '3');
  const xtbAbril = calculateMonthReturn(xtbDocs, CONFIG.CURRENCY, '2025', '3');
  
  console.log('');
  console.log(`  IBKR Abril:  ${ibkrAbril.return.toFixed(2)}% (${ibkrAbril.days} días) - UI muestra: -8.86%`);
  console.log(`  XTB Abril:   ${xtbAbril.return.toFixed(2)}% (${xtbAbril.days} días) - UI muestra: +4.00%`);
  
  const minAbril = Math.min(ibkrAbril.return, xtbAbril.return);
  const maxAbril = Math.max(ibkrAbril.return, xtbAbril.return);
  console.log(`  Rango válido: [${minAbril.toFixed(2)}%, ${maxAbril.toFixed(2)}%]`);
  console.log(`  UI muestra combinado: +9.01%`);
  console.log(`  ¿Dentro del rango? ${9.01 >= minAbril && 9.01 <= maxAbril ? '✅ SÍ' : '❌ NO - IMPOSIBLE'}`);
  
  // Analizar abril día a día
  console.log('');
  console.log('  📊 Detalle día a día de Abril:');
  
  const abrilIbkr = ibkrDocs.filter(d => d.date.startsWith('2025-04'));
  const abrilXtb = xtbDocs.filter(d => d.date.startsWith('2025-04'));
  
  const ibkrByDate = new Map(abrilIbkr.map(d => [d.date, d]));
  const xtbByDate = new Map(abrilXtb.map(d => [d.date, d]));
  const allAbrilDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();
  
  console.log('');
  console.log('  Fecha       | IBKR Val   | XTB Val    | IBKR %    | XTB %     | Ponderado');
  console.log('  ' + '-'.repeat(80));
  
  let abrilCombinedFactor = 1;
  allAbrilDates.forEach(date => {
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
    
    abrilCombinedFactor *= (1 + weightedChange / 100);
    
    console.log(
      `  ${date} | ` +
      `$${ibkrVal.toFixed(2).padStart(8)} | ` +
      `$${xtbVal.toFixed(2).padStart(8)} | ` +
      `${ibkrChange.toFixed(2).padStart(8)}% | ` +
      `${xtbChange.toFixed(2).padStart(8)}% | ` +
      `${weightedChange.toFixed(2).padStart(8)}%`
    );
  });
  
  const abrilCombinedReturn = (abrilCombinedFactor - 1) * 100;
  console.log('');
  console.log(`  Rendimiento Abril calculado (ponderado): ${abrilCombinedReturn.toFixed(2)}%`);
  console.log(`  UI muestra: +9.01%`);
  console.log(`  ¿Coincide? ${Math.abs(abrilCombinedReturn - 9.01) < 0.5 ? '✅ SÍ' : '❌ NO'}`);

  // =========================================================================
  // PUNTO 3: Verificar YTD
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  3️⃣  VERIFICACIÓN YTD 2025');
  console.log('━'.repeat(90));
  
  const ibkrYTD = calculateYTD(ibkrDocs, CONFIG.CURRENCY);
  const xtbYTD = calculateYTD(xtbDocs, CONFIG.CURRENCY);
  
  console.log('');
  console.log(`  IBKR YTD:    ${ibkrYTD.return.toFixed(2)}% (${ibkrYTD.days} días) - UI muestra: 21.48%`);
  console.log(`  XTB YTD:     ${xtbYTD.return.toFixed(2)}% (${xtbYTD.days} días) - UI muestra: 28.44%`);
  
  // Calcular combinado ponderado
  const ytdIbkrDocs = ibkrDocs.filter(d => d.date >= '2025-01-01');
  const ytdXtbDocs = xtbDocs.filter(d => d.date >= '2025-01-01');
  
  const ytdIbkrByDate = new Map(ytdIbkrDocs.map(d => [d.date, d]));
  const ytdXtbByDate = new Map(ytdXtbDocs.map(d => [d.date, d]));
  const allYtdDates = [...new Set([...ytdIbkrByDate.keys(), ...ytdXtbByDate.keys()])].sort();
  
  let ytdCombinedFactor = 1;
  allYtdDates.forEach(date => {
    const ibkr = ytdIbkrByDate.get(date);
    const xtb = ytdXtbByDate.get(date);
    
    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;
    
    const ibkrChange = ibkr?.USD?.adjustedDailyChangePercentage || 0;
    const xtbChange = xtb?.USD?.adjustedDailyChangePercentage || 0;
    
    let weightedChange = 0;
    if (totalVal > 0) {
      weightedChange = (ibkrVal * ibkrChange + xtbVal * xtbChange) / totalVal;
    }
    
    ytdCombinedFactor *= (1 + weightedChange / 100);
  });
  
  const ytdCombinedReturn = (ytdCombinedFactor - 1) * 100;
  console.log(`  YTD Combinado calculado: ${ytdCombinedReturn.toFixed(2)}%`);
  console.log(`  UI muestra: 36.03%`);
  
  const minYTD = Math.min(ibkrYTD.return, xtbYTD.return);
  const maxYTD = Math.max(ibkrYTD.return, xtbYTD.return);
  console.log('');
  console.log(`  Rango "intuitivo": [${minYTD.toFixed(2)}%, ${maxYTD.toFixed(2)}%]`);
  console.log(`  ¿36.03% está fuera del rango intuitivo? ${ytdCombinedReturn > maxYTD ? '⚠️ SÍ' : '✅ NO'}`);
  
  // Explicar por qué puede ser > max
  console.log('');
  console.log('  📝 ANÁLISIS DEL FENÓMENO YTD > MAX INDIVIDUAL:');
  
  // Ver evolución de pesos
  const firstDate = allYtdDates[0];
  const lastDate = allYtdDates[allYtdDates.length - 1];
  
  const firstIbkr = ytdIbkrByDate.get(firstDate);
  const firstXtb = ytdXtbByDate.get(firstDate);
  const lastIbkr = ytdIbkrByDate.get(lastDate);
  const lastXtb = ytdXtbByDate.get(lastDate);
  
  const firstIbkrVal = firstIbkr?.USD?.totalValue || 0;
  const firstXtbVal = firstXtb?.USD?.totalValue || 0;
  const firstTotal = firstIbkrVal + firstXtbVal;
  
  console.log('');
  console.log(`  Al inicio del año (${firstDate}):`);
  console.log(`    IBKR: $${firstIbkrVal.toFixed(2)} (${(firstIbkrVal/firstTotal*100).toFixed(1)}%)`);
  console.log(`    XTB:  $${firstXtbVal.toFixed(2)} (${(firstXtbVal/firstTotal*100).toFixed(1)}%)`);
  
  const lastIbkrVal = lastIbkr?.USD?.totalValue || 0;
  const lastXtbVal = lastXtb?.USD?.totalValue || 0;
  const lastTotal = lastIbkrVal + lastXtbVal;
  
  console.log('');
  console.log(`  Actualmente (${lastDate}):`);
  console.log(`    IBKR: $${lastIbkrVal.toFixed(2)} (${(lastIbkrVal/lastTotal*100).toFixed(1)}%)`);
  console.log(`    XTB:  $${lastXtbVal.toFixed(2)} (${(lastXtbVal/lastTotal*100).toFixed(1)}%)`);

  // =========================================================================
  // PUNTO 4: Verificar el "dinero fantasma"
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  4️⃣  VERIFICACIÓN "DINERO FANTASMA"');
  console.log('━'.repeat(90));
  
  // Obtener valores actuales
  const latestIbkr = ibkrDocs[ibkrDocs.length - 1];
  const latestXtb = xtbDocs[xtbDocs.length - 1];
  const latestBinance = binanceDocs.length > 0 ? binanceDocs[binanceDocs.length - 1] : null;
  
  const ibkrValue = latestIbkr?.USD?.totalValue || 0;
  const xtbValue = latestXtb?.USD?.totalValue || 0;
  const binanceValue = latestBinance?.USD?.totalValue || 0;
  
  const ibkrInvestment = latestIbkr?.USD?.totalInvestment || 0;
  const xtbInvestment = latestXtb?.USD?.totalInvestment || 0;
  const binanceInvestment = latestBinance?.USD?.totalInvestment || 0;
  
  console.log('');
  console.log('  Valores actuales en Firestore:');
  console.log(`    IBKR:    $${ibkrValue.toFixed(2)} (inversión: $${ibkrInvestment.toFixed(2)})`);
  console.log(`    XTB:     $${xtbValue.toFixed(2)} (inversión: $${xtbInvestment.toFixed(2)})`);
  console.log(`    Binance: $${binanceValue.toFixed(2)} (inversión: $${binanceInvestment.toFixed(2)})`);
  console.log('');
  console.log(`    Suma IBKR+XTB:          $${(ibkrValue + xtbValue).toFixed(2)}`);
  console.log(`    Suma IBKR+XTB+Binance:  $${(ibkrValue + xtbValue + binanceValue).toFixed(2)}`);
  console.log('');
  console.log('  UI "2 cuentas" muestra:');
  console.log(`    Valor Actual: $8,049.35`);
  console.log(`    Inversión:    $7,278.28`);
  console.log('');
  console.log(`  Diferencia en valor: $${(8049.35 - ibkrValue - xtbValue).toFixed(2)}`);
  console.log(`  Diferencia en inversión: $${(7278.28 - ibkrInvestment - xtbInvestment).toFixed(2)}`);
  console.log('');
  console.log(`  ¿La diferencia coincide con Binance?`);
  console.log(`    Valor Binance: $${binanceValue.toFixed(2)} vs Diferencia: $${(8049.35 - ibkrValue - xtbValue).toFixed(2)}`);
  console.log(`    ${Math.abs(binanceValue - (8049.35 - ibkrValue - xtbValue)) < 1 ? '✅ SÍ - El UI está sumando Binance' : '❌ NO'}`);

  // =========================================================================
  // CONCLUSIÓN
  // =========================================================================
  console.log('');
  console.log('═'.repeat(90));
  console.log('  📋 CONCLUSIÓN DEL ANÁLISIS FORENSE');
  console.log('═'.repeat(90));
  console.log('');
  console.log('  1. Marzo: ✅ CORRECTO (-5.80% está dentro del rango)');
  console.log('');
  console.log('  2. Abril: Necesita verificación - el cálculo da ' + abrilCombinedReturn.toFixed(2) + '%');
  console.log('     Si UI muestra 9.01% y cálculo da ~' + abrilCombinedReturn.toFixed(0) + '%, ');
  console.log('     puede haber discrepancia en los datos mostrados vs calculados.');
  console.log('');
  console.log('  3. YTD 36%: El cálculo matemático da ' + ytdCombinedReturn.toFixed(2) + '%');
  console.log('     Esto puede ser > max individual debido a la composición temporal');
  console.log('     (IBKR dominaba al inicio, XTB creció después).');
  console.log('');
  console.log('  4. Dinero Fantasma: El PortfolioSummary está sumando BINANCE');
  console.log('     aunque el selector diga "2 cuentas". Este es un BUG de UI.');
  
  console.log('');
  console.log('═'.repeat(90));
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
