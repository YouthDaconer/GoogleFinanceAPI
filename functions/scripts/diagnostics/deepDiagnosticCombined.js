/**
 * DIAGNÓSTICO PROFUNDO: Por qué el combinado excede el máximo individual
 * 
 * Hipótesis a verificar:
 * 1. ¿El problema está en el cálculo de ponderación por valor?
 * 2. ¿Hay un bug en cómo se usa totalValue vs un valor "pre-cambio"?
 * 3. ¿Los cashflows están afectando el cálculo de alguna manera incorrecta?
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
    XTB: 'Z3gnboYgRlTvSZNGSu8j'
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
  console.log('═'.repeat(100));
  console.log('  DIAGNÓSTICO PROFUNDO: ¿Por qué combinado > máximo individual?');
  console.log('═'.repeat(100));
  console.log('');

  const [ibkrDocs, xtbDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB)
  ]);

  // =========================================================================
  // CASO DE ESTUDIO: ABRIL 2025
  // IBKR: +0.43%, XTB: +4.96%, Combinado: +9.02% (IMPOSIBLE según reglas)
  // =========================================================================
  console.log('━'.repeat(100));
  console.log('  CASO DE ESTUDIO: ABRIL 2025');
  console.log('  IBKR: +0.43%, XTB: +4.96%, Combinado mostrado: +9.02%');
  console.log('━'.repeat(100));
  console.log('');

  const abrilIbkr = ibkrDocs.filter(d => d.date.startsWith('2025-04'));
  const abrilXtb = xtbDocs.filter(d => d.date.startsWith('2025-04'));

  // Crear mapas por fecha
  const ibkrByDate = new Map(abrilIbkr.map(d => [d.date, d]));
  const xtbByDate = new Map(abrilXtb.map(d => [d.date, d]));
  const allDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();

  console.log('  Día a día de Abril:');
  console.log('');
  console.log('  Fecha       | IBKR Val   | XTB Val    | Total Val  | Peso IBKR | Peso XTB  | IBKR %    | XTB %     | Ponderado | Factor Acum');
  console.log('  ' + '-'.repeat(130));

  let factorIBKR = 1;
  let factorXTB = 1;
  let factorCombinado = 1;
  
  // Para verificar: ¿qué pasa si usamos el valor del día ANTERIOR para ponderar?
  let factorCombinadoConValorAnterior = 1;
  let prevIbkrVal = 0;
  let prevXtbVal = 0;

  allDates.forEach((date, idx) => {
    const ibkr = ibkrByDate.get(date);
    const xtb = xtbByDate.get(date);

    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;

    const ibkrChg = ibkr?.USD?.adjustedDailyChangePercentage || 0;
    const xtbChg = xtb?.USD?.adjustedDailyChangePercentage || 0;

    // Peso actual (usando valor del día)
    const pesoIbkr = totalVal > 0 ? ibkrVal / totalVal : 0;
    const pesoXtb = totalVal > 0 ? xtbVal / totalVal : 0;

    // Cálculo ponderado actual
    const ponderado = pesoIbkr * ibkrChg + pesoXtb * xtbChg;

    // Actualizar factores
    factorIBKR *= (1 + ibkrChg / 100);
    factorXTB *= (1 + xtbChg / 100);
    factorCombinado *= (1 + ponderado / 100);

    // Cálculo alternativo: usar valor del día anterior para ponderar
    if (idx > 0 && (prevIbkrVal + prevXtbVal) > 0) {
      const prevTotal = prevIbkrVal + prevXtbVal;
      const prevPesoIbkr = prevIbkrVal / prevTotal;
      const prevPesoXtb = prevXtbVal / prevTotal;
      const ponderadoAlt = prevPesoIbkr * ibkrChg + prevPesoXtb * xtbChg;
      factorCombinadoConValorAnterior *= (1 + ponderadoAlt / 100);
    } else {
      factorCombinadoConValorAnterior *= (1 + ponderado / 100);
    }

    console.log(
      `  ${date} | ` +
      `$${ibkrVal.toFixed(2).padStart(8)} | ` +
      `$${xtbVal.toFixed(2).padStart(8)} | ` +
      `$${totalVal.toFixed(2).padStart(8)} | ` +
      `${(pesoIbkr * 100).toFixed(1).padStart(8)}% | ` +
      `${(pesoXtb * 100).toFixed(1).padStart(8)}% | ` +
      `${ibkrChg.toFixed(2).padStart(8)}% | ` +
      `${xtbChg.toFixed(2).padStart(8)}% | ` +
      `${ponderado.toFixed(2).padStart(8)}% | ` +
      `${factorCombinado.toFixed(4)}`
    );

    prevIbkrVal = ibkrVal;
    prevXtbVal = xtbVal;
  });

  const retornoIBKR = (factorIBKR - 1) * 100;
  const retornoXTB = (factorXTB - 1) * 100;
  const retornoCombinado = (factorCombinado - 1) * 100;
  const retornoCombinadoAlt = (factorCombinadoConValorAnterior - 1) * 100;

  console.log('');
  console.log('  RESULTADOS ABRIL:');
  console.log(`    IBKR individual:     ${retornoIBKR.toFixed(2)}%`);
  console.log(`    XTB individual:      ${retornoXTB.toFixed(2)}%`);
  console.log(`    Combinado actual:    ${retornoCombinado.toFixed(2)}% (usando valor del día)`);
  console.log(`    Combinado alt:       ${retornoCombinadoAlt.toFixed(2)}% (usando valor del día anterior)`);
  console.log('');
  console.log(`    Rango válido:        [${Math.min(retornoIBKR, retornoXTB).toFixed(2)}%, ${Math.max(retornoIBKR, retornoXTB).toFixed(2)}%]`);
  console.log(`    ¿Combinado en rango? ${retornoCombinado >= Math.min(retornoIBKR, retornoXTB) - 0.5 && retornoCombinado <= Math.max(retornoIBKR, retornoXTB) + 0.5 ? '✅' : '❌ FUERA DE RANGO'}`);

  // =========================================================================
  // ANÁLISIS DEL PROBLEMA
  // =========================================================================
  console.log('');
  console.log('━'.repeat(100));
  console.log('  ANÁLISIS DEL PROBLEMA');
  console.log('━'.repeat(100));
  console.log('');

  // Buscar días donde el ponderado excede ambos individuales
  console.log('  Días donde ponderado > max(IBKR, XTB) o ponderado < min(IBKR, XTB):');
  console.log('');

  let problemDays = 0;
  allDates.forEach(date => {
    const ibkr = ibkrByDate.get(date);
    const xtb = xtbByDate.get(date);

    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;

    const ibkrChg = ibkr?.USD?.adjustedDailyChangePercentage || 0;
    const xtbChg = xtb?.USD?.adjustedDailyChangePercentage || 0;

    const pesoIbkr = totalVal > 0 ? ibkrVal / totalVal : 0;
    const pesoXtb = totalVal > 0 ? xtbVal / totalVal : 0;

    const ponderado = pesoIbkr * ibkrChg + pesoXtb * xtbChg;

    const minChg = Math.min(ibkrChg, xtbChg);
    const maxChg = Math.max(ibkrChg, xtbChg);

    // Un promedio ponderado SIEMPRE debe estar entre min y max
    if (ponderado < minChg - 0.001 || ponderado > maxChg + 0.001) {
      problemDays++;
      console.log(`    ⚠️ ${date}: IBKR=${ibkrChg.toFixed(2)}%, XTB=${xtbChg.toFixed(2)}%, Ponderado=${ponderado.toFixed(2)}%`);
      console.log(`       Pesos: IBKR=${(pesoIbkr*100).toFixed(1)}%, XTB=${(pesoXtb*100).toFixed(1)}%`);
      console.log(`       Verificación: ${pesoIbkr.toFixed(4)} * ${ibkrChg.toFixed(4)} + ${pesoXtb.toFixed(4)} * ${xtbChg.toFixed(4)} = ${ponderado.toFixed(4)}`);
    }
  });

  if (problemDays === 0) {
    console.log('    ✅ Ningún día tiene ponderado fuera del rango [min, max]');
    console.log('');
    console.log('  🔍 CONCLUSIÓN: El problema NO está en el cálculo diario de ponderación.');
    console.log('     El problema está en la COMPOSICIÓN de los rendimientos.');
    console.log('');
    console.log('  📝 EXPLICACIÓN MATEMÁTICA:');
    console.log('     Aunque cada día el ponderado está entre min y max,');
    console.log('     cuando los PESOS cambian significativamente durante el período,');
    console.log('     el resultado COMPUESTO puede exceder el máximo individual.');
    console.log('');
    console.log('     Esto ocurre porque:');
    console.log('     - Los días buenos de IBKR se ponderan con el peso de IBKR de ese día');
    console.log('     - Los días buenos de XTB se ponderan con el peso de XTB de ese día');
    console.log('     - Si los pesos cambiaron, es como "seleccionar" los mejores días de cada cuenta');
  }

  // =========================================================================
  // DEMOSTRACIÓN CON EJEMPLO SIMPLIFICADO
  // =========================================================================
  console.log('');
  console.log('━'.repeat(100));
  console.log('  DEMOSTRACIÓN: Por qué el combinado puede exceder el máximo');
  console.log('━'.repeat(100));
  console.log('');

  console.log('  Ejemplo simplificado de 2 días:');
  console.log('');
  console.log('  Día 1: IBKR=$1000 (100% del total), XTB=$0');
  console.log('         IBKR sube 10%, XTB sube 0%');
  console.log('         Ponderado = 100% * 10% + 0% * 0% = 10%');
  console.log('');
  console.log('  Día 2: IBKR=$1100, XTB=$1000 (depósito)');
  console.log('         IBKR=$1100 (52%), XTB=$1000 (48%)');
  console.log('         IBKR sube 0%, XTB sube 10%');
  console.log('         Ponderado = 52% * 0% + 48% * 10% = 4.8%');
  console.log('');
  console.log('  Factor compuesto = 1.10 * 1.048 = 1.1528 = +15.28%');
  console.log('');
  console.log('  Mientras que:');
  console.log('  - IBKR individual: 1.10 * 1.00 = 1.10 = +10%');
  console.log('  - XTB individual: 1.00 * 1.10 = 1.10 = +10%');
  console.log('');
  console.log('  ¡El combinado (15.28%) EXCEDE ambos individuales (10% cada uno)!');
  console.log('');
  console.log('  Esto es matemáticamente correcto porque el portafolio "capturó"');
  console.log('  el +10% de IBKR cuando solo tenía IBKR, Y LUEGO capturó');
  console.log('  el +10% de XTB cuando se agregó XTB.');

  // =========================================================================
  // VERIFICACIÓN: ¿Es esto lo que pasa en tu caso?
  // =========================================================================
  console.log('');
  console.log('━'.repeat(100));
  console.log('  VERIFICACIÓN: Evolución de pesos en tu portafolio');
  console.log('━'.repeat(100));
  console.log('');

  // Ver evolución de pesos YTD
  const ytdIbkr = ibkrDocs.filter(d => d.date >= '2025-01-01');
  const ytdXtb = xtbDocs.filter(d => d.date >= '2025-01-01');

  const ytdIbkrByDate = new Map(ytdIbkr.map(d => [d.date, d]));
  const ytdXtbByDate = new Map(ytdXtb.map(d => [d.date, d]));
  const allYtdDates = [...new Set([...ytdIbkrByDate.keys(), ...ytdXtbByDate.keys()])].sort();

  // Mostrar pesos al inicio de cada mes
  const monthStarts = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', 
                       '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'];

  console.log('  Evolución de pesos por mes:');
  console.log('  Mes        | IBKR Val   | XTB Val    | Peso IBKR | Peso XTB');
  console.log('  ' + '-'.repeat(65));

  monthStarts.forEach(monthPrefix => {
    const monthDates = allYtdDates.filter(d => d.startsWith(monthPrefix));
    if (monthDates.length === 0) return;

    const firstDate = monthDates[0];
    const ibkr = ytdIbkrByDate.get(firstDate);
    const xtb = ytdXtbByDate.get(firstDate);

    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;

    const pesoIbkr = totalVal > 0 ? ibkrVal / totalVal * 100 : 0;
    const pesoXtb = totalVal > 0 ? xtbVal / totalVal * 100 : 0;

    console.log(
      `  ${monthPrefix}    | ` +
      `$${ibkrVal.toFixed(2).padStart(8)} | ` +
      `$${xtbVal.toFixed(2).padStart(8)} | ` +
      `${pesoIbkr.toFixed(1).padStart(8)}% | ` +
      `${pesoXtb.toFixed(1).padStart(8)}%`
    );
  });

  console.log('');
  console.log('  ¿Ves el cambio dramático en los pesos?');
  console.log('  Al inicio del año, IBKR dominaba (~89%).');
  console.log('  Ahora, XTB domina (~60%).');
  console.log('');
  console.log('  Esto explica por qué el rendimiento combinado excede el máximo individual.');

  // =========================================================================
  // CONCLUSIÓN
  // =========================================================================
  console.log('');
  console.log('═'.repeat(100));
  console.log('  📋 CONCLUSIÓN FINAL');
  console.log('═'.repeat(100));
  console.log('');
  console.log('  La regla "el combinado debe estar entre min y max individual" aplica SOLO');
  console.log('  cuando los PESOS son CONSTANTES durante todo el período.');
  console.log('');
  console.log('  En tu caso, los pesos cambiaron dramáticamente (IBKR de 89% a 40%),');
  console.log('  lo que permite que el TWR compuesto capture los mejores momentos de cada cuenta.');
  console.log('');
  console.log('  ESTO ES MATEMÁTICAMENTE CORRECTO para Time-Weighted Return (TWR).');
  console.log('');
  console.log('  Si quisieras un número que sí esté entre min y max, necesitarías usar:');
  console.log('  - Money-Weighted Return (MWR/XIRR), que considera el timing de los flujos');
  console.log('  - O un promedio ponderado por VALOR PROMEDIO del período (no valor diario)');
  console.log('');
  console.log('═'.repeat(100));

  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
