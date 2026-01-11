/**
 * ANÁLISIS CRÍTICO: Validación Matemática de Rendimientos
 * 
 * Este script verifica si los rendimientos mostrados son matemáticamente correctos
 * comparando los valores RAW de Firestore con los cálculos esperados.
 * 
 * DATOS DEL UI (después de invalidar caché):
 * 
 * IBKR (Imagen 1):
 *   - Inversión: $2,494.92
 *   - Valor Actual: $3,012.58
 *   - Valorización: $517.67
 *   - YTD: 21.48%
 *   - Marzo: -6.75%
 * 
 * XTB (Imagen 2):
 *   - Inversión: $4,259.37
 *   - Valor Actual: $4,527.74
 *   - Valorización: $268.36
 *   - YTD: 28.44%
 *   - Marzo: -5.13%
 * 
 * 2 Cuentas IBKR+XTB (Imagen 3):
 *   - Inversión: $7,278.28
 *   - Valor Actual: $8,049.35
 *   - Valorización: $771.08
 *   - YTD: 36.03%
 *   - Marzo: -4.25%
 * 
 * PREGUNTA CLAVE: ¿Son estos números matemáticamente posibles?
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

// ============================================================================
// ANÁLISIS MATEMÁTICO
// ============================================================================

async function main() {
  console.log('');
  console.log('═'.repeat(90));
  console.log('  ANÁLISIS CRÍTICO: VALIDACIÓN MATEMÁTICA DE RENDIMIENTOS');
  console.log('═'.repeat(90));
  console.log('');

  // Obtener datos
  const [ibkrDocs, xtbDocs] = await Promise.all([
    getAccountDocs(CONFIG.ACCOUNTS.IBKR),
    getAccountDocs(CONFIG.ACCOUNTS.XTB)
  ]);

  // =========================================================================
  // VERIFICACIÓN 1: ROI Simple vs YTD mostrado
  // =========================================================================
  console.log('━'.repeat(90));
  console.log('  1️⃣  VERIFICACIÓN: ROI SIMPLE vs YTD MOSTRADO');
  console.log('━'.repeat(90));
  console.log('');
  
  // IBKR
  const ibkrInversion = 2494.92;
  const ibkrValorActual = 3012.58;
  const ibkrValorizacion = 517.67;
  const ibkrROI = ((ibkrValorActual - ibkrInversion) / ibkrInversion) * 100;
  
  console.log('  IBKR:');
  console.log(`    Inversión:    $${ibkrInversion.toFixed(2)}`);
  console.log(`    Valor Actual: $${ibkrValorActual.toFixed(2)}`);
  console.log(`    Valorización: $${ibkrValorizacion.toFixed(2)}`);
  console.log(`    ROI Simple:   ${ibkrROI.toFixed(2)}% (Valorización / Inversión)`);
  console.log(`    YTD UI:       21.48%`);
  console.log(`    ¿Coherente?   ${Math.abs(ibkrROI - 21.48) < 1 ? '✅ SÍ' : '⚠️ DIFERENTE (TWR ≠ ROI Simple)'}`);
  
  // XTB
  const xtbInversion = 4259.37;
  const xtbValorActual = 4527.74;
  const xtbValorizacion = 268.36;
  const xtbROI = ((xtbValorActual - xtbInversion) / xtbInversion) * 100;
  
  console.log('');
  console.log('  XTB:');
  console.log(`    Inversión:    $${xtbInversion.toFixed(2)}`);
  console.log(`    Valor Actual: $${xtbValorActual.toFixed(2)}`);
  console.log(`    Valorización: $${xtbValorizacion.toFixed(2)}`);
  console.log(`    ROI Simple:   ${xtbROI.toFixed(2)}% (Valorización / Inversión)`);
  console.log(`    YTD UI:       28.44%`);
  console.log(`    ¿Coherente?   ${Math.abs(xtbROI - 28.44) < 1 ? '✅ SÍ' : '⚠️ DIFERENTE (TWR ≠ ROI Simple)'}`);
  
  console.log('');
  console.log('  📝 NOTA: Es NORMAL que TWR ≠ ROI Simple cuando hay depósitos/retiros.');
  console.log('     TWR mide el rendimiento del dinero independiente de flujos de efectivo.');
  console.log('     ROI Simple = (Valor Final - Inversión) / Inversión');

  // =========================================================================
  // VERIFICACIÓN 2: Coherencia de sumas
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  2️⃣  VERIFICACIÓN: COHERENCIA DE SUMAS (IBKR + XTB = Combinado)');
  console.log('━'.repeat(90));
  console.log('');
  
  const combinadoInversion = 7278.28;
  const combinadoValorActual = 8049.35;
  const combinadoValorizacion = 771.08;
  
  const sumaInversion = ibkrInversion + xtbInversion;
  const sumaValorActual = ibkrValorActual + xtbValorActual;
  const sumaValorizacion = ibkrValorizacion + xtbValorizacion;
  
  console.log('  Sumas calculadas:');
  console.log(`    IBKR Inversión + XTB Inversión = $${sumaInversion.toFixed(2)}`);
  console.log(`    IBKR Valor + XTB Valor = $${sumaValorActual.toFixed(2)}`);
  console.log(`    IBKR Valorización + XTB Valorización = $${sumaValorizacion.toFixed(2)}`);
  console.log('');
  console.log('  Valores UI "2 cuentas":');
  console.log(`    Inversión:    $${combinadoInversion.toFixed(2)} (diff: $${(combinadoInversion - sumaInversion).toFixed(2)})`);
  console.log(`    Valor Actual: $${combinadoValorActual.toFixed(2)} (diff: $${(combinadoValorActual - sumaValorActual).toFixed(2)})`);
  console.log(`    Valorización: $${combinadoValorizacion.toFixed(2)} (diff: $${(combinadoValorizacion - sumaValorizacion).toFixed(2)})`);
  
  const inversionOK = Math.abs(combinadoInversion - sumaInversion) < 1;
  const valorOK = Math.abs(combinadoValorActual - sumaValorActual) < 1;
  const valorizacionOK = Math.abs(combinadoValorizacion - sumaValorizacion) < 1;
  
  console.log('');
  console.log(`  ¿Sumas correctas? ${inversionOK && valorOK && valorizacionOK ? '✅ SÍ' : '❌ NO'}`);

  // =========================================================================
  // VERIFICACIÓN 3: YTD combinado vs YTD individuales
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  3️⃣  VERIFICACIÓN: YTD COMBINADO (36.03%) vs INDIVIDUALES (21.48%, 28.44%)');
  console.log('━'.repeat(90));
  console.log('');
  
  console.log('  PREGUNTA: ¿Es posible que IBKR=21% + XTB=28% = Combinado=36%?');
  console.log('');
  console.log('  ❌ NO ES POSIBLE con promedio ponderado simple.');
  console.log('     El promedio ponderado por valor actual sería:');
  
  const pesoIBKR = ibkrValorActual / (ibkrValorActual + xtbValorActual);
  const pesoXTB = xtbValorActual / (ibkrValorActual + xtbValorActual);
  const promedioSimple = pesoIBKR * 21.48 + pesoXTB * 28.44;
  
  console.log(`     (${(pesoIBKR*100).toFixed(1)}% × 21.48%) + (${(pesoXTB*100).toFixed(1)}% × 28.44%) = ${promedioSimple.toFixed(2)}%`);
  console.log('');
  console.log('  ⚠️ PERO: TWR compuesto día a día puede dar resultados diferentes');
  console.log('     porque depende de CUÁNDO cada cuenta tenía más o menos peso.');
  
  // =========================================================================
  // VERIFICACIÓN 4: Calcular TWR real desde Firestore
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  4️⃣  CÁLCULO TWR REAL DESDE FIRESTORE (YTD 2025)');
  console.log('━'.repeat(90));
  console.log('');
  
  // Filtrar documentos YTD 2025
  const ytdStart = '2025-01-01';
  const ytdIBKR = ibkrDocs.filter(d => d.date >= ytdStart);
  const ytdXTB = xtbDocs.filter(d => d.date >= ytdStart);
  
  // TWR IBKR
  let ibkrFactor = 1;
  ytdIBKR.forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    ibkrFactor *= (1 + change / 100);
  });
  const ibkrTWR = (ibkrFactor - 1) * 100;
  
  // TWR XTB
  let xtbFactor = 1;
  ytdXTB.forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    xtbFactor *= (1 + change / 100);
  });
  const xtbTWR = (xtbFactor - 1) * 100;
  
  console.log(`  IBKR YTD TWR calculado: ${ibkrTWR.toFixed(2)}% (UI muestra: 21.48%)`);
  console.log(`  XTB YTD TWR calculado:  ${xtbTWR.toFixed(2)}% (UI muestra: 28.44%)`);
  
  // TWR Combinado ponderado día a día
  const ibkrByDate = new Map(ytdIBKR.map(d => [d.date, d]));
  const xtbByDate = new Map(ytdXTB.map(d => [d.date, d]));
  const allYtdDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();
  
  let combinedFactor = 1;
  allYtdDates.forEach(date => {
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
    
    combinedFactor *= (1 + weightedChange / 100);
  });
  const combinedTWR = (combinedFactor - 1) * 100;
  
  console.log(`  Combinado YTD TWR calculado: ${combinedTWR.toFixed(2)}% (UI muestra: 36.03%)`);
  
  // =========================================================================
  // VERIFICACIÓN 5: Marzo 2025
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  5️⃣  CÁLCULO TWR REAL DESDE FIRESTORE (MARZO 2025)');
  console.log('━'.repeat(90));
  console.log('');
  
  const marchIBKR = ibkrDocs.filter(d => d.date >= '2025-03-01' && d.date <= '2025-03-31');
  const marchXTB = xtbDocs.filter(d => d.date >= '2025-03-01' && d.date <= '2025-03-31');
  
  // TWR IBKR Marzo
  let ibkrMarchFactor = 1;
  marchIBKR.forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    ibkrMarchFactor *= (1 + change / 100);
  });
  const ibkrMarchTWR = (ibkrMarchFactor - 1) * 100;
  
  // TWR XTB Marzo
  let xtbMarchFactor = 1;
  marchXTB.forEach(d => {
    const change = d.USD?.adjustedDailyChangePercentage || 0;
    xtbMarchFactor *= (1 + change / 100);
  });
  const xtbMarchTWR = (xtbMarchFactor - 1) * 100;
  
  // TWR Combinado Marzo
  const marchIbkrByDate = new Map(marchIBKR.map(d => [d.date, d]));
  const marchXtbByDate = new Map(marchXTB.map(d => [d.date, d]));
  const allMarchDates = [...new Set([...marchIbkrByDate.keys(), ...marchXtbByDate.keys()])].sort();
  
  let marchCombinedFactor = 1;
  allMarchDates.forEach(date => {
    const ibkr = marchIbkrByDate.get(date);
    const xtb = marchXtbByDate.get(date);
    
    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;
    
    const ibkrChange = ibkr?.USD?.adjustedDailyChangePercentage || 0;
    const xtbChange = xtb?.USD?.adjustedDailyChangePercentage || 0;
    
    let weightedChange = 0;
    if (totalVal > 0) {
      weightedChange = (ibkrVal * ibkrChange + xtbVal * xtbChange) / totalVal;
    }
    
    marchCombinedFactor *= (1 + weightedChange / 100);
  });
  const marchCombinedTWR = (marchCombinedFactor - 1) * 100;
  
  console.log(`  IBKR Marzo TWR calculado: ${ibkrMarchTWR.toFixed(2)}% (UI muestra: -6.75%)`);
  console.log(`  XTB Marzo TWR calculado:  ${xtbMarchTWR.toFixed(2)}% (UI muestra: -5.13%)`);
  console.log(`  Combinado Marzo TWR calculado: ${marchCombinedTWR.toFixed(2)}% (UI muestra: -4.25%)`);
  
  // Verificar rangos
  const minMarch = Math.min(ibkrMarchTWR, xtbMarchTWR);
  const maxMarch = Math.max(ibkrMarchTWR, xtbMarchTWR);
  
  console.log('');
  console.log(`  Rango esperado para combinado: [${minMarch.toFixed(2)}%, ${maxMarch.toFixed(2)}%]`);
  
  if (marchCombinedTWR >= minMarch && marchCombinedTWR <= maxMarch) {
    console.log(`  ✅ El combinado (${marchCombinedTWR.toFixed(2)}%) está DENTRO del rango esperado`);
  } else {
    console.log(`  ⚠️ El combinado está fuera del rango, pero puede ser correcto si`);
    console.log(`     los pesos relativos de las cuentas variaron durante el mes.`);
  }

  // =========================================================================
  // VERIFICACIÓN 6: Explicación del fenómeno YTD > máximo individual
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  6️⃣  EXPLICACIÓN: ¿Por qué YTD combinado puede ser > máximo individual?');
  console.log('━'.repeat(90));
  console.log('');
  
  // Analizar evolución de pesos durante el año
  const firstDate = allYtdDates[0];
  const lastDate = allYtdDates[allYtdDates.length - 1];
  
  const firstIbkr = ibkrByDate.get(firstDate);
  const firstXtb = xtbByDate.get(firstDate);
  const lastIbkr = ibkrByDate.get(lastDate);
  const lastXtb = xtbByDate.get(lastDate);
  
  const firstIbkrVal = firstIbkr?.USD?.totalValue || 0;
  const firstXtbVal = firstXtb?.USD?.totalValue || 0;
  const firstTotal = firstIbkrVal + firstXtbVal;
  
  const lastIbkrVal = lastIbkr?.USD?.totalValue || 0;
  const lastXtbVal = lastXtb?.USD?.totalValue || 0;
  const lastTotal = lastIbkrVal + lastXtbVal;
  
  console.log(`  Al inicio del año (${firstDate}):`);
  console.log(`    IBKR: $${firstIbkrVal.toFixed(2)} (${(firstIbkrVal/firstTotal*100).toFixed(1)}%)`);
  console.log(`    XTB:  $${firstXtbVal.toFixed(2)} (${(firstXtbVal/firstTotal*100).toFixed(1)}%)`);
  console.log('');
  console.log(`  Actualmente (${lastDate}):`);
  console.log(`    IBKR: $${lastIbkrVal.toFixed(2)} (${(lastIbkrVal/lastTotal*100).toFixed(1)}%)`);
  console.log(`    XTB:  $${lastXtbVal.toFixed(2)} (${(lastXtbVal/lastTotal*100).toFixed(1)}%)`);
  console.log('');
  
  if (firstIbkrVal > 0 && firstXtbVal < firstIbkrVal / 10) {
    console.log('  💡 EXPLICACIÓN:');
    console.log('     XTB empezó el año con muy poco dinero comparado con IBKR.');
    console.log('     Los primeros días/semanas, IBKR dominaba el portafolio.');
    console.log('     XTB creció mucho (por depósitos + rendimiento).');
    console.log('     El TWR compuesto "captura" el rendimiento de IBKR al inicio');
    console.log('     y el rendimiento de XTB después, sumándolos en efecto.');
    console.log('');
    console.log('     Es como si dijeras:');
    console.log('     - Enero-Feb: Casi todo era IBKR, que tuvo buen rendimiento');
    console.log('     - Mar-Dic: XTB creció y también tuvo buen rendimiento');
    console.log('     - El resultado compuesto puede superar ambos individuales');
  }

  // =========================================================================
  // CONCLUSIÓN FINAL
  // =========================================================================
  console.log('');
  console.log('━'.repeat(90));
  console.log('  📋 CONCLUSIÓN FINAL');
  console.log('━'.repeat(90));
  console.log('');
  
  // Comparar valores calculados vs UI
  const ibkrYtdMatch = Math.abs(ibkrTWR - 21.48) < 1;
  const xtbYtdMatch = Math.abs(xtbTWR - 28.44) < 1;
  const combinedYtdMatch = Math.abs(combinedTWR - 36.03) < 2;
  
  const ibkrMarchMatch = Math.abs(ibkrMarchTWR - (-6.75)) < 1;
  const xtbMarchMatch = Math.abs(xtbMarchTWR - (-5.13)) < 1;
  const combinedMarchMatch = Math.abs(marchCombinedTWR - (-4.25)) < 2;
  
  console.log('  RESUMEN DE VALIDACIÓN:');
  console.log('');
  console.log('  | Métrica              | Calculado | UI      | Match |');
  console.log('  |----------------------|-----------|---------|-------|');
  console.log(`  | IBKR YTD             | ${ibkrTWR.toFixed(2).padStart(8)}% | 21.48%  | ${ibkrYtdMatch ? '✅' : '❌'}    |`);
  console.log(`  | XTB YTD              | ${xtbTWR.toFixed(2).padStart(8)}% | 28.44%  | ${xtbYtdMatch ? '✅' : '❌'}    |`);
  console.log(`  | Combinado YTD        | ${combinedTWR.toFixed(2).padStart(8)}% | 36.03%  | ${combinedYtdMatch ? '✅' : '❌'}    |`);
  console.log(`  | IBKR Marzo           | ${ibkrMarchTWR.toFixed(2).padStart(8)}% | -6.75%  | ${ibkrMarchMatch ? '✅' : '❌'}    |`);
  console.log(`  | XTB Marzo            | ${xtbMarchTWR.toFixed(2).padStart(8)}% | -5.13%  | ${xtbMarchMatch ? '✅' : '❌'}    |`);
  console.log(`  | Combinado Marzo      | ${marchCombinedTWR.toFixed(2).padStart(8)}% | -4.25%  | ${combinedMarchMatch ? '✅' : '❌'}    |`);
  
  console.log('');
  
  if (ibkrYtdMatch && xtbYtdMatch && combinedYtdMatch && ibkrMarchMatch && xtbMarchMatch && combinedMarchMatch) {
    console.log('  ✅ TODOS LOS CÁLCULOS SON CORRECTOS');
    console.log('');
    console.log('  Los valores del UI coinciden con los cálculos desde los datos raw.');
    console.log('  La aparente "inconsistencia" (36% > 21% y 28%) es un comportamiento');
    console.log('  matemáticamente correcto del TWR compuesto cuando los pesos');
    console.log('  de las cuentas cambian significativamente durante el período.');
  } else {
    console.log('  ⚠️ HAY DISCREPANCIAS ENTRE UI Y DATOS RAW');
    console.log('');
    console.log('  Revisar los valores que no coinciden (❌) para identificar');
    console.log('  dónde está el problema en el cálculo.');
  }
  
  console.log('');
  console.log('═'.repeat(90));
  console.log('  ✅ ANÁLISIS COMPLETO');
  console.log('═'.repeat(90));
  
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
