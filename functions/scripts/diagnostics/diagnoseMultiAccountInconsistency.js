/**
 * Script de Diagnóstico: Inconsistencias Multi-Cuenta
 * 
 * Analiza las diferencias entre:
 * - Vista consolidada (2 cuentas IBKR + XTB)
 * - Vistas individuales (IBKR, XTB)
 * 
 * Enfoque específico en:
 * 1. Marzo 2025: -6.75% (IBKR) y -5.13% (XTB) vs -4.25% (consolidado) - IMPOSIBLE
 * 2. YTD 2025: 21.48% (IBKR) y 28.44% (XTB) vs 36.03% (consolidado) - IMPOSIBLE
 * 3. Valores totales: $3,012 + $4,527 = $7,540 vs $8,049 mostrado
 * 
 * @see docs/stories/27.story.md (si existe)
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
    XTB: 'Z3gnboYgRlTvSZNGSu8j'
  },
  CURRENCY: 'USD',
  // Fechas de análisis
  ANALYSIS_MONTHS: ['2025-03'], // Marzo donde detectamos la anomalía
  YEAR: '2025'
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
    'CALC': '🧮',
    'DATA': '📊',
  }[level] || '•';
  
  console.log(`${prefix} ${message}`);
  if (data) {
    if (typeof data === 'object') {
      Object.entries(data).forEach(([key, value]) => {
        if (typeof value === 'number') {
          console.log(`   ${key}: ${value.toFixed(4)}`);
        } else {
          console.log(`   ${key}: ${value}`);
        }
      });
    } else {
      console.log(`   ${data}`);
    }
  }
}

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

/**
 * Obtener todos los documentos de performance para una cuenta en un rango de fechas
 */
async function getAccountPerformance(accountId, startDate, endDate) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`;
  
  const snapshot = await db.collection(path)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs.map(doc => ({
    id: doc.id,
    ...doc.data()
  }));
}

/**
 * Obtener todos los documentos de performance OVERALL (consolidado)
 */
async function getOverallPerformance(startDate, endDate) {
  const path = `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  
  const snapshot = await db.collection(path)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs.map(doc => ({
    id: doc.id,
    ...doc.data()
  }));
}

/**
 * Obtener el último documento de performance disponible
 */
async function getLatestPerformance(accountId = null) {
  const path = accountId 
    ? `portfolioPerformance/${CONFIG.USER_ID}/accounts/${accountId}/dates`
    : `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  
  const snapshot = await db.collection(path)
    .orderBy('date', 'desc')
    .limit(1)
    .get();
  
  if (snapshot.empty) return null;
  return { id: snapshot.docs[0].id, ...snapshot.docs[0].data() };
}

// ============================================================================
// ANÁLISIS
// ============================================================================

/**
 * Calcular el rendimiento mensual compuesto a partir de cambios diarios ajustados
 */
function calculateMonthlyReturn(docs, currency) {
  let compoundFactor = 1;
  let validDays = 0;
  
  docs.forEach(doc => {
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
 * Calcular el rendimiento esperado combinando dos cuentas
 * Usa promedio ponderado por valor de cada día
 */
function calculateExpectedCombinedReturn(ibkrDocs, xtbDocs, currency) {
  // Crear mapas por fecha
  const ibkrByDate = new Map(ibkrDocs.map(d => [d.date, d]));
  const xtbByDate = new Map(xtbDocs.map(d => [d.date, d]));
  
  // Encontrar todas las fechas únicas
  const allDates = [...new Set([...ibkrByDate.keys(), ...xtbByDate.keys()])].sort();
  
  let compoundFactor = 1;
  let validDays = 0;
  const dailyDetails = [];
  
  allDates.forEach(date => {
    const ibkr = ibkrByDate.get(date);
    const xtb = xtbByDate.get(date);
    
    const ibkrData = ibkr?.[currency];
    const xtbData = xtb?.[currency];
    
    const ibkrValue = ibkrData?.totalValue || 0;
    const xtbValue = xtbData?.totalValue || 0;
    const totalValue = ibkrValue + xtbValue;
    
    if (totalValue === 0) return;
    
    const ibkrChange = ibkrData?.adjustedDailyChangePercentage || 0;
    const xtbChange = xtbData?.adjustedDailyChangePercentage || 0;
    
    // Promedio ponderado por valor
    const weightedChange = (ibkrValue * ibkrChange + xtbValue * xtbChange) / totalValue;
    
    compoundFactor *= (1 + weightedChange / 100);
    validDays++;
    
    dailyDetails.push({
      date,
      ibkrValue: ibkrValue.toFixed(2),
      xtbValue: xtbValue.toFixed(2),
      ibkrChange: ibkrChange.toFixed(4),
      xtbChange: xtbChange.toFixed(4),
      weightedChange: weightedChange.toFixed(4),
      compoundFactor: compoundFactor.toFixed(6)
    });
  });
  
  return {
    return: (compoundFactor - 1) * 100,
    validDays,
    compoundFactor,
    dailyDetails
  };
}

// ============================================================================
// DIAGNÓSTICO PRINCIPAL
// ============================================================================

async function main() {
  console.log('');
  console.log('═'.repeat(80));
  console.log('  DIAGNÓSTICO DE INCONSISTENCIAS MULTI-CUENTA');
  console.log('  Usuario:', CONFIG.USER_ID);
  console.log('  Cuentas: IBKR + XTB');
  console.log('═'.repeat(80));
  console.log('');

  // =========================================================================
  // 1. ANÁLISIS DE VALORES ACTUALES (La diferencia de $509)
  // =========================================================================
  console.log('');
  console.log('━'.repeat(80));
  console.log('  1️⃣  ANÁLISIS DE VALORES ACTUALES');
  console.log('━'.repeat(80));
  
  const latestOverall = await getLatestPerformance(null);
  const latestIBKR = await getLatestPerformance(CONFIG.ACCOUNTS.IBKR);
  const latestXTB = await getLatestPerformance(CONFIG.ACCOUNTS.XTB);
  
  log('DATA', 'Último documento OVERALL:', {
    fecha: latestOverall?.date,
    totalValue: latestOverall?.USD?.totalValue,
    totalInvestment: latestOverall?.USD?.totalInvestment
  });
  
  log('DATA', 'Último documento IBKR:', {
    fecha: latestIBKR?.date,
    totalValue: latestIBKR?.USD?.totalValue,
    totalInvestment: latestIBKR?.USD?.totalInvestment
  });
  
  log('DATA', 'Último documento XTB:', {
    fecha: latestXTB?.date,
    totalValue: latestXTB?.USD?.totalValue,
    totalInvestment: latestXTB?.USD?.totalInvestment
  });
  
  const sumValues = (latestIBKR?.USD?.totalValue || 0) + (latestXTB?.USD?.totalValue || 0);
  const overallValue = latestOverall?.USD?.totalValue || 0;
  const difference = overallValue - sumValues;
  
  console.log('');
  log('CALC', 'COMPARACIÓN DE VALORES:', {
    'IBKR + XTB': sumValues,
    'OVERALL': overallValue,
    'DIFERENCIA': difference
  });
  
  if (Math.abs(difference) > 1) {
    log('WARNING', `¡HAY UNA DIFERENCIA DE $${difference.toFixed(2)}!`);
    log('INFO', 'Esto sugiere que OVERALL incluye datos de otras cuentas o hay inconsistencia');
  }

  // =========================================================================
  // 2. ANÁLISIS DE MARZO 2025 (El mes imposible)
  // =========================================================================
  console.log('');
  console.log('━'.repeat(80));
  console.log('  2️⃣  ANÁLISIS DE MARZO 2025 (Anomalía detectada)');
  console.log('━'.repeat(80));
  
  const marchStart = '2025-03-01';
  const marchEnd = '2025-03-31';
  
  const marchIBKR = await getAccountPerformance(CONFIG.ACCOUNTS.IBKR, marchStart, marchEnd);
  const marchXTB = await getAccountPerformance(CONFIG.ACCOUNTS.XTB, marchStart, marchEnd);
  const marchOverall = await getOverallPerformance(marchStart, marchEnd);
  
  log('DATA', `Documentos en Marzo - IBKR: ${marchIBKR.length}, XTB: ${marchXTB.length}, OVERALL: ${marchOverall.length}`);
  
  // Calcular rendimientos mensuales
  const ibkrMarchReturn = calculateMonthlyReturn(marchIBKR, CONFIG.CURRENCY);
  const xtbMarchReturn = calculateMonthlyReturn(marchXTB, CONFIG.CURRENCY);
  const overallMarchReturn = calculateMonthlyReturn(marchOverall, CONFIG.CURRENCY);
  
  console.log('');
  log('CALC', 'RENDIMIENTOS DE MARZO 2025:');
  console.log(`   IBKR:    ${ibkrMarchReturn.return.toFixed(2)}% (${ibkrMarchReturn.validDays} días)`);
  console.log(`   XTB:     ${xtbMarchReturn.return.toFixed(2)}% (${xtbMarchReturn.validDays} días)`);
  console.log(`   OVERALL: ${overallMarchReturn.return.toFixed(2)}% (${overallMarchReturn.validDays} días)`);
  
  // Calcular el rendimiento esperado combinando ambas cuentas
  const expectedCombined = calculateExpectedCombinedReturn(marchIBKR, marchXTB, CONFIG.CURRENCY);
  console.log(`   ESPERADO (ponderado): ${expectedCombined.return.toFixed(2)}%`);
  
  // Verificar si es matemáticamente posible
  const minReturn = Math.min(ibkrMarchReturn.return, xtbMarchReturn.return);
  const maxReturn = Math.max(ibkrMarchReturn.return, xtbMarchReturn.return);
  
  console.log('');
  if (overallMarchReturn.return < minReturn || overallMarchReturn.return > maxReturn) {
    log('ERROR', '¡IMPOSIBLE MATEMÁTICAMENTE!');
    log('ERROR', `El OVERALL (${overallMarchReturn.return.toFixed(2)}%) está FUERA del rango [${minReturn.toFixed(2)}%, ${maxReturn.toFixed(2)}%]`);
    log('INFO', 'Esto indica que OVERALL incluye datos de otras fuentes o el cálculo está mal');
  } else {
    log('SUCCESS', 'El OVERALL está dentro del rango esperado');
  }

  // =========================================================================
  // 3. ANÁLISIS DÍA A DÍA DE MARZO
  // =========================================================================
  console.log('');
  console.log('━'.repeat(80));
  console.log('  3️⃣  ANÁLISIS DÍA A DÍA DE MARZO 2025');
  console.log('━'.repeat(80));
  
  // Crear mapas por fecha
  const ibkrByDate = new Map(marchIBKR.map(d => [d.date, d]));
  const xtbByDate = new Map(marchXTB.map(d => [d.date, d]));
  const overallByDate = new Map(marchOverall.map(d => [d.date, d]));
  
  const allMarchDates = [...new Set([
    ...ibkrByDate.keys(), 
    ...xtbByDate.keys(), 
    ...overallByDate.keys()
  ])].sort();
  
  console.log('');
  console.log('Fecha       | IBKR Value | XTB Value  | Overall Value | Sum Values | Diff');
  console.log('-'.repeat(85));
  
  let anomaliesFound = 0;
  
  allMarchDates.forEach(date => {
    const ibkr = ibkrByDate.get(date);
    const xtb = xtbByDate.get(date);
    const overall = overallByDate.get(date);
    
    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const overallVal = overall?.USD?.totalValue || 0;
    const sumVal = ibkrVal + xtbVal;
    const diff = overallVal - sumVal;
    
    const diffFlag = Math.abs(diff) > 10 ? ' ⚠️' : '';
    if (Math.abs(diff) > 10) anomaliesFound++;
    
    console.log(
      `${date} | ` +
      `$${ibkrVal.toFixed(2).padStart(9)} | ` +
      `$${xtbVal.toFixed(2).padStart(9)} | ` +
      `$${overallVal.toFixed(2).padStart(12)} | ` +
      `$${sumVal.toFixed(2).padStart(9)} | ` +
      `$${diff.toFixed(2).padStart(7)}${diffFlag}`
    );
  });
  
  console.log('');
  if (anomaliesFound > 0) {
    log('WARNING', `Se encontraron ${anomaliesFound} días con diferencias > $10`);
  }

  // =========================================================================
  // 4. ANÁLISIS DE adjustedDailyChangePercentage
  // =========================================================================
  console.log('');
  console.log('━'.repeat(80));
  console.log('  4️⃣  ANÁLISIS DE adjustedDailyChangePercentage (TWR)');
  console.log('━'.repeat(80));
  
  console.log('');
  console.log('Fecha       | IBKR %     | XTB %      | Overall %  | Expected % | Diff');
  console.log('-'.repeat(85));
  
  let twrAnomalies = 0;
  
  allMarchDates.forEach(date => {
    const ibkr = ibkrByDate.get(date);
    const xtb = xtbByDate.get(date);
    const overall = overallByDate.get(date);
    
    const ibkrChange = ibkr?.USD?.adjustedDailyChangePercentage || 0;
    const xtbChange = xtb?.USD?.adjustedDailyChangePercentage || 0;
    const overallChange = overall?.USD?.adjustedDailyChangePercentage || 0;
    
    // Calcular cambio esperado ponderado por valor
    const ibkrVal = ibkr?.USD?.totalValue || 0;
    const xtbVal = xtb?.USD?.totalValue || 0;
    const totalVal = ibkrVal + xtbVal;
    
    let expectedChange = 0;
    if (totalVal > 0) {
      expectedChange = (ibkrVal * ibkrChange + xtbVal * xtbChange) / totalVal;
    }
    
    const diff = overallChange - expectedChange;
    const diffFlag = Math.abs(diff) > 0.5 ? ' ⚠️' : '';
    if (Math.abs(diff) > 0.5) twrAnomalies++;
    
    console.log(
      `${date} | ` +
      `${ibkrChange.toFixed(4).padStart(9)}% | ` +
      `${xtbChange.toFixed(4).padStart(9)}% | ` +
      `${overallChange.toFixed(4).padStart(9)}% | ` +
      `${expectedChange.toFixed(4).padStart(9)}% | ` +
      `${diff.toFixed(4).padStart(7)}%${diffFlag}`
    );
  });
  
  console.log('');
  if (twrAnomalies > 0) {
    log('WARNING', `Se encontraron ${twrAnomalies} días con diferencias TWR > 0.5%`);
  }

  // =========================================================================
  // 5. ANÁLISIS YTD 2025
  // =========================================================================
  console.log('');
  console.log('━'.repeat(80));
  console.log('  5️⃣  ANÁLISIS YTD 2025');
  console.log('━'.repeat(80));
  
  const ytdStart = '2025-01-01';
  const ytdEnd = '2025-12-31';
  
  const ytdIBKR = await getAccountPerformance(CONFIG.ACCOUNTS.IBKR, ytdStart, ytdEnd);
  const ytdXTB = await getAccountPerformance(CONFIG.ACCOUNTS.XTB, ytdStart, ytdEnd);
  const ytdOverall = await getOverallPerformance(ytdStart, ytdEnd);
  
  log('DATA', `Documentos YTD - IBKR: ${ytdIBKR.length}, XTB: ${ytdXTB.length}, OVERALL: ${ytdOverall.length}`);
  
  const ibkrYTD = calculateMonthlyReturn(ytdIBKR, CONFIG.CURRENCY);
  const xtbYTD = calculateMonthlyReturn(ytdXTB, CONFIG.CURRENCY);
  const overallYTD = calculateMonthlyReturn(ytdOverall, CONFIG.CURRENCY);
  const expectedYTD = calculateExpectedCombinedReturn(ytdIBKR, ytdXTB, CONFIG.CURRENCY);
  
  console.log('');
  log('CALC', 'RENDIMIENTOS YTD 2025:');
  console.log(`   IBKR:    ${ibkrYTD.return.toFixed(2)}% (${ibkrYTD.validDays} días)`);
  console.log(`   XTB:     ${xtbYTD.return.toFixed(2)}% (${xtbYTD.validDays} días)`);
  console.log(`   OVERALL: ${overallYTD.return.toFixed(2)}% (${overallYTD.validDays} días)`);
  console.log(`   ESPERADO (ponderado): ${expectedYTD.return.toFixed(2)}%`);
  
  const ytdMinReturn = Math.min(ibkrYTD.return, xtbYTD.return);
  const ytdMaxReturn = Math.max(ibkrYTD.return, xtbYTD.return);
  
  console.log('');
  if (overallYTD.return < ytdMinReturn || overallYTD.return > ytdMaxReturn) {
    log('ERROR', '¡YTD IMPOSIBLE MATEMÁTICAMENTE!');
    log('ERROR', `El OVERALL YTD (${overallYTD.return.toFixed(2)}%) está FUERA del rango [${ytdMinReturn.toFixed(2)}%, ${ytdMaxReturn.toFixed(2)}%]`);
  } else {
    log('SUCCESS', 'El YTD OVERALL está dentro del rango esperado');
  }

  // =========================================================================
  // 6. VERIFICAR SI OVERALL INCLUYE OTRAS CUENTAS
  // =========================================================================
  console.log('');
  console.log('━'.repeat(80));
  console.log('  6️⃣  VERIFICAR CONTENIDO DE OVERALL');
  console.log('━'.repeat(80));
  
  // Obtener todas las subcolecciones de accounts
  const accountsRef = db.collection(`portfolioPerformance/${CONFIG.USER_ID}/accounts`);
  const accountsSnapshot = await accountsRef.get();
  
  console.log('');
  log('DATA', `Cuentas encontradas en portfolioPerformance/accounts: ${accountsSnapshot.docs.length}`);
  
  for (const accDoc of accountsSnapshot.docs) {
    const accId = accDoc.id;
    const latestDoc = await getLatestPerformance(accId);
    
    // Obtener nombre de la cuenta
    const accountInfo = await db.collection('portfolioAccounts').doc(accId).get();
    const accountName = accountInfo.exists ? accountInfo.data().name : 'Desconocida';
    
    console.log(`   - ${accountName} (${accId})`);
    console.log(`     Última fecha: ${latestDoc?.date || 'N/A'}`);
    console.log(`     Valor USD: $${latestDoc?.USD?.totalValue?.toFixed(2) || '0.00'}`);
  }

  // =========================================================================
  // 7. RESUMEN Y CONCLUSIONES
  // =========================================================================
  console.log('');
  console.log('━'.repeat(80));
  console.log('  📋 RESUMEN Y CONCLUSIONES');
  console.log('━'.repeat(80));
  
  console.log('');
  console.log('  HALLAZGOS:');
  console.log('  ──────────');
  
  if (Math.abs(difference) > 1) {
    console.log(`  ❌ Diferencia de valores: OVERALL tiene $${difference.toFixed(2)} más que IBKR+XTB`);
  }
  
  if (overallMarchReturn.return < minReturn || overallMarchReturn.return > maxReturn) {
    console.log(`  ❌ Marzo 2025: OVERALL (${overallMarchReturn.return.toFixed(2)}%) fuera de rango [${minReturn.toFixed(2)}%, ${maxReturn.toFixed(2)}%]`);
  }
  
  if (overallYTD.return < ytdMinReturn || overallYTD.return > ytdMaxReturn) {
    console.log(`  ❌ YTD 2025: OVERALL (${overallYTD.return.toFixed(2)}%) fuera de rango [${ytdMinReturn.toFixed(2)}%, ${ytdMaxReturn.toFixed(2)}%]`);
  }
  
  console.log('');
  console.log('  POSIBLES CAUSAS:');
  console.log('  ────────────────');
  console.log('  1. OVERALL incluye datos de cuentas adicionales (ej: Binance)');
  console.log('  2. El backfill calculó OVERALL de forma independiente, no como agregación');
  console.log('  3. Hay documentos de performance de una cuenta inactiva/eliminada');
  console.log('  4. El scheduler calcula OVERALL diferente al multi-account del frontend');
  
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
