/**
 * Diagnóstico profundo: indexHistories S&P 500 - problemas de consistencia
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const NYSE_HOLIDAYS_2026 = [
  '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
  '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
  '2026-11-26', '2026-12-25',
];

function isWeekend(dateStr) {
  const d = new Date(dateStr + 'T12:00:00Z');
  return d.getUTCDay() === 0 || d.getUTCDay() === 6;
}

async function main() {
  console.log('='.repeat(80));
  console.log('DIAGNÓSTICO PROFUNDO: indexHistories/GSPC - S&P 500');
  console.log('='.repeat(80));
  
  // 1. Obtener todos los datos YTD
  const datesRef = db.collection('indexHistories').doc('GSPC').collection('dates');
  const ytdSnap = await datesRef
    .where('date', '>=', '2025-12-30')
    .orderBy('date', 'asc')
    .get();
  
  console.log(`\n📊 Total documentos desde 30-dic-2025: ${ytdSnap.size}`);
  
  // 2. Analizar cada documento
  console.log('\n📋 TODOS los datos en detalle:');
  console.log('─'.repeat(120));
  console.log(`${'Fecha'.padEnd(12)} ${'Score'.padEnd(10)} ${'Change'.padEnd(10)} ${'PctChg%'.padEnd(10)} ${'Timestamp'.padEnd(25)} ${'Problemas'}`);
  console.log('─'.repeat(120));
  
  let prevScore = null;
  let prevDate = null;
  const allData = [];
  
  ytdSnap.docs.forEach(doc => {
    const data = doc.data();
    const date = doc.id;
    const issues = [];
    
    // Check si es fin de semana
    if (isWeekend(date)) issues.push('⚠️ FIN_DE_SEMANA');
    // Check si es festivo
    if (NYSE_HOLIDAYS_2026.includes(date)) issues.push('⚠️ FESTIVO_NYSE');
    
    // Check timestamp intraday (NYSE cierra 21:00 UTC, cualquier dato antes de ~20:30 UTC es intraday)
    const ts = new Date(data.timestamp);
    const hourUTC = ts.getUTCHours();
    const isIntraday = hourUTC < 20 && hourUTC >= 13; // Entre 13:00-20:00 UTC = durante horario NYSE
    if (isIntraday) issues.push(`🔴 INTRADAY (${hourUTC}:${ts.getUTCMinutes().toString().padStart(2,'0')} UTC)`);
    
    // Check pctChange=0 cuando debería tener valor
    if (data.percentChange === 0 && data.change !== 0) issues.push(`🟡 PCT=0 pero change=${data.change}`);
    
    // Check si el cambio calculado coincide
    if (prevScore !== null) {
      const expectedChange = data.score - prevScore;
      const expectedPct = (expectedChange / prevScore) * 100;
      const actualPct = data.percentChange || 0;
      
      if (Math.abs(expectedPct - actualPct) > 0.1) {
        issues.push(`🟠 PctCalc=${expectedPct.toFixed(2)}% vs stored=${actualPct}%`);
      }
      
      if (Math.abs(expectedChange - (data.change || 0)) > 1) {
        issues.push(`🟠 ChgCalc=${expectedChange.toFixed(2)} vs stored=${data.change}`);
      }
    }
    
    const tsStr = ts.toISOString().replace('T', ' ').substr(0, 19);
    const pctStr = data.percentChange !== undefined ? `${data.percentChange}%` : 'N/A';
    
    console.log(`${date.padEnd(12)} ${String(data.score).padEnd(10)} ${String(data.change).padEnd(10)} ${pctStr.padEnd(10)} ${tsStr.padEnd(25)} ${issues.join(' | ') || '✅'}`);
    
    allData.push({ date, ...data, issues });
    prevScore = data.score;
    prevDate = date;
  });
  
  console.log('─'.repeat(120));
  
  // 3. Resumen de problemas
  console.log('\n📊 RESUMEN DE PROBLEMAS:');
  const intradayDays = allData.filter(d => d.issues.some(i => i.includes('INTRADAY')));
  const holidayDays = allData.filter(d => d.issues.some(i => i.includes('FESTIVO')));
  const pctZeroDays = allData.filter(d => d.issues.some(i => i.includes('PCT=0')));
  const calcMismatch = allData.filter(d => d.issues.some(i => i.includes('PctCalc') || i.includes('ChgCalc')));
  
  console.log(`  🔴 Datos INTRADAY (no cierre): ${intradayDays.length}`);
  intradayDays.forEach(d => console.log(`     ${d.date}: score=${d.score}`));
  
  console.log(`  ⚠️ Datos en FESTIVOS NYSE: ${holidayDays.length}`);
  holidayDays.forEach(d => console.log(`     ${d.date}: score=${d.score}`));
  
  console.log(`  🟡 PctChange=0 con cambio real: ${pctZeroDays.length}`);
  pctZeroDays.forEach(d => console.log(`     ${d.date}: change=${d.change}, pct=${d.percentChange}`));
  
  console.log(`  🟠 Cálculo pct/change no coincide: ${calcMismatch.length}`);
  calcMismatch.forEach(d => {
    const issue = d.issues.find(i => i.includes('PctCalc') || i.includes('ChgCalc'));
    console.log(`     ${d.date}: ${issue}`);
  });
  
  // 4. Cálculo YTD correcto vs almacenado
  // Buscar el cierre del 31 dic 2025 o el primer dato de 2025
  console.log('\n📊 CÁLCULO YTD:');
  const dec31Snap = await datesRef.where('date', '==', '2025-12-31').get();
  const jan2Data = allData.find(d => d.date === '2026-01-02');
  const latestData = allData[allData.length - 1];
  
  if (!dec31Snap.empty) {
    const dec31Score = dec31Snap.docs[0].data().score;
    console.log(`  Cierre 31-dic-2025: ${dec31Score}`);
    console.log(`  Último dato (${latestData.date}): ${latestData.score}`);
    const ytdPct = ((latestData.score - dec31Score) / dec31Score) * 100;
    console.log(`  YTD calculado: ${ytdPct.toFixed(2)}%`);
  } else {
    console.log('  ❌ No hay datos para 2025-12-31');
    if (jan2Data) {
      console.log(`  Primer dato 2026 (${jan2Data.date}): ${jan2Data.score}`);
      console.log(`  Último dato (${latestData.date}): ${latestData.score}`);
      const ytdPct = ((latestData.score - jan2Data.score) / jan2Data.score) * 100;
      console.log(`  YTD calculado (desde primer día): ${ytdPct.toFixed(2)}%`);
    }
  }
  
  // Verificar datos de diciembre 2025 
  console.log('\n📋 Datos de cierre de 2025 (últimos días):');
  const dec2025Snap = await datesRef
    .where('date', '>=', '2025-12-26')
    .where('date', '<=', '2025-12-31')
    .orderBy('date', 'asc')
    .get();
  
  dec2025Snap.docs.forEach(doc => {
    const d = doc.data();
    const ts = new Date(d.timestamp);
    console.log(`  ${doc.id}: score=${d.score}, change=${d.change}, pct=${d.percentChange}%, ts=${ts.toISOString()}`);
  });
  
  // 5. Comparar con Google Finance
  console.log('\n📊 COMPARATIVA con Google Finance (4 mar intraday):');
  console.log(`  Google Finance: 6,874.68 (+0.24% YTD)`);
  console.log(`  Nuestro último dato (${latestData.date}): ${latestData.score}`);
  if (jan2Data) {
    const ourYtd = ((latestData.score - jan2Data.score) / jan2Data.score) * 100;
    console.log(`  Nuestro YTD: ${ourYtd.toFixed(2)}%`);
  }
  
  // 6. Verificar cuentas del usuario desde portfolioPerformance directamente
  console.log('\n' + '='.repeat(80));
  console.log('DIAGNÓSTICO portfolioPerformance - cuentas');
  console.log('='.repeat(80));
  
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  
  // Intentar obtener cuentas desde la subcolección accounts de portfolioPerformance
  const accountsRef = db.collection(`portfolioPerformance/${userId}/accounts`);
  const accountsSnap = await accountsRef.get();
  console.log(`\nCuentas en portfolioPerformance: ${accountsSnap.size}`);
  
  for (const accDoc of accountsSnap.docs) {
    console.log(`\n── Cuenta: ${accDoc.id} ──`);
    const accDatesSnap = await db.collection(`portfolioPerformance/${userId}/accounts/${accDoc.id}/dates`)
      .where('date', '>=', '2026-01-01')
      .orderBy('date', 'asc')
      .get();
    
    console.log(`  Documentos YTD: ${accDatesSnap.size}`);
    
    // Primero y último 
    if (accDatesSnap.size > 0) {
      const first = accDatesSnap.docs[0];
      const last = accDatesSnap.docs[accDatesSnap.size - 1];
      const firstUsd = first.data().USD || {};
      const lastUsd = last.data().USD || {};
      console.log(`  Primer: ${first.id} - totalValue=${firstUsd.totalValue?.toFixed(2)}, totalInvestment=${firstUsd.totalInvestment?.toFixed(2)}`);
      console.log(`  Último: ${last.id} - totalValue=${lastUsd.totalValue?.toFixed(2)}, totalInvestment=${lastUsd.totalInvestment?.toFixed(2)}`);
      
      // Verificar gaps
      const accDates = accDatesSnap.docs.map(d => d.id);
      const expectedDays = [];
      let cur = new Date('2026-01-02T12:00:00Z');
      const endD = new Date('2026-03-03T12:00:00Z');
      while (cur <= endD) {
        const dow = cur.getUTCDay();
        const ds = cur.toISOString().split('T')[0];
        if (dow !== 0 && dow !== 6 && !NYSE_HOLIDAYS_2026.includes(ds)) {
          expectedDays.push(ds);
        }
        cur.setUTCDate(cur.getUTCDate() + 1);
      }
      const missing = expectedDays.filter(d => !accDates.includes(d));
      console.log(`  Días faltantes: ${missing.length}`);
      if (missing.length > 0) console.log(`    ${missing.join(', ')}`);
    }
  }
  
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
