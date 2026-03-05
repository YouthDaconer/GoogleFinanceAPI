/**
 * Script de diagnóstico: indexHistories (S&P 500) + portfolioPerformance YTD
 * Verifica la consistencia de datos para el año 2026 (YTD)
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// ============================================================================
// PARTE 1: Diagnóstico de indexHistories para S&P 500 (GSPC)
// ============================================================================
async function diagnoseIndexHistories() {
  console.log('='.repeat(80));
  console.log('PARTE 1: DIAGNÓSTICO indexHistories - S&P 500 (GSPC)');
  console.log('='.repeat(80));
  
  // 1. Obtener datos generales del índice
  const indexDoc = await db.collection('indexHistories').doc('GSPC').get();
  if (!indexDoc.exists) {
    console.log('❌ No existe el documento indexHistories/GSPC');
    return;
  }
  console.log('\n📋 Datos generales del índice:', JSON.stringify(indexDoc.data(), null, 2));
  
  // 2. Obtener TODOS los datos YTD (desde 2026-01-01)
  const datesRef = db.collection('indexHistories').doc('GSPC').collection('dates');
  const ytdSnap = await datesRef
    .where('date', '>=', '2026-01-01')
    .orderBy('date', 'asc')
    .get();
  
  console.log(`\n📊 Documentos YTD encontrados: ${ytdSnap.size}`);
  
  // Generar días hábiles esperados
  const NYSE_HOLIDAYS_2026 = [
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
    '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
    '2026-11-26', '2026-12-25',
  ];
  
  const expectedDays = [];
  let current = new Date('2026-01-02T12:00:00Z');
  const end = new Date('2026-03-03T12:00:00Z'); // hasta ayer
  while (current <= end) {
    const dayOfWeek = current.getUTCDay();
    const dateStr = current.toISOString().split('T')[0];
    if (dayOfWeek !== 0 && dayOfWeek !== 6 && !NYSE_HOLIDAYS_2026.includes(dateStr)) {
      expectedDays.push(dateStr);
    }
    current.setUTCDate(current.getUTCDate() + 1);
  }
  
  console.log(`📅 Días hábiles esperados (2 ene - 3 mar): ${expectedDays.length}`);
  
  // Mapear documentos existentes
  const existingDates = new Map();
  ytdSnap.docs.forEach(doc => {
    const data = doc.data();
    existingDates.set(doc.id, data);
  });
  
  // Encontrar gaps
  const missingDates = expectedDays.filter(d => !existingDates.has(d));
  console.log(`\n❌ Días FALTANTES en indexHistories/GSPC: ${missingDates.length}`);
  if (missingDates.length > 0) {
    console.log('  ', missingDates.join(', '));
  }
  
  // Mostrar últimos 10 documentos con datos clave
  console.log('\n📋 Últimos 15 documentos:');
  const sortedDates = [...existingDates.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  const last15 = sortedDates.slice(-15);
  last15.forEach(([date, data]) => {
    console.log(`  ${date}: score=${data.score}, change=${data.change}, pctChange=${data.percentChange}%, timestamp=${new Date(data.timestamp).toISOString()}`);
  });
  
  // Verificar datos sospechosos (score=0, percentChange extremo, etc.)
  console.log('\n🔍 Datos sospechosos:');
  let suspicious = 0;
  sortedDates.forEach(([date, data]) => {
    const issues = [];
    if (!data.score || data.score === 0) issues.push('score=0');
    if (Math.abs(data.percentChange || 0) > 5) issues.push(`pctChange=${data.percentChange}% (extremo)`);
    if (!data.timestamp) issues.push('sin timestamp');
    if (!data.change && data.change !== 0) issues.push('sin change');
    if (issues.length > 0) {
      console.log(`  ⚠️ ${date}: ${issues.join(', ')}`);
      suspicious++;
    }
  });
  if (suspicious === 0) console.log('  ✅ No se encontraron datos sospechosos');
  
  // Resumen de rango de scores
  const scores = sortedDates.map(([, d]) => d.score).filter(s => s > 0);
  if (scores.length > 0) {
    console.log(`\n📈 Rango de scores: min=${Math.min(...scores).toFixed(2)}, max=${Math.max(...scores).toFixed(2)}`);
    console.log(`   Primer score: ${sortedDates[0]?.[1]?.score}, Último score: ${sortedDates[sortedDates.length-1]?.[1]?.score}`);
  }
  
  return { existingDates: sortedDates, missingDates };
}

// ============================================================================
// PARTE 2: Diagnóstico de portfolioPerformance para el usuario
// ============================================================================
async function diagnosePortfolioPerformance() {
  console.log('\n' + '='.repeat(80));
  console.log('PARTE 2: DIAGNÓSTICO portfolioPerformance - YTD');
  console.log('='.repeat(80));
  
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  
  // 1. Obtener cuentas
  const accountsSnap = await db.collection(`users/${userId}/portfolioAccounts`)
    .where('isActive', '==', true)
    .get();
  
  const accounts = [];
  accountsSnap.docs.forEach(doc => {
    accounts.push({ id: doc.id, name: doc.data().name });
  });
  console.log(`\n📋 Cuentas activas: ${accounts.length}`);
  accounts.forEach(a => console.log(`   - ${a.name} (${a.id})`));
  
  // Días hábiles esperados
  const NYSE_HOLIDAYS_2026 = [
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
    '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
    '2026-11-26', '2026-12-25',
  ];
  const expectedDays = [];
  let current = new Date('2026-01-02T12:00:00Z');
  const end = new Date('2026-03-03T12:00:00Z');
  while (current <= end) {
    const dayOfWeek = current.getUTCDay();
    const dateStr = current.toISOString().split('T')[0];
    if (dayOfWeek !== 0 && dayOfWeek !== 6 && !NYSE_HOLIDAYS_2026.includes(dateStr)) {
      expectedDays.push(dateStr);
    }
    current.setUTCDate(current.getUTCDate() + 1);
  }
  console.log(`\n📅 Días hábiles esperados (2 ene - 3 mar): ${expectedDays.length}`);
  
  // 2. Verificar OVERALL
  console.log('\n── OVERALL ──');
  const overallSnap = await db.collection(`portfolioPerformance/${userId}/dates`)
    .where('date', '>=', '2026-01-01')
    .orderBy('date', 'asc')
    .get();
  
  console.log(`  Documentos encontrados: ${overallSnap.size}`);
  const overallDates = new Map();
  overallSnap.docs.forEach(doc => {
    overallDates.set(doc.id, doc.data());
  });
  
  const overallMissing = expectedDays.filter(d => !overallDates.has(d));
  console.log(`  Días faltantes: ${overallMissing.length}`);
  if (overallMissing.length > 0 && overallMissing.length <= 10) {
    console.log(`    ${overallMissing.join(', ')}`);
  } else if (overallMissing.length > 10) {
    console.log(`    Primeros 10: ${overallMissing.slice(0, 10).join(', ')}`);
  }
  
  // Mostrar últimos 5 del overall con USD
  const overallSorted = [...overallDates.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  console.log(`  Últimos 5 documentos (USD):`);
  overallSorted.slice(-5).forEach(([date, data]) => {
    const usd = data.USD || {};
    console.log(`    ${date}: totalValue=${usd.totalValue?.toFixed(2)}, totalInvestment=${usd.totalInvestment?.toFixed(2)}, adjChange=${usd.adjustedDailyChangePercentage?.toFixed(4)}%`);
  });
  
  // Verificar datos sospechosos en overall
  let overallSuspicious = 0;
  overallSorted.forEach(([date, data]) => {
    const usd = data.USD || {};
    const issues = [];
    if (!usd.totalValue || usd.totalValue === 0) issues.push('totalValue=0');
    if (Math.abs(usd.adjustedDailyChangePercentage || 0) > 10) issues.push(`adjChange=${usd.adjustedDailyChangePercentage}% (extremo)`);
    if (issues.length > 0) {
      console.log(`    ⚠️ ${date}: ${issues.join(', ')}`);
      overallSuspicious++;
    }
  });
  if (overallSuspicious === 0) console.log('  ✅ No hay datos sospechosos en OVERALL');
  
  // 3. Verificar cada cuenta
  for (const account of accounts) {
    console.log(`\n── ${account.name} (${account.id}) ──`);
    const accSnap = await db.collection(`portfolioPerformance/${userId}/accounts/${account.id}/dates`)
      .where('date', '>=', '2026-01-01')
      .orderBy('date', 'asc')
      .get();
    
    console.log(`  Documentos encontrados: ${accSnap.size}`);
    const accDates = new Map();
    accSnap.docs.forEach(doc => {
      accDates.set(doc.id, doc.data());
    });
    
    const accMissing = expectedDays.filter(d => !accDates.has(d));
    console.log(`  Días faltantes: ${accMissing.length}`);
    if (accMissing.length > 0 && accMissing.length <= 10) {
      console.log(`    ${accMissing.join(', ')}`);
    } else if (accMissing.length > 10) {
      console.log(`    Primeros 10: ${accMissing.slice(0, 10).join(', ')}`);
    }
    
    // Últimos 5
    const accSorted = [...accDates.entries()].sort((a, b) => a[0].localeCompare(b[0]));
    console.log(`  Últimos 5 documentos (USD):`);
    accSorted.slice(-5).forEach(([date, data]) => {
      const usd = data.USD || {};
      console.log(`    ${date}: totalValue=${usd.totalValue?.toFixed(2)}, totalInvestment=${usd.totalInvestment?.toFixed(2)}, adjChange=${usd.adjustedDailyChangePercentage?.toFixed(4)}%`);
    });
    
    // Datos sospechosos
    let accSuspicious = 0;
    accSorted.forEach(([date, data]) => {
      const usd = data.USD || {};
      const issues = [];
      if (!usd.totalValue || usd.totalValue === 0) issues.push('totalValue=0');
      if (Math.abs(usd.adjustedDailyChangePercentage || 0) > 10) issues.push(`adjChange=${usd.adjustedDailyChangePercentage}% (extremo)`);
      if (issues.length > 0) {
        console.log(`    ⚠️ ${date}: ${issues.join(', ')}`);
        accSuspicious++;
      }
    });
    if (accSuspicious === 0) console.log(`  ✅ No hay datos sospechosos en ${account.name}`);
  }
}

// ============================================================================
// MAIN
// ============================================================================
async function main() {
  try {
    const indexResult = await diagnoseIndexHistories();
    await diagnosePortfolioPerformance();
    
    console.log('\n' + '='.repeat(80));
    console.log('DIAGNÓSTICO COMPLETO');
    console.log('='.repeat(80));
  } catch (error) {
    console.error('Error:', error);
  } finally {
    process.exit(0);
  }
}

main();
