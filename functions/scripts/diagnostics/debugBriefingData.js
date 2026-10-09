/**
 * Diagnóstico: Verificar datos reales de Firestore vs lo mostrado en el briefing
 * 
 * Compara los datos de portfolioPerformance con los valores del correo evening briefing
 * para el usuario ccaicedousman@gmail.com (DDeR8P5hYgfuN8gcU4RsQfdTJqx2)
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

async function main() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const currency = 'USD';
  
  console.log('================================================================');
  console.log('  DIAGNÓSTICO: Briefing Data vs Firestore Real');
  console.log('  Usuario: ccaicedousman@gmail.com');
  console.log('  Fecha: 2026-03-06');
  console.log('================================================================\n');

  // 1. Buscar los últimos 5 días de portfolioPerformance
  console.log('📊 1. Últimos documentos de portfolioPerformance/{userId}/dates\n');
  
  const datesRef = db.collection(`portfolioPerformance/${userId}/dates`);
  const recentDocs = await datesRef.orderBy('date', 'desc').limit(5).get();
  
  if (recentDocs.empty) {
    console.log('   ❌ No se encontraron documentos de performance');
    process.exit(1);
  }

  const latestDoc = recentDocs.docs[0];
  const latestData = latestDoc.data();
  const latestDate = latestData.date;
  const usdData = latestData[currency] || {};

  console.log(`   Último documento: ${latestDate} (doc ID: ${latestDoc.id})`);
  console.log(`   Fechas disponibles: ${recentDocs.docs.map(d => d.data().date).join(', ')}\n`);

  // 2. Datos USD completos del último documento
  console.log(`📊 2. Datos ${currency} del documento ${latestDate}\n`);
  
  const fieldsToShow = [
    'totalValue', 'totalInvestment', 'totalCashFlow',
    'doneProfitAndLoss', 'unrealizedProfitAndLoss',
    'dailyChangePercentage', 'rawDailyChangePercentage', 
    'adjustedDailyChangePercentage',
    'dailyReturn', 'monthlyReturn', 'annualReturn',
    'totalROI', 'factor'
  ];
  
  for (const field of fieldsToShow) {
    const val = usdData[field];
    if (val !== undefined) {
      console.log(`   ${field}: ${typeof val === 'number' ? val.toFixed(6) : val}`);
    }
  }

  // 3. Día anterior para calcular cambio intraday
  console.log('\n📊 3. Documento del día anterior\n');
  
  let prevDoc = null;
  let prevUsd = {};
  if (recentDocs.docs.length > 1) {
    prevDoc = recentDocs.docs[1];
    const prevData = prevDoc.data();
    prevUsd = prevData[currency] || {};
    console.log(`   Fecha anterior: ${prevData.date}`);
    console.log(`   totalValue anterior: ${prevUsd.totalValue?.toFixed(2)}`);
    console.log(`   totalInvestment anterior: ${prevUsd.totalInvestment?.toFixed(2)}`);
  }

  // 4. Cálculos que debería hacer el briefing
  console.log('\n📊 4. CÁLCULOS CORRECTOS para el briefing\n');

  const totalValue = usdData.totalValue || 0;
  const totalInvestment = usdData.totalInvestment || 0;
  const dailyChangePct = usdData.dailyChangePercentage || 0;
  const prevTotalValue = prevUsd.totalValue || 0;
  
  // Método A: dayReturn desde el porcentaje (como lo calcula el briefing actual)
  let dayReturnFromPct = 0;
  if (dailyChangePct !== 0 && totalValue !== 0) {
    const yesterdayValue = totalValue / (1 + dailyChangePct / 100);
    dayReturnFromPct = totalValue - yesterdayValue;
  }
  
  // Método B: dayReturn desde valores de documentos consecutivos
  const dayReturnFromDocs = totalValue - prevTotalValue;
  
  // Método C: unrealizedPnL frente a totalInvestment
  const overallReturn = totalValue - totalInvestment;
  const overallReturnPct = totalInvestment ? (overallReturn / totalInvestment * 100) : 0;

  console.log('   --- Resultado del Día (dayReturn) ---');
  console.log(`   Método A (desde %): $${dayReturnFromPct.toFixed(2)} (${dailyChangePct.toFixed(2)}%)`);
  console.log(`   Método B (doc-doc): $${dayReturnFromDocs.toFixed(2)} (${prevTotalValue ? ((dayReturnFromDocs / prevTotalValue) * 100).toFixed(2) : 'N/A'}%)`);
  console.log(`   dailyChangePercentage (Firestore): ${dailyChangePct.toFixed(4)}%`);
  console.log(`   adjustedDailyChangePercentage: ${(usdData.adjustedDailyChangePercentage || 0).toFixed(4)}%`);
  console.log('');
  console.log('   --- Valor de Cierre ---');
  console.log(`   totalValue (Firestore): $${totalValue.toFixed(2)}`);
  console.log('');
  console.log('   --- Rendimiento Total ---');
  console.log(`   totalInvestment: $${totalInvestment.toFixed(2)}`);
  console.log(`   overallReturn: $${overallReturn.toFixed(2)} (${overallReturnPct.toFixed(2)}%)`);

  // 5. Comparar con lo que muestra el correo
  console.log('\n📊 5. COMPARACIÓN: Correo vs Valores Reales\n');
  
  const emailDayReturn = 62.88;
  const emailDayPct = 0.78;
  const emailCloseValue = 8127.95;
  
  console.log('   | Campo              | Correo        | Firestore Real | ¿Match? |');
  console.log('   |--------------------|---------------|----------------|---------|');
  console.log(`   | dayReturn (USD)    | $${emailDayReturn}      | $${dayReturnFromPct.toFixed(2)}       | ${Math.abs(emailDayReturn - dayReturnFromPct) < 1 ? '✅' : '❌'}     |`);
  console.log(`   | dayReturn (%)      | ${emailDayPct}%        | ${dailyChangePct.toFixed(2)}%         | ${Math.abs(emailDayPct - dailyChangePct) < 0.01 ? '✅' : '❌'}     |`);
  console.log(`   | closeValue         | $${emailCloseValue}  | $${totalValue.toFixed(2)}       | ${Math.abs(emailCloseValue - totalValue) < 1 ? '✅' : '❌'}     |`);
  console.log(`   | vs_benchmark       | +0.78%        | Revisar abajo  |         |`);

  // 6. Verificar el vs_benchmark
  console.log('\n📊 6. Verificación vs S&P 500 benchmark\n');
  console.log(`   dailyChangePercentage usuario: ${dailyChangePct.toFixed(4)}%`);
  console.log('   NOTA: vs_benchmark = dailyChangePercentage - S&P500_dailyChange');
  console.log('   Si muestra +0.78% vs S&P, significa que S&P cambió 0% ese día');
  console.log('   O que se está comparando dailyChangePct consigo mismo');
  
  // 7. Datos de userData (stocks/holdings)
  console.log('\n📊 7. Datos de userData (holdings)\n');
  const userDoc = await db.collection('userData').doc(userId).get();
  if (userDoc.exists) {
    const userData = userDoc.data();
    const stocks = userData.stocks || [];
    console.log(`   Holdings: ${stocks.length} activos`);
    console.log(`   defaultCurrency: ${userData.defaultCurrency}`);
    for (const s of stocks.slice(0, 10)) {
      console.log(`     - ${s.name}: ${s.units} unidades`);
    }
  }

  // 8. Dashboard values (from screenshot: inversión $6,875.91, valor $7,365.44)
  console.log('\n📊 8. COMPARACIÓN con Dashboard (screenshot)\n');
  console.log('   Dashboard muestra:');
  console.log('     Inversión Total: $6,875.91');
  console.log('     Valor Actual:    $7,365.44');
  console.log('     Valorización:    $489.53');
  console.log('');
  console.log('   Firestore muestra:');
  console.log(`     totalInvestment: $${totalInvestment.toFixed(2)}`);
  console.log(`     totalValue:      $${totalValue.toFixed(2)}`);
  console.log(`     Valorización:    $${overallReturn.toFixed(2)}`);
  console.log('');
  
  if (Math.abs(totalValue - 7365.44) > 100) {
    console.log('   ⚠️  DISCREPANCIA: El totalValue de Firestore NO coincide con el Dashboard');
    console.log('   Posible causa: El doc más reciente puede no reflejar el valor intraday actual');
    console.log('   El Dashboard calcula el valor en tiempo real con precios actuales');
    console.log('   Mientras el doc de Firestore es del cierre del día anterior (calculado por Cloud Function)');
  }

  console.log('\n================================================================');
  console.log('  DIAGNÓSTICO COMPLETADO');
  console.log('================================================================');
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
