/**
 * Diagnóstico: Verificar coherencia de fechas y monedas en briefing
 */
const admin = require('firebase-admin');
const sa = require('../../key.json');
admin.initializeApp({credential: admin.credential.cert(sa)});
const db = admin.firestore();

(async () => {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  
  console.log('================================================================');
  console.log('  DIAGNÓSTICO: Coherencia de fechas y monedas en briefing');
  console.log('================================================================\n');

  // 1. ¿Qué fecha tiene el performance doc usado?
  console.log('📊 1. Fecha del doc de performance del usuario\n');
  const perfDocs = await db.collection(`portfolioPerformance/${userId}/dates`)
    .orderBy('date', 'desc').limit(5).get();
  
  for (const doc of perfDocs.docs) {
    console.log(`   ${doc.data().date}`);
  }
  const perfDate = perfDocs.docs[0].data().date;
  console.log(`\n   ✅ Fecha usada en briefing: ${perfDate}\n`);

  // 2. ¿Qué fecha tiene el S&P 500 devuelto por el fallback?
  console.log('📊 2. Fechas disponibles del S&P 500 (GSPC)\n');
  const gspcDocs = await db.collection('indexHistories/GSPC/dates')
    .orderBy('date', 'desc').limit(5).get();
  
  for (const doc of gspcDocs.docs) {
    const d = doc.data();
    console.log(`   ${d.date}: score=${d.score}, change=${d.change}, pctChange=${d.percentChange}%`);
  }
  const gspcDate = gspcDocs.docs[0].data().date;
  console.log(`\n   ✅ Fecha que usará el fallback: ${gspcDate}`);
  
  if (perfDate !== gspcDate) {
    console.log(`   ❌ DESALINEACIÓN: Portafolio es de ${perfDate} pero S&P 500 es de ${gspcDate}!`);
    console.log(`      El briefing compara rendimientos de DÍAS DIFERENTES`);
  } else {
    console.log(`   ✅ Fechas alineadas`);
  }

  // 3. Contributors: verificar monedas
  console.log('\n📊 3. Verificar contributions y monedas\n');
  const perfData = perfDocs.docs[0].data();
  const usd = perfData.USD || {};
  const ap = usd.assetPerformance || {};
  
  // Los holdings extraídos del assetPerformance
  const holdings = Object.entries(ap).map(([key, data]) => {
    const parts = key.rsplit ? key.rsplit('_', 1) : key.split('_');
    const symbol = parts.slice(0, -1).join('_') || parts[0];
    const type = parts[parts.length - 1];
    return { symbol, type, ...data };
  }).filter(h => h.units > 0);

  // Separar por mercado
  const clStocks = holdings.filter(h => h.symbol.endsWith('.CL'));
  const usStocks = holdings.filter(h => !h.symbol.endsWith('.CL'));
  
  console.log(`   Stocks colombianos (.CL): ${clStocks.length}`);
  for (const s of clStocks) {
    console.log(`     ${s.symbol}: units=${s.units}, totalValue=$${s.totalValue?.toFixed(2)}, totalInvestment=$${s.totalInvestment?.toFixed(2)}`);
    console.log(`       → Estos valores están en USD (convertidos por scheduledPortfolioCalculations)`);
  }
  
  console.log(`\n   Stocks USA/crypto: ${usStocks.length}`);
  for (const s of usStocks.slice(0, 5)) {
    console.log(`     ${s.symbol}: units=${s.units}, totalValue=$${s.totalValue?.toFixed(2)}`);
  }
  console.log(`     ... y ${Math.max(0, usStocks.length - 5)} más\n`);

  // 4. Simular lo que hace _select_contributors
  console.log('📊 4. Simulación: Lo que haría _select_contributors\n');
  console.log('   El briefing hace: contribution = units × change_usd (del quote)');
  console.log('   Para .CL stocks, Yahoo devuelve "change" en PESOS COLOMBIANOS');
  console.log('   Para USA stocks, Yahoo devuelve "change" en USD');
  console.log('');
  console.log('   Ejemplo: PFCIBEST.CL');
  console.log('     - units=2 (acciones compradas)');
  console.log('     - Yahoo change ~= +780 COP (cambio diario del precio en COP)');
  console.log('     - contribution = 2 × 780 = 1,560 → muestra "$1,560.00"');
  console.log('     - PERO realmente es 1,560 COP ≈ $0.36 USD!');
  console.log('');
  console.log('   Ejemplo: BTC-USD');
  console.log('     - units=0.00784');
  console.log('     - Yahoo change ~= -$2,963 USD');
  console.log('     - contribution = 0.00784 × -2963 = -$23.22 → CORRECTO');

  // 5. Verificar la performance del portafolio vs performance del 03-04
  console.log('\n📊 5. ¿Es el dailyChangePercentage del portafolio comparable al S&P?\n');
  const perfMar05 = perfData.USD;
  const perfMar04Doc = await db.doc(`portfolioPerformance/${userId}/dates/2026-03-04`).get();
  const perfMar04 = perfMar04Doc.data()?.USD;
  
  console.log(`   Portfolio 2026-03-05:`);
  console.log(`     dailyChangePercentage: ${perfMar05.dailyChangePercentage?.toFixed(4)}%`);
  console.log(`     adjustedDailyChangePercentage: ${perfMar05.adjustedDailyChangePercentage?.toFixed(4)}%`);
  console.log(`     totalValue: $${perfMar05.totalValue?.toFixed(2)}`);
  console.log(`   Portfolio 2026-03-04:`);
  console.log(`     dailyChangePercentage: ${perfMar04.dailyChangePercentage?.toFixed(4)}%`);
  console.log(`     adjustedDailyChangePercentage: ${perfMar04.adjustedDailyChangePercentage?.toFixed(4)}%`);
  console.log(`     totalValue: $${perfMar04.totalValue?.toFixed(2)}`);
  console.log('');
  console.log(`   Cambio real del portafolio (doc-a-doc): $${(perfMar05.totalValue - perfMar04.totalValue).toFixed(2)}`);
  console.log(`   Porcentaje: ${((perfMar05.totalValue - perfMar04.totalValue) / perfMar04.totalValue * 100).toFixed(4)}%`);
  
  // 6. Resumen de problemas
  console.log('\n================================================================');
  console.log('  RESUMEN DE PROBLEMAS');
  console.log('================================================================');
  console.log(`\n  1. FECHAS DESALINEADAS:`);
  console.log(`     - Portfolio: ${perfDate} (rendimiento del 5 de marzo)`);
  console.log(`     - S&P 500:   ${gspcDate} (rendimiento del ${gspcDate})`);
  if (perfDate !== gspcDate) {
    console.log(`     → Estamos comparando rendimientos de DÍAS DIFERENTES!`);
  }
  console.log(`\n  2. MONEDAS MEZCLADAS en Contributors:`);
  console.log(`     - .CL stocks: units × change_COP → contribución en COP, NO USD`);
  console.log(`     - USA stocks: units × change_USD → contribución correcta`);
  console.log(`     → Las contribuciones de stocks colombianos están infladas ~4,200x`);
  console.log(`\n  3. SIGNIFICADO CONFUSO de dailyChangePercentage:`);
  console.log(`     - Es el cambio % calculado por scheduledPortfolioCalculations`);
  console.log(`     - Incluye efectos de nuevas compras/ventas del día (flujos de capital)`);
  console.log(`     - NO es comparable directamente con el S&P 500 price return`);

  console.log('\n================================================================');
  process.exit(0);
})();
