/**
 * Diagnóstico: ¿Por qué Cierre $8,127.95 vs Dashboard $8,074.13?
 */
const admin = require('firebase-admin');
const sa = require('../../key.json');
admin.initializeApp({credential: admin.credential.cert(sa)});
const db = admin.firestore();

(async () => {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  
  console.log('================================================================');
  console.log('  ¿POR QUÉ CIERRE $8,127.95 vs DASHBOARD $8,074.13?');
  console.log('================================================================\n');

  // 1. El correo usa el doc de portfolioPerformance del 2026-03-05
  const doc05 = await db.doc(`portfolioPerformance/${userId}/dates/2026-03-05`).get();
  const usd05 = doc05.data()?.USD || {};
  console.log('📧 CORREO (portfolioPerformance/2026-03-05/USD):');
  console.log(`   totalValue:      $${usd05.totalValue?.toFixed(2)}`);
  console.log(`   totalInvestment: $${usd05.totalInvestment?.toFixed(2)}`);
  console.log(`   fecha snapshot:  5 de marzo 2026 (cierre de mercado)`);
  console.log('');
  
  // 2. Dashboard en vivo (6 de marzo)
  console.log('📊 DASHBOARD (valores del 6 de marzo, en la captura):');
  console.log('   Inversión Total: $7,811.46');
  console.log('   Valor Actual:    $8,074.13');
  console.log('   Valorización:    $262.67');
  console.log('');
  
  // 3. Diferencias
  const emailValue = 8127.95;
  const emailInv = 7787.67;
  const dashValue = 8074.13;
  const dashInv = 7811.46;
  
  console.log('📐 DIFERENCIAS:');
  console.log(`   totalValue:      $${emailValue} → $${dashValue} = ${(dashValue - emailValue).toFixed(2)} (${((dashValue - emailValue) / emailValue * 100).toFixed(2)}%)`);
  console.log(`   totalInvestment: $${emailInv} → $${dashInv} = +${(dashInv - emailInv).toFixed(2)}`);
  console.log('');
  
  // 4. ¿Hay un doc del 6 de marzo?
  const doc06 = await db.doc(`portfolioPerformance/${userId}/dates/2026-03-06`).get();
  console.log('📅 ¿Existe doc del 2026-03-06?', doc06.exists ? 'SÍ' : 'NO');
  if (doc06.exists) {
    const usd06 = doc06.data()?.USD || {};
    console.log(`   totalValue:      $${usd06.totalValue?.toFixed(2)}`);
    console.log(`   totalInvestment: $${usd06.totalInvestment?.toFixed(2)}`);
  }
  console.log('');
  
  // 5. ¿Hubo transacciones el 6 de marzo? (explicaría cambio en investment)
  console.log('📋 ¿Nuevos activos entre 4-5 y 5-6 de marzo?');
  const doc04 = await db.doc(`portfolioPerformance/${userId}/dates/2026-03-04`).get();
  const usd04 = doc04.data()?.USD || {};
  const ap04 = Object.keys(usd04.assetPerformance || {}).sort();
  const ap05 = Object.keys(usd05.assetPerformance || {}).sort();
  
  // Assets en 05 que no estaban en 04
  const newAssets = ap05.filter(a => !ap04.includes(a));
  const removedAssets = ap04.filter(a => !ap05.includes(a));
  
  console.log(`   Assets en 04: ${ap04.length}`);
  console.log(`   Assets en 05: ${ap05.length}`);
  console.log(`   Nuevos en 05: ${newAssets.length > 0 ? newAssets.join(', ') : 'Ninguno'}`);
  console.log(`   Removidos: ${removedAssets.length > 0 ? removedAssets.join(', ') : 'Ninguno'}`);
  
  // Investment differences per asset
  console.log('\n📋 Cambio en totalInvestment por activo (05 vs 04):');
  let totalInvDiff = 0;
  for (const key of ap05) {
    const inv04 = usd04.assetPerformance?.[key]?.totalInvestment || 0;
    const inv05 = usd05.assetPerformance?.[key]?.totalInvestment || 0;
    const diff = inv05 - inv04;
    if (Math.abs(diff) > 0.01) {
      totalInvDiff += diff;
      console.log(`   ${key}: $${inv04.toFixed(2)} → $${inv05.toFixed(2)} (${diff > 0 ? '+' : ''}${diff.toFixed(2)})`);
    }
  }
  console.log(`   TOTAL cambio en inversión: $${totalInvDiff.toFixed(2)}`);
  
  // 6. Explicación
  console.log('\n================================================================');
  console.log('  EXPLICACIÓN');
  console.log('================================================================');
  console.log('');
  console.log('  El CORREO muestra datos del CIERRE del 5 de marzo:');
  console.log('    → Snapshot calculado por scheduledPortfolioCalculations a medianoche');
  console.log('    → Precios de cierre del 5 de marzo');
  console.log('');
  console.log('  El DASHBOARD muestra datos EN VIVO del 6 de marzo:');
  console.log('    → Precios intraday actuales via WebSocket/API');
  console.log(`    → El mercado BAJÓ hoy: $${emailValue} → $${dashValue} = $${(dashValue - emailValue).toFixed(2)}`);
  console.log('');
  console.log('  La diferencia en inversión ($' + (dashInv - emailInv).toFixed(2) + ') indica:');
  console.log('    → O se hizo una compra hoy, O la conversión COP→USD cambió');
  console.log('    → (Los activos .CL tienen inversión en COP, convertida a USD)');
  console.log('');
  console.log('  ❌ NO es una inconsistencia. Son datos de MOMENTOS DIFERENTES.');
  console.log('  ⚠️  PERO el header del correo dice "Viernes, 6 de Mar de 2026"');
  console.log('    cuando los datos son del 5 de marzo. ESO sí es confuso.');

  // 7. S&P 500 check
  console.log('\n================================================================');
  console.log('  S&P 500 VERIFICACIÓN');
  console.log('================================================================');
  console.log('');
  
  const gspc04 = await db.doc('indexHistories/GSPC/dates/2026-03-04').get();
  const gspc05 = await db.doc('indexHistories/GSPC/dates/2026-03-05').get();
  
  console.log('  GSPC 2026-03-04:', JSON.stringify(gspc04.data() || {}));
  console.log('  GSPC 2026-03-05:', gspc05.exists ? JSON.stringify(gspc05.data()) : 'NO EXISTE');
  console.log('');
  console.log('  El correo muestra "S&P 500 +0.84%%" (con doble %)');
  console.log('  Ese +0.84% es del 4 de MARZO, NO del 5');
  console.log('  → El S&P del 5 de marzo NO existe en Firestore');
  console.log('  → El fallback devolvió el más reciente disponible (4 de marzo)');
  console.log('  → Por eso el vs_benchmark se omitió (fechas diferentes)');
  console.log('  → PERO el índice AÚN SE MUESTRA como referencia → CONFUSO');
  
  console.log('\n================================================================');
  process.exit(0);
})();
