/**
 * Diagnóstico de assets y transacciones de cuenta Trii (COP)
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();
const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const triiAccountId = 'ggM52GimbLL7jwvegc9o';

async function main() {
  console.log('═'.repeat(80));
  console.log('DIAGNÓSTICO CUENTA TRII (COP)');
  console.log('═'.repeat(80));
  
  // 1. Obtener todos los assets de Trii (activos e inactivos)
  console.log('\n📋 ASSETS EN TRII:');
  const assetsSnap = await db.collection('assets')
    .where('userId', '==', userId)
    .where('portfolioAccountId', '==', triiAccountId)
    .get();
  
  console.log(`   Total: ${assetsSnap.size}`);
  assetsSnap.docs.forEach(doc => {
    const a = doc.data();
    console.log(`   - ${a.name}: ${a.units} @ ${a.unitValue} ${a.currency}, acquisitionDate=${a.acquisitionDate}, isActive=${a.isActive}`);
    console.log(`     averagePurchasePrice=${a.averagePurchasePrice}, acquisitionDollarValue=${a.acquisitionDollarValue}`);
  });

  // 2. Obtener transacciones de Trii del 4 de marzo
  console.log('\n📋 TRANSACCIONES DEL 4 DE MARZO:');
  const txSnap = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('portfolioAccountId', '==', triiAccountId)
    .where('date', '>=', '2026-03-04')
    .where('date', '<', '2026-03-05')
    .get();
  
  console.log(`   Total: ${txSnap.size}`);
  txSnap.docs.forEach(doc => {
    const t = doc.data();
    console.log(`   - ${t.type}: ${t.name || t.ticker}, amount=${t.amount}, price=${t.price}, currency=${t.currency}`);
    console.log(`     assetId=${t.assetId}, dollarPriceToDate=${t.dollarPriceToDate}`);
  });

  // 3. Calcular cashFlows esperados
  console.log('\n📊 ANÁLISIS DE CASHFLOWS:');
  
  let expectedCashFlowCOP = 0;
  let expectedCashFlowUSD = 0;
  
  txSnap.docs.forEach(doc => {
    const t = doc.data();
    if (t.type === 'buy') {
      // Compras = cashFlow negativo
      const cf = -(t.amount || 0) * (t.price || 0);
      expectedCashFlowCOP += cf;
      
      // Convertir a USD usando dollarPriceToDate
      if (t.dollarPriceToDate && t.currency === 'COP') {
        expectedCashFlowUSD += cf / parseFloat(t.dollarPriceToDate);
      }
      
      console.log(`   ${t.name || t.ticker}: ${t.amount}x @ ${t.price} ${t.currency} = ${cf} COP (${t.dollarPriceToDate ? (cf / parseFloat(t.dollarPriceToDate)).toFixed(2) : '?'} USD)`);
    }
  });
  
  console.log(`\n   CashFlow esperado (solo compras):`);
  console.log(`     COP: ${expectedCashFlowCOP.toFixed(0)}`);
  console.log(`     USD: ${expectedCashFlowUSD.toFixed(2)}`);

  // 4. Obtener datos del día 03-03 para Trii
  console.log('\n📋 PORTFOLIOPERFORMANCE TRII 03-03:');
  const perf03 = await db.doc(`portfolioPerformance/${userId}/accounts/${triiAccountId}/dates/2026-03-03`).get();
  if (perf03.exists) {
    const data = perf03.data();
    console.log(`   USD: totalValue=${data.USD?.totalValue}, totalInvestment=${data.USD?.totalInvestment}`);
    console.log(`   COP: totalValue=${data.COP?.totalValue}, totalInvestment=${data.COP?.totalInvestment}`);
    console.log(`   assetPerformance:`, JSON.stringify(data.USD?.assetPerformance || {}, null, 2).substring(0, 500));
  } else {
    console.log('   NO EXISTE');
  }

  // 5. Obtener datos del día 04-03 para Trii
  console.log('\n📋 PORTFOLIOPERFORMANCE TRII 04-03:');
  const perf04 = await db.doc(`portfolioPerformance/${userId}/accounts/${triiAccountId}/dates/2026-03-04`).get();
  if (perf04.exists) {
    const data = perf04.data();
    console.log(`   USD: totalValue=${data.USD?.totalValue}, totalInvestment=${data.USD?.totalInvestment}, totalCashFlow=${data.USD?.totalCashFlow}`);
    console.log(`   COP: totalValue=${data.COP?.totalValue}, totalInvestment=${data.COP?.totalInvestment}, totalCashFlow=${data.COP?.totalCashFlow}`);
    console.log(`   adjustedDailyChangePercentage USD: ${data.USD?.adjustedDailyChangePercentage}%`);
    console.log(`   assetPerformance:`, JSON.stringify(data.USD?.assetPerformance || {}, null, 2).substring(0, 1000));
  } else {
    console.log('   NO EXISTE');
  }

  // 6. Verificar cuántos assets son "nuevos" (units > 0 ayer = 0)
  console.log('\n📊 ANÁLISIS DE "NUEVAS INVERSIONES" (isNewInvestment):');
  
  if (perf03.exists && perf04.exists) {
    const assetPerf03 = perf03.data().USD?.assetPerformance || {};
    const assetPerf04 = perf04.data().USD?.assetPerformance || {};
    
    for (const [key, data04] of Object.entries(assetPerf04)) {
      const data03 = assetPerf03[key] || {};
      const units03 = data03.units || 0;
      const units04 = data04.units || 0;
      
      const isNewInvestment = units04 > 0 && units03 === 0;
      
      console.log(`   ${key}:`);
      console.log(`     units 03-03: ${units03}, units 04-03: ${units04}`);
      console.log(`     isNewInvestment: ${isNewInvestment}`);
      console.log(`     totalInvestment: ${data04.totalInvestment?.toFixed(2)}`);
      console.log(`     totalCashFlow: ${data04.totalCashFlow?.toFixed(2)}`);
      
      if (isNewInvestment) {
        console.log(`     ⚠️ Este asset es NUEVA INVERSIÓN - su totalInvestment (${data04.totalInvestment}) se agrega negativamente al cashFlow`);
      }
    }
  }

  console.log('\n' + '═'.repeat(80));
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
