/**
 * Diagnóstico: Verificar benchmark S&P 500 y discrepancias Dashboard vs Firestore
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

async function main() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  
  console.log('================================================================');
  console.log('  DIAGNÓSTICO PROFUNDO: Benchmark + Dashboard Comparison');
  console.log('================================================================\n');

  // 1. S&P 500 history for 2026-03-05
  console.log('📊 1. indexHistories/GSPC (S&P 500) - últimas fechas\n');
  const gspcDocs = await db.collection('indexHistories/GSPC/dates')
    .orderBy('date', 'desc')
    .limit(3)
    .get();
  
  if (!gspcDocs.empty) {
    for (const doc of gspcDocs.docs) {
      const d = doc.data();
      console.log(`   ${d.date}: close=${d.close}, change=${d.change}, changePercent=${d.changePercent}%`);
    }
  } else {
    console.log('   ❌ No hay documentos de GSPC');
    
    // Try alternative path
    console.log('   Buscando en ruta alternativa...');
    const altDocs = await db.collection('indexHistories')
      .limit(5).get();
    console.log(`   indexHistories tiene ${altDocs.docs.length} documentos`);
    for (const d of altDocs.docs) {
      console.log(`     - ${d.id}: ${JSON.stringify(Object.keys(d.data())).slice(0, 80)}`);
    }
  }
  
  // 2. portfolio data for user - detailed for 03-05 and 03-04
  console.log('\n📊 2. Performance detallada - 2 últimos días\n');
  
  for (const dateStr of ['2026-03-05', '2026-03-04']) {
    const doc = await db.collection(`portfolioPerformance/${userId}/dates`).doc(dateStr).get();
    if (doc.exists) {
      const data = doc.data();
      const usd = data.USD || {};
      console.log(`   --- ${dateStr} ---`);
      console.log(`   totalValue:       $${usd.totalValue?.toFixed(2)}`);
      console.log(`   totalInvestment:  $${usd.totalInvestment?.toFixed(2)}`);
      console.log(`   dailyChangePct:   ${usd.dailyChangePercentage?.toFixed(4)}%`);
      console.log(`   adjDailyChangePct: ${usd.adjustedDailyChangePercentage?.toFixed(4)}%`);
      console.log(`   totalROI:         ${usd.totalROI?.toFixed(4)}%`);
      console.log(`   factor:           ${usd.factor}`);
      console.log(`   unrealizedPnL:    $${usd.unrealizedProfitAndLoss?.toFixed(2)}`);
      
      // Check assetPerformance
      const ap = usd.assetPerformance || {};
      const assetKeys = Object.keys(ap);
      console.log(`   Activos: ${assetKeys.length}`);
      let sumAssetValue = 0;
      let sumAssetInv = 0;
      for (const [key, asset] of Object.entries(ap)) {
        sumAssetValue += asset.totalValue || 0;
        sumAssetInv += asset.totalInvestment || 0;
        console.log(`     ${key}: value=$${asset.totalValue?.toFixed(2)}, inv=$${asset.totalInvestment?.toFixed(2)}, units=${asset.units}`);
      }
      console.log(`   Sum assets value: $${sumAssetValue.toFixed(2)} (vs totalValue: $${usd.totalValue?.toFixed(2)})`);
      console.log(`   Sum assets inv:   $${sumAssetInv.toFixed(2)} (vs totalInvestment: $${usd.totalInvestment?.toFixed(2)})`);
      console.log('');
    }
  }

  // 3. Check actual stocks in user's account
  console.log('📊 3. Assets del usuario (collection assets)\n');
  const assetsSnap = await db.collectionGroup('holdings')
    .where('userId', '==', userId)
    .get();
  
  if (!assetsSnap.empty) {
    let totalBought = 0;
    for (const doc of assetsSnap.docs) {
      const a = doc.data();
      const cost = (a.amount || a.units || 0) * (a.averagePrice || a.price || 0);
      totalBought += cost;
      console.log(`   ${a.symbol || a.name || doc.id}: units=${a.amount || a.units}, avgPrice=$${a.averagePrice || a.price}, cost=$${cost.toFixed(2)}`);
    }
    console.log(`   Total cost from assets: $${totalBought.toFixed(2)}`);
  } else {
    console.log('   Buscando en assets/{userId}/holdings...');
    const assetsAlt = await db.collection(`assets/${userId}/holdings`).get();
    if (!assetsAlt.empty) {
      for (const doc of assetsAlt.docs) {
        const a = doc.data();
        console.log(`   ${a.symbol || a.name || doc.id}: units=${a.amount || a.units || a.quantity}, type=${a.type || a.assetType || 'N/A'}`);
        console.log(`     Raw data keys: ${Object.keys(a).join(', ')}`);
      }
    } else {
      console.log('   No assets found. Checking all known paths...');
      // Check if assets are in portfolioAccounts
      const accounts = await db.collection(`portfolioAccounts/${userId}/accounts`).get();
      console.log(`   portfolioAccounts: ${accounts.docs.length} cuentas`);
      for (const acc of accounts.docs) {
        console.log(`     Account ${acc.id}: ${JSON.stringify(acc.data()).slice(0, 100)}`);
      }
    }
  }

  // 4. Dashboard vs Firestore discrepancy analysis
  console.log('\n📊 4. Análisis de discrepancia Dashboard ($7,365.44) vs Firestore ($8,127.95)\n');
  
  const dashboardValue = 7365.44;
  const dashboardInv = 6875.91;
  const firestoreValue = 8127.95;
  const firestoreInv = 7787.67;
  
  console.log(`   Diferencia en totalValue:      $${(firestoreValue - dashboardValue).toFixed(2)} (${((firestoreValue - dashboardValue) / dashboardValue * 100).toFixed(1)}%)`);
  console.log(`   Diferencia en totalInvestment: $${(firestoreInv - dashboardInv).toFixed(2)} (${((firestoreInv - dashboardInv) / dashboardInv * 100).toFixed(1)}%)`);
  console.log('');
  console.log('   Posibles causas:');
  console.log('   a) Dashboard usa SOLO cuentas activas; performance incluye todas');
  console.log('   b) Dashboard calcula intraday con precios live; Firestore es del día anterior');
  console.log('   c) Dashboard excluye cash/bonds; performance los incluye');
  console.log('   d) Dashboard y Firestore usan diferentes períodos de cálculo');

  // 5. Check userData.stocks (the holdings array the briefing uses)
  console.log('\n📊 5. userData.stocks (lo que usa el briefing para holdings)\n');
  const userDoc = await db.collection('userData').doc(userId).get();
  if (userDoc.exists) {
    const ud = userDoc.data();
    const stocks = ud.stocks || [];
    console.log(`   stocks array: ${stocks.length} elementos`);
    if (stocks.length > 0) {
      for (const s of stocks) {
        console.log(`     ${s.name}: units=${s.units}, type=${s.type}`);
      }
    } else {
      console.log('   ⚠️  stocks array VACÍO - el briefing NO tiene holdings!');
      console.log('   Esto significa Contributors vacío en el correo');
    }
    
    // Check briefingPreferences
    const bp = ud.briefingPreferences || {};
    console.log(`\n   briefingPreferences: ${JSON.stringify(bp)}`);
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
