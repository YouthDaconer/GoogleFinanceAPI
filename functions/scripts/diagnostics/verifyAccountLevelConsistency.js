/**
 * Verificación de consistencia a nivel de cuenta y overall
 * 
 * Compara el adjustedDailyChangePercentage a nivel de cuenta/overall
 * con la suma ponderada de los cambios de cada asset
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';

async function verifyAccountLevelConsistency() {
  console.log('='.repeat(100));
  console.log('VERIFICACIÓN DE CONSISTENCIA A NIVEL DE CUENTA Y OVERALL');
  console.log('='.repeat(100));
  console.log();

  // 1. Obtener todas las cuentas del usuario
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', USER_ID)
    .get();

  const accounts = accountsSnapshot.docs.map(d => ({ id: d.id, ...d.data() }));
  console.log(`📦 Cuentas encontradas: ${accounts.length}`);
  accounts.forEach(a => console.log(`   - ${a.id}: ${a.name}`));
  console.log();

  // 2. Verificar nivel OVERALL
  console.log('='.repeat(100));
  console.log('NIVEL OVERALL (Todas las cuentas)');
  console.log('='.repeat(100));
  console.log();

  const overallSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  console.log(`📂 Total documentos: ${overallSnapshot.docs.length}`);
  console.log();

  let overallProblems = [];
  let previousOverallDoc = null;

  for (const doc of overallSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    
    if (!currencyData || !currencyData.assetPerformance) continue;

    const accountAdjChange = currencyData.adjustedDailyChangePercentage || 0;
    const accountTotalCashFlow = currencyData.totalCashFlow || 0;
    const accountTotalValue = currencyData.totalValue || 0;
    
    // Obtener valor anterior
    const previousTotalValue = previousOverallDoc?.data()?.USD?.totalValue || 0;
    
    // Sumar cashflows de todos los assets
    let assetsCashFlowSum = 0;
    let assetsCount = 0;
    
    for (const [assetKey, assetData] of Object.entries(currencyData.assetPerformance)) {
      assetsCashFlowSum += assetData.totalCashFlow || 0;
      assetsCount++;
    }

    // Calcular lo que debería ser el adjustedDailyChangePercentage
    // Fórmula: (endValue - startValue + cashFlow) / startValue * 100
    let expectedAdjChange = 0;
    if (previousTotalValue > 0) {
      expectedAdjChange = ((accountTotalValue - previousTotalValue + assetsCashFlowSum) / previousTotalValue) * 100;
    }

    // Detectar discrepancia
    const discrepancy = Math.abs(accountAdjChange - expectedAdjChange);
    
    // Solo reportar si hay discrepancia significativa (> 0.5%)
    if (discrepancy > 0.5 && previousTotalValue > 0) {
      overallProblems.push({
        docRef: doc.ref,
        date: data.date,
        accountAdjChange,
        expectedAdjChange,
        discrepancy,
        accountTotalCashFlow,
        assetsCashFlowSum,
        previousTotalValue,
        accountTotalValue
      });
    }

    previousOverallDoc = doc;
  }

  if (overallProblems.length === 0) {
    console.log('✅ No se encontraron discrepancias a nivel OVERALL');
  } else {
    console.log(`⚠️ ${overallProblems.length} discrepancias encontradas:`);
    overallProblems.slice(0, 10).forEach(p => {
      console.log(`   📅 ${p.date}:`);
      console.log(`      Account adjChange: ${p.accountAdjChange.toFixed(4)}%`);
      console.log(`      Expected adjChange: ${p.expectedAdjChange.toFixed(4)}%`);
      console.log(`      Discrepancia: ${p.discrepancy.toFixed(4)}pp`);
      console.log(`      Account cashFlow: $${p.accountTotalCashFlow.toFixed(2)}, Assets sum: $${p.assetsCashFlowSum.toFixed(2)}`);
    });
    if (overallProblems.length > 10) {
      console.log(`   ... y ${overallProblems.length - 10} más`);
    }
  }
  console.log();

  // 3. Verificar cada cuenta individualmente
  for (const account of accounts) {
    console.log('='.repeat(100));
    console.log(`CUENTA: ${account.name} (${account.id})`);
    console.log('='.repeat(100));
    console.log();

    const accountSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/accounts/${account.id}/dates`)
      .orderBy('date', 'asc')
      .get();

    if (accountSnapshot.empty) {
      console.log('   (Sin datos de performance)');
      continue;
    }

    console.log(`📂 Total documentos: ${accountSnapshot.docs.length}`);

    let accountProblems = [];
    let previousAccountDoc = null;

    for (const doc of accountSnapshot.docs) {
      const data = doc.data();
      const currencyData = data.USD;
      
      if (!currencyData || !currencyData.assetPerformance) continue;

      const accountAdjChange = currencyData.adjustedDailyChangePercentage || 0;
      const accountTotalValue = currencyData.totalValue || 0;
      
      const previousTotalValue = previousAccountDoc?.data()?.USD?.totalValue || 0;
      
      // Sumar cashflows de todos los assets
      let assetsCashFlowSum = 0;
      
      for (const [assetKey, assetData] of Object.entries(currencyData.assetPerformance)) {
        assetsCashFlowSum += assetData.totalCashFlow || 0;
      }

      // Calcular expected
      let expectedAdjChange = 0;
      if (previousTotalValue > 0) {
        expectedAdjChange = ((accountTotalValue - previousTotalValue + assetsCashFlowSum) / previousTotalValue) * 100;
      }

      const discrepancy = Math.abs(accountAdjChange - expectedAdjChange);
      
      if (discrepancy > 0.5 && previousTotalValue > 0) {
        accountProblems.push({
          docRef: doc.ref,
          date: data.date,
          accountAdjChange,
          expectedAdjChange,
          discrepancy,
          assetsCashFlowSum,
          previousTotalValue,
          accountTotalValue
        });
      }

      previousAccountDoc = doc;
    }

    if (accountProblems.length === 0) {
      console.log('✅ No se encontraron discrepancias');
    } else {
      console.log(`⚠️ ${accountProblems.length} discrepancias encontradas:`);
      accountProblems.slice(0, 5).forEach(p => {
        console.log(`   📅 ${p.date}: guardado=${p.accountAdjChange.toFixed(2)}%, esperado=${p.expectedAdjChange.toFixed(2)}%, diff=${p.discrepancy.toFixed(2)}pp`);
      });
      if (accountProblems.length > 5) {
        console.log(`   ... y ${accountProblems.length - 5} más`);
      }
    }
    console.log();
  }

  // 4. Resumen final
  console.log('='.repeat(100));
  console.log('RESUMEN FINAL');
  console.log('='.repeat(100));
  console.log();
  console.log('El adjustedDailyChangePercentage a nivel de cuenta/overall se calcula');
  console.log('usando las transacciones del día, no la suma de los assets.');
  console.log();
  console.log('Si hay discrepancias, es porque:');
  console.log('1. Las transacciones originales no se reflejaron (mismo problema que assets)');
  console.log('2. El cálculo a nivel de cuenta usa una fórmula diferente');
  console.log();

  process.exit(0);
}

verifyAccountLevelConsistency().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
