/**
 * Script para corregir el adjustedDailyChangePercentage a nivel de cuenta y overall
 * 
 * Cuando corregimos los assets, también debemos recalcular el nivel de cuenta
 * usando la suma ponderada de los assets o recalculando con los cashflows correctos
 */

const admin = require('firebase-admin');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const CURRENCIES = ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'];

async function fixAccountLevelAdjustedChange() {
  console.log('='.repeat(100));
  console.log('CORRECCIÓN DE adjustedDailyChangePercentage A NIVEL DE CUENTA/OVERALL');
  console.log('='.repeat(100));
  console.log();

  // El approach correcto: recalcular el adjustedDailyChangePercentage a nivel de cuenta
  // usando la suma de cashflows de los assets (que ya están corregidos)

  // 1. Corregir nivel OVERALL
  console.log('='.repeat(50));
  console.log('NIVEL OVERALL');
  console.log('='.repeat(50));
  console.log();

  const overallSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  console.log(`📂 Total documentos: ${overallSnapshot.docs.length}`);

  let overallCorrections = [];
  let previousOverallDoc = null;

  for (const doc of overallSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    
    if (!currencyData || !currencyData.assetPerformance) {
      previousOverallDoc = doc;
      continue;
    }

    const currentAdjChange = currencyData.adjustedDailyChangePercentage || 0;
    const accountTotalValue = currencyData.totalValue || 0;
    const previousTotalValue = previousOverallDoc?.data()?.USD?.totalValue || 0;
    
    // Sumar cashflows de todos los assets (ya corregidos)
    let assetsCashFlowSum = 0;
    for (const [assetKey, assetData] of Object.entries(currencyData.assetPerformance)) {
      assetsCashFlowSum += assetData.totalCashFlow || 0;
    }

    // Recalcular el adjustedDailyChangePercentage usando los cashflows corregidos
    let correctedAdjChange = 0;
    if (previousTotalValue > 0) {
      correctedAdjChange = ((accountTotalValue - previousTotalValue + assetsCashFlowSum) / previousTotalValue) * 100;
    }

    const discrepancy = Math.abs(currentAdjChange - correctedAdjChange);
    
    // Solo corregir si hay discrepancia significativa
    if (discrepancy > 0.01 && previousTotalValue > 0) {
      overallCorrections.push({
        docRef: doc.ref,
        date: data.date,
        currentAdjChange,
        correctedAdjChange,
        assetsCashFlowSum,
        discrepancy
      });
    }

    previousOverallDoc = doc;
  }

  console.log(`📝 Correcciones necesarias: ${overallCorrections.length}`);
  
  if (overallCorrections.length > 0) {
    // Mostrar algunas correcciones significativas
    const significantCorrections = overallCorrections.filter(c => c.discrepancy > 1);
    console.log(`   (${significantCorrections.length} con discrepancia > 1pp)`);
    significantCorrections.slice(0, 5).forEach(c => {
      console.log(`   📅 ${c.date}: ${c.currentAdjChange.toFixed(2)}% → ${c.correctedAdjChange.toFixed(2)}%`);
    });
    
    // Aplicar correcciones
    console.log();
    console.log('🔧 Aplicando correcciones...');
    
    const batchSize = 450;
    for (let i = 0; i < overallCorrections.length; i += batchSize) {
      const batch = db.batch();
      const batchCorrections = overallCorrections.slice(i, i + batchSize);
      
      for (const correction of batchCorrections) {
        const updateData = {};
        for (const currency of CURRENCIES) {
          updateData[`${currency}.adjustedDailyChangePercentage`] = correction.correctedAdjChange;
          updateData[`${currency}.totalCashFlow`] = correction.assetsCashFlowSum;
        }
        batch.update(correction.docRef, updateData);
      }
      
      await batch.commit();
    }
    console.log(`   ✅ ${overallCorrections.length} documentos corregidos`);
  }
  console.log();

  // 2. Corregir cada cuenta
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', USER_ID)
    .get();

  for (const accountDoc of accountsSnapshot.docs) {
    const account = accountDoc.data();
    const accountId = accountDoc.id;

    console.log('='.repeat(50));
    console.log(`CUENTA: ${account.name} (${accountId})`);
    console.log('='.repeat(50));
    console.log();

    const accountSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/accounts/${accountId}/dates`)
      .orderBy('date', 'asc')
      .get();

    if (accountSnapshot.empty) {
      console.log('   (Sin datos)');
      continue;
    }

    console.log(`📂 Total documentos: ${accountSnapshot.docs.length}`);

    let accountCorrections = [];
    let previousAccountDoc = null;

    for (const doc of accountSnapshot.docs) {
      const data = doc.data();
      const currencyData = data.USD;
      
      if (!currencyData || !currencyData.assetPerformance) {
        previousAccountDoc = doc;
        continue;
      }

      const currentAdjChange = currencyData.adjustedDailyChangePercentage || 0;
      const accountTotalValue = currencyData.totalValue || 0;
      const previousTotalValue = previousAccountDoc?.data()?.USD?.totalValue || 0;
      
      let assetsCashFlowSum = 0;
      for (const [assetKey, assetData] of Object.entries(currencyData.assetPerformance)) {
        assetsCashFlowSum += assetData.totalCashFlow || 0;
      }

      let correctedAdjChange = 0;
      if (previousTotalValue > 0) {
        correctedAdjChange = ((accountTotalValue - previousTotalValue + assetsCashFlowSum) / previousTotalValue) * 100;
      }

      const discrepancy = Math.abs(currentAdjChange - correctedAdjChange);
      
      if (discrepancy > 0.01 && previousTotalValue > 0) {
        accountCorrections.push({
          docRef: doc.ref,
          date: data.date,
          currentAdjChange,
          correctedAdjChange,
          assetsCashFlowSum,
          discrepancy
        });
      }

      previousAccountDoc = doc;
    }

    console.log(`📝 Correcciones necesarias: ${accountCorrections.length}`);
    
    if (accountCorrections.length > 0) {
      const significantCorrections = accountCorrections.filter(c => c.discrepancy > 1);
      console.log(`   (${significantCorrections.length} con discrepancia > 1pp)`);
      
      // Aplicar correcciones
      const batchSize = 450;
      for (let i = 0; i < accountCorrections.length; i += batchSize) {
        const batch = db.batch();
        const batchCorrections = accountCorrections.slice(i, i + batchSize);
        
        for (const correction of batchCorrections) {
          const updateData = {};
          for (const currency of CURRENCIES) {
            updateData[`${currency}.adjustedDailyChangePercentage`] = correction.correctedAdjChange;
            updateData[`${currency}.totalCashFlow`] = correction.assetsCashFlowSum;
          }
          batch.update(correction.docRef, updateData);
        }
        
        await batch.commit();
      }
      console.log(`   ✅ ${accountCorrections.length} documentos corregidos`);
    }
    console.log();
  }

  console.log('='.repeat(100));
  console.log('✅ TODAS LAS CORRECCIONES A NIVEL DE CUENTA/OVERALL APLICADAS');
  console.log('='.repeat(100));
  console.log();
  console.log('⚠️ Recuerda invalidar el cache:');
  console.log('   node scripts/invalidatePerformanceCache.js');

  process.exit(0);
}

fixAccountLevelAdjustedChange().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
