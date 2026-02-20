/**
 * Script de diagnóstico para analizar discrepancia de performance entre 2026-02-18 y 2026-02-19
 * 
 * PROPÓSITO: Investigar la caída significativa de -3.16% a -6.09% (casi 3% en un día)
 * 
 * USO: node analyzePerformanceDiscrepancy.js
 */

const admin = require('firebase-admin');

// Inicializar Firebase Admin
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const DATES_TO_ANALYZE = ['2026-02-17', '2026-02-18', '2026-02-19'];

async function analyzePerformanceDiscrepancy() {
  console.log('='.repeat(80));
  console.log('ANÁLISIS DE DISCREPANCIA DE PERFORMANCE');
  console.log('Usuario:', USER_ID);
  console.log('Fechas:', DATES_TO_ANALYZE.join(', '));
  console.log('='.repeat(80));

  // 1. Obtener documentos de performance para las fechas
  console.log('\n📊 1. DATOS DE PORTFOLIO PERFORMANCE (OVERALL)\n');
  
  const performanceData = {};
  for (const date of DATES_TO_ANALYZE) {
    const docRef = db.collection('portfolioPerformance')
      .doc(USER_ID)
      .collection('dates')
      .doc(date);
    
    const doc = await docRef.get();
    if (doc.exists) {
      performanceData[date] = doc.data();
      console.log(`\n📅 ${date}:`);
      
      const usdData = doc.data().USD;
      console.log('  USD.totalValue:', usdData?.totalValue?.toFixed(2) || 'N/A');
      console.log('  USD.totalInvestment:', usdData?.totalInvestment?.toFixed(2) || 'N/A');
      console.log('  USD.dailyChangePercentage:', usdData?.dailyChangePercentage?.toFixed(4) || 'N/A');
      console.log('  USD.adjustedDailyChangePercentage:', usdData?.adjustedDailyChangePercentage?.toFixed(4) || 'N/A');
      console.log('  USD.rawDailyChangePercentage:', usdData?.rawDailyChangePercentage?.toFixed(4) || 'N/A');
      console.log('  USD.totalCashFlow:', usdData?.totalCashFlow?.toFixed(2) || 'N/A');
      
      // Mostrar activos si existen
      if (usdData?.assetPerformance) {
        console.log('\n  Activos:');
        for (const [assetKey, assetData] of Object.entries(usdData.assetPerformance)) {
          console.log(`    ${assetKey}:`);
          console.log(`      totalValue: ${assetData.totalValue?.toFixed(2) || 'N/A'}`);
          console.log(`      totalInvestment: ${assetData.totalInvestment?.toFixed(2) || 'N/A'}`);
          console.log(`      units: ${assetData.units?.toFixed(6) || 'N/A'}`);
          console.log(`      adjustedDailyChangePercentage: ${assetData.adjustedDailyChangePercentage?.toFixed(4) || 'N/A'}`);
        }
      }
    } else {
      console.log(`\n📅 ${date}: NO EXISTE`);
    }
  }

  // 2. Comparar valores entre 18 y 19
  console.log('\n' + '='.repeat(80));
  console.log('📈 2. COMPARACIÓN VALORES 18 vs 19 FEB\n');
  
  const feb18 = performanceData['2026-02-18']?.USD;
  const feb19 = performanceData['2026-02-19']?.USD;
  
  if (feb18 && feb19) {
    console.log('Valor total:');
    console.log(`  18 Feb: $${feb18.totalValue?.toFixed(2)}`);
    console.log(`  19 Feb: $${feb19.totalValue?.toFixed(2)}`);
    console.log(`  Cambio: $${(feb19.totalValue - feb18.totalValue)?.toFixed(2)} (${((feb19.totalValue - feb18.totalValue) / feb18.totalValue * 100).toFixed(2)}%)`);
    
    // Verificar qué activos cambiaron más
    console.log('\nCambios por activo:');
    const assets18 = feb18.assetPerformance || {};
    const assets19 = feb19.assetPerformance || {};
    
    const allAssets = new Set([...Object.keys(assets18), ...Object.keys(assets19)]);
    for (const asset of allAssets) {
      const v18 = assets18[asset]?.totalValue || 0;
      const v19 = assets19[asset]?.totalValue || 0;
      const u18 = assets18[asset]?.units || 0;
      const u19 = assets19[asset]?.units || 0;
      const change = v19 - v18;
      const changePct = v18 > 0 ? (change / v18 * 100) : 0;
      
      console.log(`  ${asset}:`);
      console.log(`    Valor 18: $${v18.toFixed(2)} (${u18.toFixed(6)} units)`);
      console.log(`    Valor 19: $${v19.toFixed(2)} (${u19.toFixed(6)} units)`);
      console.log(`    Cambio: $${change.toFixed(2)} (${changePct.toFixed(2)}%)`);
      console.log(`    Cambio unidades: ${(u19 - u18).toFixed(6)}`);
    }
  }

  // 3. Verificar transacciones del 19 de febrero
  console.log('\n' + '='.repeat(80));
  console.log('📝 3. TRANSACCIONES DEL 19 DE FEBRERO 2026\n');
  
  const txSnapshot = await db.collection('transactions')
    .where('date', '>=', '2026-02-19T00:00:00.000Z')
    .where('date', '<=', '2026-02-19T23:59:59.999Z')
    .get();
  
  if (txSnapshot.empty) {
    console.log('No hay transacciones del 19 de febrero.');
  } else {
    console.log(`Encontradas ${txSnapshot.docs.length} transacciones:`);
    txSnapshot.docs.forEach(doc => {
      const tx = doc.data();
      console.log(`  - ${tx.type}: ${tx.amount} units @ ${tx.price} ${tx.currency}`);
      console.log(`    Asset: ${tx.assetId || 'N/A'}`);
      console.log(`    Account: ${tx.portfolioAccountId}`);
    });
  }

  // 4. Buscar transacciones también con formato de solo fecha
  console.log('\n📝 3b. TRANSACCIONES CON FORMATO YYYY-MM-DD\n');
  
  const txSnapshot2 = await db.collection('transactions')
    .where('date', '==', '2026-02-19')
    .get();
  
  if (txSnapshot2.empty) {
    console.log('No hay transacciones con fecha exacta 2026-02-19.');
  } else {
    console.log(`Encontradas ${txSnapshot2.docs.length} transacciones:`);
    txSnapshot2.docs.forEach(doc => {
      const tx = doc.data();
      console.log(`  - ${tx.type}: ${tx.amount} units @ ${tx.price} ${tx.currency}`);
    });
  }

  // 5. Verificar los activos actuales
  console.log('\n' + '='.repeat(80));
  console.log('💼 4. ACTIVOS ACTUALES DEL USUARIO\n');
  
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', USER_ID)
    .where('isActive', '==', true)
    .get();
  
  const accountIds = accountsSnapshot.docs.map(d => d.id);
  console.log('Cuentas activas:', accountIds);
  
  for (const accountId of accountIds) {
    const assetsSnapshot = await db.collection('assets')
      .where('portfolioAccount', '==', accountId)
      .where('isActive', '==', true)
      .get();
    
    console.log(`\nCuenta ${accountId}:`);
    assetsSnapshot.docs.forEach(doc => {
      const asset = doc.data();
      console.log(`  - ${asset.name} (${asset.assetType})`);
      console.log(`    Units: ${asset.units}`);
      console.log(`    UnitValue: ${asset.unitValue} ${asset.currency}`);
      console.log(`    Market: ${asset.market}`);
    });
  }

  // 6. Verificar datos de las cuentas individuales para el 18 y 19
  console.log('\n' + '='.repeat(80));
  console.log('📊 5. DATOS POR CUENTA INDIVIDUAL\n');
  
  for (const accountId of accountIds) {
    console.log(`\nCuenta: ${accountId}`);
    
    for (const date of ['2026-02-18', '2026-02-19']) {
      const docRef = db.collection('portfolioPerformance')
        .doc(USER_ID)
        .collection('accounts')
        .doc(accountId)
        .collection('dates')
        .doc(date);
      
      const doc = await docRef.get();
      if (doc.exists) {
        const usdData = doc.data().USD;
        console.log(`  ${date}:`);
        console.log(`    totalValue: $${usdData?.totalValue?.toFixed(2)}`);
        console.log(`    adjustedDailyChangePercentage: ${usdData?.adjustedDailyChangePercentage?.toFixed(4)}%`);
        console.log(`    totalCashFlow: $${usdData?.totalCashFlow?.toFixed(2)}`);
      } else {
        console.log(`  ${date}: NO EXISTE`);
      }
    }
  }

  // 7. Investigar el "lastPerformance" que usó el schedule para el 19
  console.log('\n' + '='.repeat(80));
  console.log('🔍 6. ANÁLISIS DEL CACHÉ (lastPerformance)\n');
  
  // El schedule busca el documento más reciente ANTES de la fecha actual
  // Para el 19 de febrero, debería usar el documento del 18 de febrero
  
  const lastPerfQuery = await db.collection('portfolioPerformance')
    .doc(USER_ID)
    .collection('dates')
    .where('date', '<', '2026-02-19')
    .orderBy('date', 'desc')
    .limit(1)
    .get();
  
  if (!lastPerfQuery.empty) {
    const lastDoc = lastPerfQuery.docs[0];
    console.log(`Último documento antes del 19 Feb: ${lastDoc.id}`);
    const lastUsd = lastDoc.data().USD;
    console.log(`  totalValue: $${lastUsd?.totalValue?.toFixed(2)}`);
    
    // Verificar si este es el valor correcto
    if (lastDoc.id === '2026-02-18') {
      console.log('✅ Correcto: El schedule usó el documento del 18 Feb como base');
    } else {
      console.log(`⚠️ PROBLEMA: El schedule usó el documento del ${lastDoc.id} en lugar del 18 Feb`);
    }
  }

  console.log('\n' + '='.repeat(80));
  console.log('ANÁLISIS COMPLETADO');
  console.log('='.repeat(80));
  
  process.exit(0);
}

analyzePerformanceDiscrepancy().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
