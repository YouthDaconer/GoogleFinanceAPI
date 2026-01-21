/**
 * Script detallado para investigar la corrupción de datos del 2026-01-16
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

async function investigate() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  
  console.log('='.repeat(90));
  console.log('INVESTIGACIÓN DETALLADA: 2026-01-16');
  console.log('='.repeat(90));
  
  // 1. Verificar el documento overall del 2026-01-16
  console.log('\n1. DOCUMENTO OVERALL 2026-01-16:');
  const doc16 = await db.doc(`portfolioPerformance/${userId}/dates/2026-01-16`).get();
  if (doc16.exists) {
    const data = doc16.data();
    console.log('Fecha:', data.date);
    console.log('Source:', data.source);
    console.log('UpdatedAt:', data.updatedAt);
    console.log('\nUSD:', JSON.stringify(data.USD, null, 2).substring(0, 500) + '...');
  } else {
    console.log('❌ Documento NO existe');
  }
  
  // 2. Verificar si existen datos de accounts para 2026-01-16
  console.log('\n2. DATOS DE ACCOUNTS PARA 2026-01-16:');
  const accounts = ['BZHvXz4QT2yqqqlFP22X', 'Z3gnboYgRlTvSZNGSu8j', 'zHZCvwpQeA2HoYMxDtPF'];
  
  for (const accountId of accounts) {
    const accDoc = await db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/2026-01-16`).get();
    const accountDoc = await db.collection('portfolioAccounts').doc(accountId).get();
    const accountName = accountDoc.exists ? accountDoc.data().name : accountId;
    
    if (accDoc.exists) {
      console.log(`✅ ${accountName}: Existe - totalValue USD: $${accDoc.data().USD?.totalValue || 0}`);
    } else {
      console.log(`❌ ${accountName}: NO EXISTE`);
    }
  }
  
  // 3. Verificar si existen datos de accounts para 2026-01-15
  console.log('\n3. DATOS DE ACCOUNTS PARA 2026-01-15 (último día bueno):');
  
  for (const accountId of accounts) {
    const accDoc = await db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/2026-01-15`).get();
    const accountDoc = await db.collection('portfolioAccounts').doc(accountId).get();
    const accountName = accountDoc.exists ? accountDoc.data().name : accountId;
    
    if (accDoc.exists) {
      const usd = accDoc.data().USD || {};
      console.log(`✅ ${accountName}: $${(usd.totalValue || 0).toFixed(2)}, adjChange: ${((usd.adjustedDailyChangePercentage || 0) * 100).toFixed(4)}%`);
    } else {
      console.log(`❌ ${accountName}: NO EXISTE`);
    }
  }
  
  // 4. Verificar documento overall de 2026-01-15
  console.log('\n4. COMPARAR OVERALL 2026-01-15 vs 2026-01-16:');
  const doc15 = await db.doc(`portfolioPerformance/${userId}/dates/2026-01-15`).get();
  
  if (doc15.exists && doc16.exists) {
    const data15 = doc15.data();
    const data16 = doc16.data();
    
    console.log('Fecha      | totalValue  | assetPerformance keys | source');
    console.log('-'.repeat(80));
    console.log(`2026-01-15 | $${(data15.USD?.totalValue || 0).toFixed(2).padStart(9)} | ${Object.keys(data15.USD?.assetPerformance || {}).length} assets | ${data15.source || 'N/A'}`);
    console.log(`2026-01-16 | $${(data16.USD?.totalValue || 0).toFixed(2).padStart(9)} | ${Object.keys(data16.USD?.assetPerformance || {}).length} assets | ${data16.source || 'N/A'}`);
  }
  
  // 5. Verificar qué función escribió el 2026-01-16
  console.log('\n5. METADATA DEL DOCUMENTO 2026-01-16:');
  if (doc16.exists) {
    const data = doc16.data();
    console.log('Source:', data.source);
    console.log('UpdatedAt:', data.updatedAt);
    console.log('Campos principales:', Object.keys(data).join(', '));
  }
  
  // 6. Verificar documento 2026-01-20 para comparar estructura
  console.log('\n6. COMPARAR ESTRUCTURA 2026-01-20:');
  const doc20 = await db.doc(`portfolioPerformance/${userId}/dates/2026-01-20`).get();
  if (doc20.exists) {
    const data = doc20.data();
    console.log('Source:', data.source || 'N/A');
    console.log('UpdatedAt:', data.updatedAt || 'N/A');
    console.log('Campos:', Object.keys(data).join(', '));
    console.log('USD totalValue:', data.USD?.totalValue);
    console.log('USD assetPerformance keys:', Object.keys(data.USD?.assetPerformance || {}).length);
  }
  
  // 7. Verificar si hay documento de cuenta para 2026-01-20
  console.log('\n7. DATOS DE ACCOUNTS PARA 2026-01-20:');
  
  for (const accountId of accounts) {
    const accDoc = await db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/2026-01-20`).get();
    const accountDoc = await db.collection('portfolioAccounts').doc(accountId).get();
    const accountName = accountDoc.exists ? accountDoc.data().name : accountId;
    
    if (accDoc.exists) {
      const usd = accDoc.data().USD || {};
      console.log(`✅ ${accountName}: $${(usd.totalValue || 0).toFixed(2)}, adjChange: ${((usd.adjustedDailyChangePercentage || 0) * 100).toFixed(4)}%`);
    } else {
      console.log(`❌ ${accountName}: NO EXISTE`);
    }
  }
  
  // CONCLUSIONES
  console.log('\n' + '='.repeat(90));
  console.log('CONCLUSIONES:');
  console.log('='.repeat(90));
  console.log(`
  El problema tiene TRES capas:
  
  1. 2026-01-16 tiene datos OVERALL pero NO tiene datos por CUENTA
     - Esto es una escritura incompleta (batch parcialmente fallido?)
     - O fue escrito por una función diferente (scheduledPortfolioCalculations vs unifiedMarketDataUpdate)
  
  2. 2026-01-17 (viernes) NO tiene datos en absoluto
     - El mercado estaba abierto
     - unifiedMarketDataUpdate debió ejecutarse el sábado 18/01 a las 00:05
     - Posiblemente fue bloqueado por verificación de festivos incorrecta
  
  3. 2026-01-20 fue escrito con caché corrupto
     - El caché de cuentas encontró 2026-01-15 (no 2026-01-16)
     - El caché de overall encontró 2026-01-16
     - Esta inconsistencia causó cálculos incorrectos
  `);
}

investigate().catch(console.error).finally(() => process.exit(0));
