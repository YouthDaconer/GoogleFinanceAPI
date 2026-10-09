/**
 * Diagnóstico: ¿Por qué no se guardaron los datos del 9 de marzo de 2026?
 * La función unifiedMarketDataUpdate debió ejecutarse el 10 de marzo a las 00:05 ET
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

async function main() {
  console.log('═'.repeat(80));
  console.log('DIAGNÓSTICO: Datos faltantes del 9 de marzo 2026');
  console.log('═'.repeat(80));

  // 1. Verificar qué fechas existen en portfolioPerformance OVERALL
  console.log('\n📋 Últimos 10 días en portfolioPerformance OVERALL:');
  const overallSnap = await db.collection(`portfolioPerformance/${userId}/dates`)
    .orderBy('date', 'desc')
    .limit(15)
    .get();
  
  const existingDates = [];
  overallSnap.docs.forEach(doc => {
    const data = doc.data();
    existingDates.push(doc.id);
    const usd = data.USD || {};
    console.log(`   ${doc.id}: val=${usd.totalValue?.toFixed(2)}, inv=${usd.totalInvestment?.toFixed(2)}, adj%=${usd.adjustedDailyChangePercentage?.toFixed(4)}%`);
  });

  // 2. Verificar qué fechas FALTAN entre 2026-03-01 y 2026-03-10
  console.log('\n📋 Verificación de fechas 2026-03-01 a 2026-03-10:');
  const expectedTradingDays = [
    '2026-03-02', '2026-03-03', '2026-03-04', '2026-03-05', '2026-03-06',
    '2026-03-09' // 7-8 son fin de semana
  ];
  
  for (const date of expectedTradingDays) {
    const docSnap = await db.doc(`portfolioPerformance/${userId}/dates/${date}`).get();
    const status = docSnap.exists ? '✅ EXISTE' : '❌ FALTA';
    if (docSnap.exists) {
      const usd = docSnap.data().USD || {};
      console.log(`   ${date}: ${status} (val=${usd.totalValue?.toFixed(2)}, adj%=${usd.adjustedDailyChangePercentage?.toFixed(4)}%)`);
    } else {
      console.log(`   ${date}: ${status}`);
    }
  }

  // 3. Verificar las cuentas individuales para 2026-03-09
  console.log('\n📋 Cuentas para 2026-03-09:');
  const accountsRef = db.collection(`portfolioPerformance/${userId}/accounts`);
  const accountsSnap = await accountsRef.get();
  
  for (const accDoc of accountsSnap.docs) {
    const accId = accDoc.id;
    const dateDoc = await db.doc(`portfolioPerformance/${userId}/accounts/${accId}/dates/2026-03-09`).get();
    const status = dateDoc.exists ? '✅' : '❌';
    if (dateDoc.exists) {
      const usd = dateDoc.data().USD || {};
      console.log(`   ${accId}: ${status} val=${usd.totalValue?.toFixed(2)}`);
    } else {
      console.log(`   ${accId}: ${status} NO EXISTE`);
    }
  }

  // 4. ¿El 9 de marzo fue festivo?
  console.log('\n📋 Verificación de festivos:');
  const holidayDoc = await db.doc('marketHolidays/US').get();
  if (holidayDoc.exists) {
    const hData = holidayDoc.data();
    console.log(`   marketHolidays/US keys: ${Object.keys(hData).join(', ')}`);
    
    // Try different structures
    const holidays = hData.holidays || hData.dates || [];
    const isArray = Array.isArray(holidays);
    
    if (isArray) {
      const march9Holiday = holidays.find(h => (h.date || h.atDate || h) === '2026-03-09');
      console.log(`   2026-03-09 in holidays array: ${march9Holiday ? JSON.stringify(march9Holiday) : 'NO'}`);
    } else if (typeof holidays === 'object') {
      console.log(`   holidays is object with keys: ${Object.keys(holidays).slice(0, 10).join(', ')}...`);
    }
    
    // Check if 2026-03-09 is directly a field
    if (hData['2026-03-09']) {
      console.log(`   ⚠️ 2026-03-09 IS a holiday field: ${JSON.stringify(hData['2026-03-09'])}`);
    }
    
    // Show all keys that contain '2026-03'
    const marchKeys = Object.keys(hData).filter(k => k.includes('2026-03'));
    if (marchKeys.length > 0) {
      console.log(`   March 2026 keys: ${marchKeys.join(', ')}`);
    }
    
    // Print raw data (truncated)
    const raw = JSON.stringify(hData).substring(0, 1000);
    console.log(`   Raw data (first 1000 chars): ${raw}`);
  } else {
    console.log('   marketHolidays/US NO EXISTE');
  }

  // 5. Verificar isValidTradingDay para el 9 marzo (lunes)
  const date = new Date('2026-03-09');
  const dayOfWeek = date.getDay(); // 0=Sunday, 1=Monday
  console.log(`\n   2026-03-09 es día de la semana: ${dayOfWeek} (${['Dom','Lun','Mar','Mie','Jue','Vie','Sab'][dayOfWeek]})`);

  // 6. Verificar si hay transacciones del 9 de marzo
  console.log('\n📋 Transacciones del 9 de marzo:');
  const txSnap = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('date', '>=', '2026-03-09')
    .where('date', '<', '2026-03-10')
    .get();
  console.log(`   Total: ${txSnap.size}`);
  txSnap.docs.forEach(doc => {
    const t = doc.data();
    console.log(`   - ${t.type}: ${t.name || t.ticker}, amount=${t.amount}x @ ${t.price} ${t.currency}`);
  });

  // También buscar con timestamp
  const txSnap2 = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('date', '>=', '2026-03-09T00:00:00.000Z')
    .where('date', '<=', '2026-03-09T23:59:59.999Z')
    .get();
  console.log(`   Con timestamp: ${txSnap2.size}`);

  // 7. Verificar si existen assets activos con portfolioAccounts activas
  console.log('\n📋 Cuentas activas del usuario:');
  const userAccountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  console.log(`   Total: ${userAccountsSnap.size}`);
  userAccountsSnap.docs.forEach(doc => {
    const a = doc.data();
    console.log(`   - ${a.name} (${doc.id})`);
  });

  console.log('\n📋 Assets activos del usuario:');
  const assetsSnap = await db.collection('assets')
    .where('isActive', '==', true)
    .get();
  const userAssets = assetsSnap.docs.filter(doc => {
    const a = doc.data();
    return userAccountsSnap.docs.some(acc => acc.id === a.portfolioAccount);
  });
  console.log(`   Total assets del usuario: ${userAssets.length}`);

  // 8. ¿El scheduler se corrió? Verificar si hay datos para OTROS usuarios el 9 de marzo
  console.log('\n📋 ¿Otros usuarios tienen datos para 2026-03-09?');
  const allUsersSnap = await db.collection('portfolioPerformance').get();
  for (const userDoc of allUsersSnap.docs) {
    const otherUserId = userDoc.id;
    const dateDoc = await db.doc(`portfolioPerformance/${otherUserId}/dates/2026-03-09`).get();
    const status = dateDoc.exists ? '✅' : '❌';
    console.log(`   ${otherUserId}: ${status}`);
  }

  // 9. Verificar el documento de trading day validation
  console.log('\n📋 markets/US status:');
  const marketDoc = await db.doc('markets/US').get();
  if (marketDoc.exists) {
    const mData = marketDoc.data();
    console.log(`   isOpen: ${mData.isOpen}`);
    console.log(`   lastUpdated: ${mData.lastUpdated}`);
  }

  // 10. Verificar datos del 10 de marzo (para ver si la función corrió pero saltó el 9)
  console.log('\n📋 ¿Existe 2026-03-10?');
  const mar10 = await db.doc(`portfolioPerformance/${userId}/dates/2026-03-10`).get();
  console.log(`   ${mar10.exists ? '✅ EXISTE' : '❌ NO EXISTE'}`);

  console.log('\n' + '═'.repeat(80));
  console.log('FIN DEL DIAGNÓSTICO');
  console.log('═'.repeat(80));
  
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
