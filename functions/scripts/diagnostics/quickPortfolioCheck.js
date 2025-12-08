/**
 * Script de Diagnóstico: Ver estructura de portfolioPerformance
 * 
 * Consulta simple para ver qué datos hay disponibles
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

async function main() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  
  console.log('📊 Analizando portfolioPerformance...');
  console.log('');
  
  // Opción 1: Consultar como colección raíz con userId
  console.log('🔍 Opción 1: Consultar colección raíz con where userId...');
  const snapshot1 = await db.collection('portfolioPerformance')
    .where('userId', '==', userId)
    .limit(5)
    .get();
  console.log(`   Documentos encontrados: ${snapshot1.docs.length}`);
  
  if (snapshot1.docs.length > 0) {
    console.log('   Primer doc ID:', snapshot1.docs[0].id);
    console.log('   Campos:', Object.keys(snapshot1.docs[0].data()));
  }
  console.log('');
  
  // Opción 2: Consultar como subcolección users/{userId}/portfolioPerformance
  console.log('🔍 Opción 2: Consultar subcolección users/{userId}/portfolioPerformance...');
  const snapshot2 = await db.collection('users').doc(userId)
    .collection('portfolioPerformance')
    .limit(5)
    .get();
  console.log(`   Documentos encontrados: ${snapshot2.docs.length}`);
  
  if (snapshot2.docs.length > 0) {
    const doc = snapshot2.docs[0];
    const data = doc.data();
    console.log('   Primer doc ID:', doc.id);
    console.log('   Campos de primer nivel:', Object.keys(data));
    console.log('');
    console.log('   📋 Ejemplo de documento:');
    console.log('   - date:', data.date);
    console.log('   - portfolioAccount:', data.portfolioAccount);
    
    // Ver si tiene USD
    if (data.USD) {
      console.log('   - USD.totalValue:', data.USD.totalValue);
      console.log('   - USD.totalInvestment:', data.USD.totalInvestment);
      console.log('   - USD.totalCashFlow:', data.USD.totalCashFlow);
      console.log('   - USD.adjustedDailyChangePercentage:', data.USD.adjustedDailyChangePercentage);
    }
  }
  console.log('');
  
  // Opción 3: Ver qué colecciones hay bajo el documento principal
  console.log('🔍 Opción 3: Ver subcolecciones del documento raíz portfolioPerformance/{userId}...');
  const docRef = db.collection('portfolioPerformance').doc(userId);
  const collections = await docRef.listCollections();
  console.log(`   Subcolecciones encontradas: ${collections.length}`);
  collections.forEach(coll => console.log(`   - ${coll.id}`));
  
  if (collections.length > 0) {
    // Explorar la subcolección 'dates'
    console.log('');
    console.log('🔍 Explorando subcolección "dates"...');
    const datesSnapshot = await docRef.collection('dates').limit(5).get();
    console.log(`   Documentos en dates: ${datesSnapshot.docs.length}`);
    
    if (datesSnapshot.docs.length > 0) {
      const doc = datesSnapshot.docs[0];
      const data = doc.data();
      console.log('');
      console.log('   📋 Ejemplo de documento en dates:');
      console.log('   - ID (fecha?):', doc.id);
      console.log('   - date:', data.date);
      console.log('   - portfolioAccount:', data.portfolioAccount);
      console.log('   - Campos de primer nivel:', Object.keys(data).slice(0, 10));
      
      if (data.USD) {
        console.log('');
        console.log('   📊 Datos en USD:');
        console.log('      - totalValue:', data.USD.totalValue);
        console.log('      - totalInvestment:', data.USD.totalInvestment);
        console.log('      - totalCashFlow:', data.USD.totalCashFlow);
        console.log('      - adjustedDailyChangePercentage:', data.USD.adjustedDailyChangePercentage);
        console.log('      - doneProfitAndLoss:', data.USD.doneProfitAndLoss);
        console.log('      - unrealizedProfitAndLoss:', data.USD.unrealizedProfitAndLoss);
      }
    }
    
    // Explorar la subcolección 'accounts'
    console.log('');
    console.log('🔍 Explorando subcolección "accounts"...');
    const accountsSnapshot = await docRef.collection('accounts').get();
    console.log(`   Documentos en accounts: ${accountsSnapshot.docs.length}`);
    
    for (const accDoc of accountsSnapshot.docs) {
      const accData = accDoc.data();
      console.log(`   - Account ID: ${accDoc.id}`);
      console.log(`     name: ${accData.name || accData.accountName || 'N/A'}`);
      
      // Ver si tiene subcolección 'dates' dentro
      const accDatesSnapshot = await accDoc.ref.collection('dates').limit(3).get();
      console.log(`     dates subdocs: ${accDatesSnapshot.docs.length}`);
      
      if (accDatesSnapshot.docs.length > 0) {
        const dateDoc = accDatesSnapshot.docs[0];
        const dateData = dateDoc.data();
        console.log(`     Sample date ID: ${dateDoc.id}`);
        console.log(`     Sample date fields: ${Object.keys(dateData).slice(0, 5)}`);
        
        if (dateData.USD) {
          console.log(`     USD.totalValue: ${dateData.USD.totalValue}`);
          console.log(`     USD.totalCashFlow: ${dateData.USD.totalCashFlow}`);
        }
      }
    }
  }
  
  console.log('');
  console.log('✅ Análisis completo');
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
