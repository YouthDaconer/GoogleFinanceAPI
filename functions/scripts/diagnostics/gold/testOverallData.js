/**
 * Script para verificar qué datos devuelve getHistoricalReturns para overall
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

async function testOverall() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const currency = 'USD';
  
  console.log('=== TEST: getHistoricalReturns para overall ===');
  console.log('');
  
  // Simular lo que hace getHistoricalReturns internamente
  const overallSnap = await db.collection(`portfolioPerformance/${userId}/dates`)
    .orderBy('date', 'asc')
    .get();
  
  console.log('Documentos en overall:', overallSnap.size);
  
  if (overallSnap.size > 0) {
    const firstDoc = overallSnap.docs[0].data();
    const lastDoc = overallSnap.docs[overallSnap.docs.length - 1].data();
    console.log('Primer documento:', firstDoc.date);
    console.log('Último documento:', lastDoc.date);
    console.log('');
    
    // Calcular performanceByYear (simulando lo que hace el backend)
    const performanceByYear = {};
    
    overallSnap.docs.forEach(doc => {
      const data = doc.data();
      const currencyData = data[currency];
      if (!currencyData) return;
      
      const date = data.date;
      const [year, month] = date.split('-');
      const monthNum = parseInt(month);
      const change = currencyData.adjustedDailyChangePercentage || 0;
      
      if (!performanceByYear[year]) {
        performanceByYear[year] = { months: {}, factor: 1 };
        for (let m = 1; m <= 12; m++) performanceByYear[year].months[m] = 0;
      }
      
      // Acumular cambio diario al mes
      performanceByYear[year].months[monthNum] = (performanceByYear[year].months[monthNum] || 0) + change;
      performanceByYear[year].factor *= (1 + change / 100);
    });
    
    // Calcular totales
    Object.keys(performanceByYear).forEach(year => {
      performanceByYear[year].total = (performanceByYear[year].factor - 1) * 100;
    });
    
    const availableYears = Object.keys(performanceByYear).sort().reverse();
    
    console.log('availableYears:', availableYears);
    console.log('');
    console.log('performanceByYear:');
    availableYears.forEach(year => {
      console.log(`  ${year}: ${performanceByYear[year].total.toFixed(2)}%`);
    });
  }
  
  process.exit(0);
}

testOverall().catch(e => { console.error(e); process.exit(1); });
