/**
 * DIAGNÓSTICO: Verificar estructura completa de documentos de OVERALL
 * 
 * Analiza qué campos tienen cada documento y si hay inconsistencias
 * en otras monedas o en assetPerformance.
 */

const admin = require('firebase-admin');

const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  // Documentos conocidos con errores previos
  DATES_TO_CHECK: ['2025-01-02', '2025-03-05', '2025-11-18']
};

async function main() {
  console.log('');
  console.log('═'.repeat(100));
  console.log('  DIAGNÓSTICO: Estructura completa de documentos OVERALL');
  console.log('═'.repeat(100));
  console.log('');

  // Obtener documentos específicos
  const overallPath = `portfolioPerformance/${CONFIG.USER_ID}/dates`;
  
  for (const date of CONFIG.DATES_TO_CHECK) {
    console.log('━'.repeat(100));
    console.log(`  DOCUMENTO: ${date}`);
    console.log('━'.repeat(100));
    
    const docRef = db.collection(overallPath).doc(date);
    const doc = await docRef.get();
    
    if (!doc.exists) {
      console.log('  ❌ Documento no existe');
      continue;
    }
    
    const data = doc.data();
    
    // Mostrar estructura del documento
    console.log('');
    console.log('  Campos de nivel superior:');
    Object.keys(data).forEach(key => {
      if (key === 'date') {
        console.log(`    ${key}: ${data[key]}`);
      } else {
        console.log(`    ${key}: [objeto de moneda]`);
      }
    });
    
    // Analizar cada moneda
    console.log('');
    console.log('  Por moneda:');
    
    Object.entries(data).forEach(([currency, currData]) => {
      if (currency === 'date') return;
      
      console.log(`\n    ${currency}:`);
      console.log(`      totalValue: $${(currData.totalValue || 0).toFixed(2)}`);
      console.log(`      totalInvestment: $${(currData.totalInvestment || 0).toFixed(2)}`);
      console.log(`      adjustedDailyChangePercentage: ${(currData.adjustedDailyChangePercentage || 0).toFixed(4)}%`);
      console.log(`      rawDailyChangePercentage: ${(currData.rawDailyChangePercentage || 0).toFixed(4)}%`);
      console.log(`      dailyChangePercentage: ${(currData.dailyChangePercentage || 0).toFixed(4)}%`);
      
      // Verificar assetPerformance
      if (currData.assetPerformance) {
        const assetCount = Object.keys(currData.assetPerformance).length;
        console.log(`      assetPerformance: ${assetCount} assets`);
        
        // Mostrar algunos assets de ejemplo
        const assetEntries = Object.entries(currData.assetPerformance).slice(0, 3);
        assetEntries.forEach(([assetKey, assetData]) => {
          console.log(`        ${assetKey}:`);
          console.log(`          totalValue: $${(assetData.totalValue || 0).toFixed(2)}`);
          console.log(`          adjustedDailyChangePercentage: ${(assetData.adjustedDailyChangePercentage || 0).toFixed(4)}%`);
        });
        if (assetCount > 3) {
          console.log(`        ... y ${assetCount - 3} assets más`);
        }
      }
    });
  }

  // Obtener documento de cuenta individual para comparar
  console.log('');
  console.log('━'.repeat(100));
  console.log('  COMPARACIÓN CON CUENTA INDIVIDUAL (IBKR) - 2025-01-02');
  console.log('━'.repeat(100));
  
  const ibkrPath = `portfolioPerformance/${CONFIG.USER_ID}/accounts/BZHvXz4QT2yqqqlFP22X/dates`;
  const ibkrDoc = await db.collection(ibkrPath).doc('2025-01-02').get();
  
  if (ibkrDoc.exists) {
    const ibkrData = ibkrDoc.data();
    
    console.log('');
    console.log('  IBKR 2025-01-02:');
    Object.entries(ibkrData).forEach(([currency, currData]) => {
      if (currency === 'date') return;
      
      console.log(`\n    ${currency}:`);
      console.log(`      totalValue: $${(currData.totalValue || 0).toFixed(2)}`);
      console.log(`      adjustedDailyChangePercentage: ${(currData.adjustedDailyChangePercentage || 0).toFixed(4)}%`);
      console.log(`      rawDailyChangePercentage: ${(currData.rawDailyChangePercentage || 0).toFixed(4)}%`);
      
      if (currData.assetPerformance) {
        Object.entries(currData.assetPerformance).slice(0, 2).forEach(([assetKey, assetData]) => {
          console.log(`        ${assetKey}: adj=${(assetData.adjustedDailyChangePercentage || 0).toFixed(4)}%`);
        });
      }
    });
  }

  console.log('');
  console.log('═'.repeat(100));
  console.log('  📋 ANÁLISIS');
  console.log('═'.repeat(100));
  console.log('');
  console.log('  El script fixOverallAdjustedChange.js actualmente SOLO corrige:');
  console.log('    ✅ USD.adjustedDailyChangePercentage');
  console.log('');
  console.log('  NO corrige (potencialmente inconsistente):');
  console.log('    ❓ Otras monedas (COP, EUR, etc.)');
  console.log('    ❓ USD.rawDailyChangePercentage');
  console.log('    ❓ USD.dailyChangePercentage');
  console.log('    ❓ assetPerformance.*.adjustedDailyChangePercentage');
  console.log('');
  console.log('  NOTA: El assetPerformance de OVERALL debería coincidir con la');
  console.log('  suma/promedio de los mismos assets en las cuentas individuales.');
  console.log('');
  console.log('═'.repeat(100));

  process.exit(0);
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
