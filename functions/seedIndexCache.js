/**
 * Script para poblar el cache inicial de índices
 * Ejecutar: node seedIndexCache.js
 */

require('dotenv').config();
const { calculateIndexData } = require('./services/indexHistoryService');
const admin = require('./services/firebaseAdmin');
const db = admin.firestore();

const RANGES = ['1M', '3M', '6M', 'YTD', '1Y', '5Y', 'MAX'];

async function seedCache() {
  console.log('🚀 Iniciando seed de cache de índices...');
  
  const indicesSnapshot = await db.collection('indexHistories').get();
  const indices = indicesSnapshot.docs.map(doc => doc.id);
  
  console.log(`📊 Procesando ${indices.length} índices x ${RANGES.length} rangos = ${indices.length * RANGES.length} caches`);
  
  let created = 0;
  let errors = 0;
  
  for (const code of indices) {
    console.log(`\n📈 Procesando ${code}...`);
    for (const range of RANGES) {
      try {
        const result = await calculateIndexData(code, range);
        const cacheKey = `${code}_${range}`;
        
        await db.collection('indexCache').doc(cacheKey).set({
          ...result,
          lastUpdated: Date.now(),
        });
        
        created++;
        process.stdout.write(`  ✅ ${range} (${result.chartData.length} pts) `);
      } catch (err) {
        errors++;
        process.stdout.write(`  ❌ ${range} `);
      }
    }
  }
  
  console.log(`\n\n${'='.repeat(50)}`);
  console.log(`✅ Completado: ${created} caches creados, ${errors} errores`);
  console.log(`${'='.repeat(50)}\n`);
  process.exit(0);
}

seedCache().catch(err => {
  console.error('Error fatal:', err);
  process.exit(1);
});
