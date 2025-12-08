/**
 * Script para invalidar el cache de performance de un usuario
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();
const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';

async function invalidateCache() {
  console.log('='.repeat(80));
  console.log('INVALIDANDO CACHE DE PERFORMANCE');
  console.log('='.repeat(80));
  console.log();

  const cacheCollection = db.collection(`userData/${USER_ID}/performanceCache`);
  const cacheSnapshot = await cacheCollection.get();

  console.log(`📦 Documentos de cache encontrados: ${cacheSnapshot.docs.length}`);

  if (cacheSnapshot.empty) {
    console.log('✅ No hay cache que invalidar');
    process.exit(0);
    return;
  }

  const batch = db.batch();
  
  for (const doc of cacheSnapshot.docs) {
    console.log(`   🗑️ Eliminando: ${doc.id}`);
    batch.delete(doc.ref);
  }

  await batch.commit();

  console.log();
  console.log('='.repeat(80));
  console.log('✅ CACHE INVALIDADO EXITOSAMENTE');
  console.log('='.repeat(80));
  
  process.exit(0);
}

invalidateCache().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
