/**
 * Script para buscar transacciones del 18-19 de febrero 2026
 */
const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
}
const db = admin.firestore();

const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';

async function findTransactions() {
  console.log('=== Transacciones del 18 de febrero 2026 ===');
  
  const tx18 = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('date', '>=', '2026-02-18T00:00:00.000Z')
    .where('date', '<=', '2026-02-18T23:59:59.999Z')
    .get();
  
  for (const doc of tx18.docs) {
    const tx = doc.data();
    console.log(`${tx.date} - ${tx.type} ${tx.amount} ${tx.assetName} @ ${tx.price} ${tx.currency}`);
    console.log(`  Created: ${tx.createdAt}`);
    console.log(`  Asset ID: ${tx.assetId}`);
  }
  console.log(`Total 18 Feb: ${tx18.docs.length} transacciones\n`);

  console.log('=== Transacciones del 19 de febrero 2026 ===');
  
  const tx19 = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('date', '>=', '2026-02-19T00:00:00.000Z')
    .where('date', '<=', '2026-02-19T23:59:59.999Z')
    .get();
  
  for (const doc of tx19.docs) {
    const tx = doc.data();
    console.log(`${tx.date} - ${tx.type} ${tx.amount} ${tx.assetName} @ ${tx.price} ${tx.currency}`);
    console.log(`  Created: ${tx.createdAt}`);
    console.log(`  Asset ID: ${tx.assetId}`);
  }
  console.log(`Total 19 Feb: ${tx19.docs.length} transacciones\n`);

  // Buscar SPYG y DPZ que aumentaron unidades
  console.log('=== Transacciones de SPYG (todas las fechas) ===');
  const spygTx = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('assetName', '==', 'SPYG')
    .orderBy('date', 'desc')
    .limit(10)
    .get();
  
  for (const doc of spygTx.docs) {
    const tx = doc.data();
    console.log(`${tx.date} - ${tx.type} ${tx.amount} @ ${tx.price} (created: ${tx.createdAt})`);
  }

  console.log('\n=== Transacciones de DPZ (todas las fechas) ===');
  const dpzTx = await db.collection('transactions')
    .where('userId', '==', userId)
    .where('assetName', '==', 'DPZ')
    .orderBy('date', 'desc')
    .limit(10)
    .get();
  
  for (const doc of dpzTx.docs) {
    const tx = doc.data();
    console.log(`${tx.date} - ${tx.type} ${tx.amount} @ ${tx.price} (created: ${tx.createdAt})`);
  }

  process.exit(0);
}

findTransactions().catch(err => { console.error(err); process.exit(1); });
