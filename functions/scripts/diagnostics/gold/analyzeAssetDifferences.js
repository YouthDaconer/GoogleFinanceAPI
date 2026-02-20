/**
 * Script para analizar diferencias de unidades entre días 
 * y transacciones recientes de SPYG y DPZ
 */
const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
}
const db = admin.firestore();

const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';

async function analyzeAssets() {
  console.log('='.repeat(80));
  console.log('ANÁLISIS DE ACTIVOS CON DIFERENCIAS DE UNIDADES');
  console.log('='.repeat(80));

  // Obtener cuentas del usuario
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  const accountIds = accountsSnap.docs.map(d => d.id);
  console.log('\nCuentas activas:', accountIds);

  // Buscar todos los assets SPYG
  console.log('\n=== ASSETS DE SPYG ===');
  for (const accountId of accountIds) {
    const assetsSnap = await db.collection('assets')
      .where('portfolioAccount', '==', accountId)
      .where('name', '==', 'SPYG')
      .get();
    
    if (assetsSnap.docs.length > 0) {
      console.log(`\nCuenta ${accountId}:`);
      for (const doc of assetsSnap.docs) {
        const asset = doc.data();
        console.log(`  Asset ID: ${doc.id}`);
        console.log(`    units: ${asset.units}`);
        console.log(`    isActive: ${asset.isActive}`);
        console.log(`    acquisitionDate: ${asset.acquisitionDate}`);
        console.log(`    market: ${asset.market}`);
      }
    }
  }

  // Buscar todos los assets DPZ
  console.log('\n=== ASSETS DE DPZ ===');
  for (const accountId of accountIds) {
    const assetsSnap = await db.collection('assets')
      .where('portfolioAccount', '==', accountId)
      .where('name', '==', 'DPZ')
      .get();
    
    if (assetsSnap.docs.length > 0) {
      console.log(`\nCuenta ${accountId}:`);
      for (const doc of assetsSnap.docs) {
        const asset = doc.data();
        console.log(`  Asset ID: ${doc.id}`);
        console.log(`    units: ${asset.units}`);
        console.log(`    isActive: ${asset.isActive}`);
        console.log(`    acquisitionDate: ${asset.acquisitionDate}`);
      }
    }
  }

  // Buscar transacciones recientes por assetId para SPYG
  console.log('\n=== TRANSACCIONES RECIENTES (por assetId individualmente) ===');
  
  // Primero obtener los IDs de assets SPYG
  const spygAssetIds = [];
  for (const accountId of accountIds) {
    const assetsSnap = await db.collection('assets')
      .where('portfolioAccount', '==', accountId)
      .where('name', '==', 'SPYG')
      .get();
    spygAssetIds.push(...assetsSnap.docs.map(d => d.id));
  }
  
  console.log('\nAsset IDs de SPYG:', spygAssetIds);
  
  // Buscar transacciones de cada asset SPYG
  for (const assetId of spygAssetIds.slice(0, 5)) { // Limitar a 5 para no tardar mucho
    const txSnap = await db.collection('transactions')
      .where('assetId', '==', assetId)
      .orderBy('date', 'desc')
      .limit(3)
      .get();
    
    if (txSnap.docs.length > 0) {
      console.log(`\nTransacciones de asset ${assetId}:`);
      for (const doc of txSnap.docs) {
        const tx = doc.data();
        console.log(`  ${tx.date} - ${tx.type} ${tx.amount} @ ${tx.price}`);
        console.log(`    createdAt: ${JSON.stringify(tx.createdAt)}`);
      }
    }
  }

  // También revisar las transacciones creadas recientemente
  console.log('\n=== TRANSACCIONES CREADAS RECIENTEMENTE (últimas 24h) ===');
  const recentDate = new Date();
  recentDate.setDate(recentDate.getDate() - 1);
  
  const recentTxSnap = await db.collection('transactions')
    .where('userId', '==', userId)
    .orderBy('createdAt', 'desc')
    .limit(20)
    .get();
  
  console.log(`\nÚltimas 20 transacciones por createdAt:`);
  for (const doc of recentTxSnap.docs) {
    const tx = doc.data();
    const createdAt = tx.createdAt?.toDate?.() || tx.createdAt;
    console.log(`${tx.assetName} - ${tx.type} ${tx.amount} units`);
    console.log(`  date: ${tx.date}`);
    console.log(`  createdAt: ${createdAt}`);
  }

  process.exit(0);
}

analyzeAssets().catch(err => { console.error(err); process.exit(1); });
