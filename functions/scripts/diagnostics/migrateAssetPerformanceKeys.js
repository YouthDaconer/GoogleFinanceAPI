/**
 * Script para migrar datos de assetPerformance al formato correcto
 * 
 * PROBLEMA:
 * Datos de 2024-08-16 a 2025-06-05 tienen keys como "VUAA.L" 
 * en lugar de "VUAA.L_etf"
 * 
 * SOLUCIÓN:
 * 1. Leer cada documento con formato incorrecto
 * 2. Mapear cada key al formato correcto usando el tipo de asset
 * 3. Actualizar el documento con los nuevos keys
 * 
 * NOTA: En Firestore, los keys con "." o "_" se manejan como strings normales
 * cuando se acceden usando bracket notation o FieldPath.
 */

const admin = require('firebase-admin');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const CURRENCIES = ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'];

// Mapeo de símbolos al tipo de asset correcto
// Basado en los assets históricos del usuario
const ASSET_TYPE_MAP = {
  // ETFs
  'VUAA.L': 'etf',
  'EIMI.L': 'etf',
  'SPYG': 'etf',
  'SOXQ': 'etf',
  'TLT': 'etf',
  'XLRE': 'etf',
  
  // Stocks
  'AAPL': 'stock',
  'AMZN': 'stock',
  'BABA': 'stock',
  'CAT': 'stock',
  'COST': 'stock',
  'CRM': 'stock',
  'DIS': 'stock',
  'DPZ': 'stock',
  'FSLR': 'stock',
  'GOOGL': 'stock',
  'HD': 'stock',
  'JNJ': 'stock',
  'LLY': 'stock',
  'LMT': 'stock',
  'MA': 'stock',
  'MC.PA': 'stock',
  'MCD': 'stock',
  'MELI': 'stock',
  'META': 'stock',
  'MSFT': 'stock',
  'NKE': 'stock',
  'NU': 'stock',
  'NVDA': 'stock',
  'NVO': 'stock',
  'PDD': 'stock',
  'PEP': 'stock',
  'PFE': 'stock',
  'PG': 'stock',
  'PYPL': 'stock',
  'SBUX': 'stock',
  'TMUS': 'stock',
  'UNH': 'stock',
  'V': 'stock',
  'WM': 'stock',
  
  // Crypto
  'BTC-USD': 'crypto',
  'ETH-USD': 'crypto'
};

// Modo de ejecución
const DRY_RUN = process.argv.includes('--dry-run');
const FIX = process.argv.includes('--fix');

async function migrateAssetPerformanceKeys() {
  console.log('='.repeat(100));
  console.log('MIGRACIÓN DE ASSET PERFORMANCE KEYS');
  console.log('='.repeat(100));
  console.log();
  console.log(`Modo: ${DRY_RUN ? 'DRY RUN (solo muestra cambios)' : FIX ? 'FIX (aplica cambios)' : 'ANÁLISIS'}`);
  console.log();

  if (!DRY_RUN && !FIX) {
    console.log('Uso:');
    console.log('  node migrateAssetPerformanceKeys.js --dry-run  # Ver cambios sin aplicar');
    console.log('  node migrateAssetPerformanceKeys.js --fix      # Aplicar cambios');
    console.log();
  }

  // Obtener todas las cuentas del usuario
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', USER_ID)
    .get();
  
  const accountIds = accountsSnapshot.docs.map(d => d.id);
  console.log(`📦 Cuentas: ${accountIds.length}`);
  console.log();

  // 1. Migrar nivel OVERALL
  console.log('='.repeat(50));
  console.log('NIVEL OVERALL');
  console.log('='.repeat(50));
  console.log();

  await migrateCollection(`portfolioPerformance/${USER_ID}/dates`);

  // 2. Migrar cada cuenta
  for (const accountId of accountIds) {
    const accountDoc = await db.collection('portfolioAccounts').doc(accountId).get();
    const accountName = accountDoc.data()?.name || accountId;
    
    console.log();
    console.log('='.repeat(50));
    console.log(`CUENTA: ${accountName}`);
    console.log('='.repeat(50));
    console.log();

    await migrateCollection(`portfolioPerformance/${USER_ID}/accounts/${accountId}/dates`);
  }

  console.log();
  console.log('='.repeat(100));
  console.log('MIGRACIÓN COMPLETADA');
  console.log('='.repeat(100));

  if (DRY_RUN) {
    console.log();
    console.log('⚠️ Este fue un DRY RUN. Para aplicar los cambios, ejecuta:');
    console.log('   node migrateAssetPerformanceKeys.js --fix');
  }

  process.exit(0);
}

async function migrateCollection(collectionPath) {
  const snapshot = await db.collection(collectionPath)
    .orderBy('date', 'asc')
    .get();

  console.log(`📂 Total documentos: ${snapshot.docs.length}`);

  let migratedCount = 0;
  let skippedCount = 0;
  let batch = db.batch();
  let batchCount = 0;
  const MAX_BATCH = 20; // Reducido porque cada documento tiene muchos datos

  for (const doc of snapshot.docs) {
    const data = doc.data();
    let needsMigration = false;
    const updates = {};

    for (const currency of CURRENCIES) {
      const currencyData = data[currency];
      if (!currencyData || !currencyData.assetPerformance) continue;

      const assetPerf = currencyData.assetPerformance;
      const newAssetPerf = {};
      let currencyNeedsMigration = false;

      for (const [key, value] of Object.entries(assetPerf)) {
        // Verificar si ya tiene el formato correcto
        if (key.includes('_')) {
          // Ya tiene el formato correcto, mantener
          newAssetPerf[key] = value;
        } else {
          // Necesita migración
          const assetType = ASSET_TYPE_MAP[key];
          if (assetType) {
            const newKey = `${key}_${assetType}`;
            newAssetPerf[newKey] = value;
            currencyNeedsMigration = true;
            
            if (DRY_RUN && migratedCount < 5) {
              console.log(`   ${data.date}: ${key} → ${newKey}`);
            }
          } else {
            // Si no encontramos el tipo, mantener el original y advertir
            console.log(`   ⚠️ ${data.date}: No se encontró tipo para "${key}", manteniendo original`);
            newAssetPerf[key] = value;
          }
        }
      }

      if (currencyNeedsMigration) {
        updates[`${currency}.assetPerformance`] = newAssetPerf;
        needsMigration = true;
      }
    }

    if (needsMigration) {
      migratedCount++;
      
      if (FIX) {
        // Hacer update individual porque los documentos son muy grandes para batch
        try {
          await doc.ref.update(updates);
          if (migratedCount % 10 === 0) {
            console.log(`   ✅ Migrados ${migratedCount} documentos...`);
          }
        } catch (err) {
          console.log(`   ❌ Error en ${data.date}: ${err.message}`);
        }
      }
    } else {
      skippedCount++;
    }
  }

  console.log(`📝 Documentos migrados: ${migratedCount}`);
  console.log(`✅ Documentos ya correctos: ${skippedCount}`);
}

migrateAssetPerformanceKeys().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
