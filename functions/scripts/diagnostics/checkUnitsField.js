/**
 * Verificar si los documentos tienen el campo 'units' en assetPerformance
 */

const admin = require('firebase-admin');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const ACCOUNT_ID = 'Z3gnboYgRlTvSZNGSu8j'; // XTB

async function checkUnitsField() {
  console.log('='.repeat(100));
  console.log('VERIFICACIÓN DE CAMPO "units" EN assetPerformance');
  console.log('='.repeat(100));
  console.log();

  const accountSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/accounts/${ACCOUNT_ID}/dates`)
    .orderBy('date', 'asc')
    .limit(20) // Primeros 20 documentos
    .get();

  console.log('Fecha          | Assets | Con Units | Sin Units | Ejemplo');
  console.log('-'.repeat(100));

  for (const doc of accountSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    
    if (!currencyData || !currencyData.assetPerformance) continue;

    const assetPerf = currencyData.assetPerformance;
    const assetKeys = Object.keys(assetPerf);
    
    let withUnits = 0;
    let withoutUnits = 0;
    let exampleWithout = '';
    
    for (const key of assetKeys) {
      if (assetPerf[key].units !== undefined) {
        withUnits++;
      } else {
        withoutUnits++;
        if (!exampleWithout) exampleWithout = key;
      }
    }

    console.log(`${data.date} | ${assetKeys.length.toString().padStart(6)} | ${withUnits.toString().padStart(9)} | ${withoutUnits.toString().padStart(9)} | ${exampleWithout}`);
  }

  console.log();
  console.log('='.repeat(100));
  console.log('MUESTRA DE UN DOCUMENTO ANTIGUO');
  console.log('='.repeat(100));
  console.log();

  const oldDoc = accountSnapshot.docs[0];
  if (oldDoc) {
    const data = oldDoc.data();
    console.log(`📄 Documento: ${data.date}`);
    const assetPerf = data.USD?.assetPerformance || {};
    const firstAsset = Object.keys(assetPerf)[0];
    if (firstAsset) {
      console.log(`   Asset: ${firstAsset}`);
      console.log(`   Campos:`, Object.keys(assetPerf[firstAsset]));
      console.log(`   Valores:`, JSON.stringify(assetPerf[firstAsset], null, 2));
    }
  }

  process.exit(0);
}

checkUnitsField().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
