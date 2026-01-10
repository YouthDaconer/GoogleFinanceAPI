/**
 * Script para comparar las fuentes de datos del dashboard
 * Identifica discrepancias entre portfolioPerformance y cálculo desde activos
 */

const admin = require('firebase-admin');
const path = require('path');

// Inicializar Firebase
const keyPath = path.join(__dirname, '../../../key.json');
const serviceAccount = require(keyPath);

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

async function compareDataSources() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const date = '2026-01-09';
  
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║     COMPARACIÓN DE FUENTES DE DATOS DEL DASHBOARD              ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  // 1. Obtener datos de portfolioPerformance (fuente del gráfico)
  const perfDoc = await db.doc(`portfolioPerformance/${userId}/dates/${date}`).get();
  const perfData = perfDoc.data()?.USD;
  
  console.log('1️⃣  portfolioPerformance (fuente del PortfolioGrowthChart):');
  console.log(`    totalValue: $${perfData?.totalValue?.toFixed(2) || 'N/A'}`);
  console.log(`    totalInvestment: $${perfData?.totalInvestment?.toFixed(2) || 'N/A'}\n`);

  // 2. Obtener cuentas del usuario
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  const userAccountIds = accountsSnap.docs.map(d => d.id);
  console.log('2️⃣  Cuentas del usuario:');
  accountsSnap.docs.forEach(d => console.log(`    - ${d.data().name} (${d.id})`));
  console.log('');

  // 3. Obtener activos activos del usuario
  const assetsSnap = await db.collection('assets')
    .where('isActive', '==', true)
    .where('portfolioAccount', 'in', userAccountIds)
    .get();
  
  console.log(`3️⃣  Activos activos: ${assetsSnap.size}\n`);

  // 4. Obtener tasas de cambio
  const currenciesSnap = await db.collection('currencies')
    .where('isActive', '==', true)
    .get();
  
  const rates = {};
  currenciesSnap.docs.forEach(d => {
    rates[d.data().code] = d.data().exchangeRate;
  });

  // 5. Obtener precios actuales
  const pricesSnap = await db.collection('currentPrices').get();
  const priceMap = {};
  pricesSnap.docs.forEach(d => {
    const data = d.data();
    priceMap[data.symbol] = { 
      price: data.price, 
      currency: data.currency || 'USD' 
    };
  });

  // 6. Calcular valor desde activos (como lo hace PortfolioSummary)
  let totalValueFromAssets = 0;
  let totalInvestmentFromAssets = 0;
  const missingPrices = [];
  const assetDetails = [];

  for (const doc of assetsSnap.docs) {
    const asset = doc.data();
    const priceData = priceMap[asset.name];
    
    if (!priceData) {
      missingPrices.push(asset.name);
      continue;
    }

    const priceCurrency = priceData.currency || 'USD';
    let priceInUSD = priceData.price;
    
    // Convertir precio a USD si no está en USD
    if (priceCurrency !== 'USD' && rates[priceCurrency]) {
      priceInUSD = priceData.price / rates[priceCurrency];
    }

    const valueUSD = asset.units * priceInUSD;
    
    // Calcular inversión (como lo hace el frontend)
    let investmentUSD;
    if (asset.currency === 'USD') {
      investmentUSD = asset.unitValue * asset.units;
    } else {
      // Para monedas no-USD, usar acquisitionDollarValue o tasa actual
      const rate = asset.acquisitionDollarValue || rates[asset.currency] || 1;
      investmentUSD = (asset.unitValue * asset.units) / rate;
    }

    totalValueFromAssets += valueUSD;
    totalInvestmentFromAssets += investmentUSD;

    assetDetails.push({
      name: asset.name,
      units: asset.units,
      valueUSD: valueUSD,
      investmentUSD: investmentUSD
    });
  }

  console.log('4️⃣  Cálculo desde activos (fuente del PortfolioSummary):');
  console.log(`    totalValue: $${totalValueFromAssets.toFixed(2)}`);
  console.log(`    totalInvestment: $${totalInvestmentFromAssets.toFixed(2)}\n`);

  if (missingPrices.length > 0) {
    console.log('⚠️  Activos sin precio actual:');
    missingPrices.forEach(name => console.log(`    - ${name}`));
    console.log('');
  }

  // 7. Calcular diferencia
  const valueDiff = (perfData?.totalValue || 0) - totalValueFromAssets;
  const investDiff = (perfData?.totalInvestment || 0) - totalInvestmentFromAssets;

  console.log('5️⃣  DIFERENCIAS:');
  console.log(`    Value: portfolioPerformance - activos = $${valueDiff.toFixed(2)}`);
  console.log(`    Investment: portfolioPerformance - activos = $${investDiff.toFixed(2)}\n`);

  // 8. Comparar por cuenta
  console.log('6️⃣  Comparación por cuenta:');
  console.log('    ┌──────────────────┬───────────────────┬───────────────────┬──────────────┐');
  console.log('    │ Cuenta           │ portfolioPerf     │ Desde Activos     │ Diferencia   │');
  console.log('    ├──────────────────┼───────────────────┼───────────────────┼──────────────┤');

  for (const accDoc of accountsSnap.docs) {
    const accId = accDoc.id;
    const accName = accDoc.data().name;

    // Valor en portfolioPerformance
    const accPerfDoc = await db.doc(`portfolioPerformance/${userId}/accounts/${accId}/dates/${date}`).get();
    const accPerfValue = accPerfDoc.data()?.USD?.totalValue || 0;

    // Valor desde activos
    let accAssetsValue = 0;
    for (const detail of assetDetails) {
      const assetDoc = assetsSnap.docs.find(d => d.data().name === detail.name && d.data().portfolioAccount === accId);
      if (assetDoc) {
        accAssetsValue += detail.valueUSD;
      }
    }

    // Calcular correctamente por cuenta
    const accountAssets = assetsSnap.docs.filter(d => d.data().portfolioAccount === accId);
    accAssetsValue = 0;
    for (const assetDoc of accountAssets) {
      const asset = assetDoc.data();
      const priceData = priceMap[asset.name];
      if (priceData) {
        const priceCurrency = priceData.currency || 'USD';
        let priceInUSD = priceData.price;
        if (priceCurrency !== 'USD' && rates[priceCurrency]) {
          priceInUSD = priceData.price / rates[priceCurrency];
        }
        accAssetsValue += asset.units * priceInUSD;
      }
    }

    const diff = accPerfValue - accAssetsValue;
    const diffStr = diff >= 0 ? `+$${diff.toFixed(2)}` : `-$${Math.abs(diff).toFixed(2)}`;

    console.log(`    │ ${accName.padEnd(16)} │ $${accPerfValue.toFixed(2).padStart(15)} │ $${accAssetsValue.toFixed(2).padStart(15)} │ ${diffStr.padStart(12)} │`);
  }
  console.log('    └──────────────────┴───────────────────┴───────────────────┴──────────────┘\n');

  // 9. Identificar causa probable
  console.log('7️⃣  ANÁLISIS:');
  if (Math.abs(valueDiff) > 1) {
    console.log('    ❌ Hay una discrepancia significativa entre las fuentes.');
    console.log('    Posibles causas:');
    console.log('    1. portfolioPerformance se calculó con precios diferentes (snapshots históricos)');
    console.log('    2. Los precios actuales han cambiado desde que se guardó portfolioPerformance');
    console.log('    3. Hay activos que fueron desactivados después de calcular portfolioPerformance');
    console.log('    4. El backfill usó tipos de cambio diferentes a los actuales');
  } else {
    console.log('    ✅ Las fuentes están sincronizadas (diferencia < $1)');
  }

  process.exit(0);
}

compareDataSources().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
