/**
 * Script para comparar activos detalladamente por cuenta
 */

const admin = require('firebase-admin');
const path = require('path');

const keyPath = path.join(__dirname, '../../../key.json');
const serviceAccount = require(keyPath);

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

async function compareIBKR() {
  const accountId = 'BZHvXz4QT2yqqqlFP22X';
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  const date = '2026-01-09';

  console.log('=== COMPARACIÓN DETALLADA IBKR ===\n');

  // 1. Obtener portfolioPerformance
  const perf = await db.doc(`portfolioPerformance/${userId}/accounts/${accountId}/dates/${date}`).get();
  const perfData = perf.data()?.USD;
  
  console.log('Desde portfolioPerformance:');
  let perfTotal = 0;
  const perfAssets = {};
  Object.entries(perfData?.assetPerformance || {}).forEach(([key, val]) => {
    perfTotal += val.totalValue || 0;
    perfAssets[key] = { value: val.totalValue, units: val.units };
    console.log(`  ${key}: $${val.totalValue?.toFixed(2)} (${val.units} units)`);
  });
  console.log(`  TOTAL: $${perfTotal.toFixed(2)}\n`);

  // 2. Obtener activos y precios
  const assets = await db.collection('assets')
    .where('isActive', '==', true)
    .where('portfolioAccount', '==', accountId)
    .get();

  const prices = await db.collection('currentPrices').get();
  const priceMap = {};
  prices.docs.forEach(d => {
    const data = d.data();
    priceMap[data.symbol] = { price: data.price, currency: data.currency || 'USD' };
  });

  const currencies = await db.collection('currencies').where('isActive', '==', true).get();
  const rates = {};
  currencies.docs.forEach(d => {
    rates[d.data().code] = d.data().exchangeRate;
  });

  // 3. Agrupar activos
  const grouped = {};
  for (const doc of assets.docs) {
    const a = doc.data();
    const key = `${a.name}_${a.assetType}`;
    if (!grouped[key]) grouped[key] = { units: 0, value: 0 };
    grouped[key].units += a.units;
    
    const p = priceMap[a.name];
    if (p) {
      const priceCurrency = p.currency || 'USD';
      let priceUSD = p.price;
      if (priceCurrency !== 'USD' && rates[priceCurrency]) {
        priceUSD = p.price / rates[priceCurrency];
      }
      grouped[key].value += a.units * priceUSD;
    }
  }

  console.log('Desde activos (agrupados):');
  let assetsTotal = 0;
  Object.entries(grouped).forEach(([key, val]) => {
    assetsTotal += val.value;
    console.log(`  ${key}: $${val.value.toFixed(2)} (${val.units.toFixed(4)} units)`);
  });
  console.log(`  TOTAL: $${assetsTotal.toFixed(2)}\n`);

  // 4. Comparar diferencias
  console.log('DIFERENCIAS POR ACTIVO:');
  const allKeys = new Set([...Object.keys(perfAssets), ...Object.keys(grouped)]);
  
  for (const key of allKeys) {
    const perfVal = perfAssets[key]?.value || 0;
    const assetVal = grouped[key]?.value || 0;
    const diff = perfVal - assetVal;
    
    if (Math.abs(diff) > 0.01) {
      const perfUnits = perfAssets[key]?.units || 0;
      const assetUnits = grouped[key]?.units || 0;
      console.log(`  ${key}:`);
      console.log(`    portfolioPerf: $${perfVal.toFixed(2)} (${perfUnits} units)`);
      console.log(`    activos:       $${assetVal.toFixed(2)} (${assetUnits.toFixed(4)} units)`);
      console.log(`    DIFF: $${diff.toFixed(2)}`);
      
      // Calcular precio implícito
      if (perfUnits > 0) {
        const implicitPrice = perfVal / perfUnits;
        const currentPrice = priceMap[key.split('_')[0]]?.price || 0;
        console.log(`    Precio implícito en perf: $${implicitPrice.toFixed(2)}`);
        console.log(`    Precio actual: $${currentPrice.toFixed(2)}`);
        console.log(`    Diff precio: $${(implicitPrice - currentPrice).toFixed(2)} (${((implicitPrice/currentPrice - 1) * 100).toFixed(2)}%)`);
      }
      console.log('');
    }
  }

  console.log(`\nDIFERENCIA TOTAL: $${(perfTotal - assetsTotal).toFixed(2)}`);

  process.exit(0);
}

compareIBKR().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
