/**
 * Script genérico para diagnosticar cualquier asset
 * 
 * Uso: node diagnoseAsset.js <ASSET_KEY>
 * Ejemplo: node diagnoseAsset.js BTC-USD_crypto
 *          node diagnoseAsset.js V_stock
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';

// Obtener asset key desde argumentos
const ASSET_KEY = process.argv[2];

if (!ASSET_KEY) {
  console.log('❌ Error: Debes proporcionar el asset key como argumento');
  console.log('');
  console.log('Uso: node diagnoseAsset.js <ASSET_KEY>');
  console.log('');
  console.log('Ejemplos:');
  console.log('  node diagnoseAsset.js BTC-USD_crypto');
  console.log('  node diagnoseAsset.js V_stock');
  console.log('  node diagnoseAsset.js SPYG_etf');
  process.exit(1);
}

async function diagnoseAsset() {
  console.log('='.repeat(80));
  console.log(`DIAGNÓSTICO DETALLADO DE ${ASSET_KEY}`);
  console.log('='.repeat(80));
  console.log();

  const [assetName, assetType] = ASSET_KEY.split('_');

  // 1. Obtener assets
  const assetsSnapshot = await db.collection('assets')
    .where('name', '==', assetName)
    .get();

  console.log('📦 Assets encontrados:');
  let totalUnits = 0;
  let totalInvestment = 0;
  
  for (const doc of assetsSnapshot.docs) {
    const asset = doc.data();
    console.log(`   ID: ${doc.id}`);
    console.log(`   - Account: ${asset.portfolioAccount}`);
    console.log(`   - Units: ${asset.units}`);
    console.log(`   - Acquisition Date: ${asset.acquisitionDate}`);
    console.log(`   - Unit Value: $${asset.unitValue}`);
    console.log(`   - isActive: ${asset.isActive}`);
    console.log();
    
    if (asset.isActive) {
      totalUnits += asset.units;
      totalInvestment += asset.unitValue * asset.units;
    }
  }

  console.log(`📊 Total units activas: ${totalUnits}`);
  console.log(`📊 Total inversión: $${totalInvestment.toFixed(2)}`);
  console.log();

  // 2. Obtener precio actual
  const priceDoc = await db.collection('currentPrices')
    .where('symbol', '==', assetName)
    .limit(1)
    .get();
  
  const currentPrice = priceDoc.empty ? 0 : priceDoc.docs[0].data().price;
  console.log(`💰 Precio actual: $${currentPrice}`);
  console.log(`📊 Valor actual calculado: $${(totalUnits * currentPrice).toFixed(2)}`);
  console.log(`📊 Valorización: $${((totalUnits * currentPrice) - totalInvestment).toFixed(2)}`);
  console.log(`📊 ROI: ${(((totalUnits * currentPrice) - totalInvestment) / totalInvestment * 100).toFixed(2)}%`);
  console.log();

  // 3. Mostrar datos históricos
  console.log('='.repeat(80));
  console.log('DATOS HISTÓRICOS');
  console.log('='.repeat(80));
  console.log();

  const overallSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  const now = DateTime.now().setZone('America/New_York');
  const oneMonthAgo = now.minus({ months: 1 }).toISODate();
  const threeMonthsAgo = now.minus({ months: 3 }).toISODate();
  const sixMonthsAgo = now.minus({ months: 6 }).toISODate();
  const startOfYear = now.startOf('year').toISODate();

  console.log(`📅 Fecha actual: ${now.toISODate()}`);
  console.log(`📅 1M ago: ${oneMonthAgo}`);
  console.log(`📅 3M ago: ${threeMonthsAgo}`);
  console.log(`📅 6M ago: ${sixMonthsAgo}`);
  console.log(`📅 YTD start: ${startOfYear}`);
  console.log();

  let currentFactor = 1;
  let oneMonthStartFactor = null, threeMonthStartFactor = null, sixMonthStartFactor = null;
  let ytdStartFactor = null;
  let firstDate = null;
  let problemCount = 0;

  console.log('Fecha          | Units    | TotalValue | AdjChange  | CashFlow   | Factor Acum | Nota');
  console.log('-'.repeat(100));

  let previousDoc = null;

  for (const doc of overallSnapshot.docs) {
    const data = doc.data();
    const assetData = data.USD?.assetPerformance?.[ASSET_KEY];
    
    if (!assetData) continue;

    if (!firstDate) firstDate = data.date;

    const adjChange = assetData.adjustedDailyChangePercentage || 0;
    const units = assetData.units || 0;
    const totalValue = assetData.totalValue || 0;
    const cashFlow = assetData.totalCashFlow || 0;

    // Detectar problemas
    let nota = '';
    
    if (previousDoc) {
      const prevData = previousDoc.data();
      const prevAssetData = prevData.USD?.assetPerformance?.[ASSET_KEY];
      if (prevAssetData) {
        const prevUnits = prevAssetData.units || 0;
        const unitsDiff = units - prevUnits;
        if (Math.abs(unitsDiff) > 0.00000001 && Math.abs(cashFlow) < 0.01 && prevUnits > 0) {
          nota = '⚠️ PROBLEMA: units cambiaron sin cashflow';
          problemCount++;
        }
      }
    }

    // Marcar inicio de períodos
    if (ytdStartFactor === null && data.date >= startOfYear) {
      ytdStartFactor = currentFactor;
      nota = nota || '← YTD Start';
    }
    if (sixMonthStartFactor === null && data.date >= sixMonthsAgo) {
      sixMonthStartFactor = currentFactor;
      nota = nota || '← 6M Start';
    }
    if (threeMonthStartFactor === null && data.date >= threeMonthsAgo) {
      threeMonthStartFactor = currentFactor;
      nota = nota || '← 3M Start';
    }
    if (oneMonthStartFactor === null && data.date >= oneMonthAgo) {
      oneMonthStartFactor = currentFactor;
      nota = nota || '← 1M Start';
    }

    // Detectar eventos importantes
    if (Math.abs(cashFlow) > 0.01 && !nota.includes('PROBLEMA')) {
      nota = nota || `CF: $${cashFlow.toFixed(2)}`;
    }
    if (Math.abs(adjChange) > 10 && !nota.includes('PROBLEMA')) {
      nota = nota ? nota + ' ⚠️ ALTO' : '⚠️ ALTO';
    }

    console.log(`${data.date} | ${units.toFixed(4).padStart(8)} | $${totalValue.toFixed(2).padStart(9)} | ${adjChange.toFixed(4).padStart(9)}% | $${cashFlow.toFixed(2).padStart(9)} | ${currentFactor.toFixed(6)} | ${nota}`);

    currentFactor = currentFactor * (1 + adjChange / 100);
    previousDoc = doc;
  }

  console.log();
  console.log('='.repeat(80));
  console.log('RESULTADOS CALCULADOS');
  console.log('='.repeat(80));
  console.log();

  console.log(`📂 Rango de datos: ${firstDate} a ${overallSnapshot.docs[overallSnapshot.docs.length - 1]?.data().date}`);
  console.log(`📊 Factor final: ${currentFactor.toFixed(6)}`);
  console.log(`⚠️ Problemas detectados: ${problemCount}`);
  console.log();

  const ytdReturn = ytdStartFactor ? (currentFactor / ytdStartFactor - 1) * 100 : 0;
  const sixMonthReturn = sixMonthStartFactor ? (currentFactor / sixMonthStartFactor - 1) * 100 : 0;
  const threeMonthReturn = threeMonthStartFactor ? (currentFactor / threeMonthStartFactor - 1) * 100 : 0;
  const oneMonthReturn = oneMonthStartFactor ? (currentFactor / oneMonthStartFactor - 1) * 100 : 0;
  const totalReturn = (currentFactor - 1) * 100;

  console.log(`📈 Total Return (desde ${firstDate}): ${totalReturn.toFixed(2)}%`);
  console.log(`📈 YTD Return: ${ytdReturn.toFixed(2)}%`);
  console.log(`📈 6M Return: ${sixMonthReturn.toFixed(2)}%`);
  console.log(`📈 3M Return: ${threeMonthReturn.toFixed(2)}%`);
  console.log(`📈 1M Return: ${oneMonthReturn.toFixed(2)}%`);

  if (problemCount > 0) {
    console.log();
    console.log('='.repeat(80));
    console.log('⚠️ SE DETECTARON PROBLEMAS');
    console.log('='.repeat(80));
    console.log();
    console.log('Ejecuta el siguiente comando para corregir:');
    console.log('  node diagnoseAndFixAllAssets.js');
  }

  process.exit(0);
}

diagnoseAsset().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
