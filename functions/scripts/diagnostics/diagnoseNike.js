/**
 * Diagnóstico detallado de NKE (Nike)
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const ASSET_KEY = 'NKE_stock';

async function diagnoseNike() {
  console.log('='.repeat(80));
  console.log('DIAGNÓSTICO DETALLADO DE NIKE (NKE)');
  console.log('='.repeat(80));
  console.log();

  // 1. Obtener assets de NKE
  const assetsSnapshot = await db.collection('assets')
    .where('name', '==', 'NKE')
    .get();

  console.log('📦 Assets de NKE encontrados:');
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
  const priceDoc = await db.collection('currentPrices').doc('NKE:NYSE').get();
  const currentPrice = priceDoc.exists ? priceDoc.data().price : 0;
  console.log(`💰 Precio actual: $${currentPrice}`);
  console.log(`📊 Valor actual calculado: $${(totalUnits * currentPrice).toFixed(2)}`);
  console.log(`📊 Valorización: $${((totalUnits * currentPrice) - totalInvestment).toFixed(2)}`);
  console.log(`📊 ROI: ${(((totalUnits * currentPrice) - totalInvestment) / totalInvestment * 100).toFixed(2)}%`);
  console.log();

  // 3. Mostrar todos los datos históricos de NKE
  console.log('='.repeat(80));
  console.log('DATOS HISTÓRICOS DE NKE');
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
  let oneMonthStartFactor = 1, threeMonthStartFactor = 1, sixMonthStartFactor = 1, ytdStartFactor = 1;
  let foundOneMonth = false, foundThreeMonth = false, foundSixMonth = false, foundYTD = false;
  let firstDate = null;

  console.log('Fecha          | Units    | TotalValue | AdjChange  | Factor Acum | Nota');
  console.log('-'.repeat(90));

  for (const doc of overallSnapshot.docs) {
    const data = doc.data();
    const assetData = data.USD?.assetPerformance?.[ASSET_KEY];
    
    if (!assetData) continue;

    if (!firstDate) firstDate = data.date;

    const adjChange = assetData.adjustedDailyChangePercentage || 0;
    const units = assetData.units || 0;
    const totalValue = assetData.totalValue || 0;
    const cashFlow = assetData.totalCashFlow || 0;

    // Marcar inicio de períodos
    let nota = '';
    if (!foundYTD && data.date >= startOfYear) {
      ytdStartFactor = currentFactor;
      foundYTD = true;
      nota = '← YTD Start';
    }
    if (!foundSixMonth && data.date >= sixMonthsAgo) {
      sixMonthStartFactor = currentFactor;
      foundSixMonth = true;
      nota = nota || '← 6M Start';
    }
    if (!foundThreeMonth && data.date >= threeMonthsAgo) {
      threeMonthStartFactor = currentFactor;
      foundThreeMonth = true;
      nota = nota || '← 3M Start';
    }
    if (!foundOneMonth && data.date >= oneMonthAgo) {
      oneMonthStartFactor = currentFactor;
      foundOneMonth = true;
      nota = nota || '← 1M Start';
    }

    // Detectar eventos importantes
    if (Math.abs(cashFlow) > 0.01) {
      nota = `CF: $${cashFlow.toFixed(2)}`;
    }
    if (Math.abs(adjChange) > 10) {
      nota = nota ? nota + ' ⚠️' : '⚠️ ALTO';
    }

    console.log(`${data.date} | ${units.toFixed(4).padStart(8)} | $${totalValue.toFixed(2).padStart(9)} | ${adjChange.toFixed(4).padStart(9)}% | ${currentFactor.toFixed(6)} | ${nota}`);

    currentFactor = currentFactor * (1 + adjChange / 100);
  }

  console.log();
  console.log('='.repeat(80));
  console.log('RESULTADOS CALCULADOS');
  console.log('='.repeat(80));
  console.log();

  console.log(`📊 Factor actual: ${currentFactor.toFixed(6)}`);
  console.log(`📊 Primera fecha de datos: ${firstDate}`);
  console.log();
  
  const ytdReturn = foundYTD ? (currentFactor / ytdStartFactor - 1) * 100 : 0;
  const sixMonthReturn = foundSixMonth ? (currentFactor / sixMonthStartFactor - 1) * 100 : 0;
  const threeMonthReturn = foundThreeMonth ? (currentFactor / threeMonthStartFactor - 1) * 100 : 0;
  const oneMonthReturn = foundOneMonth ? (currentFactor / oneMonthStartFactor - 1) * 100 : 0;

  console.log(`📈 YTD Return: ${ytdReturn.toFixed(2)}% (desde ${startOfYear}, factor inicio: ${ytdStartFactor.toFixed(6)})`);
  console.log(`📈 6M Return: ${sixMonthReturn.toFixed(2)}% (desde ${sixMonthsAgo}, factor inicio: ${sixMonthStartFactor.toFixed(6)})`);
  console.log(`📈 3M Return: ${threeMonthReturn.toFixed(2)}% (desde ${threeMonthsAgo}, factor inicio: ${threeMonthStartFactor.toFixed(6)})`);
  console.log(`📈 1M Return: ${oneMonthReturn.toFixed(2)}% (desde ${oneMonthAgo}, factor inicio: ${oneMonthStartFactor.toFixed(6)})`);
  console.log();

  // Comparar con lo que muestra la UI
  console.log('='.repeat(80));
  console.log('COMPARACIÓN CON UI');
  console.log('='.repeat(80));
  console.log();
  console.log('                    Calculado    |    UI');
  console.log('-'.repeat(50));
  console.log(`   YTD Return:     ${ytdReturn.toFixed(2).padStart(8)}%    |   43.19%`);
  console.log(`   6M Return:      ${sixMonthReturn.toFixed(2).padStart(8)}%    |   42.89%`);
  console.log(`   3M Return:      ${threeMonthReturn.toFixed(2).padStart(8)}%    |  -10.94%`);
  console.log(`   1M Return:      ${oneMonthReturn.toFixed(2).padStart(8)}%    |    6.05%`);

  process.exit(0);
}

diagnoseNike().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
