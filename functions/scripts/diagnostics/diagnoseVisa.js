/**
 * Diagnóstico de rendimientos históricos para VISA (V)
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const ASSET_KEY = 'V_stock';
const TICKER = 'V';

async function diagnoseVisa() {
  console.log('='.repeat(80));
  console.log('DIAGNÓSTICO DE RENDIMIENTOS HISTÓRICOS - VISA (V)');
  console.log('='.repeat(80));
  console.log();

  // 1. Buscar en qué cuenta está Visa
  const assetsSnapshot = await db.collection('assets')
    .where('name', '==', TICKER)
    .where('isActive', '==', true)
    .get();

  if (assetsSnapshot.empty) {
    console.log('❌ No se encontraron assets de VISA activos');
    process.exit(1);
  }

  console.log('📦 Assets de VISA encontrados:');
  const accountIds = new Set();
  let totalUnits = 0;
  let totalInvestment = 0;
  
  for (const doc of assetsSnapshot.docs) {
    const asset = doc.data();
    console.log(`   ID: ${doc.id}`);
    console.log(`   - Account: ${asset.portfolioAccount}`);
    console.log(`   - Units: ${asset.units}`);
    console.log(`   - Acquisition Date: ${asset.acquisitionDate}`);
    console.log(`   - Unit Value: $${asset.unitValue}`);
    console.log();
    
    accountIds.add(asset.portfolioAccount);
    totalUnits += asset.units;
    totalInvestment += asset.unitValue * asset.units;
  }

  console.log(`📊 Total units: ${totalUnits}`);
  console.log(`📊 Total inversión: $${totalInvestment.toFixed(2)}`);
  console.log();

  // 2. Obtener precio actual
  const priceDoc = await db.collection('currentPrices').doc(`${TICKER}:NYSE`).get();
  const currentPrice = priceDoc.exists ? priceDoc.data().price : 0;
  console.log(`💰 Precio actual: $${currentPrice}`);
  console.log(`📊 Valor actual: $${(totalUnits * currentPrice).toFixed(2)}`);
  console.log(`📊 Valorización: $${((totalUnits * currentPrice) - totalInvestment).toFixed(2)}`);
  console.log();

  // 3. Analizar datos a nivel overall
  console.log('='.repeat(80));
  console.log('DATOS A NIVEL OVERALL');
  console.log('='.repeat(80));
  console.log();

  const overallSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  let previousDoc = null;
  const problemDates = [];

  console.log('Fechas con datos de V_stock:');
  for (const doc of overallSnapshot.docs) {
    const data = doc.data();
    const assetData = data.USD?.assetPerformance?.[ASSET_KEY];
    
    if (assetData) {
      const currentUnits = assetData.units || 0;
      const previousUnits = previousDoc ? 
        (previousDoc.data().USD?.assetPerformance?.[ASSET_KEY]?.units || 0) : 0;
      const cashFlow = assetData.totalCashFlow || 0;
      const adjChange = assetData.adjustedDailyChangePercentage || 0;
      const unitsDiff = currentUnits - previousUnits;
      
      // Detectar problema
      const hasProblem = Math.abs(unitsDiff) > 0.00000001 && Math.abs(cashFlow) < 0.01 && previousUnits > 0;
      
      console.log(`   ${data.date}: units=${currentUnits.toFixed(4)}, adjChange=${adjChange.toFixed(4)}%, cashFlow=$${cashFlow.toFixed(2)}${hasProblem ? ' ⚠️ PROBLEMA' : ''}`);
      
      if (hasProblem) {
        problemDates.push({
          date: data.date,
          previousUnits,
          currentUnits,
          unitsDiff,
          adjChange,
          startValue: previousDoc?.data().USD?.assetPerformance?.[ASSET_KEY]?.totalValue || 0,
          endValue: assetData.totalValue || 0
        });
      }
    }
    
    if (data.USD?.assetPerformance?.[ASSET_KEY]) {
      previousDoc = doc;
    }
  }

  console.log();
  console.log('='.repeat(80));
  console.log(`PROBLEMAS DETECTADOS: ${problemDates.length}`);
  console.log('='.repeat(80));
  console.log();

  if (problemDates.length > 0) {
    for (const p of problemDates) {
      const impliedPrice = p.currentUnits > 0 ? p.endValue / p.currentUnits : 0;
      const implicitCashFlow = -p.unitsDiff * impliedPrice;
      const correctedChange = p.startValue > 0 ? 
        ((p.endValue - p.startValue + implicitCashFlow) / p.startValue) * 100 : 0;
      
      console.log(`📅 ${p.date}:`);
      console.log(`   Units: ${p.previousUnits.toFixed(4)} → ${p.currentUnits.toFixed(4)} (diff: ${p.unitsDiff.toFixed(4)})`);
      console.log(`   StartValue: $${p.startValue.toFixed(2)}, EndValue: $${p.endValue.toFixed(2)}`);
      console.log(`   AdjChange guardado: ${p.adjChange.toFixed(4)}%`);
      console.log(`   AdjChange corregido: ${correctedChange.toFixed(4)}%`);
      console.log();
    }
  } else {
    console.log('✅ No se detectaron problemas de cashflow implícito');
  }

  // 4. Calcular rendimientos actuales
  console.log('='.repeat(80));
  console.log('CÁLCULO DE RENDIMIENTOS ACTUALES');
  console.log('='.repeat(80));
  console.log();

  const now = DateTime.now().setZone('America/New_York');
  const oneMonthAgo = now.minus({ months: 1 }).toISODate();
  const threeMonthsAgo = now.minus({ months: 3 }).toISODate();
  const startOfYear = now.startOf('year').toISODate();

  console.log(`📅 Fecha actual: ${now.toISODate()}`);
  console.log(`📅 Hace 1 mes: ${oneMonthAgo}`);
  console.log(`📅 Hace 3 meses: ${threeMonthsAgo}`);
  console.log(`📅 Inicio del año: ${startOfYear}`);
  console.log();

  let currentFactor = 1;
  let oneMonthStartFactor = 1, threeMonthStartFactor = 1, ytdStartFactor = 1;
  let foundOneMonth = false, foundThreeMonth = false, foundYTD = false;

  for (const doc of overallSnapshot.docs) {
    const data = doc.data();
    const assetData = data.USD?.assetPerformance?.[ASSET_KEY];
    
    if (!assetData) continue;

    const adjChange = assetData.adjustedDailyChangePercentage || 0;

    if (!foundYTD && data.date >= startOfYear) {
      ytdStartFactor = currentFactor;
      foundYTD = true;
      console.log(`🟢 YTD Start: ${data.date}, Factor: ${ytdStartFactor.toFixed(6)}`);
    }
    if (!foundThreeMonth && data.date >= threeMonthsAgo) {
      threeMonthStartFactor = currentFactor;
      foundThreeMonth = true;
      console.log(`🟢 3M Start: ${data.date}, Factor: ${threeMonthStartFactor.toFixed(6)}`);
    }
    if (!foundOneMonth && data.date >= oneMonthAgo) {
      oneMonthStartFactor = currentFactor;
      foundOneMonth = true;
      console.log(`🟢 1M Start: ${data.date}, Factor: ${oneMonthStartFactor.toFixed(6)}`);
    }

    currentFactor = currentFactor * (1 + adjChange / 100);
  }

  console.log();
  console.log('📊 RESULTADOS:');
  console.log(`   Factor actual: ${currentFactor.toFixed(6)}`);
  console.log(`   YTD Return: ${((currentFactor / ytdStartFactor - 1) * 100).toFixed(2)}%`);
  console.log(`   3M Return: ${((currentFactor / threeMonthStartFactor - 1) * 100).toFixed(2)}%`);
  console.log(`   1M Return: ${((currentFactor / oneMonthStartFactor - 1) * 100).toFixed(2)}%`);

  process.exit(0);
}

diagnoseVisa().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
