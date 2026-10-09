/**
 * Script SIMPLE para corregir ECOPETROL.CL
 * Actualiza directamente sin cálculos complejos
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
}

const db = admin.firestore();

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  ASSET_KEY: 'ECOPETROL.CL_stock',
  CORRECT_INVESTMENT_USD: 10.69, // 40100 COP / 3750.75
  AFFECTED_DATES: ['2026-01-12', '2026-01-13', '2026-01-14', '2026-01-15', '2026-01-16'],
  ACCOUNT_ID: 'ggM52GimbLL7jwvegc9o',
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
};

// Valores correctos por fecha (ya calculados)
const CORRECT_VALUES = {
  '2026-01-12': { totalValue: 10.95, totalROI: 2.44, unrealizedPnL: 0.26 },
  '2026-01-13': { totalValue: 11.12, totalROI: 4.02, unrealizedPnL: 0.43 },
  '2026-01-14': { totalValue: 11.53, totalROI: 7.88, unrealizedPnL: 0.84 },
  '2026-01-15': { totalValue: 11.79, totalROI: 10.30, unrealizedPnL: 1.10 },
  '2026-01-16': { totalValue: 12.30, totalROI: 15.01, unrealizedPnL: 1.60 },
};

async function fixDocument(docPath, date) {
  const docRef = db.doc(docPath);
  const doc = await docRef.get();
  
  if (!doc.exists) {
    console.log(`❌ Documento no existe: ${docPath}`);
    return false;
  }
  
  const data = doc.data();
  const correctValues = CORRECT_VALUES[date];
  
  // Derivar tasas de cambio del documento existente
  const rates = {};
  const usdValue = data.USD?.totalValue || 1;
  for (const currency of CONFIG.CURRENCIES) {
    const currValue = data[currency]?.totalValue || usdValue;
    rates[currency] = currValue / usdValue;
  }
  
  // Calcular el totalInvestment correcto del documento completo
  // Primero obtener el corrupto actual
  const corruptAssetInvestment = data.USD?.assetPerformance?.[CONFIG.ASSET_KEY]?.totalInvestment || 0;
  const currentDocInvestment = data.USD?.totalInvestment || 0;
  
  // Si el asset tiene inversión corrupta (> 1M), corregir
  const isCorrupt = corruptAssetInvestment > 1000000;
  
  if (!isCorrupt) {
    console.log(`✅ ${docPath} - Ya está correcto (investment: ${corruptAssetInvestment})`);
    return true;
  }
  
  // El totalInvestment correcto del doc = actual - corrupto + correcto
  const correctedDocInvestment = currentDocInvestment - corruptAssetInvestment + CONFIG.CORRECT_INVESTMENT_USD;
  
  console.log(`📝 ${date}: Corrigiendo ${docPath}`);
  console.log(`   Asset Investment: ${corruptAssetInvestment} -> ${CONFIG.CORRECT_INVESTMENT_USD}`);
  console.log(`   Doc Investment: ${currentDocInvestment} -> ${correctedDocInvestment}`);
  
  // Construir updates usando FieldPath para campos anidados
  const updates = {};
  
  for (const currency of CONFIG.CURRENCIES) {
    const rate = rates[currency] || 1;
    
    // Actualizar asset performance
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.units`] = 20;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.totalValue`] = correctValues.totalValue * rate;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.totalInvestment`] = CONFIG.CORRECT_INVESTMENT_USD * rate;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.totalROI`] = correctValues.totalROI;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.unrealizedProfitAndLoss`] = correctValues.unrealizedPnL * rate;
    
    // Actualizar totales del documento
    updates[`${currency}.totalInvestment`] = correctedDocInvestment * rate;
    
    // Recalcular ROI del documento
    const docTotalValue = data[currency]?.totalValue || 0;
    const newDocROI = correctedDocInvestment > 0 
      ? ((docTotalValue / rate - correctedDocInvestment) / correctedDocInvestment) * 100
      : 0;
    updates[`${currency}.totalROI`] = newDocROI;
  }
  
  // Aplicar updates
  await docRef.update(updates);
  console.log(`   ✅ Actualizado con ${Object.keys(updates).length} campos`);
  
  return true;
}

async function main() {
  console.log('='.repeat(60));
  console.log('CORRECCIÓN SIMPLE DE ECOPETROL.CL');
  console.log('='.repeat(60));
  
  for (const date of CONFIG.AFFECTED_DATES) {
    console.log(`\n--- ${date} ---`);
    
    // Overall
    const overallPath = `portfolioPerformance/${CONFIG.USER_ID}/dates/${date}`;
    await fixDocument(overallPath, date);
    
    // Account
    const accountPath = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${CONFIG.ACCOUNT_ID}/dates/${date}`;
    await fixDocument(accountPath, date);
  }
  
  console.log('\n' + '='.repeat(60));
  console.log('✅ CORRECCIÓN COMPLETADA');
  console.log('='.repeat(60));
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
