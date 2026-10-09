/**
 * Script para RESTAURAR ECOPETROL.CL usando merge
 * Actualiza el objeto completo en lugar de campos individuales
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
}

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

const CONFIG = {
  USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  ASSET_KEY: 'ECOPETROL.CL_stock',
  REFERENCE_DATE: '2026-01-09',
  AFFECTED_DATES: ['2026-01-12', '2026-01-13', '2026-01-14', '2026-01-15', '2026-01-16'],
  ACCOUNT_ID: 'ggM52GimbLL7jwvegc9o',
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
};

// Valores correctos de ECOPETROL por fecha
const ECOPETROL_VALUES = {
  '2026-01-12': { totalValue: 10.95, totalROI: 2.44, unrealizedPnL: 0.26, totalInvestment: 10.69 },
  '2026-01-13': { totalValue: 11.12, totalROI: 4.02, unrealizedPnL: 0.43, totalInvestment: 10.69 },
  '2026-01-14': { totalValue: 11.53, totalROI: 7.88, unrealizedPnL: 0.84, totalInvestment: 10.69 },
  '2026-01-15': { totalValue: 11.79, totalROI: 10.30, unrealizedPnL: 1.10, totalInvestment: 10.69 },
  '2026-01-16': { totalValue: 12.30, totalROI: 15.01, unrealizedPnL: 1.60, totalInvestment: 10.69 },
};

async function fixDocument(docPath, date, correctDocInvestment) {
  const docRef = db.doc(docPath);
  const doc = await docRef.get();
  
  if (!doc.exists) {
    console.log(`❌ Documento no existe: ${docPath}`);
    return false;
  }
  
  const data = doc.data();
  const ecopetrolValues = ECOPETROL_VALUES[date];
  
  // Derivar tasas de cambio
  const rates = {};
  const usdValue = data.USD?.totalValue || 1;
  for (const currency of CONFIG.CURRENCIES) {
    const currValue = data[currency]?.totalValue || usdValue;
    rates[currency] = currValue / usdValue;
  }
  
  console.log(`📝 ${date}: Corrigiendo ${docPath}`);
  
  // Construir el documento actualizado completo
  const updatedData = { ...data };
  
  for (const currency of CONFIG.CURRENCIES) {
    const rate = rates[currency] || 1;
    
    // Asegurar que existe la estructura
    if (!updatedData[currency]) updatedData[currency] = {};
    if (!updatedData[currency].assetPerformance) updatedData[currency].assetPerformance = {};
    
    // Actualizar asset performance de ECOPETROL
    updatedData[currency].assetPerformance[CONFIG.ASSET_KEY] = {
      ...(data[currency]?.assetPerformance?.[CONFIG.ASSET_KEY] || {}),
      units: 20,
      totalValue: ecopetrolValues.totalValue * rate,
      totalInvestment: ecopetrolValues.totalInvestment * rate,
      totalROI: ecopetrolValues.totalROI,
      unrealizedProfitAndLoss: ecopetrolValues.unrealizedPnL * rate,
      doneProfitAndLoss: 0,
    };
    
    // Establecer totalInvestment del documento
    updatedData[currency].totalInvestment = correctDocInvestment * rate;
    
    // Recalcular ROI del documento
    const docTotalValue = data[currency]?.totalValue || 0;
    const newDocROI = correctDocInvestment > 0 
      ? ((docTotalValue / rate - correctDocInvestment) / correctDocInvestment) * 100
      : 0;
    updatedData[currency].totalROI = newDocROI;
  }
  
  // Aplicar con set + merge para mantener otros campos
  await docRef.set(updatedData, { merge: true });
  console.log(`   ✅ totalInvestment: $${correctDocInvestment.toFixed(2)}, ECOPETROL: $${ecopetrolValues.totalInvestment}`);
  
  return true;
}

async function main() {
  console.log('='.repeat(60));
  console.log('RESTAURACIÓN DE ECOPETROL.CL (usando merge)');
  console.log('='.repeat(60));
  console.log();
  
  // Obtener referencia del día correcto
  const refDoc = await db.doc(`portfolioPerformance/${CONFIG.USER_ID}/dates/${CONFIG.REFERENCE_DATE}`).get();
  const refInvestment = refDoc.data().USD?.totalInvestment || 7463.62;
  
  console.log(`Referencia ${CONFIG.REFERENCE_DATE}: totalInvestment = $${refInvestment.toFixed(2)}`);
  console.log();
  
  for (const date of CONFIG.AFFECTED_DATES) {
    console.log(`\n--- ${date} ---`);
    
    // Overall
    const overallPath = `portfolioPerformance/${CONFIG.USER_ID}/dates/${date}`;
    await fixDocument(overallPath, date, refInvestment);
    
    // Account
    const accountRefPath = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${CONFIG.ACCOUNT_ID}/dates/${CONFIG.REFERENCE_DATE}`;
    const accountRefDoc = await db.doc(accountRefPath).get();
    const accountRefInvestment = accountRefDoc.exists 
      ? accountRefDoc.data().USD?.totalInvestment || 10.80 
      : 10.80;
    
    const accountPath = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${CONFIG.ACCOUNT_ID}/dates/${date}`;
    const accountDoc = await db.doc(accountPath).get();
    if (accountDoc.exists) {
      await fixDocument(accountPath, date, accountRefInvestment);
    }
  }
  
  console.log('\n' + '='.repeat(60));
  console.log('✅ RESTAURACIÓN COMPLETADA');
  console.log('='.repeat(60));
  
  process.exit(0);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
