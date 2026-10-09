/**
 * Script para RESTAURAR los valores correctos de ECOPETROL.CL
 * Usa el día 2026-01-09 como referencia (último día correcto)
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
  REFERENCE_DATE: '2026-01-09', // Último día correcto
  AFFECTED_DATES: ['2026-01-12', '2026-01-13', '2026-01-14', '2026-01-15', '2026-01-16'],
  ACCOUNT_ID: 'ggM52GimbLL7jwvegc9o',
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
};

// Valores correctos de ECOPETROL por fecha (calculados previamente)
const ECOPETROL_VALUES = {
  '2026-01-12': { totalValue: 10.95, totalROI: 2.44, unrealizedPnL: 0.26, totalInvestment: 10.69 },
  '2026-01-13': { totalValue: 11.12, totalROI: 4.02, unrealizedPnL: 0.43, totalInvestment: 10.69 },
  '2026-01-14': { totalValue: 11.53, totalROI: 7.88, unrealizedPnL: 0.84, totalInvestment: 10.69 },
  '2026-01-15': { totalValue: 11.79, totalROI: 10.30, unrealizedPnL: 1.10, totalInvestment: 10.69 },
  '2026-01-16': { totalValue: 12.30, totalROI: 15.01, unrealizedPnL: 1.60, totalInvestment: 10.69 },
};

async function getBaseInvestment() {
  // Obtener el totalInvestment del doc de referencia (2026-01-09)
  // y el investment de ECOPETROL en ese día
  const refDoc = await db.doc(`portfolioPerformance/${CONFIG.USER_ID}/dates/${CONFIG.REFERENCE_DATE}`).get();
  
  if (!refDoc.exists) {
    throw new Error('Documento de referencia no existe');
  }
  
  const data = refDoc.data();
  const totalInvestment = data.USD?.totalInvestment || 0;
  const ecopetrolInvestment = data.USD?.assetPerformance?.[CONFIG.ASSET_KEY]?.totalInvestment || 0;
  
  // La inversión base (sin ECOPETROL) sería:
  const baseWithoutEcopetrol = totalInvestment - ecopetrolInvestment;
  
  console.log('Referencia 2026-01-09:');
  console.log(`  Total Investment: $${totalInvestment.toFixed(2)}`);
  console.log(`  ECOPETROL Investment: $${ecopetrolInvestment.toFixed(2)}`);
  console.log(`  Base sin ECOPETROL: $${baseWithoutEcopetrol.toFixed(2)}`);
  
  return { totalInvestment, baseWithoutEcopetrol };
}

async function fixDocument(docPath, date, correctDocInvestment) {
  const docRef = db.doc(docPath);
  const doc = await docRef.get();
  
  if (!doc.exists) {
    console.log(`❌ Documento no existe: ${docPath}`);
    return false;
  }
  
  const data = doc.data();
  const ecopetrolValues = ECOPETROL_VALUES[date];
  
  // Derivar tasas de cambio del documento existente
  const rates = {};
  const usdValue = data.USD?.totalValue || 1;
  for (const currency of CONFIG.CURRENCIES) {
    const currValue = data[currency]?.totalValue || usdValue;
    rates[currency] = currValue / usdValue;
  }
  
  console.log(`📝 ${date}: Corrigiendo ${docPath}`);
  
  // Construir updates
  const updates = {};
  
  for (const currency of CONFIG.CURRENCIES) {
    const rate = rates[currency] || 1;
    
    // Actualizar asset performance de ECOPETROL
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.units`] = 20;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.totalValue`] = ecopetrolValues.totalValue * rate;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.totalInvestment`] = ecopetrolValues.totalInvestment * rate;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.totalROI`] = ecopetrolValues.totalROI;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.unrealizedProfitAndLoss`] = ecopetrolValues.unrealizedPnL * rate;
    updates[`${currency}.assetPerformance.${CONFIG.ASSET_KEY}.doneProfitAndLoss`] = 0;
    
    // Establecer totalInvestment CORRECTO del documento
    updates[`${currency}.totalInvestment`] = correctDocInvestment * rate;
    
    // Recalcular ROI del documento
    const docTotalValue = data[currency]?.totalValue || 0;
    const newDocROI = correctDocInvestment > 0 
      ? ((docTotalValue / rate - correctDocInvestment) / correctDocInvestment) * 100
      : 0;
    updates[`${currency}.totalROI`] = newDocROI;
  }
  
  // Aplicar updates
  await docRef.update(updates);
  console.log(`   ✅ totalInvestment: $${correctDocInvestment.toFixed(2)}, ${Object.keys(updates).length} campos`);
  
  return true;
}

async function main() {
  console.log('='.repeat(60));
  console.log('RESTAURACIÓN DE ECOPETROL.CL');
  console.log('='.repeat(60));
  console.log();
  
  // Obtener el totalInvestment base del día de referencia
  const { totalInvestment: refInvestment } = await getBaseInvestment();
  
  console.log();
  console.log('Corrigiendo documentos afectados...');
  console.log();
  
  for (const date of CONFIG.AFFECTED_DATES) {
    console.log(`\n--- ${date} ---`);
    
    // El totalInvestment correcto es el mismo que el de referencia
    // (asumiendo que no hubo más compras/ventas entre 2026-01-09 y estas fechas)
    const correctDocInvestment = refInvestment;
    
    // Overall
    const overallPath = `portfolioPerformance/${CONFIG.USER_ID}/dates/${date}`;
    await fixDocument(overallPath, date, correctDocInvestment);
    
    // Account - necesitamos obtener su referencia también
    const accountRefPath = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${CONFIG.ACCOUNT_ID}/dates/${CONFIG.REFERENCE_DATE}`;
    const accountRefDoc = await db.doc(accountRefPath).get();
    const accountRefInvestment = accountRefDoc.exists 
      ? accountRefDoc.data().USD?.totalInvestment || 0 
      : 0;
    
    if (accountRefInvestment > 0) {
      const accountPath = `portfolioPerformance/${CONFIG.USER_ID}/accounts/${CONFIG.ACCOUNT_ID}/dates/${date}`;
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
