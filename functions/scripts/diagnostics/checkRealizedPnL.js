/**
 * Script de diagnóstico: Verificar P&L Realizada en assetPerformance
 */

const admin = require('../../services/firebaseAdmin');
const db = admin.firestore();

async function check() {
  const doc = await db.doc('portfolioPerformance/DDeR8P5hYgfuN8gcU4RsQfdTJqx2/dates/2025-12-17').get();
  const data = doc.data();
  const usd = data.USD;
  
  console.log('=== PORTFOLIO LEVEL ===');
  console.log('totalValue:', usd.totalValue);
  console.log('totalInvestment:', usd.totalInvestment);
  console.log('unrealizedPnL:', usd.unrealizedProfitAndLoss);
  console.log('doneProfitAndLoss:', usd.doneProfitAndLoss);
  console.log('totalROI:', usd.totalROI);
  console.log('');
  
  // Ver un activo específico que tenga ventas realizadas
  const assets = usd.assetPerformance || {};
  console.log('=== ASSETS CON P&L REALIZADA ===');
  
  let countWithRealized = 0;
  let countWithoutRealized = 0;
  
  // Buscar activos con doneProfitAndLoss > 0
  for (const [key, asset] of Object.entries(assets)) {
    if (asset.doneProfitAndLoss && asset.doneProfitAndLoss !== 0) {
      countWithRealized++;
      console.log('');
      console.log('Asset:', key);
      console.log('  totalValue:', asset.totalValue?.toFixed(2));
      console.log('  totalInvestment:', asset.totalInvestment?.toFixed(2));
      console.log('  unrealizedPnL:', asset.unrealizedProfitAndLoss?.toFixed(2));
      console.log('  doneProfitAndLoss:', asset.doneProfitAndLoss?.toFixed(2));
      console.log('  totalROI:', asset.totalROI?.toFixed(2) + '%');
    } else {
      countWithoutRealized++;
    }
  }
  
  console.log('');
  console.log('=== RESUMEN ===');
  console.log('Assets con P&L realizada:', countWithRealized);
  console.log('Assets sin P&L realizada:', countWithoutRealized);
  console.log('Total assets:', Object.keys(assets).length);
  
  process.exit(0);
}
check();
