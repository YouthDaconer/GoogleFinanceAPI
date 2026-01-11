/**
 * Script de diagnóstico: Comparar totalROI vs YTD Return
 */

const admin = require('../../services/firebaseAdmin');
const db = admin.firestore();

async function check() {
  // Ver portfolioPerformance para el ROI
  const doc = await db.doc('portfolioPerformance/DDeR8P5hYgfuN8gcU4RsQfdTJqx2/dates/2025-12-17').get();
  const data = doc.data();
  const usd = data.USD;
  
  console.log('=== PORTFOLIOPERFORMANCE (assetPerformance) ===');
  console.log('totalROI:', usd.totalROI?.toFixed(2) + '%');
  console.log('Esto es ROI TOTAL desde compra, NO YTD');
  console.log('');
  
  // Ver el retorno YTD real (calculado por TWR)
  // Buscar el documento del 1 de enero
  const startOfYear = '2025-01-01';
  const startDoc = await db.collection('portfolioPerformance/DDeR8P5hYgfuN8gcU4RsQfdTJqx2/dates')
    .where('date', '>=', startOfYear)
    .orderBy('date', 'asc')
    .limit(1)
    .get();
  
  if (!startDoc.empty) {
    const startData = startDoc.docs[0].data();
    const startUsd = startData.USD;
    console.log('=== INICIO DEL AÑO ===');
    console.log('Fecha:', startDoc.docs[0].id);
    console.log('totalValue:', startUsd.totalValue?.toFixed(2));
    console.log('');
    
    // Calcular YTD manualmente
    const startValue = startUsd.totalValue || 0;
    const endValue = usd.totalValue || 0;
    const ytdReturn = startValue > 0 ? ((endValue / startValue) - 1) * 100 : 0;
    
    console.log('=== YTD CALCULADO (Simple) ===');
    console.log('Valor inicio año:', startValue.toFixed(2));
    console.log('Valor actual:', endValue.toFixed(2));
    console.log('YTD Return (simple):', ytdReturn.toFixed(2) + '%');
    console.log('');
    console.log('NOTA: El YTD real (24.64%) usa TWR que ajusta por flujos de caja');
  }
  
  process.exit(0);
}
check();
