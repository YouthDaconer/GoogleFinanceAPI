/**
 * Quick diagnostic: Verify Harold Serrano's performance data
 */
const admin = require('firebase-admin');
const sa = require('../../../key.json');
if (!admin.apps.length) admin.initializeApp({ credential: admin.credential.cert(sa) });
const db = admin.firestore();
const USER = 'GlWtKSe53tMfcqNeFHeGf9OhowN2';

(async () => {
  // Último documento de performance
  const lastDoc = await db.collection(`portfolioPerformance/${USER}/dates`)
    .orderBy('date', 'desc').limit(1).get();

  // Primer día 2025
  const firstDay2025 = await db.collection(`portfolioPerformance/${USER}/dates`)
    .where('date', '>=', '2025-01-01').orderBy('date', 'asc').limit(1).get();

  // Último día 2024
  const lastDay2024 = await db.collection(`portfolioPerformance/${USER}/dates`)
    .where('date', '<=', '2024-12-31').orderBy('date', 'desc').limit(1).get();

  // Último día 2025
  const lastDay2025 = await db.collection(`portfolioPerformance/${USER}/dates`)
    .where('date', '<=', '2025-12-31').orderBy('date', 'desc').limit(1).get();

  // Consolidados
  const monthly = await db.collection(`portfolioPerformance/${USER}/consolidatedPeriods/monthly/periods`)
    .orderBy('periodKey', 'desc').limit(4).get();

  const yearly = await db.collection(`portfolioPerformance/${USER}/consolidatedPeriods/yearly/periods`)
    .orderBy('periodKey', 'desc').limit(2).get();

  // Algunos días clave para verificar continuidad
  const sampleDates = ['2025-01-02', '2025-03-03', '2025-06-05', '2025-06-06', '2025-09-01', '2025-12-30', '2026-01-02', '2026-03-28'];
  
  console.log('='.repeat(80));
  console.log('  VERIFICACIÓN DE RENDIMIENTOS - HAROLD SERRANO');
  console.log('='.repeat(80));

  console.log('\n📊 ÚLTIMO DOCUMENTO OVERALL');
  console.log('-'.repeat(40));
  if (!lastDoc.empty) {
    const d = lastDoc.docs[0].data();
    const u = d.USD;
    console.log(`Fecha: ${d.date}`);
    console.log(`USD totalValue: $${u?.totalValue?.toFixed(2)}`);
    console.log(`USD totalInvestment: $${u?.totalInvestment?.toFixed(2)}`);
    console.log(`USD unrealizedPnL: $${u?.unrealizedProfitAndLoss?.toFixed(2)}`);
    console.log(`USD totalROI: ${u?.totalROI?.toFixed(2)}%`);
    console.log(`USD dailyChange: ${u?.dailyChangePercentage?.toFixed(4)}%`);
    console.log(`USD adjDailyChange: ${u?.adjustedDailyChangePercentage?.toFixed(4)}%`);
    console.log(`Assets: ${JSON.stringify(Object.keys(u?.assetPerformance || {}))}`);
    
    // Check asset key format
    const assetKeys = Object.keys(u?.assetPerformance || {});
    const allHaveUnderscore = assetKeys.every(k => k.includes('_'));
    console.log(`Asset keys formato correcto: ${allHaveUnderscore ? '✅ SÍ' : '❌ NO'}`);
  }

  console.log('\n📊 PRIMER DÍA 2025');
  console.log('-'.repeat(40));
  if (!firstDay2025.empty) {
    const d = firstDay2025.docs[0].data();
    console.log(`Fecha: ${d.date}`);
    console.log(`USD totalValue: $${d.USD?.totalValue?.toFixed(2)}`);
    console.log(`USD totalInvestment: $${d.USD?.totalInvestment?.toFixed(2)}`);
    console.log(`USD adjDailyChange: ${d.USD?.adjustedDailyChangePercentage?.toFixed(4)}%`);
    const assetKeys = Object.keys(d.USD?.assetPerformance || {});
    console.log(`Assets: ${JSON.stringify(assetKeys)}`);
    console.log(`Asset keys formato correcto: ${assetKeys.every(k => k.includes('_')) ? '✅ SÍ' : '❌ NO'}`);
  }

  console.log('\n📊 ÚLTIMO DÍA 2024');
  console.log('-'.repeat(40));
  if (!lastDay2024.empty) {
    const d = lastDay2024.docs[0].data();
    console.log(`Fecha: ${d.date}, USD totalValue: $${d.USD?.totalValue?.toFixed(2)}`);
  } else {
    console.log('No hay datos de 2024');
  }

  console.log('\n📊 ÚLTIMO DÍA 2025');
  console.log('-'.repeat(40));
  if (!lastDay2025.empty) {
    const d = lastDay2025.docs[0].data();
    console.log(`Fecha: ${d.date}`);
    console.log(`USD totalValue: $${d.USD?.totalValue?.toFixed(2)}`);
    console.log(`USD totalROI: ${d.USD?.totalROI?.toFixed(2)}%`);
  }

  // YTD calculations
  console.log('\n📈 CÁLCULOS YTD');
  console.log('-'.repeat(40));
  if (!lastDoc.empty && !lastDay2025.empty) {
    const currentVal = lastDoc.docs[0].data().USD?.totalValue || 0;
    const endOf2025Val = lastDay2025.docs[0].data().USD?.totalValue || 0;
    if (endOf2025Val > 0) {
      const ytd2026 = ((currentVal - endOf2025Val) / endOf2025Val) * 100;
      console.log(`YTD 2026: ${ytd2026.toFixed(2)}% (${endOf2025Val.toFixed(2)} → ${currentVal.toFixed(2)})`);
    }
  }
  if (!lastDay2025.empty && !lastDay2024.empty) {
    const endOf2025 = lastDay2025.docs[0].data().USD?.totalValue || 0;
    const endOf2024 = lastDay2024.docs[0].data().USD?.totalValue || 0;
    if (endOf2024 > 0) {
      const ytd2025 = ((endOf2025 - endOf2024) / endOf2024) * 100;
      console.log(`Return 2025: ${ytd2025.toFixed(2)}% (${endOf2024.toFixed(2)} → ${endOf2025.toFixed(2)})`);
    }
  }

  // Días clave - verificar continuidad y keys
  console.log('\n📅 DÍAS CLAVE (continuidad y formato)');
  console.log('-'.repeat(80));
  for (const targetDate of sampleDates) {
    const snap = await db.collection(`portfolioPerformance/${USER}/dates`).doc(targetDate).get();
    if (snap.exists) {
      const d = snap.data();
      const u = d.USD;
      const assetKeys = Object.keys(u?.assetPerformance || {});
      const keysOk = assetKeys.every(k => k.includes('_'));
      console.log(`${targetDate}: value=$${u?.totalValue?.toFixed(2)}, inv=$${u?.totalInvestment?.toFixed(2)}, ROI=${u?.totalROI?.toFixed(2)}%, adjChange=${u?.adjustedDailyChangePercentage?.toFixed(4)}%, keys=${keysOk ? '✅' : '❌'} [${assetKeys.join(', ')}]`);
    } else {
      console.log(`${targetDate}: ⚠️ No existe`);
    }
  }

  // Consolidados mensuales
  console.log('\n📊 CONSOLIDADOS MENSUALES (últimos 4)');
  console.log('-'.repeat(60));
  monthly.docs.reverse().forEach(doc => {
    const d = doc.data();
    console.log(`${d.periodKey}: TWR=${(d.USD?.periodReturn || 0).toFixed(2)}%, personalReturn=${(d.USD?.personalReturn || 0).toFixed(2)}%, docs=${d.docsCount}`);
  });

  console.log('\n📊 CONSOLIDADOS ANUALES');
  console.log('-'.repeat(60));
  yearly.docs.reverse().forEach(doc => {
    const d = doc.data();
    console.log(`${d.periodKey}: TWR=${(d.USD?.periodReturn || 0).toFixed(2)}%, personalReturn=${(d.USD?.personalReturn || 0).toFixed(2)}%, startVal=$${(d.USD?.startTotalValue || 0).toFixed(2)}, endVal=$${(d.USD?.endTotalValue || 0).toFixed(2)}`);
  });

  // Comparar con VOO real
  console.log('\n📊 COMPARACIÓN CON VOO (benchmark)');
  console.log('-'.repeat(40));
  try {
    const resp = await fetch('https://api.portastock.net/v1/historical?symbol=VOO&range=2y&interval=1d', {
      headers: { 'x-service-token': '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10', 'origin': 'https://portastock.net', 'referer': 'https://portastock.net' }
    });
    const prices = await resp.json();
    const dates = Object.keys(prices).sort();
    const lastPrice = prices[dates[dates.length - 1]]?.close;
    const lastPriceDate = dates[dates.length - 1];
    
    // Fin 2025
    const dec25 = dates.filter(d => d <= '2025-12-31');
    const vooEnd2025 = dec25.length > 0 ? prices[dec25[dec25.length - 1]]?.close : null;
    
    // Fin 2024
    const dec24 = dates.filter(d => d <= '2024-12-31');
    const vooEnd2024 = dec24.length > 0 ? prices[dec24[dec24.length - 1]]?.close : null;

    console.log(`VOO último: $${lastPrice?.toFixed(2)} (${lastPriceDate})`);
    if (vooEnd2025) {
      console.log(`VOO fin 2025: $${vooEnd2025?.toFixed(2)} (${dec25[dec25.length - 1]})`);
      console.log(`VOO YTD 2026: ${(((lastPrice - vooEnd2025) / vooEnd2025) * 100).toFixed(2)}%`);
    }
    if (vooEnd2024) {
      console.log(`VOO fin 2024: $${vooEnd2024?.toFixed(2)} (${dec24[dec24.length - 1]})`);
      if (vooEnd2025) console.log(`VOO Return 2025: ${(((vooEnd2025 - vooEnd2024) / vooEnd2024) * 100).toFixed(2)}%`);
    }
  } catch(e) {
    console.log('Error fetching VOO:', e.message);
  }

  process.exit(0);
})();
