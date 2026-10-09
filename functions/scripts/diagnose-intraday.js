/**
 * Script de diagnóstico para comparar cálculos de PortfolioSummary vs Attribution
 */
const admin = require('firebase-admin');
if (!admin.apps.length) {
  admin.initializeApp({ projectId: 'portafolio-inversiones' });
}
const db = admin.firestore();

// Precios actuales (obtenidos del API)
const CURRENT_PRICES = {
  'VUAA.L': 131.94,
  'SPYG': 104.68,
  'BTC-USD': 89290.02,
  'MSFT': 454.52,
  'AMZN': 231.00,
  // Agregaremos más después
};

async function analyze() {
  const userId = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
  
  console.log('='.repeat(60));
  console.log('DIAGNÓSTICO DE CÁLCULO INTRADAY');
  console.log('='.repeat(60));
  console.log('Fecha actual: 2026-01-21');
  console.log('Último día con datos: 2026-01-16');
  console.log('Usuario:', userId);
  console.log('');
  
  // 1. Obtener cuentas del usuario
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  const accountIds = accountsSnap.docs.map(d => d.id);
  console.log('1. CUENTAS DEL USUARIO:', accountIds.length);
  accountsSnap.docs.forEach(d => console.log('   -', d.data().name, '(' + d.id + ')'));
  console.log('');
  
  // 2. Obtener assets activos
  let allAssets = [];
  for (const accountId of accountIds) {
    const assetsSnap = await db.collection('assets')
      .where('portfolioAccount', '==', accountId)
      .where('isActive', '==', true)
      .get();
    allAssets.push(...assetsSnap.docs.map(d => d.data()));
  }
  
  console.log('2. ASSETS ACTIVOS:', allAssets.length);
  
  // 3. Agrupar por símbolo y sumar unidades
  const bySymbol = {};
  for (const asset of allAssets) {
    const symbol = asset.name;
    if (!bySymbol[symbol]) {
      bySymbol[symbol] = { units: 0, investment: 0, currency: asset.currency };
    }
    bySymbol[symbol].units += asset.units || 0;
    bySymbol[symbol].investment += (asset.unitValue || 0) * (asset.units || 0);
  }
  
  console.log('\n3. RESUMEN POR SÍMBOLO:');
  console.log('-'.repeat(60));
  console.log('Símbolo        | Unidades   | Inversión    | Moneda');
  console.log('-'.repeat(60));
  
  const symbols = Object.keys(bySymbol).sort();
  for (const symbol of symbols) {
    const data = bySymbol[symbol];
    console.log(
      symbol.padEnd(14) + ' | ' +
      data.units.toFixed(4).padStart(10) + ' | ' +
      ('$' + data.investment.toFixed(2)).padStart(12) + ' | ' +
      data.currency
    );
  }
  console.log('-'.repeat(60));
  console.log('TOTAL SÍMBOLOS:', symbols.length);
  
  // 4. Obtener previousDayPerformance (16-ene)
  const perfDoc = await db.doc('portfolioPerformance/' + userId + '/dates/2026-01-16').get();
  const perfData = perfDoc.data();
  
  console.log('\n4. DATOS DEL 16 DE ENERO (portfolioPerformance):');
  console.log('   USD.totalValue:', perfData.USD.totalValue.toFixed(2));
  console.log('   USD.totalInvestment:', perfData.USD.totalInvestment.toFixed(2));
  console.log('   USD.adjustedDailyChangePercentage:', perfData.USD.adjustedDailyChangePercentage);
  console.log('   USD.factor:', perfData.USD.factor);
  
  // 5. Calcular valor actual esperado (con precios actuales que tenemos)
  console.log('\n5. CÁLCULO DEL VALOR ACTUAL (parcial - solo algunos símbolos):');
  let calculatedValue = 0;
  let symbolsWithPrice = 0;
  
  for (const symbol of symbols) {
    if (CURRENT_PRICES[symbol]) {
      const value = bySymbol[symbol].units * CURRENT_PRICES[symbol];
      calculatedValue += value;
      symbolsWithPrice++;
      console.log('   ' + symbol + ': ' + bySymbol[symbol].units.toFixed(4) + ' × $' + CURRENT_PRICES[symbol] + ' = $' + value.toFixed(2));
    }
  }
  
  console.log('\n   Valor parcial calculado: $' + calculatedValue.toFixed(2));
  console.log('   Símbolos con precio: ' + symbolsWithPrice + '/' + symbols.length);
  
  // 6. Calcular el factor esperado
  console.log('\n6. CÁLCULO DEL FACTOR INTRADAY:');
  console.log('   previousDayTotalValue: $' + perfData.USD.totalValue.toFixed(2));
  console.log('   (Necesitamos el valor actual completo para calcular el factor)');
  
  console.log('\n7. TWR HISTÓRICO (YTD hasta 16-ene):');
  // Calcular TWR desde el 1 de enero
  const ytdDocs = await db.collection('portfolioPerformance/' + userId + '/dates')
    .where('date', '>=', '2026-01-01')
    .orderBy('date', 'asc')
    .get();
  
  let twrFactor = 1.0;
  ytdDocs.docs.forEach(doc => {
    const data = doc.data();
    const dailyChange = data.USD?.adjustedDailyChangePercentage || 0;
    if (dailyChange !== 0) {
      twrFactor *= (1 + dailyChange / 100);
    }
  });
  
  const twrPercent = (twrFactor - 1) * 100;
  console.log('   Documentos YTD:', ytdDocs.size);
  console.log('   TWR calculado: ' + twrPercent.toFixed(4) + '%');
  console.log('   Factor TWR: ' + twrFactor.toFixed(6));
  
  process.exit(0);
}

analyze().catch(e => { console.error(e); process.exit(1); });
