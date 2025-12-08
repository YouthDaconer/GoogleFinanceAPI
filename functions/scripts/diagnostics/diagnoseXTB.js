/**
 * Diagnóstico detallado de la cuenta XTB
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const ACCOUNT_ID = 'Z3gnboYgRlTvSZNGSu8j'; // XTB

async function diagnoseXTB() {
  console.log('='.repeat(100));
  console.log('DIAGNÓSTICO DETALLADO DE CUENTA XTB');
  console.log('='.repeat(100));
  console.log();

  // 1. Obtener datos de la cuenta
  const accountDoc = await db.collection('portfolioAccounts').doc(ACCOUNT_ID).get();
  console.log(`📦 Cuenta: ${accountDoc.data().name}`);
  console.log();

  // 2. Obtener assets de esta cuenta
  const assetsSnapshot = await db.collection('assets')
    .where('portfolioAccount', '==', ACCOUNT_ID)
    .where('isActive', '==', true)
    .get();

  // Obtener precios actuales
  const pricesSnapshot = await db.collection('currentPrices').get();
  const prices = {};
  pricesSnapshot.docs.forEach(d => {
    const data = d.data();
    prices[data.symbol] = data.price;
  });

  console.log('📊 Assets activos en XTB:');
  let totalInvestment = 0;
  let totalCurrentValue = 0;
  
  for (const doc of assetsSnapshot.docs) {
    const asset = doc.data();
    const investment = asset.unitValue * asset.units;
    const currentPrice = prices[asset.name] || 0;
    const currentValue = currentPrice * asset.units;
    
    totalInvestment += investment;
    totalCurrentValue += currentValue;
    
    console.log(`   ${asset.name}: ${asset.units} units @ $${asset.unitValue} = $${investment.toFixed(2)} → $${currentValue.toFixed(2)} (${((currentValue/investment - 1) * 100).toFixed(2)}%)`);
  }

  console.log();
  console.log(`📊 Total Inversión: $${totalInvestment.toFixed(2)}`);
  console.log(`📊 Total Valor Actual: $${totalCurrentValue.toFixed(2)}`);
  console.log(`📊 Valorización: $${(totalCurrentValue - totalInvestment).toFixed(2)}`);
  console.log(`📊 ROI Simple: ${((totalCurrentValue / totalInvestment - 1) * 100).toFixed(2)}%`);
  console.log();

  // 3. Obtener datos históricos de la cuenta XTB
  console.log('='.repeat(100));
  console.log('DATOS HISTÓRICOS DE LA CUENTA XTB');
  console.log('='.repeat(100));
  console.log();

  const accountPerformanceSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/accounts/${ACCOUNT_ID}/dates`)
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
  let lastDate = null;

  // Detectar problemas
  let previousDoc = null;
  const problems = [];

  console.log('Fecha          | TotalValue  | AdjChange   | TotalCashFlow | Factor    | Nota');
  console.log('-'.repeat(100));

  for (const doc of accountPerformanceSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    
    if (!currencyData) continue;

    if (!firstDate) firstDate = data.date;
    lastDate = data.date;

    const adjChange = currencyData.adjustedDailyChangePercentage || 0;
    const totalValue = currencyData.totalValue || 0;
    const totalCashFlow = currencyData.totalCashFlow || 0;

    let nota = '';

    // Marcar inicio de períodos
    if (ytdStartFactor === null && data.date >= startOfYear) {
      ytdStartFactor = currentFactor;
      nota = '← YTD';
    }
    if (sixMonthStartFactor === null && data.date >= sixMonthsAgo) {
      sixMonthStartFactor = currentFactor;
      nota = nota || '← 6M';
    }
    if (threeMonthStartFactor === null && data.date >= threeMonthsAgo) {
      threeMonthStartFactor = currentFactor;
      nota = nota || '← 3M';
    }
    if (oneMonthStartFactor === null && data.date >= oneMonthAgo) {
      oneMonthStartFactor = currentFactor;
      nota = nota || '← 1M';
    }

    // Detectar cambios significativos
    if (Math.abs(adjChange) > 5) {
      nota = nota ? nota + ' ⚠️' : '⚠️ ALTO';
    }

    // Solo mostrar algunos días clave
    const shouldShow = nota !== '' || 
                       Math.abs(adjChange) > 3 || 
                       data.date >= oneMonthAgo ||
                       data.date === firstDate;

    if (shouldShow) {
      console.log(`${data.date} | $${totalValue.toFixed(2).padStart(10)} | ${adjChange.toFixed(4).padStart(10)}% | $${totalCashFlow.toFixed(2).padStart(12)} | ${currentFactor.toFixed(6)} | ${nota}`);
    }

    currentFactor = currentFactor * (1 + adjChange / 100);
    previousDoc = doc;
  }

  console.log();
  console.log('='.repeat(100));
  console.log('RESULTADOS CALCULADOS');
  console.log('='.repeat(100));
  console.log();

  console.log(`📂 Rango de datos: ${firstDate} a ${lastDate}`);
  console.log(`📊 Factor final: ${currentFactor.toFixed(6)}`);
  console.log();

  const ytdReturn = ytdStartFactor ? (currentFactor / ytdStartFactor - 1) * 100 : 0;
  const sixMonthReturn = sixMonthStartFactor ? (currentFactor / sixMonthStartFactor - 1) * 100 : 0;
  const threeMonthReturn = threeMonthStartFactor ? (currentFactor / threeMonthStartFactor - 1) * 100 : 0;
  const oneMonthReturn = oneMonthStartFactor ? (currentFactor / oneMonthStartFactor - 1) * 100 : 0;

  console.log('='.repeat(100));
  console.log('COMPARACIÓN CON UI');
  console.log('='.repeat(100));
  console.log();
  console.log('                    Calculado    |    UI       |  Diferencia');
  console.log('-'.repeat(70));
  console.log(`   YTD Return:     ${ytdReturn.toFixed(2).padStart(8)}%    |   -1.26%    |  ${(ytdReturn - (-1.26)).toFixed(2)}pp`);
  console.log(`   6M Return:      ${sixMonthReturn.toFixed(2).padStart(8)}%    |   22.92%    |  ${(sixMonthReturn - 22.92).toFixed(2)}pp`);
  console.log(`   3M Return:      ${threeMonthReturn.toFixed(2).padStart(8)}%    |    8.31%    |  ${(threeMonthReturn - 8.31).toFixed(2)}pp`);
  console.log(`   1M Return:      ${oneMonthReturn.toFixed(2).padStart(8)}%    |    1.69%    |  ${(oneMonthReturn - 1.69).toFixed(2)}pp`);
  console.log();

  // Verificar consistencia con valorización
  console.log('='.repeat(100));
  console.log('VERIFICACIÓN DE CONSISTENCIA');
  console.log('='.repeat(100));
  console.log();

  console.log('📊 Datos de UI:');
  console.log('   Inversión Total: $4,259.44');
  console.log('   Valor Actual: $4,564.98');
  console.log('   Valorización: $305.54');
  console.log('   ROI Simple: ' + ((4564.98 / 4259.44 - 1) * 100).toFixed(2) + '%');
  console.log();
  
  console.log('📊 Datos calculados desde assets:');
  console.log(`   Inversión Total: $${totalInvestment.toFixed(2)}`);
  console.log(`   Valor Actual: $${totalCurrentValue.toFixed(2)}`);
  console.log(`   Valorización: $${(totalCurrentValue - totalInvestment).toFixed(2)}`);
  console.log(`   ROI Simple: ${((totalCurrentValue / totalInvestment - 1) * 100).toFixed(2)}%`);

  process.exit(0);
}

diagnoseXTB().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
