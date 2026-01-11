/**
 * Verificación de consistencia entre rendimientos históricos y P&L
 * 
 * Compara:
 * - P&L No Realizada = Valor Actual - Inversión Total
 * - Rendimientos históricos vs rendimiento simple (valorización/inversión)
 */

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const serviceAccount = require('../../key.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';

async function verifyReturnsVsPnL() {
  console.log('='.repeat(100));
  console.log('VERIFICACIÓN DE RENDIMIENTOS VS P&L');
  console.log('='.repeat(100));
  console.log();

  // 1. Obtener datos actuales de assets
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', USER_ID)
    .get();

  const accountIds = accountsSnapshot.docs.map(d => d.id);

  const assetsSnapshot = await db.collection('assets')
    .where('isActive', '==', true)
    .get();

  let totalInvestment = 0;
  let totalCurrentValue = 0;
  const assetsByAccount = {};

  // Obtener precios actuales
  const pricesSnapshot = await db.collection('currentPrices').get();
  const prices = {};
  pricesSnapshot.docs.forEach(d => {
    const data = d.data();
    prices[data.symbol] = data.price;
  });

  for (const doc of assetsSnapshot.docs) {
    const asset = doc.data();
    
    // Verificar que pertenece al usuario
    if (!accountIds.includes(asset.portfolioAccount)) continue;
    
    const investment = asset.unitValue * asset.units;
    const currentPrice = prices[asset.name] || 0;
    const currentValue = currentPrice * asset.units;
    
    totalInvestment += investment;
    totalCurrentValue += currentValue;
    
    if (!assetsByAccount[asset.portfolioAccount]) {
      assetsByAccount[asset.portfolioAccount] = [];
    }
    assetsByAccount[asset.portfolioAccount].push({
      name: asset.name,
      units: asset.units,
      investment,
      currentValue,
      pnl: currentValue - investment
    });
  }

  const unrealizedPnL = totalCurrentValue - totalInvestment;
  const simpleROI = (unrealizedPnL / totalInvestment) * 100;

  console.log('📊 DATOS ACTUALES (calculados desde assets + precios):');
  console.log(`   Inversión Total: $${totalInvestment.toFixed(2)}`);
  console.log(`   Valor Actual: $${totalCurrentValue.toFixed(2)}`);
  console.log(`   P&L No Realizada: $${unrealizedPnL.toFixed(2)}`);
  console.log(`   ROI Simple: ${simpleROI.toFixed(2)}%`);
  console.log();

  console.log('📊 DATOS DE LA UI (según imagen):');
  console.log(`   Inversión Total: $7,144.48`);
  console.log(`   Valor Actual: $7,932.45`);
  console.log(`   P&L No Realizada: $787.97`);
  console.log(`   P&L Realizada: $570.52`);
  console.log(`   P&L Total: $${(787.97 + 570.52).toFixed(2)}`);
  console.log();

  // 2. Calcular rendimientos históricos usando el método de factor acumulativo
  console.log('='.repeat(100));
  console.log('CÁLCULO DE RENDIMIENTOS HISTÓRICOS');
  console.log('='.repeat(100));
  console.log();

  const overallSnapshot = await db.collection(`portfolioPerformance/${USER_ID}/dates`)
    .orderBy('date', 'asc')
    .get();

  const now = DateTime.now().setZone('America/New_York');
  const oneMonthAgo = now.minus({ months: 1 }).toISODate();
  const threeMonthsAgo = now.minus({ months: 3 }).toISODate();
  const sixMonthsAgo = now.minus({ months: 6 }).toISODate();
  const oneYearAgo = now.minus({ years: 1 }).toISODate();
  const startOfYear = now.startOf('year').toISODate();

  console.log(`📅 Fecha actual: ${now.toISODate()}`);
  console.log(`📅 1M ago: ${oneMonthAgo}`);
  console.log(`📅 3M ago: ${threeMonthsAgo}`);
  console.log(`📅 6M ago: ${sixMonthsAgo}`);
  console.log(`📅 YTD start: ${startOfYear}`);
  console.log();

  let currentFactor = 1;
  let oneMonthStartFactor = null, threeMonthStartFactor = null, sixMonthStartFactor = null;
  let ytdStartFactor = null, oneYearStartFactor = null;
  let firstDate = null, lastDate = null;

  // También calcular suma de rendimientos para verificar
  let sumAdjustedChanges = 0;
  let countDays = 0;

  for (const doc of overallSnapshot.docs) {
    const data = doc.data();
    const currencyData = data.USD;
    
    if (!currencyData) continue;

    if (!firstDate) firstDate = data.date;
    lastDate = data.date;

    const adjChange = currencyData.adjustedDailyChangePercentage || 0;
    sumAdjustedChanges += adjChange;
    countDays++;

    // Marcar inicio de períodos
    if (ytdStartFactor === null && data.date >= startOfYear) {
      ytdStartFactor = currentFactor;
      console.log(`🟢 YTD Start: ${data.date}, Factor: ${currentFactor.toFixed(6)}`);
    }
    if (oneYearStartFactor === null && data.date >= oneYearAgo) {
      oneYearStartFactor = currentFactor;
    }
    if (sixMonthStartFactor === null && data.date >= sixMonthsAgo) {
      sixMonthStartFactor = currentFactor;
      console.log(`🟢 6M Start: ${data.date}, Factor: ${currentFactor.toFixed(6)}`);
    }
    if (threeMonthStartFactor === null && data.date >= threeMonthsAgo) {
      threeMonthStartFactor = currentFactor;
      console.log(`🟢 3M Start: ${data.date}, Factor: ${currentFactor.toFixed(6)}`);
    }
    if (oneMonthStartFactor === null && data.date >= oneMonthAgo) {
      oneMonthStartFactor = currentFactor;
      console.log(`🟢 1M Start: ${data.date}, Factor: ${currentFactor.toFixed(6)}`);
    }

    currentFactor = currentFactor * (1 + adjChange / 100);
  }

  console.log();
  console.log(`📂 Rango de datos: ${firstDate} a ${lastDate} (${countDays} días)`);
  console.log(`📊 Factor final: ${currentFactor.toFixed(6)}`);
  console.log();

  // Calcular rendimientos
  const ytdReturn = ytdStartFactor ? (currentFactor / ytdStartFactor - 1) * 100 : 0;
  const sixMonthReturn = sixMonthStartFactor ? (currentFactor / sixMonthStartFactor - 1) * 100 : 0;
  const threeMonthReturn = threeMonthStartFactor ? (currentFactor / threeMonthStartFactor - 1) * 100 : 0;
  const oneMonthReturn = oneMonthStartFactor ? (currentFactor / oneMonthStartFactor - 1) * 100 : 0;
  const totalReturn = (currentFactor - 1) * 100;

  console.log('='.repeat(100));
  console.log('COMPARACIÓN DE RENDIMIENTOS');
  console.log('='.repeat(100));
  console.log();
  console.log('                    Calculado    |    UI       |  Diferencia');
  console.log('-'.repeat(70));
  console.log(`   YTD Return:     ${ytdReturn.toFixed(2).padStart(8)}%    |   11.34%    |  ${(ytdReturn - 11.34).toFixed(2)}pp`);
  console.log(`   6M Return:      ${sixMonthReturn.toFixed(2).padStart(8)}%    |   20.08%    |  ${(sixMonthReturn - 20.08).toFixed(2)}pp`);
  console.log(`   3M Return:      ${threeMonthReturn.toFixed(2).padStart(8)}%    |    7.45%    |  ${(threeMonthReturn - 7.45).toFixed(2)}pp`);
  console.log(`   1M Return:      ${oneMonthReturn.toFixed(2).padStart(8)}%    |    1.23%    |  ${(oneMonthReturn - 1.23).toFixed(2)}pp`);
  console.log();

  // 3. Análisis de consistencia
  console.log('='.repeat(100));
  console.log('ANÁLISIS DE CONSISTENCIA');
  console.log('='.repeat(100));
  console.log();

  // El rendimiento "total" desde el inicio debería aproximarse al P&L total / inversión inicial
  // Pero hay que considerar que:
  // - P&L Realizada ($570.52) = ganancias de ventas ya realizadas
  // - P&L No Realizada ($787.97) = valorización actual de posiciones abiertas
  // - El rendimiento histórico incluye TODO (realizadas + no realizadas)

  const totalPnL = 570.52 + 787.97; // $1,358.49
  
  // Pero la inversión inicial no es la actual ($7,144.48)
  // Es la inversión inicial histórica. Necesitamos calcularla.
  
  // Total de compras - Total de ventas = Inversión neta actual
  // $11,139.42 - $4,511.64 = $6,627.78 (inversión neta después de ventas)
  // Pero la UI muestra $7,144.48 como inversión total
  
  console.log('📊 Análisis de flujos:');
  console.log(`   Total Compras: $11,139.42`);
  console.log(`   Total Ventas: $4,511.64`);
  console.log(`   Inversión Neta (compras - ventas): $${(11139.42 - 4511.64).toFixed(2)}`);
  console.log(`   Inversión mostrada en UI: $7,144.48`);
  console.log();
  
  console.log('📊 Verificación de P&L:');
  console.log(`   P&L Realizada: $570.52`);
  console.log(`   P&L No Realizada: $787.97`);
  console.log(`   P&L Total: $${totalPnL.toFixed(2)}`);
  console.log();
  
  // El rendimiento "total" desde el inicio del histórico
  console.log(`📈 Rendimiento Total (desde ${firstDate}): ${totalReturn.toFixed(2)}%`);
  console.log();

  // Verificar si el rendimiento YTD es coherente
  // YTD = 11.34% significa que desde el 1 de enero, el portafolio creció 11.34%
  // Si el factor al 1 de enero era X y ahora es Y, entonces (Y/X - 1) * 100 = 11.34%
  
  console.log('💡 INTERPRETACIÓN:');
  console.log();
  console.log('   Los rendimientos históricos (1M, 3M, 6M, YTD) miden el RENDIMIENTO');
  console.log('   del portafolio ajustado por flujos de caja (compras/ventas).');
  console.log();
  console.log('   Esto es diferente del ROI simple = (Valor - Inversión) / Inversión');
  console.log(`   ROI Simple actual: ${simpleROI.toFixed(2)}%`);
  console.log(`   P&L No Realizada / Inversión: ${(787.97 / 7144.48 * 100).toFixed(2)}%`);
  console.log();
  console.log('   La diferencia se debe a que el rendimiento histórico considera');
  console.log('   CUÁNDO se hicieron las inversiones (Time-Weighted Return).');

  process.exit(0);
}

verifyReturnsVsPnL().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
