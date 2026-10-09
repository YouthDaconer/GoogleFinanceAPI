/**
 * Debug de calculateAccountPerformance para ECOPETROL
 */
process.env.CF_SERVICE_TOKEN = '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10';

const admin = require('../../services/firebaseAdmin');
const { getPricesFromApi, getCurrencyRatesFromApi } = require('../../services/marketDataHelper');
const { calculateAccountPerformance } = require('../../utils/portfolioCalculations');

const db = admin.firestore();

async function debug() {
  console.log('=== DEBUG DE calculateAccountPerformance ===\n');

  // 1. Obtener currencies
  const currencies = await getCurrencyRatesFromApi();
  console.log('Currencies:', currencies.map(c => `${c.code}=${c.exchangeRate}`).join(', '));

  // 2. Obtener solo el asset de ECOPETROL
  const ecopetrolSnapshot = await db.collection('assets')
    .where('portfolioAccount', '==', 'ggM52GimbLL7jwvegc9o')
    .get();
  
  const assets = ecopetrolSnapshot.docs.map(d => ({ id: d.id, ...d.data() }));
  console.log('\nAssets:', JSON.stringify(assets, null, 2));

  // 3. Obtener precio de ECOPETROL
  const prices = await getPricesFromApi(['ECOPETROL.CL']);
  console.log('\nPrecios:', JSON.stringify(prices, null, 2));

  // 4. Calcular manualmente
  const asset = assets[0];
  const priceData = prices.find(p => p.symbol === asset.name);
  
  console.log('\n=== CÁLCULO MANUAL ===');
  console.log(`unitValue: ${asset.unitValue}`);
  console.log(`units: ${asset.units}`);
  console.log(`currency: ${asset.currency}`);
  console.log(`acquisitionDollarValue: ${asset.acquisitionDollarValue}`);
  console.log(`currentPrice: ${priceData?.price}`);
  console.log(`priceCurrency: ${priceData?.currency}`);
  
  const initialInvestmentInAssetCurrency = asset.unitValue * asset.units;
  console.log(`\ninitialInvestmentInAssetCurrency: ${initialInvestmentInAssetCurrency} ${asset.currency}`);
  
  let initialInvestmentUSD;
  if (asset.currency !== 'USD' && asset.acquisitionDollarValue) {
    initialInvestmentUSD = initialInvestmentInAssetCurrency / asset.acquisitionDollarValue;
    console.log(`initialInvestmentUSD (convertido): ${initialInvestmentUSD}`);
  } else {
    initialInvestmentUSD = initialInvestmentInAssetCurrency;
    console.log(`initialInvestmentUSD (sin conversión): ${initialInvestmentUSD}`);
  }
  
  // 5. Ejecutar calculateAccountPerformance
  console.log('\n=== RESULTADO DE calculateAccountPerformance ===');
  console.log('Currencies format:', JSON.stringify(currencies.slice(0, 2), null, 2));
  
  // Verificar si el formato es correcto para convertCurrency
  const copCurrency = currencies.find(c => c.code === 'COP');
  const usdCurrency = currencies.find(c => c.code === 'USD');
  console.log('COP currency object:', copCurrency);
  console.log('USD currency object:', usdCurrency);
  
  const lastTotalValue = { USD: { totalValue: 0 }, COP: { totalValue: 0 } };
  const result = calculateAccountPerformance(assets, prices, currencies, lastTotalValue, []);
  
  console.log('USD.totalInvestment:', result.USD?.totalInvestment);
  console.log('USD.totalValue:', result.USD?.totalValue);
  console.log('COP.totalInvestment:', result.COP?.totalInvestment);
  console.log('COP.totalValue:', result.COP?.totalValue);
}

debug().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
