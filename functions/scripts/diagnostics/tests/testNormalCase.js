/**
 * Test unitario para verificar que el caso normal (con transacciones) sigue funcionando
 */

const { calculateAccountPerformance } = require('../../../utils/portfolioCalculations');

// Simular escenario NORMAL:
// - Ayer tenía 0.001 BTC con valor de $100
// - Hoy tengo 0.002 BTC con valor de $200 (compré 0.001 BTC)
// - SÍ hay transacción del día (la compra se hizo en horario del job)

const currencies = [{ code: 'USD', exchangeRate: 1 }];

const currentPrices = [{ symbol: 'BTC-USD', price: 100000 }]; // $100,000 por BTC

const assets = [
  {
    id: 'asset1',
    name: 'BTC-USD',
    assetType: 'crypto',
    portfolioAccount: 'account1',
    units: 0.002,
    unitValue: 100000,
    acquisitionDate: '2025-11-01',
    isActive: true,
    defaultCurrencyForAdquisitionDollar: 'USD',
    acquisitionDollarValue: 1
  }
];

const totalValueYesterday = {
  USD: {
    totalValue: 100,
    'BTC-USD_crypto': {
      totalValue: 100,
      units: 0.001
    }
  }
};

// SÍ hay transacción de compra
const todaysTransactions = [
  {
    id: 'tx1',
    assetId: 'asset1',
    type: 'buy',
    amount: 0.001, // Compré 0.001 BTC
    price: 100000, // A $100,000
    currency: 'USD',
    portfolioAccountId: 'account1',
    dollarPriceToDate: 1,
    defaultCurrencyForAdquisitionDollar: 'USD'
  }
];

console.log('='.repeat(80));
console.log('TEST: CASO NORMAL CON TRANSACCIÓN DEL DÍA');
console.log('='.repeat(80));
console.log();

console.log('📋 ESCENARIO:');
console.log('   - Ayer: 0.001 BTC = $100');
console.log('   - Hoy: 0.002 BTC = $200 (compré 0.001 BTC EN horario del job)');
console.log('   - Transacción del día: Compra de 0.001 BTC a $100,000');
console.log();

console.log('📊 ESPERADO:');
console.log('   - adjustedDailyChangePercentage: ~0% (el aumento es por la compra)');
console.log('   - totalCashFlow: ~$-100 (la compra registrada)');
console.log();

const result = calculateAccountPerformance(
  assets,
  currentPrices,
  currencies,
  totalValueYesterday,
  todaysTransactions
);

console.log('📈 RESULTADO:');
console.log(`   - adjustedDailyChangePercentage: ${result.USD.adjustedDailyChangePercentage.toFixed(4)}%`);
console.log(`   - totalCashFlow: $${result.USD.totalCashFlow.toFixed(2)}`);
console.log(`   - dailyChangePercentage (raw): ${result.USD.dailyChangePercentage.toFixed(4)}%`);
console.log();

const assetResult = result.USD.assetPerformance['BTC-USD_crypto'];
console.log('📈 RESULTADO A NIVEL DE ASSET:');
console.log(`   - adjustedDailyChangePercentage: ${assetResult.adjustedDailyChangePercentage.toFixed(4)}%`);
console.log(`   - totalCashFlow: $${assetResult.totalCashFlow.toFixed(2)}`);
console.log();

// Verificar
const isAccountLevelCorrect = Math.abs(result.USD.adjustedDailyChangePercentage) < 5;
const isAssetLevelCorrect = Math.abs(assetResult.adjustedDailyChangePercentage) < 5;
const hasCashFlow = Math.abs(result.USD.totalCashFlow) > 50;

console.log('='.repeat(80));
console.log('VERIFICACIÓN:');
console.log('='.repeat(80));
console.log(`   ✅ Nivel cuenta adjChange cercano a 0: ${isAccountLevelCorrect ? 'PASS' : 'FAIL'}`);
console.log(`   ✅ Nivel asset adjChange cercano a 0: ${isAssetLevelCorrect ? 'PASS' : 'FAIL'}`);
console.log(`   ✅ CashFlow registrado: ${hasCashFlow ? 'PASS' : 'FAIL'}`);
console.log();

if (isAccountLevelCorrect && isAssetLevelCorrect && hasCashFlow) {
  console.log('✅ TODOS LOS TESTS PASARON');
} else {
  console.log('❌ ALGUNOS TESTS FALLARON');
}
