/**
 * Test: Día normal sin transacciones, solo variación de precio
 */

const { calculateAccountPerformance } = require('../../../utils/portfolioCalculations');

// Simular escenario:
// - Ayer tenía 0.001 BTC a $100,000 = $100
// - Hoy tengo 0.001 BTC a $105,000 = $105 (subió 5%)
// - NO hay transacciones

const currencies = [{ code: 'USD', exchangeRate: 1 }];

const currentPrices = [{ symbol: 'BTC-USD', price: 105000 }]; // Subió a $105,000

const assets = [
  {
    id: 'asset1',
    name: 'BTC-USD',
    assetType: 'crypto',
    portfolioAccount: 'account1',
    units: 0.001, // Mismas unidades que ayer
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
      units: 0.001 // Mismas unidades
    }
  }
};

const todaysTransactions = []; // Sin transacciones

console.log('='.repeat(80));
console.log('TEST: DÍA NORMAL SIN TRANSACCIONES (SOLO VARIACIÓN DE PRECIO)');
console.log('='.repeat(80));
console.log();

console.log('📋 ESCENARIO:');
console.log('   - Ayer: 0.001 BTC @ $100,000 = $100');
console.log('   - Hoy: 0.001 BTC @ $105,000 = $105');
console.log('   - Transacciones: NINGUNA');
console.log();

console.log('📊 ESPERADO:');
console.log('   - adjustedDailyChangePercentage: 5% (variación de precio)');
console.log('   - totalCashFlow: $0');
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
const isAdjChangeCorrect = Math.abs(result.USD.adjustedDailyChangePercentage - 5) < 0.1; // Debería ser ~5%
const isCashFlowZero = Math.abs(result.USD.totalCashFlow) < 0.01;

console.log('='.repeat(80));
console.log('VERIFICACIÓN:');
console.log('='.repeat(80));
console.log(`   ✅ adjChange es ~5%: ${isAdjChangeCorrect ? 'PASS' : 'FAIL'}`);
console.log(`   ✅ CashFlow es $0: ${isCashFlowZero ? 'PASS' : 'FAIL'}`);
console.log();

if (isAdjChangeCorrect && isCashFlowZero) {
  console.log('✅ TODOS LOS TESTS PASARON');
} else {
  console.log('❌ ALGUNOS TESTS FALLARON');
}
