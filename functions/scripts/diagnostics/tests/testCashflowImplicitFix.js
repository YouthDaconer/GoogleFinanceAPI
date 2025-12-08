/**
 * Test unitario para verificar el fix de cashflow implícito
 */

const { calculateAccountPerformance } = require('../../../utils/portfolioCalculations');

// Simular escenario: 
// - Ayer tenía 0.001 BTC con valor de $100
// - Hoy tengo 0.002 BTC con valor de $200 (compré 0.001 BTC más)
// - NO hay transacción del día (la compra se hizo fuera del horario del job)
// - El rendimiento debería ser ~0%, no +100%

const currencies = [{ code: 'USD', exchangeRate: 1 }];

const currentPrices = [{ symbol: 'BTC-USD', price: 100000 }]; // $100,000 por BTC

const assets = [
  {
    id: 'asset1',
    name: 'BTC-USD',
    assetType: 'crypto',
    portfolioAccount: 'account1',
    units: 0.002, // Hoy tengo 0.002 BTC
    unitValue: 100000,
    acquisitionDate: '2025-11-01',
    isActive: true,
    defaultCurrencyForAdquisitionDollar: 'USD',
    acquisitionDollarValue: 1
  }
];

// Datos de ayer
const totalValueYesterday = {
  USD: {
    totalValue: 100, // Ayer valía $100
    'BTC-USD_crypto': {
      totalValue: 100,
      units: 0.001 // Ayer tenía 0.001 BTC
    }
  }
};

// NO hay transacciones del día (la compra se hizo fuera del horario)
const todaysTransactions = [];

console.log('='.repeat(80));
console.log('TEST: CASHFLOW IMPLÍCITO POR DIFERENCIA DE UNIDADES');
console.log('='.repeat(80));
console.log();

console.log('📋 ESCENARIO:');
console.log('   - Ayer: 0.001 BTC = $100');
console.log('   - Hoy: 0.002 BTC = $200 (compré 0.001 BTC fuera del horario)');
console.log('   - Transacciones del día: NINGUNA');
console.log('   - Precio BTC: $100,000');
console.log();

console.log('📊 ESPERADO:');
console.log('   - adjustedDailyChangePercentage: ~0% (el aumento de valor es por la compra)');
console.log('   - totalCashFlow: ~$-100 (la compra implícita)');
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

// Asset level
const assetResult = result.USD.assetPerformance['BTC-USD_crypto'];
console.log('📈 RESULTADO A NIVEL DE ASSET:');
console.log(`   - adjustedDailyChangePercentage: ${assetResult.adjustedDailyChangePercentage.toFixed(4)}%`);
console.log(`   - totalCashFlow: $${assetResult.totalCashFlow.toFixed(2)}`);
console.log(`   - units: ${assetResult.units}`);
console.log();

// Verificar
const isAccountLevelCorrect = Math.abs(result.USD.adjustedDailyChangePercentage) < 5; // Debería ser cercano a 0
const isAssetLevelCorrect = Math.abs(assetResult.adjustedDailyChangePercentage) < 5;
const hasCashFlow = Math.abs(result.USD.totalCashFlow) > 50; // Debería tener cashflow detectado

console.log('='.repeat(80));
console.log('VERIFICACIÓN:');
console.log('='.repeat(80));
console.log(`   ✅ Nivel cuenta adjChange cercano a 0: ${isAccountLevelCorrect ? 'PASS' : 'FAIL'}`);
console.log(`   ✅ Nivel asset adjChange cercano a 0: ${isAssetLevelCorrect ? 'PASS' : 'FAIL'}`);
console.log(`   ✅ CashFlow implícito detectado: ${hasCashFlow ? 'PASS' : 'FAIL'}`);
console.log();

if (isAccountLevelCorrect && isAssetLevelCorrect && hasCashFlow) {
  console.log('✅ TODOS LOS TESTS PASARON');
} else {
  console.log('❌ ALGUNOS TESTS FALLARON');
}
