/**
 * Test Attribution Service
 * 
 * Script para probar el servicio de atribución del portafolio.
 * 
 * Uso: node scripts/diagnostics/testAttributionService.js
 */

const { 
  getPortfolioAttribution, 
  getTopContributors,
  checkAttributionAvailability 
} = require('../../services/attribution');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================
const USER_ID = 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const CURRENCY = 'USD';
const PERIOD = 'YTD';

// ============================================================================
// HELPERS
// ============================================================================

function formatPercent(value) {
  if (value === null || value === undefined) return 'N/A';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

function formatCurrency(value, currency = 'USD') {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat('en-US', { 
    style: 'currency', 
    currency: currency,
    minimumFractionDigits: 2 
  }).format(value);
}

function formatPP(value) {
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}pp`;
}

// ============================================================================
// TESTS
// ============================================================================

async function testCheckAvailability() {
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  TEST 1: checkAttributionAvailability');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  const result = await checkAttributionAvailability(USER_ID);
  
  console.log('📊 Resultado:');
  console.log(`   Available: ${result.available ? '✅ Sí' : '❌ No'}`);
  console.log(`   Last Update: ${result.lastUpdate}`);
  console.log(`   Currencies: ${result.availableCurrencies?.join(', ')}`);
  console.log(`   Asset Count: ${result.assetCount}`);
  
  return result.available;
}

async function testGetPortfolioAttribution() {
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  TEST 2: getPortfolioAttribution');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  const result = await getPortfolioAttribution({
    userId: USER_ID,
    period: PERIOD,
    currency: CURRENCY,
    accountIds: ['overall'],
    options: {
      benchmarkReturn: 15, // S&P 500 aproximado
      maxWaterfallBars: 8,
      includeMetadata: true
    }
  });
  
  if (!result.success) {
    console.log(`❌ Error: ${result.error}`);
    return false;
  }
  
  console.log('✅ Atribución calculada correctamente\n');
  
  // Summary
  const summary = result.summary;
  console.log('📊 RESUMEN:');
  console.log(`   Período: ${summary.periodLabel}`);
  console.log(`   Retorno del Portafolio: ${formatPercent(summary.portfolioReturn)}`);
  console.log(`   Retorno Absoluto: ${formatCurrency(summary.portfolioReturnAbsolute)}`);
  console.log(`   Benchmark: ${formatPercent(summary.benchmarkReturn)}`);
  console.log(`   Alpha: ${formatPP(summary.alpha)}`);
  console.log(`   Batiendo Benchmark: ${summary.beatingBenchmark ? '✅ Sí' : '❌ No'}`);
  console.log('');
  console.log(`   Top Contributor: ${summary.topContributor.ticker} (${formatPP(summary.topContributor.contribution)})`);
  console.log(`   Worst Contributor: ${summary.worstContributor.ticker} (${formatPP(summary.worstContributor.contribution)})`);
  console.log('');
  console.log(`   Total Assets: ${summary.totalAssets}`);
  console.log(`   Positivos: ${summary.positiveContributors} | Negativos: ${summary.negativeContributors}`);
  console.log('');
  
  // Top 5 attributions
  console.log('📈 TOP 5 CONTRIBUYENTES:');
  console.log('─'.repeat(80));
  console.log('Rank  Ticker      Peso      ROI           Contribución');
  console.log('─'.repeat(80));
  
  result.assetAttributions.slice(0, 5).forEach((attr, i) => {
    console.log(
      `${(i + 1).toString().padStart(2)}.   ` +
      `${attr.ticker.padEnd(12)}` +
      `${(attr.weightEnd * 100).toFixed(1)}%`.padEnd(10) +
      `${formatPercent(attr.returnPercent).padEnd(14)}` +
      `${formatPP(attr.contribution)}`
    );
  });
  
  // Bottom 5
  console.log('\n📉 BOTTOM 5 CONTRIBUYENTES:');
  console.log('─'.repeat(80));
  
  result.assetAttributions.slice(-5).reverse().forEach((attr, i) => {
    const rank = result.assetAttributions.length - 4 + i;
    console.log(
      `${rank.toString().padStart(2)}.   ` +
      `${attr.ticker.padEnd(12)}` +
      `${(attr.weightEnd * 100).toFixed(1)}%`.padEnd(10) +
      `${formatPercent(attr.returnPercent).padEnd(14)}` +
      `${formatPP(attr.contribution)}`
    );
  });
  
  // Waterfall summary
  console.log('\n📊 WATERFALL DATA:');
  console.log(`   Total points: ${result.waterfallData.length}`);
  console.log(`   Start value: ${formatCurrency(result.waterfallData[0]?.value)}`);
  console.log(`   End value: ${formatCurrency(result.waterfallData[result.waterfallData.length - 1]?.value)}`);
  
  // Metadata
  if (result.metadata) {
    console.log('\n📋 METADATA:');
    console.log(`   Calculated at: ${result.metadata.calculatedAt}`);
    console.log(`   Processing time: ${result.metadata.processingTimeMs}ms`);
    console.log(`   Data source: ${result.metadata.dataSource}`);
    console.log(`   Portfolio date: ${result.metadata.portfolioDate}`);
    console.log('');
    console.log('   Diagnostics:');
    console.log(`     Sum of contributions: ${formatPP(result.metadata.diagnostics.sumOfContributions)}`);
    console.log(`     Portfolio return: ${formatPercent(result.metadata.diagnostics.portfolioReturn)}`);
    console.log(`     Discrepancy: ${formatPP(result.metadata.diagnostics.discrepancy)}`);
    console.log(`     Normalized: ${result.metadata.diagnostics.normalized ? '✅' : '❌'}`);
  }
  
  return true;
}

async function testGetTopContributors() {
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  TEST 3: getTopContributors (Versión Ligera)');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  const result = await getTopContributors({
    userId: USER_ID,
    period: PERIOD,
    currency: CURRENCY,
    topN: 5
  });
  
  if (!result.success) {
    console.log(`❌ Error: ${result.error}`);
    return false;
  }
  
  console.log('✅ Top contributors obtenidos correctamente\n');
  
  console.log(`📊 Portfolio Return: ${formatPercent(result.portfolioReturn)}`);
  console.log(`📊 Total Assets: ${result.totalAssets}\n`);
  
  console.log('📈 TOP 5:');
  result.topContributors.forEach((attr, i) => {
    console.log(`   ${i + 1}. ${attr.ticker}: ${formatPP(attr.contribution)} (ROI: ${formatPercent(attr.returnPercent)})`);
  });
  
  console.log('\n📉 BOTTOM 5:');
  result.bottomContributors.forEach((attr, i) => {
    console.log(`   ${i + 1}. ${attr.ticker}: ${formatPP(attr.contribution)} (ROI: ${formatPercent(attr.returnPercent)})`);
  });
  
  return true;
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  TEST: Attribution Service');
  console.log('  User ID: ' + USER_ID);
  console.log('  Currency: ' + CURRENCY);
  console.log('  Period: ' + PERIOD);
  console.log('═══════════════════════════════════════════════════════════════');
  
  try {
    // Test 1: Check availability
    const available = await testCheckAvailability();
    if (!available) {
      console.log('\n❌ No hay datos disponibles para este usuario');
      process.exit(1);
    }
    
    // Test 2: Full attribution
    const fullOk = await testGetPortfolioAttribution();
    if (!fullOk) {
      console.log('\n❌ Error en getPortfolioAttribution');
      process.exit(1);
    }
    
    // Test 3: Top contributors (lite)
    const topOk = await testGetTopContributors();
    if (!topOk) {
      console.log('\n❌ Error en getTopContributors');
      process.exit(1);
    }
    
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('  ✅ TODOS LOS TESTS PASARON');
    console.log('═══════════════════════════════════════════════════════════════\n');
    
    process.exit(0);
    
  } catch (error) {
    console.error('\n❌ Error inesperado:', error);
    process.exit(1);
  }
}

main();
