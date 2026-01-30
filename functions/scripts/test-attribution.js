/**
 * Debug Script: Test Attribution Service
 * 
 * Script para probar el servicio de atribución corregido.
 */

require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') });

// Configurar token de servicio para autenticación
process.env.CF_SERVICE_TOKEN = '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10';

const { getPortfolioAttribution } = require('../services/attribution');

const userId = process.argv[2] || 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const period = process.argv[3] || 'YTD';
const currency = process.argv[4] || 'USD';

console.log('='.repeat(80));
console.log('🧪 TEST SERVICIO DE ATRIBUCIÓN');
console.log('='.repeat(80));
console.log(`Usuario: ${userId}`);
console.log(`Período: ${period}`);
console.log(`Moneda: ${currency}`);
console.log('');

async function testAttribution() {
  try {
    console.log('📊 Ejecutando getPortfolioAttribution...\n');
    
    const result = await getPortfolioAttribution({
      userId,
      period,
      currency,
      accountIds: ['overall'],
      options: {
        benchmarkReturn: 0,
        maxWaterfallBars: 8,
        includeMetadata: true,
        includeIntraday: true
      }
    });
    
    if (!result.success) {
      console.error('❌ Error:', result.error);
      return;
    }
    
    console.log('\n' + '='.repeat(80));
    console.log('📊 RESULTADOS DE ATRIBUCIÓN');
    console.log('='.repeat(80));
    
    // Mostrar resumen
    const summary = result.summary;
    console.log('\n📈 RESUMEN:');
    console.log(`  Retorno del portafolio: ${summary?.portfolioReturn?.toFixed(2)}%`);
    console.log(`  Alpha vs benchmark: ${summary?.alpha?.toFixed(2)}pp`);
    console.log(`  Top contribuyente: ${summary?.topContributor?.ticker} (${summary?.topContributor?.contribution?.toFixed(2)}pp)`);
    console.log(`  Contribuyentes positivos: ${summary?.positiveContributors}`);
    console.log(`  Contribuyentes negativos: ${summary?.negativeContributors}`);
    
    // Mostrar diagnósticos
    const metadata = result.metadata;
    console.log('\n📋 DIAGNÓSTICO:');
    console.log(`  TWR histórico: ${metadata?.diagnostics?.historicalTWR?.toFixed(2)}%`);
    console.log(`  TWR del período: ${metadata?.diagnostics?.periodTWR?.toFixed(2)}%`);
    console.log(`  Suma de contribuciones: ${metadata?.diagnostics?.sumOfContributions?.toFixed(2)}%`);
    console.log(`  Normalizado: ${metadata?.diagnostics?.normalized}`);
    console.log(`  Discrepancia: ${metadata?.diagnostics?.discrepancy?.toFixed(2)}pp`);
    
    // Info de intraday
    console.log('\n⏱️ INTRADAY:');
    console.log(`  Incluido: ${metadata?.intraday?.included}`);
    console.log(`  Aplicado: ${metadata?.intraday?.applied}`);
    if (metadata?.intraday?.dailyChangePercent) {
      console.log(`  Cambio diario: ${metadata?.intraday?.dailyChangePercent?.toFixed(2)}%`);
    }
    
    // Mostrar top 10 contribuciones
    console.log('\n📊 TOP 10 CONTRIBUCIONES:');
    console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+');
    console.log('| Ticker        | Retorno %   | Contrib pp  | Status    |');
    console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+');
    
    const sortedByContrib = [...result.assetAttributions]
      .sort((a, b) => Math.abs(b.contribution) - Math.abs(a.contribution))
      .slice(0, 10);
    
    for (const attr of sortedByContrib) {
      const sign = attr.contribution >= 0 ? '+' : '';
      console.log(`| ${attr.ticker.padEnd(13)} | ${attr.returnPercent?.toFixed(2).padStart(9)}% | ${sign}${attr.contribution?.toFixed(4).padStart(9)} | ${(attr.hasUnitChange ? 'cambió' : 'igual').padEnd(8)} |`);
    }
    console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+');
    
    // Verificar suma
    const sumContrib = result.assetAttributions.reduce((sum, a) => sum + a.contribution, 0);
    console.log(`\n📊 Suma de contribuciones calculada: ${sumContrib.toFixed(4)}%`);
    console.log(`📊 Retorno del período (TWR): ${summary?.portfolioReturn?.toFixed(4)}%`);
    console.log(`📊 Discrepancia: ${Math.abs(sumContrib - (summary?.portfolioReturn || 0)).toFixed(4)}pp`);
    
    // Mostrar activos con contribución 0
    const zeroContrib = result.assetAttributions.filter(a => Math.abs(a.contribution) < 0.01);
    if (zeroContrib.length > 0) {
      console.log(`\n⚠️ Activos con contribución ~0: ${zeroContrib.length}`);
      for (const attr of zeroContrib.slice(0, 5)) {
        console.log(`   - ${attr.ticker}: ${attr.contribution?.toFixed(4)}pp (retorno: ${attr.returnPercent?.toFixed(2)}%)`);
      }
    }
    
    console.log('\n' + '='.repeat(80));
    console.log('✅ TEST COMPLETADO');
    console.log('='.repeat(80));
    
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    process.exit(0);
  }
}

testAttribution();
