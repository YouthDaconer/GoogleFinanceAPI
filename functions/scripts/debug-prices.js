/**
 * Debug Script: Verificar Precios de Mercado vs Calculados
 * 
 * Script para comparar los precios calculados desde portfolioPerformance
 * con los precios reales de mercado.
 */

require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') });

// Configurar token de servicio para autenticación
process.env.CF_SERVICE_TOKEN = '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10';

const admin = require('../services/firebaseAdmin');
const db = admin.firestore();
const { getQuotes } = require('../services/financeQuery');

const userId = process.argv[2] || 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2';
const currency = 'USD';

async function verifyPrices() {
  console.log('='.repeat(80));
  console.log('🔍 VERIFICACIÓN DE PRECIOS DE MERCADO vs CALCULADOS');
  console.log('='.repeat(80));
  
  try {
    // Obtener datos de portfolioPerformance
    const latestSnapshot = await db.collection(`portfolioPerformance/${userId}/dates`)
      .orderBy('date', 'desc')
      .limit(1)
      .get();
    
    if (latestSnapshot.empty) {
      console.log('❌ No hay datos de performance');
      return;
    }
    
    const latestDoc = latestSnapshot.docs[0];
    const latestData = latestDoc.data();
    const assetPerformance = latestData[currency]?.assetPerformance || {};
    
    console.log(`\nDatos del documento: ${latestDoc.id}`);
    console.log('-'.repeat(60));
    
    // Obtener tickers únicos
    const tickers = [...new Set(Object.keys(assetPerformance).map(k => k.split('_')[0]))];
    console.log(`\nTickers a verificar: ${tickers.join(', ')}`);
    
    // Obtener precios de mercado
    console.log('\n📈 Obteniendo precios de mercado...');
    const quotes = await getQuotes(tickers.join(','));
    
    const marketPrices = {};
    for (const quote of quotes) {
      // Manejar precios que vienen como string con comas
      let price = quote.price;
      if (typeof price === 'string') {
        price = parseFloat(price.replace(/,/g, ''));
      }
      marketPrices[quote.symbol] = price || 0;
    }
    
    console.log('\n' + '+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+');
    console.log('| Ticker        | Unidades   | Valor      | Precio Calc| Precio Mkt | Diff     |');
    console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+');
    
    for (const [key, data] of Object.entries(assetPerformance)) {
      const ticker = key.split('_')[0];
      const units = data.units || 0;
      const value = data.totalValue || 0;
      const calcPrice = units > 0 ? value / units : 0;
      const mktPrice = marketPrices[ticker] || 0;
      
      const diff = mktPrice > 0 ? Math.abs(calcPrice - mktPrice) / mktPrice * 100 : 0;
      const status = diff > 5 ? '❌' : '✅';
      
      console.log(`| ${ticker.padEnd(13)} | ${units.toFixed(4).padStart(10)} | $${value.toFixed(2).padStart(9)} | $${calcPrice.toFixed(2).padStart(9)} | $${mktPrice.toFixed(2).padStart(9)} | ${diff.toFixed(1).padStart(5)}% ${status} |`);
    }
    
    console.log('+' + '-'.repeat(15) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(12) + '+' + '-'.repeat(10) + '+');
    
    // Ahora verificar inicio del período
    console.log('\n\n📊 VERIFICAR DATOS INICIO YTD');
    console.log('-'.repeat(60));
    
    const startSnapshot = await db.collection(`portfolioPerformance/${userId}/dates`)
      .where('date', '>=', '2026-01-01')
      .orderBy('date', 'asc')
      .limit(1)
      .get();
    
    if (!startSnapshot.empty) {
      const startDoc = startSnapshot.docs[0];
      const startData = startDoc.data();
      const startAssets = startData[currency]?.assetPerformance || {};
      
      console.log(`\nDatos inicio (${startDoc.id}):`);
      
      for (const [key, endData] of Object.entries(assetPerformance)) {
        const ticker = key.split('_')[0];
        const startAsset = startAssets[key] || { units: 0, totalValue: 0 };
        
        const unitsStart = startAsset.units || 0;
        const unitsEnd = endData.units || 0;
        const valueStart = startAsset.totalValue || 0;
        const valueEnd = endData.totalValue || 0;
        
        if (Math.abs(unitsEnd - unitsStart) > 0.0001) {
          const priceStartCalc = unitsStart > 0 ? valueStart / unitsStart : 0;
          const priceEndCalc = unitsEnd > 0 ? valueEnd / unitsEnd : 0;
          const mktPrice = marketPrices[ticker] || 0;
          
          console.log(`\n${ticker} (cambió unidades):`);
          console.log(`  Inicio: ${unitsStart.toFixed(4)} unidades, valor $${valueStart.toFixed(2)}, precio calc $${priceStartCalc.toFixed(2)}`);
          console.log(`  Fin:    ${unitsEnd.toFixed(4)} unidades, valor $${valueEnd.toFixed(2)}, precio calc $${priceEndCalc.toFixed(2)}`);
          console.log(`  Precio mercado actual: $${mktPrice.toFixed(2)}`);
          
          // Calcular contribución correcta vs incorrecta
          const startTotalValue = startData[currency]?.totalValue || 8224.41;
          
          // Método actual (erróneo)
          const priceChangeWrong = priceEndCalc - priceStartCalc;
          const contribWrong = startTotalValue > 0 ? (priceChangeWrong * unitsStart / startTotalValue) * 100 : 0;
          
          // Método correcto (usar precio de mercado)
          const priceChangeCorrect = mktPrice - priceStartCalc;
          const contribCorrect = startTotalValue > 0 ? (priceChangeCorrect * unitsStart / startTotalValue) * 100 : 0;
          
          console.log(`  Contribución calculada (erróneo): ${contribWrong.toFixed(4)} pp`);
          console.log(`  Contribución correcta (con precio mkt): ${contribCorrect.toFixed(4)} pp`);
        }
      }
    }
    
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    process.exit(0);
  }
}

verifyPrices();
