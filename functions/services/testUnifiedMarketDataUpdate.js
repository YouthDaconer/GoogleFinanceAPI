const admin = require('./firebaseAdmin');
const axios = require('axios');

const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

/**
 * 🚀 OPTIMIZACIÓN: Función unificada que obtiene todos los datos de mercado en una sola llamada
 * Combina monedas y símbolos de activos para minimizar llamadas a la API Lambda
 */
async function getAllMarketDataBatch(currencyCodes, assetSymbols) {
  try {
    // Preparar símbolos de monedas (agregar %3DX para codificación URL)
    const currencySymbols = currencyCodes.map(code => `${code}%3DX`);
    
    // Combinar todos los símbolos en una sola consulta
    const allSymbols = [...currencySymbols, ...assetSymbols];
    
    // Dividir en lotes más grandes (100 símbolos por llamada para optimizar)
    const batchSize = 100;
    const results = {
      currencies: {},
      assets: new Map()
    };
    
    console.log(`📡 Consultando ${allSymbols.length} símbolos en ${Math.ceil(allSymbols.length / batchSize)} lotes optimizados`);
    
    for (let i = 0; i < allSymbols.length; i += batchSize) {
      const symbolBatch = allSymbols.slice(i, i + batchSize);
      const symbolsParam = symbolBatch.join(',');
      
      const url = `${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`;
      console.log(`🔄 Lote ${Math.floor(i/batchSize) + 1}: ${symbolBatch.length} símbolos`);
      console.log(`   URL: ${url.substring(0, 100)}...`);
      
      const { data } = await axios.get(url);
      
      if (Array.isArray(data)) {
        data.forEach(item => {
          if (item.symbol && item.regularMarketPrice) {
            // Si es una moneda (termina en =X en la respuesta)
            if (item.symbol.includes('=X')) {
              const currencyCode = item.symbol.replace('=X', '');
              if (currencyCodes.includes(currencyCode)) {
                results.currencies[currencyCode] = item.regularMarketPrice;
                console.log(`   💱 Moneda obtenida: ${currencyCode} = ${item.regularMarketPrice}`);
              }
            } 
            // Si es un activo normal
            else if (assetSymbols.includes(item.symbol)) {
              results.assets.set(item.symbol, item);
              console.log(`   📈 Activo obtenido: ${item.symbol} = ${item.regularMarketPrice} ${item.currency}`);
            }
          }
        });
      }
    }
    
    console.log(`✅ Datos obtenidos: ${Object.keys(results.currencies).length} monedas, ${results.assets.size} activos`);
    return results;
  } catch (error) {
    console.error(`❌ Error al obtener datos de mercado en lote:`, error.message);
    return { currencies: {}, assets: new Map() };
  }
}

/**
 * Actualiza las tasas de cambio de monedas usando datos ya obtenidos
 */
async function updateCurrencyRates(db, currencyRates) {
  console.log('🔄 Actualizando tasas de cambio...');
  
  const currenciesRef = db.collection('currencies');
  const snapshot = await currenciesRef.where('isActive', '==', true).get();
  const batch = db.batch();
  let updatesCount = 0;

  const activeCurrencies = snapshot.docs.map(doc => ({
    code: doc.data().code,
    ref: doc.ref,
    data: doc.data()
  }));
  
  activeCurrencies.forEach(currency => {
    const { code, ref, data } = currency;
    const newRate = currencyRates[code];
    
    if (newRate && !isNaN(newRate) && newRate > 0) {
      const updatedData = {
        code: code,
        name: data.name,
        symbol: data.symbol,
        exchangeRate: newRate,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      };

      batch.update(ref, updatedData);
      updatesCount++;
      console.log(`   ✅ Actualizada tasa de cambio para USD:${code} a ${newRate}`);
    } else {
      console.warn(`   ⚠️ Valor inválido para USD:${code}: ${newRate}`);
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    console.log(`✅ ${updatesCount} tasas de cambio actualizadas`);
  }
  
  return updatesCount;
}

/**
 * Actualiza los precios actuales de los activos usando datos ya obtenidos
 */
async function updateCurrentPrices(db, assetQuotes) {
  console.log('🔄 Actualizando precios actuales...');
  
  const currentPricesRef = db.collection('currentPrices');
  const snapshot = await currentPricesRef.get();
  const batch = db.batch();
  let updatesCount = 0;

  snapshot.docs.forEach(doc => {
    const docData = doc.data();
    const symbol = docData.symbol;
    const quote = assetQuotes.get(symbol);
    
    if (quote && quote.regularMarketPrice) {
      const updatedData = {
        symbol: symbol,
        price: quote.regularMarketPrice,
        lastUpdated: Date.now(),
        change: quote.regularMarketChange,
        percentChange: quote.regularMarketChangePercent,
        previousClose: quote.regularMarketPreviousClose,
        currency: quote.currency,
        marketState: quote.marketState,
        quoteType: quote.quoteType,
        exchange: quote.exchange,
        fullExchangeName: quote.fullExchangeName
      };
      
      // Mantener campos existentes
      if (docData.name) updatedData.name = docData.name;
      if (docData.isin) updatedData.isin = docData.isin;
      if (docData.type) updatedData.type = docData.type;
      
      batch.update(doc.ref, updatedData);
      updatesCount++;
      console.log(`   ✅ Actualizado precio para ${symbol}: ${quote.regularMarketPrice} ${quote.currency}`);
    } else {
      console.log(`   ⚠️ No se pudo obtener precio para ${symbol}`);
    }
  });

  if (updatesCount > 0) {
    await batch.commit();
    console.log(`✅ ${updatesCount} precios actualizados`);
  } else {
    console.log('ℹ️ No se requirieron actualizaciones de precios');
  }
  
  return updatesCount;
}

/**
 * Simula el cálculo de rendimiento diario del portafolio (versión simplificada para pruebas)
 */
async function testCalculateDailyPortfolioPerformance(db) {
  console.log('🔄 Simulando cálculo de rendimiento diario del portafolio...');
  
  try {
    // Solo contar usuarios con cuentas activas para la prueba
    const portfolioAccountsSnapshot = await db.collection('portfolioAccounts')
      .where('isActive', '==', true)
      .get();
    
    const userIds = [...new Set(portfolioAccountsSnapshot.docs.map(doc => doc.data().userId))];
    
    console.log(`   📊 Encontrados ${userIds.length} usuarios con cuentas activas`);
    console.log(`   📊 Total de cuentas activas: ${portfolioAccountsSnapshot.docs.length}`);
    
    return userIds.length;
  } catch (error) {
    console.error('   ❌ Error en simulación de cálculo de rendimiento:', error.message);
    return 0;
  }
}

/**
 * Simula el cálculo de riesgo del portafolio (versión simplificada para pruebas)
 */
async function testCalculatePortfolioRisk() {
  console.log('🔄 Simulando cálculo de riesgo del portafolio...');
  
  try {
    // En pruebas, solo simulamos que se ejecuta correctamente
    console.log('   📊 Simulación de cálculo de riesgo completada');
    return true;
  } catch (error) {
    console.error('   ❌ Error en simulación de cálculo de riesgo:', error.message);
    return false;
  }
}

/**
 * Función principal de prueba unificada que ejecuta todas las actualizaciones
 */
async function testUnifiedMarketDataUpdate() {
  console.log('🚀 INICIANDO PRUEBA DE ACTUALIZACIÓN UNIFICADA DE DATOS DE MERCADO...');
  console.log('='.repeat(80));
  
  const db = admin.firestore();
  const startTime = Date.now();
  
  try {
    // Paso 1: Obtener códigos de monedas y símbolos de activos dinámicamente
    console.log('\n📋 PASO 1: Obteniendo códigos de monedas y símbolos de activos...');
    const [currenciesSnapshot, currentPricesSnapshot] = await Promise.all([
      db.collection('currencies').where('isActive', '==', true).get(),
      db.collection('currentPrices').get()
    ]);
    
    const currencyCodes = currenciesSnapshot.docs.map(doc => doc.data().code);
    const assetSymbols = currentPricesSnapshot.docs.map(doc => doc.data().symbol);
    
    console.log(`   📊 Monedas activas encontradas: ${currencyCodes.length}`);
    console.log(`   📊 Activos encontrados: ${assetSymbols.length}`);
    console.log(`   💱 Códigos de monedas: ${currencyCodes.join(', ')}`);
    console.log(`   📈 Primeros 10 activos: ${assetSymbols.slice(0, 10).join(', ')}${assetSymbols.length > 10 ? '...' : ''}`);
    
    // Paso 2: Obtener TODOS los datos de mercado en llamadas optimizadas
    console.log('\n📡 PASO 2: Obteniendo datos de mercado de la API Lambda...');
    const marketData = await getAllMarketDataBatch(currencyCodes, assetSymbols);
    
    // Paso 3: Actualizar tasas de cambio con datos ya obtenidos
    console.log('\n💱 PASO 3: Actualizando tasas de cambio...');
    const currencyUpdates = await updateCurrencyRates(db, marketData.currencies);
    
    // Paso 4: Actualizar precios actuales con datos ya obtenidos
    console.log('\n📈 PASO 4: Actualizando precios actuales...');
    const priceUpdates = await updateCurrentPrices(db, marketData.assets);
    
    // Paso 5: Simular cálculo de rendimiento del portafolio
    console.log('\n📊 PASO 5: Simulando cálculo de rendimiento del portafolio...');
    const portfolioCalculations = await testCalculateDailyPortfolioPerformance(db);
    
    // Paso 6: Simular cálculo de riesgo del portafolio
    console.log('\n⚡ PASO 6: Simulando cálculo de riesgo del portafolio...');
    const riskCalculationSuccess = await testCalculatePortfolioRisk();
    
    const endTime = Date.now();
    const executionTime = (endTime - startTime) / 1000;
    
    console.log('\n' + '='.repeat(80));
    console.log('🎉 PRUEBA DE ACTUALIZACIÓN UNIFICADA COMPLETADA');
    console.log('='.repeat(80));
    console.log(`⏱️  Tiempo total de ejecución: ${executionTime.toFixed(2)}s`);
    console.log(`💱 Tasas de cambio actualizadas: ${currencyUpdates}`);
    console.log(`📈 Precios de activos actualizados: ${priceUpdates}`);
    console.log(`👥 Usuarios con cuentas activas: ${portfolioCalculations}`);
    console.log(`⚡ Cálculo de riesgo: ${riskCalculationSuccess ? 'Exitoso' : 'Falló'}`);
    console.log('='.repeat(80));
    
    return {
      success: true,
      executionTime,
      currencyUpdates,
      priceUpdates,
      portfolioCalculations,
      riskCalculationSuccess
    };
  } catch (error) {
    console.error('\n❌ ERROR EN PRUEBA DE ACTUALIZACIÓN UNIFICADA:', error);
    console.error('Stack trace:', error.stack);
    return {
      success: false,
      error: error.message
    };
  }
}

// Ejecutar la prueba
console.log('🧪 Iniciando prueba de función unificada...');
testUnifiedMarketDataUpdate()
  .then(result => {
    if (result) {
      console.log('\n✅ Prueba finalizada:', result.success ? 'EXITOSA' : 'CON ERRORES');
    }
    process.exit(result?.success ? 0 : 1);
  })
  .catch(error => {
    console.error('\n💥 Error fatal en la prueba:', error);
    process.exit(1);
  }); 