const admin = require('./firebaseAdmin');
const axios = require('axios');

/**
 * Obtiene la tasa de cambio actual de una moneda usando Yahoo Finance
 * @param {string} currencyCode - Código de la moneda a consultar
 * @return {Promise<number|null>} - Retorna la tasa de cambio o null si hay error
 */
async function getCurrencyRateFromYahoo(currencyCode) {
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${currencyCode}=X?lang=en-US&region=US`;
    console.log(`Consultando tasa para ${currencyCode} en Yahoo Finance: ${url}`);
    
    const { data } = await axios.get(url);
    
    // Verificar si hay resultados y meta datos en la respuesta
    if (data?.chart?.result?.[0]?.meta) {
      const meta = data.chart.result[0].meta;
      const rate = meta.regularMarketPrice || null;
      
      console.log(`✓ Tasa obtenida para ${currencyCode}: ${rate}`);
      return rate;
    }
    
    console.warn(`✗ No se encontraron datos para ${currencyCode}`);
    return null;
  } catch (error) {
    console.error(`✗ Error al obtener tasa para ${currencyCode} desde Yahoo Finance:`, error.message);
    return null;
  }
}

/**
 * Actualiza todas las monedas activas en la colección currencies
 * con los datos más recientes de Yahoo Finance
 */
async function testUpdateCurrencyRates() {
  try {
    const db = admin.firestore();
    const currenciesRef = db.collection('currencies');

    // Obtener todas las monedas activas
    const snapshot = await currenciesRef.where('isActive', '==', true).get();
    
    if (snapshot.empty) {
      console.log('No hay monedas activas en la colección "currencies"');
      return 0;
    }
    
    console.log(`Se encontraron ${snapshot.size} monedas activas`);
    
    // Crear batch para actualización masiva
    const batch = db.batch();
    let updatesCount = 0;

    // Procesar cada moneda
    for (const doc of snapshot.docs) {
      const { code, name, symbol } = doc.data();

      try {
        // Obtener tasa de cambio desde Yahoo Finance
        const newRate = await getCurrencyRateFromYahoo(code);

        if (newRate && !isNaN(newRate) && newRate > 0) {
          const updatedData = {
            code: code,
            name: name,
            symbol: symbol,
            exchangeRate: newRate,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp()
          };

          batch.update(doc.ref, updatedData);
          updatesCount++;
        } else {
          console.warn(`✗ Valor inválido para ${code}: ${newRate}`);
        }
      } catch (error) {
        console.error(`✗ Error al procesar ${code}:`, error.message);
      }
    }

    // Guardar cambios en Firestore
    if (updatesCount > 0) {
      await batch.commit();
      console.log(`✅ ${updatesCount} tasas de cambio han sido actualizadas`);
    } else {
      console.log('No se requirieron actualizaciones');
    }
    
    return updatesCount;
  } catch (error) {
    console.error('🔥 Error general al actualizar tasas de cambio:', error);
    throw error;
  }
}

/**
 * Prueba la obtención de tasa de cambio para una moneda específica
 * @param {string} currencyCode - Código de la moneda a probar
 */
async function testSingleCurrency(currencyCode) {
  try {
    console.log(`Probando obtención de tasa para: ${currencyCode}`);
    const rate = await getCurrencyRateFromYahoo(currencyCode);
    
    if (rate) {
      console.log(`✅ Prueba exitosa para ${currencyCode}: ${rate}`);
    } else {
      console.log(`❌ Prueba fallida para ${currencyCode}: no se pudo obtener la tasa`);
    }
    
    return rate;
  } catch (error) {
    console.error(`❌ Error en prueba para ${currencyCode}:`, error);
    return null;
  }
}

// Ejecutar prueba de una sola moneda (USD a COP)
testSingleCurrency('COP')
  .then(() => {
    // Luego de probar una moneda específica, actualizar todas si la prueba fue exitosa
    console.log('\n--- Actualizando todas las monedas activas ---\n');
    return testUpdateCurrencyRates();
  })
  .then(count => {
    console.log(`Proceso de prueba completado. ${count} monedas actualizadas.`);
  })
  .catch(error => {
    console.error('Error en el proceso de prueba:', error);
  });

module.exports = { 
  getCurrencyRateFromYahoo, 
  testUpdateCurrencyRates,
  testSingleCurrency
}; 