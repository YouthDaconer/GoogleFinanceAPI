const admin = require('./firebaseAdmin');
const axios = require('axios');

// Horarios estáticos para NYSE (en UTC)
const NYSE_OPEN_HOUR = 13.5;  // 9:30 AM EST
const NYSE_CLOSE_HOUR = 20;   // 4:00 PM EST

/**
 * Obtiene las tasas de cambio actuales de múltiples monedas en una sola petición
 * @param {string[]} currencyCodes - Array de códigos de monedas a consultar
 * @return {Promise<Object|null>} - Retorna un objeto con las tasas de cambio o null si hay error
 */
async function getCurrencyRatesBatch(currencyCodes) {
  try {
    // Formatea los códigos de moneda para la URL
    const symbolsParam = currencyCodes.map(code => `${code}%3DX`).join(',');
    const url = `https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1/market-quotes?symbols=${symbolsParam}`;
    
    console.log(`Consultando tasas para múltiples monedas: ${url}`);
    
    const { data } = await axios.get(url);
    
    // Procesar los resultados y organizarlos por código de moneda
    const rates = {};
    
    if (Array.isArray(data)) {
      data.forEach(currencyData => {
        // Extraer el código de la moneda del símbolo (eliminando '%3DX')
        const code = currencyData.symbol.replace('%3DX', '');
        if (currencyData.regularMarketPrice && !isNaN(currencyData.regularMarketPrice)) {
          rates[code] = currencyData.regularMarketPrice;
        }
      });
      
      return rates;
    }
    
    console.warn('Formato de respuesta inesperado:', data);
    return null;
  } catch (error) {
    console.error(`Error al obtener tasas de cambio en lote:`, error.message);
    return null;
  }
}

function isNYSEMarketOpen() {
  const now = new Date();
  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60;
  return utcHour >= NYSE_OPEN_HOUR && utcHour < NYSE_CLOSE_HOUR;
}

/**
 * Actualiza todas las monedas activas en la colección currencies
 * con los datos más recientes de Yahoo Finance
 */
async function testUpdateCurrencyRates() {
  /*if (!isNYSEMarketOpen()) {
    console.log('El mercado NYSE está cerrado. No se actualizarán las tasas de cambio.');
    return null;
  }*/

  const db = admin.firestore();
  const currenciesRef = db.collection('currencies');

  try {
    const snapshot = await currenciesRef.where('isActive', '==', true).get();
    const batch = db.batch();
    let updatesCount = 0;

    // Extraer todos los códigos de moneda activos
    const activeCurrencies = snapshot.docs.map(doc => ({
      code: doc.data().code,
      ref: doc.ref,
      data: doc.data()
    }));
    
    const currencyCodes = activeCurrencies.map(currency => currency.code);
    
    // Obtener todas las tasas de cambio en una sola petición
    const exchangeRates = await getCurrencyRatesBatch(currencyCodes);
    
    if (exchangeRates) {
      activeCurrencies.forEach(currency => {
        const { code, ref, data } = currency;
        const newRate = exchangeRates[`${code}=X`];
        
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
          console.log(`Actualizada tasa de cambio para USD:${code} a ${newRate}`);
        } else {
          console.warn(`Valor inválido para USD:${code}: ${newRate}`);
        }
      });

      if (updatesCount > 0) {
        await batch.commit();
        console.log(`${updatesCount} tasas de cambio han sido actualizadas`);
      } else {
        console.log('No se requirieron actualizaciones');
      }
    } else {
      console.error('No se pudieron obtener las tasas de cambio');
    }
  } catch (error) {
    console.error('Error al actualizar tasas de cambio:', error);
  }

  return null;
}

testUpdateCurrencyRates();

