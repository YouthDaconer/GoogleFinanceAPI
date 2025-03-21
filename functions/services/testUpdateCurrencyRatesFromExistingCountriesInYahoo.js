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
 * Obtiene las monedas únicas de los países en Firestore y actualiza la colección currencies
 * con datos de Yahoo Finance, solo si la moneda no existe previamente.
 * Además, elimina países cuyas monedas no pueden consultarse en Yahoo Finance.
 */
async function updateCurrencyRatesFromExistingCountriesInYahoo() {
  try {
    const db = admin.firestore();
    
    // 1. Obtener todos los países de Firestore
    const countriesSnapshot = await db.collection('countries').get();
    
    if (countriesSnapshot.empty) {
      console.log('No hay países en la colección "countries"');
      return;
    }
    
    console.log(`Se encontraron ${countriesSnapshot.size} países en Firestore`);
    
    // Mapa para relacionar códigos de moneda con países
    const currencyToCountries = new Map();
    
    // 2. Extraer monedas únicas por currencyCode y mapear a qué países pertenecen
    const uniqueCurrencies = new Map();
    
    countriesSnapshot.forEach(doc => {
      const country = doc.data();
      if (country.currencyCode) {
        // Guardar relación entre moneda y países
        if (!currencyToCountries.has(country.currencyCode)) {
          currencyToCountries.set(country.currencyCode, []);
        }
        currencyToCountries.get(country.currencyCode).push({
          id: doc.id,
          code2: country.code2,
          country: country.country
        });
        
        // Guardar moneda única
        if (!uniqueCurrencies.has(country.currencyCode)) {
          uniqueCurrencies.set(country.currencyCode, {
            code: country.currencyCode,
            name: country.currency,
            symbol: '',
            country: country.country
          });
        }
      }
    });
    
    console.log(`Se identificaron ${uniqueCurrencies.size} monedas únicas`);
    
    // 3. Verificar cuáles monedas ya existen en la colección currencies
    const existingCurrenciesSnapshot = await db.collection('currencies').get();
    const existingCurrencyCodes = new Set();
    
    existingCurrenciesSnapshot.forEach(doc => {
      const currency = doc.data();
      existingCurrencyCodes.add(currency.code);
    });
    
    console.log(`Ya existen ${existingCurrencyCodes.size} monedas en la colección "currencies"`);
    
    // 4. Filtrar solo las monedas que no existen
    const newCurrencies = Array.from(uniqueCurrencies.values())
      .filter(currency => !existingCurrencyCodes.has(currency.code));
    
    console.log(`Se procesarán ${newCurrencies.length} nuevas monedas`);
    
    // 5. Obtener tasas de cambio y guardar en Firestore
    const batch = db.batch();
    let successCount = 0;
    
    // Conjunto para almacenar códigos de moneda que no se pudieron consultar
    const failedCurrencyCodes = new Set();
    
    for (const currency of newCurrencies) {
      try {
        // Intentar obtener la tasa de cambio desde Yahoo Finance
        console.log(`Consultando tasa para ${currency.code} (${currency.name})...`);
        const exchangeRate = await getCurrencyRateFromYahoo(currency.code);
        
        // Si se obtuvo correctamente y tiene un valor actual
        if (exchangeRate && !isNaN(exchangeRate) && exchangeRate > 0) {
          // Crear un nuevo documento para la moneda
          const newCurrencyRef = db.collection('currencies').doc();
          
          const currencyObject = {
            code: currency.code,
            name: currency.name,
            symbol: '',
            exchangeRate: exchangeRate,
            isActive: true,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp()
          };
          
          batch.set(newCurrencyRef, currencyObject);
          successCount++;
          
          console.log(`✓ Moneda ${currency.code} (${currency.name}) lista para guardar con tasa: ${exchangeRate}`);
        } else {
          console.warn(`✗ Tasa inválida para ${currency.code}`);
          failedCurrencyCodes.add(currency.code);
        }
      } catch (error) {
        console.error(`✗ Error al procesar ${currency.code} (${currency.name}):`, error.message);
        failedCurrencyCodes.add(currency.code);
      }
    }
    
    // 6. Guardar los cambios de monedas en Firestore
    if (successCount > 0) {
      await batch.commit();
      console.log(`✅ Se guardaron ${successCount} nuevas monedas en Firestore`);
    } else {
      console.log('No se guardaron nuevas monedas en Firestore');
    }
    
    // 7. Eliminar países con monedas que no se pudieron consultar
    if (failedCurrencyCodes.size > 0) {
      const countriesToDelete = [];
      
      // Obtener todos los países asociados a las monedas fallidas
      failedCurrencyCodes.forEach(currencyCode => {
        const countries = currencyToCountries.get(currencyCode) || [];
        countriesToDelete.push(...countries);
      });
      
      console.log(`Se eliminarán ${countriesToDelete.length} países con monedas inválidas`);
      
      // Crear un nuevo batch para las eliminaciones (evitar mezclar operaciones)
      const deleteBatch = db.batch();
      
      countriesToDelete.forEach(country => {
        console.log(`Eliminando país: ${country.country} (${country.code2})`);
        const countryRef = db.collection('countries').doc(country.id);
        deleteBatch.delete(countryRef);
      });
      
      if (countriesToDelete.length > 0) {
        await deleteBatch.commit();
        console.log(`✅ Se eliminaron ${countriesToDelete.length} países con monedas inválidas`);
      }
    } else {
      console.log('Todos los países tienen monedas válidas');
    }
    
    return {
      currenciesAdded: successCount,
      countriesDeleted: failedCurrencyCodes.size > 0 ? 
        countriesToDelete?.length || 0 : 0
    };
  } catch (error) {
    console.error('🔥 Error general en la actualización de monedas:', error);
    throw error;
  }
}

// Ejecutar la función
updateCurrencyRatesFromExistingCountriesInYahoo()
  .then(result => {
    console.log(`Proceso completado. ${result.currenciesAdded} monedas actualizadas. ${result.countriesDeleted} países eliminados.`);
  })
  .catch(error => {
    console.error('Error en la ejecución:', error);
  });

module.exports = { updateCurrencyRatesFromExistingCountriesInYahoo }; 