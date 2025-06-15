const admin = require('../services/firebaseAdmin');
const axios = require('axios');

/**
 * Actualiza todas las monedas en la colección currencies
 * agregando el campo flagCurrency con la URL de la bandera
 * del país emisor de la moneda
 */
async function updateCurrencyFlags() {
  try {
    const db = admin.firestore();
    
    // 1. Obtener todas las monedas de Firestore
    const currenciesSnapshot = await db.collection('currencies').get();
    
    if (currenciesSnapshot.empty) {
      console.log('No hay monedas en la colección "currencies"');
      return 0;
    }
    
    console.log(`Se encontraron ${currenciesSnapshot.size} monedas en Firestore`);
    
    // 2. Obtener todos los países para buscar las banderas correspondientes
    const countriesSnapshot = await db.collection('countries').get();
    
    // Crear un mapa de códigos de moneda a países
    const currencyCodeToCountry = new Map();
    
    countriesSnapshot.forEach(doc => {
      const country = doc.data();
      if (country.currencyCode && country.code2) {
        // En caso de que varios países usen la misma moneda, nos quedamos con el primero
        if (!currencyCodeToCountry.has(country.currencyCode)) {
          currencyCodeToCountry.set(country.currencyCode, {
            code2: country.code2,
            country: country.country,
            flagUrl: country.flagUrl || `https://flagcdn.com/${country.code2.toLowerCase()}.svg`
          });
        }
      }
    });
    
    console.log(`Mapa de monedas a países creado con ${currencyCodeToCountry.size} entradas`);
    
    // 3. Actualizar cada moneda con la URL de su bandera
    const batch = db.batch();
    let updatesCount = 0;
    
    currenciesSnapshot.forEach(doc => {
      const currency = doc.data();
      let flagUrl = null;
      
      // Casos especiales
      if (currency.code === 'USD') {
        flagUrl = 'https://flagcdn.com/us.svg';
      } else if (currency.code === 'EUR') {
        flagUrl = 'https://flagcdn.com/eu.svg';
      } else {
        // Buscar el país correspondiente a la moneda
        const countryInfo = currencyCodeToCountry.get(currency.code);
        if (countryInfo) {
          flagUrl = countryInfo.flagUrl;
        }
      }
      
      // Solo actualizar si se encontró una bandera
      if (flagUrl) {
        batch.update(doc.ref, { flagCurrency: flagUrl });
        updatesCount++;
        console.log(`✓ Moneda ${currency.code} (${currency.name}) - Flag: ${flagUrl}`);
      } else {
        console.warn(`✗ No se encontró bandera para ${currency.code} (${currency.name})`);
      }
    });
    
    // 4. Guardar los cambios en Firestore
    if (updatesCount > 0) {
      await batch.commit();
      console.log(`✅ Se actualizaron ${updatesCount} monedas con sus banderas`);
    } else {
      console.log('No se actualizaron monedas');
    }
    
    return updatesCount;
  } catch (error) {
    console.error('🔥 Error al actualizar banderas de monedas:', error);
    throw error;
  }
}

// Verificar si una URL existe (devuelve 200)
async function urlExists(url) {
  try {
    const response = await axios.head(url);
    return response.status === 200;
  } catch (error) {
    return false;
  }
}

// Ejecutar la función
updateCurrencyFlags()
  .then(count => {
    console.log(`Proceso completado. ${count} monedas actualizadas con sus banderas.`);
  })
  .catch(error => {
    console.error('Error en la ejecución:', error);
  });

module.exports = { updateCurrencyFlags }; 