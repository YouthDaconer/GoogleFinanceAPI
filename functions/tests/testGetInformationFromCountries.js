const admin = require('../services/firebaseAdmin');
const axios = require('axios');

/**
 * Obtiene información de países desde Finnhub y guarda en Firestore los países
 * que tienen región, moneda y código de moneda válidos y bandera disponible
 * @param {string} token - Token de API de Finnhub
 */
async function getInformationFromCountries(token) {
  try {
    // Obtener datos de la API de Finnhub
    const response = await axios.get(`https://finnhub.io/api/v1/country?token=${token}`);
    const countries = response.data;
    
    // Filtrar países que cumplen con los criterios básicos
    const countriesWithBasicInfo = countries.filter(country => 
      country.region !== "" && 
      country.currency !== "" && 
      country.currencyCode !== ""
    );
    
    console.log(`Total países recibidos: ${countries.length}`);
    console.log(`Países con información básica: ${countriesWithBasicInfo.length}`);
    
    // Verificar banderas disponibles
    const validCountries = [];
    for (const country of countriesWithBasicInfo) {
      try {
        // Verificar que la bandera existe
        const flagUrl = `https://flagcdn.com/${country.code2.toLowerCase()}.svg`;
        const flagResponse = await axios.head(flagUrl);
        
        // Si la bandera existe, agregar a lista de países válidos
        if (flagResponse.status === 200) {
          // Agregar la URL de la bandera al objeto del país
          country.flagUrl = flagUrl;
          validCountries.push(country);
        } else {
          console.log(`País ${country.country} (${country.code2}) no tiene bandera válida`);
        }
      } catch (flagError) {
        // Si hay error (404 u otro), la bandera no existe
        console.log(`País ${country.country} (${country.code2}) no tiene bandera válida: ${flagError.message}`);
      }
    }
    
    console.log(`Países con bandera válida: ${validCountries.length}`);
    
    // Guardar en Firestore
    const db = admin.firestore();
    const batch = db.batch();
    
    validCountries.forEach(country => {
      const countryRef = db.collection('countries').doc(country.code2);
      batch.set(countryRef, country);
    });
    
    await batch.commit();
    console.log(`✅ ${validCountries.length} países guardados en Firestore`);
    
    return validCountries;
  } catch (error) {
    console.error("🔥 Error al obtener/guardar información de países:", error);
    throw error;
  }
}

// Ejemplo de uso (comentado para evitar ejecución accidental)
getInformationFromCountries("cuf2kipr01qno7m4pgk0cuf2kipr01qno7m4pgkg")
  .catch(error => console.error("Error en la ejecución:", error));

module.exports = { getInformationFromCountries }; 