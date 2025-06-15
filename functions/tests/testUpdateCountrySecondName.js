const admin = require('../services/firebaseAdmin');
const fs = require('fs');
const path = require('path');

/**
 * Normaliza un ID eliminando ceros a la izquierda
 * @param {string} id - ID a normalizar
 * @returns {string} - ID normalizado
 */
function normalizeId(id) {
  // Convertir a string por si acaso
  const strId = String(id);
  // Eliminar ceros a la izquierda
  return strId.replace(/^0+/, '');
}

/**
 * Actualiza todos los países en la colección countries
 * agregando el campo name2 con el nombre del país según
 * el archivo world-countries.json, relacionando por codeNo e id
 */
async function updateCountrySecondName() {
  try {
    const db = admin.firestore();
    
    // 1. Obtener todos los países de Firestore
    const countriesSnapshot = await db.collection('countries').get();
    
    if (countriesSnapshot.empty) {
      console.log('No hay países en la colección "countries"');
      return 0;
    }
    
    console.log(`Se encontraron ${countriesSnapshot.size} países en Firestore`);
    
    // 2. Leer el archivo world-countries.json
    const worldCountriesPath = path.join(__dirname, 'world-countries.json');
    const worldCountriesData = JSON.parse(fs.readFileSync(worldCountriesPath, 'utf8'));
    
    // 3. Crear un mapa de ID a nombre de país del archivo JSON
    const idToName = new Map();
    
    // El archivo tiene una estructura de tipo GeoJSON, extraemos los datos necesarios
    if (worldCountriesData && worldCountriesData.objects && worldCountriesData.objects.countries) {
      const geometries = worldCountriesData.objects.countries.geometries;
      
      geometries.forEach(country => {
        if (country.id && country.properties && country.properties.name) {
          // Normalizar ID eliminando ceros a la izquierda antes de guardarlo en el mapa
          const normalizedId = normalizeId(country.id);
          idToName.set(normalizedId, country.properties.name);
        }
      });
      
      console.log(`Se extrajeron ${idToName.size} países del archivo JSON`);
    } else {
      console.error('Estructura del archivo JSON no reconocida');
      return 0;
    }
    
    // 4. Actualizar cada país en Firestore con el nombre del archivo JSON
    const batch = db.batch();
    let updatesCount = 0;
    let notFoundCount = 0;
    
    countriesSnapshot.forEach(doc => {
      const country = doc.data();
      
      // Verificar que existe el campo codeNo para relacionar
      if (country.codeNo) {
        // Normalizar el codeNo de Firebase antes de buscar en el mapa
        const normalizedCodeNo = normalizeId(country.codeNo);
        const name2 = idToName.get(normalizedCodeNo);
        
        if (name2) {
          batch.update(doc.ref, { name2 });
          updatesCount++;
          console.log(`✓ País ${country.country} (código: ${country.codeNo} → ${normalizedCodeNo}) - name2: ${name2}`);
        } else {
          notFoundCount++;
          console.warn(`✗ No se encontró nombre para ${country.country} (código: ${country.codeNo} → ${normalizedCodeNo})`);
        }
      } else {
        notFoundCount++;
        console.warn(`✗ País ${country.country} no tiene campo codeNo`);
      }
    });
    
    // 5. Guardar los cambios en Firestore
    if (updatesCount > 0) {
      await batch.commit();
      console.log(`✅ Se actualizaron ${updatesCount} países con el campo name2`);
    } else {
      console.log('No se actualizaron países');
    }
    
    console.log(`📊 Resumen: ${updatesCount} actualizados, ${notFoundCount} no encontrados`);
    
    return updatesCount;
  } catch (error) {
    console.error('🔥 Error al actualizar países con second name:', error);
    throw error;
  }
}

// Ejecutar la función
updateCountrySecondName()
  .then(count => {
    console.log(`Proceso completado. ${count} países actualizados con second name.`);
  })
  .catch(error => {
    console.error('Error en la ejecución:', error);
  });

module.exports = { updateCountrySecondName }; 