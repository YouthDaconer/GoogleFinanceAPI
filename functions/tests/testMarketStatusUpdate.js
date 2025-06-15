// functions/services/testMarketStatusUpdate.js
const admin = require('../services/firebaseAdmin');
const { DateTime } = require('luxon');
const axios = require('axios');

// Cargar variables de entorno desde el archivo .env
require('dotenv').config();

// Token de Finnhub desde variables de entorno
const FINNHUB_TOKEN = process.env.FINNHUB_TOKEN;

// Documento donde almacenaremos el estado del mercado
const MARKET_DOC_ID = 'US';

/**
 * Consulta el estado del mercado desde Finnhub
 */
async function fetchMarketStatus() {
  try {
    console.log('Consultando API de Finnhub para obtener estado del mercado...');
    const response = await axios.get(`https://finnhub.io/api/v1/stock/market-status?exchange=US&token=${FINNHUB_TOKEN}`);
    console.log('Respuesta recibida:', JSON.stringify(response.data, null, 2));
    return response.data;
  } catch (error) {
    console.error('Error al consultar estado del mercado:', error);
    throw error;
  }
}

/**
 * Actualiza el estado del mercado en Firestore
 */
async function updateMarketStatus(forceUpdate = false) {
  const db = admin.firestore();
  const now = DateTime.now().setZone('America/New_York');
  
  try {
    // Verificar si es fin de semana (sábado=6, domingo=7)
    const dayOfWeek = now.weekday;
    const isWeekend = dayOfWeek === 6 || dayOfWeek === 7;
    
    // Si es fin de semana y no se fuerza actualización, no consultamos la API
    if (isWeekend && !forceUpdate) {
      console.log('Es fin de semana, el mercado está cerrado');
      
      await db.collection('markets').doc(MARKET_DOC_ID).set({
        exchange: 'US',
        isOpen: false,
        session: 'closed',
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        dayOfWeek: dayOfWeek,
        isWeekend: true,
        timestamp: now.toISO()
      }, { merge: true });
      
      console.log('Registro actualizado en Firestore (datos de fin de semana)');
      return {
        exchange: 'US',
        isOpen: false,
        session: 'closed',
        dayOfWeek: dayOfWeek,
        isWeekend: true,
        timestamp: now.toISO()
      };
    }
    
    // Consultar estado desde la API
    const marketStatus = await fetchMarketStatus();
    
    // Agregar datos adicionales
    const dataToSave = {
      ...marketStatus,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dayOfWeek: dayOfWeek,
      isWeekend: isWeekend,
      timestamp: now.toISO()
    };
    
    // Guardar en Firestore
    await db.collection('markets').doc(MARKET_DOC_ID).set(dataToSave, { merge: true });
    
    console.log(`Estado del mercado actualizado en Firestore: ${marketStatus.isOpen ? 'Abierto' : 'Cerrado'} (${marketStatus.session})`);
    
    // Retornar para propósitos de prueba
    return dataToSave;
    
  } catch (error) {
    console.error('Error al actualizar estado del mercado:', error);
    throw error;
  }
}

/**
 * Obtiene el estado actual del mercado de Firestore
 */
async function getMarketStatusFromFirestore() {
  const db = admin.firestore();
  
  try {
    const doc = await db.collection('markets').doc(MARKET_DOC_ID).get();
    
    if (!doc.exists) {
      console.log('No existe documento de estado del mercado en Firestore');
      return null;
    }
    
    console.log('Estado del mercado encontrado en Firestore:', JSON.stringify(doc.data(), null, 2));
    return doc.data();
  } catch (error) {
    console.error('Error al obtener estado del mercado desde Firestore:', error);
    throw error;
  }
}

/**
 * Función principal de prueba
 */
async function testMarketStatusUpdate() {
  try {
    console.log('=== Prueba de Servicio de Estado del Mercado ===');
    
    // 1. Verificar si ya existe información en Firestore
    console.log('\n1. Verificando datos existentes en Firestore...');
    const existingData = await getMarketStatusFromFirestore();
    
    if (existingData) {
      console.log('Datos encontrados. Última actualización:', 
        existingData.lastUpdated ? new Date(existingData.lastUpdated.toDate()).toISOString() : 'N/A');
    } else {
      console.log('No se encontraron datos previos.');
    }
    
    // 2. Actualizar estado del mercado (forzando actualización)
    console.log('\n2. Actualizando estado del mercado...');
    const updatedData = await updateMarketStatus(true);
    console.log('Estado actualizado:', JSON.stringify(updatedData, null, 2));
    
    // 3. Verificar los datos actualizados
    console.log('\n3. Verificando datos después de actualización...');
    const newData = await getMarketStatusFromFirestore();
    
    if (newData) {
      console.log('Estado del mercado actual:', 
        `${newData.isOpen ? 'ABIERTO' : 'CERRADO'} (${newData.session})`);
      console.log('Horario:', newData.timezone);
      console.log('Timestamp:', newData.timestamp);
    }
    
    console.log('\n=== Prueba completada ===');
    return newData;
  } catch (error) {
    console.error('Error en prueba de estado del mercado:', error);
    throw error;
  }
}

// Auto-ejecutar la prueba
testMarketStatusUpdate()
  .then(() => {
    console.log('Proceso de prueba finalizado exitosamente.');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error en proceso de prueba:', error);
    process.exit(1);
  });

module.exports = {
  testMarketStatusUpdate
};