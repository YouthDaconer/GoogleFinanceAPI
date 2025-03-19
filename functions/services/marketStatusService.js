// services/marketStatusService.js
const functions = require('firebase-functions');
const admin = require('firebase-admin');
const axios = require('axios');
const { DateTime } = require('luxon');

// Token de Finnhub
const FINNHUB_TOKEN = process.env.FINNHUB_TOKEN || 
  (process.env.FUNCTIONS_EMULATOR ? 
    require('dotenv').config().parsed.FINNHUB_TOKEN : 
    functions.config().finnhub.token);

// Documento donde almacenaremos el estado del mercado
const MARKET_DOC_ID = 'US';

/**
 * Consulta el estado del mercado desde Finnhub
 */
async function fetchMarketStatus() {
  try {
    const response = await axios.get(`https://finnhub.io/api/v1/stock/market-status?exchange=US&token=${FINNHUB_TOKEN}`);
    return response.data;
  } catch (error) {
    console.error('Error al consultar estado del mercado:', error);
    throw error;
  }
}

/**
 * Actualiza el estado del mercado en Firestore
 */
async function updateMarketStatus() {
  const db = admin.firestore();
  const now = DateTime.now().setZone('America/New_York');
  
  try {
    // Verificar si es fin de semana (sábado=6, domingo=7)
    const dayOfWeek = now.weekday;
    const isWeekend = dayOfWeek === 6 || dayOfWeek === 7;
    
    // Si es fin de semana, no consultamos la API y actualizamos directamente
    if (isWeekend) {
      await db.collection('markets').doc(MARKET_DOC_ID).set({
        exchange: 'US',
        isOpen: false,
        session: 'closed',
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        dayOfWeek: dayOfWeek,
        isWeekend: true,
        timestamp: now.toISO()
      }, { merge: true });
      
      console.log('Fin de semana: mercado cerrado, no fue necesario consultar la API');
      return;
    }
    
    // Consultar estado desde la API
    const marketStatus = await fetchMarketStatus();
    
    // Guardar en Firestore con metadatos adicionales
    await db.collection('markets').doc(MARKET_DOC_ID).set({
      ...marketStatus,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dayOfWeek: dayOfWeek,
      isWeekend: false,
      timestamp: now.toISO()
    }, { merge: true });
    
    console.log(`Estado del mercado actualizado: ${marketStatus.isOpen ? 'Abierto' : 'Cerrado'} (${marketStatus.session})`);
    
  } catch (error) {
    console.error('Error al actualizar estado del mercado:', error);
    throw error;
  }
}

/**
 * Obtiene el estado actual del mercado
 */
async function getMarketStatus() {
  const db = admin.firestore();
  
  try {
    const doc = await db.collection('markets').doc(MARKET_DOC_ID).get();
    
    if (!doc.exists) {
      // Si no existe el documento, realizar consulta inicial
      await updateMarketStatus();
      return (await db.collection('markets').doc(MARKET_DOC_ID).get()).data();
    }
    
    const data = doc.data();
    const lastUpdated = data.lastUpdated?.toDate() || new Date(0);
    const now = new Date();
    
    // Verificar si los datos son recientes (menos de 30 minutos)
    const diffMinutes = (now - lastUpdated) / (1000 * 60);
    
    if (diffMinutes > 30) {
      // Si los datos son antiguos, actualizar
      await updateMarketStatus();
      return (await db.collection('markets').doc(MARKET_DOC_ID).get()).data();
    }
    
    return data;
  } catch (error) {
    console.error('Error al obtener estado del mercado:', error);
    throw error;
  }
}

// Programar la función para ejecutarse en momentos estratégicos
exports.scheduledMarketStatusUpdate = functions.pubsub
  .schedule('0 4,8,9,10,16,20,21 * * 1-5') // 4AM, 9AM, 4PM, 8PM de lunes a viernes
  .timeZone('America/New_York')
  .onRun(async () => {
    await updateMarketStatus();
    return null;
  });

// Actualización bajo demanda a través de HTTP
exports.updateMarketStatusHttp = functions.https.onRequest(async (req, res) => {
  try {
    await updateMarketStatus();
    res.status(200).send({ success: true, message: 'Estado del mercado actualizado' });
  } catch (error) {
    res.status(500).send({ success: false, error: error.message });
  }
});

// Exportar todas las funciones al final
module.exports = {
  updateMarketStatus,
  getMarketStatus,
  scheduledMarketStatusUpdate: exports.scheduledMarketStatusUpdate,
  updateMarketStatusHttp: exports.updateMarketStatusHttp
};