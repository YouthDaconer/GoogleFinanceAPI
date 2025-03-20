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
    const dayOfWeek = now.weekday;
    const isWeekend = dayOfWeek === 6 || dayOfWeek === 7;
    
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
    
    const marketStatus = await fetchMarketStatus();
    
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
      await updateMarketStatus();
      return (await db.collection('markets').doc(MARKET_DOC_ID).get()).data();
    }
    
    const data = doc.data();
    const lastUpdated = data.lastUpdated?.toDate() || new Date(0);
    const now = new Date();
    
    const diffMinutes = (now - lastUpdated) / (1000 * 60);
    
    if (diffMinutes > 30) {
      await updateMarketStatus();
      return (await db.collection('markets').doc(MARKET_DOC_ID).get()).data();
    }
    
    return data;
  } catch (error) {
    console.error('Error al obtener estado del mercado:', error);
    throw error;
  }
}

// Programar las funciones para ejecutarse en los horarios definidos
exports.scheduledMarketStatusUpdate = functions.pubsub
  .schedule('0 4,8,9,10,16,20,21 * * 1-5') // 4AM, 8AM, 9AM, 10AM, 4PM, 8PM, 9PM de lunes a viernes
  .timeZone('America/New_York')
  .onRun(async () => {
    await updateMarketStatus();
    return null;
  });

exports.scheduledMarketStatusUpdateAdditional = functions.pubsub
  .schedule('30 8,9,16 * * 1-5') // 8:30AM, 9:30AM, 4:30PM de lunes a viernes
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

// Exportar todas las funciones
module.exports = {
  updateMarketStatus,
  getMarketStatus,
  scheduledMarketStatusUpdate: exports.scheduledMarketStatusUpdate,
  scheduledMarketStatusUpdateAdditional: exports.scheduledMarketStatusUpdateAdditional,
  updateMarketStatusHttp: exports.updateMarketStatusHttp
};
