const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onRequest } = require("firebase-functions/v2/https");
const admin = require('firebase-admin');
const axios = require('axios');
const { DateTime } = require('luxon');
const functions = require('firebase-functions');
const { getCircuit } = require('../utils/circuitBreaker');
const { getCachedMarketStatus, cacheMarketStatus } = require('./cacheService');

// Cargar variables de entorno desde el archivo .env en desarrollo
if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// Asegurarse que admin esté inicializado
try {
  admin.app();
} catch (e) {
  admin.initializeApp();
}

// Token de Finnhub desde variables de entorno o configuración de Firebase
let FINNHUB_TOKEN;
try {
  FINNHUB_TOKEN = process.env.NODE_ENV !== 'production'
    ? process.env.FINNHUB_TOKEN
    : functions.config().finnhub?.token;
  
  if (!FINNHUB_TOKEN) {
    console.error('No se ha configurado FINNHUB_TOKEN. Por favor configúralo con firebase functions:config:set finnhub.token="TU_TOKEN"');
    // Usar un valor por defecto para evitar errores, aunque no funcionará correctamente
    FINNHUB_TOKEN = 'token_no_configurado';
  }
} catch (error) {
  console.error('Error al obtener FINNHUB_TOKEN:', error);
  FINNHUB_TOKEN = 'token_con_error';
}

// Documento donde almacenaremos el estado del mercado
const MARKET_DOC_ID = 'US';

// Circuit breaker for Finnhub API - less critical, use lower threshold
const finnhubCircuit = getCircuit('finnhub-market-status', {
  failureThreshold: 3,
  resetTimeout: 120000, // 2 minutes
});

/**
 * Consulta el estado del mercado desde Finnhub con circuit breaker
 */
async function fetchMarketStatus() {
  return finnhubCircuit.execute(
    async () => {
      console.log(`Consultando API Finnhub con token: ${FINNHUB_TOKEN.substring(0, 5)}...`);
      const response = await axios.get(
        `https://finnhub.io/api/v1/stock/market-status?exchange=US&token=${FINNHUB_TOKEN}`
      );
      
      // Cache successful response for future fallback
      await cacheMarketStatus(response.data);
      
      return response.data;
    },
    async () => {
      console.log('Circuit breaker active - using cached market status');
      return getCachedMarketStatus();
    }
  );
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
      const weekendData = {
        exchange: 'US',
        isOpen: false,
        session: 'closed',
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        dayOfWeek: dayOfWeek,
        isWeekend: true,
        timestamp: now.toISO()
      };
      
      await db.collection('markets').doc(MARKET_DOC_ID).set(weekendData, { merge: true });
      console.log('Fin de semana: mercado cerrado, no fue necesario consultar la API');
      return weekendData;
    }
    
    const marketStatus = await fetchMarketStatus();
    
    const dataToSave = {
      ...marketStatus,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dayOfWeek: dayOfWeek,
      isWeekend: false,
      timestamp: now.toISO()
    };
    
    await db.collection('markets').doc(MARKET_DOC_ID).set(dataToSave, { merge: true });
    console.log(`Estado del mercado actualizado: ${marketStatus.isOpen ? 'Abierto' : 'Cerrado'} (${marketStatus.session})`);
    return dataToSave;
    
  } catch (error) {
    console.error('Error al actualizar estado del mercado:', error);
    // En lugar de lanzar un error, devolver un status
    return { error: error.message, timestamp: now.toISO() };
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
exports.scheduledMarketStatusUpdate = onSchedule({
  schedule: '0 4,8,9,10,16,20,21 * * 1-5', // 4AM, 8AM, 9AM, 10AM, 4PM, 8PM, 9PM de lunes a viernes
  timeZone: 'America/New_York',
  retryCount: 3,
  minBackoff: '1m',
}, async (event) => {
  console.log('Ejecutando actualización programada de estado del mercado');
  const result = await updateMarketStatus();
  console.log('Actualización completada:', result);
  return null;
});

exports.scheduledMarketStatusUpdateAdditional = onSchedule({
  schedule: '30 8,9,16 * * 1-5', // 8:30AM, 9:30AM, 4:30PM de lunes a viernes
  timeZone: 'America/New_York',
  retryCount: 3,
  minBackoff: '1m',
}, async (event) => {
  console.log('Ejecutando actualización adicional programada de estado del mercado');
  const result = await updateMarketStatus();
  console.log('Actualización adicional completada:', result);
  return null;
});

// Actualización bajo demanda a través de HTTP
exports.updateMarketStatusHttp = onRequest({
}, async (req, res) => {
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
