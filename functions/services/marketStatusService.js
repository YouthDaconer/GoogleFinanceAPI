const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onRequest } = require("firebase-functions/v2/https");
const admin = require('firebase-admin');
const axios = require('axios');
const { DateTime } = require('luxon');
const { getCircuit } = require('../utils/circuitBreaker');
const { getCachedMarketStatus, cacheMarketStatus } = require('./cacheService');

// Cargar variables de entorno desde el archivo .env
require('dotenv').config();

// Asegurarse que admin esté inicializado
try {
  admin.app();
} catch (e) {
  admin.initializeApp();
}

// Token de Finnhub desde variables de entorno (Firebase Functions v2)
const FINNHUB_TOKEN = process.env.FINNHUB_TOKEN;

if (!FINNHUB_TOKEN) {
  console.error('No se ha configurado FINNHUB_TOKEN. Por favor agrega FINNHUB_TOKEN al archivo .env');
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
// WS-OPT-009: Optimizado para cubrir transiciones críticas del mercado
exports.scheduledMarketStatusUpdate = onSchedule({
  // Horarios estratégicos:
  // - 4:00 AM: Inicio pre-market
  // - 9:00 AM: 30 min antes de apertura (preparación)
  // - 9:30 AM: Apertura del mercado (CRÍTICO)
  // - 16:00 PM: Cierre del mercado (CRÍTICO)
  // - 20:00 PM: Fin de post-market
  schedule: '0 4,9,16,20 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 3,
  minBackoff: '1m',
}, async (event) => {
  console.log('Ejecutando actualización programada de estado del mercado');
  const result = await updateMarketStatus();
  console.log('Actualización completada:', result);
  return null;
});

// WS-OPT-009: Updates en minutos críticos de transición
exports.scheduledMarketStatusUpdateAdditional = onSchedule({
  // Minutos críticos:
  // - 9:30 AM: Apertura exacta del mercado
  // - 9:31 AM: Confirmación post-apertura (para clientes que cargaron justo antes)
  schedule: '30,31 9 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 3,
  minBackoff: '1m',
}, async (event) => {
  console.log('Ejecutando actualización de transición de estado del mercado');
  const result = await updateMarketStatus();
  console.log('Actualización de transición completada:', result);
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

/**
 * OPT-DEMAND-400-FIX: Sincronizar festivos de NYSE desde Finnhub
 * 
 * Esta función consulta el endpoint /stock/market-holiday de Finnhub
 * y guarda todos los festivos en la colección marketHolidays/US.
 * 
 * Se ejecuta mensualmente (1er día de cada mes a las 2:00 AM ET)
 * para mantener la lista de festivos actualizada.
 * 
 * @see https://finnhub.io/docs/api/market-holiday
 */
async function syncMarketHolidays() {
  const db = admin.firestore();
  const now = DateTime.now().setZone('America/New_York');
  
  try {
    console.log('🗓️ Sincronizando festivos de NYSE desde Finnhub...');
    
    const response = await axios.get(
      `https://finnhub.io/api/v1/stock/market-holiday?exchange=US&token=${FINNHUB_TOKEN}`
    );
    
    if (!response.data || !response.data.data) {
      console.error('❌ Respuesta inválida de Finnhub market-holiday');
      return { success: false, error: 'Invalid response' };
    }
    
    const holidays = response.data.data;
    
    // Crear un mapa de festivos por fecha para búsqueda rápida
    // Formato: { "2026-01-19": "Martin Luther King Jr. Day", ... }
    const holidayMap = {};
    const holidayList = [];
    
    for (const holiday of holidays) {
      if (holiday.atDate) {
        holidayMap[holiday.atDate] = holiday.eventName;
        holidayList.push({
          date: holiday.atDate,
          name: holiday.eventName,
          tradingHour: holiday.tradingHour || null // null = cerrado todo el día
        });
      }
    }
    
    // Guardar en Firestore
    const dataToSave = {
      exchange: 'US',
      holidays: holidayMap,
      holidayList: holidayList,
      totalHolidays: holidayList.length,
      lastSynced: admin.firestore.FieldValue.serverTimestamp(),
      syncedAt: now.toISO(),
      source: 'finnhub'
    };
    
    await db.collection('marketHolidays').doc('US').set(dataToSave, { merge: true });
    
    console.log(`✅ Festivos sincronizados: ${holidayList.length} días festivos guardados`);
    console.log(`📅 Próximos festivos:`, holidayList.slice(0, 5).map(h => `${h.date}: ${h.name}`));
    
    return { success: true, holidaysCount: holidayList.length };
    
  } catch (error) {
    console.error('❌ Error sincronizando festivos:', error.message);
    return { success: false, error: error.message };
  }
}

/**
 * Sincronización mensual de festivos de NYSE
 * Ejecuta el 1er día de cada mes a las 2:00 AM ET
 */
exports.scheduledHolidaySync = onSchedule({
  schedule: '0 2 1 * *', // 1er día de cada mes a las 2:00 AM
  timeZone: 'America/New_York',
  retryCount: 3,
  minBackoff: '5m',
}, async (event) => {
  console.log('🗓️ Ejecutando sincronización mensual de festivos de NYSE');
  const result = await syncMarketHolidays();
  console.log('Sincronización completada:', result);
  return null;
});

/**
 * Sincronización de festivos bajo demanda (HTTP)
 */
exports.syncHolidaysHttp = onRequest({
}, async (req, res) => {
  try {
    const result = await syncMarketHolidays();
    res.status(200).send({ success: true, message: 'Festivos sincronizados', ...result });
  } catch (error) {
    res.status(500).send({ success: false, error: error.message });
  }
});

// Exportar todas las funciones
module.exports = {
  updateMarketStatus,
  getMarketStatus,
  syncMarketHolidays,
  scheduledMarketStatusUpdate: exports.scheduledMarketStatusUpdate,
  scheduledMarketStatusUpdateAdditional: exports.scheduledMarketStatusUpdateAdditional,
  updateMarketStatusHttp: exports.updateMarketStatusHttp,
  scheduledHolidaySync: exports.scheduledHolidaySync,
  syncHolidaysHttp: exports.syncHolidaysHttp
};
