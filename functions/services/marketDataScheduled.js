/**
 * COST-OPT-004: Scheduled Functions Optimizadas para Datos de Mercado
 * 
 * Separa y optimiza la función original saveAllIndicesAndSectorsHistoryData:
 * 
 * ANTES: 1 función cada 10 min (48 ejecuciones/día)
 * AHORA: 2 funciones específicas con frecuencias optimizadas
 * 
 * - saveIndicesHistoryData: 2x/día (apertura y cierre de mercado)
 *   Solo guarda datos históricos de índices para gráficos
 * 
 * - saveSectorsSnapshot: 1x/día (solo cierre de mercado)
 *   Guarda snapshot histórico de rendimiento sectorial
 *   NOTA: El frontend ahora usa API directa, esto es solo para histórico
 * 
 * Beneficios:
 * - 96% menos ejecuciones para índices (48 → 2)
 * - 98% menos ejecuciones para sectores (48 → 1)
 * - Elimina writes redundantes (sobrescribía mismo documento)
 * 
 * @module services/marketDataScheduled
 * @see docs/architecture/firebase-cost-analysis-detailed.md
 * @see docs/architecture/SEC-CF-001-cloudflare-tunnel-migration-plan.md
 */

const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const admin = require('./firebaseAdmin');
const axios = require('axios');
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('./config');

/**
 * SEC-TOKEN-001: Secret para autenticación server-to-server con API finance-query
 * Usado por saveIndicesHistoryData y saveSectorsSnapshot.
 * 
 * @see docs/architecture/SEC-TOKEN-001-api-security-hardening-plan.md
 */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

// ============================================================================
// UTILIDADES
// ============================================================================

/**
 * Obtiene todos los índices del endpoint de finanzas
 * SEC-CF-001: Migrado a Cloudflare Tunnel
 * SEC-TOKEN-004: Incluye headers de autenticación de servicio
 * 
 * @returns {Promise<Array>} Array con todos los índices
 */
async function requestIndicesFromFinance() {
  try {
    const response = await axios.get(
      `${FINANCE_QUERY_API_URL}/indices`,
      { headers: getServiceHeaders() }
    );
    return response.data;
  } catch (error) {
    throw new Error(`Error fetching indices: ${error.message}`);
  }
}

/**
 * Normaliza un valor numérico desde string con formato (%, +, etc.)
 */
const normalizeNumber = (value) => {
  if (!value) return null;
  return parseFloat(value.replace(/[%,+]/g, ''));
};

/**
 * Mapeo de nombres de sectores a nombres ETF estándar
 */
const SECTOR_MAPPING = {
  "Technology": "INFORMATION TECHNOLOGY",
  "Consumer Cyclical": "CONSUMER DISCRETIONARY",
  "Communication Services": "COMMUNICATION SERVICES",
  "Financial Services": "FINANCIALS",
  "Healthcare": "HEALTH CARE",
  "Energy": "ENERGY",
  "Consumer Defensive": "CONSUMER STAPLES",
  "Basic Materials": "MATERIALS",
  "Industrials": "INDUSTRIALS",
  "Utilities": "UTILITIES",
  "Real Estate": "REAL ESTATE"
};

// ============================================================================
// SCHEDULED FUNCTION: saveIndicesHistoryData
// ============================================================================

/**
 * Guarda datos históricos de índices de mercado
 * 
 * Schedule: 2x/día - 9:35 AM y 4:35 PM ET
 * - 9:35: Captura valores de apertura (35 min después para estabilización)
 * - 16:35: Captura valores de cierre (5 min después del cierre)
 * 
 * Datos guardados:
 * - indexHistories/{code}: Información general (name, region)
 * - indexHistories/{code}/dates/{date}: Datos del día (score, change, percentChange)
 * 
 * @see docs/stories/14.story.md (OPT-009)
 */
const saveIndicesHistoryData = onSchedule({
  schedule: '35 9,16 * * 1-5', // 9:35 y 16:35 ET, lunes a viernes
  timeZone: 'America/New_York',
  retryCount: 2,
  memory: '256MiB',
  secrets: [cfServiceToken],  // SEC-TOKEN-001: Binding del secret para API auth
}, async (event) => {
  const startTime = Date.now();
  const formattedDate = new Date().toISOString().split('T')[0];
  const currentHour = new Date().toLocaleString('en-US', { 
    hour: 'numeric', 
    hour12: true, 
    timeZone: 'America/New_York' 
  });
  
  console.log(`[saveIndicesHistoryData] Iniciando captura de índices - ${formattedDate} ${currentHour}`);

  try {
    const indices = await requestIndicesFromFinance();
    
    if (!indices || indices.length === 0) {
      console.warn('[saveIndicesHistoryData] No se encontraron índices');
      return null;
    }

    const batch = admin.firestore().batch();
    let count = 0;

    indices.forEach(index => {
      // Documento principal con info general
      const generalDocRef = admin.firestore()
        .collection('indexHistories')
        .doc(index.code);

      batch.set(generalDocRef, {
        name: index.name,
        code: index.code,
        region: index.region,
        lastUpdated: Date.now()
      }, { merge: true });

      // Documento de fecha con datos del día
      const dateDocRef = generalDocRef.collection('dates').doc(formattedDate);
      
      batch.set(dateDocRef, {
        score: index.value,
        change: index.change,
        percentChange: normalizeNumber(index.percentChange),
        date: formattedDate,
        timestamp: Date.now(),
        captureType: currentHour.includes('9') ? 'open' : 'close'
      }, { merge: true });

      count++;
    });

    await batch.commit();
    
    const duration = Date.now() - startTime;
    console.log(`[saveIndicesHistoryData] Guardados ${count} índices en ${duration}ms`);

  } catch (error) {
    console.error('[saveIndicesHistoryData] Error:', error.message);
    throw error; // Re-throw para activar retry
  }

  return null;
});

// ============================================================================
// SCHEDULED FUNCTION: saveSectorsSnapshot
// ============================================================================

/**
 * Guarda snapshot histórico de rendimiento sectorial al cierre del mercado
 * 
 * Schedule: 1x/día - 4:35 PM ET (después del cierre)
 * 
 * NOTA: El frontend ya NO lee estos datos (usa API Lambda directa).
 * Esta función solo mantiene histórico para análisis futuro.
 * 
 * Datos guardados:
 * - sectors/{sectorName}: Datos actuales de rendimiento
 * - sectorsHistory/{date}: Snapshot del día (opcional, para análisis histórico)
 * 
 * @see docs/architecture/firebase-cost-analysis-detailed.md
 */
const saveSectorsSnapshot = onSchedule({
  schedule: '35 16 * * 1-5', // 16:35 ET, lunes a viernes (solo cierre)
  timeZone: 'America/New_York',
  retryCount: 2,
  memory: '256MiB',
  secrets: [cfServiceToken],  // SEC-TOKEN-001: Binding del secret para API auth
}, async (event) => {
  const startTime = Date.now();
  const formattedDate = new Date().toISOString().split('T')[0];
  
  console.log(`[saveSectorsSnapshot] Iniciando captura de sectores - ${formattedDate}`);

  // SEC-CF-001: URL via Cloudflare Tunnel
  // SEC-TOKEN-004: Incluir headers de autenticación
  try {
    const response = await axios.get(
      `${FINANCE_QUERY_API_URL}/sectors`,
      { headers: getServiceHeaders() }
    );
    const sectors = response.data;

    if (!sectors || sectors.length === 0) {
      console.warn('[saveSectorsSnapshot] No se encontraron sectores');
      return null;
    }

    const batch = admin.firestore().batch();
    let count = 0;

    // Objeto para snapshot histórico del día
    const dailySnapshot = {
      date: formattedDate,
      timestamp: Date.now(),
      sectors: {}
    };

    sectors.forEach(sector => {
      const etfSectorName = SECTOR_MAPPING[sector.sector] || sector.sector;

      const sectorData = {
        sector: sector.sector,
        etfSectorName: etfSectorName,
        dayReturn: normalizeNumber(sector.dayReturn),
        ytdReturn: normalizeNumber(sector.ytdReturn),
        yearReturn: normalizeNumber(sector.yearReturn),
        threeYearReturn: normalizeNumber(sector.threeYearReturn),
        fiveYearReturn: normalizeNumber(sector.fiveYearReturn),
        lastUpdated: Date.now()
      };

      // Documento actual del sector (para compatibilidad legacy)
      const sectorDocRef = admin.firestore()
        .collection('sectors')
        .doc(sector.sector);
      batch.set(sectorDocRef, sectorData, { merge: true });

      // Agregar al snapshot del día
      dailySnapshot.sectors[sector.sector] = sectorData;
      
      count++;
    });

    // Guardar snapshot histórico del día
    const historyDocRef = admin.firestore()
      .collection('sectorsHistory')
      .doc(formattedDate);
    batch.set(historyDocRef, dailySnapshot);

    await batch.commit();
    
    const duration = Date.now() - startTime;
    console.log(`[saveSectorsSnapshot] Guardados ${count} sectores en ${duration}ms`);

  } catch (error) {
    console.error('[saveSectorsSnapshot] Error:', error.message);
    throw error; // Re-throw para activar retry
  }

  return null;
});

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  saveIndicesHistoryData,
  saveSectorsSnapshot
};
