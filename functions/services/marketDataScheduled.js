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
// OPT-CACHE-001: Importar función para invalidar cache de índices
const { invalidateAllIndexCaches } = require('./indexHistoryService');

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
 * FIX-INDEX-002: Lista de índices clave para fallback cuando /indices falla
 * Símbolos compatibles con Yahoo Finance /quotes endpoint
 */
const KEY_INDEX_SYMBOLS = [
  { symbol: '^GSPC', code: 'GSPC', name: 'S&P 500', region: 'US' },
  { symbol: '^DJI', code: 'DJI', name: 'Dow Jones Industrial Average', region: 'US' },
  { symbol: '^IXIC', code: 'IXIC', name: 'NASDAQ Composite', region: 'US' },
  { symbol: '^RUT', code: 'RUT', name: 'Russell 2000', region: 'US' },
  { symbol: '^VIX', code: 'VIX', name: 'CBOE Volatility Index', region: 'US' },
  { symbol: '^NYA', code: 'NYA', name: 'NYSE Composite', region: 'US' },
  { symbol: '^FTSE', code: 'FTSE', name: 'FTSE 100', region: 'UK' },
  { symbol: '^GDAXI', code: 'GDAXI', name: 'DAX Performance Index', region: 'DE' },
  { symbol: '^FCHI', code: 'FCHI', name: 'CAC 40', region: 'FR' },
  { symbol: '^N225', code: 'N225', name: 'Nikkei 225', region: 'JP' },
  { symbol: '^HSI', code: 'HSI', name: 'Hang Seng Index', region: 'HK' },
];

/**
 * FIX-INDEX-002: Fallback - obtiene índices via /quotes cuando /indices falla
 * El endpoint /indices usa scraping de Yahoo Finance que puede fallar.
 * El endpoint /quotes es más estable y proporciona datos equivalentes.
 * 
 * @returns {Promise<Array>} Array con índices en formato compatible
 */
async function requestIndicesViaQuotes() {
  const symbols = KEY_INDEX_SYMBOLS.map(i => i.symbol).join(',');
  
  try {
    const response = await axios.get(
      `${FINANCE_QUERY_API_URL}/quotes`,
      { 
        headers: getServiceHeaders(),
        params: { symbols }
      }
    );
    
    const quotes = response.data || [];
    
    // Transformar formato de /quotes al formato de /indices
    return quotes.map(quote => {
      const indexInfo = KEY_INDEX_SYMBOLS.find(i => i.symbol === quote.symbol) || {};
      
      // Parsear valores - pueden venir con comas y símbolos
      const parseValue = (val) => {
        if (!val) return 0;
        const str = String(val).replace(/[,%$+]/g, '');
        return parseFloat(str) || 0;
      };
      
      return {
        code: indexInfo.code || quote.symbol.replace('^', ''),
        name: indexInfo.name || quote.name || quote.symbol,
        region: indexInfo.region || 'US',
        value: parseValue(quote.price),
        change: parseValue(quote.change),
        percentChange: quote.percentChange || '0%',
      };
    });
  } catch (error) {
    throw new Error(`Error fetching indices via quotes: ${error.message}`);
  }
}

/**
 * Obtiene todos los índices del endpoint de finanzas
 * SEC-CF-001: Migrado a Cloudflare Tunnel
 * SEC-TOKEN-004: Incluye headers de autenticación de servicio
 * FIX-INDEX-002: Fallback a /quotes si /indices falla
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
    console.warn(`[requestIndicesFromFinance] /indices falló: ${error.message}, usando fallback /quotes`);
    
    // FIX-INDEX-002: Usar endpoint /quotes como fallback
    try {
      const fallbackData = await requestIndicesViaQuotes();
      console.log(`[requestIndicesFromFinance] Fallback exitoso: ${fallbackData.length} índices obtenidos via /quotes`);
      return fallbackData;
    } catch (fallbackError) {
      throw new Error(`Error fetching indices (primary + fallback failed): ${error.message} | ${fallbackError.message}`);
    }
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
// INTERNAL: saveIndicesHistoryDataInternal
// OPT-SNAP-INCR Fase 3: Core logic extracted for pipeline consolidation.
// Can be called standalone (scheduled) or as a step within unifiedMarketDataUpdate.
// ============================================================================

/**
 * Persiste los datos de cierre de índices bursátiles para una fecha dada.
 *
 * @param {Object} params
 * @param {string} params.formattedDate - Fecha del trading day (YYYY-MM-DD)
 * @param {boolean} [params.skipCacheInvalidation=false] - Skip invalidation when refreshIndexCache runs after
 * @returns {Promise<{success: boolean, count: number, durationMs: number}>}
 */
async function saveIndicesHistoryDataInternal({ formattedDate, skipCacheInvalidation = false }) {
  const startTime = Date.now();
  const db = admin.firestore();

  const indices = await requestIndicesFromFinance();

  if (!indices || indices.length === 0) {
    console.warn('[saveIndicesHistoryDataInternal] No se encontraron índices');
    return { success: false, count: 0, durationMs: Date.now() - startTime };
  }

  const batch = db.batch();
  let count = 0;

  indices.forEach(index => {
    const generalDocRef = db.collection('indexHistories').doc(index.code);

    batch.set(generalDocRef, {
      name: index.name,
      code: index.code,
      region: index.region,
      lastUpdated: Date.now()
    }, { merge: true });

    const dateDocRef = generalDocRef.collection('dates').doc(formattedDate);

    batch.set(dateDocRef, {
      score: index.value,
      change: index.change,
      percentChange: normalizeNumber(index.percentChange),
      date: formattedDate,
      timestamp: Date.now(),
      captureType: 'close'
    }, { merge: true });

    count++;
  });

  // OPT-FS-202: Documento resumen consolidado
  const summaryDocRef = db.collection('indexHistories').doc('_summary');
  batch.set(summaryDocRef, {
    indices: indices.map(index => ({
      code: index.code,
      name: index.name,
      region: index.region,
      score: index.value,
      change: index.change,
      percentChange: normalizeNumber(index.percentChange),
    })),
    date: formattedDate,
    lastUpdated: Date.now(),
  });

  await batch.commit();

  // OPT-CACHE-001: Invalidar cache de índices (skip when refreshIndexCache runs immediately after)
  if (!skipCacheInvalidation) {
    try {
      const invalidatedCount = await invalidateAllIndexCaches();
      console.log(`[saveIndicesHistoryDataInternal] Cache invalidado: ${invalidatedCount} documentos`);
    } catch (cacheError) {
      console.warn(`[saveIndicesHistoryDataInternal] Error invalidando cache: ${cacheError.message}`);
    }
  }

  const durationMs = Date.now() - startTime;
  console.log(`[saveIndicesHistoryDataInternal] ✅ Guardados ${count} índices + summary para ${formattedDate} en ${durationMs}ms`);

  return { success: true, count, durationMs };
}

// ============================================================================
// SCHEDULED FUNCTION: saveIndicesHistoryData
// ============================================================================

/**
 * Festivos NYSE — misma lista usada por unifiedMarketDataUpdate
 * Sirve como fallback cuando marketHolidays/US no está disponible en Firestore.
 */
const NYSE_HOLIDAYS_FALLBACK = new Set([
  // 2025
  '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26',
  '2025-06-19', '2025-07-04', '2025-09-01', '2025-11-27', '2025-12-25',
  // 2026
  '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25',
  '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
  // 2027
  '2027-01-01', '2027-01-18', '2027-02-15', '2027-03-26', '2027-05-31',
  '2027-06-18', '2027-07-05', '2027-09-06', '2027-11-25', '2027-12-24',
]);

/**
 * Verifica si una fecha es un día de trading válido de NYSE.
 * Consulta primero marketHolidays/US (Finnhub), luego fallback estático.
 * Misma lógica que unifiedMarketDataUpdate.isValidTradingDay.
 * 
 * @param {string} dateStr - Fecha en formato YYYY-MM-DD
 * @returns {Promise<{isValid: boolean, reason: string}>}
 */
async function isValidTradingDayForIndices(dateStr) {
  const d = new Date(dateStr + 'T12:00:00Z');
  const dayOfWeek = d.getUTCDay();
  
  // Fin de semana
  if (dayOfWeek === 0 || dayOfWeek === 6) {
    return { isValid: false, reason: dayOfWeek === 0 ? 'sunday' : 'saturday' };
  }
  
  // Verificar festivo en marketHolidays/US (fuente principal)
  try {
    const holidaysDoc = await admin.firestore().collection('marketHolidays').doc('US').get();
    if (holidaysDoc.exists) {
      const holidaysData = holidaysDoc.data();
      if (holidaysData.holidays && holidaysData.holidays[dateStr]) {
        return { isValid: false, reason: `holiday: ${holidaysData.holidays[dateStr]}` };
      }
      return { isValid: true, reason: 'trading-day' };
    }
  } catch (err) {
    console.warn(`[saveIndicesHistoryData] Error consultando marketHolidays: ${err.message}, usando fallback`);
  }
  
  // Fallback estático
  if (NYSE_HOLIDAYS_FALLBACK.has(dateStr)) {
    return { isValid: false, reason: 'holiday-fallback' };
  }
  
  return { isValid: true, reason: 'trading-day' };
}

/**
 * Guarda datos históricos de índices de mercado
 * 
 * FIX-INDEX-001: Alineado con la temporalidad de unifiedMarketDataUpdate
 * 
 * ANTES: 2x/día a las 9:35 y 16:35 ET (L-V) — causaba datos intraday
 *        cuando la ejecución de las 16:35 fallaba, quedaban precios de apertura
 * 
 * AHORA: 1x/día a las 00:10 ET (Ma-Sáb) — después de medianoche
 *        Guarda datos del DÍA ANTERIOR (igual que unifiedMarketDataUpdate)
 *        Valida festivos/fines de semana antes de escribir
 * 
 * Schedule: 00:10 ET Martes-Sábado (cubre trading days Lunes-Viernes)
 * Se ejecuta 5 min DESPUÉS de unifiedMarketDataUpdate (00:05) para no competir.
 * 
 * Datos guardados:
 * - indexHistories/{code}: Información general (name, region)
 * - indexHistories/{code}/dates/{date}: Datos del día (score, change, percentChange)
 * 
 * @see docs/stories/14.story.md (OPT-009)
 * @see unifiedMarketDataUpdate.js (función de referencia para temporalidad)
 */
const saveIndicesHistoryData = onSchedule({
  // FIX-INDEX-001: 00:10 ET Martes-Sábado = datos de cierre definitivos del día anterior
  // OPT-SNAP-INCR Fase 3: Kept as standalone fallback. Primary execution now via unifiedMarketDataUpdate.
  schedule: '10 0 * * 2-6',
  timeZone: 'America/New_York',
  retryCount: 2,
  memory: '256MiB',
  secrets: [cfServiceToken],  // SEC-TOKEN-001: Binding del secret para API auth
  labels: {
    status: 'active',
    purpose: 'index-history-eod-fallback',
    updated: '2026-04-30'
  }
}, async (event) => {
  // FIX-INDEX-001: Calcular la fecha del DÍA ANTERIOR (el trading day que cerró)
  const now = new Date();
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  const formattedDate = yesterday.toISOString().split('T')[0];

  console.log(`[saveIndicesHistoryData] Standalone execution - target: ${formattedDate}`);

  // Validar trading day
  const tradingDayCheck = await isValidTradingDayForIndices(formattedDate);
  if (!tradingDayCheck.isValid) {
    console.log(`[saveIndicesHistoryData] ⏭️ Saltando: ${formattedDate} no es día de trading (${tradingDayCheck.reason})`);
    return null;
  }

  try {
    await saveIndicesHistoryDataInternal({ formattedDate });
  } catch (error) {
    console.error('[saveIndicesHistoryData] Error:', error.message);
    throw error;
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
// SCHEDULED FUNCTION: updateRiskFreeRate
// ============================================================================

/**
 * Actualiza la tasa libre de riesgo desde ^IRX (13-Week T-Bill) y ^TNX (10Y Treasury)
 * 
 * Schedule: 1x/día — 17:00 ET (después del cierre, Lunes-Viernes)
 * Escribe en: benchmarks/risk_free_rate
 * 
 * Esto asegura que getDynamicRiskFreeRate() en benchmarkCache.js
 * encuentre datos frescos en Firestore sin necesidad de llamar a la API.
 * 
 * @see docs/architecture/RISK-METRICS-DYNAMIC-BENCHMARKS-analysis.md
 */
const updateRiskFreeRate = onSchedule({
  schedule: '0 17 * * 1-5',
  timeZone: 'America/New_York',
  retryCount: 2,
  memory: '256MiB',
  secrets: [cfServiceToken],
  labels: {
    status: 'active',
    purpose: 'risk-free-rate-update',
    updated: '2026-03-26'
  }
}, async (event) => {
  const startTime = Date.now();
  console.log('[updateRiskFreeRate] Starting daily update');
  
  try {
    const response = await axios.get(
      `${FINANCE_QUERY_API_URL}/quotes`,
      {
        headers: getServiceHeaders(),
        params: { symbols: '^IRX,^TNX' },
        timeout: 15000
      }
    );
    
    const quotes = response.data || [];
    const irx = quotes.find(q => q.symbol === '^IRX');
    const tnx = quotes.find(q => q.symbol === '^TNX');
    
    // Preferir ^IRX (T-Bill 13 semanas), fallback a ^TNX (Treasury 10Y)
    const source = irx || tnx;
    if (!source || !source.price) {
      console.warn('[updateRiskFreeRate] No rate data from ^IRX or ^TNX');
      return null;
    }
    
    const priceStr = String(source.price).replace(/[%,+]/g, '');
    const ratePercent = parseFloat(priceStr);
    
    if (isNaN(ratePercent) || ratePercent <= 0 || ratePercent >= 20) {
      console.warn(`[updateRiskFreeRate] Invalid rate: ${source.price} → ${ratePercent}`);
      return null;
    }
    
    const rate = ratePercent / 100;
    
    await admin.firestore().collection('benchmarks').doc('risk_free_rate').set({
      rate,
      ratePercent,
      source: irx ? 'IRX' : 'TNX',
      symbol: source.symbol,
      rawPrice: source.price,
      updatedAt: Date.now(),
      updatedDate: new Date().toISOString().split('T')[0],
      description: irx ? '13-Week Treasury Bill Yield' : '10-Year Treasury Note Yield'
    });
    
    const duration = Date.now() - startTime;
    console.log(`[updateRiskFreeRate] ✅ Updated: ${ratePercent.toFixed(2)}% from ${source.symbol} in ${duration}ms`);
  } catch (error) {
    console.error('[updateRiskFreeRate] Error:', error.message);
  }
  
  return null;
});

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  saveIndicesHistoryData,
  saveIndicesHistoryDataInternal,
  saveSectorsSnapshot,
  updateRiskFreeRate
};
