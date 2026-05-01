/**
 * Index History Service - OPT-009
 * 
 * Cloud Functions para pre-cálculo y cache de datos históricos de índices de mercado.
 * Reduce lecturas de Firestore de ~1,300 a 1 por consulta usando cache global.
 * 
 * @module indexHistoryService
 * @see docs/stories/14.story.md
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require('./firebaseAdmin');
const db = admin.firestore();
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('./config');

// Importar rate limiter (SCALE-BE-004)
const { withRateLimit } = require('../utils/rateLimiter');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const callableConfig = {
  cors: true,
  region: "us-central1",
  maxInstances: 10,
  memory: "256MiB",
  timeoutSeconds: 60,
};

const VALID_RANGES = ["1M", "3M", "6M", "YTD", "1Y", "5Y", "MAX"];
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 horas (histórico)
// OPT-FIRESTORE-002: Aumentado de 5min a 1h para reducir cache misses on-demand.
// Los datos de índice son para referencia visual en charts, no para trading en tiempo real.
const INTRADAY_CACHE_TTL_MS = 60 * 60 * 1000; // 1 hora (cuando falta punto intraday de hoy)

// In-memory cache for intraday quote (avoids hammering API on every CF call)
let _intradayCache = { data: null, timestamp: 0 };

// ============================================================================
// CLOUD FUNCTION: getIndexHistory (Callable)
// ============================================================================

/**
 * Obtiene datos históricos de un índice de mercado con cache.
 * 
 * @param {Object} request - Request de Firebase Functions
 * @param {Object} request.data - Datos de la solicitud
 * @param {string} request.data.code - Código del índice (ej: "GSPC", "DJI")
 * @param {string} request.data.range - Rango de tiempo ("1M", "3M", "6M", "YTD", "1Y", "5Y", "MAX")
 * 
 * @returns {Object} Datos del índice formateados para gráficos
 * @returns {Array} returns.chartData - Array de puntos {date, value, percentChange}
 * @returns {number} returns.overallChange - Cambio porcentual total del período
 * @returns {number} returns.latestValue - Último valor del índice
 * @returns {Object} returns.indexInfo - Info del índice {name, region, code}
 * @returns {boolean} returns.cacheHit - Si los datos vinieron del cache
 * @returns {number} returns.cacheTimestamp - Timestamp del cache
 */
const getIndexHistory = onCall(callableConfig, withRateLimit('getIndexHistory')(async (request) => {
  const { data } = request;
  const { code, range } = data || {};

  // Validación de parámetros
  if (!code || typeof code !== 'string') {
    throw new HttpsError("invalid-argument", "El parámetro 'code' es requerido y debe ser un string");
  }

  if (!range || typeof range !== 'string') {
    throw new HttpsError("invalid-argument", "El parámetro 'range' es requerido y debe ser un string");
  }

  if (!VALID_RANGES.includes(range)) {
    throw new HttpsError(
      "invalid-argument", 
      `El parámetro 'range' debe ser uno de: ${VALID_RANGES.join(", ")}`
    );
  }

  const cacheKey = `${code}_${range}`;
  const cacheRef = db.collection("indexCache").doc(cacheKey);

  try {
    // 1. Intentar obtener del cache
    const cacheDoc = await cacheRef.get();
    
    if (cacheDoc.exists) {
      const cacheData = cacheDoc.data();
      const cacheAge = Date.now() - (cacheData.lastUpdated || 0);

      // FIX-INDEX-INTRADAY: Use shorter TTL if cached data doesn't include today
      const todayStr = new Date().toISOString().split('T')[0];
      const lastCachedDate = (cacheData.chartData || []).length > 0
        ? cacheData.chartData[cacheData.chartData.length - 1].date
        : '';
      // If cache has today's data → 24h TTL; if not → 5min TTL (will recalculate with intraday)
      const effectiveTTL = lastCachedDate === todayStr ? CACHE_TTL_MS : INTRADAY_CACHE_TTL_MS;

      if (cacheAge < effectiveTTL) {
        console.log(`[getIndexHistory] Cache hit para ${cacheKey}`);
        return {
          chartData: cacheData.chartData || [],
          overallChange: cacheData.overallChange || 0,
          latestValue: cacheData.latestValue || 0,
          indexInfo: cacheData.indexInfo || { name: code, region: "Unknown", code },
          cacheHit: true,
          cacheTimestamp: cacheData.lastUpdated,
        };
      }
    }

    // 2. Cache miss o expirado - calcular datos
    console.log(`[getIndexHistory] Cache miss para ${cacheKey}, calculando...`);
    const result = await calculateIndexData(code, range);
    
    // 3. Guardar en cache
    const cachePayload = {
      ...result,
      lastUpdated: Date.now(),
    };
    
    await cacheRef.set(cachePayload);
    console.log(`[getIndexHistory] Cache guardado para ${cacheKey}`);

    return {
      ...result,
      cacheHit: false,
      cacheTimestamp: cachePayload.lastUpdated,
    };

  } catch (error) {
    console.error(`[getIndexHistory] Error para ${code}/${range}:`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError("internal", `Error al obtener datos del índice: ${error.message}`);
  }
}));

// ============================================================================
// FUNCIÓN AUXILIAR: calculateIndexData
// ============================================================================

/**
 * Calcula los datos históricos de un índice desde Firestore.
 * 
 * @param {string} code - Código del índice
 * @param {string} range - Rango de tiempo
 * @returns {Object} Datos calculados del índice
 */
async function calculateIndexData(code, range) {
  // Obtener info del índice
  const indexRef = db.collection("indexHistories").doc(code);
  const indexDoc = await indexRef.get();
  
  if (!indexDoc.exists) {
    throw new HttpsError("not-found", `Índice '${code}' no encontrado en indexHistories`);
  }

  const indexData = indexDoc.data();

  // Calcular fechas según rango
  const { startDate, endDate } = calculateDateRange(range);

  // Consultar datos de la subcolección dates
  const datesRef = db.collection("indexHistories").doc(code).collection("dates");
  const q = datesRef
    .where("date", ">=", startDate)
    .where("date", "<=", endDate)
    .orderBy("date", "asc");

  const snapshot = await q.get();
  
  if (snapshot.empty) {
    console.log(`[calculateIndexData] No hay datos para ${code} en rango ${range}`);
    return {
      chartData: [],
      overallChange: 0,
      latestValue: 0,
      indexInfo: {
        name: indexData.name || code,
        region: indexData.region || "Unknown",
        code,
      },
    };
  }

  // Mapear documentos a formato de gráfico
  const chartData = snapshot.docs.map(doc => {
    const d = doc.data();
    const value = parseFloat(d.score) || 0;
    let percentChange = parseFloat(d.percentChange) || 0;
    
    // Manejar valores no válidos (Infinity, NaN)
    if (!isFinite(percentChange)) {
      percentChange = 0;
    }
    if (!isFinite(value)) {
      return null; // Filtrar documentos con valores inválidos
    }
    
    return {
      date: d.date,
      value,
      percentChange,
    };
  }).filter(Boolean); // Eliminar nulls

  // Calcular cambio general del período
  let overallChange = 0;
  let latestValue = 0;

  if (chartData.length >= 2) {
    const initialValue = chartData[0].value;
    latestValue = chartData[chartData.length - 1].value;
    
    if (initialValue > 0 && isFinite(initialValue) && isFinite(latestValue)) {
      overallChange = ((latestValue - initialValue) / initialValue) * 100;
    }
  } else if (chartData.length === 1) {
    latestValue = chartData[0].value;
  }

  // Asegurar que los valores finales sean válidos para JSON
  if (!isFinite(overallChange)) overallChange = 0;
  if (!isFinite(latestValue)) latestValue = 0;

  // ── FIX-INDEX-INTRADAY: Append live intraday point if last data is not today ──
  // FIX-BENCH-003: Preserve historicalOverallChange for accurate summary cards.
  // The intraday point from /v1/quotes can differ from the official close stored
  // in indexHistories (pre-close vs official close, after-hours data, etc.).
  // overallChange should only use verified historical data + intraday ONLY during
  // active market hours when the close hasn't been recorded yet.
  const historicalOverallChange = Math.round(overallChange * 100) / 100;
  
  const todayStr = new Date().toISOString().split('T')[0];
  const lastChartDate = chartData.length > 0 ? chartData[chartData.length - 1].date : '';
  
  if (lastChartDate !== todayStr) {
    const intradayPoint = await _fetchIntradayPoint(code, todayStr);
    if (intradayPoint) {
      chartData.push(intradayPoint);
      latestValue = intradayPoint.value;
      
      // FIX-BENCH-003: Only update overallChange with intraday data if:
      // 1. It's a weekday (Mon-Fri) — market could be open
      // 2. The intraday value is reasonably close to the last historical close
      //    (within 5% — avoids stale after-hours/pre-market contamination)
      const dayOfWeek = new Date().getUTCDay(); // 0=Sun, 6=Sat
      const isWeekday = dayOfWeek >= 1 && dayOfWeek <= 5;
      const lastHistoricalValue = chartData.length >= 2 ? chartData[chartData.length - 2].value : 0;
      const intradayDrift = lastHistoricalValue > 0 
        ? Math.abs((intradayPoint.value - lastHistoricalValue) / lastHistoricalValue) 
        : 0;
      
      if (isWeekday && intradayDrift < 0.05) {
        // Recalculate overallChange with the intraday point
        if (chartData.length >= 2) {
          const initialValue = chartData[0].value;
          if (initialValue > 0 && isFinite(initialValue) && isFinite(latestValue)) {
            overallChange = Math.round(((latestValue - initialValue) / initialValue) * 100 * 100) / 100;
          }
        }
        console.log(`[calculateIndexData] Appended intraday point for ${code}: ${latestValue} on ${todayStr} (weekday, drift=${(intradayDrift*100).toFixed(2)}%)`);
      } else {
        // Keep historical overallChange — intraday data is stale or it's a weekend
        overallChange = historicalOverallChange;
        console.log(`[calculateIndexData] Appended intraday point for ${code}: ${latestValue} on ${todayStr} (using historicalOverallChange=${historicalOverallChange}, drift=${(intradayDrift*100).toFixed(2)}%, weekend=${!isWeekday})`);
      }
    }
  }

  return {
    chartData,
    overallChange: Math.round(overallChange * 100) / 100, // 2 decimales
    latestValue: Math.round(latestValue * 100) / 100,
    indexInfo: {
      name: indexData.name || code,
      region: indexData.region || "Unknown",
      code,
    },
  };
}

// ============================================================================
// FUNCIÓN AUXILIAR: _fetchIntradayPoint
// ============================================================================

/**
 * FIX-INDEX-INTRADAY: Fetch live S&P 500 data from /v1/quotes endpoint.
 * Uses an in-memory cache (5 min TTL) to avoid hammering the API.
 * Does NOT write to Firestore — pure ephemeral cache.
 * 
 * @param {string} code - Index code (e.g., "GSPC")
 * @param {string} todayStr - Today's date as YYYY-MM-DD
 * @returns {Object|null} Chart data point {date, value, percentChange} or null
 */
async function _fetchIntradayPoint(code, todayStr) {
  // Map index code to Yahoo Finance symbol
  const symbolMap = {
    'GSPC': '^GSPC',
    'DJI': '^DJI',
    'IXIC': '^IXIC',
    'RUT': '^RUT',
    'VIX': '^VIX',
  };
  
  const yahooSymbol = symbolMap[code];
  if (!yahooSymbol) {
    console.log(`[_fetchIntradayPoint] No symbol mapping for code: ${code}`);
    return null;
  }
  
  // Check in-memory cache
  const cacheKey = `intraday_${code}`;
  if (_intradayCache.data && 
      _intradayCache.key === cacheKey &&
      Date.now() - _intradayCache.timestamp < INTRADAY_CACHE_TTL_MS) {
    console.log(`[_fetchIntradayPoint] Memory cache hit for ${code}`);
    return _intradayCache.data;
  }
  
  try {
    const url = `${FINANCE_QUERY_API_URL}/quotes?symbols=${encodeURIComponent(yahooSymbol)}`;
    const headers = getServiceHeaders();
    
    const response = await fetch(url, { 
      headers,
      signal: AbortSignal.timeout(8000), // 8 second timeout
    });
    
    if (!response.ok) {
      console.warn(`[_fetchIntradayPoint] API returned ${response.status} for ${code}`);
      return null;
    }
    
    const data = await response.json();
    const quote = Array.isArray(data) ? data[0] : data;
    
    if (!quote || !quote.price) {
      console.warn(`[_fetchIntradayPoint] No price data for ${code}`);
      return null;
    }
    
    // Parse values (handle comma-formatted numbers like "6,740.02")
    const price = parseFloat(String(quote.price).replace(/,/g, '')) || 0;
    const pctRaw = String(quote.percentChange || '0').replace(/[%+,]/g, '');
    const percentChange = parseFloat(pctRaw) || 0;
    
    if (price <= 0) return null;
    
    const point = {
      date: todayStr,
      value: Math.round(price * 100) / 100,
      percentChange: Math.round(percentChange * 100) / 100,
    };
    
    // Store in memory cache
    _intradayCache = { data: point, key: cacheKey, timestamp: Date.now() };
    
    console.log(`[_fetchIntradayPoint] Live ${code}: ${point.value} (${point.percentChange}%)`);
    return point;
    
  } catch (error) {
    console.warn(`[_fetchIntradayPoint] Failed to fetch ${code}: ${error.message}`);
    return null;
  }
}

// ============================================================================
// FUNCIÓN AUXILIAR: calculateDateRange
// ============================================================================

/**
 * Calcula el rango de fechas basado en el período solicitado.
 * 
 * @param {string} range - Rango de tiempo
 * @returns {Object} { startDate: string, endDate: string } en formato YYYY-MM-DD
 */
function calculateDateRange(range) {
  const endDate = new Date();
  let startDate = new Date();

  switch (range) {
    case "1M":
      startDate.setMonth(endDate.getMonth() - 1);
      break;
    case "3M":
      startDate.setMonth(endDate.getMonth() - 3);
      break;
    case "6M":
      startDate.setMonth(endDate.getMonth() - 6);
      break;
    case "YTD":
      startDate = new Date(endDate.getFullYear(), 0, 1);
      break;
    case "1Y":
      startDate.setFullYear(endDate.getFullYear() - 1);
      break;
    case "5Y":
      startDate.setFullYear(endDate.getFullYear() - 5);
      break;
    case "MAX":
      startDate = new Date("2010-01-01");
      break;
    default:
      startDate.setMonth(endDate.getMonth() - 1); // Default: 1M
  }

  return {
    startDate: startDate.toISOString().split("T")[0],
    endDate: endDate.toISOString().split("T")[0],
  };
}

// ============================================================================
// FUNCIÓN AUXILIAR: _mergeIncrementalCache
// ============================================================================

/**
 * OPT-FIRESTORE-002 P0-D+: Merge incremental — combina datos existentes del cache
 * con nuevos puntos de Firestore sin re-leer toda la subcolección dates.
 *
 * @param {Object} existingCache - Datos actuales del cache doc
 * @param {Array} newPoints - Nuevos puntos de datos [{date, value, percentChange}]
 * @param {string} range - Rango de tiempo (para calcular ventana de trimming)
 * @returns {Object} Datos actualizados listos para escribir en indexCache
 */
function _mergeIncrementalCache(existingCache, newPoints, range) {
  const { startDate } = calculateDateRange(range);

  // Merge: existing + new, dedup by date (prefer new → official close reemplaza intraday)
  const dateMap = new Map();
  for (const point of existingCache.chartData) {
    dateMap.set(point.date, point);
  }
  for (const point of newPoints) {
    dateMap.set(point.date, point); // overwrites intraday with official close
  }

  // Sort by date + trim before startDate (sliding window ranges)
  let mergedData = Array.from(dateMap.values())
    .filter(point => point.date >= startDate)
    .sort((a, b) => a.date.localeCompare(b.date));

  // Recalculate overallChange and latestValue
  let overallChange = 0;
  let latestValue = 0;

  if (mergedData.length >= 2) {
    const initialValue = mergedData[0].value;
    latestValue = mergedData[mergedData.length - 1].value;
    if (initialValue > 0 && isFinite(initialValue) && isFinite(latestValue)) {
      overallChange = ((latestValue - initialValue) / initialValue) * 100;
    }
  } else if (mergedData.length === 1) {
    latestValue = mergedData[0].value;
  }

  if (!isFinite(overallChange)) overallChange = 0;
  if (!isFinite(latestValue)) latestValue = 0;

  return {
    chartData: mergedData,
    overallChange: Math.round(overallChange * 100) / 100,
    latestValue: Math.round(latestValue * 100) / 100,
    indexInfo: existingCache.indexInfo,
  };
}

// ============================================================================
// INTERNAL: refreshIndexCacheInternal
// OPT-SNAP-INCR Fase 3: Core logic extracted for pipeline consolidation.
// Can be called standalone (scheduled) or as a step within unifiedMarketDataUpdate.
// ============================================================================

/**
 * Refresh incremental de todos los caches de índices.
 * Lee solo fechas nuevas de Firestore y las mergea con el cache existente.
 *
 * @returns {Promise<{success: boolean, refreshed: number, incrementalMerges: number, fullRecalcs: number, errors: number, duration: number}>}
 */
async function refreshIndexCacheInternal() {
  const startTime = Date.now();
  let refreshed = 0;
  let errors = 0;
  let fullRecalcs = 0;
  let incrementalMerges = 0;
  const errorDetails = [];

  const indicesSnapshot = await db.collection("indexHistories").get();
  const indices = indicesSnapshot.docs.map(doc => ({ id: doc.id, data: doc.data() }));

  console.log(`[refreshIndexCacheInternal] Procesando ${indices.length} índices incrementalmente`);

  for (const index of indices) {
    const code = index.id;

    try {
      // Leer los 7 cache docs en paralelo
      const cacheRefs = VALID_RANGES.map(range =>
        db.collection("indexCache").doc(`${code}_${range}`).get()
      );
      const cacheDocs = await Promise.all(cacheRefs);

      // Determinar la última fecha cacheada
      let lastCachedDate = null;
      for (const cacheDoc of cacheDocs) {
        if (cacheDoc.exists) {
          const data = cacheDoc.data();
          const cd = data.chartData;
          if (cd && cd.length > 0) {
            const d = cd[cd.length - 1].date;
            if (!lastCachedDate || d > lastCachedDate) lastCachedDate = d;
          }
        }
      }

      // Query nuevas fechas desde Firestore (solo dates >= lastCachedDate)
      const endDate = new Date().toISOString().split("T")[0];
      let newPoints = [];

      if (lastCachedDate) {
        const newDatesSnapshot = await db
          .collection("indexHistories").doc(code).collection("dates")
          .where("date", ">=", lastCachedDate)
          .where("date", "<=", endDate)
          .orderBy("date", "asc")
          .get();

        newPoints = newDatesSnapshot.docs.map(doc => {
          const d = doc.data();
          const value = parseFloat(d.score) || 0;
          let percentChange = parseFloat(d.percentChange) || 0;
          if (!isFinite(percentChange)) percentChange = 0;
          if (!isFinite(value)) return null;
          return { date: d.date, value, percentChange };
        }).filter(Boolean);

        if (newPoints.length > 0) {
          console.log(`[refreshIndexCacheInternal] ${code}: ${newPoints.length} punto(s) desde ${lastCachedDate}`);
        }
      }

      // Append intraday point (ensures 24h TTL instead of 1h)
      const todayStr = new Date().toISOString().split("T")[0];
      const hasToday = newPoints.some(p => p.date === todayStr);

      if (!hasToday) {
        const intradayPoint = await _fetchIntradayPoint(code, todayStr);
        if (intradayPoint) {
          newPoints.push(intradayPoint);
          console.log(`[refreshIndexCacheInternal] ${code}: appended intraday point ${intradayPoint.value} for ${todayStr}`);
        }
      }

      // Actualizar cada rango: incremental merge o full recalc
      for (let i = 0; i < VALID_RANGES.length; i++) {
        const range = VALID_RANGES[i];

        try {
          let result;
          const cacheDoc = cacheDocs[i];
          const hasValidCache = cacheDoc.exists
            && cacheDoc.data().chartData?.length > 0
            && cacheDoc.data().indexInfo;

          if (hasValidCache && lastCachedDate) {
            result = _mergeIncrementalCache(cacheDoc.data(), newPoints, range);
            incrementalMerges++;
          } else {
            result = await calculateIndexData(code, range);
            fullRecalcs++;
          }

          await db.collection("indexCache").doc(`${code}_${range}`).set({
            ...result,
            lastUpdated: Date.now(),
          });

          refreshed++;
        } catch (error) {
          errors++;
          errorDetails.push(`${code}/${range}: ${error.message}`);
          console.error(`[refreshIndexCacheInternal] Error ${code}/${range}:`, error.message);
        }
      }

    } catch (error) {
      errors += VALID_RANGES.length;
      errorDetails.push(`${code}/*: ${error.message}`);
      console.error(`[refreshIndexCacheInternal] Error procesando ${code}:`, error.message);
    }
  }

  const duration = Math.round((Date.now() - startTime) / 1000);
  console.log(
    `[refreshIndexCacheInternal] Completado en ${duration}s: ${refreshed} caches ` +
    `(${incrementalMerges} incremental, ${fullRecalcs} full), ${errors} errores`
  );

  if (errorDetails.length > 0) {
    console.log(`[refreshIndexCacheInternal] Errores:`, errorDetails.slice(0, 10));
  }

  return { success: true, refreshed, incrementalMerges, fullRecalcs, errors, duration, totalIndices: indices.length };
}

// ============================================================================
// CLOUD FUNCTION: refreshIndexCache (Scheduled)
// OPT-SNAP-INCR Fase 3: Kept as standalone fallback. Primary execution now via unifiedMarketDataUpdate.
// ============================================================================

/**
 * Función programada para refrescar todos los caches de índices.
 * Se ejecuta diariamente a las 00:30 UTC (después del cierre de mercados US).
 *
 * OPT-FIRESTORE-002 P0-D+: Refresh incremental — lee solo fechas nuevas de Firestore
 * y las mergea con el cache existente en vez de recalcular los 7 rangos completos.
 * Reduce lecturas de ~8,400/ejecución a ~108 (98.7% reducción).
 *
 * Patrón por índice:
 *  1. Lee los 7 cache docs en paralelo (7 reads)
 *  2. Query dates subcollection solo desde la última fecha cacheada (1 read, ~1-2 docs)
 *  3. Merge incremental para cada rango (0 reads)
 *  4. Cold start fallback a calculateIndexData si no hay cache previo
 */
const refreshIndexCache = onSchedule(
  {
    // OPT-SNAP-INCR Fase 3: Disabled daily cron — primary execution is via unifiedMarketDataUpdate.
    // Weekly Sunday backup: rebuilds cache if pipeline failed all week.
    schedule: "30 0 * * 0",
    timeZone: "America/New_York",
    region: "us-central1",
    memory: "512MiB",
    timeoutSeconds: 540,
  },
  async (event) => {
    console.log("[refreshIndexCache] Weekly backup execution — delegating to internal");
    try {
      return await refreshIndexCacheInternal();
    } catch (error) {
      console.error("[refreshIndexCache] Error fatal:", error);
      return { success: false, error: error.message };
    }
  }
);

// ============================================================================
// FUNCIÓN AUXILIAR: invalidateIndexCache (para uso externo)
// ============================================================================

/**
 * Invalida el cache de un índice específico para todos los rangos.
 * Puede ser llamada desde otras Cloud Functions (ej: unifiedMarketDataUpdate).
 * 
 * @param {string} code - Código del índice a invalidar
 */
async function invalidateIndexCache(code) {
  console.log(`[invalidateIndexCache] Invalidando cache para índice: ${code}`);
  
  const batch = db.batch();
  
  for (const range of VALID_RANGES) {
    const cacheKey = `${code}_${range}`;
    const cacheRef = db.collection("indexCache").doc(cacheKey);
    batch.delete(cacheRef);
  }
  
  await batch.commit();
  console.log(`[invalidateIndexCache] Cache invalidado para ${code} (${VALID_RANGES.length} rangos)`);
}

/**
 * Invalida todos los caches de índices.
 */
async function invalidateAllIndexCaches() {
  console.log("[invalidateAllIndexCaches] Invalidando todos los caches...");
  
  const snapshot = await db.collection("indexCache").get();
  
  if (snapshot.empty) {
    console.log("[invalidateAllIndexCaches] No hay caches para invalidar");
    return 0;
  }
  
  const batch = db.batch();
  snapshot.docs.forEach(doc => batch.delete(doc.ref));
  await batch.commit();
  
  console.log(`[invalidateAllIndexCaches] ${snapshot.size} caches invalidados`);
  return snapshot.size;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = { 
  getIndexHistory,
  refreshIndexCache,
  refreshIndexCacheInternal,
  invalidateIndexCache,
  invalidateAllIndexCaches,
  calculateIndexData, // Exportado para tests
};
