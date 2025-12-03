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
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 horas

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
const getIndexHistory = onCall(callableConfig, async (request) => {
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

      if (cacheAge < CACHE_TTL_MS) {
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
});

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
// CLOUD FUNCTION: refreshIndexCache (Scheduled)
// ============================================================================

/**
 * Función programada para refrescar todos los caches de índices.
 * Se ejecuta diariamente a las 00:30 UTC (después del cierre de mercados US).
 */
const refreshIndexCache = onSchedule(
  {
    schedule: "30 0 * * *", // Diario a las 00:30 UTC
    timeZone: "UTC",
    region: "us-central1",
    memory: "512MiB",
    timeoutSeconds: 540, // 9 minutos para procesar todos los índices
  },
  async (event) => {
    console.log("[refreshIndexCache] Iniciando refresh de caches de índices...");

    const startTime = Date.now();
    let refreshed = 0;
    let errors = 0;
    const errorDetails = [];

    try {
      // Obtener lista de todos los índices
      const indicesSnapshot = await db.collection("indexHistories").get();
      const indices = indicesSnapshot.docs.map(doc => doc.id);
      
      console.log(`[refreshIndexCache] Procesando ${indices.length} índices x ${VALID_RANGES.length} rangos = ${indices.length * VALID_RANGES.length} caches`);

      // Procesar cada combinación índice + rango
      for (const code of indices) {
        for (const range of VALID_RANGES) {
          try {
            const result = await calculateIndexData(code, range);
            const cacheKey = `${code}_${range}`;
            
            await db.collection("indexCache").doc(cacheKey).set({
              ...result,
              lastUpdated: Date.now(),
            });

            refreshed++;
            
            // Log de progreso cada 20 caches
            if (refreshed % 20 === 0) {
              console.log(`[refreshIndexCache] Progreso: ${refreshed} caches actualizados`);
            }

          } catch (error) {
            errors++;
            errorDetails.push(`${code}/${range}: ${error.message}`);
            console.error(`[refreshIndexCache] Error procesando ${code}/${range}:`, error.message);
          }
        }
      }

      const duration = Math.round((Date.now() - startTime) / 1000);
      console.log(`[refreshIndexCache] Completado en ${duration}s: ${refreshed} caches actualizados, ${errors} errores`);
      
      if (errorDetails.length > 0) {
        console.log(`[refreshIndexCache] Errores detallados:`, errorDetails.slice(0, 10));
      }

      return { 
        success: true, 
        refreshed, 
        errors, 
        duration,
        totalIndices: indices.length,
      };

    } catch (error) {
      console.error("[refreshIndexCache] Error fatal:", error);
      return { 
        success: false, 
        error: error.message,
        refreshed,
        errors,
      };
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
  invalidateIndexCache,
  invalidateAllIndexCaches,
  calculateIndexData, // Exportado para tests
};
