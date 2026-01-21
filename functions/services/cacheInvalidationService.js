/**
 * Cache Invalidation Service - Funciones para invalidar caches de usuario
 * 
 * Consolidación de funciones de invalidación de cache que antes estaban en:
 * - historicalReturnsService.js (invalidatePerformanceCache, invalidatePerformanceCacheBatch)
 * - portfolioDistributionService.js (invalidateDistributionCache)
 * 
 * @module services/cacheInvalidationService
 * @see docs/stories/56.story.md (SCALE-CF-001)
 */

const admin = require('./firebaseAdmin');
const { DateTime } = require('luxon');
const db = admin.firestore();

// ============================================================================
// PERFORMANCE CACHE INVALIDATION
// ============================================================================

/**
 * Invalida el cache de rendimientos para un usuario
 * 
 * @param {string} userId - ID del usuario
 */
async function invalidatePerformanceCache(userId) {
  try {
    const cacheCollection = db.collection(`userData/${userId}/performanceCache`);
    const cacheSnapshot = await cacheCollection.get();

    if (cacheSnapshot.empty) {
      console.log(`[invalidatePerformanceCache] Sin cache para ${userId}`);
      return;
    }

    const batch = db.batch();
    cacheSnapshot.docs.forEach(doc => {
      batch.delete(doc.ref);
    });

    await batch.commit();
    console.log(`[invalidatePerformanceCache] Cache invalidado para ${userId} (${cacheSnapshot.size} documentos)`);
  } catch (error) {
    console.error(`[invalidatePerformanceCache] Error: ${error.message}`);
  }
}

/**
 * Invalida el cache de rendimientos para múltiples usuarios en batch
 * Optimizado para minimizar lecturas y escrituras de Firestore
 * 
 * @param {string[]} userIds - Array de IDs de usuarios
 * @returns {Promise<{usersProcessed: number, cachesDeleted: number}>}
 */
async function invalidatePerformanceCacheBatch(userIds) {
  if (!userIds || userIds.length === 0) {
    return { usersProcessed: 0, cachesDeleted: 0 };
  }

  let totalDeleted = 0;

  // Consultar todos los caches en paralelo
  const cachePromises = userIds.map(async (userId) => {
    const cacheCollection = db.collection(`userData/${userId}/performanceCache`);
    const snapshot = await cacheCollection.limit(20).get();
    return { userId, docs: snapshot.docs };
  });
  
  const results = await Promise.all(cachePromises);
  
  // Agrupar todas las eliminaciones en un solo batch
  const deleteBatch = db.batch();
  
  for (const { docs } of results) {
    for (const doc of docs) {
      deleteBatch.delete(doc.ref);
      totalDeleted++;
    }
  }
  
  // Solo commit si hay documentos que eliminar
  if (totalDeleted > 0) {
    await deleteBatch.commit();
  }

  console.log(`[invalidatePerformanceCacheBatch] Eliminados ${totalDeleted} caches para ${userIds.length} usuarios`);
  return { usersProcessed: userIds.length, cachesDeleted: totalDeleted };
}

// ============================================================================
// DISTRIBUTION CACHE INVALIDATION
// ============================================================================

// Cache en memoria para distribución (5 minutos TTL)
const distributionCache = new Map();
const DISTRIBUTION_CACHE_TTL = 5 * 60 * 1000; // 5 minutos

/**
 * Invalida el cache de distribución para un usuario
 * Elimina todas las entradas de cache relacionadas con el userId
 * 
 * @param {string} userId - ID del usuario
 */
function invalidateDistributionCache(userId) {
  const keysToDelete = [];
  
  for (const key of distributionCache.keys()) {
    if (key.startsWith(`dist:${userId}`)) {
      keysToDelete.push(key);
    }
  }
  
  keysToDelete.forEach(key => distributionCache.delete(key));
  
  if (keysToDelete.length > 0) {
    console.log(`[invalidateDistributionCache] Eliminadas ${keysToDelete.length} entradas para ${userId}`);
  }
}

// ============================================================================
// TTL UTILITIES
// ============================================================================

/**
 * Calcula el TTL dinámico basado en el estado del mercado NYSE
 * - Durante horario de mercado (9:30-16:00 ET): 2 minutos
 * - Fuera de horario: hasta próxima apertura (9:30 AM ET)
 * - Fines de semana: hasta lunes 9:30 AM ET
 * 
 * @returns {Date} Fecha de expiración del cache
 */
function calculateDynamicTTL() {
  const now = DateTime.now().setZone('America/New_York');
  const hour = now.hour + now.minute / 60;
  
  const MARKET_OPEN = 9.5;  // 9:30 AM
  const MARKET_CLOSE = 16;   // 4:00 PM
  
  // Sábado: válido hasta lunes 9:30 AM
  if (now.weekday === 6) {
    return now.plus({ days: 2 }).set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
  }
  
  // Domingo: válido hasta lunes 9:30 AM
  if (now.weekday === 7) {
    return now.plus({ days: 1 }).set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
  }
  
  // Durante horario de mercado: TTL de 2 minutos
  if (hour >= MARKET_OPEN && hour < MARKET_CLOSE) {
    return now.plus({ minutes: 2 }).toJSDate();
  }
  
  // Después del cierre
  if (hour >= MARKET_CLOSE) {
    // Viernes después del cierre: válido hasta lunes 9:30 AM
    if (now.weekday === 5) {
      return now.plus({ days: 3 }).set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
    }
    // Lunes-Jueves después del cierre: válido hasta mañana 9:30 AM
    return now.plus({ days: 1 }).set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
  }
  
  // Antes de apertura: válido hasta las 9:30 AM de hoy
  return now.set({ hour: 9, minute: 30, second: 0, millisecond: 0 }).toJSDate();
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Performance cache
  invalidatePerformanceCache,
  invalidatePerformanceCacheBatch,
  
  // Distribution cache
  invalidateDistributionCache,
  distributionCache,
  DISTRIBUTION_CACHE_TTL,
  
  // TTL utilities
  calculateDynamicTTL,
};
