/**
 * performanceStaleMarker.js
 * 
 * Utilidades para detectar y marcar documentos de performance como stale
 * cuando se registran transacciones retroactivas.
 * 
 * @see LATE-REG-002
 * @see docs/architecture/LATE-REGISTRATION-001-retroactive-transactions-analysis.md
 */

const { getFirestore, FieldValue } = require('firebase-admin/firestore');

const db = getFirestore();

/**
 * Máximo de trading days que se recalcularán automáticamente
 * Rangos mayores se truncan y se marcan como truncated
 */
const MAX_TRADING_DAYS = 30;

/**
 * Obtiene la fecha del último documento de portfolioPerformance del usuario
 * 
 * @param {string} userId - ID del usuario
 * @returns {Promise<string|null>} Fecha en formato YYYY-MM-DD o null si no hay documentos
 */
async function getLatestPerformanceDate(userId) {
  try {
    const perfDoc = await db.collection('portfolioPerformance').doc(userId).get();
    
    if (!perfDoc.exists) {
      return null;
    }
    
    const data = perfDoc.data();
    
    // El campo 'date' contiene la fecha del último cálculo
    // Formato: "2026-02-13" o ISO string "2026-02-13T00:00:00.000Z"
    if (data.date) {
      return data.date.split('T')[0];
    }
    
    // Si no hay campo 'date' en el documento principal, buscar en la subcollection 'dates'
    // Esto cubre el caso donde el documento solo tiene userId y los datos están en dates/
    const datesSnap = await db.collection('portfolioPerformance')
      .doc(userId)
      .collection('dates')
      .orderBy('date', 'desc')
      .limit(1)
      .get();
    
    if (datesSnap.empty) {
      return null;
    }
    
    // El ID del documento es la fecha (YYYY-MM-DD)
    return datesSnap.docs[0].id;
    
  } catch (error) {
    console.error(`[performanceStaleMarker] Error getting latest date for user ${userId}:`, error.message);
    return null;
  }
}

/**
 * Extrae la parte de fecha (YYYY-MM-DD) de un string de fecha/timestamp
 * @param {string} dateString - Fecha en formato YYYY-MM-DD o YYYY-MM-DDTHH:MM:SS.sssZ
 * @returns {string} Fecha en formato YYYY-MM-DD
 */
function getDatePart(dateString) {
  if (!dateString) return '';
  if (dateString.includes('T')) {
    return dateString.substring(0, 10);
  }
  return dateString;
}

/**
 * Determina si una transacción es retroactiva
 * Una transacción es retroactiva si su fecha es ANTERIOR a la fecha
 * del último documento de portfolioPerformance calculado
 * 
 * @param {string} transactionDate - Fecha de la transacción (YYYY-MM-DD o ISO)
 * @param {string} userId - ID del usuario
 * @returns {Promise<boolean>}
 */
async function isRetroactiveTransaction(transactionDate, userId) {
  const latestDate = await getLatestPerformanceDate(userId);
  
  if (!latestDate) {
    // No hay performance calculado aún, no puede ser retroactivo
    return false;
  }
  
  const txDate = getDatePart(transactionDate);
  
  // Es retroactivo si la fecha de la transacción es ESTRICTAMENTE anterior
  // al último día de performance calculado
  return txDate < latestDate;
}

/**
 * Calcula el número aproximado de trading days entre dos fechas
 * Usa la aproximación de 5/7 (5 trading days por semana)
 * 
 * @param {string} startDate - Fecha inicio YYYY-MM-DD
 * @param {string} endDate - Fecha fin YYYY-MM-DD
 * @returns {number} Número aproximado de trading days
 */
function estimateTradingDays(startDate, endDate) {
  const start = new Date(startDate + 'T12:00:00Z');
  const end = new Date(endDate + 'T12:00:00Z');
  
  if (end <= start) {
    return 0;
  }
  
  const calendarDays = Math.ceil((end - start) / (1000 * 60 * 60 * 24));
  
  // Aproximación conservadora: ~5/7 de los días calendario son trading days
  return Math.ceil(calendarDays * 5 / 7);
}

/**
 * Marca el documento de performance como stale
 * 
 * Si ya existe un campo _stale, solo actualiza 'since' si la nueva fecha es más antigua.
 * Esto asegura que múltiples transacciones retroactivas se agrupen correctamente.
 * 
 * @param {string} userId - ID del usuario
 * @param {string} transactionDate - Fecha de la transacción retroactiva (YYYY-MM-DD o ISO)
 * @param {Object} metadata - Información adicional
 * @param {string} metadata.reason - Razón del stale (default: 'retroactive_transaction')
 * @param {string} metadata.transactionType - Tipo de transacción (buy, sell, deposit, withdrawal)
 * @param {string} [metadata.portfolioAccount] - ID de la cuenta afectada (opcional)
 * @returns {Promise<void>}
 */
async function markPerformanceAsStale(userId, transactionDate, metadata = {}) {
  const perfRef = db.collection('portfolioPerformance').doc(userId);
  const txDate = getDatePart(transactionDate);
  
  try {
    // Obtener la última fecha de performance ANTES de la transacción
    // para calcular correctamente el rango de días a reconciliar
    const latestDate = await getLatestPerformanceDate(userId);
    
    await db.runTransaction(async (transaction) => {
      const perfDoc = await transaction.get(perfRef);
      
      if (!perfDoc.exists) {
        // No hay documento de performance, nada que marcar
        console.log(`[performanceStaleMarker] User ${userId}: No performance doc exists. Skipping stale mark.`);
        return;
      }
      
      const currentData = perfDoc.data();
      const currentStale = currentData._stale;
      
      // latestDate ya fue obtenido antes de la transacción usando getLatestPerformanceDate
      // que busca en la subcollection dates si no hay campo date en el documento principal
      
      // Calcular si el rango es muy grande (> MAX_TRADING_DAYS)
      const tradingDays = latestDate ? estimateTradingDays(txDate, latestDate) : 0;
      const truncated = tradingDays > MAX_TRADING_DAYS;
      
      if (truncated) {
        console.warn(`[performanceStaleMarker] User ${userId}: Range of ${tradingDays} trading days exceeds max (${MAX_TRADING_DAYS}). Will be truncated.`);
      }
      
      // Determinar la fecha "since" más antigua
      // Si ya hay un _stale con fecha más antigua, mantenerla
      let newSince = txDate;
      if (currentStale?.since && currentStale.since < txDate) {
        newSince = currentStale.since;
      }
      
      const staleData = {
        since: newSince,
        reason: metadata.reason || 'retroactive_transaction',
        transactionType: metadata.transactionType || 'unknown',
        portfolioAccount: metadata.portfolioAccount || null,
        registeredAt: new Date().toISOString(),
        // Mantener truncated si ya estaba true, o marcar true si el nuevo rango es muy grande
        truncated: truncated || (currentStale?.truncated === true),
        // Resetear retryCount si estamos actualizando con nueva info
        retryCount: currentStale?.retryCount || 0
      };
      
      transaction.update(perfRef, { _stale: staleData });
    });
    
    console.log(`[performanceStaleMarker] Marked stale for user ${userId} since ${txDate}`);
    
  } catch (error) {
    // Log pero NO fallar la operación principal del usuario
    console.error(`[performanceStaleMarker] Error marking stale for user ${userId}:`, error.message);
  }
}

/**
 * Helper principal para usar en handlers de transacciones
 * 
 * Detecta si la transacción es retroactiva y, si lo es, marca el performance como stale.
 * Esta función es "fire and forget" - no bloquea la operación del usuario.
 * 
 * @param {string} userId - ID del usuario
 * @param {string} transactionDate - Fecha de la transacción (YYYY-MM-DD o ISO)
 * @param {Object} metadata - Metadata adicional (transactionType, portfolioAccount, etc.)
 * @returns {Promise<void>}
 */
async function checkAndMarkStaleIfRetroactive(userId, transactionDate, metadata = {}) {
  try {
    const isRetroactive = await isRetroactiveTransaction(transactionDate, userId);
    
    if (isRetroactive) {
      // Fire and forget - no bloqueamos al usuario esperando esta operación
      markPerformanceAsStale(userId, transactionDate, metadata).catch(err => {
        console.error(`[performanceStaleMarker] Background mark failed for user ${userId}:`, err.message);
      });
    }
  } catch (error) {
    // Never fail the main operation
    console.error(`[performanceStaleMarker] Error checking retroactive for user ${userId}:`, error.message);
  }
}

/**
 * Limpia el campo _stale de un documento de performance
 * Usado después de una reconciliación exitosa
 * 
 * @param {string} userId - ID del usuario
 * @returns {Promise<void>}
 */
async function clearStaleMarker(userId) {
  try {
    await db.collection('portfolioPerformance').doc(userId).update({
      _stale: FieldValue.delete()
    });
    console.log(`[performanceStaleMarker] Cleared stale marker for user ${userId}`);
  } catch (error) {
    console.error(`[performanceStaleMarker] Error clearing stale for user ${userId}:`, error.message);
  }
}

module.exports = {
  getLatestPerformanceDate,
  isRetroactiveTransaction,
  estimateTradingDays,
  markPerformanceAsStale,
  checkAndMarkStaleIfRetroactive,
  clearStaleMarker,
  MAX_TRADING_DAYS,
};
