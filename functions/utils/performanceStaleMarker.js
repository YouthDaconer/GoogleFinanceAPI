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
 * Obtiene la fecha del último documento de portfolioPerformance del usuario
 * 
 * @param {string} userId - ID del usuario
 * @returns {Promise<string|null>} Fecha en formato YYYY-MM-DD o null si no hay documentos
 */
async function getLatestPerformanceDate(userId) {
  try {
    // La fuente es el último doc diario y no el campo `date` del doc principal:
    // ese campo solo lo escribía el backfill y el EOD nunca lo actualiza, así
    // que se quedaba congelado en el último backfill (visto: 2026-06-30 con
    // docs diarios hasta octubre) y toda transacción tardía posterior parecía
    // a tiempo.
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
 * Una transacción es retroactiva si su fecha es ANTERIOR O IGUAL a la fecha
 * del último documento de portfolioPerformance calculado: ese día ya lo cerró
 * el EOD sin ella, y el EOD siguiente solo cuenta transacciones de su propio
 * día, así que nadie más la va a contabilizar.
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
  
  // `<=` y no `<`: el caso más común es "compré ayer y lo registro hoy", con
  // fecha igual al último día calculado. Con `<` no se marcaba, el EOD de hoy
  // veía las unidades nuevas sin flujo de caja y contaba la compra como
  // rendimiento (MA_stock 2026-05-07: +47,9 % con el mercado en +1,8 %).
  return txDate <= latestDate;
}

/**
 * Marca el documento de performance como stale
 * 
 * Si ya existe un campo _stale, solo actualiza 'since' si la nueva fecha es más antigua.
 * Esto asegura que múltiples transacciones retroactivas se agrupen correctamente.
 *
 * HU 6.8.1 (RN-6.8.1-O): la corrección nocturna avanza `since` tramo a tramo,
 * así que `since` es el primer día aún sin corregir. Una operación anterior lo
 * mueve hacia atrás y se rehace desde ella; una posterior no lo mueve y entra
 * cuando la corrección llegue a su fecha. Ya no hay recorte: sin `truncated`.
 * @see platform-docs/stories/6.8.1-historial-corregido-por-tramos/historia.md
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
    await db.runTransaction(async (transaction) => {
      const perfDoc = await transaction.get(perfRef);
      
      if (!perfDoc.exists) {
        // No hay documento de performance, nada que marcar
        console.log(`[performanceStaleMarker] User ${userId}: No performance doc exists. Skipping stale mark.`);
        return;
      }
      
      const currentData = perfDoc.data();
      const currentStale = currentData._stale;
      
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
  markPerformanceAsStale,
  checkAndMarkStaleIfRetroactive,
  clearStaleMarker,
};
