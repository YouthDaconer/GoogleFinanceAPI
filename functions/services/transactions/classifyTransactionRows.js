/**
 * Cloud Function: classifyTransactionRows
 *
 * HU 1.3: clasifica las filas de una reimportación ANTES de confirmarla, para que
 * el usuario vea qué es nuevo y qué ya está registrado y decida él (RN-08).
 *
 * Es una operación de SOLO LECTURA. No escribe nada, no reserva nada y no cambia
 * el estado de la importación: es una consulta previa cuyo resultado el usuario
 * puede ignorar.
 *
 * @module transactions/classifyTransactionRows
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('../firebaseAdmin');

// GATE-006: Validación de features por plan
const { validateFeatureAccess } = require('../helpers/subscriptionValidator');

const { classifyRows } = require('./services/rowClassifier');
const { LIMITS } = require('./types');

const db = admin.firestore();

// ============================================================================
// CLOUD FUNCTION CONFIGURATION
// ============================================================================

/**
 * Lectura acotada: una consulta por ticker distinto del archivo.
 * El timeout es holgado porque un archivo con muchos activos distintos hace
 * varias consultas secuenciales.
 */
const FUNCTION_CONFIG = {
  cors: true,
  memory: "256MiB",
  timeoutSeconds: 120,
  maxInstances: 20,
  minInstances: 0,
  region: 'us-central1',
};

// ============================================================================
// MAIN CLOUD FUNCTION
// ============================================================================

/**
 * Clasifica filas en nuevas / ya registradas.
 *
 * @param {Object} request
 * @param {Object} request.data
 * @param {string} request.data.portfolioAccountId - Cuenta destino
 * @param {Object[]} request.data.rows - Filas válidas con ticker YA RESUELTO
 * @returns {Object} { success, classification, counts }
 */
const classifyTransactionRows = onCall(
  FUNCTION_CONFIG,
  async (request) => {
    const startTime = Date.now();
    const { auth, data } = request;

    // ─────────────────────────────────────────────────────────────────────
    // 1. AUTHENTICATION
    // ─────────────────────────────────────────────────────────────────────
    if (!auth) {
      throw new HttpsError(
        'unauthenticated',
        'Usuario debe estar autenticado para clasificar filas'
      );
    }

    const userId = auth.uid;

    // GATE-006: el gate del canal se mantiene sin cambios
    await validateFeatureAccess(userId, 'hasImport');

    // ─────────────────────────────────────────────────────────────────────
    // 2. PAYLOAD VALIDATION
    // ─────────────────────────────────────────────────────────────────────
    const { portfolioAccountId, rows } = data || {};

    if (!portfolioAccountId || typeof portfolioAccountId !== 'string') {
      throw new HttpsError('invalid-argument', 'Se requiere portfolioAccountId');
    }

    if (!Array.isArray(rows)) {
      throw new HttpsError('invalid-argument', 'rows debe ser un array');
    }

    // RN-13: se respetan los límites operativos vigentes del canal
    if (rows.length > LIMITS.maxBatchTransactions) {
      throw new HttpsError(
        'invalid-argument',
        `Máximo ${LIMITS.maxBatchTransactions} filas por clasificación. Recibidas: ${rows.length}`
      );
    }

    // ─────────────────────────────────────────────────────────────────────
    // 3. ACCOUNT OWNERSHIP
    // ─────────────────────────────────────────────────────────────────────
    // Sin esta verificación, la clasificación revelaría si una operación existe
    // en la cuenta de otro usuario.
    const ownsAccount = await verifyAccountAccess(portfolioAccountId, userId);

    if (!ownsAccount) {
      throw new HttpsError(
        'permission-denied',
        'No tiene acceso a esta cuenta de portafolio'
      );
    }

    // ─────────────────────────────────────────────────────────────────────
    // 4. CLASSIFY
    // ─────────────────────────────────────────────────────────────────────
    const { classification, counts } = await classifyRows(rows, userId, portfolioAccountId);

    const duration = Date.now() - startTime;
    console.log(
      `[classifyTransactionRows] ${rows.length} rows in ${duration}ms - new: ${counts.new}, existing: ${counts.existing}`
    );

    return {
      success: true,
      classification,
      counts,
      processingTimeMs: duration,
    };
  }
);

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Verifica que la cuenta pertenece al usuario.
 *
 * Mismo criterio que `importTransactionBatch.verifyAccountAccess`.
 *
 * @param {string} portfolioAccountId
 * @param {string} userId
 * @returns {Promise<boolean>}
 */
async function verifyAccountAccess(portfolioAccountId, userId) {
  try {
    const accountDoc = await db.collection('portfolioAccounts')
      .doc(portfolioAccountId)
      .get();

    if (!accountDoc.exists) {
      return false;
    }

    return accountDoc.data().userId === userId;
  } catch (error) {
    console.error('[classifyTransactionRows] Error verifying account:', error);
    return false;
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  classifyTransactionRows,

  // For testing
  verifyAccountAccess,
};
