/**
 * Global Equivalence Repository
 *
 * HU 1.6: catálogo global de equivalencias de símbolo, construido por evidencia de
 * varios usuarios.
 *
 * ────────────────────────────────────────────────────────────────────────────
 * SEPARACIÓN DE DATOS — es lo que cumple RN-30
 * ────────────────────────────────────────────────────────────────────────────
 *
 * `symbolEquivalences/{clave}`
 *   El CATÁLOGO. Contiene únicamente el símbolo de origen, su formato de origen y
 *   el ticker canónico (más `assetType` y `currency`, que son propiedades del
 *   ticker canónico y sin las cuales la resolución no sería utilizable).
 *   NINGÚN identificador de usuario, cuenta, cantidad ni valor.
 *
 * `symbolEquivalenceEvidence/{clave}/confirmations/{uidHash}`
 * `symbolEquivalenceEvidence/{clave}/contradictions/{uidHash}`
 *   La EVIDENCIA, en una colección separada. El id de documento es un HMAC del uid
 *   con un secreto de servidor: da desduplicación por usuario (RN-07, escenario 5)
 *   sin almacenar una identidad reversible.
 *
 * `symbolEquivalenceAudit/{autoId}`
 *   Traza de promociones y retiros.
 *
 * Ninguna de las cuatro es accesible desde el cliente.
 *
 * @module transactions/services/globalEquivalenceRepository
 */

const crypto = require('crypto');
const admin = require('../../firebaseAdmin');
const { buildEquivalenceKey } = require('./importMemoryRepository');
const {
  GLOBAL_EQUIVALENCE_THRESHOLDS,
  GLOBAL_EQUIVALENCE_STATUS,
  GLOBAL_EQUIVALENCE_AUDIT_ACTIONS,
} = require('../types');

const db = admin.firestore();

// ============================================================================
// CONSTANTS
// ============================================================================

const CATALOG_COLLECTION = 'symbolEquivalences';
const EVIDENCE_COLLECTION = 'symbolEquivalenceEvidence';
const AUDIT_COLLECTION = 'symbolEquivalenceAudit';

const CONFIRMATIONS_SUBCOLLECTION = 'confirmations';
const CONTRADICTIONS_SUBCOLLECTION = 'contradictions';

/** Firestore limita `getAll` en lotes razonables */
const CATALOG_READ_BATCH_SIZE = 30;

// ============================================================================
// USER HASHING
// ============================================================================

/**
 * Convierte un uid en un identificador no reversible.
 *
 * Se usa como id de documento de la evidencia: dos confirmaciones del mismo usuario
 * caen en el mismo documento, así que el conteo de usuarios distintos es el conteo
 * de documentos. No se guarda el uid en ningún campo.
 *
 * @param {string} userId
 * @param {string} salt - Secreto de servidor (EQUIV_HASH_SALT)
 * @returns {string} Hash hexadecimal
 */
function hashUserId(userId, salt) {
  if (!userId) {
    return '';
  }

  // Sin secreto configurado no se puede anonimizar de forma no reversible.
  // Se falla en cerrado: mejor no registrar evidencia que registrarla reversible.
  if (!salt) {
    throw new Error('EQUIV_HASH_SALT no está configurado: no se puede anonimizar la evidencia');
  }

  return crypto
    .createHmac('sha256', salt)
    .update(String(userId))
    .digest('hex');
}

// ============================================================================
// CATALOG READ (RN-06)
// ============================================================================

/**
 * Lee las entradas ACTIVAS del catálogo global para un conjunto de claves.
 *
 * Solo se llama para los símbolos que NO tienen equivalencia propia del usuario:
 * la precedencia de la memoria propia (RN-06) la aplica el llamador.
 *
 * @param {string[]} keys - Claves `sourceFormatId::SÍMBOLO`
 * @returns {Promise<Object>} Mapa clave → entrada activa
 */
async function getActiveEntries(keys) {
  if (!Array.isArray(keys) || keys.length === 0) {
    return {};
  }

  const uniqueKeys = [...new Set(keys.filter(Boolean))];
  const result = {};

  try {
    for (let i = 0; i < uniqueKeys.length; i += CATALOG_READ_BATCH_SIZE) {
      const batch = uniqueKeys.slice(i, i + CATALOG_READ_BATCH_SIZE);
      const refs = batch.map(key => db.collection(CATALOG_COLLECTION).doc(key));
      const docs = await db.getAll(...refs);

      docs.forEach((doc) => {
        if (!doc.exists) {
          return;
        }

        const data = doc.data();

        // Escenario 6: una entrada retirada deja de proponerse
        if (data.status !== GLOBAL_EQUIVALENCE_STATUS.ACTIVE) {
          return;
        }

        result[doc.id] = {
          sourceFormatId: data.sourceFormatId,
          sourceSymbol: data.sourceSymbol,
          resolvedSymbol: data.resolvedSymbol,
          assetType: data.assetType || 'stock',
          currency: data.currency || 'USD',
          origin: 'global',
        };
      });
    }
  } catch (error) {
    // RN-11: catálogo inaccesible → comportamiento equivalente a la HU 1.2
    console.error('[globalEquivalenceRepository] Error reading catalog:', error);
    return {};
  }

  return result;
}

// ============================================================================
// EVIDENCE WRITE
// ============================================================================

/**
 * Registra que un usuario confirmó una equivalencia.
 *
 * Idempotente por usuario: el id del documento es su hash, así que confirmar diez
 * veces la misma equivalencia cuenta una sola (RN-07, escenario 5).
 *
 * @param {Object} params
 * @param {string} params.key - Clave de la equivalencia
 * @param {string} params.userId
 * @param {string} params.salt
 * @param {string} params.resolvedSymbol - Ticker canónico confirmado
 * @returns {Promise<void>}
 */
async function recordConfirmation({ key, userId, salt, resolvedSymbol }) {
  if (!key || !userId || !resolvedSymbol) {
    return;
  }

  const uidHash = hashUserId(userId, salt);

  await db
    .collection(EVIDENCE_COLLECTION)
    .doc(key)
    .collection(CONFIRMATIONS_SUBCOLLECTION)
    .doc(uidHash)
    .set({
      resolvedSymbol,
      at: new Date().toISOString(),
    });
}

/**
 * Registra que un usuario contradijo el valor que proponía el catálogo global.
 *
 * RN-33: una corrección cuenta en ambos sentidos. El llamador debe invocar también
 * `recordConfirmation` con el valor nuevo.
 *
 * @param {Object} params
 * @param {string} params.key
 * @param {string} params.userId
 * @param {string} params.salt
 * @param {string} params.against - Ticker canónico que el usuario rechazó
 * @returns {Promise<void>}
 */
async function recordContradiction({ key, userId, salt, against }) {
  if (!key || !userId || !against) {
    return;
  }

  const uidHash = hashUserId(userId, salt);

  await db
    .collection(EVIDENCE_COLLECTION)
    .doc(key)
    .collection(CONTRADICTIONS_SUBCOLLECTION)
    .doc(uidHash)
    .set({
      against,
      at: new Date().toISOString(),
    });
}

// ============================================================================
// PROMOTION (RN-07)
// ============================================================================

/**
 * Evalúa si una equivalencia alcanzó el umbral de promoción y, si es así, la
 * publica en el catálogo.
 *
 * @param {Object} params
 * @param {string} params.key
 * @param {string} params.sourceFormatId
 * @param {string} params.sourceSymbol
 * @param {string} params.resolvedSymbol
 * @param {string} [params.assetType]
 * @param {string} [params.currency]
 * @returns {Promise<boolean>} true si se promovió en esta evaluación
 */
async function evaluatePromotion({
  key,
  sourceFormatId,
  sourceSymbol,
  resolvedSymbol,
  assetType,
  currency,
}) {
  if (!key || !resolvedSymbol) {
    return false;
  }

  // Conteo de usuarios DISTINTOS que confirmaron ESTE ticker canónico.
  // Cada documento es un usuario (su id es el hash del uid).
  const confirmations = await db
    .collection(EVIDENCE_COLLECTION)
    .doc(key)
    .collection(CONFIRMATIONS_SUBCOLLECTION)
    .where('resolvedSymbol', '==', resolvedSymbol)
    .count()
    .get();

  const distinctUsers = confirmations.data().count;

  if (distinctUsers < GLOBAL_EQUIVALENCE_THRESHOLDS.promoteDistinctUsers) {
    return false;
  }

  const catalogRef = db.collection(CATALOG_COLLECTION).doc(key);
  const existing = await catalogRef.get();

  // Ya está publicada con ese mismo valor: nada que hacer
  if (existing.exists
      && existing.data().resolvedSymbol === resolvedSymbol
      && existing.data().status === GLOBAL_EQUIVALENCE_STATUS.ACTIVE) {
    return false;
  }

  // RN-30: el documento del catálogo NO contiene ningún dato de usuario.
  // assetType y currency son propiedades del ticker canónico, no del usuario.
  await catalogRef.set({
    sourceFormatId,
    sourceSymbol,
    resolvedSymbol,
    assetType: assetType || 'stock',
    currency: currency || 'USD',
    status: GLOBAL_EQUIVALENCE_STATUS.ACTIVE,
    promotedAt: new Date().toISOString(),
  });

  await writeAudit({
    key,
    action: GLOBAL_EQUIVALENCE_AUDIT_ACTIONS.PROMOTED,
    actor: 'system',
    reason: `${distinctUsers} usuarios distintos confirmaron ${resolvedSymbol}`,
  });

  console.log(`[globalEquivalenceRepository] Promoted ${key} -> ${resolvedSymbol} (${distinctUsers} users)`);

  return true;
}

// ============================================================================
// RETIREMENT (RN-31)
// ============================================================================

/**
 * Evalúa si una entrada activa acumuló contradicción sostenida y, si es así, la
 * retira.
 *
 * @param {string} key
 * @returns {Promise<boolean>} true si se retiró en esta evaluación
 */
async function evaluateRetirement(key) {
  if (!key) {
    return false;
  }

  const catalogRef = db.collection(CATALOG_COLLECTION).doc(key);
  const existing = await catalogRef.get();

  if (!existing.exists || existing.data().status !== GLOBAL_EQUIVALENCE_STATUS.ACTIVE) {
    return false;
  }

  const activeSymbol = existing.data().resolvedSymbol;
  const cutoff = windowCutoff(GLOBAL_EQUIVALENCE_THRESHOLDS.retireWindowDays);

  // Usuarios distintos que contradijeron EL VALOR ACTIVO dentro de la ventana
  const contradictions = await db
    .collection(EVIDENCE_COLLECTION)
    .doc(key)
    .collection(CONTRADICTIONS_SUBCOLLECTION)
    .where('against', '==', activeSymbol)
    .where('at', '>=', cutoff)
    .count()
    .get();

  const distinctUsers = contradictions.data().count;

  if (distinctUsers < GLOBAL_EQUIVALENCE_THRESHOLDS.retireDistinctUsers) {
    return false;
  }

  // RN-32: retirar solo cambia el estado. No se toca ninguna transacción.
  await catalogRef.update({
    status: GLOBAL_EQUIVALENCE_STATUS.RETIRED,
    retiredAt: new Date().toISOString(),
  });

  await writeAudit({
    key,
    action: GLOBAL_EQUIVALENCE_AUDIT_ACTIONS.RETIRED_BY_EVIDENCE,
    actor: 'system',
    reason: `${distinctUsers} usuarios distintos contradijeron ${activeSymbol} en ${GLOBAL_EQUIVALENCE_THRESHOLDS.retireWindowDays} días`,
  });

  console.log(`[globalEquivalenceRepository] Retired ${key} (${distinctUsers} contradictions)`);

  return true;
}

/**
 * Retiro operativo inmediato por parte del equipo de producto (escenario 7).
 *
 * @param {Object} params
 * @param {string} params.key
 * @param {string} params.reason
 * @param {string} params.actor - Quién ejecuta el retiro
 * @returns {Promise<boolean>} true si la entrada existía y quedó retirada
 */
async function retireEntry({ key, reason, actor }) {
  const catalogRef = db.collection(CATALOG_COLLECTION).doc(key);
  const existing = await catalogRef.get();

  if (!existing.exists) {
    return false;
  }

  await catalogRef.update({
    status: GLOBAL_EQUIVALENCE_STATUS.RETIRED,
    retiredAt: new Date().toISOString(),
  });

  // DoD: la utilidad debe dejar registro de qué entrada se retiró y cuándo
  await writeAudit({
    key,
    action: GLOBAL_EQUIVALENCE_AUDIT_ACTIONS.RETIRED_BY_OPERATOR,
    actor: actor || 'unknown',
    reason: reason || 'Retiro operativo',
  });

  return true;
}

// ============================================================================
// HELPERS
// ============================================================================

/**
 * Fecha ISO de inicio de la ventana de contradicción.
 *
 * @param {number} days
 * @returns {string} ISO
 */
function windowCutoff(days) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);

  return cutoff.toISOString();
}

/**
 * Escribe una entrada de auditoría del catálogo global.
 *
 * No contiene datos de usuario: `actor` es 'system' o el operador que ejecutó el
 * retiro, nunca el usuario final cuya confirmación disparó el cambio.
 *
 * @param {Object} params
 * @returns {Promise<void>}
 */
async function writeAudit({ key, action, actor, reason }) {
  try {
    await db.collection(AUDIT_COLLECTION).add({
      key,
      action,
      actor,
      reason,
      at: new Date().toISOString(),
    });
  } catch (error) {
    // La auditoría no debe impedir el cambio que documenta
    console.error('[globalEquivalenceRepository] Could not write audit entry:', error);
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Lectura del catálogo
  getActiveEntries,

  // Evidencia
  recordConfirmation,
  recordContradiction,

  // Umbrales
  evaluatePromotion,
  evaluateRetirement,

  // Operación
  retireEntry,

  // Utilidades
  hashUserId,
  buildEquivalenceKey,

  // For testing
  windowCutoff,
  writeAudit,
  CATALOG_COLLECTION,
  EVIDENCE_COLLECTION,
  AUDIT_COLLECTION,
  CONFIRMATIONS_SUBCOLLECTION,
  CONTRADICTIONS_SUBCOLLECTION,
};
