/**
 * Cloud Function: saveImportMemory
 *
 * HU 1.1: Persiste la memoria interna del canal de importación después de que el
 * usuario confirmó una importación.
 *
 * Es el ÚNICO punto de escritura de la memoria de importación. Las historias 1.2
 * (equivalencias propias) y 1.6 (catálogo global) extienden esta misma función.
 *
 * REGLAS QUE IMPLEMENTA:
 * - RN-03: solo se llama tras una importación confirmada. Un asistente abandonado
 *   nunca llega aquí, así que no deja memoria.
 * - RN-14: coexisten varios perfiles, uno por formato de origen.
 * - RN-15: la cuenta destino no se recibe ni se guarda.
 *
 * @module transactions/saveImportMemory
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");

// GATE-006: Validación de features por plan
const { validateFeatureAccess } = require('../helpers/subscriptionValidator');

const { isValidSourceFormatId } = require('./services/formatFingerprint');
const {
  saveProfile,
  saveUserEquivalences,
  buildEquivalenceKey,
} = require('./services/importMemoryRepository');
// HU 1.6: evidencia y umbrales del catálogo global
const {
  recordConfirmation,
  recordContradiction,
  evaluatePromotion,
  evaluateRetirement,
} = require('./services/globalEquivalenceRepository');

// ============================================================================
// SECRET DEFINITIONS
// ============================================================================

/**
 * HU 1.6: secreto con el que se anonimiza el uid en la evidencia del catálogo
 * global. Sin él, el conteo de usuarios distintos exigiría almacenar identidades
 * reversibles, lo que contradice RN-30.
 */
const equivHashSalt = defineSecret("EQUIV_HASH_SALT");

// ============================================================================
// LIMITS
// ============================================================================

/**
 * Tope defensivo de equivalencias por invocación. Un archivo con más símbolos
 * distintos que esto es un caso extremo; el exceso simplemente no se recuerda.
 */
const MAX_EQUIVALENCES_PER_CALL = 500;

// ============================================================================
// CLOUD FUNCTION CONFIGURATION
// ============================================================================

/**
 * Escritura pequeña y acotada: unos pocos documentos por invocación.
 */
const FUNCTION_CONFIG = {
  cors: true,
  memory: "256MiB",
  // HU 1.6: la evaluación de umbrales añade consultas de conteo por símbolo
  timeoutSeconds: 60,
  maxInstances: 20,
  minInstances: 0,
  region: 'us-central1',
  secrets: [equivHashSalt],
};

// ============================================================================
// MAIN CLOUD FUNCTION
// ============================================================================

/**
 * Guarda la memoria de importación del usuario.
 *
 * @param {Object} request - Cloud Function request
 * @param {Object} request.data - Payload
 * @param {string} request.data.sourceFormatId - Identidad del formato (de analyzeTransactionFile)
 * @param {Object[]} request.data.mappings - Mapeo confirmado columna → campo
 * @param {Object} request.data.defaultValues - Valores por defecto confirmados
 * @param {boolean} request.data.hasHeader
 * @param {number} request.data.headerRowIndex
 * @param {string|null} request.data.detectedBroker
 * @returns {Object} { success, profileSaved }
 */
const saveImportMemory = onCall(
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
        'Usuario debe estar autenticado para guardar la memoria de importación'
      );
    }

    const userId = auth.uid;

    // GATE-006 / RN: sin plan que habilite importación no se crea ninguna memoria
    await validateFeatureAccess(userId, 'hasImport');

    // ─────────────────────────────────────────────────────────────────────
    // 2. PAYLOAD VALIDATION
    // ─────────────────────────────────────────────────────────────────────
    const {
      sourceFormatId,
      mappings,
      defaultValues,
      hasHeader,
      headerRowIndex,
      detectedBroker,
      // HU 1.2: equivalencias aplicadas en la importación confirmada
      equivalences,
    } = data || {};

    if (!isValidSourceFormatId(sourceFormatId)) {
      throw new HttpsError(
        'invalid-argument',
        'sourceFormatId inválido'
      );
    }

    if (!Array.isArray(mappings)) {
      throw new HttpsError(
        'invalid-argument',
        'mappings debe ser un array'
      );
    }

    if (equivalences !== undefined && !Array.isArray(equivalences)) {
      throw new HttpsError(
        'invalid-argument',
        'equivalences debe ser un array'
      );
    }

    // ─────────────────────────────────────────────────────────────────────
    // 3. PERSIST PROFILE (HU 1.1)
    // ─────────────────────────────────────────────────────────────────────
    let profileSaved = false;

    try {
      profileSaved = await saveProfile(userId, {
        sourceFormatId,
        mappings,
        defaultValues,
        hasHeader,
        headerRowIndex,
        detectedBroker,
      });
    } catch (error) {
      // La memoria es una optimización, no parte del historial financiero.
      // Un fallo aquí no debe presentarse como un fallo de la importación.
      console.error('[saveImportMemory] Failed to save profile:', error);
      throw new HttpsError(
        'internal',
        'No se pudo guardar la memoria de importación'
      );
    }

    // ─────────────────────────────────────────────────────────────────────
    // 4. PERSIST TICKER EQUIVALENCES (HU 1.2)
    // ─────────────────────────────────────────────────────────────────────
    // El perfil y las equivalencias son memorias independientes: si las
    // equivalencias fallan, el mapeo recordado ya quedó guardado y sigue siendo
    // útil. No se revierte.
    let equivalencesSaved = 0;
    const boundedEquivalences = Array.isArray(equivalences)
      ? equivalences.slice(0, MAX_EQUIVALENCES_PER_CALL)
      : [];

    if (boundedEquivalences.length > 0) {
      try {
        equivalencesSaved = await saveUserEquivalences(
          userId,
          sourceFormatId,
          boundedEquivalences
        );
      } catch (error) {
        console.error('[saveImportMemory] Failed to save equivalences:', error);
      }
    }

    // ─────────────────────────────────────────────────────────────────────
    // 5. GLOBAL CATALOG EVIDENCE (HU 1.6)
    // ─────────────────────────────────────────────────────────────────────
    // El catálogo global es una optimización derivada de la memoria propia. Si esta
    // parte falla, la importación y la memoria del usuario ya quedaron correctas.
    let evidenceRecorded = 0;

    if (boundedEquivalences.length > 0) {
      try {
        evidenceRecorded = await recordGlobalEvidence({
          userId,
          sourceFormatId,
          equivalences: boundedEquivalences,
          salt: equivHashSalt.value(),
        });
      } catch (error) {
        console.error('[saveImportMemory] Failed to record global evidence:', error);
      }
    }

    const duration = Date.now() - startTime;
    console.log(
      `[saveImportMemory] Complete in ${duration}ms - profileSaved: ${profileSaved}, `
      + `equivalencesSaved: ${equivalencesSaved}, evidenceRecorded: ${evidenceRecorded}`
    );

    return {
      success: true,
      profileSaved,
      equivalencesSaved,
      evidenceRecorded,
      processingTimeMs: duration,
    };
  }
);

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * HU 1.6: registra la evidencia de cada equivalencia confirmada y re-evalúa los
 * umbrales del catálogo global.
 *
 * RN-33 — la corrección cuenta en ambos sentidos: si el usuario confirmó un ticker
 * distinto del que le propuso el catálogo global, se escribe simultáneamente
 *   - una CONFIRMACIÓN a favor del valor nuevo (evidencia de promoción, RN-07), y
 *   - una CONTRADICCIÓN contra el valor anterior (evidencia de retiro, RN-31).
 *
 * `proposedFrom` viaja desde el frontend, que lo obtuvo del `origin` de la
 * equivalencia que le devolvió el análisis. Solo se toma en cuenta cuando el origen
 * era el catálogo global: contradecir la memoria propia del usuario no tiene sentido.
 *
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.sourceFormatId
 * @param {Object[]} params.equivalences
 * @param {string} params.salt
 * @returns {Promise<number>} Cuántas equivalencias generaron evidencia
 */
async function recordGlobalEvidence({ userId, sourceFormatId, equivalences, salt }) {
  let recorded = 0;

  for (const equivalence of equivalences) {
    const sourceSymbol = String(equivalence?.sourceSymbol || '').trim().toUpperCase();
    const resolvedSymbol = String(equivalence?.resolvedSymbol || '').trim();

    if (!sourceSymbol || !resolvedSymbol) {
      continue;
    }

    const key = buildEquivalenceKey(sourceFormatId, sourceSymbol);

    // Confirmación a favor del valor que el usuario acaba de confirmar
    await recordConfirmation({ key, userId, salt, resolvedSymbol });

    // RN-33: contradicción contra lo que proponía el catálogo, si difiere
    const proposedFrom = equivalence.proposedFrom;
    const wasGlobalProposal = proposedFrom
      && proposedFrom.origin === 'global'
      && proposedFrom.resolvedSymbol
      && proposedFrom.resolvedSymbol !== resolvedSymbol;

    if (wasGlobalProposal) {
      await recordContradiction({
        key,
        userId,
        salt,
        against: proposedFrom.resolvedSymbol,
      });

      // El retiro se evalúa solo cuando hubo contradicción: es lo único que puede
      // haber cambiado el conteo.
      await evaluateRetirement(key);
    }

    await evaluatePromotion({
      key,
      sourceFormatId,
      sourceSymbol,
      resolvedSymbol,
      assetType: equivalence.assetType,
      currency: equivalence.currency,
    });

    recorded++;
  }

  return recorded;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  saveImportMemory,

  // For testing
  recordGlobalEvidence,
};
