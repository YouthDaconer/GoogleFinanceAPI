/**
 * Import Memory Repository
 *
 * HU 1.1: Memoria interna del sistema para el canal de importación.
 * Guarda el mapeo de columnas que el usuario confirmó, por formato de origen,
 * para que la siguiente carga del mismo formato llegue ya resuelta.
 *
 * PRINCIPIOS:
 * - RN-02: es memoria invisible. No hay CRUD ni pantalla; el usuario no nombra nada.
 * - RN-03: se escribe SOLO cuando el usuario confirmó una importación completa.
 * - RN-04: un perfil que dejó de coincidir se descarta en silencio.
 * - RN-15: la cuenta destino NUNCA se guarda.
 *
 * Ubicación: userData/{userId}/importProfiles/{sourceFormatId}
 * El acceso es exclusivamente server-side (admin SDK); firestore.rules deniega
 * cualquier acceso del cliente a esta subcolección.
 *
 * @module transactions/services/importMemoryRepository
 */

const admin = require('../../firebaseAdmin');
const { normalizeHeader } = require('./formatFingerprint');

const db = admin.firestore();

// ============================================================================
// CONSTANTS
// ============================================================================

const USER_DATA_COLLECTION = 'userData';
const PROFILES_SUBCOLLECTION = 'importProfiles';
const EQUIVALENCES_SUBCOLLECTION = 'tickerEquivalences';

/** Campos de transacción que pueden aparecer en un mapeo persistido */
const PERSISTABLE_MAPPING_KEYS = ['sourceColumn', 'sourceHeader', 'targetField'];

/** Separador de la clave compuesta formato + símbolo (RN-16) */
const EQUIVALENCE_KEY_SEPARATOR = '::';

/**
 * Firestore limita los `in` a 30 valores por consulta. Las equivalencias se leen
 * por id de documento, así que se lee en lotes de este tamaño.
 */
const EQUIVALENCE_READ_BATCH_SIZE = 30;

// ============================================================================
// TYPES (JSDoc)
// ============================================================================

/**
 * @typedef {Object} PersistedMapping
 * @property {number} sourceColumn - Índice de columna (0-based)
 * @property {string} sourceHeader - Cabecera original de esa columna
 * @property {string} targetField - Campo de transacción al que se mapea
 */

/**
 * @typedef {Object} ImportProfile
 * @property {string} sourceFormatId
 * @property {PersistedMapping[]} mappings
 * @property {Object} defaultValues - { type, currency, commission }
 * @property {boolean} hasHeader
 * @property {number} headerRowIndex
 * @property {string|null} detectedBroker
 * @property {number} confirmedImportCount
 * @property {string} updatedAt - ISO
 */

// ============================================================================
// PROFILE READ
// ============================================================================

/**
 * Lee el perfil recordado de un formato para un usuario.
 *
 * @param {string} userId
 * @param {string} sourceFormatId
 * @returns {Promise<ImportProfile|null>}
 */
async function getProfile(userId, sourceFormatId) {
  if (!userId || !sourceFormatId) {
    return null;
  }

  try {
    const doc = await db
      .collection(USER_DATA_COLLECTION)
      .doc(userId)
      .collection(PROFILES_SUBCOLLECTION)
      .doc(sourceFormatId)
      .get();

    if (!doc.exists) {
      return null;
    }

    return { sourceFormatId, ...doc.data() };
  } catch (error) {
    // RN-11: la ausencia de memoria nunca puede degradar el asistente.
    // Si la lectura falla, el usuario simplemente ve el comportamiento actual.
    console.error(`[importMemoryRepository] Error reading profile ${sourceFormatId}:`, error);
    return null;
  }
}

// ============================================================================
// PROFILE VALIDATION (RN-04)
// ============================================================================

/**
 * Verifica si un perfil recordado sigue siendo aplicable al archivo cargado.
 *
 * Un perfil deja de ser válido cuando el broker cambió su formato: la columna que
 * el perfil apunta ya no existe, o ya no tiene la misma cabecera. En ese caso se
 * descarta EN SILENCIO (RN-04): sin advertencia ni mensaje de error al usuario.
 *
 * @param {ImportProfile|null} profile
 * @param {string[]|null} headers - Cabeceras del archivo actual (null si no hay header)
 * @param {number} columnCount - Número de columnas del archivo actual
 * @returns {boolean} true si el perfil sigue vigente
 */
function isProfileStillValid(profile, headers, columnCount) {
  if (!profile || !Array.isArray(profile.mappings) || profile.mappings.length === 0) {
    return false;
  }

  const totalColumns = typeof columnCount === 'number'
    ? columnCount
    : (headers ? headers.length : 0);

  for (const mapping of profile.mappings) {
    const index = mapping.sourceColumn;

    // La columna referenciada tiene que seguir existiendo
    if (typeof index !== 'number' || index < 0 || index >= totalColumns) {
      return false;
    }

    // Si el archivo tiene cabeceras, la de esa posición tiene que ser la misma.
    // Si el perfil se guardó sin cabeceras, solo se valida la forma del archivo.
    if (headers && profile.hasHeader) {
      if (normalizeHeader(headers[index]) !== normalizeHeader(mapping.sourceHeader)) {
        return false;
      }
    }
  }

  return true;
}

// ============================================================================
// PROFILE WRITE
// ============================================================================

/**
 * Reduce un mapeo del wizard a los campos que se persisten.
 * Confianza, método de detección y valores de muestra son datos de la sesión de
 * análisis, no del formato: no se guardan.
 *
 * @param {Object[]} mappings
 * @returns {PersistedMapping[]}
 */
function sanitizeMappings(mappings) {
  if (!Array.isArray(mappings)) {
    return [];
  }

  return mappings
    .filter((m) => m
      && typeof m.sourceColumn === 'number'
      && m.sourceColumn >= 0
      && typeof m.targetField === 'string'
      && m.targetField.length > 0)
    .map((m) => ({
      sourceColumn: m.sourceColumn,
      sourceHeader: String(m.sourceHeader || ''),
      targetField: m.targetField,
    }));
}

/**
 * Normaliza los valores por defecto que el usuario dejó en el paso de mapeo.
 *
 * @param {Object} defaultValues
 * @returns {Object}
 */
function sanitizeDefaultValues(defaultValues) {
  const source = defaultValues || {};
  const type = String(source.type || 'buy').toLowerCase();
  const commission = Number(source.commission);

  return {
    type: type === 'sell' ? 'sell' : 'buy',
    currency: String(source.currency || 'USD').toUpperCase().slice(0, 8),
    commission: Number.isFinite(commission) && commission >= 0 ? commission : 0,
  };
}

/**
 * Guarda (o reemplaza) el perfil de un formato.
 *
 * RN-14: coexisten varios perfiles, uno por formato. El confirmado reemplaza al
 * anterior DEL MISMO formato, nunca los de otros brokers.
 * RN-15: portfolioAccountId no se recibe ni se guarda.
 *
 * @param {string} userId
 * @param {Object} params
 * @param {string} params.sourceFormatId
 * @param {Object[]} params.mappings
 * @param {Object} params.defaultValues
 * @param {boolean} params.hasHeader
 * @param {number} params.headerRowIndex
 * @param {string|null} params.detectedBroker
 * @returns {Promise<boolean>} true si se guardó
 */
async function saveProfile(userId, params) {
  const {
    sourceFormatId,
    mappings,
    defaultValues,
    hasHeader,
    headerRowIndex,
    detectedBroker,
  } = params || {};

  const sanitizedMappings = sanitizeMappings(mappings);

  if (!userId || !sourceFormatId || sanitizedMappings.length === 0) {
    return false;
  }

  const ref = db
    .collection(USER_DATA_COLLECTION)
    .doc(userId)
    .collection(PROFILES_SUBCOLLECTION)
    .doc(sourceFormatId);

  const existing = await ref.get();
  const previousCount = existing.exists ? (existing.data().confirmedImportCount || 0) : 0;

  await ref.set({
    sourceFormatId,
    mappings: sanitizedMappings,
    defaultValues: sanitizeDefaultValues(defaultValues),
    hasHeader: hasHeader !== false,
    headerRowIndex: typeof headerRowIndex === 'number' ? headerRowIndex : 0,
    detectedBroker: detectedBroker || null,
    confirmedImportCount: previousCount + 1,
    updatedAt: new Date().toISOString(),
  });

  return true;
}

// ============================================================================
// TICKER EQUIVALENCES (HU 1.2)
// ============================================================================

/**
 * Normaliza un símbolo de archivo para usarlo como parte de la clave.
 *
 * @param {*} symbol
 * @returns {string}
 */
function normalizeSymbol(symbol) {
  return String(symbol === null || symbol === undefined ? '' : symbol)
    .trim()
    .toUpperCase();
}

/**
 * Construye la clave de una equivalencia.
 *
 * RN-16: la clave incluye el formato de origen. Es lo que impide que el `ETH` de
 * una plataforma de criptomonedas contamine el `ETH` de un broker de renta
 * variable: son dos documentos distintos.
 *
 * @param {string} sourceFormatId
 * @param {string} symbol
 * @returns {string} Clave del documento
 */
function buildEquivalenceKey(sourceFormatId, symbol) {
  return `${sourceFormatId}${EQUIVALENCE_KEY_SEPARATOR}${normalizeSymbol(symbol)}`;
}

/**
 * Lee las equivalencias propias del usuario para un conjunto de símbolos.
 *
 * @param {string} userId
 * @param {string} sourceFormatId
 * @param {string[]} symbols - Símbolos tal como vienen en el archivo
 * @returns {Promise<Object>} Mapa símbolo normalizado → equivalencia
 */
async function getUserEquivalences(userId, sourceFormatId, symbols) {
  if (!userId || !sourceFormatId || !Array.isArray(symbols) || symbols.length === 0) {
    return {};
  }

  const uniqueSymbols = [...new Set(symbols.map(normalizeSymbol).filter(Boolean))];

  if (uniqueSymbols.length === 0) {
    return {};
  }

  const collectionRef = db
    .collection(USER_DATA_COLLECTION)
    .doc(userId)
    .collection(EQUIVALENCES_SUBCOLLECTION);

  const result = {};

  try {
    // Lectura por lotes de referencias concretas: no requiere índice compuesto.
    for (let i = 0; i < uniqueSymbols.length; i += EQUIVALENCE_READ_BATCH_SIZE) {
      const batch = uniqueSymbols.slice(i, i + EQUIVALENCE_READ_BATCH_SIZE);
      const refs = batch.map(symbol => collectionRef.doc(buildEquivalenceKey(sourceFormatId, symbol)));
      const docs = await db.getAll(...refs);

      docs.forEach((doc) => {
        if (!doc.exists) {
          return;
        }

        const data = doc.data();

        if (!data.resolvedSymbol || !data.sourceSymbol) {
          return;
        }

        result[normalizeSymbol(data.sourceSymbol)] = {
          sourceSymbol: normalizeSymbol(data.sourceSymbol),
          resolvedSymbol: data.resolvedSymbol,
          assetType: data.assetType || 'stock',
          currency: data.currency || 'USD',
          exchange: data.exchange || null,
          name: data.name || null,
          origin: 'user',
        };
      });
    }
  } catch (error) {
    // RN-11: sin memoria, sin regresión. Si la lectura falla, el usuario resuelve
    // los símbolos como lo hace hoy.
    console.error('[importMemoryRepository] Error reading equivalences:', error);
    return {};
  }

  return result;
}

/**
 * Normaliza una equivalencia recibida del cliente antes de persistirla.
 *
 * @param {Object} equivalence
 * @returns {Object|null} Equivalencia saneada, o null si no es utilizable
 */
function sanitizeEquivalence(equivalence) {
  if (!equivalence) {
    return null;
  }

  const sourceSymbol = normalizeSymbol(equivalence.sourceSymbol);
  const resolvedSymbol = String(equivalence.resolvedSymbol || '').trim();

  if (!sourceSymbol || !resolvedSymbol) {
    return null;
  }

  // Límite defensivo: son símbolos de mercado, no texto libre
  if (sourceSymbol.length > 40 || resolvedSymbol.length > 40) {
    return null;
  }

  return {
    sourceSymbol,
    resolvedSymbol,
    assetType: String(equivalence.assetType || 'stock'),
    currency: String(equivalence.currency || 'USD').toUpperCase().slice(0, 8),
    exchange: equivalence.exchange ? String(equivalence.exchange).slice(0, 40) : null,
    name: equivalence.name ? String(equivalence.name).slice(0, 200) : null,
  };
}

/**
 * Guarda (o reemplaza) las equivalencias confirmadas del usuario.
 *
 * RN-05: la confirmación más reciente del usuario manda. Se hace `set` sin merge
 * sobre el documento de la clave, así que una corrección reemplaza limpiamente la
 * equivalencia anterior en lugar de fusionarse con ella.
 *
 * @param {string} userId
 * @param {string} sourceFormatId
 * @param {Object[]} equivalences - Equivalencias aplicadas en la importación confirmada
 * @returns {Promise<number>} Cuántas se guardaron
 */
async function saveUserEquivalences(userId, sourceFormatId, equivalences) {
  if (!userId || !sourceFormatId || !Array.isArray(equivalences) || equivalences.length === 0) {
    return 0;
  }

  const sanitized = equivalences
    .map(sanitizeEquivalence)
    .filter(Boolean);

  if (sanitized.length === 0) {
    return 0;
  }

  const collectionRef = db
    .collection(USER_DATA_COLLECTION)
    .doc(userId)
    .collection(EQUIVALENCES_SUBCOLLECTION);

  const updatedAt = new Date().toISOString();
  const batch = db.batch();

  for (const equivalence of sanitized) {
    const ref = collectionRef.doc(buildEquivalenceKey(sourceFormatId, equivalence.sourceSymbol));

    batch.set(ref, {
      ...equivalence,
      sourceFormatId,
      updatedAt,
    });
  }

  await batch.commit();

  return sanitized.length;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // HU 1.1: perfil de mapeo
  getProfile,
  saveProfile,
  isProfileStillValid,

  // HU 1.2: equivalencias de símbolo
  getUserEquivalences,
  saveUserEquivalences,
  buildEquivalenceKey,

  // For testing
  sanitizeMappings,
  sanitizeDefaultValues,
  sanitizeEquivalence,
  normalizeSymbol,
  PERSISTABLE_MAPPING_KEYS,
  USER_DATA_COLLECTION,
  PROFILES_SUBCOLLECTION,
  EQUIVALENCES_SUBCOLLECTION,
  EQUIVALENCE_KEY_SEPARATOR,
};
