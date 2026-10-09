/**
 * Format Fingerprint Service
 *
 * HU 1.1: Identifica de forma determinista el "formato" de un archivo de broker,
 * para poder recordar el mapeo de columnas que el usuario confirmó (O-1) y para
 * servir de parte de la clave de las equivalencias de símbolo (O-2, RN-16).
 *
 * La huella se calcula SOLO aquí (backend) y viaja al frontend en la respuesta de
 * analyzeTransactionFile. Así no se duplica la lógica en JS y TS.
 *
 * @module transactions/services/formatFingerprint
 */

const crypto = require('crypto');

// ============================================================================
// CONSTANTS
// ============================================================================

/** Longitud del hash truncado que se usa en el id de formato */
const FINGERPRINT_LENGTH = 16;

/** Prefijo de los formatos identificados por broker reconocido */
const BROKER_PREFIX = 'broker:';

/** Prefijo de los formatos identificados por huella de cabeceras */
const FORMAT_PREFIX = 'fmt:';

// ============================================================================
// FUNCTIONS
// ============================================================================

/**
 * Normaliza una cabecera para que la huella sea estable frente a diferencias
 * irrelevantes (espacios, mayúsculas, espacios internos repetidos).
 *
 * @param {*} header - Valor crudo de la cabecera
 * @returns {string} Cabecera normalizada
 */
function normalizeHeader(header) {
  return String(header === null || header === undefined ? '' : header)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

/**
 * Calcula la huella de las cabeceras de un archivo.
 *
 * Es posicional a propósito: dos archivos con las mismas cabeceras en distinto
 * orden son formatos distintos, porque el mapeo columna → campo no sería el mismo.
 *
 * @param {string[]} headers - Cabeceras del archivo
 * @returns {string} Huella hexadecimal truncada
 */
function computeHeaderFingerprint(headers) {
  const normalized = (headers || []).map(normalizeHeader);
  const payload = `${normalized.join('|')}#${normalized.length}`;

  return crypto
    .createHash('sha1')
    .update(payload, 'utf8')
    .digest('hex')
    .slice(0, FINGERPRINT_LENGTH);
}

/**
 * Construye el identificador de formato de origen.
 *
 * Precedencia:
 * 1. Broker reconocido → `broker:<brokerId>`. Es el identificador más estable y el
 *    que permite que dos usuarios del mismo broker compartan clave (necesario para
 *    el catálogo global de la HU 1.6).
 * 2. Con cabeceras → `fmt:<huella>`. Determinista entre usuarios con el mismo export.
 * 3. Sin cabeceras → `fmt:cols<N>`. Lo único estable que queda es la forma del archivo.
 *
 * @param {Object} params
 * @param {string|null} params.detectedBroker - Broker detectado, si hubo
 * @param {string[]|null} params.headers - Cabeceras del archivo (null si no hay)
 * @param {number} params.columnCount - Número de columnas del archivo
 * @returns {string} Identificador de formato de origen
 *
 * @example
 * buildSourceFormatId({ detectedBroker: 'degiro', headers: [...], columnCount: 9 })
 * // → 'broker:degiro'
 */
function buildSourceFormatId({ detectedBroker, headers, columnCount }) {
  if (detectedBroker) {
    return `${BROKER_PREFIX}${detectedBroker}`;
  }

  if (headers && headers.length > 0) {
    return `${FORMAT_PREFIX}${computeHeaderFingerprint(headers)}`;
  }

  return `${FORMAT_PREFIX}cols${columnCount || 0}`;
}

/**
 * Indica si un identificador de formato corresponde a un broker reconocido.
 *
 * @param {string} sourceFormatId
 * @returns {boolean}
 */
function isBrokerFormat(sourceFormatId) {
  return typeof sourceFormatId === 'string' && sourceFormatId.startsWith(BROKER_PREFIX);
}

/**
 * Valida que un identificador de formato tenga una forma aceptable.
 * Se usa para rechazar payloads manipulados en las Cloud Functions de escritura.
 *
 * @param {*} sourceFormatId
 * @returns {boolean}
 */
function isValidSourceFormatId(sourceFormatId) {
  if (typeof sourceFormatId !== 'string' || sourceFormatId.length === 0) {
    return false;
  }

  if (sourceFormatId.length > 120) {
    return false;
  }

  return /^(broker:[a-z0-9_]+|fmt:(cols\d+|[0-9a-f]{16}))$/.test(sourceFormatId);
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  buildSourceFormatId,
  computeHeaderFingerprint,
  normalizeHeader,
  isBrokerFormat,
  isValidSourceFormatId,

  // For testing
  FINGERPRINT_LENGTH,
  BROKER_PREFIX,
  FORMAT_PREFIX,
};
