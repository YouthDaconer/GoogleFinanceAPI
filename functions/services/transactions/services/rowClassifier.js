/**
 * Row Classifier Service
 *
 * HU 1.3: clasifica las filas de una reimportación en NUEVAS y YA REGISTRADAS
 * ANTES de que el usuario confirme, para que la decisión sea suya (RN-08).
 *
 * Reutiliza el criterio de duplicado vigente del canal (`duplicateDetector`):
 * misma firma TICKER|FECHA|CANTIDAD|PRECIO|TIPO|CUENTA. No define un criterio
 * propio — si el criterio cambia, cambia en un solo sitio.
 *
 * El tercer grupo, RECHAZADAS, NO se calcula aquí: son las filas que no pasaron la
 * validación local, algo que el cliente ya sabe sin consultar nada. Este servicio
 * solo responde la pregunta que requiere leer Firestore.
 *
 * @module transactions/services/rowClassifier
 */

const {
  createSignature,
  getExistingTransactionsForTicker,
} = require('./duplicateDetector');

// ============================================================================
// CONSTANTS
// ============================================================================

/** Grupos que decide este servicio */
const ROW_GROUP = {
  NEW: 'new',
  EXISTING: 'existing',
};

// ============================================================================
// TYPES (JSDoc)
// ============================================================================

/**
 * @typedef {Object} RowToClassify
 * @property {number} originalRowNumber - Número de fila en el archivo
 * @property {string} ticker - Ticker YA RESUELTO (el que se escribiría como assetName)
 * @property {string} date
 * @property {'buy'|'sell'} type
 * @property {number} amount
 * @property {number} price
 */

/**
 * @typedef {Object} ClassificationResult
 * @property {Object.<number, string>} classification - rowNumber → 'new' | 'existing'
 * @property {{ new: number, existing: number }} counts
 */

// ============================================================================
// MAIN
// ============================================================================

/**
 * Clasifica las filas contra el historial del usuario.
 *
 * Replica la lógica de conteo de ocurrencias de `duplicateDetector`: tres ventas
 * idénticas del mismo activo el mismo día a igual precio son operaciones legítimas
 * distintas, no duplicados. Solo se marca como "ya registrada" la ocurrencia que
 * excede lo que ya existe en el historial.
 *
 * @param {RowToClassify[]} rows - Filas válidas a clasificar
 * @param {string} userId
 * @param {string} portfolioAccountId - Cuenta destino de esta importación
 * @returns {Promise<ClassificationResult>}
 */
async function classifyRows(rows, userId, portfolioAccountId) {
  const classification = {};
  const counts = { new: 0, existing: 0 };

  if (!Array.isArray(rows) || rows.length === 0) {
    return { classification, counts };
  }

  // La firma incluye la cuenta: la misma operación en dos cuentas distintas no es
  // un duplicado (p. ej. comprar AAPL el mismo día en IBKR y en XTB).
  const normalizedRows = rows.map(row => ({
    ...row,
    assetName: String(row.ticker || row.assetName || '').toUpperCase(),
    portfolioAccountId,
  }));

  // ── 1. Firmas ya presentes en el historial ───────────────────────────────
  const existingSignatureCounts = await countExistingSignatures(normalizedRows, userId);

  // ── 2. Cuántas veces aparece cada firma en el archivo ────────────────────
  const batchSignatureTotals = new Map();

  for (const row of normalizedRows) {
    const signature = createSignature(row);
    batchSignatureTotals.set(signature, (batchSignatureTotals.get(signature) || 0) + 1);
  }

  // ── 3. Clasificación ─────────────────────────────────────────────────────
  const acceptedCounts = new Map();

  for (const row of normalizedRows) {
    const signature = createSignature(row);
    const existingCount = existingSignatureCounts.get(signature) || 0;
    const batchTotal = batchSignatureTotals.get(signature) || 1;
    const accepted = acceptedCounts.get(signature) || 0;

    // Si el historial ya tiene tantas ocurrencias como trae el archivo, esta fila
    // no aporta nada nuevo.
    const allowedNew = Math.max(0, batchTotal - existingCount);
    const isNew = accepted < allowedNew;

    if (isNew) {
      acceptedCounts.set(signature, accepted + 1);
      classification[row.originalRowNumber] = ROW_GROUP.NEW;
      counts.new++;
    } else {
      classification[row.originalRowNumber] = ROW_GROUP.EXISTING;
      counts.existing++;
    }
  }

  return { classification, counts };
}

// ============================================================================
// HELPERS
// ============================================================================

/**
 * Cuenta las firmas ya presentes en el historial del usuario.
 *
 * Consulta una vez por ticker distinto, no por fila: el número de consultas escala
 * con los activos del archivo, no con su tamaño (RN-13).
 *
 * @param {Object[]} rows - Filas normalizadas (con assetName)
 * @param {string} userId
 * @param {string} portfolioAccountId
 * @returns {Promise<Map<string, number>>} firma → nº de ocurrencias existentes
 */
async function countExistingSignatures(rows, userId) {
  const counts = new Map();
  const tickers = [...new Set(rows.map(r => r.assetName).filter(Boolean))];

  for (const ticker of tickers) {
    const existing = await getExistingTransactionsForTicker(ticker, userId);

    for (const tx of existing) {
      // RN-19: el criterio se aplica contra TODAS las transacciones del usuario,
      // incluidas las creadas manualmente. No se filtra por origen.
      //
      // La firma se calcula tal cual la calcula el canal hoy, sin normalizar la
      // cuenta ausente: una transacción heredada sin portfolioAccountId produce
      // una firma distinta y NO se considera duplicado. Esto es deliberado — la
      // historia expone el criterio vigente, no lo cambia.
      const signature = createSignature(tx);

      counts.set(signature, (counts.get(signature) || 0) + 1);
    }
  }

  return counts;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  classifyRows,
  countExistingSignatures,
  ROW_GROUP,
};
