/**
 * HU 2.4 — Recálculo del histórico de posiciones cerradas.
 *
 * Las ventas anteriores a esta historia se registraron con el tipo de cambio de
 * la **compra**, así que su resultado en moneda local no incluye el efecto
 * divisa. Este servicio las recorre, resuelve las dos tasas —la del lote y la
 * del día de la venta— y escribe la misma descomposición que escribiría una
 * venta nueva.
 *
 * Tres propiedades que lo hacen seguro de ejecutar (D7):
 *
 * - **Idempotente**: una venta ya resuelta se salta por la presencia de
 *   `realizationRateSource`. Las que quedaron `unavailable` sí se reintentan,
 *   porque la ausencia pudo ser un fallo transitorio del proveedor de tasas.
 * - **Acotado**: hay tope de escrituras y de consultas nuevas de tasa por
 *   invocación. Si se alcanza, devuelve `hasMore` y el cliente continúa.
 * - **No toca el rendimiento**: corrige la descomposición de la venta, no las
 *   series de `portfolioPerformance` ni los snapshots. La línea agregada de
 *   resultado por diferencia en cambio pertenece a 2.5.
 *
 * El acceso a las ventas reutiliza la consulta que el panel de posiciones
 * cerradas ya hace (`portfolioAccountId in [...]`, filtrando el tipo en
 * memoria), para no exigir un índice compuesto nuevo — la misma decisión que
 * tomó 2.3 con la estimación de la base de costo.
 *
 * @module services/realizedFxBackfill
 * @see platform-docs/stories/2.4-venta-dividendo-tasa-del-dia/refinamiento.md (D7)
 */

const admin = require('./firebaseAdmin');
const db = admin.firestore();

const { getUserReferenceCurrency } = require('./helpers/balanceCostBasis');
const {
  resolveLotAcquisitionRate,
  resolveRealizationRate,
  decomposeRealizedResult,
  buildRealizedFxFields,
  REALIZATION_RATE_SOURCES,
} = require('./helpers/realizedFxDecomposition');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/** Máximo de documentos por lote de escritura (Firestore admite 500) */
const WRITE_BATCH_SIZE = 400;

/** Máximo de ventas corregidas por invocación */
const MAX_UPDATES_PER_RUN = 400;

/**
 * Máximo de resoluciones de tasa NUEVAS por invocación.
 *
 * Cada una puede salir a Yahoo si la fecha no está en
 * `historicalExchangeRates`. Es el factor que limita el tiempo de la función, no
 * el número de documentos: un usuario con 300 ventas en 12 fechas distintas
 * termina en una sola pasada.
 */
const MAX_RATE_LOOKUPS_PER_RUN = 120;

/** Tamaño de chunk del operador `in` de Firestore */
const IN_CHUNK_SIZE = 30;

/** Tamaño de chunk para la lectura masiva de activos */
const ASSET_CHUNK_SIZE = 200;

// ============================================================================
// HELPERS INTERNOS
// ============================================================================

/**
 * Parte un array en trozos del tamaño indicado.
 *
 * @param {Array} items - Elementos a repartir
 * @param {number} size - Tamaño de cada trozo
 * @returns {Array<Array>}
 */
function chunk(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

/**
 * Decide si una venta necesita recálculo.
 *
 * @param {Object} data - Documento de la transacción
 * @returns {boolean}
 */
function needsBackfill(data) {
  if (data.type !== 'sell') return false;
  if (!data.realizationRateSource) return true;
  // Una ausencia pudo ser un fallo transitorio del proveedor: se reintenta.
  return data.realizationRateSource === REALIZATION_RATE_SOURCES.UNAVAILABLE;
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Recalcula la descomposición del resultado realizado de las ventas de un
 * usuario que aún no la tienen.
 *
 * @param {string} userId - UID del usuario
 * @param {Object} [options]
 * @param {number} [options.maxUpdates] - Tope de ventas corregidas en esta pasada
 * @param {number} [options.maxRateLookups] - Tope de consultas de tasa nuevas
 * @returns {Promise<{updatedCount: number, unavailableCount: number,
 *   scannedCount: number, hasMore: boolean, referenceCurrency: string}>}
 */
async function backfillRealizedFxForUser(userId, options = {}) {
  const maxUpdates = options.maxUpdates || MAX_UPDATES_PER_RUN;
  const maxRateLookups = options.maxRateLookups || MAX_RATE_LOOKUPS_PER_RUN;

  const referenceCurrency = await getUserReferenceCurrency(userId);

  // 1. Cuentas del usuario — misma puerta de entrada que usa el panel.
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .get();

  const accountIds = accountsSnapshot.docs.map((doc) => doc.id);

  if (accountIds.length === 0) {
    return {
      updatedCount: 0,
      unavailableCount: 0,
      scannedCount: 0,
      hasMore: false,
      referenceCurrency,
    };
  }

  // 2. Ventas pendientes de recálculo.
  const pending = [];

  for (const accountChunk of chunk(accountIds, IN_CHUNK_SIZE)) {
    const snapshot = await db.collection('transactions')
      .where('portfolioAccountId', 'in', accountChunk)
      .get();

    snapshot.docs.forEach((doc) => {
      const data = doc.data();
      if (needsBackfill(data)) {
        pending.push({ ref: doc.ref, data });
      }
    });
  }

  if (pending.length === 0) {
    return {
      updatedCount: 0,
      unavailableCount: 0,
      scannedCount: 0,
      hasMore: false,
      referenceCurrency,
    };
  }

  // 3. Activos de esas ventas, en lecturas masivas en vez de una por documento.
  const assetIds = [...new Set(pending.map((item) => item.data.assetId).filter(Boolean))];
  const assetsById = new Map();

  for (const idChunk of chunk(assetIds, ASSET_CHUNK_SIZE)) {
    const refs = idChunk.map((id) => db.collection('assets').doc(id));
    const docs = await db.getAll(...refs);
    docs.forEach((doc) => {
      if (doc.exists) assetsById.set(doc.id, { id: doc.id, ...doc.data() });
    });
  }

  // 4. Recálculo, con las tasas memoizadas por (divisa, fecha).
  const rateCache = new Map();
  let rateLookups = 0;

  const cachedRealizationRate = async (currency, date) => {
    const key = `${currency}_${date}`;
    if (rateCache.has(key)) return rateCache.get(key);
    if (currency !== referenceCurrency && rateLookups >= maxRateLookups) return null;

    if (currency !== referenceCurrency) rateLookups += 1;
    const resolved = await resolveRealizationRate(currency, referenceCurrency, date);
    rateCache.set(key, resolved);
    return resolved;
  };

  let updatedCount = 0;
  let unavailableCount = 0;
  let scannedCount = 0;
  let hasMore = false;
  let batch = db.batch();
  let batchSize = 0;

  for (const item of pending) {
    if (updatedCount >= maxUpdates) {
      hasMore = true;
      break;
    }

    const { ref, data } = item;
    const currency = data.currency || 'USD';
    const sellDate = typeof data.date === 'string' ? data.date.substring(0, 10) : null;

    const realization = sellDate ? await cachedRealizationRate(currency, sellDate) : null;

    if (realization === null) {
      // Se agotó el presupuesto de consultas: quedan para la siguiente pasada.
      hasMore = true;
      break;
    }

    scannedCount += 1;

    const asset = assetsById.get(data.assetId);
    const { acquisitionRate, acquisitionRateSource } = asset
      ? await resolveLotAcquisitionRate(asset, referenceCurrency)
      : { acquisitionRate: null, acquisitionRateSource: 'unavailable' };

    // El precio de compra se despeja del propio documento, igual que hace
    // `deriveLotCostBasis` en el visor: `price - valuePnL / amount`. Cero
    // lecturas extra y válido también para los lotes de una venta FIFO.
    const units = Number(data.amount) || 0;
    const sellPrice = Number(data.price) || 0;
    const valuePnL = Number(data.valuePnL) || 0;
    const buyPrice = units > 0 ? sellPrice - valuePnL / units : sellPrice;

    const grossProceeds = units * sellPrice;
    const commission = Number(data.commission) || 0;

    const decomposition = decomposeRealizedResult({
      grossProceeds,
      invested: units * buyPrice,
      acquisitionRate,
      realizationRate: realization.realizationRate,
    });

    if (decomposition.availability === 'available') {
      updatedCount += 1;
    } else {
      unavailableCount += 1;
    }

    batch.update(ref, buildRealizedFxFields({
      referenceCurrency,
      acquisitionRate,
      acquisitionRateSource,
      realizationRate: realization.realizationRate,
      realizationRateSource: realization.realizationRateSource,
      decomposition,
      acquisitionCost: realization.realizationRate === null
        ? null
        : (grossProceeds - commission) * realization.realizationRate,
    }));

    batchSize += 1;

    if (batchSize >= WRITE_BATCH_SIZE) {
      await batch.commit();
      batch = db.batch();
      batchSize = 0;
    }
  }

  if (batchSize > 0) {
    await batch.commit();
  }

  return { updatedCount, unavailableCount, scannedCount, hasMore, referenceCurrency };
}

module.exports = {
  backfillRealizedFxForUser,
  WRITE_BATCH_SIZE,
  MAX_UPDATES_PER_RUN,
  MAX_RATE_LOOKUPS_PER_RUN,
  // Exportado para test
  _needsBackfill: needsBackfill,
};
