/**
 * HU 6.9.1 — La marca de que las compras y ventas de un inversor cambiaron.
 *
 * Las posiciones cerradas se calculan en el navegador desde **todas** las
 * transacciones del inversor. Para no volver a leerlas en cada apertura, el
 * navegador guarda una copia y la reutiliza mientras esta marca no cambie
 * (RN-6.9-J). La marca es `transactionVersions/{userId}.version` y sube con
 * cada compra o venta que se crea, se edita o se borra, **también con fecha
 * pasada** (RN-6.9.1-J).
 *
 * **Por qué un disparador y no un aviso desde cada escritor.** Escriben en
 * `transactions` más de quince caminos: las callables, la importación de JS y
 * la de Python, la fase de coherencia que reescribe `valuePnL`/`closedPnL` de
 * ventas ya existentes, el backfill de diferencia en cambio y los scripts
 * manuales. Un recálculo de la historia solo cambia las posiciones cerradas
 * cuando reescribe transacciones (RN-6.9.1-K), y entonces pasa por aquí como
 * cualquier otra escritura. Lo que "se acuerda" cada escritor se olvida en el
 * siguiente: escuchar la colección los atrapa a todos sin tocar ninguno.
 *
 * `onDocumentWritten` ya está en la lista de §4 regla 7 de los estándares
 * (HU 5.2): no se amplía.
 *
 * @module services/closedPositions/transactionVersionTrigger
 * @see platform-docs/stories/6.9.1-posiciones-cerradas-sin-recalculo/refinamiento.md (T1, T2)
 */

const { onDocumentWritten } = require('firebase-functions/v2/firestore');
const admin = require('../firebaseAdmin');

const TRANSACTIONS_COLLECTION = 'transactions';
const TRANSACTION_VERSIONS_COLLECTION = 'transactionVersions';
const PORTFOLIO_ACCOUNTS_COLLECTION = 'portfolioAccounts';

/**
 * Los únicos tipos que lee `buildRawClosedPositions` en el navegador. Efectivo,
 * dividendos y ajustes no cambian una posición cerrada: se ignoran, y con eso
 * la mayoría de las escrituras programadas no cuestan nada aquí.
 */
const CLOSED_POSITION_TYPES = new Set(['buy', 'sell']);

/**
 * `retry: true`: una importación escribe cientos de transacciones en segundos
 * y todos los incrementos caen sobre el mismo documento. Si alguno choca por
 * contención, se reintenta; un incremento de más es inofensivo (la marca solo
 * tiene que cambiar), uno de menos dejaría una copia vieja en uso.
 */
const FUNCTION_CONFIG = {
  document: `${TRANSACTIONS_COLLECTION}/{txId}`,
  memory: '256MiB',
  timeoutSeconds: 60,
  maxInstances: 10,
  minInstances: 0,
  retry: true,
};

/**
 * ¿Esta escritura cambia las posiciones cerradas de alguien, y de quién?
 *
 * Basta con que la compra o venta lo sea antes **o** después: convertir una
 * venta en otra cosa, o borrarla, también la saca de la lista.
 *
 * @param {object|null} before - Documento antes de la escritura (null si se creó)
 * @param {object|null} after - Documento después de la escritura (null si se borró)
 * @returns {{ relevant: boolean, owners: Array<{ userId: string|null, portfolioAccountId: string|null }> }}
 *   `owners` trae una entrada por cada lado de la escritura que existe; si el
 *   documento no guarda `userId`, hay que resolverlo por la cuenta.
 */
function transactionVersionDecision(before, after) {
  const sides = [before, after].filter(Boolean);
  const relevant = sides.some((side) => CLOSED_POSITION_TYPES.has(side.type));
  if (!relevant) return { relevant: false, owners: [] };

  return {
    relevant: true,
    owners: sides.map((side) => ({
      userId: side.userId || null,
      portfolioAccountId: side.portfolioAccountId || null,
    })),
  };
}

/**
 * Resuelve el inversor de un lado de la escritura.
 *
 * Todos los caminos de creación vigentes escriben `userId`, pero en documentos
 * antiguos no está garantizado; `portfolioAccountId` sí, y la cuenta tiene el
 * `userId` (1 lectura, solo en ese caso).
 *
 * @param {{ userId: string|null, portfolioAccountId: string|null }} owner
 * @returns {Promise<string|null>}
 */
async function resolveOwnerUserId(owner) {
  if (owner.userId) return owner.userId;
  if (!owner.portfolioAccountId) return null;

  const accountSnap = await admin.firestore()
    .collection(PORTFOLIO_ACCOUNTS_COLLECTION)
    .doc(owner.portfolioAccountId)
    .get();
  if (!accountSnap.exists) return null;
  return accountSnap.data().userId || null;
}

/**
 * Sube la marca del inversor. `increment` es atómico: dos escrituras a la vez
 * no se pisan.
 *
 * @param {string} userId
 * @returns {Promise<void>}
 */
async function bumpTransactionVersion(userId) {
  const { FieldValue } = admin.firestore;
  await admin.firestore()
    .collection(TRANSACTION_VERSIONS_COLLECTION)
    .doc(userId)
    .set({
      version: FieldValue.increment(1),
      changedAt: FieldValue.serverTimestamp(),
    }, { merge: true });
}

/**
 * Procesa una escritura de `transactions`.
 *
 * Separado del disparador para poder probarlo sin el runtime de Functions.
 *
 * @param {object|null} before
 * @param {object|null} after
 * @param {string} txId - Solo para el log
 * @returns {Promise<string[]>} Los inversores cuya marca subió
 */
async function handleTransactionWrite(before, after, txId) {
  const decision = transactionVersionDecision(before, after);
  if (!decision.relevant) return [];

  const userIds = new Set();
  for (const owner of decision.owners) {
    const userId = await resolveOwnerUserId(owner);
    if (userId) userIds.add(userId);
  }

  if (userIds.size === 0) {
    // Reintentar no lo arreglaría: sin dueño no hay copia que invalidar.
    console.warn(`[transactionVersionOnWrite] ${txId}: sin userId ni cuenta resoluble; no se sube ninguna marca`);
    return [];
  }

  for (const userId of userIds) {
    await bumpTransactionVersion(userId);
  }
  return [...userIds];
}

const transactionVersionOnWrite = onDocumentWritten(FUNCTION_CONFIG, async (event) => {
  const before = event.data && event.data.before.exists ? event.data.before.data() : null;
  const after = event.data && event.data.after.exists ? event.data.after.data() : null;
  await handleTransactionWrite(before, after, event.params.txId);
});

module.exports = {
  transactionVersionOnWrite,
  transactionVersionDecision,
  handleTransactionWrite,
  FUNCTION_CONFIG,
  TRANSACTION_VERSIONS_COLLECTION,
};
