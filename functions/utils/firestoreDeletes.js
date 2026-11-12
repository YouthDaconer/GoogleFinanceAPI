/**
 * Borrado de documentos grandes sin reventar el límite de una escritura.
 *
 * Borrar 261 snapshots de un inversor en un solo batch (el tope de 400 que se
 * usaba) falla con `3 INVALID_ARGUMENT: Transaction too big`: cada snapshot
 * lleva mapas con cientos de fechas y cada entrada es un índice que el borrado
 * tiene que retirar en la misma operación. Así fallaba la reconciliación de
 * DDeR8P desde el 2026-11-05: recalculaba y luego no podía propagar.
 *
 * @module utils/firestoreDeletes
 */

/** Documentos por batch. Diez snapshots grandes caben de sobra. */
const DELETE_BATCH_SIZE = 10;

function isTooBig(error) {
  return /transaction too big|too big|exceeds/i.test(String(error?.message || ''));
}

/**
 * Borra `refs` en lotes de `size`; si un lote es demasiado grande, sus
 * documentos se borran uno a uno.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {FirebaseFirestore.DocumentReference[]} refs
 * @param {number} [size]
 * @returns {Promise<number>} Documentos borrados
 */
async function deleteInSmallBatches(db, refs, size = DELETE_BATCH_SIZE) {
  let deleted = 0;
  for (let i = 0; i < refs.length; i += size) {
    const chunk = refs.slice(i, i + size);
    try {
      const batch = db.batch();
      chunk.forEach((ref) => batch.delete(ref));
      await batch.commit();
    } catch (error) {
      if (chunk.length === 1 || !isTooBig(error)) throw error;
      for (const ref of chunk) {
        const single = db.batch();
        single.delete(ref);
        await single.commit();
      }
    }
    deleted += chunk.length;
  }
  return deleted;
}

module.exports = { DELETE_BATCH_SIZE, deleteInSmallBatches };
