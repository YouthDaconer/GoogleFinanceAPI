/**
 * Las posiciones de un usuario, por la única vía que existe.
 *
 * Un documento de `assets` **no tiene `userId`**. La propiedad se establece por
 * su cuenta: `assets.portfolioAccount` → `portfolioAccounts/{id}.userId`, que es
 * el mismo criterio de las reglas de Firestore
 * (`belongsToUser(resource.data.portfolioAccount)`) y el que ya usaba
 * `accountHandlers.findAccountAssets`.
 *
 * Esto existe porque la HU 5.2 filtró `assets` por `userId` en dos sitios —la
 * compuerta del lanzador y el constructor del payload— y en Firestore una
 * igualdad sobre un campo **ausente** no coincide con nada: la consulta devolvía
 * cero para todo el mundo, así que el Copilot respondía "necesitas al menos una
 * posición" a usuarios con cientos de activos. Dos sitios con la misma regla
 * escrita dos veces es lo que permitió que el mismo error viviera por duplicado;
 * por eso ahora la regla está aquí y sólo aquí.
 *
 * @module services/copilot/copilotPositions
 */

const admin = require('../firebaseAdmin');

/**
 * Los dos nombres con los que un activo puede apuntar a su cuenta.
 *
 * Hay documentos antiguos escritos con `portfolioAccountId`. Se consultan los
 * dos y se deduplica por id, igual que `accountHandlers.findAccountAssets`: no
 * se usa un filtro OR porque abarca campos distintos y exigiría un índice
 * compuesto.
 */
const CAMPOS_DE_CUENTA = ['portfolioAccount', 'portfolioAccountId'];

/**
 * Las cuentas de portafolio del usuario.
 *
 * `portfolioAccounts` sí tiene `userId`: es la frontera de propiedad.
 *
 * @param {string} userId
 * @returns {Promise<FirebaseFirestore.QueryDocumentSnapshot[]>}
 */
async function cuentasDelUsuario(userId) {
  const snapshot = await admin
    .firestore()
    .collection('portfolioAccounts')
    .where('userId', '==', userId)
    .get();

  return snapshot.docs;
}

/**
 * ¿El usuario tiene al menos una posición abierta? (Escenario 7 de la HU 5.2)
 *
 * Corta en cuanto encuentra una: la pregunta es si existe alguna, no cuántas.
 *
 * @param {string} userId
 * @returns {Promise<boolean>}
 */
async function hayPosicionAbierta(userId) {
  const db = admin.firestore();
  const cuentas = await cuentasDelUsuario(userId);

  for (const cuenta of cuentas) {
    for (const campo of CAMPOS_DE_CUENTA) {
      const snapshot = await db
        .collection('assets')
        .where(campo, '==', cuenta.id)
        .where('isActive', '==', true)
        .limit(1)
        .get();

      if (!snapshot.empty) return true;
    }
  }

  return false;
}

/**
 * Los activos abiertos del usuario y sus cuentas, en una sola lectura.
 *
 * Devuelve también las cuentas porque quien pide los activos las necesita
 * siempre, y volver a consultarlas sería pagar dos veces por la misma frontera
 * de propiedad.
 *
 * @param {string} userId
 * @returns {Promise<{assets: object[], accounts: object[]}>}
 */
async function leerPosiciones(userId) {
  const db = admin.firestore();
  const cuentas = await cuentasDelUsuario(userId);

  const consultas = [];
  for (const cuenta of cuentas) {
    for (const campo of CAMPOS_DE_CUENTA) {
      consultas.push(
        db.collection('assets')
          .where(campo, '==', cuenta.id)
          .where('isActive', '==', true)
          .get(),
      );
    }
  }

  const resultados = await Promise.all(consultas);

  // Deduplicado por id: un activo escrito con los dos campos aparecería dos
  // veces, y duplicarlo falsearía los pesos del portafolio.
  const porId = new Map();
  for (const snapshot of resultados) {
    for (const doc of snapshot.docs) {
      porId.set(doc.id, { id: doc.id, ...doc.data() });
    }
  }

  return {
    assets: [...porId.values()],
    accounts: cuentas.map((doc) => ({ id: doc.id, ...doc.data() })),
  };
}

module.exports = {
  CAMPOS_DE_CUENTA,
  cuentasDelUsuario,
  hayPosicionAbierta,
  leerPosiciones,
};
