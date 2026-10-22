/**
 * HU 6.3 — La cuenta de quién quedó procesado y quién no.
 *
 * Cuando el cierre nocturno era un solo proceso, el recuento vivía en memoria y
 * se perdía al terminar: nadie podía responder «¿se procesaron todos?» sin leer
 * los logs. Al repartir el trabajo en una tarea por inversor eso deja de ser un
 * lujo y pasa a ser la columna vertebral:
 *
 * - es la **constancia de cuántos se procesaron** del Escenario 1;
 * - es la **fuente del aviso** del Escenario 4 (cuántos, cuáles y por qué);
 * - es la **guarda de «ya procesado»** que hace que un reintento no duplique
 *   ni altere lo ya hecho (RN-6.3-L, Escenario 6).
 *
 * **Por qué un documento por inversor y no un arreglo.** El plan proponía
 * `failedUsers[]` dentro del documento del día. Con la meta de capacidad de la
 * historia —5.000 inversores (RN-6.3-J)— un arreglo de fallos cabe en el tope
 * de 1 MiB de Firestore sólo mientras los fallos sean pocos, y precisamente el
 * día en que no lo sean el aviso se perdería por desbordar el documento. Así
 * que el documento del día lleva los **contadores** y la subcolección
 * `usuarios` lleva las **identidades**: «cuántos» y «cuáles» se conservan
 * enteros, y además la subcolección es lo que permite preguntar por un solo
 * inversor sin leer el día completo, que es lo que hace la guarda de reintento.
 *
 * Todos los incrementos van dentro de una transacción contra el documento del
 * inversor: dos entregas de la misma tarea no pueden contarla dos veces.
 *
 * @module services/eodProgressService
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T5, T8)
 */

const { FieldValue } = require('firebase-admin/firestore');

const COLECCION_PROGRESO = 'eodProgress';
const SUBCOLECCION_USUARIOS = 'usuarios';

/** Estados del día. */
const ESTADO_DIA = {
  EN_CURSO: 'en-curso',
  COMPLETO: 'completo',
  INCOMPLETO: 'incompleto',
};

/** Estados de un inversor dentro del día. */
const ESTADO_INVERSOR = {
  COMPLETADO: 'completado',
  FALLIDO: 'fallido',
};

/**
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha - `YYYY-MM-DD`
 * @returns {FirebaseFirestore.DocumentReference}
 */
function refDelDia(db, fecha) {
  return db.collection(COLECCION_PROGRESO).doc(fecha);
}

/**
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {string} userId
 * @returns {FirebaseFirestore.DocumentReference}
 */
function refDelInversor(db, fecha, userId) {
  return refDelDia(db, fecha).collection(SUBCOLECCION_USUARIOS).doc(userId);
}

/**
 * Abre el día: deja escrito a cuántos inversores hay que llegar.
 *
 * Lo llama el orquestador **antes** de encolar nada, para que el cierre póstumo
 * pueda comparar contra un total que no depende de volver a contar inversores.
 * Es idempotente: un relanzamiento del mismo día actualiza el total y conserva
 * lo que ya se haya procesado.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {Object} params
 * @param {number} params.totalUsers
 * @param {string[]} [params.userIds] - A quiénes se encoló; lo necesita
 *   `listarSinRastro` para detectar al que nunca llegó a ejecutarse
 * @param {string} [params.runId]
 * @returns {Promise<void>}
 */
async function iniciarProgreso(db, fecha, { totalUsers, userIds = [], runId = null }) {
  await refDelDia(db, fecha).set({
    date: fecha,
    totalUsers,
    userIds,
    estado: ESTADO_DIA.EN_CURSO,
    startedAt: new Date().toISOString(),
    finishedAt: null,
    runId,
    // Sólo se inicializan si el documento es nuevo; un relanzamiento no borra
    // la cuenta de lo que ya se hizo.
    completedUsers: FieldValue.increment(0),
    failedUsers: FieldValue.increment(0),
  }, { merge: true });

  console.log(`[eodProgressService] Día ${fecha} abierto para ${totalUsers} inversores (run ${runId})`);
}

/**
 * ¿Este inversor ya quedó procesado en este día?
 *
 * Es la guarda de RN-6.3-L: la tarea la consulta antes de calcular nada, así
 * que un reintento sobre un inversor que ya terminó no vuelve a escribir sus
 * cifras.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {string} userId
 * @returns {Promise<boolean>}
 */
async function yaProcesado(db, fecha, userId) {
  const doc = await refDelInversor(db, fecha, userId).get();
  return doc.exists && doc.data().estado === ESTADO_INVERSOR.COMPLETADO;
}

/**
 * Marca un inversor como terminado y mueve los contadores del día.
 *
 * La transacción es lo que hace que la cuenta sea exacta: si la misma tarea se
 * entrega dos veces, la segunda ve el inversor ya en `completado` y no vuelve a
 * incrementar.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {string} userId
 * @param {Object} [datos]
 * @param {number} [datos.durationMs]
 * @param {number} [datos.intento]
 * @returns {Promise<{contado: boolean}>} `contado: false` si ya estaba marcado.
 */
async function marcarCompletado(db, fecha, userId, { durationMs = null, intento = 0 } = {}) {
  const diaRef = refDelDia(db, fecha);
  const inversorRef = refDelInversor(db, fecha, userId);

  return db.runTransaction(async (tx) => {
    const actual = await tx.get(inversorRef);
    const estabaFallido = actual.exists && actual.data().estado === ESTADO_INVERSOR.FALLIDO;

    if (actual.exists && actual.data().estado === ESTADO_INVERSOR.COMPLETADO) {
      return { contado: false };
    }

    tx.set(inversorRef, {
      userId,
      estado: ESTADO_INVERSOR.COMPLETADO,
      intentos: intento + 1,
      durationMs,
      error: null,
      updatedAt: new Date().toISOString(),
    });

    const cambios = { completedUsers: FieldValue.increment(1) };
    if (estabaFallido) {
      // Dejó de estar fuera: el aviso del Escenario 4 no debe seguir contándolo.
      cambios.failedUsers = FieldValue.increment(-1);
    }
    tx.set(diaRef, cambios, { merge: true });

    return { contado: true };
  });
}

/**
 * Marca un inversor como fallido tras agotar su último intento.
 *
 * No se llama en cada fallo intermedio: Cloud Tasks reintenta por su cuenta y
 * un fallo que después se recupera no es nadie «fuera». Sólo cuando ya no
 * quedan intentos el inversor entra en la lista que lee el aviso (RN-6.3-N).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {string} userId
 * @param {string} motivo
 * @param {number} [intento]
 * @returns {Promise<{contado: boolean}>}
 */
async function marcarFallido(db, fecha, userId, motivo, intento = 0) {
  const diaRef = refDelDia(db, fecha);
  const inversorRef = refDelInversor(db, fecha, userId);

  return db.runTransaction(async (tx) => {
    const actual = await tx.get(inversorRef);
    const estado = actual.exists ? actual.data().estado : null;

    if (estado === ESTADO_INVERSOR.COMPLETADO) {
      // Llegó tarde el fallo de un intento anterior sobre alguien que ya
      // terminó. No se degrada un resultado bueno.
      return { contado: false };
    }

    tx.set(inversorRef, {
      userId,
      estado: ESTADO_INVERSOR.FALLIDO,
      intentos: intento + 1,
      error: String(motivo || '').slice(0, 500),
      updatedAt: new Date().toISOString(),
    });

    if (estado !== ESTADO_INVERSOR.FALLIDO) {
      tx.set(diaRef, { failedUsers: FieldValue.increment(1) }, { merge: true });
      return { contado: true };
    }
    return { contado: false };
  });
}

/**
 * Lee el estado del día: cuántos se esperaban y cuántos van.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @returns {Promise<{date: string, totalUsers: number, completedUsers: number, failedUsers: number, estado: string, startedAt: string|null, finishedAt: string|null, runId: string|null}|null>}
 */
async function leerProgreso(db, fecha) {
  const doc = await refDelDia(db, fecha).get();
  if (!doc.exists) return null;

  const datos = doc.data();
  return {
    date: datos.date || fecha,
    totalUsers: datos.totalUsers || 0,
    userIds: datos.userIds || [],
    completedUsers: datos.completedUsers || 0,
    failedUsers: datos.failedUsers || 0,
    estado: datos.estado || ESTADO_DIA.EN_CURSO,
    startedAt: datos.startedAt || null,
    finishedAt: datos.finishedAt || null,
    runId: datos.runId || null,
  };
}

/**
 * Quiénes quedaron fuera y por qué — el «cuáles» del Escenario 4.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {number} [limite] - Tope de inversores devueltos
 * @returns {Promise<Array<{userId: string, error: string|null, intentos: number}>>}
 */
async function listarFallidos(db, fecha, limite = 500) {
  const snapshot = await refDelDia(db, fecha)
    .collection(SUBCOLECCION_USUARIOS)
    .where('estado', '==', ESTADO_INVERSOR.FALLIDO)
    .limit(limite)
    .get();

  return snapshot.docs.map((d) => ({
    userId: d.data().userId || d.id,
    error: d.data().error || null,
    intentos: d.data().intentos || 0,
  }));
}

/**
 * Quiénes nunca llegaron a dejar rastro.
 *
 * Un inversor puede quedar fuera sin estar marcado como fallido: si su tarea
 * jamás se ejecutó —nunca se encoló, o la cola la descartó— no hay documento
 * suyo. Comparar los esperados contra los que dejaron rastro es la única forma
 * de que ese caso no pase en silencio, que es exactamente lo que prohíbe
 * RN-6-E.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {string[]} userIdsEsperados
 * @returns {Promise<string[]>}
 */
async function listarSinRastro(db, fecha, userIdsEsperados) {
  const snapshot = await refDelDia(db, fecha).collection(SUBCOLECCION_USUARIOS).get();
  const conRastro = new Set(snapshot.docs.map((d) => d.id));
  return userIdsEsperados.filter((userId) => !conRastro.has(userId));
}

/**
 * Cierra el día con su veredicto.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} fecha
 * @param {string} estado - Uno de `ESTADO_DIA`
 * @returns {Promise<void>}
 */
async function cerrarProgreso(db, fecha, estado) {
  await refDelDia(db, fecha).set({
    estado,
    finishedAt: new Date().toISOString(),
  }, { merge: true });

  console.log(`[eodProgressService] Día ${fecha} cerrado como ${estado}`);
}

module.exports = {
  iniciarProgreso,
  yaProcesado,
  marcarCompletado,
  marcarFallido,
  leerProgreso,
  listarFallidos,
  listarSinRastro,
  cerrarProgreso,
  refDelDia,
  refDelInversor,
  ESTADO_DIA,
  ESTADO_INVERSOR,
  COLECCION_PROGRESO,
};
