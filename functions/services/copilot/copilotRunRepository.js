/**
 * HU 5.2 — El análisis en curso, como documento propio.
 *
 * `copilotRuns/{userId}`: el id del documento **es** el usuario. Con eso
 * RN-5-B ("un análisis en curso por usuario, desde ningún dispositivo ni
 * pestaña") deja de ser una comprobación que alguien puede olvidar y pasa a ser
 * una propiedad del almacenamiento: no existe un segundo sitio donde escribir
 * un segundo análisis en curso.
 *
 * Toda escritura del ciclo de vida pasa por aquí, porque aquí vive la guarda de
 * `runId` que descarta un resultado tardío (RN-5.2-Q, QA-21). Repartirla entre
 * el orquestador y el vigilante sería perderla.
 *
 * @module services/copilot/copilotRunRepository
 * @see platform-docs/stories/5.2-copilot-segundo-plano-progreso/refinamiento.md (T2)
 */

const crypto = require('crypto');
const admin = require('../firebaseAdmin');
const {
  STEP_KIND,
  STEP_STATUS,
  RUN_STATUS,
  ACTIVE_RUN_STATUSES,
  STALL_TIMEOUT_MS,
  COPILOT_RUNS_COLLECTION,
} = require('./copilotConstants');

/**
 * ¿Este análisis todavía ocupa el turno del usuario?
 *
 * @param {object|null} run
 * @returns {boolean}
 */
function isActive(run) {
  return Boolean(run) && ACTIVE_RUN_STATUSES.includes(run.status);
}

/**
 * RN-5.2-Q — colgado es no cambiar de etapa en 15 minutos.
 *
 * Pura a propósito: el frontend deriva lo mismo del documento suscrito para
 * poder dibujar el fallo justo al cumplirse el plazo (QA-20, filas de 14:59 y
 * 15:00), sin esperar al vigilante.
 *
 * @param {object|null} run - El documento de `copilotRuns`.
 * @param {Date|number} [now] - Momento contra el que se mide.
 * @returns {boolean}
 */
function isStalled(run, now = Date.now()) {
  if (!isActive(run)) return false;
  const stageAt = Date.parse(run.stageAt || run.startedAt);
  if (Number.isNaN(stageAt)) return false;
  const nowMs = now instanceof Date ? now.getTime() : now;
  return nowMs - stageAt >= STALL_TIMEOUT_MS;
}

/**
 * Referencia al documento del análisis en curso de un usuario.
 *
 * @param {string} userId
 * @returns {FirebaseFirestore.DocumentReference}
 */
function runRef(userId) {
  return admin.firestore().collection(COPILOT_RUNS_COLLECTION).doc(userId);
}

/**
 * El análisis en curso del usuario, o `null` si no hay ninguno.
 *
 * @param {string} userId
 * @returns {Promise<object|null>}
 */
async function getRun(userId) {
  const snapshot = await runRef(userId).get();
  return snapshot.exists ? snapshot.data() : null;
}

/**
 * Abre un análisis, o devuelve el que ya estaba corriendo.
 *
 * Toda la regla "uno a la vez" cabe en esta transacción: el doble clic, las dos
 * pestañas y los dos dispositivos compiten por el mismo documento, y sólo uno
 * lo escribe (QA-14). Un análisis colgado **no** bloquea: se considera libre y
 * el nuevo lo sustituye, que es lo que exige el Escenario 9 ("puedo lanzar uno
 * nuevo de inmediato").
 *
 * @param {string} userId
 * @param {{ language: string, currency: string }} opciones
 * @returns {Promise<{ started: boolean, run: object }>} `started: false` cuando
 *   ya había uno en curso; `run` es siempre el vigente, para llevar al usuario
 *   a su progreso.
 */
async function startRun(userId, { language, currency }) {
  const db = admin.firestore();
  const ref = runRef(userId);

  return db.runTransaction(async (tx) => {
    const snapshot = await tx.get(ref);
    const current = snapshot.exists ? snapshot.data() : null;

    if (isActive(current) && !isStalled(current)) {
      return { started: false, run: current };
    }

    const now = new Date().toISOString();
    const run = {
      runId: crypto.randomUUID(),
      userId,
      status: RUN_STATUS.QUEUED,
      // HU 5.3: el plan arranca con la depuración, que es lo único que se sabe
      // antes de mirar el portafolio. El resto de pasos los añade el
      // orquestador cuando ya sabe qué activos va a investigar.
      steps: [{ kind: STEP_KIND.CLEANSING, status: STEP_STATUS.PENDING }],
      stepIndex: 0,
      startedAt: now,
      stageAt: now,
      finishedAt: null,
      language,
      currency,
      analysisId: null,
      errorCode: null,
    };

    tx.set(ref, run);
    return { started: true, run };
  });
}

/**
 * Escribe el plan de pasos completo, una vez que se sabe qué se va a investigar.
 *
 * El primer paso (la depuración) ya está en marcha cuando esto se llama: se
 * conserva tal cual y los demás se añaden detrás, para que el usuario no vea
 * desaparecer la etapa que está mirando.
 *
 * @param {string} userId
 * @param {string} runId
 * @param {Array<{kind: string, ticker?: string, reason?: string, variation?: number}>} pasos
 * @returns {Promise<boolean>} `false` si el análisis ya no es el vigente.
 */
async function setSteps(userId, runId, pasos) {
  const db = admin.firestore();
  const ref = runRef(userId);

  return db.runTransaction(async (tx) => {
    const snapshot = await tx.get(ref);
    const current = snapshot.exists ? snapshot.data() : null;
    if (!current || current.runId !== runId || !isActive(current)) return false;

    const yaHechos = (current.steps || []).slice(0, current.stepIndex + 1);
    const nuevos = pasos.map((paso) => ({ ...paso, status: STEP_STATUS.PENDING }));

    tx.update(ref, { steps: [...yaHechos, ...nuevos] });
    return true;
  });
}

/**
 * Entra en un paso: lo marca en curso y estampa el latido de RN-5.2-Q.
 *
 * Con una etapa por activo, el latido es mucho más fino que con tres etapas
 * fijas: una investigación larga avanza paso a paso y nunca parece colgada.
 *
 * @param {string} userId
 * @param {string} runId
 * @param {number} indice - Posición del paso en el plan.
 * @returns {Promise<boolean>} `false` si el análisis ya no es el vigente: el
 *   llamador debe abandonar sin escribir nada más.
 */
async function startStep(userId, runId, indice) {
  const db = admin.firestore();
  const ref = runRef(userId);

  return db.runTransaction(async (tx) => {
    const snapshot = await tx.get(ref);
    const current = snapshot.exists ? snapshot.data() : null;
    if (!current || current.runId !== runId || !isActive(current)) return false;

    const steps = [...(current.steps || [])];
    if (!steps[indice]) return false;
    steps[indice] = { ...steps[indice], status: STEP_STATUS.RUNNING };

    tx.update(ref, {
      status: RUN_STATUS.RUNNING,
      steps,
      stepIndex: indice,
      stageAt: new Date().toISOString(),
    });
    return true;
  });
}

/**
 * Cierra un paso con lo que obtuvo, o declarando que no se pudo.
 *
 * El resumen es lo que el usuario lee al lado de la etapa completada
 * ("resultados Q3, consenso, noticias"). Un paso fallido se cierra como tal y
 * no con un resumen vacío: una ausencia y un fallo no son lo mismo (RN-5.3-R).
 *
 * @param {string} userId
 * @param {string} runId
 * @param {number} indice
 * @param {{ ok: boolean, summary?: string }} desenlace
 * @returns {Promise<boolean>}
 */
async function finishStep(userId, runId, indice, { ok, summary = null }) {
  const db = admin.firestore();
  const ref = runRef(userId);

  return db.runTransaction(async (tx) => {
    const snapshot = await tx.get(ref);
    const current = snapshot.exists ? snapshot.data() : null;
    if (!current || current.runId !== runId || !isActive(current)) return false;

    const steps = [...(current.steps || [])];
    if (!steps[indice]) return false;
    steps[indice] = {
      ...steps[indice],
      status: ok ? STEP_STATUS.DONE : STEP_STATUS.FAILED,
      summary,
    };

    tx.update(ref, { steps, stageAt: new Date().toISOString() });
    return true;
  });
}

/**
 * Cierra el análisis con éxito.
 *
 * @param {string} userId
 * @param {string} runId
 * @param {string} analysisId - El documento de `copilotAnalyses` ya escrito.
 * @returns {Promise<boolean>} `false` si otro camino ya cerró este análisis
 *   —el vigilante lo dio por colgado, o el usuario lanzó otro—: el resultado
 *   llega tarde y se descarta (QA-21).
 */
async function completeRun(userId, runId, analysisId) {
  return closeRun(userId, runId, {
    status: RUN_STATUS.COMPLETED,
    analysisId,
    errorCode: null,
  });
}

/**
 * Cierra el análisis como fallido.
 *
 * @param {string} userId
 * @param {string} runId
 * @param {string} errorCode - Un valor de `FAILURE_CODE`.
 * @returns {Promise<boolean>} `false` si ya estaba cerrado: no se avisa dos
 *   veces del mismo fallo (RN-5.2-P).
 */
async function failRun(userId, runId, errorCode) {
  return closeRun(userId, runId, {
    status: RUN_STATUS.FAILED,
    analysisId: null,
    errorCode,
  });
}

/**
 * El cierre, con la guarda compartida: sólo cierra quien sigue siendo el
 * análisis vigente y sólo si nadie lo cerró antes.
 *
 * @param {string} userId
 * @param {string} runId
 * @param {{ status: string, analysisId: string|null, errorCode: string|null }} desenlace
 * @returns {Promise<boolean>}
 */
async function closeRun(userId, runId, desenlace) {
  const db = admin.firestore();
  const ref = runRef(userId);

  return db.runTransaction(async (tx) => {
    const snapshot = await tx.get(ref);
    const current = snapshot.exists ? snapshot.data() : null;

    if (!current || current.runId !== runId || !isActive(current)) return false;

    tx.update(ref, {
      ...desenlace,
      finishedAt: new Date().toISOString(),
    });
    return true;
  });
}

/**
 * Los análisis que llevan demasiado tiempo sin avanzar de etapa.
 *
 * El vigilante los recoge cada minuto. La consulta se apoya en el índice
 * compuesto `copilotRuns (status, stageAt)`; sin él Firestore responde con un
 * error de precondición fallida y el job cae en silencio.
 *
 * @param {Date|number} [now]
 * @param {number} [limite]
 * @returns {Promise<object[]>}
 */
async function findStalledRuns(now = Date.now(), limite = 50) {
  const nowMs = now instanceof Date ? now.getTime() : now;
  const corte = new Date(nowMs - STALL_TIMEOUT_MS).toISOString();

  const snapshot = await admin
    .firestore()
    .collection(COPILOT_RUNS_COLLECTION)
    .where('status', 'in', ACTIVE_RUN_STATUSES)
    .where('stageAt', '<=', corte)
    .limit(limite)
    .get();

  return snapshot.docs.map((doc) => doc.data());
}

module.exports = {
  isActive,
  isStalled,
  getRun,
  startRun,
  setSteps,
  startStep,
  finishStep,
  completeRun,
  failRun,
  findStalledRuns,
  runRef,
};
