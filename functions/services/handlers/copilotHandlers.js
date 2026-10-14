/**
 * HU 5.2 — Lanzar un análisis, y nada más.
 *
 * El lanzamiento es una petición corta: comprueba el cupo y que haya algo que
 * analizar, abre el documento del análisis y devuelve. El trabajo lo recoge el
 * worker, y por eso el usuario puede irse (RN-5.2-N).
 *
 * El Copilot es de Pro y Lifetime. La HU 5.5 lo había abierto al plan gratuito
 * apoyándose en que el motor era gratis; no lo es, y cada análisis cuesta
 * dinero, así que el PO lo devolvió a los planes de pago (2026-10-13).
 *
 * @module services/handlers/copilotHandlers
 * @see platform-docs/stories/5.2-copilot-segundo-plano-progreso/refinamiento.md (T9)
 */

const { HttpsError } = require('firebase-functions/v2/https');
const admin = require('../firebaseAdmin');
const runRepository = require('../copilot/copilotRunRepository');
const quota = require('../copilot/copilotQuota');
const positions = require('../copilot/copilotPositions');
const spendGuard = require('../copilot/copilotSpendGuard');

/** Idiomas que el análisis sabe escribir (`prompt_template.py`). */
const IDIOMAS = ['es', 'en'];

/**
 * ¿El usuario tiene al menos una posición abierta?
 *
 * Escenario 7: sin posiciones no se lanza nada y no se consume crédito.
 *
 * La pregunta parece trivial y no lo es: un `asset` **no tiene `userId`**, así
 * que hay que llegar a él por su cuenta. Filtrar por `userId` devolvía cero para
 * todo el mundo —en Firestore una igualdad sobre un campo ausente no coincide
 * con nada— y el Copilot respondía "necesitas al menos una posición" a usuarios
 * con cientos de activos. La regla vive en `copilotPositions` y sólo ahí.
 *
 * @param {string} userId
 * @returns {Promise<boolean>}
 */
async function tienePosicionesAbiertas(userId) {
  return positions.hayPosicionAbierta(userId);
}

/**
 * Lanza un análisis, o devuelve el que ya está corriendo.
 *
 * @param {{auth: {uid: string}}} context
 * @param {{language?: string, currency?: string}} payload
 * @returns {Promise<{started: boolean, run: object, nextAvailable: string|null,
 *   remaining: number}>}
 * @throws {HttpsError} `permission-denied` sin plan, `failed-precondition` sin
 *   posiciones, `resource-exhausted` sin cupo.
 */
async function startAnalysis(context, payload) {
  const userId = context.auth.uid;
  const { language = 'es', currency = 'USD' } = payload || {};

  if (!IDIOMAS.includes(language)) {
    throw new HttpsError('invalid-argument', `Idioma no soportado: ${language}`);
  }
  if (typeof currency !== 'string' || currency.length !== 3) {
    throw new HttpsError('invalid-argument', 'La moneda debe ser un código de 3 letras');
  }

  const userSnapshot = await admin.firestore().collection('userData').doc(userId).get();
  const userData = userSnapshot.exists ? userSnapshot.data() : null;

  if (!quota.tieneAcceso(userData)) {
    throw new HttpsError(
      'permission-denied',
      'El Copilot requiere un plan Pro o Lifetime activo',
    );
  }

  // Si ya hay uno en curso, el usuario no lanza otro: va a su progreso
  // (RN-5-B, Escenario 6). Se comprueba antes que el cupo porque el análisis en
  // curso todavía no lo consumió, y responder "sin cupo" sería mentir.
  const actual = await runRepository.getRun(userId);
  if (runRepository.isActive(actual) && !runRepository.isStalled(actual)) {
    return {
      started: false, run: actual, nextAvailable: null, remaining: quota.restantesHoy(userData),
    };
  }

  const nextAvailable = quota.nextAvailableAt(userData);
  if (nextAvailable) {
    const error = new HttpsError(
      'resource-exhausted',
      'Ya usaste tu análisis de este periodo',
      { nextAvailable },
    );
    throw error;
  }

  if (!(await tienePosicionesAbiertas(userId))) {
    throw new HttpsError(
      'failed-precondition',
      'Necesitas al menos una posición para generar un análisis',
    );
  }

  // El último guardia, y el único que habla de dinero: el saldo del motor es
  // prepago y un pico de uso puede vaciarlo. Va al final para no gastar
  // presupuesto en un lanzamiento que iba a rechazarse igual.
  const presupuesto = await spendGuard.reservar();
  if (!presupuesto.concedido) {
    throw new HttpsError(
      'resource-exhausted',
      'El Copilot alcanzó su límite de análisis de hoy. Vuelve mañana.',
      { platformLimit: true },
    );
  }

  const { started, run } = await runRepository.startRun(userId, { language, currency });
  console.log(`[copilotHandlers][startAnalysis] userId: ${userId}, started: ${started}`);

  // Lo que le queda **después** de este: el cupo se consume al completar, pero
  // el usuario ya no puede gastar éste dos veces.
  return {
    started, run, nextAvailable: null, remaining: Math.max(0, quota.restantesHoy(userData) - 1),
  };
}

/**
 * El análisis en curso del usuario y su cupo, para quien no tenga la
 * suscripción en vivo (por ejemplo, al entrar).
 *
 * Devuelve también **cuántos análisis le quedan hoy**. Lo dice quien aplica la
 * regla: deducirlo en el cliente sería contarla por segunda vez y en otro sitio
 * (RN-5.5-O).
 *
 * @param {{auth: {uid: string}}} context
 * @returns {Promise<{run: object|null, nextAvailable: string|null, hasAccess: boolean,
 *   remaining: number}>}
 */
async function getAnalysisState(context) {
  const userId = context.auth.uid;

  const [userSnapshot, run] = await Promise.all([
    admin.firestore().collection('userData').doc(userId).get(),
    runRepository.getRun(userId),
  ]);

  const userData = userSnapshot.exists ? userSnapshot.data() : null;

  return {
    run,
    nextAvailable: quota.nextAvailableAt(userData),
    hasAccess: quota.tieneAcceso(userData),
    remaining: quota.restantesHoy(userData),
  };
}

module.exports = {
  IDIOMAS,
  tienePosicionesAbiertas,
  startAnalysis,
  getAnalysisState,
};
