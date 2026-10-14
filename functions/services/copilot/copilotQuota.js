/**
 * Quién puede lanzar un análisis, y cuántos al día.
 *
 * **El Copilot vuelve a ser exclusivo de Pro y Lifetime** (decisión del PO,
 * 2026-10-13). La HU 5.5 lo había abierto al plan gratuito con 1 análisis cada
 * 7 días, apoyándose en que el motor era gratis. No lo es: la capa gratuita de
 * Gemini son 20 peticiones al día para todo el proyecto y un análisis cuesta
 * ~14, así que sostenerlo exige facturación y cada análisis pasa a costar
 * dinero (~$0.11 medido).
 *
El cupo es **un análisis por semana**, en ventana móvil desde el último
 * completado. Pasó por un tope diario de cinco durante unas horas, hasta que la
 * cuenta salió: a ese ritmo un usuario costaría seis veces su suscripción. Lo
 * que acota el cupo es el gasto, y el gasto manda sobre la generosidad.
 *
 * Sigue siendo **el único sitio donde se cuenta** (RN-5.5-O): la fecha que ve
 * el usuario es la que aplica el lanzador.
 *
 * @module services/copilot/copilotQuota
 */

const { DateTime } = require('luxon');
const admin = require('../firebaseAdmin');
const { DIAS_ENTRE_ANALISIS, ZONA_DEL_CUPO } = require('./copilotConstants');

/** Planes con acceso al Copilot. */
const PLANES_CON_ACCESO = ['pro', 'lifetime'];

/**
 * Estados de suscripción que se consideran vigentes.
 *
 * El pago pendiente cuenta como activo, igual que en el resto de funciones de
 * pago de la app. Es el comportamiento vigente desde antes de la épica.
 */
const ESTADOS_VIGENTES = ['active', 'past_due'];

/**
 * ¿Esta suscripción ya venció?
 *
 * Mismo criterio que el frontend (`hooks/useSubscription.ts`, guarda
 * PAY-GUARD-001): el cron que degrada a gratuito puede no haber corrido
 * todavía, y hasta que corra el documento sigue diciendo `active`. Si el
 * backend no mirara la fecha, le daría análisis a quien la app ya trata como
 * gratuito — y los dos lados dirían cosas distintas del mismo usuario.
 *
 * Lifetime no vence; el plan gratuito no tiene de qué vencer.
 *
 * @param {object} subscription
 * @param {Date} now
 * @returns {boolean}
 */
function suscripcionVencida(subscription, now) {
  if (!subscription.currentPeriodEnd) return false;
  if (subscription.planId === 'free' || subscription.planId === 'lifetime') return false;

  const fin = DateTime.fromISO(subscription.currentPeriodEnd, { zone: 'utc' });
  if (!fin.isValid) return false;

  return fin < DateTime.fromJSDate(now, { zone: 'utc' });
}

/**
 * ¿La suscripción da acceso al Copilot?
 *
 * @param {object|null} userData - El documento `userData/{uid}`.
 * @param {Date} [now]
 * @returns {boolean}
 */
function tieneAcceso(userData, now = new Date()) {
  const subscription = (userData && userData.subscription) || {};

  return (
    PLANES_CON_ACCESO.includes(subscription.planId) &&
    ESTADOS_VIGENTES.includes(subscription.status) &&
    !suscripcionVencida(subscription, now)
  );
}

/**
 * El día al que se imputa un instante, en la zona del mercado.
 *
 * @param {Date} now
 * @returns {string} `YYYY-MM-DD`.
 */
function diaDelCupo(now) {
  return DateTime.fromJSDate(now, { zone: ZONA_DEL_CUPO }).toISODate();
}

/**
 * Cuándo estará disponible el siguiente análisis, o `null` si ya lo está.
 *
 * Recibe el usuario entero y no su contador a propósito: **no se puede calcular
 * esta fecha sin saber el plan**, y una firma que lo permitiera es la clase de
 * error que ya costó una producción rota.
 *
 * Los siete días se cuentan desde el instante exacto del último análisis
 * **completado** —`lastCopilotAnalysis` sólo se escribe al cerrar con éxito—,
 * no desde el lunes ni desde la medianoche: un análisis que falla no cuesta
 * cupo (RN-5-C).
 *
 * @param {object|null} userData - El documento `userData/{uid}`.
 * @param {Date} [now]
 * @returns {string|null} ISO 8601, o `null` si hay cupo.
 */
function nextAvailableAt(userData, now = new Date()) {
  if (!tieneAcceso(userData, now)) return null;

  const ultimo = (userData || {}).lastCopilotAnalysis;
  if (!ultimo) return null;

  const desde = DateTime.fromISO(ultimo, { zone: 'utc' });
  if (!desde.isValid) return null;

  const disponible = desde.plus({ days: DIAS_ENTRE_ANALISIS });
  return disponible > DateTime.fromJSDate(now, { zone: 'utc' }) ? disponible.toISO() : null;
}

/**
 * ¿Hay cupo para lanzar ahora?
 *
 * @param {object|null} userData
 * @param {Date} [now]
 * @returns {boolean}
 */
function tieneCupo(userData, now = new Date()) {
  return nextAvailableAt(userData, now) === null;
}

/**
 * Cuántos análisis le quedan, para decírselo al usuario.
 *
 * Es 1 o 0: el cupo es uno por semana. Se devuelve como número y no como
 * booleano porque el frontend ya lo pinta así y porque el día que la frecuencia
 * cambie, no habrá que tocar la frontera.
 *
 * @param {object|null} userData
 * @param {Date} [now]
 * @returns {number}
 */
function restantesHoy(userData, now = new Date()) {
  if (!tieneAcceso(userData, now)) return 0;
  return nextAvailableAt(userData, now) === null ? 1 : 0;
}

/**
 * Consume el cupo. **Sólo lo llama el cierre con éxito** (RN-5-C): un análisis
 * que falla, o que se da por colgado, no cuesta nada.
 *
 * `copilotAnalysisCount` es contabilidad histórica acumulada y nunca decrece,
 * ni siquiera al borrar un análisis del historial.
 *
 * Y todo esto vive en `userData/{uid}`, no en el análisis: por eso borrar un
 * análisis del historial no devuelve cupo.
 *
 * @param {string} userId
 * @param {Date} [now]
 * @returns {Promise<void>}
 */
async function consumeQuota(userId, now = new Date()) {
  await admin
    .firestore()
    .collection('userData')
    .doc(userId)
    .set(
      {
        lastCopilotAnalysis: now.toISOString(),
        copilotAnalysisCount: admin.firestore.FieldValue.increment(1),
      },
      { merge: true },
    );
}

module.exports = {
  PLANES_CON_ACCESO,
  ESTADOS_VIGENTES,
  DIAS_ENTRE_ANALISIS,
  tieneAcceso,
  suscripcionVencida,
  diaDelCupo,
  restantesHoy,
  tieneCupo,
  nextAvailableAt,
  consumeQuota,
};
