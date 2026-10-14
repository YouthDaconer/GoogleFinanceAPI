/**
 * HU 5.2 — Los dos avisos del Copilot en la campana.
 *
 * Replica lo que hace el evaluador de alertas de precio
 * (`src/finance-query/src/streaming/alert_evaluator.py`, `_process_triggered_alerts`),
 * que es el único código del proyecto que crea una notificación: escribir en
 * `userNotifications` **y** estampar `userData/{uid}.lastNotificationAt`. La
 * segunda escritura es la que hace que la campana se entere sin recargar
 * (Escenario 3), porque `userData` ya está suscrito con `onSnapshot` en el
 * frontend; sin ella el aviso no aparecería hasta un refresco posterior.
 *
 * Diferencia deliberada con el evaluador: éste guarda el texto ya traducido al
 * idioma que el usuario tenía al dispararse la alerta. QA-30 pide lo contrario
 * para el Copilot — el aviso se lee en el idioma activo **al leerlo**—, así que
 * lo que manda es el `type` y el texto se resuelve con i18n en el cliente.
 * `title` y `message` se escriben igual, en el idioma del lanzamiento, como
 * respaldo de cualquier consumidor que lea el documento tal cual.
 *
 * RN-5-K — sólo dentro de la app y persistente: ni push ni correo.
 *
 * @module services/copilot/copilotNotifications
 * @see platform-docs/stories/5.2-copilot-segundo-plano-progreso/refinamiento.md (T7)
 */

const admin = require('../firebaseAdmin');
const { NOTIFICATION_TYPE } = require('./copilotConstants');

/**
 * Textos de respaldo. El usuario ve los del diccionario i18n del frontend; si
 * algo leyera el documento en crudo, encontraría lo mismo.
 *
 * El aviso de fallo no menciona proveedores, códigos ni "502" (Escenario 8).
 * La HU 5.5 ajustará este texto por plan.
 */
const TEXTOS = {
  [NOTIFICATION_TYPE.ANALYSIS_READY]: {
    es: {
      title: 'Tu análisis del portafolio está listo',
      message: 'Ya puedes ver el análisis completo de tu portafolio.',
    },
    en: {
      title: 'Your portfolio analysis is ready',
      message: 'You can now see the full analysis of your portfolio.',
    },
  },
  [NOTIFICATION_TYPE.ANALYSIS_FAILED]: {
    es: {
      title: 'No pudimos completar tu análisis',
      message: 'Tu crédito no fue consumido. Inténtalo más tarde.',
    },
    en: {
      title: "We couldn't complete your analysis",
      message: 'Your credit was not used. Try again later.',
    },
  },
};

/**
 * Crea el aviso y despierta a la campana.
 *
 * @param {string} userId
 * @param {string} type - Un valor de `NOTIFICATION_TYPE`.
 * @param {{ language?: string, analysisId?: string|null }} [opciones]
 * @returns {Promise<void>}
 */
async function notify(userId, type, { language = 'es', analysisId = null } = {}) {
  const db = admin.firestore();
  const textos = TEXTOS[type];
  if (!textos) throw new Error(`Tipo de aviso desconocido: ${type}`);

  const texto = textos[language] || textos.es;
  const now = new Date().toISOString();

  await db.collection('userNotifications').add({
    userId,
    type,
    title: texto.title,
    message: texto.message,
    // `analysisId` es lo que hace que "Ver análisis" lleve a ese análisis
    // concreto (Escenario 3). No se reutiliza `alertId`: el store de la campana
    // deduplica por ese campo y mezclarlos borraría avisos legítimos.
    data: analysisId ? { analysisId } : {},
    isRead: false,
    createdAt: now,
  });

  try {
    await db.collection('userData').doc(userId).set({ lastNotificationAt: now }, { merge: true });
  } catch (error) {
    // El aviso ya está guardado: el usuario lo verá en el siguiente refresco.
    // Perder la señal es una molestia, no un motivo para dar el análisis por
    // fallido.
    console.error('[copilotNotifications] No se pudo estampar lastNotificationAt:', error);
  }
}

/**
 * "Tu análisis del portafolio está listo".
 *
 * @param {string} userId
 * @param {{ language?: string, analysisId: string }} opciones
 * @returns {Promise<void>}
 */
function notifyAnalysisReady(userId, { language, analysisId }) {
  return notify(userId, NOTIFICATION_TYPE.ANALYSIS_READY, { language, analysisId });
}

/**
 * "No pudimos completar tu análisis. Tu crédito no fue consumido."
 *
 * @param {string} userId
 * @param {{ language?: string }} [opciones]
 * @returns {Promise<void>}
 */
function notifyAnalysisFailed(userId, { language } = {}) {
  return notify(userId, NOTIFICATION_TYPE.ANALYSIS_FAILED, { language });
}

module.exports = {
  TEXTOS,
  notify,
  notifyAnalysisReady,
  notifyAnalysisFailed,
};
