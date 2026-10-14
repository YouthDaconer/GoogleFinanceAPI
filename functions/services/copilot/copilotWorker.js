/**
 * HU 5.2 — Quien hace el trabajo cuando el usuario ya se fue.
 *
 * Se dispara al abrirse un análisis y corre las tres etapas. Es la pieza que
 * cumple RN-5.2-N: el análisis termina o falla por sí mismo, con independencia
 * de lo que haga el usuario —navegar, cerrar el panel, recargar, cerrar el
 * navegador, cerrar sesión o cambiar de dispositivo—.
 *
 * **Por qué un disparador de Firestore.** Los estándares (§4.7) listan `onCall`,
 * `onRequest`, `onSchedule` y `beforeUserCreated`; ninguno sirve para trabajo
 * que sobrevive a la petición. El único sustituto dentro de esa lista sería un
 * `onSchedule` que recogiera la cola cada minuto, y eso mete hasta 60 s de
 * espera muerta antes de que el análisis arranque. `onDocumentWritten` es v2,
 * que es lo que la regla exige; la ampliación queda declarada en el plan.
 *
 * El documento es `copilotRuns/{userId}`, uno por usuario, así que un segundo
 * análisis **actualiza** el mismo documento en vez de crear otro: por eso se
 * escucha `onDocumentWritten` y no `onDocumentCreated`, y por eso la guarda
 * mira que el `runId` haya cambiado.
 *
 * @module services/copilot/copilotWorker
 * @see platform-docs/stories/5.2-copilot-segundo-plano-progreso/refinamiento.md (T10)
 */

const { onDocumentWritten } = require('firebase-functions/v2/firestore');
const { defineSecret } = require('firebase-functions/params');
const analysisService = require('./copilotAnalysisService');
const { RUN_STATUS, COPILOT_RUNS_COLLECTION } = require('./copilotConstants');

/** Firma la llamada al motor (HU 5.2, D5). Lo comparte con la Lambda. */
const marketDataTokenSecret = defineSecret('MARKET_DATA_TOKEN_SECRET');

/** Autentica las llamadas a finance-query (analistas, noticias, riesgo). */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

/**
 * 540 s es el techo de una función v2 por eventos. El motor tardó 45 s de
 * mediana y 51 s como máximo en la prueba de la HU 5.1, y las dos primeras
 * etapas están acotadas por sus propios plazos: el margen es amplio a
 * propósito, porque quedarse corto aquí deja un análisis colgado que el
 * vigilante tiene que recoger 15 minutos después.
 */
const FUNCTION_CONFIG = {
  document: `${COPILOT_RUNS_COLLECTION}/{userId}`,
  memory: '512MiB',
  timeoutSeconds: 540,
  maxInstances: 10,
  minInstances: 0,
  retry: false,
  secrets: [marketDataTokenSecret, cfServiceToken],
};

/**
 * ¿Esta escritura abre un análisis nuevo?
 *
 * Las escrituras de progreso del propio worker vuelven a disparar la función;
 * sin esta guarda, cada avance de etapa lanzaría otro análisis.
 *
 * @param {object|null} antes
 * @param {object|null} despues
 * @returns {boolean}
 */
function esLanzamiento(antes, despues) {
  if (!despues || despues.status !== RUN_STATUS.QUEUED) return false;
  return !antes || antes.runId !== despues.runId;
}

const copilotAnalysisWorker = onDocumentWritten(FUNCTION_CONFIG, async (event) => {
  const antes = event.data && event.data.before.exists ? event.data.before.data() : null;
  const despues = event.data && event.data.after.exists ? event.data.after.data() : null;

  if (!esLanzamiento(antes, despues)) return;

  const run = { ...despues, userId: despues.userId || event.params.userId };
  console.log(`[copilotAnalysisWorker] Análisis ${run.runId} de ${run.userId}`);

  const resultado = await analysisService.runAnalysis(run, {
    marketDataTokenSecret: marketDataTokenSecret.value(),
  });

  console.log(`[copilotAnalysisWorker] Análisis ${run.runId}: ${resultado.desenlace}`);
});

module.exports = { copilotAnalysisWorker, esLanzamiento, FUNCTION_CONFIG };
