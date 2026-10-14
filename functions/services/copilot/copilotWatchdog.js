/**
 * HU 5.2 — Un análisis colgado no bloquea a nadie.
 *
 * RN-5.2-Q: 15 minutos sin cambiar de etapa es un fallo. El frontend ya lo
 * deriva del documento suscrito para dibujarlo justo al cumplirse el plazo,
 * pero un aviso en la campana no lo puede crear el navegador, y liberar el cupo
 * tampoco. De eso se encarga este vigilante.
 *
 * Corre cada minuto. Es una consulta acotada contra un índice y, en el caso
 * normal, no encuentra nada: ningún análisis sano llega a los 15 minutos sin
 * avanzar.
 *
 * El plazo se mide contra la **última etapa**, no contra el inicio: un análisis
 * de 20 minutos repartido en etapas de 6, 7 y 7 no está colgado (QA-20).
 *
 * @module services/copilot/copilotWatchdog
 * @see platform-docs/stories/5.2-copilot-segundo-plano-progreso/refinamiento.md (T11)
 */

const { onSchedule } = require('firebase-functions/v2/scheduler');
const runRepository = require('./copilotRunRepository');
const notifications = require('./copilotNotifications');
const { FAILURE_CODE } = require('./copilotConstants');

const FUNCTION_CONFIG = {
  schedule: 'every 1 minutes',
  timeZone: 'America/New_York',
  memory: '256MiB',
  timeoutSeconds: 60,
  maxInstances: 1,
  minInstances: 0,
};

/**
 * Da por fallidos los análisis que dejaron de avanzar.
 *
 * @param {Date} [now]
 * @returns {Promise<{revisados: number, fallidos: number}>}
 */
async function marcarColgados(now = new Date()) {
  const colgados = await runRepository.findStalledRuns(now);
  let fallidos = 0;

  for (const run of colgados) {
    // La guarda de `failRun` vuelve a comprobar el estado dentro de una
    // transacción: si el análisis terminó entre la consulta y ahora, no se
    // pisa su desenlace ni se avisa de un fallo que no ocurrió.
    const cerrado = await runRepository.failRun(run.userId, run.runId, FAILURE_CODE.STALLED);
    if (!cerrado) continue;

    fallidos += 1;
    console.warn(
      `[copilotWatchdog] Análisis ${run.runId} de ${run.userId} colgado en la etapa ${run.stage}`,
    );

    try {
      await notifications.notifyAnalysisFailed(run.userId, { language: run.language });
    } catch (error) {
      // El análisis ya quedó liberado y el cupo intacto, que es lo que impide
      // que el usuario se quede bloqueado. El aviso se perdió: se registra.
      console.error(`[copilotWatchdog] No se pudo avisar a ${run.userId}:`, error);
    }
  }

  return { revisados: colgados.length, fallidos };
}

const scheduledCopilotWatchdog = onSchedule(FUNCTION_CONFIG, async () => {
  const { revisados, fallidos } = await marcarColgados();
  if (revisados > 0) {
    console.log(`[copilotWatchdog] Revisados: ${revisados}, dados por fallidos: ${fallidos}`);
  }
});

module.exports = { scheduledCopilotWatchdog, marcarColgados, FUNCTION_CONFIG };
