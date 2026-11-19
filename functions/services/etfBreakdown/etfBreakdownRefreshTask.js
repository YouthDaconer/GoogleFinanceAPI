/**
 * HU 6.16 — El refresco aparte de un desglose de ETF vencido (RN-6.16-B, D1).
 *
 * Cuando un cálculo de la distribución usa un desglose con más de 7 días, lo
 * usa tal cual y encola aquí su refresco: el inversor no espera a la fuente.
 * Una tarea y no «lanzar y no esperar» dentro de la petición porque en Cloud
 * Functions v2 la CPU se corta al responder y el refresco moriría a medias.
 *
 * - **Un intento al día** (ajuste 1 del PO): quien encola ya reclamó el día en
 *   `etfBreakdowns/{SÍMBOLO}.ultimoIntentoEn`. Por eso la cola no reintenta
 *   (`maxAttempts: 1`): el reintento es el encolado de otro día (RN-6.16-C).
 * - **Si la fuente falla** o trae menos de la mitad de las posiciones, se
 *   conserva el guardado y se anota el motivo (RN-6.16-C, RN-6.16-F); eso lo
 *   hace `refrescarDesglose`.
 *
 * La cola `etfBreakdownRefreshTask` se crea en la publicación (ver
 * `dev-record.md` de la HU).
 *
 * @module services/etfBreakdown/etfBreakdownRefreshTask
 * @see platform-docs/stories/6.16-desglose-etf-compartido/refinamiento.md (T3)
 */

const { onTaskDispatched } = require('firebase-functions/v2/tasks');
const { defineSecret } = require('firebase-functions/params');
const admin = require('../firebaseAdmin');

const { refrescarDesglose, normalizarSimbolo } = require('./etfBreakdownService');

/** SEC-TOKEN-001: la fuente es finance-query, que exige el token de servicio. */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

/**
 * Una instancia y un refresco a la vez bastan: hay pocos ETF y como mucho un
 * refresco al día por cada uno. 256 MiB alcanzan para un fondo de ~9.800 posiciones (~1 MB en JSON).
 * 120 s cubren la cadena de fuentes en frío (~18 s) con reintentos por 429.
 */
const FUNCTION_CONFIG = {
  retryConfig: {
    maxAttempts: 1,
  },
  rateLimits: {
    maxConcurrentDispatches: 1,
    maxDispatchesPerSecond: 1,
  },
  memory: '256MiB',
  timeoutSeconds: 120,
  maxInstances: 1,
  minInstances: 0,
  // Con 256 MiB la CPU es fraccional y Cloud Run exige `concurrency: 1`
  concurrency: 1,
  secrets: [cfServiceToken],
  labels: {
    status: 'active',
    purpose: 'etf-breakdown-refresh',
  },
};

/**
 * Atiende una tarea `{ symbol }`. Nunca lanza: un fallo de la fuente ya queda
 * anotado en el documento, y un error inesperado se registra para que el
 * siguiente día lo vuelva a intentar.
 *
 * @param {{data?: {symbol?: string}}} request
 * @returns {Promise<Object|undefined>} El resultado de `refrescarDesglose`
 */
async function atenderRefresco(request) {
  const symbol = normalizarSimbolo(request?.data?.symbol);
  if (!symbol) {
    // Una tarea mal formada no se arregla reintentándola: se registra y se descarta.
    console.error(`[etfBreakdownRefreshTask] Tarea inválida, falta symbol: ${JSON.stringify(request?.data)}`);
    return undefined;
  }

  try {
    const resultado = await refrescarDesglose(admin.firestore(), symbol);
    console.log(`[etfBreakdownRefreshTask] ${symbol}: ${resultado.resultado}` +
      (resultado.motivo ? ` (${resultado.motivo})` : ''));
    return resultado;
  } catch (error) {
    console.error(`[etfBreakdownRefreshTask] ${symbol}: error inesperado, se reintenta otro día — ${error.message}`);
    return undefined;
  }
}

const etfBreakdownRefreshTask = onTaskDispatched(FUNCTION_CONFIG, atenderRefresco);

module.exports = {
  etfBreakdownRefreshTask,
  atenderRefresco,
  FUNCTION_CONFIG,
};
