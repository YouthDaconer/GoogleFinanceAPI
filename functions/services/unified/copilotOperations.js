/**
 * HU 5.2 — Router de operaciones del Copilot.
 *
 * Mismo patrón que `queryOperations`: un `onCall` fino que autentica, limita
 * por acción y delega en `services/handlers/copilotHandlers.js`. El Copilot no
 * pasaba por Cloud Functions hasta esta historia; el navegador llamaba directo
 * a la Lambda por el Worker de Cloudflare que devuelve 502.
 *
 * @module services/unified/copilotOperations
 * @see platform-docs/stories/5.2-copilot-segundo-plano-progreso/refinamiento.md (T9)
 */

const { onCall, HttpsError } = require('firebase-functions/v2/https');
const { rateLimiter } = require('../../utils/rateLimiter');
const { getRateLimitConfig } = require('../../config/rateLimits');
const copilotHandlers = require('../handlers/copilotHandlers');

/**
 * Lanzar un análisis es barato —abre un documento y vuelve—, pero dispara
 * trabajo en segundo plano, así que el límite es el de una escritura costosa.
 */
const FUNCTION_CONFIG = {
  cors: true,
  memory: '256MiB',
  timeoutSeconds: 60,
  maxInstances: 10,
  minInstances: 0,
};

const ACTION_HANDLERS = {
  startAnalysis: copilotHandlers.startAnalysis,
  getAnalysisState: copilotHandlers.getAnalysisState,
};

const VALID_ACTIONS = Object.keys(ACTION_HANDLERS);

const copilotOperations = onCall(FUNCTION_CONFIG, async (request) => {
  const { auth, data } = request;

  if (!auth) {
    throw new HttpsError('unauthenticated', 'Autenticación requerida');
  }

  const { action, payload } = data || {};

  if (!action || typeof action !== 'string') {
    throw new HttpsError('invalid-argument', 'Se requiere especificar una acción');
  }

  const handler = ACTION_HANDLERS[action];
  if (!handler) {
    throw new HttpsError(
      'invalid-argument',
      `Acción no válida: ${action}. Acciones permitidas: ${VALID_ACTIONS.join(', ')}`,
    );
  }

  const rateLimitKey = `copilotOperations:${action}`;
  try {
    await rateLimiter.checkLimit(auth.uid, rateLimitKey, getRateLimitConfig(action));
  } catch (rateLimitError) {
    if (rateLimitError.code === 'resource-exhausted') {
      console.warn(`[copilotOperations][${action}] Rate limit excedido para: ${auth.uid}`);
      throw rateLimitError;
    }
    console.error(`[copilotOperations][${action}] Error del rate limiter:`, rateLimitError);
  }

  try {
    return await handler({ auth, rawRequest: request.rawRequest }, payload);
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error(`[copilotOperations][${action}] Error:`, error);
    throw new HttpsError('internal', 'No se pudo completar la operación del Copilot');
  }
});

module.exports = { copilotOperations, ACTION_HANDLERS, VALID_ACTIONS };
