/**
 * Settings Operations - Router unificado para operaciones de currencies y userData
 * 
 * SCALE-CF-001: Consolida 6 Cloud Functions HTTP en un solo endpoint
 * con action router para reducir cold starts y costos.
 * 
 * Acciones disponibles:
 * - addCurrency
 * - updateCurrency
 * - deleteCurrency
 * - updateDefaultCurrency
 * - updateUserCountry
 * - updateUserDisplayName
 * 
 * @module unified/settingsOperations
 * @see docs/stories/56.story.md
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { rateLimiter } = require('../../utils/rateLimiter');
const { getRateLimitConfig } = require('../../config/rateLimits');

// Importar handlers individuales
const settingsHandlers = require('../handlers/settingsHandlers');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/**
 * Configuración de Cloud Function
 */
const FUNCTION_CONFIG = {
  cors: true,
  memory: "256MiB",
  timeoutSeconds: 30,
  maxInstances: 5,
  minInstances: 0,
};

/**
 * Mapeo de acciones a handlers
 */
const ACTION_HANDLERS = {
  addCurrency: settingsHandlers.addCurrency,
  updateCurrency: settingsHandlers.updateCurrency,
  deleteCurrency: settingsHandlers.deleteCurrency,
  updateDefaultCurrency: settingsHandlers.updateDefaultCurrency,
  updateUserCountry: settingsHandlers.updateUserCountry,
  updateUserDisplayName: settingsHandlers.updateUserDisplayName,
};

/**
 * Lista de acciones válidas para mensajes de error
 */
const VALID_ACTIONS = Object.keys(ACTION_HANDLERS);

// ============================================================================
// ROUTER PRINCIPAL
// ============================================================================

/**
 * Router de operaciones de settings
 * 
 * @example
 * // Llamada desde frontend
 * const settingsOperations = httpsCallable(functions, 'settingsOperations');
 * const result = await settingsOperations({
 *   action: 'addCurrency',
 *   payload: { code: 'EUR', name: 'Euro', symbol: '€', exchangeRate: 0.92 }
 * });
 */
const settingsOperations = onCall(
  FUNCTION_CONFIG,
  async (request) => {
    const { auth, data } = request;
    const startTime = Date.now();
    
    // 1. Validar autenticación
    if (!auth) {
      throw new HttpsError('unauthenticated', 'Autenticación requerida');
    }

    const { action, payload } = data || {};

    // 2. Validar que se especificó una acción
    if (!action || typeof action !== 'string') {
      throw new HttpsError(
        'invalid-argument', 
        'Se requiere especificar una acción'
      );
    }

    // 3. Validar que la acción existe
    const handler = ACTION_HANDLERS[action];
    if (!handler) {
      throw new HttpsError(
        'invalid-argument', 
        `Acción no válida: ${action}. Acciones permitidas: ${VALID_ACTIONS.join(', ')}`
      );
    }

    // 4. Aplicar rate limiting POR ACCIÓN
    const rateLimitConfig = getRateLimitConfig(action);
    const rateLimitKey = `settingsOperations:${action}`;
    
    let rateLimitInfo;
    try {
      rateLimitInfo = await rateLimiter.checkLimit(
        auth.uid, 
        rateLimitKey, 
        rateLimitConfig
      );
    } catch (rateLimitError) {
      if (rateLimitError.code === 'resource-exhausted') {
        console.warn(`[settingsOperations][${action}] Rate limit exceeded for user: ${auth.uid}`);
        throw rateLimitError;
      }
      console.error(`[settingsOperations][${action}] Rate limiter error:`, rateLimitError);
    }

    // 5. Log de inicio
    console.log(`[settingsOperations][${action}] Start - userId: ${auth.uid}`);

    // 6. Ejecutar handler
    try {
      const context = {
        auth,
        rawRequest: request.rawRequest,
      };
      
      const result = await handler(context, payload);
      
      const duration = Date.now() - startTime;
      console.log(`[settingsOperations][${action}] Success - userId: ${auth.uid}, duration: ${duration}ms`);
      
      if (result && typeof result === 'object' && !Array.isArray(result) && rateLimitInfo) {
        return {
          ...result,
          _rateLimitInfo: rateLimitInfo,
        };
      }
      
      return result;
      
    } catch (error) {
      const duration = Date.now() - startTime;
      console.error(`[settingsOperations][${action}] Error - userId: ${auth.uid}, duration: ${duration}ms`, {
        errorCode: error.code,
        errorMessage: error.message,
      });
      
      if (error.code?.startsWith('functions/') || error.httpErrorCode) {
        throw error;
      }
      
      throw new HttpsError('internal', 'Error procesando la operación');
    }
  }
);

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = { 
  settingsOperations,
  VALID_ACTIONS,
};
