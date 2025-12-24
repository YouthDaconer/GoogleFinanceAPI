/**
 * Account Operations - Router unificado para operaciones de portfolioAccounts
 * 
 * SCALE-CF-001: Consolida 4 Cloud Functions HTTP en un solo endpoint
 * con action router para reducir cold starts y costos.
 * 
 * Acciones disponibles:
 * - addPortfolioAccount
 * - updatePortfolioAccount
 * - deletePortfolioAccount
 * - updatePortfolioAccountBalance
 * 
 * @module unified/accountOperations
 * @see docs/stories/56.story.md
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { rateLimiter } = require('../../utils/rateLimiter');
const { getRateLimitConfig } = require('../../config/rateLimits');

// Importar handlers individuales
const accountHandlers = require('../handlers/accountHandlers');

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
  addPortfolioAccount: accountHandlers.addPortfolioAccount,
  updatePortfolioAccount: accountHandlers.updatePortfolioAccount,
  deletePortfolioAccount: accountHandlers.deletePortfolioAccount,
  updatePortfolioAccountBalance: accountHandlers.updatePortfolioAccountBalance,
};

/**
 * Lista de acciones válidas para mensajes de error
 */
const VALID_ACTIONS = Object.keys(ACTION_HANDLERS);

// ============================================================================
// ROUTER PRINCIPAL
// ============================================================================

/**
 * Router de operaciones de cuentas de portafolio
 * 
 * @example
 * // Llamada desde frontend
 * const accountOperations = httpsCallable(functions, 'accountOperations');
 * const result = await accountOperations({
 *   action: 'addPortfolioAccount',
 *   payload: { name: 'Mi Cuenta', description: 'Cuenta principal' }
 * });
 */
const accountOperations = onCall(
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
    const rateLimitKey = `accountOperations:${action}`;
    
    let rateLimitInfo;
    try {
      rateLimitInfo = await rateLimiter.checkLimit(
        auth.uid, 
        rateLimitKey, 
        rateLimitConfig
      );
    } catch (rateLimitError) {
      if (rateLimitError.code === 'resource-exhausted') {
        console.warn(`[accountOperations][${action}] Rate limit exceeded for user: ${auth.uid}`);
        throw rateLimitError;
      }
      console.error(`[accountOperations][${action}] Rate limiter error:`, rateLimitError);
    }

    // 5. Log de inicio
    console.log(`[accountOperations][${action}] Start - userId: ${auth.uid}`);

    // 6. Ejecutar handler
    try {
      const context = {
        auth,
        rawRequest: request.rawRequest,
      };
      
      const result = await handler(context, payload);
      
      const duration = Date.now() - startTime;
      console.log(`[accountOperations][${action}] Success - userId: ${auth.uid}, duration: ${duration}ms`);
      
      if (result && typeof result === 'object' && !Array.isArray(result) && rateLimitInfo) {
        return {
          ...result,
          _rateLimitInfo: rateLimitInfo,
        };
      }
      
      return result;
      
    } catch (error) {
      const duration = Date.now() - startTime;
      console.error(`[accountOperations][${action}] Error - userId: ${auth.uid}, duration: ${duration}ms`, {
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
  accountOperations,
  VALID_ACTIONS,
};
