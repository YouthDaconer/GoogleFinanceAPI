/**
 * Query Operations - Router unificado para operaciones de consulta
 * 
 * SCALE-CF-001: Consolida 6 Cloud Functions HTTP de consulta en un solo endpoint
 * con action router para reducir cold starts y costos.
 * 
 * COST-OPT-001: Agregadas acciones getHistoricalReturnsOptimized y 
 * getConsolidatedDataStatus para rendimientos con períodos consolidados.
 * 
 * NOTA: Esta función usa 512MiB de memoria porque getHistoricalReturns y 
 * getMultiAccountHistoricalReturns lo requieren. Las otras funciones están
 * sobre-provisionadas pero se benefician de instancias calientes compartidas.
 * 
 * Acciones disponibles:
 * - getCurrentPricesForUser
 * - getHistoricalReturns
 * - getHistoricalReturnsOptimized (COST-OPT-001: V2 con períodos consolidados)
 * - getMultiAccountHistoricalReturns
 * - getIndexHistory
 * - getPortfolioDistribution
 * - getAvailableSectors
 * - getConsolidatedDataStatus (COST-OPT-001: Diagnóstico de datos)
 * - getPerformanceOnDemand (OPT-DEMAND-102: Rendimiento con precios live)
 *
 * @module unified/queryOperations
 * @see docs/stories/56.story.md
 * @see docs/stories/62.story.md (COST-OPT-001)
 * @see docs/stories/74.story.md (OPT-DEMAND-102)
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { rateLimiter } = require('../../utils/rateLimiter');
const { getRateLimitConfig } = require('../../config/rateLimits');

// Importar handlers individuales
const queryHandlers = require('../handlers/queryHandlers');
const { getPerformanceOnDemand } = require('../handlers/performanceOnDemandHandler');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/**
 * Configuración de Cloud Function
 * 
 * Memory: 512MiB (requerido por getHistoricalReturns)
 * MaxInstances: 20 (alto tráfico esperado ~1500 invocaciones/día)
 * 
 * SEC-TOKEN-001: CF_SERVICE_TOKEN se carga desde .env para autenticación
 * server-to-server con el API finance-query via Cloudflare Tunnel.
 */
const FUNCTION_CONFIG = {
  cors: true,
  memory: "512MiB",
  timeoutSeconds: 60,
  maxInstances: 20,
  minInstances: 0,
};

/**
 * Mapeo de acciones a handlers
 */
const ACTION_HANDLERS = {
  getCurrentPricesForUser: queryHandlers.getCurrentPricesForUser,
  getHistoricalReturns: queryHandlers.getHistoricalReturns,
  getMultiAccountHistoricalReturns: queryHandlers.getMultiAccountHistoricalReturns,
  getIndexHistory: queryHandlers.getIndexHistory,
  getPortfolioDistribution: queryHandlers.getPortfolioDistribution,
  getAvailableSectors: queryHandlers.getAvailableSectors,
  // COST-OPT-001: Nuevas acciones para rendimientos optimizados (V2)
  getHistoricalReturnsOptimized: queryHandlers.getHistoricalReturnsOptimized,
  getConsolidatedDataStatus: queryHandlers.getConsolidatedDataStatus,
  // OPT-DEMAND-102: Rendimiento on-demand con precios en vivo del API Lambda
  getPerformanceOnDemand: getPerformanceOnDemand,
};

/**
 * Lista de acciones válidas para mensajes de error
 */
const VALID_ACTIONS = Object.keys(ACTION_HANDLERS);

// ============================================================================
// ROUTER PRINCIPAL
// ============================================================================

/**
 * Router de operaciones de consulta
 * 
 * @example
 * // Llamada desde frontend
 * const queryOperations = httpsCallable(functions, 'queryOperations');
 * const result = await queryOperations({
 *   action: 'getHistoricalReturns',
 *   payload: { currency: 'USD', accountId: '...' }
 * });
 */
const queryOperations = onCall(
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
    const rateLimitKey = `queryOperations:${action}`;
    
    let rateLimitInfo;
    try {
      rateLimitInfo = await rateLimiter.checkLimit(
        auth.uid, 
        rateLimitKey, 
        rateLimitConfig
      );
    } catch (rateLimitError) {
      if (rateLimitError.code === 'resource-exhausted') {
        console.warn(`[queryOperations][${action}] Rate limit exceeded for user: ${auth.uid}`);
        throw rateLimitError;
      }
      console.error(`[queryOperations][${action}] Rate limiter error:`, rateLimitError);
    }

    // 5. Log de inicio
    console.log(`[queryOperations][${action}] Start - userId: ${auth.uid}`);

    // 6. Ejecutar handler
    try {
      const context = {
        auth,
        rawRequest: request.rawRequest,
      };
      
      const result = await handler(context, payload);
      
      const duration = Date.now() - startTime;
      console.log(`[queryOperations][${action}] Success - userId: ${auth.uid}, duration: ${duration}ms`);
      
      if (result && typeof result === 'object' && !Array.isArray(result) && rateLimitInfo) {
        return {
          ...result,
          _rateLimitInfo: rateLimitInfo,
        };
      }
      
      return result;
      
    } catch (error) {
      const duration = Date.now() - startTime;
      console.error(`[queryOperations][${action}] Error - userId: ${auth.uid}, duration: ${duration}ms`, {
        errorCode: error.code,
        errorMessage: error.message,
      });
      
      if (error.code?.startsWith('functions/') || error.httpErrorCode) {
        throw error;
      }
      
      throw new HttpsError('internal', 'Error procesando la consulta');
    }
  }
);

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = { 
  queryOperations,
  VALID_ACTIONS,
};
