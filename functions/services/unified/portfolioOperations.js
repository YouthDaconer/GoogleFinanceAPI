/**
 * Portfolio Operations - Router unificado para operaciones de assets
 * 
 * SCALE-CF-001: Consolida 8 Cloud Functions HTTP en un solo endpoint
 * con action router para reducir cold starts y costos.
 * 
 * Acciones disponibles:
 * - createAsset
 * - updateAsset
 * - sellAsset
 * - deleteAsset
 * - deleteAssets
 * - sellPartialAssetsFIFO
 * - addCashTransaction
 * - updateStockSector
 * 
 * @module unified/portfolioOperations
 * @see docs/stories/56.story.md
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { rateLimiter } = require('../../utils/rateLimiter');
const { getRateLimitConfig } = require('../../config/rateLimits');

// Importar handlers individuales
const assetHandlers = require('../handlers/assetHandlers');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/**
 * Configuración de Cloud Function
 */
const FUNCTION_CONFIG = {
  cors: true,
  memory: "256MiB",
  timeoutSeconds: 60,
  maxInstances: 10,
  minInstances: 0,
};

/**
 * Mapeo de acciones a handlers
 */
const ACTION_HANDLERS = {
  createAsset: assetHandlers.createAsset,
  updateAsset: assetHandlers.updateAsset,
  sellAsset: assetHandlers.sellAsset,
  deleteAsset: assetHandlers.deleteAsset,
  deleteAssets: assetHandlers.deleteAssets,
  sellPartialAssetsFIFO: assetHandlers.sellPartialAssetsFIFO,
  addCashTransaction: assetHandlers.addCashTransaction,
  updateStockSector: assetHandlers.updateStockSector,
};

/**
 * Lista de acciones válidas para mensajes de error
 */
const VALID_ACTIONS = Object.keys(ACTION_HANDLERS);

// ============================================================================
// ROUTER PRINCIPAL
// ============================================================================

/**
 * Router de operaciones de portafolio
 * 
 * CRÍTICO: Rate limiting se aplica POR ACCIÓN, no por función unificada.
 * Esto preserva la granularidad de límites existente en config/rateLimits.js
 * 
 * @example
 * // Llamada desde frontend
 * const portfolioOperations = httpsCallable(functions, 'portfolioOperations');
 * const result = await portfolioOperations({
 *   action: 'createAsset',
 *   payload: { portfolioAccount: '...', name: 'AAPL', ... }
 * });
 */
const portfolioOperations = onCall(
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

    // 4. Aplicar rate limiting POR ACCIÓN (no por función)
    // Usa la configuración existente de config/rateLimits.js
    const rateLimitConfig = getRateLimitConfig(action);
    const rateLimitKey = `portfolioOperations:${action}`;
    
    let rateLimitInfo;
    try {
      rateLimitInfo = await rateLimiter.checkLimit(
        auth.uid, 
        rateLimitKey, 
        rateLimitConfig
      );
    } catch (rateLimitError) {
      // Si es error de rate limit, propagarlo directamente
      if (rateLimitError.code === 'resource-exhausted') {
        console.warn(`[portfolioOperations][${action}] Rate limit exceeded for user: ${auth.uid}`);
        throw rateLimitError;
      }
      // Otros errores del rate limiter no deberían bloquear la operación
      console.error(`[portfolioOperations][${action}] Rate limiter error:`, rateLimitError);
    }

    // 5. Log de inicio
    console.log(`[portfolioOperations][${action}] Start - userId: ${auth.uid}`);

    // 6. Ejecutar handler con manejo de errores unificado
    try {
      const context = {
        auth,
        rawRequest: request.rawRequest,
      };
      
      const result = await handler(context, payload);
      
      // Log de éxito con timing
      const duration = Date.now() - startTime;
      console.log(`[portfolioOperations][${action}] Success - userId: ${auth.uid}, duration: ${duration}ms`);
      
      // Incluir info de rate limit en respuesta si está disponible
      if (result && typeof result === 'object' && !Array.isArray(result) && rateLimitInfo) {
        return {
          ...result,
          _rateLimitInfo: rateLimitInfo,
        };
      }
      
      return result;
      
    } catch (error) {
      // Log de error con detalles
      const duration = Date.now() - startTime;
      console.error(`[portfolioOperations][${action}] Error - userId: ${auth.uid}, duration: ${duration}ms`, {
        errorCode: error.code,
        errorMessage: error.message,
      });
      
      // Si ya es HttpsError, propagarlo directamente
      if (error.code?.startsWith('functions/') || error.httpErrorCode) {
        throw error;
      }
      
      // Envolver errores no manejados
      throw new HttpsError('internal', 'Error procesando la operación');
    }
  }
);

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = { 
  portfolioOperations,
  // Exportar también la lista de acciones para documentación/testing
  VALID_ACTIONS,
};
