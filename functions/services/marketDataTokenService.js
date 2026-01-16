/**
 * Market Data Token Service
 * 
 * Genera tokens temporales HMAC-SHA256 para autenticar llamadas
 * desde el frontend al API Lambda finance-query.
 * 
 * @module marketDataTokenService
 * @see docs/stories/71.story.md (OPT-DEMAND-101)
 * @see docs/architecture/on-demand-pricing-architecture.md
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const crypto = require('crypto');

// Importar logger estructurado (SCALE-CORE-002)
const { StructuredLogger } = require('../utils/logger');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/**
 * Secret compartido con API Lambda para firmar tokens
 * @see docs/stories/70.story.md (OPT-DEMAND-100-B)
 */
const tokenSecret = defineSecret('MARKET_DATA_TOKEN_SECRET');

/**
 * Tiempo de vida del token en segundos
 * 5 minutos = balance entre seguridad y UX
 */
const TOKEN_TTL_SECONDS = 300;

/**
 * Audience del token - identifica para quién es válido
 */
const TOKEN_AUDIENCE = 'finance-query-api';

/**
 * Configuración de Cloud Function
 * 
 * Memory: 256MiB (necesario para Firebase Functions v2 overhead)
 * Timeout: 10s (debería completar en <100ms)
 * MaxInstances: 10 (bajo volumen esperado: 1 call / 5 min / usuario)
 */
const FUNCTION_CONFIG = {
  cors: [
    'https://portafolio-inversiones.web.app',
    'https://portafolio-inversiones.firebaseapp.com',
    'http://localhost:3000',
    'http://localhost:3001',
  ],
  secrets: [tokenSecret],  // ← Binding del secret
  timeoutSeconds: 10,
  memory: "256MiB",  // Aumentado de 128MiB (causaba OOM)
  minInstances: 0,
  maxInstances: 10,
};

// ============================================================================
// FUNCIONES AUXILIARES
// ============================================================================

/**
 * Valida que el usuario esté autenticado
 * @param {object} auth - Objeto de autenticación de Firebase
 * @throws {HttpsError} Si no hay autenticación
 */
const validateAuth = (auth) => {
  if (!auth) {
    throw new HttpsError(
      'unauthenticated',
      'Autenticación requerida'
    );
  }
};

/**
 * Genera un token HMAC-SHA256 firmado
 * 
 * Formato: payload_base64url.signature_base64url
 * 
 * @param {string} userId - UID del usuario
 * @param {string} secret - Secret para firmar
 * @param {number} ttlSeconds - Tiempo de vida en segundos
 * @returns {{ token: string, expiresAt: number }} Token y timestamp de expiración
 */
const generateToken = (userId, secret, ttlSeconds) => {
  const now = Math.floor(Date.now() / 1000);
  const expiresAt = now + ttlSeconds;
  
  // Payload del token
  const payload = {
    uid: userId,
    iat: now,           // Issued At
    exp: expiresAt,     // Expires At
    aud: TOKEN_AUDIENCE // Audience
  };
  
  // Codificar payload en base64url
  const payloadBase64 = Buffer.from(JSON.stringify(payload)).toString('base64url');
  
  // Firmar con HMAC-SHA256
  const signature = crypto
    .createHmac('sha256', secret)
    .update(payloadBase64)
    .digest('base64url');
  
  // Token final: payload.signature
  const token = `${payloadBase64}.${signature}`;
  
  return {
    token,
    expiresAt: expiresAt * 1000  // Convertir a milisegundos para JS
  };
};

// ============================================================================
// CLOUD FUNCTION PRINCIPAL
// ============================================================================

/**
 * Cloud Function: getMarketDataToken
 * 
 * Genera un token temporal para acceder al API Lambda finance-query.
 * El frontend usa este token para llamar directamente al API.
 * 
 * @example
 * const getMarketDataToken = httpsCallable(functions, 'getMarketDataToken');
 * const { token, expiresAt } = await getMarketDataToken();
 * 
 * // Usar token en llamada a API Lambda
 * fetch(`${API_URL}/market-quotes?symbols=AAPL`, {
 *   headers: { 'x-market-token': token }
 * });
 * 
 * @param {object} request - Request de Firebase
 * @param {object} request.auth - Información de autenticación
 * @returns {Promise<{ token: string, expiresAt: number, ttlSeconds: number }>}
 */
const getMarketDataToken = onCall(
  FUNCTION_CONFIG,
  async (request) => {
    const startTime = Date.now();
    const { auth } = request;
    
    // Crear logger con contexto
    const logger = new StructuredLogger('getMarketDataToken', {
      userId: auth?.uid || 'anonymous'
    });
    
    try {
      // 1. Validar autenticación
      validateAuth(auth);
      const userId = auth.uid;
      
      logger.info('Token requested', { userId });
      
      // 2. Obtener secret
      const secret = tokenSecret.value();
      
      if (!secret) {
        logger.error('Token secret not configured', new Error('MARKET_DATA_TOKEN_SECRET not set'));
        throw new HttpsError(
          'internal',
          'Error de configuración del servidor'
        );
      }
      
      // 3. Generar token
      const { token, expiresAt } = generateToken(userId, secret, TOKEN_TTL_SECONDS);
      
      // 4. Log de auditoría (sin exponer el token)
      const duration = Date.now() - startTime;
      logger.info('Token generated', {
        userId,
        expiresAt: new Date(expiresAt).toISOString(),
        ttlSeconds: TOKEN_TTL_SECONDS,
        durationMs: duration
      });
      
      // 5. Retornar respuesta
      return {
        token,
        expiresAt,
        ttlSeconds: TOKEN_TTL_SECONDS
      };
      
    } catch (error) {
      // Re-throw HttpsError sin modificar
      if (error instanceof HttpsError) {
        throw error;
      }
      
      // Log y wrap otros errores
      logger.error('Error generating token', error);
      
      throw new HttpsError(
        'internal',
        'Error generando token'
      );
    }
  }
);

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  getMarketDataToken,
  // Exportar para testing
  _generateToken: generateToken,
  _validateAuth: validateAuth,
  _TOKEN_TTL_SECONDS: TOKEN_TTL_SECONDS,
  _TOKEN_AUDIENCE: TOKEN_AUDIENCE
};
