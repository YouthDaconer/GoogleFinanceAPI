/**
 * Configuración centralizada de URLs de API
 * 
 * SEC-CF-001: Migración a Cloudflare Tunnel
 * SEC-TOKEN-001: Token de servicio para autenticación server-to-server
 * Fecha: 19 de Enero de 2026
 * 
 * Este archivo centraliza la configuración de URLs del API de finanzas
 * para facilitar cambios futuros y permitir configuración por entorno.
 * 
 * @module services/config
 * @see docs/architecture/SEC-CF-001-cloudflare-tunnel-migration-plan.md
 * @see docs/architecture/SEC-TOKEN-001-api-security-hardening-plan.md
 */

/**
 * URL base del API de finanzas (finance-query)
 * 
 * En producción: via Cloudflare Tunnel (wss://ws.portastock.top)
 * En desarrollo: localhost o variable de entorno
 * 
 * La variable de entorno FINANCE_QUERY_API_URL puede configurarse en:
 * - Firebase Functions: firebase functions:config:set api.finance_query_url="..."
 * - Archivo .env: FINANCE_QUERY_API_URL=...
 * 
 * @type {string}
 */
const FINANCE_QUERY_API_URL = process.env.FINANCE_QUERY_API_URL || 
  'https://ws.portastock.top/v1';

/**
 * URL legacy de Lambda (solo para referencia/fallback)
 * @deprecated Usar FINANCE_QUERY_API_URL en su lugar
 * @type {string}
 */
const LEGACY_LAMBDA_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

/**
 * SEC-TOKEN-003: Token de servicio para autenticación server-to-server
 * 
 * Este token se envía en el header x-service-token para que el API
 * pueda identificar llamadas desde Cloud Functions sin requerir
 * el token HMAC de usuario.
 * 
 * Usa CF_SERVICE_TOKEN como nombre de variable para evitar conflicto con
 * secrets previamente configurados en Cloud Run.
 * 
 * @type {string}
 */
const SERVICE_TOKEN_SECRET = process.env.CF_SERVICE_TOKEN || 
  process.env.SERVICE_TOKEN_SECRET || '';

/**
 * Genera headers de autenticación para llamadas al API
 * 
 * SEC-TOKEN-004: Todas las llamadas server-to-server deben incluir
 * el header x-service-token para pasar el middleware de autenticación.
 * 
 * SEC-CF-002: Incluye Referer y User-Agent para pasar Cloudflare WAF.
 * 
 * @param {Object} additionalHeaders - Headers adicionales a incluir
 * @returns {Object} Headers con autenticación
 */
function getServiceHeaders(additionalHeaders = {}) {
  const headers = {
    'Content-Type': 'application/json',
    // SEC-CF-002: Cloudflare WAF requiere User-Agent y Referer válidos
    'User-Agent': 'google-cloud-functions/1.0 portafolio-inversiones',
    'Referer': 'https://us-central1-portafolio-inversiones.cloudfunctions.net',
    ...additionalHeaders,
  };
  
  // Solo agregar si está configurado
  if (SERVICE_TOKEN_SECRET) {
    headers['x-service-token'] = SERVICE_TOKEN_SECRET;
  }
  
  return headers;
}

/**
 * Verifica si la autenticación de servicio está configurada
 * @returns {boolean}
 */
function isServiceAuthConfigured() {
  return Boolean(SERVICE_TOKEN_SECRET);
}

module.exports = {
  FINANCE_QUERY_API_URL,
  LEGACY_LAMBDA_URL,
  SERVICE_TOKEN_SECRET,
  getServiceHeaders,
  isServiceAuthConfigured,
};
