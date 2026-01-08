/**
 * Authorization Utilities - Sistema de roles con Custom Claims
 * 
 * RBAC-001: Middleware de autorización para Cloud Functions
 * 
 * Firebase Best Practice: Usa Custom Claims del ID Token para verificar roles
 * en lugar de consultar Firestore (zero-cost, zero-latency).
 * 
 * @module utils/authorization
 * @see https://firebase.google.com/docs/auth/admin/custom-claims
 * @see docs/architecture/role-based-access-control-design.md
 */

const { HttpsError } = require('firebase-functions/v2/https');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/**
 * UIDs de administradores (fallback si Custom Claims no están configurados)
 * 
 * ⚠️ IMPORTANTE: Este es un fallback temporal durante la migración.
 * Una vez que todos los admins tengan Custom Claims configurados,
 * este fallback puede ser removido para mayor seguridad.
 */
const ADMIN_UIDS = ['DDeR8P5hYgfuN8gcU4RsQfdTJqx2'];

/**
 * Si se debe usar el fallback por UID cuando los claims no están configurados
 * Set to false después de ejecutar el script set-admin-claims.ts
 */
const ENABLE_UID_FALLBACK = true;

// ============================================================================
// FUNCIONES DE VERIFICACIÓN
// ============================================================================

/**
 * Verifica si el usuario es administrador usando Custom Claims.
 * 
 * ✅ Firebase Best Practice: Los claims ya vienen en context.auth.token
 * ✅ Zero-cost: No requiere consulta adicional a Firestore
 * ✅ Zero-latency: Los claims viajan en el JWT
 * 
 * @param {Object} context - Contexto de la Cloud Function (request en v2)
 * @returns {boolean} true si el usuario es admin
 * 
 * @example
 * if (isAdmin(request)) {
 *   // Ejecutar lógica de admin
 * }
 */
function isAdmin(context) {
  // Verificar que hay autenticación
  if (!context.auth) {
    return false;
  }

  const { token, uid } = context.auth;

  // ✅ Método principal: Verificar Custom Claims
  // El claim 'admin' se configura via Firebase Admin SDK
  if (token && token.admin === true) {
    return true;
  }

  // También verificar el claim 'role' por consistencia
  if (token && token.role === 'admin') {
    return true;
  }

  // ⚠️ Fallback: Verificar por UID (solo durante migración)
  if (ENABLE_UID_FALLBACK && ADMIN_UIDS.includes(uid)) {
    console.warn(
      `[Authorization] Admin verificado por UID fallback: ${uid}. ` +
      'Considera ejecutar set-admin-claims.ts para configurar Custom Claims.'
    );
    return true;
  }

  return false;
}

/**
 * Obtiene el rol del usuario desde Custom Claims.
 * 
 * @param {Object} context - Contexto de la Cloud Function
 * @returns {'admin' | 'user'} Rol del usuario
 * 
 * @example
 * const role = getUserRole(request);
 * console.log(`User role: ${role}`);
 */
function getUserRole(context) {
  if (!context.auth) {
    return 'user';
  }

  const { token, uid } = context.auth;

  // Verificar claim 'role' directamente
  if (token && token.role) {
    return token.role;
  }

  // Verificar claim 'admin' boolean
  if (token && token.admin === true) {
    return 'admin';
  }

  // Fallback por UID
  if (ENABLE_UID_FALLBACK && ADMIN_UIDS.includes(uid)) {
    return 'admin';
  }

  return 'user';
}

// ============================================================================
// MIDDLEWARE DE AUTORIZACIÓN
// ============================================================================

/**
 * Middleware que requiere autenticación.
 * Lanza HttpsError si el usuario no está autenticado.
 * 
 * @param {Object} context - Contexto de la Cloud Function
 * @throws {HttpsError} code 'unauthenticated' si no hay usuario
 * @returns {boolean} true si está autenticado
 * 
 * @example
 * async function myHandler(request) {
 *   requireAuth(request);
 *   // Usuario está autenticado, continuar...
 * }
 */
function requireAuth(context) {
  if (!context.auth) {
    throw new HttpsError(
      'unauthenticated',
      'Autenticación requerida para esta operación'
    );
  }
  return true;
}

/**
 * Middleware que requiere rol de administrador.
 * Lanza HttpsError si el usuario no es admin.
 * 
 * ✅ Zero-cost verification: Claims vienen en el token
 * 
 * @param {Object} context - Contexto de la Cloud Function
 * @throws {HttpsError} code 'unauthenticated' si no hay usuario
 * @throws {HttpsError} code 'permission-denied' si no es admin
 * @returns {boolean} true si es admin
 * 
 * @example
 * async function addCurrency(context, payload) {
 *   requireAdmin(context);
 *   // Solo admins llegan aquí...
 * }
 */
function requireAdmin(context) {
  // Primero verificar autenticación
  requireAuth(context);

  // Luego verificar rol de admin
  if (!isAdmin(context)) {
    console.warn(
      `[Authorization] Acceso denegado para usuario: ${context.auth.uid}. ` +
      'Se requiere rol de administrador.'
    );
    
    throw new HttpsError(
      'permission-denied',
      'Se requiere rol de administrador para esta operación'
    );
  }

  return true;
}

/**
 * Middleware que verifica si el usuario es el propietario del recurso.
 * Útil para operaciones que solo el propietario puede realizar.
 * 
 * @param {Object} context - Contexto de la Cloud Function
 * @param {string} resourceOwnerId - UID del propietario del recurso
 * @throws {HttpsError} code 'permission-denied' si no es el propietario
 * @returns {boolean} true si es el propietario
 * 
 * @example
 * async function updateUserProfile(context, payload) {
 *   requireOwnership(context, payload.userId);
 *   // Solo el propietario llega aquí...
 * }
 */
function requireOwnership(context, resourceOwnerId) {
  requireAuth(context);

  if (context.auth.uid !== resourceOwnerId) {
    // Los admins pueden acceder a cualquier recurso
    if (isAdmin(context)) {
      console.log(
        `[Authorization] Admin ${context.auth.uid} accediendo a recurso de ${resourceOwnerId}`
      );
      return true;
    }

    throw new HttpsError(
      'permission-denied',
      'No tienes permiso para acceder a este recurso'
    );
  }

  return true;
}

/**
 * Middleware genérico que verifica un rol específico.
 * 
 * @param {Object} context - Contexto de la Cloud Function
 * @param {'admin' | 'user'} requiredRole - Rol requerido
 * @throws {HttpsError} Si no tiene el rol requerido
 * @returns {boolean} true si tiene el rol
 * 
 * @example
 * requireRole(context, 'admin');
 */
function requireRole(context, requiredRole) {
  requireAuth(context);

  if (requiredRole === 'admin') {
    return requireAdmin(context);
  }

  // Para rol 'user', cualquier usuario autenticado califica
  return true;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Verificación
  isAdmin,
  getUserRole,
  
  // Middleware
  requireAuth,
  requireAdmin,
  requireOwnership,
  requireRole,
  
  // Configuración (para testing)
  ADMIN_UIDS,
  ENABLE_UID_FALLBACK,
};
