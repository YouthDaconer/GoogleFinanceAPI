/**
 * Auth Triggers - Cloud Functions que responden a eventos de Firebase Auth
 * 
 * RBAC-001: Trigger para asignar Custom Claims a nuevos usuarios
 * 
 * Firebase Best Practice: Usar Custom Claims para roles
 * @see https://firebase.google.com/docs/auth/admin/custom-claims
 * @see docs/architecture/role-based-access-control-design.md
 * 
 * @module triggers/authTriggers
 */

const { beforeUserCreated } = require('firebase-functions/v2/identity');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/**
 * Claims por defecto para nuevos usuarios
 */
const DEFAULT_USER_CLAIMS = {
  admin: false,
  role: 'user',
};

// ============================================================================
// TRIGGER: BEFORE USER CREATED (Firebase v2)
// ============================================================================

/**
 * Trigger que se ejecuta cuando se crea un nuevo usuario en Firebase Auth.
 * 
 * Asigna automáticamente Custom Claims con role: 'user' para que el sistema
 * RBAC funcione correctamente desde el primer momento.
 * 
 * ✅ Firebase Best Practice: 
 *    - Asignar claims inmediatamente al crear usuario
 *    - Usar Admin SDK para modificar claims (no se puede desde cliente)
 * 
 * @example
 * // El trigger se activa automáticamente cuando:
 * // - Usuario se registra con email/password
 * // - Usuario hace sign-in con Google por primera vez
 * // - Usuario se crea via Admin SDK
 */
const onUserCreate = beforeUserCreated(async (event) => {
  const user = event.data;
  const startTime = Date.now();
  
  console.log(`[onUserCreate] New user created: ${user.uid}`);
  console.log(`[onUserCreate] Email: ${user.email || 'No email'}`);

  try {
    // En beforeUserCreated, podemos retornar customClaims directamente
    const duration = Date.now() - startTime;
    console.log(`[onUserCreate] Setting default claims: ${JSON.stringify(DEFAULT_USER_CLAIMS)}`);
    console.log(`[onUserCreate] Duration: ${duration}ms`);

    // Retornar los claims que se asignarán al usuario
    return {
      customClaims: DEFAULT_USER_CLAIMS,
    };

  } catch (error) {
    // No lanzar error para no bloquear el registro del usuario
    console.error(`[onUserCreate] Error setting claims:`, {
      code: error.code,
      message: error.message,
    });
    
    // Retornar sin claims en caso de error (el usuario se crea igual)
    return {};
  }
});

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  onUserCreate,
};