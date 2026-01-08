/**
 * Script para configurar Custom Claims de administrador en Firebase Auth
 * 
 * RBAC-001: Implementación de sistema de roles basado en Custom Claims
 * 
 * Uso:
 *   node scripts/set-admin-claims.js
 * 
 * @module scripts/set-admin-claims
 * @see docs/architecture/role-based-access-control-design.md
 */

const admin = require('firebase-admin');
const { getAuth } = require('firebase-admin/auth');
const path = require('path');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

/**
 * UIDs de los usuarios administradores.
 */
const ADMIN_UIDS = ['DDeR8P5hYgfuN8gcU4RsQfdTJqx2'];

/**
 * Custom Claims que se asignarán a los administradores
 */
const ADMIN_CLAIMS = {
  admin: true,
  role: 'admin',
};

// ============================================================================
// INICIALIZACIÓN
// ============================================================================

/**
 * Inicializa Firebase Admin SDK
 */
function initializeFirebaseAdmin() {
  // Intentar cargar service account key desde diferentes ubicaciones
  const possiblePaths = [
    path.join(process.cwd(), 'key.json'),
    path.join(process.cwd(), 'serviceAccountKey.json'),
    path.join(__dirname, '../key.json'),
  ];

  let serviceAccountPath = null;

  for (const p of possiblePaths) {
    try {
      require(p);
      serviceAccountPath = p;
      break;
    } catch {
      // Continuar buscando
    }
  }

  if (serviceAccountPath) {
    console.log(`📁 Usando service account: ${serviceAccountPath}`);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccountPath),
    });
  } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    console.log('📁 Usando GOOGLE_APPLICATION_CREDENTIALS');
    admin.initializeApp();
  } else {
    throw new Error(
      'No se encontró el archivo key.json o serviceAccountKey.json.\n' +
      'Por favor, verifica que existe en el directorio de functions.'
    );
  }
}

// ============================================================================
// FUNCIONES PRINCIPALES
// ============================================================================

/**
 * Asigna Custom Claims de administrador a los UIDs configurados
 */
async function setAdminClaims() {
  const auth = getAuth();

  console.log('\n🔐 Configurando Custom Claims de Administrador\n');
  console.log(`   UIDs a configurar: ${ADMIN_UIDS.length}`);
  console.log('');

  let successCount = 0;
  let errorCount = 0;

  for (const uid of ADMIN_UIDS) {
    try {
      // Verificar que el usuario existe
      const user = await auth.getUser(uid);
      
      console.log(`📧 Usuario: ${user.email || 'Sin email'}`);
      console.log(`   UID: ${uid}`);
      
      // Obtener claims actuales
      const currentClaims = user.customClaims || {};
      console.log(`   Claims actuales: ${JSON.stringify(currentClaims)}`);

      // Verificar si ya es admin
      if (currentClaims.admin === true && currentClaims.role === 'admin') {
        console.log('   ✓ Ya tiene claims de admin configurados');
        successCount++;
        continue;
      }

      // Asignar Custom Claims de admin
      await auth.setCustomUserClaims(uid, ADMIN_CLAIMS);

      // Verificar que se aplicaron correctamente
      const updatedUser = await auth.getUser(uid);
      const newClaims = updatedUser.customClaims;
      
      console.log(`   Nuevos claims: ${JSON.stringify(newClaims)}`);
      console.log('   ✅ Claims de admin configurados exitosamente');
      
      successCount++;
    } catch (error) {
      console.error(`   ❌ Error configurando claims para ${uid}:`);
      console.error(`      ${error.message}`);
      errorCount++;
    }

    console.log('');
  }

  // Resumen
  console.log('═'.repeat(50));
  console.log('\n📊 Resumen:\n');
  console.log(`   ✅ Exitosos: ${successCount}`);
  console.log(`   ❌ Errores: ${errorCount}`);
  console.log('');

  if (successCount > 0) {
    console.log('⚠️  IMPORTANTE:');
    console.log('   Los usuarios deben cerrar sesión y volver a iniciar');
    console.log('   para que los nuevos claims tomen efecto.');
    console.log('');
  }
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  console.log('\n');
  console.log('═'.repeat(50));
  console.log('  RBAC Setup - Custom Claims Configuration');
  console.log('═'.repeat(50));
  
  try {
    initializeFirebaseAdmin();
    await setAdminClaims();
    
    console.log('🎉 Configuración completada!\n');
    process.exit(0);
  } catch (error) {
    console.error('\n❌ Error fatal:', error.message);
    console.error('');
    process.exit(1);
  }
}

// Ejecutar
main();
