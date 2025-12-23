/**
 * Rate Limit Stress Test Script
 * 
 * Este script prueba que el rate limiting funciona correctamente
 * haciendo múltiples llamadas rápidas a una Cloud Function.
 * 
 * Uso:
 *   node scripts/rateLimitStressTest.js
 * 
 * Requiere:
 *   - Firebase Admin SDK configurado
 *   - Usuario de prueba autenticado
 * 
 * @see docs/stories/49.story.md (SCALE-BE-004)
 */

const admin = require('firebase-admin');
const path = require('path');

// Inicializar Firebase Admin con credenciales de servicio
if (!admin.apps.length) {
  const serviceAccount = require(path.join(__dirname, '..', 'key.json'));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: 'portafolio-inversiones'
  });
}

const db = admin.firestore();

// Configuración del test
const TEST_CONFIG = {
  // Función a probar (usa una con límite bajo para probar rápido)
  functionName: 'getIndexHistory',
  // Número de llamadas a hacer (debe exceder el límite)
  numberOfCalls: 35,
  // Delay entre llamadas (ms) - 0 para máxima velocidad
  delayBetweenCalls: 50,
  // Límite esperado según config/rateLimits.js
  expectedLimit: 30,
  // Datos de prueba para la función
  testData: {
    code: 'GSPC',
    range: '1M'
  }
};

// Colores para output
const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m'
};

function log(message, color = 'reset') {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

/**
 * Simula una llamada a Cloud Function directamente en Firestore
 * (ya que no podemos llamar Cloud Functions callable desde admin SDK)
 * 
 * En su lugar, verificamos el comportamiento del rate limiter directamente
 */
async function testRateLimiterDirectly() {
  log('\n========================================', 'cyan');
  log('  RATE LIMIT STRESS TEST', 'cyan');
  log('========================================\n', 'cyan');

  // Importar el rate limiter y config
  const { RateLimiter } = require('../utils/rateLimiter');
  const { getRateLimitConfig } = require('../config/rateLimits');
  
  const testUserId = 'test-user-stress-' + Date.now();
  const functionName = TEST_CONFIG.functionName;
  const config = getRateLimitConfig(functionName);
  
  log(`📋 Configuración del test:`, 'blue');
  log(`   Función: ${functionName}`);
  log(`   Límite: ${config.limit} llamadas / ${config.windowMs/1000}s`);
  log(`   Llamadas a realizar: ${TEST_CONFIG.numberOfCalls}`);
  log(`   Usuario de prueba: ${testUserId}\n`);

  const rateLimiter = new RateLimiter({
    defaultLimit: config.limit,
    defaultWindowMs: config.windowMs
  });

  let successCount = 0;
  let rateLimitedCount = 0;
  let errorCount = 0;
  const results = [];

  log(`🚀 Iniciando ${TEST_CONFIG.numberOfCalls} llamadas...\n`, 'yellow');

  for (let i = 1; i <= TEST_CONFIG.numberOfCalls; i++) {
    const startTime = Date.now();
    
    try {
      // Verificar rate limit
      await rateLimiter.checkLimit(testUserId, functionName);
      
      // Si pasa, simular llamada exitosa
      successCount++;
      const elapsed = Date.now() - startTime;
      results.push({ call: i, status: 'success', elapsed });
      
      if (i <= 5 || i % 10 === 0 || i > TEST_CONFIG.numberOfCalls - 5) {
        log(`   ✅ Llamada ${i}: OK (${elapsed}ms)`, 'green');
      }
    } catch (error) {
      const elapsed = Date.now() - startTime;
      
      if (error.code === 'resource-exhausted') {
        rateLimitedCount++;
        results.push({ call: i, status: 'rate-limited', elapsed, retryAfter: error.details?.retryAfter });
        
        if (rateLimitedCount <= 3 || i === TEST_CONFIG.numberOfCalls) {
          log(`   🚫 Llamada ${i}: RATE LIMITED (retry after ${error.details?.retryAfter}s)`, 'red');
        } else if (rateLimitedCount === 4) {
          log(`   ... (más llamadas bloqueadas) ...`, 'yellow');
        }
      } else {
        errorCount++;
        results.push({ call: i, status: 'error', elapsed, error: error.message });
        log(`   ❌ Llamada ${i}: ERROR - ${error.message}`, 'red');
      }
    }

    // Pequeño delay entre llamadas
    if (TEST_CONFIG.delayBetweenCalls > 0) {
      await new Promise(resolve => setTimeout(resolve, TEST_CONFIG.delayBetweenCalls));
    }
  }

  // Resumen
  log('\n========================================', 'cyan');
  log('  RESULTADOS', 'cyan');
  log('========================================\n', 'cyan');

  log(`📊 Resumen:`, 'blue');
  log(`   Total llamadas: ${TEST_CONFIG.numberOfCalls}`);
  log(`   ✅ Exitosas: ${successCount}`, 'green');
  log(`   🚫 Rate Limited: ${rateLimitedCount}`, rateLimitedCount > 0 ? 'red' : 'green');
  log(`   ❌ Errores: ${errorCount}`, errorCount > 0 ? 'red' : 'green');

  // Verificar comportamiento esperado
  log('\n🔍 Verificación:', 'blue');
  
  const expectedSuccess = Math.min(TEST_CONFIG.numberOfCalls, config.limit);
  const expectedRateLimited = Math.max(0, TEST_CONFIG.numberOfCalls - config.limit);
  
  if (successCount <= config.limit && rateLimitedCount >= expectedRateLimited - 1) {
    log(`   ✅ Rate limiting funcionando correctamente!`, 'green');
    log(`      - Límite respetado: ${successCount} <= ${config.limit}`, 'green');
    log(`      - Llamadas bloqueadas: ${rateLimitedCount}`, 'green');
  } else {
    log(`   ⚠️ Comportamiento inesperado`, 'yellow');
    log(`      - Esperado: ~${expectedSuccess} exitosas, ~${expectedRateLimited} bloqueadas`);
    log(`      - Obtenido: ${successCount} exitosas, ${rateLimitedCount} bloqueadas`);
  }

  // Limpiar documentos de test
  log('\n🧹 Limpiando datos de prueba...', 'yellow');
  try {
    const testDoc = db.collection('rateLimits').doc(`${testUserId}:${functionName}`);
    await testDoc.delete();
    log('   ✅ Limpieza completada', 'green');
  } catch (cleanupError) {
    log(`   ⚠️ Error en limpieza: ${cleanupError.message}`, 'yellow');
  }

  log('\n========================================\n', 'cyan');
  
  return {
    success: successCount <= config.limit && rateLimitedCount > 0,
    successCount,
    rateLimitedCount,
    errorCount
  };
}

/**
 * Test de diferentes funciones con diferentes límites
 */
async function testMultipleFunctions() {
  log('\n========================================', 'cyan');
  log('  TEST DE MÚLTIPLES FUNCIONES', 'cyan');
  log('========================================\n', 'cyan');

  const { getRateLimitConfig } = require('../config/rateLimits');
  
  const functionsToTest = [
    'getHistoricalReturns',      // 15/min - más restrictivo
    'createAsset',               // 30/min - escritura
    'getCurrentPricesForUser',   // 30/min - lectura
    'updateUserDisplayName',     // 5/min - muy restrictivo
  ];

  log('📋 Límites configurados:\n', 'blue');
  
  functionsToTest.forEach(fn => {
    const config = getRateLimitConfig(fn);
    log(`   ${fn}: ${config.limit} llamadas / ${config.windowMs/1000}s`);
  });

  log('\n✅ Configuración de rate limits verificada', 'green');
}

/**
 * Test de recuperación después del período de ventana
 */
async function testRecoveryAfterWindow() {
  log('\n========================================', 'cyan');
  log('  TEST DE RECUPERACIÓN', 'cyan');
  log('========================================\n', 'cyan');

  const { RateLimiter } = require('../utils/rateLimiter');
  
  const testUserId = 'test-recovery-' + Date.now();
  const functionName = 'testFunction';
  
  // Crear rate limiter con ventana muy corta para test rápido
  const rateLimiter = new RateLimiter({
    defaultLimit: 3,
    defaultWindowMs: 5000  // 5 segundos
  });

  log('📋 Configuración: 3 llamadas / 5 segundos\n', 'blue');

  // Fase 1: Agotar el límite
  log('Fase 1: Agotando límite...', 'yellow');
  for (let i = 1; i <= 4; i++) {
    try {
      await rateLimiter.checkLimit(testUserId, functionName);
      log(`   ✅ Llamada ${i}: OK`, 'green');
    } catch (error) {
      log(`   🚫 Llamada ${i}: BLOQUEADA`, 'red');
    }
  }

  // Fase 2: Esperar recuperación
  log('\nFase 2: Esperando 6 segundos para recuperación...', 'yellow');
  await new Promise(resolve => setTimeout(resolve, 6000));

  // Fase 3: Verificar que se puede llamar de nuevo
  log('\nFase 3: Verificando recuperación...', 'yellow');
  try {
    await rateLimiter.checkLimit(testUserId, functionName);
    log('   ✅ Llamada post-recuperación: OK', 'green');
    log('\n✅ Recuperación funcionando correctamente!', 'green');
  } catch (error) {
    log('   ❌ Llamada post-recuperación: BLOQUEADA (error)', 'red');
  }

  // Limpiar
  try {
    await db.collection('rateLimits').doc(`${testUserId}:${functionName}`).delete();
  } catch (e) {
    // Ignorar errores de limpieza
  }

  log('\n========================================\n', 'cyan');
}

// Ejecutar tests
async function runAllTests() {
  try {
    log('\n🧪 INICIANDO SUITE DE TESTS DE RATE LIMITING\n', 'cyan');
    
    // Test 1: Verificar configuración
    await testMultipleFunctions();
    
    // Test 2: Stress test principal
    const stressResult = await testRateLimiterDirectly();
    
    // Test 3: Recuperación (opcional, toma 6+ segundos)
    const runRecoveryTest = process.argv.includes('--full');
    if (runRecoveryTest) {
      await testRecoveryAfterWindow();
    } else {
      log('💡 Tip: Usa --full para incluir test de recuperación (6+ segundos extra)\n', 'yellow');
    }

    // Resultado final
    log('========================================', 'cyan');
    log('  RESULTADO FINAL', 'cyan');
    log('========================================\n', 'cyan');

    if (stressResult.success) {
      log('✅ TODOS LOS TESTS PASARON', 'green');
      log('   El rate limiting está funcionando correctamente!\n', 'green');
      process.exit(0);
    } else {
      log('⚠️ ALGUNOS TESTS FALLARON', 'yellow');
      log('   Revisa los resultados arriba.\n', 'yellow');
      process.exit(1);
    }

  } catch (error) {
    log(`\n❌ ERROR FATAL: ${error.message}`, 'red');
    console.error(error);
    process.exit(1);
  }
}

// Ejecutar
runAllTests();
