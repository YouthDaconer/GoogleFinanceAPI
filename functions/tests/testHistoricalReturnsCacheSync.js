/**
 * Test para OPT-010: Sincronización de Cache de Rendimientos Históricos
 * 
 * Pruebas manuales para:
 * - calculateDynamicTTL(): TTL basado en horario de mercado
 * - invalidatePerformanceCacheBatch(): Invalidación batch de caches
 * 
 * Uso: node tests/testHistoricalReturnsCacheSync.js
 */

require('dotenv').config();
const admin = require('../services/firebaseAdmin');
const db = admin.firestore();
const { DateTime } = require('luxon');

const { 
  calculateDynamicTTL, 
  invalidatePerformanceCacheBatch 
} = require('../services/historicalReturnsService');

// ============================================================================
// CONFIGURACIÓN DE TESTS
// ============================================================================

const TEST_USER_IDS = []; // Se llena dinámicamente

// ============================================================================
// FUNCIONES DE TEST
// ============================================================================

/**
 * Test: calculateDynamicTTL con diferentes horarios simulados
 */
async function testCalculateDynamicTTL() {
  console.log("\n📊 TEST: calculateDynamicTTL");
  console.log("=".repeat(60));
  
  // No podemos simular la hora fácilmente, pero verificamos el comportamiento actual
  const now = DateTime.now().setZone('America/New_York');
  const ttl = calculateDynamicTTL();
  const ttlDate = DateTime.fromJSDate(ttl).setZone('America/New_York');
  
  console.log(`\n📅 Hora actual (NY): ${now.toFormat('yyyy-MM-dd HH:mm:ss EEEE')}`);
  console.log(`📅 TTL calculado: ${ttlDate.toFormat('yyyy-MM-dd HH:mm:ss EEEE')}`);
  
  const diffMinutes = ttlDate.diff(now, 'minutes').minutes;
  console.log(`⏱️ Diferencia: ${diffMinutes.toFixed(1)} minutos`);
  
  // Validaciones según el horario actual
  const hour = now.hour + now.minute / 60;
  const MARKET_OPEN = 9.5;
  const MARKET_CLOSE = 16;
  
  if (now.weekday === 6 || now.weekday === 7) {
    console.log(`\n✅ Es fin de semana - TTL debería ser hasta lunes 9:30 AM`);
    const expectedDay = now.weekday === 6 ? 'Monday' : 'Monday';
    console.log(`   TTL día: ${ttlDate.toFormat('EEEE')} (esperado: ${expectedDay})`);
    console.log(`   TTL hora: ${ttlDate.toFormat('HH:mm')} (esperado: 09:30)`);
  } else if (hour >= MARKET_OPEN && hour < MARKET_CLOSE) {
    console.log(`\n✅ Mercado abierto - TTL debería ser ~5 minutos`);
    console.log(`   Diferencia: ${diffMinutes.toFixed(1)} min (esperado: ~5 min)`);
    if (diffMinutes >= 4 && diffMinutes <= 6) {
      console.log(`   ✅ CORRECTO`);
    } else {
      console.log(`   ⚠️ INESPERADO`);
    }
  } else if (hour >= MARKET_CLOSE) {
    console.log(`\n✅ Mercado cerrado (después de las 4PM) - TTL hasta mañana 9:30 AM`);
    console.log(`   TTL hora: ${ttlDate.toFormat('HH:mm')} (esperado: 09:30)`);
  } else {
    console.log(`\n✅ Antes de apertura - TTL hasta hoy 9:30 AM`);
    console.log(`   TTL hora: ${ttlDate.toFormat('HH:mm')} (esperado: 09:30)`);
  }
}

/**
 * Test: Obtener usuarios con cache existente
 */
async function getTestUserIds() {
  console.log("\n🔍 Buscando usuarios con cache de performance...");
  
  const userDataSnapshot = await db.collection('userData').limit(10).get();
  const usersWithCache = [];
  
  for (const userDoc of userDataSnapshot.docs) {
    const cacheSnapshot = await db.collection(`userData/${userDoc.id}/performanceCache`).limit(1).get();
    if (!cacheSnapshot.empty) {
      usersWithCache.push(userDoc.id);
      console.log(`   ✅ Usuario con cache: ${userDoc.id}`);
    }
  }
  
  if (usersWithCache.length === 0) {
    console.log("   ⚠️ No hay usuarios con cache de performance");
  }
  
  return usersWithCache;
}

/**
 * Test: invalidatePerformanceCacheBatch
 */
async function testInvalidatePerformanceCacheBatch() {
  console.log("\n🗑️ TEST: invalidatePerformanceCacheBatch");
  console.log("=".repeat(60));
  
  const userIds = await getTestUserIds();
  
  if (userIds.length === 0) {
    console.log("\n⚠️ No hay usuarios con cache para probar invalidación");
    console.log("   Ejecuta getHistoricalReturns primero para crear caches");
    return;
  }
  
  console.log(`\n📋 Usuarios a procesar: ${userIds.length}`);
  
  // Verificar caches antes
  let totalCachesBefore = 0;
  for (const userId of userIds) {
    const snapshot = await db.collection(`userData/${userId}/performanceCache`).get();
    totalCachesBefore += snapshot.size;
  }
  console.log(`📊 Caches antes: ${totalCachesBefore}`);
  
  // Ejecutar invalidación
  console.log("\n🔄 Ejecutando invalidación...");
  const startTime = Date.now();
  const result = await invalidatePerformanceCacheBatch(userIds);
  const duration = Date.now() - startTime;
  
  console.log(`\n📊 Resultado:`);
  console.log(`   - Usuarios procesados: ${result.usersProcessed}`);
  console.log(`   - Caches eliminados: ${result.cachesDeleted}`);
  console.log(`   - Tiempo: ${duration}ms`);
  
  if (duration < 500) {
    console.log(`   ✅ Performance OK (<500ms)`);
  } else {
    console.log(`   ⚠️ Performance LENTA (>500ms)`);
  }
  
  // Verificar caches después
  let totalCachesAfter = 0;
  for (const userId of userIds) {
    const snapshot = await db.collection(`userData/${userId}/performanceCache`).get();
    totalCachesAfter += snapshot.size;
  }
  console.log(`\n📊 Caches después: ${totalCachesAfter}`);
  
  if (totalCachesAfter === 0) {
    console.log(`   ✅ Todos los caches eliminados correctamente`);
  } else {
    console.log(`   ⚠️ Quedan ${totalCachesAfter} caches sin eliminar`);
  }
}

/**
 * Test: Performance bajo carga simulada
 */
async function testPerformanceUnder500ms() {
  console.log("\n⚡ TEST: Performance de Invalidación (<500ms)");
  console.log("=".repeat(60));
  
  // Simular array de userIds (aunque no tengan cache)
  const simulatedUserIds = [];
  for (let i = 0; i < 20; i++) {
    simulatedUserIds.push(`test_user_${i}`);
  }
  
  console.log(`\n📋 Simulando invalidación para ${simulatedUserIds.length} usuarios`);
  
  const iterations = 5;
  const times = [];
  
  for (let i = 0; i < iterations; i++) {
    const startTime = Date.now();
    await invalidatePerformanceCacheBatch(simulatedUserIds);
    const duration = Date.now() - startTime;
    times.push(duration);
    console.log(`   Iteración ${i + 1}: ${duration}ms`);
  }
  
  const avgTime = times.reduce((a, b) => a + b, 0) / times.length;
  const maxTime = Math.max(...times);
  
  console.log(`\n📊 Estadísticas:`);
  console.log(`   - Promedio: ${avgTime.toFixed(1)}ms`);
  console.log(`   - Máximo: ${maxTime}ms`);
  
  if (maxTime < 500) {
    console.log(`   ✅ Performance OK - Todas las iteraciones <500ms`);
  } else {
    console.log(`   ⚠️ Performance LENTA - Algunas iteraciones >500ms`);
  }
}

/**
 * Ejecutar todos los tests
 */
async function runAllTests() {
  console.log("\n" + "=".repeat(60));
  console.log("🧪 OPT-010: TESTS DE SINCRONIZACIÓN DE CACHE");
  console.log("=".repeat(60));
  console.log(`📅 Fecha: ${new Date().toISOString()}`);
  
  try {
    await testCalculateDynamicTTL();
    await testInvalidatePerformanceCacheBatch();
    await testPerformanceUnder500ms();
    
    console.log("\n" + "=".repeat(60));
    console.log("✅ TODOS LOS TESTS COMPLETADOS");
    console.log("=".repeat(60));
    
  } catch (error) {
    console.error("\n❌ Error en tests:", error);
  }
  
  process.exit(0);
}

// Ejecutar tests
runAllTests();
