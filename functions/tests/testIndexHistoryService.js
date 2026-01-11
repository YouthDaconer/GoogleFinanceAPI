/**
 * Test para indexHistoryService - OPT-009
 * 
 * Pruebas manuales para la Cloud Function getIndexHistory.
 * 
 * Uso: node tests/testIndexHistoryService.js
 */

require('dotenv').config();
const admin = require('../services/firebaseAdmin');
const db = admin.firestore();

// Importar la función auxiliar (no la Cloud Function directamente)
const { calculateIndexData, invalidateIndexCache, invalidateAllIndexCaches } = require('../services/indexHistoryService');

// ============================================================================
// CONFIGURACIÓN DE TESTS
// ============================================================================

const TEST_INDEX_CODE = "GSPC"; // S&P 500
const TEST_RANGES = ["1M", "3M", "6M", "YTD", "1Y", "5Y"];

// ============================================================================
// FUNCIONES DE TEST
// ============================================================================

async function testCalculateIndexData() {
  console.log("\n📊 TEST: calculateIndexData");
  console.log("=".repeat(50));

  for (const range of TEST_RANGES) {
    try {
      console.log(`\n📈 Probando ${TEST_INDEX_CODE}/${range}...`);
      const startTime = Date.now();
      
      const result = await calculateIndexData(TEST_INDEX_CODE, range);
      
      const duration = Date.now() - startTime;
      
      console.log(`  ✅ Éxito en ${duration}ms`);
      console.log(`  📊 Puntos de datos: ${result.chartData.length}`);
      console.log(`  📈 Cambio general: ${result.overallChange.toFixed(2)}%`);
      console.log(`  💰 Último valor: ${result.latestValue.toFixed(2)}`);
      console.log(`  🏷️ Nombre: ${result.indexInfo.name}`);
      
      if (result.chartData.length > 0) {
        console.log(`  📅 Rango: ${result.chartData[0].date} → ${result.chartData[result.chartData.length - 1].date}`);
      }
      
    } catch (error) {
      console.log(`  ❌ Error: ${error.message}`);
    }
  }
}

async function testCacheOperations() {
  console.log("\n🗄️ TEST: Operaciones de Cache");
  console.log("=".repeat(50));

  const cacheKey = `${TEST_INDEX_CODE}_1M`;
  const cacheRef = db.collection("indexCache").doc(cacheKey);

  try {
    // 1. Crear cache de prueba
    console.log(`\n📝 Creando cache para ${cacheKey}...`);
    const testData = await calculateIndexData(TEST_INDEX_CODE, "1M");
    
    await cacheRef.set({
      ...testData,
      lastUpdated: Date.now(),
    });
    console.log("  ✅ Cache creado");

    // 2. Verificar que existe
    const cacheDoc = await cacheRef.get();
    if (cacheDoc.exists) {
      const data = cacheDoc.data();
      console.log(`  ✅ Cache verificado - ${data.chartData.length} puntos de datos`);
      console.log(`  📅 Última actualización: ${new Date(data.lastUpdated).toISOString()}`);
    } else {
      console.log("  ❌ Cache no encontrado");
    }

    // 3. Probar invalidación individual
    console.log(`\n🗑️ Invalidando cache para ${TEST_INDEX_CODE}...`);
    await invalidateIndexCache(TEST_INDEX_CODE);
    
    const afterInvalidate = await cacheRef.get();
    if (!afterInvalidate.exists) {
      console.log("  ✅ Cache invalidado correctamente");
    } else {
      console.log("  ❌ Cache no fue invalidado");
    }

  } catch (error) {
    console.log(`  ❌ Error: ${error.message}`);
  }
}

async function testListAvailableIndices() {
  console.log("\n📋 TEST: Listar Índices Disponibles");
  console.log("=".repeat(50));

  try {
    const snapshot = await db.collection("indexHistories").get();
    
    console.log(`\n📊 Total de índices: ${snapshot.size}`);
    console.log("\n| Código | Nombre | Región |");
    console.log("|--------|--------|--------|");
    
    snapshot.forEach(doc => {
      const data = doc.data();
      console.log(`| ${doc.id.padEnd(6)} | ${(data.name || "N/A").substring(0, 25).padEnd(25)} | ${(data.region || "N/A").padEnd(6)} |`);
    });

  } catch (error) {
    console.log(`  ❌ Error: ${error.message}`);
  }
}

async function testPerformanceComparison() {
  console.log("\n⚡ TEST: Comparación de Rendimiento");
  console.log("=".repeat(50));

  const range = "1Y";
  const code = TEST_INDEX_CODE;

  try {
    // 1. Medir tiempo de consulta directa (simulada)
    console.log("\n📊 Midiendo tiempo de cálculo directo...");
    const directStart = Date.now();
    await calculateIndexData(code, range);
    const directTime = Date.now() - directStart;
    console.log(`  ⏱️ Tiempo de cálculo: ${directTime}ms`);

    // 2. Crear cache
    console.log("\n📝 Creando cache...");
    const cacheKey = `${code}_${range}`;
    const cacheRef = db.collection("indexCache").doc(cacheKey);
    
    const data = await calculateIndexData(code, range);
    await cacheRef.set({
      ...data,
      lastUpdated: Date.now(),
    });

    // 3. Medir tiempo de lectura de cache
    console.log("\n🗄️ Midiendo tiempo de lectura de cache...");
    const cacheStart = Date.now();
    const cacheDoc = await cacheRef.get();
    const cacheTime = Date.now() - cacheStart;
    console.log(`  ⏱️ Tiempo de cache: ${cacheTime}ms`);

    // 4. Comparar
    const improvement = ((directTime - cacheTime) / directTime * 100).toFixed(1);
    console.log(`\n📈 Mejora de rendimiento: ${improvement}%`);
    console.log(`  🚀 Cache es ${(directTime / cacheTime).toFixed(1)}x más rápido`);

    // 5. Limpiar
    await cacheRef.delete();

  } catch (error) {
    console.log(`  ❌ Error: ${error.message}`);
  }
}

async function testAllRangesForIndex() {
  console.log("\n🔄 TEST: Todos los Rangos para un Índice");
  console.log("=".repeat(50));

  const allRanges = ["1M", "3M", "6M", "YTD", "1Y", "5Y", "MAX"];
  const results = [];

  for (const range of allRanges) {
    try {
      const startTime = Date.now();
      const data = await calculateIndexData(TEST_INDEX_CODE, range);
      const duration = Date.now() - startTime;
      
      results.push({
        range,
        points: data.chartData.length,
        change: data.overallChange,
        duration,
        status: "✅"
      });
    } catch (error) {
      results.push({
        range,
        points: 0,
        change: 0,
        duration: 0,
        status: "❌ " + error.message.substring(0, 30)
      });
    }
  }

  console.log("\n| Rango | Puntos | Cambio % | Tiempo | Estado |");
  console.log("|-------|--------|----------|--------|--------|");
  results.forEach(r => {
    console.log(`| ${r.range.padEnd(5)} | ${String(r.points).padEnd(6)} | ${r.change.toFixed(2).padStart(8)} | ${String(r.duration + "ms").padEnd(6)} | ${r.status} |`);
  });
}

// ============================================================================
// EJECUTAR TESTS
// ============================================================================

async function runAllTests() {
  console.log("\n" + "=".repeat(60));
  console.log("🧪 TESTS: indexHistoryService (OPT-009)");
  console.log("=".repeat(60));
  console.log(`📅 Fecha: ${new Date().toISOString()}`);
  console.log(`📊 Índice de prueba: ${TEST_INDEX_CODE}`);

  await testListAvailableIndices();
  await testCalculateIndexData();
  await testAllRangesForIndex();
  await testCacheOperations();
  await testPerformanceComparison();

  console.log("\n" + "=".repeat(60));
  console.log("✅ Tests completados");
  console.log("=".repeat(60) + "\n");

  process.exit(0);
}

// Ejecutar
runAllTests().catch(error => {
  console.error("❌ Error fatal:", error);
  process.exit(1);
});
