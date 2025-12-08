/**
 * Tests para las funciones de cálculo de MWR
 * 
 * Ejecutar: node scripts/diagnostics/testMWRCalculations.js
 */

const { 
  daysBetween,
  calculateSimplePersonalReturn,
  calculateModifiedDietzReturn,
  calculateAllPersonalReturns
} = require('../../utils/mwrCalculations');

console.log('='.repeat(100));
console.log('TESTS DE MWR CALCULATIONS');
console.log('='.repeat(100));
console.log();

let passed = 0;
let failed = 0;

function test(name, actual, expected, tolerance = 0.01) {
  const diff = Math.abs(actual - expected);
  if (diff <= tolerance) {
    console.log(`✅ ${name}`);
    console.log(`   Esperado: ${expected.toFixed(2)}%, Obtenido: ${actual.toFixed(2)}%, Diff: ${diff.toFixed(4)}%`);
    passed++;
  } else {
    console.log(`❌ ${name}`);
    console.log(`   Esperado: ${expected.toFixed(2)}%, Obtenido: ${actual.toFixed(2)}%, Diff: ${diff.toFixed(4)}%`);
    failed++;
  }
}

// ============================================================================
// TEST 1: daysBetween
// ============================================================================
console.log('\n📊 TEST: daysBetween');
console.log('-'.repeat(50));

const days1 = daysBetween('2025-01-01', '2025-12-07');
console.log(`   daysBetween('2025-01-01', '2025-12-07') = ${days1} días`);
if (days1 >= 340 && days1 <= 341) {
  console.log('   ✅ Correcto');
  passed++;
} else {
  console.log('   ❌ Incorrecto');
  failed++;
}

const days2 = daysBetween('2025-12-01', '2025-12-07');
console.log(`   daysBetween('2025-12-01', '2025-12-07') = ${days2} días`);
if (days2 === 6) {
  console.log('   ✅ Correcto');
  passed++;
} else {
  console.log('   ❌ Incorrecto');
  failed++;
}

// ============================================================================
// TEST 2: calculateSimplePersonalReturn - Caso básico sin cashflows
// ============================================================================
console.log('\n📊 TEST: calculateSimplePersonalReturn - Sin cashflows');
console.log('-'.repeat(50));

// Caso: Inversión de $100 que creció a $110 (10% de ganancia)
const simple1 = calculateSimplePersonalReturn(100, 110, 0);
test('Crecimiento simple 10%', simple1, 10);

// Caso: Inversión de $1000 que bajó a $900 (-10% de pérdida)
const simple2 = calculateSimplePersonalReturn(1000, 900, 0);
test('Pérdida simple -10%', simple2, -10);

// ============================================================================
// TEST 3: calculateSimplePersonalReturn - Con depósitos
// ============================================================================
console.log('\n📊 TEST: calculateSimplePersonalReturn - Con depósitos');
console.log('-'.repeat(50));

// Caso: $100 inicial, depositó $100 más, valor final $210
// Ganancia real: $210 - $100 - $100 = $10
// Base: $100 + $100/2 = $150
// Return: $10 / $150 = 6.67%
const simple3 = calculateSimplePersonalReturn(100, 210, -100); // -100 = depósito de $100
test('Con depósito de $100', simple3, 6.67, 0.1);

// Caso: Sin valor inicial, solo depósito de $100, valor final $105
// Return: ($105 - $100) / $100 = 5%
const simple4 = calculateSimplePersonalReturn(0, 105, -100);
test('Solo depósito inicial', simple4, 5);

// ============================================================================
// TEST 4: calculateSimplePersonalReturn - Con retiros
// ============================================================================
console.log('\n📊 TEST: calculateSimplePersonalReturn - Con retiros');
console.log('-'.repeat(50));

// Caso: $200 inicial, retiró $50, valor final $160
// Ganancia real: $160 - $200 + $50 = $10
// Base: $200 - $50/2 = $175
// Return: $10 / $175 = 5.71%
const simple5 = calculateSimplePersonalReturn(200, 160, 50); // 50 = retiro de $50
test('Con retiro de $50', simple5, 5.71, 0.1);

// ============================================================================
// TEST 5: calculateModifiedDietzReturn
// ============================================================================
console.log('\n📊 TEST: calculateModifiedDietzReturn - Cashflows ponderados');
console.log('-'.repeat(50));

// Caso: $1000 inicial, depósito de $500 al inicio del período, valor final $1600
// Días totales: 30, depósito en día 0 (30 días restantes)
// Peso del depósito: 30/30 = 1.0
// Weighted deposits: $500 * 1.0 = $500
// Base: $1000 + $500 = $1500
// Ganancia: $1600 - $1000 - $500 = $100
// Return: $100 / $1500 = 6.67%
const dietz1 = calculateModifiedDietzReturn(
  1000, 
  1600, 
  [{ date: '2025-11-07', amount: -500 }], // Depósito de $500
  '2025-11-07',
  '2025-12-07'
);
test('Depósito al inicio del período', dietz1, 6.67, 0.2);

// Caso: $1000 inicial, depósito de $500 a mitad del período, valor final $1600
// Días totales: 30, depósito en día 15 (15 días restantes)
// Peso del depósito: 15/30 = 0.5
// Weighted deposits: $500 * 0.5 = $250
// Base: $1000 + $250 = $1250
// Ganancia: $1600 - $1000 - $500 = $100
// Return: $100 / $1250 = 8%
const dietz2 = calculateModifiedDietzReturn(
  1000, 
  1600, 
  [{ date: '2025-11-22', amount: -500 }], // Depósito de $500 a mitad
  '2025-11-07',
  '2025-12-07'
);
test('Depósito a mitad del período', dietz2, 8, 0.2);

// Caso: $1000 inicial, depósito de $500 al final del período, valor final $1600
// Días totales: 30, depósito en día 29 (1 día restante)
// Peso del depósito: 1/30 = 0.033
// Weighted deposits: $500 * 0.033 = $16.67
// Base: $1000 + $16.67 = $1016.67
// Ganancia: $1600 - $1000 - $500 = $100
// Return: $100 / $1016.67 = 9.84%
const dietz3 = calculateModifiedDietzReturn(
  1000, 
  1600, 
  [{ date: '2025-12-06', amount: -500 }], // Depósito de $500 al final
  '2025-11-07',
  '2025-12-07'
);
test('Depósito al final del período', dietz3, 9.84, 0.3);

// ============================================================================
// TEST 6: Caso real XTB (valores del diagnóstico)
// ============================================================================
console.log('\n📊 TEST: Caso real XTB');
console.log('-'.repeat(50));

// YTD XTB: 
// Valor Inicial: $252.26
// Valor Final: $4556.02
// CashFlow Total: $-4185.70 (depósitos)
const xtbYTD = calculateSimplePersonalReturn(252.26, 4556.02, -4185.70);
console.log(`   XTB YTD Simple: ${xtbYTD.toFixed(2)}%`);
console.log(`   Esperado: ~5% (según verifyMWRDataAvailability.js)`);
if (xtbYTD > 4 && xtbYTD < 6) {
  console.log('   ✅ En rango esperado');
  passed++;
} else {
  console.log('   ❌ Fuera de rango');
  failed++;
}

// 1M XTB:
// Valor Inicial: $4270.76
// Valor Final: $4556.02
// CashFlow Total: $-158.95
const xtb1M = calculateSimplePersonalReturn(4270.76, 4556.02, -158.95);
console.log(`   XTB 1M Simple: ${xtb1M.toFixed(2)}%`);
console.log(`   Esperado: ~2.9% (según verifyMWRDataAvailability.js)`);
if (xtb1M > 2 && xtb1M < 4) {
  console.log('   ✅ En rango esperado');
  passed++;
} else {
  console.log('   ❌ Fuera de rango');
  failed++;
}

// ============================================================================
// TEST 7: Edge cases
// ============================================================================
console.log('\n📊 TEST: Edge cases');
console.log('-'.repeat(50));

// Caso: Todo a cero
const edge1 = calculateSimplePersonalReturn(0, 0, 0);
test('Todo cero', edge1, 0);

// Caso: Valor inicial negativo (no debería pasar, pero por seguridad)
const edge2 = calculateSimplePersonalReturn(-100, 50, 0);
console.log(`   Valor inicial negativo: ${edge2.toFixed(2)}%`);
console.log('   (No hay expectativa específica, solo verificar que no explote)');
passed++;

// ============================================================================
// TEST 8: Verificar integración con periodCalculations.js
// ============================================================================
console.log('\n📊 TEST: Integración con periodCalculations.js');
console.log('-'.repeat(50));

const { 
  getPeriodBoundaries, 
  sortDocumentsByDate, 
  extractDocumentData,
  initializePeriods,
  normalizeApiKey,
  MIN_DOCS
} = require('../../utils/periodCalculations');

// Test getPeriodBoundaries
const boundaries = getPeriodBoundaries();
if (boundaries.todayISO && boundaries.periods.ytd && boundaries.periods.oneMonth) {
  console.log('   ✅ getPeriodBoundaries funciona correctamente');
  console.log(`      - Today: ${boundaries.todayISO}`);
  console.log(`      - YTD Start: ${boundaries.periods.ytd.startDate}`);
  console.log(`      - 1M Start: ${boundaries.periods.oneMonth.startDate}`);
  passed++;
} else {
  console.log('   ❌ getPeriodBoundaries falló');
  failed++;
}

// Test sortDocumentsByDate
const unsortedDocs = [
  { date: '2025-12-05' },
  { date: '2025-12-01' },
  { date: '2025-12-03' }
];
const sortedDocs = sortDocumentsByDate(unsortedDocs);
if (sortedDocs[0].date === '2025-12-01' && sortedDocs[2].date === '2025-12-05') {
  console.log('   ✅ sortDocumentsByDate ordena correctamente');
  passed++;
} else {
  console.log('   ❌ sortDocumentsByDate falló');
  failed++;
}

// Test extractDocumentData
const mockDoc = {
  date: '2025-12-07',
  USD: {
    totalValue: 1000,
    totalCashFlow: -100,
    totalInvestment: 900,
    adjustedDailyChangePercentage: 0.5
  }
};
const extracted = extractDocumentData(mockDoc, 'USD');
if (extracted && extracted.totalValue === 1000 && extracted.totalCashFlow === -100) {
  console.log('   ✅ extractDocumentData extrae datos correctamente');
  passed++;
} else {
  console.log('   ❌ extractDocumentData falló');
  failed++;
}

// Test initializePeriods
const periods = initializePeriods(boundaries, { includeTWR: false, includeMWR: true });
if (periods.ytd && periods.oneMonth && periods.ytd.cashFlows !== undefined) {
  console.log('   ✅ initializePeriods inicializa correctamente');
  passed++;
} else {
  console.log('   ❌ initializePeriods falló');
  failed++;
}

// Test normalizeApiKey
const normalized = normalizeApiKey('threeMonths', 'PersonalReturn');
if (normalized === 'threeMonthPersonalReturn') {
  console.log('   ✅ normalizeApiKey normaliza correctamente');
  passed++;
} else {
  console.log(`   ❌ normalizeApiKey falló: esperado 'threeMonthPersonalReturn', obtuvo '${normalized}'`);
  failed++;
}

// Test MIN_DOCS
if (MIN_DOCS.oneMonth === 21 && MIN_DOCS.ytd === 1) {
  console.log('   ✅ MIN_DOCS tiene valores correctos');
  passed++;
} else {
  console.log('   ❌ MIN_DOCS tiene valores incorrectos');
  failed++;
}

// ============================================================================
// RESUMEN
// ============================================================================
console.log('\n' + '='.repeat(100));
console.log('RESUMEN DE TESTS');
console.log('='.repeat(100));
console.log();
console.log(`✅ Pasados: ${passed}`);
console.log(`❌ Fallidos: ${failed}`);
console.log();

if (failed === 0) {
  console.log('🎉 TODOS LOS TESTS PASARON');
} else {
  console.log('⚠️ ALGUNOS TESTS FALLARON');
}

process.exit(failed > 0 ? 1 : 0);
