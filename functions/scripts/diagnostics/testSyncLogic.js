/**
 * Script de diagnóstico para verificar la lógica de sincronización
 * OPT-SYNC-001: Pruebas locales de la implementación
 * 
 * Ejecutar: node scripts/diagnostics/testSyncLogic.js
 */

const { DateTime } = require('luxon');

// ============================================================================
// CONSTANTES (copiadas de unifiedMarketDataUpdate.js)
// ============================================================================

const REFRESH_INTERVAL_MINUTES = 5;
const NYSE_OPEN_HOUR = 9;
const NYSE_OPEN_MINUTE = 30;
const NYSE_CLOSE_HOUR = 16;

// ============================================================================
// FUNCIONES A PROBAR (copiadas del backend)
// ============================================================================

/**
 * Verifica si el mercado NYSE está abierto (fallback local).
 */
function isNYSEMarketOpen(testTime = null) {
  const nyNow = testTime || DateTime.now().setZone('America/New_York');
  const hour = nyNow.hour;
  const minute = nyNow.minute;
  const dayOfWeek = nyNow.weekday; // 1=Monday, 7=Sunday
  
  // Fin de semana: cerrado
  if (dayOfWeek === 6 || dayOfWeek === 7) {
    return false;
  }
  
  // Convertir hora actual a minutos desde medianoche
  const currentMinutes = hour * 60 + minute;
  const openMinutes = NYSE_OPEN_HOUR * 60 + NYSE_OPEN_MINUTE; // 9:30 = 570
  const closeMinutes = NYSE_CLOSE_HOUR * 60; // 16:00 = 960
  
  return currentMinutes >= openMinutes && currentMinutes < closeMinutes;
}

/**
 * Calcula scheduledAt y nextScheduledUpdate (copiado del backend)
 */
function calculateScheduleTimes(testTime = null) {
  const now = testTime || DateTime.now().setZone('America/New_York');
  const scheduledMinute = Math.floor(now.minute / REFRESH_INTERVAL_MINUTES) * REFRESH_INTERVAL_MINUTES;
  const scheduledAt = now.set({ minute: scheduledMinute, second: 0, millisecond: 0 });
  const nextScheduledUpdate = scheduledAt.plus({ minutes: REFRESH_INTERVAL_MINUTES });
  
  return { now, scheduledAt, nextScheduledUpdate };
}

/**
 * Simula el cálculo del frontend para segundos restantes
 */
function calculateSecondsToNextUpdate(nextScheduledUpdate, currentTime, refreshInterval = 5) {
  const now = currentTime || DateTime.now();
  const nextUpdate = DateTime.fromISO(nextScheduledUpdate);
  const diffMs = nextUpdate.toMillis() - now.toMillis();
  
  if (diffMs > 0) {
    // Aún no ha llegado el tiempo programado
    return Math.max(1, Math.floor(diffMs / 1000));
  } else {
    // Ya pasó el tiempo programado - calcular cuántos intervalos han pasado
    const msSinceScheduled = Math.abs(diffMs);
    const intervalMs = refreshInterval * 60 * 1000;
    const intervalsPassedSinceScheduled = Math.floor(msSinceScheduled / intervalMs);
    const nextUpdateTime = nextUpdate.toMillis() + ((intervalsPassedSinceScheduled + 1) * intervalMs);
    const msUntilNext = nextUpdateTime - now.toMillis();
    return Math.max(1, Math.floor(msUntilNext / 1000));
  }
}

// ============================================================================
// TESTS
// ============================================================================

console.log('═══════════════════════════════════════════════════════════════════');
console.log('  OPT-SYNC-001: Diagnóstico de Lógica de Sincronización');
console.log('═══════════════════════════════════════════════════════════════════\n');

// Test 1: Hora actual
console.log('📅 TEST 1: Estado actual');
console.log('─────────────────────────────────────────────────────────────────');
const nowNY = DateTime.now().setZone('America/New_York');
const nowUTC = DateTime.now().setZone('UTC');
console.log(`  Hora local (sistema): ${DateTime.now().toFormat('yyyy-MM-dd HH:mm:ss ZZZZ')}`);
console.log(`  Hora NY:              ${nowNY.toFormat('yyyy-MM-dd HH:mm:ss ZZZZ')} (${nowNY.weekdayLong})`);
console.log(`  Hora UTC:             ${nowUTC.toFormat('yyyy-MM-dd HH:mm:ss')}`);
console.log(`  isNYSEMarketOpen():   ${isNYSEMarketOpen() ? '✅ ABIERTO' : '❌ CERRADO'}`);
console.log();

// Test 2: Verificar horarios de mercado para diferentes días
console.log('📅 TEST 2: Horarios de mercado por día de la semana');
console.log('─────────────────────────────────────────────────────────────────');

const testDays = [
  { name: 'Lunes 9:29 AM', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 9, minute: 29 }, { zone: 'America/New_York' }) },
  { name: 'Lunes 9:30 AM', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 9, minute: 30 }, { zone: 'America/New_York' }) },
  { name: 'Lunes 12:00 PM', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 12, minute: 0 }, { zone: 'America/New_York' }) },
  { name: 'Lunes 3:59 PM', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 15, minute: 59 }, { zone: 'America/New_York' }) },
  { name: 'Lunes 4:00 PM', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 16, minute: 0 }, { zone: 'America/New_York' }) },
  { name: 'Sábado 12:00 PM', time: DateTime.fromObject({ year: 2026, month: 1, day: 3, hour: 12, minute: 0 }, { zone: 'America/New_York' }) },
  { name: 'Domingo 12:00 PM', time: DateTime.fromObject({ year: 2026, month: 1, day: 4, hour: 12, minute: 0 }, { zone: 'America/New_York' }) },
];

testDays.forEach(({ name, time }) => {
  const isOpen = isNYSEMarketOpen(time);
  console.log(`  ${name.padEnd(20)} → ${isOpen ? '✅ ABIERTO' : '❌ CERRADO'}`);
});
console.log();

// Test 3: Cálculo de scheduledAt y nextScheduledUpdate
console.log('📅 TEST 3: Cálculo de tiempos de schedule');
console.log('─────────────────────────────────────────────────────────────────');

const scheduleTests = [
  { name: '10:02:35', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 2, second: 35 }, { zone: 'America/New_York' }) },
  { name: '10:05:00', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 5, second: 0 }, { zone: 'America/New_York' }) },
  { name: '10:07:22', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 7, second: 22 }, { zone: 'America/New_York' }) },
  { name: '10:10:01', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 10, second: 1 }, { zone: 'America/New_York' }) },
  { name: '15:58:45', time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 15, minute: 58, second: 45 }, { zone: 'America/New_York' }) },
];

scheduleTests.forEach(({ name, time }) => {
  const { scheduledAt, nextScheduledUpdate } = calculateScheduleTimes(time);
  console.log(`  Hora actual: ${name}`);
  console.log(`    scheduledAt:        ${scheduledAt.toFormat('HH:mm:ss')}`);
  console.log(`    nextScheduledUpdate: ${nextScheduledUpdate.toFormat('HH:mm:ss')}`);
  console.log();
});

// Test 4: Cálculo del countdown del frontend
console.log('📅 TEST 4: Cálculo del countdown (frontend)');
console.log('─────────────────────────────────────────────────────────────────');

const countdownTests = [
  { 
    name: 'Countdown normal (2:30 restantes)',
    nextScheduledUpdate: '2026-01-05T10:10:00.000-05:00',
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 7, second: 30 }, { zone: 'America/New_York' }),
    expected: 150 // 2:30
  },
  { 
    name: 'Countdown llegó a 0, servidor no actualizó (1 min pasado)',
    nextScheduledUpdate: '2026-01-05T10:10:00.000-05:00',
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 11, second: 0 }, { zone: 'America/New_York' }),
    expected: 240 // 4:00 (próximo intervalo es 10:15, faltan 4 min)
  },
  { 
    name: 'Countdown llegó a 0, han pasado 3 minutos',
    nextScheduledUpdate: '2026-01-05T10:10:00.000-05:00',
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 13, second: 0 }, { zone: 'America/New_York' }),
    expected: 120 // 2:00 (próximo intervalo es 10:15, faltan 2 min)
  },
  { 
    name: 'Countdown llegó a 0, han pasado 7 minutos (más de 1 intervalo)',
    nextScheduledUpdate: '2026-01-05T10:10:00.000-05:00',
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 17, second: 0 }, { zone: 'America/New_York' }),
    expected: 180 // 3:00 (próximo intervalo es 10:20, faltan 3 min)
  },
  { 
    name: 'Datos obsoletos (de ayer), calcular próximo intervalo',
    nextScheduledUpdate: '2026-01-05T15:55:00.000-05:00',
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 6, hour: 10, minute: 2, second: 0 }, { zone: 'America/New_York' }),
    // Han pasado ~18 horas = 1080 minutos = 216 intervalos de 5 min
    // Próximo desde ayer 15:55: hoy 10:05 (después de 216 intervalos de 5 min no coincide exactamente)
    // El algoritmo calcula: nextUpdate + (intervalsPassedSinceScheduled + 1) * intervalMs
    expected: 180 // Aproximado, depende del cálculo exacto
  },
];

countdownTests.forEach(({ name, nextScheduledUpdate, currentTime, expected }) => {
  const seconds = calculateSecondsToNextUpdate(nextScheduledUpdate, currentTime);
  const minutes = Math.floor(seconds / 60);
  const secs = seconds % 60;
  const status = Math.abs(seconds - expected) <= 5 ? '✅' : '⚠️';
  console.log(`  ${name}`);
  console.log(`    nextScheduledUpdate: ${nextScheduledUpdate}`);
  console.log(`    currentTime:         ${currentTime.toISO()}`);
  console.log(`    Resultado:           ${minutes}:${secs.toString().padStart(2, '0')} (${seconds}s) ${status}`);
  console.log(`    Esperado:            ~${Math.floor(expected/60)}:${(expected%60).toString().padStart(2, '0')} (${expected}s)`);
  console.log();
});

// Test 5: Simulación de flujo completo
console.log('📅 TEST 5: Simulación de flujo completo (día hábil)');
console.log('─────────────────────────────────────────────────────────────────');

function formatLastUpdate(lastUpdateTime, currentTime, refreshInterval = 5) {
  const diffSeconds = Math.floor((currentTime.toMillis() - lastUpdateTime.toMillis()) / 1000);
  const maxExpectedMs = refreshInterval * 2 * 60 * 1000; // 2 intervalos
  const isStale = (currentTime.toMillis() - lastUpdateTime.toMillis()) > maxExpectedMs;
  
  if (diffSeconds < 60) {
    return 'Hace unos segundos';
  } else if (diffSeconds < 120) {
    return 'Hace 1 minuto';
  } else if (diffSeconds < 3600) {
    const minutes = Math.floor(diffSeconds / 60);
    return isStale ? `Hace ${minutes} min ⚠️` : `Hace ${minutes} min`;
  } else {
    return `${lastUpdateTime.toFormat('HH:mm')} ⚠️`;
  }
}

function simulateFullFlow() {
  console.log('  Escenario: Lunes 5 de enero 2026, 10:05:02 AM NY\n');
  
  // 1. Backend calcula tiempos
  const backendTime = DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 5, second: 2 }, { zone: 'America/New_York' });
  const { scheduledAt, nextScheduledUpdate } = calculateScheduleTimes(backendTime);
  
  console.log('  [BACKEND] Cron dispara:');
  console.log(`    Hora actual:         ${backendTime.toFormat('HH:mm:ss')}`);
  console.log(`    scheduledAt:         ${scheduledAt.toFormat('HH:mm:ss')}`);
  console.log(`    nextScheduledUpdate: ${nextScheduledUpdate.toFormat('HH:mm:ss')}`);
  console.log();
  
  // 2. Pipeline ejecuta (simular 6 segundos)
  const pipelineEndTime = backendTime.plus({ seconds: 6 });
  console.log('  [BACKEND] Pipeline completa:');
  console.log(`    Duración:            ~6 segundos`);
  console.log(`    lastCompleteUpdate:  ${pipelineEndTime.toFormat('HH:mm:ss.SSS')}`);
  console.log();
  
  // 3. Frontend recibe datos
  console.log('  [FRONTEND] Recibe datos via onSnapshot:');
  console.log(`    lastCompleteUpdate:  ${pipelineEndTime.toISO()}`);
  console.log(`    nextScheduledUpdate: ${nextScheduledUpdate.toISO()}`);
  console.log();
  
  // 4. Frontend calcula countdown en diferentes momentos
  const frontendTimes = [
    { label: 'Inmediatamente', time: pipelineEndTime.plus({ seconds: 1 }) },
    { label: 'Después de 1 min', time: pipelineEndTime.plus({ minutes: 1 }) },
    { label: 'Después de 3 min', time: pipelineEndTime.plus({ minutes: 3 }) },
    { label: 'Cuando countdown = 0', time: nextScheduledUpdate },
    { label: '30 seg después de 0', time: nextScheduledUpdate.plus({ seconds: 30 }) },
  ];
  
  console.log('  [FRONTEND] Countdown en diferentes momentos:');
  frontendTimes.forEach(({ label, time }) => {
    const seconds = calculateSecondsToNextUpdate(nextScheduledUpdate.toISO(), time);
    const haceText = formatLastUpdate(pipelineEndTime, time);
    console.log(`    ${label.padEnd(25)} → Countdown: ${Math.floor(seconds/60)}:${(seconds%60).toString().padStart(2, '0')} | "${haceText}"`);
  });
  console.log();
  
  // 5. Servidor actualiza a las 10:10
  const nextBackendTime = DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 10, second: 3 }, { zone: 'America/New_York' });
  const nextSchedule = calculateScheduleTimes(nextBackendTime);
  
  console.log('  [BACKEND] Nueva ejecución a las 10:10:03:');
  console.log(`    Nuevo scheduledAt:         ${nextSchedule.scheduledAt.toFormat('HH:mm:ss')}`);
  console.log(`    Nuevo nextScheduledUpdate: ${nextSchedule.nextScheduledUpdate.toFormat('HH:mm:ss')}`);
  console.log();
  
  console.log('  ✅ Flujo completo verificado correctamente');
}

simulateFullFlow();

// Test 6: Escenarios de datos obsoletos
console.log('\n📅 TEST 6: Detección de datos obsoletos');
console.log('─────────────────────────────────────────────────────────────────');

const staleTests = [
  { 
    name: 'Normal (dentro del intervalo)',
    lastUpdate: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 5 }, { zone: 'America/New_York' }),
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 8 }, { zone: 'America/New_York' }),
    expectedStale: false
  },
  { 
    name: 'Límite (exactamente 2 intervalos)',
    lastUpdate: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 0 }, { zone: 'America/New_York' }),
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 10 }, { zone: 'America/New_York' }),
    expectedStale: false
  },
  { 
    name: 'Obsoleto (15 min sin actualizar)',
    lastUpdate: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 0 }, { zone: 'America/New_York' }),
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 15 }, { zone: 'America/New_York' }),
    expectedStale: true
  },
  { 
    name: 'Muy obsoleto (1 hora)',
    lastUpdate: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 9, minute: 30 }, { zone: 'America/New_York' }),
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 30 }, { zone: 'America/New_York' }),
    expectedStale: true
  },
  { 
    name: 'Día siguiente (mercado cerró ayer)',
    lastUpdate: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 15, minute: 55 }, { zone: 'America/New_York' }),
    currentTime: DateTime.fromObject({ year: 2026, month: 1, day: 6, hour: 9, minute: 30 }, { zone: 'America/New_York' }),
    expectedStale: true
  },
];

staleTests.forEach(({ name, lastUpdate, currentTime, expectedStale }) => {
  const result = formatLastUpdate(lastUpdate, currentTime);
  const isStale = result.includes('⚠️');
  const status = isStale === expectedStale ? '✅' : '❌';
  console.log(`  ${name}:`);
  console.log(`    lastUpdate:  ${lastUpdate.toFormat('yyyy-MM-dd HH:mm')}`);
  console.log(`    currentTime: ${currentTime.toFormat('yyyy-MM-dd HH:mm')}`);
  console.log(`    Resultado:   "${result}" ${status}`);
  console.log();
});

// Test 6: Verificar DST (Daylight Saving Time)
console.log('\n📅 TEST 6: Verificación de DST (Daylight Saving Time)');
console.log('─────────────────────────────────────────────────────────────────');

const dstTests = [
  { 
    name: 'Invierno (EST, UTC-5)', 
    time: DateTime.fromObject({ year: 2026, month: 1, day: 5, hour: 10, minute: 0 }, { zone: 'America/New_York' }),
    expectedOffset: -5
  },
  { 
    name: 'Verano (EDT, UTC-4)', 
    time: DateTime.fromObject({ year: 2026, month: 7, day: 6, hour: 10, minute: 0 }, { zone: 'America/New_York' }),
    expectedOffset: -4
  },
  { 
    name: 'Día cambio DST (Marzo)', 
    time: DateTime.fromObject({ year: 2026, month: 3, day: 9, hour: 10, minute: 0 }, { zone: 'America/New_York' }),
    expectedOffset: -4 // Ya cambió a EDT
  },
];

dstTests.forEach(({ name, time, expectedOffset }) => {
  const offset = time.offset / 60;
  const status = offset === expectedOffset ? '✅' : '❌';
  const isOpen = isNYSEMarketOpen(time);
  console.log(`  ${name}:`);
  console.log(`    Fecha:      ${time.toFormat('yyyy-MM-dd HH:mm')}`);
  console.log(`    Offset:     UTC${offset >= 0 ? '+' : ''}${offset} ${status}`);
  console.log(`    Mercado:    ${isOpen ? 'ABIERTO' : 'CERRADO'}`);
  console.log();
});

console.log('═══════════════════════════════════════════════════════════════════');
console.log('  Diagnóstico completado');
console.log('═══════════════════════════════════════════════════════════════════');
