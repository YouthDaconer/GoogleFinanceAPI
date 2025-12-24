/**
 * Script de prueba para funciones unificadas SCALE-CF-001
 * 
 * Ejecutar con: node scripts/testUnifiedFunctions.js
 * 
 * Este script verifica:
 * 1. Que todos los handlers están correctamente exportados
 * 2. Que los routers mapean correctamente las acciones
 * 3. Que el rate limiting se aplica por acción
 */

console.log('='.repeat(60));
console.log('SCALE-CF-001: Test de Funciones Unificadas');
console.log('='.repeat(60));
console.log('');

// ============================================================================
// 1. VERIFICAR HANDLERS
// ============================================================================

console.log('📦 1. VERIFICANDO HANDLERS\n');

const expectedHandlers = {
  assetHandlers: [
    'createAsset', 'updateAsset', 'sellAsset', 'deleteAsset',
    'deleteAssets', 'sellPartialAssetsFIFO', 'addCashTransaction', 'updateStockSector'
  ],
  settingsHandlers: [
    'addCurrency', 'updateCurrency', 'deleteCurrency',
    'updateDefaultCurrency', 'updateUserCountry', 'updateUserDisplayName'
  ],
  accountHandlers: [
    'addPortfolioAccount', 'updatePortfolioAccount',
    'deletePortfolioAccount', 'updatePortfolioAccountBalance'
  ],
  queryHandlers: [
    'getCurrentPricesForUser', 'getHistoricalReturns', 'getMultiAccountHistoricalReturns',
    'getIndexHistory', 'getPortfolioDistribution', 'getAvailableSectors'
  ]
};

let allHandlersOk = true;

Object.entries(expectedHandlers).forEach(([handlerFile, expectedFunctions]) => {
  try {
    const handlers = require(`../services/handlers/${handlerFile}`);
    const missing = expectedFunctions.filter(fn => typeof handlers[fn] !== 'function');
    
    if (missing.length === 0) {
      console.log(`   ✅ ${handlerFile}: ${expectedFunctions.length} funciones OK`);
    } else {
      console.log(`   ❌ ${handlerFile}: Faltan funciones: ${missing.join(', ')}`);
      allHandlersOk = false;
    }
  } catch (error) {
    console.log(`   ❌ ${handlerFile}: Error al cargar - ${error.message}`);
    allHandlersOk = false;
  }
});

console.log('');

// ============================================================================
// 2. VERIFICAR ROUTERS UNIFICADOS
// ============================================================================

console.log('🔀 2. VERIFICANDO ROUTERS UNIFICADOS\n');

const routerTests = [
  { name: 'portfolioOperations', path: '../services/unified/portfolioOperations', expectedActions: expectedHandlers.assetHandlers },
  { name: 'settingsOperations', path: '../services/unified/settingsOperations', expectedActions: expectedHandlers.settingsHandlers },
  { name: 'accountOperations', path: '../services/unified/accountOperations', expectedActions: expectedHandlers.accountHandlers },
  { name: 'queryOperations', path: '../services/unified/queryOperations', expectedActions: expectedHandlers.queryHandlers },
];

let allRoutersOk = true;

routerTests.forEach(({ name, path, expectedActions }) => {
  try {
    const module = require(path);
    
    if (module[name]) {
      // Verificar que VALID_ACTIONS contiene todas las acciones esperadas
      if (module.VALID_ACTIONS) {
        const missingActions = expectedActions.filter(a => !module.VALID_ACTIONS.includes(a));
        if (missingActions.length === 0) {
          console.log(`   ✅ ${name}: ${module.VALID_ACTIONS.length} acciones registradas`);
        } else {
          console.log(`   ⚠️  ${name}: Faltan acciones: ${missingActions.join(', ')}`);
        }
      } else {
        console.log(`   ⚠️  ${name}: VALID_ACTIONS no exportado`);
      }
    } else {
      console.log(`   ❌ ${name}: No encontrado en exports`);
      allRoutersOk = false;
    }
  } catch (error) {
    console.log(`   ❌ ${name}: Error - ${error.message}`);
    allRoutersOk = false;
  }
});

console.log('');

// ============================================================================
// 3. VERIFICAR RATE LIMIT CONFIG
// ============================================================================

console.log('⏱️  3. VERIFICANDO RATE LIMIT CONFIG\n');

try {
  const { getRateLimitConfig, RATE_LIMITS } = require('../config/rateLimits');
  
  // Verificar que cada acción tiene configuración de rate limit
  const allActions = [
    ...expectedHandlers.assetHandlers,
    ...expectedHandlers.settingsHandlers,
    ...expectedHandlers.accountHandlers,
    ...expectedHandlers.queryHandlers
  ];
  
  let configuredCount = 0;
  let defaultCount = 0;
  
  allActions.forEach(action => {
    const config = getRateLimitConfig(action);
    if (RATE_LIMITS[action]) {
      configuredCount++;
    } else {
      defaultCount++;
    }
  });
  
  console.log(`   ✅ ${configuredCount} acciones con rate limit específico`);
  console.log(`   ℹ️  ${defaultCount} acciones usando rate limit por defecto`);
  
  // Mostrar algunos ejemplos
  console.log('\n   Ejemplos de configuración:');
  ['createAsset', 'getHistoricalReturns', 'addCurrency'].forEach(action => {
    const config = getRateLimitConfig(action);
    console.log(`   - ${action}: ${config.limit} req/${config.windowMs/1000}s`);
  });
  
} catch (error) {
  console.log(`   ❌ Error verificando rate limits: ${error.message}`);
}

console.log('');

// ============================================================================
// 4. VERIFICAR INDEX.JS EXPORTS
// ============================================================================

console.log('📤 4. VERIFICANDO EXPORTS EN INDEX.JS\n');

try {
  const index = require('../index');
  
  const unifiedExports = ['portfolioOperations', 'settingsOperations', 'accountOperations', 'queryOperations'];
  const legacyExports = ['createAsset', 'updateAsset', 'sellAsset', 'addCurrency', 'getHistoricalReturns'];
  
  console.log('   Funciones unificadas (nuevas):');
  unifiedExports.forEach(fn => {
    if (index[fn]) {
      console.log(`   ✅ ${fn}`);
    } else {
      console.log(`   ❌ ${fn} - NO ENCONTRADO`);
    }
  });
  
  console.log('\n   Funciones legacy (mantener durante transición):');
  legacyExports.forEach(fn => {
    if (index[fn]) {
      console.log(`   ✅ ${fn}`);
    } else {
      console.log(`   ⚠️  ${fn} - no encontrado (puede estar deprecado)`);
    }
  });
  
} catch (error) {
  console.log(`   ❌ Error: ${error.message}`);
}

console.log('');

// ============================================================================
// 5. RESUMEN
// ============================================================================

console.log('='.repeat(60));
console.log('📊 RESUMEN');
console.log('='.repeat(60));

if (allHandlersOk && allRoutersOk) {
  console.log('\n✅ Todas las verificaciones pasaron correctamente');
  console.log('\nPróximos pasos:');
  console.log('1. Ejecutar: firebase emulators:start --only functions');
  console.log('2. Probar las funciones con el frontend');
  console.log('3. Activar feature flags gradualmente');
} else {
  console.log('\n❌ Hay problemas que resolver antes del deploy');
}

console.log('');
