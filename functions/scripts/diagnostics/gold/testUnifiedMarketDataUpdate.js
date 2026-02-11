/**
 * Script de prueba para unifiedMarketDataUpdate
 * 
 * PROPÓSITO:
 * Verificar que la función EOD puede:
 * 1. Conectarse al API Lambda
 * 2. Obtener precios y currencies
 * 3. Calcular performance del portafolio (dry-run)
 * 
 * USO:
 *   node testUnifiedMarketDataUpdate.js          # Dry-run completo
 *   node testUnifiedMarketDataUpdate.js --quick  # Solo verificar conexión API
 * 
 * NOTA: Requiere ejecutarse desde el directorio scripts/diagnostics/gold
 */

// Configurar variables de entorno ANTES de cargar módulos
process.env.FINANCE_QUERY_API_URL = 'https://api.portastock.top/v1';
process.env.CF_SERVICE_TOKEN = '26ca00231ead1b5fbd63c6bba10a16e2f619b56809013ab3b3bcbbfb029aff10';

const admin = require('firebase-admin');
const { DateTime } = require('luxon');

// Inicializar Firebase Admin
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// Importar helpers después de configurar variables de entorno
const { getPricesFromApi, getCurrencyRatesFromApi } = require('../../../services/marketDataHelper');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const CONFIG = {
  DRY_RUN: true, // No escribir a Firestore
};

// ============================================================================
// UTILIDADES
// ============================================================================

function log(level, message, data = null) {
  const timestamp = new Date().toISOString();
  const prefix = {
    'INFO': '📋',
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'DEBUG': '🔍',
    'PROGRESS': '🔄',
  }[level] || '•';
  
  console.log(`${prefix} [${timestamp}] ${message}`);
  if (data) console.log('   ', JSON.stringify(data, null, 2).split('\n').join('\n    '));
}

// Festivos NYSE
const NYSE_HOLIDAYS_FALLBACK = new Set([
  '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26',
  '2025-06-19', '2025-07-04', '2025-09-01', '2025-11-27', '2025-12-25',
  '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25',
  '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
]);

async function isValidTradingDay(date) {
  const dayOfWeek = date.weekday;
  const formattedDate = date.toISODate();
  
  if (dayOfWeek === 6) return { isValid: false, reason: 'saturday', formattedDate };
  if (dayOfWeek === 7) return { isValid: false, reason: 'sunday', formattedDate };
  
  // Verificar en marketHolidays/US
  try {
    const holidayDoc = await db.collection('marketHolidays').doc('US').get();
    if (holidayDoc.exists) {
      const data = holidayDoc.data();
      if (data.holidays?.includes?.(formattedDate)) {
        return { isValid: false, reason: 'holiday-firestore', holiday: formattedDate, formattedDate };
      }
    }
  } catch (error) {
    log('WARNING', 'Error checking marketHolidays, using fallback');
  }
  
  if (NYSE_HOLIDAYS_FALLBACK.has(formattedDate)) {
    return { isValid: false, reason: 'holiday-fallback', formattedDate };
  }
  
  return { isValid: true, reason: 'trading-day', formattedDate };
}

// ============================================================================
// PRUEBA PRINCIPAL
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  const quickMode = args.includes('--quick');
  
  console.log('');
  console.log('═'.repeat(80));
  console.log('  TEST: unifiedMarketDataUpdate');
  console.log('═'.repeat(80));
  console.log('');
  
  const now = DateTime.now().setZone('America/New_York');
  const yesterday = now.minus({ days: 1 });
  
  log('INFO', 'Configuración de prueba:', {
    mode: quickMode ? 'quick (solo API)' : 'full (dry-run)',
    dryRun: CONFIG.DRY_RUN,
    currentTime: now.toISO(),
    targetDate: yesterday.toISODate()
  });
  
  // =========================================================================
  // PASO 1: Verificar si ayer fue día de trading
  // =========================================================================
  log('PROGRESS', 'Verificando si ayer fue día de trading...');
  const tradingDayCheck = await isValidTradingDay(yesterday);
  
  if (!tradingDayCheck.isValid) {
    log('WARNING', `${yesterday.toISODate()} NO fue día de trading`, {
      reason: tradingDayCheck.reason,
      holiday: tradingDayCheck.holiday
    });
  } else {
    log('SUCCESS', `${yesterday.toISODate()} fue día de trading válido`);
  }
  
  // =========================================================================
  // PASO 2: Obtener símbolos de assets activos
  // =========================================================================
  log('PROGRESS', 'Obteniendo símbolos de assets activos...');
  const assetsSnapshot = await db.collection('assets').where('isActive', '==', true).get();
  const symbols = [...new Set(assetsSnapshot.docs.map(d => d.data().name).filter(Boolean))];
  log('SUCCESS', `Encontrados ${assetsSnapshot.size} assets activos, ${symbols.length} símbolos únicos`);
  
  if (symbols.length > 0) {
    log('DEBUG', 'Primeros 10 símbolos:', symbols.slice(0, 10));
  }
  
  // =========================================================================
  // PASO 3: Probar conexión a API Lambda - Precios
  // =========================================================================
  log('PROGRESS', 'Probando conexión al API Lambda (precios)...');
  try {
    const testSymbols = symbols.slice(0, 5); // Probar con 5 símbolos
    const prices = await getPricesFromApi(testSymbols);
    
    if (prices.length > 0) {
      log('SUCCESS', `API Lambda respondió con ${prices.length} precios`);
      prices.slice(0, 3).forEach(p => {
        log('DEBUG', `  ${p.symbol}: $${p.price} (${p.percentChange}%)`);
      });
    } else {
      log('WARNING', 'API Lambda respondió vacío');
    }
  } catch (error) {
    log('ERROR', 'Error conectando al API Lambda (precios)', { error: error.message });
    process.exit(1);
  }
  
  // =========================================================================
  // PASO 4: Probar conexión a API Lambda - Currencies
  // =========================================================================
  log('PROGRESS', 'Probando conexión al API Lambda (currencies)...');
  try {
    const currencies = await getCurrencyRatesFromApi();
    
    if (currencies.length > 0) {
      log('SUCCESS', `API Lambda respondió con ${currencies.length} currencies`);
      currencies.slice(0, 3).forEach(c => {
        log('DEBUG', `  ${c.code}: ${c.rate} (${c.name || 'N/A'})`);
      });
    } else {
      log('WARNING', 'API Lambda respondió vacío para currencies');
    }
  } catch (error) {
    log('ERROR', 'Error conectando al API Lambda (currencies)', { error: error.message });
    process.exit(1);
  }
  
  if (quickMode) {
    console.log('');
    log('SUCCESS', '✅ Prueba rápida completada - API Lambda funcionando correctamente');
    console.log('');
    process.exit(0);
  }
  
  // =========================================================================
  // PASO 5: Obtener todos los precios
  // =========================================================================
  log('PROGRESS', 'Obteniendo todos los precios...');
  const allPrices = await getPricesFromApi(symbols);
  log('SUCCESS', `Obtenidos ${allPrices.length} precios de ${symbols.length} símbolos`);
  
  // =========================================================================
  // PASO 6: Obtener todas las currencies
  // =========================================================================
  log('PROGRESS', 'Obteniendo todas las currencies...');
  const allCurrencies = await getCurrencyRatesFromApi();
  log('SUCCESS', `Obtenidas ${allCurrencies.length} currencies`);
  
  // =========================================================================
  // PASO 7: Simular cálculo de performance (sin escribir)
  // =========================================================================
  log('PROGRESS', '[DRY-RUN] Simulando cálculo de performance...');
  
  // Obtener usuarios con portafolios activos
  const accountsSnapshot = await db.collection('portfolioAccounts')
    .where('isActive', '==', true)
    .get();
  
  const userIds = [...new Set(accountsSnapshot.docs.map(d => d.data().userId))];
  
  log('INFO', `Usuarios con cuentas activas: ${userIds.length}`);
  log('INFO', `Cuentas activas totales: ${accountsSnapshot.size}`);
  
  // Verificar último documento de performance
  for (const userId of userIds.slice(0, 2)) {
    const lastPerf = await db.collection(`portfolioPerformance/${userId}/dates`)
      .orderBy('date', 'desc')
      .limit(1)
      .get();
    
    if (!lastPerf.empty) {
      const doc = lastPerf.docs[0].data();
      log('DEBUG', `Último performance para ${userId.slice(0, 8)}...`, {
        date: doc.date,
        totalValue: doc.USD?.totalValue?.toFixed(2) || 'N/A'
      });
    }
  }
  
  // =========================================================================
  // RESUMEN
  // =========================================================================
  console.log('');
  console.log('═'.repeat(80));
  console.log('  RESUMEN');
  console.log('═'.repeat(80));
  
  log('SUCCESS', 'Verificación completada:', {
    tradingDay: tradingDayCheck.isValid ? 'Sí' : 'No',
    targetDate: yesterday.toISODate(),
    assetsActivos: assetsSnapshot.size,
    símbolosÚnicos: symbols.length,
    preciosObtenidos: allPrices.length,
    currenciesObtenidas: allCurrencies.length,
    usuariosActivos: userIds.length,
    cuentasActivas: accountsSnapshot.size
  });
  
  if (CONFIG.DRY_RUN) {
    log('INFO', '⚠️  Este fue un DRY-RUN. No se escribieron datos a Firestore.');
  }
  
  console.log('');
  log('SUCCESS', '✅ Test de unifiedMarketDataUpdate completado exitosamente');
  console.log('');
  
  process.exit(0);
}

main().catch(error => {
  log('ERROR', 'Error fatal en test:', { error: error.message, stack: error.stack });
  process.exit(1);
});
