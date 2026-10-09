/**
 * Script de Sincronización de Transacciones con Assets
 * 
 * PROPÓSITO:
 * Identificar y corregir discrepancias entre las unidades calculadas desde
 * transacciones y las unidades actuales en la colección assets.
 * 
 * Esto es necesario cuando:
 * - Se importaron assets manualmente sin crear transacciones
 * - Se modificaron assets directamente en Firestore
 * - Hay inconsistencias por bugs anteriores
 * 
 * USO:
 *   node syncTransactionsWithAssets.js --analyze     # Solo análisis
 *   node syncTransactionsWithAssets.js --dry-run     # Ver transacciones a crear
 *   node syncTransactionsWithAssets.js --fix         # Crear transacciones
 * 
 * OPCIONES:
 *   --user=<userId>        # Usuario específico (default: DDeR8P5hYgfuN8gcU4RsQfdTJqx2)
 *   --date=YYYY-MM-DD      # Fecha de las transacciones de ajuste (default: día anterior)
 * 
 * @see backfillPortfolioPerformance.js
 */

const admin = require('firebase-admin');

// Inicializar Firebase Admin
const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

const CONFIG = {
  DEFAULT_USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  // Fecha por defecto: día anterior al actual
  DEFAULT_ADJUSTMENT_DATE: getYesterday(),
  // Umbral mínimo de diferencia para crear transacción
  MIN_DIFFERENCE_THRESHOLD: 0.0001,
};

function getYesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().split('T')[0];
}

// ============================================================================
// UTILIDADES
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'analyze',
    userId: CONFIG.DEFAULT_USER_ID,
    adjustmentDate: CONFIG.DEFAULT_ADJUSTMENT_DATE,
  };

  args.forEach(arg => {
    if (arg === '--analyze') options.mode = 'analyze';
    else if (arg === '--dry-run') options.mode = 'dry-run';
    else if (arg === '--fix') options.mode = 'fix';
    else if (arg.startsWith('--user=')) options.userId = arg.split('=')[1];
    else if (arg.startsWith('--date=')) options.adjustmentDate = arg.split('=')[1];
  });

  return options;
}

function log(level, message, data = null) {
  const prefix = {
    'INFO': '📋',
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'DEBUG': '🔍',
    'PROGRESS': '🔄',
  }[level] || '•';
  
  console.log(`${prefix} ${message}`);
  if (data) {
    const lines = JSON.stringify(data, null, 2).split('\n');
    lines.forEach(line => console.log('    ' + line));
  }
}

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

async function getUserAccounts(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function getAccountAssets(accountId) {
  // Obtener TODOS los assets (activos e inactivos) para comparar
  const activeSnap = await db.collection('assets')
    .where('portfolioAccount', '==', accountId)
    .where('isActive', '==', true)
    .get();
  
  const inactiveSnap = await db.collection('assets')
    .where('portfolioAccount', '==', accountId)
    .where('isActive', '==', false)
    .get();
  
  const assets = [];
  
  activeSnap.docs.forEach(doc => {
    assets.push({ id: doc.id, ...doc.data(), isActive: true });
  });
  
  inactiveSnap.docs.forEach(doc => {
    assets.push({ id: doc.id, ...doc.data(), isActive: false });
  });
  
  return assets;
}

async function getAccountTransactions(accountId) {
  const snapshot = await db.collection('transactions')
    .where('portfolioAccountId', '==', accountId)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

async function getSampleSellTransaction() {
  // Obtener una transacción de venta como referencia de estructura
  const snapshot = await db.collection('transactions')
    .where('type', '==', 'sell')
    .limit(1)
    .get();
  
  if (snapshot.empty) return null;
  return snapshot.docs[0].data();
}

async function getCurrentPrices(symbols) {
  // Obtener precios actuales desde el API
  const prices = {};
  
  try {
    const fetch = require('node-fetch');
    const symbolsParam = symbols.join(',');
    const url = `https://354sdh5hcrnztw5vquw6sxiduu0gigku.lambda-url.us-east-1.on.aws/v1/market-quotes?symbols=${encodeURIComponent(symbolsParam)}`;
    
    const response = await fetch(url);
    if (response.ok) {
      const data = await response.json();
      data.forEach(quote => {
        prices[quote.symbol] = quote.price || quote.regularMarketPrice || 100;
      });
    }
  } catch (e) {
    log('WARNING', 'No se pudieron obtener precios actuales, usando estimados');
  }
  
  return prices;
}

// ============================================================================
// CÁLCULO DE HOLDINGS DESDE TRANSACCIONES
// ============================================================================

function calculateHoldingsFromTransactions(transactions) {
  const holdings = new Map();
  
  // Ordenar por fecha, luego BUY antes de SELL
  const typeOrder = { 'buy': 0, 'sell': 1 };
  const sortedTx = [...transactions].sort((a, b) => {
    if (a.date !== b.date) return a.date.localeCompare(b.date);
    return (typeOrder[a.type] || 99) - (typeOrder[b.type] || 99);
  });
  
  sortedTx.forEach(tx => {
    if (!tx.assetName || (tx.type !== 'buy' && tx.type !== 'sell')) return;
    
    const assetKey = `${tx.assetName}_${tx.assetType || 'stock'}`;
    const current = holdings.get(assetKey) || { 
      units: 0, 
      assetName: tx.assetName,
      assetType: tx.assetType || 'stock',
      assetId: tx.assetId,
      currency: tx.currency || 'USD',
      market: tx.market,
      lastPrice: 0,
      lastPriceDate: null
    };
    
    if (tx.type === 'buy') {
      current.units += tx.amount || 0;
      // Guardar el assetId más reciente
      if (tx.assetId) current.assetId = tx.assetId;
      if (tx.currency) current.currency = tx.currency;
      if (tx.market) current.market = tx.market;
    } else if (tx.type === 'sell') {
      current.units -= tx.amount || 0;
    }
    
    // Siempre guardar el último precio (la transacción más reciente)
    if (tx.price && (!current.lastPriceDate || tx.date >= current.lastPriceDate)) {
      current.lastPrice = tx.price;
      current.lastPriceDate = tx.date;
    }
    
    holdings.set(assetKey, current);
  });
  
  return holdings;
}

// ============================================================================
// ANÁLISIS DE DIFERENCIAS
// ============================================================================

async function analyzeAccount(account, options) {
  const transactions = await getAccountTransactions(account.id);
  const assets = await getAccountAssets(account.id);
  
  // Calcular holdings desde transacciones
  const txHoldings = calculateHoldingsFromTransactions(transactions);
  
  // Crear mapa de assets actuales
  // IMPORTANTE: Solo sumar unidades de assets ACTIVOS
  const assetMap = new Map();
  assets.forEach(asset => {
    const assetKey = `${asset.name}_${asset.assetType || 'stock'}`;
    // Si hay múltiples assets con el mismo key, sumar unidades SOLO de activos
    const existing = assetMap.get(assetKey);
    if (existing) {
      // Solo sumar si el asset es activo
      if (asset.isActive) {
        existing.units += asset.units || 0;
        existing.assetId = asset.id;
        existing.isActive = true;
      }
    } else {
      assetMap.set(assetKey, {
        // Solo contar unidades si es activo
        units: asset.isActive ? (asset.units || 0) : 0,
        assetName: asset.name,
        assetType: asset.assetType || 'stock',
        assetId: asset.id,
        currency: asset.currency || 'USD',
        market: asset.market,
        isActive: asset.isActive,
        unitValue: asset.unitValue
      });
    }
  });
  
  // Encontrar diferencias
  const differences = [];
  
  // Assets que están en assets pero no coinciden con transacciones
  assetMap.forEach((asset, assetKey) => {
    const txHolding = txHoldings.get(assetKey);
    const txUnits = txHolding?.units || 0;
    const assetUnits = asset.units;
    
    const diff = assetUnits - txUnits;
    
    if (Math.abs(diff) > CONFIG.MIN_DIFFERENCE_THRESHOLD) {
      differences.push({
        assetKey,
        assetName: asset.assetName,
        assetType: asset.assetType,
        assetId: asset.assetId,
        currency: asset.currency,
        market: asset.market,
        unitValue: asset.unitValue,
        isActive: asset.isActive,
        unitsFromTx: txUnits,
        unitsInAsset: assetUnits,
        difference: diff,
        action: diff > 0 ? 'BUY' : 'SELL',
        accountId: account.id,
        accountName: account.name
      });
    }
  });
  
  // Assets que están en transacciones pero no en assets (vendidos completamente)
  txHoldings.forEach((holding, assetKey) => {
    if (!assetMap.has(assetKey) && holding.units > CONFIG.MIN_DIFFERENCE_THRESHOLD) {
      // Hay unidades en transacciones pero no existe el asset
      // Esto significa que el asset fue eliminado pero las transacciones dicen que debería existir
      differences.push({
        assetKey,
        assetName: holding.assetName,
        assetType: holding.assetType,
        assetId: holding.assetId,
        currency: holding.currency,
        market: holding.market,
        isActive: false,
        unitsFromTx: holding.units,
        unitsInAsset: 0,
        difference: -holding.units, // Necesitamos SELL para eliminar
        action: 'SELL',
        accountId: account.id,
        accountName: account.name,
        note: 'Asset no existe, crear SELL para eliminar de transacciones'
      });
    }
  });
  
  return {
    account,
    totalTransactions: transactions.length,
    totalAssets: assets.length,
    txHoldings,
    assetMap,
    differences
  };
}

// ============================================================================
// GENERACIÓN DE TRANSACCIONES DE AJUSTE
// ============================================================================

async function generateAdjustmentTransactions(differences, options, txHoldingsMap) {
  const transactions = [];
  
  for (const diff of differences) {
    const isBuy = diff.difference > 0;
    const amount = Math.abs(diff.difference);
    
    // Usar el precio de la última transacción del activo
    // Pero la fecha SIEMPRE es la fecha de ajuste (--date)
    const txHolding = txHoldingsMap.get(diff.assetKey);
    let price = txHolding?.lastPrice || diff.unitValue || 100;
    let txDate = options.adjustmentDate; // SIEMPRE usar la fecha de ajuste
    let priceSource = txHolding?.lastPrice ? `última tx (${txHolding.lastPriceDate})` : 'estimado';
    
    // Estructura base común para BUY y SELL
    const baseTx = {
      // Campos esenciales (orden alfabético como en Firestore)
      amount: amount,
      assetId: diff.assetId || null,
      assetName: diff.assetName,
      assetType: diff.assetType,
      commission: 0,
      createdAt: admin.firestore.Timestamp.now(),
      currency: diff.currency || 'USD',
      date: txDate, // USAR FECHA DE AJUSTE (--date)
      defaultCurrencyForAdquisitionDollar: 'USD',
      dollarPriceToDate: 1,
      market: diff.market || 'UNKNOWN',
      portfolioAccountId: diff.accountId,
      price: price,
      type: isBuy ? 'buy' : 'sell',
      userId: options.userId,
    };
    
    // Para SELL, agregar campos de P&L
    if (!isBuy) {
      baseTx.closedPnL = true; // boolean como en el ejemplo
      baseTx.valuePnL = 0; // P&L realizado = 0 para ajustes
    }
    
    transactions.push({
      tx: baseTx,
      diff,
      priceSource,
      txDate,
      description: `${isBuy ? 'BUY' : 'SELL'} ${amount.toFixed(4)} ${diff.assetName} @ $${price.toFixed(2)}`
    });
  }
  
  return transactions;
}

// ============================================================================
// PROCESO PRINCIPAL
// ============================================================================

async function main() {
  const options = parseArgs();
  
  console.log('');
  console.log('═'.repeat(70));
  console.log('  SINCRONIZACIÓN DE TRANSACCIONES CON ASSETS');
  console.log('═'.repeat(70));
  console.log('');
  log('INFO', 'Modo: ' + options.mode.toUpperCase());
  log('INFO', 'Usuario: ' + options.userId);
  log('INFO', 'Fecha de ajuste: ' + options.adjustmentDate);
  console.log('');
  
  // 1. Obtener cuentas del usuario
  log('PROGRESS', 'Obteniendo cuentas del usuario...');
  const accounts = await getUserAccounts(options.userId);
  log('SUCCESS', `Encontradas ${accounts.length} cuentas activas`);
  
  // 2. Analizar cada cuenta
  const allDifferences = [];
  const allSymbols = new Set();
  const allTxHoldings = new Map(); // Mapa global de holdings por assetKey
  
  for (const account of accounts) {
    log('PROGRESS', `Analizando cuenta: ${account.name}...`);
    const analysis = await analyzeAccount(account, options);
    
    if (analysis.differences.length > 0) {
      log('WARNING', `  ${analysis.differences.length} diferencias encontradas`);
      allDifferences.push(...analysis.differences);
      analysis.differences.forEach(d => allSymbols.add(d.assetName));
    } else {
      log('SUCCESS', `  Sin diferencias`);
    }
    
    // Guardar holdings de transacciones para obtener último precio
    analysis.txHoldings.forEach((holding, key) => {
      allTxHoldings.set(key, holding);
    });
  }
  
  console.log('');
  console.log('─'.repeat(70));
  console.log('');
  
  // 3. Mostrar resumen de diferencias
  if (allDifferences.length === 0) {
    log('SUCCESS', '¡No hay diferencias! Las transacciones están sincronizadas con los assets.');
    process.exit(0);
  }
  
  log('WARNING', `Total de diferencias encontradas: ${allDifferences.length}`);
  console.log('');
  
  // Mostrar tabla de diferencias
  console.log('DIFERENCIAS ENCONTRADAS:');
  console.log('');
  console.log(
    'Cuenta'.padEnd(12) + ' | ' +
    'Asset'.padEnd(18) + ' | ' +
    'Tx Units'.padEnd(10) + ' | ' +
    'Asset Units'.padEnd(12) + ' | ' +
    'Diff'.padEnd(10) + ' | ' +
    'Acción'
  );
  console.log('─'.repeat(85));
  
  allDifferences.forEach(diff => {
    console.log(
      diff.accountName.substring(0, 11).padEnd(12) + ' | ' +
      (diff.assetKey.substring(0, 17)).padEnd(18) + ' | ' +
      diff.unitsFromTx.toFixed(4).padEnd(10) + ' | ' +
      diff.unitsInAsset.toFixed(4).padEnd(12) + ' | ' +
      (diff.difference > 0 ? '+' : '') + diff.difference.toFixed(4).padEnd(9) + ' | ' +
      diff.action
    );
    if (diff.note) {
      console.log('  └─ ' + diff.note);
    }
  });
  
  console.log('');
  
  // Si es solo análisis, terminar aquí
  if (options.mode === 'analyze') {
    console.log('─'.repeat(70));
    log('INFO', 'Modo ANALYZE completado. Para ver transacciones a crear:');
    console.log('    node syncTransactionsWithAssets.js --dry-run');
    process.exit(0);
  }
  
  // 4. Generar transacciones de ajuste (usando último precio de transacciones)
  log('PROGRESS', 'Generando transacciones de ajuste con precios de última transacción...');
  const adjustmentTxs = await generateAdjustmentTransactions(allDifferences, options, allTxHoldings);
  log('SUCCESS', `${adjustmentTxs.length} transacciones generadas`);
  
  console.log('');
  console.log('─'.repeat(70));
  console.log('');
  console.log('TRANSACCIONES DE AJUSTE A CREAR:');
  console.log('');
  
  adjustmentTxs.forEach((item, i) => {
    console.log(`${i + 1}. ${item.description}`);
    console.log(`   Cuenta: ${item.diff.accountName}`);
    console.log(`   Tipo: ${item.tx.type.toUpperCase()}`);
    console.log(`   Asset: ${item.tx.assetName}_${item.tx.assetType}`);
    console.log(`   Unidades: ${item.tx.amount.toFixed(4)}`);
    console.log(`   Precio: $${item.tx.price.toFixed(2)} (${item.priceSource})`);
    console.log(`   Fecha TX: ${item.txDate}`);
    if (!item.diff.difference > 0) {
      console.log(`   P&L: closedPnL=${item.tx.closedPnL}, valuePnL=${item.tx.valuePnL}`);
    }
    console.log('');
  });
  
  // Si es dry-run, terminar aquí
  if (options.mode === 'dry-run') {
    console.log('─'.repeat(70));
    log('INFO', 'Modo DRY-RUN completado. Para aplicar cambios:');
    console.log('    node syncTransactionsWithAssets.js --fix');
    process.exit(0);
  }
  
  // 6. Modo FIX: Crear transacciones
  if (options.mode === 'fix') {
    console.log('─'.repeat(70));
    log('PROGRESS', 'Creando transacciones de ajuste...');
    
    let created = 0;
    let errors = 0;
    
    for (const item of adjustmentTxs) {
      try {
        const docRef = await db.collection('transactions').add(item.tx);
        log('SUCCESS', `  Creada: ${item.description} (ID: ${docRef.id})`);
        created++;
      } catch (error) {
        log('ERROR', `  Error creando ${item.description}: ${error.message}`);
        errors++;
      }
    }
    
    console.log('');
    console.log('─'.repeat(70));
    log('SUCCESS', `Transacciones creadas: ${created}`);
    if (errors > 0) {
      log('ERROR', `Errores: ${errors}`);
    }
    
    console.log('');
    log('INFO', 'Próximos pasos:');
    console.log('    1. Borrar documentos de portfolioPerformance de enero 2026');
    console.log('    2. Ejecutar backfill: node backfillPortfolioPerformance.js --fix --start=2026-01-02 --end=2026-01-21');
    console.log('    3. Verificar que las unidades coincidan');
  }
  
  console.log('');
  process.exit(0);
}

// Ejecutar
main().catch(error => {
  log('ERROR', 'Error fatal: ' + error.message);
  console.error(error.stack);
  process.exit(1);
});
