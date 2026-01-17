/**
 * Daily EOD Snapshot - Captura de precios End-Of-Day
 * 
 * @deprecated OPT-DEMAND-CLEANUP: Esta función está DEPRECADA desde 2026-01-16.
 * 
 * La funcionalidad de cálculos EOD ahora está consolidada en:
 * - unifiedMarketDataUpdate (ejecuta 1x/día a las 17:05 ET)
 * 
 * RAZÓN: Esta función escribía a `currentPrices` y `currencies` en Firestore,
 * lo cual es innecesario ya que ahora los datos vienen del API Lambda on-demand.
 * 
 * Esta función se mantiene temporalmente deshabilitada para posible rollback.
 * Se eliminará completamente después de 2 semanas de estabilidad.
 * 
 * @module services/dailyEODSnapshot
 * @see docs/stories/84.story.md (OPT-DEMAND-301)
 * @see docs/architecture/OPT-DEMAND-CLEANUP-phase4-closure-subplan.md
 */

const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('firebase-admin');
const axios = require('axios');
const { DateTime } = require('luxon');
const { StructuredLogger } = require('../utils/logger');

// ============================================================================
// Configuration
// ============================================================================

const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

// Tamaño de lote para consultas al API
const BATCH_SIZE = 100;

// Límite de documentos por batch write en Firestore
const FIRESTORE_BATCH_LIMIT = 500;

// Logger para este módulo
let logger = null;

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Determina el tipo de snapshot basándose en la hora actual.
 * @returns {'pre-market' | 'post-market' | 'unknown'}
 */
function getSnapshotType() {
  const nyNow = DateTime.now().setZone('America/New_York');
  const hour = nyNow.hour;
  
  // Pre-market: 7-10 AM
  if (hour >= 7 && hour < 10) {
    return 'pre-market';
  }
  
  // Post-market: 4-7 PM
  if (hour >= 16 && hour < 19) {
    return 'post-market';
  }
  
  return 'unknown';
}

/**
 * Obtiene todos los símbolos únicos de currentPrices.
 * @param {FirebaseFirestore.Firestore} db 
 * @returns {Promise<string[]>}
 */
async function getAllSymbols(db) {
  const snapshot = await db.collection('currentPrices').get();
  const symbols = snapshot.docs
    .map(doc => doc.data().symbol)
    .filter(Boolean);
  
  return [...new Set(symbols)]; // Únicos
}

/**
 * Obtiene todas las currencies activas.
 * @param {FirebaseFirestore.Firestore} db 
 * @returns {Promise<Array<{code: string, ref: FirebaseFirestore.DocumentReference, data: object}>>}
 */
async function getActiveCurrencies(db) {
  const snapshot = await db.collection('currencies')
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => ({
    code: doc.data().code,
    ref: doc.ref,
    data: doc.data()
  }));
}

/**
 * Consulta el API Lambda para obtener quotes de múltiples símbolos.
 * @param {string[]} symbols - Lista de símbolos (incluye currencies como COP=X)
 * @returns {Promise<Map<string, object>>}
 */
async function fetchQuotesFromAPI(symbols) {
  const quotes = new Map();
  
  if (symbols.length === 0) {
    return quotes;
  }
  
  if (logger) {
    logger.info(`Fetching ${symbols.length} symbols from API Lambda`, {
      batches: Math.ceil(symbols.length / BATCH_SIZE)
    });
  }
  
  // Procesar en lotes
  for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
    const batch = symbols.slice(i, i + BATCH_SIZE);
    const symbolsParam = batch.join(',');
    
    try {
      const url = `${API_BASE_URL}/market-quotes?symbols=${symbolsParam}`;
      const response = await axios.get(url, { timeout: 30000 });
      
      if (Array.isArray(response.data)) {
        response.data.forEach(item => {
          if (item.symbol && item.regularMarketPrice) {
            quotes.set(item.symbol, item);
          }
        });
      }
      
      if (logger) {
        logger.debug(`Batch ${Math.floor(i/BATCH_SIZE) + 1} completed`, {
          requested: batch.length,
          received: response.data?.length || 0
        });
      }
      
    } catch (error) {
      if (logger) {
        logger.error(`Error fetching batch ${Math.floor(i/BATCH_SIZE) + 1}`, error, {
          symbols: batch.slice(0, 5) // Solo primeros 5 para log
        });
      }
      // Continuar con el siguiente batch
    }
  }
  
  if (logger) {
    logger.info(`Fetched ${quotes.size} quotes from API`);
  }
  return quotes;
}

/**
 * Actualiza la colección currentPrices con los datos del snapshot.
 * @param {FirebaseFirestore.Firestore} db 
 * @param {Map<string, object>} quotes 
 * @returns {Promise<{updated: number, failed: number}>}
 */
async function updateCurrentPricesSnapshot(db, quotes) {
  const snapshot = await db.collection('currentPrices').get();
  const now = Date.now();
  
  let updated = 0;
  let failed = 0;
  let batch = db.batch();
  let batchCount = 0;
  
  for (const doc of snapshot.docs) {
    const docData = doc.data();
    const symbol = docData.symbol;
    const quote = quotes.get(symbol);
    
    if (quote) {
      const updatedData = {
        symbol: symbol,
        price: quote.regularMarketPrice,
        lastUpdated: now,
        change: quote.regularMarketChange || 0,
        percentChange: quote.regularMarketChangePercent || 0,
        previousClose: quote.regularMarketPreviousClose || 0,
        currency: quote.currency || 'USD',
        marketState: quote.marketState || 'CLOSED',
        quoteType: quote.quoteType || docData.quoteType,
        exchange: quote.exchange || docData.exchange,
        fullExchangeName: quote.fullExchangeName || docData.fullExchangeName,
        // Preservar campos existentes
        ...(docData.name && { name: docData.name }),
        ...(docData.isin && { isin: docData.isin }),
        ...(docData.type && { type: docData.type }),
        ...(docData.logo && { logo: docData.logo }),
        ...(docData.website && { website: docData.website }),
        ...(docData.sector && { sector: docData.sector }),
        ...(docData.industry && { industry: docData.industry }),
        ...(docData.about && { about: docData.about }),
        ...(docData.country && { country: docData.country }),
        ...(docData.employees && { employees: docData.employees }),
        // Marcar como EOD snapshot
        snapshotType: 'eod',
        snapshotTimestamp: now,
      };
      
      batch.update(doc.ref, updatedData);
      updated++;
      batchCount++;
      
      // Commit batch si alcanza el límite
      if (batchCount >= FIRESTORE_BATCH_LIMIT) {
        await batch.commit();
        batch = db.batch();
        batchCount = 0;
      }
    } else {
      failed++;
    }
  }
  
  // Commit batch final
  if (batchCount > 0) {
    await batch.commit();
  }
  
  if (logger) {
    logger.info(`currentPrices updated`, { updated, failed });
  }
  return { updated, failed };
}

/**
 * Actualiza la colección currencies con los datos del snapshot.
 * @param {FirebaseFirestore.Firestore} db 
 * @param {Map<string, object>} quotes 
 * @param {Array<{code: string, ref: any, data: any}>} currencies 
 * @returns {Promise<{updated: number, failed: number}>}
 */
async function updateCurrenciesSnapshot(db, quotes, currencies) {
  if (currencies.length === 0) {
    if (logger) {
      logger.info('No active currencies to update');
    }
    return { updated: 0, failed: 0 };
  }
  
  const batch = db.batch();
  let updated = 0;
  let failed = 0;
  
  for (const currency of currencies) {
    const symbol = `${currency.code}=X`;
    const quote = quotes.get(symbol);
    
    if (quote && quote.regularMarketPrice) {
      batch.update(currency.ref, {
        exchangeRate: quote.regularMarketPrice,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        // Preservar metadata
        code: currency.data.code,
        name: currency.data.name,
        symbol: currency.data.symbol || '',
        isActive: true,
        // Marcar como EOD snapshot
        snapshotType: 'eod',
      });
      updated++;
    } else {
      failed++;
      if (logger) {
        logger.warn(`No quote for currency ${currency.code}`);
      }
    }
  }
  
  if (updated > 0) {
    await batch.commit();
  }
  
  if (logger) {
    logger.info(`currencies updated`, { updated, failed });
  }
  return { updated, failed };
}

/**
 * Actualiza systemStatus para indicar el último EOD snapshot.
 * @param {FirebaseFirestore.Firestore} db 
 * @param {object} stats 
 */
async function updateSystemStatus(db, stats) {
  const statusRef = db.collection('systemStatus').doc('eodSnapshot');
  
  await statusRef.set({
    lastRun: admin.firestore.FieldValue.serverTimestamp(),
    lastRunType: stats.type,
    pricesUpdated: stats.pricesUpdated,
    currenciesUpdated: stats.currenciesUpdated,
    executionTimeMs: stats.executionTimeMs,
    errors: stats.errors || [],
  }, { merge: true });
  
  if (logger) {
    logger.info(`systemStatus/eodSnapshot updated`);
  }
}

/**
 * Lógica principal del EOD snapshot (reutilizada por pre-market, post-market y manual)
 * @param {FirebaseFirestore.Firestore} db 
 * @param {string} type - Tipo de snapshot: 'pre-market' | 'post-market' | 'manual'
 * @returns {Promise<{pricesUpdated: number, currenciesUpdated: number, executionTimeMs: number}>}
 */
async function executeEODSnapshot(db, type) {
  const startTime = Date.now();
  const errors = [];
  
  try {
    // 1. Obtener todos los símbolos
    const symbols = await getAllSymbols(db);
    const currencies = await getActiveCurrencies(db);
    const currencyCodes = currencies.map(c => c.code);
    
    if (logger) {
      logger.info('Data to fetch', {
        symbols: symbols.length,
        currencies: currencyCodes.length
      });
    }
    
    // 2. Preparar símbolos para el API (currencies con =X)
    const currencySymbols = currencyCodes.map(code => `${code}=X`);
    const allSymbols = [...symbols, ...currencySymbols];
    
    // 3. Fetch desde API Lambda
    const quotes = await fetchQuotesFromAPI(allSymbols);
    
    // 4. Actualizar currentPrices
    const pricesResult = await updateCurrentPricesSnapshot(db, quotes);
    
    // 5. Actualizar currencies
    const currenciesResult = await updateCurrenciesSnapshot(db, quotes, currencies);
    
    // 6. Actualizar systemStatus
    const executionTimeMs = Date.now() - startTime;
    await updateSystemStatus(db, {
      type,
      pricesUpdated: pricesResult.updated,
      currenciesUpdated: currenciesResult.updated,
      executionTimeMs,
      errors
    });
    
    if (logger) {
      logger.info(`${type} EOD snapshot completed`, {
        executionTimeMs,
        pricesUpdated: pricesResult.updated,
        currenciesUpdated: currenciesResult.updated
      });
    }
    
    return {
      pricesUpdated: pricesResult.updated,
      currenciesUpdated: currenciesResult.updated,
      executionTimeMs
    };
    
  } catch (error) {
    if (logger) {
      logger.error(`${type} EOD snapshot failed`, error);
    }
    errors.push(error.message);
    
    const executionTimeMs = Date.now() - startTime;
    
    // Intentar actualizar systemStatus con error
    try {
      await updateSystemStatus(db, {
        type,
        pricesUpdated: 0,
        currenciesUpdated: 0,
        executionTimeMs,
        errors
      });
    } catch (statusError) {
      // Ignorar error de actualización de status
    }
    
    throw error;
  }
}

// ============================================================================
// Cloud Functions - Scheduled
// ============================================================================

/**
 * @deprecated OPT-DEMAND-CLEANUP: DEPRECADA - Ver unifiedMarketDataUpdate
 * 
 * Daily EOD Snapshot - Pre-Market (8:30 AM ET)
 * Schedule deshabilitado para evitar ejecución accidental.
 */
const dailyEODSnapshotPreMarket = onSchedule(
  {
    // DEPRECATED: Schedule original comentado
    // schedule: '30 8 * * 1-5', // 8:30 AM ET
    schedule: '0 0 1 1 *',  // Nunca ejecutar (1 de enero a medianoche)
    timeZone: 'America/New_York',
    memory: '512MiB',
    timeoutSeconds: 540,
    retryCount: 0,
    labels: {
      status: 'deprecated',
      deprecated: '2026-01-16',
      replacedby: 'unified-market-data-update'
    }
  },
  async (event) => {
    console.warn('⚠️ DEPRECATED: dailyEODSnapshotPreMarket ejecutada pero está deprecada');
    console.warn('La funcionalidad EOD ahora está en unifiedMarketDataUpdate (17:05 ET)');
    return null;
  }
);

/**
 * @deprecated OPT-DEMAND-CLEANUP: DEPRECADA - Ver unifiedMarketDataUpdate
 * 
 * Daily EOD Snapshot - Post-Market (5:00 PM ET)
 * Schedule deshabilitado para evitar ejecución accidental.
 */
const dailyEODSnapshotPostMarket = onSchedule(
  {
    // DEPRECATED: Schedule original comentado
    // schedule: '0 17 * * 1-5', // 5:00 PM ET
    schedule: '0 0 1 1 *',  // Nunca ejecutar (1 de enero a medianoche)
    timeZone: 'America/New_York',
    memory: '512MiB',
    timeoutSeconds: 540,
    retryCount: 0,
    labels: {
      status: 'deprecated',
      deprecated: '2026-01-16',
      replacedby: 'unified-market-data-update'
    }
  },
  async (event) => {
    console.warn('⚠️ DEPRECATED: dailyEODSnapshotPostMarket ejecutada pero está deprecada');
    console.warn('La funcionalidad EOD ahora está en unifiedMarketDataUpdate (17:05 ET)');
    return null;
  }
);

// ============================================================================
// Cloud Functions - Manual Trigger
// ============================================================================

/**
 * Manual trigger para testing (solo admins)
 * Llamar via Firebase callable function
 */
const dailyEODSnapshotManual = onCall(
  {
    memory: '512MiB',
    timeoutSeconds: 540,
    enforceAppCheck: false,
  },
  async (request) => {
    // Verificar autenticación
    const auth = request.auth;
    if (!auth) {
      throw new HttpsError('unauthenticated', 'Authentication required');
    }
    
    // Verificar que es admin (custom claim)
    if (!auth.token.admin) {
      throw new HttpsError('permission-denied', 'Admin privileges required');
    }
    
    logger = new StructuredLogger('dailyEODSnapshot', { type: 'manual' });
    logger.info('Starting manual EOD snapshot', { uid: auth.uid });
    
    const db = admin.firestore();
    
    try {
      const result = await executeEODSnapshot(db, 'manual');
      
      return {
        success: true,
        ...result
      };
      
    } catch (error) {
      logger.error('Manual EOD snapshot failed', error);
      return {
        success: false,
        error: error.message,
        executionTimeMs: 0,
      };
    }
  }
);

// ============================================================================
// Exports
// ============================================================================

module.exports = {
  dailyEODSnapshotPreMarket,
  dailyEODSnapshotPostMarket,
  dailyEODSnapshotManual,
  // Exportar helpers para testing
  getAllSymbols,
  getActiveCurrencies,
  fetchQuotesFromAPI,
  updateCurrentPricesSnapshot,
  updateCurrenciesSnapshot,
  updateSystemStatus,
  executeEODSnapshot,
  getSnapshotType,
};
