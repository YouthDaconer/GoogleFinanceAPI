/**
 * Cloud Function Callable para obtener precios filtrados por usuario
 * 
 * OPT-DEMAND-CLEANUP: Migrado para usar API Lambda en lugar de Firestore.
 * 
 * Esta función optimiza las lecturas retornando solo los precios de los
 * símbolos que el usuario posee en sus portfolioAccounts.
 * 
 * @module userPricesService
 * @see docs/stories/6.story.md (OPT-001)
 * @see docs/architecture/OPT-DEMAND-CLEANUP-phase4-closure-subplan.md
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('./firebaseAdmin');
const db = admin.firestore();

// OPT-DEMAND-CLEANUP: Importar helper para obtener precios del API Lambda
const { getPricesFromApi } = require('./marketDataHelper');

// Importar rate limiter (SCALE-BE-004)
const { withRateLimit } = require('../utils/rateLimiter');

/**
 * Configuración común para Cloud Functions Callable
 */
const callableConfig = {
  cors: true,
  enforceAppCheck: false,
  timeoutSeconds: 60,
  memory: "256MiB",
};

/**
 * Valida que el usuario esté autenticado
 * @param {object} auth - Objeto de autenticación de Firebase
 * @throws {HttpsError} Si no hay autenticación
 */
const validateAuth = (auth) => {
  if (!auth) {
    throw new HttpsError(
      'unauthenticated',
      'Debes iniciar sesión para obtener los precios'
    );
  }
};

/**
 * Cloud Function: getCurrentPricesForUser
 * 
 * Obtiene los precios actuales solo para los símbolos que el usuario posee.
 * 
 * Flujo:
 * 1. Verificar autenticación
 * 2. Obtener portfolioAccounts activas del usuario
 * 3. Obtener símbolos únicos de assets activos
 * 4. Batch get de precios para esos símbolos
 * 
 * @param {object} request - Objeto de request de Firebase
 * @param {object} request.auth - Información de autenticación
 * @returns {Promise<{prices: Array, symbols: Array, timestamp: number}>}
 */
const getCurrentPricesForUser = onCall(callableConfig, withRateLimit('getCurrentPricesForUser')(async (request) => {
  const { auth } = request;
  
  console.log(`[getCurrentPricesForUser] Iniciando - userId: ${auth?.uid}`);
  
  try {
    // 1. Verificar autenticación
    validateAuth(auth);
    const userId = auth.uid;
    
    // 2. Obtener portfolioAccounts activas del usuario
    const accountsSnapshot = await db.collection('portfolioAccounts')
      .where('userId', '==', userId)
      .where('isActive', '==', true)
      .get();
    
    if (accountsSnapshot.empty) {
      console.log(`[getCurrentPricesForUser] Usuario sin cuentas activas`);
      return { prices: [], symbols: [], timestamp: Date.now() };
    }
    
    const accountIds = accountsSnapshot.docs.map(doc => doc.id);
    console.log(`[getCurrentPricesForUser] Cuentas encontradas: ${accountIds.length}`);
    
    // 3. Obtener símbolos únicos de assets activos
    // Nota: Firestore 'in' query tiene límite de 10 elementos, usar batches
    const symbolsSet = new Set();
    
    for (let i = 0; i < accountIds.length; i += 10) {
      const batchAccountIds = accountIds.slice(i, i + 10);
      
      const assetsSnapshot = await db.collection('assets')
        .where('portfolioAccount', 'in', batchAccountIds)
        .where('isActive', '==', true)
        .get();
      
      assetsSnapshot.docs.forEach(doc => {
        const name = doc.data().name;
        if (name) {
          symbolsSet.add(name);
        }
      });
    }
    
    const symbols = Array.from(symbolsSet);
    
    if (symbols.length === 0) {
      console.log(`[getCurrentPricesForUser] Usuario sin assets activos`);
      return { prices: [], symbols: [], timestamp: Date.now() };
    }
    
    console.log(`[getCurrentPricesForUser] Símbolos únicos: ${symbols.length}`);
    
    // OPT-DEMAND-CLEANUP: Obtener precios del API Lambda en lugar de Firestore
    console.log(`[getCurrentPricesForUser] Consultando API Lambda para ${symbols.length} símbolos`);
    
    const prices = await getPricesFromApi(symbols);
    
    // Formatear respuesta para mantener compatibilidad con el frontend
    const formattedPrices = prices.map(price => ({
      id: price.symbol,
      symbol: price.symbol,
      ...price
    }));
    
    console.log(`[getCurrentPricesForUser] Éxito - precios: ${formattedPrices.length}, símbolos: ${symbols.length}, fuente: API Lambda`);
    
    return {
      prices: formattedPrices,
      symbols,
      timestamp: Date.now(),
      source: 'api-lambda'  // OPT-DEMAND-CLEANUP: Indicar fuente de datos
    };
    
  } catch (error) {
    console.error(`[getCurrentPricesForUser] Error - userId: ${auth?.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError(
      'internal',
      'Error al obtener los precios del usuario',
      { originalError: error.message }
    );
  }
}));

module.exports = {
  getCurrentPricesForUser
};
