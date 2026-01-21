/**
 * Performance On-Demand Handler
 * 
 * Calcula el rendimiento actual del portafolio usando precios en tiempo real
 * del API Lambda, en lugar de datos cacheados en Firestore.
 * 
 * OPT-DEMAND-102: Implementa cálculo de rendimiento con precios live.
 * 
 * @module handlers/performanceOnDemandHandler
 * @see docs/stories/74.story.md (OPT-DEMAND-102)
 * @see docs/architecture/on-demand-pricing-architecture.md
 */

const { HttpsError } = require("firebase-functions/v2/https");
const admin = require('../firebaseAdmin');
const db = admin.firestore();
// SEC-CF-001: Configuración centralizada de URLs y headers
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('../config');

// ============================================================================
// CONFIGURACIÓN
// ============================================================================

// SEC-CF-001: Usar URL centralizada (sin /v1 al final)
const FINANCE_QUERY_API = FINANCE_QUERY_API_URL.replace('/v1', '');

// Cache simple en memoria (1 minuto TTL)
const cache = new Map();
const CACHE_TTL_MS = 60 * 1000; // 1 minuto

// ============================================================================
// FUNCIONES AUXILIARES
// ============================================================================

/**
 * Obtiene precios en tiempo real del API Lambda
 * 
 * @param {string[]} symbols - Símbolos de activos (ej: ['AAPL', 'VOO'])
 * @param {string[]} currencyCodes - Códigos de moneda (ej: ['COP', 'EUR'])
 * @returns {Promise<{prices: Object, currencies: Object, timestamp: number}>}
 */
async function fetchLivePrices(symbols, currencyCodes) {
  const ADMIN_API_KEY = process.env.ADMIN_API_KEY;
  
  if (!ADMIN_API_KEY) {
    throw new HttpsError('internal', 'API key no configurada');
  }
  
  // Construir lista de símbolos (activos + currencies)
  const allSymbols = [
    ...symbols,
    ...currencyCodes.filter(c => c !== 'USD').map(c => `${c}=X`)
  ];
  
  if (allSymbols.length === 0) {
    return { prices: {}, currencies: { USD: 1 }, timestamp: Date.now() };
  }
  
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000);
    
    // SEC-TOKEN-004: Usar headers con token de servicio
    const response = await fetch(
      `${FINANCE_QUERY_API}/v1/market-quotes?symbols=${allSymbols.join(',')}`,
      { 
        headers: getServiceHeaders({
          'Accept': 'application/json'
        }),
        signal: controller.signal
      }
    );
    
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    // Separar precios y currencies
    const prices = {};
    const currencies = { USD: 1 }; // USD siempre es 1
    
    if (Array.isArray(data)) {
      data.forEach(item => {
        if (!item || !item.symbol) return;
        
        if (item.symbol.includes('=X')) {
          // Es una divisa (ej: COP=X)
          const code = item.symbol.replace('=X', '');
          currencies[code] = item.regularMarketPrice || 1;
        } else {
          // Es un activo (ej: AAPL)
          prices[item.symbol] = {
            price: item.regularMarketPrice || 0,
            change: item.regularMarketChange || 0,
            changePercent: item.regularMarketChangePercent || 0,
            previousClose: item.regularMarketPreviousClose || item.regularMarketPrice || 0,
            currency: item.currency || 'USD',
            timestamp: item.regularMarketTime || Math.floor(Date.now() / 1000)
          };
        }
      });
    }
    
    return { prices, currencies, timestamp: Date.now() };
    
  } catch (error) {
    if (error.name === 'AbortError') {
      console.error('[fetchLivePrices] Timeout');
      throw new HttpsError('deadline-exceeded', 'Timeout obteniendo precios de mercado');
    }
    console.error('[fetchLivePrices] Error:', error.message);
    throw new HttpsError('internal', 'Error obteniendo precios de mercado');
  }
}

/**
 * Obtiene las cuentas del usuario
 * 
 * @param {string} userId - ID del usuario
 * @param {string} [accountId] - ID de cuenta específica
 * @param {string[]} [accountIds] - IDs de múltiples cuentas
 * @returns {Promise<Array>}
 */
async function getUserAccounts(userId, accountId, accountIds) {
  let query = db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true);
  
  const snapshot = await query.get();
  
  if (snapshot.empty) {
    return [];
  }
  
  let accounts = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  
  // Filtrar por cuenta específica si se proporciona
  if (accountId) {
    accounts = accounts.filter(a => a.id === accountId);
    if (accounts.length === 0) {
      throw new HttpsError('not-found', 'Cuenta no encontrada');
    }
  }
  
  // Filtrar por múltiples cuentas si se proporcionan
  if (accountIds && accountIds.length > 0) {
    accounts = accounts.filter(a => accountIds.includes(a.id));
  }
  
  return accounts;
}

/**
 * Obtiene los assets de las cuentas especificadas
 * 
 * @param {string[]} accountIds - IDs de cuentas
 * @returns {Promise<Array>}
 */
async function getAssetsByAccounts(accountIds) {
  if (accountIds.length === 0) return [];
  
  const assets = [];
  
  // Firestore 'in' tiene límite de 30 elementos
  for (let i = 0; i < accountIds.length; i += 30) {
    const batchIds = accountIds.slice(i, i + 30);
    
    const snapshot = await db.collection('assets')
      .where('portfolioAccount', 'in', batchIds)
      .where('isActive', '==', true)
      .get();
    
    snapshot.docs.forEach(doc => {
      assets.push({ id: doc.id, ...doc.data() });
    });
  }
  
  return assets;
}

/**
 * Obtiene las currencies activas del usuario
 * 
 * @param {string} userId - ID del usuario
 * @returns {Promise<string[]>}
 */
async function getUserCurrencies(userId) {
  const snapshot = await db.collection('currencies')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => doc.data().code);
}

/**
 * Convierte un valor de una moneda a otra
 * 
 * @param {number} value - Valor a convertir
 * @param {string} fromCurrency - Moneda origen
 * @param {string} toCurrency - Moneda destino
 * @param {Object} currencies - Tasas de cambio (currency -> USD rate)
 * @returns {number}
 */
function convertCurrency(value, fromCurrency, toCurrency, currencies) {
  if (fromCurrency === toCurrency) return value;
  if (!value || isNaN(value)) return 0;
  
  // Si toCurrency es USD, dividir por la tasa de la moneda origen
  if (toCurrency === 'USD') {
    const rate = currencies[fromCurrency] || 1;
    return value / rate;
  }
  
  // Si fromCurrency es USD, multiplicar por la tasa de la moneda destino
  if (fromCurrency === 'USD') {
    const rate = currencies[toCurrency] || 1;
    return value * rate;
  }
  
  // Cross-currency: convertir primero a USD, luego a destino
  const toUSD = value / (currencies[fromCurrency] || 1);
  return toUSD * (currencies[toCurrency] || 1);
}

/**
 * Calcula el rendimiento del portafolio
 * 
 * @param {Array} assets - Lista de assets
 * @param {Array} accounts - Lista de cuentas
 * @param {Object} prices - Precios por símbolo
 * @param {Object} currencies - Tasas de cambio
 * @param {string} targetCurrency - Moneda objetivo
 * @returns {Object}
 */
function calculatePerformance(assets, accounts, prices, currencies, targetCurrency) {
  let totalValue = 0;
  let totalInvestment = 0;
  let totalCashFlow = 0;
  let cashBalance = 0;
  let realizedPnL = 0;
  let dailyChange = 0;
  let previousTotalValue = 0;
  
  // Calcular balance de efectivo de las cuentas
  accounts.forEach(account => {
    const balances = account.balances || {};
    Object.entries(balances).forEach(([currency, amount]) => {
      if (typeof amount === 'number') {
        cashBalance += convertCurrency(amount, currency, targetCurrency, currencies);
      }
    });
  });
  
  // Calcular valor de los assets
  assets.forEach(asset => {
    const priceData = prices[asset.name];
    const assetCurrency = asset.currency || 'USD';
    
    if (!priceData) {
      // Si no hay precio, usar el precio promedio como fallback
      const value = (asset.units || 0) * (asset.averagePrice || 0);
      const investment = (asset.units || 0) * (asset.averagePrice || 0);
      
      totalValue += convertCurrency(value, assetCurrency, targetCurrency, currencies);
      totalInvestment += convertCurrency(investment, assetCurrency, targetCurrency, currencies);
      previousTotalValue += convertCurrency(value, assetCurrency, targetCurrency, currencies);
      return;
    }
    
    const currentPrice = priceData.price;
    const previousClose = priceData.previousClose || currentPrice;
    const priceCurrency = priceData.currency || 'USD';
    
    // Valores en moneda del precio
    const units = asset.units || 0;
    const value = units * currentPrice;
    const previousValue = units * previousClose;
    
    // Inversión en moneda del asset
    const investment = units * (asset.averagePrice || 0);
    
    // Convertir a moneda objetivo
    const valueInTarget = convertCurrency(value, priceCurrency, targetCurrency, currencies);
    const investmentInTarget = convertCurrency(investment, assetCurrency, targetCurrency, currencies);
    const previousValueInTarget = convertCurrency(previousValue, priceCurrency, targetCurrency, currencies);
    
    totalValue += valueInTarget;
    totalInvestment += investmentInTarget;
    previousTotalValue += previousValueInTarget;
    dailyChange += (valueInTarget - previousValueInTarget);
    
    // Sumar cashflow del asset
    totalCashFlow += convertCurrency(asset.totalCashFlow || 0, assetCurrency, targetCurrency, currencies);
    
    // Sumar PnL realizado del asset
    realizedPnL += convertCurrency(asset.doneProfitAndLoss || 0, assetCurrency, targetCurrency, currencies);
  });
  
  // Agregar cash balance al valor total
  totalValue += cashBalance;
  
  // Calcular unrealized PnL (valor de assets - inversión)
  const assetsValue = totalValue - cashBalance;
  const unrealizedPnL = assetsValue - totalInvestment;
  const unrealizedPnLPercent = totalInvestment > 0 
    ? (unrealizedPnL / totalInvestment) * 100 
    : 0;
  
  // Calcular cambio diario porcentual
  const dailyChangePercent = previousTotalValue > 0 
    ? (dailyChange / previousTotalValue) * 100 
    : 0;
  
  return {
    totalValue: Math.round(totalValue * 100) / 100,
    totalInvestment: Math.round(totalInvestment * 100) / 100,
    totalCashFlow: Math.round(totalCashFlow * 100) / 100,
    cashBalance: Math.round(cashBalance * 100) / 100,
    unrealizedPnL: Math.round(unrealizedPnL * 100) / 100,
    unrealizedPnLPercent: Math.round(unrealizedPnLPercent * 100) / 100,
    realizedPnL: Math.round(realizedPnL * 100) / 100,
    dailyChange: Math.round(dailyChange * 100) / 100,
    dailyChangePercent: Math.round(dailyChangePercent * 100) / 100
  };
}

// ============================================================================
// HANDLER PRINCIPAL
// ============================================================================

/**
 * Handler para obtener rendimiento on-demand
 * 
 * @param {Object} context - Contexto con auth
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>}
 */
async function getPerformanceOnDemand(context, payload) {
  const { auth } = context;
  const { 
    currency = 'USD', 
    accountId, 
    accountIds, 
    forceRefresh = false 
  } = payload || {};
  
  if (!auth) {
    throw new HttpsError('unauthenticated', 'Autenticación requerida');
  }
  
  const userId = auth.uid;
  const startTime = Date.now();
  
  console.log('[getPerformanceOnDemand] Start', { 
    userId, 
    currency, 
    accountId: accountId || 'all' 
  });
  
  // 1. Verificar cache
  const cacheKey = `perf:${userId}:${currency}:${accountId || 'all'}:${accountIds?.join(',') || ''}`;
  
  if (!forceRefresh) {
    const cached = cache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
      console.log('[getPerformanceOnDemand] Cache hit');
      return { ...cached.data, fromCache: true };
    }
  }
  
  try {
    // 2. Obtener cuentas del usuario
    const accounts = await getUserAccounts(userId, accountId, accountIds);
    
    if (accounts.length === 0) {
      return {
        totalValue: 0,
        totalInvestment: 0,
        totalCashFlow: 0,
        cashBalance: 0,
        unrealizedPnL: 0,
        unrealizedPnLPercent: 0,
        realizedPnL: 0,
        dailyChange: 0,
        dailyChangePercent: 0,
        currency,
        timestamp: Date.now(),
        pricesTimestamp: 0,
        fromCache: false,
        assetCount: 0,
        accountCount: 0
      };
    }
    
    const accountIdsList = accounts.map(a => a.id);
    
    // 3. Obtener assets
    const assets = await getAssetsByAccounts(accountIdsList);
    
    // 4. Obtener símbolos únicos y currencies
    const symbols = [...new Set(assets.map(a => a.name).filter(Boolean))];
    const userCurrencies = await getUserCurrencies(userId);
    
    // Agregar la moneda objetivo si no está
    if (!userCurrencies.includes(currency) && currency !== 'USD') {
      userCurrencies.push(currency);
    }
    
    // 5. Obtener precios en tiempo real
    const { prices, currencies, timestamp: pricesTimestamp } = await fetchLivePrices(
      symbols, 
      userCurrencies
    );
    
    // 6. Calcular rendimiento
    const performance = calculatePerformance(
      assets, 
      accounts, 
      prices, 
      currencies, 
      currency
    );
    
    // 7. Construir respuesta
    const result = {
      ...performance,
      currency,
      timestamp: Date.now(),
      pricesTimestamp,
      fromCache: false,
      assetCount: assets.length,
      accountCount: accounts.length
    };
    
    // 8. Guardar en cache
    cache.set(cacheKey, { data: result, timestamp: Date.now() });
    
    const duration = Date.now() - startTime;
    console.log('[getPerformanceOnDemand] Complete', { 
      userId, 
      duration: `${duration}ms`, 
      assetCount: assets.length,
      accountCount: accounts.length 
    });
    
    return result;
    
  } catch (error) {
    console.error('[getPerformanceOnDemand] Error:', error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', 'Error calculando rendimiento');
  }
}

/**
 * Limpia el cache (útil para testing)
 */
function clearCache() {
  cache.clear();
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  getPerformanceOnDemand,
  // Exportar para testing
  _fetchLivePrices: fetchLivePrices,
  _getUserAccounts: getUserAccounts,
  _getAssetsByAccounts: getAssetsByAccounts,
  _getUserCurrencies: getUserCurrencies,
  _calculatePerformance: calculatePerformance,
  _convertCurrency: convertCurrency,
  _cache: cache,
  _clearCache: clearCache
};
