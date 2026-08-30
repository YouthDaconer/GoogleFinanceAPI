/**
 * Intraday Calculator Service
 * 
 * Calcula el rendimiento intraday del portafolio usando precios en tiempo real.
 * Replica la lógica del frontend (useIntradayPerformance + calculateIntradayChange)
 * para mantener consistencia en los cálculos de atribución.
 * 
 * INTRADAY-001: Este módulo permite que el backend incluya el rendimiento del
 * día actual en los cálculos de atribución, similar al hook useIntradayAwareReturns.
 * 
 * @module services/attribution/intradayCalculator
 * @see docs/architecture/on-demand-intraday-performance-plan.md
 * @see src/portafolio-inversiones/lib/intradayPerformance.ts (versión frontend)
 */

const admin = require('../firebaseAdmin');
const db = admin.firestore();
const { getQuotes, getExchangeRates } = require('../financeQuery');

// ============================================================================
// FUNCIONES DE DATOS
// ============================================================================

/**
 * Obtiene las cuentas activas del usuario
 * @param {string} userId - ID del usuario
 * @returns {Promise<string[]>} Array de IDs de cuentas
 */
async function getUserAccountIds(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => doc.id);
}

/**
 * Obtiene los activos activos del usuario
 * @param {string} userId - ID del usuario
 * @param {string[]} accountIds - IDs de cuentas a filtrar (['overall'] para todas)
 * @returns {Promise<Object[]>} Array de activos
 */
async function getActiveAssets(userId, accountIds = ['overall']) {
  const userAccountIds = await getUserAccountIds(userId);
  
  if (!userAccountIds.length) {
    console.log(`[IntradayCalc] No accounts found for user ${userId}`);
    return [];
  }
  
  // Determinar qué cuentas filtrar
  let targetAccountIds = userAccountIds;
  if (!accountIds.includes('overall') && !accountIds.includes('all') && accountIds.length > 0) {
    targetAccountIds = accountIds.filter(id => userAccountIds.includes(id));
  }
  
  if (!targetAccountIds.length) {
    console.log(`[IntradayCalc] No valid accounts for user ${userId}`);
    return [];
  }
  
  // Firestore permite máximo 10 valores en 'in', hacemos batch si es necesario
  const allAssets = [];
  for (let i = 0; i < targetAccountIds.length; i += 10) {
    const batch = targetAccountIds.slice(i, i + 10);
    const snapshot = await db.collection('assets')
      .where('portfolioAccount', 'in', batch)
      .where('isActive', '==', true)
      .get();
    
    allAssets.push(...snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() })));
  }
  
  console.log(`[IntradayCalc] Found ${allAssets.length} active assets for user ${userId}`);
  return allAssets;
}

/**
 * Obtiene los códigos de monedas activas desde Firestore
 * @returns {Promise<string[]>} Array de códigos de moneda (ej: ['COP', 'EUR', 'MXN'])
 */
async function getActiveCurrencyCodes() {
  const snapshot = await db.collection('currencies')
    .where('isActive', '==', true)
    .get();
  
  const codes = [];
  snapshot.docs.forEach(doc => {
    const data = doc.data();
    if (data.code && data.code !== 'USD') {
      codes.push(data.code);
    }
  });
  
  console.log(`[IntradayCalc] Active currency codes: ${codes.join(', ')}`);
  return codes;
}

/**
 * Obtiene las tasas de cambio vigentes desde el canal de datos de mercado.
 *
 * HU #3: las tasas llegan por el mismo canal que los precios de los activos
 * (RN-3-E) y **no hay caída a `currencies` de Firestore**. Ese campo se mantiene
 * a mano y nadie lo refresca: usarlo como reserva convertía una ausencia en una
 * cifra creíble, que es exactamente lo que producía un efecto divisa falso.
 *
 * Una divisa sin tasa queda **fuera del mapa**. Quien la necesite verá que falta
 * en lugar de recibir un número inventado (RN-3-D).
 *
 * @returns {Promise<Object>} Mapa de código de moneda -> { code, exchangeRate }
 * @see platform-docs/stories/3-tasa-vigente-canal-mercado/refinamiento.md (T8, D7)
 */
async function getCurrencyRates() {
  // 1. Obtener monedas activas desde Firestore (metadata del catálogo, no tasas)
  const currencyCodes = await getActiveCurrencyCodes();

  const currencies = {};

  // USD siempre tiene tasa 1: es la base, no un dato consultado
  currencies['USD'] = { code: 'USD', exchangeRate: 1 };

  if (currencyCodes.length === 0) {
    console.log(`[IntradayCalc] No active currencies found, using only USD`);
    return currencies;
  }

  try {
    console.log(`[IntradayCalc] Fetching currency rates for: ${currencyCodes.join(',')}`);

    const response = await getExchangeRates(currencyCodes);

    const rates = (response && response.rates) || {};

    for (const code of currencyCodes) {
      const raw = rates[code];
      const parsed = typeof raw === 'string' ? parseFloat(raw) : raw;

      if (typeof parsed === 'number' && Number.isFinite(parsed) && parsed > 0) {
        currencies[code] = { code, exchangeRate: parsed };
        console.log(`[IntradayCalc] ${code}: ${parsed}`);
      } else {
        console.warn(`[IntradayCalc] RATE-MISS: sin tasa vigente para ${code}; queda sin valorar`);
      }
    }

    const unavailable = (response && response.unavailable) || [];
    if (unavailable.length > 0) {
      console.warn(`[IntradayCalc] El canal declaró no disponibles: ${unavailable.join(', ')}`);
    }

  } catch (error) {
    // Sin tasas del canal no se sustituye por las persistidas: el cálculo del día
    // se queda con las divisas que sí llegaron (RN-3-D)
    console.error(`[IntradayCalc] El canal de mercado no devolvió tasas de cambio:`, error.message);
  }

  console.log(`[IntradayCalc] Loaded ${Object.keys(currencies).length} currencies with rates`);
  return currencies;
}

/**
 * Obtiene los datos del día anterior de portfolioPerformance
 * @param {string} userId - ID del usuario
 * @param {string} accountId - ID de cuenta o 'overall'
 * @returns {Promise<Object|null>} Datos del día anterior o null
 */
async function getPreviousDayPerformance(userId, accountId = 'overall') {
  // Obtener la fecha de ayer (o el último día hábil)
  const today = new Date();
  const yesterday = getLastTradingDay(today);
  
  const path = accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;
  
  // Buscar el documento más reciente (que debería ser ayer o antes)
  const snapshot = await db.collection(path)
    .orderBy('date', 'desc')
    .limit(1)
    .get();
  
  if (snapshot.empty) {
    console.log(`[IntradayCalc] No previous day performance found for ${userId}/${accountId}`);
    return null;
  }
  
  const doc = snapshot.docs[0];
  const data = { id: doc.id, ...doc.data() };
  
  console.log(`[IntradayCalc] Previous day performance found: ${data.date} for ${userId}/${accountId}`);
  return data;
}

/**
 * Obtiene los datos del día anterior de múltiples cuentas y los agrega
 * MULTI-ACCOUNT-FIX: Suma los valores de todas las cuentas seleccionadas
 * 
 * @param {string} userId - ID del usuario
 * @param {string[]} accountIds - IDs de cuentas
 * @param {string} currency - Moneda para agregar
 * @returns {Promise<Object|null>} Datos agregados del día anterior
 */
async function getPreviousDayPerformanceMultiAccount(userId, accountIds, currency) {
  // Si es 'overall' o un solo account, usar la función simple
  if (accountIds.includes('overall') || accountIds.includes('all')) {
    return getPreviousDayPerformance(userId, 'overall');
  }
  
  if (accountIds.length === 1) {
    return getPreviousDayPerformance(userId, accountIds[0]);
  }
  
  console.log(`[IntradayCalc] Getting previous day for ${accountIds.length} accounts`);
  
  // Obtener datos de todas las cuentas en paralelo
  const promises = accountIds.map(accountId => 
    getPreviousDayPerformance(userId, accountId)
  );
  const results = await Promise.all(promises);
  
  // Agregar los valores de todas las cuentas
  let totalValue = 0;
  let latestDate = null;
  let validAccounts = 0;
  
  for (const data of results) {
    if (data) {
      const currencyData = data[currency] || data.USD;
      if (currencyData?.totalValue) {
        totalValue += currencyData.totalValue;
        validAccounts++;
        // Usar la fecha más reciente
        if (!latestDate || data.date > latestDate) {
          latestDate = data.date;
        }
      }
    }
  }
  
  if (validAccounts === 0) {
    console.log(`[IntradayCalc] No previous day data found for any of ${accountIds.length} accounts`);
    return null;
  }
  
  console.log(`[IntradayCalc] Aggregated previous day from ${validAccounts} accounts: ${totalValue.toFixed(2)} ${currency}`);
  
  // Retornar estructura compatible
  return {
    date: latestDate,
    [currency]: {
      totalValue
    }
  };
}

/**
 * Obtiene el último día hábil (retrocede fines de semana)
 * @param {Date} from - Fecha de referencia
 * @returns {string} Fecha en formato YYYY-MM-DD
 */
function getLastTradingDay(from = new Date()) {
  const date = new Date(from);
  date.setDate(date.getDate() - 1); // Empezar con ayer
  
  // Retroceder si es fin de semana (0=domingo, 6=sábado)
  while (date.getDay() === 0 || date.getDay() === 6) {
    date.setDate(date.getDate() - 1);
  }
  
  return date.toISOString().split('T')[0];
}

/**
 * Obtiene la fecha de hoy en formato YYYY-MM-DD (hora local)
 * @returns {string} Fecha de hoy
 */
function getTodayDate() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
}

// ============================================================================
// CONVERSIÓN DE MONEDAS
// ============================================================================

/**
 * Convierte un valor de una moneda a otra
 * Replica la lógica de currencyUtils.ts del frontend
 * 
 * @param {number} amount - Cantidad a convertir
 * @param {string} fromCurrency - Moneda de origen
 * @param {string} toCurrency - Moneda de destino
 * @param {Object} currencies - Mapa de monedas con tasas de cambio
 * @param {number} acquisitionDollarValue - Valor de adquisición en USD (opcional)
 * @param {string} defaultCurrency - Moneda por defecto (opcional)
 * @returns {number|null} Cantidad convertida, o `null` si falta alguna tasa (RN-3-D)
 */
function convertCurrency(amount, fromCurrency, toCurrency, currencies, acquisitionDollarValue, defaultCurrency) {
  // Validar que amount sea un número válido
  if (typeof amount !== 'number' || isNaN(amount)) {
    return 0;
  }
  
  // Si las monedas son iguales, no hay conversión necesaria
  if (fromCurrency === toCurrency) {
    return amount;
  }
  
  // HU #3: sin tasa no hay conversión. `|| 1` trataba 4.000 COP como 4.000 USD
  // y presentaba el resultado como una cifra buena (RN-3-D)
  const fromRate = currencies[fromCurrency]?.exchangeRate;
  const toRate = currencies[toCurrency]?.exchangeRate;

  if (!fromRate || !toRate) {
    console.warn(`[IntradayCalc] RATE-MISS: sin tasa para convertir ${fromCurrency} → ${toCurrency}`);
    return null;
  }
  
  // Caso especial para valores de adquisición en USD
  if (fromCurrency === 'USD' && toCurrency === defaultCurrency && acquisitionDollarValue) {
    return amount * acquisitionDollarValue;
  }
  
  // Conversión usando USD como moneda intermediaria
  if (fromCurrency !== 'USD' && toCurrency !== 'USD') {
    // Convertir de fromCurrency a USD
    const amountInUSD = amount / fromRate;
    // Convertir de USD a toCurrency
    return amountInUSD * toRate;
  }
  
  // Conversión directa cuando una de las monedas es USD
  return (amount * toRate) / fromRate;
}

// ============================================================================
// CÁLCULO PRINCIPAL
// ============================================================================

/**
 * Calcula el rendimiento intraday del portafolio
 * 
 * Este es el equivalente backend de calculateIntradayChange del frontend.
 * Compara el valor actual del portafolio (usando precios en tiempo real)
 * con el valor al cierre del día anterior.
 * 
 * @param {Object} params - Parámetros de cálculo
 * @param {string} params.userId - ID del usuario
 * @param {string} params.currency - Moneda para los cálculos ('USD', 'COP', etc.)
 * @param {string[]} params.accountIds - IDs de cuentas (['overall'] para todas)
 * @returns {Promise<Object>} Resultado del cálculo intraday
 */
async function calculateIntradayPerformance(params) {
  const {
    userId,
    currency = 'USD',
    accountIds = ['overall']
  } = params;
  
  const today = getTodayDate();
  
  try {
    // =========================================================================
    // 1. OBTENER DATOS NECESARIOS EN PARALELO
    // =========================================================================
    // MULTI-ACCOUNT-FIX: Usar función de multi-cuenta para obtener datos agregados
    const [assets, currencies, previousDayData] = await Promise.all([
      getActiveAssets(userId, accountIds),
      getCurrencyRates(),
      getPreviousDayPerformanceMultiAccount(userId, accountIds, currency)
    ]);
    
    if (!assets.length) {
      console.log(`[IntradayCalc] No assets found for user ${userId}`);
      return {
        success: false,
        error: 'No active assets found',
        todayFactor: 1,
        dailyChangePercent: 0
      };
    }
    
    // =========================================================================
    // 2. OBTENER PRECIOS ACTUALES
    // =========================================================================
    const symbols = [...new Set(assets.map(a => a.name).filter(Boolean))];
    
    if (!symbols.length) {
      console.log(`[IntradayCalc] No symbols to fetch for user ${userId}`);
      return {
        success: false,
        error: 'No symbols to fetch',
        todayFactor: 1,
        dailyChangePercent: 0
      };
    }
    
    const pricesResponse = await getQuotes(symbols.join(','));
    
    // INTRADAY-DEBUG: Diagnosticar respuesta del API
    console.log(`[IntradayCalc] Quotes response type: ${typeof pricesResponse}, isArray: ${Array.isArray(pricesResponse)}`);
    
    // Crear mapa de precios symbol -> { price, currency }
    // La respuesta puede ser un array de objetos [{symbol, price, currency}, ...]
    // o un objeto {SYMBOL: {price, currency}, ...}
    const currentPrices = {};
    
    if (Array.isArray(pricesResponse)) {
      // Formato array: [{symbol: "AAPL", price: "150.25", currency: "USD"}, ...]
      console.log(`[IntradayCalc] Processing array response with ${pricesResponse.length} items`);
      for (const quote of pricesResponse) {
        if (quote && quote.symbol) {
          // INTRADAY-FIX: Eliminar comas de miles antes de parsear (ej: "89,290.02" → 89290.02)
          const price = typeof quote.price === 'string' 
            ? parseFloat(quote.price.replace(/,/g, '')) 
            : quote.price;
          if (typeof price === 'number' && !isNaN(price) && price > 0) {
            currentPrices[quote.symbol] = {
              price: price,
              currency: quote.currency || 'USD'
            };
          }
        }
      }
    } else if (pricesResponse && typeof pricesResponse === 'object') {
      // Formato objeto: {AAPL: {price: 150.25, currency: "USD"}, ...}
      console.log(`[IntradayCalc] Processing object response with keys: ${Object.keys(pricesResponse).slice(0, 5).join(', ')}...`);
      for (const [symbol, data] of Object.entries(pricesResponse)) {
        if (data && (typeof data.price === 'number' || typeof data.price === 'string')) {
          // INTRADAY-FIX: Eliminar comas de miles antes de parsear (ej: "89,290.02" → 89290.02)
          const price = typeof data.price === 'string' 
            ? parseFloat(data.price.replace(/,/g, '')) 
            : data.price;
          if (!isNaN(price) && price > 0) {
            currentPrices[symbol] = {
              price: price,
              currency: data.currency || 'USD'
            };
          }
        }
      }
    }
    
    console.log(`[IntradayCalc] Fetched prices for ${Object.keys(currentPrices).length}/${symbols.length} symbols`);
    
    // =========================================================================
    // 3. CALCULAR VALOR ACTUAL DEL PORTAFOLIO
    // =========================================================================
    let totalValue = 0;
    let totalInvestment = 0;
    let assetsWithPrice = 0;
    let assetsWithoutRate = 0;
    
    for (const asset of assets) {
      if (!asset.isActive || !asset.name) continue;
      
      const priceData = currentPrices[asset.name];
      if (!priceData) {
        console.log(`[IntradayCalc] No price for ${asset.name}`);
        continue;
      }
      
      const currentPrice = priceData.price;
      const priceCurrency = priceData.currency || asset.currency || 'USD';
      
      // Valor actual del activo convertido a la moneda seleccionada
      const assetValue = convertCurrency(
        currentPrice * (asset.units || 0),
        priceCurrency,
        currency,
        currencies
      );
      
      // Inversión original del activo convertida a la moneda seleccionada
      const assetInvestment = convertCurrency(
        (asset.unitValue || 0) * (asset.units || 0),
        asset.currency || 'USD',
        currency,
        currencies,
        asset.acquisitionDollarValue,
        asset.defaultCurrencyForAdquisitionDollar
      );
      
      // HU #3: un activo cuya divisa no tiene tasa queda fuera del agregado en
      // lugar de entrar con una cifra sin convertir (RN-3-D)
      if (assetValue === null || assetInvestment === null) {
        assetsWithoutRate++;
        console.warn(`[IntradayCalc] RATE-MISS: ${asset.name} queda fuera del cálculo por falta de tasa`);
        continue;
      }

      totalValue += assetValue;
      totalInvestment += assetInvestment;
      assetsWithPrice++;
    }

    if (assetsWithoutRate > 0) {
      console.warn(`[IntradayCalc] ${assetsWithoutRate} activo(s) excluidos del intradía por falta de tasa de cambio`);
    }
    
    console.log(`[IntradayCalc] Portfolio value: ${totalValue.toFixed(2)} ${currency} (${assetsWithPrice} assets with prices)`);
    
    // =========================================================================
    // VALIDACIÓN: Si no se obtuvieron precios, no calcular intraday
    // =========================================================================
    if (assetsWithPrice === 0 || totalValue === 0) {
      console.warn(`[IntradayCalc] No prices obtained for any asset. Skipping intraday calculation.`);
      return {
        success: false,
        error: 'No prices obtained for any asset',
        todayFactor: 1,
        dailyChangePercent: 0,
        assetsWithPrice: 0,
        symbolsRequested: symbols.length,
        symbolsWithPrice: Object.keys(currentPrices).length
      };
    }
    
    // =========================================================================
    // 4. OBTENER VALOR DEL DÍA ANTERIOR
    // =========================================================================
    let previousDayTotalValue = 0;
    
    if (previousDayData) {
      // Obtener el valor del día anterior en la moneda seleccionada
      const currencyData = previousDayData[currency] || previousDayData.USD;
      if (currencyData && currencyData.totalValue) {
        previousDayTotalValue = currencyData.totalValue;
      }
    }
    
    // Si no hay valor del día anterior, usar la inversión como base
    if (!previousDayTotalValue || previousDayTotalValue === 0) {
      console.log(`[IntradayCalc] No previous day value, using investment as base`);
      previousDayTotalValue = totalInvestment;
    }
    
    console.log(`[IntradayCalc] Previous day value: ${previousDayTotalValue.toFixed(2)} ${currency}`);
    
    // =========================================================================
    // 5. CALCULAR CAMBIO INTRADAY
    // =========================================================================
    const dailyChangeAbsolute = totalValue - previousDayTotalValue;
    
    // Cambio porcentual simple
    const dailyChangePercent = previousDayTotalValue > 0
      ? (dailyChangeAbsolute / previousDayTotalValue) * 100
      : 0;
    
    // TWR ajustado por cashflows (Modified Dietz simplificado)
    // Por ahora asumimos todayCashflows = 0 (no detectamos transacciones de hoy)
    const todayCashflows = 0;
    const adjustedNumerator = totalValue - previousDayTotalValue - todayCashflows;
    const adjustedDenominator = previousDayTotalValue + (todayCashflows * 0.5);
    const adjustedDailyChangePercent = adjustedDenominator > 0
      ? (adjustedNumerator / adjustedDenominator) * 100
      : 0;
    
    // Factor para multiplicar con históricos: 1 + (cambio / 100)
    const todayFactor = 1 + (adjustedDailyChangePercent / 100);
    
    console.log(`[IntradayCalc] Daily change: ${dailyChangePercent.toFixed(2)}%, Factor: ${todayFactor.toFixed(6)}`);
    
    // =========================================================================
    // 6. RETORNAR RESULTADO
    // =========================================================================
    return {
      success: true,
      date: today,
      totalValue,
      totalInvestment,
      previousDayTotalValue,
      previousDayDate: previousDayData?.date || null,
      dailyChangeAbsolute,
      dailyChangePercent,
      adjustedDailyChangePercent,
      todayFactor,
      currency,
      assetsWithPrice,
      totalAssets: assets.length,
      symbolsRequested: symbols.length,
      symbolsWithPrice: Object.keys(currentPrices).length
    };
    
  } catch (error) {
    console.error('[IntradayCalc] Error calculating intraday performance:', error);
    return {
      success: false,
      error: error.message,
      todayFactor: 1,
      dailyChangePercent: 0
    };
  }
}

/**
 * Combina rendimiento histórico con factor intraday usando TWR
 * 
 * Fórmula: (1 + historical) × todayFactor - 1
 * 
 * @param {number} historicalReturnPercent - Rendimiento histórico en porcentaje (ej: 8.27 para 8.27%)
 * @param {number} todayFactor - Factor del día actual (ej: 1.005 para +0.5%)
 * @returns {number} Rendimiento combinado en porcentaje
 * 
 * @example
 * // YTD histórico 8.27%, hoy +0.5%
 * const combined = combineHistoricalWithIntraday(8.27, 1.005)
 * // Resultado: 8.81% ((1.0827) × 1.005 - 1 = 0.0881)
 */
function combineHistoricalWithIntraday(historicalReturnPercent, todayFactor) {
  const historicalFactor = 1 + (historicalReturnPercent / 100);
  const combinedFactor = historicalFactor * todayFactor;
  return (combinedFactor - 1) * 100;
}

/**
 * Calcula las contribuciones intraday por activo
 * 
 * INTRADAY-002: Esta función calcula cuánto contribuye cada activo al cambio 
 * intraday del portafolio. Esto permite que las contribuciones individuales
 * sumen al TWR ajustado (incluyendo intraday).
 * 
 * Fórmula por activo:
 *   contribuciónIntraday = (precioActual - precioAlCierre) × unidades / valorPortafolioAlCierre
 * 
 * @param {Object} params - Parámetros de cálculo
 * @param {string} params.userId - ID del usuario
 * @param {string} params.currency - Moneda para los cálculos
 * @param {string[]} params.accountIds - IDs de cuentas
 * @param {Object} params.latestPerformanceData - Datos del último documento de portfolioPerformance
 * @returns {Promise<Object>} Mapa de assetKey → contribuciónIntraday
 */
async function calculateIntradayContributions(params) {
  const {
    userId,
    currency = 'USD',
    accountIds = ['overall'],
    latestPerformanceData
  } = params;
  
  try {
    // Si no hay datos del día anterior, no podemos calcular intraday
    if (!latestPerformanceData) {
      console.log('[IntradayCalc] No latestPerformanceData provided, skipping intraday contributions');
      return { success: false, contributions: {}, error: 'No latestPerformanceData' };
    }
    
    const currencyData = latestPerformanceData[currency] || latestPerformanceData.USD || {};
    const previousDayTotalValue = currencyData.totalValue || 0;
    const assetPerformance = currencyData.assetPerformance || {};
    
    if (previousDayTotalValue === 0) {
      console.log('[IntradayCalc] No previous day value, skipping intraday contributions');
      return { success: false, contributions: {}, error: 'No previousDayTotalValue' };
    }
    
    // Obtener todos los tickers de los activos
    const tickers = Object.keys(assetPerformance).map(key => key.split('_')[0]);
    const uniqueTickers = [...new Set(tickers)];
    
    if (uniqueTickers.length === 0) {
      console.log('[IntradayCalc] No assets to calculate intraday contributions');
      return { success: false, contributions: {}, error: 'No assets' };
    }
    
    console.log(`[IntradayCalc] Calculating intraday contributions for ${uniqueTickers.length} assets`);
    
    // Obtener tasas de cambio para conversión de monedas
    const currencyRates = await getCurrencyRates();
    
    // Obtener precios de mercado actuales
    const quotes = await getQuotes(uniqueTickers.join(','));
    
    // FIX-PERF-002: Manejar tanto respuesta array como objeto de getQuotes
    // (la misma lógica que calculateIntradayPerformance ya tiene)
    const marketData = {};
    
    if (Array.isArray(quotes)) {
      for (const quote of quotes) {
        if (quote && quote.symbol) {
          let price = quote.price;
          if (typeof price === 'string') {
            price = parseFloat(price.replace(/,/g, ''));
          }
          if (typeof price === 'number' && !isNaN(price) && price > 0) {
            marketData[quote.symbol] = {
              price: price,
              currency: quote.currency || 'USD'
            };
          }
        }
      }
    } else if (quotes && typeof quotes === 'object') {
      for (const [symbol, data] of Object.entries(quotes)) {
        if (data && (typeof data.price === 'number' || typeof data.price === 'string')) {
          const price = typeof data.price === 'string' 
            ? parseFloat(data.price.replace(/,/g, '')) 
            : data.price;
          if (!isNaN(price) && price > 0) {
            marketData[symbol] = {
              price: price,
              currency: data.currency || 'USD'
            };
          }
        }
      }
    }
    
    console.log(`[IntradayCalc] Got market prices for ${Object.keys(marketData).length}/${uniqueTickers.length} assets`);
    
    // Calcular contribución intraday de cada activo
    const contributions = {};
    let totalIntradayChange = 0;
    
    for (const [assetKey, assetData] of Object.entries(assetPerformance)) {
      const ticker = assetKey.split('_')[0];
      const units = assetData.units || 0;
      const valueAtClose = assetData.totalValue || 0;
      const quoteData = marketData[ticker];
      
      if (!quoteData || units === 0 || valueAtClose === 0) {
        contributions[assetKey] = 0;
        continue;
      }
      
      let marketPrice = quoteData.price;
      const quoteCurrency = quoteData.currency;
      
      // INTRADAY-FIX: Convertir precio de mercado a USD si es necesario
      // El valueAtClose ya está en USD (de portfolioPerformance)
      if (quoteCurrency && quoteCurrency !== 'USD' && currencyRates[quoteCurrency]) {
        const exchangeRate = currencyRates[quoteCurrency].exchangeRate;
        if (exchangeRate && exchangeRate > 0) {
          // Convertir de moneda local a USD (dividir por tasa)
          const priceInUSD = marketPrice / exchangeRate;
          console.log(`[IntradayCalc] ${ticker}: Converting ${quoteCurrency} ${marketPrice.toFixed(2)} → USD $${priceInUSD.toFixed(2)} (rate: ${exchangeRate})`);
          marketPrice = priceInUSD;
        }
      }
      
      // Precio al cierre (del documento de portfolioPerformance, ya en USD)
      const priceAtClose = valueAtClose / units;
      
      // Cambio de precio desde el cierre (ahora ambos en USD)
      const priceChange = marketPrice - priceAtClose;
      
      // Cambio de valor de este activo
      const valueChange = priceChange * units;
      
      // Contribución intraday = cambioValor / valorInicialPortafolio × 100
      const intradayContribution = (valueChange / previousDayTotalValue) * 100;
      
      contributions[assetKey] = intradayContribution;
      totalIntradayChange += intradayContribution;
      
      if (Math.abs(intradayContribution) > 0.01) {
        console.log(`[IntradayCalc] ${ticker}: priceClose=$${priceAtClose.toFixed(2)}, priceNow=$${marketPrice.toFixed(2)}, intradayContrib=${intradayContribution.toFixed(4)}pp`);
      }
    }
    
    console.log(`[IntradayCalc] Total intraday contribution: ${totalIntradayChange.toFixed(4)}pp`);
    
    return {
      success: true,
      contributions,
      totalIntradayChange,
      assetsWithPrice: Object.keys(marketData).length,
      totalAssets: uniqueTickers.length,
      previousDayTotalValue,
      previousDayDate: latestPerformanceData.date || latestPerformanceData.id
    };
    
  } catch (error) {
    console.error('[IntradayCalc] Error calculating intraday contributions:', error);
    return {
      success: false,
      contributions: {},
      error: error.message
    };
  }
}

module.exports = {
  calculateIntradayPerformance,
  calculateIntradayContributions,
  combineHistoricalWithIntraday,
  getActiveAssets,
  getActiveCurrencyCodes,
  getCurrencyRates,
  getPreviousDayPerformance,
  getPreviousDayPerformanceMultiAccount,
  convertCurrency
};
