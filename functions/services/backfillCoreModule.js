/**
 * backfillCoreModule.js
 * 
 * Core logic for recalculating portfolioPerformance documents.
 * Extracted from backfillPortfolioPerformance.js for use in Cloud Functions.
 * 
 * @see LATE-REG-003
 * @see docs/architecture/LATE-REGISTRATION-001-retroactive-transactions-analysis.md
 */

const fetch = require('node-fetch');
const admin = require('./firebaseAdmin');
const db = admin.firestore();

// ============================================================================
// CONFIGURATION
// ============================================================================

const CONFIG = {
  // API de precios históricos
  HISTORICAL_API_BASE: 'https://api.portastock.top/v1',
  
  // Headers de autenticación para API (sin fallback por seguridad)
  API_HEADERS: {
    'x-service-token': process.env.PORTASTOCK_API_TOKEN,
    'origin': 'https://portafolio-inversiones.web.app',
    'referer': 'https://portafolio-inversiones.web.app'
  },
  
  // Rate limiting
  API_DELAY_MS: 200,
};

// Cache para datos dinámicos de Firestore
let _cachedCurrencies = null;
let _cachedHolidays = null;
let _cacheTimestamp = 0;
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutos

/**
 * Obtiene las monedas activas desde la colección currencies de Firestore
 * @returns {Promise<string[]>} Array de códigos de moneda activos
 */
async function getActiveCurrencies() {
  const now = Date.now();
  
  // Usar cache si es válido
  if (_cachedCurrencies && (now - _cacheTimestamp) < CACHE_TTL_MS) {
    return _cachedCurrencies;
  }
  
  try {
    // Solo obtener currencies con isActive = true
    const snapshot = await db.collection('currencies')
      .where('isActive', '==', true)
      .get();
    const currencies = [];
    
    snapshot.forEach(doc => {
      const data = doc.data();
      if (data.code) {
        currencies.push(data.code);
      } else {
        currencies.push(doc.id);
      }
    });
    
    // Asegurar que USD siempre esté incluido
    if (!currencies.includes('USD')) {
      currencies.unshift('USD');
    }
    
    _cachedCurrencies = currencies;
    _cacheTimestamp = now;
    
    console.log(`[backfillCore] Loaded ${currencies.length} active currencies from Firestore`);
    return currencies;
    
  } catch (error) {
    console.error('[backfillCore] Error loading currencies from Firestore:', error.message);
    // Fallback mínimo en caso de error
    return ['USD'];
  }
}

/**
 * Obtiene los días festivos de NYSE desde la colección marketHolidays de Firestore
 * (sincronizados por scheduledHolidaySync desde Finnhub)
 * @returns {Promise<string[]>} Array de fechas festivas en formato YYYY-MM-DD
 */
async function getNYSEHolidays() {
  const now = Date.now();
  
  // Usar cache si es válido
  if (_cachedHolidays && (now - _cacheTimestamp) < CACHE_TTL_MS) {
    return _cachedHolidays;
  }
  
  try {
    const doc = await db.collection('marketHolidays').doc('US').get();
    
    if (!doc.exists) {
      console.warn('[backfillCore] No marketHolidays/US document found. Using empty holiday list.');
      return [];
    }
    
    const data = doc.data();
    
    // El documento tiene un campo 'holidays' que es un map de fecha -> nombre
    // O un campo 'holidayList' que es un array de { date, name }
    let holidays = [];
    
    if (data.holidays) {
      // Formato map: { "2026-01-19": "Martin Luther King Jr. Day", ... }
      holidays = Object.keys(data.holidays);
    } else if (data.holidayList && Array.isArray(data.holidayList)) {
      // Formato array: [{ date: "2026-01-19", name: "..." }, ...]
      holidays = data.holidayList.map(h => h.date);
    }
    
    _cachedHolidays = holidays;
    _cacheTimestamp = now;
    
    console.log(`[backfillCore] Loaded ${holidays.length} NYSE holidays from Firestore`);
    return holidays;
    
  } catch (error) {
    console.error('[backfillCore] Error loading NYSE holidays from Firestore:', error.message);
    return [];
  }
}

// ============================================================================
// UTILITIES
// ============================================================================

/**
 * Sleep utility for rate limiting
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Extract date part (YYYY-MM-DD) from a date string
 */
function getDatePart(dateString) {
  if (!dateString) return '';
  if (dateString.includes('T')) {
    return dateString.substring(0, 10);
  }
  return dateString;
}

/**
 * Check if date is on or before target
 */
function isDateOnOrBefore(txDate, targetDate) {
  return getDatePart(txDate) <= targetDate;
}

/**
 * Check if two dates are equal (comparing only date part)
 */
function isDateEqual(txDate, targetDate) {
  return getDatePart(txDate) === targetDate;
}

/**
 * Calculate daily cash flow for TWR adjustment
 * 
 * SIGN CONVENTION (same as backfillPortfolioPerformance.js gold standard):
 * - Negative: money leaves investor's pocket INTO portfolio (buys)
 * - Positive: money enters investor's pocket FROM portfolio (sells)
 * 
 * NOTE: cash_income/cash_outcome are NOT included because they are internal
 * cash transfers within the portfolio, not external capital injections.
 * 
 * @param {Array} transactions - All transactions
 * @param {string} targetDate - Target date (YYYY-MM-DD)
 * @param {Object} exchangeRates - Map of currency -> rate (1 USD = X currency)
 * @returns {number} Daily cashflow in USD
 */
function calculateDailyCashFlow(transactions, targetDate, exchangeRates = {}) {
  return transactions
    .filter(tx => isDateEqual(tx.date, targetDate))
    .reduce((sum, tx) => {
      const txCurrency = tx.currency || 'USD';
      
      // Convert to USD if not USD
      let conversionRate = 1;
      if (txCurrency !== 'USD' && exchangeRates[txCurrency]) {
        // exchangeRates[COP] = 4100 means 1 USD = 4100 COP
        // To convert COP to USD: value / exchangeRates[COP]
        conversionRate = 1 / exchangeRates[txCurrency];
      }
      
      if (tx.type === 'buy') {
        // Buy: money leaves pocket -> negative
        return sum - (tx.amount || 0) * (tx.price || 0) * conversionRate;
      }
      if (tx.type === 'sell') {
        // Sell: money enters pocket -> positive
        return sum + (tx.amount || 0) * (tx.price || 0) * conversionRate;
      }
      // cash_income/cash_outcome NOT included - internal transfers
      return sum;
    }, 0);
}

/**
 * Get today's date in YYYY-MM-DD format
 */
function getTodayDate() {
  return new Date().toISOString().split('T')[0];
}

/**
 * Generate NYSE trading days between two dates
 * @param {string} startDate - YYYY-MM-DD
 * @param {string} endDate - YYYY-MM-DD
 * @param {string[]} [holidays] - Optional pre-fetched holidays array
 * @returns {Promise<string[]>} Array of trading dates
 */
async function generateTradingDays(startDate, endDate, holidays = null) {
  // Obtener festivos de Firestore si no se proporcionaron
  const nyseHolidays = holidays || await getNYSEHolidays();
  
  const days = [];
  let current = new Date(startDate + 'T12:00:00Z');
  const end = new Date(endDate + 'T12:00:00Z');
  
  while (current <= end) {
    const dayOfWeek = current.getUTCDay();
    const dateStr = current.toISOString().split('T')[0];
    
    // Exclude weekends (0=Sunday, 6=Saturday) and holidays
    if (dayOfWeek !== 0 && dayOfWeek !== 6 && !nyseHolidays.includes(dateStr)) {
      days.push(dateStr);
    }
    current.setUTCDate(current.getUTCDate() + 1);
  }
  
  return days;
}

// ============================================================================
// DATA FETCHING
// ============================================================================

/**
 * Fetch historical prices for a symbol
 * @param {string} symbol - Ticker symbol
 * @param {string} startDate - Start date for range determination
 * @returns {Promise<Object>} Map of date -> close price
 */
async function fetchHistoricalPrices(symbol, startDate = null) {
  try {
    let range = 'ytd';
    if (startDate) {
      const start = new Date(startDate);
      const now = new Date();
      const monthsAgo = (now.getFullYear() - start.getFullYear()) * 12 + (now.getMonth() - start.getMonth());
      
      if (monthsAgo > 12) range = '2y';
      else if (monthsAgo > 6) range = '1y';
    }
    
    const url = `${CONFIG.HISTORICAL_API_BASE}/historical?symbol=${encodeURIComponent(symbol)}&range=${range}&interval=1d`;
    const response = await fetch(url, { headers: CONFIG.API_HEADERS });
    
    if (!response.ok) {
      console.warn(`[backfillCore] No prices for ${symbol}: ${response.status}`);
      return {};
    }
    
    const data = await response.json();
    const priceMap = {};
    
    Object.entries(data).forEach(([date, ohlcv]) => {
      priceMap[date] = ohlcv.close;
    });
    
    return priceMap;
  } catch (error) {
    console.error(`[backfillCore] Error fetching prices for ${symbol}:`, error.message);
    return {};
  }
}

/**
 * Fetch historical exchange rate for a currency
 * @param {string} currency - Currency code
 * @param {Date} date - Target date
 * @returns {Promise<number|null>} Exchange rate relative to USD
 */
async function fetchHistoricalExchangeRate(currency, date) {
  if (currency === 'USD') return 1;
  
  try {
    let symbol;
    if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
      symbol = `${currency}USD`;
    } else {
      symbol = `USD${currency}`;
    }
    
    const timestamp = Math.floor(date.getTime() / 1000);
    const nextDay = timestamp + 86400;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}%3DX?period1=${timestamp}&period2=${nextDay}&interval=1d`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    if (data.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.[0]) {
      let rate = data.chart.result[0].indicators.quote[0].close[0];
      
      if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
        rate = 1 / rate;
      }
      
      return rate;
    }
    
    return null;
  } catch (error) {
    console.warn(`[backfillCore] Error fetching exchange rate for ${currency}:`, error.message);
    return null;
  }
}

/**
 * Get price for a date, with fallback to the closest previous day
 * Identical to backfillPortfolioPerformance.js getPriceForDate
 * 
 * @param {Object} symbolPrices - Map of date -> price for a symbol
 * @param {string} targetDate - Target date (YYYY-MM-DD)
 * @returns {number} Price (0 if not found)
 */
function getPriceForDate(symbolPrices, targetDate) {
  if (!symbolPrices || Object.keys(symbolPrices).length === 0) return 0;
  
  // First try exact date
  if (symbolPrices[targetDate]) {
    return symbolPrices[targetDate];
  }
  
  // If no price for that date, find the closest previous day
  const sortedDates = Object.keys(symbolPrices).sort().reverse();
  for (const date of sortedDates) {
    if (date < targetDate) {
      return symbolPrices[date];
    }
  }
  
  // If no previous date, use the first available
  return symbolPrices[sortedDates[sortedDates.length - 1]] || 0;
}

// ============================================================================
// USER DATA
// ============================================================================

/**
 * Get all portfolio account IDs for a user
 */
async function getUserAccountIds(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .get();
  
  return snapshot.docs.map(doc => doc.id);
}

/**
 * Get all assets for user accounts
 */
async function getAssetsForUser(userId) {
  const accountIds = await getUserAccountIds(userId);
  
  if (accountIds.length === 0) {
    return [];
  }
  
  // Firestore 'in' query is limited to 10 items
  const allAssets = [];
  for (let i = 0; i < accountIds.length; i += 10) {
    const batch = accountIds.slice(i, i + 10);
    const snapshot = await db.collection('assets')
      .where('portfolioAccount', 'in', batch)
      .get();
    
    allAssets.push(...snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() })));
  }
  
  return allAssets;
}

/**
 * Get all transactions for a user
 */
async function getTransactionsForUser(userId) {
  const snapshot = await db.collection('transactions')
    .where('userId', '==', userId)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

// ============================================================================
// ASSET STATE RECONSTRUCTION
// ============================================================================

/**
 * Reconstruct asset state as of a specific date
 * Uses transactions to build up the state incrementally
 * 
 * @param {Array} transactions - All user transactions sorted by date
 * @param {string} targetDate - Target date (YYYY-MM-DD)
 * @param {Object} exchangeRates - Map of currency code -> exchange rate to USD
 *                                 e.g. { COP: 4100, EUR: 0.92 }
 *                                 For COP, 1 USD = 4100 COP, so to convert COP to USD: value / 4100
 * @returns {Array} Array of active assets with { name, units, totalCostUSD, currency, assetType, portfolioAccount }
 */
function reconstructAssetStateForDate(transactions, targetDate, exchangeRates = {}) {
  const assetState = {};
  
  // Type order for sorting: buy before sell in same day
  const typeOrder = { 'buy': 0, 'cash_income': 1, 'dividendPay': 2, 'sell': 3, 'cash_outcome': 4 };
  
  // Filter and sort transactions
  const relevantTx = transactions
    .filter(tx => getDatePart(tx.date) <= targetDate)
    .sort((a, b) => {
      const dateA = getDatePart(a.date);
      const dateB = getDatePart(b.date);
      if (dateA !== dateB) return dateA.localeCompare(dateB);
      return (typeOrder[a.type] ?? 99) - (typeOrder[b.type] ?? 99);
    });
  
  for (const tx of relevantTx) {
    const key = tx.assetName || tx.assetId;
    if (!key) continue;
    
    // Skip cash transactions
    if (tx.type === 'cash_income' || tx.type === 'cash_outcome' || tx.type === 'dividendPay') {
      continue;
    }
    
    const txCurrency = tx.currency || 'USD';
    
    // Calculate conversion rate to USD
    // exchangeRates[COP] = 4100 means 1 USD = 4100 COP
    // To convert COP to USD: value / 4100
    let toUsdRate = 1;
    if (txCurrency !== 'USD' && exchangeRates[txCurrency]) {
      toUsdRate = 1 / exchangeRates[txCurrency];
    }
    
    if (!assetState[key]) {
      assetState[key] = {
        name: tx.assetName || key,
        units: 0,
        totalCostLocalCurrency: 0, // Cost in original currency
        totalCostUSD: 0,            // Cost converted to USD
        currency: txCurrency,
        assetType: tx.assetType || 'stock',
        portfolioAccount: tx.portfolioAccountId,
      };
    }
    
    if (tx.type === 'buy') {
      const amount = tx.amount || 0;
      const price = tx.price || 0;
      // NOTE: NO incluimos comisión, igual que backfillPortfolioPerformance.js
      // const commission = tx.commission || 0;
      const costLocal = amount * price;
      const costUSD = costLocal * toUsdRate;
      
      assetState[key].units += amount;
      assetState[key].totalCostLocalCurrency += costLocal;
      assetState[key].totalCostUSD += costUSD;
      
    } else if (tx.type === 'sell') {
      const unitsToSell = tx.amount || 0;
      const current = assetState[key];
      
      if (current.units > 0) {
        const ratio = unitsToSell / current.units;
        current.units -= unitsToSell;
        current.totalCostLocalCurrency -= current.totalCostLocalCurrency * ratio;
        current.totalCostUSD -= current.totalCostUSD * ratio;
      }
    }
  }
  
  // Filter out assets with 0 units
  const activeAssets = [];
  for (const [key, state] of Object.entries(assetState)) {
    if (state.units > 0.0001) {
      activeAssets.push({
        name: state.name,
        units: state.units,
        totalCostUSD: state.totalCostUSD,
        totalCostLocalCurrency: state.totalCostLocalCurrency,
        currency: state.currency,
        assetType: state.assetType,
        portfolioAccount: state.portfolioAccount,
        unitCostUSD: state.totalCostUSD / state.units,
        unitCostLocal: state.totalCostLocalCurrency / state.units,
      });
    }
  }
  
  return activeAssets;
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================

/**
 * Recalculate performance for a user over a range of trading days
 * 
 * @param {string} userId - User ID
 * @param {string[]} tradingDays - Array of dates (YYYY-MM-DD) to recalculate
 * @returns {Promise<{success: boolean, daysProcessed: number, errors: string[]}>}
 */
async function backfillUserPerformance(userId, tradingDays) {
  const errors = [];
  let daysProcessed = 0;
  
  if (!tradingDays || tradingDays.length === 0) {
    return { success: true, daysProcessed: 0, errors: [] };
  }
  
  const startDate = tradingDays[0];
  console.log(`[backfillCore] Starting backfill for user ${userId}: ${tradingDays.length} days from ${startDate}`);
  
  try {
    // 1. Get all transactions for the user
    const allTransactions = await getTransactionsForUser(userId);
    
    if (allTransactions.length === 0) {
      console.log(`[backfillCore] User ${userId} has no transactions. Skipping.`);
      return { success: true, daysProcessed: 0, errors: [] };
    }
    
    // 2. Get unique symbols from transactions
    const symbols = [...new Set(allTransactions.map(t => t.assetName).filter(Boolean))];
    
    if (symbols.length === 0) {
      console.log(`[backfillCore] User ${userId} has no valid symbols. Skipping.`);
      return { success: true, daysProcessed: 0, errors: [] };
    }
    
    // 3. Fetch historical prices for all symbols
    console.log(`[backfillCore] Fetching prices for ${symbols.length} symbols...`);
    const pricesBySymbol = {};
    for (const symbol of symbols) {
      pricesBySymbol[symbol] = await fetchHistoricalPrices(symbol, startDate);
      await sleep(CONFIG.API_DELAY_MS);
    }
    
    // 4. Fetch exchange rates for all target dates
    // Obtener monedas activas desde Firestore
    const activeCurrencies = await getActiveCurrencies();
    console.log(`[backfillCore] Fetching exchange rates for ${activeCurrencies.length} currencies...`);
    const ratesByDate = {};
    for (const date of tradingDays) {
      ratesByDate[date] = { USD: 1 };
      for (const currency of activeCurrencies) {
        if (currency === 'USD') continue;
        const rate = await fetchHistoricalExchangeRate(currency, new Date(date + 'T12:00:00Z'));
        if (rate) ratesByDate[date][currency] = rate;
        await sleep(CONFIG.API_DELAY_MS / 2);
      }
    }
    
    // 5. Process each trading day
    let previousDayData = null;
    
    for (const date of tradingDays) {
      try {
        // Get exchange rates for this date
        const exchangeRates = ratesByDate[date] || { USD: 1 };
        
        // Reconstruct asset state for this date with currency conversion
        const assets = reconstructAssetStateForDate(allTransactions, date, exchangeRates);
        
        if (assets.length === 0) {
          console.log(`[backfillCore] User ${userId} day ${date}: No assets. Skipping.`);
          continue;
        }
        
        // Calculate values with current prices
        // IMPORTANT: Prices from API are in the asset's native currency
        // For assets in non-USD currencies (e.g., ECOPETROL.CL in COP),
        // we need to convert the market value to USD using exchange rates
        let totalValueUSD = 0;
        let totalInvestmentUSD = 0;
        
        for (const asset of assets) {
          // Use getPriceForDate with fallback to previous days (same as backfillPortfolioPerformance)
          const symbolPrices = pricesBySymbol[asset.name];
          const price = getPriceForDate(symbolPrices, date);
          
          if (price > 0) {
            // Price is in asset's native currency
            const valueInLocalCurrency = asset.units * price;
            
            // Convert to USD if asset is not in USD
            let valueInUSD = valueInLocalCurrency;
            if (asset.currency && asset.currency !== 'USD' && exchangeRates[asset.currency]) {
              // exchangeRates[COP] = 4100 means 1 USD = 4100 COP
              // To convert COP to USD: value / exchangeRates[COP]
              valueInUSD = valueInLocalCurrency / exchangeRates[asset.currency];
            }
            
            totalValueUSD += valueInUSD;
            totalInvestmentUSD += asset.totalCostUSD;
          }
        }
        
        // =========================================================================
        // PERFORMANCE CALCULATION (identical to backfillPortfolioPerformance.js)
        // =========================================================================
        
        const previousTotalValue = previousDayData?.totalValue || 0;
        
        // Detect if this is a "new investment" (no value yesterday but value today)
        const isNewInvestment = previousTotalValue === 0 && totalValueUSD > 0;
        
        // Calculate daily cashflow for TWR adjustment
        // Same convention as backfillPortfolioPerformance.js:
        // - Negative for buys (money leaves pocket)
        // - Positive for sells (money enters pocket)
        const dailyCashFlowUSD = calculateDailyCashFlow(allTransactions, date, exchangeRates);
        
        // 1. rawDailyChangePercentage: Raw change without adjustments
        let rawDailyChangePercentage = 0;
        if (previousTotalValue > 0) {
          rawDailyChangePercentage = ((totalValueUSD - previousTotalValue) / previousTotalValue) * 100;
        }
        
        // 2. adjustedDailyChangePercentage: TWR-adjusted with cashflow
        //    Formula: (endValue - startValue + cashFlow) / startValue * 100
        //    cashFlow is negative for buys, positive for sells
        //    Adding it "cancels out" the buy from the return calculation
        let adjustedDailyChangePercentage = 0;
        if (isNewInvestment) {
          // First investment: 0% return (LATE-REG-001)
          adjustedDailyChangePercentage = 0;
        } else if (previousTotalValue > 0) {
          // TWR formula with cashflow adjustment
          adjustedDailyChangePercentage = ((totalValueUSD - previousTotalValue + dailyCashFlowUSD) / previousTotalValue) * 100;
        }
        
        // 3. dailyChangePercentage: By convention, same as rawDailyChangePercentage
        const dailyChangePercentage = rawDailyChangePercentage;
        
        // Total ROI
        const totalROI = totalInvestmentUSD > 0 
          ? ((totalValueUSD - totalInvestmentUSD) / totalInvestmentUSD) * 100 
          : 0;
        
        // Unrealized P&L
        const unrealizedProfitAndLoss = totalValueUSD - totalInvestmentUSD;
        
        // Prepare document data
        const performanceData = {
          date,
          USD: {
            totalValue: totalValueUSD,
            totalInvestment: totalInvestmentUSD,
            totalCashFlow: dailyCashFlowUSD, // TWR cashflow for the day
            unrealizedProfitAndLoss,
            doneProfitAndLoss: 0, // Simplified: no done P&L tracking
            totalROI,
            dailyChangePercentage,
            rawDailyChangePercentage,
            adjustedDailyChangePercentage,
            dailyReturn: adjustedDailyChangePercentage / 100,
            monthlyReturn: 0,
            annualReturn: 0,
          },
          overall: {
            totalValue: totalValueUSD,
            totalInvestment: totalInvestmentUSD,
            totalROI,
          },
          lastUpdated: new Date().toISOString(),
        };
        
        // Save to Firestore
        await db.collection('portfolioPerformance').doc(userId).set(performanceData, { merge: true });
        
        // Also save to dates subcollection for historical tracking
        await db.collection('portfolioPerformance').doc(userId)
          .collection('dates').doc(date)
          .set(performanceData, { merge: true });
        
        previousDayData = { totalValue: totalValueUSD, totalInvestment: totalInvestmentUSD };
        daysProcessed++;
        
      } catch (dayError) {
        const errMsg = `Day ${date}: ${dayError.message}`;
        errors.push(errMsg);
        console.error(`[backfillCore] User ${userId}:`, errMsg);
      }
    }
    
    console.log(`[backfillCore] Completed backfill for user ${userId}: ${daysProcessed}/${tradingDays.length} days processed`);
    
    return {
      success: errors.length === 0,
      daysProcessed,
      errors
    };
    
  } catch (error) {
    console.error(`[backfillCore] Fatal error for user ${userId}:`, error);
    return {
      success: false,
      daysProcessed,
      errors: [...errors, `Fatal: ${error.message}`]
    };
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Configuration
  CONFIG,
  
  // Dynamic data from Firestore
  getActiveCurrencies,
  getNYSEHolidays,
  
  // Utilities
  sleep,
  getDatePart,
  getTodayDate,
  generateTradingDays,
  isDateEqual,
  calculateDailyCashFlow,
  
  // Data fetching
  fetchHistoricalPrices,
  fetchHistoricalExchangeRate,
  getPriceForDate,
  
  // User data
  getUserAccountIds,
  getAssetsForUser,
  getTransactionsForUser,
  
  // State reconstruction
  reconstructAssetStateForDate,
  
  // Main backfill
  backfillUserPerformance,
};
