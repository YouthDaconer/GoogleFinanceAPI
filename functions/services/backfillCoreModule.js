/**
 * backfillCoreModule.js
 * 
 * Core logic for recalculating portfolioPerformance documents.
 * Extracted from backfillPortfolioPerformance.js for use in Cloud Functions.
 * 
 * @see LATE-REG-003
 * @see docs/architecture/LATE-REGISTRATION-001-retroactive-transactions-analysis.md
 */

// node-fetch v3 exports as ESM, so we need to handle both v2 and v3
const nodeFetch = require('node-fetch');
const fetch = nodeFetch.default || nodeFetch;

const admin = require('./firebaseAdmin');
const db = admin.firestore();

// ============================================================================
// CONFIGURATION
// ============================================================================

const CONFIG = {
  // API de precios históricos
  HISTORICAL_API_BASE: 'https://api.portastock.net/v1',
  
  // Headers de autenticación para API (sin fallback por seguridad)
  API_HEADERS: {
    'x-service-token': process.env.PORTASTOCK_API_TOKEN,
    'origin': 'https://portastock.net',
    'referer': 'https://portastock.net'
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
 * Calculate daily realized P&L (doneProfitAndLoss) from sell transactions
 * This represents gains/losses REALIZED from sales on a specific day.
 * 
 * @param {Array} transactions - All transactions
 * @param {string} targetDate - Target date (YYYY-MM-DD)
 * @returns {number} Total realized P&L for the day in original currencies
 */
function calculateDailyDonePnL(transactions, targetDate) {
  return transactions
    .filter(tx => isDateEqual(tx.date, targetDate) && tx.type === 'sell')
    .reduce((sum, tx) => sum + (tx.valuePnL || 0), 0);
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

/**
 * Get the previous NYSE trading day before a given date
 * Skips weekends and holidays going backwards
 * 
 * @param {string} dateStr - YYYY-MM-DD
 * @param {number} maxLookback - Maximum days to look back (default: 10)
 * @returns {Promise<string|null>} Previous trading day or null if not found
 */
async function getPreviousTradingDay(dateStr, maxLookback = 10) {
  const nyseHolidays = await getNYSEHolidays();
  
  let current = new Date(dateStr + 'T12:00:00Z');
  current.setUTCDate(current.getUTCDate() - 1); // Start from day before
  
  for (let i = 0; i < maxLookback; i++) {
    const dayOfWeek = current.getUTCDay();
    const checkDate = current.toISOString().split('T')[0];
    
    // Check if it's a trading day (not weekend, not holiday)
    if (dayOfWeek !== 0 && dayOfWeek !== 6 && !nyseHolidays.includes(checkDate)) {
      return checkDate;
    }
    
    current.setUTCDate(current.getUTCDate() - 1);
  }
  
  return null;
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
 * Get all active portfolio accounts for a user (with full data)
 * @param {string} userId - User ID
 * @returns {Promise<Array<{id: string, name: string, ...}>>} Array of account objects
 */
async function getUserAccounts(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Get transactions for a specific account
 * @param {string} accountId - Portfolio account ID
 * @returns {Promise<Array>} Transactions sorted by date
 */
async function getTransactionsByAccount(accountId) {
  const snapshot = await db.collection('transactions')
    .where('portfolioAccountId', '==', accountId)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
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
 * Calculate performance for a single account on a single day
 * 
 * @param {Array} transactions - Account transactions
 * @param {string} date - Target date
 * @param {Object} exchangeRates - Exchange rates for this date
 * @param {Object} pricesBySymbol - Historical prices by symbol
 * @param {Object|null} previousDayData - Previous day's data for daily change calculation
 * @returns {Object} Performance data for this day
 */
function calculateAccountDayPerformance(transactions, date, exchangeRates, pricesBySymbol, previousDayData) {
  // Reconstruct asset state for this date
  const assets = reconstructAssetStateForDate(transactions, date, exchangeRates);
  
  if (assets.length === 0) {
    return null;
  }
  
  // Calculate values with current prices
  let totalValueUSD = 0;
  let totalInvestmentUSD = 0;
  let skippedAssets = 0;
  
  for (const asset of assets) {
    const symbolPrices = pricesBySymbol[asset.name];
    const price = getPriceForDate(symbolPrices, date);
    
    if (price > 0) {
      const valueInLocalCurrency = asset.units * price;
      
      let valueInUSD = valueInLocalCurrency;
      if (asset.currency && asset.currency !== 'USD') {
        if (exchangeRates[asset.currency]) {
          valueInUSD = valueInLocalCurrency / exchangeRates[asset.currency];
        } else {
          // CRITICAL: Skip assets without exchange rate to avoid inflated values
          // A COP value treated as USD would be ~4000x inflated
          console.warn(`[backfillCore] Skipping asset ${asset.name} (${asset.currency}): no exchange rate available`);
          skippedAssets++;
          continue;
        }
      }
      
      totalValueUSD += valueInUSD;
      totalInvestmentUSD += asset.totalCostUSD;
    }
  }
  
  const previousTotalValue = previousDayData?.totalValue || 0;
  const isNewInvestment = previousTotalValue === 0 && totalValueUSD > 0;
  
  const dailyCashFlowUSD = calculateDailyCashFlow(transactions, date, exchangeRates);
  const dailyDonePnL = calculateDailyDonePnL(transactions, date);
  
  // rawDailyChangePercentage
  let rawDailyChangePercentage = 0;
  if (previousTotalValue > 0) {
    rawDailyChangePercentage = ((totalValueUSD - previousTotalValue) / previousTotalValue) * 100;
  }
  
  // adjustedDailyChangePercentage (TWR)
  let adjustedDailyChangePercentage = 0;
  if (isNewInvestment) {
    adjustedDailyChangePercentage = 0;
  } else if (previousTotalValue > 0) {
    adjustedDailyChangePercentage = ((totalValueUSD - previousTotalValue + dailyCashFlowUSD) / previousTotalValue) * 100;
  }
  
  const dailyChangePercentage = rawDailyChangePercentage;
  const unrealizedProfitAndLoss = totalValueUSD - totalInvestmentUSD;
  const totalROI = totalInvestmentUSD > 0 
    ? ((totalValueUSD - totalInvestmentUSD) / totalInvestmentUSD) * 100 
    : 0;
  
  return {
    date,
    USD: {
      totalValue: totalValueUSD,
      totalInvestment: totalInvestmentUSD,
      totalCashFlow: dailyCashFlowUSD,
      unrealizedProfitAndLoss,
      doneProfitAndLoss: dailyDonePnL,
      totalROI,
      dailyChangePercentage,
      rawDailyChangePercentage,
      adjustedDailyChangePercentage,
      dailyReturn: adjustedDailyChangePercentage / 100,
      monthlyReturn: 0,
      annualReturn: 0,
    },
    lastUpdated: new Date().toISOString(),
  };
}

/**
 * Aggregate OVERALL performance from multiple accounts using pre-change value method
 * This ensures the combined change is always between min and max of individual accounts
 * 
 * @param {Map<string, Object>} accountsPerformance - Map of accountId -> performance data
 * @param {string} date - Target date
 * @returns {Object} Aggregated OVERALL performance
 */
function aggregateOverallPerformance(accountsPerformance, date) {
  let totalValue = 0;
  let totalInvestment = 0;
  let totalCashFlow = 0;
  let totalDonePnL = 0;
  
  // Pre-change value weighted aggregation
  let totalPreChangeValue = 0;
  let weightedAdjustedChange = 0;
  let weightedRawChange = 0;
  
  for (const [accountId, perfData] of accountsPerformance.entries()) {
    const usdData = perfData.USD;
    if (!usdData) continue;
    
    const accountValue = usdData.totalValue || 0;
    const accountAdjChange = usdData.adjustedDailyChangePercentage || 0;
    const accountRawChange = usdData.rawDailyChangePercentage || 0;
    
    totalValue += accountValue;
    totalInvestment += usdData.totalInvestment || 0;
    totalCashFlow += usdData.totalCashFlow || 0;
    totalDonePnL += usdData.doneProfitAndLoss || 0;
    
    // Pre-change value method for weighted average
    if (accountValue > 0) {
      const preChangeValue = accountAdjChange !== 0 
        ? accountValue / (1 + accountAdjChange / 100) 
        : accountValue;
      
      totalPreChangeValue += preChangeValue;
      weightedAdjustedChange += preChangeValue * accountAdjChange;
      weightedRawChange += preChangeValue * accountRawChange;
    }
  }
  
  // Calculate weighted percentages
  let rawDailyChangePercentage = 0;
  let adjustedDailyChangePercentage = 0;
  
  if (totalPreChangeValue > 0) {
    adjustedDailyChangePercentage = weightedAdjustedChange / totalPreChangeValue;
    rawDailyChangePercentage = weightedRawChange / totalPreChangeValue;
  }
  
  const unrealizedProfitAndLoss = totalValue - totalInvestment;
  const totalROI = totalInvestment > 0 
    ? ((totalValue - totalInvestment) / totalInvestment) * 100 
    : 0;
  
  return {
    date,
    USD: {
      totalValue,
      totalInvestment,
      totalCashFlow,
      doneProfitAndLoss: totalDonePnL,
      unrealizedProfitAndLoss,
      totalROI,
      dailyChangePercentage: rawDailyChangePercentage,
      rawDailyChangePercentage,
      adjustedDailyChangePercentage,
      dailyReturn: adjustedDailyChangePercentage / 100,
      monthlyReturn: 0,
      annualReturn: 0,
    },
    overall: {
      totalValue,
      totalInvestment,
      totalROI,
    },
    lastUpdated: new Date().toISOString(),
  };
}

/**
 * SCALE-003: Capture existing performance documents before overwriting
 */
async function capturePreBackfillSnapshot(userId, accounts, tradingDays) {
  const firstDay = tradingDays[0];
  const lastDay = tradingDays[tradingDays.length - 1];
  let totalDocs = 0;

  const accountsData = {};

  for (const account of accounts) {
    const snapshot = await db.collection('portfolioPerformance')
      .doc(userId)
      .collection('accounts').doc(account.id)
      .collection('dates')
      .where('date', '>=', firstDay)
      .where('date', '<=', lastDay)
      .get();

    if (!snapshot.empty) {
      accountsData[account.id] = {};
      snapshot.docs.forEach(doc => {
        accountsData[account.id][doc.id] = doc.data();
        totalDocs++;
      });
    }
  }

  const overallSnapshot = await db.collection('portfolioPerformance')
    .doc(userId)
    .collection('dates')
    .where('date', '>=', firstDay)
    .where('date', '<=', lastDay)
    .get();

  const overallData = {};
  if (!overallSnapshot.empty) {
    overallSnapshot.docs.forEach(doc => {
      overallData[doc.id] = doc.data();
      totalDocs++;
    });
  }

  if (totalDocs === 0) return null;

  const AVG_DOC_SIZE_BYTES = 12 * 1024;
  const estimatedSize = totalDocs * AVG_DOC_SIZE_BYTES;
  if (estimatedSize > 800 * 1024) {
    console.warn(`[backfillCore] Snapshot too large (~${(estimatedSize / 1024).toFixed(0)} KB for ${totalDocs} docs), saving OVERALL only`);
    return {
      userId,
      tradingDays,
      snapshotAt: new Date().toISOString(),
      source: 'reconcileStalePerformance',
      totalDocs: Object.keys(overallData).length,
      truncated: true,
      affectedAccounts: accounts.map(a => a.id),
      accounts: {},
      overall: overallData
    };
  }

  return {
    userId,
    tradingDays,
    snapshotAt: new Date().toISOString(),
    source: 'reconcileStalePerformance',
    totalDocs,
    accounts: accountsData,
    overall: overallData
  };
}

/**
 * Recalculate performance for a user over a range of trading days
 * 
 * ATOMIC BACKFILL: Writes to both:
 * - portfolioPerformance/{userId}/accounts/{accountId}/dates/{date} (per account)
 * - portfolioPerformance/{userId}/dates/{date} (OVERALL aggregation)
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
  console.log(`[backfillCore] Starting ATOMIC backfill for user ${userId}: ${tradingDays.length} days from ${startDate}`);
  
  try {
    // 1. Get all accounts for the user
    const accounts = await getUserAccounts(userId);
    
    if (accounts.length === 0) {
      console.log(`[backfillCore] User ${userId} has no active accounts. Skipping.`);
      return { success: true, daysProcessed: 0, errors: [] };
    }
    
    console.log(`[backfillCore] Found ${accounts.length} accounts for user ${userId}`);
    
    // 2. Get transactions for each account
    const transactionsByAccount = new Map();
    const allSymbols = new Set();
    
    for (const account of accounts) {
      const transactions = await getTransactionsByAccount(account.id);
      transactionsByAccount.set(account.id, transactions);
      
      transactions.forEach(tx => {
        if (tx.assetName) allSymbols.add(tx.assetName);
      });
    }
    
    if (allSymbols.size === 0) {
      console.log(`[backfillCore] User ${userId} has no valid symbols. Skipping.`);
      return { success: true, daysProcessed: 0, errors: [] };
    }
    
    // 3. Fetch historical prices for all symbols
    console.log(`[backfillCore] Fetching prices for ${allSymbols.size} symbols...`);
    const pricesBySymbol = {};
    for (const symbol of allSymbols) {
      pricesBySymbol[symbol] = await fetchHistoricalPrices(symbol, startDate);
      await sleep(CONFIG.API_DELAY_MS);
    }
    
    // 4. Fetch exchange rates for all target dates
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
    
    // 5. Process each account for each trading day
    // Track previous day data per account for daily change calculation
    const previousDayByAccount = new Map();
    
    // 5.0 CRITICAL: Load previous day data from Firestore for the day BEFORE the first trading day
    // This ensures accurate TWR calculation for the first day in the range
    const dayBeforeStart = await getPreviousTradingDay(startDate);
    if (dayBeforeStart) {
      console.log(`[backfillCore] Loading previous day data from ${dayBeforeStart} for TWR baseline...`);
      
      for (const account of accounts) {
        try {
          const prevDoc = await db.collection('portfolioPerformance')
            .doc(userId)
            .collection('accounts')
            .doc(account.id)
            .collection('dates')
            .doc(dayBeforeStart)
            .get();
          
          if (prevDoc.exists) {
            const prevData = prevDoc.data();
            previousDayByAccount.set(account.id, {
              totalValue: prevData?.USD?.totalValue || 0,
              totalInvestment: prevData?.USD?.totalInvestment || 0,
            });
          }
        } catch (e) {
          console.warn(`[backfillCore] Could not load previous day for account ${account.id}: ${e.message}`);
        }
      }
      
      console.log(`[backfillCore] Loaded baseline data for ${previousDayByAccount.size} accounts`);
    }
    
    // SCALE-003: Capture snapshot before overwriting
    try {
      const snapshotData = await capturePreBackfillSnapshot(userId, accounts, tradingDays);
      if (snapshotData) {
        const snapshotId = `${userId}_${tradingDays[0]}`;
        const ttlExpiry = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);
        
        await db.collection('_backfillSnapshots').doc(snapshotId).set({
          ...snapshotData,
          ttlExpiry
        });
        console.log(`[backfillCore] Snapshot saved: _backfillSnapshots/${snapshotId} (${snapshotData.totalDocs} docs)`);
      }
    } catch (snapshotError) {
      console.warn(`[backfillCore] Could not save pre-backfill snapshot: ${snapshotError.message}`);
    }
    
    for (const date of tradingDays) {
      try {
        const exchangeRates = ratesByDate[date] || { USD: 1 };
        const accountsPerformanceForDay = new Map();
        let anyAccountProcessed = false;
        
        // 5a. Calculate and write performance for each account
        for (const account of accounts) {
          const transactions = transactionsByAccount.get(account.id);
          if (!transactions || transactions.length === 0) continue;
          
          const previousDayData = previousDayByAccount.get(account.id) || null;
          
          const accountPerformance = calculateAccountDayPerformance(
            transactions,
            date,
            exchangeRates,
            pricesBySymbol,
            previousDayData
          );
          
          if (accountPerformance) {
            // Store for OVERALL aggregation
            accountsPerformanceForDay.set(account.id, accountPerformance);
            
            // Write to accounts/{accountId}/dates/{date}
            await db.collection('portfolioPerformance').doc(userId)
              .collection('accounts').doc(account.id)
              .collection('dates').doc(date)
              .set(accountPerformance, { merge: true });
            
            // Update previous day tracker
            previousDayByAccount.set(account.id, {
              totalValue: accountPerformance.USD.totalValue,
              totalInvestment: accountPerformance.USD.totalInvestment,
            });
            
            anyAccountProcessed = true;
          }
        }
        
        // 5b. Aggregate OVERALL and write to dates/{date}
        if (accountsPerformanceForDay.size > 0) {
          const overallPerformance = aggregateOverallPerformance(accountsPerformanceForDay, date);
          
          // Write to main document (for latest state)
          await db.collection('portfolioPerformance').doc(userId)
            .set(overallPerformance, { merge: true });
          
          // Write to dates/{date} subcollection
          await db.collection('portfolioPerformance').doc(userId)
            .collection('dates').doc(date)
            .set(overallPerformance, { merge: true });
        }
        
        if (anyAccountProcessed) {
          daysProcessed++;
        }
        
      } catch (dayError) {
        const errMsg = `Day ${date}: ${dayError.message}`;
        errors.push(errMsg);
        console.error(`[backfillCore] User ${userId}:`, errMsg);
      }
    }
    
    console.log(`[backfillCore] Completed ATOMIC backfill for user ${userId}: ${daysProcessed}/${tradingDays.length} days, ${accounts.length} accounts`);
    
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
  getPreviousTradingDay,
  isDateEqual,
  calculateDailyCashFlow,
  calculateDailyDonePnL,
  
  // Data fetching
  fetchHistoricalPrices,
  fetchHistoricalExchangeRate,
  getPriceForDate,
  
  // User data
  getUserAccountIds,
  getUserAccounts,
  getAssetsForUser,
  getTransactionsForUser,
  getTransactionsByAccount,
  
  // State reconstruction
  reconstructAssetStateForDate,
  
  // Performance calculation helpers
  calculateAccountDayPerformance,
  aggregateOverallPerformance,
  
  // Snapshot (SCALE-003)
  capturePreBackfillSnapshot,
  
  // Main backfill
  backfillUserPerformance,
};
