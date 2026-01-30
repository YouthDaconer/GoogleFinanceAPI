/**
 * Script de Backfill de Portfolio Performance
 * 
 * PROPÓSITO:
 * Reconstruir datos de rendimiento histórico faltantes utilizando:
 * - Precios históricos reales de activos (API finance-query)
 * - Tipos de cambio históricos (Yahoo Finance)
 * - Transacciones históricas (Firestore)
 * 
 * USO:
 *   node backfillPortfolioPerformance.js --dry-run    # Ver cambios sin aplicar
 *   node backfillPortfolioPerformance.js --fix        # Aplicar cambios a Firestore
 *   node backfillPortfolioPerformance.js --analyze    # Solo análisis de gaps
 * 
 * OPCIONES ADICIONALES:
 *   --user=<userId>        # Usuario específico (default: DDeR8P5hYgfuN8gcU4RsQfdTJqx2)
 *   --account=<accountId>  # Cuenta específica (default: todas las cuentas del usuario)
 *   --start=YYYY-MM-DD     # Fecha inicio (default: 2025-01-02)
 *   --end=YYYY-MM-DD       # Fecha fin (default: 2025-05-31)
 * 
 * MÉTODO DE AGREGACIÓN OVERALL:
 * Para calcular el adjustedDailyChangePercentage de OVERALL (combinación de cuentas),
 * este script usa el método de "valor pre-cambio":
 * 
 *   preChangeValue = currentValue / (1 + change/100)
 *   combinedChange = Σ(preChangeValue_i × change_i) / Σ(preChangeValue_i)
 * 
 * Este método garantiza que el cambio combinado esté siempre entre el mínimo
 * y máximo de las cuentas individuales, y es matemáticamente correcto para TWR.
 * 
 * @see docs/stories/26.story.md (Backfill de datos históricos)
 * @see fixOverallComplete.js (Script alternativo para corregir OVERALL existente)
 */

const admin = require('firebase-admin');
const fetch = require('node-fetch');

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
  // API de precios históricos
  HISTORICAL_API_BASE: 'https://354sdh5hcrnztw5vquw6sxiduu0gigku.lambda-url.us-east-1.on.aws/v1',
  
  // Monedas activas
  CURRENCIES: ['USD', 'COP', 'EUR', 'MXN', 'BRL', 'GBP', 'CAD'],
  
  // Días festivos NYSE 2024
  NYSE_HOLIDAYS_2024: [
    '2024-01-01', '2024-01-15', '2024-02-19', '2024-03-29',
    '2024-05-27', '2024-06-19', '2024-07-04', '2024-09-02',
    '2024-11-28', '2024-12-25'
  ],
  
  // Días festivos NYSE 2025
  NYSE_HOLIDAYS_2025: [
    '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18',
    '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01',
    '2025-11-27', '2025-12-25'
  ],
  
  // Días festivos NYSE 2026
  NYSE_HOLIDAYS_2026: [
    '2026-01-01', // New Year's Day
    '2026-01-19', // MLK Day
    '2026-02-16', // Presidents Day
    '2026-04-03', // Good Friday
    '2026-05-25', // Memorial Day
    '2026-06-19', // Juneteenth
    '2026-07-03', // Independence Day (observed)
    '2026-09-07', // Labor Day
    '2026-11-26', // Thanksgiving
    '2026-12-25', // Christmas
  ],
  
  // Defaults
  DEFAULT_USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  DEFAULT_START_DATE: '2025-01-02',
  DEFAULT_END_DATE: '2025-05-31',
  
  // Rate limiting
  API_DELAY_MS: 200,
  BATCH_SIZE: 20, // Reducido para evitar "Transaction too big" con documentos grandes
};

// ============================================================================
// UTILIDADES
// ============================================================================

/**
 * Parsear argumentos de línea de comandos
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'analyze', // analyze, dry-run, fix
    userId: CONFIG.DEFAULT_USER_ID,
    accountId: null, // null = todas las cuentas
    startDate: CONFIG.DEFAULT_START_DATE,
    endDate: CONFIG.DEFAULT_END_DATE,
    overwrite: false, // Si true, sobrescribe documentos existentes
  };

  args.forEach(arg => {
    if (arg === '--dry-run') options.mode = 'dry-run';
    else if (arg === '--fix') options.mode = 'fix';
    else if (arg === '--analyze') options.mode = 'analyze';
    else if (arg === '--overwrite') options.overwrite = true;
    else if (arg.startsWith('--user=')) options.userId = arg.split('=')[1];
    else if (arg.startsWith('--account=')) options.accountId = arg.split('=')[1];
    else if (arg.startsWith('--start=')) options.startDate = arg.split('=')[1];
    else if (arg.startsWith('--end=')) options.endDate = arg.split('=')[1];
  });

  return options;
}

/**
 * Generar días hábiles NYSE entre dos fechas
 */
function generateBusinessDays(startDate, endDate) {
  const days = [];
  // Usar hora fija para evitar problemas de zona horaria
  let current = new Date(startDate + 'T12:00:00Z');
  const end = new Date(endDate + 'T12:00:00Z');
  
  // Combinar festivos de todos los años
  const allHolidays = [
    ...CONFIG.NYSE_HOLIDAYS_2024, 
    ...CONFIG.NYSE_HOLIDAYS_2025,
    ...CONFIG.NYSE_HOLIDAYS_2026
  ];

  while (current <= end) {
    // Usar getUTCDay para evitar problemas de zona horaria
    const dayOfWeek = current.getUTCDay();
    const dateStr = current.toISOString().split('T')[0];
    
    // Excluir fines de semana (0=domingo, 6=sábado) y festivos
    if (dayOfWeek !== 0 && dayOfWeek !== 6 && !allHolidays.includes(dateStr)) {
      days.push(dateStr);
    }
    current.setUTCDate(current.getUTCDate() + 1);
  }
  
  return days;
}

/**
 * FIX-TIMESTAMP-002: Extrae la parte de fecha (YYYY-MM-DD) de un string
 * Soporta tanto formato solo fecha como timestamp completo
 * @param {string} dateString - Fecha en formato YYYY-MM-DD o YYYY-MM-DDTHH:MM:SS.sssZ
 * @returns {string} Fecha en formato YYYY-MM-DD
 */
function getDatePart(dateString) {
  if (!dateString) return '';
  // Si tiene 'T', tomar solo los primeros 10 caracteres (YYYY-MM-DD)
  if (dateString.includes('T')) {
    return dateString.substring(0, 10);
  }
  return dateString;
}

/**
 * FIX-TIMESTAMP-002: Compara si una fecha de transacción está en o antes de una fecha objetivo
 * @param {string} txDate - Fecha de la transacción (puede tener timestamp)
 * @param {string} targetDate - Fecha objetivo en formato YYYY-MM-DD
 * @returns {boolean} true si txDate <= targetDate
 */
function isDateOnOrBefore(txDate, targetDate) {
  return getDatePart(txDate) <= targetDate;
}

/**
 * FIX-TIMESTAMP-002: Compara si una fecha de transacción es exactamente igual a una fecha objetivo
 * @param {string} txDate - Fecha de la transacción (puede tener timestamp)
 * @param {string} targetDate - Fecha objetivo en formato YYYY-MM-DD
 * @returns {boolean} true si txDate === targetDate (ignorando hora)
 */
function isDateEqual(txDate, targetDate) {
  return getDatePart(txDate) === targetDate;
}

/**
 * FIX-TIMESTAMP-002: Compara si una fecha de transacción está después de una fecha
 * @param {string} txDate - Fecha de la transacción (puede tener timestamp)
 * @param {string} afterDate - Fecha a comparar en formato YYYY-MM-DD
 * @returns {boolean} true si txDate > afterDate
 */
function isDateAfter(txDate, afterDate) {
  return getDatePart(txDate) > afterDate;
}

/**
 * Sleep utility para rate limiting
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Logger con timestamp
 */
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

// ============================================================================
// OBTENCIÓN DE DATOS
// ============================================================================

/**
 * Obtener precios históricos de un símbolo
 * @param {string} symbol - Ticker del activo
 * @param {string} startDate - Fecha de inicio para determinar el rango
 * @returns {Object} Map de fecha -> precio de cierre
 */
async function fetchHistoricalPrices(symbol, startDate = null) {
  try {
    // Determinar el rango basado en la fecha de inicio
    let range = 'ytd';
    if (startDate) {
      const start = new Date(startDate);
      const now = new Date();
      const monthsAgo = (now.getFullYear() - start.getFullYear()) * 12 + (now.getMonth() - start.getMonth());
      
      if (monthsAgo > 12) {
        range = '2y'; // Más de 1 año, usar 2 años
      } else if (monthsAgo > 6) {
        range = '1y'; // Más de 6 meses, usar 1 año
      }
    }
    
    const url = `${CONFIG.HISTORICAL_API_BASE}/historical?symbol=${encodeURIComponent(symbol)}&range=${range}&interval=1d`;
    const response = await fetch(url);
    
    if (!response.ok) {
      log('WARNING', `No se pudieron obtener precios para ${symbol}: ${response.status}`);
      return {};
    }
    
    const data = await response.json();
    
    // Convertir a map fecha -> close price
    const priceMap = {};
    Object.entries(data).forEach(([date, ohlcv]) => {
      priceMap[date] = ohlcv.close;
    });
    
    return priceMap;
  } catch (error) {
    log('ERROR', `Error obteniendo precios históricos para ${symbol}`, { error: error.message });
    return {};
  }
}

/**
 * Obtener tipo de cambio histórico
 * @param {string} currency - Código de moneda (COP, EUR, etc.)
 * @param {Date} date - Fecha objetivo
 * @returns {number|null} Tipo de cambio relativo a USD
 */
async function fetchHistoricalExchangeRate(currency, date) {
  if (currency === 'USD') return 1;
  
  try {
    // Determinar el símbolo correcto del par de divisas
    let symbol;
    if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
      symbol = `${currency}USD`; // Estas monedas cotizan como XXXUSD
    } else {
      symbol = `USD${currency}`; // Las demás cotizan como USDXXX
    }
    
    const timestamp = Math.floor(date.getTime() / 1000);
    const nextDay = timestamp + 86400;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}%3DX?period1=${timestamp}&period2=${nextDay}&interval=1d`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    if (data.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.[0]) {
      let rate = data.chart.result[0].indicators.quote[0].close[0];
      
      // Para EUR y GBP, necesitamos invertir (son XXXUSD, pero queremos USD/XXX)
      if (['EUR', 'GBP', 'AUD', 'NZD'].includes(currency)) {
        rate = 1 / rate;
      }
      
      return rate;
    }
    
    return null;
  } catch (error) {
    log('WARNING', `Error obteniendo tipo de cambio para ${currency}`, { error: error.message });
    return null;
  }
}

/**
 * Obtener todas las transacciones de una cuenta
 */
async function getTransactions(accountId) {
  const snapshot = await db.collection('transactions')
    .where('portfolioAccountId', '==', accountId)
    .orderBy('date', 'asc')
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Obtener documentos de performance existentes para una cuenta
 */
async function getExistingPerformance(userId, accountId, startDate, endDate) {
  const path = accountId 
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates`
    : `portfolioPerformance/${userId}/dates`;
  
  const snapshot = await db.collection(path)
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .orderBy('date', 'asc')
    .get();
  
  const existing = new Map();
  snapshot.docs.forEach(doc => {
    existing.set(doc.data().date, doc.data());
  });
  
  return existing;
}

/**
 * Obtener el último documento de performance ANTES de una fecha específica.
 * Útil para calcular dailyChangePercentage del primer día de un backfill.
 */
async function getLastPerformanceBefore(userId, accountId, beforeDate) {
  const path = accountId 
    ? `portfolioPerformance/${userId}/accounts/${accountId}/dates`
    : `portfolioPerformance/${userId}/dates`;
  
  const snapshot = await db.collection(path)
    .where('date', '<', beforeDate)
    .orderBy('date', 'desc')
    .limit(1)
    .get();
  
  if (snapshot.empty) return null;
  return snapshot.docs[0].data();
}

/**
 * Obtener cuentas del usuario
 */
async function getUserAccounts(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

// ============================================================================
// CÁLCULO DE HOLDINGS
// ============================================================================

/**
 * Calcular holdings (posiciones) hasta una fecha específica
 * @param {Array} transactions - Todas las transacciones de la cuenta
 * @param {string} targetDate - Fecha límite (inclusive)
 * @param {Object} exchangeRates - Tipos de cambio para convertir a USD (ej: { COP: 3750, EUR: 0.85 })
 * @returns {Object} { holdings: Map<assetKey, {units, totalInvestment, assetType}>, totalCashFlow }
 */
function calculateHoldingsAtDate(transactions, targetDate, exchangeRates = {}) {
  const holdings = new Map();
  let totalInvestmentUSD = 0;
  let totalCashFlowUSD = 0;
  
  // FIX-TIMESTAMP-002: Filtrar transacciones hasta la fecha objetivo (soporta timestamps)
  const relevantTx = transactions.filter(tx => isDateOnOrBefore(tx.date, targetDate));
  
  // BUGFIX: Ordenar transacciones para garantizar que BUY se procese antes de SELL
  // en el mismo día. Sin esto, el orden depende del document ID de Firestore,
  // lo cual puede causar que SELL se procese antes de que exista el holding.
  // NOTA: Usamos ?? en lugar de || porque 0 es un valor válido (buy=0)
  // FIX-TIMESTAMP-003: Usar getDatePart() para comparar solo la parte de fecha,
  // no el timestamp completo. Esto asegura que transacciones del mismo día
  // se ordenen por tipo (BUY antes de SELL) independientemente del timestamp.
  const typeOrder = { 'buy': 0, 'cash_income': 1, 'dividendPay': 2, 'sell': 3, 'cash_outcome': 4 };
  relevantTx.sort((a, b) => {
    // Primero ordenar por fecha (solo parte YYYY-MM-DD, ignorando timestamp)
    const dateA = getDatePart(a.date);
    const dateB = getDatePart(b.date);
    if (dateA !== dateB) return dateA.localeCompare(dateB);
    // Luego por tipo: buy/cash_income antes de sell/cash_outcome
    const orderA = typeOrder[a.type] ?? 99;
    const orderB = typeOrder[b.type] ?? 99;
    return orderA - orderB;
  });
  
  relevantTx.forEach(tx => {
    const assetKey = tx.assetName ? `${tx.assetName}_${tx.assetType || 'stock'}` : null;
    
    // Determinar el factor de conversión a USD
    // Si la transacción está en una moneda diferente a USD, convertir
    const txCurrency = tx.currency || 'USD';
    let conversionRate = 1;
    if (txCurrency !== 'USD' && exchangeRates[txCurrency]) {
      // exchangeRates[COP] = 3750 significa 1 USD = 3750 COP
      // Para convertir COP a USD: valor_COP / exchangeRates[COP]
      conversionRate = 1 / exchangeRates[txCurrency];
    }
    
    // Procesar según tipo de transacción
    switch (tx.type) {
      case 'buy':
        if (assetKey) {
          const current = holdings.get(assetKey) || { 
            units: 0, 
            totalInvestment: 0, 
            assetType: tx.assetType || 'stock',
            symbol: tx.assetName,
            currency: txCurrency // Guardar la moneda del activo para conversión de precios
          };
          current.units += tx.amount || 0;
          // Convertir la inversión a USD
          const investmentInTxCurrency = (tx.amount || 0) * (tx.price || 0);
          const investmentInUSD = investmentInTxCurrency * conversionRate;
          current.totalInvestment += investmentInUSD;
          // Mantener la moneda original del activo (la primera compra define la moneda)
          if (!current.currency) {
            current.currency = txCurrency;
          }
          holdings.set(assetKey, current);
          totalInvestmentUSD += investmentInUSD;
        }
        break;
        
      case 'sell':
        if (assetKey) {
          const current = holdings.get(assetKey);
          if (current) {
            // Reducir unidades
            // Nota: avgCost ya está en USD porque se convirtió al comprar
            const soldUnits = tx.amount || 0;
            const avgCost = current.units > 0 ? current.totalInvestment / current.units : 0;
            current.units -= soldUnits;
            current.totalInvestment -= soldUnits * avgCost;
            
            // Eliminar si no quedan unidades
            if (current.units <= 0.0001) {
              holdings.delete(assetKey);
            } else {
              holdings.set(assetKey, current);
            }
            totalInvestmentUSD -= soldUnits * avgCost;
          }
        }
        break;
        
      case 'cash_income':
        // Ingreso de efectivo - solo afecta cashflow, NO inversión
        // Convertir a USD si está en otra moneda
        totalCashFlowUSD += (tx.amount || 0) * conversionRate;
        // NO sumar a totalInvestmentUSD
        break;
        
      case 'cash_outcome':
        // Retiro de efectivo - solo afecta cashflow, NO inversión
        // Convertir a USD si está en otra moneda
        totalCashFlowUSD -= (tx.amount || 0) * conversionRate;
        // NO restar de totalInvestmentUSD
        break;
        
      case 'dividendPay':
        // Dividendos recibidos (no afectan holdings)
        break;
    }
  });
  
  return {
    holdings,
    totalInvestmentUSD: Math.max(0, totalInvestmentUSD),
    dailyCashFlow: calculateDailyCashFlow(transactions, targetDate)
  };
}

/**
 * Calcular P&L realizado del día específico (doneProfitAndLoss)
 * 
 * El doneProfitAndLoss es la suma de los valuePnL de las ventas del día.
 * Esto representa las ganancias/pérdidas REALIZADAS por ventas.
 * 
 * @param {Array} transactions - Todas las transacciones
 * @param {string} targetDate - Fecha objetivo
 * @returns {Object} { total: number, byAsset: Map<assetKey, number> }
 */
function calculateDailyDonePnL(transactions, targetDate) {
  const byAsset = new Map();
  let total = 0;
  
  // FIX-TIMESTAMP-002: Usar isDateEqual para soportar timestamps
  transactions
    .filter(tx => isDateEqual(tx.date, targetDate) && tx.type === 'sell')
    .forEach(tx => {
      const pnl = tx.valuePnL || 0;
      total += pnl;
      
      // Por asset (assetName_assetType)
      const assetKey = `${tx.assetName}_${tx.assetType}`;
      byAsset.set(assetKey, (byAsset.get(assetKey) || 0) + pnl);
    });
  
  return { total, byAsset };
}

/**
 * Calcular cash flow del día específico
 * 
 * CONVENCIÓN DE SIGNOS PARA TWR:
 * - Negativo: dinero que sale del bolsillo del inversor hacia el portfolio
 *   (compras de activos)
 * - Positivo: dinero que entra al bolsillo del inversor desde el portfolio
 *   (ventas de activos)
 * 
 * NOTA: cash_income y cash_outcome NO se incluyen en el cashflow para TWR
 * porque representan transferencias internas de efectivo dentro del portfolio,
 * no inyecciones/retiros reales de capital. El efectivo ya está en el portfolio
 * como parte del valor total (aunque no visible en los activos).
 */
function calculateDailyCashFlow(transactions, targetDate) {
  // FIX-TIMESTAMP-002: Usar isDateEqual para soportar timestamps
  return transactions
    .filter(tx => isDateEqual(tx.date, targetDate))
    .reduce((sum, tx) => {
      if (tx.type === 'buy') return sum - (tx.amount || 0) * (tx.price || 0);
      if (tx.type === 'sell') return sum + (tx.amount || 0) * (tx.price || 0);
      // cash_income/cash_outcome NO se incluyen - son transferencias internas
      return sum;
    }, 0);
}

/**
 * Calcular cashflow acumulado desde una fecha hasta otra (exclusive end)
 * Incluye cashflows de días intermedios que no tienen documento
 * 
 * @param {Array} transactions - Todas las transacciones
 * @param {string} startDateExclusive - Fecha inicio (exclusive)
 * @param {string} endDateInclusive - Fecha fin (inclusive)
 * @returns {number} Cashflow acumulado
 */
function calculateAccumulatedCashFlow(transactions, startDateExclusive, endDateInclusive) {
  // FIX-TIMESTAMP-002: Usar funciones helper para soportar timestamps
  return transactions
    .filter(tx => isDateAfter(tx.date, startDateExclusive) && isDateOnOrBefore(tx.date, endDateInclusive))
    .reduce((sum, tx) => {
      if (tx.type === 'buy') return sum - (tx.amount || 0) * (tx.price || 0);
      if (tx.type === 'sell') return sum + (tx.amount || 0) * (tx.price || 0);
      // cash_income/cash_outcome NO se incluyen - son transferencias internas
      return sum;
    }, 0);
}

// ============================================================================
// CÁLCULO DE PERFORMANCE
// ============================================================================

/**
 * Verificar si un día tiene precios de mercado reales
 * Un día tiene precios si AL MENOS UN símbolo tiene precio exacto para esa fecha
 * @param {Map} pricesBySymbol - Mapa de símbolo -> mapa de fecha -> precio
 * @param {string} targetDate - Fecha a verificar
 * @returns {boolean} true si hay precios para esa fecha
 */
function dayHasMarketPrices(pricesBySymbol, targetDate) {
  for (const [symbol, prices] of pricesBySymbol) {
    if (prices && prices[targetDate] !== undefined) {
      return true;
    }
  }
  return false;
}

/**
 * Obtener precio para una fecha, con fallback al día anterior más cercano
 */
function getPriceForDate(symbolPrices, targetDate, allDates) {
  if (!symbolPrices || Object.keys(symbolPrices).length === 0) return 0;
  
  // Primero intentar con la fecha exacta
  if (symbolPrices[targetDate]) {
    return symbolPrices[targetDate];
  }
  
  // Si no hay precio para esa fecha, buscar el día anterior más cercano
  const sortedDates = Object.keys(symbolPrices).sort().reverse();
  for (const date of sortedDates) {
    if (date < targetDate) {
      return symbolPrices[date];
    }
  }
  
  // Si no hay fecha anterior, usar la primera disponible
  return symbolPrices[sortedDates[sortedDates.length - 1]] || 0;
}

/**
 * Calcular performance para un día específico
 * 
 * IMPORTANTE: Las métricas de cambio diario siguen las convenciones del scheduler:
 * - dailyChangePercentage: Cambio bruto (endValue - startValue) / startValue * 100
 * - rawDailyChangePercentage: Igual que dailyChangePercentage (cambio sin ajustes)
 * - adjustedDailyChangePercentage: Cambio ajustado por cashflows usando fórmula TWR
 *   Formula: (endValue - startValue + cashFlow) / startValue * 100
 *   Donde cashFlow es NEGATIVO para compras (sale dinero) y POSITIVO para ventas (entra dinero)
 *   Al sumar el cashFlow, las compras se "cancelan" del rendimiento
 * 
 * @param {Map} holdings - Holdings del día
 * @param {Map} pricesBySymbol - Precios por símbolo
 * @param {string} targetDate - Fecha objetivo
 * @param {Object} exchangeRates - Tasas de cambio por moneda
 * @param {number} totalInvestmentUSD - Inversión total en USD
 * @param {number} dailyCashFlowUSD - Cash flow del día en USD
 * @param {Object} previousDayPerformance - Performance del día anterior
 * @param {Map} previousDayHoldings - Holdings del día anterior
 * @param {Object} dailyDonePnL - P&L realizado del día { total: number, byAsset: Map }
 */
function calculateDayPerformance(
  holdings,
  pricesBySymbol,
  targetDate,
  exchangeRates,
  totalInvestmentUSD,
  dailyCashFlowUSD,
  previousDayPerformance,
  previousDayHoldings,
  dailyDonePnL = { total: 0, byAsset: new Map() }
) {
  const result = {};
  
  CONFIG.CURRENCIES.forEach(currency => {
    const exchangeRate = exchangeRates[currency] || 1;
    
    // Calcular valor total y performance por activo
    let totalValue = 0;
    const assetPerformance = {};
    
    // Calcular doneProfitAndLoss para esta moneda
    const dailyDonePnLForCurrency = dailyDonePnL.total * exchangeRate;
    
    holdings.forEach((holding, assetKey) => {
      const symbolPrices = pricesBySymbol.get(holding.symbol);
      const priceInAssetCurrency = getPriceForDate(symbolPrices, targetDate);
      
      // IMPORTANTE: Convertir el precio a USD si el activo cotiza en otra moneda
      // El precio viene en la moneda del activo (holding.currency), necesitamos convertir a USD
      let priceInUSD = priceInAssetCurrency;
      const assetCurrency = holding.currency || 'USD';
      if (assetCurrency !== 'USD' && exchangeRates[assetCurrency]) {
        // exchangeRates[COP] = 3750 significa 1 USD = 3750 COP
        // Para convertir COP a USD: precio_COP / exchangeRates[COP]
        priceInUSD = priceInAssetCurrency / exchangeRates[assetCurrency];
      }
      
      const valueUSD = holding.units * priceInUSD;
      const value = valueUSD * exchangeRate;
      const investment = holding.totalInvestment * exchangeRate;
      
      // doneProfitAndLoss por activo (del día)
      const assetDonePnL = (dailyDonePnL.byAsset.get(assetKey) || 0) * exchangeRate;
      
      totalValue += value;
      
      assetPerformance[assetKey] = {
        units: holding.units,
        totalValue: value,
        totalInvestment: investment,
        totalCashFlow: 0,
        unrealizedProfitAndLoss: value - investment,
        doneProfitAndLoss: assetDonePnL,
        totalROI: investment > 0 ? ((value - investment) / investment) * 100 : 0,
        dailyChangePercentage: 0,
        rawDailyChangePercentage: 0,
        adjustedDailyChangePercentage: 0,
        dailyReturn: 0,
        monthlyReturn: 0,
        annualReturn: 0,
      };
    });
    
    const totalInvestment = totalInvestmentUSD * exchangeRate;
    // dailyCashFlowUSD ya viene con signo correcto: negativo para compras, positivo para ventas
    const totalCashFlow = dailyCashFlowUSD * exchangeRate;
    
    const prevPerf = previousDayPerformance?.[currency];
    const previousTotalValue = prevPerf?.totalValue || 0;
    
    // Determinar si es una nueva inversión (no teníamos valor ayer pero sí hoy)
    const isNewInvestment = previousTotalValue === 0 && totalValue > 0;
    
    // =========================================================================
    // CÁLCULO DE MÉTRICAS DE CAMBIO DIARIO
    // =========================================================================
    
    // 1. rawDailyChangePercentage: Cambio bruto sin ajustes
    //    Formula: (endValue - startValue) / startValue * 100
    let rawDailyChangePercentage = 0;
    if (previousTotalValue > 0) {
      rawDailyChangePercentage = ((totalValue - previousTotalValue) / previousTotalValue) * 100;
    }
    
    // 2. adjustedDailyChangePercentage: Cambio ajustado por cashflows (TWR)
    //    Formula: (endValue - startValue + cashFlow) / startValue * 100
    //    cashFlow es negativo para compras, positivo para ventas
    //    Al sumarlo, las compras "se cancelan" del rendimiento
    let adjustedDailyChangePercentage = 0;
    if (isNewInvestment) {
      // Primera inversión: rendimiento 0%
      adjustedDailyChangePercentage = 0;
    } else if (previousTotalValue > 0) {
      // Fórmula TWR: el cashFlow ya tiene signo correcto
      adjustedDailyChangePercentage = ((totalValue - previousTotalValue + totalCashFlow) / previousTotalValue) * 100;
    }
    
    // =========================================================================
    // FIX-BACKFILL-001: ELIMINADA la lógica de "corrección" de cambios anormales
    // 
    // La lógica anterior ponía 0% cuando detectaba cambios > 5% sin cashflow
    // significativo. Esto era INCORRECTO porque:
    // 1. El mercado puede subir/bajar más de 5% en un día
    // 2. Compras pequeñas pueden cambiar significativamente el promedio
    // 3. Los datos reales del mercado son la fuente de verdad
    // 
    // Si hay datos incorrectos, deben corregirse en la fuente (assets, 
    // transactions, precios), no enmascarándolos con 0%.
    // =========================================================================
    
    // 3. dailyChangePercentage: Por convención, igual que rawDailyChangePercentage
    const dailyChangePercentage = rawDailyChangePercentage;
    
    // 4. dailyReturn: Formato decimal del adjustedDailyChangePercentage
    const dailyReturn = adjustedDailyChangePercentage / 100;
    
    result[currency] = {
      totalValue,
      totalInvestment,
      totalCashFlow,
      doneProfitAndLoss: dailyDonePnLForCurrency,
      unrealizedProfitAndLoss: totalValue - totalInvestment,
      totalROI: totalInvestment > 0 ? ((totalValue - totalInvestment) / totalInvestment) * 100 : 0,
      dailyChangePercentage,
      rawDailyChangePercentage,
      adjustedDailyChangePercentage,
      dailyReturn,
      monthlyReturn: 0,
      annualReturn: 0,
      assetPerformance,
    };
    
    // =========================================================================
    // CÁLCULO DE MÉTRICAS POR ACTIVO
    // =========================================================================
    Object.entries(assetPerformance).forEach(([assetKey, perf]) => {
      const prevAssetPerf = prevPerf?.assetPerformance?.[assetKey];
      const prevAssetValue = prevAssetPerf?.totalValue || 0;
      const prevAssetUnits = prevAssetPerf?.units || 0;
      
      // Detectar si hubo cashflow en este activo (cambio en unidades)
      const unitsDiff = perf.units - prevAssetUnits;
      const hadAssetCashFlow = Math.abs(unitsDiff) > 0.0001;
      
      // PROTECCIÓN: Si el valor anterior es muy pequeño o cero, es nueva inversión
      // Esto evita división por cero y valores astronómicos cuando un asset
      // reaparece después de haber sido vendido completamente
      const isAssetNewInvestment = prevAssetValue < 0.01 && perf.totalValue > 0;
      
      if (isAssetNewInvestment) {
        // Nueva inversión o reaparición después de venta: 0%
        perf.dailyChangePercentage = 0;
        perf.rawDailyChangePercentage = 0;
        perf.adjustedDailyChangePercentage = 0;
        perf.dailyReturn = 0;
      } else if (prevAssetValue >= 0.01) {
        // Calcular cambio bruto
        perf.rawDailyChangePercentage = ((perf.totalValue - prevAssetValue) / prevAssetValue) * 100;
        perf.dailyChangePercentage = perf.rawDailyChangePercentage;
        
        // Calcular cambio ajustado
        if (hadAssetCashFlow) {
          // Estimar cashflow del activo basado en el cambio de inversión
          const assetCashFlow = -(perf.totalInvestment - (prevAssetPerf?.totalInvestment || 0));
          perf.adjustedDailyChangePercentage = ((perf.totalValue - prevAssetValue + assetCashFlow) / prevAssetValue) * 100;
        } else {
          // Sin cashflow: adjusted = raw
          perf.adjustedDailyChangePercentage = perf.rawDailyChangePercentage;
        }
        
        // PROTECCIÓN: Limitar cambios extremos a ±50%
        // Cambios mayores indican problemas de datos (gaps, precios incorrectos)
        if (Math.abs(perf.adjustedDailyChangePercentage) > 50) {
          perf.adjustedDailyChangePercentage = 0;
          perf.rawDailyChangePercentage = 0;
          perf.dailyChangePercentage = 0;
        }
        
        perf.dailyReturn = perf.adjustedDailyChangePercentage / 100;
      }
    });
  });
  
  return result;
}

// ============================================================================
// PROCESO PRINCIPAL
// ============================================================================

async function main() {
  const options = parseArgs();
  
  console.log('');
  console.log('═'.repeat(80));
  console.log('  BACKFILL DE PORTFOLIO PERFORMANCE');
  console.log('═'.repeat(80));
  console.log('');
  log('INFO', 'Configuración:', options);
  console.log('');
  
  // 1. Obtener cuentas del usuario
  log('PROGRESS', 'Obteniendo cuentas del usuario...');
  const accounts = await getUserAccounts(options.userId);
  log('SUCCESS', `Encontradas ${accounts.length} cuentas activas`);
  
  // Filtrar por cuenta específica si se proporcionó
  const targetAccounts = options.accountId 
    ? accounts.filter(a => a.id === options.accountId)
    : accounts;
  
  if (targetAccounts.length === 0) {
    log('ERROR', 'No se encontraron cuentas para procesar');
    process.exit(1);
  }
  
  // 2. Generar días hábiles esperados
  const expectedDays = generateBusinessDays(options.startDate, options.endDate);
  log('INFO', `Días hábiles en el período: ${expectedDays.length}`);
  
  // 3. Recopilar todos los símbolos únicos para obtener precios
  log('PROGRESS', 'Analizando transacciones para identificar símbolos...');
  const allSymbols = new Set();
  const transactionsByAccount = new Map();
  
  for (const account of targetAccounts) {
    const transactions = await getTransactions(account.id);
    transactionsByAccount.set(account.id, transactions);
    
    transactions.forEach(tx => {
      if (tx.assetName) {
        allSymbols.add(tx.assetName);
      }
    });
  }
  
  log('SUCCESS', `Símbolos únicos identificados: ${allSymbols.size}`, [...allSymbols]);
  
  // 4. Obtener precios históricos para todos los símbolos
  log('PROGRESS', 'Obteniendo precios históricos...');
  const pricesBySymbol = new Map();
  
  for (const symbol of allSymbols) {
    log('DEBUG', `  Obteniendo precios para ${symbol}...`);
    const prices = await fetchHistoricalPrices(symbol, options.startDate);
    pricesBySymbol.set(symbol, prices);
    await sleep(CONFIG.API_DELAY_MS);
  }
  
  log('SUCCESS', 'Precios históricos obtenidos');
  
  // 5. Obtener tipos de cambio históricos para cada día a procesar
  log('PROGRESS', 'Obteniendo tipos de cambio históricos...');
  const exchangeRatesByDate = new Map();
  
  // Obtener para todos los días que vamos a procesar
  for (const account of targetAccounts) {
    const existing = await getExistingPerformance(
      options.userId, 
      account.id, 
      options.startDate, 
      options.endDate
    );
    
    // Con --overwrite, procesar todos los días; sin él, solo los faltantes
    const daysToProcess = options.overwrite 
      ? expectedDays 
      : expectedDays.filter(d => !existing.has(d));
    
    for (const date of daysToProcess) {
      if (!exchangeRatesByDate.has(date)) {
        const rates = { USD: 1 };
        const dateObj = new Date(date + 'T12:00:00Z');
        
        for (const currency of CONFIG.CURRENCIES.filter(c => c !== 'USD')) {
          const rate = await fetchHistoricalExchangeRate(currency, dateObj);
          if (rate) rates[currency] = rate;
          await sleep(50); // Rate limiting más suave para divisas
        }
        
        exchangeRatesByDate.set(date, rates);
        log('DEBUG', `  Tipos de cambio para ${date}:`, rates);
      }
    }
  }
  
  log('SUCCESS', 'Tipos de cambio obtenidos');
  
  // 6. Procesar cada cuenta
  const results = {
    accountsProcessed: 0,
    daysCreated: 0,
    daysSkipped: 0,
    errors: [],
  };
  
  for (const account of targetAccounts) {
    console.log('');
    log('PROGRESS', `═══ Procesando cuenta: ${account.name} (${account.id}) ═══`);
    
    const transactions = transactionsByAccount.get(account.id);
    const existing = await getExistingPerformance(
      options.userId, 
      account.id, 
      options.startDate, 
      options.endDate
    );
    
    // Si --overwrite está activo, procesar todos los días; de lo contrario solo los faltantes
    const missingDays = options.overwrite 
      ? expectedDays 
      : expectedDays.filter(d => !existing.has(d));
    
    if (options.overwrite) {
      log('INFO', `  [OVERWRITE] Procesando ${missingDays.length} días (sobrescribiendo existentes)`);
    } else {
      log('INFO', `  Días existentes: ${existing.size}, Días faltantes: ${missingDays.length}`);
    }
    
    if (missingDays.length === 0) {
      log('SUCCESS', '  No hay días para procesar en esta cuenta');
      continue;
    }
    
    // Procesar días faltantes en orden cronológico
    const documentsToWrite = [];
    
    // Mapa para guardar performance calculada por día (para días consecutivos faltantes)
    const calculatedPerformance = new Map();
    
    // Contador de días saltados por falta de precios
    let skippedNoPrices = 0;
    
    for (const date of missingDays.sort()) {
      // =====================================================================
      // IMPORTANTE: Verificar si hay precios de mercado para este día
      // Si no hay precios, es un día festivo o sin trading - saltar
      // =====================================================================
      if (!dayHasMarketPrices(pricesBySymbol, date)) {
        log('DEBUG', `  Saltando ${date} - sin precios de mercado`);
        skippedNoPrices++;
        continue;
      }
      
      log('DEBUG', `  Calculando performance para ${date}...`);
      
      try {
        // Buscar el día anterior más cercano (puede estar en existing O en calculatedPerformance)
        const allDaysSorted = [...expectedDays].sort();
        const currentIdx = allDaysSorted.indexOf(date);
        let previousDayPerformance = null;
        let previousDayDate = null;
        
        // Buscar hacia atrás hasta encontrar un día con datos
        for (let i = currentIdx - 1; i >= 0; i--) {
          const prevDate = allDaysSorted[i];
          if (calculatedPerformance.has(prevDate)) {
            // Usar el performance que acabamos de calcular en este backfill
            previousDayPerformance = calculatedPerformance.get(prevDate);
            previousDayDate = prevDate;
            break;
          } else if (existing.has(prevDate)) {
            // Usar el performance que ya existía en Firestore
            previousDayPerformance = existing.get(prevDate);
            previousDayDate = prevDate;
            break;
          }
        }
        
        // FIX: Si no encontramos día anterior dentro del rango, buscar ANTES del rango
        if (!previousDayPerformance && currentIdx === 0) {
          const beforeRangePerf = await getLastPerformanceBefore(options.userId, account.id, date);
          if (beforeRangePerf) {
            previousDayPerformance = beforeRangePerf;
            previousDayDate = beforeRangePerf.date;
            log('DEBUG', `    Usando documento anterior fuera del rango: ${previousDayDate}`);
          }
        }
        
        // Obtener tipos de cambio para esta fecha (necesario para convertir transacciones a USD)
        const exchangeRates = exchangeRatesByDate.get(date) || { USD: 1 };
        
        // Calcular holdings hasta esta fecha (pasando exchangeRates para conversión de moneda)
        const { holdings, totalInvestmentUSD } = calculateHoldingsAtDate(transactions, date, exchangeRates);
        
        // =====================================================================
        // IMPORTANTE: Calcular cashflow ACUMULADO desde el día anterior
        // Esto incluye cashflows de días intermedios sin documento
        // =====================================================================
        const accumulatedCashFlow = previousDayDate 
          ? calculateAccumulatedCashFlow(transactions, previousDayDate, date)
          : calculateDailyCashFlow(transactions, date); // Para el primer día
        
        // =====================================================================
        // Calcular P&L realizado del día (doneProfitAndLoss)
        // Suma de valuePnL de las ventas del día
        // =====================================================================
        const dailyDonePnL = calculateDailyDonePnL(transactions, date);
        
        // Calcular performance
        const performance = calculateDayPerformance(
          holdings,
          pricesBySymbol,
          date,
          exchangeRates,
          totalInvestmentUSD,
          accumulatedCashFlow,
          previousDayPerformance,
          null, // previousDayHoldings - no necesario, usamos previousDayPerformance
          dailyDonePnL
        );
        
        // Guardar para usar como previousDayPerformance en el siguiente día
        calculatedPerformance.set(date, performance);
        
        // Preparar documento
        const doc = {
          date,
          ...performance,
        };
        
        documentsToWrite.push({
          path: `portfolioPerformance/${options.userId}/accounts/${account.id}/dates/${date}`,
          data: doc,
        });
        
      } catch (error) {
        log('ERROR', `  Error procesando ${date}`, { error: error.message });
        results.errors.push({ date, account: account.id, error: error.message });
      }
    }
    
    // 7. Escribir a Firestore (según modo)
    if (skippedNoPrices > 0) {
      log('WARNING', `  Saltados ${skippedNoPrices} días sin precios de mercado`);
    }
    
    if (options.mode === 'dry-run') {
      log('INFO', `  [DRY-RUN] Se crearían ${documentsToWrite.length} documentos`);
      
      // Mostrar muestra de los primeros 3
      documentsToWrite.slice(0, 3).forEach(doc => {
        log('DEBUG', `  Documento: ${doc.path}`);
        log('DEBUG', `    USD totalValue: ${doc.data.USD?.totalValue?.toFixed(2)}`);
        log('DEBUG', `    USD totalInvestment: ${doc.data.USD?.totalInvestment?.toFixed(2)}`);
      });
      
      results.daysCreated += documentsToWrite.length;
      
    } else if (options.mode === 'fix') {
      log('PROGRESS', `  Escribiendo ${documentsToWrite.length} documentos a Firestore...`);
      
      // Escribir en batches
      for (let i = 0; i < documentsToWrite.length; i += CONFIG.BATCH_SIZE) {
        const batch = db.batch();
        const chunk = documentsToWrite.slice(i, i + CONFIG.BATCH_SIZE);
        
        chunk.forEach(doc => {
          const ref = db.doc(doc.path);
          batch.set(ref, doc.data, { merge: true });
        });
        
        await batch.commit();
        log('SUCCESS', `  Batch ${Math.floor(i / CONFIG.BATCH_SIZE) + 1} escrito (${chunk.length} docs)`);
      }
      
      results.daysCreated += documentsToWrite.length;
      
    } else {
      // Modo analyze: solo mostrar estadísticas
      log('INFO', `  [ANALYZE] ${missingDays.length} días faltantes`);
      results.daysSkipped += missingDays.length;
    }
    
    results.accountsProcessed++;
  }
  
  // 8. También actualizar nivel OVERALL (agregado de todas las cuentas)
  if (options.mode !== 'analyze') {
    log('PROGRESS', 'Procesando nivel OVERALL...');
    
    // Obtener TODAS las cuentas activas del usuario (no solo las procesadas)
    const allActiveAccounts = await getUserAccounts(options.userId);
    
    if (allActiveAccounts.length > 0) {
      // Encontrar todos los días únicos de todas las cuentas
      const allAccountDates = new Set();
      const accountPerformanceByDate = new Map(); // Map<date, Map<accountId, performance>>
      
      for (const account of allActiveAccounts) {
        const accountDocs = await db.collection(`portfolioPerformance/${options.userId}/accounts/${account.id}/dates`)
          .where('date', '>=', options.startDate)
          .where('date', '<=', options.endDate)
          .orderBy('date', 'asc')
          .get();
        
        accountDocs.docs.forEach(doc => {
          const data = doc.data();
          allAccountDates.add(data.date);
          
          if (!accountPerformanceByDate.has(data.date)) {
            accountPerformanceByDate.set(data.date, new Map());
          }
          accountPerformanceByDate.get(data.date).set(account.id, data);
        });
      }
      
      log('INFO', `  Días únicos en cuentas: ${allAccountDates.size}`);
      
      // Obtener días existentes en OVERALL
      const existingOverall = await getExistingPerformance(options.userId, null, options.startDate, options.endDate);
      
      // Si --overwrite está activo, procesar todos los días; de lo contrario solo los faltantes
      const missingOverallDays = options.overwrite
        ? [...allAccountDates].sort()
        : [...allAccountDates].filter(d => !existingOverall.has(d)).sort();
      
      if (options.overwrite) {
        log('INFO', `  [OVERWRITE] Procesando ${missingOverallDays.length} días OVERALL (sobrescribiendo existentes)`);
      } else {
        log('INFO', `  Días faltantes en OVERALL: ${missingOverallDays.length}`);
      }
      
      if (missingOverallDays.length > 0) {
        const overallDocuments = [];
        
        // Mapa para guardar OVERALL calculado (para calcular cambios diarios consecutivos)
        const calculatedOverall = new Map();
        
        for (const date of missingOverallDays) {
          const accountsData = accountPerformanceByDate.get(date);
          if (!accountsData || accountsData.size === 0) continue;
          
          // Agregar datos de todas las cuentas para este día
          const aggregatedPerformance = {};
          
          CONFIG.CURRENCIES.forEach(currency => {
            let totalValue = 0;
            let totalInvestment = 0;
            let totalCashFlow = 0;
            let totalDoneProfitAndLoss = 0;
            const combinedAssetPerformance = {};
            
            // =================================================================
            // PASO 1: Calcular adjustedDailyChangePercentage usando método de
            // VALOR PRE-CAMBIO (promedio ponderado correcto)
            // =================================================================
            let totalPreChangeValue = 0;
            let weightedAdjustedChange = 0;
            let weightedRawChange = 0;
            
            // Sumar valores de todas las cuentas
            accountsData.forEach((perfData, accountId) => {
              const currencyData = perfData[currency];
              if (currencyData) {
                const accountValue = currencyData.totalValue || 0;
                const accountAdjChange = currencyData.adjustedDailyChangePercentage || 0;
                const accountRawChange = currencyData.rawDailyChangePercentage || currencyData.dailyChangePercentage || 0;
                
                totalValue += accountValue;
                totalInvestment += currencyData.totalInvestment || 0;
                totalCashFlow += currencyData.totalCashFlow || 0;
                totalDoneProfitAndLoss += currencyData.doneProfitAndLoss || 0;
                
                // Calcular valor pre-cambio para ponderación correcta
                if (accountValue > 0) {
                  const preChangeValue = accountAdjChange !== 0 
                    ? accountValue / (1 + accountAdjChange / 100) 
                    : accountValue;
                  
                  totalPreChangeValue += preChangeValue;
                  weightedAdjustedChange += preChangeValue * accountAdjChange;
                  weightedRawChange += preChangeValue * accountRawChange;
                }
                
                // Agregar assetPerformance
                if (currencyData.assetPerformance) {
                  Object.entries(currencyData.assetPerformance).forEach(([assetKey, assetPerf]) => {
                    if (!combinedAssetPerformance[assetKey]) {
                      combinedAssetPerformance[assetKey] = {
                        units: 0,
                        totalValue: 0,
                        totalInvestment: 0,
                        totalCashFlow: 0,
                        unrealizedProfitAndLoss: 0,
                        doneProfitAndLoss: 0,
                        totalROI: 0,
                        dailyChangePercentage: 0,
                        rawDailyChangePercentage: 0,
                        adjustedDailyChangePercentage: 0,
                        dailyReturn: 0,
                        monthlyReturn: 0,
                        annualReturn: 0,
                        // Para cálculo de valor pre-cambio
                        _preChangeValue: 0,
                        _weightedAdjChange: 0,
                        _weightedRawChange: 0,
                      };
                    }
                    const combined = combinedAssetPerformance[assetKey];
                    combined.units += assetPerf.units || 0;
                    combined.totalValue += assetPerf.totalValue || 0;
                    combined.totalInvestment += assetPerf.totalInvestment || 0;
                    combined.totalCashFlow += assetPerf.totalCashFlow || 0;
                    combined.unrealizedProfitAndLoss += assetPerf.unrealizedProfitAndLoss || 0;
                    combined.doneProfitAndLoss += assetPerf.doneProfitAndLoss || 0;
                    
                    // Calcular valor pre-cambio para ponderación correcta del asset
                    const assetValue = assetPerf.totalValue || 0;
                    const assetAdjChange = assetPerf.adjustedDailyChangePercentage || 0;
                    const assetRawChange = assetPerf.rawDailyChangePercentage || assetPerf.dailyChangePercentage || 0;
                    
                    if (assetValue > 0) {
                      const assetPreChange = assetAdjChange !== 0 
                        ? assetValue / (1 + assetAdjChange / 100) 
                        : assetValue;
                      
                      combined._preChangeValue += assetPreChange;
                      combined._weightedAdjChange += assetPreChange * assetAdjChange;
                      combined._weightedRawChange += assetPreChange * assetRawChange;
                    }
                  });
                }
              }
            });
            
            // Calcular métricas agregadas
            const unrealizedProfitAndLoss = totalValue - totalInvestment;
            const totalROI = totalInvestment > 0 ? ((totalValue - totalInvestment) / totalInvestment) * 100 : 0;
            
            // =================================================================
            // PASO 2: Usar los cambios ponderados por valor pre-cambio
            // Este método garantiza que el cambio combinado esté siempre entre
            // el mínimo y máximo de las cuentas individuales
            // =================================================================
            let rawDailyChangePercentage = 0;
            let adjustedDailyChangePercentage = 0;
            
            if (totalPreChangeValue > 0) {
              adjustedDailyChangePercentage = weightedAdjustedChange / totalPreChangeValue;
              rawDailyChangePercentage = weightedRawChange / totalPreChangeValue;
            }
            
            // =================================================================
            // PASO 3: Recalcular ROI y cambios diarios por activo
            // Usar el método de valor pre-cambio ya calculado durante la agregación
            // =================================================================
            Object.entries(combinedAssetPerformance).forEach(([assetKey, assetPerf]) => {
              // ROI
              if (assetPerf.totalInvestment > 0) {
                assetPerf.totalROI = ((assetPerf.totalValue - assetPerf.totalInvestment) / assetPerf.totalInvestment) * 100;
              }
              
              // Usar valores pre-calculados por método de valor pre-cambio
              if (assetPerf._preChangeValue > 0) {
                assetPerf.adjustedDailyChangePercentage = assetPerf._weightedAdjChange / assetPerf._preChangeValue;
                assetPerf.rawDailyChangePercentage = assetPerf._weightedRawChange / assetPerf._preChangeValue;
                assetPerf.dailyChangePercentage = assetPerf.rawDailyChangePercentage;
                assetPerf.dailyReturn = assetPerf.adjustedDailyChangePercentage / 100;
              }
              
              // Limpiar campos temporales
              delete assetPerf._preChangeValue;
              delete assetPerf._weightedAdjChange;
              delete assetPerf._weightedRawChange;
            });
            
            aggregatedPerformance[currency] = {
              totalValue,
              totalInvestment,
              totalCashFlow,
              doneProfitAndLoss: totalDoneProfitAndLoss,
              unrealizedProfitAndLoss,
              totalROI,
              dailyChangePercentage: rawDailyChangePercentage,
              rawDailyChangePercentage,
              adjustedDailyChangePercentage,
              dailyReturn: adjustedDailyChangePercentage / 100,
              monthlyReturn: 0,
              annualReturn: 0,
              assetPerformance: combinedAssetPerformance,
            };
          });
          
          // Guardar para cálculos consecutivos
          calculatedOverall.set(date, aggregatedPerformance);
          
          overallDocuments.push({
            path: `portfolioPerformance/${options.userId}/dates/${date}`,
            data: {
              date,
              ...aggregatedPerformance,
            },
          });
        }
        
        // Escribir documentos OVERALL
        if (options.mode === 'fix' && overallDocuments.length > 0) {
          log('PROGRESS', `  Escribiendo ${overallDocuments.length} documentos OVERALL...`);
          
          for (let i = 0; i < overallDocuments.length; i += CONFIG.BATCH_SIZE) {
            const batch = db.batch();
            const chunk = overallDocuments.slice(i, i + CONFIG.BATCH_SIZE);
            
            chunk.forEach(doc => {
              const ref = db.doc(doc.path);
              batch.set(ref, doc.data, { merge: true });
            });
            
            await batch.commit();
            log('SUCCESS', `    Batch OVERALL ${Math.floor(i / CONFIG.BATCH_SIZE) + 1} escrito (${chunk.length} docs)`);
          }
          
          results.daysCreated += overallDocuments.length;
        } else if (options.mode === 'dry-run') {
          log('INFO', `  [DRY-RUN] Se crearían ${overallDocuments.length} documentos OVERALL`);
        }
      } else {
        log('SUCCESS', '  OVERALL está completo, no hay días faltantes');
      }
    }
  }
  
  // 9. Resumen final
  console.log('');
  console.log('═'.repeat(80));
  console.log('  RESUMEN');
  console.log('═'.repeat(80));
  log('SUCCESS', `Cuentas procesadas: ${results.accountsProcessed}`);
  log('SUCCESS', `Días creados/simulados: ${results.daysCreated}`);
  if (results.daysSkipped > 0) log('INFO', `Días omitidos (analyze): ${results.daysSkipped}`);
  if (results.errors.length > 0) log('WARNING', `Errores: ${results.errors.length}`);
  
  if (options.mode === 'dry-run') {
    console.log('');
    log('INFO', '⚠️  Este fue un DRY-RUN. Para aplicar los cambios, ejecuta:');
    console.log('    node backfillPortfolioPerformance.js --fix');
  }
  
  console.log('');
  process.exit(results.errors.length > 0 ? 1 : 0);
}

// Ejecutar
main().catch(error => {
  log('ERROR', 'Error fatal', { error: error.message, stack: error.stack });
  process.exit(1);
});
