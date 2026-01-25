/**
 * Types and Constants for Transaction Import Analysis
 * 
 * @module transactions/types
 * @see docs/stories/89.story.md (IMPORT-001)
 */

// ============================================================================
// FIELD DEFINITIONS
// ============================================================================

/**
 * Transaction fields that can be mapped from file columns
 * @typedef {'ticker'|'type'|'amount'|'price'|'date'|'currency'|'commission'|'market'|'total'|'description'} TransactionField
 */

/**
 * All valid transaction fields
 * @type {TransactionField[]}
 */
const ALL_FIELDS = [
  'ticker',       // Asset symbol (AAPL, NVDA)
  'type',         // buy | sell
  'amount',       // Quantity/units
  'price',        // Unit price
  'date',         // Transaction date
  'currency',     // USD, EUR, COP
  'commission',   // Broker commission/fee
  'market',       // Exchange (NASDAQ, NYSE)
  'total',        // Total amount (to derive price if needed)
  'description',  // Optional notes
];

/**
 * Required fields for a valid transaction import
 * @type {TransactionField[]}
 */
const REQUIRED_FIELDS = [
  'ticker', 
  'type', 
  'amount', 
  'price', 
  'date',
];

/**
 * Optional fields with default values
 * @type {TransactionField[]}
 */
const OPTIONAL_FIELDS = [
  'currency',     // Default: USD
  'commission',   // Default: 0
  'market',       // Inferred from ticker
  'description',  // Default: null
];

// ============================================================================
// DETECTION METHODS
// ============================================================================

/**
 * Method used to detect column mapping
 * @typedef {'header'|'content'|'context'|'broker'|'manual'} DetectionMethod
 */

/**
 * Confidence scores by detection method
 * @type {Object.<DetectionMethod, number>}
 */
const DETECTION_CONFIDENCE = {
  header: 0.9,    // By column header name (high confidence)
  content: 0.7,   // By analyzing values (medium confidence)
  context: 0.5,   // By relation with other columns (low confidence)
  broker: 0.95,   // By known broker pattern (very high confidence)
  manual: 1.0,    // Assigned manually by user
};

// ============================================================================
// HEADER PATTERNS (AC-010 to AC-016)
// ============================================================================

/**
 * Regex patterns for detecting columns by header name
 * Each pattern has a field target and confidence boost
 */
const HEADER_PATTERNS = {
  ticker: {
    patterns: [
      /^(symbol|ticker|stock|asset|símbolo|simbolo|activo|instrumento|security)$/i,
      /\b(symbol|ticker|stock)\b/i,
    ],
    priority: 1,
  },
  type: {
    patterns: [
      /^(type|action|operation|side|tipo|operación|operacion|acción|accion)$/i,
      /\b(buy.?sell|action|type)\b/i,
    ],
    priority: 2,
  },
  amount: {
    patterns: [
      /^(quantity|qty|shares|units|amount|cantidad|unidades|acciones)$/i,
      /\b(quantity|shares|units)\b/i,
    ],
    priority: 3,
  },
  price: {
    patterns: [
      /^(price|cost|valor|precio|unit.?price|t\.?\s*price|trade.?price)$/i,
      /\b(price|cost|valor)\b/i,
    ],
    priority: 4,
  },
  date: {
    patterns: [
      /^(date|fecha|trade.?date|settlement|fecha.?operación|execution.?date)$/i,
      /\bdate\b/i,
    ],
    priority: 5,
  },
  currency: {
    patterns: [
      /^(currency|ccy|curr|moneda|divisa)$/i,
      /\bcurrency\b/i,
    ],
    priority: 6,
  },
  commission: {
    patterns: [
      /^(commission|fee|comm|comisión|comision|fees|comm.?fee)$/i,
      /\b(commission|fee)\b/i,
    ],
    priority: 7,
  },
  market: {
    patterns: [
      /^(market|exchange|mercado|bolsa)$/i,
    ],
    priority: 8,
  },
  total: {
    patterns: [
      /^(total|amount|net.?amount|proceeds|importe|monto)$/i,
    ],
    priority: 9,
  },
};

// ============================================================================
// CONTENT PATTERNS (AC-017 to AC-021)
// ============================================================================

/**
 * Regex patterns for detecting columns by content analysis
 */
const CONTENT_PATTERNS = {
  // Ticker: 1-5 uppercase letters, optionally followed by .XX
  ticker: /^[A-Z]{1,5}(\.[A-Z]{1,2})?$/,
  
  // Type: buy/sell variations
  type: {
    buy: /^(buy|b|compra|c|bot|bought|open|long)$/i,
    sell: /^(sell|s|venta|v|sld|sold|close|short)$/i,
  },
  
  // Number with decimals (for amount, price, commission)
  number: /^-?[\d,]+\.?\d*$/,
  
  // Date formats
  date: {
    iso: /^\d{4}-\d{2}-\d{2}$/,                           // YYYY-MM-DD
    usSlash: /^\d{1,2}\/\d{1,2}\/\d{4}$/,                 // MM/DD/YYYY or DD/MM/YYYY
    euSlash: /^\d{1,2}\/\d{1,2}\/\d{4}$/,                 // Same pattern, context determines
    usDash: /^\d{1,2}-\d{1,2}-\d{4}$/,                    // MM-DD-YYYY
    textMonth: /^[A-Z][a-z]{2}\s+\d{1,2},?\s+\d{4}$/,    // Jan 15, 2024
    dateTime: /^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}/,      // ISO with time
  },
  
  // Currency: 3-letter ISO code
  currency: /^[A-Z]{3}$/,
};

// ============================================================================
// KNOWN BROKERS (AC-005 to AC-008)
// ============================================================================

/**
 * Known broker identifiers
 * @type {string[]}
 */
const KNOWN_BROKERS = [
  'interactive_brokers',
  'td_ameritrade',
  'fidelity',
  'etoro',
  'charles_schwab',
  'robinhood',
];

// ============================================================================
// VALIDATION LIMITS (AC-003, AC-004, AC-022)
// ============================================================================

const LIMITS = {
  maxPayloadSize: 1024 * 1024,    // 1MB
  maxSampleRows: 100,             // Max rows to analyze
  maxTickerSample: 20,            // Max tickers to validate against API
  maxSampleValuesPerColumn: 5,    // Sample values to show in mapping
  maxBatchTransactions: 500,      // Max transactions per import batch
  maxFirestoreBatch: 500,         // Firestore batch limit
};

// ============================================================================
// IMPORT ERROR CODES (IMPORT-002)
// ============================================================================

/**
 * Error codes for import failures
 * @typedef {'INVALID_TICKER'|'ASSET_NOT_FOUND'|'INSUFFICIENT_UNITS'|'ENRICHMENT_FAILED'|'WRITE_FAILED'|'DUPLICATE_DETECTED'|'INVALID_DATA'} ImportErrorCode
 */

const IMPORT_ERROR_CODES = {
  INVALID_TICKER: 'INVALID_TICKER',
  ASSET_NOT_FOUND: 'ASSET_NOT_FOUND',
  INSUFFICIENT_UNITS: 'INSUFFICIENT_UNITS',
  ENRICHMENT_FAILED: 'ENRICHMENT_FAILED',
  WRITE_FAILED: 'WRITE_FAILED',
  DUPLICATE_DETECTED: 'DUPLICATE_DETECTED',
  INVALID_DATA: 'INVALID_DATA',
};

// ============================================================================
// ASSET TYPE MAPPING
// ============================================================================

/**
 * Maps quoteType from API to internal asset type
 * @type {Object.<string, string>}
 */
const QUOTE_TYPE_MAPPING = {
  EQUITY: 'stock',
  ETF: 'etf',
  MUTUALFUND: 'etf',
  CRYPTOCURRENCY: 'crypto',
  CURRENCY: 'crypto',
};

/**
 * Default currency by market/exchange
 * @type {Object.<string, string>}
 */
const MARKET_CURRENCY_MAP = {
  NMS: 'USD',         // NASDAQ
  NYQ: 'USD',         // NYSE
  NASDAQ: 'USD',
  NYSE: 'USD',
  NYSEARCA: 'USD',
  AMEX: 'USD',
  LSE: 'GBP',
  LON: 'GBP',
  FRA: 'EUR',
  PAR: 'EUR',
  TYO: 'JPY',
  HKG: 'HKD',
  BVC: 'COP',         // Colombia
};

/**
 * Transaction types
 * @type {Object.<string, string>}
 */
const TRANSACTION_TYPES = {
  BUY: 'buy',
  SELL: 'sell',
};

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Field definitions
  ALL_FIELDS,
  REQUIRED_FIELDS,
  OPTIONAL_FIELDS,
  
  // Detection
  DETECTION_CONFIDENCE,
  HEADER_PATTERNS,
  CONTENT_PATTERNS,
  
  // Brokers
  KNOWN_BROKERS,
  
  // Limits
  LIMITS,
  
  // Import-002: Error codes and mappings
  IMPORT_ERROR_CODES,
  QUOTE_TYPE_MAPPING,
  MARKET_CURRENCY_MAP,
  TRANSACTION_TYPES,
};
