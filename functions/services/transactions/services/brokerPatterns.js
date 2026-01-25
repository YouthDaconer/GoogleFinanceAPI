/**
 * Broker Pattern Detection and Mapping
 * 
 * Detects known broker formats by analyzing headers and filename,
 * then provides pre-defined column mappings for each broker.
 * 
 * Supported brokers (AC-005 to AC-008):
 * - Interactive Brokers (IBKR)
 * - TD Ameritrade
 * - Fidelity
 * - eToro
 * 
 * @module transactions/services/brokerPatterns
 * @see docs/stories/89.story.md (IMPORT-001)
 */

const { DETECTION_CONFIDENCE } = require('../types');

// ============================================================================
// BROKER DETECTION PATTERNS
// ============================================================================

/**
 * Header signatures for each broker
 * These are unique patterns that identify a specific broker's export format
 */
const BROKER_SIGNATURES = {
  interactive_brokers: {
    // IBKR Flex Query exports have these distinctive headers
    headers: [
      ['Symbol', 'Date/Time', 'Quantity', 'T. Price', 'Comm/Fee'],
      ['Symbol', 'TradeDate', 'Quantity', 'TradePrice', 'Commission'],
      ['Símbolo', 'Fecha/Hora', 'Cantidad', 'Precio T.', 'Comisión'],
    ],
    // Filename patterns
    filePatterns: [
      /ibkr/i,
      /interactive.?brokers/i,
      /flex.?query/i,
      /statement_/i,
    ],
    // Unique headers that only IBKR uses
    uniqueHeaders: ['T. Price', 'Comm/Fee', 'Realized P/L', 'MTM P/L'],
  },
  
  td_ameritrade: {
    headers: [
      ['Symbol', 'Trade Date', 'Quantity', 'Price', 'Commission'],
      ['SYMBOL', 'TRADE DATE', 'QTY', 'PRICE', 'COMMISSION'],
    ],
    filePatterns: [
      /td.?ameritrade/i,
      /tda/i,
      /schwab/i,  // After merger
    ],
    uniqueHeaders: ['REG FEE', 'SHORT-TERM RDM FEE'],
  },
  
  fidelity: {
    headers: [
      ['Symbol', 'Action', 'Quantity', 'Price', 'Settlement Date'],
      ['Symbol', 'Security Description', 'Action', 'Quantity', 'Price'],
    ],
    filePatterns: [
      /fidelity/i,
      /brokerage/i,
    ],
    uniqueHeaders: ['Security Description', 'Settlement Date', 'Account Name'],
  },
  
  etoro: {
    headers: [
      ['Position ID', 'Action', 'Amount', 'Units', 'Open Rate', 'Close Rate'],
      ['Asset', 'Action', 'Amount', 'Units', 'Rate'],
    ],
    filePatterns: [
      /etoro/i,
    ],
    uniqueHeaders: ['Position ID', 'Open Rate', 'Close Rate', 'Profit'],
  },
  
  charles_schwab: {
    headers: [
      ['Symbol', 'Action', 'Quantity', 'Price', 'Date'],
      ['Symbol', 'Description', 'Action', 'Qty', 'Price', 'Fees & Comm'],
    ],
    filePatterns: [
      /schwab/i,
    ],
    uniqueHeaders: ['Fees & Comm', 'Account Number'],
  },
  
  robinhood: {
    headers: [
      ['Instrument', 'Activity Date', 'Quantity', 'Average Price'],
    ],
    filePatterns: [
      /robinhood/i,
    ],
    uniqueHeaders: ['Instrument', 'Activity Date', 'Average Price'],
  },
};

// ============================================================================
// BROKER COLUMN MAPPINGS
// ============================================================================

/**
 * Pre-defined column mappings for each broker
 * Maps source column names to target transaction fields
 */
const BROKER_MAPPINGS = {
  interactive_brokers: {
    // Header name -> target field
    columnMappings: {
      // English
      'Symbol': 'ticker',
      'Date/Time': 'date',
      'TradeDate': 'date',
      'Quantity': 'amount',
      'T. Price': 'price',
      'TradePrice': 'price',
      'Comm/Fee': 'commission',
      'Commission': 'commission',
      'Currency': 'currency',
      'Exchange': 'market',
      // Spanish
      'Símbolo': 'ticker',
      'Fecha/Hora': 'date',
      'Cantidad': 'amount',
      'Precio T.': 'price',
      'Comisión': 'commission',
    },
    // How to derive type from IBKR data
    // IBKR uses positive/negative quantity: + = buy, - = sell
    typeDerivation: 'quantity_sign',
    // Default currency if not present
    defaultCurrency: 'USD',
    // Date format used
    dateFormat: 'YYYY-MM-DD, HH:mm:ss',
  },
  
  td_ameritrade: {
    columnMappings: {
      'Symbol': 'ticker',
      'SYMBOL': 'ticker',
      'Trade Date': 'date',
      'TRADE DATE': 'date',
      'Quantity': 'amount',
      'QTY': 'amount',
      'Price': 'price',
      'PRICE': 'price',
      'Commission': 'commission',
      'COMMISSION': 'commission',
      'Action': 'type',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Bought', 'BUY', 'BOUGHT'],
      sell: ['Sold', 'SELL', 'SOLD'],
    },
    defaultCurrency: 'USD',
    dateFormat: 'MM/DD/YYYY',
  },
  
  fidelity: {
    columnMappings: {
      'Symbol': 'ticker',
      'Action': 'type',
      'Quantity': 'amount',
      'Price': 'price',
      'Settlement Date': 'date',
      'Commission': 'commission',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['YOU BOUGHT', 'BOUGHT', 'BUY'],
      sell: ['YOU SOLD', 'SOLD', 'SELL'],
    },
    defaultCurrency: 'USD',
    dateFormat: 'MM/DD/YYYY',
  },
  
  etoro: {
    columnMappings: {
      'Asset': 'ticker',
      'Action': 'type',
      'Amount': 'total',    // eToro uses total amount
      'Units': 'amount',
      'Open Rate': 'price', // For open positions
      'Rate': 'price',
      'Open Date': 'date',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Buy', 'Open', 'Long'],
      sell: ['Sell', 'Close', 'Short'],
    },
    defaultCurrency: 'USD',
    dateFormat: 'DD/MM/YYYY HH:mm:ss',
  },
  
  charles_schwab: {
    columnMappings: {
      'Symbol': 'ticker',
      'Action': 'type',
      'Quantity': 'amount',
      'Qty': 'amount',
      'Price': 'price',
      'Date': 'date',
      'Fees & Comm': 'commission',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Buy', 'Bought'],
      sell: ['Sell', 'Sold'],
    },
    defaultCurrency: 'USD',
    dateFormat: 'MM/DD/YYYY',
  },
  
  robinhood: {
    columnMappings: {
      'Instrument': 'ticker',
      'Activity Date': 'date',
      'Quantity': 'amount',
      'Average Price': 'price',
      'Trans Code': 'type',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Buy', 'BUY'],
      sell: ['Sell', 'SLL'],
    },
    defaultCurrency: 'USD',
    dateFormat: 'YYYY-MM-DD',
  },
};

// ============================================================================
// DETECTION FUNCTIONS
// ============================================================================

/**
 * Detects broker format from headers and filename
 * 
 * @param {string[]|null} headers - First row of the file (if hasHeader=true)
 * @param {string} fileName - Name of the uploaded file
 * @returns {string|null} Detected broker ID or null if generic
 * 
 * @example
 * detectBrokerFormat(['Symbol', 'T. Price', 'Comm/Fee'], 'ibkr_trades.xlsx')
 * // Returns: 'interactive_brokers'
 */
function detectBrokerFormat(headers, fileName) {
  // If no headers, try filename only
  if (!headers || headers.length === 0) {
    return detectByFilename(fileName);
  }
  
  // Normalize headers for comparison
  const normalizedHeaders = headers.map(h => 
    String(h || '').trim()
  );
  
  // 1. Check for unique headers (most reliable)
  for (const [brokerId, signature] of Object.entries(BROKER_SIGNATURES)) {
    if (signature.uniqueHeaders) {
      const hasUniqueHeader = signature.uniqueHeaders.some(unique =>
        normalizedHeaders.includes(unique)
      );
      if (hasUniqueHeader) {
        console.log(`[brokerPatterns] Detected ${brokerId} by unique header`);
        return brokerId;
      }
    }
  }
  
  // 2. Check for header pattern match
  for (const [brokerId, signature] of Object.entries(BROKER_SIGNATURES)) {
    for (const expectedHeaders of signature.headers) {
      const matchCount = expectedHeaders.filter(expected =>
        normalizedHeaders.some(h => 
          h.toLowerCase() === expected.toLowerCase()
        )
      ).length;
      
      // If we match 80% of expected headers, it's a match
      if (matchCount >= expectedHeaders.length * 0.8) {
        console.log(`[brokerPatterns] Detected ${brokerId} by header match (${matchCount}/${expectedHeaders.length})`);
        return brokerId;
      }
    }
  }
  
  // 3. Fall back to filename detection
  return detectByFilename(fileName);
}

/**
 * Detects broker by filename patterns
 * 
 * @param {string} fileName - Filename to analyze
 * @returns {string|null} Detected broker or null
 */
function detectByFilename(fileName) {
  if (!fileName) return null;
  
  const normalizedName = fileName.toLowerCase();
  
  for (const [brokerId, signature] of Object.entries(BROKER_SIGNATURES)) {
    if (signature.filePatterns) {
      for (const pattern of signature.filePatterns) {
        if (pattern.test(normalizedName)) {
          console.log(`[brokerPatterns] Detected ${brokerId} by filename pattern`);
          return brokerId;
        }
      }
    }
  }
  
  return null;
}

/**
 * Gets column mappings for a detected broker
 * 
 * @param {string} brokerId - Detected broker identifier
 * @param {string[][]} sampleData - Sample data from file
 * @param {boolean} hasHeader - Whether first row is header
 * @returns {import('../types').ColumnMapping[]} Array of column mappings
 */
function getBrokerMappings(brokerId, sampleData, hasHeader) {
  const brokerConfig = BROKER_MAPPINGS[brokerId];
  if (!brokerConfig) {
    console.warn(`[brokerPatterns] No mapping config for broker: ${brokerId}`);
    return [];
  }
  
  const headers = hasHeader ? sampleData[0] : null;
  if (!headers) {
    console.warn(`[brokerPatterns] No headers available for broker mapping`);
    return [];
  }
  
  const mappings = [];
  const dataRows = hasHeader ? sampleData.slice(1) : sampleData;
  
  // Map each recognized header to its field
  headers.forEach((header, columnIndex) => {
    const normalizedHeader = String(header || '').trim();
    const targetField = brokerConfig.columnMappings[normalizedHeader];
    
    if (targetField) {
      // Extract sample values
      const sampleValues = dataRows
        .slice(0, 5)
        .map(row => String(row[columnIndex] || ''))
        .filter(v => v.length > 0);
      
      mappings.push({
        sourceColumn: columnIndex,
        sourceHeader: normalizedHeader,
        targetField,
        confidence: DETECTION_CONFIDENCE.broker,
        detectionMethod: 'broker',
        sampleValues,
        transformation: getTransformation(targetField, brokerId),
      });
    }
  });
  
  // Handle type derivation for brokers that don't have explicit type column
  if (!mappings.find(m => m.targetField === 'type')) {
    const derivedType = deriveTypeMapping(brokerId, brokerConfig, headers, mappings);
    if (derivedType) {
      mappings.push(derivedType);
    }
  }
  
  return mappings;
}

/**
 * Gets transformation hint for a field based on broker
 * 
 * @param {string} targetField - Target transaction field
 * @param {string} brokerId - Broker identifier
 * @returns {string|undefined} Transformation hint
 */
function getTransformation(targetField, brokerId) {
  const brokerConfig = BROKER_MAPPINGS[brokerId];
  
  switch (targetField) {
    case 'ticker':
      return 'uppercase';
    case 'date':
      return `parseDate:${brokerConfig?.dateFormat || 'auto'}`;
    case 'amount':
      return 'parseNumber';
    case 'price':
      return 'parseNumber';
    case 'commission':
      return 'parseNumber:absolute';
    case 'type':
      return 'normalizeType';
    default:
      return undefined;
  }
}

/**
 * Derives type mapping for brokers that use quantity sign or other methods
 * 
 * @param {string} brokerId - Broker identifier
 * @param {Object} config - Broker configuration
 * @param {string[]} headers - Column headers
 * @param {Object[]} mappings - Existing mappings
 * @returns {Object|null} Type mapping or null
 */
function deriveTypeMapping(brokerId, config, headers, mappings) {
  if (config.typeDerivation === 'quantity_sign') {
    // Type is derived from quantity sign (IBKR style)
    const amountMapping = mappings.find(m => m.targetField === 'amount');
    if (amountMapping) {
      return {
        sourceColumn: amountMapping.sourceColumn,
        sourceHeader: amountMapping.sourceHeader,
        targetField: 'type',
        confidence: DETECTION_CONFIDENCE.broker * 0.9, // Slightly lower
        detectionMethod: 'broker',
        sampleValues: [],
        transformation: 'deriveFromQuantitySign',
        derivedFrom: 'amount',
      };
    }
  }
  
  return null;
}

/**
 * Gets broker display name
 * 
 * @param {string} brokerId - Broker identifier
 * @returns {string} Human-readable broker name
 */
function getBrokerDisplayName(brokerId) {
  const names = {
    interactive_brokers: 'Interactive Brokers',
    td_ameritrade: 'TD Ameritrade',
    fidelity: 'Fidelity',
    etoro: 'eToro',
    charles_schwab: 'Charles Schwab',
    robinhood: 'Robinhood',
  };
  
  return names[brokerId] || brokerId;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Detection
  detectBrokerFormat,
  detectByFilename,
  
  // Mappings
  getBrokerMappings,
  getBrokerDisplayName,
  
  // For testing
  BROKER_SIGNATURES,
  BROKER_MAPPINGS,
};
