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
// BROKER DISPLAY NAMES
// ============================================================================

/**
 * Nombre visible de cada broker.
 *
 * Fuente única: el frontend lo consume vía `lib/transactionImport/brokerNames.ts`,
 * que se mantiene sincronizado con esta lista. Antes existían dos mapas duplicados
 * y ya habían divergido.
 */
const BROKER_DISPLAY_NAMES = {
  // Anglosajones (catálogo original)
  interactive_brokers: 'Interactive Brokers',
  td_ameritrade: 'TD Ameritrade',
  fidelity: 'Fidelity',
  etoro: 'eToro',
  charles_schwab: 'Charles Schwab',
  robinhood: 'Robinhood',

  // HU 1.5 — Colombia
  trii: 'Trii',
  tyba: 'Tyba',
  valores_bancolombia: 'Valores Bancolombia',

  // HU 1.5 — Europa
  degiro: 'DEGIRO',
  trade_republic: 'Trade Republic',
  xtb: 'XTB',
  renta4: 'Renta 4',

  // HU 1.5 — Criptomonedas
  binance: 'Binance',
  coinbase: 'Coinbase',
  bitso: 'Bitso',
};

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
    // HU 1.5: se amplían las variantes de formato e idioma reconocidas
    headers: [
      ['Symbol', 'Date/Time', 'Quantity', 'T. Price', 'Comm/Fee'],
      ['Symbol', 'TradeDate', 'Quantity', 'TradePrice', 'Commission'],
      ['Símbolo', 'Fecha/Hora', 'Cantidad', 'Precio T.', 'Comisión'],
      // Activity Statement (sección Trades)
      ['Symbol', 'Date/Time', 'Quantity', 'T. Price', 'C. Price', 'Proceeds', 'Comm/Fee'],
      // Trade Confirmation Flex Query
      ['Symbol', 'TradeDate', 'TradeTime', 'Quantity', 'Price', 'IBCommission'],
      // Alemán
      ['Symbol', 'Datum/Zeit', 'Menge', 'T. Kurs', 'Prov./Gebühr'],
      // Francés
      ['Symbole', 'Date/Heure', 'Quantité', 'Prix T.', 'Comm/Frais'],
      // Portugués
      ['Símbolo', 'Data/Hora', 'Quantidade', 'Preço T.', 'Comissão'],
    ],
    // Filename patterns
    filePatterns: [
      /ibkr/i,
      /interactive.?brokers/i,
      /flex.?query/i,
      /statement_/i,
      /activity.?statement/i,
      /trade.?confirmation/i,
    ],
    // Unique headers that only IBKR uses
    uniqueHeaders: [
      'T. Price', 'Comm/Fee', 'Realized P/L', 'MTM P/L',
      'IBCommission', 'C. Price',
      'Precio T.', 'T. Kurs', 'Prov./Gebühr', 'Prix T.', 'Comm/Frais', 'Preço T.',
    ],
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
    // HU 1.5: se amplían las variantes de formato reconocidas
    headers: [
      ['Symbol', 'Action', 'Quantity', 'Price', 'Date'],
      ['Symbol', 'Description', 'Action', 'Qty', 'Price', 'Fees & Comm'],
      // Transactions export (History)
      ['Date', 'Action', 'Symbol', 'Description', 'Quantity', 'Price', 'Fees & Comm', 'Amount'],
      // Realized gain/loss export
      ['Symbol', 'Name', 'Closed Date', 'Opened Date', 'Quantity', 'Proceeds', 'Cost Basis'],
      // Layout heredado de TD Ameritrade tras la fusión
      ['Symbol', 'Trade Date', 'Settlement Date', 'Action', 'Quantity', 'Price', 'Commission'],
    ],
    filePatterns: [
      /schwab/i,
      /charles.?schwab/i,
    ],
    uniqueHeaders: ['Fees & Comm', 'Account Number', 'Cost Basis', 'Closed Date'],
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

  // ==========================================================================
  // HU 1.5: CATÁLOGO AMPLIADO — COLOMBIA, EUROPA, CRIPTO
  //
  // Los `uniqueHeaders` se eligen para ser DISCRIMINANTES: se evalúan antes que
  // los patrones de cabecera y devuelven en el primer acierto, así que una cabecera
  // genérica ('Symbol', 'Fecha', 'Cantidad') aquí provocaría falsos positivos.
  // ==========================================================================

  // ── Colombia ─────────────────────────────────────────────────────────────
  trii: {
    headers: [
      ['Fecha', 'Tipo', 'Simbolo', 'Cantidad', 'Precio'],
      ['Fecha', 'Tipo de operación', 'Símbolo', 'Cantidad', 'Precio', 'Comisión'],
    ],
    filePatterns: [
      /trii/i,
    ],
    // 'Tipo de operación' + 'Símbolo' juntos son propios de Trii; ninguno de los
    // brokers anglosajones del catálogo usa esa combinación acentuada.
    uniqueHeaders: ['Tipo de operación', 'Valor total operación'],
  },

  tyba: {
    headers: [
      ['Fecha', 'Fondo', 'Tipo', 'Unidades', 'Valor unidad'],
      ['Fecha', 'Portafolio', 'Movimiento', 'Unidades', 'Valor de la unidad'],
    ],
    filePatterns: [
      /tyba/i,
    ],
    uniqueHeaders: ['Valor unidad', 'Valor de la unidad', 'Portafolio'],
  },

  valores_bancolombia: {
    headers: [
      ['Fecha', 'Especie', 'Operacion', 'Cantidad', 'Precio'],
      ['Fecha', 'Especie', 'Operación', 'Cantidad', 'Precio', 'Comisión'],
    ],
    filePatterns: [
      /bancolombia/i,
      /valores.?bancolombia/i,
    ],
    // 'Especie' es el término de la BVC para el instrumento: muy discriminante.
    uniqueHeaders: ['Especie'],
  },

  // ── Europa ───────────────────────────────────────────────────────────────
  degiro: {
    headers: [
      // Español
      ['Fecha', 'Hora', 'Producto', 'ISIN', 'Bolsa de', 'Número', 'Precio'],
      // Inglés
      ['Date', 'Time', 'Product', 'ISIN', 'Reference exchange', 'Quantity', 'Price'],
      // Neerlandés
      ['Datum', 'Tijd', 'Product', 'ISIN', 'Beurs', 'Aantal', 'Koers'],
    ],
    filePatterns: [
      /degiro/i,
      // NO se añaden /transactions/ ni /transacciones/: son nombres de archivo
      // genéricos que usan muchos brokers, y `detectByFilename` devuelve en el
      // primer acierto. Un "coinbase_transactions.csv" se atribuiría a DEGIRO.
    ],
    // 'ID Orden'/'Order ID' junto a 'Centro de ejecución'/'Venue' identifica
    // inequívocamente el export de transacciones de DEGIRO.
    uniqueHeaders: [
      'Centro de ejecución', 'ID Orden',
      'Reference exchange', 'Order ID',
      'Beurs', 'Uitvoeringsplaats',
    ],
  },

  trade_republic: {
    headers: [
      ['Date', 'Type', 'Value', 'Note', 'ISIN', 'Shares'],
      ['Fecha', 'Tipo', 'Valor', 'Nota', 'ISIN', 'Acciones'],
      ['Datum', 'Typ', 'Wert', 'Notiz', 'ISIN', 'Anteile'],
    ],
    filePatterns: [
      /trade.?republic/i,
      /traderepublic/i,
    ],
    uniqueHeaders: ['Notiz', 'Anteile'],
  },

  xtb: {
    headers: [
      ['ID', 'Type', 'Time', 'Symbol', 'Comment', 'Amount'],
      ['Position', 'Symbol', 'Type', 'Volume', 'Open time', 'Open price'],
      ['ID', 'Tipo', 'Hora', 'Símbolo', 'Comentario', 'Importe'],
    ],
    filePatterns: [
      /xtb/i,
      /xstation/i,
    ],
    uniqueHeaders: ['Open time', 'Close time', 'Gross P/L'],
  },

  renta4: {
    headers: [
      ['Fecha', 'Valor', 'Operacion', 'Titulos', 'Precio', 'Efectivo'],
      ['Fecha', 'Valor', 'Operación', 'Títulos', 'Precio', 'Efectivo', 'Comisión'],
    ],
    filePatterns: [
      /renta4/i,
      /renta.?4/i,
    ],
    // 'Títulos' + 'Efectivo' es la terminología de Renta4 para unidades e importe.
    uniqueHeaders: ['Titulos', 'Títulos', 'Efectivo'],
  },

  // ── Criptomonedas ────────────────────────────────────────────────────────
  binance: {
    headers: [
      ['Date(UTC)', 'Pair', 'Side', 'Price', 'Executed', 'Amount'],
      ['Date(UTC)', 'Market', 'Type', 'Price', 'Amount', 'Total'],
      ['UTC_Time', 'Account', 'Operation', 'Coin', 'Change'],
    ],
    filePatterns: [
      /binance/i,
    ],
    uniqueHeaders: ['Date(UTC)', 'Executed', 'UTC_Time'],
  },

  coinbase: {
    headers: [
      ['Timestamp', 'Transaction Type', 'Asset', 'Quantity Transacted', 'Spot Price at Transaction'],
      ['Timestamp', 'Transaction Type', 'Asset', 'Quantity Transacted', 'Subtotal', 'Total'],
    ],
    filePatterns: [
      /coinbase/i,
    ],
    uniqueHeaders: ['Quantity Transacted', 'Spot Price at Transaction', 'Spot Price Currency'],
  },

  bitso: {
    headers: [
      ['oid', 'side', 'major', 'minor', 'price', 'book'],
      ['Fecha', 'Tipo', 'Moneda', 'Cantidad', 'Precio', 'Comisión'],
    ],
    filePatterns: [
      /bitso/i,
    ],
    // 'book', 'major' y 'minor' son la nomenclatura de la API de Bitso.
    uniqueHeaders: ['book', 'major', 'minor', 'oid'],
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
      'IBCommission': 'commission',
      // Spanish
      'Símbolo': 'ticker',
      'Fecha/Hora': 'date',
      'Cantidad': 'amount',
      'Precio T.': 'price',
      'Comisión': 'commission',
      // German
      'Datum/Zeit': 'date',
      'Menge': 'amount',
      'T. Kurs': 'price',
      'Prov./Gebühr': 'commission',
      // French
      'Symbole': 'ticker',
      'Date/Heure': 'date',
      'Quantité': 'amount',
      'Prix T.': 'price',
      'Comm/Frais': 'commission',
      // Portuguese
      'Data/Hora': 'date',
      'Quantidade': 'amount',
      'Preço T.': 'price',
      'Comissão': 'commission',
    },
    // How to derive type from IBKR data
    // IBKR uses positive/negative quantity: + = buy, - = sell
    typeDerivation: 'quantity_sign',
    // Default currency if not present
    defaultCurrency: 'USD',
    // Date format used
    dateFormat: 'YYYY-MM-DD, HH:mm:ss',
    numberFormat: 'us',
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
      'Trade Date': 'date',
      'Fees & Comm': 'commission',
      'Commission': 'commission',
      'Description': 'description',
      'Amount': 'total',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Buy', 'Bought', 'Buy to Open', 'Reinvest Shares'],
      sell: ['Sell', 'Sold', 'Sell to Close'],
    },
    defaultCurrency: 'USD',
    dateFormat: 'MM/DD/YYYY',
    numberFormat: 'us',
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

  // ==========================================================================
  // HU 1.5: MAPEOS DEL CATÁLOGO AMPLIADO
  //
  // `numberFormat` es un dato DECLARADO, no lógica por broker: dice si el archivo
  // usa coma o punto como separador decimal. Sin él, un importe europeo como
  // "1.234,56" se interpretaría como 1.23456 sin lanzar ningún error.
  // ==========================================================================

  // ── Colombia ─────────────────────────────────────────────────────────────
  trii: {
    columnMappings: {
      'Fecha': 'date',
      'Tipo': 'type',
      'Tipo de operación': 'type',
      'Simbolo': 'ticker',
      'Símbolo': 'ticker',
      'Cantidad': 'amount',
      'Precio': 'price',
      'Comision': 'commission',
      'Comisión': 'commission',
      'Moneda': 'currency',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Compra', 'COMPRA', 'Buy'],
      sell: ['Venta', 'VENTA', 'Sell'],
    },
    defaultCurrency: 'COP',
    dateFormat: 'DD/MM/YYYY',
    numberFormat: 'eu',
  },

  tyba: {
    columnMappings: {
      'Fecha': 'date',
      'Fondo': 'ticker',
      'Portafolio': 'ticker',
      'Tipo': 'type',
      'Movimiento': 'type',
      'Unidades': 'amount',
      'Valor unidad': 'price',
      'Valor de la unidad': 'price',
      'Comisión': 'commission',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Aporte', 'APORTE', 'Compra'],
      sell: ['Retiro', 'RETIRO', 'Venta'],
    },
    defaultCurrency: 'COP',
    dateFormat: 'DD/MM/YYYY',
    numberFormat: 'eu',
  },

  valores_bancolombia: {
    columnMappings: {
      'Fecha': 'date',
      'Especie': 'ticker',
      'Operacion': 'type',
      'Operación': 'type',
      'Cantidad': 'amount',
      'Precio': 'price',
      'Comision': 'commission',
      'Comisión': 'commission',
      'Mercado': 'market',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Compra', 'COMPRA', 'C'],
      sell: ['Venta', 'VENTA', 'V'],
    },
    defaultCurrency: 'COP',
    dateFormat: 'DD/MM/YYYY',
    numberFormat: 'eu',
  },

  // ── Europa ───────────────────────────────────────────────────────────────
  degiro: {
    columnMappings: {
      // Español
      'Fecha': 'date',
      'Producto': 'ticker',
      'Número': 'amount',
      'Numero': 'amount',
      'Precio': 'price',
      'Bolsa de': 'market',
      'Costes de transacción': 'commission',
      // Inglés
      'Date': 'date',
      'Product': 'ticker',
      'Quantity': 'amount',
      'Price': 'price',
      'Reference exchange': 'market',
      'Transaction and/or third': 'commission',
      // Neerlandés
      'Datum': 'date',
      'Aantal': 'amount',
      'Koers': 'price',
      'Beurs': 'market',
      'Transactiekosten': 'commission',
    },
    // DEGIRO usa el signo de la cantidad: + = compra, - = venta
    typeDerivation: 'quantity_sign',
    defaultCurrency: 'EUR',
    dateFormat: 'DD-MM-YYYY',
    numberFormat: 'eu',
  },

  trade_republic: {
    columnMappings: {
      'Date': 'date',
      'Fecha': 'date',
      'Datum': 'date',
      'Type': 'type',
      'Tipo': 'type',
      'Typ': 'type',
      'ISIN': 'ticker',
      'Shares': 'amount',
      'Acciones': 'amount',
      'Anteile': 'amount',
      'Value': 'total',
      'Valor': 'total',
      'Wert': 'total',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Buy', 'Compra', 'Kauf', 'Kaufen'],
      sell: ['Sell', 'Venta', 'Verkauf', 'Verkaufen'],
    },
    defaultCurrency: 'EUR',
    dateFormat: 'DD.MM.YYYY',
    numberFormat: 'eu',
  },

  xtb: {
    columnMappings: {
      'Symbol': 'ticker',
      'Símbolo': 'ticker',
      'Type': 'type',
      'Tipo': 'type',
      'Time': 'date',
      'Hora': 'date',
      'Open time': 'date',
      'Volume': 'amount',
      'Open price': 'price',
      'Amount': 'total',
      'Importe': 'total',
      'Commission': 'commission',
      'Comisión': 'commission',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Buy', 'BUY', 'Compra', 'Stocks purchase'],
      sell: ['Sell', 'SELL', 'Venta', 'Stocks sale'],
    },
    defaultCurrency: 'EUR',
    dateFormat: 'DD.MM.YYYY',
    numberFormat: 'eu',
  },

  renta4: {
    columnMappings: {
      'Fecha': 'date',
      'Valor': 'ticker',
      'Operacion': 'type',
      'Operación': 'type',
      'Titulos': 'amount',
      'Títulos': 'amount',
      'Precio': 'price',
      'Comision': 'commission',
      'Comisión': 'commission',
      'Efectivo': 'total',
      'Divisa': 'currency',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Compra', 'COMPRA', 'Suscripción'],
      sell: ['Venta', 'VENTA', 'Reembolso'],
    },
    defaultCurrency: 'EUR',
    dateFormat: 'DD/MM/YYYY',
    numberFormat: 'eu',
  },

  // ── Criptomonedas ────────────────────────────────────────────────────────
  binance: {
    columnMappings: {
      'Date(UTC)': 'date',
      'UTC_Time': 'date',
      'Pair': 'ticker',
      'Market': 'ticker',
      'Coin': 'ticker',
      'Side': 'type',
      'Type': 'type',
      'Operation': 'type',
      'Price': 'price',
      'Executed': 'amount',
      'Change': 'amount',
      'Amount': 'total',
      'Total': 'total',
      'Fee': 'commission',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['BUY', 'Buy', 'Deposit'],
      sell: ['SELL', 'Sell', 'Withdraw'],
    },
    defaultCurrency: 'USD',
    dateFormat: 'YYYY-MM-DD',
    numberFormat: 'us',
  },

  coinbase: {
    columnMappings: {
      'Timestamp': 'date',
      'Transaction Type': 'type',
      'Asset': 'ticker',
      'Quantity Transacted': 'amount',
      'Spot Price at Transaction': 'price',
      'Spot Price Currency': 'currency',
      'Subtotal': 'total',
      'Fees': 'commission',
      'Notes': 'description',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['Buy', 'Receive', 'Advanced Trade Buy'],
      sell: ['Sell', 'Send', 'Advanced Trade Sell'],
    },
    defaultCurrency: 'USD',
    dateFormat: 'ISO',
    numberFormat: 'us',
  },

  bitso: {
    columnMappings: {
      // Nomenclatura de la API
      'created_at': 'date',
      'side': 'type',
      'book': 'ticker',
      'major': 'amount',
      'price': 'price',
      'fees_amount': 'commission',
      'minor': 'total',
      // Export en español
      'Fecha': 'date',
      'Tipo': 'type',
      'Moneda': 'ticker',
      'Cantidad': 'amount',
      'Precio': 'price',
      'Comisión': 'commission',
    },
    typeDerivation: 'action_column',
    typePatterns: {
      buy: ['buy', 'Buy', 'Compra'],
      sell: ['sell', 'Sell', 'Venta'],
    },
    defaultCurrency: 'MXN',
    dateFormat: 'ISO',
    numberFormat: 'us',
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
  return BROKER_DISPLAY_NAMES[brokerId] || brokerId;
}

/**
 * Formato numérico declarado de un broker (HU 1.5).
 *
 * Determina si el archivo usa coma o punto como separador decimal. Es un dato del
 * formato, no lógica: los brokers europeos y colombianos exportan "1.234,56" y sin
 * esta declaración ese importe se leería como 1.23456 sin lanzar ningún error.
 *
 * @param {string|null} brokerId
 * @returns {'us'|'eu'} Formato numérico; 'us' por defecto (comportamiento actual)
 */
function getBrokerNumberFormat(brokerId) {
  if (!brokerId) {
    return 'us';
  }

  return BROKER_MAPPINGS[brokerId]?.numberFormat || 'us';
}

/**
 * Formato numérico inferido del CONTENIDO de un conjunto de valores (IMPORT-004).
 *
 * `getBrokerNumberFormat` declara el formato por el broker detectado, pero la
 * detección de broker puede dar falsos positivos (un reporte genérico en
 * español que coincide con la firma de Trii). Esta función aplica la MISMA
 * heurística de evidencia que el frontend (`detectNumberFormat` de
 * numberParser.ts) para verificar la declaración contra los valores reales:
 *
 * - Dos separadores: el que aparece más a la derecha es el decimal (+2 evidencia)
 * - Un separador con 1-2 dígitos al final: candidato a decimal (+1 evidencia)
 *
 * @param {string[]} values - Valores crudos de las columnas numéricas
 * @returns {'us'|'eu'|null} El formato con más evidencia; null si no hay
 *   evidencia (o empate), en cuyo caso se debe respetar la declaración.
 */
function inferNumberFormatFromValues(values) {
  const NOISE = /[$€£¥₡₱₩¤\s '"]/g;
  const CURRENCY_CODE = /\b[A-Z]{3}\b/g;

  let euEvidence = 0;
  let usEvidence = 0;

  for (const raw of values) {
    if (!raw) continue;

    const cleaned = String(raw)
      .replace(CURRENCY_CODE, '')
      .replace(NOISE, '')
      .replace(/^\((.*)\)$/, '-$1')
      .trim();

    // Dos separadores distintos: el último es el decimal, sin ambigüedad
    if (cleaned.includes(',') && cleaned.includes('.')) {
      if (cleaned.lastIndexOf(',') > cleaned.lastIndexOf('.')) {
        euEvidence += 2;
      } else {
        usEvidence += 2;
      }
      continue;
    }

    // Un solo tipo de separador: solo cuenta si tiene forma decimal (1 o 2 dígitos)
    if (/,\d{1,2}$/.test(cleaned)) {
      euEvidence += 1;
    } else if (/\.\d{1,2}$/.test(cleaned)) {
      usEvidence += 1;
    }
  }

  if (euEvidence === usEvidence) {
    return null;
  }

  return euEvidence > usEvidence ? 'eu' : 'us';
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
  // HU 1.5: formato numérico declarado del broker
  getBrokerNumberFormat,
  // IMPORT-004: formato numérico inferido del contenido (verifica la declaración)
  inferNumberFormatFromValues,

  // For testing
  BROKER_SIGNATURES,
  BROKER_MAPPINGS,
  BROKER_DISPLAY_NAMES,
};
