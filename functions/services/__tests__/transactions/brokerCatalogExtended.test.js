/**
 * Tests for the extended broker catalog
 *
 * HU 1.5: reconocimiento automático de los brokers de Colombia, Europa y
 * criptomonedas, más las variantes ampliadas de IBKR y Charles Schwab.
 *
 * Vive en su propio archivo para no mezclarse con los tests del catálogo original.
 *
 * @see platform-docs/stories/1.5-catalogo-formatos-broker-ampliado/
 */

const {
  detectBrokerFormat,
  detectByFilename,
  getBrokerMappings,
  getBrokerDisplayName,
  getBrokerNumberFormat,
  BROKER_SIGNATURES,
  BROKER_MAPPINGS,
  BROKER_DISPLAY_NAMES,
} = require('../../transactions/services/brokerPatterns');

const { KNOWN_BROKERS, REQUIRED_FIELDS } = require('../../transactions/types');

// ============================================================================
// TEST DATA — cabeceras reales de cada broker del catálogo ampliado
// ============================================================================

const HEADERS = {
  // Colombia
  trii: ['Fecha', 'Tipo de operación', 'Símbolo', 'Cantidad', 'Precio', 'Comisión', 'Valor total operación'],
  tyba: ['Fecha', 'Fondo', 'Tipo', 'Unidades', 'Valor unidad', 'Comisión'],
  valores_bancolombia: ['Fecha', 'Especie', 'Operación', 'Cantidad', 'Precio', 'Comisión', 'Mercado'],

  // Europa
  degiro: ['Fecha', 'Hora', 'Producto', 'ISIN', 'Bolsa de', 'Centro de ejecución', 'Número', 'Precio', 'Divisa', 'Valor', 'Costes de transacción', 'Total', 'ID Orden'],
  degiro_en: ['Date', 'Time', 'Product', 'ISIN', 'Reference exchange', 'Venue', 'Quantity', 'Price', 'Order ID'],
  trade_republic: ['Date', 'Type', 'Value', 'Note', 'ISIN', 'Shares'],
  xtb: ['ID', 'Type', 'Time', 'Symbol', 'Comment', 'Open price', 'Volume', 'Amount'],
  renta4: ['Fecha', 'Valor', 'Operacion', 'Titulos', 'Precio', 'Efectivo', 'Comision', 'Divisa'],

  // Cripto
  binance: ['Date(UTC)', 'Pair', 'Side', 'Price', 'Executed', 'Amount', 'Fee'],
  coinbase: ['Timestamp', 'Transaction Type', 'Asset', 'Quantity Transacted', 'Spot Price Currency', 'Spot Price at Transaction', 'Subtotal', 'Total', 'Fees', 'Notes'],
  bitso: ['oid', 'side', 'major', 'minor', 'price', 'book', 'fees_amount', 'created_at'],
};

/** Los 10 brokers que incorpora RN-12 */
const NEW_BROKERS = [
  'trii', 'tyba', 'valores_bancolombia',
  'degiro', 'trade_republic', 'xtb', 'renta4',
  'binance', 'coinbase', 'bitso',
];

function sampleData(headers) {
  return [
    headers,
    headers.map((_, i) => `v${i}`),
    headers.map((_, i) => `w${i}`),
  ];
}

// ============================================================================
// Escenario 1 — reconocimiento en la primera carga
// ============================================================================

describe('Escenario 1: reconocimiento en la primera carga', () => {
  test.each([
    ['trii', HEADERS.trii],
    ['tyba', HEADERS.tyba],
    ['valores_bancolombia', HEADERS.valores_bancolombia],
    ['degiro', HEADERS.degiro],
    ['trade_republic', HEADERS.trade_republic],
    ['xtb', HEADERS.xtb],
    ['renta4', HEADERS.renta4],
    ['binance', HEADERS.binance],
    ['coinbase', HEADERS.coinbase],
    ['bitso', HEADERS.bitso],
  ])('detecta %s por sus cabeceras, sin ayuda del nombre de archivo', (brokerId, headers) => {
    expect(detectBrokerFormat(headers, 'export.csv')).toBe(brokerId);
  });

  test.each([
    ['trii', 'trii_movimientos.csv'],
    ['tyba', 'tyba_export.xlsx'],
    ['valores_bancolombia', 'valores_bancolombia_2024.csv'],
    ['degiro', 'DEGIRO_Transactions.csv'],
    ['trade_republic', 'trade_republic_2024.csv'],
    ['xtb', 'xtb_history.csv'],
    ['renta4', 'renta4_movimientos.csv'],
    ['binance', 'binance_trades.csv'],
    ['coinbase', 'coinbase_transactions.csv'],
    ['bitso', 'bitso_trades.csv'],
  ])('detecta %s por el nombre del archivo', (brokerId, fileName) => {
    expect(detectByFilename(fileName)).toBe(brokerId);
  });

  test.each(NEW_BROKERS)('%s propone el mapeo de los campos requeridos', (brokerId) => {
    const headers = HEADERS[brokerId];
    const mappings = getBrokerMappings(brokerId, sampleData(headers), true);
    const mappedFields = new Set(mappings.map(m => m.targetField));

    // El campo `type` puede derivarse del signo de la cantidad, así que no se exige
    expect(mappedFields.has('ticker')).toBe(true);
    expect(mappedFields.has('date')).toBe(true);
    expect(mappedFields.has('amount')).toBe(true);

    // El precio unitario puede venir como tal o haber que derivarlo del importe
    // total: el export de Trade Republic no incluye precio por unidad, solo el
    // valor de la operación y el número de acciones.
    expect(mappedFields.has('price') || mappedFields.has('total')).toBe(true);
  });

  test('Trade Republic: su export no trae precio unitario, solo el importe total', () => {
    // Limitación real del formato, no del catálogo. Se declara `total` para que el
    // precio se pueda derivar; hoy el asistente pedirá al usuario mapear el precio
    // o dejará el campo como requerido faltante.
    const mappings = getBrokerMappings('trade_republic', sampleData(HEADERS.trade_republic), true);
    const mappedFields = new Set(mappings.map(m => m.targetField));

    expect(mappedFields.has('total')).toBe(true);
    expect(mappedFields.has('price')).toBe(false);
  });

  test.each(NEW_BROKERS)('%s propone el mapeo con la confianza de broker', (brokerId) => {
    const mappings = getBrokerMappings(brokerId, sampleData(HEADERS[brokerId]), true);

    expect(mappings.length).toBeGreaterThan(0);
    mappings.forEach((m) => {
      expect(m.detectionMethod).toBe('broker');
      expect(m.confidence).toBeGreaterThanOrEqual(0.8);
    });
  });
});

// ============================================================================
// Escenario 2 — variantes del mismo broker
// ============================================================================

describe('Escenario 2: variantes de formato e idioma del mismo broker', () => {
  test('DEGIRO se reconoce en español y en inglés', () => {
    expect(detectBrokerFormat(HEADERS.degiro, 'x.csv')).toBe('degiro');
    expect(detectBrokerFormat(HEADERS.degiro_en, 'x.csv')).toBe('degiro');
  });

  test('cada variante de DEGIRO resuelve a SUS propias columnas', () => {
    const es = getBrokerMappings('degiro', sampleData(HEADERS.degiro), true);
    const en = getBrokerMappings('degiro', sampleData(HEADERS.degiro_en), true);

    expect(es.find(m => m.targetField === 'ticker').sourceHeader).toBe('Producto');
    expect(en.find(m => m.targetField === 'ticker').sourceHeader).toBe('Product');
    expect(es.find(m => m.targetField === 'amount').sourceHeader).toBe('Número');
    expect(en.find(m => m.targetField === 'amount').sourceHeader).toBe('Quantity');
  });

  test('IBKR: Activity Statement se reconoce', () => {
    const headers = ['Symbol', 'Date/Time', 'Quantity', 'T. Price', 'C. Price', 'Proceeds', 'Comm/Fee'];

    expect(detectBrokerFormat(headers, 'x.csv')).toBe('interactive_brokers');
  });

  test('IBKR: Trade Confirmation Flex Query se reconoce', () => {
    const headers = ['Symbol', 'TradeDate', 'TradeTime', 'Quantity', 'Price', 'IBCommission'];

    expect(detectBrokerFormat(headers, 'x.csv')).toBe('interactive_brokers');
  });

  test.each([
    ['alemán', ['Symbol', 'Datum/Zeit', 'Menge', 'T. Kurs', 'Prov./Gebühr']],
    ['francés', ['Symbole', 'Date/Heure', 'Quantité', 'Prix T.', 'Comm/Frais']],
    ['portugués', ['Símbolo', 'Data/Hora', 'Quantidade', 'Preço T.', 'Comissão']],
  ])('IBKR en %s se reconoce y mapea sus columnas', (_lang, headers) => {
    expect(detectBrokerFormat(headers, 'x.csv')).toBe('interactive_brokers');

    const mappings = getBrokerMappings('interactive_brokers', sampleData(headers), true);
    const mappedFields = new Set(mappings.map(m => m.targetField));

    expect(mappedFields.has('ticker')).toBe(true);
    expect(mappedFields.has('date')).toBe(true);
    expect(mappedFields.has('amount')).toBe(true);
    expect(mappedFields.has('price')).toBe(true);
  });

  test('Charles Schwab: export de History se reconoce', () => {
    const headers = ['Date', 'Action', 'Symbol', 'Description', 'Quantity', 'Price', 'Fees & Comm', 'Amount'];

    expect(detectBrokerFormat(headers, 'x.csv')).toBe('charles_schwab');
  });

  test('Charles Schwab: export de Realized gain/loss se reconoce', () => {
    const headers = ['Symbol', 'Name', 'Closed Date', 'Opened Date', 'Quantity', 'Proceeds', 'Cost Basis'];

    expect(detectBrokerFormat(headers, 'x.csv')).toBe('charles_schwab');
  });
});

// ============================================================================
// Escenario 3 — RN-29: broker fuera del catálogo
// ============================================================================

describe('Escenario 3: broker fuera del catálogo (RN-29)', () => {
  test('un archivo genérico no se atribuye a ningún broker', () => {
    const generic = ['Ticker', 'Tipo', 'Unidades', 'Valor', 'Dia'];

    expect(detectBrokerFormat(generic, 'mi_archivo.csv')).toBeNull();
  });

  test('un broker desconocido con nombre propio tampoco se fuerza', () => {
    const headers = ['Instrumento', 'Movimiento', 'Nominales', 'Cotizacion', 'Dia'];

    expect(detectBrokerFormat(headers, 'broker_desconocido.csv')).toBeNull();
  });
});

// ============================================================================
// Escenario 6 — RN-28: falsos positivos
// ============================================================================

describe('Escenario 6: la detección no se dispara por cabeceras genéricas (RN-28)', () => {
  test.each([
    ['Fecha', ['Fecha', 'Concepto', 'Importe']],
    ['Cantidad', ['Producto', 'Cantidad', 'Total']],
    ['Precio', ['Item', 'Precio', 'Fecha']],
    ['Symbol', ['Symbol', 'Notes']],
    ['Tipo', ['Tipo', 'Detalle', 'Fecha']],
    ['Date', ['Date', 'Description', 'Amount']],
  ])('una cabecera genérica como "%s" no atribuye el archivo a un broker', (_label, headers) => {
    expect(detectBrokerFormat(headers, 'archivo.csv')).toBeNull();
  });

  test('ningún uniqueHeaders del catálogo contiene una cabecera genérica', () => {
    // Los uniqueHeaders se evalúan ANTES que los patrones y devuelven en el primer
    // acierto: una cabecera genérica aquí provocaría falsos positivos masivos.
    const tooGeneric = new Set([
      'Fecha', 'Date', 'Symbol', 'Ticker', 'Cantidad', 'Quantity',
      'Precio', 'Price', 'Tipo', 'Type', 'Total', 'Amount', 'Valor',
      'Comisión', 'Commission', 'Moneda', 'Currency',
    ]);

    for (const [brokerId, signature] of Object.entries(BROKER_SIGNATURES)) {
      for (const header of signature.uniqueHeaders || []) {
        expect(`${brokerId}:${header}`).toBe(tooGeneric.has(header) ? 'GENERICO' : `${brokerId}:${header}`);
      }
    }
  });

  test('los uniqueHeaders no se solapan entre brokers', () => {
    // Un mismo uniqueHeader en dos brokers haría la detección dependiente del
    // orden de declaración del objeto.
    const seen = new Map();

    for (const [brokerId, signature] of Object.entries(BROKER_SIGNATURES)) {
      for (const header of signature.uniqueHeaders || []) {
        const previous = seen.get(header);
        expect(previous ? `"${header}" duplicado: ${previous} y ${brokerId}` : null).toBeNull();
        seen.set(header, brokerId);
      }
    }
  });
});

// ============================================================================
// RN-12 / DoD — la ampliación es declarativa
// ============================================================================

describe('RN-12: la ampliación es declarativa', () => {
  test.each(NEW_BROKERS)('%s está registrado en KNOWN_BROKERS', (brokerId) => {
    expect(KNOWN_BROKERS).toContain(brokerId);
  });

  test.each(NEW_BROKERS)('%s tiene firma y mapeo declarados', (brokerId) => {
    expect(BROKER_SIGNATURES[brokerId]).toBeDefined();
    expect(BROKER_MAPPINGS[brokerId]).toBeDefined();
    expect(BROKER_MAPPINGS[brokerId].columnMappings).toBeDefined();
  });

  test.each(NEW_BROKERS)('%s tiene un nombre visible propio', (brokerId) => {
    const name = getBrokerDisplayName(brokerId);

    expect(name).toBeTruthy();
    expect(name).not.toBe(brokerId);
  });

  test('cada broker conocido tiene nombre visible', () => {
    KNOWN_BROKERS.forEach((brokerId) => {
      expect(BROKER_DISPLAY_NAMES[brokerId] || `SIN NOMBRE: ${brokerId}`).toBe(BROKER_DISPLAY_NAMES[brokerId]);
    });
  });

  test.each(NEW_BROKERS)('%s declara una estrategia de tipo soportada', (brokerId) => {
    // Si un broker necesitara una estrategia nueva, dejaría de ser declarativo
    expect(['quantity_sign', 'action_column'])
      .toContain(BROKER_MAPPINGS[brokerId].typeDerivation);
  });

  test.each(NEW_BROKERS)('%s declara moneda y formato de fecha por defecto', (brokerId) => {
    expect(BROKER_MAPPINGS[brokerId].defaultCurrency).toBeTruthy();
    expect(BROKER_MAPPINGS[brokerId].dateFormat).toBeTruthy();
  });
});

// ============================================================================
// Formato numérico declarado
// ============================================================================

describe('formato numérico declarado', () => {
  test.each(['trii', 'tyba', 'valores_bancolombia', 'degiro', 'trade_republic', 'xtb', 'renta4'])(
    '%s declara formato europeo (coma decimal)',
    (brokerId) => {
      expect(getBrokerNumberFormat(brokerId)).toBe('eu');
    }
  );

  test.each(['binance', 'coinbase', 'bitso', 'interactive_brokers', 'charles_schwab'])(
    '%s declara formato estadounidense (punto decimal)',
    (brokerId) => {
      expect(getBrokerNumberFormat(brokerId)).toBe('us');
    }
  );

  test('sin broker detectado se usa el formato actual, sin regresión', () => {
    expect(getBrokerNumberFormat(null)).toBe('us');
    expect(getBrokerNumberFormat(undefined)).toBe('us');
  });

  test('un broker sin numberFormat declarado usa us por defecto', () => {
    // Los brokers del catálogo original no lo declaraban: su comportamiento no cambia
    expect(getBrokerNumberFormat('fidelity')).toBe('us');
    expect(getBrokerNumberFormat('robinhood')).toBe('us');
  });
});

// ============================================================================
// RN-12 — alcance: LatAm fuera del catálogo
// ============================================================================

describe('RN-12: alcance del catálogo', () => {
  test('no se incorporan brokers del resto de LatAm', () => {
    const outOfScope = ['xp_investimentos', 'clear', 'rico', 'gbm', 'balanz', 'buenbit'];

    outOfScope.forEach((brokerId) => {
      expect(KNOWN_BROKERS).not.toContain(brokerId);
    });
  });
});

// ============================================================================
// RN-27 — reconocer el formato no implica confiar en el símbolo
// ============================================================================

describe('RN-27: el reconocimiento no asume que el símbolo es el ticker canónico', () => {
  test.each(NEW_BROKERS)('%s solo mapea la columna del símbolo, no lo transforma', (brokerId) => {
    const mappings = getBrokerMappings(brokerId, sampleData(HEADERS[brokerId]), true);
    const tickerMapping = mappings.find(m => m.targetField === 'ticker');

    // La única transformación admitida es normalizar la caja; nada de resolver alias
    expect(tickerMapping).toBeDefined();
    expect(tickerMapping.transformation).toBe('uppercase');
  });
});
