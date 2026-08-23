/**
 * Tests de precedencia y evidencia del catálogo global
 *
 * HU 1.6: RN-06 (la memoria propia gana), escenarios 2, 3 y 9, y RN-33 (la
 * corrección cuenta en ambos sentidos).
 *
 * @see platform-docs/stories/1.6-catalogo-global-equivalencias/
 */

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

const mockGetUserEquivalences = jest.fn();
const mockGetActiveEntries = jest.fn();

jest.mock('../../transactions/services/importMemoryRepository', () => {
  const actual = jest.requireActual('../../transactions/services/importMemoryRepository');

  return {
    buildEquivalenceKey: actual.buildEquivalenceKey,
    getUserEquivalences: (...args) => mockGetUserEquivalences(...args),
  };
});

jest.mock('../../transactions/services/globalEquivalenceRepository', () => ({
  getActiveEntries: (...args) => mockGetActiveEntries(...args),
}));

jest.mock('../../financeQuery', () => ({
  getMarketQuotes: jest.fn(),
  search: jest.fn(),
}));

// El repositorio real de memoria requiere firebaseAdmin a través de requireActual
jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: jest.fn(() => ({ doc: jest.fn(() => ({ collection: jest.fn(() => ({ doc: jest.fn() })) })) })),
    getAll: jest.fn(async () => []),
    batch: jest.fn(() => ({ set: jest.fn(), commit: jest.fn() })),
  })),
}));

const { resolveSymbols } = require('../../transactions/services/symbolEquivalenceResolver');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CTX = { userId: 'user-1', sourceFormatId: 'broker:degiro' };

/** Validador falso: declara qué tickers canónicos son válidos */
function fakeValidator(validSymbols) {
  return jest.fn(async (tickers) => {
    const result = {
      total: tickers.length,
      valid: 0,
      invalid: 0,
      unverified: 0,
      invalidTickers: [],
      unverifiedTickers: [],
      suggestions: {},
      details: {},
      validDetails: {},
    };

    for (const ticker of tickers) {
      if (validSymbols[ticker]) {
        result.valid++;
        result.details[ticker] = {
          originalTicker: ticker,
          isValid: true,
          normalizedTicker: ticker,
          currency: validSymbols[ticker].currency,
        };
        result.validDetails[ticker] = {
          symbol: ticker,
          currency: validSymbols[ticker].currency,
          quoteType: validSymbols[ticker].quoteType,
        };
      } else {
        result.invalid++;
        result.invalidTickers.push(ticker);
        result.details[ticker] = { originalTicker: ticker, isValid: false, error: 'Ticker not found' };
      }
    }

    return result;
  });
}

function userEquivalence(symbol, resolvedSymbol) {
  return {
    [symbol]: {
      sourceSymbol: symbol,
      resolvedSymbol,
      assetType: 'etf',
      currency: 'GBP',
      origin: 'user',
    },
  };
}

function globalEntry(key, symbol, resolvedSymbol) {
  return {
    [key]: {
      sourceFormatId: 'broker:degiro',
      sourceSymbol: symbol,
      resolvedSymbol,
      assetType: 'etf',
      currency: 'EUR',
      origin: 'global',
    },
  };
}

describe('precedencia del catálogo global (RN-06)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockGetUserEquivalences.mockResolvedValue({});
    mockGetActiveEntries.mockResolvedValue({});
  });

  // =========================================================================
  // Escenario 1 — beneficio del conocimiento acumulado
  // =========================================================================

  test('Escenario 1: un símbolo que el usuario nunca cargó llega resuelto del catálogo', async () => {
    mockGetActiveEntries.mockResolvedValue(
      globalEntry('broker:degiro::VWCE', 'VWCE', 'VWCE.DE')
    );

    const validate = fakeValidator({ 'VWCE.DE': { currency: 'EUR', quoteType: 'ETF' } });

    const { equivalences, tickerValidation } = await resolveSymbols({
      ...CTX, symbols: ['VWCE'], validate,
    });

    expect(equivalences.VWCE).toMatchObject({
      resolvedSymbol: 'VWCE.DE',
      origin: 'global',
    });
    expect(tickerValidation.invalidTickers).not.toContain('VWCE');
  });

  test('valida el ticker canónico del catálogo, no el símbolo del archivo', async () => {
    mockGetActiveEntries.mockResolvedValue(
      globalEntry('broker:degiro::VWCE', 'VWCE', 'VWCE.DE')
    );

    const validate = fakeValidator({ 'VWCE.DE': { currency: 'EUR', quoteType: 'ETF' } });

    await resolveSymbols({ ...CTX, symbols: ['VWCE'], validate });

    expect(validate).toHaveBeenCalledWith(['VWCE.DE']);
  });

  // =========================================================================
  // Escenario 2 — RN-06: la memoria propia gana
  // =========================================================================

  test('Escenario 2: la memoria propia gana sobre el catálogo global', async () => {
    mockGetUserEquivalences.mockResolvedValue(userEquivalence('VUAA', 'VUAA.L'));
    // El catálogo propondría otra cosa, pero no debe llegar a consultarse para VUAA
    mockGetActiveEntries.mockResolvedValue(
      globalEntry('broker:degiro::VUAA', 'VUAA', 'DISTINTO.L')
    );

    const validate = fakeValidator({ 'VUAA.L': { currency: 'GBP', quoteType: 'ETF' } });

    const { equivalences } = await resolveSymbols({ ...CTX, symbols: ['VUAA'], validate });

    expect(equivalences.VUAA.resolvedSymbol).toBe('VUAA.L');
    expect(equivalences.VUAA.origin).toBe('user');
  });

  test('el catálogo NO se consulta para símbolos con equivalencia propia', async () => {
    mockGetUserEquivalences.mockResolvedValue(userEquivalence('VUAA', 'VUAA.L'));

    const validate = fakeValidator({ 'VUAA.L': { currency: 'GBP', quoteType: 'ETF' } });

    await resolveSymbols({ ...CTX, symbols: ['VUAA'], validate });

    // Con un único símbolo, todo resuelto por memoria propia: no hay nada que pedir
    expect(mockGetActiveEntries).not.toHaveBeenCalled();
  });

  test('el catálogo se consulta SOLO para los símbolos sin memoria propia', async () => {
    mockGetUserEquivalences.mockResolvedValue(userEquivalence('VUAA', 'VUAA.L'));
    mockGetActiveEntries.mockResolvedValue({});

    const validate = fakeValidator({ 'VUAA.L': { currency: 'GBP', quoteType: 'ETF' } });

    await resolveSymbols({ ...CTX, symbols: ['VUAA', 'VWCE'], validate });

    expect(mockGetActiveEntries).toHaveBeenCalledWith(['broker:degiro::VWCE']);
  });

  // =========================================================================
  // Escenario 3 — la corrección del usuario prevalece
  // =========================================================================

  test('Escenario 3: tras corregir, el símbolo tiene memoria propia y el catálogo deja de consultarse', async () => {
    // Estado después de que el usuario corrigió: ya existe equivalencia propia
    mockGetUserEquivalences.mockResolvedValue(userEquivalence('VWCE', 'MI-ELECCION.DE'));

    const validate = fakeValidator({ 'MI-ELECCION.DE': { currency: 'EUR', quoteType: 'ETF' } });

    const { equivalences } = await resolveSymbols({ ...CTX, symbols: ['VWCE'], validate });

    expect(equivalences.VWCE.resolvedSymbol).toBe('MI-ELECCION.DE');
    expect(mockGetActiveEntries).not.toHaveBeenCalled();
  });

  // =========================================================================
  // Escenario 4 — por debajo del umbral no hay entrada activa
  // =========================================================================

  test('Escenario 4: sin entrada promovida el símbolo sigue el flujo normal', async () => {
    mockGetActiveEntries.mockResolvedValue({});

    const validate = fakeValidator({});

    const { equivalences, tickerValidation } = await resolveSymbols({
      ...CTX, symbols: ['DESCONOCIDO'], validate,
    });

    expect(equivalences).toEqual({});
    expect(tickerValidation.invalidTickers).toContain('DESCONOCIDO');
  });

  // =========================================================================
  // Escenario 6 — una entrada retirada deja de proponerse
  // =========================================================================

  test('Escenario 6: una entrada retirada no llega al resolver', async () => {
    // getActiveEntries filtra por status: una retirada simplemente no vuelve
    mockGetActiveEntries.mockResolvedValue({});

    const validate = fakeValidator({});

    const { equivalences } = await resolveSymbols({ ...CTX, symbols: ['RETIRADO'], validate });

    expect(equivalences).toEqual({});
  });

  // =========================================================================
  // Escenario 9 — catálogo vacío, sin regresión
  // =========================================================================

  test('Escenario 9: con catálogo vacío el comportamiento es el de la HU 1.2', async () => {
    mockGetActiveEntries.mockResolvedValue({});

    const validate = fakeValidator({ AAPL: { currency: 'USD', quoteType: 'EQUITY' } });

    const { equivalences, tickerValidation } = await resolveSymbols({
      ...CTX, symbols: ['AAPL'], validate,
    });

    expect(equivalences).toEqual({});
    expect(tickerValidation.valid).toBe(1);
    expect(validate).toHaveBeenCalledWith(['AAPL']);
  });

  test('Escenario 9: un catálogo inaccesible no degrada la resolución', async () => {
    mockGetActiveEntries.mockResolvedValue({});

    const validate = fakeValidator({ AAPL: { currency: 'USD', quoteType: 'EQUITY' } });

    const { tickerValidation } = await resolveSymbols({ ...CTX, symbols: ['AAPL'], validate });

    expect(tickerValidation.valid).toBe(1);
  });

  // =========================================================================
  // RN-17 aplicado a entradas globales
  // =========================================================================

  test('una entrada global hacia un activo inválido se degrada igual que una propia', async () => {
    mockGetActiveEntries.mockResolvedValue(
      globalEntry('broker:degiro::VIEJO', 'VIEJO', 'DELISTED.DE')
    );

    const validate = fakeValidator({});

    const { equivalences, tickerValidation } = await resolveSymbols({
      ...CTX, symbols: ['VIEJO'], validate,
    });

    expect(equivalences.VIEJO).toBeUndefined();
    expect(tickerValidation.invalidTickers).toContain('VIEJO');
    expect(tickerValidation.details.VIEJO.error).toContain('ya no está disponible');
  });

  // =========================================================================
  // Mezcla de orígenes
  // =========================================================================

  test('convive memoria propia con entradas globales en el mismo archivo', async () => {
    mockGetUserEquivalences.mockResolvedValue(userEquivalence('VUAA', 'VUAA.L'));
    mockGetActiveEntries.mockResolvedValue(
      globalEntry('broker:degiro::VWCE', 'VWCE', 'VWCE.DE')
    );

    const validate = fakeValidator({
      'VUAA.L': { currency: 'GBP', quoteType: 'ETF' },
      'VWCE.DE': { currency: 'EUR', quoteType: 'ETF' },
    });

    const { equivalences } = await resolveSymbols({
      ...CTX, symbols: ['VUAA', 'VWCE'], validate,
    });

    expect(equivalences.VUAA.origin).toBe('user');
    expect(equivalences.VWCE.origin).toBe('global');
  });
});

// ============================================================================
// RN-33 — la corrección cuenta en ambos sentidos
// ============================================================================

describe('RN-33: la corrección cuenta en ambos sentidos', () => {
  const mockRecordConfirmation = jest.fn();
  const mockRecordContradiction = jest.fn();
  const mockEvaluatePromotion = jest.fn();
  const mockEvaluateRetirement = jest.fn();

  let recordGlobalEvidence;

  beforeAll(() => {
    jest.resetModules();

    jest.doMock('firebase-functions/v2/https', () => ({
      onCall: jest.fn((config, handler) => ({ _handler: handler, _config: config })),
      HttpsError: class HttpsError extends Error {
        constructor(code, message) { super(message); this.code = code; }
      },
    }));

    jest.doMock('firebase-functions/params', () => ({
      defineSecret: jest.fn(() => ({ value: () => 'test-salt' })),
    }));

    jest.doMock('../../helpers/subscriptionValidator', () => ({
      validateFeatureAccess: jest.fn().mockResolvedValue(undefined),
      validateQuantityLimit: jest.fn().mockResolvedValue(undefined),
    }));

    jest.doMock('../../transactions/services/globalEquivalenceRepository', () => ({
      recordConfirmation: (...a) => mockRecordConfirmation(...a),
      recordContradiction: (...a) => mockRecordContradiction(...a),
      evaluatePromotion: (...a) => mockEvaluatePromotion(...a),
      evaluateRetirement: (...a) => mockEvaluateRetirement(...a),
    }));

    // eslint-disable-next-line global-require
    recordGlobalEvidence = require('../../transactions/saveImportMemory').recordGlobalEvidence;
  });

  beforeEach(() => {
    mockRecordConfirmation.mockClear();
    mockRecordContradiction.mockClear();
    mockEvaluatePromotion.mockClear();
    mockEvaluateRetirement.mockClear();
  });

  const BASE = {
    userId: 'user-1',
    sourceFormatId: 'broker:degiro',
    salt: 'test-salt',
  };

  test('una confirmación sin cambios solo cuenta como confirmación', async () => {
    await recordGlobalEvidence({
      ...BASE,
      equivalences: [{
        sourceSymbol: 'VWCE',
        resolvedSymbol: 'VWCE.DE',
        proposedFrom: { origin: 'global', resolvedSymbol: 'VWCE.DE' },
      }],
    });

    expect(mockRecordConfirmation).toHaveBeenCalledTimes(1);
    expect(mockRecordContradiction).not.toHaveBeenCalled();
  });

  test('corregir una propuesta global cuenta a favor del valor nuevo Y contra el anterior', async () => {
    await recordGlobalEvidence({
      ...BASE,
      equivalences: [{
        sourceSymbol: 'VWCE',
        resolvedSymbol: 'MI-ELECCION.DE',
        proposedFrom: { origin: 'global', resolvedSymbol: 'VWCE.DE' },
      }],
    });

    expect(mockRecordConfirmation).toHaveBeenCalledWith(expect.objectContaining({
      resolvedSymbol: 'MI-ELECCION.DE',
    }));
    expect(mockRecordContradiction).toHaveBeenCalledWith(expect.objectContaining({
      against: 'VWCE.DE',
    }));
  });

  test('la contradicción dispara la evaluación de retiro', async () => {
    await recordGlobalEvidence({
      ...BASE,
      equivalences: [{
        sourceSymbol: 'VWCE',
        resolvedSymbol: 'OTRO.DE',
        proposedFrom: { origin: 'global', resolvedSymbol: 'VWCE.DE' },
      }],
    });

    expect(mockEvaluateRetirement).toHaveBeenCalledWith('broker:degiro::VWCE');
  });

  test('corregir la propia memoria NO contradice el catálogo global', async () => {
    // Contradecir el catálogo por cambiar de opinión sobre tu propia elección
    // penalizaría una entrada que nunca se te propuso.
    await recordGlobalEvidence({
      ...BASE,
      equivalences: [{
        sourceSymbol: 'VUAA',
        resolvedSymbol: 'VUSA.L',
        proposedFrom: { origin: 'user', resolvedSymbol: 'VUAA.L' },
      }],
    });

    expect(mockRecordContradiction).not.toHaveBeenCalled();
    expect(mockEvaluateRetirement).not.toHaveBeenCalled();
  });

  test('sin propuesta previa solo se confirma', async () => {
    await recordGlobalEvidence({
      ...BASE,
      equivalences: [{ sourceSymbol: 'NUEVO', resolvedSymbol: 'NUEVO.L' }],
    });

    expect(mockRecordConfirmation).toHaveBeenCalledTimes(1);
    expect(mockRecordContradiction).not.toHaveBeenCalled();
  });

  test('toda confirmación evalúa la promoción', async () => {
    await recordGlobalEvidence({
      ...BASE,
      equivalences: [{ sourceSymbol: 'NUEVO', resolvedSymbol: 'NUEVO.L', assetType: 'etf', currency: 'EUR' }],
    });

    expect(mockEvaluatePromotion).toHaveBeenCalledWith(expect.objectContaining({
      key: 'broker:degiro::NUEVO',
      sourceSymbol: 'NUEVO',
      resolvedSymbol: 'NUEVO.L',
      assetType: 'etf',
      currency: 'EUR',
    }));
  });

  test('normaliza el símbolo de origen en la clave', async () => {
    await recordGlobalEvidence({
      ...BASE,
      equivalences: [{ sourceSymbol: ' vwce ', resolvedSymbol: 'VWCE.DE' }],
    });

    expect(mockEvaluatePromotion).toHaveBeenCalledWith(expect.objectContaining({
      key: 'broker:degiro::VWCE',
    }));
  });

  test('descarta equivalencias sin símbolo o sin ticker', async () => {
    const recorded = await recordGlobalEvidence({
      ...BASE,
      equivalences: [
        { sourceSymbol: '', resolvedSymbol: 'X.L' },
        { sourceSymbol: 'Y' },
        null,
        { sourceSymbol: 'OK', resolvedSymbol: 'OK.L' },
      ],
    });

    expect(recorded).toBe(1);
    expect(mockRecordConfirmation).toHaveBeenCalledTimes(1);
  });

  test('procesa varias equivalencias en la misma importación', async () => {
    const recorded = await recordGlobalEvidence({
      ...BASE,
      equivalences: [
        { sourceSymbol: 'A', resolvedSymbol: 'A.L' },
        { sourceSymbol: 'B', resolvedSymbol: 'B.DE' },
      ],
    });

    expect(recorded).toBe(2);
    expect(mockEvaluatePromotion).toHaveBeenCalledTimes(2);
  });
});
