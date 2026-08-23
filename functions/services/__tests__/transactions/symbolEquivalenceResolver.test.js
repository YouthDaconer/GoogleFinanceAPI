/**
 * Tests for symbolEquivalenceResolver
 *
 * HU 1.2: resolución de símbolos aplicando la memoria del usuario, validando el
 * TICKER CANÓNICO contra las fuentes de datos.
 *
 * @see platform-docs/stories/1.2-memoria-equivalencias-simbolo-usuario/
 */

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

const mockGetUserEquivalences = jest.fn();

jest.mock('../../transactions/services/importMemoryRepository', () => {
  // `buildEquivalenceKey` es una función pura: se usa la real para que la clave que
  // se consulta en el catálogo global sea la de verdad.
  const actual = jest.requireActual('../../transactions/services/importMemoryRepository');

  return {
    buildEquivalenceKey: actual.buildEquivalenceKey,
    getUserEquivalences: (...args) => mockGetUserEquivalences(...args),
  };
});

// HU 1.6: el catálogo global es una segunda pasada. En estos tests, que cubren la
// memoria propia del usuario (HU 1.2), se mantiene vacío.
const mockGetActiveEntries = jest.fn();

jest.mock('../../transactions/services/globalEquivalenceRepository', () => ({
  getActiveEntries: (...args) => mockGetActiveEntries(...args),
}));

// requireActual de importMemoryRepository arrastra firebaseAdmin
jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: jest.fn(() => ({ doc: jest.fn(() => ({ collection: jest.fn(() => ({ doc: jest.fn() })) })) })),
    getAll: jest.fn(async () => []),
    batch: jest.fn(() => ({ set: jest.fn(), commit: jest.fn() })),
  })),
}));

// tickerValidator solo aporta normalizeTicker; financeQuery no se toca porque la
// validación se inyecta.
jest.mock('../../financeQuery', () => ({
  getMarketQuotes: jest.fn(),
  search: jest.fn(),
}));

const {
  resolveSymbols,
  translateValidation,
  findDetail,
} = require('../../transactions/services/symbolEquivalenceResolver');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Construye un `validate` falso que declara qué tickers son válidos.
 *
 * @param {Object} spec - { valid: {ticker: {...}}, unverified: [], suggestions: {} }
 */
function fakeValidator(spec = {}) {
  const validMap = spec.valid || {};
  const unverified = new Set(spec.unverified || []);
  const suggestions = spec.suggestions || {};

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
      if (unverified.has(ticker)) {
        result.unverified++;
        result.unverifiedTickers.push(ticker);
        result.details[ticker] = {
          originalTicker: ticker,
          isValid: false,
          isUnverified: true,
          error: 'Timeout',
        };
        continue;
      }

      const meta = validMap[ticker];

      if (meta) {
        result.valid++;
        result.details[ticker] = {
          originalTicker: ticker,
          isValid: true,
          normalizedTicker: ticker,
          currency: meta.currency,
          assetType: meta.assetType,
          companyName: meta.name,
        };
        result.validDetails[ticker] = {
          symbol: ticker,
          name: meta.name,
          currency: meta.currency,
          quoteType: meta.quoteType,
        };
        continue;
      }

      result.invalid++;
      result.invalidTickers.push(ticker);
      result.details[ticker] = {
        originalTicker: ticker,
        isValid: false,
        error: 'Ticker not found',
      };

      if (suggestions[ticker]) {
        result.suggestions[ticker] = suggestions[ticker];
      }
    }

    return result;
  });
}

const CTX = { userId: 'user-1', sourceFormatId: 'broker:degiro' };

describe('symbolEquivalenceResolver', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockGetUserEquivalences.mockResolvedValue({});
    mockGetActiveEntries.mockResolvedValue({});
  });

  // =========================================================================
  // Escenario 1 — símbolo recordado llega resuelto
  // =========================================================================

  describe('Escenario 1: símbolo ya resuelto llega resuelto', () => {
    test('valida el ticker canónico, no el símbolo del archivo', async () => {
      mockGetUserEquivalences.mockResolvedValue({
        VUAA: {
          sourceSymbol: 'VUAA',
          resolvedSymbol: 'VUAA.L',
          assetType: 'etf',
          currency: 'GBP',
          origin: 'user',
        },
      });

      const validate = fakeValidator({
        valid: { 'VUAA.L': { currency: 'GBP', assetType: 'etf', name: 'Vanguard', quoteType: 'ETF' } },
      });

      await resolveSymbols({ ...CTX, symbols: ['VUAA'], validate });

      expect(validate).toHaveBeenCalledWith(['VUAA.L']);
    });

    test('el símbolo no aparece como inválido y expone su equivalencia', async () => {
      mockGetUserEquivalences.mockResolvedValue({
        VUAA: {
          sourceSymbol: 'VUAA',
          resolvedSymbol: 'VUAA.L',
          assetType: 'etf',
          currency: 'GBP',
          origin: 'user',
        },
      });

      const validate = fakeValidator({
        valid: { 'VUAA.L': { currency: 'GBP', assetType: 'etf', name: 'Vanguard', quoteType: 'ETF' } },
      });

      const { tickerValidation, equivalences } = await resolveSymbols({
        ...CTX, symbols: ['VUAA'], validate,
      });

      expect(tickerValidation.invalidTickers).not.toContain('VUAA');
      expect(tickerValidation.valid).toBe(1);
      expect(equivalences.VUAA).toMatchObject({
        resolvedSymbol: 'VUAA.L',
        origin: 'user',
      });
    });

    test('los conteos se expresan en símbolos del archivo, no en tickers canónicos', async () => {
      // Dos símbolos distintos del archivo apuntan al mismo ticker canónico
      mockGetUserEquivalences.mockResolvedValue({
        VUAA: { sourceSymbol: 'VUAA', resolvedSymbol: 'VUAA.L', assetType: 'etf', currency: 'GBP', origin: 'user' },
        VUAAL: { sourceSymbol: 'VUAAL', resolvedSymbol: 'VUAA.L', assetType: 'etf', currency: 'GBP', origin: 'user' },
      });

      const validate = fakeValidator({
        valid: { 'VUAA.L': { currency: 'GBP', assetType: 'etf', name: 'Vanguard', quoteType: 'ETF' } },
      });

      const { tickerValidation } = await resolveSymbols({
        ...CTX, symbols: ['VUAA', 'VUAAL'], validate,
      });

      // Se validó una sola vez, pero el usuario ve sus dos símbolos resueltos
      expect(validate).toHaveBeenCalledWith(['VUAA.L']);
      expect(tickerValidation.total).toBe(2);
      expect(tickerValidation.valid).toBe(2);
    });

    test('la moneda la manda la fuente de datos, no la memoria', async () => {
      mockGetUserEquivalences.mockResolvedValue({
        VUAA: { sourceSymbol: 'VUAA', resolvedSymbol: 'VUAA.L', assetType: 'etf', currency: 'USD', origin: 'user' },
      });

      const validate = fakeValidator({
        valid: { 'VUAA.L': { currency: 'GBP', assetType: 'etf', name: 'Vanguard', quoteType: 'ETF' } },
      });

      const { equivalences } = await resolveSymbols({ ...CTX, symbols: ['VUAA'], validate });

      expect(equivalences.VUAA.currency).toBe('GBP');
    });
  });

  // =========================================================================
  // Escenario 2 — equivalencia desconocida
  // =========================================================================

  describe('Escenario 2: equivalencia desconocida', () => {
    test('un símbolo que la fuente resuelve sigue el flujo normal', async () => {
      const validate = fakeValidator({
        valid: { AAPL: { currency: 'USD', assetType: 'stock', name: 'Apple', quoteType: 'EQUITY' } },
      });

      const { tickerValidation, equivalences } = await resolveSymbols({
        ...CTX, symbols: ['AAPL'], validate,
      });

      expect(tickerValidation.valid).toBe(1);
      expect(equivalences).toEqual({});
    });

    test('un símbolo que la fuente no resuelve queda como inválido', async () => {
      const validate = fakeValidator({ valid: {} });

      const { tickerValidation } = await resolveSymbols({
        ...CTX, symbols: ['XYZQ'], validate,
      });

      expect(tickerValidation.invalid).toBe(1);
      expect(tickerValidation.invalidTickers).toContain('XYZQ');
    });

    test('propaga la sugerencia de la fuente para símbolos sin equivalencia', async () => {
      const validate = fakeValidator({ valid: {}, suggestions: { APPL: 'AAPL' } });

      const { tickerValidation } = await resolveSymbols({
        ...CTX, symbols: ['APPL'], validate,
      });

      expect(tickerValidation.suggestions.APPL).toBe('AAPL');
    });
  });

  // =========================================================================
  // Escenario 3 — colisión entre formatos (RN-16)
  // =========================================================================

  describe('Escenario 3: mismo símbolo en dos brokers distintos', () => {
    test('consulta la memoria con el formato de origen del archivo cargado', async () => {
      const validate = fakeValidator({
        valid: { 'ETH-USD': { currency: 'USD', assetType: 'crypto', name: 'Ethereum', quoteType: 'CRYPTOCURRENCY' } },
      });

      mockGetUserEquivalences.mockResolvedValue({
        ETH: { sourceSymbol: 'ETH', resolvedSymbol: 'ETH-USD', assetType: 'crypto', currency: 'USD', origin: 'user' },
      });

      const { equivalences } = await resolveSymbols({
        userId: 'user-1',
        sourceFormatId: 'broker:binance',
        symbols: ['ETH'],
        validate,
      });

      expect(mockGetUserEquivalences).toHaveBeenCalledWith('user-1', 'broker:binance', ['ETH']);
      expect(equivalences.ETH.resolvedSymbol).toBe('ETH-USD');
    });

    test('sin equivalencia para ese formato, el símbolo se valida tal cual', async () => {
      // La memoria del OTRO broker no aplica: el repositorio no la devuelve
      mockGetUserEquivalences.mockResolvedValue({});

      const validate = fakeValidator({
        valid: { ETH: { currency: 'USD', assetType: 'stock', name: 'Ethan Allen', quoteType: 'EQUITY' } },
      });

      const { equivalences } = await resolveSymbols({
        userId: 'user-1',
        sourceFormatId: 'broker:interactive_brokers',
        symbols: ['ETH'],
        validate,
      });

      expect(validate).toHaveBeenCalledWith(['ETH']);
      expect(equivalences).toEqual({});
    });
  });

  // =========================================================================
  // Escenario 7 — RN-17: activo que dejó de existir
  // =========================================================================

  describe('Escenario 7: equivalencia hacia un activo inválido (RN-17)', () => {
    const staleMemory = {
      OLDTK: {
        sourceSymbol: 'OLDTK',
        resolvedSymbol: 'DELISTED.L',
        assetType: 'etf',
        currency: 'GBP',
        origin: 'user',
      },
    };

    test('el símbolo se degrada a no resuelto', async () => {
      mockGetUserEquivalences.mockResolvedValue(staleMemory);
      const validate = fakeValidator({ valid: {} });

      const { tickerValidation, equivalences } = await resolveSymbols({
        ...CTX, symbols: ['OLDTK'], validate,
      });

      expect(equivalences.OLDTK).toBeUndefined();
      expect(tickerValidation.invalidTickers).toContain('OLDTK');
      expect(tickerValidation.invalid).toBe(1);
    });

    test('el motivo explica que el activo vinculado ya no está disponible', async () => {
      mockGetUserEquivalences.mockResolvedValue(staleMemory);
      const validate = fakeValidator({ valid: {} });

      const { tickerValidation } = await resolveSymbols({
        ...CTX, symbols: ['OLDTK'], validate,
      });

      expect(tickerValidation.details.OLDTK.error).toContain('ya no está disponible');
    });

    test('no se propone una sugerencia alternativa: el usuario debe elegir', async () => {
      mockGetUserEquivalences.mockResolvedValue(staleMemory);
      const validate = fakeValidator({ valid: {}, suggestions: { 'DELISTED.L': 'OTRO.L' } });

      const { tickerValidation } = await resolveSymbols({
        ...CTX, symbols: ['OLDTK'], validate,
      });

      expect(tickerValidation.suggestions.OLDTK).toBeUndefined();
    });

    test('un fallo de verificación de la fuente no borra la equivalencia, la deja sin verificar', async () => {
      mockGetUserEquivalences.mockResolvedValue(staleMemory);
      const validate = fakeValidator({ unverified: ['DELISTED.L'] });

      const { tickerValidation, equivalences } = await resolveSymbols({
        ...CTX, symbols: ['OLDTK'], validate,
      });

      expect(equivalences.OLDTK).toBeUndefined();
      expect(tickerValidation.unverified).toBe(1);
      expect(tickerValidation.unverifiedTickers).toContain('OLDTK');
      expect(tickerValidation.invalidTickers).not.toContain('OLDTK');
    });
  });

  // =========================================================================
  // Escenario 8 — sin memoria, sin regresión
  // =========================================================================

  describe('Escenario 8: sin memoria previa, sin regresión', () => {
    test('valida exactamente los símbolos del archivo', async () => {
      const validate = fakeValidator({
        valid: {
          AAPL: { currency: 'USD', assetType: 'stock', name: 'Apple', quoteType: 'EQUITY' },
          MSFT: { currency: 'USD', assetType: 'stock', name: 'Microsoft', quoteType: 'EQUITY' },
        },
      });

      const { tickerValidation, equivalences } = await resolveSymbols({
        ...CTX, symbols: ['AAPL', 'MSFT'], validate,
      });

      expect(validate).toHaveBeenCalledWith(['AAPL', 'MSFT']);
      expect(tickerValidation.valid).toBe(2);
      expect(equivalences).toEqual({});
    });

    test('normaliza y deduplica los símbolos del archivo', async () => {
      const validate = fakeValidator({
        valid: { AAPL: { currency: 'USD', assetType: 'stock', name: 'Apple', quoteType: 'EQUITY' } },
      });

      const { tickerValidation } = await resolveSymbols({
        ...CTX, symbols: ['AAPL', 'aapl', ' $AAPL ', ''], validate,
      });

      expect(validate).toHaveBeenCalledWith(['AAPL']);
      expect(tickerValidation.total).toBe(1);
    });

    test('sin símbolos no consulta memoria', async () => {
      const validate = fakeValidator();

      await resolveSymbols({ ...CTX, symbols: [], validate });

      expect(mockGetUserEquivalences).not.toHaveBeenCalled();
    });
  });

  // =========================================================================
  // Helpers internos
  // =========================================================================

  describe('findDetail', () => {
    test('encuentra el detalle tolerando diferencias de caja', () => {
      const validation = { details: { 'VUAA.L': { isValid: true } } };

      expect(findDetail(validation, 'vuaa.l')).toEqual({ isValid: true });
    });

    test('devuelve null si no hay detalle', () => {
      expect(findDetail({ details: {} }, 'X')).toBeNull();
      expect(findDetail(null, 'X')).toBeNull();
      expect(findDetail({ details: {} }, null)).toBeNull();
    });
  });

  describe('translateValidation', () => {
    test('mezcla símbolos válidos, inválidos y no verificados en un solo resultado', () => {
      const { tickerValidation } = translateValidation({
        fileSymbols: ['OK', 'BAD', 'UNK'],
        targetBySymbol: { OK: 'OK', BAD: 'BAD', UNK: 'UNK' },
        remembered: {},
        rawValidation: {
          details: {
            OK: { isValid: true, normalizedTicker: 'OK' },
            BAD: { isValid: false, error: 'Ticker not found' },
            UNK: { isValid: false, isUnverified: true, error: 'Timeout' },
          },
          validDetails: { OK: { currency: 'USD' } },
          suggestions: {},
        },
      });

      expect(tickerValidation.total).toBe(3);
      expect(tickerValidation.valid).toBe(1);
      expect(tickerValidation.invalid).toBe(1);
      expect(tickerValidation.unverified).toBe(1);
    });
  });
});
