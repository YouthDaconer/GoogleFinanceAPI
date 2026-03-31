/**
 * Tests for getExchangeRatesForDates, getRateWithFallback, chunkArray (SCALE-005)
 * 
 * @module __tests__/services/backfillCoreModule.exchangeRates.test
 * @see docs/stories/SCALE-005.story.md
 */

// Mock firebaseAdmin before importing
const mockGet = jest.fn();
const mockSet = jest.fn().mockResolvedValue();
const mockUpdate = jest.fn().mockResolvedValue();
const mockGetAll = jest.fn();
const mockDoc = jest.fn();
const mockCollection = jest.fn();
const mockWhere = jest.fn();

jest.mock('../firebaseAdmin', () => {
  mockWhere.mockReturnThis();

  mockDoc.mockReturnValue({
    get: mockGet,
    set: mockSet,
    update: mockUpdate,
    collection: mockCollection,
  });

  mockCollection.mockReturnValue({
    where: mockWhere,
    get: mockGet,
    doc: mockDoc,
  });

  const mockAdmin = {
    firestore: jest.fn(() => ({
      collection: mockCollection,
      doc: mockDoc,
      getAll: mockGetAll,
    })),
    __esModule: false,
  };

  // Provide admin.firestore.FieldValue for SCALE-005 code
  mockAdmin.firestore.FieldValue = {
    serverTimestamp: () => 'SERVER_TIMESTAMP',
    increment: (n) => `INCREMENT_${n}`,
  };

  return mockAdmin;
});

// Mock node-fetch
jest.mock('node-fetch', () => {
  const fn = jest.fn().mockResolvedValue({
    json: () => Promise.resolve({ chart: { result: null } }),
    ok: true,
  });
  fn.default = fn;
  return fn;
});

const {
  getExchangeRatesForDates,
  getRateWithFallback,
  chunkArray,
  getActiveCurrencies,
} = require('../backfillCoreModule');

// Helper to create Firestore query snapshot with forEach (matching real API)
function mockQuerySnapshot(docs) {
  return {
    forEach: (fn) => docs.forEach(fn),
    docs,
    empty: docs.length === 0,
    size: docs.length,
  };
}

// Helper to create individual doc in a query snapshot
function mockQueryDoc(id, data) {
  return {
    id,
    data: () => data,
    ref: { path: `currencies/${id}` },
  };
}

// Helper to create a mock Firestore document snapshot (for doc().get())
function mockSnapshot(id, data, exists = true) {
  return {
    id,
    exists,
    data: () => data,
    ref: { update: mockUpdate, id },
  };
}

describe('chunkArray', () => {
  test('splits array into chunks of given size', () => {
    expect(chunkArray([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
  });

  test('returns single chunk when array is smaller than size', () => {
    expect(chunkArray([1, 2], 5)).toEqual([[1, 2]]);
  });

  test('returns empty array for empty input', () => {
    expect(chunkArray([], 3)).toEqual([]);
  });

  test('handles chunk size of 1', () => {
    expect(chunkArray([1, 2, 3], 1)).toEqual([[1], [2], [3]]);
  });
});

describe('getExchangeRatesForDates', () => {

  let testTimeOffset = 0;

  beforeEach(() => {
    jest.clearAllMocks();
    mockCollection.mockReturnValue({
      where: mockWhere,
      get: mockGet,
      doc: mockDoc,
    });
    mockWhere.mockReturnThis();
    // Each test advances the clock by an additional 10min to ensure currency cache is expired
    testTimeOffset += 10 * 60 * 1000;
    jest.spyOn(Date, 'now').mockReturnValue(Date.now() + testTimeOffset);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('returns cached rates for full cache hits without calling Yahoo Finance', async () => {
    // Setup: getActiveCurrencies → forEach-based snapshot
    mockGet.mockResolvedValueOnce(
      mockQuerySnapshot([
        mockQueryDoc('USD', { code: 'USD', isActive: true }),
        mockQueryDoc('COP', { code: 'COP', isActive: true }),
        mockQueryDoc('EUR', { code: 'EUR', isActive: true }),
      ])
    );

    // Mock db.getAll returns full cache hits
    mockGetAll.mockResolvedValueOnce([
      mockSnapshot('2026-03-27', {
        date: '2026-03-27',
        rates: { USD: 1, COP: 4285, EUR: 0.917 },
        _meta: { source: 'eod-pipeline', version: 1 },
      }),
      mockSnapshot('2026-03-28', {
        date: '2026-03-28',
        rates: { USD: 1, COP: 4290, EUR: 0.918 },
        _meta: { source: 'eod-pipeline', version: 1 },
      }),
    ]);

    const result = await getExchangeRatesForDates(['2026-03-27', '2026-03-28']);

    expect(result['2026-03-27']).toEqual({ USD: 1, COP: 4285, EUR: 0.917 });
    expect(result['2026-03-28']).toEqual({ USD: 1, COP: 4290, EUR: 0.918 });

    // Yahoo Finance (node-fetch) should NOT have been called
    const nodeFetch = require('node-fetch');
    expect(nodeFetch).not.toHaveBeenCalled();
  });

  test('fetches from Yahoo Finance for cache misses and writes through', async () => {
    // getActiveCurrencies
    mockGet.mockResolvedValueOnce(
      mockQuerySnapshot([
        mockQueryDoc('USD', { code: 'USD', isActive: true }),
        mockQueryDoc('COP', { code: 'COP', isActive: true }),
      ])
    );

    // db.getAll: cache miss
    mockGetAll.mockResolvedValueOnce([
      mockSnapshot('2026-03-27', null, false),
    ]);

    // fetchHistoricalExchangeRate calls node-fetch for COP (USDCOP=X)
    const nodeFetch = require('node-fetch');
    nodeFetch.mockResolvedValueOnce({
      json: () => Promise.resolve({
        chart: {
          result: [{
            indicators: { quote: [{ close: [4285] }] },
          }],
        },
      }),
      ok: true,
    });

    // Mock for write-through: doc.get returns non-existing
    mockGet.mockResolvedValueOnce({ exists: false });

    const result = await getExchangeRatesForDates(['2026-03-27']);

    expect(result['2026-03-27']).toBeDefined();
    expect(result['2026-03-27'].USD).toBe(1);
    expect(result['2026-03-27'].COP).toBe(4285);

    // Should have called set for write-through
    expect(mockSet).toHaveBeenCalled();
  });

  test('patches document when cache hit is partial (missing currency)', async () => {
    // getActiveCurrencies: USD, COP, EUR
    mockGet.mockResolvedValueOnce(
      mockQuerySnapshot([
        mockQueryDoc('USD', { code: 'USD', isActive: true }),
        mockQueryDoc('COP', { code: 'COP', isActive: true }),
        mockQueryDoc('EUR', { code: 'EUR', isActive: true }),
      ])
    );

    // db.getAll: cache has COP but NOT EUR
    mockGetAll.mockResolvedValueOnce([
      mockSnapshot('2026-03-27', {
        date: '2026-03-27',
        rates: { USD: 1, COP: 4285 },
        _meta: { source: 'eod-pipeline', version: 1 },
      }),
    ]);

    // fetchHistoricalExchangeRate for EUR (EURUSD=X)
    const nodeFetch = require('node-fetch');
    nodeFetch.mockResolvedValueOnce({
      json: () => Promise.resolve({
        chart: {
          result: [{
            indicators: { quote: [{ close: [1.09] }] },
          }],
        },
      }),
      ok: true,
    });

    const result = await getExchangeRatesForDates(['2026-03-27']);

    expect(result['2026-03-27'].COP).toBe(4285);
    // EUR was fetched and inverted (EURUSD=1.09 → 1/1.09)
    expect(result['2026-03-27'].EUR).toBeCloseTo(0.9174, 3);

    // Should have patched the document
    expect(mockUpdate).toHaveBeenCalled();
    const patchArg = mockUpdate.mock.calls[0][0];
    expect(patchArg['rates.EUR']).toBeCloseTo(0.9174, 3);
  });

  test('returns correct structure matching original ratesByDate format', async () => {
    // getActiveCurrencies
    mockGet.mockResolvedValueOnce(
      mockQuerySnapshot([
        mockQueryDoc('USD', { code: 'USD', isActive: true }),
      ])
    );

    // All cache hits (only USD)
    mockGetAll.mockResolvedValueOnce([
      mockSnapshot('2026-03-27', {
        date: '2026-03-27',
        rates: { USD: 1 },
        _meta: { source: 'eod-pipeline', version: 1 },
      }),
    ]);

    const result = await getExchangeRatesForDates(['2026-03-27']);

    // Structure: { "YYYY-MM-DD": { USD: 1, ... } }
    expect(result).toHaveProperty('2026-03-27');
    expect(typeof result['2026-03-27']).toBe('object');
    expect(result['2026-03-27'].USD).toBe(1);
  });
});

describe('getRateWithFallback', () => {

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('USD always returns 1', async () => {
    const result = await getRateWithFallback('2026-03-27', 'USD');
    expect(result).toBe(1);
    expect(mockDoc).not.toHaveBeenCalled();
  });

  test('returns exact date rate from cache', async () => {
    mockGet.mockResolvedValueOnce({
      exists: true,
      data: () => ({ rates: { COP: 4285 } }),
    });

    const result = await getRateWithFallback('2026-03-27', 'COP');
    expect(result).toBe(4285);
  });

  test('falls back to previous day when exact date missing', async () => {
    // Exact date: no rate for COP
    mockGet.mockResolvedValueOnce({
      exists: true,
      data: () => ({ rates: { EUR: 0.917 } }),
    });

    // Day -1: has COP
    mockGet.mockResolvedValueOnce({
      exists: true,
      data: () => ({ rates: { COP: 4280 } }),
    });

    const result = await getRateWithFallback('2026-03-27', 'COP');
    expect(result).toBe(4280);
  });

  test('falls back to Firestore currencies collection when proximity fails', async () => {
    // Exact date: miss
    mockGet.mockResolvedValueOnce({ exists: false });
    // Days -1 to -5: all miss
    for (let i = 0; i < 5; i++) {
      mockGet.mockResolvedValueOnce({ exists: false });
    }
    // currencies/{code} fallback
    mockGet.mockResolvedValueOnce({
      exists: true,
      data: () => ({ exchangeRate: 4250 }),
    });

    const result = await getRateWithFallback('2026-03-27', 'COP');
    expect(result).toBe(4250);
  });

  test('returns null when no rate available anywhere', async () => {
    mockGet.mockResolvedValueOnce({ exists: false });
    for (let i = 0; i < 5; i++) {
      mockGet.mockResolvedValueOnce({ exists: false });
    }
    mockGet.mockResolvedValueOnce({ exists: false });

    const result = await getRateWithFallback('2026-03-27', 'COP');
    expect(result).toBeNull();
  });
});
