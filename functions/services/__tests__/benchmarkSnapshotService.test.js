/**
 * Tests para benchmarkSnapshotService
 *
 * @see docs/stories/VS-009.story.md
 */

// ============================================================================
// Mocks
// ============================================================================

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: {
      serverTimestamp: jest.fn(() => 'mock-server-timestamp'),
    },
  }),
  initializeApp: jest.fn(),
}));

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: jest.fn(),
  FieldValue: {
    serverTimestamp: () => 'SERVER_TIMESTAMP',
    increment: (n) => `INCREMENT_${n}`,
  },
}));

jest.mock('../marketDataHelper', () => ({
  getPricesFromApi: jest.fn(),
}));

jest.mock('../../utils/portfolioCalculations', () => ({
  convertCurrency: jest.fn((amount, from, to, currencies) => {
    if (to === 'COP') return amount * 4200;
    return amount;
  }),
}));

const { DateTime } = require('luxon');
const { getPricesFromApi } = require('../marketDataHelper');
const { convertCurrency } = require('../../utils/portfolioCalculations');

// ============================================================================
// Helpers
// ============================================================================

function createMockDb(existingDocs = {}) {
  const writtenDocs = {};
  const batchOps = [];

  const batch = {
    set: jest.fn((ref, data) => {
      batchOps.push({ ref, data });
      writtenDocs[ref._docId] = data;
    }),
    commit: jest.fn().mockResolvedValue(undefined),
  };

  const db = {
    collection: jest.fn((collectionName) => ({
      doc: jest.fn((docId) => {
        const docRef = {
          _docId: docId,
          get: jest.fn().mockResolvedValue({
            exists: !!existingDocs[docId],
            data: () => existingDocs[docId] || null,
          }),
        };
        return docRef;
      }),
    })),
    batch: jest.fn(() => batch),
    _writtenDocs: writtenDocs,
    _batch: batch,
    _batchOps: batchOps,
  };

  return db;
}

function createMockLogger() {
  return {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    startOperation: jest.fn(() => ({
      success: jest.fn(),
      failure: jest.fn(),
    })),
  };
}

function createMockQuote(symbol, price, percentChange) {
  return {
    symbol,
    price,
    regularMarketPrice: price,
    name: symbol,
    percentChange,
    change: price * percentChange / 100,
    currency: 'USD',
    type: 'etf',
  };
}

const MOCK_CURRENCIES = [
  { code: 'USD', exchangeRate: 1, isActive: true },
  { code: 'COP', exchangeRate: 4200, isActive: true },
];

const MOCK_TRADING_DAY = DateTime.fromISO('2026-04-15', { zone: 'America/New_York' });

// ============================================================================
// Tests: computeReturnsFromTimeline (función pura)
// ============================================================================

describe('benchmarkSnapshotService', () => {
  let computeReturnsFromTimeline;
  let resolveBenchmarkQuotes;
  let updateBenchmarkSnapshots;
  let BENCHMARK_CONFIG;

  beforeEach(() => {
    jest.clearAllMocks();
    process.env.NODE_ENV = 'test';

    const service = require('../benchmarkSnapshotService');
    updateBenchmarkSnapshots = service.updateBenchmarkSnapshots;
    computeReturnsFromTimeline = service._testExports.computeReturnsFromTimeline;
    resolveBenchmarkQuotes = service._testExports.resolveBenchmarkQuotes;
    BENCHMARK_CONFIG = service._testExports.BENCHMARK_CONFIG;
  });

  describe('computeReturnsFromTimeline', () => {
    test('timeline vacío retorna returns en 0 con twoYear/fiveYear null', () => {
      const result = computeReturnsFromTimeline([]);

      expect(result.ytdReturn).toBe(0);
      expect(result.oneMonthReturn).toBe(0);
      expect(result.threeMonthReturn).toBe(0);
      expect(result.sixMonthReturn).toBe(0);
      expect(result.oneYearReturn).toBe(0);
      expect(result.twoYearReturn).toBeNull();
      expect(result.fiveYearReturn).toBeNull();
    });

    test('timeline null retorna returns en 0', () => {
      const result = computeReturnsFromTimeline(null);

      expect(result.ytdReturn).toBe(0);
      expect(result.twoYearReturn).toBeNull();
    });

    test('timeline con 30 puntos calcula oneMonthReturn', () => {
      const now = DateTime.now().setZone('America/New_York');
      const timeline = [];

      for (let i = 29; i >= 0; i--) {
        const date = now.minus({ days: i }).toISODate();
        timeline.push({ d: date, v: 500 + i, c: 0.5 });
      }

      const result = computeReturnsFromTimeline(timeline);

      expect(result.oneMonthReturn).not.toBe(0);
      expect(typeof result.oneMonthReturn).toBe('number');
    });

    test('timeline con 365 puntos calcula oneYearReturn y ytdReturn', () => {
      const now = DateTime.now().setZone('America/New_York');
      const timeline = [];

      for (let i = 364; i >= 0; i--) {
        const date = now.minus({ days: i }).toISODate();
        timeline.push({ d: date, v: 500, c: 0.1 });
      }

      const result = computeReturnsFromTimeline(timeline);

      expect(result.oneYearReturn).not.toBe(0);
      expect(result.ytdReturn).not.toBe(0);
      expect(typeof result.oneYearReturn).toBe('number');
    });

    test('timeline < 2 años calcula twoYearReturn parcial, fiveYearReturn parcial', () => {
      const now = DateTime.now().setZone('America/New_York');
      const timeline = [];

      for (let i = 200; i >= 0; i--) {
        const date = now.minus({ days: i }).toISODate();
        timeline.push({ d: date, v: 500, c: 0.05 });
      }

      const result = computeReturnsFromTimeline(timeline);

      // Con datos recientes, twoYear y fiveYear tienen found=true (datos están dentro del rango)
      expect(typeof result.twoYearReturn).toBe('number');
      expect(typeof result.fiveYearReturn).toBe('number');
      // Pero con timeline vacío, son null
      const emptyResult = computeReturnsFromTimeline([]);
      expect(emptyResult.twoYearReturn).toBeNull();
      expect(emptyResult.fiveYearReturn).toBeNull();
    });
  });

  // ============================================================================
  // Tests: updateBenchmarkSnapshots (integración con mock DB)
  // ============================================================================

  describe('updateBenchmarkSnapshots', () => {
    test('reutiliza quotes de pipelineQuotes sin llamar getPricesFromApi', async () => {
      const db = createMockDb();
      const logger = createMockLogger();
      const pipelineQuotes = [
        createMockQuote('SPY', 530.25, 0.85),
        createMockQuote('QQQ', 455.10, 1.12),
      ];

      await updateBenchmarkSnapshots(db, pipelineQuotes, MOCK_CURRENCIES, MOCK_TRADING_DAY, logger);

      expect(getPricesFromApi).not.toHaveBeenCalled();
    });

    test('hace fallback a getPricesFromApi cuando quotes no están en pipeline', async () => {
      const db = createMockDb();
      const logger = createMockLogger();

      getPricesFromApi.mockResolvedValue([
        createMockQuote('SPY', 530.25, 0.85),
        createMockQuote('QQQ', 455.10, 1.12),
      ]);

      await updateBenchmarkSnapshots(db, [], MOCK_CURRENCIES, MOCK_TRADING_DAY, logger);

      expect(getPricesFromApi).toHaveBeenCalledWith(['SPY', 'QQQ']);
    });

    test('append a timeline existente sin reescribir', async () => {
      const existingTimeline = [
        { d: '2026-04-14', v: 528.00, c: -0.3 },
      ];

      const db = createMockDb({
        'SPY_etf_USD': { timeline: existingTimeline, benchmarkId: 'SPY_etf' },
      });
      const logger = createMockLogger();
      const pipelineQuotes = [
        createMockQuote('SPY', 530.25, 0.85),
        createMockQuote('QQQ', 455.10, 1.12),
      ];

      await updateBenchmarkSnapshots(db, pipelineQuotes, MOCK_CURRENCIES, MOCK_TRADING_DAY, logger);

      const spyUsdWrite = db._writtenDocs['SPY_etf_USD'];
      expect(spyUsdWrite).toBeDefined();
      expect(spyUsdWrite.timeline).toHaveLength(2);
      expect(spyUsdWrite.timeline[0].d).toBe('2026-04-14');
      expect(spyUsdWrite.timeline[1].d).toBe('2026-04-15');
      expect(spyUsdWrite.timeline[1].v).toBe(530.25);
    });

    test('skip sin duplicar cuando mismo día ya está en timeline', async () => {
      const existingTimeline = [
        { d: '2026-04-15', v: 528.00, c: -0.3 },
      ];

      const db = createMockDb({
        'SPY_etf_USD': { timeline: existingTimeline },
        'SPY_etf_COP': { timeline: [{ d: '2026-04-15', v: 2217600, c: -0.3 }] },
        'QQQ_etf_USD': { timeline: [{ d: '2026-04-15', v: 450.00, c: 0.5 }] },
        'QQQ_etf_COP': { timeline: [{ d: '2026-04-15', v: 1890000, c: 0.5 }] },
      });
      const logger = createMockLogger();
      const pipelineQuotes = [
        createMockQuote('SPY', 530.25, 0.85),
        createMockQuote('QQQ', 455.10, 1.12),
      ];

      const result = await updateBenchmarkSnapshots(db, pipelineQuotes, MOCK_CURRENCIES, MOCK_TRADING_DAY, logger);

      expect(result.skipped).toBeGreaterThan(0);
    });

    test('genera documentos para múltiples monedas con conversión', async () => {
      const db = createMockDb();
      const logger = createMockLogger();
      const pipelineQuotes = [
        createMockQuote('SPY', 530.25, 0.85),
        createMockQuote('QQQ', 455.10, 1.12),
      ];

      await updateBenchmarkSnapshots(db, pipelineQuotes, MOCK_CURRENCIES, MOCK_TRADING_DAY, logger);

      expect(convertCurrency).toHaveBeenCalled();
      expect(db._writtenDocs['SPY_etf_COP']).toBeDefined();
      expect(db._writtenDocs['SPY_etf_COP'].currency).toBe('COP');
    });

    test('skip benchmark cuando quote no tiene precio', async () => {
      const db = createMockDb();
      const logger = createMockLogger();
      const pipelineQuotes = [
        createMockQuote('SPY', 0, 0),
        createMockQuote('QQQ', 455.10, 1.12),
      ];

      const result = await updateBenchmarkSnapshots(db, pipelineQuotes, MOCK_CURRENCIES, MOCK_TRADING_DAY, logger);

      expect(result.skipped).toBeGreaterThanOrEqual(1);
      expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('SPY'));
    });

    test('retorna métricas success/failed/skipped', async () => {
      const db = createMockDb();
      const logger = createMockLogger();
      const pipelineQuotes = [
        createMockQuote('SPY', 530.25, 0.85),
        createMockQuote('QQQ', 455.10, 1.12),
      ];

      const result = await updateBenchmarkSnapshots(db, pipelineQuotes, MOCK_CURRENCIES, MOCK_TRADING_DAY, logger);

      expect(typeof result.success).toBe('number');
      expect(typeof result.failed).toBe('number');
      expect(typeof result.skipped).toBe('number');
      expect(result.success).toBeGreaterThan(0);
    });

    test('schema del documento incluye campos requeridos', async () => {
      const db = createMockDb();
      const logger = createMockLogger();
      const pipelineQuotes = [
        createMockQuote('SPY', 530.25, 0.85),
        createMockQuote('QQQ', 455.10, 1.12),
      ];

      await updateBenchmarkSnapshots(db, pipelineQuotes, MOCK_CURRENCIES, MOCK_TRADING_DAY, logger);

      const doc = db._writtenDocs['SPY_etf_USD'];
      expect(doc.benchmarkId).toBe('SPY_etf');
      expect(doc.currency).toBe('USD');
      expect(doc.ticker).toBe('SPY');
      expect(doc.name).toBe('S&P 500 (SPY)');
      expect(doc.timeline).toBeInstanceOf(Array);
      expect(doc.returns).toBeDefined();
      expect(doc.returns.ytdReturn).toBeDefined();
      expect(doc.lastUpdated).toBeDefined();
      expect(doc.dataPoints).toBe(doc.timeline.length);
    });
  });
});
