/**
 * PERF-SNAP-012: Tests para calculateContributions con snapshot latestAssetPerformance
 *
 * @see docs/stories/PERF-SNAP-012.story.md
 */

// ============================================================================
// Mocks
// ============================================================================

const mockCollectionGet = jest.fn();
const mockCollectionWhere = jest.fn();
const mockCollectionOrderBy = jest.fn();
const mockCollectionLimit = jest.fn();
const mockDocGet = jest.fn();

mockCollectionLimit.mockReturnValue({ get: mockCollectionGet });
mockCollectionOrderBy.mockReturnValue({ get: mockCollectionGet, limit: mockCollectionLimit });
mockCollectionWhere.mockReturnValue({
  where: mockCollectionWhere,
  orderBy: mockCollectionOrderBy,
});

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: {
      serverTimestamp: jest.fn(() => 'mock-server-timestamp'),
    },
  }),
  initializeApp: jest.fn(),
}));

jest.mock('../../firebaseAdmin', () => {
  const firestoreMock = () => ({
    collection: jest.fn(() => ({
      where: mockCollectionWhere,
      orderBy: mockCollectionOrderBy,
    })),
    doc: jest.fn(() => ({
      get: mockDocGet,
    })),
  });
  firestoreMock.FieldValue = { serverTimestamp: jest.fn() };
  return {
    firestore: firestoreMock,
    initializeApp: jest.fn(),
  };
});

jest.mock('../../financeQuery', () => ({
  getQuotes: jest.fn().mockResolvedValue([]),
}));

// ============================================================================
// Helpers
// ============================================================================

function buildFirestoreDoc(id, data) {
  return {
    id,
    exists: true,
    data: () => data,
  };
}

function buildFirestoreSnapshot(docs) {
  return {
    empty: docs.length === 0,
    docs: docs.map(d => ({
      id: d.id,
      data: () => d.data(),
      ...d,
    })),
    size: docs.length,
  };
}

// ============================================================================
// Test Data
// ============================================================================

const SNAPSHOT_WITH_ASSETS = {
  // Los snapshots persisten puntos {d, v, c} (ver services/snapshotGenerator.js)
  timeline: [
    { d: '2026-04-01', v: 10000, c: 0 },
    { d: '2026-04-10', v: 11000, c: 1.2 },
    { d: '2026-04-11', v: 11500, c: 4.5 },
  ],
  latestAssetPerformance: {
    'AAPL_stock': {
      totalValue: 5000,
      totalInvestment: 4000,
      units: 20,
      unrealizedPnL: 1000,
      totalROI: 25.0,
      dailyChangePercentage: 1.5,
    },
    'MSFT_stock': {
      totalValue: 4500,
      totalInvestment: 3800,
      units: 10,
      unrealizedPnL: 700,
      totalROI: 18.42,
      dailyChangePercentage: 0.8,
    },
    'AMZN_stock': {
      totalValue: 2000,
      totalInvestment: 1700,
      units: 5,
      unrealizedPnL: 300,
      totalROI: 17.65,
      dailyChangePercentage: -0.3,
    },
  },
};

const START_DATA = {
  id: '2026-01-02',
  date: '2026-01-02',
  USD: {
    totalValue: 10000,
    totalInvestment: 9000,
    assetPerformance: {
      'AAPL_stock': { totalValue: 4000, totalInvestment: 4000, units: 20 },
      'MSFT_stock': { totalValue: 3500, totalInvestment: 3800, units: 10 },
      'AMZN_stock': { totalValue: 1500, totalInvestment: 1700, units: 5 },
    },
  },
};

// ============================================================================
// Tests
// ============================================================================

const { calculateContributions, mapSnapshotAssetPerformance } = require('../contributionCalculator');

describe('PERF-SNAP-012: calculateContributions with snapshot', () => {
  beforeEach(() => {
    jest.clearAllMocks();

    // Default: start data query returns START_DATA
    mockCollectionGet.mockResolvedValue(
      buildFirestoreSnapshot([buildFirestoreDoc('2026-01-02', START_DATA)])
    );

    // Active assets query (portfolioAccounts + assets)
    mockCollectionWhere.mockReturnValue({
      where: mockCollectionWhere,
      orderBy: mockCollectionOrderBy,
      get: jest.fn().mockResolvedValue(buildFirestoreSnapshot([])),
    });
  });

  describe('mapSnapshotAssetPerformance', () => {
    it('maps unrealizedPnL to unrealizedProfitAndLoss', () => {
      const input = {
        'AAPL_stock': { totalValue: 5000, totalInvestment: 4000, units: 20, unrealizedPnL: 1000 },
        'MSFT_stock': { totalValue: 4500, totalInvestment: 3800, units: 10, unrealizedPnL: 700 },
      };

      const result = mapSnapshotAssetPerformance(input);

      expect(result['AAPL_stock'].unrealizedProfitAndLoss).toBe(1000);
      expect(result['MSFT_stock'].unrealizedProfitAndLoss).toBe(700);
      expect(result['AAPL_stock'].totalValue).toBe(5000);
      expect(result['AAPL_stock'].units).toBe(20);
    });

    it('preserves unrealizedProfitAndLoss if already present', () => {
      const input = {
        'AAPL_stock': { totalValue: 5000, unrealizedProfitAndLoss: 999 },
      };

      const result = mapSnapshotAssetPerformance(input);
      expect(result['AAPL_stock'].unrealizedProfitAndLoss).toBe(999);
    });

    it('defaults to 0 if neither field exists', () => {
      const input = {
        'AAPL_stock': { totalValue: 5000 },
      };

      const result = mapSnapshotAssetPerformance(input);
      expect(result['AAPL_stock'].unrealizedProfitAndLoss).toBe(0);
    });
  });

  describe('overall sin dateRange + snapshot con latestAssetPerformance (AC1)', () => {
    it('uses snapshot data instead of calling getLatestPerformanceData', async () => {
      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], undefined,
        { snapshot: SNAPSHOT_WITH_ASSETS }
      );

      expect(result.attributions).toBeDefined();
      expect(result.totalPortfolioValue).toBe(11500); // timeline[last].v
      expect(result.latestDate).toBe('2026-04-11'); // timeline[last].d

      // getLatestPerformanceData should NOT have been called for latest data
      // (only findNearestPerformanceData for start data is expected)
      // Verify we got attributions for all 3 assets
      const tickers = result.attributions.map(a => a.ticker).sort();
      expect(tickers).toEqual(['AAPL', 'AMZN', 'MSFT']);
    });

    it('derives totalPortfolioValue from snapshot.timeline[last].v', async () => {
      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], undefined,
        { snapshot: SNAPSHOT_WITH_ASSETS }
      );

      expect(result.totalPortfolioValue).toBe(11500);
    });

    it('derives totalPortfolioInvestment from sum of assets', async () => {
      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], undefined,
        { snapshot: SNAPSHOT_WITH_ASSETS }
      );

      // 4000 + 3800 + 1700 = 9500
      expect(result.totalPortfolioInvestment).toBe(9500);
    });

    it('derives latestDate from snapshot.timeline[last].d', async () => {
      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], undefined,
        { snapshot: SNAPSHOT_WITH_ASSETS }
      );

      expect(result.latestDate).toBe('2026-04-11');
    });
  });

  describe('mapeo unrealizedPnL → unrealizedProfitAndLoss produce contribuciones correctas (AC2)', () => {
    it('correctly maps unrealizedPnL from snapshot for contribution calculation', async () => {
      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], undefined,
        { snapshot: SNAPSHOT_WITH_ASSETS }
      );

      expect(result.attributions.length).toBeGreaterThan(0);
      const aapl = result.attributions.find(a => a.ticker === 'AAPL');
      expect(aapl).toBeDefined();
      expect(aapl._source.unrealizedPnL).toBe(1000);
    });
  });

  describe('fallback sin snapshot (AC3)', () => {
    it('falls back to getLatestPerformanceData when snapshot has no latestAssetPerformance', async () => {
      const snapshotWithoutAssets = {
        timeline: [{ d: '2026-04-11', v: 11500, c: 0 }],
        returns: { ytdReturn: 15 },
      };

      const latestDoc = buildFirestoreDoc('2026-04-11', {
        date: '2026-04-11',
        USD: {
          totalValue: 11500,
          totalInvestment: 9500,
          assetPerformance: {
            'AAPL_stock': { totalValue: 5000, totalInvestment: 4000, units: 20, unrealizedProfitAndLoss: 1000 },
          },
        },
      });

      // getLatestPerformanceData (orderBy desc, limit 1) then findNearestPerformanceData for start
      mockCollectionGet
        .mockResolvedValueOnce(buildFirestoreSnapshot([latestDoc]))
        .mockResolvedValueOnce(buildFirestoreSnapshot([buildFirestoreDoc('2026-01-02', START_DATA)]));

      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], undefined,
        { snapshot: snapshotWithoutAssets }
      );

      expect(result.attributions).toBeDefined();
      expect(result.totalPortfolioValue).toBe(11500);
    });

    it('falls back to getLatestPerformanceData when no snapshot provided', async () => {
      const latestDoc = buildFirestoreDoc('2026-04-11', {
        date: '2026-04-11',
        USD: {
          totalValue: 11500,
          totalInvestment: 9500,
          assetPerformance: {
            'AAPL_stock': { totalValue: 5000, totalInvestment: 4000, units: 20, unrealizedProfitAndLoss: 1000 },
          },
        },
      });

      mockCollectionGet
        .mockResolvedValueOnce(buildFirestoreSnapshot([latestDoc]))
        .mockResolvedValueOnce(buildFirestoreSnapshot([buildFirestoreDoc('2026-01-02', START_DATA)]));

      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], undefined,
        {}
      );

      expect(result.attributions).toBeDefined();
    });

    it('falls back when latestAssetPerformance is empty object', async () => {
      const snapshotWithEmpty = {
        timeline: [{ d: '2026-04-11', v: 11500, c: 0 }],
        latestAssetPerformance: {},
      };

      const latestDoc = buildFirestoreDoc('2026-04-11', {
        date: '2026-04-11',
        USD: {
          totalValue: 11500,
          totalInvestment: 9500,
          assetPerformance: {
            'AAPL_stock': { totalValue: 5000, totalInvestment: 4000, units: 20, unrealizedProfitAndLoss: 1000 },
          },
        },
      });

      mockCollectionGet
        .mockResolvedValueOnce(buildFirestoreSnapshot([latestDoc]))
        .mockResolvedValueOnce(buildFirestoreSnapshot([buildFirestoreDoc('2026-01-02', START_DATA)]));

      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], undefined,
        { snapshot: snapshotWithEmpty }
      );

      expect(result.attributions).toBeDefined();
    });
  });

  describe('con dateRange → siempre legacy (ignora snapshot)', () => {
    it('ignores snapshot when dateRange is provided', async () => {
      const dateRange = {
        startDate: new Date('2026-03-01'),
        endDate: new Date('2026-03-31'),
      };

      const latestDoc = buildFirestoreDoc('2026-03-31', {
        date: '2026-03-31',
        USD: {
          totalValue: 10500,
          totalInvestment: 9500,
          assetPerformance: {
            'AAPL_stock': { totalValue: 5000, totalInvestment: 4000, units: 20, unrealizedProfitAndLoss: 1000 },
          },
        },
      });

      const startDoc = buildFirestoreDoc('2026-03-01', {
        date: '2026-03-01',
        USD: {
          totalValue: 10000,
          totalInvestment: 9500,
          assetPerformance: {
            'AAPL_stock': { totalValue: 4500, totalInvestment: 4000, units: 20 },
          },
        },
      });

      mockCollectionGet
        .mockResolvedValueOnce(buildFirestoreSnapshot([latestDoc]))
        .mockResolvedValueOnce(buildFirestoreSnapshot([startDoc]));

      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['overall'], dateRange,
        { snapshot: SNAPSHOT_WITH_ASSETS }
      );

      // Should use dateRange data, not snapshot
      expect(result.totalPortfolioValue).toBe(10500);
      expect(result.latestDate).toBe('2026-03-31');
    });
  });

  describe('multi-account → siempre legacy (ignora snapshot)', () => {
    it('ignores snapshot for multi-account queries', async () => {
      const accDoc = buildFirestoreDoc('2026-04-11', {
        date: '2026-04-11',
        USD: {
          totalValue: 6000,
          totalInvestment: 5000,
          assetPerformance: {
            'AAPL_stock': { totalValue: 6000, totalInvestment: 5000, units: 30, unrealizedProfitAndLoss: 1000 },
          },
        },
      });

      const accStartDoc = buildFirestoreDoc('2026-01-02', {
        date: '2026-01-02',
        USD: {
          totalValue: 5000,
          totalInvestment: 5000,
          assetPerformance: {
            'AAPL_stock': { totalValue: 5000, totalInvestment: 5000, units: 30 },
          },
        },
      });

      // Multi-account mock: every chained query returns docs
      const mockGet = jest.fn().mockResolvedValue(buildFirestoreSnapshot([accDoc]));
      mockCollectionLimit.mockReturnValue({ get: mockGet });
      mockCollectionOrderBy.mockReturnValue({ get: mockGet, limit: mockCollectionLimit });
      mockCollectionWhere.mockReturnValue({
        where: mockCollectionWhere,
        orderBy: mockCollectionOrderBy,
        get: mockGet,
      });
      mockCollectionGet.mockResolvedValue(buildFirestoreSnapshot([accDoc]));

      const result = await calculateContributions(
        'user123', 'YTD', 'USD', ['acc1', 'acc2'], undefined,
        { snapshot: SNAPSHOT_WITH_ASSETS }
      );

      // Multi-account path aggregates per-account — should NOT use snapshot.timeline values
      expect(result.totalPortfolioValue).not.toBe(11500);
    });
  });
});
