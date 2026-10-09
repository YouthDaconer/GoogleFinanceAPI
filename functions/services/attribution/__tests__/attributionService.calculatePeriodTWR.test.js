/**
 * PERF-SNAP-010: Tests para calculatePeriodTWR y calculateMultiAccountTWR con snapshots
 *
 * @see docs/stories/PERF-SNAP-010.story.md
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

const mockCollectionGet = jest.fn();
const mockCollectionWhere = jest.fn();
const mockCollectionOrderBy = jest.fn();
const mockSnapshotDocGet = jest.fn();

// Chain: db.collection(path).where(...).where(...).orderBy(...).get()
mockCollectionOrderBy.mockReturnValue({ get: mockCollectionGet });
mockCollectionWhere.mockReturnValue({
  where: mockCollectionWhere,
  orderBy: mockCollectionOrderBy,
});

const mockDocRef = jest.fn(() => ({
  get: mockSnapshotDocGet,
}));

jest.mock('../../firebaseAdmin', () => {
  const firestoreMock = () => ({
    collection: jest.fn(() => ({
      where: mockCollectionWhere,
      orderBy: mockCollectionOrderBy,
    })),
    doc: mockDocRef,
  });
  firestoreMock.FieldValue = { serverTimestamp: jest.fn() };
  return {
    firestore: firestoreMock,
    initializeApp: jest.fn(),
  };
});

jest.mock('../contributionCalculator', () => ({
  calculateContributions: jest.fn(),
  enrichWithCurrentPrices: jest.fn(),
  findNearestPerformanceData: jest.fn(),
  getLatestPerformanceData: jest.fn(),
}));

jest.mock('../waterfallGenerator', () => ({
  generateWaterfallFromContributions: jest.fn(),
}));

jest.mock('../summaryGenerator', () => ({
  generateSummary: jest.fn(),
}));

jest.mock('../types', () => ({
  getPeriodLabel: jest.fn((p) => p),
  getPeriodStartDate: jest.fn(() => new Date('2026-01-01')),
}));

jest.mock('../intradayCalculator', () => ({
  calculateIntradayPerformance: jest.fn(),
  calculateIntradayContributions: jest.fn(),
  combineHistoricalWithIntraday: jest.fn(),
}));

jest.mock('../../../services/financeQuery', () => ({
  getQuotes: jest.fn(),
}));

const mockBuildSnapshotDocId = jest.fn((userId, accountId, currency) => {
  if (accountId === 'overall') return `${userId}_${currency}`;
  return `${userId}_${accountId}_${currency}`;
});
const mockGeneratePerformanceSnapshot = jest.fn().mockResolvedValue();

jest.mock('../../../services/snapshotGenerator', () => ({
  buildSnapshotDocId: (...args) => mockBuildSnapshotDocId(...args),
  generatePerformanceSnapshot: (...args) => mockGeneratePerformanceSnapshot(...args),
}));

// ============================================================================
// Import
// ============================================================================

const {
  calculatePeriodTWR,
  calculateMultiAccountTWR,
  PERIOD_TO_SNAPSHOT_FIELD,
} = require('../attributionService');

// ============================================================================
// Fixtures
// ============================================================================

const MOCK_SNAPSHOT = {
  returns: {
    ytdReturn: 12.5,
    oneMonthReturn: 2.3,
    threeMonthReturn: 5.6,
    sixMonthReturn: 8.1,
    oneYearReturn: 15.2,
    twoYearReturn: 28.4,
    fiveYearReturn: 45.7,
  },
  timeline: [
    ['2026-04-09', 50000, 0],
    ['2026-04-10', 50250, 0.5],
    ['2026-04-11', 50500, 0.497],
  ],
};

const buildLegacyDocs = (dailyChanges) => ({
  empty: dailyChanges.length === 0,
  size: dailyChanges.length,
  docs: dailyChanges.map((change, i) => ({
    data: () => ({
      date: `2026-01-${String(i + 1).padStart(2, '0')}`,
      USD: { adjustedDailyChangePercentage: change, totalValue: 50000 + i * 100 },
    }),
  })),
});

// ============================================================================
// Tests: calculatePeriodTWR
// ============================================================================

describe('calculatePeriodTWR', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockCollectionGet.mockResolvedValue(buildLegacyDocs([1.0, 0.5, -0.3]));
  });

  describe('snapshot path', () => {
    test('AC1: retorna TWR del snapshot para YTD sin leer daily docs', async () => {
      const result = await calculatePeriodTWR('user1', 'YTD', 'USD', 'overall', undefined, {
        snapshot: MOCK_SNAPSHOT,
      });

      expect(result).toEqual({ twr: 12.5, hasData: true, docsCount: 0 });
      expect(mockCollectionGet).not.toHaveBeenCalled();
    });

    test.each([
      ['YTD', 'ytdReturn', 12.5],
      ['1M', 'oneMonthReturn', 2.3],
      ['3M', 'threeMonthReturn', 5.6],
      ['6M', 'sixMonthReturn', 8.1],
      ['1Y', 'oneYearReturn', 15.2],
      ['2Y', 'twoYearReturn', 28.4],
      ['ALL', 'fiveYearReturn', 45.7],
      ['5Y', 'fiveYearReturn', 45.7],
    ])('AC2: período %s → campo %s = %s', async (period, field, expectedTWR) => {
      const result = await calculatePeriodTWR('user1', period, 'USD', 'overall', undefined, {
        snapshot: MOCK_SNAPSHOT,
      });

      expect(result.twr).toBe(expectedTWR);
      expect(result.hasData).toBe(true);
      expect(result.docsCount).toBe(0);
    });

    test('AC3: 0 reads de daily docs cuando snapshot disponible', async () => {
      await calculatePeriodTWR('user1', '3M', 'USD', 'overall', undefined, {
        snapshot: MOCK_SNAPSHOT,
      });

      expect(mockCollectionGet).not.toHaveBeenCalled();
    });

    test('AC5: snapshot pasado como options.snapshot, no leído internamente', async () => {
      await calculatePeriodTWR('user1', 'YTD', 'USD', 'overall', undefined, {
        snapshot: MOCK_SNAPSHOT,
      });

      expect(mockSnapshotDocGet).not.toHaveBeenCalled();
      expect(mockDocRef).not.toHaveBeenCalled();
    });
  });

  describe('fallback a legacy', () => {
    test('sin snapshot → full-scan legacy', async () => {
      const result = await calculatePeriodTWR('user1', 'YTD', 'USD', 'overall');

      expect(mockCollectionGet).toHaveBeenCalledTimes(1);
      expect(result.hasData).toBe(true);
      expect(result.docsCount).toBe(3);
    });

    test('con dateRange → siempre legacy (ignora snapshot)', async () => {
      const dateRange = {
        startDate: new Date('2026-02-01'),
        endDate: new Date('2026-02-28'),
      };

      const result = await calculatePeriodTWR('user1', 'YTD', 'USD', 'overall', dateRange, {
        snapshot: MOCK_SNAPSHOT,
      });

      expect(mockCollectionGet).toHaveBeenCalledTimes(1);
      expect(result.docsCount).toBe(3);
    });

    test('snapshot con campo undefined → fallback a legacy', async () => {
      const incompleteSnapshot = {
        returns: { ytdReturn: 12.5 },
      };

      const result = await calculatePeriodTWR('user1', '3M', 'USD', 'overall', undefined, {
        snapshot: incompleteSnapshot,
      });

      expect(mockCollectionGet).toHaveBeenCalledTimes(1);
      expect(result.docsCount).toBe(3);
    });

    test('snapshot con returns null → fallback a legacy', async () => {
      const result = await calculatePeriodTWR('user1', 'YTD', 'USD', 'overall', undefined, {
        snapshot: { returns: null },
      });

      expect(mockCollectionGet).toHaveBeenCalledTimes(1);
    });

    test('options vacío → legacy', async () => {
      const result = await calculatePeriodTWR('user1', 'YTD', 'USD', 'overall', undefined, {});

      expect(mockCollectionGet).toHaveBeenCalledTimes(1);
    });
  });
});

// ============================================================================
// Tests: calculateMultiAccountTWR
// ============================================================================

describe('calculateMultiAccountTWR', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockCollectionGet.mockResolvedValue(buildLegacyDocs([1.0, 0.5]));
  });

  describe('single account / overall con snapshot', () => {
    test('overall + snapshot → delega a calculatePeriodTWR con snapshot', async () => {
      const result = await calculateMultiAccountTWR(
        'user1', 'YTD', 'USD', ['overall'], undefined, { snapshot: MOCK_SNAPSHOT }
      );

      expect(result).toEqual({ twr: 12.5, hasData: true, docsCount: 0 });
      expect(mockCollectionGet).not.toHaveBeenCalled();
    });

    test('single account + snapshot → delega a calculatePeriodTWR con snapshot', async () => {
      const result = await calculateMultiAccountTWR(
        'user1', 'YTD', 'USD', ['acc1'], undefined, { snapshot: MOCK_SNAPSHOT }
      );

      expect(result).toEqual({ twr: 12.5, hasData: true, docsCount: 0 });
      expect(mockCollectionGet).not.toHaveBeenCalled();
    });

    test('empty accountIds → delega a overall con snapshot', async () => {
      const result = await calculateMultiAccountTWR(
        'user1', 'YTD', 'USD', [], undefined, { snapshot: MOCK_SNAPSHOT }
      );

      expect(result).toEqual({ twr: 12.5, hasData: true, docsCount: 0 });
    });
  });

  describe('multi-account con snapshots', () => {
    const buildSnapshotDoc = (returns, timeline) => ({
      exists: true,
      data: () => ({ returns, timeline }),
    });

    test('N > 1, todos snapshots existen → promedio ponderado por totalValue, docsCount: 0', async () => {
      const snap1 = buildSnapshotDoc(
        { ytdReturn: 10.0 },
        [['2026-04-11', 60000, 0.5]]
      );
      const snap2 = buildSnapshotDoc(
        { ytdReturn: 20.0 },
        [['2026-04-11', 40000, 0.3]]
      );

      mockSnapshotDocGet
        .mockResolvedValueOnce(snap1)
        .mockResolvedValueOnce(snap2);

      const result = await calculateMultiAccountTWR(
        'user1', 'YTD', 'USD', ['acc1', 'acc2'], undefined, {}
      );

      // Weighted: (10 * 60000/100000) + (20 * 40000/100000) = 6 + 8 = 14
      expect(result.twr).toBeCloseTo(14.0, 2);
      expect(result.hasData).toBe(true);
      expect(result.docsCount).toBe(0);
      expect(mockCollectionGet).not.toHaveBeenCalled();
    });

    test('N > 1, un snapshot faltante → fallback a legacy completo (all-or-nothing)', async () => {
      const snap1 = buildSnapshotDoc(
        { ytdReturn: 10.0 },
        [['2026-04-11', 60000, 0.5]]
      );
      const missingSnap = { exists: false };

      mockSnapshotDocGet
        .mockResolvedValueOnce(snap1)
        .mockResolvedValueOnce(missingSnap);

      const result = await calculateMultiAccountTWR(
        'user1', 'YTD', 'USD', ['acc1', 'acc2'], undefined, {}
      );

      // Se ejecuta legacy (mockCollectionGet)
      expect(mockCollectionGet).toHaveBeenCalled();
    });

    test('multi-account + dateRange → siempre legacy', async () => {
      const dateRange = {
        startDate: new Date('2026-02-01'),
        endDate: new Date('2026-02-28'),
      };

      const result = await calculateMultiAccountTWR(
        'user1', 'YTD', 'USD', ['acc1', 'acc2'], dateRange, {}
      );

      expect(mockCollectionGet).toHaveBeenCalled();
      expect(mockSnapshotDocGet).not.toHaveBeenCalled();
    });

    test('N > 1, todos snapshots existen pero totalValue=0 → promedio simple', async () => {
      const snap1 = buildSnapshotDoc({ ytdReturn: 10.0 }, []);
      const snap2 = buildSnapshotDoc({ ytdReturn: 20.0 }, []);

      mockSnapshotDocGet
        .mockResolvedValueOnce(snap1)
        .mockResolvedValueOnce(snap2);

      const result = await calculateMultiAccountTWR(
        'user1', 'YTD', 'USD', ['acc1', 'acc2'], undefined, {}
      );

      expect(result.twr).toBeCloseTo(15.0, 2);
      expect(result.docsCount).toBe(0);
    });

    test('filtra accountId overall de la lista multi-cuenta', async () => {
      const snap1 = buildSnapshotDoc(
        { ytdReturn: 10.0 },
        [['2026-04-11', 50000, 0.5]]
      );

      mockSnapshotDocGet.mockResolvedValueOnce(snap1);

      const result = await calculateMultiAccountTWR(
        'user1', 'YTD', 'USD', ['overall', 'acc1'], undefined, {}
      );

      expect(mockBuildSnapshotDocId).toHaveBeenCalledWith('user1', 'acc1', 'USD');
      expect(result.twr).toBeCloseTo(10.0, 2);
      expect(result.docsCount).toBe(0);
    });
  });
});

// ============================================================================
// Tests: PERIOD_TO_SNAPSHOT_FIELD constant
// ============================================================================

describe('PERIOD_TO_SNAPSHOT_FIELD', () => {
  test('contiene todos los períodos estándar', () => {
    expect(Object.keys(PERIOD_TO_SNAPSHOT_FIELD).sort()).toEqual(
      ['1M', '1Y', '2Y', '3M', '5Y', '6M', 'ALL', 'YTD']
    );
  });

  test('ALL y 5Y apuntan al mismo campo', () => {
    expect(PERIOD_TO_SNAPSHOT_FIELD['ALL']).toBe(PERIOD_TO_SNAPSHOT_FIELD['5Y']);
    expect(PERIOD_TO_SNAPSHOT_FIELD['ALL']).toBe('fiveYearReturn');
  });
});
