/**
 * PERF-SNAP-013: Tests para calculateCustomRangeTWR y su integración en calculatePeriodTWR
 *
 * @see docs/stories/PERF-SNAP-013.story.md
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

jest.mock('../../../services/snapshotGenerator', () => ({
  buildSnapshotDocId: jest.fn((userId, accountId, currency) => {
    if (accountId === 'overall') return `${userId}_${currency}`;
    return `${userId}_${accountId}_${currency}`;
  }),
  generatePerformanceSnapshot: jest.fn().mockResolvedValue(),
}));

// ============================================================================
// Import
// ============================================================================

const {
  calculateCustomRangeTWR,
  calculatePeriodTWR,
} = require('../attributionService');

// ============================================================================
// Fixtures
// ============================================================================

const MOCK_TIMELINE_SNAPSHOT = {
  returns: {
    ytdReturn: 12.5,
    oneMonthReturn: 2.3,
  },
  timeline: [
    ['2026-02-01', 50000, 0],
    ['2026-02-02', 50100, 0.2],
    ['2026-02-03', 50250, 0.3],
    ['2026-02-04', 50000, -0.497],
    ['2026-03-01', 51000, 0.5],
    ['2026-03-15', 51500, 0.98],
    ['2026-03-31', 52000, 0.97],
  ],
};

const buildLegacyDocs = (dailyChanges) => ({
  empty: dailyChanges.length === 0,
  size: dailyChanges.length,
  docs: dailyChanges.map((change, i) => ({
    data: () => ({
      date: `2026-02-${String(i + 1).padStart(2, '0')}`,
      USD: { adjustedDailyChangePercentage: change, totalValue: 50000 + i * 100 },
    }),
  })),
});

// ============================================================================
// Tests: calculateCustomRangeTWR
// ============================================================================

describe('calculateCustomRangeTWR', () => {
  test('AC1: rango que cubre entries completos retorna TWR compuesto correcto', () => {
    const result = calculateCustomRangeTWR(MOCK_TIMELINE_SNAPSHOT, '2026-02-01', '2026-03-31');

    // Composición manual: (1+0/100)*(1+0.2/100)*(1+0.3/100)*(1-0.497/100)*(1+0.5/100)*(1+0.98/100)*(1+0.97/100) - 1
    const expected =
      (1 + 0 / 100) *
      (1 + 0.2 / 100) *
      (1 + 0.3 / 100) *
      (1 - 0.497 / 100) *
      (1 + 0.5 / 100) *
      (1 + 0.98 / 100) *
      (1 + 0.97 / 100);
    const expectedTWR = (expected - 1) * 100;

    expect(result.twr).toBeCloseTo(expectedTWR, 6);
    expect(result.hasData).toBe(true);
  });

  test('AC2: retorna docsCount: 0 siempre', () => {
    const result = calculateCustomRangeTWR(MOCK_TIMELINE_SNAPSHOT, '2026-02-01', '2026-03-31');

    expect(result.docsCount).toBe(0);
  });

  test('AC3: TWR equivalente a composición manual de dailyChangePercentage', () => {
    const changes = [0.2, 0.3, -0.497, 0.5, 0.98, 0.97];
    let manualCompound = 1.0;
    for (const c of changes) {
      manualCompound *= (1 + c / 100);
    }
    const manualTWR = (manualCompound - 1) * 100;

    const result = calculateCustomRangeTWR(MOCK_TIMELINE_SNAPSHOT, '2026-02-01', '2026-03-31');

    expect(result.twr).toBeCloseTo(manualTWR, 6);
  });

  test('AC1: timeline vacío retorna { twr: 0, hasData: false, docsCount: 0 }', () => {
    const emptySnapshot = { timeline: [] };
    const result = calculateCustomRangeTWR(emptySnapshot, '2026-02-01', '2026-03-31');

    expect(result).toEqual({ twr: 0, hasData: false, docsCount: 0 });
  });

  test('timeline undefined retorna { twr: 0, hasData: false, docsCount: 0 }', () => {
    const result = calculateCustomRangeTWR({}, '2026-02-01', '2026-03-31');

    expect(result).toEqual({ twr: 0, hasData: false, docsCount: 0 });
  });

  test('snapshot null retorna { twr: 0, hasData: false, docsCount: 0 }', () => {
    const result = calculateCustomRangeTWR(null, '2026-02-01', '2026-03-31');

    expect(result).toEqual({ twr: 0, hasData: false, docsCount: 0 });
  });

  test('rango fuera de timeline (startDate antes del primer entry) retorna null para fallback', () => {
    const result = calculateCustomRangeTWR(MOCK_TIMELINE_SNAPSHOT, '2026-01-01', '2026-01-31');

    expect(result).toBeNull();
  });

  test('rango parcialmente cubierto (startDate antes del timeline) retorna null para fallback', () => {
    const result = calculateCustomRangeTWR(MOCK_TIMELINE_SNAPSHOT, '2025-12-01', '2026-02-15');

    expect(result).toBeNull();
  });

  test('rango con solo entries de dailyChange 0 retorna hasData false', () => {
    const snapshotOnlyZeros = {
      timeline: [
        ['2026-02-01', 50000, 0],
        ['2026-02-02', 50000, 0],
      ],
    };
    const result = calculateCustomRangeTWR(snapshotOnlyZeros, '2026-02-01', '2026-02-02');

    expect(result.twr).toBe(0);
    expect(result.hasData).toBe(false);
    expect(result.docsCount).toBe(0);
  });

  test('sub-rango dentro del timeline retorna TWR solo de ese rango', () => {
    const result = calculateCustomRangeTWR(MOCK_TIMELINE_SNAPSHOT, '2026-03-01', '2026-03-31');

    const expected = (1 + 0.5 / 100) * (1 + 0.98 / 100) * (1 + 0.97 / 100);
    const expectedTWR = (expected - 1) * 100;

    expect(result.twr).toBeCloseTo(expectedTWR, 6);
    expect(result.hasData).toBe(true);
  });
});

// ============================================================================
// Tests: calculatePeriodTWR integración con dateRange + snapshot
// ============================================================================

describe('calculatePeriodTWR con dateRange + snapshot (PERF-SNAP-013)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockCollectionGet.mockResolvedValue(buildLegacyDocs([1.0, 0.5, -0.3]));
  });

  test('AC4: dateRange + snapshot con cobertura → usa calculateCustomRangeTWR (0 reads)', async () => {
    const dateRange = {
      startDate: new Date('2026-02-01'),
      endDate: new Date('2026-03-31'),
    };

    const result = await calculatePeriodTWR('user1', 'CUSTOM', 'USD', 'overall', dateRange, {
      snapshot: MOCK_TIMELINE_SNAPSHOT,
    });

    expect(result.docsCount).toBe(0);
    expect(result.hasData).toBe(true);
    expect(mockCollectionGet).not.toHaveBeenCalled();
  });

  test('AC4: dateRange + snapshot sin cobertura (startDate antes del timeline) → fallback a legacy', async () => {
    const dateRange = {
      startDate: new Date('2025-12-01'),
      endDate: new Date('2026-02-28'),
    };

    const result = await calculatePeriodTWR('user1', 'CUSTOM', 'USD', 'overall', dateRange, {
      snapshot: MOCK_TIMELINE_SNAPSHOT,
    });

    expect(mockCollectionGet).toHaveBeenCalledTimes(1);
    expect(result.docsCount).toBe(3);
  });

  test('AC4: dateRange sin snapshot → legacy directamente', async () => {
    const dateRange = {
      startDate: new Date('2026-02-01'),
      endDate: new Date('2026-03-31'),
    };

    const result = await calculatePeriodTWR('user1', 'CUSTOM', 'USD', 'overall', dateRange);

    expect(mockCollectionGet).toHaveBeenCalledTimes(1);
    expect(result.docsCount).toBe(3);
  });

  test('período estándar sin dateRange + snapshot → sigue usando snapshot.returns', async () => {
    const result = await calculatePeriodTWR('user1', 'YTD', 'USD', 'overall', undefined, {
      snapshot: MOCK_TIMELINE_SNAPSHOT,
    });

    expect(result).toEqual({ twr: 12.5, hasData: true, docsCount: 0 });
    expect(mockCollectionGet).not.toHaveBeenCalled();
  });
});
