/**
 * HU 6.9.2 (T4) — La atribución deriva del USD los resúmenes en monedas poco
 * usadas en lugar de leer los guardados, que nadie mantiene.
 *
 * @see platform-docs/stories/6.9.2-resumenes-poco-usados-incrementales/refinamiento.md (T6c)
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

const mockObtenerDerivado = jest.fn();
jest.mock('../../snapshots/derivedCurrencySnapshotService', () => ({
  resumenesDerivadosDelUsd: () => process.env.DERIVED_CURRENCY_SNAPSHOTS !== 'false',
  obtenerResumenDerivado: (...args) => mockObtenerDerivado(...args),
}));

const { calculateMultiAccountTWR } = require('../attributionService');

const resumen = (twr, valor) => ({ returns: { ytdReturn: twr }, timeline: [{ d: '2026-11-13', v: valor, c: 0 }] });

describe('HU 6.9.2: atribución de varias cuentas en una moneda poco usada', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  });

  it('pondera el rendimiento de cada cuenta derivado del USD, sin leer los guardados en esa moneda', async () => {
    mockObtenerDerivado.mockImplementation(async (db, { accountId }) => ({
      snapshot: accountId === 'acc-1' ? resumen(10, 3000000) : resumen(4, 1000000),
      metodo: 'derivado',
    }));

    const r = await calculateMultiAccountTWR('user-1', 'YTD', 'COP', ['acc-1', 'acc-2'], null);

    expect(mockObtenerDerivado.mock.calls.map(([, p]) => [p.accountId, p.currency])).toEqual([['acc-1', 'COP'], ['acc-2', 'COP']]);
    expect(r.twr).toBeCloseTo(8.5, 10); // 10 · 3/4 + 4 · 1/4
    expect(mockSnapshotDocGet).not.toHaveBeenCalled();
  });

  it('en USD, o con el interruptor apagado, lee los resúmenes guardados como siempre (QA-09, QA-13)', async () => {
    mockSnapshotDocGet.mockResolvedValue({ exists: true, data: () => resumen(6, 100) });

    await calculateMultiAccountTWR('user-1', 'YTD', 'USD', ['acc-1', 'acc-2'], null);
    process.env.DERIVED_CURRENCY_SNAPSHOTS = 'false';
    await calculateMultiAccountTWR('user-1', 'YTD', 'COP', ['acc-1', 'acc-2'], null);

    expect(mockObtenerDerivado).not.toHaveBeenCalled();
    expect(mockSnapshotDocGet).toHaveBeenCalledTimes(4);
  });
});
