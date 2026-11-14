/**
 * HU 6.4 — El detalle de un activo prepara su resumen al abrirlo.
 *
 * - Sin resumen (nunca abierto, o borrado por una corrección del historial):
 *   se prepara entero y queda guardado (Escenarios 1 y 5).
 * - La segunda vez es inmediato: no se vuelve a preparar (Escenario 2).
 * - Viejo tras un cierre: se pone al día con los días que faltan (Escenario 4).
 * - Con ASSET_SNAPSHOTS_ON_DEMAND=false, el comportamiento de siempre (RN-6-H).
 *
 * @see platform-docs/stories/6.4-resumenes-activo-bajo-demanda/refinamiento.md (T4, T8d)
 */

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: { serverTimestamp: jest.fn(() => 'mock-server-timestamp') },
  }),
  initializeApp: jest.fn(),
}));

jest.mock('firebase-functions/v2/https', () => ({
  HttpsError: class HttpsError extends Error {
    constructor(code, message) { super(message); this.code = code; }
  },
}));

jest.mock('../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(() => new Date('2026-11-14T23:59:59Z')),
}));
jest.mock('../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: jest.fn(),
}));
jest.mock('../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: jest.fn(),
  checkConsolidatedDataStatus: jest.fn(),
}));
jest.mock('../portfolioDistributionService', () => ({}));
jest.mock('../indexHistoryService', () => ({ calculateIndexData: jest.fn() }));
jest.mock('../marketDataHelper', () => ({ getPricesFromApi: jest.fn() }));
jest.mock('../../utils/mwrCalculations', () => ({
  calculateSimplePersonalReturn: jest.fn(),
  calculateModifiedDietzReturn: jest.fn(),
}));
jest.mock('../riskMetrics/riskMetricsCache', () => ({
  isNYSEMarketOpen: jest.fn(() => false),
  calculateTTLUntilNextEOD: jest.fn(() => 86400000),
  MARKET_CACHE_TTL_MS: 300000,
}));

const mockGenerateAssetSnapshot = jest.fn();
jest.mock('../snapshotGenerator', () => ({
  buildSnapshotDocId: (userId, accountId, currency, ticker, assetType) => (accountId === 'overall'
    ? `${userId}_${ticker}_${assetType}_${currency}`
    : `${userId}_${ticker}_${assetType}_${accountId}_${currency}`),
  generatePerformanceSnapshot: jest.fn(),
  generateAssetSnapshot: (...args) => mockGenerateAssetSnapshot(...args),
}));

const mockPonerAlDia = jest.fn();
jest.mock('../snapshots/assetSnapshotOnDemandService', () => ({
  resumenesPorActivoBajoDemanda: () => process.env.ASSET_SNAPSHOTS_ON_DEMAND !== 'false',
  ponerAlDiaResumenDeActivo: (...args) => mockPonerAlDia(...args),
}));

// Firestore: cada ruta de documento devuelve lo que diga `mockDocumentos`
const mockDocumentos = {};
const mockLecturas = [];
jest.mock('../firebaseAdmin', () => {
  const doc = (ruta) => ({
    get: async () => {
      mockLecturas.push(ruta);
      const data = mockDocumentos[ruta];
      return { exists: data !== undefined, data: () => data };
    },
    set: jest.fn(),
  });
  const mockDb = { doc, collection: jest.fn() };
  return { firestore: () => mockDb, ...mockDb };
});

const { getHistoricalReturns, clearSnapshotMemCache } = require('../handlers/queryHandlers');

// ============================================================================
// Datos
// ============================================================================

const UID = 'user-1';
const ID_RESUMEN = `performanceSnapshots/${UID}_AAPL_stock_USD`;
const SENAL = '2026-11-14T05:05:00.000Z';

const resumen = (overrides = {}) => ({
  userId: UID,
  currency: 'USD',
  accountId: 'overall',
  ticker: 'AAPL',
  assetType: 'stock',
  type: 'asset',
  schemaVersion: 3,
  timelineGranularity: 'daily',
  lastDateInTimeline: '2026-11-12',
  lastUpdated: '2026-11-13T05:10:00.000Z',
  timeline: [{ d: '2026-11-12', v: 5050, c: 1, u: 10 }],
  returns: { ytdReturn: 12.5, hasYtdData: true, hasOneMonthData: true, hasThreeMonthData: true },
  performanceByYear: {},
  monthlyCompound: {},
  availableYears: ['2026'],
  startDate: '2026-01-02',
  ...overrides,
});

const abrirActivo = () => getHistoricalReturns(
  { auth: { uid: UID } },
  { currency: 'USD', accountId: 'overall', ticker: 'AAPL', assetType: 'stock' },
);

// ============================================================================
// Tests
// ============================================================================

describe('HU 6.4: abrir el detalle de un activo', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotMemCache();
    mockLecturas.length = 0;
    Object.keys(mockDocumentos).forEach((k) => delete mockDocumentos[k]);
    mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: SENAL };
    delete process.env.ASSET_SNAPSHOTS_ON_DEMAND;
  });

  afterEach(() => { delete process.env.ASSET_SNAPSHOTS_ON_DEMAND; });

  it('sin resumen (nunca abierto o borrado por una corrección): lo prepara entero y muestra sus cifras (Escenarios 1 y 5)', async () => {
    mockGenerateAssetSnapshot.mockResolvedValue(resumen({ lastUpdated: '2026-11-14T15:00:00.000Z' }));

    const r = await abrirActivo();

    expect(mockGenerateAssetSnapshot).toHaveBeenCalledWith(expect.anything(), UID, 'overall', 'USD', 'AAPL', 'stock');
    expect(mockPonerAlDia).not.toHaveBeenCalled();
    expect(r.returns.ytdReturn).toBe(12.5);
    expect(r._metadata.version).toBe('snapshot');
  });

  it('la segunda vez es inmediata: no se prepara de nuevo ni se vuelve a leer el resumen (Escenario 2)', async () => {
    mockGenerateAssetSnapshot.mockResolvedValue(resumen({ lastUpdated: '2026-11-14T15:00:00.000Z' }));
    await abrirActivo();
    mockLecturas.length = 0;

    const r = await abrirActivo();

    expect(mockGenerateAssetSnapshot).toHaveBeenCalledTimes(1);
    expect(mockLecturas).toEqual([`portfolioPerformance/${UID}`]);
    expect(r.returns.ytdReturn).toBe(12.5);
  });

  it('un resumen guardado y al día se sirve tal cual', async () => {
    mockDocumentos[ID_RESUMEN] = resumen({ lastUpdated: '2026-11-14T06:00:00.000Z' });

    const r = await abrirActivo();

    expect(mockGenerateAssetSnapshot).not.toHaveBeenCalled();
    expect(mockPonerAlDia).not.toHaveBeenCalled();
    expect(r.returns.ytdReturn).toBe(12.5);
  });

  it('viejo tras un cierre: se pone al día con el resumen guardado y la señal del cierre (Escenario 4)', async () => {
    const viejo = resumen();
    mockDocumentos[ID_RESUMEN] = viejo;
    mockPonerAlDia.mockResolvedValue({
      snapshot: resumen({ lastDateInTimeline: '2026-11-13', returns: { ytdReturn: 13.1, hasYtdData: true } }),
      metodo: 'incremental',
      diasLeidos: 1,
    });

    const r = await abrirActivo();

    expect(mockPonerAlDia).toHaveBeenCalledWith(expect.anything(), {
      userId: UID, accountId: 'overall', currency: 'USD', ticker: 'AAPL', assetType: 'stock', snapshot: viejo, senal: SENAL,
    });
    expect(mockGenerateAssetSnapshot).not.toHaveBeenCalled();
    expect(r.returns.ytdReturn).toBe(13.1);
  });

  it('con ASSET_SNAPSHOTS_ON_DEMAND=false un resumen viejo se rehace entero, como siempre (RN-6-H)', async () => {
    process.env.ASSET_SNAPSHOTS_ON_DEMAND = 'false';
    mockDocumentos[ID_RESUMEN] = resumen();
    mockGenerateAssetSnapshot.mockResolvedValue(resumen({ lastUpdated: '2026-11-14T15:00:00.000Z' }));

    await abrirActivo();

    expect(mockPonerAlDia).not.toHaveBeenCalled();
    expect(mockGenerateAssetSnapshot).toHaveBeenCalledTimes(1);
  });
});
