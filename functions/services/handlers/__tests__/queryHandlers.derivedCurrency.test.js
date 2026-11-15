/**
 * HU 6.9.2 — Los rendimientos en una moneda poco usada salen del USD.
 *
 * - Portafolio o una cuenta en una moneda poco usada: se deriva del USD (QA-01).
 * - La segunda consulta sale de la caché en memoria (QA-02, QA-04).
 * - Un cierre o una corrección posteriores (señal nueva) obligan a volver a
 *   derivar (Escenario 3, QA-06, QA-08).
 * - USD o la moneda principal siguen su camino de siempre (QA-09).
 * - Varias cuentas en una moneda poco usada: cada una se deriva y se responde por
 *   la ruta de resúmenes, sin caer a la legacy.
 * - Con DERIVED_CURRENCY_SNAPSHOTS=false, el comportamiento de siempre (RN-6-H, QA-13).
 *
 * @see platform-docs/stories/6.9.2-resumenes-poco-usados-incrementales/refinamiento.md (T4, T6c)
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

jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(() => new Date('2026-11-14T23:59:59Z')),
}));
jest.mock('../../historicalReturnsService', () => ({
  // La ruta de varias cuentas agrega las series con esta función.
  calculateHistoricalReturns: jest.fn(() => ({ returns: { ytdReturn: 9.75 }, totalValueData: { dates: [], values: [] } })),
  getHistoricalReturnsInternal: jest.fn(),
}));
jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: jest.fn(),
  checkConsolidatedDataStatus: jest.fn(),
}));
jest.mock('../../portfolioDistributionService', () => ({}));
jest.mock('../../indexHistoryService', () => ({ calculateIndexData: jest.fn() }));
jest.mock('../../marketDataHelper', () => ({ getPricesFromApi: jest.fn() }));
jest.mock('../../../utils/mwrCalculations', () => ({
  calculateSimplePersonalReturn: jest.fn(),
  calculateModifiedDietzReturn: jest.fn(),
}));
jest.mock('../../riskMetrics/riskMetricsCache', () => ({
  isNYSEMarketOpen: jest.fn(() => false),
  calculateTTLUntilNextEOD: jest.fn(() => 86400000),
  MARKET_CACHE_TTL_MS: 300000,
}));

const mockGeneratePerformanceSnapshot = jest.fn();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (userId, accountId, currency) => (accountId === 'overall'
    ? `${userId}_${currency}`
    : `${userId}_${accountId}_${currency}`),
  generatePerformanceSnapshot: (...args) => mockGeneratePerformanceSnapshot(...args),
  generateAssetSnapshot: jest.fn(),
}));

jest.mock('../../snapshots/assetSnapshotOnDemandService', () => ({
  resumenesPorActivoBajoDemanda: () => true,
  ponerAlDiaResumenDeActivo: jest.fn(),
}));

const mockObtenerDerivado = jest.fn();
jest.mock('../../snapshots/derivedCurrencySnapshotService', () => ({
  resumenesDerivadosDelUsd: () => process.env.DERIVED_CURRENCY_SNAPSHOTS !== 'false',
  obtenerResumenDerivado: (...args) => mockObtenerDerivado(...args),
}));

// Firestore: cada ruta de documento devuelve lo que diga `mockDocumentos`
const mockDocumentos = {};
const mockLecturas = [];
const mockCuentas = [];
jest.mock('../../firebaseAdmin', () => {
  const doc = (ruta) => ({
    get: async () => {
      mockLecturas.push(ruta);
      const data = mockDocumentos[ruta];
      return { exists: data !== undefined, data: () => data };
    },
    set: jest.fn(),
  });
  const consultaDeCuentas = {
    where: () => consultaDeCuentas,
    get: async () => ({ docs: mockCuentas.map((id) => ({ id, data: () => ({}) })) }),
  };
  const mockDb = { doc, collection: (nombre) => (nombre === 'portfolioAccounts' ? consultaDeCuentas : { doc: (id) => doc(`${nombre}/${id}`) }) };
  return { firestore: () => mockDb, ...mockDb };
});

const { getHistoricalReturns, getMultiAccountHistoricalReturns, clearSnapshotMemCache } = require('../queryHandlers');

// ============================================================================
// Datos
// ============================================================================

const UID = 'user-1';
const SENAL = '2026-11-14T05:05:00.000Z';

const derivado = (overrides = {}) => ({
  userId: UID,
  currency: 'COP',
  accountId: 'overall',
  schemaVersion: 3,
  lastDateInTimeline: '2026-11-13',
  timeline: [{ d: '2026-11-12', v: 4000000, c: 0 }, { d: '2026-11-13', v: 4080400, c: 2.01 }],
  returns: { ytdReturn: 9.75, hasYtdData: true, hasOneMonthData: true, hasThreeMonthData: true },
  performanceByYear: {},
  monthlyCompound: {},
  availableYears: ['2026'],
  startDate: '2026-01-02',
  derivedFrom: 'USD',
  ...overrides,
});

const consultar = (currency = 'COP', accountId = 'overall') => getHistoricalReturns(
  { auth: { uid: UID } },
  { currency, accountId },
);

// ============================================================================
// Tests
// ============================================================================

describe('HU 6.9.2: rendimientos en una moneda poco usada', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotMemCache();
    mockLecturas.length = 0;
    mockCuentas.length = 0;
    Object.keys(mockDocumentos).forEach((k) => delete mockDocumentos[k]);
    mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: SENAL };
    delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  });

  afterEach(() => { delete process.env.DERIVED_CURRENCY_SNAPSHOTS; });

  it('se deriva del USD con la señal del inversor y no lee el resumen guardado en esa moneda (QA-01)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_COP`] = derivado({ returns: { ytdReturn: -99.98, hasYtdData: true } });
    mockObtenerDerivado.mockResolvedValue({ snapshot: derivado(), metodo: 'derivado' });

    const r = await consultar();

    expect(mockObtenerDerivado).toHaveBeenCalledWith(expect.anything(), {
      userId: UID, accountId: 'overall', currency: 'COP', senal: SENAL, prepararUsd: true,
    });
    expect(r.returns.ytdReturn).toBe(9.75);
    expect(r.totalValueData.values).toEqual([4000000, 4080400]);
    expect(r.lastSnapshotUpdate).toBe(SENAL);
    expect(mockLecturas).not.toContain(`performanceSnapshots/${UID}_COP`);
  });

  it('la segunda consulta sale de la caché: ni deriva otra vez ni lee nada más que la señal (QA-02, QA-04)', async () => {
    mockObtenerDerivado.mockResolvedValue({ snapshot: derivado(), metodo: 'derivado' });
    await consultar();
    mockLecturas.length = 0;

    const r = await consultar();

    expect(mockObtenerDerivado).toHaveBeenCalledTimes(1);
    expect(mockLecturas).toEqual([`portfolioPerformance/${UID}`]);
    expect(r.returns.ytdReturn).toBe(9.75);
  });

  it('Escenario 3: una señal posterior (cierre o corrección) obliga a volver a derivar (QA-06, QA-08)', async () => {
    mockObtenerDerivado.mockResolvedValue({ snapshot: derivado(), metodo: 'derivado' });
    await consultar();
    mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: new Date(Date.now() + 60000).toISOString() };
    mockObtenerDerivado.mockResolvedValue({ snapshot: derivado({ returns: { ytdReturn: 8.1, hasYtdData: true } }), metodo: 'derivado' });

    const r = await consultar();

    expect(mockObtenerDerivado).toHaveBeenCalledTimes(2);
    expect(r.returns.ytdReturn).toBe(8.1);
  });

  it('USD no pasa por la derivación, y la moneda principal sigue con su resumen guardado (QA-09)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = derivado({ currency: 'USD', lastUpdated: '2026-11-14T06:00:00.000Z' });
    mockDocumentos[`performanceSnapshots/${UID}_EUR`] = derivado({ currency: 'EUR', lastUpdated: '2026-11-14T06:00:00.000Z', returns: { ytdReturn: 4.4, hasYtdData: true } });
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo: 'no-aplica' });

    await consultar('USD');
    const r = await consultar('EUR');

    expect(mockObtenerDerivado).toHaveBeenCalledTimes(1); // solo EUR pregunta; USD ni pregunta
    expect(r.returns.ytdReturn).toBe(4.4);
  });

  it('varias cuentas en una moneda poco usada: cada una se deriva y responde la ruta de resúmenes', async () => {
    mockCuentas.push('acc-1', 'acc-2', 'acc-3');
    mockObtenerDerivado.mockImplementation(async (db, { accountId }) => ({
      snapshot: derivado({ accountId }), metodo: 'derivado',
    }));

    const r = await getMultiAccountHistoricalReturns(
      { auth: { uid: UID } },
      { accountIds: ['acc-1', 'acc-2'], currency: 'COP' },
    );

    expect(mockObtenerDerivado.mock.calls.map(([, p]) => p.accountId)).toEqual(['acc-1', 'acc-2']);
    expect(r._metadata.version).toBe('snapshot-multi');
    expect(mockGeneratePerformanceSnapshot).not.toHaveBeenCalled();
  });

  it('con DERIVED_CURRENCY_SNAPSHOTS=false no se deriva: el resumen viejo se rehace como siempre (RN-6-H, QA-13)', async () => {
    process.env.DERIVED_CURRENCY_SNAPSHOTS = 'false';
    mockDocumentos[`performanceSnapshots/${UID}_COP`] = derivado({ lastUpdated: '2026-04-15T17:18:22.116Z' });
    mockGeneratePerformanceSnapshot.mockResolvedValue(derivado({ returns: { ytdReturn: 7.7, hasYtdData: true } }));

    const r = await consultar();

    expect(mockObtenerDerivado).not.toHaveBeenCalled();
    expect(mockGeneratePerformanceSnapshot).toHaveBeenCalledWith(expect.anything(), UID, 'overall', 'COP');
    expect(r.returns.ytdReturn).toBe(7.7);
  });
});
