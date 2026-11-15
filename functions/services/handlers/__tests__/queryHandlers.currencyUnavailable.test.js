/**
 * HU 6.11.1 — Sin con qué calcular en una moneda poco usada, se avisa y se ofrece USD.
 *
 * - Portafolio, una cuenta o varias en una moneda poco usada sin derivado (servicio
 *   de tasas caído, hueco de más de 5 días, tasa inválida): responden el aviso y
 *   **no leen nada guardado en esa moneda** (Escenario 1, RN-6.11-J, RN-6.11-K;
 *   QA-01, QA-02, QA-03, QA-16 con las lecturas contadas).
 * - El aviso no se recuerda: la siguiente consulta vuelve a intentarlo y, si ya
 *   hay tasas, responde la cifra (Escenario 3, QA-06).
 * - USD y la moneda principal no cambian, ni con el servicio caído (Escenario 6, QA-09).
 * - Con CURRENCY_UNAVAILABLE_NOTICE=false, el comportamiento de la 6.9.2 (Escenario 7, QA-10).
 *
 * @see platform-docs/stories/6.11.1-sin-tasa-aviso-y-usd/refinamiento.md (T1, T8b)
 */

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: { serverTimestamp: jest.fn(() => 'mock-server-timestamp') },
  }),
  initializeApp: jest.fn(),
}));

jest.mock('firebase-functions/v2/https', () => ({
  HttpsError: class HttpsError extends Error {
    constructor(code, message, details) {
      super(message);
      this.code = code;
      this.details = details;
      this.httpErrorCode = { canonicalName: code };
    }
  },
}));

jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(() => new Date('2026-11-14T23:59:59Z')),
}));
jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(() => ({ returns: { ytdReturn: 9.75 }, totalValueData: { dates: [], values: [] } })),
  getHistoricalReturnsInternal: jest.fn(),
}));
const mockLegacyV2 = jest.fn();
jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: (...args) => mockLegacyV2(...args),
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

// El servicio de derivación: `obtenerResumenDerivado` lo decide cada test; el
// aviso es el mismo error que construye el servicio (probado en su propio test).
const mockObtenerDerivado = jest.fn();
jest.mock('../../snapshots/derivedCurrencySnapshotService', () => {
  const { HttpsError } = require('firebase-functions/v2/https');
  return {
    resumenesDerivadosDelUsd: () => process.env.DERIVED_CURRENCY_SNAPSHOTS !== 'false',
    avisoMonedaNoDisponible: () => process.env.CURRENCY_UNAVAILABLE_NOTICE !== 'false',
    monedaNoDisponible: (currency, motivo) => new HttpsError(
      'unavailable',
      `La cifra no está disponible en ${currency} por ahora`,
      { code: 'CURRENCY_UNAVAILABLE', currency, motivo },
    ),
    obtenerResumenDerivado: (...args) => mockObtenerDerivado(...args),
  };
});

// Firestore: cada ruta de documento devuelve lo que diga `mockDocumentos`, y
// cada lectura (documento o colección) queda anotada en `mockLecturas`.
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
  const coleccion = (nombre) => {
    if (nombre === 'portfolioAccounts') return consultaDeCuentas;
    const consulta = {
      where: () => consulta,
      orderBy: () => consulta,
      get: async () => { mockLecturas.push(nombre); return { empty: true, docs: [] }; },
    };
    return { doc: (id) => doc(`${nombre}/${id}`), ...consulta };
  };
  const mockDb = { doc, collection: coleccion };
  return { firestore: () => mockDb, ...mockDb };
});

const { getHistoricalReturns, getMultiAccountHistoricalReturns, clearSnapshotMemCache } = require('../queryHandlers');

// ============================================================================
// Datos
// ============================================================================

const UID = 'uid-inversor-M';
const SENAL = '2026-11-14T05:05:00.000Z';
/** GUA-MARCA: lo guardado en la moneda poco usada, imposible de obtener al calcular. */
const MARCA = 1111111111.11;

const resumen = (currency, overrides = {}) => ({
  userId: UID,
  currency,
  accountId: 'overall',
  schemaVersion: 3,
  lastUpdated: '2026-11-14T06:00:00.000Z',
  lastDateInTimeline: '2026-11-13',
  timeline: [{ d: '2026-11-12', v: 18000, c: 0 }, { d: '2026-11-13', v: 18240.57, c: 1.34 }],
  returns: { ytdReturn: 9.75, hasYtdData: true, hasOneMonthData: true, hasThreeMonthData: true },
  performanceByYear: {},
  monthlyCompound: {},
  availableYears: ['2026'],
  startDate: '2026-03-12',
  ...overrides,
});

const marcado = (currency, accountId = 'overall') => resumen(currency, {
  accountId,
  timeline: [{ d: '2026-11-13', v: MARCA, c: 0 }],
  returns: { ytdReturn: -99.98, hasYtdData: true },
});

const consultar = (currency = 'COP', accountId = 'overall') => getHistoricalReturns(
  { auth: { uid: UID } },
  { currency, accountId },
);

const consultarVarias = (accountIds, currency = 'COP') => getMultiAccountHistoricalReturns(
  { auth: { uid: UID } },
  { accountIds, currency },
);

const noDerivable = (motivo) => ({ snapshot: null, metodo: 'no-derivable', motivo });

/** Lo que se leyó guardado en la moneda poco usada (resúmenes o días). */
const lecturasDe = (currency) => mockLecturas.filter((ruta) => ruta.endsWith(`_${currency}`) || ruta.includes('/dates'));

// ============================================================================
// Tests
// ============================================================================

describe('HU 6.11.1: sin con qué calcular en una moneda poco usada, aviso en lugar de lo guardado', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotMemCache();
    mockLecturas.length = 0;
    mockCuentas.length = 0;
    Object.keys(mockDocumentos).forEach((k) => delete mockDocumentos[k]);
    mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: SENAL };
    // GUA-MARCA en el portafolio y en las dos cuentas
    mockDocumentos[`performanceSnapshots/${UID}_COP`] = marcado('COP');
    mockDocumentos[`performanceSnapshots/${UID}_acc-us_COP`] = marcado('COP', 'acc-us');
    mockDocumentos[`performanceSnapshots/${UID}_acc-co_COP`] = marcado('COP', 'acc-co');
    mockGeneratePerformanceSnapshot.mockResolvedValue(marcado('COP'));
    mockLegacyV2.mockResolvedValue({ returns: { ytdReturn: -99.98 } });
    delete process.env.CURRENCY_UNAVAILABLE_NOTICE;
    delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  });

  afterAll(() => {
    delete process.env.CURRENCY_UNAVAILABLE_NOTICE;
    delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  });

  it.each([
    ['el servicio de tasas no responde (QA-01)', 'sin-tasas'],
    ['falta el cierre en los 5 días anteriores a un día (QA-02)', 'sin-tasa-2026-10-16'],
    ['una tasa de cierre es inválida (QA-03)', 'tasa-invalida-2026-09-15'],
  ])('portafolio: si %s responde el aviso y no lee nada guardado en COP', async (_caso, motivo) => {
    mockObtenerDerivado.mockResolvedValue(noDerivable(motivo));

    const error = await consultar().catch((e) => e);

    expect(error.code).toBe('unavailable');
    expect(error.details).toEqual({ code: 'CURRENCY_UNAVAILABLE', currency: 'COP', motivo });
    expect(lecturasDe('COP')).toEqual([]);
    expect(mockGeneratePerformanceSnapshot).not.toHaveBeenCalled();
    expect(mockLegacyV2).not.toHaveBeenCalled();
  });

  it('una sola cuenta (V-GRAF-1): el mismo aviso, sin leer el resumen guardado de la cuenta en EUR', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_acc-co_EUR`] = marcado('EUR', 'acc-co');
    mockObtenerDerivado.mockResolvedValue(noDerivable('sin-tasas'));

    const error = await consultar('EUR', 'acc-co').catch((e) => e);

    expect(mockObtenerDerivado).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ accountId: 'acc-co', currency: 'EUR' }));
    expect(error.details).toEqual({ code: 'CURRENCY_UNAVAILABLE', currency: 'EUR', motivo: 'sin-tasas' });
    expect(lecturasDe('EUR')).toEqual([]);
  });

  it('varias cuentas: si una no se puede derivar, aviso para la consulta entera, sin resúmenes guardados ni ruta legacy', async () => {
    mockCuentas.push('acc-us', 'acc-co', 'acc-br');
    mockObtenerDerivado.mockImplementation(async (db, { accountId }) => (accountId === 'acc-co'
      ? noDerivable('sin-tasa-2026-10-16')
      : { snapshot: resumen('COP', { accountId }), metodo: 'derivado' }));

    const error = await consultarVarias(['acc-us', 'acc-co']).catch((e) => e);

    expect(error.code).toBe('unavailable');
    expect(error.details.currency).toBe('COP');
    expect(lecturasDe('COP')).toEqual([]);
    expect(mockGeneratePerformanceSnapshot).not.toHaveBeenCalled();
  });

  it('Escenario 3: el aviso no se recuerda; se reintenta en cada consulta y, cuando vuelven las tasas, responde la cifra (QA-06)', async () => {
    mockObtenerDerivado.mockResolvedValue(noDerivable('sin-tasas'));
    await expect(consultar()).rejects.toMatchObject({ code: 'unavailable' });
    await expect(consultar()).rejects.toMatchObject({ code: 'unavailable' }); // 10:05, sigue caído

    mockObtenerDerivado.mockResolvedValue({ snapshot: resumen('COP', { timeline: [{ d: '2026-11-13', v: 68258037, c: 0 }] }), metodo: 'derivado' });
    const r = await consultar(); // 10:20, ya responde

    expect(mockObtenerDerivado).toHaveBeenCalledTimes(3);
    expect(r.totalValueData.values).toEqual([68258037]);
    expect(lecturasDe('COP')).toEqual([]);
  });

  it('Escenario 6: en USD no se pregunta por la derivación ni hay aviso, aunque el servicio de tasas esté caído (QA-09)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen('USD');
    mockObtenerDerivado.mockResolvedValue(noDerivable('sin-tasas'));

    const r = await consultar('USD');

    expect(mockObtenerDerivado).not.toHaveBeenCalled();
    expect(r.totalValueData.values).toEqual([18000, 18240.57]);
  });

  it('Escenario 6: en la moneda principal distinta de USD sigue su resumen guardado, como antes (QA-09, RN-6.11-N)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_COP`] = resumen('COP', { returns: { ytdReturn: 4.4, hasYtdData: true } });
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo: 'no-aplica' });

    const r = await consultar('COP');

    expect(r.returns.ytdReturn).toBe(4.4);
    expect(mockLecturas).toContain(`performanceSnapshots/${UID}_COP`);
  });

  it('Escenario 7: con CURRENCY_UNAVAILABLE_NOTICE=false vuelve el comportamiento de la 6.9.2: lee lo guardado (RN-6-H, QA-10)', async () => {
    process.env.CURRENCY_UNAVAILABLE_NOTICE = 'false';
    mockObtenerDerivado.mockResolvedValue(noDerivable('sin-tasas'));

    const r = await consultar();

    expect(mockLecturas).toContain(`performanceSnapshots/${UID}_COP`);
    expect(r.totalValueData.values).toEqual([MARCA]);
  });
});
