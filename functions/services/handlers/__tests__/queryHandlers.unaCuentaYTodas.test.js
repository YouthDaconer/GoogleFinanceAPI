/**
 * HU 6.11.2 — Una cuenta, todas y el Copiloto ya no recorren la historia.
 *
 * `getMultiAccountHistoricalReturns` con un solo alcance (una cuenta, todas las
 * cuentas elegidas una a una, o `overall` como pide el Copiloto) sale de los
 * resúmenes por `getHistoricalReturns`, no de `getHistoricalReturnsInternal`
 * (caché vieja o todos los documentos diarios).
 *
 * @see platform-docs/stories/6.11.2-una-cuenta-y-copiloto-sin-recorrer/refinamiento.md (T1, T5a)
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
  calculateDynamicTTL: jest.fn(() => new Date('2026-11-17T23:59:59Z')),
}));

const mockInternal = jest.fn();
jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: (...args) => mockInternal(...args),
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

const mockGenerate = jest.fn();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (userId, accountId, currency) => (accountId === 'overall'
    ? `${userId}_${currency}`
    : `${userId}_${accountId}_${currency}`),
  generatePerformanceSnapshot: (...args) => mockGenerate(...args),
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
  esMonedaPocoUsada: (currency, defaultCurrency) => !['USD', defaultCurrency].includes(currency),
  monedaPrincipalDe: async (db, userId) => (userId === 'inv-p' ? 'COP' : 'USD'),
  // Integración con la 6.11.1: sin con qué calcular, la pantalla recibe el aviso.
  avisoMonedaNoDisponible: () => process.env.CURRENCY_UNAVAILABLE_NOTICE !== 'false',
  monedaNoDisponible: (currency, motivo) => Object.assign(new Error(`La cifra no está disponible en ${currency} por ahora`), {
    code: 'unavailable', details: { code: 'CURRENCY_UNAVAILABLE', currency, motivo },
  }),
  CODIGO_MONEDA_NO_DISPONIBLE: 'CURRENCY_UNAVAILABLE',
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
    get: async () => {
      mockLecturas.push('portfolioAccounts?userId');
      return { docs: mockCuentas.map((id) => ({ id, data: () => ({}) })) };
    },
  };
  const mockDb = {
    doc,
    collection: (nombre) => (nombre === 'portfolioAccounts'
      ? consultaDeCuentas
      : { doc: (id) => doc(`${nombre}/${id}`) }),
  };
  return { firestore: () => mockDb, ...mockDb };
});

const {
  getMultiAccountHistoricalReturns,
  rendimientoDisponibleEn,
  clearSnapshotMemCache,
} = require('../queryHandlers');

// ============================================================================
// Datos
// ============================================================================

const UID = 'inv-m';
// En el pasado del reloj real: una señal futura invalidaría cualquier caché.
const SENAL = '2026-11-10T05:05:00.000Z';
const AL_DIA = '2026-11-10T05:10:00.000Z';

/** Resumen ya preparado: una entrada por día de mercado de la historia. */
const resumen = (overrides = {}) => ({
  userId: UID,
  currency: 'USD',
  accountId: 'overall',
  lastUpdated: AL_DIA,
  lastDateInTimeline: '2026-11-16',
  timeline: [{ d: '2026-11-13', v: 47000, c: 0 }, { d: '2026-11-16', v: 48250, c: 2.66 }],
  returns: { ytdReturn: 7.3413, hasYtdData: true, oneMonthReturn: 1.25 },
  validDocsCountByPeriod: { ytd: 220 },
  performanceByYear: {},
  monthlyCompound: {},
  availableYears: ['2026'],
  startDate: '2026-03-12',
  ...overrides,
});

const consultar = (accountIds, currency = 'USD', extra = {}, uid = UID) => getMultiAccountHistoricalReturns(
  { auth: { uid } },
  { accountIds, currency, ...extra },
);

// ============================================================================
// Tests
// ============================================================================

describe('HU 6.11.2: una cuenta, todas y el Copiloto salen de los resúmenes', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotMemCache();
    mockLecturas.length = 0;
    mockCuentas.length = 0;
    Object.keys(mockDocumentos).forEach((k) => delete mockDocumentos[k]);
    mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: SENAL };
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo: 'no-aplica' });
    delete process.env.PERFORMANCE_FROM_SNAPSHOTS;
    delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  });

  afterEach(() => {
    delete process.env.PERFORMANCE_FROM_SNAPSHOTS;
    delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  });

  it.each([
    ['el Copiloto (overall)', ['overall'], `performanceSnapshots/${UID}_USD`],
    ['todas (all)', ['all'], `performanceSnapshots/${UID}_USD`],
    ['una sola cuenta', ['cta-co'], `performanceSnapshots/${UID}_cta-co_USD`],
  ])('%s lee el resumen y no recorre la historia (QA-01, QA-02)', async (_caso, accountIds, ruta) => {
    mockDocumentos[ruta] = resumen();

    const r = await consultar(accountIds);

    expect(mockInternal).not.toHaveBeenCalled();
    expect(mockLecturas).toEqual([`portfolioPerformance/${UID}`, ruta]);
    expect(r.returns.ytdReturn).toBe(7.3413);
    expect(r.totalValueData.values).toEqual([47000, 48250]);
    expect(r._metadata.version).toBe('snapshot');
  });

  it('todas las cuentas elegidas una a una salen del resumen del portafolio (QA-01)', async () => {
    mockCuentas.push('cta-co', 'cta-us', 'cta-ret');
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen();

    const r = await consultar(['cta-us', 'cta-co', 'cta-ret']);

    expect(mockInternal).not.toHaveBeenCalled();
    expect(mockLecturas).toContain(`performanceSnapshots/${UID}_USD`);
    expect(r.totalValueData.values).toEqual([47000, 48250]);
  });

  it('el Copiloto relanzado da las mismas cifras sin volver a leer el resumen (QA-02)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen();
    const primero = await consultar(['overall']);
    mockLecturas.length = 0;

    const relanzado = await consultar(['overall']);

    expect(relanzado.returns).toEqual(primero.returns);
    expect(mockLecturas).toEqual([`portfolioPerformance/${UID}`]);
  });

  it('las lecturas no dependen de los años de historia: 170 y 1.260 días leen lo mismo (QA-04, RN-6-I)', async () => {
    const serie = (dias) => Array.from({ length: dias }, (_, i) => ({ d: `d${i}`, v: 1000 + i, c: 0.1 }));

    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen({ timeline: serie(170) });
    await consultar(['overall']);
    const lecturasMeses = mockLecturas.length;

    clearSnapshotMemCache();
    mockLecturas.length = 0;
    mockDocumentos[`portfolioPerformance/inv-y`] = { lastSnapshotUpdate: SENAL };
    mockDocumentos['performanceSnapshots/inv-y_USD'] = resumen({ userId: 'inv-y', timeline: serie(1260) });
    await consultar(['overall'], 'USD', {}, 'inv-y');

    expect(lecturasMeses).toBe(2);
    expect(mockLecturas.length).toBe(2);
  });

  it('en una moneda poco usada se deriva del USD y no se lee lo guardado en esa moneda (QA-03, RN-6.11-J)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_EUR`] = resumen({ currency: 'EUR', timeline: [{ d: '2026-11-16', v: 41512.8, c: 0 }] });
    mockObtenerDerivado.mockResolvedValue({
      snapshot: resumen({ currency: 'EUR', timeline: [{ d: '2026-11-13', v: 40655, c: 0 }, { d: '2026-11-16', v: 41736.25, c: 2.66 }] }),
      metodo: 'derivado',
    });

    const r = await consultar(['overall'], 'EUR');

    expect(mockObtenerDerivado).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
      userId: UID, accountId: 'overall', currency: 'EUR', prepararUsd: true,
    }));
    expect(r.totalValueData.values).toEqual([40655, 41736.25]);
    expect(mockLecturas).not.toContain(`performanceSnapshots/${UID}_EUR`);
    expect(mockInternal).not.toHaveBeenCalled();
  });

  it('la moneda principal COP sale de su resumen, sin derivar ni pedir tasas (QA-05, RN-6.11-N)', async () => {
    mockDocumentos['portfolioPerformance/inv-p'] = { lastSnapshotUpdate: SENAL };
    mockDocumentos['performanceSnapshots/inv-p_COP'] = resumen({ userId: 'inv-p', currency: 'COP', returns: { ytdReturn: 5.5 } });

    const r = await consultar(['overall'], 'COP', {}, 'inv-p');

    expect(r.returns.ytdReturn).toBe(5.5);
    expect(mockLecturas).toContain('performanceSnapshots/inv-p_COP');
    expect(mockInternal).not.toHaveBeenCalled();
  });

  it('Escenario 3: un resumen viejo tras un cierre se prepara una vez y la siguiente consulta ya no lo prepara (QA-10)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen({ lastUpdated: '2026-11-07T05:10:00.000Z', lastDateInTimeline: '2026-11-13' });
    mockGenerate.mockResolvedValue(resumen({ returns: { ytdReturn: 7.5, hasYtdData: true } }));

    const primera = await consultar(['overall']);
    const segunda = await consultar(['overall']);

    expect(mockGenerate).toHaveBeenCalledTimes(1);
    expect(mockGenerate).toHaveBeenCalledWith(expect.anything(), UID, 'overall', 'USD');
    expect(primera.returns.ytdReturn).toBe(7.5);
    expect(segunda.returns.ytdReturn).toBe(7.5);
  });

  it('Escenario 3: un resumen que no existe (cuenta nueva) se prepara una vez (QA-10)', async () => {
    mockGenerate.mockResolvedValue(resumen({ accountId: 'cta-ret', returns: { ytdReturn: 0.4 } }));

    const r = await consultar(['cta-ret']);
    await consultar(['cta-ret']);

    expect(mockGenerate).toHaveBeenCalledTimes(1);
    expect(r.returns.ytdReturn).toBe(0.4);
  });

  it('consultas simultáneas sobre el mismo resumen viejo lo preparan una sola vez (QA-11)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen({ lastUpdated: '2026-11-07T05:10:00.000Z' });
    let terminar;
    mockGenerate.mockImplementation(() => new Promise((resolve) => { terminar = resolve; }));

    const pantalla = consultar(['overall']);
    const copiloto = consultar(['all']);
    await new Promise((r) => setImmediate(r));
    terminar(resumen({ returns: { ytdReturn: 7.5 } }));
    const [a, b] = await Promise.all([pantalla, copiloto]);

    expect(mockGenerate).toHaveBeenCalledTimes(1);
    expect(a.returns.ytdReturn).toBe(7.5);
    expect(b.returns.ytdReturn).toBe(7.5);
  });

  it('ticker y forceRefresh siguen por el camino antiguo', async () => {
    mockInternal.mockResolvedValue({ returns: { ytdReturn: 1 } });

    await consultar(['cta-co'], 'USD', { ticker: 'PFBCOLOM', assetType: 'stock' });
    await consultar(['overall'], 'USD', { forceRefresh: true });

    expect(mockInternal).toHaveBeenCalledTimes(2);
    expect(mockInternal).toHaveBeenLastCalledWith(UID, expect.objectContaining({ accountId: 'overall', forceRefresh: true }));
  });

  it('con PERFORMANCE_FROM_SNAPSHOTS=false vuelve al camino anterior (Escenario 6, RN-6-H, QA-19)', async () => {
    process.env.PERFORMANCE_FROM_SNAPSHOTS = 'false';
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen();
    mockInternal.mockResolvedValue({ returns: { ytdReturn: 7.3412 } });

    const r = await consultar(['overall']);

    expect(mockInternal).toHaveBeenCalledWith(UID, expect.objectContaining({ accountId: 'overall', currency: 'USD' }));
    expect(r.returns.ytdReturn).toBe(7.3412);
    expect(mockLecturas).not.toContain(`performanceSnapshots/${UID}_USD`);
  });

  it('cada inversor solo ve su resumen: el alcance sale de la sesión, no de la petición (QA-18)', async () => {
    mockDocumentos[`performanceSnapshots/${UID}_USD`] = resumen();
    mockDocumentos['performanceSnapshots/inv-p_USD'] = resumen({ userId: 'inv-p', returns: { ytdReturn: 99 } });

    const r = await consultar(['overall'], 'USD', { userId: 'inv-p' });

    expect(r.returns.ytdReturn).toBe(7.3413);
    expect(mockLecturas).not.toContain('performanceSnapshots/inv-p_USD');
  });
});

describe('HU 6.11.2: rendimientoDisponibleEn (el Copiloto decide su moneda)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    clearSnapshotMemCache();
    mockLecturas.length = 0;
    mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: SENAL };
    delete process.env.DERIVED_CURRENCY_SNAPSHOTS;
  });

  it('USD y la moneda principal están disponibles sin preguntar a la derivación (QA-05)', async () => {
    expect(await rendimientoDisponibleEn(UID, 'USD')).toBe(true);
    expect(await rendimientoDisponibleEn('inv-p', 'COP')).toBe(true);
    expect(mockObtenerDerivado).not.toHaveBeenCalled();
  });

  it('una moneda poco usada sin con qué calcular no está disponible (QA-14, QA-15)', async () => {
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-tasas' });

    expect(await rendimientoDisponibleEn(UID, 'EUR')).toBe(false);
  });

  it('integración 6.11.1: el aviso CURRENCY_UNAVAILABLE de la pantalla, para el Copiloto, es "no disponible" (analiza en USD)', async () => {
    process.env.CURRENCY_UNAVAILABLE_NOTICE = 'true';
    mockObtenerDerivado.mockResolvedValue({ snapshot: null, metodo: 'no-derivable', motivo: 'tasa-invalida-2026-11-13' });

    await expect(rendimientoDisponibleEn(UID, 'EUR')).resolves.toBe(false);
    delete process.env.CURRENCY_UNAVAILABLE_NOTICE;
  });

  it('integración 6.11.1: un error que no es el aviso sigue su curso', async () => {
    mockObtenerDerivado.mockRejectedValue(new Error('Firestore caído'));

    await expect(rendimientoDisponibleEn(UID, 'EUR')).rejects.toThrow('Firestore caído');
  });

  it('una moneda poco usada derivable está disponible y el derivado queda para el rendimiento (QA-03)', async () => {
    mockObtenerDerivado.mockResolvedValue({ snapshot: resumen({ currency: 'EUR' }), metodo: 'derivado' });

    expect(await rendimientoDisponibleEn(UID, 'EUR')).toBe(true);
    await consultar(['overall'], 'EUR');

    expect(mockObtenerDerivado).toHaveBeenCalledTimes(1);
  });
});
