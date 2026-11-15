/**
 * HU 6.11.3 — Detalle de activo y Versus en monedas poco usadas desde USD.
 *
 * - Detalle de un activo (portafolio, una cuenta, varias): sale de sus días en
 *   USD y no lee el resumen guardado en esa moneda, ni lo rehace, ni va a la ruta
 *   legacy (Escenario 1, RN-6.11-J; QA-01, QA-05, QA-15 con las lecturas contadas).
 * - Sin con qué calcular, el aviso de la 6.11.1 en lugar de lo guardado, sin
 *   recordarlo (Escenario 3, RN-6.11-K; QA-07, QA-09, QA-10).
 * - Versus excluyendo activos: se calcula en USD y se convierte; la caché lleva
 *   `_usd` y nunca guarda el aviso (QA-02, QA-09).
 * - USD y la moneda principal siguen su camino (Escenario 6, QA-11) y el
 *   interruptor apagado devuelve el comportamiento anterior (Escenario 7, QA-18).
 *
 * @see platform-docs/stories/6.11.3-activo-comparacion-riesgo-usd/refinamiento.md (T8b)
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
const mockLegacyInterno = jest.fn();
jest.mock('../../historicalReturnsService', () => ({
  // Devuelve lo que recibe, para ver qué documentos se agregaron.
  calculateHistoricalReturns: jest.fn((docs, currency, ticker, assetType) => ({
    returns: { ytdReturn: 0 },
    totalValueData: { dates: docs.map((d) => d.data().date), values: [] },
    _calculo: { currency, ticker, assetType, docs: docs.map((d) => d.data()) },
  })),
  getHistoricalReturnsInternal: (...args) => mockLegacyInterno(...args),
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

const mockGenerateAssetSnapshot = jest.fn();
const mockFetchAssetDailyDocs = jest.fn();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (userId, accountId, currency, ticker, assetType) => [
    userId, accountId === 'overall' ? null : accountId, currency, ticker, assetType,
  ].filter(Boolean).join('_'),
  generatePerformanceSnapshot: jest.fn(),
  generateAssetSnapshot: (...args) => mockGenerateAssetSnapshot(...args),
  fetchAssetDailyDocs: (...args) => mockFetchAssetDailyDocs(...args),
}));

const mockPonerAlDia = jest.fn();
jest.mock('../../snapshots/assetSnapshotOnDemandService', () => ({
  resumenesPorActivoBajoDemanda: () => true,
  ponerAlDiaResumenDeActivo: (...args) => mockPonerAlDia(...args),
}));

jest.mock('../../snapshots/derivedCurrencySnapshotService', () => {
  const { HttpsError } = require('firebase-functions/v2/https');
  return {
    resumenesDerivadosDelUsd: () => true,
    avisoMonedaNoDisponible: () => true,
    monedaNoDisponible: (currency, motivo) => new HttpsError(
      'unavailable',
      `La cifra no está disponible en ${currency} por ahora`,
      { code: 'CURRENCY_UNAVAILABLE', currency, motivo },
    ),
    obtenerResumenDerivado: jest.fn(),
    esMonedaPocoUsada: jest.fn(),
    monedaPrincipalDe: jest.fn(),
    CODIGO_MONEDA_NO_DISPONIBLE: 'CURRENCY_UNAVAILABLE',
  };
});

// La decisión "¿desde USD?" es la del servicio (probada en su test): aquí, poco
// usada = ni USD ni COP (la principal de INV-P), salvo con el interruptor apagado.
const mockResumenDeActivo = jest.fn();
const mockDiasEnMoneda = jest.fn();
jest.mock('../../snapshots/rareCurrencyScreensService', () => ({
  calculaDesdeUsd: async (db, userId, currency) => process.env.RARE_CURRENCY_SCREENS_FROM_USD !== 'false'
    && !['USD', 'COP'].includes(currency),
  resumenDeActivoDesdeUsd: (...args) => mockResumenDeActivo(...args),
  diasEnMonedaDesdeUsd: (...args) => mockDiasEnMoneda(...args),
}));

const mockCarveOut = jest.fn();
jest.mock('../../carveOutReturnsService', () => ({
  getCarveOutHistoricalReturns: (...args) => mockCarveOut(...args),
}));

// Firestore: cada lectura (documento o colección) queda anotada en `mockLecturas`.
const mockDocumentos = {};
const mockLecturas = [];
const mockEscrituras = [];
const mockCuentas = [];
jest.mock('../../firebaseAdmin', () => {
  const doc = (ruta) => ({
    get: async () => {
      mockLecturas.push(ruta);
      const data = mockDocumentos[ruta];
      return { exists: data !== undefined, data: () => data };
    },
    set: async (data) => { mockEscrituras.push(ruta); mockDocumentos[ruta] = data; },
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
      get: async () => { mockLecturas.push(nombre); return { empty: true, size: 0, docs: [] }; },
    };
    return { doc: (id) => doc(`${nombre}/${id}`), ...consulta };
  };
  const mockDb = { doc, collection: coleccion };
  return { firestore: () => mockDb, ...mockDb };
});

const {
  getHistoricalReturns,
  getMultiAccountHistoricalReturns,
  clearSnapshotMemCache,
} = require('../queryHandlers');

// ============================================================================
// Datos
// ============================================================================

const UID = 'uid-inversor-A';
const SENAL = '2026-11-14T05:05:00.000Z';
/** GUARD-X: lo guardado en la moneda poco usada, imposible de obtener al calcular. */
const MARCA = 1111111111.11;

const resumenActivo = (overrides = {}) => ({
  type: 'asset',
  timelineGranularity: 'daily',
  timeline: [
    { d: '2025-06-13', v: 8964 * 4100, c: 0, u: 20 },
    { d: '2025-06-16', v: 0, c: 0, u: 0 },
    { d: '2025-09-22', v: 7576.5 * 3950, c: 0, u: 15 },
  ],
  returns: { ytdReturn: 3.1, hasYtdData: true },
  performanceByYear: {},
  monthlyCompound: {},
  ...overrides,
});

const contexto = { auth: { uid: UID } };
const detalle = (currency, accountId = 'overall', extra = {}) => getHistoricalReturns(
  contexto, { currency, accountId, ticker: 'MSFT', assetType: 'stock', ...extra },
);
const derivado = (snapshot = resumenActivo()) => ({ snapshot, metodo: 'derivado' });
const noDerivable = (motivo) => ({ snapshot: null, metodo: 'no-derivable', motivo });

/** Lo que se leyó guardado en la moneda: resúmenes `…_EUR…` o documentos diarios. */
const lecturasDe = (currency) => mockLecturas.filter((ruta) => ruta.includes(`_${currency}`) || ruta.includes('/dates'));

beforeEach(() => {
  jest.clearAllMocks();
  clearSnapshotMemCache();
  mockLecturas.length = 0;
  mockEscrituras.length = 0;
  mockCuentas.length = 0;
  Object.keys(mockDocumentos).forEach((k) => delete mockDocumentos[k]);
  mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: SENAL };
  // GUARD-X: resúmenes por activo guardados en EUR, con la marca
  mockDocumentos[`performanceSnapshots/${UID}_EUR_MSFT_stock`] = resumenActivo({ timeline: [{ d: '2025-09-22', v: MARCA, c: 0, u: 15 }] });
  mockDocumentos[`performanceSnapshots/${UID}_acc-us_EUR_MSFT_stock`] = resumenActivo({ timeline: [{ d: '2025-09-22', v: MARCA, c: 0, u: 15 }] });
  mockGenerateAssetSnapshot.mockResolvedValue(resumenActivo({ timeline: [{ d: '2025-09-22', v: MARCA, c: 0, u: 15 }] }));
  mockLegacyV2.mockResolvedValue({ returns: { ytdReturn: -99.98 } });
  mockLegacyInterno.mockResolvedValue({ returns: { ytdReturn: -99.98 } });
  delete process.env.RARE_CURRENCY_SCREENS_FROM_USD;
});

afterAll(() => {
  delete process.env.RARE_CURRENCY_SCREENS_FROM_USD;
});

// ============================================================================
// Detalle de activo
// ============================================================================

describe('HU 6.11.3: detalle de un activo en una moneda poco usada', () => {
  it('sale de los días en USD y no lee, rehace ni recurre a lo guardado en EUR (Escenario 1, QA-01, QA-15)', async () => {
    mockResumenDeActivo.mockResolvedValue(derivado());

    const r = await detalle('EUR');

    expect(mockResumenDeActivo).toHaveBeenCalledWith(expect.anything(), {
      userId: UID, accountId: 'overall', currency: 'EUR', ticker: 'MSFT', assetType: 'stock',
    });
    expect(r.totalValueData.values).toEqual([8964 * 4100, 0, 7576.5 * 3950]);
    expect(lecturasDe('EUR')).toEqual([]);
    expect(mockGenerateAssetSnapshot).not.toHaveBeenCalled();
    expect(mockPonerAlDia).not.toHaveBeenCalled();
    expect(mockLegacyV2).not.toHaveBeenCalled();
    expect(mockEscrituras).toEqual([]);
  });

  it('vendido y vuelto a comprar: el periodo sin posición sale igual que con la serie (Escenario 5, QA-05)', async () => {
    mockResumenDeActivo.mockResolvedValue(derivado());

    const r = await detalle('EUR');

    expect(r.totalValueData.soldRanges).toEqual([{ from: '2025-06-16', to: '2025-09-22' }]);
  });

  it.each([
    ['sin cierre en los 5 días naturales (QA-07)', 'sin-tasa-2026-08-10'],
    ['el servicio de tasas no responde (QA-09)', 'sin-tasas'],
    ['una tasa inválida (QA-09)', 'tasa-invalida-2026-10-15'],
  ])('%s: el aviso de la 6.11.1, sin lo guardado ni la ruta legacy', async (_caso, motivo) => {
    mockResumenDeActivo.mockResolvedValue(noDerivable(motivo));

    const error = await detalle('EUR').catch((e) => e);

    expect(error.code).toBe('unavailable');
    expect(error.details).toEqual({ code: 'CURRENCY_UNAVAILABLE', currency: 'EUR', motivo });
    expect(lecturasDe('EUR')).toEqual([]);
    expect(mockGenerateAssetSnapshot).not.toHaveBeenCalled();
    expect(mockLegacyV2).not.toHaveBeenCalled();
  });

  it('el aviso no se recuerda; la cifra sí, hasta el siguiente cierre (QA-10, QA-16)', async () => {
    mockResumenDeActivo.mockResolvedValueOnce(noDerivable('sin-tasas'));
    await expect(detalle('EUR')).rejects.toMatchObject({ code: 'unavailable' });

    mockResumenDeActivo.mockResolvedValue(derivado());
    await detalle('EUR'); // 10:05: se reintenta y calcula
    await detalle('EUR'); // 16:00: sin cierre ni cambio, de la memoria
    expect(mockResumenDeActivo).toHaveBeenCalledTimes(2);

    mockDocumentos[`portfolioPerformance/${UID}`] = { lastSnapshotUpdate: new Date(Date.now() + 60000).toISOString() };
    await detalle('EUR'); // tras un cierre o una corrección, una vez más
    expect(mockResumenDeActivo).toHaveBeenCalledTimes(3);
  });

  it('un activo sin datos en ese alcance responde vacío, sin la ruta legacy', async () => {
    mockResumenDeActivo.mockResolvedValue({ snapshot: null, metodo: 'sin-datos' });

    const r = await detalle('EUR', 'acc-us');

    expect(r.totalValueData.dates).toEqual([]);
    expect(mockLegacyV2).not.toHaveBeenCalled();
    expect(lecturasDe('EUR')).toEqual([]);
  });

  it('forceRefresh también sale de USD', async () => {
    mockResumenDeActivo.mockResolvedValue(derivado());

    await detalle('EUR', 'overall', { forceRefresh: true });

    expect(mockResumenDeActivo).toHaveBeenCalledTimes(1);
    expect(mockLegacyV2).not.toHaveBeenCalled();
  });

  it.each([['USD'], ['COP']])('en %s (USD o la principal) lee su resumen por activo como antes (Escenario 6, QA-11)', async (moneda) => {
    mockDocumentos[`performanceSnapshots/${UID}_${moneda}_MSFT_stock`] = resumenActivo();

    const r = await detalle(moneda);

    expect(mockResumenDeActivo).not.toHaveBeenCalled();
    expect(mockLecturas).toContain(`performanceSnapshots/${UID}_${moneda}_MSFT_stock`);
    expect(r.returns.ytdReturn).toBe(3.1);
  });

  it('con RARE_CURRENCY_SCREENS_FROM_USD=false vuelve a lo de antes (Escenario 7, QA-18)', async () => {
    process.env.RARE_CURRENCY_SCREENS_FROM_USD = 'false';

    const r = await detalle('EUR');

    expect(mockResumenDeActivo).not.toHaveBeenCalled();
    expect(r.totalValueData.values).toEqual([MARCA]);
  });
});

describe('HU 6.11.3: detalle de un activo con la selección de cuentas', () => {
  const consultarVarias = (accountIds, currency = 'EUR') => getMultiAccountHistoricalReturns(
    contexto, { accountIds, currency, ticker: 'MSFT', assetType: 'stock' },
  );

  it('"todas" o una cuenta van por el mismo camino desde USD, no por el antiguo', async () => {
    mockResumenDeActivo.mockResolvedValue(derivado());

    await consultarVarias(['overall']);
    await consultarVarias(['acc-us']);

    expect(mockResumenDeActivo.mock.calls.map((c) => c[1].accountId)).toEqual(['overall', 'acc-us']);
    expect(mockLegacyInterno).not.toHaveBeenCalled();
    expect(lecturasDe('EUR')).toEqual([]);
  });

  it('dos cuentas: sus días del activo en USD, convertidos y agregados, sin leer los días guardados', async () => {
    mockCuentas.push('acc-us', 'acc-co', 'acc-br');
    const diasUsd = { 'acc-us': [{ date: '2026-11-13', USD: { totalValue: 1 } }], 'acc-co': [{ date: '2026-11-13', USD: { totalValue: 2 } }] };
    mockFetchAssetDailyDocs.mockImplementation(async (db, uid, accountId) => diasUsd[accountId]);
    mockDiasEnMoneda.mockImplementation(async (dias) => ({
      dias: dias.map((d) => ({
        date: d.date,
        EUR: { totalValue: d.USD.totalValue * 0.8625, assetPerformance: { MSFT_stock: { totalValue: d.USD.totalValue * 0.8625 } } },
      })),
    }));

    const r = await consultarVarias(['acc-us', 'acc-co']);

    expect(mockFetchAssetDailyDocs.mock.calls.map((c) => [c[2], c[3]])).toEqual([['acc-us', 'USD'], ['acc-co', 'USD']]);
    expect(mockDiasEnMoneda).toHaveBeenCalledWith(diasUsd['acc-us'], 'EUR', { claveActivo: 'MSFT_stock' });
    expect(r._calculo.currency).toBe('EUR');
    expect(r._calculo.docs[0].EUR.assetPerformance.MSFT_stock.totalValue).toBe(3 * 0.8625);
    expect(lecturasDe('EUR')).toEqual([]);
    expect(mockEscrituras).toEqual([]);
  });

  it('dos cuentas: si una no tiene con qué calcular, el aviso para la consulta entera', async () => {
    mockCuentas.push('acc-us', 'acc-co', 'acc-br');
    mockFetchAssetDailyDocs.mockResolvedValue([]);
    mockDiasEnMoneda.mockResolvedValueOnce({ dias: [] }).mockResolvedValueOnce({ motivo: 'sin-tasas' });

    const error = await consultarVarias(['acc-us', 'acc-co']).catch((e) => e);

    expect(error.details).toEqual({ code: 'CURRENCY_UNAVAILABLE', currency: 'EUR', motivo: 'sin-tasas' });
    expect(lecturasDe('EUR')).toEqual([]);
  });
});

// ============================================================================
// Versus excluyendo activos
// ============================================================================

describe('HU 6.11.3: Versus excluyendo activos en una moneda poco usada', () => {
  const excluyendo = (currency) => getHistoricalReturns(
    contexto, { currency, accountId: 'overall', excludeTickers: ['MSFT', 'PFBCOLOM'] },
  );

  it('se calcula en USD y se convierte; la caché lleva _usd (QA-02)', async () => {
    mockCarveOut.mockImplementation(async (params, deps, opciones) => {
      const docs = await opciones.convertirDocs([{ date: '2026-11-13', USD: { totalValue: 100 } }]);
      return { returns: { ytdReturn: 4.2 }, docs };
    });
    mockDiasEnMoneda.mockResolvedValue({ dias: [{ date: '2026-11-13', EUR: { totalValue: 86.25 } }] });

    const r = await excluyendo('EUR');

    const [params, deps, opciones] = mockCarveOut.mock.calls[0];
    expect(params.currency).toBe('EUR');
    expect(deps).toBeUndefined();
    expect(opciones.monedaCalculo).toBe('USD');
    expect(mockDiasEnMoneda).toHaveBeenCalledWith([{ date: '2026-11-13', USD: { totalValue: 100 } }], 'EUR');
    expect(r.docs).toEqual([{ date: '2026-11-13', EUR: { totalValue: 86.25 } }]);
    expect(mockEscrituras).toHaveLength(1);
    expect(mockEscrituras[0]).toMatch(/^userData\/uid-inversor-A\/performanceCache\/EUR_overall_ex_[0-9a-f]{12}_usd$/);
  });

  it('no sirve lo que se guardó en la caché antes de la HU, hecho con lo guardado en EUR', async () => {
    mockCarveOut.mockResolvedValue({ returns: { ytdReturn: 4.2 } });
    // Una entrada vieja, vigente, con la clave de antes
    await excluyendo('USD');
    const claveVieja = mockEscrituras[0].replace('USD_', 'EUR_');
    mockDocumentos[claveVieja] = { data: { returns: { ytdReturn: -99.98 } }, validUntil: '2999-01-01T00:00:00Z' };
    mockDiasEnMoneda.mockResolvedValue({ dias: [] });

    const r = await excluyendo('EUR');

    expect(r.returns.ytdReturn).toBe(4.2);
    expect(mockLecturas).not.toContain(claveVieja);
  });

  it('sin con qué convertir, el aviso; no se guarda en la caché (QA-09, QA-10)', async () => {
    mockCarveOut.mockImplementation(async (params, deps, opciones) => opciones.convertirDocs([]));
    mockDiasEnMoneda.mockResolvedValue({ motivo: 'sin-tasa-2024-12-13' });

    const error = await excluyendo('EUR').catch((e) => e);

    expect(error.details).toEqual({ code: 'CURRENCY_UNAVAILABLE', currency: 'EUR', motivo: 'sin-tasa-2024-12-13' });
    expect(mockEscrituras).toEqual([]);
  });

  it.each([['USD'], ['COP']])('en %s se calcula como antes, sin conversión ni sufijo (Escenario 6)', async (moneda) => {
    mockCarveOut.mockResolvedValue({ returns: { ytdReturn: 4.2 } });

    await excluyendo(moneda);

    expect(mockCarveOut.mock.calls[0][2]).toBeUndefined();
    expect(mockEscrituras[0]).toMatch(new RegExp(`/${moneda}_overall_ex_[0-9a-f]{12}$`));
  });
});
