/**
 * HU 6.11.4 (T9b) — La contribución por activo en una moneda poco usada se
 * calcula desde USD (camino 1 de la HU).
 *
 * - RN-6-A: es **idéntica** (todos los decimales) a la de siempre calculada
 *   sobre REF-0: el bloque COP de cada día = USD × la tasa de cierre del día, la
 *   serie COP de las conversiones = los cierres y el P&G de las ventas = USD ×
 *   tasa del día (QA-01, QA-02, QA-03).
 * - RN-6.11-J: no se usa nunca el bloque guardado en COP (QA-15), ni se pide la
 *   serie de Yahoo de la moneda; la serie en USD se comparte con la caché de USD.
 * - RN-6.11-L / RN-6.11-K: sin el cierre de algún día del periodo, el aviso (QA-08).
 *
 * @see platform-docs/stories/6.11.4-atribucion-desde-usd/refinamiento.md (T9b)
 */

jest.mock('../../financeQuery', () => ({ getExchangeRates: jest.fn() }));

const { calculateHoldingsContributions, _seriesCache, _quoteCurrencyCache } = require('../holdingsContributions');
const { conversorDeCierres } = require('../attributionFromUsdService');
const { limpiarCachesDerivados } = require('../../snapshots/derivedCurrencySnapshotService');

const UID = 'uid-inversor-M';

// Cierres COP por USD. El 2026-01-03/04 es fin de semana: toma el del viernes 02.
const CIERRES = {
  '2026-01-02': 4000.5,
  '2026-01-05': 4010.25,
  '2026-01-06': 3990.75,
  '2026-01-07': 4020.125,
  '2026-01-08': 4005.375,
};

// ============================================================================
// Firestore en memoria (con traza de los usos del bloque COP)
// ============================================================================

const cumple = (valor, op, v) => (
  op === '==' ? valor === v : op === '<' ? valor < v : op === '<=' ? valor <= v
    : op === '>' ? valor > v : op === '>=' ? valor >= v : false);

function fakeDb(colecciones) {
  const usosCop = [];
  const vigilar = (ruta, data) => (ruta.startsWith('portfolioPerformance/')
    ? new Proxy(data, { get: (o, k) => { if (k === 'COP') usosCop.push(ruta); return o[k]; } })
    : data);
  const consulta = (ruta, filtros = [], orden = null, tope = null) => ({
    where: (f, op, v) => consulta(ruta, [...filtros, [f, op, v]], orden, tope),
    orderBy: (f, dir = 'asc') => consulta(ruta, filtros, [f, dir], tope),
    limit: (n) => consulta(ruta, filtros, orden, n),
    select: () => consulta(ruta, filtros, orden, tope),
    get: async () => {
      let docs = (colecciones[ruta] || []).filter((d) => filtros.every(([f, op, v]) => cumple(d.data[f], op, v)));
      const campo = orden ? orden[0] : (filtros.find(([, op]) => op !== '==') || [])[0];
      if (campo) docs = docs.slice().sort((x, y) => (x.data[campo] < y.data[campo] ? -1 : 1) * (orden?.[1] === 'desc' ? -1 : 1));
      if (tope !== null) docs = docs.slice(0, tope);
      return { empty: docs.length === 0, size: docs.length, docs: docs.map((d) => ({ id: d.id, data: () => vigilar(ruta, d.data) })) };
    },
  });
  return { usosCop, collection: (ruta) => consulta(ruta) };
}

/** Día con su bloque USD y un bloque COP guardado con cifras viejas que no deben usarse. */
const dia = (date, tenencias) => {
  const assetPerformance = Object.fromEntries(Object.entries(tenencias).map(([k, [v, u]]) => [k, { totalValue: v, units: u }]));
  const totalValue = Object.values(tenencias).reduce((s, [v]) => s + v, 0);
  return {
    id: date,
    data: {
      date,
      USD: { totalValue, assetPerformance },
      COP: { totalValue: 1, assetPerformance: { A_stock: { totalValue: 1, units: 1 } } },
    },
  };
};

const DIAS = [
  dia('2026-01-05', { A_stock: [600, 6], B_stock: [400, 4] }),
  dia('2026-01-06', { A_stock: [660, 6], B_stock: [380, 4] }),
  dia('2026-01-07', { A_stock: [726, 6] }),
  dia('2026-01-08', { A_stock: [1452, 12] }),
];
const DIAS_ACC2 = [
  dia('2026-01-05', { C_stock: [300, 3] }),
  dia('2026-01-07', { C_stock: [330, 3] }),
];

/** REF-0: el bloque COP de cada documento es USD × la tasa de cierre de su día. */
function conBloqueRef0(dias, tasaDe) {
  return dias.map(({ id, data }) => {
    const t = tasaDe(data.date);
    const ap = Object.fromEntries(Object.entries(data.USD.assetPerformance)
      .map(([k, a]) => [k, { totalValue: a.totalValue * t, units: a.units }]));
    return { id, data: { date: data.date, COP: { totalValue: data.USD.totalValue * t, assetPerformance: ap } } };
  });
}

function colecciones(transformar = (d) => d) {
  return {
    portfolioAccounts: [
      { id: 'acc-1', data: { userId: UID, isActive: true } },
      { id: 'acc-2', data: { userId: UID, isActive: true } },
      { id: 'acc-3', data: { userId: UID, isActive: true } },
    ],
    assets: [
      { id: 'a1', data: { portfolioAccount: 'acc-1', isActive: true, units: 12, name: 'A' } },
      { id: 'c1', data: { portfolioAccount: 'acc-2', isActive: true, units: 3, name: 'C' } },
    ],
    transactions: [
      // Dividendo de A: 6 unidades × 1 USD neto el jueves 2026-01-07
      { id: 'dv1', data: { portfolioAccountId: 'acc-1', type: 'dividendPay', symbol: 'A', date: '2026-01-07', amount: 6, price: 1, currency: 'USD' } },
    ],
    [`portfolioPerformance/${UID}/dates`]: transformar(DIAS),
    [`portfolioPerformance/${UID}/accounts/acc-1/dates`]: transformar(DIAS),
    [`portfolioPerformance/${UID}/accounts/acc-2/dates`]: transformar(DIAS_ACC2),
    [`portfolioPerformance/${UID}/accounts/acc-3/dates`]: [],
  };
}

const serie = (precios) => ({ dates: Object.keys(precios), closes: Object.values(precios) });
const PRECIOS = {
  A: serie({ '2026-01-05': 100, '2026-01-06': 110, '2026-01-07': 121, '2026-01-08': 121 }),
  C: serie({ '2026-01-05': 100, '2026-01-07': 110 }),
};
const SERIE_CIERRES_COP = serie({
  '2026-01-02': CIERRES['2026-01-02'], '2026-01-05': CIERRES['2026-01-05'], '2026-01-06': CIERRES['2026-01-06'],
  '2026-01-07': CIERRES['2026-01-07'], '2026-01-08': CIERRES['2026-01-08'],
});

/** Ventas de B en USD (lo que devuelve `getSellTransactionsInPeriod` en USD). */
const VENTAS_USD = {
  B_stock: { ticker: 'B', assetType: 'stock', totalRealizedPnL: 12.5, totalSold: 380, transactions: [{ date: '2026-01-07T15:20:41.053Z', pnl: 10 }, { date: '2026-01-07', pnl: 2.5 }] },
};

function deps(db, overrides = {}) {
  return {
    db,
    getPricesFromApi: async (tickers) => tickers.map((t) => ({ symbol: t, currency: 'USD' })),
    getHistoricalPrices: async () => { throw new Error('no debería pedirse'); },
    fetchSeries: jest.fn(async (simbolo) => PRECIOS[simbolo] || null),
    mapLimit: async (items, _limit, fn) => Promise.all(items.map(fn)),
    chooseHistoryRange: () => '5y',
    fetchConcurrency: 6,
    getSellTransactionsInPeriod: jest.fn(async () => VENTAS_USD),
    priceBudgetMs: 1000,
    conversorDeCierres: (moneda, fechas) => conversorDeCierres(moneda, fechas, { tasasDelPeriodo: async () => CIERRES }),
    ...overrides,
  };
}

const RANGO = { startDate: new Date('2026-01-06T00:00:00Z'), endDate: new Date('2026-01-08T00:00:00Z') };
const sinTiempos = (r) => ({ ...r, coverage: { ...r.coverage, timingsMs: undefined } });

beforeEach(() => {
  _seriesCache.clear();
  _quoteCurrencyCache.clear();
  limpiarCachesDerivados();
  jest.spyOn(console, 'log').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('HU 6.11.4 — contribución por activo en una moneda poco usada, desde USD', () => {
  it.each([
    ['portafolio (QA-01)', ['overall']],
    ['una cuenta (QA-01)', ['acc-1']],
    ['varias cuentas con fechas (QA-02)', ['acc-1', 'acc-2']],
  ])('%s: idéntica a la de siempre sobre REF-0 (USD × tasa de cierre), sin usar el bloque COP', async (_n, accountIds) => {
    const db = fakeDb(colecciones());
    const nuevo = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', currency: 'COP', accountIds, dateRange: RANGO, desdeUsd: true }, deps(db));

    _seriesCache.clear();
    _quoteCurrencyCache.clear();
    const { tasaDe } = await conversorDeCierres('COP', Object.keys(CIERRES).concat('2026-01-03'), { tasasDelPeriodo: async () => CIERRES });
    const ref0Db = fakeDb(colecciones((dias) => conBloqueRef0(dias, tasaDe)));
    const ref0 = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', currency: 'COP', accountIds, dateRange: RANGO },
      deps(ref0Db, {
        fetchSeries: async (simbolo) => (simbolo === 'COP=X' ? SERIE_CIERRES_COP : PRECIOS[simbolo] || null),
        getSellTransactionsInPeriod: async () => ({
          B_stock: { ...VENTAS_USD.B_stock, totalRealizedPnL: 10 * CIERRES['2026-01-07'] + 2.5 * CIERRES['2026-01-07'] },
        }),
      }));

    expect(sinTiempos(nuevo)).toEqual(sinTiempos(ref0));
    expect(nuevo.attributions.length).toBeGreaterThan(0);
    expect(db.usosCop).toEqual([]);
  });

  it('las cifras son USD × la tasa del día: valores, dividendo y P&G realizado (decisión del PO)', async () => {
    const db = fakeDb(colecciones());
    const d = deps(db);
    const r = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', currency: 'COP', accountIds: ['overall'], dateRange: RANGO, desdeUsd: true }, d);

    expect(r.startTotalValue).toBe(600 * CIERRES['2026-01-05'] + 400 * CIERRES['2026-01-05']);
    expect(r.totalPortfolioValue).toBe(1452 * CIERRES['2026-01-08']);
    // El dividendo pasa por `convertAmount` (÷ 1/tasa), la misma ruta que en
    // REF-0: la igualdad exacta frente a REF-0 la prueban los casos de arriba.
    expect(r.coverage.dividends).toBeCloseTo(6 * CIERRES['2026-01-07'], 8);
    expect(r.realizedPnL.total).toBe(10 * CIERRES['2026-01-07'] + 2.5 * CIERRES['2026-01-07']);
    expect(r.latestPerformanceData).toEqual({ date: '2026-01-08', COP: expect.objectContaining({ totalValue: 1452 * CIERRES['2026-01-08'] }) });
    // Las ventas se piden en USD y la serie de Yahoo de COP no se pide nunca
    expect(d.getSellTransactionsInPeriod.mock.calls[0][4]).toBe('USD');
    expect(d.fetchSeries.mock.calls.map(([s]) => s)).not.toContain('COP=X');
  });

  it('QA-03: la serie leída es la de USD y queda en la caché de USD (se comparte con la consulta en USD)', async () => {
    const db = fakeDb(colecciones());
    await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', currency: 'COP', accountIds: ['overall'], dateRange: RANGO, desdeUsd: true }, deps(db));
    expect([..._seriesCache.keys()]).toEqual([`${UID}|overall|USD`]);
    await calculateHoldingsContributions(
      { userId: UID, period: '1M', currency: 'COP', accountIds: ['overall'], dateRange: RANGO, desdeUsd: true }, deps(db));
    expect(db.usosCop).toEqual([]);
  });

  it('QA-08: sin el cierre de un día del periodo (más de 5 días naturales), el aviso; nunca lo guardado', async () => {
    const db = fakeDb(colecciones());
    const sinEnero = { '2025-12-24': 4100 };
    const error = await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', currency: 'COP', accountIds: ['overall'], dateRange: RANGO, desdeUsd: true },
      deps(db, { conversorDeCierres: (m, f) => conversorDeCierres(m, f, { tasasDelPeriodo: async () => sinEnero }) }),
    ).catch((e) => e);
    expect(error.details).toMatchObject({ code: 'CURRENCY_UNAVAILABLE', currency: 'COP' });
    expect(db.usosCop).toEqual([]);
  });

  it('sin desdeUsd (USD, moneda principal o interruptor apagado) lee el bloque de la moneda como siempre', async () => {
    const db = fakeDb(colecciones());
    const d = deps(db);
    await calculateHoldingsContributions(
      { userId: UID, period: 'YTD', currency: 'COP', accountIds: ['overall'], dateRange: RANGO }, d);
    expect(db.usosCop.length).toBeGreaterThan(0);
    expect(d.getSellTransactionsInPeriod.mock.calls[0][4]).toBe('COP');
  });
});
