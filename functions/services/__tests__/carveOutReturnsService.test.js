/**
 * Tests para carveOutReturnsService (FEAT-EXCLUDE-001)
 *
 * Firestore y la API están falseados; calculateHistoricalReturns es el real,
 * para comprobar el cableado completo hasta la forma de respuesta.
 *
 * @module __tests__/services/carveOutReturnsService.test
 */

// Algunos módulos importados tocan Firestore al cargarse; aquí no se usa.
jest.mock('../firebaseAdmin', () => {
  const stub = { collection: () => stub, doc: () => stub };
  const firestore = () => stub;
  firestore.FieldValue = { serverTimestamp: () => null };
  return { firestore };
});
jest.mock('../financeQuery', () => ({ getHistoricalPrices: jest.fn() }));
jest.mock('../marketDataHelper', () => ({ getPricesFromApi: jest.fn() }));

const {
  getCarveOutHistoricalReturns,
  chooseHistoryRange,
  _historyCache,
} = require('../carveOutReturnsService');
const { buildExcludeSet } = require('../../utils/carveOutReturns');

// ============================================================================
// Fixtures
// ============================================================================

const ap = (values) => Object.fromEntries(Object.entries(values).map(([k, v]) => [k, { totalValue: v }]));

const DAYS = [
  { date: '2026-01-05', USD: { assetPerformance: ap({ 'VUAA.L_etf': 500, MSFT_stock: 300, 'MC.PA_stock': 200 }) } },
  { date: '2026-01-06', USD: { assetPerformance: ap({ 'VUAA.L_etf': 505, MSFT_stock: 306, 'MC.PA_stock': 204 }) } },
  {
    date: '2026-01-07',
    USD: {
      assetPerformance: {
        ...ap({ 'VUAA.L_etf': 507, MSFT_stock: 303, 'MC.PA_stock': 206 }),
        VUAA: { L_etf: { doneProfitAndLoss: 0 } }, // anidada por un "." en la ruta
      },
    },
  },
  { date: '2026-01-08', USD: { assetPerformance: ap({ 'VUAA.L_etf': 510, MSFT_stock: 312, 'MC.PA_stock': 199 }) } },
];

const HISTORY = {
  MSFT: { '2026-01-05': 100, '2026-01-06': 102, '2026-01-07': 101, '2026-01-08': 104 },
  'MC.PA': { '2026-01-05': 50, '2026-01-06': 50.5, '2026-01-07': 51, '2026-01-08': 50 },
  'EUR=X': { '2026-01-05': 0.92, '2026-01-06': 0.91, '2026-01-07': 0.91, '2026-01-08': 0.93 },
};

const DIVIDENDS = [
  { portfolioAccountId: 'acc-1', type: 'dividendPay', symbol: 'MSFT', date: '2026-01-07', amount: 5, price: 0.5, currency: 'USD' },
  { portfolioAccountId: 'acc-2', type: 'dividendPay', symbol: 'MSFT', date: '2026-01-07', amount: 100, price: 0.5, currency: 'USD' },
  { portfolioAccountId: 'acc-1', type: 'dividendPay', symbol: 'VUAA.L', date: '2026-01-07', amount: 40, price: 1, currency: 'USD' },
];

function fakeDb({ days = DAYS, accounts = ['acc-1', 'acc-2'], dividends = DIVIDENDS } = {}) {
  const paths = [];
  return {
    paths,
    collection: (path) => {
      paths.push(path);
      if (path.startsWith('portfolioPerformance/')) {
        return {
          orderBy: () => ({
            get: async () => ({ docs: days.map((d) => ({ id: d.date, data: () => d })) }),
          }),
        };
      }
      if (path === 'portfolioAccounts') {
        return { where: () => ({ get: async () => ({ docs: accounts.map((id) => ({ id })) }) }) };
      }
      if (path === 'transactions') {
        return {
          where: (field, op, ids) => ({
            where: () => ({
              get: async () => ({
                docs: dividends.filter((t) => ids.includes(t.portfolioAccountId)).map((t) => ({ data: () => t })),
              }),
            }),
          }),
        };
      }
      throw new Error(`colección inesperada ${path}`);
    },
  };
}

const toHistorical = (map) => Object.fromEntries(Object.entries(map).map(([d, close]) => [d, { close }]));

function deps({ quotes = [{ symbol: 'MSFT', currency: 'USD' }, { symbol: 'MC.PA', currency: 'EUR' }] } = {}) {
  return {
    getPricesFromApi: jest.fn(async () => quotes),
    getHistoricalPrices: jest.fn(async (symbol) => {
      if (!HISTORY[symbol]) throw new Error('404');
      return toHistorical(HISTORY[symbol]);
    }),
  };
}

const pct = (a, b) => (b / a - 1) * 100;
const eur = (d0, d1) => ((HISTORY['MC.PA'][d1] / HISTORY['MC.PA'][d0]) * (HISTORY['EUR=X'][d0] / HISTORY['EUR=X'][d1]) - 1) * 100;
const msft = (d0, d1) => pct(HISTORY.MSFT[d0], HISTORY.MSFT[d1]);

// ============================================================================
// Tests
// ============================================================================

describe('carveOutReturnsService', () => {
  beforeEach(() => _historyCache.clear());

  it('calcula el sub-portafolio sin VUAA.L con FX y dividendos de todas las cuentas', async () => {
    const d = deps();
    const result = await getCarveOutHistoricalReturns(
      { db: fakeDb(), userId: 'u1', currency: 'USD', accountId: 'overall', excludeSet: buildExcludeSet(['VUAA.L']) },
      d
    );

    const expected = [
      0,
      (300 * msft('2026-01-05', '2026-01-06') + 200 * eur('2026-01-05', '2026-01-06')) / 500,
      (306 * msft('2026-01-06', '2026-01-07') + 204 * eur('2026-01-06', '2026-01-07') + 100 * (2.5 + 50)) / 510,
      (303 * msft('2026-01-07', '2026-01-08') + 206 * eur('2026-01-07', '2026-01-08')) / 509,
    ];
    expect(result.totalValueData.dates).toEqual(DAYS.map((x) => x.date));
    result.totalValueData.percentChanges.forEach((r, i) => expect(r).toBeCloseTo(expected[i], 10));
    expect(result.totalValueData.values).toEqual([500, 510, 509, 511]);

    const range = chooseHistoryRange('2026-01-05');
    const requested = d.getHistoricalPrices.mock.calls.map((c) => c[0]).sort();
    expect(requested).toEqual(['EUR=X', 'MC.PA', 'MSFT']);
    d.getHistoricalPrices.mock.calls.forEach((c) => expect(c[1]).toBe(range));
    expect(d.getPricesFromApi).toHaveBeenCalledWith(['MC.PA', 'MSFT']);

    expect(Object.keys(result.returns).some((k) => /Personal/.test(k))).toBe(false);
    expect(result.carveOut).toMatchObject({
      method: 'holdings-twr',
      dividends: 'registered-net',
      excludedTickers: ['VUAA.L'],
      excludedTickersNotFound: [],
      historyRange: range,
      coverage: {
        totalDays: 4,
        usableDays: 4,
        dividendsIncluded: 52.5,
        missingPriceTickers: [],
        tickersWithoutQuote: [],
        currenciesWithoutFx: [],
      },
    });
  });

  it('una cuenta concreta lee su ruta y solo sus dividendos', async () => {
    const db = fakeDb();
    const result = await getCarveOutHistoricalReturns(
      { db, userId: 'u1', currency: 'USD', accountId: 'acc-1', excludeSet: buildExcludeSet(['VUAA.L']) },
      deps()
    );
    expect(db.paths).toContain('portfolioPerformance/u1/accounts/acc-1/dates');
    expect(result.carveOut.coverage.dividendsIncluded).toBe(2.5);
  });

  it('una cuenta ajena no aporta dividendos', async () => {
    const result = await getCarveOutHistoricalReturns(
      { db: fakeDb({ accounts: ['acc-1'] }), userId: 'u1', currency: 'USD', accountId: 'acc-x', excludeSet: buildExcludeSet(['VUAA.L']) },
      deps()
    );
    expect(result.carveOut.coverage.dividendsIncluded).toBe(0);
  });

  it('un ticker sin quote deja sus días sin calcular y lo reporta', async () => {
    const result = await getCarveOutHistoricalReturns(
      { db: fakeDb(), userId: 'u1', currency: 'USD', accountId: 'overall', excludeSet: buildExcludeSet(['VUAA.L']) },
      deps({ quotes: [{ symbol: 'MSFT', currency: 'USD' }] })
    );
    expect(result.carveOut.coverage).toMatchObject({
      usableDays: 1,
      tickersWithoutQuote: ['MC.PA'],
      missingPriceTickers: ['MC.PA'],
      unusableByStatus: { incomplete: 3 },
    });
  });

  it('reporta exclusiones que no existen en el historial', async () => {
    const result = await getCarveOutHistoricalReturns(
      { db: fakeDb(), userId: 'u1', currency: 'USD', accountId: 'overall', excludeSet: buildExcludeSet(['VUAA', 'VUAA.L']) },
      deps()
    );
    expect(result.carveOut.excludedTickersNotFound).toEqual(['VUAA']);
  });

  it('historial vacío', async () => {
    const d = deps();
    const result = await getCarveOutHistoricalReturns(
      { db: fakeDb({ days: [] }), userId: 'u1', currency: 'USD', accountId: 'overall', excludeSet: buildExcludeSet(['VOO']) },
      d
    );
    expect(result.carveOut.coverage).toEqual({ totalDays: 0, usableDays: 0 });
    expect(d.getHistoricalPrices).not.toHaveBeenCalled();
  });

  it('reutiliza las series en cache entre consultas', async () => {
    const d = deps();
    const params = { db: fakeDb(), userId: 'u1', currency: 'USD', accountId: 'overall', excludeSet: buildExcludeSet(['VUAA.L']) };
    await getCarveOutHistoricalReturns(params, d);
    await getCarveOutHistoricalReturns(params, d);
    expect(d.getHistoricalPrices).toHaveBeenCalledTimes(3);
  });

  describe('chooseHistoryRange', () => {
    const now = new Date('2026-10-08T12:00:00Z');
    it.each([
      ['2026-01-05', '1y'],
      ['2025-10-01', '2y'],
      ['2024-08-16', '5y'],
      ['2020-01-01', '10y'],
    ])('%s -> %s', (first, range) => {
      expect(chooseHistoryRange(first, now)).toBe(range);
    });
  });
});
