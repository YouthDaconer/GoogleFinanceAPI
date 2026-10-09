/**
 * Tests para carveOutReturns.js (FEAT-EXCLUDE-001) — TWR por tenencias.
 *
 * El oráculo es calculateAccountPerformance, la misma función que usa el EOD
 * para escribir portfolioPerformance. Cada escenario genera días con ella y
 * comprueba que el TWR por tenencias, alimentado con los MISMOS precios:
 *  - sin excluir nada, da el adjustedDailyChangePercentage oficial;
 *  - excluyendo B de {A, B, C}, da lo mismo que un portafolio real {A, C};
 *  - donde difiere (operaciones a precio ≠ cierre, registro retroactivo),
 *    difiere exactamente en la cantidad que predice la teoría.
 *
 * @module __tests__/utils/carveOutReturns.test
 */

const { calculateAccountPerformance } = require('../../utils/portfolioCalculations');
const {
  MAX_EXCLUDED_TICKERS,
  MAX_PRICE_STALE_DAYS,
  CARVE_OUT_STATUS,
  tickerFromAssetKey,
  buildExcludeSet,
  isExcluded,
  extractAssetValues,
  collectHeldTickers,
  findUnmatchedExclusions,
  toPriceSeries,
  closeAsOf,
  normalizeCurrency,
  fxSymbol,
  convertAmount,
  marketReturnPct,
  dividendNetCash,
  groupDividends,
  dividendsBetween,
  computeCarveOutDay,
  buildCarveOutSeries,
  toPerformanceDocs,
} = require('../../utils/carveOutReturns');

// ============================================================================
// Builders + oráculo
// ============================================================================

const USD = { code: 'USD', exchangeRate: 1 };

const createAsset = (overrides = {}) => ({
  id: 'asset-a',
  name: 'AAA',
  assetType: 'stock',
  units: 10,
  unitValue: 100,
  currency: 'USD',
  defaultCurrencyForAdquisitionDollar: 'USD',
  acquisitionDollarValue: 1,
  acquisitionDate: '2024-01-02',
  portfolioAccount: 'acc-1',
  isActive: true,
  ...overrides,
});

const createTx = (overrides = {}) => ({
  type: 'buy',
  assetId: 'asset-a',
  amount: 1,
  price: 100,
  currency: 'USD',
  defaultCurrencyForAdquisitionDollar: 'USD',
  dollarPriceToDate: 1,
  ...overrides,
});

const toQuotes = (map) => Object.entries(map).map(([symbol, price]) => ({ symbol, price, currency: 'USD' }));

/** Replica cómo calculateDailyPortfolioPerformance arma el "ayer" desde el doc previo. */
function toYesterday(perf) {
  if (!perf) return { USD: { totalValue: 0 } };
  const out = {};
  for (const [code, data] of Object.entries(perf)) {
    out[code] = { totalValue: data.totalValue || 0 };
    for (const [key, a] of Object.entries(data.assetPerformance || {})) {
      out[code][key] = { totalValue: a.totalValue || 0, units: a.units || 0 };
    }
  }
  return out;
}

/**
 * Corre una secuencia de días con el oráculo.
 * @param {Array<{assets: Object[], prices: Object<string, number>, txs?: Object[]}>} steps
 */
function runOracle(steps) {
  const docs = [];
  let prev = null;
  for (const step of steps) {
    const perf = calculateAccountPerformance(step.assets, toQuotes(step.prices), [USD], toYesterday(prev), step.txs || []);
    docs.push(perf);
    prev = perf;
  }
  return docs;
}

/** Retorno de mercado tomado de los mismos precios del escenario. */
const marketFrom = (steps, i) => (ticker) => {
  const p0 = steps[i - 1].prices[ticker];
  const p1 = steps[i].prices[ticker];
  return p0 && p1 ? (p1 / p0 - 1) * 100 : null;
};

const NONE = new Set();

/** TWR por tenencias del día i (i ≥ 1). */
function holdings(steps, docs, i, excludeSet = NONE, dividendCash) {
  return computeCarveOutDay(
    extractAssetValues(docs[i - 1].USD.assetPerformance),
    docs[i].USD.assetPerformance,
    excludeSet,
    { marketReturn: marketFrom(steps, i), dividendCash }
  );
}

const A0 = createAsset({ id: 'a', name: 'AAA', units: 10, unitValue: 100 });
const B0 = createAsset({ id: 'b', name: 'BBB', assetType: 'etf', units: 5, unitValue: 400 });
const C0 = createAsset({ id: 'c', name: 'CCC', units: 20, unitValue: 50 });
const P0 = { AAA: 100, BBB: 400, CCC: 50 };
const P1 = { AAA: 103, BBB: 396, CCC: 51.5 };

// ============================================================================
// Tests
// ============================================================================

describe('carveOutReturns', () => {
  // ==========================================================================
  // Sin excluir nada: debe reproducir el dato oficial
  // ==========================================================================

  describe('E = ∅ reproduce adjustedDailyChangePercentage del portafolio', () => {
    it('solo movimientos de precio en tres activos', () => {
      const steps = [{ assets: [A0, B0, C0], prices: P0 }, { assets: [A0, B0, C0], prices: P1 }];
      const docs = runOracle(steps);
      const day = holdings(steps, docs, 1);
      expect(day.status).toBe(CARVE_OUT_STATUS.OK);
      expect(day.adjustedDailyChangePercentage).toBeCloseTo(docs[1].USD.adjustedDailyChangePercentage, 10);
      expect(day.totalValue).toBeCloseTo(docs[1].USD.totalValue, 8);
    });

    it('venta parcial al precio de cierre', () => {
      const steps = [
        { assets: [A0, B0, C0], prices: P0 },
        {
          assets: [A0, { ...B0, units: 2 }, C0],
          prices: P1,
          txs: [createTx({ type: 'sell', assetId: 'b', amount: 3, price: P1.BBB })],
        },
      ];
      const docs = runOracle(steps);
      expect(holdings(steps, docs, 1).adjustedDailyChangePercentage)
        .toBeCloseTo(docs[1].USD.adjustedDailyChangePercentage, 10);
    });

    it('venta total al precio de cierre', () => {
      const steps = [
        { assets: [A0, B0, C0], prices: P0 },
        {
          assets: [A0, { ...B0, units: 0, isActive: false }, C0],
          prices: P1,
          txs: [createTx({ type: 'sell', assetId: 'b', amount: 5, price: P1.BBB })],
        },
      ];
      const docs = runOracle(steps);
      expect(holdings(steps, docs, 1).adjustedDailyChangePercentage)
        .toBeCloseTo(docs[1].USD.adjustedDailyChangePercentage, 10);
    });

    it('dividendo registrado', () => {
      const steps = [
        { assets: [A0, B0, C0], prices: P0 },
        {
          assets: [A0, B0, C0],
          prices: P1,
          txs: [createTx({ type: 'dividendPay', assetId: 'c', amount: 12.5, price: 0 })],
        },
      ];
      const docs = runOracle(steps);
      const day = holdings(steps, docs, 1, NONE, (t) => (t === 'CCC' ? 12.5 : 0));
      expect(day.adjustedDailyChangePercentage).toBeCloseTo(docs[1].USD.adjustedDailyChangePercentage, 10);
      expect(day.dividends).toBe(12.5);
    });

    it('posición nueva comprada al precio de cierre', () => {
      const D = createAsset({ id: 'd', name: 'DDD', units: 3, unitValue: 80 });
      const steps = [
        { assets: [A0, B0, C0], prices: P0 },
        {
          assets: [A0, B0, C0, D],
          prices: { ...P1, DDD: 80 },
          txs: [createTx({ type: 'buy', assetId: 'd', amount: 3, price: 80 })],
        },
      ];
      const docs = runOracle(steps);
      expect(holdings(steps, docs, 1).adjustedDailyChangePercentage)
        .toBeCloseTo(docs[1].USD.adjustedDailyChangePercentage, 10);
    });
  });

  // ==========================================================================
  // Diferencias conocidas: el oficial captura P&L intradía y registros
  // retroactivos; las tenencias se valoran a cierre
  // ==========================================================================

  describe('diferencias conocidas frente al oficial', () => {
    it('compra y venta a precio ≠ cierre: difiere exactamente en el P&L intradía', () => {
      const steps = [
        { assets: [A0, B0, C0], prices: P0 },
        {
          assets: [{ ...A0, units: 14 }, { ...B0, units: 2 }, C0],
          prices: P1,
          txs: [
            createTx({ type: 'buy', assetId: 'a', amount: 4, price: 101.2 }),
            createTx({ type: 'sell', assetId: 'b', amount: 3, price: 398 }),
          ],
        },
      ];
      const docs = runOracle(steps);
      const intraday = 4 * (P1.AAA - 101.2) + 3 * (398 - P1.BBB);
      expect(docs[1].USD.adjustedDailyChangePercentage - holdings(steps, docs, 1).adjustedDailyChangePercentage)
        .toBeCloseTo((intraday / docs[0].USD.totalValue) * 100, 10);
    });

    it('registro retroactivo (activo nuevo sin transacción): el oficial suma valor − inversión', () => {
      const D = createAsset({ id: 'd', name: 'DDD', units: 3, unitValue: 80 });
      const steps = [
        { assets: [A0, B0, C0], prices: P0 },
        { assets: [A0, B0, C0, D], prices: { ...P1, DDD: 84 } },
      ];
      const docs = runOracle(steps);
      expect(docs[1].USD.adjustedDailyChangePercentage - holdings(steps, docs, 1).adjustedDailyChangePercentage)
        .toBeCloseTo(((3 * 84 - 3 * 80) / docs[0].USD.totalValue) * 100, 10);
    });

    it('unidades que cambian sin transacción no contaminan el retorno del día', () => {
      // Registro retroactivo de una compra adicional de AAA: el valor salta,
      // pero el retorno por tenencias sigue siendo el del mercado.
      const steps = [
        { assets: [A0, B0, C0], prices: P0 },
        { assets: [{ ...A0, units: 30 }, B0, C0], prices: P1 },
      ];
      const docs = runOracle(steps);
      const w = (v) => v / docs[0].USD.totalValue;
      const expected = w(1000) * 3 + w(2000) * -1 + w(1000) * 3;
      expect(holdings(steps, docs, 1).adjustedDailyChangePercentage).toBeCloseTo(expected, 10);
      expect(docs[1].USD.adjustedDailyChangePercentage).toBeGreaterThan(expected + 40);
    });
  });

  // ==========================================================================
  // Exclusión: {A, B, C} sin B debe ser igual a un portafolio real {A, C}
  // ==========================================================================

  describe('excluir B equivale a un portafolio que nunca tuvo B', () => {
    const P2 = { AAA: 99, BBB: 410, CCC: 52 };
    const P3 = { AAA: 101, BBB: 380, CCC: 53.3 };
    const steps = (withB) => {
      const keep = (list) => list.filter((a) => withB || a.id !== 'b');
      const keepTx = (list) => list.filter((t) => withB || t.assetId !== 'b');
      return [
        { assets: keep([A0, B0, C0]), prices: P0 },
        {
          assets: keep([{ ...A0, units: 7 }, { ...B0, units: 6 }, C0]),
          prices: P1,
          txs: keepTx([
            createTx({ type: 'sell', assetId: 'a', amount: 3, price: P1.AAA }),
            createTx({ type: 'buy', assetId: 'b', amount: 1, price: P1.BBB }),
          ]),
        },
        {
          assets: keep([{ ...A0, units: 7 }, { ...B0, units: 6 }, { ...C0, units: 25 }]),
          prices: P2,
          txs: keepTx([
            createTx({ type: 'buy', assetId: 'c', amount: 5, price: P2.CCC }),
            createTx({ type: 'dividendPay', assetId: 'c', amount: 9, price: 0 }),
            createTx({ type: 'dividendPay', assetId: 'b', amount: 30, price: 0 }),
          ]),
        },
        { assets: keep([{ ...A0, units: 7 }, { ...B0, units: 6 }, { ...C0, units: 25 }]), prices: P3 },
      ];
    };

    const fullSteps = steps(true);
    const full = runOracle(fullSteps);
    const withoutB = runOracle(steps(false));
    const excludeB = buildExcludeSet(['bbb']);
    const divs = { 2: { CCC: 9, BBB: 30 } };
    const cash = (i) => (t) => divs[i]?.[t] || 0;

    it.each([1, 2, 3])('día %i', (i) => {
      const day = holdings(fullSteps, full, i, excludeB, cash(i));
      expect(day.status).toBe(CARVE_OUT_STATUS.OK);
      expect(day.adjustedDailyChangePercentage).toBeCloseTo(withoutB[i].USD.adjustedDailyChangePercentage, 10);
      expect(day.totalValue).toBeCloseTo(withoutB[i].USD.totalValue, 8);
    });

    it('el dividendo del excluido no se filtra al sub-portafolio', () => {
      expect(holdings(fullSteps, full, 2, excludeB, cash(2)).dividends).toBe(9);
    });

    it('TWR encadenado vía buildCarveOutSeries', () => {
      const dates = ['2026-01-05', '2026-01-06', '2026-01-07', '2026-01-08'];
      const series = buildCarveOutSeries(
        full.map((perf, i) => ({ date: dates[i], currencyData: perf.USD })),
        excludeB,
        {
          marketReturn: (t, d0, d1) => marketFrom(fullSteps, dates.indexOf(d1))(t),
          dividendCash: (t, d0, d1) => cash(dates.indexOf(d1))(t),
        }
      );
      const chain = (rs) => rs.reduce((f, r) => f * (1 + r / 100), 1);
      expect(chain(series.days.map((d) => d.day.adjustedDailyChangePercentage)))
        .toBeCloseTo(chain(withoutB.map((perf) => perf.USD.adjustedDailyChangePercentage)), 12);
      expect(series.coverage.usableDays).toBe(4);
      expect(series.coverage.dividendsIncluded).toBe(9);
    });

    it('la exclusión sí cambia el resultado', () => {
      const excluded = holdings(fullSteps, full, 3, excludeB).adjustedDailyChangePercentage;
      expect(Math.abs(excluded - full[3].USD.adjustedDailyChangePercentage)).toBeGreaterThan(0.1);
    });
  });

  // ==========================================================================
  // Datos sucios del histórico
  // ==========================================================================

  describe('datos sucios del histórico', () => {
    const ap = (entries) => Object.fromEntries(entries.map(([k, v]) => [k, { totalValue: v }]));
    const market = (map) => ({ marketReturn: (t) => (t in map ? map[t] : null) });

    it('ignora entradas anidadas por un "." en la ruta (VUAA -> {L_etf})', () => {
      const prev = { ...ap([['VUAA.L_etf', 100], ['MSFT_stock', 100]]), VUAA: { L_etf: { totalValue: 999 } } };
      expect(extractAssetValues(prev)).toEqual({ 'VUAA.L_etf': 100, MSFT_stock: 100 });
      const day = computeCarveOutDay(extractAssetValues(prev), prev, NONE, market({ 'VUAA.L': 1, MSFT: 3 }));
      expect(day.adjustedDailyChangePercentage).toBeCloseTo(2, 12);
      expect(day.totalValue).toBe(200);
    });

    it('suma el peso de llaves distintas del mismo ticker (VUAA.L_etf y VUAA.L_undefined)', () => {
      const prev = ap([['VUAA.L_etf', 100], ['VUAA.L_undefined', 100], ['MSFT_stock', 200]]);
      const day = computeCarveOutDay(extractAssetValues(prev), prev, NONE, market({ 'VUAA.L': 1, MSFT: 3 }));
      expect(day.adjustedDailyChangePercentage).toBeCloseTo(2, 12);
    });

    it('activo con peso sin precio => incompleto, no re-pondera', () => {
      const prev = ap([['XXX_stock', 100], ['YYY_stock', 300]]);
      const day = computeCarveOutDay(extractAssetValues(prev), prev, NONE, market({ XXX: 10 }));
      expect(day.status).toBe(CARVE_OUT_STATUS.INCOMPLETE);
      expect(day.adjustedDailyChangePercentage).toBeNull();
      expect(day.missingValue).toBe(300);
      expect(day.missingTickers).toEqual(['YYY']);
    });

    it('si el único activo sin precio está excluido, el día sí se calcula', () => {
      const prev = ap([['XXX_stock', 100], ['VUAA.L_undefined', 300]]);
      const day = computeCarveOutDay(extractAssetValues(prev), prev, buildExcludeSet(['VUAA.L']), market({ XXX: 10 }));
      expect(day.status).toBe(CARVE_OUT_STATUS.OK);
      expect(day.adjustedDailyChangePercentage).toBeCloseTo(10, 12);
    });

    it('sub-portafolio sin valor ayer => START con 0%', () => {
      const day = computeCarveOutDay({}, ap([['XXX_stock', 110]]), NONE, market({}));
      expect(day.status).toBe(CARVE_OUT_STATUS.START);
      expect(day.adjustedDailyChangePercentage).toBe(0);
    });

    it('cobertura de una serie con huecos', () => {
      const days = [
        { date: '2026-01-01', currencyData: { assetPerformance: ap([['XXX_stock', 100]]) } },
        { date: '2026-01-02', currencyData: { assetPerformance: ap([['XXX_stock', 101]]) } },
        { date: '2026-01-03', currencyData: { totalValue: 102 } },
        { date: '2026-01-04', currencyData: { assetPerformance: ap([['XXX_stock', 103]]) } },
        { date: '2026-01-05', currencyData: { assetPerformance: ap([['XXX_stock', 104], ['ZZZ_stock', 5]]) } },
        { date: '2026-01-06', currencyData: { assetPerformance: ap([['XXX_stock', 104], ['ZZZ_stock', 5]]) } },
      ];
      const series = buildCarveOutSeries(days, NONE, { marketReturn: (t) => (t === 'XXX' ? 1 : null) });
      expect(series.days.map((d) => d.day.status)).toEqual([
        CARVE_OUT_STATUS.START,
        CARVE_OUT_STATUS.OK,
        CARVE_OUT_STATUS.NO_ASSET_DATA,
        CARVE_OUT_STATUS.NO_PRIOR_ASSET_DATA,
        CARVE_OUT_STATUS.OK,
        CARVE_OUT_STATUS.INCOMPLETE,
      ]);
      expect(series.coverage).toMatchObject({
        totalDays: 6,
        usableDays: 3,
        firstUsableDate: '2026-01-01',
        lastUsableDate: '2026-01-05',
        unusableByStatus: {
          [CARVE_OUT_STATUS.NO_ASSET_DATA]: 1,
          [CARVE_OUT_STATUS.NO_PRIOR_ASSET_DATA]: 1,
          [CARVE_OUT_STATUS.INCOMPLETE]: 1,
        },
        missingPriceTickers: ['ZZZ'],
      });
    });

    it('toPerformanceDocs omite los días no calculables e iguala el cambio diario al ajustado', () => {
      const series = {
        days: [
          { date: '2026-01-01', day: { adjustedDailyChangePercentage: 0, totalValue: 10 } },
          { date: '2026-01-02', day: { adjustedDailyChangePercentage: null, totalValue: 11 } },
          { date: '2026-01-03', day: { adjustedDailyChangePercentage: 2.5, totalValue: 12 } },
        ],
      };
      const docs = toPerformanceDocs(series, 'COP');
      expect(docs.map((d) => d.date)).toEqual(['2026-01-01', '2026-01-03']);
      expect(docs[1].COP).toMatchObject({ adjustedDailyChangePercentage: 2.5, dailyChangePercentage: 2.5, totalValue: 12 });
    });
  });

  // ==========================================================================
  // Precios y tasas
  // ==========================================================================

  describe('precios y tasas de cambio', () => {
    const series = toPriceSeries({
      '2026-01-07': { close: 103 },
      '2026-01-05': { close: 100 },
      '2026-01-06 00:00:00': { close: 101 },
      '2026-01-08': { close: 0 },
      '2026-01-09': { close: null },
    });

    it('toPriceSeries ordena, recorta la hora y descarta cierres inválidos', () => {
      expect(series).toEqual({ dates: ['2026-01-05', '2026-01-06', '2026-01-07'], closes: [100, 101, 103] });
    });

    it('closeAsOf toma el último cierre <= fecha y descarta los demasiado viejos', () => {
      expect(closeAsOf(series, '2026-01-06')).toBe(101);
      expect(closeAsOf(series, '2026-01-10')).toBe(103); // fin de semana
      expect(closeAsOf(series, '2026-01-04')).toBeNull();
      const stale = new Date(Date.parse('2026-01-07T00:00:00Z') + (MAX_PRICE_STALE_DAYS + 1) * 86400000)
        .toISOString().slice(0, 10);
      expect(closeAsOf(series, stale)).toBeNull();
    });

    it('normaliza monedas y arma el símbolo FX de la API', () => {
      expect(normalizeCurrency('GBp')).toBe('GBP');
      expect(normalizeCurrency(undefined)).toBe('USD');
      expect(fxSymbol('cop')).toBe('COP=X');
      expect(fxSymbol('USD')).toBeNull();
    });

    const fx = {
      COP: toPriceSeries({ '2026-01-05': { close: 4000 }, '2026-01-06': { close: 4100 } }),
      EUR: toPriceSeries({ '2026-01-05': { close: 0.92 }, '2026-01-06': { close: 0.9 } }),
    };
    const px = toPriceSeries({ '2026-01-05': { close: 100 }, '2026-01-06': { close: 102 } });

    it('activo en USD reportado en COP incorpora la variación cambiaria', () => {
      const r = marketReturnPct({ priceSeries: px, quoteCurrency: 'USD', reportCurrency: 'COP', fxSeriesByCurrency: fx },
        '2026-01-05', '2026-01-06');
      expect(r).toBeCloseTo((1.02 * (4100 / 4000) - 1) * 100, 10);
    });

    it('activo en EUR reportado en USD', () => {
      const r = marketReturnPct({ priceSeries: px, quoteCurrency: 'EUR', reportCurrency: 'USD', fxSeriesByCurrency: fx },
        '2026-01-05', '2026-01-06');
      expect(r).toBeCloseTo((1.02 * (0.92 / 0.9) - 1) * 100, 10);
    });

    it('misma moneda: solo precio; sin serie FX: null', () => {
      expect(marketReturnPct({ priceSeries: px, quoteCurrency: 'USD', reportCurrency: 'USD', fxSeriesByCurrency: {} },
        '2026-01-05', '2026-01-06')).toBeCloseTo(2, 12);
      expect(marketReturnPct({ priceSeries: px, quoteCurrency: 'GBP', reportCurrency: 'USD', fxSeriesByCurrency: {} },
        '2026-01-05', '2026-01-06')).toBeNull();
    });

    it('convertAmount convierte entre dos monedas no-USD vía USD', () => {
      expect(convertAmount(92, 'EUR', 'COP', fx, '2026-01-05')).toBeCloseTo((92 / 0.92) * 4000, 8);
    });
  });

  // ==========================================================================
  // Dividendos registrados
  // ==========================================================================

  describe('dividendos registrados', () => {
    // dividendPay: amount = UNIDADES, price = dividendo por unidad neto
    const fx = { EUR: toPriceSeries({ '2026-01-05': { close: 0.9 } }) };
    const txs = [
      { symbol: 'msft', date: '2026-01-05T10:00:00', amount: 10, price: 0.85, currency: 'USD' },
      { symbol: 'MC.PA', date: '2026-01-05', amount: 3, price: 3, currency: 'EUR' },
      { symbol: 'XYZ', date: '2026-01-05', amount: 4, price: 1, currency: 'GBP' },
      { symbol: 'MSFT', date: '2026-01-07', amount: 2, price: 0.5, currency: 'USD' },
      { date: '2026-01-05', amount: 3, price: 1, currency: 'USD' },
    ];
    const { byTicker, unconverted } = groupDividends(txs, { reportCurrency: 'USD', fxSeriesByCurrency: fx });

    it('dividendNetCash: unidades × precio neto, o bruto − retención (registro real de MSFT)', () => {
      expect(dividendNetCash({ amount: 2.5, price: 0.01 })).toBeCloseTo(0.025, 12);
      const msft = { amount: 0.985, price: 0.637, grossAmount: 0.89635, taxDeductionAmount: 0.268905 };
      expect(dividendNetCash(msft)).toBeCloseTo(0.985 * 0.637, 10);
    });

    it('asocia por symbol, convierte moneda y cuenta los no convertibles', () => {
      expect(byTicker.get('MSFT')).toEqual([{ date: '2026-01-05', amount: 8.5 }, { date: '2026-01-07', amount: 1 }]);
      expect(byTicker.get('MC.PA')[0].amount).toBeCloseTo(10, 10);
      expect(byTicker.has('XYZ')).toBe(false);
      expect(unconverted).toBe(1);
    });

    it('dividendsBetween usa el intervalo (d0, d1]', () => {
      expect(dividendsBetween(byTicker, 'MSFT', '2026-01-04', '2026-01-05')).toBe(8.5);
      expect(dividendsBetween(byTicker, 'MSFT', '2026-01-05', '2026-01-07')).toBe(1);
      expect(dividendsBetween(byTicker, 'NADA', '2026-01-01', '2026-12-31')).toBe(0);
    });
  });

  // ==========================================================================
  // Tickers y exclusión
  // ==========================================================================

  describe('tickers y exclusión', () => {
    it.each([
      ['VUAA.L_undefined', 'VUAA.L'],
      ['VUAA.L_etf', 'VUAA.L'],
      ['ECOPETROL.CL_stock', 'ECOPETROL.CL'],
      ['BRK_B_stock', 'BRK_B'],
      ['META', 'META'],
    ])('%s -> %s', (key, ticker) => {
      expect(tickerFromAssetKey(key)).toBe(ticker);
    });

    it('excluye sin importar mayúsculas ni el sufijo de assetType', () => {
      const set = buildExcludeSet([' vuaa.l ']);
      expect(isExcluded('VUAA.L_undefined', set)).toBe(true);
      expect(isExcluded('VUAA.L_etf', set)).toBe(true);
    });

    it('no excluye tickers que solo comparten prefijo', () => {
      const set = buildExcludeSet(['VUAA.L', 'SPY']);
      expect(isExcluded('VUAA_stock', set)).toBe(false);
      expect(isExcluded('SPYG_etf', set)).toBe(false);
    });

    const days = [
      { date: '2026-01-01', currencyData: { assetPerformance: { 'VUAA.L_etf': { totalValue: 5 }, MSFT_stock: { totalValue: 0 } } } },
      { date: '2026-01-02', currencyData: { assetPerformance: { NVDA_stock: { totalValue: 3 }, VUAA: { L_etf: {} } } } },
    ];

    it('collectHeldTickers: solo tickers con valor, sin excluidos ni anidados', () => {
      expect(collectHeldTickers(days, buildExcludeSet(['VUAA.L']))).toEqual(['NVDA']);
    });

    it('findUnmatchedExclusions detecta tickers que no existen en el historial', () => {
      expect(findUnmatchedExclusions(days, buildExcludeSet(['VUAA.L', 'VUAA', 'voo']))).toEqual(['VOO', 'VUAA']);
    });

    describe('buildExcludeSet', () => {
      it('null/undefined => conjunto vacío', () => {
        expect(buildExcludeSet(undefined).size).toBe(0);
        expect(buildExcludeSet(null).size).toBe(0);
      });

      it('deduplica tras normalizar', () => {
        expect([...buildExcludeSet(['voo', 'VOO', ' Voo'])]).toEqual(['VOO']);
      });

      it.each([['VOO'], [['']], [[42]], [[null]]])('rechaza entrada inválida %p', (input) => {
        expect(() => buildExcludeSet(input)).toThrow();
      });

      it(`rechaza más de ${MAX_EXCLUDED_TICKERS} tickers`, () => {
        const many = Array.from({ length: MAX_EXCLUDED_TICKERS + 1 }, (_, i) => `T${i}`);
        expect(() => buildExcludeSet(many)).toThrow();
      });
    });
  });
});
