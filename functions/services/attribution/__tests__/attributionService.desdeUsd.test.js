/**
 * HU 6.11.4 (T9c) — getPortfolioAttribution en una moneda poco usada: ningún
 * camino lee lo guardado en esa moneda (RN-6.11-J).
 *
 * Caminos (historia.md, Descripción):
 * - contribución por activo → `calculateHoldingsContributions` con `desdeUsd` (QA-01, QA-03);
 * - varias cuentas con fechas → días en USD × tasa (QA-02);
 * - respaldo por periodo → días en USD × tasa, sin generar resúmenes (QA-04);
 * - día en curso → intradía en USD con la tasa vigente (QA-11);
 * - legacy → aviso, nunca lo guardado (RN-6.11-K).
 *
 * Sin con qué calcular → `currencyUnavailable` (QA-09). USD y la moneda
 * principal sin cambios (QA-12). Interruptor apagado → como antes (QA-13).
 *
 * Firestore está falseado en memoria y registra cada uso de lo guardado en la
 * moneda: el bloque `COP`/`EUR` de un día o un resumen `_COP`/`_EUR` (QA-15).
 *
 * @see platform-docs/stories/6.11.4-atribucion-desde-usd/refinamiento.md (T9c)
 */

// ============================================================================
// Firestore en memoria con traza de MED-MON
// ============================================================================

const mockEstado = { colecciones: {}, documentos: {}, medMon: [], lecturasResumen: [] };

jest.mock('../../firebaseAdmin', () => {
  const MONEDAS = ['COP', 'EUR'];
  const cumple = (a, op, b) => (op === '==' ? a === b : op === '<' ? a < b : op === '<=' ? a <= b
    : op === '>' ? a > b : op === '>=' ? a >= b : false);
  const vigilar = (ruta, data) => new Proxy(data, {
    get: (o, k) => { if (MONEDAS.includes(k)) mockEstado.medMon.push(`${ruta} bloque ${k}`); return o[k]; },
  });
  const consulta = (ruta, filtros = [], orden = null, tope = null) => ({
    where: (f, op, v) => consulta(ruta, [...filtros, [f, op, v]], orden, tope),
    orderBy: (f, dir = 'asc') => consulta(ruta, filtros, [f, dir], tope),
    limit: (n) => consulta(ruta, filtros, orden, n),
    select: (...campos) => {
      if (campos.some((c) => MONEDAS.some((m) => c === m || c.startsWith(`${m}.`)))) {
        mockEstado.medMon.push(`${ruta} proyección ${campos.join(',')}`);
      }
      return consulta(ruta, filtros, orden, tope);
    },
    get: async () => {
      let docs = (mockEstado.colecciones[ruta] || []).filter((d) => filtros.every(([f, op, v]) => cumple(d[f], op, v)));
      if (orden) docs = docs.slice().sort((x, y) => (x[orden[0]] < y[orden[0]] ? -1 : 1) * (orden[1] === 'desc' ? -1 : 1));
      if (tope !== null) docs = docs.slice(0, tope);
      return { empty: docs.length === 0, size: docs.length, docs: docs.map((d) => ({ id: d.date || d.id, data: () => vigilar(ruta, d) })) };
    },
  });
  const db = {
    collection: (ruta) => consulta(ruta),
    doc: (ruta) => ({
      get: async () => {
        mockEstado.lecturasResumen.push(ruta);
        if (/_(COP|EUR)$/.test(ruta)) mockEstado.medMon.push(`${ruta} resumen`);
        const data = mockEstado.documentos[ruta];
        return { exists: data !== undefined, data: () => data };
      },
    }),
  };
  const firestore = () => db;
  firestore.FieldValue = { serverTimestamp: () => null };
  return { firestore };
});

jest.mock('../holdingsContributions', () => ({ calculateHoldingsContributions: jest.fn() }));

jest.mock('../contributionCalculator', () => ({
  calculateContributions: jest.fn(),
  enrichWithCurrentPrices: jest.fn(async (attrs) => attrs),
  findNearestPerformanceData: jest.fn(),
  getLatestPerformanceData: jest.fn(),
}));

jest.mock('../intradayCalculator', () => ({
  ...jest.requireActual('../intradayCalculator'),
  calculateIntradayPerformance: jest.fn(),
  calculateIntradayContributions: jest.fn(async () => ({ success: false, error: 'sin precios' })),
}));

const mockGenerate = jest.fn(async () => null);
jest.mock('../../snapshotGenerator', () => ({
  ...jest.requireActual('../../snapshotGenerator'),
  generatePerformanceSnapshot: (...a) => mockGenerate(...a),
}));

const mockGetExchangeRates = jest.fn();
jest.mock('../../financeQuery', () => ({
  getQuotes: jest.fn(async () => []),
  getExchangeRates: (...a) => mockGetExchangeRates(...a),
}));

const mockDerivado = jest.fn();
const mockTasas = jest.fn();
const PRINCIPAL = { 'uid-inversor-M': 'USD', 'uid-inversor-P': 'COP' };
jest.mock('../../snapshots/derivedCurrencySnapshotService', () => ({
  ...jest.requireActual('../../snapshots/derivedCurrencySnapshotService'),
  obtenerResumenDerivado: (...a) => mockDerivado(...a),
  tasasDelPeriodo: (...a) => mockTasas(...a),
  monedaPrincipalDe: async (_db, uid) => ({ 'uid-inversor-M': 'USD', 'uid-inversor-P': 'COP' })[uid],
}));

const { getPortfolioAttribution } = require('../attributionService');
const { calculateHoldingsContributions } = require('../holdingsContributions');
const { calculateContributions } = require('../contributionCalculator');
const { calculateIntradayPerformance } = require('../intradayCalculator');

// ============================================================================
// Datos (qa.md)
// ============================================================================

const M = 'uid-inversor-M';
const CIERRES_COP = { '2026-10-30': 3912.4, '2026-11-03': 3905.1, '2026-11-12': 3880.25 };

/** Un día con su bloque USD y un bloque COP guardado que no debe usarse. */
const dia = (date, totalValue, cambio) => ({
  date,
  USD: { totalValue, adjustedDailyChangePercentage: cambio },
  COP: { totalValue: 1, adjustedDailyChangePercentage: 999 },
});

function holdingsResult(currency = 'COP') {
  return {
    method: 'holdings',
    attributions: [
      { assetKey: 'AAPL_stock', ticker: 'AAPL', name: 'AAPL', status: 'active', contribution: 4, weightStart: 0.5, weightEnd: 0.5, weightAverage: 0.5, returnPercent: 8, contributionAbsolute: 400, valueStart: 5000, valueEnd: 5400, valueChange: 400, type: 'stock' },
      { assetKey: 'ECOPETROL_stock', ticker: 'ECOPETROL', name: 'ECOPETROL', status: 'active', contribution: 1, weightStart: 0.5, weightEnd: 0.5, weightAverage: 0.5, returnPercent: 2, contributionAbsolute: 100, valueStart: 5000, valueEnd: 5100, valueChange: 100, type: 'stock' },
    ],
    totalPortfolioValue: 10500,
    totalPortfolioInvestment: 0,
    portfolioReturn: 5,
    holdingsTWR: 5,
    growthFactor: 1.05,
    sumOfContributions: 5,
    latestDate: '2026-11-12',
    periodStartDate: '2026-11-02',
    baseDate: '2026-10-30',
    startTotalValue: 10000,
    latestPerformanceData: { date: '2026-11-12', [currency]: { totalValue: 10500, assetPerformance: {} } },
    coverage: { steps: 8 },
    realizedPnL: { total: 0, assetsWithSales: 0, closedPositions: 0 },
  };
}

const RANGO = { startDate: new Date('2026-11-02T00:00:00Z'), endDate: new Date('2026-11-03T00:00:00Z') };

function correr({ currency = 'COP', userId = M, accountIds = ['overall'], dateRange, period = 'YTD', includeIntraday = false } = {}) {
  return getPortfolioAttribution({
    userId, period, currency, accountIds, dateRange,
    options: { includeIntraday, includeIntradayContributions: true },
  });
}

beforeEach(() => {
  jest.clearAllMocks();
  delete process.env.ATTRIBUTION_FROM_USD;
  mockEstado.colecciones = {};
  mockEstado.documentos = {};
  mockEstado.medMon = [];
  mockEstado.lecturasResumen = [];
  mockTasas.mockImplementation(async () => CIERRES_COP);
  mockDerivado.mockImplementation(async () => ({ snapshot: { returns: { ytdReturn: 7.5 }, timeline: [{ d: '2026-11-12', v: 1, c: 0 }] }, metodo: 'derivado' }));
  calculateHoldingsContributions.mockImplementation(async (p) => holdingsResult(p.currency));
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

// ============================================================================
// Escenario 1: desde USD, sin leer lo guardado
// ============================================================================

describe('HU 6.11.4 — la atribución en una moneda poco usada se calcula desde USD (Escenario 1)', () => {
  it('QA-01: portafolio en COP: contribución por activo desde USD y rendimiento del resumen derivado; 0 MED-MON', async () => {
    const r = await correr();

    expect(r.success).toBe(true);
    expect(calculateHoldingsContributions.mock.calls[0][0]).toMatchObject({ currency: 'COP', desdeUsd: true });
    expect(mockDerivado.mock.calls[0][1]).toMatchObject({ userId: M, accountId: 'overall', currency: 'COP' });
    expect(r.summary.portfolioReturn).toBe(7.5);
    expect(calculateContributions).not.toHaveBeenCalled();
    expect(mockGenerate).not.toHaveBeenCalled();
    expect(mockEstado.medMon).toEqual([]);
  });

  it('QA-04: sin resumen derivado, el respaldo por periodo compone los días en USD × tasa, sin generar el resumen en COP', async () => {
    mockDerivado.mockImplementation(async () => ({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-resumen-usd-al-dia' }));
    mockEstado.colecciones[`portfolioPerformance/${M}/dates`] = [
      dia('2026-10-30', 100000, 0.2), dia('2026-11-03', 101000, 1), dia('2026-11-12', 102000, 0.5),
    ];

    const r = await correr({ dateRange: { startDate: new Date('2026-11-01T00:00:00Z'), endDate: new Date('2026-11-12T00:00:00Z') } });

    // Base 2026-10-30 (anterior al rango): convierte el primer día del rango
    const c1 = ((1 + 1 / 100) * 3905.1 / 3912.4 - 1) * 100;
    const c2 = ((1 + 0.5 / 100) * 3880.25 / 3905.1 - 1) * 100;
    const twr = ((1 + c1 / 100) * (1 + c2 / 100) - 1) * 100;
    expect(r.metadata.diagnostics.periodTWR).toBe(twr);
    expect(r.summary.portfolioReturn).toBe(twr);
    expect(mockGenerate).not.toHaveBeenCalled();
    expect(mockEstado.lecturasResumen.filter((x) => x.endsWith('_COP'))).toEqual([]);
    expect(mockEstado.medMon).toEqual([]);
  });

  it('QA-02: varias cuentas con fechas: cada cuenta desde sus días en USD × tasa, ponderada por su valor en COP', async () => {
    mockEstado.colecciones[`portfolioPerformance/${M}/accounts/CTA-US/dates`] = [
      dia('2026-10-30', 52000, 0.1), dia('2026-11-03', 52610, 1),
    ];
    mockEstado.colecciones[`portfolioPerformance/${M}/accounts/CTA-CO/dates`] = [
      dia('2026-10-30', 20000, 0.1), dia('2026-11-03', 19800, -1),
    ];

    const r = await correr({ accountIds: ['CTA-US', 'CTA-CO'], dateRange: RANGO });

    // TWR de cada cuenta: un día (2026-11-03) convertido contra la base del 2026-10-30
    const twrDe = (cUsd) => ((1 + ((1 + cUsd / 100) * 3905.1 / 3912.4 - 1) * 100 / 100) - 1) * 100;
    const us = twrDe(1);
    const co = twrDe(-1);
    const vUs = 52610 * 3905.1;
    const vCo = 19800 * 3905.1;
    const total = vUs + vCo;
    expect(r.metadata.diagnostics.periodTWR).toBe(0 + us * (vUs / total) + co * (vCo / total));
    expect(calculateHoldingsContributions.mock.calls[0][0]).toMatchObject({ accountIds: ['CTA-US', 'CTA-CO'], desdeUsd: true });
    expect(mockEstado.medMon).toEqual([]);
  });

  it('QA-11: el día en curso desde USD con la tasa vigente (3.874,60) y el cierre del día anterior (3.880,25)', async () => {
    calculateIntradayPerformance.mockImplementation(async () => ({
      success: true, date: '2026-11-13', totalValue: 119500, totalInvestment: 100000,
      previousDayTotalValue: 118940, previousDayDate: '2026-11-12', todayFactor: 119500 / 118940,
    }));
    mockGetExchangeRates.mockImplementation(async () => ({ rates: { COP: 3874.6 } }));

    const r = await correr({ includeIntraday: true });

    expect(calculateIntradayPerformance.mock.calls[0][0]).toMatchObject({ currency: 'USD' });
    const hoy = 119500 * 3874.6;
    const ayer = 118940 * 3880.25;
    expect(r.metadata.intraday.todayFactor).toBe(1 + ((hoy - ayer) / ayer * 100) / 100);
    expect(r.metadata.intraday.totalValue).toBe(hoy);
    expect(mockEstado.medMon).toEqual([]);
  });
});

// ============================================================================
// Escenario 3: sin con qué calcular
// ============================================================================

describe('HU 6.11.4 — sin con qué calcular, el aviso; nunca lo guardado (Escenario 3, RN-6.11-K)', () => {
  it('QA-09: el servicio de tasas no responde en el respaldo → currencyUnavailable, sin cifras', async () => {
    mockDerivado.mockImplementation(async () => ({ snapshot: null, metodo: 'no-derivable', motivo: 'sin-tasas' }));
    mockTasas.mockImplementation(async () => null);
    mockEstado.colecciones[`portfolioPerformance/${M}/dates`] = [dia('2026-11-03', 101000, 1)];

    const r = await correr();

    expect(r).toMatchObject({ success: false, currencyUnavailable: { currency: 'COP', motivo: 'sin-tasas' }, summary: null, assetAttributions: [] });
    expect(mockEstado.medMon).toEqual([]);
  });

  it('la contribución por activo sin tasa de un día → el aviso, no el cálculo legacy', async () => {
    const { monedaNoDisponible } = jest.requireActual('../../snapshots/derivedCurrencySnapshotService');
    calculateHoldingsContributions.mockImplementation(async () => { throw monedaNoDisponible('EUR', 'sin-tasa-2026-09-17'); });

    const r = await correr({ currency: 'EUR' });

    expect(r.currencyUnavailable).toEqual({ currency: 'EUR', motivo: 'sin-tasa-2026-09-17' });
    expect(calculateContributions).not.toHaveBeenCalled();
  });

  it('sin tenencias diarias (menos de 2 días) → el aviso en lugar del legacy, que solo sabe leer lo guardado', async () => {
    calculateHoldingsContributions.mockImplementation(async () => null);

    const r = await correr();

    expect(r.currencyUnavailable).toEqual({ currency: 'COP', motivo: 'sin-tenencias-diarias' });
    expect(calculateContributions).not.toHaveBeenCalled();
    expect(mockEstado.medMon).toEqual([]);
  });

  it('QA-09: día en curso con el servicio de tasas caído → el aviso, no un día en curso a 0', async () => {
    calculateIntradayPerformance.mockImplementation(async () => ({
      success: true, totalValue: 1, totalInvestment: 1, previousDayTotalValue: 1, previousDayDate: '2026-11-12', todayFactor: 1,
    }));
    mockGetExchangeRates.mockImplementation(async () => null);

    const r = await correr({ includeIntraday: true });

    expect(r.currencyUnavailable).toEqual({ currency: 'COP', motivo: 'sin-tasas' });
  });
});

// ============================================================================
// Escenarios 5 y 6
// ============================================================================

describe('HU 6.11.4 — USD, la moneda principal y el interruptor no cambian (Escenarios 5 y 6)', () => {
  it('QA-12: en USD, la contribución por activo es la de siempre (sin desdeUsd)', async () => {
    await correr({ currency: 'USD' });
    expect(calculateHoldingsContributions.mock.calls[0][0]).toMatchObject({ currency: 'USD', desdeUsd: false });
  });

  it('QA-12: en la moneda principal (COP para INV-P) se lee lo guardado como antes', async () => {
    mockDerivado.mockImplementation(async () => ({ snapshot: null, metodo: 'no-aplica' }));
    calculateHoldingsContributions.mockImplementation(async () => null);
    calculateContributions.mockImplementation(async () => ({ attributions: [], portfolioReturn: 0, error: 'No performance data found' }));

    await correr({ userId: 'uid-inversor-P' });

    expect(calculateHoldingsContributions.mock.calls[0][0]).toMatchObject({ desdeUsd: false });
    expect(mockEstado.lecturasResumen).toContain('performanceSnapshots/uid-inversor-P_COP');
    expect(calculateContributions).toHaveBeenCalled();
  });

  it('QA-13: con ATTRIBUTION_FROM_USD=false vuelve la atribución de antes en COP', async () => {
    process.env.ATTRIBUTION_FROM_USD = 'false';
    mockDerivado.mockImplementation(async () => ({ snapshot: null, metodo: 'no-derivable' }));
    calculateHoldingsContributions.mockImplementation(async () => null);
    calculateContributions.mockImplementation(async () => ({ attributions: [], portfolioReturn: 0, error: 'No performance data found' }));

    const r = await correr();

    expect(calculateHoldingsContributions.mock.calls[0][0]).toMatchObject({ desdeUsd: false });
    expect(mockEstado.lecturasResumen).toContain(`performanceSnapshots/${M}_COP`);
    expect(calculateContributions).toHaveBeenCalled();
    expect(r.currencyUnavailable).toBeUndefined();
  });
});
