/**
 * FIX-ATTR-HOLDINGS-001: getPortfolioAttribution con el método por tenencias.
 *
 * Comprueba que las contribuciones NO se fuerzan a sumar el TWR (el residuo
 * se informa como no atribuido), que el intradía por activo se encadena con
 * el crecimiento del período y que sin serie diaria se cae al legacy.
 */

jest.mock('../../firebaseAdmin', () => {
  const ref = { get: async () => ({ exists: false }) };
  const db = { collection: () => ({}), doc: () => ref };
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
  calculateIntradayContributions: jest.fn(),
}));

jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: jest.fn(() => 'snap'),
  generatePerformanceSnapshot: jest.fn(async () => null),
}));

jest.mock('../../financeQuery', () => ({ getQuotes: jest.fn(async () => []) }));

const { getPortfolioAttribution } = require('../attributionService');
const { calculateHoldingsContributions } = require('../holdingsContributions');
const { calculateContributions } = require('../contributionCalculator');
const { calculateIntradayContributions } = require('../intradayCalculator');

const attr = (over) => ({
  sector: 'Unknown',
  type: 'stock',
  weightStart: 0.5,
  weightEnd: 0.5,
  weightAverage: 0.5,
  returnPercent: 10,
  contributionAbsolute: 50,
  valueStart: 500,
  valueEnd: 550,
  valueChange: 50,
  ...over,
});

function holdingsResult() {
  return {
    method: 'holdings',
    attributions: [
      attr({ assetKey: 'A_stock', ticker: 'A', name: 'A', status: 'active', contribution: 8 }),
      attr({ assetKey: 'B_stock', ticker: 'B', name: 'B', status: 'sold', contribution: 2, weightEnd: 0, valueEnd: 0 }),
    ],
    totalPortfolioValue: 1000,
    totalPortfolioInvestment: 0,
    portfolioReturn: 10,
    holdingsTWR: 10,
    growthFactor: 1.1,
    sumOfContributions: 10,
    latestDate: '2026-11-09',
    periodStartDate: '2026-01-02',
    baseDate: '2025-12-31',
    startTotalValue: 900,
    latestPerformanceData: { date: '2026-11-09', USD: { totalValue: 1000, assetPerformance: {} } },
    coverage: { steps: 200 },
    realizedPnL: { total: 0, assetsWithSales: 0, closedPositions: 1 },
  };
}

const run = (options = {}, currency = 'USD') => getPortfolioAttribution({
  userId: 'u1',
  period: 'YTD',
  currency,
  accountIds: ['overall'],
  options: { portfolioReturn: 12, includeIntraday: false, includeIntradayContributions: true, ...options },
});

beforeEach(() => jest.clearAllMocks());

describe('getPortfolioAttribution — método por tenencias', () => {
  it('no reparte el residuo: informa lo no atribuido y encadena el intradía con G', async () => {
    calculateHoldingsContributions.mockResolvedValue(holdingsResult());
    calculateIntradayContributions.mockResolvedValue({ success: true, contributions: { A_stock: 1, B_stock: 0.5 } });

    const res = await run();

    expect(res.success).toBe(true);
    expect(calculateContributions).not.toHaveBeenCalled();
    const a = res.assetAttributions.find((x) => x.ticker === 'A');
    const b = res.assetAttributions.find((x) => x.ticker === 'B');
    expect(a.contribution).toBeCloseTo(8 + 1 * 1.1, 10); // intradía × crecimiento del período
    expect(a.intradayContribution).toBeCloseTo(1.1, 10);
    expect(b.contribution).toBe(2); // vendido: sin intradía y sin ajuste
    expect(b.originalContribution).toBeUndefined();
    // TWR mostrado = TWR del frontend combinado con el intradía
    expect(res.summary.portfolioReturn).toBeCloseTo((1.12 * 1.01 - 1) * 100, 10);
    expect(res.summary.attributedReturn).toBeCloseTo(11.1, 10);
    expect(res.summary.unattributedReturn).toBeCloseTo((1.12 * 1.01 - 1) * 100 - 11.1, 10);
    expect(res.summary.attributionMethod).toBe('holdings');
    expect(res.summary.includesIntraday).toBe(true);
    expect(res.metadata.attributionMethod).toBe('holdings');
    expect(res.metadata.dataSource).toBe('holdings-daily + intraday');
  });

  it('sin intradía las contribuciones quedan tal cual y el retorno es el del frontend', async () => {
    calculateHoldingsContributions.mockResolvedValue(holdingsResult());

    const res = await run({ includeIntradayContributions: false });

    expect(calculateIntradayContributions).not.toHaveBeenCalled();
    expect(res.summary.portfolioReturn).toBe(12);
    expect(res.summary.attributedReturn).toBeCloseTo(10, 10);
    expect(res.summary.unattributedReturn).toBeCloseTo(2, 10);
    expect(res.summary.includesIntraday).toBe(false);
  });

  it('fuera de USD no calcula el intradía (las cotizaciones se convierten a USD)', async () => {
    calculateHoldingsContributions.mockResolvedValue({
      ...holdingsResult(),
      latestPerformanceData: { date: '2026-11-09', COP: { totalValue: 1, assetPerformance: {} } },
    });

    await run({}, 'COP');

    expect(calculateIntradayContributions).not.toHaveBeenCalled();
  });

  it('sin serie diaria cae al cálculo legacy', async () => {
    calculateHoldingsContributions.mockResolvedValue(null);
    calculateContributions.mockResolvedValue({
      attributions: [attr({ assetKey: 'A_stock', ticker: 'A', status: 'active', contribution: 12 })],
      totalPortfolioValue: 1000,
      totalPortfolioInvestment: 900,
      portfolioReturn: 11,
      latestDate: '2026-11-09',
      periodStartDate: '2026-01-02',
      startTotalValue: 900,
      sumOfContributions: 12,
      discrepancy: 0,
      normalized: false,
      realizedPnL: { total: 0, assetsWithSales: 0, closedPositions: 0 },
    });

    const res = await run();

    expect(calculateContributions).toHaveBeenCalled();
    expect(res.summary.attributionMethod).toBe('legacy');
    expect(res.summary.unattributedReturn).toBe(0);
    // legacy conserva su normalización al TWR (aquí la suma ya era 12)
    expect(res.assetAttributions[0].contribution).toBeCloseTo(12, 10);
    expect(res.summary.portfolioReturn).toBe(12);
  });

  it('si el método por tenencias lanza, también cae a legacy', async () => {
    calculateHoldingsContributions.mockRejectedValue(new Error('firestore caído'));
    calculateContributions.mockResolvedValue({
      attributions: [],
      totalPortfolioValue: 0,
      portfolioReturn: 0,
      error: 'No performance data found',
    });

    const res = await run();

    expect(calculateContributions).toHaveBeenCalled();
    expect(res.success).toBe(false);
  });
});
