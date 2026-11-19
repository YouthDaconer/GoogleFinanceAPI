/**
 * HU 6.15 — Acciones de la carga ligera de la distribución:
 * `getPortfolioDistribution` con `holdingsLimit` y `getPortfolioDistributionHoldings`
 * (el resto, al usar la tabla), solo del usuario de la sesión.
 *
 * El cálculo, la caché y los motivos del recálculo se prueban en
 * `services/__tests__/portfolioDistributionService.cargaLigera.test.js`; aquí,
 * la validación de entrada, que se pasa el `auth.uid`, el log y el registro en
 * el router.
 *
 * @see platform-docs/stories/6.15-distribucion-portafolio-carga-ligera/refinamiento.md (T3)
 */

const mockDistribucion = jest.fn();
const mockResto = jest.fn();
jest.mock('../../portfolioDistributionService', () => ({
  getPortfolioDistribution: (...args) => mockDistribucion(...args),
  obtenerRestoDePosiciones: (...args) => mockResto(...args),
}));

const mockDb = { collection: jest.fn() };
jest.mock('../../firebaseAdmin', () => ({ firestore: () => mockDb }));
jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockDb,
  FieldValue: { serverTimestamp: () => 'SERVER_TIMESTAMP', delete: () => 'FIELD_DELETE' },
}));

// --- Dependencias pesadas que esta prueba no ejercita -----------------------
jest.mock('../../cacheInvalidationService', () => ({ calculateDynamicTTL: jest.fn() }));
jest.mock('../../consolidatedReturnsService', () => ({}));
jest.mock('../../historicalReturnsService', () => ({}));
jest.mock('../../indexHistoryService', () => ({ calculateIndexData: jest.fn() }));
jest.mock('../../../utils/mwrCalculations', () => ({}));
jest.mock('../../marketDataHelper', () => ({ getPricesFromApi: jest.fn() }));
jest.mock('../../historicalRateService', () => ({}));
jest.mock('../../carveOutReturnsService', () => ({ getCarveOutHistoricalReturns: jest.fn() }));
jest.mock('../../snapshots/derivedCurrencySnapshotService', () => ({
  resumenesDerivadosDelUsd: jest.fn(() => true),
  obtenerResumenDerivado: jest.fn(),
}));
jest.mock('../../snapshots/previousDayPerformanceService', () => ({ obtenerCabeceraDiaAnterior: jest.fn() }));
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: jest.fn(),
  generatePerformanceSnapshot: jest.fn(),
  generateAssetSnapshot: jest.fn(),
}));
jest.mock('../../riskMetrics/riskMetricsCache', () => ({
  isNYSEMarketOpen: jest.fn(() => false),
  calculateTTLUntilNextEOD: jest.fn(() => 3600000),
  MARKET_CACHE_TTL_MS: 300000,
}));

const { HttpsError } = require('firebase-functions/v2/https');
const { getPortfolioDistribution, getPortfolioDistributionHoldings } = require('../queryHandlers');

const contextOf = (uid) => ({ auth: { uid }, rawRequest: {} });
const SNAPSHOT = '2026-11-19T15:00:00.000Z';

let logs;
beforeEach(() => {
  mockDistribucion.mockReset();
  mockResto.mockReset();
  logs = [];
  jest.spyOn(console, 'log').mockImplementation((...a) => logs.push(a.join(' ')));
  jest.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('getPortfolioDistribution — holdingsLimit (RN-6.15-A)', () => {
  test('pasa el límite al servicio con el uid de la sesión', async () => {
    mockDistribucion.mockResolvedValue({ sectors: [], holdings: [], holdingsTotal: 0 });
    await getPortfolioDistribution(contextOf('inv-g'), { accountIds: ['acc-1'], holdingsLimit: 100, userId: 'otro' });

    expect(mockDistribucion).toHaveBeenCalledWith('inv-g', {
      accountIds: ['acc-1'],
      accountId: undefined,
      currency: 'USD',
      includeHoldings: true,
      forceRefresh: false,
      holdingsLimit: 100,
    });
  });

  test('sin holdingsLimit la llamada es la de siempre (lista completa)', async () => {
    mockDistribucion.mockResolvedValue({ sectors: [] });
    await getPortfolioDistribution(contextOf('inv-g'), { currency: 'EUR' });
    expect(mockDistribucion.mock.calls[0][1].holdingsLimit).toBeUndefined();
  });

  test.each([
    ['menor que la primera página', 9],
    ['mayor que el tope', 1001],
    ['decimal', 100.5],
    ['texto', '100'],
  ])('holdingsLimit inválido (%s) → invalid-argument, sin calcular', async (_caso, holdingsLimit) => {
    const call = getPortfolioDistribution(contextOf('inv-g'), { holdingsLimit });
    await expect(call).rejects.toBeInstanceOf(HttpsError);
    await expect(call).rejects.toMatchObject({ code: 'invalid-argument' });
    expect(mockDistribucion).not.toHaveBeenCalled();
  });
});

describe('getPortfolioDistributionHoldings — el resto (RN-6.15-C, RN-6.15-D)', () => {
  test('pide el resto del usuario de la sesión con sus cuentas, moneda, offset e instantánea', async () => {
    const resto = { holdings: [{ symbol: 'DIRR3' }], offset: 100, holdingsTotal: 101, snapshot: SNAPSHOT, origen: 'cache' };
    mockResto.mockResolvedValue(resto);

    await expect(getPortfolioDistributionHoldings(contextOf('inv-g'), {
      accountIds: ['acc-1', 'acc-2'], offset: 100, snapshot: SNAPSHOT, userId: 'otro',
    })).resolves.toBe(resto);
    expect(mockResto).toHaveBeenCalledWith(
      'inv-g',
      { accountIds: ['acc-1', 'acc-2'], accountId: undefined, currency: 'USD' },
      { offset: 100, snapshot: SNAPSHOT }
    );
    expect(logs).toContain('[getPortfolioDistributionHoldings] inv-g: cache, 1 posiciones desde 100 de 101');
  });

  test.each([
    ['otra-instancia'],
    ['cache-vencida'],
    ['instantanea-reemplazada'],
  ])('el log distingue el motivo del recálculo (%s, D4)', async (motivo) => {
    mockResto.mockResolvedValue({
      holdings: [{ symbol: 'A' }, { symbol: 'B' }], offset: 0, holdingsTotal: 2, snapshot: 'nueva', origen: 'recalculado', motivo,
    });
    await getPortfolioDistributionHoldings(contextOf('inv-g'), { offset: 100, snapshot: SNAPSHOT });
    expect(logs).toContain(`[getPortfolioDistributionHoldings] inv-g: recalculado (${motivo}), 2 posiciones desde 0 de 2`);
  });

  test.each([
    ['sin offset', { snapshot: SNAPSHOT }],
    ['offset negativo', { offset: -1, snapshot: SNAPSHOT }],
    ['offset decimal', { offset: 1.5, snapshot: SNAPSHOT }],
    ['sin instantánea', { offset: 100 }],
    ['instantánea vacía', { offset: 100, snapshot: '  ' }],
  ])('entrada inválida (%s) → invalid-argument, sin consultar', async (_caso, payload) => {
    const call = getPortfolioDistributionHoldings(contextOf('inv-g'), payload);
    await expect(call).rejects.toBeInstanceOf(HttpsError);
    await expect(call).rejects.toMatchObject({ code: 'invalid-argument' });
    expect(mockResto).not.toHaveBeenCalled();
  });

  test('si falla responde un error interno en español, sin detalles', async () => {
    mockResto.mockRejectedValue(new Error('finance-query no responde'));
    const call = getPortfolioDistributionHoldings(contextOf('inv-g'), { offset: 100, snapshot: SNAPSHOT });
    await expect(call).rejects.toMatchObject({
      code: 'internal',
      message: 'Error obteniendo la lista completa de posiciones',
    });
  });
});

describe('router queryOperations', () => {
  test('la acción getPortfolioDistributionHoldings está registrada, con su límite por acción', () => {
    jest.isolateModules(() => {
      jest.doMock('../performanceOnDemandHandler', () => ({ getPerformanceOnDemand: jest.fn() }));
      jest.doMock('../../../utils/rateLimiter', () => ({ rateLimiter: { checkLimit: jest.fn() } }));
      const { VALID_ACTIONS } = require('../../unified/queryOperations');
      expect(VALID_ACTIONS).toContain('getPortfolioDistributionHoldings');
    });
    const { getRateLimitConfig } = require('../../../config/rateLimits');
    expect(getRateLimitConfig('getPortfolioDistributionHoldings')).toEqual({ limit: 30, windowMs: 60000 });
  });
});
