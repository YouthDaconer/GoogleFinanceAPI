/**
 * HU 6.11.5 — Acción `getPreviousDayPerformance`: la cabecera del día anterior
 * en una moneda, solo del usuario de la sesión.
 *
 * El cálculo se prueba en `previousDayPerformanceService.test.js`; aquí, la
 * validación de entrada, que se pasa el `auth.uid` y el registro en el router.
 *
 * @see platform-docs/stories/6.11.5-recargar-portfolio-sin-lecturas/refinamiento.md (T2)
 */

const mockObtener = jest.fn();
jest.mock('../../snapshots/previousDayPerformanceService', () => ({
  obtenerCabeceraDiaAnterior: (...args) => mockObtener(...args),
}));

const mockDb = { collection: jest.fn() };
jest.mock('../../firebaseAdmin', () => ({ firestore: () => mockDb }));
jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockDb,
  FieldValue: { serverTimestamp: () => 'SERVER_TIMESTAMP', delete: () => 'FIELD_DELETE' },
}));

// --- Dependencias pesadas que esta prueba no ejercita -----------------------
jest.mock('../../cacheInvalidationService', () => ({ calculateDynamicTTL: jest.fn() }));
jest.mock('../../portfolioDistributionService', () => ({}));
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
const { getPreviousDayPerformance } = require('../queryHandlers');

const contextOf = (uid) => ({ auth: { uid }, rawRequest: {} });

beforeEach(() => {
  mockObtener.mockReset();
  jest.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('getPreviousDayPerformance — entrada y alcance', () => {
  test('pide la cabecera del usuario de la sesión en la moneda pedida y la devuelve tal cual', async () => {
    const respuesta = { disponible: true, date: '2026-11-12', currency: 'COP', data: { totalValue: 1 }, derivedFrom: 'USD' };
    mockObtener.mockResolvedValue(respuesta);

    await expect(getPreviousDayPerformance(contextOf('inv-u'), { currency: 'COP' })).resolves.toBe(respuesta);
    expect(mockObtener).toHaveBeenCalledWith(mockDb, { userId: 'inv-u', currency: 'COP' });
  });

  test('QA-07: nunca usa un userId del payload, solo el de la sesión', async () => {
    mockObtener.mockResolvedValue({ disponible: true, date: '', currency: 'USD', data: null });
    await getPreviousDayPerformance(contextOf('inv-c'), { currency: 'USD', userId: 'inv-u' });
    expect(mockObtener.mock.calls[0][1].userId).toBe('inv-c');
  });

  test.each([
    ['sin moneda', {}],
    ['minúsculas', { currency: 'cop' }],
    ['más de 3 letras', { currency: 'COPX' }],
    ['no es texto', { currency: 3 }],
  ])('moneda inválida (%s) → invalid-argument, sin leer nada', async (_caso, payload) => {
    const call = getPreviousDayPerformance(contextOf('inv-u'), payload);
    await expect(call).rejects.toBeInstanceOf(HttpsError);
    await expect(call).rejects.toMatchObject({ code: 'invalid-argument' });
    expect(mockObtener).not.toHaveBeenCalled();
  });

  test('si falla la lectura responde un error interno en español, sin detalles', async () => {
    mockObtener.mockRejectedValue(new Error('Firestore no responde'));
    const call = getPreviousDayPerformance(contextOf('inv-u'), { currency: 'USD' });
    await expect(call).rejects.toMatchObject({
      code: 'internal',
      message: 'Error consultando el día anterior del portafolio',
    });
  });
});

describe('router queryOperations', () => {
  test('la acción getPreviousDayPerformance está registrada, con su límite por acción', () => {
    jest.isolateModules(() => {
      jest.doMock('../performanceOnDemandHandler', () => ({ getPerformanceOnDemand: jest.fn() }));
      jest.doMock('../../../utils/rateLimiter', () => ({ rateLimiter: { checkLimit: jest.fn() } }));
      const { VALID_ACTIONS } = require('../../unified/queryOperations');
      expect(VALID_ACTIONS).toContain('getPreviousDayPerformance');
    });
    const { getRateLimitConfig } = require('../../../config/rateLimits');
    expect(getRateLimitConfig('getPreviousDayPerformance')).toEqual({ limit: 30, windowMs: 60000 });
  });
});
