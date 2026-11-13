/**
 * HU 6.8.1: el estado de la corrección del historial, para el aviso.
 *
 * Sale de las mismas fuentes que usa la corrección nocturna: la marca `_stale`
 * de `portfolioPerformance/{uid}` y la revisión en `reconcileFailures/{uid}`.
 * Solo se lee lo del `auth.uid`.
 *
 * @see platform-docs/stories/6.8.1-historial-corregido-por-tramos/qa.md (QA-13, 14, 18, 19)
 */

// --- Firestore falso: dos documentos por usuario ----------------------------
const mockDocs = new Map();
const mockReads = [];
const mockFail = { on: null };

function mockSnap(path, fieldMask = null) {
  if (mockFail.on === path) throw new Error('Firestore no responde');
  mockReads.push({ path, fieldMask });
  const data = mockDocs.get(path);
  const masked = data && fieldMask
    ? Object.fromEntries(Object.entries(data).filter(([k]) => fieldMask.includes(k)))
    : data;
  return { id: path.split('/').pop(), exists: data !== undefined, data: () => masked };
}

const mockDb = {
  collection: (name) => ({
    doc: (id) => ({ path: `${name}/${id}`, get: async () => mockSnap(`${name}/${id}`) }),
  }),
  getAll: async (...args) => {
    const options = typeof args[args.length - 1]?.fieldMask !== 'undefined' ? args.pop() : {};
    return args.map((ref) => mockSnap(ref.path, options.fieldMask || null));
  },
};

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
const { getHistoryCorrectionStatus } = require('../queryHandlers');

const contextOf = (uid) => ({ auth: { uid }, rawRequest: {} });
const marked = (since = '2026-08-12') => ({
  userId: 'x', totalValue: 1000, _stale: { since, registeredAt: '2026-11-12T15:00:00.000Z', retryCount: 0 },
});

beforeEach(() => {
  mockDocs.clear();
  mockReads.length = 0;
  mockFail.on = null;
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('getHistoryCorrectionStatus — lo que decide el aviso (RN-6.8.1-N)', () => {
  test('QA-13: con la marca de corrección pendiente → pending', async () => {
    mockDocs.set('portfolioPerformance/inv-b', marked());
    await expect(getHistoryCorrectionStatus(contextOf('inv-b'))).resolves.toEqual({ status: 'pending' });
  });

  test('QA-14 y QA-18: sin marca (nunca la tuvo o la corrección terminó) → none', async () => {
    mockDocs.set('portfolioPerformance/inv-c', { userId: 'inv-c', totalValue: 1000 });
    await expect(getHistoryCorrectionStatus(contextOf('inv-c'))).resolves.toEqual({ status: 'none' });
  });

  test('un inversor sin documento de rendimiento todavía → none', async () => {
    await expect(getHistoryCorrectionStatus(contextOf('nuevo'))).resolves.toEqual({ status: 'none' });
  });

  test('QA-19: en revisión → review, y manda sobre la marca: un solo aviso', async () => {
    mockDocs.set('reconcileFailures/inv-d', { userId: 'inv-d', abandonedAt: '2026-11-13T07:05:00.000Z' });
    await expect(getHistoryCorrectionStatus(contextOf('inv-d'))).resolves.toEqual({ status: 'review' });

    // Volvió a la cola y sigue sin resolver: sigue en revisión
    mockDocs.set('portfolioPerformance/inv-d', marked());
    await expect(getHistoryCorrectionStatus(contextOf('inv-d'))).resolves.toEqual({ status: 'review' });
  });

  test('revisión resuelta (`resolvedAt`): vuelve a pendiente si sigue en cola, o desaparece', async () => {
    mockDocs.set('reconcileFailures/inv-d', { userId: 'inv-d', resolvedAt: '2026-11-16T14:00:00.000Z' });
    mockDocs.set('portfolioPerformance/inv-d', marked());
    await expect(getHistoryCorrectionStatus(contextOf('inv-d'))).resolves.toEqual({ status: 'pending' });

    mockDocs.delete('portfolioPerformance/inv-d');
    await expect(getHistoryCorrectionStatus(contextOf('inv-d'))).resolves.toEqual({ status: 'none' });
  });

  test('QA-14: solo lee lo del usuario de la sesión; la corrección de otro no le hace aparecer el aviso', async () => {
    mockDocs.set('portfolioPerformance/inv-b', marked());
    mockDocs.set('reconcileFailures/inv-d', { userId: 'inv-d' });

    await expect(getHistoryCorrectionStatus(contextOf('inv-c'))).resolves.toEqual({ status: 'none' });
    expect(mockReads.map((r) => r.path).sort()).toEqual(['portfolioPerformance/inv-c', 'reconcileFailures/inv-c']);
  });

  test('del documento de rendimiento solo pide la marca', async () => {
    mockDocs.set('portfolioPerformance/inv-b', marked());
    await getHistoryCorrectionStatus(contextOf('inv-b'));
    expect(mockReads.find((r) => r.path === 'portfolioPerformance/inv-b').fieldMask).toEqual(['_stale']);
  });

  test('si Firestore falla responde un error interno en español, sin detalles', async () => {
    mockFail.on = 'reconcileFailures/inv-b';
    const call = getHistoryCorrectionStatus(contextOf('inv-b'));
    await expect(call).rejects.toBeInstanceOf(HttpsError);
    await expect(call).rejects.toMatchObject({
      code: 'internal',
      message: 'Error consultando el estado de la corrección del historial',
    });
  });
});

describe('router queryOperations', () => {
  test('la acción getHistoryCorrectionStatus está registrada, con su límite por acción', () => {
    jest.isolateModules(() => {
      jest.doMock('../performanceOnDemandHandler', () => ({ getPerformanceOnDemand: jest.fn() }));
      jest.doMock('../../../utils/rateLimiter', () => ({ rateLimiter: { checkLimit: jest.fn() } }));
      const { VALID_ACTIONS } = require('../../unified/queryOperations');
      expect(VALID_ACTIONS).toContain('getHistoryCorrectionStatus');
    });
    const { getRateLimitConfig } = require('../../../config/rateLimits');
    expect(getRateLimitConfig('getHistoryCorrectionStatus')).toEqual({ limit: 30, windowMs: 60000 });
  });
});
