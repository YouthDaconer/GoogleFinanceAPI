/**
 * FEAT-EXCLUDE-001: getHistoricalReturns con excludeTickers
 *
 * Ruteo a la rama de carve-out, validación, cache y rechazo explícito en los
 * endpoints que no soportan la exclusión.
 */

// ============================================================================
// Mocks (mismo andamiaje que queryHandlers.getHistoricalReturns.test.js)
// ============================================================================

jest.mock('firebase-admin', () => ({
  firestore: Object.assign(jest.fn(() => ({})), {
    FieldValue: { serverTimestamp: jest.fn(() => 'mock-server-timestamp') },
  }),
  initializeApp: jest.fn(),
}));

const mockGetHistoricalReturnsV2 = jest.fn();
jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: (...args) => mockGetHistoricalReturnsV2(...args),
  checkConsolidatedDataStatus: jest.fn(),
}));

jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: jest.fn(),
}));

jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(() => new Date('2099-01-01T00:00:00Z')),
}));

jest.mock('../../indexHistoryService', () => ({ calculateIndexData: jest.fn() }));
jest.mock('../../portfolioDistributionService', () => ({}));
jest.mock('../../../utils/mwrCalculations', () => ({
  calculateSimplePersonalReturn: jest.fn(),
  calculateModifiedDietzReturn: jest.fn(),
}));
jest.mock('../../marketDataHelper', () => ({ getPricesFromApi: jest.fn() }));

const mockBuildSnapshotDocId = jest.fn();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (...args) => mockBuildSnapshotDocId(...args),
  generatePerformanceSnapshot: jest.fn(),
  generateAssetSnapshot: jest.fn(),
}));

jest.mock('../../riskMetrics/riskMetricsCache', () => ({
  isNYSEMarketOpen: jest.fn(() => false),
  calculateTTLUntilNextEOD: jest.fn(() => 6 * 60 * 60 * 1000),
  MARKET_CACHE_TTL_MS: 5 * 60 * 1000,
}));

const mockGetCarveOut = jest.fn();
jest.mock('../../carveOutReturnsService', () => ({
  getCarveOutHistoricalReturns: (...args) => mockGetCarveOut(...args),
}));

const mockCacheGet = jest.fn();
const mockCacheSet = jest.fn().mockResolvedValue();
const mockDoc = jest.fn(() => ({ get: mockCacheGet, set: mockCacheSet }));

jest.mock('../../firebaseAdmin', () => {
  const firestoreMock = () => ({ doc: mockDoc, collection: jest.fn(() => ({ doc: mockDoc })) });
  firestoreMock.FieldValue = { serverTimestamp: jest.fn() };
  return { firestore: firestoreMock, initializeApp: jest.fn() };
});

const {
  getHistoricalReturns,
  getHistoricalReturnsOptimized,
  getMultiAccountHistoricalReturns,
} = require('../queryHandlers');

// ============================================================================
// Tests
// ============================================================================

const context = { auth: { uid: 'user1' } };
const carveResult = {
  returns: { oneYearReturn: 12 },
  totalValueData: { dates: [], values: [], percentChanges: [] },
  carveOut: { method: 'holdings-twr', excludedTickers: ['VUAA.L'] },
};

const cachePaths = () => mockDoc.mock.calls.map((c) => c[0]);

describe('FEAT-EXCLUDE-001: getHistoricalReturns con excludeTickers', () => {
  beforeEach(() => {
    mockCacheGet.mockResolvedValue({ exists: false });
    mockGetCarveOut.mockResolvedValue(carveResult);
  });

  it('rutea al carve-out y no toca snapshots', async () => {
    const result = await getHistoricalReturns(context, { currency: 'USD', excludeTickers: ['VUAA.L'] });

    expect(mockBuildSnapshotDocId).not.toHaveBeenCalled();
    expect(mockGetHistoricalReturnsV2).not.toHaveBeenCalled();
    const params = mockGetCarveOut.mock.calls[0][0];
    expect(params).toMatchObject({ userId: 'user1', currency: 'USD', accountId: 'overall' });
    expect([...params.excludeSet]).toEqual(['VUAA.L']);
    expect(result).toMatchObject({ ...carveResult, cacheHit: false });
    expect(mockCacheSet).toHaveBeenCalledWith(expect.objectContaining({ data: carveResult }));
  });

  it('la clave de cache incluye la exclusión y no depende de orden ni mayúsculas', async () => {
    await getHistoricalReturns(context, { accountId: 'acc-1', excludeTickers: ['VUAA.L', 'voo'] });
    await getHistoricalReturns(context, { accountId: 'acc-1', excludeTickers: ['VOO', 'vuaa.l'] });
    await getHistoricalReturns(context, { accountId: 'acc-1', excludeTickers: ['VOO'] });

    const [a, b, c] = cachePaths();
    expect(a).toMatch(/^userData\/user1\/performanceCache\/USD_acc-1_ex_[0-9a-f]{12}$/);
    expect(b).toBe(a);
    expect(c).not.toBe(a);
  });

  it('devuelve el cache vigente sin recalcular', async () => {
    mockCacheGet.mockResolvedValue({
      exists: true,
      data: () => ({ data: carveResult, lastCalculated: 'x', validUntil: '2099-01-01T00:00:00Z' }),
    });
    const result = await getHistoricalReturns(context, { excludeTickers: ['VUAA.L'] });
    expect(result.cacheHit).toBe(true);
    expect(mockGetCarveOut).not.toHaveBeenCalled();
  });

  it('forceRefresh ignora el cache', async () => {
    await getHistoricalReturns(context, { excludeTickers: ['VUAA.L'], forceRefresh: true });
    expect(mockCacheGet).not.toHaveBeenCalled();
    expect(mockGetCarveOut).toHaveBeenCalled();
  });

  it.each([
    [{ excludeTickers: [] }, /vacío/],
    [{ excludeTickers: 'VUAA.L' }, /arreglo/],
    [{ excludeTickers: [''] }, /no vacíos/],
    [{ excludeTickers: ['VUAA.L'], ticker: 'MSFT', assetType: 'stock' }, /ticker\/assetType/],
  ])('rechaza %p', async (payload, message) => {
    await expect(getHistoricalReturns(context, payload)).rejects.toMatchObject({
      code: 'invalid-argument',
      message: expect.stringMatching(message),
    });
    expect(mockGetCarveOut).not.toHaveBeenCalled();
  });

  it('sin excludeTickers sigue la ruta de siempre', async () => {
    mockBuildSnapshotDocId.mockReturnValue('snap-id');
    mockCacheGet.mockResolvedValue({ exists: false });
    await getHistoricalReturns(context, { currency: 'USD' }).catch(() => {});
    expect(mockBuildSnapshotDocId).toHaveBeenCalled();
    expect(mockGetCarveOut).not.toHaveBeenCalled();
  });

  it.each([
    ['getHistoricalReturnsOptimized', () => getHistoricalReturnsOptimized(context, { excludeTickers: ['VUAA.L'] })],
    ['getMultiAccountHistoricalReturns', () => getMultiAccountHistoricalReturns(context, { accountIds: ['a'], excludeTickers: ['VUAA.L'] })],
  ])('%s rechaza excludeTickers en lugar de ignorarlo', async (name, call) => {
    await expect(call()).rejects.toMatchObject({ code: 'invalid-argument', message: expect.stringMatching(name) });
  });
});
