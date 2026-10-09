/**
 * HU 2.3 — Tests del saldo cuya base de costo no se conoce.
 *
 * Cubren las dos acciones que hacen posible el escenario 3: la **estimación**
 * que se le propone al usuario y la **confirmación única** que fija la base y
 * hace que ninguna compra posterior vuelva a preguntar (RN-12).
 *
 * @module handlers/__tests__/balanceCostBasisMigration.test
 * @see platform-docs/stories/2.3-compra-sin-declarar-tasa/refinamiento.md (T10, D9)
 */

const { HttpsError } = require('firebase-functions/v2/https');

// ============================================================================
// Firestore falso — comparte store entre los dos módulos bajo prueba
// ============================================================================

const store = {
  portfolioAccounts: {},
  userData: {},
};

/** Transacciones que devuelve la query de efectivo de la cuenta */
let cashTransactions = [];

/** Últimas actualizaciones aplicadas a `portfolioAccounts/{id}` */
const accountUpdates = [];

const makeDocRef = (collectionName, docId) => ({
  id: docId,
  get: jest.fn().mockImplementation(async () => {
    const data = store[collectionName]?.[docId];
    return { exists: data !== undefined, id: docId, data: () => data };
  }),
  set: jest.fn().mockResolvedValue(undefined),
  update: jest.fn().mockImplementation(async (payload) => {
    accountUpdates.push(payload);
  }),
});

const makeQuery = () => {
  const query = {
    where: jest.fn(() => query),
    orderBy: jest.fn(() => query),
    limit: jest.fn(() => query),
    get: jest.fn(async () => ({
      empty: cashTransactions.length === 0,
      docs: cashTransactions.map((tx) => ({ data: () => tx })),
    })),
  };
  return query;
};

const makeCollection = (collectionName) => {
  const collection = makeQuery();
  collection.doc = (docId) => makeDocRef(collectionName, docId);
  return collection;
};

const mockFirestore = {
  collection: jest.fn((name) => makeCollection(name)),
  doc: jest.fn((path) => makeDocRef(...path.split('/'))),
};

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = { firestore: jest.fn(() => mockFirestore) };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockFirestore,
  FieldValue: { serverTimestamp: () => 'SERVER_TIMESTAMP' },
}));

// --- Dependencias pesadas de queryHandlers que esta prueba no ejercita -------

jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: jest.fn(),
  checkConsolidatedDataStatus: jest.fn(),
}));
jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: jest.fn(),
}));
jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(),
  invalidatePerformanceCache: jest.fn(),
  invalidateDistributionCache: jest.fn(),
}));
jest.mock('../../indexHistoryService', () => ({ calculateIndexData: jest.fn() }));
jest.mock('../../portfolioDistributionService', () => ({
  invalidateDistributionCache: jest.fn(),
}));
jest.mock('../../../utils/mwrCalculations', () => ({
  calculateSimplePersonalReturn: jest.fn(),
  calculateModifiedDietzReturn: jest.fn(),
}));
jest.mock('../../marketDataHelper', () => ({ getPricesFromApi: jest.fn() }));
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
jest.mock('../../helpers/subscriptionValidator', () => ({
  validateQuantityLimit: jest.fn().mockResolvedValue(undefined),
  validateFeatureAccess: jest.fn(),
}));

jest.mock('../../historicalRateService', () => ({
  getCrossRate: jest.fn(),
  getRateForDate: jest.fn(),
}));

const historicalRateService = require('../../historicalRateService');
const { getBalanceCostBasisEstimate } = require('../queryHandlers');
const { confirmBalanceCostBasis } = require('../accountHandlers');

// ============================================================================
// Fixtures
// ============================================================================

const context = { auth: { uid: 'user-123' } };

const givenAccount = (account) => {
  store.portfolioAccounts = { 'account-123': { userId: 'user-123', ...account } };
};

/** El fragmento `balanceCostBasis.{CUR}` de la última escritura */
const lastBasisWrite = (currency) =>
  accountUpdates[accountUpdates.length - 1]?.[`balanceCostBasis.${currency}`];

beforeEach(() => {
  jest.clearAllMocks();
  accountUpdates.length = 0;
  cashTransactions = [];

  store.userData = { 'user-123': { defaultCurrency: 'COP' } };
  givenAccount({ balances: { USD: 1000 } });

  historicalRateService.getCrossRate.mockResolvedValue({
    rate: 3800, rateDate: '2024-01-15', source: 'yahoo',
  });
});

// ============================================================================

describe('getBalanceCostBasisEstimate — la estimación que se le propone (HU 2.3)', () => {
  it('estima con la tasa de la fecha del primer movimiento de ese saldo', async () => {
    cashTransactions = [
      { currency: 'USD', date: '2025-06-02T10:00:00.000Z' },
      { currency: 'USD', date: '2024-01-15T10:00:00.000Z' },
      { currency: 'USD', date: '2025-01-01T10:00:00.000Z' },
    ];

    const result = await getBalanceCostBasisEstimate(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(result.firstMovementDate).toBe('2024-01-15');
    expect(result.estimatedRate).toBe(3800);
    expect(historicalRateService.getCrossRate).toHaveBeenCalledWith('USD', 'COP', '2024-01-15');
  });

  it('cuenta también la conversión que entró a esa divisa por el otro lado (HU 2.2)', async () => {
    cashTransactions = [
      { currency: 'USD', date: '2025-06-02T10:00:00.000Z' },
      { currency: 'COP', toCurrency: 'USD', date: '2023-03-08T10:00:00.000Z' },
    ];

    const result = await getBalanceCostBasisEstimate(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(result.firstMovementDate).toBe('2023-03-08');
  });

  it('ignora los movimientos de otras divisas', async () => {
    cashTransactions = [
      { currency: 'MXN', date: '2020-01-01T10:00:00.000Z' },
      { currency: 'USD', date: '2025-06-02T10:00:00.000Z' },
    ];

    const result = await getBalanceCostBasisEstimate(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(result.firstMovementDate).toBe('2025-06-02');
  });

  it('sin movimientos previos cae a la fecha de la compra que se está registrando', async () => {
    const result = await getBalanceCostBasisEstimate(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      fallbackDate: '2026-03-12',
    });

    expect(result.firstMovementDate).toBeNull();
    expect(historicalRateService.getCrossRate).toHaveBeenCalledWith('USD', 'COP', '2026-03-12');
  });

  it('sin movimientos y sin fecha de respaldo, declara la ausencia (RN-13)', async () => {
    const result = await getBalanceCostBasisEstimate(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(result.estimatedRate).toBeNull();
    expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
  });

  it('sin tasa de mercado devuelve null en vez de un valor por defecto (RN-13)', async () => {
    cashTransactions = [{ currency: 'USD', date: '2024-01-15T10:00:00.000Z' }];
    historicalRateService.getCrossRate.mockResolvedValue(null);

    const result = await getBalanceCostBasisEstimate(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(result.estimatedRate).toBeNull();
    expect(result.rateDate).toBeNull();
  });

  it('un saldo en la moneda de referencia no tiene nada que estimar (RN-14)', async () => {
    const result = await getBalanceCostBasisEstimate(context, {
      portfolioAccountId: 'account-123',
      currency: 'COP',
    });

    expect(result.estimatedRate).toBe(1);
    expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
  });

  it('rechaza una cuenta que no es del usuario', async () => {
    givenAccount({ userId: 'otro-usuario', balances: { USD: 1000 } });

    await expect(
      getBalanceCostBasisEstimate(context, { portfolioAccountId: 'account-123', currency: 'USD' })
    ).rejects.toMatchObject({ code: 'permission-denied' });
  });

  it('exige cuenta y divisa', async () => {
    await expect(
      getBalanceCostBasisEstimate(context, { currency: 'USD' })
    ).rejects.toThrow(HttpsError);
  });
});

// ============================================================================

describe('confirmBalanceCostBasis — se pregunta una sola vez (HU 2.3, RN-12)', () => {
  it('fija la base con el costo que resulta de la tasa confirmada', async () => {
    const result = await confirmBalanceCostBasis(context, {
      accountId: 'account-123',
      currency: 'USD',
      rate: 4100,
    });

    expect(result).toMatchObject({ success: true, rate: 4100, cost: 4100000, referenceCurrency: 'COP' });
    expect(lastBasisWrite('USD')).toMatchObject({
      cost: 4100000,
      referenceCurrency: 'COP',
      status: 'known',
      source: 'user-confirmed',
      confirmedRate: 4100,
    });
  });

  it('deja el saldo listo para que la siguiente compra derive su tasa del promedio', async () => {
    await confirmBalanceCostBasis(context, {
      accountId: 'account-123', currency: 'USD', rate: 4100,
    });

    const written = lastBasisWrite('USD');

    // Es la propiedad que hace que no se vuelva a preguntar: `known` con costo
    // en la referencia vigente es exactamente lo que `deriveAverageRate` acepta.
    expect(written.status).toBe('known');
    expect(written.cost / 1000).toBe(4100);
  });

  it('rechaza una segunda confirmación sobre una base ya conocida', async () => {
    givenAccount({
      balances: { USD: 1000 },
      balanceCostBasis: { USD: { cost: 4000000, referenceCurrency: 'COP', status: 'known' } },
    });

    await expect(
      confirmBalanceCostBasis(context, { accountId: 'account-123', currency: 'USD', rate: 4100 })
    ).rejects.toMatchObject({ code: 'failed-precondition' });

    expect(accountUpdates).toHaveLength(0);
  });

  it('sí permite confirmar sobre una base marcada como desconocida', async () => {
    givenAccount({
      balances: { USD: 1000 },
      balanceCostBasis: { USD: { cost: null, referenceCurrency: 'COP', status: 'unknown' } },
    });

    const result = await confirmBalanceCostBasis(context, {
      accountId: 'account-123', currency: 'USD', rate: 4100,
    });

    expect(result.success).toBe(true);
  });

  it('sí permite confirmar cuando la base quedó en otra moneda de referencia', async () => {
    givenAccount({
      balances: { USD: 1000 },
      balanceCostBasis: { USD: { cost: 20000, referenceCurrency: 'MXN', status: 'known' } },
    });

    const result = await confirmBalanceCostBasis(context, {
      accountId: 'account-123', currency: 'USD', rate: 4100,
    });

    expect(result.success).toBe(true);
  });

  it('rechaza una tasa que no es un número mayor que cero', async () => {
    for (const rate of [0, -1, 'abc', null, undefined]) {
      await expect(
        confirmBalanceCostBasis(context, { accountId: 'account-123', currency: 'USD', rate })
      ).rejects.toMatchObject({ code: 'invalid-argument' });
    }

    expect(accountUpdates).toHaveLength(0);
  });

  it('rechaza confirmar la base de un saldo en la propia moneda de referencia (RN-14)', async () => {
    await expect(
      confirmBalanceCostBasis(context, { accountId: 'account-123', currency: 'COP', rate: 1 })
    ).rejects.toMatchObject({ code: 'failed-precondition' });
  });

  it('rechaza una cuenta que no es del usuario', async () => {
    givenAccount({ userId: 'otro-usuario', balances: { USD: 1000 } });

    await expect(
      confirmBalanceCostBasis(context, { accountId: 'account-123', currency: 'USD', rate: 4100 })
    ).rejects.toMatchObject({ code: 'permission-denied' });
  });

  it('exige cuenta y divisa', async () => {
    await expect(
      confirmBalanceCostBasis(context, { currency: 'USD', rate: 4100 })
    ).rejects.toThrow(HttpsError);

    await expect(
      confirmBalanceCostBasis(context, { accountId: 'account-123', rate: 4100 })
    ).rejects.toThrow(HttpsError);
  });
});
