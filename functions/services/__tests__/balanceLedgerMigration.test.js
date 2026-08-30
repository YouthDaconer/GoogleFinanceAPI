/**
 * HU 2.6 — Tests de la migración de los saldos preexistentes.
 *
 * Es la parte que hace que la épica sirva a quien ya existía. Se comprueba que
 * reconstruye lo que puede, que estima sólo donde no puede, que declara la
 * ausencia cuando tampoco puede estimar, y que no vuelve a tocar nada de lo que
 * ya resolvió.
 *
 * @module services/__tests__/balanceLedgerMigration.test
 * @see platform-docs/stories/2.6-libro-mayor-saldo-migracion/refinamiento.md (T23)
 */

// ============================================================================
// Firestore falso
// ============================================================================

const store = {
  portfolioAccounts: {},
  userData: {},
  transactions: {},
};

const mockBatch = {
  set: jest.fn(),
  update: jest.fn(),
  delete: jest.fn(),
  commit: jest.fn().mockResolvedValue(undefined),
};

let generatedDocId = 0;

const makeDocRef = (collectionName, docId) => ({
  id: docId,
  get: jest.fn(async () => {
    const data = store[collectionName]?.[docId];
    return { exists: data !== undefined, id: docId, data: () => data };
  }),
  set: jest.fn(async () => undefined),
  update: jest.fn(async () => undefined),
});

const makeQuery = (collectionName, clauses = []) => ({
  where: (field, _op, value) => makeQuery(collectionName, [...clauses, [field, value]]),
  orderBy: function () { return this; },
  limit: function () { return this; },
  get: jest.fn(async () => {
    const docs = Object.entries(store[collectionName] || {})
      .filter(([, data]) => clauses.every(([field, value]) => data[field] === value))
      .map(([id, data]) => ({ id, data: () => data, ref: makeDocRef(collectionName, id) }));

    return { empty: docs.length === 0, docs, forEach: (fn) => docs.forEach(fn) };
  }),
});

const makeCollection = (collectionName) => {
  const collection = makeQuery(collectionName);
  collection.doc = (docId) => makeDocRef(collectionName, docId ?? `generated-${++generatedDocId}`);
  return collection;
};

const mockDb = {
  collection: jest.fn((name) => makeCollection(name)),
  doc: jest.fn((docPath) => makeDocRef(...docPath.split('/'))),
  batch: jest.fn(() => mockBatch),
};

jest.mock('../firebaseAdmin', () => {
  const mockAdmin = { firestore: jest.fn(() => mockDb) };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

jest.mock('../historicalRateService', () => ({
  getCrossRate: jest.fn(),
  getRateForDate: jest.fn(),
}));

const historicalRateService = require('../historicalRateService');
const { migrateBalanceLedgerForUser, MIGRATION_SOURCES } = require('../balanceLedgerMigration');

// ============================================================================
// Fixtures
// ============================================================================

/** Documentos de apertura que la migración escribió */
const openings = () => mockBatch.set.mock.calls.map(([, data]) => data);

/** Actualizaciones de cuenta que la migración preparó */
const accountUpdate = () => mockBatch.update.mock.calls[0]?.[1] || {};

const seedAccount = (overrides = {}) => {
  store.portfolioAccounts = {
    'account-123': {
      userId: 'user-123',
      name: 'Interactive Brokers',
      balances: { USD: 1000 },
      createdAt: '2026-01-02T00:00:00.000Z',
      ...overrides,
    },
  };
};

beforeEach(() => {
  jest.clearAllMocks();
  generatedDocId = 0;
  mockBatch.commit.mockResolvedValue(undefined);

  seedAccount();
  store.userData = { 'user-123': { defaultCurrency: 'COP' } };
  store.transactions = {};

  historicalRateService.getCrossRate.mockResolvedValue({
    rate: 3950, rateDate: '2026-01-02', source: 'cache',
  });
});

// ============================================================================

describe('migrateBalanceLedgerForUser — reconstruir lo que se puede', () => {
  it('cuando el historial explica el saldo, la base sale del replay y no se escribe apertura', async () => {
    store.transactions = {
      'tx-1': {
        portfolioAccountId: 'account-123',
        type: 'cash_income',
        amount: 1000,
        price: 1,
        currency: 'USD',
        date: '2026-02-01T10:00:00.000Z',
        acquisitionRate: 4000,
        acquisitionCost: 4000000,
      },
    };

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result.migratedCount).toBe(1);
    expect(openings()).toHaveLength(0);

    expect(accountUpdate()['balanceCostBasis.USD']).toMatchObject({
      cost: 4000000,
      status: 'known',
      source: MIGRATION_SOURCES.LEDGER_REPLAY,
    });
    expect(result.notices).toHaveLength(0);
  });

  it('anota el veredicto de conciliación aunque no haya nada que migrar (D4)', async () => {
    store.transactions = {
      'tx-1': {
        portfolioAccountId: 'account-123',
        type: 'cash_income',
        amount: 1000,
        price: 1,
        currency: 'USD',
        date: '2026-02-01T10:00:00.000Z',
        acquisitionRate: 4000,
        acquisitionCost: 4000000,
      },
    };

    await migrateBalanceLedgerForUser('user-123');

    expect(accountUpdate()['balanceReconciliation.USD']).toMatchObject({
      ledgerBalance: 1000,
      difference: 0,
      status: 'reconciled',
    });
  });
});

describe('migrateBalanceLedgerForUser — estimar sólo donde no se puede reconstruir', () => {
  it('un saldo sin historial recibe UN asiento de apertura con la tasa estimada (AC-8, D5)', async () => {
    const result = await migrateBalanceLedgerForUser('user-123');

    expect(openings()).toHaveLength(1);
    expect(openings()[0]).toMatchObject({
      type: 'cash_adjustment',
      adjustmentReason: 'opening',
      adjustmentDelta: 1000,
      currency: 'USD',
      acquisitionRate: 3950,
      acquisitionCost: 3950000,
      costBasisEstimated: true,
      portfolioAccountId: 'account-123',
    });

    expect(result.estimatedCount).toBe(1);
  });

  it('la base queda marcada como estimada, para poder rotularla y confirmarla (RN-12, D14)', async () => {
    await migrateBalanceLedgerForUser('user-123');

    expect(accountUpdate()['balanceCostBasis.USD']).toMatchObject({
      source: MIGRATION_SOURCES.MIGRATION_ESTIMATED,
      estimatedRate: 3950,
      status: 'known',
    });
  });

  it('con la apertura escrita el saldo pasa a cuadrar con su historial', async () => {
    await migrateBalanceLedgerForUser('user-123');

    expect(accountUpdate()['balanceReconciliation.USD']).toMatchObject({
      ledgerBalance: 1000,
      difference: 0,
      status: 'reconciled',
    });
  });

  it('la apertura se fecha en el primer movimiento del saldo cuando lo hay', async () => {
    store.transactions = {
      'tx-1': {
        portfolioAccountId: 'account-123',
        type: 'cash_income',
        amount: 300,
        price: 1,
        currency: 'USD',
        date: '2026-05-20T10:00:00.000Z',
        acquisitionRate: 4100,
        acquisitionCost: 1230000,
      },
    };

    await migrateBalanceLedgerForUser('user-123');

    expect(openings()[0].date.substring(0, 10)).toBe('2026-05-20');
    // Sólo la diferencia: los 300 que el historial sí explica no se duplican.
    expect(openings()[0].adjustmentDelta).toBe(700);
  });

  it('sin ningún movimiento se fecha en la creación de la cuenta', async () => {
    await migrateBalanceLedgerForUser('user-123');

    expect(openings()[0].date.substring(0, 10)).toBe('2026-01-02');
  });

  it('deja un aviso con la tasa que propone (AC-6)', async () => {
    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result.notices).toEqual([
      expect.objectContaining({
        accountId: 'account-123',
        accountName: 'Interactive Brokers',
        currency: 'USD',
        estimatedRate: 3950,
      }),
    ]);
  });
});

describe('migrateBalanceLedgerForUser — un dato ausente se declara ausente (RN-13)', () => {
  it('sin tasa disponible la apertura se escribe igual, con la base indeterminada', async () => {
    historicalRateService.getCrossRate.mockResolvedValue(null);

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(openings()).toHaveLength(1);
    expect(openings()[0].acquisitionRate).toBeNull();
    expect(openings()[0].acquisitionCost).toBeNull();

    expect(accountUpdate()['balanceCostBasis.USD']).toMatchObject({
      status: 'unknown',
      source: MIGRATION_SOURCES.MIGRATION_ESTIMATED,
    });
    expect(result.unavailableCount).toBe(1);
  });

  it('un fallo del proveedor de tasas no deja el saldo sin su apertura', async () => {
    historicalRateService.getCrossRate.mockRejectedValue(new Error('Yahoo caído'));

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(openings()).toHaveLength(1);
    expect(result.unavailableCount).toBe(1);
  });

  it('un historial que llega al saldo pero no dice a qué tasa, se declara sin base', async () => {
    store.transactions = {
      'tx-1': {
        portfolioAccountId: 'account-123',
        type: 'cash_income',
        amount: 1000,
        price: 1,
        currency: 'USD',
        date: '2026-02-01T10:00:00.000Z',
        acquisitionRate: null,
        acquisitionCost: null,
      },
    };

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(openings()).toHaveLength(0);
    expect(accountUpdate()['balanceCostBasis.USD'].status).toBe('unknown');
    expect(result.notices).toHaveLength(1);
    expect(result.notices[0].estimatedRate).toBeNull();
  });
});

describe('migrateBalanceLedgerForUser — idempotencia (D13)', () => {
  it('una base ya migrada no se vuelve a tocar', async () => {
    seedAccount({
      balanceCostBasis: {
        USD: { cost: 3950000, referenceCurrency: 'COP', status: 'known', source: MIGRATION_SOURCES.MIGRATION_ESTIMATED },
      },
    });

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result.migratedCount).toBe(0);
    expect(openings()).toHaveLength(0);
    expect(accountUpdate()).not.toHaveProperty('balanceCostBasis.USD');
  });

  it('una base que confirmó el usuario es intocable (RN-12)', async () => {
    seedAccount({
      balanceCostBasis: {
        USD: { cost: 4200000, referenceCurrency: 'COP', status: 'known', source: 'user-confirmed', confirmedRate: 4200 },
      },
    });

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result.migratedCount).toBe(0);
    expect(openings()).toHaveLength(0);
  });

  it('una base construida por 2.1 movimiento a movimiento se respeta', async () => {
    seedAccount({
      balanceCostBasis: {
        USD: { cost: 4000000, referenceCurrency: 'COP', status: 'known' },
      },
    });

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result.migratedCount).toBe(0);
  });

  it('pero una base indeterminada sí se reintenta', async () => {
    seedAccount({
      balanceCostBasis: {
        USD: { cost: null, referenceCurrency: 'COP', status: 'unknown' },
      },
    });

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result.migratedCount).toBe(1);
    expect(openings()).toHaveLength(1);
  });
});

describe('migrateBalanceLedgerForUser — acotación', () => {
  it('con más saldos de los que caben en una pasada, devuelve hasMore', async () => {
    seedAccount({ balances: { USD: 1000, EUR: 400, GBP: 200 } });

    const result = await migrateBalanceLedgerForUser('user-123', { maxBalances: 1 });

    expect(result.hasMore).toBe(true);
    expect(result.migratedCount).toBe(1);
  });

  it('el tope de consultas de tasa también corta la pasada', async () => {
    seedAccount({ balances: { USD: 1000, EUR: 400 } });

    const result = await migrateBalanceLedgerForUser('user-123', { maxRateLookups: 1 });

    expect(result.hasMore).toBe(true);
  });
});

describe('migrateBalanceLedgerForUser — sin exposición cambiaria, sin ruido (AC-9, RN-14)', () => {
  it('un saldo en la moneda de referencia no estima nada ni deja aviso', async () => {
    seedAccount({ balances: { COP: 5000000 } });

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result.notices).toHaveLength(0);
    expect(openings()).toHaveLength(0);
    expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
  });

  it('pero su veredicto de conciliación sí se anota', async () => {
    seedAccount({ balances: { COP: 5000000 } });

    await migrateBalanceLedgerForUser('user-123');

    expect(accountUpdate()['balanceReconciliation.COP']).toMatchObject({
      difference: 5000000,
      status: 'drift',
    });
  });
});

describe('migrateBalanceLedgerForUser — usuarios sin nada que migrar', () => {
  it('sin cuentas no hace nada', async () => {
    store.portfolioAccounts = {};

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result).toMatchObject({ migratedCount: 0, hasMore: false, notices: [] });
  });

  it('una cuenta sin saldos no se lee siquiera', async () => {
    seedAccount({ balances: {} });

    const result = await migrateBalanceLedgerForUser('user-123');

    expect(result.scannedCount).toBe(0);
    expect(mockBatch.commit).not.toHaveBeenCalled();
  });
});
