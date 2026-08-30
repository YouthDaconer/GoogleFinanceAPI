/**
 * HU 2.6 — Tests del ajuste manual y de los caminos que movían un saldo sin
 * dejar rastro.
 *
 * Lo que se comprueba es la invariante de la historia: después de esta HU no
 * queda ninguna acción del producto que cambie un saldo sin registrar su
 * movimiento, y ninguna que borre un movimiento sin devolver su efectivo.
 *
 * @module handlers/__tests__/balanceAdjustment.test
 * @see platform-docs/stories/2.6-libro-mayor-saldo-migracion/refinamiento.md (T22)
 */

// ============================================================================
// Firestore falso, compartido por los dos handlers
// ============================================================================

const store = {
  portfolioAccounts: {},
  userData: {},
  transactions: {},
  assets: {},
};

/** Escrituras sueltas (fuera de batch) sobre `portfolioAccounts` */
const directUpdates = [];

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
    return { exists: data !== undefined, id: docId, data: () => data, ref: makeDocRef(collectionName, docId) };
  }),
  set: jest.fn(async () => undefined),
  update: jest.fn(async (payload) => {
    if (collectionName === 'portfolioAccounts') directUpdates.push({ docId, payload });
  }),
});

/**
 * Query que filtra el almacén por las cláusulas `where` acumuladas. Sólo
 * soporta `==`, que es lo único que usan los handlers de esta historia.
 */
const makeQuery = (collectionName, clauses = []) => ({
  where: (field, _op, value) => makeQuery(collectionName, [...clauses, [field, value]]),
  orderBy: function () { return this; },
  limit: function () { return this; },
  get: jest.fn(async () => {
    const docs = Object.entries(store[collectionName] || {})
      .filter(([, data]) => clauses.every(([field, value]) => data[field] === value))
      .map(([id, data]) => ({ id, data: () => data, ref: makeDocRef(collectionName, id) }));

    return {
      empty: docs.length === 0,
      docs,
      forEach: (fn) => docs.forEach(fn),
    };
  }),
});

const makeCollection = (collectionName) => {
  const collection = makeQuery(collectionName);
  collection.doc = (docId) => makeDocRef(collectionName, docId ?? `generated-${++generatedDocId}`);
  collection.add = jest.fn(async () => makeDocRef(collectionName, `generated-${++generatedDocId}`));
  return collection;
};

const mockDb = {
  collection: jest.fn((name) => makeCollection(name)),
  doc: jest.fn((docPath) => makeDocRef(...docPath.split('/'))),
  batch: jest.fn(() => mockBatch),
};

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = { firestore: jest.fn(() => mockDb) };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockDb,
  FieldValue: {
    serverTimestamp: () => 'SERVER_TIMESTAMP',
    delete: () => 'FIELD_DELETE',
  },
}));

jest.mock('../../cacheInvalidationService', () => ({
  invalidatePerformanceCache: jest.fn().mockResolvedValue(undefined),
  invalidateDistributionCache: jest.fn(),
}));

jest.mock('../../portfolioDistributionService', () => ({
  invalidateDistributionCache: jest.fn(),
}));

jest.mock('../../helpers/subscriptionValidator', () => ({
  validateQuantityLimit: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../financeQuery', () => ({ getQuotes: jest.fn().mockResolvedValue([]) }));

jest.mock('../../../utils/logoGenerator', () => ({
  generateLogoUrl: jest.fn().mockReturnValue('https://logo.url'),
}));

jest.mock('../../../utils/performanceStaleMarker', () => ({
  checkAndMarkStaleIfRetroactive: jest.fn(),
}));

jest.mock('../../historicalRateService', () => ({
  getCrossRate: jest.fn(),
  getRateForDate: jest.fn(),
}));

const historicalRateService = require('../../historicalRateService');
const assetHandlers = require('../assetHandlers');
const accountHandlers = require('../accountHandlers');

// ============================================================================
// Fixtures
// ============================================================================

const context = { auth: { uid: 'user-123' } };

/** Documentos que el batch escribió en `transactions` */
const writtenTransactions = () => mockBatch.set.mock.calls.map(([, data]) => data);

/** Actualizaciones de cuenta que el batch preparó */
const batchAccountUpdates = () => mockBatch.update.mock.calls.map(([, payload]) => payload);

beforeEach(() => {
  jest.clearAllMocks();
  directUpdates.length = 0;
  generatedDocId = 0;
  mockBatch.commit.mockResolvedValue(undefined);

  store.portfolioAccounts = {
    'account-123': {
      userId: 'user-123',
      name: 'Interactive Brokers',
      balances: { USD: 1000, EUR: 400 },
      balanceCostBasis: {
        USD: { cost: 4000000, referenceCurrency: 'COP', status: 'known' },
        EUR: { cost: 1800000, referenceCurrency: 'COP', status: 'known' },
      },
    },
  };
  store.userData = { 'user-123': { defaultCurrency: 'COP' } };
  store.transactions = {};
  store.assets = {};

  historicalRateService.getCrossRate.mockResolvedValue({
    rate: 4300, rateDate: '2026-08-20', source: 'cache',
  });
  historicalRateService.getRateForDate.mockResolvedValue({
    rate: 4300, rateDate: '2026-08-20', source: 'cache',
  });
});

// ============================================================================

describe('registerBalanceAdjustment — corregir a mano deja rastro (AC-4)', () => {
  it('el ajuste que sube el saldo se registra con su tasa, su fecha y su motivo', async () => {
    const result = await assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 1050,
      date: '2026-08-20',
      reason: 'No coincidía con el extracto',
    });

    expect(result.delta).toBe(50);
    expect(result.newBalance).toBe(1050);

    const transaction = writtenTransactions()[0];
    expect(transaction).toMatchObject({
      type: 'cash_adjustment',
      adjustmentReason: 'manual',
      adjustmentDelta: 50,
      amount: 50,
      currency: 'USD',
      description: 'No coincidía con el extracto',
      acquisitionRate: 4300,
      // RN-2.6-B: la entrada aporta su costo al saldo
      acquisitionCost: 215000,
    });
    expect(transaction.date.substring(0, 10)).toBe('2026-08-20');
  });

  it('el costo del saldo sube con el ajuste, no se queda atado al saldo viejo', async () => {
    await assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 1050,
      date: '2026-08-20',
    });

    const update = batchAccountUpdates()[0];
    expect(update['balances.USD']).toBe(1050);
    expect(update['balanceCostBasis.USD'].cost).toBe(4215000);
  });

  it('sin tipo de cambio no se registra un ajuste que añade divisa extranjera (RN-2.6-B)', async () => {
    historicalRateService.getCrossRate.mockResolvedValue(null);

    await expect(assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 1050,
      date: '2026-08-20',
    })).rejects.toThrow(/tipo de cambio/i);

    expect(mockBatch.commit).not.toHaveBeenCalled();
  });

  it('la tasa que declara el usuario manda sobre la de mercado', async () => {
    const result = await assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 1050,
      date: '2026-08-20',
      exchangeRate: 4111,
    });

    expect(result.exchangeRate).toBe(4111);
    expect(writtenTransactions()[0].acquisitionRateSource).toBe('user');
  });

  it('el ajuste que baja el saldo consume base al promedio y no pregunta tasa', async () => {
    historicalRateService.getCrossRate.mockResolvedValue(null);

    await assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 900,
      date: '2026-08-20',
    });

    const update = batchAccountUpdates()[0];
    expect(update['balances.USD']).toBe(900);
    // 100 USD salen a la tasa promedio de 4.000: el costo baja 400.000
    expect(update['balanceCostBasis.USD'].cost).toBe(3600000);
  });

  it('un ajuste NO realiza diferencia en cambio (D7)', async () => {
    await assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 900,
      date: '2026-08-20',
    });

    const transaction = writtenTransactions()[0];
    expect(transaction.realizedFxAvailability).toBe('not-applicable');
    expect(transaction.realizedFxAmount).toBeNull();
  });

  it('sin exposición cambiaria el documento no lleva ni un campo de divisa (RN-14)', async () => {
    store.portfolioAccounts['account-123'].balances.COP = 1000000;

    await assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'COP',
      newBalance: 1200000,
      date: '2026-08-20',
    });

    const transaction = writtenTransactions()[0];
    expect(transaction).not.toHaveProperty('realizedFxAvailability');
    expect(transaction.acquisitionRate).toBeNull();
  });

  it('un ajuste que no cambia el saldo se rechaza en lugar de escribir un movimiento vacío', async () => {
    await expect(assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 1000,
      date: '2026-08-20',
    })).rejects.toThrow(/no cambia el saldo/i);
  });

  it('un ajuste que dejaría el saldo en negativo se rechaza', async () => {
    await expect(assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: -10,
      date: '2026-08-20',
    })).rejects.toThrow(/negativo/i);
  });

  it('deja el veredicto de conciliación recalculado con el asiento ya escrito (D4)', async () => {
    const result = await assetHandlers.registerBalanceAdjustment(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 1050,
      date: '2026-08-20',
    });

    expect(result.reconciliation).not.toBeNull();
    expect(directUpdates.some(({ payload }) =>
      Object.keys(payload).includes('balanceReconciliation.USD'))).toBe(true);
  });
});

// ============================================================================

describe('addPortfolioAccount — los saldos iniciales nacen como movimiento (AC-5, RN-2.6-C)', () => {
  it('cada saldo inicial deja su asiento de apertura con su tipo de cambio', async () => {
    const result = await accountHandlers.addPortfolioAccount(context, {
      name: 'Nueva cuenta',
      balances: { USD: 500 },
      balanceRates: { USD: 3950 },
    });

    expect(result.openingEntries).toBe(1);

    const opening = writtenTransactions().find((doc) => doc.type === 'cash_adjustment');
    expect(opening).toMatchObject({
      adjustmentReason: 'opening',
      adjustmentDelta: 500,
      currency: 'USD',
      acquisitionRate: 3950,
      acquisitionCost: 1975000,
      acquisitionRateSource: 'user',
    });
  });

  it('la cuenta nace con la base de costo de sus saldos iniciales', async () => {
    await accountHandlers.addPortfolioAccount(context, {
      name: 'Nueva cuenta',
      balances: { USD: 500 },
      balanceRates: { USD: 3950 },
    });

    const account = mockBatch.set.mock.calls
      .map(([, data]) => data)
      .find((data) => data.name === 'Nueva cuenta');

    expect(account.balances).toEqual({ USD: 500 });
    expect(account.balanceCostBasis.USD).toMatchObject({ cost: 1975000, status: 'known' });
  });

  it('sin tasa declarada se usa la de mercado de la fecha de creación', async () => {
    await accountHandlers.addPortfolioAccount(context, {
      name: 'Nueva cuenta',
      balances: { USD: 500 },
    });

    const opening = writtenTransactions().find((doc) => doc.type === 'cash_adjustment');
    expect(opening.acquisitionRate).toBe(4300);
    expect(opening.acquisitionRateSource).toBe('market-date');
  });

  it('sin tasa disponible la cuenta se crea igual, con la base declarada ausente (D9, RN-13)', async () => {
    historicalRateService.getCrossRate.mockResolvedValue(null);

    const result = await accountHandlers.addPortfolioAccount(context, {
      name: 'Nueva cuenta',
      balances: { USD: 500 },
    });

    expect(result.success).toBe(true);

    const opening = writtenTransactions().find((doc) => doc.type === 'cash_adjustment');
    expect(opening.acquisitionRate).toBeNull();
    expect(opening.acquisitionCost).toBeNull();

    const account = mockBatch.set.mock.calls
      .map(([, data]) => data)
      .find((data) => data.name === 'Nueva cuenta');
    expect(account.balanceCostBasis.USD.status).toBe('unknown');
  });

  it('una cuenta sin saldos iniciales no escribe ningún asiento', async () => {
    const result = await accountHandlers.addPortfolioAccount(context, { name: 'Vacía' });

    expect(result.openingEntries).toBe(0);
    expect(writtenTransactions().some((doc) => doc.type === 'cash_adjustment')).toBe(false);
  });
});

// ============================================================================

describe('updatePortfolioAccount — editar la cuenta deja de mover saldos (RN-06, D10)', () => {
  it('cambiar el monto de una divisa guardada se rechaza y señala el ajuste', async () => {
    await expect(accountHandlers.updatePortfolioAccount(context, {
      accountId: 'account-123',
      updates: { balances: { USD: 1500, EUR: 400 } },
    })).rejects.toThrow(/ajuste/i);
  });

  it('el resto de la cuenta se sigue pudiendo editar', async () => {
    const result = await accountHandlers.updatePortfolioAccount(context, {
      accountId: 'account-123',
      updates: { name: 'Otro nombre' },
    });

    expect(result.success).toBe(true);
  });

  it('una divisa nueva nace con su asiento de apertura', async () => {
    const result = await accountHandlers.updatePortfolioAccount(context, {
      accountId: 'account-123',
      updates: { balances: { USD: 1000, EUR: 400, GBP: 200 }, balanceRates: { GBP: 5200 } },
    });

    expect(result.openingEntries).toBe(1);

    const opening = writtenTransactions().find((doc) => doc.type === 'cash_adjustment');
    expect(opening).toMatchObject({
      adjustmentReason: 'opening',
      currency: 'GBP',
      adjustmentDelta: 200,
      acquisitionRate: 5200,
      portfolioAccountId: 'account-123',
    });
  });

  it('quitar una divisa con saldo sigue bloqueado (RN-11, no-regresión de 2.5)', async () => {
    await expect(accountHandlers.updatePortfolioAccount(context, {
      accountId: 'account-123',
      updates: { balances: { USD: 1000 } },
    })).rejects.toThrow(/saldo/i);
  });
});

// ============================================================================

describe('updatePortfolioAccountBalance — la acción directa deja su asiento (D11)', () => {
  it('mover el saldo escribe su movimiento', async () => {
    const result = await accountHandlers.updatePortfolioAccountBalance(context, {
      accountId: 'account-123',
      currency: 'USD',
      amount: 100,
      operation: 'add',
    });

    expect(result.transactionId).not.toBeNull();

    const transaction = writtenTransactions()[0];
    expect(transaction).toMatchObject({
      type: 'cash_adjustment',
      adjustmentReason: 'manual',
      adjustmentDelta: 100,
      currency: 'USD',
    });
  });

  it('un movimiento de cero no escribe nada', async () => {
    const result = await accountHandlers.updatePortfolioAccountBalance(context, {
      accountId: 'account-123',
      currency: 'USD',
      amount: 1000,
      operation: 'set',
    });

    expect(result.transactionId).toBeNull();
    expect(mockBatch.commit).not.toHaveBeenCalled();
  });
});

// ============================================================================

describe('deleteAsset — borrar un activo devuelve su efectivo (AC-2, D12)', () => {
  beforeEach(() => {
    store.assets = {
      'asset-1': {
        portfolioAccount: 'account-123',
        currency: 'USD',
        name: 'META',
      },
    };
    store.transactions = {
      'tx-buy': {
        assetId: 'asset-1',
        type: 'buy',
        amount: 10,
        price: 40,
        commission: 0,
        currency: 'USD',
        date: '2026-03-12T10:00:00.000Z',
        releasedCost: 1600000,
        portfolioAccountId: 'account-123',
      },
    };
  });

  it('el saldo recupera lo que la compra consumió', async () => {
    await assetHandlers.deleteAsset(context, { assetId: 'asset-1' });

    const update = batchAccountUpdates()[0];
    expect(update['balances.USD']).toBe(1400);
  });

  it('el costo vuelve tal como salió, sin dejar el saldo indeterminado', async () => {
    await assetHandlers.deleteAsset(context, { assetId: 'asset-1' });

    const update = batchAccountUpdates()[0];
    expect(update['balanceCostBasis.USD']).toMatchObject({
      cost: 5600000,
      status: 'known',
    });
  });

  it('borrar un activo sin transacciones que muevan caja no toca el saldo', async () => {
    store.transactions = {};

    await assetHandlers.deleteAsset(context, { assetId: 'asset-1' });

    expect(mockBatch.update).not.toHaveBeenCalled();
  });
});
