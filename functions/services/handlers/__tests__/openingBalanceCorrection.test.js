/**
 * HU 2.7 — Tests de las dos acciones de la corrección del saldo inicial.
 *
 * Lo que se comprueba es que el recorrido completo —planear, validar y
 * escribir— deja el saldo de hoy exactamente en la cifra que el usuario
 * escribió, sin tocar `balances` por la puerta que 2.6 cerró y sin alterar un
 * solo movimiento posterior.
 *
 * @module handlers/__tests__/openingBalanceCorrection.test
 * @see platform-docs/stories/2.7-ajuste-saldo-registro-o-apertura/refinamiento.md (T11)
 */

// ============================================================================
// Firestore falso, compartido por el plan y la corrección
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
      .map(([id, data]) => ({ id, data: () => data }));

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
  doc: jest.fn((path) => makeDocRef(...path.split('/'))),
  batch: jest.fn(() => mockBatch),
};

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = { firestore: jest.fn(() => mockDb) };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockDb,
  FieldValue: { serverTimestamp: () => 'SERVER_TIMESTAMP', delete: () => 'FIELD_DELETE' },
}));

// --- Dependencias pesadas que esta prueba no ejercita -----------------------

jest.mock('../../cacheInvalidationService', () => ({
  calculateDynamicTTL: jest.fn(),
  invalidatePerformanceCache: jest.fn().mockResolvedValue(undefined),
  invalidateDistributionCache: jest.fn(),
}));
jest.mock('../../portfolioDistributionService', () => ({
  invalidateDistributionCache: jest.fn(),
}));
jest.mock('../../consolidatedReturnsService', () => ({
  getHistoricalReturnsV2: jest.fn(),
  checkConsolidatedDataStatus: jest.fn(),
}));
jest.mock('../../historicalReturnsService', () => ({
  calculateHistoricalReturns: jest.fn(),
  getHistoricalReturnsInternal: jest.fn(),
}));
jest.mock('../../indexHistoryService', () => ({ calculateIndexData: jest.fn() }));
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
const { checkAndMarkStaleIfRetroactive } = require('../../../utils/performanceStaleMarker');
const { correctOpeningBalance } = require('../assetHandlers');
const { getOpeningCorrectionPlan } = require('../queryHandlers');

// ============================================================================
// Fixtures — cuenta IBKR, saldo en dólares, moneda de referencia el peso
// ============================================================================

const context = { auth: { uid: 'user-123' } };

/** Lo que el batch escribió en el documento de la apertura */
const openingWrite = () => {
  const created = mockBatch.set.mock.calls[0];
  if (created) return { creates: true, id: created[0].id, payload: created[1] };

  const patched = mockBatch.update.mock.calls
    .find(([ref]) => ref.id === 'tx-opening');

  return patched ? { creates: false, id: patched[0].id, payload: patched[1] } : null;
};

/** Lo que el batch escribió en el documento de la cuenta */
const accountWrite = () => {
  const call = mockBatch.update.mock.calls.find(([ref]) => ref.id === 'account-123');
  return call ? call[1] : null;
};

beforeEach(() => {
  jest.clearAllMocks();
  generatedDocId = 0;
  mockBatch.commit.mockResolvedValue(undefined);

  store.portfolioAccounts = {
    'account-123': {
      userId: 'user-123',
      name: 'Interactive Brokers',
      balances: { USD: 180 },
      balanceCostBasis: {
        USD: { cost: 726000, referenceCurrency: 'COP', status: 'known' },
      },
      createdAt: '2024-03-15T08:00:00.000Z',
    },
  };
  store.userData = { 'user-123': { defaultCurrency: 'COP' } };

  store.transactions = {
    'tx-opening': {
      userId: 'user-123',
      portfolioAccountId: 'account-123',
      type: 'cash_adjustment',
      adjustmentReason: 'opening',
      adjustmentDelta: 60,
      amount: 60,
      price: 1,
      currency: 'USD',
      date: '2024-03-15T10:00:00.000Z',
      acquisitionRate: 3900,
      acquisitionRateSource: 'market-date',
      acquisitionCost: 234000,
      dollarPriceToDate: 3900,
      referenceCurrency: 'COP',
      costBasisEstimated: false,
    },
    'tx-income': {
      userId: 'user-123',
      portfolioAccountId: 'account-123',
      type: 'cash_income',
      amount: 200,
      price: 1,
      currency: 'USD',
      date: '2026-08-18T10:00:00.000Z',
      acquisitionRate: 3960,
      acquisitionCost: 792000,
      referenceCurrency: 'COP',
    },
    'tx-buy': {
      userId: 'user-123',
      portfolioAccountId: 'account-123',
      type: 'buy',
      assetName: 'VOO',
      amount: 1,
      price: 80,
      commission: 0,
      currency: 'USD',
      date: '2026-09-02T10:00:00.000Z',
      acquisitionRate: 3905,
      referenceCurrency: 'COP',
    },
  };

  historicalRateService.getCrossRate.mockResolvedValue({
    rate: 3900, rateDate: '2024-03-15', source: 'cache',
  });
  historicalRateService.getRateForDate.mockResolvedValue({
    rate: 3900, rateDate: '2024-03-15', source: 'cache',
  });
});

// ============================================================================
// El plan
// ============================================================================

describe('getOpeningCorrectionPlan — lo que el diálogo necesita saber', () => {
  it('devuelve la apertura del saldo con su fecha, su monto y el recorrido', async () => {
    const plan = await getOpeningCorrectionPlan(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(plan.hasOpening).toBe(true);
    expect(plan.openingId).toBe('tx-opening');
    expect(plan.openingDate).toBe('2024-03-15');
    expect(plan.openingAmount).toBe(60);
    // 60 + 200 − 80 = 180
    expect(plan.ledgerBalance).toBe(180);
    expect(plan.movementCountAfter).toBe(2);
    expect(plan.currency).toBe('USD');
    expect(plan.referenceCurrency).toBe('COP');
  });

  it('la fecha para la que proponer la tasa es la de la apertura (RN-2.7-E)', async () => {
    const plan = await getOpeningCorrectionPlan(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(plan.proposedRateDate).toBe('2024-03-15');
    expect(plan.openingRateMissing).toBe(false);
  });

  it('propone la fecha del día anterior cuando el saldo no tiene apertura (AC-6)', async () => {
    delete store.transactions['tx-opening'];

    const plan = await getOpeningCorrectionPlan(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(plan.hasOpening).toBe(false);
    expect(plan.earliestMovementDate).toBe('2026-08-18');
    expect(plan.newOpeningDate).toBe('2026-08-17');
    expect(plan.proposedRateDate).toBe('2026-08-17');
  });

  it('no escribe nada: es una lectura', async () => {
    await getOpeningCorrectionPlan(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    });

    expect(mockDb.batch).not.toHaveBeenCalled();
    expect(mockBatch.set).not.toHaveBeenCalled();
    expect(mockBatch.update).not.toHaveBeenCalled();
  });

  it('rechaza la cuenta de otro usuario', async () => {
    store.portfolioAccounts['account-123'].userId = 'otro-usuario';

    await expect(getOpeningCorrectionPlan(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
    })).rejects.toThrow(/acceso/i);
  });

  it('exige cuenta y divisa', async () => {
    await expect(getOpeningCorrectionPlan(context, { currency: 'USD' }))
      .rejects.toThrow(/requeridos/);
  });
});

// ============================================================================
// La corrección
// ============================================================================

describe('correctOpeningBalance — el saldo de hoy queda en la cifra escrita', () => {
  it('corrige la apertura para llegar al saldo objetivo (AC-4, D3)', async () => {
    const result = await correctOpeningBalance(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 300,
    });

    expect(result.success).toBe(true);
    expect(result.created).toBe(false);
    expect(result.previousOpeningAmount).toBe(60);
    expect(result.newOpeningAmount).toBe(180);
    expect(result.shift).toBe(120);
    expect(result.newBalance).toBe(300);
    expect(result.movementCountAfter).toBe(2);
  });

  it('el asiento de apertura y la cuenta se escriben en el mismo batch (D9)', async () => {
    await correctOpeningBalance(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 300,
    });

    expect(mockDb.batch).toHaveBeenCalledTimes(1);
    expect(mockBatch.commit).toHaveBeenCalledTimes(1);
    expect(openingWrite()).not.toBeNull();
    expect(accountWrite()).not.toBeNull();
  });

  it('el saldo y la base salen del replay, con su origen (D2, RN-06)', async () => {
    await correctOpeningBalance(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 300,
    });

    const update = accountWrite();

    expect(update['balances.USD']).toBe(300);
    expect(update['balanceCostBasis.USD']).toMatchObject({
      status: 'known',
      referenceCurrency: 'COP',
      source: 'ledger-replay',
    });
    expect(update['balanceCostBasis.USD'].updatedAt).toBe('SERVER_TIMESTAMP');
  });

  it('la marca de no conciliado desaparece (AC-10)', async () => {
    store.portfolioAccounts['account-123'].balanceReconciliation = {
      USD: { ledgerBalance: 180, difference: -4.71, status: 'drift' },
    };

    const result = await correctOpeningBalance(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 300,
    });

    expect(result.reconciliation).toMatchObject({ difference: 0, status: 'reconciled' });
    expect(accountWrite()['balanceReconciliation.USD']).toMatchObject({
      difference: 0,
      status: 'reconciled',
      checkedAt: 'SERVER_TIMESTAMP',
    });
  });

  it('deja constancia de cuánto decía la apertura y cuándo se corrigió (AC-8)', async () => {
    await correctOpeningBalance(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 300,
    });

    expect(openingWrite().payload).toMatchObject({
      adjustmentDelta: 180,
      amount: 180,
      openingCorrectedFrom: 60,
      openingCorrectedAt: 'SERVER_TIMESTAMP',
    });
  });

  it('no toca la fecha de la apertura (AC-9, RN-2.7-E)', async () => {
    await correctOpeningBalance(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 300,
    });

    const payload = openingWrite().payload;

    expect(payload).not.toHaveProperty('date');
    expect(payload).not.toHaveProperty('createdAt');
  });

  it('no mueve el saldo por la puerta que 2.6 cerró: sólo el asiento y el replay', async () => {
    await correctOpeningBalance(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 300,
    });

    // Ninguna escritura suelta sobre la cuenta fuera del batch.
    const accountUpdates = mockBatch.update.mock.calls
      .filter(([ref]) => ref.id === 'account-123');

    expect(accountUpdates).toHaveLength(1);
  });

  it('declara el histórico obsoleto con la fecha de la apertura (D10)', async () => {
    await correctOpeningBalance(context, {
      portfolioAccountId: 'account-123',
      currency: 'USD',
      newBalance: 300,
    });

    expect(checkAndMarkStaleIfRetroactive).toHaveBeenCalledWith(
      'user-123',
      '2024-03-15T00:00:00.000Z',
      expect.objectContaining({ transactionType: 'cash_adjustment' })
    );
  });

  describe('el tipo de cambio de la apertura (AC-7, RN-2.7-E)', () => {
    it('reutiliza la tasa que ya tenía y no consulta al proveedor', async () => {
      const result = await correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: 300,
      });

      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
      expect(result.exchangeRate).toBe(3900);
      // 180 USD × 3.900 COP
      expect(openingWrite().payload.acquisitionCost).toBe(702000);
    });

    it('acepta la tasa declarada cuando la apertura la tenía estimada', async () => {
      store.transactions['tx-opening'].costBasisEstimated = true;

      await correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: 300,
        exchangeRate: 4000,
      });

      const payload = openingWrite().payload;

      expect(payload.acquisitionRate).toBe(4000);
      expect(payload.acquisitionRateSource).toBe('user');
      // Declarada por el usuario: deja de ser una estimación.
      expect(payload.costBasisEstimated).toBe(false);
      expect(payload.acquisitionCost).toBe(720000);
    });

    it('rechaza cuando la apertura no tiene tasa y nadie la puede resolver', async () => {
      store.transactions['tx-opening'].acquisitionRate = null;
      historicalRateService.getCrossRate.mockResolvedValue(null);

      await expect(correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: 300,
      })).rejects.toThrow(/tipo de cambio/i);
    });

    it('no pregunta nada sin exposición cambiaria (AC-11, RN-14)', async () => {
      store.userData['user-123'].defaultCurrency = 'USD';

      await correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: 300,
      });

      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
      expect(accountWrite()).not.toHaveProperty('balanceCostBasis.USD');
    });
  });

  describe('el recorrido en negativo se rechaza con su fecha y su mínimo (AC-3)', () => {
    beforeEach(() => {
      store.transactions['tx-withdrawal'] = {
        userId: 'user-123',
        portfolioAccountId: 'account-123',
        type: 'cash_expense',
        amount: 150,
        price: 1,
        currency: 'USD',
        date: '2025-07-12T10:00:00.000Z',
        realizationRate: 4010,
        referenceCurrency: 'COP',
      };
      // 60 − 150 + 200 − 80 = 30
      store.portfolioAccounts['account-123'].balances.USD = 30;
    });

    it('nombra la fecha del conflicto y el saldo mínimo admisible', async () => {
      // Bajar la apertura de 60 a 0 dejaría el 12/07/2025 en −150.
      await expect(correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: -30,
      })).rejects.toThrow(/2025-07-12/);
    });

    it('no escribe nada cuando rechaza', async () => {
      await expect(correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: -30,
      })).rejects.toThrow();

      expect(mockBatch.commit).not.toHaveBeenCalled();
    });

    it('con el mínimo que indica, la corrección pasa', async () => {
      let minimum = null;

      try {
        await correctOpeningBalance(context, {
          portfolioAccountId: 'account-123',
          currency: 'USD',
          newBalance: -30,
        });
      } catch (error) {
        minimum = error.details.minimumBalance;
      }

      expect(minimum).not.toBeNull();

      const result = await correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: minimum,
      });

      expect(result.newBalance).toBe(minimum);
    });
  });

  describe('un saldo sin apertura la recibe (AC-6, RN-2.7-F)', () => {
    beforeEach(() => {
      delete store.transactions['tx-opening'];
      // 200 − 80 = 120
      store.portfolioAccounts['account-123'].balances.USD = 120;
    });

    it('crea el asiento fechado el día anterior al movimiento más antiguo', async () => {
      const result = await correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: 140,
        exchangeRate: 3900,
      });

      expect(result.created).toBe(true);
      expect(result.openingDate).toBe('2026-08-17');

      const written = openingWrite();

      expect(written.creates).toBe(true);
      expect(written.payload).toMatchObject({
        type: 'cash_adjustment',
        adjustmentReason: 'opening',
        adjustmentDelta: 20,
        currency: 'USD',
        userId: 'user-123',
        portfolioAccountId: 'account-123',
      });
      expect(written.payload.date.substring(0, 10)).toBe('2026-08-17');
    });

    it('el saldo resultante cuenta ya con la apertura creada', async () => {
      const result = await correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: 140,
        exchangeRate: 3900,
      });

      expect(result.newBalance).toBe(140);
      expect(accountWrite()['balances.USD']).toBe(140);
    });
  });

  describe('validaciones de entrada', () => {
    it('exige cuenta y divisa', async () => {
      await expect(correctOpeningBalance(context, { currency: 'USD' }))
        .rejects.toThrow(/requeridos/);
    });

    it('exige un saldo correcto numérico', async () => {
      await expect(correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
      })).rejects.toThrow(/saldo correcto/i);
    });

    it('rechaza una corrección que no cambia el saldo del historial', async () => {
      await expect(correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: 180,
      })).rejects.toThrow(/nada que corregir/i);
    });

    it('rechaza la cuenta de otro usuario', async () => {
      store.portfolioAccounts['account-123'].userId = 'otro-usuario';

      await expect(correctOpeningBalance(context, {
        portfolioAccountId: 'account-123',
        currency: 'USD',
        newBalance: 300,
      })).rejects.toThrow(/permiso/i);
    });
  });
});
