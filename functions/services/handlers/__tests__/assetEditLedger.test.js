/**
 * Editar un activo no puede separar el saldo de su historial (RN-06).
 *
 * El caso que lo rompía en producción: un activo con una venta parcial. Desde
 * esa venta, `asset.units` (lo que se tiene hoy) y `amount` de la compra (lo
 * que se compró) dejan de ser el mismo número. La edición cobraba el efectivo
 * por la diferencia de unidades del **activo** y, al parchear la compra,
 * escribía en ella la tenencia: cuando ésta ya coincidía con lo comprado, el
 * documento no cambiaba, el libro mayor no se movía y el saldo guardado se iba
 * solo. El aviso de "no conciliado" que salía después no había manera de
 * cerrarlo, porque el ajuste movía los dos saldos a la vez.
 *
 * Lo que se fija aquí es la invariante, no la implementación: **lo que el saldo
 * se mueve es exactamente lo que cambia en el libro mayor**, pase lo que pase
 * con las unidades.
 *
 * @module handlers/__tests__/assetEditLedger.test
 */

const store = {
  portfolioAccounts: {},
  userData: {},
  transactions: {},
  assets: {},
};

/**
 * Aplica un fragmento de actualización al almacén, entendiendo las rutas con
 * punto (`balances.USD`) igual que Firestore.
 *
 * @param {Object} target - Documento sobre el que se escribe
 * @param {Object} payload - Fragmento de actualización
 */
const applyUpdate = (target, payload) => {
  for (const [path, value] of Object.entries(payload)) {
    const keys = path.split('.');
    let node = target;

    while (keys.length > 1) {
      const key = keys.shift();
      node[key] = node[key] || {};
      node = node[key];
    }

    node[keys[0]] = value;
  }
};

const mockBatch = {
  set: jest.fn((ref, data) => {
    if (ref?.collectionName) store[ref.collectionName][ref.id] = data;
  }),
  update: jest.fn((ref, payload) => {
    const doc = store[ref?.collectionName]?.[ref?.id];
    if (doc) applyUpdate(doc, payload);
  }),
  delete: jest.fn(),
  commit: jest.fn().mockResolvedValue(undefined),
};

let generatedDocId = 0;

const makeDocRef = (collectionName, docId) => ({
  id: docId,
  collectionName,
  get: jest.fn(async () => {
    const data = store[collectionName]?.[docId];
    return { exists: data !== undefined, id: docId, data: () => data, ref: makeDocRef(collectionName, docId) };
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

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = { firestore: jest.fn(() => mockDb) };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockDb,
  FieldValue: { serverTimestamp: () => 'SERVER_TIMESTAMP', delete: () => 'FIELD_DELETE' },
}));

jest.mock('../../cacheInvalidationService', () => ({
  invalidatePerformanceCache: jest.fn().mockResolvedValue(undefined),
  invalidateDistributionCache: jest.fn(),
}));

jest.mock('../../portfolioDistributionService', () => ({ invalidateDistributionCache: jest.fn() }));
jest.mock('../../helpers/subscriptionValidator', () => ({
  validateQuantityLimit: jest.fn().mockResolvedValue(undefined),
}));
jest.mock('../../financeQuery', () => ({ getQuotes: jest.fn().mockResolvedValue([]) }));
jest.mock('../../../utils/logoGenerator', () => ({ generateLogoUrl: jest.fn() }));
jest.mock('../../../utils/performanceStaleMarker', () => ({ checkAndMarkStaleIfRetroactive: jest.fn() }));
jest.mock('../../historicalRateService', () => ({
  getCrossRate: jest.fn().mockResolvedValue({ rate: 4300, rateDate: '2026-11-04' }),
  getRateForDate: jest.fn().mockResolvedValue({ rate: 4300, rateDate: '2026-11-04' }),
}));

const assetHandlers = require('../assetHandlers');
const { projectBalanceLedger } = require('../../helpers/balanceLedger');

const context = { auth: { uid: 'user-123' } };

/** Diferencia entre el saldo guardado y el que proyectan sus movimientos */
const drift = (currency = 'USD') => projectBalanceLedger({
  transactions: Object.entries(store.transactions).map(([id, data]) => ({ id, ...data })),
  currency,
  referenceCurrency: 'USD',
  balance: store.portfolioAccounts['account-123'].balances[currency],
}).reconciliation.difference;

beforeEach(() => {
  jest.clearAllMocks();
  generatedDocId = 0;

  store.portfolioAccounts = {
    'account-123': {
      userId: 'user-123',
      name: 'XTB',
      balances: { USD: 10 },
    },
  };
  store.userData = { 'user-123': { defaultCurrency: 'USD' } };
  store.assets = {};
  store.transactions = {};
});

/** Fragmentos de actualización que el batch preparó sobre la cuenta */
const balanceUpdates = () => mockBatch.update.mock.calls
  .map(([, payload]) => payload)
  .filter((payload) => Object.keys(payload).some((key) => key.startsWith('balances.')));

/** Fragmento con el que se corrigió la compra */
const purchaseUpdate = () => mockBatch.update.mock.calls
  .map(([, payload]) => payload)
  .find((payload) => payload.amount !== undefined || payload.price !== undefined);

describe('updateAsset — el saldo se mueve lo que se mueve su compra (RN-06)', () => {
  describe('un activo con una venta parcial', () => {
    beforeEach(() => {
      // Se compraron 0,2786 a 387,62 y se vendieron 0,0096: hoy quedan 0,269.
      store.assets = {
        'asset-msft': {
          portfolioAccount: 'account-123',
          currency: 'USD',
          name: 'MSFT',
          units: 0.269,
          unitValue: 387.62,
          commission: 0,
          acquisitionDate: '2026-06-11T16:09:55.273Z',
        },
      };
      store.transactions = {
        'tx-income': {
          type: 'cash_income',
          amount: 200,
          currency: 'USD',
          portfolioAccountId: 'account-123',
          date: '2026-06-01T10:00:00.000Z',
        },
        'tx-buy': {
          assetId: 'asset-msft',
          type: 'buy',
          amount: 0.2786,
          price: 387.62,
          commission: 0,
          currency: 'USD',
          portfolioAccountId: 'account-123',
          date: '2026-06-11T16:09:55.273Z',
        },
        'tx-sell': {
          assetId: 'asset-msft',
          type: 'sell',
          amount: 0.0096,
          price: 486.48,
          commission: 0,
          currency: 'USD',
          portfolioAccountId: 'account-123',
          date: '2026-08-03T10:00:00.000Z',
        },
      };

      // El saldo cuadra con su historial antes de tocar nada.
      store.portfolioAccounts['account-123'].balances.USD =
        projectBalanceLedger({
          transactions: Object.entries(store.transactions).map(([id, data]) => ({ id, ...data })),
          currency: 'USD',
          referenceCurrency: 'USD',
          balance: 0,
        }).reconciliation.ledgerBalance;
    });

    it('corrige las unidades COMPRADAS, no las escribe con la tenencia de hoy', async () => {
      await assetHandlers.updateAsset(context, {
        assetId: 'asset-msft',
        updates: { units: 0.2786 },
      });

      // Tener 0,0096 más de lo anotado significa haber comprado 0,0096 más.
      // Escribir 0,2786 —la tenencia— dejaría la compra como estaba y el cobro
      // sin movimiento que lo explique.
      expect(purchaseUpdate().amount).toBe(0.2882);
    });

    it('cobra al saldo exactamente lo que crece la compra', async () => {
      const balanceBefore = store.portfolioAccounts['account-123'].balances.USD;

      const result = await assetHandlers.updateAsset(context, {
        assetId: 'asset-msft',
        updates: { units: 0.2786 },
      });

      // 0,0096 × 387,62 = 3,721152
      expect(result.balanceAdjustment).toBe(-3.721152);
      expect(balanceUpdates()[0]['balances.USD']).toBe(Number((balanceBefore - 3.721152).toFixed(8)));
    });

    it('no deja ninguna deriva detrás', async () => {
      expect(drift()).toBe(0);

      await assetHandlers.updateAsset(context, {
        assetId: 'asset-msft',
        updates: { units: 0.2786 },
      });

      // El batch ya escribió las dos cosas en el almacén: el saldo y la compra.
      expect(drift()).toBe(0);
    });

    it('bajar la tenencia devuelve el efectivo y encoge la compra', async () => {
      const balanceBefore = store.portfolioAccounts['account-123'].balances.USD;

      await assetHandlers.updateAsset(context, {
        assetId: 'asset-msft',
        updates: { units: 0.2594 },
      });

      expect(purchaseUpdate().amount).toBe(0.269);
      expect(balanceUpdates()[0]['balances.USD']).toBeGreaterThan(balanceBefore);
      expect(drift()).toBe(0);
    });
  });

  describe('un activo sin ventas', () => {
    beforeEach(() => {
      store.assets = {
        'asset-aapl': {
          portfolioAccount: 'account-123',
          currency: 'USD',
          name: 'AAPL',
          units: 10,
          unitValue: 20,
          commission: 0,
          acquisitionDate: '2026-06-11T16:09:55.273Z',
        },
      };
      store.transactions = {
        'tx-income': {
          type: 'cash_income',
          amount: 300,
          currency: 'USD',
          portfolioAccountId: 'account-123',
          date: '2026-06-01T10:00:00.000Z',
        },
        'tx-buy': {
          assetId: 'asset-aapl',
          type: 'buy',
          amount: 10,
          price: 20,
          commission: 0,
          currency: 'USD',
          portfolioAccountId: 'account-123',
          date: '2026-06-11T16:09:55.273Z',
        },
      };
      // 300 que entraron menos 200 de la compra: el saldo cuadra con su historial.
      store.portfolioAccounts['account-123'].balances.USD = 100;
    });

    it('se comporta como siempre: la compra pasa a tener las unidades nuevas', async () => {
      await assetHandlers.updateAsset(context, {
        assetId: 'asset-aapl',
        updates: { units: 12 },
      });

      expect(purchaseUpdate().amount).toBe(12);
      expect(balanceUpdates()[0]['balances.USD']).toBe(60);
    });

    it('rechaza la corrección que no alcanza a pagarse', async () => {
      await expect(assetHandlers.updateAsset(context, {
        assetId: 'asset-aapl',
        updates: { units: 20 },
      })).rejects.toThrow(/Saldo insuficiente/i);

      expect(mockBatch.commit).not.toHaveBeenCalled();
    });

    it('subir el precio de compra también se cobra', async () => {
      await assetHandlers.updateAsset(context, {
        assetId: 'asset-aapl',
        updates: { unitValue: 21 },
      });

      expect(purchaseUpdate().price).toBe(21);
      expect(balanceUpdates()[0]['balances.USD']).toBe(90);
      expect(drift()).toBe(0);
    });
  });

  describe('un activo sin compra asociada', () => {
    beforeEach(() => {
      store.assets = {
        'asset-huerfano': {
          portfolioAccount: 'account-123',
          currency: 'USD',
          name: 'HUERFANO',
          units: 10,
          unitValue: 20,
          commission: 0,
          acquisitionDate: '2026-06-11T16:09:55.273Z',
        },
      };
      store.transactions = {};
      store.portfolioAccounts['account-123'].balances.USD = 100;
    });

    it('no mueve el saldo: no hay movimiento que pueda explicar el cobro', async () => {
      const result = await assetHandlers.updateAsset(context, {
        assetId: 'asset-huerfano',
        updates: { units: 12 },
      });

      expect(result.balanceAdjustment).toBe(0);
      expect(balanceUpdates()).toHaveLength(0);
      expect(store.portfolioAccounts['account-123'].balances.USD).toBe(100);
    });
  });
});
