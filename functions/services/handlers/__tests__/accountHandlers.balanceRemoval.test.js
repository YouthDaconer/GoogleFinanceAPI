/**
 * HU 2.5 (RN-11) — Tests del guardarraíl que impide perder base de costo.
 *
 * Una divisa solo se elimina de una cuenta con saldo en cero. El guardarraíl
 * vive en el servidor porque los dos botones de borrado del cliente pasan por
 * este handler; comprobarlo aquí es comprobarlo para los dos.
 *
 * @module handlers/__tests__/accountHandlers.balanceRemoval.test
 * @see platform-docs/stories/2.5-diferencia-cambio-salidas-efectivo/refinamiento.md (T17, D9)
 */

const { HttpsError } = require('firebase-functions/v2/https');

// ============================================================================
// Firestore falso
// ============================================================================

const store = { portfolioAccounts: {} };

/** Actualizaciones aplicadas a `portfolioAccounts/{id}` */
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
    get: jest.fn(async () => ({ empty: true, docs: [] })),
  };
  return query;
};

const makeCollection = (collectionName) => {
  const collection = makeQuery();
  collection.doc = (docId) => makeDocRef(collectionName, docId);
  return collection;
};

/**
 * HU 2.6: `updatePortfolioAccount` escribe en batch, porque una divisa nueva
 * nace con su asiento de apertura en la misma operación. Las actualizaciones de
 * cuenta se recogen igual, vengan del batch o de un `update` suelto.
 */
const mockBatch = {
  set: jest.fn(),
  update: jest.fn((_ref, payload) => { accountUpdates.push(payload); }),
  delete: jest.fn(),
  commit: jest.fn().mockResolvedValue(undefined),
};

const mockFirestore = {
  collection: jest.fn((name) => makeCollection(name)),
  doc: jest.fn((path) => makeDocRef(...path.split('/'))),
  batch: jest.fn(() => mockBatch),
};

const mockDeleteSentinel = Symbol('FieldValue.delete');

jest.mock('../../firebaseAdmin', () => {
  const mockAdmin = { firestore: jest.fn(() => mockFirestore) };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockFirestore,
  FieldValue: {
    serverTimestamp: () => 'SERVER_TIMESTAMP',
    delete: () => mockDeleteSentinel,
  },
}));

jest.mock('../../portfolioDistributionService', () => ({
  invalidateDistributionCache: jest.fn(),
}));

jest.mock('../../helpers/subscriptionValidator', () => ({
  validateQuantityLimit: jest.fn().mockResolvedValue(undefined),
}));

// HU 2.6: una divisa que nace resuelve la tasa de su asiento de apertura.
jest.mock('../../historicalRateService', () => ({
  getCrossRate: jest.fn().mockResolvedValue({ rate: 4300, rateDate: '2026-08-29', source: 'cache' }),
  getRateForDate: jest.fn().mockResolvedValue({ rate: 4300, rateDate: '2026-08-29', source: 'cache' }),
}));

const accountHandlers = require('../accountHandlers');

// ============================================================================
// Fixtures
// ============================================================================

const context = { auth: { uid: 'user-123' } };

const lastUpdate = () => accountUpdates[accountUpdates.length - 1];

beforeEach(() => {
  jest.clearAllMocks();
  accountUpdates.length = 0;

  store.portfolioAccounts = {
    'account-123': {
      userId: 'user-123',
      name: 'Interactive Brokers',
      balances: { USD: 500, COP: 1000000 },
      balanceCostBasis: {
        USD: { cost: 2000000, referenceCurrency: 'COP', status: 'known' },
      },
    },
  };
});

// ============================================================================

describe('updatePortfolioAccount — una divisa solo se elimina con saldo en cero (RN-11)', () => {
  describe('Escenario 7 — la divisa todavía tiene saldo', () => {
    it('rechaza la eliminación en lugar de perder la base de costo', async () => {
      await expect(
        accountHandlers.updatePortfolioAccount(context, {
          accountId: 'account-123',
          updates: { balances: { COP: 1000000 } },
        })
      ).rejects.toThrow(HttpsError);
    });

    it('el error nombra la divisa y el monto que estorba', async () => {
      await expect(
        accountHandlers.updatePortfolioAccount(context, {
          accountId: 'account-123',
          updates: { balances: { COP: 1000000 } },
        })
      ).rejects.toThrow(/500\.00 USD/);
    });

    it('ofrece la salida en el propio mensaje, no deja al usuario sin camino', async () => {
      await expect(
        accountHandlers.updatePortfolioAccount(context, {
          accountId: 'account-123',
          updates: { balances: { COP: 1000000 } },
        })
      ).rejects.toThrow(/Retira o convierte/i);
    });

    it('no escribe nada cuando rechaza', async () => {
      await accountHandlers
        .updatePortfolioAccount(context, {
          accountId: 'account-123',
          updates: { balances: { COP: 1000000 } },
        })
        .catch(() => {});

      expect(accountUpdates).toHaveLength(0);
    });

    it('un saldo negativo bloquea igual: lo que importa es que no esté en cero', async () => {
      store.portfolioAccounts['account-123'].balances = { USD: -300, COP: 1000000 };

      await expect(
        accountHandlers.updatePortfolioAccount(context, {
          accountId: 'account-123',
          updates: { balances: { COP: 1000000 } },
        })
      ).rejects.toThrow(/USD/);
    });

    it('bloquea aunque se eliminen varias divisas a la vez', async () => {
      store.portfolioAccounts['account-123'].balances = { USD: 500, EUR: 200, COP: 0 };

      await expect(
        accountHandlers.updatePortfolioAccount(context, {
          accountId: 'account-123',
          updates: { balances: {} },
        })
      ).rejects.toThrow(/USD.*EUR|EUR.*USD/);
    });
  });

  // ==========================================================================
  describe('Escenario 8 — con el saldo en cero la divisa se elimina', () => {
    beforeEach(() => {
      store.portfolioAccounts['account-123'].balances = { USD: 0, COP: 1000000 };
      store.portfolioAccounts['account-123'].balanceCostBasis = {
        USD: { cost: 0, referenceCurrency: 'COP', status: 'known' },
      };
    });

    it('la eliminación procede', async () => {
      const result = await accountHandlers.updatePortfolioAccount(context, {
        accountId: 'account-123',
        updates: { balances: { COP: 1000000 } },
      });

      expect(result.success).toBe(true);
      expect(lastUpdate().balances).toEqual({ COP: 1000000 });
    });

    it('la base de costo se va con la divisa, sin dejar un costo huérfano', async () => {
      await accountHandlers.updatePortfolioAccount(context, {
        accountId: 'account-123',
        updates: { balances: { COP: 1000000 } },
      });

      expect(lastUpdate()['balanceCostBasis.USD']).toBe(mockDeleteSentinel);
    });

    it('un residuo de redondeo por debajo del céntimo no bloquea', async () => {
      store.portfolioAccounts['account-123'].balances = { USD: 0.001, COP: 1000000 };

      const result = await accountHandlers.updatePortfolioAccount(context, {
        accountId: 'account-123',
        updates: { balances: { COP: 1000000 } },
      });

      expect(result.success).toBe(true);
    });
  });

  // ==========================================================================
  describe('No-regresión — el resto de actualizaciones no cambia', () => {
    it('renombrar la cuenta sin tocar los saldos no pasa por el guardarraíl', async () => {
      const result = await accountHandlers.updatePortfolioAccount(context, {
        accountId: 'account-123',
        updates: { name: 'Nuevo nombre' },
      });

      expect(result.success).toBe(true);
      expect(lastUpdate().name).toBe('Nuevo nombre');
      expect(Object.keys(lastUpdate())).not.toContain('balanceCostBasis.USD');
    });

    // HU 2.6 (RN-06, D10): este caso cambió de contrato. Cambiar el importe de un
    // saldo guardado desde la edición de la cuenta lo movía sin dejar movimiento,
    // que es justo el camino que la 2.6 cierra. Ahora se hace con un ajuste.
    it('cambiar el importe de un saldo guardado se rechaza y señala el ajuste (HU 2.6)', async () => {
      await expect(
        accountHandlers.updatePortfolioAccount(context, {
          accountId: 'account-123',
          updates: { balances: { USD: 800, COP: 1000000 } },
        })
      ).rejects.toThrow(/ajuste/i);
    });

    it('añadir una divisa nueva se permite', async () => {
      const result = await accountHandlers.updatePortfolioAccount(context, {
        accountId: 'account-123',
        updates: { balances: { USD: 500, COP: 1000000, EUR: 0 } },
      });

      expect(result.success).toBe(true);
      expect(lastUpdate().balances.EUR).toBe(0);
    });

    it('sigue rechazando la cuenta de otro usuario', async () => {
      store.portfolioAccounts['account-123'].userId = 'otro-usuario';

      await expect(
        accountHandlers.updatePortfolioAccount(context, {
          accountId: 'account-123',
          updates: { name: 'X' },
        })
      ).rejects.toThrow(/permiso/i);
    });
  });

  // ==========================================================================
  describe('findRemovedCurrenciesWithBalance — el predicado, aislado', () => {
    const find = accountHandlers._findRemovedCurrenciesWithBalance;

    it('solo devuelve las divisas que desaparecen y tienen saldo', () => {
      expect(find({ USD: 500, EUR: 0, COP: 100 }, { COP: 100 }))
        .toEqual([{ currency: 'USD', balance: 500 }]);
    });

    it('una divisa que se queda no es candidata, tenga el saldo que tenga', () => {
      expect(find({ USD: 500 }, { USD: 0 })).toEqual([]);
    });

    it('sin `balances` en la actualización no hay nada que vigilar', () => {
      expect(find({ USD: 500 }, undefined)).toEqual([]);
    });
  });
});
