/**
 * HU 2.1 — Tests del ingreso de efectivo con tipo de cambio de su fecha.
 *
 * @module handlers/__tests__/assetHandlers.cashCostBasis.test
 * @see platform-docs/stories/2.1-base-costo-saldo-efectivo/refinamiento.md (T5, T17)
 */

const { HttpsError } = require('firebase-functions/v2/https');

// ============================================================================
// Firestore falso enrutado por colección
// ============================================================================

const store = {
  portfolioAccounts: {},
  userData: {},
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
  get: jest.fn().mockImplementation(async () => {
    const data = store[collectionName]?.[docId];
    return {
      exists: data !== undefined,
      id: docId,
      data: () => data,
    };
  }),
  set: jest.fn().mockResolvedValue(undefined),
  update: jest.fn().mockResolvedValue(undefined),
});

const makeCollection = (collectionName) => ({
  doc: (docId) => makeDocRef(collectionName, docId ?? `generated-${++generatedDocId}`),
  where: jest.fn().mockReturnThis(),
  limit: jest.fn().mockReturnThis(),
  get: jest.fn().mockResolvedValue({ empty: true, docs: [] }),
});

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

jest.mock('../../cacheInvalidationService', () => ({
  invalidatePerformanceCache: jest.fn().mockResolvedValue(undefined),
  invalidateDistributionCache: jest.fn(),
}));

jest.mock('../../portfolioDistributionService', () => ({
  invalidateDistributionCache: jest.fn(),
}));

jest.mock('../../financeQuery', () => ({
  getQuotes: jest.fn().mockResolvedValue([]),
}));

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

// ============================================================================
// Fixtures
// ============================================================================

const context = { auth: { uid: 'user-123' } };

const basePayload = {
  portfolioAccountId: 'account-123',
  type: 'cash_income',
  amount: 1000,
  currency: 'USD',
  date: '2026-03-12',
};

/** Última actualización de cuenta que se envió al batch */
const lastAccountUpdate = () => mockBatch.update.mock.calls[mockBatch.update.mock.calls.length - 1][1];

/** Documento de transacción que se envió al batch */
const writtenTransaction = () => mockBatch.set.mock.calls[0][1];

describe('addCashTransaction — base de costo y tipo de cambio (HU 2.1)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBatch.commit.mockResolvedValue(undefined);

    store.portfolioAccounts = {
      'account-123': { userId: 'user-123', balances: { USD: 0 } },
    };
    store.userData = {
      'user-123': { defaultCurrency: 'COP' },
    };
  });

  describe('Escenario 1 — el ingreso registra a qué tasa entró el dinero', () => {
    it('propone la tasa de la fecha del movimiento, no la de hoy', async () => {
      historicalRateService.getCrossRate.mockResolvedValue({
        rate: 4000, rateDate: '2026-03-12', source: 'cache',
      });

      await assetHandlers.addCashTransaction(context, basePayload);

      expect(historicalRateService.getCrossRate).toHaveBeenCalledWith('USD', 'COP', '2026-03-12');
    });

    it('deja el saldo con un costo igual al monto por la tasa', async () => {
      historicalRateService.getCrossRate.mockResolvedValue({
        rate: 4000, rateDate: '2026-03-12', source: 'cache',
      });

      const result = await assetHandlers.addCashTransaction(context, basePayload);

      expect(result.success).toBe(true);
      expect(lastAccountUpdate()).toMatchObject({
        'balances.USD': 1000,
        'balanceCostBasis.USD': expect.objectContaining({
          cost: 4000000,
          referenceCurrency: 'COP',
          status: 'known',
        }),
      });
    });

    it('corrige dollarPriceToDate: antes guardaba 1 para un ingreso en dólares', async () => {
      historicalRateService.getCrossRate.mockResolvedValue({
        rate: 4000, rateDate: '2026-03-12', source: 'cache',
      });

      await assetHandlers.addCashTransaction(context, basePayload);

      const tx = writtenTransaction();
      expect(tx.dollarPriceToDate).toBe(4000);
      expect(tx.defaultCurrencyForAdquisitionDollar).toBe('COP');
      expect(tx.acquisitionRate).toBe(4000);
      expect(tx.acquisitionCost).toBe(4000000);
      expect(tx.referenceCurrency).toBe('COP');
    });

    it('respeta la tasa que corrige el usuario por encima de la propuesta', async () => {
      historicalRateService.getRateForDate.mockResolvedValue({
        rate: 4000, rateDate: '2026-03-12', source: 'cache',
      });

      await assetHandlers.addCashTransaction(context, { ...basePayload, exchangeRate: 4150 });

      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
      expect(writtenTransaction().acquisitionRate).toBe(4150);
      expect(writtenTransaction().acquisitionRateSource).toBe('user');
      expect(lastAccountUpdate()['balanceCostBasis.USD'].cost).toBe(4150000);
    });
  });

  describe('Escenario 2 — la moneda de referencia no pregunta nada', () => {
    beforeEach(() => {
      store.portfolioAccounts['account-123'].balances = { COP: 0 };
    });

    it('no consulta ningún tipo de cambio al ingresar la moneda de referencia', async () => {
      await assetHandlers.addCashTransaction(context, {
        ...basePayload,
        currency: 'COP',
        amount: 2500000,
      });

      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
    });

    it('no le crea base de costo al saldo en la moneda de referencia', async () => {
      await assetHandlers.addCashTransaction(context, {
        ...basePayload,
        currency: 'COP',
        amount: 2500000,
      });

      expect(lastAccountUpdate()).toEqual({ 'balances.COP': 2500000 });
    });
  });

  describe('Escenario 3 — no hay tipo de cambio para la fecha (RN-05)', () => {
    it('no guarda el movimiento y pide la tasa', async () => {
      historicalRateService.getCrossRate.mockResolvedValue(null);

      await expect(
        assetHandlers.addCashTransaction(context, { ...basePayload, date: '2026-12-25' })
      ).rejects.toThrow(HttpsError);

      expect(mockBatch.commit).not.toHaveBeenCalled();
      expect(mockBatch.set).not.toHaveBeenCalled();
    });

    it('el mensaje nombra la divisa, la referencia y la fecha', async () => {
      historicalRateService.getCrossRate.mockResolvedValue(null);

      await expect(
        assetHandlers.addCashTransaction(context, { ...basePayload, date: '2026-12-25' })
      ).rejects.toThrow(/USD.*COP.*2026-12-25/);
    });

    it('acepta el movimiento si el usuario escribe la tasa', async () => {
      historicalRateService.getCrossRate.mockResolvedValue(null);
      historicalRateService.getRateForDate.mockResolvedValue(null);

      const result = await assetHandlers.addCashTransaction(context, {
        ...basePayload,
        date: '2026-12-25',
        exchangeRate: 4400,
      });

      expect(result.success).toBe(true);
      expect(lastAccountUpdate()['balanceCostBasis.USD'].cost).toBe(4400000);
    });

    it('ignora una tasa no positiva y vuelve a exigirla', async () => {
      historicalRateService.getCrossRate.mockResolvedValue(null);

      await expect(
        assetHandlers.addCashTransaction(context, { ...basePayload, exchangeRate: 0 })
      ).rejects.toThrow(HttpsError);
    });
  });

  describe('Escenario 5 — ingresos sucesivos promedian (RN-02)', () => {
    it('acumula costo sobre el saldo existente', async () => {
      store.portfolioAccounts['account-123'] = {
        userId: 'user-123',
        balances: { USD: 1000 },
        balanceCostBasis: {
          USD: { cost: 4000000, referenceCurrency: 'COP', status: 'known' },
        },
      };

      historicalRateService.getCrossRate.mockResolvedValue({
        rate: 4400, rateDate: '2026-04-10', source: 'cache',
      });

      await assetHandlers.addCashTransaction(context, { ...basePayload, date: '2026-04-10' });

      // 2.000 USD con 8.400.000 COP → promedio 4.200
      expect(lastAccountUpdate()['balances.USD']).toBe(2000);
      expect(lastAccountUpdate()['balanceCostBasis.USD'].cost).toBe(8400000);
    });
  });

  describe('Egresos', () => {
    beforeEach(() => {
      store.portfolioAccounts['account-123'] = {
        userId: 'user-123',
        balances: { USD: 2000 },
        balanceCostBasis: {
          USD: { cost: 8400000, referenceCurrency: 'COP', status: 'known' },
        },
      };
    });

    it('retira costo a la tasa promedio, sin exigir tasa del día', async () => {
      historicalRateService.getCrossRate.mockResolvedValue(null);
      historicalRateService.getRateForDate.mockResolvedValue({
        rate: 4300, rateDate: '2026-05-02', source: 'cache',
      });

      await assetHandlers.addCashTransaction(context, {
        ...basePayload,
        type: 'cash_expense',
        amount: 500,
        date: '2026-05-02',
      });

      expect(lastAccountUpdate()['balances.USD']).toBe(1500);
      expect(lastAccountUpdate()['balanceCostBasis.USD'].cost).toBe(6300000); // 1.500 × 4.200
    });

    it('sigue rechazando un egreso sin saldo suficiente', async () => {
      await expect(
        assetHandlers.addCashTransaction(context, {
          ...basePayload,
          type: 'cash_expense',
          amount: 5000,
        })
      ).rejects.toThrow('Saldo insuficiente');
    });
  });

  describe('Escenario 7 — sin exposición cambiaria, sin ruido (RN-14)', () => {
    beforeEach(() => {
      store.userData['user-123'] = { defaultCurrency: 'USD' };
    });

    it('un usuario con referencia en dólares no consulta tasas ni gana campos', async () => {
      await assetHandlers.addCashTransaction(context, basePayload);

      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
      expect(historicalRateService.getRateForDate).not.toHaveBeenCalled();
      expect(lastAccountUpdate()).toEqual({ 'balances.USD': 1000 });
      expect(writtenTransaction().dollarPriceToDate).toBe(1);
    });
  });

  describe('Validaciones existentes', () => {
    it('sigue exigiendo los campos obligatorios', async () => {
      await expect(
        assetHandlers.addCashTransaction(context, { ...basePayload, currency: undefined })
      ).rejects.toThrow('requeridos');
    });

    it('sigue rechazando un tipo de movimiento desconocido', async () => {
      await expect(
        assetHandlers.addCashTransaction(context, { ...basePayload, type: 'cash_transfer' })
      ).rejects.toThrow('cash_income o cash_expense');
    });

    it('sigue validando la propiedad de la cuenta', async () => {
      store.portfolioAccounts['account-123'].userId = 'otro-usuario';

      await expect(
        assetHandlers.addCashTransaction(context, basePayload)
      ).rejects.toThrow('No tienes permiso');
    });
  });
});
