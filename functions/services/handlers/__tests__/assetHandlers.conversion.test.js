/**
 * HU 2.2 — Tests de la conversión de divisa dentro de una misma cuenta.
 *
 * @module handlers/__tests__/assetHandlers.conversion.test
 * @see platform-docs/stories/2.2-conversion-divisa-en-cuenta/refinamiento.md (T1, T11)
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

/** Escenario 1: 4.000.000 COP a USD a 1/4000, con el peso como referencia */
const copToUsd = {
  portfolioAccountId: 'account-123',
  fromCurrency: 'COP',
  toCurrency: 'USD',
  amount: 4000000,
  conversionRate: 0.00025,
  date: '2026-03-12',
};

/** Última actualización de cuenta que se envió al batch */
const accountUpdate = () => mockBatch.update.mock.calls[mockBatch.update.mock.calls.length - 1][1];

/** Documento de transacción que se envió al batch */
const writtenTransaction = () => mockBatch.set.mock.calls[0][1];

describe('convertAccountCurrency — conversión de divisa en cuenta (HU 2.2)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBatch.commit.mockResolvedValue(undefined);

    store.portfolioAccounts = {
      'account-123': {
        userId: 'user-123',
        balances: { COP: 5000000 },
      },
    };
    store.userData = {
      'user-123': { defaultCurrency: 'COP' },
    };

    // Tasa vigente de la referencia contra el dólar, para `dollarPriceToDate`.
    historicalRateService.getRateForDate.mockResolvedValue({
      rate: 4000, rateDate: '2026-03-12', source: 'cache',
    });
  });

  // ==========================================================================
  describe('Escenario 1 — convertir moneda local a divisa extranjera', () => {
    it('baja el origen y sube el destino en la misma escritura', async () => {
      const result = await assetHandlers.convertAccountCurrency(context, copToUsd);

      expect(result.success).toBe(true);
      expect(accountUpdate()).toMatchObject({
        'balances.COP': 1000000,
        'balances.USD': 1000,
      });
    });

    it('deja los dólares con el costo exacto que se pagó por ellos', async () => {
      // RN-01: el costo se traslada. No es el valor de mercado de los USD ese
      // día, son los pesos que efectivamente salieron.
      await assetHandlers.convertAccountCurrency(context, copToUsd);

      expect(accountUpdate()['balanceCostBasis.USD']).toMatchObject({
        cost: 4000000,
        referenceCurrency: 'COP',
        status: 'known',
      });
    });

    it('no consulta ninguna tasa de mercado cuando lo que sale es la moneda de referencia', async () => {
      await assetHandlers.convertAccountCurrency(context, copToUsd);

      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
    });

    it('no realiza diferencia en cambio al convertir la propia moneda de referencia', async () => {
      const result = await assetHandlers.convertAccountCurrency(context, copToUsd);

      expect(result.realizedFxAmount).toBe(0);
      expect(writtenTransaction().realizedFxAmount).toBe(0);
    });

    it('no le lleva base de costo al saldo en la moneda de referencia', async () => {
      await assetHandlers.convertAccountCurrency(context, copToUsd);

      expect(accountUpdate()).not.toHaveProperty('balanceCostBasis.COP');
    });
  });

  // ==========================================================================
  describe('Escenario 2 — no se convierte más de lo que hay', () => {
    it('rechaza indicando el saldo disponible de la divisa de origen', async () => {
      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, amount: 6000000 })
      ).rejects.toThrow(HttpsError);

      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, amount: 6000000 })
      ).rejects.toThrow(/Disponible: 5000000\.00 COP/);
    });

    it('no registra ningún movimiento ni altera ninguno de los dos saldos', async () => {
      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, amount: 6000000 })
      ).rejects.toThrow(HttpsError);

      expect(mockBatch.set).not.toHaveBeenCalled();
      expect(mockBatch.update).not.toHaveBeenCalled();
      expect(mockBatch.commit).not.toHaveBeenCalled();
    });

    it('acepta convertir el saldo completo pese al ruido de punto flotante', async () => {
      store.portfolioAccounts['account-123'].balances = { COP: 4999999.994 };

      const result = await assetHandlers.convertAccountCurrency(context, {
        ...copToUsd,
        amount: 5000000,
      });

      expect(result.success).toBe(true);
    });
  });

  // ==========================================================================
  describe('Escenario 3 — conversión entre dos divisas extranjeras', () => {
    const usdToEur = {
      portfolioAccountId: 'account-123',
      fromCurrency: 'USD',
      toCurrency: 'EUR',
      amount: 1000,
      conversionRate: 0.92,
      date: '2026-03-12',
    };

    beforeEach(() => {
      // 2.000 USD que costaron 8.000.000 COP: tasa promedio 4.000
      store.portfolioAccounts['account-123'] = {
        userId: 'user-123',
        balances: { USD: 2000 },
        balanceCostBasis: {
          USD: { cost: 8000000, referenceCurrency: 'COP', status: 'known' },
        },
      };
      historicalRateService.getCrossRate.mockResolvedValue({
        rate: 4200, rateDate: '2026-03-12', source: 'cache',
      });
      historicalRateService.getRateForDate.mockResolvedValue({
        rate: 4200, rateDate: '2026-03-12', source: 'cache',
      });
    });

    it('consume monto y costo del origen según su tasa promedio', async () => {
      await assetHandlers.convertAccountCurrency(context, usdToEur);

      expect(accountUpdate()['balances.USD']).toBe(1000);
      // 8.000.000 − 1.000 × 4.000 = 4.000.000
      expect(accountUpdate()['balanceCostBasis.USD'].cost).toBe(4000000);
    });

    it('RN-08: la tasa promedio del saldo que queda no cambia', async () => {
      await assetHandlers.convertAccountCurrency(context, usdToEur);

      const update = accountUpdate();
      const remainingRate = update['balanceCostBasis.USD'].cost / update['balances.USD'];

      expect(remainingRate).toBe(4000);
    });

    it('el saldo en EUR queda con el costo equivalente en COP a la fecha', async () => {
      await assetHandlers.convertAccountCurrency(context, usdToEur);

      expect(accountUpdate()['balances.EUR']).toBe(920);
      // 1.000 USD valían 4.200.000 COP el día de la conversión
      expect(accountUpdate()['balanceCostBasis.EUR']).toMatchObject({
        cost: 4200000,
        referenceCurrency: 'COP',
        status: 'known',
      });
    });

    it('RN-07: realiza la diferencia en cambio de los dólares que salieron', async () => {
      const result = await assetHandlers.convertAccountCurrency(context, usdToEur);

      // Valían 4.200.000 y costaron 4.000.000
      expect(result.realizedFxAmount).toBe(200000);
      expect(writtenTransaction().realizedFxAmount).toBe(200000);
      expect(writtenTransaction().realizedFxCurrency).toBe('COP');
    });

    it('valora la salida con la tasa de la FECHA de la conversión', async () => {
      await assetHandlers.convertAccountCurrency(context, usdToEur);

      expect(historicalRateService.getCrossRate).toHaveBeenCalledWith('USD', 'COP', '2026-03-12');
    });

    it('RN-13: sin tasa de valoración, el destino entra con costo desconocido', async () => {
      historicalRateService.getCrossRate.mockResolvedValue(null);
      historicalRateService.getRateForDate.mockResolvedValue(null);

      const result = await assetHandlers.convertAccountCurrency(context, usdToEur);

      // El movimiento SÍ se registra: el usuario hizo la conversión.
      expect(result.success).toBe(true);
      expect(accountUpdate()['balances.EUR']).toBe(920);
      expect(accountUpdate()['balanceCostBasis.EUR']).toMatchObject({
        cost: null,
        status: 'unknown',
      });
      expect(result.realizedFxAmount).toBeNull();
    });

    it('cuando el destino es la moneda de referencia usa la tasa del usuario, no la de mercado', async () => {
      // RN-2.2-B: el bróker rara vez aplica la tasa de mercado.
      const result = await assetHandlers.convertAccountCurrency(context, {
        ...usdToEur,
        toCurrency: 'COP',
        conversionRate: 4150,
      });

      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
      expect(accountUpdate()['balances.COP']).toBe(4150000);
      // Valorados a 4.150 y costaron 4.000 → 150.000 de diferencia
      expect(result.realizedFxAmount).toBe(150000);
    });
  });

  // ==========================================================================
  describe('Escenario 4 — un solo movimiento, visible desde ambos saldos', () => {
    it('escribe UN documento que identifica origen, destino y tasa', async () => {
      await assetHandlers.convertAccountCurrency(context, copToUsd);

      expect(mockBatch.set).toHaveBeenCalledTimes(1);
      expect(writtenTransaction()).toMatchObject({
        type: 'cash_conversion',
        assetType: 'cash',
        currency: 'COP',
        amount: 4000000,
        toCurrency: 'USD',
        toAmount: 1000,
        conversionRate: 0.00025,
      });
    });

    it('RN-2.2-A: los dos saldos se mueven en una sola actualización de la cuenta', async () => {
      await assetHandlers.convertAccountCurrency(context, copToUsd);

      expect(mockBatch.update).toHaveBeenCalledTimes(1);
      expect(mockBatch.commit).toHaveBeenCalledTimes(1);

      const update = accountUpdate();
      expect(Object.keys(update)).toEqual(
        expect.arrayContaining(['balances.COP', 'balances.USD', 'balanceCostBasis.USD'])
      );
    });
  });

  // ==========================================================================
  describe('Escenario 6 — sin tipo de cambio, no hay conversión', () => {
    it('rechaza la conversión sin tasa', async () => {
      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, conversionRate: undefined })
      ).rejects.toThrow(/tipo de cambio mayor que cero/);
    });

    it('rechaza una tasa de cero o negativa', async () => {
      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, conversionRate: 0 })
      ).rejects.toThrow(/tipo de cambio mayor que cero/);

      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, conversionRate: -1 })
      ).rejects.toThrow(/tipo de cambio mayor que cero/);
    });

    it('no escribe nada cuando falta la tasa', async () => {
      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, conversionRate: 0 })
      ).rejects.toThrow(HttpsError);

      expect(mockBatch.commit).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  describe('Validaciones de contrato', () => {
    it('rechaza convertir una divisa por sí misma', async () => {
      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, toCurrency: 'COP' })
      ).rejects.toThrow(/deben ser distintas/);
    });

    it('rechaza un monto no positivo', async () => {
      await expect(
        assetHandlers.convertAccountCurrency(context, { ...copToUsd, amount: -100 })
      ).rejects.toThrow(HttpsError);
    });

    it('exige los campos mínimos', async () => {
      await expect(
        assetHandlers.convertAccountCurrency(context, { fromCurrency: 'COP' })
      ).rejects.toThrow(/son requeridos/);
    });

    it('RN-15: no acepta cuenta de destino — la operación vive en una sola cuenta', async () => {
      await assetHandlers.convertAccountCurrency(context, {
        ...copToUsd,
        toAccountId: 'otra-cuenta',
      });

      // El documento escrito solo conoce una cuenta.
      expect(writtenTransaction().portfolioAccountId).toBe('account-123');
      expect(writtenTransaction()).not.toHaveProperty('toAccountId');
    });

    it('rechaza operar sobre una cuenta ajena', async () => {
      store.portfolioAccounts['account-123'].userId = 'otro-usuario';

      await expect(
        assetHandlers.convertAccountCurrency(context, copToUsd)
      ).rejects.toThrow(/No tienes permiso/);
    });
  });

  // ==========================================================================
  describe('Escenario 7 — sin exposición cambiaria, sin ruido', () => {
    it('un usuario con referencia USD convierte sin que se le lleve base de costo del origen', async () => {
      store.userData['user-123'] = { defaultCurrency: 'USD' };
      store.portfolioAccounts['account-123'] = {
        userId: 'user-123',
        balances: { USD: 5000 },
      };
      historicalRateService.getRateForDate.mockResolvedValue(null);

      const result = await assetHandlers.convertAccountCurrency(context, {
        portfolioAccountId: 'account-123',
        fromCurrency: 'USD',
        toCurrency: 'EUR',
        amount: 1000,
        conversionRate: 0.92,
        date: '2026-03-12',
      });

      expect(result.success).toBe(true);
      expect(accountUpdate()).not.toHaveProperty('balanceCostBasis.USD');
      // Los euros sí tienen exposición: costaron 1.000 USD.
      expect(accountUpdate()['balanceCostBasis.EUR']).toMatchObject({
        cost: 1000,
        referenceCurrency: 'USD',
        status: 'known',
      });
      expect(result.realizedFxAmount).toBe(0);
    });
  });
});
