/**
 * HU 2.5 — Tests del retiro que realiza su diferencia en cambio, de la
 * conversión recableada a la misma fórmula, y de la no-regresión del
 * rendimiento del portafolio.
 *
 * @module handlers/__tests__/assetHandlers.cashOutflowFx.test
 * @see platform-docs/stories/2.5-diferencia-cambio-salidas-efectivo/refinamiento.md (T16)
 */

const fs = require('fs');
const path = require('path');

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
    return { exists: data !== undefined, id: docId, data: () => data };
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
  doc: jest.fn((docPath) => makeDocRef(...docPath.split('/'))),
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
const { deriveAverageRate } = require('../../helpers/balanceCostBasis');

// ============================================================================
// Fixtures
// ============================================================================

const context = { auth: { uid: 'user-123' } };

/** Escenario 1: retirar 500 de los 1.000 USD adquiridos a un promedio de 4.000 */
const withdraw500 = {
  portfolioAccountId: 'account-123',
  type: 'cash_expense',
  amount: 500,
  currency: 'USD',
  date: '2026-03-12',
};

/** Última actualización de cuenta enviada al batch */
const accountUpdate = () => mockBatch.update.mock.calls[mockBatch.update.mock.calls.length - 1][1];

/** Documento de transacción enviado al batch */
const writtenTransaction = () => mockBatch.set.mock.calls[0][1];

/**
 * Cuenta con 1.000 USD y su base de costo, más los pesos de referencia.
 *
 * @param {number} cost - Costo del saldo en COP
 * @param {Object} [extra] - Sobrescrituras de la entrada de base de costo
 */
const seedAccount = (cost = 4000000, extra = {}) => {
  store.portfolioAccounts = {
    'account-123': {
      userId: 'user-123',
      balances: { USD: 1000, COP: 5000000 },
      balanceCostBasis: {
        USD: {
          cost,
          referenceCurrency: 'COP',
          status: 'known',
          // Estos escenarios describen dólares **comprados** con pesos, que es
          // lo único que realiza diferencia en cambio al salir. Un saldo que
          // hubiera llegado por un ingreso o un dividendo realizaría cero.
          convertedAmount: 1000,
          convertedCost: cost,
          ...extra,
        },
      },
    },
  };
};

beforeEach(() => {
  jest.clearAllMocks();
  mockBatch.commit.mockResolvedValue(undefined);

  seedAccount();
  store.userData = { 'user-123': { defaultCurrency: 'COP' } };

  historicalRateService.getCrossRate.mockResolvedValue({
    rate: 4300, rateDate: '2026-03-12', source: 'cache',
  });
  historicalRateService.getRateForDate.mockResolvedValue({
    rate: 4300, rateDate: '2026-03-12', source: 'cache',
  });
});

// ============================================================================

describe('addCashTransaction — el retiro realiza la diferencia en cambio (HU 2.5)', () => {
  describe('Escenario 1 — retirar con la divisa a favor', () => {
    it('registra +150.000 COP de diferencia en cambio realizada', async () => {
      const result = await assetHandlers.addCashTransaction(context, withdraw500);

      expect(result.realizedFxAmount).toBe(150000);
      expect(writtenTransaction().realizedFxAmount).toBe(150000);
      expect(writtenTransaction().realizedFxCurrency).toBe('COP');
      expect(writtenTransaction().realizedFxAvailability).toBe('available');
    });

    it('persiste la tasa del día con su nombre propio, no como una adquisición', async () => {
      await assetHandlers.addCashTransaction(context, withdraw500);

      expect(writtenTransaction().realizationRate).toBe(4300);
      expect(writtenTransaction().realizationRateSource).toBe('market-date');
      expect(writtenTransaction().releasedCost).toBe(2000000);
    });

    it('RN-08: el tipo de cambio promedio de los 500 USD que quedan sigue siendo 4.000', async () => {
      await assetHandlers.addCashTransaction(context, withdraw500);

      const update = accountUpdate();
      expect(update['balances.USD']).toBe(500);
      expect(deriveAverageRate(update['balanceCostBasis.USD'], 500, 'COP')).toBe(4000);
    });

    it('la tasa que declara el usuario manda sobre la del mercado', async () => {
      const result = await assetHandlers.addCashTransaction(context, {
        ...withdraw500,
        exchangeRate: 4500,
      });

      expect(result.realizedFxAmount).toBe(250000);
      expect(writtenTransaction().realizationRateSource).toBe('user');
    });
  });

  // ==========================================================================
  describe('Escenario 3 — la divisa se movió en contra', () => {
    it('registra −150.000 COP y no lo esconde', async () => {
      seedAccount(4300000);
      historicalRateService.getCrossRate.mockResolvedValue({
        rate: 4000, rateDate: '2026-03-12', source: 'cache',
      });

      const result = await assetHandlers.addCashTransaction(context, withdraw500);

      expect(result.realizedFxAmount).toBe(-150000);
      expect(writtenTransaction().realizedFxAmount).toBe(-150000);
    });

    it('la tasa promedio del remanente sigue siendo 4.300', async () => {
      seedAccount(4300000);
      historicalRateService.getCrossRate.mockResolvedValue({
        rate: 4000, rateDate: '2026-03-12', source: 'cache',
      });

      await assetHandlers.addCashTransaction(context, withdraw500);

      expect(deriveAverageRate(accountUpdate()['balanceCostBasis.USD'], 500, 'COP')).toBe(4300);
    });
  });

  // ==========================================================================
  describe('Escenario 4 — retirar en la propia moneda de referencia (RN-14)', () => {
    const withdrawCop = {
      portfolioAccountId: 'account-123',
      type: 'cash_expense',
      amount: 1000000,
      currency: 'COP',
      date: '2026-03-12',
    };

    it('no registra ninguna diferencia en cambio', async () => {
      const result = await assetHandlers.addCashTransaction(context, withdrawCop);

      expect(result.realizedFxAmount).toBeNull();
      expect(writtenTransaction()).not.toHaveProperty('realizedFxAmount');
      expect(writtenTransaction()).not.toHaveProperty('realizedFxAvailability');
    });

    it('el retiro se comporta exactamente como hoy', async () => {
      await assetHandlers.addCashTransaction(context, withdrawCop);

      expect(accountUpdate()['balances.COP']).toBe(4000000);
      expect(accountUpdate()).not.toHaveProperty('balanceCostBasis.COP');
      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  describe('Escenario 8 — retiro que vacía el saldo', () => {
    it('realiza la totalidad y deja el saldo y su costo en cero', async () => {
      seedAccount(4000000);

      const result = await assetHandlers.addCashTransaction(context, {
        ...withdraw500,
        amount: 1000,
      });

      expect(result.realizedFxAmount).toBe(300000);
      expect(accountUpdate()['balances.USD']).toBe(0);
      expect(accountUpdate()['balanceCostBasis.USD'].cost).toBe(0);
    });

    it('la divisa sigue existiendo en la cuenta: el saldo se vacía, no se elimina', async () => {
      await assetHandlers.addCashTransaction(context, { ...withdraw500, amount: 1000 });

      // El fragmento escribe la ruta del saldo, nunca lo borra.
      expect(Object.keys(accountUpdate())).toContain('balances.USD');
      expect(accountUpdate()['balances.USD']).not.toBeUndefined();
    });
  });

  // ==========================================================================
  describe('D4 — un retiro no se bloquea por falta de dato', () => {
    it('sin tasa del día el retiro SÍ se registra, con la ausencia declarada', async () => {
      historicalRateService.getCrossRate.mockResolvedValue(null);

      const result = await assetHandlers.addCashTransaction(context, withdraw500);

      expect(result.success).toBe(true);
      expect(result.realizedFxAmount).toBeNull();
      expect(writtenTransaction().realizedFxAvailability).toBe('unavailable');
      expect(writtenTransaction().realizedFxUnavailableReason).toBe('missing-realization-rate');
    });

    it('sin base de costo declara el motivo, y el saldo se mueve igual', async () => {
      seedAccount(null, { status: 'unknown' });

      const result = await assetHandlers.addCashTransaction(context, withdraw500);

      expect(result.success).toBe(true);
      expect(writtenTransaction().realizedFxUnavailableReason).toBe('unknown-cost-basis');
      expect(accountUpdate()['balances.USD']).toBe(500);
    });

    it('la asimetría con el ingreso es deliberada: el ingreso sí se bloquea (RN-05)', async () => {
      historicalRateService.getCrossRate.mockResolvedValue(null);

      await expect(
        assetHandlers.addCashTransaction(context, { ...withdraw500, type: 'cash_income' })
      ).rejects.toThrow(/tipo de cambio/i);
    });
  });

  // ==========================================================================
  describe('Contrato conservado — un ingreso no gana campos de salida', () => {
    it('el documento de un ingreso no lleva diferencia en cambio', async () => {
      await assetHandlers.addCashTransaction(context, {
        ...withdraw500,
        type: 'cash_income',
      });

      expect(writtenTransaction()).not.toHaveProperty('realizedFxAmount');
      expect(writtenTransaction()).not.toHaveProperty('realizationRate');
    });

    it('`acquisitionRate` y `acquisitionCost` conservan su semántica en el retiro', async () => {
      await assetHandlers.addCashTransaction(context, withdraw500);

      // No se tocan (conservación, como D3 de 2.4): los lectores actuales del
      // documento siguen leyendo lo mismo que leían.
      expect(writtenTransaction().acquisitionRate).toBe(4300);
      expect(writtenTransaction().acquisitionCost).toBe(2150000);
    });
  });
});

// ============================================================================

describe('convertAccountCurrency — misma fórmula, cifras intactas (HU 2.5, T3)', () => {
  const usdToEur = {
    portfolioAccountId: 'account-123',
    fromCurrency: 'USD',
    toCurrency: 'EUR',
    amount: 500,
    conversionRate: 0.9,
    date: '2026-03-12',
  };

  it('la conversión que saca divisa extranjera realiza la misma cifra que el retiro', async () => {
    const result = await assetHandlers.convertAccountCurrency(context, usdToEur);

    // 500 × (4.300 − 4.000) = 150.000, exactamente igual que retirarlos.
    expect(result.realizedFxAmount).toBe(150000);
    expect(result.realizedFxAvailability).toBe('available');
  });

  it('conserva al céntimo lo que 2.2 ya persistía', async () => {
    await assetHandlers.convertAccountCurrency(context, usdToEur);

    const tx = writtenTransaction();
    expect(tx.releasedCost).toBe(2000000);
    expect(tx.acquisitionCost).toBe(2150000);
    expect(tx.realizedFxAmount).toBe(150000);
    expect(tx.realizedFxCurrency).toBe('COP');
  });

  it('sacar la propia moneda de referencia realiza cero, que es un cero medido', async () => {
    const result = await assetHandlers.convertAccountCurrency(context, {
      ...usdToEur,
      fromCurrency: 'COP',
      toCurrency: 'USD',
      amount: 4000000,
      conversionRate: 0.00025,
    });

    expect(result.realizedFxAmount).toBe(0);
    expect(result.realizedFxAvailability).toBe('available');
  });

  it('sin tasa de valoración declara el motivo en lugar de un null mudo', async () => {
    historicalRateService.getCrossRate.mockResolvedValue(null);

    const result = await assetHandlers.convertAccountCurrency(context, usdToEur);

    expect(result.realizedFxAmount).toBeNull();
    expect(result.realizedFxAvailability).toBe('unavailable');
    expect(result.realizedFxUnavailableReason).toBe('missing-realization-rate');
  });
});

// ============================================================================

describe('AC-6 — la diferencia en cambio no contamina el rendimiento (D7)', () => {
  it('el rendimiento de fin de día no lee los saldos de efectivo', () => {
    // La revaluación del efectivo NUNCA entró en `portfolioPerformance`: el
    // cálculo diario trabaja sobre activos. Se mide sobre el código en lugar de
    // suponerlo, porque de esa suposición depende que la línea nueva pueda
    // presentarse aparte sin corregir series ya generadas.
    const source = fs.readFileSync(
      path.join(__dirname, '../../calculateDailyPortfolioPerformance.js'),
      'utf8'
    );

    expect(source).not.toMatch(/\bbalances\b/);
    expect(source).not.toMatch(/balanceCostBasis/);
  });

  it('tampoco lo hace el cálculo de contribuciones de la atribución', () => {
    const source = fs.readFileSync(
      path.join(__dirname, '../../attribution/contributionCalculator.js'),
      'utf8'
    );

    expect(source).not.toMatch(/balanceCostBasis/);
  });
});
