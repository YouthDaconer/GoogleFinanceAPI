/**
 * HU 2.3 — Tests de la compra que deriva su propia tasa del saldo que la paga.
 *
 * @module handlers/__tests__/assetHandlers.buyRate.test
 * @see platform-docs/stories/2.3-compra-sin-declarar-tasa/refinamiento.md (T9, D2, D3)
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

/** Compra de 1.000 USD: 10 unidades a 100, sin comisión */
const buyUsd = {
  portfolioAccount: 'account-123',
  name: 'AAPL',
  assetType: 'stock',
  market: 'NASDAQ',
  currency: 'USD',
  units: 10,
  unitValue: 100,
  commission: 0,
  acquisitionDate: '2026-03-12',
};

/** Documento del activo que se envió al batch (primer `set`) */
const writtenAsset = () => mockBatch.set.mock.calls[0][1];

/** Documento de la transacción de compra (segundo `set`) */
const writtenTransaction = () => mockBatch.set.mock.calls[1][1];

/** Actualización de la cuenta que se envió al batch */
const accountUpdate = () => mockBatch.update.mock.calls[0][1];

/**
 * Deja la cuenta con un saldo y, opcionalmente, su base de costo.
 *
 * @param {Object} balances - Saldos por divisa
 * @param {Object} [balanceCostBasis] - Base de costo por divisa
 */
const givenAccount = (balances, balanceCostBasis) => {
  store.portfolioAccounts = {
    'account-123': {
      userId: 'user-123',
      balances,
      ...(balanceCostBasis ? { balanceCostBasis } : {}),
    },
  };
};

/** Base de costo conocida: `amount` unidades que costaron `amount × rate` */
const knownBasis = (amount, rate, referenceCurrency = 'COP') => ({
  cost: amount * rate,
  referenceCurrency,
  status: 'known',
});

describe('createAsset — la compra deriva su tasa del saldo (HU 2.3)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBatch.commit.mockResolvedValue(undefined);

    store.userData = { 'user-123': { defaultCurrency: 'COP' } };
    givenAccount({ USD: 2000 }, { USD: knownBasis(2000, 4133.33) });

    // Tasa de mercado por defecto: 1 USD = 4.000 COP
    historicalRateService.getRateForDate.mockResolvedValue({
      rate: 4000, rateDate: '2026-03-12', source: 'cache',
    });
    historicalRateService.getCrossRate.mockResolvedValue({
      rate: 4000, rateDate: '2026-03-12', source: 'cache',
    });
  });

  // ==========================================================================
  describe('Escenario 1 — la compra deja de preguntar el tipo de cambio', () => {
    it('aplica la tasa promedio del saldo que paga, no la de mercado', async () => {
      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenAsset().acquisitionRate).toBe(4133.33);
      expect(writtenAsset().acquisitionRateSource).toBe('balance-average');
    });

    it('IGNORA la tasa que mande el cliente: el dato ya no es suyo (RN-04)', async () => {
      // Aunque el payload traiga una tasa inventada, la que se persiste es la
      // del saldo. Es lo que hace imposible que dos compras desde el mismo
      // saldo declaren tasas distintas.
      await assetHandlers.createAsset(context, {
        ...buyUsd,
        acquisitionDollarValue: 999,
        defaultCurrencyForAdquisitionDollar: 'MXN',
      });

      expect(writtenAsset().acquisitionRate).toBe(4133.33);
      expect(writtenAsset().acquisitionDollarValue).toBe(4133.33);
      expect(writtenAsset().defaultCurrencyForAdquisitionDollar).toBe('COP');
    });

    it('devuelve al cliente la tasa aplicada y de dónde salió', async () => {
      const result = await assetHandlers.createAsset(context, buyUsd);

      expect(result).toMatchObject({
        success: true,
        acquisitionRate: 4133.33,
        acquisitionRateSource: 'balance-average',
        referenceCurrency: 'COP',
      });
    });

    it('escribe la misma tasa en la transacción de compra', async () => {
      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenTransaction()).toMatchObject({
        type: 'buy',
        acquisitionRate: 4133.33,
        acquisitionRateSource: 'balance-average',
        referenceCurrency: 'COP',
      });
    });
  });

  // ==========================================================================
  describe('Escenario 4 — la compra transfiere el costo del efectivo al activo', () => {
    beforeEach(() => {
      // 1.000 USD con tasa promedio de 4.000
      givenAccount({ USD: 1000 }, { USD: knownBasis(1000, 4000) });
    });

    it('deja el saldo en cero y el activo con el costo base que salió del efectivo', async () => {
      await assetHandlers.createAsset(context, buyUsd);

      expect(accountUpdate()['balances.USD']).toBe(0);
      expect(writtenAsset().acquisitionCost).toBe(4000000);
    });

    it('el costo que libera el efectivo es el mismo que hereda el activo (RN-01)', async () => {
      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenTransaction().releasedCost).toBe(writtenAsset().acquisitionCost);
    });

    it('NO reporta diferencia en cambio: comprar la difiere, no la realiza (RN-07)', async () => {
      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenTransaction().realizedFxAmount).toBe(0);
      expect(writtenTransaction().realizedFxCurrency).toBe('COP');
    });
  });

  // ==========================================================================
  describe('Escenario 5 — compra parcial del saldo', () => {
    beforeEach(() => {
      // 2.000 USD con tasa promedio de 4.200
      givenAccount({ USD: 2000 }, { USD: knownBasis(2000, 4200) });
    });

    it('el activo queda con el costo de lo que se gastó', async () => {
      // 500 USD × 4.200 = 2.100.000 COP
      await assetHandlers.createAsset(context, { ...buyUsd, units: 5, unitValue: 100 });

      expect(writtenAsset().acquisitionCost).toBe(2100000);
    });

    it('la tasa promedio del remanente NO cambia (RN-08)', async () => {
      await assetHandlers.createAsset(context, { ...buyUsd, units: 5, unitValue: 100 });

      const update = accountUpdate();
      const remainingBalance = update['balances.USD'];
      const remainingCost = update['balanceCostBasis.USD'].cost;

      expect(remainingBalance).toBe(1500);
      expect(remainingCost / remainingBalance).toBeCloseTo(4200, 6);
    });
  });

  // ==========================================================================
  describe('Escenario 6 — activos en divisas distintas del dólar', () => {
    beforeEach(() => {
      // 1.000 EUR adquiridos a 4.700 COP cada uno
      givenAccount({ EUR: 1000 }, { EUR: knownBasis(1000, 4700) });
    });

    const buyEur = { ...buyUsd, name: 'ASML', currency: 'EUR', market: 'AMS' };

    it('deriva la tasa del saldo en EUR, exactamente igual que en dólares', async () => {
      await assetHandlers.createAsset(context, buyEur);

      expect(writtenAsset().acquisitionRate).toBe(4700);
      expect(writtenAsset().acquisitionRateSource).toBe('balance-average');
    });

    it('el costo base del activo queda en la moneda de referencia', async () => {
      await assetHandlers.createAsset(context, buyEur);

      expect(writtenAsset().acquisitionCost).toBe(4700000);
      expect(writtenAsset().referenceCurrency).toBe('COP');
    });

    it('conserva el ancla histórica del campo antiguo: divisa del activo y tasa contra el dólar (D3)', async () => {
      // Los seis calculadores en producción leen `acquisitionDollarValue` como
      // "unidades de `defaultCurrencyForAdquisitionDollar` por 1 USD". Para un
      // activo no-USD eso siempre fue su propia divisa; cambiarlo los rompería.
      historicalRateService.getRateForDate.mockResolvedValue({
        rate: 0.92, rateDate: '2026-03-12', source: 'cache',
      });

      await assetHandlers.createAsset(context, buyEur);

      expect(writtenAsset().defaultCurrencyForAdquisitionDollar).toBe('EUR');
      expect(writtenAsset().acquisitionDollarValue).toBe(0.92);
      expect(writtenTransaction().dollarPriceToDate).toBe(0.92);
    });
  });

  // ==========================================================================
  describe('Escenario 7 — comprar en la propia moneda de referencia', () => {
    beforeEach(() => {
      givenAccount({ COP: 10000000 });
    });

    const buyCop = { ...buyUsd, name: 'ECOPETROL', currency: 'COP', market: 'BVC', unitValue: 2000 };

    it('la tasa es 1 y no se consulta ninguna base ni ningún mercado (RN-14)', async () => {
      await assetHandlers.createAsset(context, buyCop);

      expect(writtenAsset().acquisitionRate).toBe(1);
      expect(writtenAsset().acquisitionRateSource).toBe('identity');
      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
    });

    it('no le lleva base de costo al saldo en la moneda de referencia', async () => {
      await assetHandlers.createAsset(context, buyCop);

      expect(accountUpdate()).not.toHaveProperty('balanceCostBasis.COP');
    });
  });

  // ==========================================================================
  describe('Escenario 8 — sin efecto divisa, sin ruido', () => {
    beforeEach(() => {
      store.userData = { 'user-123': { defaultCurrency: 'USD' } };
      givenAccount({ USD: 5000 });
    });

    it('el documento queda igual que antes de esta historia: tasa 1 y ancla USD', async () => {
      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenAsset().acquisitionDollarValue).toBe(1);
      expect(writtenAsset().defaultCurrencyForAdquisitionDollar).toBe('USD');
      expect(writtenTransaction().dollarPriceToDate).toBe(1);
    });

    it('no consulta ninguna tasa: no hay nada que convertir', async () => {
      await assetHandlers.createAsset(context, buyUsd);

      expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
      expect(historicalRateService.getRateForDate).not.toHaveBeenCalled();
    });
  });

  // ==========================================================================
  describe('Precedencia de resolución de la tasa (D2)', () => {
    it('rama 3 — sin base determinable, cae a la tasa de mercado de la fecha', async () => {
      givenAccount({ USD: 1000 });
      historicalRateService.getCrossRate.mockResolvedValue({
        rate: 4050, rateDate: '2026-03-12', source: 'yahoo',
      });

      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenAsset().acquisitionRate).toBe(4050);
      expect(writtenAsset().acquisitionRateSource).toBe('market-date');
      expect(historicalRateService.getCrossRate).toHaveBeenCalledWith('USD', 'COP', '2026-03-12');
    });

    it('rama 3 — una base marcada como desconocida no se usa', async () => {
      givenAccount({ USD: 1000 }, { USD: { cost: null, referenceCurrency: 'COP', status: 'unknown' } });

      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenAsset().acquisitionRateSource).toBe('market-date');
    });

    it('rama 3 — una base en otra moneda de referencia tampoco describe el saldo', async () => {
      givenAccount({ USD: 1000 }, { USD: knownBasis(1000, 20, 'MXN') });

      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenAsset().acquisitionRateSource).toBe('market-date');
    });

    it('rama 4 — sin base y sin mercado, la ausencia se declara (RN-13)', async () => {
      givenAccount({ USD: 1000 });
      historicalRateService.getCrossRate.mockResolvedValue(null);
      historicalRateService.getRateForDate.mockResolvedValue(null);

      await assetHandlers.createAsset(context, buyUsd);

      expect(writtenAsset().acquisitionRate).toBeNull();
      expect(writtenAsset().acquisitionRateSource).toBe('unavailable');
      expect(writtenAsset().acquisitionCost).toBeNull();
      expect(writtenTransaction().releasedCost).toBeNull();
    });

    it('rama 4 — la compra se registra igual: la trazabilidad no bloquea', async () => {
      givenAccount({ USD: 1000 });
      historicalRateService.getCrossRate.mockResolvedValue(null);
      historicalRateService.getRateForDate.mockResolvedValue(null);

      const result = await assetHandlers.createAsset(context, buyUsd);

      expect(result.success).toBe(true);
      expect(mockBatch.commit).toHaveBeenCalledTimes(1);
      expect(accountUpdate()['balances.USD']).toBe(0);
    });

    it('un fallo del servicio de tasas se trata como ausencia, no como error', async () => {
      givenAccount({ USD: 1000 });
      historicalRateService.getCrossRate.mockRejectedValue(new Error('Yahoo caído'));
      historicalRateService.getRateForDate.mockRejectedValue(new Error('Yahoo caído'));

      const result = await assetHandlers.createAsset(context, buyUsd);

      expect(result.success).toBe(true);
      expect(writtenAsset().acquisitionRateSource).toBe('unavailable');
    });

    it('la tasa se pide para el día que eligió el usuario, no para el ISO resultante', async () => {
      // `combineDateWithCurrentTime` devuelve UTC y de tarde en América cae en
      // el día siguiente. Bug detectado en 2.1.
      givenAccount({ USD: 1000 });

      await assetHandlers.createAsset(context, { ...buyUsd, acquisitionDate: '2026-03-12' });

      expect(historicalRateService.getCrossRate).toHaveBeenCalledWith('USD', 'COP', '2026-03-12');
    });
  });

  // ==========================================================================
  describe('No-regresión de lo que la historia no toca', () => {
    it('la validación de fondos conserva su comportamiento y su mensaje (RN-2.3-B)', async () => {
      givenAccount({ USD: 500 }, { USD: knownBasis(500, 4000) });

      await expect(
        assetHandlers.createAsset(context, buyUsd)
      ).rejects.toThrow(/Saldo insuficiente\. Disponible: 500\.00 USD, Requerido: 1000\.00 USD/);
    });

    it('sin fondos no se escribe absolutamente nada', async () => {
      givenAccount({ USD: 500 }, { USD: knownBasis(500, 4000) });

      await expect(assetHandlers.createAsset(context, buyUsd)).rejects.toThrow(HttpsError);

      expect(mockBatch.set).not.toHaveBeenCalled();
      expect(mockBatch.commit).not.toHaveBeenCalled();
    });

    it('los campos obligatorios siguen siendo los mismos: la tasa NO es uno de ellos', async () => {
      const { acquisitionDollarValue, ...withoutRate } = buyUsd;

      await expect(assetHandlers.createAsset(context, withoutRate)).resolves.toMatchObject({
        success: true,
      });
    });

    it('la compra sigue siendo una sola escritura atómica', async () => {
      await assetHandlers.createAsset(context, buyUsd);

      // Activo + transacción en el mismo batch que el descuento del saldo.
      expect(mockBatch.set).toHaveBeenCalledTimes(2);
      expect(mockBatch.update).toHaveBeenCalledTimes(1);
      expect(mockBatch.commit).toHaveBeenCalledTimes(1);
    });
  });
});
