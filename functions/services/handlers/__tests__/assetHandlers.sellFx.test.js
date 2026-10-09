/**
 * HU 2.4 — Tests de la venta que captura el tipo de cambio de su propio día.
 *
 * @module handlers/__tests__/assetHandlers.sellFx.test
 * @see platform-docs/stories/2.4-venta-dividendo-tasa-del-dia/refinamiento.md (T2, T3, T15)
 */

// ============================================================================
// Firestore falso enrutado por colección
// ============================================================================

const store = {
  assets: {},
  portfolioAccounts: {},
  userData: {},
};

/** Lotes que devuelve la consulta FIFO de `assets` */
let fifoLots = [];

const mockBatch = {
  set: jest.fn(),
  update: jest.fn(),
  delete: jest.fn(),
  commit: jest.fn().mockResolvedValue(undefined),
};

let generatedDocId = 0;

const makeDocRef = (collectionName, docId) => ({
  id: docId,
  collectionName,
  get: jest.fn().mockImplementation(async () => {
    const data = store[collectionName]?.[docId];
    return { exists: data !== undefined, id: docId, data: () => data };
  }),
  set: jest.fn().mockResolvedValue(undefined),
  update: jest.fn().mockResolvedValue(undefined),
});

const makeCollection = (collectionName) => {
  const query = {
    doc: (docId) => makeDocRef(collectionName, docId ?? `generated-${++generatedDocId}`),
    where: jest.fn(() => query),
    orderBy: jest.fn(() => query),
    limit: jest.fn(() => query),
    get: jest.fn().mockImplementation(async () => {
      if (collectionName === 'assets') {
        const docs = fifoLots.map((lot) => ({ id: lot.id, data: () => lot }));
        return { empty: docs.length === 0, docs, forEach: (fn) => docs.forEach(fn) };
      }
      // Las transacciones de compra que se marcan como cerradas no participan
      // en ninguna de las aserciones de esta suite.
      return { empty: true, docs: [], forEach: () => {} };
    }),
  };
  return query;
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

jest.mock('../../realizedFxBackfill', () => ({
  backfillRealizedFxForUser: jest.fn(),
}));

const historicalRateService = require('../../historicalRateService');
const assetHandlers = require('../assetHandlers');

// ============================================================================
// Fixtures
// ============================================================================

const context = { auth: { uid: 'user-123' } };

/**
 * El caso de referencia de la historia: se compró por 1.000 USD con la TRM en
 * 4.000 (2 unidades a 500) y se vende por 1.200 USD con la TRM en 4.300.
 */
const metaLot = (overrides = {}) => ({
  id: 'asset-1',
  portfolioAccount: 'account-123',
  name: 'META',
  assetType: 'stock',
  market: 'NASDAQ',
  currency: 'USD',
  units: 2,
  unitValue: 500,
  isActive: true,
  acquisitionDate: '2026-03-12',
  acquisitionRate: 4000,
  referenceCurrency: 'COP',
  acquisitionDollarValue: 4000,
  defaultCurrencyForAdquisitionDollar: 'COP',
  ...overrides,
});

const sellPayload = (overrides = {}) => ({
  assetId: 'asset-1',
  portfolioAccountId: 'account-123',
  sellAmount: 2,
  sellPrice: 600,
  sellCommission: 0,
  sellDate: '2026-08-28',
  ...overrides,
});

/** Documento de la transacción de venta escrito en el batch */
const writtenSale = (index = 0) => mockBatch.set.mock.calls[index][1];

/** Actualización de la cuenta escrita en el batch */
const accountUpdate = () => {
  const call = mockBatch.update.mock.calls.find(
    ([, payload]) => payload && Object.keys(payload).some((k) => k.startsWith('balances.'))
  );
  return call ? call[1] : null;
};

const givenUser = (defaultCurrency = 'COP') => {
  store.userData = { 'user-123': { defaultCurrency } };
};

const givenAccount = (balances = {}, balanceCostBasis) => {
  store.portfolioAccounts = {
    'account-123': {
      userId: 'user-123',
      balances,
      ...(balanceCostBasis ? { balanceCostBasis } : {}),
    },
  };
};

const givenSellRate = (rate) => {
  historicalRateService.getCrossRate.mockResolvedValue(
    rate === null ? null : { rate, rateDate: '2026-08-28', source: 'cache' }
  );
};

beforeEach(() => {
  jest.clearAllMocks();
  generatedDocId = 0;
  fifoLots = [];
  store.assets = { 'asset-1': metaLot() };
  givenUser('COP');
  givenAccount({ USD: 0 });
  givenSellRate(4300);
});

// ============================================================================
// AC-1 — el dinero vuelve con la tasa de su propio día
// ============================================================================

describe('sellAsset — el caso de referencia (AC-1)', () => {
  it('los 1.200 USD entran al saldo con un costo de 5.160.000 COP, el de hoy', async () => {
    await assetHandlers.sellAsset(context, sellPayload());

    const update = accountUpdate();
    expect(update['balances.USD']).toBe(1200);
    expect(update['balanceCostBasis.USD']).toMatchObject({
      cost: 5160000,
      referenceCurrency: 'COP',
      status: 'known',
    });
  });

  it('la operación reporta 1.160.000 descompuestos en 800.000 y 360.000', async () => {
    const result = await assetHandlers.sellAsset(context, sellPayload());

    expect(result.assetMeritAmount).toBe(800000);
    expect(result.realizedFxAmount).toBe(360000);
    expect(result.realizedTotalAmount).toBe(1160000);
    expect(result.realizedFxAvailability).toBe('available');
  });

  it('la transacción persiste las dos tasas y la descomposición', async () => {
    await assetHandlers.sellAsset(context, sellPayload());

    expect(writtenSale()).toMatchObject({
      type: 'sell',
      referenceCurrency: 'COP',
      acquisitionRate: 4000,
      acquisitionRateSource: 'asset',
      realizationRate: 4300,
      realizationRateSource: 'market-date',
      assetMeritAmount: 800000,
      realizedFxAmount: 360000,
      realizedTotalAmount: 1160000,
      acquisitionCost: 5160000,
    });
  });

  it('la tasa se pide para el día que eligió el usuario, no para el instante UTC', async () => {
    await assetHandlers.sellAsset(context, sellPayload({ sellDate: '2026-08-28' }));

    expect(historicalRateService.getCrossRate).toHaveBeenCalledWith('USD', 'COP', '2026-08-28');
  });

  it('la comisión se descuenta del dinero que entra y de su costo', async () => {
    await assetHandlers.sellAsset(context, sellPayload({ sellCommission: 20 }));

    const update = accountUpdate();
    expect(update['balances.USD']).toBe(1180);
    expect(update['balanceCostBasis.USD'].cost).toBe(1180 * 4300);
  });

  it('la comisión no altera la descomposición, que se calcula sobre el bruto', async () => {
    const result = await assetHandlers.sellAsset(context, sellPayload({ sellCommission: 20 }));

    expect(result.assetMeritAmount).toBe(800000);
    expect(result.realizedFxAmount).toBe(360000);
  });
});

// ============================================================================
// D3 — `dollarPriceToDate` conserva su semántica
// ============================================================================

describe('sellAsset — el contrato de `dollarPriceToDate` no se toca (D3)', () => {
  it('sigue siendo la tasa de adquisición del activo, no la del día de la venta', async () => {
    await assetHandlers.sellAsset(context, sellPayload());

    // Seis calculadores en producción la leen con esta semántica. La tasa del día
    // de la venta vive en `realizationRate`, no aquí.
    expect(writtenSale().dollarPriceToDate).toBe(4000);
    expect(writtenSale().realizationRate).toBe(4300);
  });

  it('`valuePnL` sigue siendo el resultado bruto en la divisa del activo', async () => {
    await assetHandlers.sellAsset(context, sellPayload());

    expect(writtenSale().valuePnL).toBe(200);
  });
});

// ============================================================================
// D5 — una venta no se bloquea por falta de tasa
// ============================================================================

describe('sellAsset — sin tasa del día (D5, RN-13, AC-9)', () => {
  beforeEach(() => givenSellRate(null));

  it('la venta se registra igual: el usuario vendió', async () => {
    const result = await assetHandlers.sellAsset(context, sellPayload());

    expect(result.success).toBe(true);
    expect(mockBatch.commit).toHaveBeenCalled();
  });

  it('el saldo queda indeterminado antes que mostrar un costo inventado', async () => {
    await assetHandlers.sellAsset(context, sellPayload());

    const update = accountUpdate();
    expect(update['balances.USD']).toBe(1200);
    expect(update['balanceCostBasis.USD']).toMatchObject({ cost: null, status: 'unknown' });
  });

  it('la descomposición se declara no disponible, nunca cero efecto divisa', async () => {
    const result = await assetHandlers.sellAsset(context, sellPayload());

    expect(result.realizedFxAvailability).toBe('unavailable');
    expect(result.realizedFxAmount).toBeNull();
    expect(writtenSale().realizedFxUnavailableReason).toBe('missing-realization-rate');
  });
});

describe('sellAsset — sin tasa de compra determinable (AC-9)', () => {
  it('un activo antiguo sin ancla utilizable se declara no disponible', async () => {
    store.assets['asset-1'] = metaLot({
      acquisitionRate: undefined,
      referenceCurrency: undefined,
      acquisitionDollarValue: 4000,
      defaultCurrencyForAdquisitionDollar: 'USD',
      acquisitionDate: undefined,
    });

    const result = await assetHandlers.sellAsset(context, sellPayload());

    expect(result.realizedFxAvailability).toBe('unavailable');
    expect(writtenSale().realizedFxUnavailableReason).toBe('missing-acquisition-rate');
  });

  it('un activo en USD anclado a la moneda de referencia sí resuelve, sin consultar el mercado', async () => {
    store.assets['asset-1'] = metaLot({ acquisitionRate: undefined, referenceCurrency: undefined });

    const result = await assetHandlers.sellAsset(context, sellPayload());

    expect(result.acquisitionRate).toBe(4000);
    expect(writtenSale().acquisitionRateSource).toBe('legacy-acquisition');
    expect(result.realizedTotalAmount).toBe(1160000);
  });
});

// ============================================================================
// AC-10 — sin efecto divisa, sin ruido
// ============================================================================

describe('sellAsset — la moneda de referencia es la del activo (AC-10, RN-14)', () => {
  beforeEach(() => {
    givenUser('USD');
    givenAccount({ USD: 0 });
  });

  it('no consulta ninguna tasa', async () => {
    await assetHandlers.sellAsset(context, sellPayload());

    expect(historicalRateService.getCrossRate).not.toHaveBeenCalled();
  });

  it('el efecto divisa es cero y el saldo no lleva base de costo', async () => {
    const result = await assetHandlers.sellAsset(context, sellPayload());

    expect(result.realizedFxAmount).toBe(0);
    expect(result.assetMeritAmount).toBe(200);
    expect(accountUpdate()['balanceCostBasis.USD']).toBeUndefined();
  });
});

// ============================================================================
// AC-6 — venta que consume varios lotes
// ============================================================================

describe('sellPartialAssetsFIFO — una operación, varios lotes (AC-6, RN-2.4-A)', () => {
  /**
   * Los tres lotes del preview de la historia, comprados a tipos de cambio
   * distintos y vendidos el mismo día a 600 USD con la TRM en 4.300.
   */
  const givenThreeLots = () => {
    fifoLots = [
      { ...metaLot(), id: 'lot-1', units: 2, unitValue: 500, acquisitionRate: 4000 },
      { ...metaLot(), id: 'lot-2', units: 1, unitValue: 525, acquisitionRate: 4150 },
      { ...metaLot(), id: 'lot-3', units: 1, unitValue: 570, acquisitionRate: 4280 },
    ];
    store.assets = Object.fromEntries(fifoLots.map((lot) => [lot.id, lot]));
  };

  const fifoPayload = {
    ticker: 'META',
    portfolioAccountId: 'account-123',
    unitsToSell: 4,
    pricePerUnit: 600,
    totalCommission: 0,
    sellDate: '2026-08-28',
  };

  beforeEach(givenThreeLots);

  it('consulta la tasa del día UNA sola vez para toda la operación', async () => {
    await assetHandlers.sellPartialAssetsFIFO(context, fifoPayload);

    expect(historicalRateService.getCrossRate).toHaveBeenCalledTimes(1);
  });

  it('devuelve una sola cifra de mérito y una de efecto divisa', async () => {
    const result = await assetHandlers.sellPartialAssetsFIFO(context, fifoPayload);

    // lote 1: (1200−1000)×4000 = 800.000 | 1200×300 = 360.000
    // lote 2: (600−525)×4150  = 311.250 | 600×150  =  90.000
    // lote 3: (600−570)×4280  = 128.400 | 600×20   =  12.000
    expect(result.assetMeritAmount).toBe(800000 + 311250 + 128400);
    expect(result.realizedFxAmount).toBe(360000 + 90000 + 12000);
    expect(result.realizedTotalAmount).toBe(result.assetMeritAmount + result.realizedFxAmount);
  });

  it('al abrirla se ve la contribución de cada lote con su tipo de cambio de compra', async () => {
    const { soldAssets } = await assetHandlers.sellPartialAssetsFIFO(context, fifoPayload);

    expect(soldAssets).toHaveLength(3);
    expect(soldAssets[0]).toMatchObject({ acquisitionRate: 4000, assetMeritAmount: 800000, realizedFxAmount: 360000 });
    expect(soldAssets[1]).toMatchObject({ acquisitionRate: 4150, assetMeritAmount: 311250, realizedFxAmount: 90000 });
    expect(soldAssets[2]).toMatchObject({ acquisitionRate: 4280, assetMeritAmount: 128400, realizedFxAmount: 12000 });
  });

  it('los lotes suman exactamente la cifra consolidada', async () => {
    const { soldAssets, assetMeritAmount, realizedFxAmount } =
      await assetHandlers.sellPartialAssetsFIFO(context, fifoPayload);

    const cents = (v) => Math.round(v * 100);
    expect(soldAssets.reduce((s, l) => s + cents(l.assetMeritAmount), 0)).toBe(cents(assetMeritAmount));
    expect(soldAssets.reduce((s, l) => s + cents(l.realizedFxAmount), 0)).toBe(cents(realizedFxAmount));
  });

  it('el saldo recibe todo el producto con el costo del día de la venta', async () => {
    await assetHandlers.sellPartialAssetsFIFO(context, fifoPayload);

    const update = accountUpdate();
    expect(update['balances.USD']).toBe(2400);
    expect(update['balanceCostBasis.USD'].cost).toBe(2400 * 4300);
  });

  it('cada documento de lote lleva su propia tasa de compra y la misma tasa de venta', async () => {
    await assetHandlers.sellPartialAssetsFIFO(context, fifoPayload);

    const lots = mockBatch.set.mock.calls.map(([, doc]) => doc);
    expect(lots.map((l) => l.acquisitionRate)).toEqual([4000, 4150, 4280]);
    expect(lots.map((l) => l.realizationRate)).toEqual([4300, 4300, 4300]);
    expect(lots.map((l) => l.operationId)).toEqual([lots[0].operationId, lots[0].operationId, lots[0].operationId]);
  });

  it('si un solo lote no se puede descomponer, la operación entera se declara no disponible', async () => {
    fifoLots[1] = {
      ...fifoLots[1],
      acquisitionRate: undefined,
      referenceCurrency: undefined,
      defaultCurrencyForAdquisitionDollar: 'USD',
      acquisitionDollarValue: undefined,
      acquisitionDate: undefined,
    };
    store.assets = Object.fromEntries(fifoLots.map((lot) => [lot.id, lot]));

    const result = await assetHandlers.sellPartialAssetsFIFO(context, fifoPayload);

    expect(result.realizedFxAvailability).toBe('unavailable');
    expect(result.assetMeritAmount).toBeNull();
  });
});
