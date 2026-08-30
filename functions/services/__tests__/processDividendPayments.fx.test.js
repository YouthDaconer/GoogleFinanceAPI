/**
 * HU 2.4 — Tests del dividendo que entra al saldo con la tasa del día del pago.
 *
 * Cubre además la corrección estructural de D6: este job era el único punto de
 * escritura de saldo que no pasaba por el helper de 2.1, así que subía
 * `balances` sin tocar `balanceCostBasis` y la tasa promedio derivada quedaba
 * mintiendo a la baja con cada dividendo cobrado.
 *
 * @module __tests__/services/processDividendPayments.fx.test
 * @see platform-docs/stories/2.4-venta-dividendo-tasa-del-dia/refinamiento.md (T4, T16)
 */

// ============================================================================
// Firestore falso enrutado por colección
// ============================================================================

const store = {
  assets: {},
  portfolioAccounts: {},
  transactions: {},
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
});

const snapshotOf = (collectionName) => {
  const docs = Object.entries(store[collectionName] || {}).map(([id, data]) => ({
    id,
    data: () => data,
  }));
  return { empty: docs.length === 0, docs, forEach: (fn) => docs.forEach(fn) };
};

const makeCollection = (collectionName) => {
  const query = {
    doc: (docId) => makeDocRef(collectionName, docId ?? `generated-${++generatedDocId}`),
    where: jest.fn(() => query),
    orderBy: jest.fn(() => query),
    limit: jest.fn(() => query),
    get: jest.fn().mockImplementation(async () => {
      // No hay dividendos ya pagados hoy: la consulta de duplicados va vacía.
      if (collectionName === 'transactions') return snapshotOf('__none__');
      return snapshotOf(collectionName);
    }),
  };
  return query;
};

const mockDb = {
  collection: jest.fn((name) => makeCollection(name)),
  doc: jest.fn((path) => makeDocRef(...path.split('/'))),
  batch: jest.fn(() => mockBatch),
};

const mockAdmin = { firestore: jest.fn(() => mockDb) };
mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };

jest.mock('firebase-admin', () => mockAdmin);
jest.mock('../firebaseAdmin', () => mockAdmin);

jest.mock('firebase-functions/v2/scheduler', () => ({
  onSchedule: jest.fn((opts, handler) => handler),
}));

jest.mock('firebase-functions/params', () => ({
  defineSecret: jest.fn(() => ({ value: () => 'token' })),
}));

jest.mock('luxon', () => {
  const actual = jest.requireActual('luxon');
  return {
    ...actual,
    DateTime: {
      ...actual.DateTime,
      // Viernes 28/08/2026: día hábil, así que el job no sale por el atajo de
      // fin de semana.
      now: () => actual.DateTime.fromISO('2026-08-28T09:00:00', { zone: 'America/New_York' }),
      fromFormat: (...args) => actual.DateTime.fromFormat(...args),
      fromISO: (...args) => actual.DateTime.fromISO(...args),
    },
  };
});

jest.mock('../scrapeDividendsInfoFromStock', () => ({
  scrapeDividendsInfoFromStockEvents: jest.fn().mockResolvedValue(undefined),
}));

const mockGetPricesFromApi = jest.fn();
const mockGetCurrencyRatesFromApi = jest.fn();

jest.mock('../marketDataHelper', () => ({
  getPricesFromApi: (...args) => mockGetPricesFromApi(...args),
  getCurrencyRatesFromApi: (...args) => mockGetCurrencyRatesFromApi(...args),
}));

const mockGetCrossRate = jest.fn();
jest.mock('../historicalRateService', () => ({
  getCrossRate: (...args) => mockGetCrossRate(...args),
  getRateForDate: jest.fn(),
}));

const processDividendPayments = require('../processDividendPayments').processDividendPayments;

// ============================================================================
// Fixtures
// ============================================================================

/** Dividendo anual de 80 USD por unidad → 20 USD trimestrales por unidad */
const dividendPrice = (symbol = 'KO', currency = 'USD') => ({
  symbol,
  currency,
  dividend: '80',
  dividendDate: 'Aug 28, 2026',
  exDividend: 'Aug 1, 2026',
});

const givenAsset = (id, overrides = {}) => {
  store.assets[id] = {
    name: 'KO',
    assetType: 'stock',
    currency: 'USD',
    units: 1,
    isActive: true,
    portfolioAccount: 'account-123',
    acquisitionDate: '2026-01-10',
    defaultCurrencyForAdquisitionDollar: 'COP',
    ...overrides,
  };
};

const givenAccount = (id, overrides = {}) => {
  store.portfolioAccounts[id] = {
    userId: 'user-123',
    isActive: true,
    balances: { USD: 1000 },
    balanceCostBasis: { USD: { cost: 4000000, referenceCurrency: 'COP', status: 'known' } },
    taxDeductionPercentage: 0,
    ...overrides,
  };
};

/** Documento de la transacción de dividendo escrito en el batch */
const writtenDividend = (index = 0) => mockBatch.set.mock.calls[index][1];

/** Actualización de saldos de la cuenta */
const accountUpdate = () => {
  const call = mockBatch.update.mock.calls.find(
    ([, payload]) => payload && Object.keys(payload).some((k) => k.startsWith('balances.'))
  );
  return call ? call[1] : null;
};

beforeEach(() => {
  jest.clearAllMocks();
  generatedDocId = 0;
  store.assets = {};
  store.portfolioAccounts = {};
  store.transactions = {};
  store.userData = { 'user-123': { defaultCurrency: 'COP' } };

  mockGetCurrencyRatesFromApi.mockResolvedValue([{ code: 'USD', exchangeRate: 1 }]);
  mockGetPricesFromApi.mockResolvedValue([dividendPrice()]);
  mockGetCrossRate.mockResolvedValue({ rate: 4500, rateDate: '2026-08-28', source: 'cache' });
});

// ============================================================================
// AC-5 — el dividendo entra con la tasa del día del pago
// ============================================================================

describe('processDividendPayments — dividendo en divisa extranjera (AC-5, RN-03)', () => {
  beforeEach(() => {
    // 1 unidad × 80/4 = 20 USD de dividendo
    givenAsset('asset-1');
    givenAccount('account-123');
  });

  it('los 20 USD entran al saldo con un costo de 90.000 COP, la tasa del día del pago', async () => {
    await processDividendPayments({});

    const update = accountUpdate();
    expect(update['balances.USD']).toBe(1020);
    expect(update['balanceCostBasis.USD']).toMatchObject({
      cost: 4000000 + 90000,
      referenceCurrency: 'COP',
      status: 'known',
    });
  });

  it('la tasa promedio del saldo se recalcula incorporándolos', async () => {
    await processDividendPayments({});

    const { cost } = accountUpdate()['balanceCostBasis.USD'];
    const balance = accountUpdate()['balances.USD'];

    // 4.090.000 / 1.020 = 4.009,80… — antes del cambio habría quedado en 3.921,57
    expect(cost / balance).toBeCloseTo(4009.8, 1);
  });

  it('pide la tasa del día del pago, no la de hoy ni la spot', async () => {
    await processDividendPayments({});

    expect(mockGetCrossRate).toHaveBeenCalledWith('USD', 'COP', '2026-08-28');
  });

  it('la transacción deja constancia de la tasa y del costo', async () => {
    await processDividendPayments({});

    expect(writtenDividend()).toMatchObject({
      type: 'dividendPay',
      currency: 'USD',
      referenceCurrency: 'COP',
      realizationRate: 4500,
      realizationRateSource: 'market-date',
      acquisitionCost: 90000,
    });
  });

  it('`dollarPriceToDate` conserva su semántica: no es la tasa del día (D3)', async () => {
    await processDividendPayments({});

    // Sigue saliendo de la tasa spot del USD que devuelve el API de divisas.
    expect(writtenDividend().dollarPriceToDate).toBe(1);
    expect(writtenDividend().realizationRate).toBe(4500);
  });

  it('el saldo se escribe con rutas por divisa, no reemplazando el objeto `balances`', async () => {
    await processDividendPayments({});

    // Reemplazar el objeto entero pisaba las demás divisas y perdía escrituras
    // concurrentes; además saltaba la invariante del costo de 2.1.
    // La clave es literalmente `balances.USD`, no una ruta anidada: se pasa como
    // array para que jest no la interprete como `balances` → `USD`.
    expect(Object.keys(accountUpdate())).not.toContain('balances');
    expect(Object.keys(accountUpdate())).toContain('balances.USD');
  });
});

// ============================================================================
// Acumulación por cuenta y divisa
// ============================================================================

describe('processDividendPayments — varios dividendos de la misma cuenta y divisa', () => {
  it('se acumulan en un solo fragmento con el costo sumado', async () => {
    mockGetPricesFromApi.mockResolvedValue([dividendPrice('KO'), dividendPrice('PEP')]);
    givenAsset('asset-1', { name: 'KO' });
    givenAsset('asset-2', { name: 'PEP' });
    givenAccount('account-123');

    await processDividendPayments({});

    // 20 + 20 = 40 USD, con un costo de 40 × 4.500 = 180.000 COP
    const update = accountUpdate();
    expect(update['balances.USD']).toBe(1040);
    expect(update['balanceCostBasis.USD'].cost).toBe(4000000 + 180000);
  });

  it('la tasa del día se resuelve una sola vez para toda la corrida', async () => {
    mockGetPricesFromApi.mockResolvedValue([dividendPrice('KO'), dividendPrice('PEP')]);
    givenAsset('asset-1', { name: 'KO' });
    givenAsset('asset-2', { name: 'PEP' });
    givenAccount('account-123');

    await processDividendPayments({});

    expect(mockGetCrossRate).toHaveBeenCalledTimes(1);
  });
});

// ============================================================================
// RN-13 / RN-14
// ============================================================================

describe('processDividendPayments — ausencias y no-regresión', () => {
  it('sin tasa del día el dividendo se registra igual y el saldo queda indeterminado', async () => {
    mockGetCrossRate.mockResolvedValue(null);
    givenAsset('asset-1');
    givenAccount('account-123');

    await processDividendPayments({});

    expect(writtenDividend().acquisitionCost).toBeNull();
    expect(accountUpdate()['balances.USD']).toBe(1020);
    expect(accountUpdate()['balanceCostBasis.USD']).toMatchObject({ cost: null, status: 'unknown' });
  });

  it('un dividendo en la moneda de referencia no lleva base de costo (RN-14)', async () => {
    store.userData = { 'user-123': { defaultCurrency: 'USD' } };
    givenAsset('asset-1');
    givenAccount('account-123', { balanceCostBasis: undefined });

    await processDividendPayments({});

    expect(mockGetCrossRate).not.toHaveBeenCalled();
    expect(accountUpdate()['balances.USD']).toBe(1020);
    expect(accountUpdate()['balanceCostBasis.USD']).toBeUndefined();
  });

  it('la deducción de impuestos sigue aplicándose sobre el bruto antes del costo', async () => {
    givenAsset('asset-1');
    givenAccount('account-123', { taxDeductionPercentage: 25 });

    await processDividendPayments({});

    // 20 USD brutos − 25% = 15 USD netos → 15 × 4.500 = 67.500 COP
    expect(writtenDividend().acquisitionCost).toBe(67500);
    expect(accountUpdate()['balances.USD']).toBe(1015);
  });
});
