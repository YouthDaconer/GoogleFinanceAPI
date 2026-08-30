/**
 * HU 2.4 — Tests del recálculo del histórico de posiciones cerradas.
 *
 * @module __tests__/services/realizedFxBackfill.test
 * @see platform-docs/stories/2.4-venta-dividendo-tasa-del-dia/refinamiento.md (T5, T17, D7)
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

/** Todas las actualizaciones enviadas a los batches, en orden */
let updates = [];
/** Número de `commit()` ejecutados */
let commits = 0;

const makeDocRef = (collectionName, docId) => ({
  id: docId,
  collectionName,
  get: jest.fn().mockImplementation(async () => {
    const data = store[collectionName]?.[docId];
    return { exists: data !== undefined, id: docId, data: () => data };
  }),
});

const docsOf = (collectionName, filter = () => true) =>
  Object.entries(store[collectionName] || {})
    .filter(([, data]) => filter(data))
    .map(([id, data]) => ({ id, ref: makeDocRef(collectionName, id), data: () => data }));

const makeCollection = (collectionName) => {
  const filters = [];
  const query = {
    doc: (docId) => makeDocRef(collectionName, docId),
    where: jest.fn((field, op, value) => {
      filters.push({ field, op, value });
      return query;
    }),
    orderBy: jest.fn(() => query),
    limit: jest.fn(() => query),
    get: jest.fn().mockImplementation(async () => {
      const docs = docsOf(collectionName, (data) =>
        filters.every(({ field, op, value }) =>
          op === 'in' ? value.includes(data[field]) : data[field] === value
        )
      );
      return { empty: docs.length === 0, docs, forEach: (fn) => docs.forEach(fn) };
    }),
  };
  return query;
};

const mockDb = {
  collection: jest.fn((name) => makeCollection(name)),
  doc: jest.fn((path) => makeDocRef(...path.split('/'))),
  getAll: jest.fn(async (...refs) =>
    refs.map((ref) => {
      const data = store[ref.collectionName]?.[ref.id];
      return { exists: data !== undefined, id: ref.id, data: () => data };
    })
  ),
  batch: jest.fn(() => ({
    update: jest.fn((ref, payload) => updates.push({ id: ref.id, payload })),
    set: jest.fn(),
    commit: jest.fn(async () => { commits += 1; }),
  })),
};

jest.mock('../firebaseAdmin', () => {
  const mockAdmin = { firestore: jest.fn(() => mockDb) };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

const mockGetCrossRate = jest.fn();
jest.mock('../historicalRateService', () => ({
  getCrossRate: (...args) => mockGetCrossRate(...args),
  getRateForDate: jest.fn(),
}));

const { backfillRealizedFxForUser, _needsBackfill } = require('../realizedFxBackfill');

// ============================================================================
// Fixtures
// ============================================================================

/** Venta del caso de referencia: 2 unidades compradas a 500 y vendidas a 600 */
const legacySale = (id, overrides = {}) => ({
  type: 'sell',
  assetId: 'asset-1',
  portfolioAccountId: 'account-123',
  amount: 2,
  price: 600,
  valuePnL: 200,
  commission: 0,
  currency: 'USD',
  date: '2026-08-28',
  // La tasa de la COMPRA, que es lo que escribían las ventas antiguas
  dollarPriceToDate: 4000,
  defaultCurrencyForAdquisitionDollar: 'COP',
  ...overrides,
});

const updateFor = (id) => updates.find((u) => u.id === id)?.payload;

beforeEach(() => {
  jest.clearAllMocks();
  updates = [];
  commits = 0;

  store.userData = { 'user-123': { defaultCurrency: 'COP' } };
  store.portfolioAccounts = { 'account-123': { userId: 'user-123' } };
  store.assets = {
    'asset-1': {
      currency: 'USD',
      acquisitionDate: '2026-03-12',
      acquisitionRate: 4000,
      referenceCurrency: 'COP',
    },
  };
  store.transactions = { 'sale-1': legacySale('sale-1') };

  mockGetCrossRate.mockResolvedValue({ rate: 4300, rateDate: '2026-08-28', source: 'cache' });
});

// ============================================================================
// AC-8 — el histórico queda recalculado
// ============================================================================

describe('backfillRealizedFxForUser — recálculo (AC-8)', () => {
  it('corrige una venta antigua con el tipo de cambio del día en que se vendió', async () => {
    const result = await backfillRealizedFxForUser('user-123');

    expect(result.updatedCount).toBe(1);
    expect(updateFor('sale-1')).toMatchObject({
      referenceCurrency: 'COP',
      acquisitionRate: 4000,
      realizationRate: 4300,
      assetMeritAmount: 800000,
      realizedFxAmount: 360000,
      realizedTotalAmount: 1160000,
      realizedFxAvailability: 'available',
    });
  });

  it('despeja el precio de compra del propio documento, sin releer la transacción de compra', async () => {
    // El lote se compró a 500 (600 − 200/2) y eso es lo que sostiene el mérito.
    await backfillRealizedFxForUser('user-123');

    expect(updateFor('sale-1').assetMeritAmount).toBe((1200 - 1000) * 4000);
  });

  it('no toca `dollarPriceToDate` ni ningún campo existente (D3)', async () => {
    await backfillRealizedFxForUser('user-123');

    expect(updateFor('sale-1')).not.toHaveProperty('dollarPriceToDate');
    expect(updateFor('sale-1')).not.toHaveProperty('valuePnL');
    expect(updateFor('sale-1')).not.toHaveProperty('date');
  });

  it('ignora todo lo que no sea una venta', async () => {
    store.transactions['buy-1'] = { type: 'buy', portfolioAccountId: 'account-123', amount: 2, price: 500 };
    store.transactions['cash-1'] = { type: 'cash_income', portfolioAccountId: 'account-123' };

    await backfillRealizedFxForUser('user-123');

    expect(updates).toHaveLength(1);
    expect(updates[0].id).toBe('sale-1');
  });

  it('un usuario sin cuentas no produce ninguna escritura', async () => {
    store.portfolioAccounts = {};

    const result = await backfillRealizedFxForUser('user-123');

    expect(result).toMatchObject({ updatedCount: 0, hasMore: false });
    expect(commits).toBe(0);
  });
});

// ============================================================================
// Idempotencia
// ============================================================================

describe('backfillRealizedFxForUser — idempotencia (D7)', () => {
  it('una venta ya resuelta no se vuelve a tocar', async () => {
    store.transactions = {
      'sale-1': legacySale('sale-1', { realizationRateSource: 'market-date', realizationRate: 4300 }),
    };

    const result = await backfillRealizedFxForUser('user-123');

    expect(result.updatedCount).toBe(0);
    expect(updates).toHaveLength(0);
  });

  it('una que quedó no disponible sí se reintenta: la ausencia pudo ser transitoria', async () => {
    store.transactions = {
      'sale-1': legacySale('sale-1', { realizationRateSource: 'unavailable', realizationRate: null }),
    };

    const result = await backfillRealizedFxForUser('user-123');

    expect(result.updatedCount).toBe(1);
    expect(updateFor('sale-1').realizationRate).toBe(4300);
  });

  it('la segunda pasada sobre un histórico ya corregido no escribe nada', async () => {
    await backfillRealizedFxForUser('user-123');

    // Se aplica al store lo que la primera pasada escribió.
    store.transactions['sale-1'] = { ...store.transactions['sale-1'], ...updateFor('sale-1') };
    updates = [];

    const second = await backfillRealizedFxForUser('user-123');

    expect(second.updatedCount).toBe(0);
    expect(updates).toHaveLength(0);
  });
});

// ============================================================================
// AC-9 — lo no determinable se declara
// ============================================================================

describe('backfillRealizedFxForUser — tipo de cambio no determinable (AC-9, RN-13)', () => {
  it('sin tasa del día de la venta se marca no disponible, no cero', async () => {
    mockGetCrossRate.mockResolvedValue(null);

    const result = await backfillRealizedFxForUser('user-123');

    expect(result.unavailableCount).toBe(1);
    expect(result.updatedCount).toBe(0);
    expect(updateFor('sale-1')).toMatchObject({
      realizedFxAvailability: 'unavailable',
      realizedFxUnavailableReason: 'missing-realization-rate',
      realizedFxAmount: null,
      realizedTotalAmount: null,
    });
  });

  it('una venta cuyo activo ya no existe se marca no disponible', async () => {
    store.assets = {};
    mockGetCrossRate.mockResolvedValue({ rate: 4300, rateDate: '2026-08-28' });

    const result = await backfillRealizedFxForUser('user-123');

    expect(result.unavailableCount).toBe(1);
    expect(updateFor('sale-1').realizedFxUnavailableReason).toBe('missing-acquisition-rate');
  });

  it('el resultado total conserva su valor: no se escribe nada que lo altere', async () => {
    mockGetCrossRate.mockResolvedValue(null);

    await backfillRealizedFxForUser('user-123');

    expect(updateFor('sale-1')).not.toHaveProperty('valuePnL');
  });
});

// ============================================================================
// Acotación
// ============================================================================

describe('backfillRealizedFxForUser — trabajo acotado (D7)', () => {
  const givenSales = (count) => {
    store.transactions = {};
    for (let i = 0; i < count; i++) {
      store.transactions[`sale-${i}`] = legacySale(`sale-${i}`, { date: `2026-08-${String((i % 28) + 1).padStart(2, '0')}` });
    }
  };

  it('devuelve `hasMore` cuando se agota el tope de escrituras', async () => {
    givenSales(5);

    const result = await backfillRealizedFxForUser('user-123', { maxUpdates: 3 });

    expect(result.updatedCount).toBe(3);
    expect(result.hasMore).toBe(true);
  });

  it('devuelve `hasMore` cuando se agota el presupuesto de consultas de tasa', async () => {
    givenSales(5);

    const result = await backfillRealizedFxForUser('user-123', { maxRateLookups: 2 });

    expect(result.hasMore).toBe(true);
    expect(mockGetCrossRate).toHaveBeenCalledTimes(2);
  });

  it('las ventas del mismo día comparten una sola consulta de tasa', async () => {
    givenSales(4);
    Object.keys(store.transactions).forEach((id) => {
      store.transactions[id] = { ...store.transactions[id], date: '2026-08-28' };
    });

    await backfillRealizedFxForUser('user-123');

    expect(mockGetCrossRate).toHaveBeenCalledTimes(1);
  });

  it('sin exposición cambiaria no consulta ninguna tasa (AC-10)', async () => {
    store.userData = { 'user-123': { defaultCurrency: 'USD' } };
    givenSales(3);

    await backfillRealizedFxForUser('user-123');

    expect(mockGetCrossRate).not.toHaveBeenCalled();
  });
});

// ============================================================================
// Predicado
// ============================================================================

describe('_needsBackfill', () => {
  it.each([
    ['una venta sin trazabilidad', { type: 'sell' }, true],
    ['una venta marcada no disponible', { type: 'sell', realizationRateSource: 'unavailable' }, true],
    ['una venta ya resuelta', { type: 'sell', realizationRateSource: 'market-date' }, false],
    ['una venta sin exposición cambiaria ya resuelta', { type: 'sell', realizationRateSource: 'identity' }, false],
    ['una compra', { type: 'buy' }, false],
    ['un dividendo', { type: 'dividendPay' }, false],
  ])('%s → %s', (_name, doc, expected) => {
    expect(_needsBackfill(doc)).toBe(expected);
  });
});
