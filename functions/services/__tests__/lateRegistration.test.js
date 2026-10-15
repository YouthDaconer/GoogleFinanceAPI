/**
 * LATE-REG: una transacción registrada después de que el EOD cerró su día.
 *
 * Caso real (MA_stock, 2026-05-07): compra de 0.118 MA con fecha 05-06
 * registrada el 05-07. No se marcó como retroactiva (`<` en vez de `<=` y la
 * fecha "más reciente" leída de un campo congelado), el EOD del 05-07 vio las
 * unidades sin flujo de caja y guardó +47,9 % con el mercado en +1,8 %. Y aun
 * marcada, la reconciliación sólo reescribía USD y no tocaba el adjusted por
 * activo.
 */

// --- Firestore falso: sólo lo que leen estos módulos al cargarse ------------
const mockLatestDocs = { docs: [] };
const mockDb = {
  collection: jest.fn(() => ({
    doc: jest.fn(() => ({
      get: jest.fn(async () => ({ exists: true, data: () => ({ date: '2026-06-30' }) })),
      collection: jest.fn(() => ({
        orderBy: jest.fn(() => ({
          limit: jest.fn(() => ({ get: jest.fn(async () => mockLatestDocs) })),
        })),
      })),
    })),
  })),
  runTransaction: jest.fn(),
  batch: jest.fn(),
};

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => mockDb,
  FieldValue: { serverTimestamp: () => 'ts', increment: () => 1, delete: () => 'DELETE' },
  FieldPath: { documentId: () => '__name__' },
}));
jest.mock('../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({ collection: jest.fn(), doc: jest.fn() })),
}));
jest.mock('firebase-functions/v2/scheduler', () => ({ onSchedule: (_opts, fn) => fn }));
jest.mock('../periodConsolidationScheduled', () => ({ consolidateMonth: jest.fn(), consolidateYear: jest.fn() }));
jest.mock('../cacheInvalidationService', () => ({ invalidatePerformanceCache: jest.fn() }));

const { detectUnexplainedUnitChanges } = require('../../utils/portfolioCalculations');
const { isRetroactiveTransaction } = require('../../utils/performanceStaleMarker');
const {
  calculateAccountDayPerformance,
  aggregateOverallPerformance,
  staleAssetKeyDeletes,
  historicalRangeFor,
} = require('../backfillCoreModule');
const { closedMonthsSince, getLastClosedDate } = require('../reconcileStalePerformance');
const { DateTime } = require('luxon');

// Precios reales de MA y una tasa COP por día (1 USD = X COP)
const PRICES = { MA: { '2026-05-05': 497.08, '2026-05-06': 491.89, '2026-05-07': 500.94 } };
const RATES = {
  '2026-05-05': { USD: 1, COP: 3700 },
  '2026-05-06': { USD: 1, COP: 3706 },
  '2026-05-07': { USD: 1, COP: 3720 },
};
const buy = (date, amount, price) => ({
  type: 'buy', assetName: 'MA', assetType: 'stock', date, amount, price,
  currency: 'USD', portfolioAccountId: 'ibkr',
});
const TXNS = [
  buy('2026-01-28T18:15:18.870Z', 0.2609, 520),
  // La compra tardía: fecha 05-06, registrada el 05-07
  buy('2026-05-06T13:29:21.231Z', 0.118, 491.55),
];

describe('isRetroactiveTransaction', () => {
  beforeEach(() => { mockLatestDocs.docs = [{ id: '2026-05-06' }]; });

  test('una transacción con fecha del último día calculado es retroactiva', async () => {
    // El EOD del 05-06 ya corrió sin ella y el del 05-07 sólo mira el 05-07
    await expect(isRetroactiveTransaction('2026-05-06T13:29:21.231Z', 'u1')).resolves.toBe(true);
  });

  test('una del día siguiente al último calculado no lo es', async () => {
    await expect(isRetroactiveTransaction('2026-05-07T10:00:00.000Z', 'u1')).resolves.toBe(false);
  });

  test('usa el último doc diario, no el campo `date` congelado del doc principal', async () => {
    // El doc principal dice 2026-06-30; los diarios llegan a octubre
    mockLatestDocs.docs = [{ id: '2026-10-14' }];
    await expect(isRetroactiveTransaction('2026-09-01T10:00:00.000Z', 'u1')).resolves.toBe(true);
  });
});

describe('detectUnexplainedUnitChanges (red de seguridad del EOD)', () => {
  const yesterday = { USD: { totalValue: 128.33, MA_stock: { totalValue: 128.33, units: 0.2609 } } };
  const assets = [
    { id: 'a1', name: 'MA', assetType: 'stock', units: 0.2609, acquisitionDate: '2026-01-28T18:15:18.847Z' },
    { id: 'a2', name: 'MA', assetType: 'stock', units: 0.118, acquisitionDate: '2026-05-06T13:29:21.231Z' },
  ];

  test('señala unidades nuevas sin transacción del día y da la fecha de la compra tardía', () => {
    const out = detectUnexplainedUnitChanges(assets, yesterday, []);
    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({ groupKey: 'MA_stock', unitsBefore: 0.2609, latestAcquisitionDate: '2026-05-06' });
    expect(out[0].unitsNow).toBeCloseTo(0.3789, 6);
  });

  test('no señala nada si la transacción es del día', () => {
    const todays = [{ assetId: 'a2', type: 'buy', amount: 0.118 }];
    expect(detectUnexplainedUnitChanges(assets, yesterday, todays)).toEqual([]);
  });

  test('señala un activo que desapareció sin venta (borrado)', () => {
    const out = detectUnexplainedUnitChanges([], yesterday, []);
    expect(out).toEqual([expect.objectContaining({ groupKey: 'MA_stock', unitsNow: 0 })]);
  });

  test('una venta total del día queda explicada', () => {
    const sold = [{ id: 'a1', name: 'MA', assetType: 'stock', units: 0 }];
    const todays = [{ assetId: 'a1', type: 'sell', amount: 0.2609 }];
    expect(detectUnexplainedUnitChanges(sold, yesterday, todays)).toEqual([]);
  });

  test('sin día anterior no compara (primer cálculo del usuario)', () => {
    expect(detectUnexplainedUnitChanges(assets, { USD: { totalValue: 0 } }, [])).toEqual([]);
  });
});

describe('backfill con registro tardío — caso MA 2026-05-07', () => {
  // Recalcula 05-06 y 05-07 encadenando, con la base del 05-05 reconstruida
  const run = () => {
    const base = calculateAccountDayPerformance(TXNS, '2026-05-05', RATES['2026-05-05'], PRICES, null);
    const d06 = calculateAccountDayPerformance(TXNS, '2026-05-06', RATES['2026-05-06'], PRICES, base);
    const d07 = calculateAccountDayPerformance(TXNS, '2026-05-07', RATES['2026-05-07'], PRICES, d06);
    return { base, d06, d07 };
  };

  test('el 05-07 rinde lo que se movió el precio, no +47,9 %', () => {
    const { d07 } = run();
    const marketMove = (500.94 / 491.89 - 1) * 100; // +1,84 %
    expect(d07.USD.assetPerformance.MA_stock.adjustedDailyChangePercentage).toBeCloseTo(marketMove, 6);
    expect(d07.USD.adjustedDailyChangePercentage).toBeCloseTo(marketMove, 6);
  });

  test('el 05-06 lleva la compra como flujo de caja y su adjusted la neutraliza', () => {
    const { d06 } = run();
    const ma = d06.USD.assetPerformance.MA_stock;
    expect(ma.units).toBeCloseTo(0.3789, 6);
    expect(ma.totalCashFlow).toBeCloseTo(-0.118 * 491.55, 6);
    // (V - V_ayer + CF) / V_ayer: el precio cayó y la compra no cuenta como ganancia
    const expected = ((0.3789 * 491.89 - 0.2609 * 497.08 - 0.118 * 491.55) / (0.2609 * 497.08)) * 100;
    expect(ma.adjustedDailyChangePercentage).toBeCloseTo(expected, 6);
  });

  test('escribe también las demás monedas, con el mismo adjusted por activo + el FX', () => {
    const { d07 } = run();
    const cop = d07.COP.assetPerformance.MA_stock;
    const expected = ((500.94 * 3720) / (491.89 * 3706) - 1) * 100;
    expect(cop.adjustedDailyChangePercentage).toBeCloseTo(expected, 6);
    expect(d07.COP.totalValue).toBeCloseTo(0.3789 * 500.94 * 3720, 4);
    // Inversión/ROI sólo en USD: en COP se conserva lo del EOD al hacer merge
    expect(cop.totalInvestment).toBeUndefined();
    expect(d07.USD.assetPerformance.MA_stock.totalInvestment).toBeGreaterThan(0);
  });

  test('el overall con día anterior usa los totales, igual que el EOD', () => {
    const { base, d06, d07 } = run();
    const prevOverall = aggregateOverallPerformance(new Map([['ibkr', base]]), '2026-05-05');
    const o06 = aggregateOverallPerformance(new Map([['ibkr', d06]]), '2026-05-06', prevOverall);
    const o07 = aggregateOverallPerformance(new Map([['ibkr', d07]]), '2026-05-07', o06);
    expect(o07.USD.assetPerformance.MA_stock.adjustedDailyChangePercentage)
      .toBeCloseTo(d07.USD.assetPerformance.MA_stock.adjustedDailyChangePercentage, 9);
    expect(o06.USD.adjustedDailyChangePercentage)
      .toBeCloseTo(d06.USD.adjustedDailyChangePercentage, 9);
    expect(o07.COP).toBeDefined();
  });
});

describe('backfill — flujos que no caen en un día hábil o salen de las tenencias', () => {
  test('una compra de cripto en fin de semana es flujo del lunes, no rendimiento', () => {
    const prices = { 'BTC-USD': { '2026-07-24': 65000, '2026-07-25': 64338, '2026-07-26': 64705, '2026-07-27': 64620 } };
    const txns = [
      { type: 'buy', assetName: 'BTC-USD', assetType: 'crypto', date: '2026-07-01T10:00:00Z', amount: 0.00866, price: 60000, currency: 'USD' },
      { type: 'buy', assetName: 'BTC-USD', assetType: 'crypto', date: '2026-07-25T18:15:07Z', amount: 0.00031, price: 64338.38, currency: 'USD' },
      { type: 'buy', assetName: 'BTC-USD', assetType: 'crypto', date: '2026-07-26T18:15:55Z', amount: 0.00147, price: 64705.05, currency: 'USD' },
    ];
    const fri = calculateAccountDayPerformance(txns, '2026-07-24', { USD: 1 }, prices, null);
    const mon = calculateAccountDayPerformance(txns, '2026-07-27', { USD: 1 }, prices, fri);
    const btc = mon.USD.assetPerformance['BTC-USD_crypto'];
    expect(btc.totalCashFlow).toBeCloseTo(-(0.00031 * 64338.38 + 0.00147 * 64705.05), 6);
    // Ni +19,9 %: sólo lo que se movió el precio sobre lo que había y lo comprado
    expect(Math.abs(btc.adjustedDailyChangePercentage)).toBeLessThan(1);
  });

  test('vender todo un activo en una cuenta deja su flujo, con 0 unidades', () => {
    const prices = { AMZN: { '2026-04-28': 260, '2026-04-29': 263.5 }, MA: { '2026-04-28': 500, '2026-04-29': 505 } };
    const txns = [
      { type: 'buy', assetName: 'AMZN', assetType: 'stock', date: '2026-01-05T10:00:00Z', amount: 1, price: 200, currency: 'USD' },
      { type: 'buy', assetName: 'MA', assetType: 'stock', date: '2026-01-05T10:00:00Z', amount: 1, price: 450, currency: 'USD' },
      { type: 'sell', assetName: 'AMZN', assetType: 'stock', date: '2026-04-29T19:55:00Z', amount: 1, price: 263.5, currency: 'USD' },
    ];
    const d28 = calculateAccountDayPerformance(txns, '2026-04-28', { USD: 1 }, prices, null);
    const d29 = calculateAccountDayPerformance(txns, '2026-04-29', { USD: 1 }, prices, d28);
    const amzn = d29.USD.assetPerformance.AMZN_stock;
    expect(amzn).toMatchObject({ units: 0, totalValue: 0 });
    expect(amzn.totalCashFlow).toBeCloseTo(263.5, 6);
    expect(amzn.adjustedDailyChangePercentage).toBeCloseTo((263.5 / 260 - 1) * 100, 6);
    expect(d29.USD.adjustedDailyChangePercentage).toBeCloseTo(((505 + 263.5) / 760 - 1) * 100, 6);
  });

  test('una cuenta que vendió todo ese día devuelve el doc de la venta, no null', () => {
    const prices = { AMZN: { '2026-04-28': 260, '2026-04-29': 263.5 } };
    const txns = [
      { type: 'buy', assetName: 'AMZN', assetType: 'stock', date: '2026-01-05T10:00:00Z', amount: 1, price: 200, currency: 'USD' },
      { type: 'sell', assetName: 'AMZN', assetType: 'stock', date: '2026-04-29T19:55:00Z', amount: 1, price: 263.5, currency: 'USD' },
    ];
    const d28 = calculateAccountDayPerformance(txns, '2026-04-28', { USD: 1 }, prices, null);
    const d29 = calculateAccountDayPerformance(txns, '2026-04-29', { USD: 1 }, prices, d28);
    expect(d29).not.toBeNull();
    expect(d29.USD.totalValue).toBe(0);
    expect(d29.USD.adjustedDailyChangePercentage).toBeCloseTo((263.5 / 260 - 1) * 100, 6);
    // Al día siguiente ya no hay nada
    expect(calculateAccountDayPerformance(txns, '2026-04-30', { USD: 1 }, prices, d29)).toBeNull();
  });
});

describe('staleAssetKeyDeletes', () => {
  test('borra llaves de activos que ya no existen, no las que sólo faltan de precio', () => {
    const existing = { USD: { assetPerformance: { MA_stock: {}, OLD_stock: {}, NOPRICE_stock: {} } } };
    const fresh = { USD: { assetPerformance: { MA_stock: {} } } };
    const held = new Set(['MA_stock', 'NOPRICE_stock']);
    expect(staleAssetKeyDeletes(existing, fresh, held)).toEqual({
      USD: { assetPerformance: { OLD_stock: 'DELETE' } },
    });
  });
});

describe('historicalRangeFor', () => {
  test('cubre un inicio del año anterior aunque esté a menos de 6 meses', () => {
    const fortyDaysAgo = new Date(Date.now() - 40 * 86400000).toISOString().slice(0, 10);
    expect(historicalRangeFor(fortyDaysAgo)).toBe('6mo');
    const tenMonthsAgo = new Date(Date.now() - 300 * 86400000).toISOString().slice(0, 10);
    expect(historicalRangeFor(tenMonthsAgo)).toBe('1y');
  });
});

describe('reconcile — límites del rango', () => {
  test('termina en el último día que cerró el EOD (ayer en Nueva York)', () => {
    const now = DateTime.fromISO('2026-10-15T02:00:00', { zone: 'America/New_York' });
    expect(getLastClosedDate(now)).toBe('2026-10-14');
  });

  test('rehace los consolidados de los meses ya cerrados que tocó', () => {
    const now = DateTime.fromISO('2026-10-15T02:00:00', { zone: 'America/New_York' });
    expect(closedMonthsSince('2026-08-20', now)).toEqual(['2026-08', '2026-09']);
    expect(closedMonthsSince('2026-10-02', now)).toEqual([]);
  });
});
