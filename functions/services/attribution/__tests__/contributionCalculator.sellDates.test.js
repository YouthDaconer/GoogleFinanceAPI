/**
 * FIX-ATTR-HOLDINGS-001: las fechas de las transacciones son ISO con hora.
 * Compararlas enteras contra 'YYYY-MM-DD' dejaba fuera las ventas del último
 * día del período ('2026-11-10T15:20:41Z' > '2026-11-10').
 */

const mockSells = [];

jest.mock('../../firebaseAdmin', () => {
  const query = () => ({
    where: () => query(),
    get: async () => ({ docs: mockSells.map((data, i) => ({ id: `t${i}`, data: () => data })) }),
  });
  const db = { collection: () => query(), doc: () => ({ get: async () => ({ exists: false }) }) };
  const firestore = () => db;
  firestore.FieldValue = { serverTimestamp: () => null };
  return { firestore };
});

jest.mock('../../financeQuery', () => ({ getQuotes: jest.fn(async () => []) }));

const { getSellTransactionsInPeriod, transactionDateStr } = require('../contributionCalculator');

describe('transactionDateStr', () => {
  it('normaliza string ISO, fecha simple y Timestamp', () => {
    expect(transactionDateStr('2026-11-10T15:20:41.053Z')).toBe('2026-11-10');
    expect(transactionDateStr('2026-11-10')).toBe('2026-11-10');
    expect(transactionDateStr({ toDate: () => new Date('2026-11-10T12:00:00Z') })).toBe('2026-11-10');
    expect(transactionDateStr(undefined)).toBeNull();
  });
});

describe('getSellTransactionsInPeriod', () => {
  beforeEach(() => {
    mockSells.length = 0;
    mockSells.push(
      { assetName: 'MSFT', assetType: 'stock', date: '2026-11-10T15:20:41.053Z', amount: 1, price: 500, valuePnL: 50, currency: 'USD' },
      { assetName: 'MSFT', assetType: 'stock', date: '2026-10-31T13:00:00.000Z', amount: 1, price: 480, valuePnL: 30, currency: 'USD' },
      { assetName: 'MSFT', assetType: 'stock', date: '2026-11-11T09:00:00.000Z', amount: 1, price: 510, valuePnL: 60, currency: 'USD' },
      { assetName: 'MSFT', assetType: 'stock', date: '2026-09-30T09:00:00.000Z', amount: 1, price: 470, valuePnL: 20, currency: 'USD' },
    );
  });

  it('incluye las ventas del último día del período y excluye las de fuera', async () => {
    const sells = await getSellTransactionsInPeriod(
      'u1',
      new Date('2026-10-01T00:00:00Z'),
      new Date('2026-11-10T20:00:00Z'),
      ['acc-1'],
      'USD'
    );
    expect(sells.MSFT_stock.transactions).toHaveLength(2);
    expect(sells.MSFT_stock.totalRealizedPnL).toBe(80);
  });
});
