/**
 * readExistingDocs: los docs ya guardados en el rango, de cada cuenta y del
 * portafolio. El recálculo los usa para borrar llaves y días sobrantes y para
 * deshacer la segunda pasada. Antes se probaba a través del respaldo de
 * SCALE-003 (`capturePreBackfillSnapshot`), retirado el 2026-11-23.
 *
 * @module __tests__/services/backfillCoreModule.readExistingDocs.test
 */

// Mock firebaseAdmin before importing
const mockGet = jest.fn();
const mockSet = jest.fn().mockResolvedValue();
const mockWhere = jest.fn();
const mockDoc = jest.fn();
const mockCollection = jest.fn();

jest.mock('../firebaseAdmin', () => {
  mockWhere.mockReturnThis();
  
  mockDoc.mockReturnValue({
    collection: mockCollection,
    set: mockSet,
  });

  const collectionReturn = {
    where: mockWhere,
    get: mockGet,
    doc: mockDoc,
  };

  mockCollection.mockReturnValue(collectionReturn);

  return {
    firestore: jest.fn(() => ({
      collection: mockCollection,
      doc: mockDoc,
    })),
  };
});

// Mock node-fetch
jest.mock('node-fetch', () => {
  const fn = jest.fn().mockResolvedValue({ json: () => ({}) });
  fn.default = fn;
  return fn;
});

const { readExistingDocs } = require('../backfillCoreModule');

// ============================================================================
// HELPERS
// ============================================================================

function buildFirestoreDoc(id, data) {
  return {
    id,
    data: () => data,
    ref: { path: `portfolioPerformance/user1/dates/${id}` },
  };
}

function buildEmptySnapshot() {
  return { empty: true, docs: [] };
}

function buildSnapshot(docs) {
  return { empty: docs.length === 0, docs };
}

// ============================================================================
// TESTS
// ============================================================================

describe('readExistingDocs', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('sin docs en el rango devuelve todo vacío', async () => {
    mockGet.mockResolvedValue(buildEmptySnapshot());

    const result = await readExistingDocs('user1', [{ id: 'acc1' }], ['2026-03-01', '2026-03-05']);

    expect(result).toEqual({ accounts: {}, overall: {}, totalDocs: 0 });
  });

  test('devuelve los docs completos de la cuenta y del portafolio', async () => {
    const accountDoc = buildFirestoreDoc('2026-03-01', { date: '2026-03-01', USD: { totalValue: 1000 } });
    const overallDoc = buildFirestoreDoc('2026-03-01', { date: '2026-03-01', USD: { totalValue: 1000 } });

    // Primero la consulta de la cuenta, después la del portafolio
    mockGet
      .mockResolvedValueOnce(buildSnapshot([accountDoc]))
      .mockResolvedValueOnce(buildSnapshot([overallDoc]));

    const result = await readExistingDocs('user1', [{ id: 'acc1' }], ['2026-03-01', '2026-03-05']);

    expect(result.totalDocs).toBe(2);
    expect(result.accounts.acc1['2026-03-01']).toEqual({ date: '2026-03-01', USD: { totalValue: 1000 } });
    expect(result.overall['2026-03-01']).toEqual({ date: '2026-03-01', USD: { totalValue: 1000 } });
  });

  test('consulta del primer al último día del rango', async () => {
    mockGet.mockResolvedValue(buildEmptySnapshot());

    await readExistingDocs('user1', [{ id: 'acc1' }], ['2026-03-01', '2026-03-03', '2026-03-05']);

    expect(mockWhere).toHaveBeenCalledWith('date', '>=', '2026-03-01');
    expect(mockWhere).toHaveBeenCalledWith('date', '<=', '2026-03-05');
  });

  test('no recorta: con muchos docs devuelve todas las cuentas', async () => {
    // El respaldo retirado recortaba a solo el portafolio por encima de ~67 docs
    const manyAccountDocs = Array.from({ length: 68 }, (_, i) => {
      const date = `2026-03-${String(i + 1).padStart(2, '0')}`;
      return buildFirestoreDoc(date, { date, USD: { totalValue: 1000 } });
    });
    const overallDoc = buildFirestoreDoc('2026-03-01', { date: '2026-03-01', USD: { totalValue: 1000 } });

    mockGet
      .mockResolvedValueOnce(buildSnapshot(manyAccountDocs))
      .mockResolvedValueOnce(buildSnapshot([overallDoc]));

    const result = await readExistingDocs('user1', [{ id: 'acc1' }], ['2026-03-01', '2026-03-05']);

    expect(Object.keys(result.accounts.acc1)).toHaveLength(68);
    expect(result.totalDocs).toBe(69);
  });

  test('separa los docs de varias cuentas', async () => {
    const acc1Doc = buildFirestoreDoc('2026-03-01', { date: '2026-03-01', USD: { totalValue: 500 } });
    const acc2Doc = buildFirestoreDoc('2026-03-01', { date: '2026-03-01', USD: { totalValue: 700 } });
    const overallDoc = buildFirestoreDoc('2026-03-01', { date: '2026-03-01', USD: { totalValue: 1200 } });

    mockGet
      .mockResolvedValueOnce(buildSnapshot([acc1Doc]))
      .mockResolvedValueOnce(buildSnapshot([acc2Doc]))
      .mockResolvedValueOnce(buildSnapshot([overallDoc]));

    const result = await readExistingDocs('user1', [{ id: 'acc1' }, { id: 'acc2' }], ['2026-03-01']);

    expect(result.totalDocs).toBe(3);
    expect(result.accounts.acc1['2026-03-01'].USD.totalValue).toBe(500);
    expect(result.accounts.acc2['2026-03-01'].USD.totalValue).toBe(700);
    expect(result.overall['2026-03-01'].USD.totalValue).toBe(1200);
  });
});
