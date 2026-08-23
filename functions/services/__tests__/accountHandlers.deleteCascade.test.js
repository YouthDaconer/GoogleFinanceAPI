/**
 * FIX-DELETE-002: Tests del borrado en cascada de deletePortfolioAccount
 *
 * Dos defectos que dejaban assets huerfanos al eliminar una cuenta:
 *
 *   1. Los assets se borraban en UN SOLO batch. Firestore admite 500
 *      operaciones por batch, asi que una cuenta con mas de 500 assets hacia
 *      fallar el commit. Como los assets son el paso 1 de la cascada, al lanzar
 *      abortaba todo: no se borraban ni transacciones ni la cuenta.
 *
 *   2. La consulta solo miraba el campo `portfolioAccount`. Los assets escritos
 *      antes de FIX-DELETE-001 usan `portfolioAccountId`, asi que quedaban
 *      invisibles al borrado y huerfanos para siempre.
 */

const { HttpsError } = require("firebase-functions/v2/https");

// ============================================================================
// MOCKS
// ============================================================================

const mockBatchDelete = jest.fn();
const mockBatchCommit = jest.fn();
/** Registra el tamaño de cada batch para poder comprobar el troceado */
const batchSizes = [];

const mockBatch = jest.fn(() => {
  let size = 0;
  return {
    delete: (...args) => { size++; mockBatchDelete(...args); },
    commit: () => { batchSizes.push(size); return mockBatchCommit(); },
  };
});

const mockAccountGet = jest.fn();
const mockTransactionsGet = jest.fn();
const mockDistributionGet = jest.fn();
const mockSnapshotsGet = jest.fn();
const mockDocDelete = jest.fn().mockResolvedValue();
const mockDocUpdate = jest.fn().mockResolvedValue();

/** Por nombre de campo -> resultado de la consulta de assets */
const assetsByField = {};
/** Campos por los que se consulto la coleccion assets */
const assetFieldsQueried = [];

jest.mock("firebase-admin/firestore", () => {
  const FieldValue = {
    serverTimestamp: () => "SERVER_TIMESTAMP",
    delete: () => "FIELD_DELETE",
  };

  const getFirestore = () => ({
    collection: jest.fn((name) => {
      if (name === 'portfolioAccounts') {
        return { doc: jest.fn(() => ({ get: mockAccountGet, delete: mockDocDelete })) };
      }
      if (name === 'assets') {
        // Captura el nombre del campo del primer where para devolver el
        // resultado correspondiente
        return {
          where: jest.fn((field) => {
            assetFieldsQueried.push(field);
            return {
              where: jest.fn(() => ({
                get: jest.fn().mockResolvedValue(
                  assetsByField[field] || { empty: true, docs: [] }
                ),
              })),
            };
          }),
        };
      }
      if (name === 'transactions') {
        return { where: jest.fn(() => ({ get: mockTransactionsGet })) };
      }
      if (name === 'portfolioDistribution') {
        return { doc: jest.fn(() => ({ get: mockDistributionGet, update: mockDocUpdate })) };
      }
      if (name === 'performanceSnapshots') {
        return { where: jest.fn(() => ({ where: jest.fn(() => ({ get: mockSnapshotsGet })) })) };
      }
      return { doc: jest.fn(() => ({ get: jest.fn(), set: jest.fn() })) };
    }),
    batch: mockBatch,
  });

  return { getFirestore, FieldValue };
});

jest.mock("../portfolioDistributionService", () => ({
  invalidateDistributionCache: jest.fn(),
}));

jest.mock("../helpers/subscriptionValidator", () => ({
  validateQuantityLimit: jest.fn(),
  validateFeatureAccess: jest.fn(),
}));

const { deletePortfolioAccount } = require("../handlers/accountHandlers");

// ============================================================================
// HELPERS
// ============================================================================

/** Genera n documentos falsos con ids unicos */
function makeDocs(n, prefix = 'a') {
  return Array.from({ length: n }, (_, i) => ({
    id: `${prefix}${i}`,
    ref: { id: `${prefix}${i}` },
  }));
}

function setAssets({ portfolioAccount = [], portfolioAccountId = [] }) {
  assetsByField.portfolioAccount = { empty: !portfolioAccount.length, docs: portfolioAccount };
  assetsByField.portfolioAccountId = { empty: !portfolioAccountId.length, docs: portfolioAccountId };
}

const CONTEXT = { auth: { uid: 'user-1' } };
const PAYLOAD = { accountId: 'acc-123' };

describe('FIX-DELETE-002: borrado en cascada de la cuenta', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    batchSizes.length = 0;
    assetFieldsQueried.length = 0;
    Object.keys(assetsByField).forEach(k => delete assetsByField[k]);

    mockAccountGet.mockResolvedValue({
      exists: true,
      data: () => ({ userId: 'user-1', name: 'Cuenta de prueba' }),
    });
    mockTransactionsGet.mockResolvedValue({ empty: true, docs: [] });
    mockDistributionGet.mockResolvedValue({ exists: false });
    mockSnapshotsGet.mockResolvedValue({ empty: true, docs: [] });
    mockBatchCommit.mockResolvedValue();
    setAssets({});
  });

  // =========================================================================
  // Defecto 1 — troceado en batches de 500
  // =========================================================================

  describe('troceado de assets en batches de 500', () => {
    test('una cuenta con 1367 assets se borra completa, no falla', async () => {
      // 1367 es el numero real que dejo huerfana una cuenta en produccion
      setAssets({ portfolioAccount: makeDocs(1367) });

      const result = await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(result.success).toBe(true);
      expect(result.deletedAssets).toBe(1367);
    });

    test('ningun batch excede el limite de 500 de Firestore', async () => {
      setAssets({ portfolioAccount: makeDocs(1367) });

      await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(batchSizes.length).toBeGreaterThan(1);
      batchSizes.forEach(size => expect(size).toBeLessThanOrEqual(500));
    });

    test('trocea en 500 + 500 + 367', async () => {
      setAssets({ portfolioAccount: makeDocs(1367) });

      await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(batchSizes).toEqual([500, 500, 367]);
    });

    test('exactamente 500 assets cabe en un solo batch', async () => {
      setAssets({ portfolioAccount: makeDocs(500) });

      await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(batchSizes).toEqual([500]);
    });

    test('501 assets requieren dos batches', async () => {
      setAssets({ portfolioAccount: makeDocs(501) });

      await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(batchSizes).toEqual([500, 1]);
    });

    test('las transacciones tambien se trocean', async () => {
      mockTransactionsGet.mockResolvedValue({ empty: false, docs: makeDocs(750, 't') });

      const result = await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(result.deletedTransactions).toBe(750);
      expect(batchSizes).toEqual([500, 250]);
    });

    test('los snapshots tambien se trocean', async () => {
      mockSnapshotsGet.mockResolvedValue({ empty: false, docs: makeDocs(600, 's') });

      await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(batchSizes).toEqual([500, 100]);
    });
  });

  // =========================================================================
  // Defecto 2 — ambos nombres de campo
  // =========================================================================

  describe('cobertura de los dos nombres de campo historicos', () => {
    test('se consultan portfolioAccount Y portfolioAccountId', async () => {
      await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(assetFieldsQueried).toContain('portfolioAccount');
      expect(assetFieldsQueried).toContain('portfolioAccountId');
    });

    test('borra los assets legacy escritos con portfolioAccountId', async () => {
      // Es el caso de los 450 assets huerfanos encontrados en produccion
      setAssets({ portfolioAccountId: makeDocs(450, 'legacy') });

      const result = await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(result.deletedAssets).toBe(450);
    });

    test('borra los de ambos campos sumandolos', async () => {
      setAssets({
        portfolioAccount: makeDocs(10, 'nuevo'),
        portfolioAccountId: makeDocs(5, 'legacy'),
      });

      const result = await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(result.deletedAssets).toBe(15);
    });

    test('un asset que aparece en las DOS consultas se borra una sola vez', async () => {
      // Un documento con ambos campos poblados saldria en las dos consultas.
      // Sin deduplicar, el batch intentaria borrarlo dos veces.
      const shared = makeDocs(3, 'ambos');
      setAssets({ portfolioAccount: shared, portfolioAccountId: shared });

      const result = await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(result.deletedAssets).toBe(3);
      expect(mockBatchDelete).toHaveBeenCalledTimes(3);
    });
  });

  // =========================================================================
  // No regresion de las guardas existentes
  // =========================================================================

  describe('guardas de seguridad', () => {
    test('sin accountId lanza invalid-argument', async () => {
      await expect(deletePortfolioAccount(CONTEXT, {})).rejects.toThrow(HttpsError);
    });

    test('una cuenta inexistente lanza not-found', async () => {
      mockAccountGet.mockResolvedValue({ exists: false });

      await expect(deletePortfolioAccount(CONTEXT, PAYLOAD)).rejects.toThrow(HttpsError);
    });

    test('la cuenta de otro usuario lanza permission-denied', async () => {
      mockAccountGet.mockResolvedValue({
        exists: true,
        data: () => ({ userId: 'otro-usuario', name: 'Ajena' }),
      });

      await expect(deletePortfolioAccount(CONTEXT, PAYLOAD))
        .rejects.toMatchObject({ code: 'permission-denied' });
    });

    test('una cuenta sin datos asociados se borra sin error', async () => {
      const result = await deletePortfolioAccount(CONTEXT, PAYLOAD);

      expect(result.success).toBe(true);
      expect(result.deletedAssets).toBe(0);
      expect(result.deletedTransactions).toBe(0);
      expect(mockDocDelete).toHaveBeenCalled();
    });

    test('la cuenta se borra DESPUES de sus datos', async () => {
      setAssets({ portfolioAccount: makeDocs(3) });

      await deletePortfolioAccount(CONTEXT, PAYLOAD);

      // Si el borrado de assets fallara, la cuenta no debe desaparecer:
      // es lo que dejaba huerfanos irrecuperables.
      expect(mockBatchCommit).toHaveBeenCalled();
      expect(mockDocDelete).toHaveBeenCalled();
    });

    test('si el borrado de assets falla, la cuenta NO se borra', async () => {
      setAssets({ portfolioAccount: makeDocs(600) });
      mockBatchCommit.mockRejectedValue(new Error('maximum 500 writes allowed per request'));

      await expect(deletePortfolioAccount(CONTEXT, PAYLOAD)).rejects.toThrow();
      expect(mockDocDelete).not.toHaveBeenCalled();
    });
  });
});
