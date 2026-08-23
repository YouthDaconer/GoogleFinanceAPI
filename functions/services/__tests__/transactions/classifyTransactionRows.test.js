/**
 * Tests for classifyTransactionRows Cloud Function
 *
 * HU 1.3: consulta previa de solo lectura que alimenta la previsualización
 * clasificada.
 *
 * @see platform-docs/stories/1.3-reimportacion-clasificada/
 */

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

jest.mock('firebase-functions/v2/https', () => ({
  onCall: jest.fn((config, handler) => ({ _handler: handler, _config: config })),
  HttpsError: class HttpsError extends Error {
    constructor(code, message) {
      super(message);
      this.code = code;
      this.message = message;
    }
  },
}));

const mockValidateFeatureAccess = jest.fn().mockResolvedValue(undefined);
jest.mock('../../helpers/subscriptionValidator', () => ({
  validateFeatureAccess: (...args) => mockValidateFeatureAccess(...args),
  validateQuantityLimit: jest.fn().mockResolvedValue(undefined),
}));

const mockClassifyRows = jest.fn();
jest.mock('../../transactions/services/rowClassifier', () => ({
  classifyRows: (...args) => mockClassifyRows(...args),
}));

// Firestore: solo se usa para verificar la propiedad de la cuenta
const mockAccountGet = jest.fn();
jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: jest.fn(() => ({
      doc: jest.fn(() => ({ get: (...args) => mockAccountGet(...args) })),
    })),
  })),
}));

const { classifyTransactionRows } = require('../../transactions/classifyTransactionRows');
const { HttpsError } = require('firebase-functions/v2/https');
const { LIMITS } = require('../../transactions/types');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const handler = classifyTransactionRows._handler;

const VALID_ROWS = [
  { originalRowNumber: 1, ticker: 'AAPL', date: '2024-01-15', type: 'buy', amount: 10, price: 150.5 },
];

const VALID_PAYLOAD = {
  portfolioAccountId: 'account-1',
  rows: VALID_ROWS,
};

function call(data, auth = { uid: 'user-1' }) {
  return handler({ auth, data });
}

/** La cuenta pertenece al usuario indicado */
function accountOwnedBy(userId) {
  mockAccountGet.mockResolvedValue({
    exists: true,
    data: () => ({ userId }),
  });
}

describe('classifyTransactionRows', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockValidateFeatureAccess.mockResolvedValue(undefined);
    mockClassifyRows.mockResolvedValue({
      classification: { 1: 'new' },
      counts: { new: 1, existing: 0 },
    });
    accountOwnedBy('user-1');
  });

  // =========================================================================
  // Autenticación y plan
  // =========================================================================

  describe('autenticación y plan', () => {
    test('rechaza peticiones sin autenticar', async () => {
      await expect(handler({ auth: null, data: VALID_PAYLOAD }))
        .rejects.toThrow(HttpsError);
      expect(mockClassifyRows).not.toHaveBeenCalled();
    });

    test('Escenario 10: la restricción por plan se mantiene sin cambios', async () => {
      mockValidateFeatureAccess.mockRejectedValue(
        new HttpsError('permission-denied', 'Esta funcionalidad requiere un plan Pro activo.')
      );

      await expect(call(VALID_PAYLOAD)).rejects.toMatchObject({ code: 'permission-denied' });
      expect(mockClassifyRows).not.toHaveBeenCalled();
    });

    test('valida el feature hasImport del usuario autenticado', async () => {
      await call(VALID_PAYLOAD);

      expect(mockValidateFeatureAccess).toHaveBeenCalledWith('user-1', 'hasImport');
    });
  });

  // =========================================================================
  // Propiedad de la cuenta
  // =========================================================================

  describe('propiedad de la cuenta', () => {
    test('rechaza una cuenta que pertenece a otro usuario', async () => {
      accountOwnedBy('otro-usuario');

      await expect(call(VALID_PAYLOAD)).rejects.toMatchObject({ code: 'permission-denied' });
      expect(mockClassifyRows).not.toHaveBeenCalled();
    });

    test('rechaza una cuenta inexistente', async () => {
      mockAccountGet.mockResolvedValue({ exists: false });

      await expect(call(VALID_PAYLOAD)).rejects.toMatchObject({ code: 'permission-denied' });
    });

    test('un fallo al verificar la cuenta deniega, no permite', async () => {
      mockAccountGet.mockRejectedValue(new Error('firestore down'));

      await expect(call(VALID_PAYLOAD)).rejects.toMatchObject({ code: 'permission-denied' });
      expect(mockClassifyRows).not.toHaveBeenCalled();
    });
  });

  // =========================================================================
  // Validación de payload
  // =========================================================================

  describe('validación de payload', () => {
    test('exige portfolioAccountId', async () => {
      await expect(call({ rows: VALID_ROWS }))
        .rejects.toThrow('Se requiere portfolioAccountId');
    });

    test('rechaza portfolioAccountId no string', async () => {
      await expect(call({ portfolioAccountId: 42, rows: VALID_ROWS }))
        .rejects.toThrow('Se requiere portfolioAccountId');
    });

    test('rechaza rows que no es array', async () => {
      await expect(call({ portfolioAccountId: 'account-1', rows: 'nope' }))
        .rejects.toThrow('rows debe ser un array');
    });

    test('RN-13: respeta el límite de filas por lote del canal', async () => {
      const tooMany = Array.from({ length: LIMITS.maxBatchTransactions + 1 }, (_, i) => ({
        originalRowNumber: i + 1,
        ticker: 'AAPL',
        date: '2024-01-15',
        type: 'buy',
        amount: 1,
        price: 100,
      }));

      await expect(call({ portfolioAccountId: 'account-1', rows: tooMany }))
        .rejects.toThrow(/Máximo/);
      expect(mockClassifyRows).not.toHaveBeenCalled();
    });

    test('acepta exactamente el límite', async () => {
      const atLimit = Array.from({ length: LIMITS.maxBatchTransactions }, (_, i) => ({
        originalRowNumber: i + 1,
        ticker: 'AAPL',
        date: '2024-01-15',
        type: 'buy',
        amount: 1,
        price: 100,
      }));

      await expect(call({ portfolioAccountId: 'account-1', rows: atLimit })).resolves.toBeDefined();
    });

    test('tolera payload vacío', async () => {
      await expect(call(undefined)).rejects.toThrow('Se requiere portfolioAccountId');
    });

    test('una lista vacía es válida y devuelve conteos en cero', async () => {
      mockClassifyRows.mockResolvedValue({ classification: {}, counts: { new: 0, existing: 0 } });

      const result = await call({ portfolioAccountId: 'account-1', rows: [] });

      expect(result.counts).toEqual({ new: 0, existing: 0 });
    });
  });

  // =========================================================================
  // Resultado
  // =========================================================================

  describe('resultado', () => {
    test('devuelve la clasificación y los conteos', async () => {
      mockClassifyRows.mockResolvedValue({
        classification: { 1: 'existing', 2: 'new' },
        counts: { new: 1, existing: 1 },
      });

      const result = await call(VALID_PAYLOAD);

      expect(result.success).toBe(true);
      expect(result.classification).toEqual({ 1: 'existing', 2: 'new' });
      expect(result.counts).toEqual({ new: 1, existing: 1 });
    });

    test('usa el uid del token, nunca un userId del payload', async () => {
      accountOwnedBy('user-real');

      await call({ ...VALID_PAYLOAD, userId: 'otro' }, { uid: 'user-real' });

      expect(mockClassifyRows).toHaveBeenCalledWith(
        VALID_ROWS,
        'user-real',
        'account-1'
      );
    });

    test('pasa la cuenta destino al clasificador (la firma la incluye)', async () => {
      await call(VALID_PAYLOAD);

      expect(mockClassifyRows).toHaveBeenCalledWith(
        expect.any(Array),
        expect.any(String),
        'account-1'
      );
    });
  });

  // =========================================================================
  // Configuración
  // =========================================================================

  describe('configuración de la función', () => {
    test('es una operación de lectura acotada', () => {
      expect(classifyTransactionRows._config.memory).toBe('256MiB');
      expect(classifyTransactionRows._config.timeoutSeconds).toBeLessThanOrEqual(300);
    });
  });
});
