/**
 * Tests for rowClassifier service
 *
 * HU 1.3: clasificación de la reimportación en nuevas / ya registradas ANTES de
 * confirmar, reutilizando el criterio de duplicado vigente del canal.
 *
 * @see platform-docs/stories/1.3-reimportacion-clasificada/
 */

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Solo se mockea la LECTURA de Firestore. `createSignature` se usa de verdad:
// el objetivo es comprobar que se respeta el criterio vigente, no reimplementarlo.
const mockGetExistingTransactionsForTicker = jest.fn();

jest.mock('../../transactions/services/duplicateDetector', () => {
  const actual = jest.requireActual('../../transactions/services/duplicateDetector');

  return {
    ...actual,
    getExistingTransactionsForTicker: (...args) => mockGetExistingTransactionsForTicker(...args),
  };
});

jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: jest.fn(() => ({
      where: jest.fn(() => ({
        where: jest.fn(() => ({ get: jest.fn() })),
      })),
    })),
  })),
}));

const { classifyRows, ROW_GROUP } = require('../../transactions/services/rowClassifier');

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const ACCOUNT = 'account-1';
const USER = 'user-1';

/** Fila del archivo que se va a clasificar */
function row(rowNumber, ticker, date, type, amount, price) {
  return { originalRowNumber: rowNumber, ticker, date, type, amount, price };
}

/** Transacción tal como está guardada en Firestore */
function stored(ticker, date, type, amount, price, accountId = ACCOUNT) {
  return {
    assetName: ticker,
    date,
    type,
    amount,
    price,
    portfolioAccountId: accountId,
  };
}

/** Devuelve `existing` para cualquier ticker consultado */
function withHistory(existing) {
  mockGetExistingTransactionsForTicker.mockImplementation(async (ticker) =>
    existing.filter(tx => tx.assetName.toUpperCase() === ticker.toUpperCase())
  );
}

describe('rowClassifier', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    withHistory([]);
  });

  // =========================================================================
  // Escenario 1 — reimportación clasificada
  // =========================================================================

  describe('Escenario 1: reimportación de un archivo actualizado', () => {
    test('separa lo que ya está registrado de lo nuevo', async () => {
      withHistory([
        stored('AAPL', '2024-01-15', 'buy', 10, 150.5),
        stored('MSFT', '2024-01-16', 'buy', 5, 375),
      ]);

      const rows = [
        row(1, 'AAPL', '2024-01-15', 'buy', 10, 150.5),   // ya registrada
        row(2, 'MSFT', '2024-01-16', 'buy', 5, 375),       // ya registrada
        row(3, 'NVDA', '2024-02-01', 'buy', 3, 700),       // nueva
      ];

      const { classification, counts } = await classifyRows(rows, USER, ACCOUNT);

      expect(classification[1]).toBe(ROW_GROUP.EXISTING);
      expect(classification[2]).toBe(ROW_GROUP.EXISTING);
      expect(classification[3]).toBe(ROW_GROUP.NEW);
      expect(counts).toEqual({ new: 1, existing: 2 });
    });

    test('consulta una vez por ticker distinto, no por fila (RN-13)', async () => {
      const rows = [
        row(1, 'AAPL', '2024-01-15', 'buy', 10, 150),
        row(2, 'AAPL', '2024-01-16', 'buy', 10, 151),
        row(3, 'AAPL', '2024-01-17', 'buy', 10, 152),
        row(4, 'MSFT', '2024-01-15', 'buy', 5, 375),
      ];

      await classifyRows(rows, USER, ACCOUNT);

      expect(mockGetExistingTransactionsForTicker).toHaveBeenCalledTimes(2);
    });
  });

  // =========================================================================
  // Escenario 3 — archivo completamente repetido
  // =========================================================================

  describe('Escenario 3: archivo completamente repetido', () => {
    test('todas las filas quedan como ya registradas', async () => {
      withHistory([
        stored('AAPL', '2024-01-15', 'buy', 10, 150.5),
        stored('MSFT', '2024-01-16', 'buy', 5, 375),
      ]);

      const rows = [
        row(1, 'AAPL', '2024-01-15', 'buy', 10, 150.5),
        row(2, 'MSFT', '2024-01-16', 'buy', 5, 375),
      ];

      const { counts } = await classifyRows(rows, USER, ACCOUNT);

      expect(counts).toEqual({ new: 0, existing: 2 });
    });
  });

  // =========================================================================
  // Escenario 5 — duplicado contra transacción manual (RN-19)
  // =========================================================================

  describe('Escenario 5: duplicado contra una transacción registrada manualmente', () => {
    test('se detecta igual, sin importar el origen de la transacción original', async () => {
      // Una transacción manual no tiene importSessionId ni importSource.
      // El criterio no mira el origen, solo la firma.
      withHistory([
        { assetName: 'AAPL', date: '2024-01-15', type: 'buy', amount: 10, price: 150.5, portfolioAccountId: ACCOUNT },
      ]);

      const { classification } = await classifyRows(
        [row(1, 'AAPL', '2024-01-15', 'buy', 10, 150.5)],
        USER,
        ACCOUNT
      );

      expect(classification[1]).toBe(ROW_GROUP.EXISTING);
    });
  });

  // =========================================================================
  // Criterio vigente: conteo de ocurrencias
  // =========================================================================

  describe('conteo de ocurrencias (criterio vigente del canal)', () => {
    test('tres ventas idénticas legítimas no se marcan como repetidas', async () => {
      // El usuario vendió tres lotes del mismo activo el mismo día al mismo precio.
      // Son operaciones distintas, no duplicados.
      withHistory([]);

      const rows = [
        row(1, 'AAPL', '2024-01-15', 'sell', 10, 150.5),
        row(2, 'AAPL', '2024-01-15', 'sell', 10, 150.5),
        row(3, 'AAPL', '2024-01-15', 'sell', 10, 150.5),
      ];

      const { counts } = await classifyRows(rows, USER, ACCOUNT);

      expect(counts).toEqual({ new: 3, existing: 0 });
    });

    test('si una de las tres ya existe, solo esa se marca como registrada', async () => {
      withHistory([stored('AAPL', '2024-01-15', 'sell', 10, 150.5)]);

      const rows = [
        row(1, 'AAPL', '2024-01-15', 'sell', 10, 150.5),
        row(2, 'AAPL', '2024-01-15', 'sell', 10, 150.5),
        row(3, 'AAPL', '2024-01-15', 'sell', 10, 150.5),
      ];

      const { counts } = await classifyRows(rows, USER, ACCOUNT);

      expect(counts).toEqual({ new: 2, existing: 1 });
    });

    test('si el historial tiene más ocurrencias que el archivo, todas quedan registradas', async () => {
      withHistory([
        stored('AAPL', '2024-01-15', 'sell', 10, 150.5),
        stored('AAPL', '2024-01-15', 'sell', 10, 150.5),
        stored('AAPL', '2024-01-15', 'sell', 10, 150.5),
      ]);

      const { counts } = await classifyRows(
        [row(1, 'AAPL', '2024-01-15', 'sell', 10, 150.5)],
        USER,
        ACCOUNT
      );

      expect(counts).toEqual({ new: 0, existing: 1 });
    });
  });

  // =========================================================================
  // Discriminación por atributos de la firma
  // =========================================================================

  describe('atributos que hacen distinta a una operación', () => {
    const existing = [stored('AAPL', '2024-01-15', 'buy', 10, 150.5)];

    test('una fecha distinta es una operación nueva', async () => {
      withHistory(existing);
      const { classification } = await classifyRows(
        [row(1, 'AAPL', '2024-01-16', 'buy', 10, 150.5)], USER, ACCOUNT
      );
      expect(classification[1]).toBe(ROW_GROUP.NEW);
    });

    test('una cantidad distinta es una operación nueva', async () => {
      withHistory(existing);
      const { classification } = await classifyRows(
        [row(1, 'AAPL', '2024-01-15', 'buy', 11, 150.5)], USER, ACCOUNT
      );
      expect(classification[1]).toBe(ROW_GROUP.NEW);
    });

    test('un precio distinto es una operación nueva', async () => {
      withHistory(existing);
      const { classification } = await classifyRows(
        [row(1, 'AAPL', '2024-01-15', 'buy', 10, 151)], USER, ACCOUNT
      );
      expect(classification[1]).toBe(ROW_GROUP.NEW);
    });

    test('un tipo distinto es una operación nueva', async () => {
      withHistory(existing);
      const { classification } = await classifyRows(
        [row(1, 'AAPL', '2024-01-15', 'sell', 10, 150.5)], USER, ACCOUNT
      );
      expect(classification[1]).toBe(ROW_GROUP.NEW);
    });

    test('la misma operación en OTRA cuenta es nueva', async () => {
      // Comprar AAPL el mismo día en IBKR y en XTB son dos operaciones reales
      withHistory([stored('AAPL', '2024-01-15', 'buy', 10, 150.5, 'otra-cuenta')]);

      const { classification } = await classifyRows(
        [row(1, 'AAPL', '2024-01-15', 'buy', 10, 150.5)], USER, ACCOUNT
      );

      expect(classification[1]).toBe(ROW_GROUP.NEW);
    });

    test('el ticker se compara sin distinguir mayúsculas', async () => {
      withHistory([stored('AAPL', '2024-01-15', 'buy', 10, 150.5)]);

      const { classification } = await classifyRows(
        [row(1, 'aapl', '2024-01-15', 'buy', 10, 150.5)], USER, ACCOUNT
      );

      expect(classification[1]).toBe(ROW_GROUP.EXISTING);
    });
  });

  // =========================================================================
  // Escenario 8 — primera importación
  // =========================================================================

  describe('Escenario 8: primera importación, sin regresión', () => {
    test('sin historial todas las filas son nuevas', async () => {
      withHistory([]);

      const rows = [
        row(1, 'AAPL', '2024-01-15', 'buy', 10, 150.5),
        row(2, 'MSFT', '2024-01-16', 'buy', 5, 375),
      ];

      const { counts } = await classifyRows(rows, USER, ACCOUNT);

      expect(counts).toEqual({ new: 2, existing: 0 });
    });

    test('sin filas no consulta nada', async () => {
      const result = await classifyRows([], USER, ACCOUNT);

      expect(result).toEqual({ classification: {}, counts: { new: 0, existing: 0 } });
      expect(mockGetExistingTransactionsForTicker).not.toHaveBeenCalled();
    });

    test('tolera entradas no-array', async () => {
      const result = await classifyRows(null, USER, ACCOUNT);

      expect(result.counts).toEqual({ new: 0, existing: 0 });
    });
  });

  // =========================================================================
  // Criterio vigente: transacciones heredadas sin cuenta
  // =========================================================================

  describe('transacciones heredadas sin cuenta', () => {
    test('no se consideran duplicado, igual que hoy', async () => {
      // El canal calcula la firma con `portfolioAccountId || ''`. Una transacción
      // antigua sin ese campo produce una firma distinta. La historia expone el
      // criterio vigente, no lo cambia.
      withHistory([
        { assetName: 'AAPL', date: '2024-01-15', type: 'buy', amount: 10, price: 150.5 },
      ]);

      const { classification } = await classifyRows(
        [row(1, 'AAPL', '2024-01-15', 'buy', 10, 150.5)], USER, ACCOUNT
      );

      expect(classification[1]).toBe(ROW_GROUP.NEW);
    });
  });
});
