/**
 * Tests for importMemoryRepository — ticker equivalences surface
 *
 * HU 1.2: memoria propia de equivalencias de símbolo del usuario.
 *
 * Vive en su propio archivo porque las equivalencias necesitan mockear `getAll` y
 * `batch` del cliente de Firestore, mientras que los tests de perfil (HU 1.1) solo
 * necesitan `collection().doc()`.
 *
 * @see platform-docs/stories/1.2-memoria-equivalencias-simbolo-usuario/
 */

// ---------------------------------------------------------------------------
// Firestore mock: userData/{userId}/tickerEquivalences/{formato::SÍMBOLO}
// ---------------------------------------------------------------------------

const mockEquivalenceDoc = jest.fn(() => ({}));
const mockSubCollection = jest.fn(() => ({ doc: mockEquivalenceDoc }));
const mockUserDoc = jest.fn(() => ({ collection: mockSubCollection }));
const mockRootCollection = jest.fn(() => ({ doc: mockUserDoc }));

const mockGetAll = jest.fn();
const mockBatchSet = jest.fn();
const mockBatchCommit = jest.fn();
const mockBatch = jest.fn(() => ({ set: mockBatchSet, commit: mockBatchCommit }));

jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: (...args) => mockRootCollection(...args),
    getAll: (...args) => mockGetAll(...args),
    batch: (...args) => mockBatch(...args),
  })),
}));

const {
  getUserEquivalences,
  saveUserEquivalences,
  buildEquivalenceKey,
  sanitizeEquivalence,
  normalizeSymbol,
  EQUIVALENCES_SUBCOLLECTION,
} = require('../../transactions/services/importMemoryRepository');

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const VUAA = {
  sourceSymbol: 'VUAA',
  resolvedSymbol: 'VUAA.L',
  assetType: 'etf',
  currency: 'GBP',
  exchange: 'LSE',
  name: 'Vanguard S&P 500 UCITS ETF',
};

function existingDoc(sourceSymbol, resolvedSymbol, extra = {}) {
  return {
    exists: true,
    data: () => ({
      sourceSymbol,
      resolvedSymbol,
      assetType: 'etf',
      currency: 'GBP',
      exchange: 'LSE',
      name: 'Vanguard S&P 500 UCITS ETF',
      ...extra,
    }),
  };
}

describe('importMemoryRepository — equivalencias (HU 1.2)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockEquivalenceDoc.mockReturnValue({});
    mockSubCollection.mockReturnValue({ doc: mockEquivalenceDoc });
    mockUserDoc.mockReturnValue({ collection: mockSubCollection });
    mockRootCollection.mockReturnValue({ doc: mockUserDoc });
    mockBatch.mockReturnValue({ set: mockBatchSet, commit: mockBatchCommit });
    mockBatchCommit.mockResolvedValue(undefined);
    mockGetAll.mockResolvedValue([]);
  });

  // =========================================================================
  // Clave compuesta — RN-16
  // =========================================================================

  describe('buildEquivalenceKey (RN-16)', () => {
    test('la clave incluye el formato de origen y el símbolo', () => {
      expect(buildEquivalenceKey('broker:degiro', 'VUAA')).toBe('broker:degiro::VUAA');
    });

    test('normaliza el símbolo a mayúsculas y sin espacios', () => {
      expect(buildEquivalenceKey('broker:degiro', '  vuaa  ')).toBe('broker:degiro::VUAA');
    });

    test('Escenario 3: el mismo símbolo en dos formatos produce claves distintas', () => {
      const cripto = buildEquivalenceKey('broker:binance', 'ETH');
      const equity = buildEquivalenceKey('broker:interactive_brokers', 'ETH');

      expect(cripto).not.toBe(equity);
    });
  });

  describe('normalizeSymbol', () => {
    test('recorta y pasa a mayúsculas', () => {
      expect(normalizeSymbol(' vuaa ')).toBe('VUAA');
    });

    test('tolera null y undefined', () => {
      expect(normalizeSymbol(null)).toBe('');
      expect(normalizeSymbol(undefined)).toBe('');
    });
  });

  // =========================================================================
  // Lectura
  // =========================================================================

  describe('getUserEquivalences', () => {
    test('Escenario 1: devuelve la equivalencia recordada del símbolo', async () => {
      mockGetAll.mockResolvedValue([existingDoc('VUAA', 'VUAA.L')]);

      const result = await getUserEquivalences('user-1', 'broker:degiro', ['VUAA']);

      expect(result.VUAA).toMatchObject({
        sourceSymbol: 'VUAA',
        resolvedSymbol: 'VUAA.L',
        assetType: 'etf',
        currency: 'GBP',
        origin: 'user',
      });
    });

    test('lee de la subcolección correcta', async () => {
      await getUserEquivalences('user-1', 'broker:degiro', ['VUAA']);

      expect(mockRootCollection).toHaveBeenCalledWith('userData');
      expect(mockUserDoc).toHaveBeenCalledWith('user-1');
      expect(mockSubCollection).toHaveBeenCalledWith(EQUIVALENCES_SUBCOLLECTION);
    });

    test('Escenario 3: consulta la clave del formato cargado, no la de otro broker', async () => {
      await getUserEquivalences('user-1', 'broker:binance', ['ETH']);

      expect(mockEquivalenceDoc).toHaveBeenCalledWith('broker:binance::ETH');
      expect(mockEquivalenceDoc).not.toHaveBeenCalledWith('broker:interactive_brokers::ETH');
    });

    test('Escenario 8: sin equivalencias devuelve un mapa vacío', async () => {
      mockGetAll.mockResolvedValue([{ exists: false }]);

      expect(await getUserEquivalences('user-1', 'broker:degiro', ['AAPL'])).toEqual({});
    });

    test('descarta documentos corruptos sin resolvedSymbol', async () => {
      mockGetAll.mockResolvedValue([
        { exists: true, data: () => ({ sourceSymbol: 'VUAA' }) },
      ]);

      expect(await getUserEquivalences('user-1', 'broker:degiro', ['VUAA'])).toEqual({});
    });

    test('deduplica símbolos repetidos del archivo', async () => {
      await getUserEquivalences('user-1', 'broker:degiro', ['VUAA', 'vuaa', ' VUAA ']);

      expect(mockEquivalenceDoc).toHaveBeenCalledTimes(1);
    });

    test('lee en lotes cuando hay más de 30 símbolos', async () => {
      const symbols = Array.from({ length: 65 }, (_, i) => `SYM${i}`);

      await getUserEquivalences('user-1', 'broker:degiro', symbols);

      // 65 símbolos → 3 lotes (30 + 30 + 5)
      expect(mockGetAll).toHaveBeenCalledTimes(3);
    });

    test('RN-11: un fallo de lectura degrada a "sin equivalencias"', async () => {
      mockGetAll.mockRejectedValue(new Error('firestore unavailable'));

      await expect(getUserEquivalences('user-1', 'broker:degiro', ['VUAA']))
        .resolves.toEqual({});
    });

    test('no consulta si faltan argumentos', async () => {
      expect(await getUserEquivalences(null, 'broker:degiro', ['VUAA'])).toEqual({});
      expect(await getUserEquivalences('user-1', null, ['VUAA'])).toEqual({});
      expect(await getUserEquivalences('user-1', 'broker:degiro', [])).toEqual({});
      expect(mockGetAll).not.toHaveBeenCalled();
    });

    test('marca siempre origin=user', async () => {
      mockGetAll.mockResolvedValue([existingDoc('VUAA', 'VUAA.L')]);

      const result = await getUserEquivalences('user-1', 'broker:degiro', ['VUAA']);

      expect(result.VUAA.origin).toBe('user');
    });
  });

  // =========================================================================
  // Escritura
  // =========================================================================

  describe('saveUserEquivalences', () => {
    test('guarda las equivalencias en un batch y devuelve el conteo', async () => {
      const saved = await saveUserEquivalences('user-1', 'broker:degiro', [VUAA]);

      expect(saved).toBe(1);
      expect(mockBatchSet).toHaveBeenCalledTimes(1);
      expect(mockBatchCommit).toHaveBeenCalledTimes(1);
    });

    test('RN-16: escribe en la clave formato + símbolo', async () => {
      await saveUserEquivalences('user-1', 'broker:degiro', [VUAA]);

      expect(mockEquivalenceDoc).toHaveBeenCalledWith('broker:degiro::VUAA');
    });

    test('Escenario 4: la corrección reemplaza la equivalencia anterior', async () => {
      await saveUserEquivalences('user-1', 'broker:degiro', [
        { ...VUAA, resolvedSymbol: 'VUSA.L' },
      ]);

      const written = mockBatchSet.mock.calls[0][1];

      // set sin merge: la equivalencia nueva sustituye limpiamente a la anterior
      expect(written.resolvedSymbol).toBe('VUSA.L');
      expect(written.sourceFormatId).toBe('broker:degiro');
      expect(written.updatedAt).toBeTruthy();
    });

    test('persiste el símbolo de origen normalizado', async () => {
      await saveUserEquivalences('user-1', 'broker:degiro', [
        { ...VUAA, sourceSymbol: ' vuaa ' },
      ]);

      expect(mockBatchSet.mock.calls[0][1].sourceSymbol).toBe('VUAA');
      expect(mockEquivalenceDoc).toHaveBeenCalledWith('broker:degiro::VUAA');
    });

    test('no guarda ningún dato ajeno al par símbolo → activo', async () => {
      await saveUserEquivalences('user-1', 'broker:degiro', [
        { ...VUAA, portfolioAccountId: 'acct-1', amount: 42, userId: 'otro' },
      ]);

      const written = mockBatchSet.mock.calls[0][1];

      expect(written).not.toHaveProperty('portfolioAccountId');
      expect(written).not.toHaveProperty('amount');
      expect(written).not.toHaveProperty('userId');
    });

    test('no escribe nada ante lista vacía o argumentos faltantes', async () => {
      expect(await saveUserEquivalences('user-1', 'broker:degiro', [])).toBe(0);
      expect(await saveUserEquivalences(null, 'broker:degiro', [VUAA])).toBe(0);
      expect(await saveUserEquivalences('user-1', null, [VUAA])).toBe(0);
      expect(mockBatchCommit).not.toHaveBeenCalled();
    });

    test('descarta equivalencias no utilizables sin abortar el resto', async () => {
      const saved = await saveUserEquivalences('user-1', 'broker:degiro', [
        VUAA,
        { sourceSymbol: '', resolvedSymbol: 'X' },
        { sourceSymbol: 'ABC' },
        null,
      ]);

      expect(saved).toBe(1);
      expect(mockBatchSet).toHaveBeenCalledTimes(1);
    });

    test('escribe varias equivalencias en un único commit', async () => {
      const saved = await saveUserEquivalences('user-1', 'broker:degiro', [
        VUAA,
        { sourceSymbol: 'VWCE', resolvedSymbol: 'VWCE.DE', assetType: 'etf', currency: 'EUR' },
      ]);

      expect(saved).toBe(2);
      expect(mockBatchSet).toHaveBeenCalledTimes(2);
      expect(mockBatchCommit).toHaveBeenCalledTimes(1);
    });
  });

  // =========================================================================
  // Saneamiento
  // =========================================================================

  describe('sanitizeEquivalence', () => {
    test('aplica valores por defecto seguros', () => {
      expect(sanitizeEquivalence({ sourceSymbol: 'x', resolvedSymbol: 'X.L' })).toEqual({
        sourceSymbol: 'X',
        resolvedSymbol: 'X.L',
        assetType: 'stock',
        currency: 'USD',
        exchange: null,
        name: null,
      });
    });

    test('rechaza equivalencias sin símbolo de origen o destino', () => {
      expect(sanitizeEquivalence({ resolvedSymbol: 'X.L' })).toBeNull();
      expect(sanitizeEquivalence({ sourceSymbol: 'X' })).toBeNull();
      expect(sanitizeEquivalence(null)).toBeNull();
    });

    test('rechaza símbolos absurdamente largos', () => {
      expect(sanitizeEquivalence({
        sourceSymbol: 'A'.repeat(100),
        resolvedSymbol: 'X.L',
      })).toBeNull();
    });

    test('recorta el nombre a un tamaño razonable', () => {
      const result = sanitizeEquivalence({
        sourceSymbol: 'X',
        resolvedSymbol: 'X.L',
        name: 'N'.repeat(500),
      });

      expect(result.name).toHaveLength(200);
    });

    test('normaliza la moneda a mayúsculas', () => {
      expect(sanitizeEquivalence({
        sourceSymbol: 'X',
        resolvedSymbol: 'X.L',
        currency: 'gbp',
      }).currency).toBe('GBP');
    });
  });
});
