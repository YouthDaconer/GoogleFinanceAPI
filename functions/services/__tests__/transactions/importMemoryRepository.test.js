/**
 * Tests for importMemoryRepository service
 *
 * HU 1.1: memoria del mapeo confirmado por formato de origen.
 *
 * @see platform-docs/stories/1.1-perfil-importacion-recordado/
 */

// ---------------------------------------------------------------------------
// Firestore mock: userData/{userId}/importProfiles/{sourceFormatId}
// ---------------------------------------------------------------------------

const mockDocGet = jest.fn();
const mockDocSet = jest.fn();
const mockProfileDoc = jest.fn(() => ({ get: mockDocGet, set: mockDocSet }));
const mockProfilesCollection = jest.fn(() => ({ doc: mockProfileDoc }));
const mockUserDoc = jest.fn(() => ({ collection: mockProfilesCollection }));
const mockRootCollection = jest.fn(() => ({ doc: mockUserDoc }));

// HU 1.2: las equivalencias se leen con getAll (referencias concretas) y se
// escriben con un batch.
const mockGetAll = jest.fn();
const mockBatchSet = jest.fn();
const mockBatchCommit = jest.fn().mockResolvedValue(undefined);
const mockBatch = jest.fn(() => ({ set: mockBatchSet, commit: mockBatchCommit }));

jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: (...args) => mockRootCollection(...args),
    getAll: (...args) => mockGetAll(...args),
    batch: (...args) => mockBatch(...args),
  })),
}));

const {
  getProfile,
  saveProfile,
  isProfileStillValid,
  sanitizeMappings,
  sanitizeDefaultValues,
  // HU 1.2
  getUserEquivalences,
  saveUserEquivalences,
  buildEquivalenceKey,
  sanitizeEquivalence,
  normalizeSymbol,
} = require('../../transactions/services/importMemoryRepository');

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const IBKR_HEADERS = ['Symbol', 'TradeDate', 'Quantity', 'TradePrice', 'Commission'];

function buildProfile(overrides = {}) {
  return {
    sourceFormatId: 'broker:interactive_brokers',
    mappings: [
      { sourceColumn: 0, sourceHeader: 'Symbol', targetField: 'ticker' },
      { sourceColumn: 1, sourceHeader: 'TradeDate', targetField: 'date' },
      { sourceColumn: 2, sourceHeader: 'Quantity', targetField: 'amount' },
      { sourceColumn: 3, sourceHeader: 'TradePrice', targetField: 'price' },
    ],
    defaultValues: { type: 'buy', currency: 'USD', commission: 0 },
    hasHeader: true,
    headerRowIndex: 0,
    detectedBroker: 'interactive_brokers',
    confirmedImportCount: 2,
    updatedAt: '2026-08-01T10:00:00.000Z',
    ...overrides,
  };
}

describe('importMemoryRepository', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockProfileDoc.mockReturnValue({ get: mockDocGet, set: mockDocSet });
    mockProfilesCollection.mockReturnValue({ doc: mockProfileDoc });
    mockUserDoc.mockReturnValue({ collection: mockProfilesCollection });
    mockRootCollection.mockReturnValue({ doc: mockUserDoc });
    mockBatch.mockReturnValue({ set: mockBatchSet, commit: mockBatchCommit });
    mockBatchCommit.mockResolvedValue(undefined);
    mockGetAll.mockResolvedValue([]);
  });

  // =========================================================================
  // isProfileStillValid — RN-04
  // =========================================================================

  describe('isProfileStillValid (RN-04)', () => {
    test('un perfil vigente sigue siendo válido', () => {
      expect(isProfileStillValid(buildProfile(), IBKR_HEADERS, 5)).toBe(true);
    });

    test('Escenario 3: el broker renombró una columna → perfil descartado', () => {
      const changedHeaders = ['Symbol', 'Settlement Date', 'Quantity', 'TradePrice', 'Commission'];

      expect(isProfileStillValid(buildProfile(), changedHeaders, 5)).toBe(false);
    });

    test('Escenario 3: el broker eliminó columnas → perfil descartado', () => {
      const shorterHeaders = ['Symbol', 'TradeDate'];

      expect(isProfileStillValid(buildProfile(), shorterHeaders, 2)).toBe(false);
    });

    test('Escenario 3: el broker reordenó columnas → perfil descartado', () => {
      const reordered = ['TradeDate', 'Symbol', 'Quantity', 'TradePrice', 'Commission'];

      expect(isProfileStillValid(buildProfile(), reordered, 5)).toBe(false);
    });

    test('tolera diferencias de mayúsculas y espacios en las cabeceras', () => {
      const cosmetic = [' symbol ', 'TRADEDATE', 'Quantity', 'TradePrice', 'Commission'];

      expect(isProfileStillValid(buildProfile(), cosmetic, 5)).toBe(true);
    });

    test('Escenario 7: sin perfil no hay nada que aplicar', () => {
      expect(isProfileStillValid(null, IBKR_HEADERS, 5)).toBe(false);
    });

    test('un perfil sin mapeos no es válido', () => {
      expect(isProfileStillValid(buildProfile({ mappings: [] }), IBKR_HEADERS, 5)).toBe(false);
    });

    test('un perfil con mappings corrupto no es válido', () => {
      expect(isProfileStillValid(buildProfile({ mappings: 'nope' }), IBKR_HEADERS, 5)).toBe(false);
    });

    test('archivo sin cabeceras: solo se valida la forma del archivo', () => {
      const profile = buildProfile({ hasHeader: false });

      expect(isProfileStillValid(profile, null, 5)).toBe(true);
      expect(isProfileStillValid(profile, null, 3)).toBe(false);
    });
  });

  // =========================================================================
  // getProfile
  // =========================================================================

  describe('getProfile', () => {
    test('devuelve el perfil almacenado', async () => {
      mockDocGet.mockResolvedValue({ exists: true, data: () => buildProfile() });

      const profile = await getProfile('user-1', 'broker:interactive_brokers');

      expect(profile).not.toBeNull();
      expect(profile.sourceFormatId).toBe('broker:interactive_brokers');
      expect(mockRootCollection).toHaveBeenCalledWith('userData');
      expect(mockUserDoc).toHaveBeenCalledWith('user-1');
      expect(mockProfilesCollection).toHaveBeenCalledWith('importProfiles');
      expect(mockProfileDoc).toHaveBeenCalledWith('broker:interactive_brokers');
    });

    test('Escenario 7: sin memoria previa devuelve null', async () => {
      mockDocGet.mockResolvedValue({ exists: false });

      expect(await getProfile('user-nuevo', 'broker:degiro')).toBeNull();
    });

    test('Escenario 4: cada formato se lee de su propio documento', async () => {
      mockDocGet.mockResolvedValue({ exists: false });

      await getProfile('user-1', 'broker:interactive_brokers');
      await getProfile('user-1', 'broker:degiro');

      expect(mockProfileDoc).toHaveBeenNthCalledWith(1, 'broker:interactive_brokers');
      expect(mockProfileDoc).toHaveBeenNthCalledWith(2, 'broker:degiro');
    });

    test('RN-11: un fallo de lectura degrada a "sin memoria", no propaga el error', async () => {
      mockDocGet.mockRejectedValue(new Error('firestore unavailable'));

      await expect(getProfile('user-1', 'broker:degiro')).resolves.toBeNull();
    });

    test('devuelve null sin consultar si faltan argumentos', async () => {
      expect(await getProfile(null, 'broker:degiro')).toBeNull();
      expect(await getProfile('user-1', null)).toBeNull();
      expect(mockDocGet).not.toHaveBeenCalled();
    });
  });

  // =========================================================================
  // saveProfile
  // =========================================================================

  describe('saveProfile', () => {
    const validParams = {
      sourceFormatId: 'broker:degiro',
      mappings: [
        {
          sourceColumn: 0,
          sourceHeader: 'Producto',
          targetField: 'ticker',
          confidence: 0.95,
          detectionMethod: 'broker',
          sampleValues: ['VUAA', 'VWCE'],
        },
      ],
      defaultValues: { type: 'buy', currency: 'EUR', commission: 0 },
      hasHeader: true,
      headerRowIndex: 0,
      detectedBroker: 'degiro',
    };

    test('guarda el perfil y devuelve true', async () => {
      mockDocGet.mockResolvedValue({ exists: false });

      const saved = await saveProfile('user-1', validParams);

      expect(saved).toBe(true);
      expect(mockDocSet).toHaveBeenCalledTimes(1);
    });

    test('RN-15: la cuenta destino NUNCA se guarda', async () => {
      mockDocGet.mockResolvedValue({ exists: false });

      await saveProfile('user-1', {
        ...validParams,
        portfolioAccountId: 'account-que-no-debe-persistirse',
      });

      const written = mockDocSet.mock.calls[0][0];

      expect(written).not.toHaveProperty('portfolioAccountId');
      expect(JSON.stringify(written)).not.toContain('account-que-no-debe-persistirse');
    });

    test('no persiste datos de la sesión de análisis (confianza, método, muestras)', async () => {
      mockDocGet.mockResolvedValue({ exists: false });

      await saveProfile('user-1', validParams);

      const written = mockDocSet.mock.calls[0][0];

      expect(written.mappings[0]).toEqual({
        sourceColumn: 0,
        sourceHeader: 'Producto',
        targetField: 'ticker',
      });
    });

    test('RN-14: el confirmado reemplaza al anterior del mismo formato e incrementa el contador', async () => {
      mockDocGet.mockResolvedValue({
        exists: true,
        data: () => ({ confirmedImportCount: 4 }),
      });

      await saveProfile('user-1', validParams);

      expect(mockDocSet.mock.calls[0][0].confirmedImportCount).toBe(5);
      expect(mockProfileDoc).toHaveBeenCalledWith('broker:degiro');
    });

    test('no guarda si no hay mapeos utilizables', async () => {
      const saved = await saveProfile('user-1', { ...validParams, mappings: [] });

      expect(saved).toBe(false);
      expect(mockDocSet).not.toHaveBeenCalled();
    });

    test('no guarda sin userId ni sourceFormatId', async () => {
      expect(await saveProfile(null, validParams)).toBe(false);
      expect(await saveProfile('user-1', { ...validParams, sourceFormatId: null })).toBe(false);
      expect(mockDocSet).not.toHaveBeenCalled();
    });
  });

  // =========================================================================
  // Sanitizers
  // =========================================================================

  describe('sanitizeMappings', () => {
    test('descarta mapeos sin columna o sin campo destino', () => {
      const result = sanitizeMappings([
        { sourceColumn: 0, sourceHeader: 'A', targetField: 'ticker' },
        { sourceColumn: -1, sourceHeader: 'B', targetField: 'date' },
        { sourceColumn: 2, sourceHeader: 'C', targetField: '' },
        { sourceHeader: 'D', targetField: 'price' },
        null,
      ]);

      expect(result).toEqual([
        { sourceColumn: 0, sourceHeader: 'A', targetField: 'ticker' },
      ]);
    });

    test('devuelve array vacío ante entradas no-array', () => {
      expect(sanitizeMappings(null)).toEqual([]);
      expect(sanitizeMappings('nope')).toEqual([]);
    });
  });

  describe('sanitizeDefaultValues', () => {
    test('normaliza tipo y moneda', () => {
      expect(sanitizeDefaultValues({ type: 'SELL', currency: 'eur', commission: 1.5 }))
        .toEqual({ type: 'sell', currency: 'EUR', commission: 1.5 });
    });

    test('aplica valores por defecto seguros', () => {
      expect(sanitizeDefaultValues(undefined))
        .toEqual({ type: 'buy', currency: 'USD', commission: 0 });
    });

    test('rechaza tipos desconocidos y comisiones inválidas', () => {
      expect(sanitizeDefaultValues({ type: 'transfer', commission: -5 }))
        .toEqual({ type: 'buy', currency: 'USD', commission: 0 });
      expect(sanitizeDefaultValues({ commission: 'abc' }).commission).toBe(0);
    });
  });
});
