/**
 * Tests for saveImportMemory Cloud Function
 *
 * HU 1.1: único punto de escritura de la memoria de importación.
 *
 * @see platform-docs/stories/1.1-perfil-importacion-recordado/
 */

// Mock Firebase Functions
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

// GATE-006: gate de plan
const mockValidateFeatureAccess = jest.fn().mockResolvedValue(undefined);
jest.mock('../../helpers/subscriptionValidator', () => ({
  validateFeatureAccess: (...args) => mockValidateFeatureAccess(...args),
  validateQuantityLimit: jest.fn().mockResolvedValue(undefined),
}));

// Repositorio de memoria
const mockSaveProfile = jest.fn().mockResolvedValue(true);
const mockSaveUserEquivalences = jest.fn().mockResolvedValue(0);
jest.mock('../../transactions/services/importMemoryRepository', () => ({
  saveProfile: (...args) => mockSaveProfile(...args),
  saveUserEquivalences: (...args) => mockSaveUserEquivalences(...args),
}));

const { saveImportMemory } = require('../../transactions/saveImportMemory');
const { HttpsError } = require('firebase-functions/v2/https');

// ============================================================================
// HELPERS
// ============================================================================

const handler = saveImportMemory._handler;

const VALID_PAYLOAD = {
  sourceFormatId: 'broker:degiro',
  mappings: [
    { sourceColumn: 0, sourceHeader: 'Producto', targetField: 'ticker' },
    { sourceColumn: 1, sourceHeader: 'Fecha', targetField: 'date' },
  ],
  defaultValues: { type: 'buy', currency: 'EUR', commission: 0 },
  hasHeader: true,
  headerRowIndex: 0,
  detectedBroker: 'degiro',
};

function call(data, auth = { uid: 'user-1' }) {
  return handler({ auth, data });
}

describe('saveImportMemory', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockValidateFeatureAccess.mockResolvedValue(undefined);
    mockSaveProfile.mockResolvedValue(true);
    mockSaveUserEquivalences.mockResolvedValue(0);
  });

  // =========================================================================
  // Autenticación y control de acceso
  // =========================================================================

  describe('autenticación y plan', () => {
    test('rechaza peticiones sin autenticar', async () => {
      await expect(handler({ auth: null, data: VALID_PAYLOAD }))
        .rejects.toThrow(HttpsError);
      expect(mockSaveProfile).not.toHaveBeenCalled();
    });

    test('Escenario 9: sin plan que habilite importación no se crea ninguna memoria', async () => {
      mockValidateFeatureAccess.mockRejectedValue(
        new HttpsError('permission-denied', 'Esta funcionalidad requiere un plan Pro activo.')
      );

      await expect(call(VALID_PAYLOAD)).rejects.toThrow('plan Pro');
      expect(mockSaveProfile).not.toHaveBeenCalled();
    });

    test('valida el feature hasImport del usuario autenticado', async () => {
      await call(VALID_PAYLOAD);

      expect(mockValidateFeatureAccess).toHaveBeenCalledWith('user-1', 'hasImport');
    });
  });

  // =========================================================================
  // Validación de payload
  // =========================================================================

  describe('validación de payload', () => {
    test('rechaza sourceFormatId ausente', async () => {
      await expect(call({ ...VALID_PAYLOAD, sourceFormatId: undefined }))
        .rejects.toThrow('sourceFormatId inválido');
    });

    test('rechaza sourceFormatId con forma no reconocida', async () => {
      await expect(call({ ...VALID_PAYLOAD, sourceFormatId: 'algo/raro' }))
        .rejects.toThrow('sourceFormatId inválido');
    });

    test('rechaza intentos de path traversal en el id de documento', async () => {
      await expect(call({ ...VALID_PAYLOAD, sourceFormatId: 'broker:../../userData' }))
        .rejects.toThrow('sourceFormatId inválido');
      expect(mockSaveProfile).not.toHaveBeenCalled();
    });

    test('rechaza mappings que no es array', async () => {
      await expect(call({ ...VALID_PAYLOAD, mappings: 'nope' }))
        .rejects.toThrow('mappings debe ser un array');
    });

    test('tolera payload vacío sin romper', async () => {
      await expect(call(undefined)).rejects.toThrow('sourceFormatId inválido');
    });
  });

  // =========================================================================
  // Persistencia
  // =========================================================================

  describe('persistencia', () => {
    test('guarda el perfil y reporta el resultado', async () => {
      const result = await call(VALID_PAYLOAD);

      expect(result.success).toBe(true);
      expect(result.profileSaved).toBe(true);
      expect(mockSaveProfile).toHaveBeenCalledWith('user-1', expect.objectContaining({
        sourceFormatId: 'broker:degiro',
        detectedBroker: 'degiro',
      }));
    });

    test('RN-15: la cuenta destino no se reenvía al repositorio aunque venga en el payload', async () => {
      await call({ ...VALID_PAYLOAD, portfolioAccountId: 'account-xyz' });

      const forwarded = mockSaveProfile.mock.calls[0][1];

      expect(forwarded).not.toHaveProperty('portfolioAccountId');
    });

    test('usa el uid del token, nunca un userId del payload', async () => {
      await call({ ...VALID_PAYLOAD, userId: 'otro-usuario' }, { uid: 'user-real' });

      expect(mockSaveProfile).toHaveBeenCalledWith('user-real', expect.any(Object));
    });

    test('reporta profileSaved false cuando el repositorio no guardó nada', async () => {
      mockSaveProfile.mockResolvedValue(false);

      const result = await call({ ...VALID_PAYLOAD, mappings: [] });

      expect(result.success).toBe(true);
      expect(result.profileSaved).toBe(false);
    });

    test('convierte un fallo de escritura en HttpsError interno', async () => {
      mockSaveProfile.mockRejectedValue(new Error('firestore down'));

      await expect(call(VALID_PAYLOAD))
        .rejects.toThrow('No se pudo guardar la memoria de importación');
    });
  });

  // =========================================================================
  // HU 1.2: equivalencias de símbolo
  // =========================================================================

  describe('equivalencias de símbolo (HU 1.2)', () => {
    const VUAA = {
      sourceSymbol: 'VUAA',
      resolvedSymbol: 'VUAA.L',
      assetType: 'etf',
      currency: 'GBP',
    };

    test('persiste las equivalencias recibidas y reporta el conteo', async () => {
      mockSaveUserEquivalences.mockResolvedValue(1);

      const result = await call({ ...VALID_PAYLOAD, equivalences: [VUAA] });

      expect(result.equivalencesSaved).toBe(1);
      expect(mockSaveUserEquivalences).toHaveBeenCalledWith(
        'user-1',
        'broker:degiro',
        [VUAA]
      );
    });

    test('rechaza equivalences que no es array', async () => {
      await expect(call({ ...VALID_PAYLOAD, equivalences: 'nope' }))
        .rejects.toThrow('equivalences debe ser un array');
    });

    test('acepta el payload sin equivalences (compatibilidad con 1.1)', async () => {
      const result = await call(VALID_PAYLOAD);

      expect(result.equivalencesSaved).toBe(0);
      expect(mockSaveUserEquivalences).not.toHaveBeenCalled();
    });

    test('una lista vacía no dispara escritura', async () => {
      const result = await call({ ...VALID_PAYLOAD, equivalences: [] });

      expect(result.equivalencesSaved).toBe(0);
      expect(mockSaveUserEquivalences).not.toHaveBeenCalled();
    });

    test('un fallo al guardar equivalencias NO invalida el perfil ya guardado', async () => {
      mockSaveUserEquivalences.mockRejectedValue(new Error('firestore down'));

      const result = await call({ ...VALID_PAYLOAD, equivalences: [VUAA] });

      expect(result.success).toBe(true);
      expect(result.profileSaved).toBe(true);
      expect(result.equivalencesSaved).toBe(0);
    });

    test('acota el número de equivalencias por invocación', async () => {
      mockSaveUserEquivalences.mockResolvedValue(500);

      const many = Array.from({ length: 900 }, (_, i) => ({
        sourceSymbol: `SYM${i}`,
        resolvedSymbol: `SYM${i}.L`,
        assetType: 'stock',
        currency: 'USD',
      }));

      await call({ ...VALID_PAYLOAD, equivalences: many });

      expect(mockSaveUserEquivalences.mock.calls[0][2]).toHaveLength(500);
    });

    test('usa el uid del token para las equivalencias', async () => {
      mockSaveUserEquivalences.mockResolvedValue(1);

      await call({ ...VALID_PAYLOAD, equivalences: [VUAA], userId: 'otro' }, { uid: 'user-real' });

      expect(mockSaveUserEquivalences).toHaveBeenCalledWith(
        'user-real',
        expect.any(String),
        expect.any(Array)
      );
    });
  });

  // =========================================================================
  // Configuración
  // =========================================================================

  describe('configuración de la función', () => {
    test('es una escritura acotada: memoria y timeout modestos', () => {
      expect(saveImportMemory._config.memory).toBe('256MiB');
      expect(saveImportMemory._config.timeoutSeconds).toBeLessThanOrEqual(60);
    });
  });
});
