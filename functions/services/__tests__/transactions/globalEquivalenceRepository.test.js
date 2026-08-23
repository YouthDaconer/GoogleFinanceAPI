/**
 * Tests for globalEquivalenceRepository
 *
 * HU 1.6: promoción por evidencia, retiro por contradicción sostenida y privacidad
 * del catálogo global.
 *
 * @see platform-docs/stories/1.6-catalogo-global-equivalencias/
 */

// ---------------------------------------------------------------------------
// Firestore mock
// ---------------------------------------------------------------------------

/** Documento del catálogo: get/set/update */
const mockCatalogGet = jest.fn();
const mockCatalogSet = jest.fn();
const mockCatalogUpdate = jest.fn();

/** Evidencia: subcolecciones de confirmaciones y contradicciones */
const mockEvidenceDocSet = jest.fn();
const mockCountGet = jest.fn();
const mockWhere = jest.fn();
const mockAuditAdd = jest.fn();

/** Registra la última consulta de conteo para poder inspeccionarla */
let lastCountQuery = null;

function buildCountQuery() {
  const query = {
    where: (field, op, value) => {
      lastCountQuery.filters.push({ field, op, value });
      mockWhere(field, op, value);
      return query;
    },
    count: () => ({ get: mockCountGet }),
  };

  return query;
}

const mockEvidenceSubcollection = jest.fn((name) => {
  lastCountQuery = { subcollection: name, filters: [] };

  const query = buildCountQuery();

  return {
    doc: jest.fn(() => ({ set: mockEvidenceDocSet })),
    where: query.where,
    count: query.count,
  };
});

jest.mock('../../firebaseAdmin', () => ({
  firestore: jest.fn(() => ({
    collection: jest.fn((name) => {
      if (name === 'symbolEquivalences') {
        return {
          doc: jest.fn(() => ({
            get: mockCatalogGet,
            set: mockCatalogSet,
            update: mockCatalogUpdate,
          })),
        };
      }

      if (name === 'symbolEquivalenceEvidence') {
        return {
          doc: jest.fn(() => ({ collection: mockEvidenceSubcollection })),
        };
      }

      if (name === 'symbolEquivalenceAudit') {
        return { add: mockAuditAdd };
      }

      return { doc: jest.fn(() => ({})) };
    }),
    getAll: jest.fn(async (...refs) => refs.map(r => r.__doc)),
    batch: jest.fn(() => ({ set: jest.fn(), commit: jest.fn() })),
  })),
}));

const {
  hashUserId,
  recordConfirmation,
  recordContradiction,
  evaluatePromotion,
  evaluateRetirement,
  retireEntry,
  windowCutoff,
} = require('../../transactions/services/globalEquivalenceRepository');

const {
  GLOBAL_EQUIVALENCE_THRESHOLDS,
  GLOBAL_EQUIVALENCE_STATUS,
  GLOBAL_EQUIVALENCE_AUDIT_ACTIONS,
} = require('../../transactions/types');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SALT = 'test-salt-not-a-real-secret';
const KEY = 'broker:degiro::VUAA';

function withCount(n) {
  mockCountGet.mockResolvedValue({ data: () => ({ count: n }) });
}

function catalogEntry(overrides = {}) {
  return {
    exists: true,
    data: () => ({
      sourceFormatId: 'broker:degiro',
      sourceSymbol: 'VUAA',
      resolvedSymbol: 'VUAA.L',
      assetType: 'etf',
      currency: 'GBP',
      status: GLOBAL_EQUIVALENCE_STATUS.ACTIVE,
      promotedAt: '2026-06-01T00:00:00.000Z',
      ...overrides,
    }),
  };
}

const PROMOTION_PARAMS = {
  key: KEY,
  sourceFormatId: 'broker:degiro',
  sourceSymbol: 'VUAA',
  resolvedSymbol: 'VUAA.L',
  assetType: 'etf',
  currency: 'GBP',
};

describe('globalEquivalenceRepository', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockCatalogGet.mockResolvedValue({ exists: false });
    mockAuditAdd.mockResolvedValue({ id: 'audit-1' });
    withCount(0);
  });

  // =========================================================================
  // Anonimización de la evidencia (RN-30)
  // =========================================================================

  describe('hashUserId (RN-30)', () => {
    test('el hash es determinista para el mismo usuario', () => {
      expect(hashUserId('user-1', SALT)).toBe(hashUserId('user-1', SALT));
    });

    test('usuarios distintos producen hashes distintos', () => {
      expect(hashUserId('user-1', SALT)).not.toBe(hashUserId('user-2', SALT));
    });

    test('el hash no contiene el uid original', () => {
      const hash = hashUserId('user-identificable-123', SALT);

      expect(hash).not.toContain('user-identificable-123');
      expect(hash).toMatch(/^[0-9a-f]{64}$/);
    });

    test('el mismo uid con otro secreto produce otro hash', () => {
      expect(hashUserId('user-1', SALT)).not.toBe(hashUserId('user-1', 'otro-secreto'));
    });

    test('falla en cerrado si no hay secreto configurado', () => {
      // Mejor no registrar evidencia que registrarla de forma reversible
      expect(() => hashUserId('user-1', null)).toThrow('EQUIV_HASH_SALT');
      expect(() => hashUserId('user-1', '')).toThrow('EQUIV_HASH_SALT');
    });

    test('un uid ausente no produce hash', () => {
      expect(hashUserId(null, SALT)).toBe('');
    });
  });

  // =========================================================================
  // Registro de evidencia
  // =========================================================================

  describe('recordConfirmation', () => {
    test('escribe la confirmación indexada por el hash del usuario', async () => {
      await recordConfirmation({ key: KEY, userId: 'user-1', salt: SALT, resolvedSymbol: 'VUAA.L' });

      expect(mockEvidenceSubcollection).toHaveBeenCalledWith('confirmations');
      expect(mockEvidenceDocSet).toHaveBeenCalledWith(expect.objectContaining({
        resolvedSymbol: 'VUAA.L',
      }));
    });

    test('la confirmación NO guarda el uid en ningún campo (RN-30)', async () => {
      await recordConfirmation({ key: KEY, userId: 'user-identificable', salt: SALT, resolvedSymbol: 'VUAA.L' });

      const written = mockEvidenceDocSet.mock.calls[0][0];

      expect(JSON.stringify(written)).not.toContain('user-identificable');
      expect(written).not.toHaveProperty('userId');
    });

    test('no escribe si faltan datos', async () => {
      await recordConfirmation({ key: null, userId: 'user-1', salt: SALT, resolvedSymbol: 'X' });
      await recordConfirmation({ key: KEY, userId: null, salt: SALT, resolvedSymbol: 'X' });
      await recordConfirmation({ key: KEY, userId: 'user-1', salt: SALT, resolvedSymbol: '' });

      expect(mockEvidenceDocSet).not.toHaveBeenCalled();
    });
  });

  describe('recordContradiction', () => {
    test('escribe la contradicción contra el valor rechazado', async () => {
      await recordContradiction({ key: KEY, userId: 'user-1', salt: SALT, against: 'VUAA.L' });

      expect(mockEvidenceSubcollection).toHaveBeenCalledWith('contradictions');
      expect(mockEvidenceDocSet).toHaveBeenCalledWith(expect.objectContaining({
        against: 'VUAA.L',
      }));
    });

    test('la contradicción tampoco guarda el uid', async () => {
      await recordContradiction({ key: KEY, userId: 'user-identificable', salt: SALT, against: 'VUAA.L' });

      expect(JSON.stringify(mockEvidenceDocSet.mock.calls[0][0])).not.toContain('user-identificable');
    });
  });

  // =========================================================================
  // Escenario 1, 4 y 5 — promoción (RN-07)
  // =========================================================================

  describe('evaluatePromotion (RN-07)', () => {
    test('Escenario 1: con 5 usuarios distintos se promueve', async () => {
      withCount(GLOBAL_EQUIVALENCE_THRESHOLDS.promoteDistinctUsers);

      const promoted = await evaluatePromotion(PROMOTION_PARAMS);

      expect(promoted).toBe(true);
      expect(mockCatalogSet).toHaveBeenCalledTimes(1);
    });

    test('Escenario 4: con menos de 5 usuarios NO se promueve', async () => {
      withCount(GLOBAL_EQUIVALENCE_THRESHOLDS.promoteDistinctUsers - 1);

      expect(await evaluatePromotion(PROMOTION_PARAMS)).toBe(false);
      expect(mockCatalogSet).not.toHaveBeenCalled();
    });

    test('con más de 5 usuarios también se promueve', async () => {
      withCount(GLOBAL_EQUIVALENCE_THRESHOLDS.promoteDistinctUsers + 10);

      expect(await evaluatePromotion(PROMOTION_PARAMS)).toBe(true);
    });

    test('Escenario 5: el conteo es de documentos, uno por usuario', async () => {
      // Cada confirmación se escribe en el documento del hash del usuario, así que
      // repetir la confirmación no incrementa el conteo. Se verifica que la consulta
      // cuenta documentos filtrados por el ticker canónico, no eventos.
      withCount(1);

      await evaluatePromotion(PROMOTION_PARAMS);

      expect(lastCountQuery.subcollection).toBe('confirmations');
      expect(lastCountQuery.filters).toEqual([
        { field: 'resolvedSymbol', op: '==', value: 'VUAA.L' },
      ]);
    });

    test('el conteo se filtra por el par clave + ticker canónico (RN-07)', async () => {
      withCount(5);

      await evaluatePromotion({ ...PROMOTION_PARAMS, resolvedSymbol: 'VUSA.L' });

      expect(lastCountQuery.filters[0]).toEqual({
        field: 'resolvedSymbol', op: '==', value: 'VUSA.L',
      });
    });

    test('Escenario 8: el documento publicado NO contiene datos de usuario (RN-30)', async () => {
      withCount(5);

      await evaluatePromotion(PROMOTION_PARAMS);

      const published = mockCatalogSet.mock.calls[0][0];

      expect(Object.keys(published).sort()).toEqual([
        'assetType', 'currency', 'promotedAt', 'resolvedSymbol',
        'sourceFormatId', 'sourceSymbol', 'status',
      ]);
      expect(published).not.toHaveProperty('userId');
      expect(published).not.toHaveProperty('confirmedBy');
      expect(published).not.toHaveProperty('confirmCount');
      expect(published).not.toHaveProperty('portfolioAccountId');
    });

    test('no republica una entrada que ya está activa con el mismo valor', async () => {
      withCount(5);
      mockCatalogGet.mockResolvedValue(catalogEntry());

      expect(await evaluatePromotion(PROMOTION_PARAMS)).toBe(false);
      expect(mockCatalogSet).not.toHaveBeenCalled();
    });

    test('republica cuando el valor confirmado cambió', async () => {
      withCount(5);
      mockCatalogGet.mockResolvedValue(catalogEntry({ resolvedSymbol: 'OTRO.L' }));

      expect(await evaluatePromotion(PROMOTION_PARAMS)).toBe(true);
    });

    test('deja traza de auditoría al promover', async () => {
      withCount(5);

      await evaluatePromotion(PROMOTION_PARAMS);

      expect(mockAuditAdd).toHaveBeenCalledWith(expect.objectContaining({
        key: KEY,
        action: GLOBAL_EQUIVALENCE_AUDIT_ACTIONS.PROMOTED,
        actor: 'system',
      }));
    });

    test('la auditoría no identifica a los usuarios que confirmaron', async () => {
      withCount(5);

      await evaluatePromotion(PROMOTION_PARAMS);

      const audit = mockAuditAdd.mock.calls[0][0];

      expect(audit.actor).toBe('system');
      expect(audit).not.toHaveProperty('userId');
    });

    test('no evalúa sin clave o sin ticker canónico', async () => {
      expect(await evaluatePromotion({ ...PROMOTION_PARAMS, key: null })).toBe(false);
      expect(await evaluatePromotion({ ...PROMOTION_PARAMS, resolvedSymbol: '' })).toBe(false);
    });
  });

  // =========================================================================
  // Escenario 6 — retiro por contradicción sostenida (RN-31)
  // =========================================================================

  describe('evaluateRetirement (RN-31)', () => {
    test('Escenario 6: con 3 usuarios distintos en la ventana se retira', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());
      withCount(GLOBAL_EQUIVALENCE_THRESHOLDS.retireDistinctUsers);

      expect(await evaluateRetirement(KEY)).toBe(true);
      expect(mockCatalogUpdate).toHaveBeenCalledWith(expect.objectContaining({
        status: GLOBAL_EQUIVALENCE_STATUS.RETIRED,
      }));
    });

    test('con menos de 3 contradicciones NO se retira', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());
      withCount(GLOBAL_EQUIVALENCE_THRESHOLDS.retireDistinctUsers - 1);

      expect(await evaluateRetirement(KEY)).toBe(false);
      expect(mockCatalogUpdate).not.toHaveBeenCalled();
    });

    test('el umbral de retiro es MENOR que el de promoción', () => {
      // RN-31: el daño de propagar una equivalencia errónea supera el costo de
      // dejar de proponer una válida
      expect(GLOBAL_EQUIVALENCE_THRESHOLDS.retireDistinctUsers)
        .toBeLessThan(GLOBAL_EQUIVALENCE_THRESHOLDS.promoteDistinctUsers);
    });

    test('la consulta se acota al valor activo y a la ventana de 90 días', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());
      withCount(3);

      await evaluateRetirement(KEY);

      expect(lastCountQuery.subcollection).toBe('contradictions');
      expect(lastCountQuery.filters[0]).toEqual({
        field: 'against', op: '==', value: 'VUAA.L',
      });
      expect(lastCountQuery.filters[1].field).toBe('at');
      expect(lastCountQuery.filters[1].op).toBe('>=');
    });

    test('la ventana declarada es de 90 días', () => {
      expect(GLOBAL_EQUIVALENCE_THRESHOLDS.retireWindowDays).toBe(90);
    });

    test('una entrada ya retirada no se vuelve a retirar', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry({ status: GLOBAL_EQUIVALENCE_STATUS.RETIRED }));
      withCount(10);

      expect(await evaluateRetirement(KEY)).toBe(false);
      expect(mockCatalogUpdate).not.toHaveBeenCalled();
    });

    test('una entrada que no existe no se retira', async () => {
      mockCatalogGet.mockResolvedValue({ exists: false });

      expect(await evaluateRetirement(KEY)).toBe(false);
    });

    test('RN-32: el retiro NO toca transacciones, solo el estado', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());
      withCount(3);

      await evaluateRetirement(KEY);

      const updated = mockCatalogUpdate.mock.calls[0][0];

      expect(Object.keys(updated).sort()).toEqual(['retiredAt', 'status']);
    });

    test('deja traza de auditoría al retirar por evidencia', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());
      withCount(3);

      await evaluateRetirement(KEY);

      expect(mockAuditAdd).toHaveBeenCalledWith(expect.objectContaining({
        action: GLOBAL_EQUIVALENCE_AUDIT_ACTIONS.RETIRED_BY_EVIDENCE,
      }));
    });

    test('no evalúa sin clave', async () => {
      expect(await evaluateRetirement(null)).toBe(false);
    });
  });

  // =========================================================================
  // Escenario 7 — retiro operativo inmediato
  // =========================================================================

  describe('Escenario 7: retiro operativo (retireEntry)', () => {
    test('retira la entrada de inmediato', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());

      const retired = await retireEntry({ key: KEY, reason: 'Apuntaba al ETF equivocado', actor: 'carlos' });

      expect(retired).toBe(true);
      expect(mockCatalogUpdate).toHaveBeenCalledWith(expect.objectContaining({
        status: GLOBAL_EQUIVALENCE_STATUS.RETIRED,
      }));
    });

    test('DoD: registra qué entrada se retiró, cuándo y por qué', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());

      await retireEntry({ key: KEY, reason: 'Apuntaba al ETF equivocado', actor: 'carlos' });

      const audit = mockAuditAdd.mock.calls[0][0];

      expect(audit.key).toBe(KEY);
      expect(audit.action).toBe(GLOBAL_EQUIVALENCE_AUDIT_ACTIONS.RETIRED_BY_OPERATOR);
      expect(audit.actor).toBe('carlos');
      expect(audit.reason).toBe('Apuntaba al ETF equivocado');
      expect(audit.at).toBeTruthy();
    });

    test('RN-32: no modifica transacciones', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());

      await retireEntry({ key: KEY, reason: 'x', actor: 'y' });

      expect(Object.keys(mockCatalogUpdate.mock.calls[0][0]).sort())
        .toEqual(['retiredAt', 'status']);
    });

    test('una entrada inexistente no se puede retirar', async () => {
      mockCatalogGet.mockResolvedValue({ exists: false });

      expect(await retireEntry({ key: KEY, reason: 'x', actor: 'y' })).toBe(false);
      expect(mockCatalogUpdate).not.toHaveBeenCalled();
    });

    test('registra un actor por defecto si no se indica', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());

      await retireEntry({ key: KEY, reason: 'x' });

      expect(mockAuditAdd.mock.calls[0][0].actor).toBe('unknown');
    });
  });

  // =========================================================================
  // Ventana temporal
  // =========================================================================

  describe('windowCutoff', () => {
    test('devuelve una fecha en el pasado', () => {
      const cutoff = new Date(windowCutoff(90));

      expect(cutoff.getTime()).toBeLessThan(Date.now());
    });

    test('90 días atrás está aproximadamente a 90 días', () => {
      const cutoff = new Date(windowCutoff(90));
      const days = (Date.now() - cutoff.getTime()) / (1000 * 60 * 60 * 24);

      expect(days).toBeGreaterThan(89);
      expect(days).toBeLessThan(91);
    });

    test('una ventana de 0 días es ahora mismo', () => {
      const cutoff = new Date(windowCutoff(0));

      expect(Math.abs(Date.now() - cutoff.getTime())).toBeLessThan(2000);
    });
  });

  // =========================================================================
  // Resiliencia
  // =========================================================================

  describe('resiliencia', () => {
    test('un fallo de auditoría no impide la promoción que documenta', async () => {
      withCount(5);
      mockAuditAdd.mockRejectedValue(new Error('audit collection unavailable'));

      await expect(evaluatePromotion(PROMOTION_PARAMS)).resolves.toBe(true);
      expect(mockCatalogSet).toHaveBeenCalled();
    });

    test('un fallo de auditoría no impide el retiro', async () => {
      mockCatalogGet.mockResolvedValue(catalogEntry());
      withCount(3);
      mockAuditAdd.mockRejectedValue(new Error('audit collection unavailable'));

      await expect(evaluateRetirement(KEY)).resolves.toBe(true);
    });
  });
});
