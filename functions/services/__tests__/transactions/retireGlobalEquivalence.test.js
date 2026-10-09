/**
 * Tests de la utilidad interna de retiro operativo
 *
 * HU 1.6, escenario 7: el retiro por parte del equipo de producto surte efecto de
 * inmediato y deja registro, sin pantalla de administración.
 *
 * El script en sí es un envoltorio de CLI; lo que se verifica aquí es el contrato
 * que expone y las garantías del repositorio que invoca.
 *
 * @see platform-docs/stories/1.6-catalogo-global-equivalencias/
 */

const fs = require('fs');
const path = require('path');

const SCRIPT_PATH = path.join(__dirname, '../../../scripts/retireGlobalEquivalence.js');

// ---------------------------------------------------------------------------
// Existencia y forma de la utilidad
// ---------------------------------------------------------------------------

describe('utilidad interna de retiro', () => {
  let source;

  beforeAll(() => {
    source = fs.readFileSync(SCRIPT_PATH, 'utf-8');
  });

  test('la utilidad existe', () => {
    expect(fs.existsSync(SCRIPT_PATH)).toBe(true);
  });

  test('es ejecutable como script de línea de comandos', () => {
    expect(source.startsWith('#!/usr/bin/env node')).toBe(true);
  });

  test('usa el admin SDK, no una Cloud Function expuesta', () => {
    expect(source).toContain("require(\"../services/firebaseAdmin\")");
  });

  test('invoca el retiro del repositorio, no escribe el catálogo por su cuenta', () => {
    expect(source).toContain('retireEntry');
  });

  test('exige un motivo del retiro para que quede en la auditoría', () => {
    expect(source).toContain('se requieren la clave y el motivo');
  });

  test('registra quién ejecuta el retiro', () => {
    expect(source).toMatch(/process\.env\.USER/);
  });

  test('advierte que el retiro no reescribe historial (RN-32)', () => {
    expect(source).toContain('NO se modifican');
  });

  test('ofrece inspección antes de retirar', () => {
    expect(source).toContain('--list');
    expect(source).toContain('--show');
  });

  test('NO construye ninguna interfaz gráfica ni endpoint HTTP', () => {
    // El escenario 7 lo pide explícitamente: utilidad interna sin interfaz
    expect(source).not.toContain('onCall');
    expect(source).not.toContain('onRequest');
    expect(source).not.toContain('express');
  });

  test('no introduce control de acceso por rol', () => {
    // El control es el del entorno: quien puede ejecutarlo ya tiene credenciales
    expect(source).not.toContain('isAdmin');
    expect(source).not.toContain('custom claim');
  });
});

// ---------------------------------------------------------------------------
// Contrato con el repositorio
// ---------------------------------------------------------------------------

describe('contrato del retiro operativo', () => {
  const mockCatalogGet = jest.fn();
  const mockCatalogUpdate = jest.fn();
  const mockAuditAdd = jest.fn();

  let retireEntry;
  let GLOBAL_EQUIVALENCE_STATUS;
  let GLOBAL_EQUIVALENCE_AUDIT_ACTIONS;

  beforeAll(() => {
    jest.resetModules();

    jest.doMock('../../firebaseAdmin', () => ({
      firestore: jest.fn(() => ({
        collection: jest.fn((name) => {
          if (name === 'symbolEquivalenceAudit') {
            return { add: mockAuditAdd };
          }

          return {
            doc: jest.fn(() => ({
              get: mockCatalogGet,
              update: mockCatalogUpdate,
              set: jest.fn(),
              collection: jest.fn(() => ({ doc: jest.fn(() => ({ set: jest.fn() })) })),
            })),
          };
        }),
        getAll: jest.fn(async () => []),
        batch: jest.fn(() => ({ set: jest.fn(), commit: jest.fn() })),
      })),
    }));

    // eslint-disable-next-line global-require
    ({ retireEntry } = require('../../transactions/services/globalEquivalenceRepository'));
    // eslint-disable-next-line global-require
    ({ GLOBAL_EQUIVALENCE_STATUS, GLOBAL_EQUIVALENCE_AUDIT_ACTIONS } = require('../../transactions/types'));
  });

  beforeEach(() => {
    mockCatalogGet.mockReset();
    mockCatalogUpdate.mockReset();
    mockAuditAdd.mockReset();
    mockAuditAdd.mockResolvedValue({ id: 'audit-1' });
    mockCatalogGet.mockResolvedValue({
      exists: true,
      data: () => ({
        resolvedSymbol: 'VUAA.L',
        status: 'active',
      }),
    });
  });

  test('Escenario 7: el retiro surte efecto de inmediato', async () => {
    const result = await retireEntry({
      key: 'broker:degiro::VUAA',
      reason: 'Apuntaba al ETF de acumulación equivocado',
      actor: 'carlos',
    });

    expect(result).toBe(true);
    expect(mockCatalogUpdate).toHaveBeenCalledWith(expect.objectContaining({
      status: GLOBAL_EQUIVALENCE_STATUS.RETIRED,
    }));
  });

  test('DoD: deja registro de la entrada retirada y su fecha', async () => {
    await retireEntry({
      key: 'broker:degiro::VUAA',
      reason: 'Motivo del retiro',
      actor: 'carlos',
    });

    const audit = mockAuditAdd.mock.calls[0][0];

    expect(audit).toMatchObject({
      key: 'broker:degiro::VUAA',
      action: GLOBAL_EQUIVALENCE_AUDIT_ACTIONS.RETIRED_BY_OPERATOR,
      actor: 'carlos',
      reason: 'Motivo del retiro',
    });
    expect(audit.at).toBeTruthy();
    expect(new Date(audit.at).toString()).not.toBe('Invalid Date');
  });

  test('RN-32: solo cambia estado y fecha, ninguna transacción', async () => {
    await retireEntry({ key: 'k', reason: 'r', actor: 'a' });

    expect(Object.keys(mockCatalogUpdate.mock.calls[0][0]).sort())
      .toEqual(['retiredAt', 'status']);
  });

  test('la auditoría no expone datos de ningún usuario final', async () => {
    await retireEntry({ key: 'k', reason: 'r', actor: 'operador' });

    const audit = mockAuditAdd.mock.calls[0][0];

    expect(audit).not.toHaveProperty('userId');
    expect(audit).not.toHaveProperty('userHash');
    expect(audit).not.toHaveProperty('portfolioAccountId');
  });

  test('una entrada inexistente no se retira', async () => {
    mockCatalogGet.mockResolvedValue({ exists: false });

    expect(await retireEntry({ key: 'no-existe', reason: 'r', actor: 'a' })).toBe(false);
    expect(mockCatalogUpdate).not.toHaveBeenCalled();
  });
});
