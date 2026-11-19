/**
 * HU 6.13 — Nadie vuelve a escribir consolidados de meses y años.
 *
 * - Los trabajos mensual y anual siguen exportados (para volver atrás sin perder
 *   sus jobs), pero con `RETIRE_CONSOLIDATED_PERIODS` encendido terminan sin leer
 *   ni escribir nada, ni siquiera sus métricas (Escenario 1, RN-6.13-B; QA-01).
 *   Tampoco borran nada: los consolidados guardados se conservan (RN-6.13-C, QA-10).
 * - Apagado, consolidan como antes (RN-6-H, QA-12).
 * - El interruptor y el aviso de rendimientos no disponibles (RN-6.13-A).
 *
 * La corrección nocturna se prueba en `reconcileTramos.test.js` (QA-02).
 *
 * @see platform-docs/stories/6.13-retirar-ruta-antigua-consolidados/refinamiento.md (T1, T3, T7a)
 */

// Firestore en memoria mínimo: anota cada lectura y cada escritura.
const mockOperaciones = [];
jest.mock('../../firebaseAdmin', () => {
  const consulta = (ruta) => ({
    where: () => consulta(ruta),
    orderBy: () => consulta(ruta),
    get: async () => {
      mockOperaciones.push(`get ${ruta}`);
      return { empty: true, size: 0, docs: [] };
    },
  });
  const doc = (ruta) => ({
    get: async () => { mockOperaciones.push(`get ${ruta}`); return { exists: false, data: () => undefined }; },
    set: async () => { mockOperaciones.push(`set ${ruta}`); },
    delete: async () => { mockOperaciones.push(`delete ${ruta}`); },
    collection: (sub) => coleccion(`${ruta}/${sub}`),
  });
  const coleccion = (ruta) => ({ ...consulta(ruta), doc: (id) => doc(`${ruta}/${id}`) });
  const db = { collection: coleccion, doc, batch: () => ({ set: () => {}, commit: async () => { mockOperaciones.push('batch.commit'); } }) };
  return { firestore: () => db };
});
jest.mock('firebase-functions/v2/scheduler', () => ({ onSchedule: (_opciones, fn) => fn }));

const {
  consolidateMonthlyPerformance,
  consolidateYearlyPerformance,
} = require('../../periodConsolidationScheduled');
const {
  consolidadosRetirados,
  rendimientosNoDisponibles,
  pantallaDe,
  CODIGO_RENDIMIENTOS_NO_DISPONIBLES,
} = require('../consolidatedPeriodsRetirementService');

let registros;

beforeEach(() => {
  mockOperaciones.length = 0;
  registros = [];
  delete process.env.RETIRE_CONSOLIDATED_PERIODS;
  const capturar = (...a) => registros.push(a.map(String).join(' '));
  jest.spyOn(console, 'log').mockImplementation(capturar);
  jest.spyOn(console, 'warn').mockImplementation(capturar);
  jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
  delete process.env.RETIRE_CONSOLIDATED_PERIODS;
});

describe('HU 6.13 — los trabajos mensual y anual quedan sin efecto (RN-6.13-B)', () => {
  test.each([
    ['consolidateMonthlyPerformance', () => consolidateMonthlyPerformance({})],
    ['consolidateYearlyPerformance', () => consolidateYearlyPerformance({})],
  ])('QA-01: %s encendido no lee, no escribe ni borra nada y lo deja en el registro', async (nombre, correr) => {
    await correr();

    expect(mockOperaciones).toEqual([]);
    expect(registros).toEqual([`[${nombre}] Retirado (HU 6.13): no se consolida`]);
  });

  test.each([
    ['consolidateMonthlyPerformance', () => consolidateMonthlyPerformance({}), 'monthly_'],
    ['consolidateYearlyPerformance', () => consolidateYearlyPerformance({}), 'yearly_'],
  ])('QA-12: %s con RETIRE_CONSOLIDATED_PERIODS=false recorre a los inversores y guarda sus métricas, como antes', async (nombre, correr, prefijo) => {
    process.env.RETIRE_CONSOLIDATED_PERIODS = 'false';

    await correr();

    expect(mockOperaciones[0]).toBe('get portfolioPerformance');
    expect(mockOperaciones.some((op) => op.startsWith(`set _systemMetrics/consolidation/executions/${prefijo}`))).toBe(true);
    expect(registros.some((r) => r.startsWith(`[${nombre}] Completado`))).toBe(true);
  });
});

describe('HU 6.13 — el interruptor (RN-6-H)', () => {
  test('encendido salvo RETIRE_CONSOLIDATED_PERIODS=false, leído en cada llamada', () => {
    expect(consolidadosRetirados()).toBe(true);
    process.env.RETIRE_CONSOLIDATED_PERIODS = 'false';
    expect(consolidadosRetirados()).toBe(false);
    process.env.RETIRE_CONSOLIDATED_PERIODS = 'true';
    expect(consolidadosRetirados()).toBe(true);
  });
});

describe('HU 6.13 — el aviso de rendimientos no disponibles (RN-6.13-A, Escenario 3)', () => {
  test('la pantalla sale del alcance y del activo', () => {
    expect(pantallaDe({ accountId: 'overall' })).toBe('portafolio');
    expect(pantallaDe({ accountId: 'cta-1' })).toBe('cuenta');
    expect(pantallaDe({ accountId: 'overall', ticker: 'AAPL', assetType: 'stock' })).toBe('activo');
  });

  test('es un `unavailable` con código, pantalla y motivo, y registra al inversor', () => {
    const error = rendimientosNoDisponibles({
      userId: 'uid-inversor-F', accountId: 'cta-1', currency: 'COP', ticker: 'VOO', assetType: 'etf', motivo: 'sin-resumen',
    });

    expect(error.code).toBe('unavailable');
    expect(error.details).toEqual({ code: CODIGO_RENDIMIENTOS_NO_DISPONIBLES, pantalla: 'activo', motivo: 'sin-resumen' });
    expect(CODIGO_RENDIMIENTOS_NO_DISPONIBLES).toBe('RETURNS_UNAVAILABLE');
    expect(registros).toEqual(['[returnsUnavailable] uid-inversor-F activo cta-1 COP VOO_etf: sin-resumen']);
  });
});
