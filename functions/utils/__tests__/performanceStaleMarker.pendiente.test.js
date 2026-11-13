/**
 * HU 6.8.2 — La marca recuerda desde cuándo espera el inversor y sus intentos
 * sin avance (RN-6.8-P, RN-6.8.2-J).
 *
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T5, T16)
 */

const {
  store: mockStore,
  clone: mockClone,
} = require('../../services/__tests__/support/firestoreEnMemoria');

jest.mock('firebase-admin/firestore', () => require('../../services/__tests__/support/firestoreEnMemoria').firestoreModule);

const { markPerformanceAsStale } = require('../performanceStaleMarker');

const RUTA = 'portfolioPerformance/inv-a';
const staleOf = () => mockClone(mockStore.docs.get(RUTA)._stale);

beforeEach(() => {
  mockStore.docs = new Map([[RUTA, { userId: 'inv-a' }]]);
  mockStore.failCommit = null;
  mockStore.afterCommit = null;
  jest.spyOn(console, 'log').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('markPerformanceAsStale — desde cuándo espera y los intentos (HU 6.8.2)', () => {
  test('la primera marca empieza a esperar al registrarse, sin intentos', async () => {
    await markPerformanceAsStale('inv-a', '2026-10-01', { reason: 'retroactive_transaction' });

    const stale = staleOf();
    expect(stale.pendienteDesde).toBe(stale.registeredAt);
    expect(stale).toMatchObject({ since: '2026-10-01', retryCount: 0, intentos: [] });
  });

  test('una operación nueva no reinicia la espera ni borra los intentos sin avance', async () => {
    const intentos = [{ noche: '2026-11-13', motivo: 'memoria', detalle: 'x', at: '2026-11-13T07:01:00.000Z' }];
    mockStore.docs.get(RUTA)._stale = {
      since: '2026-10-01',
      registeredAt: '2026-11-05T12:00:00.000Z',
      pendienteDesde: '2026-11-05T12:00:00.000Z',
      retryCount: 1,
      intentos,
      enCurso: '2026-11-14T07:00:10.000Z',
    };

    await markPerformanceAsStale('inv-a', '2026-09-15', { reason: 'asset_edit' });

    const stale = staleOf();
    expect(stale).toMatchObject({
      since: '2026-09-15',
      pendienteDesde: '2026-11-05T12:00:00.000Z',
      retryCount: 1,
      intentos,
      enCurso: '2026-11-14T07:00:10.000Z',
    });
    expect(stale.registeredAt > '2026-11-05T12:00:00.000Z').toBe(true);
  });

  test('una marca anterior a la HU, sin pendienteDesde, conserva como espera su registeredAt', async () => {
    mockStore.docs.get(RUTA)._stale = { since: '2026-10-01', registeredAt: '2026-11-08T12:00:00.000Z', retryCount: 0 };

    await markPerformanceAsStale('inv-a', '2026-10-20');

    expect(staleOf()).toMatchObject({ since: '2026-10-01', pendienteDesde: '2026-11-08T12:00:00.000Z' });
  });
});
