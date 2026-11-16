/**
 * HU 6.12 — La marca recuerda la fecha más antigua de toda la corrección
 * (`desdeInicial`), desde la que se rehacen los resúmenes al terminarla
 * (RN-6.12-C; QA-05, QA-08).
 *
 * @see platform-docs/stories/6.12-correccion-desde-fecha-afectada/refinamiento.md (T3, T6c)
 */

const {
  store: mockStore,
  clone: mockClone,
} = require('../../services/__tests__/support/firestoreEnMemoria');

jest.mock('firebase-admin/firestore', () => require('../../services/__tests__/support/firestoreEnMemoria').firestoreModule);

const { markPerformanceAsStale, fechaMasAntiguaDeLaCorreccion } = require('../performanceStaleMarker');

const RUTA = 'portfolioPerformance/uid-inversor-G';
const staleOf = () => mockClone(mockStore.docs.get(RUTA)._stale);

beforeEach(() => {
  mockStore.docs = new Map([[RUTA, { userId: 'uid-inversor-G' }]]);
  mockStore.failCommit = null;
  mockStore.afterCommit = null;
  jest.spyOn(console, 'log').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('markPerformanceAsStale — la fecha más antigua de la corrección (HU 6.12, RN-6.12-C)', () => {
  test.each([
    ['COR-3S y después COR-2M', ['2026-10-26', '2026-09-16']],
    ['COR-2M y después COR-3S', ['2026-09-16', '2026-10-26']],
  ])('QA-05 %s: desdeInicial es la más antigua', async (_caso, fechas) => {
    for (const f of fechas) await markPerformanceAsStale('uid-inversor-G', f, { transactionType: 'buy' });
    expect(staleOf()).toMatchObject({ since: '2026-09-16', desdeInicial: '2026-09-16' });
  });

  test('QA-08: una operación posterior al tramo en curso no mueve desdeInicial aunque since ya avanzó', async () => {
    mockStore.docs.get(RUTA)._stale = {
      since: '2025-09-01', desdeInicial: '2025-06-02', registeredAt: '2026-11-16T12:00:00.000Z',
      pendienteDesde: '2026-11-16T12:00:00.000Z', retryCount: 0, intentos: [],
    };

    await markPerformanceAsStale('uid-inversor-G', '2025-08-04', { transactionType: 'sell' });

    expect(staleOf()).toMatchObject({ since: '2025-08-04', desdeInicial: '2025-06-02' });
  });

  test('una marca anterior a la HU, sin desdeInicial, no la gana: termina por el camino de hoy', async () => {
    mockStore.docs.get(RUTA)._stale = {
      since: '2025-09-01', registeredAt: '2026-11-10T12:00:00.000Z', pendienteDesde: '2026-11-10T12:00:00.000Z',
      retryCount: 0, intentos: [],
    };

    await markPerformanceAsStale('uid-inversor-G', '2025-08-04', { transactionType: 'sell' });

    expect(staleOf().since).toBe('2025-08-04');
    expect(staleOf()).not.toHaveProperty('desdeInicial');
  });
});

describe('fechaMasAntiguaDeLaCorreccion', () => {
  test('sin marca, con marca nueva y con marca anterior a la HU', () => {
    expect(fechaMasAntiguaDeLaCorreccion(null, '2026-10-26')).toBe('2026-10-26');
    expect(fechaMasAntiguaDeLaCorreccion({ desdeInicial: '2026-09-16' }, '2026-10-26')).toBe('2026-09-16');
    expect(fechaMasAntiguaDeLaCorreccion({ desdeInicial: '2026-10-26' }, '2026-09-16')).toBe('2026-09-16');
    expect(fechaMasAntiguaDeLaCorreccion({ since: '2026-10-26' }, '2026-09-16')).toBeNull();
  });
});
