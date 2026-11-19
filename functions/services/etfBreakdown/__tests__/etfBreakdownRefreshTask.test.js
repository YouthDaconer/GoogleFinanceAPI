/**
 * HU 6.16 — La tarea que refresca aparte un desglose vencido (RN-6.16-B, D1).
 *
 * @see platform-docs/stories/6.16-desglose-etf-compartido/refinamiento.md (T3, T9)
 */

const mockDb = { nombre: 'db-de-prueba' };
jest.mock('../../firebaseAdmin', () => ({ firestore: () => mockDb }));

const mockRefrescar = jest.fn();
jest.mock('../etfBreakdownService', () => ({
  ...jest.requireActual('../etfBreakdownService'),
  refrescarDesglose: (...a) => mockRefrescar(...a),
}));

const { atenderRefresco, FUNCTION_CONFIG } = require('../etfBreakdownRefreshTask');

beforeEach(() => {
  mockRefrescar.mockReset();
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => jest.restoreAllMocks());

describe('HU 6.16 — etfBreakdownRefreshTask', () => {
  test('refresca el ETF de la tarea, con el símbolo normalizado', async () => {
    mockRefrescar.mockResolvedValue({ resultado: 'actualizado', motivo: null, posiciones: 1860 });

    const r = await atenderRefresco({ data: { symbol: ' eimi.l ' } });

    expect(mockRefrescar).toHaveBeenCalledWith(mockDb, 'EIMI.L');
    expect(r).toMatchObject({ resultado: 'actualizado', posiciones: 1860 });
    expect(console.log).toHaveBeenCalledWith('[etfBreakdownRefreshTask] EIMI.L: actualizado');
  });

  test('si la fuente falla deja en el registro el motivo con el que se conservó el guardado (RN-6.16-C)', async () => {
    mockRefrescar.mockResolvedValue({ resultado: 'conservado', motivo: 'la fuente no respondió', posiciones: 1858 });

    await atenderRefresco({ data: { symbol: 'EIMI.L' } });

    expect(console.log).toHaveBeenCalledWith('[etfBreakdownRefreshTask] EIMI.L: conservado (la fuente no respondió)');
  });

  test('una tarea sin símbolo se descarta sin lanzar ni refrescar', async () => {
    await expect(atenderRefresco({ data: {} })).resolves.toBeUndefined();
    expect(mockRefrescar).not.toHaveBeenCalled();
  });

  test('un error inesperado no lanza: la cola no reintenta y el día siguiente vuelve a intentarlo', async () => {
    mockRefrescar.mockRejectedValue(new Error('DEADLINE_EXCEEDED'));

    await expect(atenderRefresco({ data: { symbol: 'VUAA.L' } })).resolves.toBeUndefined();
    expect(console.error).toHaveBeenCalledWith(expect.stringContaining('VUAA.L: error inesperado'));
  });

  test('la cola no reintenta (el intento del día ya está gastado) y no hay instancias fijas', () => {
    expect(FUNCTION_CONFIG.retryConfig.maxAttempts).toBe(1);
    expect(FUNCTION_CONFIG.minInstances).toBe(0);
    expect(FUNCTION_CONFIG.maxInstances).toBe(1);
  });
});
