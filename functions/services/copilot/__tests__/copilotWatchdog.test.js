/**
 * HU 5.2 — RN-5.2-Q: el vigilante del análisis colgado.
 *
 * Un aviso en la campana no lo puede crear el navegador, ni liberar el cupo
 * tampoco: por eso existe este job (Escenario 9, QA-20).
 *
 * @see services/copilot/copilotWatchdog.js
 */

jest.mock('firebase-functions/v2/scheduler', () => ({
  onSchedule: (_opts, handler) => handler,
}));
jest.mock('../copilotRunRepository');
jest.mock('../copilotNotifications');

const runRepository = require('../copilotRunRepository');
const notifications = require('../copilotNotifications');
const { marcarColgados, FUNCTION_CONFIG } = require('../copilotWatchdog');
const { FAILURE_CODE } = require('../copilotConstants');

const crearColgado = (overrides = {}) => ({
  runId: 'run-colgado',
  userId: 'u-pro',
  stage: 'fetching_insights',
  language: 'es',
  ...overrides,
});

beforeEach(() => {
  jest.clearAllMocks();
  notifications.notifyAnalysisFailed.mockResolvedValue(undefined);
});

describe('marcarColgados', () => {
  it('da por fallido cada análisis colgado y avisa en la campana', async () => {
    runRepository.findStalledRuns.mockResolvedValue([
      crearColgado(),
      crearColgado({ runId: 'run-2', userId: 'u-life', language: 'en' }),
    ]);
    runRepository.failRun.mockResolvedValue(true);

    const { revisados, fallidos } = await marcarColgados();

    expect(revisados).toBe(2);
    expect(fallidos).toBe(2);
    expect(runRepository.failRun).toHaveBeenCalledWith('u-pro', 'run-colgado', FAILURE_CODE.STALLED);
    expect(notifications.notifyAnalysisFailed).toHaveBeenCalledWith('u-life', { language: 'en' });
  });

  it('no avisa de un análisis que terminó entre la consulta y el cierre', async () => {
    runRepository.findStalledRuns.mockResolvedValue([crearColgado()]);
    runRepository.failRun.mockResolvedValue(false);

    const { revisados, fallidos } = await marcarColgados();

    expect(revisados).toBe(1);
    expect(fallidos).toBe(0);
    expect(notifications.notifyAnalysisFailed).not.toHaveBeenCalled();
  });

  it('con nada colgado no toca nada: es el caso normal', async () => {
    runRepository.findStalledRuns.mockResolvedValue([]);

    const { revisados, fallidos } = await marcarColgados();

    expect(revisados).toBe(0);
    expect(fallidos).toBe(0);
    expect(runRepository.failRun).not.toHaveBeenCalled();
  });

  it('un aviso que no se puede entregar no deja el análisis bloqueado', async () => {
    runRepository.findStalledRuns.mockResolvedValue([crearColgado()]);
    runRepository.failRun.mockResolvedValue(true);
    notifications.notifyAnalysisFailed.mockRejectedValue(new Error('campana caída'));

    const { fallidos } = await marcarColgados();

    // Lo que importa del Escenario 9 es que el usuario pueda lanzar otro: el
    // análisis quedó liberado aunque el aviso se perdiera.
    expect(fallidos).toBe(1);
    expect(runRepository.failRun).toHaveBeenCalled();
  });
});

describe('configuración del job', () => {
  it('corre cada minuto para que el plazo no se estire de más', () => {
    expect(FUNCTION_CONFIG.schedule).toBe('every 1 minutes');
  });

  it('usa la zona horaria de los demás jobs del proyecto', () => {
    expect(FUNCTION_CONFIG.timeZone).toBe('America/New_York');
  });
});
