/**
 * HU 5.2 — Las compuertas del lanzamiento.
 *
 * Son las que antes vivían en la Lambda (`rate_limit.py`, GATE 3 y 4) y que
 * desde mayo de 2026 no llegaban a ejecutarse, porque el 502 cortaba antes.
 * Cubren QA-13, QA-14, QA-16, QA-27, QA-28 y QA-29.
 *
 * @see services/handlers/copilotHandlers.js
 */

const firestoreFalso = {};

jest.mock('../../firebaseAdmin', () => ({
  firestore: () => firestoreFalso,
}));
jest.mock('../../copilot/copilotRunRepository');
jest.mock('../../copilot/copilotPositions');
jest.mock('../../copilot/copilotSpendGuard');

const runRepository = require('../../copilot/copilotRunRepository');
const positions = require('../../copilot/copilotPositions');
const spendGuard = require('../../copilot/copilotSpendGuard');
const handlers = require('../copilotHandlers');

/**
 * Firestore con el documento del usuario, y el módulo de posiciones diciendo si
 * las hay.
 *
 * Las posiciones se mockean **por módulo** y no falsificando la cadena de
 * consulta: cuando este archivo fingía un `where().limit().get()` genérico,
 * daba por buena cualquier consulta —incluida la que filtraba `assets` por un
 * `userId` que esos documentos no tienen—. Cómo se resuelve la propiedad de un
 * activo se prueba en `copilotPositions.test.js`, contra la forma real.
 */
function montarFirestore({ userData = null, tienePosiciones = true } = {}) {
  firestoreFalso.collection = () => ({
    doc: () => ({
      get: async () => ({ exists: userData !== null, data: () => userData }),
    }),
  });

  positions.hayPosicionAbierta.mockResolvedValue(tienePosiciones);
  spendGuard.reservar.mockResolvedValue({ concedido: true, usados: 1 });
}

const CONTEXTO = { auth: { uid: 'u-pro' } };

const userDataPro = (overrides = {}) => ({
  subscription: { planId: 'pro', status: 'active' },
  lastCopilotAnalysis: null,
  ...overrides,
});

/** El plan gratuito ya no entra al Copilot (decisión del PO, 2026-10-13). */
const userDataFree = (overrides = {}) => ({
  subscription: { planId: 'free', status: 'active' },
  ...overrides,
});

/** Un Pro que ya gastó su tope de hoy. */
const userDataProSinCupo = () => userDataPro({
  lastCopilotAnalysis: '2026-10-09T09:00:00.000Z',
});

const runEnCurso = {
  runId: 'run-ya-corriendo',
  userId: 'u-pro',
  status: 'running',
  stage: 'fetching_insights',
  startedAt: '2026-10-09T10:00:00.000Z',
  stageAt: '2026-10-09T10:01:00.000Z',
};

/**
 * El reloj, fijo. `startAnalysis` y `getAnalysisState` calculan la fecha con
 * `new Date()` por dentro, así que un test con fechas absolutas y el reloj real
 * caduca solo: el que defiende RN-5.5-O habría empezado a fallar el 13 de
 * octubre, y una red que se cae sola no defiende nada.
 */
const HOY = new Date('2026-10-09T10:00:00.000Z');

beforeAll(() => {
  jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] });
  jest.setSystemTime(HOY);
});

afterAll(() => {
  jest.useRealTimers();
});

beforeEach(() => {
  jest.clearAllMocks();
  runRepository.getRun.mockResolvedValue(null);
  runRepository.isActive.mockImplementation((r) => !!r && ['queued', 'running'].includes(r.status));
  runRepository.isStalled.mockReturnValue(false);
  runRepository.startRun.mockImplementation(async (userId, opciones) => ({
    started: true,
    run: { runId: 'run-nuevo', userId, status: 'queued', ...opciones },
  }));
});

describe('startAnalysis — el Copilot es de Pro y Lifetime', () => {
  it('Pro activo sí puede', async () => {
    montarFirestore({ userData: userDataPro() });

    const { started } = await handlers.startAnalysis(CONTEXTO, {});

    expect(started).toBe(true);
  });

  it('el plan gratuito se rechaza por plan, no por cupo', async () => {
    // La diferencia importa: "sin cupo" le haría esperar a una fecha que nunca
    // le va a servir. Lo que le falta es el plan.
    montarFirestore({ userData: userDataFree() });

    await expect(handlers.startAnalysis(CONTEXTO, {})).rejects.toMatchObject({
      code: 'permission-denied',
    });
    expect(runRepository.startRun).not.toHaveBeenCalled();
  });

  it('quien no tiene suscripción tampoco entra', async () => {
    montarFirestore({ userData: null });

    await expect(handlers.startAnalysis(CONTEXTO, {})).rejects.toMatchObject({
      code: 'permission-denied',
    });
  });

  it('dice cuántos análisis le quedan después de éste', async () => {
    montarFirestore({ userData: userDataPro() });

    const respuesta = await handlers.startAnalysis(CONTEXTO, {});

    // Tenía su análisis de la semana y acaba de gastarlo.
    expect(respuesta.remaining).toBe(0);
  });
});

describe('startAnalysis — uno a la vez (QA-13, QA-14)', () => {
  it('con uno en curso devuelve ese, para llevar al usuario a su progreso', async () => {
    montarFirestore({ userData: userDataPro() });
    runRepository.getRun.mockResolvedValue(runEnCurso);

    const respuesta = await handlers.startAnalysis(CONTEXTO, {});

    expect(respuesta.started).toBe(false);
    expect(respuesta.run.runId).toBe('run-ya-corriendo');
    expect(runRepository.startRun).not.toHaveBeenCalled();
  });

  it('el análisis en curso manda sobre el cupo: todavía no lo ha consumido', async () => {
    // Con el cupo agotado de verdad: si se comprobara el cupo primero, el
    // usuario con un análisis corriendo veria "sin cupo" en lugar de su propio
    // progreso.
    montarFirestore({ userData: userDataProSinCupo() });
    runRepository.getRun.mockResolvedValue(runEnCurso);

    const respuesta = await handlers.startAnalysis(CONTEXTO, {});

    // Si se comprobara el cupo primero, esto sería un `resource-exhausted` y el
    // usuario vería "sin cupo" en lugar de su propio progreso.
    expect(respuesta.started).toBe(false);
    expect(respuesta.run.runId).toBe('run-ya-corriendo');
    expect(respuesta.nextAvailable).toBeNull();
  });

  it('un análisis colgado no bloquea el lanzamiento (Escenario 9)', async () => {
    montarFirestore({ userData: userDataPro() });
    runRepository.getRun.mockResolvedValue(runEnCurso);
    runRepository.isStalled.mockReturnValue(true);

    const { started } = await handlers.startAnalysis(CONTEXTO, {});

    expect(started).toBe(true);
  });
});

describe('startAnalysis — el cupo semanal', () => {
  it('con el análisis de la semana gastado responde cuándo vuelve', async () => {
    montarFirestore({ userData: userDataProSinCupo() });

    await expect(handlers.startAnalysis(CONTEXTO, {})).rejects.toMatchObject({
      code: 'resource-exhausted',
    });
    expect(runRepository.startRun).not.toHaveBeenCalled();
  });

  it('el rechazo lleva **la misma fecha** que informa el estado (RN-5.5-O)', async () => {
    // Ningún camino aplica otra cuenta: la fecha que el usuario ve y la que le
    // aplica el sistema salen de la misma llamada.
    montarFirestore({ userData: userDataProSinCupo() });
    runRepository.getRun.mockResolvedValue(null);

    const estado = await handlers.getAnalysisState(CONTEXTO);
    const rechazo = await handlers.startAnalysis(CONTEXTO, {}).catch((e) => e);

    expect(rechazo.details.nextAvailable).toBe(estado.nextAvailable);
    // Una semana desde el último completado.
    expect(new Date(estado.nextAvailable).toISOString()).toBe('2026-10-16T09:00:00.000Z');
  });

  it('con cupo de sobra no se le pone ninguna fecha', async () => {
    montarFirestore({ userData: userDataPro() });
    runRepository.getRun.mockResolvedValue(null);

    const estado = await handlers.getAnalysisState(CONTEXTO);

    expect(estado.nextAvailable).toBeNull();
    expect(estado.remaining).toBe(1);
  });

  it('con un análisis en curso no se le muestra fecha de cupo', async () => {
    // Todavía no lo ha consumido: decirle "disponible el [fecha]" mientras su
    // análisis corre sería contar un cupo que sigue siendo suyo.
    montarFirestore({ userData: userDataProSinCupo() });
    runRepository.getRun.mockResolvedValue(runEnCurso);

    const respuesta = await handlers.startAnalysis(CONTEXTO, {});

    expect(respuesta.started).toBe(false);
    expect(respuesta.nextAvailable).toBeNull();
    // Y sin el análisis en curso, ese mismo usuario sí tendría fecha.
    runRepository.getRun.mockResolvedValue(null);
    const rechazo = await handlers.startAnalysis(CONTEXTO, {}).catch((e) => e);
    expect(rechazo.details.nextAvailable).not.toBeUndefined();
  });
});

describe('startAnalysis — sin posiciones (QA-16)', () => {
  it('no lanza nada ni consume crédito', async () => {
    montarFirestore({ userData: userDataPro(), tienePosiciones: false });

    await expect(handlers.startAnalysis(CONTEXTO, {})).rejects.toMatchObject({
      code: 'failed-precondition',
    });
    expect(runRepository.startRun).not.toHaveBeenCalled();
  });
});

describe('startAnalysis — validación de entrada', () => {
  it('rechaza un idioma que el análisis no sabe escribir', async () => {
    montarFirestore({ userData: userDataPro() });

    await expect(handlers.startAnalysis(CONTEXTO, { language: 'pt' })).rejects.toMatchObject({
      code: 'invalid-argument',
    });
  });

  it('rechaza una moneda mal formada', async () => {
    montarFirestore({ userData: userDataPro() });

    await expect(handlers.startAnalysis(CONTEXTO, { currency: 'DOLARES' })).rejects.toMatchObject({
      code: 'invalid-argument',
    });
  });

  it('guarda el idioma y la moneda con que se lanzó (Escenario 16)', async () => {
    montarFirestore({ userData: userDataPro() });

    await handlers.startAnalysis(CONTEXTO, { language: 'en', currency: 'EUR' });

    expect(runRepository.startRun).toHaveBeenCalledWith('u-pro', {
      language: 'en',
      currency: 'EUR',
    });
  });
});

describe('startAnalysis — aislamiento entre usuarios (QA-29)', () => {
  it('el análisis que se consulta es siempre el de quien llama', async () => {
    montarFirestore({ userData: userDataPro() });

    await handlers.startAnalysis({ auth: { uid: 'u-pro-b' } }, {});

    expect(runRepository.getRun).toHaveBeenCalledWith('u-pro-b');
    expect(runRepository.startRun).toHaveBeenCalledWith('u-pro-b', expect.any(Object));
  });
});

describe('getAnalysisState', () => {
  it('devuelve el análisis en curso, el acceso y lo que queda del tope', async () => {
    montarFirestore({ userData: userDataPro() });
    runRepository.getRun.mockResolvedValue(runEnCurso);

    const estado = await handlers.getAnalysisState(CONTEXTO);

    expect(estado.run.runId).toBe('run-ya-corriendo');
    expect(estado.hasAccess).toBe(true);
    expect(estado.remaining).toBe(1);
    expect(estado.nextAvailable).toBeNull();
  });

  it('al plan gratuito le dice que no tiene acceso, sin fecha que esperar', async () => {
    montarFirestore({ userData: userDataFree() });
    runRepository.getRun.mockResolvedValue(null);

    const estado = await handlers.getAnalysisState(CONTEXTO);

    expect(estado.hasAccess).toBe(false);
    expect(estado.remaining).toBe(0);
    expect(estado.nextAvailable).toBeNull();
  });

  it('a quien gastó su análisis le da la fecha en que vuelve', async () => {
    montarFirestore({ userData: userDataProSinCupo() });
    runRepository.getRun.mockResolvedValue(null);

    const estado = await handlers.getAnalysisState(CONTEXTO);

    expect(estado.remaining).toBe(0);
    expect(estado.nextAvailable).not.toBeNull();
  });
});

describe('el tope de gasto de la plataforma', () => {
  it('agotado el presupuesto del día, no se lanza nada', async () => {
    montarFirestore({ userData: userDataPro() });
    spendGuard.reservar.mockResolvedValue({ concedido: false, usados: 10 });

    await expect(handlers.startAnalysis(CONTEXTO, {})).rejects.toMatchObject({
      code: 'resource-exhausted',
      details: { platformLimit: true },
    });
    expect(runRepository.startRun).not.toHaveBeenCalled();
  });

  it('se reserva al final: un lanzamiento que iba a rechazarse no gasta presupuesto', async () => {
    // Sin posiciones el análisis no sale; apartar presupuesto para él lo
    // habría restado del día sin que nadie llegara a investigar nada.
    montarFirestore({ userData: userDataPro(), tienePosiciones: false });

    await expect(handlers.startAnalysis(CONTEXTO, {})).rejects.toMatchObject({
      code: 'failed-precondition',
    });
    expect(spendGuard.reservar).not.toHaveBeenCalled();
  });

  it('al plan gratuito tampoco le gasta presupuesto', async () => {
    montarFirestore({ userData: userDataFree() });

    await handlers.startAnalysis(CONTEXTO, {}).catch(() => {});

    expect(spendGuard.reservar).not.toHaveBeenCalled();
  });
});
