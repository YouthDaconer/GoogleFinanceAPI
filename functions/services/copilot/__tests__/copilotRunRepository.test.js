/**
 * HU 5.2 — El ciclo de vida del análisis.
 *
 * Defiende las tres reglas que el repositorio del análisis en curso sostiene
 * por sí solo: uno a la vez (RN-5-B), el plazo de los 15 minutos medido contra
 * la última etapa (RN-5.2-Q) y que un resultado tardío no pise un desenlace ya
 * escrito (QA-21).
 *
 * @see services/copilot/copilotRunRepository.js
 */

const firestoreFalso = {};

jest.mock('../../firebaseAdmin', () => ({
  firestore: () => firestoreFalso,
}));

const repo = require('../copilotRunRepository');
const { RUN_STATUS, STEP_KIND, STEP_STATUS } = require('../copilotConstants');

// ============================================================================
// FIRESTORE DE MENTIRA
// ============================================================================

/**
 * Un Firestore con un solo documento por usuario y transacciones de verdad
 * (lectura y escritura sobre el mismo objeto), que es lo único que necesita
 * este módulo.
 */
function montarFirestore(documentos = {}) {
  const docs = { ...documentos };
  const consultas = [];

  const refDe = (id) => ({
    _id: id,
    async get() {
      return { exists: docs[id] !== undefined, data: () => docs[id] };
    },
  });

  firestoreFalso.collection = (nombre) => {
    const q = { coleccion: nombre, filtros: [] };
    const consulta = {
      doc: refDe,
      where(campo, op, valor) {
        q.filtros.push({ campo, op, valor });
        return consulta;
      },
      limit(n) {
        q.limite = n;
        return consulta;
      },
      async get() {
        consultas.push(q);
        return {
          docs: Object.entries(docs)
            .filter(([, d]) => cumple(d, q.filtros))
            .map(([id, d]) => ({ id, data: () => d })),
        };
      },
    };
    return consulta;
  };

  firestoreFalso.runTransaction = async (fn) =>
    fn({
      get: (ref) => ref.get(),
      set: (ref, valor) => {
        docs[ref._id] = valor;
      },
      update: (ref, cambios) => {
        docs[ref._id] = { ...docs[ref._id], ...cambios };
      },
    });

  return { docs, consultas };
}

function cumple(doc, filtros) {
  return filtros.every(({ campo, op, valor }) => {
    const actual = doc[campo];
    if (op === 'in') return valor.includes(actual);
    if (op === '<=') return actual <= valor;
    return actual === valor;
  });
}

const AHORA = new Date('2026-10-09T10:00:00.000Z');

/**
 * El reloj se congela porque `isStalled` mide contra `Date.now()`: con el reloj
 * real, cualquier dato de prueba acaba "colgado" al dia siguiente de escribirlo.
 */
beforeEach(() => {
  jest.useFakeTimers().setSystemTime(new Date('2026-10-09T10:02:00.000Z'));
});

afterEach(() => {
  jest.useRealTimers();
});

const crearRun = (overrides = {}) => ({
  runId: 'run-1',
  userId: 'u-pro',
  status: RUN_STATUS.RUNNING,
  // HU 5.3: el plan de pasos sustituye al enum de tres etapas
  steps: [
    { kind: STEP_KIND.CLEANSING, status: STEP_STATUS.DONE },
    { kind: STEP_KIND.ASSET, ticker: 'MSFT', status: STEP_STATUS.RUNNING },
    { kind: STEP_KIND.COMPOSING, status: STEP_STATUS.PENDING },
  ],
  stepIndex: 1,
  startedAt: '2026-10-09T10:00:00.000Z',
  stageAt: '2026-10-09T10:01:00.000Z',
  finishedAt: null,
  language: 'es',
  currency: 'USD',
  analysisId: null,
  errorCode: null,
  ...overrides,
});

// ============================================================================
// TESTS
// ============================================================================

describe('isStalled — RN-5.2-Q: 15 minutos sin cambiar de paso', () => {
  it('a los 14 min 59 s sigue en curso (QA-20, primera fila)', () => {
    const run = crearRun({ stageAt: '2026-10-09T10:01:00.000Z' });
    expect(repo.isStalled(run, new Date('2026-10-09T10:15:59.000Z'))).toBe(false);
  });

  it('a los 15 minutos exactos se da por colgado (QA-20, segunda fila)', () => {
    const run = crearRun({ stageAt: '2026-10-09T10:01:00.000Z' });
    expect(repo.isStalled(run, new Date('2026-10-09T10:16:00.000Z'))).toBe(true);
  });

  it('un análisis de 20 minutos con pasos de 6, 7 y 7 NO está colgado (QA-20, tercera fila)', () => {
    // La medida es contra el último paso, no contra el inicio: lleva 20 min
    // desde que arrancó, pero sólo 7 en el paso en que va.
    const run = crearRun({
      startedAt: '2026-10-09T10:00:00.000Z',
      stageAt: '2026-10-09T10:13:00.000Z',
    });
    expect(repo.isStalled(run, new Date('2026-10-09T10:20:00.000Z'))).toBe(false);
  });

  it('un análisis terminado nunca está colgado', () => {
    const run = crearRun({ status: RUN_STATUS.COMPLETED, stageAt: '2026-01-01T00:00:00.000Z' });
    expect(repo.isStalled(run, AHORA)).toBe(false);
  });

  it('sin análisis no hay nada colgado', () => {
    expect(repo.isStalled(null, AHORA)).toBe(false);
  });
});

describe('startRun — RN-5-B: un análisis en curso por usuario', () => {
  it('abre el análisis cuando el usuario no tiene ninguno', async () => {
    const { docs } = montarFirestore();

    const { started, run } = await repo.startRun('u-pro', { language: 'es', currency: 'USD' });

    expect(started).toBe(true);
    expect(run.status).toBe(RUN_STATUS.QUEUED);
    // HU 5.3: nace con un solo paso, la depuración; el resto se añaden cuando
    // ya se sabe qué activos se van a investigar.
    expect(run.steps).toEqual([{ kind: STEP_KIND.CLEANSING, status: STEP_STATUS.PENDING }]);
    expect(run.stepIndex).toBe(0);
    expect(docs['u-pro'].runId).toBe(run.runId);
  });

  it('con uno en curso devuelve el que ya corre y no abre otro (QA-14)', async () => {
    const existente = crearRun({ runId: 'run-ya-corriendo' });
    const { docs } = montarFirestore({ 'u-pro': existente });

    const { started, run } = await repo.startRun('u-pro', { language: 'es', currency: 'USD' });

    expect(started).toBe(false);
    expect(run.runId).toBe('run-ya-corriendo');
    expect(docs['u-pro'].runId).toBe('run-ya-corriendo');
  });

  it('un análisis colgado no bloquea: el nuevo lo sustituye (Escenario 9)', async () => {
    const colgado = crearRun({ runId: 'run-colgado', stageAt: '2026-01-01T00:00:00.000Z' });
    const { docs } = montarFirestore({ 'u-pro': colgado });

    const { started, run } = await repo.startRun('u-pro', { language: 'es', currency: 'USD' });

    expect(started).toBe(true);
    expect(run.runId).not.toBe('run-colgado');
    expect(docs['u-pro'].status).toBe(RUN_STATUS.QUEUED);
  });

  it('tras uno completado se puede abrir otro', async () => {
    const terminado = crearRun({ status: RUN_STATUS.COMPLETED, analysisId: '20261009T100000Z' });
    montarFirestore({ 'u-pro': terminado });

    const { started } = await repo.startRun('u-pro', { language: 'en', currency: 'EUR' });

    expect(started).toBe(true);
  });

  it('guarda el idioma y la moneda del lanzamiento (Escenario 16)', async () => {
    const { docs } = montarFirestore();

    await repo.startRun('u-life', { language: 'en', currency: 'EUR' });

    expect(docs['u-life'].language).toBe('en');
    expect(docs['u-life'].currency).toBe('EUR');
  });
});

describe('setSteps — el plan, cuando ya se sabe qué investigar (HU 5.3)', () => {
  it('conserva los pasos ya hechos y añade los nuevos detrás', async () => {
    const recienLanzado = crearRun({
      steps: [{ kind: STEP_KIND.CLEANSING, status: STEP_STATUS.RUNNING }],
      stepIndex: 0,
    });
    const { docs } = montarFirestore({ 'u-pro': recienLanzado });

    const ok = await repo.setSteps('u-pro', 'run-1', [
      { kind: STEP_KIND.ASSET, ticker: 'MSFT', reason: 'peso' },
      { kind: STEP_KIND.MACRO },
    ]);

    expect(ok).toBe(true);
    // El usuario no ve desaparecer la etapa que está mirando.
    expect(docs['u-pro'].steps[0].kind).toBe(STEP_KIND.CLEANSING);
    expect(docs['u-pro'].steps.map((p) => p.kind)).toEqual([
      STEP_KIND.CLEANSING,
      STEP_KIND.ASSET,
      STEP_KIND.MACRO,
    ]);
    expect(docs['u-pro'].steps[1].status).toBe(STEP_STATUS.PENDING);
  });

  it('no toca el plan de un análisis que ya no es el vigente', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun({ runId: 'run-nuevo' }) });

    const ok = await repo.setSteps('u-pro', 'run-viejo', [{ kind: STEP_KIND.MACRO }]);

    expect(ok).toBe(false);
    expect(docs['u-pro'].steps).toHaveLength(3);
  });
});

describe('startStep y finishStep — el latido, paso a paso (HU 5.3)', () => {
  it('entrar en un paso lo marca en curso y estampa el latido', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun() });

    const ok = await repo.startStep('u-pro', 'run-1', 2);

    expect(ok).toBe(true);
    expect(docs['u-pro'].steps[2].status).toBe(STEP_STATUS.RUNNING);
    expect(docs['u-pro'].stepIndex).toBe(2);
    expect(docs['u-pro'].status).toBe(RUN_STATUS.RUNNING);
    // Con una etapa por activo el latido es mucho más fino: una investigación
    // larga avanza paso a paso y nunca parece colgada.
    expect(docs['u-pro'].stageAt).toBe('2026-10-09T10:02:00.000Z');
  });

  it('cerrar un paso guarda lo que obtuvo', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun() });

    await repo.finishStep('u-pro', 'run-1', 1, { ok: true, summary: 'resultados Q3, consenso' });

    expect(docs['u-pro'].steps[1].status).toBe(STEP_STATUS.DONE);
    expect(docs['u-pro'].steps[1].summary).toBe('resultados Q3, consenso');
  });

  it('un paso que no se pudo hacer se cierra como fallido, sin resumen (RN-5.3-R)', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun() });

    await repo.finishStep('u-pro', 'run-1', 1, { ok: false });

    expect(docs['u-pro'].steps[1].status).toBe(STEP_STATUS.FAILED);
    expect(docs['u-pro'].steps[1].summary).toBeNull();
  });

  it('no avanza un análisis que ya no es el vigente', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun({ runId: 'run-nuevo' }) });

    expect(await repo.startStep('u-pro', 'run-viejo', 2)).toBe(false);
    expect(docs['u-pro'].steps[2].status).toBe(STEP_STATUS.PENDING);
  });

  it('un índice que no existe en el plan no escribe nada', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun() });

    expect(await repo.startStep('u-pro', 'run-1', 99)).toBe(false);
    expect(docs['u-pro'].stepIndex).toBe(1);
  });
});

describe('completeRun y failRun — QA-21: el resultado tardío se descarta', () => {
  it('completa el análisis y guarda su id', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun() });

    const cerro = await repo.completeRun('u-pro', 'run-1', '20261009T100000Z');

    expect(cerro).toBe(true);
    expect(docs['u-pro'].status).toBe(RUN_STATUS.COMPLETED);
    expect(docs['u-pro'].analysisId).toBe('20261009T100000Z');
  });

  it('no completa un análisis que el vigilante ya dio por fallido', async () => {
    const yaFallido = crearRun({ status: RUN_STATUS.FAILED, errorCode: 'STALLED' });
    const { docs } = montarFirestore({ 'u-pro': yaFallido });

    const cerro = await repo.completeRun('u-pro', 'run-1', '20261009T100000Z');

    expect(cerro).toBe(false);
    expect(docs['u-pro'].status).toBe(RUN_STATUS.FAILED);
    expect(docs['u-pro'].analysisId).toBeNull();
  });

  it('no completa un análisis que el usuario ya reemplazó por otro', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun({ runId: 'run-2' }) });

    const cerro = await repo.completeRun('u-pro', 'run-1', '20261009T100000Z');

    expect(cerro).toBe(false);
    expect(docs['u-pro'].runId).toBe('run-2');
  });

  it('no avisa dos veces del mismo fallo (RN-5.2-P)', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun({ status: RUN_STATUS.FAILED }) });

    const cerro = await repo.failRun('u-pro', 'run-1', 'ENGINE_FAILED');

    expect(cerro).toBe(false);
    expect(docs['u-pro'].errorCode).toBeNull();
  });

  it('marca el fallo con su código y no deja análisis', async () => {
    const { docs } = montarFirestore({ 'u-pro': crearRun() });

    const cerro = await repo.failRun('u-pro', 'run-1', 'ENGINE_QUOTA_EXHAUSTED');

    expect(cerro).toBe(true);
    expect(docs['u-pro'].status).toBe(RUN_STATUS.FAILED);
    expect(docs['u-pro'].errorCode).toBe('ENGINE_QUOTA_EXHAUSTED');
    expect(docs['u-pro'].analysisId).toBeNull();
  });
});

describe('findStalledRuns — lo que recoge el vigilante', () => {
  it('sólo trae análisis activos con el paso más viejo que el plazo', async () => {
    montarFirestore({
      'u-colgado': crearRun({ userId: 'u-colgado', stageAt: '2026-10-09T09:40:00.000Z' }),
      'u-sano': crearRun({ userId: 'u-sano', stageAt: '2026-10-09T09:59:00.000Z' }),
      'u-terminado': crearRun({
        userId: 'u-terminado',
        status: RUN_STATUS.COMPLETED,
        stageAt: '2026-01-01T00:00:00.000Z',
      }),
    });

    const colgados = await repo.findStalledRuns(AHORA);

    expect(colgados.map((r) => r.userId)).toEqual(['u-colgado']);
  });

  it('filtra por los dos campos del índice compuesto', async () => {
    const { consultas } = montarFirestore({});

    await repo.findStalledRuns(AHORA);

    const campos = consultas[0].filtros.map((f) => f.campo);
    expect(campos).toEqual(['status', 'stageAt']);
  });
});
