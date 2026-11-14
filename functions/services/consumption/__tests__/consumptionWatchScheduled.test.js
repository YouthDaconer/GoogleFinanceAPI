/**
 * HU 6.7 — El vigilante: avisa una vez por día y por consumo, el log va antes
 * que el estado, un día habitual no avisa y un fallo al medir no marca nada.
 *
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T3, T11)
 */

// === Mocks ===

jest.mock('../../firebaseAdmin', () => ({ firestore: jest.fn(() => ({})), credential: {} }));

jest.mock('firebase-functions/v2/scheduler', () => ({
  onSchedule: jest.fn((opts, handler) => handler),
}));

const { onSchedule } = require('firebase-functions/v2/scheduler');
const { BYTES_POR_GIB, lineaAviso } = require('../consumptionAlertService');

const lineaDe = (emitir, n = 0) => lineaAviso(emitir.mock.calls[n][0]);
const { vigilarConsumo, emitirAviso, FUNCTION_CONFIG } = require('../consumptionWatchScheduled');

// clearMocks borra las llamadas antes de cada test: se guarda la del registro del job.
const registroDelJob = [...onSchedule.mock.calls];

// ============================================================================
// Fábricas locales
// ============================================================================

const col = (dia, hora) => new Date(`${dia}T${hora}:00-05:00`);

const minuto = (dia, hora, valor) => {
  const inicio = col(dia, hora);
  return { inicio: inicio.toISOString(), fin: new Date(inicio.getTime() + 60000).toISOString(), valor };
};

const almacenamiento = (dia, hora, gib) => ({ inicio: '', fin: col(dia, hora).toISOString(), valor: Math.round(gib * BYTES_POR_GIB) });

/** Firestore de mentira que recuerda lo escrito y el orden de los sucesos. */
const crearDb = (inicial = {}, sucesos = []) => {
  const docs = new Map(Object.entries(inicial));
  return {
    docs,
    sucesos,
    collection: (nombre) => ({
      doc: (id) => ({
        get: async () => {
          const data = docs.get(`${nombre}/${id}`);
          return { exists: data !== undefined, data: () => data };
        },
        set: async (data, opciones) => {
          sucesos.push(`set ${nombre}/${id}`);
          const previo = opciones && opciones.merge ? docs.get(`${nombre}/${id}`) || {} : {};
          docs.set(`${nombre}/${id}`, { ...previo, ...data });
        },
      }),
    }),
  };
};

/** Medición de mentira: un día habitual salvo lo que se sobrescriba (QA-08). */
const medicionHabitual = (overrides = {}) => ({
  medirConteoDelDia: jest.fn(async ({ consumo, dia }) => (consumo === 'lecturas'
    ? [minuto(dia, '09:00', 15500)]
    : [minuto(dia, '09:00', 476)])),
  medirAlmacenamiento: jest.fn(async () => [almacenamiento('2026-11-19', '09:00', 0.355)]),
  leerUsuariosIa: jest.fn(async () => []),
  ...overrides,
});

let warnSpy;
let errorSpy;

beforeEach(() => {
  warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
  errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
});

// ============================================================================
// Tests
// ============================================================================

describe('consumptionWatchScheduled — configuración', () => {
  test('cada 15 min, una sola instancia, con FUNCTION_CONFIG del estándar', () => {
    expect(FUNCTION_CONFIG).toMatchObject({
      schedule: '*/15 * * * *',
      timeZone: 'America/New_York',
      memory: '256MiB',
      timeoutSeconds: 120,
      maxInstances: 1,
      minInstances: 0,
    });
    expect(registroDelJob).toEqual([[FUNCTION_CONFIG, expect.any(Function)]]);
  });

  test('emitirAviso escribe una sola línea JSON con WARNING', () => {
    emitirAviso({ consumo: 'lecturas', valor: 200001, desde: col('2026-11-19', '14:20').toISOString() });
    const linea = JSON.parse(warnSpy.mock.calls[0][0]);
    expect(linea).toMatchObject({ severity: 'WARNING', event: 'consumption_alert', consumo: 'lecturas', valor: '200.001 lecturas hoy' });
  });
});

describe('consumptionWatchScheduled — una ejecución', () => {
  test('QA-08: un día habitual (15.500 lecturas, 476 escrituras, 0,355 GiB, sin IA) no avisa ni escribe nada', async () => {
    const db = crearDb();
    const emitir = jest.fn();

    const r = await vigilarConsumo({ db, token: 't', ahora: col('2026-11-19', '18:00'), medicion: medicionHabitual(), emitir });

    expect(emitir).not.toHaveBeenCalled();
    expect(db.sucesos).toEqual([]);
    expect(r).toMatchObject({ dia: '2026-11-19', avisos: [], fallos: [], medidos: { lecturas: 15500, escrituras: 476 } });
  });

  test('QA-04: lecturas, escrituras y almacenamiento cruzan a la vez y cada uno avisa; el log va antes que su estado', async () => {
    const sucesos = [];
    const db = crearDb({}, sucesos);
    const emitir = jest.fn((aviso) => sucesos.push(`aviso ${aviso.consumo}`));
    const medicion = medicionHabitual({
      medirConteoDelDia: jest.fn(async ({ consumo, dia }) => (consumo === 'lecturas'
        ? [minuto(dia, '14:20', 200001)]
        : [minuto(dia, '09:10', 100001)])),
      medirAlmacenamiento: jest.fn(async () => [almacenamiento('2026-11-19', '11:30', 0.81)]),
    });

    const r = await vigilarConsumo({ db, token: 't', ahora: col('2026-11-19', '14:30'), medicion, emitir });

    expect(r.avisos).toEqual(['lecturas', 'escrituras', 'almacenamiento']);
    expect(sucesos).toEqual([
      'aviso lecturas', 'set consumptionAlerts/2026-11-19',
      'aviso escrituras', 'set consumptionAlerts/2026-11-19',
      'aviso almacenamiento', 'set consumptionAlerts/estado',
    ]);
    expect(db.docs.get('consumptionAlerts/2026-11-19').lecturas).toMatchObject({ valor: 200001, desde: col('2026-11-19', '14:20').toISOString() });
  });

  test('QA-04: a las 20:30 de Colombia (01:30 UTC del día siguiente) el aviso de lecturas del D1 no se repite', async () => {
    const db = crearDb({ 'consumptionAlerts/2026-11-19': { lecturas: { valor: 200001 } } });
    const emitir = jest.fn();
    const medicion = medicionHabitual({ medirConteoDelDia: jest.fn(async ({ consumo, dia }) => [minuto(dia, '20:00', consumo === 'lecturas' ? 330000 : 10)]) });

    await vigilarConsumo({ db, token: 't', ahora: new Date('2026-11-20T01:30:00Z'), medicion, emitir });

    expect(medicion.medirConteoDelDia).toHaveBeenCalledWith(expect.objectContaining({ dia: '2026-11-19' }));
    expect(emitir).not.toHaveBeenCalled();
  });

  test('QA-05: el D2 vuelve a avisar las lecturas aunque el D1 ya avisó', async () => {
    const db = crearDb({ 'consumptionAlerts/2026-11-19': { lecturas: { valor: 450000 } } });
    const emitir = jest.fn();
    const medicion = medicionHabitual({ medirConteoDelDia: jest.fn(async ({ consumo, dia }) => [minuto(dia, '11:00', consumo === 'lecturas' ? 200001 : 10)]) });

    const r = await vigilarConsumo({ db, token: 't', ahora: col('2026-11-20', '11:15'), medicion, emitir });

    expect(r.avisos).toEqual(['lecturas']);
    expect(db.docs.get('consumptionAlerts/2026-11-20').lecturas.valor).toBe(200001);
  });

  test('QA-07: tras los avisos del D1, lecturas, escrituras e IA bajos el D2 no envían aviso de vuelta a la normalidad', async () => {
    const db = crearDb({
      'consumptionAlerts/2026-11-19': { lecturas: { valor: 450000 }, escrituras: { valor: 180000 }, ia: { usuarios: ['uid-inversor-pro-01'], analisis: ['uid-inversor-pro-01/p2'], avisos: 1 } },
    });
    const emitir = jest.fn();
    const medicion = medicionHabitual({
      medirConteoDelDia: jest.fn(async ({ consumo, dia }) => [minuto(dia, '10:00', consumo === 'lecturas' ? 9800 : 520)]),
      leerUsuariosIa: jest.fn(async () => [{
        uid: 'uid-inversor-pro-01',
        conAcceso: true,
        analisis: [{ id: 'p1', createdAt: col('2026-11-14', '18:30').toISOString() }, { id: 'p2', createdAt: col('2026-11-19', '09:00').toISOString() }],
      }]),
    });

    const r = await vigilarConsumo({ db, token: 't', ahora: col('2026-11-20', '10:45'), medicion, emitir });

    expect(emitir).not.toHaveBeenCalled();
    expect(r.avisos).toEqual([]);
    expect(db.sucesos).toEqual([]);
  });

  test('en la primera media hora del día se cierra el anterior: un cruce a las 23:50 que la ejecución de las 23:45 no vio avisa a las 00:15', async () => {
    const db = crearDb({ 'consumptionAlerts/2026-11-19': {} });
    const emitir = jest.fn();
    const medicion = medicionHabitual({
      medirConteoDelDia: jest.fn(async ({ consumo, dia }) => (dia === '2026-11-19' && consumo === 'lecturas'
        ? [minuto(dia, '18:00', 199000), minuto(dia, '23:50', 1500)]
        : [minuto(dia, '00:05', 10)])),
    });

    const r = await vigilarConsumo({ db, token: 't', ahora: col('2026-11-20', '00:15'), medicion, emitir });

    expect(r.avisos).toEqual(['lecturas']);
    expect(lineaDe(emitir).desde).toBe('2026-11-19 23:50 (hora de Colombia)');
    expect(db.docs.get('consumptionAlerts/2026-11-19').lecturas.valor).toBe(200500);
    expect(r.medidos.lecturas).toBe(10);

    // A las 00:45 ya no mira el día anterior
    medicion.medirConteoDelDia.mockClear();
    await vigilarConsumo({ db, token: 't', ahora: col('2026-11-20', '00:45'), medicion, emitir });
    expect(medicion.medirConteoDelDia.mock.calls.map(([a]) => a.dia)).toEqual(['2026-11-20', '2026-11-20']);
  });

  test('QA-06: el almacenamiento vuelve por debajo y llega la normalización', async () => {
    const db = crearDb({ 'consumptionAlerts/estado': { almacenamiento: { porEncima: true, ultimoCruceDia: '2026-11-19' } } });
    const emitir = jest.fn();
    const medicion = medicionHabitual({ medirAlmacenamiento: jest.fn(async () => [almacenamiento('2026-11-19', '15:59', 0.81), almacenamiento('2026-11-19', '16:00', 0.79)]) });

    const r = await vigilarConsumo({ db, token: 't', ahora: col('2026-11-19', '16:15'), medicion, emitir });

    expect(r.avisos).toEqual(['almacenamiento-normal']);
    expect(db.docs.get('consumptionAlerts/estado').almacenamiento).toMatchObject({ porEncima: false, ultimaNormalDia: '2026-11-19' });
  });

  test('QA-14: los análisis de IA fuera de cupo avisan una vez y se guarda la lista completa; ayer solo se lee si hay candidatos', async () => {
    const db = crearDb({ 'consumptionAlerts/2026-12-11': { ia: { analisis: [] } } });
    const emitir = jest.fn();
    const medicion = medicionHabitual({
      leerUsuariosIa: jest.fn(async () => [
        { uid: 'uid-inversor-free-01', conAcceso: false, analisis: [{ id: 'f1', createdAt: col('2026-12-12', '10:00').toISOString() }] },
      ]),
    });

    const r = await vigilarConsumo({ db, token: 't', ahora: col('2026-12-12', '10:15'), medicion, emitir });

    expect(r.avisos).toEqual(['ia']);
    expect(emitir.mock.calls[0][0].usuarios).toEqual([{ uid: 'uid-inversor-free-01', conAcceso: false, analisisSieteDias: 1 }]);
    expect(db.docs.get('consumptionAlerts/2026-12-12').ia).toEqual({ usuarios: ['uid-inversor-free-01'], analisis: ['uid-inversor-free-01/f1'], avisos: 1 });

    // Siguiente ejecución: el mismo análisis no repite el aviso
    emitir.mockClear();
    await vigilarConsumo({ db, token: 't', ahora: col('2026-12-12', '10:30'), medicion, emitir });
    expect(emitir).not.toHaveBeenCalled();
  });

  test('Vacío 6: si Monitoring falla con las lecturas, no marca nada de lecturas y sigue con lo demás', async () => {
    const db = crearDb();
    const emitir = jest.fn();
    const medicion = medicionHabitual({
      medirConteoDelDia: jest.fn(async ({ consumo, dia }) => {
        if (consumo === 'lecturas') throw new Error('Monitoring → 503');
        return [minuto(dia, '09:10', 100001)];
      }),
    });

    const r = await vigilarConsumo({ db, token: 't', ahora: col('2026-11-19', '09:30'), medicion, emitir });

    expect(r.fallos).toEqual(['lecturas']);
    expect(r.avisos).toEqual(['escrituras']);
    expect(db.docs.get('consumptionAlerts/2026-11-19').lecturas).toBeUndefined();
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining('[consumptionWatch] No se pudo vigilar lecturas del 2026-11-19: Monitoring → 503'));
  });

  test('si guardar el estado falla después del log, el aviso se repite en la ejecución siguiente (nunca se pierde)', async () => {
    const db = crearDb();
    const original = db.collection;
    let fallar = true;
    db.collection = (nombre) => {
      const col_ = original(nombre);
      return {
        doc: (id) => {
          const ref = col_.doc(id);
          return { ...ref, set: async (...a) => { if (fallar) throw new Error('Firestore caído'); return ref.set(...a); } };
        },
      };
    };
    const emitir = jest.fn();
    const medicion = medicionHabitual({ medirConteoDelDia: jest.fn(async ({ consumo, dia }) => [minuto(dia, '14:20', consumo === 'lecturas' ? 200001 : 1)]) });

    await vigilarConsumo({ db, token: 't', ahora: col('2026-11-19', '14:30'), medicion, emitir });
    fallar = false;
    await vigilarConsumo({ db, token: 't', ahora: col('2026-11-19', '14:45'), medicion, emitir });
    await vigilarConsumo({ db, token: 't', ahora: col('2026-11-19', '15:00'), medicion, emitir });

    expect(emitir).toHaveBeenCalledTimes(2);
  });
});
