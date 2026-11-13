/**
 * HU 6.8.2 — El estado de cada mañana y la definición de atrasado
 * (Escenarios 4 y 5, RN-6.8.2-K, RN-6.8.2-L).
 *
 * `calcularEstadoNoche` es puro: se prueba con la cola tal como queda al
 * terminar la corrida. `publicarEstadoNoche`, con un doble de Firestore.
 *
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/qa.md
 */

const {
  calcularEstadoNoche,
  publicarEstadoNoche,
  porEspera,
  esperaDe,
  PREFIJO_ALERTA,
  COLECCION_NOCHES,
  MAX_NOMBRADOS,
} = require('../reconcileNightReportService');

// N1 = 2026-11-13 02:00 ET (07:00 UTC); su último día cerrado es el 2026-11-12
const INICIO = Date.parse('2026-11-13T07:00:00.000Z');
const FIN = INICIO + 420 * 1000;

const enCola = (id, since, pendienteDesde, extra = {}) => ({
  id,
  stale: { since, pendienteDesde: `${pendienteDesde}T12:00:00.000Z`, registeredAt: `${pendienteDesde}T12:00:00.000Z`, ...extra },
});

const estadoDe = (overrides = {}) => calcularEstadoNoche({
  noche: '2026-11-13',
  inicioMs: INICIO,
  finMs: FIN,
  lastClosedDate: '2026-11-12',
  totals: { tramos: 6, rounds: 2, reconciled: 2, advanced: 1, failed: 1 },
  atendidos: new Map(),
  colaFinal: [],
  revisionesNuevas: [],
  totalEnRevision: 0,
  tiempoAgotado: true,
  ...overrides,
});

describe('calcularEstadoNoche — quién está atrasado (RN-6.8.2-K)', () => {
  test('QA-08: una corta sin procesar, la del límite de 6 meses y una larga sin avance son atrasadas, con su fecha', () => {
    const estado = estadoDe({
      colaFinal: [
        enCola('inv-c', '2026-10-20', '2026-11-10'),
        enCola('inv-d', '2026-05-12', '2026-11-12'),
        enCola('inv-a', '2024-08-05', '2026-11-05'),
      ],
      atendidos: new Map([['inv-a', { sinceInicial: '2024-08-05', avanzo: false }]]),
    });

    expect(estado.atrasados.total).toBe(3);
    expect(estado.atrasados.masAntiguoDesde).toBe('2026-11-05T12:00:00.000Z');
    expect(estado.atrasados.lista.map((p) => [p.userId, p.tipo, p.pendienteDesde])).toEqual([
      ['inv-a', 'larga', '2026-11-05T12:00:00.000Z'],
      ['inv-c', 'corta', '2026-11-10T12:00:00.000Z'],
      ['inv-d', 'corta', '2026-11-12T12:00:00.000Z'],
    ]);
  });

  test('QA-09: una larga que avanzó un tramo (también la de 6 meses y 1 día) no está atrasada', () => {
    const estado = estadoDe({
      colaFinal: [
        enCola('inv-a', '2024-11-04', '2026-11-05'),
        enCola('inv-f', '2026-08-10', '2026-11-12'),
      ],
      atendidos: new Map([
        ['inv-a', { sinceInicial: '2024-08-05', avanzo: true }],
        ['inv-f', { sinceInicial: '2026-05-11', avanzo: true }],
      ]),
    });

    expect(estado.atrasados.total).toBe(0);
    expect(estado.pendientes.total).toBe(2);
    expect(estado.pendientes.lista.map((p) => [p.userId, p.tipo])).toEqual([['inv-a', 'larga'], ['inv-f', 'larga']]);
  });

  test('corta o larga se mide con el since del INICIO de la noche: una larga que avanzó no se vuelve corta', () => {
    // Su since actual ya está dentro de los 6 meses, pero empezó la noche en 2026-05-11
    const estado = estadoDe({
      colaFinal: [enCola('inv-f', '2026-08-10', '2026-11-12')],
      atendidos: new Map([['inv-f', { sinceInicial: '2026-05-11', avanzo: true }]]),
    });
    expect(estado.pendientes.lista[0].tipo).toBe('larga');
    expect(estado.atrasados.total).toBe(0);
  });

  test('una corta que avanzó pero no terminó SÍ está atrasada: no quedó lista a la mañana siguiente', () => {
    const estado = estadoDe({
      colaFinal: [enCola('inv-d', '2026-08-10', '2026-11-12')],
      atendidos: new Map([['inv-d', { sinceInicial: '2026-05-12', avanzo: true }]]),
    });
    expect(estado.atrasados.lista).toEqual([expect.objectContaining({ userId: 'inv-d', tipo: 'corta', avanzo: true })]);
  });

  test('el que quedó pendiente durante la corrida no está atrasado: nadie podía tenerlo listo esa mañana', () => {
    const estado = estadoDe({
      colaFinal: [{ id: 'inv-n', stale: { since: '2026-11-12', pendienteDesde: '2026-11-13T07:03:00.000Z' } }],
    });
    expect(estado.pendientes.total).toBe(1);
    expect(estado.atrasados.total).toBe(0);
  });

  test('una marca anterior a la HU, sin pendienteDesde, espera desde su registeredAt', () => {
    const vieja = { id: 'inv-v', stale: { since: '2026-10-01', registeredAt: '2026-11-08T12:00:00.000Z' } };
    expect(esperaDe(vieja.stale)).toBe('2026-11-08T12:00:00.000Z');
    expect(estadoDe({ colaFinal: [vieja] }).atrasados.lista[0]).toMatchObject({ userId: 'inv-v', pendienteDesde: '2026-11-08T12:00:00.000Z' });
  });

  test('los que están en revisión no cuentan como pendientes ni atrasados: figuran aparte (vacío 3)', () => {
    const estado = estadoDe({
      colaFinal: [],
      revisionesNuevas: [{ userId: 'inv-g', intentos: [{ motivo: 'error', noche: '2026-11-13' }], pendienteDesde: '2026-11-10T12:00:00.000Z' }],
      totalEnRevision: 2,
    });
    expect(estado.pendientes.total).toBe(0);
    expect(estado.atrasados.total).toBe(0);
    expect(estado.revision).toMatchObject({ totalNuevos: 1, totalEnRevision: 2 });
  });

  test(`con más de ${MAX_NOMBRADOS} pendientes nombra los ${MAX_NOMBRADOS} que más esperan y los recuentos son exactos`, () => {
    const colaFinal = Array.from({ length: 250 }, (_, i) => ({
      id: `inv-${String(i).padStart(3, '0')}`,
      stale: { since: '2026-10-01', pendienteDesde: new Date(INICIO - (300 - i) * 60000).toISOString() },
    }));
    const estado = estadoDe({ colaFinal });
    expect(estado.pendientes.total).toBe(250);
    expect(estado.atrasados.total).toBe(250);
    expect(estado.pendientes.lista).toHaveLength(MAX_NOMBRADOS);
    expect(estado.pendientes.lista[0].userId).toBe('inv-000');
    expect(estado.truncado).toBe(true);
  });
});

describe('porEspera — el orden de la cola (RN-6.8-P)', () => {
  test('primero el que más espera; a igual espera, el uid; sin fecha, al final', () => {
    const cola = [
      { id: 'inv-f', stale: { pendienteDesde: '2026-11-12T21:40:00.000Z' } },
      { id: 'inv-z', stale: {} },
      { id: 'inv-d', stale: { pendienteDesde: '2026-11-12T14:15:00.000Z' } },
      { id: 'inv-c', stale: { pendienteDesde: '2026-11-10T12:00:00.000Z' } },
      { id: 'inv-b', stale: { pendienteDesde: '2026-11-10T12:00:00.000Z' } },
    ];
    expect(cola.sort(porEspera).map((e) => e.id)).toEqual(['inv-b', 'inv-c', 'inv-d', 'inv-f', 'inv-z']);
  });
});

describe('publicarEstadoNoche — el estado consta y el atraso llega a operación (Escenarios 4 y 5)', () => {
  let escritos;
  let errores;
  let registros;
  const dbDoble = ({ falla = false } = {}) => ({
    collection: (nombre) => ({
      doc: (id) => ({
        set: async (datos) => {
          if (falla) throw new Error('UNAVAILABLE');
          escritos.push({ ruta: `${nombre}/${id}`, datos });
        },
      }),
    }),
  });

  beforeEach(() => {
    escritos = [];
    errores = [];
    registros = [];
    jest.spyOn(console, 'error').mockImplementation((m) => errores.push(String(m)));
    jest.spyOn(console, 'log').mockImplementation((m) => registros.push(String(m)));
  });
  afterEach(() => jest.restoreAllMocks());

  test('QA-08: con atrasados, guarda el estado y emite el aviso con cuántos y desde cuándo', async () => {
    const estado = estadoDe({
      colaFinal: [enCola('inv-c', '2026-10-20', '2026-11-10'), enCola('inv-a', '2024-08-05', '2026-11-05')],
    });

    const { docId, avisado } = await publicarEstadoNoche(dbDoble(), estado);

    expect(docId).toBe('2026-11-13_020000');
    expect(avisado).toBe(true);
    expect(escritos).toEqual([{ ruta: `${COLECCION_NOCHES}/2026-11-13_020000`, datos: estado }]);
    expect(errores).toEqual([
      `${PREFIJO_ALERTA} 2026-11-13: 2 inversor(es) atrasado(s), el más antiguo pendiente desde 2026-11-05; ` +
      `0 pasaron a revisión. No se amplió la corrección. Detalle en ${COLECCION_NOCHES}/2026-11-13_020000`,
    ]);
    expect(registros[0]).toMatch(/^\[reconcile\] ESTADO 2026-11-13: 2 corregidos, 2 pendientes \(el más antiguo desde 2026-11-05\), 2 atrasados/);
  });

  test('QA-09 y QA-13: sin atrasados ni revisiones nuevas, el estado consta y no hay aviso', async () => {
    const { avisado } = await publicarEstadoNoche(dbDoble(), estadoDe());

    expect(avisado).toBe(false);
    expect(escritos).toHaveLength(1);
    expect(errores).toEqual([]);
    expect(registros[0]).toMatch(/0 pendientes \(el más antiguo desde —\), 0 atrasados, 0 pasaron a revisión/);
  });

  test('si el estado no se puede guardar, el aviso sale igual (RN-6-E)', async () => {
    const estado = estadoDe({ colaFinal: [enCola('inv-c', '2026-10-20', '2026-11-10')] });

    const { avisado } = await publicarEstadoNoche(dbDoble({ falla: true }), estado);

    expect(avisado).toBe(true);
    expect(errores[0]).toMatch(/No se pudo guardar el estado de la noche 2026-11-13_020000: UNAVAILABLE/);
    expect(errores[1]).toMatch(new RegExp(`^\\${PREFIJO_ALERTA} 2026-11-13: 1 inversor\\(es\\) atrasado\\(s\\).*Detalle en no se pudo guardar el detalle$`));
  });
});
