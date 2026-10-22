/**
 * HU 6.3 — La cuenta de quién quedó procesado y quién no.
 *
 * Defiende el Escenario 1 (queda constancia de cuántos se procesaron), el
 * Escenario 4 (cuántos y cuáles quedaron fuera) y RN-6.3-L / Escenario 6
 * (un reintento no duplica ni altera lo ya hecho).
 *
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T5, T8, T9)
 */

jest.mock('firebase-admin/firestore', () => ({
  FieldValue: {
    // El incremento se representa como una marca que el Firestore de mentira
    // resuelve al escribir: así el test comprueba el CONTADOR, no la llamada.
    increment: (n) => ({ __increment: n }),
  },
}));

const progreso = require('../eodProgressService');

// ============================================================================
// Fábricas locales
// ============================================================================

/**
 * Firestore en memoria con `set({merge})`, `runTransaction` y consultas simples
 * sobre una subcolección. Resuelve los `FieldValue.increment` para que los
 * contadores se puedan afirmar con valores concretos.
 */
const crearDb = () => {
  const docs = new Map();

  const aplicar = (ruta, data, merge) => {
    const previo = merge ? (docs.get(ruta) || {}) : {};
    const nuevo = { ...previo };
    for (const [clave, valor] of Object.entries(data)) {
      if (valor && typeof valor === 'object' && '__increment' in valor) {
        nuevo[clave] = (previo[clave] || 0) + valor.__increment;
      } else {
        nuevo[clave] = valor;
      }
    }
    docs.set(ruta, nuevo);
  };

  const crearColeccion = (prefijo) => {
    const query = (filtros, limite) => ({
      where: (campo, _op, valor) => query([...filtros, [campo, valor]], limite),
      limit: (n) => query(filtros, n),
      get: async () => {
        let hijos = [...docs.entries()]
          .filter(([ruta]) => ruta.startsWith(`${prefijo}/`) && !ruta.slice(prefijo.length + 1).includes('/'))
          .map(([ruta, data]) => ({ id: ruta.split('/').pop(), data: () => data }));
        for (const [campo, valor] of filtros) {
          hijos = hijos.filter((d) => d.data()[campo] === valor);
        }
        if (limite) hijos = hijos.slice(0, limite);
        return { docs: hijos, size: hijos.length, empty: hijos.length === 0 };
      },
    });

    return {
      doc: (id) => crearDoc(`${prefijo}/${id}`),
      ...query([], null),
    };
  };

  const crearDoc = (ruta) => ({
    ruta,
    set: async (data, opciones = {}) => aplicar(ruta, data, !!opciones.merge),
    get: async () => ({ exists: docs.has(ruta), data: () => docs.get(ruta), id: ruta.split('/').pop() }),
    collection: (nombre) => crearColeccion(`${ruta}/${nombre}`),
  });

  return {
    collection: (nombre) => crearColeccion(nombre),
    runTransaction: async (fn) => fn({
      get: async (ref) => ref.get(),
      set: (ref, data, opciones = {}) => aplicar(ref.ruta, data, !!opciones.merge),
    }),
    _docs: docs,
  };
};

const FECHA = '2026-10-20';

// ============================================================================

describe('eodProgressService — la constancia del cierre (HU 6.3, Escenario 1)', () => {
  it('al abrir el día queda escrito a cuántos inversores hay que llegar', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 3, userIds: ['u1', 'u2', 'u3'], runId: 'run-1' });

    const estado = await progreso.leerProgreso(db, FECHA);
    expect(estado.totalUsers).toBe(3);
    expect(estado.completedUsers).toBe(0);
    expect(estado.failedUsers).toBe(0);
    expect(estado.estado).toBe(progreso.ESTADO_DIA.EN_CURSO);
  });

  it('cada inversor terminado suma uno, y la cuenta final cuadra con el total', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 3, userIds: ['u1', 'u2', 'u3'] });

    await progreso.marcarCompletado(db, FECHA, 'u1');
    await progreso.marcarCompletado(db, FECHA, 'u2');
    await progreso.marcarCompletado(db, FECHA, 'u3');

    const estado = await progreso.leerProgreso(db, FECHA);
    expect(estado.completedUsers).toBe(3);
    expect(estado.completedUsers).toBe(estado.totalUsers);
  });
});

describe('eodProgressService — un reintento no duplica (HU 6.3, RN-6.3-L, Escenario 6)', () => {
  it('marcar dos veces al mismo inversor deja el contador en uno', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 1, userIds: ['u1'] });

    const primera = await progreso.marcarCompletado(db, FECHA, 'u1');
    const segunda = await progreso.marcarCompletado(db, FECHA, 'u1');

    expect(primera.contado).toBe(true);
    expect(segunda.contado).toBe(false);
    expect((await progreso.leerProgreso(db, FECHA)).completedUsers).toBe(1);
  });

  it('`yaProcesado` es la guarda: dice que sí sólo después de terminar', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 1, userIds: ['u1'] });

    expect(await progreso.yaProcesado(db, FECHA, 'u1')).toBe(false);
    await progreso.marcarCompletado(db, FECHA, 'u1');
    expect(await progreso.yaProcesado(db, FECHA, 'u1')).toBe(true);
  });

  it('un inversor fallido que después se recupera deja de contar como fuera', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 1, userIds: ['u1'] });

    await progreso.marcarFallido(db, FECHA, 'u1', 'timeout', 2);
    expect((await progreso.leerProgreso(db, FECHA)).failedUsers).toBe(1);

    await progreso.marcarCompletado(db, FECHA, 'u1');
    const estado = await progreso.leerProgreso(db, FECHA);
    expect(estado.failedUsers).toBe(0);
    expect(estado.completedUsers).toBe(1);
  });

  it('un fallo que llega tarde no degrada a quien ya había terminado', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 1, userIds: ['u1'] });

    await progreso.marcarCompletado(db, FECHA, 'u1');
    const resultado = await progreso.marcarFallido(db, FECHA, 'u1', 'entrega tardía', 2);

    expect(resultado.contado).toBe(false);
    const estado = await progreso.leerProgreso(db, FECHA);
    expect(estado.completedUsers).toBe(1);
    expect(estado.failedUsers).toBe(0);
    expect(await progreso.yaProcesado(db, FECHA, 'u1')).toBe(true);
  });

  it('dos fallos del mismo inversor no lo cuentan dos veces como fuera', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 1, userIds: ['u1'] });

    await progreso.marcarFallido(db, FECHA, 'u1', 'primero', 1);
    await progreso.marcarFallido(db, FECHA, 'u1', 'segundo', 2);

    expect((await progreso.leerProgreso(db, FECHA)).failedUsers).toBe(1);
  });
});

describe('eodProgressService — cuántos y cuáles quedaron fuera (HU 6.3, RN-6.3-N, Escenario 4)', () => {
  it('los fallidos se pueden nombrar, con su motivo', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 3, userIds: ['u1', 'u2', 'u3'] });

    await progreso.marcarCompletado(db, FECHA, 'u1');
    await progreso.marcarFallido(db, FECHA, 'u2', 'DEADLINE_EXCEEDED al leer transacciones', 2);

    const fallidos = await progreso.listarFallidos(db, FECHA);
    expect(fallidos).toHaveLength(1);
    expect(fallidos[0].userId).toBe('u2');
    expect(fallidos[0].error).toBe('DEADLINE_EXCEEDED al leer transacciones');
    expect(fallidos[0].intentos).toBe(3);
  });

  it('el que nunca llegó a ejecutarse también se detecta: no basta con mirar los fallos', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 3, userIds: ['u1', 'u2', 'u3'] });

    await progreso.marcarCompletado(db, FECHA, 'u1');
    await progreso.marcarFallido(db, FECHA, 'u2', 'error', 2);
    // u3 nunca se encoló: no tiene ni documento.

    const sinRastro = await progreso.listarSinRastro(db, FECHA, ['u1', 'u2', 'u3']);
    expect(sinRastro).toEqual(['u3']);
  });

  it('el error se recorta para que un stack trace no reviente el documento', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 1, userIds: ['u1'] });

    await progreso.marcarFallido(db, FECHA, 'u1', 'x'.repeat(5000), 2);

    const fallidos = await progreso.listarFallidos(db, FECHA);
    expect(fallidos[0].error).toHaveLength(500);
  });

  it('cerrar el día deja su veredicto por escrito', async () => {
    const db = crearDb();
    await progreso.iniciarProgreso(db, FECHA, { totalUsers: 1, userIds: ['u1'] });
    await progreso.cerrarProgreso(db, FECHA, progreso.ESTADO_DIA.INCOMPLETO);

    const estado = await progreso.leerProgreso(db, FECHA);
    expect(estado.estado).toBe(progreso.ESTADO_DIA.INCOMPLETO);
    expect(estado.finishedAt).not.toBeNull();
  });
});
