/**
 * El tope de gasto de la plataforma.
 *
 * No es el cupo del usuario: aquel reparte un derecho entre quienes pagan,
 * éste protege el saldo prepago del motor.
 *
 * @see services/copilot/copilotSpendGuard.js
 */

const firestoreFalso = {};

jest.mock('../../firebaseAdmin', () => ({
  firestore: () => firestoreFalso,
}));

const guard = require('../copilotSpendGuard');

/** Las 14:00 de Nueva York. */
const AHORA = new Date('2026-10-13T18:00:00.000Z');

/**
 * Firestore con el contador del día en el valor dado.
 *
 * @param {number|null} usados - `null` si el documento no existe.
 * @returns {Array} Lo que se va escribiendo.
 */
function montar(usados) {
  const escrituras = [];
  const ref = {};
  firestoreFalso.collection = () => ({ doc: () => ref });
  firestoreFalso.runTransaction = async (fn) => fn({
    get: async () => ({ exists: usados !== null, data: () => ({ used: usados }) }),
    set: (destino, datos, opciones) => escrituras.push({ datos, opciones }),
  });
  return escrituras;
}

describe('reservar — aparta un análisis del presupuesto de hoy', () => {
  it('el primero del día abre el contador', async () => {
    const escrituras = montar(null);

    const { concedido, usados } = await guard.reservar(AHORA);

    expect(concedido).toBe(true);
    expect(usados).toBe(1);
    expect(escrituras[0].datos.used).toBe(1);
  });

  it('con presupuesto de sobra, concede y suma', async () => {
    // Uno menos del tope, sea cual sea: el numero lo mueve el PO.
    const usadosPrevios = guard.ANALISIS_POR_DIA_PLATAFORMA - 1;
    const escrituras = montar(usadosPrevios);

    const { concedido, usados } = await guard.reservar(AHORA);

    expect(concedido).toBe(true);
    expect(usados).toBe(usadosPrevios + 1);
    expect(escrituras[0].datos.used).toBe(usadosPrevios + 1);
  });

  it('alcanzado el tope, deniega y NO suma', async () => {
    const escrituras = montar(guard.ANALISIS_POR_DIA_PLATAFORMA);

    const { concedido } = await guard.reservar(AHORA);

    expect(concedido).toBe(false);
    expect(escrituras).toHaveLength(0);
  });

  it('reserva dentro de una transacción', async () => {
    // Dos lanzamientos a la vez leerían el mismo contador y lo dejarían en uno:
    // justo el caso que el tope existe para evitar.
    let usoTransaccion = false;
    firestoreFalso.collection = () => ({ doc: () => ({}) });
    firestoreFalso.runTransaction = async (fn) => {
      usoTransaccion = true;
      return fn({ get: async () => ({ exists: false, data: () => ({}) }), set: () => {} });
    };

    await guard.reservar(AHORA);

    expect(usoTransaccion).toBe(true);
  });

  it('si Firestore falla, deja pasar', async () => {
    // Un guardia de gasto que tumba el producto cuando falla la base cuesta
    // más de lo que ahorra; el cupo por usuario sigue acotando mientras tanto.
    firestoreFalso.collection = () => { throw new Error('sin conexión'); };

    expect((await guard.reservar(AHORA)).concedido).toBe(true);
  });

  it('el día es el del mercado, no el de UTC', async () => {
    // 21:00 de Nueva York del 13 son las 01:00 UTC del 14. En UTC, el contador
    // se reiniciaría a mitad de la tarde de Nueva York.
    expect(guard.dia(new Date('2026-10-14T01:00:00.000Z'))).toBe('2026-10-13');
  });
});

describe('usadosHoy — consultar sin reservar', () => {
  it('devuelve el contador del día', async () => {
    firestoreFalso.collection = () => ({
      doc: () => ({ get: async () => ({ exists: true, data: () => ({ used: 7 }) }) }),
    });

    expect(await guard.usadosHoy(AHORA)).toBe(7);
  });

  it('sin contador, cero', async () => {
    firestoreFalso.collection = () => ({
      doc: () => ({ get: async () => ({ exists: false, data: () => ({}) }) }),
    });

    expect(await guard.usadosHoy(AHORA)).toBe(0);
  });

  it('no reserva nada: sólo mira', async () => {
    let reservo = false;
    firestoreFalso.runTransaction = async () => { reservo = true; };
    firestoreFalso.collection = () => ({
      doc: () => ({ get: async () => ({ exists: true, data: () => ({ used: 2 }) }) }),
    });

    await guard.usadosHoy(AHORA);

    expect(reservo).toBe(false);
  });
});
