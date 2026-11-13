/**
 * HU 6.8.2 — Un tramo que se queda sin memoria o sin tiempo no tumba al proceso
 * que lo lanzó (Escenario 1, vacío 7).
 *
 * Con workers reales: el tope de memoria de un worker solo se puede probar
 * dejando que V8 lo alcance.
 *
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T1, T12)
 */

const path = require('path');
const { crearEjecutorAislado, crearEjecutorEnProceso } = require('../ejecutorAislado');

jest.setTimeout(60000);

const SCRIPT = path.join(__dirname, 'fixtures', 'workerDePrueba.js');

const crear = (overrides = {}) => crearEjecutorAislado({
  script: SCRIPT,
  maxMemoriaMb: 32,
  tiempoMaxMs: 5000,
  etiqueta: 'test',
  ...overrides,
});

describe('crearEjecutorAislado — cada tarea en un worker con topes propios (HU 6.8.2, Escenario 1)', () => {
  let ejecutor;
  beforeEach(() => jest.spyOn(console, 'error').mockImplementation(() => {}));
  afterEach(async () => {
    await ejecutor.cerrar();
    jest.restoreAllMocks();
  });

  test('una tarea que termina devuelve su resultado, y el worker atiende la siguiente sin volver a arrancar', async () => {
    ejecutor = crear();

    expect(await ejecutor.ejecutar({ accion: 'doblar', valor: 21 })).toEqual({ ok: true, resultado: { doble: 42, atendidas: 1 } });
    expect(await ejecutor.ejecutar({ accion: 'doblar', valor: 5 })).toEqual({ ok: true, resultado: { doble: 10, atendidas: 2 } });
  });

  test('sin memoria: el motivo es "memoria", el proceso principal sigue y la tarea siguiente corre en un worker nuevo', async () => {
    ejecutor = crear({ maxMemoriaMb: 16 });

    const salida = await ejecutor.ejecutar({ accion: 'agotarMemoria' });

    expect(salida).toEqual({ ok: false, motivo: 'memoria', detalle: 'superó el tope de 16 MB de memoria' });
    // Worker nuevo: la cuenta de tareas empieza otra vez
    expect(await ejecutor.ejecutar({ accion: 'doblar', valor: 1 })).toEqual({ ok: true, resultado: { doble: 2, atendidas: 1 } });
  });

  test('sin tiempo: el motivo es "tiempo", el worker se termina y la tarea siguiente corre en uno nuevo', async () => {
    ejecutor = crear({ tiempoMaxMs: 300 });

    const inicio = Date.now();
    const salida = await ejecutor.ejecutar({ accion: 'colgarse' });

    expect(salida).toEqual({ ok: false, motivo: 'tiempo', detalle: 'superó el tope de 0.3 s' });
    expect(Date.now() - inicio).toBeLessThan(3000);
    expect(await ejecutor.ejecutar({ accion: 'doblar', valor: 3 })).toEqual({ ok: true, resultado: { doble: 6, atendidas: 1 } });
  });

  test('con error: el motivo es "error" con su mensaje, y el mismo worker sigue atendiendo', async () => {
    ejecutor = crear();

    expect(await ejecutor.ejecutar({ accion: 'fallar', valor: 'API caída' }))
      .toEqual({ ok: false, motivo: 'error', detalle: 'fallo pedido: API caída' });
    expect(await ejecutor.ejecutar({ accion: 'doblar', valor: 4 })).toEqual({ ok: true, resultado: { doble: 8, atendidas: 2 } });
  });

  test('un script de worker que no carga es un "error", no tumba al principal', async () => {
    ejecutor = crear({ script: path.join(__dirname, 'fixtures', 'noExiste.js') });

    const salida = await ejecutor.ejecutar({ accion: 'doblar', valor: 1 });

    expect(salida).toMatchObject({ ok: false, motivo: 'error' });
  });
});

describe('crearEjecutorEnProceso — el mismo contrato, sin aislar', () => {
  test('devuelve el resultado o el error con motivo "error"', async () => {
    const ejecutor = crearEjecutorEnProceso(async ({ valor }) => {
      if (valor < 0) throw new Error('negativo');
      return valor + 1;
    });

    expect(await ejecutor.ejecutar({ valor: 1 })).toEqual({ ok: true, resultado: 2 });
    expect(await ejecutor.ejecutar({ valor: -1 })).toEqual({ ok: false, motivo: 'error', detalle: 'negativo' });
  });
});
