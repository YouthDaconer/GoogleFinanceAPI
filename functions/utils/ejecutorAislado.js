/**
 * HU 6.8.2 — Un trabajo que se queda sin memoria o sin tiempo no tumba al
 * proceso que lo lanzó.
 *
 * La corrección nocturna es UNA ejecución por noche (RN-6.8-N). Hasta ahora,
 * si el tramo de un inversor agotaba la memoria, moría el proceso entero: el
 * intento no se contaba y nadie detrás de él se corregía esa noche (noches del
 * 07, 11 y 12 de noviembre de 2026). El Escenario 1 pide lo contrario: que los
 * demás se procesen **esa misma noche**.
 *
 * Por eso cada tarea corre en un `worker_thread`:
 * - con su propio tope de memoria (`resourceLimits.maxOldGenerationSizeMb`):
 *   al pasarlo, muere el worker con `ERR_WORKER_OUT_OF_MEMORY` y el proceso
 *   principal sigue;
 * - con su propio tope de tiempo: al vencer, el worker se termina desde fuera
 *   (`terminate()`), que es la única forma de cancelar de verdad un trabajo en
 *   curso en Node.
 *
 * El worker es **de larga vida**: atiende una tarea tras otra y conserva sus
 * cachés (los precios de la noche). Si muere, la tarea siguiente arranca uno
 * nuevo.
 *
 * Protocolo: el principal envía `{ id, payload }`; el worker responde
 * `{ id, ok: true, resultado }` o `{ id, ok: false, error }`. El lado del
 * worker es `atenderTareas`.
 *
 * @module utils/ejecutorAislado
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T1)
 */

const { Worker, parentPort } = require('worker_threads');

/** Código con el que Node termina un worker que pasó su tope de memoria. */
const CODIGO_SIN_MEMORIA = 'ERR_WORKER_OUT_OF_MEMORY';

/**
 * @typedef {Object} SalidaTarea
 * @property {boolean} ok
 * @property {*} [resultado] - Lo que devolvió la tarea (solo con `ok`)
 * @property {'memoria'|'tiempo'|'error'} [motivo] - Por qué no terminó (sin `ok`)
 * @property {string} [detalle]
 */

/**
 * Crea un ejecutor que corre cada tarea en un worker aislado.
 *
 * @param {Object} opciones
 * @param {string} opciones.script - Ruta absoluta del punto de entrada del worker
 * @param {number} opciones.maxMemoriaMb - Tope del montón del worker
 * @param {number} opciones.tiempoMaxMs - Tope de cada tarea
 * @param {string} [opciones.etiqueta='ejecutorAislado'] - Prefijo de los logs
 * @returns {{ ejecutar: (payload: *) => Promise<SalidaTarea>, cerrar: () => Promise<void> }}
 */
function crearEjecutorAislado({ script, maxMemoriaMb, tiempoMaxMs, etiqueta = 'ejecutorAislado' }) {
  let worker = null;
  let secuencia = 0;

  const arrancar = () => {
    const nuevo = new Worker(script, { resourceLimits: { maxOldGenerationSizeMb: maxMemoriaMb } });
    // Un 'error' sin oyente tumbaría al proceso principal, que es justo lo
    // que este módulo existe para evitar. Este oyente vive tanto como el worker.
    nuevo.on('error', (error) => {
      console.error(`[${etiqueta}] el worker terminó con error: ${error.code || ''} ${error.message}`);
    });
    nuevo.on('exit', () => {
      if (worker === nuevo) worker = null;
    });
    worker = nuevo;
    return nuevo;
  };

  /**
   * @param {*} payload - Debe poder clonarse (structured clone)
   * @returns {Promise<SalidaTarea>}
   */
  const ejecutar = (payload) => new Promise((resolve) => {
    const w = worker || arrancar();
    const id = ++secuencia;
    let terminada = false;

    const terminar = (salida, { descartarWorker = false } = {}) => {
      if (terminada) return;
      terminada = true;
      clearTimeout(reloj);
      w.off('message', alMensaje);
      w.off('error', alError);
      w.off('exit', alSalir);
      if (descartarWorker && worker === w) worker = null;
      resolve(salida);
    };

    const alMensaje = (mensaje) => {
      if (!mensaje || mensaje.id !== id) return;
      if (mensaje.ok) terminar({ ok: true, resultado: mensaje.resultado });
      else terminar({ ok: false, motivo: 'error', detalle: mensaje.error || 'sin detalle' });
    };
    const alError = (error) => {
      const sinMemoria = error && error.code === CODIGO_SIN_MEMORIA;
      terminar({
        ok: false,
        motivo: sinMemoria ? 'memoria' : 'error',
        detalle: sinMemoria ? `superó el tope de ${maxMemoriaMb} MB de memoria` : (error && error.message) || 'sin detalle',
      }, { descartarWorker: true });
    };
    const alSalir = (codigo) => {
      terminar({ ok: false, motivo: 'error', detalle: `el worker se cerró (código ${codigo}) sin responder` },
        { descartarWorker: true });
    };

    const reloj = setTimeout(() => {
      terminar({ ok: false, motivo: 'tiempo', detalle: `superó el tope de ${tiempoMaxMs / 1000} s` },
        { descartarWorker: true });
      w.terminate().catch((error) => console.error(`[${etiqueta}] no se pudo terminar el worker: ${error.message}`));
    }, tiempoMaxMs);

    w.on('message', alMensaje);
    w.on('error', alError);
    w.on('exit', alSalir);
    w.postMessage({ id, payload });
  });

  const cerrar = async () => {
    const w = worker;
    worker = null;
    if (w) await w.terminate();
  };

  return { ejecutar, cerrar };
}

/**
 * Mismo contrato que `crearEjecutorAislado`, sin aislar: la tarea corre en el
 * propio proceso. Lo usan los tests (donde los mocks de Jest no llegan a un
 * worker) y quien no necesita topes.
 *
 * @param {(payload: *) => Promise<*>} tarea
 * @returns {{ ejecutar: (payload: *) => Promise<SalidaTarea>, cerrar: () => Promise<void> }}
 */
function crearEjecutorEnProceso(tarea) {
  return {
    ejecutar: async (payload) => {
      try {
        return { ok: true, resultado: await tarea(payload) };
      } catch (error) {
        return { ok: false, motivo: 'error', detalle: error.message };
      }
    },
    cerrar: async () => {},
  };
}

/**
 * Lado del worker: atiende las tareas que envía el principal, una a una.
 *
 * @param {(payload: *) => Promise<*>} tarea
 */
function atenderTareas(tarea) {
  if (!parentPort) throw new Error('atenderTareas solo se usa dentro de un worker');
  parentPort.on('message', async ({ id, payload }) => {
    try {
      parentPort.postMessage({ id, ok: true, resultado: await tarea(payload) });
    } catch (error) {
      parentPort.postMessage({ id, ok: false, error: error.message });
    }
  });
}

module.exports = {
  crearEjecutorAislado,
  crearEjecutorEnProceso,
  atenderTareas,
  CODIGO_SIN_MEMORIA,
};
