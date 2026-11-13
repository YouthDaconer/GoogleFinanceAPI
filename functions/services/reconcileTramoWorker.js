/**
 * HU 6.8.2 — Punto de entrada del worker que corrige UN tramo de un inversor.
 *
 * Corre aislado del proceso principal de la corrección nocturna, con su propio
 * tope de memoria y de tiempo (`utils/ejecutorAislado.js`): si el tramo de un
 * inversor agota la memoria, muere este worker y la noche sigue con los demás
 * (Escenario 1).
 *
 * El worker es un aislado de V8 nuevo, sin los módulos del principal: por eso
 * inicializa aquí la app de Firebase (`./firebaseAdmin`) antes de cargar el
 * motor. El token del API de mercado le llega por la variable de entorno del
 * secreto, que el worker hereda.
 *
 * @module services/reconcileTramoWorker
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T2)
 */

require('./firebaseAdmin');
const { atenderTareas } = require('../utils/ejecutorAislado');
const { ejecutarTramo } = require('./reconcileStalePerformance');

atenderTareas(ejecutarTramo);
