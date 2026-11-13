/**
 * HU 7 — Las rutas de la function `app` que responden **sin sesión**, y por qué.
 *
 * **Toda ruta de `app` exige sesión salvo las de esta lista** (RN-7-F). Así
 * una consulta nueva con datos de un inversor nace protegida aunque quien la
 * escriba no se acuerde de protegerla: el guardia se engancha por defecto en
 * `httpApi.js`, y esta lista es la única forma de abrir una ruta.
 *
 * **Abrir una ruta es una decisión, y se registra aquí** (Escenario 9, RN-7-F):
 * cada entrada lleva `motivo`, `decididoPor` y `fecha`. La auditoría
 * (`scripts/investorRouteAudit.js`) las lista aparte como «abiertas por
 * decisión», y marca como fallo cualquier entrada a la que le falte uno de esos
 * tres campos. **No agregues aquí una ruta que devuelva datos de un inversor.**
 *
 * Las entradas del 2026-11-12 son las rutas que ya estaban abiertas y no
 * contienen datos de ningún inversor: mercado, búsqueda, noticias, análisis de
 * archivos de ETF y salud (RN-7-G).
 *
 * @module config/publicHttpRoutes
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T3)
 */

const DECISION_HU7 = Object.freeze({
  decididoPor: 'Carlos Caicedo (PO)',
  fecha: '2026-11-12',
});

const MERCADO = 'Datos de mercado públicos; no contiene datos de ningún inversor (RN-7-G)';

/**
 * @typedef {object} RutaAbierta
 * @property {string} method - Método HTTP en mayúsculas.
 * @property {string} path - Ruta tal como se registra en Express (admite `:param`).
 * @property {string} motivo - Por qué puede responder sin sesión.
 * @property {string} decididoPor - Quién tomó la decisión.
 * @property {string} fecha - Cuándo (AAAA-MM-DD).
 */

/** @type {ReadonlyArray<RutaAbierta>} */
const RUTAS_ABIERTAS = Object.freeze([
  { method: 'GET', path: '/', motivo: 'Comprobación de salud; no devuelve datos', ...DECISION_HU7 },
  { method: 'GET', path: '/health', motivo: 'Comprobación de salud; no devuelve datos', ...DECISION_HU7 },
  { method: 'GET', path: '/indices', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/fullQuote', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/quote', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/apiQuote', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/currencie', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/active', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/gainers', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/losers', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/news', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/quotes', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/simple-quotes', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/news-from-quote', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/search', motivo: MERCADO, ...DECISION_HU7 },
  { method: 'GET', path: '/news/batch', motivo: MERCADO, ...DECISION_HU7 },
  {
    method: 'POST',
    path: '/process-etf-excel',
    motivo: 'Analiza el archivo de composición de un ETF que sube el usuario y devuelve el resultado; no lee datos de ningún inversor',
    ...DECISION_HU7,
  },
  {
    method: 'POST',
    path: '/process-etf-excel-lite',
    motivo: 'Igual que /process-etf-excel, versión ligera; no lee datos de ningún inversor',
    ...DECISION_HU7,
  },
  {
    method: 'GET',
    path: '/upload-progress/:dataKey',
    motivo: 'Progreso en memoria del análisis de un archivo de ETF; no contiene datos de ningún inversor',
    ...DECISION_HU7,
  },
]);

/** Campos que hacen de una entrada una decisión registrada. */
const CAMPOS_DECISION = Object.freeze(['motivo', 'decididoPor', 'fecha']);

/**
 * Normaliza una ruta como la compara Express por defecto: sin distinguir
 * mayúsculas (`caseSensitive: false`) y con una barra final opcional
 * (`strict: false`). Compararla más estricto que Express no abre nada (lo
 * que no es abierto queda protegido); compararla igual evita cerrar una ruta
 * de mercado que hoy responde, p. ej., `/Quotes`.
 *
 * @param {string} ruta
 * @returns {string[]} Segmentos en minúsculas.
 */
function segmentos(ruta) {
  const limpia = String(ruta || '').toLowerCase().replace(/\/+$/, '');
  return limpia === '' ? [] : limpia.split('/').slice(1);
}

/**
 * ¿La ruta registrada `patron` (con `:param`) cubre la ruta pedida `ruta`?
 *
 * @param {string} patron
 * @param {string} ruta
 * @returns {boolean}
 */
function coincideRuta(patron, ruta) {
  const p = segmentos(patron);
  const r = segmentos(ruta);
  if (p.length !== r.length) return false;
  return p.every((seg, i) => (seg.startsWith(':') ? r[i] !== '' : seg === r[i]));
}

/**
 * Busca la entrada de la lista que abre esta petición.
 *
 * `HEAD` se trata como `GET`, igual que en Express.
 *
 * @param {string} method
 * @param {string} ruta - `req.path`
 * @param {ReadonlyArray<RutaAbierta>} [rutas]
 * @returns {RutaAbierta|null}
 */
function buscarRutaAbierta(method, ruta, rutas = RUTAS_ABIERTAS) {
  const metodo = String(method || '').toUpperCase() === 'HEAD' ? 'GET' : String(method || '').toUpperCase();
  return rutas.find((r) => r.method === metodo && coincideRuta(r.path, ruta)) || null;
}

/**
 * ¿La entrada trae su decisión completa?
 *
 * @param {RutaAbierta} entrada
 * @returns {boolean}
 */
function tieneDecision(entrada) {
  return CAMPOS_DECISION.every((campo) => typeof entrada[campo] === 'string' && entrada[campo].trim() !== '');
}

/**
 * ¿La app tiene registrada una ruta que atienda esta petición?
 *
 * Usa la pila interna del router de Express 4 (`app._router.stack`, y
 * `layer.match`, el mismo que usa Express al despachar). **Ante la duda responde
 * `true`**: lo único que depende de esto es si un rechazo se registra, así que
 * equivocarse hacia `true` como mucho registra de más. Nunca abre nada.
 *
 * @param {object} app - La app de Express.
 * @param {string} method
 * @param {string} ruta - `req.path`
 * @returns {boolean}
 */
function rutaRegistradaEn(app, method, ruta) {
  const pila = app && app._router && app._router.stack;
  if (!Array.isArray(pila)) return true;
  const metodo = String(method || '').toLowerCase() === 'head' ? 'get' : String(method || '').toLowerCase();
  try {
    return pila.some((capa) => {
      if (!capa.route || !capa.route.methods) return false;
      if (!capa.route.methods[metodo] && !capa.route.methods._all) return false;
      return typeof capa.match === 'function' ? capa.match(ruta) : true;
    });
  } catch (error) {
    return true;
  }
}

module.exports = {
  RUTAS_ABIERTAS,
  CAMPOS_DECISION,
  coincideRuta,
  buscarRutaAbierta,
  tieneDecision,
  rutaRegistradaEn,
};
