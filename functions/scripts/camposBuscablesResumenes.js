/**
 * HU 6.10.2 — Qué de los resúmenes y de los consolidados se puede buscar, y qué ya no.
 *
 * **Por qué existe este módulo.** Firestore prepara para búsqueda (índice de
 * campo único) **cada campo hoja** de cada documento. Los resúmenes de
 * rendimiento (`performanceSnapshots`) guardan listas que crecen un registro por
 * día —la serie (`timeline`), los flujos de dinero (`cashFlows`), el P&G
 * realizado (`realizedPnL`) y los días de cada activo (`assetDays`)— y los
 * consolidados de meses y años (`periods`) el resultado de cada periodo. Nadie
 * los busca: se leen enteros por su ID. La HU 6.10.2 apaga esa preparación con
 * una exención comodín (`*` sin índices) por grupo y la vuelve a encender solo
 * para los campos de la lista de búsquedas (RN-6.10-M, RN-6.10.2-J). Así una
 * lista que añada una historia futura nace sin preparar (RN-6.10.2-L).
 *
 * A partir de ahí, filtrar u ordenar por otro campo da `FAILED_PRECONDITION`, y
 * el mensaje de Firestore ofrece un enlace para crear el índice que falta, que
 * devolvería el gasto. Este módulo da el error que dice qué pasa y cómo se
 * vuelve atrás (Escenario 3, RN-6.10.2-K).
 *
 * Es la misma idea que `camposBuscablesHistorial.js` (HU 6.2) para `dates`, con
 * otro modelo: allí se vigila una lista cerrada de cifras por su nombre; aquí lo
 * vigilado es **todo lo que no esté en la lista**, atado a la colección.
 *
 * @module scripts/camposBuscablesResumenes
 * @see platform-docs/architecture/FIRESTORE-INDEXES-FUENTE-UNICA.md
 * @see platform-docs/stories/6.10.2-indices-solo-donde-se-busca/refinamiento.md (T4)
 */

const { RUTA_CANONICO } = require('./camposBuscablesHistorial');

/** Órdenes en que Firestore puede servir un campo con índice de campo único. */
const ASC = 'asc';
const DESC = 'desc';

/**
 * Los grupos de colección con la exención comodín y los campos que conservan.
 *
 * Cada campo declara **solo** los órdenes que alguna búsqueda usa (ajuste del
 * PO, 2026-11-16). Las búsquedas por igualdad y por rango ascendente usan
 * el ascendente.
 *
 * El rango ascendente por ID (`documentId()`) **no** necesita declararse: se
 * comprobó en producción el 2026-11-16 sobre `dates`, que tiene el comodín desde
 * la 6.2. El orden descendente por ID, en cambio, falla: lo vigila el guardián.
 */
const GRUPOS = Object.freeze({
  performanceSnapshots: Object.freeze({
    descripcion: 'Resúmenes de rendimiento',
    campos: Object.freeze({
      // R1, R2: los resúmenes de un inversor, y de una de sus cuentas (igualdades).
      userId: Object.freeze([ASC]),
      accountId: Object.freeze([ASC]),
      // R3, R4: los resúmenes por activo.
      type: Object.freeze([ASC]),
      // R3 ordena por él dentro del compuesto `type + lastUpdated`. Se conserva
      // también el simple porque lo pide la especificación de la HU.
      lastUpdated: Object.freeze([ASC]),
    }),
  }),
  periods: Object.freeze({
    descripcion: 'Consolidados de meses y años',
    campos: Object.freeze({
      // C1: rango de periodos en ascendente; C2: los últimos, en descendente.
      periodKey: Object.freeze([ASC, DESC]),
    }),
  }),
});

/**
 * Las listas de los resúmenes que crecen un registro por día (RN-6.10.2-L).
 *
 * No hace falta para la configuración, que las cubre con el comodín sin
 * nombrarlas. Sirve para que los tests comprueben que ninguna se habilita.
 */
const LISTAS_QUE_CRECEN_POR_DIA = Object.freeze(['timeline', 'cashFlows', 'realizedPnL', 'assetDays']);

/**
 * Los campos buscables de un grupo, o `null` si el grupo no tiene exención.
 *
 * @param {string} grupo Grupo de colección (`performanceSnapshots`, `periods`).
 * @returns {Object<string, Array<string>>|null} Campo → órdenes conservados.
 */
function camposDe(grupo) {
  return Object.prototype.hasOwnProperty.call(GRUPOS, grupo) ? GRUPOS[grupo].campos : null;
}

/**
 * ¿Se puede filtrar u ordenar este grupo por este campo, en este orden?
 *
 * Lo que decide es la **raíz** de una ruta con punto (`timeline.0.value` →
 * `timeline`): los subcampos de un mapa heredan la exención del mapa.
 *
 * @param {string} grupo Grupo de colección.
 * @param {string} campo Campo tal como se pasaría a `.where()` o `.orderBy()`.
 * @param {string} [orden='asc'] `asc` o `desc`. Un `.where()` usa el ascendente.
 * @returns {boolean} `true` solo si el campo conserva ese orden.
 */
function esBuscable(grupo, campo, orden = ASC) {
  const campos = camposDe(grupo);
  if (!campos || typeof campo !== 'string' || campo.length === 0) return false;

  const raiz = campo.split('.')[0];
  const ordenes = Object.prototype.hasOwnProperty.call(campos, raiz) ? campos[raiz] : null;
  return Boolean(ordenes && ordenes.includes(orden === DESC ? DESC : ASC));
}

/**
 * El error explícito de una búsqueda que necesita un campo retirado.
 *
 * Es el procedimiento del Escenario 3: se añade el campo al canónico y se
 * publica. No se pierde ningún dato ni se rehace nada (RN-6.10.2-K): Firestore
 * reconstruye el índice a partir de los documentos.
 *
 * @param {string} grupo Grupo de colección.
 * @param {string} campo El campo por el que se intentó buscar.
 * @param {string} [orden='asc'] El orden pedido.
 * @returns {string} Mensaje listo para lanzar o registrar.
 */
function mensajeDeCampoNoBuscable(grupo, campo, orden = ASC) {
  const campos = camposDe(grupo) || {};
  const conservados = Object.entries(campos)
    .map(([nombre, ordenes]) => `'${nombre}' (${ordenes.join('/')})`)
    .join(', ');

  return (
    `'${grupo}' no está preparado para buscar ni ordenar por '${campo}' en orden ${orden === DESC ? DESC : ASC} (HU 6.10.2). ` +
    `Solo lo están ${conservados || 'ninguno'}, y el rango ascendente por ID. ` +
    `Si de verdad hace falta, añade una exención para '${campo}' del grupo '${grupo}' en ${RUTA_CANONICO}, ` +
    `añádelo a GRUPOS en scripts/camposBuscablesResumenes.js y publícalo con ` +
    "'node scripts/firestore-indexes.js deploy'. No se pierde ningún dato. " +
    'No crees el índice desde la consola de Firebase: devolvería el gasto sin que nadie lo vea.'
  );
}

module.exports = {
  ASC,
  DESC,
  GRUPOS,
  LISTAS_QUE_CRECEN_POR_DIA,
  camposDe,
  esBuscable,
  mensajeDeCampoNoBuscable,
};
