/**
 * HU 6.2 — Qué del historial de rendimientos se puede buscar, y qué ya no.
 *
 * **Por qué existe este módulo.** Firestore construye un índice de campo único
 * para **cada campo hoja** de cada documento. En `portfolioPerformance/**\/dates`
 * eso son 8 monedas × 13 métricas, más `assetPerformance` con N activos × 13
 * métricas, por día, en el total y en cada cuenta: 3,99 millones de cifras, el
 * ~95 % de los 2,79 GiB que ocupaba la base el 20/10/2026. **Nadie las
 * consulta**: el historial solo se busca y se ordena por `date`.
 *
 * La HU 6.2 apaga ese indexado con una exención comodín. A partir de ahí,
 * filtrar u ordenar por una de esas cifras deja de funcionar — y Firestore, ante
 * una consulta así, responde con *«an error message with a link that you can
 * follow to create the missing index»*, es decir, **invitando a crear el índice
 * que esta historia vino a quitar**. Este módulo existe para que ese error no
 * sea el que da Firestore, sino uno que dice qué pasa y dónde se arregla
 * (RN-6.2-M, RN-6-F).
 *
 * @module scripts/camposBuscablesHistorial
 * @see platform-docs/architecture/FIRESTORE-INDEXES-FUENTE-UNICA.md
 * @see platform-docs/stories/6.2-dejar-pagar-busqueda-sin-uso/refinamiento.md (T5)
 */

/**
 * Grupo de colección con la exención comodín.
 *
 * **Lo comparten dos padres distintos**, y Firestore declara las exenciones por
 * nombre de grupo, no por ruta: `portfolioPerformance/{uid}/dates` (y
 * `/accounts/{acc}/dates`) e `indexHistories/{codigo}/dates`. Alcanzar a los dos
 * no se puede evitar, y es seguro: el histórico de índices también se consulta
 * solo por `date`.
 */
const GRUPO_EXENTO = 'dates';

/** Los únicos campos de `dates` que siguen indexados. */
const CAMPOS_BUSCABLES = Object.freeze(['date', '__name__']);

/**
 * Las cifras de rendimiento que dejaron de estar preparadas para buscar.
 *
 * Viven dentro del mapa de cada moneda (`USD`, `COP`, `EUR`…) y, repetidas, dentro
 * de cada activo de `assetPerformance`. No hace falta listar las monedas: los
 * subcampos de un mapa heredan la exención del mapa.
 */
const CIFRAS_EXCLUIDAS = Object.freeze([
  'totalInvestment',
  'totalValue',
  'totalROI',
  'dailyReturn',
  'monthlyReturn',
  'annualReturn',
  'dailyChangePercentage',
  'adjustedDailyChangePercentage',
  'rawDailyChangePercentage',
  'totalCashFlow',
  'doneProfitAndLoss',
  'unrealizedProfitAndLoss',
  'units',
  'assetPerformance',
]);

/**
 * Las que se buscan en el código al vigilar que nadie estrene una consulta.
 *
 * Es `CIFRAS_EXCLUIDAS` **menos `units`**, a propósito: `units` también es un
 * campo de la colección `assets`, donde sí es legítimo consultarlo, y un escaneo
 * por nombre no distingue una colección de otra. Dejarlo dentro convertiría el
 * guardia en ruido, y un guardia que avisa de lo que está bien se acaba
 * ignorando. `units` sigue excluida para `esBuscable`.
 */
const CIFRAS_VIGILADAS = Object.freeze(CIFRAS_EXCLUIDAS.filter((cifra) => cifra !== 'units'));

/** Dónde se habilita una búsqueda nueva. Es lo que el mensaje de error tiene que decir. */
const RUTA_CANONICO = 'src/GoogleFinanceAPI/firestore.indexes.json';

/**
 * ¿Se puede filtrar u ordenar el historial por este campo?
 *
 * Acepta rutas con punto (`USD.totalValue`, `USD.assetPerformance.AAPL_stock.totalROI`):
 * lo que decide es la **raíz**, porque la exención es del mapa y los subcampos la
 * heredan.
 *
 * @param {string} campo Campo tal como se pasaría a `.where()` o `.orderBy()`.
 * @returns {boolean} `true` solo para los campos que siguen indexados.
 */
function esBuscable(campo) {
  if (typeof campo !== 'string' || campo.length === 0) return false;
  if (CAMPOS_BUSCABLES.includes(campo)) return true;

  // `date` es un campo raíz; cualquier ruta con punto cae dentro de un mapa de
  // moneda, y esos están exentos enteros.
  return false;
}

/**
 * El error explícito que pide RN-6.2-M: qué campo, qué le pasa y dónde se arregla.
 *
 * Nombra el fichero canónico en vez del enlace que ofrece Firestore, que lleva a
 * crear un índice compuesto — y un índice compuesto **sí** puede indexar un campo
 * exento, con lo que devolvería el gasto que esta historia quitó.
 *
 * @param {string} campo El campo por el que se intentó buscar.
 * @returns {string} Mensaje listo para lanzar o registrar.
 */
function mensajeDeCampoNoBuscable(campo) {
  return (
    `El historial de rendimientos no está habilitado para buscar ni ordenar por '${campo}'. ` +
    `Solo lo están ${CAMPOS_BUSCABLES.map((c) => `'${c}'`).join(' y ')}: las cifras de rendimiento ` +
    'se leen en memoria del documento del día, no se consultan. ' +
    `Si de verdad hace falta buscarlo, habilítalo de forma explícita en ${RUTA_CANONICO} ` +
    `(exención del grupo '${GRUPO_EXENTO}') y publícalo con ` +
    "'node scripts/firestore-indexes.js deploy'. No crees un índice desde la consola de Firebase: " +
    'eso devuelve el gasto que la HU 6.2 quitó.'
  );
}

/**
 * Comprueba un campo antes de usarlo en una consulta al historial.
 *
 * @param {string} campo Campo que se va a filtrar u ordenar.
 * @throws {Error} Con el mensaje de `mensajeDeCampoNoBuscable` si no está habilitado.
 * @returns {string} El mismo campo, para poder encadenarlo.
 */
function exigirCampoBuscable(campo) {
  if (!esBuscable(campo)) {
    throw new Error(mensajeDeCampoNoBuscable(campo));
  }
  return campo;
}

module.exports = {
  GRUPO_EXENTO,
  CAMPOS_BUSCABLES,
  CIFRAS_EXCLUIDAS,
  CIFRAS_VIGILADAS,
  RUTA_CANONICO,
  esBuscable,
  mensajeDeCampoNoBuscable,
  exigirCampoBuscable,
};
