/**
 * Ventana de flujos de caja de un día calculado.
 *
 * El rendimiento de un día se calcula contra **el último doc que existe**, no
 * contra "ayer": si el doc de ayer falta (el cierre no corrió, un festivo mal
 * detectado, una tarea perdida), la base es el de anteayer. Para que la fórmula
 * `(V - V_base + flujo) / V_base` siga siendo correcta, el flujo tiene que ser
 * **todo lo que se movió desde esa base**, no sólo lo fechado el día que se
 * cierra. Si no, el dinero de una venta del día sin doc sale del portafolio
 * sin entrar como flujo y cuenta como pérdida (2026-07-23: −4,6 % en IBKR con
 * el mercado en −2,2 %, porque las ventas del 07-22 no tenían doc propio).
 *
 * Es la misma regla que ya aplicaba la reconciliación (`isFlowOfDay` con
 * `fromExclusive`); aquí se comparte para que el cierre nocturno y la
 * reconciliación no puedan divergir.
 *
 * @module utils/flowWindow
 */

/**
 * Tope de la ventana, en días naturales. Un fin de semana largo con festivo
 * son 4 días; una caída del cierre de una semana, 7-9. Más allá, el hueco no es
 * un día perdido sino un problema de datos que debe recalcular la
 * reconciliación, y en el proceso único una ventana sin tope convertiría la
 * consulta global de transacciones en un escaneo de meses.
 */
const MAX_FLOW_WINDOW_DAYS = 14;

/**
 * Parte de fecha (YYYY-MM-DD) de un campo `date` de transacción: ISO con hora,
 * sólo fecha, `Date` o Timestamp de Firestore.
 *
 * @param {string|Date|{toDate: Function}|null|undefined} value
 * @returns {string} '' si no se puede leer
 */
function getDatePart(value) {
  if (!value) return '';
  if (typeof value === 'string') return value.substring(0, 10);
  if (typeof value.toDate === 'function') return value.toDate().toISOString().substring(0, 10);
  if (value instanceof Date) return value.toISOString().substring(0, 10);
  return '';
}

/**
 * ¿La transacción es un flujo del día `targetDate`? Con `fromExclusive` (el
 * último día calculado) cuenta todo lo fechado en (fromExclusive, targetDate];
 * sin él, sólo lo fechado ese día.
 *
 * @param {*} txDate - Campo `date` de la transacción
 * @param {string} targetDate - Día que se calcula (YYYY-MM-DD)
 * @param {string|null} [fromExclusive] - Último día calculado (YYYY-MM-DD)
 * @returns {boolean}
 */
function isFlowOfDay(txDate, targetDate, fromExclusive = null) {
  const day = getDatePart(txDate);
  if (!day) return false;
  return fromExclusive ? day > fromExclusive && day <= targetDate : day === targetDate;
}

/** `YYYY-MM-DD` menos `days` días naturales (UTC, sin zona: sólo aritmética de fechas). */
function minusDays(date, days) {
  const d = new Date(`${date}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().substring(0, 10);
}

/**
 * Inicio exclusivo de la ventana de flujos de un ámbito (portafolio o cuenta)
 * cuyo último doc es `lastDate`.
 *
 * @param {string} targetDate - Día que se calcula
 * @param {string|null} lastDate - Fecha del último doc del ámbito (< targetDate)
 * @param {number} [maxDays]
 * @returns {{fromExclusive: string|null, capped: boolean}} `fromExclusive`
 *   null si no hay doc previo (primer cálculo: sólo cuenta el día); `capped`
 *   si el hueco supera el tope y se recortó.
 */
function flowWindowFor(targetDate, lastDate, maxDays = MAX_FLOW_WINDOW_DAYS) {
  if (!lastDate || lastDate >= targetDate) return { fromExclusive: null, capped: false };
  const floor = minusDays(targetDate, maxDays);
  if (lastDate < floor) return { fromExclusive: floor, capped: true };
  return { fromExclusive: lastDate, capped: false };
}

/**
 * Inicio exclusivo de la consulta de transacciones que cubre a varios ámbitos:
 * el más antiguo de sus `fromExclusive`, o el día anterior a `targetDate` si
 * ninguno tiene doc previo.
 *
 * @param {string} targetDate
 * @param {Array<string|null>} lastDates
 * @param {number} [maxDays]
 * @returns {string} YYYY-MM-DD exclusivo
 */
function queryWindowStart(targetDate, lastDates, maxDays = MAX_FLOW_WINDOW_DAYS) {
  let start = minusDays(targetDate, 1);
  for (const lastDate of lastDates) {
    const { fromExclusive } = flowWindowFor(targetDate, lastDate, maxDays);
    if (fromExclusive && fromExclusive < start) start = fromExclusive;
  }
  return start;
}

/**
 * Límites de la consulta por rango sobre el campo string `date`
 * (`where('date', '>', from).where('date', '<=', to)`). Con el sufijo de fin
 * de día cubre tanto ISO con hora como fechas sin hora.
 *
 * @param {string} fromExclusive - YYYY-MM-DD
 * @param {string} targetDate - YYYY-MM-DD
 * @returns {{after: string, until: string}}
 */
function queryBounds(fromExclusive, targetDate) {
  return { after: `${fromExclusive}T23:59:59.999Z`, until: `${targetDate}T23:59:59.999Z` };
}

module.exports = {
  MAX_FLOW_WINDOW_DAYS,
  getDatePart,
  isFlowOfDay,
  minusDays,
  flowWindowFor,
  queryWindowStart,
  queryBounds,
};
