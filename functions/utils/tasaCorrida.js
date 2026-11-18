/**
 * HU 6.14.2 (T1) — Los días guardados con la tasa del día siguiente.
 *
 * Hasta el 2026-11-14 la serie histórica de tasas fechaba cada cierre un día
 * antes en el verano británico (6.9.2 T0): el lunes llevaba el cierre del
 * martes y el viernes faltaba. Los días que la corrección nocturna o una
 * reconstrucción reescribieron con esa serie quedaron con la tasa del día
 * siguiente. Aquí se detectan **sin leer nada**: el script
 * `scripts/tasa-corrida.js` lee los días y la serie corregida y llama a estas
 * funciones.
 *
 * **La tasa deducida de un día** es `<moneda>.totalValue / USD.totalValue` de su
 * documento general. La corrección convierte todo el día con una sola tasa por
 * moneda (`backfillCoreModule`: bloque = USD × tasa), así que ese cociente es la
 * tasa que usó, con error ≤ 2,2·10⁻¹⁶ (medido en producción el 2026-11-16). Vale
 * igual para la moneda principal y para el USD de los activos cotizados en COP.
 *
 * **Un día está afectado** (RN-6.14.2-A) si cumple las cuatro condiciones:
 * - lo reescribió una corrección (tiene `lastUpdated`; el cierre diario no lo
 *   escribe y usa la tasa del momento, así que no tiene este error);
 * - se reescribió antes del corte;
 * - es de verano británico;
 * - su tasa deducida coincide con el cierre del día siguiente y no con el de
 *   su día.
 *
 * Si los dos cierres son prácticamente iguales, no hay error que corregir y el
 * día queda `ambiguo` (RN-6.14.2-B). Si no coincide con ninguno, queda para
 * revisión (RN-6.14-C).
 *
 * El umbral U es relativo y no redondea nada (estándares §4 regla 13): con 10⁻⁴
 * el cierre diario daba coincidencias por casualidad; un día de la corrección
 * coincide con 10⁻¹⁶.
 *
 * @module utils/tasaCorrida
 * @see platform-docs/stories/6.14.2-dias-tasa-corrida-recalculados/refinamiento.md (T1)
 */

const { DateTime } = require('luxon');
const { MAX_LOOKBACK_DAYS } = require('../services/historicalRateService');

/** U (RN-6.14.2-B): 0,0001 % relativo, para "coincide" y para "prácticamente iguales". */
const UMBRAL_COINCIDE = 1e-6;

/**
 * Días reescritos desde este instante ya usaron la serie corregida. Es el día
 * siguiente al arreglo de la 6.9.2 (su hora exacta no consta). Un día reescrito
 * el 14 después del arreglo coincide con su cierre y no se marca igual.
 */
const CORTE_SERIE_CORREGIDA = '2026-11-15T00:00:00Z';

/** La razón de la marca `_stale` que pone esta HU. */
const MOTIVO_MARCA = 'tasa-corrida-6.14.2';

const ZONA_LONDRES = 'Europe/London';

/** Un cierre válido es un número finito positivo (RN-6.14-D). */
function esCierreValido(valor) {
  return typeof valor === 'number' && Number.isFinite(valor) && valor > 0;
}

function sumarDias(fecha, dias) {
  return DateTime.fromISO(fecha, { zone: 'utc' }).plus({ days: dias }).toISODate();
}

/**
 * El cierre de un día: el suyo o el hábil anterior, hasta `MAX_LOOKBACK_DAYS`
 * días naturales, como `historicalRateService` (RN-6.11-L).
 *
 * @param {Object<string, number>} serie - `{ 'YYYY-MM-DD': cierre }`
 * @param {string} fecha - YYYY-MM-DD
 * @returns {{fecha: string, cierre: number}|null}
 */
function cierreDelDia(serie, fecha) {
  for (let i = 0; i <= MAX_LOOKBACK_DAYS; i++) {
    const dia = sumarDias(fecha, -i);
    if (esCierreValido(serie[dia])) return { fecha: dia, cierre: serie[dia] };
  }
  return null;
}

/**
 * El cierre del día siguiente: el primer cierre posterior a la fecha, hasta
 * `MAX_LOOKBACK_DAYS` días naturales. De un viernes o un sábado es el lunes.
 *
 * @param {Object<string, number>} serie
 * @param {string} fecha - YYYY-MM-DD
 * @returns {{fecha: string, cierre: number}|null}
 */
function cierreSiguiente(serie, fecha) {
  for (let i = 1; i <= MAX_LOOKBACK_DAYS; i++) {
    const dia = sumarDias(fecha, i);
    if (esCierreValido(serie[dia])) return { fecha: dia, cierre: serie[dia] };
  }
  return null;
}

/** Del último domingo de marzo al último domingo de octubre (hora de verano en Londres). */
function esVeranoBritanico(fecha) {
  return DateTime.fromISO(fecha, { zone: ZONA_LONDRES }).set({ hour: 12 }).isInDST;
}

/** `|a / b − 1| ≤ umbral`: relativo, sin redondear. */
function coincide(a, b, umbral) {
  return Math.abs(a / b - 1) <= umbral;
}

/**
 * `<moneda>.totalValue / USD.totalValue`, o `null` si el día no guarda la moneda
 * o no tiene valor.
 */
function tasaDeducida(dia, moneda) {
  const valor = dia?.[moneda]?.totalValue;
  const usd = dia?.USD?.totalValue;
  if (!(typeof valor === 'number' && valor > 0) || !(typeof usd === 'number' && usd > 0)) return null;
  return valor / usd;
}

/** `lastUpdated` como ISO, venga como texto, `Date` o `Timestamp`. */
function lastUpdatedIso(valor) {
  if (!valor) return null;
  if (typeof valor === 'string') return valor;
  if (typeof valor.toDate === 'function') return valor.toDate().toISOString();
  if (valor instanceof Date) return valor.toISOString();
  return null;
}

/**
 * Clasifica un día guardado frente a la serie corregida.
 *
 * | Motivo | Cuándo |
 * |---|---|
 * | `afectado` | Verano, reescrito antes del corte; coincide con el cierre siguiente y no con el suyo |
 * | `suyo` | Coincide con el cierre de su día |
 * | `ambiguo` | Los dos cierres son prácticamente iguales: no hay error que corregir |
 * | `sin-coincidencia` | No coincide con ningún cierre: para revisión |
 * | `invierno` | Coincidiría con el siguiente, pero es de invierno: no hubo corrimiento |
 * | `cierre-diario` | Sin `lastUpdated`: lo escribió el cierre diario, con la tasa del momento |
 * | `posterior-al-corte` | Reescrito con la serie ya corregida |
 * | `sin-tasa` | El día no guarda la moneda, o su valor es 0 |
 * | `sin-cierre` | La serie no tiene el cierre de su día o el del siguiente |
 *
 * @param {Object} dia - El documento general del día (`date`, `lastUpdated`, `USD`, `<moneda>`)
 * @param {Object<string, number>} serie - Cierres corregidos de la moneda
 * @param {Object} opciones
 * @param {string} opciones.moneda
 * @param {number} [opciones.umbral=UMBRAL_COINCIDE]
 * @param {string} [opciones.corte=CORTE_SERIE_CORREGIDA]
 * @returns {{fecha: string, motivo: string, tasa?: number, cierreSuyo?: number, cierreSiguiente?: number}}
 */
function clasificarDia(dia, serie, { moneda, umbral = UMBRAL_COINCIDE, corte = CORTE_SERIE_CORREGIDA }) {
  const fecha = dia.date;
  const escrito = lastUpdatedIso(dia.lastUpdated);
  if (!escrito) return { fecha, motivo: 'cierre-diario' };
  if (new Date(escrito).getTime() >= new Date(corte).getTime()) return { fecha, motivo: 'posterior-al-corte' };

  const tasa = tasaDeducida(dia, moneda);
  if (tasa === null) return { fecha, motivo: 'sin-tasa' };

  const suyo = cierreDelDia(serie, fecha);
  const siguiente = cierreSiguiente(serie, fecha);
  if (!suyo || !siguiente) return { fecha, motivo: 'sin-cierre', tasa };

  const base = { fecha, tasa, cierreSuyo: suyo.cierre, cierreSiguiente: siguiente.cierre };
  const conSuyo = coincide(tasa, suyo.cierre, umbral);
  const conSiguiente = coincide(tasa, siguiente.cierre, umbral);
  // Lo que no coincide con ningún cierre va a revisión aunque los dos cierres sean casi iguales
  if (!conSuyo && !conSiguiente) return { ...base, motivo: 'sin-coincidencia' };
  if (coincide(siguiente.cierre, suyo.cierre, umbral)) return { ...base, motivo: 'ambiguo' };
  if (conSuyo) return { ...base, motivo: 'suyo' };
  return { ...base, motivo: esVeranoBritanico(fecha) ? 'afectado' : 'invierno' };
}

/**
 * La detección de un inversor en una moneda (RN-6.14-B, RN-6-B).
 *
 * `recalculados` son los días **posteriores** al primero afectado que no están
 * afectados: no cuentan como afectados ni deciden la fecha, pero la corrección
 * nocturna los rehace igual, porque corrige desde el primer día afectado hasta
 * hoy (decisión del PO del 2026-11-16, QA-05). La lista previa muestra los que no
 * son de rutina (todo salvo `suyo`).
 *
 * @param {Object[]} dias - Documentos generales del inversor
 * @param {Object<string, number>} serie
 * @param {Object} opciones - Las de `clasificarDia`
 * @returns {{
 *   moneda: string, dias: number, diasAfectados: number, primerDiaAfectado: string|null,
 *   ultimoDiaAfectado: string|null, ambiguos: number, sinCoincidencia: string[],
 *   porMotivo: Object<string, number>,
 *   recalculados: {total: number, porMotivo: Object<string, number>, aRevisar: {fecha: string, motivo: string}[]}
 * }}
 */
function detectarInversor(dias, serie, opciones) {
  const clasificados = [...dias]
    .sort((a, b) => (a.date < b.date ? -1 : 1))
    .map((dia) => clasificarDia(dia, serie, opciones));

  const porMotivo = {};
  for (const { motivo } of clasificados) porMotivo[motivo] = (porMotivo[motivo] || 0) + 1;

  const afectados = clasificados.filter((c) => c.motivo === 'afectado');
  const primerDiaAfectado = afectados.length ? afectados[0].fecha : null;
  const ultimoDiaAfectado = afectados.length ? afectados[afectados.length - 1].fecha : null;

  const recalculados = { total: 0, porMotivo: {}, aRevisar: [] };
  if (primerDiaAfectado) {
    for (const c of clasificados) {
      if (c.fecha <= primerDiaAfectado || c.motivo === 'afectado') continue;
      recalculados.total++;
      recalculados.porMotivo[c.motivo] = (recalculados.porMotivo[c.motivo] || 0) + 1;
      if (c.motivo !== 'suyo') recalculados.aRevisar.push({ fecha: c.fecha, motivo: c.motivo });
    }
  }

  return {
    moneda: opciones.moneda,
    dias: clasificados.length,
    diasAfectados: afectados.length,
    primerDiaAfectado,
    ultimoDiaAfectado,
    ambiguos: porMotivo.ambiguo || 0,
    sinCoincidencia: clasificados.filter((c) => c.motivo === 'sin-coincidencia').map((c) => c.fecha),
    porMotivo,
    recalculados,
  };
}

/**
 * El primer día afectado de un inversor entre todas sus monedas, o `null`.
 *
 * @param {{primerDiaAfectado: string|null}[]} porMoneda
 * @returns {string|null}
 */
function primerDiaDelInversor(porMoneda) {
  const fechas = porMoneda.map((r) => r.primerDiaAfectado).filter(Boolean).sort();
  return fechas[0] || null;
}

/**
 * Qué hace `--marcar` con un inversor (RN-6.14-F, QA-08):
 * - `sin-afectados`: nada que marcar;
 * - `ya-cubierto`: ya tiene una corrección pendiente desde ese día o antes. No
 *   se escribe nada: ni marca dos veces ni adelanta su fecha;
 * - `adelantar`: tiene una corrección pendiente desde un día posterior; se marca
 *   y `markPerformanceAsStale` la lleva al primer día afectado;
 * - `marcar`: sin marca.
 *
 * @param {Object|null|undefined} staleActual - `_stale` del inversor
 * @param {string|null} primerDia
 * @returns {'sin-afectados'|'ya-cubierto'|'adelantar'|'marcar'}
 */
function decidirMarca(staleActual, primerDia) {
  if (!primerDia) return 'sin-afectados';
  if (!staleActual) return 'marcar';
  if (staleActual.since && staleActual.since <= primerDia) return 'ya-cubierto';
  return 'adelantar';
}

module.exports = {
  UMBRAL_COINCIDE,
  CORTE_SERIE_CORREGIDA,
  MOTIVO_MARCA,
  esCierreValido,
  cierreDelDia,
  cierreSiguiente,
  esVeranoBritanico,
  tasaDeducida,
  clasificarDia,
  detectarInversor,
  primerDiaDelInversor,
  decidirMarca,
};
