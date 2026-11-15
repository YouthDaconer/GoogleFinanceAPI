/**
 * HU 6.11.5 — La cabecera del día anterior sin descargar todas las monedas.
 *
 * La cabecera de /portfolio calcula el resultado de hoy contra el último día
 * cerrado. Hasta esta HU el navegador leía ese documento diario entero, con
 * todas sus monedas, para usar una sola. Ahora lo lee el servidor y solo el
 * bloque que hace falta (`select`), y devuelve solo esa moneda:
 *
 * - **USD y la moneda principal**: el bloque guardado de esa moneda, con los
 *   mismos valores por defecto que aplicaba el navegador (Escenario 6).
 * - **Una moneda poco usada** (RN-6.9.2-J): el bloque en USD × la tasa de
 *   cierre de ese día (decisión del PO del 2026-11-14), sin leer lo guardado en
 *   esa moneda (RN-6.11-J). Un día sin cierre toma el cierre hábil anterior
 *   hasta 5 días naturales (RN-6.11-L); una tasa cero, negativa o vacía no
 *   cuenta. Sin tasa, o con el servicio de tasas caído, responde que no está
 *   disponible: nunca la cifra guardada (RN-6.11-K).
 *
 * Solo se derivan los **importes**: el cálculo de la cabecera
 * (`lib/intradayPerformance.ts`) no usa `factor` ni
 * `adjustedDailyChangePercent`, y derivarlos exigiría la tasa del día previo.
 *
 * Cuesta 1 lectura (como antes en el navegador) y, en una moneda poco usada,
 * 1 llamada a finance-query que comparte la caché de los resúmenes derivados.
 * Con `DERIVED_CURRENCY_SNAPSHOTS=false` vuelve a leer lo guardado en esa
 * moneda, como antes de la 6.9.2 (RN-6-H).
 *
 * @module services/snapshots/previousDayPerformanceService
 * @see platform-docs/stories/6.11.5-recargar-portfolio-sin-lecturas/refinamiento.md (T1)
 */

const { DateTime } = require('luxon');
const {
  resumenesDerivadosDelUsd,
  esMonedaPocoUsada,
  monedaPrincipalDe,
  tasasDelPeriodo,
  tasasDeLaSerie,
} = require('./derivedCurrencySnapshotService');

/** Los importes del día anterior: son los que se convierten con la tasa. */
const IMPORTES = [
  'totalValue',
  'totalInvestment',
  'totalCashFlow',
  'doneProfitAndLoss',
  'unrealizedProfitAndLoss',
];

/**
 * El bloque de una moneda del documento diario, con los mismos valores por
 * defecto que aplicaba `fetchPreviousDayPerformance` en el navegador, para que
 * USD y la moneda principal den exactamente las mismas cifras (Escenario 6).
 *
 * @param {Object} bloque - `docData[moneda]` del documento diario
 * @returns {{totalValue: number, totalInvestment: number, factor: number,
 *   adjustedDailyChangePercent: number, totalCashFlow: number,
 *   doneProfitAndLoss: number, unrealizedProfitAndLoss: number}}
 */
function extraerCabecera(bloque) {
  return {
    totalValue: bloque.totalValue || 0,
    totalInvestment: bloque.totalInvestment || 0,
    factor: bloque.factor || 1,
    adjustedDailyChangePercent: bloque.adjustedDailyChangePercent || 0,
    totalCashFlow: bloque.totalCashFlow || 0,
    doneProfitAndLoss: bloque.doneProfitAndLoss || 0,
    unrealizedProfitAndLoss: bloque.unrealizedProfitAndLoss || 0,
  };
}

/**
 * Los importes del día en otra moneda: USD × tasa, sin redondear (estándar
 * §4.13). `factor` y `adjustedDailyChangePercent` no se derivan (ver módulo).
 *
 * @param {Object} bloqueUsd - `docData.USD` del documento diario
 * @param {number} tasa - Unidades de la moneda por 1 USD, ya validada (> 0)
 * @returns {Object<string, number>}
 */
function derivarCabecera(bloqueUsd, tasa) {
  const usd = extraerCabecera(bloqueUsd);
  return Object.fromEntries(IMPORTES.map((campo) => [campo, usd[campo] * tasa]));
}

const ZONA = 'America/New_York';

/**
 * Hoy en Nueva York (estándar §4.10). El navegador filtraba con la fecha UTC,
 * pero eligen el mismo documento: el cierre escribe el día D a las 00:05 ET de
 * D+1, así que nunca hay un documento con la fecha de hoy (ni en NY ni en UTC).
 */
function hoyEnNuevaYork() {
  return DateTime.now().setZone(ZONA).toISODate();
}

/**
 * La cabecera del día anterior de un inversor en una moneda.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.currency
 * @param {string} [params.hoy] - `YYYY-MM-DD`; por defecto hoy en Nueva York
 * @returns {Promise<
 *   {disponible: true, date: string, currency: string, data: Object|null, derivedFrom?: 'USD'} |
 *   {disponible: false, date: string, currency: string, motivo: string}
 * >} `data` en `null` y `date` vacío si el inversor aún no tiene días cerrados
 */
async function obtenerCabeceraDiaAnterior(db, { userId, currency, hoy = hoyEnNuevaYork() }) {
  const inicio = Date.now();
  const derivar = currency !== 'USD' &&
    resumenesDerivadosDelUsd() &&
    esMonedaPocoUsada(currency, await monedaPrincipalDe(db, userId));
  const bloque = derivar ? 'USD' : currency;

  const terminar = (resultado, metodo) => {
    console.log(`[previousDayPerformance] ${userId} ${currency}: ${metodo}` +
      `${resultado.motivo ? ` (${resultado.motivo})` : ''}, día ${resultado.date || '—'}, ${Date.now() - inicio} ms`);
    return resultado;
  };

  // Solo la fecha y el bloque que hace falta: el documento no viaja entero (Escenario 4).
  const snapshot = await db.collection(`portfolioPerformance/${userId}/dates`)
    .where('date', '<', hoy)
    .orderBy('date', 'desc')
    .limit(1)
    .select('date', bloque)
    .get();

  if (snapshot.empty) {
    return terminar({ disponible: true, date: '', currency, data: null }, 'sin-historia');
  }

  const docData = snapshot.docs[0].data();
  const date = docData.date;
  const datosDelBloque = docData[bloque];

  if (!derivar) {
    // El navegador omitía las monedas que el día no tenía: lo mismo aquí.
    const data = datosDelBloque ? extraerCabecera(datosDelBloque) : null;
    return terminar({ disponible: true, date, currency, data }, 'guardado');
  }

  if (!datosDelBloque) {
    return terminar({ disponible: true, date, currency, data: null }, 'usd-sin-bloque');
  }

  const cierres = await tasasDelPeriodo(currency, date, date);
  if (!cierres) {
    return terminar({ disponible: false, date, currency, motivo: 'sin-tasas' }, 'no-disponible');
  }
  const { tasas, faltante } = tasasDeLaSerie([{ d: date }], cierres);
  if (faltante) {
    return terminar({ disponible: false, date, currency, motivo: 'sin-tasa-del-dia' }, 'no-disponible');
  }

  const data = derivarCabecera(datosDelBloque, tasas[date]);
  return terminar({ disponible: true, date, currency, data, derivedFrom: 'USD' }, 'derivado');
}

/**
 * Comparación previa al despliegue (RN-6-B, acordada del PO): cada campo debe
 * ser idéntico **en todos sus decimales**, importes y porcentajes por igual.
 * La usa `scripts/cabecera-dia-anterior.js comparar`; no se llama por consulta.
 *
 * @param {Object|null} obtenida - Lo que devuelve el servicio (`data`)
 * @param {Object|null} referencia - Lo de antes (USD y principal) o REF-0
 * @returns {string[]} Una línea por campo distinto; vacía si son idénticas
 */
function diferenciasDeCabecera(obtenida, referencia) {
  if (obtenida === null || referencia === null) {
    return obtenida === referencia ? [] : [`datos: ${JSON.stringify(obtenida)} ≠ ${JSON.stringify(referencia)}`];
  }
  const campos = [...new Set([...Object.keys(obtenida), ...Object.keys(referencia)])].sort();
  return campos
    .filter((campo) => obtenida[campo] !== referencia[campo])
    .map((campo) => `${campo}: ${obtenida[campo]} ≠ ${referencia[campo]}`);
}

module.exports = {
  obtenerCabeceraDiaAnterior,
  extraerCabecera,
  derivarCabecera,
  diferenciasDeCabecera,
  IMPORTES,
};
