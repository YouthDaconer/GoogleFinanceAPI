/**
 * HU 6.11.4 — La atribución en monedas poco usadas se calcula desde USD.
 *
 * En una moneda poco usada (RN-6.9.2-J: ni USD ni la principal del inversor),
 * ningún camino de la atribución lee lo guardado en esa moneda (RN-6.11-J). Cada
 * cifra sale de los datos en USD × la tasa de cierre del día (decisión del PO
 * del 2026-11-14), con las mismas funciones de cálculo de siempre:
 *
 * - **Contribución por activo** (`holdingsContributions`): la serie diaria en
 *   USD × la tasa de cada día; las cotizaciones y los dividendos se convierten
 *   con la serie de cierres del servicio de tasas, y el P&G de las ventas es el
 *   de USD × la tasa del día de la venta.
 * - **Respaldo por periodo y varias cuentas con fechas** (`twrDeRangoDesdeUsd`):
 *   los días en USD, con el cambio diario convertido con la fórmula de la 6.9.2,
 *   `(1 + cUSD) · f(d) / f(d−1) − 1`.
 * - **Día en curso** (`intradiaDesdeUsd`): el intradía en USD; el valor de hoy
 *   con la tasa vigente y el del día anterior con la de su cierre.
 *
 * Un día sin cierre toma el cierre hábil anterior hasta 5 días naturales
 * (RN-6.11-L); una tasa cero, negativa o vacía no cuenta. Sin tasa, o con el
 * servicio de tasas caído, se lanza el aviso de la 6.11.1 (`monedaNoDisponible`):
 * nunca la cifra guardada ni una tasa inventada (RN-6.11-K).
 *
 * `ATTRIBUTION_FROM_USD=false` devuelve la atribución de antes de esta HU sin
 * apagar la derivación ni el aviso de las hermanas (RN-6-H).
 *
 * @module services/attribution/attributionFromUsdService
 * @see platform-docs/stories/6.11.4-atribucion-desde-usd/refinamiento.md (T1, T4)
 */

const {
  esMonedaPocoUsada,
  monedaPrincipalDe,
  tasasDelPeriodo,
  tasasDeLaSerie,
  cambioConvertido,
  monedaNoDisponible,
  CODIGO_MONEDA_NO_DISPONIBLE,
} = require('../snapshots/derivedCurrencySnapshotService');
const { getExchangeRates } = require('../financeQuery');

// ============================================================================
// INTERRUPTOR Y ALCANCE
// ============================================================================

/**
 * Se lee en cada llamada: basta con cambiar la variable y desplegar para volver
 * atrás (RN-6-H).
 *
 * @returns {boolean} `true` salvo `ATTRIBUTION_FROM_USD=false`
 */
function atribucionDesdeUsd() {
  return process.env.ATTRIBUTION_FROM_USD !== 'false';
}

/**
 * ¿Esta atribución se calcula desde USD? Solo en una moneda poco usada y con el
 * interruptor encendido. USD y la moneda principal siguen su camino de siempre
 * (Escenario 5, RN-6.11-N).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} userId
 * @param {string} currency
 * @returns {Promise<boolean>}
 */
async function monedaPocoUsadaDeAtribucion(db, userId, currency) {
  if (!currency || currency === 'USD' || !atribucionDesdeUsd()) return false;
  return esMonedaPocoUsada(currency, await monedaPrincipalDe(db, userId));
}

/**
 * ¿El error es el aviso "no disponible en esa moneda por ahora"?
 *
 * @param {*} error
 * @returns {boolean}
 */
function esAvisoMonedaNoDisponible(error) {
  return !!error && error.details?.code === CODIGO_MONEDA_NO_DISPONIBLE;
}

// ============================================================================
// TASAS DE CIERRE
// ============================================================================

/**
 * La tasa de cierre de cada fecha pedida, con la regla de los resúmenes
 * derivados (`tasasDeLaSerie`): cierre hábil anterior hasta 5 días naturales;
 * una tasa inválida deja la cifra sin calcular.
 *
 * @param {string} currency
 * @param {Array<string|null|undefined>} fechas - `YYYY-MM-DD`
 * @param {Object} [deps]
 * @param {Function} [deps.tasasDelPeriodo] - Inyectable para tests
 * @returns {Promise<{tasaDe: (fecha: string) => number|undefined,
 *   serie: {dates: string[], closes: number[]}}>} `serie` sirve como serie de la
 *   moneda en `fxSeriesByCurrency` (`utils/carveOutReturns`)
 * @throws {HttpsError} `unavailable` con `CURRENCY_UNAVAILABLE` si no hay con qué calcular
 */
async function conversorDeCierres(currency, fechas, deps = {}) {
  const unicas = [...new Set((fechas || []).filter(Boolean))].sort();
  if (unicas.length === 0) {
    return { tasaDe: () => undefined, serie: { dates: [], closes: [] } };
  }
  const obtener = deps.tasasDelPeriodo || tasasDelPeriodo;
  const cierres = await obtener(currency, unicas[0], unicas[unicas.length - 1]);
  if (!cierres) throw monedaNoDisponible(currency, 'sin-tasas');

  const { tasas, faltante, invalida } = tasasDeLaSerie(unicas.map((d) => ({ d })), cierres);
  if (invalida) throw monedaNoDisponible(currency, `tasa-invalida-${invalida}`);
  if (faltante) throw monedaNoDisponible(currency, `sin-tasa-${faltante}`);

  return {
    tasaDe: (fecha) => tasas[fecha],
    serie: { dates: unicas, closes: unicas.map((d) => tasas[d]) },
  };
}

/**
 * La tasa vigente de la moneda en el servicio de tasas (Escenario 4).
 *
 * @param {string} currency
 * @param {Object} [deps]
 * @param {Function} [deps.getExchangeRates] - Inyectable para tests
 * @returns {Promise<number>}
 * @throws {HttpsError} Sin tasa vigente válida (servicio caído, ausente o inválida)
 */
async function tasaVigente(currency, deps = {}) {
  const obtener = deps.getExchangeRates || getExchangeRates;
  const respuesta = await obtener([currency]);
  if (!respuesta) throw monedaNoDisponible(currency, 'sin-tasas');
  const cruda = respuesta.rates?.[currency];
  const tasa = typeof cruda === 'string' ? parseFloat(cruda) : cruda;
  if (typeof tasa !== 'number' || !Number.isFinite(tasa) || tasa <= 0) {
    throw monedaNoDisponible(currency, cruda === undefined ? 'sin-tasa-vigente' : 'tasa-vigente-invalida');
  }
  return tasa;
}

// ============================================================================
// CONVERSIONES PURAS
// ============================================================================

/**
 * Un día de la serie de tenencias (`{date, currencyData}` de
 * `holdingsContributions`) en USD, llevado a la otra moneda: valores × tasa;
 * las unidades no cambian. Un bloque por activo ausente sigue ausente (doc de
 * backfill que solo trae totales).
 *
 * @param {{date: string, currencyData: {totalValue: number, assetPerformance?: Object}}} dia
 * @param {number} tasa
 * @returns {{date: string, currencyData: {totalValue: number, assetPerformance?: Object}}}
 */
function convertirDiaDeTenencias(dia, tasa) {
  const { totalValue, assetPerformance } = dia.currencyData || {};
  let convertidos;
  if (assetPerformance) {
    convertidos = {};
    for (const [clave, activo] of Object.entries(assetPerformance)) {
      convertidos[clave] = { totalValue: activo.totalValue * tasa, units: activo.units };
    }
  }
  return {
    date: dia.date,
    currencyData: { totalValue: (totalValue || 0) * tasa, assetPerformance: convertidos },
  };
}

/**
 * Fecha `YYYY-MM-DD` de una transacción (texto ISO o Timestamp de Firestore).
 *
 * Es a propósito la misma regla que `contributionCalculator#transactionDateStr`
 * (día UTC, no Luxon en Nueva York): con ella `getSellTransactionsInPeriod`
 * decide en qué día del periodo cae la venta, y la venta tiene que tomar la tasa
 * de ese mismo día. No se importa de allí porque ese módulo abre Firestore al
 * cargarse, y este lo cargan las tenencias y sus tests sin Firestore.
 */
function fechaDeTransaccion(value) {
  if (typeof value === 'string') return value.slice(0, 10);
  if (value && typeof value.toDate === 'function') return value.toDate().toISOString().slice(0, 10);
  return null;
}

/**
 * Fechas de las ventas de `getSellTransactionsInPeriod`.
 *
 * @param {Object} ventasPorActivo
 * @returns {string[]}
 */
function fechasDeVentas(ventasPorActivo) {
  const fechas = [];
  for (const venta of Object.values(ventasPorActivo || {})) {
    for (const tx of venta.transactions || []) fechas.push(fechaDeTransaccion(tx.date));
  }
  return fechas.filter(Boolean);
}

/**
 * Las ventas en USD de `getSellTransactionsInPeriod`, en la otra moneda: el P&G
 * de cada venta × la tasa del día de la venta (decisión del PO del 2026-11-14).
 *
 * @param {Object} ventasPorActivo - Resultado en USD
 * @param {(fecha: string) => number} tasaDe
 * @returns {Object} La misma forma, con `pnl` y `totalRealizedPnL` convertidos
 */
function convertirVentas(ventasPorActivo, tasaDe) {
  const resultado = {};
  for (const [clave, venta] of Object.entries(ventasPorActivo || {})) {
    const transactions = (venta.transactions || []).map((tx) => ({
      ...tx,
      pnl: (tx.pnl || 0) * tasaDe(fechaDeTransaccion(tx.date)),
    }));
    resultado[clave] = {
      ...venta,
      totalRealizedPnL: transactions.reduce((suma, tx) => suma + tx.pnl, 0),
      transactions,
    };
  }
  return resultado;
}

// ============================================================================
// RENDIMIENTO DE UN RANGO DESDE LOS DÍAS EN USD
// ============================================================================

function rutaDeDias(userId, accountId) {
  return accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;
}

/** Solo la fecha y lo que se usa del bloque en USD (RN-6.11-J). */
const CAMPOS_USD = ['date', 'USD.adjustedDailyChangePercentage', 'USD.totalValue'];

function diaEnUsd(doc) {
  const data = doc.data() || {};
  const usd = data.USD || {};
  return {
    date: data.date || doc.id,
    c: usd.adjustedDailyChangePercentage || 0,
    v: usd.totalValue || 0,
  };
}

/**
 * Rendimiento compuesto de un alcance en un rango, desde los días en USD:
 * respaldo por periodo y varias cuentas con fechas (caminos 2 y 3 de la HU).
 *
 * Lee los mismos documentos que la ruta de siempre, pero solo el bloque en USD.
 * Para convertir el primer día del rango usa el último día anterior (base).
 * Compone con la regla de `calculatePeriodTWR`: los días sin cambio no cuentan.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.accountId - `'overall'` o el id de la cuenta
 * @param {string} params.currency
 * @param {string} params.startStr - `YYYY-MM-DD`
 * @param {string|null} [params.endStr] - `YYYY-MM-DD`; `null` = hasta hoy
 * @param {Object} [deps] - Inyectables para tests (`tasasDelPeriodo`)
 * @returns {Promise<{twr: number, hasData: boolean, docsCount: number, value: number}>}
 *   `value` es el valor del último día del rango en la otra moneda
 * @throws {HttpsError} Sin con qué calcular
 */
async function twrDeRangoDesdeUsd(db, params, deps = {}) {
  const { userId, accountId, currency, startStr, endStr = null } = params;
  const col = db.collection(rutaDeDias(userId, accountId));
  let rango = col.where('date', '>=', startStr);
  if (endStr) rango = rango.where('date', '<=', endStr);

  const [baseSnap, rangoSnap] = await Promise.all([
    col.where('date', '<', startStr).orderBy('date', 'desc').limit(1).select(...CAMPOS_USD).get(),
    rango.orderBy('date', 'asc').select(...CAMPOS_USD).get(),
  ]);
  if (rangoSnap.empty) return { twr: 0, hasData: false, docsCount: 0, value: 0 };

  const dias = rangoSnap.docs.map(diaEnUsd);
  const base = baseSnap.empty ? null : diaEnUsd(baseSnap.docs[0]);
  const { tasaDe } = await conversorDeCierres(currency, [base?.date, ...dias.map((d) => d.date)], deps);

  let factor = 1;
  let validos = 0;
  let previo = base;
  let valor = 0;
  for (const dia of dias) {
    const cambio = cambioConvertido(dia.c, previo?.v, tasaDe(dia.date), previo ? tasaDe(previo.date) : undefined);
    if (cambio !== 0) {
      factor *= 1 + cambio / 100;
      validos++;
    }
    valor = dia.v * tasaDe(dia.date);
    previo = dia;
  }

  const twr = (factor - 1) * 100;
  console.log(`[attributionFromUsd] ${userId}/${accountId} ${currency} ${startStr}..${endStr || 'hoy'}: ` +
    `${dias.length} días en USD, ${validos} con cambio, TWR=${twr.toFixed(2)}%`);
  return { twr, hasData: validos > 0, docsCount: dias.length, value: valor };
}

// ============================================================================
// DÍA EN CURSO DESDE USD
// ============================================================================

/**
 * El rendimiento del día en curso en la otra moneda (Escenario 4): el intradía
 * en USD (`calculateIntradayPerformance`), con el valor de hoy a la **tasa
 * vigente** y el del día anterior a la **tasa de cierre** de ese día. Sin día
 * anterior, la base (la inversión) va a la tasa vigente, como la convertía
 * `convertCurrency`.
 *
 * @param {Object} params - Los de `calculateIntradayPerformance` (`userId`, `currency`, `accountIds`)
 * @param {Object} [deps] - Inyectables para tests
 * @returns {Promise<Object>} La forma de `calculateIntradayPerformance`, más `derivedFrom: 'USD'`
 * @throws {HttpsError} Sin tasa vigente o sin cierre del día anterior
 */
async function intradiaDesdeUsd(params, deps = {}) {
  const { currency } = params;
  // Diferido: el intradía carga Firestore y el cliente de mercado.
  const calcular = deps.calculateIntradayPerformance ||
    require('./intradayCalculator').calculateIntradayPerformance;
  const usd = await calcular({ ...params, currency: 'USD' });
  if (!usd || !usd.success) return usd;

  const vigente = await tasaVigente(currency, deps);
  let tasaPrevia = vigente;
  if (usd.previousDayDate) {
    const { tasaDe } = await conversorDeCierres(currency, [usd.previousDayDate], deps);
    tasaPrevia = tasaDe(usd.previousDayDate);
  }

  const totalValue = usd.totalValue * vigente;
  const totalInvestment = usd.totalInvestment * vigente;
  const previousDayTotalValue = usd.previousDayTotalValue * tasaPrevia;
  const dailyChangeAbsolute = totalValue - previousDayTotalValue;
  // Las mismas fórmulas de `calculateIntradayPerformance` (sin flujos del día).
  const dailyChangePercent = previousDayTotalValue > 0 ? (dailyChangeAbsolute / previousDayTotalValue) * 100 : 0;
  const adjustedDailyChangePercent = dailyChangePercent;
  const todayFactor = 1 + adjustedDailyChangePercent / 100;

  console.log(`[attributionFromUsd] intradía ${params.userId} ${currency}: factor USD=${usd.todayFactor.toFixed(6)}, ` +
    `vigente=${vigente}, cierre ${usd.previousDayDate || '—'}=${tasaPrevia} → factor=${todayFactor.toFixed(6)}`);

  return {
    ...usd,
    currency,
    totalValue,
    totalInvestment,
    previousDayTotalValue,
    dailyChangeAbsolute,
    dailyChangePercent,
    adjustedDailyChangePercent,
    todayFactor,
    derivedFrom: 'USD',
  };
}

module.exports = {
  atribucionDesdeUsd,
  monedaPocoUsadaDeAtribucion,
  esAvisoMonedaNoDisponible,
  conversorDeCierres,
  tasaVigente,
  convertirDiaDeTenencias,
  fechasDeVentas,
  convertirVentas,
  twrDeRangoDesdeUsd,
  intradiaDesdeUsd,
  CAMPOS_USD,
};
