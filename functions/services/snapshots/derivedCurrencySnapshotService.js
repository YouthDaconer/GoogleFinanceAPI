/**
 * HU 6.9.2 — Los resúmenes en monedas poco usadas se derivan del USD.
 *
 * Un resumen de portafolio o de cuenta en una moneda que no es USD ni la
 * principal del inversor (RN-6.9.2-J) ya no se guarda ni se rehace leyendo la
 * historia. Se calcula al pedirlo con el resumen en USD del mismo alcance, que
 * mantiene la noche, y la tasa de cierre de cada día que da finance-query:
 * **USD × tasa del día** (decisión del PO, 2026-11-14), también para la
 * inversión y el P&G.
 *
 * - 1 lectura (el resumen en USD) y 1 llamada a finance-query, haya un mes o
 *   cinco años de historia (Escenario 5).
 * - No escribe nada, así que no hay nada que invalidar: un cambio en la historia
 *   rehace el resumen en USD y el derivado lo sigue (Escenario 3).
 * - Las cifras salen de **las mismas funciones** del cálculo desde cero
 *   (`computePortfolioReturnsFromDailyDocs`): derivar el resumen guardado da lo
 *   mismo que derivar uno hecho desde cero con los documentos diarios (RN-6-A).
 * - Un día sin cierre de divisa (festivo de Londres con la bolsa de EE. UU.
 *   abierta, como el lunes de Pascua) toma el cierre hábil anterior, hasta
 *   `MAX_LOOKBACK_DAYS` días: la misma regla de `historicalRateService` (HU #3).
 *   Más allá no hay tasa y no se deriva (RN-3-D): quien pregunta sigue su camino
 *   de siempre. El servicio no rehace nada en esa moneda: para la atribución y el
 *   riesgo eso costaría la historia entera donde hoy cuesta una lectura.
 * - `DERIVED_CURRENCY_SNAPSHOTS=false` devuelve el comportamiento de siempre (RN-6-H).
 *
 * La fórmula del cambio diario, `(1 + cUSD) · f(d) / f(d−1) − 1`, es exacta
 * porque el cierre convierte valores y flujos a la tasa del día
 * (`utils/portfolioCalculations.js#convertCurrency`). Se comprobó con datos
 * reales (p95 de 0,0000 pp donde la historia se recalculó con tasas de cierre).
 *
 * HU 6.11.1 — Sin con qué calcular, se avisa y se ofrece USD. En una moneda poco
 * usada, la lectura de rendimientos ya no vuelve a lo guardado en esa moneda
 * cuando no puede derivar: responde `monedaNoDisponible` (RN-6.11-J, RN-6.11-K).
 * Un resumen en USD sin sus listas por día se prepara de nuevo antes de derivar,
 * detrás de `CURRENCY_UNAVAILABLE_NOTICE` (RN-6-H). Una tasa recibida inválida
 * ya no es "sin con qué calcular": desde la HU 6.14.2 es un día sin cierre
 * (RN-6.14-D, ver `tasasDeLaSerie`).
 *
 * @module services/snapshots/derivedCurrencySnapshotService
 * @see platform-docs/stories/6.9.2-resumenes-poco-usados-incrementales/refinamiento.md (T2, T3)
 * @see platform-docs/stories/6.11.1-sin-tasa-aviso-y-usd/refinamiento.md (T1, T2, T3)
 */

const { DateTime } = require('luxon');
const { HttpsError } = require('firebase-functions/v2/https');
const {
  buildSnapshotDocId,
  computePortfolioReturnsFromDailyDocs,
} = require('../snapshotGenerator');
const { prepararResumenUnaVez } = require('./snapshotPreparationService');
const { SCHEMA_VERSION_INCREMENTAL } = require('../snapshotIncrementalService');
const { monedasDelCierre } = require('./assetSnapshotOnDemandService');
const { getExchangeRates } = require('../financeQuery');
const { MAX_LOOKBACK_DAYS } = require('../historicalRateService');

const ZONA = 'America/New_York';

/** La moneda principal casi nunca cambia: se recuerda unos minutos por inversor. */
const MONEDA_PRINCIPAL_TTL_MS = 10 * 60 * 1000;
const monedaPrincipalCache = new Map();

/** Las tasas de un periodo ya pedido se recuerdan una hora en la instancia. */
const TASAS_TTL_MS = 60 * 60 * 1000;
const tasasCache = new Map();

// ============================================================================
// INTERRUPTOR Y ALCANCE (T3)
// ============================================================================

/**
 * Se lee en cada llamada, como `resumenesPorActivoBajoDemanda`: basta con
 * cambiar la variable y desplegar para volver atrás (RN-6-H).
 *
 * @returns {boolean} `true` salvo `DERIVED_CURRENCY_SNAPSHOTS=false`
 */
function resumenesDerivadosDelUsd() {
  return process.env.DERIVED_CURRENCY_SNAPSHOTS !== 'false';
}

/**
 * HU 6.11.1 (T1): con el interruptor encendido, una moneda poco usada sin con qué
 * calcular responde el aviso en lugar de lo guardado. Se lee en cada llamada;
 * `CURRENCY_UNAVAILABLE_NOTICE=false` devuelve el comportamiento de la 6.9.2
 * sin apagar la derivación (RN-6-H).
 *
 * @returns {boolean} `true` salvo `CURRENCY_UNAVAILABLE_NOTICE=false`
 */
function avisoMonedaNoDisponible() {
  return process.env.CURRENCY_UNAVAILABLE_NOTICE !== 'false';
}

/** Código que el navegador reconoce en `details.code` para pintar el aviso. */
const CODIGO_MONEDA_NO_DISPONIBLE = 'CURRENCY_UNAVAILABLE';

/**
 * HU 6.11.1 (T1): el error con el que la lectura de rendimientos dice que la
 * cifra no está disponible en esa moneda por ahora (RN-6.11-K). Es un error y no
 * una respuesta vacía para que el navegador nunca lo guarde: al recargar o
 * cambiar de moneda se vuelve a intentar (Escenario 3).
 *
 * @param {string} currency
 * @param {string} motivo - El de `obtenerResumenDerivado` (`sin-tasas`, `sin-tasa-<día>`…)
 * @param {string} [id] - Id del resumen, solo para el log
 * @returns {HttpsError}
 */
function monedaNoDisponible(currency, motivo, id = currency) {
  console.log(`[currencyUnavailable] ${id}: ${motivo}`);
  return new HttpsError(
    'unavailable',
    `La cifra no está disponible en ${currency} por ahora`,
    { code: CODIGO_MONEDA_NO_DISPONIBLE, currency, motivo },
  );
}

/**
 * RN-6.9.2-J: poco usada = ni USD ni la moneda principal, las que mantiene la noche.
 *
 * @param {string} currency
 * @param {string|null|undefined} defaultCurrency
 * @returns {boolean}
 */
function esMonedaPocoUsada(currency, defaultCurrency) {
  return !monedasDelCierre(defaultCurrency).includes(currency);
}

async function monedaPrincipalDe(db, userId) {
  const recordada = monedaPrincipalCache.get(userId);
  if (recordada && Date.now() - recordada.at < MONEDA_PRINCIPAL_TTL_MS) return recordada.valor;
  const doc = await db.collection('userData').doc(userId).get();
  const valor = doc.exists ? doc.data()?.defaultCurrency || null : null;
  monedaPrincipalCache.set(userId, { valor, at: Date.now() });
  return valor;
}

// ============================================================================
// DERIVACIÓN PURA (T2)
// ============================================================================

const PERIODOS_CON_DATOS = ['hasYtdData', 'hasOneMonthData', 'hasThreeMonthData'];

/**
 * La tasa de cada día de la serie. Un día sin cierre toma el cierre hábil
 * anterior hasta `MAX_LOOKBACK_DAYS` días, como `historicalRateService`; más
 * allá no hay tasa (RN-3-D).
 *
 * Una tasa **inválida** (cero, negativa o vacía) es un día sin cierre, como un
 * festivo: toma el cierre hábil anterior y, más allá de los 5 días, la cifra
 * queda sin calcular (decisión del PO del 2026-11-16, RN-6.14-D). Es lo que ya
 * hacen finance-query (`_is_valid_rate`) y `historicalRateService`. Sustituye la
 * regla de la 6.11.1 T2 y la 6.11.2 QA-15, que la trataba como "sin con qué
 * calcular" aunque hubiera un cierre válido el día anterior.
 *
 * @returns {{tasas?: Object<string, number>, faltante?: string}}
 * @see platform-docs/stories/6.14.2-dias-tasa-corrida-recalculados/refinamiento.md (T3)
 */
function tasasDeLaSerie(timeline, cierres) {
  const valida = (dia) => typeof cierres[dia] === 'number' && Number.isFinite(cierres[dia]) && cierres[dia] > 0;

  const tasas = {};
  for (const { d } of timeline) {
    let tasa;
    for (let i = 0; tasa === undefined && i <= MAX_LOOKBACK_DAYS; i++) {
      const dia = i === 0 ? d : DateTime.fromISO(d).minus({ days: i }).toISODate();
      if (valida(dia)) tasa = cierres[dia];
    }
    if (tasa === undefined) return { faltante: d };
    tasas[d] = tasa;
  }
  return { tasas };
}

/** Cambio diario en la otra moneda; sin valor previo no hay variación que convertir. */
function cambioConvertido(cambioUsd, valorPrevioUsd, tasa, tasaPrevia) {
  if (!(valorPrevioUsd > 0) || tasaPrevia === undefined) return cambioUsd;
  return ((1 + cambioUsd / 100) * tasa / tasaPrevia - 1) * 100;
}

/**
 * Deriva un resumen a otra moneda desde el de USD y las tasas de cierre.
 *
 * Devuelve la misma forma que `generatePerformanceSnapshot`, más
 * `derivedFrom: 'USD'`.
 *
 * @param {Object} resumenUsd - Resumen en USD con `cashFlows` y `realizedPnL`
 * @param {Object<string, number>} cierres - Cierres de finance-query: `{ 'YYYY-MM-DD': unidades de la moneda por 1 USD }`
 * @param {string} currency
 * @param {Object} [options]
 * @param {DateTime} [options.now] - "Ahora" de los periodos, como en el desde cero
 * @returns {{snapshot: Object|null, motivo?: string}} `snapshot` en `null` si no se puede derivar
 * @see platform-docs/stories/6.9.2-resumenes-poco-usados-incrementales/refinamiento.md (T2)
 */
function derivarResumenDeMoneda(resumenUsd, cierres, currency, options = {}) {
  const timelineUsd = resumenUsd?.timeline || [];
  if (timelineUsd.length === 0) return { snapshot: null, motivo: 'usd-sin-serie' };
  if (!Array.isArray(resumenUsd.cashFlows) || !Array.isArray(resumenUsd.realizedPnL)) {
    return { snapshot: null, motivo: 'usd-sin-listas' };
  }

  const { tasas, faltante } = tasasDeLaSerie(timelineUsd, cierres);
  if (faltante) return { snapshot: null, motivo: `sin-tasa-${faltante}` };

  const now = options.now || DateTime.now().setZone(ZONA);
  const flujos = new Map(resumenUsd.cashFlows.map((x) => [x.d, x.f]));

  // 1. Serie: valor a la tasa del día y cambio diario convertido.
  const timeline = timelineUsd.map((p, i) => {
    const previo = timelineUsd[i - 1];
    return {
      d: p.d,
      v: p.v * tasas[p.d],
      c: cambioConvertido(p.c, previo?.v, tasas[p.d], previo ? tasas[previo.d] : undefined),
    };
  });

  // 2. Periodos y rendimiento personal: la misma función del desde cero, con
  //    los campos que lee (valor, cambio y flujo del día en la otra moneda).
  const documentos = timeline.map((p) => ({
    date: p.d,
    [currency]: {
      totalValue: p.v,
      adjustedDailyChangePercentage: p.c,
      dailyChangePercentage: p.c,
      totalCashFlow: (flujos.get(p.d) || 0) * tasas[p.d],
    },
  }));
  const calculado = computePortfolioReturnsFromDailyDocs(documentos, currency, now);
  if (!calculado || !calculado.returns || !PERIODOS_CON_DATOS.some((k) => calculado.returns[k])) {
    return { snapshot: null, motivo: 'sin-datos' };
  }

  const snapshot = {
    userId: resumenUsd.userId,
    currency,
    accountId: resumenUsd.accountId,
    lastUpdated: new Date().toISOString(),
    schemaVersion: SCHEMA_VERSION_INCREMENTAL,
    returns: calculado.returns,
    timeline,
    performanceByYear: calculado.performanceByYear || {},
    monthlyCompound: derivarMonthlyCompound(resumenUsd, timeline, tasas, now),
    validDocsCountByPeriod: calculado.validDocsCountByPeriod || {},
    availableYears: calculado.availableYears || [],
    startDate: calculado.startDate || '',
    latestAssetPerformance: derivarLatestAssetPerformance(resumenUsd.latestAssetPerformance, timelineUsd, tasas),
    lastDateInTimeline: timeline[timeline.length - 1].d,
    derivedFrom: 'USD',
  };
  return { snapshot };
}

/**
 * El P&G por mes en la otra moneda, con las reglas de
 * `buildMonthlyCompoundFromDailyDocs`: inicio en el primer día del mes, cierre
 * en el último, P&G realizado como suma de los días y rendimiento compuesto de
 * la serie. Cada importe va a la tasa de su día.
 */
function derivarMonthlyCompound(resumenUsd, timeline, tasas, now) {
  const mesActual = now.toFormat('yyyy-MM');
  const diasPorMes = new Map();
  for (const p of timeline) {
    const mes = p.d.substring(0, 7);
    if (!diasPorMes.has(mes)) diasPorMes.set(mes, []);
    diasPorMes.get(mes).push(p);
  }
  const realizadoPorMes = new Map();
  for (const { d, p } of resumenUsd.realizedPnL) {
    const mes = d.substring(0, 7);
    realizadoPorMes.set(mes, (realizadoPorMes.get(mes) || 0) + p * (tasas[d] || 0));
  }

  const resultado = {};
  for (const [anio, meses] of Object.entries(resumenUsd.monthlyCompound || {})) {
    for (const [mesNum, usd] of Object.entries(meses)) {
      const mes = `${anio}-${String(mesNum).padStart(2, '0')}`;
      const dias = diasPorMes.get(mes);
      if (!dias || dias.length === 0) continue;
      const tasaInicio = tasas[dias[0].d];
      const tasaFin = tasas[dias[dias.length - 1].d];

      let factor = 1;
      for (const p of dias) factor *= (1 + p.c / 100);

      const realizado = realizadoPorMes.get(mes) || 0;
      const noRealizado = (usd.unrealizedProfitAndLoss ?? 0) * tasaFin;
      if (!resultado[anio]) resultado[anio] = {};
      resultado[anio][mesNum] = {
        returnPct: (factor - 1) * 100,
        startTotalValue: (usd.startTotalValue ?? 0) * tasaInicio,
        startTotalInvestment: (usd.startTotalInvestment ?? 0) * tasaInicio,
        endTotalValue: (usd.endTotalValue ?? 0) * tasaFin,
        endTotalInvestment: (usd.endTotalInvestment ?? 0) * tasaFin,
        totalCashFlow: (usd.totalCashFlow ?? 0) * tasaFin,
        profit: realizado + noRealizado,
        doneProfitAndLoss: realizado,
        unrealizedProfitAndLoss: noRealizado,
        lastDayOfMonth: mes < mesActual,
      };
    }
  }
  return resultado;
}

/** El detalle por activo del último día, a la tasa de ese día. El ROI no cambia. */
function derivarLatestAssetPerformance(latestUsd, timelineUsd, tasas) {
  const ultimo = timelineUsd[timelineUsd.length - 1];
  const previo = timelineUsd[timelineUsd.length - 2];
  const tasa = tasas[ultimo.d];
  const tasaPrevia = previo ? tasas[previo.d] : undefined;
  const resultado = {};
  for (const [clave, activo] of Object.entries(latestUsd || {})) {
    resultado[clave] = {
      totalValue: (activo.totalValue ?? 0) * tasa,
      totalInvestment: (activo.totalInvestment ?? 0) * tasa,
      units: activo.units ?? 0,
      unrealizedPnL: (activo.unrealizedPnL ?? 0) * tasa,
      totalROI: activo.totalROI ?? 0,
      dailyChangePercentage: tasaPrevia === undefined
        ? (activo.dailyChangePercentage ?? 0)
        : ((1 + (activo.dailyChangePercentage ?? 0) / 100) * tasa / tasaPrevia - 1) * 100,
    };
  }
  return resultado;
}

// ============================================================================
// OBTENER EL DERIVADO (T3)
// ============================================================================

/**
 * Tasas de cierre de una moneda en un periodo, en una llamada a finance-query.
 * El inicio se lleva al 1 de enero de su año: inversores con historia del mismo
 * año comparten la consulta (y la caché de 24 h de finance-query).
 *
 * @returns {Promise<Object<string, number>|null>} `null` si el canal no responde
 */
async function tasasDelPeriodo(currency, desde, hasta) {
  // Unos días antes del primero, para el cierre hábil anterior de un primer día festivo.
  const conMargen = DateTime.fromISO(desde).minus({ days: MAX_LOOKBACK_DAYS + 2 }).toISODate();
  const inicio = `${conMargen.substring(0, 4)}-01-01`;
  const clave = `${currency}|${inicio}|${hasta}`;
  const recordada = tasasCache.get(clave);
  if (recordada && Date.now() - recordada.at < TASAS_TTL_MS) return recordada.tasas;

  const respuesta = await getExchangeRates([currency], inicio, hasta);
  const tasas = respuesta?.rates?.[currency] || null;
  if (tasas) tasasCache.set(clave, { tasas, at: Date.now() });
  return tasas;
}

/**
 * El resumen en USD del alcance, al día con la señal del inversor. Si falta o
 * está viejo, solo se prepara (`generatePerformanceSnapshot`) cuando quien
 * pregunta lo habría preparado igual: la lectura de rendimientos. La atribución
 * y el riesgo nunca preparaban resúmenes, y no empiezan a hacerlo aquí.
 *
 * HU 6.11.1 (T3, Escenario 4): un resumen en USD al día pero sin sus listas por
 * día (`cashFlows`, `realizedPnL`) también se prepara de nuevo, con la misma
 * función, en lugar de dar la moneda poco usada por no derivable. La
 * preparación lo guarda con sus listas: la siguiente consulta ya no lo prepara.
 */
async function resumenUsdAlDia(db, userId, accountId, senal, prepararUsd) {
  const id = buildSnapshotDocId(userId, accountId, 'USD');
  const doc = await db.doc(`performanceSnapshots/${id}`).get();
  const data = doc.exists ? doc.data() : null;
  const viejo = data && senal && data.lastUpdated &&
    new Date(senal).getTime() > new Date(data.lastUpdated).getTime();
  const sinListas = data && !(Array.isArray(data.cashFlows) && Array.isArray(data.realizedPnL));
  const prepararSinListas = sinListas && prepararUsd && avisoMonedaNoDisponible();
  if (data && !viejo && !prepararSinListas) return data;
  if (!prepararUsd) return null;
  // HU 6.11.2 (T2): consultas simultáneas del mismo USD viejo lo preparan una vez.
  return (await prepararResumenUnaVez(db, userId, accountId, 'USD')) || null;
}

/**
 * El resumen de un alcance en una moneda poco usada, derivado del USD.
 *
 * - Si la moneda no es poco usada devuelve `metodo: 'no-aplica'` y
 *   `snapshot: null`.
 * - Si no se puede derivar (sin tasa de un día, resumen en USD sin listas…)
 *   devuelve `metodo: 'no-derivable'` con el motivo y `snapshot: null`.
 *
 * En los dos casos quien llama sigue **su** camino de siempre: la lectura de
 * rendimientos rehace el resumen como antes; la atribución y el riesgo leen el
 * guardado. Así nada cuesta más que antes de esta HU.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.accountId - `'overall'` o el id de la cuenta
 * @param {string} params.currency
 * @param {string|null} [params.senal] - `lastSnapshotUpdate` del inversor (ISO)
 * @param {boolean} [params.prepararUsd=false] - Preparar el resumen en USD si
 *   falta o está viejo; solo la lectura de rendimientos, que ya lo hacía
 * @returns {Promise<{snapshot: Object|null, metodo: 'derivado'|'no-derivable'|'no-aplica', motivo?: string}>}
 */
async function obtenerResumenDerivado(db, params) {
  const { userId, accountId, currency, senal = null, prepararUsd = false } = params;
  if (currency === 'USD') return { snapshot: null, metodo: 'no-aplica' };

  const defaultCurrency = await monedaPrincipalDe(db, userId);
  if (!esMonedaPocoUsada(currency, defaultCurrency)) return { snapshot: null, metodo: 'no-aplica' };

  const id = buildSnapshotDocId(userId, accountId, currency);
  const inicio = Date.now();
  const terminar = (resultado, dias) => {
    const motivo = resultado.motivo ? ` (${resultado.motivo})` : '';
    console.log(`[derivedCurrencySnapshot] ${id}: ${resultado.metodo}${motivo}, ${dias} días, ${Date.now() - inicio} ms`);
    return resultado;
  };
  const noDerivable = (motivo, dias = 0) => terminar({ snapshot: null, metodo: 'no-derivable', motivo }, dias);

  const resumenUsd = await resumenUsdAlDia(db, userId, accountId, senal, prepararUsd);
  if (!resumenUsd || !resumenUsd.timeline?.length) return noDerivable('sin-resumen-usd-al-dia');
  if (!Array.isArray(resumenUsd.cashFlows) || !Array.isArray(resumenUsd.realizedPnL)) {
    return noDerivable('usd-sin-listas', resumenUsd.timeline.length);
  }

  const serie = resumenUsd.timeline;
  const tasas = await tasasDelPeriodo(currency, serie[0].d, serie[serie.length - 1].d);
  if (!tasas) return noDerivable('sin-tasas', serie.length);

  const { snapshot, motivo } = derivarResumenDeMoneda(resumenUsd, tasas, currency);
  if (!snapshot) return noDerivable(motivo, serie.length);
  return terminar({ snapshot, metodo: 'derivado' }, serie.length);
}

/** Solo para tests: olvida lo recordado en la instancia. */
function limpiarCachesDerivados() {
  monedaPrincipalCache.clear();
  tasasCache.clear();
}

module.exports = {
  resumenesDerivadosDelUsd,
  avisoMonedaNoDisponible,
  monedaNoDisponible,
  CODIGO_MONEDA_NO_DISPONIBLE,
  esMonedaPocoUsada,
  // HU 6.11.2: el Copiloto pregunta si su moneda es poco usada con la misma caché.
  // HU 6.11.5: la cabecera del día anterior usa la misma moneda principal y
  // la misma regla de los 5 días que los resúmenes derivados.
  monedaPrincipalDe,
  tasasDeLaSerie,
  // HU 6.11.3 (activo, Versus, riesgo) y 6.11.4 (atribución) convierten el cambio diario con la misma fórmula.
  cambioConvertido,
  derivarResumenDeMoneda,
  obtenerResumenDerivado,
  tasasDelPeriodo,
  limpiarCachesDerivados,
};
