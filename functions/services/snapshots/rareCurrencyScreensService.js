/**
 * HU 6.11.3 — Detalle de activo, comparación y riesgo en monedas poco usadas desde USD.
 *
 * En una moneda poco usada (ni USD ni la principal del inversor, RN-6.9.2-J) estas
 * pantallas ya no leen lo guardado en esa moneda (RN-6.11-J). Leen los días en
 * **USD**, los convierten con la regla de la 6.9.2 (USD × tasa de cierre del día,
 * también la inversión y el P&G; decisión del PO del 2026-11-14) y pasan el
 * resultado por **las mismas funciones** que hoy calculan esas cifras. Calcular
 * desde USD y calcular desde cero son así la misma cuenta (RN-6-A).
 *
 * - Un día sin cierre toma el cierre hábil anterior hasta 5 días naturales; una
 *   tasa inválida o un hueco mayor dejan la cifra sin calcular (`tasasDeLaSerie`,
 *   RN-6.11-L). Quien llama responde entonces el aviso de la 6.11.1 (RN-6.11-K).
 * - La tasa solo se exige en los días que la cifra usa: el detalle de un activo
 *   no se queda sin calcular por un día sin tasa anterior a su compra (QA-08).
 * - `RARE_CURRENCY_SCREENS_FROM_USD=false` devuelve el comportamiento de antes en
 *   las tres pantallas, sin apagar la 6.9.2 ni la 6.11.1 (RN-6-H, Escenario 7).
 *
 * @module services/snapshots/rareCurrencyScreensService
 * @see platform-docs/stories/6.11.3-activo-comparacion-riesgo-usd/refinamiento.md (T1, T2)
 */

const {
  resumenesDerivadosDelUsd,
  esMonedaPocoUsada,
  monedaPrincipalDe,
  tasasDeLaSerie,
  tasasDelPeriodo,
  cambioConvertido,
} = require('./derivedCurrencySnapshotService');
const { fetchAssetDailyDocs, armarResumenDeActivo } = require('../snapshotGenerator');

/** Importes de un día (portafolio, cuenta o activo): van a la tasa del día. */
const CAMPOS_IMPORTE = ['totalValue', 'totalInvestment', 'totalCashFlow', 'doneProfitAndLoss', 'unrealizedProfitAndLoss', 'unrealizedPnL'];

/** Cambios diarios en %: se convierten con `cambioConvertido`. */
const CAMPOS_CAMBIO = ['adjustedDailyChangePercentage', 'dailyChangePercentage', 'rawDailyChangePercentage'];

// ============================================================================
// INTERRUPTOR Y ALCANCE
// ============================================================================

/**
 * Se lee en cada llamada, como los interruptores de la 6.9.2 y la 6.11.1: basta
 * con cambiar la variable y desplegar para volver atrás (RN-6-H).
 *
 * @returns {boolean} `true` salvo `RARE_CURRENCY_SCREENS_FROM_USD=false`
 */
function pantallasDesdeUsd() {
  return process.env.RARE_CURRENCY_SCREENS_FROM_USD !== 'false';
}

/**
 * ¿Esta consulta se calcula desde USD? Solo en una moneda poco usada y con el
 * interruptor encendido. En USD y en la moneda principal nada cambia (Escenario 6).
 *
 * Exige también el de la 6.9.2 (`DERIVED_CURRENCY_SNAPSHOTS`), sobre la que se
 * construye esta HU: apagarla devuelve todo lo derivado del USD a lo de antes, y
 * el riesgo, que con ella apagada lee el resumen guardado, no puede ir desde USD.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} userId
 * @param {string} currency
 * @returns {Promise<boolean>}
 */
async function calculaDesdeUsd(db, userId, currency) {
  if (!currency || currency === 'USD' || !pantallasDesdeUsd() || !resumenesDerivadosDelUsd()) return false;
  return esMonedaPocoUsada(currency, await monedaPrincipalDe(db, userId));
}

// ============================================================================
// CONVERSIÓN PURA (T1)
// ============================================================================

const datosDe = (dia) => (dia && typeof dia.data === 'function' ? dia.data() : dia);

/**
 * Convierte un bloque de un día (el de la moneda, o el de un activo) a otra
 * moneda. Los importes van a la tasa del día; los cambios diarios con la fórmula
 * de la 6.9.2 contra el valor y la tasa del día anterior; las unidades, el ROI y
 * los rendimientos en % no cambian.
 */
function convertirBloque(bloque, tasa, valorPrevioUsd, tasaPrevia) {
  const convertido = { ...bloque };
  for (const campo of CAMPOS_IMPORTE) {
    if (typeof bloque[campo] === 'number') convertido[campo] = bloque[campo] * tasa;
  }
  for (const campo of CAMPOS_CAMBIO) {
    if (typeof bloque[campo] === 'number') {
      convertido[campo] = cambioConvertido(bloque[campo], valorPrevioUsd, tasa, tasaPrevia);
    }
  }
  return convertido;
}

/**
 * Convierte días en USD (`{ date, USD: {…} }`, o snapshots de Firestore) a la
 * moneda poco usada, con los cierres de finance-query.
 *
 * - Sin `claveActivo` (Versus) la tasa se exige en todos los días.
 * - Con `claveActivo` (detalle de un activo) solo en los días en que el activo
 *   aparece. Los demás quedan con el bloque de la moneda vacío: la marca de
 *   presencia con la que `buildAssetDailyTimeline` y la agregación por cuentas
 *   ven la venta y la recompra igual que en USD (Escenario 5).
 *
 * @param {Array} dias - En orden de fecha
 * @param {string} currency
 * @param {Object<string, number>} cierres - `{ 'YYYY-MM-DD': unidades por 1 USD }`
 * @param {Object} [opciones]
 * @param {string} [opciones.claveActivo] - `${ticker}_${assetType}`
 * @returns {{dias?: Object[], motivo?: string}} `motivo` si falta o es inválida una tasa
 */
function convertirDiasDesdeUsd(dias, currency, cierres, opciones = {}) {
  const { claveActivo } = opciones;
  const planos = dias.map(datosDe);
  const usaTasa = (d) => !!d?.USD && (!claveActivo || !!d.USD.assetPerformance?.[claveActivo]);

  const { tasas, faltante, invalida } = tasasDeLaSerie(planos.filter(usaTasa).map((d) => ({ d: d.date })), cierres);
  if (invalida) return { motivo: `tasa-invalida-${invalida}` };
  if (faltante) return { motivo: `sin-tasa-${faltante}` };

  const convertidos = planos.map((d, i) => {
    if (!d?.USD) return { date: d?.date };
    if (!usaTasa(d)) return { date: d.date, [currency]: {} };

    const previo = planos[i - 1];
    const tasaPrevia = previo && usaTasa(previo) ? tasas[previo.date] : undefined;
    const tasa = tasas[d.date];
    const { assetPerformance, ...totales } = d.USD;
    const bloque = convertirBloque(totales, tasa, previo?.USD?.totalValue, tasaPrevia);

    if (assetPerformance) {
      bloque.assetPerformance = {};
      for (const [clave, activo] of Object.entries(assetPerformance)) {
        if (claveActivo && clave !== claveActivo) continue;
        const valorPrevio = previo?.USD?.assetPerformance?.[clave]?.totalValue;
        bloque.assetPerformance[clave] = convertirBloque(activo, tasa, valorPrevio, tasaPrevia);
      }
    }
    return { date: d.date, [currency]: bloque };
  });
  return { dias: convertidos };
}

/**
 * Las tasas de cierre que necesitan unos días y su conversión, en una llamada a
 * finance-query (la de `tasasDelPeriodo`, con su caché de una hora).
 *
 * @returns {Promise<{dias?: Object[], motivo?: string}>} `motivo: 'sin-tasas'` si el servicio no responde
 */
async function diasEnMonedaDesdeUsd(dias, currency, opciones = {}) {
  const fechas = dias.map(datosDe).filter((d) => d?.USD).map((d) => d.date);
  if (fechas.length === 0) return { dias: dias.map(datosDe) };
  const cierres = await tasasDelPeriodo(currency, fechas[0], fechas[fechas.length - 1]);
  if (!cierres) return { motivo: 'sin-tasas' };
  return convertirDiasDesdeUsd(dias, currency, cierres, opciones);
}

// ============================================================================
// DETALLE DE ACTIVO (T2)
// ============================================================================

/**
 * El resumen de un activo en una moneda poco usada, desde sus días en USD.
 *
 * Lee solo los campos del activo en USD (`fetchAssetDailyDocs`, HU 6.4) y corre
 * el mismo cálculo que `generateAssetSnapshot`, sin escribir nada. Hoy, en una
 * moneda que no mantiene la noche, ese resumen se rehacía entero con lo guardado
 * en esa moneda en cada primera apertura tras un cierre: leer los días en USD
 * cuesta lo mismo (decisión 1 del refinamiento).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} params - `{ userId, accountId, currency, ticker, assetType }`
 * @returns {Promise<{snapshot: Object|null, metodo: 'derivado'|'no-derivable'|'sin-datos', motivo?: string}>}
 */
async function resumenDeActivoDesdeUsd(db, params) {
  const { userId, accountId, currency, ticker, assetType } = params;
  const claveActivo = `${ticker}_${assetType}`;
  const inicio = Date.now();
  const terminar = (resultado, dias) => {
    const motivo = resultado.motivo ? ` (${resultado.motivo})` : '';
    console.log(`[rareCurrencyScreens] activo ${userId}/${accountId}/${claveActivo} ${currency}: ${resultado.metodo}${motivo}, ${dias} días en USD, ${Date.now() - inicio} ms`);
    return resultado;
  };

  const dias = await fetchAssetDailyDocs(db, userId, accountId, 'USD', ticker, assetType);
  const conversion = await diasEnMonedaDesdeUsd(dias, currency, { claveActivo });
  if (conversion.motivo) return terminar({ snapshot: null, metodo: 'no-derivable', motivo: conversion.motivo }, dias.length);

  const snapshot = armarResumenDeActivo(conversion.dias, { userId, accountId, currency, ticker, assetType });
  if (!snapshot) return terminar({ snapshot: null, metodo: 'sin-datos' }, dias.length);
  return terminar({ snapshot: { ...snapshot, derivedFrom: 'USD' }, metodo: 'derivado' }, dias.length);
}

module.exports = {
  pantallasDesdeUsd,
  calculaDesdeUsd,
  convertirDiasDesdeUsd,
  diasEnMonedaDesdeUsd,
  resumenDeActivoDesdeUsd,
};
