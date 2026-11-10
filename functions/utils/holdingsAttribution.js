/**
 * FIX-ATTR-HOLDINGS-001: Atribución de rendimiento por tenencias diarias.
 *
 * Responde "¿cuántos puntos del rendimiento del período aportó cada activo?"
 * con la definición estándar de contribución:
 *
 *   w_a(t)  = V_a(t-1) / Σ_b V_b(t-1)          peso al cierre del día anterior
 *   r_a(t)  = retorno de mercado del ticker entre los dos docs (+ dividendos)
 *   R(t)    = Σ_a w_a(t) · r_a(t)               retorno del portafolio en el día
 *   G(t)    = Π_{s≤t} (1 + R(s))                crecimiento acumulado
 *   c_a     = Σ_t w_a(t) · r_a(t) · G(t-1)      contribución encadenada
 *
 * Con ese encadenamiento Σ_a c_a = G(fin) - 1: la suma de contribuciones ES el
 * TWR por tenencias del período, sin repartir residuos. Los aportes y retiros
 * no generan rendimiento: cambian el peso del día siguiente, nunca el retorno
 * del día.
 *
 * Reemplaza al cálculo anterior (contribución = ganancia en $ / valor del
 * portafolio al INICIO del período), que inflaba los pesos y las
 * contribuciones de forma creciente con la longitud del período cuando el
 * portafolio crecía por aportes (medido: MSFT "pesaba" 118 % en "Todo", MELI
 * restaba 22 pp con 5 % de peso).
 *
 * Precio: retorno de mercado (cierre, convertido a la moneda del reporte) vía
 * sources.marketReturn. Si falta, se usa el precio implícito del doc
 * (valor / unidades), descartando saltos imposibles. El precio implícito NO es
 * la fuente principal: en el histórico de VUAA.L alterna +10 % / -9 % en días
 * en que el mercado se movió 1 %.
 *
 * Diferencia conocida: una compra o venta a un precio distinto del cierre deja
 * un P&L intradía que el método por tenencias no captura (se valora a cierre).
 *
 * Módulo puro, sin I/O. Lo orquesta services/attribution/holdingsContributions.js.
 *
 * @module utils/holdingsAttribution
 */

const { tickerFromAssetKey } = require('./carveOutReturns');

/** Días calendario que una cuenta puede arrastrar su último doc al fusionar cuentas. */
const MAX_CARRY_DAYS = 7;

/**
 * Salto diario máximo (%) aceptado para un precio implícito. Por encima es un
 * error de datos (precio en otra moneda, split sin ajustar), no un movimiento.
 */
const MAX_IMPLIED_DAILY_RETURN_PCT = 50;

const VALID_TYPES = new Set(['stock', 'etf', 'crypto']);

// ============================================================================
// Helpers
// ============================================================================

function daysBetween(a, b) {
  return Math.round((Date.parse(`${b}T00:00:00Z`) - Date.parse(`${a}T00:00:00Z`)) / 86400000);
}

/**
 * Entradas válidas de assetPerformance (descarta las anidadas que deja un "."
 * en la ruta de un update, ej. `VUAA: { L_etf: {...} }`).
 */
function assetEntries(assetPerformance) {
  if (!assetPerformance || typeof assetPerformance !== 'object') return [];
  return Object.entries(assetPerformance).filter(
    ([, data]) => data && typeof data === 'object' && Number.isFinite(Number(data.totalValue))
  );
}

/** Tipo de activo a partir del sufijo de la llave; null si no es reconocible. */
function typeFromAssetKey(assetKey) {
  const idx = assetKey.lastIndexOf('_');
  const type = idx > 0 ? assetKey.slice(idx + 1).toLowerCase() : '';
  return VALID_TYPES.has(type) ? type : null;
}

/** Ticker tal como lo escribe el usuario (sin normalizar mayúsculas). */
function rawTickerFromAssetKey(assetKey) {
  const idx = assetKey.lastIndexOf('_');
  return idx > 0 ? assetKey.slice(0, idx) : assetKey;
}

/**
 * ¿El doc trae el bloque por activo? Un mapa vacío SÍ es dato (la cuenta no
 * tiene nada: lo vendió todo); un bloque ausente no lo es (doc de backfill
 * que sólo trae totales) y se arrastra el día anterior.
 */
function hasAssetData(currencyData) {
  const ap = currencyData?.assetPerformance;
  return !!ap && typeof ap === 'object';
}

/**
 * Tenencias de un doc agrupadas por ticker. Varias llaves pueden ser el mismo
 * ticker (`VUAA.L_etf` y `VUAA.L_undefined`): se suman valor y unidades.
 *
 * @param {Object|undefined|null} assetPerformance
 * @returns {Map<string, {value: number, units: number, keys: string[]}>|null}
 *   null si el doc no trae el bloque por activo; Map vacío si lo trae vacío
 */
function holdingsByTicker(assetPerformance) {
  if (!assetPerformance || typeof assetPerformance !== 'object') return null;
  const entries = assetEntries(assetPerformance);
  const map = new Map();
  for (const [key, data] of entries) {
    const ticker = tickerFromAssetKey(key);
    const h = map.get(ticker) || { value: 0, units: 0, keys: [] };
    h.value += Number(data.totalValue) || 0;
    h.units += Number(data.units) || 0;
    h.keys.push(key);
    map.set(ticker, h);
  }
  return map;
}

function totalValueOf(holdings) {
  let total = 0;
  if (!holdings) return 0;
  for (const h of holdings.values()) if (h.value > 0) total += h.value;
  return total;
}

/** Retorno (%) por precio implícito valor/unidades; null si no se puede calcular. */
function impliedReturnPct(prev, cur) {
  if (!prev || !cur || prev.value <= 0 || prev.units <= 0 || cur.value <= 0 || cur.units <= 0) return null;
  const p0 = prev.value / prev.units;
  const p1 = cur.value / cur.units;
  if (!Number.isFinite(p0) || !Number.isFinite(p1) || p0 <= 0) return null;
  return (p1 / p0 - 1) * 100;
}

// ============================================================================
// Fusión de cuentas
// ============================================================================

/**
 * Une los docs diarios de varias cuentas en una sola serie por fecha.
 *
 * Todas las cuentas se suman en LA MISMA fecha (el cálculo anterior sumaba el
 * primer doc de cada cuenta, aunque fueran de fechas distintas). Si a una
 * cuenta le falta el doc de una fecha (o el doc no trae assetPerformance), se
 * arrastra su último doc válido hasta MAX_CARRY_DAYS; antes de su primer doc
 * la cuenta no aporta nada. Un assetPerformance vacío es válido: la cuenta no
 * tenía nada ese día.
 *
 * También sirve para una sola serie: normaliza la forma y salta los docs sin
 * datos por activo arrastrando el anterior.
 *
 * @param {Array<Array<{date: string, currencyData: Object|undefined}>>} daysByAccount
 *   Cada serie ordenada por fecha ascendente
 * @param {number} [maxCarryDays]
 * @returns {Array<{date: string, currencyData: {totalValue: number, assetPerformance: Object}}>}
 */
function mergeAccountDays(daysByAccount, maxCarryDays = MAX_CARRY_DAYS) {
  const series = (daysByAccount || []).filter((days) => Array.isArray(days) && days.length > 0);
  const dates = [...new Set(series.flatMap((days) => days.map((d) => d.date)))].sort();
  const pointers = series.map(() => ({ idx: 0, last: null }));
  const merged = [];

  for (const date of dates) {
    let any = false;
    let totalValue = 0;
    const assetPerformance = {};

    series.forEach((days, a) => {
      const ptr = pointers[a];
      while (ptr.idx < days.length && days[ptr.idx].date <= date) {
        const doc = days[ptr.idx];
        if (hasAssetData(doc.currencyData)) ptr.last = doc;
        ptr.idx++;
      }
      if (!ptr.last || daysBetween(ptr.last.date, date) > maxCarryDays) return;
      any = true;
      for (const [key, data] of assetEntries(ptr.last.currencyData.assetPerformance)) {
        const value = Number(data.totalValue) || 0;
        const entry = assetPerformance[key] || { totalValue: 0, units: 0 };
        entry.totalValue += value;
        entry.units += Number(data.units) || 0;
        assetPerformance[key] = entry;
        totalValue += value;
      }
    });

    if (any) merged.push({ date, currencyData: { totalValue, assetPerformance } });
  }
  return merged;
}

// ============================================================================
// Cálculo
// ============================================================================

/**
 * Contribución de cada activo al TWR por tenencias del período.
 *
 * @param {Array<{date: string, currencyData: Object}>} days - Ordenados por
 *   fecha. days[0] es la BASE: el cierre anterior al primer día del período
 *   (sus pesos son el "peso al inicio"); si no hay doc previo, el primer doc.
 * @param {Object} [sources]
 * @param {(ticker: string, d0: string, d1: string) => number|null} [sources.marketReturn]
 *   Retorno de mercado (%) del ticker normalizado entre dos fechas
 * @param {(ticker: string, d0: string, d1: string) => number} [sources.dividendCash]
 *   Dividendos cobrados en (d0, d1], en la moneda del reporte
 * @returns {Object|null} null si no hay al menos un día con datos por activo
 */
function computeHoldingsAttribution(days, sources = {}) {
  const marketReturn = sources.marketReturn || (() => null);
  const dividendCash = sources.dividendCash || (() => 0);
  if (!Array.isArray(days) || days.length === 0) return null;

  const assets = new Map();
  const ensure = (ticker) => {
    let a = assets.get(ticker);
    if (!a) {
      a = {
        ticker,
        displayTicker: ticker,
        assetKey: null,
        type: null,
        linked: 0,
        dollarChange: 0,
        exposureSum: 0,
        heldSteps: 0,
        growth: 1,
        weightStart: 0,
        valueStart: 0,
        weightEnd: 0,
        valueEnd: 0,
        firstHeldDate: null,
        lastHeldDate: null,
        sources: { market: 0, implied: 0, missing: 0 },
      };
      assets.set(ticker, a);
    }
    return a;
  };
  // La llave más reciente con tipo válido es la canónica (la que usa el resto de la app)
  const noteKeys = (holdings, date) => {
    if (!holdings) return;
    for (const [ticker, h] of holdings) {
      if (h.value <= 0) continue;
      const a = ensure(ticker);
      const typed = h.keys.find((k) => typeFromAssetKey(k)) || null;
      if (typed || !a.assetKey) {
        a.assetKey = typed || h.keys[0];
        a.displayTicker = rawTickerFromAssetKey(a.assetKey);
        a.type = typeFromAssetKey(a.assetKey) || a.type;
      }
      if (!a.firstHeldDate) a.firstHeldDate = date;
      a.lastHeldDate = date;
    }
  };

  const coverage = {
    steps: 0,
    marketReturns: 0,
    impliedReturns: 0,
    missingReturns: 0,
    discardedImpliedJumps: 0,
    missingTickers: new Set(),
    dividends: 0,
  };

  // Base: pesos y valores al inicio
  const firstWithData = days.findIndex((d) => hasAssetData(d.currencyData));
  if (firstWithData < 0) return null;
  let prev = holdingsByTicker(days[firstWithData].currencyData.assetPerformance);
  let prevDate = days[firstWithData].date;
  const startValue = totalValueOf(prev);
  for (const [ticker, h] of prev) {
    if (h.value <= 0) continue;
    const a = ensure(ticker);
    a.valueStart = h.value;
    a.weightStart = startValue > 0 ? h.value / startValue : 0;
  }
  noteKeys(prev, prevDate);

  let factor = 1;
  let last = prev;
  let lastDate = prevDate;

  for (let i = firstWithData + 1; i < days.length; i++) {
    const cur = holdingsByTicker(days[i].currencyData?.assetPerformance);
    const date = days[i].date;
    if (!cur) continue; // sin datos por activo: se compara el siguiente doc contra el último válido

    const total = totalValueOf(prev);
    if (total > 0) {
      let R = 0;
      const step = [];
      for (const [ticker, h] of prev) {
        if (h.value <= 0) continue;
        const w = h.value / total;
        let r = marketReturn(ticker, prevDate, date);
        let source = 'market';
        if (typeof r !== 'number' || !Number.isFinite(r)) {
          r = impliedReturnPct(h, cur.get(ticker));
          source = 'implied';
          if (r !== null && Math.abs(r) > MAX_IMPLIED_DAILY_RETURN_PCT) {
            coverage.discardedImpliedJumps++;
            r = null;
          }
        }
        if (r === null) {
          // Sin precio (o salió de cartera sin cotización): no aporta ese día
          r = 0;
          source = 'missing';
          coverage.missingTickers.add(ticker);
        }
        const cash = Number(dividendCash(ticker, prevDate, date)) || 0;
        coverage.dividends += cash;
        const dayReturn = r / 100 + cash / h.value;
        R += w * dayReturn;
        step.push([ticker, w, dayReturn, h.value, source]);
      }
      for (const [ticker, w, dayReturn, value, source] of step) {
        const a = ensure(ticker);
        a.linked += w * dayReturn * factor;
        a.dollarChange += value * dayReturn;
        a.exposureSum += w;
        a.heldSteps += 1;
        a.growth *= 1 + dayReturn;
        a.sources[source]++;
        if (source === 'market') coverage.marketReturns++;
        else if (source === 'implied') coverage.impliedReturns++;
        else coverage.missingReturns++;
      }
      factor *= 1 + R;
      coverage.steps++;
    }

    noteKeys(cur, date);
    prev = cur;
    prevDate = date;
    last = cur;
    lastDate = date;
  }

  const endValue = totalValueOf(last);
  const latestAssetPerformance = {};
  for (const [ticker, h] of last) {
    if (h.value <= 0) continue;
    const a = ensure(ticker);
    a.valueEnd = h.value;
    a.weightEnd = endValue > 0 ? h.value / endValue : 0;
  }
  // Para el cálculo intradía: el último cierre por llave original
  const lastDoc = days.slice().reverse().find((d) => d.date === lastDate);
  for (const [key, data] of assetEntries(lastDoc?.currencyData?.assetPerformance)) {
    if ((Number(data.totalValue) || 0) > 0) {
      latestAssetPerformance[key] = { totalValue: Number(data.totalValue) || 0, units: Number(data.units) || 0 };
    }
  }

  return {
    twr: (factor - 1) * 100,
    factor,
    baseDate: days[firstWithData].date,
    endDate: lastDate,
    startValue,
    endValue,
    latest: { totalValue: endValue, assetPerformance: latestAssetPerformance },
    assets: [...assets.values()].map((a) => ({
      ticker: a.displayTicker,
      normalizedTicker: a.ticker,
      assetKey: a.assetKey,
      type: a.type || 'stock',
      contribution: a.linked * 100,
      dollarChange: a.dollarChange,
      returnPct: a.heldSteps > 0 ? (a.growth - 1) * 100 : 0,
      avgWeight: a.heldSteps > 0 ? a.exposureSum / a.heldSteps : 0,
      heldSteps: a.heldSteps,
      weightStart: a.weightStart,
      valueStart: a.valueStart,
      weightEnd: a.weightEnd,
      valueEnd: a.valueEnd,
      firstHeldDate: a.firstHeldDate,
      lastHeldDate: a.lastHeldDate,
      sources: a.sources,
    })),
    coverage: {
      ...coverage,
      missingTickers: [...coverage.missingTickers].sort(),
    },
  };
}

module.exports = {
  MAX_CARRY_DAYS,
  MAX_IMPLIED_DAILY_RETURN_PCT,
  hasAssetData,
  typeFromAssetKey,
  holdingsByTicker,
  impliedReturnPct,
  mergeAccountDays,
  computeHoldingsAttribution,
};
