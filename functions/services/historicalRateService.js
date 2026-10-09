/**
 * historicalRateService.js
 *
 * Tasa de cambio de una fecha concreta, servida como consulta al canal de datos
 * de mercado.
 *
 * HU 2.1 lo creó sobre la caché persistente `historicalExchangeRates/{fecha}`.
 * HU #3 retira esa caché: ninguna tasa se guarda (RN-3-A). Lo que hace inmutable
 * el pasado no es el archivo de tasas sino el propio movimiento, donde la épica
 * #2 escribe `acquisitionRate` / `realizationRate` en el momento de registrarlo
 * (RN-3-B). Retirar el archivo no puede alterar ninguna cifra ya registrada.
 *
 * Tres cosas cambian por dentro y ninguna por fuera:
 *
 * 1. **Las tasas se piden por rango, no por día** (RN-3-C). El lookback de días
 *    no hábiles, que antes costaba hasta 5 consultas encadenadas, entra en la
 *    misma llamada: se pide `[fecha − MAX_LOOKBACK_DAYS, fecha]` de una vez.
 * 2. **Todo llega por el canal de mercado**, el mismo que sirve los precios de
 *    los activos, con su autenticación de Yahoo ya resuelta (RN-3-E). Este
 *    módulo ya no habla con Yahoo por su cuenta.
 * 3. **Lo único que se conserva es memoria del proceso**, con vida corta y
 *    acotada. Una tasa recordada dentro de la misma invocación evita que dos
 *    cifras de la misma operación salgan distintas; una tasa guardada en la base
 *    de datos envejece en silencio, que es el problema de partida.
 *
 * Se mantiene la diferencia deliberada frente al backfill: aquí **nunca** se cae
 * a la tasa vigente cuando no hay dato de la fecha. Una tasa de adquisición
 * declarada por el usuario con un número inventado dejaría un saldo con un costo
 * falso (RN-05: sin tipo de cambio no hay movimiento).
 *
 * @module services/historicalRateService
 * @see platform-docs/stories/3-tasa-vigente-canal-mercado/refinamiento.md (T4, D4, D5, D6)
 * @see platform-docs/stories/2.1-base-costo-saldo-efectivo/refinamiento.md (D4, D5)
 */

const { getExchangeRates } = require('./financeQuery');

// ============================================================================
// CONFIGURATION
// ============================================================================

/** Días naturales hacia atrás que se aceptan cuando la fecha pedida no cotiza */
const MAX_LOOKBACK_DAYS = 5;

/**
 * Origen de la tasa devuelta, para que la interfaz pueda explicarse.
 *
 * `CACHE` conserva su valor histórico: sigue significando "no se acaba de pedir
 * al mercado", sólo que ahora la memoria es la del proceso y no un documento.
 */
const RATE_SOURCES = {
  CACHE: 'cache',
  YAHOO: 'yahoo',
  PREVIOUS_CLOSE: 'previous-close',
};

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/** Vida de una tasa recordada, en milisegundos. Corta y acotada (RN-3-A) */
const MEMORY_TTL_MS = 5 * 60 * 1000;

/** Tope de fechas recordadas por divisa, para no crecer sin límite en caliente */
const MAX_DATES_PER_CURRENCY = 800;

// ============================================================================
// MEMORIA DEL PROCESO
// ============================================================================

/**
 * `código de divisa → { rates: Map<fecha, tasa>, covered: [{from, to}], expiresAt }`
 *
 * `covered` es lo que permite distinguir "ese día no cotizó" de "ese día no se
 * ha preguntado": sin ese registro habría que volver a llamar por cada fin de
 * semana, que es justo el coste que esta historia elimina.
 */
const memory = new Map();

/** Vacía la memoria. Existe para los tests y para un reinicio explícito. */
function resetMemory() {
  memory.clear();
}

function getEntry(currency) {
  const entry = memory.get(currency);

  if (entry && entry.expiresAt <= Date.now()) {
    memory.delete(currency);
    return null;
  }

  return entry || null;
}

function ensureEntry(currency) {
  const existing = getEntry(currency);
  if (existing) return existing;

  const entry = { rates: new Map(), covered: [], expiresAt: Date.now() + MEMORY_TTL_MS };
  memory.set(currency, entry);
  return entry;
}

/** Fusiona un intervalo nuevo con los ya cubiertos, uniendo los que se tocan. */
function addCoverage(entry, from, to) {
  const merged = [];
  let start = from;
  let end = to;

  for (const range of entry.covered) {
    // Adyacente cuenta como solapado: [1-5] y [6-9] cubren [1-9] sin huecos
    if (range.to < shiftDate(start, 1) || range.from > shiftDate(end, -1)) {
      merged.push(range);
    } else {
      if (range.from < start) start = range.from;
      if (range.to > end) end = range.to;
    }
  }

  merged.push({ from: start, to: end });
  merged.sort((a, b) => (a.from < b.from ? -1 : 1));
  entry.covered = merged;
}

function isCovered(entry, from, to) {
  return entry.covered.some((range) => range.from <= from && range.to >= to);
}

function rememberSeries(currency, series, from, to) {
  const entry = ensureEntry(currency);

  for (const [date, rate] of Object.entries(series || {})) {
    if (typeof rate === 'number' && Number.isFinite(rate) && rate > 0) {
      entry.rates.set(date, rate);
    }
  }

  addCoverage(entry, from, to);

  // Tope por divisa: se descartan las fechas más antiguas, que son las que menos
  // se vuelven a pedir, y con ellas la cobertura que ya no se puede sostener.
  if (entry.rates.size > MAX_DATES_PER_CURRENCY) {
    const ordered = [...entry.rates.keys()].sort();
    const excess = entry.rates.size - MAX_DATES_PER_CURRENCY;
    for (const date of ordered.slice(0, excess)) {
      entry.rates.delete(date);
    }
    const oldestKept = ordered[excess];
    entry.covered = entry.covered
      .map((range) => (range.to < oldestKept ? null : { from: range.from < oldestKept ? oldestKept : range.from, to: range.to }))
      .filter(Boolean);
  }
}

// ============================================================================
// HELPERS
// ============================================================================

/**
 * Resta días naturales a una fecha ISO sin arrastrar zona horaria.
 * @param {string} date - Fecha ISO (YYYY-MM-DD)
 * @param {number} days - Días a restar
 * @returns {string} Fecha ISO resultante
 */
function shiftDate(date, days) {
  const d = new Date(`${date}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().substring(0, 10);
}

/**
 * Valida el formato de una fecha ISO.
 * @param {string} date - Fecha a validar
 * @returns {boolean}
 */
function isValidDate(date) {
  return typeof date === 'string' && DATE_PATTERN.test(date) && !Number.isNaN(Date.parse(date));
}

/**
 * Pide al canal de mercado las divisas que aún no están cubiertas en el rango.
 *
 * Una sola llamada para todas las divisas que falten: el coste crece con el
 * número de divisas, nunca con el de días (RN-3-C).
 *
 * @param {string[]} currencies - Códigos de divisa
 * @param {string} from - Primer día del rango (YYYY-MM-DD)
 * @param {string} to - Último día del rango (YYYY-MM-DD)
 * @returns {Promise<void>}
 */
async function warmRange(currencies, from, to) {
  const missing = currencies.filter((currency) => {
    if (currency === 'USD') return false;
    const entry = getEntry(currency);
    return !entry || !isCovered(entry, from, to);
  });

  if (missing.length === 0) return;

  let response;
  try {
    response = await getExchangeRates(missing, from, to);
  } catch (error) {
    console.warn(`[historicalRateService] El canal de mercado no respondió para ${missing.join(',')} en [${from}, ${to}]:`, error.message);
    return;
  }

  if (!response || !response.rates) {
    console.warn(`[historicalRateService] Sin tasas del canal para ${missing.join(',')} en [${from}, ${to}]`);
    return;
  }

  for (const currency of missing) {
    const series = response.rates[currency];
    if (series && Object.keys(series).length > 0) {
      rememberSeries(currency, series, from, to);
    }
  }
}

/**
 * Busca en memoria la tasa de una fecha, o la del cierre anterior más cercano.
 *
 * @param {string} currency - Código de divisa
 * @param {string} date - Fecha ISO
 * @returns {{rate: number, rateDate: string}|null}
 */
function findInMemory(currency, date) {
  const entry = getEntry(currency);
  if (!entry) return null;

  const exact = entry.rates.get(date);
  if (exact !== undefined) {
    return { rate: exact, rateDate: date };
  }

  for (let i = 1; i <= MAX_LOOKBACK_DAYS; i++) {
    const previousDate = shiftDate(date, i);
    const previous = entry.rates.get(previousDate);
    if (previous !== undefined) {
      return { rate: previous, rateDate: previousDate };
    }
  }

  return null;
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Obtiene los cierres de un período para varias divisas, en base USD.
 *
 * Es la forma que sustituye a la caché por fecha: un año de histórico con tres
 * divisas cuesta tres llamadas, no una por día y divisa (RN-3-C).
 *
 * @param {string[]} currencies - Códigos de divisa
 * @param {string} start - Primer día (YYYY-MM-DD)
 * @param {string} end - Último día (YYYY-MM-DD)
 * @returns {Promise<Object>} `{ 'YYYY-MM-DD': { USD: 1, COP: N, ... } }` — sólo
 *   las fechas con cotización; los días sin cierre se resuelven al anterior
 */
async function getRatesForRange(currencies, start, end) {
  if (!isValidDate(start) || !isValidDate(end)) {
    throw new Error(`Rango inválido: ${start} → ${end}`);
  }

  const codes = [...new Set((currencies || []).filter((c) => c && c !== 'USD'))];

  if (codes.length === 0) {
    return {};
  }

  // El margen hacia atrás permite resolver un primer día no hábil sin una
  // segunda llamada (D5).
  const from = shiftDate(start, MAX_LOOKBACK_DAYS + 2);
  await warmRange(codes, from, end);

  const byDate = {};

  for (const currency of codes) {
    const entry = getEntry(currency);
    if (!entry) continue;

    for (const [date, rate] of entry.rates) {
      if (date < start || date > end) continue;
      if (!byDate[date]) byDate[date] = { USD: 1 };
      byDate[date][currency] = rate;
    }
  }

  return byDate;
}

/**
 * Obtiene la tasa de una divisa para una fecha, en base USD.
 *
 * Cadena: memoria del proceso → canal de mercado por rango → día hábil anterior
 * dentro del mismo rango (hasta MAX_LOOKBACK_DAYS) → null.
 * Sin fallback a la tasa vigente (RN-05, RN-3-D).
 *
 * @param {string} currency - Código de divisa
 * @param {string} date - Fecha ISO (YYYY-MM-DD)
 * @returns {Promise<{rate: number, rateDate: string, source: string}|null>}
 */
async function getRateForDate(currency, date) {
  if (!isValidDate(date)) {
    throw new Error(`Fecha inválida: ${date}`);
  }

  if (currency === 'USD') {
    return { rate: 1, rateDate: date, source: RATE_SOURCES.CACHE };
  }

  const remembered = findInMemory(currency, date);
  if (remembered) {
    return {
      rate: remembered.rate,
      rateDate: remembered.rateDate,
      source: remembered.rateDate === date ? RATE_SOURCES.CACHE : RATE_SOURCES.PREVIOUS_CLOSE,
    };
  }

  // Una sola llamada cubre la fecha pedida y su lookback completo (D5)
  await warmRange([currency], shiftDate(date, MAX_LOOKBACK_DAYS + 2), date);

  const fetched = findInMemory(currency, date);
  if (fetched) {
    return {
      rate: fetched.rate,
      rateDate: fetched.rateDate,
      source: fetched.rateDate === date ? RATE_SOURCES.YAHOO : RATE_SOURCES.PREVIOUS_CLOSE,
    };
  }

  console.warn(`[historicalRateService] RATE-MISS: sin tasa para ${currency} en ${date} ni en los ${MAX_LOOKBACK_DAYS} días anteriores`);
  return null;
}

/**
 * Obtiene la tasa cruzada entre dos divisas para una fecha: cuántas unidades de
 * `toCurrency` cuesta 1 unidad de `fromCurrency`.
 *
 * Es la convención que ve el usuario en el diálogo de ingreso ("COP por USD"),
 * y se deriva de las dos tasas en base USD.
 *
 * @param {string} fromCurrency - Divisa que se ingresa (ej. USD)
 * @param {string} toCurrency - Moneda de referencia del usuario (ej. COP)
 * @param {string} date - Fecha ISO (YYYY-MM-DD)
 * @returns {Promise<{rate: number, rateDate: string, source: string}|null>}
 */
async function getCrossRate(fromCurrency, toCurrency, date) {
  if (fromCurrency === toCurrency) {
    return { rate: 1, rateDate: date, source: RATE_SOURCES.CACHE };
  }

  if (!isValidDate(date)) {
    throw new Error(`Fecha inválida: ${date}`);
  }

  // Las dos divisas se piden juntas: una tasa cruzada no debería costar dos
  // llamadas al canal (D5).
  await warmRange(
    [fromCurrency, toCurrency],
    shiftDate(date, MAX_LOOKBACK_DAYS + 2),
    date
  );

  const to = await getRateForDate(toCurrency, date);
  if (to === null) return null;

  const from = await getRateForDate(fromCurrency, date);
  if (from === null || !from.rate) return null;

  // Ambas en base USD: (unidades de `to` por USD) / (unidades de `from` por USD)
  const rate = to.rate / from.rate;

  // La fecha informada es la más lejana de las dos, que es la que el usuario
  // necesita conocer para decidir si corrige el valor.
  const rateDate = to.rateDate < from.rateDate ? to.rateDate : from.rateDate;
  const source = (to.source === RATE_SOURCES.PREVIOUS_CLOSE || from.source === RATE_SOURCES.PREVIOUS_CLOSE)
    ? RATE_SOURCES.PREVIOUS_CLOSE
    : to.source;

  return { rate, rateDate, source };
}

module.exports = {
  getRateForDate,
  getCrossRate,
  getRatesForRange,
  RATE_SOURCES,
  MAX_LOOKBACK_DAYS,
  // Exportados para test
  _shiftDate: shiftDate,
  _isValidDate: isValidDate,
  _resetMemory: resetMemory,
};
