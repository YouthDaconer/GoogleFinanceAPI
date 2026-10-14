/**
 * HU 5.3 — Qué activos se investigan a fondo (RN-5-L, RN-5.3-N).
 *
 * Los **10 de mayor peso**, más cualquier otro con una variación de precio
 * fuerte. El resto se trata en conjunto. Con 10 activos o menos, todos.
 *
 * El umbral de "fuerte" no se improvisa: es el que aprobó el PO en la HU 5.1
 * (`comparacion-motor.md` §7.2), con un valor por clase de activo porque la
 * volatilidad de una cripto es de 3 a 4 veces la de un índice — un umbral único
 * marcaría las cripto todos los días y las acciones casi nunca.
 *
 * §7.2 da además un segundo criterio, `|z| > 1,96` sobre el rendimiento anormal
 * semanal. Necesita una serie de rendimientos anormales contra un índice de
 * referencia que el proyecto no calcula para un activo suelto; aquí se aplica
 * el umbral absoluto, que sale de `/historical`, ya montado y cacheado.
 *
 * @module services/copilot/copilotRelevance
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T2)
 */

const { DateTime } = require('luxon');
const { getHistoricalPrices } = require('../financeQuery');

/**
 * Cuántos activos se investigan por peso (RN-5-L).
 *
 * Bajó de 10 a 6 por coste: cada activo investigado es una búsqueda facturada
 * (~$0.014) y era el grueso del gasto de un análisis. Los que salen del corte
 * no desaparecen del informe — pasan al bloque "resto del portafolio", con su
 * peso y su sector.
 */
const TOP_POR_PESO = 6;

/**
 * Umbral de variación semanal que convierte a un activo en relevante aunque no
 * esté entre los de mayor peso. HU 5.1 §7.2.
 */
const UMBRAL_VARIACION = {
  etf: 5,
  stock: 8,
  crypto: 15,
};

/** Para lo que no declara tipo, el umbral de una acción. */
const UMBRAL_POR_DEFECTO = UMBRAL_VARIACION.stock;

/** Las series diarias cambian una vez al día y se comparten entre usuarios. */
const HISTORIA_TTL_MS = 6 * 60 * 60 * 1000;

/** Llamadas simultáneas a `/historical`, igual que `carveOutReturnsService`. */
const CONCURRENCIA = 6;

const cacheHistoria = new Map();

/**
 * Clase de activo a efectos del umbral.
 *
 * @param {object} holding - Posición agregada, con `asset_type`.
 * @returns {'etf'|'stock'|'crypto'}
 */
function claseDeActivo(holding) {
  const tipo = String(holding.asset_type || '').toLowerCase();
  if (tipo.includes('crypto')) return 'crypto';
  if (tipo.includes('etf') || tipo.includes('fund')) return 'etf';
  return 'stock';
}

/**
 * El umbral que le toca a un activo.
 *
 * @param {object} holding
 * @returns {number} Porcentaje, siempre positivo.
 */
function umbralDe(holding) {
  return UMBRAL_VARIACION[claseDeActivo(holding)] || UMBRAL_POR_DEFECTO;
}

/**
 * Variación porcentual de la última semana a partir de una serie diaria.
 *
 * Pura: recibe la serie ya descargada. Devuelve `null` cuando no hay dos
 * cierres con los que comparar — sin dato no se decide nada, que es lo que
 * RN-5-H pide en todas partes.
 *
 * @param {object} serie - `{ 'YYYY-MM-DD': { close: number } }`
 * @param {Date} [now]
 * @returns {number|null} Porcentaje con signo.
 */
function variacionSemanal(serie, now = new Date()) {
  if (!serie || typeof serie !== 'object') return null;

  const fechas = Object.keys(serie).sort();
  if (fechas.length < 2) return null;

  const cierreDe = (fecha) => {
    const punto = serie[fecha];
    const valor = punto && (punto.close !== undefined ? punto.close : punto.adjclose);
    return typeof valor === 'number' && Number.isFinite(valor) && valor > 0 ? valor : null;
  };

  const ultimaFecha = fechas[fechas.length - 1];
  const actual = cierreDe(ultimaFecha);
  if (actual === null) return null;

  // El cierre más reciente de hace siete días o más: con una semana de festivos
  // la serie puede no tener el día exacto. La fecha se calcula en el huso del
  // mercado, que es el de las claves de la serie.
  const corte = DateTime.fromJSDate(now, { zone: 'America/New_York' })
    .minus({ days: 7 })
    .toISODate();
  let previo = null;
  for (let i = fechas.length - 1; i >= 0; i--) {
    if (fechas[i] <= corte) {
      previo = cierreDe(fechas[i]);
      if (previo !== null) break;
    }
  }

  if (previo === null) return null;
  return ((actual - previo) / previo) * 100;
}

/**
 * Serie diaria del último mes, con caché compartida.
 *
 * @param {string} symbol
 * @param {Date} [now]
 * @returns {Promise<object|null>}
 */
async function serieDiaria(symbol, now = new Date()) {
  const cacheado = cacheHistoria.get(symbol);
  if (cacheado && now.getTime() - cacheado.at < HISTORIA_TTL_MS) return cacheado.serie;

  try {
    const serie = await getHistoricalPrices(symbol, '1mo');
    cacheHistoria.set(symbol, { at: now.getTime(), serie });
    return serie;
  } catch (error) {
    // Una serie que no llega no excluye al activo: sólo impide que entre por
    // movimiento fuerte. Si está entre los de mayor peso, entra igual.
    console.error(`[copilotRelevance] Sin serie histórica para ${symbol}:`, error.message);
    cacheHistoria.set(symbol, { at: now.getTime(), serie: null });
    return null;
  }
}

/**
 * Ejecuta las promesas en tandas, para no abrir 25 conexiones a la vez.
 *
 * @param {Array<*>} items
 * @param {function(*): Promise<*>} fn
 * @param {number} [limite]
 * @returns {Promise<Array<*>>}
 */
async function enTandas(items, fn, limite = CONCURRENCIA) {
  const resultados = [];
  for (let i = 0; i < items.length; i += limite) {
    const tanda = items.slice(i, i + limite);
    resultados.push(...(await Promise.all(tanda.map(fn))));
  }
  return resultados;
}

/**
 * Decide qué se investiga a fondo y qué va al conjunto.
 *
 * @param {Array<object>} holdings - Posiciones agregadas por ticker, ya
 *   depuradas y ordenadas por peso descendente.
 * @param {Date} [now]
 * @param {string[]} [recomendados] - HU 5.4 (RN-5.4-X): tickers con una
 *   recomendación vigente del análisis anterior. Entran aunque no estén entre
 *   los de mayor peso ni se hayan movido: lo que se recomendó se vuelve a
 *   mirar. **Se suman**, no desplazan a nadie.
 * @returns {Promise<{ relevantes: object[], resto: object[] }>} Cada relevante
 *   lleva `motivo` (`peso`, `variacion` o `recomendacion`) y, si entró por
 *   variación, su `variacionSemanal` para la línea "Incluido por: cayó 11 %
 *   esta semana".
 */
async function seleccionarRelevantes(holdings, now = new Date(), recomendados = []) {
  const porRecomendacion = new Set(recomendados || []);
  const porPeso = [...holdings].sort((a, b) => b.weight - a.weight);

  // Con 10 o menos, todos: no hay nada que seleccionar y no habrá apartado
  // "Resto del portafolio" (Escenario 3 de la HU 5.3).
  if (porPeso.length <= TOP_POR_PESO) {
    return {
      relevantes: porPeso.map((h) => ({ ...h, motivo: 'peso' })),
      resto: [],
    };
  }

  const top = porPeso.slice(0, TOP_POR_PESO).map((h) => ({ ...h, motivo: 'peso' }));
  const candidatos = porPeso.slice(TOP_POR_PESO);

  const variaciones = await enTandas(candidatos, async (holding) => ({
    holding,
    variacion: variacionSemanal(await serieDiaria(holding.symbol, now), now),
  }));

  const relevantes = [...top];
  const resto = [];

  for (const { holding, variacion } of variaciones) {
    if (variacion !== null && Math.abs(variacion) >= umbralDe(holding)) {
      relevantes.push({
        ...holding,
        motivo: 'variacion',
        variacionSemanal: Math.round(variacion * 10) / 10,
      });
    } else if (porRecomendacion.has(holding.symbol)) {
      // HU 5.4 (RN-5.4-X): lo que se recomendó se vuelve a mirar, aunque no se
      // haya movido. El motivo se declara como cualquier otro (RN-5.3-N).
      relevantes.push({ ...holding, motivo: 'recomendacion' });
    } else {
      resto.push(holding);
    }
  }

  return { relevantes, resto };
}

module.exports = {
  TOP_POR_PESO,
  UMBRAL_VARIACION,
  HISTORIA_TTL_MS,
  claseDeActivo,
  umbralDe,
  variacionSemanal,
  seleccionarRelevantes,
  _cacheHistoria: cacheHistoria,
};
