/**
 * HU 5.6 — Los datos duros salen de la plataforma, no del modelo.
 *
 * Hasta esta historia, el Copilot le pagaba a Gemini por buscar en internet
 * cifras que `finance-query` ya sirve. Era caro dos veces:
 *
 * - **En dinero.** Cada búsqueda con *grounding* se factura aparte (~$0.014) y
 *   eran el 70% del coste de un análisis.
 * - **En fiabilidad.** El 13/10/2026 llegó a producción un informe donde MSFT y
 *   CRM aparecían con resultados y dividendos de **2024**, porque el modelo dio
 *   por "último trimestre publicado" lo primero que encontró. Un endpoint no se
 *   equivoca de trimestre.
 *
 * Lo que se trae de aquí **no pasa por el modelo** (RN-5.6-A): no se le pide y
 * no se contrasta contra lo que él diga. No es una discrepancia entre dos
 * fuentes, es un dato y una conjetura.
 *
 * Cuatro orígenes, los cuatro ya existentes:
 *
 * | Origen | Qué trae |
 * |---|---|
 * | `/financials/{s}/income?frequency=quarterly` | Ingresos, margen operativo, utilidad neta y crecimiento interanual |
 * | `/quotes?symbols=` (por lote) | P/E, capitalización y fecha ex-dividendo |
 * | `/analysis/{s}` | Consenso, precio objetivo y próxima fecha de resultados |
 * | `/news?symbol=` | Titulares recientes, con el enlace real al medio |
 *
 * **Nada de esto puede tumbar un análisis** (RN-5.6-D, Escenario 5): cada
 * origen degrada por su cuenta, y un activo sin datos en la plataforma se
 * investiga por web como hasta ahora (RN-5.6-E, Escenario 4).
 *
 * @module services/copilot/copilotFundamentals
 * @see platform-docs/stories/5.6-cifras-duras-desde-la-plataforma/refinamiento.md (T3)
 */

const { DateTime } = require('luxon');
const enricher = require('./copilotEnricher');
const { claseDeActivo } = require('./copilotRelevance');
const { esRancia } = require('./copilotFindings');

/**
 * La fuente que se le atribuye a un dato de la plataforma (RN-5.6-C).
 *
 * Es **una sola** para todo el análisis, y no la URL de cada endpoint, por dos
 * razones: los endpoints están autenticados —quien pinche el enlace recibe un
 * 401, y un enlace que el lector no puede abrir es peor fuente que decirle de
 * dónde salió— y con seis activos y cuatro endpoints serían dos docenas de
 * entradas en una sección Fuentes que hoy tiene una docena de medios reales.
 *
 * Va marcada `verified: true` por construcción: su procedencia se conoce. La
 * marca "procedencia no verificada" existe para lo que el modelo citó sin
 * respaldo, y esto no lo citó el modelo.
 */
const FUENTE_PLATAFORMA = {
  url: 'https://api.portastock.net',
  titulo: {
    es: 'Portastock — datos de la plataforma (finance-query)',
    en: 'Portastock — platform data (finance-query)',
  },
};

/**
 * Las etiquetas de cada hallazgo, en los dos idiomas (Escenario 8).
 *
 * Los hallazgos del modelo vienen ya redactados en el idioma del análisis;
 * éstos los escribe el código, así que el idioma lo pone el código. Replica el
 * mapa `TEXTOS` de `copilotNotifications`, que es como este proyecto resuelve
 * i18n en Cloud Functions.
 */
const ETIQUETAS = {
  es: {
    ingresos: 'Ingresos',
    margenOperativo: 'Margen operativo',
    utilidadNeta: 'Utilidad neta',
    crecimientoIngresos: 'Crecimiento interanual de ingresos',
    crecimientoAnual: 'Crecimiento anual de ingresos',
    per: 'P/E (TTM)',
    capitalizacion: 'Capitalización de mercado',
    consenso: 'Consenso de analistas',
    precioObjetivo: 'Precio objetivo medio',
    proximosResultados: 'Próxima fecha de resultados',
    exDividendo: 'Fecha ex-dividendo',
    titular: 'Titular',
    analistas: 'analistas',
  },
  en: {
    ingresos: 'Revenue',
    margenOperativo: 'Operating margin',
    utilidadNeta: 'Net income',
    crecimientoIngresos: 'Year-over-year revenue growth',
    crecimientoAnual: 'Annual revenue growth',
    per: 'P/E (TTM)',
    capitalizacion: 'Market cap',
    consenso: 'Analyst consensus',
    precioObjetivo: 'Mean target price',
    proximosResultados: 'Next earnings date',
    exDividendo: 'Ex-dividend date',
    titular: 'Headline',
    analistas: 'analysts',
  },
};

/** Cuántos titulares por activo se convierten en hallazgo. */
const MAX_TITULARES = 3;

/**
 * Cuántos activos se consultan a la vez.
 *
 * Cada activo son **cuatro** peticiones (las dos series de `/financials`,
 * `/analysis` y `/news`), así que siete activos de golpe son veintiocho
 * conexiones simultáneas. Medido el 14/10/2026 contra producción: de 28
 * llamadas a la vez, **18 vuelven con HTTP 503**; de seis en seis, ninguna.
 *
 * Y un 503 aquí no se ve: el activo se queda sin cifras, cae a la búsqueda web
 * y el informe sale igual — con los números que el modelo encuentre, que es
 * exactamente lo que esta historia vino a evitar. En el análisis de las 17:01
 * se perdieron así las cifras de MSFT, UNH, MELI y TMUS, los cuatro con datos
 * disponibles en la plataforma.
 *
 * Dos activos son ocho conexiones, que es el orden del resto del proyecto
 * (`copilotRelevance` usa 6, `copilotSources` 8).
 */
const ACTIVOS_EN_PARALELO = 2;

/**
 * Ejecuta en tandas, para no abrir una conexión por cada cosa que haga falta.
 *
 * @param {Array<*>} items
 * @param {function(*): Promise<*>} fn
 * @param {number} [limite]
 * @returns {Promise<Array<*>>}
 */
async function enTandas(items, fn, limite = ACTIVOS_EN_PARALELO) {
  const salida = [];
  for (let i = 0; i < items.length; i += limite) {
    salida.push(...(await Promise.all(items.slice(i, i + limite).map(fn))));
  }
  return salida;
}

/**
 * Margen de días con el que se busca "el mismo trimestre del año pasado".
 *
 * Los cierres fiscales no caen el mismo día cada año y alguna serie se salta un
 * trimestre. Buscar por fecha aproximada es lo que hace que el crecimiento
 * interanual compare lo comparable; tomar el cuarto elemento de la lista daría
 * un número con la etiqueta equivocada en cuanto falte un trimestre.
 */
const TOLERANCIA_INTERANUAL_DIAS = 45;

/**
 * El idioma, normalizado a los dos que existen.
 *
 * @param {string} language
 * @returns {'es'|'en'}
 */
function idioma(language) {
  return language === 'en' ? 'en' : 'es';
}

/**
 * Un hallazgo con la forma que espera el informe, atribuido a la plataforma.
 *
 * @param {string} label
 * @param {string} value
 * @param {object} [extra] - `period`, `publishedAt`, `sourceUrl`, `sourceTitle`.
 * @param {string} [lang]
 * @returns {object}
 */
function hallazgo(label, value, extra = {}, lang = 'es') {
  return {
    label,
    value: String(value),
    period: extra.period || null,
    publishedAt: extra.publishedAt || null,
    sourceUrl: extra.sourceUrl || FUENTE_PLATAFORMA.url,
    sourceTitle: extra.sourceTitle || FUENTE_PLATAFORMA.titulo[lang],
    // Lo que hace que esta fuente no se marque "procedencia no verificada"
    // (RN-5.6-C). Lo lee `copilotComposer` al armar las fuentes crudas.
    verified: true,
  };
}

/**
 * Un importe grande, legible, sin perder la escala.
 *
 * El redondeo es **de presentación** y se aplica al final: el margen y el
 * crecimiento se calculan antes con los enteros crudos, como exige el estándar
 * de montos.
 *
 * @param {number} valor
 * @param {string} moneda
 * @returns {string}
 */
function formatearImporte(valor, moneda) {
  const abs = Math.abs(valor);
  const sufijo = moneda ? ` ${moneda}` : '';

  if (abs >= 1e9) return `${(valor / 1e9).toFixed(2)} B${sufijo}`;
  if (abs >= 1e6) return `${(valor / 1e6).toFixed(1)} M${sufijo}`;
  return `${Math.round(valor)}${sufijo}`;
}

/**
 * Los estados de resultados de una serie, del más reciente al más antiguo.
 *
 * Sirve igual para la serie trimestral y para la anual: lo que ordena son
 * cierres, y cuál es cada uno lo decide quién la pidió.
 *
 * @param {object|null} income - La respuesta de `/financials/{s}/income`.
 * @returns {object[]}
 */
function periodosOrdenados(income) {
  const statements = (income && income.statements) || [];
  return [...statements]
    .filter((s) => s && s.endDate)
    .sort((a, b) => String(b.endDate).localeCompare(String(a.endDate)));
}

/**
 * El trimestre de hace un año respecto a otro, si está en la serie.
 *
 * @param {object[]} trimestres - Ya ordenados de más reciente a más antiguo.
 * @param {object} ultimo
 * @returns {object|null}
 */
function mismoTrimestreDelAnoPasado(trimestres, ultimo) {
  const objetivo = DateTime.fromISO(String(ultimo.endDate)).minus({ years: 1 });
  if (!objetivo.isValid) return null;

  for (const t of trimestres) {
    const fecha = DateTime.fromISO(String(t.endDate));
    if (!fecha.isValid) continue;
    if (Math.abs(fecha.diff(objetivo, 'days').days) <= TOLERANCIA_INTERANUAL_DIAS) return t;
  }

  return null;
}

/**
 * Los hallazgos que salen del estado de resultados (Escenarios 1 y 6).
 *
 * Si el trimestre más reciente que sirve la plataforma tiene más de un año, **no
 * se inyecta nada**: sería exactamente el defecto que esta historia viene a
 * cerrar, sólo que con otra procedencia. El activo cae a la web.
 *
 * @param {object|null} income - La serie trimestral.
 * @param {string} language
 * @param {Date} [now]
 * @param {object|null} [anual] - La serie **anual**, de donde sale el
 *   crecimiento cuando la trimestral no alcanza. Medido el 13/10/2026 contra
 *   producción: `frequency=quarterly` devuelve **4 trimestres**, así que el
 *   mismo trimestre del año pasado nunca está y el crecimiento interanual se
 *   quedaba siempre sin calcular. `frequency=annual` sí trae cuatro cierres
 *   anuales, y con ellos el crecimiento es real — anual en vez de trimestral,
 *   que es lo que de verdad se puede medir y así se etiqueta.
 * @returns {object[]}
 */
function hallazgosDeIngresos(income, language, now = new Date(), anual = null) {
  const lang = idioma(language);
  const t = ETIQUETAS[lang];
  const trimestres = periodosOrdenados(income);
  if (trimestres.length === 0) return [];

  const ultimo = trimestres[0];
  const periodo = enricher.formatEarningsPeriod(ultimo.endDate);
  // El `endDate` es el cierre del trimestre, no la fecha de publicación — que
  // llega entre tres y cuatro semanas después y la plataforma no la da. Usarlo
  // envejece la cifra un poco de más, que es el lado seguro; inventar la fecha
  // de publicación sería fabricar un dato justo donde esta historia los quita.
  const publicado = String(ultimo.endDate).slice(0, 10);

  if (esRancia(publicado, now)) {
    console.warn(
      `[copilotFundamentals] ${income.symbol}: el último trimestre de la plataforma `
      + `(${publicado}) tiene más de un año; se deja a la búsqueda web`,
    );
    return [];
  }

  const moneda = income.currency || 'USD';
  const comun = { period: periodo || null, publishedAt: publicado };
  const salida = [];

  if (typeof ultimo.totalRevenue === 'number') {
    salida.push(hallazgo(t.ingresos, formatearImporte(ultimo.totalRevenue, moneda), comun, lang));
  }

  if (typeof ultimo.operatingIncome === 'number' && ultimo.totalRevenue) {
    const margen = (ultimo.operatingIncome / ultimo.totalRevenue) * 100;
    salida.push(hallazgo(t.margenOperativo, `${margen.toFixed(1)}%`, comun, lang));
  }

  if (typeof ultimo.netIncome === 'number') {
    salida.push(hallazgo(t.utilidadNeta, formatearImporte(ultimo.netIncome, moneda), comun, lang));
  }

  const crecimiento = hallazgoDeCrecimiento(trimestres, ultimo, anual, periodo, publicado, lang);
  if (crecimiento) salida.push(crecimiento);

  return salida;
}

/**
 * El crecimiento de ingresos, con el mejor par comparable que haya.
 *
 * Primero el trimestral contra el mismo trimestre del año pasado, que es el que
 * pide la historia. Si no está en la serie —lo normal: la plataforma devuelve
 * cuatro trimestres— se compara el último año fiscal con el anterior y **se
 * dice que es anual**. Lo que no se hace es comparar contra el trimestre
 * anterior y llamarlo interanual: sería un número correcto con una etiqueta
 * falsa, que es peor que no tenerlo.
 *
 * @param {object[]} trimestres
 * @param {object} ultimo
 * @param {object|null} anual
 * @param {string} periodo
 * @param {string} publicado
 * @param {'es'|'en'} lang
 * @returns {object|null}
 */
function hallazgoDeCrecimiento(trimestres, ultimo, anual, periodo, publicado, lang) {
  const t = ETIQUETAS[lang];
  const variacion = (actual, previo) => ((actual - previo) / previo) * 100;
  const conSigno = (v) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;

  const previoTrimestre = mismoTrimestreDelAnoPasado(trimestres, ultimo);
  if (previoTrimestre && previoTrimestre.totalRevenue && typeof ultimo.totalRevenue === 'number') {
    return hallazgo(
      t.crecimientoIngresos,
      conSigno(variacion(ultimo.totalRevenue, previoTrimestre.totalRevenue)),
      {
        period: `${periodo} vs ${enricher.formatEarningsPeriod(previoTrimestre.endDate)}`,
        publishedAt: publicado,
      },
      lang,
    );
  }

  const anos = periodosOrdenados(anual);
  if (anos.length < 2) return null;

  const [esteAno, anoPasado] = anos;
  if (!anoPasado.totalRevenue || typeof esteAno.totalRevenue !== 'number') return null;

  const ano = (s) => String(s.endDate).slice(0, 4);
  return hallazgo(
    t.crecimientoAnual,
    conSigno(variacion(esteAno.totalRevenue, anoPasado.totalRevenue)),
    {
      period: `FY ${ano(esteAno)} vs FY ${ano(anoPasado)}`,
      publishedAt: String(esteAno.endDate).slice(0, 10),
    },
    lang,
  );
}

/**
 * P/E, capitalización y ex-dividendo, del quote que ya se pide por lote.
 *
 * No llevan periodo: son el valor de hoy, no el de un trimestre cerrado.
 *
 * @param {object|null} quote
 * @param {string} language
 * @returns {object[]}
 */
function hallazgosDeValoracion(quote, language) {
  if (!quote) return [];

  const lang = idioma(language);
  const t = ETIQUETAS[lang];
  const salida = [];

  if (quote.pe) salida.push(hallazgo(t.per, quote.pe, {}, lang));
  if (quote.marketCap) salida.push(hallazgo(t.capitalizacion, quote.marketCap, {}, lang));
  if (quote.exDividend) salida.push(hallazgo(t.exDividendo, quote.exDividend, {}, lang));

  return salida;
}

/**
 * Consenso, precio objetivo y próxima fecha de resultados (RN-5.6-B).
 *
 * @param {object|null} analysis - La respuesta de `/analysis/{symbol}`.
 * @param {string} language
 * @returns {object[]}
 */
function hallazgosDeAnalistas(analysis, language) {
  if (!analysis) return [];

  const lang = idioma(language);
  const t = ETIQUETAS[lang];
  const salida = [];
  const objetivo = analysis.targetPrice;

  if (analysis.recommendationKey) {
    salida.push(hallazgo(
      t.consenso,
      enricher.mapRecommendationKey(analysis.recommendationKey),
      {},
      lang,
    ));
  }

  if (objetivo && typeof objetivo.mean === 'number') {
    const n = objetivo.numberOfAnalysts;
    const detalle = n ? ` (${n} ${t.analistas})` : '';
    salida.push(hallazgo(t.precioObjetivo, `${objetivo.mean}${detalle}`, {}, lang));
  }

  const proximo = (analysis.earningsEstimates || [])[0];
  if (proximo && proximo.endDate) {
    salida.push(hallazgo(
      t.proximosResultados,
      String(proximo.endDate).slice(0, 10),
      { period: enricher.formatEarningsPeriod(proximo.endDate) || null },
      lang,
    ));
  }

  return salida;
}

/**
 * Los titulares recientes, como hallazgos citables.
 *
 * Llevan **el enlace real al medio**, no la fuente genérica de plataforma: es
 * una URL que el lector puede abrir, y viene marcada como verificada porque nos
 * la dio `finance-query`, no el modelo. Son las únicas fuentes verificadas con
 * destino real que un análisis puede tener hoy.
 *
 * @param {Array<object>|null} news
 * @param {string} language
 * @returns {object[]}
 */
function hallazgosDeTitulares(news, language) {
  if (!Array.isArray(news)) return [];

  const lang = idioma(language);
  const t = ETIQUETAS[lang];

  return news
    .filter((n) => n && n.title && n.link)
    .slice(0, MAX_TITULARES)
    .map((n) => hallazgo(
      t.titular,
      n.title,
      { sourceUrl: n.link, sourceTitle: n.source || n.link },
      lang,
    ));
}

/**
 * Los fetchers reales, sobre el canal de servicio que ya usa el enriquecedor.
 *
 * @returns {{fetchIncome: Function, fetchQuotes: Function, fetchAnalysis: Function, fetchNews: Function}}
 */
function createFundamentalsFetchers() {
  const pedir = (endpoint) => enricher.pedirAFinanceQuery(endpoint, 'copilotFundamentals');

  return {
    fetchIncome: (symbol, frequency = 'quarterly') =>
      pedir(`/financials/${encodeURIComponent(symbol)}/income?frequency=${frequency}`),
    fetchQuotes: (symbols) => pedir(`/quotes?symbols=${encodeURIComponent(symbols.join(','))}`),
    fetchAnalysis: (symbol) => pedir(`/analysis/${encodeURIComponent(symbol)}`),
    fetchNews: (symbol) => pedir(`/news?symbol=${encodeURIComponent(symbol)}`),
  };
}

/**
 * El quote de cada símbolo, con una sola llamada para toda la tanda.
 *
 * @param {string[]} symbols
 * @param {object} fetchers
 * @returns {Promise<Map<string, object>>} Vacío si el endpoint no responde.
 */
async function quotesPorSimbolo(symbols, fetchers) {
  if (symbols.length === 0) return new Map();

  const crudo = await enricher.withTimeout(
    fetchers.fetchQuotes(symbols),
    enricher.FETCH_TIMEOUT_MS,
  );

  const lista = Array.isArray(crudo) ? crudo : [];
  return new Map(lista.filter((q) => q && q.symbol).map((q) => [String(q.symbol).toUpperCase(), q]));
}

/**
 * Todo lo que la plataforma sabe de los activos de una tanda.
 *
 * Nunca lanza y nunca deja a un activo fuera: lo que no se consiguió sale con
 * `findings: []` y `cubierto: false`, que es la señal de que ese activo tiene
 * que buscarse en la web como hasta ahora (Escenarios 4 y 5).
 *
 * @param {object[]} holdings - Posiciones con `symbol` y `asset_type`.
 * @param {{language: string}} opciones
 * @param {object} [fetchers]
 * @param {Date} [now]
 * @returns {Promise<Map<string, {findings: object[], cubierto: boolean}>>}
 */
async function obtenerDatosDePlataforma(
  holdings,
  { language },
  fetchers = createFundamentalsFetchers(),
  now = new Date(),
) {
  const simbolos = holdings.map((h) => h.symbol);
  const quotes = await quotesPorSimbolo(simbolos, fetchers);

  const resultados = await enTandas(holdings, async (holding) => {
    const { symbol } = holding;
    // RN-5.6-E: un ETF y una cripto no tienen 10-Q, así que preguntar por su
    // estado de resultados es una llamada garantizada a 404. Una acción de
    // bolsa local sí se consulta: ahí el 404 es información.
    const esAccion = claseDeActivo(holding) === 'stock';

    const [income, anual, analysis, news] = await Promise.all([
      esAccion
        ? enricher.withTimeout(fetchers.fetchIncome(symbol), enricher.FETCH_TIMEOUT_MS)
        : null,
      esAccion
        ? enricher.withTimeout(fetchers.fetchIncome(symbol, 'annual'), enricher.FETCH_TIMEOUT_MS)
        : null,
      enricher.withTimeout(fetchers.fetchAnalysis(symbol), enricher.FETCH_TIMEOUT_MS),
      enricher.withTimeout(fetchers.fetchNews(symbol), enricher.FETCH_TIMEOUT_MS),
    ]);

    const deIngresos = hallazgosDeIngresos(income, language, now, anual);
    const findings = [
      ...deIngresos,
      ...hallazgosDeValoracion(quotes.get(String(symbol).toUpperCase()), language),
      ...hallazgosDeAnalistas(analysis, language),
      ...hallazgosDeTitulares(news, language),
    ];

    // "Cubierto" no es "trajo algo": es que la plataforma puso **las cifras
    // fundamentales**. De eso depende que se le ahorren o no las preguntas al
    // modelo (RN-5.6-A). Un activo con titulares pero sin estado de resultados
    // sigue necesitando que la web le busque las cifras.
    const cubierto = deIngresos.length > 0;

    if (!cubierto && esAccion) {
      console.warn(
        `[copilotFundamentals] ${symbol}: sin estados financieros en la plataforma; `
        + 'sus cifras se buscan en la web',
      );
    }

    return [symbol, { findings, cubierto }];
  });

  return new Map(resultados);
}

module.exports = {
  FUENTE_PLATAFORMA,
  ETIQUETAS,
  MAX_TITULARES,
  ACTIVOS_EN_PARALELO,
  enTandas,
  TOLERANCIA_INTERANUAL_DIAS,
  formatearImporte,
  periodosOrdenados,
  mismoTrimestreDelAnoPasado,
  hallazgosDeIngresos,
  hallazgoDeCrecimiento,
  hallazgosDeValoracion,
  hallazgosDeAnalistas,
  hallazgosDeTitulares,
  createFundamentalsFetchers,
  obtenerDatosDePlataforma,
};
