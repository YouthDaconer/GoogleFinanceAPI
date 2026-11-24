/**
 * HU 5.7.1 — La ficha de cuatro pilares de cada activo, y las señales con las
 * que se decide.
 *
 * El análisis del 2026-11-19 recomendó reducir NVDA tras una caída del 39 %
 * apoyándose en tres noticias sin verificar: el redactor nunca vio el precio,
 * el rango de 52 semanas, las medias, el RSI ni el potencial hasta el objetivo.
 * Este módulo los calcula **en código** a partir de lo que la plataforma ya
 * sirve, y deja **ya evaluadas** las señales del criterio de decisión:
 * sobrepeso (RN-5.7-E), valoración extrema (RN-5.7-F) y deterioro del Nivel 2
 * (RN-5.7-D).
 *
 * Que las evalúe el código y no el modelo es la lección de la HU 5.1/5.3: la
 * frontera del 10,0 % frente al 10,1 % (QA-12) o "P/E 28 ≤ 30" (QA-13) no puede
 * depender de la aritmética del modelo.
 *
 * Todo es **puro**: recibe las respuestas ya descargadas. Lo que falta se
 * declara con su motivo y nunca se rellena (RN-5-H).
 *
 * Aquí viven también los ayudantes de series y formato que comparte con
 * `copilotFundamentals` (`idioma`, `formatearImporte`, `periodosOrdenados`,
 * `crecimientoAnualPct`): ese módulo pide los datos y construye esta ficha, así
 * que la dependencia va de él hacia aquí y no al revés.
 *
 * @module services/copilot/copilotAssetCardService
 * @see platform-docs/stories/5.7.1-recomendaciones-sin-panico/refinamiento.md (T2)
 */

const { DateTime } = require('luxon');
const enricher = require('./copilotEnricher');
const { claseDeActivo } = require('./copilotRelevance');
const {
  UMBRAL_SOBREPESO_PCT, CAIDA_MATERIAL_MARGEN_PP, EXCEPCION_OPORTUNIDAD, DCA,
} = require('./copilotConstants');
const { accionesPermitidas } = require('./copilotDecisionGuard');

/** El huso en que la serie diaria pone sus fechas: el del mercado. */
const ZONA_MERCADO = 'America/New_York';

/** En cuántos tramos, como mínimo, se reparte el margen para comprar (RN-5.7-G). */
const TRAMOS_PARA_EL_MARGEN = 3;

/** Sesiones de cada media móvil y del RSI. */
const SESIONES_SMA_CORTA = 50;
const SESIONES_SMA_LARGA = 200;
const SESIONES_RSI = 14;

/**
 * Días de holgura al buscar el cierre de hace un año.
 *
 * La serie de `range=1y` empieza el mismo día de hace un año, o en la primera
 * sesión siguiente si ese día no hubo mercado: el cierre "de hace un año o
 * antes" casi nunca está. Se acepta el primero de la serie si cae dentro de
 * esta holgura (un fin de semana largo); más allá, el dato no existe.
 */
const TOLERANCIA_INICIO_SERIE_DIAS = 7;

/** Cuántos trimestres muestra la tendencia: los que da `frequency=quarterly`. */
const TRIMESTRES_EN_FICHA = 4;

/**
 * Las etiquetas de la ficha, en los dos idiomas (Escenario 14, QA-21). Replica
 * el mapa `ETIQUETAS` de `copilotFundamentals`.
 */
const ETIQUETAS_FICHA = {
  es: {
    precio: 'Precio (último cierre)',
    variacion1m: 'Variación a 1 mes',
    variacion3m: 'Variación a 3 meses',
    variacionAno: 'Variación en el año',
    variacion1a: 'Variación a 1 año',
    maximo52: 'Máximo de 52 semanas (distancia)',
    minimo52: 'Mínimo de 52 semanas (distancia)',
    sma50: 'Media de 50 sesiones (SMA50)',
    sma200: 'Media de 200 sesiones (SMA200)',
    rsi: 'RSI (14)',
    tecnico: 'Datos técnicos',
    per: 'P/E (TTM)',
    bpa: 'BPA (TTM)',
    perFrenteCrecimiento: 'P/E frente al crecimiento anual de ingresos',
    consenso: 'Consenso de analistas',
    objetivos: 'Objetivos de analistas (bajo / medio / alto)',
    potencial: 'Potencial hasta el objetivo medio',
    crecimientoBpa: 'Crecimiento esperado del BPA',
    ingresosTrimestrales: 'Ingresos de los últimos trimestres',
    margenTrimestral: 'Margen operativo de los últimos trimestres',
    crecimientoAnual: 'Crecimiento anual de ingresos',
    margenAnual: 'Margen operativo anual',
    resultados: 'Estados de resultados',
    porEncima: 'precio por encima',
    porDebajo: 'precio por debajo',
    analistas: 'analistas',
    creciente: 'creciente',
    decreciente: 'decreciente',
    mixta: 'mixta',
    motivos: {
      sinHistorial: 'la plataforma no devolvió el historial de precios',
      historialInsuficiente: (n, requeridas) => `historial insuficiente: ${n} de ${requeridas} sesiones`,
      sinCierreBase: 'el historial no llega tan atrás',
      sinPrecio: 'sin precio de la plataforma',
      sinObjetivos: 'la plataforma no tiene objetivos de analistas para este activo',
      sinConsenso: 'la plataforma no tiene consenso de analistas para este activo',
      sinPer: 'la plataforma no da el P/E de este activo',
      sinCrecimientoBpa: 'la plataforma no da el crecimiento esperado del BPA',
      sinResultados: 'la plataforma no tiene estados de resultados de este activo',
      sinAnual: 'la plataforma no tiene dos años fiscales que comparar',
      perNoComparable: 'solo se compara con P/E y crecimiento positivos',
    },
  },
  en: {
    precio: 'Price (last close)',
    variacion1m: '1-month change',
    variacion3m: '3-month change',
    variacionAno: 'Year-to-date change',
    variacion1a: '1-year change',
    maximo52: '52-week high (distance)',
    minimo52: '52-week low (distance)',
    sma50: '50-day moving average (SMA50)',
    sma200: '200-day moving average (SMA200)',
    rsi: 'RSI (14)',
    tecnico: 'Technical data',
    per: 'P/E (TTM)',
    bpa: 'EPS (TTM)',
    perFrenteCrecimiento: 'P/E versus annual revenue growth',
    consenso: 'Analyst consensus',
    objetivos: 'Analyst targets (low / mean / high)',
    potencial: 'Upside to the mean target',
    crecimientoBpa: 'Expected EPS growth',
    ingresosTrimestrales: 'Revenue, last quarters',
    margenTrimestral: 'Operating margin, last quarters',
    crecimientoAnual: 'Annual revenue growth',
    margenAnual: 'Annual operating margin',
    resultados: 'Income statements',
    porEncima: 'price above',
    porDebajo: 'price below',
    analistas: 'analysts',
    creciente: 'rising',
    decreciente: 'falling',
    mixta: 'mixed',
    motivos: {
      sinHistorial: 'the platform returned no price history',
      historialInsuficiente: (n, requeridas) => `not enough history: ${n} of ${requeridas} sessions`,
      sinCierreBase: 'the history does not go back that far',
      sinPrecio: 'no platform price',
      sinObjetivos: 'the platform has no analyst targets for this asset',
      sinConsenso: 'the platform has no analyst consensus for this asset',
      sinPer: 'the platform does not provide this asset\'s P/E',
      sinCrecimientoBpa: 'the platform does not provide the expected EPS growth',
      sinResultados: 'the platform has no income statements for this asset',
      sinAnual: 'the platform does not have two fiscal years to compare',
      perNoComparable: 'only compared with positive P/E and growth',
    },
  },
};

/**
 * Las señales de deterioro del Nivel 2 (RN-5.7-D), como códigos estables que
 * el prompt nombra.
 */
const SENAL_DETERIORO = {
  INGRESOS_FY: 'fy_revenue_down',
  MARGEN_FY: 'fy_operating_margin_down',
  INGRESOS_2T: 'revenue_down_two_quarters',
  MARGEN_2T: 'operating_margin_down_two_quarters',
  BPA_ESPERADO: 'expected_eps_growth_negative',
};

/** Qué umbral de valoración extrema se superó (RN-5.7-F). */
const VALORACION_EXTREMA = {
  OBJETIVO_ALTO: 'above_high_target',
  OBJETIVO_MEDIO_Y_PE: 'above_mean_target_with_pe_over_twice_growth',
};

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
 * El crecimiento de ingresos del último año fiscal frente al anterior.
 *
 * Es el mismo número que `copilotFundamentals.hallazgoDeCrecimiento` publica
 * como hallazgo y el que la ficha compara con el P/E (RN-5.7-F): un solo
 * cálculo para los dos.
 *
 * @param {object|null} anual - La serie anual de `/financials`.
 * @returns {{pct: number, esteAno: object, anoPasado: object}|null} Sin
 *   redondear; `null` si no hay dos años comparables.
 */
function crecimientoAnualPct(anual) {
  const anos = periodosOrdenados(anual);
  if (anos.length < 2) return null;

  const [esteAno, anoPasado] = anos;
  if (!anoPasado.totalRevenue || typeof esteAno.totalRevenue !== 'number') return null;

  return {
    pct: ((esteAno.totalRevenue - anoPasado.totalRevenue) / anoPasado.totalRevenue) * 100,
    esteAno,
    anoPasado,
  };
}

/**
 * Un número de lo que manda `finance-query`, que a veces llega como texto
 * (`"15.68"`, `"+12.00%"`, `"1,234.5"`).
 *
 * @param {*} valor
 * @returns {number|null}
 */
function numero(valor) {
  if (typeof valor === 'number') return Number.isFinite(valor) ? valor : null;
  if (typeof valor !== 'string') return null;

  const limpio = valor.replace(/[,%+\s]/g, '');
  if (!limpio) return null;
  const n = Number(limpio);
  return Number.isFinite(n) ? n : null;
}

/** Porcentaje con signo y un decimal: `+0.2%`, `-39.0%`. Sólo para presentar. */
function conSigno(pct) {
  return `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`;
}

/** Un precio con dos decimales. Sólo para presentar. */
function precioTexto(valor) {
  return valor.toFixed(2);
}

/** Variación porcentual, sin redondear. */
function variacion(actual, base) {
  return ((actual - base) / base) * 100;
}

/** El año fiscal de un cierre: `FY 2026`. */
function anoFiscal(statement) {
  return `FY ${String(statement.endDate).slice(0, 4)}`;
}

/**
 * Los cierres válidos de la serie diaria, del más antiguo al más reciente.
 *
 * @param {object|null} serie - `{ 'YYYY-MM-DD': { close, high, low, adjclose } }`.
 * @returns {Array<{fecha: string, cierre: number, alto: number, bajo: number}>}
 */
function cierresDeLaSerie(serie) {
  if (!serie || typeof serie !== 'object') return [];

  const valido = (v) => typeof v === 'number' && Number.isFinite(v) && v > 0;
  const salida = [];

  for (const fecha of Object.keys(serie).sort()) {
    const punto = serie[fecha];
    if (!punto || typeof punto !== 'object') continue;
    const cierre = punto.close !== undefined ? punto.close : punto.adjclose;
    if (!valido(cierre)) continue;
    salida.push({
      fecha: fecha.slice(0, 10),
      cierre,
      alto: valido(punto.high) ? punto.high : cierre,
      bajo: valido(punto.low) ? punto.low : cierre,
    });
  }

  return salida;
}

/**
 * El cierre más reciente en o antes de una fecha.
 *
 * @param {object[]} cierres - Ascendentes.
 * @param {string} corte - `YYYY-MM-DD`.
 * @param {number} [holguraDias] - Si no hay ninguno antes, acepta el primero de
 *   la serie cuando cae a menos de estos días del corte.
 * @returns {object|null}
 */
function cierreEnOAntes(cierres, corte, holguraDias = 0) {
  for (let i = cierres.length - 1; i >= 0; i--) {
    if (cierres[i].fecha <= corte) return cierres[i];
  }

  if (holguraDias > 0 && cierres.length > 0) {
    const limite = DateTime.fromISO(corte, { zone: ZONA_MERCADO })
      .plus({ days: holguraDias })
      .toISODate();
    if (cierres[0].fecha <= limite) return cierres[0];
  }

  return null;
}

/**
 * Media simple de los últimos `n` cierres.
 *
 * @param {number[]} valores - Ascendentes.
 * @param {number} n
 * @returns {number|null} `null` con menos de `n` sesiones.
 */
function mediaMovil(valores, n) {
  if (valores.length < n) return null;
  const ultimos = valores.slice(-n);
  return ultimos.reduce((suma, v) => suma + v, 0) / n;
}

/**
 * RSI de Wilder.
 *
 * La primera media de subidas y bajadas es la simple de los primeros `n`
 * cambios; después se suaviza con `(media·(n−1) + cambio) / n` sobre toda la
 * serie, que es como lo calculan los proveedores.
 *
 * @param {number[]} valores - Cierres ascendentes.
 * @param {number} [n]
 * @returns {number|null} `null` con menos de `n + 1` sesiones.
 */
function rsiWilder(valores, n = SESIONES_RSI) {
  if (valores.length < n + 1) return null;

  let subidas = 0;
  let bajadas = 0;
  for (let i = 1; i <= n; i++) {
    const cambio = valores[i] - valores[i - 1];
    if (cambio > 0) subidas += cambio;
    else bajadas -= cambio;
  }

  let mediaSubidas = subidas / n;
  let mediaBajadas = bajadas / n;

  for (let i = n + 1; i < valores.length; i++) {
    const cambio = valores[i] - valores[i - 1];
    mediaSubidas = (mediaSubidas * (n - 1) + Math.max(cambio, 0)) / n;
    mediaBajadas = (mediaBajadas * (n - 1) + Math.max(-cambio, 0)) / n;
  }

  if (mediaBajadas === 0) return 100;
  return 100 - 100 / (1 + mediaSubidas / mediaBajadas);
}

/**
 * El pilar técnico, de la serie diaria de 1 año.
 *
 * Las variaciones se anclan en el **último cierre**, no en "ahora": un análisis
 * de domingo compara el viernes con el viernes de hace un mes. El corte del año
 * es el último cierre del año anterior en el huso del mercado.
 *
 * @param {object|null} serie
 * @param {Date} [now]
 * @returns {object|null} `null` si la serie no trae ni un cierre válido (QA-19).
 *   Todo sin redondear; lo que no se puede calcular queda `null`.
 */
function tecnicoDeLaSerie(serie, now = new Date()) {
  const cierres = cierresDeLaSerie(serie);
  if (cierres.length === 0) return null;

  const ultimo = cierres[cierres.length - 1];
  const precio = ultimo.cierre;
  const fechaUltimo = DateTime.fromISO(ultimo.fecha, { zone: ZONA_MERCADO });

  const variacionDesde = (base) => (base ? {
    pct: variacion(precio, base.cierre),
    desde: base.fecha,
  } : null);

  const finDelAnoPasado = DateTime.fromJSDate(now, { zone: ZONA_MERCADO })
    .startOf('year')
    .minus({ days: 1 })
    .toISODate();

  // El rango de 52 semanas, sobre los máximos y mínimos diarios del último año.
  const haceUnAno = fechaUltimo.minus({ years: 1 }).toISODate();
  const ultimoAno = cierres.filter((c) => c.fecha >= haceUnAno);
  const maximo = ultimoAno.reduce((m, c) => (c.alto > m.alto ? c : m), ultimoAno[0]);
  const minimo = ultimoAno.reduce((m, c) => (c.bajo < m.bajo ? c : m), ultimoAno[0]);

  const valores = cierres.map((c) => c.cierre);
  const sma50 = mediaMovil(valores, SESIONES_SMA_CORTA);
  const sma200 = mediaMovil(valores, SESIONES_SMA_LARGA);

  return {
    precio,
    fecha: ultimo.fecha,
    sesiones: cierres.length,
    variaciones: {
      m1: variacionDesde(cierreEnOAntes(cierres, fechaUltimo.minus({ months: 1 }).toISODate())),
      m3: variacionDesde(cierreEnOAntes(cierres, fechaUltimo.minus({ months: 3 }).toISODate())),
      ytd: variacionDesde(cierreEnOAntes(cierres, finDelAnoPasado)),
      y1: variacionDesde(cierreEnOAntes(cierres, haceUnAno, TOLERANCIA_INICIO_SERIE_DIAS)),
    },
    maximo52: { valor: maximo.alto, fecha: maximo.fecha, distanciaPct: variacion(precio, maximo.alto) },
    minimo52: { valor: minimo.bajo, fecha: minimo.fecha, distanciaPct: variacion(precio, minimo.bajo) },
    sma50: sma50 === null ? null : { valor: sma50, posicionPct: variacion(precio, sma50) },
    sma200: sma200 === null ? null : { valor: sma200, posicionPct: variacion(precio, sma200) },
    rsi14: rsiWilder(valores),
  };
}

/**
 * El pilar de valoración.
 *
 * @param {object|null} quote - De `/quotes` (`pe`, `eps` como texto).
 * @param {object|null} analysis - De `/analysis/{s}`.
 * @param {number|null} precio - El último cierre de la serie. Sin él no hay
 *   potencial (QA-19): no se toma el precio de otro sitio.
 * @param {number|null} crecimientoAnual - `crecimientoAnualPct(anual).pct`.
 * @returns {object} Todo sin redondear; lo que falta, `null`.
 */
function valoracionDe(quote, analysis, precio, crecimientoAnual) {
  const objetivo = (analysis && analysis.targetPrice) || {};
  const estimacion = ((analysis && analysis.earningsEstimates) || [])[0] || null;

  const pe = numero(quote && quote.pe);
  const objetivoMedio = numero(objetivo.mean);
  const crecimiento = typeof crecimientoAnual === 'number' ? crecimientoAnual : null;

  return {
    pe,
    eps: numero(quote && quote.eps),
    consenso: analysis && analysis.recommendationKey
      ? enricher.mapRecommendationKey(analysis.recommendationKey)
      : null,
    objetivoBajo: numero(objetivo.low),
    objetivoMedio,
    objetivoAlto: numero(objetivo.high),
    analistas: numero(objetivo.numberOfAnalysts),
    potencialPct: objetivoMedio !== null && precio ? variacion(objetivoMedio, precio) : null,
    crecimientoBpaEsperadoPct: estimacion ? numero(estimacion.growth) : null,
    periodoBpaEsperado: estimacion ? enricher.formatEarningsPeriod(estimacion.endDate) || null : null,
    crecimientoAnualPct: crecimiento,
    // RN-5.7-F: "P/E frente a su crecimiento" sólo tiene sentido con los dos
    // positivos; con pérdidas o sin crecimiento no se compara.
    peFrenteACrecimiento: pe !== null && pe > 0 && crecimiento !== null && crecimiento > 0
      ? pe / crecimiento
      : null,
  };
}

/**
 * Cómo se mueve una serie de más antiguo a más reciente.
 *
 * @param {number[]} valores - Del más antiguo al más reciente.
 * @returns {'creciente'|'decreciente'|'mixta'|null}
 */
function direccion(valores) {
  if (valores.length < 2) return null;
  const sube = valores.every((v, i) => i === 0 || v > valores[i - 1]);
  const baja = valores.every((v, i) => i === 0 || v < valores[i - 1]);
  if (sube) return 'creciente';
  if (baja) return 'decreciente';
  return 'mixta';
}

/**
 * El pilar de fundamentales: ingresos y margen de los últimos trimestres y el
 * último año fiscal frente al anterior.
 *
 * @param {object|null} incomeTrimestral
 * @param {object|null} incomeAnual
 * @returns {{trimestres: object[], tendenciaIngresos: string|null, tendenciaMargen: string|null,
 *   anual: object|null, moneda: string}}
 */
function tendenciaDe(incomeTrimestral, incomeAnual) {
  const margenDe = (s) => (typeof s.operatingIncome === 'number' && s.totalRevenue
    ? (s.operatingIncome / s.totalRevenue) * 100
    : null);

  const trimestres = periodosOrdenados(incomeTrimestral)
    .slice(0, TRIMESTRES_EN_FICHA)
    .map((s) => ({
      periodo: enricher.formatEarningsPeriod(s.endDate) || String(s.endDate).slice(0, 10),
      ingresos: typeof s.totalRevenue === 'number' ? s.totalRevenue : null,
      margenPct: margenDe(s),
    }));

  const ascendentes = [...trimestres].reverse();
  const ingresos = ascendentes.map((t) => t.ingresos).filter((v) => v !== null);
  const margenes = ascendentes.map((t) => t.margenPct).filter((v) => v !== null);

  const crecimiento = crecimientoAnualPct(incomeAnual);
  const anual = crecimiento ? {
    periodo: `${anoFiscal(crecimiento.esteAno)} vs ${anoFiscal(crecimiento.anoPasado)}`,
    ingresos: crecimiento.esteAno.totalRevenue,
    ingresosPrevios: crecimiento.anoPasado.totalRevenue,
    crecimientoPct: crecimiento.pct,
    margenPct: margenDe(crecimiento.esteAno),
    margenPrevioPct: margenDe(crecimiento.anoPasado),
  } : null;

  return {
    trimestres,
    tendenciaIngresos: direccion(ingresos),
    tendenciaMargen: direccion(margenes),
    anual,
    // HU 5.7.2 corrige la moneda de quien no reporta en USD; hasta entonces la
    // ficha usa la misma que los hallazgos, para que no se contradigan.
    moneda: (incomeTrimestral && incomeTrimestral.currency)
      || (incomeAnual && incomeAnual.currency)
      || 'USD',
  };
}

/**
 * ¿Los dos últimos trimestres publicados caen uno tras otro? (q0 < q1 < q2).
 *
 * @param {Array<number|null>} recientes - Del más reciente al más antiguo.
 * @returns {boolean}
 */
function caeDosTrimestres(recientes) {
  const [q0, q1, q2] = recientes;
  if ([q0, q1, q2].some((v) => typeof v !== 'number')) return false;
  return q0 < q1 && q1 < q2;
}

/**
 * ¿Los dos últimos trimestres publicados suben uno tras otro? (q0 > q1 > q2).
 * El espejo de `caeDosTrimestres`: si el margen sube así, una caída del margen
 * anual ya no describe a la empresa de hoy.
 *
 * @param {Array<number|null>} recientes - Del más reciente al más antiguo.
 * @returns {boolean}
 */
function subeDosTrimestres(recientes) {
  const [q0, q1, q2] = recientes;
  if ([q0, q1, q2].some((v) => typeof v !== 'number')) return false;
  return q0 > q1 && q1 > q2;
}

/**
 * Las señales del criterio de decisión, ya evaluadas.
 *
 * - **Sobrepeso** (RN-5.7-E): `peso > 10` en una acción o una cripto. Se mide
 *   sobre el peso **con un decimal**, el mismo que lee el usuario: un 10,04 %
 *   se muestra como 10,0 % y no es sobrepeso (QA-12). Nunca aplica a un ETF.
 * - **Margen hasta el umbral** (RN-5.7-G): cuánto se puede aumentar sin pasar
 *   del 10 %; 0 si ya está en él o por encima.
 * - **Valoración extrema** (RN-5.7-F): precio sobre el objetivo alto, o sobre
 *   el medio con un P/E mayor que el doble del crecimiento de ingresos (sólo
 *   con los dos positivos).
 * - **Deterioro del Nivel 2** (RN-5.7-D): cada señal con el dato que la prueba.
 *   El margen operativo anual solo cuenta si cae al menos
 *   `CAIDA_MATERIAL_MARGEN_PP` y no sube en los dos últimos trimestres.
 * - **Excepción de oportunidad** (RN-5.7-E, decisión del PO del 2026-11-20):
 *   una acción que cumple todas las condiciones puede pasar del 10 % hasta un
 *   techo, más alto si el usuario compra de forma escalonada (DCA).
 * - **Acciones permitidas** (HU 5.7.3, RN-5.7-T y RN-5.7-R): qué se puede
 *   recomendar con estas señales, y si mantener tiene la convicción limitada.
 *   Sin el Nivel 1, que depende de las noticias: lo añade el composer al
 *   armar el contexto (`level1Eligible`).
 *
 * @param {object} entrada
 * @param {'stock'|'etf'|'crypto'} entrada.clase
 * @param {number|null} entrada.pesoPct - Peso en % del portafolio.
 * @param {object|null} entrada.tecnico
 * @param {object|null} entrada.valoracion
 * @param {object|null} entrada.tendencia
 * @param {string[]} [entrada.compras] - Fechas `YYYY-MM-DD` de los lotes abiertos.
 * @param {Date} [entrada.now]
 * @returns {{weightPct: number|null, overweightThresholdPct: number|null, overweight: boolean,
 *   roomToThresholdPct: number|null, maxTranchePct: number|null, extremeValuation: string|null,
 *   deterioration: Array<{signal: string, evidence: string}>,
 *   opportunityException: object|null, allowedActions: string[], holdMaxConviction: string|null}}
 */
function senalesDeDecision({
  clase, pesoPct, tecnico, valoracion, tendencia, compras = [], now = new Date(),
}) {
  const aplicaUmbral = clase !== 'etf';
  const peso = typeof pesoPct === 'number' && Number.isFinite(pesoPct)
    ? Math.round(pesoPct * 10) / 10
    : null;

  const overweight = aplicaUmbral && peso !== null && peso > UMBRAL_SOBREPESO_PCT;
  const deterioration = clase === 'stock' ? senalesDeDeterioro(valoracion, tendencia) : [];
  const roomToThresholdPct = aplicaUmbral && peso !== null
    ? Math.max(0, Math.round((UMBRAL_SOBREPESO_PCT - peso) * 10) / 10)
    : null;

  const senales = {
    weightPct: peso,
    overweightThresholdPct: aplicaUmbral ? UMBRAL_SOBREPESO_PCT : null,
    overweight,
    roomToThresholdPct,
    maxTranchePct: tramoMaximo(roomToThresholdPct),
    extremeValuation: clase === 'stock' ? valoracionExtrema(tecnico, valoracion) : null,
    deterioration,
    opportunityException: clase === 'stock'
      ? excepcionDeOportunidad({
        peso, tecnico, valoracion, deterioration, dca: compraEscalonada(compras, now),
      })
      : null,
  };

  // HU 5.7.3: el modelo recibe la lista ya hecha y el código la vuelve a
  // aplicar antes de guardar (copilotDecisionGuard).
  const permitidas = accionesPermitidas(senales, { clase });
  return {
    ...senales,
    allowedActions: permitidas.allowed,
    holdMaxConviction: permitidas.holdMaxConviction,
  };
}

/**
 * ¿Las compras recientes de una acción son escalonadas (DCA)?
 *
 * Cuenta las semanas distintas (en Nueva York, como el resto del cupo y de la
 * ficha) con al menos una compra dentro de los últimos `DCA.VENTANA_DIAS`
 * días. Varias compras el mismo día o la misma semana son un solo escalón.
 *
 * @param {string[]} compras - Fechas `YYYY-MM-DD`.
 * @param {Date} now
 * @returns {{purchases: number, weeks: number, from: string, to: string}|null}
 *   El patrón, o `null` si no llega a `DCA.MIN_SEMANAS` semanas.
 */
function compraEscalonada(compras, now) {
  const hoy = DateTime.fromJSDate(now, { zone: ZONA_MERCADO }).startOf('day');
  const desde = hoy.minus({ days: DCA.VENTANA_DIAS });

  const recientes = [...new Set(compras || [])]
    .map((f) => DateTime.fromISO(f, { zone: ZONA_MERCADO }))
    .filter((d) => d.isValid && d >= desde && d <= hoy)
    .sort((a, b) => a - b);

  const semanas = new Set(recientes.map((d) => `${d.weekYear}-${d.weekNumber}`));
  if (semanas.size < DCA.MIN_SEMANAS) return null;

  return {
    purchases: recientes.length,
    weeks: semanas.size,
    from: recientes[0].toISODate(),
    to: recientes[recientes.length - 1].toISODate(),
  };
}

/**
 * RN-5.7-E, excepción de oportunidad: ¿puede esta acción pasar del umbral de
 * sobrepeso, y hasta dónde?
 *
 * Exige TODAS las condiciones de `EXCEPCION_OPORTUNIDAD`, cada una con su dato
 * en `evidence`: ninguna señal de deterioro, crecimiento, consenso, potencial,
 * valoración barata frente al crecimiento y precio lejos del máximo. Con
 * compras escalonadas recientes, el techo es más alto.
 *
 * @param {object} entrada
 * @param {number|null} entrada.peso - Peso a un decimal.
 * @param {object|null} entrada.tecnico
 * @param {object|null} entrada.valoracion
 * @param {object[]} entrada.deterioration
 * @param {object|null} entrada.dca
 * @returns {{ceilingPct: number, roomToCeilingPct: number|null, maxTranchePct: number|null,
 *   dca: object|null, evidence: string[]}|null}
 */
function excepcionDeOportunidad({ peso, tecnico, valoracion, deterioration, dca }) {
  const E = EXCEPCION_OPORTUNIDAD;
  if (!tecnico || !valoracion || deterioration.length > 0) return null;

  const {
    crecimientoAnualPct: crecimiento, consenso, potencialPct, peFrenteACrecimiento,
  } = valoracion;
  const distancia = tecnico.maximo52 ? tecnico.maximo52.distanciaPct : null;

  const cumple = crecimiento !== null && crecimiento >= E.CRECIMIENTO_INGRESOS_MIN_PCT
    && E.CONSENSOS.includes(consenso)
    && potencialPct !== null && potencialPct >= E.POTENCIAL_MIN_PCT
    && peFrenteACrecimiento !== null && peFrenteACrecimiento <= E.PE_FRENTE_A_CRECIMIENTO_MAX
    && distancia !== null && distancia <= E.DISTANCIA_MAXIMO_52_MAX_PCT;
  if (!cumple) return null;

  const ceilingPct = dca ? E.TECHO_CON_DCA_PCT : E.TECHO_PCT;
  const evidence = [
    'no deterioration signals',
    `revenue growth ${conSigno(crecimiento)}`,
    `consensus ${consenso}`,
    `upside to mean target ${conSigno(potencialPct)}`,
    `P/E vs revenue growth ${peFrenteACrecimiento.toFixed(2)}`,
    `${conSigno(distancia)} from 52-week high`,
  ];
  if (dca) {
    evidence.push(`staggered buying: ${dca.purchases} purchases in ${dca.weeks} weeks (${dca.from} → ${dca.to})`);
  }

  const roomToCeilingPct = peso !== null
    ? Math.max(0, Math.round((ceilingPct - peso) * 10) / 10)
    : null;
  return {
    ceilingPct,
    roomToCeilingPct,
    maxTranchePct: tramoMaximo(roomToCeilingPct),
    dca,
    evidence,
  };
}

/**
 * Lo máximo que puede pesar un tramo: un tercio del margen, redondeado hacia
 * abajo a un decimal (decisión del PO, 2026-11-20: "no hacerlo de golpe").
 * Con tres tramos se llega al margen entero, nunca con uno.
 *
 * @param {number|null} margenPct
 * @returns {number|null}
 */
function tramoMaximo(margenPct) {
  if (margenPct === null || margenPct === undefined) return null;
  // Sobre décimas enteras: 9,6 / 3 da 3,1999… en coma flotante y se iría a 3,1.
  return Math.floor(Math.round(margenPct * 10) / TRAMOS_PARA_EL_MARGEN) / 10;
}

/**
 * RN-5.7-F, evaluada.
 *
 * @param {object|null} tecnico
 * @param {object|null} valoracion
 * @returns {string|null} El umbral superado, o `null`.
 */
function valoracionExtrema(tecnico, valoracion) {
  if (!tecnico || !valoracion) return null;
  const { precio } = tecnico;

  if (valoracion.objetivoAlto !== null && precio > valoracion.objetivoAlto) {
    return VALORACION_EXTREMA.OBJETIVO_ALTO;
  }

  const { pe, crecimientoAnualPct: crecimiento, objetivoMedio } = valoracion;
  const peComparable = pe !== null && pe > 0 && crecimiento !== null && crecimiento > 0;
  if (objetivoMedio !== null && precio > objetivoMedio && peComparable && pe > 2 * crecimiento) {
    return VALORACION_EXTREMA.OBJETIVO_MEDIO_Y_PE;
  }

  return null;
}

/**
 * RN-5.7-D, Nivel 2: las señales de deterioro, cada una con su dato.
 *
 * @param {object|null} valoracion
 * @param {object|null} tendencia
 * @returns {Array<{signal: string, evidence: string}>}
 */
function senalesDeDeterioro(valoracion, tendencia) {
  const salida = [];

  if (tendencia && tendencia.anual) {
    const a = tendencia.anual;
    if (a.ingresos < a.ingresosPrevios) {
      salida.push({
        signal: SENAL_DETERIORO.INGRESOS_FY,
        evidence: `${a.periodo}: ${formatearImporte(a.ingresos, tendencia.moneda)} < `
          + `${formatearImporte(a.ingresosPrevios, tendencia.moneda)}`,
      });
    }
    // Decisión del PO (2026-11-20): una caída anual pequeña, o desmentida por
    // los últimos trimestres, no rompe la tesis. La caída se mide a un decimal,
    // como la evidencia que lee el usuario: 3,0 pp cuenta y 2,9 no.
    const caidaMaterial = a.margenPct !== null && a.margenPrevioPct !== null
      && Math.round((a.margenPrevioPct - a.margenPct) * 10) / 10 >= CAIDA_MATERIAL_MARGEN_PP;
    const trimestresLoDesmienten = Boolean(tendencia.trimestres)
      && subeDosTrimestres(tendencia.trimestres.map((q) => q.margenPct));
    if (caidaMaterial && !trimestresLoDesmienten) {
      salida.push({
        signal: SENAL_DETERIORO.MARGEN_FY,
        evidence: `${a.periodo}: ${a.margenPct.toFixed(1)}% < ${a.margenPrevioPct.toFixed(1)}%`,
      });
    }
  }

  if (tendencia) {
    const t = tendencia.trimestres;
    const tramo = t.slice(0, 3).map((q) => q.periodo).reverse().join(' → ');

    if (caeDosTrimestres(t.map((q) => q.ingresos))) {
      salida.push({
        signal: SENAL_DETERIORO.INGRESOS_2T,
        evidence: `${tramo}: ${t.slice(0, 3).map((q) => formatearImporte(q.ingresos, tendencia.moneda)).reverse().join(' → ')}`,
      });
    }
    if (caeDosTrimestres(t.map((q) => q.margenPct))) {
      salida.push({
        signal: SENAL_DETERIORO.MARGEN_2T,
        evidence: `${tramo}: ${t.slice(0, 3).map((q) => `${q.margenPct.toFixed(1)}%`).reverse().join(' → ')}`,
      });
    }
  }

  if (valoracion && valoracion.crecimientoBpaEsperadoPct !== null
    && valoracion.crecimientoBpaEsperadoPct < 0) {
    salida.push({
      signal: SENAL_DETERIORO.BPA_ESPERADO,
      evidence: `${valoracion.periodoBpaEsperado || ''} ${conSigno(valoracion.crecimientoBpaEsperadoPct)}`.trim(),
    });
  }

  return salida;
}

/**
 * Los datos del pilar técnico como filas de la ficha.
 *
 * @param {object|null} tecnico
 * @param {object} t - Etiquetas del idioma.
 * @param {object[]} items
 * @param {object[]} missing
 * @returns {void}
 */
function filasTecnicas(tecnico, t, items, missing) {
  if (!tecnico) {
    missing.push({ label: t.tecnico, reason: t.motivos.sinHistorial });
    return;
  }

  const { fecha } = tecnico;
  items.push({ label: t.precio, value: precioTexto(tecnico.precio), period: fecha });

  const variaciones = [
    [t.variacion1m, tecnico.variaciones.m1],
    [t.variacion3m, tecnico.variaciones.m3],
    [t.variacionAno, tecnico.variaciones.ytd],
    [t.variacion1a, tecnico.variaciones.y1],
  ];
  for (const [label, v] of variaciones) {
    if (v) items.push({ label, value: conSigno(v.pct), period: `${v.desde} → ${fecha}` });
    else missing.push({ label, reason: t.motivos.sinCierreBase });
  }

  items.push({
    label: t.maximo52,
    value: `${precioTexto(tecnico.maximo52.valor)} (${conSigno(tecnico.maximo52.distanciaPct)})`,
    period: tecnico.maximo52.fecha,
  });
  items.push({
    label: t.minimo52,
    value: `${precioTexto(tecnico.minimo52.valor)} (${conSigno(tecnico.minimo52.distanciaPct)})`,
    period: tecnico.minimo52.fecha,
  });

  const medias = [
    [t.sma50, tecnico.sma50, SESIONES_SMA_CORTA],
    [t.sma200, tecnico.sma200, SESIONES_SMA_LARGA],
  ];
  for (const [label, media, requeridas] of medias) {
    if (!media) {
      missing.push({ label, reason: t.motivos.historialInsuficiente(tecnico.sesiones, requeridas) });
      continue;
    }
    const lado = media.posicionPct >= 0 ? t.porEncima : t.porDebajo;
    items.push({
      label,
      value: `${precioTexto(media.valor)} (${lado}, ${conSigno(media.posicionPct)})`,
      period: fecha,
    });
  }

  if (tecnico.rsi14 === null) {
    missing.push({ label: t.rsi, reason: t.motivos.historialInsuficiente(tecnico.sesiones, SESIONES_RSI + 1) });
  } else {
    items.push({ label: t.rsi, value: tecnico.rsi14.toFixed(1), period: fecha });
  }
}

/**
 * Los datos de valoración de una acción como filas de la ficha.
 *
 * @param {object} valoracion
 * @param {object|null} tendencia
 * @param {string} hoy - `YYYY-MM-DD`: la fecha de los datos "de hoy" (quote, analistas).
 * @param {boolean} hayPrecio
 * @param {object} t
 * @param {object[]} items
 * @param {object[]} missing
 * @returns {void}
 */
function filasDeValoracion(valoracion, tendencia, hoy, hayPrecio, t, items, missing) {
  const v = valoracion;

  if (v.pe !== null) items.push({ label: t.per, value: v.pe.toFixed(2), period: hoy });
  else missing.push({ label: t.per, reason: t.motivos.sinPer });

  if (v.eps !== null) items.push({ label: t.bpa, value: v.eps.toFixed(2), period: hoy });

  if (v.peFrenteACrecimiento !== null) {
    items.push({
      label: t.perFrenteCrecimiento,
      value: `${v.peFrenteACrecimiento.toFixed(2)} (P/E ${v.pe.toFixed(2)} / ${conSigno(v.crecimientoAnualPct)})`,
      period: tendencia && tendencia.anual ? tendencia.anual.periodo : hoy,
    });
  } else if (v.pe !== null && v.crecimientoAnualPct !== null) {
    missing.push({ label: t.perFrenteCrecimiento, reason: t.motivos.perNoComparable });
  }

  if (v.consenso) items.push({ label: t.consenso, value: v.consenso, period: hoy });
  else missing.push({ label: t.consenso, reason: t.motivos.sinConsenso });

  const hayObjetivos = [v.objetivoBajo, v.objetivoMedio, v.objetivoAlto].some((o) => o !== null);
  if (hayObjetivos) {
    const texto = (o) => (o === null ? '—' : precioTexto(o));
    const analistas = v.analistas ? ` (${v.analistas} ${t.analistas})` : '';
    items.push({
      label: t.objetivos,
      value: `${texto(v.objetivoBajo)} / ${texto(v.objetivoMedio)} / ${texto(v.objetivoAlto)}${analistas}`,
      period: hoy,
    });
  } else {
    missing.push({ label: t.objetivos, reason: t.motivos.sinObjetivos });
  }

  if (v.potencialPct !== null) {
    items.push({ label: t.potencial, value: conSigno(v.potencialPct), period: hoy });
  } else {
    missing.push({
      label: t.potencial,
      reason: hayPrecio ? t.motivos.sinObjetivos : t.motivos.sinPrecio,
    });
  }

  if (v.crecimientoBpaEsperadoPct !== null) {
    items.push({
      label: t.crecimientoBpa,
      value: conSigno(v.crecimientoBpaEsperadoPct),
      period: v.periodoBpaEsperado || hoy,
    });
  } else {
    missing.push({ label: t.crecimientoBpa, reason: t.motivos.sinCrecimientoBpa });
  }
}

/**
 * Los datos de fundamentales de una acción como filas de la ficha.
 *
 * @param {object} tendencia
 * @param {object} t
 * @param {object[]} items
 * @param {object[]} missing
 * @returns {void}
 */
function filasDeFundamentales(tendencia, t, items, missing) {
  const { trimestres, moneda } = tendencia;
  const conIngresos = [...trimestres].reverse().filter((q) => q.ingresos !== null);
  const conMargen = [...trimestres].reverse().filter((q) => q.margenPct !== null);
  const tramo = (lista) => `${lista[0].periodo} – ${lista[lista.length - 1].periodo}`;

  if (conIngresos.length > 0) {
    const serie = conIngresos.map((q) => formatearImporte(q.ingresos, moneda)).join(' → ');
    const tendenciaTexto = tendencia.tendenciaIngresos ? ` (${t[tendencia.tendenciaIngresos]})` : '';
    items.push({ label: t.ingresosTrimestrales, value: `${serie}${tendenciaTexto}`, period: tramo(conIngresos) });
  } else {
    missing.push({ label: t.resultados, reason: t.motivos.sinResultados });
  }

  if (conMargen.length > 0) {
    const serie = conMargen.map((q) => `${q.margenPct.toFixed(1)}%`).join(' → ');
    const tendenciaTexto = tendencia.tendenciaMargen ? ` (${t[tendencia.tendenciaMargen]})` : '';
    items.push({ label: t.margenTrimestral, value: `${serie}${tendenciaTexto}`, period: tramo(conMargen) });
  }

  const { anual } = tendencia;
  if (anual) {
    items.push({ label: t.crecimientoAnual, value: conSigno(anual.crecimientoPct), period: anual.periodo });
    if (anual.margenPct !== null && anual.margenPrevioPct !== null) {
      items.push({
        label: t.margenAnual,
        value: `${anual.margenPct.toFixed(1)}% vs ${anual.margenPrevioPct.toFixed(1)}%`,
        period: anual.periodo,
      });
    }
  } else {
    missing.push({ label: t.crecimientoAnual, reason: t.motivos.sinAnual });
  }
}

/**
 * La ficha de un activo (Escenario 2).
 *
 * Qué lleva según su tipo (RN-5.7-A):
 * - **Acción:** técnico, valoración y fundamentales.
 * - **ETF:** técnico, y el P/E del fondo sólo si el quote lo trae (QA-04).
 * - **Cripto:** sólo técnico; no tiene P/E, resultados ni objetivos.
 *
 * Nunca lanza: con todo caído devuelve una ficha que sólo declara lo que falta.
 *
 * @param {object} entrada
 * @param {object} entrada.holding - Con `symbol`, `asset_type` y `weight` (fracción).
 * @param {object|null} entrada.quote
 * @param {object|null} entrada.analysis
 * @param {object|null} entrada.income - Serie trimestral.
 * @param {object|null} entrada.anual - Serie anual.
 * @param {object|null} entrada.serie - Historial diario de 1 año.
 * @param {string} entrada.language
 * @param {Date} [entrada.now]
 * @returns {{kind: string, asOf: string, items: Array<{label: string, value: string, period: string}>,
 *   missing: Array<{label: string, reason: string}>, signals: object}}
 */
function construirFicha({
  holding, quote = null, analysis = null, income = null, anual = null, serie = null,
  language, now = new Date(),
}) {
  const t = ETIQUETAS_FICHA[idioma(language)];
  const clase = claseDeActivo(holding);
  const hoy = DateTime.fromJSDate(now, { zone: ZONA_MERCADO }).toISODate();

  const tecnico = tecnicoDeLaSerie(serie, now);
  const precio = tecnico ? tecnico.precio : null;
  const tendencia = clase === 'stock' ? tendenciaDe(income, anual) : null;
  const crecimiento = tendencia && tendencia.anual ? tendencia.anual.crecimientoPct : null;
  const valoracion = clase === 'stock' ? valoracionDe(quote, analysis, precio, crecimiento) : null;

  const items = [];
  const missing = [];

  filasTecnicas(tecnico, t, items, missing);

  if (clase === 'stock') {
    filasDeValoracion(valoracion, tendencia, hoy, precio !== null, t, items, missing);
    filasDeFundamentales(tendencia, t, items, missing);
  } else if (clase === 'etf') {
    const peFondo = numero(quote && quote.pe);
    if (peFondo !== null) items.push({ label: t.per, value: peFondo.toFixed(2), period: hoy });
  }

  const pesoPct = typeof holding.weight === 'number' ? holding.weight * 100 : null;

  return {
    kind: clase,
    asOf: tecnico ? tecnico.fecha : hoy,
    // HU 5.7.3: el último cierre, sin redondear. Es el precio de hoy cuando el
    // portafolio no trae uno (RN-5.7-L).
    price: precio,
    items,
    missing,
    signals: senalesDeDecision({
      clase, pesoPct, tecnico, valoracion, tendencia, compras: holding.purchaseDates, now,
    }),
  };
}

module.exports = {
  ETIQUETAS_FICHA,
  SENAL_DETERIORO,
  VALORACION_EXTREMA,
  SESIONES_SMA_CORTA,
  SESIONES_SMA_LARGA,
  SESIONES_RSI,
  TOLERANCIA_INICIO_SERIE_DIAS,
  idioma,
  formatearImporte,
  periodosOrdenados,
  crecimientoAnualPct,
  numero,
  cierresDeLaSerie,
  cierreEnOAntes,
  mediaMovil,
  rsiWilder,
  tecnicoDeLaSerie,
  valoracionDe,
  tendenciaDe,
  senalesDeDecision,
  compraEscalonada,
  excepcionDeOportunidad,
  tramoMaximo,
  construirFicha,
};
