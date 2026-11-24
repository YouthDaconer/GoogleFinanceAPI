/**
 * HU 5.7.3 — Lo que el código decide sobre una recomendación antes de que se
 * escriba el informe.
 *
 * La revisión de la 5.7.1 en producción dejó medido que el modelo no siempre
 * cumple lo que el prompt ya le pide: vendió con una sola señal, aumentó una
 * cripto porque había caído, puso un tramo de 3,1 % para llegar del 0,6 % al
 * 3 % y dio por vigente una reducción mientras recomendaba aumentar el mismo
 * activo. Todo eso se puede comprobar con datos, y el PO decidió (2026-11-23)
 * que lo compruebe el código, como la 5.4 ya hace al cerrar la recomendación
 * de un activo que se vendió (RN-5.4-T).
 *
 * Funciones puras: sin Firestore, sin red y sin reloj. Reciben la ficha (o sus
 * señales) y devuelven la recomendación corregida con la lista de lo que se
 * corrigió, para poder verificarlo en producción.
 *
 * @module services/copilot/copilotDecisionGuard
 * @see platform-docs/stories/5.7.3-seguimiento-con-precio-y-criterio/refinamiento.md (T3)
 */

const {
  FRACCION_REDUCCION_SIN_VENTA,
  TOLERANCIA_TRAMOS_PP,
} = require('./copilotConstants');

/** Las acciones de una recomendación (RN-5.7-K). */
const ACCION = {
  VENDER: 'vender',
  REDUCIR: 'reducir',
  MANTENER: 'mantener',
  AUMENTAR: 'aumentar',
  COMPRAR: 'comprar',
};

/** Las que sacan dinero del activo: llevan nivel de la jerarquía (RN-5.7-D). */
const ACCIONES_QUE_REDUCEN = [ACCION.VENDER, ACCION.REDUCIR];

/** Las que meten dinero en el activo. */
const ACCIONES_QUE_COMPRAN = [ACCION.AUMENTAR, ACCION.COMPRAR];

/** El estado de una recomendación anterior (RN-5.4-O). */
const ESTADO = {
  VIGENTE: 'vigente',
  AJUSTADA: 'ajustada',
  NO_VIGENTE: 'no_vigente',
};

/** Por qué una recomendación anterior quedó en el estado que quedó (RN-5.7-P). */
const MOTIVO_DE_ESTADO = {
  /** Misma acción y mismos parámetros que la de hoy. */
  IGUAL: 'misma_recomendacion',
  /** Misma acción con otro objetivo, otros tramos u otra convicción. */
  PARAMETROS: 'parametros',
  /** Hoy se recomienda otra acción sobre el mismo activo. */
  OTRA_ACCION: 'otra_accion',
  /** El precio de hoy ya entró en su zona (Esc. 4). */
  ZONA: 'zona_alcanzada',
  /** No había recomendación de hoy con qué compararla: el estado lo declaró el modelo. */
  DECLARADO: 'declarado',
  /** El modelo la dio por vigente, pero la ficha de hoy ya no permite esa acción. */
  CRITERIO: 'criterio',
};

/** Las reglas que puede aplicar la guarda, tal como quedan en `_meta.guardCorrections`. */
const REGLA = {
  SENALES: 'RN-5.7-T',
  CRIPTO: 'RN-5.7-R',
  TRAMOS: 'RN-5.7-Q',
};

/** RN-5.7-Q: una compra por tramos se reparte en 2 o 3. */
const TRAMOS_MIN = 2;
const TRAMOS_MAX = 3;

/**
 * Lo que dice un tramo sin zona de precio: la compra se escalona en el tiempo
 * en lugar de inventarse un precio (criterio e de la 5.7.1).
 */
const TRAMO_EN_EL_TIEMPO = {
  es: 'escalonado: dentro de 2 a 4 semanas, si la tesis sigue intacta',
  en: 'staggered: in 2 to 4 weeks, if the thesis is still intact',
};

/** Palabras que delatan una zona que es un plazo y no un precio. */
const ZONA_EN_EL_TIEMPO = /seman|week|d[ií]a|day|mes|month|año|year/i;

/** Los verbos con que empieza un enunciado, en los dos idiomas. */
const VERBOS = [
  { accion: ACCION.VENDER, patron: /\b(vender|vende|venda|sell)\b/ },
  { accion: ACCION.REDUCIR, patron: /\b(reducir|reduce|reduzca|recortar|trim)\b/ },
  { accion: ACCION.MANTENER, patron: /\b(mantener|mant[eé]n|mantenga|conservar|hold|keep)\b/ },
  { accion: ACCION.AUMENTAR, patron: /\b(aumentar|aumenta|aumente|incrementar|increase|add)\b/ },
  { accion: ACCION.COMPRAR, patron: /\b(comprar|compra|compre|buy)\b/ },
];

/** Redondea a una décima. Los pesos y los tramos se expresan así (decisión del PO). */
function aDecima(valor) {
  return Math.round(valor * 10) / 10;
}

/** `comprar` y `aumentar` compran las dos: para comparar son la misma acción. */
function familia(accion) {
  return accion === ACCION.COMPRAR ? ACCION.AUMENTAR : accion;
}

/**
 * La acción de un enunciado, para las recomendaciones de antes de la 5.7.1,
 * que no guardaron `action`.
 *
 * Gana el verbo que aparece primero: «Reducir NVDA para comprar TLT» reduce.
 *
 * @param {string} statement
 * @returns {string|null}
 */
function accionDeEnunciado(statement) {
  const texto = String(statement || '').toLowerCase().replace(/[*_`~]/g, '');
  let mejor = null;

  for (const { accion, patron } of VERBOS) {
    const coincidencia = patron.exec(texto);
    if (coincidencia && (mejor === null || coincidencia.index < mejor.indice)) {
      mejor = { accion, indice: coincidencia.index };
    }
  }

  return mejor ? mejor.accion : null;
}

/**
 * La acción de una recomendación: la que guardó, o la de su enunciado.
 *
 * @param {object} rec
 * @returns {string|null}
 */
function accionDe(rec) {
  if (!rec) return null;
  return Object.values(ACCION).includes(rec.action) ? rec.action : accionDeEnunciado(rec.statement);
}

/**
 * ¿El sobrepeso justifica reducir? (Nivel 3, RN-5.7-E)
 *
 * Con la excepción de oportunidad, solo por encima de su techo.
 *
 * @param {object} signals
 * @returns {boolean}
 */
function sobrepesoQueReduce(signals) {
  if (!signals.overweight) return false;
  const excepcion = signals.opportunityException;
  if (!excepcion) return true;
  return typeof signals.weightPct === 'number' && signals.weightPct > excepcion.ceilingPct;
}

/**
 * RN-5.7-T y RN-5.7-R — Qué acciones permite la ficha de un activo.
 *
 * - siempre se puede mantener;
 * - reducir, con una señal de deterioro (Nivel 2), con sobrepeso que la
 *   justifica o con valoración extrema (Nivel 3), o con un hecho de Nivel 1;
 * - vender, con dos o más señales de deterioro o con un hecho de Nivel 1;
 * - aumentar o comprar, si queda margen hasta el umbral (o hasta el techo de la
 *   excepción) y el activo no es una cripto: sin fundamentales ni valoración, el
 *   técnico decide cuándo, no qué;
 * - con una sola señal de deterioro, mantener no pasa de convicción baja.
 *
 * @param {object|null} signals - `card.signals`.
 * @param {object} [opciones]
 * @param {string} [opciones.clase] - `card.kind`: 'stock', 'etf' o 'crypto'.
 * @param {boolean} [opciones.level1Eligible] - Hay un hecho que toca la tesis
 *   con fuente verificada.
 * @returns {{allowed: string[], holdMaxConviction: string|null}|null} `null`
 *   sin señales: entonces no hay con qué limitar nada.
 */
function accionesPermitidas(signals, { clase = null, level1Eligible = false } = {}) {
  if (!signals || typeof signals !== 'object') return null;

  const deterioro = Array.isArray(signals.deterioration) ? signals.deterioration.length : 0;
  const nivel3 = sobrepesoQueReduce(signals) || Boolean(signals.extremeValuation);
  const allowed = [ACCION.MANTENER];

  if (deterioro >= 1 || nivel3 || level1Eligible) allowed.push(ACCION.REDUCIR);
  if (deterioro >= 2 || level1Eligible) allowed.push(ACCION.VENDER);

  if (clase !== 'crypto') {
    const excepcion = signals.opportunityException;
    const margen = excepcion ? excepcion.roomToCeilingPct : signals.roomToThresholdPct;
    // Un ETF no tiene umbral (`null`): su margen no limita la compra.
    if (margen === null || margen === undefined || margen > 0) {
      allowed.push(ACCION.AUMENTAR, ACCION.COMPRAR);
    }
  }

  return { allowed, holdMaxConviction: deterioro === 1 ? 'baja' : null };
}

/**
 * Un porcentaje del portafolio, para un enunciado: «2,1 %» o «2.1%».
 *
 * @param {number} valor
 * @param {string} language
 * @returns {string}
 */
function pctTexto(valor, language) {
  const redondeado = aDecima(valor);
  const texto = Number.isInteger(redondeado) ? String(redondeado) : redondeado.toFixed(1);
  return language === 'en' ? `${texto}%` : `${texto.replace('.', ',')} %`;
}

/**
 * El enunciado de una recomendación, armado con su acción, su activo y su
 * magnitud.
 *
 * Lo arma el código y no el modelo: si una corrección cambia la acción, el
 * enunciado no puede quedarse diciendo la anterior, y el seguimiento compara
 * siempre frases del mismo formato.
 *
 * @param {object} rec - Con `action`, `asset`, `targetPct` y `tranches`.
 * @param {number|null} pesoPct - Peso de hoy en % del portafolio.
 * @param {string} language
 * @returns {string|null} `null` si no hay activo o acción con qué armarlo.
 */
function enunciadoDe(rec, pesoPct, language) {
  if (!rec || !rec.asset || !rec.action) return null;

  const en = language === 'en';
  const { asset, targetPct } = rec;
  const hayPeso = typeof pesoPct === 'number' && Number.isFinite(pesoPct);
  const hayObjetivo = typeof targetPct === 'number' && Number.isFinite(targetPct);
  const desdeHasta = hayPeso && hayObjetivo;
  const porTramos = Array.isArray(rec.tranches) && rec.tranches.length > 0;
  const p = hayPeso ? pctTexto(pesoPct, language) : null;
  const t = hayObjetivo ? pctTexto(targetPct, language) : null;

  switch (rec.action) {
    case ACCION.VENDER:
      return en ? `Sell ${asset}` : `Vender ${asset}`;
    case ACCION.REDUCIR:
      if (desdeHasta) {
        return en
          ? `Reduce ${asset} from ${p} to ${t} of the portfolio`
          : `Reducir ${asset} del ${p} al ${t} del portafolio`;
      }
      return en ? `Reduce ${asset}` : `Reducir ${asset}`;
    case ACCION.MANTENER:
      return en ? `Hold ${asset}` : `Mantener ${asset}`;
    case ACCION.AUMENTAR: {
      const verbo = en
        ? `Increase ${asset}${porTramos ? ' in tranches' : ''}`
        : `Aumentar ${asset}${porTramos ? ' por tramos' : ''}`;
      if (desdeHasta) {
        return en ? `${verbo} from ${p} to ${t} of the portfolio` : `${verbo} del ${p} al ${t} del portafolio`;
      }
      if (hayObjetivo) return en ? `${verbo} up to ${t} of the portfolio` : `${verbo} hasta el ${t} del portafolio`;
      return verbo;
    }
    case ACCION.COMPRAR:
      if (hayObjetivo) {
        return en ? `Buy ${asset} up to ${t} of the portfolio` : `Comprar ${asset} hasta el ${t} del portafolio`;
      }
      return en ? `Buy ${asset}` : `Comprar ${asset}`;
    default:
      return null;
  }
}

/**
 * RN-5.7-Q — Los tramos de una recomendación de aumentar.
 *
 * Son válidos si son 2 o 3, ninguno supera el tope de la ficha y entre todos no
 * pasan de la distancia al objetivo (ni del margen hasta el umbral, o hasta el
 * techo de la excepción). Si no, se reparte esa distancia en tramos iguales que
 * respetan el tope, conservando las zonas que el modelo propuso, en orden.
 *
 * @param {object} rec - Una recomendación de aumentar.
 * @param {object} signals - `card.signals`.
 * @param {string} language
 * @returns {{rec: object, corregida: boolean, sinMargen: boolean}} Con
 *   `sinMargen` no hay nada que aumentar: quien llama la deja en mantener.
 */
function repartirTramos(rec, signals, language) {
  const peso = signals ? signals.weightPct : null;
  const objetivo = rec.targetPct;
  if (typeof peso !== 'number' || typeof objetivo !== 'number') {
    return { rec, corregida: false, sinMargen: false };
  }

  const excepcion = signals.opportunityException;
  const margen = excepcion ? excepcion.roomToCeilingPct : signals.roomToThresholdPct;
  const tope = excepcion ? excepcion.maxTranchePct : signals.maxTranchePct;
  const hayMargen = typeof margen === 'number';

  const distancia = aDecima(hayMargen ? Math.min(objetivo - peso, margen) : objetivo - peso);
  if (distancia <= 0) return { rec, corregida: false, sinMargen: true };

  const tramos = Array.isArray(rec.tranches) ? rec.tranches : [];
  const suma = tramos.reduce((total, t) => total + t.pct, 0);
  const validos = tramos.length >= TRAMOS_MIN && tramos.length <= TRAMOS_MAX
    && tramos.every((t) => typeof tope !== 'number' || t.pct <= tope + TOLERANCIA_TRAMOS_PP)
    && suma <= distancia + TOLERANCIA_TRAMOS_PP
    && objetivo - peso <= distancia + TOLERANCIA_TRAMOS_PP;
  if (validos) return { rec, corregida: false, sinMargen: false };

  // Tres tramos si la distancia da para una décima en cada uno; si no, dos.
  // Sobre décimas enteras, como `tramoMaximo`, para no perder una por la coma
  // flotante (2,4 / 3 = 0,7999…).
  const decimas = Math.round(distancia * 10);
  let n = TRAMOS_MAX;
  let pct = Math.floor(decimas / TRAMOS_MAX) / 10;
  if (pct < 0.1) {
    n = TRAMOS_MIN;
    pct = Math.floor(decimas / TRAMOS_MIN) / 10;
  }
  if (pct < 0.1) return { rec, corregida: false, sinMargen: true };
  if (typeof tope === 'number' && pct > tope) pct = tope;

  const lang = language === 'en' ? 'en' : 'es';
  const nuevos = Array.from({ length: n }, (_, i) => ({
    zone: (tramos[i] && tramos[i].zone) || (i === 0 && rec.priceZone) || TRAMO_EN_EL_TIEMPO[lang],
    pct,
  }));

  return {
    rec: {
      ...rec,
      tranches: nuevos,
      // Si el objetivo pasaba del margen, baja hasta donde el margen deja.
      targetPct: objetivo - peso > distancia ? aDecima(peso + distancia) : objetivo,
    },
    corregida: true,
    sinMargen: false,
  };
}

/** Una recomendación pasada a mantener: sin objetivo, sin tramos, sin nivel. */
function aMantener(rec) {
  return {
    ...rec, action: ACCION.MANTENER, targetPct: null, tranches: [], level: null, priceZone: null,
  };
}

/**
 * Un «vender» que la ficha no permite, pasado a «reducir» (RN-5.7-T).
 *
 * El modelo no decidió cuánto reducir. Si el único nivel es el sobrepeso, el
 * objetivo es el umbral (o el techo de la excepción), como dice el criterio d;
 * si no, la fracción del peso que fija el PO.
 *
 * @param {object} rec
 * @param {object} signals
 * @returns {object}
 */
function aReducir(rec, signals) {
  const peso = signals.weightPct;
  const deterioro = (signals.deterioration || []).length;
  const soloSobrepeso = deterioro === 0 && sobrepesoQueReduce(signals);

  let targetPct = null;
  if (typeof peso === 'number') {
    if (soloSobrepeso) {
      targetPct = signals.opportunityException
        ? signals.opportunityException.ceilingPct
        : signals.overweightThresholdPct;
    } else {
      targetPct = aDecima(peso * FRACCION_REDUCCION_SIN_VENTA);
    }
  }

  return {
    ...rec,
    action: ACCION.REDUCIR,
    targetPct,
    tranches: [],
    level: deterioro >= 1 ? 2 : 3,
  };
}

/**
 * Corrige una recomendación con la ficha de su activo (RN-5.7-Q, R y T).
 *
 * @param {object} rec - Normalizada (`normalizarRecomendaciones`).
 * @param {object} contexto
 * @param {object|null} contexto.card - La ficha del activo, o `null`.
 * @param {boolean} [contexto.level1Eligible]
 * @param {number|null} [contexto.pesoPct] - Peso de hoy, si no hay ficha.
 * @param {string} contexto.language
 * @returns {{rec: object, corrections: Array<{asset: string|null, rule: string,
 *   field: string, from: *, to: *}>}}
 */
function corregirRecomendacion(rec, {
  card = null, level1Eligible = false, pesoPct = null, language = 'es',
} = {}) {
  const corrections = [];
  const anotar = (rule, field, from, to) => {
    if (from !== to) corrections.push({ asset: rec.asset || null, rule, field, from, to });
  };

  let r = { ...rec, action: accionDe(rec) };
  const signals = card && card.signals ? card.signals : null;
  const permitidas = accionesPermitidas(signals, { clase: card ? card.kind : null, level1Eligible });

  if (permitidas && r.action && !permitidas.allowed.includes(r.action)) {
    const antes = r.action;
    if (antes === ACCION.VENDER && permitidas.allowed.includes(ACCION.REDUCIR)) {
      r = aReducir(r, signals);
    } else {
      r = aMantener(r);
    }
    const esCripto = card.kind === 'crypto' && ACCIONES_QUE_COMPRAN.includes(antes);
    anotar(esCripto ? REGLA.CRIPTO : REGLA.SENALES, 'action', antes, r.action);
  }

  if (permitidas && r.action === ACCION.MANTENER && permitidas.holdMaxConviction === 'baja'
    && ['alta', 'media'].includes(r.conviction)) {
    anotar(REGLA.SENALES, 'conviction', r.conviction, 'baja');
    r = { ...r, conviction: 'baja' };
  }

  if (permitidas && ACCIONES_QUE_REDUCEN.includes(r.action)) {
    const deterioro = (signals.deterioration || []).length;
    // Una venta que solo sostienen las señales es Nivel 2; un Nivel 1 sin un
    // hecho verificado que lo respalde, tampoco lo es.
    let nivel = r.level;
    if (!level1Eligible && (r.action === ACCION.VENDER || nivel === 1)) {
      nivel = deterioro >= 1 ? 2 : 3;
    }
    anotar(REGLA.SENALES, 'level', r.level, nivel);
    r = { ...r, level: nivel };

    // RN-5.7-T: un Nivel 2 se decide por los fundamentales, nunca por las noticias.
    if (r.level === 2 && r.pillar !== 'fundamentales') {
      anotar(REGLA.SENALES, 'pillar', r.pillar, 'fundamentales');
      r = { ...r, pillar: 'fundamentales' };
    }
  }

  if (signals && r.action === ACCION.AUMENTAR) {
    const tramos = repartirTramos(r, signals, language);
    if (tramos.sinMargen) {
      anotar(REGLA.TRAMOS, 'action', r.action, ACCION.MANTENER);
      r = aMantener(r);
    } else if (tramos.corregida) {
      anotar(REGLA.TRAMOS, 'tranches', JSON.stringify(r.tranches), JSON.stringify(tramos.rec.tranches));
      anotar(REGLA.TRAMOS, 'targetPct', r.targetPct, tramos.rec.targetPct);
      r = tramos.rec;
    }
  }

  const peso = signals && typeof signals.weightPct === 'number' ? signals.weightPct : pesoPct;
  const statement = enunciadoDe(r, peso, language);
  if (statement) r = { ...r, statement };

  return { rec: r, corrections };
}

/**
 * Corrige todas las recomendaciones de un análisis.
 *
 * @param {object[]} recomendaciones
 * @param {object} contexto
 * @param {Map<string, object>} contexto.fichas - Activo → su ficha (con
 *   `signals.level1Eligible` ya calculado).
 * @param {Map<string, number>} [contexto.pesos] - Activo → peso en %, para los
 *   que no tienen ficha.
 * @param {string} contexto.language
 * @returns {{recomendaciones: object[], correcciones: object[]}}
 */
function aplicarGuarda(recomendaciones, { fichas = new Map(), pesos = new Map(), language = 'es' } = {}) {
  const salida = [];
  const correcciones = [];

  for (const rec of recomendaciones || []) {
    const card = rec.asset ? fichas.get(rec.asset) || null : null;
    const { rec: corregida, corrections } = corregirRecomendacion(rec, {
      card,
      level1Eligible: Boolean(card && card.signals && card.signals.level1Eligible),
      pesoPct: rec.asset && pesos.has(rec.asset) ? pesos.get(rec.asset) : null,
      language,
    });
    salida.push(corregida);
    correcciones.push(...corrections);
  }

  return { recomendaciones: salida, correcciones };
}

/**
 * Los números de una zona de precio («88–90», «130-135 USD», «61.914»).
 *
 * @param {string|null} zona
 * @returns {{min: number, max: number}|null} `null` si no es un precio, o si
 *   es un plazo («dentro de 2 a 4 semanas»).
 */
function limitesDeZona(zona) {
  if (typeof zona !== 'string' || ZONA_EN_EL_TIEMPO.test(zona)) return null;

  const numeros = [];
  for (const m of zona.matchAll(/\d[\d.,]*/g)) {
    // Un porcentaje («tramo de 0,8 %») no es un precio.
    if (/^\s*%/.test(zona.slice(m.index + m[0].length))) continue;
    let token = m[0].replace(/[.,]$/, '');
    if (/^\d{1,3}(\.\d{3})+(,\d+)?$/.test(token)) token = token.replace(/\./g, '').replace(',', '.');
    else if (/^\d{1,3}(,\d{3})+(\.\d+)?$/.test(token)) token = token.replace(/,/g, '');
    else token = token.replace(',', '.');
    const n = Number(token);
    if (Number.isFinite(n) && n > 0) numeros.push(n);
    if (numeros.length === 2) break;
  }

  if (numeros.length === 0) return null;
  return { min: Math.min(...numeros), max: Math.max(...numeros) };
}

/**
 * ¿El precio de hoy ya entró en la zona de una recomendación? (Esc. 4)
 *
 * Para comprar, cuando el precio bajó hasta la zona o por debajo; para reducir
 * o vender, cuando subió hasta ella. Se mira la zona del primer tramo o, si no
 * hay tramos, la de la recomendación.
 *
 * @param {object} rec
 * @param {number|null} precioHoy
 * @returns {{zone: string}|null} La zona alcanzada, o `null`.
 */
function zonaCruzada(rec, precioHoy) {
  if (typeof precioHoy !== 'number' || !Number.isFinite(precioHoy)) return null;

  const zona = Array.isArray(rec.tranches) && rec.tranches[0] ? rec.tranches[0].zone : rec.priceZone;
  const limites = limitesDeZona(zona);
  if (!limites) return null;

  const accion = familia(accionDe(rec));
  if (accion === ACCION.AUMENTAR && precioHoy <= limites.max) return { zone: zona };
  if (ACCIONES_QUE_REDUCEN.includes(accion) && precioHoy >= limites.min) return { zone: zona };
  return null;
}

/** ¿Dos porcentajes son distintos? Un `null` frente a un número también lo es. */
function pctDistinto(a, b) {
  const numA = typeof a === 'number';
  const numB = typeof b === 'number';
  if (numA !== numB) return true;
  return numA && Math.abs(a - b) > TOLERANCIA_TRAMOS_PP;
}

/** Los porcentajes de los tramos de una recomendación. */
function pctDeTramos(rec) {
  return (Array.isArray(rec.tranches) ? rec.tranches : []).map((t) => t.pct);
}

/**
 * RN-5.7-P — El estado de una recomendación anterior frente a la de hoy del
 * mismo activo.
 *
 * - otra acción: ya no vigente;
 * - misma acción y mismos parámetros (objetivo, tramos, convicción): vigente,
 *   salvo que el precio ya haya entrado en su zona (Esc. 4), que la ajusta;
 * - misma acción con otros parámetros: ajustada, con qué cambió.
 *
 * La variación del precio sola nunca cambia el estado (RN-5.7-C).
 *
 * @param {object} anterior - La recomendación del análisis de referencia.
 * @param {object[]} recsDeHoy - Las de hoy, ya corregidas.
 * @param {object} [opciones]
 * @param {number|null} [opciones.precioHoy]
 * @returns {{state: string, reason: string, changes: object[]}|null} `null` si
 *   no hay con qué comparar: sin activo, sin acción legible o sin recomendación
 *   de hoy para ese activo.
 */
function compararConHoy(anterior, recsDeHoy, { precioHoy = null } = {}) {
  if (!anterior || !anterior.asset) return null;

  const antes = accionDe(anterior);
  const hoy = (recsDeHoy || []).find((r) => r.asset === anterior.asset);
  if (!antes || !hoy || !hoy.action) return null;

  if (familia(antes) !== familia(hoy.action)) {
    return {
      state: ESTADO.NO_VIGENTE,
      reason: MOTIVO_DE_ESTADO.OTRA_ACCION,
      changes: [{ field: 'action', from: antes, to: hoy.action }],
    };
  }

  const changes = [];
  if (pctDistinto(anterior.targetPct, hoy.targetPct)) {
    changes.push({ field: 'targetPct', from: anterior.targetPct ?? null, to: hoy.targetPct ?? null });
  }

  const tramosAntes = pctDeTramos(anterior);
  const tramosHoy = pctDeTramos(hoy);
  if (tramosAntes.length !== tramosHoy.length
    || tramosAntes.some((pct, i) => pctDistinto(pct, tramosHoy[i]))) {
    changes.push({ field: 'tranches', from: tramosAntes, to: tramosHoy });
  }

  if (anterior.conviction && hoy.conviction && anterior.conviction !== hoy.conviction) {
    changes.push({ field: 'conviction', from: anterior.conviction, to: hoy.conviction });
  }

  if (changes.length > 0) {
    return { state: ESTADO.AJUSTADA, reason: MOTIVO_DE_ESTADO.PARAMETROS, changes };
  }

  const cruce = zonaCruzada(anterior, precioHoy);
  if (cruce) {
    return {
      state: ESTADO.AJUSTADA,
      reason: MOTIVO_DE_ESTADO.ZONA,
      changes: [{ field: 'zone', from: cruce.zone, to: 'reached', price: precioHoy }],
    };
  }

  return { state: ESTADO.VIGENTE, reason: MOTIVO_DE_ESTADO.IGUAL, changes: [] };
}

module.exports = {
  ACCION,
  ESTADO,
  MOTIVO_DE_ESTADO,
  REGLA,
  TRAMO_EN_EL_TIEMPO,
  accionDeEnunciado,
  accionDe,
  accionesPermitidas,
  enunciadoDe,
  repartirTramos,
  corregirRecomendacion,
  aplicarGuarda,
  limitesDeZona,
  zonaCruzada,
  compararConHoy,
};
