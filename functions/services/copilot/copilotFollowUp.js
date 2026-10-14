/**
 * HU 5.4 — Qué cambió desde el último análisis.
 *
 * Aquí vive todo lo que se decide **sin preguntarle nada al modelo**: contra
 * qué análisis se compara, qué recomendaciones se cierran de entrada y qué
 * estado tiene la comparación. Al modelo sólo le llegan las que de verdad
 * requieren juicio.
 *
 * Es la misma lección que dejó medida la HU 5.1 y que 5.3 aplicó a la
 * depuración: una regla que depende de que el modelo la obedezca no es una
 * regla. "Ya no tienes INTC" es una comprobación contra el portafolio, y "su
 * horizonte terminó" es una fecha.
 *
 * @module services/copilot/copilotFollowUp
 * @see platform-docs/stories/5.4-que-cambio-ultimo-analisis/refinamiento.md (T3)
 */

const { DateTime } = require('luxon');
const admin = require('../firebaseAdmin');
const { COPILOT_ANALYSES_SUBCOLLECTION } = require('./copilotConstants');

/**
 * En qué situación está la comparación. Son cuatro cosas distintas y ninguna
 * se puede deducir de la ausencia de las otras (Escenarios 6, 9 y 13).
 */
const COMPARISON_STATE = {
  /** Hay referencia con recomendaciones estructuradas: se compara. */
  COMPARED: 'compared',
  /** El usuario no tiene ningún análisis completado. */
  FIRST: 'first',
  /** Hay referencia, pero sin recomendaciones comparables (formato antiguo). */
  LEGACY: 'legacy',
  /** Existe, pero no se pudo leer. **No** es un primer análisis. */
  UNAVAILABLE: 'unavailable',
};

/** El estado de una recomendación anterior (RN-5.4-O). */
const RECOMMENDATION_STATE = {
  STILL_VALID: 'vigente',
  ADJUSTED: 'ajustada',
  NO_LONGER: 'no_vigente',
};

/** El estado de una tesis anterior, tal como la sección 2 la deja. */
const THESIS_STATE = {
  CONFIRMED: 'confirmada',
  INVALIDATED: 'invalidada',
  UNCHANGED: 'sin_cambio',
};

/** Por qué se cerró una recomendación sin preguntarle al modelo. */
const CLOSE_REASON = {
  /** RN-5.4-T: el usuario ya no tiene el activo. */
  ASSET_GONE: 'activo_ausente',
  /** RN-5.4-U: el horizonte de la recomendación ya terminó. */
  HORIZON_OVER: 'horizonte_cumplido',
};

/**
 * RN-5.4-U — a partir de aquí, la sección avisa de que el análisis es antiguo.
 * El PO puede moverlo; por eso vive en una constante con nombre.
 */
const DIAS_PARA_AVISO_DE_ANTIGUEDAD = 90;

/**
 * Cuántos análisis se miran para encontrar la referencia.
 *
 * Se busca el **completado** más reciente que exista. Un puñado basta: habría
 * que tener varios fallidos seguidos para pasar de aquí, y si eso ocurre, no
 * hay nada reciente que comparar de todos modos.
 */
const MAX_CANDIDATOS = 10;

/**
 * El análisis contra el que se compara (RN-5.4-N).
 *
 * El completado más reciente que el usuario **conserve**: los fallidos no
 * cuentan (Escenario 7) y los borrados no están (Escenario 8). No se mira el
 * historial que ve el usuario, que está limitado a 20: se mira Firestore.
 *
 * @param {string} userId
 * @returns {Promise<object|null>} El documento, o `null` si no hay ninguno.
 * @throws {Error} Si Firestore no responde. El llamador lo trata como
 *   `unavailable`, nunca como "primer análisis" (Escenario 13).
 */
async function buscarReferencia(userId) {
  const snapshot = await admin
    .firestore()
    .collection('userData')
    .doc(userId)
    .collection(COPILOT_ANALYSES_SUBCOLLECTION)
    .orderBy('createdAt', 'desc')
    .limit(MAX_CANDIDATOS)
    .get();

  for (const doc of snapshot.docs) {
    const datos = doc.data();
    if (datos.status === 'completed') return { id: doc.id, ...datos };
  }

  return null;
}

/**
 * ¿Este análisis tiene recomendaciones comparables?
 *
 * Se mira el campo, no la fecha ni la versión del prompt (D5): un análisis
 * reciente cuya extracción falló tampoco las tiene, y tratarlo como si las
 * tuviera sería inventárselas.
 *
 * @param {object|null} analisis
 * @returns {boolean}
 */
function tieneRecomendacionesComparables(analisis) {
  return Boolean(analisis)
    && Array.isArray(analisis.recommendations)
    && analisis.recommendations.length > 0;
}

/**
 * Cuánto hace del análisis de referencia, en palabras y en días.
 *
 * @param {string} createdAt - ISO 8601.
 * @param {string} language - 'es' o 'en'.
 * @param {Date} [now]
 * @returns {{dias: number, texto: string, stale: boolean}}
 */
function tiempoTranscurrido(createdAt, language, now = new Date()) {
  const desde = DateTime.fromISO(createdAt, { zone: 'America/New_York' });
  const hasta = DateTime.fromJSDate(now, { zone: 'America/New_York' });
  const dias = Math.max(0, Math.floor(hasta.diff(desde, 'days').days));

  const texto = desde.setLocale(language === 'en' ? 'en' : 'es').toRelative({ base: hasta, style: 'long' })
    || `${dias} días`;

  return {
    dias,
    // `toRelative` devuelve "hace 27 días"; la línea ya dice "hace", así que se
    // quita el prefijo para no decirlo dos veces.
    texto: texto.replace(/^hace\s+/i, '').replace(/\s+ago$/i, ''),
    stale: dias > DIAS_PARA_AVISO_DE_ANTIGUEDAD,
  };
}

/**
 * Fecha del análisis de referencia, legible.
 *
 * @param {string} createdAt - ISO 8601.
 * @param {string} language
 * @returns {string}
 */
function fechaLegible(createdAt, language) {
  return DateTime.fromISO(createdAt, { zone: 'America/New_York' })
    .setLocale(language === 'en' ? 'en' : 'es')
    .toFormat('d LLL yyyy');
}

/**
 * ¿El horizonte de esta recomendación ya terminó? (RN-5.4-U)
 *
 * El horizonte llega como lo escribió el analista ("3-6 meses", "3-6 months").
 * Se lee el número mayor, que es cuando la recomendación deja de sostenerse del
 * todo, y se cuenta desde la fecha del análisis que la emitió. Lo que no se
 * pueda leer **no se cierra**: cerrar una recomendación por no entender su
 * horizonte sería peor que dejarla abierta para que el analista la juzgue.
 *
 * @param {string|null} horizon
 * @param {string} emitidoEn - ISO 8601 del análisis de referencia.
 * @param {Date} [now]
 * @returns {{cumplido: boolean, finalizaEn: string|null}}
 */
function horizonteCumplido(horizon, emitidoEn, now = new Date()) {
  if (!horizon || typeof horizon !== 'string') return { cumplido: false, finalizaEn: null };

  const numeros = horizon.match(/\d+/g);
  if (!numeros) return { cumplido: false, finalizaEn: null };

  const cantidad = Math.max(...numeros.map(Number));
  const texto = horizon.toLowerCase();

  let unidad = null;
  if (/mes|month/.test(texto)) unidad = 'months';
  else if (/sem|week/.test(texto)) unidad = 'weeks';
  else if (/añ|ano|year/.test(texto)) unidad = 'years';
  else if (/día|dia|day/.test(texto)) unidad = 'days';

  if (!unidad) return { cumplido: false, finalizaEn: null };

  const emitido = DateTime.fromISO(emitidoEn, { zone: 'America/New_York' });
  if (!emitido.isValid) return { cumplido: false, finalizaEn: null };

  const fin = emitido.plus({ [unidad]: cantidad });
  const ahora = DateTime.fromJSDate(now, { zone: 'America/New_York' });

  return { cumplido: fin < ahora, finalizaEn: fin.toISODate() };
}

/**
 * Cierra las recomendaciones que no necesitan juicio (RN-5.4-T, RN-5.4-U).
 *
 * @param {object[]} recomendaciones - Las del análisis de referencia.
 * @param {object} contexto
 * @param {Set<string>} contexto.tickersEnCartera - Los que el usuario todavía tiene.
 * @param {string} contexto.emitidoEn - ISO 8601 del análisis de referencia.
 * @param {Date} [contexto.now]
 * @returns {Array<object>} Cada una con `closedState` y `closedReason`, o
 *   ambos a `null` si la decide el analista.
 */
function cerrarDeEntrada(recomendaciones, { tickersEnCartera, emitidoEn, now = new Date() }) {
  return (recomendaciones || []).map((rec) => {
    const activo = rec.asset;

    // RN-5.4-T: una recomendación de comprar un activo que el usuario nunca
    // tuvo NO cae por este motivo — se evalúa como cualquier otra.
    const sobreUnActivoQueTenia = Boolean(activo) && !rec.isNewAsset;
    if (sobreUnActivoQueTenia && !tickersEnCartera.has(activo)) {
      return { ...rec, closedState: RECOMMENDATION_STATE.NO_LONGER, closedReason: CLOSE_REASON.ASSET_GONE };
    }

    const horizonte = horizonteCumplido(rec.horizon, emitidoEn, now);
    if (horizonte.cumplido) {
      return {
        ...rec,
        closedState: RECOMMENDATION_STATE.NO_LONGER,
        closedReason: CLOSE_REASON.HORIZON_OVER,
        closedAt: horizonte.finalizaEn,
      };
    }

    return { ...rec, closedState: null, closedReason: null };
  });
}

/**
 * Los activos que hay que volver a investigar por su recomendación anterior
 * (RN-5.4-X, Escenario 18).
 *
 * Sólo los de recomendaciones **vigentes**: una que ya se cerró de entrada no
 * añade su activo, porque no hay nada que investigar para sostener algo que ya
 * está cerrado.
 *
 * @param {object[]} recomendacionesCerradas - La salida de `cerrarDeEntrada`.
 * @returns {string[]} Tickers, sin repetidos.
 */
function activosARevisar(recomendacionesCerradas) {
  const tickers = new Set();

  for (const rec of recomendacionesCerradas || []) {
    if (rec.closedState !== null) continue;
    if (rec.asset) tickers.add(rec.asset);
  }

  return [...tickers];
}

/**
 * Un enunciado reducido a lo que sirve para reconocerlo.
 *
 * El estado vuelve del informe emparejado por su enunciado, y el modelo lo
 * reescribe con una cursiva, una coma o un espacio de más. Eso no puede costar
 * una pareja, así que se comparan sin adornos.
 *
 * @param {string} statement
 * @returns {string}
 */
function claveDeEnunciado(statement) {
  return String(statement || '')
    .toLowerCase()
    .replace(/[*_`~]/g, '')
    .replace(/[.,;:!?¡¿"'()[\]]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Empareja cada elemento anterior con el estado que el informe le dio.
 *
 * Por enunciado; y si el modelo lo reescribió de más, por activo, pero **sólo
 * cuando no hay ambigüedad**: un activo con dos estados sin emparejar no se
 * adjudica a ninguno. Cada estado se consume una vez, para que uno solo no
 * responda por dos recomendaciones distintas.
 *
 * Lo que no se empareja se queda **sin estado**, no en "vigente": el recuento
 * de la tarjeta se lee como un hecho, y dar por vigente algo que nadie declaró
 * es afirmar lo que no se sabe.
 *
 * @param {object[]} anteriores - Los elementos del análisis de referencia.
 * @param {object[]} declarados - Los estados extraídos del informe.
 * @returns {object[]} Los anteriores, cada uno con su `state` o `null`.
 */
function emparejarEstados(anteriores, declarados) {
  const libres = (declarados || []).filter((d) => d && d.state);
  const usados = new Set();

  const porEnunciado = new Map();
  for (const [i, d] of libres.entries()) {
    const clave = claveDeEnunciado(d.statement);
    if (clave && !porEnunciado.has(clave)) porEnunciado.set(clave, i);
  }

  const tomar = (indice) => {
    if (indice === undefined || usados.has(indice)) return null;
    usados.add(indice);
    return libres[indice].state;
  };

  const emparejados = (anteriores || []).map((item) => ({
    ...item,
    state: tomar(porEnunciado.get(claveDeEnunciado(item.statement))),
  }));

  // Segunda pasada por activo, con lo que quedó suelto a los dos lados.
  return emparejados.map((item) => {
    if (item.state || !item.asset) return item;

    const candidatos = libres
      .map((d, i) => (d.asset === item.asset && !usados.has(i) ? i : -1))
      .filter((i) => i !== -1);

    return candidatos.length === 1 ? { ...item, state: tomar(candidatos[0]) } : item;
  });
}

/**
 * El estado final de cada recomendación anterior (RN-5.4-O).
 *
 * Dos orígenes, y el orden importa: lo que el backend cerró de entrada manda
 * —no es juicio, es una comprobación contra el portafolio o una fecha—, y el
 * resto lo decidió el analista dentro del informe y vuelve por la extracción.
 *
 * @param {object[]} recomendacionesCerradas - La salida de `cerrarDeEntrada`.
 * @param {object[]} declarados - `followUp.recommendations` de la extracción.
 * @returns {object[]}
 */
function estadosFinales(recomendacionesCerradas, declarados) {
  const emparejadas = emparejarEstados(recomendacionesCerradas, declarados);

  return emparejadas.map((rec) => (rec.closedState
    ? { ...rec, state: rec.closedState }
    : rec));
}

/**
 * El estado final de cada tesis anterior.
 *
 * @param {object[]} tesisAnteriores - Las de la referencia.
 * @param {object[]} declarados - `followUp.theses` de la extracción.
 * @returns {object[]}
 */
function estadosDeTesis(tesisAnteriores, declarados) {
  return emparejarEstados(tesisAnteriores, declarados);
}

/**
 * El recuento que muestra la tarjeta del historial (Escenario 15).
 *
 * Se fija al generarse el análisis y no se recalcula nunca (RN-5.4-V): sólo sus
 * etiquetas se traducen al leerlas.
 *
 * **Espera listas que ya traigan `state`** — las de `estadosFinales` y
 * `estadosDeTesis`, no las de `prepararComparacion`: ésas llevan `closedState`
 * y aún les falta lo que decidió el informe, y contarlas aquí daría ceros.
 *
 * @param {object[]} estados - Recomendaciones con su estado final.
 * @param {object[]} tesis - Tesis con su estado final.
 * @returns {{vigentes: number, ajustadas: number, noVigentes: number, tesisInvalidadas: number}}
 */
function contarSeguimiento(estados, tesis) {
  const cuenta = (lista, estado) => (lista || []).filter((e) => e.state === estado).length;

  return {
    vigentes: cuenta(estados, RECOMMENDATION_STATE.STILL_VALID),
    ajustadas: cuenta(estados, RECOMMENDATION_STATE.ADJUSTED),
    noVigentes: cuenta(estados, RECOMMENDATION_STATE.NO_LONGER),
    tesisInvalidadas: cuenta(tesis, THESIS_STATE.INVALIDATED),
  };
}

/**
 * Prepara todo lo que la comparación necesita, antes de investigar nada.
 *
 * Nunca lanza: un fallo al leer la referencia devuelve `unavailable` y el
 * análisis sigue su curso (RN-5-C, Escenario 13).
 *
 * @param {string} userId
 * @param {object} opciones
 * @param {Set<string>} opciones.tickersEnCartera
 * @param {string} opciones.language
 * @param {Date} [opciones.now]
 * @returns {Promise<{estado: string, referencia: object|null, recomendaciones: object[], tesis: object[], referenceLine: object|null, activosARevisar: string[]}>}
 */
async function prepararComparacion(userId, { tickersEnCartera, language, now = new Date() }) {
  const vacio = {
    referencia: null,
    recomendaciones: [],
    tesis: [],
    referenceLine: null,
    activosARevisar: [],
  };

  let referencia;
  try {
    referencia = await buscarReferencia(userId);
  } catch (error) {
    // El análisis no falla por esto, y no se trata como primer análisis.
    console.error('[copilotFollowUp] No se pudo consultar el análisis anterior:', error.message);
    return { ...vacio, estado: COMPARISON_STATE.UNAVAILABLE };
  }

  if (!referencia) return { ...vacio, estado: COMPARISON_STATE.FIRST };

  const transcurrido = tiempoTranscurrido(referencia.createdAt, language, now);
  const referenceLine = {
    fecha: fechaLegible(referencia.createdAt, language),
    transcurrido: transcurrido.texto,
    stale: transcurrido.stale,
    dias: transcurrido.dias,
  };

  if (!tieneRecomendacionesComparables(referencia)) {
    return { ...vacio, estado: COMPARISON_STATE.LEGACY, referencia, referenceLine };
  }

  const recomendaciones = cerrarDeEntrada(referencia.recommendations, {
    tickersEnCartera,
    emitidoEn: referencia.createdAt,
    now,
  });

  return {
    estado: COMPARISON_STATE.COMPARED,
    referencia,
    recomendaciones,
    tesis: referencia.theses || [],
    referenceLine,
    activosARevisar: activosARevisar(recomendaciones),
  };
}

module.exports = {
  COMPARISON_STATE,
  RECOMMENDATION_STATE,
  THESIS_STATE,
  CLOSE_REASON,
  DIAS_PARA_AVISO_DE_ANTIGUEDAD,
  MAX_CANDIDATOS,
  buscarReferencia,
  tieneRecomendacionesComparables,
  tiempoTranscurrido,
  fechaLegible,
  horizonteCumplido,
  cerrarDeEntrada,
  activosARevisar,
  claveDeEnunciado,
  emparejarEstados,
  estadosFinales,
  estadosDeTesis,
  contarSeguimiento,
  prepararComparacion,
};
