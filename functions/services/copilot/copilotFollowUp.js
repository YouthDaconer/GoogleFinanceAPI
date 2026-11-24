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
 * HU 5.7.3: el estado de las recomendaciones anteriores también lo decide el
 * código. Se compara cada una con la de hoy del mismo activo (RN-5.7-P), se
 * cierran de entrada las reducciones o ventas que la ficha de hoy ya no
 * permite, y cada una lleva el precio de cuando se emitió y el de hoy
 * (RN-5.7-L). Al modelo solo le queda justificar.
 *
 * @module services/copilot/copilotFollowUp
 * @see platform-docs/stories/5.4-que-cambio-ultimo-analisis/refinamiento.md (T3)
 * @see platform-docs/stories/5.7.3-seguimiento-con-precio-y-criterio/refinamiento.md (T4)
 */

const { DateTime } = require('luxon');
const admin = require('../firebaseAdmin');
const { COPILOT_ANALYSES_SUBCOLLECTION } = require('./copilotConstants');
const guard = require('./copilotDecisionGuard');
const { cierresDeLaSerie, cierreEnOAntes } = require('./copilotAssetCardService');

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
  /** HU 5.7.3 (RN-5.7-P): reduce o vende, y la ficha de hoy ya no lo permite. */
  CRITERIA: 'criterio',
};

/** HU 5.7.3 — De dónde salió el precio de emisión (RN-5.7-L). */
const ORIGEN_PRECIO = {
  /** Lo guardó la recomendación al emitirse. */
  GUARDADO: 'guardado',
  /** El cierre de ese día, del historial (recomendaciones de antes de la 5.7.3). */
  CIERRE: 'cierre',
};

/** HU 5.7.3 — Los textos fijos de la línea de precio (Esc. 5 y 7). */
const TEXTOS_PRECIO = {
  es: {
    cierreDel: (fecha) => `cierre del ${fecha}`,
    emisionNoDisponible: 'precio de emisión no disponible',
    hoyNoDisponible: 'precio de hoy no disponible',
    hoy: 'hoy',
    locale: 'es-ES',
    pct: (texto) => `${texto} %`,
  },
  en: {
    cierreDel: (fecha) => `close of ${fecha}`,
    emisionNoDisponible: 'issue price not available',
    hoyNoDisponible: 'price today not available',
    hoy: 'today',
    locale: 'en-US',
    pct: (texto) => `${texto}%`,
  },
};

/** El signo menos tipográfico, el que usa la historia: «−13,6 %». */
const SIGNO_MENOS = '\u2212';

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
 * HU 5.7.3 (RN-5.7-P) — Cierra las reducciones o ventas anteriores que la ficha
 * de hoy de su activo ya no permite.
 *
 * Se hace antes de decidir, para que el modelo las reciba ya marcadas: si no,
 * una reducción que se apoyaba solo en noticias sin verificar seguiría viva
 * porque "la incertidumbre persiste" (caso NVDA, 2026-11-19).
 *
 * Solo toca las que siguen abiertas y cuyo activo tiene ficha: sin señales no
 * hay con qué juzgarla, y se queda para comparar con la de hoy.
 *
 * @param {object[]} recomendaciones - Con `closedState` ya resuelto.
 * @param {Map<string, string[]>} permitidas - Activo → acciones que permite hoy.
 * @returns {object[]}
 */
function cerrarPorCriterio(recomendaciones, permitidas) {
  return (recomendaciones || []).map((rec) => {
    if (rec.closedState || !rec.asset || !permitidas.has(rec.asset)) return rec;

    const accion = guard.accionDe(rec);
    const reduce = accion === guard.ACCION.VENDER || accion === guard.ACCION.REDUCIR;
    if (!reduce || permitidas.get(rec.asset).includes(accion)) return rec;

    return { ...rec, closedState: RECOMMENDATION_STATE.NO_LONGER, closedReason: CLOSE_REASON.CRITERIA };
  });
}

/**
 * HU 5.7.3 (RN-5.7-L) — El precio con que se emitió una recomendación.
 *
 * El que guardó; si no lo guardó (se emitió antes de esta historia), el cierre
 * del día del análisis de referencia, o el de la sesión anterior si ese día no
 * hubo mercado. Si el historial tampoco lo tiene, no hay precio: nunca se
 * inventa uno (RN-5-H).
 *
 * @param {object} rec
 * @param {object} contexto
 * @param {string} contexto.emitidoEn - ISO 8601 del análisis de referencia.
 * @param {object|null} [contexto.serie] - Serie diaria del activo.
 * @returns {{value: number|null, source: string|null, date: string|null}}
 */
function precioDeEmision(rec, { emitidoEn, serie = null }) {
  if (typeof rec.priceAtIssue === 'number' && Number.isFinite(rec.priceAtIssue) && rec.priceAtIssue > 0) {
    return { value: rec.priceAtIssue, source: ORIGEN_PRECIO.GUARDADO, date: null };
  }

  const dia = DateTime.fromISO(emitidoEn, { zone: 'America/New_York' });
  const cierre = dia.isValid ? cierreEnOAntes(cierresDeLaSerie(serie), dia.toISODate()) : null;
  if (!cierre) return { value: null, source: null, date: null };

  return { value: cierre.cierre, source: ORIGEN_PRECIO.CIERRE, date: cierre.fecha };
}

/**
 * Un precio para leer: dos decimales, o ninguno a partir de 1.000 (61.914).
 *
 * @param {number} valor
 * @param {object} t - Textos del idioma.
 * @returns {string}
 */
function precioLegible(valor, t) {
  const decimales = Math.abs(valor) >= 1000 ? 0 : 2;
  return new Intl.NumberFormat(t.locale, {
    minimumFractionDigits: decimales,
    maximumFractionDigits: decimales,
  }).format(valor);
}

/**
 * HU 5.7.3 (Esc. 1 y 5) — La línea «precio entonces → hoy (variación)».
 *
 * La arma el código y el redactor la copia tal cual, igual que la línea de
 * referencia de la 5.4: son datos, no una redacción.
 *
 * @param {object} precios
 * @param {{value: number|null, source: string|null, date: string|null}} precios.emision
 * @param {number|null} precios.hoy
 * @param {string} language
 * @returns {{line: string, changePct: number|null}}
 */
function lineaDePrecio({ emision, hoy }, language) {
  const t = TEXTOS_PRECIO[language === 'en' ? 'en' : 'es'];
  const hayEmision = typeof emision.value === 'number';
  const hayHoy = typeof hoy === 'number' && Number.isFinite(hoy);
  const sufijoCierre = emision.source === ORIGEN_PRECIO.CIERRE
    ? ` · ${t.cierreDel(fechaLegible(emision.date, language))}`
    : '';

  if (hayEmision && hayHoy) {
    // Sin redondear para el dato; el redondeo solo al presentarlo.
    const changePct = ((hoy - emision.value) / emision.value) * 100;
    const magnitud = Math.abs(changePct).toFixed(1);
    const signo = changePct < 0 ? SIGNO_MENOS : '+';
    const variacion = t.pct(language === 'en' ? magnitud : magnitud.replace('.', ','));
    return {
      line: `${precioLegible(emision.value, t)} → ${precioLegible(hoy, t)} (${signo}${variacion})${sufijoCierre}`,
      changePct,
    };
  }

  if (hayEmision) {
    return { line: `${precioLegible(emision.value, t)} → ${t.hoyNoDisponible}${sufijoCierre}`, changePct: null };
  }

  if (hayHoy) {
    return { line: `${t.emisionNoDisponible} · ${t.hoy} ${precioLegible(hoy, t)}`, changePct: null };
  }

  return { line: t.emisionNoDisponible, changePct: null };
}

/**
 * HU 5.7.3 — El precio de una recomendación anterior, listo para el informe.
 *
 * @param {object} rec
 * @param {object} contexto
 * @param {string} contexto.emitidoEn
 * @param {object|null} [contexto.serie]
 * @param {number|null} [contexto.precioHoy]
 * @param {string} contexto.language
 * @returns {{issue: number|null, issueSource: string|null, issueDate: string|null,
 *   today: number|null, changePct: number|null, line: string}}
 */
function precioDeRecomendacion(rec, {
  emitidoEn, serie = null, precioHoy = null, language,
}) {
  const emision = precioDeEmision(rec, { emitidoEn, serie });
  const hoy = typeof precioHoy === 'number' && Number.isFinite(precioHoy) ? precioHoy : null;
  const { line, changePct } = lineaDePrecio({ emision, hoy }, language);

  return {
    issue: emision.value,
    issueSource: emision.source,
    issueDate: emision.date,
    today: hoy,
    changePct,
    line,
  };
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
 * HU 5.7.3 (RN-5.7-S) — Empareja por `id` y, lo que quede, por enunciado.
 *
 * Desde esta historia cada elemento anterior viaja con su `id`, y el modelo
 * devuelve el estado por `id`: el idioma del enunciado ya no importa. El
 * emparejamiento por enunciado y luego por activo (`emparejarEstados`) queda
 * para lo que llegue sin `id`, como la ruta degradada, que lee la prosa.
 *
 * @param {object[]} anteriores - Con `id`.
 * @param {object[]} declarados - `{id?, statement?, asset?, state}`.
 * @returns {object[]} Los anteriores, cada uno con su `state` o `null`.
 */
function emparejarPorId(anteriores, declarados) {
  const lista = (declarados || []).filter((d) => d && d.state);
  const ids = new Set((anteriores || []).map((a) => (a.id ? String(a.id) : null)).filter(Boolean));

  const porId = new Map();
  for (const d of lista) {
    if (d.id && ids.has(String(d.id)) && !porId.has(String(d.id))) porId.set(String(d.id), d.state);
  }

  const conId = (anteriores || []).map((a) => ({ ...a, state: (a.id && porId.get(String(a.id))) || null }));
  const sueltos = conId.filter((a) => !a.state);
  if (sueltos.length === 0) return conId;

  const libres = lista.filter((d) => !d.id || !ids.has(String(d.id)));
  const emparejados = emparejarEstados(sueltos, libres);
  let i = 0;
  return conId.map((a) => (a.state ? a : emparejados[i++]));
}

/**
 * HU 5.7.3 (RN-5.7-P) — El estado de cada recomendación anterior, calculado.
 *
 * En este orden:
 * 1. lo que se cerró de entrada (activo ausente, horizonte, criterio) manda;
 * 2. si hoy hay una recomendación del mismo activo, el estado sale de
 *    compararlas (`copilotDecisionGuard.compararConHoy`);
 * 3. si no, el que el modelo declaró para ella; pero un "vigente" o un
 *    "ajustada" cuya acción la ficha de hoy ya no permite es "ya no vigente";
 * 4. si tampoco hay eso, queda sin estado: no se cuenta (RN-5.4).
 *
 * @param {object[]} anteriores - Las de la referencia, con `id` y su cierre.
 * @param {object[]} recsDeHoy - Las de hoy, ya corregidas.
 * @param {object[]} declarados - Los estados que declaró el modelo.
 * @param {object} [contexto]
 * @param {Map<string, number>} [contexto.preciosHoy] - Activo → precio de hoy.
 * @param {Map<string, string[]>} [contexto.permitidas] - Activo → acciones
 *   que permite hoy su ficha.
 * @returns {object[]} Cada anterior con `state`, `stateReason` y `changes`.
 */
function estadosCalculados(anteriores, recsDeHoy, declarados, {
  preciosHoy = new Map(), permitidas = new Map(),
} = {}) {
  return emparejarPorId(anteriores, declarados).map((rec) => {
    const declarado = rec.state;

    if (rec.closedState) {
      return {
        ...rec, state: rec.closedState, stateReason: rec.closedReason, changes: [],
      };
    }

    const precioHoy = rec.asset && preciosHoy.has(rec.asset) ? preciosHoy.get(rec.asset) : null;
    const comparado = guard.compararConHoy(rec, recsDeHoy, { precioHoy });
    if (comparado) {
      return {
        ...rec, state: comparado.state, stateReason: comparado.reason, changes: comparado.changes,
      };
    }

    if (!declarado) return { ...rec, state: null, stateReason: null, changes: [] };

    const accion = guard.accionDe(rec);
    const hoyPermitidas = rec.asset ? permitidas.get(rec.asset) : null;
    const yaNoSePermite = declarado !== RECOMMENDATION_STATE.NO_LONGER
      && Array.isArray(hoyPermitidas) && Boolean(accion) && !hoyPermitidas.includes(accion);

    return yaNoSePermite
      ? {
        ...rec,
        state: RECOMMENDATION_STATE.NO_LONGER,
        stateReason: guard.MOTIVO_DE_ESTADO.CRITERIO,
        changes: [],
      }
      : {
        ...rec, state: declarado, stateReason: guard.MOTIVO_DE_ESTADO.DECLARADO, changes: [],
      };
  });
}

/**
 * El estado final de cada tesis anterior: por `id` y, si no, por enunciado.
 *
 * @param {object[]} tesisAnteriores - Las de la referencia.
 * @param {object[]} declarados - `followUp.theses` de la decisión o de la extracción.
 * @returns {object[]}
 */
function estadosDeTesis(tesisAnteriores, declarados) {
  return emparejarPorId(tesisAnteriores, declarados);
}

/**
 * El recuento que muestra la tarjeta del historial (Escenario 15).
 *
 * Se fija al generarse el análisis y no se recalcula nunca (RN-5.4-V): sólo sus
 * etiquetas se traducen al leerlas.
 *
 * **Espera listas que ya traigan `state`** — las de `estadosCalculados` y
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
 * HU 5.7.3: cada recomendación y tesis anterior lleva un `id` (`R1`, `T1`…)
 * con el que vuelve su estado, y `criterionChanged` dice si la referencia es
 * anterior al criterio de decisión (Esc. 6): un análisis sin
 * `decisionCriteria` se generó antes de la 5.7.1.
 *
 * @returns {Promise<{estado: string, referencia: object|null, recomendaciones: object[], tesis: object[], referenceLine: object|null, activosARevisar: string[], criterionChanged: boolean}>}
 */
async function prepararComparacion(userId, { tickersEnCartera, language, now = new Date() }) {
  const vacio = {
    referencia: null,
    recomendaciones: [],
    tesis: [],
    referenceLine: null,
    activosARevisar: [],
    criterionChanged: false,
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

  const recomendaciones = cerrarDeEntrada(
    referencia.recommendations.map((rec, i) => ({ ...rec, id: `R${i + 1}` })),
    { tickersEnCartera, emitidoEn: referencia.createdAt, now },
  );

  return {
    estado: COMPARISON_STATE.COMPARED,
    referencia,
    recomendaciones,
    tesis: (referencia.theses || []).map((t, i) => ({ ...t, id: `T${i + 1}` })),
    referenceLine,
    activosARevisar: activosARevisar(recomendaciones),
    criterionChanged: !referencia.decisionCriteria,
  };
}

module.exports = {
  COMPARISON_STATE,
  RECOMMENDATION_STATE,
  THESIS_STATE,
  CLOSE_REASON,
  ORIGEN_PRECIO,
  TEXTOS_PRECIO,
  DIAS_PARA_AVISO_DE_ANTIGUEDAD,
  MAX_CANDIDATOS,
  buscarReferencia,
  tieneRecomendacionesComparables,
  tiempoTranscurrido,
  fechaLegible,
  horizonteCumplido,
  cerrarDeEntrada,
  cerrarPorCriterio,
  precioDeEmision,
  lineaDePrecio,
  precioDeRecomendacion,
  activosARevisar,
  claveDeEnunciado,
  emparejarEstados,
  emparejarPorId,
  estadosCalculados,
  estadosDeTesis,
  contarSeguimiento,
  prepararComparacion,
};
