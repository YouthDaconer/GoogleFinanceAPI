/**
 * HU 5.3 — Qué se queda de lo que el modelo investigó, y qué se tira.
 *
 * El modelo devuelve JSON; esto lo somete a las reglas de la historia **antes**
 * de que llegue a la redacción final:
 *
 * - **RN-5-E**: un hallazgo sin fuente no se publica.
 * - **RN-5.3-Q**: una cifra fundamental sin su periodo y su fecha de
 *   publicación no se publica.
 * - **RN-5.3-R**: una **ausencia** ("sin cobertura de analistas") es un dato
 *   que se entrega; un **fallo** ("no pudimos investigar") es otra cosa y no se
 *   disfraza de ausencia.
 *
 * Hacer esto en código y no pidiéndoselo al modelo es la lección de la HU 5.1:
 * con la estrategia dentro de las instrucciones, dos de tres motores se
 * inventaron una cifra y dos ignoraron en silencio lo que faltaba.
 *
 * @module services/copilot/copilotFindings
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T9)
 */

/** Qué le pasó a la investigación de un activo (RN-5.3-R). */
const RESEARCH_OUTCOME = {
  /** Se investigó y hay hallazgos. */
  OK: 'ok',
  /** Se investigó y la fuente dice que no hay información. */
  SIN_DATOS: 'sin_datos',
  /** No se pudo investigar. No es lo mismo, y no se presenta como si lo fuera. */
  FALLO: 'fallo',
};

/**
 * Etiquetas cuyo valor es una cifra fundamental y por tanto exigen periodo y
 * fecha de publicación (RN-5.3-Q). Se reconocen por palabra, no por una lista
 * cerrada de nombres: el modelo elige la etiqueta y nunca dirá exactamente lo
 * que una lista cerrada espere.
 */
const PALABRAS_DE_CIFRA_FUNDAMENTAL = [
  'ingreso', 'revenue', 'margen', 'margin', 'utilidad', 'income', 'profit',
  'beneficio', 'bpa', 'eps', 'crecimiento', 'growth', 'ebitda',
];

/**
 * ¿Este hallazgo es una cifra fundamental?
 *
 * @param {object} finding
 * @returns {boolean}
 */
function esCifraFundamental(finding) {
  const etiqueta = String(finding.label || '').toLowerCase();
  return PALABRAS_DE_CIFRA_FUNDAMENTAL.some((palabra) => etiqueta.includes(palabra));
}

/**
 * Lee el JSON del modelo, tolerando que lo envuelva en un bloque de código.
 *
 * @param {string} texto
 * @returns {object|null} `null` si no hay JSON utilizable.
 */
function parsearRespuesta(texto) {
  if (typeof texto !== 'string') return null;

  const limpio = texto
    .replace(/^\s*```(?:json)?\s*/i, '')
    .replace(/\s*```\s*$/i, '')
    .trim();

  const intentar = (candidato) => {
    try {
      const datos = JSON.parse(candidato);
      return datos && typeof datos === 'object' ? datos : null;
    } catch (error) {
      return null;
    }
  };

  const directo = intentar(limpio);
  if (directo) return directo;

  // Un JSON roto es un fallo de la rama, no una ausencia de datos. Pero antes
  // de darlo por perdido se prueban dos reparaciones, las dos que de verdad
  // ocurren: prosa alrededor del objeto, y saltos de línea crudos dentro de un
  // texto. Lo segundo lo hace `gemini-2.5-flash` (medido el 14/10/2026) y basta
  // para tirar una tanda de siete activos que por lo demás vino perfecta.
  const primero = limpio.indexOf('{');
  const ultimo = limpio.lastIndexOf('}');
  if (primero === -1 || ultimo <= primero) return null;

  const acotado = limpio.slice(primero, ultimo + 1);
  return intentar(acotado) || intentar(escaparControlesEnCadenas(acotado));
}

/**
 * Escapa los caracteres de control que el modelo dejó crudos **dentro** de una
 * cadena JSON.
 *
 * `JSON.parse` rechaza un salto de línea literal entre comillas, y el modelo lo
 * escribe cuando el texto de un hallazgo es largo. Se recorre el texto sabiendo
 * si se está dentro o fuera de una cadena —respetando lo ya escapado— para no
 * tocar el formato del propio JSON.
 *
 * @param {string} json
 * @returns {string}
 */
function escaparControlesEnCadenas(json) {
  const ESCAPES = { '\n': '\\n', '\r': '\\r', '\t': '\\t' };
  let salida = '';
  let dentro = false;
  let escapado = false;

  for (const c of json) {
    if (escapado) { salida += c; escapado = false; continue; }
    if (c === '\\') { salida += c; escapado = dentro; continue; }
    if (c === '"') { dentro = !dentro; salida += c; continue; }

    if (dentro && c < ' ') {
      salida += ESCAPES[c] || '';
      continue;
    }
    salida += c;
  }

  return salida;
}

/**
 * A partir de cuántos meses una cifra fundamental deja de valer como "lo
 * último publicado".
 *
 * Un trimestre tarda tres meses y su publicación un mes más; doce da margen de
 * sobra para el resultado más reciente de cualquier empresa y corta en seco lo
 * que es de otro año.
 */
const MESES_PARA_CIFRA_RANCIA = 12;

/**
 * ¿Esta cifra es de hace demasiado?
 *
 * El modelo no sabe qué día es hoy salvo que se le diga, y sin ancla temporal
 * da por "último resultado trimestral" uno de hace dos años — pasó con MSFT y
 * CRM, que llegaron con cifras y fuentes de 2024 a un análisis de 2026. Decirle
 * la fecha es la mitad del arreglo; esto es la otra.
 *
 * Lo que no trae fecha **no** se juzga aquí: de eso se encarga la regla que
 * exige periodo y fecha a toda cifra fundamental.
 *
 * @param {string|null} publishedAt - ISO 8601.
 * @param {Date} [now]
 * @returns {boolean}
 */
function esRancia(publishedAt, now = new Date()) {
  if (!publishedAt) return false;

  const fecha = new Date(publishedAt);
  if (Number.isNaN(fecha.getTime())) return false;

  const limite = new Date(now);
  limite.setMonth(limite.getMonth() - MESES_PARA_CIFRA_RANCIA);

  return fecha < limite;
}

/**
 * Deja sólo los hallazgos que cumplen las reglas, y dice cuáles se cayeron.
 *
 * @param {Array<object>} findings
 * @returns {{ validos: object[], descartados: Array<{label: string, motivo: string}> }}
 */
function filtrarHallazgos(findings) {
  const validos = [];
  const descartados = [];

  for (const finding of findings || []) {
    if (!finding || typeof finding !== 'object') continue;

    const label = String(finding.label || '').trim();
    const value = finding.value;

    if (!label || value === undefined || value === null || value === '') {
      descartados.push({ label: label || '(sin etiqueta)', motivo: 'vacio' });
      continue;
    }

    // RN-5-E: sin fuente no se publica.
    if (!finding.sourceUrl || typeof finding.sourceUrl !== 'string') {
      descartados.push({ label, motivo: 'sin_fuente' });
      continue;
    }

    // RN-5.3-Q: una cifra fundamental sin periodo y fecha no se publica.
    if (esCifraFundamental(finding) && esRancia(finding.publishedAt)) {
      descartados.push({ label: finding.label, motivo: 'cifra desactualizada' });
      continue;
    }

    if (esCifraFundamental(finding) && (!finding.period || !finding.publishedAt)) {
      descartados.push({ label, motivo: 'sin_periodo' });
      continue;
    }

    validos.push({
      label,
      value: String(value),
      period: finding.period || null,
      publishedAt: finding.publishedAt || null,
      sourceUrl: finding.sourceUrl,
      sourceTitle: finding.sourceTitle || null,
    });
  }

  return { validos, descartados };
}

/**
 * Un conflicto sirve si tiene al menos dos versiones **con fuente**: lo que el
 * Escenario 9 pide mostrar son las dos versiones *y* de dónde sale cada una.
 *
 * @param {Array<object>} conflicts
 * @returns {Array<object>}
 */
function filtrarConflictos(conflicts) {
  const salida = [];

  for (const conflicto of conflicts || []) {
    if (!conflicto || typeof conflicto !== 'object') continue;
    const versions = (conflicto.versions || []).filter(
      (v) => v && v.value !== undefined && v.value !== null && v.sourceUrl,
    );
    if (versions.length < 2) continue;
    salida.push({
      about: String(conflicto.about || '').trim() || 'dato',
      versions: versions.map((v) => ({ value: String(v.value), sourceUrl: v.sourceUrl })),
    });
  }

  return salida;
}

/**
 * Normaliza lo que devolvió una etapa de investigación.
 *
 * @param {string} ticker
 * @param {string|null} texto - La respuesta del modelo, o `null` si falló.
 * @returns {{ticker: string, outcome: string, findings: object[], notes: string[], conflicts: object[], descartados: object[]}}
 */
function normalizarInvestigacion(ticker, texto) {
  if (texto === null || texto === undefined) return normalizarDatos(ticker, null);
  return normalizarDatos(ticker, parsearRespuesta(texto));
}

/**
 * Lo mismo, a partir del objeto ya parseado.
 *
 * Existe aparte porque el lote (HU 5.6) parsea **una vez** y reparte: volver a
 * serializar cada bloque para que `normalizarInvestigacion` lo parseara otra
 * vez funcionaba, pero cualquiera que leyera el código se preguntaría por qué.
 *
 * @param {string} ticker
 * @param {object|null} datos
 * @returns {object} Igual que `normalizarInvestigacion`.
 */
function normalizarDatos(ticker, datos) {
  const vacio = {
    ticker,
    outcome: RESEARCH_OUTCOME.FALLO,
    findings: [],
    notes: [],
    conflicts: [],
    descartados: [],
  };

  // Un JSON que no se puede leer es un fallo de la investigación, no un activo
  // sin datos: decir "sin cobertura" aquí engañaría al usuario (RN-5.3-R).
  if (datos === null || typeof datos !== 'object') return vacio;

  const { validos, descartados } = filtrarHallazgos(datos.findings);
  const notes = (datos.notes || [])
    .filter((n) => typeof n === 'string' && n.trim())
    .map((n) => n.trim());

  return {
    ticker,
    // Con hallazgos, hay datos. Sin hallazgos pero con notas, la fuente
    // respondió que no hay información: eso es una ausencia, y se entrega.
    outcome: validos.length > 0 ? RESEARCH_OUTCOME.OK
      : notes.length > 0 ? RESEARCH_OUTCOME.SIN_DATOS
        : RESEARCH_OUTCOME.FALLO,
    findings: validos,
    notes,
    conflicts: filtrarConflictos(datos.conflicts),
    descartados,
  };
}

/**
 * HU 5.6 — Reparte por clave lo que devolvió una tanda.
 *
 * El lote responde `{"<TICKER>": {...}, "macro": {...}}`. Esto lo parte, de
 * forma que cada activo y el macro sigan pasando por las mismas reglas de
 * RN-5-E y RN-5.3-Q que cuando cada uno tenía su llamada.
 *
 * Una clave que el modelo **omita** queda como `fallo`, no como "sin datos": no
 * sabemos si no encontró nada o si se saltó el bloque, y presentarlo como una
 * ausencia comprobada engañaría al usuario (RN-5.3-R).
 *
 * La búsqueda de la clave es insensible a mayúsculas porque el ticker lo
 * reescribe el modelo, y "msft" es lo que devolvió más de una vez.
 *
 * @param {string[]} claves - Los tickers pedidos, y `'macro'` si la tanda lo llevaba.
 * @param {string|null} texto - La respuesta del modelo, o `null` si falló.
 * @returns {Map<string, object>} Una entrada por clave, siempre.
 */
function normalizarInvestigacionLote(claves, texto) {
  const datos = texto === null || texto === undefined ? null : parsearRespuesta(texto);

  const porClaveMinuscula = new Map();
  if (datos && typeof datos === 'object') {
    for (const [clave, valor] of Object.entries(datos)) {
      porClaveMinuscula.set(clave.toLowerCase(), valor);
    }
  }

  const salida = new Map();
  for (const clave of claves) {
    const bloque = porClaveMinuscula.get(String(clave).toLowerCase());
    salida.set(clave, normalizarDatos(clave, bloque === undefined ? null : bloque));
  }

  return salida;
}

/**
 * Todas las fuentes citadas por un conjunto de investigaciones, en el orden en
 * que aparecen.
 *
 * @param {Array<object>} investigaciones
 * @returns {Array<{title: string, url: string, date: string|null}>}
 */
function recolectarFuentes(investigaciones) {
  const fuentes = [];

  for (const inv of investigaciones) {
    for (const f of inv.findings) {
      fuentes.push({
        title: f.sourceTitle || '',
        url: f.sourceUrl,
        date: f.publishedAt,
        verified: f.verified === true,
      });
    }
    for (const c of inv.conflicts) {
      for (const v of c.versions) fuentes.push({ title: '', url: v.sourceUrl, date: null });
    }
  }

  return fuentes;
}

module.exports = {
  MESES_PARA_CIFRA_RANCIA,
  esRancia,
  RESEARCH_OUTCOME,
  esCifraFundamental,
  parsearRespuesta,
  filtrarHallazgos,
  filtrarConflictos,
  escaparControlesEnCadenas,
  normalizarDatos,
  normalizarInvestigacion,
  normalizarInvestigacionLote,
  recolectarFuentes,
};
