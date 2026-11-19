/**
 * HU 6.16 — El desglose de cada ETF se guarda una vez y lo usan todas las consultas.
 *
 * Hasta esta historia, el desglose de un ETF (sus posiciones, sectores y
 * países) solo vivía en la memoria de cada instancia de `queryOperations`.
 * Con pocos usuarios el servicio se apaga entre visitas, así que el 72 % de los
 * cálculos de la distribución esperaba 12–21 s a las fuentes externas
 * (medición del 2026-11-05 al 2026-11-19).
 *
 * Aquí vive el desglose **compartido**: un documento por ETF en
 * `etfBreakdowns/{SÍMBOLO}`, común a todos los inversores (RN-6.16-E).
 *
 * - **Formato (RN-6.16-D, D2).** Solo los campos que usa el cálculo, en JSON
 *   sin redondear, comprimido con gzip en un campo binario. Un binario es una
 *   sola entrada de índice; 9.800 posiciones como arreglo de mapas serían miles.
 *   Si el comprimido pasa de `BYTES_POR_PARTE`, la raíz lleva la parte 0 y el
 *   resto va en `partes/{i}`. Todo se escribe en **un lote atómico** y cada
 *   parte lleva la `version` de la raíz: una lectura con versiones mezcladas se
 *   descarta y cuenta como no guardado (ajuste 2 del PO).
 * - **Vigencia (RN-6.16-B).** 7 días desde que se obtuvo. Vencido se sigue
 *   usando, y su refresco va aparte por Cloud Tasks (D1).
 * - **Un intento al día (ajuste 1 del PO).** La raíz guarda `ultimoIntentoEn`
 *   (`AAAA-MM-DD` en Nueva York), también si el intento falla. Quien encuentra
 *   un vencido lo **reclama** con una transacción antes de encolar.
 * - **La fuente falla (RN-6.16-C, RN-6.16-F).** Si no responde, no trae datos o
 *   trae menos de la mitad de las posiciones guardadas, se conserva el guardado
 *   y se anota `ultimoFallo` con su motivo.
 *
 * El cliente de la fuente (`fetchETFDataFromAPI*`) se movió aquí desde
 * `portfolioDistributionService.js` para que lo compartan la distribución, la
 * tarea del refresco y la herramienta de la publicación.
 *
 * @module services/etfBreakdown/etfBreakdownService
 * @see platform-docs/stories/6.16-desglose-etf-compartido/refinamiento.md (T1)
 */

const zlib = require('zlib');
const { DateTime } = require('luxon');
const { getFunctions } = require('firebase-admin/functions');

const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('../config');

/**
 * Registro con `console.*` y prefijo `[etfBreakdown]` (estándares §7). No se
 * usa `crearLoggerDeConsola` de `utils/logger` porque los tests existentes de
 * la distribución sustituyen ese módulo entero por un doble sin esa función.
 */
const logger = {
  info: (mensaje) => console.log(`[etfBreakdown] ${mensaje}`),
  warn: (mensaje) => console.warn(`[etfBreakdown] ${mensaje}`),
};

/** Colección del desglose compartido. */
const COLECCION = 'etfBreakdowns';
/** Subcolección con las partes 1..n-1 cuando el comprimido no cabe en la raíz. */
const SUBCOLECCION_PARTES = 'partes';
/** Formato del campo `datos`: JSON comprimido con gzip, en partes binarias. */
const FORMATO = 'gzip-json-v1';

/** RN-6.16-B: el desglose guardado vale 7 días desde que se obtuvo (decisión del PO, 2026-11-19). */
const VIGENCIA_DIAS = 7;
const VIGENCIA_MS = VIGENCIA_DIAS * 24 * 60 * 60 * 1000;

/**
 * Bytes comprimidos por parte. Un documento de Firestore no pasa de 1 MiB
 * (1.048.576 B); 900.000 deja margen para los metadatos de la raíz.
 */
const BYTES_POR_PARTE = 900000;

/** Cola de Cloud Tasks del refresco aparte; se llama como la función que la atiende. */
const COLA_REFRESCO = 'etfBreakdownRefreshTask';

/** La fecha del «un intento al día» se cuenta en el huso del mercado (estándar §4.10). */
const ZONA = 'America/New_York';

/**
 * FIX-ETF-EU-001: timeout de cada llamada a /v1/etf/{symbol}/unified.
 * La cadena de fuentes del API tarda ~18 s en frío para un UCITS europeo.
 */
const ETF_API_TIMEOUT_MS = 25000;

/** Campos de cada lista que usa el cálculo de la distribución (y nada más). */
const CAMPOS = {
  holdings: ['symbol', 'isin', 'name', 'weight', 'asset_type'],
  sectors: ['name', 'weight'],
  countries: ['name', 'weight'],
};

// ============================================================================
// Interruptor (D7, RN-6-H)
// ============================================================================

/**
 * HU 6.16 (D7) — ¿Se usa el desglose compartido?
 *
 * Encendido salvo `ETF_BREAKDOWN_STORE=false`. Apagado, la distribución hace
 * exactamente lo de hoy: 24 h en memoria, sin leer ni escribir `etfBreakdowns`
 * y sin encolar tareas (ajuste 4 del PO).
 *
 * @returns {boolean}
 */
function desgloseCompartidoActivo() {
  return process.env.ETF_BREAKDOWN_STORE !== 'false';
}

// ============================================================================
// Cliente de la fuente (movido sin cambios de lógica desde portfolioDistributionService)
// ============================================================================

/**
 * ¿La respuesta trae algo con que desglosar el ETF?
 *
 * Basta con posiciones, sectores o países: una fuente puede publicar el
 * desglose sectorial o geográfico sin la cartera (justETF sin top-10).
 *
 * @param {Object|null} data - Respuesta de /v1/etf/{symbol}/unified
 * @returns {boolean} true si aporta holdings, sectores o países
 */
function hasUsableETFData(data) {
  if (!data) return false;
  return Boolean(
    (data.holdings && data.holdings.length > 0) ||
    (data.sectors && data.sectors.length > 0) ||
    (data.countries && data.countries.length > 0)
  );
}

/**
 * Ejecuta `fetchFn` sobre los símbolos de `limit` en `limit`, con una pausa
 * corta entre lotes para no provocar 429.
 *
 * @param {string[]} symbols
 * @param {(symbol: string) => Promise<*>} fetchFn
 * @param {number} limit
 * @returns {Promise<Array<{symbol: string, data: *}>>}
 */
async function fetchWithConcurrencyLimit(symbols, fetchFn, limit) {
  const results = [];

  for (let i = 0; i < symbols.length; i += limit) {
    const batch = symbols.slice(i, i + limit);
    const batchResults = await Promise.all(
      batch.map(async (symbol) => ({
        symbol,
        data: await fetchFn(symbol),
      }))
    );
    results.push(...batchResults);

    if (i + limit < symbols.length) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  return results;
}

/**
 * Pide el desglose de un ETF a finance-query.
 *
 * Devuelve `'RATE_LIMITED'` ante un 429 (para reintentar) y `null` ante un 204,
 * un error HTTP, un cuerpo vacío o un timeout: para quien llama, todos son
 * «la fuente no respondió».
 *
 * @param {string} symbol
 * @returns {Promise<Object|null|'RATE_LIMITED'>}
 */
async function fetchETFDataFromAPI(symbol) {
  const baseUrl = FINANCE_QUERY_API_URL.replace('/v1', '');
  const url = `${baseUrl}/v1/etf/${symbol}/unified`;
  const headers = getServiceHeaders({ Accept: 'application/json' });

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), ETF_API_TIMEOUT_MS);

  try {
    const response = await fetch(url, { headers, signal: controller.signal });
    clearTimeout(timeoutId);

    if (response.status === 429) return 'RATE_LIMITED';
    if (response.status === 204) return null;
    if (!response.ok) {
      logger.warn(`La fuente respondió ${response.status} para ${symbol}`);
      return null;
    }

    const text = await response.text();
    if (!text || text.trim() === '') return null;
    return JSON.parse(text);
  } catch (error) {
    clearTimeout(timeoutId);
    logger.warn(`Error pidiendo el desglose de ${symbol} a la fuente: ${error.name}: ${error.message}`);
    return null;
  }
}

/**
 * `fetchETFDataFromAPI` con reintentos ante 429 (backoff exponencial).
 *
 * @param {string} symbol
 * @param {number} [maxRetries=3]
 * @returns {Promise<Object|null>}
 */
async function fetchETFDataFromAPIWithRetry(symbol, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const result = await fetchETFDataFromAPI(symbol);
    if (result !== 'RATE_LIMITED') return result;

    const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  logger.warn(`Se agotaron los reintentos por 429 de ${symbol}`);
  return null;
}

// ============================================================================
// Formato del desglose guardado (D2, RN-6.16-D)
// ============================================================================

/**
 * Normaliza un símbolo como lo hace el cálculo (`trim` + mayúsculas).
 *
 * @param {string} symbol
 * @returns {string}
 */
function normalizarSimbolo(symbol) {
  return String(symbol || '').trim().toUpperCase();
}

/**
 * Id del documento: el símbolo normalizado. Una `/` partiría la ruta, así que
 * se sustituye (ningún ETF de hoy la lleva).
 *
 * @param {string} symbol
 * @returns {string}
 */
function idDeDocumento(symbol) {
  return normalizarSimbolo(symbol).replace(/\//g, '_');
}

/**
 * Se queda solo con los campos que usa el cálculo (`CAMPOS`), sin tocar sus
 * valores. Un campo ausente no se escribe: para el cálculo, ausente y
 * `undefined` son lo mismo.
 *
 * @param {Object} data - Respuesta de la fuente
 * @returns {{holdings: Object[], sectors: Object[], countries: Object[]}}
 */
function proyectarDesglose(data) {
  const proyectar = (lista, campos) => (Array.isArray(lista) ? lista : []).map((item) => {
    const salida = {};
    for (const campo of campos) {
      if (item && item[campo] !== undefined) salida[campo] = item[campo];
    }
    return salida;
  });

  return {
    holdings: proyectar(data?.holdings, CAMPOS.holdings),
    sectors: proyectar(data?.sectors, CAMPOS.sectors),
    countries: proyectar(data?.countries, CAMPOS.countries),
  };
}

/**
 * JSON → gzip → partes de como mucho `bytesPorParte`.
 *
 * @param {Object} desglose
 * @param {number} [bytesPorParte=BYTES_POR_PARTE]
 * @returns {Buffer[]}
 */
function codificar(desglose, bytesPorParte = BYTES_POR_PARTE) {
  const comprimido = zlib.gzipSync(Buffer.from(JSON.stringify(desglose), 'utf8'));
  const partes = [];
  for (let i = 0; i < comprimido.length; i += bytesPorParte) {
    partes.push(comprimido.subarray(i, i + bytesPorParte));
  }
  return partes.length > 0 ? partes : [comprimido];
}

/**
 * Partes → gzip → JSON. Lanza si el contenido está corrupto.
 *
 * @param {Array<Buffer|Uint8Array>} partes - En orden
 * @returns {Object}
 */
function decodificar(partes) {
  const comprimido = Buffer.concat(partes.map((p) => Buffer.from(p)));
  return JSON.parse(zlib.gunzipSync(comprimido).toString('utf8'));
}

/**
 * La fecha de hoy en Nueva York, `AAAA-MM-DD`.
 *
 * @param {Date} ahora
 * @returns {string}
 */
function hoyEnNuevaYork(ahora) {
  return DateTime.fromJSDate(ahora).setZone(ZONA).toISODate();
}

/**
 * RN-6.16-B — ¿El desglose guardado tiene más de 7 días?
 *
 * @param {{obtenidoEn: string}} meta
 * @param {Date} ahora
 * @returns {boolean}
 */
function estaVencido(meta, ahora) {
  const obtenido = Date.parse(meta?.obtenidoEn);
  if (!Number.isFinite(obtenido)) return true;
  return ahora.getTime() - obtenido > VIGENCIA_MS;
}

/**
 * Los metadatos de una raíz, sin el binario.
 *
 * @param {Object} raiz - Datos del documento raíz
 * @returns {{version: string, obtenidoEn: string, ultimoIntentoEn: string|null,
 *   posiciones: number, partes: number, bytes: number, ultimoFallo: Object|null}}
 */
function metaDeRaiz(raiz) {
  return {
    version: raiz.version,
    obtenidoEn: raiz.obtenidoEn,
    ultimoIntentoEn: raiz.ultimoIntentoEn || null,
    posiciones: raiz.posiciones || 0,
    partes: raiz.partes || 1,
    bytes: raiz.bytes || 0,
    ultimoFallo: raiz.ultimoFallo || null,
  };
}

// ============================================================================
// Escritura y lectura
// ============================================================================

/**
 * Guarda el desglose de un ETF (D2, RN-6.16-D, ajuste 2 del PO).
 *
 * Raíz y partes van en **un lote atómico**: nadie lee nunca una versión a
 * medias. El mismo lote borra las partes sobrantes de una versión anterior más
 * larga. Guardar cuenta como el intento del día (`ultimoIntentoEn`).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} symbol
 * @param {Object} data - Respuesta de la fuente (se proyecta aquí)
 * @param {Object} [opciones]
 * @param {Date} [opciones.ahora]
 * @param {number} [opciones.bytesPorParte] - Solo para pruebas del troceado
 * @param {{partes: number}|null} [opciones.anterior] - Metadatos ya leídos; si
 *   no se pasan, se lee la raíz para saber cuántas partes sobran
 * @returns {Promise<{version: string, partes: number, bytes: number, posiciones: number, escrituras: number}>}
 */
async function guardarDesglose(db, symbol, data, opciones = {}) {
  const ahora = opciones.ahora || new Date();
  const bytesPorParte = opciones.bytesPorParte || BYTES_POR_PARTE;
  const ref = db.collection(COLECCION).doc(idDeDocumento(symbol));

  let anterior = opciones.anterior;
  if (anterior === undefined) {
    const snap = await ref.get();
    anterior = snap.exists ? metaDeRaiz(snap.data()) : null;
  }

  const desglose = proyectarDesglose(data);
  const partes = codificar(desglose, bytesPorParte);
  const version = `${ahora.getTime()}-${Math.random().toString(36).slice(2, 8)}`;
  const bytes = partes.reduce((suma, p) => suma + p.length, 0);

  const lote = db.batch();
  let escrituras = 0;
  lote.set(ref, {
    symbol: normalizarSimbolo(symbol),
    formato: FORMATO,
    version,
    obtenidoEn: ahora.toISOString(),
    ultimoIntentoEn: hoyEnNuevaYork(ahora),
    ultimoFallo: null,
    posiciones: desglose.holdings.length,
    sectores: desglose.sectors.length,
    paises: desglose.countries.length,
    partes: partes.length,
    bytes,
    datos: partes[0],
  });
  escrituras++;

  for (let i = 1; i < partes.length; i++) {
    lote.set(ref.collection(SUBCOLECCION_PARTES).doc(String(i)), { indice: i, version, datos: partes[i] });
    escrituras++;
  }
  for (let i = partes.length; i < (anterior?.partes || 1); i++) {
    lote.delete(ref.collection(SUBCOLECCION_PARTES).doc(String(i)));
    escrituras++;
  }

  await lote.commit();
  return { version, partes: partes.length, bytes, posiciones: desglose.holdings.length, escrituras };
}

/**
 * Lee el desglose guardado de varios ETF en una sola ida (`getAll`).
 *
 * Un documento con formato desconocido, partes que faltan, versiones mezcladas
 * o contenido corrupto se **descarta** y cuenta como no guardado (ajuste 2 del
 * PO): quien llama lo tratará como un ETF que nunca se guardó.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string[]} symbols - Normalizados
 * @returns {Promise<{guardados: Map<string, {data: Object, meta: Object}>, lecturas: number, descartados: string[]}>}
 */
async function leerDesgloses(db, symbols) {
  const guardados = new Map();
  const descartados = [];
  if (symbols.length === 0) return { guardados, lecturas: 0, descartados };

  const col = db.collection(COLECCION);
  const refs = symbols.map((s) => col.doc(idDeDocumento(s)));
  const raices = await db.getAll(...refs);
  let lecturas = refs.length;

  for (let i = 0; i < symbols.length; i++) {
    const symbol = symbols[i];
    const snap = raices[i];
    if (!snap || !snap.exists) continue;

    const raiz = snap.data();
    if (raiz.formato !== FORMATO || !raiz.datos) {
      descartados.push(symbol);
      continue;
    }

    try {
      const partes = [raiz.datos];
      const total = raiz.partes || 1;
      if (total > 1) {
        const refsPartes = [];
        for (let p = 1; p < total; p++) refsPartes.push(refs[i].collection(SUBCOLECCION_PARTES).doc(String(p)));
        const extras = await db.getAll(...refsPartes);
        lecturas += refsPartes.length;
        const completas = extras.every((e) => e && e.exists && e.data().version === raiz.version);
        if (!completas) {
          logger.warn(`${symbol}: las partes no coinciden con la versión ${raiz.version}; se descarta y cuenta como no guardado`);
          descartados.push(symbol);
          continue;
        }
        extras.forEach((e) => partes.push(e.data().datos));
      }
      guardados.set(symbol, { data: decodificar(partes), meta: metaDeRaiz(raiz) });
    } catch (error) {
      logger.warn(`${symbol}: el desglose guardado no se pudo leer (${error.message}); cuenta como no guardado`);
      descartados.push(symbol);
    }
  }

  return { guardados, lecturas, descartados };
}

// ============================================================================
// Refresco (RN-6.16-B, RN-6.16-C, RN-6.16-F)
// ============================================================================

/**
 * RN-6.16-F — ¿Sirve la respuesta de la fuente para reemplazar el guardado?
 *
 * - `null`: la fuente no respondió (error, timeout, 204, cuerpo vacío).
 * - Sin posiciones, sectores ni países: no trae datos.
 * - Menos de la mitad de las posiciones guardadas: respuesta corta (p. ej. el
 *   top-10 de un fondo europeo en lugar de 504). La mitad exacta sí vale.
 *
 * @param {Object|null} nuevo - Respuesta de la fuente
 * @param {{posiciones: number}|null} guardado - Metadatos del guardado, si hay
 * @returns {{valido: boolean, motivo: string|null}}
 */
function validarRefresco(nuevo, guardado) {
  if (nuevo === null || nuevo === undefined) return { valido: false, motivo: 'la fuente no respondió' };
  if (!hasUsableETFData(nuevo)) return { valido: false, motivo: 'la fuente no trajo datos' };

  const posiciones = Array.isArray(nuevo.holdings) ? nuevo.holdings.length : 0;
  if (guardado && posiciones < guardado.posiciones / 2) {
    return {
      valido: false,
      motivo: `respuesta corta: ${posiciones} posiciones frente a ${guardado.posiciones} guardadas`,
    };
  }
  return { valido: true, motivo: null };
}

/**
 * Trae el desglose de la fuente y lo guarda si sirve; si no, conserva el
 * guardado y anota el fallo (RN-6.16-C). Lo usan la tarea del refresco y la
 * herramienta de la publicación.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} symbol
 * @param {Object} [opciones]
 * @param {Date} [opciones.ahora]
 * @param {(symbol: string) => Promise<Object|null>} [opciones.obtener] - La fuente
 * @returns {Promise<{resultado: 'actualizado'|'conservado'|'sin-guardar', motivo: string|null,
 *   nuevo: Object|null, posiciones: number|null}>}
 *   `nuevo` es la respuesta tal cual llegó (la comparación la usa como referencia).
 */
async function refrescarDesglose(db, symbol, opciones = {}) {
  const ahora = opciones.ahora || new Date();
  const obtener = opciones.obtener || fetchETFDataFromAPIWithRetry;
  const normalizado = normalizarSimbolo(symbol);
  const ref = db.collection(COLECCION).doc(idDeDocumento(normalizado));

  const snap = await ref.get();
  const guardado = snap.exists ? metaDeRaiz(snap.data()) : null;

  const nuevo = await obtener(normalizado);
  const { valido, motivo } = validarRefresco(nuevo, guardado);

  if (valido) {
    const r = await guardarDesglose(db, normalizado, nuevo, { ahora, anterior: guardado });
    logger.info(`Desglose de ${normalizado} guardado: ${r.posiciones} posiciones, ${r.bytes} B en ${r.partes} parte(s)` +
      (guardado ? ` (antes ${guardado.posiciones}, del ${guardado.obtenidoEn})` : ' (primera vez)'));
    return { resultado: 'actualizado', motivo: null, nuevo, posiciones: r.posiciones };
  }

  if (!guardado) {
    logger.warn(`Refresco fallido de ${normalizado}: ${motivo}; nunca se guardó, queda sin desglosar`);
    return { resultado: 'sin-guardar', motivo, nuevo, posiciones: null };
  }

  await ref.set({
    ultimoIntentoEn: hoyEnNuevaYork(ahora),
    ultimoFallo: { en: ahora.toISOString(), motivo },
  }, { merge: true });
  logger.warn(`Refresco fallido de ${normalizado}: ${motivo}; se conserva el guardado ` +
    `(${guardado.posiciones} posiciones, del ${guardado.obtenidoEn}) y se reintenta otro día`);
  return { resultado: 'conservado', motivo, nuevo, posiciones: guardado.posiciones };
}

/**
 * Ajuste 1 del PO — Reclama el intento de refresco del día para un ETF.
 *
 * En una transacción: si `ultimoIntentoEn` ya es hoy, otro cálculo lo reclamó
 * (o el intento ya se hizo) y no se encola; si no, se pone en hoy. Así hay
 * como mucho un intento al día por ETF entre todas las instancias, sin
 * depender de los nombres de tarea de Cloud Tasks.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} symbol
 * @param {string} hoy - `AAAA-MM-DD`
 * @returns {Promise<boolean>} true si este llamador se quedó con el intento
 */
async function reclamarIntentoDelDia(db, symbol, hoy) {
  const ref = db.collection(COLECCION).doc(idDeDocumento(symbol));
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (!snap.exists || snap.data().ultimoIntentoEn === hoy) return false;
    tx.update(ref, { ultimoIntentoEn: hoy });
    return true;
  });
}

/**
 * RN-6.16-B — Programa el refresco aparte de un desglose vencido.
 *
 * Nadie espera: quien llama no lo aguarda para calcular. Si el intento del día
 * ya está tomado, no hace nada. Si el encolado falla, el día queda gastado y
 * se reintenta mañana (RN-6.16-C); se registra.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} symbol
 * @param {{ultimoIntentoEn: string|null}} meta - Se actualiza en memoria si se reclama
 * @param {Object} [opciones]
 * @param {Date} [opciones.ahora]
 * @param {(cuerpo: Object) => Promise<void>} [opciones.encolar] - Para pruebas
 * @returns {Promise<{estado: 'encolado'|'ya-intentado-hoy'|'error', lecturas: number, escrituras: number}>}
 *   Las lecturas y escrituras son las de la transacción, para la medición (RN-6-I).
 */
async function programarRefresco(db, symbol, meta, opciones = {}) {
  const hoy = hoyEnNuevaYork(opciones.ahora || new Date());
  if (meta.ultimoIntentoEn === hoy) return { estado: 'ya-intentado-hoy', lecturas: 0, escrituras: 0 };

  let reclamado = false;
  try {
    reclamado = await reclamarIntentoDelDia(db, symbol, hoy);
    meta.ultimoIntentoEn = hoy;
    if (!reclamado) return { estado: 'ya-intentado-hoy', lecturas: 1, escrituras: 0 };

    const encolar = opciones.encolar
      || ((cuerpo) => getFunctions().taskQueue(COLA_REFRESCO).enqueue(cuerpo, { dispatchDeadlineSeconds: 120 }));
    await encolar({ symbol });
    return { estado: 'encolado', lecturas: 1, escrituras: 1 };
  } catch (error) {
    logger.warn(`No se pudo programar el refresco de ${symbol}: ${error.message}; se reintenta otro día`);
    return { estado: 'error', lecturas: 1, escrituras: reclamado ? 1 : 0 };
  }
}

module.exports = {
  // Interruptor
  desgloseCompartidoActivo,
  // Cliente de la fuente
  hasUsableETFData,
  fetchWithConcurrencyLimit,
  fetchETFDataFromAPI,
  fetchETFDataFromAPIWithRetry,
  // Guardado
  normalizarSimbolo,
  proyectarDesglose,
  guardarDesglose,
  leerDesgloses,
  estaVencido,
  hoyEnNuevaYork,
  // Refresco
  validarRefresco,
  refrescarDesglose,
  reclamarIntentoDelDia,
  programarRefresco,
  // Constantes
  COLECCION,
  SUBCOLECCION_PARTES,
  FORMATO,
  VIGENCIA_DIAS,
  BYTES_POR_PARTE,
  COLA_REFRESCO,
  ETF_API_TIMEOUT_MS,
};
