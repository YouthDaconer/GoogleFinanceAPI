/**
 * HU 6.7 — Las reglas del aviso de consumo: qué cruza su umbral, desde qué
 * hora, qué ya se avisó hoy y qué dice la línea que se convierte en correo.
 *
 * **Sin red ni Firestore.** Lo usan tres sitios y los tres deben decidir lo
 * mismo: el vigilante programado (`consumptionWatchScheduled.js`), el aviso de
 * prueba (`scripts/lanzarAvisoConsumoPrueba.js`) y el simulador del pasado
 * (`scripts/simularAvisosConsumo.js`). Un umbral o un día de Colombia en dos
 * sitios serían dos reglas.
 *
 * **El día se cuenta en hora de Colombia** (RN-6.7-L). Es un choque declarado
 * con el estándar de CF, que fija `America/New_York` para las fechas: manda la
 * regla de la HU. A las 20:30 de Colombia ya es el día siguiente en UTC, y sigue
 * siendo el mismo día del aviso (QA-04).
 *
 * Decisiones sobre los vacíos de `qa.md` (refinamiento, apartado de vacíos):
 * - **IA, ventana móvil** (Vacío 1): un análisis está fuera de cupo si el
 *   usuario tiene otro **menos de 7×24 h antes**. Exactamente 7 días después ya
 *   hay cupo, igual que en el lanzador (`copilotQuota.nextAvailableAt`). Para un
 *   usuario sin acceso al Copilot, cualquiera está fuera de cupo.
 * - **Varios días por encima** (Vacío 3): lecturas y escrituras se reinician
 *   cada día y avisan cada día que crucen; el almacenamiento avisa al cruzar y
 *   al volver por debajo, no cada día que siga arriba; cada análisis fuera de
 *   cupo se avisa una sola vez.
 * - **Almacenamiento** (Vacío 4): como mucho un cruce y una normalización por
 *   día; 0,80 GiB exactos no está por encima.
 * - **Segundo aviso de IA el mismo día** (Vacío 7, decidido por el PO): sí, con
 *   los usuarios aún no avisados ese día. Nadie aparece dos veces el mismo día.
 *
 * @module services/consumption/consumptionAlertService
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T1)
 */

const { DateTime } = require('luxon');
const { DIAS_ENTRE_ANALISIS } = require('../copilot/copilotConstants');

/** Zona del día del aviso (RN-6.7-L). */
const ZONA_AVISO = 'America/Bogota';

/** 1 GiB, la unidad de la tabla de la HU y del nivel gratuito de Firestore. */
const BYTES_POR_GIB = 1073741824;

/** Umbrales de RN-6.7-J. Se avisa al **superarlos**, no al igualarlos (QA-03). */
const UMBRALES = Object.freeze({
  lecturas: 200000,
  escrituras: 100000,
  almacenamientoGib: 0.8,
});

/** Los tipos de aviso. Cada uno tiene su propia política de Monitoring. */
const CONSUMO = Object.freeze({
  LECTURAS: 'lecturas',
  ESCRITURAS: 'escrituras',
  ALMACENAMIENTO: 'almacenamiento',
  ALMACENAMIENTO_NORMAL: 'almacenamiento-normal',
  IA: 'ia',
  PRUEBA: 'prueba',
});

/**
 * Prefijo del mensaje del aviso y evento de la línea JSON. Las políticas de
 * `scripts/consumoAlerts.js` filtran por `jsonPayload.event` y
 * `jsonPayload.consumo`: cambiar uno sin el otro deja el aviso mudo (lo vigila
 * un test).
 */
const PREFIJO_AVISO = '[CONSUMO-AVISO]';
const EVENTO_AVISO = 'consumption_alert';

/** Colección del estado de los avisos: un documento por día y uno de estado. */
const COLECCION_AVISOS = 'consumptionAlerts';
const DOC_ESTADO = 'estado';

/**
 * Tope de usuarios nombrados en el correo de IA (D5). El valor de una etiqueta
 * de Monitoring tiene un tamaño máximo; la lista completa queda en
 * `consumptionAlerts/{día}`.
 */
const MAX_USUARIOS_EN_CORREO = 20;
const TOPE_TEXTO_USUARIOS = 900;

/**
 * Horas hacia atrás en que se buscan análisis fuera de cupo. Más de un día,
 * para que un análisis lanzado a las 23:50 y guardado a las 00:05 no se pierda
 * entre dos días.
 */
const VENTANA_CANDIDATOS_HORAS = 26;

const MS_CUPO = DIAS_ENTRE_ANALISIS * 24 * 60 * 60 * 1000;

const formatoNumero = new Intl.NumberFormat('es-CO');

/**
 * @param {Date|string} instante
 * @returns {DateTime}
 */
function enColombia(instante) {
  const fecha = typeof instante === 'string'
    ? DateTime.fromISO(instante, { zone: 'utc' })
    : DateTime.fromJSDate(instante, { zone: 'utc' });
  return fecha.setZone(ZONA_AVISO);
}

/**
 * El día del aviso al que pertenece un instante (RN-6.7-L).
 *
 * @param {Date|string} instante
 * @returns {string} `YYYY-MM-DD`
 */
function diaColombia(instante) {
  return enColombia(instante).toISODate();
}

/**
 * De las 00:00 de un día de Colombia a las 00:00 del siguiente, en UTC.
 *
 * @param {string} dia - `YYYY-MM-DD`
 * @returns {{inicio: string, fin: string}} ISO 8601 en UTC
 */
function intervaloDelDia(dia) {
  const inicio = DateTime.fromISO(dia, { zone: ZONA_AVISO }).startOf('day');
  return {
    inicio: inicio.toUTC().toISO(),
    fin: inicio.plus({ days: 1 }).toUTC().toISO(),
  };
}

/**
 * @param {Date|string} instante
 * @returns {string} `2026-11-19 14:20 (hora de Colombia)`
 */
function horaColombia(instante) {
  return `${enColombia(instante).toFormat('yyyy-LL-dd HH:mm')} (hora de Colombia)`;
}

/** @param {number} n @returns {string} `200.001` */
function formatoEntero(n) {
  return formatoNumero.format(Math.round(n));
}

/** @param {number} bytes @returns {string} `0,810 GiB` */
function formatoGib(bytes) {
  return `${(bytes / BYTES_POR_GIB).toFixed(3).replace('.', ',')} GiB`;
}

/**
 * @param {Array<{valor: number}>} puntos
 * @returns {number}
 */
function totalDe(puntos) {
  return (puntos || []).reduce((suma, p) => suma + p.valor, 0);
}

/**
 * El minuto en que el total acumulado del día pasó el umbral (D3).
 *
 * @param {Array<{inicio: string, valor: number}>} puntos - Serie por minuto, en orden
 * @param {number} umbral
 * @returns {string|null} ISO del inicio de ese minuto
 */
function minutoDelCruce(puntos, umbral) {
  let acumulado = 0;
  for (const punto of puntos || []) {
    acumulado += punto.valor;
    if (acumulado > umbral) return punto.inicio;
  }
  return null;
}

/**
 * Lecturas o escrituras: total del día de Colombia contra su umbral.
 *
 * @param {object} p
 * @param {'lecturas'|'escrituras'} p.consumo
 * @param {Array<{inicio: string, valor: number}>} p.puntos - La serie del día
 * @param {boolean} p.yaAvisado - Ya hubo aviso de este consumo hoy (RN-6.7-L)
 * @returns {{consumo: string, valor: number, umbral: number, desde: string}|null}
 */
function evaluarConteoDiario({ consumo, puntos, yaAvisado }) {
  const umbral = UMBRALES[consumo];
  if (umbral === undefined) throw new Error(`Consumo sin umbral diario: ${consumo}`);

  const total = totalDe(puntos);
  if (yaAvisado || total <= umbral) return null;

  return { consumo, valor: total, umbral, desde: minutoDelCruce(puntos, umbral) };
}

/**
 * La racha actual del almacenamiento: si el último valor está por encima, y
 * desde cuándo sin interrupción (D3).
 *
 * @param {Array<{fin: string, valor: number}>} puntos - Serie en orden, en bytes
 * @returns {{porEncima: boolean, valor: number, desde: string}|null}
 */
function rachaAlmacenamiento(puntos) {
  if (!puntos || puntos.length === 0) return null;
  const umbralBytes = UMBRALES.almacenamientoGib * BYTES_POR_GIB;
  const encima = (p) => p.valor > umbralBytes;

  const ultimo = puntos[puntos.length - 1];
  const porEncima = encima(ultimo);
  let i = puntos.length - 1;
  while (i > 0 && encima(puntos[i - 1]) === porEncima) i -= 1;

  return { porEncima, valor: ultimo.valor, desde: puntos[i].fin };
}

/**
 * Almacenamiento: aviso al cruzar y aviso de vuelta a la normalidad, como mucho
 * uno de cada por día (RN-6.7-L, Vacíos 3 y 4).
 *
 * @param {object} p
 * @param {Array<{fin: string, valor: number}>} p.puntos
 * @param {{porEncima?: boolean, ultimoCruceDia?: string, ultimaNormalDia?: string}} [p.estado]
 * @param {string} p.hoy - Día de Colombia
 * @returns {{aviso: object|null, estadoNuevo: object|null}} `estadoNuevo` es `null` si no cambió
 */
function evaluarAlmacenamiento({ puntos, estado, hoy }) {
  const racha = rachaAlmacenamiento(puntos);
  const previo = estado || {};
  if (!racha || racha.porEncima === Boolean(previo.porEncima)) {
    return { aviso: null, estadoNuevo: null };
  }

  const umbral = UMBRALES.almacenamientoGib;
  if (racha.porEncima) {
    const tocaAviso = previo.ultimoCruceDia !== hoy;
    return {
      aviso: tocaAviso
        ? { consumo: CONSUMO.ALMACENAMIENTO, valor: racha.valor, umbral, desde: racha.desde }
        : null,
      estadoNuevo: { ...previo, porEncima: true, desde: racha.desde, ...(tocaAviso ? { ultimoCruceDia: hoy } : {}) },
    };
  }

  const tocaAviso = previo.ultimaNormalDia !== hoy;
  return {
    aviso: tocaAviso
      ? { consumo: CONSUMO.ALMACENAMIENTO_NORMAL, valor: racha.valor, umbral, desde: racha.desde }
      : null,
    estadoNuevo: { ...previo, porEncima: false, desde: racha.desde, ...(tocaAviso ? { ultimaNormalDia: hoy } : {}) },
  };
}

/**
 * Los análisis de un usuario que superaron su cupo (RN-6.7-J, Vacío 1).
 *
 * @param {Array<{id: string, createdAt: string}>} analisis
 * @param {boolean} conAcceso - `copilotQuota.tieneAcceso` al evaluar (D2)
 * @returns {Array<{id: string, createdAt: string}>} En orden de lanzamiento
 */
function analisisFueraDeCupo(analisis, conAcceso) {
  const ordenados = [...(analisis || [])]
    .filter((a) => a && a.createdAt)
    .sort((a, b) => Date.parse(a.createdAt) - Date.parse(b.createdAt));
  if (!conAcceso) return ordenados;

  // Ordenados, basta mirar el anterior: si hay alguno a menos de 7 días, es él.
  return ordenados.filter(
    (a, i) => i > 0 && Date.parse(a.createdAt) - Date.parse(ordenados[i - 1].createdAt) < MS_CUPO,
  );
}

/** Clave con la que se recuerda un análisis ya avisado. */
const claveAnalisis = (uid, id) => `${uid}/${id}`;

/**
 * IA: un aviso que lista a todos los usuarios con análisis fuera de cupo aún no
 * avisados (RN-6.7-O, Vacíos 3 y 7).
 *
 * Un usuario ya avisado hoy no vuelve a aparecer hoy: sus análisis nuevos fuera
 * de cupo se dan por avisados sin otro correo (`absorbidos`).
 *
 * @param {object} p
 * @param {Array<{uid: string, conAcceso: boolean, analisis: Array<{id: string, createdAt: string}>}>} p.usuarios
 * @param {Date} p.ahora
 * @param {{usuarios?: string[], analisis?: string[], avisos?: number}} [p.iaHoy] - Estado de hoy
 * @param {{analisis?: string[]}} [p.iaAyer] - Estado del día anterior
 * @returns {{aviso: object|null, iaNuevo: object|null}} `iaNuevo` es `null` si no cambió
 */
function evaluarIa({ usuarios, ahora, iaHoy, iaAyer }) {
  const hoy = iaHoy || {};
  const yaAvisados = new Set([...(hoy.analisis || []), ...((iaAyer || {}).analisis || [])]);
  const usuariosHoy = new Set(hoy.usuarios || []);
  const desdeVentana = ahora.getTime() - VENTANA_CANDIDATOS_HORAS * 60 * 60 * 1000;
  const desdeSieteDias = ahora.getTime() - MS_CUPO;

  const listados = [];
  const absorbidos = [];
  for (const usuario of usuarios || []) {
    const pendientes = analisisFueraDeCupo(usuario.analisis, usuario.conAcceso)
      .filter((a) => Date.parse(a.createdAt) >= desdeVentana)
      .filter((a) => !yaAvisados.has(claveAnalisis(usuario.uid, a.id)));
    if (pendientes.length === 0) continue;

    const claves = pendientes.map((a) => claveAnalisis(usuario.uid, a.id));
    if (usuariosHoy.has(usuario.uid)) {
      absorbidos.push(...claves);
      continue;
    }
    listados.push({
      uid: usuario.uid,
      conAcceso: usuario.conAcceso,
      analisisSieteDias: usuario.analisis.filter((a) => Date.parse(a.createdAt) > desdeSieteDias).length,
      primero: pendientes[0].createdAt,
      claves,
    });
  }

  if (listados.length === 0 && absorbidos.length === 0) return { aviso: null, iaNuevo: null };

  const iaNuevo = {
    usuarios: [...(hoy.usuarios || []), ...listados.map((u) => u.uid)],
    analisis: [...(hoy.analisis || []), ...listados.flatMap((u) => u.claves), ...absorbidos],
    avisos: (hoy.avisos || 0) + (listados.length > 0 ? 1 : 0),
  };
  if (listados.length === 0) return { aviso: null, iaNuevo };

  listados.sort((a, b) => Date.parse(a.primero) - Date.parse(b.primero));
  return {
    aviso: {
      consumo: CONSUMO.IA,
      valor: listados.length,
      desde: listados[0].primero,
      usuarios: listados.map(({ uid, conAcceso, analisisSieteDias }) => ({ uid, conAcceso, analisisSieteDias })),
    },
    iaNuevo,
  };
}

/**
 * La lista de usuarios del correo de IA: solo el identificador y sus análisis
 * de los últimos 7 días, sin más datos personales (RN-6.7-O).
 *
 * @param {Array<{uid: string, conAcceso: boolean, analisisSieteDias: number}>} usuarios
 * @returns {string}
 */
function textoUsuarios(usuarios) {
  const partes = [];
  let largo = 0;
  for (const u of usuarios) {
    const parte = `${u.uid}: ${u.analisisSieteDias} análisis en 7 días${u.conAcceso ? '' : ' (sin acceso al Copilot)'}`;
    if (partes.length >= MAX_USUARIOS_EN_CORREO || largo + parte.length > TOPE_TEXTO_USUARIOS) break;
    partes.push(parte);
    largo += parte.length + 2;
  }
  const resto = usuarios.length - partes.length;
  if (resto === 0) return partes.join('; ');
  return [...partes, `y ${resto} más (lista completa en Firestore)`].join('; ');
}

/** Lo que dice cada aviso: título, valor y umbral legibles. */
const PRESENTACION = Object.freeze({
  [CONSUMO.LECTURAS]: {
    titulo: 'Lecturas de la base de datos por encima del umbral',
    valor: (a) => `${formatoEntero(a.valor)} lecturas hoy`,
    umbral: () => `${formatoEntero(UMBRALES.lecturas)} lecturas al día`,
  },
  [CONSUMO.ESCRITURAS]: {
    titulo: 'Escrituras de la base de datos por encima del umbral',
    valor: (a) => `${formatoEntero(a.valor)} escrituras hoy`,
    umbral: () => `${formatoEntero(UMBRALES.escrituras)} escrituras al día`,
  },
  [CONSUMO.ALMACENAMIENTO]: {
    titulo: 'Almacenamiento de la base de datos por encima del umbral',
    valor: (a) => formatoGib(a.valor),
    umbral: () => `${String(UMBRALES.almacenamientoGib).replace('.', ',')} GiB`,
  },
  [CONSUMO.ALMACENAMIENTO_NORMAL]: {
    titulo: 'Almacenamiento de la base de datos de vuelta por debajo del umbral',
    valor: (a) => formatoGib(a.valor),
    umbral: () => `${String(UMBRALES.almacenamientoGib).replace('.', ',')} GiB`,
  },
  [CONSUMO.IA]: {
    titulo: 'Análisis de IA por encima del cupo',
    valor: (a) => `${a.valor} ${a.valor === 1 ? 'usuario' : 'usuarios'} fuera de cupo`,
    umbral: () => `más de 1 análisis en ${DIAS_ENTRE_ANALISIS} días (Pro y Lifetime), o cualquiera sin acceso al Copilot`,
  },
  [CONSUMO.PRUEBA]: {
    titulo: 'PRUEBA — aviso de consumo',
    valor: (a) => a.valor,
    umbral: () => [
      `lecturas > ${formatoEntero(UMBRALES.lecturas)} al día`,
      `escrituras > ${formatoEntero(UMBRALES.escrituras)} al día`,
      `almacenamiento > ${String(UMBRALES.almacenamientoGib).replace('.', ',')} GiB`,
      `IA > 1 análisis en ${DIAS_ENTRE_ANALISIS} días`,
    ].join(' · '),
  },
});

/**
 * El valor del aviso de prueba: los cuatro consumos medidos al lanzarlo
 * (Vacío 5). Así la prueba comprueba también la medición.
 *
 * @param {{lecturas: number|null, escrituras: number|null, almacenamientoBytes: number|null, usuariosIa: number|null}} medidos
 * @returns {string}
 */
function valorPrueba({ lecturas, escrituras, almacenamientoBytes, usuariosIa }) {
  const o = (v, f) => (v === null || v === undefined ? 'no se pudo medir' : f(v));
  return [
    `lecturas hoy ${o(lecturas, formatoEntero)}`,
    `escrituras hoy ${o(escrituras, formatoEntero)}`,
    `almacenamiento ${o(almacenamientoBytes, formatoGib)}`,
    `usuarios con IA fuera de cupo ${o(usuariosIa, String)}`,
  ].join(' · ');
}

/**
 * La línea de log del aviso. Una sola línea JSON: Cloud Logging la guarda como
 * `jsonPayload` y la política de su tipo extrae de ahí el valor, el umbral, la
 * hora y los usuarios para pintarlos en el correo.
 *
 * @param {{consumo: string, valor: number|string, desde: string, usuarios?: Array}} aviso
 * @returns {object}
 */
function lineaAviso(aviso) {
  const presentacion = PRESENTACION[aviso.consumo];
  if (!presentacion) throw new Error(`Aviso desconocido: ${aviso.consumo}`);

  const valor = presentacion.valor(aviso);
  const umbral = presentacion.umbral();
  const desde = aviso.desde ? horaColombia(aviso.desde) : 'sin hora';
  return {
    severity: 'WARNING',
    message: `${PREFIJO_AVISO} ${presentacion.titulo}: ${valor} (umbral ${umbral}), desde ${desde}`,
    event: EVENTO_AVISO,
    consumo: aviso.consumo,
    titulo: presentacion.titulo,
    valor,
    umbral,
    desde,
    dia: aviso.desde ? diaColombia(aviso.desde) : null,
    usuarios: aviso.usuarios ? textoUsuarios(aviso.usuarios) : '',
  };
}

module.exports = {
  ZONA_AVISO,
  BYTES_POR_GIB,
  UMBRALES,
  CONSUMO,
  PREFIJO_AVISO,
  EVENTO_AVISO,
  COLECCION_AVISOS,
  DOC_ESTADO,
  MAX_USUARIOS_EN_CORREO,
  VENTANA_CANDIDATOS_HORAS,
  diaColombia,
  intervaloDelDia,
  horaColombia,
  formatoEntero,
  formatoGib,
  totalDe,
  minutoDelCruce,
  evaluarConteoDiario,
  rachaAlmacenamiento,
  evaluarAlmacenamiento,
  analisisFueraDeCupo,
  evaluarIa,
  textoUsuarios,
  valorPrueba,
  lineaAviso,
};
