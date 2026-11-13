/**
 * HU 7 — El aviso a operación cuando se repiten los intentos de leer cifras
 * ajenas (Escenario 6, RN-7-D).
 *
 * **Cómo funciona.** Cada rechazo deja una línea en Cloud Logging con
 * `event: 'investor_data_access_denied'` (`utils/investorSession.js`). De ahí:
 *
 * 1. una **métrica basada en logs** cuenta esas líneas con dos etiquetas,
 *    `origin` (la IP) y `target` (el inversor indicado);
 * 2. dos **políticas de Cloud Monitoring** avisan al canal de correo de
 *    operación cuando hay **10 o más en 60 minutos**: una agrupando por origen
 *    («desde un mismo origen») y otra por inversor («contra un mismo inversor»).
 *
 * **Por qué así y no contando en Firestore.** Contar en la base costaría una
 * escritura por rechazo: un atacante convertiría sus intentos en factura. El
 * registro y la métrica están dentro del nivel gratuito.
 *
 * **Decisiones sobre los vacíos de `qa.md`** (refinamiento, apartado de vacíos):
 * - la ventana es **móvil**: Cloud Monitoring evalúa la suma de los últimos
 *   60 min en cada comprobación (Vacío 3);
 * - los rechazos sin sesión **cuentan** (Vacío 1); el origen es la IP (Vacío 2);
 * - un aviso por incidente abierto, sin repetir mientras siga abierto; con las
 *   dos condiciones a la vez llegan dos, uno por política (Vacío 4).
 *
 * Este módulo **no toca la red**: construye los cuerpos que se envían a las
 * APIs de Logging y Monitoring. El borde es `scripts/aplicarAvisosAccesoInversor.js`.
 *
 * @module scripts/investorAccessAlerts
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T6)
 */

const { EVENTO_RECHAZO } = require('../utils/investorSession');

/** Proyecto de GCP. Solo hay producción (no existe entorno de pruebas). */
const PROYECTO = 'portafolio-inversiones';

/** Nombre de la métrica basada en logs. */
const NOMBRE_METRICA = 'investor_data_access_denied';

/** Tipo de la métrica tal como la ve Cloud Monitoring. */
const TIPO_METRICA = `logging.googleapis.com/user/${NOMBRE_METRICA}`;

/** Umbral del Escenario 6: 10 intentos. La condición es «mayor que 9». */
const UMBRAL_INTENTOS = 10;

/** Ventana del Escenario 6: una hora, móvil. */
const VENTANA = '3600s';

/**
 * Buzón de operación. **El canal se elige por esta dirección, no por su nombre**:
 * hasta el 2026-11-12 el único canal del proyecto («Portastock») apuntaba a un
 * buzón que no existía, y todas las alertas se perdían sin que nada fallara.
 * Elegir por dirección hace que, si el canal vuelve a apuntar a otra parte, el
 * script falle en lugar de aplicar avisos que no le llegan a nadie.
 */
const CORREO_OPERACION = 'info@portastock.net';

/** Etiqueta con la que se reconocen las políticas de esta HU al volver a aplicarlas. */
const ETIQUETA_HU = 'hu-7';

/**
 * Las dos dimensiones del aviso. `etiqueta` es la de la métrica; `otra` es la que
 * el aviso no agrupa y que el enlace al registro permite ver.
 */
const DIMENSIONES = Object.freeze({
  origen: Object.freeze({
    etiqueta: 'origin',
    campoRegistro: 'origin',
    otraCampoRegistro: 'targetUserId',
    titulo: 'HU 7 — Intentos repetidos de leer cifras ajenas desde un mismo origen',
    condicion: `≥ ${UMBRAL_INTENTOS} rechazos en 60 min desde una misma IP`,
    queAgrupa: 'Origen (IP)',
    queVer: 'contra qué inversores',
  }),
  inversor: Object.freeze({
    etiqueta: 'target',
    campoRegistro: 'targetUserId',
    otraCampoRegistro: 'origin',
    titulo: 'HU 7 — Intentos repetidos de leer las cifras de un mismo inversor',
    condicion: `≥ ${UMBRAL_INTENTOS} rechazos en 60 min contra un mismo inversor`,
    queAgrupa: 'Inversor (uid)',
    queVer: 'desde qué orígenes',
  }),
});

/**
 * La métrica basada en logs.
 *
 * @returns {object} Cuerpo de `projects.metrics.create` (Logging API v2).
 */
function definicionMetrica() {
  return {
    name: NOMBRE_METRICA,
    description: 'HU 7 — Rechazos de consultas con datos de un inversor (sin sesión, sesión inválida, cifras ajenas o token de servicio inválido).',
    filter: `jsonPayload.event="${EVENTO_RECHAZO}"`,
    metricDescriptor: {
      metricKind: 'DELTA',
      valueType: 'INT64',
      unit: '1',
      labels: [
        { key: 'origin', valueType: 'STRING', description: 'IP de origen de la petición' },
        { key: 'target', valueType: 'STRING', description: 'Inversor indicado en la petición (vacío si no indicó)' },
      ],
    },
    labelExtractors: {
      origin: 'EXTRACT(jsonPayload.origin)',
      target: 'EXTRACT(jsonPayload.targetUserId)',
    },
  };
}

/**
 * Enlace a Logs Explorer con los rechazos de un valor de la dimensión, para ver
 * la otra (contra quién, o desde dónde). `${metric.label.X}` lo rellena Cloud
 * Monitoring al enviar el aviso.
 *
 * @param {object} dimension
 * @returns {string}
 */
function enlaceRegistro(dimension) {
  const consulta = [
    `jsonPayload.event="${EVENTO_RECHAZO}"`,
    `jsonPayload.${dimension.campoRegistro}="\${metric.label.${dimension.etiqueta}}"`,
  ].join('\n');
  return `https://console.cloud.google.com/logs/query;query=${encodeURIComponent(consulta)
    .replace(/%24%7B/g, '${').replace(/%7D/g, '}')};duration=PT2H?project=${PROYECTO}`;
}

/**
 * Una política de alerta.
 *
 * @param {'origen'|'inversor'} clave
 * @param {string} canal - Nombre completo del canal (`projects/…/notificationChannels/…`).
 * @returns {object} Cuerpo de `projects.alertPolicies.create` (Monitoring API v3).
 */
function definicionPolitica(clave, canal) {
  const dimension = DIMENSIONES[clave];
  if (!dimension) throw new Error(`Dimensión desconocida: ${clave}`);
  if (!canal) throw new Error('Falta el canal de notificación de operación');

  return {
    displayName: dimension.titulo,
    combiner: 'OR',
    enabled: true,
    userLabels: { hu: ETIQUETA_HU, aviso: clave },
    conditions: [
      {
        displayName: dimension.condicion,
        conditionThreshold: {
          filter: `metric.type="${TIPO_METRICA}" AND resource.type="cloud_run_revision"`,
          aggregations: [
            {
              // Suma de los últimos 60 min, recalculada en cada evaluación: ventana móvil.
              alignmentPeriod: VENTANA,
              perSeriesAligner: 'ALIGN_SUM',
              crossSeriesReducer: 'REDUCE_SUM',
              groupByFields: [`metric.label.${dimension.etiqueta}`],
            },
          ],
          comparison: 'COMPARISON_GT',
          thresholdValue: UMBRAL_INTENTOS - 1,
          duration: '0s',
          trigger: { count: 1 },
        },
      },
    ],
    notificationChannels: [canal],
    alertStrategy: { autoClose: '86400s' },
    documentation: {
      mimeType: 'text/markdown',
      subject: `[Portastock] ${dimension.titulo}`,
      content: [
        `**${dimension.condicion}.** Alguien intenta leer cifras de inversores que no le corresponden (HU 7).`,
        '',
        `- **${dimension.queAgrupa}:** \`\${metric.label.${dimension.etiqueta}}\``,
        '- **Intentos en la última hora:** el valor observado de este aviso',
        `- **Ver ${dimension.queVer} y cuándo:** [registro de rechazos](${enlaceRegistro(dimension)})`,
        '',
        'Las peticiones ya se rechazaron sin devolver datos ni leer la base. Este aviso es para decidir si hay que bloquear el origen o avisar al inversor.',
        'Guía: platform-docs/stories/7-solo-consulta-sus-propias-cifras/verificacion-produccion.md',
      ].join('\n'),
    },
  };
}

/**
 * Busca el canal de operación entre los del proyecto.
 *
 * @param {Array<{name: string, type: string, labels: object}>} canales
 * @param {string} [correo]
 * @returns {string|null} Nombre completo del canal, o `null`.
 */
function elegirCanal(canales, correo = CORREO_OPERACION) {
  const canal = (canales || []).find(
    (c) => c.type === 'email' && c.enabled !== false && c.labels && c.labels.email_address === correo,
  );
  return canal ? canal.name : null;
}

/**
 * Encuentra la política ya creada para una dimensión (para actualizarla en
 * lugar de duplicarla).
 *
 * @param {Array<object>} politicas
 * @param {'origen'|'inversor'} clave
 * @returns {object|null}
 */
function politicaExistente(politicas, clave) {
  return (politicas || []).find(
    (p) => p.userLabels && p.userLabels.hu === ETIQUETA_HU && p.userLabels.aviso === clave,
  ) || null;
}

module.exports = {
  PROYECTO,
  NOMBRE_METRICA,
  TIPO_METRICA,
  UMBRAL_INTENTOS,
  VENTANA,
  CORREO_OPERACION,
  ETIQUETA_HU,
  DIMENSIONES,
  definicionMetrica,
  definicionPolitica,
  enlaceRegistro,
  elegirCanal,
  politicaExistente,
};
