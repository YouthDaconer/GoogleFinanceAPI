/**
 * HU 6.7 — Los correos del aviso de consumo: políticas, canales y panel.
 *
 * **El mecanismo es el de las HU 6.3 y 6.8.2**: el vigilante
 * (`services/consumption/consumptionWatchScheduled.js`) escribe una línea JSON
 * con `event: 'consumption_alert'` y una política de Cloud Monitoring basada en
 * logs la convierte en correo. Hay **una política por tipo de aviso**: el límite
 * de notificaciones de Monitoring (mínimo 5 min) es por política, y con una sola
 * un aviso de lecturas silenciaría al de escrituras de la misma ejecución
 * (QA-04: un consumo no silencia el aviso de otro).
 *
 * Cada política extrae de la línea el valor, el umbral, la hora de inicio y los
 * usuarios (`labelExtractors`) y los pinta en el correo, junto con el enlace al
 * panel de consumo y las causas conocidas (Escenario 6).
 *
 * **Destinatarios (D1, RN-6.7-M)**: quien tenga `roles/billing.admin` en la
 * cuenta de facturación —el mismo destinatario del aviso de presupuesto, que no
 * pasa por Monitoring— y el buzón de operación. Las direcciones de facturación
 * se leen en vivo al aplicar: ninguna dirección personal entra al repositorio.
 *
 * Este módulo **no toca la red**: construye los cuerpos que se envían a las
 * APIs de Monitoring. El borde es `scripts/aplicarAvisosConsumo.js`.
 *
 * @module scripts/consumoAlerts
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T4)
 */

const { PROYECTO, CORREO_OPERACION, elegirCanal } = require('./investorAccessAlerts');
const {
  CONSUMO,
  EVENTO_AVISO,
  UMBRALES,
  BYTES_POR_GIB,
  COLECCION_AVISOS,
} = require('../services/consumption/consumptionAlertService');

/** Etiqueta con la que se reconocen las políticas, canales y panel de esta HU. */
const ETIQUETA_HU = 'hu-6-7';

/** Servicio de Cloud Run del vigilante (el nombre de la función en minúsculas). */
const SERVICIO_VIGILANTE = 'scheduledconsumptionwatch';

/** Registro donde escribe el aviso de prueba (`scripts/lanzarAvisoConsumoPrueba.js`). */
const LOG_PRUEBA = 'consumo-aviso-prueba';

/** El mínimo de Monitoring: dos avisos de prueba a 5 min llegan como dos correos (QA-10). */
const LIMITE_NOTIFICACION = '300s';

/** El incidente se cierra solo a los 30 min (el mínimo), para no tapar el aviso siguiente. */
const CIERRE_AUTOMATICO = '1800s';

/** Nombre visible del panel de consumo (D6). */
const TITULO_PANEL = 'Consumo de la plataforma (HU 6.7)';

/** Las etiquetas que la política saca de la línea del aviso. */
const ETIQUETAS = Object.freeze(['valor', 'umbral', 'desde', 'usuarios']);

/** Las cuatro causas conocidas de un pico de consumo (Escenario 6, QA-11). */
const CAUSAS_CONOCIDAS = Object.freeze([
  '**Consultas abiertas:** pantallas o consultas que leen colecciones enteras sin límite. En el panel, «Lecturas por hora» ubica la hora del pico.',
  '**Corrección nocturna:** `reconcileStalePerformance` (02:00 ET) y el cierre nocturno. Ver los registros `[reconcile]` y la colección `reconcileNights`.',
  '**Recálculos manuales:** scripts de recálculo o de relleno lanzados a mano (`functions/scripts/`) y los recálculos de un portafolio entero.',
  '**Análisis de IA:** análisis del Copilot por encima del cupo. La lista completa del día está en `consumptionAlerts/{día}` de Firestore.',
]);

/** Las políticas, por clave. `clave` es el `consumo` de la línea que escuchan. */
const AVISOS = Object.freeze({
  [CONSUMO.LECTURAS]: Object.freeze({
    titulo: 'Consumo — lecturas de la base de datos por encima de 200.000 al día',
    consumo: 'Lecturas de la base de datos (Firestore)',
  }),
  [CONSUMO.ESCRITURAS]: Object.freeze({
    titulo: 'Consumo — escrituras de la base de datos por encima de 100.000 al día',
    consumo: 'Escrituras de la base de datos (Firestore)',
  }),
  [CONSUMO.ALMACENAMIENTO]: Object.freeze({
    titulo: 'Consumo — almacenamiento de la base de datos por encima de 0,8 GiB',
    consumo: 'Almacenamiento de la base de datos (Firestore)',
  }),
  [CONSUMO.ALMACENAMIENTO_NORMAL]: Object.freeze({
    titulo: 'Consumo — el almacenamiento volvió por debajo de 0,8 GiB',
    consumo: 'Almacenamiento de la base de datos (Firestore), de vuelta a la normalidad',
    desdeEs: 'Por debajo desde',
  }),
  [CONSUMO.IA]: Object.freeze({
    titulo: 'Consumo — análisis de IA por encima del cupo',
    consumo: 'Análisis de IA del Copilot',
    conUsuarios: true,
  }),
  [CONSUMO.PRUEBA]: Object.freeze({
    titulo: 'PRUEBA — aviso de consumo',
    consumo: 'PRUEBA: los cuatro consumos vigilados, medidos al lanzar la prueba',
    desdeEs: 'Lanzado',
    prueba: true,
  }),
});

/**
 * El filtro de la política: la línea de su tipo, y solo de quien puede
 * escribirla (el vigilante, o el registro del aviso de prueba).
 *
 * @param {string} clave
 * @returns {string}
 */
function filtroDe(clave) {
  const origen = clave === CONSUMO.PRUEBA
    ? `logName="projects/${PROYECTO}/logs/${LOG_PRUEBA}"`
    : `resource.type="cloud_run_revision" AND resource.labels.service_name="${SERVICIO_VIGILANTE}"`;
  return `${origen} AND jsonPayload.event="${EVENTO_AVISO}" AND jsonPayload.consumo="${clave}"`;
}

/**
 * Enlace al panel de consumo en la consola.
 *
 * @param {string} nombrePanel - `projects/{número}/dashboards/{id}`
 * @returns {string}
 */
function enlacePanel(nombrePanel) {
  const id = String(nombrePanel || '').split('/').pop();
  if (!id) throw new Error('Falta el panel de consumo');
  return `https://console.cloud.google.com/monitoring/dashboards/builder/${id}?project=${PROYECTO}`;
}

/**
 * El cuerpo del correo (Escenario 6): el consumo, cuánto va, el umbral, desde
 * qué hora, el enlace al panel y las causas conocidas. `${log.extracted_label.X}`
 * lo rellena Cloud Monitoring con lo que extrae de la línea.
 *
 * @param {string} clave
 * @param {string} enlace
 * @returns {string}
 */
function contenidoDe(clave, enlace) {
  const aviso = AVISOS[clave];
  const lineas = [
    aviso.prueba
      ? '**PRUEBA — esto es un aviso de prueba del aviso de consumo (HU 6.7).** No hay ningún incidente: comprueba que el correo llega y muestra los consumos de este momento.'
      : `**${aviso.titulo.replace('Consumo — ', '')}** (HU 6.7, RN-6.7-J).`,
    '',
    `- **Consumo:** ${aviso.consumo}`,
    `- **Cuánto va:** \${log.extracted_label.valor}`,
    `- **Umbral:** \${log.extracted_label.umbral}`,
    `- **${aviso.desdeEs || 'Desde'}:** \${log.extracted_label.desde}`,
  ];
  if (aviso.conUsuarios) lineas.push('- **Usuarios (identificador y análisis de los últimos 7 días):** ${log.extracted_label.usuarios}');
  lineas.push(
    '',
    `**Panel de consumo:** ${enlace}`,
    '',
    '**Causas conocidas, por dónde empezar:**',
    ...CAUSAS_CONOCIDAS.map((c) => `- ${c}`),
    '',
    `Detalle del día: \`${COLECCION_AVISOS}/{día}\` en Firestore (día en hora de Colombia).`,
    'Este aviso mira el uso; el aviso de presupuesto sigue mirando el dinero (RN-6.7-N).',
    'Guía: platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md',
  );
  return lineas.join('\n');
}

/**
 * Una política basada en logs.
 *
 * @param {string} clave - Una de `AVISOS`
 * @param {string[]} canales - Nombres completos de los canales
 * @param {string} enlace - Enlace al panel de consumo
 * @returns {object} Cuerpo de `projects.alertPolicies.create` (Monitoring API v3)
 */
function definicionPolitica(clave, canales, enlace) {
  const aviso = AVISOS[clave];
  if (!aviso) throw new Error(`Aviso desconocido: ${clave}`);
  if (!canales || canales.length === 0) throw new Error('Faltan los canales de notificación');
  if (!enlace) throw new Error('Falta el enlace al panel de consumo');

  return {
    displayName: aviso.titulo,
    combiner: 'OR',
    enabled: true,
    userLabels: { hu: ETIQUETA_HU, aviso: clave },
    conditions: [
      {
        displayName: `Línea ${EVENTO_AVISO} con consumo=${clave}`,
        conditionMatchedLog: {
          filter: filtroDe(clave),
          labelExtractors: Object.fromEntries(ETIQUETAS.map((e) => [e, `EXTRACT(jsonPayload.${e})`])),
        },
      },
    ],
    notificationChannels: [...canales],
    alertStrategy: {
      notificationRateLimit: { period: LIMITE_NOTIFICACION },
      autoClose: CIERRE_AUTOMATICO,
    },
    documentation: {
      mimeType: 'text/markdown',
      subject: `[Portastock] ${aviso.titulo}`,
      content: contenidoDe(clave, enlace),
    },
  };
}

/**
 * Encuentra la política ya creada para un aviso (para actualizarla en lugar de
 * duplicarla).
 *
 * @param {Array<object>} politicas
 * @param {string} clave
 * @returns {object|null}
 */
function politicaExistente(politicas, clave) {
  return (politicas || []).find(
    (p) => p.userLabels && p.userLabels.hu === ETIQUETA_HU && p.userLabels.aviso === clave,
  ) || null;
}

/**
 * Las direcciones de quien administra la facturación, de la política IAM de la
 * cuenta de facturación (RN-6.7-M). Personas y grupos; nunca cuentas de servicio.
 *
 * @param {{bindings?: Array<{role: string, members: string[]}>}} politicaIam
 * @returns {string[]}
 */
function correosFacturacion(politicaIam) {
  const correos = new Set();
  for (const binding of (politicaIam && politicaIam.bindings) || []) {
    if (binding.role !== 'roles/billing.admin') continue;
    for (const miembro of binding.members || []) {
      const [tipo, correo] = miembro.split(':');
      if ((tipo === 'user' || tipo === 'group') && correo) correos.add(correo.toLowerCase());
    }
  }
  return [...correos];
}

/**
 * Un canal de correo hacia quien administra la facturación.
 *
 * @param {string} correo
 * @returns {object} Cuerpo de `projects.notificationChannels.create`
 */
function definicionCanalFacturacion(correo) {
  return {
    type: 'email',
    displayName: 'Facturación — avisos de consumo (HU 6.7)',
    description: 'Quien tiene roles/billing.admin en la cuenta de facturación: el mismo destinatario del aviso de presupuesto (RN-6.7-M).',
    labels: { email_address: correo },
    userLabels: { hu: ETIQUETA_HU, destino: 'facturacion' },
    enabled: true,
  };
}

/** Oculta una dirección para mostrarla en consola. */
function enmascarar(correo) {
  const [usuario, dominio] = String(correo).split('@');
  return `${usuario.slice(0, 3)}***@${dominio}`;
}

/** Un gráfico del panel. */
function grafico({ titulo, metrica, periodo, alineador, tipo, umbral, etiquetaUmbral }) {
  return {
    title: titulo,
    xyChart: {
      dataSets: [
        {
          timeSeriesQuery: {
            timeSeriesFilter: {
              filter: `metric.type="${metrica}"`,
              aggregation: { alignmentPeriod: periodo, perSeriesAligner: alineador, crossSeriesReducer: 'REDUCE_SUM' },
            },
          },
          plotType: tipo,
          targetAxis: 'Y1',
        },
      ],
      thresholds: umbral !== undefined ? [{ value: umbral, label: etiquetaUmbral, targetAxis: 'Y1' }] : [],
      yAxis: { scale: 'LINEAR' },
    },
  };
}

/**
 * El panel «Consumo de la plataforma» (D6): lecturas y escrituras por día y por
 * hora, y el almacenamiento, con la línea de cada umbral. La IA no es una
 * métrica: su detalle va en el correo y en `consumptionAlerts/{día}`.
 *
 * @returns {object} Cuerpo de `projects.dashboards.create` (Monitoring API v1)
 */
function definicionPanel() {
  const lecturas = 'firestore.googleapis.com/document/read_count';
  const escrituras = 'firestore.googleapis.com/document/write_count';
  const almacenamiento = 'firestore.googleapis.com/storage/data_and_index_storage_bytes';
  const tile = (xPos, yPos, widget, height = 4) => ({ xPos, yPos, width: 6, height, widget });

  return {
    displayName: TITULO_PANEL,
    labels: { [ETIQUETA_HU]: '' },
    mosaicLayout: {
      columns: 12,
      tiles: [
        tile(0, 0, grafico({ titulo: 'Lecturas por día (día UTC)', metrica: lecturas, periodo: '86400s', alineador: 'ALIGN_SUM', tipo: 'STACKED_BAR', umbral: UMBRALES.lecturas, etiquetaUmbral: 'Umbral: 200.000 al día' })),
        tile(6, 0, grafico({ titulo: 'Escrituras por día (día UTC)', metrica: escrituras, periodo: '86400s', alineador: 'ALIGN_SUM', tipo: 'STACKED_BAR', umbral: UMBRALES.escrituras, etiquetaUmbral: 'Umbral: 100.000 al día' })),
        tile(0, 4, grafico({ titulo: 'Lecturas por hora (para ubicar el pico)', metrica: lecturas, periodo: '3600s', alineador: 'ALIGN_SUM', tipo: 'STACKED_BAR' })),
        tile(6, 4, grafico({ titulo: 'Escrituras por hora (para ubicar el pico)', metrica: escrituras, periodo: '3600s', alineador: 'ALIGN_SUM', tipo: 'STACKED_BAR' })),
        tile(0, 8, grafico({ titulo: 'Almacenamiento (bytes)', metrica: almacenamiento, periodo: '300s', alineador: 'ALIGN_MEAN', tipo: 'LINE', umbral: UMBRALES.almacenamientoGib * BYTES_POR_GIB, etiquetaUmbral: 'Umbral: 0,8 GiB' })),
        tile(6, 8, {
          title: 'Causas conocidas de un pico de consumo',
          text: { format: 'MARKDOWN', content: CAUSAS_CONOCIDAS.map((c) => `- ${c}`).join('\n') },
        }),
      ],
    },
  };
}

/**
 * Encuentra el panel ya creado (para actualizarlo en lugar de duplicarlo).
 *
 * @param {Array<object>} paneles
 * @returns {object|null}
 */
function panelExistente(paneles) {
  return (paneles || []).find((d) => d.labels && Object.prototype.hasOwnProperty.call(d.labels, ETIQUETA_HU)) || null;
}

module.exports = {
  PROYECTO,
  CORREO_OPERACION,
  ETIQUETA_HU,
  SERVICIO_VIGILANTE,
  LOG_PRUEBA,
  TITULO_PANEL,
  CAUSAS_CONOCIDAS,
  AVISOS,
  filtroDe,
  enlacePanel,
  definicionPolitica,
  politicaExistente,
  correosFacturacion,
  definicionCanalFacturacion,
  definicionPanel,
  panelExistente,
  elegirCanal,
  enmascarar,
};
