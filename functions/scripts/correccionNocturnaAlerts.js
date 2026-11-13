/**
 * HU 6.8.2 — Los avisos a operación de la corrección nocturna, y los del
 * cierre nocturno de la HU 6.3 con el canal activo (Escenario 4, QA-11).
 *
 * **El mecanismo es el de la HU 6.3**: el código escribe una línea de error
 * con un prefijo fijo y una política de Cloud Monitoring basada en logs la
 * convierte en un correo al canal de operación. Las políticas de la 6.3 estaban
 * descritas en `alert-policies.yaml` pero nunca llegaron a producción
 * (comprobado el 2026-11-13: ninguna de las 5 políticas existentes es suya),
 * así que su aviso no le llegaba a nadie. Activarlo es parte de esta HU.
 *
 * Las políticas:
 * - `atraso`: la corrección terminó con inversores atrasados o alguno pasó a
 *   revisión (`[CORRECCION-ATRASO]`, `services/reconcileNightReportService.js`).
 * - `cortada`: la plataforma cortó la corrección (sin memoria o con la
 *   petición fallida). Es la red del único caso que el código no puede
 *   avisar: cuando el proceso entero muere.
 * - `eod-incompleto` (HU 6.3, Escenario 4): el cierre nocturno dejó
 *   inversores sin su rendimiento del día (`[EOD-INCOMPLETO]`).
 * - `eod-cierre-fallido` (HU 6.3, T6): el cierre póstumo no terminó; sin él,
 *   nadie comprueba si alguien quedó fuera.
 *
 * La política 7 de `alert-policies.yaml` (tasa de errores de las tareas del
 * cierre) no es un aviso de "quién quedó fuera" sino un diagnóstico, y
 * necesita una métrica basada en logs: queda fuera.
 *
 * Este módulo **no toca la red**: construye los cuerpos que se envían a la API
 * de Monitoring. El borde es `scripts/aplicarAvisosCorreccionNocturna.js`.
 *
 * @module scripts/correccionNocturnaAlerts
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T9)
 */

const { PROYECTO, CORREO_OPERACION, elegirCanal } = require('./investorAccessAlerts');
const { PREFIJO_ALERTA: PREFIJO_CORRECCION, COLECCION_NOCHES } = require('../services/reconcileNightReportService');

/**
 * Prefijo del aviso del cierre nocturno. Es el contrato con
 * `services/eodFinalizationScheduled.js#PREFIJO_ALERTA` (lo vigila un test).
 */
const PREFIJO_EOD = '[EOD-INCOMPLETO]';

/** Nombres de servicio de Cloud Run de las funciones vigiladas (en minúsculas). */
const SERVICIO_CORRECCION = 'reconcilestaleperformance';
const SERVICIO_CIERRE_POSTUMO = 'scheduledeodfinalization';

/** Un aviso por hora como mucho, aunque el log se repita. */
const LIMITE_NOTIFICACION = '3600s';

/** El incidente se cierra solo al día siguiente si no se repite. */
const CIERRE_AUTOMATICO = '86400s';

/** Proceso cortado por la plataforma: falta de memoria o petición fallida. */
const filtroProcesoCortado = (servicio) => [
  'resource.type="cloud_run_revision"',
  `resource.labels.service_name="${servicio}"`,
  '(textPayload:"Memory limit" OR httpRequest.status>=500)',
].join(' AND ');

const GUIA = 'Guía: platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md';

/** Las políticas, por clave. `hu` y `clave` las reconocen al volver a aplicarlas. */
const AVISOS = Object.freeze({
  'atraso': Object.freeze({
    hu: 'hu-6-8-2',
    titulo: 'Corrección nocturna — inversores atrasados o en revisión',
    condicion: `Log con ${PREFIJO_CORRECCION}`,
    filtro: [
      'resource.type="cloud_run_revision"',
      `resource.labels.service_name="${SERVICIO_CORRECCION}"`,
      'severity>=ERROR',
      `textPayload:"${PREFIJO_CORRECCION}"`,
    ].join(' AND '),
    contenido: [
      '**La corrección nocturna del historial terminó con inversores atrasados, o alguno pasó a revisión** (HU 6.8.2, RN-6.8.2-K).',
      '',
      `- **Detalle:** el documento \`${COLECCION_NOCHES}/{noche}_{hora}\` de Firestore: cuántos y cuáles están atrasados, desde cuándo esperan, y los que pasaron a revisión con el motivo de cada intento.`,
      '- **La corrección no se amplió** (RN-6.8.2-L): sigue siendo una ejecución por noche. Si el atraso se repite, decidir si dar más capacidad.',
      '- **Liberar a un inversor en revisión:** `node scripts/liberarRevisionCorreccion.js <uid> --aplicar` (en `functions/`).',
      '',
      GUIA,
    ].join('\n'),
  }),
  'cortada': Object.freeze({
    hu: 'hu-6-8-2',
    titulo: 'Corrección nocturna — la plataforma cortó la ejecución',
    condicion: 'Sin memoria o petición fallida en reconcileStalePerformance',
    filtro: filtroProcesoCortado(SERVICIO_CORRECCION),
    contenido: [
      '**La corrección nocturna del historial se cortó antes de terminar** (sin memoria o con la petición fallida).',
      '',
      '- El intento del inversor en curso ya quedó contado (HU 6.8.2): al tercero pasa a revisión.',
      '- Esa noche puede no haber dejado su estado: revisar los registros `[reconcile]` de la ejecución.',
      '',
      GUIA,
    ].join('\n'),
  }),
  'eod-incompleto': Object.freeze({
    hu: 'hu-6-3',
    titulo: 'EOD — Inversores sin rendimiento del día',
    condicion: `Log con ${PREFIJO_EOD}`,
    filtro: [
      'resource.type="cloud_run_revision"',
      'severity>=ERROR',
      `textPayload:"${PREFIJO_EOD}"`,
    ].join(' AND '),
    contenido: [
      '**El cierre nocturno terminó con inversores sin su rendimiento del día** (HU 6.3, RN-6.3-N).',
      '',
      '- **Detalle:** `eodFailures/{YYYY-MM-DD}` en Firestore (cuántos, cuáles y por qué).',
      '- La corrección de las 02:00 ET reintenta a los marcados `_stale`.',
    ].join('\n'),
  }),
  'eod-cierre-fallido': Object.freeze({
    hu: 'hu-6-3',
    titulo: 'Scheduled Job — scheduledEodFinalization Failed',
    condicion: 'Sin memoria o petición fallida en scheduledEodFinalization',
    filtro: filtroProcesoCortado(SERVICIO_CIERRE_POSTUMO),
    contenido: '**El cierre póstumo del EOD ha fallado.** Sin él, nadie comprueba si algún inversor quedó fuera (HU 6.3).',
  }),
});

/**
 * Una política basada en logs.
 *
 * @param {string} clave - Una de `AVISOS`
 * @param {string} canal - Nombre completo del canal (`projects/…/notificationChannels/…`)
 * @returns {object} Cuerpo de `projects.alertPolicies.create` (Monitoring API v3)
 */
function definicionPolitica(clave, canal) {
  const aviso = AVISOS[clave];
  if (!aviso) throw new Error(`Aviso desconocido: ${clave}`);
  if (!canal) throw new Error('Falta el canal de notificación de operación');

  return {
    displayName: aviso.titulo,
    combiner: 'OR',
    enabled: true,
    userLabels: { hu: aviso.hu, aviso: clave },
    conditions: [
      {
        displayName: aviso.condicion,
        conditionMatchedLog: { filter: aviso.filtro },
      },
    ],
    notificationChannels: [canal],
    alertStrategy: {
      notificationRateLimit: { period: LIMITE_NOTIFICACION },
      autoClose: CIERRE_AUTOMATICO,
    },
    documentation: {
      mimeType: 'text/markdown',
      subject: `[Portastock] ${aviso.titulo}`,
      content: aviso.contenido,
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
  const aviso = AVISOS[clave];
  return (politicas || []).find(
    (p) => p.userLabels && p.userLabels.hu === aviso.hu && p.userLabels.aviso === clave,
  ) || null;
}

module.exports = {
  PROYECTO,
  CORREO_OPERACION,
  PREFIJO_CORRECCION,
  PREFIJO_EOD,
  SERVICIO_CORRECCION,
  SERVICIO_CIERRE_POSTUMO,
  AVISOS,
  definicionPolitica,
  politicaExistente,
  elegirCanal,
};
