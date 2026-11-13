/**
 * HU 6.8.2 — Los avisos a operación de la corrección nocturna y de la HU 6.3
 * llegan por el canal de operación (Escenario 4, QA-11 en lo automatizable).
 *
 * Lo que un test puede asegurar: que cada política escucha exactamente la
 * línea que el código escribe, que el canal se elige por el buzón y que cada
 * política se reconoce al volver a aplicarla. Que el correo llegue se prueba
 * en producción.
 *
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T9, T15)
 */

const fs = require('fs');
const path = require('path');
const {
  AVISOS,
  PREFIJO_CORRECCION,
  PREFIJO_EOD,
  SERVICIO_CORRECCION,
  CORREO_OPERACION,
  definicionPolitica,
  politicaExistente,
  elegirCanal,
} = require('../correccionNocturnaAlerts');
const { PREFIJO_ALERTA } = require('../../services/reconcileNightReportService');

const CANAL = 'projects/portafolio-inversiones/notificationChannels/17171909552384294197';

describe('correccionNocturnaAlerts — las políticas escuchan lo que el código escribe', () => {
  test('el aviso de atraso filtra el mismo prefijo que emite reconcileNightReportService, en la función de la corrección', () => {
    const filtro = definicionPolitica('atraso', CANAL).conditions[0].conditionMatchedLog.filter;

    expect(PREFIJO_CORRECCION).toBe(PREFIJO_ALERTA);
    expect(filtro).toContain(`textPayload:"${PREFIJO_ALERTA}"`);
    expect(filtro).toContain('severity>=ERROR');
    expect(filtro).toContain(`resource.labels.service_name="${SERVICIO_CORRECCION}"`);
  });

  test('el aviso del cierre (HU 6.3) filtra el prefijo que escribe eodFinalizationScheduled', () => {
    const fuente = fs.readFileSync(path.join(__dirname, '../../services/eodFinalizationScheduled.js'), 'utf8');
    const filtro = definicionPolitica('eod-incompleto', CANAL).conditions[0].conditionMatchedLog.filter;

    expect(fuente).toContain(`const PREFIJO_ALERTA = '${PREFIJO_EOD}';`);
    expect(filtro).toContain(`textPayload:"${PREFIJO_EOD}"`);
  });

  test('la corrección cortada vigila el log real de falta de memoria (07, 11 y 12-nov-2026) y la petición fallida', () => {
    const filtro = definicionPolitica('cortada', CANAL).conditions[0].conditionMatchedLog.filter;
    expect(filtro).toBe('resource.type="cloud_run_revision" AND resource.labels.service_name="reconcilestaleperformance" ' +
      'AND (textPayload:"Memory limit" OR httpRequest.status>=500)');
  });

  test.each(Object.keys(AVISOS))('«%s»: política basada en logs, con el canal, un aviso por hora y reconocible por etiquetas', (clave) => {
    const politica = definicionPolitica(clave, CANAL);

    expect(politica.notificationChannels).toEqual([CANAL]);
    expect(politica.enabled).toBe(true);
    expect(politica.alertStrategy.notificationRateLimit.period).toBe('3600s');
    expect(politica.userLabels).toEqual({ hu: AVISOS[clave].hu, aviso: clave });
    // Las etiquetas de Monitoring solo admiten minúsculas, dígitos, - y _
    Object.values(politica.userLabels).forEach((v) => expect(v).toMatch(/^[a-z0-9_-]+$/));
    expect(politica.conditions[0].conditionMatchedLog.filter).toMatch(/^resource\.type="cloud_run_revision"/);
  });

  test('sin canal o con un aviso desconocido, no construye nada', () => {
    expect(() => definicionPolitica('atraso', null)).toThrow('Falta el canal de notificación de operación');
    expect(() => definicionPolitica('otro', CANAL)).toThrow('Aviso desconocido: otro');
  });
});

describe('correccionNocturnaAlerts — canal y políticas existentes', () => {
  test('el canal se elige por el buzón de operación, no por el nombre', () => {
    const canales = [
      { name: 'projects/p/notificationChannels/1', type: 'email', labels: { email_address: 'portastockcol@gmail.com' }, displayName: 'Portastock' },
      { name: CANAL, type: 'email', enabled: true, labels: { email_address: CORREO_OPERACION } },
    ];
    expect(elegirCanal(canales)).toBe(CANAL);
    expect(elegirCanal(canales.slice(0, 1))).toBeNull();
  });

  test('reconoce la política ya creada de cada aviso para actualizarla en lugar de duplicarla', () => {
    const politicas = [
      { name: 'projects/p/alertPolicies/1', userLabels: { hu: 'hu-7', aviso: 'origen' } },
      { name: 'projects/p/alertPolicies/2', userLabels: { hu: 'hu-6-8-2', aviso: 'atraso' } },
      { name: 'projects/p/alertPolicies/3', userLabels: { hu: 'hu-6-3', aviso: 'eod-incompleto' } },
    ];
    expect(politicaExistente(politicas, 'atraso').name).toBe('projects/p/alertPolicies/2');
    expect(politicaExistente(politicas, 'eod-incompleto').name).toBe('projects/p/alertPolicies/3');
    expect(politicaExistente(politicas, 'cortada')).toBeNull();
  });
});
