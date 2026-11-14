/**
 * HU 6.7 — Los correos del aviso de consumo (Escenarios 5 y 6, QA-09 a QA-12
 * en lo automatizable).
 *
 * Lo que un test puede asegurar: que cada política escucha exactamente la línea
 * que el vigilante escribe, que la de prueba va aparte, que el correo lleva el
 * valor, el umbral, la hora, el enlace al panel y las cuatro causas, que los
 * destinatarios son facturación y operación, que todo se reconoce al reaplicar
 * y que el borde nunca toca el presupuesto. Que el correo llegue se prueba en
 * producción con el aviso de prueba.
 *
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T4, T5, T12)
 */

const fs = require('fs');
const path = require('path');
const {
  AVISOS,
  CAUSAS_CONOCIDAS,
  SERVICIO_VIGILANTE,
  LOG_PRUEBA,
  CORREO_OPERACION,
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
} = require('../consumoAlerts');
const { CONSUMO, EVENTO_AVISO, lineaAviso } = require('../../services/consumption/consumptionAlertService');
const { FUNCTION_CONFIG } = require('../../services/consumption/consumptionWatchScheduled');

jest.mock('../../services/firebaseAdmin', () => ({ firestore: jest.fn(() => ({})), credential: {} }));
jest.mock('firebase-functions/v2/scheduler', () => ({ onSchedule: jest.fn((opts, handler) => handler) }));

const CANAL_OPERACION = 'projects/portafolio-inversiones/notificationChannels/17171909552384294197';
const CANAL_FACTURACION = 'projects/portafolio-inversiones/notificationChannels/999';
const ENLACE = 'https://console.cloud.google.com/monitoring/dashboards/builder/abc-123?project=portafolio-inversiones';

/** Comprueba un filtro de log contra una entrada, para lo que usan estas políticas (AND de igualdades). */
const casa = (filtro, entrada) => filtro.split(' AND ').every((condicion) => {
  const [campo, valor] = condicion.split('=');
  const real = campo.split('.').reduce((o, k) => (o ? o[k] : undefined), entrada);
  return JSON.stringify(String(real)) === valor;
});

const entradaDelVigilante = (consumo) => ({
  resource: { type: 'cloud_run_revision', labels: { service_name: SERVICIO_VIGILANTE } },
  jsonPayload: lineaAviso({ consumo, valor: 1, desde: '2026-11-19T19:20:00.000Z', usuarios: consumo === 'ia' ? [] : undefined }),
});

describe('consumoAlerts — cada política escucha la línea que escribe el vigilante', () => {
  test('hay una política por tipo de aviso, y son exactamente los tipos del código', () => {
    expect(Object.keys(AVISOS).sort()).toEqual(Object.values(CONSUMO).sort());
  });

  test.each(Object.values(CONSUMO).filter((c) => c !== CONSUMO.PRUEBA))('«%s»: casa con su línea y con ninguna otra', (consumo) => {
    const filtro = definicionPolitica(consumo, [CANAL_OPERACION], ENLACE).conditions[0].conditionMatchedLog.filter;

    expect(casa(filtro, entradaDelVigilante(consumo))).toBe(true);
    Object.values(CONSUMO).filter((otro) => otro !== consumo)
      .forEach((otro) => expect(casa(filtro, entradaDelVigilante(otro))).toBe(false));
  });

  test('el servicio vigilado es la función del vigilante, en minúsculas como la nombra Cloud Run', () => {
    const fuente = fs.readFileSync(path.join(__dirname, '../../index.js'), 'utf8');
    expect(fuente).toContain('exports.scheduledConsumptionWatch = scheduledConsumptionWatch;');
    expect(SERVICIO_VIGILANTE).toBe('scheduledConsumptionWatch'.toLowerCase());
    expect(FUNCTION_CONFIG.schedule).toBe('*/15 * * * *');
  });

  test('QA-09/QA-10: la política de prueba escucha solo el registro del aviso de prueba, no la función', () => {
    const filtro = filtroDe(CONSUMO.PRUEBA);
    expect(filtro).toBe(`logName="projects/portafolio-inversiones/logs/${LOG_PRUEBA}" AND jsonPayload.event="${EVENTO_AVISO}" AND jsonPayload.consumo="prueba"`);
    expect(casa(filtro, entradaDelVigilante(CONSUMO.PRUEBA))).toBe(false);
  });
});

describe('consumoAlerts — el correo (Escenario 6, QA-11)', () => {
  test.each(Object.keys(AVISOS))('«%s»: valor, umbral, hora, enlace al panel y las cuatro causas conocidas', (clave) => {
    const politica = definicionPolitica(clave, [CANAL_OPERACION, CANAL_FACTURACION], ENLACE);
    const contenido = politica.documentation.content;

    expect(contenido).toContain('${log.extracted_label.valor}');
    expect(contenido).toContain('${log.extracted_label.umbral}');
    expect(contenido).toContain('${log.extracted_label.desde}');
    expect(contenido).toContain(`**Panel de consumo:** ${ENLACE}`);
    ['Consultas abiertas', 'Corrección nocturna', 'Recálculos manuales', 'Análisis de IA']
      .forEach((causa) => expect(contenido).toContain(`**${causa}:**`));
    expect(CAUSAS_CONOCIDAS).toHaveLength(4);
    // Cada etiqueta del correo se extrae de un campo que la línea sí trae
    const linea = lineaAviso({ consumo: clave, valor: 1, desde: '2026-11-19T19:20:00.000Z', usuarios: [] });
    Object.entries(politica.conditions[0].conditionMatchedLog.labelExtractors).forEach(([etiqueta, extractor]) => {
      expect(extractor).toBe(`EXTRACT(jsonPayload.${etiqueta})`);
      expect(linea).toHaveProperty(etiqueta);
    });
  });

  test('RN-6.7-O: solo el correo de IA lista usuarios', () => {
    expect(definicionPolitica('ia', [CANAL_OPERACION], ENLACE).documentation.content).toContain('${log.extracted_label.usuarios}');
    expect(definicionPolitica('lecturas', [CANAL_OPERACION], ENLACE).documentation.content).not.toContain('usuarios}');
  });

  test('QA-09: el aviso de prueba va marcado como PRUEBA en el asunto y en el cuerpo', () => {
    const politica = definicionPolitica('prueba', [CANAL_OPERACION], ENLACE);
    expect(politica.documentation.subject).toBe('[Portastock] PRUEBA — aviso de consumo');
    expect(politica.documentation.content.startsWith('**PRUEBA')).toBe(true);
  });

  test('el enlace al panel sale del nombre del panel creado', () => {
    expect(enlacePanel('projects/439172760821/dashboards/abc-123')).toBe(ENLACE);
    expect(() => enlacePanel('')).toThrow('Falta el panel de consumo');
  });
});

describe('consumoAlerts — destinatarios, reaplicar y límites', () => {
  test.each(Object.keys(AVISOS))('«%s»: a los canales de facturación y operación, 5 min entre correos y reconocible por etiquetas', (clave) => {
    const politica = definicionPolitica(clave, [CANAL_OPERACION, CANAL_FACTURACION], ENLACE);

    expect(politica.notificationChannels).toEqual([CANAL_OPERACION, CANAL_FACTURACION]);
    expect(politica.enabled).toBe(true);
    expect(politica.alertStrategy.notificationRateLimit.period).toBe('300s');
    expect(politica.userLabels).toEqual({ hu: 'hu-6-7', aviso: clave });
    Object.values(politica.userLabels).forEach((v) => expect(v).toMatch(/^[a-z0-9_-]+$/));
  });

  test('sin canales, sin enlace o con un aviso desconocido, no construye nada', () => {
    expect(() => definicionPolitica('lecturas', [], ENLACE)).toThrow('Faltan los canales de notificación');
    expect(() => definicionPolitica('lecturas', [CANAL_OPERACION], '')).toThrow('Falta el enlace al panel de consumo');
    expect(() => definicionPolitica('otro', [CANAL_OPERACION], ENLACE)).toThrow('Aviso desconocido: otro');
  });

  test('RN-6.7-M: el destinatario de facturación son las personas y grupos con roles/billing.admin, nunca cuentas de servicio', () => {
    const iam = {
      bindings: [
        { role: 'roles/billing.admin', members: ['user:Admin.Facturacion@ejemplo.com', 'serviceAccount:bot@p.iam.gserviceaccount.com', 'group:finanzas@ejemplo.com'] },
        { role: 'roles/billing.viewer', members: ['user:lector@ejemplo.com'] },
      ],
    };
    expect(correosFacturacion(iam)).toEqual(['admin.facturacion@ejemplo.com', 'finanzas@ejemplo.com']);
    expect(correosFacturacion({})).toEqual([]);
  });

  test('el canal de facturación es de correo y se reconoce por su buzón al reaplicar', () => {
    const canal = definicionCanalFacturacion('admin.facturacion@ejemplo.com');
    expect(canal).toMatchObject({ type: 'email', labels: { email_address: 'admin.facturacion@ejemplo.com' }, userLabels: { hu: 'hu-6-7', destino: 'facturacion' } });
    const existentes = [{ name: CANAL_FACTURACION, ...canal }, { name: CANAL_OPERACION, type: 'email', labels: { email_address: CORREO_OPERACION } }];
    expect(elegirCanal(existentes, 'admin.facturacion@ejemplo.com')).toBe(CANAL_FACTURACION);
    expect(elegirCanal(existentes, CORREO_OPERACION)).toBe(CANAL_OPERACION);
  });

  test('reconoce la política y el panel ya creados para actualizarlos en lugar de duplicarlos', () => {
    const politicas = [
      { name: 'projects/p/alertPolicies/1', userLabels: { hu: 'hu-6-8-2', aviso: 'atraso' } },
      { name: 'projects/p/alertPolicies/2', userLabels: { hu: 'hu-6-7', aviso: 'lecturas' } },
    ];
    expect(politicaExistente(politicas, 'lecturas').name).toBe('projects/p/alertPolicies/2');
    expect(politicaExistente(politicas, 'escrituras')).toBeNull();

    const paneles = [{ name: 'projects/1/dashboards/x', labels: {} }, { name: 'projects/1/dashboards/y', labels: { 'hu-6-7': '' } }];
    expect(panelExistente(paneles).name).toBe('projects/1/dashboards/y');
    expect(panelExistente([])).toBeNull();
  });

  test('D6: el panel muestra lecturas, escrituras y almacenamiento con la línea de cada umbral, y las causas', () => {
    const panel = definicionPanel();
    const graficos = panel.mosaicLayout.tiles.map((t) => t.widget).filter((w) => w.xyChart);
    const umbrales = graficos.flatMap((w) => w.xyChart.thresholds.map((u) => u.value));

    expect(panel.labels).toEqual({ 'hu-6-7': '' });
    expect(umbrales).toEqual([200000, 100000, 0.8 * 1073741824]);
    expect(panel.mosaicLayout.tiles.some((t) => t.widget.text)).toBe(true);
  });

  test('las direcciones se muestran enmascaradas en la consola', () => {
    expect(enmascarar('admin.facturacion@ejemplo.com')).toBe('adm***@ejemplo.com');
  });

  test('QA-12 / RN-6.7-N: el borde solo lee la facturación y nunca llama a la API de presupuestos', () => {
    const borde = fs.readFileSync(path.join(__dirname, '../aplicarAvisosConsumo.js'), 'utf8');
    expect(borde).not.toMatch(/billingbudgets|budgets/i);
    const llamadasAFacturacion = borde.match(/llamar\(token, '(\w+)', `\$\{FACTURACION\}[^`]*`/g);
    expect(llamadasAFacturacion).toHaveLength(2);
    llamadasAFacturacion.forEach((llamada) => expect(llamada).toContain("'GET'"));
  });

  test('ninguna dirección personal está escrita en los scripts de esta HU', () => {
    ['consumoAlerts.js', 'aplicarAvisosConsumo.js', 'lanzarAvisoConsumoPrueba.js', 'simularAvisosConsumo.js'].forEach((archivo) => {
      const fuente = fs.readFileSync(path.join(__dirname, '..', archivo), 'utf8');
      expect(fuente).not.toMatch(/@gmail\.com/i);
    });
  });
});
