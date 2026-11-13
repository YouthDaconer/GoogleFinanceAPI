/**
 * HU 7 — La definición del aviso a operación por intentos repetidos
 * (Escenario 6, RN-7-D).
 *
 * Que el correo llegue se prueba en producción (no hay entorno de pruebas). Aquí
 * se fija lo que decide **cuándo** llega: qué cuenta la métrica, cómo se agrupa,
 * la ventana y el umbral.
 *
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T6)
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/qa.md (QA-11, QA-12)
 */

const {
  NOMBRE_METRICA,
  TIPO_METRICA,
  definicionMetrica,
  definicionPolitica,
  elegirCanal,
  politicaExistente,
} = require('../investorAccessAlerts');
const { EVENTO_RECHAZO } = require('../../utils/investorSession');

// === Fábricas locales ===

const CANAL = 'projects/portafolio-inversiones/notificationChannels/17171909552384294197';
const canal = (overrides = {}) => ({
  name: CANAL, displayName: 'Portastock', type: 'email', enabled: true, labels: { email_address: 'info@portastock.net' }, ...overrides,
});

/**
 * Lo que evalúa Cloud Monitoring con la política: la suma por grupo en la ventana
 * móvil, contra el umbral. Reproduce `ALIGN_SUM` + `REDUCE_SUM` + `groupByFields`
 * + `COMPARISON_GT` para comprobar los ejemplos de qa.md.
 */
function gruposQueAvisan(politica, intentos, finVentana) {
  const condicion = politica.conditions[0].conditionThreshold;
  const [agregacion] = condicion.aggregations;
  const ventanaMs = parseInt(agregacion.alignmentPeriod, 10) * 1000;
  const etiqueta = agregacion.groupByFields[0].replace('metric.label.', '');
  const conteo = {};
  for (const i of intentos) {
    if (i.at > finVentana - ventanaMs && i.at <= finVentana) conteo[i[etiqueta]] = (conteo[i[etiqueta]] || 0) + 1;
  }
  return Object.entries(conteo).filter(([, n]) => n > condicion.thresholdValue).map(([grupo, n]) => ({ grupo, n }));
}

const hora = (hhmm) => Date.parse(`2026-11-19T${hhmm}:00-05:00`);
const intento = (at, origin, target) => ({ at, origin, target });
/** n intentos desde la hora local inicio (Colombia), uno cada paso minutos. */
const minutos = (inicio, n, paso, fabrica) =>
  Array.from({ length: n }, (_, i) => fabrica(hora(inicio) + i * paso * 60000, i));

// === Métrica ===

describe('HU 7 — la métrica cuenta cada rechazo con su origen y su inversor', () => {
  it('filtra exactamente el evento que escribe el guardia', () => {
    expect(definicionMetrica().filter).toBe(`jsonPayload.event="${EVENTO_RECHAZO}"`);
    expect(EVENTO_RECHAZO).toBe('investor_data_access_denied');
  });

  it('es un contador con etiquetas origin y target sacadas del registro', () => {
    const metrica = definicionMetrica();

    expect(metrica.name).toBe(NOMBRE_METRICA);
    expect(metrica.metricDescriptor).toMatchObject({ metricKind: 'DELTA', valueType: 'INT64' });
    expect(metrica.labelExtractors).toEqual({
      origin: 'EXTRACT(jsonPayload.origin)',
      target: 'EXTRACT(jsonPayload.targetUserId)',
    });
  });
});

// === Políticas ===

describe('HU 7 — dos avisos: por origen y por inversor, ≥ 10 en 60 min móviles', () => {
  it.each([
    ['origen', 'metric.label.origin'],
    ['inversor', 'metric.label.target'],
  ])('la política por %s agrupa por %s', (clave, campo) => {
    const politica = definicionPolitica(clave, CANAL);
    const condicion = politica.conditions[0].conditionThreshold;

    expect(condicion.filter).toBe(`metric.type="${TIPO_METRICA}" AND resource.type="cloud_run_revision"`);
    expect(condicion.aggregations).toEqual([{
      alignmentPeriod: '3600s', perSeriesAligner: 'ALIGN_SUM', crossSeriesReducer: 'REDUCE_SUM', groupByFields: [campo],
    }]);
    expect(condicion.comparison).toBe('COMPARISON_GT');
    expect(condicion.thresholdValue).toBe(9);
    expect(politica.notificationChannels).toEqual([CANAL]);
    expect(politica.userLabels).toEqual({ hu: 'hu-7', aviso: clave });
  });

  it('el aviso dice qué agrupó y enlaza al registro para ver la otra dimensión', () => {
    const { content } = definicionPolitica('origen', CANAL).documentation;

    expect(content).toContain('`${metric.label.origin}`');
    expect(content).toContain('contra qué inversores');
    expect(content).toContain('jsonPayload.origin%3D%22${metric.label.origin}%22');
  });

  it('sin canal de operación no se puede definir', () => {
    expect(() => definicionPolitica('origen', null)).toThrow('Falta el canal de notificación de operación');
  });
});

describe('HU 7 — los ejemplos de qa.md con la regla de la política', () => {
  it('QA-11 combinación 1: 10 desde OR-1 (5 contra B y 5 contra G) → avisa el origen, no los inversores', () => {
    const intentos = minutos('10:00', 10, 5, (h, i) => intento(h, '203.0.113.45', i % 2 ? 'uid-inversor-B' : 'uid-inversor-grande'));

    expect(gruposQueAvisan(definicionPolitica('origen', CANAL), intentos, hora('10:45'))).toEqual([{ grupo: '203.0.113.45', n: 10 }]);
    expect(gruposQueAvisan(definicionPolitica('inversor', CANAL), intentos, hora('10:45'))).toEqual([]);
  });

  it('QA-11 combinación 2: 10 contra B desde 10 IPs distintas → avisa el inversor, no los orígenes', () => {
    const intentos = minutos('12:00', 10, 5, (h, i) => intento(h, `198.51.100.${11 + i}`, 'uid-inversor-B'));

    expect(gruposQueAvisan(definicionPolitica('inversor', CANAL), intentos, hora('12:45'))).toEqual([{ grupo: 'uid-inversor-B', n: 10 }]);
    expect(gruposQueAvisan(definicionPolitica('origen', CANAL), intentos, hora('12:45'))).toEqual([]);
  });

  it('QA-12: 9 desde un origen contra un inversor → ningún aviso', () => {
    const intentos = minutos('14:00', 9, 6, (h) => intento(h, '203.0.113.45', 'uid-inversor-B'));

    expect(gruposQueAvisan(definicionPolitica('origen', CANAL), intentos, hora('14:50'))).toEqual([]);
    expect(gruposQueAvisan(definicionPolitica('inversor', CANAL), intentos, hora('14:50'))).toEqual([]);
  });

  it('QA-12: 18 repartidos 9 y 9 entre dos orígenes y dos inversores → ningún aviso', () => {
    const intentos = [
      ...minutos('16:00', 9, 6, (h) => intento(h, '203.0.113.45', 'uid-inversor-B')),
      ...minutos('16:00', 9, 6, (h) => intento(h, '198.51.100.11', 'uid-inversor-grande')),
    ];

    expect(gruposQueAvisan(definicionPolitica('origen', CANAL), intentos, hora('16:50'))).toEqual([]);
    expect(gruposQueAvisan(definicionPolitica('inversor', CANAL), intentos, hora('16:50'))).toEqual([]);
  });

  it('ventana móvil (Vacío 3): 5 a las 10:50 y 5 a las 11:10 sí avisan', () => {
    const intentos = [
      ...minutos('10:50', 5, 1, (h) => intento(h, '203.0.113.45', 'uid-inversor-B')),
      ...minutos('11:10', 5, 1, (h) => intento(h, '203.0.113.45', 'uid-inversor-B')),
    ];

    expect(gruposQueAvisan(definicionPolitica('origen', CANAL), intentos, hora('11:14'))).toEqual([{ grupo: '203.0.113.45', n: 10 }]);
  });
});

// === Aplicación idempotente ===

describe('HU 7 — aplicar dos veces no duplica', () => {
  it('elige el canal por el buzón de operación, no por su nombre', () => {
    expect(elegirCanal([canal({ type: 'sms', name: 'otro' }), canal()])).toBe(CANAL);
    expect(elegirCanal([canal({ displayName: 'Otro nombre' })])).toBe(CANAL);
  });

  it('un canal con el nombre de siempre pero otro buzón NO sirve (el fallo del 2026-11-12)', () => {
    expect(elegirCanal([canal({ labels: { email_address: 'portastockcol@gmail.com' } })])).toBeNull();
    expect(elegirCanal([canal({ enabled: false })])).toBeNull();
  });

  it('reconoce la política ya creada por sus etiquetas', () => {
    const existentes = [
      { name: 'p/1', displayName: 'Cloud Functions - High Error Rate' },
      { name: 'p/2', userLabels: { hu: 'hu-7', aviso: 'inversor' } },
    ];

    expect(politicaExistente(existentes, 'inversor')).toEqual(existentes[1]);
    expect(politicaExistente(existentes, 'origen')).toBeNull();
  });
});
