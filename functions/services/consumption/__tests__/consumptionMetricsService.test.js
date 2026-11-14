/**
 * HU 6.7 — La medición: qué se le pide a Monitoring, cómo se lee su respuesta
 * y de dónde salen los análisis de IA.
 *
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T2, T10)
 */

jest.mock('../../firebaseAdmin', () => ({ firestore: jest.fn(() => ({})) }));

const {
  METRICAS,
  consultaSerie,
  puntosDeSeries,
  medirConteoDelDia,
  medirAlmacenamiento,
  leerUsuariosIa,
} = require('../consumptionMetricsService');

// ============================================================================
// Fábricas locales
// ============================================================================

/** Una respuesta de `timeSeries.list` con los puntos del más nuevo al más viejo, como Monitoring. */
const respuestaSeries = (puntos, extra = {}) => ({
  timeSeries: [{ points: [...puntos].reverse().map(([fin, valor]) => ({ interval: { startTime: fin, endTime: fin }, value: valor })) }],
  ...extra,
});

const fetchQueResponde = (...cuerpos) => {
  const fn = jest.fn();
  cuerpos.forEach((c) => fn.mockResolvedValueOnce({ ok: true, status: 200, text: async () => JSON.stringify(c) }));
  return fn;
};

const parametros = (url) => Object.fromEntries(new URL(url).searchParams.entries());

/** Firestore de mentira para la consulta de candidatos y sus análisis. */
const crearDb = (usuarios) => {
  const consultas = [];
  return {
    consultas,
    collection: (nombre) => ({
      where: (campo, op, valor) => {
        consultas.push({ nombre, campo, op, valor });
        return {
          get: async () => ({
            docs: usuarios
              .filter((u) => u.data.lastCopilotAnalysis >= valor)
              .map((u) => ({
                id: u.uid,
                data: () => u.data,
                ref: {
                  collection: (sub) => ({
                    where: (c, o, desde) => {
                      consultas.push({ nombre: sub, campo: c, op: o, valor: desde });
                      return {
                        get: async () => ({
                          docs: u.analisis
                            .filter((a) => a.createdAt >= desde)
                            .map((a) => ({ id: a.id, data: () => ({ createdAt: a.createdAt, status: 'completed' }) })),
                        }),
                      };
                    },
                  }),
                },
              })),
          }),
        };
      },
    }),
  };
};

// ============================================================================
// Monitoring
// ============================================================================

describe('consumptionMetricsService — la consulta a Monitoring', () => {
  test('lecturas del día de Colombia: filtro de la métrica, suma por minuto de todas las series y el intervalo del día hasta ahora', async () => {
    const fetchFn = fetchQueResponde(respuestaSeries([]));

    await medirConteoDelDia({ token: 't0k', consumo: 'lecturas', dia: '2026-11-19', hasta: new Date('2026-11-19T19:20:00Z'), fetchFn });

    const [url, opciones] = fetchFn.mock.calls[0];
    expect(url.startsWith('https://monitoring.googleapis.com/v3/projects/portafolio-inversiones/timeSeries?')).toBe(true);
    expect(parametros(url)).toEqual({
      filter: `metric.type="${METRICAS.lecturas}"`,
      'interval.startTime': '2026-11-19T05:00:00.000Z',
      'interval.endTime': '2026-11-19T19:20:00.000Z',
      'aggregation.alignmentPeriod': '60s',
      'aggregation.perSeriesAligner': 'ALIGN_SUM',
      'aggregation.crossSeriesReducer': 'REDUCE_SUM',
    });
    expect(opciones.headers.Authorization).toBe('Bearer t0k');
  });

  test('un día ya cerrado se pide hasta su fin, no hasta ahora', async () => {
    const fetchFn = fetchQueResponde(respuestaSeries([]));
    await medirConteoDelDia({ token: 't', consumo: 'escrituras', dia: '2026-11-10', hasta: new Date('2026-11-19T19:20:00Z'), fetchFn });
    expect(parametros(fetchFn.mock.calls[0][0])['interval.endTime']).toBe('2026-11-11T05:00:00.000Z');
    expect(parametros(fetchFn.mock.calls[0][0]).filter).toBe(`metric.type="${METRICAS.escrituras}"`);
  });

  test('a las 00:00:30 el día aún no tiene un minuto: no se consulta (Monitoring rechaza un intervalo vacío)', async () => {
    const fetchFn = jest.fn();
    const puntos = await medirConteoDelDia({ token: 't', consumo: 'lecturas', dia: '2026-11-20', hasta: new Date('2026-11-20T05:00:30Z'), fetchFn });
    expect(puntos).toEqual([]);
    expect(fetchFn).not.toHaveBeenCalled();
  });

  test('el almacenamiento se pide como media por minuto de las últimas 24 h', async () => {
    const fetchFn = fetchQueResponde(respuestaSeries([]));
    await medirAlmacenamiento({ token: 't', hasta: new Date('2026-11-19T20:00:00Z'), fetchFn });
    expect(parametros(fetchFn.mock.calls[0][0])).toMatchObject({
      filter: `metric.type="${METRICAS.almacenamiento}"`,
      'interval.startTime': '2026-11-18T20:00:00.000Z',
      'aggregation.perSeriesAligner': 'ALIGN_MEAN',
    });
  });

  test('lee la respuesta en orden, con enteros como texto, y recorre todas las páginas', async () => {
    const fetchFn = fetchQueResponde(
      respuestaSeries([['2026-11-19T19:21:00Z', { int64Value: '150000' }]], { nextPageToken: 'p2' }),
      respuestaSeries([['2026-11-19T19:22:00Z', { int64Value: '50001' }]]),
    );

    const puntos = await medirConteoDelDia({ token: 't', consumo: 'lecturas', dia: '2026-11-19', hasta: new Date('2026-11-19T19:30:00Z'), fetchFn });

    expect(parametros(fetchFn.mock.calls[1][0]).pageToken).toBe('p2');
    expect(puntos).toEqual([
      { inicio: '2026-11-19T19:20:00.000Z', fin: '2026-11-19T19:21:00.000Z', valor: 150000 },
      { inicio: '2026-11-19T19:21:00.000Z', fin: '2026-11-19T19:22:00.000Z', valor: 50001 },
    ]);
  });

  test('si llegan varias series, las suma por minuto', () => {
    const serie = (valor) => ({ points: [{ interval: { endTime: '2026-11-19T10:01:00Z' }, value: { doubleValue: valor } }] });
    expect(puntosDeSeries([serie(1.5), serie(2.5)])).toEqual([
      { inicio: '2026-11-19T10:00:00.000Z', fin: '2026-11-19T10:01:00.000Z', valor: 4 },
    ]);
  });

  test('Vacío 6: si Monitoring falla, la medición lanza (y el vigilante no marca nada)', async () => {
    const fetchFn = jest.fn().mockResolvedValue({ ok: false, status: 503, text: async () => 'Service Unavailable' });
    await expect(medirAlmacenamiento({ token: 't', hasta: new Date(), fetchFn })).rejects.toThrow('→ 503');
  });

  test('la consulta no pide nada más que la serie', () => {
    expect(consultaSerie({ metrica: 'm', inicio: 'a', fin: 'b', alineador: 'ALIGN_SUM' })).not.toContain('pageToken');
  });
});

// ============================================================================
// Análisis de IA
// ============================================================================

describe('consumptionMetricsService — los análisis de IA salen de Firestore', () => {
  const ahora = new Date('2026-12-12T15:15:00Z');

  test('candidatos por lastCopilotAnalysis de las últimas 26 h, y de cada uno sus análisis de los 7 días anteriores', async () => {
    const db = crearDb([
      {
        uid: 'uid-inversor-pro-01',
        data: { lastCopilotAnalysis: '2026-12-12T15:05:00.000Z', subscription: { planId: 'pro', status: 'active' }, email: 'inversor-pro@ejemplo.com' },
        analisis: [
          { id: 'p0', createdAt: '2026-11-30T10:00:00.000Z' },
          { id: 'p1', createdAt: '2026-12-07T23:00:00.000Z' },
          { id: 'p2', createdAt: '2026-12-12T15:00:00.000Z' },
        ],
      },
      { uid: 'uid-viejo', data: { lastCopilotAnalysis: '2026-12-01T10:00:00.000Z' }, analisis: [] },
    ]);

    const usuarios = await leerUsuariosIa({ db, ahora });

    expect(db.consultas[0]).toEqual({ nombre: 'userData', campo: 'lastCopilotAnalysis', op: '>=', valor: '2026-12-11T13:15:00.000Z' });
    expect(db.consultas[1]).toEqual({ nombre: 'copilotAnalyses', campo: 'createdAt', op: '>=', valor: '2026-12-04T13:15:00.000Z' });
    expect(usuarios).toEqual([
      {
        uid: 'uid-inversor-pro-01',
        conAcceso: true,
        analisis: [{ id: 'p1', createdAt: '2026-12-07T23:00:00.000Z' }, { id: 'p2', createdAt: '2026-12-12T15:00:00.000Z' }],
      },
    ]);
  });

  test('D2: el acceso es el del plan al evaluar; un plan gratuito no tiene acceso', async () => {
    const db = crearDb([
      { uid: 'uid-inversor-free-01', data: { lastCopilotAnalysis: '2026-12-12T15:00:00.000Z', subscription: { planId: 'free', status: 'active' } }, analisis: [] },
      { uid: 'uid-inversor-life-01', data: { lastCopilotAnalysis: '2026-12-12T15:00:00.000Z', subscription: { planId: 'lifetime', status: 'active' } }, analisis: [] },
    ]);
    const usuarios = await leerUsuariosIa({ db, ahora });
    expect(usuarios.map((u) => [u.uid, u.conAcceso])).toEqual([['uid-inversor-free-01', false], ['uid-inversor-life-01', true]]);
  });
});
