/**
 * El caché de investigación por activo.
 *
 * Cada búsqueda con *grounding* se factura aparte y es el grueso del coste de
 * un análisis. Investigar MSFT dos veces el mismo día es pagar dos veces por
 * la misma respuesta.
 *
 * @see services/copilot/copilotResearchCache.js
 */

const firestoreFalso = {};

jest.mock('../../firebaseAdmin', () => ({
  firestore: () => firestoreFalso,
}));

const cache = require('../copilotResearchCache');

const AHORA = new Date('2026-10-13T18:00:00.000Z');

const investigacion = (overrides = {}) => ({
  ticker: 'MSFT',
  outcome: 'ok',
  findings: [{ label: 'Ingresos', value: '70.000 M', sourceUrl: 'https://x' }],
  notes: [],
  conflicts: [],
  groundingSources: [{ url: 'https://vertexaisearch/abc' }],
  tokensUsed: 5000,
  ...overrides,
});

/**
 * Firestore con un documento guardado hace las horas dadas.
 *
 * @param {number|null} horas - `null` si no hay documento.
 */
function montarLectura(horas) {
  const guardadoEn = horas === null
    ? null
    : new Date(AHORA.getTime() - horas * 3600 * 1000).toISOString();

  firestoreFalso.collection = () => ({
    doc: () => ({
      get: async () => ({
        exists: horas !== null,
        data: () => ({ at: guardadoEn, research: cache.paraGuardar(investigacion()) }),
      }),
    }),
  });
}

describe('clave — el idioma forma parte de la identidad', () => {
  it('distingue español de inglés', () => {
    // Los hallazgos vienen redactados: servir los de un análisis en español a
    // uno en inglés metería textos en el idioma equivocado en el informe.
    expect(cache.clave('MSFT', 'es')).not.toBe(cache.clave('MSFT', 'en'));
  });

  it('el ticker no distingue mayúsculas', () => {
    expect(cache.clave('msft', 'es')).toBe(cache.clave('MSFT', 'es'));
  });
});

describe('leer — sólo lo que sigue vigente', () => {
  it('una investigación reciente se reutiliza', async () => {
    montarLectura(2);

    const leida = await cache.leer('MSFT', 'es', AHORA);

    expect(leida.findings).toHaveLength(1);
    expect(leida.desdeCache).toBe(true);
  });

  it('pasadas las horas de vida, ya no', async () => {
    montarLectura(cache.HORAS_DE_VIDA + 1);

    expect(await cache.leer('MSFT', 'es', AHORA)).toBeNull();
  });

  it('justo en el límite tampoco', async () => {
    montarLectura(cache.HORAS_DE_VIDA);

    expect(await cache.leer('MSFT', 'es', AHORA)).toBeNull();
  });

  it('sin nada guardado, null', async () => {
    montarLectura(null);

    expect(await cache.leer('MSFT', 'es', AHORA)).toBeNull();
  });

  it('lo servido del caché no suma tokens', async () => {
    // Lo que costó la primera vez no es un dato de la segunda; sumarlo otra
    // vez falsearía el consumo que se le atribuye a este análisis.
    montarLectura(1);

    expect((await cache.leer('MSFT', 'es', AHORA)).tokensUsed).toBe(0);
  });

  it('si Firestore falla, es un fallo de caché, no del análisis', async () => {
    firestoreFalso.collection = () => { throw new Error('sin conexión'); };

    expect(await cache.leer('MSFT', 'es', AHORA)).toBeNull();
  });
});

describe('guardar — sólo lo que vale la pena servir', () => {
  const capturar = () => {
    const escrituras = [];
    firestoreFalso.collection = () => ({
      doc: () => ({ set: async (datos) => escrituras.push(datos) }),
    });
    return escrituras;
  };

  it('guarda una investigación con hallazgos', async () => {
    const escrituras = capturar();

    await cache.guardar('MSFT', 'es', investigacion(), AHORA);

    expect(escrituras).toHaveLength(1);
    expect(escrituras[0].at).toBe(AHORA.toISOString());
    expect(escrituras[0].research.findings).toHaveLength(1);
  });

  it('NO guarda un fallo', async () => {
    // Servirlo durante horas convertiría un tropiezo puntual en una avería
    // larga, y para todos los usuarios.
    const escrituras = capturar();

    await cache.guardar('MSFT', 'es', investigacion({ outcome: 'fallo' }), AHORA);

    expect(escrituras).toHaveLength(0);
  });

  it('NO guarda una investigación sin hallazgos', async () => {
    const escrituras = capturar();

    await cache.guardar('MSFT', 'es', investigacion({ findings: [] }), AHORA);

    expect(escrituras).toHaveLength(0);
  });

  it('no guarda los tokens: no son un dato del activo', async () => {
    const escrituras = capturar();

    await cache.guardar('MSFT', 'es', investigacion(), AHORA);

    expect(escrituras[0].research).not.toHaveProperty('tokensUsed');
  });
});
