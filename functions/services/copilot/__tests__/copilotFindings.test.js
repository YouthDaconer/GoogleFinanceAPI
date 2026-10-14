/**
 * HU 5.3 — Qué se queda de lo que el modelo investigó, y qué se tira.
 *
 * Las tres reglas que esto defiende son las que hacen verificable el análisis:
 * un hallazgo sin fuente no se publica (RN-5-E), una cifra fundamental sin su
 * periodo y su fecha no se publica (RN-5.3-Q), y **una ausencia no es un
 * fallo** (RN-5.3-R) — decir "sin cobertura de analistas" cuando en realidad no
 * se pudo consultar engaña al usuario.
 *
 * @see services/copilot/copilotFindings.js
 */

const {
  RESEARCH_OUTCOME,
  esCifraFundamental,
  parsearRespuesta,
  filtrarHallazgos,
  filtrarConflictos,
  normalizarInvestigacion,
  normalizarInvestigacionLote,
  esRancia,
} = require('../copilotFindings');

const crearHallazgo = (overrides = {}) => ({
  label: 'Noticia',
  value: 'Microsoft presenta resultados',
  period: null,
  publishedAt: '2026-10-01',
  sourceUrl: 'https://reuters.com/x',
  sourceTitle: 'Reuters',
  ...overrides,
});

const crearCifra = (overrides = {}) => crearHallazgo({
  label: 'Ingresos',
  value: '65.000 M USD',
  period: 'Q3 2026',
  publishedAt: '2026-10-01',
  ...overrides,
});

describe('parsearRespuesta', () => {
  it('lee un JSON limpio', () => {
    expect(parsearRespuesta('{"findings": []}')).toEqual({ findings: [] });
  });

  it('lee un JSON envuelto en un bloque de código', () => {
    expect(parsearRespuesta('```json\n{"findings": []}\n```')).toEqual({ findings: [] });
  });

  it('rescata el JSON cuando el modelo lo rodea de prosa', () => {
    expect(parsearRespuesta('Aquí tienes:\n{"findings": []}\nEspero que sirva'))
      .toEqual({ findings: [] });
  });

  it('devuelve null si no hay JSON utilizable', () => {
    expect(parsearRespuesta('no pude encontrar nada')).toBeNull();
    expect(parsearRespuesta(null)).toBeNull();
  });
});

describe('filtrarHallazgos', () => {
  it('conserva el hallazgo completo', () => {
    const { validos } = filtrarHallazgos([crearHallazgo()]);
    expect(validos).toHaveLength(1);
  });

  it('descarta el hallazgo sin fuente (RN-5-E)', () => {
    const { validos, descartados } = filtrarHallazgos([crearHallazgo({ sourceUrl: undefined })]);

    expect(validos).toEqual([]);
    expect(descartados[0].motivo).toBe('sin_fuente');
  });

  it('descarta la cifra fundamental sin periodo (RN-5.3-Q)', () => {
    const { validos, descartados } = filtrarHallazgos([crearCifra({ period: null })]);

    expect(validos).toEqual([]);
    expect(descartados[0].motivo).toBe('sin_periodo');
  });

  it('descarta la cifra fundamental sin fecha de publicación', () => {
    const { validos } = filtrarHallazgos([crearCifra({ publishedAt: null })]);
    expect(validos).toEqual([]);
  });

  it('a una noticia no le exige periodo: no es una cifra fundamental', () => {
    const { validos } = filtrarHallazgos([crearHallazgo({ period: null })]);
    expect(validos).toHaveLength(1);
  });

  it('descarta lo vacío', () => {
    const { validos } = filtrarHallazgos([crearHallazgo({ value: '' }), crearHallazgo({ label: '' })]);
    expect(validos).toEqual([]);
  });

  it('reconoce una cifra fundamental por su etiqueta, en los dos idiomas', () => {
    expect(esCifraFundamental({ label: 'Ingresos' })).toBe(true);
    expect(esCifraFundamental({ label: 'Revenue' })).toBe(true);
    expect(esCifraFundamental({ label: 'Margen operativo' })).toBe(true);
    expect(esCifraFundamental({ label: 'Noticia' })).toBe(false);
  });
});

describe('filtrarConflictos (Escenario 9)', () => {
  it('conserva el conflicto con dos versiones y sus fuentes', () => {
    const conflictos = filtrarConflictos([{
      about: 'precio objetivo',
      versions: [
        { value: '610 USD', sourceUrl: 'https://a' },
        { value: '540 USD', sourceUrl: 'https://b' },
      ],
    }]);

    expect(conflictos).toHaveLength(1);
    expect(conflictos[0].versions).toHaveLength(2);
  });

  it('descarta el conflicto de una sola versión: no hay contradicción que mostrar', () => {
    expect(filtrarConflictos([{ about: 'x', versions: [{ value: 'a', sourceUrl: 'https://a' }] }]))
      .toEqual([]);
  });

  it('descarta la versión sin fuente: el caso pide mostrar de dónde sale cada una', () => {
    expect(filtrarConflictos([{
      about: 'x',
      versions: [{ value: 'a', sourceUrl: 'https://a' }, { value: 'b' }],
    }])).toEqual([]);
  });
});

describe('normalizarInvestigacion — ausencia frente a fallo (RN-5.3-R)', () => {
  it('con hallazgos, el activo se investigó', () => {
    const texto = JSON.stringify({ findings: [crearHallazgo()] });
    expect(normalizarInvestigacion('MSFT', texto).outcome).toBe(RESEARCH_OUTCOME.OK);
  });

  it('sin hallazgos pero con una nota, es una AUSENCIA y se entrega', () => {
    const texto = JSON.stringify({
      findings: [],
      notes: ['Sin cobertura de analistas en las fuentes consultadas'],
    });
    const inv = normalizarInvestigacion('PFBCOLOM', texto);

    expect(inv.outcome).toBe(RESEARCH_OUTCOME.SIN_DATOS);
    expect(inv.notes[0]).toContain('Sin cobertura');
  });

  it('sin respuesta, es un FALLO: no se disfraza de ausencia', () => {
    const inv = normalizarInvestigacion('NVDA', null);

    expect(inv.outcome).toBe(RESEARCH_OUTCOME.FALLO);
    expect(inv.notes).toEqual([]);
  });

  it('un JSON ilegible es un fallo, no un activo sin datos', () => {
    // Decir "sin cobertura" aquí le diría al usuario algo que nadie comprobó.
    expect(normalizarInvestigacion('NVDA', 'se me cayó la conexión').outcome)
      .toBe(RESEARCH_OUTCOME.FALLO);
  });

  it('una respuesta vacía de todo también es un fallo', () => {
    expect(normalizarInvestigacion('NVDA', '{"findings": [], "notes": []}').outcome)
      .toBe(RESEARCH_OUTCOME.FALLO);
  });

  it('un hallazgo que no pasa el filtro no convierte el activo en investigado', () => {
    const texto = JSON.stringify({ findings: [crearHallazgo({ sourceUrl: undefined })], notes: [] });
    const inv = normalizarInvestigacion('MSFT', texto);

    expect(inv.outcome).toBe(RESEARCH_OUTCOME.FALLO);
    expect(inv.descartados[0].motivo).toBe('sin_fuente');
  });
});

describe('esRancia — una cifra de hace demasiado no es "lo último"', () => {
  const AHORA = new Date('2026-10-13T12:00:00Z');

  it('un resultado de hace dos años sí lo es', () => {
    // Es lo que llegó a producción: MSFT y CRM con cifras y fuentes de 2024 en
    // un análisis de 2026, y una recomendación anclada a un precio que ya no
    // existía.
    expect(esRancia('2024-07-30', AHORA)).toBe(true);
  });

  it('el trimestre más reciente no', () => {
    expect(esRancia('2026-08-05', AHORA)).toBe(false);
  });

  it('justo en el límite de los 12 meses, todavía vale', () => {
    expect(esRancia('2025-10-14', AHORA)).toBe(false);
    expect(esRancia('2025-10-12', AHORA)).toBe(true);
  });

  it('lo que no trae fecha no se juzga aquí', () => {
    // De eso se encarga la regla que exige periodo y fecha a toda cifra
    // fundamental; tratarlo como rancio lo descartaría dos veces por lo mismo.
    expect(esRancia(null, AHORA)).toBe(false);
    expect(esRancia('no-es-una-fecha', AHORA)).toBe(false);
  });
});

// ============================================================================
// HU 5.6 — El reparto de la tanda
// ============================================================================

describe('HU 5.6 — una tanda se reparte por clave', () => {
  const hallazgoValido = (valor) => ({
    label: 'Qué está pasando',
    value: valor,
    sourceUrl: 'https://reuters.com/a',
    sourceTitle: 'Reuters',
  });

  const tanda = (claves) => JSON.stringify(Object.fromEntries(
    claves.map((c) => [c, { findings: [hallazgoValido(`lo de ${c}`)], notes: [], conflicts: [] }]),
  ));

  it('cada clave recibe lo suyo, y el macro también', () => {
    const repartida = normalizarInvestigacionLote(
      ['MSFT', 'AAPL', 'macro'], tanda(['MSFT', 'AAPL', 'macro']),
    );

    expect(repartida.get('MSFT').findings[0].value).toBe('lo de MSFT');
    expect(repartida.get('AAPL').findings[0].value).toBe('lo de AAPL');
    expect(repartida.get('macro').findings[0].value).toBe('lo de macro');
  });

  it('la clave que el modelo omite queda fallida, no "sin datos" (RN-5.3-R)', () => {
    const repartida = normalizarInvestigacionLote(['MSFT', 'AAPL'], tanda(['MSFT']));

    expect(repartida.get('MSFT').outcome).toBe(RESEARCH_OUTCOME.OK);
    expect(repartida.get('AAPL').outcome).toBe(RESEARCH_OUTCOME.FALLO);
    expect(repartida.get('AAPL').notes).toEqual([]);
  });

  it('el ticker en minúsculas también se encuentra', () => {
    // El ticker lo reescribe el modelo, y "msft" es lo que devolvió más de una vez.
    const repartida = normalizarInvestigacionLote(['MSFT'], tanda(['msft']));

    expect(repartida.get('MSFT').outcome).toBe(RESEARCH_OUTCOME.OK);
  });

  it('un JSON ilegible deja fallidas a todas las claves, no a ninguna', () => {
    const repartida = normalizarInvestigacionLote(['MSFT', 'macro'], 'esto no es JSON');

    expect([...repartida.values()].every((i) => i.outcome === RESEARCH_OUTCOME.FALLO)).toBe(true);
  });

  it('una tanda que ni se llegó a pedir devuelve una entrada por clave igualmente', () => {
    const repartida = normalizarInvestigacionLote(['MSFT', 'macro'], null);

    expect([...repartida.keys()]).toEqual(['MSFT', 'macro']);
  });

  it('las reglas de cada hallazgo se siguen aplicando dentro del lote', () => {
    // Una cifra fundamental sin periodo no se publica (RN-5.3-Q), venga de
    // donde venga: que la respuesta sea una tanda no relaja el filtro.
    const sinPeriodo = JSON.stringify({
      MSFT: {
        findings: [{ label: 'Ingresos', value: '70 B', sourceUrl: 'https://x' }],
        notes: ['no se encontró el periodo'],
        conflicts: [],
      },
    });

    const repartida = normalizarInvestigacionLote(['MSFT'], sinPeriodo);

    expect(repartida.get('MSFT').findings).toEqual([]);
    expect(repartida.get('MSFT').outcome).toBe(RESEARCH_OUTCOME.SIN_DATOS);
  });
});

describe('HU 5.6 — un salto de línea crudo no tira la tanda entera', () => {
  it('rescata el JSON con un salto de línea dentro de un texto', () => {
    // Medido el 14/10/2026: `gemini-2.5-flash` escribe el valor de un hallazgo
    // largo con saltos de línea literales, y JSON.parse lo rechaza. Un carácter
    // tiraba una tanda de siete activos que por lo demás vino perfecta.
    const roto = '{"MSFT": {"findings": [{"label": "Qué pasa",'
      + ' "value": "Primera línea\nsegunda línea", "sourceUrl": "https://x"}],'
      + ' "notes": [], "conflicts": []}}';

    const datos = parsearRespuesta(roto);

    expect(datos).not.toBeNull();
    expect(datos.MSFT.findings[0].value).toBe('Primera línea\nsegunda línea');
  });

  it('no toca el JSON que ya era válido', () => {
    const bueno = '{"a": {"b": "con \n escapado"}}';

    expect(parsearRespuesta(bueno)).toEqual({ a: { b: 'con \n escapado' } });
  });

  it('respeta las comillas escapadas al decidir qué está dentro de una cadena', () => {
    const conComillas = '{"a": "dice \\"hola\\"\ny sigue"}';

    expect(parsearRespuesta(conComillas).a).toBe('dice "hola"\ny sigue');
  });

  it('lo que no es JSON ni con reparación sigue siendo un fallo', () => {
    expect(parsearRespuesta('esto no es JSON en absoluto')).toBeNull();
    expect(parsearRespuesta('{roto sin cerrar')).toBeNull();
  });

  it('sigue tolerando el bloque de código y la prosa alrededor', () => {
    expect(parsearRespuesta('```json\n{"a": 1}\n```')).toEqual({ a: 1 });
    expect(parsearRespuesta('Aquí tienes:\n{"a": 1}\nespero que sirva')).toEqual({ a: 1 });
  });
});
