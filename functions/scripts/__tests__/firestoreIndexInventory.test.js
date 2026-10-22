/**
 * HU 6.1 — Una sola fuente para la configuración de búsqueda.
 *
 * Defiende la comparación entre lo declarado y lo activo: es la que decide si
 * publicar puede borrar una entrada que otro proceso necesita.
 *
 * @see platform-docs/stories/6.1-fuente-unica-configuracion-busqueda/historia.md
 * @see platform-docs/stories/6.1-fuente-unica-configuracion-busqueda/refinamiento.md (T5)
 */

const fs = require('fs');
const path = require('path');

const {
  normalizarIndice,
  normalizarFieldOverride,
  describirIndice,
  compararConActivos,
  compararDeclaraciones,
  motivosParaNoPublicar,
  formatearInforme,
} = require('../firestoreIndexInventory');

// === Fábricas locales ===

const campo = (fieldPath, order = 'ASCENDING') => ({ fieldPath, order });

const indice = (overrides = {}) => ({
  collectionGroup: 'transactions',
  queryScope: 'COLLECTION',
  fields: [campo('userId'), campo('date', 'DESCENDING')],
  ...overrides,
});

/** El índice del que depende la reconciliación nocturna, tal cual lo declara el fichero. */
const indiceReconciliacion = () =>
  indice({
    collectionGroup: 'portfolioPerformance',
    fields: [campo('_stale.since', 'DESCENDING'), campo('_stale', 'DESCENDING')],
  });

/** El mismo índice tal cual lo informa el CLI: con `__name__` y con `density`. */
const indiceReconciliacionActivo = () => ({
  collectionGroup: 'portfolioPerformance',
  queryScope: 'COLLECTION',
  fields: [
    campo('_stale.since', 'DESCENDING'),
    campo('_stale', 'DESCENDING'),
    campo('__name__', 'DESCENDING'),
  ],
  density: 'SPARSE_ALL',
});

const override = (overrides = {}) => ({
  collectionGroup: 'dates',
  fieldPath: 'date',
  ttl: false,
  indexes: [
    { order: 'ASCENDING', queryScope: 'COLLECTION' },
    { order: 'DESCENDING', queryScope: 'COLLECTION' },
  ],
  ...overrides,
});

const configuracion = (indexes = [], fieldOverrides = []) => ({ indexes, fieldOverrides });

// === Tests ===

describe('normalizarIndice — el `__name__` implícito no es una diferencia', () => {
  it('da la misma clave al índice declarado y al que informa producción', () => {
    expect(normalizarIndice(indiceReconciliacionActivo())).toBe(
      normalizarIndice(indiceReconciliacion()),
    );
  });

  it('ignora `density`, que el CLI añade y el fichero declarado nunca escribe', () => {
    expect(normalizarIndice(indice({ density: 'SPARSE_ALL' }))).toBe(normalizarIndice(indice()));
  });

  it('asume COLLECTION cuando el índice declarado no dice el ámbito', () => {
    const sinAmbito = indice();
    delete sinAmbito.queryScope;

    expect(normalizarIndice(sinAmbito)).toBe(normalizarIndice(indice({ queryScope: 'COLLECTION' })));
  });

  it('distingue dos índices que sólo se diferencian en la dirección de un campo', () => {
    const ascendente = indice({ fields: [campo('userId'), campo('date', 'ASCENDING')] });
    const descendente = indice({ fields: [campo('userId'), campo('date', 'DESCENDING')] });

    expect(normalizarIndice(ascendente)).not.toBe(normalizarIndice(descendente));
  });

  it('distingue dos índices con los mismos campos en distinto orden', () => {
    const unOrden = indice({ fields: [campo('userId'), campo('type')] });
    const otroOrden = indice({ fields: [campo('type'), campo('userId')] });

    expect(normalizarIndice(unOrden)).not.toBe(normalizarIndice(otroOrden));
  });
});

describe('normalizarFieldOverride — el orden de las configuraciones no significa nada', () => {
  it('da la misma clave aunque las configuraciones vengan al revés', () => {
    const alReves = override({
      indexes: [
        { order: 'DESCENDING', queryScope: 'COLLECTION' },
        { order: 'ASCENDING', queryScope: 'COLLECTION' },
      ],
    });

    expect(normalizarFieldOverride(alReves)).toBe(normalizarFieldOverride(override()));
  });

  it('distingue un override con TTL de uno sin TTL', () => {
    expect(normalizarFieldOverride(override({ ttl: true }))).not.toBe(
      normalizarFieldOverride(override({ ttl: false })),
    );
  });
});

describe('compararConActivos — Escenario 3: la diferencia se ve antes de publicar', () => {
  it('señala como faltante la entrada activa que nadie declara (RN-6.1-J)', () => {
    const resultado = compararConActivos({
      declarada: configuracion([indice()]),
      activa: configuracion([indice(), indiceReconciliacionActivo()]),
    });

    expect(resultado.alineado).toBe(false);
    expect(resultado.indices.faltantes).toHaveLength(1);
    expect(describirIndice(resultado.indices.faltantes[0])).toBe(
      'portfolioPerformance [COLLECTION] _stale.since:DESCENDING, _stale:DESCENDING',
    );
    expect(resultado.indices.sobrantes).toEqual([]);
  });

  it('señala como sobrante la entrada declarada que todavía no está activa', () => {
    const resultado = compararConActivos({
      declarada: configuracion([indice(), indiceReconciliacion()]),
      activa: configuracion([indice()]),
    });

    expect(resultado.alineado).toBe(false);
    expect(resultado.indices.faltantes).toEqual([]);
    expect(resultado.indices.sobrantes).toHaveLength(1);
    expect(resultado.indices.sobrantes[0].collectionGroup).toBe('portfolioPerformance');
  });

  it('también compara las configuraciones de campo, que entran en la misma pregunta de borrado', () => {
    const resultado = compararConActivos({
      declarada: configuracion([indice()], []),
      activa: configuracion([indice()], [override()]),
    });

    expect(resultado.alineado).toBe(false);
    expect(resultado.fieldOverrides.faltantes).toHaveLength(1);
    expect(resultado.fieldOverrides.faltantes[0].fieldPath).toBe('date');
  });
});

describe('compararConActivos — Escenario 2: publicar no puede borrar lo que otro necesita', () => {
  it('no deja ningún faltante cuando lo declarado cubre exactamente lo activo', () => {
    const resultado = compararConActivos({
      declarada: configuracion([indice(), indiceReconciliacion()], [override()]),
      activa: configuracion([indice(), indiceReconciliacionActivo()], [override()]),
    });

    expect(resultado.alineado).toBe(true);
    expect(resultado.indices.faltantes).toEqual([]);
    expect(resultado.indices.sobrantes).toEqual([]);
    expect(resultado.fieldOverrides.faltantes).toEqual([]);
  });
});

describe('compararDeclaraciones — el espejo no se edita a mano', () => {
  it('detecta lo que el canónico declara y el espejo todavía no', () => {
    const resultado = compararDeclaraciones({
      canonica: configuracion([indice(), indiceReconciliacion()]),
      espejo: configuracion([indice()]),
    });

    expect(resultado.identicos).toBe(false);
    expect(resultado.indices.soloEnCanonico).toHaveLength(1);
    expect(resultado.indices.soloEnCanonico[0].collectionGroup).toBe('portfolioPerformance');
    expect(resultado.indices.soloEnEspejo).toEqual([]);
  });

  it('detecta lo que alguien añadió al espejo sin pasar por el canónico', () => {
    const resultado = compararDeclaraciones({
      canonica: configuracion([indice()]),
      espejo: configuracion([indice(), indiceReconciliacion()]),
    });

    expect(resultado.identicos).toBe(false);
    expect(resultado.indices.soloEnEspejo).toHaveLength(1);
    expect(resultado.indices.soloEnCanonico).toEqual([]);
  });

  it('da por idénticas dos declaraciones con el mismo contenido', () => {
    const resultado = compararDeclaraciones({
      canonica: configuracion([indice(), indiceReconciliacion()], [override()]),
      espejo: configuracion([indice(), indiceReconciliacion()], [override()]),
    });

    expect(resultado.identicos).toBe(true);
  });
});

describe('formatearInforme — el aviso dice qué está desalineado', () => {
  it('nombra la entrada de la reconciliación cuando nadie la declara (Escenario 3)', () => {
    const comparacionActivos = compararConActivos({
      declarada: configuracion([indice()]),
      activa: configuracion([indice(), indiceReconciliacionActivo()]),
    });

    const informe = formatearInforme({ comparacionActivos, comparacionEspejo: null });

    expect(informe).toContain('publicar ofrecería BORRARLAS');
    expect(informe).toContain('portfolioPerformance [COLLECTION] _stale.since:DESCENDING, _stale:DESCENDING');
  });

  it('dice que no hay diferencias cuando todo coincide (Escenario 2)', () => {
    const comparacionActivos = compararConActivos({
      declarada: configuracion([indice()]),
      activa: configuracion([indice()]),
    });

    expect(formatearInforme({ comparacionActivos, comparacionEspejo: null })).toBe(
      '  Sin diferencias: lo declarado es exactamente lo que está activo.',
    );
  });
});

describe('motivosParaNoPublicar — las dos direcciones de una divergencia no son iguales', () => {
  const estado = (overrides = {}) => ({
    comparacionActivos: compararConActivos({
      declarada: configuracion([indice()]),
      activa: configuracion([indice()]),
    }),
    comparacionEspejo: compararDeclaraciones({
      canonica: configuracion([indice()]),
      espejo: configuracion([indice()]),
    }),
    mismoTexto: true,
    hayEspejo: true,
    ...overrides,
  });

  it('deja publicar cuando todo coincide', () => {
    expect(motivosParaNoPublicar(estado())).toEqual([]);
  });

  it('DEJA publicar una entrada declarada que todavía no está activa: es lo que crea el despliegue', () => {
    const motivos = motivosParaNoPublicar(
      estado({
        comparacionActivos: compararConActivos({
          declarada: configuracion([indice(), indiceReconciliacion()]),
          activa: configuracion([indice()]),
        }),
      }),
    );

    expect(motivos).toEqual([]);
  });

  it('deja publicar una configuración de campo declarada que todavía no está activa (HU 6.2)', () => {
    const comodin = { collectionGroup: 'dates', fieldPath: '*', indexes: [] };
    const motivos = motivosParaNoPublicar(
      estado({
        comparacionActivos: compararConActivos({
          declarada: configuracion([indice()], [comodin]),
          activa: configuracion([indice()], []),
        }),
      }),
    );

    expect(motivos).toEqual([]);
  });

  it('NO deja publicar si hay un índice activo que nadie declara: publicar ofrecería borrarlo', () => {
    const motivos = motivosParaNoPublicar(
      estado({
        comparacionActivos: compararConActivos({
          declarada: configuracion([indice()]),
          activa: configuracion([indice(), indiceReconciliacionActivo()]),
        }),
      }),
    );

    expect(motivos).toHaveLength(1);
    expect(motivos[0]).toContain('ofrecería borrarlos');
  });

  it('NO deja publicar si hay una configuración de campo activa que nadie declara', () => {
    const motivos = motivosParaNoPublicar(
      estado({
        comparacionActivos: compararConActivos({
          declarada: configuracion([indice()], []),
          activa: configuracion([indice()], [override()]),
        }),
      }),
    );

    expect(motivos).toHaveLength(1);
    expect(motivos[0]).toContain('ofrecería borrarlas');
  });

  it('NO deja publicar si el espejo se editó a mano', () => {
    const motivos = motivosParaNoPublicar(
      estado({
        comparacionEspejo: compararDeclaraciones({
          canonica: configuracion([indice()]),
          espejo: configuracion([indice(), indiceReconciliacion()]),
        }),
      }),
    );

    expect(motivos).toHaveLength(1);
    expect(motivos[0]).toContain('copia exacta');
  });

  it('NO deja publicar si el espejo declara lo mismo pero el texto difiere', () => {
    const motivos = motivosParaNoPublicar(estado({ mismoTexto: false }));

    expect(motivos).toHaveLength(1);
    expect(motivos[0]).toContain('`sync`');
  });

  it('ignora el espejo si el repositorio de la aplicación no está en el workspace', () => {
    expect(
      motivosParaNoPublicar(estado({ hayEspejo: false, comparacionEspejo: null, mismoTexto: true })),
    ).toEqual([]);
  });
});

describe('Escenario 1 — la configuración canónica de este repositorio', () => {
  const RUTA_CANONICO = path.resolve(__dirname, '../../../firestore.indexes.json');
  const RUTA_ESPEJO = path.resolve(
    __dirname,
    '../../../../portafolio-inversiones/firestore.indexes.json',
  );

  const canonica = JSON.parse(fs.readFileSync(RUTA_CANONICO, 'utf8'));

  it('declara las 34 entradas activas en producción (RN-6.1-J)', () => {
    expect(canonica.indexes).toHaveLength(34);
  });

  it('declara el índice del que depende la reconciliación nocturna (Escenario 4)', () => {
    const claves = canonica.indexes.map(normalizarIndice);

    expect(claves).toContain(normalizarIndice(indiceReconciliacion()));
  });

  it('no declara dos veces el mismo índice', () => {
    const claves = canonica.indexes.map(normalizarIndice);

    expect(new Set(claves).size).toBe(claves.length);
  });

  // El repositorio de la aplicación es otro repositorio git: puede no estar en
  // el workspace, y el de servicios tiene que seguir siendo clonable suelto.
  const hayEspejo = fs.existsSync(RUTA_ESPEJO);
  const siHayEspejo = hayEspejo ? it : it.skip;

  siHayEspejo('tiene como espejo una copia exacta en el repositorio de la aplicación', () => {
    expect(fs.readFileSync(RUTA_ESPEJO, 'utf8')).toBe(fs.readFileSync(RUTA_CANONICO, 'utf8'));
  });
});
