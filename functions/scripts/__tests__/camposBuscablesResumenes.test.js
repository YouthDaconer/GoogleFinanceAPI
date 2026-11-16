/**
 * HU 6.10.2 — Qué se prepara para búsqueda en resúmenes y consolidados, y el
 * aviso cuando una búsqueda pide algo retirado.
 *
 * La configuración real solo se puede ver aplicada en producción (no hay
 * entorno de pruebas, y el emulador de Firestore no aplica índices). Por eso
 * estos tests vigilan lo que **se publica**: el canónico declara exactamente
 * la lista de campos buscables, ni uno menos (rompería una búsqueda) ni uno
 * más (devolvería el gasto).
 *
 * @see platform-docs/stories/6.10.2-indices-solo-donde-se-busca/historia.md
 * @see platform-docs/stories/6.10.2-indices-solo-donde-se-busca/refinamiento.md (T7)
 */

const fs = require('fs');
const path = require('path');

const {
  ASC,
  DESC,
  GRUPOS,
  LISTAS_QUE_CRECEN_POR_DIA,
  camposDe,
  esBuscable,
  mensajeDeCampoNoBuscable,
} = require('../camposBuscablesResumenes');
const { RUTA_CANONICO } = require('../camposBuscablesHistorial');
const { normalizarFieldOverride } = require('../firestoreIndexInventory');

const RUTA_CANONICA = path.resolve(__dirname, '../../../firestore.indexes.json');
const RUTA_ESPEJO = path.resolve(__dirname, '../../../../portafolio-inversiones/firestore.indexes.json');
const canonica = JSON.parse(fs.readFileSync(RUTA_CANONICA, 'utf8'));
const overrides = canonica.fieldOverrides || [];

/** Órdenes declarados de una exención, en el vocabulario del módulo (`asc`/`desc`). */
const ordenesDeclarados = (override) =>
  (override.indexes || [])
    .map((i) => ({ ASCENDING: ASC, DESCENDING: DESC })[i.order])
    .sort();

/** Las exenciones de un grupo, salvo el comodín. */
const especificasDe = (grupo) => overrides.filter((o) => o.collectionGroup === grupo && o.fieldPath !== '*');

describe('Escenarios 1 y 5 — el canónico apaga la preparación por defecto', () => {
  it.each(Object.keys(GRUPOS))('declara el comodín de `%s` sin índices', (grupo) => {
    const comodin = overrides.find((o) => o.collectionGroup === grupo && o.fieldPath === '*');

    expect(comodin).toBeDefined();
    expect(comodin.indexes).toEqual([]);
  });

  it('ninguna lista que crece por día está habilitada (RN-6.10.2-L, QA-03, QA-12)', () => {
    const habilitadas = especificasDe('performanceSnapshots')
      .map((o) => o.fieldPath.split('.')[0])
      .filter((raiz) => LISTAS_QUE_CRECEN_POR_DIA.includes(raiz));

    expect(habilitadas).toEqual([]);
    LISTAS_QUE_CRECEN_POR_DIA.forEach((lista) => {
      expect(esBuscable('performanceSnapshots', lista)).toBe(false);
    });
  });

  it('una lista futura nace sin preparar: nada fuera de la lista es buscable (QA-13)', () => {
    expect(esBuscable('performanceSnapshots', 'listaQueAnadiraOtraHistoria')).toBe(false);
    expect(esBuscable('periods', 'assetPerformance')).toBe(false);
  });
});

describe('Escenario 2 — cada búsqueda conserva lo que necesita (RN-6.10.2-J, QA-02)', () => {
  it.each(Object.keys(GRUPOS))('las exenciones de `%s` son exactamente las de la lista, con sus órdenes', (grupo) => {
    const declaradas = Object.fromEntries(especificasDe(grupo).map((o) => [o.fieldPath, ordenesDeclarados(o)]));
    const esperadas = Object.fromEntries(Object.entries(camposDe(grupo)).map(([c, ordenes]) => [c, [...ordenes].sort()]));

    expect(declaradas).toEqual(esperadas);
  });

  it('todas las exenciones conservadas son de alcance COLLECTION, sin TTL', () => {
    Object.keys(GRUPOS).forEach((grupo) => {
      especificasDe(grupo).forEach((o) => {
        expect({ campo: o.fieldPath, ttl: o.ttl, alcances: o.indexes.map((i) => i.queryScope) }).toEqual({
          campo: o.fieldPath,
          ttl: false,
          alcances: o.indexes.map(() => 'COLLECTION'),
        });
      });
    });
  });

  it('los resúmenes solo conservan el ascendente; los consolidados, los dos órdenes de `periodKey`', () => {
    expect(GRUPOS.performanceSnapshots.campos).toEqual({
      userId: [ASC],
      accountId: [ASC],
      type: [ASC],
      lastUpdated: [ASC],
    });
    expect(GRUPOS.periods.campos).toEqual({ periodKey: [ASC, DESC] });
  });

  it('el compuesto `type + lastUpdated` de la limpieza sigue declarado', () => {
    const compuesto = canonica.indexes.find(
      (i) =>
        i.collectionGroup === 'performanceSnapshots' &&
        i.fields.map((f) => `${f.fieldPath}:${f.order}`).join(',') === 'type:ASCENDING,lastUpdated:ASCENDING',
    );

    expect(compuesto).toBeDefined();
  });

  it('la herramienta de la 6.1 distingue el comodín de cada campo', () => {
    Object.keys(GRUPOS).forEach((grupo) => {
      const claves = overrides.filter((o) => o.collectionGroup === grupo).map(normalizarFieldOverride);
      expect(new Set(claves).size).toBe(claves.length);
      expect(claves).toContain(`${grupo}|*|ttl=false|`);
    });
  });

  it('el espejo de la aplicación es copia exacta del canónico', () => {
    expect(fs.readFileSync(RUTA_ESPEJO, 'utf8')).toBe(fs.readFileSync(RUTA_CANONICA, 'utf8'));
  });
});

describe('esBuscable — campo, orden y grupo', () => {
  it('acepta los campos conservados en su orden', () => {
    expect(esBuscable('performanceSnapshots', 'userId')).toBe(true);
    expect(esBuscable('performanceSnapshots', 'lastUpdated', ASC)).toBe(true);
    expect(esBuscable('periods', 'periodKey', DESC)).toBe(true);
  });

  it('rechaza un orden que el campo no conserva', () => {
    expect(esBuscable('performanceSnapshots', 'lastUpdated', DESC)).toBe(false);
    expect(esBuscable('performanceSnapshots', 'userId', DESC)).toBe(false);
  });

  it('decide por la raíz de una ruta con punto', () => {
    expect(esBuscable('performanceSnapshots', 'timeline.0.value')).toBe(false);
    expect(esBuscable('performanceSnapshots', 'assetDays.2026-11-16')).toBe(false);
  });

  it('no confunde un campo de un grupo con el de otro', () => {
    expect(esBuscable('periods', 'userId')).toBe(false);
    expect(esBuscable('performanceSnapshots', 'periodKey')).toBe(false);
  });

  it('rechaza un grupo sin exención o una entrada que no es texto', () => {
    expect(esBuscable('dates', 'date')).toBe(false);
    expect(esBuscable('performanceSnapshots', '')).toBe(false);
    expect(esBuscable('performanceSnapshots', undefined)).toBe(false);
    expect(camposDe('toString')).toBeNull();
  });
});

describe('mensajeDeCampoNoBuscable — Escenario 3: se vuelve atrás sin perder datos (RN-6.10.2-K, QA-09)', () => {
  const mensaje = mensajeDeCampoNoBuscable('performanceSnapshots', 'assetDays', DESC);

  it('nombra el grupo, el campo y el orden pedidos', () => {
    expect(mensaje).toContain("'performanceSnapshots'");
    expect(mensaje).toContain("'assetDays'");
    expect(mensaje).toContain('orden desc');
  });

  it('dice qué sigue preparado', () => {
    expect(mensaje).toContain("'userId' (asc)");
    expect(mensajeDeCampoNoBuscable('periods', 'x')).toContain("'periodKey' (asc/desc)");
  });

  it('da el procedimiento de vuelta atrás y que no se pierden datos', () => {
    expect(mensaje).toContain(RUTA_CANONICO);
    expect(mensaje).toContain('scripts/camposBuscablesResumenes.js');
    expect(mensaje).toContain('node scripts/firestore-indexes.js deploy');
    expect(mensaje).toContain('No se pierde ningún dato');
  });

  it('desaconseja crear el índice desde la consola', () => {
    expect(mensaje).toContain('No crees el índice desde la consola de Firebase');
  });
});
