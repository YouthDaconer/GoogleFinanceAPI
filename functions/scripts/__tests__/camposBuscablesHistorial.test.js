/**
 * HU 6.2 — Qué del historial se puede buscar, y el aviso cuando no.
 *
 * @see platform-docs/stories/6.2-dejar-pagar-busqueda-sin-uso/historia.md
 * @see platform-docs/stories/6.2-dejar-pagar-busqueda-sin-uso/refinamiento.md (T7)
 */

const fs = require('fs');
const path = require('path');

const {
  GRUPO_EXENTO,
  CAMPOS_BUSCABLES,
  CIFRAS_EXCLUIDAS,
  CIFRAS_VIGILADAS,
  RUTA_CANONICO,
  esBuscable,
  mensajeDeCampoNoBuscable,
  exigirCampoBuscable,
} = require('../camposBuscablesHistorial');

const { normalizarFieldOverride } = require('../firestoreIndexInventory');

describe('esBuscable — Escenario 4: las cifras dejan de estar preparadas para buscar', () => {
  it('deja buscable `date`, que es la única búsqueda que usa el historial (RN-6.2-K)', () => {
    expect(esBuscable('date')).toBe(true);
    expect(esBuscable('__name__')).toBe(true);
  });

  it('no deja buscable una métrica dentro de un mapa de moneda', () => {
    expect(esBuscable('USD.totalValue')).toBe(false);
    expect(esBuscable('COP.adjustedDailyChangePercentage')).toBe(false);
  });

  it('no deja buscable una métrica del detalle por activo', () => {
    expect(esBuscable('USD.assetPerformance.AAPL_stock.totalROI')).toBe(false);
  });

  it('no deja buscable ninguna de las cifras declaradas', () => {
    CIFRAS_EXCLUIDAS.forEach((cifra) => {
      expect(esBuscable(`USD.${cifra}`)).toBe(false);
    });
  });

  it('rechaza una entrada vacía o que no sea texto', () => {
    expect(esBuscable('')).toBe(false);
    expect(esBuscable(undefined)).toBe(false);
    expect(esBuscable(null)).toBe(false);
  });
});

describe('mensajeDeCampoNoBuscable — RN-6.2-M: error explícito, no resultado vacío', () => {
  const mensaje = mensajeDeCampoNoBuscable('USD.totalROI');

  it('nombra el campo por el que se intentó buscar', () => {
    expect(mensaje).toContain("'USD.totalROI'");
  });

  it('dice dónde se habilita', () => {
    expect(mensaje).toContain(RUTA_CANONICO);
    expect(mensaje).toContain(`'${GRUPO_EXENTO}'`);
    expect(mensaje).toContain('node scripts/firestore-indexes.js deploy');
  });

  it('desaconseja el camino que ofrece Firestore, que devolvería el gasto', () => {
    expect(mensaje).toContain('No crees un índice desde la consola de Firebase');
  });

  it('`exigirCampoBuscable` lanza con ese mensaje y devuelve el campo si vale', () => {
    expect(() => exigirCampoBuscable('USD.totalROI')).toThrow('no está habilitado para buscar');
    expect(exigirCampoBuscable('date')).toBe('date');
  });
});

describe('CIFRAS_VIGILADAS — qué se escanea y qué no', () => {
  it('vigila todas las cifras excluidas salvo `units`', () => {
    expect(CIFRAS_VIGILADAS).toEqual(CIFRAS_EXCLUIDAS.filter((c) => c !== 'units'));
    expect(CIFRAS_VIGILADAS).not.toContain('units');
  });

  it('deja `units` fuera del escaneo pero no de la exclusión', () => {
    // `units` también es un campo de `assets`, donde sí es legítimo consultarlo:
    // escanearlo por nombre daría falsos positivos.
    expect(esBuscable('USD.units')).toBe(false);
  });

  it('vigila las métricas que de verdad están en el historial', () => {
    ['totalValue', 'totalROI', 'adjustedDailyChangePercentage', 'assetPerformance'].forEach((c) => {
      expect(CIFRAS_VIGILADAS).toContain(c);
    });
  });
});

describe('Escenario 1 y 5 — la exención declarada en el canónico', () => {
  const RUTA = path.resolve(__dirname, '../../../firestore.indexes.json');
  const canonica = JSON.parse(fs.readFileSync(RUTA, 'utf8'));

  const overrides = canonica.fieldOverrides || [];
  const comodin = overrides.find((o) => o.collectionGroup === GRUPO_EXENTO && o.fieldPath === '*');
  const porFecha = overrides.find((o) => o.collectionGroup === GRUPO_EXENTO && o.fieldPath === 'date');

  it('declara la exención comodín del grupo `dates` con cero índices', () => {
    expect(comodin).toBeDefined();
    expect(comodin.indexes).toEqual([]);
  });

  it('conserva `date` indexado, que es lo que mantiene vivo el historial (RN-6.2-K)', () => {
    expect(porFecha).toBeDefined();
    expect(porFecha.indexes.length).toBeGreaterThan(0);
    expect(porFecha.indexes.map((i) => i.order)).toContain('ASCENDING');
    expect(porFecha.indexes.map((i) => i.order)).toContain('DESCENDING');
  });

  it('la herramienta de la 6.1 sabe comparar el comodín sin confundirlo con `date`', () => {
    expect(normalizarFieldOverride(comodin)).not.toBe(normalizarFieldOverride(porFecha));
    expect(normalizarFieldOverride(comodin)).toBe(`${GRUPO_EXENTO}|*|ttl=false|`);
  });

  it('no declara ninguna otra exención del grupo `dates` que la contradiga', () => {
    const delGrupo = overrides.filter((o) => o.collectionGroup === GRUPO_EXENTO);

    expect(delGrupo.map((o) => o.fieldPath).sort()).toEqual(['*', 'date']);
  });
});
