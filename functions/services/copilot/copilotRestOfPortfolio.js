/**
 * HU 5.3 — "Resto del portafolio", en conjunto y sin investigar.
 *
 * Los activos que no entraron entre los relevantes no se investigan uno a uno
 * (RN-5-L): se agrupan por sector, por tipo y por divisa con datos que el
 * backend ya tiene. No hace ninguna llamada: agrupar no necesita internet, y
 * cada llamada que no se hace es cuota gratuita que queda para lo que sí la
 * necesita.
 *
 * @module services/copilot/copilotRestOfPortfolio
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T3)
 */

/** Cuántos grupos se le pasan al modelo por cada criterio. */
const MAX_GRUPOS = 8;

/**
 * Agrupa y ordena por peso descendente.
 *
 * @param {object[]} holdings
 * @param {function(object): string} clave
 * @returns {Array<{nombre: string, peso: number, activos: number, tickers: string[]}>}
 */
function agrupar(holdings, clave) {
  const grupos = new Map();

  for (const h of holdings) {
    const nombre = clave(h) || 'Otros';
    const grupo = grupos.get(nombre) || { nombre, peso: 0, activos: 0, tickers: [] };
    grupo.peso += h.weight * 100;
    grupo.activos += 1;
    grupo.tickers.push(h.symbol);
    grupos.set(nombre, grupo);
  }

  return [...grupos.values()]
    .map((g) => ({ ...g, peso: Math.round(g.peso * 10) / 10 }))
    .sort((a, b) => b.peso - a.peso)
    .slice(0, MAX_GRUPOS);
}

/**
 * El retrato del resto del portafolio.
 *
 * @param {object[]} resto - Los no relevantes.
 * @returns {object|null} `null` si no hay resto: entonces el apartado no
 *   aparece (Escenario 3).
 */
function resumirResto(resto) {
  if (!resto || resto.length === 0) return null;

  const pesoTotal = resto.reduce((suma, h) => suma + h.weight * 100, 0);

  return {
    activos: resto.length,
    pesoTotal: Math.round(pesoTotal * 10) / 10,
    porSector: agrupar(resto, (h) => h.sector),
    porTipo: agrupar(resto, (h) => h.asset_type),
    porDivisa: agrupar(resto, (h) => h.currency),
  };
}

module.exports = { MAX_GRUPOS, agrupar, resumirResto };
