/**
 * HU 7 — Qué rutas de la function `app` responden sin sesión, y si alguna
 * lo hace sin una decisión registrada.
 *
 * **Por qué existe** (Escenario 9, RN-7-F). Que una consulta con datos de un
 * inversor quede abierta no lo detectan ni los tests de esa consulta ni el lint:
 * así quedaron abiertas las cinco que motivaron la HU 7. Esta comprobación
 * recorre las rutas **realmente registradas** en la app de Express y las
 * clasifica:
 *
 * - **abiertas por decisión**: responden sin sesión y tienen su decisión en
 *   `config/publicHttpRoutes.js`;
 * - **protegidas**: pasan por el guardia de sesión;
 * - **abiertas sin decisión**: lo que **debe dar cero**. Son las registradas
 *   antes del guardia (quedan fuera de él) o todas, si el guardia no está.
 *
 * Además avisa de entradas de la lista sin decisión completa (el guardia no
 * las abre) y de entradas que ya no corresponden a ninguna ruta.
 *
 * Este módulo **no lee ficheros, no toca la red y no imprime**: recibe la pila
 * del router y devuelve el veredicto. El borde es `scripts/auditarConsultasInversor.js`.
 *
 * @module scripts/investorRouteAudit
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T5)
 */

const {
  RUTAS_ABIERTAS,
  buscarRutaAbierta,
  tieneDecision,
  coincideRuta,
} = require('../config/publicHttpRoutes');
const { MENSAJE_RECHAZO } = require('../utils/investorSession');

/** Nombre de la función con que `httpApi.js` engancha el guardia. Es lo que se busca en la pila. */
const NOMBRE_GUARDIA = 'guardiaConsultasInversor';

/** `userId` con el que se sondean las rutas en vivo: no es de nadie. */
const USER_ID_SONDA = 'sonda-auditoria-hu7';

/**
 * Lista las rutas de la pila de Express 4 y la posición del guardia.
 *
 * @param {Array<object>} pila - `app._router.stack`
 * @returns {{ rutas: Array<{method: string, path: string, indice: number}>, indiceGuardia: number }}
 */
function listarRutas(pila) {
  const rutas = [];
  let indiceGuardia = -1;
  (pila || []).forEach((capa, indice) => {
    if (!capa.route && capa.name === NOMBRE_GUARDIA && indiceGuardia === -1) indiceGuardia = indice;
    if (!capa.route) return;
    const metodos = Object.keys(capa.route.methods || {}).filter((m) => m !== '_all');
    for (const metodo of metodos) {
      rutas.push({ method: metodo.toUpperCase(), path: capa.route.path, indice });
    }
  });
  return { rutas, indiceGuardia };
}

/**
 * Clasifica las rutas.
 *
 * @param {object} params
 * @param {Array<{method: string, path: string, indice: number}>} params.rutas
 * @param {number} params.indiceGuardia - `-1` si el guardia no está.
 * @param {ReadonlyArray<object>} [params.rutasAbiertas] - La lista de decisiones.
 * @returns {{
 *   abiertasPorDecision: object[],
 *   protegidas: object[],
 *   abiertasSinDecision: object[],
 *   entradasIncompletas: object[],
 *   entradasHuerfanas: object[],
 *   enCero: boolean,
 * }}
 */
function auditarRutas({ rutas, indiceGuardia, rutasAbiertas = RUTAS_ABIERTAS }) {
  const abiertasPorDecision = [];
  const protegidas = [];
  const abiertasSinDecision = [];

  for (const ruta of rutas) {
    const entrada = buscarRutaAbierta(ruta.method, ruta.path, rutasAbiertas);
    const conDecision = Boolean(entrada && tieneDecision(entrada));
    const fueraDelGuardia = indiceGuardia === -1 || ruta.indice < indiceGuardia;

    if (conDecision) {
      abiertasPorDecision.push({ ...ruta, motivo: entrada.motivo, decididoPor: entrada.decididoPor, fecha: entrada.fecha });
    } else if (fueraDelGuardia) {
      abiertasSinDecision.push({
        ...ruta,
        causa: indiceGuardia === -1 ? 'no hay guardia de sesión en la app' : 'registrada antes del guardia de sesión',
      });
    } else {
      protegidas.push(ruta);
    }
  }

  const entradasIncompletas = rutasAbiertas.filter((e) => !tieneDecision(e));
  const entradasHuerfanas = rutasAbiertas.filter(
    (e) => !rutas.some((r) => r.method === e.method && coincideRuta(e.path, r.path)),
  );

  return {
    abiertasPorDecision,
    protegidas,
    abiertasSinDecision,
    entradasIncompletas,
    entradasHuerfanas,
    enCero: abiertasSinDecision.length === 0 && entradasIncompletas.length === 0,
  };
}

/**
 * Las peticiones de la sonda en vivo: una por ruta protegida, **sin
 * credenciales** y con un `userId` que no es de nadie. Los parámetros de ruta
 * (`:id`) se rellenan con la sonda.
 *
 * @param {object[]} protegidas
 * @param {string} [userIdSonda]
 * @returns {Array<{method: string, ruta: string, url: string}>}
 */
function planDeSondas(protegidas, userIdSonda = USER_ID_SONDA) {
  return protegidas.map((r) => {
    const ruta = r.path.split('/').map((s) => (s.startsWith(':') ? userIdSonda : s)).join('/');
    const query = new URLSearchParams({ userId: userIdSonda }).toString();
    return { method: r.method, ruta: r.path, url: `${ruta}?${query}` };
  });
}

/**
 * ¿La respuesta de una sonda sin credenciales es el rechazo esperado?
 *
 * @param {number} status
 * @param {object|null} cuerpo - El JSON de la respuesta, o `null` si no lo era.
 * @returns {{ ok: boolean, detalle: string }}
 */
function evaluarSonda(status, cuerpo) {
  const mensaje = cuerpo && typeof cuerpo === 'object' ? cuerpo.message : undefined;
  if (status === 401 && mensaje === MENSAJE_RECHAZO) return { ok: true, detalle: '401 No autorizado' };
  return { ok: false, detalle: `respondió ${status}${mensaje ? ` («${mensaje}»)` : ''} en lugar de 401 «${MENSAJE_RECHAZO}»` };
}

/**
 * El informe en texto, listo para imprimir.
 *
 * @param {ReturnType<typeof auditarRutas>} resultado
 * @returns {string}
 */
function formatearInforme(resultado) {
  const lineas = [];
  const ruta = (r) => `${r.method.padEnd(5)} ${r.path}`;

  lineas.push(`Abiertas sin decisión: ${resultado.abiertasSinDecision.length}${resultado.abiertasSinDecision.length ? '  ✗' : '  ✓'}`);
  for (const r of resultado.abiertasSinDecision) lineas.push(`  ✗ ${ruta(r)} — ${r.causa}`);

  lineas.push(`Protegidas (sesión del dueño o sistema): ${resultado.protegidas.length}`);
  for (const r of resultado.protegidas) lineas.push(`  · ${ruta(r)}`);

  lineas.push(`Abiertas por decisión: ${resultado.abiertasPorDecision.length}`);
  for (const r of resultado.abiertasPorDecision) {
    lineas.push(`  · ${ruta(r)} — ${r.motivo} (${r.decididoPor}, ${r.fecha})`);
  }

  if (resultado.entradasIncompletas.length) {
    lineas.push(`Entradas de la lista sin decisión completa (no se abren): ${resultado.entradasIncompletas.length}  ✗`);
    for (const e of resultado.entradasIncompletas) lineas.push(`  ✗ ${ruta(e)}`);
  }
  if (resultado.entradasHuerfanas.length) {
    lineas.push(`Entradas de la lista sin ruta que las use: ${resultado.entradasHuerfanas.length}`);
    for (const e of resultado.entradasHuerfanas) lineas.push(`  ! ${ruta(e)}`);
  }

  lineas.push(resultado.enCero
    ? 'Resultado: 0 consultas abiertas sin decisión ✓'
    : 'Resultado: HAY consultas abiertas sin decisión ✗');
  return lineas.join('\n');
}

module.exports = {
  NOMBRE_GUARDIA,
  USER_ID_SONDA,
  listarRutas,
  auditarRutas,
  planDeSondas,
  evaluarSonda,
  formatearInforme,
};
