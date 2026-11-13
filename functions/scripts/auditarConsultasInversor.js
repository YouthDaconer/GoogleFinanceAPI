/**
 * HU 7 — Comprobación repetible: ¿alguna consulta de la function `app`
 * responde datos de un inversor sin exigir sesión?
 *
 * Tres usos (Escenarios 8 y 9, RN-7-F):
 *
 *   node scripts/auditarConsultasInversor.js
 *     Inventario de las rutas registradas en el código: abiertas por decisión,
 *     protegidas y abiertas sin decisión (debe dar 0).
 *
 *   node scripts/auditarConsultasInversor.js --base-url https://app-dt7wehvfba-uc.a.run.app
 *     Además, sondea en vivo cada ruta protegida SIN credenciales y exige
 *     401 «No autorizado». Cada sonda queda en el registro de rechazos como un
 *     intento desde tu IP (5 sondas < umbral de 10 del aviso).
 *
 *   node scripts/auditarConsultasInversor.js --base-url <url> --user-id <uid>
 *     Además, repite la petición exacta del 2026-11-12 (disponibilidad de la
 *     atribución sin credenciales con el uid de un inversor real) y exige el
 *     mismo rechazo. Es el Escenario 8 / QA-14.
 *
 * Sale con código 1 si algo queda abierto. La lógica vive en
 * `scripts/investorRouteAudit.js`, con tests; esto es solo el borde.
 *
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T5)
 */

const {
  listarRutas,
  auditarRutas,
  planDeSondas,
  evaluarSonda,
  formatearInforme,
} = require('./investorRouteAudit');

// ============================================================================
// USAGE
// ============================================================================

function printUsage() {
  console.log(`
Comprueba que ninguna consulta de la function app devuelva datos de un
inversor sin exigir sesión (HU 7).

  --base-url <url>   Sondea en vivo las rutas protegidas sin credenciales.
                     Producción: https://app-dt7wehvfba-uc.a.run.app
  --user-id <uid>    Con --base-url: repite la petición del 2026-11-12
                     (GET /attribution/check?userId=<uid> sin credenciales).
  --help             Esta ayuda.
`);
}

function parseArgs(argv) {
  const args = { baseUrl: null, userId: null, help: false };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--base-url') args.baseUrl = (argv[++i] || '').replace(/\/+$/, '') || null;
    else if (arg === '--user-id') args.userId = argv[++i] || null;
    else {
      console.error(`Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }
  if (args.userId && !args.baseUrl) {
    console.error('--user-id necesita --base-url');
    args.help = true;
  }
  return args;
}

// ============================================================================
// SONDA EN VIVO
// ============================================================================

/**
 * Hace una petición sin credenciales y evalúa la respuesta.
 *
 * @param {string} baseUrl
 * @param {{method: string, url: string}} sonda
 * @returns {Promise<{ok: boolean, detalle: string}>}
 */
async function sondear(baseUrl, sonda) {
  const respuesta = await fetch(`${baseUrl}${sonda.url}`, {
    method: sonda.method,
    signal: AbortSignal.timeout(30000),
  });
  let cuerpo = null;
  try {
    cuerpo = await respuesta.json();
  } catch (error) {
    cuerpo = null;
  }
  return evaluarSonda(respuesta.status, cuerpo);
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return 2;
  }

  // Se carga la app real: el inventario es el de las rutas que de verdad registra.
  const app = require('../httpApi');
  const { rutas, indiceGuardia } = listarRutas(app._router && app._router.stack);
  const resultado = auditarRutas({ rutas, indiceGuardia });

  console.log(`Auditoría de consultas con datos de inversores — ${new Date().toISOString()}`);
  console.log('');
  console.log(formatearInforme(resultado));
  let fallos = resultado.enCero ? 0 : 1;

  if (args.baseUrl) {
    console.log('');
    console.log(`Sonda en vivo sin credenciales contra ${args.baseUrl}:`);
    const sondas = planDeSondas(resultado.protegidas);
    if (args.userId) {
      sondas.push({ method: 'GET', ruta: '/attribution/check (petición del 2026-11-12)', url: `/attribution/check?${new URLSearchParams({ userId: args.userId })}` });
    }
    for (const sonda of sondas) {
      try {
        const { ok, detalle } = await sondear(args.baseUrl, sonda);
        console.log(`  ${ok ? '✓' : '✗'} ${sonda.method.padEnd(5)} ${sonda.ruta} — ${detalle}`);
        if (!ok) fallos += 1;
      } catch (error) {
        console.log(`  ✗ ${sonda.method.padEnd(5)} ${sonda.ruta} — sin respuesta: ${error.message}`);
        fallos += 1;
      }
    }
  }

  console.log('');
  console.log(fallos === 0 ? 'OK: ninguna consulta con datos de inversores responde sin sesión.' : `FALLO: ${fallos} comprobación(es) no pasaron.`);
  return fallos === 0 ? 0 : 1;
}

(async () => {
  try {
    process.exit(await main());
  } catch (error) {
    console.error('');
    console.error('ERROR:', error.message);
    process.exit(1);
  }
})();
