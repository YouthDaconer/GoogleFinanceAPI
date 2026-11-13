/**
 * HU 6.8.2 — Operación libera a un inversor en revisión y su corrección vuelve
 * a la cola (Escenario 2, disparador relanzable).
 *
 *   node scripts/liberarRevisionCorreccion.js <uid>            → solo muestra lo que haría
 *   node scripts/liberarRevisionCorreccion.js <uid> --aplicar  → lo aplica
 *
 * Al liberarlo, su cuenta de intentos vuelve a 0 y conserva su espera
 * original. Liberarlo dos veces no hace nada la segunda: vuelve a la cola una
 * sola vez. No hay entorno de pruebas: `--aplicar` escribe en producción.
 *
 * La lógica vive en `services/reconcileStalePerformance.js#liberarRevision`,
 * con tests; esto es solo el borde.
 *
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T8)
 */

require('../services/firebaseAdmin');
const { liberarRevision } = require('../services/reconcileStalePerformance');

function printUsage() {
  console.log(`
Libera a un inversor en revisión de la corrección nocturna.

  node scripts/liberarRevisionCorreccion.js <uid> [--aplicar]

  --aplicar   Escribe en producción. Sin esto, solo muestra lo que haría.
  --help      Esta ayuda.
`);
}

function parseArgs(argv) {
  const args = { uid: null, aplicar: false, help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--aplicar') args.aplicar = true;
    else if (!arg.startsWith('-') && !args.uid) args.uid = arg;
    else {
      console.error(`Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }
  if (!args.uid) args.help = true;
  return args;
}

const MENSAJES = {
  'liberado': (r) => `✓ Liberado: vuelve a la cola desde ${r.since}, esperando desde ${r.pendienteDesde}, sin intentos.`,
  'simulado': (r) => `Volvería a la cola desde ${r.since}, esperando desde ${r.pendienteDesde}, sin intentos. ` +
    'Repite con --aplicar para aplicarlo.',
  'ya-liberado': (r) => `Ya estaba liberado (${r.resolvedAt}). No se hizo nada.`,
  'sin-revision': () => 'Este inversor no está en revisión. No se hizo nada.',
  'sin-rendimiento': () => 'El inversor no tiene documento de rendimiento. No se hizo nada.',
};

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return 2;
  }
  const resultado = await liberarRevision(args.uid, { aplicar: args.aplicar });
  console.log(`[liberarRevision] ${args.uid}: ${MENSAJES[resultado.estado](resultado)}`);
  return 0;
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
