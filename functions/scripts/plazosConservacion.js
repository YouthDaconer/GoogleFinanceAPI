/**
 * HU 6.10.1 — Operación ensaya, activa y revisa la limpieza de datos vencidos.
 *
 *   node scripts/plazosConservacion.js --ensayo                      → cuenta todo, no retira nada
 *   node scripts/plazosConservacion.js --activar <tipo>              → solo muestra lo que haría
 *   node scripts/plazosConservacion.js --activar <tipo> --aplicar    → activa la retirada del tipo
 *   node scripts/plazosConservacion.js --informe [--ultimas N]       → constancias y espacio liberado
 *
 * Orden de la primera vez (Escenario 4, RN-6.10-O):
 * 1. `--ensayo`;
 * 2. revisar el conteo por tipo y mes con `--informe`;
 * 3. `--activar <tipo> --aplicar`, tipo por tipo.
 * No se puede activar un tipo sin un ensayo completo. La limpieza diaria de
 * las 06:00 ET retira solo los tipos activados; los demás los sigue contando.
 *
 * `--ensayo` no retira nada, pero sí escribe su constancia
 * (`dataRetentionRuns`) y el último ensayo de cada tipo (`dataRetentionPolicies/config`).
 * Tampoco arranca entre las 00:00 y las 03:00 ET (RN-6.10.1-J).
 * No hay entorno de pruebas: todo esto actúa sobre producción.
 *
 * Usa las credenciales de `gcloud` para medir el almacenamiento (RN-6-I).
 * La lógica vive en `services/retention/retentionService.js`, con tests; esto
 * es solo el borde.
 *
 * @see platform-docs/stories/6.10.1-plazos-conservacion-datos/refinamiento.md (T4)
 */

const { execSync } = require('child_process');
const admin = require('../services/firebaseAdmin');
const { TIPOS } = require('../services/retention/retentionPolicies');
const { ejecutarLimpieza, activarTipo, leerInforme } = require('../services/retention/retentionService');
const { crearMedidorDeEspacio } = require('../services/retention/retentionScheduled');
const { tokenDeAcceso } = require('./gcpMonitoringApi');

const BYTES_POR_MIB = 1024 * 1024;

function printUsage() {
  console.log(`
Plazos de conservación de los datos operativos (HU 6.10.1).

  node scripts/plazosConservacion.js --ensayo
  node scripts/plazosConservacion.js --activar <tipo> [--aplicar]
  node scripts/plazosConservacion.js --informe [--ultimas N]

  --ensayo           Cuenta, por tipo y mes, lo que se retiraría. No retira nada.
  --activar <tipo>   Activa la retirada de un tipo ya ensayado. Sin --aplicar solo lo simula.
  --informe          Últimas constancias, tipos activados y espacio liberado.
  --help             Esta ayuda.

  Tipos: ${TIPOS.map((t) => t.id).join(', ')}
`);
}

function parseArgs(argv) {
  const args = { accion: null, tipo: null, aplicar: false, ultimas: 10, help: false };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--ensayo') args.accion = 'ensayo';
    else if (arg === '--informe') args.accion = 'informe';
    else if (arg === '--activar') {
      args.accion = 'activar';
      args.tipo = argv[++i] || null;
    } else if (arg === '--aplicar') args.aplicar = true;
    else if (arg === '--ultimas') args.ultimas = Number(argv[++i]) || 10;
    else {
      console.error(`Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }
  if (!args.accion || (args.accion === 'activar' && !args.tipo)) args.help = true;
  return args;
}

/** Quién ejecuta el script, para la constancia de la activación. */
function operador() {
  try {
    return execSync('gcloud config get-value account', { encoding: 'utf8' }).trim() || 'operacion';
  } catch (error) {
    return 'operacion';
  }
}

function mib(bytes) {
  return typeof bytes === 'number' ? `${(bytes / BYTES_POR_MIB).toFixed(1)} MiB` : '—';
}

/** La diferencia con la medición anterior: liberado si bajó, creció si subió. */
function espacioFrenteAnterior(liberadoBytes) {
  if (typeof liberadoBytes !== 'number') return '';
  return liberadoBytes >= 0
    ? `, ${mib(liberadoBytes)} menos que la medición anterior`
    : `, ${mib(-liberadoBytes)} más que la medición anterior`;
}

function imprimirTipos(tipos) {
  for (const tipo of TIPOS) {
    const t = tipos[tipo.id];
    if (!t) {
      console.log(`  ${tipo.id.padEnd(20)} (no se llegó a procesar)`);
      continue;
    }
    if (t.error) {
      console.log(`  ${tipo.id.padEnd(20)} ERROR: ${t.error}`);
      continue;
    }
    const tramos = Object.entries(t.porTramo || {})
      .sort(([a], [b]) => (a < b ? -1 : 1))
      .map(([mes, n]) => `${mes}: ${n}`)
      .join(' · ') || 'nada';
    const verbo = t.modo === 'retirada' ? 'retirados' : 'vencidos (ensayo)';
    console.log(`  ${tipo.id.padEnd(20)} corte ${t.corte}: ${t.registros} ${verbo}` +
      `${t.completo ? '' : ` — sin terminar (${t.detenidoPor || 'interrumpido'})`}`);
    console.log(`  ${''.padEnd(20)} ${tramos}`);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return 2;
  }
  const db = admin.firestore();

  if (args.accion === 'ensayo') {
    const constancia = await ejecutarLimpieza(db, {
      soloEnsayo: true,
      origen: 'ensayo-manual',
      medirEspacio: crearMedidorDeEspacio({ obtenerToken: async () => tokenDeAcceso() }),
    });
    console.log(`\nEnsayo ${constancia.id}: ${constancia.estado}. Almacenamiento: ${mib(constancia.almacenamientoBytes)}`);
    imprimirTipos(constancia.tipos);
    console.log('\nRevisa el conteo y activa cada tipo con --activar <tipo> --aplicar.');
    return 0;
  }

  if (args.accion === 'activar') {
    const r = await activarTipo(db, args.tipo, { aplicar: args.aplicar, quien: operador() });
    const mensajes = {
      'activado': () => `✓ ${args.tipo} activado. La limpieza de las 06:00 ET lo retirará (ensayo revisado: ${r.ensayo.runId}).`,
      'simulado': () => `Se activaría ${args.tipo}. Su último ensayo (${r.ensayo.runId}, corte ${r.ensayo.corte}) ` +
        `cuenta ${r.ensayo.registros} registros vencidos. Repite con --aplicar para activarlo.`,
      'ya-activo': () => `${args.tipo} ya estaba activo desde ${r.activadoAt}. No se hizo nada.`,
      'sin-ensayo': () => `${args.tipo} no tiene un ensayo completo. Lanza --ensayo y revísalo primero.`,
      'tipo-desconocido': () => `Tipo desconocido: ${args.tipo}. Tipos: ${TIPOS.map((t) => t.id).join(', ')}`,
    };
    console.log(`[plazosConservacion] ${mensajes[r.estado]()}`);
    return r.estado === 'tipo-desconocido' || r.estado === 'sin-ensayo' ? 1 : 0;
  }

  const { config, constancias } = await leerInforme(db, { limite: args.ultimas });
  console.log('\nTipos activados:');
  for (const tipo of TIPOS) {
    const t = config.tipos[tipo.id];
    console.log(`  ${tipo.id.padEnd(20)} ${t && t.activo ? `activo desde ${t.activadoAt} (${t.activadoPor})` : 'solo ensayo'}`);
  }
  for (const c of constancias) {
    console.log(`\n${c.id} — ${c.origen}${c.soloEnsayo ? ' (ensayo)' : ''}, ${c.estado}. ` +
      `Almacenamiento al empezar: ${mib(c.almacenamientoBytes)}` +
      `${espacioFrenteAnterior(c.liberadoBytes)}`);
    imprimirTipos(c.tipos || {});
  }
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
