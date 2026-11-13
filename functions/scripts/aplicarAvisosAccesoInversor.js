/**
 * HU 7 — Crea o actualiza en producción el aviso a operación por intentos
 * repetidos de leer cifras ajenas (Escenario 6).
 *
 *   node scripts/aplicarAvisosAccesoInversor.js            → solo muestra lo que haría
 *   node scripts/aplicarAvisosAccesoInversor.js --aplicar  → lo aplica
 *
 * Idempotente: si la métrica o las políticas ya existen, las actualiza; nunca
 * las duplica. Usa las credenciales de `gcloud` de quien lo ejecuta (necesita
 * permisos de Logging y Monitoring sobre el proyecto). No hay entorno de
 * pruebas en GCP: `--aplicar` escribe en producción.
 *
 * La definición vive en `scripts/investorAccessAlerts.js`, con tests; esto es
 * solo el borde.
 *
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T6)
 */

const { tokenDeAcceso, llamar, esperar } = require('./gcpMonitoringApi');
const {
  PROYECTO,
  NOMBRE_METRICA,
  CORREO_OPERACION,
  DIMENSIONES,
  definicionMetrica,
  definicionPolitica,
  elegirCanal,
  politicaExistente,
} = require('./investorAccessAlerts');

const LOGGING = `https://logging.googleapis.com/v2/projects/${PROYECTO}`;
const MONITORING = `https://monitoring.googleapis.com/v3/projects/${PROYECTO}`;

/** Una métrica recién creada tarda en ser visible para Monitoring. */
const REINTENTOS_POLITICA = 6;
const ESPERA_REINTENTO_MS = 10000;

// ============================================================================
// USAGE
// ============================================================================

function printUsage() {
  console.log(`
Crea o actualiza la métrica y las dos políticas del aviso de la HU 7.

  --aplicar   Escribe en producción. Sin esto, solo muestra lo que haría.
  --help      Esta ayuda.
`);
}

function parseArgs(argv) {
  const args = { aplicar: false, help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--aplicar') args.aplicar = true;
    else {
      console.error(`Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }
  return args;
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

  const token = tokenDeAcceso();
  const metrica = definicionMetrica();

  // 1. Métrica basada en logs
  const actual = await llamar(token, 'GET', `${LOGGING}/metrics/${NOMBRE_METRICA}`);
  const accionMetrica = actual.status === 404 ? 'crear' : 'actualizar';
  console.log(`Métrica ${NOMBRE_METRICA}: ${accionMetrica}`);
  console.log(`  filtro: ${metrica.filter}`);

  // 2. Canal de operación
  const canales = await llamar(token, 'GET', `${MONITORING}/notificationChannels`);
  const canal = elegirCanal(canales.json.notificationChannels);
  if (!canal) throw new Error(`Ningún canal de correo activo de ${PROYECTO} envía a ${CORREO_OPERACION}`);
  console.log(`Canal de operación: ${canal}`);

  // 3. Políticas
  const politicas = await llamar(token, 'GET', `${MONITORING}/alertPolicies`);
  const planes = Object.keys(DIMENSIONES).map((clave) => {
    const existente = politicaExistente(politicas.json.alertPolicies, clave);
    return { clave, existente, cuerpo: definicionPolitica(clave, canal) };
  });
  for (const plan of planes) {
    console.log(`Política «${plan.cuerpo.displayName}»: ${plan.existente ? 'actualizar' : 'crear'}`);
  }

  if (!args.aplicar) {
    console.log('');
    console.log('Simulación: no se escribió nada. Repite con --aplicar para aplicarlo.');
    return 0;
  }

  if (accionMetrica === 'crear') {
    await llamar(token, 'POST', `${LOGGING}/metrics`, metrica);
  } else {
    await llamar(token, 'PUT', `${LOGGING}/metrics/${NOMBRE_METRICA}`, metrica);
  }
  console.log(`✓ Métrica ${NOMBRE_METRICA}`);

  for (const plan of planes) {
    for (let intento = 1; ; intento++) {
      try {
        if (plan.existente) {
          await llamar(token, 'PATCH', `https://monitoring.googleapis.com/v3/${plan.existente.name}`, plan.cuerpo);
        } else {
          await llamar(token, 'POST', `${MONITORING}/alertPolicies`, plan.cuerpo);
        }
        console.log(`✓ Política «${plan.cuerpo.displayName}»`);
        break;
      } catch (error) {
        if (intento >= REINTENTOS_POLITICA) throw error;
        console.log(`  … la métrica aún no es visible para Monitoring (intento ${intento}): ${error.message}`);
        await esperar(ESPERA_REINTENTO_MS);
      }
    }
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
