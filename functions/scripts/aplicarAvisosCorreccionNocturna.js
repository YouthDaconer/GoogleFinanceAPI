/**
 * HU 6.8.2 — Crea o actualiza en producción los avisos a operación de la
 * corrección nocturna y los del cierre nocturno de la HU 6.3 (Escenario 4).
 *
 *   node scripts/aplicarAvisosCorreccionNocturna.js            → solo muestra lo que haría
 *   node scripts/aplicarAvisosCorreccionNocturna.js --aplicar  → lo aplica
 *
 * Idempotente: si una política ya existe, la actualiza; nunca la duplica. Usa
 * las credenciales de `gcloud` de quien lo ejecuta (necesita permisos de
 * Monitoring sobre el proyecto). No hay entorno de pruebas en GCP: `--aplicar`
 * escribe en producción.
 *
 * La definición vive en `scripts/correccionNocturnaAlerts.js`, con tests; esto
 * es solo el borde.
 *
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T9)
 */

const { tokenDeAcceso, llamar } = require('./gcpMonitoringApi');
const {
  PROYECTO,
  CORREO_OPERACION,
  AVISOS,
  definicionPolitica,
  politicaExistente,
  elegirCanal,
} = require('./correccionNocturnaAlerts');

const MONITORING = `https://monitoring.googleapis.com/v3/projects/${PROYECTO}`;

function printUsage() {
  console.log(`
Crea o actualiza los avisos a operación de la corrección nocturna (HU 6.8.2)
y del cierre nocturno (HU 6.3).

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

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return 2;
  }

  const token = tokenDeAcceso();

  // El canal se elige por el buzón, no por el nombre (HU 7)
  const canales = await llamar(token, 'GET', `${MONITORING}/notificationChannels`);
  const canal = elegirCanal(canales.json.notificationChannels);
  if (!canal) throw new Error(`Ningún canal de correo activo de ${PROYECTO} envía a ${CORREO_OPERACION}`);
  console.log(`Canal de operación: ${canal} (${CORREO_OPERACION})`);

  const politicas = await llamar(token, 'GET', `${MONITORING}/alertPolicies`);
  const planes = Object.keys(AVISOS).map((clave) => ({
    clave,
    existente: politicaExistente(politicas.json.alertPolicies, clave),
    cuerpo: definicionPolitica(clave, canal),
  }));
  for (const plan of planes) {
    console.log(`Política «${plan.cuerpo.displayName}»: ${plan.existente ? 'actualizar' : 'crear'}`);
    console.log(`  filtro: ${plan.cuerpo.conditions[0].conditionMatchedLog.filter}`);
  }

  if (!args.aplicar) {
    console.log('');
    console.log('Simulación: no se escribió nada. Repite con --aplicar para aplicarlo.');
    return 0;
  }

  for (const plan of planes) {
    if (plan.existente) {
      await llamar(token, 'PATCH', `https://monitoring.googleapis.com/v3/${plan.existente.name}`, plan.cuerpo);
    } else {
      await llamar(token, 'POST', `${MONITORING}/alertPolicies`, plan.cuerpo);
    }
    console.log(`✓ Política «${plan.cuerpo.displayName}»`);
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
