/**
 * HU 6.10.2 — Escenario 4: el ahorro se mide con la medición de la plataforma.
 * **Solo lee.**
 *
 *   node scripts/medirEspacioBusqueda.js medir --etiqueta antes     → justo antes de publicar
 *   node scripts/medirEspacioBusqueda.js medir --etiqueta despues   → 2 días después
 *   node scripts/medirEspacioBusqueda.js comparar
 *
 * El espacio sale de la misma métrica que vigila el aviso de consumo de la 6.7
 * (`storage/data_and_index_storage_bytes`, vía `medirAlmacenamiento`): es la
 * medición de la plataforma, no una estimación (RN-6-I).
 *
 * **Firestore actualiza esa métrica una vez al día**, y Monitoring repite el
 * valor minuto a minuto hasta la siguiente. Por eso cada medición guarda también
 * desde cuándo está vigente su valor: si el de `despues` no entró en vigor
 * después de medir `antes`, todavía no refleja el cambio y hay que esperar
 * (decisión 4 del PO).
 *
 * No hay un ahorro mínimo: si el espacio no baja, `comparar` lo dice y sale con 0.
 * El cambio no hace daño y no se revierte (decisión 4).
 *
 * Usa las credenciales de `gcloud` de quien lo ejecuta.
 *
 * @module scripts/medirEspacioBusqueda
 * @see platform-docs/stories/6.10.2-indices-solo-donde-se-busca/refinamiento.md (T8)
 */

const fs = require('fs');
const path = require('path');

const { tokenDeAcceso } = require('./gcpMonitoringApi');
const { medirAlmacenamiento } = require('../services/consumption/consumptionMetricsService');
const { compararEspacio, BYTES_POR_GIB } = require('./medicionHistorial');

const DIR_MEDICIONES = path.resolve(
  __dirname,
  '../../../../platform-docs/stories/6.10.2-indices-solo-donde-se-busca/mediciones',
);

const ETIQUETAS = Object.freeze(['antes', 'despues']);

/** Horas hacia atrás que se leen: dos actualizaciones diarias de la métrica, con margen. */
const HORAS_LEIDAS = 50;

function printUsage() {
  console.log(`
Mide el espacio de Firestore antes y después de publicar la HU 6.10.2. Solo lee.

  medir --etiqueta <antes|despues>   Guarda la medición en ${path.relative(process.cwd(), DIR_MEDICIONES)}
  comparar                           Compara las dos mediciones guardadas
  --help                             Esta ayuda
`);
}

/**
 * @param {Array<string>} argv Argumentos sin `node` ni el script.
 * @returns {{comando: string|null, etiqueta: string|null, help: boolean}}
 */
function parseArgs(argv) {
  const args = { comando: null, etiqueta: null, help: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--etiqueta') {
      args.etiqueta = argv[i + 1] || null;
      i += 1;
    } else if (!args.comando) args.comando = arg;
    else {
      console.error(`[medirEspacioBusqueda] Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }
  return args;
}

/**
 * El valor vigente de la serie y desde cuándo lo está.
 *
 * `vigenteDesde` es el primer minuto con el valor actual **después de un
 * cambio**. Si el valor no cambió en toda la ventana leída, no se sabe desde
 * cuándo está vigente y queda en `null`: poner el inicio de la ventana haría
 * pasar por reciente un valor viejo.
 *
 * @param {Array<{inicio: string, fin: string, valor: number}>} puntos En orden, del más viejo al más nuevo.
 * @returns {{bytes: number, instante: string, vigenteDesde: (string|null)}}
 * @throws {Error} Si la serie viene vacía.
 */
function valorVigente(puntos) {
  if (!Array.isArray(puntos) || puntos.length === 0) {
    throw new Error('Cloud Monitoring no devolvió ningún punto de almacenamiento.');
  }

  const ultimo = puntos[puntos.length - 1];
  let desde = puntos.length - 1;
  while (desde > 0 && puntos[desde - 1].valor === ultimo.valor) desde -= 1;

  return { bytes: ultimo.valor, instante: ultimo.fin, vigenteDesde: desde > 0 ? puntos[desde].inicio : null };
}

/**
 * Antes contra después, sin mínimo (decisión 4).
 *
 * @param {{antes: {bytes: number, medidoEn: string}, despues: {bytes: number, vigenteDesde: (string|null)}}} p
 * @returns {Object} Las cifras de `compararEspacio` más `diferenciaBytes`, `baja` y `actualizada`.
 */
function compararMediciones({ antes, despues }) {
  const { cumple, ...cifras } = compararEspacio({ bytesAntes: antes.bytes, bytesDespues: despues.bytes });

  return {
    ...cifras,
    diferenciaBytes: antes.bytes - despues.bytes,
    baja: despues.bytes < antes.bytes,
    // `antes` se mide justo antes de publicar. Si el valor de `despues` no entró
    // en vigor después de ese momento, la métrica aún no refleja la publicación.
    actualizada: despues.vigenteDesde !== null && Date.parse(despues.vigenteDesde) > Date.parse(antes.medidoEn),
  };
}

/**
 * El veredicto en texto, listo para pegar en el `dev-record.md`.
 *
 * @param {Object} c Resultado de `compararMediciones`.
 * @param {{antes: Object, despues: Object}} m Las dos mediciones.
 * @returns {string} Varias líneas.
 */
function formatearComparacion(c, { antes, despues }) {
  const gib = (b) => (b / BYTES_POR_GIB).toFixed(4);
  const desde = (m) => m.vigenteDesde || `antes de la ventana leída (${HORAS_LEIDAS} h)`;
  const lineas = [
    `Antes  : ${gib(antes.bytes)} GiB (${antes.bytes} bytes), medido ${antes.medidoEn}, vigente desde ${desde(antes)}`,
    `Después: ${gib(despues.bytes)} GiB (${despues.bytes} bytes), medido ${despues.medidoEn}, vigente desde ${desde(despues)}`,
    `Diferencia: ${gib(c.diferenciaBytes)} GiB (${c.diferenciaBytes} bytes), ${(c.reduccion * 100).toFixed(1)} %`,
  ];

  if (!c.actualizada) {
    lineas.push('⏳ La métrica no se ha actualizado desde la medición «antes»: espera y vuelve a medir «despues».');
  } else if (c.baja) {
    lineas.push('✅ El espacio bajó.');
  } else {
    lineas.push('ℹ️ El espacio no bajó. Se registra y no se revierte (decisión 4 del PO).');
  }

  return lineas.join('\n');
}

function rutaDeMedicion(etiqueta) {
  return path.join(DIR_MEDICIONES, `${etiqueta}.json`);
}

async function medir(args) {
  if (!ETIQUETAS.includes(args.etiqueta)) {
    throw new Error(`--etiqueta tiene que ser una de: ${ETIQUETAS.join(', ')}.`);
  }

  const puntos = await medirAlmacenamiento({ token: tokenDeAcceso(), hasta: new Date(), horas: HORAS_LEIDAS });
  const medicion = { etiqueta: args.etiqueta, medidoEn: new Date().toISOString(), ...valorVigente(puntos) };

  fs.mkdirSync(DIR_MEDICIONES, { recursive: true });
  fs.writeFileSync(rutaDeMedicion(args.etiqueta), `${JSON.stringify(medicion, null, 2)}\n`);

  console.log(`[medirEspacioBusqueda] ${args.etiqueta}: ${(medicion.bytes / BYTES_POR_GIB).toFixed(4)} GiB ` +
    `(${medicion.bytes} bytes), vigente desde ${medicion.vigenteDesde}`);
  console.log(`[medirEspacioBusqueda] Guardada en ${rutaDeMedicion(args.etiqueta)}`);
  return 0;
}

function comparar() {
  const leer = (etiqueta) => {
    const ruta = rutaDeMedicion(etiqueta);
    if (!fs.existsSync(ruta)) throw new Error(`Falta la medición «${etiqueta}»: corre 'medir --etiqueta ${etiqueta}'.`);
    return JSON.parse(fs.readFileSync(ruta, 'utf8'));
  };
  const mediciones = { antes: leer('antes'), despues: leer('despues') };

  console.log(formatearComparacion(compararMediciones(mediciones), mediciones));
  return 0;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.comando) {
    printUsage();
    return 0;
  }
  if (args.comando === 'medir') return medir(args);
  if (args.comando === 'comparar') return comparar();

  console.error(`[medirEspacioBusqueda] Comando no reconocido: ${args.comando}`);
  printUsage();
  return 2;
}

if (require.main === module) {
  (async () => {
    try {
      process.exit(await main());
    } catch (error) {
      console.error('');
      console.error('[medirEspacioBusqueda] ERROR:', error.message);
      process.exit(1);
    }
  })();
}

module.exports = { parseArgs, valorVigente, compararMediciones, formatearComparacion };
