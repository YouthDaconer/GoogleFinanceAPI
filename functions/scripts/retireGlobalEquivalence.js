#!/usr/bin/env node

/**
 * CLI Script — Retiro operativo de una entrada del catálogo global de equivalencias
 *
 * HU 1.6, escenario 7: cuando se detecta una entrada claramente errónea y con
 * impacto relevante, el equipo de producto la retira y deja de proponerse de
 * inmediato.
 *
 * Es deliberadamente una utilidad de línea de comandos, SIN interfaz gráfica y sin
 * control de acceso por rol: es una acción excepcional y construir una pantalla de
 * administración para ella contradiría RN-02 (la memoria es invisible) sin aportar
 * nada. El control de acceso es el del propio entorno: quien puede ejecutar este
 * script ya tiene credenciales de administración del proyecto.
 *
 * RN-32: retirar NO modifica las transacciones que los usuarios ya importaron.
 * Solo cambia el estado de la entrada para que deje de proponerse.
 *
 * Uso:
 *   node scripts/retireGlobalEquivalence.js <clave> "<motivo>" [operador]
 *   node scripts/retireGlobalEquivalence.js --list [limite]
 *   node scripts/retireGlobalEquivalence.js --show <clave>
 *
 * Ejemplos:
 *   node scripts/retireGlobalEquivalence.js "broker:degiro::VUAA" "Apuntaba al ETF de acumulacion equivocado" carlos
 *   node scripts/retireGlobalEquivalence.js --list 20
 *   node scripts/retireGlobalEquivalence.js --show "broker:degiro::VUAA"
 *
 * @see platform-docs/stories/1.6-catalogo-global-equivalencias/
 */

const admin = require("../services/firebaseAdmin");
const {
  retireEntry,
  CATALOG_COLLECTION,
  AUDIT_COLLECTION,
} = require("../services/transactions/services/globalEquivalenceRepository");
const { GLOBAL_EQUIVALENCE_STATUS } = require("../services/transactions/types");

const db = admin.firestore();

// ============================================================================
// USAGE
// ============================================================================

function printUsage() {
  console.error("Uso:");
  console.error('  node scripts/retireGlobalEquivalence.js <clave> "<motivo>" [operador]');
  console.error("  node scripts/retireGlobalEquivalence.js --list [limite]");
  console.error("  node scripts/retireGlobalEquivalence.js --show <clave>");
  console.error("");
  console.error("La clave tiene la forma <sourceFormatId>::<SIMBOLO>, por ejemplo:");
  console.error('  "broker:degiro::VUAA"');
  console.error('  "fmt:a1b2c3d4e5f60718::VWCE"');
}

// ============================================================================
// COMMANDS
// ============================================================================

/**
 * Lista las entradas activas del catálogo.
 */
async function listActive(limit) {
  const snapshot = await db
    .collection(CATALOG_COLLECTION)
    .where("status", "==", GLOBAL_EQUIVALENCE_STATUS.ACTIVE)
    .limit(limit)
    .get();

  if (snapshot.empty) {
    console.log("El catálogo global no tiene entradas activas.");
    return;
  }

  console.log(`Entradas activas (${snapshot.size}):`);
  console.log("");

  snapshot.docs.forEach((doc) => {
    const d = doc.data();
    console.log(`  ${doc.id}`);
    console.log(`    → ${d.resolvedSymbol}  [${d.assetType} / ${d.currency}]`);
    console.log(`    promovida: ${d.promotedAt}`);
  });
}

/**
 * Muestra una entrada y su historial de auditoría.
 */
async function showEntry(key) {
  const doc = await db.collection(CATALOG_COLLECTION).doc(key).get();

  if (!doc.exists) {
    console.log(`No existe la entrada "${key}".`);
    return;
  }

  const d = doc.data();

  console.log(`Entrada: ${key}`);
  console.log(`  símbolo de origen : ${d.sourceSymbol}`);
  console.log(`  formato de origen : ${d.sourceFormatId}`);
  console.log(`  ticker canónico   : ${d.resolvedSymbol}`);
  console.log(`  tipo / moneda     : ${d.assetType} / ${d.currency}`);
  console.log(`  estado            : ${d.status}`);
  console.log(`  promovida         : ${d.promotedAt}`);

  if (d.retiredAt) {
    console.log(`  retirada          : ${d.retiredAt}`);
  }

  const audit = await db
    .collection(AUDIT_COLLECTION)
    .where("key", "==", key)
    .get();

  if (!audit.empty) {
    console.log("");
    console.log("Auditoría:");

    audit.docs
      .map((a) => a.data())
      .sort((a, b) => String(a.at).localeCompare(String(b.at)))
      .forEach((entry) => {
        console.log(`  ${entry.at}  ${entry.action}  por ${entry.actor}`);
        console.log(`    ${entry.reason}`);
      });
  }
}

/**
 * Retira una entrada.
 */
async function retire(key, reason, actor) {
  const before = await db.collection(CATALOG_COLLECTION).doc(key).get();

  if (!before.exists) {
    console.error(`Error: no existe la entrada "${key}".`);
    console.error("Usa --list para ver las entradas activas.");
    process.exit(1);
  }

  if (before.data().status === GLOBAL_EQUIVALENCE_STATUS.RETIRED) {
    console.log(`La entrada "${key}" ya estaba retirada (${before.data().retiredAt}).`);
    return;
  }

  const retired = await retireEntry({ key, reason, actor });

  if (!retired) {
    console.error(`Error: no se pudo retirar "${key}".`);
    process.exit(1);
  }

  console.log(`Retirada la entrada "${key}".`);
  console.log(`  ticker que dejaba de proponerse: ${before.data().resolvedSymbol}`);
  console.log(`  motivo   : ${reason}`);
  console.log(`  operador : ${actor}`);
  console.log("");
  console.log("Deja de proponerse de inmediato.");
  console.log("Las transacciones ya importadas por los usuarios NO se modifican (RN-32).");
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0) {
    printUsage();
    process.exit(1);
  }

  if (args[0] === "--list") {
    const limit = Number(args[1]) || 50;
    await listActive(limit);
    return;
  }

  if (args[0] === "--show") {
    if (!args[1]) {
      printUsage();
      process.exit(1);
    }
    await showEntry(args[1]);
    return;
  }

  const [key, reason, actor] = args;

  if (!key || !reason) {
    console.error("Error: se requieren la clave y el motivo del retiro.");
    console.error("El motivo queda en la auditoría: describe por qué la entrada era incorrecta.");
    console.error("");
    printUsage();
    process.exit(1);
  }

  await retire(key, reason, actor || process.env.USER || process.env.USERNAME || "operador");
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error("Error:", error.message);
    process.exit(1);
  });
