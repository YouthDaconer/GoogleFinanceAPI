#!/usr/bin/env node

/**
 * CLI Script — Reabrir la migración del libro mayor de saldos para un usuario.
 *
 * **Por qué hace falta.** La migración de la HU 2.6 corre una sola vez por
 * usuario: `useBalanceLedgerMigration` lee `userData/{uid}.balanceLedgerMigration`
 * y, si su `status` es `done`, sale sin hacer nada. Es lo correcto en régimen
 * normal —nadie quiere que se reprocese en cada visita—, pero significa que un
 * arreglo posterior de la migración **no alcanza a quien ya la corrió**.
 *
 * Y hubo uno: los saldos en la **moneda de referencia** se quedaban sin su
 * asiento de apertura. La rama que lo escribe estaba detrás del guard de
 * exposición cambiaria, así que un saldo en USD de un usuario con referencia USD
 * recibía el veredicto de deriva pero nunca el asiento que la explica. Resultado:
 * "No conciliado" permanente, con un botón de ajuste que el usuario no tiene cómo
 * cuadrar. Este script borra la marca para que la migración —ya arreglada—
 * vuelva a correr en la siguiente visita a la página de cuentas.
 *
 * **Qué NO hace.** No toca saldos, ni movimientos, ni bases de costo. Sólo
 * retira la marca de "ya terminé". La migración es idempotente: los saldos que
 * ya cuadran no reciben asiento, y una base `user-confirmed` es intocable.
 *
 * **Qué se conserva.** `acknowledgedAccounts` — los avisos de tasa que el
 * usuario ya descartó siguen descartados. Reabrir la migración no debe volver a
 * preguntarle lo que ya contestó.
 *
 * Uso:
 *   node scripts/resetBalanceLedgerMigration.js --email alguien@dominio.com
 *   node scripts/resetBalanceLedgerMigration.js --uid gTZ6Ie8...
 *   node scripts/resetBalanceLedgerMigration.js --email alguien@dominio.com --apply
 *
 * Por defecto es **dry-run**: enseña qué haría y no escribe nada.
 *
 * @see services/balanceLedgerMigration.js
 * @see platform-docs/stories/2.6-libro-mayor-saldo-migracion/refinamiento.md (D13)
 */

const admin = require("../services/firebaseAdmin");

const db = admin.firestore();

const FIELD = "balanceLedgerMigration";

// ============================================================================
// USAGE
// ============================================================================

function printUsage() {
  console.log(`
Reabre la migración del libro mayor de saldos para UN usuario.

  --email <correo>   Identifica al usuario por su correo (recomendado)
  --uid <uid>        Identifica al usuario por su UID
  --apply            Escribe de verdad. Sin esto, sólo informa.
  --help             Esta ayuda.

Hay que indicar --email o --uid, no los dos.
`);
}

function parseArgs(argv) {
  const args = { email: null, uid: null, apply: false, help: false };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") args.help = true;
    else if (arg === "--apply") args.apply = true;
    else if (arg === "--email") args.email = argv[++i] || null;
    else if (arg === "--uid") args.uid = argv[++i] || null;
    else {
      console.error(`Argumento no reconocido: ${arg}`);
      args.help = true;
    }
  }

  return args;
}

// ============================================================================
// RESOLUCIÓN DE USUARIO
// ============================================================================

/**
 * Resuelve el usuario y comprueba que existe en Auth.
 *
 * Se verifica siempre contra Auth, incluso cuando se pasa el UID: un UID mal
 * tecleado apuntaría a un documento inexistente —o peor, al de otro usuario—
 * y este script escribe en producción.
 *
 * @param {{email: string|null, uid: string|null}} args
 * @returns {Promise<{uid: string, email: string}>}
 */
async function resolveUser({ email, uid }) {
  if (email && uid) {
    throw new Error("Indica --email o --uid, no los dos.");
  }
  if (!email && !uid) {
    throw new Error("Falta --email o --uid.");
  }

  const record = email
    ? await admin.auth().getUserByEmail(email)
    : await admin.auth().getUser(uid);

  return { uid: record.uid, email: record.email || "(sin correo)" };
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (args.help) {
    printUsage();
    process.exit(0);
  }

  const { uid, email } = await resolveUser(args);

  console.log("");
  console.log("=".repeat(64));
  console.log("  Reabrir migración del libro mayor de saldos");
  console.log("=".repeat(64));
  console.log(`  Usuario : ${email}`);
  console.log(`  UID     : ${uid}`);
  console.log(`  Modo    : ${args.apply ? "APLICAR" : "dry-run (no escribe)"}`);
  console.log("");

  const userRef = db.collection("userData").doc(uid);
  const snapshot = await userRef.get();

  if (!snapshot.exists) {
    console.log("El usuario no tiene documento en `userData`.");
    console.log("La migración correrá sola en su próxima visita. Nada que hacer.");
    return;
  }

  const mark = snapshot.data()?.[FIELD];

  if (!mark) {
    console.log("El usuario no tiene marca de migración.");
    console.log("La migración correrá sola en su próxima visita. Nada que hacer.");
    return;
  }

  const acknowledged = Array.isArray(mark.acknowledgedAccounts)
    ? mark.acknowledgedAccounts
    : [];

  console.log("Marca actual:");
  console.log(`  status            : ${mark.status || "(sin status)"}`);
  console.log(`  migratedCount     : ${mark.migratedCount ?? 0}`);
  console.log(`  driftCount        : ${mark.driftCount ?? 0}`);
  console.log(`  avisos pendientes : ${(mark.notices || []).length}`);
  console.log(`  avisos descartados: ${acknowledged.length}  <- se conservan`);
  console.log("");

  if (!args.apply) {
    console.log("Dry-run: no se ha escrito nada.");
    console.log("Se borraría la marca conservando `acknowledgedAccounts`.");
    console.log("Vuelve a lanzarlo con --apply para hacerlo de verdad.");
    return;
  }

  // Se reemplaza la marca entera por sólo lo que debe sobrevivir. Un
  // `FieldValue.delete()` del campo completo perdería los avisos ya
  // descartados y volvería a preguntar lo que el usuario ya contestó.
  await userRef.set(
    { [FIELD]: { acknowledgedAccounts: acknowledged } },
    { mergeFields: [FIELD] },
  );

  console.log("Marca reabierta.");
  console.log("");
  console.log("Siguiente paso: entra a /portfolio-account-management con ese");
  console.log("usuario. La migración corre sola, escribe las aperturas que");
  console.log("faltaban y los saldos pasan a conciliados.");
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error("");
    console.error("ERROR:", error.message);
    process.exit(1);
  });
