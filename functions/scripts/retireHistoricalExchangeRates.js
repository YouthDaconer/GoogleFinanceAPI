#!/usr/bin/env node

/**
 * CLI Script — Retirada del archivo de tasas de cambio por fecha
 *
 * HU #3: ninguna tasa de cambio se guarda. La colección
 * `historicalExchangeRates/{YYYY-MM-DD}` nació para no repetir consultas cuando
 * se preguntaba **día por día y divisa por divisa**. Al pasar a consultar por
 * rango —una llamada por divisa cubre un año entero— la caché deja de ahorrar y
 * solo queda su coste: espacio, una segunda verdad que puede quedar incompleta,
 * y la posibilidad de que el dato guardado y el real difieran (RN-3-A).
 *
 * **Lo que hace segura la retirada:** el registro contable no vive aquí. La tasa
 * aplicada a cada operación se escribe en el propio documento del movimiento
 * (`acquisitionRate`, `realizationRate`, el costo ya convertido) desde la épica
 * #2. Borrar esta colección no puede alterar ninguna cifra ya registrada
 * (RN-3-B).
 *
 * Se ejecuta **después** de desplegar el código que deja de leerla y escribirla,
 * no antes: separar los dos momentos permite volver atrás sin haber perdido nada
 * durante la ventana de verificación.
 *
 * Uso:
 *   node scripts/retireHistoricalExchangeRates.js            # dry-run (por defecto)
 *   node scripts/retireHistoricalExchangeRates.js --apply    # borra de verdad
 *   node scripts/retireHistoricalExchangeRates.js --apply --batch 200
 *
 * @see platform-docs/stories/3-tasa-vigente-canal-mercado/refinamiento.md (T17, D10)
 * @see docs/architecture/SCALE-PERF-002-historical-exchange-rates-cache-design.md (DEROGADO)
 */

const admin = require("../services/firebaseAdmin");

const db = admin.firestore();

const COLLECTION = "historicalExchangeRates";
const DEFAULT_BATCH_SIZE = 300;
/** Tope de Firestore para escrituras por lote */
const MAX_BATCH_SIZE = 500;

// ============================================================================
// USAGE
// ============================================================================

function printUsage() {
  console.error("Uso:");
  console.error("  node scripts/retireHistoricalExchangeRates.js            # dry-run");
  console.error("  node scripts/retireHistoricalExchangeRates.js --apply");
  console.error("  node scripts/retireHistoricalExchangeRates.js --apply --batch 200");
  console.error("");
  console.error("Sin --apply no se borra nada: solo se informa de cuántos documentos hay.");
}

// ============================================================================
// COMMANDS
// ============================================================================

/**
 * Recorre la colección contando documentos y mostrando el rango de fechas.
 *
 * @returns {Promise<{count: number, first: string|null, last: string|null}>}
 */
async function inspect() {
  let count = 0;
  let first = null;
  let last = null;
  let cursor = null;

  for (;;) {
    let query = db.collection(COLLECTION).orderBy(admin.firestore.FieldPath.documentId()).limit(DEFAULT_BATCH_SIZE);
    if (cursor) query = query.startAfter(cursor);

    const snapshot = await query.get();
    if (snapshot.empty) break;

    if (first === null) first = snapshot.docs[0].id;
    last = snapshot.docs[snapshot.docs.length - 1].id;
    count += snapshot.size;
    cursor = snapshot.docs[snapshot.docs.length - 1];

    if (snapshot.size < DEFAULT_BATCH_SIZE) break;
  }

  return { count, first, last };
}

/**
 * Borra la colección por lotes.
 *
 * @param {number} batchSize - Documentos por lote (máximo 500)
 * @returns {Promise<number>} Documentos borrados
 */
async function deleteAll(batchSize) {
  let deleted = 0;

  for (;;) {
    const snapshot = await db.collection(COLLECTION).limit(batchSize).get();
    if (snapshot.empty) break;

    const batch = db.batch();
    snapshot.docs.forEach((doc) => batch.delete(doc.ref));
    await batch.commit();

    deleted += snapshot.size;
    console.log(`  ... ${deleted} documentos borrados`);

    if (snapshot.size < batchSize) break;
  }

  return deleted;
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const args = process.argv.slice(2);

  if (args.includes("--help") || args.includes("-h")) {
    printUsage();
    process.exit(0);
  }

  const apply = args.includes("--apply");

  const batchIndex = args.indexOf("--batch");
  const batchSize = batchIndex >= 0 ? Number.parseInt(args[batchIndex + 1], 10) : DEFAULT_BATCH_SIZE;

  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
    console.error(`--batch debe ser un entero entre 1 y ${MAX_BATCH_SIZE}`);
    process.exit(1);
  }

  console.log(`Colección: ${COLLECTION}`);
  console.log(`Modo: ${apply ? "BORRADO REAL" : "dry-run (no se borra nada)"}`);
  console.log("");

  const { count, first, last } = await inspect();

  if (count === 0) {
    console.log("No hay documentos que retirar. La colección ya no existe o está vacía.");
    process.exit(0);
  }

  console.log(`Documentos encontrados: ${count}`);
  console.log(`Rango de fechas: ${first} → ${last}`);
  console.log("");

  if (!apply) {
    console.log("Dry-run: nada se ha borrado. Repite con --apply para retirarla.");
    console.log("");
    console.log("Antes de aplicar, comprueba que el despliegue que deja de leer esta");
    console.log("colección ya está en producción. Las tasas de los movimientos ya");
    console.log("registrados viven en los propios movimientos y no dependen de esto.");
    process.exit(0);
  }

  const deleted = await deleteAll(batchSize);

  console.log("");
  console.log(`Retirada completada: ${deleted} documentos borrados.`);
  console.log("Recuerda quitar también la regla de firestore.rules si sigue ahí.");
}

main().catch((error) => {
  console.error("Error retirando el archivo de tasas:", error);
  process.exit(1);
});
