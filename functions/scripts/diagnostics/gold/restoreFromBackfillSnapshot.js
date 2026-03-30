/**
 * restoreFromBackfillSnapshot.js
 * 
 * Restore portfolio performance documents from a pre-backfill snapshot.
 * 
 * USAGE:
 *   node restoreFromBackfillSnapshot.js <snapshot-file> --dry-run
 *   node restoreFromBackfillSnapshot.js <snapshot-file> --fix
 * 
 * @see SCALE-003
 */

const admin = require('firebase-admin');
const fs = require('fs');

const serviceAccount = require('../../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

const BATCH_SIZE = 20;

function log(level, message, details) {
  const timestamp = new Date().toISOString().split('T')[1].split('.')[0];
  const prefix = { INFO: 'ℹ️', SUCCESS: '✅', ERROR: '❌', WARNING: '⚠️', PROGRESS: '🔄' }[level] || '';
  console.log(`[${timestamp}] ${prefix} ${message}`);
  if (details) console.log('   ', JSON.stringify(details, null, 2));
}

function parseArgs() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.log('USO: node restoreFromBackfillSnapshot.js <snapshot-file> --dry-run|--fix');
    process.exit(1);
  }

  const filePath = args[0];
  const mode = args.find(a => a === '--dry-run' || a === '--fix');

  if (!mode) {
    console.log('ERROR: Debes especificar --dry-run o --fix');
    process.exit(1);
  }

  if (!fs.existsSync(filePath)) {
    console.log(`ERROR: Archivo no encontrado: ${filePath}`);
    process.exit(1);
  }

  return { filePath, mode: mode.replace('--', '') };
}

function validateSnapshot(data) {
  const errors = [];

  if (!data.userId || typeof data.userId !== 'string') {
    errors.push('Falta campo "userId" (string)');
  }

  if (!Array.isArray(data.documents)) {
    errors.push('Falta campo "documents" (array)');
  } else if (data.documents.length === 0) {
    errors.push('El array "documents" está vacío');
  } else {
    const firstDoc = data.documents[0];
    if (!firstDoc.path || typeof firstDoc.path !== 'string') {
      errors.push('Documentos deben tener campo "path" (string)');
    }
    if (!firstDoc.data || typeof firstDoc.data !== 'object') {
      errors.push('Documentos deben tener campo "data" (object)');
    }
  }

  if (!data.timestamp || isNaN(Date.parse(data.timestamp))) {
    errors.push('Falta campo "timestamp" (ISO string válido)');
  }

  return errors;
}

async function main() {
  const { filePath, mode } = parseArgs();

  console.log('═'.repeat(60));
  console.log('  RESTAURACIÓN DESDE SNAPSHOT PRE-BACKFILL');
  console.log('═'.repeat(60));
  console.log('');

  log('INFO', `Archivo: ${filePath}`);
  log('INFO', `Modo: ${mode}`);
  console.log('');

  let rawData;
  try {
    rawData = fs.readFileSync(filePath, 'utf8');
  } catch (readError) {
    log('ERROR', `No se pudo leer el archivo: ${readError.message}`);
    process.exit(1);
  }

  let snapshot;
  try {
    snapshot = JSON.parse(rawData);
  } catch (parseError) {
    log('ERROR', `JSON inválido: ${parseError.message}`);
    process.exit(1);
  }

  const validationErrors = validateSnapshot(snapshot);
  if (validationErrors.length > 0) {
    log('ERROR', 'Snapshot inválido:');
    validationErrors.forEach(e => log('ERROR', `  - ${e}`));
    process.exit(1);
  }

  log('SUCCESS', 'Snapshot válido');
  log('INFO', `Usuario: ${snapshot.userId}`);
  log('INFO', `Rango: ${snapshot.startDate} → ${snapshot.endDate}`);
  log('INFO', `Documentos: ${snapshot.documentsCount || snapshot.documents.length}`);
  log('INFO', `Fecha del snapshot: ${snapshot.timestamp}`);
  console.log('');

  let restored = 0;
  let failed = 0;

  if (mode === 'dry-run') {
    log('INFO', '[DRY-RUN] Documentos que se restaurarían:');
    snapshot.documents.forEach(doc => {
      log('INFO', `  Restauraría: ${doc.path}`);
    });
    log('INFO', `[DRY-RUN] Total: ${snapshot.documents.length} documentos`);
  } else {
    log('PROGRESS', `Restaurando ${snapshot.documents.length} documentos...`);

    for (let i = 0; i < snapshot.documents.length; i += BATCH_SIZE) {
      const batch = db.batch();
      const chunk = snapshot.documents.slice(i, i + BATCH_SIZE);

      chunk.forEach(doc => {
        const ref = db.doc(doc.path);
        batch.set(ref, doc.data);
      });

      try {
        await batch.commit();
        restored += chunk.length;
        log('SUCCESS', `Batch ${Math.floor(i / BATCH_SIZE) + 1} restaurado (${chunk.length} docs)`);
      } catch (error) {
        failed += chunk.length;
        log('ERROR', `Batch ${Math.floor(i / BATCH_SIZE) + 1} falló: ${error.message}`);
      }
    }

    console.log('');
    log('SUCCESS', `Restauración completada: ${restored} de ${snapshot.documents.length} documentos`);
    if (failed > 0) {
      log('WARNING', `${failed} documentos no pudieron ser restaurados`);
    }
  }
}

main().catch(error => {
  log('ERROR', `Error fatal: ${error.message}`);
  process.exit(1);
});
