/**
 * Script de Migración de Precisión de Timestamps
 * 
 * FIX-TIMESTAMP-001: Migrar campos de fecha para incluir hora, minutos, segundos y milisegundos
 * 
 * Problema: Los campos acquisitionDate (assets) y date (transactions) actualmente
 * solo almacenan la fecha (YYYY-MM-DD), lo cual impide ordenamiento preciso cuando
 * hay múltiples operaciones el mismo día.
 * 
 * Solución: Combinar la fecha existente con la hora del campo createdAt.
 * Si createdAt no existe, usar hora 00:00:00.000Z.
 * 
 * Uso:
 *   - Dry-run (solo analiza): node scripts/migrate-timestamp-precision.js --dry-run
 *   - Ejecutar migración:     node scripts/migrate-timestamp-precision.js --fix
 *   - Analizar solo assets:   node scripts/migrate-timestamp-precision.js --dry-run --collection=assets
 *   - Analizar solo trans:    node scripts/migrate-timestamp-precision.js --dry-run --collection=transactions
 * 
 * @author Arquitecto Ceiba
 * @date 2026-01-28
 */

const admin = require('firebase-admin');

// Inicializar Firebase Admin
if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();

// Configuración
const BATCH_SIZE = 500; // Firestore permite máximo 500 operaciones por batch

// Contadores para estadísticas
const stats = {
  assets: {
    total: 0,
    needsMigration: 0,
    alreadyMigrated: 0,
    noCreatedAt: 0,
    migrated: 0,
    errors: 0,
  },
  transactions: {
    total: 0,
    needsMigration: 0,
    alreadyMigrated: 0,
    noCreatedAt: 0,
    migrated: 0,
    errors: 0,
  },
};

/**
 * Extrae la hora de un campo createdAt
 * Maneja múltiples formatos: ISO string, Firestore Timestamp, number (epoch ms)
 * 
 * @param {any} createdAt - Campo createdAt en cualquier formato
 * @returns {object} { hours, minutes, seconds, milliseconds }
 */
function extractTimeFromCreatedAt(createdAt) {
  let date;

  if (!createdAt) {
    return { hours: 0, minutes: 0, seconds: 0, milliseconds: 0 };
  }

  // Caso 1: Firestore Timestamp object con método toDate()
  if (createdAt && typeof createdAt.toDate === 'function') {
    date = createdAt.toDate();
  }
  // Caso 2: Firestore Timestamp serializado (objeto con _seconds y _nanoseconds)
  else if (createdAt && typeof createdAt === 'object' && createdAt._seconds !== undefined) {
    // Reconstruir desde _seconds y _nanoseconds
    const milliseconds = createdAt._seconds * 1000 + Math.floor((createdAt._nanoseconds || 0) / 1000000);
    date = new Date(milliseconds);
  }
  // Caso 3: Firestore Timestamp serializado (objeto con seconds y nanoseconds - sin underscore)
  else if (createdAt && typeof createdAt === 'object' && createdAt.seconds !== undefined) {
    const milliseconds = createdAt.seconds * 1000 + Math.floor((createdAt.nanoseconds || 0) / 1000000);
    date = new Date(milliseconds);
  }
  // Caso 4: ISO String (ej: "2025-01-28T14:30:45.123Z")
  else if (typeof createdAt === 'string') {
    date = new Date(createdAt);
  }
  // Caso 5: Number (epoch milliseconds)
  else if (typeof createdAt === 'number') {
    date = new Date(createdAt);
  }
  // Caso 6: Ya es un Date object
  else if (createdAt instanceof Date) {
    date = createdAt;
  }
  // Caso desconocido
  else {
    console.warn(`  ⚠️ Formato desconocido de createdAt:`, typeof createdAt, JSON.stringify(createdAt));
    return { hours: 0, minutes: 0, seconds: 0, milliseconds: 0 };
  }

  // Validar que la fecha sea válida
  if (isNaN(date.getTime())) {
    console.warn(`  ⚠️ createdAt inválido:`, createdAt);
    return { hours: 0, minutes: 0, seconds: 0, milliseconds: 0 };
  }

  return {
    hours: date.getUTCHours(),
    minutes: date.getUTCMinutes(),
    seconds: date.getUTCSeconds(),
    milliseconds: date.getUTCMilliseconds(),
  };
}

/**
 * Detecta el tipo/formato de un campo createdAt para diagnóstico
 * @param {any} createdAt - Campo createdAt
 * @returns {string} Descripción del formato
 */
function getCreatedAtType(createdAt) {
  if (!createdAt) return 'null';
  if (typeof createdAt.toDate === 'function') return 'Firestore Timestamp';
  if (typeof createdAt === 'object' && createdAt._seconds !== undefined) return 'Timestamp serializado (_seconds)';
  if (typeof createdAt === 'object' && createdAt.seconds !== undefined) return 'Timestamp serializado (seconds)';
  if (typeof createdAt === 'string') return 'ISO string';
  if (typeof createdAt === 'number') return 'epoch ms';
  if (createdAt instanceof Date) return 'Date object';
  return `desconocido (${typeof createdAt})`;
}

/**
 * Verifica si un campo de fecha ya tiene timestamp completo
 * @param {string} dateValue - Valor del campo fecha
 * @returns {boolean} true si ya tiene timestamp
 */
function hasTimestamp(dateValue) {
  if (!dateValue) return false;
  return dateValue.includes('T');
}

/**
 * Combina una fecha YYYY-MM-DD con componentes de hora
 * @param {string} dateString - Fecha en formato YYYY-MM-DD
 * @param {object} time - Componentes de hora { hours, minutes, seconds, milliseconds }
 * @returns {string} Fecha ISO con hora (ej: "2025-01-28T14:30:45.123Z")
 */
function combineDateWithTime(dateString, time) {
  const [year, month, day] = dateString.split('-').map(Number);
  const combined = new Date(Date.UTC(
    year,
    month - 1, // JavaScript months are 0-indexed
    day,
    time.hours,
    time.minutes,
    time.seconds,
    time.milliseconds
  ));
  return combined.toISOString();
}

/**
 * Procesa la colección de assets
 * @param {boolean} dryRun - Si true, solo analiza sin modificar
 * @returns {Map} Cache de assets con id -> { createdAt } para fallback en transactions
 */
async function processAssets(dryRun) {
  console.log('\n📦 Procesando colección: assets');
  console.log('   Campo a migrar: acquisitionDate');
  console.log('   Campo fuente de hora: createdAt\n');

  const snapshot = await db.collection('assets').get();
  stats.assets.total = snapshot.size;

  const documentsToUpdate = [];
  const assetsCache = new Map(); // Cache para usar en transactions

  for (const doc of snapshot.docs) {
    const data = doc.data();
    const acquisitionDate = data.acquisitionDate;
    const createdAt = data.createdAt;

    // Guardar en cache para uso posterior en transactions
    assetsCache.set(doc.id, { createdAt: createdAt });

    // Si ya tiene timestamp, está migrado
    if (hasTimestamp(acquisitionDate)) {
      stats.assets.alreadyMigrated++;
      continue;
    }

    // Si no tiene acquisitionDate, error
    if (!acquisitionDate) {
      console.log(`  ❌ Asset ${doc.id}: No tiene acquisitionDate`);
      stats.assets.errors++;
      continue;
    }

    // Extraer hora de createdAt
    const time = extractTimeFromCreatedAt(createdAt);
    
    if (!createdAt) {
      stats.assets.noCreatedAt++;
    }

    // Calcular nuevo valor
    const newAcquisitionDate = combineDateWithTime(acquisitionDate, time);

    documentsToUpdate.push({
      id: doc.id,
      name: data.name || 'N/A',
      oldValue: acquisitionDate,
      newValue: newAcquisitionDate,
      hasCreatedAt: !!createdAt,
      createdAtType: getCreatedAtType(createdAt),
    });

    stats.assets.needsMigration++;
  }

  // Mostrar resumen
  console.log('   Documentos analizados:', stats.assets.total);
  console.log('   Ya migrados (con hora):', stats.assets.alreadyMigrated);
  console.log('   Necesitan migración:', stats.assets.needsMigration);
  console.log('   Sin campo createdAt:', stats.assets.noCreatedAt);
  console.log('   Errores:', stats.assets.errors);

  if (documentsToUpdate.length > 0) {
    console.log('\n   Ejemplos de cambios:');
    documentsToUpdate.slice(0, 5).forEach((doc) => {
      console.log(`     📄 ${doc.id} (${doc.name})`);
      console.log(`        Antes: ${doc.oldValue}`);
      console.log(`        Después: ${doc.newValue}`);
      console.log(`        createdAt: ${doc.hasCreatedAt ? `presente (${doc.createdAtType})` : 'ausente (hora=00:00:00)'}`);
    });
    
    if (documentsToUpdate.length > 5) {
      console.log(`     ... y ${documentsToUpdate.length - 5} más`);
    }
  }

  // Ejecutar migración si no es dry-run
  if (!dryRun && documentsToUpdate.length > 0) {
    console.log('\n   🔄 Ejecutando migración...');
    
    // Procesar en batches
    for (let i = 0; i < documentsToUpdate.length; i += BATCH_SIZE) {
      const batch = db.batch();
      const batchDocs = documentsToUpdate.slice(i, i + BATCH_SIZE);

      for (const doc of batchDocs) {
        const ref = db.collection('assets').doc(doc.id);
        batch.update(ref, { acquisitionDate: doc.newValue });
      }

      await batch.commit();
      stats.assets.migrated += batchDocs.length;
      console.log(`     ✅ Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${batchDocs.length} documentos actualizados`);
    }

    console.log(`   ✅ Migración completada: ${stats.assets.migrated} assets actualizados`);
  }

  return assetsCache;
}

/**
 * Procesa la colección de transactions
 * @param {boolean} dryRun - Si true, solo analiza sin modificar
 * @param {Map} assetsCache - Cache de assets con su createdAt (opcional, para fallback)
 */
async function processTransactions(dryRun, assetsCache = null) {
  console.log('\n💳 Procesando colección: transactions');
  console.log('   Campo a migrar: date');
  console.log('   Campo fuente de hora: createdAt');
  console.log('   Fallback: createdAt del asset asociado (si existe)\n');

  const snapshot = await db.collection('transactions').get();
  stats.transactions.total = snapshot.size;

  // Contadores adicionales para la mejora
  let usedAssetFallback = 0;
  let assetFallbackFailed = 0;

  const documentsToUpdate = [];

  for (const doc of snapshot.docs) {
    const data = doc.data();
    const dateValue = data.date;
    let createdAt = data.createdAt;
    let timeSource = 'transaction.createdAt';

    // Si ya tiene timestamp, está migrado
    if (hasTimestamp(dateValue)) {
      stats.transactions.alreadyMigrated++;
      continue;
    }

    // Si no tiene date, error
    if (!dateValue) {
      console.log(`  ❌ Transaction ${doc.id}: No tiene date`);
      stats.transactions.errors++;
      continue;
    }

    // Si no tiene createdAt, intentar usar el del asset asociado
    if (!createdAt && data.assetId && assetsCache) {
      const assetData = assetsCache.get(data.assetId);
      if (assetData && assetData.createdAt) {
        createdAt = assetData.createdAt;
        timeSource = 'asset.createdAt (fallback)';
        usedAssetFallback++;
      } else {
        assetFallbackFailed++;
        stats.transactions.noCreatedAt++;
      }
    } else if (!createdAt) {
      stats.transactions.noCreatedAt++;
    }

    // Extraer hora de createdAt (ya sea de transaction o asset)
    const time = extractTimeFromCreatedAt(createdAt);

    // Calcular nuevo valor
    const newDate = combineDateWithTime(dateValue, time);

    documentsToUpdate.push({
      id: doc.id,
      assetName: data.assetName || data.symbol || 'N/A',
      type: data.type || 'N/A',
      oldValue: dateValue,
      newValue: newDate,
      hasCreatedAt: !!data.createdAt,
      createdAtType: getCreatedAtType(data.createdAt),
      timeSource: timeSource,
      usedFallback: timeSource.includes('fallback'),
    });

    stats.transactions.needsMigration++;
  }

  // Mostrar estadísticas de fallback
  if (assetsCache) {
    console.log('   📊 Estadísticas de fallback a asset.createdAt:');
    console.log(`      Usaron fallback exitosamente: ${usedAssetFallback}`);
    console.log(`      Fallback falló (asset sin createdAt): ${assetFallbackFailed}`);
  }

  // Mostrar resumen
  console.log('\n   Documentos analizados:', stats.transactions.total);
  console.log('   Ya migrados (con hora):', stats.transactions.alreadyMigrated);
  console.log('   Necesitan migración:', stats.transactions.needsMigration);
  console.log('   Sin createdAt (usarán 00:00:00):', stats.transactions.noCreatedAt);
  console.log('   Errores:', stats.transactions.errors);

  if (documentsToUpdate.length > 0) {
    console.log('\n   Ejemplos de cambios:');
    // Mostrar algunos con fallback y algunos sin
    const withFallback = documentsToUpdate.filter(d => d.usedFallback).slice(0, 3);
    const withoutFallback = documentsToUpdate.filter(d => !d.usedFallback && d.hasCreatedAt).slice(0, 2);
    const noCreatedAt = documentsToUpdate.filter(d => !d.hasCreatedAt && !d.usedFallback).slice(0, 2);
    
    [...withoutFallback, ...withFallback, ...noCreatedAt].forEach((doc) => {
      console.log(`     📄 ${doc.id} (${doc.assetName} - ${doc.type})`);
      console.log(`        Antes: ${doc.oldValue}`);
      console.log(`        Después: ${doc.newValue}`);
      if (doc.usedFallback) {
        console.log(`        ⬆️ Hora tomada de: ${doc.timeSource}`);
      } else if (doc.hasCreatedAt) {
        console.log(`        createdAt: presente (${doc.createdAtType})`);
      } else {
        console.log(`        createdAt: ausente (hora=00:00:00)`);
      }
    });
    
    if (documentsToUpdate.length > 7) {
      console.log(`     ... y ${documentsToUpdate.length - 7} más`);
    }
  }

  // Ejecutar migración si no es dry-run
  if (!dryRun && documentsToUpdate.length > 0) {
    console.log('\n   🔄 Ejecutando migración...');
    
    // Procesar en batches
    for (let i = 0; i < documentsToUpdate.length; i += BATCH_SIZE) {
      const batch = db.batch();
      const batchDocs = documentsToUpdate.slice(i, i + BATCH_SIZE);

      for (const doc of batchDocs) {
        const ref = db.collection('transactions').doc(doc.id);
        batch.update(ref, { date: doc.newValue });
      }

      await batch.commit();
      stats.transactions.migrated += batchDocs.length;
      console.log(`     ✅ Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${batchDocs.length} documentos actualizados`);
    }

    console.log(`   ✅ Migración completada: ${stats.transactions.migrated} transactions actualizadas`);
  }
}

/**
 * Función principal
 */
async function main() {
  console.log('╔══════════════════════════════════════════════════════════════╗');
  console.log('║   FIX-TIMESTAMP-001: Migración de Precisión de Timestamps    ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  // Parsear argumentos
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const fix = args.includes('--fix');
  const onlyAssets = args.includes('--collection=assets');
  const onlyTransactions = args.includes('--collection=transactions');

  // Validar argumentos
  if (!dryRun && !fix) {
    console.log('❌ Error: Debes especificar --dry-run o --fix\n');
    console.log('Uso:');
    console.log('  node scripts/migrate-timestamp-precision.js --dry-run');
    console.log('  node scripts/migrate-timestamp-precision.js --fix');
    console.log('  node scripts/migrate-timestamp-precision.js --dry-run --collection=assets');
    console.log('  node scripts/migrate-timestamp-precision.js --dry-run --collection=transactions');
    process.exit(1);
  }

  if (dryRun && fix) {
    console.log('❌ Error: No puedes usar --dry-run y --fix al mismo tiempo');
    process.exit(1);
  }

  // Mostrar modo
  console.log(`📋 Modo: ${dryRun ? 'DRY-RUN (solo análisis, sin cambios)' : 'FIX (ejecutando migración)'}`);
  
  if (fix) {
    console.log('\n⚠️  ADVERTENCIA: Este modo modificará datos en Firestore.');
    console.log('   Se recomienda hacer un backup antes de continuar.');
    console.log('   Presiona Ctrl+C en los próximos 5 segundos para cancelar...\n');
    await new Promise(resolve => setTimeout(resolve, 5000));
  }

  try {
    let assetsCache = null;

    // Procesar assets primero (genera cache para transactions)
    if (!onlyTransactions) {
      assetsCache = await processAssets(dryRun);
    } else {
      // Si solo procesamos transactions, igual cargamos el cache de assets
      console.log('\n📦 Cargando cache de assets para fallback...');
      const assetsSnapshot = await db.collection('assets').get();
      assetsCache = new Map();
      assetsSnapshot.docs.forEach(doc => {
        const data = doc.data();
        assetsCache.set(doc.id, { createdAt: data.createdAt });
      });
      console.log(`   ✅ Cache cargado: ${assetsCache.size} assets`);
    }
    
    if (!onlyAssets) {
      await processTransactions(dryRun, assetsCache);
    }

    // Resumen final
    console.log('\n╔══════════════════════════════════════════════════════════════╗');
    console.log('║                      RESUMEN FINAL                           ║');
    console.log('╚══════════════════════════════════════════════════════════════╝');
    
    if (!onlyTransactions) {
      console.log('\n📦 Assets:');
      console.log(`   Total: ${stats.assets.total}`);
      console.log(`   Ya migrados: ${stats.assets.alreadyMigrated}`);
      console.log(`   Pendientes: ${stats.assets.needsMigration}`);
      if (fix) {
        console.log(`   Migrados: ${stats.assets.migrated}`);
      }
    }
    
    if (!onlyAssets) {
      console.log('\n💳 Transactions:');
      console.log(`   Total: ${stats.transactions.total}`);
      console.log(`   Ya migrados: ${stats.transactions.alreadyMigrated}`);
      console.log(`   Pendientes: ${stats.transactions.needsMigration}`);
      if (fix) {
        console.log(`   Migrados: ${stats.transactions.migrated}`);
      }
    }

    if (dryRun) {
      console.log('\n📌 Ejecuta con --fix para aplicar los cambios.');
    } else {
      console.log('\n✅ Migración completada exitosamente.');
    }

  } catch (error) {
    console.error('\n❌ Error durante la migración:', error);
    process.exit(1);
  }

  process.exit(0);
}

// Ejecutar
main();
