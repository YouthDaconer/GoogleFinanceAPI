#!/usr/bin/env node

/**
 * FEAT-PRICING-RESTRUCTURE-001: Migración de features Free users existentes
 *
 * Actualiza subscription.features para todos los usuarios con planId: 'free'
 * aplicando los 21 keys del nuevo contrato (5 nuevos + 4 valores modificados).
 *
 * Idempotente: usa merge, no destruye keys extra existentes.
 * Zero-downtime: no requiere parar servicios.
 *
 * Uso: node scripts/migrate-free-features-v2.js [--dry-run]
 *
 * @see docs/architecture/FEAT-PRICING-RESTRUCTURE-001-pricing-paywall-strategy-design.md § 11
 */

const admin = require("../services/firebaseAdmin");
const { PLAN_FEATURES } = require("../services/payment/planFeatures");

const db = admin.firestore();
const BATCH_LIMIT = 500;
const isDryRun = process.argv.includes("--dry-run");

async function migrateFreeFeatures() {
  console.log(`\n🔄 FEAT-PRICING-RESTRUCTURE-001: Migrar features de Free users a v2 (21 keys)`);
  console.log(`   Modo: ${isDryRun ? "DRY RUN (sin escrituras)" : "EJECUCIÓN REAL"}\n`);

  const usersSnapshot = await db.collection("userData").get();
  const freeUsers = usersSnapshot.docs.filter((doc) => {
    const data = doc.data();
    return data.subscription && data.subscription.planId === "free";
  });

  console.log(`📊 Total usuarios: ${usersSnapshot.size}`);
  console.log(`📊 Con planId 'free': ${freeUsers.length}`);

  if (freeUsers.length === 0) {
    console.log("\n✅ No hay usuarios Free para migrar.");
    return;
  }

  if (isDryRun) {
    console.log("\n🔍 Usuarios que serían migrados:");
    freeUsers.forEach((doc) => console.log(`   - ${doc.id}`));
    console.log(`\n📋 Features a aplicar:`, JSON.stringify(PLAN_FEATURES.free, null, 2));
    console.log(`\nEjecuta sin --dry-run para aplicar cambios.`);
    return;
  }

  let migratedCount = 0;
  let errorCount = 0;

  for (let i = 0; i < freeUsers.length; i += BATCH_LIMIT) {
    const chunk = freeUsers.slice(i, i + BATCH_LIMIT);
    const batch = db.batch();
    let chunkErrors = 0;

    for (const doc of chunk) {
      try {
        batch.set(doc.ref, { subscription: { features: PLAN_FEATURES.free } }, { merge: true });
      } catch (err) {
        console.error(`   ❌ Error preparando ${doc.id}: ${err.message}`);
        chunkErrors++;
      }
    }

    await batch.commit();
    errorCount += chunkErrors;
    migratedCount += chunk.length - chunkErrors;
    console.log(`   Batch ${Math.floor(i / BATCH_LIMIT) + 1}: ${chunk.length - chunkErrors} usuarios migrados${chunkErrors > 0 ? `, ${chunkErrors} errores` : ''}`);
  }

  console.log(`\n✅ Migración completada: ${migratedCount} usuarios Free actualizados a v2.`);
  if (errorCount > 0) {
    console.log(`⚠️  ${errorCount} errores individuales.`);
  }
}

migrateFreeFeatures().catch((error) => {
  console.error("❌ Error en migración:", error);
  process.exit(1);
});
