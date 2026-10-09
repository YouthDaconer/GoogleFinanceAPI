#!/usr/bin/env node

/**
 * BUG-GATE-002-D: Migración de features Pro/Lifetime al contrato de 21 keys
 *
 * Actualiza usuarios Pro/Lifetime que:
 * (a) Tienen features con contrato viejo (< 21 keys) → merge con PLAN_FEATURES[plan]
 * (b) No tienen subscriptionOrigin → setea "mock_checkout" (usuarios pre-trial)
 *
 * Uso: node scripts/migrateProFeatures21Keys.js [--dry-run]
 *
 * @see docs/architecture/BUG-GATE-002-etf-analyzer-locked-trial-heuristic-analysis.md
 */

const admin = require("../services/firebaseAdmin");
const { PLAN_FEATURES } = require("../services/payment/planFeatures");

const db = admin.firestore();
const BATCH_LIMIT = 500;
const isDryRun = process.argv.includes("--dry-run");

// Key centinela: si falta, el documento tiene el contrato viejo
const SENTINEL_KEY = "hasEtfAnalyzer";

async function migrateProFeatures() {
  console.log(`\n🔄 BUG-GATE-002-D: Migrar Pro/Lifetime al contrato de 21 keys + subscriptionOrigin`);
  console.log(`   Modo: ${isDryRun ? "DRY RUN (sin escrituras)" : "EJECUCIÓN REAL"}\n`);

  const usersSnapshot = await db.collection("userData").get();

  const usersToMigrate = usersSnapshot.docs.filter((doc) => {
    const sub = doc.data().subscription;
    if (!sub) return false;
    if (sub.planId !== "pro" && sub.planId !== "lifetime") return false;

    const needsFeatures = !sub.features || !(SENTINEL_KEY in sub.features);
    const needsOrigin = !sub.subscriptionOrigin;

    return needsFeatures || needsOrigin;
  });

  console.log(`📊 Total usuarios: ${usersSnapshot.size}`);
  console.log(`📊 Pro/Lifetime que necesitan migración: ${usersToMigrate.length}`);

  if (usersToMigrate.length === 0) {
    console.log("\n✅ No hay usuarios para migrar.");
    return;
  }

  if (isDryRun) {
    console.log("\n🔍 Usuarios que serían migrados:");
    usersToMigrate.forEach((doc) => {
      const sub = doc.data().subscription;
      const needsFeatures = !sub.features || !(SENTINEL_KEY in sub.features);
      const needsOrigin = !sub.subscriptionOrigin;
      console.log(
        `   - ${doc.id} (${sub.planId}) → features: ${needsFeatures ? "ACTUALIZAR" : "OK"}, origin: ${needsOrigin ? "SETEAR mock_checkout" : sub.subscriptionOrigin}`
      );
    });
    console.log(`\nEjecuta sin --dry-run para aplicar cambios.`);
    return;
  }

  let migratedCount = 0;

  for (let i = 0; i < usersToMigrate.length; i += BATCH_LIMIT) {
    const chunk = usersToMigrate.slice(i, i + BATCH_LIMIT);
    const batch = db.batch();

    for (const doc of chunk) {
      const sub = doc.data().subscription;
      const planFeatures = PLAN_FEATURES[sub.planId];
      const update = {};

      // (a) Merge features faltantes con las del plan
      if (!sub.features || !(SENTINEL_KEY in sub.features)) {
        update["subscription.features"] = { ...planFeatures, ...(sub.features || {}) };
      }

      // (b) Setear subscriptionOrigin si falta
      if (!sub.subscriptionOrigin) {
        update["subscription.subscriptionOrigin"] = "mock_checkout";
      }

      update["subscription.updatedAt"] = new Date().toISOString();

      batch.update(doc.ref, update);
    }

    await batch.commit();
    migratedCount += chunk.length;
    console.log(
      `   Batch ${Math.floor(i / BATCH_LIMIT) + 1}: ${chunk.length} usuarios migrados`
    );
  }

  console.log(
    `\n✅ Migración completada: ${migratedCount} usuarios Pro/Lifetime actualizados.`
  );
}

migrateProFeatures().catch((error) => {
  console.error("❌ Error en migración:", error);
  process.exit(1);
});
