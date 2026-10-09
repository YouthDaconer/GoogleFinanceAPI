#!/usr/bin/env node

/**
 * GATE-006: Migración de usuarios existentes al plan Free explícito
 *
 * Añade subscription: buildSubscriptionData('free', 'month', 'active')
 * a todos los usuarios en userData que NO tengan campo 'subscription'.
 *
 * Uso: node scripts/migrateExistingUsersToFreePlan.js [--dry-run]
 *
 * @see docs/stories/GATE-006.story.md (AC-17, AC-18)
 */

const admin = require("../services/firebaseAdmin");
const { buildSubscriptionData } = require("../services/payment/planFeatures");

const db = admin.firestore();
const BATCH_LIMIT = 500;
const isDryRun = process.argv.includes("--dry-run");

async function migrateUsersToFreePlan() {
  console.log(`\n🔄 GATE-006: Migrar usuarios sin subscription al plan Free`);
  console.log(`   Modo: ${isDryRun ? "DRY RUN (sin escrituras)" : "EJECUCIÓN REAL"}\n`);

  const usersSnapshot = await db.collection("userData").get();
  const usersWithoutSubscription = usersSnapshot.docs.filter(
    (doc) => !doc.data().subscription
  );

  console.log(`📊 Total usuarios: ${usersSnapshot.size}`);
  console.log(`📊 Sin subscription: ${usersWithoutSubscription.length}`);

  if (usersWithoutSubscription.length === 0) {
    console.log("\n✅ No hay usuarios para migrar.");
    return;
  }

  if (isDryRun) {
    console.log("\n🔍 Usuarios que serían migrados:");
    usersWithoutSubscription.forEach((doc) => console.log(`   - ${doc.id}`));
    console.log(`\nEjecuta sin --dry-run para aplicar cambios.`);
    return;
  }

  let migratedCount = 0;
  const subscription = await buildSubscriptionData("free", "month", "active");

  for (let i = 0; i < usersWithoutSubscription.length; i += BATCH_LIMIT) {
    const chunk = usersWithoutSubscription.slice(i, i + BATCH_LIMIT);
    const batch = db.batch();

    for (const doc of chunk) {
      batch.update(doc.ref, { subscription });
    }

    await batch.commit();
    migratedCount += chunk.length;
    console.log(`   Batch ${Math.floor(i / BATCH_LIMIT) + 1}: ${chunk.length} usuarios migrados`);
  }

  console.log(`\n✅ Migración completada: ${migratedCount} usuarios migrados al plan Free.`);
}

migrateUsersToFreePlan().catch((error) => {
  console.error("❌ Error en migración:", error);
  process.exit(1);
});
