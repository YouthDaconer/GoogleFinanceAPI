/**
 * GATE-006: Subscription Validator — Validación de features por plan
 *
 * Helper reutilizable para Cloud Functions que verifican acceso a features premium.
 * Usa admin SDK (bypasa Security Rules).
 *
 * @module services/helpers/subscriptionValidator
 */

const { getFirestore } = require("firebase-admin/firestore");
const { HttpsError } = require("firebase-functions/v2/https");
const { PLAN_FEATURES } = require("../payment/planFeatures");

const db = getFirestore();

async function validateFeatureAccess(userId, featureKey) {
  const userDoc = await db.collection("userData").doc(userId).get();
  const features = userDoc.data()?.subscription?.features ?? PLAN_FEATURES.free;

  if (!features[featureKey]) {
    throw new HttpsError(
      "permission-denied",
      "Esta funcionalidad requiere un plan Pro activo."
    );
  }
}

async function validateQuantityLimit(userId, featureKey, collection, countFilters = {}) {
  const userDoc = await db.collection("userData").doc(userId).get();
  const limit = userDoc.data()?.subscription?.features?.[featureKey] ?? PLAN_FEATURES.free[featureKey];

  let query = db.collection(collection).where("userId", "==", userId);
  for (const [key, value] of Object.entries(countFilters)) {
    query = query.where(key, "==", value);
  }

  const countResult = await query.count().get();
  if (countResult.data().count >= limit) {
    throw new HttpsError(
      "resource-exhausted",
      `Límite de ${limit} alcanzado. Actualiza a Pro para acceso ilimitado.`
    );
  }
}

module.exports = { validateFeatureAccess, validateQuantityLimit };
