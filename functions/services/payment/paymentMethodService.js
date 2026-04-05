const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");

const lsApiKey = defineSecret("LEMONSQUEEZY_API_KEY");

const db = admin.firestore();

const getPaymentMethod = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 15, secrets: [lsApiKey] },
  async (request) => {
    if (!request.auth?.uid) {
      throw new HttpsError("unauthenticated", "Authentication required");
    }

    const userId = request.auth.uid;
    const userDoc = await db.collection("userData").doc(userId).get();
    const subscription = userDoc.data()?.subscription;

    if (!subscription || subscription.planId === "free" || !subscription.subscriptionId) {
      return { paymentMethod: null, reason: "no_active_subscription" };
    }

    if (process.env.PAYMENT_MOCK_ENABLED === "true") {
      return { paymentMethod: null, reason: "mock_mode" };
    }

    const provider = getPaymentProvider();
    const lsCircuit = getCircuit("lemonSqueezy");

    try {
      const details = await lsCircuit.execute(
        () => provider.getSubscription(subscription.subscriptionId),
        () => null
      );

      if (!details) {
        return { paymentMethod: null, reason: "provider_unavailable" };
      }

      return {
        paymentMethod: {
          cardBrand: details.cardBrand || null,
          cardLastFour: details.cardLastFour || null,
        },
      };
    } catch (err) {
      console.warn("[PaymentMethod] Failed to fetch:", err.message);
      return { paymentMethod: null, reason: "fetch_error" };
    }
  }
);

module.exports = { getPaymentMethod };
