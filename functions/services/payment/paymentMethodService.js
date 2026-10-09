const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");

// WHOP-005: Whop secrets wired via onCall secrets option

const db = admin.firestore();

const getPaymentMethod = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 15,
    secrets: ["WHOP_API_KEY", "WHOP_WEBHOOK_SECRET", "WHOP_COMPANY_ID"] },
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
    const paymentCircuit = getCircuit("whop");

    try {
      const details = await paymentCircuit.execute(
        () => provider.getPaymentMethod(subscription.subscriptionId),
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
