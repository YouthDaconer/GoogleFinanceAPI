const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");

// WHOP-005: Whop secrets wired via onCall secrets option
const getSubscriptionInvoices = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 30,
    secrets: ["WHOP_API_KEY", "WHOP_WEBHOOK_SECRET", "WHOP_COMPANY_ID"] },
  async (request) => {
    if (!request.auth?.uid) {
      throw new HttpsError("unauthenticated", "Authentication required");
    }

    const userId = request.auth.uid;
    const db = admin.firestore();
    const userDoc = await db.collection("userData").doc(userId).get();
    const subscription = userDoc.data()?.subscription;

    if (!subscription || subscription.planId === "free") {
      return { invoices: [], source: "none" };
    }

    const subscriptionId = subscription.subscriptionId;
    if (subscriptionId && process.env.PAYMENT_MOCK_ENABLED !== "true") {
      try {
        const provider = getPaymentProvider();
        if (provider) {
          const paymentCircuit = getCircuit("whop");

          const providerInvoices = await paymentCircuit.execute(
            () => provider.getSubscriptionInvoices(subscriptionId),
            () => null
          );

          if (providerInvoices && providerInvoices.length > 0) {
            console.log(`[Invoices] Provider API returned ${providerInvoices.length} invoices for sub ${subscriptionId}`);
            return {
              invoices: providerInvoices.map((inv) => ({
                id: inv.id,
                date: inv.createdAt,
                amount: inv.total,
                amountFormatted: inv.totalFormatted,
                currency: inv.currency,
                status: inv.status,
                invoiceUrl: inv.invoiceUrl,
                cardBrand: inv.cardBrand || null,
                cardLastFour: inv.cardLastFour || null,
              })),
              source: "provider",
            };
          }
        }
      } catch (err) {
        console.warn("[Invoices] Provider API failed — falling back to events:", err.message);
      }
    }

    try {
      const eventsSnapshot = await db
        .collection("subscriptionEvents")
        .where("userId", "==", userId)
        .orderBy("processedAt", "desc")
        .limit(20)
        .get();

      const events = eventsSnapshot.docs.map((doc) => {
        const data = doc.data();
        return {
          id: doc.id,
          date: data.processedAt?.toDate?.()?.toISOString() || null,
          type: data.type,
          amount: null,
          amountFormatted: null,
          currency: null,
          status: "processed",
          invoiceUrl: null,
        };
      });

      return { invoices: events, source: "events" };
    } catch (err) {
      console.warn("[Invoices] subscriptionEvents query failed (missing composite index?):", err.message);
      return { invoices: [], source: "none" };
    }
  }
);

module.exports = { getSubscriptionInvoices };
