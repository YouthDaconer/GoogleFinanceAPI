/**
 * Webhook Handler — HTTP endpoint for Lemon Squeezy webhook events
 *
 * Validates HMAC signature, deduplicates via subscriptionEvents/{eventId}
 * Firestore transaction, and delegates processing to subscriptionService.
 *
 * Idempotency: db.runTransaction() atomizes check + process + register.
 * Error strategy: 400 for HMAC failures (no retry), 500 for transient errors (LS retries 72h).
 *
 * @see docs/architecture/AUDIT-SUBSCRIPTION-PAYMENT-SYSTEM-2026-04-03.md § 8.3
 * @module services/payment/webhookHandler
 */

const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { processWebhookEvent } = require("./subscriptionService");
const { PAYMENT_EVENT_TYPES } = require("./paymentProvider");

const db = admin.firestore();

function extractEventId(webhookResult) {
  return webhookResult.eventId
    || webhookResult.rawData?.meta?.webhook_event_id
    || null;
}

function deriveAfterState(webhookResult) {
  switch (webhookResult.type) {
    case PAYMENT_EVENT_TYPES.CHECKOUT_COMPLETED:
    case PAYMENT_EVENT_TYPES.SUBSCRIPTION_CREATED:
      return { planId: webhookResult.planId || "pro", status: "active" };
    case PAYMENT_EVENT_TYPES.SUBSCRIPTION_UPDATED:
      return { planId: null, status: "active" };
    case PAYMENT_EVENT_TYPES.SUBSCRIPTION_CANCELED:
      return { planId: "free", status: "canceled" };
    case PAYMENT_EVENT_TYPES.PAYMENT_FAILED:
      return { planId: null, status: "past_due" };
    case PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED:
      return { planId: null, status: "active" };
    default:
      return { planId: null, status: null };
  }
}

async function handleWebhook(req, res) {
  const signature = req.headers["x-signature"];
  if (!signature) {
    res.status(400).json({ error: "Missing x-signature header" });
    return;
  }

  const provider = getPaymentProvider();
  if (!provider) {
    res.status(500).json({ error: "Payment provider is not configured" });
    return;
  }

  let webhookResult;
  try {
    webhookResult = await provider.parseWebhook(req.rawBody, signature);
  } catch (err) {
    console.error("[Webhook] Firma inválida:", err.message);
    res.status(400).json({ error: "Invalid webhook signature" });
    return;
  }

  if (webhookResult.type === "unknown") {
    console.log(`[Webhook] Evento no mapeado: ${webhookResult.rawData?.meta?.event_name}`);
    res.status(200).json({ received: true, processed: false });
    return;
  }

  const rawEventId = extractEventId(webhookResult);
  const eventId = rawEventId
    ? String(rawEventId).replace(/[\/\\]/g, "_").slice(0, 128)
    : null;

  try {
    if (!eventId) {
      console.warn("[Webhook] Sin eventId — procesando sin idempotencia");
      await processWebhookEvent(webhookResult);
      res.status(200).json({ received: true });
      return;
    }

    const result = await db.runTransaction(async (transaction) => {
      const eventRef = db.doc(`subscriptionEvents/${eventId}`);
      const eventDoc = await transaction.get(eventRef);

      if (eventDoc.exists) {
        console.log(`[Webhook] Deduplicado: ${eventId} (${webhookResult.type})`);
        return { deduplicated: true };
      }

      const userRef = webhookResult.userId
        ? db.collection("userData").doc(webhookResult.userId)
        : null;
      let beforeState = { planId: null, status: null };
      if (userRef) {
        const userDoc = await transaction.get(userRef);
        const sub = userDoc.exists ? userDoc.data()?.subscription : null;
        beforeState = {
          planId: sub?.planId || null,
          status: sub?.status || null,
        };
      }

      await processWebhookEvent(webhookResult, transaction);

      const afterState = deriveAfterState(webhookResult);

      transaction.set(eventRef, {
        eventId,
        type: webhookResult.type,
        userId: webhookResult.userId || null,
        processedAt: admin.firestore.FieldValue.serverTimestamp(),
        raw: { meta: webhookResult.rawData?.meta || {} },
        before: beforeState,
        after: afterState,
      });

      return { deduplicated: false };
    });

    const logPrefix = result.deduplicated ? "Deduplicado" : "Procesado OK";
    console.log(`[Webhook] ${logPrefix}: ${webhookResult.type} eventId=${eventId}`);
    res.status(200).json({ received: true });
  } catch (err) {
    console.error("[Webhook] Error procesando:", err.message);
    res.status(500).json({ error: "Webhook processing failed" });
  }
}

module.exports = { handleWebhook, extractEventId, deriveAfterState };
