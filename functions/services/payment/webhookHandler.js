/**
 * Webhook Handler — HTTP endpoint for Whop Standard Webhooks
 *
 * Validates Standard Webhooks signature via provider.parseWebhook(),
 * deduplicates via subscriptionEvents/{eventId} check, resolves Firebase UID
 * via metadata or email fallback, and delegates to processWebhookEvent().
 *
 * Idempotency: check .get() → process → .set() (no transaction needed —
 * eventId is deterministic from Standard Webhooks `webhook-id`).
 *
 * @see docs/architecture/MIGRATION-WHOP-PAYMENT-GATEWAY-2026-04-06.md § 4.5
 * @module services/payment/webhookHandler
 */

const admin = require("../firebaseAdmin");
const { getPaymentProvider } = require("./providerFactory");
const { processWebhookEvent } = require("./subscriptionService");
const { PAYMENT_EVENT_TYPES } = require("./paymentProvider");
const { sendTransactionalEmail, EMAIL_TEMPLATES } = require("./emailService");

const db = admin.firestore();

const FIREBASE_UID_REGEX = /^[a-zA-Z0-9]{20,128}$/;

async function resolveFirebaseUid(data) {
  const metadataUid = data?.metadata?.firebase_uid;
  if (metadataUid) {
    if (!FIREBASE_UID_REGEX.test(metadataUid)) {
      console.warn("[Webhook] Invalid firebase_uid format:", metadataUid?.slice(0, 32));
      return null;
    }
    return metadataUid;
  }

  const email = data?.user?.email;
  if (email) {
    try {
      const userRecord = await admin.auth().getUserByEmail(email);
      return userRecord.uid;
    } catch (err) {
      console.warn("[Webhook] resolveFirebaseUid email fallback failed:", err.message);
    }
  }

  return null;
}

async function handleWebhook(req, res) {
  const provider = getPaymentProvider();
  if (!provider) {
    res.status(200).json({ received: true, mode: "mock" });
    return;
  }

  const headers = {
    "webhook-id": req.headers["webhook-id"],
    "webhook-signature": req.headers["webhook-signature"],
    "webhook-timestamp": req.headers["webhook-timestamp"],
  };

  let webhookResult;
  try {
    const rawBody = req.rawBody.toString("utf8");
    webhookResult = await provider.parseWebhook(rawBody, headers);
  } catch (err) {
    console.error("[Webhook] Invalid signature:", err.message);
    console.error("[Webhook] Headers received:", JSON.stringify({
      "webhook-id": headers["webhook-id"]?.slice(0, 20),
      "webhook-signature": headers["webhook-signature"]?.slice(0, 30),
      "webhook-timestamp": headers["webhook-timestamp"],
    }));
    res.status(400).json({ error: "Invalid webhook signature" });
    return;
  }

  const { type, data, rawType } = webhookResult;
  const eventId = webhookResult.eventId
    ? String(webhookResult.eventId).replace(/[\/\\.]/g, "_").slice(0, 128)
    : null;

  if (type.startsWith("UNKNOWN_")) {
    console.log(`[Webhook] Unmapped event: ${rawType}`);
    res.status(200).json({ received: true, processed: false });
    return;
  }

  try {
    if (eventId) {
      const eventRef = db.collection("subscriptionEvents").doc(eventId);
      const eventDoc = await eventRef.get();
      if (eventDoc.exists) {
        console.log(`[Webhook] Duplicate: ${eventId} (${type})`);
        res.status(200).json({ received: true, duplicate: true });
        return;
      }
    }

    const userId = await resolveFirebaseUid(data);

    if (!userId) {
      console.warn(`[Webhook] No Firebase UID for event ${eventId} (${type})`);
      if (eventId) {
        await db.collection("subscriptionEvents").doc(eventId).set({
          type,
          rawType,
          userId: null,
          provider: "whop",
          timestamp: new Date().toISOString(),
          processedAt: new Date().toISOString(),
          result: "skipped",
          reason: "no_uid",
        });
      }
      res.status(200).json({ received: true, skipped: "no_firebase_uid" });
      return;
    }

    await processWebhookEvent({ type, data, userId, eventId });

    if (eventId) {
      await db.collection("subscriptionEvents").doc(eventId).set({
        type,
        rawType,
        userId,
        provider: "whop",
        timestamp: new Date().toISOString(),
        processedAt: new Date().toISOString(),
        result: "success",
      });
    }

    console.log(`[Webhook] Processed: ${type} eventId=${eventId} userId=${userId}`);

    try {
      const userDoc = await db.collection("userData").doc(userId).get();
      const email = userDoc.data()?.email;

      if (email) {
        const emailOpts = { maxRetries: 0, timeoutMs: 5000 };
        switch (type) {
          case PAYMENT_EVENT_TYPES.PAYMENT_FAILED:
            await sendTransactionalEmail(EMAIL_TEMPLATES.PAYMENT_FAILED, email, {
              userName: userDoc.data()?.displayName || email.split("@")[0],
              updatePaymentUrl: `${process.env.APP_URL || "https://portastock.net"}/settings`,
            }, emailOpts);
            break;

          case PAYMENT_EVENT_TYPES.PAYMENT_SUCCEEDED:
            await sendTransactionalEmail(EMAIL_TEMPLATES.PAYMENT_RENEWED, email, {
              userName: userDoc.data()?.displayName || email.split("@")[0],
              planName: "Pro",
              amount: null,
            }, emailOpts);
            break;
        }
      }
    } catch (emailErr) {
      console.error("[Webhook] Email side-effect failed:", emailErr.message);
    }

    res.status(200).json({ received: true });
  } catch (err) {
    console.error("[Webhook] Processing error:", err.message);
    res.status(500).json({ error: "processing_failed" });
  }
}

module.exports = { handleWebhook, resolveFirebaseUid };
