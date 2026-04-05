/**
 * Cancel Subscription Service — Cloud Function Callable
 *
 * Permite a un usuario cancelar/degradar su suscripción Pro.
 * - Mock mode: Degrada a Free inmediatamente via buildSubscriptionData
 * - Real mode: Comunica cancelación a LS vía provider + circuit breaker
 *
 * Rechaza si el usuario es Free (nada que cancelar) o Lifetime (no cancelable).
 *
 * @see docs/architecture/FEAT-GATE-001-feature-gating-subscription-plans-design.md
 * @module services/payment/cancelSubscriptionService
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const admin = require("../firebaseAdmin");
const { buildSubscriptionData } = require("./planFeatures");
const { getPaymentProvider } = require("./providerFactory");
const { getCircuit } = require("../../utils/circuitBreaker");
const { sendTransactionalEmail, EMAIL_TEMPLATES } = require("./emailService");

const lsApiKey = defineSecret("LEMONSQUEEZY_API_KEY");

// PAY-CANCEL-001: Closed enum for cancellation reasons
const VALID_REASONS = [
  "too_expensive", "not_using_features", "found_alternative",
  "temporary_need", "technical_issues", "other",
];
const MAX_COMMENT_LENGTH = 500;

const cancelSubscription = onCall(
  { cors: true, memory: "256MiB", timeoutSeconds: 30, secrets: [lsApiKey, "AWS_SES_ACCESS_KEY_ID", "AWS_SES_SECRET_ACCESS_KEY"] },
  async (request) => {
    if (!request.auth?.uid) {
      throw new HttpsError("unauthenticated", "Authentication required");
    }

    const userId = request.auth.uid;
    const db = admin.firestore();

    // Leer suscripción actual
    const userDoc = await db.collection("userData").doc(userId).get();
    const userData = userDoc.data();
    const subscription = userData?.subscription;

    if (!subscription || subscription.planId === "free") {
      throw new HttpsError(
        "failed-precondition",
        "No active subscription to cancel"
      );
    }

    if (subscription.planId === "lifetime") {
      throw new HttpsError(
        "failed-precondition",
        "Lifetime plans cannot be canceled"
      );
    }

    // PAY-CANCEL-001: Extract & validate cancellation reason
    const { cancellationReason, cancellationComment } = request.data || {};

    let validatedReason = null;
    let validatedComment = null;

    if (cancellationReason) {
      if (!VALID_REASONS.includes(cancellationReason)) {
        throw new HttpsError("invalid-argument", "Invalid cancellation reason");
      }
      validatedReason = cancellationReason;
    }

    if (cancellationComment && typeof cancellationComment === "string") {
      validatedComment = cancellationComment.trim().slice(0, MAX_COMMENT_LENGTH);
    }

    const isMockMode = process.env.PAYMENT_MOCK_ENABLED === "true";

    if (isMockMode) {
      // PAY-MOCK-001: Grace period for mock scheduled cancel (read per-request for testability)
      const mockGracePeriodSeconds =
        parseInt(process.env.MOCK_CANCEL_GRACE_SECONDS || "0", 10) || 0;

      // PAY-MOCK-001: Scheduled cancel (simulates real gateway behavior)
      if (mockGracePeriodSeconds > 0) {
        const gracePeriodEnd = new Date(
          Date.now() + mockGracePeriodSeconds * 1000
        ).toISOString();

        await db.collection("userData").doc(userId).set(
          {
            subscription: {
              cancelAtPeriodEnd: true,
              currentPeriodEnd: gracePeriodEnd,
              updatedAt: new Date().toISOString(),
              ...(validatedReason && {
                cancellationReason: validatedReason,
                cancellationComment: validatedComment,
                cancelledAt: new Date().toISOString(),
              }),
            },
          },
          { merge: true }
        );

        try {
          await db.collection("subscriptionEvents").doc().set({
            type: "SUBSCRIPTION_CANCEL_SCHEDULED",
            userId,
            planId: subscription.planId,
            gracePeriodEnd,
            reason: validatedReason,
            comment: validatedComment,
            mode: "mock-scheduled",
            processedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
        } catch (err) {
          console.error("[Cancel] Failed to write audit event (mock-scheduled):", err.message);
        }

        try {
          const userEmail = request.auth.token?.email;
          if (userEmail) {
            await sendTransactionalEmail(EMAIL_TEMPLATES.SUBSCRIPTION_CANCELLED, userEmail, {
              userName: userData?.displayName || userEmail.split("@")[0],
              planName: subscription.planId === "pro" ? "Pro" : subscription.planId,
              effectiveDate: gracePeriodEnd,
              cancellationReason: validatedReason,
            });
          }
        } catch (emailErr) {
          console.error("[Cancel] Email side-effect failed (mock-scheduled):", emailErr.message);
        }

        return {
          success: true,
          effectiveDate: gracePeriodEnd,
          newPlan: "free",
          message: `Subscription will cancel at end of mock period: ${gracePeriodEnd}`,
        };
      }

      // Mock immediate cancel (MOCK_CANCEL_GRACE_SECONDS=0 or undefined)
      const freeSubscription = await buildSubscriptionData("free", "month");

      // PAY-009: Preservar trial history (campos sticky)
      if (subscription.hasUsedTrial) {
        freeSubscription.hasUsedTrial = true;
      }
      if (subscription.trialStartedAt) {
        freeSubscription.trialStartedAt = subscription.trialStartedAt;
      }
      if (subscription.subscriptionOrigin === "trial") {
        freeSubscription.trialEndedAt = new Date().toISOString();
      }

      if (validatedReason) {
        freeSubscription.cancellationReason = validatedReason;
        freeSubscription.cancellationComment = validatedComment;
        freeSubscription.cancelledAt = new Date().toISOString();
      }

      await db
        .collection("userData")
        .doc(userId)
        .set({ subscription: freeSubscription }, { merge: true });

      try {
        await db.collection("subscriptionEvents").doc().set({
          type: "SUBSCRIPTION_CANCELLED",
          userId,
          planId: subscription.planId,
          reason: validatedReason,
          comment: validatedComment,
          mode: "mock",
          processedAt: admin.firestore.FieldValue.serverTimestamp(),
        });
      } catch (err) {
        console.error("[Cancel] Failed to write audit event (mock):", err.message);
      }

      try {
        const userEmail = request.auth.token?.email;
        if (userEmail) {
          await sendTransactionalEmail(EMAIL_TEMPLATES.SUBSCRIPTION_CANCELLED, userEmail, {
            userName: userData?.displayName || userEmail.split("@")[0],
            planName: subscription.planId === "pro" ? "Pro" : subscription.planId,
            effectiveDate: new Date().toISOString(),
            cancellationReason: validatedReason,
          });
        }
      } catch (emailErr) {
        console.error("[Cancel] Email side-effect failed (mock):", emailErr.message);
      }

      return {
        success: true,
        effectiveDate: new Date().toISOString(),
        newPlan: "free",
        message: "Subscription canceled — downgraded to Free (mock mode, immediate)",
      };
    }

    // Real mode: cancelar vía provider + circuit breaker, fallback a Firestore-only
    const subscriptionId = subscription.subscriptionId;
    if (!subscriptionId) {
      throw new HttpsError(
        "failed-precondition",
        "No subscription ID found — cannot cancel with payment provider"
      );
    }

    const provider = getPaymentProvider();
    const lsCircuit = getCircuit("lemonSqueezy");

    let providerSuccess = false;
    try {
      const result = await lsCircuit.execute(
        () => provider.cancelSubscription(subscriptionId),
        () => {
          console.warn("[Cancel] LS circuit open — fallback to Firestore-only");
          return { success: false, fallback: true };
        }
      );
      providerSuccess = result.success && !result.fallback;
    } catch (err) {
      console.warn("[Cancel] LS API error — fallback:", err.message);
    }

    const effectiveDate = subscription.currentPeriodEnd || new Date().toISOString();
    await db
      .collection("userData")
      .doc(userId)
      .set(
        {
          subscription: {
            cancelAtPeriodEnd: true,
            updatedAt: new Date().toISOString(),
            ...(validatedReason && {
              cancellationReason: validatedReason,
              cancellationComment: validatedComment,
              cancelledAt: new Date().toISOString(),
            }),
          },
        },
        { merge: true }
      );

    try {
      await db.collection("subscriptionEvents").doc().set({
        type: "SUBSCRIPTION_CANCELLED",
        userId,
        planId: subscription.planId,
        reason: validatedReason,
        comment: validatedComment,
        mode: "real",
        processedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    } catch (err) {
      console.error("[Cancel] Failed to write audit event (real):", err.message);
    }

    const logMsg = providerSuccess
      ? "[Cancel] LS API confirmed cancel"
      : "[Cancel] Firestore-only cancel (LS API failed or unavailable)";
    console.log(logMsg);

    try {
      const userEmail = request.auth.token?.email;
      if (userEmail) {
        await sendTransactionalEmail(EMAIL_TEMPLATES.SUBSCRIPTION_CANCELLED, userEmail, {
          userName: userData?.displayName || userEmail.split("@")[0],
          planName: subscription.planId === "pro" ? "Pro" : subscription.planId,
          effectiveDate,
          cancellationReason: validatedReason,
        });
      }
    } catch (emailErr) {
      console.error("[Cancel] Email side-effect failed (real):", emailErr.message);
    }

    return {
      success: true,
      effectiveDate,
      newPlan: "free",
      message: `Subscription will be canceled at end of period: ${effectiveDate}`,
    };
  }
);

module.exports = { cancelSubscription };
