/**
 * Payment Provider — Interface contract, JSDoc types and event constants
 *
 * Defines the abstract interface for payment provider adapters.
 * Concrete implementations (lemonSqueezyProvider) implement this contract.
 *
 * @see docs/architecture/STRIPE-001-payment-subscription-integration-design.md
 * @module services/payment/paymentProvider
 */

/**
 * @typedef {'trial' | 'mock_checkout' | 'checkout'} SubscriptionOrigin
 */

/**
 * @typedef {Object} CreateCheckoutOptions
 * @property {string} userId - Firebase UID
 * @property {string} email - User email for pre-filling checkout
 * @property {string} planId - "pro" | "lifetime"
 * @property {string} interval - "month" | "year" | "lifetime"
 * @property {string} [successUrl] - Redirect URL after successful checkout
 * @property {string} [cancelUrl] - Redirect URL if user cancels checkout
 */

/**
 * @typedef {Object} CheckoutResult
 * @property {string} checkoutUrl - URL to redirect the user to
 */

/**
 * @typedef {Object} PortalResult
 * @property {string} portalUrl - URL to the customer billing portal
 */

/**
 * @typedef {Object} NormalizedSubscription
 * @property {string} subscriptionId - Provider subscription ID
 * @property {string} customerId - Provider customer ID
 * @property {string} planId - "pro" | "lifetime"
 * @property {string} status - Provider subscription status
 * @property {string} interval - "month" | "year" | "lifetime"
 * @property {string|null} currentPeriodEnd - ISO date of period end
 * @property {boolean} cancelAtPeriodEnd - Whether subscription cancels at period end
 */

/**
 * @typedef {Object} WebhookResult
 * @property {string} type - One of PAYMENT_EVENT_TYPES values
 * @property {string} userId - Firebase UID from customData.user_id
 * @property {string} planId - "pro" | "lifetime"
 * @property {string} interval - "month" | "year" | "lifetime"
 * @property {string|null} subscriptionId - Provider subscription ID
 * @property {string|null} customerId - Provider customer ID
 * @property {Object} rawData - Raw webhook payload for audit trail
 */

/**
 * @typedef {Object} CancelSubscriptionResult
 * @property {boolean} success - Whether the cancellation was acknowledged
 * @property {string|null} [effectiveDate] - ISO date when subscription ends
 * @property {boolean} [alreadyCanceled] - True if subscription was already canceled in provider
 */

/**
 * @typedef {Object} NormalizedInvoice
 * @property {string} id - Invoice ID (stringified)
 * @property {string} createdAt - ISO date of invoice creation
 * @property {number} total - Amount in cents
 * @property {string} totalFormatted - Formatted amount (e.g. "$9.99")
 * @property {string} currency - Currency code (e.g. "USD")
 * @property {string} status - "paid" | "pending" | "refunded" | "void"
 * @property {string|null} invoiceUrl - URL to invoice PDF
 * @property {string|null} cardBrand - Card brand (e.g. "visa")
 * @property {string|null} cardLastFour - Last four digits of card
 */

/**
 * @typedef {Object} PaymentProvider
 * @property {function(CreateCheckoutOptions): Promise<CheckoutResult>} createCheckoutSession
 * @property {function(string): Promise<PortalResult>} createPortalSession
 * @property {function(string): Promise<NormalizedSubscription>} getSubscription
 * @property {function(string): Promise<CancelSubscriptionResult>} cancelSubscription
 * @property {function(string): Promise<NormalizedInvoice[]>} getSubscriptionInvoices
 * @property {function(string, string): Promise<WebhookResult>} parseWebhook
 */

const PAYMENT_EVENT_TYPES = Object.freeze({
  CHECKOUT_COMPLETED: "checkout_completed",
  SUBSCRIPTION_UPDATED: "subscription_updated",
  SUBSCRIPTION_CANCELED: "subscription_canceled",
  SUBSCRIPTION_CREATED: "subscription_created",
  PAYMENT_FAILED: "payment_failed",
  PAYMENT_SUCCEEDED: "payment_succeeded",
});

module.exports = { PAYMENT_EVENT_TYPES };
