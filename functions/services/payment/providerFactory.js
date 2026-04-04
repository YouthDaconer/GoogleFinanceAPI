/**
 * Payment Provider Factory — Singleton resolver for payment provider adapters
 *
 * Resolves PAYMENT_PROVIDER env var to a concrete adapter instance.
 * Uses singleton pattern: one provider instance per Cloud Function lifecycle.
 *
 * @see docs/architecture/STRIPE-001-payment-subscription-integration-design.md
 * @module services/payment/providerFactory
 */

const { createLemonSqueezyProvider } = require("./lemonSqueezyProvider");

let cachedProvider = null;
let cachedProviderName = null;

function getPaymentProvider() {
  const providerName = process.env.PAYMENT_PROVIDER;

  if (cachedProvider && cachedProviderName === providerName) {
    return cachedProvider;
  }

  switch (providerName) {
    case "lemonsqueezy":
      cachedProvider = createLemonSqueezyProvider();
      cachedProviderName = providerName;
      return cachedProvider;

    case "mock":
      cachedProvider = null;
      cachedProviderName = providerName;
      return null;

    default:
      throw new Error(
        `Unknown PAYMENT_PROVIDER: "${providerName}". Must be "lemonsqueezy" or "mock".`
      );
  }
}

function resetProviderCache() {
  cachedProvider = null;
  cachedProviderName = null;
}

module.exports = { getPaymentProvider, resetProviderCache };
