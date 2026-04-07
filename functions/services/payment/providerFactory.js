/**
 * Payment Provider Factory — Singleton resolver for payment provider adapters
 *
 * Resolves PAYMENT_PROVIDER env var to a concrete adapter instance.
 * Uses singleton pattern: one provider instance per Cloud Function lifecycle.
 *
 * @see docs/architecture/MIGRATION-WHOP-PAYMENT-GATEWAY-2026-04-06.md
 * @module services/payment/providerFactory
 */

const { createWhopProvider } = require("./whopProvider");

let cachedProvider = null;
let cachedProviderName = null;

function getPaymentProvider() {
  const providerName = process.env.PAYMENT_PROVIDER;

  if (cachedProvider && cachedProviderName === providerName) {
    return cachedProvider;
  }

  switch (providerName) {
    case "whop":
      cachedProvider = createWhopProvider({
        apiKey: process.env.WHOP_API_KEY,
        webhookSecret: process.env.WHOP_WEBHOOK_SECRET,
        companyId: process.env.WHOP_COMPANY_ID,
      });
      cachedProviderName = providerName;
      return cachedProvider;

    case "mock":
      cachedProvider = null;
      cachedProviderName = providerName;
      return null;

    default:
      throw new Error(
        `Unknown PAYMENT_PROVIDER: "${providerName}". Must be "whop" or "mock".`
      );
  }
}

function resetProviderCache() {
  cachedProvider = null;
  cachedProviderName = null;
}

module.exports = { getPaymentProvider, resetProviderCache };
