/**
 * Rate Limits Configuration for all 22 Callable Cloud Functions
 * 
 * Limits per minute by criticality:
 * - 🔴 Costosas (15/min): Cálculos pesados, múltiples queries
 * - 🟡 Normales (30-50/min): Operaciones típicas de usuario
 * - 🟢 Infrecuentes (5-10/min): Settings, configuración
 * 
 * @see SCALE-BE-004 - Rate Limiting Implementation
 */

const RATE_LIMITS = {
  // ═══════════════════════════════════════════════════════════════
  // 🔴 Funciones Costosas - Cálculos Pesados
  // ═══════════════════════════════════════════════════════════════
  getHistoricalReturns: { limit: 15, windowMs: 60000 },
  getMultiAccountHistoricalReturns: { limit: 15, windowMs: 60000 },

  // ═══════════════════════════════════════════════════════════════
  // 🔴 Operaciones de Escritura Críticas
  // ═══════════════════════════════════════════════════════════════
  createAsset: { limit: 30, windowMs: 60000 },
  sellAsset: { limit: 30, windowMs: 60000 },
  sellPartialAssetsFIFO: { limit: 30, windowMs: 60000 },
  addCashTransaction: { limit: 30, windowMs: 60000 },

  // ═══════════════════════════════════════════════════════════════
  // 🟡 Operaciones de Lectura
  // ═══════════════════════════════════════════════════════════════
  getCurrentPricesForUser: { limit: 30, windowMs: 60000 },
  getIndexHistory: { limit: 30, windowMs: 60000 },

  // ═══════════════════════════════════════════════════════════════
  // 🟡 Operaciones de Escritura Normales
  // ═══════════════════════════════════════════════════════════════
  updateAsset: { limit: 50, windowMs: 60000 },
  deleteAsset: { limit: 20, windowMs: 60000 },
  deleteAssets: { limit: 10, windowMs: 60000 },
  updateStockSector: { limit: 20, windowMs: 60000 },

  // ═══════════════════════════════════════════════════════════════
  // 🟢 Settings - Operaciones Infrecuentes
  // ═══════════════════════════════════════════════════════════════
  addCurrency: { limit: 10, windowMs: 60000 },
  updateCurrency: { limit: 10, windowMs: 60000 },
  deleteCurrency: { limit: 10, windowMs: 60000 },
  updateDefaultCurrency: { limit: 10, windowMs: 60000 },
  updateUserCountry: { limit: 5, windowMs: 60000 },
  updateUserDisplayName: { limit: 5, windowMs: 60000 },

  // ═══════════════════════════════════════════════════════════════
  // 🟡 On-Demand Performance (OPT-DEMAND-102)
  // ═══════════════════════════════════════════════════════════════
  getPerformanceOnDemand: { limit: 20, windowMs: 60000 },

  // ═══════════════════════════════════════════════════════════════
  // 🟢 Portfolio Accounts
  // ═══════════════════════════════════════════════════════════════
  addPortfolioAccount: { limit: 10, windowMs: 60000 },
  updatePortfolioAccount: { limit: 20, windowMs: 60000 },
  deletePortfolioAccount: { limit: 5, windowMs: 60000 },
  updatePortfolioAccountBalance: { limit: 30, windowMs: 60000 },
};

const DEFAULT_LIMIT = { limit: 30, windowMs: 60000 };

function getRateLimitConfig(functionName) {
  return RATE_LIMITS[functionName] || DEFAULT_LIMIT;
}

function getAllRateLimits() {
  return { ...RATE_LIMITS };
}

module.exports = { 
  RATE_LIMITS, 
  DEFAULT_LIMIT,
  getRateLimitConfig,
  getAllRateLimits,
};
