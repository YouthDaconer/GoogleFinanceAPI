const { DateTime } = require('luxon');

const RISK_CACHE_MAX_SIZE = 500;
const MARKET_CACHE_TTL_MS = 5 * 60 * 1000;
const NYSE_OPEN_HOUR = 9;
const NYSE_OPEN_MINUTE = 30;
const NYSE_CLOSE_HOUR = 16;

const cache = new Map();

function isNYSEMarketOpen() {
  const nyNow = DateTime.now().setZone('America/New_York');
  const dayOfWeek = nyNow.weekday;

  if (dayOfWeek === 6 || dayOfWeek === 7) {
    return false;
  }

  const currentMinutes = nyNow.hour * 60 + nyNow.minute;
  const openMinutes = NYSE_OPEN_HOUR * 60 + NYSE_OPEN_MINUTE;
  const closeMinutes = NYSE_CLOSE_HOUR * 60;

  return currentMinutes >= openMinutes && currentMinutes < closeMinutes;
}

function calculateTTLUntilNextEOD() {
  const nyNow = DateTime.now().setZone('America/New_York');
  let nextEOD = nyNow.set({ hour: NYSE_CLOSE_HOUR, minute: 0, second: 0, millisecond: 0 });

  if (nyNow >= nextEOD) {
    nextEOD = nextEOD.plus({ days: 1 });
  }

  while (nextEOD.weekday === 6 || nextEOD.weekday === 7) {
    nextEOD = nextEOD.plus({ days: 1 });
  }

  return Math.max(nextEOD.toMillis() - nyNow.toMillis(), MARKET_CACHE_TTL_MS);
}

function getRiskCacheTTL() {
  return isNYSEMarketOpen() ? MARKET_CACHE_TTL_MS : calculateTTLUntilNextEOD();
}

function buildRiskCacheKey(userId, period, currency, accountIds) {
  const sorted = accountIds && accountIds.length > 0
    ? [...accountIds].sort().join(',')
    : 'overall';
  return `${userId}_${period}_${currency}_${sorted}`;
}

function getCachedRiskMetrics(cacheKey) {
  const entry = cache.get(cacheKey);
  if (!entry) return null;

  if (Date.now() - entry.timestamp >= getRiskCacheTTL()) {
    cache.delete(cacheKey);
    return null;
  }

  cache.delete(cacheKey);
  cache.set(cacheKey, entry);

  return entry.data;
}

function setCachedRiskMetrics(cacheKey, data) {
  cache.set(cacheKey, { data, timestamp: Date.now() });

  if (cache.size > RISK_CACHE_MAX_SIZE) {
    const oldestKey = cache.keys().next().value;
    cache.delete(oldestKey);
  }
}

function clearCache() {
  cache.clear();
}

function getCacheSize() {
  return cache.size;
}

module.exports = {
  buildRiskCacheKey,
  getCachedRiskMetrics,
  setCachedRiskMetrics,
  getRiskCacheTTL,
  isNYSEMarketOpen,
  calculateTTLUntilNextEOD,
  clearCache,
  getCacheSize,
  RISK_CACHE_MAX_SIZE,
  MARKET_CACHE_TTL_MS,
};
