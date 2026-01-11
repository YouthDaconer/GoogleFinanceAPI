/**
 * Rate Limiter for Cloud Functions
 * 
 * Implements sliding window rate limiting per user per function.
 * Uses Firestore for distributed state with atomic transactions.
 * 
 * @see SCALE-BE-004 - Rate Limiting Implementation
 */

const { HttpsError } = require('firebase-functions/v2/https');
const admin = require('../services/firebaseAdmin');
const db = admin.firestore();
const { StructuredLogger } = require('./logger');

const RATE_LIMITS_COLLECTION = 'rateLimits';

class RateLimiter {
  constructor(options = {}) {
    this.defaultLimit = options.defaultLimit || 30;
    this.defaultWindowMs = options.defaultWindowMs || 60000;
    this.collection = db.collection(RATE_LIMITS_COLLECTION);
    this.logger = new StructuredLogger('RateLimiter');
  }

  async checkLimit(userId, functionName, options = {}) {
    const limit = options.limit || this.defaultLimit;
    const windowMs = options.windowMs || this.defaultWindowMs;
    
    const key = `${userId}:${functionName}`;
    const now = Date.now();
    const windowStart = now - windowMs;
    
    const docRef = this.collection.doc(key);
    
    return db.runTransaction(async (transaction) => {
      const doc = await transaction.get(docRef);
      
      let data = doc.exists ? doc.data() : { requests: [] };
      
      data.requests = (data.requests || []).filter(ts => ts > windowStart);
      
      if (data.requests.length >= limit) {
        const oldestRequest = Math.min(...data.requests);
        const retryAfter = Math.ceil((oldestRequest + windowMs - now) / 1000);
        
        this.logger.warn('Rate limit exceeded', {
          userId,
          functionName,
          limit,
          currentCount: data.requests.length,
          retryAfter,
        });
        
        throw new HttpsError(
          'resource-exhausted',
          'Rate limit exceeded. Please try again later.',
          {
            limit,
            remaining: 0,
            retryAfter,
            windowEnd: new Date(oldestRequest + windowMs).toISOString(),
          }
        );
      }
      
      data.requests.push(now);
      data.lastUpdated = now;
      transaction.set(docRef, data);
      
      return {
        limit,
        remaining: limit - data.requests.length,
        reset: new Date(now + windowMs).toISOString(),
      };
    });
  }

  async getInfo(userId, functionName, options = {}) {
    const limit = options.limit || this.defaultLimit;
    const windowMs = options.windowMs || this.defaultWindowMs;
    
    const key = `${userId}:${functionName}`;
    const now = Date.now();
    const windowStart = now - windowMs;
    
    const doc = await this.collection.doc(key).get();
    
    if (!doc.exists) {
      return { limit, remaining: limit, reset: new Date(now + windowMs).toISOString() };
    }
    
    const data = doc.data();
    const validRequests = (data.requests || []).filter(ts => ts > windowStart);
    
    return {
      limit,
      remaining: Math.max(0, limit - validRequests.length),
      reset: new Date(now + windowMs).toISOString(),
    };
  }
}

const rateLimiter = new RateLimiter();

function withRateLimit(functionName, options = {}) {
  const { getRateLimitConfig } = require('../config/rateLimits');
  const config = { ...getRateLimitConfig(functionName), ...options };
  
  return function(handler) {
    return async (request) => {
      if (!request.auth) {
        throw new HttpsError('unauthenticated', 'User must be authenticated');
      }
      
      const rateLimitInfo = await rateLimiter.checkLimit(
        request.auth.uid, 
        functionName, 
        config
      );
      
      const result = await handler(request);
      
      if (result && typeof result === 'object' && !Array.isArray(result)) {
        return {
          ...result,
          _rateLimitInfo: rateLimitInfo,
        };
      }
      
      return result;
    };
  };
}

module.exports = { 
  RateLimiter, 
  rateLimiter, 
  withRateLimit,
  RATE_LIMITS_COLLECTION,
};
