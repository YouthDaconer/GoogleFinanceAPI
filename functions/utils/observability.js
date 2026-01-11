/**
 * Observability Wrappers for Cloud Functions
 * 
 * HOC patterns for automatic logging and metrics
 * 
 * @see SCALE-CORE-002 - Observabilidad End-to-End
 */

const { StructuredLogger } = require('./logger');

/**
 * HOC for callable functions with observability
 * 
 * @param {string} functionName - Name of the function for logging
 * @param {Function} handler - Async handler (request, logger) => result
 * @returns {Function} Wrapped handler
 * 
 * @example
 * exports.myFunction = onCall(
 *   { cors: true },
 *   withObservability('myFunction', async (request, logger) => {
 *     logger.info('Processing', { data: request.data });
 *     return result;
 *   })
 * );
 */
function withObservability(functionName, handler) {
  return async (request) => {
    const logger = StructuredLogger.forCallable(functionName, request);
    const mainOp = logger.startOperation('execution');
    
    try {
      logger.info('Function invoked', {
        hasAuth: !!request.auth,
        dataKeys: Object.keys(request.data || {}),
      });
      
      const result = await handler(request, logger);
      
      const resultSize = result ? JSON.stringify(result).length : 0;
      mainOp.success({ 
        resultType: typeof result,
        resultSize: resultSize > 10000 ? '>10KB' : resultSize,
      });
      
      return result;
    } catch (error) {
      mainOp.failure(error);
      throw error;
    }
  };
}

/**
 * HOC for scheduled functions with observability
 * 
 * @param {string} functionName - Name of the function for logging
 * @param {Function} handler - Async handler (event, logger) => void
 * @returns {Function} Wrapped handler
 * 
 * @example
 * exports.myScheduledFunction = onSchedule(
 *   { schedule: 'every 5 minutes' },
 *   withScheduledObservability('myScheduledFunction', async (event, logger) => {
 *     logger.info('Starting scheduled task');
 *     // ... implementation
 *   })
 * );
 */
function withScheduledObservability(functionName, handler) {
  return async (event) => {
    const logger = StructuredLogger.forScheduled(functionName);
    const mainOp = logger.startOperation('scheduled_execution');
    
    try {
      logger.info('Scheduled function started');
      await handler(event, logger);
      mainOp.success();
    } catch (error) {
      mainOp.failure(error);
      throw error;
    }
  };
}

/**
 * Track external API calls with timing
 * 
 * @param {StructuredLogger} logger - Logger instance
 * @param {string} apiName - Name of the API being called
 * @param {Function} operation - Async operation to execute
 * @returns {Promise<any>} Result of the operation
 * 
 * @example
 * const data = await trackExternalAPI(logger, 'finance-query', async () => {
 *   return await axios.get(apiUrl);
 * });
 */
async function trackExternalAPI(logger, apiName, operation) {
  const timer = logger.startOperation(`${apiName}.call`);
  
  try {
    const result = await operation();
    timer.success({ api: apiName });
    return result;
  } catch (error) {
    timer.failure(error, { api: apiName });
    throw error;
  }
}

module.exports = { 
  withObservability, 
  withScheduledObservability,
  trackExternalAPI 
};
