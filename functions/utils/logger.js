/**
 * Structured Logger for Cloud Functions
 * 
 * Outputs JSON that Cloud Logging indexes automatically.
 * Enables queries like:
 *   jsonPayload.function="getHistoricalReturns"
 *   jsonPayload.userId="abc123"
 *   jsonPayload.duration>1000
 * 
 * @see SCALE-CORE-002 - Observabilidad End-to-End
 */

const functions = require('firebase-functions');

class StructuredLogger {
  constructor(functionName, context = {}) {
    this.functionName = functionName;
    this.traceId = context.traceId || this._generateTraceId();
    this.userId = context.userId || 'system';
    this.startTime = Date.now();
  }

  _generateTraceId() {
    return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  _createEntry(severity, message, data = {}) {
    return {
      severity,
      message,
      timestamp: new Date().toISOString(),
      traceId: this.traceId,
      function: this.functionName,
      userId: this.userId,
      elapsedMs: Date.now() - this.startTime,
      ...data,
    };
  }

  debug(message, data) {
    functions.logger.debug(this._createEntry('DEBUG', message, data));
  }
  
  info(message, data) {
    functions.logger.info(this._createEntry('INFO', message, data));
  }
  
  warn(message, data) {
    functions.logger.warn(this._createEntry('WARNING', message, data));
  }
  
  error(message, error, data = {}) {
    const errorData = {
      ...data,
      error: {
        message: error?.message || String(error),
        name: error?.name,
        stack: error?.stack?.split('\n').slice(0, 5),
      },
    };
    functions.logger.error(this._createEntry('ERROR', message, errorData));
  }

  startOperation(name) {
    const opStart = Date.now();
    const self = this;
    
    return {
      success: (data = {}) => {
        self.info(`${name} completed`, {
          operation: name,
          duration: Date.now() - opStart,
          status: 'success',
          ...data,
        });
      },
      failure: (error, data = {}) => {
        self.error(`${name} failed`, error, {
          operation: name,
          duration: Date.now() - opStart,
          status: 'error',
          ...data,
        });
      },
    };
  }

  static forScheduled(functionName) {
    return new StructuredLogger(functionName, {
      userId: 'scheduler',
      traceId: `sched-${Date.now()}`,
    });
  }

  static forCallable(functionName, request) {
    return new StructuredLogger(functionName, {
      userId: request.auth?.uid || 'anonymous',
      traceId: request.rawRequest?.headers?.['x-cloud-trace-context'] || undefined,
    });
  }
}

module.exports = { StructuredLogger };
