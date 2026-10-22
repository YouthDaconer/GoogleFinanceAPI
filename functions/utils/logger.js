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

/**
 * HU 6.3 — Adaptador de `console.*` con la forma que espera un `StructuredLogger`.
 *
 * Hay módulos antiguos que reciben un logger y lo usan sin comprobar que exista
 * (`updateBenchmarkSnapshots`) o que lo guardan en una global que sólo poblaba
 * el job programado (`unifiedMarketDataUpdate`). Al llamarlos desde código
 * nuevo —que por estándar (§7) registra con `console.*` y prefijo `[unidad]`—
 * hace falta un puente. Este es ese puente, en un solo sitio: lo usan la tarea
 * por inversor y el cierre póstumo.
 *
 * @param {string} prefijo - La unidad, sin corchetes. Ej.: `eodFinalization`
 * @returns {{debug: Function, info: Function, warn: Function, error: Function}}
 * @see platform-docs/stories/6.3-cierre-nocturno-termina-siempre/refinamiento.md (T3, T6)
 */
function crearLoggerDeConsola(prefijo) {
  return {
    // `debug` se descarta a propósito: en producción es ruido, y el detalle que
    // importa de estos caminos ya sale por `info`.
    debug: () => {},
    info: (mensaje) => console.log(`[${prefijo}] ${mensaje}`),
    warn: (mensaje) => console.warn(`[${prefijo}] ${mensaje}`),
    error: (mensaje, error) => console.error(`[${prefijo}] ${mensaje}`, error || ''),
  };
}

module.exports = { StructuredLogger, crearLoggerDeConsola };
