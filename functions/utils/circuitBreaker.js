/**
 * Circuit Breaker Pattern Implementation
 * 
 * Protects external API calls by detecting failures and preventing
 * cascade failures. When an API fails consistently, the circuit "opens"
 * and returns cached/fallback data instead of making more failing calls.
 * 
 * States:
 * - CLOSED: Normal operation, requests go through
 * - OPEN: API is failing, use fallback immediately
 * - HALF_OPEN: Testing if API has recovered
 * 
 * @see SCALE-BE-003 - Circuit Breaker para APIs Externas
 */

const { StructuredLogger } = require('./logger');

const CircuitState = {
  CLOSED: 'CLOSED',
  OPEN: 'OPEN',
  HALF_OPEN: 'HALF_OPEN',
};

class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.logger = new StructuredLogger('CircuitBreaker', { circuit: name });
    
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeout = options.resetTimeout || 60000;
    this.halfOpenRequests = options.halfOpenRequests || 1;
    
    this.state = CircuitState.CLOSED;
    this.failures = 0;
    this.successes = 0;
    this.lastFailureTime = null;
    this.halfOpenAttempts = 0;
    
    this.metrics = {
      totalRequests: 0,
      successfulRequests: 0,
      failedRequests: 0,
      fallbacksUsed: 0,
      circuitTrips: 0,
      recoveries: 0,
    };
  }

  async execute(primaryFn, fallbackFn) {
    this.metrics.totalRequests++;

    if (this.state === CircuitState.OPEN) {
      if (this._shouldAttemptRecovery()) {
        this._transitionTo(CircuitState.HALF_OPEN);
        this.halfOpenAttempts = 0;
      } else {
        return this._executeFallback(fallbackFn, 'circuit_open');
      }
    }

    try {
      const result = await primaryFn();
      this._recordSuccess();
      return result;
    } catch (error) {
      this._recordFailure(error);
      
      if (this.state === CircuitState.OPEN) {
        return this._executeFallback(fallbackFn, 'primary_failed');
      }
      
      throw error;
    }
  }

  _shouldAttemptRecovery() {
    if (!this.lastFailureTime) return false;
    return Date.now() - this.lastFailureTime >= this.resetTimeout;
  }

  _transitionTo(newState) {
    const previousState = this.state;
    this.state = newState;
    
    this.logger.info('Circuit state transition', {
      circuit: this.name,
      previousState,
      newState,
      failures: this.failures,
      resetTimeout: this.resetTimeout,
    });
  }

  _recordSuccess() {
    this.failures = 0;
    this.successes++;
    this.metrics.successfulRequests++;
    
    if (this.state === CircuitState.HALF_OPEN) {
      this.halfOpenAttempts++;
      if (this.halfOpenAttempts >= this.halfOpenRequests) {
        this._transitionTo(CircuitState.CLOSED);
        this.metrics.recoveries++;
        this.logger.info('Circuit recovered', {
          circuit: this.name,
          successfulAttempts: this.halfOpenAttempts,
        });
      }
    }
  }

  _recordFailure(error) {
    this.failures++;
    this.lastFailureTime = Date.now();
    this.metrics.failedRequests++;
    
    this.logger.warn('Circuit recorded failure', {
      circuit: this.name,
      failures: this.failures,
      threshold: this.failureThreshold,
      errorMessage: error.message,
    });

    if (this.state === CircuitState.HALF_OPEN) {
      this._transitionTo(CircuitState.OPEN);
      this.logger.warn('Circuit re-opened after failed recovery', {
        circuit: this.name,
      });
    } else if (this.failures >= this.failureThreshold) {
      this._transitionTo(CircuitState.OPEN);
      this.metrics.circuitTrips++;
      this.logger.error('Circuit OPENED due to failures', error, {
        circuit: this.name,
        failures: this.failures,
        threshold: this.failureThreshold,
      });
    }
  }

  async _executeFallback(fallbackFn, reason) {
    this.metrics.fallbacksUsed++;
    
    this.logger.info('Executing fallback', {
      circuit: this.name,
      reason,
      state: this.state,
    });
    
    if (!fallbackFn) {
      throw new Error(`Circuit ${this.name} is open and no fallback provided`);
    }
    
    const result = await fallbackFn();
    return result;
  }

  getState() {
    return {
      name: this.name,
      state: this.state,
      failures: this.failures,
      lastFailureTime: this.lastFailureTime,
      config: {
        failureThreshold: this.failureThreshold,
        resetTimeout: this.resetTimeout,
      },
    };
  }

  getMetrics() {
    return {
      ...this.metrics,
      circuitName: this.name,
      currentState: this.state,
    };
  }

  reset() {
    this.state = CircuitState.CLOSED;
    this.failures = 0;
    this.successes = 0;
    this.lastFailureTime = null;
    this.halfOpenAttempts = 0;
    this.logger.info('Circuit manually reset', { circuit: this.name });
  }

  isOpen() {
    return this.state === CircuitState.OPEN;
  }

  isClosed() {
    return this.state === CircuitState.CLOSED;
  }
}

const circuits = new Map();

function getCircuit(name, options = {}) {
  if (!circuits.has(name)) {
    circuits.set(name, new CircuitBreaker(name, options));
  }
  return circuits.get(name);
}

function getAllCircuitStates() {
  const states = {};
  circuits.forEach((circuit, name) => {
    states[name] = circuit.getState();
  });
  return states;
}

function resetCircuit(name) {
  if (circuits.has(name)) {
    circuits.get(name).reset();
    return true;
  }
  return false;
}

function resetAllCircuits() {
  circuits.forEach(circuit => circuit.reset());
}

module.exports = {
  CircuitBreaker,
  CircuitState,
  getCircuit,
  getAllCircuitStates,
  resetCircuit,
  resetAllCircuits,
};
