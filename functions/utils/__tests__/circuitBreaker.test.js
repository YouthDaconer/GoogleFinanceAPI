/**
 * Circuit Breaker Unit Tests
 * 
 * Tests the circuit breaker pattern implementation including:
 * - State transitions (CLOSED → OPEN → HALF_OPEN → CLOSED)
 * - Failure threshold behavior
 * - Recovery timeout behavior
 * - Fallback execution
 * - Metrics tracking
 * 
 * @see SCALE-BE-003 - Circuit Breaker para APIs Externas
 */

const { CircuitBreaker, CircuitState, getCircuit, resetAllCircuits } = require('../circuitBreaker');

// Mock the logger to avoid Firebase dependencies in tests
jest.mock('../logger', () => ({
  StructuredLogger: jest.fn().mockImplementation(() => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  })),
}));

describe('CircuitBreaker', () => {
  let circuit;

  beforeEach(() => {
    resetAllCircuits();
    circuit = new CircuitBreaker('test-circuit', {
      failureThreshold: 3,
      resetTimeout: 1000,
      halfOpenRequests: 1,
    });
  });

  describe('Initial State', () => {
    it('should start in CLOSED state', () => {
      expect(circuit.state).toBe(CircuitState.CLOSED);
      expect(circuit.isClosed()).toBe(true);
      expect(circuit.isOpen()).toBe(false);
    });

    it('should have zero failures initially', () => {
      expect(circuit.failures).toBe(0);
    });

    it('should have correct default configuration', () => {
      const defaultCircuit = new CircuitBreaker('default-test');
      expect(defaultCircuit.failureThreshold).toBe(5);
      expect(defaultCircuit.resetTimeout).toBe(60000);
    });
  });

  describe('Successful Requests', () => {
    it('should execute primary function when CLOSED', async () => {
      const primaryFn = jest.fn().mockResolvedValue('success');
      const fallbackFn = jest.fn().mockResolvedValue('fallback');

      const result = await circuit.execute(primaryFn, fallbackFn);

      expect(result).toBe('success');
      expect(primaryFn).toHaveBeenCalled();
      expect(fallbackFn).not.toHaveBeenCalled();
    });

    it('should reset failure count on success', async () => {
      circuit.failures = 2;
      const primaryFn = jest.fn().mockResolvedValue('success');

      await circuit.execute(primaryFn, jest.fn());

      expect(circuit.failures).toBe(0);
    });

    it('should track successful requests in metrics', async () => {
      const primaryFn = jest.fn().mockResolvedValue('success');

      await circuit.execute(primaryFn, jest.fn());
      await circuit.execute(primaryFn, jest.fn());

      const metrics = circuit.getMetrics();
      expect(metrics.successfulRequests).toBe(2);
      expect(metrics.totalRequests).toBe(2);
    });
  });

  describe('Failed Requests', () => {
    it('should increment failure count on error', async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('API error'));

      await expect(circuit.execute(primaryFn, jest.fn())).rejects.toThrow('API error');
      expect(circuit.failures).toBe(1);
    });

    it('should open circuit after reaching failure threshold', async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('API error'));
      const fallbackFn = jest.fn().mockResolvedValue('fallback');

      for (let i = 0; i < 3; i++) {
        try {
          await circuit.execute(primaryFn, fallbackFn);
        } catch (e) {
          // Expected to throw until circuit opens
        }
      }

      expect(circuit.state).toBe(CircuitState.OPEN);
      expect(circuit.isOpen()).toBe(true);
    });

    it('should record lastFailureTime on failure', async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('API error'));
      const beforeTime = Date.now();

      try {
        await circuit.execute(primaryFn, jest.fn());
      } catch (e) {
        // Expected
      }

      expect(circuit.lastFailureTime).toBeGreaterThanOrEqual(beforeTime);
    });
  });

  describe('Open State Behavior', () => {
    beforeEach(async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('API error'));
      for (let i = 0; i < 3; i++) {
        try {
          await circuit.execute(primaryFn, jest.fn());
        } catch (e) {
          // Opening circuit
        }
      }
    });

    it('should execute fallback when circuit is OPEN', async () => {
      const primaryFn = jest.fn().mockResolvedValue('success');
      const fallbackFn = jest.fn().mockResolvedValue('fallback');

      const result = await circuit.execute(primaryFn, fallbackFn);

      expect(result).toBe('fallback');
      expect(primaryFn).not.toHaveBeenCalled();
      expect(fallbackFn).toHaveBeenCalled();
    });

    it('should throw error if no fallback provided when OPEN', async () => {
      const primaryFn = jest.fn().mockResolvedValue('success');

      await expect(circuit.execute(primaryFn, null)).rejects.toThrow(
        'Circuit test-circuit is open and no fallback provided'
      );
    });

    it('should track fallback usage in metrics', async () => {
      const initialFallbacks = circuit.getMetrics().fallbacksUsed;
      const primaryFn = jest.fn().mockResolvedValue('success');
      const fallbackFn = jest.fn().mockResolvedValue('fallback');

      await circuit.execute(primaryFn, fallbackFn);
      await circuit.execute(primaryFn, fallbackFn);

      const metrics = circuit.getMetrics();
      expect(metrics.fallbacksUsed).toBe(initialFallbacks + 2);
    });
  });

  describe('Recovery (HALF_OPEN State)', () => {
    beforeEach(async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('API error'));
      for (let i = 0; i < 3; i++) {
        try {
          await circuit.execute(primaryFn, jest.fn());
        } catch (e) {
          // Opening circuit
        }
      }
      circuit.lastFailureTime = Date.now() - 2000;
    });

    it('should transition to HALF_OPEN after resetTimeout', async () => {
      const primaryFn = jest.fn().mockResolvedValue('success');
      const fallbackFn = jest.fn().mockResolvedValue('fallback');

      await circuit.execute(primaryFn, fallbackFn);

      expect(primaryFn).toHaveBeenCalled();
    });

    it('should close circuit after successful recovery', async () => {
      const primaryFn = jest.fn().mockResolvedValue('success');
      const fallbackFn = jest.fn().mockResolvedValue('fallback');

      await circuit.execute(primaryFn, fallbackFn);

      expect(circuit.state).toBe(CircuitState.CLOSED);
    });

    it('should re-open circuit if recovery fails', async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('Still failing'));
      const fallbackFn = jest.fn().mockResolvedValue('fallback');

      const result = await circuit.execute(primaryFn, fallbackFn);

      expect(result).toBe('fallback');
      expect(circuit.state).toBe(CircuitState.OPEN);
    });

    it('should track recovery in metrics', async () => {
      const primaryFn = jest.fn().mockResolvedValue('success');

      await circuit.execute(primaryFn, jest.fn());

      const metrics = circuit.getMetrics();
      expect(metrics.recoveries).toBe(1);
    });
  });

  describe('getState()', () => {
    it('should return current circuit state', () => {
      const state = circuit.getState();

      expect(state).toEqual({
        name: 'test-circuit',
        state: CircuitState.CLOSED,
        failures: 0,
        lastFailureTime: null,
        config: {
          failureThreshold: 3,
          resetTimeout: 1000,
        },
      });
    });
  });

  describe('reset()', () => {
    it('should reset circuit to initial state', async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('API error'));
      for (let i = 0; i < 3; i++) {
        try {
          await circuit.execute(primaryFn, jest.fn());
        } catch (e) {
          // Opening circuit
        }
      }

      circuit.reset();

      expect(circuit.state).toBe(CircuitState.CLOSED);
      expect(circuit.failures).toBe(0);
      expect(circuit.lastFailureTime).toBeNull();
    });
  });

  describe('getCircuit() Singleton', () => {
    beforeEach(() => {
      resetAllCircuits();
    });

    it('should return same instance for same name', () => {
      const circuit1 = getCircuit('singleton-test', { failureThreshold: 5 });
      const circuit2 = getCircuit('singleton-test', { failureThreshold: 10 });

      expect(circuit1).toBe(circuit2);
      expect(circuit1.failureThreshold).toBe(5);
    });

    it('should return different instances for different names', () => {
      const circuit1 = getCircuit('circuit-a');
      const circuit2 = getCircuit('circuit-b');

      expect(circuit1).not.toBe(circuit2);
    });
  });

  describe('Metrics Tracking', () => {
    it('should track circuit trips', async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('API error'));

      for (let i = 0; i < 3; i++) {
        try {
          await circuit.execute(primaryFn, jest.fn());
        } catch (e) {
          // Expected
        }
      }

      const metrics = circuit.getMetrics();
      expect(metrics.circuitTrips).toBe(1);
    });

    it('should provide complete metrics object', async () => {
      const metrics = circuit.getMetrics();

      expect(metrics).toHaveProperty('totalRequests');
      expect(metrics).toHaveProperty('successfulRequests');
      expect(metrics).toHaveProperty('failedRequests');
      expect(metrics).toHaveProperty('fallbacksUsed');
      expect(metrics).toHaveProperty('circuitTrips');
      expect(metrics).toHaveProperty('recoveries');
      expect(metrics).toHaveProperty('circuitName');
      expect(metrics).toHaveProperty('currentState');
    });
  });
});
