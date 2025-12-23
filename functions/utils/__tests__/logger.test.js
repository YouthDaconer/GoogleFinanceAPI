/**
 * Tests for StructuredLogger
 * 
 * @see SCALE-CORE-002 - Observabilidad End-to-End
 */

const { StructuredLogger } = require('../logger');

// Mock firebase-functions
jest.mock('firebase-functions', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  },
}));

const functions = require('firebase-functions');

describe('StructuredLogger', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create logger with function name', () => {
      const logger = new StructuredLogger('testFunction');
      
      expect(logger.functionName).toBe('testFunction');
      expect(logger.userId).toBe('system');
      expect(logger.traceId).toBeDefined();
    });

    it('should accept custom context', () => {
      const logger = new StructuredLogger('testFunction', {
        userId: 'user123',
        traceId: 'trace456',
      });
      
      expect(logger.userId).toBe('user123');
      expect(logger.traceId).toBe('trace456');
    });
  });

  describe('logging methods', () => {
    it('should log info with structured data', () => {
      const logger = new StructuredLogger('testFunction');
      
      logger.info('Test message', { extra: 'data' });
      
      expect(functions.logger.info).toHaveBeenCalledTimes(1);
      const logEntry = functions.logger.info.mock.calls[0][0];
      
      expect(logEntry.severity).toBe('INFO');
      expect(logEntry.message).toBe('Test message');
      expect(logEntry.function).toBe('testFunction');
      expect(logEntry.extra).toBe('data');
      expect(logEntry.timestamp).toBeDefined();
      expect(logEntry.traceId).toBeDefined();
    });

    it('should log error with error object', () => {
      const logger = new StructuredLogger('testFunction');
      const error = new Error('Test error');
      
      logger.error('Error occurred', error, { context: 'test' });
      
      expect(functions.logger.error).toHaveBeenCalledTimes(1);
      const logEntry = functions.logger.error.mock.calls[0][0];
      
      expect(logEntry.severity).toBe('ERROR');
      expect(logEntry.error.message).toBe('Test error');
      expect(logEntry.error.name).toBe('Error');
      expect(logEntry.context).toBe('test');
    });

    it('should log debug messages', () => {
      const logger = new StructuredLogger('testFunction');
      
      logger.debug('Debug message');
      
      expect(functions.logger.debug).toHaveBeenCalledTimes(1);
    });

    it('should log warn messages', () => {
      const logger = new StructuredLogger('testFunction');
      
      logger.warn('Warning message');
      
      expect(functions.logger.warn).toHaveBeenCalledTimes(1);
    });
  });

  describe('startOperation', () => {
    it('should track successful operation', async () => {
      const logger = new StructuredLogger('testFunction');
      
      const op = logger.startOperation('myOperation');
      
      await new Promise(resolve => setTimeout(resolve, 10));
      
      op.success({ items: 5 });
      
      expect(functions.logger.info).toHaveBeenCalledTimes(1);
      const logEntry = functions.logger.info.mock.calls[0][0];
      
      expect(logEntry.operation).toBe('myOperation');
      expect(logEntry.status).toBe('success');
      expect(logEntry.duration).toBeGreaterThanOrEqual(10);
      expect(logEntry.items).toBe(5);
    });

    it('should track failed operation', () => {
      const logger = new StructuredLogger('testFunction');
      const error = new Error('Operation failed');
      
      const op = logger.startOperation('myOperation');
      op.failure(error, { attempts: 3 });
      
      expect(functions.logger.error).toHaveBeenCalledTimes(1);
      const logEntry = functions.logger.error.mock.calls[0][0];
      
      expect(logEntry.operation).toBe('myOperation');
      expect(logEntry.status).toBe('error');
      expect(logEntry.attempts).toBe(3);
    });
  });

  describe('static factory methods', () => {
    it('should create logger for scheduled function', () => {
      const logger = StructuredLogger.forScheduled('scheduledTask');
      
      expect(logger.functionName).toBe('scheduledTask');
      expect(logger.userId).toBe('scheduler');
      expect(logger.traceId).toMatch(/^sched-\d+$/);
    });

    it('should create logger for callable function with auth', () => {
      const mockRequest = {
        auth: { uid: 'user123' },
        rawRequest: {
          headers: { 'x-cloud-trace-context': 'trace789' },
        },
      };
      
      const logger = StructuredLogger.forCallable('callableFunc', mockRequest);
      
      expect(logger.functionName).toBe('callableFunc');
      expect(logger.userId).toBe('user123');
      expect(logger.traceId).toBe('trace789');
    });

    it('should create logger for callable function without auth', () => {
      const mockRequest = {
        rawRequest: { headers: {} },
      };
      
      const logger = StructuredLogger.forCallable('callableFunc', mockRequest);
      
      expect(logger.userId).toBe('anonymous');
    });
  });

  describe('elapsedMs tracking', () => {
    it('should track elapsed time in log entries', async () => {
      const logger = new StructuredLogger('testFunction');
      
      await new Promise(resolve => setTimeout(resolve, 50));
      
      logger.info('After delay');
      
      const logEntry = functions.logger.info.mock.calls[0][0];
      expect(logEntry.elapsedMs).toBeGreaterThanOrEqual(50);
    });
  });
});
