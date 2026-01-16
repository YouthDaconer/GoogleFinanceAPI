/**
 * Tests para marketDataTokenService.js
 * 
 * OPT-DEMAND-101: Verifica la generación de tokens HMAC-SHA256
 * 
 * @module __tests__/services/marketDataTokenService.test
 * @see docs/stories/71.story.md (OPT-DEMAND-101)
 */

const crypto = require('crypto');

// Mock de Firebase Functions antes de importar el módulo
jest.mock('firebase-functions/v2/https', () => ({
  onCall: jest.fn((config, handler) => {
    // Retornar el handler para poder probarlo
    return { _handler: handler, _config: config };
  }),
  HttpsError: class HttpsError extends Error {
    constructor(code, message) {
      super(message);
      this.code = code;
      this.message = message;
    }
  }
}));

jest.mock('firebase-functions/params', () => ({
  defineSecret: jest.fn((name) => ({
    name,
    value: jest.fn(() => 'test-secret-for-unit-tests-32chars!')
  }))
}));

jest.mock('../../utils/logger', () => ({
  StructuredLogger: jest.fn().mockImplementation(() => ({
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    debug: jest.fn()
  }))
}));

// Importar funciones después de los mocks
const { 
  _generateToken, 
  _validateAuth,
  _TOKEN_TTL_SECONDS, 
  _TOKEN_AUDIENCE,
  getMarketDataToken 
} = require('../marketDataTokenService');

const { HttpsError } = require('firebase-functions/v2/https');

// ============================================================================
// TESTS PARA generateToken
// ============================================================================

describe('generateToken', () => {
  const mockUserId = 'test-user-123';
  const mockSecret = 'test-secret-for-unit-tests-32chars!';
  const mockTtl = 300;
  
  beforeEach(() => {
    // Fijar el tiempo para tests determinísticos
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-01-15T12:00:00Z'));
  });
  
  afterEach(() => {
    jest.useRealTimers();
  });
  
  test('genera token con formato correcto (payload.signature)', () => {
    const result = _generateToken(mockUserId, mockSecret, mockTtl);
    
    expect(result.token).toBeDefined();
    expect(typeof result.token).toBe('string');
    
    const parts = result.token.split('.');
    expect(parts).toHaveLength(2);
    expect(parts[0].length).toBeGreaterThan(0);
    expect(parts[1].length).toBeGreaterThan(0);
  });
  
  test('payload contiene campos requeridos', () => {
    const result = _generateToken(mockUserId, mockSecret, mockTtl);
    
    const [payloadB64] = result.token.split('.');
    const payload = JSON.parse(Buffer.from(payloadB64, 'base64url').toString());
    
    expect(payload.uid).toBe(mockUserId);
    expect(payload.aud).toBe(_TOKEN_AUDIENCE);
    expect(typeof payload.iat).toBe('number');
    expect(typeof payload.exp).toBe('number');
    expect(payload.exp - payload.iat).toBe(mockTtl);
  });
  
  test('expiresAt está en milisegundos y es correcto', () => {
    const result = _generateToken(mockUserId, mockSecret, mockTtl);
    
    const nowSeconds = Math.floor(Date.now() / 1000);
    const expectedExpiresAtMs = (nowSeconds + mockTtl) * 1000;
    
    expect(result.expiresAt).toBe(expectedExpiresAtMs);
    expect(result.expiresAt).toBeGreaterThan(Date.now());
  });
  
  test('firma es verificable con el mismo secret', () => {
    const result = _generateToken(mockUserId, mockSecret, mockTtl);
    
    const [payloadB64, signature] = result.token.split('.');
    
    // Regenerar firma
    const expectedSignature = crypto
      .createHmac('sha256', mockSecret)
      .update(payloadB64)
      .digest('base64url');
    
    expect(signature).toBe(expectedSignature);
  });
  
  test('firma falla con secret diferente', () => {
    const result = _generateToken(mockUserId, mockSecret, mockTtl);
    
    const [payloadB64, signature] = result.token.split('.');
    
    // Intentar verificar con secret diferente
    const wrongSignature = crypto
      .createHmac('sha256', 'wrong-secret')
      .update(payloadB64)
      .digest('base64url');
    
    expect(signature).not.toBe(wrongSignature);
  });
  
  test('tokens diferentes para usuarios diferentes', () => {
    const result1 = _generateToken('user-1', mockSecret, mockTtl);
    const result2 = _generateToken('user-2', mockSecret, mockTtl);
    
    expect(result1.token).not.toBe(result2.token);
  });
  
  test('TTL configurable afecta expiración', () => {
    const shortTtl = 60; // 1 minuto
    const longTtl = 600; // 10 minutos
    
    const shortResult = _generateToken(mockUserId, mockSecret, shortTtl);
    const longResult = _generateToken(mockUserId, mockSecret, longTtl);
    
    expect(longResult.expiresAt - shortResult.expiresAt).toBe((longTtl - shortTtl) * 1000);
  });
});

// ============================================================================
// TESTS PARA validateAuth
// ============================================================================

describe('validateAuth', () => {
  test('no lanza error con auth válido', () => {
    expect(() => {
      _validateAuth({ uid: 'user-123' });
    }).not.toThrow();
  });
  
  test('lanza HttpsError unauthenticated sin auth', () => {
    expect(() => {
      _validateAuth(null);
    }).toThrow(HttpsError);
  });
  
  test('lanza HttpsError unauthenticated con auth undefined', () => {
    expect(() => {
      _validateAuth(undefined);
    }).toThrow(HttpsError);
  });
  
  test('error tiene código y mensaje correctos', () => {
    try {
      _validateAuth(null);
      fail('Debería haber lanzado error');
    } catch (error) {
      expect(error.code).toBe('unauthenticated');
      expect(error.message).toBe('Autenticación requerida');
    }
  });
});

// ============================================================================
// TESTS PARA getMarketDataToken (Cloud Function)
// ============================================================================

describe('getMarketDataToken Cloud Function', () => {
  test('retorna token para usuario autenticado', async () => {
    const handler = getMarketDataToken._handler;
    
    const mockRequest = {
      auth: { uid: 'test-user-123' }
    };
    
    const result = await handler(mockRequest);
    
    expect(result).toHaveProperty('token');
    expect(result).toHaveProperty('expiresAt');
    expect(result).toHaveProperty('ttlSeconds');
    
    expect(typeof result.token).toBe('string');
    expect(result.token.split('.')).toHaveLength(2);
    expect(result.expiresAt).toBeGreaterThan(Date.now());
    expect(result.ttlSeconds).toBe(_TOKEN_TTL_SECONDS);
  });
  
  test('rechaza usuario no autenticado', async () => {
    const handler = getMarketDataToken._handler;
    
    const mockRequest = {
      auth: null
    };
    
    await expect(handler(mockRequest)).rejects.toThrow(HttpsError);
    
    try {
      await handler(mockRequest);
    } catch (error) {
      expect(error.code).toBe('unauthenticated');
    }
  });
  
  test('configuración de función es correcta', () => {
    const config = getMarketDataToken._config;
    
    expect(config.cors).toBe(true);
    expect(config.secrets).toBeDefined();
    expect(config.secrets).toHaveLength(1);
    expect(config.timeoutSeconds).toBe(10);
    expect(config.memory).toBe('128MiB');
    expect(config.maxInstances).toBe(10);
  });
});

// ============================================================================
// TESTS DE CONSTANTES
// ============================================================================

describe('Constantes de configuración', () => {
  test('TTL es 300 segundos (5 minutos)', () => {
    expect(_TOKEN_TTL_SECONDS).toBe(300);
  });
  
  test('Audience es finance-query-api', () => {
    expect(_TOKEN_AUDIENCE).toBe('finance-query-api');
  });
});

// ============================================================================
// TESTS DE SEGURIDAD
// ============================================================================

describe('Seguridad', () => {
  test('token no contiene el secret', () => {
    const secret = 'my-super-secret-key-12345678901234';
    const result = _generateToken('user-1', secret, 300);
    
    expect(result.token).not.toContain(secret);
    expect(result.token).not.toContain(Buffer.from(secret).toString('base64'));
  });
  
  test('payload no contiene información sensible', () => {
    const result = _generateToken('user-1', 'secret', 300);
    
    const [payloadB64] = result.token.split('.');
    const payload = JSON.parse(Buffer.from(payloadB64, 'base64url').toString());
    
    // Solo debe contener campos esperados
    const allowedKeys = ['uid', 'iat', 'exp', 'aud'];
    const actualKeys = Object.keys(payload);
    
    expect(actualKeys.sort()).toEqual(allowedKeys.sort());
  });
  
  test('signature tiene longitud correcta para SHA256', () => {
    const result = _generateToken('user-1', 'secret', 300);
    
    const [, signature] = result.token.split('.');
    
    // SHA256 = 32 bytes = 43-44 chars en base64url
    expect(signature.length).toBeGreaterThanOrEqual(42);
    expect(signature.length).toBeLessThanOrEqual(44);
  });
});
