/**
 * Tests for formatFingerprint service
 *
 * HU 1.1: identidad determinista del formato de un archivo de broker.
 *
 * @see platform-docs/stories/1.1-perfil-importacion-recordado/
 */

// Path: services/__tests__/transactions/ -> services/transactions/services/formatFingerprint.js
const {
  buildSourceFormatId,
  computeHeaderFingerprint,
  normalizeHeader,
  isBrokerFormat,
  isValidSourceFormatId,
  FINGERPRINT_LENGTH,
} = require('../../transactions/services/formatFingerprint');

describe('formatFingerprint', () => {
  describe('normalizeHeader', () => {
    test('recorta espacios y pasa a minúsculas', () => {
      expect(normalizeHeader('  Trade Date  ')).toBe('trade date');
    });

    test('colapsa espacios internos repetidos', () => {
      expect(normalizeHeader('T.   Price')).toBe('t. price');
    });

    test('trata null y undefined como cadena vacía', () => {
      expect(normalizeHeader(null)).toBe('');
      expect(normalizeHeader(undefined)).toBe('');
    });

    test('normaliza valores no string', () => {
      expect(normalizeHeader(42)).toBe('42');
    });
  });

  describe('computeHeaderFingerprint', () => {
    test('es determinista para las mismas cabeceras', () => {
      const headers = ['Symbol', 'Trade Date', 'Quantity', 'Price'];

      expect(computeHeaderFingerprint(headers)).toBe(computeHeaderFingerprint(headers));
    });

    test('es estable frente a mayúsculas y espacios', () => {
      const a = computeHeaderFingerprint(['Symbol', 'Trade Date']);
      const b = computeHeaderFingerprint([' symbol ', 'TRADE  DATE']);

      expect(a).toBe(b);
    });

    test('cambia si cambian las cabeceras', () => {
      const a = computeHeaderFingerprint(['Symbol', 'Trade Date']);
      const b = computeHeaderFingerprint(['Symbol', 'Settlement Date']);

      expect(a).not.toBe(b);
    });

    test('es posicional: distinto orden es distinto formato', () => {
      const a = computeHeaderFingerprint(['Symbol', 'Quantity']);
      const b = computeHeaderFingerprint(['Quantity', 'Symbol']);

      expect(a).not.toBe(b);
    });

    test('devuelve un hash de la longitud configurada', () => {
      expect(computeHeaderFingerprint(['A', 'B'])).toHaveLength(FINGERPRINT_LENGTH);
    });
  });

  describe('buildSourceFormatId', () => {
    test('usa el broker cuando fue detectado', () => {
      const id = buildSourceFormatId({
        detectedBroker: 'degiro',
        headers: ['Fecha', 'Producto'],
        columnCount: 2,
      });

      expect(id).toBe('broker:degiro');
    });

    test('usa la huella de cabeceras cuando no hay broker', () => {
      const id = buildSourceFormatId({
        detectedBroker: null,
        headers: ['Symbol', 'Qty'],
        columnCount: 2,
      });

      expect(id).toMatch(/^fmt:[0-9a-f]{16}$/);
    });

    test('usa el número de columnas cuando no hay cabeceras', () => {
      const id = buildSourceFormatId({
        detectedBroker: null,
        headers: null,
        columnCount: 7,
      });

      expect(id).toBe('fmt:cols7');
    });

    test('Escenario 4: dos brokers distintos producen ids distintos', () => {
      const ibkr = buildSourceFormatId({
        detectedBroker: 'interactive_brokers',
        headers: ['Symbol', 'T. Price'],
        columnCount: 2,
      });
      const schwab = buildSourceFormatId({
        detectedBroker: 'charles_schwab',
        headers: ['Symbol', 'Fees & Comm'],
        columnCount: 2,
      });

      expect(ibkr).not.toBe(schwab);
    });

    test('dos usuarios con el mismo export sin broker obtienen el mismo id', () => {
      // Necesario para que la clave sea compartible en el catálogo global (HU 1.6)
      const headers = ['Ticker', 'Fecha', 'Unidades', 'Precio'];

      const userA = buildSourceFormatId({ detectedBroker: null, headers, columnCount: 4 });
      const userB = buildSourceFormatId({ detectedBroker: null, headers: [...headers], columnCount: 4 });

      expect(userA).toBe(userB);
    });
  });

  describe('isBrokerFormat', () => {
    test('reconoce los formatos de broker', () => {
      expect(isBrokerFormat('broker:degiro')).toBe(true);
    });

    test('rechaza los formatos por huella', () => {
      expect(isBrokerFormat('fmt:abcdef0123456789')).toBe(false);
    });

    test('rechaza valores no string', () => {
      expect(isBrokerFormat(null)).toBe(false);
      expect(isBrokerFormat(42)).toBe(false);
    });
  });

  describe('isValidSourceFormatId', () => {
    test('acepta las tres formas válidas', () => {
      expect(isValidSourceFormatId('broker:interactive_brokers')).toBe(true);
      expect(isValidSourceFormatId('fmt:abcdef0123456789')).toBe(true);
      expect(isValidSourceFormatId('fmt:cols12')).toBe(true);
    });

    test('rechaza formas inválidas', () => {
      expect(isValidSourceFormatId('')).toBe(false);
      expect(isValidSourceFormatId(null)).toBe(false);
      expect(isValidSourceFormatId('broker:Bad-Id')).toBe(false);
      expect(isValidSourceFormatId('fmt:xyz')).toBe(false);
      expect(isValidSourceFormatId('otra/cosa')).toBe(false);
    });

    test('rechaza intentos de path traversal en el id de documento', () => {
      expect(isValidSourceFormatId('broker:../../userData')).toBe(false);
      expect(isValidSourceFormatId('fmt:aaaaaaaaaaaaaaaa/../x')).toBe(false);
    });

    test('rechaza ids excesivamente largos', () => {
      expect(isValidSourceFormatId(`broker:${'a'.repeat(200)}`)).toBe(false);
    });
  });
});
