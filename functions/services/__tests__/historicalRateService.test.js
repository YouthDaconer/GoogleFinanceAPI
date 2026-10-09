/**
 * HU #3 — Tests del servicio de tasa de cambio por fecha, ya sin archivo.
 *
 * Lo que estos tests defienden no es "que devuelva el número correcto": es que
 * **ninguna tasa se guarde** y que **una ausencia se declare ausente**. Las dos
 * cosas que fallaron en producción fueron un dato persistido que envejeció en
 * silencio y un fallback que convertía esa ausencia en una cifra creíble.
 *
 * @module __tests__/services/historicalRateService.test
 * @see platform-docs/stories/3-tasa-vigente-canal-mercado/refinamiento.md (T21)
 */

// El servicio ya no habla con Firestore. Se mockea igualmente para poder
// afirmar que NADIE lo llama: es la garantía de RN-3-A.
const mockDoc = jest.fn();
const mockCollection = jest.fn();

jest.mock('../firebaseAdmin', () => {
  const mockAdmin = {
    firestore: jest.fn(() => ({ doc: mockDoc, collection: mockCollection })),
  };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => 'SERVER_TIMESTAMP' };
  return mockAdmin;
});

// Tampoco habla con Yahoo por su cuenta: todo pasa por el canal de mercado.
const mockFetch = jest.fn();
jest.mock('node-fetch', () => {
  const fn = (...args) => mockFetch(...args);
  fn.default = fn;
  return fn;
});

const mockGetExchangeRates = jest.fn();
jest.mock('../financeQuery', () => ({
  getExchangeRates: (...args) => mockGetExchangeRates(...args),
}));

const historicalRateService = require('../historicalRateService');
const {
  getRateForDate,
  getCrossRate,
  getRatesForRange,
  RATE_SOURCES,
  _resetMemory,
} = historicalRateService;

/** Respuesta del canal con las series indicadas */
const channel = (rates, unavailable = []) => ({ base: 'USD', rates, unavailable });

describe('historicalRateService (HU #3)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    _resetMemory();
  });

  describe('ninguna tasa se guarda (RN-3-A)', () => {
    it('no lee ni escribe en Firestore para resolver una fecha', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({ COP: { '2026-03-12': 3900.5 } }));

      await getRateForDate('COP', '2026-03-12');

      expect(mockDoc).not.toHaveBeenCalled();
      expect(mockCollection).not.toHaveBeenCalled();
    });

    it('no llama a Yahoo por su cuenta: todo pasa por el canal de mercado (RN-3-E)', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({ COP: { '2026-03-12': 3900.5 } }));

      await getRateForDate('COP', '2026-03-12');

      expect(mockFetch).not.toHaveBeenCalled();
      expect(mockGetExchangeRates).toHaveBeenCalledTimes(1);
    });
  });

  describe('getRateForDate', () => {
    it('devuelve 1 para USD sin consultar nada', async () => {
      const result = await getRateForDate('USD', '2026-03-12');

      expect(result).toEqual({ rate: 1, rateDate: '2026-03-12', source: RATE_SOURCES.CACHE });
      expect(mockGetExchangeRates).not.toHaveBeenCalled();
    });

    it('devuelve la tasa del día pedido cuando el mercado la tiene', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({ COP: { '2026-03-12': 3900.5 } }));

      const result = await getRateForDate('COP', '2026-03-12');

      expect(result).toEqual({
        rate: 3900.5,
        rateDate: '2026-03-12',
        source: RATE_SOURCES.YAHOO,
      });
    });

    it('resuelve un día no hábil al cierre anterior en UNA sola llamada (RN-3-C)', async () => {
      // 2026-03-14 es sábado: el rango pedido incluye el viernes
      mockGetExchangeRates.mockResolvedValue(channel({
        COP: { '2026-03-12': 3890, '2026-03-13': 3900.5 },
      }));

      const result = await getRateForDate('COP', '2026-03-14');

      expect(result).toEqual({
        rate: 3900.5,
        rateDate: '2026-03-13',
        source: RATE_SOURCES.PREVIOUS_CLOSE,
      });
      expect(mockGetExchangeRates).toHaveBeenCalledTimes(1);
    });

    it('devuelve null cuando el canal no responde, sin inventar una tasa (RN-3-D)', async () => {
      mockGetExchangeRates.mockResolvedValue(null);

      const result = await getRateForDate('COP', '2026-03-12');

      expect(result).toBeNull();
    });

    it('devuelve null cuando el canal responde sin esa divisa', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({}, ['COP']));

      const result = await getRateForDate('COP', '2026-03-12');

      expect(result).toBeNull();
    });

    it('devuelve null cuando la fecha queda fuera del lookback, sin caer a la tasa vigente', async () => {
      // El cierre más reciente es de hace 10 días: más allá del lookback
      mockGetExchangeRates.mockResolvedValue(channel({ COP: { '2026-03-02': 3716.57 } }));

      const result = await getRateForDate('COP', '2026-03-12');

      expect(result).toBeNull();
    });

    it('rechaza una fecha con formato inválido', async () => {
      await expect(getRateForDate('COP', '12/03/2026')).rejects.toThrow('Fecha inválida');
    });
  });

  describe('memoria del proceso (RN-3-A, D6)', () => {
    it('no repite la llamada para la misma divisa y fecha', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({ COP: { '2026-03-12': 3900.5 } }));

      const first = await getRateForDate('COP', '2026-03-12');
      const second = await getRateForDate('COP', '2026-03-12');

      expect(mockGetExchangeRates).toHaveBeenCalledTimes(1);
      expect(second.rate).toBe(first.rate);
      // La segunda vez el dato no se acaba de pedir al mercado
      expect(second.source).toBe(RATE_SOURCES.CACHE);
    });

    it('aprovecha el rango ya traído para una fecha vecina', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({
        COP: { '2026-03-10': 3880, '2026-03-11': 3890, '2026-03-12': 3900.5 },
      }));

      await getRateForDate('COP', '2026-03-12');
      const vecina = await getRateForDate('COP', '2026-03-11');

      expect(mockGetExchangeRates).toHaveBeenCalledTimes(1);
      expect(vecina.rate).toBe(3890);
    });
  });

  describe('getCrossRate', () => {
    it('devuelve 1 cuando las dos divisas coinciden', async () => {
      const result = await getCrossRate('USD', 'USD', '2026-03-12');

      expect(result).toEqual({ rate: 1, rateDate: '2026-03-12', source: RATE_SOURCES.CACHE });
      expect(mockGetExchangeRates).not.toHaveBeenCalled();
    });

    it('deriva la tasa cruzada de las dos tasas en base USD', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({
        COP: { '2026-03-12': 4000 },
        EUR: { '2026-03-12': 0.8 },
      }));

      const result = await getCrossRate('EUR', 'COP', '2026-03-12');

      // 4000 COP por USD / 0,8 EUR por USD = 5000 COP por EUR
      expect(result.rate).toBeCloseTo(5000, 6);
    });

    it('pide las dos divisas juntas, no una llamada por cada una (RN-3-C)', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({
        COP: { '2026-03-12': 4000 },
        EUR: { '2026-03-12': 0.8 },
      }));

      await getCrossRate('EUR', 'COP', '2026-03-12');

      expect(mockGetExchangeRates).toHaveBeenCalledTimes(1);
      const [currencies] = mockGetExchangeRates.mock.calls[0];
      expect(currencies).toEqual(expect.arrayContaining(['EUR', 'COP']));
    });

    it('devuelve null si falta cualquiera de las dos tasas (RN-3-D)', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({ COP: { '2026-03-12': 4000 } }, ['EUR']));

      const result = await getCrossRate('EUR', 'COP', '2026-03-12');

      expect(result).toBeNull();
    });

    it('informa la fecha más lejana de las dos y marca el cierre anterior', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({
        COP: { '2026-03-12': 4000 },
        EUR: { '2026-03-11': 0.8 },
      }));

      const result = await getCrossRate('EUR', 'COP', '2026-03-12');

      expect(result.rateDate).toBe('2026-03-11');
      expect(result.source).toBe(RATE_SOURCES.PREVIOUS_CLOSE);
    });
  });

  describe('getRatesForRange', () => {
    it('pide todas las divisas del período en UNA llamada (RN-3-C)', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({
        COP: { '2026-03-10': 3880, '2026-03-11': 3890 },
        EUR: { '2026-03-10': 0.81, '2026-03-11': 0.82 },
      }));

      const result = await getRatesForRange(['COP', 'EUR'], '2026-03-10', '2026-03-11');

      expect(mockGetExchangeRates).toHaveBeenCalledTimes(1);
      expect(result['2026-03-10']).toEqual({ USD: 1, COP: 3880, EUR: 0.81 });
      expect(result['2026-03-11']).toEqual({ USD: 1, COP: 3890, EUR: 0.82 });
    });

    it('no devuelve nada fuera del rango pedido', async () => {
      mockGetExchangeRates.mockResolvedValue(channel({
        COP: { '2026-03-05': 3800, '2026-03-10': 3880, '2026-03-20': 3950 },
      }));

      const result = await getRatesForRange(['COP'], '2026-03-10', '2026-03-11');

      expect(Object.keys(result)).toEqual(['2026-03-10']);
    });

    it('no consulta nada si solo se pide USD', async () => {
      const result = await getRatesForRange(['USD'], '2026-03-10', '2026-03-11');

      expect(result).toEqual({});
      expect(mockGetExchangeRates).not.toHaveBeenCalled();
    });

    it('devuelve vacío cuando el canal no responde, sin tasas viejas (RN-3-D)', async () => {
      mockGetExchangeRates.mockRejectedValue(new Error('canal caído'));

      const result = await getRatesForRange(['COP'], '2026-03-10', '2026-03-11');

      expect(result).toEqual({});
    });

    it('rechaza un rango con fechas inválidas', async () => {
      await expect(getRatesForRange(['COP'], 'ayer', '2026-03-11')).rejects.toThrow('Rango inválido');
    });
  });
});
