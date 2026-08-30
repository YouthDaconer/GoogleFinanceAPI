/**
 * HU #3 — Tests de las tasas del backfill: por rango, sin archivo, sin fallback.
 *
 * Lo que estos tests defienden es el cambio que hace innecesaria la caché: el
 * coste de resolver un período crece con el **número de divisas**, nunca con el
 * de días. Antes era una llamada por día y divisa, con una pausa deliberada
 * entre cada una; un año con tres divisas eran más de mil llamadas encadenadas.
 *
 * Y defienden lo segundo, que es lo que produjo el efecto divisa falso: cuando
 * no hay tasa, el día se queda sin calcular. Nunca se rescata con la tasa
 * vigente de `currencies/{code}`, que nadie refresca.
 *
 * @module __tests__/services/backfillCoreModule.exchangeRates.test
 * @see platform-docs/stories/3-tasa-vigente-canal-mercado/refinamiento.md (T22)
 */

const mockGet = jest.fn();
const mockSet = jest.fn().mockResolvedValue();
const mockUpdate = jest.fn().mockResolvedValue();
const mockGetAll = jest.fn();
const mockDoc = jest.fn();
const mockCollection = jest.fn();
const mockWhere = jest.fn();

jest.mock('../firebaseAdmin', () => {
  mockWhere.mockReturnThis();

  mockDoc.mockReturnValue({
    get: mockGet,
    set: mockSet,
    update: mockUpdate,
    collection: mockCollection,
  });

  mockCollection.mockReturnValue({
    where: mockWhere,
    get: mockGet,
    doc: mockDoc,
  });

  const mockAdmin = {
    firestore: jest.fn(() => ({
      collection: mockCollection,
      doc: mockDoc,
      getAll: mockGetAll,
    })),
    __esModule: false,
  };

  mockAdmin.firestore.FieldValue = {
    serverTimestamp: () => 'SERVER_TIMESTAMP',
    increment: (n) => `INCREMENT_${n}`,
  };

  return mockAdmin;
});

jest.mock('node-fetch', () => {
  const fn = jest.fn().mockResolvedValue({
    json: () => Promise.resolve({ chart: { result: null } }),
    ok: true,
  });
  fn.default = fn;
  return fn;
});

// El canal de mercado, mockeado en su frontera real: el cliente HTTP.
const mockGetExchangeRates = jest.fn();
jest.mock('../financeQuery', () => ({
  getExchangeRates: (...args) => mockGetExchangeRates(...args),
}));

const { getExchangeRatesForDates, chunkArray } = require('../backfillCoreModule');
const { _resetMemory } = require('../historicalRateService');

/** Snapshot de query de Firestore con `forEach`, como el API real */
function mockQuerySnapshot(docs) {
  return {
    forEach: (fn) => docs.forEach(fn),
    docs,
    empty: docs.length === 0,
    size: docs.length,
  };
}

/** Documento de divisa activa del catálogo (sin `exchangeRate`, HU #3) */
const currencyDoc = (code) => ({ id: code, data: () => ({ code, isActive: true }) });

/** Genera N días consecutivos desde una fecha */
function daysFrom(start, count) {
  const days = [];
  const base = new Date(`${start}T12:00:00Z`);
  for (let i = 0; i < count; i++) {
    const d = new Date(base);
    d.setUTCDate(d.getUTCDate() + i);
    days.push(d.toISOString().substring(0, 10));
  }
  return days;
}

/** Serie completa de una divisa para los días indicados */
function seriesFor(days, rate) {
  return days.reduce((acc, day, index) => ({ ...acc, [day]: rate + index }), {});
}

describe('getExchangeRatesForDates (HU #3)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    _resetMemory();
    mockCollection.mockReturnValue({ where: mockWhere, get: mockGet, doc: mockDoc });
    mockWhere.mockReturnThis();
    // El catálogo sigue diciendo qué divisas están activas: eso sí es suyo
    mockGet.mockResolvedValue(mockQuerySnapshot([currencyDoc('COP'), currencyDoc('EUR')]));
  });

  it('resuelve un año con tres divisas con UNA consulta por divisa (RN-3-C)', async () => {
    const days = daysFrom('2026-01-05', 250);

    mockGet.mockResolvedValue(mockQuerySnapshot([
      currencyDoc('COP'), currencyDoc('EUR'), currencyDoc('MXN'),
    ]));
    mockGetExchangeRates.mockResolvedValue({
      base: 'USD',
      rates: {
        COP: seriesFor(days, 3900),
        EUR: seriesFor(days, 0.85),
        MXN: seriesFor(days, 17),
      },
      unavailable: [],
    });

    const result = await getExchangeRatesForDates(days);

    // Una sola consulta al canal, con las tres divisas dentro
    expect(mockGetExchangeRates).toHaveBeenCalledTimes(1);
    const [currencies, start, end] = mockGetExchangeRates.mock.calls[0];
    expect(currencies).toEqual(expect.arrayContaining(['COP', 'EUR', 'MXN']));
    expect(start <= days[0]).toBe(true);
    expect(end).toBe(days[days.length - 1]);

    expect(Object.keys(result)).toHaveLength(250);
    expect(result[days[0]].COP).toBe(3900);
  });

  it('no lee ni escribe la colección de tasas archivadas (RN-3-A)', async () => {
    const days = ['2026-03-10', '2026-03-11'];
    mockGetExchangeRates.mockResolvedValue({
      base: 'USD',
      rates: { COP: { '2026-03-10': 3880, '2026-03-11': 3890 }, EUR: {} },
      unavailable: ['EUR'],
    });

    await getExchangeRatesForDates(days);

    expect(mockGetAll).not.toHaveBeenCalled();
    const touchedPaths = mockDoc.mock.calls.map((call) => call[0]);
    expect(touchedPaths.some((path) => String(path).includes('historicalExchangeRates'))).toBe(false);
    expect(mockSet).not.toHaveBeenCalled();
  });

  it('arrastra el cierre anterior a los días sin cotización, dentro del mismo rango', async () => {
    // 2026-03-14 y 15 son fin de semana
    const days = ['2026-03-13', '2026-03-14', '2026-03-15', '2026-03-16'];

    mockGetExchangeRates.mockResolvedValue({
      base: 'USD',
      rates: { COP: { '2026-03-13': 3900, '2026-03-16': 3910 }, EUR: { '2026-03-13': 0.85, '2026-03-16': 0.86 } },
      unavailable: [],
    });

    const result = await getExchangeRatesForDates(days);

    expect(mockGetExchangeRates).toHaveBeenCalledTimes(1);
    expect(result['2026-03-14'].COP).toBe(3900);
    expect(result['2026-03-15'].COP).toBe(3900);
    expect(result['2026-03-16'].COP).toBe(3910);
  });

  it('deja el día sin la divisa cuando no hay tasa, en vez de rescatarla con la vigente (RN-3-D)', async () => {
    const days = ['2026-03-10', '2026-03-11'];

    mockGetExchangeRates.mockResolvedValue({
      base: 'USD',
      rates: { COP: { '2026-03-10': 3880, '2026-03-11': 3890 } },
      unavailable: ['EUR'],
    });

    const result = await getExchangeRatesForDates(days);

    expect(result['2026-03-10']).toEqual({ USD: 1, COP: 3880 });
    expect(result['2026-03-10'].EUR).toBeUndefined();
    // Nadie fue a buscar `currencies/{code}.exchangeRate`
    const touchedPaths = mockDoc.mock.calls.map((call) => String(call[0]));
    expect(touchedPaths.some((path) => path.startsWith('currencies/'))).toBe(false);
  });

  it('deja el día solo con USD cuando el canal no responde', async () => {
    mockGetExchangeRates.mockResolvedValue(null);

    const result = await getExchangeRatesForDates(['2026-03-10']);

    expect(result['2026-03-10']).toEqual({ USD: 1 });
  });

  it('devuelve vacío sin días que resolver', async () => {
    expect(await getExchangeRatesForDates([])).toEqual({});
    expect(mockGetExchangeRates).not.toHaveBeenCalled();
  });

  it('USD siempre vale 1 y no se le pregunta al mercado', async () => {
    mockGet.mockResolvedValue(mockQuerySnapshot([currencyDoc('COP')]));
    mockGetExchangeRates.mockResolvedValue({
      base: 'USD',
      rates: { COP: { '2026-03-10': 3880 } },
      unavailable: [],
    });

    const result = await getExchangeRatesForDates(['2026-03-10']);

    expect(result['2026-03-10'].USD).toBe(1);
    const [currencies] = mockGetExchangeRates.mock.calls[0];
    expect(currencies).not.toContain('USD');
  });
});

describe('chunkArray', () => {
  it('parte el array en trozos del tamaño pedido', () => {
    expect(chunkArray([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
  });

  it('devuelve un solo trozo cuando cabe entero', () => {
    expect(chunkArray([1, 2], 10)).toEqual([[1, 2]]);
  });

  it('devuelve vacío para un array vacío', () => {
    expect(chunkArray([], 10)).toEqual([]);
  });
});
