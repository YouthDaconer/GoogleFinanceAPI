/**
 * HU 6.14.1 — Las 3 entradas de junio de 2026 quedan con el cierre de su día.
 *
 * Los datos son los 20 movimientos reales de la cuenta Trii (saldo en COP,
 * referencia USD), leídos de producción el 2026-11-16. Los cierres del día son
 * los de la HU; los del día siguiente, las tasas que hoy tienen guardadas.
 *
 * @see scripts/rebuildBalanceCostBasisByDate.js
 * @see platform-docs/stories/6.14.1-tres-operaciones-tasa-corregida/qa.md
 */

jest.mock('../../services/firebaseAdmin', () => {
  const firestore = jest.fn(() => ({}));
  firestore.FieldValue = { serverTimestamp: () => 'TS' };
  return { firestore, auth: jest.fn() };
});
jest.mock('../../services/historicalRateService', () => ({ getCrossRate: jest.fn() }));

const {
  MOTIVOS,
  costoDe,
  esVeranoBritanico,
  diaSiguiente,
  decimalesDe,
  coincide,
  clasificarTasa,
  detectarCorridas,
  seleccionarPorId,
  planificarReemplazo,
  aplicarReemplazo,
} = require('../rebuildBalanceCostBasisByDate');

// ============================================================================
// DATOS: la cuenta Trii real
// ============================================================================

const mov = (id, type, { createdAtSeconds, ...datos }) => ({
  id,
  type,
  ...datos,
  createdAt: { _seconds: createdAtSeconds },
});

const trii = () => [
  mov('YLa1yzCf2LCxWckSDgFV', 'cash_adjustment', { amount: 100000, price: 1, commission: 0, currency: 'COP', date: '2026-01-08T04:47:27.013Z', adjustmentDelta: 100000, adjustmentReason: 'opening', acquisitionRate: 0.00026661, acquisitionRateSource: 'market-date', acquisitionCost: 26.66, realizedFxAvailability: 'not-applicable', createdAtSeconds: 1788065247 }),
  mov('sZa7BZjtfjUVYdpeRrQU', 'buy', { amount: 20, price: 2005, commission: 14875, currency: 'COP', date: '2026-01-08T19:39:45.848Z', createdAtSeconds: 1767901185 }),
  mov('IYKY64GrOCextl6zAfRa', 'cash_income', { amount: 115000, price: 1, commission: 0, currency: 'COP', date: '2026-02-05T20:21:32.008Z', acquisitionRate: 0.0002756172470453071, acquisitionRateSource: 'market-date', acquisitionCost: 31.7, createdAtSeconds: 1770322892 }),
  mov('33650U8fza39ca5Nl5hQ', 'buy', { amount: 2, price: 63560, commission: 14875, currency: 'COP', date: '2026-03-04T17:25:41.832Z', createdAtSeconds: 1772645141 }),
  mov('mWb7MWmz07Flc1FIzdm4', 'cash_income', { amount: 71000, price: 1, commission: 0, currency: 'COP', date: '2026-03-04T17:27:35.483Z', acquisitionRate: 0.00026307066536647076, acquisitionRateSource: 'market-date', acquisitionCost: 18.68, createdAtSeconds: 1772645255 }),
  mov('txhIe9N5YVexZupbG0pU', 'buy', { amount: 5, price: 12500, commission: 14875, currency: 'COP', date: '2026-03-04T17:27:54.594Z', createdAtSeconds: 1772645274 }),
  mov('xYptWnaKfMr38qVzjVwd', 'cash_income', { amount: 52000, price: 1, commission: 0, currency: 'COP', date: '2026-03-04T19:53:49.332Z', acquisitionRate: 0.00026307066536647076, acquisitionRateSource: 'market-date', acquisitionCost: 13.68, createdAtSeconds: 1772654029 }),
  mov('QqukFJSFgLMwU6KufDKO', 'buy', { amount: 5, price: 4805, commission: 4875, currency: 'COP', date: '2026-03-04T19:56:21.282Z', createdAtSeconds: 1772654181 }),
  mov('EwjMm7Rgw6SRlgvNkRs5', 'cash_income', { amount: 194000, price: 1, commission: 0, currency: 'COP', date: '2026-03-04T20:05:22.824Z', acquisitionRate: 0.00026307066536647076, acquisitionRateSource: 'market-date', acquisitionCost: 51.04, createdAtSeconds: 1772654722 }),
  mov('MIQoKwZZTSRMlJuBceY6', 'buy', { amount: 3, price: 42080, commission: 14875, currency: 'COP', date: '2026-03-04T20:07:57.795Z', createdAtSeconds: 1772654877 }),
  mov('ckE4A8n2y5FtK2Gte14k', 'buy', { amount: 3, price: 15440, commission: 14875, currency: 'COP', date: '2026-03-04T20:10:45.591Z', createdAtSeconds: 1772655045 }),
  mov('otsX8V1M3k53cwzYbaOL', 'sell', { amount: 5, price: 9002, commission: 133.9, currency: 'COP', date: '2026-06-03T17:06:25.730Z', realizationRate: 0.00027971479743861555, realizationRateSource: 'market-date', acquisitionCost: 12.55, createdAtSeconds: 1780506385 }),
  mov('r3CFKCiK8yCFdsXKfPmM', 'cash_income', { amount: 250000, price: 1, commission: 0, currency: 'COP', date: '2026-06-10T17:08:15.193Z', acquisitionRate: 0.0002810038643519705, acquisitionRateSource: 'market-date', acquisitionCost: 70.25, createdAtSeconds: 1781111295 }),
  mov('O0eskvKxl9K2xrZ1mU3I', 'buy', { amount: 94, price: 2970, commission: 14875, currency: 'COP', date: '2026-06-10T17:08:49.240Z', createdAtSeconds: 1781111329 }),
  mov('eJwWwZc48xk0YKGu0dgB', 'cash_income', { amount: 600000, price: 1, commission: 0, currency: 'COP', date: '2026-06-22T14:12:08.433Z', acquisitionRate: 0.00029041907060650576, acquisitionRateSource: 'market-date', acquisitionCost: 174.25, createdAtSeconds: 1782137528 }),
  mov('U8Jm6YveU43ABFkUmLYZ', 'buy', { amount: 140, price: 2780, commission: 14875, currency: 'COP', date: '2026-06-22T14:12:14.019Z', createdAtSeconds: 1782137534 }),
  mov('5CHhOnRvRbla2qWH3ai9', 'cash_expense', { amount: 223191.1, price: 1, commission: 0, currency: 'COP', date: '2026-08-30T19:39:31.017Z', acquisitionRate: 0.00031231, acquisitionRateSource: 'cache', realizationRate: 0.000312, realizationRateSource: 'market-date', acquisitionCost: 69.70408977, releasedCost: 64.71, realizedFxAmount: 0, realizedFxAvailability: 'available', createdAtSeconds: 1788118771 }),
  mov('DOXcpFBoUIxIPlh6HPXX', 'sell', { amount: 2, price: 78600, commission: 14875, currency: 'COP', date: '2026-09-09T19:03:24.963Z', acquisitionRate: 0.00026307, acquisitionRateSource: 'market-date', realizationRate: 0.00032209, realizationRateSource: 'market-date', acquisitionCost: 45.84, realizedFxAmount: 9.28, realizedFxAvailability: 'available', createdAtSeconds: 1788980605 }),
  mov('d2W2t1iqL1Fn9nuleih5', 'sell', { amount: 3, price: 56560, commission: 14875, currency: 'COP', date: '2026-10-02T19:14:03.961Z', acquisitionRate: 0.00026307, acquisitionRateSource: 'market-date', realizationRate: 0.00030650999999999996, realizationRateSource: 'market-date', acquisitionCost: 47.45, realizedFxAmount: 7.37, realizedFxAvailability: 'available', createdAtSeconds: 1790968445 }),
  mov('TOzFlHN9jLxgIV1bdFuU', 'cash_expense', { amount: 297130, price: 1, commission: 0, currency: 'COP', date: '2026-10-09T18:55:50.566Z', acquisitionRate: 0.00031392, acquisitionRateSource: 'cache', realizationRate: 0.000314, realizationRateSource: 'market-date', acquisitionCost: 93.27607398, releasedCost: 93.29, realizedFxAmount: 0, realizedFxAvailability: 'available', createdAtSeconds: 1791572151 }),
];

const OP1 = 'otsX8V1M3k53cwzYbaOL';
const OP2 = 'r3CFKCiK8yCFdsXKfPmM';
const OP3 = 'eJwWwZc48xk0YKGu0dgB';
const RET1 = '5CHhOnRvRbla2qWH3ai9';
const RET2 = 'TOzFlHN9jLxgIV1bdFuU';
const CUENTA = 'ggM52GimbLL7jwvegc9o';

/**
 * Cierres COP→USD (USD por COP). Junio: los de la HU para su día; para el
 * siguiente, la tasa que la serie corrida dejó guardada. Septiembre y octubre:
 * ninguno coincide con lo guardado (las ventas de esos meses no encajan).
 */
const CIERRES = {
  '2026-06-03': 1 / 3587.26,
  '2026-06-04': 0.00027971479743861555,
  '2026-06-10': 1 / 3574.94,
  '2026-06-11': 0.0002810038643519705,
  '2026-06-22': 1 / 3445.19,
  '2026-06-23': 0.00029041907060650576,
  '2026-07-15': 1 / 3610.22,
  '2026-07-16': 1 / 3601.5,
  '2026-09-09': 1 / 3890.1,
  '2026-09-10': 1 / 3880.4,
  '2026-10-02': 1 / 3850.2,
  '2026-10-03': 1 / 3845.7,
};
const obtenerCierre = jest.fn(async (dia) => (CIERRES[dia] !== undefined ? CIERRES[dia] : null));

const planDe = (transactions, reemplazos, overrides = {}) => planificarReemplazo({
  transactions,
  currency: 'COP',
  referenceCurrency: 'USD',
  balance: 0,
  storedBasis: { cost: 0, status: 'known' },
  reemplazos,
  ...overrides,
});

const reemplazosDe = (seleccion) => seleccion.corregir.map((c) => ({ id: c.id, tasa: c.tasa, costo: c.costo }));

/** Los movimientos tal como quedan tras escribir un plan. */
function aplicarEnMemoria(transactions, plan) {
  return transactions.map((t) => {
    const m = plan.movimientos.find((x) => x.id === t.id);
    if (m) return { ...t, [m.campoTasa]: m.tasaDespues, acquisitionCost: m.costoDespues };
    const r = plan.retiros.find((x) => x.id === t.id);
    if (r) return { ...t, releasedCost: r.costoLiberadoDespues };
    return t;
  });
}

beforeEach(() => obtenerCierre.mockClear());

// ============================================================================
// PIEZAS PURAS
// ============================================================================

describe('esVeranoBritanico — RN-6.14.1-A, solo los días en que la serie estaba corrida', () => {
  it('va del último domingo de marzo al último de octubre de 2026', () => {
    expect(esVeranoBritanico('2026-03-28')).toBe(false);
    expect(esVeranoBritanico('2026-03-29')).toBe(true);
    expect(esVeranoBritanico('2026-06-03')).toBe(true);
    expect(esVeranoBritanico('2026-10-24')).toBe(true);
    expect(esVeranoBritanico('2026-10-25')).toBe(false);
    expect(esVeranoBritanico('2026-02-05')).toBe(false);
  });

  it('el día siguiente cruza meses y años', () => {
    expect(diaSiguiente('2026-06-30')).toBe('2026-07-01');
    expect(diaSiguiente('2026-12-31')).toBe('2027-01-01');
  });
});

describe('coincide — la precisión guardada decide cuánto se puede distinguir', () => {
  it('cuenta los decimales guardados, también con exponente', () => {
    expect(decimalesDe(0.000312)).toBe(6);
    expect(decimalesDe(0.00032209)).toBe(8);
    expect(decimalesDe(3e-7)).toBe(7);
  });

  it('con precisión completa solo vale el mismo número', () => {
    expect(coincide(0.0002810038643519705, 0.0002810038643519705)).toBe(true);
    expect(coincide(0.0002810038643519705, 1 / 3574.94)).toBe(false);
  });

  it('con 8 decimales admite media unidad del último', () => {
    expect(coincide(0.00032209, 0.000322093)).toBe(true);
    expect(coincide(0.00032209, 0.0003221)).toBe(false);
  });

  it('con 6 decimales dos cierres cercanos quedan ambiguos', () => {
    expect(clasificarTasa(0.000312, 0.0003118, 0.0003124)).toBe(MOTIVOS.AMBIGUA);
  });

  it('clasifica por el cierre con el que coincide', () => {
    expect(clasificarTasa(0.0002810038643519705, 1 / 3574.94, 0.0002810038643519705)).toBe(MOTIVOS.CORREGIR);
    expect(clasificarTasa(1 / 3574.94, 1 / 3574.94, 0.0002810038643519705)).toBe(MOTIVOS.YA_CORRECTA);
    expect(clasificarTasa(1 / 3500, 1 / 3574.94, 0.0002810038643519705)).toBe(MOTIVOS.NO_ENCAJA);
    expect(clasificarTasa(1 / 3500, null, 0.0002810038643519705)).toBe(MOTIVOS.SIN_CIERRE);
  });

  it('el costo se redondea a 2 decimales, como lo escriben los demás (Decisión 3)', () => {
    expect(costoDe(250000, 1 / 3574.94)).toBe(69.93);
  });
});

// ============================================================================
// DETECCIÓN
// ============================================================================

describe('detectarCorridas — QA-05: solo las 3 de junio', () => {
  it('encuentra exactamente las 3 entradas con el cierre del día siguiente', async () => {
    const { corregir } = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });

    expect(corregir.map((c) => [c.id, c.fecha, c.tasa])).toEqual([
      [OP1, '2026-06-03', 1 / 3587.26],
      [OP2, '2026-06-10', 1 / 3574.94],
      [OP3, '2026-06-22', 1 / 3445.19],
    ]);
  });

  it('lista con su motivo lo que no toca: invierno y las ventas de septiembre y octubre', async () => {
    const { noTocadas } = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });

    expect(noTocadas.map((n) => [n.id, n.motivo])).toEqual([
      ['YLa1yzCf2LCxWckSDgFV', MOTIVOS.INVIERNO],
      ['IYKY64GrOCextl6zAfRa', MOTIVOS.INVIERNO],
      ['mWb7MWmz07Flc1FIzdm4', MOTIVOS.INVIERNO],
      ['xYptWnaKfMr38qVzjVwd', MOTIVOS.INVIERNO],
      ['EwjMm7Rgw6SRlgvNkRs5', MOTIVOS.INVIERNO],
      ['DOXcpFBoUIxIPlh6HPXX', MOTIVOS.NO_ENCAJA],
      ['d2W2t1iqL1Fn9nuleih5', MOTIVOS.NO_ENCAJA],
    ]);
  });

  it('no mira los retiros: no son entradas', async () => {
    const { corregir, noTocadas } = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });
    const ids = [...corregir, ...noTocadas].map((x) => x.id);

    expect(ids).not.toContain(RET1);
    expect(ids).not.toContain(RET2);
  });

  it('no toca una tasa escrita a mano, una ya correcta ni una posterior al arreglo', async () => {
    const extra = [
      mov('manual', 'cash_income', { amount: 1000, currency: 'COP', date: '2026-06-10T12:00:00Z', acquisitionRate: 0.0002810038643519705, acquisitionRateSource: 'user', acquisitionCost: 0.28, createdAtSeconds: 1 }),
      mov('correcta', 'cash_income', { amount: 1000, currency: 'COP', date: '2026-07-15T12:00:00Z', acquisitionRate: 1 / 3610.22, acquisitionRateSource: 'market-date', acquisitionCost: 0.28, createdAtSeconds: 2 }),
      mov('nueva', 'cash_income', { amount: 1000, currency: 'COP', date: '2026-11-14T12:00:00Z', acquisitionRate: 0.00025, acquisitionRateSource: 'market-date', acquisitionCost: 0.25, createdAtSeconds: 3 }),
    ];
    const { corregir, noTocadas } = await detectarCorridas(extra, { currency: 'COP', obtenerCierre });

    expect(corregir).toEqual([]);
    expect(noTocadas.map((n) => [n.id, n.motivo])).toEqual([
      ['manual', MOTIVOS.OTRA_FUENTE],
      ['correcta', MOTIVOS.YA_CORRECTA],
      ['nueva', MOTIVOS.POSTERIOR_AL_ARREGLO],
    ]);
  });
});

// ============================================================================
// PLAN
// ============================================================================

describe('planificarReemplazo — QA-01, QA-02, QA-03: lo que cambia y lo que no', () => {
  let plan;
  beforeAll(async () => {
    const seleccion = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });
    plan = planDe(trii(), reemplazosDe(seleccion));
  });

  it('las 3 entradas quedan con el cierre de su día y su costo 12,51, 69,93 y 174,16 USD', () => {
    expect(plan.movimientos.map((m) => [m.id, m.campoTasa, m.tasaAntes, m.tasaDespues, m.costoAntes, m.costoDespues]))
      .toEqual([
        [OP1, 'realizationRate', 0.00027971479743861555, 1 / 3587.26, 12.55, 12.51],
        [OP2, 'acquisitionRate', 0.0002810038643519705, 1 / 3574.94, 70.25, 69.93],
        [OP3, 'acquisitionRate', 0.00029041907060650576, 1 / 3445.19, 174.25, 174.16],
      ]);
  });

  it('el retiro del 2026-08-30 libera 64,67 USD en vez de 64,71; el del 2026-10-09 no cambia', () => {
    expect(plan.retiros).toEqual([{
      id: RET1,
      fecha: '2026-08-30',
      monto: 223191.1,
      costoLiberadoAntes: 64.71,
      costoLiberadoDespues: 64.67,
    }]);
  });

  it('el saldo en COP no cambia: sigue en 0 con costo 0 y conciliado', () => {
    expect(plan.base).toMatchObject({ saldo: 0, costoAntes: 0, costoDespues: 0, estado: 'known' });
    expect(plan.base.conciliacion.status).toBe('reconciled');
  });

  it('marca desde la primera entrada y no hay dependientes que no sepa recalcular', () => {
    expect(plan.desde).toBe('2026-06-03');
    expect(plan.noSoportados).toEqual([]);
  });

  it('preparar el plan no toca los movimientos que recibe', () => {
    const transactions = trii();
    const copia = JSON.parse(JSON.stringify(transactions));
    planDe(transactions, plan.movimientos.map((m) => ({ id: m.id, tasa: m.tasaDespues })));

    expect(transactions).toEqual(copia);
  });

  it('una compra posterior valorada al promedio del saldo deja la cuenta sin tocar', () => {
    const conCompra = [...trii(), mov('compraPromedio', 'buy', {
      amount: 10, price: 3000, commission: 0, currency: 'COP', date: '2026-07-01T15:00:00Z',
      acquisitionRate: 0.000279, acquisitionRateSource: 'balance-average', createdAtSeconds: 1782900000,
    })];
    const conDependiente = planDe(conCompra, plan.movimientos.map((m) => ({ id: m.id, tasa: m.tasaDespues })));

    expect(conDependiente.noSoportados).toEqual([{
      id: 'compraPromedio', fecha: '2026-07-01', tipo: 'buy', motivo: 'compra valorada al promedio del saldo',
    }]);
  });
});

// ============================================================================
// RELANZAR Y DESHACER
// ============================================================================

describe('relanzar — QA-06: con las tasas ya corregidas no hay nada que cambiar', () => {
  it('la detección no encuentra ninguna y el plan sale vacío', async () => {
    const seleccion = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });
    const corregidas = aplicarEnMemoria(trii(), planDe(trii(), reemplazosDe(seleccion)));

    const otraVez = await detectarCorridas(corregidas, { currency: 'COP', obtenerCierre });
    const plan = planDe(corregidas, reemplazosDe(otraVez));

    expect(otraVez.corregir).toEqual([]);
    expect(otraVez.noTocadas.filter((n) => n.motivo === MOTIVOS.YA_CORRECTA).map((n) => n.id)).toEqual([OP1, OP2, OP3]);
    expect(plan).toEqual({ movimientos: [], retiros: [], noSoportados: [], desde: null, base: null });
  });

  it('pedirlas por id tampoco cambia nada', async () => {
    const seleccion = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });
    const corregidas = aplicarEnMemoria(trii(), planDe(trii(), reemplazosDe(seleccion)));

    const porId = await seleccionarPorId(corregidas, [OP1, OP2, OP3], { currency: 'COP', obtenerCierre });

    expect(porId.corregir).toEqual([]);
    expect(planDe(corregidas, reemplazosDe(porId)).movimientos).toEqual([]);
  });
});

describe('deshacer con la constancia — QA-07 (RN-6-H)', () => {
  let corregidas;
  let constancia;
  beforeAll(async () => {
    const seleccion = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });
    const plan = planDe(trii(), reemplazosDe(seleccion));
    corregidas = aplicarEnMemoria(trii(), plan);
    constancia = { cuentas: [{ cuentaId: CUENTA, movimientos: plan.movimientos, retiros: plan.retiros }] };
  });

  it('devolver una entrada le vuelve su tasa y su costo exactos y deja las otras corregidas', async () => {
    const seleccion = await seleccionarPorId(corregidas, [OP2], { currency: 'COP', obtenerCierre, constanciaPrevia: constancia });
    const plan = planDe(corregidas, reemplazosDe(seleccion));

    expect(plan.movimientos.map((m) => [m.id, m.tasaDespues, m.costoDespues]))
      .toEqual([[OP2, 0.0002810038643519705, 70.25]]);
    expect(plan.desde).toBe('2026-06-10');
    expect(obtenerCierre).not.toHaveBeenCalled();
  });

  it('el retiro recupera solo la parte que dependía de esa entrada', async () => {
    const seleccion = await seleccionarPorId(corregidas, [OP2], { currency: 'COP', obtenerCierre, constanciaPrevia: constancia });
    const plan = planDe(corregidas, reemplazosDe(seleccion));

    // De los +0,32 USD de esa entrada, la compra de ECOPETROL del mismo día se
    // llevó el 91,5 % y la del 06-22 el 64 % de lo que quedaba: al retiro le
    // llegan +0,0097 → 64,68.
    expect(plan.retiros).toEqual([expect.objectContaining({ id: RET1, costoLiberadoAntes: 64.67, costoLiberadoDespues: 64.68 })]);
  });

  it('devolver las 3 deja la cuenta como estaba, retiro incluido', async () => {
    const seleccion = await seleccionarPorId(corregidas, [OP1, OP2, OP3], { currency: 'COP', obtenerCierre, constanciaPrevia: constancia });
    const plan = planDe(corregidas, reemplazosDe(seleccion));
    const vuelta = aplicarEnMemoria(corregidas, plan);

    expect(vuelta).toEqual(trii());
  });

  it('una entrada que no está en la constancia no se toca', async () => {
    const seleccion = await seleccionarPorId(corregidas, ['IYKY64GrOCextl6zAfRa'], { currency: 'COP', obtenerCierre, constanciaPrevia: constancia });

    expect(seleccion.corregir).toEqual([]);
    expect(seleccion.noTocadas).toEqual([expect.objectContaining({ id: 'IYKY64GrOCextl6zAfRa', motivo: MOTIVOS.NO_ESTA_EN_CONSTANCIA })]);
  });

  it('un retiro pedido por id no es una entrada', async () => {
    const seleccion = await seleccionarPorId(trii(), [RET1], { currency: 'COP', obtenerCierre });

    expect(seleccion.noTocadas).toEqual([expect.objectContaining({ id: RET1, motivo: MOTIVOS.NO_ES_ENTRADA })]);
  });
});

// ============================================================================
// ESCRITURA
// ============================================================================

describe('aplicarReemplazo — QA-01, QA-03: constancia primero, lo escrito es el plan, y la marca', () => {
  /** Firestore mínimo: anota cada paso en orden. */
  function crearDb(pasos) {
    return {
      collection: (nombre) => ({ doc: (id) => ({ path: `${nombre}/${id}` }) }),
      batch: () => {
        const updates = [];
        return {
          update: (ref, datos) => updates.push({ ruta: ref.path, datos }),
          commit: async () => pasos.push({ paso: 'lote', updates }),
        };
      },
    };
  }

  it('guarda la constancia antes de escribir, escribe lo planificado y marca desde el 2026-06-03', async () => {
    const seleccion = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });
    const plan = planDe(trii(), reemplazosDe(seleccion));
    const pasos = [];
    const constancia = { historia: '6.14.1' };

    const resultado = await aplicarReemplazo({
      db: crearDb(pasos),
      FieldValue: { serverTimestamp: () => 'TS' },
      userId: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
      currency: 'COP',
      referenceCurrency: 'USD',
      cuentas: [{ accountId: CUENTA, plan }],
      constancia,
      escribirConstancia: (c) => pasos.push({ paso: 'constancia', c }),
      marcarDesactualizado: async (uid, desde, meta) => pasos.push({ paso: 'marca', uid, desde, meta }),
    });

    expect(pasos.map((p) => p.paso)).toEqual(['constancia', 'lote', 'marca']);
    expect(pasos[0].c).toBe(constancia);
    expect(pasos[1].updates).toEqual([
      { ruta: `transactions/${OP1}`, datos: { realizationRate: 1 / 3587.26, acquisitionCost: 12.51 } },
      { ruta: `transactions/${OP2}`, datos: { acquisitionRate: 1 / 3574.94, acquisitionCost: 69.93 } },
      { ruta: `transactions/${OP3}`, datos: { acquisitionRate: 1 / 3445.19, acquisitionCost: 174.16 } },
      { ruta: `transactions/${RET1}`, datos: { releasedCost: 64.67 } },
      {
        ruta: `portfolioAccounts/${CUENTA}`,
        datos: {
          'balanceCostBasis.COP': {
            cost: 0, referenceCurrency: 'USD', status: 'known', source: 'ledger-replay',
            convertedAmount: 0, convertedCost: 0, updatedAt: 'TS',
          },
          'balanceReconciliation.COP': {
            ledgerBalance: 0, difference: 0, status: 'reconciled', checkedAt: 'TS',
          },
        },
      },
    ]);
    expect(pasos[2]).toMatchObject({
      uid: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
      desde: '2026-06-03',
      meta: { reason: 'exchange-rate-correction', portfolioAccount: CUENTA },
    });
    expect(resultado).toEqual({ escrituras: 6, desde: '2026-06-03' });
  });

  it('si la constancia no se puede guardar, no escribe nada', async () => {
    const seleccion = await detectarCorridas(trii(), { currency: 'COP', obtenerCierre });
    const plan = planDe(trii(), reemplazosDe(seleccion));
    const pasos = [];

    await expect(aplicarReemplazo({
      db: crearDb(pasos),
      FieldValue: { serverTimestamp: () => 'TS' },
      userId: 'u',
      currency: 'COP',
      referenceCurrency: 'USD',
      cuentas: [{ accountId: CUENTA, plan }],
      constancia: {},
      escribirConstancia: () => { throw new Error('disco lleno'); },
      marcarDesactualizado: async () => pasos.push({ paso: 'marca' }),
    })).rejects.toThrow('disco lleno');

    expect(pasos).toEqual([]);
  });
});
