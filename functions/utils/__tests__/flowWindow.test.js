/**
 * FLOW-WINDOW: el flujo de un día es todo lo movido desde el último doc.
 */
const {
  MAX_FLOW_WINDOW_DAYS,
  getDatePart,
  isFlowOfDay,
  minusDays,
  flowWindowFor,
  queryWindowStart,
  queryBounds,
} = require('../flowWindow');

describe('getDatePart', () => {
  test('lee ISO con hora, fecha sola, Date y Timestamp', () => {
    expect(getDatePart('2026-07-22T15:04:05.000Z')).toBe('2026-07-22');
    expect(getDatePart('2026-07-22')).toBe('2026-07-22');
    expect(getDatePart(new Date('2026-07-22T15:00:00Z'))).toBe('2026-07-22');
    expect(getDatePart({ toDate: () => new Date('2026-07-22T15:00:00Z') })).toBe('2026-07-22');
    expect(getDatePart(null)).toBe('');
  });
});

describe('isFlowOfDay', () => {
  test('sin doc previo sólo cuenta el día', () => {
    expect(isFlowOfDay('2026-07-23T10:00:00Z', '2026-07-23')).toBe(true);
    expect(isFlowOfDay('2026-07-22T10:00:00Z', '2026-07-23')).toBe(false);
  });

  test('con doc previo cuenta (último doc, día]', () => {
    expect(isFlowOfDay('2026-07-22T14:47:36Z', '2026-07-23', '2026-07-21')).toBe(true);
    expect(isFlowOfDay('2026-07-23T10:00:00Z', '2026-07-23', '2026-07-21')).toBe(true);
    // Lo del día del último doc ya entró en ese doc
    expect(isFlowOfDay('2026-07-21T20:00:00Z', '2026-07-23', '2026-07-21')).toBe(false);
    // Lo posterior al día que se cierra es del siguiente
    expect(isFlowOfDay('2026-07-24T01:00:00Z', '2026-07-23', '2026-07-21')).toBe(false);
  });

  test('una transacción sin fecha nunca es flujo', () => {
    expect(isFlowOfDay(undefined, '2026-07-23', '2026-07-21')).toBe(false);
  });
});

describe('flowWindowFor', () => {
  test('el doc de ayer da la ventana de un día', () => {
    expect(flowWindowFor('2026-07-23', '2026-07-22')).toEqual({ fromExclusive: '2026-07-22', capped: false });
  });

  test('un doc faltante amplía la ventana hasta el último que existe', () => {
    expect(flowWindowFor('2026-07-23', '2026-07-21')).toEqual({ fromExclusive: '2026-07-21', capped: false });
  });

  test('un fin de semana largo cabe', () => {
    expect(flowWindowFor('2026-09-08', '2026-09-04')).toEqual({ fromExclusive: '2026-09-04', capped: false });
  });

  test('un hueco mayor que el tope se recorta y se avisa', () => {
    const out = flowWindowFor('2026-07-23', '2026-06-01');
    expect(out.capped).toBe(true);
    expect(out.fromExclusive).toBe(minusDays('2026-07-23', MAX_FLOW_WINDOW_DAYS));
  });

  test('sin doc previo no hay ventana (primer cálculo)', () => {
    expect(flowWindowFor('2026-07-23', null)).toEqual({ fromExclusive: null, capped: false });
  });
});

describe('queryWindowStart / queryBounds', () => {
  test('con todos los docs de ayer es exactamente el día', () => {
    const start = queryWindowStart('2026-07-23', ['2026-07-22', '2026-07-22', null]);
    expect(start).toBe('2026-07-22');
    expect(queryBounds(start, '2026-07-23')).toEqual({
      after: '2026-07-22T23:59:59.999Z',
      until: '2026-07-23T23:59:59.999Z',
    });
  });

  test('empieza en el último doc más antiguo de los ámbitos', () => {
    expect(queryWindowStart('2026-07-23', ['2026-07-22', '2026-07-21'])).toBe('2026-07-21');
  });

  test('el rango string incluye fechas con y sin hora del día y excluye el día base', () => {
    const { after, until } = queryBounds('2026-07-21', '2026-07-23');
    const inRange = (d) => d > after && d <= until;
    expect(inRange('2026-07-22')).toBe(true);
    expect(inRange('2026-07-22T00:00:00.000Z')).toBe(true);
    expect(inRange('2026-07-23T23:59:59.000Z')).toBe(true);
    expect(inRange('2026-07-21T23:00:00.000Z')).toBe(false);
    expect(inRange('2026-07-21')).toBe(false);
    expect(inRange('2026-07-24T00:00:00.000Z')).toBe(false);
  });
});
