/**
 * HU 6.10.1 — El catálogo de plazos: cortes, franja nocturna, lo que nunca se
 * retira y lo que ya tenía plazo.
 *
 * @see services/retention/retentionPolicies.js
 * @see platform-docs/stories/6.10.1-plazos-conservacion-datos/qa.md
 */

const fs = require('fs');
const path = require('path');

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => ({}),
  FieldValue: {},
  FieldPath: { documentId: () => '__name__' },
}));
jest.mock('../../firebaseAdmin', () => ({}));
jest.mock('firebase-functions/v2/scheduler', () => ({
  onSchedule: (_opts, handler) => handler,
}));

const {
  TIPOS,
  COLECCIONES_PROTEGIDAS,
  COLECCION_FALLOS_CIERRE,
  tipoPorId,
  corteDe,
  enHorarioNocturno,
  tramoDe,
  fechaDelDocumento,
  idDeConstancia,
} = require('../retentionPolicies');

/** La limpieza de referencia de qa.md: 2026-11-12, 06:00 ET. */
const LIMPIEZA = new Date('2026-11-12T11:00:00Z');

/** Una hora de Nueva York (noviembre: UTC-5). */
const horaNY = (hhmm, dia = '2026-11-12') => new Date(`${dia}T${hhmm}:00-05:00`);

describe('corteDe — los plazos de RN-6.10-J en días naturales de Nueva York (vacío 1)', () => {
  it('registros operativos, 90 días: el corte es el 2026-08-14 (QA-01, QA-16)', () => {
    // Se retira lo anterior al corte: 2026-08-13 (91 d) sí; 2026-08-14 (90 d) no
    expect(corteDe(tipoPorId('fallosCierre'), LIMPIEZA)).toEqual({
      fecha: '2026-08-14',
      iso: '2026-08-14T04:00:00.000Z',
    });
    expect('2026-08-13' < corteDe(tipoPorId('progresoCierre'), LIMPIEZA).fecha).toBe(true);
    expect('2026-08-14' < corteDe(tipoPorId('progresoCierre'), LIMPIEZA).fecha).toBe(false);
  });

  it('briefings, 1 año: el corte es el 2025-11-12 a las 00:00 NY (QA-03)', () => {
    expect(corteDe(tipoPorId('briefings'), LIMPIEZA)).toEqual({
      fecha: '2025-11-12',
      iso: '2025-11-12T05:00:00.000Z',
    });
  });

  it('resúmenes archivados, 2 años desde el archivo: el corte es el 2024-11-12 (QA-04)', () => {
    expect(corteDe(tipoPorId('resumenesArchivados'), LIMPIEZA).fecha).toBe('2024-11-12');
  });

  it('cuenta en la fecha de Nueva York aunque en UTC ya sea el día siguiente', () => {
    // 2026-11-12 22:30 NY = 2026-11-13 03:30 UTC: la limpieza sigue siendo del 12
    expect(corteDe(tipoPorId('fallosCierre'), new Date('2026-11-13T03:30:00Z')).fecha).toBe('2026-08-14');
  });
});

describe('enHorarioNocturno — RN-6.10.1-J, de 00:00 a 03:00 ET', () => {
  it.each([
    ['00:00', true],
    ['00:45', true],
    ['02:10', true],
    ['02:59', true],
    ['03:00', false],
    ['06:00', false],
    ['23:59', false],
  ])('a las %s ET → %s', (hhmm, esperado) => {
    expect(enHorarioNocturno(horaNY(hhmm))).toBe(esperado);
  });

  it('usa la hora de Nueva York también en verano (UTC-4)', () => {
    expect(enHorarioNocturno(new Date('2026-07-15T05:30:00Z'))).toBe(true); // 01:30 EDT
    expect(enHorarioNocturno(new Date('2026-07-15T07:30:00Z'))).toBe(false); // 03:30 EDT
  });
});

describe('tramoDe — el conteo del ensayo va por mes (vacío 7)', () => {
  it('toma el mes del id que empieza por la fecha', () => {
    expect(tramoDe('2026-05-20')).toBe('2026-05');
    expect(tramoDe('2026-08-13_020000')).toBe('2026-08');
  });

  it('pasa los ISO a Nueva York antes de tomar el mes', () => {
    // 2025-12-01 03:00 UTC es el 30 de noviembre en NY
    expect(tramoDe('2025-12-01T03:00:00.000000+00:00')).toBe('2025-11');
  });

  it('marca sin fecha lo que no se puede leer', () => {
    expect(tramoDe(undefined)).toBe('sin-fecha');
    expect(tramoDe('x')).toBe('sin-fecha');
  });
});

describe('fechaDelDocumento e idDeConstancia', () => {
  it('lee la fecha del id o del campo según el tipo', () => {
    expect(fechaDelDocumento(tipoPorId('metricasCorreccion'), '2026-05-20_020000', {})).toBe('2026-05-20_020000');
    expect(fechaDelDocumento(tipoPorId('briefings'), 'abc', { sentAt: '2025-11-11T12:30:00+00:00' }))
      .toBe('2025-11-11T12:30:00+00:00');
  });

  it('la constancia lleva la fecha y la hora de Nueva York, como reconcileNights', () => {
    expect(idDeConstancia(LIMPIEZA)).toBe('2026-11-12_060000');
  });
});

describe('el catálogo — RN-6.10-J, RN-6.10-K, RN-6.10-L', () => {
  it('tiene los ocho tipos acordados con sus plazos', () => {
    const plazos = Object.fromEntries(TIPOS.map((t) => [t.id, t.plazo]));
    expect(plazos).toEqual({
      fallosCierre: { days: 90 },
      fallosCorreccion: { days: 90 },
      metricasCorreccion: { days: 90 },
      constancias: { days: 90 },
      briefings: { years: 1 },
      resumenesArchivados: { years: 2 },
      preciosCierre: { days: 90 },
      progresoCierre: { days: 90 },
    });
  });

  it('apunta a las colecciones reales de cada dato', () => {
    const rutas = Object.fromEntries(TIPOS.map((t) => [t.id, t.ruta]));
    expect(rutas).toEqual({
      fallosCierre: 'eodFailures',
      fallosCorreccion: 'reconcileFailures',
      metricasCorreccion: 'reconcileNights',
      constancias: 'dataRetentionRuns',
      briefings: 'briefingHistory',
      resumenesArchivados: 'archivedSnapshots',
      preciosCierre: 'marketData/eod/dates',
      progresoCierre: 'eodProgress',
    });
    expect(tipoPorId('progresoCierre').subcolecciones).toEqual(['usuarios']);
    expect(tipoPorId('preciosCierre').subcolecciones).toEqual(['chunks']);
  });

  it('los fallos de la corrección cuentan desde la liberación, no desde el abandono (RN-6.10.1-K)', () => {
    expect(tipoPorId('fallosCorreccion').fecha).toEqual({ campo: 'resolvedAt' });
  });

  it('los resúmenes archivados cuentan desde que se archivaron (RN-6.10.1-L)', () => {
    expect(tipoPorId('resumenesArchivados').fecha).toEqual({ campo: 'archivedAt' });
  });

  it('QA-05: ningún tipo toca pagos, suscripciones, historiales de mercado ni datos de origen', () => {
    for (const tipo of TIPOS) {
      const primerSegmento = tipo.ruta.split('/')[0];
      expect(COLECCIONES_PROTEGIDAS).not.toContain(primerSegmento);
      expect(COLECCIONES_PROTEGIDAS).not.toContain(tipo.ruta);
      for (const sub of tipo.subcolecciones) expect(COLECCIONES_PROTEGIDAS).not.toContain(sub);
    }
    expect(COLECCIONES_PROTEGIDAS).toEqual(expect.arrayContaining([
      'subscriptionEvents', 'userData', 'indexHistories', 'sectorsHistory', 'currentPrices', 'portfolioPerformance',
    ]));
  });

  it('el literal de los fallos del cierre es el de eodFinalizationScheduled', () => {
    const fuente = fs.readFileSync(path.join(__dirname, '..', '..', 'eodFinalizationScheduled.js'), 'utf8');
    const literal = fuente.match(/const COLECCION_FALLOS = '([^']+)'/)[1];
    expect(COLECCION_FALLOS_CIERRE).toBe(literal);
  });
});

describe('QA-06 — los plazos que ya existían no cambian', () => {
  it('el catálogo no incluye ninguna colección que ya tenía plazo', () => {
    const rutas = TIPOS.map((t) => t.ruta);
    for (const existente of ['userNotifications', 'alertHistory', 'performanceSnapshots']) {
      expect(rutas).not.toContain(existente);
    }
  });

  it('notificaciones 90 días e historial de alertas 1 año, como antes', () => {
    const { NOTIFICATION_RETENTION_DAYS, HISTORY_RETENTION_DAYS } = require('../../alertRetention');
    expect(NOTIFICATION_RETENTION_DAYS).toBe(90);
    expect(HISTORY_RETENTION_DAYS).toBe(365);
  });

  it('los resúmenes de activos vendidos siguen con su retirada semanal tras un año', () => {
    const { SOLD_ASSET_GRACE_DAYS } = require('../../snapshotCleanup');
    expect(SOLD_ASSET_GRACE_DAYS).toBe(365);
  });
});
