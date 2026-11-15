/**
 * HU 6.10.1 — El disparador diario: hora, horario y medición del espacio.
 *
 * @see services/retention/retentionScheduled.js
 */

jest.mock('firebase-admin/firestore', () => ({ FieldValue: {} }));
jest.mock('../../firebaseAdmin', () => ({
  firestore: () => ({ falso: true }),
  credential: { applicationDefault: () => ({ getAccessToken: async () => ({ access_token: 'token-adc' }) }) },
}));
jest.mock('firebase-functions/v2/scheduler', () => ({
  onSchedule: (opts, handler) => Object.assign(handler, { __opts: opts }),
}));
jest.mock('../../consumption/consumptionMetricsService', () => ({
  medirAlmacenamiento: jest.fn(),
}));
jest.mock('../retentionService', () => ({
  ejecutarLimpieza: jest.fn(async () => ({ estado: 'completa' })),
}));

const { medirAlmacenamiento } = require('../../consumption/consumptionMetricsService');
const { ejecutarLimpieza } = require('../retentionService');
const { enHorarioNocturno } = require('../retentionPolicies');
const {
  scheduledDataRetention,
  crearMedidorDeEspacio,
  tokenDelRuntime,
  FUNCTION_CONFIG,
} = require('../retentionScheduled');

describe('scheduledDataRetention — diaria a las 06:00 ET (decisión del PO, RN-6.10.1-J)', () => {
  it('corre todos los días a las 06:00 de Nueva York, con un solo proceso y sin reintentos', () => {
    expect(FUNCTION_CONFIG).toMatchObject({
      schedule: '0 6 * * *',
      timeZone: 'America/New_York',
      timeoutSeconds: 540,
      maxInstances: 1,
      minInstances: 0,
      retryCount: 0,
    });
    expect(scheduledDataRetention.__opts).toBe(FUNCTION_CONFIG);
  });

  it('su hora queda fuera del horario nocturno, en invierno y en verano', () => {
    expect(enHorarioNocturno(new Date('2026-11-12T11:00:00Z'))).toBe(false); // 06:00 EST
    expect(enHorarioNocturno(new Date('2026-07-15T10:00:00Z'))).toBe(false); // 06:00 EDT
  });

  it('lanza la limpieza programada con un medidor de espacio', async () => {
    await scheduledDataRetention();
    expect(ejecutarLimpieza).toHaveBeenCalledWith({ falso: true }, expect.objectContaining({
      origen: 'programada',
      medirEspacio: expect.any(Function),
    }));
  });
});

describe('crearMedidorDeEspacio — RN-6-I, el espacio lo mide la plataforma', () => {
  beforeEach(() => medirAlmacenamiento.mockReset());

  it('devuelve el último punto de la serie de almacenamiento', async () => {
    medirAlmacenamiento.mockResolvedValue([
      { fin: '2026-11-12T10:58:00.000Z', valor: 381 },
      { fin: '2026-11-12T10:59:00.000Z', valor: 380 },
    ]);
    const ahora = new Date('2026-11-12T11:00:00Z');
    const medir = crearMedidorDeEspacio({ obtenerToken: async () => 't-1', ahora: () => ahora });

    expect(await medir()).toBe(380);
    expect(medirAlmacenamiento).toHaveBeenCalledWith({ token: 't-1', hasta: ahora });
  });

  it('sin puntos devuelve null', async () => {
    medirAlmacenamiento.mockResolvedValue([]);
    expect(await crearMedidorDeEspacio({ obtenerToken: async () => 't' })()).toBeNull();
  });

  it('el token sale de la credencial del runtime', async () => {
    expect(await tokenDelRuntime()).toBe('token-adc');
  });
});
