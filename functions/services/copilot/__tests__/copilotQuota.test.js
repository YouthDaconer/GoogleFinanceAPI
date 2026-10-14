/**
 * El Copilot es de Pro y Lifetime, con un tope de análisis por día.
 *
 * Este archivo ha cambiado de regla dos veces: "sólo Pro, 1 cada 7 días" en la
 * HU 5.2, "todos los planes, 1 cada 7 días para el gratuito" en la 5.5, y ahora
 * "sólo Pro, N al día". Lo que no cambia es que **se cuenta en un solo sitio**:
 * la fecha que calcula este módulo es la que aplica el lanzador y la que ve el
 * usuario.
 *
 * @see services/copilot/copilotQuota.js
 */

const firestoreFalso = {};

jest.mock('../../firebaseAdmin', () => ({
  firestore: () => firestoreFalso,
  firestore_: null,
}));

const admin = require('../../firebaseAdmin');
admin.firestore.FieldValue = { increment: (n) => ({ __increment: n }) };

const quota = require('../copilotQuota');

/** Las 14:00 de Nueva York: media jornada, lejos de cualquier frontera de día. */
const AHORA = new Date('2026-10-13T18:00:00.000Z');
const HOY = '2026-10-13';

const pro = (overrides = {}) => ({
  subscription: { planId: 'pro', status: 'active' },
  ...overrides,
});

describe('tieneAcceso — el Copilot es de quien paga', () => {
  it('Pro activo entra', () => {
    expect(quota.tieneAcceso(pro(), AHORA)).toBe(true);
  });

  it('Lifetime activo entra', () => {
    expect(quota.tieneAcceso({ subscription: { planId: 'lifetime', status: 'active' } }, AHORA))
      .toBe(true);
  });

  it('con el pago pendiente se trata como activo, igual que el resto de la app', () => {
    expect(quota.tieneAcceso({ subscription: { planId: 'pro', status: 'past_due' } }, AHORA))
      .toBe(true);
  });

  it('el plan gratuito NO entra', () => {
    expect(quota.tieneAcceso({ subscription: { planId: 'free', status: 'active' } }, AHORA))
      .toBe(false);
  });

  it('una suscripción cancelada tampoco', () => {
    expect(quota.tieneAcceso({ subscription: { planId: 'pro', status: 'canceled' } }, AHORA))
      .toBe(false);
  });

  it('ni quien no tiene suscripción', () => {
    expect(quota.tieneAcceso({}, AHORA)).toBe(false);
    expect(quota.tieneAcceso(null, AHORA)).toBe(false);
  });

  it('una suscripción vencida deja de dar acceso aunque siga diciendo "active"', () => {
    // El cron que degrada a gratuito puede no haber corrido; la app ya lo trata
    // como gratuito y el backend no puede decir lo contrario.
    const vencido = {
      subscription: { planId: 'pro', status: 'active', currentPeriodEnd: '2026-10-01T00:00:00Z' },
    };

    expect(quota.tieneAcceso(vencido, AHORA)).toBe(false);
  });

  it('Lifetime no vence aunque traiga una fecha pasada', () => {
    const lifetime = {
      subscription: {
        planId: 'lifetime', status: 'active', currentPeriodEnd: '2026-10-01T00:00:00Z',
      },
    };

    expect(quota.tieneAcceso(lifetime, AHORA)).toBe(true);
  });
});

describe('un análisis por semana', () => {
  it('son 7 días', () => {
    expect(quota.DIAS_ENTRE_ANALISIS).toBe(7);
  });

  it('sin análisis previo, el cupo está disponible', () => {
    expect(quota.restantesHoy(pro(), AHORA)).toBe(1);
    expect(quota.nextAvailableAt(pro(), AHORA)).toBeNull();
  });

  it('recién usado, no hay cupo hasta dentro de una semana', () => {
    const recien = pro({ lastCopilotAnalysis: '2026-10-13T16:00:00.000Z' });

    expect(quota.restantesHoy(recien, AHORA)).toBe(0);
    expect(new Date(quota.nextAvailableAt(recien, AHORA)).toISOString())
      .toBe('2026-10-20T16:00:00.000Z');
  });

  it('pasada la semana, vuelve a haber', () => {
    const viejo = pro({ lastCopilotAnalysis: '2026-10-05T16:00:00.000Z' });

    expect(quota.restantesHoy(viejo, AHORA)).toBe(1);
    expect(quota.nextAvailableAt(viejo, AHORA)).toBeNull();
  });

  it('los 7 días corren desde la hora exacta, no desde el lunes', () => {
    const domingo = pro({ lastCopilotAnalysis: '2026-10-11T20:00:00.000Z' });

    expect(new Date(quota.nextAvailableAt(domingo, AHORA)).toISOString())
      .toBe('2026-10-18T20:00:00.000Z');
  });

  it('una fecha ilegible no deja al usuario encerrado', () => {
    expect(quota.restantesHoy(pro({ lastCopilotAnalysis: 'no-es-fecha' }), AHORA)).toBe(1);
  });

  it('a quien no tiene acceso no se le cuenta cupo: se le niega el acceso', () => {
    // Devolver una fecha a un usuario gratuito le haría creer que algún día
    // podrá, y lo que le falta es el plan, no esperar.
    const gratuito = { subscription: { planId: 'free', status: 'active' } };

    expect(quota.nextAvailableAt(gratuito, AHORA)).toBeNull();
    expect(quota.restantesHoy(gratuito, AHORA)).toBe(0);
  });
});

describe('consumeQuota — sólo con un análisis completado (RN-5-C)', () => {
  it('estampa la fecha del último análisis y suma al histórico', async () => {
    const escrituras = [];
    firestoreFalso.collection = () => ({
      doc: () => ({ set: async (datos, opciones) => escrituras.push({ datos, opciones }) }),
    });

    await quota.consumeQuota('u-pro', AHORA);

    expect(escrituras[0].datos.lastCopilotAnalysis).toBe(AHORA.toISOString());
    expect(escrituras[0].datos.copilotAnalysisCount).toEqual({ __increment: 1 });
    expect(escrituras[0].opciones).toEqual({ merge: true });
  });

  it('escribe en userData: borrar un análisis del historial no devuelve cupo', async () => {
    const colecciones = [];
    firestoreFalso.collection = (nombre) => {
      colecciones.push(nombre);
      return { doc: () => ({ set: async () => {} }) };
    };

    await quota.consumeQuota('u-pro', AHORA);

    expect(colecciones).toEqual(['userData']);
  });
});
