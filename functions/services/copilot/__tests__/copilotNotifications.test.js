/**
 * HU 5.2 — Los avisos del Copilot en la campana.
 *
 * Replican el contrato del evaluador de alertas: escribir en
 * `userNotifications` **y** estampar `userData.lastNotificationAt`, que es lo
 * que hace que la campana se entere sin recargar (Escenario 3, QA-05).
 *
 * @see services/copilot/copilotNotifications.js
 */

const avisos = [];
const señales = [];

jest.mock('../../firebaseAdmin', () => ({
  firestore: () => ({
    collection: (nombre) => ({
      add: async (datos) => {
        avisos.push({ coleccion: nombre, datos });
        return { id: 'notif-1' };
      },
      doc: (id) => ({
        set: async (datos, opciones) => señales.push({ coleccion: nombre, id, datos, opciones }),
      }),
    }),
  }),
}));

const notifications = require('../copilotNotifications');
const { NOTIFICATION_TYPE } = require('../copilotConstants');

beforeEach(() => {
  avisos.length = 0;
  señales.length = 0;
});

describe('notifyAnalysisReady — "Tu análisis del portafolio está listo"', () => {
  it('escribe el aviso sin leer, con su tipo y el análisis al que lleva', async () => {
    await notifications.notifyAnalysisReady('u-pro', {
      language: 'es',
      analysisId: '20261009T100000Z',
    });

    expect(avisos).toHaveLength(1);
    expect(avisos[0].coleccion).toBe('userNotifications');
    expect(avisos[0].datos.type).toBe(NOTIFICATION_TYPE.ANALYSIS_READY);
    expect(avisos[0].datos.userId).toBe('u-pro');
    expect(avisos[0].datos.isRead).toBe(false);
    expect(avisos[0].datos.data).toEqual({ analysisId: '20261009T100000Z' });
  });

  it('no reutiliza alertId: el store de la campana deduplica por ese campo', async () => {
    await notifications.notifyAnalysisReady('u-pro', {
      language: 'es',
      analysisId: '20261009T100000Z',
    });

    expect(avisos[0].datos.data.alertId).toBeUndefined();
  });

  it('despierta a la campana con lastNotificationAt (Escenario 3)', async () => {
    await notifications.notifyAnalysisReady('u-pro', {
      language: 'es',
      analysisId: '20261009T100000Z',
    });

    expect(señales).toHaveLength(1);
    expect(señales[0].coleccion).toBe('userData');
    expect(señales[0].id).toBe('u-pro');
    expect(señales[0].datos.lastNotificationAt).toBe(avisos[0].datos.createdAt);
    expect(señales[0].opciones).toEqual({ merge: true });
  });

  it('guarda el texto de respaldo en el idioma del lanzamiento', async () => {
    await notifications.notifyAnalysisReady('u-life', {
      language: 'en',
      analysisId: '20261009T100000Z',
    });

    expect(avisos[0].datos.title).toBe('Your portfolio analysis is ready');
  });
});

describe('notifyAnalysisFailed — Escenario 8', () => {
  it('no menciona proveedores, códigos ni "502"', async () => {
    await notifications.notifyAnalysisFailed('u-pro', { language: 'es' });

    const texto = `${avisos[0].datos.title} ${avisos[0].datos.message}`;
    expect(texto).toContain('Tu crédito no fue consumido');
    expect(texto).not.toMatch(/502|Gemini|Lambda|Cloudflare/i);
  });

  it('no lleva análisis: no hay ninguno al que ir', async () => {
    await notifications.notifyAnalysisFailed('u-pro', { language: 'es' });

    expect(avisos[0].datos.type).toBe(NOTIFICATION_TYPE.ANALYSIS_FAILED);
    expect(avisos[0].datos.data).toEqual({});
  });

  it('cae al español cuando el idioma no está soportado', async () => {
    await notifications.notifyAnalysisFailed('u-pro', { language: 'pt' });

    expect(avisos[0].datos.title).toBe('No pudimos completar tu análisis');
  });
});
