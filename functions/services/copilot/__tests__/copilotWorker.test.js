/**
 * HU 5.2 — La guarda del worker.
 *
 * El documento del análisis es uno por usuario (`copilotRuns/{uid}`), así que
 * un análisis nuevo **actualiza** el mismo documento en vez de crear otro, y
 * cada avance de etapa vuelve a disparar la función. Sin esta guarda, cada
 * avance lanzaría otro análisis.
 *
 * @see services/copilot/copilotWorker.js
 */

jest.mock('firebase-functions/v2/firestore', () => ({
  onDocumentWritten: (_opts, handler) => handler,
}));
jest.mock('firebase-functions/params', () => ({
  defineSecret: () => ({ value: () => 'secreto' }),
}));
jest.mock('../copilotAnalysisService');

const { esLanzamiento } = require('../copilotWorker');

const crearRun = (overrides = {}) => ({
  runId: 'run-1',
  userId: 'u-pro',
  status: 'queued',
  stage: 'preparing_data',
  ...overrides,
});

describe('esLanzamiento', () => {
  it('reconoce el primer análisis de un usuario', () => {
    expect(esLanzamiento(null, crearRun())).toBe(true);
  });

  it('reconoce un análisis nuevo que sustituye a uno terminado', () => {
    const antes = crearRun({ runId: 'run-viejo', status: 'completed' });
    expect(esLanzamiento(antes, crearRun({ runId: 'run-nuevo' }))).toBe(true);
  });

  it('NO vuelve a lanzar cuando el worker avanza de etapa', () => {
    const antes = crearRun({ status: 'running', stage: 'preparing_data' });
    const despues = crearRun({ status: 'running', stage: 'fetching_insights' });
    expect(esLanzamiento(antes, despues)).toBe(false);
  });

  it('NO lanza al cerrarse el análisis', () => {
    const antes = crearRun({ status: 'running' });
    const despues = crearRun({ status: 'completed', analysisId: '20261009T100000Z' });
    expect(esLanzamiento(antes, despues)).toBe(false);
  });

  it('NO lanza si el documento se borró', () => {
    expect(esLanzamiento(crearRun(), null)).toBe(false);
  });

  it('NO lanza dos veces el mismo análisis aunque se reescriba en cola', () => {
    expect(esLanzamiento(crearRun(), crearRun())).toBe(false);
  });
});
