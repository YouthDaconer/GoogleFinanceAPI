/**
 * HU 7 — La comprobación repetible de consultas con datos de inversores sin
 * sesión (Escenario 9, RN-7-F).
 *
 * Se arma con apps de Express reales (sin red) para que la pila del router sea
 * la misma que recorre el CLI sobre `httpApi.js`.
 *
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/refinamiento.md (T5)
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/qa.md (QA-15 a QA-18)
 */

const express = require('express');

const {
  NOMBRE_GUARDIA,
  USER_ID_SONDA,
  listarRutas,
  auditarRutas,
  planDeSondas,
  evaluarSonda,
  formatearInforme,
} = require('../investorRouteAudit');

// === Fábricas locales ===

const nada = (req, res) => res.end();
const RUTAS_INVERSOR = ['/attribution', '/attribution/top', '/attribution/check', '/risk-metrics', '/closed-positions'];
const RUTAS_MERCADO = ['/quotes', '/search', '/news', '/indices'];

/** Una entrada de decisión con overrides. */
const abierta = (overrides = {}) => ({
  method: 'GET',
  path: '/quotes',
  motivo: 'Datos de mercado',
  decididoPor: 'Carlos Caicedo (PO)',
  fecha: '2026-11-12',
  ...overrides,
});
const LISTA_MERCADO = RUTAS_MERCADO.map((path) => abierta({ path }));

/**
 * Una app con rutas de mercado y de inversor.
 *
 * @param {{ conGuardia?: boolean, antesDelGuardia?: string[] }} opciones
 */
function crearApp({ conGuardia = true, antesDelGuardia = [] } = {}) {
  const app = express();
  app.use(express.json());
  for (const ruta of antesDelGuardia) app.get(ruta, nada);
  if (conGuardia) {
    app.use(function guardiaConsultasInversor(req, res, next) { next(); });
  }
  for (const ruta of [...RUTAS_MERCADO, ...RUTAS_INVERSOR]) app.get(ruta, nada);
  return app;
}

const auditar = (app, rutasAbiertas = LISTA_MERCADO) =>
  auditarRutas({ ...listarRutas(app._router.stack), rutasAbiertas });

const rutasDe = (lista) => lista.map((r) => r.path);

// === Tests ===

describe('HU 7 — listarRutas lee la pila real del router', () => {
  it('encuentra cada ruta con su método y la posición del guardia', () => {
    const app = crearApp();
    app.post('/process-etf-excel', nada);
    const { rutas, indiceGuardia } = listarRutas(app._router.stack);

    expect(indiceGuardia).toBeGreaterThan(-1);
    expect(app._router.stack[indiceGuardia].name).toBe(NOMBRE_GUARDIA);
    expect(rutas.filter((r) => r.method === 'POST').map((r) => r.path)).toEqual(['/process-etf-excel']);
    expect(rutas.every((r) => r.indice > indiceGuardia)).toBe(true);
  });
});

describe('HU 7 — antes del cambio, las cinco consultas aparecen abiertas sin decisión (QA-15)', () => {
  it('sin guardia: C1–C5 abiertas sin decisión; ninguna consulta de mercado entre ellas', () => {
    const resultado = auditar(crearApp({ conGuardia: false }));

    expect(rutasDe(resultado.abiertasSinDecision)).toEqual(RUTAS_INVERSOR);
    expect(resultado.abiertasSinDecision[0].causa).toBe('no hay guardia de sesión en la app');
    expect(rutasDe(resultado.abiertasPorDecision)).toEqual(RUTAS_MERCADO);
    expect(resultado.enCero).toBe(false);
  });
});

describe('HU 7 — con el guardia, cero abiertas sin decisión (QA-16)', () => {
  it('las cinco quedan protegidas y el mercado abierto por decisión', () => {
    const resultado = auditar(crearApp());

    expect(resultado.abiertasSinDecision).toEqual([]);
    expect(rutasDe(resultado.protegidas)).toEqual(RUTAS_INVERSOR);
    expect(rutasDe(resultado.abiertasPorDecision)).toEqual(RUTAS_MERCADO);
    expect(resultado.enCero).toBe(true);
  });

  it('relanzarla da exactamente el mismo resultado', () => {
    const app = crearApp();

    expect(auditar(app)).toEqual(auditar(app));
  });
});

describe('HU 7 — una consulta nueva nace protegida, salvo decisión registrada (QA-17, QA-18)', () => {
  it('una ruta nueva registrada después del guardia queda protegida sin hacer nada', () => {
    const app = crearApp();
    app.get('/dividendos-cobrados', nada);
    const resultado = auditar(app);

    expect(rutasDe(resultado.protegidas)).toContain('/dividendos-cobrados');
    expect(resultado.enCero).toBe(true);
  });

  it('una ruta registrada ANTES del guardia se informa como abierta sin decisión', () => {
    const resultado = auditar(crearApp({ antesDelGuardia: ['/dividendos-cobrados'] }));

    expect(resultado.abiertasSinDecision).toEqual([
      expect.objectContaining({ path: '/dividendos-cobrados', causa: 'registrada antes del guardia de sesión' }),
    ]);
    expect(resultado.enCero).toBe(false);
  });

  it('abierta con su decisión (fecha, quién y motivo) se lista aparte y no cuenta como fallo (QA-18)', () => {
    const app = crearApp();
    app.get('/dividendos-cobrados', nada);
    const decision = abierta({ path: '/dividendos-cobrados', motivo: 'Demostración pública acordada', fecha: '2026-11-19' });
    const resultado = auditar(app, [...LISTA_MERCADO, decision]);

    expect(resultado.abiertasPorDecision).toContainEqual(expect.objectContaining({
      path: '/dividendos-cobrados', decididoPor: 'Carlos Caicedo (PO)', fecha: '2026-11-19', motivo: 'Demostración pública acordada',
    }));
    expect(resultado.enCero).toBe(true);
  });

  it('una entrada sin decisión completa no abre la ruta y hace fallar la comprobación', () => {
    const app = crearApp();
    app.get('/dividendos-cobrados', nada);
    const incompleta = { method: 'GET', path: '/dividendos-cobrados', motivo: 'sin firmar' };
    const resultado = auditar(app, [...LISTA_MERCADO, incompleta]);

    expect(rutasDe(resultado.protegidas)).toContain('/dividendos-cobrados');
    expect(resultado.entradasIncompletas).toEqual([incompleta]);
    expect(resultado.enCero).toBe(false);
  });

  it('avisa de entradas de la lista que ya no corresponden a ninguna ruta', () => {
    const resultado = auditar(crearApp(), [...LISTA_MERCADO, abierta({ path: '/ruta-retirada' })]);

    expect(rutasDe(resultado.entradasHuerfanas)).toEqual(['/ruta-retirada']);
    expect(resultado.enCero).toBe(true);
  });
});

describe('HU 7 — sonda en vivo y repetición de la petición del 2026-11-12 (Escenario 8, QA-14)', () => {
  it('planea una petición sin credenciales por ruta protegida, con un userId que no es de nadie', () => {
    const plan = planDeSondas([{ method: 'GET', path: '/risk-metrics' }, { method: 'GET', path: '/x/:id' }]);

    expect(plan).toEqual([
      { method: 'GET', ruta: '/risk-metrics', url: `/risk-metrics?userId=${USER_ID_SONDA}` },
      { method: 'GET', ruta: '/x/:id', url: `/x/${USER_ID_SONDA}?userId=${USER_ID_SONDA}` },
    ]);
  });

  it('solo 401 con «No autorizado» pasa la sonda', () => {
    expect(evaluarSonda(401, { message: 'No autorizado' }).ok).toBe(true);
    expect(evaluarSonda(200, { available: true })).toEqual({ ok: false, detalle: 'respondió 200 en lugar de 401 «No autorizado»' });
    expect(evaluarSonda(401, { message: 'Token expired' }).ok).toBe(false);
    expect(evaluarSonda(404, null).ok).toBe(false);
  });
});

describe('HU 7 — el informe', () => {
  it('dice el conteo de abiertas sin decisión y el resultado final', () => {
    const texto = formatearInforme(auditar(crearApp({ conGuardia: false })));

    expect(texto).toContain('Abiertas sin decisión: 5  ✗');
    expect(texto).toContain('✗ GET   /attribution/check — no hay guardia de sesión en la app');
    expect(texto).toContain('Resultado: HAY consultas abiertas sin decisión ✗');
  });

  it('en verde dice cero', () => {
    expect(formatearInforme(auditar(crearApp()))).toContain('Resultado: 0 consultas abiertas sin decisión ✓');
  });
});
