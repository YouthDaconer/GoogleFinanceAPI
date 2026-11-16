/**
 * HU 6.11.2 — Un resumen que falta o quedó viejo se prepara una sola vez.
 *
 * @see platform-docs/stories/6.11.2-una-cuenta-y-copiloto-sin-recorrer/refinamiento.md (T2, T5b)
 */

const mockGenerate = jest.fn();
jest.mock('../../snapshotGenerator', () => ({
  buildSnapshotDocId: (userId, accountId, currency) => (accountId === 'overall'
    ? `${userId}_${currency}`
    : `${userId}_${accountId}_${currency}`),
  generatePerformanceSnapshot: (...args) => mockGenerate(...args),
}));

const { prepararResumenUnaVez, prepararUnaVez, preparacionesEnCurso } = require('../snapshotPreparationService');

const DB = { nombre: 'db' };

/** Una preparación que termina cuando el test lo decide. */
function preparacionPendiente() {
  let terminar;
  let fallar;
  mockGenerate.mockImplementationOnce(() => new Promise((resolve, reject) => {
    terminar = resolve;
    fallar = reject;
  }));
  return { terminar: (v) => terminar(v), fallar: (e) => fallar(e) };
}

describe('HU 6.11.2: prepararResumenUnaVez (Escenario 3)', () => {
  beforeEach(() => jest.clearAllMocks());

  it('tres peticiones simultáneas del mismo resumen lo preparan una sola vez (QA-11)', async () => {
    const pendiente = preparacionPendiente();

    const peticiones = [
      prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD'),
      prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD'),
      prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD'),
    ];
    pendiente.terminar({ returns: { ytdReturn: 7.5 } });
    const resultados = await Promise.all(peticiones);

    expect(mockGenerate).toHaveBeenCalledTimes(1);
    expect(mockGenerate).toHaveBeenCalledWith(DB, 'inv-m', 'overall', 'USD');
    expect(resultados.map((r) => r.returns.ytdReturn)).toEqual([7.5, 7.5, 7.5]);
    expect(preparacionesEnCurso()).toBe(0);
  });

  it('resúmenes distintos (otra cuenta, otra moneda) no se esperan entre sí', async () => {
    mockGenerate.mockResolvedValue({ ok: true });

    await Promise.all([
      prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD'),
      prepararResumenUnaVez(DB, 'inv-m', 'cta-co', 'USD'),
      prepararResumenUnaVez(DB, 'inv-m', 'overall', 'COP'),
    ]);

    expect(mockGenerate).toHaveBeenCalledTimes(3);
  });

  it('terminada la preparación, una petición posterior prepara de nuevo (no guarda resultados)', async () => {
    mockGenerate.mockResolvedValue({ returns: { ytdReturn: 7.5 } });

    await prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD');
    await prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD');

    expect(mockGenerate).toHaveBeenCalledTimes(2);
  });

  it('si la preparación falla, todos los que esperaban reciben el error y se puede volver a intentar', async () => {
    const pendiente = preparacionPendiente();
    const a = prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD');
    const b = prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD');
    pendiente.fallar(new Error('DEADLINE_EXCEEDED'));

    await expect(a).rejects.toThrow('DEADLINE_EXCEEDED');
    await expect(b).rejects.toThrow('DEADLINE_EXCEEDED');
    expect(preparacionesEnCurso()).toBe(0);

    mockGenerate.mockResolvedValueOnce({ returns: { ytdReturn: 7.5 } });
    const reintento = await prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD');
    expect(reintento.returns.ytdReturn).toBe(7.5);
    expect(mockGenerate).toHaveBeenCalledTimes(2);
  });
});

describe('HU 6.11.6: prepararUnaVez, la preparación única para cualquier resumen (RN-6.11.6-J)', () => {
  it('dos aperturas a la vez del mismo resumen por activo esperan la misma preparación', async () => {
    let terminar;
    const preparar = jest.fn(() => new Promise((resolve) => { terminar = resolve; }));

    const a = prepararUnaVez('inv-m_MSFT_stock_USD', preparar);
    const b = prepararUnaVez('inv-m_MSFT_stock_USD', preparar);
    terminar({ assetDays: [] });

    await expect(Promise.all([a, b])).resolves.toEqual([{ assetDays: [] }, { assetDays: [] }]);
    expect(preparar).toHaveBeenCalledTimes(1);
    expect(preparacionesEnCurso()).toBe(0);
  });

  it('comparte el registro con prepararResumenUnaVez: el mismo id espera la preparación en curso', async () => {
    let terminar;
    mockGenerate.mockImplementationOnce(() => new Promise((resolve) => { terminar = resolve; }));
    const otra = jest.fn();

    const a = prepararResumenUnaVez(DB, 'inv-m', 'overall', 'USD');
    const b = prepararUnaVez('inv-m_USD', otra);
    terminar({ ok: true });

    await expect(Promise.all([a, b])).resolves.toEqual([{ ok: true }, { ok: true }]);
    expect(otra).not.toHaveBeenCalled();
  });

  it('un error llega a todos los que esperaban y se puede reintentar', async () => {
    const preparar = jest.fn().mockRejectedValueOnce(new Error('DEADLINE_EXCEEDED')).mockResolvedValueOnce('listo');

    await expect(prepararUnaVez('inv-m_MSFT_stock_USD#dias', preparar)).rejects.toThrow('DEADLINE_EXCEEDED');
    await expect(prepararUnaVez('inv-m_MSFT_stock_USD#dias', preparar)).resolves.toBe('listo');
    expect(preparacionesEnCurso()).toBe(0);
  });
});
