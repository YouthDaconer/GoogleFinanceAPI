/**
 * HU 7 — El guardia que decide de quién son las cifras que recibe una petición.
 *
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/historia.md (RN-7-A, B, C, D, H, I)
 * @see platform-docs/stories/7-solo-consulta-sus-propias-cifras/qa.md
 */

const {
  crearGuardiaInversor,
  RESPUESTA_RECHAZO,
  EVENTO_RECHAZO,
  EVENTO_ACCESO_SERVICIO,
  MOTIVOS,
  secretosIguales,
  origenDe,
  leerUserIdSolicitado,
} = require('../investorSession');

// === Fábricas locales ===

const TOKEN_SERVICIO = 'a1b2c3d4e5f6';
const TOKENS = {
  'token-inv-a': { uid: 'uid-inversor-A' },
  'token-inv-b': { uid: 'uid-inversor-B' },
  'token-admin': { uid: 'uid-admin-01', admin: true, role: 'admin' },
  'token-soporte': { uid: 'uid-soporte-01', role: 'support' },
  'token-operacion': { uid: 'uid-operacion-01', role: 'operations' },
};
const AHORA = new Date('2026-11-19T13:15:00.000Z'); // 08:15 en Colombia (QA-07)

const peticion = (overrides = {}) => ({
  method: 'GET',
  path: '/closed-positions',
  query: {},
  headers: { 'x-forwarded-for': '203.0.113.45' },
  socket: { remoteAddress: '10.0.0.1' },
  ...overrides,
});

const conSesion = (token, query = {}, extra = {}) =>
  peticion({ query, headers: { 'x-forwarded-for': '203.0.113.45', authorization: `Bearer ${token}` }, ...extra });

const comoServicio = (token, query = {}) =>
  peticion({ query, headers: { 'x-forwarded-for': '198.51.100.11', 'x-service-token': token } });

const respuesta = () => {
  const res = { statusCode: null, cuerpo: null };
  res.status = jest.fn((codigo) => { res.statusCode = codigo; return res; });
  res.json = jest.fn((cuerpo) => { res.cuerpo = cuerpo; return res; });
  return res;
};

function montarGuardia(overrides = {}) {
  const registro = [];
  const verificarIdToken = jest.fn(async (token) => {
    if (!TOKENS[token]) throw new Error('auth/argument-error');
    return TOKENS[token];
  });
  const guardia = crearGuardiaInversor({
    verificarIdToken,
    obtenerTokenServicio: () => TOKEN_SERVICIO,
    registrar: (linea) => registro.push(linea),
    reloj: () => AHORA,
    ...overrides,
  });
  return { guardia, registro, verificarIdToken };
}

async function ejecutar(guardia, req) {
  const res = respuesta();
  const next = jest.fn();
  await guardia(req, res, next);
  return { res, next, req };
}

// === Persona: solo sus propias cifras ===

describe('HU 7 — persona con sesión: solo sus cifras (RN-7-A)', () => {
  it('con su propio userId pasa y fija el inversor de la sesión (QA-01)', async () => {
    const { guardia, registro } = montarGuardia();
    const { next, req } = await ejecutar(guardia, conSesion('token-inv-a', { userId: 'uid-inversor-A' }));

    expect(next).toHaveBeenCalledTimes(1);
    expect(req.investorUid).toBe('uid-inversor-A');
    expect(req.investorAccess).toEqual({ tipo: 'persona', actorUid: 'uid-inversor-A' });
    expect(registro).toEqual([]);
  });

  it('sin userId responde las cifras del dueño de la sesión (QA-19)', async () => {
    const { guardia } = montarGuardia();
    const { next, req } = await ejecutar(guardia, conSesion('token-inv-b'));

    expect(next).toHaveBeenCalledTimes(1);
    expect(req.investorUid).toBe('uid-inversor-B');
  });

  it('con el userId de otro inversor rechaza y NO lo sustituye por el propio (QA-06)', async () => {
    const { guardia, registro } = montarGuardia();
    const { res, next, req } = await ejecutar(guardia, conSesion('token-inv-a', { userId: 'uid-inversor-B' }));

    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(401);
    expect(res.cuerpo).toEqual({ success: false, error: 'UNAUTHORIZED', message: 'No autorizado' });
    expect(req.investorUid).toBeUndefined();
    expect(registro).toHaveLength(1);
    expect(registro[0].reason).toBe(MOTIVOS.CIFRAS_AJENAS);
  });

  it('con un userId inexistente rechaza igual (QA-06)', async () => {
    const { guardia } = montarGuardia();
    const { res, next } = await ejecutar(guardia, conSesion('token-inv-a', { userId: 'uid-inexistente-99' }));

    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(401);
  });

  it('un userId repetido en la URL no es un inversor: se rechaza', async () => {
    const { guardia } = montarGuardia();
    const { res, next } = await ejecutar(guardia, conSesion('token-inv-a', { userId: ['uid-inversor-A', 'uid-inversor-B'] }));

    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(401);
  });
});

describe('HU 7 — ningún rol abre cifras ajenas (RN-7-B, QA-08)', () => {
  it.each([
    ['administrador', 'token-admin', 'uid-admin-01'],
    ['soporte', 'token-soporte', 'uid-soporte-01'],
    ['operación', 'token-operacion', 'uid-operacion-01'],
  ])('%s con sesión no lee las cifras de uid-inversor-B', async (_rol, token, uid) => {
    const { guardia, registro } = montarGuardia();
    const { res, next } = await ejecutar(guardia, conSesion(token, { userId: 'uid-inversor-B' }));

    expect(next).not.toHaveBeenCalled();
    expect(res.cuerpo).toEqual(RESPUESTA_RECHAZO);
    expect(registro[0]).toMatchObject({ actorUid: uid, targetUserId: 'uid-inversor-B', reason: MOTIVOS.CIFRAS_AJENAS });
  });
});

// === Sin sesión ===

describe('HU 7 — sin sesión no hay datos (Escenario 2, RN-7-C)', () => {
  it('sin credenciales rechaza sin verificar nada ni dejar pasar (QA-04, QA-05)', async () => {
    const { guardia, verificarIdToken, registro } = montarGuardia();
    const { res, next } = await ejecutar(guardia, peticion({ query: { userId: 'uid-inversor-A' } }));

    expect(next).not.toHaveBeenCalled();
    expect(verificarIdToken).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(401);
    expect(registro[0]).toMatchObject({
      event: EVENTO_RECHAZO,
      actorUid: null,
      targetUserId: 'uid-inversor-A',
      origin: '203.0.113.45',
      reason: MOTIVOS.SIN_SESION,
    });
  });

  it('un token alterado o vencido es una sesión inválida (QA-04)', async () => {
    const { guardia, registro } = montarGuardia();
    const { res, next } = await ejecutar(guardia, conSesion('token-alterado', { userId: 'uid-inversor-A' }));

    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(401);
    expect(registro[0].reason).toBe(MOTIVOS.SESION_INVALIDA);
  });

  it('un Authorization que no es Bearer cuenta como sin sesión', async () => {
    const { guardia, verificarIdToken } = montarGuardia();
    const req = peticion({ headers: { authorization: 'Basic dXNlcjpwYXNz' } });
    const { res } = await ejecutar(guardia, req);

    expect(verificarIdToken).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(401);
  });
});

describe('HU 7 — todo rechazo responde igual (RN-7-H, QA-20)', () => {
  it('sin sesión, sesión inválida, cifras ajenas, otro rol y token de servicio falso: mismo código y mismo cuerpo', async () => {
    const casos = [
      peticion({ query: { userId: 'uid-inversor-B' } }),
      peticion({ query: { userId: 'uid-inexistente-99' } }),
      conSesion('token-vencido', { userId: 'uid-inversor-A' }),
      conSesion('token-inv-a', { userId: 'uid-inversor-B' }),
      conSesion('token-inv-a', { userId: 'uid-inexistente-99' }),
      conSesion('token-admin', { userId: 'uid-inversor-B' }),
      comoServicio('token-falso', { userId: 'uid-inversor-B' }),
    ];
    const { guardia } = montarGuardia();

    const respuestas = [];
    for (const req of casos) {
      const { res } = await ejecutar(guardia, req);
      respuestas.push({ status: res.statusCode, cuerpo: JSON.stringify(res.cuerpo) });
    }

    expect(new Set(respuestas.map((r) => `${r.status}|${r.cuerpo}`)).size).toBe(1);
    expect(respuestas[0]).toEqual({ status: 401, cuerpo: '{"success":false,"error":"UNAUTHORIZED","message":"No autorizado"}' });
  });
});

describe('HU 7 — registro de cada intento rechazado (RN-7-D, QA-07)', () => {
  it('deja una línea con quién, contra quién, desde dónde y cuándo', async () => {
    const { guardia, registro } = montarGuardia();
    await ejecutar(guardia, conSesion('token-inv-a', { userId: 'uid-inversor-B' }));

    expect(registro).toEqual([
      {
        severity: 'WARNING',
        message: '[investorSession] Acceso denegado a cifras de un inversor',
        event: 'investor_data_access_denied',
        actorUid: 'uid-inversor-A',
        targetUserId: 'uid-inversor-B',
        origin: '203.0.113.45',
        forwardedFor: '203.0.113.45',
        route: '/closed-positions',
        method: 'GET',
        reason: 'cifras_ajenas',
        at: '2026-11-19T13:15:00.000Z',
      },
    ]);
  });

  it('el userId que escribe el atacante se recorta a 128 caracteres en el registro', async () => {
    const { guardia, registro } = montarGuardia();
    await ejecutar(guardia, peticion({ query: { userId: 'x'.repeat(5000) } }));

    expect(registro[0].targetUserId).toHaveLength(128);
  });

  it('una ruta que no existe se rechaza igual pero no se registra (barridos de bots)', async () => {
    const { guardia, registro } = montarGuardia({ esRutaRegistrada: () => false });
    const { res } = await ejecutar(guardia, peticion({ path: '/wp-login.php' }));

    expect(res.statusCode).toBe(401);
    expect(registro).toEqual([]);
  });

  it('la petición aceptada del dueño no deja ningún rechazo (QA-03)', async () => {
    const { guardia, registro } = montarGuardia();
    await ejecutar(guardia, conSesion('token-inv-a', { userId: 'uid-inversor-A' }));
    await ejecutar(guardia, conSesion('token-inv-a', { userId: 'uid-inversor-A' }));

    expect(registro).toEqual([]);
  });
});

// === Sistema ===

describe('HU 7 — el sistema consulta a cualquier inversor (RN-7-I)', () => {
  it.each(['uid-inversor-A', 'uid-inversor-B', 'uid-inversor-grande'])(
    'con el token de servicio lee las cifras de %s',
    async (uid) => {
      const { guardia, verificarIdToken } = montarGuardia();
      const { next, req } = await ejecutar(guardia, comoServicio(TOKEN_SERVICIO, { userId: uid }));

      expect(next).toHaveBeenCalledTimes(1);
      expect(req.investorUid).toBe(uid);
      expect(req.investorAccess).toEqual({ tipo: 'servicio', actorUid: null });
      expect(verificarIdToken).not.toHaveBeenCalled();
    },
  );

  it('cada consulta del sistema queda en la auditoría, sin contarse como rechazo', async () => {
    const { guardia, registro } = montarGuardia();
    await ejecutar(guardia, comoServicio(TOKEN_SERVICIO, { userId: 'uid-inversor-B' }));

    expect(registro).toHaveLength(1);
    expect(registro[0]).toMatchObject({ event: EVENTO_ACCESO_SERVICIO, severity: 'INFO', targetUserId: 'uid-inversor-B' });
  });

  it('el sistema debe decir de quién pide: sin userId responde 400', async () => {
    const { guardia } = montarGuardia();
    const { res, next } = await ejecutar(guardia, comoServicio(TOKEN_SERVICIO));

    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(400);
    expect(res.cuerpo.error).toBe('MISSING_PARAM');
  });

  it('un token de servicio falso se rechaza y se registra, aunque traiga además una sesión válida', async () => {
    const { guardia, registro } = montarGuardia();
    const req = comoServicio('token-falso', { userId: 'uid-inversor-B' });
    req.headers.authorization = 'Bearer token-inv-b';
    const { res, next } = await ejecutar(guardia, req);

    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(401);
    expect(registro[0].reason).toBe(MOTIVOS.SERVICIO_INVALIDO);
  });

  it('si el secreto de servicio no está configurado, ningún token de servicio vale', async () => {
    const { guardia } = montarGuardia({ obtenerTokenServicio: () => '' });
    const { res } = await ejecutar(guardia, comoServicio('', { userId: 'uid-inversor-B' }));

    expect(res.statusCode).toBe(401);
  });
});

// === Piezas puras ===

describe('HU 7 — piezas del guardia', () => {
  it('secretosIguales compara el contenido, también con largos distintos', () => {
    expect(secretosIguales('abc123', 'abc123')).toBe(true);
    expect(secretosIguales('abc12', 'abc123')).toBe(false);
    expect(secretosIguales('', '')).toBe(false);
    expect(secretosIguales(undefined, 'abc')).toBe(false);
  });

  it('el origen es la última IP de X-Forwarded-For (la que añade Google), no la que escribe el cliente', () => {
    expect(origenDe(peticion({ headers: { 'x-forwarded-for': '1.2.3.4, 203.0.113.45' } }))).toBe('203.0.113.45');
    expect(origenDe(peticion({ headers: {} }))).toBe('10.0.0.1');
  });

  it('leerUserIdSolicitado trata el vacío como ausente', () => {
    expect(leerUserIdSolicitado({ userId: '' })).toEqual({ presente: false, valor: null, valido: true });
    expect(leerUserIdSolicitado({})).toEqual({ presente: false, valor: null, valido: true });
    expect(leerUserIdSolicitado({ userId: 'uid-inversor-A' })).toEqual({ presente: true, valor: 'uid-inversor-A', valido: true });
  });
});
