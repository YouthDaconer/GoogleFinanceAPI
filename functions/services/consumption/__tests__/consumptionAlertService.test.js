/**
 * HU 6.7 — Las reglas del aviso de consumo: umbrales, día de Colombia, sin
 * avisos repetidos, normalización solo del almacenamiento y cupo de IA.
 *
 * Los días de `qa.md`: D1 = 2026-11-19, D2 = 2026-11-20. Colombia es UTC-5 todo
 * el año (sin horario de verano).
 *
 * @see platform-docs/stories/6.7-aviso-consumo-antes-factura/refinamiento.md (T1, T9)
 */

const {
  UMBRALES,
  BYTES_POR_GIB,
  CONSUMO,
  PREFIJO_AVISO,
  EVENTO_AVISO,
  diaColombia,
  intervaloDelDia,
  horaColombia,
  minutoDelCruce,
  evaluarConteoDiario,
  evaluarAlmacenamiento,
  analisisFueraDeCupo,
  evaluarIa,
  textoUsuarios,
  valorPrueba,
  lineaAviso,
} = require('../consumptionAlertService');

// ============================================================================
// Fábricas locales
// ============================================================================

/** Instante a partir de una hora de Colombia (UTC-5). */
const col = (dia, hora) => new Date(`${dia}T${hora}:00-05:00`);

/** Un punto por minuto, con el minuto en hora de Colombia. */
const minuto = (dia, hora, valor) => {
  const inicio = col(dia, hora);
  return { inicio: inicio.toISOString(), fin: new Date(inicio.getTime() + 60000).toISOString(), valor };
};

const GIB = (g) => Math.round(g * BYTES_POR_GIB);

/** Un análisis de IA lanzado a una hora de Colombia. */
const analisis = (id, dia, hora) => ({ id, createdAt: col(dia, hora).toISOString() });

const usuarioIa = (overrides = {}) => ({ uid: 'uid-inversor-pro-01', conAcceso: true, analisis: [], ...overrides });

// ============================================================================
// Día de Colombia (RN-6.7-L, Vacío 2)
// ============================================================================

describe('consumptionAlertService — el día del aviso es el de Colombia', () => {
  test('QA-04: las 20:30 de Colombia del D1 ya son el día siguiente en UTC y siguen siendo D1', () => {
    const instante = new Date('2026-11-20T01:30:00Z');
    expect(diaColombia(instante)).toBe('2026-11-19');
    expect(horaColombia(instante)).toBe('2026-11-19 20:30 (hora de Colombia)');
  });

  test('el día va de las 00:00 a las 00:00 del siguiente en Colombia, es decir de 05:00 a 05:00 UTC', () => {
    expect(intervaloDelDia('2026-11-19')).toEqual({
      inicio: '2026-11-19T05:00:00.000Z',
      fin: '2026-11-20T05:00:00.000Z',
    });
  });
});

// ============================================================================
// Lecturas y escrituras (RN-6.7-J, RN-6.7-L)
// ============================================================================

describe('consumptionAlertService — lecturas y escrituras: total del día contra su umbral', () => {
  test('QA-01: 200.001 lecturas a las 14:20 avisan con el valor, el umbral y la hora del cruce', () => {
    const puntos = [minuto('2026-11-19', '09:00', 150000), minuto('2026-11-19', '14:20', 50001)];

    const aviso = evaluarConteoDiario({ consumo: 'lecturas', puntos, yaAvisado: false });

    expect(aviso).toEqual({ consumo: 'lecturas', valor: 200001, umbral: 200000, desde: col('2026-11-19', '14:20').toISOString() });
    const linea = lineaAviso(aviso);
    expect(linea.valor).toBe('200.001 lecturas hoy');
    expect(linea.umbral).toBe('200.000 lecturas al día');
    expect(linea.desde).toBe('2026-11-19 14:20 (hora de Colombia)');
  });

  test('QA-02: 100.001 escrituras a las 09:10 avisan con su umbral de 100.000', () => {
    const aviso = evaluarConteoDiario({ consumo: 'escrituras', puntos: [minuto('2026-11-19', '09:10', 100001)], yaAvisado: false });
    expect(lineaAviso(aviso)).toMatchObject({ valor: '100.001 escrituras hoy', umbral: '100.000 escrituras al día', desde: '2026-11-19 09:10 (hora de Colombia)' });
  });

  test('QA-03: 200.000 lecturas y 100.000 escrituras, iguales al umbral, no avisan', () => {
    expect(evaluarConteoDiario({ consumo: 'lecturas', puntos: [minuto('2026-11-21', '23:59', 200000)], yaAvisado: false })).toBeNull();
    expect(evaluarConteoDiario({ consumo: 'escrituras', puntos: [minuto('2026-11-21', '23:59', 100000)], yaAvisado: false })).toBeNull();
  });

  test('QA-04: ya avisado hoy, 450.000 lecturas a las 23:00 no repiten el aviso', () => {
    const puntos = [minuto('2026-11-19', '14:20', 200001), minuto('2026-11-19', '23:00', 249999)];
    expect(evaluarConteoDiario({ consumo: 'lecturas', puntos, yaAvisado: true })).toBeNull();
  });

  test('QA-05: el nuevo día cuenta desde cero y avisa con la hora del cruce del D2', () => {
    const puntos = [minuto('2026-11-20', '10:00', 9800), minuto('2026-11-20', '11:00', 190201)];
    const aviso = evaluarConteoDiario({ consumo: 'lecturas', puntos, yaAvisado: false });
    expect(lineaAviso(aviso).desde).toBe('2026-11-20 11:00 (hora de Colombia)');
  });

  test('la hora del cruce es el minuto en que el acumulado supera el umbral, no el último', () => {
    const puntos = [minuto('2026-11-19', '10:00', 199999), minuto('2026-11-19', '10:01', 1), minuto('2026-11-19', '10:02', 1)];
    expect(minutoDelCruce(puntos, 200000)).toBe(col('2026-11-19', '10:02').toISOString());
  });
});

// ============================================================================
// Almacenamiento (RN-6.7-L, Vacíos 3 y 4)
// ============================================================================

describe('consumptionAlertService — almacenamiento: cruce y vuelta a la normalidad', () => {
  const serieAlm = (pares) => pares.map(([dia, hora, gib]) => ({ fin: col(dia, hora).toISOString(), valor: GIB(gib) }));

  test('QA-02: 0,81 GiB desde las 11:30 avisa una vez con la hora del inicio de la racha', () => {
    const puntos = serieAlm([['2026-11-19', '11:29', 0.79], ['2026-11-19', '11:30', 0.81], ['2026-11-19', '11:45', 0.81]]);

    const { aviso, estadoNuevo } = evaluarAlmacenamiento({ puntos, estado: undefined, hoy: '2026-11-19' });

    expect(aviso.consumo).toBe(CONSUMO.ALMACENAMIENTO);
    expect(lineaAviso(aviso)).toMatchObject({ valor: '0,810 GiB', umbral: '0,8 GiB', desde: '2026-11-19 11:30 (hora de Colombia)' });
    expect(estadoNuevo).toMatchObject({ porEncima: true, ultimoCruceDia: '2026-11-19' });
  });

  test('QA-04: sigue por encima hasta las 15:59 y no repite el aviso', () => {
    const puntos = serieAlm([['2026-11-19', '11:30', 0.81], ['2026-11-19', '15:59', 0.81]]);
    const estado = { porEncima: true, ultimoCruceDia: '2026-11-19' };
    expect(evaluarAlmacenamiento({ puntos, estado, hoy: '2026-11-19' })).toEqual({ aviso: null, estadoNuevo: null });
  });

  test('QA-06: 0,79 GiB a las 16:00 envía el aviso de vuelta a la normalidad el mismo día', () => {
    const puntos = serieAlm([['2026-11-19', '15:59', 0.81], ['2026-11-19', '16:00', 0.79]]);

    const { aviso, estadoNuevo } = evaluarAlmacenamiento({ puntos, estado: { porEncima: true, ultimoCruceDia: '2026-11-19' }, hoy: '2026-11-19' });

    expect(aviso.consumo).toBe(CONSUMO.ALMACENAMIENTO_NORMAL);
    expect(lineaAviso(aviso)).toMatchObject({ valor: '0,790 GiB', desde: '2026-11-19 16:00 (hora de Colombia)' });
    expect(estadoNuevo).toMatchObject({ porEncima: false, ultimaNormalDia: '2026-11-19', ultimoCruceDia: '2026-11-19' });
  });

  test('Vacío 3: arriba varios días seguidos no avisa cada día, solo al cruzar', () => {
    const puntos = serieAlm([['2026-11-20', '08:00', 0.82]]);
    expect(evaluarAlmacenamiento({ puntos, estado: { porEncima: true, ultimoCruceDia: '2026-11-19' }, hoy: '2026-11-20' }).aviso).toBeNull();
  });

  test('Vacío 4: si vuelve a subir el mismo día, la segunda subida no avisa pero el estado sí cambia', () => {
    const puntos = serieAlm([['2026-11-19', '17:00', 0.79], ['2026-11-19', '18:00', 0.83]]);
    const estado = { porEncima: false, ultimoCruceDia: '2026-11-19', ultimaNormalDia: '2026-11-19' };

    const { aviso, estadoNuevo } = evaluarAlmacenamiento({ puntos, estado, hoy: '2026-11-19' });

    expect(aviso).toBeNull();
    expect(estadoNuevo.porEncima).toBe(true);
  });

  test('QA-03 / Vacío 4: 0,80 GiB exactos no están por encima del umbral', () => {
    const puntos = [{ fin: col('2026-11-21', '12:00').toISOString(), valor: UMBRALES.almacenamientoGib * BYTES_POR_GIB }];
    expect(evaluarAlmacenamiento({ puntos, estado: undefined, hoy: '2026-11-21' })).toEqual({ aviso: null, estadoNuevo: null });
  });

  test('sin puntos (Monitoring sin datos) no cambia nada', () => {
    expect(evaluarAlmacenamiento({ puntos: [], estado: { porEncima: true }, hoy: '2026-11-21' })).toEqual({ aviso: null, estadoNuevo: null });
  });
});

// ============================================================================
// IA: cupo, ventana móvil y un aviso con todos (RN-6.7-J, RN-6.7-O, Vacíos 1, 3 y 7)
// ============================================================================

describe('consumptionAlertService — análisis de IA fuera de cupo', () => {
  test('QA-02: Pro con dos análisis en 5 días, el segundo está fuera de cupo', () => {
    const fuera = analisisFueraDeCupo([analisis('a1', '2026-11-14', '18:30'), analisis('a2', '2026-11-19', '09:00')], true);
    expect(fuera.map((a) => a.id)).toEqual(['a2']);
  });

  test('QA-03 / Vacío 1: 8 días entre ambos está dentro del cupo, y exactamente 7 días también', () => {
    expect(analisisFueraDeCupo([analisis('a1', '2026-11-13', '09:00'), analisis('a2', '2026-11-21', '09:30')], true)).toEqual([]);
    expect(analisisFueraDeCupo([analisis('a1', '2026-11-14', '09:00'), analisis('a2', '2026-11-21', '09:00')], true)).toEqual([]);
  });

  test('QA-13: sin acceso al Copilot, el primer análisis ya está fuera de cupo', () => {
    expect(analisisFueraDeCupo([analisis('f1', '2026-11-23', '08:30')], false).map((a) => a.id)).toEqual(['f1']);
    expect(analisisFueraDeCupo([analisis('p1', '2026-11-23', '08:30')], true)).toEqual([]);
  });

  test('QA-14: un solo aviso con los tres que superaron su cupo, su identificador y sus análisis de 7 días, sin el control', () => {
    const ahora = col('2026-12-12', '10:15');
    const usuarios = [
      usuarioIa({ uid: 'uid-inversor-pro-01', analisis: [analisis('p1', '2026-12-07', '18:00'), analisis('p2', '2026-12-12', '10:00')] }),
      usuarioIa({ uid: 'uid-inversor-life-01', analisis: [analisis('l1', '2026-12-12', '09:30'), analisis('l2', '2026-12-12', '10:00')] }),
      usuarioIa({ uid: 'uid-inversor-free-01', conAcceso: false, analisis: [analisis('f1', '2026-12-12', '10:00')] }),
      usuarioIa({ uid: 'uid-inversor-pro-02', analisis: [analisis('q1', '2026-12-12', '10:00')] }),
    ];

    const { aviso, iaNuevo } = evaluarIa({ usuarios, ahora });

    expect(aviso.valor).toBe(3);
    expect(aviso.usuarios.map((u) => u.uid).sort()).toEqual(['uid-inversor-free-01', 'uid-inversor-life-01', 'uid-inversor-pro-01']);
    const linea = lineaAviso(aviso);
    expect(linea.usuarios).toContain('uid-inversor-pro-01: 2 análisis en 7 días');
    expect(linea.usuarios).toContain('uid-inversor-life-01: 2 análisis en 7 días');
    expect(linea.usuarios).toContain('uid-inversor-free-01: 1 análisis en 7 días (sin acceso al Copilot)');
    expect(linea.usuarios).not.toContain('uid-inversor-pro-02');
    expect(JSON.stringify(linea)).not.toMatch(/@/);
    expect(iaNuevo).toEqual({
      usuarios: expect.arrayContaining(['uid-inversor-pro-01', 'uid-inversor-life-01', 'uid-inversor-free-01']),
      analisis: expect.arrayContaining(['uid-inversor-pro-01/p2', 'uid-inversor-life-01/l2', 'uid-inversor-free-01/f1']),
      avisos: 1,
    });
  });

  test('QA-13: el aviso de un usuario sin acceso trae la hora del análisis y el umbral que superó', () => {
    const { aviso } = evaluarIa({
      usuarios: [usuarioIa({ uid: 'uid-inversor-free-01', conAcceso: false, analisis: [analisis('f1', '2026-11-23', '08:30')] })],
      ahora: col('2026-11-23', '08:45'),
    });
    const linea = lineaAviso(aviso);
    expect(linea.desde).toBe('2026-11-23 08:30 (hora de Colombia)');
    expect(linea.umbral).toContain('cualquiera sin acceso al Copilot');
  });

  test('Vacío 3 / QA-08: un análisis ya avisado no vuelve a avisar, ni hoy ni al día siguiente', () => {
    const usuarios = [usuarioIa({ uid: 'uid-inversor-life-01', analisis: [analisis('l1', '2026-11-20', '10:00'), analisis('l2', '2026-11-20', '10:20')] })];

    const mismoDia = evaluarIa({ usuarios, ahora: col('2026-11-20', '15:00'), iaHoy: { usuarios: ['uid-inversor-life-01'], analisis: ['uid-inversor-life-01/l2'], avisos: 1 } });
    const diaSiguiente = evaluarIa({ usuarios, ahora: col('2026-11-21', '08:00'), iaHoy: {}, iaAyer: { analisis: ['uid-inversor-life-01/l2'] } });
    const semanaDespues = evaluarIa({ usuarios, ahora: col('2026-11-25', '08:00') });

    expect(mismoDia).toEqual({ aviso: null, iaNuevo: null });
    expect(diaSiguiente).toEqual({ aviso: null, iaNuevo: null });
    expect(semanaDespues).toEqual({ aviso: null, iaNuevo: null });
  });

  test('Vacío 7: un segundo aviso de IA el mismo día lista solo a los nuevos; el ya avisado no reaparece', () => {
    const ahora = col('2026-12-12', '15:15');
    const iaHoy = { usuarios: ['uid-inversor-pro-01'], analisis: ['uid-inversor-pro-01/p2'], avisos: 1 };
    const usuarios = [
      usuarioIa({ uid: 'uid-inversor-pro-01', analisis: [analisis('p1', '2026-12-07', '18:00'), analisis('p2', '2026-12-12', '10:00'), analisis('p3', '2026-12-12', '15:00')] }),
      usuarioIa({ uid: 'uid-inversor-free-01', conAcceso: false, analisis: [analisis('f1', '2026-12-12', '15:00')] }),
    ];

    const { aviso, iaNuevo } = evaluarIa({ usuarios, ahora, iaHoy });

    expect(aviso.usuarios.map((u) => u.uid)).toEqual(['uid-inversor-free-01']);
    expect(iaNuevo.usuarios).toEqual(['uid-inversor-pro-01', 'uid-inversor-free-01']);
    expect(iaNuevo.analisis).toEqual(expect.arrayContaining(['uid-inversor-pro-01/p3', 'uid-inversor-free-01/f1']));
    expect(iaNuevo.avisos).toBe(2);
  });

  test('un análisis lanzado a las 23:50 y guardado tras la medianoche se avisa al día siguiente', () => {
    const usuarios = [usuarioIa({ uid: 'uid-inversor-free-01', conAcceso: false, analisis: [analisis('f1', '2026-11-19', '23:50')] })];
    const { aviso } = evaluarIa({ usuarios, ahora: col('2026-11-20', '00:15'), iaHoy: {}, iaAyer: {} });
    expect(aviso.usuarios[0].uid).toBe('uid-inversor-free-01');
  });

  test('D5: el correo nombra hasta 20 usuarios y dice cuántos más hay', () => {
    const muchos = Array.from({ length: 25 }, (_, i) => ({ uid: `uid-${String(i).padStart(2, '0')}`, conAcceso: true, analisisSieteDias: 2 }));
    const texto = textoUsuarios(muchos);
    expect(texto).toContain('uid-19: 2 análisis en 7 días');
    expect(texto).not.toContain('uid-20:');
    expect(texto).toMatch(/y 5 más \(lista completa en Firestore\)$/);
  });
});

// ============================================================================
// La línea del aviso y el aviso de prueba
// ============================================================================

describe('consumptionAlertService — la línea del aviso', () => {
  test('una sola línea JSON con el prefijo, el evento y el tipo que escuchan las políticas', () => {
    const linea = lineaAviso({ consumo: 'escrituras', valor: 180000, umbral: 100000, desde: col('2026-11-19', '09:10').toISOString() });
    expect(linea).toMatchObject({ severity: 'WARNING', event: EVENTO_AVISO, consumo: 'escrituras', dia: '2026-11-19' });
    expect(linea.message.startsWith(`${PREFIJO_AVISO} `)).toBe(true);
    expect(JSON.stringify(linea)).not.toContain('\n');
  });

  test('Vacío 5: el aviso de prueba lleva la marca PRUEBA y los cuatro consumos medidos', () => {
    const valor = valorPrueba({ lecturas: 5116, escrituras: 476, almacenamientoBytes: GIB(0.355), usuariosIa: 0 });
    const linea = lineaAviso({ consumo: CONSUMO.PRUEBA, valor, desde: col('2026-11-24', '10:00').toISOString() });

    expect(linea.titulo).toContain('PRUEBA');
    expect(linea.message).toContain('PRUEBA');
    expect(linea.valor).toBe('lecturas hoy 5.116 · escrituras hoy 476 · almacenamiento 0,355 GiB · usuarios con IA fuera de cupo 0');
    expect(linea.umbral).toContain('lecturas > 200.000 al día');
  });

  test('si un consumo no se pudo medir, la prueba lo dice y sigue', () => {
    expect(valorPrueba({ lecturas: null, escrituras: 1, almacenamientoBytes: null, usuariosIa: 0 })).toContain('lecturas hoy no se pudo medir');
  });

  test('un tipo desconocido no construye nada', () => {
    expect(() => lineaAviso({ consumo: 'otro', valor: 1 })).toThrow('Aviso desconocido: otro');
    expect(() => evaluarConteoDiario({ consumo: 'almacenamiento', puntos: [], yaAvisado: false })).toThrow('Consumo sin umbral diario');
  });
});
