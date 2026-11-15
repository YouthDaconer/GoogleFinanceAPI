/**
 * HU 6.10.1 — La limpieza de datos vencidos: ensayar, retirar por tandas y
 * dejar constancia.
 *
 * Cada ejecución recorre el catálogo de `retentionPolicies.js`, y para cada
 * tipo hace una de dos cosas:
 *
 * - **Ensayo en seco** (RN-6.10-O, RN-6-B): si operación todavía no activó el
 *   tipo, o si la ejecución es solo de ensayo. Cuenta por mes lo que
 *   retiraría y no toca nada. El último ensayo queda en
 *   `dataRetentionPolicies/config.ultimosEnsayos`, y sin él no se puede activar el tipo.
 * - **Retirada** (RN-6.10-J): si operación lo activó tras revisar su ensayo.
 *   Avanza por tandas de hasta 500 operaciones (`db.batch()`, estándar §4
 *   regla 10).
 *
 * **A medias sin romper nada** (RN-6-H, Escenario 5):
 * - antes de cada tanda se mira el presupuesto de tiempo y la franja nocturna
 *   (RN-6.10.1-J);
 * - un lote es atómico, así que cada documento queda retirado o intacto;
 * - las subcolecciones se retiran antes que su padre, así que un día cortado
 *   a la mitad sigue vencido y la siguiente ejecución lo retoma, sin estado
 *   propio que pueda corromperse.
 *
 * **Constancia** (RN-6.10-N, RN-6-I): cada ejecución escribe
 * `dataRetentionRuns/{fecha}_{HHmmss}` al empezar y la actualiza después de
 * cada tanda. Si el proceso muere, lo retirado hasta la última tanda ya consta.
 * Guarda también el almacenamiento medido por la plataforma al empezar. La
 * diferencia entre dos ejecuciones es el espacio liberado (`leerInforme`).
 *
 * @module services/retention/retentionService
 * @see platform-docs/stories/6.10.1-plazos-conservacion-datos/refinamiento.md (T2)
 */

const admin = require('../firebaseAdmin');
const {
  TIPOS,
  COLECCION_CONSTANCIAS,
  COLECCION_CONFIG,
  DOC_CONFIG,
  tipoPorId,
  corteDe,
  enHorarioNocturno,
  tramoDe,
  fechaDelDocumento,
  idDeConstancia,
} = require('./retentionPolicies');

/** Operaciones por tanda: el tope de `db.batch()`. */
const TAMANO_TANDA = 500;

/** Presupuesto de una ejecución: deja ~2 min de margen al timeout de 540 s. */
const PRESUPUESTO_MS = 420 * 1000;

/** Motivos por los que una ejecución se detiene antes de terminar. */
const MOTIVO_PARADA = Object.freeze({
  PRESUPUESTO: 'presupuesto',
  HORARIO_NOCTURNO: 'horario-nocturno',
});

const MODO = Object.freeze({ ENSAYO: 'ensayo', RETIRADA: 'retirada' });

/** Ruta del id del documento, para consultar y ordenar por él. */
const { FieldPath } = admin.firestore;

// ============================================================================
// LECTURA DE LO VENCIDO
// ============================================================================

/**
 * Una página de documentos vencidos de un tipo, en orden de antigüedad.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} tipo
 * @param {{fecha: string, iso: string}} corte
 * @param {Object} [opciones]
 * @param {number} [opciones.limite=TAMANO_TANDA]
 * @param {FirebaseFirestore.QueryDocumentSnapshot} [opciones.despuesDe] - Cursor (solo el ensayo pagina)
 * @returns {Promise<Array<{id: string, ref: FirebaseFirestore.DocumentReference, fecha: string}>>}
 */
async function paginaVencidos(db, tipo, corte, { limite = TAMANO_TANDA, despuesDe = null } = {}) {
  const coleccion = db.collection(tipo.ruta);

  if (tipo.listarDocumentos) {
    // Incluye los días sin raíz (trozos escritos y raíz nunca escrita).
    const refs = await coleccion.listDocuments();
    return refs
      .filter((ref) => ref.id < corte.fecha && (!despuesDe || ref.id > despuesDe.id))
      .sort((a, b) => (a.id < b.id ? -1 : 1))
      .slice(0, limite)
      .map((ref) => ({ id: ref.id, ref, fecha: ref.id, cursor: ref }));
  }

  let query = tipo.fecha.porId
    ? coleccion.where(FieldPath.documentId(), '<', corte.fecha).orderBy(FieldPath.documentId()).select()
    : coleccion.where(tipo.fecha.campo, '<', corte.iso).orderBy(tipo.fecha.campo).select(tipo.fecha.campo);
  if (despuesDe) query = query.startAfter(despuesDe);

  const snapshot = await query.limit(limite).get();
  return snapshot.docs.map((doc) => ({
    id: doc.id,
    ref: doc.ref,
    fecha: fechaDelDocumento(tipo, doc.id, doc.data()),
    cursor: doc,
  }));
}

/**
 * Suma `cantidad` al tramo de `fecha`.
 *
 * @param {Object<string, number>} porTramo
 * @param {string} fecha
 * @param {number} cantidad
 */
function sumarTramo(porTramo, fecha, cantidad) {
  if (cantidad <= 0) return;
  const tramo = tramoDe(fecha);
  porTramo[tramo] = (porTramo[tramo] || 0) + cantidad;
}

// ============================================================================
// ENSAYO EN SECO
// ============================================================================

/**
 * Cuenta, por mes, lo que se retiraría de un tipo. No escribe nada.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} tipo
 * @param {{fecha: string, iso: string}} corte
 * @param {Object} [opciones]
 * @param {Function} [opciones.debeParar] - Devuelve un motivo de parada o `null`
 * @returns {Promise<{registros: number, documentos: number, porTramo: Object, completo: boolean, detenidoPor: string|null}>}
 */
async function contarVencidos(db, tipo, corte, { debeParar = () => null } = {}) {
  const resultado = { registros: 0, documentos: 0, porTramo: {}, completo: false, detenidoPor: null };
  let despuesDe = null;

  for (;;) {
    const motivo = debeParar();
    if (motivo) return { ...resultado, detenidoPor: motivo };

    const pagina = await paginaVencidos(db, tipo, corte, { despuesDe });
    if (pagina.length === 0) break;

    for (const doc of pagina) {
      let hijos = 0;
      for (const sub of tipo.subcolecciones) {
        const conteo = await doc.ref.collection(sub).count().get();
        const n = conteo.data().count;
        hijos += n;
        if (tipo.registros === 'subcoleccion' && sub === tipo.subcolecciones[0]) {
          resultado.registros += n;
          sumarTramo(resultado.porTramo, doc.fecha, n);
        }
      }
      if (tipo.registros !== 'subcoleccion') {
        resultado.registros += 1;
        sumarTramo(resultado.porTramo, doc.fecha, 1);
      }
      resultado.documentos += 1 + hijos;
    }

    if (pagina.length < TAMANO_TANDA) break;
    despuesDe = pagina[pagina.length - 1].cursor;
  }

  return { ...resultado, completo: true };
}

// ============================================================================
// RETIRADA POR TANDAS
// ============================================================================

/**
 * Retira, por tandas, lo vencido de un tipo.
 *
 * Siempre vuelve a pedir la **primera** página de vencidos: lo retirado ya no
 * aparece, así que no hace falta cursor, y una ejecución nueva empieza
 * exactamente donde quedó la anterior (QA-11).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} tipo
 * @param {{fecha: string, iso: string}} corte
 * @param {Object} [opciones]
 * @param {Function} [opciones.debeParar] - Devuelve un motivo de parada o `null`
 * @param {Function} [opciones.alTerminarTanda] - Recibe el avance acumulado tras cada tanda
 * @returns {Promise<{registros: number, documentos: number, porTramo: Object, tandas: number, completo: boolean, detenidoPor: string|null}>}
 */
async function retirarVencidos(db, tipo, corte, { debeParar = () => null, alTerminarTanda = async () => {} } = {}) {
  const avance = { registros: 0, documentos: 0, porTramo: {}, tandas: 0 };

  const cerrarTanda = async () => {
    avance.tandas += 1;
    await alTerminarTanda({ ...avance, porTramo: { ...avance.porTramo } });
  };

  for (;;) {
    let motivo = debeParar();
    if (motivo) return { ...avance, completo: false, detenidoPor: motivo };

    const pagina = await paginaVencidos(db, tipo, corte);
    if (pagina.length === 0) break;

    if (tipo.subcolecciones.length === 0) {
      const batch = db.batch();
      pagina.forEach((doc) => batch.delete(doc.ref));
      await batch.commit();
      avance.documentos += pagina.length;
      avance.registros += pagina.length;
      pagina.forEach((doc) => sumarTramo(avance.porTramo, doc.fecha, 1));
      await cerrarTanda();
      continue;
    }

    // Con subcolecciones, padre por padre: primero sus hijos, al final él.
    for (const doc of pagina) {
      for (const sub of tipo.subcolecciones) {
        for (;;) {
          motivo = debeParar();
          if (motivo) return { ...avance, completo: false, detenidoPor: motivo };

          const hijos = await doc.ref.collection(sub).limit(TAMANO_TANDA).get();
          if (hijos.empty) break;

          const batch = db.batch();
          hijos.docs.forEach((hijo) => batch.delete(hijo.ref));
          await batch.commit();
          avance.documentos += hijos.size;
          if (tipo.registros === 'subcoleccion' && sub === tipo.subcolecciones[0]) {
            avance.registros += hijos.size;
            sumarTramo(avance.porTramo, doc.fecha, hijos.size);
          }
          await cerrarTanda();
          if (hijos.size < TAMANO_TANDA) break;
        }
      }

      motivo = debeParar();
      if (motivo) return { ...avance, completo: false, detenidoPor: motivo };

      await doc.ref.delete();
      avance.documentos += 1;
      if (tipo.registros !== 'subcoleccion') {
        avance.registros += 1;
        sumarTramo(avance.porTramo, doc.fecha, 1);
      }
    }
    await cerrarTanda();
  }

  return { ...avance, completo: true, detenidoPor: null };
}

// ============================================================================
// CONFIGURACIÓN: QUÉ ACTIVÓ OPERACIÓN
// ============================================================================

/**
 * @param {FirebaseFirestore.Firestore} db
 * @returns {FirebaseFirestore.DocumentReference}
 */
function refConfig(db) {
  return db.collection(COLECCION_CONFIG).doc(DOC_CONFIG);
}

/**
 * @param {FirebaseFirestore.Firestore} db
 * @returns {Promise<{tipos: Object, ultimosEnsayos: Object}>}
 */
async function leerConfig(db) {
  const snap = await refConfig(db).get();
  const datos = snap.exists ? snap.data() || {} : {};
  return { tipos: datos.tipos || {}, ultimosEnsayos: datos.ultimosEnsayos || {} };
}

/**
 * Operación activa la retirada de un tipo **después** de revisar su ensayo en
 * seco (RN-6.10-O, Escenario 4).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {string} tipoId
 * @param {Object} [opciones]
 * @param {string} [opciones.quien] - Quién lo activa, para la constancia
 * @param {boolean} [opciones.aplicar=false] - Sin aplicar, solo dice lo que haría
 * @param {Function} [opciones.ahora]
 * @returns {Promise<{estado: 'activado'|'simulado'|'ya-activo'|'sin-ensayo'|'tipo-desconocido', ensayo?: Object}>}
 */
async function activarTipo(db, tipoId, { quien = 'operacion', aplicar = false, ahora = () => new Date() } = {}) {
  if (!tipoPorId(tipoId)) return { estado: 'tipo-desconocido' };

  const config = await leerConfig(db);
  if (config.tipos[tipoId] && config.tipos[tipoId].activo === true) {
    return { estado: 'ya-activo', activadoAt: config.tipos[tipoId].activadoAt };
  }

  const ensayo = config.ultimosEnsayos[tipoId];
  if (!ensayo || ensayo.completo !== true) return { estado: 'sin-ensayo' };

  if (!aplicar) return { estado: 'simulado', ensayo };

  await refConfig(db).set({
    tipos: {
      [tipoId]: {
        activo: true,
        activadoAt: ahora().toISOString(),
        activadoPor: quien,
        ensayoRevisado: ensayo.runId,
      },
    },
  }, { mergeFields: [`tipos.${tipoId}`] });
  console.log(`[retention] ${tipoId} activado por ${quien} tras revisar el ensayo ${ensayo.runId}`);
  return { estado: 'activado', ensayo };
}

// ============================================================================
// LA EJECUCIÓN
// ============================================================================

/**
 * Una ejecución de la limpieza: todos los tipos, con su constancia.
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} [opciones]
 * @param {Function} [opciones.ahora] - Reloj; también fija la fecha de los cortes
 * @param {boolean} [opciones.soloEnsayo=false] - Cuenta todos los tipos sin retirar nada
 * @param {Function} [opciones.medirEspacio] - Bytes ocupados según la plataforma (best-effort)
 * @param {number} [opciones.presupuestoMs=PRESUPUESTO_MS]
 * @param {string} [opciones.origen='programada']
 * @returns {Promise<Object>} La constancia, tal como quedó escrita
 */
async function ejecutarLimpieza(db, {
  ahora = () => new Date(),
  soloEnsayo = false,
  medirEspacio = null,
  presupuestoMs = PRESUPUESTO_MS,
  origen = 'programada',
} = {}) {
  const inicio = ahora();
  const id = idDeConstancia(inicio);
  const ref = db.collection(COLECCION_CONSTANCIAS).doc(id);

  const constancia = {
    id,
    origen,
    soloEnsayo,
    inicio: inicio.toISOString(),
    fin: null,
    estado: 'en-curso',
    detenidoPor: null,
    almacenamientoBytes: null,
    tipos: {},
  };

  // RN-6.10.1-J: lanzada a mano dentro de la franja, no arranca.
  if (enHorarioNocturno(inicio)) {
    Object.assign(constancia, { estado: 'no-iniciada', detenidoPor: MOTIVO_PARADA.HORARIO_NOCTURNO, fin: inicio.toISOString() });
    await ref.set(constancia);
    console.warn(`[retention] ${id}: no se inicia dentro del horario nocturno (00:00–03:00 ET)`);
    return constancia;
  }

  if (medirEspacio) {
    try {
      constancia.almacenamientoBytes = await medirEspacio();
    } catch (error) {
      console.warn(`[retention] ${id}: no se pudo medir el almacenamiento — ${error.message}`);
    }
  }

  const config = await leerConfig(db);
  await ref.set(constancia);

  const debeParar = () => {
    const t = ahora();
    if (t.getTime() - inicio.getTime() >= presupuestoMs) return MOTIVO_PARADA.PRESUPUESTO;
    if (enHorarioNocturno(t)) return MOTIVO_PARADA.HORARIO_NOCTURNO;
    return null;
  };

  const ensayos = {};

  for (const tipo of TIPOS) {
    const activo = !soloEnsayo && Boolean(config.tipos[tipo.id] && config.tipos[tipo.id].activo === true);
    const modo = activo ? MODO.RETIRADA : MODO.ENSAYO;
    const corte = corteDe(tipo, inicio);
    const base = { modo, corte: corte.fecha, plazo: { ...tipo.plazo } };

    const motivoAntes = debeParar();
    if (motivoAntes) {
      constancia.detenidoPor = motivoAntes;
      break;
    }

    try {
      const resultado = activo
        ? await retirarVencidos(db, tipo, corte, {
          debeParar,
          alTerminarTanda: async (avance) => {
            constancia.tipos[tipo.id] = { ...base, ...avance, completo: false };
            await ref.set(constancia);
          },
        })
        : await contarVencidos(db, tipo, corte, { debeParar });

      constancia.tipos[tipo.id] = { ...base, ...resultado };
      if (!activo) {
        ensayos[tipo.id] = {
          runId: id,
          at: inicio.toISOString(),
          corte: corte.fecha,
          registros: resultado.registros,
          porTramo: resultado.porTramo,
          completo: resultado.completo,
        };
      }
      console.log(`[retention] ${id} ${tipo.id} (${modo}, corte ${corte.fecha}): ${resultado.registros} registros` +
        `${activo ? ' retirados' : ' vencidos'}${resultado.completo ? '' : ` — detenido por ${resultado.detenidoPor}`}`);
    } catch (error) {
      constancia.tipos[tipo.id] = { ...base, completo: false, error: error.message };
      console.error(`[retention] ${id} ${tipo.id}: ${error.message}`);
    }

    await ref.set(constancia);

    if (constancia.tipos[tipo.id].detenidoPor) {
      constancia.detenidoPor = constancia.tipos[tipo.id].detenidoPor;
      break;
    }
  }

  const conError = Object.values(constancia.tipos).some((t) => t.error);
  constancia.fin = ahora().toISOString();
  constancia.estado = constancia.detenidoPor ? 'parcial' : (conError ? 'con-errores' : 'completa');
  await ref.set(constancia);

  // `mergeFields` reemplaza el ensayo de cada tipo entero: con `merge` los
  // meses de un ensayo anterior se quedarían pegados al nuevo.
  const tiposEnsayados = Object.keys(ensayos);
  if (tiposEnsayados.length > 0) {
    await refConfig(db).set(
      { ultimosEnsayos: ensayos },
      { mergeFields: tiposEnsayados.map((t) => `ultimosEnsayos.${t}`) }
    );
  }

  console.log(`[retention] ${id}: ${constancia.estado}` +
    `${constancia.detenidoPor ? ` (detenida por ${constancia.detenidoPor}; la siguiente ejecución continúa)` : ''}`);
  return constancia;
}

// ============================================================================
// INFORME PARA OPERACIÓN
// ============================================================================

/**
 * Las últimas constancias, de la más antigua a la más reciente. Cada una lleva
 * la diferencia de almacenamiento frente a la anterior medida: positiva si
 * ocupa menos (RN-6-I).
 *
 * @param {FirebaseFirestore.Firestore} db
 * @param {Object} [opciones]
 * @param {number} [opciones.limite=10]
 * @returns {Promise<{config: Object, constancias: Array<Object>}>}
 */
async function leerInforme(db, { limite = 10 } = {}) {
  const [config, snapshot] = await Promise.all([
    leerConfig(db),
    db.collection(COLECCION_CONSTANCIAS).orderBy(FieldPath.documentId(), 'desc').limit(limite).get(),
  ]);

  const constancias = snapshot.docs.map((d) => d.data()).reverse();
  let previa = null;
  for (const c of constancias) {
    c.liberadoBytes = (previa !== null && typeof c.almacenamientoBytes === 'number')
      ? previa - c.almacenamientoBytes
      : null;
    if (typeof c.almacenamientoBytes === 'number') previa = c.almacenamientoBytes;
  }
  return { config, constancias };
}

module.exports = {
  TAMANO_TANDA,
  PRESUPUESTO_MS,
  MOTIVO_PARADA,
  MODO,
  paginaVencidos,
  contarVencidos,
  retirarVencidos,
  leerConfig,
  activarTipo,
  ejecutarLimpieza,
  leerInforme,
};
