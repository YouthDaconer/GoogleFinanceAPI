#!/usr/bin/env node

/**
 * CLI Script — Reconstruir la base de costo de un saldo, entrada a entrada.
 *
 * **El problema que cierra.** Cuando el historial de un saldo tiene entradas sin
 * tasa —movimientos importados de antes de la épica #2—, el replay no puede
 * determinar su costo y la migración acaba pidiendo al usuario **una sola tasa
 * para todo el saldo**. Ese número global tapa la realidad: un saldo de
 * 223.191 COP formado por siete entradas repartidas entre enero y junio se
 * valoraba con una única tasa tecleada a mano, un 20 % desviada de la de
 * mercado y sin nada que lo delatara.
 *
 * Lo correcto es lo que hace este script: **cada entrada se valora a la tasa de
 * SU fecha**, esa tasa se escribe en el propio movimiento —que es lo que hace
 * inmutable el pasado (RN-3-B)— y la base sale del replay, no de una
 * afirmación del usuario.
 *
 * **Origen del efectivo.** Ninguna de estas entradas es una compra: un ingreso,
 * un dividendo o una venta en la propia divisa no gastan moneda de referencia.
 * El script las marca como traducidas (`convertedAmount: 0`), así que al salir
 * no realizarán una diferencia en cambio que nadie tuvo. Sólo una conversión
 * compra divisa de verdad, y una conversión ya trae su tasa.
 *
 * **Qué NO toca.** Ni saldos, ni montos, ni movimientos de salida. Sólo añade
 * la tasa que faltaba en las entradas y reescribe la base derivada de ellas.
 * (La opción de la HU 6.14.1, más abajo, sí ajusta el costo liberado de los
 * retiros que dependen de la tasa que reemplaza.)
 *
 * Uso:
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP --account ID
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP --apply
 *
 * Por defecto es **dry-run**: enseña la tabla de tasas y el costo resultante
 * sin escribir nada.
 *
 * **HU 6.14.1 — Reemplazar la tasa de entradas ya valoradas.** Este script
 * valoró el 2026-08-31 tres entradas de un saldo en COP con la serie de tasas
 * que, en verano británico, fechaba cada cierre un día antes (arreglada en la
 * 6.9.2 el 2026-11-14). `--reemplazar-tasas` les pone el cierre de su día:
 *
 *   --reemplazar-tasas corridas   las de verano anteriores al 2026-11-14 con
 *                                 fuente `market-date` cuya tasa coincide con el
 *                                 cierre del día siguiente y no con el de su día
 *                                 (RN-6.14.1-A)
 *   --reemplazar-tasas <id,id>    esas entradas concretas
 *   --tasas-de <constancia.json>  en vez del cierre del día, los valores
 *                                 ANTERIORES de esa constancia: es como se deshace
 *                                 (RN-6-H)
 *
 * Recalcula el costo de cada entrada, el costo que liberaron los retiros
 * posteriores (solo en lo que cambia por la tasa, RN-6.14.1-B) y la base con el
 * mismo replay. El saldo no cambia: los montos están en su divisa. Con `--apply`
 * guarda **antes de escribir** una constancia JSON con los valores anteriores y
 * los nuevos (en la carpeta de la historia, o en `--salida`) y marca al inversor
 * para la corrección nocturna desde la primera fecha. Sin nada que cambiar no
 * escribe, no deja constancia ni marca (RN-6.14-F).
 *
 * Necesita el token del servicio de tasas en `CF_SERVICE_TOKEN`, salvo con
 * `--tasas-de`.
 *
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP --reemplazar-tasas corridas
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP --reemplazar-tasas corridas --apply
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP --reemplazar-tasas <id> --tasas-de <constancia.json> --apply
 *
 * @see services/helpers/balanceLedger.js
 * @see services/helpers/balanceCostBasis.js
 * @see platform-docs/stories/6.14.1-tres-operaciones-tasa-corregida/refinamiento.md
 */

const fs = require('fs');
const path = require('path');
const { DateTime } = require('luxon');
const admin = require('../services/firebaseAdmin');
const historicalRateService = require('../services/historicalRateService');
const { projectBalanceLedger } = require('../services/helpers/balanceLedger');
const { getUserReferenceCurrency } = require('../services/helpers/balanceCostBasis');

const db = admin.firestore();

/**
 * Todas las formas en que puede entrar dinero a un saldo sin haberlo comprado.
 *
 * Se cubren las cuatro porque el replay es implacable: **una sola** entrada sin
 * tasa deja el saldo indeterminado a partir de ahí, y ninguna entrada posterior
 * con costo conocido lo rescata (es deliberado — no se conoce el costo del
 * dinero que ya estaba). Arreglar sólo los ingresos y dejar una venta sin tasa
 * no arregla nada.
 *
 * La conversión no está aquí: siempre trae su tasa, y además sí es una compra.
 *
 * Cada tipo declara de qué campo sale el monto que entra y en qué campo vive su
 * tasa, porque no coinciden: 2.4 le dio nombre propio (`realizationRate`) a la
 * tasa de las entradas que vienen de una realización.
 */
const INFLOW_SPECS = {
  cash_income: {
    rateField: 'acquisitionRate',
    amountOf: (t) => Number(t.amount),
  },
  cash_adjustment: {
    rateField: 'acquisitionRate',
    amountOf: (t) => Number(t.adjustmentDelta),
  },
  sell: {
    rateField: 'realizationRate',
    // Entra el producto neto de comisión (2.4).
    amountOf: (t) => (Number(t.amount) || 0) * (Number(t.price) || 0) - (Number(t.commission) || 0),
  },
  dividendPay: {
    rateField: 'realizationRate',
    // `price` ya viene neto por unidad tras la retención.
    amountOf: (t) => (Number(t.amount) || 0) * (Number(t.price) || 0),
  },
};

function printUsage() {
  console.log('');
  console.log('Reconstruye la base de costo de un saldo valorando cada entrada a la tasa de su fecha.');
  console.log('');
  console.log('  --email <correo>     Usuario (obligatorio)');
  console.log('  --currency <ISO>     Divisa del saldo, ej. COP (obligatorio)');
  console.log('  --account <id>       Limitar a una cuenta. Si se omite, todas las del usuario.');
  console.log('  --apply              Escribe de verdad. Sin esto, solo informa.');
  console.log('');
  console.log('  HU 6.14.1 — reemplazar la tasa de entradas ya valoradas:');
  console.log('  --reemplazar-tasas corridas|<id,id>');
  console.log('                       Las de verano anteriores al 2026-11-14 que tienen el cierre');
  console.log('                       del día siguiente, o esas entradas. Les pone el cierre de su día.');
  console.log('  --tasas-de <json>    Usa los valores ANTERIORES de una constancia (deshacer).');
  console.log('  --salida <json>      Dónde guardar la constancia (por defecto, la carpeta de la HU).');
  console.log('  --help');
  console.log('');
}

function parseArgs(argv) {
  const args = {
    email: null,
    currency: null,
    account: null,
    apply: false,
    help: false,
    reemplazarTasas: null,
    tasasDe: null,
    salida: null,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--help' || a === '-h') args.help = true;
    else if (a === '--apply') args.apply = true;
    else if (a === '--email') args.email = argv[++i] || null;
    else if (a === '--currency') args.currency = (argv[++i] || '').toUpperCase() || null;
    else if (a === '--account') args.account = argv[++i] || null;
    else if (['--reemplazar-tasas', '--tasas-de', '--salida'].includes(a)) {
      // Sin valor no se sigue: `--reemplazar-tasas` vacío caería en el flujo
      // de siempre, que con `--apply` escribe otras cosas.
      const valor = argv[++i];
      if (!valor || valor.startsWith('--')) {
        console.error('Falta el valor de ' + a);
        args.help = true;
      } else if (a === '--reemplazar-tasas') args.reemplazarTasas = valor;
      else if (a === '--tasas-de') args.tasasDe = valor;
      else args.salida = valor;
    } else {
      console.error('Argumento no reconocido: ' + a);
      args.help = true;
    }
  }
  return args;
}

/**
 * Día de un movimiento, venga como string ISO o como Timestamp.
 *
 * @param {*} value - Campo `date` del documento
 * @returns {string} `YYYY-MM-DD`
 */
function dayOf(value) {
  if (!value) return '';
  if (typeof value === 'string') return value.substring(0, 10);
  if (typeof value.toDate === 'function') return value.toDate().toISOString().substring(0, 10);
  return String(value).substring(0, 10);
}

/**
 * Costo en la moneda de referencia de lo que entra, a 2 decimales.
 *
 * Es el formato que ya tiene `acquisitionCost` en todos sus escritores (este
 * script y `balanceAdjustment.js`). La HU 6.14.1 conserva ese formato aunque los
 * estándares pidan no redondear lo que entra en otro cálculo: un costo con otro
 * redondeo cambiaría el replay del resto de entradas (decisión del PO).
 *
 * @param {number} monto - Monto que entra, en la divisa del saldo
 * @param {number} tasa - Unidades de la referencia por 1 de la divisa
 * @returns {number}
 */
function costoDe(monto, tasa) {
  return Math.round(monto * tasa * 100) / 100;
}

/**
 * Campo donde un tipo de entrada guarda el origen de su tasa.
 *
 * @param {Object} spec - Entrada de `INFLOW_SPECS`
 * @returns {string}
 */
function rateSourceFieldOf(spec) {
  return spec.rateField === 'realizationRate' ? 'realizationRateSource' : 'acquisitionRateSource';
}

// ============================================================================
// HU 6.14.1 — REEMPLAZAR LA TASA DE ENTRADAS YA VALORADAS
// ============================================================================

/** Carpeta de la historia: ahí va la constancia si no se dice otra cosa */
const CARPETA_HISTORIA_6_14_1 = path.resolve(
  __dirname, '..', '..', '..', '..',
  'platform-docs', 'stories', '6.14.1-tres-operaciones-tasa-corregida',
);

/** Desde este día la serie ya fecha bien los cierres de verano (HU 6.9.2) */
const SERIE_CORREGIDA_DESDE = '2026-11-14';

/** Origen con el que este script marcó las tasas que valoró */
const FUENTE_MERCADO_FECHA = 'market-date';

/** Por qué una entrada se corrige o se deja como está (RN-6.14-C) */
const MOTIVOS = Object.freeze({
  CORREGIR: 'corregir',
  YA_CORRECTA: 'ya-correcta',
  AMBIGUA: 'ambigua',
  NO_ENCAJA: 'no-encaja',
  INVIERNO: 'invierno',
  POSTERIOR_AL_ARREGLO: 'posterior-al-arreglo',
  OTRA_FUENTE: 'otra-fuente',
  SIN_CIERRE: 'sin-cierre',
  SIN_TASA: 'sin-tasa',
  NO_ES_ENTRADA: 'no-es-entrada',
  NO_ENCONTRADA: 'no-encontrada',
  NO_ESTA_EN_CONSTANCIA: 'no-esta-en-la-constancia',
});

/**
 * Último domingo de un mes, `YYYY-MM-DD`.
 *
 * @param {number} anio
 * @param {number} mes - 1 a 12
 * @returns {string}
 */
function ultimoDomingo(anio, mes) {
  const ultimo = DateTime.fromObject({ year: anio, month: mes, day: 1 }, { zone: 'UTC' }).endOf('month');
  // Luxon numera los días de 1 (lunes) a 7 (domingo).
  return ultimo.minus({ days: ultimo.weekday % 7 }).toISODate();
}

/**
 * ¿El día cae en verano británico? Del último domingo de marzo al último de
 * octubre: es cuando la serie de tasas fechaba los cierres un día antes.
 *
 * @param {string} dia - `YYYY-MM-DD`
 * @returns {boolean}
 */
function esVeranoBritanico(dia) {
  const anio = Number(String(dia || '').substring(0, 4));
  if (!anio) return false;
  return dia >= ultimoDomingo(anio, 3) && dia < ultimoDomingo(anio, 10);
}

/**
 * Día natural siguiente, `YYYY-MM-DD`.
 *
 * @param {string} dia
 * @returns {string}
 */
function diaSiguiente(dia) {
  return DateTime.fromISO(dia, { zone: 'UTC' }).plus({ days: 1 }).toISODate();
}

/**
 * Decimales con que se guardó un número (`0.000312` → 6), con o sin exponente.
 *
 * @param {number} valor
 * @returns {number}
 */
function decimalesDe(valor) {
  const [mantisa, exponente] = String(valor).toLowerCase().split('e');
  const decimales = (mantisa.split('.')[1] || '').length;
  return Math.max(0, decimales - (exponente ? Number(exponente) : 0));
}

/**
 * ¿La tasa guardada es ese cierre?
 *
 * Una tasa guardada con pocos decimales (los retiros guardan 6) no se puede
 * distinguir más allá de su último decimal: se acepta media unidad de él. Con
 * precisión completa, una diferencia relativa de 1e-7, muy por debajo de lo que
 * se mueve una tasa de un día al siguiente (la menor del caso, 0,05 %).
 *
 * @param {number} guardada
 * @param {number} cierre
 * @returns {boolean}
 */
function coincide(guardada, cierre) {
  const g = Number(guardada);
  const c = Number(cierre);
  if (!(g > 0) || !(c > 0)) return false;

  const decimales = decimalesDe(g);
  const cuantizacion = decimales < 15 ? 0.5 * 10 ** -decimales : 0;
  return Math.abs(g - c) <= Math.max(cuantizacion, c * 1e-7);
}

/**
 * Qué hacer con una tasa de verano según los dos cierres (RN-6.14.1-A).
 *
 * @param {number} guardada
 * @param {number|null} cierreDelDia
 * @param {number|null} cierreDelSiguiente
 * @returns {string} Uno de `MOTIVOS`
 */
function clasificarTasa(guardada, cierreDelDia, cierreDelSiguiente) {
  if (!(cierreDelDia > 0) || !(cierreDelSiguiente > 0)) return MOTIVOS.SIN_CIERRE;

  const delDia = coincide(guardada, cierreDelDia);
  const delSiguiente = coincide(guardada, cierreDelSiguiente);

  if (delDia && delSiguiente) return MOTIVOS.AMBIGUA;
  if (delDia) return MOTIVOS.YA_CORRECTA;
  if (delSiguiente) return MOTIVOS.CORREGIR;
  return MOTIVOS.NO_ENCAJA;
}

/**
 * Las entradas de un saldo que tienen el cierre del día siguiente (RN-6.14.1-A).
 *
 * Solo mira entradas que ya tienen tasa: las que no la tienen son del flujo de
 * siempre. Lo que no encaja se devuelve con su motivo para listarlo (RN-6.14-C).
 *
 * @param {Array<Object>} transactions - Movimientos de la cuenta, con `id`
 * @param {Object} params
 * @param {string} params.currency - Divisa del saldo
 * @param {function(string): Promise<number|null>} params.obtenerCierre - Cierre de un día
 * @returns {Promise<{corregir: Array<Object>, noTocadas: Array<Object>}>}
 */
async function detectarCorridas(transactions, { currency, obtenerCierre }) {
  const corregir = [];
  const noTocadas = [];

  const entradas = transactions
    .filter((t) => t.currency === currency && INFLOW_SPECS[t.type])
    .sort((a, b) => dayOf(a.date).localeCompare(dayOf(b.date)));

  for (const t of entradas) {
    const spec = INFLOW_SPECS[t.type];
    const guardada = t[spec.rateField];
    if (guardada === undefined || guardada === null) continue;

    const dia = dayOf(t.date);
    const base = { id: t.id, fecha: dia, tipo: t.type, tasaActual: Number(guardada) };

    if (t[rateSourceFieldOf(spec)] !== FUENTE_MERCADO_FECHA) {
      noTocadas.push({ ...base, motivo: MOTIVOS.OTRA_FUENTE });
      continue;
    }
    if (dia >= SERIE_CORREGIDA_DESDE) {
      noTocadas.push({ ...base, motivo: MOTIVOS.POSTERIOR_AL_ARREGLO });
      continue;
    }
    if (!esVeranoBritanico(dia)) {
      noTocadas.push({ ...base, motivo: MOTIVOS.INVIERNO });
      continue;
    }

    const cierreDelDia = await obtenerCierre(dia);
    const cierreDelSiguiente = await obtenerCierre(diaSiguiente(dia));
    const motivo = clasificarTasa(guardada, cierreDelDia, cierreDelSiguiente);
    const fila = { ...base, cierreDelDia, cierreDelSiguiente };

    if (motivo === MOTIVOS.CORREGIR) corregir.push({ ...fila, tasa: cierreDelDia });
    else noTocadas.push({ ...fila, motivo });
  }

  return { corregir, noTocadas };
}

/**
 * Un movimiento de una constancia, por id.
 *
 * @param {Object} constancia - JSON guardado por `--apply`
 * @param {string} id
 * @returns {Object|null}
 */
function movimientoDeConstancia(constancia, id) {
  for (const cuenta of (constancia && constancia.cuentas) || []) {
    const movimiento = (cuenta.movimientos || []).find((m) => m.id === id);
    if (movimiento) return movimiento;
  }
  return null;
}

/**
 * Las entradas pedidas por id, con la tasa que les toca: el cierre de su día o,
 * con una constancia, los valores que tenían antes (deshacer, RN-6-H).
 *
 * @param {Array<Object>} transactions - Movimientos de la cuenta, con `id`
 * @param {Array<string>} ids - Entradas pedidas
 * @param {Object} params
 * @param {string} params.currency
 * @param {function(string): Promise<number|null>} params.obtenerCierre
 * @param {Object|null} [params.constanciaPrevia]
 * @returns {Promise<{corregir: Array<Object>, noTocadas: Array<Object>}>}
 */
async function seleccionarPorId(transactions, ids, { currency, obtenerCierre, constanciaPrevia = null }) {
  const corregir = [];
  const noTocadas = [];

  for (const t of transactions.filter((tx) => ids.includes(tx.id))) {
    const spec = INFLOW_SPECS[t.type];
    const dia = dayOf(t.date);
    const base = { id: t.id, fecha: dia, tipo: t.type };

    if (!spec || t.currency !== currency) {
      noTocadas.push({ ...base, motivo: MOTIVOS.NO_ES_ENTRADA });
      continue;
    }

    const guardada = t[spec.rateField];
    if (guardada === undefined || guardada === null) {
      noTocadas.push({ ...base, motivo: MOTIVOS.SIN_TASA });
      continue;
    }
    base.tasaActual = Number(guardada);

    if (constanciaPrevia) {
      const previo = movimientoDeConstancia(constanciaPrevia, t.id);
      if (!previo) noTocadas.push({ ...base, motivo: MOTIVOS.NO_ESTA_EN_CONSTANCIA });
      else corregir.push({ ...base, tasa: previo.tasaAntes, costo: previo.costoAntes });
      continue;
    }

    const cierreDelDia = await obtenerCierre(dia);
    if (!(cierreDelDia > 0)) noTocadas.push({ ...base, motivo: MOTIVOS.SIN_CIERRE });
    else if (coincide(guardada, cierreDelDia)) noTocadas.push({ ...base, cierreDelDia, motivo: MOTIVOS.YA_CORRECTA });
    else corregir.push({ ...base, cierreDelDia, tasa: cierreDelDia });
  }

  return { corregir, noTocadas };
}

/**
 * Lo que cambia en un saldo al poner otra tasa a algunas de sus entradas.
 *
 * El replay se hace dos veces, con las tasas de hoy y con las nuevas, por el
 * mismo `projectBalanceLedger` que rehace la base. De los retiros posteriores
 * cambia **solo lo que cambia la tasa**: el costo liberado guardado se mueve por
 * `monto × (promedio nuevo − promedio de hoy)` justo antes del retiro, así que
 * una deriva anterior no se toca ni se esconde (RN-6.14.1-B). Su diferencia en
 * cambio no se recalcula: depende de la porción comprada por conversión, que
 * estas entradas no tienen.
 *
 * Lo que heredaría el cambio y este script no sabe recalcular deja la cuenta
 * sin tocar (`noSoportados`): una compra posterior valorada al promedio del
 * saldo, una conversión que saca esa divisa, o una venta o dividendo a corregir
 * que ya tiene el desglose de la 2.4.
 *
 * @param {Object} params
 * @param {Array<Object>} params.transactions - Movimientos de la cuenta, con `id`
 * @param {string} params.currency
 * @param {string} params.referenceCurrency
 * @param {number} params.balance - Saldo guardado
 * @param {Object|null} params.storedBasis - `balanceCostBasis.{divisa}` guardado
 * @param {Array<{id: string, tasa: number, costo?: number}>} params.reemplazos
 * @returns {{movimientos: Array<Object>, retiros: Array<Object>, noSoportados: Array<Object>,
 *   desde: string|null, base: Object|null}}
 */
function planificarReemplazo({ transactions, currency, referenceCurrency, balance, storedBasis, reemplazos }) {
  const porId = new Map(reemplazos.map((r) => [r.id, r]));
  const movimientos = [];

  const nuevas = transactions.map((t) => {
    const reemplazo = porId.get(t.id);
    if (!reemplazo) return t;

    const spec = INFLOW_SPECS[t.type];
    const monto = spec.amountOf(t);
    const costo = reemplazo.costo !== undefined && reemplazo.costo !== null
      ? reemplazo.costo
      : costoDe(monto, reemplazo.tasa);
    const tasaAntes = Number(t[spec.rateField]);
    const costoAntes = t.acquisitionCost !== undefined ? t.acquisitionCost : null;

    if (tasaAntes === reemplazo.tasa && costoAntes === costo) return t;

    movimientos.push({
      id: t.id,
      fecha: dayOf(t.date),
      tipo: t.type,
      campoTasa: spec.rateField,
      monto,
      tasaAntes,
      tasaDespues: reemplazo.tasa,
      costoAntes,
      costoDespues: costo,
    });
    return Object.assign({}, t, { [spec.rateField]: reemplazo.tasa, acquisitionCost: costo });
  });

  if (movimientos.length === 0) {
    return { movimientos, retiros: [], noSoportados: [], desde: null, base: null };
  }

  // Firestore no los devuelve en orden: la lista y la constancia van por fecha.
  movimientos.sort((a, b) => a.fecha.localeCompare(b.fecha));
  const desde = movimientos[0].fecha;
  const corregidas = new Set(movimientos.map((m) => m.id));

  const noSoportados = transactions
    .filter((t) => dayOf(t.date) >= desde)
    .map((t) => {
      let motivo = null;
      if (t.type === 'buy' && t.currency === currency && t.acquisitionRateSource === 'balance-average') {
        motivo = 'compra valorada al promedio del saldo';
      } else if (t.type === 'cash_conversion' && t.currency === currency) {
        motivo = 'conversión que saca la divisa';
      } else if (corregidas.has(t.id) && (t.type === 'sell' || t.type === 'dividendPay')
        && t.realizedFxAvailability !== undefined && t.realizedFxAvailability !== null) {
        // Una apertura también trae `realizedFxAvailability` ('not-applicable'):
        // solo una venta o un dividendo tienen el desglose que esto no recalcula.
        motivo = 'entrada con desglose de la 2.4';
      }
      return motivo ? { id: t.id, fecha: dayOf(t.date), tipo: t.type, motivo } : null;
    })
    .filter(Boolean);

  const params = { currency, referenceCurrency, balance };
  const actual = projectBalanceLedger(Object.assign({ transactions }, params));
  const corregido = projectBalanceLedger(Object.assign({ transactions: nuevas }, params));

  // Las filas vienen de la más reciente a la más antigua; el orden no depende
  // de las tasas, así que las dos proyecciones se recorren a la par.
  const filasHoy = actual.rows.slice().reverse();
  const filasNuevas = corregido.rows.slice().reverse();
  const porIdTx = new Map(transactions.map((t) => [t.id, t]));
  const retiros = [];

  filasHoy.forEach((fila, i) => {
    if (filasNuevas[i].id !== fila.id) {
      throw new Error(`El replay corregido no sigue el mismo orden (${fila.id} / ${filasNuevas[i].id})`);
    }
    if (fila.type !== 'cash_expense' || fila.date < desde || i === 0) return;

    const guardado = porIdTx.get(fila.id).releasedCost;
    if (guardado === undefined || guardado === null) return;

    const promedioHoy = filasHoy[i - 1].averageRateAfter;
    const promedioNuevo = filasNuevas[i - 1].averageRateAfter;
    if (promedioHoy === null || promedioNuevo === null) return;

    const monto = Math.abs(fila.amount);
    const costoLiberado = Math.round((guardado + monto * (promedioNuevo - promedioHoy)) * 100) / 100;
    if (costoLiberado === guardado) return;

    retiros.push({
      id: fila.id,
      fecha: fila.date,
      monto,
      costoLiberadoAntes: guardado,
      costoLiberadoDespues: costoLiberado,
    });
  });

  const replay = corregido.replayedCostBasis;

  return {
    movimientos,
    retiros,
    noSoportados,
    desde,
    base: {
      saldo: balance,
      costoAntes: storedBasis && storedBasis.cost !== undefined ? storedBasis.cost : null,
      costoDespues: replay && replay.cost !== undefined ? replay.cost : null,
      estado: (replay && replay.status) || 'unknown',
      convertedAmount: replay && replay.convertedAmount !== undefined ? replay.convertedAmount : 0,
      convertedCost: replay && replay.convertedCost !== undefined ? replay.convertedCost : 0,
      conciliacion: corregido.reconciliation,
    },
  };
}

/**
 * Lo que se escribe en la cuenta: la base que sale del replay y su conciliación.
 *
 * El flujo de siempre pone la porción convertida a 0 (sus entradas no compraron
 * divisa); la HU 6.14.1 conserva la que da el replay.
 *
 * @param {Object} params
 * @param {string} params.currency
 * @param {string} params.referenceCurrency
 * @param {{cost: *, status: string}|null} params.replayed - Base del replay
 * @param {Object} params.reconciliation - Conciliación del replay
 * @param {{convertedAmount: number, convertedCost: number}} params.conversiones
 * @param {Object} params.FieldValue - `admin.firestore.FieldValue`
 * @returns {Object} Actualización para el documento de la cuenta
 */
function actualizacionDeBase({ currency, referenceCurrency, replayed, reconciliation, conversiones, FieldValue }) {
  return {
    ['balanceCostBasis.' + currency]: {
      cost: replayed && replayed.cost !== undefined ? replayed.cost : null,
      referenceCurrency,
      status: (replayed && replayed.status) || 'unknown',
      source: 'ledger-replay',
      convertedAmount: conversiones.convertedAmount,
      convertedCost: conversiones.convertedCost,
      updatedAt: FieldValue.serverTimestamp(),
    },
    ['balanceReconciliation.' + currency]: {
      ledgerBalance: reconciliation.ledgerBalance,
      difference: reconciliation.difference,
      status: reconciliation.status,
      checkedAt: FieldValue.serverTimestamp(),
    },
  };
}

/**
 * Escribe lo planificado: primero la constancia, después un lote por cuenta y
 * al final la marca para la corrección nocturna (HU 6.14.1).
 *
 * La constancia va **antes** de tocar nada: si el lote falla, los valores
 * anteriores ya están a salvo para deshacer (RN-6-H, RN-6.14-E).
 *
 * @param {Object} params
 * @param {Object} params.db - Firestore
 * @param {Object} params.FieldValue - `admin.firestore.FieldValue`
 * @param {string} params.userId
 * @param {string} params.currency
 * @param {string} params.referenceCurrency
 * @param {Array<{accountId: string, plan: Object}>} params.cuentas - Solo las aplicables
 * @param {Object} params.constancia - Lo que se guarda
 * @param {function(Object): void} params.escribirConstancia
 * @param {function(string, string, Object): Promise<void>} params.marcarDesactualizado
 * @returns {Promise<{escrituras: number, desde: string}>}
 */
async function aplicarReemplazo({
  db,
  FieldValue,
  userId,
  currency,
  referenceCurrency,
  cuentas,
  constancia,
  escribirConstancia,
  marcarDesactualizado,
}) {
  escribirConstancia(constancia);

  let escrituras = 0;
  for (const { accountId, plan } of cuentas) {
    const batch = db.batch();
    let operaciones = 0;

    for (const m of plan.movimientos) {
      batch.update(db.collection('transactions').doc(m.id), {
        [m.campoTasa]: m.tasaDespues,
        acquisitionCost: m.costoDespues,
      });
      operaciones += 1;
    }
    for (const r of plan.retiros) {
      batch.update(db.collection('transactions').doc(r.id), { releasedCost: r.costoLiberadoDespues });
      operaciones += 1;
    }
    batch.update(db.collection('portfolioAccounts').doc(accountId), actualizacionDeBase({
      currency,
      referenceCurrency,
      replayed: { cost: plan.base.costoDespues, status: plan.base.estado },
      reconciliation: plan.base.conciliacion,
      conversiones: { convertedAmount: plan.base.convertedAmount, convertedCost: plan.base.convertedCost },
      FieldValue,
    }));
    operaciones += 1;

    await batch.commit();
    escrituras += operaciones;
  }

  const desde = cuentas.map((c) => c.plan.desde).sort()[0];
  await marcarDesactualizado(userId, desde, {
    reason: 'exchange-rate-correction',
    transactionType: 'rate-correction',
    portfolioAccount: cuentas.length === 1 ? cuentas[0].accountId : null,
  });
  escrituras += 1;

  return { escrituras, desde };
}

/** Tasa como la lee una persona: COP por USD en vez de USD por COP. */
function inversa(tasa) {
  return tasa > 0 ? (1 / tasa).toFixed(2) : '—';
}

/**
 * Imprime lo que cambiaría en una cuenta.
 *
 * @param {Object} seleccion - `corregir` y `noTocadas`
 * @param {Object} plan - De `planificarReemplazo`
 * @param {string} referenceCurrency
 */
function imprimirPlan(seleccion, plan, referenceCurrency) {
  if (seleccion.noTocadas.length > 0) {
    console.log('  no se tocan:');
    for (const n of seleccion.noTocadas) {
      console.log('    ' + n.fecha + '  ' + n.tipo.padEnd(15) + ' ' + n.id + '  tasa '
        + (n.tasaActual !== undefined ? n.tasaActual + ' (1/x ' + inversa(n.tasaActual) + ')' : '—')
        + (n.cierreDelDia ? '  cierre del día 1/x ' + inversa(n.cierreDelDia) : '')
        + (n.cierreDelSiguiente ? '  del siguiente 1/x ' + inversa(n.cierreDelSiguiente) : '')
        + '  → ' + n.motivo);
    }
  }

  if (plan.movimientos.length === 0) {
    console.log('  Nada que cambiar en esta cuenta.');
    return;
  }

  console.log('  entradas:');
  for (const m of plan.movimientos) {
    const sel = seleccion.corregir.find((c) => c.id === m.id) || {};
    console.log('    ' + m.fecha + '  ' + m.tipo.padEnd(15) + ' ' + m.id + '  ' + m.monto);
    console.log('        ' + m.campoTasa + ': ' + m.tasaAntes + ' (1/x ' + inversa(m.tasaAntes) + ') -> '
      + m.tasaDespues + ' (1/x ' + inversa(m.tasaDespues) + ')'
      + (sel.cierreDelSiguiente ? '   cierre del día siguiente 1/x ' + inversa(sel.cierreDelSiguiente) : ''));
    console.log('        acquisitionCost: ' + m.costoAntes + ' -> ' + m.costoDespues + ' ' + referenceCurrency);
  }
  for (const r of plan.retiros) {
    console.log('    retiro ' + r.fecha + ' ' + r.id + '  ' + r.monto
      + '  releasedCost: ' + r.costoLiberadoAntes + ' -> ' + r.costoLiberadoDespues + ' ' + referenceCurrency);
  }
  console.log('  saldo: ' + plan.base.saldo + ' (no cambia)   costo del saldo: ' + plan.base.costoAntes
    + ' -> ' + plan.base.costoDespues + ' ' + referenceCurrency + '   conciliación: ' + plan.base.conciliacion.status);
  console.log('  marca para la corrección nocturna desde: ' + plan.desde);
  if (plan.noSoportados.length > 0) {
    console.log('  NO SE APLICA esta cuenta: hay movimientos que heredan el cambio y este script no recalcula:');
    plan.noSoportados.forEach((n) => console.log('    ' + n.fecha + '  ' + n.tipo + ' ' + n.id + '  → ' + n.motivo));
  }
}

/**
 * Flujo de `--reemplazar-tasas` (HU 6.14.1).
 *
 * @param {Object} args - Argumentos ya leídos
 * @param {{uid: string, email: string}} userRecord
 * @param {string} referenceCurrency
 */
async function mainReemplazo(args, userRecord, referenceCurrency) {
  const userId = userRecord.uid;
  const currency = args.currency;
  const ids = args.reemplazarTasas === 'corridas'
    ? null
    : args.reemplazarTasas.split(',').map((s) => s.trim()).filter(Boolean);

  if (args.tasasDe && !ids) {
    console.log('Con --tasas-de hay que decir qué entradas devolver: --reemplazar-tasas <id,id>.');
    return;
  }
  if (currency === referenceCurrency) {
    console.log('La divisa del saldo es la de referencia: sus entradas no tienen tasa.');
    return;
  }

  const constanciaPrevia = args.tasasDe ? JSON.parse(fs.readFileSync(args.tasasDe, 'utf8')) : null;
  console.log('  Reemplazo : ' + (ids ? ids.join(', ') : 'corridas (verano, cierre del día siguiente)')
    + (constanciaPrevia ? '   valores ANTERIORES de ' + args.tasasDe : '   cierre del día'));
  console.log('');

  const cierres = new Map();
  const obtenerCierre = async (dia) => {
    if (!cierres.has(dia)) {
      let tasa = null;
      try {
        const resolved = await historicalRateService.getCrossRate(currency, referenceCurrency, dia);
        tasa = resolved && resolved.rate > 0 ? resolved.rate : null;
      } catch (error) {
        console.log('    ' + dia + '  ERROR consultando tasa: ' + error.message);
      }
      cierres.set(dia, tasa);
    }
    return cierres.get(dia);
  };

  let lecturas = 1; // la moneda de referencia del usuario
  const accountsSnap = await db.collection('portfolioAccounts').where('userId', '==', userId).get();
  lecturas += accountsSnap.size;
  const accounts = accountsSnap.docs.filter(
    (d) => (!args.account || d.id === args.account) && d.data() && d.data().balances
      && d.data().balances[currency] !== undefined,
  );

  const resultados = [];
  const vistos = new Set();

  for (const accountDoc of accounts) {
    const account = accountDoc.data();
    console.log('-'.repeat(70));
    console.log('Cuenta: ' + (account.name || accountDoc.id) + '  (' + accountDoc.id + ')');

    const txSnap = await db.collection('transactions').where('portfolioAccountId', '==', accountDoc.id).get();
    lecturas += txSnap.size;
    const transactions = txSnap.docs.map((d) => Object.assign({ id: d.id }, d.data()));
    transactions.forEach((t) => vistos.add(t.id));

    const seleccion = ids
      ? await seleccionarPorId(transactions, ids, { currency, obtenerCierre, constanciaPrevia })
      : await detectarCorridas(transactions, { currency, obtenerCierre });

    const plan = planificarReemplazo({
      transactions,
      currency,
      referenceCurrency,
      balance: Number(account.balances[currency]) || 0,
      storedBasis: (account.balanceCostBasis || {})[currency] || null,
      reemplazos: seleccion.corregir.map((c) => ({ id: c.id, tasa: c.tasa, costo: c.costo })),
    });

    imprimirPlan(seleccion, plan, referenceCurrency);
    resultados.push({ accountId: accountDoc.id, nombre: account.name || null, seleccion, plan });
  }

  (ids || []).filter((id) => !vistos.has(id)).forEach((id) => {
    console.log('  ' + id + '  → ' + MOTIVOS.NO_ENCONTRADA + ' (no es de este usuario o de esta divisa)');
  });

  const aplicables = resultados.filter((r) => r.plan.movimientos.length > 0 && r.plan.noSoportados.length === 0);
  const escriturasPrevistas = aplicables.reduce(
    (s, r) => s + r.plan.movimientos.length + r.plan.retiros.length + 1, 0,
  ) + (aplicables.length > 0 ? 1 : 0);

  console.log('');
  console.log('Lecturas: ' + lecturas + ' documentos (más las consultas al servicio de tasas: '
    + cierres.size + ' días).');

  if (aplicables.length === 0) {
    console.log('Nada que cambiar: no se escribe, no se guarda constancia y no se marca a nadie.');
    return;
  }

  console.log('Escrituras previstas: ' + escriturasPrevistas + ' (movimientos, retiros, base y la marca nocturna).');

  if (!args.apply) {
    console.log('Dry-run: no se ha escrito nada.');
    return;
  }

  const fecha = new Date().toISOString();
  const archivo = args.salida
    || path.join(CARPETA_HISTORIA_6_14_1, 'constancia-tasas-' + fecha.replace(/[:.]/g, '-') + '.json');
  const constancia = {
    historia: '6.14.1',
    reglas: ['RN-6.14.1-A', 'RN-6.14.1-B', 'RN-6-H'],
    fecha,
    estado: 'pendiente',
    origenDeLasTasas: constanciaPrevia ? 'constancia ' + args.tasasDe : 'cierre del día',
    usuario: { uid: userId, email: userRecord.email },
    moneda: currency,
    referencia: referenceCurrency,
    lecturas,
    escriturasPrevistas,
    cuentas: aplicables.map((r) => ({
      cuentaId: r.accountId,
      nombre: r.nombre,
      marcaDesde: r.plan.desde,
      movimientos: r.plan.movimientos,
      retiros: r.plan.retiros,
      base: {
        saldo: r.plan.base.saldo,
        costoAntes: r.plan.base.costoAntes,
        costoDespues: r.plan.base.costoDespues,
      },
    })),
  };
  const escribir = (contenido) => fs.writeFileSync(archivo, JSON.stringify(contenido, null, 2) + '\n');

  // Se requiere aquí: toma Firestore al cargarse y el flujo de siempre no la usa.
  const { markPerformanceAsStale } = require('../utils/performanceStaleMarker');

  const resultado = await aplicarReemplazo({
    db,
    FieldValue: admin.firestore.FieldValue,
    userId,
    currency,
    referenceCurrency,
    cuentas: aplicables.map((r) => ({ accountId: r.accountId, plan: r.plan })),
    constancia,
    escribirConstancia: escribir,
    marcarDesactualizado: markPerformanceAsStale,
  });

  const perf = await db.collection('portfolioPerformance').doc(userId).get();
  const marca = perf.exists ? perf.data()._stale : null;

  escribir(Object.assign({}, constancia, {
    estado: 'aplicada',
    aplicadaAt: new Date().toISOString(),
    escrituras: resultado.escrituras,
    marcaNocturna: marca ? { since: marca.since, desdeInicial: marca.desdeInicial || null } : null,
  }));

  console.log('Aplicado: ' + resultado.escrituras + ' escrituras. Marca nocturna: '
    + (marca ? 'desde ' + marca.since : 'NO QUEDÓ (revisar los registros de performanceStaleMarker)'));
  console.log('Constancia: ' + archivo);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.email || !args.currency) {
    printUsage();
    process.exit(args.help ? 0 : 1);
  }

  const userRecord = await admin.auth().getUserByEmail(args.email);
  const userId = userRecord.uid;
  const referenceCurrency = await getUserReferenceCurrency(userId);
  const currency = args.currency;

  console.log('');
  console.log('='.repeat(70));
  console.log('  Reconstruir base de costo por fecha de cada entrada');
  console.log('='.repeat(70));
  console.log('  Usuario    : ' + userRecord.email + ' (' + userId + ')');
  console.log('  Divisa     : ' + currency + '   Referencia: ' + referenceCurrency);
  console.log('  Modo       : ' + (args.apply ? 'APLICAR' : 'dry-run (no escribe)'));

  // HU 6.14.1: reemplazar la tasa de entradas que ya la tienen.
  if (args.reemplazarTasas) {
    await mainReemplazo(args, userRecord, referenceCurrency);
    return;
  }
  console.log('');

  if (currency === referenceCurrency) {
    console.log('La divisa del saldo es la de referencia: no hay base de costo que reconstruir.');
    return;
  }

  const accountsSnap = await db.collection('portfolioAccounts').where('userId', '==', userId).get();
  const accounts = accountsSnap.docs.filter(
    (d) => (!args.account || d.id === args.account) && d.data() && d.data().balances
      && d.data().balances[currency] !== undefined,
  );

  if (accounts.length === 0) {
    console.log('Ninguna cuenta con saldo en ' + currency + '.');
    return;
  }

  for (const accountDoc of accounts) {
    const account = accountDoc.data();
    console.log('-'.repeat(70));
    console.log('Cuenta: ' + (account.name || accountDoc.id) + '  (' + accountDoc.id + ')');
    console.log('  saldo: ' + account.balances[currency] + ' ' + currency);

    const before = (account.balanceCostBasis || {})[currency];
    console.log('  base actual: cost=' + (before ? before.cost : undefined)
      + ' source=' + ((before && before.source) || '(sin source)'));

    const txSnap = await db.collection('transactions')
      .where('portfolioAccountId', '==', accountDoc.id).get();
    const transactions = txSnap.docs.map((d) => Object.assign({ id: d.id, ref: d.ref }, d.data()));

    // Entradas de esta divisa a las que les falta la tasa.
    const pending = transactions.filter((t) => {
      if (t.currency !== currency) return false;
      const spec = INFLOW_SPECS[t.type];
      if (!spec) return false;
      if (!(spec.amountOf(t) > 0)) return false;
      const rate = t[spec.rateField];
      return rate === undefined || rate === null;
    }).sort((a, b) => dayOf(a.date).localeCompare(dayOf(b.date)));

    if (pending.length === 0) {
      console.log('  Todas las entradas ya tienen su tasa. Nada que hacer.');
      continue;
    }

    console.log('  entradas sin tasa: ' + pending.length);
    console.log('');
    console.log('    fecha       tipo                  monto   tasa                valor');

    const writes = [];
    let missing = 0;

    for (const t of pending) {
      const spec = INFLOW_SPECS[t.type];
      const day = dayOf(t.date);
      const amount = spec.amountOf(t);

      let resolved = null;
      try {
        resolved = await historicalRateService.getCrossRate(currency, referenceCurrency, day);
      } catch (error) {
        console.log('    ' + day + '  ERROR consultando tasa: ' + error.message);
      }

      if (!resolved || !(resolved.rate > 0)) {
        // RN-13: un dato ausente se declara ausente. No se rellena.
        missing += 1;
        console.log('    ' + day + '  ' + t.type.padEnd(15) + String(amount).padStart(12) + '   SIN TASA — se deja sin valorar');
        continue;
      }

      const cost = costoDe(amount, resolved.rate);
      console.log('    ' + day + '  ' + t.type.padEnd(15) + String(amount).padStart(12) + '   '
        + String(resolved.rate).padEnd(18) + '  ' + cost + ' ' + referenceCurrency);

      // La tasa va en el campo que su tipo de movimiento lee, y el costo en
      // `acquisitionCost`, que es de donde el replay saca el `costDelta`.
      const data = {
        acquisitionCost: cost,
        referenceCurrency,
      };
      data[spec.rateField] = resolved.rate;
      data[rateSourceFieldOf(spec)] = FUENTE_MERCADO_FECHA;

      writes.push({ ref: t.ref, data });
    }

    console.log('');
    if (missing > 0) {
      console.log('  ' + missing + ' entrada(s) sin tasa: la base quedara indeterminada (RN-13).');
      console.log('  Es correcto: mejor declararlo que inventar un numero.');
    }

    if (!args.apply) {
      console.log('  Dry-run: no se ha escrito nada.');
      continue;
    }

    // 1. La tasa se escribe en cada movimiento: es lo que hace inmutable el pasado.
    const batch = db.batch();
    writes.forEach((w) => batch.update(w.ref, w.data));
    await batch.commit();
    console.log('  ' + writes.length + ' movimiento(s) actualizados con su tasa.');

    // 2. La base sale del replay de esos movimientos, no de una afirmacion.
    const fresh = await db.collection('transactions')
      .where('portfolioAccountId', '==', accountDoc.id).get();
    const projection = projectBalanceLedger({
      transactions: fresh.docs.map((d) => Object.assign({ id: d.id }, d.data())),
      currency,
      referenceCurrency,
      balance: Number(account.balances[currency]) || 0,
    });

    const update = actualizacionDeBase({
      currency,
      referenceCurrency,
      replayed: projection.replayedCostBasis,
      reconciliation: projection.reconciliation,
      // Ninguna de estas entradas compro divisa: no realizan al salir.
      conversiones: { convertedAmount: 0, convertedCost: 0 },
      FieldValue: admin.firestore.FieldValue,
    });

    await accountDoc.ref.update(update);

    console.log('  base nueva: cost=' + update['balanceCostBasis.' + currency].cost
      + ' status=' + update['balanceCostBasis.' + currency].status + ' (ledger-replay)');
    console.log('  conciliacion: ' + projection.reconciliation.status);
  }

  console.log('');
  console.log('Listo.');
}

if (require.main === module) {
  main().then(() => process.exit(0)).catch((e) => {
    console.error('');
    console.error('ERROR:', e.message);
    process.exit(1);
  });
}

// HU 6.14.1: exportados para test
module.exports = {
  INFLOW_SPECS,
  MOTIVOS,
  SERIE_CORREGIDA_DESDE,
  costoDe,
  ultimoDomingo,
  esVeranoBritanico,
  diaSiguiente,
  decimalesDe,
  coincide,
  clasificarTasa,
  detectarCorridas,
  seleccionarPorId,
  planificarReemplazo,
  actualizacionDeBase,
  aplicarReemplazo,
};
