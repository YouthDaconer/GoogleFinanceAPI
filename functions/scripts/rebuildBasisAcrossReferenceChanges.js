#!/usr/bin/env node

/**
 * CLI Script — HU 2.8: rehacer la base de un saldo que pasó por un cambio de
 * moneda de referencia.
 *
 * **El problema que cierra.** Antes de la HU 2.8, mientras el usuario usaba
 * como referencia la propia divisa de un saldo (por ejemplo, dólares para un
 * saldo en dólares), la base de ese saldo en la referencia anterior (pesos) se
 * quedaba congelada. Al volver a pesos, una base vieja describía un saldo que ya
 * era otro: 691.456 COP sobre 0,53 USD, 1.297.036 COP por dólar.
 *
 * **Lo que hace.** Reproduce cada saldo con las reglas de la HU 2.8, el mismo
 * `buildBalanceUpdate` que hoy usa la escritura, **en el orden en que se
 * registraron los movimientos**, que es el que vio la escritura en vivo. Por el
 * camino:
 * - lo declarado entra tal cual (RN-2.8-C);
 * - las salidas reducen la base en proporción (RN-2.8-B);
 * - lo que nadie declaró en la moneda de la base se valora al cierre de su día
 *   (RN-2.8-D, RN-2.8-E), y esa valoración se escribe en el propio movimiento
 *   (`basisValuation`), igual que la escribe hoy la escritura, para que el pasado
 *   no cambie (RN-3-B).
 *
 * **Qué toca.** Solo `balanceCostBasis.{DIVISA}` de la cuenta y el campo
 * `basisValuation` de los movimientos que la necesitaron. Ni saldos, ni montos,
 * ni tasas declaradas.
 *
 * **Movimientos sin `referenceCurrency`** (anteriores a la HU 2.1): no dicen con
 * qué referencia se registraron. `--ref-legado <ISO>` la declara; sin ella se
 * toman en la referencia de hoy, como hace el libro mayor.
 *
 * Por defecto es **modo prueba**: enseña el antes y el después sin escribir.
 * Con `--apply`, primero guarda la constancia (valores anteriores y nuevos, que es
 * también como se deshace, RN-6-H) y después escribe. Si no hay nada que
 * cambiar, no escribe ni deja constancia (RN-2.8-H).
 *
 * Uso:
 *   node scripts/rebuildBasisAcrossReferenceChanges.js --email x@y.com --ref-legado USD
 *   node scripts/rebuildBasisAcrossReferenceChanges.js --email x@y.com --ref-legado USD --account ID --currency USD
 *   node scripts/rebuildBasisAcrossReferenceChanges.js --email x@y.com --ref-legado USD --apply
 *   node scripts/rebuildBasisAcrossReferenceChanges.js --detectar
 *
 * Necesita el token del servicio de tasas en `CF_SERVICE_TOKEN` (Secret
 * Manager), nunca escrito en el repositorio.
 *
 * @see services/helpers/balanceCostBasis.js
 * @see services/helpers/basisValuation.js
 * @see platform-docs/stories/2.8-base-costo-cambio-referencia/refinamiento.md (T5)
 */

const fs = require('fs');
const path = require('path');
const admin = require('../services/firebaseAdmin');
const {
  basisValuationNeed,
  buildBalanceUpdate,
  getUserReferenceCurrency,
  isUsableRate,
} = require('../services/helpers/balanceCostBasis');
const { projectBalanceLedger, resolveCashImpact } = require('../services/helpers/balanceLedger');
const { resolveRealizationRate } = require('../services/helpers/realizedFxDecomposition');
const { BASIS_VALUATION_SOURCES } = require('../services/helpers/basisValuation');

/** Carpeta de la historia: ahí va la constancia si no se dice otra cosa */
const CARPETA_HISTORIA = path.join(
  __dirname, '..', '..', '..', '..', 'platform-docs', 'stories', '2.8-base-costo-cambio-referencia',
);

/** Diferencia de costo por debajo de la cual dos bases se consideran iguales */
const TOLERANCIA_COSTO = 0.01;

/** Máximo de operaciones por lote de Firestore */
const MAX_OPERACIONES_LOTE = 500;

// ============================================================================
// ORDEN DE REGISTRO
// ============================================================================

/** @param {*} createdAt @returns {number} Milisegundos, 0 si no se sabe */
function millisDe(createdAt) {
  if (!createdAt) return 0;
  if (typeof createdAt.toMillis === 'function') return createdAt.toMillis();
  if (typeof createdAt._seconds === 'number') return createdAt._seconds * 1000;
  if (createdAt instanceof Date) return createdAt.getTime();
  return 0;
}

/**
 * Clave de orden por **registro**: primero cuándo se creó el movimiento y, a
 * igualdad, su fecha, su lote y su id. Es el orden en que la escritura en vivo
 * movió el saldo; un movimiento retroactivo entra cuando se registró, no en la
 * fecha que dice.
 *
 * @param {Object} transaction
 * @returns {string}
 */
function ordenDeRegistro(transaction) {
  const lot = typeof transaction.lotIndex === 'number' ? transaction.lotIndex : 0;
  return [
    String(millisDe(transaction.createdAt)).padStart(16, '0'),
    String(transaction.date || '').padEnd(30, '0'),
    String(lot).padStart(6, '0'),
    String(transaction.id || ''),
  ].join('|');
}

// ============================================================================
// PLAN (puro salvo la consulta de tasas, que se inyecta)
// ============================================================================

/**
 * Recorre el saldo en orden de registro y descubre qué movimientos necesitan una
 * valoración que no tienen. Usa exactamente la regla de la escritura
 * (`basisValuationNeed` + `buildBalanceUpdate`).
 *
 * @param {Object} params
 * @param {Array<Object>} params.transactions - Movimientos de la cuenta, con `id`
 * @param {string} params.currency - Divisa del saldo
 * @param {string} params.referenceCurrency - Referencia de hoy (la de los movimientos que no la traen sin `legacy`)
 * @param {string|null} params.legacyReferenceCurrency - Referencia declarada de los movimientos sin ella
 * @param {function(string, string, string): Promise<Object|null>} params.obtenerTasa -
 *   `(divisa, monedaBase, fecha) → { rate, rateDate } | null`
 * @returns {Promise<{transacciones: Array<Object>, valoraciones: Array<Object>}>}
 *   Los movimientos con sus valoraciones y la lista de las que se añadieron
 */
async function completarValoraciones({
  transactions,
  currency,
  referenceCurrency,
  legacyReferenceCurrency,
  obtenerTasa,
}) {
  const ordenados = transactions
    .map((transaction) => ({ transaction, impact: resolveCashImpact(transaction, currency) }))
    .filter((m) => m.impact)
    .sort((a, b) => (ordenDeRegistro(a.transaction) < ordenDeRegistro(b.transaction) ? -1 : 1));

  const porId = new Map(transactions.map((t) => [t.id, t]));
  const valoraciones = [];
  let state = { balances: { [currency]: 0 }, balanceCostBasis: {} };

  for (const { transaction, impact } of ordenados) {
    const movementReference = transaction.referenceCurrency || legacyReferenceCurrency || referenceCurrency;
    const costDelta = impact.amount > 0 ? impact.costDelta : null;

    const objetivo = basisValuationNeed({
      account: state,
      currency,
      amountDelta: impact.amount,
      costDelta,
      referenceCurrency: movementReference,
    });

    let basisValuation = transaction.basisValuation || null;

    // Una valoración que se guardó sin tasa (el canal no respondió ese día) se
    // vuelve a intentar: lo que estaba era la ausencia, no un número.
    const faltaValoracion = !basisValuation
      || basisValuation.referenceCurrency !== objetivo
      || !isUsableRate(basisValuation.rate);

    if (objetivo && faltaValoracion) {
      const fecha = String(transaction.date || '').substring(0, 10);
      const tasa = await obtenerTasa(currency, objetivo, fecha);
      basisValuation = tasa && isUsableRate(tasa.rate)
        ? {
          referenceCurrency: objetivo,
          rate: tasa.rate,
          rateDate: tasa.rateDate || fecha,
          source: BASIS_VALUATION_SOURCES.MARKET_DATE,
        }
        : {
          referenceCurrency: objetivo,
          rate: null,
          rateDate: null,
          source: BASIS_VALUATION_SOURCES.UNAVAILABLE,
        };

      porId.set(transaction.id, { ...transaction, basisValuation });
      valoraciones.push({
        id: transaction.id,
        fecha,
        tipo: transaction.type,
        monto: impact.amount,
        antes: transaction.basisValuation || null,
        despues: basisValuation,
      });
    }

    const update = buildBalanceUpdate({
      account: state,
      currency,
      amountDelta: impact.amount,
      costDelta,
      referenceCurrency: movementReference,
      basisValuation,
    });

    state = {
      balances: { [currency]: update[`balances.${currency}`] },
      balanceCostBasis: update[`balanceCostBasis.${currency}`] !== undefined
        ? { [currency]: update[`balanceCostBasis.${currency}`] }
        : state.balanceCostBasis,
    };
  }

  return { transacciones: [...porId.values()], valoraciones };
}

/** Base sin el centinela de fecha de Firestore, para comparar y guardar */
function baseLimpia(basis) {
  if (!basis) return null;
  const { updatedAt, ...resto } = basis;
  return resto;
}

/**
 * ¿Describen dos bases lo mismo?
 *
 * @param {Object|null} a
 * @param {Object|null} b
 * @returns {boolean}
 */
function mismaBase(a, b) {
  if (!a || !b) return !a && !b;
  if (a.status !== b.status || a.referenceCurrency !== b.referenceCurrency) return false;
  if (a.cost === null || b.cost === null) return a.cost === b.cost;
  return Math.abs(Number(a.cost) - Number(b.cost)) < TOLERANCIA_COSTO;
}

/**
 * Plan de corrección de un saldo: la base que resulta de las reglas de la HU 2.8
 * y las valoraciones que hay que escribir para que el historial la reproduzca.
 *
 * @param {Object} params - Los de `completarValoraciones`, más:
 * @param {number} params.storedBalance - Saldo guardado hoy
 * @param {Object|null} params.storedBasis - Base guardada hoy
 * @returns {Promise<Object>} Plan con `cambia`, `baseAntes`, `baseDespues`, `valoraciones`
 */
async function planificarCorreccion(params) {
  const { transacciones, valoraciones } = await completarValoraciones(params);

  // El número final lo da el mismo replay que usa todo el producto, con el orden
  // de registro: así el plan no tiene una lógica propia que pueda divergir.
  const proyeccion = projectBalanceLedger({
    transactions: transacciones,
    currency: params.currency,
    referenceCurrency: params.referenceCurrency,
    balance: params.storedBalance,
    legacyReferenceCurrency: params.legacyReferenceCurrency,
    orderBy: ordenDeRegistro,
  });

  const baseDespues = baseLimpia(proyeccion.replayedCostBasis);
  const baseAntes = baseLimpia(params.storedBasis);

  return {
    currency: params.currency,
    saldoGuardado: params.storedBalance,
    saldoReproducido: proyeccion.reconciliation.ledgerBalance,
    baseAntes,
    baseDespues,
    valoraciones,
    cambia: !mismaBase(baseAntes, baseDespues) || valoraciones.length > 0,
  };
}

/**
 * Tasa promedio legible de una base, para el informe.
 *
 * @param {Object|null} basis
 * @param {number} balance
 * @returns {string}
 */
function tasaLegible(basis, balance) {
  if (!basis || basis.status !== 'known' || !balance || typeof basis.cost !== 'number') return 'no disponible';
  const tasa = basis.cost / balance;
  return tasa < 1
    ? (1 / tasa).toFixed(2) + ' (invertida)'
    : tasa.toFixed(2);
}

// ============================================================================
// ESCRITURA
// ============================================================================

/**
 * Escribe un plan: primero la constancia y después los lotes (≤ 500
 * operaciones). La constancia va **antes** de tocar nada: si un lote falla, los
 * valores anteriores ya están a salvo para deshacer (RN-6-H).
 *
 * @param {Object} params
 * @param {Object} params.db - Firestore
 * @param {Object} params.FieldValue - `admin.firestore.FieldValue`
 * @param {Array<{accountId: string, plan: Object}>} params.cuentas - Solo las que cambian
 * @param {Object} params.constancia - Lo que se guarda
 * @param {function(Object): void} params.escribirConstancia
 * @returns {Promise<{escrituras: number}>}
 */
async function aplicarCorreccion({ db, FieldValue, cuentas, constancia, escribirConstancia }) {
  escribirConstancia(constancia);

  const operaciones = [];
  for (const { accountId, plan } of cuentas) {
    for (const v of plan.valoraciones) {
      operaciones.push([db.collection('transactions').doc(v.id), { basisValuation: v.despues }]);
    }
    operaciones.push([db.collection('portfolioAccounts').doc(accountId), {
      [`balanceCostBasis.${plan.currency}`]: {
        ...plan.baseDespues,
        source: 'reference-change-rebuild',
        updatedAt: FieldValue.serverTimestamp(),
      },
    }]);
  }

  let escrituras = 0;
  for (let i = 0; i < operaciones.length; i += MAX_OPERACIONES_LOTE) {
    const batch = db.batch();
    for (const [ref, datos] of operaciones.slice(i, i + MAX_OPERACIONES_LOTE)) {
      batch.update(ref, datos);
    }
    await batch.commit();
    escrituras += Math.min(MAX_OPERACIONES_LOTE, operaciones.length - i);
  }

  return { escrituras };
}

// ============================================================================
// DETECCIÓN
// ============================================================================

/**
 * ¿Pasó este saldo por más de una moneda de referencia? Solo así puede estar
 * afectado por el defecto.
 *
 * @param {Array<Object>} transactions - Movimientos de la cuenta
 * @param {string} currency - Divisa del saldo
 * @returns {boolean}
 */
function pasoPorVariasReferencias(transactions, currency) {
  const referencias = new Set();
  for (const t of transactions) {
    if (!resolveCashImpact(t, currency)) continue;
    if (t.referenceCurrency) referencias.add(t.referenceCurrency);
  }
  return referencias.size > 1;
}

// ============================================================================
// CLI
// ============================================================================

function printUsage() {
  console.log('');
  console.log('Rehace la base de costo de los saldos que pasaron por un cambio de moneda de referencia (HU 2.8).');
  console.log('');
  console.log('  --email <correo>     Usuario');
  console.log('  --account <id>       Limitar a una cuenta');
  console.log('  --currency <ISO>     Limitar a una divisa');
  console.log('  --ref-legado <ISO>   Referencia de los movimientos que no la registran');
  console.log('  --apply              Escribe de verdad. Sin esto, solo informa.');
  console.log('  --salida <json>      Dónde guardar la constancia (por defecto, la carpeta de la HU)');
  console.log('  --detectar           Cuenta los saldos de todos los inversores que pasaron por más de una referencia (no escribe)');
  console.log('  --help');
  console.log('');
}

function parseArgs(argv) {
  const args = {
    email: null, account: null, currency: null, refLegado: null,
    apply: false, salida: null, detectar: false, help: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--email') args.email = argv[++i];
    else if (a === '--account') args.account = argv[++i];
    else if (a === '--currency') args.currency = argv[++i];
    else if (a === '--ref-legado') args.refLegado = argv[++i];
    else if (a === '--salida') args.salida = argv[++i];
    else if (a === '--apply') args.apply = true;
    else if (a === '--detectar') args.detectar = true;
    else if (a === '--help' || a === '-h') args.help = true;
  }
  return args;
}

/** Tasa del cierre del día por el mismo camino que la escritura (HU 2.4) */
async function obtenerTasaDeMercado(currency, monedaBase, fecha) {
  const r = await resolveRealizationRate(currency, monedaBase, fecha);
  return r && typeof r.realizationRate === 'number'
    ? { rate: r.realizationRate, rateDate: r.realizationRateDate }
    : null;
}

async function leerMovimientos(db, accountId) {
  const snap = await db.collection('transactions').where('portfolioAccountId', '==', accountId).get();
  return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
}

async function mainDetectar(db) {
  const cuentas = await db.collection('portfolioAccounts').get();
  const afectados = new Map();

  for (const doc of cuentas.docs) {
    const account = doc.data();
    const divisas = Object.keys(account.balances || {});
    if (divisas.length === 0) continue;

    const movimientos = await leerMovimientos(db, doc.id);
    for (const currency of divisas) {
      if (pasoPorVariasReferencias(movimientos, currency)) {
        const lista = afectados.get(account.userId) || [];
        lista.push(`${doc.id}/${currency}`);
        afectados.set(account.userId, lista);
      }
    }
  }

  console.log('');
  console.log(`Inversores con algún saldo que pasó por más de una referencia: ${afectados.size}`);
  for (const [userId, saldos] of afectados) {
    console.log(`  ${userId}: ${saldos.join(', ')}`);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || (!args.email && !args.detectar)) {
    printUsage();
    process.exit(args.help ? 0 : 1);
  }

  const db = admin.firestore();

  if (args.detectar) {
    await mainDetectar(db);
    return;
  }

  const userRecord = await admin.auth().getUserByEmail(args.email);
  const userId = userRecord.uid;
  const referenceCurrency = await getUserReferenceCurrency(userId);

  console.log('');
  console.log('='.repeat(70));
  console.log('  HU 2.8 — Base de costo a través de cambios de moneda de referencia');
  console.log('='.repeat(70));
  console.log(`  Usuario    : ${userRecord.email} (${userId})`);
  console.log(`  Referencia : ${referenceCurrency}   Legado: ${args.refLegado || '(la de hoy)'}`);
  console.log(`  Modo       : ${args.apply ? 'APLICAR' : 'prueba (no escribe)'}`);

  const accountsSnap = await db.collection('portfolioAccounts').where('userId', '==', userId).get();
  const planes = [];

  for (const accountDoc of accountsSnap.docs) {
    if (args.account && accountDoc.id !== args.account) continue;
    const account = accountDoc.data();
    const movimientos = await leerMovimientos(db, accountDoc.id);

    for (const currency of Object.keys(account.balances || {})) {
      if (args.currency && currency !== args.currency) continue;
      if (!pasoPorVariasReferencias(movimientos, currency)) continue;

      const plan = await planificarCorreccion({
        transactions: movimientos,
        currency,
        referenceCurrency,
        legacyReferenceCurrency: args.refLegado,
        obtenerTasa: obtenerTasaDeMercado,
        storedBalance: account.balances[currency],
        storedBasis: account.balanceCostBasis ? account.balanceCostBasis[currency] : null,
      });

      console.log('');
      console.log(`  ${account.name || accountDoc.id} · ${currency}`);
      console.log(`    saldo guardado ${plan.saldoGuardado}   reproducido ${plan.saldoReproducido}`);
      console.log(`    base antes  : ${JSON.stringify(plan.baseAntes)}   tasa ${tasaLegible(plan.baseAntes, plan.saldoGuardado)}`);
      console.log(`    base después: ${JSON.stringify(plan.baseDespues)}   tasa ${tasaLegible(plan.baseDespues, plan.saldoGuardado)}`);
      console.log(`    valoraciones a escribir: ${plan.valoraciones.length}`);
      for (const v of plan.valoraciones) {
        console.log(`      ${v.fecha} ${v.tipo.padEnd(16)} ${String(v.monto).padStart(14)}  ${v.despues.referenceCurrency} ${v.despues.rate ?? 'sin tasa'}`);
      }

      if (plan.cambia) planes.push({ accountId: accountDoc.id, accountName: account.name || null, plan });
    }
  }

  console.log('');
  if (planes.length === 0) {
    console.log('Nada que cambiar: no se escribe ni se guarda constancia.');
    return;
  }

  if (!args.apply) {
    console.log(`${planes.length} saldo(s) cambiarían. Repite con --apply para escribir.`);
    return;
  }

  const fecha = new Date().toISOString();
  const archivo = args.salida
    || path.join(CARPETA_HISTORIA, `constancia-base-${fecha.replace(/[:.]/g, '-')}.json`);

  const constancia = {
    historia: '2.8-base-costo-cambio-referencia',
    fecha,
    userId,
    referenciaDeHoy: referenceCurrency,
    referenciaLegado: args.refLegado,
    cuentas: planes.map(({ accountId, accountName, plan }) => ({ accountId, accountName, ...plan })),
  };

  const { escrituras } = await aplicarCorreccion({
    db,
    FieldValue: admin.firestore.FieldValue,
    cuentas: planes,
    constancia,
    escribirConstancia: (c) => fs.writeFileSync(archivo, JSON.stringify(c, null, 2) + '\n'),
  });

  console.log(`Constancia: ${archivo}`);
  console.log(`Escrituras: ${escrituras}`);
}

if (require.main === module) {
  (async () => {
    try {
      await main();
      process.exit(0);
    } catch (e) {
      console.error('');
      console.error('ERROR:', e.message);
      process.exit(1);
    }
  })();
}

// Exportados para test
module.exports = {
  ordenDeRegistro,
  completarValoraciones,
  planificarCorreccion,
  aplicarCorreccion,
  pasoPorVariasReferencias,
  mismaBase,
  tasaLegible,
};
