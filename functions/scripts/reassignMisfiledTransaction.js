#!/usr/bin/env node

/**
 * CLI Script — HU 4.4: devolver un movimiento a la cuenta de su activo.
 *
 * **El problema que cierra.** Una venta de 0,3 NVDA (2025-09-02) quedó guardada
 * con la cuenta de un inversor, pero su activo y su compra son de la cuenta de
 * otro. En la historia del primero deja las unidades en negativo y fusiona dos
 * inversiones; al segundo le falta el movimiento que explica por qué su activo
 * quedó en cero.
 *
 * **Lo que hace.** Un movimiento pertenece a la cuenta de su activo (RN-4.4-A):
 * se le cambian `portfolioAccountId` y `userId` a los de esa cuenta, y nada más.
 * El saldo de la cuenta origen cuadraba gracias a ese movimiento, así que se
 * compensa con **un** ajuste de saldo fechado el día del movimiento, con un motivo
 * que lo explica (RN-4.4-B). El importe no se escribe a mano: es el impacto en
 * caja del propio movimiento.
 *
 * **Qué NO toca.** Ningún `balances.*` almacenado (el efectivo que el usuario ve
 * no cambia), ningún documento de rendimiento ni marca de recálculo (RN-4.4-C).
 * Solo el movimiento, el ajuste nuevo y el veredicto de conciliación de las dos
 * cuentas.
 *
 * Por defecto es **modo prueba**: enseña el plan sin escribir. Con `--apply`,
 * primero guarda la constancia (valores anteriores y nuevos, que es también como
 * se deshace) y después escribe en un solo lote. Relanzado sobre una corrección
 * ya aplicada, lo detecta y no escribe nada.
 *
 * Uso:
 *   node scripts/reassignMisfiledTransaction.js --tx <id> --to-account <id>
 *   node scripts/reassignMisfiledTransaction.js --tx <id> --to-account <id> --apply
 *
 * @see services/helpers/balanceAdjustment.js
 * @see services/helpers/balanceLedger.js
 * @see platform-docs/stories/4.4-coherencia-historia-activo/refinamiento.md (T1)
 */

const fs = require('fs');
const path = require('path');
const { DateTime } = require('luxon');
const admin = require('../services/firebaseAdmin');
const {
  buildAdjustment,
  resolveAdjustmentRate,
  ADJUSTMENT_REASONS,
} = require('../services/helpers/balanceAdjustment');
const { projectBalanceLedger, resolveCashImpact } = require('../services/helpers/balanceLedger');
const { getUserReferenceCurrency } = require('../services/helpers/balanceCostBasis');
const { resolveBasisValuation } = require('../services/helpers/basisValuation');

/** Carpeta de la historia: ahí va la constancia si no se dice otra cosa */
const CARPETA_HISTORIA = path.join(
  __dirname, '..', '..', '..', '..', 'platform-docs', 'stories', '4.4-coherencia-historia-activo',
);

/** Estados del plan */
const PLAN_STATUS = {
  READY: 'READY',
  ALREADY_APPLIED: 'ALREADY_APPLIED',
  BLOCKED: 'BLOCKED',
};

/** Zona horaria con la que el producto fecha los movimientos (estándar CF §4 regla 10) */
const ZONA_MERCADO = 'America/New_York';

/** Id provisional del ajuste dentro de la proyección, antes de existir en Firestore */
const ID_AJUSTE_PROYECTADO = '__ajuste-reasignacion__';

// ============================================================================
// PLAN (puro)
// ============================================================================

/**
 * Día del movimiento en la zona del mercado. Cortar la fecha ISO en UTC daría
 * el día siguiente para un movimiento hecho después de las 20:00 de Nueva York.
 *
 * @param {string} date - Fecha del movimiento (ISO con hora, o `YYYY-MM-DD`)
 * @returns {string} `YYYY-MM-DD`
 */
function diaDelMovimiento(date) {
  const texto = String(date);
  if (!texto.includes('T')) return texto.slice(0, 10);
  return DateTime.fromISO(texto, { zone: 'utc' }).setZone(ZONA_MERCADO).toISODate();
}

/**
 * Concilia un saldo con su libro mayor.
 *
 * @param {Array<Object>} transactions - Movimientos de la cuenta
 * @param {Object} account - Documento de la cuenta
 * @param {string} currency - Divisa del saldo
 * @param {string} referenceCurrency - Moneda de referencia del dueño
 * @returns {{ledgerBalance: number, difference: number, status: string}}
 */
function conciliar(transactions, account, currency, referenceCurrency) {
  const { reconciliation } = projectBalanceLedger({
    transactions,
    currency,
    referenceCurrency,
    balance: account.balances?.[currency] || 0,
  });
  return {
    ledgerBalance: reconciliation.ledgerBalance,
    difference: reconciliation.difference,
    status: reconciliation.status,
  };
}

/**
 * Decide qué hay que escribir para devolver un movimiento a la cuenta de su
 * activo. No lee ni escribe nada: todo llega por parámetro.
 *
 * @param {Object} params
 * @param {string} params.txId - Id del movimiento
 * @param {Object|null} params.tx - Documento del movimiento
 * @param {Object|null} params.asset - Documento del activo al que apunta
 * @param {{id: string, data: Object|null}} params.source - Cuenta donde está hoy
 * @param {{id: string, data: Object|null}} params.target - Cuenta de su activo
 * @param {Array<Object>} params.sourceTransactions - Movimientos de la cuenta origen (con `id`)
 * @param {Array<Object>} params.targetTransactions - Movimientos de la cuenta destino (con `id`)
 * @param {string} params.sourceReferenceCurrency - Referencia del dueño del origen
 * @param {string} params.targetReferenceCurrency - Referencia del dueño del destino
 * @param {Object} [params.adjustmentRate] - Resultado de `resolveAdjustmentRate`
 * @param {Object|null} [params.basisValuation] - Resultado de `resolveBasisValuation`
 * @returns {Object} `{ status, reasons, ... }`
 */
function buildReassignmentPlan({
  txId,
  tx,
  asset,
  source,
  target,
  sourceTransactions,
  targetTransactions,
  sourceReferenceCurrency,
  targetReferenceCurrency,
  adjustmentRate = null,
  basisValuation = null,
}) {
  const blocked = (reason) => ({ status: PLAN_STATUS.BLOCKED, reasons: [reason] });

  if (!tx) return blocked(`El movimiento ${txId} no existe`);

  const currency = tx.currency;
  const existingAdjustment = (sourceTransactions || [])
    .find((t) => t.misfiledTransactionId === txId) || null;
  const alreadyMoved = tx.portfolioAccountId === target.id;

  // Relanzar no duplica: con el movimiento ya en su cuenta y el ajuste escrito,
  // no queda nada que hacer (Escenario 2).
  if (alreadyMoved && existingAdjustment) {
    return {
      status: PLAN_STATUS.ALREADY_APPLIED,
      reasons: [`El movimiento ya está en ${target.id} y el ajuste ${existingAdjustment.id} existe`],
    };
  }
  if (alreadyMoved || existingAdjustment) {
    return blocked('Corrección a medias: el movimiento y el ajuste no están en el estado esperado; revisar a mano');
  }

  if (!source.data) return blocked(`La cuenta origen ${source.id} no existe`);
  if (!target.data) return blocked(`La cuenta destino ${target.id} no existe`);
  if (source.id === target.id) return blocked('La cuenta origen y la destino son la misma');
  if (tx.portfolioAccountId !== source.id) {
    return blocked(`El movimiento no está en la cuenta origen ${source.id}`);
  }
  if (!asset) return blocked(`El activo ${tx.assetId} del movimiento no existe`);
  if (asset.portfolioAccount !== target.id) {
    return blocked(`El activo ${tx.assetId} no está en la cuenta destino ${target.id}`);
  }
  if (!source.data.userId) return blocked('La cuenta origen no tiene dueño');
  if (!target.data.userId) return blocked('La cuenta destino no tiene dueño');

  // Si el origen ya no cuadra, la compensación no tendría contra qué medirse.
  const sourceBefore = conciliar(sourceTransactions, source.data, currency, sourceReferenceCurrency);
  if (sourceBefore.status !== 'reconciled') {
    return blocked(`La cuenta origen no cuadra antes de tocarla (diferencia ${sourceBefore.difference} ${currency})`);
  }
  const targetBefore = conciliar(targetTransactions, target.data, currency, targetReferenceCurrency);

  const impact = resolveCashImpact(tx, currency);
  if (!impact || !Number.isFinite(impact.amount) || impact.amount === 0) {
    return blocked('El movimiento no mueve caja: no hay nada que compensar');
  }

  const adjustment = buildAdjustment({
    account: source.data,
    accountId: source.id,
    userId: source.data.userId,
    currency,
    delta: impact.amount,
    date: tx.date,
    referenceCurrency: sourceReferenceCurrency,
    adjustmentReason: ADJUSTMENT_REASONS.MANUAL,
    // El usuario lo lee en el historial de su saldo: sin ids internos.
    description: `Corrección: compensa una ${tx.type === 'sell' ? 'venta' : 'operación'} de `
      + `${tx.amount} ${tx.assetName || ''} del ${diaDelMovimiento(tx.date)} que se había registrado `
      + 'en esta cuenta por error y pertenecía a otra.',
    acquisitionRate: adjustmentRate?.acquisitionRate ?? null,
    acquisitionRateSource: adjustmentRate?.acquisitionRateSource,
    dollarPriceToDate: adjustmentRate?.dollarPriceToDate ?? 1,
    basisValuation,
  });

  const adjustmentData = { ...adjustment.transactionData, misfiledTransactionId: txId };
  const movedTx = { ...tx, portfolioAccountId: target.id, userId: target.data.userId };

  const sourceAfter = conciliar(
    sourceTransactions.filter((t) => t.id !== txId).concat({ ...adjustmentData, id: ID_AJUSTE_PROYECTADO }),
    source.data,
    currency,
    sourceReferenceCurrency,
  );
  const targetAfter = conciliar(
    targetTransactions.concat({ ...movedTx, id: txId }),
    target.data,
    currency,
    targetReferenceCurrency,
  );

  if (sourceAfter.status !== 'reconciled') {
    return blocked(`Con el plan, la cuenta origen dejaría de cuadrar (diferencia ${sourceAfter.difference} ${currency})`);
  }

  return {
    status: PLAN_STATUS.READY,
    reasons: [],
    currency,
    delta: impact.amount,
    move: {
      txId,
      before: { portfolioAccountId: tx.portfolioAccountId, userId: tx.userId ?? null },
      after: { portfolioAccountId: target.id, userId: target.data.userId },
    },
    adjustment: adjustmentData,
    reconciliation: {
      source: { accountId: source.id, before: sourceBefore, after: sourceAfter },
      target: { accountId: target.id, before: targetBefore, after: targetAfter },
    },
  };
}

// ============================================================================
// ESCRITURA
// ============================================================================

/**
 * Escribe un plan `READY`: primero la constancia y después un solo lote. La
 * constancia va **antes** de tocar nada: si el lote falla, los valores
 * anteriores ya están a salvo para deshacer.
 *
 * @param {Object} params
 * @param {Object} params.db - Firestore
 * @param {Object} params.FieldValue - `admin.firestore.FieldValue`
 * @param {Object} params.plan - Resultado `READY` de `buildReassignmentPlan`
 * @param {Object} params.constancia - Lo que se guarda
 * @param {function(Object): void} params.escribirConstancia
 * @returns {Promise<{adjustmentId: string}>}
 */
async function applyReassignment({ db, FieldValue, plan, constancia, escribirConstancia }) {
  escribirConstancia(constancia);

  const batch = db.batch();
  const adjustmentRef = db.collection('transactions').doc();

  batch.update(db.collection('transactions').doc(plan.move.txId), plan.move.after);
  batch.set(adjustmentRef, plan.adjustment);

  // Solo el veredicto: el saldo guardado no se toca (RN-4.4-B).
  for (const { accountId, after } of [plan.reconciliation.source, plan.reconciliation.target]) {
    batch.update(db.collection('portfolioAccounts').doc(accountId), {
      [`balanceReconciliation.${plan.currency}`]: { ...after, checkedAt: FieldValue.serverTimestamp() },
    });
  }

  await batch.commit();
  return { adjustmentId: adjustmentRef.id };
}

// ============================================================================
// CLI
// ============================================================================

function printUsage() {
  console.log('');
  console.log('Devuelve un movimiento a la cuenta de su activo y compensa el saldo de origen (HU 4.4).');
  console.log('');
  console.log('  --tx <id>            Movimiento mal asignado');
  console.log('  --to-account <id>    Cuenta a la que pertenece su activo');
  console.log('  --apply              Escribe de verdad. Sin esto, solo informa.');
  console.log('  --salida <json>      Dónde guardar la constancia (por defecto, la carpeta de la HU)');
  console.log('  --help');
  console.log('');
}

function parseArgs(argv) {
  const args = { tx: null, toAccount: null, apply: false, salida: null, help: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--tx') args.tx = argv[++i];
    else if (a === '--to-account') args.toAccount = argv[++i];
    else if (a === '--salida') args.salida = argv[++i];
    else if (a === '--apply') args.apply = true;
    else if (a === '--help' || a === '-h') args.help = true;
  }
  return args;
}

async function leerMovimientos(db, accountId) {
  const snap = await db.collection('transactions').where('portfolioAccountId', '==', accountId).get();
  return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
}

async function leerCuenta(db, accountId) {
  const doc = await db.collection('portfolioAccounts').doc(accountId).get();
  return { id: accountId, data: doc.exists ? doc.data() : null };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.tx || !args.toAccount) {
    printUsage();
    process.exit(args.help ? 0 : 1);
  }

  const db = admin.firestore();
  const txDoc = await db.collection('transactions').doc(args.tx).get();
  const tx = txDoc.exists ? txDoc.data() : null;

  // Si ya se movió, el origen se reconoce por el ajuste que dejó la corrección.
  let sourceId = tx?.portfolioAccountId || null;
  if (tx && sourceId === args.toAccount) {
    const ajuste = await db.collection('transactions').where('misfiledTransactionId', '==', args.tx).limit(1).get();
    sourceId = ajuste.empty ? sourceId : ajuste.docs[0].data().portfolioAccountId;
  }

  const source = sourceId ? await leerCuenta(db, sourceId) : { id: null, data: null };
  const target = await leerCuenta(db, args.toAccount);
  const assetDoc = tx?.assetId ? await db.collection('assets').doc(tx.assetId).get() : null;
  const asset = assetDoc && assetDoc.exists ? assetDoc.data() : null;

  const sourceTransactions = source.data ? await leerMovimientos(db, source.id) : [];
  const targetTransactions = target.data ? await leerMovimientos(db, target.id) : [];
  const sourceReferenceCurrency = source.data ? await getUserReferenceCurrency(source.data.userId) : 'USD';
  const targetReferenceCurrency = target.data ? await getUserReferenceCurrency(target.data.userId) : 'USD';

  let adjustmentRate = null;
  let basisValuation = null;
  if (tx && source.data) {
    const impact = resolveCashImpact(tx, tx.currency);
    const fecha = diaDelMovimiento(tx.date);
    adjustmentRate = await resolveAdjustmentRate({
      currency: tx.currency, referenceCurrency: sourceReferenceCurrency, date: fecha,
    });
    basisValuation = impact ? await resolveBasisValuation({
      account: source.data,
      currency: tx.currency,
      amountDelta: impact.amount,
      costDelta: null,
      referenceCurrency: sourceReferenceCurrency,
      date: fecha,
    }) : null;
  }

  const plan = buildReassignmentPlan({
    txId: args.tx, tx, asset, source, target, sourceTransactions, targetTransactions,
    sourceReferenceCurrency, targetReferenceCurrency, adjustmentRate, basisValuation,
  });

  console.log('');
  console.log('='.repeat(70));
  console.log('  HU 4.4 — Devolver un movimiento a la cuenta de su activo');
  console.log('='.repeat(70));
  console.log(`  Movimiento : ${args.tx}${tx ? ` · ${tx.type} ${tx.amount} ${tx.assetName || ''} a ${tx.price} · ${tx.date}` : ''}`);
  console.log(`  Origen     : ${source.data?.name || '?'} (${source.id}) · dueño ${source.data?.userId || '?'} · ref ${sourceReferenceCurrency}`);
  console.log(`  Destino    : ${target.data?.name || '?'} (${target.id}) · dueño ${target.data?.userId || '?'} · ref ${targetReferenceCurrency}`);
  console.log(`  Modo       : ${args.apply ? 'APLICAR' : 'prueba (no escribe)'}`);
  console.log(`  Estado     : ${plan.status}`);
  plan.reasons.forEach((r) => console.log(`    - ${r}`));

  if (plan.status !== PLAN_STATUS.READY) {
    console.log('');
    console.log('No se escribe nada.');
    if (plan.status === PLAN_STATUS.BLOCKED) process.exitCode = 2;
    return;
  }

  const { source: rs, target: rt } = plan.reconciliation;
  console.log('');
  console.log(`  Mover el movimiento: ${JSON.stringify(plan.move.before)} -> ${JSON.stringify(plan.move.after)}`);
  console.log(`  Ajuste en origen   : ${plan.delta > 0 ? '+' : ''}${plan.delta} ${plan.currency} fechado ${plan.adjustment.date}`);
  console.log(`  Conciliación origen : ${rs.before.status} (dif ${rs.before.difference}) -> ${rs.after.status} (dif ${rs.after.difference})`);
  console.log(`  Conciliación destino: ${rt.before.status} (dif ${rt.before.difference}) -> ${rt.after.status} (dif ${rt.after.difference})`);
  console.log('  Saldos guardados    : sin cambios');

  if (!args.apply) {
    console.log('');
    console.log('Repite con --apply para escribir.');
    return;
  }

  const fecha = new Date().toISOString();
  const archivo = args.salida
    || path.join(CARPETA_HISTORIA, `constancia-reasignacion-${fecha.replace(/[:.]/g, '-')}.json`);

  const { adjustmentId } = await applyReassignment({
    db,
    FieldValue: admin.firestore.FieldValue,
    plan,
    constancia: {
      historia: '4.4-coherencia-historia-activo',
      fecha,
      movimiento: plan.move,
      ajuste: { ...plan.adjustment, createdAt: '(serverTimestamp)' },
      conciliacion: plan.reconciliation,
      // Deshacer: devolver `move.before` al movimiento (borrando `userId` si era
      // null), borrar el ajuste con `misfiledTransactionId` y recalcular los dos
      // veredictos desde el historial de conciliación.
      comoDeshacer: 'Restaurar movimiento.before, borrar el ajuste con misfiledTransactionId y volver a conciliar',
    },
    escribirConstancia: (c) => fs.writeFileSync(archivo, JSON.stringify(c, null, 2) + '\n'),
  });

  console.log('');
  console.log(`Constancia: ${archivo}`);
  console.log(`Ajuste creado: ${adjustmentId}`);
}

if (require.main === module) {
  (async () => {
    try {
      await main();
      process.exit(process.exitCode || 0);
    } catch (e) {
      console.error('');
      console.error('[reassignMisfiledTransaction] ERROR:', e.message);
      process.exit(1);
    }
  })();
}

// Exportados para test
module.exports = {
  buildReassignmentPlan,
  diaDelMovimiento,
  applyReassignment,
  PLAN_STATUS,
};
