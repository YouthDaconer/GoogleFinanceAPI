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
 *
 * Uso:
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP --account ID
 *   node scripts/rebuildBalanceCostBasisByDate.js --email x@y.com --currency COP --apply
 *
 * Por defecto es **dry-run**: enseña la tabla de tasas y el costo resultante
 * sin escribir nada.
 *
 * @see services/helpers/balanceLedger.js
 * @see services/helpers/balanceCostBasis.js
 */

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
  console.log('  --help');
  console.log('');
}

function parseArgs(argv) {
  const args = { email: null, currency: null, account: null, apply: false, help: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--help' || a === '-h') args.help = true;
    else if (a === '--apply') args.apply = true;
    else if (a === '--email') args.email = argv[++i] || null;
    else if (a === '--currency') args.currency = (argv[++i] || '').toUpperCase() || null;
    else if (a === '--account') args.account = argv[++i] || null;
    else {
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

      const cost = Math.round(amount * resolved.rate * 100) / 100;
      console.log('    ' + day + '  ' + t.type.padEnd(15) + String(amount).padStart(12) + '   '
        + String(resolved.rate).padEnd(18) + '  ' + cost + ' ' + referenceCurrency);

      // La tasa va en el campo que su tipo de movimiento lee, y el costo en
      // `acquisitionCost`, que es de donde el replay saca el `costDelta`.
      const data = {
        acquisitionCost: cost,
        referenceCurrency,
      };
      data[spec.rateField] = resolved.rate;
      data[spec.rateField === 'realizationRate' ? 'realizationRateSource' : 'acquisitionRateSource'] = 'market-date';

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

    const replayed = projection.replayedCostBasis;
    const update = {};
    update['balanceCostBasis.' + currency] = {
      cost: replayed && replayed.cost !== undefined ? replayed.cost : null,
      referenceCurrency,
      status: (replayed && replayed.status) || 'unknown',
      source: 'ledger-replay',
      // Ninguna de estas entradas compro divisa: no realizan al salir.
      convertedAmount: 0,
      convertedCost: 0,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    update['balanceReconciliation.' + currency] = {
      ledgerBalance: projection.reconciliation.ledgerBalance,
      difference: projection.reconciliation.difference,
      status: projection.reconciliation.status,
      checkedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await accountDoc.ref.update(update);

    console.log('  base nueva: cost=' + update['balanceCostBasis.' + currency].cost
      + ' status=' + update['balanceCostBasis.' + currency].status + ' (ledger-replay)');
    console.log('  conciliacion: ' + projection.reconciliation.status);
  }

  console.log('');
  console.log('Listo.');
}

main().then(() => process.exit(0)).catch((e) => {
  console.error('');
  console.error('ERROR:', e.message);
  process.exit(1);
});
