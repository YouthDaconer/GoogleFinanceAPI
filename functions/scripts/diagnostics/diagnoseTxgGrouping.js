/**
 * TXG — Diagnóstico de agrupación de ventas multi-lote.
 *
 * SOLO LECTURA. No escribe nada.
 *
 * Reproduce exactamente `resolveOperationKey` del frontend
 * (app/portfolio/transactions/utils/transactionGrouping.ts) sobre los datos reales
 * y muestra por qué un conjunto de ventas se agrupa o no.
 *
 * Uso:
 *   node scripts/diagnostics/diagnoseTxgGrouping.js <userId> [ticker] [YYYY-MM-DD]
 */

const admin = require('firebase-admin');
const serviceAccount = require('../../key.json');

if (!admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
}

const db = admin.firestore();

const userId = process.argv[2];
const tickerFilter = process.argv[3] || null;
const dayFilter = process.argv[4] || null;

if (!userId) {
  console.error('Uso: node diagnoseTxgGrouping.js <userId> [ticker] [YYYY-MM-DD]');
  process.exit(1);
}

/**
 * Réplica exacta de resolveOperationKey del frontend.
 * TXG-003: clave de granularidad de DÍA, sin prioridad de operationId.
 */
function resolveOperationKey(t) {
  return [
    'day',
    String(t.date).slice(0, 10),
    t.assetName || t.symbol || '',
    t.portfolioAccountId,
    t.price,
    t.currency,
  ].join('|');
}

/** Réplica de las guardas isGroupable del frontend. */
function guardReport(lots) {
  const reasons = [];
  if (lots.length < 2) reasons.push(`menos de 2 documentos (${lots.length})`);

  const currencies = new Set(lots.map((l) => l.currency));
  if (currencies.size > 1) reasons.push(`monedas distintas: ${[...currencies].join(', ')}`);

  // TXG-003: la guarda de assetId único se retiró; con clave por día un lote puede
  // consumirse en varias tandas legítimamente.
  return reasons;
}

async function main() {
  console.log(`\n=== TXG · Diagnóstico de agrupación ===`);
  console.log(`userId: ${userId}`);
  if (tickerFilter) console.log(`ticker: ${tickerFilter}`);
  if (dayFilter) console.log(`día:    ${dayFilter}`);

  // 1. Cuentas del usuario
  const accountsSnap = await db.collection('portfolioAccounts').where('userId', '==', userId).get();
  const accountIds = accountsSnap.docs.map((d) => d.id);
  const accountNames = new Map(accountsSnap.docs.map((d) => [d.id, d.data().name]));
  console.log(`\nCuentas (${accountIds.length}): ${accountIds.map((id) => `${accountNames.get(id)} [${id}]`).join(', ')}`);

  if (accountIds.length === 0) {
    console.log('Sin cuentas. Fin.');
    return;
  }

  // 2. Ventas del usuario
  const chunks = [];
  for (let i = 0; i < accountIds.length; i += 30) chunks.push(accountIds.slice(i, i + 30));

  let sells = [];
  for (const chunk of chunks) {
    const snap = await db
      .collection('transactions')
      .where('portfolioAccountId', 'in', chunk)
      .where('type', '==', 'sell')
      .get();
    sells = sells.concat(snap.docs.map((d) => ({ id: d.id, ...d.data() })));
  }

  if (tickerFilter) {
    sells = sells.filter((t) => (t.assetName || t.symbol) === tickerFilter);
  }
  if (dayFilter) {
    sells = sells.filter((t) => String(t.date).startsWith(dayFilter));
  }

  console.log(`Ventas encontradas: ${sells.length}`);
  if (sells.length === 0) return;

  // 3. Agrupar por clave de operación
  const buckets = new Map();
  for (const t of sells) {
    const key = resolveOperationKey(t);
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(t);
  }

  console.log(`Claves de operación distintas: ${buckets.size}`);
  console.log(`Con operationId: ${sells.filter((t) => t.operationId).length} / ${sells.length}`);

  // 4. Detalle por clave
  const sorted = [...buckets.entries()].sort((a, b) => String(a[1][0].date).localeCompare(String(b[1][0].date)));

  for (const [key, lots] of sorted) {
    const reasons = guardReport(lots);
    const status = lots.length < 2 ? 'FILA SIMPLE' : reasons.length === 0 ? '✅ AGRUPA' : '❌ NO AGRUPA';

    console.log(`\n${'─'.repeat(100)}`);
    console.log(`${status}  ·  ${lots.length} doc(s)`);
    console.log(`clave: ${key}`);
    if (reasons.length) console.log(`guardas incumplidas: ${reasons.join(' | ')}`);

    for (const l of lots) {
      const created = l.createdAt && l.createdAt.toDate ? l.createdAt.toDate().toISOString() : String(l.createdAt);
      console.log(
        `   doc=${l.id} assetId=${l.assetId ?? '(ninguno)'} amount=${l.amount} price=${l.price} ` +
          `pnl=${l.valuePnL} closed=${l.closedPnL} cur=${l.currency}`,
      );
      console.log(`     date=${l.date}  createdAt=${created}  operationId=${l.operationId ?? '(ninguno)'} lotIndex=${l.lotIndex ?? '(ninguno)'}`);
    }
  }

  // 5. Ventas que comparten día+ticker+cuenta+precio pero NO clave (el síntoma reportado)
  console.log(`\n${'═'.repeat(100)}`);
  console.log('ANÁLISIS: ventas que el usuario esperaría juntas pero tienen claves distintas');

  const loose = new Map();
  for (const t of sells) {
    const day = String(t.date).slice(0, 10);
    const k = [day, t.assetName || t.symbol, t.portfolioAccountId, t.price, t.currency].join('|');
    if (!loose.has(k)) loose.set(k, []);
    loose.get(k).push(t);
  }

  for (const [k, group] of loose) {
    const keys = new Set(group.map(resolveOperationKey));
    if (group.length > 1 && keys.size > 1) {
      console.log(`\n▶ ${k}`);
      console.log(`  ${group.length} ventas comparten día/ticker/cuenta/precio, pero producen ${keys.size} claves distintas.`);
      const byDate = new Map();
      for (const t of group) {
        if (!byDate.has(t.date)) byDate.set(t.date, []);
        byDate.get(t.date).push(t);
      }
      console.log(`  Marcas temporales distintas: ${byDate.size}`);
      for (const [d, ts] of byDate) {
        console.log(`    date=${d}  →  ${ts.length} doc(s)  assetIds=[${ts.map((t) => t.assetId).join(', ')}]`);
      }
    }
  }

  // 6. SIMULACIÓN de la clave por día + ticker + cuenta + precio (regla del PO, 08/08/2026)
  console.log(`\n${'═'.repeat(100)}`);
  console.log('SIMULACIÓN: clave por DÍA + ticker + cuenta + precio + moneda');

  const dayBuckets = new Map();
  for (const t of sells) {
    const k = [
      'day',
      String(t.date).slice(0, 10),
      t.assetName || t.symbol || '',
      t.portfolioAccountId,
      t.price,
      t.currency,
    ].join('|');
    if (!dayBuckets.has(k)) dayBuckets.set(k, []);
    dayBuckets.get(k).push(t);
  }

  let grouped = 0;
  let singles = 0;
  let blockedByAssetId = 0;
  let blockedByCurrency = 0;
  const collisions = [];

  for (const [k, lots] of dayBuckets) {
    if (lots.length < 2) {
      singles++;
      continue;
    }
    const ids = lots.map((l) => l.assetId ?? '');
    const dupIds = ids.filter((id, i) => ids.indexOf(id) !== i);
    const curs = new Set(lots.map((l) => l.currency));

    if (curs.size > 1) {
      blockedByCurrency++;
      continue;
    }
    grouped++;
    if (dupIds.length > 0) {
      blockedByAssetId++;
      collisions.push({ k, lots, dupIds: [...new Set(dupIds)] });
    }
  }

  console.log(`\nClaves totales: ${dayBuckets.size}`);
  console.log(`  → agrupan (>=2 docs, misma moneda): ${grouped}`);
  console.log(`  → filas simples:                    ${singles}`);
  console.log(`  → no agrupan por moneda distinta:   ${blockedByCurrency}`);
  console.log(`  → grupos con el mismo lote en varias tandas: ${blockedByAssetId} (legitimo desde TXG-003)`);

  if (collisions.length) {
    console.log(`\nDetalle de los grupos donde un lote se consumio en varias tandas:\n`);
    for (const c of collisions) {
      console.log(`▶ ${c.k}`);
      console.log(`  ${c.lots.length} docs · assetId repetido: ${c.dupIds.join(', ')}`);
      for (const l of c.lots) {
        console.log(
          `    doc=${l.id} assetId=${l.assetId} amount=${l.amount} pnl=${l.valuePnL} closed=${l.closedPnL} date=${l.date}`,
        );
      }
      console.log('');
    }
  }

  console.log('\n=== Fin (solo lectura, no se escribió nada) ===\n');
}

main().then(() => process.exit(0)).catch((e) => {
  console.error(e);
  process.exit(1);
});

