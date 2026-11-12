#!/usr/bin/env node

/**
 * CLI Script — Recalcular los docs diarios de rendimiento de un usuario.
 *
 * Es la misma operación que hace la reconciliación nocturna con un marcador
 * `_stale` (`backfillUserPerformance` + `propagateReconciledRange`), pero para
 * un rango que se elige a mano y con una prueba en seco que enseña qué cambia
 * antes de escribir.
 *
 * **Para qué.** Corregir historia escrita antes de FLOW-WINDOW y LEDGER-LOTS:
 * días sin doc cuyos flujos se perdieron (2026-07-22), días con el cambio en 0
 * (2026-07-01), saltos de unidades en las fronteras entre el EOD y
 * reconstrucciones viejas (2026-01-02, 2026-04-07) y docs de fines de semana
 * escritos por procesos antiguos.
 *
 * **Qué escribe con --apply.** Los docs diarios del rango (cuentas activas y
 * portafolio, merge), borra los del rango que el recálculo no produce, rehace
 * los consolidados de meses y años cerrados, borra los snapshots del usuario
 * (se regeneran al leerse) e invalida su caché. Antes de escribir guarda el
 * respaldo `_backfillSnapshots/{uid}_{desde}` (recortado si es grande).
 *
 * Requisitos: credenciales de Firestore (key.json o ADC) y el token del API de
 * mercado en `CF_SERVICE_TOKEN` (sin él no hay precios y no se escribe nada).
 *
 * Uso:
 *   node scripts/recalcularRendimientoDiario.js --uid UID --from 2024-08-16
 *   node scripts/recalcularRendimientoDiario.js --uid UID --from 2024-08-16 --to 2026-11-10
 *   node scripts/recalcularRendimientoDiario.js --uid UID --from 2024-08-16 --apply
 *
 * Por defecto es **dry-run**.
 *
 * @see services/backfillCoreModule.js (backfillUserPerformance)
 * @see services/reconcileStalePerformance.js (propagateReconciledRange)
 */

const admin = require('../services/firebaseAdmin');
const { FieldValue } = require('firebase-admin/firestore');
const { generateTradingDays, backfillUserPerformance } = require('../services/backfillCoreModule');
const { propagateReconciledRange, getLastClosedDate } = require('../services/reconcileStalePerformance');

const db = admin.firestore();

function parseArgs(argv) {
  const args = { apply: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--apply') args.apply = true;
    else if (argv[i].startsWith('--')) args[argv[i].slice(2)] = argv[++i];
  }
  return args;
}

const adj = (doc) => Number(doc?.USD?.adjustedDailyChangePercentage || 0);
const pct = (g) => `${((g - 1) * 100).toFixed(2)} %`;

/** TWR encadenado de un ámbito: todos sus docs del rango, como los lee el frontend. */
function chain(docsByDate, from, to) {
  return Object.keys(docsByDate)
    .filter((d) => d >= from && d <= to)
    .sort()
    .reduce((g, d) => g * (1 + adj(docsByDate[d]) / 100), 1);
}

function summarize(result, from, to) {
  console.log(`\nCorrecciones libro↔lotes (${result.corrections.length}):`);
  result.corrections.forEach((c) => console.log(
    `  ${c.accountId} ${c.name}: lote ${c.lotUnits}, libro ${c.ledgerUnits.toFixed(4)} → ${c.gap > 0 ? '+' : ''}${c.gap.toFixed(4)} desde ${c.date}`));
  console.log(`Transacciones de lotes que ya no existen, ignoradas: ${result.orphans}`);
  console.log(`Docs del rango que el recálculo no produce (se borran): ${result.staleDocs.length}`);

  if (!result.plan) return;
  const after = { overall: {} };
  for (const day of result.plan) {
    after.overall[day.date] = day.overall;
    for (const [accountId, doc] of Object.entries(day.accounts)) (after[accountId] = after[accountId] || {})[day.date] = doc;
  }
  const before = { overall: result.existing.overall, ...result.existing.accounts };

  for (const scope of Object.keys({ ...before, ...after })) {
    const b = before[scope] || {};
    const a = after[scope] || {};
    const diffs = [...new Set([...Object.keys(b), ...Object.keys(a)])]
      .filter((d) => d >= from && d <= to)
      .map((d) => ({ d, b: b[d] ? adj(b[d]) : null, a: a[d] ? adj(a[d]) : null }))
      .filter((r) => r.b === null || r.a === null || Math.abs(r.a - r.b) > 0.25)
      .sort((x, y) => Math.abs((y.a || 0) - (y.b || 0)) - Math.abs((x.a || 0) - (x.b || 0)));
    console.log(`\n${scope}: TWR ${pct(chain(b, from, to))} → ${pct(chain(a, from, to))}; ${diffs.length} día(s) cambian más de 0,25 pp`);
    diffs.slice(0, 8).forEach((r) => console.log(
      `  ${r.d}: ${r.b === null ? 'sin doc' : `${r.b.toFixed(2)} %`} → ${r.a === null ? 'sin doc' : `${r.a.toFixed(2)} %`}`));
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.uid || !args.from) {
    console.error('Uso: --uid UID --from YYYY-MM-DD [--to YYYY-MM-DD] [--apply]');
    process.exit(1);
  }
  if (!process.env.CF_SERVICE_TOKEN && !process.env.PORTASTOCK_API_TOKEN) {
    console.error('Falta CF_SERVICE_TOKEN: sin token el API de precios responde 401 y no se puede recalcular.');
    process.exit(1);
  }

  const to = args.to || getLastClosedDate();
  const days = await generateTradingDays(args.from, to);
  console.log(`${args.uid}: ${days.length} días hábiles de ${days[0]} a ${days[days.length - 1]} — ${args.apply ? 'APLICAR' : 'en seco'}`);

  const dry = await backfillUserPerformance(args.uid, days, { dryRun: true });
  summarize(dry, days[0], days[days.length - 1]);
  if (!dry.success) {
    console.error(`\nNo se puede recalcular: ${dry.errors.slice(0, 5).join('; ')}`);
    process.exit(1);
  }
  if (!args.apply) {
    console.log('\nEn seco: no se escribió nada. Repite con --apply para escribir.');
    return;
  }

  const result = await backfillUserPerformance(args.uid, days);
  if (!result.success) {
    console.error(`\nFalló al escribir: ${result.errors.slice(0, 5).join('; ')}`);
    process.exit(1);
  }
  console.log(`\nEscritos ${result.daysProcessed} días; ${result.staleDocs.length} docs sobrantes borrados.`);

  const accountIds = (await db.collection('portfolioAccounts').where('userId', '==', args.uid).get()).docs.map((d) => d.id);
  await propagateReconciledRange(args.uid, days[0], accountIds);

  // Un marcador de reconciliación cubierto por este rango ya no tiene nada que hacer
  const ref = db.collection('portfolioPerformance').doc(args.uid);
  const stale = (await ref.get()).data()?._stale;
  if (stale && stale.since >= days[0]) {
    await ref.update({ _stale: FieldValue.delete() });
    console.log(`Marcador _stale (desde ${stale.since}) cubierto por el recálculo: borrado.`);
  }
  console.log('Listo.');
}

main().then(() => process.exit(0)).catch((error) => {
  console.error('FALLO:', error);
  process.exit(1);
});
