/**
 * snapshotCleanup.js
 * 
 * PERF-SNAP-028: Snapshot Lifecycle & Cleanup
 * 
 * Three cleanup vectors:
 *   Vector 1: Delete asset snapshots sold >365 days ago with no re-purchase (weekly)
 *   Vector 3: Archive snapshots for users inactive >365 days (monthly)
 * 
 * Vector 2 (account deletion) is handled in accountHandlers.js
 * 
 * @see docs/stories/PERF-SNAP-028.story.md
 * @see docs/architecture/AUDIT-SNAPSHOT-SUSTAINABILITY-PLAN.md — Section 6
 */

const { onSchedule } = require('firebase-functions/v2/scheduler');
const { getFirestore } = require('firebase-admin/firestore');
const admin = require('./firebaseAdmin');
const { DateTime } = require('luxon');

const db = getFirestore();

// ============================================================================
// CONSTANTS
// ============================================================================

const SOLD_ASSET_GRACE_DAYS = 365;
const INACTIVE_USER_DAYS = 365;
const QUERY_BATCH_SIZE = 100;
const FIRESTORE_BATCH_LIMIT = 500;

// ============================================================================
// VECTOR 1: CLEANUP SOLD ASSET SNAPSHOTS
// ============================================================================

/**
 * @param {FirebaseFirestore.Firestore} firestore
 * @returns {Promise<{scanned: number, deleted: number, skipped: number}>}
 */
async function cleanupSoldAssetSnapshots(firestore) {
  const cutoffDate = DateTime.now()
    .setZone('America/New_York')
    .minus({ days: SOLD_ASSET_GRACE_DAYS })
    .toISODate();

  let deleted = 0;
  let scanned = 0;
  let skipped = 0;
  let lastDoc = null;

  while (true) {
    let query = firestore.collection('performanceSnapshots')
      .where('type', '==', 'asset')
      .orderBy('lastUpdated', 'asc')
      .limit(QUERY_BATCH_SIZE);

    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();
    if (snapshot.empty) break;

    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    for (const doc of snapshot.docs) {
      scanned++;
      const data = doc.data();
      const timeline = data.timeline || [];

      if (timeline.length === 0) continue;

      const lastEntry = timeline[timeline.length - 1];

      if (lastEntry.u !== 0) {
        skipped++;
        continue;
      }

      const lastActiveEntry = [...timeline].reverse().find(e => e.u > 0);
      if (!lastActiveEntry || lastActiveEntry.d > cutoffDate) {
        skipped++;
        continue;
      }

      const recentBuys = await firestore.collection('transactions')
        .where('userId', '==', data.userId)
        .where('assetName', '==', data.ticker)
        .where('type', '==', 'buy')
        .where('date', '>=', lastActiveEntry.d)
        .limit(1)
        .get();

      if (!recentBuys.empty) {
        skipped++;
        continue;
      }

      await doc.ref.delete();
      deleted++;
      console.log(`[snapshotCleanup][cleanupSoldAssets] Deleted: ${doc.id} (last active: ${lastActiveEntry.d})`);
    }

    if (snapshot.docs.length < QUERY_BATCH_SIZE) break;
  }

  console.log(`[snapshotCleanup][cleanupSoldAssets] Complete: scanned=${scanned}, deleted=${deleted}, skipped=${skipped}`);
  return { scanned, deleted, skipped };
}

// ============================================================================
// VECTOR 3: ARCHIVE INACTIVE USER SNAPSHOTS
// ============================================================================

/**
 * @param {FirebaseFirestore.Firestore} firestore
 * @param {admin.auth.Auth} auth
 * @returns {Promise<{archived: number, errors: number}>}
 */
async function archiveInactiveUserSnapshots(firestore, auth) {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - INACTIVE_USER_DAYS);

  let archived = 0;
  let errors = 0;
  let pageToken;

  do {
    const listResult = await auth.listUsers(1000, pageToken);

    for (const user of listResult.users) {
      const lastSignIn = user.metadata.lastSignInTime
        ? new Date(user.metadata.lastSignInTime)
        : null;

      if (lastSignIn && lastSignIn >= cutoffDate) continue;

      const probe = await firestore.collection('performanceSnapshots')
        .where('userId', '==', user.uid)
        .limit(1)
        .get();

      if (probe.empty) continue;

      const userDataDoc = await firestore.doc(`userData/${user.uid}`).get();
      if (userDataDoc.exists && userDataDoc.data()?.snapshotsArchived) continue;

      try {
        await archiveUserSnapshots(firestore, user.uid);
        archived++;
      } catch (err) {
        errors++;
        console.error(`[snapshotCleanup][archiveInactiveUsers] Failed to archive ${user.uid}: ${err.message}`);
      }
    }

    pageToken = listResult.pageToken;
  } while (pageToken);

  console.log(`[snapshotCleanup][archiveInactiveUsers] Complete: archived=${archived}, errors=${errors}`);
  return { archived, errors };
}

/**
 * @param {FirebaseFirestore.Firestore} firestore
 * @param {string} userId
 */
async function archiveUserSnapshots(firestore, userId) {
  const snapshots = await firestore.collection('performanceSnapshots')
    .where('userId', '==', userId)
    .get();

  if (snapshots.empty) return;

  let batch = firestore.batch();
  let batchCount = 0;

  for (const doc of snapshots.docs) {
    const archiveRef = firestore.collection('archivedSnapshots').doc(doc.id);
    batch.set(archiveRef, {
      ...doc.data(),
      archivedAt: new Date().toISOString(),
      originalDocId: doc.id,
    });
    batch.delete(doc.ref);
    batchCount += 2;

    if (batchCount >= FIRESTORE_BATCH_LIMIT - 2) {
      await batch.commit();
      batch = firestore.batch();
      batchCount = 0;
    }
  }

  if (batchCount > 0) await batch.commit();

  await firestore.doc(`userData/${userId}`).set({
    snapshotsArchived: true,
    snapshotsArchivedAt: new Date().toISOString(),
  }, { merge: true });

  console.log(`[snapshotCleanup][archiveUserSnapshots] Archived ${snapshots.size} snapshots for user ${userId}`);
}

// ============================================================================
// SCHEDULED FUNCTIONS
// ============================================================================

const scheduledSnapshotCleanup = onSchedule({
  schedule: '0 3 * * 0',
  timeZone: 'America/New_York',
  timeoutSeconds: 540,
  memory: '512MiB',
  labels: { component: 'perf-snap', purpose: 'cleanup-sold-assets' },
}, async () => {
  await cleanupSoldAssetSnapshots(db);
});

const scheduledSnapshotArchival = onSchedule({
  schedule: '0 4 1 * *',
  timeZone: 'America/New_York',
  timeoutSeconds: 540,
  memory: '512MiB',
  labels: { component: 'perf-snap', purpose: 'archive-inactive-users' },
}, async () => {
  const auth = admin.auth();
  await archiveInactiveUserSnapshots(db, auth);
});

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  cleanupSoldAssetSnapshots,
  archiveInactiveUserSnapshots,
  archiveUserSnapshots,
  scheduledSnapshotCleanup,
  scheduledSnapshotArchival,
  SOLD_ASSET_GRACE_DAYS,
  INACTIVE_USER_DAYS,
  QUERY_BATCH_SIZE,
  FIRESTORE_BATCH_LIMIT,
};
