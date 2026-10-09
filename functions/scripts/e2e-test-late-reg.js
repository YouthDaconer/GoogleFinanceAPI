/**
 * E2E Test Script for LATE-REG Epic
 * 
 * Tests the complete flow:
 * 1. Copy data from source user to test user
 * 2. Simulate a retroactive transaction
 * 3. Verify stale marking
 * 4. Run reconciliation
 * 5. Verify atomic backfill (accounts + overall)
 * 
 * Usage:
 *   node e2e-test-late-reg.js --setup      # Copy data from source to test user
 *   node e2e-test-late-reg.js --simulate   # Simulate retroactive transaction
 *   node e2e-test-late-reg.js --check      # Check stale status and performance
 *   node e2e-test-late-reg.js --reconcile  # Run reconciliation manually
 *   node e2e-test-late-reg.js --cleanup    # Remove test user data
 * 
 * @see LATE-REG Epic
 */

const admin = require('firebase-admin');
const serviceAccount = require('../key.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// ============================================================================
// CONFIGURATION
// ============================================================================

const CONFIG = {
  SOURCE_USER_ID: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2',
  TEST_USER_ID: 'wn6djgpjUyQbJW3NFZfF9H3QLCB2',
  
  // Sample dates to copy (recent trading days)
  SAMPLE_DATES: [
    '2026-02-10',
    '2026-02-11',
    '2026-02-12',
    '2026-02-13',
  ],
  
  // Retroactive date for simulation (1 day back)
  RETROACTIVE_DATE: '2026-02-12',
};

// ============================================================================
// UTILITIES
// ============================================================================

function log(level, message, data = null) {
  const timestamp = new Date().toISOString().substring(11, 19);
  const prefix = {
    'INFO': '📋',
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'PROGRESS': '🔄',
  }[level] || '•';
  
  console.log(`[${timestamp}] ${prefix} ${message}`);
  if (data) console.log('   ', JSON.stringify(data, null, 2).split('\n').join('\n    '));
}

// ============================================================================
// STEP 1: SETUP - Copy data from source user to test user
// ============================================================================

async function setupTestUser() {
  log('PROGRESS', 'Starting setup: copying data from source to test user...');
  
  const sourceUserId = CONFIG.SOURCE_USER_ID;
  const testUserId = CONFIG.TEST_USER_ID;
  
  // 1. Get source user's portfolio accounts
  log('PROGRESS', 'Fetching source portfolio accounts...');
  const accountsSnap = await db.collection('portfolioAccounts')
    .where('userId', '==', sourceUserId)
    .get();
  
  if (accountsSnap.empty) {
    log('ERROR', 'No portfolio accounts found for source user');
    return;
  }
  
  const accountIdMap = new Map(); // sourceId -> newId
  
  // 2. Copy portfolio accounts
  log('PROGRESS', `Copying ${accountsSnap.size} portfolio accounts...`);
  for (const doc of accountsSnap.docs) {
    const data = doc.data();
    const newAccountRef = db.collection('portfolioAccounts').doc();
    
    await newAccountRef.set({
      ...data,
      userId: testUserId,
      name: `[TEST] ${data.name}`,
      createdAt: new Date().toISOString(),
    });
    
    accountIdMap.set(doc.id, newAccountRef.id);
    log('SUCCESS', `Created account: ${newAccountRef.id} (from ${doc.id})`);
  }
  
  // 3. Get source user's assets
  log('PROGRESS', 'Fetching source assets...');
  const accountIds = [...accountIdMap.keys()];
  const assetIdMap = new Map(); // sourceId -> newId
  
  for (const sourceAccountId of accountIds) {
    const assetsSnap = await db.collection('assets')
      .where('portfolioAccount', '==', sourceAccountId)
      .get();
    
    log('INFO', `Found ${assetsSnap.size} assets in account ${sourceAccountId}`);
    
    for (const doc of assetsSnap.docs) {
      const data = doc.data();
      const newAssetRef = db.collection('assets').doc();
      const newAccountId = accountIdMap.get(sourceAccountId);
      
      await newAssetRef.set({
        ...data,
        userId: testUserId,
        portfolioAccount: newAccountId,
        company: `[TEST] ${data.company || data.name}`,
      });
      
      assetIdMap.set(doc.id, newAssetRef.id);
      log('SUCCESS', `Created asset: ${data.name} -> ${newAssetRef.id}`);
    }
  }
  
  // 4. Copy ALL transactions for each account (not just recent 50)
  log('PROGRESS', 'Fetching source transactions by account...');
  let totalTxCopied = 0;
  
  for (const sourceAccountId of accountIds) {
    const txSnap = await db.collection('transactions')
      .where('portfolioAccountId', '==', sourceAccountId)
      .get();
    
    log('INFO', `Found ${txSnap.size} transactions in account ${sourceAccountId}`);
    
    for (const doc of txSnap.docs) {
      const data = doc.data();
      const newTxRef = db.collection('transactions').doc();
      
      // Map old IDs to new IDs
      const newAccountId = accountIdMap.get(data.portfolioAccountId);
      const newAssetId = assetIdMap.get(data.assetId) || data.assetId;
      
      // Skip transactions with undefined required fields
      if (!newAccountId || !data.type) {
        log('WARNING', `Skipping transaction with missing fields: ${doc.id}`);
        continue;
      }
      
      // Clean undefined values
      const cleanedData = JSON.parse(JSON.stringify({
        ...data,
        userId: testUserId,
        portfolioAccountId: newAccountId,
        assetId: newAssetId || null, // Convert undefined to null
      }));
      
      await newTxRef.set(cleanedData);
      totalTxCopied++;
    }
  }
  log('SUCCESS', `Copied ${totalTxCopied} transactions total`);
  
  // 5. Copy sample performance data (dates subcollection)
  log('PROGRESS', 'Copying sample performance data...');
  
  for (const date of CONFIG.SAMPLE_DATES) {
    // Copy overall dates
    const overallDoc = await db.collection('portfolioPerformance')
      .doc(sourceUserId)
      .collection('dates')
      .doc(date)
      .get();
    
    if (overallDoc.exists) {
      await db.collection('portfolioPerformance')
        .doc(testUserId)
        .collection('dates')
        .doc(date)
        .set(overallDoc.data());
      log('SUCCESS', `Copied dates/${date}`);
    }
    
    // Copy per-account dates
    for (const [sourceAccountId, newAccountId] of accountIdMap.entries()) {
      const accountDoc = await db.collection('portfolioPerformance')
        .doc(sourceUserId)
        .collection('accounts')
        .doc(sourceAccountId)
        .collection('dates')
        .doc(date)
        .get();
      
      if (accountDoc.exists) {
        await db.collection('portfolioPerformance')
          .doc(testUserId)
          .collection('accounts')
          .doc(newAccountId)
          .collection('dates')
          .doc(date)
          .set(accountDoc.data());
        log('SUCCESS', `Copied accounts/${newAccountId}/dates/${date}`);
      }
    }
  }
  
  // 6. Set main performance document
  const mainPerfDoc = await db.collection('portfolioPerformance')
    .doc(sourceUserId)
    .get();
  
  if (mainPerfDoc.exists) {
    const perfData = mainPerfDoc.data();
    // Remove any existing _stale marker
    delete perfData._stale;
    
    await db.collection('portfolioPerformance')
      .doc(testUserId)
      .set({
        ...perfData,
        date: CONFIG.SAMPLE_DATES[CONFIG.SAMPLE_DATES.length - 1],
      });
    log('SUCCESS', 'Set main portfolioPerformance document');
  }
  
  // Store account mapping for later use
  const mappingDoc = {
    sourceUserId,
    testUserId,
    accountMapping: Object.fromEntries(accountIdMap),
    assetMapping: Object.fromEntries(assetIdMap),
    createdAt: new Date().toISOString(),
  };
  
  await db.collection('e2eTestState').doc('late-reg-mapping').set(mappingDoc);
  log('SUCCESS', 'Stored ID mappings for later use');
  
  log('SUCCESS', '=== SETUP COMPLETE ===');
  log('INFO', `Test user: ${testUserId}`);
  log('INFO', `Accounts created: ${accountIdMap.size}`);
  log('INFO', `Assets created: ${assetIdMap.size}`);
  log('INFO', `Performance dates: ${CONFIG.SAMPLE_DATES.join(', ')}`);
}

// ============================================================================
// STEP 2: SIMULATE - Create a retroactive transaction
// ============================================================================

async function simulateRetroactiveTransaction() {
  log('PROGRESS', 'Simulating retroactive transaction...');
  
  const testUserId = CONFIG.TEST_USER_ID;
  const retroactiveDate = CONFIG.RETROACTIVE_DATE;
  
  // Get mapping
  const mappingDoc = await db.collection('e2eTestState').doc('late-reg-mapping').get();
  if (!mappingDoc.exists) {
    log('ERROR', 'No mapping found. Run --setup first.');
    return;
  }
  
  const mapping = mappingDoc.data();
  const accountIds = Object.values(mapping.accountMapping);
  
  if (accountIds.length === 0) {
    log('ERROR', 'No accounts found in mapping');
    return;
  }
  
  const testAccountId = accountIds[0];
  
  // Combine date with current time (same as assetHandlers.js)
  const now = new Date();
  const [year, month, day] = retroactiveDate.split('-').map(Number);
  const combinedDate = new Date(
    year,
    month - 1,
    day,
    now.getHours(),
    now.getMinutes(),
    now.getSeconds(),
    now.getMilliseconds()
  );
  const acquisitionDateWithTime = combinedDate.toISOString();
  
  // Create a new asset with retroactive date
  const newAssetRef = db.collection('assets').doc();
  const newAsset = {
    userId: testUserId,
    portfolioAccount: testAccountId,
    name: 'MSFT',
    company: '[E2E-TEST] Microsoft Corporation',
    assetType: 'stock',
    currency: 'USD',
    market: 'NASDAQ',
    units: 10,
    unitValue: 410.50,
    acquisitionDate: acquisitionDateWithTime,
    acquisitionDollarValue: 1,
    defaultCurrencyForAdquisitionDollar: 'USD',
    isActive: true,
    commission: 0,
    createdAt: new Date().toISOString(),
  };
  
  await newAssetRef.set(newAsset);
  log('SUCCESS', `Created retroactive asset: ${newAssetRef.id}`);
  log('INFO', `Asset: MSFT, Date: ${retroactiveDate}, Units: 10, Value: $4,105`);
  
  // Create corresponding transaction
  const newTxRef = db.collection('transactions').doc();
  const newTx = {
    userId: testUserId,
    portfolioAccountId: testAccountId,
    assetId: newAssetRef.id,
    assetName: 'MSFT',
    assetType: 'stock',
    type: 'buy',
    amount: 10,
    price: 410.50,
    currency: 'USD',
    date: acquisitionDateWithTime,
    dollarPriceToDate: 1,
    defaultCurrencyForAdquisitionDollar: 'USD',
    commission: 0,
  };
  
  await newTxRef.set(newTx);
  log('SUCCESS', `Created transaction: ${newTxRef.id}`);
  
  // Now manually trigger the stale marking logic
  // (In production, this would be called by assetHandlers.js)
  log('PROGRESS', 'Marking performance as stale...');
  
  const { checkAndMarkStaleIfRetroactive } = require('../utils/performanceStaleMarker');
  
  await checkAndMarkStaleIfRetroactive(testUserId, retroactiveDate, {
    reason: 'retroactive_transaction',
    transactionType: 'buy',
    portfolioAccount: testAccountId,
  });
  
  // Wait a moment for fire-and-forget to complete
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Verify stale was marked
  const perfDoc = await db.collection('portfolioPerformance').doc(testUserId).get();
  const stale = perfDoc.data()?._stale;
  
  if (stale) {
    log('SUCCESS', '=== STALE MARKING VERIFIED ===');
    log('INFO', `Stale since: ${stale.since}`);
    log('INFO', `Reason: ${stale.reason}`);
    log('INFO', `Registered at: ${stale.registeredAt}`);
  } else {
    log('WARNING', 'No _stale marker found. The transaction may not be retroactive relative to existing performance.');
  }
  
  // Store asset/tx IDs for later cleanup
  await db.collection('e2eTestState').doc('late-reg-mapping').update({
    simulatedAssetId: newAssetRef.id,
    simulatedTxId: newTxRef.id,
    simulatedDate: retroactiveDate,
  });
}

// ============================================================================
// STEP 3: CHECK - Verify stale status and current performance
// ============================================================================

async function checkStatus() {
  log('PROGRESS', 'Checking test user status...');
  
  const testUserId = CONFIG.TEST_USER_ID;
  
  // Check main performance document
  const perfDoc = await db.collection('portfolioPerformance').doc(testUserId).get();
  
  if (!perfDoc.exists) {
    log('ERROR', 'No portfolioPerformance document found for test user');
    return;
  }
  
  const data = perfDoc.data();
  
  log('INFO', '=== MAIN PERFORMANCE DOCUMENT ===');
  log('INFO', `Date: ${data.date}`);
  log('INFO', `USD Total Value: $${data.USD?.totalValue?.toFixed(2)}`);
  log('INFO', `USD Total Investment: $${data.USD?.totalInvestment?.toFixed(2)}`);
  log('INFO', `Adjusted Daily Change: ${data.USD?.adjustedDailyChangePercentage?.toFixed(4)}%`);
  
  if (data._stale) {
    log('WARNING', '=== STALE MARKER PRESENT ===');
    log('INFO', `Since: ${data._stale.since}`);
    log('INFO', `Reason: ${data._stale.reason}`);
    log('INFO', `Retry Count: ${data._stale.retryCount || 0}`);
    log('INFO', `Truncated: ${data._stale.truncated || false}`);
  } else {
    log('SUCCESS', 'No stale marker - data is current');
  }
  
  // Check dates subcollection
  log('INFO', '\n=== OVERALL DATES SUBCOLLECTION ===');
  const datesSnap = await db.collection('portfolioPerformance')
    .doc(testUserId)
    .collection('dates')
    .orderBy('date', 'desc')
    .limit(5)
    .get();
  
  datesSnap.docs.forEach(doc => {
    const d = doc.data();
    log('INFO', `${doc.id}: Value=$${d.USD?.totalValue?.toFixed(2)}, Adj=${d.USD?.adjustedDailyChangePercentage?.toFixed(4)}%`);
  });
  
  // Check accounts subcollection
  log('INFO', '\n=== ACCOUNTS SUBCOLLECTIONS ===');
  const accountsSnap = await db.collection('portfolioPerformance')
    .doc(testUserId)
    .collection('accounts')
    .get();
  
  if (accountsSnap.empty) {
    // Try to get accounts from mapping
    const mappingDoc = await db.collection('e2eTestState').doc('late-reg-mapping').get();
    if (mappingDoc.exists) {
      const mapping = mappingDoc.data();
      for (const [source, target] of Object.entries(mapping.accountMapping || {})) {
        const accountDates = await db.collection('portfolioPerformance')
          .doc(testUserId)
          .collection('accounts')
          .doc(target)
          .collection('dates')
          .orderBy('date', 'desc')
          .limit(4)
          .get();
        
        log('INFO', `Account ${target.substring(0, 10)}... (${accountDates.size} dates):`);
        accountDates.docs.forEach(doc => {
          const d = doc.data();
          log('INFO', `  ${doc.id}: Value=$${d.USD?.totalValue?.toFixed(2)}, Adj=${d.USD?.adjustedDailyChangePercentage?.toFixed(4)}%`);
        });
      }
    }
  } else {
    for (const accountDoc of accountsSnap.docs) {
      log('INFO', `Account: ${accountDoc.id}`);
      
      const accountDates = await db.collection('portfolioPerformance')
        .doc(testUserId)
        .collection('accounts')
        .doc(accountDoc.id)
        .collection('dates')
        .orderBy('date', 'desc')
        .limit(3)
        .get();
      
      accountDates.docs.forEach(doc => {
        const d = doc.data();
        log('INFO', `  ${doc.id}: Value=$${d.USD?.totalValue?.toFixed(2)}`);
      });
    }
  }
}

// ============================================================================
// STEP 4: RECONCILE - Run reconciliation manually
// ============================================================================

async function runReconciliation() {
  log('PROGRESS', 'Running manual reconciliation...');
  
  const testUserId = CONFIG.TEST_USER_ID;
  
  // Check if stale
  const perfDoc = await db.collection('portfolioPerformance').doc(testUserId).get();
  const stale = perfDoc.data()?._stale;
  
  if (!stale) {
    log('WARNING', 'No stale marker found. Nothing to reconcile.');
    return;
  }
  
  log('INFO', `Reconciling from ${stale.since} to today...`);
  
  // Import backfill module
  const { generateTradingDays, getTodayDate, backfillUserPerformance } = require('../services/backfillCoreModule');
  
  // Generate trading days
  const today = getTodayDate();
  const tradingDays = await generateTradingDays(stale.since, today);
  
  log('INFO', `Trading days to process: ${tradingDays.length}`);
  log('INFO', `From: ${tradingDays[0]} to ${tradingDays[tradingDays.length - 1]}`);
  
  // Run backfill
  const result = await backfillUserPerformance(testUserId, tradingDays);
  
  if (result.success) {
    log('SUCCESS', '=== RECONCILIATION COMPLETE ===');
    log('INFO', `Days processed: ${result.daysProcessed}`);
    
    // Clear stale marker
    await db.collection('portfolioPerformance').doc(testUserId).update({
      _stale: admin.firestore.FieldValue.delete()
    });
    log('SUCCESS', 'Cleared _stale marker');
    
  } else {
    log('ERROR', 'Reconciliation failed');
    log('ERROR', `Errors: ${result.errors.join(', ')}`);
  }
  
  // Verify results
  log('PROGRESS', '\nVerifying reconciliation results...');
  await checkStatus();
}

// ============================================================================
// STEP 5: CLEANUP - Remove test user data
// ============================================================================

async function cleanup() {
  log('PROGRESS', 'Cleaning up test user data...');
  
  const testUserId = CONFIG.TEST_USER_ID;
  
  // Get mapping
  const mappingDoc = await db.collection('e2eTestState').doc('late-reg-mapping').get();
  
  if (!mappingDoc.exists) {
    log('WARNING', 'No mapping found. Manual cleanup may be needed.');
  }
  
  // Delete portfolioPerformance and subcollections
  log('PROGRESS', 'Deleting portfolioPerformance...');
  
  // Delete accounts subcollections first
  const accountsSnap = await db.collection('portfolioPerformance')
    .doc(testUserId)
    .collection('accounts')
    .get();
  
  for (const accountDoc of accountsSnap.docs) {
    // Delete dates within each account
    const datesSnap = await accountDoc.ref.collection('dates').get();
    for (const dateDoc of datesSnap.docs) {
      await dateDoc.ref.delete();
    }
    await accountDoc.ref.delete();
  }
  
  // Delete main dates subcollection
  const datesSnap = await db.collection('portfolioPerformance')
    .doc(testUserId)
    .collection('dates')
    .get();
  
  for (const doc of datesSnap.docs) {
    await doc.ref.delete();
  }
  
  // Delete main document
  await db.collection('portfolioPerformance').doc(testUserId).delete();
  log('SUCCESS', 'Deleted portfolioPerformance');
  
  // Delete transactions
  log('PROGRESS', 'Deleting transactions...');
  const txSnap = await db.collection('transactions')
    .where('userId', '==', testUserId)
    .get();
  
  for (const doc of txSnap.docs) {
    await doc.ref.delete();
  }
  log('SUCCESS', `Deleted ${txSnap.size} transactions`);
  
  // Delete assets
  log('PROGRESS', 'Deleting assets...');
  const assetsSnap = await db.collection('assets')
    .where('userId', '==', testUserId)
    .get();
  
  for (const doc of assetsSnap.docs) {
    await doc.ref.delete();
  }
  log('SUCCESS', `Deleted ${assetsSnap.size} assets`);
  
  // Delete portfolioAccounts
  log('PROGRESS', 'Deleting portfolioAccounts...');
  const accountsToDelete = await db.collection('portfolioAccounts')
    .where('userId', '==', testUserId)
    .get();
  
  for (const doc of accountsToDelete.docs) {
    await doc.ref.delete();
  }
  log('SUCCESS', `Deleted ${accountsToDelete.size} portfolioAccounts`);
  
  // Delete mapping document
  await db.collection('e2eTestState').doc('late-reg-mapping').delete();
  log('SUCCESS', 'Deleted mapping document');
  
  log('SUCCESS', '=== CLEANUP COMPLETE ===');
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--setup')) {
    await setupTestUser();
  } else if (args.includes('--simulate')) {
    await simulateRetroactiveTransaction();
  } else if (args.includes('--check')) {
    await checkStatus();
  } else if (args.includes('--reconcile')) {
    await runReconciliation();
  } else if (args.includes('--cleanup')) {
    await cleanup();
  } else {
    console.log(`
E2E Test Script for LATE-REG Epic

Usage:
  node e2e-test-late-reg.js --setup      # Step 1: Copy data from source to test user
  node e2e-test-late-reg.js --simulate   # Step 2: Simulate retroactive transaction
  node e2e-test-late-reg.js --check      # Step 3: Check stale status and performance
  node e2e-test-late-reg.js --reconcile  # Step 4: Run reconciliation manually
  node e2e-test-late-reg.js --cleanup    # Step 5: Remove test user data

Test User: ${CONFIG.TEST_USER_ID}
Source User: ${CONFIG.SOURCE_USER_ID}
Sample Dates: ${CONFIG.SAMPLE_DATES.join(', ')}
Retroactive Date: ${CONFIG.RETROACTIVE_DATE}
    `);
  }
  
  process.exit(0);
}

main().catch(error => {
  log('ERROR', 'Fatal error', { message: error.message, stack: error.stack });
  process.exit(1);
});
