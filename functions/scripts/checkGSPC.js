const admin = require('firebase-admin');
const sa = require('../key.json');
admin.initializeApp({credential: admin.credential.cert(sa)});
const db = admin.firestore();

(async () => {
  const doc = await db.doc('indexHistories/GSPC/dates/2026-03-04').get();
  console.log('GSPC 2026-03-04 full data:');
  console.log(JSON.stringify(doc.data(), null, 2));
  process.exit(0);
})();
