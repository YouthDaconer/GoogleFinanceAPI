const admin = require('../services/firebaseAdmin');
const db = admin.firestore();

async function check() {
  const uids = {
    A: 'gTZ6Ie8FckSqEqfMX9OWkMmJEqi2',
    B: 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2'
  };
  for (const [label, uid] of Object.entries(uids)) {
    const doc = await db.collection('userData').doc(uid).get();
    const s = doc.data().subscription;
    console.log('\n=== Usuario ' + label + ' (' + doc.data().email + ') ===');
    console.log(JSON.stringify(s, null, 2));
  }
  process.exit(0);
}
check().catch(e => { console.error(e); process.exit(1); });
