const admin = require('firebase-admin');
const sa = require('../../key.json');
admin.initializeApp({credential: admin.credential.cert(sa)});
const db = admin.firestore();

(async () => {
  const accs = await db.collection('portfolioAccounts')
    .where('userId', '==', 'DDeR8P5hYgfuN8gcU4RsQfdTJqx2').get();
  const ids = accs.docs.map(d => d.id);
  console.log('Account IDs:', ids);
  
  const assets = await db.collection('assets')
    .where('portfolioAccount', 'in', ids).get();
  let active = 0;
  const syms = [];
  assets.forEach(d => {
    const a = d.data();
    if (a.isActive && a.units > 0) {
      active++;
      syms.push(a.name);
    }
  });
  console.log(`Active assets: ${active}`);
  console.log(`Symbols: ${syms.join(', ')}`);
  process.exit(0);
})();
