/**
 * Diagnóstico de assets en monedas no-USD
 */
const admin = require('../../services/firebaseAdmin');
const db = admin.firestore();

async function check() {
  const assets = await db.collection('assets').where('isActive', '==', true).get();
  
  console.log('=== ASSETS EN MONEDAS NO-USD ===');
  assets.docs.forEach(a => {
    const d = a.data();
    if (d.currency && d.currency !== 'USD') {
      console.log(`${d.name}: ${d.units} units @ ${d.unitValue} ${d.currency} (acqDollar: ${d.acquisitionDollarValue})`);
    }
  });
  
  console.log('\n=== CUENTA ggM52Gim (ECOPETROL) ===');
  const ecopetrol = await db.collection('assets')
    .where('portfolioAccount', '==', 'ggM52GimbLL7jwvegc9o')
    .get();
  ecopetrol.docs.forEach(a => {
    const d = a.data();
    console.log(`  name: ${d.name}`);
    console.log(`  units: ${d.units}`);
    console.log(`  unitValue: ${d.unitValue}`);
    console.log(`  currency: ${d.currency}`);
    console.log(`  acquisitionDollarValue: ${d.acquisitionDollarValue}`);
    console.log(`  isActive: ${d.isActive}`);
  });
  
  // Calcular inversión total correctamente
  console.log('\n=== CÁLCULO DE INVERSIÓN TOTAL EN USD ===');
  let totalInvUSD = 0;
  let totalInvRaw = 0;
  
  assets.docs.forEach(a => {
    const d = a.data();
    const invRaw = (d.unitValue || 0) * (d.units || 0);
    totalInvRaw += invRaw;
    
    let invUSD = invRaw;
    if (d.currency && d.currency !== 'USD' && d.acquisitionDollarValue) {
      invUSD = invRaw / d.acquisitionDollarValue;
    }
    totalInvUSD += invUSD;
  });
  
  console.log(`Total bruto (sin conversión): $${totalInvRaw.toFixed(2)}`);
  console.log(`Total convertido a USD: $${totalInvUSD.toFixed(2)}`);
}

check().then(() => process.exit(0));
