/**
 * Script de diagnóstico: Verificar P&L de un activo específico
 */

const admin = require('../../services/firebaseAdmin');
const db = admin.firestore();

async function check() {
  const assetName = process.argv[2] || 'CAT';
  
  // Ver transacciones del activo
  const txSnapshot = await db.collection('transactions')
    .where('assetName', '==', assetName)
    .get();
  
  console.log(`=== TRANSACCIONES DE ${assetName} ===`);
  let totalBuy = 0;
  let totalSell = 0;
  let totalPnL = 0;
  let buyQty = 0;
  let sellQty = 0;
  
  const txs = txSnapshot.docs
    .map(doc => ({ id: doc.id, ...doc.data() }))
    .sort((a, b) => a.date.localeCompare(b.date));
  
  txs.forEach(t => {
    const amount = parseFloat(t.amount) || 0;
    const price = parseFloat(t.price) || 0;
    const pnl = parseFloat(t.valuePnL) || 0;
    
    console.log(`${t.date} | ${t.type.toUpperCase().padEnd(8)} | qty=${amount.toFixed(4)} | price=$${price.toFixed(2)} | total=$${(amount * price).toFixed(2)} | valuePnL=${pnl ? '$' + pnl.toFixed(2) : 'N/A'}`);
    
    if (t.type === 'buy') {
      totalBuy += amount * price;
      buyQty += amount;
    } else if (t.type === 'sell') {
      totalSell += amount * price;
      sellQty += amount;
      totalPnL += pnl;
    }
  });
  
  console.log('');
  console.log('=== RESUMEN ===');
  console.log('Cantidad comprada:', buyQty.toFixed(4));
  console.log('Cantidad vendida:', sellQty.toFixed(4));
  console.log('Cantidad restante:', (buyQty - sellQty).toFixed(4));
  console.log('');
  console.log('Total comprado: $' + totalBuy.toFixed(2));
  console.log('Total vendido: $' + totalSell.toFixed(2));
  console.log('P&L Realizada (valuePnL): $' + totalPnL.toFixed(2));
  console.log('P&L Calculada (venta - compra): $' + (totalSell - totalBuy).toFixed(2));
  
  // Verificar cómo el servicio de atribución calcula la P&L
  const startDate = '2025-01-01';
  const endDate = '2025-12-31';
  
  console.log('');
  console.log('=== VENTAS EN EL PERÍODO YTD ===');
  
  const sellsInPeriod = txs.filter(t => 
    t.type === 'sell' && 
    t.date >= startDate && 
    t.date <= endDate
  );
  
  let ytdPnL = 0;
  sellsInPeriod.forEach(t => {
    const pnl = parseFloat(t.valuePnL) || 0;
    ytdPnL += pnl;
    console.log(`${t.date}: vendidas ${t.amount} @ $${t.price} → P&L=$${pnl.toFixed(2)}`);
  });
  
  console.log('');
  console.log('Total P&L YTD: $' + ytdPnL.toFixed(2));
  
  process.exit(0);
}
check();
