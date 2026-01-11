/**
 * Transaction Processor
 * 
 * Procesa transacciones de compra/venta para generar posiciones cerradas.
 * Migrado desde useClosedPositions.ts del frontend.
 * 
 * @module services/closedPositions/transactionProcessor
 * @see docs/stories/36.story.md
 */

const admin = require('../firebaseAdmin');
const db = admin.firestore();

/**
 * Obtiene transacciones de un usuario para las cuentas especificadas
 * 
 * @param {string} userId - ID del usuario (para logging)
 * @param {string[]} accountIds - IDs de cuentas a consultar
 * @returns {Promise<{sells: Array, buys: Array}>}
 */
async function fetchTransactionsByAccounts(userId, accountIds) {
  const allTransactions = [];
  
  // Queries en batches de 10 para manejar el límite de Firestore 'in'
  const batches = [];
  for (let i = 0; i < accountIds.length; i += 10) {
    batches.push(accountIds.slice(i, i + 10));
  }
  
  for (const batch of batches) {
    const snapshot = await db.collection('transactions')
      .where('portfolioAccountId', 'in', batch)
      .get();
    
    snapshot.docs.forEach(doc => {
      allTransactions.push({ id: doc.id, ...doc.data() });
    });
  }
  
  // Separar compras y ventas
  const sells = allTransactions.filter(tx => tx.type === 'sell' && Number(tx.amount) > 0);
  const buys = allTransactions.filter(tx => tx.type === 'buy');
  
  console.log(`[transactionProcessor] Loaded ${sells.length} sells, ${buys.length} buys for ${accountIds.length} accounts`);
  
  return { sells, buys };
}

/**
 * Construye mapas de fechas y precios de compra por asset
 * 
 * @param {Array} buyTransactions - Transacciones de compra
 * @returns {{buyDates: Map, buyPrices: Map}}
 */
function buildBuyMaps(buyTransactions) {
  const buyDates = new Map();
  const buyPrices = new Map();
  
  for (const tx of buyTransactions) {
    const assetId = tx.assetId;
    if (!assetId) continue;
    
    // Parsear fecha como local (agregar T12:00:00 para evitar problemas de timezone)
    const buyDate = new Date(tx.date + 'T12:00:00');
    const amount = Number(tx.amount) || 0;
    const price = Number(tx.price) || 0;
    
    // Guardar fecha más antigua
    const existingDate = buyDates.get(assetId);
    if (!existingDate || buyDate < existingDate) {
      buyDates.set(assetId, buyDate);
    }
    
    // Acumular para precio promedio ponderado
    const existing = buyPrices.get(assetId) || { totalValue: 0, totalUnits: 0 };
    buyPrices.set(assetId, {
      totalValue: existing.totalValue + (amount * price),
      totalUnits: existing.totalUnits + amount
    });
  }
  
  return { buyDates, buyPrices };
}

/**
 * Procesa transacciones de venta para generar posiciones cerradas
 * 
 * @param {Array} sellTransactions - Transacciones de venta
 * @param {Map} buyDates - Mapa de assetId -> fecha de compra más antigua
 * @param {Map} buyPrices - Mapa de assetId -> {totalValue, totalUnits}
 * @param {Map} accountNames - Mapa de accountId -> nombre de cuenta
 * @returns {Array} Posiciones cerradas procesadas
 */
function processTransactions(sellTransactions, buyDates, buyPrices, accountNames) {
  const positions = [];
  
  for (const tx of sellTransactions) {
    const sellDate = new Date(tx.date + 'T12:00:00');
    const buyDate = buyDates.get(tx.assetId) || sellDate;
    
    const holdingPeriodDays = Math.max(0, Math.floor(
      (sellDate.getTime() - buyDate.getTime()) / (1000 * 60 * 60 * 24)
    ));
    
    const unitsSold = Number(tx.amount) || 0;
    const sellPrice = Number(tx.price) || 0;
    const valuePnL = Number(tx.valuePnL) || 0;
    const commission = Number(tx.commission) || 0;
    
    // Calcular precio de compra
    let buyPrice = 0;
    if (valuePnL !== 0 && unitsSold > 0) {
      buyPrice = sellPrice - (valuePnL / unitsSold);
    } else {
      const buyData = buyPrices.get(tx.assetId);
      buyPrice = buyData && buyData.totalUnits > 0 
        ? buyData.totalValue / buyData.totalUnits 
        : sellPrice;
    }
    
    buyPrice = Math.max(0, buyPrice);
    
    const totalInvested = buyPrice * unitsSold;
    const totalReceived = (sellPrice * unitsSold) - commission;
    const realizedPnLPercent = buyPrice > 0 
      ? ((sellPrice - buyPrice) / buyPrice) * 100 
      : 0;
    
    positions.push({
      id: tx.id || `${tx.assetId}_${tx.date}`,
      assetId: tx.assetId || '',
      ticker: tx.assetName || tx.symbol || 'Unknown',
      assetType: tx.assetType || 'stock',
      buyDate: buyDate.toISOString(),
      sellDate: sellDate.toISOString(),
      holdingPeriodDays,
      buyPrice,
      sellPrice,
      currency: tx.currency || 'USD',
      unitsSold,
      totalInvested,
      totalReceived,
      realizedPnL: valuePnL,
      realizedPnLPercent,
      commission,
      netPnL: valuePnL - commission,
      portfolioAccountId: tx.portfolioAccountId || '',
      accountName: accountNames.get(tx.portfolioAccountId) || 'Sin cuenta',
      market: tx.market || '',
      isFullSale: tx.closedPnL === true,
      // Datos para conversión de moneda
      dollarPriceToDate: Number(tx.dollarPriceToDate) || 1,
      originalCurrency: tx.defaultCurrencyForAdquisitionDollar || 'USD'
    });
  }
  
  return positions;
}

/**
 * Obtiene posiciones cerradas para un usuario
 * 
 * @param {string} userId - ID del usuario
 * @param {string[]} accountIds - IDs de cuentas (vacío = todas)
 * @param {Map} accountNames - Mapa de accountId -> nombre
 * @returns {Promise<Array>} Posiciones cerradas
 */
async function getClosedPositionsFromTransactions(userId, accountIds, accountNames) {
  const { sells, buys } = await fetchTransactionsByAccounts(userId, accountIds);
  
  if (sells.length === 0) {
    console.log('[transactionProcessor] No sell transactions found');
    return [];
  }
  
  const { buyDates, buyPrices } = buildBuyMaps(buys);
  const positions = processTransactions(sells, buyDates, buyPrices, accountNames);
  
  console.log(`[transactionProcessor] Processed ${positions.length} closed positions`);
  
  return positions;
}

module.exports = {
  fetchTransactionsByAccounts,
  buildBuyMaps,
  processTransactions,
  getClosedPositionsFromTransactions
};
