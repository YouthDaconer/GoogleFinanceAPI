/**
 * Contribution Calculator Service
 * 
 * SRP: Responsabilidad única de calcular contribuciones de activos al rendimiento.
 * Usa datos de assetPerformance de Firestore (ROI personal del usuario).
 * 
 * MEJORADO: Ahora incluye P&L realizada de ventas en el período para
 * cálculos de atribución más precisos.
 * 
 * @module services/attribution/contributionCalculator
 * @see docs/architecture/portfolio-attribution-coherence-analysis.md
 */

const admin = require('../firebaseAdmin');
const db = admin.firestore();

/**
 * Obtiene datos de portfolioPerformance para una fecha
 * @param {string} userId - ID del usuario
 * @param {string} dateStr - Fecha en formato YYYY-MM-DD
 * @param {string} accountId - ID de cuenta o 'overall'
 * @returns {Promise<Object|null>} Datos del documento o null
 */
async function getPerformanceDataForDate(userId, dateStr, accountId = 'overall') {
  const path = accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates/${dateStr}`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates/${dateStr}`;
  
  const doc = await db.doc(path).get();
  return doc.exists ? doc.data() : null;
}

/**
 * Busca el documento de performance más cercano a una fecha
 * @param {string} userId - ID del usuario
 * @param {string} startDateStr - Fecha de inicio en formato YYYY-MM-DD
 * @param {string} accountId - ID de cuenta o 'overall'
 * @param {string} direction - 'asc' para buscar hacia adelante, 'desc' hacia atrás
 * @returns {Promise<Object|null>} Datos del documento más cercano
 */
async function findNearestPerformanceData(userId, startDateStr, accountId = 'overall', direction = 'asc') {
  const path = accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;
  
  const snapshot = await db.collection(path)
    .where('date', direction === 'asc' ? '>=' : '<=', startDateStr)
    .orderBy('date', direction)
    .limit(1)
    .get();
  
  if (snapshot.empty) return null;
  return { id: snapshot.docs[0].id, ...snapshot.docs[0].data() };
}

/**
 * Obtiene el documento más reciente de performance
 * @param {string} userId - ID del usuario
 * @param {string} accountId - ID de cuenta o 'overall'
 * @returns {Promise<Object|null>} Datos del documento más reciente
 */
async function getLatestPerformanceData(userId, accountId = 'overall') {
  const path = accountId === 'overall'
    ? `portfolioPerformance/${userId}/dates`
    : `portfolioPerformance/${userId}/accounts/${accountId}/dates`;
  
  const snapshot = await db.collection(path)
    .orderBy('date', 'desc')
    .limit(1)
    .get();
  
  if (snapshot.empty) return null;
  return { id: snapshot.docs[0].id, ...snapshot.docs[0].data() };
}

/**
 * Obtiene las transacciones de venta realizadas en un período
 * @param {string} userId - ID del usuario
 * @param {Date} startDate - Fecha de inicio del período
 * @param {Date} endDate - Fecha de fin del período
 * @param {string[]} accountIds - IDs de cuentas a filtrar
 * @returns {Promise<Object>} Ventas agrupadas por activo con P&L
 */
async function getSellTransactionsInPeriod(userId, startDate, endDate, accountIds = []) {
  const startDateStr = startDate.toISOString().split('T')[0];
  const endDateStr = endDate.toISOString().split('T')[0];
  
  console.log(`[Attribution] Buscando ventas entre ${startDateStr} y ${endDateStr}`);
  
  // Obtener cuentas del usuario si no se especifican
  let targetAccountIds = accountIds;
  if (accountIds.length === 0 || accountIds.includes('overall')) {
    const accountsSnapshot = await db.collection('portfolioAccounts')
      .where('userId', '==', userId)
      .where('isActive', '==', true)
      .get();
    targetAccountIds = accountsSnapshot.docs.map(d => d.id);
  }
  
  // Obtener TODAS las transacciones de venta y filtrar en memoria
  // (Evita necesitar índice compuesto type+date)
  const transactionsSnapshot = await db.collection('transactions')
    .where('type', '==', 'sell')
    .get();
  
  // Filtrar por fecha y cuentas del usuario
  const userSellTransactions = transactionsSnapshot.docs
    .map(doc => ({ id: doc.id, ...doc.data() }))
    .filter(t => {
      // Filtrar por fecha
      if (t.date < startDateStr || t.date > endDateStr) return false;
      // Filtrar por cuentas del usuario
      return targetAccountIds.includes(t.portfolioAccountId);
    });
  
  console.log(`[Attribution] Encontradas ${userSellTransactions.length} ventas en el período`);
  
  // Agrupar por activo
  const sellsByAsset = {};
  for (const tx of userSellTransactions) {
    const assetKey = `${tx.assetName}_${tx.assetType || 'stock'}`;
    if (!sellsByAsset[assetKey]) {
      sellsByAsset[assetKey] = {
        ticker: tx.assetName,
        assetType: tx.assetType || 'stock',
        totalRealizedPnL: 0,
        totalSold: 0,
        transactions: []
      };
    }
    
    // Usar valuePnL si está disponible, sino es 0
    const pnl = tx.valuePnL || 0;
    sellsByAsset[assetKey].totalRealizedPnL += pnl;
    sellsByAsset[assetKey].totalSold += (parseFloat(tx.amount) || 0) * (parseFloat(tx.price) || 0);
    sellsByAsset[assetKey].transactions.push({
      date: tx.date,
      amount: tx.amount,
      price: tx.price,
      pnl
    });
  }
  
  return sellsByAsset;
}

/**
 * Calcula las contribuciones de cada activo al rendimiento del portafolio
 * 
 * MEJORADO: Ahora incluye P&L realizada de ventas en el período.
 * 
 * @param {string} userId - ID del usuario
 * @param {string} period - Período de análisis ('YTD', '1M', '3M', etc.)
 * @param {string} currency - Moneda para los cálculos
 * @param {string[]} accountIds - IDs de cuentas a incluir o ['overall']
 * @returns {Promise<Object>} Resultado con atribuciones calculadas
 */
async function calculateContributions(userId, period, currency = 'USD', accountIds = ['overall']) {
  // Determinar si usamos overall o cuentas específicas
  const useOverall = accountIds.length === 0 || accountIds.includes('overall');
  
  // Para el cálculo, siempre usamos 'overall' que tiene los totales correctos
  // Pero si hay cuentas específicas, filtramos los activos después
  const accountId = 'overall';
  
  // Si hay cuentas específicas (no overall), obtener los activos permitidos
  let allowedAssetKeys = null;
  if (!useOverall && accountIds.length > 0) {
    allowedAssetKeys = new Set();
    
    // Obtener activos de cada cuenta seleccionada
    for (const accId of accountIds) {
      const accData = await getLatestPerformanceData(userId, accId);
      if (accData) {
        const accCurrencyData = accData[currency] || accData.USD || {};
        const accAssets = Object.keys(accCurrencyData.assetPerformance || {});
        accAssets.forEach(key => allowedAssetKeys.add(key));
      }
    }
    
    console.log(`[Attribution] Filtrando activos para ${accountIds.length} cuentas: ${allowedAssetKeys.size} activos permitidos`);
  }
  
  console.log(`[Attribution] Usando datos de cuenta: ${accountId} (input: ${accountIds.join(',')})`);
  
  // 1. Obtener documento más reciente (valores actuales)
  const latestData = await getLatestPerformanceData(userId, accountId);
  if (!latestData) {
    return {
      attributions: [],
      totalPortfolioValue: 0,
      totalPortfolioInvestment: 0,
      portfolioReturn: 0,
      error: 'No performance data found'
    };
  }
  
  const latestDate = latestData.id || latestData.date;
  const currencyData = latestData[currency] || latestData.USD || {};
  const assetPerformance = currencyData.assetPerformance || {};
  
  // 2. Obtener valores de referencia
  // Si hay cuentas específicas, calcular totales solo de los activos permitidos
  let totalPortfolioValue = 0;
  let totalPortfolioInvestment = 0;
  
  if (allowedAssetKeys) {
    // Sumar solo activos de las cuentas seleccionadas
    for (const [assetKey, assetData] of Object.entries(assetPerformance)) {
      if (allowedAssetKeys.has(assetKey)) {
        totalPortfolioValue += assetData.totalValue || 0;
        totalPortfolioInvestment += assetData.totalInvestment || 0;
      }
    }
    console.log(`[Attribution] Valor filtrado para ${accountIds.length} cuentas: $${totalPortfolioValue.toFixed(2)}`);
  } else {
    totalPortfolioValue = currencyData.totalValue || 0;
    totalPortfolioInvestment = currencyData.totalInvestment || 0;
  }
  
  const portfolioROI = totalPortfolioInvestment > 0 
    ? ((totalPortfolioValue - totalPortfolioInvestment) / totalPortfolioInvestment) * 100 
    : 0;
  
  // 3. Obtener datos de inicio del período
  const { getPeriodStartDate } = require('./types');
  const periodStartDate = getPeriodStartDate(period);
  const periodStartStr = periodStartDate.toISOString().split('T')[0];
  
  const startData = await findNearestPerformanceData(userId, periodStartStr, accountId, 'asc');
  const startCurrencyData = startData?.[currency] || startData?.USD || {};
  const startAssetPerformance = startCurrencyData.assetPerformance || {};
  
  // Calcular valor inicial solo de activos permitidos
  let startTotalValue = 0;
  if (allowedAssetKeys) {
    for (const [assetKey, assetData] of Object.entries(startAssetPerformance)) {
      if (allowedAssetKeys.has(assetKey)) {
        startTotalValue += assetData.totalValue || 0;
      }
    }
  } else {
    startTotalValue = startCurrencyData.totalValue || totalPortfolioValue;
  }
  
  // 4. NUEVO: Obtener ventas realizadas en el período
  const sellsByAsset = await getSellTransactionsInPeriod(
    userId, 
    periodStartDate, 
    new Date(), 
    accountIds
  );
  
  // Calcular P&L realizada total
  let totalRealizedPnL = 0;
  for (const assetData of Object.values(sellsByAsset)) {
    totalRealizedPnL += assetData.totalRealizedPnL;
  }
  console.log(`[Attribution] Total P&L realizada en período: $${totalRealizedPnL.toFixed(2)}`);
  
  // 5. Calcular contribuciones para cada activo activo
  const attributions = [];
  const processedAssetKeys = new Set();
  
  for (const [assetKey, assetData] of Object.entries(assetPerformance)) {
    // FILTRO: Si hay cuentas específicas, solo incluir activos de esas cuentas
    if (allowedAssetKeys && !allowedAssetKeys.has(assetKey)) {
      continue; // Saltar activos que no pertenecen a las cuentas seleccionadas
    }
    
    const parts = assetKey.split('_');
    const ticker = parts[0];
    const type = parts[1] || 'stock';
    
    const assetValue = assetData.totalValue || 0;
    const assetInvestment = assetData.totalInvestment || 0;
    const assetROI = assetData.totalROI || 0;
    
    // Calcular peso actual
    const weight = totalPortfolioValue > 0 ? assetValue / totalPortfolioValue : 0;
    
    // CONTRIBUCIÓN = peso × ROI del activo
    let contribution = weight * assetROI;
    
    // P&L no realizada
    const unrealizedPnL = assetData.unrealizedProfitAndLoss || 0;
    
    // NUEVO: Agregar P&L realizada de ventas parciales en el período
    const sellData = sellsByAsset[assetKey];
    const realizedPnLInPeriod = sellData?.totalRealizedPnL || 0;
    const totalSoldAmount = sellData?.totalSold || 0;
    
    // Si hubo ventas parciales, ajustar la contribución
    if (realizedPnLInPeriod !== 0) {
      // La contribución de la venta es la P&L realizada como % del valor inicial del portafolio
      const realizedContribution = startTotalValue > 0 
        ? (realizedPnLInPeriod / startTotalValue) * 100 
        : 0;
      contribution += realizedContribution;
      console.log(`[Attribution] ${ticker}: Agregando contribución realizada: ${realizedContribution.toFixed(2)}pp ($${realizedPnLInPeriod.toFixed(2)})`);
    }
    
    const contributionAbsolute = unrealizedPnL + realizedPnLInPeriod;
    const valueChange = assetValue - assetInvestment + realizedPnLInPeriod;
    
    // Calcular ROI ajustado para activos con ventas parciales
    // ROI = (P&L Total) / (Inversión actual + Costo de lo vendido)
    // Costo de lo vendido = totalSold - realizedPnL
    let displayROI = assetROI;
    if (realizedPnLInPeriod !== 0 && totalSoldAmount > 0) {
      const costOfSold = totalSoldAmount - realizedPnLInPeriod;
      const totalInvestmentIncludingSold = assetInvestment + costOfSold;
      const totalPnL = unrealizedPnL + realizedPnLInPeriod;
      displayROI = totalInvestmentIncludingSold > 0 
        ? (totalPnL / totalInvestmentIncludingSold) * 100 
        : assetROI;
    }
    
    attributions.push({
      assetKey,
      ticker,
      name: ticker,
      sector: 'Unknown',
      type: type.toLowerCase(),
      status: 'active',
      weightStart: weight,
      weightEnd: weight,
      weightAverage: weight,
      returnPercent: displayROI, // ROI ajustado que incluye ganancias realizadas
      contribution,
      contributionAbsolute,
      valueStart: assetInvestment,
      valueEnd: assetValue,
      valueChange,
      hasPartialSales: realizedPnLInPeriod !== 0,
      partialSalesPnL: realizedPnLInPeriod !== 0 ? realizedPnLInPeriod : undefined,
      partialSalesCount: sellData?.transactions?.length,
      _source: {
        totalValue: assetValue,
        totalInvestment: assetInvestment,
        totalROI: assetROI,
        unrealizedPnL,
        realizedPnLInPeriod
      }
    });
    
    processedAssetKeys.add(assetKey);
  }
  
  // 6. NUEVO: Agregar activos que fueron vendidos completamente en el período
  for (const [assetKey, sellData] of Object.entries(sellsByAsset)) {
    if (processedAssetKeys.has(assetKey)) continue; // Ya procesado arriba
    
    const realizedPnL = sellData.totalRealizedPnL;
    const costBasis = sellData.totalSold - realizedPnL; // Costo aproximado
    
    // Contribución = P&L realizada como % del valor inicial del portafolio
    const contribution = startTotalValue > 0 
      ? (realizedPnL / startTotalValue) * 100 
      : 0;
    
    // ROI de la posición cerrada = P&L / Costo
    const returnPercent = costBasis > 0 ? (realizedPnL / costBasis) * 100 : 0;
    
    // Peso que tenía el activo al momento de venderlo (aproximado)
    const weightAtSale = startTotalValue > 0 ? costBasis / startTotalValue : 0;
    
    // Solo agregar si la contribución es significativa
    if (Math.abs(contribution) < 0.01) continue;
    
    console.log(`[Attribution] ${sellData.ticker}: Posición cerrada con contribución: ${contribution.toFixed(2)}pp ($${realizedPnL.toFixed(2)}) ROI: ${returnPercent.toFixed(1)}%`);
    
    attributions.push({
      assetKey: `${assetKey}_sold`,
      ticker: sellData.ticker,
      name: sellData.ticker,
      sector: 'Unknown',
      type: sellData.assetType.toLowerCase(),
      status: 'sold',
      weightStart: weightAtSale, // Peso aproximado al vender
      weightEnd: 0,
      weightAverage: weightAtSale / 2, // Aproximación
      returnPercent, // ROI de la posición cerrada
      contribution,
      contributionAbsolute: realizedPnL, // Valor absoluto de la ganancia/pérdida
      valueStart: costBasis,
      valueEnd: 0,
      valueChange: realizedPnL,
      hasPartialSales: false,
      _source: {
        realizedPnL,
        costBasis,
        transactionCount: sellData.transactions.length
      }
    });
  }
  
  // 7. Ordenar por contribución descendente
  attributions.sort((a, b) => b.contribution - a.contribution);
  
  // 8. Validar suma de contribuciones vs ROI del portafolio
  const sumOfContributions = attributions.reduce((sum, a) => sum + a.contribution, 0);
  const discrepancy = Math.abs(sumOfContributions - portfolioROI);
  
  // 9. Normalizar si hay discrepancia significativa (> 1pp)
  // NOTA: Solo normalizamos la contribución (puntos porcentuales), 
  // NO los valores absolutos (contributionAbsolute, valueChange) que son en USD
  let normalized = false;
  if (discrepancy > 1 && Math.abs(sumOfContributions) > 0.01) {
    const normalizationFactor = portfolioROI / sumOfContributions;
    for (const attr of attributions) {
      attr.contribution *= normalizationFactor;
      // NO normalizar contributionAbsolute ni valueChange - son valores absolutos en USD
    }
    normalized = true;
  }
  
  return {
    attributions,
    totalPortfolioValue,
    totalPortfolioInvestment,
    portfolioReturn: portfolioROI,
    latestDate,
    periodStartDate: periodStartStr,
    startTotalValue,
    sumOfContributions: normalized ? portfolioROI : sumOfContributions,
    discrepancy,
    normalized,
    // NUEVO: Info de ventas
    realizedPnL: {
      total: totalRealizedPnL,
      assetsWithSales: Object.keys(sellsByAsset).length,
      closedPositions: Object.values(sellsByAsset).filter(s => !processedAssetKeys.has(`${s.ticker}_${s.assetType}`)).length
    }
  };
}

/**
 * Enriquece las atribuciones con datos de currentPrices
 * @param {Array} attributions - Array de atribuciones
 * @returns {Promise<Array>} Atribuciones enriquecidas
 */
async function enrichWithCurrentPrices(attributions) {
  const tickers = [...new Set(attributions.map(a => a.ticker))];
  
  // Obtener precios en batches
  const pricesMap = new Map();
  for (let i = 0; i < tickers.length; i += 30) {
    const batch = tickers.slice(i, i + 30);
    const snapshot = await db.collection('currentPrices')
      .where('symbol', 'in', batch)
      .get();
    
    for (const doc of snapshot.docs) {
      const data = doc.data();
      pricesMap.set(data.symbol, {
        name: data.name || data.symbol,
        sector: data.sector || 'Unknown',
        logo: data.logo || null
      });
    }
  }
  
  // Enriquecer atribuciones
  for (const attr of attributions) {
    const priceData = pricesMap.get(attr.ticker);
    if (priceData) {
      attr.name = priceData.name;
      attr.sector = priceData.sector;
      attr.logo = priceData.logo;
    }
  }
  
  return attributions;
}

module.exports = {
  calculateContributions,
  enrichWithCurrentPrices,
  getLatestPerformanceData,
  findNearestPerformanceData,
  getPerformanceDataForDate,
  getSellTransactionsInPeriod
};
