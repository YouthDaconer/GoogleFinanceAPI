/**
 * Contribution Calculator Service
 * 
 * SRP: Responsabilidad única de calcular contribuciones de activos al rendimiento.
 * Usa datos de assetPerformance de Firestore (ROI personal del usuario).
 * 
 * MEJORADO: Ahora incluye P&L realizada de ventas en el período para
 * cálculos de atribución más precisos.
 * 
 * OPT-DEMAND-CLEANUP: enrichWithCurrentPrices ahora usa API Lambda en lugar de Firestore
 * 
 * @module services/attribution/contributionCalculator
 * @see docs/architecture/portfolio-attribution-coherence-analysis.md
 * @see docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
 */

const admin = require('../firebaseAdmin');
const db = admin.firestore();
// OPT-DEMAND-CLEANUP: Importar getQuotes para obtener datos de mercado
const { getQuotes } = require('../financeQuery');

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
    
    // FIX: Obtener activos directamente de la colección 'assets' que tiene el campo portfolioAccount
    // Los assets no tienen userId directamente, pero podemos filtrar por portfolioAccount
    try {
      // Consultar activos de cada cuenta seleccionada
      for (const accId of accountIds) {
        console.log(`[Attribution] Buscando activos para cuenta: ${accId}`);
        const assetsSnapshot = await db.collection('assets')
          .where('portfolioAccount', '==', accId)
          .where('isActive', '==', true)
          .get();
        
        console.log(`[Attribution] Encontrados ${assetsSnapshot.size} activos en cuenta ${accId}`);
        
        assetsSnapshot.docs.forEach(doc => {
          const asset = doc.data();
          // Construir la key del mismo formato que assetPerformance: {ticker}_{type}
          const assetKey = `${asset.name}_${asset.assetType || 'stock'}`;
          allowedAssetKeys.add(assetKey);
          console.log(`[Attribution] Asset permitido: ${assetKey} (${asset.name}, tipo: ${asset.assetType})`);
        });
      }
      
      console.log(`[Attribution] Filtrando activos para ${accountIds.length} cuentas: ${allowedAssetKeys.size} activos permitidos`);
      console.log(`[Attribution] AllowedAssetKeys: ${Array.from(allowedAssetKeys).join(', ')}`);
    } catch (error) {
      console.error(`[Attribution] Error obteniendo activos: ${error.message}`);
      // Fallback: intentar obtener de portfolioPerformance como antes
      for (const accId of accountIds) {
        const accData = await getLatestPerformanceData(userId, accId);
        if (accData) {
          const accCurrencyData = accData[currency] || accData.USD || {};
          const accAssets = Object.keys(accCurrencyData.assetPerformance || {});
          accAssets.forEach(key => allowedAssetKeys.add(key));
        }
      }
      console.log(`[Attribution] Fallback: ${allowedAssetKeys.size} activos de portfolioPerformance`);
    }
  }
  
  console.log(`[Attribution] Usando datos de cuenta: ${accountId} (input: ${accountIds.join(',')})`);
  console.log(`[Attribution] allowedAssetKeys es null? ${allowedAssetKeys === null}, size: ${allowedAssetKeys?.size || 0}`);  
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
  
  // =========================================================================
  // 5. Calcular contribuciones para cada activo activo
  // 
  // MÉTODO: Brinson Attribution simplificado
  // Contribución = (cambioValorActivo + P&L realizada) / valorInicialPortafolio × 100
  // 
  // Este método mide directamente cuánto contribuyó cada activo al rendimiento
  // total del portafolio, sin necesidad de obtener retornos de mercado.
  // =========================================================================
  
  const attributions = [];
  const processedAssetKeys = new Set();
  let skippedCount = 0;
  let includedCount = 0;
  
  console.log(`[Attribution] Procesando ${Object.keys(assetPerformance).length} activos de assetPerformance`);
  console.log(`[Attribution] Usando datos de inicio del período: ${startData?.id || periodStartStr}`);
  console.log(`[Attribution] Valor inicial del portafolio: $${startTotalValue.toFixed(2)}`);
  
  for (const [assetKey, assetData] of Object.entries(assetPerformance)) {
    // FILTRO: Si hay cuentas específicas, solo incluir activos de esas cuentas
    if (allowedAssetKeys && !allowedAssetKeys.has(assetKey)) {
      skippedCount++;
      continue; // Saltar activos que no pertenecen a las cuentas seleccionadas
    }
    
    includedCount++;
    const parts = assetKey.split('_');
    const ticker = parts[0];
    const type = parts[1] || 'stock';
    
    const assetValueEnd = assetData.totalValue || 0;
    const assetInvestment = assetData.totalInvestment || 0;
    const assetTotalROI = assetData.totalROI || 0; // ROI total desde compra (para referencia)
    
    // =========================================================================
    // MÉTODO BRINSON SIMPLIFICADO (sin dependencia de currentPrices)
    // 
    // Contribución al rendimiento = cambioValor / valorInicialPortafolio × 100
    // 
    // Este método es correcto tanto para assets sin cambios como con compras/ventas
    // porque mide directamente cuánto contribuyó al rendimiento del portafolio.
    // =========================================================================
    const startAssetData = startAssetPerformance[assetKey];
    const assetValueStart = startAssetData?.totalValue || 0;
    const unitsStart = startAssetData?.units || 0;
    const unitsEnd = assetData.units || 0;
    
    // P&L realizada de ventas parciales en el período
    const sellData = sellsByAsset[assetKey];
    const realizedPnLInPeriod = sellData?.totalRealizedPnL || 0;
    const totalSoldAmount = sellData?.totalSold || 0;
    
    // =========================================================================
    // CALCULAR CONTRIBUCIÓN CORRECTAMENTE
    // 
    // Para activos que EXISTÍAN al inicio del período:
    //   Contribución = (valorFinal - valorInicial + PnL realizada) / valorInicialPortafolio
    //   Esto mide cuánto aportó el cambio de precio del activo al rendimiento
    //
    // Para activos NUEVOS (comprados durante el período):
    //   El "cambio de valor" no es ganancia/pérdida, es inyección de capital nuevo
    //   Contribución = unrealizedPnL / valorInicialPortafolio
    //   Esto mide la ganancia/pérdida real desde la compra
    // =========================================================================
    
    const isNewAsset = assetValueStart === 0 && unitsStart === 0;
    const unrealizedPnL = assetData.unrealizedProfitAndLoss || (assetValueEnd - assetInvestment);
    const hasUnitChange = Math.abs(unitsEnd - unitsStart) > 0.0001;
    const hasPartialSales = unitsEnd < unitsStart - 0.0001;  // Vendió unidades
    const hasPartialBuys = unitsEnd > unitsStart + 0.0001;   // Compró unidades adicionales
    
    // =========================================================================
    // FIX-ATTRIBUTION-001: CALCULAR RETORNO DEL PERÍODO
    // 
    // El retorno del período es el cambio de PRECIO del activo.
    // 
    // PROBLEMA: Cuando hay ventas, el cálculo valueEnd/unitsEnd vs valueStart/unitsStart
    // puede dar resultados incorrectos porque los valores incluyen diferentes lotes.
    // 
    // SOLUCIÓN: Solo calculamos el retorno del precio cuando NO hay ventas.
    // Para activos con ventas, mantenemos periodReturn = 0 y usamos la contribución
    // basada en el cambio total del valor + P&L realizado.
    // =========================================================================
    let periodReturn = 0;
    let priceStart = 0;
    let priceEnd = 0;
    
    if (assetValueStart > 0 && unitsStart > 0) {
      priceStart = assetValueStart / unitsStart;
      
      if (assetValueEnd > 0 && unitsEnd > 0) {
        // FIX-ATTRIBUTION-002: Calcular priceEnd siempre que tengamos unidades al final
        // El precio actual es el mismo para todas las unidades (ya sean originales o nuevas)
        priceEnd = assetValueEnd / unitsEnd;
        periodReturn = ((priceEnd - priceStart) / priceStart) * 100;
      } else if (hasPartialSales && unitsEnd === 0) {
        // Vendió TODAS las unidades: priceEnd no aplica
        priceEnd = 0;
        periodReturn = 0; // Se calculará basado en el P&L realizado
      }
    } else if (assetValueEnd > 0 && assetInvestment > 0) {
      // Asset nuevo en el período: usar el ROI desde la compra
      periodReturn = assetTotalROI;
    }
    
    // =========================================================================
    // FIX-ATTRIBUTION-002: CALCULAR CONTRIBUCIÓN CORRECTAMENTE
    // 
    // La contribución mide cuánto del rendimiento del portafolio se debe a este activo.
    // La fórmula correcta SIEMPRE aísla el EFECTO DEL CAMBIO DE PRECIO sobre las
    // unidades que teníamos al inicio del período.
    // 
    // CASOS:
    // 1. Activo nuevo: contribution = unrealizedPnL / startTotalValue
    // 2. Activo existente (con o sin cashflows):
    //    contribution = [(priceEnd - priceStart) × unitsStart + realizedPnL] / startTotalValue
    //    Esto aísla correctamente el efecto del cambio de precio
    // =========================================================================
    
    let totalChange;
    if (isNewAsset) {
      // Activo nuevo: la contribución es el P&L desde la compra
      totalChange = unrealizedPnL + realizedPnLInPeriod;
    } else if (priceStart > 0) {
      // FIX-ATTRIBUTION-002: Para CUALQUIER activo existente (compras, ventas, o sin cambio)
      // Usar la misma fórmula: efecto del cambio de precio sobre unidades iniciales
      // Esto aísla correctamente el rendimiento y evita incluir cashflows como rendimiento
      const priceChange = priceEnd - priceStart;
      const valueChangeFromPrice = priceChange * unitsStart;
      totalChange = valueChangeFromPrice + realizedPnLInPeriod;
    } else {
      // Fallback para casos edge (ej: precio inicial 0)
      const periodValueChange = assetValueEnd - assetValueStart;
      totalChange = periodValueChange + realizedPnLInPeriod;
    }
    
    // CONTRIBUCIÓN = cambioTotal / valorInicialPortafolio × 100
    const contribution = startTotalValue > 0 
      ? (totalChange / startTotalValue) * 100 
      : 0;
    
    // Calcular peso actual (usando peso al final del período)
    const weight = totalPortfolioValue > 0 ? assetValueEnd / totalPortfolioValue : 0;
    
    console.log(`[Attribution] ${ticker}: startVal=$${assetValueStart.toFixed(2)}, endVal=$${assetValueEnd.toFixed(2)}, units=${unitsStart.toFixed(4)}->${unitsEnd.toFixed(4)}${hasUnitChange ? ' (cambio)' : ''}${isNewAsset ? ' (NUEVO)' : ''}, periodReturn=${periodReturn.toFixed(2)}%, weight=${(weight*100).toFixed(2)}%, contribution=${contribution.toFixed(4)}pp`);
    
    // ROI para mostrar: usar el retorno del período calculado
    let displayROI = periodReturn;
    if (realizedPnLInPeriod !== 0 && totalSoldAmount > 0) {
      // Ajustar ROI si hubo ventas parciales para incluir P&L realizada
      const costOfSold = totalSoldAmount - realizedPnLInPeriod;
      const totalInvestmentIncludingSold = assetInvestment + costOfSold;
      const totalPnL = unrealizedPnL + realizedPnLInPeriod;
      displayROI = totalInvestmentIncludingSold > 0 
        ? (totalPnL / totalInvestmentIncludingSold) * 100 
        : periodReturn;
    }
    
    attributions.push({
      assetKey,
      ticker,
      name: ticker, // Se puede enriquecer después desde frontend con currentPrices on-demand
      sector: 'Unknown',
      logo: null,
      type: type.toLowerCase(),
      status: 'active',
      weightStart: weight,
      weightEnd: weight,
      weightAverage: weight,
      returnPercent: displayROI, // Retorno del período (basado en precio si hubo cambio de unidades)
      contribution,
      contributionAbsolute: totalChange,
      valueStart: assetValueStart, // Valor al inicio del período
      valueEnd: assetValueEnd,     // Valor al final del período
      valueChange: totalChange,
      hasUnitChange,               // Flag: hubo compras/ventas durante el período
      isNewAsset,                  // Flag: activo comprado durante el período (no existía al inicio)
      hasPartialSales: realizedPnLInPeriod !== 0,
      partialSalesPnL: realizedPnLInPeriod !== 0 ? realizedPnLInPeriod : undefined,
      partialSalesCount: sellData?.transactions?.length,
      _source: {
        totalValue: assetValueEnd,
        totalInvestment: assetInvestment,
        totalROI: assetTotalROI, // ROI total desde compra (para referencia)
        periodReturn: periodReturn, // Retorno del período (basado en precio)
        unitsStart,
        unitsEnd,
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
  
  console.log(`[Attribution] Resumen: ${includedCount} activos incluidos, ${skippedCount} activos filtrados de ${Object.keys(assetPerformance).length} totales`);
  console.log(`[Attribution] Attributions generadas: ${attributions.length}`);
  
  // 8. Calcular suma de contribuciones (esto es el rendimiento del período basado en Brinson)
  // La suma de las contribuciones ES el rendimiento del período (cambio de valor / valor inicial)
  const sumOfContributions = attributions.reduce((sum, a) => sum + a.contribution, 0);
  
  // NOTA: NO normalizamos aquí. La normalización se hace en attributionService.js
  // usando el TWR del período que es más preciso que el portfolioROI.
  // El portfolioROI es el ROI total desde compra, no el rendimiento del período.
  const normalized = false;
  const discrepancy = Math.abs(sumOfContributions - portfolioROI);
  
  console.log(`[Attribution] Sum of contributions: ${sumOfContributions.toFixed(4)}%, portfolioROI: ${portfolioROI.toFixed(2)}%, discrepancy: ${discrepancy.toFixed(2)}pp`);
  
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
 * OPT-DEMAND-CLEANUP: Enriquece las atribuciones con datos del API Lambda
 * 
 * Migrado desde Firestore (colección currentPrices) para cumplir con 
 * arquitectura on-demand pura.
 * 
 * @param {Array} attributions - Array de atribuciones
 * @returns {Promise<Array>} Atribuciones enriquecidas
 */
async function enrichWithCurrentPrices(attributions) {
  const tickers = [...new Set(attributions.map(a => a.ticker))];
  
  if (tickers.length === 0) {
    return attributions;
  }
  
  // OPT-DEMAND-CLEANUP: Usar API Lambda en lugar de Firestore
  const pricesMap = new Map();
  
  try {
    const symbolsString = tickers.join(',');
    const quotes = await getQuotes(symbolsString);
    
    if (quotes && Array.isArray(quotes)) {
      for (const quote of quotes) {
        if (quote && quote.symbol) {
          pricesMap.set(quote.symbol, {
            name: quote.name || quote.shortName || quote.symbol,
            sector: quote.sector || 'Unknown',
            logo: quote.logo || null
          });
        }
      }
    } else if (quotes && typeof quotes === 'object') {
      // Formato objeto { AAPL: {...}, MSFT: {...} }
      for (const [symbol, quote] of Object.entries(quotes)) {
        if (quote) {
          pricesMap.set(symbol, {
            name: quote.name || quote.shortName || symbol,
            sector: quote.sector || 'Unknown',
            logo: quote.logo || null
          });
        }
      }
    }
  } catch (error) {
    console.warn('[enrichWithCurrentPrices] Error fetching from API, continuing without enrichment:', error.message);
    // No lanzar error, continuar sin enriquecer
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
