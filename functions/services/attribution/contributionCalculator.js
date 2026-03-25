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
  
  // FIX-PERF-001: Query por cuenta en paralelo en vez de full-scan de TODAS las transacciones sell.
  // La query anterior descargaba todas las ventas de todos los usuarios y filtraba en memoria,
  // lo que causaba tiempos de ~14s y posibles timeouts (HTTP 500) para períodos largos como 1Y.
  const accountQueries = targetAccountIds.map(accId =>
    db.collection('transactions')
      .where('portfolioAccountId', '==', accId)
      .where('type', '==', 'sell')
      .get()
  );
  
  const snapshots = await Promise.all(accountQueries);
  
  // Filtrar por fecha en memoria (evita necesitar índice compuesto)
  const userSellTransactions = [];
  for (const snapshot of snapshots) {
    for (const doc of snapshot.docs) {
      const data = doc.data();
      if (data.date >= startDateStr && data.date <= endDateStr) {
        userSellTransactions.push({ id: doc.id, ...data });
      }
    }
  }
  
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
async function calculateContributions(userId, period, currency = 'USD', accountIds = ['overall'], dateRange) {
  // Determinar si usamos overall o cuentas específicas
  const useOverall = accountIds.length === 0 || accountIds.includes('overall');
  
  // =========================================================================
  // FIX-MULTI-ACCOUNT-001: Cuando hay cuentas específicas, debemos agregar
  // los datos de cada cuenta individual, NO usar el documento 'overall'
  // 
  // El documento 'overall' tiene valores agregados de TODAS las cuentas.
  // Si un ticker existe en cuentas no seleccionadas, el valor en 'overall'
  // incluiría esas cuentas incorrectamente.
  // 
  // Solución: Obtener portfolioPerformance de cada cuenta seleccionada
  // y agregar los datos manualmente.
  // =========================================================================
  
  // Variables para datos agregados de cuentas específicas
  let aggregatedAssetPerformance = {};
  let aggregatedStartAssetPerformance = {};
  let aggregatedTotalValue = 0;
  let aggregatedTotalInvestment = 0;
  let aggregatedStartTotalValue = 0;
  let latestDateUsed = null;
  let startDateUsed = null;
  
  // Si hay cuentas específicas (no overall), obtener los activos permitidos Y agregar datos
  let allowedAssetKeys = null;
  
  if (!useOverall && accountIds.length > 0) {
    allowedAssetKeys = new Set();
    
    console.log(`[Attribution] FIX-MULTI-ACCOUNT-001: Agregando datos de ${accountIds.length} cuentas específicas`);
    
    const { getPeriodStartDate } = require('./types');
    const periodStartDate = dateRange?.startDate || getPeriodStartDate(period);
    const periodStartStr = periodStartDate.toISOString().split('T')[0];
    const multiAccEndStr = dateRange?.endDate ? dateRange.endDate.toISOString().split('T')[0] : null;
    
    // FIX-PERF-002: Paralelizar queries por cuenta con Promise.all
    const accountDataPromises = accountIds.map(async (accId) => {
      console.log(`[Attribution] Procesando cuenta: ${accId}`);
      
      const accLatestData = multiAccEndStr
        ? await findNearestPerformanceData(userId, multiAccEndStr, accId, 'desc')
        : await getLatestPerformanceData(userId, accId);
      
      const accStartData = await findNearestPerformanceData(userId, periodStartStr, accId, 'asc');
      
      return { accId, accLatestData, accStartData };
    });
    
    const accountResults = await Promise.all(accountDataPromises);
    
    // Agregar resultados de cada cuenta
    for (const { accId, accLatestData, accStartData } of accountResults) {
      if (!accLatestData) {
        console.log(`[Attribution] Cuenta ${accId}: sin datos de performance`);
        continue;
      }
      
      const accDate = accLatestData.id || accLatestData.date;
      if (!latestDateUsed || accDate > latestDateUsed) {
        latestDateUsed = accDate;
      }
      
      const accCurrencyData = accLatestData[currency] || accLatestData.USD || {};
      const accAssetPerformance = accCurrencyData.assetPerformance || {};
      
      // Agregar totales de esta cuenta
      aggregatedTotalValue += accCurrencyData.totalValue || 0;
      aggregatedTotalInvestment += accCurrencyData.totalInvestment || 0;
      
      // Agregar assetPerformance de esta cuenta
      for (const [assetKey, assetData] of Object.entries(accAssetPerformance)) {
        allowedAssetKeys.add(assetKey);
        
        if (!aggregatedAssetPerformance[assetKey]) {
          // Primera vez que vemos este activo - copiar datos
          aggregatedAssetPerformance[assetKey] = { ...assetData };
        } else {
          // Activo ya existe en otra cuenta - sumar valores
          const existing = aggregatedAssetPerformance[assetKey];
          existing.totalValue = (existing.totalValue || 0) + (assetData.totalValue || 0);
          existing.totalInvestment = (existing.totalInvestment || 0) + (assetData.totalInvestment || 0);
          existing.units = (existing.units || 0) + (assetData.units || 0);
          existing.unrealizedProfitAndLoss = (existing.unrealizedProfitAndLoss || 0) + (assetData.unrealizedProfitAndLoss || 0);
          // El ROI se recalcula como promedio ponderado después
        }
      }
      
      // FIX-PERF-002: accStartData ya fue obtenida en paralelo arriba
      if (accStartData) {
        const accStartDate = accStartData.id || accStartData.date;
        if (!startDateUsed || accStartDate < startDateUsed) {
          startDateUsed = accStartDate;
        }
        
        const accStartCurrencyData = accStartData[currency] || accStartData.USD || {};
        const accStartAssetPerformance = accStartCurrencyData.assetPerformance || {};
        
        // Agregar totales iniciales de esta cuenta
        aggregatedStartTotalValue += accStartCurrencyData.totalValue || 0;
        
        // Agregar startAssetPerformance de esta cuenta
        for (const [assetKey, assetData] of Object.entries(accStartAssetPerformance)) {
          if (!aggregatedStartAssetPerformance[assetKey]) {
            aggregatedStartAssetPerformance[assetKey] = { ...assetData };
          } else {
            const existing = aggregatedStartAssetPerformance[assetKey];
            existing.totalValue = (existing.totalValue || 0) + (assetData.totalValue || 0);
            existing.totalInvestment = (existing.totalInvestment || 0) + (assetData.totalInvestment || 0);
            existing.units = (existing.units || 0) + (assetData.units || 0);
          }
        }
      }
      
      console.log(`[Attribution] Cuenta ${accId}: ${Object.keys(accAssetPerformance).length} activos, valor=$${(accCurrencyData.totalValue || 0).toFixed(2)}`);
    }
    
    // Recalcular ROI para activos agregados
    for (const [assetKey, assetData] of Object.entries(aggregatedAssetPerformance)) {
      if (assetData.totalInvestment > 0) {
        assetData.totalROI = ((assetData.totalValue - assetData.totalInvestment) / assetData.totalInvestment) * 100;
      }
    }
    
    console.log(`[Attribution] FIX-MULTI-ACCOUNT-001: Agregados ${Object.keys(aggregatedAssetPerformance).length} activos únicos`);
    console.log(`[Attribution] Valor total agregado: $${aggregatedTotalValue.toFixed(2)}, Inversión: $${aggregatedTotalInvestment.toFixed(2)}`);
    console.log(`[Attribution] Valor inicial agregado: $${aggregatedStartTotalValue.toFixed(2)}`);
  }
  
  // Para overall, usar el flujo original
  const accountId = useOverall ? 'overall' : accountIds[0]; // Solo para fallback
  
  console.log(`[Attribution] Usando datos de cuenta: ${accountId} (input: ${accountIds.join(',')})`);
  console.log(`[Attribution] allowedAssetKeys es null? ${allowedAssetKeys === null}, size: ${allowedAssetKeys?.size || 0}`);
  
  // =========================================================================
  // FIX-GHOST-ASSETS: Obtener activos activos del usuario para validar fantasmas
  //
  // Esto nos permite detectar activos que aparecen en portfolioPerformance
  // pero no existen como activos activos (son datos corruptos/fantasmas).
  // =========================================================================
  const activeAssetKeys = new Set();
  try {
    // Obtener todas las cuentas del usuario
    const accountsSnapshot = await db.collection('portfolioAccounts')
      .where('userId', '==', userId)
      .where('isActive', '==', true)
      .get();
    
    const userAccountIds = accountsSnapshot.docs.map(doc => doc.id);
    
    // FIX-PERF-001: Consultar activos de todas las cuentas en paralelo
    const assetQueries = userAccountIds.map(accId =>
      db.collection('assets')
        .where('portfolioAccount', '==', accId)
        .where('isActive', '==', true)
        .get()
    );
    
    const assetSnapshots = await Promise.all(assetQueries);
    
    for (const assetsSnapshot of assetSnapshots) {
      assetsSnapshot.docs.forEach(doc => {
        const asset = doc.data();
        if (asset.units > 0) {
          const assetKey = `${asset.name}_${asset.assetType || 'stock'}`;
          activeAssetKeys.add(assetKey);
        }
      });
    }
    console.log(`[Attribution] Activos activos del usuario: ${activeAssetKeys.size}`);
  } catch (error) {
    console.warn(`[Attribution] Error obteniendo activos activos: ${error.message}`);
  }
  
  // =========================================================================
  // FIX-MULTI-ACCOUNT-001: Usar datos agregados cuando hay cuentas específicas
  // =========================================================================
  let assetPerformance;
  let startAssetPerformance;
  let totalPortfolioValue;
  let totalPortfolioInvestment;
  let startTotalValue;
  let latestDate;
  
  const { getPeriodStartDate } = require('./types');
  const periodStartDate = dateRange?.startDate || getPeriodStartDate(period);
  const periodStartStr = periodStartDate.toISOString().split('T')[0];
  const periodEndDate = dateRange?.endDate || new Date();
  const periodEndStr = dateRange?.endDate ? dateRange.endDate.toISOString().split('T')[0] : null;

  if (!useOverall && allowedAssetKeys && Object.keys(aggregatedAssetPerformance).length > 0) {
    // CUENTAS ESPECÍFICAS: Usar datos agregados
    console.log(`[Attribution] Usando datos AGREGADOS de ${accountIds.length} cuentas`);
    
    assetPerformance = aggregatedAssetPerformance;
    startAssetPerformance = aggregatedStartAssetPerformance;
    totalPortfolioValue = aggregatedTotalValue;
    totalPortfolioInvestment = aggregatedTotalInvestment;
    startTotalValue = aggregatedStartTotalValue > 0 ? aggregatedStartTotalValue : aggregatedTotalValue;
    latestDate = latestDateUsed;
    
  } else {
    // OVERALL: Usar documento overall (flujo original)
    // FEAT-UX-001: Si hay endDate, buscar datos en esa fecha, no la más reciente
    console.log(`[Attribution] Usando datos de documento 'overall'${periodEndStr ? ` (hasta ${periodEndStr})` : ''}`);
    
    const latestData = periodEndStr
      ? await findNearestPerformanceData(userId, periodEndStr, 'overall', 'desc')
      : await getLatestPerformanceData(userId, 'overall');
    if (!latestData) {
      return {
        attributions: [],
        totalPortfolioValue: 0,
        totalPortfolioInvestment: 0,
        portfolioReturn: 0,
        error: 'No performance data found'
      };
    }
    
    latestDate = latestData.id || latestData.date;
    const currencyData = latestData[currency] || latestData.USD || {};
    assetPerformance = currencyData.assetPerformance || {};
    totalPortfolioValue = currencyData.totalValue || 0;
    totalPortfolioInvestment = currencyData.totalInvestment || 0;
    
    const startData = await findNearestPerformanceData(userId, periodStartStr, 'overall', 'asc');
    // FIX-BENCH-004: Capture actual start date from Firestore (not the requested date)
    if (startData) {
      startDateUsed = startData.id || startData.date;
    }
    const startCurrencyData = startData?.[currency] || startData?.USD || {};
    startAssetPerformance = startCurrencyData.assetPerformance || {};
    startTotalValue = startCurrencyData.totalValue || totalPortfolioValue;
  }
  
  const portfolioROI = totalPortfolioInvestment > 0 
    ? ((totalPortfolioValue - totalPortfolioInvestment) / totalPortfolioInvestment) * 100 
    : 0;
  
  // 4. NUEVO: Obtener ventas realizadas en el período
  const sellsByAsset = await getSellTransactionsInPeriod(
    userId, 
    periodStartDate, 
    periodEndDate, 
    accountIds
  );
  
  // Calcular P&L realizada total
  let totalRealizedPnL = 0;
  for (const assetData of Object.values(sellsByAsset)) {
    totalRealizedPnL += assetData.totalRealizedPnL;
  }
  console.log(`[Attribution] Total P&L realizada en período: $${totalRealizedPnL.toFixed(2)}`);
  
  // =========================================================================
  // 4.1 FIX-ATTR-CONSISTENCY: NO obtener precios de mercado actuales
  // 
  // IMPORTANTE: Las contribuciones históricas deben calcularse usando los datos
  // del documento de portfolioPerformance (cierre del día anterior), NO precios actuales.
  // 
  // Los precios actuales se usan SOLO para las contribuciones intraday, que se
  // calculan por separado en intradayCalculator.js y se suman después.
  // 
  // Esto garantiza:
  // - Suma de contribuciones históricas ≈ TWR histórico
  // - Suma de contribuciones históricas + intraday ≈ TWR ajustado
  // 
  // Para activos con cambio de unidades, usamos el precio implícito del documento:
  // priceEnd = valueEnd / unitsEnd (precio promedio ponderado al cierre)
  // =========================================================================
  
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
  let skippedGhostAssets = 0;
  let includedCount = 0;
  
  console.log(`[Attribution] Procesando ${Object.keys(assetPerformance).length} activos de assetPerformance`);
  console.log(`[Attribution] Usando datos de inicio del período: ${startDateUsed || periodStartStr}`);
  console.log(`[Attribution] Valor inicial del portafolio: $${startTotalValue.toFixed(2)}`);
  
  for (const [assetKey, assetData] of Object.entries(assetPerformance)) {
    // FILTRO: Ya no necesario cuando usamos datos agregados, pero mantenemos por si acaso
    if (allowedAssetKeys && !allowedAssetKeys.has(assetKey)) {
      skippedCount++;
      continue; // Saltar activos que no pertenecen a las cuentas seleccionadas
    }
    
    const parts = assetKey.split('_');
    const ticker = parts[0];
    const type = parts[1] || 'stock';
    
    // =========================================================================
    // FIX-GHOST-ASSETS: Detectar y excluir activos "fantasma"
    // 
    // Un activo fantasma es uno que:
    // 1. NO existía al inicio del período (unitsStart = 0)
    // 2. NO tiene transacciones que lo respalden
    // 3. NO está activo actualmente en la colección assets
    // 4. Pero aparece en el documento final con unidades > 0
    // 
    // Esto puede ocurrir por datos corruptos en portfolioPerformance.
    // 
    // IMPORTANTE: Si el activo ESTÁ activo actualmente (en activeAssetKeys),
    // es válido aunque no tenga transacciones (fue importado manualmente).
    // =========================================================================
    const startAssetData = startAssetPerformance[assetKey];
    const unitsStart = startAssetData?.units || 0;
    const unitsEnd = assetData.units || 0;
    const sellData = sellsByAsset[assetKey];
    const hasSellData = sellData && sellData.totalSold > 0;
    
    const isNewAssetInPeriod = unitsStart === 0 && unitsEnd > 0;
    const hasNoTransactionSupport = !hasSellData; // No hay ventas registradas
    const isCurrentlyActive = activeAssetKeys.has(assetKey); // Existe como activo activo
    
    // Si es "nuevo" pero no hay transacciones Y no está activo actualmente, es fantasma
    if (isNewAssetInPeriod && hasNoTransactionSupport && !isCurrentlyActive) {
      console.warn(`[Attribution] ⚠️ Activo fantasma detectado: ${ticker} - apareció con ${unitsEnd} unidades sin respaldo (inactivo en assets)`);
      skippedGhostAssets++;
      continue;
    }
    
    includedCount++;
    
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
    const assetValueStart = startAssetData?.totalValue || 0;
    
    // P&L realizada de ventas parciales en el período
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
        // =========================================================================
        // FIX-ATTR-CONSISTENCY: Usar precio del documento de portfolioPerformance
        // 
        // Para contribuciones HISTÓRICAS, usamos el precio implícito del documento
        // (valueEnd / unitsEnd). Este es el precio al cierre del día anterior.
        // 
        // Las contribuciones INTRADAY (cambio desde cierre de ayer hasta ahora)
        // se calculan por separado en intradayCalculator.js usando precios actuales.
        // 
        // Esto garantiza que la suma de contribuciones coincida con el TWR del período.
        // =========================================================================
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
  
  console.log(`[Attribution] Resumen: ${includedCount} activos incluidos, ${skippedCount} filtrados por cuenta, ${skippedGhostAssets} fantasmas excluidos de ${Object.keys(assetPerformance).length} totales`);
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
    // FIX-BENCH-004: Return actual data start date, not the theoretical request date.
    // For ALL period, getPeriodStartDate returns 5y back but data may start much later.
    periodStartDate: startDateUsed || periodStartStr,
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
