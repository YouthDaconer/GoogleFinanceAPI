/**
 * Asset Handlers - Lógica de negocio para operaciones de assets
 * 
 * SCALE-CF-001: Handlers extraídos de assetOperations.js para consolidación
 * de Cloud Functions HTTP.
 * 
 * Estos handlers son llamados por el router unificado portfolioOperations.js
 * y contienen la lógica de negocio sin el wrapper onCall de Firebase.
 * 
 * @module handlers/assetHandlers
 * @see docs/stories/56.story.md
 */

const { HttpsError } = require("firebase-functions/v2/https");
const admin = require('../firebaseAdmin');
const db = admin.firestore();

// Importar funciones de invalidación de cache (consolidadas en cacheInvalidationService)
const { invalidatePerformanceCache, invalidateDistributionCache } = require('../cacheInvalidationService');

// Importar getQuotes para crear currentPrices de nuevos tickers
const { getQuotes } = require('../financeQuery');

// Importar generador de logos
const { generateLogoUrl } = require('../../utils/logoGenerator');

// ============================================================================
// UTILIDADES
// ============================================================================

/**
 * Utilidad para limpiar decimales en operaciones financieras
 * @param {number} num - Número a limpiar
 * @param {number} decimals - Cantidad de decimales (default: 8)
 * @returns {number} Número limpio
 */
const cleanDecimal = (num, decimals = 8) =>
  Number(Math.round(Number(num + "e" + decimals)) / 10 ** decimals);

/**
 * Valida que el usuario sea propietario de la cuenta de portafolio
 * @param {string} portfolioAccountId - ID de la cuenta
 * @param {string} userId - UID del usuario
 * @throws {HttpsError} Si el usuario no es propietario
 * @returns {Promise<object>} Datos de la cuenta
 */
const validateAccountOwnership = async (portfolioAccountId, userId) => {
  const accountRef = db.collection('portfolioAccounts').doc(portfolioAccountId);
  const accountDoc = await accountRef.get();

  if (!accountDoc.exists) {
    throw new HttpsError(
      'not-found',
      'La cuenta de portafolio no existe'
    );
  }

  const accountData = accountDoc.data();
  if (accountData.userId !== userId) {
    throw new HttpsError(
      'permission-denied',
      'No tienes permiso para operar en esta cuenta de portafolio'
    );
  }

  return { id: accountDoc.id, ...accountData };
};

/**
 * Valida que haya saldo suficiente en la cuenta
 * @param {object} account - Datos de la cuenta
 * @param {string} currency - Moneda a verificar
 * @param {number} requiredAmount - Monto requerido
 * @throws {HttpsError} Si el saldo es insuficiente
 */
const validateSufficientFunds = (account, currency, requiredAmount) => {
  const currentBalance = account.balances?.[currency] || 0;
  if (currentBalance < requiredAmount) {
    throw new HttpsError(
      'failed-precondition',
      `Saldo insuficiente. Disponible: ${currentBalance.toFixed(2)} ${currency}, Requerido: ${requiredAmount.toFixed(2)} ${currency}`
    );
  }
};

/**
 * @deprecated OPT-DEMAND-CLEANUP: Esta función ya NO debe usarse
 * 
 * La colección currentPrices está siendo deprecada. Los precios ahora
 * vienen exclusivamente del API Lambda on-demand.
 * 
 * @see docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
 * 
 * @param {string} symbol - Símbolo del ticker
 * @param {string} assetType - Tipo de activo
 * @returns {Promise<boolean>} Siempre retorna false (no-op)
 */
const ensureCurrentPriceExists = async (symbol, assetType) => {
  // OPT-DEMAND-CLEANUP: Función deprecada, no realiza ninguna operación
  console.log(`[ensureCurrentPriceExists] DEPRECADO - ${symbol} no se escribe a currentPrices (on-demand puro)`);
  return false;
};

// OPT-DEMAND-CLEANUP: Código legacy comentado para referencia durante transición
/*
const ensureCurrentPriceExists_LEGACY = async (symbol, assetType) => {
  const priceRef = db.collection('currentPrices').doc(symbol);
  const priceDoc = await priceRef.get();

  if (priceDoc.exists) {
    console.log(`[ensureCurrentPriceExists] ${symbol} ya existe en currentPrices`);
    return false;
  }

  console.log(`[ensureCurrentPriceExists] ${symbol} no existe, obteniendo quote del API...`);

  try {
    const quotes = await getQuotes(symbol);
    
    if (!quotes || quotes.length === 0) {
      console.warn(`[ensureCurrentPriceExists] No se obtuvo quote para ${symbol}`);
      return false;
    }

    const quote = quotes[0];
    
    const price = typeof quote.price === 'string' 
      ? parseFloat(quote.price.replace(/,/g, '')) 
      : parseFloat(quote.price);

    const currentPriceData = {
      symbol: symbol,
      price: price,
      lastUpdated: Date.now(),
      name: quote.name || symbol,
      type: assetType || 'stock',
      change: quote.change || null,
      percentChange: quote.percentChange || null,
      currency: quote.currency || 'USD',
      currencySymbol: quote.currencySymbol || '$',
      exchange: quote.exchange || null,
      exchangeName: quote.exchangeName || null,
    };

    const optionalFields = [
      'logo', 'website', 'open', 'high', 'low',
      'yearHigh', 'yearLow', 'volume', 'avgVolume',
      'marketCap', 'beta', 'pe', 'eps',
      'earningsDate', 'industry', 'sector', 'about', 'employees',
      'dividend', 'exDividend', 'yield', 'dividendDate',
      'threeMonthReturn', 'sixMonthReturn', 'ytdReturn',
      'threeYearReturn', 'yearReturn', 'fiveYearReturn',
      'country', 'city', 'fullExchangeName', 'quoteType'
    ];

    optionalFields.forEach(field => {
      if (quote[field] !== null && quote[field] !== undefined) {
        currentPriceData[field] = quote[field];
      }
    });

    if (!currentPriceData.logo) {
      const generatedLogo = generateLogoUrl(symbol, { 
        website: quote.website, 
        assetType: assetType 
      });
      if (generatedLogo) {
        currentPriceData.logo = generatedLogo;
      }
    }

    await priceRef.set(currentPriceData);
    console.log(`[ensureCurrentPriceExists] ✅ Creado currentPrices/${symbol}`);
    
    return true;
  } catch (error) {
    console.error(`[ensureCurrentPriceExists] Error al crear currentPrices para ${symbol}:`, error);
    return false;
  }
};
*/

// ============================================================================
// HANDLERS
// ============================================================================

/**
 * Crea un nuevo asset con transacción de compra y actualización de balance
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} context.auth - Información de autenticación
 * @param {Object} payload - Datos del asset
 * @returns {Promise<{success: boolean, assetId: string, transactionId: string}>}
 */
async function createAsset(context, payload) {
  const { auth } = context;
  const data = payload;
  
  console.log(`[assetHandlers][createAsset] userId: ${auth.uid}, ticker: ${data?.name}`);

  try {
    // 1. Validar datos requeridos
    const requiredFields = ['portfolioAccount', 'name', 'assetType', 'currency', 'units', 'unitValue', 'acquisitionDate'];
    for (const field of requiredFields) {
      if (data[field] === undefined || data[field] === null || data[field] === '') {
        throw new HttpsError('invalid-argument', `El campo ${field} es requerido`);
      }
    }

    // 2. Validar ownership de la cuenta
    const account = await validateAccountOwnership(data.portfolioAccount, auth.uid);

    // 3. Calcular costo total
    const units = cleanDecimal(Number(data.units));
    const unitValue = cleanDecimal(Number(data.unitValue));
    const commission = cleanDecimal(Number(data.commission) || 0);
    const totalCost = cleanDecimal(units * unitValue + commission);

    // 4. Validar saldo suficiente
    validateSufficientFunds(account, data.currency, totalCost);

    // 5. Ejecutar transacción atómica
    const batch = db.batch();

    // 5.1. Crear el asset
    const assetRef = db.collection('assets').doc();
    const assetData = {
      name: data.name,
      assetType: data.assetType,
      market: data.market || '',
      company: data.company || '',
      currency: data.currency,
      units: units,
      unitValue: unitValue,
      acquisitionDate: data.acquisitionDate,
      acquisitionDollarValue: cleanDecimal(Number(data.acquisitionDollarValue) || 1),
      defaultCurrencyForAdquisitionDollar: data.defaultCurrencyForAdquisitionDollar || 'USD',
      commission: commission,
      portfolioAccount: data.portfolioAccount,
      isActive: true,
      createdAt: new Date().toISOString(),
    };
    batch.set(assetRef, assetData);

    // 5.2. Crear transacción de compra
    const transactionRef = db.collection('transactions').doc();
    const transactionData = {
      assetId: assetRef.id,
      assetName: data.name,
      type: 'buy',
      amount: units,
      price: unitValue,
      currency: data.currency,
      date: data.acquisitionDate,
      portfolioAccountId: data.portfolioAccount,
      commission: commission,
      assetType: data.assetType,
      dollarPriceToDate: cleanDecimal(Number(data.acquisitionDollarValue) || 1),
      market: data.market || '',
      defaultCurrencyForAdquisitionDollar: data.defaultCurrencyForAdquisitionDollar || 'USD',
      userId: auth.uid,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    batch.set(transactionRef, transactionData);

    // 5.3. Actualizar balance de la cuenta
    const newBalance = cleanDecimal((account.balances?.[data.currency] || 0) - totalCost);
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccount);
    batch.update(accountRef, {
      [`balances.${data.currency}`]: newBalance,
    });

    // 6. Commit de la transacción
    await batch.commit();

    // 7. Crear currentPrices si es un ticker nuevo
    await ensureCurrentPriceExists(data.name, data.assetType);

    // 8. Invalidar caches
    await invalidatePerformanceCache(auth.uid);
    invalidateDistributionCache(auth.uid);

    console.log(`[assetHandlers][createAsset] Éxito - assetId: ${assetRef.id}`);

    return {
      success: true,
      assetId: assetRef.id,
      transactionId: transactionRef.id,
    };

  } catch (error) {
    console.error(`[assetHandlers][createAsset] Error - userId: ${auth.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al crear el activo: ${error.message}`);
  }
}

/**
 * Actualiza un asset existente con ajuste de balance
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de actualización
 * @returns {Promise<{success: boolean, assetId: string, balanceAdjustment: number}>}
 */
async function updateAsset(context, payload) {
  const { auth } = context;
  const data = payload;

  console.log(`[assetHandlers][updateAsset] userId: ${auth.uid}, assetId: ${data?.assetId}`);

  try {
    // 1. Validar datos requeridos
    if (!data.assetId) {
      throw new HttpsError('invalid-argument', 'assetId es requerido');
    }
    if (!data.updates || typeof data.updates !== 'object') {
      throw new HttpsError('invalid-argument', 'updates es requerido y debe ser un objeto');
    }

    // 2. Obtener el asset actual
    const assetRef = db.collection('assets').doc(data.assetId);
    const assetDoc = await assetRef.get();

    if (!assetDoc.exists) {
      throw new HttpsError('not-found', 'El asset no existe');
    }

    const oldAsset = assetDoc.data();

    // 3. Validar ownership via portfolioAccount
    const account = await validateAccountOwnership(oldAsset.portfolioAccount, auth.uid);

    // 3.1. Si está cambiando de cuenta, validar ownership de la nueva cuenta
    let newAccount = null;
    const isChangingAccount = data.updates.portfolioAccount && 
                              data.updates.portfolioAccount !== oldAsset.portfolioAccount;
    
    if (isChangingAccount) {
      newAccount = await validateAccountOwnership(data.updates.portfolioAccount, auth.uid);
      console.log(`[assetHandlers][updateAsset] Cambiando cuenta de ${oldAsset.portfolioAccount} a ${data.updates.portfolioAccount}`);
    }

    // 4. Calcular valores antiguos y nuevos
    const oldUnits = cleanDecimal(Number(oldAsset.units));
    const oldUnitValue = cleanDecimal(Number(oldAsset.unitValue));
    const oldCommission = cleanDecimal(Number(oldAsset.commission) || 0);
    const oldTotalValue = cleanDecimal(oldUnits * oldUnitValue + oldCommission);

    const newUnits = data.updates.units !== undefined 
      ? cleanDecimal(Number(data.updates.units)) 
      : oldUnits;
    const newUnitValue = data.updates.unitValue !== undefined 
      ? cleanDecimal(Number(data.updates.unitValue)) 
      : oldUnitValue;
    const newCommission = data.updates.commission !== undefined 
      ? cleanDecimal(Number(data.updates.commission)) 
      : oldCommission;
    const newTotalValue = cleanDecimal(newUnits * newUnitValue + newCommission);

    // 5. Calcular diferencia de valor
    const valueDifference = cleanDecimal(newTotalValue - oldTotalValue);
    const currency = data.updates.currency || oldAsset.currency;

    // 6. Verificar saldo suficiente si el nuevo valor es mayor
    if (valueDifference > 0) {
      const currentBalance = account.balances?.[currency] || 0;
      if (currentBalance < valueDifference) {
        throw new HttpsError(
          'failed-precondition',
          `Saldo insuficiente. Disponible: ${currentBalance.toFixed(2)} ${currency}, Requerido adicional: ${valueDifference.toFixed(2)} ${currency}`
        );
      }
    }

    // 7. Preparar los datos de actualización
    const updateData = { ...data.updates };
    
    if (updateData.units !== undefined) {
      updateData.units = cleanDecimal(Number(updateData.units));
    }
    if (updateData.unitValue !== undefined) {
      updateData.unitValue = cleanDecimal(Number(updateData.unitValue));
    }
    if (updateData.commission !== undefined) {
      updateData.commission = cleanDecimal(Number(updateData.commission));
    }
    if (updateData.acquisitionDollarValue !== undefined) {
      updateData.acquisitionDollarValue = cleanDecimal(Number(updateData.acquisitionDollarValue));
    }

    // 8. Ejecutar transacción atómica
    const batch = db.batch();

    batch.update(assetRef, updateData);

    // 8.0. Ajuste de balances
    if (isChangingAccount) {
      // Si cambia de cuenta: devolver valor a cuenta original, cobrar de cuenta nueva
      const oldAccountRef = db.collection('portfolioAccounts').doc(oldAsset.portfolioAccount);
      const newAccountRef = db.collection('portfolioAccounts').doc(data.updates.portfolioAccount);
      
      // Devolver el valor total a la cuenta original
      const returnBalance = cleanDecimal((account.balances?.[currency] || 0) + oldTotalValue);
      batch.update(oldAccountRef, {
        [`balances.${currency}`]: returnBalance,
      });
      
      // Validar saldo suficiente en la nueva cuenta
      const newAccountBalance = newAccount.balances?.[currency] || 0;
      if (newAccountBalance < newTotalValue) {
        throw new HttpsError(
          'failed-precondition',
          `Saldo insuficiente en la nueva cuenta. Disponible: ${newAccountBalance.toFixed(2)} ${currency}, Requerido: ${newTotalValue.toFixed(2)} ${currency}`
        );
      }
      
      // Cobrar el valor total de la nueva cuenta
      const chargeBalance = cleanDecimal(newAccountBalance - newTotalValue);
      batch.update(newAccountRef, {
        [`balances.${currency}`]: chargeBalance,
      });
      
      console.log(`[assetHandlers][updateAsset] Balance ajustado: cuenta original +${oldTotalValue}, cuenta nueva -${newTotalValue}`);
    } else if (valueDifference !== 0) {
      // Si no cambia de cuenta, solo ajustar la diferencia
      const newBalance = cleanDecimal((account.balances?.[currency] || 0) - valueDifference);
      const accountRef = db.collection('portfolioAccounts').doc(oldAsset.portfolioAccount);
      batch.update(accountRef, {
        [`balances.${currency}`]: newBalance,
      });
    }

    // 8.1. Actualizar la transacción de compra asociada (si existe)
    const transactionQuery = await db.collection('transactions')
      .where('assetId', '==', data.assetId)
      .where('type', '==', 'buy')
      .limit(1)
      .get();
    
    if (!transactionQuery.empty) {
      const transactionRef = transactionQuery.docs[0].ref;
      const transactionUpdate = {};
      
      // Solo actualizar los campos que cambiaron
      if (updateData.name !== undefined) {
        transactionUpdate.assetName = updateData.name;
      }
      if (updateData.units !== undefined) {
        transactionUpdate.amount = updateData.units;
      }
      if (updateData.unitValue !== undefined) {
        transactionUpdate.price = updateData.unitValue;
      }
      if (updateData.currency !== undefined) {
        transactionUpdate.currency = updateData.currency;
      }
      if (updateData.acquisitionDate !== undefined) {
        transactionUpdate.date = updateData.acquisitionDate;
      }
      if (updateData.commission !== undefined) {
        transactionUpdate.commission = updateData.commission;
      }
      if (updateData.assetType !== undefined) {
        transactionUpdate.assetType = updateData.assetType;
      }
      if (updateData.acquisitionDollarValue !== undefined) {
        transactionUpdate.dollarPriceToDate = updateData.acquisitionDollarValue;
      }
      if (updateData.market !== undefined) {
        transactionUpdate.market = updateData.market;
      }
      if (updateData.defaultCurrencyForAdquisitionDollar !== undefined) {
        transactionUpdate.defaultCurrencyForAdquisitionDollar = updateData.defaultCurrencyForAdquisitionDollar;
      }
      // Actualizar portfolioAccountId si cambió la cuenta
      if (updateData.portfolioAccount !== undefined) {
        transactionUpdate.portfolioAccountId = updateData.portfolioAccount;
      }
      
      if (Object.keys(transactionUpdate).length > 0) {
        batch.update(transactionRef, transactionUpdate);
        console.log(`[assetHandlers][updateAsset] Actualizando transacción asociada: ${transactionRef.id}`);
      }
    }

    await batch.commit();

    // 9. Invalidar caches
    await invalidatePerformanceCache(auth.uid);
    invalidateDistributionCache(auth.uid);

    console.log(`[assetHandlers][updateAsset] Éxito - assetId: ${data.assetId}`);

    return {
      success: true,
      assetId: data.assetId,
      balanceAdjustment: valueDifference,
    };

  } catch (error) {
    console.error(`[assetHandlers][updateAsset] Error - userId: ${auth.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al actualizar el activo: ${error.message}`);
  }
}

/**
 * Vende un asset existente (total o parcialmente)
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de venta
 * @returns {Promise<{success: boolean, transactionId: string, realizedPnL: number, isFullSale: boolean}>}
 */
async function sellAsset(context, payload) {
  const { auth } = context;
  const data = payload;

  console.log(`[assetHandlers][sellAsset] userId: ${auth.uid}, assetId: ${data?.assetId}`);

  try {
    // 1. Validar datos requeridos
    if (!data.assetId || !data.portfolioAccountId) {
      throw new HttpsError('invalid-argument', 'assetId y portfolioAccountId son requeridos');
    }

    // 2. Obtener el asset
    const assetRef = db.collection('assets').doc(data.assetId);
    const assetDoc = await assetRef.get();

    if (!assetDoc.exists) {
      throw new HttpsError('not-found', 'El activo no existe');
    }

    const asset = { id: assetDoc.id, ...assetDoc.data() };

    // 3. Validar ownership de la cuenta
    const account = await validateAccountOwnership(data.portfolioAccountId, auth.uid);

    // 4. Validar que el asset pertenece a la cuenta
    if (asset.portfolioAccount !== data.portfolioAccountId) {
      throw new HttpsError('permission-denied', 'El activo no pertenece a esta cuenta');
    }

    // 5. Validar cantidad a vender
    const sellAmount = cleanDecimal(Number(data.sellAmount));
    const currentUnits = cleanDecimal(Number(asset.units));

    if (sellAmount <= 0) {
      throw new HttpsError('invalid-argument', 'La cantidad a vender debe ser mayor a 0');
    }

    if (sellAmount > currentUnits) {
      throw new HttpsError(
        'failed-precondition',
        `No hay suficientes unidades para vender. Disponibles: ${currentUnits}, Solicitadas: ${sellAmount}`
      );
    }

    // 6. Calcular valores
    const sellPrice = cleanDecimal(Number(data.sellPrice) || 0);
    const sellCommission = cleanDecimal(Number(data.sellCommission) || 0);
    const sellValue = cleanDecimal(sellAmount * sellPrice);
    const totalRevenue = cleanDecimal(sellValue - sellCommission);
    
    const buyPrice = cleanDecimal(Number(asset.unitValue));
    const realizedPnL = cleanDecimal((sellPrice - buyPrice) * sellAmount);

    const remainingUnits = cleanDecimal(currentUnits - sellAmount);
    const isFullSale = Math.abs(remainingUnits) < Number.EPSILON || remainingUnits <= 0;

    // 7. Ejecutar transacción atómica
    const batch = db.batch();

    if (isFullSale) {
      batch.update(assetRef, { units: 0, isActive: false });
    } else {
      batch.update(assetRef, { units: remainingUnits });
    }

    const transactionRef = db.collection('transactions').doc();
    const transactionData = {
      assetId: data.assetId,
      assetName: asset.name,
      type: 'sell',
      amount: sellAmount,
      price: sellPrice,
      currency: asset.currency,
      date: new Date().toISOString().split('T')[0],
      portfolioAccountId: data.portfolioAccountId,
      commission: sellCommission,
      assetType: asset.assetType,
      dollarPriceToDate: cleanDecimal(Number(asset.acquisitionDollarValue) || 1),
      market: asset.market || '',
      defaultCurrencyForAdquisitionDollar: asset.defaultCurrencyForAdquisitionDollar || 'USD',
      valuePnL: realizedPnL,
      closedPnL: isFullSale,
      userId: auth.uid,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    batch.set(transactionRef, transactionData);

    if (isFullSale) {
      const buyTransactionQuery = db.collection('transactions')
        .where('assetId', '==', data.assetId)
        .where('type', '==', 'buy')
        .limit(1);
      
      const buyTransactionSnapshot = await buyTransactionQuery.get();
      if (!buyTransactionSnapshot.empty) {
        batch.update(buyTransactionSnapshot.docs[0].ref, { closedPnL: true });
      }
    }

    const newBalance = cleanDecimal((account.balances?.[asset.currency] || 0) + totalRevenue);
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, {
      [`balances.${asset.currency}`]: newBalance,
    });

    await batch.commit();

    // 8. Invalidar caches
    await invalidatePerformanceCache(auth.uid);
    invalidateDistributionCache(auth.uid);

    console.log(`[assetHandlers][sellAsset] Éxito - transactionId: ${transactionRef.id}`);

    return {
      success: true,
      transactionId: transactionRef.id,
      realizedPnL: realizedPnL,
      isFullSale: isFullSale,
    };

  } catch (error) {
    console.error(`[assetHandlers][sellAsset] Error - userId: ${auth.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al vender el activo: ${error.message}`);
  }
}

/**
 * Vende unidades de múltiples lotes del mismo ticker usando FIFO
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de venta FIFO
 * @returns {Promise<{success: boolean, soldAssets: Array, totalPnL: number, totalRevenue: number}>}
 */
async function sellPartialAssetsFIFO(context, payload) {
  const { auth } = context;
  const data = payload;

  console.log(`[assetHandlers][sellPartialAssetsFIFO] userId: ${auth.uid}, ticker: ${data?.ticker}`);

  try {
    // 1. Validar datos requeridos
    if (!data.ticker || !data.portfolioAccountId || !data.unitsToSell) {
      throw new HttpsError('invalid-argument', 'ticker, portfolioAccountId y unitsToSell son requeridos');
    }

    // 2. Validar ownership de la cuenta
    const account = await validateAccountOwnership(data.portfolioAccountId, auth.uid);

    // 3. Obtener assets del ticker ordenados por fecha (FIFO)
    const assetsQuery = db.collection('assets')
      .where('name', '==', data.ticker)
      .where('isActive', '==', true)
      .where('portfolioAccount', '==', data.portfolioAccountId)
      .orderBy('acquisitionDate');
    
    const assetsSnapshot = await assetsQuery.get();

    if (assetsSnapshot.empty) {
      throw new HttpsError('not-found', `No hay activos activos del ticker ${data.ticker}`);
    }

    // 4. Calcular unidades disponibles
    let totalAvailableUnits = 0;
    const assetsList = [];
    assetsSnapshot.forEach(doc => {
      const assetData = { id: doc.id, ...doc.data() };
      assetsList.push(assetData);
      totalAvailableUnits = cleanDecimal(totalAvailableUnits + Number(assetData.units));
    });

    const unitsToSell = cleanDecimal(Number(data.unitsToSell));
    if (unitsToSell > totalAvailableUnits) {
      throw new HttpsError(
        'failed-precondition',
        `No hay suficientes unidades. Disponibles: ${totalAvailableUnits}, Solicitadas: ${unitsToSell}`
      );
    }

    // 5. Procesar venta FIFO
    const batch = db.batch();
    let remainingUnitsToSell = unitsToSell;
    let totalSellValue = 0;
    let totalPnL = 0;
    const soldAssets = [];
    const pricePerUnit = cleanDecimal(Number(data.pricePerUnit) || 0);
    const totalCommission = cleanDecimal(Number(data.totalCommission) || 0);
    const today = new Date().toISOString().split('T')[0];
    const currency = assetsList[0]?.currency || 'USD';

    for (const asset of assetsList) {
      if (remainingUnitsToSell <= 0) break;

      const assetUnits = cleanDecimal(Number(asset.units));
      const unitsToSellFromAsset = cleanDecimal(Math.min(assetUnits, remainingUnitsToSell));
      
      remainingUnitsToSell = cleanDecimal(remainingUnitsToSell - unitsToSellFromAsset);
      
      const sellValueFromAsset = cleanDecimal(unitsToSellFromAsset * pricePerUnit);
      totalSellValue = cleanDecimal(totalSellValue + sellValueFromAsset);

      const buyPrice = cleanDecimal(Number(asset.unitValue));
      const lotPnL = cleanDecimal((pricePerUnit - buyPrice) * unitsToSellFromAsset);
      totalPnL = cleanDecimal(totalPnL + lotPnL);

      const assetRef = db.collection('assets').doc(asset.id);
      const remainingUnits = cleanDecimal(assetUnits - unitsToSellFromAsset);
      const isFullSale = Math.abs(remainingUnits) < Number.EPSILON;

      if (isFullSale) {
        batch.update(assetRef, { units: 0, isActive: false });
      } else {
        batch.update(assetRef, { units: remainingUnits });
      }

      const proportionalCommission = cleanDecimal((totalCommission * unitsToSellFromAsset) / unitsToSell);

      const transactionRef = db.collection('transactions').doc();
      batch.set(transactionRef, {
        assetId: asset.id,
        assetName: asset.name,
        type: 'sell',
        amount: unitsToSellFromAsset,
        price: pricePerUnit,
        currency: asset.currency,
        date: today,
        portfolioAccountId: data.portfolioAccountId,
        commission: proportionalCommission,
        assetType: asset.assetType,
        dollarPriceToDate: cleanDecimal(Number(asset.acquisitionDollarValue) || 1),
        market: asset.market || '',
        defaultCurrencyForAdquisitionDollar: asset.defaultCurrencyForAdquisitionDollar || 'USD',
        valuePnL: lotPnL,
        closedPnL: isFullSale,
        userId: auth.uid,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      if (isFullSale) {
        const buyTransactionQuery = db.collection('transactions')
          .where('assetId', '==', asset.id)
          .where('type', '==', 'buy')
          .limit(1);
        
        const buyTransactionSnapshot = await buyTransactionQuery.get();
        if (!buyTransactionSnapshot.empty) {
          batch.update(buyTransactionSnapshot.docs[0].ref, { closedPnL: true });
        }
      }

      soldAssets.push({
        assetId: asset.id,
        unitsSold: unitsToSellFromAsset,
        buyPrice: buyPrice,
        sellPrice: pricePerUnit,
        pnl: lotPnL,
        isFullSale: isFullSale,
      });
    }

    // 6. Actualizar balance de la cuenta
    const totalRevenue = cleanDecimal(totalSellValue - totalCommission);
    const newBalance = cleanDecimal((account.balances?.[currency] || 0) + totalRevenue);
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, {
      [`balances.${currency}`]: newBalance,
    });

    await batch.commit();

    // 7. Invalidar caches
    await invalidatePerformanceCache(auth.uid);
    invalidateDistributionCache(auth.uid);

    console.log(`[assetHandlers][sellPartialAssetsFIFO] Éxito - lotes: ${soldAssets.length}`);

    return {
      success: true,
      soldAssets: soldAssets,
      totalPnL: totalPnL,
      totalRevenue: totalRevenue,
    };

  } catch (error) {
    console.error(`[assetHandlers][sellPartialAssetsFIFO] Error - userId: ${auth.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al vender activos FIFO: ${error.message}`);
  }
}

/**
 * Registra una transacción de efectivo (ingreso o egreso)
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de transacción
 * @returns {Promise<{success: boolean, transactionId: string, newBalance: number}>}
 */
async function addCashTransaction(context, payload) {
  const { auth } = context;
  const data = payload;

  console.log(`[assetHandlers][addCashTransaction] userId: ${auth.uid}, type: ${data?.type}`);

  try {
    // 1. Validar datos requeridos
    if (!data.portfolioAccountId || !data.type || !data.amount || !data.currency) {
      throw new HttpsError('invalid-argument', 'portfolioAccountId, type, amount y currency son requeridos');
    }

    // 2. Validar tipo de transacción
    if (!['cash_income', 'cash_expense'].includes(data.type)) {
      throw new HttpsError('invalid-argument', 'El tipo debe ser cash_income o cash_expense');
    }

    // 3. Validar ownership de la cuenta
    const account = await validateAccountOwnership(data.portfolioAccountId, auth.uid);

    // 4. Calcular nuevo balance
    const amount = cleanDecimal(Number(data.amount));
    if (amount <= 0) {
      throw new HttpsError('invalid-argument', 'El monto debe ser mayor a 0');
    }

    const currentBalance = account.balances?.[data.currency] || 0;
    let newBalance;

    if (data.type === 'cash_income') {
      newBalance = cleanDecimal(currentBalance + amount);
    } else {
      if (currentBalance < amount) {
        throw new HttpsError(
          'failed-precondition',
          `Saldo insuficiente. Disponible: ${currentBalance.toFixed(2)} ${data.currency}, Solicitado: ${amount.toFixed(2)} ${data.currency}`
        );
      }
      newBalance = cleanDecimal(currentBalance - amount);
    }

    // 5. Ejecutar transacción atómica
    const batch = db.batch();

    const transactionRef = db.collection('transactions').doc();
    const transactionData = {
      assetName: `${data.type === 'cash_income' ? 'Ingreso' : 'Egreso'} de ${data.currency}`,
      type: data.type,
      amount: amount,
      price: 1,
      currency: data.currency,
      date: data.date || new Date().toISOString().split('T')[0],
      portfolioAccountId: data.portfolioAccountId,
      commission: 0,
      assetType: 'cash',
      dollarPriceToDate: cleanDecimal(Number(data.dollarPriceToDate) || 1),
      defaultCurrencyForAdquisitionDollar: data.defaultCurrencyForAdquisitionDollar || 'USD',
      description: data.description || '',
      userId: auth.uid,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    batch.set(transactionRef, transactionData);

    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, {
      [`balances.${data.currency}`]: newBalance,
    });

    await batch.commit();

    console.log(`[assetHandlers][addCashTransaction] Éxito - transactionId: ${transactionRef.id}`);

    return {
      success: true,
      transactionId: transactionRef.id,
      newBalance: newBalance,
    };

  } catch (error) {
    console.error(`[assetHandlers][addCashTransaction] Error - userId: ${auth.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al registrar transacción de efectivo: ${error.message}`);
  }
}

/**
 * Elimina un asset individual y sus transacciones asociadas
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de eliminación
 * @returns {Promise<{success: boolean, deletedTransactionsCount: number}>}
 */
async function deleteAsset(context, payload) {
  const { auth } = context;
  const data = payload;

  console.log(`[assetHandlers][deleteAsset] userId: ${auth.uid}, assetId: ${data?.assetId}`);

  try {
    // 1. Validar datos requeridos
    if (!data.assetId) {
      throw new HttpsError('invalid-argument', 'assetId es requerido');
    }

    // 2. Obtener el asset
    const assetRef = db.collection('assets').doc(data.assetId);
    const assetDoc = await assetRef.get();

    if (!assetDoc.exists) {
      throw new HttpsError('not-found', 'El asset no existe');
    }

    const assetData = assetDoc.data();

    // 3. Validar ownership via portfolioAccount
    await validateAccountOwnership(assetData.portfolioAccount, auth.uid);

    // 4. Buscar y eliminar transacciones asociadas
    const transactionsQuery = db.collection('transactions')
      .where('assetId', '==', data.assetId);
    
    const transactionsSnapshot = await transactionsQuery.get();

    const batch = db.batch();
    let deletedTransactionsCount = 0;

    transactionsSnapshot.forEach(txDoc => {
      batch.delete(txDoc.ref);
      deletedTransactionsCount++;
    });

    // 5. Eliminar el asset
    batch.delete(assetRef);

    await batch.commit();

    // 6. Invalidar caches
    await invalidatePerformanceCache(auth.uid);
    invalidateDistributionCache(auth.uid);

    console.log(`[assetHandlers][deleteAsset] Éxito - assetId: ${data.assetId}`);

    return {
      success: true,
      deletedTransactionsCount: deletedTransactionsCount,
    };

  } catch (error) {
    console.error(`[assetHandlers][deleteAsset] Error - userId: ${auth.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al eliminar el activo: ${error.message}`);
  }
}

/**
 * Elimina activos de una cuenta de portafolio
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de eliminación
 * @returns {Promise<{success: boolean, deletedCount: number}>}
 */
async function deleteAssets(context, payload) {
  const { auth } = context;
  const data = payload;

  console.log(`[assetHandlers][deleteAssets] userId: ${auth.uid}, accountId: ${data?.accountId}`);

  try {
    // 1. Validar datos requeridos
    if (!data.accountId) {
      throw new HttpsError('invalid-argument', 'accountId es requerido');
    }

    // 2. Validar ownership de la cuenta
    await validateAccountOwnership(data.accountId, auth.uid);

    // 3. Buscar assets a eliminar
    let assetsQuery = db.collection('assets')
      .where('portfolioAccount', '==', data.accountId);
    
    if (data.currency) {
      assetsQuery = assetsQuery.where('currency', '==', data.currency);
    }

    const assetsSnapshot = await assetsQuery.get();

    if (assetsSnapshot.empty) {
      return { success: true, deletedCount: 0 };
    }

    // 4. Eliminar assets y sus transacciones asociadas
    const batch = db.batch();
    let deletedCount = 0;

    for (const assetDoc of assetsSnapshot.docs) {
      batch.delete(assetDoc.ref);
      deletedCount++;

      const transactionsQuery = db.collection('transactions')
        .where('assetId', '==', assetDoc.id);
      
      const transactionsSnapshot = await transactionsQuery.get();
      transactionsSnapshot.forEach(txDoc => {
        batch.delete(txDoc.ref);
      });
    }

    await batch.commit();

    console.log(`[assetHandlers][deleteAssets] Éxito - deletedCount: ${deletedCount}`);

    return {
      success: true,
      deletedCount: deletedCount,
    };

  } catch (error) {
    console.error(`[assetHandlers][deleteAssets] Error - userId: ${auth.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al eliminar activos: ${error.message}`);
  }
}

/**
 * @deprecated OPT-DEMAND-CLEANUP: Esta función ya NO debe usarse
 * 
 * La colección currentPrices está siendo deprecada. Los sectores ahora
 * vienen exclusivamente del API Lambda on-demand.
 * 
 * @see docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de actualización
 * @returns {Promise<never>} Siempre lanza error
 */
async function updateStockSector(context, payload) {
  const { auth } = context;
  
  // OPT-DEMAND-CLEANUP: Función deprecada
  console.warn(`[assetHandlers][updateStockSector] DEPRECADO - Los sectores vienen del API Lambda. userId: ${auth.uid}`);
  
  throw new HttpsError(
    'failed-precondition',
    'Esta función está deprecada. Los sectores ahora se obtienen automáticamente del API.'
  );
}

// OPT-DEMAND-CLEANUP: Código legacy comentado para referencia
/*
async function updateStockSector_LEGACY(context, payload) {
  const { auth } = context;
  const data = payload;

  console.log(`[assetHandlers][updateStockSector] userId: ${auth.uid}, symbol: ${data?.symbol}`);

  try {
    // 1. Validar datos requeridos
    if (!data.symbol || !data.sector) {
      throw new HttpsError('invalid-argument', 'symbol y sector son requeridos');
    }

    // 2. Verificar que el símbolo existe en currentPrices
    const priceRef = db.collection('currentPrices').doc(data.symbol);
    const priceDoc = await priceRef.get();

    if (!priceDoc.exists) {
      throw new HttpsError('not-found', `No se encontró el símbolo ${data.symbol} en currentPrices`);
    }

    // 3. Actualizar sector
    await priceRef.update({
      sector: data.sector,
      sectorUpdatedBy: auth.uid,
      sectorUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    console.log(`[assetHandlers][updateStockSector] Éxito - symbol: ${data.symbol}`);

    return {
      success: true,
      symbol: data.symbol,
      sector: data.sector,
    };

  } catch (error) {
    console.error(`[assetHandlers][updateStockSector] Error - userId: ${auth.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al actualizar sector: ${error.message}`);
  }
}
*/

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  createAsset,
  updateAsset,
  sellAsset,
  deleteAsset,
  deleteAssets,
  sellPartialAssetsFIFO,
  addCashTransaction,
  updateStockSector,
  // Utilidades exportadas para posible reutilización
  cleanDecimal,
  validateAccountOwnership,
  validateSufficientFunds,
  ensureCurrentPriceExists,
};
