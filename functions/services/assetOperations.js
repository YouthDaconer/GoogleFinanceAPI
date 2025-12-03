/**
 * Cloud Functions Callable para Operaciones de Assets
 * 
 * Este módulo implementa las operaciones de escritura de assets que antes
 * se realizaban en el frontend (useFirestore.ts). Siguiendo principios SOLID:
 * - Single Responsibility: Cada función tiene una única responsabilidad
 * - Open/Closed: Extensible sin modificar código existente
 * 
 * @module assetOperations
 * @see docs/architecture/refactoring-analysis.md
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('./firebaseAdmin');
const db = admin.firestore();

// Importar función de invalidación de cache de rendimientos (OPT-002)
const { invalidatePerformanceCache } = require('./historicalReturnsService');

/**
 * Configuración común para Cloud Functions Callable
 */
const callableConfig = {
  cors: true,
  enforceAppCheck: false,
  timeoutSeconds: 60,
  memory: "256MiB",
};

/**
 * Utilidad para limpiar decimales en operaciones financieras
 * @param {number} num - Número a limpiar
 * @param {number} decimals - Cantidad de decimales (default: 8)
 * @returns {number} Número limpio
 */
const cleanDecimal = (num, decimals = 8) =>
  Number(Math.round(Number(num + "e" + decimals)) / 10 ** decimals);

/**
 * Valida que el usuario esté autenticado
 * @param {object} auth - Objeto de autenticación de Firebase
 * @throws {HttpsError} Si no hay autenticación
 */
const validateAuth = (auth) => {
  if (!auth) {
    throw new HttpsError(
      'unauthenticated',
      'Debes iniciar sesión para realizar esta operación'
    );
  }
};

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

// ============================================================================
// FUNCIÓN: createAsset
// ============================================================================

/**
 * Crea un nuevo asset con transacción de compra y actualización de balance
 * 
 * @description
 * Esta función reemplaza la lógica de addAsset() en useFirestore.ts.
 * Ejecuta atómicamente:
 * 1. Crear documento en `assets`
 * 2. Crear transacción de compra en `transactions`
 * 3. Descontar balance de la cuenta en `portfolioAccounts`
 * 
 * @param {object} data - Datos del asset
 * @param {string} data.portfolioAccount - ID de la cuenta de portafolio
 * @param {string} data.name - Ticker del activo (ej: "AAPL")
 * @param {string} data.assetType - Tipo de activo ("stock", "etf", etc.)
 * @param {string} data.market - Mercado (ej: "NASDAQ")
 * @param {string} data.company - Broker
 * @param {string} data.currency - Moneda (ej: "USD")
 * @param {number} data.units - Cantidad de unidades
 * @param {number} data.unitValue - Precio de compra por unidad
 * @param {string} data.acquisitionDate - Fecha de adquisición (ISO)
 * @param {number} data.acquisitionDollarValue - Tasa USD del día
 * @param {string} data.defaultCurrencyForAdquisitionDollar - Moneda base para conversión
 * @param {number} data.commission - Comisión de compra
 * 
 * @returns {Promise<object>} { success: true, assetId, transactionId }
 */
exports.createAsset = onCall(callableConfig, async (request) => {
  const { auth, data } = request;
  
  // Log de auditoría
  console.log(`[createAsset] Iniciando - userId: ${auth?.uid}, ticker: ${data?.name}`);

  try {
    // 1. Validar autenticación
    validateAuth(auth);

    // 2. Validar datos requeridos
    const requiredFields = ['portfolioAccount', 'name', 'assetType', 'currency', 'units', 'unitValue', 'acquisitionDate'];
    for (const field of requiredFields) {
      if (data[field] === undefined || data[field] === null || data[field] === '') {
        throw new HttpsError('invalid-argument', `El campo ${field} es requerido`);
      }
    }

    // 3. Validar ownership de la cuenta
    const account = await validateAccountOwnership(data.portfolioAccount, auth.uid);

    // 4. Calcular costo total
    const units = cleanDecimal(Number(data.units));
    const unitValue = cleanDecimal(Number(data.unitValue));
    const commission = cleanDecimal(Number(data.commission) || 0);
    const totalCost = cleanDecimal(units * unitValue + commission);

    // 5. Validar saldo suficiente
    validateSufficientFunds(account, data.currency, totalCost);

    // 6. Ejecutar transacción atómica
    const batch = db.batch();

    // 6.1. Crear el asset
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

    // 6.2. Crear transacción de compra
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

    // 6.3. Actualizar balance de la cuenta
    const newBalance = cleanDecimal((account.balances?.[data.currency] || 0) - totalCost);
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccount);
    batch.update(accountRef, {
      [`balances.${data.currency}`]: newBalance,
    });

    // 7. Commit de la transacción
    await batch.commit();

    // 8. Invalidar cache de rendimientos (OPT-002)
    await invalidatePerformanceCache(auth.uid);

    console.log(`[createAsset] Éxito - assetId: ${assetRef.id}, transactionId: ${transactionRef.id}`);

    return {
      success: true,
      assetId: assetRef.id,
      transactionId: transactionRef.id,
    };

  } catch (error) {
    console.error(`[createAsset] Error - userId: ${auth?.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al crear el activo: ${error.message}`);
  }
});

// ============================================================================
// FUNCIÓN: sellAsset
// ============================================================================

/**
 * Vende un asset existente (total o parcialmente)
 * 
 * @description
 * Esta función reemplaza la lógica de sellAsset() en useFirestore.ts.
 * Ejecuta atómicamente:
 * 1. Actualizar unidades del asset (o marcar inactivo si venta total)
 * 2. Crear transacción de venta con P&L calculado
 * 3. Sumar balance a la cuenta
 * 
 * @param {object} data
 * @param {string} data.assetId - ID del asset a vender
 * @param {number} data.sellAmount - Cantidad de unidades a vender
 * @param {number} data.sellPrice - Precio de venta por unidad
 * @param {number} data.sellCommission - Comisión de venta
 * @param {string} data.portfolioAccountId - ID de la cuenta
 * 
 * @returns {Promise<object>} { success, transactionId, realizedPnL, isFullSale }
 */
exports.sellAsset = onCall(callableConfig, async (request) => {
  const { auth, data } = request;

  console.log(`[sellAsset] Iniciando - userId: ${auth?.uid}, assetId: ${data?.assetId}`);

  try {
    // 1. Validar autenticación
    validateAuth(auth);

    // 2. Validar datos requeridos
    if (!data.assetId || !data.portfolioAccountId) {
      throw new HttpsError('invalid-argument', 'assetId y portfolioAccountId son requeridos');
    }

    // 3. Obtener el asset
    const assetRef = db.collection('assets').doc(data.assetId);
    const assetDoc = await assetRef.get();

    if (!assetDoc.exists) {
      throw new HttpsError('not-found', 'El activo no existe');
    }

    const asset = { id: assetDoc.id, ...assetDoc.data() };

    // 4. Validar ownership de la cuenta
    const account = await validateAccountOwnership(data.portfolioAccountId, auth.uid);

    // 5. Validar que el asset pertenece a la cuenta
    if (asset.portfolioAccount !== data.portfolioAccountId) {
      throw new HttpsError('permission-denied', 'El activo no pertenece a esta cuenta');
    }

    // 6. Validar cantidad a vender
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

    // 7. Calcular valores
    const sellPrice = cleanDecimal(Number(data.sellPrice) || 0);
    const sellCommission = cleanDecimal(Number(data.sellCommission) || 0);
    const sellValue = cleanDecimal(sellAmount * sellPrice);
    const totalRevenue = cleanDecimal(sellValue - sellCommission);
    
    // Calcular P&L realizado
    const buyPrice = cleanDecimal(Number(asset.unitValue));
    const realizedPnL = cleanDecimal((sellPrice - buyPrice) * sellAmount);

    // Determinar si es venta total
    const remainingUnits = cleanDecimal(currentUnits - sellAmount);
    const isFullSale = Math.abs(remainingUnits) < Number.EPSILON || remainingUnits <= 0;

    // 8. Ejecutar transacción atómica
    const batch = db.batch();

    // 8.1. Actualizar el asset
    if (isFullSale) {
      batch.update(assetRef, {
        units: 0,
        isActive: false,
      });
    } else {
      batch.update(assetRef, {
        units: remainingUnits,
      });
    }

    // 8.2. Crear transacción de venta
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

    // 8.3. Si es venta total, marcar transacción de compra como cerrada
    if (isFullSale) {
      const buyTransactionQuery = db.collection('transactions')
        .where('assetId', '==', data.assetId)
        .where('type', '==', 'buy')
        .limit(1);
      
      const buyTransactionSnapshot = await buyTransactionQuery.get();
      if (!buyTransactionSnapshot.empty) {
        const buyTransactionRef = buyTransactionSnapshot.docs[0].ref;
        batch.update(buyTransactionRef, { closedPnL: true });
      }
    }

    // 8.4. Actualizar balance de la cuenta
    const newBalance = cleanDecimal((account.balances?.[asset.currency] || 0) + totalRevenue);
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, {
      [`balances.${asset.currency}`]: newBalance,
    });

    // 9. Commit de la transacción
    await batch.commit();

    // 10. Invalidar cache de rendimientos (OPT-002)
    await invalidatePerformanceCache(auth.uid);

    console.log(`[sellAsset] Éxito - transactionId: ${transactionRef.id}, isFullSale: ${isFullSale}`);

    return {
      success: true,
      transactionId: transactionRef.id,
      realizedPnL: realizedPnL,
      isFullSale: isFullSale,
    };

  } catch (error) {
    console.error(`[sellAsset] Error - userId: ${auth?.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al vender el activo: ${error.message}`);
  }
});

// ============================================================================
// FUNCIÓN: sellPartialAssetsFIFO
// ============================================================================

/**
 * Vende unidades de múltiples lotes del mismo ticker usando FIFO
 * 
 * @description
 * Esta función reemplaza sellPartialAssets() en useFirestore.ts.
 * Vende los lotes más antiguos primero (First In, First Out).
 * 
 * @param {object} data
 * @param {string} data.ticker - Símbolo del activo
 * @param {number} data.unitsToSell - Cantidad total a vender
 * @param {number} data.pricePerUnit - Precio de venta
 * @param {string} data.portfolioAccountId - ID de la cuenta
 * @param {number} data.totalCommission - Comisión total
 * 
 * @returns {Promise<object>} { success, soldAssets[], totalPnL }
 */
exports.sellPartialAssetsFIFO = onCall(callableConfig, async (request) => {
  const { auth, data } = request;

  console.log(`[sellPartialAssetsFIFO] Iniciando - userId: ${auth?.uid}, ticker: ${data?.ticker}`);

  try {
    // 1. Validar autenticación
    validateAuth(auth);

    // 2. Validar datos requeridos
    if (!data.ticker || !data.portfolioAccountId || !data.unitsToSell) {
      throw new HttpsError('invalid-argument', 'ticker, portfolioAccountId y unitsToSell son requeridos');
    }

    // 3. Validar ownership de la cuenta
    const account = await validateAccountOwnership(data.portfolioAccountId, auth.uid);

    // 4. Obtener assets del ticker ordenados por fecha de adquisición (FIFO)
    const assetsQuery = db.collection('assets')
      .where('name', '==', data.ticker)
      .where('isActive', '==', true)
      .where('portfolioAccount', '==', data.portfolioAccountId)
      .orderBy('acquisitionDate');
    
    const assetsSnapshot = await assetsQuery.get();

    if (assetsSnapshot.empty) {
      throw new HttpsError('not-found', `No hay activos activos del ticker ${data.ticker}`);
    }

    // 5. Calcular unidades disponibles
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

    // 6. Procesar venta FIFO
    const batch = db.batch();
    let remainingUnitsToSell = unitsToSell;
    let totalSellValue = 0;
    let totalPnL = 0;
    const soldAssets = [];
    const pricePerUnit = cleanDecimal(Number(data.pricePerUnit) || 0);
    const totalCommission = cleanDecimal(Number(data.totalCommission) || 0);
    const today = new Date().toISOString().split('T')[0];

    // Obtener moneda del primer asset para el balance
    const currency = assetsList[0]?.currency || 'USD';

    for (const asset of assetsList) {
      if (remainingUnitsToSell <= 0) break;

      const assetUnits = cleanDecimal(Number(asset.units));
      const unitsToSellFromAsset = cleanDecimal(Math.min(assetUnits, remainingUnitsToSell));
      
      remainingUnitsToSell = cleanDecimal(remainingUnitsToSell - unitsToSellFromAsset);
      
      const sellValueFromAsset = cleanDecimal(unitsToSellFromAsset * pricePerUnit);
      totalSellValue = cleanDecimal(totalSellValue + sellValueFromAsset);

      // Calcular P&L para este lote
      const buyPrice = cleanDecimal(Number(asset.unitValue));
      const lotPnL = cleanDecimal((pricePerUnit - buyPrice) * unitsToSellFromAsset);
      totalPnL = cleanDecimal(totalPnL + lotPnL);

      // Actualizar asset
      const assetRef = db.collection('assets').doc(asset.id);
      const remainingUnits = cleanDecimal(assetUnits - unitsToSellFromAsset);
      const isFullSale = Math.abs(remainingUnits) < Number.EPSILON;

      if (isFullSale) {
        batch.update(assetRef, { units: 0, isActive: false });
      } else {
        batch.update(assetRef, { units: remainingUnits });
      }

      // Calcular comisión proporcional
      const proportionalCommission = cleanDecimal((totalCommission * unitsToSellFromAsset) / unitsToSell);

      // Crear transacción de venta para este lote
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

      // Si es venta total, marcar transacción de compra como cerrada
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

    // 7. Actualizar balance de la cuenta
    const totalRevenue = cleanDecimal(totalSellValue - totalCommission);
    const newBalance = cleanDecimal((account.balances?.[currency] || 0) + totalRevenue);
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, {
      [`balances.${currency}`]: newBalance,
    });

    // 8. Commit
    await batch.commit();

    // 9. Invalidar cache de rendimientos (OPT-002)
    await invalidatePerformanceCache(auth.uid);

    console.log(`[sellPartialAssetsFIFO] Éxito - lotes vendidos: ${soldAssets.length}, totalPnL: ${totalPnL}`);

    return {
      success: true,
      soldAssets: soldAssets,
      totalPnL: totalPnL,
      totalRevenue: totalRevenue,
    };

  } catch (error) {
    console.error(`[sellPartialAssetsFIFO] Error - userId: ${auth?.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al vender activos FIFO: ${error.message}`);
  }
});

// ============================================================================
// FUNCIÓN: addCashTransaction
// ============================================================================

/**
 * Registra una transacción de efectivo (ingreso o egreso)
 * 
 * @param {object} data
 * @param {string} data.portfolioAccountId - ID de la cuenta
 * @param {string} data.type - "cash_income" o "cash_expense"
 * @param {number} data.amount - Monto de la transacción
 * @param {string} data.currency - Moneda
 * @param {string} data.date - Fecha (ISO)
 * @param {string} data.description - Descripción opcional
 * @param {number} data.dollarPriceToDate - Tasa de cambio USD
 * @param {string} data.defaultCurrencyForAdquisitionDollar - Moneda base
 * 
 * @returns {Promise<object>} { success, transactionId, newBalance }
 */
exports.addCashTransaction = onCall(callableConfig, async (request) => {
  const { auth, data } = request;

  console.log(`[addCashTransaction] Iniciando - userId: ${auth?.uid}, type: ${data?.type}`);

  try {
    // 1. Validar autenticación
    validateAuth(auth);

    // 2. Validar datos requeridos
    if (!data.portfolioAccountId || !data.type || !data.amount || !data.currency) {
      throw new HttpsError('invalid-argument', 'portfolioAccountId, type, amount y currency son requeridos');
    }

    // 3. Validar tipo de transacción
    if (!['cash_income', 'cash_expense'].includes(data.type)) {
      throw new HttpsError('invalid-argument', 'El tipo debe ser cash_income o cash_expense');
    }

    // 4. Validar ownership de la cuenta
    const account = await validateAccountOwnership(data.portfolioAccountId, auth.uid);

    // 5. Calcular nuevo balance
    const amount = cleanDecimal(Number(data.amount));
    if (amount <= 0) {
      throw new HttpsError('invalid-argument', 'El monto debe ser mayor a 0');
    }

    const currentBalance = account.balances?.[data.currency] || 0;
    let newBalance;

    if (data.type === 'cash_income') {
      newBalance = cleanDecimal(currentBalance + amount);
    } else {
      // Validar saldo suficiente para egreso
      if (currentBalance < amount) {
        throw new HttpsError(
          'failed-precondition',
          `Saldo insuficiente. Disponible: ${currentBalance.toFixed(2)} ${data.currency}, Solicitado: ${amount.toFixed(2)} ${data.currency}`
        );
      }
      newBalance = cleanDecimal(currentBalance - amount);
    }

    // 6. Ejecutar transacción atómica
    const batch = db.batch();

    // 6.1. Crear transacción
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

    // 6.2. Actualizar balance
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, {
      [`balances.${data.currency}`]: newBalance,
    });

    // 7. Commit
    await batch.commit();

    console.log(`[addCashTransaction] Éxito - transactionId: ${transactionRef.id}, newBalance: ${newBalance}`);

    return {
      success: true,
      transactionId: transactionRef.id,
      newBalance: newBalance,
    };

  } catch (error) {
    console.error(`[addCashTransaction] Error - userId: ${auth?.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al registrar transacción de efectivo: ${error.message}`);
  }
});

// ============================================================================
// FUNCIÓN: deleteAssets
// ============================================================================

/**
 * Elimina activos de una cuenta de portafolio
 * 
 * @param {object} data
 * @param {string} data.accountId - ID de la cuenta de portafolio
 * @param {string} [data.currency] - Moneda específica (opcional, elimina todos si no se especifica)
 * 
 * @returns {Promise<object>} { success, deletedCount }
 */
exports.deleteAssets = onCall(callableConfig, async (request) => {
  const { auth, data } = request;

  console.log(`[deleteAssets] Iniciando - userId: ${auth?.uid}, accountId: ${data?.accountId}`);

  try {
    // 1. Validar autenticación
    validateAuth(auth);

    // 2. Validar datos requeridos
    if (!data.accountId) {
      throw new HttpsError('invalid-argument', 'accountId es requerido');
    }

    // 3. Validar ownership de la cuenta
    await validateAccountOwnership(data.accountId, auth.uid);

    // 4. Buscar assets a eliminar
    let assetsQuery = db.collection('assets')
      .where('portfolioAccount', '==', data.accountId);
    
    if (data.currency) {
      assetsQuery = assetsQuery.where('currency', '==', data.currency);
    }

    const assetsSnapshot = await assetsQuery.get();

    if (assetsSnapshot.empty) {
      return { success: true, deletedCount: 0 };
    }

    // 5. Eliminar assets y sus transacciones asociadas
    const batch = db.batch();
    let deletedCount = 0;

    for (const assetDoc of assetsSnapshot.docs) {
      // Eliminar asset
      batch.delete(assetDoc.ref);
      deletedCount++;

      // Eliminar transacciones asociadas
      const transactionsQuery = db.collection('transactions')
        .where('assetId', '==', assetDoc.id);
      
      const transactionsSnapshot = await transactionsQuery.get();
      transactionsSnapshot.forEach(txDoc => {
        batch.delete(txDoc.ref);
      });
    }

    // 6. Commit
    await batch.commit();

    console.log(`[deleteAssets] Éxito - deletedCount: ${deletedCount}`);

    return {
      success: true,
      deletedCount: deletedCount,
    };

  } catch (error) {
    console.error(`[deleteAssets] Error - userId: ${auth?.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al eliminar activos: ${error.message}`);
  }
});

// ============================================================================
// FUNCIÓN: updateStockSector
// ============================================================================

/**
 * Actualiza el sector de un stock en currentPrices (fallback manual)
 * 
 * @description
 * Permite a los usuarios asignar manualmente el sector a stocks que no fueron
 * detectados automáticamente por el scraping de GoogleFinanceAPI.
 * 
 * @param {object} data
 * @param {string} data.symbol - Símbolo del stock (ej: "AAPL")
 * @param {string} data.sector - Sector a asignar
 * 
 * @returns {Promise<object>} { success, symbol, sector }
 */
exports.updateStockSector = onCall(callableConfig, async (request) => {
  const { auth, data } = request;

  console.log(`[updateStockSector] Iniciando - userId: ${auth?.uid}, symbol: ${data?.symbol}`);

  try {
    // 1. Validar autenticación
    validateAuth(auth);

    // 2. Validar datos requeridos
    if (!data.symbol || !data.sector) {
      throw new HttpsError('invalid-argument', 'symbol y sector son requeridos');
    }

    // 3. Verificar que el símbolo existe en currentPrices
    const priceRef = db.collection('currentPrices').doc(data.symbol);
    const priceDoc = await priceRef.get();

    if (!priceDoc.exists) {
      throw new HttpsError('not-found', `No se encontró el símbolo ${data.symbol} en currentPrices`);
    }

    // 4. Actualizar sector
    await priceRef.update({
      sector: data.sector,
      sectorUpdatedBy: auth.uid,
      sectorUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    console.log(`[updateStockSector] Éxito - symbol: ${data.symbol}, sector: ${data.sector}`);

    return {
      success: true,
      symbol: data.symbol,
      sector: data.sector,
    };

  } catch (error) {
    console.error(`[updateStockSector] Error - userId: ${auth?.uid}`, error);
    
    if (error instanceof HttpsError) {
      throw error;
    }
    
    throw new HttpsError('internal', `Error al actualizar sector: ${error.message}`);
  }
});
