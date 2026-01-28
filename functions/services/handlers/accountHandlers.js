/**
 * Account Handlers - Lógica de negocio para operaciones de portfolioAccounts
 * 
 * SCALE-CF-001: Handlers extraídos de portfolioAccountOperations.js para consolidación
 * de Cloud Functions HTTP.
 * 
 * @module handlers/accountHandlers
 * @see docs/stories/56.story.md
 */

const { HttpsError } = require("firebase-functions/v2/https");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");

// Importar invalidación de cache de distribución
const { invalidateDistributionCache } = require('../portfolioDistributionService');

const db = getFirestore();

// ============================================================================
// HANDLERS
// ============================================================================

/**
 * Crear una nueva cuenta de portafolio
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la cuenta
 * @returns {Promise<{success: boolean, accountId: string, account: Object}>}
 */
async function addPortfolioAccount(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { name, description, isActive, taxDeductionPercentage, balances } = payload;

  console.log(`[accountHandlers][addPortfolioAccount] userId: ${userId}, name: ${name}`);

  // Validaciones
  if (!name || typeof name !== "string" || name.trim().length === 0) {
    throw new HttpsError("invalid-argument", "El nombre de la cuenta es requerido");
  }

  try {
    const newAccount = {
      userId,
      name: name.trim(),
      description: description?.trim() || "",
      isActive: isActive !== undefined ? isActive : true,
      taxDeductionPercentage: taxDeductionPercentage || 0,
      balances: balances || {},
      createdAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    };

    const docRef = await db.collection("portfolioAccounts").add(newAccount);

    // Invalidar cache de distribución
    invalidateDistributionCache(userId);

    console.log(`[accountHandlers][addPortfolioAccount] Éxito - accountId: ${docRef.id}`);

    return {
      success: true,
      accountId: docRef.id,
      account: {
        id: docRef.id,
        ...newAccount,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      },
    };
  } catch (error) {
    console.error("[accountHandlers][addPortfolioAccount] Error:", error);
    throw new HttpsError("internal", `Error al crear la cuenta: ${error.message}`);
  }
}

/**
 * Actualizar una cuenta de portafolio existente
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de actualización
 * @returns {Promise<{success: boolean, accountId: string, updatedFields: string[]}>}
 */
async function updatePortfolioAccount(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountId, updates } = payload;

  console.log(`[accountHandlers][updatePortfolioAccount] userId: ${userId}, accountId: ${accountId}`);

  // Validaciones
  if (!accountId) {
    throw new HttpsError("invalid-argument", "El ID de la cuenta es requerido");
  }

  if (!updates || typeof updates !== "object") {
    throw new HttpsError("invalid-argument", "Los datos de actualización son requeridos");
  }

  try {
    const accountRef = db.collection("portfolioAccounts").doc(accountId);
    const accountDoc = await accountRef.get();

    if (!accountDoc.exists) {
      throw new HttpsError("not-found", "La cuenta no existe");
    }

    const accountData = accountDoc.data();
    if (accountData.userId !== userId) {
      throw new HttpsError("permission-denied", "No tienes permiso para actualizar esta cuenta");
    }

    // Campos permitidos para actualizar
    const allowedFields = ["name", "description", "isActive", "taxDeductionPercentage", "balances"];
    const sanitizedUpdates = {};

    for (const field of allowedFields) {
      if (updates[field] !== undefined) {
        sanitizedUpdates[field] = updates[field];
      }
    }

    sanitizedUpdates.updatedAt = FieldValue.serverTimestamp();

    await accountRef.update(sanitizedUpdates);

    // Invalidar cache de distribución
    invalidateDistributionCache(userId);

    console.log(`[accountHandlers][updatePortfolioAccount] Éxito - accountId: ${accountId}`);

    return {
      success: true,
      accountId,
      updatedFields: Object.keys(sanitizedUpdates).filter(k => k !== "updatedAt"),
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error("[accountHandlers][updatePortfolioAccount] Error:", error);
    throw new HttpsError("internal", `Error al actualizar la cuenta: ${error.message}`);
  }
}

/**
 * Eliminar una cuenta de portafolio y todos sus datos asociados
 * 
 * REF-SEC-002: Eliminación completa manejada en backend
 * Elimina atómicamente: assets, transacciones, referencia en portfolioDistribution, y la cuenta
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la cuenta
 * @returns {Promise<{success: boolean, accountId: string, deletedAccountName: string, deletedAssets: number, deletedTransactions: number}>}
 */
async function deletePortfolioAccount(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountId } = payload;

  console.log(`[accountHandlers][deletePortfolioAccount] userId: ${userId}, accountId: ${accountId}`);

  if (!accountId) {
    throw new HttpsError("invalid-argument", "El ID de la cuenta es requerido");
  }

  try {
    const accountRef = db.collection("portfolioAccounts").doc(accountId);
    const accountDoc = await accountRef.get();

    if (!accountDoc.exists) {
      throw new HttpsError("not-found", "La cuenta no existe");
    }

    const accountData = accountDoc.data();
    if (accountData.userId !== userId) {
      throw new HttpsError("permission-denied", "No tienes permiso para eliminar esta cuenta");
    }

    // =========================================================================
    // REF-SEC-002: Eliminación completa de datos asociados
    // =========================================================================
    
    let deletedAssetsCount = 0;
    let deletedTransactionsCount = 0;

    // 1. Eliminar todos los assets asociados a esta cuenta
    console.log(`[accountHandlers][deletePortfolioAccount] Eliminando assets de la cuenta ${accountId}`);
    // FIX-DELETE-001: Campo correcto es "portfolioAccount" no "portfolioAccountId"
    const assetsQuery = db.collection("assets")
      .where("portfolioAccount", "==", accountId)
      .where("userId", "==", userId);
    
    const assetsSnapshot = await assetsQuery.get();
    
    if (!assetsSnapshot.empty) {
      const batch = db.batch();
      assetsSnapshot.docs.forEach(doc => {
        batch.delete(doc.ref);
      });
      await batch.commit();
      deletedAssetsCount = assetsSnapshot.size;
      console.log(`[accountHandlers][deletePortfolioAccount] Eliminados ${deletedAssetsCount} assets`);
    }

    // 2. Eliminar todas las transacciones asociadas a esta cuenta
    console.log(`[accountHandlers][deletePortfolioAccount] Eliminando transacciones de la cuenta ${accountId}`);
    const transactionsQuery = db.collection("transactions")
      .where("portfolioAccountId", "==", accountId);
    
    const transactionsSnapshot = await transactionsQuery.get();
    
    if (!transactionsSnapshot.empty) {
      // Eliminar en batches de 500 (límite de Firestore)
      const BATCH_SIZE = 500;
      const docs = transactionsSnapshot.docs;
      
      for (let i = 0; i < docs.length; i += BATCH_SIZE) {
        const batch = db.batch();
        const batchDocs = docs.slice(i, i + BATCH_SIZE);
        batchDocs.forEach(doc => {
          batch.delete(doc.ref);
        });
        await batch.commit();
      }
      deletedTransactionsCount = transactionsSnapshot.size;
      console.log(`[accountHandlers][deletePortfolioAccount] Eliminadas ${deletedTransactionsCount} transacciones`);
    }

    // 3. Eliminar la referencia de la cuenta en portfolioDistribution
    console.log(`[accountHandlers][deletePortfolioAccount] Limpiando portfolioDistribution`);
    const distributionRef = db.collection("portfolioDistribution").doc(userId);
    const distributionDoc = await distributionRef.get();
    
    if (distributionDoc.exists) {
      const distributionData = distributionDoc.data();
      if (distributionData?.accounts && distributionData.accounts[accountId]) {
        await distributionRef.update({
          [`accounts.${accountId}`]: FieldValue.delete()
        });
        console.log(`[accountHandlers][deletePortfolioAccount] Referencia eliminada de portfolioDistribution`);
      }
    }

    // 4. Eliminar la cuenta
    await accountRef.delete();

    // 5. Invalidar cache de distribución
    invalidateDistributionCache(userId);

    console.log(`[accountHandlers][deletePortfolioAccount] Éxito - accountId: ${accountId}, assets: ${deletedAssetsCount}, transactions: ${deletedTransactionsCount}`);

    return {
      success: true,
      accountId,
      deletedAccountName: accountData.name,
      deletedAssets: deletedAssetsCount,
      deletedTransactions: deletedTransactionsCount,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error("[accountHandlers][deletePortfolioAccount] Error:", error);
    throw new HttpsError("internal", `Error al eliminar la cuenta: ${error.message}`);
  }
}

/**
 * Actualizar el balance de una moneda específica en una cuenta
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos del balance
 * @returns {Promise<{success: boolean, accountId: string, currency: string, previousBalance: number, newBalance: number, operation: string}>}
 */
async function updatePortfolioAccountBalance(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountId, currency, amount, operation } = payload;

  console.log(`[accountHandlers][updatePortfolioAccountBalance] userId: ${userId}, accountId: ${accountId}, currency: ${currency}`);

  // Validaciones
  if (!accountId) {
    throw new HttpsError("invalid-argument", "El ID de la cuenta es requerido");
  }

  if (!currency || typeof currency !== "string") {
    throw new HttpsError("invalid-argument", "La moneda es requerida");
  }

  if (typeof amount !== "number") {
    throw new HttpsError("invalid-argument", "El monto debe ser un número");
  }

  // operation puede ser 'add' (sumar al balance), 'set' (establecer valor exacto), o 'subtract' (restar)
  const validOperations = ["add", "set", "subtract"];
  const op = operation || "add";
  if (!validOperations.includes(op)) {
    throw new HttpsError("invalid-argument", `Operación inválida. Debe ser: ${validOperations.join(", ")}`);
  }

  try {
    const accountRef = db.collection("portfolioAccounts").doc(accountId);
    const accountDoc = await accountRef.get();

    if (!accountDoc.exists) {
      throw new HttpsError("not-found", "La cuenta no existe");
    }

    const accountData = accountDoc.data();
    if (accountData.userId !== userId) {
      throw new HttpsError("permission-denied", "No tienes permiso para actualizar esta cuenta");
    }

    const currentBalance = accountData.balances?.[currency] || 0;
    let newBalance;

    switch (op) {
      case "add":
        newBalance = currentBalance + amount;
        break;
      case "subtract":
        newBalance = currentBalance - amount;
        break;
      case "set":
        newBalance = amount;
        break;
      default:
        newBalance = currentBalance + amount;
    }

    await accountRef.update({
      [`balances.${currency}`]: newBalance,
      updatedAt: FieldValue.serverTimestamp(),
    });

    console.log(`[accountHandlers][updatePortfolioAccountBalance] Éxito - ${currency}: ${currentBalance} -> ${newBalance}`);

    return {
      success: true,
      accountId,
      currency,
      previousBalance: currentBalance,
      newBalance,
      operation: op,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error("[accountHandlers][updatePortfolioAccountBalance] Error:", error);
    throw new HttpsError("internal", `Error al actualizar el balance: ${error.message}`);
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  addPortfolioAccount,
  updatePortfolioAccount,
  deletePortfolioAccount,
  updatePortfolioAccountBalance,
};
