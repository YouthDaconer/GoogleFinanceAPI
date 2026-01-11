/**
 * portfolioAccountOperations.js
 * 
 * Cloud Functions para operaciones CRUD de portfolioAccounts.
 * Estas funciones reemplazan las escrituras directas desde el frontend.
 * 
 * Operaciones:
 * - addPortfolioAccount: Crear nueva cuenta
 * - updatePortfolioAccount: Actualizar cuenta existente
 * - deletePortfolioAccount: Eliminar cuenta
 * - updatePortfolioAccountBalance: Actualizar balance de una moneda
 * 
 * @see docs/stories/9.story.md - OPT-004 PortfolioDataContext Centralizado
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");

// Importar rate limiter (SCALE-BE-004)
const { withRateLimit } = require('../utils/rateLimiter');

// Importar invalidación de cache de distribución
const { invalidateDistributionCache } = require('./portfolioDistributionService');

const db = getFirestore();

/**
 * Verifica que el usuario esté autenticado
 */
function verifyAuth(request) {
  if (!request.auth) {
    throw new HttpsError(
      "unauthenticated",
      "Debes iniciar sesión para realizar esta operación"
    );
  }
  return request.auth.uid;
}

/**
 * Crear una nueva cuenta de portafolio
 */
const addPortfolioAccount = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 30,
  },
  withRateLimit('addPortfolioAccount')(async (request) => {
    const userId = verifyAuth(request);
    const { name, description, isActive, taxDeductionPercentage, balances } = request.data;

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

      console.log(`[addPortfolioAccount] Cuenta creada: ${docRef.id} para usuario ${userId}`);

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
      console.error("[addPortfolioAccount] Error:", error);
      throw new HttpsError("internal", `Error al crear la cuenta: ${error.message}`);
    }
  })
);

/**
 * Actualizar una cuenta de portafolio existente
 */
const updatePortfolioAccount = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 30,
  },
  withRateLimit('updatePortfolioAccount')(async (request) => {
    const userId = verifyAuth(request);
    const { accountId, updates } = request.data;

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

      console.log(`[updatePortfolioAccount] Cuenta actualizada: ${accountId}`);

      return {
        success: true,
        accountId,
        updatedFields: Object.keys(sanitizedUpdates).filter(k => k !== "updatedAt"),
      };
    } catch (error) {
      if (error instanceof HttpsError) throw error;
      console.error("[updatePortfolioAccount] Error:", error);
      throw new HttpsError("internal", `Error al actualizar la cuenta: ${error.message}`);
    }
  })
);

/**
 * Eliminar una cuenta de portafolio
 * NOTA: Esta función solo elimina la cuenta. Los assets y transacciones
 * deben eliminarse previamente desde el frontend usando las Cloud Functions correspondientes.
 */
const deletePortfolioAccount = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 60,
  },
  withRateLimit('deletePortfolioAccount')(async (request) => {
    const userId = verifyAuth(request);
    const { accountId } = request.data;

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

      // Eliminar la cuenta
      await accountRef.delete();

      // Invalidar cache de distribución
      invalidateDistributionCache(userId);

      console.log(`[deletePortfolioAccount] Cuenta eliminada: ${accountId}`);

      return {
        success: true,
        accountId,
        deletedAccountName: accountData.name,
      };
    } catch (error) {
      if (error instanceof HttpsError) throw error;
      console.error("[deletePortfolioAccount] Error:", error);
      throw new HttpsError("internal", `Error al eliminar la cuenta: ${error.message}`);
    }
  })
);

/**
 * Actualizar el balance de una moneda específica en una cuenta
 */
const updatePortfolioAccountBalance = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 30,
  },
  withRateLimit('updatePortfolioAccountBalance')(async (request) => {
    const userId = verifyAuth(request);
    const { accountId, currency, amount, operation } = request.data;

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

      console.log(`[updatePortfolioAccountBalance] Balance actualizado: ${accountId}, ${currency}: ${currentBalance} -> ${newBalance}`);

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
      console.error("[updatePortfolioAccountBalance] Error:", error);
      throw new HttpsError("internal", `Error al actualizar el balance: ${error.message}`);
    }
  })
);

module.exports = {
  addPortfolioAccount,
  updatePortfolioAccount,
  deletePortfolioAccount,
  updatePortfolioAccountBalance,
};
