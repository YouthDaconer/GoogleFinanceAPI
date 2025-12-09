/**
 * Cloud Functions Callable para Operaciones de Settings (Currencies y UserData)
 * 
 * Este módulo implementa las operaciones de escritura de currencies y userData
 * que antes se realizaban directamente en el frontend.
 * 
 * Siguiendo principios SOLID:
 * - Single Responsibility: Cada función tiene una única responsabilidad
 * - Open/Closed: Extensible sin modificar código existente
 * 
 * @module settingsOperations
 * @see docs/stories/27.story.md - REF-005
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require('./firebaseAdmin');
const db = admin.firestore();

/**
 * Configuración común para Cloud Functions Callable
 */
const callableConfig = {
  cors: true,
  enforceAppCheck: false,
  timeoutSeconds: 30,
  memory: "256MiB",
};

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

// ============================================================================
// CURRENCY OPERATIONS
// ============================================================================

/**
 * Agrega una nueva moneda al sistema
 * 
 * @param {object} data - Datos de la moneda
 * @param {string} data.code - Código de la moneda (ej: "USD", "COP")
 * @param {string} data.name - Nombre de la moneda
 * @param {string} data.symbol - Símbolo de la moneda (ej: "$", "€")
 * @param {number} data.exchangeRate - Tasa de cambio respecto a USD
 * @param {boolean} data.isActive - Si la moneda está activa
 * @param {string} [data.flagCurrency] - URL de la bandera (opcional)
 * @returns {Promise<{success: boolean, currencyId: string}>}
 */
exports.addCurrency = onCall(callableConfig, async (request) => {
  const { auth, data } = request;
  validateAuth(auth);

  const { code, name, symbol, exchangeRate, isActive, flagCurrency } = data;

  // Validaciones
  if (!code || typeof code !== 'string' || code.length < 2 || code.length > 5) {
    throw new HttpsError('invalid-argument', 'El código de moneda es inválido');
  }
  if (!name || typeof name !== 'string') {
    throw new HttpsError('invalid-argument', 'El nombre de la moneda es requerido');
  }
  if (!symbol || typeof symbol !== 'string') {
    throw new HttpsError('invalid-argument', 'El símbolo de la moneda es requerido');
  }
  if (typeof exchangeRate !== 'number' || exchangeRate <= 0) {
    throw new HttpsError('invalid-argument', 'La tasa de cambio debe ser un número positivo');
  }

  try {
    // Verificar si ya existe una moneda con ese código
    const existingQuery = await db.collection('currencies')
      .where('code', '==', code.toUpperCase())
      .get();

    if (!existingQuery.empty) {
      throw new HttpsError('already-exists', `Ya existe una moneda con el código ${code}`);
    }

    const currencyData = {
      code: code.toUpperCase(),
      name,
      symbol,
      exchangeRate,
      isActive: isActive !== false, // Default true
      ...(flagCurrency && { flagCurrency }),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdBy: auth.uid,
    };

    const docRef = await db.collection('currencies').add(currencyData);

    console.log(`[addCurrency] Moneda ${code} creada por usuario ${auth.uid}`);

    return {
      success: true,
      currencyId: docRef.id,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[addCurrency] Error:', error);
    throw new HttpsError('internal', 'Error al crear la moneda');
  }
});

/**
 * Actualiza una moneda existente
 * 
 * @param {object} data - Datos de actualización
 * @param {string} data.currencyId - ID de la moneda a actualizar
 * @param {object} data.updates - Campos a actualizar
 * @returns {Promise<{success: boolean}>}
 */
exports.updateCurrency = onCall(callableConfig, async (request) => {
  const { auth, data } = request;
  validateAuth(auth);

  const { currencyId, updates } = data;

  if (!currencyId || typeof currencyId !== 'string') {
    throw new HttpsError('invalid-argument', 'El ID de la moneda es requerido');
  }
  if (!updates || typeof updates !== 'object') {
    throw new HttpsError('invalid-argument', 'Los datos de actualización son requeridos');
  }

  try {
    const currencyRef = db.collection('currencies').doc(currencyId);
    const currencyDoc = await currencyRef.get();

    if (!currencyDoc.exists) {
      throw new HttpsError('not-found', 'La moneda no existe');
    }

    const currentData = currencyDoc.data();

    // Validar que no se desactive USD
    if (updates.isActive === false && currentData.code === 'USD') {
      throw new HttpsError(
        'failed-precondition',
        'No se puede desactivar el USD ya que es la moneda base del sistema'
      );
    }

    // Validar exchangeRate si se proporciona
    if (updates.exchangeRate !== undefined && 
        (typeof updates.exchangeRate !== 'number' || updates.exchangeRate <= 0)) {
      throw new HttpsError('invalid-argument', 'La tasa de cambio debe ser un número positivo');
    }

    // Filtrar campos permitidos
    const allowedFields = ['name', 'symbol', 'exchangeRate', 'isActive', 'flagCurrency'];
    const sanitizedUpdates = {};
    for (const field of allowedFields) {
      if (updates[field] !== undefined) {
        sanitizedUpdates[field] = updates[field];
      }
    }

    sanitizedUpdates.updatedAt = admin.firestore.FieldValue.serverTimestamp();
    sanitizedUpdates.updatedBy = auth.uid;

    await currencyRef.update(sanitizedUpdates);

    console.log(`[updateCurrency] Moneda ${currencyId} actualizada por usuario ${auth.uid}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[updateCurrency] Error:', error);
    throw new HttpsError('internal', 'Error al actualizar la moneda');
  }
});

/**
 * Elimina una moneda del sistema
 * 
 * @param {object} data - Datos de la operación
 * @param {string} data.currencyId - ID de la moneda a eliminar
 * @returns {Promise<{success: boolean}>}
 */
exports.deleteCurrency = onCall(callableConfig, async (request) => {
  const { auth, data } = request;
  validateAuth(auth);

  const { currencyId } = data;

  if (!currencyId || typeof currencyId !== 'string') {
    throw new HttpsError('invalid-argument', 'El ID de la moneda es requerido');
  }

  try {
    const currencyRef = db.collection('currencies').doc(currencyId);
    const currencyDoc = await currencyRef.get();

    if (!currencyDoc.exists) {
      throw new HttpsError('not-found', 'La moneda no existe');
    }

    const currencyData = currencyDoc.data();

    // No permitir eliminar USD
    if (currencyData.code === 'USD') {
      throw new HttpsError(
        'failed-precondition',
        'No se puede eliminar el USD ya que es la moneda base del sistema'
      );
    }

    // Verificar si hay assets usando esta moneda
    const assetsQuery = await db.collection('assets')
      .where('currency', '==', currencyData.code)
      .limit(1)
      .get();

    if (!assetsQuery.empty) {
      throw new HttpsError(
        'failed-precondition',
        `No se puede eliminar ${currencyData.code} porque hay activos que la usan`
      );
    }

    await currencyRef.delete();

    console.log(`[deleteCurrency] Moneda ${currencyData.code} eliminada por usuario ${auth.uid}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[deleteCurrency] Error:', error);
    throw new HttpsError('internal', 'Error al eliminar la moneda');
  }
});

// ============================================================================
// USER DATA OPERATIONS
// ============================================================================

/**
 * Actualiza la moneda por defecto del usuario
 * 
 * @param {object} data - Datos de la operación
 * @param {string} data.currencyCode - Código de la moneda a establecer como predeterminada
 * @returns {Promise<{success: boolean}>}
 */
exports.updateDefaultCurrency = onCall(callableConfig, async (request) => {
  const { auth, data } = request;
  validateAuth(auth);

  const { currencyCode } = data;

  if (!currencyCode || typeof currencyCode !== 'string') {
    throw new HttpsError('invalid-argument', 'El código de moneda es requerido');
  }

  try {
    // Verificar que la moneda existe y está activa
    const currencyQuery = await db.collection('currencies')
      .where('code', '==', currencyCode)
      .where('isActive', '==', true)
      .get();

    if (currencyQuery.empty) {
      throw new HttpsError(
        'not-found',
        `La moneda ${currencyCode} no existe o no está activa`
      );
    }

    const userDataRef = db.collection('userData').doc(auth.uid);
    
    await userDataRef.set({
      defaultCurrency: currencyCode,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    console.log(`[updateDefaultCurrency] Usuario ${auth.uid} cambió moneda predeterminada a ${currencyCode}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[updateDefaultCurrency] Error:', error);
    throw new HttpsError('internal', 'Error al actualizar la moneda predeterminada');
  }
});

/**
 * Actualiza el país del usuario
 * 
 * @param {object} data - Datos de la operación
 * @param {string} data.countryCode - Código del país (ISO 3166-1 alpha-2)
 * @returns {Promise<{success: boolean}>}
 */
exports.updateUserCountry = onCall(callableConfig, async (request) => {
  const { auth, data } = request;
  validateAuth(auth);

  const { countryCode } = data;

  if (!countryCode || typeof countryCode !== 'string' || countryCode.length !== 2) {
    throw new HttpsError('invalid-argument', 'El código de país debe ser de 2 caracteres (ISO 3166-1)');
  }

  try {
    const userDataRef = db.collection('userData').doc(auth.uid);
    
    await userDataRef.set({
      countryCode: countryCode.toUpperCase(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    console.log(`[updateUserCountry] Usuario ${auth.uid} cambió país a ${countryCode}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[updateUserCountry] Error:', error);
    throw new HttpsError('internal', 'Error al actualizar el país');
  }
});

/**
 * Actualiza el nombre para mostrar del usuario
 * 
 * @param {object} data - Datos de la operación
 * @param {string} data.displayName - Nombre para mostrar
 * @returns {Promise<{success: boolean}>}
 */
exports.updateUserDisplayName = onCall(callableConfig, async (request) => {
  const { auth, data } = request;
  validateAuth(auth);

  const { displayName } = data;

  if (!displayName || typeof displayName !== 'string' || displayName.trim().length === 0) {
    throw new HttpsError('invalid-argument', 'El nombre para mostrar es requerido');
  }

  if (displayName.length > 100) {
    throw new HttpsError('invalid-argument', 'El nombre no puede exceder 100 caracteres');
  }

  try {
    const userDataRef = db.collection('userData').doc(auth.uid);
    
    await userDataRef.set({
      displayName: displayName.trim(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    console.log(`[updateUserDisplayName] Usuario ${auth.uid} cambió nombre a ${displayName}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[updateUserDisplayName] Error:', error);
    throw new HttpsError('internal', 'Error al actualizar el nombre');
  }
});
