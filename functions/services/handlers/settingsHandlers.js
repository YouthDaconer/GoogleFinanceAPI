/**
 * Settings Handlers - Lógica de negocio para operaciones de currencies y userData
 * 
 * SCALE-CF-001: Handlers extraídos de settingsOperations.js para consolidación
 * de Cloud Functions HTTP.
 * 
 * RBAC-001: Operaciones de currency requieren rol de administrador.
 * 
 * @module handlers/settingsHandlers
 * @see docs/stories/56.story.md
 * @see docs/architecture/role-based-access-control-design.md
 */

const { HttpsError } = require("firebase-functions/v2/https");
const admin = require('../firebaseAdmin');
const { requireAdmin } = require('../../utils/authorization');
const db = admin.firestore();

// ============================================================================
// CURRENCY HANDLERS - 🔒 SOLO ADMIN
// ============================================================================

/**
 * Agrega una nueva moneda al sistema
 * 
 * 🔒 RBAC: Requiere rol de administrador
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la moneda
 * @returns {Promise<{success: boolean, currencyId: string}>}
 */
async function addCurrency(context, payload) {
  // 🔒 RBAC-001: Verificar rol de admin
  requireAdmin(context);
  
  const { auth } = context;
  const { code, name, symbol, isActive, flagCurrency } = payload;

  console.log(`[settingsHandlers][addCurrency] Admin userId: ${auth.uid}, code: ${code}`);

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

  try {
    // Verificar si ya existe una moneda con ese código
    const existingQuery = await db.collection('currencies')
      .where('code', '==', code.toUpperCase())
      .get();

    if (!existingQuery.empty) {
      throw new HttpsError('already-exists', `Ya existe una moneda con el código ${code}`);
    }

    // HU #3: el catálogo guarda lo que es suyo —nombre, símbolo, bandera,
    // si está activa—. La tasa de cambio no: se consulta al canal de datos de
    // mercado cuando se necesita y no se persiste (RN-3-A).
    // @see platform-docs/stories/3-tasa-vigente-canal-mercado/refinamiento.md (T9, D9)
    const currencyData = {
      code: code.toUpperCase(),
      name,
      symbol,
      isActive: isActive !== false,
      ...(flagCurrency && { flagCurrency }),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdBy: auth.uid,
    };

    const docRef = await db.collection('currencies').add(currencyData);

    console.log(`[settingsHandlers][addCurrency] Éxito - currencyId: ${docRef.id}`);

    return {
      success: true,
      currencyId: docRef.id,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[settingsHandlers][addCurrency] Error:', error);
    throw new HttpsError('internal', 'Error al crear la moneda');
  }
}

/**
 * Actualiza una moneda existente
 * 
 * 🔒 RBAC: Requiere rol de administrador
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de actualización
 * @returns {Promise<{success: boolean}>}
 */
async function updateCurrency(context, payload) {
  // 🔒 RBAC-001: Verificar rol de admin
  requireAdmin(context);
  
  const { auth } = context;
  const { currencyId, updates } = payload;

  console.log(`[settingsHandlers][updateCurrency] Admin userId: ${auth.uid}, currencyId: ${currencyId}`);

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

    // HU #3: `exchangeRate` queda fuera de los campos permitidos. Está aquí y no
    // en el formulario porque es el único sitio por el que pasan todos los
    // llamadores: así un cliente antiguo tampoco puede reintroducir una tasa
    // escrita a mano (RN-3-A, D9).
    const allowedFields = ['name', 'symbol', 'isActive', 'flagCurrency'];
    const sanitizedUpdates = {};
    for (const field of allowedFields) {
      if (updates[field] !== undefined) {
        sanitizedUpdates[field] = updates[field];
      }
    }

    sanitizedUpdates.updatedAt = admin.firestore.FieldValue.serverTimestamp();
    sanitizedUpdates.updatedBy = auth.uid;

    await currencyRef.update(sanitizedUpdates);

    console.log(`[settingsHandlers][updateCurrency] Éxito - currencyId: ${currencyId}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[settingsHandlers][updateCurrency] Error:', error);
    throw new HttpsError('internal', 'Error al actualizar la moneda');
  }
}

/**
 * Elimina una moneda del sistema
 * 
 * 🔒 RBAC: Requiere rol de administrador
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la operación
 * @returns {Promise<{success: boolean}>}
 */
async function deleteCurrency(context, payload) {
  // 🔒 RBAC-001: Verificar rol de admin
  requireAdmin(context);
  
  const { auth } = context;
  const { currencyId } = payload;

  console.log(`[settingsHandlers][deleteCurrency] Admin userId: ${auth.uid}, currencyId: ${currencyId}`);

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

    console.log(`[settingsHandlers][deleteCurrency] Éxito - code: ${currencyData.code}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[settingsHandlers][deleteCurrency] Error:', error);
    throw new HttpsError('internal', 'Error al eliminar la moneda');
  }
}

// ============================================================================
// USER DATA HANDLERS - ✅ TODOS LOS USUARIOS (propio)
// ============================================================================

/**
 * Actualiza la moneda por defecto del usuario
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la operación
 * @returns {Promise<{success: boolean}>}
 */
async function updateDefaultCurrency(context, payload) {
  const { auth } = context;
  const { currencyCode } = payload;

  console.log(`[settingsHandlers][updateDefaultCurrency] userId: ${auth.uid}, currency: ${currencyCode}`);

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

    console.log(`[settingsHandlers][updateDefaultCurrency] Éxito - currency: ${currencyCode}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[settingsHandlers][updateDefaultCurrency] Error:', error);
    throw new HttpsError('internal', 'Error al actualizar la moneda predeterminada');
  }
}

/**
 * Actualiza el país del usuario
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la operación
 * @returns {Promise<{success: boolean}>}
 */
async function updateUserCountry(context, payload) {
  const { auth } = context;
  const { countryCode } = payload;

  console.log(`[settingsHandlers][updateUserCountry] userId: ${auth.uid}, country: ${countryCode}`);

  if (!countryCode || typeof countryCode !== 'string' || countryCode.length !== 2) {
    throw new HttpsError('invalid-argument', 'El código de país debe ser de 2 caracteres (ISO 3166-1)');
  }

  try {
    const userDataRef = db.collection('userData').doc(auth.uid);
    
    await userDataRef.set({
      countryCode: countryCode.toUpperCase(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    console.log(`[settingsHandlers][updateUserCountry] Éxito - country: ${countryCode}`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[settingsHandlers][updateUserCountry] Error:', error);
    throw new HttpsError('internal', 'Error al actualizar el país');
  }
}

/**
 * Actualiza el nombre para mostrar del usuario
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Datos de la operación
 * @returns {Promise<{success: boolean}>}
 */
async function updateUserDisplayName(context, payload) {
  const { auth } = context;
  const { displayName } = payload;

  console.log(`[settingsHandlers][updateUserDisplayName] userId: ${auth.uid}`);

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

    console.log(`[settingsHandlers][updateUserDisplayName] Éxito`);

    return { success: true };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[settingsHandlers][updateUserDisplayName] Error:', error);
    throw new HttpsError('internal', 'Error al actualizar el nombre');
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  addCurrency,
  updateCurrency,
  deleteCurrency,
  updateDefaultCurrency,
  updateUserCountry,
  updateUserDisplayName,
};
