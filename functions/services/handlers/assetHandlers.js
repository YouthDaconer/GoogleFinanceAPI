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

// LATE-REG-002: Detección y marcado de transacciones retroactivas
const { checkAndMarkStaleIfRetroactive } = require('../../utils/performanceStaleMarker');

// Importar getQuotes para crear currentPrices de nuevos tickers
const { getQuotes } = require('../financeQuery');

// Importar generador de logos
const { generateLogoUrl } = require('../../utils/logoGenerator');

// HU 2.1: base de costo de los saldos de efectivo y tasa de cambio por fecha
// HU 2.3: `deriveAverageRate` para derivar la tasa de compra desde el saldo
const {
  buildBalanceUpdate,
  deriveAverageRate,
  getUserReferenceCurrency,
} = require('../helpers/balanceCostBasis');
const historicalRateService = require('../historicalRateService');

// HU 2.4: el dinero que nace dentro de la cuenta entra con la tasa de su propio
// día, y el resultado realizado se descompone en mérito del activo y efecto divisa
const {
  resolveLotAcquisitionRate,
  resolveRealizationRate,
  decomposeRealizedResult,
  buildRealizedFxFields,
} = require('../helpers/realizedFxDecomposition');
const { backfillRealizedFxForUser } = require('../realizedFxBackfill');

// HU 2.5: cuando la divisa SALE de la cuenta —retiro o conversión— la
// revaluación deja de ser potencial y se realiza (RN-07). Fórmula única.
const {
  computeOutflowRealizedFx,
  buildOutflowFxFields,
  OUTFLOW_RATE_SOURCES,
  OUTFLOW_FX_AVAILABILITY,
} = require('../helpers/realizedFxOnOutflow');

// HU 2.6: ningun saldo se mueve sin dejar asiento, y el saldo pasa a ser la
// proyeccion de su historial (RN-06)
const {
  buildAdjustment,
  resolveAdjustmentRate,
  requiresExchangeRate,
  MIN_ADJUSTMENT_DELTA,
  ADJUSTMENT_REASONS,
} = require('../helpers/balanceAdjustment');
const { projectBalanceLedger, resolveCashImpact } = require('../helpers/balanceLedger');
const { migrateBalanceLedgerForUser } = require('../balanceLedgerMigration');

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
 * FIX-TIMESTAMP-001: Combina una fecha seleccionada por el usuario con la hora actual del servidor
 * 
 * Esta función garantiza precisión temporal en el registro de transacciones y assets,
 * permitiendo ordenamiento determinístico cuando hay múltiples operaciones el mismo día.
 * 
 * @param {string} dateString - Fecha en formato YYYY-MM-DD
 * @returns {string} Fecha ISO con hora actual del servidor (ej: "2025-01-28T14:30:45.123Z")
 * @example
 * // Si dateString es "2025-01-28" y la hora actual es 14:30:45.123
 * combineDateWithCurrentTime("2025-01-28") // => "2025-01-28T14:30:45.123Z"
 */
const combineDateWithCurrentTime = (dateString) => {
  // Si ya tiene formato ISO con hora, retornarlo como está
  if (dateString && dateString.includes('T')) {
    return dateString;
  }
  
  const now = new Date();
  const [year, month, day] = dateString.split('-').map(Number);
  
  // Crear fecha combinando la fecha del usuario con la hora actual
  const combined = new Date(
    year,
    month - 1, // JavaScript months are 0-indexed
    day,
    now.getHours(),
    now.getMinutes(),
    now.getSeconds(),
    now.getMilliseconds()
  );
  
  return combined.toISOString();
};

/**
 * HU 2.3 — Tasa de una fecha sin propagar los fallos del proveedor.
 *
 * La trazabilidad cambiaria nunca debe tumbar una compra: si el servicio de
 * tasas falla o la fecha es inválida, se devuelve `null` y quien llama decide
 * cómo declarar la ausencia (RN-13).
 *
 * @param {string} currency - Divisa a valorar contra USD
 * @param {string} date - Fecha `YYYY-MM-DD`
 * @returns {Promise<{rate: number}|null>}
 */
const getRateForDateSafe = async (currency, date) => {
  try {
    return await historicalRateService.getRateForDate(currency, date);
  } catch (error) {
    console.warn(`[assetHandlers] No se pudo resolver la tasa de ${currency} en ${date}: ${error.message}`);
    return null;
  }
};

/**
 * HU 2.3 — Tasa cruzada de una fecha, con la misma tolerancia a fallos.
 *
 * @param {string} fromCurrency - Divisa de origen
 * @param {string} toCurrency - Divisa de destino
 * @param {string} date - Fecha `YYYY-MM-DD`
 * @returns {Promise<{rate: number}|null>}
 */
const getCrossRateSafe = async (fromCurrency, toCurrency, date) => {
  try {
    return await historicalRateService.getCrossRate(fromCurrency, toCurrency, date);
  } catch (error) {
    console.warn(`[assetHandlers] No se pudo resolver la tasa ${fromCurrency}→${toCurrency} en ${date}: ${error.message}`);
    return null;
  }
};

/**
 * HU 2.3 — Base cambiaria de una compra, derivada del saldo que la paga.
 *
 * El usuario ya no declara a qué tipo de cambio adquirió el dinero con el que
 * compra: no puede saberlo. La tasa sale del efectivo que sale de la cuenta
 * (RN-01, RN-04), y esta función es el único sitio donde se decide cuál es.
 *
 * **Precedencia** (D2 del refinamiento):
 *
 * | # | Condición                                   | Fuente           |
 * |---|---------------------------------------------|------------------|
 * | 1 | La divisa del activo es la de referencia    | `identity`       |
 * | 2 | El saldo tiene base de costo determinable   | `balance-average`|
 * | 3 | No la tiene → tasa de mercado de la fecha   | `market-date`    |
 * | 4 | Tampoco hay tasa de mercado                 | `unavailable`    |
 *
 * La rama 3 existe porque una compra **no puede bloquearse por un dato de
 * trazabilidad**: el usuario compró y el saldo tiene que bajar. Lo único que
 * bloquea es la validación de fondos (RN-2.3-B).
 *
 * `acquisitionDollarValue` se resuelve aparte y conserva EXACTAMENTE la
 * semántica que tiene hoy —"unidades de `defaultCurrencyForAdquisitionDollar`
 * por 1 USD"— porque seis calculadores en producción la leen así (D3). Lo único
 * que cambia es que la calcula el servidor en vez del formulario.
 *
 * @param {Object} params
 * @param {Object} params.account - Documento de la cuenta que paga
 * @param {string} params.assetCurrency - Divisa en la que cotiza el activo
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {string} params.rateDate - Fecha de la compra `YYYY-MM-DD`
 * @param {number} [params.declaredRate] - Tasa que mandó el cliente. Se ignora
 *   para activos en USD; en los demás solo se usa si el mercado no responde
 * @returns {Promise<{acquisitionRate: number|null, acquisitionRateSource: string,
 *   acquisitionDollarValue: number, anchorCurrency: string}>}
 */
const resolveAcquisitionBasis = async ({
  account,
  assetCurrency,
  referenceCurrency,
  rateDate,
  declaredRate,
}) => {
  // --- Tasa de adquisición: unidades de la moneda de referencia por 1 unidad
  //     de la divisa del activo. Misma convención que 2.1 y 2.2.
  let acquisitionRate = null;
  let acquisitionRateSource = 'unavailable';

  if (assetCurrency === referenceCurrency) {
    // Sin exposición cambiaria no hay nada que derivar (RN-14).
    acquisitionRate = 1;
    acquisitionRateSource = 'identity';
  } else {
    const averageRate = deriveAverageRate(
      account?.balanceCostBasis?.[assetCurrency],
      account?.balances?.[assetCurrency] || 0,
      referenceCurrency
    );

    if (averageRate !== null) {
      acquisitionRate = averageRate;
      acquisitionRateSource = 'balance-average';
    } else {
      const resolved = await getCrossRateSafe(assetCurrency, referenceCurrency, rateDate);
      if (resolved) {
        acquisitionRate = resolved.rate;
        acquisitionRateSource = 'market-date';
      }
    }
  }

  // --- `acquisitionDollarValue`: se conserva la regla vigente tal cual (D3).
  let anchorCurrency;
  let acquisitionDollarValue;

  if (assetCurrency === 'USD') {
    // Activo en dólares: el ancla es la moneda de referencia y la cifra es la
    // misma tasa derivada. Idéntico a lo que se guardaba cuando la declaraba el
    // usuario, solo que ahora sale del saldo.
    anchorCurrency = referenceCurrency;

    if (acquisitionRate !== null) {
      acquisitionDollarValue = acquisitionRate;
    } else {
      const referenceUsdRate = await getRateForDateSafe(referenceCurrency, rateDate);
      acquisitionDollarValue = referenceUsdRate?.rate ?? 1;
    }
  } else {
    // Activo en otra divisa: el ancla sigue siendo la propia divisa del activo y
    // la cifra, su tasa de mercado contra el dólar — exactamente lo que el
    // formulario iba a buscar. Se mueve al servidor y nada más.
    anchorCurrency = assetCurrency;

    const marketRate = await getRateForDateSafe(assetCurrency, rateDate);
    acquisitionDollarValue = marketRate?.rate ?? (Number(declaredRate) || 1);
  }

  return { acquisitionRate, acquisitionRateSource, acquisitionDollarValue, anchorCurrency };
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
 * 
 * FIX-DECIMAL-001: Se usa tolerancia de 0.01 (1 centavo) para evitar falsos positivos
 * por errores de punto flotante. Los valores se comparan redondeados a 2 decimales
 * que es la precisión monetaria estándar.
 * 
 * Ejemplo del problema:
 * - units: 0.4689, unitValue: 75.95 → totalCost: 35.6177955
 * - currentBalance: 35.61 (en Firestore)
 * - Sin tolerancia: 35.61 < 35.6177955 → "Saldo insuficiente" (falso positivo)
 * - Con tolerancia: 35.61 + 0.01 >= 35.6177955 → OK
 * 
 * @param {object} account - Datos de la cuenta
 * @param {string} currency - Moneda a verificar
 * @param {number} requiredAmount - Monto requerido
 * @throws {HttpsError} Si el saldo es insuficiente
 */
const validateSufficientFunds = (account, currency, requiredAmount) => {
  const currentBalance = account.balances?.[currency] || 0;
  
  // FIX-DECIMAL-001: Redondear a 2 decimales para comparación monetaria precisa
  // Esto evita falsos positivos por errores de punto flotante
  const roundedBalance = Math.round(currentBalance * 100) / 100;
  const roundedRequired = Math.round(requiredAmount * 100) / 100;
  
  // Usar tolerancia de 1 centavo para casos límite (ej: 35.61 vs 35.6177955)
  const EPSILON = 0.01;
  
  if (roundedBalance + EPSILON < roundedRequired) {
    throw new HttpsError(
      'failed-precondition',
      `Saldo insuficiente. Disponible: ${roundedBalance.toFixed(2)} ${currency}, Requerido: ${roundedRequired.toFixed(2)} ${currency}`
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

    // FIX-TIMESTAMP-001: Combinar fecha con hora actual para precisión temporal
    const acquisitionDateWithTime = combineDateWithCurrentTime(data.acquisitionDate);

    // 4.1. HU 2.3: la tasa de la compra se DERIVA del saldo que la paga; el
    // usuario ya no la declara (RN-04). La fecha de la tasa sale del día que
    // eligió el usuario, no del ISO resultante: `combineDateWithCurrentTime`
    // devuelve UTC y de tarde en América ya cae en el día siguiente (bug
    // detectado en 2.1).
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);
    const rateDate = String(data.acquisitionDate).substring(0, 10);

    const {
      acquisitionRate,
      acquisitionRateSource,
      acquisitionDollarValue,
      anchorCurrency,
    } = await resolveAcquisitionBasis({
      account,
      assetCurrency: data.currency,
      referenceCurrency,
      rateDate,
      declaredRate: data.acquisitionDollarValue,
    });

    // Costo que el efectivo libera y que el activo hereda: es el mismo número
    // visto desde los dos lados (RN-01). Si la tasa no fue determinable, el
    // costo se declara ausente en vez de inventarse (RN-13).
    const acquisitionCost = acquisitionRate === null
      ? null
      : cleanDecimal(totalCost * acquisitionRate, 2);

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
      acquisitionDate: acquisitionDateWithTime,
      // Semántica intacta: unidades de `defaultCurrencyForAdquisitionDollar`
      // por 1 USD, ahora resuelta por el servidor (D3).
      acquisitionDollarValue: cleanDecimal(acquisitionDollarValue),
      defaultCurrencyForAdquisitionDollar: anchorCurrency,
      // HU 2.3: la base cambiaria real, en la convención de 2.1 y 2.2 —
      // unidades de la moneda de referencia por 1 unidad de la divisa del
      // activo. Es lo que hace que un activo en euros reciba el mismo
      // tratamiento que uno en dólares, sin lógica por divisa.
      acquisitionRate: acquisitionRate !== null ? cleanDecimal(acquisitionRate) : null,
      acquisitionRateSource,
      acquisitionCost,
      referenceCurrency,
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
      date: acquisitionDateWithTime,
      portfolioAccountId: data.portfolioAccount,
      commission: commission,
      assetType: data.assetType,
      dollarPriceToDate: cleanDecimal(acquisitionDollarValue),
      market: data.market || '',
      defaultCurrencyForAdquisitionDollar: anchorCurrency,
      // HU 2.3: trazabilidad del costo, mismo vocabulario que `cash_conversion`.
      acquisitionRate: acquisitionRate !== null ? cleanDecimal(acquisitionRate) : null,
      acquisitionRateSource,
      acquisitionCost,
      // El costo sale del efectivo y entra en el activo: es el mismo número
      // visto desde los dos lados.
      releasedCost: acquisitionCost,
      // RN-07: comprar NO realiza diferencia en cambio. El efecto cambiario
      // queda diferido dentro de la posición y se realizará al vender. El cero
      // es una afirmación, no un hueco que 2.5 tenga que interpretar.
      realizedFxAmount: 0,
      realizedFxCurrency: referenceCurrency,
      referenceCurrency,
      userId: auth.uid,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    batch.set(transactionRef, transactionData);

    // 5.3. Actualizar balance de la cuenta
    // HU 2.1: la compra saca efectivo, así que retira costo a la tasa promedio
    // del saldo. HU 2.3: esa misma tasa es la que hereda el activo, y ambas
    // cosas ocurren dentro de este batch — la derivación no puede quedar
    // desacoplada del descuento del saldo.
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccount);
    batch.update(accountRef, buildBalanceUpdate({
      account,
      currency: data.currency,
      amountDelta: -totalCost,
      referenceCurrency,
    }));

    // 6. Commit de la transacción
    await batch.commit();

    // 7. Crear currentPrices si es un ticker nuevo
    await ensureCurrentPriceExists(data.name, data.assetType);

    // 8. Invalidar caches
    await invalidatePerformanceCache(auth.uid);
    invalidateDistributionCache(auth.uid);

    // 9. LATE-REG-002: Detectar transacción retroactiva y marcar stale si aplica
    checkAndMarkStaleIfRetroactive(auth.uid, acquisitionDateWithTime, {
      reason: 'retroactive_transaction',
      transactionType: 'buy',
      portfolioAccount: data.portfolioAccount,
    });

    console.log(`[assetHandlers][createAsset] Éxito - assetId: ${assetRef.id}`);

    return {
      success: true,
      assetId: assetRef.id,
      transactionId: transactionRef.id,
      // HU 2.3: el cliente ya no manda la tasa, así que se le devuelve la que
      // se aplicó y de dónde salió.
      acquisitionRate: acquisitionRate !== null ? cleanDecimal(acquisitionRate) : null,
      acquisitionRateSource,
      acquisitionCost,
      referenceCurrency,
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
    // FIX-DECIMAL-001: Aplicar tolerancia para evitar falsos positivos por punto flotante
    if (valueDifference > 0) {
      const currentBalance = account.balances?.[currency] || 0;
      const roundedBalance = Math.round(currentBalance * 100) / 100;
      const roundedDifference = Math.round(valueDifference * 100) / 100;
      const EPSILON = 0.01;
      
      if (roundedBalance + EPSILON < roundedDifference) {
        throw new HttpsError(
          'failed-precondition',
          `Saldo insuficiente. Disponible: ${roundedBalance.toFixed(2)} ${currency}, Requerido adicional: ${roundedDifference.toFixed(2)} ${currency}`
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
    // FIX-TIMESTAMP-001: Combinar fecha con hora actual si se actualiza acquisitionDate
    if (updateData.acquisitionDate !== undefined) {
      updateData.acquisitionDate = combineDateWithCurrentTime(updateData.acquisitionDate);
    }

    // 8. Ejecutar transacción atómica
    const batch = db.batch();

    batch.update(assetRef, updateData);

    // 8.0. Ajuste de balances
    // HU 2.1: todo movimiento de saldo pasa por buildBalanceUpdate para que la
    // base de costo no quede atada a un saldo que ya cambió.
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);

    if (isChangingAccount) {
      // Si cambia de cuenta: devolver valor a cuenta original, cobrar de cuenta nueva
      const oldAccountRef = db.collection('portfolioAccounts').doc(oldAsset.portfolioAccount);
      const newAccountRef = db.collection('portfolioAccounts').doc(data.updates.portfolioAccount);

      // Devolver el valor total a la cuenta original. El costo de ese efectivo
      // no se conoce (venía de un activo, no de un ingreso), así que el saldo
      // pasa a indeterminado en lugar de heredar un costo inventado.
      batch.update(oldAccountRef, buildBalanceUpdate({
        account,
        currency,
        amountDelta: oldTotalValue,
        referenceCurrency,
      }));

      // Validar saldo suficiente en la nueva cuenta
      // FIX-DECIMAL-001: Aplicar tolerancia para evitar falsos positivos por punto flotante
      const newAccountBalance = newAccount.balances?.[currency] || 0;
      const roundedNewBalance = Math.round(newAccountBalance * 100) / 100;
      const roundedNewTotal = Math.round(newTotalValue * 100) / 100;
      const EPSILON = 0.01;
      
      if (roundedNewBalance + EPSILON < roundedNewTotal) {
        throw new HttpsError(
          'failed-precondition',
          `Saldo insuficiente en la nueva cuenta. Disponible: ${roundedNewBalance.toFixed(2)} ${currency}, Requerido: ${roundedNewTotal.toFixed(2)} ${currency}`
        );
      }
      
      // Cobrar el valor total de la nueva cuenta
      batch.update(newAccountRef, buildBalanceUpdate({
        account: newAccount,
        currency,
        amountDelta: -newTotalValue,
        referenceCurrency,
      }));

      console.log(`[assetHandlers][updateAsset] Balance ajustado: cuenta original +${oldTotalValue}, cuenta nueva -${newTotalValue}`);
    } else if (valueDifference !== 0) {
      // Si no cambia de cuenta, solo ajustar la diferencia
      const accountRef = db.collection('portfolioAccounts').doc(oldAsset.portfolioAccount);
      batch.update(accountRef, buildBalanceUpdate({
        account,
        currency,
        amountDelta: -valueDifference,
        referenceCurrency,
      }));
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
    // FIX-ROUNDING-001: Number.EPSILON (~2.2e-16) es demasiado estricto para redondeo financiero.
    // cleanDecimal redondea a 8 decimales, así que residuos de ~1e-8 son posibles.
    // Usar 0.0001 como threshold práctico: cualquier residuo menor a 0.01% de una unidad
    // se considera venta total, evitando assets fantasma con units ~0.
    const isFullSale = Math.abs(remainingUnits) < 0.0001 || remainingUnits <= 0;

    // 7. Ejecutar transacción atómica
    const batch = db.batch();

    if (isFullSale) {
      batch.update(assetRef, { units: 0, isActive: false });
    } else {
      batch.update(assetRef, { units: remainingUnits });
    }

    // FIX-TIMESTAMP-001: Usar fecha proporcionada o generar timestamp con hora actual
    const sellDate = data.sellDate
      ? combineDateWithCurrentTime(data.sellDate)
      : new Date().toISOString();

    // HU 2.4: la venta se valora con el tipo de cambio de SU PROPIO DÍA, no con
    // el de la compra. La fecha se toma de `data.sellDate` —el día que eligió el
    // usuario— y no del ISO ya compuesto: `combineDateWithCurrentTime` compone en
    // hora local y devuelve UTC, así que de tarde en América el instante cae en el
    // día siguiente (bug 1 del dev-record de 2.1).
    const rateDate = data.sellDate || sellDate;
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);

    const { acquisitionRate, acquisitionRateSource } =
      await resolveLotAcquisitionRate(asset, referenceCurrency);
    const { realizationRate, realizationRateSource } =
      await resolveRealizationRate(asset.currency, referenceCurrency, rateDate);

    const decomposition = decomposeRealizedResult({
      grossProceeds: sellValue,
      invested: cleanDecimal(buyPrice * sellAmount),
      acquisitionRate,
      realizationRate,
    });

    // El producto NETO es el que entra al saldo, y entra con su propio costo
    // (RN-03): los dólares que produjo la ganancia nunca costaron pesos a la tasa
    // antigua. Sin tasa del día no se inventa un costo — el saldo queda
    // indeterminado, que es exactamente lo que pasaba antes de esta historia (D5).
    const revenueCost = realizationRate === null
      ? null
      : cleanDecimal(totalRevenue * realizationRate, 2);

    const transactionRef = db.collection('transactions').doc();
    const transactionData = {
      assetId: data.assetId,
      assetName: asset.name,
      type: 'sell',
      // TXG-001.1: Identificador de operación + índice de lote. Aquí la venta es de un
      // único lote, así que la operación se identifica con el id del propio documento.
      operationId: transactionRef.id,
      lotIndex: 0,
      amount: sellAmount,
      price: sellPrice,
      currency: asset.currency,
      date: sellDate,
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
      // HU 2.4: trazabilidad cambiaria de la operación. Campos aditivos: ningún
      // lector actual de la transacción cambia, y `dollarPriceToDate` conserva su
      // semántica intacta para los seis calculadores que la leen (D3).
      ...buildRealizedFxFields({
        referenceCurrency,
        acquisitionRate,
        acquisitionRateSource,
        realizationRate,
        realizationRateSource,
        decomposition,
        acquisitionCost: revenueCost,
      }),
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

    // HU 2.4: el producto de la venta entra al saldo con el tipo de cambio del
    // día de la venta. Antes entraba sin costo (`unknown`), lo que dejaba el
    // saldo indeterminado en cuanto el usuario cerraba una posición.
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, buildBalanceUpdate({
      account,
      currency: asset.currency,
      amountDelta: totalRevenue,
      costDelta: revenueCost,
      referenceCurrency,
    }));

    await batch.commit();

    // 8. Invalidar caches
    await invalidatePerformanceCache(auth.uid);
    invalidateDistributionCache(auth.uid);

    // 9. LATE-REG-002: Detectar transacción retroactiva y marcar stale si aplica
    checkAndMarkStaleIfRetroactive(auth.uid, sellDate, {
      reason: 'retroactive_transaction',
      transactionType: 'sell',
      portfolioAccount: data.portfolioAccountId,
    });

    console.log(`[assetHandlers][sellAsset] Éxito - transactionId: ${transactionRef.id}`);

    return {
      success: true,
      transactionId: transactionRef.id,
      realizedPnL: realizedPnL,
      isFullSale: isFullSale,
      // HU 2.4: la descomposición viaja en la respuesta para que la interfaz
      // pueda mostrarla sin releer la transacción recién escrita.
      referenceCurrency,
      realizationRate,
      acquisitionRate,
      assetMeritAmount: decomposition.assetMeritAmount,
      realizedFxAmount: decomposition.realizedFxAmount,
      realizedTotalAmount: decomposition.realizedTotalAmount,
      realizedFxAvailability: decomposition.availability,
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
    // FIX-TIMESTAMP-001: Usar fecha proporcionada o generar timestamp con hora actual
    const sellDate = data.sellDate
      ? combineDateWithCurrentTime(data.sellDate)
      : new Date().toISOString();
    const currency = assetsList[0]?.currency || 'USD';
    // TXG-001.1: Identificador único de la operación de venta, generado UNA sola vez
    // antes del bucle FIFO. Los N documentos de lote lo comparten, lo que permite al
    // visor consolidarlos en una sola fila sin heurísticas de inferencia.
    // Autoid de Firestore: único sin round-trip ni dependencias nuevas.
    const operationId = db.collection('transactions').doc().id;
    // TXG-001.1: Posición del lote en el consumo FIFO (0-based). El orden FIFO no es
    // recuperable desde el cliente (todos los lotes comparten `date` y `createdAt`, y
    // los doc-id son aleatorios), así que se persiste explícitamente.
    let lotIndex = 0;

    // HU 2.4: la tasa del día de la venta es UNA para toda la operación —los N
    // lotes se venden el mismo día al mismo precio—, así que se resuelve una sola
    // vez fuera del bucle. La de compra, en cambio, es de cada lote (RN-2.4-A).
    const rateDate = data.sellDate || sellDate;
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);
    const { realizationRate, realizationRateSource } =
      await resolveRealizationRate(currency, referenceCurrency, rateDate);

    /** Suma de los méritos de cada lote — la única cifra de mérito de la operación */
    let totalAssetMerit = 0;
    /** Suma de los efectos divisa de cada lote */
    let totalRealizedFx = 0;
    /** `true` en cuanto un solo lote no pueda descomponerse: la operación entera
     *  se declara no disponible antes que presentar una suma incompleta (RN-13) */
    let anyLotUnavailable = false;

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
      // FIX-ROUNDING-001: Usar threshold práctico en vez de Number.EPSILON
      const isFullSale = Math.abs(remainingUnits) < 0.0001;

      if (isFullSale) {
        batch.update(assetRef, { units: 0, isActive: false });
      } else {
        batch.update(assetRef, { units: remainingUnits });
      }

      const proportionalCommission = cleanDecimal((totalCommission * unitsToSellFromAsset) / unitsToSell);

      // HU 2.4: cada lote se compró a su propio tipo de cambio, así que aporta su
      // propio mérito y su propio efecto divisa. El usuario ve la suma; al abrir
      // la operación, la contribución de cada uno (AC-6).
      const { acquisitionRate, acquisitionRateSource } =
        await resolveLotAcquisitionRate(asset, referenceCurrency);

      const lotDecomposition = decomposeRealizedResult({
        grossProceeds: sellValueFromAsset,
        invested: cleanDecimal(buyPrice * unitsToSellFromAsset),
        acquisitionRate,
        realizationRate,
      });

      if (lotDecomposition.availability === 'available') {
        totalAssetMerit = cleanDecimal(totalAssetMerit + lotDecomposition.assetMeritAmount, 2);
        totalRealizedFx = cleanDecimal(totalRealizedFx + lotDecomposition.realizedFxAmount, 2);
      } else {
        anyLotUnavailable = true;
      }

      const lotRevenue = cleanDecimal(sellValueFromAsset - proportionalCommission);
      const lotRevenueCost = realizationRate === null
        ? null
        : cleanDecimal(lotRevenue * realizationRate, 2);

      const transactionRef = db.collection('transactions').doc();
      batch.set(transactionRef, {
        assetId: asset.id,
        assetName: asset.name,
        type: 'sell',
        // TXG-001.1: agrupación exacta por construcción en el visor de transacciones
        operationId: operationId,
        lotIndex: lotIndex,
        amount: unitsToSellFromAsset,
        price: pricePerUnit,
        currency: asset.currency,
        date: sellDate,
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
        ...buildRealizedFxFields({
          referenceCurrency,
          acquisitionRate,
          acquisitionRateSource,
          realizationRate,
          realizationRateSource,
          decomposition: lotDecomposition,
          acquisitionCost: lotRevenueCost,
        }),
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
        // HU 2.4: contribución de este lote a la operación (AC-6)
        acquisitionRate,
        assetMeritAmount: lotDecomposition.assetMeritAmount,
        realizedFxAmount: lotDecomposition.realizedFxAmount,
      });

      // TXG-001.1: avanzar la posición FIFO sólo cuando el lote produjo documento
      lotIndex += 1;
    }

    // 6. Actualizar balance de la cuenta
    // HU 2.4: igual que en la venta simple, el producto entra con el tipo de
    // cambio del día de la venta. Un solo fragmento para toda la operación.
    const totalRevenue = cleanDecimal(totalSellValue - totalCommission);
    const totalRevenueCost = realizationRate === null
      ? null
      : cleanDecimal(totalRevenue * realizationRate, 2);
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, buildBalanceUpdate({
      account,
      currency,
      amountDelta: totalRevenue,
      costDelta: totalRevenueCost,
      referenceCurrency,
    }));

    await batch.commit();

    // 7. Invalidar caches
    await invalidatePerformanceCache(auth.uid);
    invalidateDistributionCache(auth.uid);

    // 8. LATE-REG-002: Detectar transacción retroactiva y marcar stale si aplica
    checkAndMarkStaleIfRetroactive(auth.uid, sellDate, {
      reason: 'retroactive_transaction',
      transactionType: 'sell_partial_fifo',
      portfolioAccount: data.portfolioAccountId,
    });

    console.log(`[assetHandlers][sellPartialAssetsFIFO] Éxito - lotes: ${soldAssets.length}`);

    return {
      success: true,
      soldAssets: soldAssets,
      totalPnL: totalPnL,
      totalRevenue: totalRevenue,
      // HU 2.4: una sola cifra de mérito y una de efecto divisa para toda la
      // operación; el desglose por lote va en `soldAssets` (RN-2.4-A).
      referenceCurrency,
      realizationRate,
      assetMeritAmount: anyLotUnavailable ? null : totalAssetMerit,
      realizedFxAmount: anyLotUnavailable ? null : totalRealizedFx,
      realizedTotalAmount: anyLotUnavailable
        ? null
        : cleanDecimal(totalAssetMerit + totalRealizedFx, 2),
      realizedFxAvailability: anyLotUnavailable ? 'unavailable' : 'available',
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
 * HU 2.1: un ingreso en divisa extranjera queda con el tipo de cambio de SU
 * fecha, no el de hoy, y aporta base de costo al saldo (RN-01, RN-02). Sin tasa
 * no se registra el movimiento (RN-05).
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
      // FIX-DECIMAL-001: Aplicar tolerancia para evitar falsos positivos por punto flotante
      const roundedBalance = Math.round(currentBalance * 100) / 100;
      const roundedAmount = Math.round(amount * 100) / 100;
      const EPSILON = 0.01;
      
      if (roundedBalance + EPSILON < roundedAmount) {
        throw new HttpsError(
          'failed-precondition',
          `Saldo insuficiente. Disponible: ${roundedBalance.toFixed(2)} ${data.currency}, Solicitado: ${roundedAmount.toFixed(2)} ${data.currency}`
        );
      }
      newBalance = cleanDecimal(currentBalance - amount);
    }

    // FIX-TIMESTAMP-001: Usar fecha proporcionada o generar timestamp con hora actual
    const transactionDate = data.date
      ? combineDateWithCurrentTime(data.date)
      : new Date().toISOString();

    // 5. HU 2.1: resolver el tipo de cambio de la FECHA del movimiento.
    // Se toma el día que eligió el usuario, no `transactionDate`:
    // `combineDateWithCurrentTime` compone la fecha en hora local y la devuelve en
    // UTC, así que de tarde en América el ISO ya cae en el día siguiente y se
    // pediría la tasa de un día que el usuario nunca escogió.
    const rateDate = data.date
      ? data.date.substring(0, 10)
      : new Date().toLocaleDateString('en-CA');
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);
    const needsExchangeRate = data.currency !== referenceCurrency;

    // La tasa que declara el usuario manda sobre la propuesta: puede haber
    // recibido otra de su banco o su bróker.
    const declaredRate = Number(data.exchangeRate);
    let acquisitionRate = Number.isFinite(declaredRate) && declaredRate > 0 ? declaredRate : null;
    let acquisitionRateSource = acquisitionRate !== null ? 'user' : null;

    if (needsExchangeRate && acquisitionRate === null) {
      const resolved = await historicalRateService.getCrossRate(
        data.currency,
        referenceCurrency,
        rateDate
      );

      // RN-05: sin tipo de cambio no hay movimiento. Antes de esta historia el
      // ingreso se guardaba con `dollarPriceToDate: 1`, que para un ingreso en
      // dólares con referencia en pesos no es una tasa: es un dato inventado.
      if (resolved === null && data.type === 'cash_income') {
        throw new HttpsError(
          'failed-precondition',
          `No se pudo obtener el tipo de cambio de ${data.currency} a ${referenceCurrency} para el ${rateDate}. Indícalo manualmente para registrar el movimiento.`
        );
      }

      if (resolved !== null) {
        acquisitionRate = resolved.rate;
        acquisitionRateSource = resolved.source;
      }
    }

    // `dollarPriceToDate` significa "unidades de la moneda de referencia por 1
    // USD" — es lo que `convertCurrency` asume en el cliente. Hasta esta historia
    // el diálogo enviaba la tasa spot de la divisa depositada, que no es eso.
    let dollarPriceToDate = 1;
    if (referenceCurrency !== 'USD') {
      if (data.currency === 'USD' && acquisitionRate !== null) {
        dollarPriceToDate = acquisitionRate;
      } else {
        const referenceRate = await historicalRateService.getRateForDate(referenceCurrency, rateDate);
        dollarPriceToDate = referenceRate !== null ? referenceRate.rate : 1;
      }
    }

    // 6. HU 2.5: una salida de divisa realiza la diferencia en cambio (RN-07).
    //
    // La tasa a la que sale es la que ya se resolvió arriba —la que declaró el
    // usuario o la de mercado de la fecha—; aquí sólo se le da su nombre propio
    // (`realizationRate`), porque en un egreso `acquisitionRate` no describe una
    // adquisición. `acquisitionRate` y `acquisitionCost` se conservan tal como
    // estaban, para no mover documentos que otros lectores ya interpretan.
    //
    // Sin tasa o sin base de costo, el retiro **se registra igual** y la cifra
    // se declara no disponible con su motivo (D4, RN-13): un retiro es un hecho
    // consumado en el bróker, no una declaración del usuario como sí lo es el
    // ingreso que RN-05 bloquea.
    const isOutflow = data.type === 'cash_expense';

    const outflowRateSource = acquisitionRateSource === 'user'
      ? OUTFLOW_RATE_SOURCES.USER
      : acquisitionRateSource
        ? OUTFLOW_RATE_SOURCES.MARKET_DATE
        : OUTFLOW_RATE_SOURCES.UNAVAILABLE;

    const outflowFx = isOutflow
      ? computeOutflowRealizedFx({
        account,
        currency: data.currency,
        amount,
        outflowRate: acquisitionRate,
        referenceCurrency,
      })
      : null;

    // RN-14: en la propia moneda de referencia esto es `{}` y el documento queda
    // exactamente como el de hoy (AC-4).
    const outflowFxFields = outflowFx
      ? buildOutflowFxFields({
        outcome: outflowFx,
        referenceCurrency,
        outflowRate: acquisitionRate,
        outflowRateSource,
      })
      : {};

    // 7. Ejecutar transacción atómica
    const batch = db.batch();

    const transactionRef = db.collection('transactions').doc();
    const transactionData = {
      assetName: `${data.type === 'cash_income' ? 'Ingreso' : 'Egreso'} de ${data.currency}`,
      type: data.type,
      amount: amount,
      price: 1,
      currency: data.currency,
      date: transactionDate,
      portfolioAccountId: data.portfolioAccountId,
      commission: 0,
      assetType: 'cash',
      dollarPriceToDate: cleanDecimal(dollarPriceToDate),
      defaultCurrencyForAdquisitionDollar: referenceCurrency,
      // HU 2.1: la tasa tal como la ve el usuario (referencia por unidad de la
      // divisa ingresada) y el costo que aporta al saldo. 2.6 reconstruye el
      // libro mayor desde aquí.
      acquisitionRate: acquisitionRate !== null ? cleanDecimal(acquisitionRate) : null,
      acquisitionRateSource,
      acquisitionCost: acquisitionRate !== null ? cleanDecimal(amount * acquisitionRate) : null,
      referenceCurrency,
      // HU 2.5: la diferencia en cambio que esta salida realiza. Vacío en un
      // ingreso y en cualquier movimiento sin exposición cambiaria (D11).
      ...outflowFxFields,
      description: data.description || '',
      userId: auth.uid,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    batch.set(transactionRef, transactionData);

    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    const amountDelta = data.type === 'cash_income' ? amount : -amount;
    const costDelta = (data.type === 'cash_income' && acquisitionRate !== null)
      ? amount * acquisitionRate
      : null;

    batch.update(accountRef, buildBalanceUpdate({
      account,
      currency: data.currency,
      amountDelta,
      costDelta,
      referenceCurrency,
    }));

    await batch.commit();

    // 8. LATE-REG-002: Detectar transacción retroactiva y marcar stale si aplica
    checkAndMarkStaleIfRetroactive(auth.uid, transactionDate, {
      reason: 'retroactive_transaction',
      transactionType: data.type, // 'cash_income' o 'cash_expense'
      portfolioAccount: data.portfolioAccountId,
    });

    console.log(`[assetHandlers][addCashTransaction] Éxito - transactionId: ${transactionRef.id}`);

    return {
      success: true,
      transactionId: transactionRef.id,
      newBalance: newBalance,
      exchangeRate: acquisitionRate,
      referenceCurrency,
      // HU 2.5: lo que el retiro acaba de realizar, para que el cliente confirme
      // con la cifra del servidor la que anticipó en el diálogo.
      realizedFxAmount: outflowFx ? outflowFx.realizedFxAmount : null,
      realizedFxAvailability: outflowFx ? outflowFx.availability : null,
      realizedFxUnavailableReason: outflowFx ? outflowFx.unavailableReason : null,
      releasedCost: outflowFx ? outflowFx.releasedCost : null,
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
 * HU 2.2 — Convierte una divisa por otra dentro de la MISMA cuenta.
 *
 * Cambiar pesos por dólares en el bróker es la operación más común del usuario
 * objetivo, y hasta esta historia el producto no podía representarla: había que
 * fingirla con un egreso y un ingreso sueltos, y la tasa que los vincula se
 * perdía. Aquí es una sola operación (RN-2.2-A).
 *
 * **Atomicidad sin transacción distribuida**: los dos saldos son campos del
 * mismo documento `portfolioAccounts/{id}`, así que los dos fragmentos de
 * `buildBalanceUpdate` —salida del origen, entrada del destino— se fusionan en
 * un único `update` dentro del mismo batch que escribe la transacción. O se
 * aplican los dos efectos o ninguno.
 *
 * **Costo del destino** (RN-01): es el valor de lo que se entregó, no el precio
 * de mercado de lo que se recibió. Así, quien cambia 4.000.000 COP por 1.000 USD
 * teniendo el peso como referencia obtiene dólares que costaron exactamente
 * 4.000.000 COP, y no una cifra derivada de la tasa de mercado de ese día.
 *
 * **Diferencia en cambio** (RN-07): se calcula y se persiste, no se reporta.
 * Quien la lee y la explica es 2.5.
 *
 * @param {Object} context - Contexto de ejecución (auth)
 * @param {Object} payload - `{ portfolioAccountId, fromCurrency, toCurrency, amount, conversionRate, date?, description? }`
 * @returns {Promise<Object>} Ambos saldos nuevos, el costo del destino y la diferencia en cambio
 * @see platform-docs/stories/2.2-conversion-divisa-en-cuenta/refinamiento.md (D1–D5)
 */
async function convertAccountCurrency(context, payload) {
  const { auth } = context;
  const data = payload || {};

  console.log(`[assetHandlers][convertAccountCurrency] userId: ${auth.uid}, ${data?.fromCurrency} -> ${data?.toCurrency}`);

  try {
    // 1. Validar datos requeridos
    if (!data.portfolioAccountId || !data.fromCurrency || !data.toCurrency || !data.amount) {
      throw new HttpsError(
        'invalid-argument',
        'portfolioAccountId, fromCurrency, toCurrency y amount son requeridos'
      );
    }

    if (data.fromCurrency === data.toCurrency) {
      throw new HttpsError(
        'invalid-argument',
        'La divisa de origen y la de destino deben ser distintas'
      );
    }

    const amount = cleanDecimal(Number(data.amount));
    if (!Number.isFinite(amount) || amount <= 0) {
      throw new HttpsError('invalid-argument', 'El monto a convertir debe ser mayor a 0');
    }

    // RN-05: sin tipo de cambio no hay conversión. No se asume ninguno: sin la
    // tasa, tanto el monto que entra como el costo del destino serían inventados.
    const conversionRate = Number(data.conversionRate);
    if (!Number.isFinite(conversionRate) || conversionRate <= 0) {
      throw new HttpsError(
        'failed-precondition',
        'Se requiere un tipo de cambio mayor que cero para registrar la conversión'
      );
    }

    // 2. Validar ownership de la cuenta.
    // RN-15: la operación vive dentro de UNA cuenta. El contrato no admite
    // cuenta de destino, así que la transferencia entre cuentas no se abre aquí.
    const account = await validateAccountOwnership(data.portfolioAccountId, auth.uid);

    // 3. Validar saldo disponible en el origen (escenario 2).
    // Misma tolerancia que `cash_expense` (FIX-DECIMAL-001).
    const currentFromBalance = account.balances?.[data.fromCurrency] || 0;
    const roundedBalance = Math.round(currentFromBalance * 100) / 100;
    const roundedAmount = Math.round(amount * 100) / 100;
    const EPSILON = 0.01;

    if (roundedBalance + EPSILON < roundedAmount) {
      throw new HttpsError(
        'failed-precondition',
        `Saldo insuficiente. Disponible: ${roundedBalance.toFixed(2)} ${data.fromCurrency}, Solicitado: ${roundedAmount.toFixed(2)} ${data.fromCurrency}`
      );
    }

    const toAmount = cleanDecimal(amount * conversionRate);

    // FIX-TIMESTAMP-001: fecha elegida + hora del servidor, para ordenar el día.
    const transactionDate = data.date
      ? combineDateWithCurrentTime(data.date)
      : new Date().toISOString();

    // La fecha de la tasa sale del día que eligió el usuario, no del ISO
    // resultante: `combineDateWithCurrentTime` devuelve UTC y de tarde en
    // América ya cae en el día siguiente (bug detectado en 2.1).
    const rateDate = data.date
      ? data.date.substring(0, 10)
      : new Date().toLocaleDateString('en-CA');

    const referenceCurrency = await getUserReferenceCurrency(auth.uid);

    // 4. Valorar en moneda de referencia lo que sale, para saber con qué costo
    // entra lo que llega. Se toma el camino más autoritativo disponible:
    //   - el origen ES la referencia    -> 1, sin consultar nada
    //   - el destino ES la referencia   -> la tasa que declaró el usuario, que
    //     manda sobre la de mercado (RN-2.2-B)
    //   - ambas son divisas extranjeras -> tasa de mercado de la fecha
    let referenceRate = null;
    let referenceRateSource = null;

    if (data.fromCurrency === referenceCurrency) {
      referenceRate = 1;
      referenceRateSource = 'identity';
    } else if (data.toCurrency === referenceCurrency) {
      referenceRate = conversionRate;
      referenceRateSource = 'user';
    } else {
      const resolved = await historicalRateService.getCrossRate(
        data.fromCurrency,
        referenceCurrency,
        rateDate
      );
      if (resolved) {
        referenceRate = resolved.rate;
        referenceRateSource = resolved.source;
      }
    }

    // 5. Costo que libera el origen y costo con el que entra el destino.
    //
    // `buildBalanceUpdate` es quien retira del origen su costo a la tasa
    // promedio (RN-08: la tasa del remanente no cambia). El mismo cálculo se
    // replica aquí SOLO para poder informar la diferencia en cambio; la
    // escritura del saldo sigue siendo suya y de nadie más.
    //
    // HU 2.5: la fórmula vive ahora en `computeOutflowRealizedFx`, el mismo sitio
    // que usa el retiro. Una salida es una salida, venga del diálogo que venga (D2).
    let releasedCost = null;
    let realizedFxAmount = null;
    let realizedFxAvailability = null;
    let realizedFxUnavailableReason = null;

    if (data.fromCurrency === referenceCurrency) {
      // Lo que sale es la propia moneda de referencia: costó exactamente lo que
      // vale y no realiza nada. Es un cero medido, no una ausencia de dato.
      releasedCost = amount;
      realizedFxAmount = 0;
      realizedFxAvailability = OUTFLOW_FX_AVAILABILITY.AVAILABLE;
    } else {
      const outflowFx = computeOutflowRealizedFx({
        account,
        currency: data.fromCurrency,
        amount,
        outflowRate: referenceRate,
        referenceCurrency,
      });
      releasedCost = outflowFx.releasedCost;
      realizedFxAmount = outflowFx.realizedFxAmount;
      realizedFxAvailability = outflowFx.availability;
      realizedFxUnavailableReason = outflowFx.unavailableReason;
    }

    // RN-13: si no se puede valorar lo que sale, el destino entra con costo
    // desconocido en lugar de con un costo inventado. El movimiento sí se
    // registra: el usuario hizo la conversión y el saldo debe reflejarla.
    const destinationCost = referenceRate !== null ? amount * referenceRate : null;

    // `dollarPriceToDate` significa "unidades de la moneda de referencia por 1
    // USD", que es lo que `convertCurrency` asume en el cliente. Misma
    // resolución que en `addCashTransaction`.
    let dollarPriceToDate = 1;
    if (referenceCurrency !== 'USD') {
      if (data.fromCurrency === 'USD' && referenceRate !== null) {
        dollarPriceToDate = referenceRate;
      } else {
        const referenceUsdRate = await historicalRateService.getRateForDate(referenceCurrency, rateDate);
        dollarPriceToDate = referenceUsdRate?.rate ?? 1;
      }
    }

    // 6. Escritura atómica: un documento y un solo update de la cuenta
    const batch = db.batch();

    const transactionRef = db.collection('transactions').doc();
    const transactionData = {
      assetName: `Conversión de ${data.fromCurrency} a ${data.toCurrency}`,
      type: 'cash_conversion',
      // El lado que sale usa los campos de siempre, para que todo lector actual
      // de transacciones de efectivo siga entendiendo el documento.
      amount: amount,
      price: 1,
      currency: data.fromCurrency,
      // El lado que entra vive en campos propios: un solo documento describe la
      // operación completa y se ve desde los dos saldos (RN-2.2-A).
      toCurrency: data.toCurrency,
      toAmount: toAmount,
      conversionRate: cleanDecimal(conversionRate),
      date: transactionDate,
      portfolioAccountId: data.portfolioAccountId,
      commission: 0,
      assetType: 'cash',
      dollarPriceToDate: cleanDecimal(dollarPriceToDate),
      defaultCurrencyForAdquisitionDollar: referenceCurrency,
      // Trazabilidad del costo en la convención de 2.1: unidades de la moneda de
      // referencia por 1 unidad de la divisa que se mueve.
      acquisitionRate: referenceRate !== null ? cleanDecimal(referenceRate) : null,
      acquisitionRateSource: referenceRateSource,
      acquisitionCost: destinationCost !== null ? cleanDecimal(destinationCost, 2) : null,
      releasedCost: releasedCost !== null ? cleanDecimal(releasedCost, 2) : null,
      // 2.5 lee esto; 2.2 lo producía sin mostrarlo (RN-07). La disponibilidad
      // es lo que permite a la línea agregada distinguir "no aplica" de "no se
      // pudo calcular", en vez de leer un `null` mudo (RN-13).
      realizedFxAmount,
      realizedFxCurrency: referenceCurrency,
      realizedFxAvailability,
      realizedFxUnavailableReason,
      referenceCurrency,
      description: data.description || '',
      userId: auth.uid,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    batch.set(transactionRef, transactionData);

    // Los dos fragmentos tocan rutas de campo disjuntas del MISMO documento, así
    // que se fusionan en un único update. Ahí está la atomicidad de RN-2.2-A.
    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);

    const outflowUpdate = buildBalanceUpdate({
      account,
      currency: data.fromCurrency,
      amountDelta: -amount,
      referenceCurrency,
    });

    const inflowUpdate = buildBalanceUpdate({
      account,
      currency: data.toCurrency,
      amountDelta: toAmount,
      costDelta: destinationCost,
      referenceCurrency,
    });

    batch.update(accountRef, { ...outflowUpdate, ...inflowUpdate });

    await batch.commit();

    // LATE-REG-002: una conversión con fecha pasada invalida el histórico igual
    // que cualquier otro movimiento retroactivo.
    checkAndMarkStaleIfRetroactive(auth.uid, transactionDate, {
      reason: 'retroactive_transaction',
      transactionType: 'cash_conversion',
      portfolioAccount: data.portfolioAccountId,
    });

    console.log(`[assetHandlers][convertAccountCurrency] Éxito - transactionId: ${transactionRef.id}`);

    return {
      success: true,
      transactionId: transactionRef.id,
      fromCurrency: data.fromCurrency,
      toCurrency: data.toCurrency,
      amount,
      toAmount,
      conversionRate: cleanDecimal(conversionRate),
      newFromBalance: outflowUpdate[`balances.${data.fromCurrency}`],
      newToBalance: inflowUpdate[`balances.${data.toCurrency}`],
      destinationCost: destinationCost !== null ? cleanDecimal(destinationCost, 2) : null,
      realizedFxAmount,
      realizedFxAvailability,
      realizedFxUnavailableReason,
      referenceCurrency,
    };

  } catch (error) {
    console.error(`[assetHandlers][convertAccountCurrency] Error - userId: ${auth.uid}`, error);

    if (error instanceof HttpsError) {
      throw error;
    }

    throw new HttpsError('internal', `Error al registrar la conversión de divisa: ${error.message}`);
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
    const account = await validateAccountOwnership(assetData.portfolioAccount, auth.uid);

    // 4. Buscar y eliminar transacciones asociadas
    const transactionsQuery = db.collection('transactions')
      .where('assetId', '==', data.assetId);
    
    const transactionsSnapshot = await transactionsQuery.get();

    const batch = db.batch();
    let deletedTransactionsCount = 0;

    const deletedTransactions = [];

    transactionsSnapshot.forEach(txDoc => {
      deletedTransactions.push({ id: txDoc.id, ...txDoc.data() });
      batch.delete(txDoc.ref);
      deletedTransactionsCount++;
    });

    // 5. Eliminar el asset
    batch.delete(assetRef);

    // 6. HU 2.6 (D12): el efectivo que consumieron esas transacciones vuelve al
    // saldo. Sin esto el saldo se queda sin la compra y sin el dinero, y deja de
    // cuadrar con su historial para siempre (AC-2).
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);
    const reversal = buildCashReversalUpdate({
      account,
      transactions: deletedTransactions,
      referenceCurrency,
    });

    if (Object.keys(reversal).length > 0) {
      batch.update(db.collection('portfolioAccounts').doc(assetData.portfolioAccount), {
        ...reversal,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    }

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
 * HU 2.6 (D12) - Devuelve al saldo el efectivo de las transacciones que se van.
 *
 * Borrar un activo se llevaba por delante su compra —un asiento de salida de
 * caja— sin devolver el dinero. El saldo se quedaba sin la compra y sin el
 * efectivo, y a partir de ahi no volvia a cuadrar con su historial nunca mas.
 * Deshacer un registro tiene que deshacer tambien su efecto.
 *
 * El costo que la compra retiro vuelve tal cual (`releasedCost`), asi que la
 * base de costo del saldo queda como estaba antes de comprar en lugar de pasar
 * a indeterminada.
 *
 * @param {Object} params
 * @param {Object} params.account - Documento actual de la cuenta
 * @param {Array<Object>} params.transactions - Transacciones que se van a borrar
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @returns {Object} Fragmento de update, vacio si ninguna movia caja
 */
function buildCashReversalUpdate({ account, transactions, referenceCurrency }) {
  const state = {
    balances: { ...(account?.balances || {}) },
    balanceCostBasis: { ...(account?.balanceCostBasis || {}) },
  };
  const update = {};

  for (const transaction of transactions) {
    const currency = transaction.currency;
    if (!currency) continue;

    const impact = resolveCashImpact(transaction, currency);
    if (!impact || Math.abs(impact.amount) < 1e-8) continue;

    // Al reves: lo que salio vuelve y lo que entro se va.
    const reversedDelta = -impact.amount;
    const releasedCost = Number(transaction.releasedCost);
    const costDelta = reversedDelta > 0 && Number.isFinite(releasedCost) ? releasedCost : null;

    const fragment = buildBalanceUpdate({
      account: state,
      currency,
      amountDelta: reversedDelta,
      costDelta,
      referenceCurrency,
    });

    Object.assign(update, fragment);

    state.balances[currency] = fragment[`balances.${currency}`];
    if (fragment[`balanceCostBasis.${currency}`] !== undefined) {
      state.balanceCostBasis[currency] = fragment[`balanceCostBasis.${currency}`];
    }
  }

  return update;
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
    const account = await validateAccountOwnership(data.accountId, auth.uid);

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

    const deletedTransactions = [];

    for (const assetDoc of assetsSnapshot.docs) {
      batch.delete(assetDoc.ref);
      deletedCount++;

      const transactionsQuery = db.collection('transactions')
        .where('assetId', '==', assetDoc.id);

      const transactionsSnapshot = await transactionsQuery.get();
      transactionsSnapshot.forEach(txDoc => {
        deletedTransactions.push({ id: txDoc.id, ...txDoc.data() });
        batch.delete(txDoc.ref);
      });
    }

    // HU 2.6 (D12): el efectivo de las transacciones borradas vuelve al saldo,
    // igual que en el borrado de un activo suelto (AC-2).
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);
    const reversal = buildCashReversalUpdate({
      account,
      transactions: deletedTransactions,
      referenceCurrency,
    });

    if (Object.keys(reversal).length > 0) {
      batch.update(db.collection('portfolioAccounts').doc(data.accountId), {
        ...reversal,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
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

/**
 * HU 2.4 — Recalcula el histórico de posiciones cerradas del usuario.
 *
 * Las ventas anteriores a esta historia no incluyen el efecto divisa en su
 * resultado. Esta acción las corrige y deja constancia en `userData`, que es lo
 * que permite mostrar **un aviso único**: cambiar cifras financieras que el
 * usuario ya vio sin explicárselo se percibe como un fallo, no como una
 * corrección (RN-2.4-B).
 *
 * Es reentrante: si quedó trabajo pendiente devuelve `hasMore` y el cliente
 * vuelve a llamar. El estado se marca `done` sólo cuando no queda nada.
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Sin campos obligatorios
 * @returns {Promise<{success: boolean, updatedCount: number, unavailableCount: number,
 *   hasMore: boolean, status: string}>}
 */
async function backfillRealizedFxDecomposition(context, payload) {
  const { auth } = context;

  console.log(`[assetHandlers][backfillRealizedFxDecomposition] userId: ${auth.uid}`);

  try {
    const userRef = db.collection('userData').doc(auth.uid);
    const userDoc = await userRef.get();
    const previous = userDoc.data()?.realizedFxBackfill || {};

    const result = await backfillRealizedFxForUser(auth.uid, payload || {});

    // Los conteos se acumulan entre pasadas: el aviso habla de todo lo corregido,
    // no de lo que cupo en la última invocación.
    const updatedCount = (Number(previous.updatedCount) || 0) + result.updatedCount;
    const unavailableCount = (Number(previous.unavailableCount) || 0) + result.unavailableCount;
    const status = result.hasMore ? 'in-progress' : 'done';

    await userRef.set({
      realizedFxBackfill: {
        status,
        updatedCount,
        unavailableCount,
        referenceCurrency: result.referenceCurrency,
        completedAt: result.hasMore
          ? null
          : admin.firestore.FieldValue.serverTimestamp(),
        // El descarte del aviso lo escribe el cliente; aquí se preserva para que
        // una segunda pasada no lo resucite.
        acknowledgedAt: previous.acknowledgedAt || null,
      },
    }, { merge: true });

    console.log(`[assetHandlers][backfillRealizedFxDecomposition] ${status} - corregidas: ${updatedCount}, no disponibles: ${unavailableCount}`);

    return {
      success: true,
      updatedCount,
      unavailableCount,
      hasMore: result.hasMore,
      status,
    };
  } catch (error) {
    console.error(`[assetHandlers][backfillRealizedFxDecomposition] Error - userId: ${auth.uid}`, error);

    if (error instanceof HttpsError) {
      throw error;
    }

    throw new HttpsError('internal', `Error al recalcular el histórico: ${error.message}`);
  }
}

/**
 * HU 2.6 — Corregir un saldo a mano deja rastro (AC-4).
 *
 * Quitarle al usuario la capacidad de corregir no es viable: su bróker es la
 * fuente de verdad y a veces no coincide con lo que el producto calculó. La
 * solución no es prohibir la corrección sino registrarla, con su fecha y su
 * motivo, como un movimiento más del historial (RN-06).
 *
 * Un ajuste que **aumenta** un saldo en divisa extranjera necesita tipo de
 * cambio, igual que un ingreso, porque introduce dinero cuyo costo hay que
 * conocer (RN-2.6-B). Uno que lo **disminuye** consume base al promedio vigente
 * y no pregunta nada — y por eso no realiza diferencia en cambio (D7).
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - { portfolioAccountId, currency, newBalance, date, reason, exchangeRate }
 * @returns {Promise<{success: boolean, transactionId: string, previousBalance: number,
 *   newBalance: number, delta: number, exchangeRate: number|null, reconciliation: Object}>}
 */
async function registerBalanceAdjustment(context, payload) {
  const { auth } = context;
  const data = payload || {};

  console.log(`[assetHandlers][registerBalanceAdjustment] userId: ${auth.uid}, account: ${data.portfolioAccountId}, currency: ${data.currency}`);

  try {
    if (!data.portfolioAccountId || !data.currency) {
      throw new HttpsError('invalid-argument', 'portfolioAccountId y currency son requeridos');
    }

    const account = await validateAccountOwnership(data.portfolioAccountId, auth.uid);

    const currentBalance = account.balances?.[data.currency] || 0;

    // Se acepta el saldo correcto (lo que el usuario lee en su bróker) o la
    // diferencia directa. Lo primero es lo que pide el diálogo; lo segundo, lo
    // que resulta cómodo cuando ya se conoce la deriva detectada.
    const targetBalance = Number(data.newBalance);
    const declaredDelta = Number(data.delta);

    let delta;
    if (Number.isFinite(targetBalance)) {
      delta = cleanDecimal(targetBalance - currentBalance);
    } else if (Number.isFinite(declaredDelta)) {
      delta = cleanDecimal(declaredDelta);
    } else {
      throw new HttpsError('invalid-argument', 'Indica el saldo correcto o la diferencia a ajustar');
    }

    if (Math.abs(delta) < MIN_ADJUSTMENT_DELTA) {
      throw new HttpsError('failed-precondition', 'El ajuste no cambia el saldo');
    }

    if (cleanDecimal(currentBalance + delta) < 0) {
      throw new HttpsError('failed-precondition', 'El ajuste dejaría el saldo en negativo');
    }

    const rateDate = data.date
      ? String(data.date).substring(0, 10)
      : new Date().toLocaleDateString('en-CA');
    const transactionDate = data.date
      ? combineDateWithCurrentTime(String(data.date).substring(0, 10))
      : new Date().toISOString();

    const referenceCurrency = await getUserReferenceCurrency(auth.uid);

    const resolved = await resolveAdjustmentRate({
      currency: data.currency,
      referenceCurrency,
      date: rateDate,
      declaredRate: data.exchangeRate,
    });

    // RN-2.6-B: sin tasa no se puede saber cuánto costó el dinero que entra. A
    // diferencia de crear una cuenta (D9), aquí el usuario está delante y puede
    // escribirla, así que se le pide en lugar de registrar una base ausente.
    if (requiresExchangeRate({ delta, currency: data.currency, referenceCurrency })
      && resolved.acquisitionRate === null) {
      throw new HttpsError(
        'failed-precondition',
        `No se pudo obtener el tipo de cambio de ${data.currency} a ${referenceCurrency} para el ${rateDate}. Indícalo manualmente para registrar el ajuste.`
      );
    }

    const { transactionData, balanceUpdate, newBalance } = buildAdjustment({
      account,
      accountId: data.portfolioAccountId,
      userId: auth.uid,
      currency: data.currency,
      delta,
      date: transactionDate,
      referenceCurrency,
      adjustmentReason: ADJUSTMENT_REASONS.MANUAL,
      description: data.reason || '',
      acquisitionRate: resolved.acquisitionRate,
      acquisitionRateSource: resolved.acquisitionRateSource,
      dollarPriceToDate: resolved.dollarPriceToDate,
    });

    const batch = db.batch();

    const transactionRef = db.collection('transactions').doc();
    batch.set(transactionRef, transactionData);

    const accountRef = db.collection('portfolioAccounts').doc(data.portfolioAccountId);
    batch.update(accountRef, {
      ...balanceUpdate,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    await batch.commit();

    // El ajuste existe precisamente para cerrar una deriva: el veredicto se
    // recalcula con el asiento ya escrito, no con el estado anterior (D4).
    const reconciliation = await recheckBalanceReconciliation({
      accountId: data.portfolioAccountId,
      currency: data.currency,
      referenceCurrency,
      balance: newBalance,
    });

    checkAndMarkStaleIfRetroactive(auth.uid, transactionDate, {
      reason: 'retroactive_transaction',
      transactionType: 'cash_adjustment',
      portfolioAccount: data.portfolioAccountId,
    });

    invalidateDistributionCache(auth.uid);

    console.log(`[assetHandlers][registerBalanceAdjustment] Éxito - ${data.currency}: ${currentBalance} -> ${newBalance}`);

    return {
      success: true,
      transactionId: transactionRef.id,
      previousBalance: currentBalance,
      newBalance,
      delta,
      exchangeRate: resolved.acquisitionRate,
      referenceCurrency,
      reconciliation,
    };
  } catch (error) {
    console.error(`[assetHandlers][registerBalanceAdjustment] Error - userId: ${auth.uid}`, error);

    if (error instanceof HttpsError) throw error;

    throw new HttpsError('internal', `Error al registrar el ajuste de saldo: ${error.message}`);
  }
}

/**
 * HU 2.6 — Vuelve a comparar un saldo con su libro mayor y guarda el veredicto.
 *
 * Se llama después de escribir un asiento que pudo cerrar (o abrir) una deriva.
 * Un fallo aquí no puede tumbar la operación que ya se confirmó: el ajuste está
 * escrito y el veredicto se recalculará la próxima vez que alguien despliegue
 * ese historial.
 *
 * @param {Object} params
 * @param {string} params.accountId - Cuenta a revisar
 * @param {string} params.currency - Divisa del saldo
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {number} params.balance - Saldo ya actualizado
 * @returns {Promise<Object|null>} Veredicto, o `null` si no se pudo calcular
 */
async function recheckBalanceReconciliation({ accountId, currency, referenceCurrency, balance }) {
  try {
    const snapshot = await db.collection('transactions')
      .where('portfolioAccountId', '==', accountId)
      .get();

    const projection = projectBalanceLedger({
      transactions: snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() })),
      currency,
      referenceCurrency,
      balance,
    });

    const verdict = {
      ledgerBalance: projection.reconciliation.ledgerBalance,
      difference: projection.reconciliation.difference,
      status: projection.reconciliation.status,
    };

    await db.collection('portfolioAccounts').doc(accountId).update({
      [`balanceReconciliation.${currency}`]: {
        ...verdict,
        checkedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
    });

    return verdict;
  } catch (error) {
    console.warn(`[assetHandlers][recheckBalanceReconciliation] No se pudo actualizar el veredicto de ${currency}:`, error.message);
    return null;
  }
}

/**
 * HU 2.6 — Migra los saldos preexistentes del usuario al libro mayor (AC-6).
 *
 * Reconstruye lo que puede del historial y, sólo donde no puede, estima y deja
 * constancia en `userData` para que la interfaz muestre **un aviso por cuenta**
 * —descartable y no bloqueante— con la tasa estimada (RN-12).
 *
 * Es reentrante: si quedó trabajo pendiente devuelve `hasMore` y el cliente
 * vuelve a llamar. El estado se marca `done` sólo cuando no queda nada, y el
 * descarte que escribió el usuario se preserva para que una segunda pasada no
 * lo resucite (AC-7).
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Sin campos obligatorios
 * @returns {Promise<{success: boolean, migratedCount: number, estimatedCount: number,
 *   unavailableCount: number, driftCount: number, hasMore: boolean,
 *   status: string, notices: Array<Object>}>}
 */
async function migrateBalanceLedger(context, payload) {
  const { auth } = context;

  console.log(`[assetHandlers][migrateBalanceLedger] userId: ${auth.uid}`);

  try {
    const userRef = db.collection('userData').doc(auth.uid);
    const userDoc = await userRef.get();
    const previous = userDoc.data()?.balanceLedgerMigration || {};

    const result = await migrateBalanceLedgerForUser(auth.uid, payload || {});

    // Los conteos y los avisos se acumulan entre pasadas: el aviso habla de todo
    // lo migrado, no de lo que cupo en la última invocación.
    const previousNotices = Array.isArray(previous.notices) ? previous.notices : [];
    const noticeKey = (notice) => `${notice.accountId}|${notice.currency}`;
    const seen = new Set(result.notices.map(noticeKey));
    const notices = [
      ...result.notices,
      ...previousNotices.filter((notice) => !seen.has(noticeKey(notice))),
    ];

    const status = result.hasMore ? 'in-progress' : 'done';

    await userRef.set({
      balanceLedgerMigration: {
        status,
        migratedCount: (Number(previous.migratedCount) || 0) + result.migratedCount,
        estimatedCount: (Number(previous.estimatedCount) || 0) + result.estimatedCount,
        unavailableCount: (Number(previous.unavailableCount) || 0) + result.unavailableCount,
        driftCount: result.driftCount,
        referenceCurrency: result.referenceCurrency,
        notices,
        completedAt: result.hasMore
          ? null
          : admin.firestore.FieldValue.serverTimestamp(),
        // El descarte del aviso lo escribe el cliente por cuenta; aquí sólo se
        // preserva lo que ya hubiera.
        acknowledgedAccounts: previous.acknowledgedAccounts || [],
      },
    }, { merge: true });

    console.log(`[assetHandlers][migrateBalanceLedger] Éxito - migrados: ${result.migratedCount}, avisos: ${notices.length}, status: ${status}`);

    return {
      success: true,
      migratedCount: result.migratedCount,
      estimatedCount: result.estimatedCount,
      unavailableCount: result.unavailableCount,
      driftCount: result.driftCount,
      hasMore: result.hasMore,
      referenceCurrency: result.referenceCurrency,
      status,
      notices,
    };
  } catch (error) {
    console.error(`[assetHandlers][migrateBalanceLedger] Error - userId: ${auth.uid}`, error);

    if (error instanceof HttpsError) throw error;

    throw new HttpsError('internal', `Error migrando los saldos al libro mayor: ${error.message}`);
  }
}

module.exports = {
  createAsset,
  updateAsset,
  sellAsset,
  deleteAsset,
  deleteAssets,
  sellPartialAssetsFIFO,
  addCashTransaction,
  convertAccountCurrency,
  updateStockSector,
  backfillRealizedFxDecomposition,
  // HU 2.6: el ajuste manual del saldo deja su asiento
  registerBalanceAdjustment,
  recheckBalanceReconciliation,
  // HU 2.6: migracion de los saldos preexistentes al libro mayor
  migrateBalanceLedger,
  // Utilidades exportadas para posible reutilización
  cleanDecimal,
  validateAccountOwnership,
  validateSufficientFunds,
  ensureCurrentPriceExists,
};
