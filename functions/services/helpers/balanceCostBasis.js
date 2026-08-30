/**
 * HU 2.1 — Base de costo de los saldos de efectivo.
 *
 * Cada saldo de una divisa dentro de una cuenta conoce cuánto costó, expresado
 * en la moneda de referencia del usuario (RN-01). El costo vive en
 * `portfolioAccounts/{id}.balanceCostBasis.{DIVISA}`, hermano de `balances`, que
 * no cambia de forma ni de lectores (RN-16):
 *
 *   balances:         { USD: 1000 }
 *   balanceCostBasis: { USD: { cost: 4000000, referenceCurrency: 'COP',
 *                              status: 'known', updatedAt: <Timestamp> } }
 *
 * La tasa promedio NO se persiste: se deriva de `cost / balance` (RN-02). Con una
 * sola fuente del dato, saldo y base no pueden divergir.
 *
 * Este módulo es el único punto que construye actualizaciones de `balances.{CUR}`.
 * Todo handler que mueva un saldo pasa por aquí, porque un costo que se queda
 * atado a un saldo que ya cambió es peor que no tener costo: aparenta trazabilidad.
 *
 * @module services/helpers/balanceCostBasis
 * @see platform-docs/stories/2.1-base-costo-saldo-efectivo/refinamiento.md (D1, D2)
 */

const admin = require('../firebaseAdmin');

const db = admin.firestore();

// ============================================================================
// CONSTANTES
// ============================================================================

/** Estado de la base de costo de un saldo */
const COST_BASIS_STATUS = {
  /** Se conoce el costo completo del saldo */
  KNOWN: 'known',
  /** Entró dinero cuyo costo no se pudo determinar — el costo del saldo deja de ser calculable */
  UNKNOWN: 'unknown',
};

/** Por debajo de este saldo se considera que la divisa quedó vacía */
const EMPTY_BALANCE_EPSILON = 0.005;

// ============================================================================
// HELPERS INTERNOS
// ============================================================================

/**
 * Limpia decimales con la MISMA convención que `cleanDecimal` de assetHandlers,
 * para que el saldo que escribe este helper sea idéntico al que escribían los
 * handlers antes de pasar por aquí.
 *
 * @param {number} num - Número a limpiar
 * @param {number} [decimals=8] - Cantidad de decimales
 * @returns {number}
 */
function cleanDecimal(num, decimals = 8) {
  return Number(Math.round(Number(num + 'e' + decimals)) / 10 ** decimals);
}

/**
 * Redondea un costo a 2 decimales, la precisión monetaria del producto.
 *
 * @param {number} value - Valor a redondear
 * @returns {number}
 */
function round2(value) {
  return cleanDecimal(value, 2);
}

/**
 * Obtiene la moneda de referencia del usuario.
 *
 * @param {string} userId - UID del usuario
 * @returns {Promise<string>} Código de moneda; `USD` si no está configurada
 */
async function getUserReferenceCurrency(userId) {
  try {
    const doc = await db.collection('userData').doc(userId).get();
    return doc.data()?.defaultCurrency || 'USD';
  } catch (error) {
    console.warn(`[balanceCostBasis] No se pudo leer la moneda de referencia de ${userId}:`, error.message);
    return 'USD';
  }
}

/**
 * Tasa promedio de adquisición de un saldo: cuántas unidades de la moneda de
 * referencia costó cada unidad de la divisa.
 *
 * @param {Object|undefined} costBasis - Entrada de `balanceCostBasis[divisa]`
 * @param {number} balance - Saldo actual de esa divisa
 * @param {string} referenceCurrency - Moneda de referencia vigente del usuario
 * @returns {number|null} Tasa promedio, o null si no es determinable
 */
function deriveAverageRate(costBasis, balance, referenceCurrency) {
  if (!costBasis || costBasis.status !== COST_BASIS_STATUS.KNOWN) return null;
  if (costBasis.referenceCurrency !== referenceCurrency) return null;
  if (typeof costBasis.cost !== 'number' || !Number.isFinite(costBasis.cost)) return null;
  if (!balance || Math.abs(balance) < EMPTY_BALANCE_EPSILON) return null;

  return costBasis.cost / balance;
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Construye el fragmento de actualización de `portfolioAccounts/{id}` que mueve
 * un saldo y mantiene su base de costo coherente en la misma escritura.
 *
 * Reglas:
 * - **Entrada con costo conocido** (`costDelta` numérico): el costo se suma. La
 *   tasa promedio resultante es el promedio ponderado de las entradas (RN-02),
 *   independiente del orden en que se registren.
 * - **Entrada con costo desconocido** (`costDelta === null`): el saldo pasa a
 *   `status: 'unknown'`. No se inventa un costo ni se deja el anterior, que ya
 *   no describe el saldo (RN-13). Las hijas 2.3 y 2.4 sustituirán este caso
 *   valorando la entrada al cambio de su propio día.
 * - **Salida**: el costo baja en `monto × tasa promedio`. La tasa promedio no
 *   cambia. Aquí NO se calcula diferencia en cambio realizada — eso es 2.5.
 * - **Saldo que llega a cero**: la base de costo se reinicia, para que el próximo
 *   ingreso empiece limpio en lugar de arrastrar residuos de redondeo.
 *
 * @param {Object} params
 * @param {Object} params.account - Documento actual de la cuenta
 * @param {string} params.currency - Divisa del saldo
 * @param {number} params.amountDelta - Variación del saldo (positiva entra, negativa sale)
 * @param {number|null} [params.costDelta] - Costo de la entrada en moneda de referencia.
 *   `null` o ausente en una entrada = costo desconocido. Ignorado en salidas.
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @returns {Object} Fragmento para `update()` / `batch.update()`
 */
function buildBalanceUpdate({ account, currency, amountDelta, costDelta = null, referenceCurrency }) {
  const currentBalance = account?.balances?.[currency] || 0;
  const newBalance = cleanDecimal(currentBalance + amountDelta);

  const update = { [`balances.${currency}`]: newBalance };
  const basisPath = `balanceCostBasis.${currency}`;
  const currentBasis = account?.balanceCostBasis?.[currency];

  // El efectivo en la propia moneda de referencia no tiene exposición cambiaria:
  // no se le lleva base de costo y no se le muestra ninguna métrica (RN-14).
  if (currency === referenceCurrency) {
    return update;
  }

  // Saldo agotado: la base de costo deja de tener sujeto.
  if (Math.abs(newBalance) < EMPTY_BALANCE_EPSILON) {
    update[basisPath] = {
      cost: 0,
      referenceCurrency,
      status: COST_BASIS_STATUS.KNOWN,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    return update;
  }

  // Un costo expresado en una moneda de referencia distinta de la vigente ya no
  // es comparable con el saldo: se descarta en lugar de reinterpretarlo (RN-13).
  const basisIsUsable = currentBasis
    && currentBasis.status === COST_BASIS_STATUS.KNOWN
    && currentBasis.referenceCurrency === referenceCurrency;

  if (amountDelta > 0) {
    if (costDelta === null || costDelta === undefined || !Number.isFinite(costDelta)) {
      update[basisPath] = {
        cost: null,
        referenceCurrency,
        status: COST_BASIS_STATUS.UNKNOWN,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      return update;
    }

    // Una entrada con costo conocido no rescata un saldo ya indeterminado: solo
    // se conoce el costo de lo que entra, no el del saldo que había.
    if (!basisIsUsable && currentBasis && currentBasis.status === COST_BASIS_STATUS.UNKNOWN) {
      update[basisPath] = {
        cost: null,
        referenceCurrency,
        status: COST_BASIS_STATUS.UNKNOWN,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      return update;
    }

    const previousCost = basisIsUsable ? currentBasis.cost : 0;

    update[basisPath] = {
      cost: round2(previousCost + costDelta),
      referenceCurrency,
      status: COST_BASIS_STATUS.KNOWN,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    return update;
  }

  // Salida: se retira costo a la tasa promedio vigente. Si el costo no era
  // determinable, sigue sin serlo.
  const averageRate = deriveAverageRate(currentBasis, currentBalance, referenceCurrency);

  if (averageRate === null) {
    if (currentBasis) {
      update[basisPath] = {
        cost: null,
        referenceCurrency,
        status: COST_BASIS_STATUS.UNKNOWN,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
    }
    return update;
  }

  update[basisPath] = {
    cost: round2(currentBasis.cost + amountDelta * averageRate),
    referenceCurrency,
    status: COST_BASIS_STATUS.KNOWN,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  return update;
}

module.exports = {
  buildBalanceUpdate,
  deriveAverageRate,
  getUserReferenceCurrency,
  COST_BASIS_STATUS,
  EMPTY_BALANCE_EPSILON,
};
