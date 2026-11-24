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

/**
 * Tasa media de la porción **comprada** de un saldo.
 *
 * HU 2.5 / origen del efectivo: sólo la divisa que se adquirió entregando
 * moneda de referencia —una conversión— tiene un precio de compra. La que
 * llegó por un ingreso, una venta en su propia divisa o un dividendo no costó
 * nada en moneda de referencia: su "costo" es una **traducción** al cambio del
 * día, no un desembolso. Distinguirlas es lo que impide que sacar pesos a tu
 * propio banco te fabrique un resultado cambiario realizado.
 *
 * @param {Object|undefined} costBasis - Entrada de `balanceCostBasis[divisa]`
 * @param {string} referenceCurrency - Moneda de referencia vigente
 * @returns {number|null} Tasa media de lo convertido, o `null` si no hay nada convertido
 */
function deriveConvertedAverageRate(costBasis, referenceCurrency) {
  if (!costBasis) return null;
  if (costBasis.referenceCurrency !== referenceCurrency) return null;

  const amount = toFiniteNumber(costBasis.convertedAmount);
  const cost = toFiniteNumber(costBasis.convertedCost);

  if (amount === null || cost === null) return null;
  if (Math.abs(amount) < EMPTY_BALANCE_EPSILON) return null;

  return cost / amount;
}

/**
 * Unidades del saldo que proceden de una conversión real.
 *
 * @param {Object|undefined} costBasis - Entrada de `balanceCostBasis[divisa]`
 * @returns {number} Cero cuando no consta ninguna (lo normal en datos previos)
 */
function convertedAmountOf(costBasis) {
  return toFiniteNumber(costBasis?.convertedAmount) || 0;
}

/** @param {*} value @returns {number|null} */
function toFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

// ============================================================================
// API PÚBLICA
// ============================================================================

/**
 * Cómo se lleva la base de un saldo en un movimiento concreto (HU 2.8).
 *
 * - `none`: no se lleva base. Es el efectivo en la propia moneda de referencia
 *   cuando no hay ninguna base de antes que cuidar (RN-14).
 * - `current`: la base está, o nace, en la moneda de referencia vigente. Es el
 *   comportamiento de siempre (HU 2.1).
 * - `maintain`: hay una base en **otra** moneda —la de referencia que el usuario
 *   tenía antes— y se sigue cuidando en esa moneda aunque hoy no sea la vigente
 *   (RN-2.8-A). Sin esto, cambiar de referencia congelaba la base y al volver
 *   describía un saldo que ya no existía.
 * - `rebase`: entra dinero con un costo declarado en la referencia vigente sobre
 *   un saldo cuya base no está en ella. La base pasa a la referencia vigente y el
 *   dinero que ya estaba se valora a la tasa del día del movimiento (RN-2.8-E):
 *   nunca se toma como gratis.
 */
const BASIS_MODES = {
  NONE: 'none',
  CURRENT: 'current',
  MAINTAIN: 'maintain',
  REBASE: 'rebase',
};

/**
 * Una base sirve para describir un saldo sólo si dice en qué moneda está y esa
 * moneda no es la del propio saldo: "lo que costaron mis dólares, en dólares"
 * no informa nada y sólo puede ser un residuo.
 *
 * @param {Object|undefined} basis - Entrada de `balanceCostBasis[divisa]`
 * @param {string} currency - Divisa del saldo
 * @returns {boolean}
 */
function hasMeaningfulBasis(basis, currency) {
  return !!basis
    && typeof basis.referenceCurrency === 'string'
    && basis.referenceCurrency !== currency;
}

/** @param {*} rate @returns {boolean} */
function isUsableRate(rate) {
  return typeof rate === 'number' && Number.isFinite(rate) && rate > 0;
}

/**
 * Decide en qué moneda se lleva la base de un saldo en este movimiento.
 *
 * Es la regla de la HU 2.8 en un solo sitio: la usan la escritura
 * (`buildBalanceUpdate`), quien resuelve la tasa (`basisValuationNeed`) y, a
 * través de la primera, el replay del libro mayor.
 *
 * @param {Object} params
 * @param {Object} params.account - Documento actual de la cuenta (o estado del replay)
 * @param {string} params.currency - Divisa del saldo
 * @param {number} params.amountDelta - Variación del saldo
 * @param {number|null} [params.costDelta] - Costo declarado de la entrada, en la referencia vigente
 * @param {string} params.referenceCurrency - Moneda de referencia vigente
 * @returns {{mode: string, target: string|null}} Modo y moneda en que queda la base
 */
function resolveBasisTarget({ account, currency, amountDelta, costDelta = null, referenceCurrency }) {
  const currentBasis = account?.balanceCostBasis?.[currency];
  const basis = hasMeaningfulBasis(currentBasis, currency) ? currentBasis : null;

  // RN-2.8-A: con el saldo en la propia moneda de referencia no hay nada que
  // convertir, pero una base de antes sigue siendo cierta y se sigue cuidando.
  if (currency === referenceCurrency) {
    return basis
      ? { mode: BASIS_MODES.MAINTAIN, target: basis.referenceCurrency }
      : { mode: BASIS_MODES.NONE, target: null };
  }

  if (basis && basis.referenceCurrency === referenceCurrency) {
    return { mode: BASIS_MODES.CURRENT, target: referenceCurrency };
  }

  // RN-2.8-E: una entrada con costo declarado en la referencia vigente lleva la
  // base a esa moneda. Es la única forma de que lo declarado quede tal cual.
  const declaredInflow = amountDelta > 0
    && typeof costDelta === 'number'
    && Number.isFinite(costDelta);

  if (declaredInflow) {
    return { mode: BASIS_MODES.REBASE, target: referenceCurrency };
  }

  if (basis) {
    return { mode: BASIS_MODES.MAINTAIN, target: basis.referenceCurrency };
  }

  return { mode: BASIS_MODES.CURRENT, target: referenceCurrency };
}

/**
 * ¿Necesita este movimiento una tasa de mercado para mantener la base?
 *
 * Hace falta en dos casos, y sólo en ellos (RN-2.8-C: lo declarado nunca se
 * revalora):
 * - entra dinero a una base que se lleva en otra moneda (`maintain`): nadie
 *   declaró esa tasa, así que se valora al cierre de su día (RN-2.8-D);
 * - se reubica la base (`rebase`) y en el saldo ya había dinero (RN-2.8-E).
 *
 * @param {Object} params - Los mismos de `resolveBasisTarget`
 * @returns {string|null} Moneda en la que hay que expresar la divisa, o `null`
 */
function basisValuationNeed(params) {
  const { mode, target } = resolveBasisTarget(params);
  const currentBalance = params.account?.balances?.[params.currency] || 0;

  if (mode === BASIS_MODES.MAINTAIN && params.amountDelta > 0) return target;
  if (mode === BASIS_MODES.REBASE && currentBalance >= EMPTY_BALANCE_EPSILON) return target;

  return null;
}

/**
 * Base agotada: el saldo llegó a cero o quedó en negativo y la base deja de tener
 * sujeto. Conserva la moneda en que se llevaba para que una base mantenida siga
 * siéndolo (RN-2.8-A).
 *
 * @param {string} target - Moneda de la base
 * @returns {Object}
 */
function emptyBasis(target) {
  return {
    cost: 0,
    referenceCurrency: target,
    status: COST_BASIS_STATUS.KNOWN,
    // Sin saldo no queda nada comprado que pueda realizar diferencia.
    convertedAmount: 0,
    convertedCost: 0,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

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
 *   no describe el saldo (RN-13).
 * - **Salida**: el costo baja en `monto × tasa promedio`, es decir, en la misma
 *   proporción que el saldo. La tasa promedio no cambia. Aquí NO se calcula
 *   diferencia en cambio realizada — eso es 2.5.
 * - **Saldo que llega a cero o queda en negativo**: la base se reinicia, para que
 *   el próximo ingreso empiece limpio (HU 2.8, RN-2.8-E: un negativo no aporta
 *   costo y nunca se escribe un costo negativo).
 *
 * HU 2.8 — la base sobrevive al cambio de moneda de referencia:
 * - Mientras el usuario usa otra referencia, la base se sigue llevando en la
 *   suya: las salidas la reducen en proporción (RN-2.8-B) y las entradas se
 *   valoran con `basisValuation`, la tasa de mercado de su día (RN-2.8-D).
 * - Lo declarado no se recalcula nunca (RN-2.8-C): `costDelta` entra tal cual.
 * - El dinero que ya estaba sin base en la referencia vigente se valora con
 *   `basisValuation` al entrar un movimiento con costo (RN-2.8-E).
 * - Si la tasa que hacía falta no está, la base se declara desconocida
 *   (RN-2.8-F, RN-13).
 *
 * @param {Object} params
 * @param {Object} params.account - Documento actual de la cuenta
 * @param {string} params.currency - Divisa del saldo
 * @param {number} params.amountDelta - Variación del saldo (positiva entra, negativa sale)
 * @param {number|null} [params.costDelta] - Costo de la entrada en moneda de referencia.
 *   `null` o ausente en una entrada = costo desconocido. Ignorado en salidas.
 * @param {string} params.referenceCurrency - Moneda de referencia del usuario
 * @param {{amount: number, cost: number}|null} [params.convertedDelta] - Porción
 *   comprada de la entrada (HU 2.5)
 * @param {{referenceCurrency: string, rate: number|null}|null} [params.basisValuation] -
 *   Tasa de mercado del día del movimiento, en unidades de `referenceCurrency` por
 *   1 unidad de la divisa (la que pide `basisValuationNeed`)
 * @returns {Object} Fragmento para `update()` / `batch.update()`
 * @see platform-docs/stories/2.8-base-costo-cambio-referencia/refinamiento.md (T1)
 */
function buildBalanceUpdate({
  account,
  currency,
  amountDelta,
  costDelta = null,
  referenceCurrency,
  convertedDelta = null,
  basisValuation = null,
}) {
  const currentBalance = account?.balances?.[currency] || 0;
  const newBalance = cleanDecimal(currentBalance + amountDelta);

  const update = { [`balances.${currency}`]: newBalance };
  const basisPath = `balanceCostBasis.${currency}`;
  const currentBasis = account?.balanceCostBasis?.[currency];

  const { mode, target } = resolveBasisTarget({
    account,
    currency,
    amountDelta,
    costDelta,
    referenceCurrency,
  });

  // RN-14: el efectivo en la propia moneda de referencia, sin base de antes, no
  // tiene nada que contar.
  if (mode === BASIS_MODES.NONE) {
    return update;
  }

  // Saldo agotado: la base de costo deja de tener sujeto. Un saldo negativo
  // tampoco la tiene (RN-2.8-E); si nunca hubo base, no se crea una por ello.
  if (Math.abs(newBalance) < EMPTY_BALANCE_EPSILON
    || (newBalance < 0 && currentBasis)) {
    update[basisPath] = emptyBasis(target);
    return update;
  }

  if (newBalance < 0) {
    return update;
  }

  const valuationRate = basisValuation
    && basisValuation.referenceCurrency === target
    && isUsableRate(basisValuation.rate)
    ? basisValuation.rate
    : null;

  // La base de partida, en la moneda objetivo.
  const basisIsUsable = currentBasis
    && currentBasis.status === COST_BASIS_STATUS.KNOWN
    && currentBasis.referenceCurrency === target
    && typeof currentBasis.cost === 'number'
    && Number.isFinite(currentBasis.cost);

  // Lo comprado sobrevive aunque el costo traducido se vuelva indeterminado:
  // son dos preguntas distintas, y saber que 500 COP se compraron a 4.000 no
  // deja de ser cierto porque otro ingreso llegara sin tasa. Al reubicar la base
  // en otra moneda ya no se puede comparar, y empieza en cero.
  const keepsConverted = mode !== BASIS_MODES.REBASE;
  const previousConvertedAmount = keepsConverted ? convertedAmountOf(currentBasis) : 0;
  const previousConvertedCost = keepsConverted
    ? toFiniteNumber(currentBasis?.convertedCost) || 0
    : 0;

  if (amountDelta > 0) {
    // ¿Cuánto cuesta lo que entra, en la moneda objetivo?
    // - `current` / `rebase`: lo declarado, tal cual (RN-2.8-C).
    // - `maintain`: nadie lo declaró en esa moneda; se valora a su día (RN-2.8-D).
    let inflowCost;
    if (mode === BASIS_MODES.MAINTAIN) {
      inflowCost = valuationRate !== null ? amountDelta * valuationRate : null;
    } else {
      inflowCost = typeof costDelta === 'number' && Number.isFinite(costDelta) ? costDelta : null;
    }

    // Una conversión sólo es compra en la moneda en que se pagó: con una base
    // mantenida en otra, lo que entra se trata como traducido.
    const convertedIn = mode !== BASIS_MODES.MAINTAIN
      && convertedDelta
      && Number.isFinite(convertedDelta.amount)
      && Number.isFinite(convertedDelta.cost)
      ? convertedDelta
      : null;

    // RN-2.8-E: si el saldo estaba en negativo, la parte de la entrada que lo
    // cubre no queda en el saldo. Lo que queda conserva la tasa de la entrada.
    if (currentBalance < 0) {
      const keptShare = newBalance / amountDelta;
      const keptConverted = {
        convertedAmount: convertedIn ? round2(convertedIn.amount * keptShare) : 0,
        convertedCost: convertedIn ? round2(convertedIn.cost * keptShare) : 0,
      };

      update[basisPath] = inflowCost === null
        ? {
          cost: null,
          referenceCurrency: target,
          status: COST_BASIS_STATUS.UNKNOWN,
          ...keptConverted,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        }
        : {
          cost: round2(inflowCost * keptShare),
          referenceCurrency: target,
          status: COST_BASIS_STATUS.KNOWN,
          ...keptConverted,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };
      return update;
    }

    const nextConverted = {
      convertedAmount: round2(previousConvertedAmount + (convertedIn ? convertedIn.amount : 0)),
      convertedCost: round2(previousConvertedCost + (convertedIn ? convertedIn.cost : 0)),
    };

    // ¿Cuánto costaba lo que ya había?
    let previousCost;
    if (mode === BASIS_MODES.REBASE) {
      // RN-2.8-E: el dinero que ya estaba no es gratis.
      if (currentBalance < EMPTY_BALANCE_EPSILON) {
        previousCost = 0;
      } else {
        previousCost = valuationRate !== null ? currentBalance * valuationRate : null;
      }
    } else if (basisIsUsable) {
      previousCost = currentBasis.cost;
    } else if (currentBasis && currentBasis.status === COST_BASIS_STATUS.UNKNOWN) {
      // Una entrada con costo conocido no rescata un saldo ya indeterminado: solo
      // se conoce el costo de lo que entra, no el del saldo que había.
      previousCost = null;
    } else {
      previousCost = 0;
    }

    if (inflowCost === null || previousCost === null) {
      update[basisPath] = {
        cost: null,
        referenceCurrency: target,
        status: COST_BASIS_STATUS.UNKNOWN,
        ...nextConverted,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      return update;
    }

    update[basisPath] = {
      cost: round2(previousCost + inflowCost),
      referenceCurrency: target,
      status: COST_BASIS_STATUS.KNOWN,
      ...nextConverted,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    return update;
  }

  // Salida: el dinero sale sin distinguir de dónde vino, así que **las dos
  // porciones bajan en la misma proporción**. Retirar la mitad del saldo retira
  // la mitad de lo comprado y la mitad de lo traducido; el reparto no depende
  // del orden en que se registraron las entradas, igual que el promedio
  // ponderado (RN-02). Vale igual para una base mantenida en otra moneda
  // (RN-2.8-B): no hace falta ninguna tasa.
  const outflowFraction = Math.abs(currentBalance) > EMPTY_BALANCE_EPSILON
    ? Math.min(Math.abs(amountDelta) / Math.abs(currentBalance), 1)
    : 1;
  const remainingConverted = {
    convertedAmount: round2(previousConvertedAmount * (1 - outflowFraction)),
    convertedCost: round2(previousConvertedCost * (1 - outflowFraction)),
  };

  // Salida: se retira costo a la tasa promedio vigente. Si el costo no era
  // determinable, sigue sin serlo.
  const averageRate = deriveAverageRate(currentBasis, currentBalance, target);

  if (averageRate === null) {
    if (currentBasis) {
      update[basisPath] = {
        cost: null,
        referenceCurrency: target,
        status: COST_BASIS_STATUS.UNKNOWN,
        ...remainingConverted,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
    }
    return update;
  }

  update[basisPath] = {
    cost: round2(currentBasis.cost + amountDelta * averageRate),
    referenceCurrency: target,
    status: COST_BASIS_STATUS.KNOWN,
    ...remainingConverted,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  return update;
}

module.exports = {
  buildBalanceUpdate,
  resolveBasisTarget,
  basisValuationNeed,
  deriveAverageRate,
  deriveConvertedAverageRate,
  convertedAmountOf,
  getUserReferenceCurrency,
  isUsableRate,
  BASIS_MODES,
  COST_BASIS_STATUS,
  EMPTY_BALANCE_EPSILON,
};
