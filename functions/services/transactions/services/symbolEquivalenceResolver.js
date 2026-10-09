/**
 * Symbol Equivalence Resolver
 *
 * HU 1.2: resuelve los símbolos de un archivo de broker aplicando primero la
 * memoria de equivalencias del usuario y validando contra las fuentes de datos
 * el TICKER CANÓNICO, no el símbolo del archivo.
 *
 * Ese detalle es lo que hace que el escenario 7 (activo que dejó de existir) salga
 * gratis: si el ticker canónico ya no valida, la equivalencia se degrada a "no
 * resuelto" y el usuario vuelve a elegir (RN-17).
 *
 * HU 1.6: una segunda pasada consulta el catálogo global SOLO para los símbolos que
 * el usuario no ha resuelto nunca (RN-06). Como la validación del ticker canónico
 * es la misma para ambos orígenes, una entrada global que apunte a un activo que ya
 * no existe se degrada igual que una propia (RN-17).
 *
 * @module transactions/services/symbolEquivalenceResolver
 */

const { getUserEquivalences, buildEquivalenceKey } = require('./importMemoryRepository');
const { getActiveEntries } = require('./globalEquivalenceRepository');
const { normalizeTicker } = require('./tickerValidator');

// ============================================================================
// MAIN
// ============================================================================

/**
 * Resuelve los símbolos del archivo usando la memoria del usuario.
 *
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.sourceFormatId
 * @param {string[]} params.symbols - Símbolos tal como aparecen en el archivo
 * @param {Function} params.validate - `validateTickerSample`, inyectada para poder testear
 * @returns {Promise<Object>} { tickerValidation, equivalences }
 */
async function resolveSymbols({ userId, sourceFormatId, symbols, validate }) {
  const fileSymbols = [...new Set(
    (symbols || []).map(s => normalizeTicker(s)).filter(Boolean)
  )];

  if (fileSymbols.length === 0) {
    return { tickerValidation: await validate([]), equivalences: {} };
  }

  // ── 1. Memoria propia del usuario ────────────────────────────────────────
  const remembered = await getUserEquivalences(userId, sourceFormatId, fileSymbols);

  // ── 1b. Catálogo global, SOLO para lo que el usuario no tiene (RN-06) ─────
  // La memoria propia siempre gana. Consultar el catálogo para símbolos que el
  // usuario ya resolvió sería, además de inútil, la vía por la que el catálogo
  // podría sobreescribir su corrección (escenario 3).
  const unresolvedSymbols = fileSymbols.filter(symbol => !remembered[symbol]);

  if (unresolvedSymbols.length > 0) {
    const globalKeys = unresolvedSymbols.map(symbol => buildEquivalenceKey(sourceFormatId, symbol));
    const globalEntries = await getActiveEntries(globalKeys);

    for (const symbol of unresolvedSymbols) {
      const entry = globalEntries[buildEquivalenceKey(sourceFormatId, symbol)];

      if (entry) {
        remembered[symbol] = { ...entry, sourceSymbol: symbol, origin: 'global' };
      }
    }
  }

  // ── 2. Qué se manda a validar ────────────────────────────────────────────
  // Los símbolos con equivalencia se validan por su ticker canónico. Los demás,
  // por sí mismos. Se deduplica porque dos símbolos pueden apuntar al mismo ticker.
  const validationTargets = new Set();
  /** symbol del archivo → ticker que lo representa en la validación */
  const targetBySymbol = {};

  for (const symbol of fileSymbols) {
    const equivalence = remembered[symbol];
    const target = equivalence
      ? normalizeTicker(equivalence.resolvedSymbol)
      : symbol;

    targetBySymbol[symbol] = target;
    validationTargets.add(target);
  }

  // ── 3. Validación contra las fuentes de datos ────────────────────────────
  const rawValidation = await validate([...validationTargets]);

  // ── 4. Traducción del resultado a los símbolos DEL ARCHIVO ───────────────
  // El usuario razona sobre los símbolos que ve en su archivo, no sobre los
  // tickers canónicos que resolvimos por él. Los conteos y las listas se
  // reexpresan en esos términos.
  return translateValidation({ fileSymbols, targetBySymbol, remembered, rawValidation });
}

// ============================================================================
// HELPERS
// ============================================================================

/**
 * Reexpresa el resultado de validación en términos de los símbolos del archivo y
 * construye el mapa de equivalencias aplicables.
 *
 * @param {Object} params
 * @returns {Object} { tickerValidation, equivalences }
 */
function translateValidation({ fileSymbols, targetBySymbol, remembered, rawValidation }) {
  const tickerValidation = {
    total: fileSymbols.length,
    valid: 0,
    invalid: 0,
    unverified: 0,
    invalidTickers: [],
    unverifiedTickers: [],
    suggestions: {},
    details: {},
    validDetails: {},
  };

  /** símbolo del archivo → equivalencia aplicable */
  const equivalences = {};

  for (const symbol of fileSymbols) {
    const target = targetBySymbol[symbol];
    const targetDetail = findDetail(rawValidation, target);
    const equivalence = remembered[symbol];

    const isValid = !!targetDetail?.isValid;
    const isUnverified = !!targetDetail?.isUnverified;

    if (isValid) {
      tickerValidation.valid++;

      // El detalle se indexa por el símbolo del archivo, pero describe el activo
      // real al que quedó vinculado.
      tickerValidation.details[symbol] = { ...targetDetail, originalTicker: symbol };

      const targetValidDetail = rawValidation.validDetails?.[target]
        || rawValidation.validDetails?.[target?.toUpperCase()];

      if (targetValidDetail) {
        tickerValidation.validDetails[symbol] = targetValidDetail;
      }

      if (equivalence) {
        equivalences[symbol] = {
          ...equivalence,
          // La metadata manda la fuente de datos, no lo que guardamos hace meses:
          // un ETF puede cambiar de divisa de cotización.
          currency: targetValidDetail?.currency || equivalence.currency,
          resolvedSymbol: targetDetail.normalizedTicker || equivalence.resolvedSymbol,
        };
      }

      continue;
    }

    // ── RN-17: la equivalencia apunta a un activo que ya no se reconoce ────
    // Se degrada a "no resuelto". No se borra de Firestore: el usuario vuelve a
    // elegir y su corrección la reemplaza por RN-05.
    if (isUnverified) {
      tickerValidation.unverified++;
      tickerValidation.unverifiedTickers.push(symbol);
      tickerValidation.details[symbol] = {
        originalTicker: symbol,
        isValid: false,
        isUnverified: true,
        error: targetDetail?.error || 'No se pudo verificar',
      };
      continue;
    }

    tickerValidation.invalid++;
    tickerValidation.invalidTickers.push(symbol);
    tickerValidation.details[symbol] = {
      originalTicker: symbol,
      isValid: false,
      error: equivalence
        ? 'El activo vinculado ya no está disponible'
        : (targetDetail?.error || 'Ticker not found'),
    };

    // Solo se propone sugerencia para símbolos sin equivalencia previa: si la
    // había y falló, lo correcto es que el usuario elija, no proponerle otro alias.
    if (!equivalence) {
      const suggestion = rawValidation.suggestions?.[target];
      if (suggestion) {
        tickerValidation.suggestions[symbol] = suggestion;
        tickerValidation.details[symbol].suggestion = suggestion;
      }
    }
  }

  return { tickerValidation, equivalences };
}

/**
 * Busca el detalle de validación de un ticker tolerando diferencias de caja.
 *
 * @param {Object} validation - Resultado de validateTickerSample
 * @param {string} ticker
 * @returns {Object|null}
 */
function findDetail(validation, ticker) {
  if (!validation?.details || !ticker) {
    return null;
  }

  return validation.details[ticker]
    || validation.details[ticker.toUpperCase()]
    || null;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  resolveSymbols,

  // For testing
  translateValidation,
  findDetail,
};
