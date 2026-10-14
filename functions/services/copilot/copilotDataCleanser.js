/**
 * HU 5.3 — Depurar antes de concluir (RN-5-G).
 *
 * Las posiciones sin precio actual, con unidades o valor negativos o con cifras
 * imposibles se excluyen **antes de calcular un solo peso**, y se declaran. El
 * modelo nunca llega a verlas.
 *
 * Esto es código y no una instrucción a propósito. En la prueba real de la HU
 * 5.1, con la estrategia de depuración dentro de las instrucciones, **dos de
 * los tres motores ignoraron en silencio la posición sin precio** y dos
 * afirmaron que el portafolio superaba al S&P 500 con una cifra falsa. Una
 * guarda que depende de que el modelo obedezca no es una guarda.
 *
 * @module services/copilot/copilotDataCleanser
 * @see platform-docs/stories/5.3-analista-investiga-recomienda/refinamiento.md (T1)
 */

/**
 * Por qué se excluyó una posición. El usuario ve una línea con los tickers; el
 * motivo sirve para el log y para que el QA distinga un caso de otro.
 */
const EXCLUSION_REASON = {
  /** No hay precio de mercado para el símbolo. */
  SIN_PRECIO: 'sin_precio',
  /** Precio cero o negativo: no se puede valorar la posición. */
  PRECIO_INVALIDO: 'precio_invalido',
  /** Unidades negativas: una posición abierta no puede deber acciones. */
  UNIDADES_INVALIDAS: 'unidades_invalidas',
  /** Costo de adquisición negativo o no finito. */
  COSTO_INVALIDO: 'costo_invalido',
};

/**
 * ¿Es un número utilizable en un cálculo financiero?
 *
 * `NaN` e `Infinity` entran por conversiones de divisa con tasa cero y por
 * datos importados a medias; los dos envenenan cualquier peso que se calcule
 * después.
 *
 * @param {*} valor
 * @returns {boolean}
 */
function esNumeroUsable(valor) {
  return typeof valor === 'number' && Number.isFinite(valor);
}

/**
 * Revisa una posición y devuelve el motivo por el que no sirve, o `null`.
 *
 * @param {object} asset - Documento de `assets`.
 * @param {object|undefined} priceInfo - Su entrada en `currentPrices`.
 * @returns {string|null} Un valor de `EXCLUSION_REASON`, o `null` si es válida.
 */
function motivoDeExclusion(asset, priceInfo) {
  if (!esNumeroUsable(asset.units) || asset.units <= 0) {
    return EXCLUSION_REASON.UNIDADES_INVALIDAS;
  }

  if (!priceInfo || priceInfo.price === undefined || priceInfo.price === null) {
    return EXCLUSION_REASON.SIN_PRECIO;
  }

  if (!esNumeroUsable(priceInfo.price) || priceInfo.price <= 0) {
    return EXCLUSION_REASON.PRECIO_INVALIDO;
  }

  // El costo puede faltar —una posición importada sin precio de compra— y eso
  // no impide valorarla ni analizarla; lo que no puede es ser absurdo.
  if (asset.unitValue !== undefined && asset.unitValue !== null) {
    if (!esNumeroUsable(asset.unitValue) || asset.unitValue < 0) {
      return EXCLUSION_REASON.COSTO_INVALIDO;
    }
  }

  return null;
}

/**
 * Separa las posiciones que se pueden analizar de las que no.
 *
 * @param {object[]} assets - Documentos de `assets` del usuario.
 * @param {Map<string, object>} priceMap - Precios por símbolo.
 * @returns {{ validos: object[], excluidos: Array<{ticker: string, motivo: string}> }}
 *   `excluidos` va ordenado por ticker para que la línea que lee el usuario sea
 *   estable entre análisis.
 */
function cleanse(assets, priceMap) {
  const validos = [];
  const excluidosPorTicker = new Map();

  for (const asset of assets) {
    const ticker = asset.name || '';
    if (!ticker) continue;

    const motivo = motivoDeExclusion(asset, priceMap.get(ticker));

    if (motivo === null) {
      validos.push(asset);
      continue;
    }

    // Un ticker en dos cuentas se declara una sola vez (RN-5.3-O): el usuario
    // lee "BTC-USD", no "BTC-USD, BTC-USD".
    if (!excluidosPorTicker.has(ticker)) {
      excluidosPorTicker.set(ticker, { ticker, motivo });
    }
  }

  // Una posición válida rescata a su ticker: si el usuario tiene MSFT en dos
  // cuentas y una de las dos está corrupta, MSFT se analiza con la buena en vez
  // de desaparecer del análisis entero.
  const tickersValidos = new Set(validos.map((a) => a.name));
  const excluidos = [...excluidosPorTicker.values()]
    .filter((e) => !tickersValidos.has(e.ticker))
    .sort((a, b) => a.ticker.localeCompare(b.ticker));

  return { validos, excluidos };
}

module.exports = {
  EXCLUSION_REASON,
  esNumeroUsable,
  motivoDeExclusion,
  cleanse,
};
