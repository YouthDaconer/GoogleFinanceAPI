/**
 * Query Handlers - Lógica de negocio para operaciones de consulta
 * 
 * SCALE-CF-001: Handlers para consolidación de Cloud Functions de consulta.
 * Incluye funciones migradas desde:
 * - historicalReturnsService.js
 * - userPricesService.js  
 * - indexHistoryService.js
 * - index.js (inline: getPortfolioDistribution, getAvailableSectors)
 * 
 * OPT-DEMAND-CLEANUP: getCurrentPricesForUser migrado para usar API Lambda
 * en lugar de Firestore collection('currentPrices').
 * 
 * @module handlers/queryHandlers
 * @see docs/stories/56.story.md
 * @see docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
 */

const crypto = require('crypto');
const { HttpsError } = require("firebase-functions/v2/https");
const admin = require('../firebaseAdmin');
const db = admin.firestore();

// ============================================================================
// IMPORTS DE SERVICIOS
// ============================================================================

// Servicios con lógica compleja que se reutilizan
const portfolioDistributionService = require('../portfolioDistributionService');
const { 
  calculateHistoricalReturns, 
  getHistoricalReturnsInternal 
} = require('../historicalReturnsService');
const { calculateDynamicTTL } = require('../cacheInvalidationService');
const { calculateIndexData } = require('../indexHistoryService');
const { DateTime } = require('luxon');

// COST-OPT-001: Importar servicio de rendimientos consolidados (V2)
// HU 6.13: solo lo usa la ruta antigua con el interruptor apagado.
const { getHistoricalReturnsV2 } = require('../consolidatedReturnsService');
const {
  consolidadosRetirados,
  rendimientosNoDisponibles,
} = require('../consolidation/consolidatedPeriodsRetirementService');

// Importar utilidades MWR para cálculos de multi-account
const {
  calculateSimplePersonalReturn,
  calculateModifiedDietzReturn
} = require('../../utils/mwrCalculations');

// OPT-DEMAND-CLEANUP: Importar helper para obtener precios del API Lambda
const { getPricesFromApi } = require('../marketDataHelper');

// HU 2.1: Tasa de cambio de una fecha concreta para el diálogo de efectivo
const historicalRateService = require('../historicalRateService');
const { getUserReferenceCurrency } = require('../helpers/balanceCostBasis');
// HU 2.6: proyeccion del saldo a partir de sus movimientos
const { projectBalanceLedger } = require('../helpers/balanceLedger');
// HU 2.7: todo lo que hace falta saber antes de corregir el saldo inicial
const { planOpeningCorrection } = require('../helpers/openingBalanceCorrection');

// FEAT-EXCLUDE-001: Rendimiento del portafolio excluyendo tickers
const { getCarveOutHistoricalReturns } = require('../carveOutReturnsService');
const { buildExcludeSet } = require('../../utils/carveOutReturns');

// PERF-SNAP-007: Importar helper para construir ID de snapshot
// PERF-SNAP-009: Importar generatePerformanceSnapshot para on-demand generation
// PERF-SNAP-024: Importar generateAssetSnapshot para on-demand per-asset
const { buildSnapshotDocId, generatePerformanceSnapshot, generateAssetSnapshot } = require('../snapshotGenerator');
// HU 6.4: los resúmenes por activo se ponen al día al abrirlos, no cada noche.
const { resumenesPorActivoBajoDemanda, ponerAlDiaResumenDeActivo, completarDiasDelActivo } = require('../snapshots/assetSnapshotOnDemandService');
const {
  resumenesDerivadosDelUsd,
  obtenerResumenDerivado,
  esMonedaPocoUsada,
  monedaPrincipalDe,
  avisoMonedaNoDisponible,
  monedaNoDisponible,
  CODIGO_MONEDA_NO_DISPONIBLE,
} = require('../snapshots/derivedCurrencySnapshotService');
// HU 6.11.3: detalle de activo y Versus en monedas poco usadas, desde los días en USD.
const { fetchAssetDailyDocs } = require('../snapshotGenerator');
const {
  calculaDesdeUsd,
  activoDesdeResumenUsd,
  resumenDeActivoDesdeUsd,
  diasEnMonedaDesdeUsd,
} = require('../snapshots/rareCurrencyScreensService');
// HU 6.11.6: el detalle en una moneda poco usada sale de los días guardados en el resumen en USD.
const { docsDesdeDiasDelActivo } = require('../../utils/assetDays');
// HU 6.11.2: un resumen viejo se prepara una sola vez aunque lo pidan varios a la vez.
// HU 6.11.6: también el resumen por activo en USD (`prepararUnaVez`).
const { prepararResumenUnaVez, prepararUnaVez } = require('../snapshots/snapshotPreparationService');
// HU 6.11.5: la cabecera del día anterior, solo en la moneda que se muestra
const { obtenerCabeceraDiaAnterior } = require('../snapshots/previousDayPerformanceService');

// PERF-SNAP-021: Importar funciones de mercado para TTL inteligente
const { isNYSEMarketOpen, calculateTTLUntilNextEOD, MARKET_CACHE_TTL_MS } = require('../riskMetrics/riskMetricsCache');

// ============================================================================
// CONSTANTES
// ============================================================================

/**
 * Movimientos que devuelve `getBalanceLedger` si el cliente no pide otra cosa.
 *
 * Cubre de sobra lo que la tarjeta muestra al desplegarse; el resto se pide
 * cuando el usuario decide mirar más atrás.
 */
const LEDGER_DEFAULT_ROWS = 50;

/** Tope duro por petición: nadie lee mil filas de una tabla plegable. */
const LEDGER_MAX_ROWS = 500;

const VALID_INDEX_RANGES = ["1M", "3M", "6M", "YTD", "1Y", "5Y", "MAX"];
const INDEX_CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 horas
// OPT-FIRESTORE-002: Aumentado de 5min a 1h (consistente con indexHistoryService)
const INDEX_INTRADAY_CACHE_TTL_MS = 60 * 60 * 1000; // 1 hora (cuando falta punto de hoy)

// PERF-SNAP-021: In-memory cache para snapshots a nivel de módulo (sobrevive warm starts)
const SNAPSHOT_MEM_CACHE_MAX_SIZE = 100;
const snapshotMemCache = new Map();

function getSnapshotCacheTTL() {
  return isNYSEMarketOpen() ? MARKET_CACHE_TTL_MS : calculateTTLUntilNextEOD();
}

// R-03: Read the lastSnapshotUpdate signal once per CF invocation, reuse across N calls.
async function readSnapshotSignal(userId) {
  if (!userId) return null;
  try {
    const userPerfDoc = await db.doc(`portfolioPerformance/${userId}`).get();
    return userPerfDoc.exists ? userPerfDoc.data()?.lastSnapshotUpdate || null : null;
  } catch {
    return null;
  }
}

async function getSnapshotWithCache(snapshotId, userId, preloadedSignal) {
  // R-03: Accept pre-loaded signal to avoid N reads of the same doc per CF invocation.
  // If preloadedSignal is provided (string or null), skip Firestore read entirely.
  // If preloadedSignal is undefined, read from Firestore (backwards-compatible).
  let lastSnapshotUpdate = null;
  if (preloadedSignal !== undefined) {
    lastSnapshotUpdate = preloadedSignal;
  } else if (userId) {
    try {
      const userPerfDoc = await db.doc(`portfolioPerformance/${userId}`).get();
      lastSnapshotUpdate = userPerfDoc.exists ? userPerfDoc.data()?.lastSnapshotUpdate || null : null;
    } catch {
      // Graceful degradation — rely on TTL if read fails
    }
  }

  const cached = snapshotMemCache.get(snapshotId);
  if (cached && Date.now() - cached.cachedAt < getSnapshotCacheTTL()) {
    // PERF-SNAP-023: Invalidate if EOD generated newer data
    if (lastSnapshotUpdate && new Date(lastSnapshotUpdate).getTime() > cached.cachedAt) {
      snapshotMemCache.delete(snapshotId);
    } else {
      snapshotMemCache.delete(snapshotId);
      snapshotMemCache.set(snapshotId, cached);
      return { data: cached.data, lastSnapshotUpdate };
    }
  }

  const doc = await db.doc(`performanceSnapshots/${snapshotId}`).get();
  if (!doc.exists) {
    console.log(`[SNAPSHOT-MISS] ${JSON.stringify({ snapshotId, userId, reason: 'not-found' })}`);
    return { data: null, lastSnapshotUpdate };
  }

  const data = doc.data();

  // OPT-FX-001: Detect stale Firestore snapshot (generated before last EOD run).
  // Snapshots for non-default currencies (e.g. COP when defaultCurrency=USD) are
  // only generated on-demand and never refreshed by the scheduled EOD pipeline.
  // Without this check, the stale snapshot is served indefinitely.
  if (lastSnapshotUpdate && data.lastUpdated &&
      new Date(lastSnapshotUpdate).getTime() > new Date(data.lastUpdated).getTime()) {
    console.log(`[SNAPSHOT-MISS] ${JSON.stringify({ snapshotId, userId, reason: 'stale', snapshotLastUpdated: data.lastUpdated, signalLastUpdate: lastSnapshotUpdate })}`);
    // HU 6.4: el resumen viejo se entrega aparte para ponerlo al día sin
    // volver a leerlo. `data` sigue en null: nadie más cambia de comportamiento.
    return { data: null, staleData: data, lastSnapshotUpdate };
  }

  snapshotMemCache.set(snapshotId, { data, cachedAt: Date.now() });

  if (snapshotMemCache.size > SNAPSHOT_MEM_CACHE_MAX_SIZE) {
    const oldestKey = snapshotMemCache.keys().next().value;
    snapshotMemCache.delete(oldestKey);
  }

  return { data, lastSnapshotUpdate };
}

/**
 * HU 6.9.2 (T4) — El resumen de un alcance en una moneda poco usada, derivado
 * del USD, con la misma caché en memoria y la misma regla de la señal que los
 * guardados: un cierre o una corrección posteriores obligan a volver a derivar.
 *
 * HU 6.11.1 (T1): si la moneda es poco usada pero no hay con qué derivar (el
 * servicio de tasas no responde, falta o es inválida una tasa, no hay resumen en
 * USD), lanza `monedaNoDisponible` en lugar de devolver `null`: quien llama ya no
 * llega a lo guardado en esa moneda (RN-6.11-J, RN-6.11-K). El aviso no se
 * recuerda en la caché: la siguiente consulta vuelve a intentarlo (Escenario 3).
 *
 * @returns {Promise<Object|null>} El derivado, o `null` si la moneda no es poco
 *   usada (o el interruptor está apagado) y hay que seguir el camino de siempre
 * @throws {HttpsError} `unavailable` con `details.code = 'CURRENCY_UNAVAILABLE'`
 * @see platform-docs/stories/6.9.2-resumenes-poco-usados-incrementales/refinamiento.md (T4)
 * @see platform-docs/stories/6.11.1-sin-tasa-aviso-y-usd/refinamiento.md (T1)
 */
async function resumenDerivadoConCache(userId, accountId, currency, signal) {
  if (currency === 'USD' || !resumenesDerivadosDelUsd()) return null;

  const clave = `derivado:${buildSnapshotDocId(userId, accountId, currency)}`;
  const cached = snapshotMemCache.get(clave);
  if (cached && Date.now() - cached.cachedAt < getSnapshotCacheTTL() &&
      !(signal && new Date(signal).getTime() > cached.cachedAt)) {
    return cached.data;
  }

  // Sin derivado (no aplica o no derivable) se sigue el camino de siempre.
  // `prepararUsd`: si el de USD falta o está viejo, se prepara, como ya hacía esta lectura.
  const { snapshot, metodo, motivo } = await obtenerResumenDerivado(db, { userId, accountId, currency, senal: signal, prepararUsd: true });
  if (!snapshot) {
    if (metodo === 'no-derivable' && avisoMonedaNoDisponible()) {
      throw monedaNoDisponible(currency, motivo, buildSnapshotDocId(userId, accountId, currency));
    }
    return null;
  }

  snapshotMemCache.set(clave, { data: snapshot, cachedAt: Date.now() });
  if (snapshotMemCache.size > SNAPSHOT_MEM_CACHE_MAX_SIZE) {
    snapshotMemCache.delete(snapshotMemCache.keys().next().value);
  }
  return snapshot;
}

/**
 * HU 6.11.6 (T4) — El resumen por activo de un alcance, al día: el de la memoria
 * o el guardado si siguen vigentes según la señal; si está viejo se pone al día
 * (HU 6.4) y si falta se prepara entero.
 *
 * Es el bloque por activo que tenía `getHistoricalReturns`, extraído sin cambiar
 * lo que hace en USD para que el detalle en una moneda poco usada use el mismo.
 * La preparación corre dentro de `prepararUnaVez`: dos aperturas a la vez del
 * mismo activo preparan una sola vez (RN-6.11.6-J, decisión 4 del refinamiento).
 *
 * Con `completarDias` (solo la ruta de la 6.11.6, en USD) un resumen sin sus
 * días (`assetDays`, anterior a la HU) se completa una vez sin tocar sus cifras
 * (RN-6.11.6-A, decisión 2).
 *
 * @param {string} userId
 * @param {Object} params - `{ accountId, currency, ticker, assetType }`
 * @param {string|null|undefined} signal - `lastSnapshotUpdate` ya leído
 * @param {Object} [opciones]
 * @param {boolean} [opciones.completarDias=false]
 * @returns {Promise<{snapshot: Object|null, lastSnapshotUpdate: string|null, preparado: boolean, error?: Error}>}
 *   Si la preparación falla, `snapshot: null` con el `error` (como antes, se avisa
 *   en el log y quien llama sigue su camino); un fallo al leer se propaga
 * @see platform-docs/stories/6.11.6-detalle-activo-desde-resumen-usd/refinamiento.md (T4)
 */
async function resumenDeActivoAlDia(userId, params, signal, opciones = {}) {
  const { accountId, currency, ticker, assetType } = params;
  const { completarDias = false } = opciones;
  const id = buildSnapshotDocId(userId, accountId, currency, ticker, assetType);
  const leido = await getSnapshotWithCache(id, userId, signal);
  const lastSnapshotUpdate = leido.lastSnapshotUpdate || null;
  let snapshot = leido.data;
  let preparado = false;

  try {
    if (!snapshot) {
      // HU 6.4: si el resumen existe pero está viejo, se pone al día con los días
      // que le faltan (RN-6.4-K); si no existe, se prepara entero (Escenario 1).
      const ponerAlDia = !!leido.staleData && resumenesPorActivoBajoDemanda();
      console.log(`[PERF] Asset snapshot miss - ${id}, ${ponerAlDia ? 'catching up' : 'generating'} on-demand`);
      snapshot = await prepararUnaVez(id, async () => (ponerAlDia
        ? (await ponerAlDiaResumenDeActivo(db, {
          userId,
          accountId,
          currency,
          ticker,
          assetType,
          snapshot: leido.staleData,
          senal: lastSnapshotUpdate,
          ...(completarDias ? { completarDias } : {}),
        })).snapshot
        : await generateAssetSnapshot(db, userId, accountId, currency, ticker, assetType)));
      snapshot = snapshot || null;
      preparado = true;
    }

    if (snapshot && completarDias && currency === 'USD' && !Array.isArray(snapshot.assetDays)) {
      const actual = snapshot;
      snapshot = await prepararUnaVez(`${id}#dias`, () => completarDiasDelActivo(db, {
        userId, accountId, ticker, assetType, snapshot: actual,
      }));
      preparado = true;
    }
  } catch (error) {
    console.warn(`[PERF] On-demand asset snapshot generation failed for ${id}: ${error.message}`);
    return { snapshot: null, lastSnapshotUpdate, preparado: false, error };
  }

  if (snapshot && preparado) {
    snapshotMemCache.set(id, { data: snapshot, cachedAt: Date.now() });
    if (snapshotMemCache.size > SNAPSHOT_MEM_CACHE_MAX_SIZE) {
      snapshotMemCache.delete(snapshotMemCache.keys().next().value);
    }
  }
  return { snapshot, lastSnapshotUpdate, preparado };
}

/**
 * HU 6.11.6 (T5) — Los días en USD de un activo reconstruidos desde su resumen
 * por activo en USD, preparado o completado una vez si hace falta.
 *
 * Si el resumen no se puede usar (falla al prepararlo o completarlo, no hay
 * resumen o no trae días) devuelve `null` y quien llama calcula como en la
 * 6.11.3, desde los días en USD (RN-6.11.6-D): nunca lo guardado en la moneda.
 *
 * @param {string} userId
 * @param {Object} params - `{ accountId, ticker, assetType }`
 * @param {string|null|undefined} signal
 * @returns {Promise<Object[]|null>}
 * @see platform-docs/stories/6.11.6-detalle-activo-desde-resumen-usd/refinamiento.md (T5)
 */
async function diasDelResumenUsd(userId, { accountId, ticker, assetType }, signal) {
  const id = buildSnapshotDocId(userId, accountId, 'USD', ticker, assetType);
  try {
    const { snapshot, error } = await resumenDeActivoAlDia(
      userId, { accountId, currency: 'USD', ticker, assetType }, signal, { completarDias: true });
    if (snapshot && Array.isArray(snapshot.assetDays)) {
      return docsDesdeDiasDelActivo(snapshot.assetDays, `${ticker}_${assetType}`);
    }
    if (!error) {
      console.warn(`[rareCurrencyScreens] ${id}: ${snapshot ? 'sin días' : 'sin resumen'}, se calcula desde los días en USD`);
    } else {
      console.warn(`[rareCurrencyScreens] ${id}: no se pudo preparar (${error.message}), se calcula desde los días en USD`);
    }
  } catch (err) {
    console.warn(`[rareCurrencyScreens] ${id}: no se pudo leer (${err.message}), se calcula desde los días en USD`);
  }
  return null;
}

/**
 * HU 6.11.3 (T2) — El detalle de un activo (portafolio o una cuenta) en una
 * moneda poco usada, desde sus días en USD, con la misma caché en memoria y la
 * misma regla de la señal que el derivado de la 6.9.2.
 *
 * Ya no se lee el resumen guardado en esa moneda, ni se rehace con lo guardado,
 * ni se cae a la ruta legacy (RN-6.11-J). Sin con qué calcular, el aviso de la
 * 6.11.1 (RN-6.11-K), que no se recuerda: la siguiente apertura lo reintenta.
 *
 * @returns {Promise<Object|undefined>} La respuesta, o `undefined` si la consulta
 *   no se calcula desde USD (USD, moneda principal o interruptor apagado)
 * @throws {HttpsError} `unavailable` con `details.code = 'CURRENCY_UNAVAILABLE'`
 * @see platform-docs/stories/6.11.3-activo-comparacion-riesgo-usd/refinamiento.md (T2)
 */
async function activoDesdeUsdConCache(userId, params, signal) {
  const { accountId, currency, ticker, assetType, forceRefresh } = params;
  if (!(await calculaDesdeUsd(db, userId, currency))) return undefined;

  const id = buildSnapshotDocId(userId, accountId, currency, ticker, assetType);
  const clave = `derivado:${id}`;
  const cached = snapshotMemCache.get(clave);
  const vigente = cached && Date.now() - cached.cachedAt < getSnapshotCacheTTL() &&
    !(signal && new Date(signal).getTime() > cached.cachedAt);

  let snapshot = vigente && !forceRefresh ? cached.data : null;
  if (!snapshot) {
    // HU 6.11.6 (T5): los días salen del resumen en USD (una lectura); si no se
    // puede usar, o con `forceRefresh`, se leen como en la 6.11.3.
    const dias = activoDesdeResumenUsd() && !forceRefresh
      ? await diasDelResumenUsd(userId, { accountId, ticker, assetType }, signal)
      : null;
    const params = { userId, accountId, currency, ticker, assetType };
    const resultado = dias
      ? await resumenDeActivoDesdeUsd(db, params, { dias })
      : await resumenDeActivoDesdeUsd(db, params);
    if (resultado.metodo === 'no-derivable') throw monedaNoDisponible(currency, resultado.motivo, id);
    snapshot = resultado.snapshot;
    if (snapshot) {
      snapshotMemCache.set(clave, { data: snapshot, cachedAt: Date.now() });
      if (snapshotMemCache.size > SNAPSHOT_MEM_CACHE_MAX_SIZE) {
        snapshotMemCache.delete(snapshotMemCache.keys().next().value);
      }
    }
  }

  // Sin datos del activo en ese alcance: la respuesta vacía, sin la ruta legacy.
  const result = snapshot
    ? transformSnapshotToResponse(snapshot)
    : calculateHistoricalReturns([], currency, ticker, assetType);
  return {
    ...result,
    cacheHit: false,
    lastCalculated: new Date().toISOString(),
    validUntil: calculateDynamicTTL().toISOString(),
    lastSnapshotUpdate: signal || null,
  };
}

/**
 * HU 6.11.3 (T3) — El detalle de un activo con dos o más cuentas (no todas) en
 * una moneda poco usada: los días del activo en USD de cada cuenta, convertidos,
 * y **la misma agregación** de la ruta legacy, que en USD lee los días completos.
 * No guarda nada en la caché de Firestore: es una moneda poco usada y casi nadie
 * la consulta.
 *
 * HU 6.11.6 (T5): los días de cada cuenta salen de su resumen por activo en USD
 * (una lectura por cuenta); solo la cuenta cuyo resumen no se puede usar lee sus
 * días (RN-6.11.6-E, I).
 *
 * @throws {HttpsError} `unavailable` con `details.code = 'CURRENCY_UNAVAILABLE'`
 * @see platform-docs/stories/6.11.3-activo-comparacion-riesgo-usd/refinamiento.md (T3)
 * @see platform-docs/stories/6.11.6-detalle-activo-desde-resumen-usd/refinamiento.md (T5)
 */
async function activoVariasCuentasDesdeUsd(userId, { accountIds, currency, ticker, assetType, forceRefresh = false }, signal) {
  const claveActivo = `${ticker}_${assetType}`;
  const inicio = Date.now();
  const desdeResumen = activoDesdeResumenUsd() && !forceRefresh;
  const origenes = [];
  const diasPorCuenta = await Promise.all(accountIds.map(async (accountId, i) => {
    const delResumen = desdeResumen ? await diasDelResumenUsd(userId, { accountId, ticker, assetType }, signal) : null;
    origenes[i] = delResumen ? 'resumen' : 'dias';
    const dias = delResumen || await fetchAssetDailyDocs(db, userId, accountId, 'USD', ticker, assetType);
    const conversion = await diasEnMonedaDesdeUsd(dias, currency, { claveActivo });
    if (conversion.motivo) {
      throw monedaNoDisponible(currency, conversion.motivo, buildSnapshotDocId(userId, accountId, currency, ticker, assetType));
    }
    return conversion.dias;
  }));

  const result = calculateHistoricalReturns(agregarDiasDeCuentas(diasPorCuenta), currency, ticker, assetType);
  const deDonde = accountIds.map((accountId, i) => `${accountId}:${origenes[i]}`).join(',');
  console.log(`[rareCurrencyScreens] activo ${userId}/[${deDonde}]/${claveActivo} ${currency}: derivado, ${Date.now() - inicio} ms`);
  return {
    ...result,
    cacheHit: false,
    lastCalculated: new Date().toISOString(),
    validUntil: calculateDynamicTTL().toISOString(),
  };
}

/**
 * HU 6.11.2 (T3) — ¿Se puede calcular el rendimiento del portafolio en esta
 * moneda? Solo lo pregunta quien no puede mostrar el aviso de la 6.11.1 y tiene
 * que cambiar de moneda por su cuenta: el Copiloto.
 *
 * En USD y en la moneda principal siempre (salen de sus resúmenes). En una
 * moneda poco usada, si hay derivado del USD. El derivado queda en la caché en
 * memoria, así que la consulta de rendimientos que viene después no lo repite.
 *
 * @param {string} userId
 * @param {string} currency
 * @returns {Promise<boolean>} `false` solo en una moneda poco usada sin con qué calcular
 * @see platform-docs/stories/6.11.2-una-cuenta-y-copiloto-sin-recorrer/refinamiento.md (T3)
 */
async function rendimientoDisponibleEn(userId, currency) {
  if (currency === 'USD' || !resumenesDerivadosDelUsd()) return true;
  const defaultCurrency = await monedaPrincipalDe(db, userId);
  if (!esMonedaPocoUsada(currency, defaultCurrency)) return true;

  const signal = await readSnapshotSignal(userId);
  try {
    const derivado = await resumenDerivadoConCache(userId, 'overall', currency, signal);
    return derivado !== null;
  } catch (error) {
    // Integración 6.11.1 + 6.11.2: sin con qué calcular, la 6.11.1 lanza el aviso
    // CURRENCY_UNAVAILABLE para la pantalla. Para el Copiloto eso es "no
    // disponible": analiza en USD (6.11.2). Otro error sigue su curso.
    if (error?.details?.code === CODIGO_MONEDA_NO_DISPONIBLE) return false;
    throw error;
  }
}

/**
 * HU 6.11.2 — Interruptor de "una cuenta, todas y el Copiloto salen de los
 * resúmenes". Se lee en cada llamada: basta cambiar la variable y desplegar
 * para volver al camino antiguo sin perder nada (RN-6-H, Escenario 6).
 *
 * @returns {boolean} `true` salvo `PERFORMANCE_FROM_SNAPSHOTS=false`
 */
function rendimientoDesdeResumenes() {
  return process.env.PERFORMANCE_FROM_SNAPSHOTS !== 'false';
}

/**
 * HU 6.11.2 (T1) — El rendimiento de un alcance (`'overall'` o una cuenta)
 * cuando `getMultiAccountHistoricalReturns` no tiene que agregar varias cuentas.
 *
 * Antes iba siempre a `getHistoricalReturnsInternal`, que lee una caché vieja
 * (sin mirar si hubo un cierre o una corrección) o **recorre todos los días
 * guardados** del alcance; en una moneda poco usada, lo guardado en esa moneda.
 * Ahora va por `getHistoricalReturns`, el camino de los resúmenes que ya usa la
 * vista de una cuenta y la de todas: el resumen preparado en USD y en la moneda
 * principal, derivado del USD en una moneda poco usada (HU 6.9.2), y preparado
 * una sola vez si falta o está viejo (Escenario 3).
 *
 * `ticker`/`assetType` y `forceRefresh` siguen por el camino antiguo: no son una
 * cuenta ni todas, y `forceRefresh` es pedir el cálculo completo.
 *
 * @param {Object} context - Contexto con `auth.uid`
 * @param {string} accountId - `'overall'` o el id de la cuenta
 * @param {Object} params - `{ currency, ticker, assetType, forceRefresh }`
 * @returns {Promise<Object>}
 * @see platform-docs/stories/6.11.2-una-cuenta-y-copiloto-sin-recorrer/refinamiento.md (T1)
 */
async function rendimientoDeUnAlcance(context, accountId, params) {
  const { currency, ticker, assetType, forceRefresh } = params;
  const userId = context.auth.uid;

  // HU 6.11.3 (T3): el detalle de un activo en una moneda poco usada sale de sus
  // días en USD (T2), no del camino antiguo, que lee lo guardado en esa moneda.
  if (ticker && assetType && await calculaDesdeUsd(db, userId, currency)) {
    return await getHistoricalReturns(context, { currency, accountId, ticker, assetType, forceRefresh });
  }

  if (!rendimientoDesdeResumenes() || ticker || assetType || forceRefresh) {
    return await getHistoricalReturnsInternal(userId, { currency, accountId, ticker, assetType, forceRefresh });
  }

  const inicio = Date.now();
  const resultado = await getHistoricalReturns(context, { currency, accountId });
  console.log(`[PERF][getMultiAccountHistoricalReturns] Desde resúmenes - ${accountId} ${currency} (${Date.now() - inicio} ms)`);
  return resultado;
}

function clearSnapshotMemCache() {
  snapshotMemCache.clear();
}

function getSnapshotMemCacheSize() {
  return snapshotMemCache.size;
}

// ============================================================================
// HANDLERS
// ============================================================================

/**
 * Obtiene precios actuales filtrados por los símbolos que el usuario posee
 * 
 * OPT-DEMAND-CLEANUP: Migrado para usar API Lambda en lugar de Firestore.
 * Ya NO lee de collection('currentPrices').
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta (vacío para este handler)
 * @returns {Promise<{prices: Array, symbols: Array, timestamp: number, source: string}>}
 */
async function getCurrentPricesForUser(context, payload) {
  const { auth } = context;
  const userId = auth.uid;

  // OPT-FIRESTORE-002 P0-B+: Frontend hook useUserCurrentPrices is dead code (0 imports).
  // Remaining 396 calls/week come from stale browser bundles with polling.
  // Short-circuit: return empty result without Firestore reads (-16K reads/mes).
  // Main price path uses marketDataPollingManager → API Lambda directly.
  console.log(`[queryHandlers][getCurrentPricesForUser] DEPRECATED — userId: ${userId} (stale client)`);
  return {
    prices: [],
    symbols: [],
    timestamp: Date.now(),
    source: 'deprecated',
    _deprecated: true,
    _message: 'getCurrentPricesForUser is deprecated. Use marketDataPollingManager for current prices.',
  };
}

// ============================================================================
// PERF-SNAP-008: Agregación de snapshots para multi-cuenta
// ============================================================================

/**
 * PERF-SNAP-008: Agrega N snapshots de cuentas individuales en un resultado
 * multi-cuenta unificado. Alinea timelines por fecha, pondera dailyChangePercentage
 * por valor pre-cambio (TWR multi-cuenta) y pasa "fake docs" a
 * calculateHistoricalReturns() para producir el formato final.
 *
 * @param {Array<Object>} snapshots - Snapshots individuales (cada uno con timeline, returns, etc.)
 * @param {string} currency - Código de moneda (e.g. 'USD')
 * @returns {Object} Resultado en formato idéntico al que produce la ruta legacy
 */
function aggregateSnapshotTimelines(snapshots, currency) {
  const dateMap = new Map();

  for (const snapshot of snapshots) {
    for (const entry of snapshot.timeline || []) {
      const { d: date, v: totalValue, c: dailyChangePercentage } = entry;

      if (!dateMap.has(date)) {
        dateMap.set(date, []);
      }
      dateMap.get(date).push({ totalValue, dailyChangePercentage });
    }
  }

  const sortedDates = Array.from(dateMap.keys()).sort();

  const aggregatedDocs = sortedDates.map(date => {
    const contributions = dateMap.get(date);

    const aggregatedTotalValue = contributions.reduce((sum, c) => sum + c.totalValue, 0);

    const contributionsWithPreValue = contributions.map(c => {
      const change = c.dailyChangePercentage || 0;
      const preChangeValue = change !== 0
        ? c.totalValue / (1 + change / 100)
        : c.totalValue;
      return { ...c, preChangeValue };
    });

    const totalWeight = contributionsWithPreValue.reduce((sum, c) => sum + c.preChangeValue, 0);

    let weightedChange = 0;
    if (totalWeight > 0) {
      weightedChange = contributionsWithPreValue.reduce((sum, c) => {
        return sum + (c.dailyChangePercentage || 0) * (c.preChangeValue / totalWeight);
      }, 0);
    }

    return {
      data: () => ({
        date,
        [currency]: {
          totalValue: aggregatedTotalValue,
          dailyChangePercentage: weightedChange,
          adjustedDailyChangePercentage: weightedChange,
        },
      }),
    };
  });

  return calculateHistoricalReturns(aggregatedDocs, currency);
}

/**
 * Aggregate monthlyCompound from multiple snapshots by summing P&L fields.
 * For percentage fields (returnPct), uses value-weighted average.
 */
function aggregateMonthlyCompounds(snapshots) {
  const result = {};

  for (const snapshot of snapshots) {
    const mc = snapshot.monthlyCompound || {};
    for (const [year, months] of Object.entries(mc)) {
      if (!result[year]) result[year] = {};
      for (const [month, data] of Object.entries(months)) {
        if (!result[year][month]) {
          result[year][month] = {
            returnPct: 0,
            startTotalValue: 0,
            startTotalInvestment: 0,
            endTotalValue: 0,
            endTotalInvestment: 0,
            totalCashFlow: 0,
            profit: 0,
            doneProfitAndLoss: 0,
            unrealizedProfitAndLoss: 0,
            lastDayOfMonth: data.lastDayOfMonth || false,
          };
        }
        const acc = result[year][month];
        acc.startTotalValue += data.startTotalValue || 0;
        acc.startTotalInvestment += data.startTotalInvestment || 0;
        acc.endTotalValue += data.endTotalValue || 0;
        acc.endTotalInvestment += data.endTotalInvestment || 0;
        acc.totalCashFlow += data.totalCashFlow || 0;
        acc.profit += data.profit || 0;
        acc.doneProfitAndLoss += data.doneProfitAndLoss || 0;
        acc.unrealizedProfitAndLoss += data.unrealizedProfitAndLoss || 0;
        if (data.lastDayOfMonth) acc.lastDayOfMonth = true;
      }
    }
  }

  return result;
}

/**
 * Aggregate performanceByYear from multiple snapshots.
 * Months get their TWR re-computed from the aggregated timeline (result.performanceByYear),
 * but P&L-related totals come from the snapshot compounds.
 */
function aggregatePerformanceByYear(snapshots, basePerformanceByYear) {
  const result = {};

  // Start from the base (computed from aggregated timeline — has correct TWR returns)
  for (const [year, data] of Object.entries(basePerformanceByYear || {})) {
    result[year] = { ...data };
  }

  // For each year, merge personalMonths/personalTotal from snapshots by summing
  for (const snapshot of snapshots) {
    const pby = snapshot.performanceByYear || {};
    for (const [year, data] of Object.entries(pby)) {
      if (!result[year]) {
        result[year] = { months: {}, personalMonths: {}, total: 0, personalTotal: 0 };
      }
    }
  }

  return result;
}

/**
 * PERF-SNAP-007: Transforma un snapshot pre-computado al formato de respuesta
 * que el frontend espera (idéntico a getHistoricalReturnsV2).
 */
function transformSnapshotToResponse(snapshot) {
  const dates = [];
  const values = [];
  const percentChanges = [];

  for (const entry of snapshot.timeline || []) {
    dates.push(entry.d);
    values.push(entry.v);
    percentChanges.push(entry.c);
  }

  const firstValue = values[0] || 0;
  const lastValue = values[values.length - 1] || 0;
  const overallPercentChange = firstValue > 0
    ? ((lastValue - firstValue) / firstValue) * 100
    : 0;

  const isDailyTimeline = snapshot.timelineGranularity === 'daily';

  // PERF-SNAP-025: Computar soldRanges y soldCompletelyDate desde timeline
  let soldCompletelyDate = null;
  const soldRanges = []; // Array de { from, to } — períodos sin posición
  if (isDailyTimeline) {
    const timeline = snapshot.timeline || [];
    const lastEntry = timeline[timeline.length - 1];

    // Detectar rangos vendidos: secuencias de entries con u<=0 seguidas de re-compra
    let soldStart = null;
    for (let i = 0; i < timeline.length; i++) {
      const e = timeline[i];
      if (e.u <= 0 && soldStart === null) {
        soldStart = e.d;
      } else if (e.u > 0 && soldStart !== null) {
        soldRanges.push({ from: soldStart, to: e.d });
        soldStart = null;
      }
    }
    // Si termina vendido, es venta permanente
    if (soldStart !== null) {
      soldCompletelyDate = soldStart;
    }
  }

  return {
    returns: snapshot.returns || {},
    validDocsCountByPeriod: snapshot.validDocsCountByPeriod || {},
    totalValueData: {
      dates,
      values,
      percentChanges,
      overallPercentChange,
      ...(isDailyTimeline && { timelineGranularity: 'daily' }),
      ...(isDailyTimeline && { soldCompletelyDate }),
      ...(isDailyTimeline && soldRanges.length > 0 && { soldRanges }),
    },
    performanceByYear: snapshot.performanceByYear || {},
    monthlyCompoundData: snapshot.monthlyCompound || {},
    availableYears: snapshot.availableYears || [],
    startDate: snapshot.startDate || '',
    latestAssetPerformance: snapshot.latestAssetPerformance || {},
    _metadata: {
      version: 'snapshot',
      schemaVersion: snapshot.schemaVersion,
      snapshotLastUpdated: snapshot.lastUpdated,
    },
  };
}

/**
 * Obtiene rendimientos históricos del portafolio
 * 
 * PERF-SNAP-007: Intenta leer snapshot pre-computado (1 read).
 * Si no existe o es request por ticker/assetType, cae a ruta legacy (V2/V1).
 *
 * HU 6.13: con `RETIRE_CONSOLIDATED_PERIODS` encendido (por defecto) ya no hay
 * ruta legacy: si el resumen no se puede preparar responde el aviso
 * `RETURNS_UNAVAILABLE` (RN-6.13-A) y `forceRefresh` no se salta los resúmenes.
 * @see platform-docs/stories/6.13-retirar-ruta-antigua-consolidados/refinamiento.md (T2)
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>} Rendimientos calculados
 */
async function getHistoricalReturns(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { 
    currency = "USD", 
    accountId = "overall", 
    ticker = null, 
    assetType = null, 
    forceRefresh = false 
  } = payload || {};

  console.log(`[queryHandlers][getHistoricalReturns] userId: ${userId}, currency: ${currency}, accountId: ${accountId}`);

  rejectTickerWithoutAssetType(payload, 'getHistoricalReturns');

  try {
    // FEAT-EXCLUDE-001: los snapshots son agregados y no permiten quitar
    // activos; la exclusión se calcula aparte, desde los docs diarios.
    if (payload?.excludeTickers !== undefined && payload?.excludeTickers !== null) {
      return await getHistoricalReturnsExcluding(context, payload);
    }

    // R-03: Read signal once, reuse across all snapshot lookups in this CF invocation.
    const signal = await readSnapshotSignal(userId);

    // HU 6.13: con la ruta antigua retirada, `forceRefresh` ya no se salta los
    // resúmenes: su único efecto era ir a la ruta antigua, y nadie lo manda.
    const retirada = consolidadosRetirados();
    const saltarResumenes = forceRefresh && !retirada;
    let motivoSinResumen = 'sin-resumen';

    // HU 6.11.3 (T2): en una moneda poco usada, el detalle del activo sale de sus
    // días en USD, antes del resumen guardado y de la ruta legacy en esa moneda.
    if (ticker && assetType) {
      const desdeUsd = await activoDesdeUsdConCache(userId, { accountId, currency, ticker, assetType, forceRefresh }, signal);
      if (desdeUsd) return desdeUsd;
    }

    // PERF-SNAP-024: Per-asset snapshot read path (antes del bypass legacy)
    // HU 6.11.6 (T4): memoria + señal → puesta al día o preparación, en `resumenDeActivoAlDia`.
    if (ticker && assetType && !saltarResumenes) {
      const inicioActivo = Date.now();
      const assetSnapshotId = buildSnapshotDocId(userId, accountId, currency, ticker, assetType);
      // OPT-FIRESTORE-002 P2-A: Await snapshot generation to avoid double-read with legacy.
      // Si la preparación falla, se avisa en el log y se sigue: a la ruta legacy
      // con el interruptor apagado, al aviso de la HU 6.13 con él encendido.
      const { snapshot: assetSnapshot, lastSnapshotUpdate, preparado, error: errorActivo } =
        await resumenDeActivoAlDia(userId, { accountId, currency, ticker, assetType }, signal);
      if (assetSnapshot) {
        // HU 6.4 (RN-6.4-J): la duración de la primera apertura del día y de las siguientes.
        console.log(preparado
          ? `[PERF] Asset snapshot generated on-demand - ${assetSnapshotId} (${Date.now() - inicioActivo} ms)`
          : `[PERF] Asset snapshot hit - ${assetSnapshotId} (${Date.now() - inicioActivo} ms)`);
        return {
          ...transformSnapshotToResponse(assetSnapshot),
          cacheHit: false,
          lastCalculated: new Date().toISOString(),
          validUntil: calculateDynamicTTL().toISOString(),
          lastSnapshotUpdate,
        };
      }
      motivoSinResumen = errorActivo
        ? `preparacion-fallida: ${errorActivo.message}`
        : 'resumen-por-activo-no-disponible';
    }

    // PERF-SNAP-007: Ticker/AssetType → ruta legacy
    // HU 6.13 (RN-6.13-A): retirada, el aviso en lugar de cifras de los consolidados.
    if (ticker || assetType) {
      if (retirada) {
        throw rendimientosNoDisponibles({
          userId, accountId, currency, ticker, assetType,
          motivo: ticker ? motivoSinResumen : 'activo-sin-ticker',
        });
      }
      return await getHistoricalReturnsLegacy(context, payload);
    }

    // HU 6.9.2 (T4): en una moneda poco usada el resumen se deriva del USD.
    // HU 6.11.1: si no se puede derivar, `resumenDerivadoConCache` lanza el
    // aviso y nunca se llega al resumen guardado en esa moneda (RN-6.11-J).
    if (!saltarResumenes) {
      const inicioDerivado = Date.now();
      const derivado = await resumenDerivadoConCache(userId, accountId, currency, signal);
      if (derivado) {
        console.log(`[PERF][getHistoricalReturns] Derived currency snapshot - ${buildSnapshotDocId(userId, accountId, currency)} (${Date.now() - inicioDerivado} ms)`);
        return {
          ...transformSnapshotToResponse(derivado),
          cacheHit: false,
          lastCalculated: new Date().toISOString(),
          validUntil: calculateDynamicTTL().toISOString(),
          lastSnapshotUpdate: signal || null,
        };
      }
    }

    // PERF-SNAP-007/021: Intentar leer snapshot (cache in-memory → Firestore)
    if (!saltarResumenes) {
      const snapshotDocId = buildSnapshotDocId(userId, accountId, currency);
      const snapshotResult = await getSnapshotWithCache(snapshotDocId, userId, signal);
      const snapshot = snapshotResult.data;

      if (snapshot) {
        const result = transformSnapshotToResponse(snapshot);

        console.log(`[queryHandlers][getHistoricalReturns] Snapshot hit - ${snapshotDocId}`);

        const now = new Date();
        return {
          ...result,
          cacheHit: false,
          lastCalculated: now.toISOString(),
          validUntil: calculateDynamicTTL().toISOString(),
          lastSnapshotUpdate: snapshotResult.lastSnapshotUpdate || null,
        };
      }

      // PERF-SNAP-009: Log estandarizado con snapshotId para monitoreo
      console.log(`[PERF] Snapshot not found for ${snapshotDocId}, generating on-demand`);

      // OPT-FIRESTORE-002 P2-A: Await snapshot generation to avoid double-read.
      // Previously fire-and-forget + legacy both read all daily docs (~27K reads/mes).
      // Now: generate snapshot → use directly. Legacy only on failure.
      try {
        // HU 6.11.2 (T2, Escenario 3): consultas simultáneas lo preparan una vez.
        const generatedSnapshot = await prepararResumenUnaVez(db, userId, accountId, currency);
        if (generatedSnapshot) {
          snapshotMemCache.set(snapshotDocId, { data: generatedSnapshot, cachedAt: Date.now() });
          const result = transformSnapshotToResponse(generatedSnapshot);
          const now = new Date();
          console.log(`[PERF] Snapshot generated on-demand - ${snapshotDocId}`);
          return {
            ...result,
            cacheHit: false,
            lastCalculated: now.toISOString(),
            validUntil: calculateDynamicTTL().toISOString(),
            lastSnapshotUpdate: snapshotResult.lastSnapshotUpdate || null,
          };
        }
      } catch (err) {
        console.warn(`[PERF] On-demand snapshot generation failed for ${snapshotDocId}: ${err.message}`);
        motivoSinResumen = `preparacion-fallida: ${err.message}`;
      }
    }

    // HU 6.13 (RN-6.13-A): con la ruta antigua retirada, el aviso. Nunca la caché
    // que guardó la ruta antigua (QA-07) ni cifras de los consolidados.
    if (retirada) {
      throw rendimientosNoDisponibles({ userId, accountId, currency, motivo: motivoSinResumen });
    }

    // Fallback a ruta legacy (V2/V1 + cache)
    return await getHistoricalReturnsLegacy(context, payload);

  } catch (error) {
    console.error(`[queryHandlers][getHistoricalReturns] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error calculando rendimientos históricos');
  }
}

/**
 * FEAT-EXCLUDE-001: Rendimientos del portafolio (o de una cuenta) excluyendo
 * tickers, ej: el portafolio sin VUAA.L para compararlo contra el S&P 500.
 *
 * Responde con la misma forma que getHistoricalReturns más un bloque
 * `carveOut` (método, tickers excluidos, cobertura). Cachea en
 * performanceCache con la exclusión en la clave; invalidatePerformanceCache
 * borra toda la colección, así que también la invalida.
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - { currency, accountId, excludeTickers, forceRefresh }
 * @returns {Promise<Object>}
 */
async function getHistoricalReturnsExcluding(context, payload) {
  const userId = context.auth.uid;
  const {
    currency = "USD",
    accountId = "overall",
    ticker = null,
    assetType = null,
    excludeTickers,
    forceRefresh = false,
  } = payload || {};

  if (ticker || assetType) {
    throw new HttpsError('invalid-argument',
      'excludeTickers aplica al portafolio o a una cuenta; no se combina con ticker/assetType');
  }

  let excludeSet;
  try {
    excludeSet = buildExcludeSet(excludeTickers);
  } catch (error) {
    throw new HttpsError('invalid-argument', error.message);
  }
  if (excludeSet.size === 0) {
    throw new HttpsError('invalid-argument', 'excludeTickers no puede estar vacío');
  }

  const exclusionHash = crypto.createHash('sha1')
    .update([...excludeSet].sort().join(','))
    .digest('hex')
    .slice(0, 12);
  // HU 6.11.3 (T4): en una moneda poco usada se calcula desde USD. La clave lleva
  // `_usd` para no servir lo que se guardó antes, hecho con lo guardado en esa moneda.
  const desdeUsd = await calculaDesdeUsd(db, userId, currency);
  const cacheKey = `${currency}_${accountId}_ex_${exclusionHash}${desdeUsd ? '_usd' : ''}`;
  const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);

  if (!forceRefresh) {
    const cacheDoc = await cacheRef.get();
    if (cacheDoc.exists) {
      const cacheData = cacheDoc.data();
      if (new Date(cacheData.validUntil) > new Date()) {
        return {
          ...cacheData.data,
          cacheHit: true,
          lastCalculated: cacheData.lastCalculated,
          validUntil: cacheData.validUntil,
        };
      }
    }
  }

  // Sin con qué convertir, el aviso de la 6.11.1, que no llega a la caché (RN-6.11-K).
  const opciones = desdeUsd
    ? {
      monedaCalculo: 'USD',
      convertirDocs: async (docs) => {
        const conversion = await diasEnMonedaDesdeUsd(docs, currency);
        if (conversion.motivo) throw monedaNoDisponible(currency, conversion.motivo, cacheKey);
        return conversion.dias;
      },
    }
    : undefined;
  const result = await getCarveOutHistoricalReturns({ db, userId, currency, accountId, excludeSet }, undefined, opciones);

  const now = new Date();
  const validUntil = calculateDynamicTTL();
  try {
    await cacheRef.set({
      data: result,
      lastCalculated: now.toISOString(),
      validUntil: validUntil.toISOString(),
    });
  } catch (cacheWriteError) {
    console.error(`[queryHandlers][getHistoricalReturnsExcluding] Error guardando cache:`, cacheWriteError);
  }

  return {
    ...result,
    cacheHit: false,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString(),
  };
}

/**
 * FEAT-EXCLUDE-001: Los endpoints que no soportan exclusión la rechazan en
 * lugar de ignorarla, para no devolver el portafolio completo como si fuera
 * el filtrado.
 */
function rejectExcludeTickers(payload, endpoint) {
  if (payload?.excludeTickers !== undefined && payload?.excludeTickers !== null) {
    throw new HttpsError('invalid-argument',
      `excludeTickers no está soportado en ${endpoint}; usa getHistoricalReturns`);
  }
}

/**
 * Un ticker sin su assetType no identifica al activo (la llave de
 * assetPerformance es `${ticker}_${assetType}`) y el cálculo caía a la rama
 * top-level: devolvía el rendimiento de todo el portafolio como si fuera el
 * del ticker. Mismo criterio que rejectExcludeTickers: rechazar, no adivinar.
 */
function rejectTickerWithoutAssetType(payload, endpoint) {
  if (payload?.ticker && !payload?.assetType) {
    throw new HttpsError('invalid-argument',
      `${endpoint} necesita assetType cuando se pide un ticker`);
  }
}

/**
 * Ruta legacy de rendimientos históricos (V2 consolidados + cache Firestore).
 * Usada como fallback cuando no hay snapshot, para ticker/assetType, o forceRefresh.
 *
 * HU 6.13: solo con `RETIRE_CONSOLIDATED_PERIODS=false` (volver atrás, RN-6-H).
 * Se conserva para eso; antes de apagar se rehacen los consolidados (RN-6.13-E).
 */
async function getHistoricalReturnsLegacy(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { 
    currency = "USD", 
    accountId = "overall", 
    ticker = null, 
    assetType = null, 
    forceRefresh = false 
  } = payload || {};

  const cacheKey = `${currency}_${accountId}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;

  // 1. Verificar cache (si no forceRefresh)
  if (!forceRefresh) {
    const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
    const cacheDoc = await cacheRef.get();

    if (cacheDoc.exists) {
      const cacheData = cacheDoc.data();
      const validUntil = new Date(cacheData.validUntil);

      if (validUntil > new Date()) {
        console.log(`[queryHandlers][getHistoricalReturnsLegacy] Cache hit`);
        return {
          ...cacheData.data,
          cacheHit: true,
          lastCalculated: cacheData.lastCalculated,
          validUntil: cacheData.validUntil
        };
      }
    }
  }

  const result = await getHistoricalReturnsV2(userId, {
    currency,
    accountId,
    ticker,
    assetType,
    forceRefresh,
    fallbackToV1: true
  });

  // 2. Guardar en cache
  const now = new Date();
  const validUntil = calculateDynamicTTL();

  const cacheData = {
    data: result,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };

  try {
    const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
    await cacheRef.set(cacheData);
  } catch (cacheWriteError) {
    console.error(`[queryHandlers][getHistoricalReturnsLegacy] Error guardando cache:`, cacheWriteError);
  }

  const version = result._metadata?.version || 'v1';
  console.log(`[queryHandlers][getHistoricalReturnsLegacy] Éxito - version: ${version}`);
  
  return {
    ...result,
    cacheHit: false,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };
}

/**
 * Obtiene rendimientos históricos multi-cuenta
 * 
 * SCALE-CF-001: Implementación completa de agregación multi-cuenta
 * Migrada desde historicalReturnsService.getMultiAccountHistoricalReturns
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>} Rendimientos agregados de múltiples cuentas
 */
async function getMultiAccountHistoricalReturns(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { 
    accountIds = [], 
    currency = "USD", 
    ticker = null, 
    assetType = null, 
    forceRefresh = false 
  } = payload || {};

  console.log(`[queryHandlers][getMultiAccountHistoricalReturns] userId: ${userId}, accounts: ${accountIds.length}`);

  rejectExcludeTickers(payload, 'getMultiAccountHistoricalReturns');
  rejectTickerWithoutAssetType(payload, 'getMultiAccountHistoricalReturns');

  try {
    // Validación de parámetros
    if (!Array.isArray(accountIds) || accountIds.length === 0) {
      throw new HttpsError('invalid-argument', 'Debes proporcionar al menos una cuenta en accountIds');
    }

    // HU 6.11.2 (T1): un solo alcance sale de los resúmenes, no de recorrer la historia.
    const unAlcance = { currency, ticker, assetType, forceRefresh };

    // "overall" o "all": el portafolio completo (lo pide el Copiloto)
    if (accountIds.includes("overall") || accountIds.includes("all")) {
      console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Un solo alcance (overall)`);
      return await rendimientoDeUnAlcance(context, 'overall', unAlcance);
    }

    // Una sola cuenta
    if (accountIds.length === 1) {
      console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Un solo alcance (cuenta única)`);
      return await rendimientoDeUnAlcance(context, accountIds[0], unAlcance);
    }

    // Verificar si se seleccionaron TODAS las cuentas del usuario
    const userAccountsSnapshot = await db.collection('portfolioAccounts')
      .where('userId', '==', userId)
      .where('isActive', '==', true)
      .get();
    
    const userAccountIds = userAccountsSnapshot.docs.map(doc => doc.id);
    const sortedRequestedIds = [...accountIds].sort();
    const sortedUserIds = [...userAccountIds].sort();
    
    if (sortedRequestedIds.length === sortedUserIds.length && 
        sortedRequestedIds.every((id, i) => id === sortedUserIds[i])) {
      console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Todas las cuentas, usando overall`);
      return await rendimientoDeUnAlcance(context, 'overall', unAlcance);
    }

    // ============================================================================
    // PERF-SNAP-008: Snapshot path — N reads en vez de N full collection scans
    // Uses getSnapshotWithCache for in-memory cache + lastSnapshotUpdate invalidation
    // R-03: Read signal once, reuse across all N snapshot lookups.
    // ============================================================================
    if (!forceRefresh && !ticker && !assetType) {
      const signal = await readSnapshotSignal(userId);
      // HU 6.9.2 (T4): en una moneda poco usada cada cuenta se deriva del USD.
      // HU 6.11.1: si una no se puede derivar, el aviso corta la consulta entera,
      // antes del resumen guardado y de la ruta legacy en esa moneda (RN-6.11-J).
      const snapshotPromises = accountIds.map(async (accountId) => {
        const derivado = await resumenDerivadoConCache(userId, accountId, currency, signal);
        return derivado
          ? { data: derivado, lastSnapshotUpdate: signal }
          : getSnapshotWithCache(buildSnapshotDocId(userId, accountId, currency), userId, signal);
      });
      const snapshotResults = await Promise.all(snapshotPromises);

      if (snapshotResults.every(r => r.data !== null)) {
        const snapshots = snapshotResults.map(r => r.data);
        const result = aggregateSnapshotTimelines(snapshots, currency);

        // FIX-SNAP-008c: Los snapshots individuales tienen validDocsCountByPeriod correcto
        // (calculado por V2 chainFactorsForPeriods con docsCount de consolidated periods).
        // Pero aggregateSnapshotTimelines → calculateHistoricalReturns recalcula desde
        // "fake docs" del timeline, que tiene 1 punto por período consolidado = counts bajísimos.
        // Fix: usar el MAX de validDocsCountByPeriod de los snapshots individuales.
        const aggregatedValidDocs = snapshots.reduce((acc, snap) => {
          const vd = snap.validDocsCountByPeriod || {};
          return {
            ytd: Math.max(acc.ytd, vd.ytd || 0),
            oneMonth: Math.max(acc.oneMonth, vd.oneMonth || 0),
            threeMonths: Math.max(acc.threeMonths, vd.threeMonths || 0),
            sixMonths: Math.max(acc.sixMonths, vd.sixMonths || 0),
            oneYear: Math.max(acc.oneYear, vd.oneYear || 0),
            twoYears: Math.max(acc.twoYears, vd.twoYears || 0),
            fiveYears: Math.max(acc.fiveYears, vd.fiveYears || 0),
          };
        }, { ytd: 0, oneMonth: 0, threeMonths: 0, sixMonths: 0, oneYear: 0, twoYears: 0, fiveYears: 0 });

        // FIX-SNAP-008c: Los has*Data flags también necesitan reflejar los datos reales
        const aggregatedReturns = { ...result.returns };
        aggregatedReturns.hasYtdData = aggregatedValidDocs.ytd >= 1;
        aggregatedReturns.hasOneMonthData = aggregatedValidDocs.oneMonth >= 5;
        aggregatedReturns.hasThreeMonthData = aggregatedValidDocs.threeMonths >= 15;
        aggregatedReturns.hasSixMonthData = aggregatedValidDocs.sixMonths >= 30;
        aggregatedReturns.hasOneYearData = aggregatedValidDocs.oneYear >= 60;
        aggregatedReturns.hasTwoYearData = aggregatedValidDocs.twoYears >= 120;
        aggregatedReturns.hasFiveYearData = aggregatedValidDocs.fiveYears >= 250;

        console.log(`[queryHandlers][getMultiAccountHistoricalReturns] Snapshot path - ${accountIds.length} snapshots agregados, validDocs: ytd=${aggregatedValidDocs.ytd}, 1M=${aggregatedValidDocs.oneMonth}, 3M=${aggregatedValidDocs.threeMonths}, 6M=${aggregatedValidDocs.sixMonths}, 1Y=${aggregatedValidDocs.oneYear}`);

        // FIX-SNAP-008b: Propagar lastSnapshotUpdate para invalidación de cache frontend
        const lastSnapshotUpdate = snapshotResults[0]?.lastSnapshotUpdate || null;

        // Aggregate monthlyCompound from individual snapshots (P&L fields)
        const aggregatedMonthlyCompound = aggregateMonthlyCompounds(snapshots);

        const now = new Date();
        return {
          ...result,
          returns: aggregatedReturns,
          validDocsCountByPeriod: aggregatedValidDocs,
          monthlyCompoundData: aggregatedMonthlyCompound,
          cacheHit: false,
          lastCalculated: now.toISOString(),
          validUntil: calculateDynamicTTL().toISOString(),
          lastSnapshotUpdate,
          _metadata: { ...(result._metadata || {}), version: 'snapshot-multi' },
        };
      }

      // PERF-SNAP-009: Log estandarizado con IDs de snapshots faltantes
      const missingIds = snapshotResults
        .map((r, i) => r.data !== null ? null : buildSnapshotDocId(userId, accountIds[i], currency))
        .filter(Boolean);
      console.log(`[PERF] Snapshot not found for [${missingIds.join(', ')}], falling back to legacy`);

      // PERF-SNAP-009: Generación on-demand fire-and-forget solo para cuentas sin snapshot
      const missingAccountIds = snapshotResults
        .map((r, i) => r.data !== null ? null : accountIds[i])
        .filter(Boolean);
      missingAccountIds.forEach(accountId => {
        generatePerformanceSnapshot(db, userId, accountId, currency).catch(err =>
          console.warn(`[PERF] On-demand snapshot generation failed for ${buildSnapshotDocId(userId, accountId, currency)}: ${err.message}`)
        );
      });
    }

    // HU 6.11.3 (T3): el detalle de un activo con varias cuentas en una moneda
    // poco usada no recorre lo guardado en esa moneda.
    if (ticker && assetType && await calculaDesdeUsd(db, userId, currency)) {
      // HU 6.11.6: la señal decide si el resumen en USD de cada cuenta sigue vigente.
      const senal = await readSnapshotSignal(userId);
      return await activoVariasCuentasDesdeUsd(userId, { accountIds, currency, ticker, assetType, forceRefresh }, senal);
    }

    // Fallback a ruta legacy (full collection scan + cache)
    return await getMultiAccountHistoricalReturnsLegacy(context, payload);

  } catch (error) {
    console.error(`[queryHandlers][getMultiAccountHistoricalReturns] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error calculando rendimientos multi-cuenta');
  }
}

/**
 * PERF-SNAP-008: Ruta legacy de rendimientos históricos multi-cuenta.
 * Full collection scan por cada cuenta + agregación en memoria + cache Firestore.
 * Usada como fallback cuando no hay snapshots, para ticker/assetType, o forceRefresh.
 */
async function getMultiAccountHistoricalReturnsLegacy(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const {
    accountIds = [],
    currency = "USD",
    ticker = null,
    assetType = null,
    forceRefresh = false
  } = payload || {};

  // ============================================================================
  // MULTI-CUENTA REAL: Agregar datos de múltiples cuentas
  // ============================================================================
    
  // Generar clave de cache
  const sortedIds = [...accountIds].sort().join('_');
  const cacheKey = `multi_${currency}_${sortedIds}${ticker ? `_${ticker}` : ''}${assetType ? `_${assetType}` : ''}`;

  console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Multi-cuenta, cache key: ${cacheKey}`);

  // Verificar cache (si no forceRefresh)
  if (!forceRefresh) {
    try {
      const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
      const cacheDoc = await cacheRef.get();

      if (cacheDoc.exists) {
        const cache = cacheDoc.data();
        const validUntil = new Date(cache.validUntil);

        if (validUntil > new Date()) {
          console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Cache HIT`);
          return {
            ...cache.data,
            cacheHit: true,
            lastCalculated: cache.lastCalculated,
            validUntil: cache.validUntil
          };
        }
      }
    } catch (cacheError) {
      console.warn(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Error leyendo cache:`, cacheError.message);
    }
  }

  console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Cache MISS - Agregando ${accountIds.length} cuentas`);

  // Leer datos de cada cuenta en paralelo
  const accountDataPromises = accountIds.map(accountId => 
    db.collection(`portfolioPerformance/${userId}/accounts/${accountId}/dates`)
      .orderBy("date", "asc")
      .get()
  );
  
  const accountSnapshots = await Promise.all(accountDataPromises);

  const totalDocs = accountSnapshots.reduce((sum, snap) => sum + snap.size, 0);
  const docsDetail = accountSnapshots.map((snap, i) => `${accountIds[i].substring(0, 8)}...:${snap.size}`).join(', ');
  console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Docs por cuenta: [${docsDetail}], Total: ${totalDocs}`);

  if (totalDocs === 0) {
    console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Sin datos de performance`);
    return {
      returns: {
        ytdReturn: 0, oneMonthReturn: 0, threeMonthReturn: 0, sixMonthReturn: 0,
        oneYearReturn: 0, twoYearReturn: 0, fiveYearReturn: 0,
        hasYtdData: false, hasOneMonthData: false, hasThreeMonthData: false,
        hasSixMonthData: false, hasOneYearData: false, hasTwoYearData: false,
        hasFiveYearData: false
      },
      validDocsCountByPeriod: {
        ytd: 0, oneMonth: 0, threeMonths: 0, sixMonths: 0,
        oneYear: 0, twoYears: 0, fiveYears: 0
      },
      totalValueData: {
        dates: [], values: [], percentChanges: [], overallPercentChange: 0
      },
      performanceByYear: {},
      availableYears: [],
      startDate: "",
      monthlyCompoundData: {},
      cacheHit: false,
      lastCalculated: new Date().toISOString()
    };
  }

  const aggregatedDocs = agregarDiasDeCuentas(accountSnapshots.map(snapshot => snapshot.docs));

  console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Procesando ${aggregatedDocs.length} fechas agregadas`);

  const result = calculateHistoricalReturns(aggregatedDocs, currency, ticker, assetType);
  return guardarYResponderMultiLegacy(userId, cacheKey, result);
}

/**
 * Agrega los días de varias cuentas por fecha y moneda: suma importes y activos,
 * y pondera el cambio diario por el valor previo de cada cuenta (TWR multi-cuenta).
 *
 * HU 6.11.3 (T3): extraída de `getMultiAccountHistoricalReturnsLegacy` sin cambiar
 * lo que hace, para que el detalle de un activo con varias cuentas en una moneda
 * poco usada agregue sus días en USD convertidos con la misma cuenta.
 *
 * @param {Array<Array>} diasPorCuenta - Por cuenta, sus días (snapshots o planos) en orden de fecha
 * @returns {Array<{data: Function}>} Documentos agregados, en orden de fecha
 */
function agregarDiasDeCuentas(diasPorCuenta) {
  const aggregatedByDate = new Map();

  diasPorCuenta.forEach(dias => {
    dias.forEach(doc => {
      const data = typeof doc.data === 'function' ? doc.data() : doc;
      const date = data.date;
      
      if (!aggregatedByDate.has(date)) {
        aggregatedByDate.set(date, {
          date,
          currencies: {}
        });
      }
      
      const existing = aggregatedByDate.get(date);
      
      Object.keys(data).forEach(key => {
        if (key === 'date') return;
        
        const currencyCode = key;
        const currencyData = data[currencyCode];
        
        if (!currencyData || typeof currencyData !== 'object') return;
        
        if (!existing.currencies[currencyCode]) {
          existing.currencies[currencyCode] = {
            totalInvestment: 0,
            totalValue: 0,
            totalCashFlow: 0,
            unrealizedProfitAndLoss: 0,
            doneProfitAndLoss: 0,
            assetPerformance: {},
            _accountContributions: []
          };
        }
        
        existing.currencies[currencyCode]._accountContributions.push({
          totalValue: currencyData.totalValue || 0,
          adjustedDailyChangePercentage: currencyData.adjustedDailyChangePercentage || 0,
          rawDailyChangePercentage: currencyData.rawDailyChangePercentage || currencyData.dailyChangePercentage || 0
        });
        
        existing.currencies[currencyCode].totalInvestment += currencyData.totalInvestment || 0;
        existing.currencies[currencyCode].totalValue += currencyData.totalValue || 0;
        existing.currencies[currencyCode].totalCashFlow += currencyData.totalCashFlow || 0;
        existing.currencies[currencyCode].unrealizedProfitAndLoss += currencyData.unrealizedProfitAndLoss || 0;
        existing.currencies[currencyCode].doneProfitAndLoss += currencyData.doneProfitAndLoss || 0;
        
        if (currencyData.assetPerformance) {
          Object.entries(currencyData.assetPerformance).forEach(([assetKey, assetData]) => {
            if (!existing.currencies[currencyCode].assetPerformance[assetKey]) {
              existing.currencies[currencyCode].assetPerformance[assetKey] = {
                totalInvestment: 0,
                totalValue: 0,
                totalCashFlow: 0,
                units: 0,
                unrealizedProfitAndLoss: 0,
                doneProfitAndLoss: 0
              };
            }
            
            const existingAsset = existing.currencies[currencyCode].assetPerformance[assetKey];
            existingAsset.totalInvestment += assetData.totalInvestment || 0;
            existingAsset.totalValue += assetData.totalValue || 0;
            existingAsset.totalCashFlow += assetData.totalCashFlow || 0;
            existingAsset.units += assetData.units || 0;
            existingAsset.unrealizedProfitAndLoss += assetData.unrealizedProfitAndLoss || 0;
            existingAsset.doneProfitAndLoss += assetData.doneProfitAndLoss || 0;
          });
        }
      });
    });
  });

  const sortedDates = Array.from(aggregatedByDate.keys()).sort();
  
  sortedDates.forEach(date => {
    const dateData = aggregatedByDate.get(date);
    
    Object.keys(dateData.currencies).forEach(currencyCode => {
      const c = dateData.currencies[currencyCode];
      
      c.totalROI = c.totalInvestment > 0 
        ? ((c.totalValue - c.totalInvestment) / c.totalInvestment) * 100 
        : 0;
      
      const contributions = c._accountContributions || [];
      
      const contributionsWithPreValue = contributions.map(acc => {
        const change = acc.adjustedDailyChangePercentage || 0;
        const currentValue = acc.totalValue || 0;
        const preChangeValue = change !== 0 ? currentValue / (1 + change / 100) : currentValue;
        return { ...acc, preChangeValue };
      });
      
      const totalWeight = contributionsWithPreValue.reduce((sum, acc) => sum + acc.preChangeValue, 0);
      
      if (totalWeight > 0 && contributionsWithPreValue.length > 0) {
        const weightedAdjustedChange = contributionsWithPreValue.reduce((sum, acc) => {
          const weight = acc.preChangeValue / totalWeight;
          return sum + (acc.adjustedDailyChangePercentage || 0) * weight;
        }, 0);
        
        const weightedRawChange = contributionsWithPreValue.reduce((sum, acc) => {
          const weight = acc.preChangeValue / totalWeight;
          return sum + (acc.rawDailyChangePercentage || 0) * weight;
        }, 0);
        
        c.dailyChangePercentage = weightedRawChange;
        c.rawDailyChangePercentage = weightedRawChange;
        c.adjustedDailyChangePercentage = weightedAdjustedChange;
      } else {
        c.dailyChangePercentage = 0;
        c.rawDailyChangePercentage = 0;
        c.adjustedDailyChangePercentage = 0;
      }
      
      delete c._accountContributions;
      
      Object.values(c.assetPerformance).forEach(assetData => {
        assetData.totalROI = assetData.totalInvestment > 0
          ? ((assetData.totalValue - assetData.totalInvestment) / assetData.totalInvestment) * 100
          : 0;
      });
    });
  });

  return sortedDates.map(date => {
    const dateData = aggregatedByDate.get(date);
    return {
      data: () => ({
        date: dateData.date,
        ...dateData.currencies
      })
    };
  });
}

/** Guarda en la caché de Firestore la respuesta de la ruta legacy multi-cuenta y la devuelve. */
async function guardarYResponderMultiLegacy(userId, cacheKey, result) {
  const now = new Date();
  const validUntil = calculateDynamicTTL();

  const cacheData = {
    data: result,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };

  try {
    const cacheRef = db.doc(`userData/${userId}/performanceCache/${cacheKey}`);
    await cacheRef.set(cacheData);
    console.log(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Cache guardado`);
  } catch (cacheWriteError) {
    console.error(`[queryHandlers][getMultiAccountHistoricalReturnsLegacy] Error guardando cache:`, cacheWriteError.message);
  }

  return {
    ...result,
    cacheHit: false,
    lastCalculated: now.toISOString(),
    validUntil: validUntil.toISOString()
  };
}

/**
 * Obtiene datos históricos de un índice de mercado
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de consulta
 * @returns {Promise<Object>} Datos del índice
 */
async function getIndexHistory(context, payload) {
  const { auth } = context;
  const { code, range } = payload || {};

  console.log(`[queryHandlers][getIndexHistory] userId: ${auth.uid}, code: ${code}, range: ${range}`);

  // Validación de parámetros
  if (!code || typeof code !== 'string') {
    throw new HttpsError("invalid-argument", "El parámetro 'code' es requerido");
  }

  if (!range || typeof range !== 'string') {
    throw new HttpsError("invalid-argument", "El parámetro 'range' es requerido");
  }

  if (!VALID_INDEX_RANGES.includes(range)) {
    throw new HttpsError(
      "invalid-argument", 
      `El parámetro 'range' debe ser uno de: ${VALID_INDEX_RANGES.join(", ")}`
    );
  }

  const cacheKey = `${code}_${range}`;
  const cacheRef = db.collection("indexCache").doc(cacheKey);

  try {
    // 1. Intentar obtener del cache
    const cacheDoc = await cacheRef.get();
    
    if (cacheDoc.exists) {
      const cacheData = cacheDoc.data();
      const cacheAge = Date.now() - (cacheData.lastUpdated || 0);

      // FIX-INDEX-INTRADAY: Use shorter TTL if cached data doesn't include today
      const todayStr = new Date().toISOString().split('T')[0];
      const lastCachedDate = (cacheData.chartData || []).length > 0
        ? cacheData.chartData[cacheData.chartData.length - 1].date
        : '';
      const effectiveTTL = lastCachedDate === todayStr ? INDEX_CACHE_TTL_MS : INDEX_INTRADAY_CACHE_TTL_MS;

      if (cacheAge < effectiveTTL) {
        console.log(`[queryHandlers][getIndexHistory] Cache hit para ${cacheKey} (TTL=${effectiveTTL/1000}s)`);
        return {
          chartData: cacheData.chartData || [],
          overallChange: cacheData.overallChange || 0,
          latestValue: cacheData.latestValue || 0,
          indexInfo: cacheData.indexInfo || { name: code, region: "Unknown", code },
          cacheHit: true,
          cacheTimestamp: cacheData.lastUpdated,
        };
      }
    }

    // 2. Cache miss - calcular datos
    console.log(`[queryHandlers][getIndexHistory] Cache miss para ${cacheKey}, calculando...`);
    const result = await calculateIndexData(code, range);

    // 3. Guardar en cache
    await cacheRef.set({
      ...result,
      lastUpdated: Date.now(),
    });

    console.log(`[queryHandlers][getIndexHistory] Éxito - ${cacheKey}`);

    return {
      ...result,
      cacheHit: false,
      cacheTimestamp: Date.now(),
    };

  } catch (error) {
    console.error(`[queryHandlers][getIndexHistory] Error:`, error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError('internal', 'Error obteniendo datos del índice');
  }
}

/**
 * Obtiene distribución del portafolio (sectores, países, holdings)
 * Migrado desde función inline en index.js
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Opciones de distribución
 * @returns {Promise<Object>} Distribución del portafolio
 */
async function getPortfolioDistribution(context, payload) {
  const { auth } = context;
  const userId = auth.uid;
  const { accountIds, accountId, currency, includeHoldings, forceRefresh, holdingsLimit } = payload || {};

  // HU 6.15 (RN-6.15-A): la carga ligera pide las N posiciones de mayor peso.
  // Sin `holdingsLimit` la respuesta es la de siempre (lista completa).
  if (holdingsLimit !== undefined && holdingsLimit !== null && !esLimiteDePosicionesValido(holdingsLimit)) {
    throw new HttpsError(
      'invalid-argument',
      `holdingsLimit debe ser un entero entre ${HOLDINGS_LIMIT_MIN} y ${HOLDINGS_LIMIT_MAX}`
    );
  }

  console.log(`[queryHandlers][getPortfolioDistribution] userId: ${userId}, forceRefresh: ${forceRefresh}`);

  try {
    const result = await portfolioDistributionService.getPortfolioDistribution(
      userId,
      { 
        accountIds, 
        accountId, 
        currency: currency || 'USD', 
        includeHoldings: includeHoldings ?? true,
        forceRefresh: forceRefresh ?? false,
        holdingsLimit: holdingsLimit ?? undefined
      }
    );

    console.log(`[queryHandlers][getPortfolioDistribution] Éxito - sectors: ${result.sectors?.length || 0}`);

    return result;
  } catch (error) {
    console.error(`[queryHandlers][getPortfolioDistribution] Error:`, error);
    throw new HttpsError('internal', 'Error calculando distribución del portafolio');
  }
}

/** HU 6.15: la carga ligera cubre al menos la primera página de la tabla (10 filas). */
const HOLDINGS_LIMIT_MIN = 10;
/** HU 6.15: tope para que nadie pida "ligera" una lista que ya no lo es. */
const HOLDINGS_LIMIT_MAX = 1000;

function esLimiteDePosicionesValido(valor) {
  return Number.isInteger(valor) && valor >= HOLDINGS_LIMIT_MIN && valor <= HOLDINGS_LIMIT_MAX;
}

/**
 * HU 6.15 (T3, RN-6.15-C, RN-6.15-D) — El resto de la lista de posiciones de
 * la distribución, solo del usuario de la sesión.
 *
 * El navegador lo pide una vez, cuando el inversor usa la tabla, con las mismas
 * cuentas y moneda de la carga ligera, la instantánea que recibió
 * (`metadata.calculatedAt`) y desde qué posición (`offset`). Sale de la caché
 * del servidor sin lecturas; si no puede, se recalcula y vuelve completa con
 * `offset: 0` (D4). El log dice de dónde salió y por qué (RN-6-I).
 *
 * @param {Object} context - Contexto con `auth`
 * @param {{accountIds?: string[], accountId?: string, currency?: string,
 *   offset: number, snapshot: string}} payload
 * @returns {Promise<Object>} `{ holdings, offset, holdingsTotal, snapshot, origen, motivo? }`
 * @see platform-docs/stories/6.15-distribucion-portafolio-carga-ligera/refinamiento.md (T3, D4)
 */
async function getPortfolioDistributionHoldings(context, payload) {
  const userId = context.auth.uid;
  const { accountIds, accountId, currency, offset, snapshot } = payload || {};

  if (!Number.isInteger(offset) || offset < 0) {
    throw new HttpsError('invalid-argument', 'offset debe ser un entero mayor o igual que 0');
  }
  if (typeof snapshot !== 'string' || snapshot.trim() === '') {
    throw new HttpsError('invalid-argument', 'Se requiere la instantánea de la distribución (snapshot)');
  }

  try {
    const resto = await portfolioDistributionService.obtenerRestoDePosiciones(
      userId,
      { accountIds, accountId, currency: currency || 'USD' },
      { offset, snapshot }
    );
    console.log(
      `[getPortfolioDistributionHoldings] ${userId}: ${resto.origen}` +
      `${resto.motivo ? ` (${resto.motivo})` : ''}, ${resto.holdings.length} posiciones` +
      ` desde ${resto.offset} de ${resto.holdingsTotal}`
    );
    return resto;
  } catch (error) {
    console.error('[getPortfolioDistributionHoldings] Error:', error);
    throw new HttpsError('internal', 'Error obteniendo la lista completa de posiciones');
  }
}

/**
 * Obtiene sectores disponibles en el sistema
 * Migrado desde función inline en index.js
 * 
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - Sin uso
 * @returns {Promise<{sectors: Array}>}
 */
async function getAvailableSectors(context, payload) {
  const { auth } = context;

  console.log(`[queryHandlers][getAvailableSectors] userId: ${auth.uid}`);

  try {
    const sectors = await portfolioDistributionService.getAvailableSectors();
    
    console.log(`[queryHandlers][getAvailableSectors] Éxito - ${sectors.length} sectores`);

    return { sectors };
  } catch (error) {
    console.error(`[queryHandlers][getAvailableSectors] Error:`, error);
    throw new HttpsError('internal', 'Error obteniendo sectores disponibles');
  }
}

/**
 * HU 2.1 — Tipo de cambio de una fecha concreta entre dos divisas.
 *
 * Lo consume el diálogo de movimiento de efectivo para proponer la tasa de la
 * fecha del ingreso, no la de hoy. La tasa se expresa como el usuario la
 * entiende: cuántas unidades de `toCurrency` cuesta 1 unidad de `fromCurrency`.
 *
 * Cuando no hay tasa, devuelve `rate: null` en lugar de lanzar: la ausencia es
 * una respuesta legítima que la interfaz traduce en "escríbela tú" (RN-05).
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - { date, fromCurrency, toCurrency? }
 * @returns {Promise<{rate: number|null, rateDate: string|null, isExactDate: boolean, source: string|null, fromCurrency: string, toCurrency: string}>}
 */
async function getHistoricalExchangeRate(context, payload) {
  const { auth } = context;
  const { date, fromCurrency } = payload || {};

  console.log(`[queryHandlers][getHistoricalExchangeRate] userId: ${auth.uid}, date: ${date}, from: ${fromCurrency}`);

  if (!date || !fromCurrency) {
    throw new HttpsError('invalid-argument', 'date y fromCurrency son requeridos');
  }

  try {
    // La moneda de referencia se resuelve en servidor: es una preferencia del
    // usuario, no un dato que el cliente deba poder suplantar.
    const toCurrency = payload.toCurrency || await getUserReferenceCurrency(auth.uid);

    const result = await historicalRateService.getCrossRate(fromCurrency, toCurrency, date);

    if (result === null) {
      console.warn(`[queryHandlers][getHistoricalExchangeRate] Sin tasa - ${fromCurrency}/${toCurrency} en ${date}`);
      return {
        rate: null,
        rateDate: null,
        isExactDate: false,
        source: null,
        fromCurrency,
        toCurrency,
      };
    }

    console.log(`[queryHandlers][getHistoricalExchangeRate] Éxito - ${result.rate} (${result.source})`);

    return {
      rate: result.rate,
      rateDate: result.rateDate,
      isExactDate: result.rateDate === date,
      source: result.source,
      fromCurrency,
      toCurrency,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error(`[queryHandlers][getHistoricalExchangeRate] Error:`, error);
    throw new HttpsError('internal', 'Error obteniendo el tipo de cambio de la fecha');
  }
}

/**
 * HU 2.3 — Estimación de la base de costo de un saldo sin historia conocida.
 *
 * Cuando el usuario va a comprar pagando con un saldo cuyo tipo de cambio de
 * adquisición el sistema no pudo determinar, se le pide **una sola vez** que lo
 * confirme (RN-12). Para no obligarle a inventar nada, se le propone una
 * estimación: la tasa de mercado de la fecha de su **primer movimiento** en esa
 * cuenta y esa divisa, que es lo más cercano a "cuándo llegó ese dinero" que el
 * sistema puede saber sin el libro mayor de 2.6.
 *
 * Si no hay ningún movimiento registrado, se cae a la fecha que indique el
 * cliente (la de la compra que se está registrando).
 *
 * Devuelve `estimatedRate: null` en lugar de lanzar cuando no hay tasa: la
 * ausencia es una respuesta legítima que la interfaz convierte en "escríbela
 * tú" (RN-13).
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - { portfolioAccountId, currency, fallbackDate? }
 * @returns {Promise<{estimatedRate: number|null, rateDate: string|null, firstMovementDate: string|null, currency: string, referenceCurrency: string}>}
 */
async function getBalanceCostBasisEstimate(context, payload) {
  const { auth } = context;
  const { portfolioAccountId, currency, fallbackDate } = payload || {};

  console.log(`[queryHandlers][getBalanceCostBasisEstimate] userId: ${auth.uid}, account: ${portfolioAccountId}, currency: ${currency}`);

  if (!portfolioAccountId || !currency) {
    throw new HttpsError('invalid-argument', 'portfolioAccountId y currency son requeridos');
  }

  try {
    // Ownership: la cuenta tiene que ser del usuario que pregunta.
    const accountDoc = await db.collection('portfolioAccounts').doc(portfolioAccountId).get();

    if (!accountDoc.exists || accountDoc.data()?.userId !== auth.uid) {
      throw new HttpsError('permission-denied', 'No tienes acceso a esta cuenta');
    }

    const referenceCurrency = await getUserReferenceCurrency(auth.uid);

    // Sin exposición cambiaria no hay nada que estimar (RN-14).
    if (currency === referenceCurrency) {
      return {
        estimatedRate: 1,
        rateDate: null,
        firstMovementDate: null,
        currency,
        referenceCurrency,
      };
    }

    // Primer movimiento de efectivo de esa cuenta que tocó esa divisa. Se
    // reutiliza EXACTAMENTE la query que ya usa el historial de saldo —
    // `portfolioAccountId` + `assetType == 'cash'`— y la divisa se filtra en
    // memoria, para no exigir un índice compuesto nuevo. El conjunto es el
    // efectivo de una sola cuenta: cabe de sobra.
    //
    // HU 2.2: una conversión entró a esta divisa aunque su campo `currency` sea
    // la de origen, así que cuenta como movimiento de las dos puntas.
    let firstMovementDate = null;

    const snapshot = await db.collection('transactions')
      .where('portfolioAccountId', '==', portfolioAccountId)
      .where('assetType', '==', 'cash')
      .get();

    snapshot.docs.forEach((doc) => {
      const tx = doc.data() || {};

      if (tx.currency !== currency && tx.toCurrency !== currency) return;
      if (!tx.date) return;

      const txDate = String(tx.date).substring(0, 10);

      if (firstMovementDate === null || txDate < firstMovementDate) {
        firstMovementDate = txDate;
      }
    });

    const rateDateRequested = firstMovementDate
      || (fallbackDate ? String(fallbackDate).substring(0, 10) : null);

    if (!rateDateRequested) {
      return {
        estimatedRate: null,
        rateDate: null,
        firstMovementDate,
        currency,
        referenceCurrency,
      };
    }

    const result = await historicalRateService.getCrossRate(currency, referenceCurrency, rateDateRequested);

    if (result === null) {
      console.warn(`[queryHandlers][getBalanceCostBasisEstimate] Sin tasa - ${currency}/${referenceCurrency} en ${rateDateRequested}`);
      return {
        estimatedRate: null,
        rateDate: null,
        firstMovementDate,
        currency,
        referenceCurrency,
      };
    }

    console.log(`[queryHandlers][getBalanceCostBasisEstimate] Éxito - ${result.rate} (${result.source})`);

    return {
      estimatedRate: result.rate,
      rateDate: result.rateDate,
      firstMovementDate,
      currency,
      referenceCurrency,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error(`[queryHandlers][getBalanceCostBasisEstimate] Error:`, error);
    throw new HttpsError('internal', 'Error estimando la base de costo del saldo');
  }
}

/**
 * HU 2.6 — El libro mayor de un saldo (AC-1) y su reconciliación (AC-3).
 *
 * Devuelve los movimientos que llevaron a ese saldo hasta donde está, de más
 * reciente a más antiguo, con la tasa de cada uno y el saldo y la tasa promedio
 * acumulados después de él. La aritmética vive en `balanceLedger`, que es el
 * mismo módulo con el que la migración replaya el histórico (D1).
 *
 * **Persiste el veredicto de conciliación** en la propia cuenta (D4). AC-3 pide
 * la marca de "no conciliado" al entrar a la gestión de cuentas, y recalcularla
 * en cada visita obligaría a leer todas las transacciones de todas las cuentas.
 * Como esta historia cierra los caminos que movían el saldo sin asiento, la
 * deriva sólo puede venir de datos anteriores: basta con anotar el veredicto
 * cada vez que el libro mayor se replaya de todas formas.
 *
 * La lectura reutiliza el índice `portfolioAccountId ASC + date DESC` que ya
 * existe y filtra la divisa en memoria — la misma decisión que tomaron 2.3 y
 * 2.4 para no exigir un índice compuesto nuevo. El conjunto es el de una sola
 * cuenta.
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - { portfolioAccountId, currency }
 * @returns {Promise<{rows: Array<Object>, reconciliation: Object,
 *   hasFxExposure: boolean, currency: string, referenceCurrency: string}>}
 */
async function getBalanceLedger(context, payload) {
  const { auth } = context;
  const { portfolioAccountId, currency, limit } = payload || {};

  console.log(`[queryHandlers][getBalanceLedger] userId: ${auth.uid}, account: ${portfolioAccountId}, currency: ${currency}`);

  if (!portfolioAccountId || !currency) {
    throw new HttpsError('invalid-argument', 'portfolioAccountId y currency son requeridos');
  }

  // El replay tiene que ser completo —el saldo corriente y el promedio
  // ponderado se acumulan hacia adelante y no son reversibles—, pero lo que
  // viaja al cliente no: una cuenta con años de operaciones son cientos de
  // filas que ni caben en pantalla ni aportan nada al abrir la tarjeta. Se
  // recortan **después** de calcular, así que las cifras de las filas que sí
  // se envían son las mismas que serían con el historial entero.
  const requested = Number(limit);
  const rowLimit = Number.isFinite(requested) && requested > 0
    ? Math.min(Math.trunc(requested), LEDGER_MAX_ROWS)
    : LEDGER_DEFAULT_ROWS;

  try {
    const accountRef = db.collection('portfolioAccounts').doc(portfolioAccountId);
    const accountDoc = await accountRef.get();

    if (!accountDoc.exists || accountDoc.data()?.userId !== auth.uid) {
      throw new HttpsError('permission-denied', 'No tienes acceso a esta cuenta');
    }

    const accountData = accountDoc.data();
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);

    const snapshot = await db.collection('transactions')
      .where('portfolioAccountId', '==', portfolioAccountId)
      .get();

    const transactions = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));

    const projection = projectBalanceLedger({
      transactions,
      currency,
      referenceCurrency,
      balance: accountData.balances?.[currency] || 0,
    });

    // El veredicto se guarda para que la página lo lea del documento de cuenta
    // que ya tiene en memoria, sin una sola lectura nueva por visita (D4).
    await accountRef.update({
      [`balanceReconciliation.${currency}`]: {
        ledgerBalance: projection.reconciliation.ledgerBalance,
        difference: projection.reconciliation.difference,
        status: projection.reconciliation.status,
        checkedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
    });

    // `projection.rows` viene de más reciente a más antigua, así que el recorte
    // por la cabeza deja justo los últimos movimientos, que es lo que se quiere.
    const totalCount = projection.rows.length;
    const rows = projection.rows.slice(0, rowLimit);

    console.log(`[queryHandlers][getBalanceLedger] Éxito - ${rows.length}/${totalCount} movimientos, ${projection.reconciliation.status}`);

    return {
      rows,
      /** Movimientos que tiene el saldo en total, más allá de los devueltos */
      totalCount,
      /** true si quedan movimientos más antiguos sin enviar */
      hasMore: totalCount > rows.length,
      reconciliation: {
        ledgerBalance: projection.reconciliation.ledgerBalance,
        storedBalance: projection.reconciliation.storedBalance,
        difference: projection.reconciliation.difference,
        status: projection.reconciliation.status,
        movementCount: projection.reconciliation.movementCount,
      },
      hasFxExposure: projection.hasFxExposure,
      currency,
      referenceCurrency,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[queryHandlers][getBalanceLedger] Error:', error);
    throw new HttpsError('internal', 'Error obteniendo el historial del saldo');
  }
}

/**
 * HU 2.7 — Todo lo que el diálogo necesita saber antes de corregir la apertura.
 *
 * Se pide una sola vez al abrir el diálogo. Con estas cifras el cliente deriva
 * el monto nuevo de la apertura y el mínimo admisible con dos restas, sin una
 * llamada por tecla y sin que el usuario calcule nada (RN-2.7-B, D5).
 *
 * No puede salir de `getBalanceLedger`: ése **recorta filas** después de
 * calcular, así que no ve el punto más bajo del recorrido, que es justo lo que
 * decide si la corrección es admisible (AC-3).
 *
 * No escribe nada. El plan **nunca es la autoridad**: el servidor revalida el
 * recorrido entero al guardar.
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload - { portfolioAccountId, currency }
 * @returns {Promise<Object>} El plan, más si hay que pedir tipo de cambio
 */
async function getOpeningCorrectionPlan(context, payload) {
  const { auth } = context;
  const { portfolioAccountId, currency } = payload || {};

  console.log(`[queryHandlers][getOpeningCorrectionPlan] userId: ${auth.uid}, account: ${portfolioAccountId}, currency: ${currency}`);

  if (!portfolioAccountId || !currency) {
    throw new HttpsError('invalid-argument', 'portfolioAccountId y currency son requeridos');
  }

  try {
    const accountDoc = await db.collection('portfolioAccounts').doc(portfolioAccountId).get();

    if (!accountDoc.exists || accountDoc.data()?.userId !== auth.uid) {
      throw new HttpsError('permission-denied', 'No tienes acceso a esta cuenta');
    }

    const accountData = accountDoc.data();
    const referenceCurrency = await getUserReferenceCurrency(auth.uid);

    // Una sola lectura, con el índice que ya existe desde 2.6.
    const snapshot = await db.collection('transactions')
      .where('portfolioAccountId', '==', portfolioAccountId)
      .get();

    const plan = planOpeningCorrection({
      transactions: snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() })),
      currency,
      referenceCurrency,
      storedBalance: accountData.balances?.[currency] || 0,
      accountCreatedAt: accountData.createdAt || null,
    });

    console.log(`[queryHandlers][getOpeningCorrectionPlan] Éxito - apertura: ${plan.hasOpening ? plan.openingDate : 'no existe'}, ${plan.movementCountAfter} movimientos posteriores`);

    return {
      ...plan,
      currency,
      referenceCurrency,
      // Fecha para la que el cliente propone la tasa cuando hay que declararla:
      // la de la apertura, porque el monto nuevo es del mismo día (RN-2.7-E).
      proposedRateDate: plan.hasOpening ? plan.openingDate : plan.newOpeningDate,
    };
  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error('[queryHandlers][getOpeningCorrectionPlan] Error:', error);
    throw new HttpsError('internal', 'Error preparando la corrección del saldo inicial');
  }
}

/**
 * HU 6.8.1: estado de la corrección del historial del inversor, para el aviso
 * del Dashboard e Inteligencia (RN-6.8.1-N).
 *
 * Sale de las mismas dos fuentes que usa la corrección nocturna, sin campos
 * espejo:
 * - `review`: la corrección agotó sus intentos y está en `reconcileFailures`
 *   sin `resolvedAt` (operación la resuelve borrándolo o poniéndole fecha);
 * - `pending`: `portfolioPerformance/{uid}` tiene la marca `_stale`;
 * - `none`: nada que avisar.
 *
 * Solo lee lo del `auth.uid`: el navegador no puede leer esos documentos.
 * @see platform-docs/stories/6.8.1-historial-corregido-por-tramos/historia.md
 *
 * @param {Object} context - Contexto de ejecución
 * @returns {Promise<{status: 'none'|'pending'|'review'}>}
 */
async function getHistoryCorrectionStatus(context) {
  const userId = context.auth.uid;

  try {
    const [failureDoc, [performanceDoc]] = await Promise.all([
      db.collection('reconcileFailures').doc(userId).get(),
      // Solo la marca: el doc principal no hace falta entero
      db.getAll(db.collection('portfolioPerformance').doc(userId), { fieldMask: ['_stale'] }),
    ]);

    let status = 'none';
    if (failureDoc.exists && !failureDoc.data()?.resolvedAt) status = 'review';
    else if (performanceDoc?.exists && performanceDoc.data()?._stale) status = 'pending';

    console.log(`[queryHandlers][getHistoryCorrectionStatus] userId: ${userId}, estado: ${status}`);
    return { status };
  } catch (error) {
    console.error('[queryHandlers][getHistoryCorrectionStatus] Error:', error);
    throw new HttpsError('internal', 'Error consultando el estado de la corrección del historial');
  }
}

/**
 * HU 6.11.5: la cabecera del día anterior de /portfolio, solo en la moneda que
 * se muestra.
 *
 * Antes la leía el navegador descargando el documento diario con todas sus
 * monedas. Ahora se lee aquí solo la fecha y el bloque que hace falta, y en una
 * moneda poco usada se deriva de USD × la tasa de cierre del día
 * (`previousDayPerformanceService`). Solo lee lo del `auth.uid`.
 *
 * @see platform-docs/stories/6.11.5-recargar-portfolio-sin-lecturas/refinamiento.md (T2)
 *
 * @param {Object} context - Contexto de ejecución
 * @param {Object} payload
 * @param {string} payload.currency - Código ISO de 3 letras
 * @returns {Promise<Object>} Ver `obtenerCabeceraDiaAnterior`
 */
async function getPreviousDayPerformance(context, payload) {
  const userId = context.auth.uid;
  const currency = payload?.currency;
  if (typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency)) {
    throw new HttpsError('invalid-argument', 'Se requiere una moneda válida (código de 3 letras)');
  }

  try {
    return await obtenerCabeceraDiaAnterior(db, { userId, currency });
  } catch (error) {
    console.error('[queryHandlers][getPreviousDayPerformance] Error:', error);
    throw new HttpsError('internal', 'Error consultando el día anterior del portafolio');
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  getCurrentPricesForUser,
  // HU 6.11.5
  getPreviousDayPerformance,
  getHistoricalReturns,
  getMultiAccountHistoricalReturns,
  getIndexHistory,
  getPortfolioDistribution,
  // HU 6.15: el resto de la lista de posiciones de la distribución
  getPortfolioDistributionHoldings,
  getAvailableSectors,
  // HU 2.1: Tipo de cambio de una fecha concreta
  getHistoricalExchangeRate,
  getBalanceCostBasisEstimate,
  // HU 2.6: libro mayor de un saldo
  getBalanceLedger,
  // HU 2.7: plan de la correccion del saldo inicial
  getOpeningCorrectionPlan,
  // HU 6.8.1: estado de la corrección del historial para el aviso
  getHistoryCorrectionStatus,
  // PERF-SNAP-007: Funciones expuestas para testing
  transformSnapshotToResponse,
  getHistoricalReturnsLegacy,
  // FEAT-EXCLUDE-001
  getHistoricalReturnsExcluding,
  // PERF-SNAP-008: Funciones expuestas para testing
  aggregateSnapshotTimelines,
  getMultiAccountHistoricalReturnsLegacy,
  // PERF-SNAP-021: Cache in-memory helpers para testing
  clearSnapshotMemCache,
  getSnapshotMemCacheSize,
  getSnapshotWithCache,
  getSnapshotCacheTTL,
  // HU 6.11.2: una cuenta, todas y el Copiloto desde los resúmenes
  rendimientoDesdeResumenes,
  rendimientoDisponibleEn,
  // HU 6.11.6: la misma agregación de varias cuentas para `scripts/activo-desde-resumen-usd.js`
  agregarDiasDeCuentas,
};
