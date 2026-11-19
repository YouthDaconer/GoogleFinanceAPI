/**
 * HU 6.13 — Se retiran la ruta antigua de rendimientos y los consolidados de
 * meses y años.
 *
 * Las pantallas de rendimiento ya responden desde los resúmenes (6.4, 6.9.2,
 * 6.11, 6.12). La ruta antigua (`getHistoricalReturnsLegacy` → V2 sobre
 * `consolidatedPeriods`) solo entraba cuando un resumen no se podía preparar, y
 * aun así los consolidados se rehacían cada día 1, cada 1 de enero y en cada
 * tramo de cada corrección nocturna.
 *
 * Con el interruptor encendido:
 * - nadie escribe consolidados: ni los trabajos mensual y anual (siguen
 *   exportados y desplegados, pero no hacen nada), ni la corrección nocturna
 *   (RN-6.13-B);
 * - nadie los lee: donde se caía a la ruta antigua, el inversor ve el aviso de
 *   rendimientos no disponibles por ahora (RN-6.13-A);
 * - los consolidados guardados se conservan (RN-6.13-C): este módulo no borra nada.
 *
 * `RETIRE_CONSOLIDATED_PERIODS=false` devuelve el comportamiento de antes
 * (RN-6-H). Antes de apagarlo se rehacen los consolidados con
 * `node scripts/ruta-antigua-consolidados.js rehacer --aplicar` para que la ruta
 * antigua no responda con meses desactualizados (RN-6.13-E).
 *
 * Vive en un módulo propio para que los mocks de `periodConsolidationScheduled`
 * y `consolidatedReturnsService` que usan los tests no lo pierdan.
 *
 * @module services/consolidation/consolidatedPeriodsRetirementService
 * @see platform-docs/stories/6.13-retirar-ruta-antigua-consolidados/refinamiento.md (T1)
 */

const { HttpsError } = require('firebase-functions/v2/https');

/** Código que el navegador reconoce en `details.code` para pintar el aviso. */
const CODIGO_RENDIMIENTOS_NO_DISPONIBLES = 'RETURNS_UNAVAILABLE';

/**
 * Se lee en cada llamada, como los interruptores de la 6.9.2 y la 6.12: basta
 * con cambiar la variable y desplegar para volver atrás (RN-6-H).
 *
 * @returns {boolean} `true` salvo `RETIRE_CONSOLIDATED_PERIODS=false`
 */
function consolidadosRetirados() {
  return process.env.RETIRE_CONSOLIDATED_PERIODS !== 'false';
}

/**
 * La pantalla que pidió el rendimiento, para el registro (Escenario 3).
 *
 * @param {{ accountId?: string, ticker?: string|null, assetType?: string|null }} params
 * @returns {'activo'|'portafolio'|'cuenta'}
 */
function pantallaDe({ accountId = 'overall', ticker = null, assetType = null } = {}) {
  if (ticker || assetType) return 'activo';
  return accountId === 'overall' ? 'portafolio' : 'cuenta';
}

/**
 * HU 6.13 (RN-6.13-A): el error con el que la lectura de rendimientos dice que
 * las cifras no están disponibles por ahora. Es un error y no una respuesta
 * vacía para que nada lo guarde (ni `performanceCache` ni el navegador): la
 * siguiente apertura vuelve a intentar preparar el resumen.
 *
 * Deja en el registro el inversor, la pantalla y el motivo:
 * `[returnsUnavailable] <uid> <pantalla> <alcance> <moneda>[ <activo>]: <motivo>`.
 *
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} [params.accountId='overall']
 * @param {string} [params.currency='USD']
 * @param {string|null} [params.ticker]
 * @param {string|null} [params.assetType]
 * @param {string} params.motivo - Por qué no se pudo preparar el resumen
 * @returns {HttpsError}
 */
function rendimientosNoDisponibles({
  userId,
  accountId = 'overall',
  currency = 'USD',
  ticker = null,
  assetType = null,
  motivo,
}) {
  const pantalla = pantallaDe({ accountId, ticker, assetType });
  const activo = ticker || assetType ? ` ${ticker || '-'}_${assetType || '-'}` : '';
  console.warn(`[returnsUnavailable] ${userId} ${pantalla} ${accountId} ${currency}${activo}: ${motivo}`);
  return new HttpsError(
    'unavailable',
    'Tus rendimientos no están disponibles por ahora',
    { code: CODIGO_RENDIMIENTOS_NO_DISPONIBLES, pantalla, motivo },
  );
}

module.exports = {
  CODIGO_RENDIMIENTOS_NO_DISPONIBLES,
  consolidadosRetirados,
  pantallaDe,
  rendimientosNoDisponibles,
};
