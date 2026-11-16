/**
 * HU 6.11.6 (T1) — Los días de un activo, guardados en su resumen en USD.
 *
 * El detalle de un activo en una moneda poco usada se calcula (HU 6.11.3) desde
 * los días del activo en USD, convertidos con las tasas de cierre. Leer esos días
 * cuesta una lectura por día. Aquí está el registro compacto de cada día que el
 * resumen por activo en USD guarda (`assetDays`) para no tener que leerlos:
 *
 * - Un día con el activo: `{ d, v, a, x, u, i, n, f, p }`, los campos que lee el
 *   cálculo desde cero (valor, cambio ajustado, cambio diario, unidades,
 *   inversión, P&G no realizado, flujo y P&G realizado). Se guardan tal cual,
 *   sin redondear ni sanear; los ausentes se omiten, y el flujo y el P&G
 *   realizado también cuando son 0 (el cálculo los lee con `|| 0` / `?? 0`).
 * - Un día con USD pero sin el activo: `{ d, e: 'sin-activo' }`. Es la marca de
 *   venta del gráfico y, con varias cuentas, cuenta para los años con datos.
 * - Un día sin el bloque de USD: `{ d, e: 'sin-moneda' }`.
 *
 * `docsDesdeDiasDelActivo` reconstruye con ellos los documentos que la 6.11.3
 * lee hoy de Firestore, así que el cálculo de siempre da la misma cifra
 * (RN-6.11.6-H, RN-6-A).
 *
 * @module utils/assetDays
 * @see platform-docs/stories/6.11.6-detalle-activo-desde-resumen-usd/refinamiento.md (T1)
 */

/** Campo del activo en el día → clave corta en el registro. */
const CLAVES = {
  totalValue: 'v',
  adjustedDailyChangePercentage: 'a',
  dailyChangePercentage: 'x',
  units: 'u',
  totalInvestment: 'i',
  unrealizedProfitAndLoss: 'n',
  totalCashFlow: 'f',
  doneProfitAndLoss: 'p',
};

/** Se omiten en 0: es lo que vale un día sin movimientos. */
const OMITIR_EN_CERO = new Set(['totalCashFlow', 'doneProfitAndLoss']);

const SIN_ACTIVO = 'sin-activo';
const SIN_MONEDA = 'sin-moneda';

/**
 * El registro de un día a partir del bloque del activo.
 *
 * @param {string} fecha - YYYY-MM-DD
 * @param {Object|null|undefined} datosActivo - `assetPerformance[clave]` del día (vacío = sin el activo)
 * @returns {Object}
 */
function registroDelActivo(fecha, datosActivo) {
  if (!datosActivo || typeof datosActivo !== 'object') return { d: fecha, e: SIN_ACTIVO };
  const registro = { d: fecha };
  for (const [campo, clave] of Object.entries(CLAVES)) {
    const valor = datosActivo[campo];
    if (valor === undefined || valor === null) continue;
    if (OMITIR_EN_CERO.has(campo) && valor === 0) continue;
    registro[clave] = valor;
  }
  return registro;
}

/**
 * El registro de un documento diario en una moneda.
 *
 * @param {string} fecha - YYYY-MM-DD
 * @param {Object|null|undefined} datosMoneda - El bloque de la moneda del día
 * @param {string} claveActivo - `${ticker}_${assetType}`
 * @returns {Object}
 */
function registroDelDia(fecha, datosMoneda, claveActivo) {
  if (!datosMoneda) return { d: fecha, e: SIN_MONEDA };
  return registroDelActivo(fecha, datosMoneda.assetPerformance?.[claveActivo]);
}

/**
 * Añade un día al final. Un día igual o anterior al último ya registrado se
 * ignora: la puesta al día nunca lo reescribe.
 *
 * @param {Object[]} dias
 * @param {Object} registro
 * @returns {Object[]} Una copia con el día añadido (o la misma lista si se ignoró)
 */
function agregarDiaAlActivo(dias, registro) {
  const ultimo = dias[dias.length - 1];
  if (ultimo && registro.d <= ultimo.d) return dias;
  const copia = dias.slice();
  copia.push(registro);
  return copia;
}

/**
 * Los días del activo desde los documentos diarios, uno por documento y en el
 * mismo orden (los que lee `fetchAssetDailyDocs`).
 *
 * @param {Array} docs - Snapshots de Firestore o planos, en orden de fecha
 * @param {string} currency
 * @param {string} ticker
 * @param {string} assetType
 * @returns {Object[]}
 */
function extraerDiasDelActivo(docs, currency, ticker, assetType) {
  const claveActivo = `${ticker}_${assetType}`;
  return docs.map((doc) => {
    const data = doc && typeof doc.data === 'function' ? doc.data() : doc;
    return registroDelDia(data?.date, data?.[currency], claveActivo);
  });
}

/**
 * Los documentos diarios en USD que reconstruyen los días del activo, con la
 * forma que devuelve `fetchAssetDailyDocs`: `{ date, USD: { assetPerformance } }`
 * con el activo, `{ date, USD: {} }` sin él y `{ date }` sin USD.
 *
 * @param {Object[]} dias - `assetDays` del resumen en USD
 * @param {string} claveActivo - `${ticker}_${assetType}`
 * @returns {Object[]}
 */
function docsDesdeDiasDelActivo(dias, claveActivo) {
  return dias.map((registro) => {
    if (registro.e === SIN_MONEDA) return { date: registro.d };
    if (registro.e === SIN_ACTIVO) return { date: registro.d, USD: {} };
    const activo = {};
    for (const [campo, clave] of Object.entries(CLAVES)) {
      if (registro[clave] !== undefined) activo[campo] = registro[clave];
    }
    return { date: registro.d, USD: { assetPerformance: { [claveActivo]: activo } } };
  });
}

module.exports = {
  registroDelActivo,
  registroDelDia,
  agregarDiaAlActivo,
  extraerDiasDelActivo,
  docsDesdeDiasDelActivo,
  SIN_ACTIVO,
  SIN_MONEDA,
};
