const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const admin = require('firebase-admin');
const { DateTime } = require('luxon');
const { scrapeDividendsInfoFromStockEvents } = require('./scrapeDividendsInfoFromStock');
// OPT-DEMAND-CLEANUP: Importar helper para obtener precios y currencies del API Lambda
const { getPricesFromApi, getCurrencyRatesFromApi } = require('./marketDataHelper');

// HU 2.4: un dividendo es dinero que nace dentro de la cuenta, así que entra al
// saldo con el tipo de cambio del día del pago (RN-03). Hasta ahora este job era
// el único punto de escritura de saldo que NO pasaba por el helper de 2.1: subía
// `balances` sin tocar `balanceCostBasis`, de modo que la tasa promedio derivada
// (`cost / balance`) quedaba mintiendo a la baja con cada dividendo cobrado.
const { buildBalanceUpdate, getUserReferenceCurrency } = require('./helpers/balanceCostBasis');
const { resolveRealizationRate } = require('./helpers/realizedFxDecomposition');

/**
 * SEC-TOKEN-001: Secret para autenticación server-to-server con API finance-query
 * Usado por getPricesFromApi/getCurrencyRatesFromApi para obtener datos de dividendos.
 * 
 * @see docs/architecture/SEC-TOKEN-001-api-security-hardening-plan.md
 */
const cfServiceToken = defineSecret('CF_SERVICE_TOKEN');

exports.processDividendPayments = onSchedule({
  schedule: '0 7,18 * * *',  // Ejecutar a las 7:00 AM y 6:00 PM todos los días
  timeZone: 'America/New_York',
  retryCount: 3,
  secrets: [cfServiceToken],  // SEC-TOKEN-001: Binding del secret para API auth
}, async (event) => {
  const db = admin.firestore();
  const now = DateTime.now().setZone('America/New_York');
  const formattedDate = now.toISODate();

  console.log(`Verificando dividendos para la fecha ${formattedDate}`);

  // OPT-FIRESTORE-002: Early exit en weekends y feriados.
  // Los dividendos solo se pagan en trading days. Ejecutar en weekends
  // lee ~1,800 assets × 2 veces/día sin resultado = ~25K lecturas/semana desperdiciadas.
  const dayOfWeek = now.weekday; // 1=Monday ... 7=Sunday
  if (dayOfWeek === 6 || dayOfWeek === 7) {
    console.log(`[processDividendPayments] Skipping - weekend (day ${dayOfWeek})`);
    return null;
  }

  try {
    // FIX-READS-004: Read assets ONCE and inject into scrapeDividendsInfoFromStockEvents
    // to avoid redundant global scan inside that function
    const assetsSnapshot = await db.collection('assets').where('isActive', '==', true).get();
    const assets = assetsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    // Primero actualizar información de dividendos para ETFs y acciones sin datos
    console.log('Actualizando información de dividendos de activos...');
    await scrapeDividendsInfoFromStockEvents({ injectedAssetsSnapshot: assetsSnapshot });
    console.log('Actualización de información de dividendos completada');
    
    // Extraer símbolos únicos
    const symbols = [...new Set(assets.map(a => a.name).filter(Boolean))];
    
    // OPT-DEMAND-CLEANUP: Obtener precios (incluye datos de dividendos) y currencies del API Lambda
    const [currentPricesArray, currencies] = await Promise.all([
      getPricesFromApi(symbols),
      getCurrencyRatesFromApi()
    ]);
    
    // Convertir a formato compatible con el código existente
    const currentPrices = currentPricesArray.map(price => ({
      id: price.symbol,
      symbol: price.symbol,
      ...price
    }));
    
    console.log(`[OPT-DEMAND-CLEANUP] Precios obtenidos del API: ${currentPrices.length}, Currencies: ${currencies.length}`);

    // Filtrar activos con fecha de dividendo hoy y que tengan la información de dividendo necesaria
    const todaysDividends = currentPrices.filter(price => {
      // Verificar que tenga toda la información necesaria de dividendos
      if (!price.dividendDate || !price.dividend || parseFloat(price.dividend) <= 0) {
        return false;
      }

      // Convertir la fecha de dividendo a formato ISO
      try {
        const dividendDate = DateTime.fromFormat(price.dividendDate, 'MMM d, yyyy').toISODate();
        return dividendDate === formattedDate;
      } catch (error) {
        console.log(`Error al procesar fecha de dividendo para ${price.symbol}: ${error.message}`);
        return false;
      }
    });

    if (todaysDividends.length === 0) {
      console.log('No hay dividendos programados para hoy');
      return null;
    }

    console.log(`Encontrados ${todaysDividends.length} activos con dividendos para hoy`);

    // OPT-DEMAND-CLEANUP: Assets y currencies ya fueron obtenidos arriba
    // Solo obtener cuentas de cartera
    const portfolioAccountsSnapshot = await db.collection('portfolioAccounts').where('isActive', '==', true).get();
    const portfolioAccounts = portfolioAccountsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    // Obtener las transacciones de dividendos de hoy para evitar duplicados
    const todaysDividendTransactionsSnapshot = await db.collection('transactions')
      .where('date', '==', formattedDate)
      .where('type', '==', 'dividendPay')
      .get();
    const todaysDividendTransactions = todaysDividendTransactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    // Agrupar activos por cuenta de portafolio y símbolo
    const portfolioSymbolAssets = {};

    for (const asset of assets) {
      const portfolioAccount = portfolioAccounts.find(acc => acc.id === asset.portfolioAccount);
      if (!portfolioAccount) continue;

      // Verificar si es un símbolo con dividendo hoy
      const matchingDividend = todaysDividends.find(div => div.symbol === asset.name);
      if (!matchingDividend) continue;

      // Verificar si el asset tiene fecha de adquisición y si la fecha de exDividend está disponible
      if (!matchingDividend.exDividend || !asset.acquisitionDate) {
        console.log(`Activo ${asset.id} (${asset.name}) excluido del pago de dividendos: ${!matchingDividend.exDividend ? 'falta fecha exDividend' : 'falta fecha de adquisición'}`);
        continue;
      }
      
      try {
        // Convertir la fecha de exDividend a formato ISO
        const exDividendDate = DateTime.fromFormat(matchingDividend.exDividend, 'MMM d, yyyy').toISODate();
        
        // Verificar si la fecha de adquisición es después de la fecha exDividend
        if (asset.acquisitionDate > exDividendDate) {
          console.log(`Activo ${asset.id} (${asset.name}) excluido del pago de dividendos: fue adquirido el ${asset.acquisitionDate} después de la fecha exDividend ${exDividendDate}`);
          continue;
        }
      } catch (error) {
        console.log(`Error al procesar fecha exDividend para ${matchingDividend.symbol}: ${error.message}, activo excluido`);
        continue;
      }

      const key = `${asset.portfolioAccount}_${asset.name}`;

      if (!portfolioSymbolAssets[key]) {
        portfolioSymbolAssets[key] = {
          portfolioAccountId: asset.portfolioAccount,
          userId: portfolioAccount.userId,
          symbol: asset.name,
          assetType: asset.assetType,
          currency: asset.currency,
          defaultCurrencyForAdquisitionDollar: asset.defaultCurrencyForAdquisitionDollar,
          dividend: matchingDividend,
          units: 0,
          relatedAssets: []
        };
      }

      portfolioSymbolAssets[key].units += parseFloat(asset.units || 0);
      portfolioSymbolAssets[key].relatedAssets.push(asset.id);
    }

    const batch = db.batch();
    let transactionsCreated = 0;

    // Mapa para acumular los dividendos por cuenta y moneda
    const portfolioAccountUpdates = {};

    // HU 2.4: la moneda de referencia y la tasa del día se resuelven una sola vez
    // por usuario y por divisa. Todos los dividendos de una corrida comparten la
    // misma fecha, así que la caché `historicalExchangeRates/{fecha}` resuelve la
    // primera consulta y sirve el resto sin salir a Yahoo.
    const referenceCurrencyByUser = new Map();
    const realizationRateByPair = new Map();

    const resolveReferenceCurrency = async (userId) => {
      if (!referenceCurrencyByUser.has(userId)) {
        referenceCurrencyByUser.set(userId, await getUserReferenceCurrency(userId));
      }
      return referenceCurrencyByUser.get(userId);
    };

    const resolveDayRate = async (currency, referenceCurrency) => {
      const key = `${currency}_${referenceCurrency}`;
      if (!realizationRateByPair.has(key)) {
        realizationRateByPair.set(
          key,
          await resolveRealizationRate(currency, referenceCurrency, formattedDate)
        );
      }
      return realizationRateByPair.get(key);
    };

    // Crear transacciones por cuenta y símbolo
    for (const key of Object.keys(portfolioSymbolAssets)) {
      const portfolioSymbolData = portfolioSymbolAssets[key];

      // Verificar si ya existe una transacción para esta cuenta y símbolo hoy
      const existingTransaction = todaysDividendTransactions.find(
        t => t.portfolioAccountId === portfolioSymbolData.portfolioAccountId &&
          t.symbol === portfolioSymbolData.symbol &&
          t.type === 'dividendPay'
      );

      if (existingTransaction) {
        console.log(`Ya existe una transacción de dividendo para la cuenta ${portfolioSymbolData.portfolioAccountId} y símbolo ${portfolioSymbolData.symbol}`);
        continue;
      }

      // Calcular el monto del dividendo
      const annualDividend = parseFloat(portfolioSymbolData.dividend.dividend || 0);
      const totalUnits = portfolioSymbolData.units;

      if (annualDividend <= 0 || totalUnits <= 0) {
        console.log(`Monto de dividendo calculado es cero o negativo para ${portfolioSymbolData.symbol} en cuenta ${portfolioSymbolData.portfolioAccountId}`);
        continue;
      }

      // Obtener información de la cuenta para aplicar deducción de impuestos
      const portfolioAccountRef = db.collection('portfolioAccounts').doc(portfolioSymbolData.portfolioAccountId);
      const portfolioAccountDoc = await portfolioAccountRef.get();
      const portfolioAccountData = portfolioAccountDoc.data();
      const taxDeductionPercentage = portfolioAccountData?.taxDeductionPercentage || 0;

      // Calcular montos bruto y neto
      const quarterlyDividendPerUnit = annualDividend / 4;
      const grossAmount = quarterlyDividendPerUnit * totalUnits;
      const taxDeductionAmount = grossAmount * (taxDeductionPercentage / 100);
      const netAmount = grossAmount - taxDeductionAmount;

      // HU 2.4: a qué tipo de cambio entra este dividendo al saldo (RN-03)
      const dividendCurrency = portfolioSymbolData.currency || 'USD';
      const referenceCurrency = await resolveReferenceCurrency(portfolioSymbolData.userId);
      const { realizationRate, realizationRateSource } =
        await resolveDayRate(dividendCurrency, referenceCurrency);
      // Sin tasa del día no se inventa un costo: el saldo queda indeterminado y
      // el dividendo se registra igual — el usuario lo cobró (RN-13, D5).
      const dividendCost = realizationRate === null
        ? null
        : Number((netAmount * realizationRate).toFixed(2));

      // Crear una única transacción de dividendo para esta cuenta y símbolo
      const transactionRef = db.collection('transactions').doc();
      const transaction = {
        id: transactionRef.id,
        symbol: portfolioSymbolData.symbol,
        type: 'dividendPay',
        amount: totalUnits,
        price: netAmount / totalUnits, // Precio por unidad después de impuestos
        currency: portfolioSymbolData.currency || 'USD',
        date: formattedDate,
        portfolioAccountId: portfolioSymbolData.portfolioAccountId,
        commission: 0,
        assetType: portfolioSymbolData.assetType,
        dollarPriceToDate: currencies.find(c => c.code === 'USD')?.exchangeRate || 1,
        defaultCurrencyForAdquisitionDollar: portfolioSymbolData.defaultCurrencyForAdquisitionDollar || 'USD',
        description: `Pago de dividendo anual de ${portfolioSymbolData.dividend.name || portfolioSymbolData.symbol} (${totalUnits} unidades)${taxDeductionPercentage > 0 ? ` - Impuestos deducidos: ${taxDeductionPercentage}%` : ''}`,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        userId: portfolioSymbolData.userId,
        relatedAssets: portfolioSymbolData.relatedAssets,
        taxDeductionPercentage: taxDeductionPercentage,
        taxDeductionAmount: taxDeductionAmount,
        grossAmount: grossAmount,
        // HU 2.4: trazabilidad cambiaria del dividendo. `dollarPriceToDate`
        // conserva su semántica intacta para sus lectores actuales (D3).
        referenceCurrency: referenceCurrency,
        realizationRate: realizationRate,
        realizationRateSource: realizationRateSource,
        acquisitionCost: dividendCost
      };

      batch.set(transactionRef, transaction);
      transactionsCreated++;

      console.log(`Creada transacción de dividendo para ${portfolioSymbolData.symbol} en cuenta ${portfolioSymbolData.portfolioAccountId}, unidades totales: ${totalUnits}, monto bruto: ${grossAmount.toFixed(4)}, impuestos deducidos: ${taxDeductionAmount.toFixed(4)} (${taxDeductionPercentage}%), monto neto: ${netAmount.toFixed(4)} ${transaction.currency}`);
      
      // Acumular los montos de dividendos por cuenta y moneda (usar monto neto)
      const accountKey = portfolioSymbolData.portfolioAccountId;
      const currency = transaction.currency;
      
      if (!portfolioAccountUpdates[accountKey]) {
        portfolioAccountUpdates[accountKey] = {};
      }
      
      if (!portfolioAccountUpdates[accountKey][currency]) {
        // HU 2.4: el costo se acumula en paralelo al monto. Basta que UN dividendo
        // de la misma cuenta y divisa no tenga tasa para que el costo del conjunto
        // deje de ser determinable: se marca y el saldo pasa a indeterminado.
        portfolioAccountUpdates[accountKey][currency] = { amount: 0, cost: 0, costKnown: true };
      }

      portfolioAccountUpdates[accountKey][currency].amount += netAmount;
      if (dividendCost === null) {
        portfolioAccountUpdates[accountKey][currency].costKnown = false;
      } else {
        portfolioAccountUpdates[accountKey][currency].cost += dividendCost;
      }
    }

    // Actualizar los balances de las cuentas después de acumular todos los dividendos
    for (const [accountId, currencyAmounts] of Object.entries(portfolioAccountUpdates)) {
      const portfolioAccountRef = db.collection('portfolioAccounts').doc(accountId);
      const portfolioAccountDoc = await portfolioAccountRef.get();
      const portfolioAccountData = { id: accountId, ...portfolioAccountDoc.data() };
      const referenceCurrency = await resolveReferenceCurrency(portfolioAccountData.userId);

      // HU 2.4: los fragmentos de cada divisa se fusionan en un solo `update`.
      // Son rutas con punto (`balances.USD`), no el objeto `balances` completo, así
      // que una divisa no puede pisar a otra ni perder una escritura concurrente.
      const accountUpdate = {};

      for (const [currency, accumulated] of Object.entries(currencyAmounts)) {
        Object.assign(accountUpdate, buildBalanceUpdate({
          account: portfolioAccountData,
          currency,
          amountDelta: accumulated.amount,
          costDelta: accumulated.costKnown ? Number(accumulated.cost.toFixed(2)) : null,
          referenceCurrency,
        }));

        console.log(`Acumulado dividendo neto de ${accumulated.amount.toFixed(4)} ${currency} para la cuenta ${accountId}. Nuevo balance: ${accountUpdate[`balances.${currency}`]}`);
      }

      // Actualizar la cuenta del portafolio en el batch
      batch.update(portfolioAccountRef, accountUpdate);
      console.log(`Preparada actualización de balances para la cuenta ${accountId}`);
    }

    if (transactionsCreated > 0) {
      await batch.commit();
      console.log(`${transactionsCreated} transacciones de dividendos procesadas exitosamente y balances actualizados`);
    } else {
      console.log('No se crearon transacciones de dividendos');
    }

    return null;
  } catch (error) {
    console.error('Error al procesar dividendos:', error);
    return null;
  }
});