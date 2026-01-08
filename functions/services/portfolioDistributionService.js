/**
 * Portfolio Distribution Service
 * 
 * Servicio para calcular distribución del portafolio (sectores, países, holdings).
 * Migrado desde usePortfolioDistribution.ts y useCountriesDistribution.ts del frontend.
 * 
 * @see SCALE-OPT-001 - Migración de Cálculos Frontend → Backend (SOLID)
 */

const admin = require('./firebaseAdmin');
const db = admin.firestore();
const { StructuredLogger } = require('../utils/logger');
const fetch = require('node-fetch');

const logger = new StructuredLogger('PortfolioDistributionService');

// Cache en memoria con TTL de 5 minutos
const distributionCache = new Map();
const CACHE_TTL = 5 * 60 * 1000;

// Cache para datos de ETFs (más largo TTL ya que cambian poco)
const etfDataCache = new Map();
const ETF_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 horas

// Cache para sectores (raramente cambian)
let sectorsCache = null;
let sectorsCacheTimestamp = 0;
const SECTORS_CACHE_TTL = 60 * 60 * 1000; // 1 hora

// Cache para países
let countriesCache = null;
let countriesCacheTimestamp = 0;
const COUNTRIES_CACHE_TTL = 60 * 60 * 1000; // 1 hora

// Cache para tasas de cambio de monedas
let currencyRatesCache = null;
let currencyRatesCacheTimestamp = 0;
const CURRENCY_RATES_CACHE_TTL = 15 * 60 * 1000; // 15 minutos

/**
 * Obtiene la distribución del portafolio (sectores, países, holdings)
 * @param {string} userId - ID del usuario
 * @param {Object} options - Opciones de consulta
 * @param {string[]} [options.accountIds] - IDs de cuentas específicas (opcional)
 * @param {string} [options.accountId] - ID de cuenta específica (opcional)
 * @param {string} [options.currency] - Moneda de presentación (default: USD)
 * @param {boolean} [options.includeHoldings] - Incluir holdings detallados
 * @returns {Promise<Object>} Distribución del portafolio
 */
async function getPortfolioDistribution(userId, options = {}) {
  const startTime = Date.now();
  const cacheKey = buildCacheKey(userId, options);
  
  // Verificar cache con validación de timestamp
  const cached = distributionCache.get(cacheKey);
  if (cached) {
    // Verificar si el cache no ha expirado por TTL
    const ttlValid = Date.now() - cached.timestamp < CACHE_TTL;
    
    if (ttlValid) {
      // Verificar si el portafolio fue modificado después del cache
      const lastModified = await getPortfolioLastModified(userId);
      
      if (!lastModified || cached.timestamp > lastModified) {
        logger.info('Cache hit for distribution', { 
          userId, 
          cacheKey,
          cacheAge: Date.now() - cached.timestamp,
          lastModified: lastModified ? new Date(lastModified).toISOString() : 'none'
        });
        return { ...cached.data, metadata: { ...cached.data.metadata, fromCache: true } };
      } else {
        logger.info('Cache invalidated by portfolioLastModified', { 
          userId, 
          cacheTimestamp: new Date(cached.timestamp).toISOString(),
          lastModified: new Date(lastModified).toISOString()
        });
      }
    }
  }

  try {
    // 1. Obtener assets del usuario
    const assets = await getActiveAssets(userId, options);
    
    if (!assets.length) {
      return buildEmptyResponse(options.currency);
    }

    // 2. Obtener precios actuales
    const symbols = [...new Set(assets.map(a => a.name))];
    const prices = await batchGetPrices(symbols);

    // 3. Obtener cuentas del portafolio
    const portfolioAccounts = await getPortfolioAccounts(userId);

    // 4. Obtener datos de ETFs
    const etfSymbols = assets
      .filter(a => a.assetType === 'etf' || prices[a.name]?.type === 'etf')
      .map(a => a.name);
    const etfData = await batchGetETFData(etfSymbols);

    // 5. Obtener tasas de cambio de monedas (FIX-CURRENCY-001)
    const currencyRates = await getCurrencyRates();

    // 6. Obtener mapeo de sectores
    const sectorMappings = await getSectorMappings();

    // 7. Obtener mapeo de países
    const countryMappings = await getCountryMappings();

    // 8. Calcular valor total del portafolio (FIX-CURRENCY-001: convertir a USD)
    const totalPortfolioValue = calculateTotalValue(assets, prices, currencyRates);
    
    if (totalPortfolioValue === 0) {
      return buildEmptyResponse(options.currency);
    }

    // 9. Calcular distribuciones (FIX-CURRENCY-001: pasar currencyRates)
    const { holdings, sectors, etfStats } = calculateSectorDistribution(
      assets, prices, etfData, sectorMappings, portfolioAccounts, userId, totalPortfolioValue, currencyRates
    );

    const countries = calculateCountryDistribution(
      assets, prices, etfData, countryMappings, portfolioAccounts, userId, totalPortfolioValue, currencyRates
    );

    // SCALE-OPT-001: Calcular activos sin ubicación geográfica
    const nonGeographicData = calculateNonGeographicAssets(
      assets, prices, etfData, countryMappings, portfolioAccounts, userId, totalPortfolioValue, currencyRates
    );

    // SCALE-OPT-001: Filtrar países con porcentaje muy pequeño (< 0.0001%)
    // Estos países no se pueden representar significativamente en el mapa
    const MIN_PERCENTAGE_THRESHOLD = 0.0001;
    const filteredCountries = countries.filter(c => c.percentage >= MIN_PERCENTAGE_THRESHOLD);

    // 9. Construir respuesta
    const result = {
      sectors: sectors.map(s => ({
        sector: s.sector,
        weight: s.weight,
        percentage: s.weight * 100
      })),
      countries: filteredCountries.map(c => ({
        id: c.id,
        name: c.name,
        value: c.value,
        percentage: c.percentage,
        assets: c.assets || []
      })),
      // SCALE-OPT-001: Incluir activos sin ubicación geográfica
      nonGeographicData: {
        totalPercentage: nonGeographicData.totalPercentage,
        assets: nonGeographicData.assets,
        assetTypeDistribution: nonGeographicData.assetTypeDistribution
      },
      holdings: options.includeHoldings ? holdings : undefined,
      totals: {
        portfolioValue: totalPortfolioValue,
        currency: options.currency || 'USD'
      },
      metadata: {
        calculatedAt: new Date().toISOString(),
        assetCount: assets.length,
        accountCount: new Set(assets.map(a => a.portfolioAccount).filter(Boolean)).size,
        etfCount: etfSymbols.length,
        etfDataLoaded: etfData.size,
        etfDecomposed: etfStats.decomposed,
        etfNotDecomposed: etfStats.notDecomposed
      }
    };

    // Guardar en cache
    distributionCache.set(cacheKey, { data: result, timestamp: Date.now() });
    
    logger.info('Distribution calculated', {
      userId,
      duration: Date.now() - startTime,
      assetCount: assets.length,
      sectorsCount: sectors.length,
      countriesCount: countries.length
    });

    return result;
  } catch (error) {
    logger.error('Error calculating distribution', { userId, error: error.message });
    throw error;
  }
}

/**
 * Construye la clave de cache
 */
function buildCacheKey(userId, options) {
  const accountPart = options.accountId 
    ? options.accountId 
    : options.accountIds?.join(',') || 'all';
  return `dist:${userId}:${accountPart}:${options.currency || 'USD'}`;
}

// Cache local de portfolioLastModified para evitar lecturas repetidas a Firestore
const lastModifiedCache = new Map();
const LAST_MODIFIED_CACHE_TTL = 30 * 1000; // 30 segundos

/**
 * Obtiene el timestamp de última modificación del portafolio
 * @param {string} userId - ID del usuario
 * @returns {Promise<number|null>} - Timestamp en milisegundos o null
 */
async function getPortfolioLastModified(userId) {
  // Verificar cache local primero
  const cached = lastModifiedCache.get(userId);
  if (cached && Date.now() - cached.fetchedAt < LAST_MODIFIED_CACHE_TTL) {
    return cached.timestamp;
  }
  
  try {
    const userDoc = await db.collection('users').doc(userId).get();
    
    if (!userDoc.exists) {
      return null;
    }
    
    const userData = userDoc.data();
    const lastModified = userData.portfolioLastModified;
    
    // portfolioLastModified puede ser un Firestore Timestamp o undefined
    let timestamp = null;
    if (lastModified) {
      timestamp = lastModified.toMillis ? lastModified.toMillis() : lastModified;
    }
    
    // Guardar en cache local
    lastModifiedCache.set(userId, { timestamp, fetchedAt: Date.now() });
    
    return timestamp;
  } catch (error) {
    logger.warn('Failed to get portfolioLastModified', { userId, error: error.message });
    return null;
  }
}

/**
 * Respuesta vacía para portafolios sin assets
 */
function buildEmptyResponse(currency) {
  return {
    sectors: [],
    countries: [],
    holdings: [],
    totals: { portfolioValue: 0, currency: currency || 'USD' },
    metadata: {
      calculatedAt: new Date().toISOString(),
      assetCount: 0,
      accountCount: 0
    }
  };
}

/**
 * Obtiene assets activos del usuario
 * 
 * NOTA: Los assets NO tienen userId directo. Se relacionan con el usuario
 * a través del campo portfolioAccount → portfolioAccounts.userId
 */
async function getActiveAssets(userId, options) {
  // Primero obtener las cuentas del usuario
  const userAccounts = await getPortfolioAccounts(userId);
  
  if (!userAccounts.length) {
    logger.info('No portfolio accounts found for user', { userId });
    return [];
  }
  
  const userAccountIds = userAccounts.map(a => a.id);
  logger.info('User accounts found', { userId, accountCount: userAccountIds.length });

  // Determinar qué cuentas filtrar
  let targetAccountIds = userAccountIds;
  
  if (options.accountId && options.accountId !== 'overall') {
    if (options.accountId === 'account_null') {
      // Buscar assets sin cuenta asignada (raro, pero posible)
      const snapshot = await db.collection('assets')
        .where('portfolioAccount', '==', null)
        .where('isActive', '==', true)
        .get();
      return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    }
    // Filtrar por cuenta específica (validar que pertenece al usuario)
    if (userAccountIds.includes(options.accountId)) {
      targetAccountIds = [options.accountId];
    } else {
      logger.warn('Account does not belong to user', { userId, accountId: options.accountId });
      return [];
    }
  } else if (options.accountIds?.length) {
    // Filtrar por cuentas específicas (validar que pertenecen al usuario)
    targetAccountIds = options.accountIds.filter(id => userAccountIds.includes(id));
    if (!targetAccountIds.length) {
      logger.warn('None of the requested accounts belong to user', { userId, requestedIds: options.accountIds });
      return [];
    }
  }

  // Firestore permite máximo 10 valores en 'in', hacemos batch si es necesario
  const allAssets = [];
  for (let i = 0; i < targetAccountIds.length; i += 10) {
    const batch = targetAccountIds.slice(i, i + 10);
    const snapshot = await db.collection('assets')
      .where('portfolioAccount', 'in', batch)
      .where('isActive', '==', true)
      .get();
    
    allAssets.push(...snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() })));
  }

  logger.info('Assets loaded', { userId, assetCount: allAssets.length });
  return allAssets;
}

/**
 * Obtiene precios actuales en batch
 */
async function batchGetPrices(symbols) {
  if (!symbols.length) return {};
  
  const prices = {};
  
  // Firestore limita 'in' a 10 valores, hacemos batch
  for (let i = 0; i < symbols.length; i += 10) {
    const batch = symbols.slice(i, i + 10);
    const snapshot = await db.collection('currentPrices')
      .where('__name__', 'in', batch)
      .get();
    
    snapshot.docs.forEach(doc => {
      prices[doc.id] = { symbol: doc.id, ...doc.data() };
    });
  }
  
  return prices;
}

/**
 * Obtiene cuentas del portafolio del usuario
 */
async function getPortfolioAccounts(userId) {
  const snapshot = await db.collection('portfolioAccounts')
    .where('userId', '==', userId)
    .where('isActive', '==', true)
    .get();
  
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
}

/**
 * Obtiene datos de ETFs en batch (con cache)
 * Optimizado para evitar rate limiting (429) y llamadas duplicadas
 */
async function batchGetETFData(symbols) {
  const etfData = new Map();
  const symbolsToFetch = new Set(); // Usar Set para deduplicar
  const cacheHits = [];

  // Verificar cache primero y deduplicar
  for (const symbol of symbols) {
    const normalized = symbol.trim().toUpperCase();
    const cached = etfDataCache.get(normalized);
    
    if (cached && Date.now() - cached.timestamp < ETF_CACHE_TTL) {
      // Solo usar cache si tiene datos válidos (holdings)
      if (cached.data && cached.data.holdings && cached.data.holdings.length > 0) {
        etfData.set(normalized, cached.data);
        cacheHits.push(normalized);
      } else {
        // Cache con datos inválidos, volver a buscar
        symbolsToFetch.add(normalized);
      }
    } else {
      symbolsToFetch.add(normalized);
    }
  }

  const uniqueSymbolsToFetch = Array.from(symbolsToFetch);
  
  logger.info('ETF batch fetch', { 
    totalRequests: symbols.length,
    uniqueSymbols: uniqueSymbolsToFetch.length,
    cacheHits: cacheHits.length, 
    toFetch: uniqueSymbolsToFetch
  });

  // Fetch con concurrencia limitada para evitar 429
  const CONCURRENT_LIMIT = 3;
  const results = await fetchWithConcurrencyLimit(
    uniqueSymbolsToFetch,
    fetchETFDataFromAPIWithRetry,
    CONCURRENT_LIMIT
  );

  // Procesar resultados
  for (const { symbol, data } of results) {
    if (data && data.holdings && data.holdings.length > 0) {
      etfDataCache.set(symbol, { data, timestamp: Date.now() });
      etfData.set(symbol, data);
    }
  }
  
  logger.info('ETF data loaded', { total: etfData.size, symbols: Array.from(etfData.keys()) });
  
  return etfData;
}

/**
 * Ejecuta fetches con límite de concurrencia
 */
async function fetchWithConcurrencyLimit(symbols, fetchFn, limit) {
  const results = [];
  
  for (let i = 0; i < symbols.length; i += limit) {
    const batch = symbols.slice(i, i + limit);
    const batchResults = await Promise.all(
      batch.map(async (symbol) => ({
        symbol,
        data: await fetchFn(symbol)
      }))
    );
    results.push(...batchResults);
    
    // Pequeña pausa entre batches para evitar rate limiting
    if (i + limit < symbols.length) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
  
  return results;
}

/**
 * Obtiene datos de un ETF con retry automático para errores 429
 */
async function fetchETFDataFromAPIWithRetry(symbol, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const result = await fetchETFDataFromAPI(symbol);
    
    // Si obtuvimos datos o es un error definitivo (no 429), retornar
    if (result !== 'RATE_LIMITED') {
      return result;
    }
    
    // Esperar antes de reintentar (backoff exponencial)
    const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
    logger.debug('ETF retry', { symbol, attempt, delay });
    await new Promise(resolve => setTimeout(resolve, delay));
  }
  
  logger.warn('ETF max retries exceeded', { symbol, maxRetries });
  return null;
}

/**
 * Obtiene datos de un ETF desde la API externa
 * Retorna 'RATE_LIMITED' si hay error 429 para permitir retry
 */
async function fetchETFDataFromAPI(symbol) {
  const url = `https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1/etf/${symbol}/unified`;
  
  try {
    const response = await fetch(url, { 
      timeout: 15000,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'PortfolioDistributionService/1.0'
      }
    });
    
    if (response.status === 429) {
      logger.debug('ETF API rate limited', { symbol });
      return 'RATE_LIMITED';
    }
    
    if (response.status === 204) {
      logger.debug('ETF API 204 No Content', { symbol });
      return null;
    }
    
    if (!response.ok) {
      logger.warn('ETF API error status', { symbol, status: response.status });
      return null;
    }
    
    const text = await response.text();
    if (!text || text.trim() === '') {
      logger.debug('ETF API empty response', { symbol });
      return null;
    }
    
    const data = JSON.parse(text);
    logger.debug('ETF API success', { 
      symbol, 
      holdingsCount: data.holdings?.length,
      sectorsCount: data.sectors?.length 
    });
    
    return data;
  } catch (error) {
    logger.warn('ETF API fetch error', { symbol, error: error.message, url });
    return null;
  }
}

/**
 * Obtiene mapeo de sectores desde Firestore (con cache)
 */
async function getSectorMappings() {
  if (sectorsCache && Date.now() - sectorsCacheTimestamp < SECTORS_CACHE_TTL) {
    return sectorsCache;
  }

  const snapshot = await db.collection('sectors').get();
  const mappings = {};
  
  snapshot.docs.forEach(doc => {
    const data = doc.data();
    if (data.etfSectorName && data.sector) {
      mappings[data.etfSectorName] = data.sector;
    }
    if (data.sector) {
      mappings[data.sector] = data.sector;
    }
  });

  sectorsCache = mappings;
  sectorsCacheTimestamp = Date.now();
  return mappings;
}

/**
 * Obtiene mapeo de países desde Firestore (con cache)
 */
async function getCountryMappings() {
  if (countriesCache && Date.now() - countriesCacheTimestamp < COUNTRIES_CACHE_TTL) {
    return countriesCache;
  }

  const snapshot = await db.collection('countries').get();
  const mappings = new Map();
  
  // Aliases comunes
  const countryAliases = {
    'united states': ['usa', 'us', 'u.s.', 'u.s.a.', 'america'],
    'united kingdom': ['uk', 'u.k.', 'great britain', 'britain', 'england'],
    'south korea': ['korea', 'republic of korea'],
    'taiwan': ['taiwan, province of china', 'chinese taipei'],
    'russia': ['russian federation'],
    'czech republic': ['czechia'],
    'hong kong': ['hong kong sar'],
    'macau': ['macao'],
  };

  snapshot.docs.forEach(doc => {
    const country = { id: doc.id, ...doc.data() };
    const countryLower = (country.country || '').toLowerCase();
    
    mappings.set(countryLower, country);
    
    if (country.name2) {
      mappings.set(country.name2.toLowerCase(), country);
    }
    
    const aliases = countryAliases[countryLower];
    if (aliases) {
      aliases.forEach(alias => mappings.set(alias.toLowerCase(), country));
    }
    
    if (country.code3) {
      mappings.set(country.code3.toLowerCase(), country);
    }
    if (country.code2) {
      mappings.set(country.code2.toLowerCase(), country);
    }
  });

  countriesCache = mappings;
  countriesCacheTimestamp = Date.now();
  return mappings;
}

/**
 * Obtiene las tasas de cambio de monedas activas (con cache)
 * FIX-CURRENCY-001: Necesario para convertir activos en monedas locales a USD
 */
async function getCurrencyRates() {
  if (currencyRatesCache && Date.now() - currencyRatesCacheTimestamp < CURRENCY_RATES_CACHE_TTL) {
    return currencyRatesCache;
  }

  const snapshot = await db.collection('currencies')
    .where('isActive', '==', true)
    .get();
  
  const rates = new Map();
  
  // USD siempre tiene tasa 1
  rates.set('USD', 1);
  
  snapshot.docs.forEach(doc => {
    const data = doc.data();
    if (data.code && data.exchangeRate) {
      // exchangeRate = cuántas unidades de moneda local por 1 USD
      // Ej: COP = 3707.47 significa 1 USD = 3707.47 COP
      rates.set(data.code, data.exchangeRate);
    }
  });

  currencyRatesCache = rates;
  currencyRatesCacheTimestamp = Date.now();
  
  logger.info('Currency rates loaded', { count: rates.size, currencies: Array.from(rates.keys()) });
  
  return rates;
}

/**
 * Convierte un valor de una moneda a USD
 * FIX-CURRENCY-001
 * @param {number} value - Valor en moneda local
 * @param {string} currency - Código de moneda (ej: 'COP', 'EUR', 'USD')
 * @param {Map} currencyRates - Mapa de tasas de cambio
 * @returns {number} Valor en USD
 */
function convertToUSD(value, currency, currencyRates) {
  if (!currency || currency === 'USD') {
    return value;
  }
  
  const rate = currencyRates.get(currency);
  if (!rate || rate === 0) {
    // Si no hay tasa de cambio, loguear advertencia y retornar el valor sin conversión
    logger.warn('No exchange rate found for currency', { currency, value });
    return value;
  }
  
  // rate = unidades de moneda local por 1 USD
  // Para convertir de moneda local a USD: value / rate
  return value / rate;
}

/**
 * Calcula el valor total del portafolio en USD
 * FIX-CURRENCY-001: Convierte todos los valores a USD antes de sumar
 */
function calculateTotalValue(assets, prices, currencyRates) {
  return assets.reduce((total, asset) => {
    const price = prices[asset.name];
    if (price && price.price) {
      const valueInLocalCurrency = asset.units * price.price;
      const currency = price.currency || 'USD';
      const valueInUSD = convertToUSD(valueInLocalCurrency, currency, currencyRates);
      return total + valueInUSD;
    }
    return total;
  }, 0);
}

/**
 * Calcula la distribución por sectores
 * FIX-CURRENCY-001: Usa currencyRates para convertir valores a USD
 */
function calculateSectorDistribution(assets, prices, etfData, sectorMappings, portfolioAccounts, userId, totalValue, currencyRates) {
  const holdingsMap = {};
  const sectorsMap = {};
  const etfStats = { decomposed: 0, notDecomposed: [] };

  // Filtrar assets relevantes
  const relevantAssets = assets.filter(asset => {
    if (!asset.isActive) return false;
    if (!asset.portfolioAccount) return true;
    const account = portfolioAccounts.find(acc => acc.id === asset.portfolioAccount);
    return account && account.isActive && account.userId === userId;
  });

  // Procesar holdings directos
  relevantAssets.forEach(asset => {
    const price = prices[asset.name];
    if (!price || !price.price) return;

    // FIX-CURRENCY-001: Convertir a USD antes de calcular peso
    const valueInLocalCurrency = asset.units * price.price;
    const currency = price.currency || 'USD';
    const valueInUSD = convertToUSD(valueInLocalCurrency, currency, currencyRates);
    const weight = valueInUSD / totalValue;

    // BUGFIX: Usar price.name (nombre de la empresa) en lugar de asset.company (broker)
    holdingsMap[asset.name] = {
      symbol: asset.name,
      description: price.name || asset.name, // price.name contiene el nombre real (ej: "Apple Inc.")
      weight,
      asset_type: asset.assetType,
      sector: price.sector,
      assetClass: price.sector,
      sources: [{ symbol: asset.name, contribution: weight }]
    };
  });

  // Procesar ETFs
  const etfAssets = relevantAssets.filter(asset => 
    asset.assetType === 'etf' || prices[asset.name]?.type === 'etf'
  );

  for (const etf of etfAssets) {
    const price = prices[etf.name];
    if (!price || !price.price) continue;

    // FIX-CURRENCY-001: Convertir a USD
    const valueInLocalCurrency = etf.units * price.price;
    const currency = price.currency || 'USD';
    const assetValueUSD = convertToUSD(valueInLocalCurrency, currency, currencyRates);
    const etfWeight = assetValueUSD / totalValue;
    const normalized = etf.name.trim().toUpperCase();
    const etfInfo = etfData.get(normalized);

    if (etfInfo && etfInfo.holdings && etfInfo.holdings.length > 0) {
      // BUGFIX: Eliminar el ETF del mapa de holdings ya que lo vamos a desglosar
      // Solo mostramos los holdings subyacentes, no el ETF en sí
      delete holdingsMap[etf.name];
      etfStats.decomposed++;
      
      // Procesar holdings del ETF
      for (const holding of etfInfo.holdings) {
        if (!holding.symbol && !holding.isin) continue;

        const identifier = holding.symbol || holding.isin;
        const contribution = (holding.weight || 0) * etfWeight;

        if (!holdingsMap[identifier]) {
          holdingsMap[identifier] = {
            symbol: holding.symbol || '',
            isin: holding.isin,
            description: holding.name,
            weight: 0,
            asset_type: holding.asset_type || 'stock',
            sources: []
          };
        }

        holdingsMap[identifier].weight += contribution;
        holdingsMap[identifier].sources.push({
          symbol: etf.name,
          contribution
        });
      }

      // Procesar sectores del ETF
      for (const sector of (etfInfo.sectors || [])) {
        const standardSector = sectorMappings[sector.name] || sector.name;
        if (!standardSector) continue;

        const contribution = (sector.weight || 0) * etfWeight;

        if (!sectorsMap[standardSector]) {
          sectorsMap[standardSector] = { sector: standardSector, weight: 0 };
        }
        sectorsMap[standardSector].weight += contribution;
      }
    } else {
      // Si no hay datos de ETF, el ETF permanece como holding directo
      etfStats.notDecomposed.push(etf.name);
    }
  }

  // Procesar stocks directos
  const stockAssets = relevantAssets.filter(asset =>
    asset.assetType === 'stock' || prices[asset.name]?.type === 'stock'
  );

  for (const stock of stockAssets) {
    const price = prices[stock.name];
    if (!price || !price.price || !price.sector) continue;

    // FIX-CURRENCY-001: Convertir a USD
    const valueInLocalCurrency = stock.units * price.price;
    const currency = price.currency || 'USD';
    const assetValueUSD = convertToUSD(valueInLocalCurrency, currency, currencyRates);
    const stockWeight = assetValueUSD / totalValue;
    const standardSector = sectorMappings[price.sector] || price.sector;

    if (!sectorsMap[standardSector]) {
      sectorsMap[standardSector] = { sector: standardSector, weight: 0 };
    }
    sectorsMap[standardSector].weight += stockWeight;
  }

  // Consolidar y ordenar
  const holdings = Object.values(holdingsMap)
    .map(h => ({
      ...h,
      sources: consolidateSources(h.sources)
    }))
    .sort((a, b) => b.weight - a.weight);

  const sectors = Object.values(sectorsMap)
    .sort((a, b) => b.weight - a.weight);

  return { holdings, sectors, etfStats };
}

/**
 * Consolida fuentes duplicadas
 */
function consolidateSources(sources) {
  const grouped = {};
  sources.forEach(source => {
    if (!grouped[source.symbol]) {
      grouped[source.symbol] = 0;
    }
    grouped[source.symbol] += source.contribution;
  });

  return Object.entries(grouped)
    .map(([symbol, contribution]) => ({ symbol, contribution }))
    .sort((a, b) => b.contribution - a.contribution);
}

/**
 * Calcula la distribución por países
 * 
 * SCALE-OPT-001: Usa codeNo (ISO 3166-1 numeric) para compatibilidad con TopoJSON
 * El mapa mundial usa códigos numéricos de 3 dígitos (ej: "840" para USA)
 * FIX-CURRENCY-001: Usa currencyRates para convertir valores a USD
 */
function calculateCountryDistribution(assets, prices, etfData, countryMappings, portfolioAccounts, userId, totalValue, currencyRates) {
  const countryMap = {};

  // Filtrar assets relevantes
  const relevantAssets = assets.filter(asset => {
    if (!asset.isActive) return false;
    if (!asset.portfolioAccount) return true;
    const account = portfolioAccounts.find(acc => acc.id === asset.portfolioAccount);
    return account && account.isActive && account.userId === userId;
  });

  // Procesar assets directos por país
  relevantAssets.forEach(asset => {
    const price = prices[asset.name];
    if (!price || !price.price) return;

    // FIX-CURRENCY-001: Convertir a USD
    const valueInLocalCurrency = asset.units * price.price;
    const currency = price.currency || 'USD';
    const valueInUSD = convertToUSD(valueInLocalCurrency, currency, currencyRates);
    const percentage = (valueInUSD / totalValue) * 100;

    // Obtener país del precio o del asset
    let countryName = price.country || asset.country;
    
    if (countryName) {
      const country = countryMappings.get(countryName.toLowerCase());
      if (country) {
        // SCALE-OPT-001: Usar codeNo para compatibilidad con TopoJSON del mapa mundial
        const countryId = country.codeNo || country.id;
        
        if (!countryMap[countryId]) {
          countryMap[countryId] = {
            id: countryId,
            name: country.country || countryName,
            value: 0,
            percentage: 0,
            assets: []
          };
        }

        // FIX-CURRENCY-001: Usar valueInUSD en lugar de value
        countryMap[countryId].value += valueInUSD;
        countryMap[countryId].percentage += percentage;
        // SCALE-OPT-001: Usar price.name para el nombre de la empresa, no asset.company (que es el broker)
        countryMap[countryId].assets.push({
          symbol: asset.name,
          name: price.name || asset.name,
          value: valueInUSD,
          percentage,
          assetType: asset.assetType,
          accountId: asset.portfolioAccount
        });
      }
    }
  });

  // Procesar ETFs con datos de países
  const etfAssets = relevantAssets.filter(asset =>
    asset.assetType === 'etf' || prices[asset.name]?.type === 'etf'
  );

  for (const etf of etfAssets) {
    const price = prices[etf.name];
    if (!price || !price.price) continue;

    // FIX-CURRENCY-001: Convertir ETF a USD
    const valueInLocalCurrency = etf.units * price.price;
    const currency = price.currency || 'USD';
    const assetValueUSD = convertToUSD(valueInLocalCurrency, currency, currencyRates);
    const etfWeight = assetValueUSD / totalValue;
    const normalized = etf.name.trim().toUpperCase();
    const etfInfo = etfData.get(normalized);

    if (etfInfo && etfInfo.countries) {
      for (const countryData of etfInfo.countries) {
        const countryName = countryData.name;
        if (!countryName) continue;

        const country = countryMappings.get(countryName.toLowerCase());
        if (country) {
          // SCALE-OPT-001: Usar codeNo para compatibilidad con TopoJSON del mapa mundial
          const countryId = country.codeNo || country.id;
          const contribution = (countryData.weight || 0) * etfWeight;
          const valueContribution = contribution * totalValue;
          const percentageContribution = contribution * 100;

          if (!countryMap[countryId]) {
            countryMap[countryId] = {
              id: countryId,
              name: country.country || countryName,
              value: 0,
              percentage: 0,
              assets: []
            };
          }

          countryMap[countryId].value += valueContribution;
          countryMap[countryId].percentage += percentageContribution;
          
          // SCALE-OPT-001: Agregar ETF como asset contribuyente al país
          // Evitar duplicados agrupando por símbolo y cuenta
          const assetKey = `${etf.name}-${etf.portfolioAccount || 'default'}`;
          const existingEtfAsset = countryMap[countryId].assets.find(a => 
            a.symbol === etf.name && a.accountId === etf.portfolioAccount
          );
          if (existingEtfAsset) {
            existingEtfAsset.value += valueContribution;
            existingEtfAsset.percentage += percentageContribution;
          } else {
            countryMap[countryId].assets.push({
              symbol: etf.name,
              // SCALE-OPT-001: Usar price.name para el nombre, no company (que es broker)
              name: price.name || etf.name,
              value: valueContribution,
              percentage: percentageContribution,
              assetType: 'etf',
              isFromHolding: true,
              accountId: etf.portfolioAccount
            });
          }
        }
      }
    }
  }

  // SCALE-OPT-001: Agrupar assets por símbolo dentro de cada país para evitar duplicados
  // Esto consolida activos del mismo ticker que están en diferentes cuentas
  const result = Object.values(countryMap).map(country => {
    const assetsMap = {};
    
    country.assets.forEach(asset => {
      const symbol = asset.symbol;
      if (!assetsMap[symbol]) {
        assetsMap[symbol] = {
          symbol,
          name: asset.name,
          value: 0,
          percentage: 0,
          assetType: asset.assetType,
          accountCount: 0
        };
      }
      assetsMap[symbol].value += asset.value;
      assetsMap[symbol].percentage += asset.percentage;
      assetsMap[symbol].accountCount += 1;
    });
    
    return {
      ...country,
      assets: Object.values(assetsMap).sort((a, b) => b.percentage - a.percentage)
    };
  });

  return result.sort((a, b) => b.value - a.value);
}

/**
 * Calcula activos sin ubicación geográfica (cryptos, bonos, etc.)
 * SCALE-OPT-001: Agrupa activos por símbolo para evitar duplicados
 * FIX-CURRENCY-001: Usa currencyRates para convertir valores a USD
 */
function calculateNonGeographicAssets(assets, prices, etfData, countryMappings, portfolioAccounts, userId, totalValue, currencyRates) {
  // Mapa para agrupar por símbolo
  const nonGeoAssetsMap = {};

  // Filtrar assets relevantes
  const relevantAssets = assets.filter(asset => {
    if (!asset.isActive) return false;
    if (!asset.portfolioAccount) return true;
    const account = portfolioAccounts.find(acc => acc.id === asset.portfolioAccount);
    return account && account.isActive && account.userId === userId;
  });

  relevantAssets.forEach(asset => {
    const price = prices[asset.name];
    if (!price || !price.price) return;

    // FIX-CURRENCY-001: Convertir a USD
    const valueInLocalCurrency = asset.units * price.price;
    const currency = price.currency || 'USD';
    const valueInUSD = convertToUSD(valueInLocalCurrency, currency, currencyRates);
    const percentage = (valueInUSD / totalValue) * 100;
    
    // Determinar si tiene ubicación geográfica
    let hasGeoLocation = false;
    
    // 1. Verificar si tiene país directo
    const countryName = price.country || asset.country;
    if (countryName) {
      const country = countryMappings.get(countryName.toLowerCase());
      if (country) {
        hasGeoLocation = true;
      }
    }
    
    // 2. Si es ETF, verificar si tiene datos de países
    if (!hasGeoLocation && (asset.assetType === 'etf' || price.type === 'etf')) {
      const normalized = asset.name.trim().toUpperCase();
      const etfInfo = etfData.get(normalized);
      if (etfInfo && etfInfo.countries && etfInfo.countries.length > 0) {
        hasGeoLocation = true;
      }
    }
    
    // Si no tiene ubicación geográfica, agregarlo al mapa (agrupado por símbolo)
    if (!hasGeoLocation) {
      const symbol = asset.name;
      if (!nonGeoAssetsMap[symbol]) {
        nonGeoAssetsMap[symbol] = {
          symbol,
          name: price.name || asset.name,
          value: 0,
          percentage: 0,
          assetType: asset.assetType || price.type || 'unknown',
          isMultiCountry: false,
          accountCount: 0 // Cuántas cuentas tienen este activo
        };
      }
      nonGeoAssetsMap[symbol].value += valueInUSD;
      nonGeoAssetsMap[symbol].percentage += percentage;
      nonGeoAssetsMap[symbol].accountCount += 1;
    }
  });

  // Convertir mapa a array
  const nonGeoAssets = Object.values(nonGeoAssetsMap);

  // Calcular total y distribución por tipo
  const totalPercentage = nonGeoAssets.reduce((sum, a) => sum + a.percentage, 0);
  
  const typeDistribution = {};
  nonGeoAssets.forEach(asset => {
    const type = asset.assetType || 'unknown';
    if (!typeDistribution[type]) {
      typeDistribution[type] = { type, percentage: 0, count: 0 };
    }
    typeDistribution[type].percentage += asset.percentage;
    typeDistribution[type].count += 1;
  });

  return {
    totalPercentage,
    assets: nonGeoAssets.sort((a, b) => b.percentage - a.percentage),
    assetTypeDistribution: Object.values(typeDistribution).sort((a, b) => b.percentage - a.percentage)
  };
}

/**
 * Invalida el cache de distribución para un usuario
 * Actualiza también portfolioLastModified en Firestore para invalidación entre instancias
 * @param {string} userId - ID del usuario
 */
function invalidateDistributionCache(userId) {
  // 1. Invalidar cache en memoria local
  const keysToDelete = [];
  
  for (const key of distributionCache.keys()) {
    if (key.startsWith(`dist:${userId}`)) {
      keysToDelete.push(key);
    }
  }
  
  keysToDelete.forEach(key => distributionCache.delete(key));
  
  // 2. Actualizar timestamp en Firestore (para invalidar entre instancias)
  // Esto se hace de forma asíncrona sin esperar (fire-and-forget)
  const admin = require('firebase-admin');
  admin.firestore().collection('users').doc(userId).set({
    portfolioLastModified: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true }).catch(err => {
    logger.warn('Failed to update portfolioLastModified', { userId, error: err.message });
  });
  
  logger.info('Distribution cache invalidated', { 
    userId, 
    keysInvalidated: keysToDelete.length 
  });
}

/**
 * Obtiene sectores disponibles
 */
async function getAvailableSectors() {
  const mappings = await getSectorMappings();
  return [...new Set(Object.values(mappings))];
}

module.exports = {
  getPortfolioDistribution,
  invalidateDistributionCache,
  getAvailableSectors
};
