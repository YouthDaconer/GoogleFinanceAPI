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
  
  // Verificar cache
  const cached = distributionCache.get(cacheKey);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    logger.info('Cache hit for distribution', { userId, cacheKey });
    return { ...cached.data, fromCache: true };
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

    // 5. Obtener mapeo de sectores
    const sectorMappings = await getSectorMappings();

    // 6. Obtener mapeo de países
    const countryMappings = await getCountryMappings();

    // 7. Calcular valor total del portafolio
    const totalPortfolioValue = calculateTotalValue(assets, prices);
    
    if (totalPortfolioValue === 0) {
      return buildEmptyResponse(options.currency);
    }

    // 8. Calcular distribuciones
    const { holdings, sectors } = calculateSectorDistribution(
      assets, prices, etfData, sectorMappings, portfolioAccounts, userId, totalPortfolioValue
    );

    const countries = calculateCountryDistribution(
      assets, prices, etfData, countryMappings, portfolioAccounts, userId, totalPortfolioValue
    );

    // 9. Construir respuesta
    const result = {
      sectors: sectors.map(s => ({
        sector: s.sector,
        weight: s.weight,
        percentage: s.weight * 100
      })),
      countries: countries.map(c => ({
        id: c.id,
        name: c.name,
        value: c.value,
        percentage: c.percentage,
        assets: c.assets || []
      })),
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
        etfDataLoaded: etfData.size
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
 */
async function getActiveAssets(userId, options) {
  let query = db.collection('assets')
    .where('userId', '==', userId)
    .where('isActive', '==', true);

  // Filtrar por cuenta específica si se proporciona
  if (options.accountId && options.accountId !== 'overall') {
    if (options.accountId === 'account_null') {
      query = query.where('portfolioAccount', '==', null);
    } else {
      query = query.where('portfolioAccount', '==', options.accountId);
    }
  } else if (options.accountIds?.length) {
    // Firestore permite máximo 10 valores en 'in'
    query = query.where('portfolioAccount', 'in', options.accountIds.slice(0, 10));
  }

  const snapshot = await query.get();
  return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
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
 */
async function batchGetETFData(symbols) {
  const etfData = new Map();
  const symbolsToFetch = [];

  // Verificar cache primero
  for (const symbol of symbols) {
    const normalized = symbol.trim().toUpperCase();
    const cached = etfDataCache.get(normalized);
    
    if (cached && Date.now() - cached.timestamp < ETF_CACHE_TTL) {
      etfData.set(normalized, cached.data);
    } else {
      symbolsToFetch.push(normalized);
    }
  }

  // Fetch símbolos que no están en cache
  const fetchPromises = symbolsToFetch.map(async (symbol) => {
    try {
      const data = await fetchETFDataFromAPI(symbol);
      if (data) {
        etfDataCache.set(symbol, { data, timestamp: Date.now() });
        etfData.set(symbol, data);
      }
    } catch (error) {
      logger.warn('Failed to fetch ETF data', { symbol, error: error.message });
    }
  });

  await Promise.all(fetchPromises);
  return etfData;
}

/**
 * Obtiene datos de un ETF desde la API externa
 */
async function fetchETFDataFromAPI(symbol) {
  try {
    const response = await fetch(
      `https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1/etf/${symbol}/unified`,
      { timeout: 10000 }
    );
    
    if (!response.ok || response.status === 204) {
      return null;
    }
    
    const text = await response.text();
    if (!text || text.trim() === '') {
      return null;
    }
    
    return JSON.parse(text);
  } catch (error) {
    logger.warn('ETF API error', { symbol, error: error.message });
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
 * Calcula el valor total del portafolio
 */
function calculateTotalValue(assets, prices) {
  return assets.reduce((total, asset) => {
    const price = prices[asset.name];
    if (price && price.price) {
      return total + (asset.units * price.price);
    }
    return total;
  }, 0);
}

/**
 * Calcula la distribución por sectores
 */
function calculateSectorDistribution(assets, prices, etfData, sectorMappings, portfolioAccounts, userId, totalValue) {
  const holdingsMap = {};
  const sectorsMap = {};

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

    const value = asset.units * price.price;
    const weight = value / totalValue;

    holdingsMap[asset.name] = {
      symbol: asset.name,
      description: asset.company || asset.name,
      weight,
      asset_type: asset.assetType,
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

    const assetValue = etf.units * price.price;
    const etfWeight = assetValue / totalValue;
    const normalized = etf.name.trim().toUpperCase();
    const etfInfo = etfData.get(normalized);

    if (etfInfo) {
      // Procesar holdings del ETF
      for (const holding of (etfInfo.holdings || [])) {
        if (!holding.symbol && !holding.isin) continue;

        const identifier = holding.symbol || holding.isin;
        const contribution = (holding.weight || 0) * etfWeight;

        if (!holdingsMap[identifier]) {
          holdingsMap[identifier] = {
            symbol: holding.symbol || '',
            isin: holding.isin,
            description: holding.name,
            weight: 0,
            asset_type: holding.asset_type,
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
    }
  }

  // Procesar stocks directos
  const stockAssets = relevantAssets.filter(asset =>
    asset.assetType === 'stock' || prices[asset.name]?.type === 'stock'
  );

  for (const stock of stockAssets) {
    const price = prices[stock.name];
    if (!price || !price.price || !price.sector) continue;

    const assetValue = stock.units * price.price;
    const stockWeight = assetValue / totalValue;
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

  return { holdings, sectors };
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
 */
function calculateCountryDistribution(assets, prices, etfData, countryMappings, portfolioAccounts, userId, totalValue) {
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

    const value = asset.units * price.price;
    const percentage = (value / totalValue) * 100;

    // Obtener país del precio o del asset
    let countryName = price.country || asset.country;
    
    if (countryName) {
      const country = countryMappings.get(countryName.toLowerCase());
      if (country) {
        const countryId = country.code2 || country.id;
        
        if (!countryMap[countryId]) {
          countryMap[countryId] = {
            id: countryId,
            name: country.country || countryName,
            value: 0,
            percentage: 0,
            assets: []
          };
        }

        countryMap[countryId].value += value;
        countryMap[countryId].percentage += percentage;
        countryMap[countryId].assets.push({
          symbol: asset.name,
          name: asset.company || asset.name,
          value,
          percentage,
          assetType: asset.assetType
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

    const assetValue = etf.units * price.price;
    const etfWeight = assetValue / totalValue;
    const normalized = etf.name.trim().toUpperCase();
    const etfInfo = etfData.get(normalized);

    if (etfInfo && etfInfo.countries) {
      for (const countryData of etfInfo.countries) {
        const countryName = countryData.name;
        if (!countryName) continue;

        const country = countryMappings.get(countryName.toLowerCase());
        if (country) {
          const countryId = country.code2 || country.id;
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
        }
      }
    }
  }

  return Object.values(countryMap)
    .sort((a, b) => b.value - a.value);
}

/**
 * Invalida el cache de distribución para un usuario
 * @param {string} userId - ID del usuario
 */
function invalidateDistributionCache(userId) {
  const keysToDelete = [];
  
  for (const key of distributionCache.keys()) {
    if (key.startsWith(`dist:${userId}`)) {
      keysToDelete.push(key);
    }
  }
  
  keysToDelete.forEach(key => distributionCache.delete(key));
  
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
