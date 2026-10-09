/**
 * Portfolio Distribution Service
 * 
 * Servicio para calcular distribución del portafolio (sectores, países, holdings).
 * Migrado desde usePortfolioDistribution.ts y useCountriesDistribution.ts del frontend.
 * 
 * OPT-DEMAND-SECTOR: Migrado para usar API Lambda on-demand en lugar de colección currentPrices
 * 
 * @see SCALE-OPT-001 - Migración de Cálculos Frontend → Backend (SOLID)
 * @see docs/architecture/on-demand-pricing-architecture.md
 */

const admin = require('./firebaseAdmin');
const db = admin.firestore();
const { StructuredLogger } = require('../utils/logger');
// FIX-FETCH-001: Usar fetch nativo de Node.js 18+ en lugar de node-fetch
// node-fetch no está en package.json como dependencia directa
// const fetch = require('node-fetch');

// SEC-CF-001: Configuración centralizada de URLs y headers
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('./config');
// OPT-DEMAND-SECTOR: Importar servicio de financeQuery para precios on-demand
const { getQuotes } = require('./financeQuery');

const logger = new StructuredLogger('PortfolioDistributionService');

// Cache en memoria con TTL de 5 minutos
const distributionCache = new Map();
const CACHE_TTL = 5 * 60 * 1000;

// Cache para datos de ETFs (más largo TTL ya que cambian poco)
const etfDataCache = new Map();
const ETF_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 horas

// FIX-ETF-EU-001: Cobertura mínima de holdings para considerar un ETF
// completamente desagregado.
// Fuentes como etf.com publican la cartera completa (~99.9%), pero Yahoo y
// justETF solo publican el top-10 (VUAA.L ≈ 36%, VWCE.DE ≈ 23%). Si se
// desagregara ese top-10 y se descartara el ETF, el resto del fondo
// desaparecería del treemap y los pesos dejarían de sumar 100%.
// Por debajo del umbral se conserva el remanente como posición del propio ETF.
const ETF_HOLDINGS_COVERAGE_THRESHOLD = 0.97;

// FIX-ETF-EU-001: Timeout de cada llamada a /v1/etf/{symbol}/unified.
// La cadena de fuentes del API tarda ~18s en frío para un UCITS europeo.
// Cota superior: los símbolos se piden en lotes de CONCURRENT_LIMIT en paralelo,
// así que el coste es (nº de lotes × el símbolo más lento del lote), no la suma.
// Con 2 lotes son 50s como máximo, dentro de los 60s de timeout de queryOperations.
const ETF_API_TIMEOUT_MS = 25000;

// Cache para sectores (raramente cambian)
let sectorsCache = null;
let sectorsCacheTimestamp = 0;
const SECTORS_CACHE_TTL = 60 * 60 * 1000; // 1 hora

// Cache para países
let countriesCache = null;
let countriesCacheTimestamp = 0;
// OPT-FIRESTORE-002: Aumentado de 1h a 24h. Los países son datos 100% estáticos.
// Reduce de ~23 lecturas/día a ~1-2 (solo cold-starts).
const COUNTRIES_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 horas

// Cache para tasas de cambio de monedas
let currencyRatesCache = null;
let currencyRatesCacheTimestamp = 0;
const CURRENCY_RATES_CACHE_TTL = 15 * 60 * 1000; // 15 minutos

// ============================================================================
// SECTOR-HOMOLOGATION: Mapeo de sectores externos a sectores estándar
// Los sectores estándar están definidos en la colección 'sectors' de Firestore
// Este mapeo normaliza nombres de sectores de diversas fuentes (APIs de ETFs,
// finance-query, etc.) a los 11 sectores estándar del sistema.
// ============================================================================
const EXTERNAL_SECTOR_MAPPINGS = {
  // === Technology ===
  'Electronic Technology': 'Technology',
  'Technology Services': 'Technology',
  'INFORMATION TECHNOLOGY': 'Technology',
  'Information Technology': 'Technology',
  'Tech': 'Technology',
  'IT': 'Technology',
  
  // === Financial Services ===
  'Finance': 'Financial Services',
  'FINANCIALS': 'Financial Services',
  'Financials': 'Financial Services',
  'Banking': 'Financial Services',
  'Insurance': 'Financial Services',
  'Investment Services': 'Financial Services',
  
  // === Healthcare ===
  'Health Technology': 'Healthcare',
  'Health Services': 'Healthcare',
  'HEALTH CARE': 'Healthcare',
  'Health Care': 'Healthcare',
  'Medical': 'Healthcare',
  'Pharmaceuticals': 'Healthcare',
  'Biotechnology': 'Healthcare',
  
  // === Consumer Cyclical ===
  'Consumer Durables': 'Consumer Cyclical',
  'Consumer Cyclicals': 'Consumer Cyclical', // justETF
  'Consumer Services': 'Consumer Cyclical',
  'Retail Trade': 'Consumer Cyclical',
  'CONSUMER DISCRETIONARY': 'Consumer Cyclical',
  'Consumer Discretionary': 'Consumer Cyclical',
  'Leisure': 'Consumer Cyclical',
  'Apparel': 'Consumer Cyclical',
  'Automotive': 'Consumer Cyclical',
  'Hotels': 'Consumer Cyclical',
  'Restaurants': 'Consumer Cyclical',
  
  // === Consumer Defensive ===
  'Consumer Non-Durables': 'Consumer Defensive',
  'Consumer Non-Cyclicals': 'Consumer Defensive', // justETF
  'Consumer Non-Cyclical': 'Consumer Defensive',
  'CONSUMER STAPLES': 'Consumer Defensive',
  'Consumer Staples': 'Consumer Defensive',
  'Food': 'Consumer Defensive',
  'Beverages': 'Consumer Defensive',
  'Tobacco': 'Consumer Defensive',
  'Household Products': 'Consumer Defensive',
  
  // === Industrials ===
  'Producer Manufacturing': 'Industrials',
  'Transportation': 'Industrials',
  'Commercial Services': 'Industrials',
  'Industrial Services': 'Industrials',
  'INDUSTRIALS': 'Industrials',
  'Aerospace': 'Industrials',
  'Defense': 'Industrials',
  'Machinery': 'Industrials',
  'Construction': 'Industrials',
  'Engineering': 'Industrials',
  
  // === Basic Materials ===
  'Non-Energy Minerals': 'Basic Materials',
  'MATERIALS': 'Basic Materials',
  'Materials': 'Basic Materials',
  'Chemicals': 'Basic Materials',
  'Mining': 'Basic Materials',
  'Metals': 'Basic Materials',
  'Paper': 'Basic Materials',
  'Forest Products': 'Basic Materials',
  
  // === Communication Services ===
  'COMMUNICATION SERVICES': 'Communication Services',
  'Communications': 'Communication Services',
  'Telecommunications': 'Communication Services',
  'Media': 'Communication Services',
  'Entertainment': 'Communication Services',
  'Interactive Media': 'Communication Services',
  
  // === Energy ===
  'ENERGY': 'Energy',
  'Oil & Gas': 'Energy',
  'Oil': 'Energy',
  'Gas': 'Energy',
  'Petroleum': 'Energy',
  'Energy Minerals': 'Energy',
  
  // === Utilities ===
  'UTILITIES': 'Utilities',
  'Electric Utilities': 'Utilities',
  'Water Utilities': 'Utilities',
  'Gas Utilities': 'Utilities',
  'Power': 'Utilities',
  
  // === Real Estate ===
  'REAL ESTATE': 'Real Estate',
  'REITs': 'Real Estate',
  'Property': 'Real Estate',
  'Real Estate Services': 'Real Estate',
  
  // === Other (sectores que no encajan en las categorías estándar) ===
  'Government': 'Other',
  'CASH': 'Other',
  'Cash': 'Other',
  'Miscellaneous': 'Other',
  'Other': 'Other',
  'Unknown': 'Other',
  'N/A': 'Other',
  'Unclassified': 'Other',
  'Diversified': 'Other',
};

/**
 * Normaliza un nombre de sector a los sectores estándar del sistema
 * @param {string} sectorName - Nombre del sector a normalizar
 * @param {Object} firestoreMappings - Mapeos adicionales desde Firestore
 * @returns {string} Nombre del sector estándar
 */
function normalizeSectorName(sectorName, firestoreMappings = {}) {
  if (!sectorName || typeof sectorName !== 'string') {
    return 'Other';
  }
  
  const trimmedSector = sectorName.trim();
  
  // 1. Verificar si ya es un sector estándar
  const standardSectors = [
    'Basic Materials', 'Communication Services', 'Consumer Cyclical',
    'Consumer Defensive', 'Energy', 'Financial Services', 'Healthcare',
    'Industrials', 'Real Estate', 'Technology', 'Utilities', 'Other'
  ];
  
  if (standardSectors.includes(trimmedSector)) {
    return trimmedSector;
  }
  
  // 2. Buscar en mapeos de Firestore (etfSectorName)
  if (firestoreMappings[trimmedSector]) {
    return firestoreMappings[trimmedSector];
  }
  
  // 3. Buscar en mapeos externos estáticos
  if (EXTERNAL_SECTOR_MAPPINGS[trimmedSector]) {
    return EXTERNAL_SECTOR_MAPPINGS[trimmedSector];
  }
  
  // 4. Búsqueda case-insensitive
  const upperSector = trimmedSector.toUpperCase();
  for (const [key, value] of Object.entries(EXTERNAL_SECTOR_MAPPINGS)) {
    if (key.toUpperCase() === upperSector) {
      return value;
    }
  }
  
  // 5. Búsqueda parcial (contiene)
  const lowerSector = trimmedSector.toLowerCase();
  if (lowerSector.includes('tech')) return 'Technology';
  if (lowerSector.includes('financ') || lowerSector.includes('bank')) return 'Financial Services';
  if (lowerSector.includes('health') || lowerSector.includes('pharma') || lowerSector.includes('bio')) return 'Healthcare';
  if (lowerSector.includes('consumer') && lowerSector.includes('discret')) return 'Consumer Cyclical';
  if (lowerSector.includes('consumer') && lowerSector.includes('staple')) return 'Consumer Defensive';
  if (lowerSector.includes('industr') || lowerSector.includes('manufact')) return 'Industrials';
  if (lowerSector.includes('material') || lowerSector.includes('mineral') || lowerSector.includes('metal')) return 'Basic Materials';
  if (lowerSector.includes('commun') || lowerSector.includes('telecom') || lowerSector.includes('media')) return 'Communication Services';
  if (lowerSector.includes('energy') || lowerSector.includes('oil') || lowerSector.includes('gas') || lowerSector.includes('petrol')) return 'Energy';
  if (lowerSector.includes('utilit') || lowerSector.includes('electric') || lowerSector.includes('power')) return 'Utilities';
  if (lowerSector.includes('real estate') || lowerSector.includes('reit') || lowerSector.includes('property')) return 'Real Estate';
  
  // 6. Si no se encuentra mapeo, loguear y retornar 'Other'
  logger.warn('Unmapped sector found', { originalSector: sectorName });
  return 'Other';
}

/**
 * Sanitiza valores numéricos para evitar NaN/Infinity que no se pueden serializar a JSON
 * @param {any} value - Valor a sanitizar
 * @param {number} defaultValue - Valor por defecto si es inválido
 * @returns {number} Valor sanitizado
 */
function sanitizeNumber(value, defaultValue = 0) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return defaultValue;
  }
  return value;
}

/**
 * Sanitiza recursivamente un objeto para eliminar valores NaN/Infinity
 * @param {any} obj - Objeto a sanitizar
 * @returns {any} Objeto sanitizado
 */
function sanitizeForJSON(obj) {
  if (obj === null || obj === undefined) {
    return obj;
  }
  
  if (typeof obj === 'number') {
    return sanitizeNumber(obj, 0);
  }
  
  if (Array.isArray(obj)) {
    return obj.map(item => sanitizeForJSON(item));
  }
  
  if (typeof obj === 'object') {
    const sanitized = {};
    for (const [key, value] of Object.entries(obj)) {
      sanitized[key] = sanitizeForJSON(value);
    }
    return sanitized;
  }
  
  return obj;
}

/**
 * Obtiene la distribución del portafolio (sectores, países, holdings)
 * @param {string} userId - ID del usuario
 * @param {Object} options - Opciones de consulta
 * @param {string[]} [options.accountIds] - IDs de cuentas específicas (opcional)
 * @param {string} [options.accountId] - ID de cuenta específica (opcional)
 * @param {string} [options.currency] - Moneda de presentación (default: USD)
 * @param {boolean} [options.includeHoldings] - Incluir holdings detallados
 * @param {boolean} [options.forceRefresh] - Forzar recarga ignorando cache
 * @returns {Promise<Object>} Distribución del portafolio
 */
async function getPortfolioDistribution(userId, options = {}) {
  const startTime = Date.now();
  const cacheKey = buildCacheKey(userId, options);
  
  // FIX-DIST-001: Si forceRefresh, invalidar cache para esta key
  if (options.forceRefresh) {
    distributionCache.delete(cacheKey);
    etfDataCache.clear(); // También limpiar cache de ETFs
    logger.info('Force refresh requested, cache cleared', { userId, cacheKey });
  }
  
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
        // Sanitizar datos del cache para evitar NaN/Infinity
        const sanitizedData = sanitizeForJSON(cached.data);
        return { ...sanitizedData, metadata: { ...sanitizedData.metadata, fromCache: true } };
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
        etfNotDecomposed: etfStats.notDecomposed,
        // FIX-ETF-EU-001: ETFs cuya fuente sólo publica el top-10 de la cartera.
        // El resto se conserva como posición del propio ETF en `holdings`.
        etfPartiallyDecomposed: Array.from(etfStats.partiallyDecomposed),
        etfHoldingsCoverage: etfStats.coverage
      }
    };

    // FIX-DIST-002: Solo cachear si cargamos ETF data correctamente
    // Si hay ETFs pero no se cargaron datos, no cachear el resultado degradado
    const hasFailedETFLoading = etfSymbols.length > 0 && etfData.size === 0;
    
    if (!hasFailedETFLoading) {
      // Guardar en cache solo si ETF loading fue exitoso o no hay ETFs
      distributionCache.set(cacheKey, { data: result, timestamp: Date.now() });
      logger.info('Distribution calculated and cached', {
        userId,
        duration: Date.now() - startTime,
        assetCount: assets.length,
        etfDataLoaded: etfData.size
      });
    } else {
      logger.warn('Distribution calculated but NOT cached (ETF loading failed)', {
        userId,
        etfCount: etfSymbols.length,
        etfDataLoaded: etfData.size
      });
    }
    
    logger.info('Distribution calculated', {
      userId,
      duration: Date.now() - startTime,
      assetCount: assets.length,
      sectorsCount: sectors.length,
      countriesCount: countries.length
    });

    // Sanitizar resultado para evitar NaN/Infinity que rompen JSON serialization
    return sanitizeForJSON(result);
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
 * OPT-DEMAND-SECTOR: Obtiene precios actuales desde API Lambda on-demand
 * 
 * Migrado desde Firestore (colección currentPrices) para cumplir con arquitectura on-demand.
 * Usa getQuotesWithFallback que tiene circuit breaker y fallback a cache.
 * 
 * @param {string[]} symbols - Lista de símbolos a consultar
 * @returns {Promise<Object>} Mapa de symbol -> precio/datos
 */
async function batchGetPrices(symbols) {
  if (!symbols.length) return {};
  
  const prices = {};
  
  try {
    // Llamar al API Lambda con todos los símbolos de una vez
    const symbolsString = symbols.join(',');
    logger.info('Fetching prices from API Lambda', { 
      symbolCount: symbols.length,
      source: 'on-demand'
    });
    
    const apiResponse = await getQuotes(symbolsString);
    
    // El API retorna un objeto con los símbolos como keys
    if (apiResponse && typeof apiResponse === 'object') {
      // Puede venir como { AAPL: {...}, MSFT: {...} } o como array
      if (Array.isArray(apiResponse)) {
        // Formato array
        apiResponse.forEach(quote => {
          if (quote && quote.symbol) {
            // OPT-DEMAND-SECTOR: Convertir precio de string a número (strip commas for prices like "5,784.54")
            const priceValue = parseFloat(String(quote.price).replace(/,/g, '')) || parseFloat(String(quote.regularMarketPrice).replace(/,/g, '')) || 0;
            prices[quote.symbol] = {
              symbol: quote.symbol,
              price: priceValue,
              name: quote.name || quote.shortName,
              sector: quote.sector,
              industry: quote.industry,
              type: quote.type || quote.quoteType,
              logo: quote.logo,
              currency: quote.currency,
              country: quote.country,
              exchange: quote.exchange,
              // Campos adicionales para compatibilidad
              regularMarketPrice: priceValue,
              regularMarketChange: parseFloat(String(quote.change).replace(/,/g, '')) || parseFloat(String(quote.regularMarketChange).replace(/,/g, '')) || 0,
              regularMarketChangePercent: parseFloat(String(quote.changePercent || quote.regularMarketChangePercent || '0').replace(/[%,]/g, '')) || 0,
            };
          }
        });
      } else {
        // Formato objeto { AAPL: {...}, MSFT: {...} }
        Object.entries(apiResponse).forEach(([symbol, quote]) => {
          if (quote && symbol) {
            // OPT-DEMAND-SECTOR: Convertir precio de string a número (strip commas for prices like "5,784.54")
            const priceValue = parseFloat(String(quote.price).replace(/,/g, '')) || parseFloat(String(quote.regularMarketPrice).replace(/,/g, '')) || 0;
            prices[symbol] = {
              symbol,
              price: priceValue,
              name: quote.name || quote.shortName,
              sector: quote.sector,
              industry: quote.industry,
              type: quote.type || quote.quoteType,
              logo: quote.logo,
              currency: quote.currency,
              country: quote.country,
              exchange: quote.exchange,
              // Campos adicionales para compatibilidad
              regularMarketPrice: priceValue,
              regularMarketChange: parseFloat(String(quote.change).replace(/,/g, '')) || parseFloat(String(quote.regularMarketChange).replace(/,/g, '')) || 0,
              regularMarketChangePercent: parseFloat(String(quote.changePercent || quote.regularMarketChangePercent || '0').replace(/[%,]/g, '')) || 0,
            };
          }
        });
      }
    }
    
    logger.info('Prices fetched successfully', { 
      requested: symbols.length,
      received: Object.keys(prices).length,
      source: 'api-lambda'
    });
    
  } catch (error) {
    // OPT-DEMAND-CLEANUP: NO hay fallback a Firestore
    // Si el API falla, re-lanzar el error para que el caller lo maneje
    // Ver: docs/architecture/OPT-DEMAND-CLEANUP-firestore-fallback-removal.md
    logger.error('Error fetching prices from API Lambda - NO FALLBACK', { 
      error: error.message,
      symbolCount: symbols.length
    });
    
    // Re-throw para que el servicio que llama pueda manejar el error
    throw new Error(`Failed to fetch prices from API: ${error.message}`);
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
 * FIX-ETF-EU-001: Determina si la respuesta del API de ETFs es aprovechable.
 *
 * Antes se exigían holdings; eso descartaba respuestas que sí traen sectores
 * y/o países (p.ej. justETF sin top-10), perdiendo la geografía del ETF y
 * dejándolo en `nonGeographicData`.
 *
 * @param {Object|null} data - Respuesta de /v1/etf/{symbol}/unified
 * @returns {boolean} true si aporta holdings, sectores o países
 */
function hasUsableETFData(data) {
  if (!data) return false;
  return Boolean(
    (data.holdings && data.holdings.length > 0) ||
    (data.sectors && data.sectors.length > 0) ||
    (data.countries && data.countries.length > 0)
  );
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
      // Solo usar cache si tiene datos válidos
      if (hasUsableETFData(cached.data)) {
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
    if (hasUsableETFData(data)) {
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
 * SEC-CF-001: Usa Cloudflare Tunnel y token de servicio
 * Retorna 'RATE_LIMITED' si hay error 429 para permitir retry
 * FIX-FETCH-001: Usa fetch nativo con AbortController para timeout
 */
async function fetchETFDataFromAPI(symbol) {
  // SEC-CF-001: Usar URL centralizada (sin /v1 duplicado)
  const baseUrl = FINANCE_QUERY_API_URL.replace('/v1', '');
  const url = `${baseUrl}/v1/etf/${symbol}/unified`;
  
  // FIX-ETF-DEBUG-001: Log headers para diagnóstico
  const headers = getServiceHeaders({ 'Accept': 'application/json' });
  const hasToken = Boolean(headers['x-service-token']);
  logger.debug('ETF API request', { symbol, url, hasToken });
  
  // FIX-FETCH-001: Usar AbortController para timeout (fetch nativo no soporta timeout param)
  // FIX-ETF-EU-001: Subido de 15s a 25s. Medido en producción, la cadena del API
  // para un UCITS europeo tarda ~18s en frío (etf.com 0.3s + TrackInsight
  // rate-limited 4.4s + justETF/Yahoo 13s) y ~7s en caliente. Con 15s se abortaba
  // justo el primer fetch, el que puebla la caché, y el ETF quedaba sin desagregar.
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), ETF_API_TIMEOUT_MS);
  
  try {
    const response = await fetch(url, { 
      headers,
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    
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
    clearTimeout(timeoutId); // FIX-FETCH-001: Limpiar timeout en caso de error
    // FIX-ETF-DEBUG-001: Logging detallado para diagnóstico
    logger.warn('ETF API fetch error', { 
      symbol, 
      error: error.message,
      errorName: error.name,
      errorCode: error.code,
      url,
      stack: error.stack?.split('\n').slice(0, 3).join(' | ')
    });
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
  // FIX-ETF-EU-001: `coverage` y `partiallyDecomposed` permiten diagnosticar
  // desde los logs qué ETFs sólo traen el top-10 de su cartera.
  const etfStats = { decomposed: 0, notDecomposed: [], partiallyDecomposed: new Set(), coverage: {} };

  // Filtrar assets relevantes
  const relevantAssets = assets.filter(asset => {
    if (!asset.isActive) return false;
    if (!asset.portfolioAccount) return true;
    const account = portfolioAccounts.find(acc => acc.id === asset.portfolioAccount);
    return account && account.isActive && account.userId === userId;
  });

  // Procesar holdings directos
  // FIX-MULTI-ACCOUNT-001: Acumular en lugar de sobrescribir cuando hay múltiples assets con el mismo ticker
  relevantAssets.forEach(asset => {
    const price = prices[asset.name];
    if (!price || !price.price) return;

    // FIX-CURRENCY-001: Convertir a USD antes de calcular peso
    const valueInLocalCurrency = asset.units * price.price;
    const currency = price.currency || 'USD';
    const valueInUSD = convertToUSD(valueInLocalCurrency, currency, currencyRates);
    const weight = valueInUSD / totalValue;

    // FIX-MULTI-ACCOUNT-001: Si el holding ya existe (mismo ticker en otra cuenta), acumular
    if (holdingsMap[asset.name]) {
      // Acumular peso
      holdingsMap[asset.name].weight += weight;
      // Buscar si ya existe una fuente directa para este símbolo
      const existingDirectSource = holdingsMap[asset.name].sources.find(
        src => src.symbol === asset.name
      );
      if (existingDirectSource) {
        // Sumar a la contribución directa existente
        existingDirectSource.contribution += weight;
      } else {
        // Agregar nueva fuente directa
        holdingsMap[asset.name].sources.push({ symbol: asset.name, contribution: weight });
      }
    } else {
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
    }
  });

  // Procesar ETFs
  const etfAssets = relevantAssets.filter(asset =>
    asset.assetType === 'etf' || prices[asset.name]?.type === 'etf'
  );

  // FIX-ETF-EU-001: Eliminar del mapa los ETFs que se van a desglosar ANTES de
  // recorrer las posiciones. Si el `delete` se hiciera dentro del bucle (como
  // antes), con un mismo ETF en varias cuentas la segunda iteración borraría el
  // remanente acumulado en la primera.
  const decomposableEtfSymbols = new Set();
  for (const etf of etfAssets) {
    const info = etfData.get(etf.name.trim().toUpperCase());
    if (info && info.holdings && info.holdings.length > 0) {
      decomposableEtfSymbols.add(etf.name);
    }
  }
  decomposableEtfSymbols.forEach(symbol => { delete holdingsMap[symbol]; });

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
      etfStats.decomposed++;

      // FIX-ETF-EU-001: Fracción del fondo efectivamente desagregable.
      // etf.com devuelve la cartera completa (~1.0); Yahoo/justETF solo el top-10.
      // Sólo cuentan las posiciones con identificador: las que no lo tienen se
      // descartan más abajo, así que incluirlas aquí inflaría la cobertura y su
      // peso se perdería del treemap. Es el caso de TLT, cuyos 42 bonos del
      // Tesoro llegan sin symbol ni isin: cobertura real 0 → se conserva entero.
      const rawCoverage = etfInfo.holdings
        .filter(h => h.symbol || h.isin)
        .reduce((sum, h) => sum + (h.weight || 0), 0);
      const coverage = Math.min(Math.max(rawCoverage, 0), 1);
      etfStats.coverage[etf.name] = coverage;

      // Procesar holdings del ETF
      for (const holding of etfInfo.holdings) {
        if (!holding.symbol && !holding.isin) continue;

        const identifier = holding.symbol || holding.isin;
        const contribution = (holding.weight || 0) * etfWeight;

        if (!holdingsMap[identifier]) {
          holdingsMap[identifier] = {
            // FIX-MULTI-ACCOUNT-001: Usar identifier como symbol para consistencia
            // Esto asegura que el frontend pueda identificar inversiones directas correctamente
            symbol: identifier,
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

      // FIX-ETF-EU-001: Con desglose parcial, el resto del fondo se mantiene
      // como posición del propio ETF para que los pesos sigan sumando 100%.
      if (coverage < ETF_HOLDINGS_COVERAGE_THRESHOLD) {
        const remainderWeight = etfWeight * (1 - coverage);
        if (remainderWeight > 0) {
          if (!holdingsMap[etf.name]) {
            holdingsMap[etf.name] = {
              symbol: etf.name,
              description: `${price.name || etf.name} (resto no desagregado)`,
              weight: 0,
              asset_type: 'etf',
              sector: price.sector || null,
              assetClass: price.sector || null,
              isPartialRemainder: true,
              holdingsCoverage: coverage,
              sources: []
            };
          }
          holdingsMap[etf.name].weight += remainderWeight;
          holdingsMap[etf.name].sources.push({ symbol: etf.name, contribution: remainderWeight });
          etfStats.partiallyDecomposed.add(etf.name);
        }
      }

    } else {
      // Si no hay holdings del ETF, el ETF permanece como holding directo
      etfStats.notDecomposed.push(etf.name);
    }

    // Procesar sectores del ETF
    // SECTOR-HOMOLOGATION: Usar normalizeSectorName para mapear a sectores estándar
    // FIX-ETF-EU-001: Fuera del bloque de holdings. Una fuente puede publicar el
    // desglose sectorial sin publicar la cartera (p.ej. justETF sin top-10); esa
    // exposición debe reflejarse en el gráfico de sectores igualmente.
    let sectorCoverage = 0;
    for (const sector of ((etfInfo && etfInfo.sectors) || [])) {
      const standardSector = normalizeSectorName(sector.name, sectorMappings);
      if (!standardSector) continue;

      const sectorWeight = sector.weight || 0;
      const contribution = sectorWeight * etfWeight;
      sectorCoverage += sectorWeight;

      if (!sectorsMap[standardSector]) {
        sectorsMap[standardSector] = { sector: standardSector, weight: 0 };
      }
      sectorsMap[standardSector].weight += contribution;
    }

    // FIX-ETF-EU-001: Si la fuente clasificó sólo una parte del fondo, el resto
    // se imputa a 'Other' para que la distribución sectorial siga sumando el
    // total del portafolio. Si no clasificó nada (sectorCoverage === 0) se
    // mantiene el comportamiento previo: el ETF no aporta sectores.
    const sectorRemainder = sectorCoverage > 0
      ? Math.max(0, 1 - Math.min(sectorCoverage, 1))
      : 0;
    if (sectorRemainder > 0.001) {
      if (!sectorsMap['Other']) {
        sectorsMap['Other'] = { sector: 'Other', weight: 0 };
      }
      sectorsMap['Other'].weight += sectorRemainder * etfWeight;
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
    // SECTOR-HOMOLOGATION: Usar normalizeSectorName para mapear a sectores estándar
    const standardSector = normalizeSectorName(price.sector, sectorMappings);

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

  // SECTOR-HOMOLOGATION: Agregar percentage y ordenar por peso
  const sectors = Object.values(sectorsMap)
    .map(s => ({
      sector: s.sector,
      weight: s.weight,
      percentage: s.weight * 100
    }))
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
        // FIX-ETF-EU-001: justETF agrupa la cola de la cartera en "Other" y usa
        // nombres propios de país. Sin este log, cualquier nombre que no exista
        // en la colección `countries` se descartaba de forma silenciosa.
        if (!country && !/^others?$/i.test(countryName)) {
          logger.warn('Unmapped ETF country found', { etf: etf.name, country: countryName });
        }
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
  getAvailableSectors,
  // FIX-ETF-EU-001: expuestos para pruebas unitarias del desglose de ETFs
  // (no forman parte del contrato público del servicio).
  __testing: {
    calculateSectorDistribution,
    calculateCountryDistribution,
    hasUsableETFData,
    normalizeSectorName,
    ETF_HOLDINGS_COVERAGE_THRESHOLD
  }
};
