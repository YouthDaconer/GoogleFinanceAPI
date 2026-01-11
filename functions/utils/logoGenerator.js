/**
 * Utilidades para generación de logos de empresas
 * 
 * Este módulo centraliza la lógica de generación de URLs de logos para
 * mantener consistencia en toda la aplicación.
 * 
 * @module logoGenerator
 * @see https://logo.dev/ - Servicio de logos para empresas
 * @see https://financialmodelingprep.com - Fallback para ETFs y criptos
 */

/**
 * Token de Logo.dev para generar URLs de logos
 * @constant {string}
 */
const LOGO_DEV_TOKEN = "pk_RfFunZLSTUSVSIh-73bwZQ";

/**
 * Extrae el dominio de una URL completa
 * 
 * @param {string} url - URL completa (ej: "https://www.apple.com/investors")
 * @returns {string|null} Dominio sin www (ej: "apple.com") o null si no se puede extraer
 * 
 * @example
 * extractDomain("https://www.apple.com") // => "apple.com"
 * extractDomain("http://investor.nvidia.com") // => "investor.nvidia.com"
 * extractDomain("apple.com") // => "apple.com"
 */
const extractDomain = (url) => {
  if (!url) return null;
  
  try {
    // Si la URL no tiene esquema, agregarlo para que URL pueda parsearla
    let urlToParse = url;
    if (!url.startsWith('http://') && !url.startsWith('https://')) {
      urlToParse = `https://${url}`;
    }
    
    const parsedUrl = new URL(urlToParse);
    let domain = parsedUrl.hostname;
    
    // Remover www. si está presente
    if (domain.startsWith('www.')) {
      domain = domain.substring(4);
    }
    
    return domain || null;
  } catch (error) {
    // Si URL() falla, intentar extraer el dominio manualmente
    try {
      const cleaned = url.replace(/^(https?:\/\/)?(www\.)?/, '').split('/')[0];
      return cleaned || null;
    } catch {
      return null;
    }
  }
};

/**
 * Genera la URL del logo a partir de la URL del website de la empresa
 * 
 * @param {string} websiteUrl - URL completa del website de la empresa
 * @returns {string|null} URL del logo o null si no se puede generar
 * 
 * @example
 * generateLogoFromWebsite("https://www.apple.com")
 * // => "https://img.logo.dev/apple.com?token=pk_..."
 */
const generateLogoFromWebsite = (websiteUrl) => {
  const domain = extractDomain(websiteUrl);
  if (!domain) return null;
  
  return `https://img.logo.dev/${domain}?token=${LOGO_DEV_TOKEN}`;
};

/**
 * Genera la URL del logo para un símbolo dado usando fallback
 * 
 * Esta función se usa como fallback cuando no hay website disponible.
 * Para ETFs y criptomonedas, usa financialmodelingprep.
 * 
 * @param {string} symbol - Símbolo del ticker (ej: "AAPL", "VOO")
 * @param {string} [assetType='stock'] - Tipo de activo ('stock', 'etf', 'crypto')
 * @returns {string} URL del logo de fallback
 * 
 * @example
 * generateFallbackLogo('VOO', 'etf')
 * // => 'https://financialmodelingprep.com/image-stock/VOO.png'
 */
const generateFallbackLogo = (symbol, assetType = 'stock') => {
  if (!symbol) return null;
  
  // Para todos los tipos, usar financialmodelingprep como fallback
  return `https://financialmodelingprep.com/image-stock/${symbol}.png`;
};

/**
 * Genera la URL del logo para un símbolo dado
 * 
 * Estrategia de generación:
 * 1. Si hay website, extraer dominio y usar logo.dev
 * 2. Fallback a financialmodelingprep
 * 
 * @param {string} symbol - Símbolo del ticker (ej: "AAPL", "VOO")
 * @param {Object} [options={}] - Opciones adicionales
 * @param {string} [options.website] - URL del website de la empresa
 * @param {string} [options.assetType='stock'] - Tipo de activo ('stock', 'etf', 'crypto')
 * @returns {string|null} URL del logo o null si no se puede generar
 * 
 * @example
 * // Con website disponible
 * generateLogoUrl('AAPL', { website: 'https://www.apple.com' })
 * // => 'https://img.logo.dev/apple.com?token=pk_...'
 * 
 * @example
 * // Sin website (fallback)
 * generateLogoUrl('VOO', { assetType: 'etf' })
 * // => 'https://financialmodelingprep.com/image-stock/VOO.png'
 */
const generateLogoUrl = (symbol, options = {}) => {
  const { website, assetType = 'stock' } = options;
  
  // 1. Si hay website, usar logo.dev
  if (website) {
    const logoFromWebsite = generateLogoFromWebsite(website);
    if (logoFromWebsite) return logoFromWebsite;
  }
  
  // 2. Fallback a financialmodelingprep
  return generateFallbackLogo(symbol, assetType);
};

module.exports = {
  LOGO_DEV_TOKEN,
  extractDomain,
  generateLogoFromWebsite,
  generateFallbackLogo,
  generateLogoUrl
};
