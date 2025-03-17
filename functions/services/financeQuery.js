const API_BASE_URL = 'https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1';

let data = null;
let loading = false;
let error = null;

const fetchData = async (endpoint, maxRetries = 3, delay = 1000) => {
  let attempts = 0;
  loading = true;
  error = null;

  while (attempts < maxRetries) {
    try {
      const response = await fetch(`${API_BASE_URL}${endpoint}`);
      if (!response.ok) {
        throw new Error('Network response was not ok');
      }
      data = await response.json();
      return data;
    } catch (err) {
      attempts++;
      error = err.message || 'Error desconocido';
      console.warn(`Intento ${attempts} fallido: ${error}`);

      if (attempts < maxRetries) {
        // Esperar antes de reintentar
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        console.error('Todos los intentos fallaron');
        return null;
      }
    } finally {
      loading = false;
    }
  }
};

const getIndices = () => fetchData('/indices');
const getActives = () => fetchData('/actives');
const getGainers = () => fetchData('/gainers');
const getLosers = () => fetchData('/losers');
const getNews = () => fetchData('/news');
const getNewsFromSymbol = (symbol) => fetchData(`/news?symbol=${symbol}`);
const getQuotes = (symbols) => fetchData(`/quotes?symbols=${symbols}`);
const getSimpleQuotes = (symbols) => fetchData(`/simple-quotes/?symbols=${symbols}`);
const getSimilarStocks = (symbol) => fetchData(`/similar-stocks/?symbol=${symbol}`);
const getSectors = () => fetchData('/sectors');
const search = (query) => fetchData(`/search?query=${query}`);
const getHistorical = (symbol, time, interval) => fetchData(`/historical/?symbol=${symbol}&time=${time}&interval=${interval}`);
const getIndicators = (func, symbol) => fetchData(`/indicators/?function=${func}&symbol=${symbol}`);
const getAnalysis = (symbol, time, interval) => fetchData(`/analysis/?symbol=${symbol}&time=${time}&interval=${interval}`);

module.exports = {
  getIndices,
  getActives,
  getGainers,
  getLosers,
  getNews,
  getQuotes,
  getSimpleQuotes,
  getSimilarStocks,
  getSectors,
  search,
  getHistorical,
  getIndicators,
  getAnalysis,
  getData: () => data,
  isLoading: () => loading,
  getError: () => error,
  getNewsFromSymbol
}; 