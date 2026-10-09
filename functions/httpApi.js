const express = require("express");
const cors = require('cors');
const crypto = require('crypto'); // SEC-AUDIT-002: C-MED-04 - For timing-safe comparison
const rateLimit = require("express-rate-limit");
// Agregar multer para manejar la carga de archivos
const multer = require('multer');
// Crear instancia de almacenamiento con límites adecuados
const storage = multer.memoryStorage();
// Configuración de multer con mejor manejo de errores
const upload = multer({ 
  storage: storage,
  limits: { 
    fileSize: 30 * 1024 * 1024, // Reducir a 30MB para evitar problemas de memoria
    files: 1,
    parts: 10 // Limitar número total de partes en el formulario
  }
});
const XLSX = require('xlsx');
// Importar servicios necesarios
const scrapeIndices = require("./services/scrapeIndices");
const scrapeIndicesByCountry = require("./services/scrapeIndicesByCountry");
const { scrapeFullQuote, scrapeSimpleQuote } = require("./services/scrapeQuote");
const { fetchPriceFromYahooFinance } = require("./services/yahoo/scrapeQuote");
const { scrapeActiveStock } = require("./services/scrapeActiveStock");
const { scrapeSimpleCurrencie } = require("./services/scrapeCurrencies");
const { scrapeGainers } = require("./services/scrapeGainers");
const { scrapeLosers } = require("./services/scrapeLosers");
const { scrapeNews } = require("./services/scrapeNews");
const { getQuotes, getSimpleQuotes, getNewsFromSymbol, search } = require('./services/financeQuery');

// Crear la app Express
const app = express();

// Configurar Express para confiar en el proxy de Firebase
app.set('trust proxy', true);

// Middleware para manejar errores y logging
app.use((req, res, next) => {
  console.log(`Recibida solicitud: ${req.method} ${req.originalUrl}`);
  res.on('finish', () => {
    console.log(`Respondida solicitud: ${req.method} ${req.originalUrl} ${res.statusCode}`);
  });
  next();
});

// Configurar CORS
const corsOptions = {
  origin: ["https://portastock.net", "https://www.portastock.net", "https://portafolio-inversiones.web.app", "https://portafolio-inversiones.firebaseapp.com", "http://localhost:3000", "http://localhost:3001"],
  optionsSuccessStatus: 200
};

// SEC-AUDIT-002: C-MED-03 - Helper para sanitizar errores sin exponer detalles internos
function sanitizeError(error, context = 'operation') {
  // Log completo server-side
  console.error(`[${context}] Error:`, error.message, error.stack);
  // Retornar mensaje genérico al cliente
  return `Error during ${context}`;
}

// SEC-AUDIT-002: C-MED-04 - Comparación timing-safe para API keys
function safeCompareApiKey(providedKey, expectedKey) {
  if (!providedKey || !expectedKey) return false;
  try {
    const a = Buffer.from(providedKey);
    const b = Buffer.from(expectedKey);
    // Si longitudes diferentes, usar misma longitud para evitar timing leak
    if (a.length !== b.length) {
      return crypto.timingSafeEqual(a, a.slice(0, a.length)) && false;
    }
    return crypto.timingSafeEqual(a, b);
  } catch {
    return false;
  }
}
app.use(cors(corsOptions));

// Parsear JSON en el body de las peticiones
app.use(express.json());

// Rate limiting
const demoApiKey = "demo";
const demoApiLimiter = rateLimit({
  windowMs: 60000,
  max: 10,
  message: "Too many requests from this IP, please try again after a minute"
});

app.use((req, res, next) => {
  let apiKey = req.get('X-API-Key');
  if (!apiKey) {
    apiKey = demoApiKey;
    req.headers['x-api-key'] = demoApiKey;
    demoApiLimiter(req, res, next);
  } else if (safeCompareApiKey(apiKey, process.env.API_KEY)) {
    // SEC-AUDIT-002: C-MED-04 - Use timing-safe comparison
    next();
  } else {
    demoApiLimiter(req, res, next);
  }
});

// Endpoint de salud para verificar que la app está funcionando
app.get("/", (req, res) => {
  res.status(200).json({ status: "ok", message: "API running" });
});

app.get("/health", (req, res) => {
  res.status(200).json({ status: "ok", message: "API running", timestamp: new Date().toISOString() });
});

// Endpoints de la API original

app.get("/indices", async (req, res) => {
  const { region, country } = req.query;
  if (!region) {
    res.status(400).json({
      error: "Por favor, proporcione el parámetro de consulta de región (americas, europe-middle-east-africa, o asia-pacific)",
    });
    return;
  }
  try {
    let stockIndex;
    if (country) {
      stockIndex = await scrapeIndicesByCountry(region, country);
    } else {
      stockIndex = await scrapeIndices(region);
    }
    res.status(200).json(stockIndex);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'scraping indices'),
    });
  }
});

app.get("/fullQuote", async (req, res) => {
  const { symbol, exchange } = req.query;
  try {
    if (!symbol || !exchange) {
      res.status(400).json({
        error: "Por favor, proporcione ambos parámetros de consulta: símbolo y bolsa",
      });
      return;
    }
    const fullQuote = await scrapeFullQuote(symbol, exchange);
    res.status(200).json(fullQuote);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'fetching quote'),
    });
  }
});

app.get("/quote", async (req, res) => {
  const { symbol, exchange } = req.query;
  try {
    if (!symbol || !exchange) {
      res.status(400).json({
        error: "Por favor, proporcione ambos parámetros de consulta: símbolo y bolsa",
      });
      return;
    }
    const fullQuote = await scrapeSimpleQuote(symbol, exchange);
    res.status(200).json(fullQuote);
  } catch (error) {
    console.error(error);
    if (error.message.includes("no es un número válido")) {
      res.status(400).json({
        error: sanitizeError(error, 'invalid data'),
      });
    } else {
      res.status(500).json({
        error: sanitizeError(error, 'simple quote'),
      });
    }
  }
});

app.get("/apiQuote", async (req, res) => {
  const { symbols } = req.query;
  try {
    if (!symbols) {
      res.status(400).json({
        error: "Por favor, proporcione el parámetro de consulta de símbolo",
      });
      return;
    }

    // Convertir symbols a un array de strings
    const symbolsArray = symbols.split(',').map(s => s.trim());

    // Verificar si el array es válido
    if (symbolsArray.length === 0 || symbolsArray.some(s => s === "")) {
      throw new Error("Formato de símbolos inválido");
    }

    const fullQuote = await fetchPriceFromYahooFinance(symbolsArray);
    res.status(200).json(fullQuote);
  } catch (error) {
    console.error(error);
    if (error.message === "Formato de símbolos inválido") {
      res.status(400).json({
        error: "Formato de símbolos inválido. Por favor, proporcione una lista de símbolos separados por comas.",
      });
    } else {
      res.status(500).json({
        error: sanitizeError(error, 'api quote'),
      });
    }
  }
});

app.get("/currencie", async (req, res) => {
  const { origin, target } = req.query;
  try {
    if (!origin || !target) {
      res.status(400).json({
        error: "Por favor, proporcione ambos parámetros de consulta: origen y destino",
      });
      return;
    }
    const currencie = await scrapeSimpleCurrencie(origin, target);
    res.status(200).json(currencie);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'currency'),
    });
  }
});

app.get("/active", async (req, res) => {
  try {
    const activeStocks = await scrapeActiveStock();
    res.status(200).json(activeStocks);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'active stocks'),
    });
  }
});

app.get("/gainers", async (req, res) => {
  try {
    const gainers = await scrapeGainers();
    res.status(200).json(gainers);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'gainers'),
    });
  }
});

app.get("/losers", async (req, res) => {
  try {
    const losers = await scrapeLosers();
    res.status(200).json(losers);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'losers'),
    });
  }
});

app.get("/news", async (req, res) => {
  const { symbol, exchange } = req.query;
  try {
    if (!symbol || !exchange) {
      res.status(400).json({
        error: "Por favor, proporcione ambos parámetros de consulta: símbolo y bolsa",
      });
      return;
    }
    const news = await scrapeNews(symbol, exchange);
    res.status(200).json(news);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'news'),
    });
  }
});

// HU #3: el endpoint `/api/historicalExchangeRate` se retiró junto con el camino
// directo a Yahoo. Las tasas de cualquier fecha se piden por rango al canal de
// datos de mercado: `GET /v1/exchange-rates?currencies=…&start=…&end=…`.
// @see platform-docs/stories/3-tasa-vigente-canal-mercado/refinamiento.md (T10)

app.get('/quotes', async (req, res) => {
  const { symbols } = req.query;
  try {
    if (!symbols) {
      res.status(400).json({
        error: 'Por favor, proporcione el parámetro de consulta de símbolos',
      });
      return;
    }

    const quotes = await getQuotes(symbols);
    res.status(200).json(quotes);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'quotes'),
    });
  }
});

app.get('/simple-quotes', async (req, res) => {
  const { symbols } = req.query;
  try {
    if (!symbols) {
      res.status(400).json({
        error: 'Por favor, proporcione el parámetro de consulta de símbolos',
      });
      return;
    }

    const simpleQuotes = await getSimpleQuotes(symbols);
    res.status(200).json(simpleQuotes);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'simple quotes'),
    });
  }
});

app.get('/news-from-quote', async (req, res) => {
  const { symbol } = req.query;
  try {
    if (!symbol) {
      res.status(400).json({
        error: 'Por favor, proporcione el parámetro de consulta símbolo',
      });
      return;
    }

    const newsFromQuote = await getNewsFromSymbol(symbol);
    res.status(200).json(newsFromQuote);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'news from quote'),
    });
  }
});

app.get('/search', async (req, res) => {
  const { query } = req.query;
  try {
    if (!query) {
      res.status(400).json({
        error: 'Por favor, proporcione el parámetro de consulta de búsqueda',
      });
      return;
    }

    const searchResult = await search(query);
    res.status(200).json(searchResult);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: sanitizeError(error, 'search'),
    });
  }
});

// Función auxiliar para procesar datos de ETF de forma optimizada
function processETFDataOptimized(dataRows, columnMapping, customBatchSize) {
  console.log('📊 Iniciando procesamiento optimizado de datos ETF');
  const etfMap = new Map();
  
  console.log('📊 Procesando', dataRows.length, 'filas de forma eficiente');
  
  // Procesar por lotes para evitar bloquear el bucle de eventos
  const BATCH_SIZE = customBatchSize || 500; // Usar tamaño personalizado si se proporciona
  console.log(`📊 Usando tamaño de lote: ${BATCH_SIZE}`);
  
  return new Promise((resolve, reject) => {
    let processedRows = 0;
    let errorCount = 0;
    const MAX_ERRORS = 100; // Máximo número de errores permitidos antes de abortar
    
    function processNextBatch() {
      try {
        const batchEndIndex = Math.min(processedRows + BATCH_SIZE, dataRows.length);
        console.log(`📊 Procesando lote ${processedRows + 1}-${batchEndIndex} de ${dataRows.length}`);
        
        // Procesar cada fila del lote actual
        for (let i = processedRows; i < batchEndIndex; i++) {
          const row = dataRows[i];
          if (!row || !row.length) continue;
          
          const isRowEmpty = row.every(cell => cell === undefined || cell === null || String(cell).trim() === '');
          if (isRowEmpty) continue;
  
          try {
            // Extraer datos
            const ticker = row[columnMapping['ETF']];
            const sector = row[columnMapping['Sector']];
            const companyTicker = row[columnMapping['Ticker de empresa']];
            const companyName = row[columnMapping['Nombre empresa']];
            const percentageRaw = row[columnMapping['Porcentaje']];
            
            // Asegurarse de que el porcentaje sea un número
            let percentage = NaN;
            
            if (typeof percentageRaw === 'number') {
              percentage = percentageRaw / 100;
            } else if (typeof percentageRaw === 'string' && percentageRaw.trim() !== '') {
              // Intentar convertir string a número, manejando diferentes formatos
              const cleanPercentage = percentageRaw
                .replace(/,/g, '.') // Reemplazar comas por puntos
                .replace(/[^0-9.-]/g, ''); // Eliminar caracteres no numéricos
              
              percentage = parseFloat(cleanPercentage) / 100;
            }
    
            if (!ticker || !sector || !companyTicker || !companyName || isNaN(percentage)) {
              if (i < 5 || i % 1000 === 0) {
                console.log(`📊 Omitiendo fila ${i+1} con datos incompletos:`, { 
                  ticker: ticker || 'vacío', 
                  sector: sector || 'vacío', 
                  companyTicker: companyTicker || 'vacío', 
                  companyName: companyName || 'vacío', 
                  percentageRaw: percentageRaw || 'vacío' 
                });
              }
              continue; // Saltar esta fila si faltan datos o el porcentaje no es válido
            }
    
            if (!etfMap.has(ticker)) {
              etfMap.set(ticker, {
                ticker: ticker,
                sectors: [],
                holdings: [],
                totalHoldings: 0,
                filteredHoldings: 0,
                weight: 1
              });
            }
    
            const etf = etfMap.get(ticker);
            etf.totalHoldings++;
    
            // Añadir o actualizar sector (siempre, independientemente del porcentaje)
            const existingSector = etf.sectors.find(s => s.sector === sector);
            if (existingSector) {
              existingSector.weight += percentage;
            } else {
              etf.sectors.push({ sector, weight: percentage });
            }
    
            // Añadir o actualizar holding (solo si cumple el umbral mínimo)
            if (percentage >= 0.001) { // 0.1% = 0.001 como decimal
              const existingHolding = etf.holdings.find(h => h.symbol === companyTicker);
              if (existingHolding) {
                existingHolding.weight += percentage;
              } else {
                etf.holdings.push({ 
                  symbol: companyTicker, 
                  description: companyName, 
                  weight: percentage 
                });
              }
            } else {
              etf.filteredHoldings++;
            }
          } catch (rowError) {
            errorCount++;
            if (errorCount <= 10) {
              console.error(`📊 Error procesando fila ${i+1}:`, rowError.message);
            }
            
            if (errorCount >= MAX_ERRORS) {
              reject(new Error(`Demasiados errores (${errorCount}) durante el procesamiento de datos. Abortando.`));
              return;
            }
          }
        }
        
        processedRows = batchEndIndex;
        
        // Limpiamos referencias para ayudar al recolector de basura
        if (processedRows > BATCH_SIZE * 2) {
          dataRows.splice(0, processedRows - BATCH_SIZE);
          processedRows = BATCH_SIZE;
          
          // Forzar recolección de basura si es posible
          if (global.gc) {
            try {
              global.gc();
            } catch (e) {
              // Ignorar errores del recolector de basura
            }
          }
        }
        
        if (processedRows < dataRows.length) {
          // Seguir procesando después de un breve retraso para permitir que otras operaciones se ejecuten
          setImmediate(processNextBatch);
        } else {
          // Hemos terminado, normalizar y resolver
          console.log('📊 Normalizando pesos para', etfMap.size, 'ETFs');
          
          // Normalizar pesos
          etfMap.forEach(etf => {
            const totalSectorWeight = etf.sectors.reduce((sum, sector) => sum + sector.weight, 0);
            const totalHoldingWeight = etf.holdings.reduce((sum, holding) => sum + holding.weight, 0);
        
            if (totalSectorWeight > 0) {
              etf.sectors.forEach(sector => sector.weight /= totalSectorWeight);
            }
            
            if (totalHoldingWeight > 0) {
              etf.holdings.forEach(holding => holding.weight /= totalHoldingWeight);
            }
            
            // Ordenar holdings por peso (mayor a menor)
            etf.holdings.sort((a, b) => b.weight - a.weight);
            
            // Limitar número de holdings para reducir tamaño de respuesta
            if (etf.holdings.length > 100) {
              const filteredOut = etf.holdings.length - 100;
              etf.filteredHoldings += filteredOut;
              etf.holdings = etf.holdings.slice(0, 100);
            }
          });
          
          console.log('📊 Procesamiento completado para', etfMap.size, 'ETFs');
          resolve(etfMap);
        }
      } catch (error) {
        console.error('📊 Error procesando batch:', error);
        reject(error);
      }
    }
    
    // Comenzar el procesamiento por lotes
    processNextBatch();
  });
}

// Almacén global para el progreso de carga
const uploadProgressStore = {};

// Añadir un nuevo endpoint para consultar el progreso
app.get('/upload-progress/:dataKey', (req, res) => {
  const { dataKey } = req.params;
  
  if (!uploadProgressStore[dataKey]) {
    return res.status(404).json({ 
      error: 'No se encontró información de progreso para la clave proporcionada'
    });
  }
  
  return res.status(200).json(uploadProgressStore[dataKey]);
});

// Middleware personalizado para capturar errores de multer
const uploadMiddleware = (req, res, next) => {
  console.log('📊 Iniciando procesamiento de carga de archivo');
  
  // Verificar si la solicitud es multipart/form-data
  const contentType = req.headers['content-type'] || '';
  if (!contentType.includes('multipart/form-data')) {
    console.error('Error: Content-Type incorrecto:', contentType);
    return res.status(400).json({
      error: 'El Content-Type debe ser multipart/form-data',
      details: `Content-Type actual: ${contentType}`
    });
  }
  
  upload.single('excelFile')(req, res, (err) => {
    if (err) {
      console.error('Error en la carga del archivo:', err);
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          error: 'El archivo excede el tamaño máximo permitido (30MB)',
          details: err.message
        });
      }
      if (err.code === 'LIMIT_UNEXPECTED_FILE') {
        return res.status(400).json({
          error: 'Campo de archivo no esperado',
          details: 'El nombre del campo debe ser "excelFile"'
        });
      }
      if (err.message && err.message.includes('Unexpected end of form')) {
        console.error('📊 Error de interrupción en la carga: Unexpected end of form');
        return res.status(400).json({
          error: 'La carga del archivo se interrumpió',
          details: 'La conexión se cerró antes de que el archivo se cargara completamente. Intenta con un archivo más pequeño (menos de 15MB) o utiliza un excel con menos datos.',
          solution: 'Recomendamos dividir archivos grandes en archivos más pequeños o reducir el número de filas/columnas en el Excel.'
        });
      }
      return res.status(500).json({
        error: 'Error en la carga del archivo',
        details: err.message
      });
    }

    if (!req.file) {
      console.error('📊 No se recibió el archivo. Cuerpo de la solicitud:', req.body);
      return res.status(400).json({ 
        error: 'No se proporcionó archivo de Excel', 
        details: 'Asegúrate de que el campo del archivo se llama "excelFile" en el formulario'
      });
    }

    next();
  });
};

// Endpoint completo para procesamiento de ETF
app.post('/process-etf-excel', uploadMiddleware, async (req, res) => {
  console.log('📊 Solicitud recibida en /process-etf-excel');
  
  // Establecer un timeout más corto que coincida con la configuración de Firebase
  req.setTimeout(480000); // 8 minutos (menor al límite de Firebase)
  res.setTimeout(480000);
  
  // Crear un ID único para rastrear la solicitud en los registros
  const requestId = Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
  console.log(`📊 [${requestId}] Iniciando procesamiento de solicitud`);

  // Verificar si la solicitud acepta compresión GZIP
  const acceptsGzip = req.headers['accept-encoding'] && req.headers['accept-encoding'].includes('gzip');
  
  try {
    if (!req.file) {
      console.log(`📊 [${requestId}] Error: No se proporcionó archivo`);
      return res.status(400).json({ error: 'No se proporcionó archivo de Excel' });
    }

    const fileSize = (req.file.size / (1024 * 1024)).toFixed(2);
    console.log(`📊 [${requestId}] Procesando archivo Excel: ${req.file.originalname}, Tamaño: ${fileSize} MB`);
    
    // Verificar tamaño máximo (verificación adicional)
    if (req.file.size > 30 * 1024 * 1024) { // 30MB máximo (consistente con la configuración de multer)
      console.log(`📊 [${requestId}] Error: Archivo demasiado grande (${fileSize} MB)`);
      return res.status(413).json({ 
        error: 'El archivo es demasiado grande. Máximo permitido: 30MB',
        details: `Tamaño actual: ${fileSize} MB`
      });
    }
    
    // Obtener parámetros de la solicitud
    const userId = req.body.userId || 'anonymous';
    const origin = req.body.origin || 'etf-analyzer';
    const missingETFs = req.body.missingETFs ? JSON.parse(req.body.missingETFs) : [];
    
    // Crear ID único para este conjunto de datos (solo para seguimiento)
    const timestamp = Date.now();
    const dataKey = `excel_${userId}_${requestId}`;
    
    // Inicializar progreso
    uploadProgressStore[dataKey] = {
      progress: 10,
      stage: 'Leyendo archivo Excel',
      updated: Date.now()
    };
    
    // Opciones para procesar el Excel eficientemente
    const options = { 
      type: 'buffer',
      cellStyles: false,
      cellNF: false,
      cellDates: false,
      cellText: false,
      cellFormula: false,
      dense: true,
      raw: true,
      sheetStubs: false
    };
    
    // Leer el archivo Excel con manejo de errores
    let workbook;
    try {
      workbook = XLSX.read(req.file.buffer, options);
      console.log(`📊 [${requestId}] Archivo Excel leído correctamente`);
    } catch (xlsxError) {
      console.error(`📊 [${requestId}] Error al leer archivo Excel:`, xlsxError);
      return res.status(422).json({ 
        error: 'No se pudo leer el archivo Excel', 
        details: sanitizeError(xlsxError, 'xlsx parsing') 
      });
    }
    
    // Liberar memoria
    req.file.buffer = null;
    
    // Actualizar progreso
    uploadProgressStore[dataKey] = {
      ...uploadProgressStore[dataKey],
      progress: 30,
      stage: 'Extrayendo datos',
      updated: Date.now()
    };
    
    // Obtener la primera hoja
    if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
      console.error(`📊 [${requestId}] Error: No se encontraron hojas en el archivo Excel`);
      return res.status(422).json({ error: 'El archivo Excel no contiene hojas' });
    }
    
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    
    if (!worksheet) {
      console.error(`📊 [${requestId}] Error: No se pudo acceder a la hoja ${sheetName}`);
      return res.status(422).json({ error: `No se pudo acceder a la hoja ${sheetName}` });
    }
    
    // Determinar el rango de datos
    let maxRow = 0;
    for (let cell in worksheet) {
      if (cell[0] === '!' || typeof worksheet[cell] !== 'object') continue;
      const rowMatch = cell.match(/[A-Z]+(\d+)/);
      if (rowMatch) {
        const row = parseInt(rowMatch[1]);
        if (row > maxRow) maxRow = row;
      }
    }
    
    console.log(`📊 [${requestId}] Filas detectadas: ${maxRow}`);
    
    if (maxRow > 50000) {
      console.warn(`📊 [${requestId}] Advertencia: Gran cantidad de filas (${maxRow})`);
    }
    
    // Convertir a JSON con manejo de errores
    let jsonData;
    try {
      jsonData = XLSX.utils.sheet_to_json(worksheet, { 
        header: 1,
        defval: null,
        blankrows: false,
        sheetRows: maxRow
      });
      console.log(`📊 [${requestId}] Datos extraídos correctamente: ${jsonData.length} filas`);
    } catch (jsonError) {
      console.error(`📊 [${requestId}] Error al convertir hoja a JSON:`, jsonError);
      return res.status(422).json({ 
        error: 'Error al convertir los datos de Excel', 
        details: sanitizeError(jsonError, 'json parsing') 
      });
    }
    
    // Liberar memoria del workbook ya procesado
    workbook = null;
    worksheet = null;
    
    // Actualizar progreso
    uploadProgressStore[dataKey] = {
      ...uploadProgressStore[dataKey],
      progress: 50,
      stage: 'Filtrando y transformando datos',
      updated: Date.now()
    };
    
    // Filtrar y normalizar datos
    const filteredData = jsonData
      .filter(row => 
        row && row.length > 0 && row.some(cell => cell !== null && cell !== undefined && cell !== '')
      )
      .map(row => {
        const limitedRow = row.slice(0, 5);
        while (limitedRow.length < 5) {
          limitedRow.push(null);
        }
        return limitedRow;
      });
    
    // Liberar memoria
    jsonData = null;
    
    console.log(`📊 [${requestId}] Datos filtrados: ${filteredData.length} filas válidas`);
    
    // Mapeo de columnas predeterminado
    const columnMapping = {
      'ETF': 0,
      'Ticker de empresa': 1,
      'Nombre empresa': 2,
      'Porcentaje': 3,
      'Sector': 4
    };
    
    // Actualizar progreso
    uploadProgressStore[dataKey] = {
      ...uploadProgressStore[dataKey],
      progress: 70,
      stage: 'Procesando datos ETF',
      updated: Date.now()
    };
    
    // Procesar datos (omitiendo fila de encabezado)
    if (filteredData.length <= 1) {
      console.error(`📊 [${requestId}] Error: No hay suficientes datos válidos en el archivo`);
      return res.status(422).json({ 
        error: 'El archivo no contiene datos válidos',
        details: 'Se esperaban al menos dos filas (encabezado y datos)'
      });
    }
    
    const dataRows = filteredData.slice(1);
    
    console.log(`📊 [${requestId}] Procesando ${dataRows.length} filas de datos`);
    
    let etfMap;
    try {
      etfMap = await processETFDataOptimized(dataRows, columnMapping);
      console.log(`📊 [${requestId}] Datos procesados: ${etfMap.size} ETFs encontrados`);
    } catch (procError) {
      console.error(`📊 [${requestId}] Error al procesar datos ETF:`, procError);
      return res.status(500).json({ 
        error: 'Error al procesar los datos ETF', 
        details: sanitizeError(procError, 'processing') 
      });
    }
    
    // Liberar memoria
    filteredData.length = 0;
    
    const etfData = Array.from(etfMap.values());
    
    // Actualizar progreso final
    uploadProgressStore[dataKey] = {
      progress: 100,
      stage: 'Procesamiento completado',
      updated: Date.now(),
      complete: true
    };
    
    // Calcular estadísticas de filtrado de holdings
    let totalHoldings = 0;
    let filteredHoldings = 0;
    
    etfData.forEach(etf => {
      totalHoldings += etf.totalHoldings || 0;
      filteredHoldings += etf.filteredHoldings || 0;
      
      // Limpiar propiedades de conteo que no necesitamos enviar al frontend
      delete etf.totalHoldings;
      delete etf.filteredHoldings;
    });
    
    const filteredPercentage = totalHoldings > 0 ? (filteredHoldings / totalHoldings * 100).toFixed(1) : 0;
    
    console.log(`📊 [${requestId}] Respuesta completada: ${etfData.length} ETFs procesados correctamente`);
    
    // Limitar la cantidad de datos devueltos para evitar respuestas demasiado grandes
    const etfDataForResponse = etfData.map(etf => {
      const limitedHoldings = etf.holdings.slice(0, 50); // Solo devolver hasta 50 holdings por ETF
      
      return {
        ticker: etf.ticker,
        weight: etf.weight,
        // Compactar los sectores para reducir tamaño
        sectors: etf.sectors.filter(s => s.weight > 0.01).map(s => ({
          s: s.sector.substring(0, 30), // Abreviar nombres de sectores largos
          w: parseFloat(s.weight.toFixed(4)) // Reducir precisión a 4 decimales
        })),
        // Compactar los holdings para reducir tamaño
        holdings: limitedHoldings.map(h => ({
          s: h.symbol,
          d: h.description.substring(0, 40), // Abreviar descripciones largas
          w: parseFloat(h.weight.toFixed(4)) // Reducir precisión a 4 decimales
        })),
        holdingsCount: etf.holdings.length // Informar cantidad total
      };
    });
    
    const responsePayload = {
      success: true,
      dataKey,
      totalRows: dataRows.length,
      etfCount: etfDataForResponse.length,
      message: 'Archivo procesado completamente',
      etfData: etfDataForResponse,
      filteredInfo: {
        totalHoldings,
        filteredHoldings,
        filteredPercentage: `${filteredPercentage}%`,
        message: `Se han filtrado ${filteredHoldings} holdings con menos de 0.1% de participación (${filteredPercentage}% del total)`
      }
    };
    
    // Estimar tamaño de respuesta
    const payloadSize = JSON.stringify(responsePayload).length;
    console.log(`📊 [${requestId}] Tamaño estimado de respuesta: ${(payloadSize / (1024 * 1024)).toFixed(2)} MB`);
    
    // Comprobar si la respuesta es demasiado grande
    if (payloadSize > 6 * 1024 * 1024) { // Más de 6MB
      console.warn(`📊 [${requestId}] Advertencia: Respuesta muy grande (${(payloadSize / (1024 * 1024)).toFixed(2)} MB)`);
      
      // Reducir aún más si es necesario
      responsePayload.etfData = responsePayload.etfData.map(etf => ({
        ...etf,
        holdings: etf.holdings.slice(0, 20) // Reducir a solo 20 holdings por ETF
      }));
      
      console.log(`📊 [${requestId}] Reduciendo holdings a 20 por ETF para disminuir tamaño`);
    }
    
    // Devolver datos procesados
    console.log(`📊 [${requestId}] Enviando respuesta con ${etfDataForResponse.length} ETFs procesados`);
    return res.status(200).json(responsePayload);
    
  } catch (error) {
    console.error(`📊 [${requestId}] Error general procesando archivo Excel:`, error);
    // Si ya hemos enviado una respuesta, no intentar enviar otra
    if (res.headersSent) {
      console.error(`📊 [${requestId}] Error después de enviar encabezados - no se puede responder`);
      return;
    }
    return res.status(500).json({ 
      error: 'Error procesando los datos', 
      details: sanitizeError(error, 'processing'),
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Endpoint para procesar archivos más pequeños (recomendado para Firebase)
app.post('/process-etf-excel-lite', uploadMiddleware, async (req, res) => {
  console.log('📊 Solicitud recibida en /process-etf-excel-lite (versión lite)');
  
  // Establecer un timeout adecuado para archivos pequeños
  req.setTimeout(300000); // 5 minutos
  res.setTimeout(300000);
  
  // Crear un ID único para rastrear la solicitud en los registros
  const requestId = Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
  console.log(`📊 [${requestId}] Iniciando procesamiento LITE de solicitud`);
  
  try {
    // El archivo ya se ha verificado en el middleware
    const fileSize = (req.file.size / (1024 * 1024)).toFixed(2);
    console.log(`📊 [${requestId}] Procesando archivo Excel: ${req.file.originalname}, Tamaño: ${fileSize} MB`);
    
    // Verificar tamaño ideal para versión lite
    if (req.file.size > 15 * 1024 * 1024) {
      console.warn(`📊 [${requestId}] Advertencia: Archivo grande (${fileSize} MB) para procesamiento lite`);
      // No rechazamos, pero advertimos en logs
    }
    
    // Obtener parámetros de la solicitud
    const userId = req.body.userId || 'anonymous';
    const origin = req.body.origin || 'etf-analyzer';
    
    // Crear ID único para este conjunto de datos
    const dataKey = `excel_lite_${userId}_${requestId}`;
    
    // Inicializar progreso
    uploadProgressStore[dataKey] = {
      progress: 10,
      stage: 'Leyendo archivo Excel (versión lite)',
      updated: Date.now()
    };
    
    // Opciones para Excel con optimizaciones para archivos pequeños
    const options = { 
      type: 'buffer',
      cellStyles: false,
      cellNF: false,
      cellDates: false,
      cellText: false,
      cellFormula: false,
      dense: true,
      raw: true,
      sheetStubs: false
    };
    
    // Leer el archivo Excel
    let workbook;
    try {
      workbook = XLSX.read(req.file.buffer, options);
      console.log(`📊 [${requestId}] Archivo Excel leído correctamente`);
    } catch (xlsxError) {
      console.error(`📊 [${requestId}] Error al leer archivo Excel:`, xlsxError);
      return res.status(422).json({ 
        error: 'No se pudo leer el archivo Excel', 
        details: sanitizeError(xlsxError, 'xlsx parsing') 
      });
    }
    
    // Liberar memoria
    req.file.buffer = null;
    
    // Actualizar progreso
    uploadProgressStore[dataKey] = {
      ...uploadProgressStore[dataKey],
      progress: 30,
      stage: 'Extrayendo datos (versión lite)',
      updated: Date.now()
    };
    
    // Obtener la primera hoja
    if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
      return res.status(422).json({ error: 'El archivo Excel no contiene hojas' });
    }
    
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    
    if (!worksheet) {
      return res.status(422).json({ error: `No se pudo acceder a la hoja ${sheetName}` });
    }
    
    // Limite de filas más pequeño para la versión lite
    const MAX_ROWS_LITE = 10000;
    
    // Convertir a JSON con límite de filas para versión lite
    let jsonData;
    try {
      jsonData = XLSX.utils.sheet_to_json(worksheet, { 
        header: 1,
        defval: null,
        blankrows: false,
        sheetRows: MAX_ROWS_LITE // Limitamos el número de filas
      });
      console.log(`📊 [${requestId}] Datos extraídos: ${jsonData.length} filas (máximo ${MAX_ROWS_LITE})`);
      
      if (jsonData.length >= MAX_ROWS_LITE) {
        console.warn(`📊 [${requestId}] Se alcanzó el límite de filas. Se procesaron solo ${MAX_ROWS_LITE} filas.`);
      }
    } catch (jsonError) {
      return res.status(422).json({ 
        error: 'Error al convertir los datos de Excel', 
        details: sanitizeError(jsonError, 'json parsing') 
      });
    }
    
    // Liberar memoria
    workbook = null;
    worksheet = null;
    
    // Actualizar progreso
    uploadProgressStore[dataKey] = {
      ...uploadProgressStore[dataKey],
      progress: 50,
      stage: 'Filtrando datos (versión lite)',
      updated: Date.now()
    };
    
    // Filtrar y normalizar datos
    const filteredData = jsonData
      .filter(row => 
        row && row.length > 0 && row.some(cell => cell !== null && cell !== undefined && cell !== '')
      )
      .map(row => {
        const limitedRow = row.slice(0, 5);
        while (limitedRow.length < 5) limitedRow.push(null);
        return limitedRow;
      });
    
    // Liberar memoria
    jsonData = null;
    
    // Mapeo de columnas (igual que endpoint normal)
    const columnMapping = {
      'ETF': 0,
      'Ticker de empresa': 1,
      'Nombre empresa': 2,
      'Porcentaje': 3,
      'Sector': 4
    };
    
    // Actualizar progreso
    uploadProgressStore[dataKey] = {
      ...uploadProgressStore[dataKey],
      progress: 70,
      stage: 'Procesando datos ETF (versión lite)',
      updated: Date.now()
    };
    
    // Procesar datos
    if (filteredData.length <= 1) {
      return res.status(422).json({ 
        error: 'El archivo no contiene datos válidos',
        details: 'Se esperaban al menos dos filas (encabezado y datos)'
      });
    }
    
    const dataRows = filteredData.slice(1);
    console.log(`📊 [${requestId}] Procesando ${dataRows.length} filas de datos`);
    
    // Limitar aún más los datos para procesar en versión lite
    const MAX_PROCESS_ROWS = 5000;
    const processRows = dataRows.length > MAX_PROCESS_ROWS 
      ? dataRows.slice(0, MAX_PROCESS_ROWS) 
      : dataRows;
    
    if (dataRows.length > MAX_PROCESS_ROWS) {
      console.warn(`📊 [${requestId}] Limitando procesamiento a ${MAX_PROCESS_ROWS} filas de las ${dataRows.length} disponibles`);
    }
    
    // Procesar con tamaño de lote más pequeño para versión lite
    let etfMap;
    try {
      // Usar un tamaño de lote más pequeño para versión lite
      const BATCH_SIZE_LITE = 200;
      etfMap = await processETFDataOptimized(processRows, columnMapping, BATCH_SIZE_LITE);
      console.log(`📊 [${requestId}] Datos procesados: ${etfMap.size} ETFs encontrados`);
    } catch (procError) {
      return res.status(500).json({ 
        error: 'Error al procesar los datos ETF', 
        details: sanitizeError(procError, 'processing') 
      });
    }
    
    // Liberar memoria
    filteredData.length = 0;
    
    const etfData = Array.from(etfMap.values());
    
    // Actualizar progreso final
    uploadProgressStore[dataKey] = {
      progress: 100,
      stage: 'Procesamiento completado (versión lite)',
      updated: Date.now(),
      complete: true
    };
    
    // Calcular estadísticas
    let totalHoldings = 0;
    let filteredHoldings = 0;
    
    etfData.forEach(etf => {
      totalHoldings += etf.totalHoldings || 0;
      filteredHoldings += etf.filteredHoldings || 0;
      delete etf.totalHoldings;
      delete etf.filteredHoldings;
    });
    
    const filteredPercentage = totalHoldings > 0 ? (filteredHoldings / totalHoldings * 100).toFixed(1) : 0;
    
    // Crear respuesta compacta para versión lite
    const etfDataForResponse = etfData.map(etf => {
      // Limitar a 30 holdings por ETF en versión lite
      const limitedHoldings = etf.holdings.slice(0, 30);
      
      return {
        ticker: etf.ticker,
        weight: etf.weight,
        // Muy compacto para reducir tamaño
        sectors: etf.sectors.filter(s => s.weight > 0.02).map(s => ({
          s: s.sector.substring(0, 20), // Más corto que versión normal
          w: parseFloat(s.weight.toFixed(3)) // Menos precisión
        })),
        holdings: limitedHoldings.map(h => ({
          s: h.symbol,
          d: h.description.substring(0, 30), // Más corto que versión normal
          w: parseFloat(h.weight.toFixed(3)) // Menos precisión
        })),
        holdingsCount: etf.holdings.length
      };
    });
    
    // Respuesta
    const responsePayload = {
      success: true,
      dataKey,
      isLiteVersion: true, // Indicar que es versión lite
      totalRowsInFile: jsonData ? jsonData.length : 'desconocido',
      processedRows: processRows.length,
      limitedData: dataRows.length > MAX_PROCESS_ROWS, // Indica si se limitaron datos
      etfCount: etfDataForResponse.length,
      message: 'Archivo procesado completamente (versión lite)',
      etfData: etfDataForResponse,
      filteredInfo: {
        totalHoldings,
        filteredHoldings,
        filteredPercentage: `${filteredPercentage}%`
      }
    };
    
    console.log(`📊 [${requestId}] Enviando respuesta lite con ${etfDataForResponse.length} ETFs`);
    return res.status(200).json(responsePayload);
    
  } catch (error) {
    console.error(`📊 [${requestId}] Error en procesamiento lite:`, error);
    if (res.headersSent) return;
    
    return res.status(500).json({ 
      error: 'Error procesando los datos (versión lite)', 
      details: sanitizeError(error, 'processing')
    });
  }
});

/*app.listen(3100, () => {
    console.log(`Server running on http://localhost:${3100}`);
  });
// Si se ejecuta localmente (no en Firebase), iniciar el servidor
if (process.env.NODE_ENV === 'development') {
  const port = process.env.PORT || 3100;
  app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
  });
}*/

// =============================================================================
// ATTRIBUTION API - Portfolio Performance Attribution
// =============================================================================

const { 
  getPortfolioAttribution, 
  getTopContributors, 
  checkAttributionAvailability 
} = require('./services/attribution');

/**
 * GET /attribution
 * 
 * Obtiene la atribución completa del portafolio.
 * Calcula la contribución de cada activo al rendimiento total.
 * 
 * INTRADAY-001: Ahora incluye rendimiento intraday en tiempo real por defecto.
 * FEAT-UX-001: Ahora acepta startDate/endDate opcionales para rangos exactos.
 * 
 * Query params:
 * - userId: (required) ID del usuario
 * - period: Período de análisis ('YTD', '1M', '3M', '6M', '1Y', '2Y', 'ALL')
 * - startDate: (optional) Fecha inicio explícita (YYYY-MM-DD). Sobreescribe period.
 * - endDate: (optional) Fecha fin explícita (YYYY-MM-DD). Default: hoy.
 * - currency: Moneda para cálculos ('USD', 'COP', 'EUR', etc.)
 * - accountIds: Comma-separated list de IDs de cuenta o 'overall'
 * - benchmarkReturn: Retorno del benchmark para comparar
 * - maxBars: Máximo de barras en waterfall (default: 8)
 * - portfolioReturn: (optional) TWR pre-calculado del frontend para consistencia
 * - includeIntraday: (optional) Incluir rendimiento intraday (default: 'true')
 */
app.get("/attribution", async (req, res) => {
  const { 
    userId, 
    period = 'YTD', 
    startDate: startDateParam,
    endDate: endDateParam,
    currency = 'USD',
    accountIds = 'overall',
    benchmarkReturn = '0',
    maxBars = '8',
    portfolioReturn,
    includeIntraday = 'true'
  } = req.query;
  
  try {
    if (!userId) {
      return res.status(400).json({
        error: "Por favor, proporcione el parámetro userId"
      });
    }
    
    // FEAT-UX-001: Construir dateRange si se proporcionan fechas explícitas
    let dateRange = undefined;
    if (startDateParam) {
      const startDate = new Date(startDateParam + 'T00:00:00');
      const endDate = endDateParam ? new Date(endDateParam + 'T23:59:59') : new Date();
      if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
        dateRange = { startDate, endDate };
      }
    }
    
    // FIX: Reconocer tanto 'overall' como 'all' como "todas las cuentas"
    const isAllAccounts = accountIds === 'overall' || accountIds === 'all'
    const result = await getPortfolioAttribution({
      userId,
      period,
      currency,
      accountIds: isAllAccounts ? ['overall'] : accountIds.split(','),
      dateRange,
      options: {
        benchmarkReturn: parseFloat(benchmarkReturn) || 0,
        maxWaterfallBars: parseInt(maxBars) || 8,
        includeMetadata: true,
        portfolioReturn: portfolioReturn ? parseFloat(portfolioReturn) : undefined,
        includeIntraday: includeIntraday !== 'false' // INTRADAY-001: true por defecto
      }
    });
    
    if (!result.success) {
      return res.status(404).json({
        error: result.error || 'No attribution data found'
      });
    }
    
    res.status(200).json(result);
    
  } catch (error) {
    console.error('[/attribution] Error:', error);
    res.status(500).json({
      error: sanitizeError(error, 'attribution')
    });
  }
});

/**
 * GET /attribution/top
 * 
 * Obtiene solo los top y bottom contributors.
 * Versión ligera para carga rápida.
 * 
 * Query params:
 * - userId: (required) ID del usuario
 * - period: Período de análisis
 * - currency: Moneda
 * - topN: Número de contributors a retornar (default: 5)
 */
app.get("/attribution/top", async (req, res) => {
  const { 
    userId, 
    period = 'YTD', 
    currency = 'USD',
    topN = '5'
  } = req.query;
  
  try {
    if (!userId) {
      return res.status(400).json({
        error: "Por favor, proporcione el parámetro userId"
      });
    }
    
    const result = await getTopContributors({
      userId,
      period,
      currency,
      topN: parseInt(topN) || 5
    });
    
    if (!result.success) {
      return res.status(404).json({
        error: result.error || 'No attribution data found'
      });
    }
    
    res.status(200).json(result);
    
  } catch (error) {
    console.error('[/attribution/top] Error:', error);
    res.status(500).json({
      error: sanitizeError(error, 'contributors')
    });
  }
});

/**
 * GET /attribution/check
 * 
 * Verifica si hay datos de atribución disponibles para un usuario.
 * 
 * Query params:
 * - userId: (required) ID del usuario
 */
app.get("/attribution/check", async (req, res) => {
  const { userId } = req.query;
  
  try {
    if (!userId) {
      return res.status(400).json({
        error: "Por favor, proporcione el parámetro userId"
      });
    }
    
    const result = await checkAttributionAvailability(userId);
    res.status(200).json(result);
    
  } catch (error) {
    console.error('[/attribution/check] Error:', error);
    res.status(500).json({
      error: sanitizeError(error, 'availability')
    });
  }
});

// ============================================================================
// RISK METRICS ENDPOINTS (STORY-036)
// ============================================================================

const { calculateRiskMetrics } = require('./services/riskMetrics');
const { buildRiskCacheKey, getCachedRiskMetrics, setCachedRiskMetrics } = require('./services/riskMetrics/riskMetricsCache');

/**
 * @swagger
 * /risk-metrics:
 *   get:
 *     summary: Calcula métricas de riesgo del portafolio
 *     description: |
 *       Endpoint para obtener métricas de riesgo (Sharpe, Sortino, Beta, etc.)
 *       con soporte para múltiples cuentas y agregación value-weighted.
 *     tags: [Risk Metrics]
 *     parameters:
 *       - name: userId
 *         in: query
 *         required: true
 *         description: ID del usuario
 *         schema:
 *           type: string
 *       - name: period
 *         in: query
 *         required: false
 *         description: Período de análisis (1M, 3M, 6M, YTD, 1Y, 2Y, ALL)
 *         schema:
 *           type: string
 *           default: YTD
 *       - name: currency
 *         in: query
 *         required: false
 *         description: Moneda para cálculos
 *         schema:
 *           type: string
 *           default: USD
 *       - name: accountIds
 *         in: query
 *         required: false
 *         description: IDs de cuentas separados por coma (vacío = overall)
 *         schema:
 *           type: string
 *       - name: requestId
 *         in: query
 *         required: false
 *         description: ID único de request para debugging race conditions
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Métricas calculadas exitosamente
 *       400:
 *         description: Parámetros faltantes o inválidos
 *       500:
 *         description: Error interno
 */
app.get("/risk-metrics", async (req, res) => {
  const { 
    userId, 
    period = 'YTD', 
    currency = 'USD', 
    accountIds = '',
    requestId = `req_${Date.now()}`
  } = req.query;
  
  console.log(`[/risk-metrics] Request: ${requestId}`, {
    userId,
    period,
    currency,
    accountIds
  });
  
  try {
    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "MISSING_PARAM",
        message: "Por favor, proporcione el parámetro userId"
      });
    }
    
    const validPeriods = ['1M', '3M', '6M', 'YTD', '1Y', '2Y', 'ALL'];
    if (!validPeriods.includes(period)) {
      return res.status(400).json({
        success: false,
        error: "INVALID_PERIOD",
        message: `Período inválido. Valores válidos: ${validPeriods.join(', ')}`
      });
    }
    
    const parsedAccountIds = accountIds
      ? accountIds.split(',').map(id => id.trim()).filter(Boolean)
      : [];
    
    const cacheKey = buildRiskCacheKey(userId, period, currency, parsedAccountIds);
    const cached = getCachedRiskMetrics(cacheKey);
    if (cached) {
      console.log(`[/risk-metrics] Cache hit: ${requestId}`);
      return res.status(200).json(cached);
    }
    
    const result = await calculateRiskMetrics(userId, {
      period,
      currency,
      accountIds: parsedAccountIds,
      requestId
    });
    
    console.log(`[/risk-metrics] Response: ${requestId}`, {
      success: result.success,
      durationMs: result.metadata?.durationMs
    });
    
    // BUGFIX: INSUFFICIENT_DATA no es un error 404, es una respuesta válida con success=false
    // El frontend debe manejar este caso para mostrar un mensaje apropiado al usuario
    // En vez de devolver 404 (que causa error en fetch), devolvemos 200 con success=false
    if (!result.success) {
      // Solo usar 500 para errores reales de servidor
      const statusCode = result.error === 'INSUFFICIENT_DATA' ? 200 : 500;
      return res.status(statusCode).json(result);
    }
    
    setCachedRiskMetrics(cacheKey, result);
    res.status(200).json(result);
    
  } catch (error) {
    console.error(`[/risk-metrics] Error: ${requestId}`, error);
    res.status(500).json({
      success: false,
      error: "SERVER_ERROR",
      message: sanitizeError(error, 'risk metrics'),
      requestId
    });
  }
});

// ============================================================================
// CLOSED POSITIONS ENDPOINTS (STORY-036)
// ============================================================================

const { getClosedPositions } = require('./services/closedPositions');

/**
 * @swagger
 * /closed-positions:
 *   get:
 *     summary: Obtiene posiciones cerradas (trades vendidos)
 *     description: |
 *       Endpoint para obtener historial de posiciones cerradas con métricas,
 *       filtros, ordenamiento y paginación. Soporta múltiples cuentas.
 *     tags: [Closed Positions]
 *     parameters:
 *       - name: userId
 *         in: query
 *         required: true
 *         description: ID del usuario
 *       - name: accountIds
 *         in: query
 *         required: false
 *         description: IDs de cuentas separados por coma (vacío = todas)
 *       - name: startDate
 *         in: query
 *         required: false
 *         description: Fecha inicio filtro (YYYY-MM-DD)
 *       - name: endDate
 *         in: query
 *         required: false
 *         description: Fecha fin filtro (YYYY-MM-DD)
 *       - name: tickers
 *         in: query
 *         required: false
 *         description: Tickers a filtrar separados por coma
 *       - name: currency
 *         in: query
 *         required: false
 *         description: Moneda para valores (default USD)
 *       - name: page
 *         in: query
 *         required: false
 *         description: Página (1-indexed, default 1)
 *       - name: pageSize
 *         in: query
 *         required: false
 *         description: Tamaño de página (default 50, max 200)
 *       - name: sortBy
 *         in: query
 *         required: false
 *         description: Campo de ordenamiento (sellDate, realizedPnL, ticker)
 *       - name: sortOrder
 *         in: query
 *         required: false
 *         description: Orden (asc, desc)
 *       - name: requestId
 *         in: query
 *         required: false
 *         description: ID único de request para debugging
 *     responses:
 *       200:
 *         description: Posiciones obtenidas exitosamente
 *       400:
 *         description: Parámetros faltantes o inválidos
 *       500:
 *         description: Error interno
 */
app.get("/closed-positions", async (req, res) => {
  const { 
    userId, 
    accountIds = '',
    startDate,
    endDate,
    tickers = '',
    currency = 'USD',
    page = '1',
    pageSize = '50',
    sortBy = 'sellDate',
    sortOrder = 'desc',
    requestId = `req_${Date.now()}`
  } = req.query;
  
  console.log(`[/closed-positions] Request: ${requestId}`, {
    userId,
    accountIds,
    page,
    pageSize
  });
  
  try {
    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "MISSING_PARAM",
        message: "Por favor, proporcione el parámetro userId"
      });
    }
    
    const parsedAccountIds = accountIds
      ? accountIds.split(',').map(id => id.trim()).filter(Boolean)
      : [];
    
    const parsedTickers = tickers
      ? tickers.split(',').map(t => t.trim()).filter(Boolean)
      : [];
    
    const validSortFields = ['sellDate', 'buyDate', 'realizedPnL', 'realizedPnLPercent', 'ticker', 'holdingPeriodDays'];
    const safeSortBy = validSortFields.includes(sortBy) ? sortBy : 'sellDate';
    const safeSortOrder = ['asc', 'desc'].includes(sortOrder) ? sortOrder : 'desc';
    
    const result = await getClosedPositions(userId, {
      accountIds: parsedAccountIds,
      startDate,
      endDate,
      tickers: parsedTickers,
      currency,
      page: parseInt(page) || 1,
      pageSize: parseInt(pageSize) || 50,
      sortBy: safeSortBy,
      sortOrder: safeSortOrder,
      requestId
    });
    
    console.log(`[/closed-positions] Response: ${requestId}`, {
      success: result.success,
      positions: result.positions?.length || 0,
      durationMs: result.metadata?.durationMs
    });
    
    if (!result.success) {
      const statusCode = result.error === 'NO_ACCOUNTS' ? 404 : 500;
      return res.status(statusCode).json(result);
    }
    
    res.status(200).json(result);
    
  } catch (error) {
    console.error(`[/closed-positions] Error: ${requestId}`, error);
    res.status(500).json({
      success: false,
      error: "SERVER_ERROR",
      message: sanitizeError(error, 'closed positions'),
      requestId
    });
  }
});

// ============================================================================
// BATCH NEWS ENDPOINT (STORY-036)
// ============================================================================

/**
 * @swagger
 * /news/batch:
 *   get:
 *     summary: Obtiene noticias para múltiples símbolos en una sola llamada
 *     description: |
 *       Endpoint batch para obtener noticias de hasta 10 símbolos en paralelo.
 *       Reduce N+1 queries del frontend al consolidar múltiples llamadas.
 *     tags: [News]
 *     parameters:
 *       - name: symbols
 *         in: query
 *         required: true
 *         description: Símbolos separados por coma (max 10)
 *         schema:
 *           type: string
 *           example: AAPL,MSFT,GOOGL
 *       - name: limit
 *         in: query
 *         required: false
 *         description: Máximo de noticias por símbolo (default 5)
 *         schema:
 *           type: number
 *       - name: requestId
 *         in: query
 *         required: false
 *         description: ID único de request para debugging
 *     responses:
 *       200:
 *         description: Noticias obtenidas exitosamente
 *       400:
 *         description: Parámetros faltantes o inválidos
 *       500:
 *         description: Error interno
 */
app.get("/news/batch", async (req, res) => {
  const { 
    symbols = '',
    limit = '5',
    requestId = `req_${Date.now()}`
  } = req.query;
  
  const startTime = Date.now();
  
  console.log(`[/news/batch] Request: ${requestId}`, { symbols, limit });
  
  try {
    if (!symbols || symbols.trim() === '') {
      return res.status(400).json({
        success: false,
        error: "MISSING_PARAM",
        message: "Por favor, proporcione el parámetro symbols"
      });
    }
    
    const symbolList = symbols.split(',')
      .map(s => s.trim().toUpperCase())
      .filter(Boolean)
      .slice(0, 10); // Max 10 símbolos
    
    if (symbolList.length === 0) {
      return res.status(400).json({
        success: false,
        error: "INVALID_SYMBOLS",
        message: "No se proporcionaron símbolos válidos"
      });
    }
    
    const limitNum = Math.min(Math.max(1, parseInt(limit) || 5), 20);
    
    // Fetch noticias en paralelo
    const newsPromises = symbolList.map(async (symbol) => {
      try {
        const news = await getNewsFromSymbol(symbol);
        return {
          symbol,
          success: true,
          news: Array.isArray(news) ? news.slice(0, limitNum) : []
        };
      } catch (error) {
        console.warn(`[/news/batch] Error fetching news for ${symbol}:`, error.message);
        return {
          symbol,
          success: false,
          news: [],
          error: error.message
        };
      }
    });
    
    const results = await Promise.all(newsPromises);
    
    // Organizar por símbolo
    const newsBySymbol = {};
    results.forEach(r => {
      newsBySymbol[r.symbol] = {
        success: r.success,
        news: r.news,
        count: r.news.length
      };
    });
    
    // Consolidar todas las noticias ordenadas por fecha
    const allNews = results
      .filter(r => r.success)
      .flatMap(r => r.news.map(n => ({ ...n, symbol: r.symbol })))
      .sort((a, b) => {
        const dateA = new Date(a.pubDate || a.date || 0);
        const dateB = new Date(b.pubDate || b.date || 0);
        return dateB.getTime() - dateA.getTime();
      })
      .slice(0, 50); // Max 50 noticias consolidadas
    
    const successCount = results.filter(r => r.success).length;
    
    const result = {
      success: true,
      newsBySymbol,
      consolidated: allNews,
      metadata: {
        requestId,
        symbolsRequested: symbolList.length,
        symbolsSuccessful: successCount,
        totalNews: allNews.length,
        durationMs: Date.now() - startTime
      }
    };
    
    console.log(`[/news/batch] Response: ${requestId}`, {
      symbols: symbolList.length,
      successful: successCount,
      totalNews: allNews.length,
      durationMs: result.metadata.durationMs
    });
    
    res.status(200).json(result);
    
  } catch (error) {
    console.error(`[/news/batch] Error: ${requestId}`, error);
    res.status(500).json({
      success: false,
      error: "SERVER_ERROR",
      message: sanitizeError(error, 'news'),
      requestId
    });
  }
});

module.exports = app; 