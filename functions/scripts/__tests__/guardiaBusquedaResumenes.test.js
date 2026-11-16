/**
 * HU 6.10.2 — Escenarios 1 y 2: ninguna búsqueda sobre resúmenes y consolidados
 * usa un campo que dejó de estar preparado para búsqueda.
 *
 * **Por qué es un escaneo y no un envoltorio.** Igual que en la 6.2, no hay un
 * repositorio único de resúmenes: cada servicio arma su consulta. Este test
 * recorre el código de los tres sitios donde el proyecto escribe consultas
 * (GoogleFinanceAPI, la aplicación y los scripts de operación de la raíz) y,
 * en cada consulta cuya colección es `performanceSnapshots` o un consolidado
 * (`…/periods`), comprueba que sus `.where()`/`.orderBy()` usan solo campos y
 * órdenes conservados (RN-6.10.2-J, RN-6.10-M).
 *
 * Al contrario que el guardián de la 6.2, que busca nombres de cifras en
 * cualquier consulta, aquí cada campo se ata **a la colección** de su consulta:
 * `userId` es legítimo en `portfolioAccounts` y no dice nada de los resúmenes.
 *
 * Vigila también el orden descendente por ID, que en un grupo con el comodín
 * falla (probado en producción sobre `dates` el 2026-11-16).
 *
 * Con `IMPRIMIR_INVENTARIO=1` imprime la lista de búsquedas encontradas: es la
 * que va al `dev-record.md` (QA-01).
 *
 * @see platform-docs/stories/6.10.2-indices-solo-donde-se-busca/refinamiento.md (T6)
 */

const fs = require('fs');
const path = require('path');

const {
  ASC,
  DESC,
  esBuscable,
  mensajeDeCampoNoBuscable,
} = require('../camposBuscablesResumenes');

const RAIZ_CF = path.resolve(__dirname, '../..');
const RAIZ_FE = path.resolve(__dirname, '../../../../portafolio-inversiones');
const RAIZ_PROYECTO = path.resolve(__dirname, '../../../../..');

/** Dónde se busca: las carpetas donde este proyecto escribe consultas. */
const AMBITOS = [
  { repo: 'GoogleFinanceAPI', raiz: RAIZ_CF, carpetas: ['services', 'utils', 'scripts', 'tests'] },
  {
    repo: 'portafolio-inversiones',
    raiz: RAIZ_FE,
    carpetas: ['app', 'components', 'hooks', 'lib', 'stores', 'utils', 'models', 'types', 'tests'],
  },
  { repo: '(raíz)', raiz: RAIZ_PROYECTO, carpetas: ['scripts'] },
];

const EXTENSIONES = ['.js', '.jsx', '.ts', '.tsx'];
const IGNORADOS = new Set(['node_modules', '.next', 'coverage', 'dist', 'out', '__snapshots__']);

/** Dónde termina una consulta: la lectura o el fin de la sentencia. */
const FIN_DE_CONSULTA = /;|\.get\(|\.stream\(|\.onSnapshot\(|\.count\(|\n\s*\n/;

/** `.where('campo', …)` / `.orderBy('campo', 'desc')`, con punto (admin) o sin él (SDK modular). */
const CONDICION = /(?:\.|\b)(where|orderBy)\(\s*(['"`])([^'"`$]+)\2(?:\s*,\s*(['"`])([^'"`$]*)\4)?/g;

/** Orden descendente por ID, que un grupo con el comodín no sirve. */
const ID_DESCENDENTE = /orderBy\(\s*(?:[\w.]*FieldPath\.documentId\(\)|['"`]__name__['"`])\s*,\s*['"`]desc['"`]/;

/**
 * El grupo de colección al que apunta el argumento de un `collection(…)`.
 *
 * @param {string} argumento Texto entre los paréntesis.
 * @returns {string|null} `performanceSnapshots`, `periods` o `null`.
 */
function grupoDe(argumento) {
  if (argumento.includes('performanceSnapshots')) return 'performanceSnapshots';
  if (/\bperiods\b|consolidatedPeriods|PATHS\.CONSOLIDATED/.test(argumento)) return 'periods';
  return null;
}

/**
 * Todos los ficheros de código de una carpeta, en profundidad.
 *
 * @param {string} dir Carpeta a recorrer.
 * @param {Array<string>} acumulado Rutas encontradas.
 * @returns {Array<string>} `acumulado`, con las rutas añadidas.
 */
function recorrerCarpeta(dir, acumulado = []) {
  if (!fs.existsSync(dir)) return acumulado;

  for (const entrada of fs.readdirSync(dir, { withFileTypes: true })) {
    if (IGNORADOS.has(entrada.name)) continue;
    const ruta = path.join(dir, entrada.name);

    if (entrada.isDirectory()) recorrerCarpeta(ruta, acumulado);
    else if (EXTENSIONES.includes(path.extname(entrada.name))) acumulado.push(ruta);
  }

  return acumulado;
}

/**
 * Índice del paréntesis que cierra el que abre en `inicio`.
 *
 * @param {string} texto Código.
 * @param {number} inicio Posición del `(`.
 * @returns {number} Posición del `)` que lo cierra, o `-1`.
 */
function cierreDeParentesis(texto, inicio) {
  let profundidad = 0;
  for (let i = inicio; i < texto.length; i++) {
    if (texto[i] === '(') profundidad++;
    else if (texto[i] === ')') {
      profundidad--;
      if (profundidad === 0) return i;
    }
  }
  return -1;
}

/**
 * El texto de una consulta: desde `desde` hasta su lectura o el fin de la sentencia.
 *
 * @param {string} texto Código.
 * @param {number} desde Posición de inicio.
 * @returns {string} El tramo de la consulta.
 */
function tramoDeConsulta(texto, desde) {
  const resto = texto.slice(desde);
  const fin = resto.search(FIN_DE_CONSULTA);
  return fin === -1 ? resto : resto.slice(0, fin);
}

/**
 * Las condiciones de un tramo de consulta, con su posición absoluta.
 *
 * @param {string} tramo Texto de la consulta.
 * @param {number} desplazamiento Posición del tramo en el fichero.
 * @returns {Array<{metodo: string, campo: string, orden: string, posicion: number}>}
 */
function condicionesDe(tramo, desplazamiento) {
  const condiciones = [];

  for (const m of tramo.matchAll(CONDICION)) {
    const [, metodo, , campo, , segundo] = m;
    const orden = metodo === 'orderBy' && String(segundo).toLowerCase() === DESC ? DESC : ASC;
    condiciones.push({ metodo, campo, orden, posicion: desplazamiento + m.index });
  }

  const desc = tramo.match(ID_DESCENDENTE);
  if (desc) {
    condiciones.push({ metodo: 'orderBy', campo: '__name__', orden: DESC, posicion: desplazamiento + desc.index });
  }

  return condiciones;
}

/**
 * La ruta de un `collection(…)` cuando llega en una variable.
 *
 * Varios servicios guardan la ruta antes de consultar
 * (`const monthlyPath = \`…/periods\`; db.collection(monthlyPath)`). Sin
 * resolverla, esas consultas no se verían. Se toma la última asignación de esa
 * variable anterior a la consulta. Un parámetro de función no se puede resolver,
 * y por eso el test de control exige encontrar las búsquedas conocidas.
 *
 * @param {string} contenido Código del fichero.
 * @param {string} argumento Argumento del `collection(…)`, sin espacios alrededor.
 * @param {number} hasta Posición de la consulta.
 * @returns {string} El valor asignado, o el propio argumento si no es una variable.
 */
function resolverRuta(contenido, argumento, hasta) {
  if (!/^\w+$/.test(argumento)) return argumento;

  const asignacion = new RegExp(`(?:const|let|var)\\s+${argumento}\\s*=\\s*([^;\\n]+)`, 'g');
  let valor = argumento;
  for (const m of contenido.slice(0, hasta).matchAll(asignacion)) valor = m[1];
  return valor;
}

/**
 * Las búsquedas de un fichero sobre resúmenes y consolidados.
 *
 * Mira dos formas: la cadena que sale de `collection(…)` y la que sale de una
 * variable que guardó esa colección (`const snapshots = db.collection(…)` →
 * `snapshots.where(…)`).
 *
 * @param {string} contenido Código del fichero.
 * @returns {Array<{grupo: string, metodo: string, campo: string, orden: string, posicion: number}>}
 */
function busquedasDe(contenido) {
  const busquedas = [];
  const variables = [];

  for (const m of contenido.matchAll(/\bcollection(?:Group)?\(/g)) {
    const abre = m.index + m[0].length - 1;
    const cierra = cierreDeParentesis(contenido, abre);
    if (cierra === -1) continue;

    const grupo = grupoDe(resolverRuta(contenido, contenido.slice(abre + 1, cierra).trim(), m.index));
    if (!grupo) continue;

    const tramo = tramoDeConsulta(contenido, cierra + 1);
    condicionesDe(tramo, cierra + 1).forEach((c) => busquedas.push({ grupo, ...c }));

    // ¿Se guardó la colección en una variable? Solo si la sentencia no sigue encadenando.
    const inicioLinea = contenido.lastIndexOf('\n', m.index) + 1;
    const prefijo = contenido.slice(inicioLinea, m.index);
    const asignacion = prefijo.match(/(?:const|let|var)\s+(\w+)\s*=\s*(?:await\s+)?[\w.]*$/);
    if (asignacion) variables.push({ nombre: asignacion[1], grupo, desde: cierra });
  }

  for (const { nombre, grupo, desde } of variables) {
    const uso = new RegExp(`\\b${nombre}\\s*\\.\\s*(?=where\\(|orderBy\\()`, 'g');
    uso.lastIndex = desde;
    for (let m = uso.exec(contenido); m; m = uso.exec(contenido)) {
      const inicio = m.index + m[0].length - 1;
      condicionesDe(tramoDeConsulta(contenido, inicio), inicio).forEach((c) => busquedas.push({ grupo, ...c }));
    }
  }

  return busquedas;
}

/**
 * El inventario completo: cada búsqueda con su fichero y línea.
 *
 * @returns {Array<Object>} `repo`, `archivo`, `linea`, `grupo`, `metodo`, `campo`, `orden`.
 */
function inventario() {
  const hallazgos = [];

  for (const ambito of AMBITOS) {
    for (const carpeta of ambito.carpetas) {
      for (const fichero of recorrerCarpeta(path.join(ambito.raiz, carpeta))) {
        // Este test nombra campos prohibidos a propósito.
        if (fichero.startsWith(__dirname)) continue;

        const contenido = fs.readFileSync(fichero, 'utf8');
        if (!/performanceSnapshots|periods|PATHS\.CONSOLIDATED/.test(contenido)) continue;

        for (const b of busquedasDe(contenido)) {
          hallazgos.push({
            repo: ambito.repo,
            archivo: path.relative(ambito.raiz, fichero).replace(/\\/g, '/'),
            linea: contenido.slice(0, b.posicion).split('\n').length,
            grupo: b.grupo,
            metodo: b.metodo,
            campo: b.campo,
            orden: b.orden,
          });
        }
      }
    }
  }

  return hallazgos;
}

const INVENTARIO = inventario();

if (process.env.IMPRIMIR_INVENTARIO === '1') {
  // eslint-disable-next-line no-console
  console.log(
    INVENTARIO.map((h) => `${h.grupo}\t${h.metodo}('${h.campo}' ${h.orden})\t${h.repo}/${h.archivo}:${h.linea}`).join('\n'),
  );
}

/** ¿El inventario tiene una búsqueda así? */
const encontrada = ({ grupo, campo, orden, archivo }) =>
  INVENTARIO.some((h) => h.grupo === grupo && h.campo === campo && h.orden === orden && h.archivo.endsWith(archivo));

describe('HU 6.10.2 — Escenario 1: ninguna búsqueda usa un campo que deja de estar preparado', () => {
  it('toda búsqueda sobre resúmenes y consolidados usa campos y órdenes conservados (RN-6.10.2-J)', () => {
    const prohibidas = INVENTARIO.filter((h) => !esBuscable(h.grupo, h.campo, h.orden));

    if (prohibidas.length > 0) {
      const detalle = prohibidas
        .map((h) => `  ${h.repo}/${h.archivo}:${h.linea} → ${h.grupo}.${h.metodo}('${h.campo}', ${h.orden})`)
        .join('\n');
      const primera = prohibidas[0];
      throw new Error(
        `${mensajeDeCampoNoBuscable(primera.grupo, primera.campo, primera.orden)}\n\nBúsquedas encontradas:\n${detalle}`,
      );
    }

    expect(prohibidas).toEqual([]);
  });
});

describe('HU 6.10.2 — Escenario 2: la lista de búsquedas es la del código (QA-01)', () => {
  it('encuentra las búsquedas conocidas de los resúmenes (R1–R4)', () => {
    expect(encontrada({ grupo: 'performanceSnapshots', campo: 'userId', orden: ASC, archivo: 'handlers/accountHandlers.js' })).toBe(true);
    expect(encontrada({ grupo: 'performanceSnapshots', campo: 'accountId', orden: ASC, archivo: 'handlers/accountHandlers.js' })).toBe(true);
    expect(encontrada({ grupo: 'performanceSnapshots', campo: 'userId', orden: ASC, archivo: 'services/snapshotCleanup.js' })).toBe(true);
    expect(encontrada({ grupo: 'performanceSnapshots', campo: 'type', orden: ASC, archivo: 'services/snapshotCleanup.js' })).toBe(true);
    expect(encontrada({ grupo: 'performanceSnapshots', campo: 'lastUpdated', orden: ASC, archivo: 'services/snapshotCleanup.js' })).toBe(true);
    expect(encontrada({ grupo: 'performanceSnapshots', campo: 'type', orden: ASC, archivo: 'scripts/resumenes-activo.js' })).toBe(true);
  });

  it('encuentra las búsquedas conocidas de los consolidados (C1–C2)', () => {
    expect(encontrada({ grupo: 'periods', campo: 'periodKey', orden: ASC, archivo: 'services/consolidatedReturnsService.js' })).toBe(true);
    expect(encontrada({ grupo: 'periods', campo: 'periodKey', orden: ASC, archivo: 'services/periodConsolidationScheduled.js' })).toBe(true);
    expect(encontrada({ grupo: 'periods', campo: 'periodKey', orden: DESC, archivo: 'gold/verifyHaroldPerformance.js' })).toBe(true);
  });

  it('solo encuentra campos de la lista: ninguna búsqueda por las listas que crecen por día', () => {
    const campos = [...new Set(INVENTARIO.map((h) => `${h.grupo}.${h.campo}`))].sort();

    expect(campos).toEqual([
      'performanceSnapshots.accountId',
      'performanceSnapshots.lastUpdated',
      'performanceSnapshots.type',
      'performanceSnapshots.userId',
      'periods.periodKey',
    ]);
  });

  it('el escaneo mira de verdad los tres sitios', () => {
    // Si una ruta se rompe, el primer test pasaría por no mirar nada.
    AMBITOS.forEach((ambito) => {
      const ficheros = ambito.carpetas.flatMap((c) => recorrerCarpeta(path.join(ambito.raiz, c)));
      expect({ repo: ambito.repo, mira: ficheros.length > 10 }).toEqual({ repo: ambito.repo, mira: true });
    });
  });
});

describe('HU 6.10.2 — el detector', () => {
  it('ata cada campo a la colección de su consulta', () => {
    const codigo = [
      "const a = await db.collection('portfolioAccounts').where('timeline', '==', 1).get();",
      "const b = await db.collection('performanceSnapshots').where('userId', '==', uid).get();",
    ].join('\n');

    expect(busquedasDe(codigo).map((b) => `${b.grupo}.${b.campo}`)).toEqual(['performanceSnapshots.userId']);
  });

  it('sigue la colección guardada en una variable', () => {
    const codigo = [
      "const snapshots = db.collection('performanceSnapshots');",
      "const snap = await snapshots.where('assetDays', '!=', null).get();",
    ].join('\n');

    expect(busquedasDe(codigo).map((b) => b.campo)).toEqual(['assetDays']);
  });

  it('reconoce los consolidados por su ruta y el orden descendente', () => {
    const codigo =
      "db.collection(`${basePath}/${PATHS.CONSOLIDATED}/${PATHS.MONTHLY}`).where('periodKey', '>=', k).orderBy('periodKey', 'desc').get();";

    expect(busquedasDe(codigo).map((b) => `${b.grupo}.${b.campo}.${b.orden}`)).toEqual([
      'periods.periodKey.asc',
      'periods.periodKey.desc',
    ]);
  });

  it('resuelve la ruta guardada en una variable', () => {
    const codigo = [
      'const monthlyPath = `${basePath}/consolidatedPeriods/monthly/periods`;',
      "const snap = await db.collection(monthlyPath).orderBy('result', 'asc').get();",
    ].join('\n');

    expect(busquedasDe(codigo).map((b) => `${b.grupo}.${b.campo}`)).toEqual(['periods.result']);
  });

  it('detecta el orden descendente por ID, que el comodín no sirve', () => {
    const codigo =
      "const snapshots = db.collection('performanceSnapshots');\nsnapshots.where(FieldPath.documentId(), '>=', a).orderBy(FieldPath.documentId(), 'desc').get();";
    const busquedas = busquedasDe(codigo);

    expect(busquedas.map((b) => `${b.campo}.${b.orden}`)).toEqual(['__name__.desc']);
    expect(esBuscable('performanceSnapshots', '__name__', DESC)).toBe(false);
  });

  it('no cuenta el rango ascendente por ID, que sí funciona', () => {
    const codigo =
      "const snapshots = db.collection('performanceSnapshots');\nsnapshots.where(FieldPath.documentId(), '>=', a).where(FieldPath.documentId(), '<', b).get();";

    expect(busquedasDe(codigo)).toEqual([]);
  });
});
