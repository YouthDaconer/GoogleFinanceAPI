/**
 * HU 6.2 — Escenario 4: una búsqueda nueva no se descubre rota en producción.
 *
 * **Por qué es un test y no un envoltorio en tiempo de ejecución.** No hay un
 * repositorio único del historial: al menos diez servicios repiten a mano el
 * mismo `basePath` y construyen su propia consulta. Envolver veinticinco
 * llamadas sería un refactor que esta historia no hace, y además no protegería
 * del código que se escriba mañana. Lo que sí protege es este escaneo: quien
 * estrene un `.where()` o un `.orderBy()` por una cifra de rendimiento se lo
 * encuentra al correr la suite, no en producción con un resultado vacío.
 *
 * @see platform-docs/stories/6.2-dejar-pagar-busqueda-sin-uso/historia.md
 * @see platform-docs/stories/6.2-dejar-pagar-busqueda-sin-uso/refinamiento.md (T8)
 */

const fs = require('fs');
const path = require('path');

const { CIFRAS_VIGILADAS, mensajeDeCampoNoBuscable } = require('../camposBuscablesHistorial');

const RAIZ_CF = path.resolve(__dirname, '../..');
const RAIZ_FE = path.resolve(__dirname, '../../../../portafolio-inversiones');

/** Dónde se busca. Son las carpetas donde este proyecto escribe consultas. */
const AMBITOS = [
  { repo: 'GoogleFinanceAPI', raiz: RAIZ_CF, carpetas: ['services', 'utils', 'scripts', 'tests'] },
  {
    repo: 'portafolio-inversiones',
    raiz: RAIZ_FE,
    carpetas: ['app', 'components', 'hooks', 'lib', 'stores', 'utils', 'models', 'types', 'tests'],
  },
];

const EXTENSIONES = ['.js', '.jsx', '.ts', '.tsx'];
const IGNORADOS = new Set(['node_modules', '.next', 'coverage', 'dist', 'out', '__snapshots__']);

/** `.where('campo', …)` y `.orderBy('campo', …)`, con comilla simple, doble o invertida. */
const CONSULTA = /\.(where|orderBy)\(\s*['"`]([^'"`$]+)['"`]/g;

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
 * Consultas que filtran u ordenan por una cifra de rendimiento.
 *
 * @returns {Array<Object>} Una entrada por aparición: `repo`, `archivo`, `linea`, `campo`, `metodo`.
 */
function buscarConsultasProhibidas() {
  const hallazgos = [];

  for (const ambito of AMBITOS) {
    for (const carpeta of ambito.carpetas) {
      for (const fichero of recorrerCarpeta(path.join(ambito.raiz, carpeta))) {
        // Este test nombra las cifras a propósito; escanearse a sí mismo sería
        // un falso positivo garantizado.
        if (fichero.startsWith(__dirname)) continue;

        const contenido = fs.readFileSync(fichero, 'utf8');
        if (!CIFRAS_VIGILADAS.some((cifra) => contenido.includes(cifra))) continue;

        const lineas = contenido.split('\n');
        lineas.forEach((linea, indice) => {
          for (const coincidencia of linea.matchAll(CONSULTA)) {
            const [, metodo, campo] = coincidencia;
            const raiz = campo.split('.').pop();
            if (!CIFRAS_VIGILADAS.includes(raiz)) continue;

            hallazgos.push({
              repo: ambito.repo,
              archivo: path.relative(ambito.raiz, fichero).replace(/\\/g, '/'),
              linea: indice + 1,
              campo,
              metodo,
            });
          }
        });
      }
    }
  }

  return hallazgos;
}

describe('Escenario 4 — nadie consulta el historial por una cifra de rendimiento', () => {
  it('no hay ningún .where() ni .orderBy() por una cifra excluida (RN-6-F, RN-6.2-M)', () => {
    const hallazgos = buscarConsultasProhibidas();

    // Jest ignora un segundo argumento de `expect`, así que el mensaje
    // explícito que pide RN-6.2-M se lanza a mano: si se dejara al diff del
    // matcher, quien rompa esto vería una lista de objetos y no qué hacer.
    if (hallazgos.length > 0) {
      const detalle = hallazgos
        .map((h) => `  ${h.repo}/${h.archivo}:${h.linea} → .${h.metodo}('${h.campo}')`)
        .join('\n');

      throw new Error(
        `${mensajeDeCampoNoBuscable(hallazgos[0].campo)}\n\nConsultas encontradas:\n${detalle}`,
      );
    }

    expect(hallazgos).toEqual([]);
  });

  it('el escaneo mira de verdad los dos repositorios', () => {
    // Si una ruta se rompe, el test anterior pasaría por no mirar nada: esto es
    // lo que distingue "no hay consultas prohibidas" de "no se buscó".
    AMBITOS.forEach((ambito) => {
      const ficheros = ambito.carpetas.flatMap((c) => recorrerCarpeta(path.join(ambito.raiz, c)));
      expect({ repo: ambito.repo, ficheros: ficheros.length > 100 }).toEqual({
        repo: ambito.repo,
        ficheros: true,
      });
    });
  });

  it('detecta una consulta prohibida cuando la hay', () => {
    // Se comprueba el detector sobre un texto de mentira, no sobre el repo: así
    // el test no depende de que alguien escriba la consulta mala de verdad.
    const linea = "db.collection('dates').orderBy('USD.totalValue', 'desc')";
    const campos = [...linea.matchAll(CONSULTA)].map((m) => m[2].split('.').pop());

    expect(campos).toEqual(['totalValue']);
    expect(CIFRAS_VIGILADAS).toContain('totalValue');
  });

  it('no confunde una consulta legítima por fecha con una prohibida', () => {
    const linea = "db.collection('dates').where('date', '>=', inicio).orderBy('date', 'asc')";
    const campos = [...linea.matchAll(CONSULTA)].map((m) => m[2].split('.').pop());

    expect(campos).toEqual(['date', 'date']);
    campos.forEach((c) => expect(CIFRAS_VIGILADAS).not.toContain(c));
  });
});
