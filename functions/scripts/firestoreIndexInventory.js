/**
 * HU 6.1 — Comparar la configuración de búsqueda declarada contra la activa.
 *
 * **Por qué hace falta.** Hasta ahora los dos `firestore.indexes.json` del
 * workspace se mantenían a mano y nada los contrastaba con producción. El
 * resultado, medido el 20/10/2026: el proyecto de servicios declaraba 33 de las
 * 34 entradas activas y el de la aplicación sólo 28 — entre las seis que le
 * faltaban, la que usa la reconciliación nocturna para encontrar a quién
 * recalcular (`services/reconcileStalePerformance.js`). Publicar desde el
 * proyecto equivocado y aceptar el borrado de sobrantes la dejaba sin índice, y
 * el proceso no fallaba: terminaba «bien» sin encontrar a nadie.
 *
 * **Por qué en un módulo propio y probado.** El CLI de Firebase devuelve cada
 * índice activo con un campo final `__name__` implícito que los ficheros
 * declarados no escriben, y con una clave `density` que tampoco está declarada.
 * Comparar en crudo da 34 diferencias falsas. Esa normalización es la decisión
 * que sostiene todo el guardia, así que vive en una función pura con tests y no
 * repartida por el CLI.
 *
 * Este módulo **no lee ficheros, no toca la red y no imprime**: recibe objetos
 * ya parseados y devuelve el veredicto. El borde —leer, lanzar `firebase`,
 * escribir— es `scripts/firestore-indexes.js` en la raíz del workspace.
 *
 * @module scripts/firestoreIndexInventory
 * @see platform-docs/stories/6.1-fuente-unica-configuracion-busqueda/refinamiento.md (T3)
 */

/**
 * Campo que Firestore añade a todo índice compuesto y que los ficheros
 * declarados nunca escriben. Siempre es el último.
 */
const CAMPO_IMPLICITO = '__name__';

/** Ámbito que asume Firestore cuando el índice declarado no lo dice. */
const QUERY_SCOPE_POR_DEFECTO = 'COLLECTION';

// ============================================================================
// NORMALIZACIÓN
// ============================================================================

/**
 * Dirección de un campo del índice, sea cual sea la forma en que viene.
 *
 * @param {Object} campo Campo tal cual aparece en el JSON.
 * @returns {string} `ASCENDING`, `CONTAINS`, `VECTOR:{…}`… o `?` si no la trae.
 */
function direccionDeCampo(campo) {
  if (campo.order) return campo.order;
  if (campo.arrayConfig) return campo.arrayConfig;
  if (campo.vectorConfig) return `VECTOR:${JSON.stringify(campo.vectorConfig)}`;
  return '?';
}

/**
 * Clave canónica de un índice compuesto: dos índices con la misma clave son el
 * mismo índice para Firestore, aunque el JSON no sea idéntico.
 *
 * Se descarta `__name__` —implícito en lo activo, ausente en lo declarado— y
 * cualquier clave que el CLI añada al informar (`density`): comparar sin esto
 * marca como distintas las 34 entradas que sí coinciden.
 *
 * @param {Object} indice Índice declarado o activo.
 * @returns {string} Clave estable, comparable con `===`.
 */
function normalizarIndice(indice) {
  const campos = (indice.fields || [])
    .filter((campo) => campo.fieldPath !== CAMPO_IMPLICITO)
    .map((campo) => `${campo.fieldPath}:${direccionDeCampo(campo)}`);

  const ambito = indice.queryScope || QUERY_SCOPE_POR_DEFECTO;

  return `${indice.collectionGroup}|${ambito}|${campos.join(',')}`;
}

/**
 * Clave canónica de un `fieldOverride`. Entran en la misma pregunta de borrado
 * que los índices, así que se comparan igual.
 *
 * El orden en que vienen sus configuraciones no significa nada, por eso se
 * ordenan antes de comparar.
 *
 * @param {Object} override Override declarado o activo.
 * @returns {string} Clave estable, comparable con `===`.
 */
function normalizarFieldOverride(override) {
  const configuraciones = (override.indexes || [])
    .map((cfg) => `${direccionDeCampo(cfg)}@${cfg.queryScope || QUERY_SCOPE_POR_DEFECTO}`)
    .sort();

  return `${override.collectionGroup}|${override.fieldPath}|ttl=${override.ttl === true}|${configuraciones.join(',')}`;
}

// ============================================================================
// DESCRIPCIÓN LEGIBLE
// ============================================================================

/**
 * La línea con la que un índice aparece en el informe.
 *
 * @param {Object} indice Índice declarado o activo.
 * @returns {string} Por ejemplo: `portfolioPerformance [COLLECTION] _stale.since:DESCENDING, _stale:DESCENDING`.
 */
function describirIndice(indice) {
  const campos = (indice.fields || [])
    .filter((campo) => campo.fieldPath !== CAMPO_IMPLICITO)
    .map((campo) => `${campo.fieldPath}:${direccionDeCampo(campo)}`)
    .join(', ');

  return `${indice.collectionGroup} [${indice.queryScope || QUERY_SCOPE_POR_DEFECTO}] ${campos}`;
}

/**
 * La línea con la que un `fieldOverride` aparece en el informe.
 *
 * @param {Object} override Override declarado o activo.
 * @returns {string} Por ejemplo: `dates.date (ttl=false)`.
 */
function describirFieldOverride(override) {
  return `${override.collectionGroup}.${override.fieldPath} (ttl=${override.ttl === true})`;
}

// ============================================================================
// COMPARACIÓN
// ============================================================================

/**
 * Compara dos listas por su clave canónica.
 *
 * @param {Array<Object>} izquierda Lo declarado.
 * @param {Array<Object>} derecha Lo activo.
 * @param {function(Object): string} normalizar Cómo se obtiene la clave.
 * @returns {{faltantes: Array<Object>, sobrantes: Array<Object>}} `faltantes` está en `derecha` y no en `izquierda`; `sobrantes` al revés.
 */
function diferencia(izquierda, derecha, normalizar) {
  const clavesIzquierda = new Set(izquierda.map(normalizar));
  const clavesDerecha = new Set(derecha.map(normalizar));

  return {
    faltantes: derecha.filter((elemento) => !clavesIzquierda.has(normalizar(elemento))),
    sobrantes: izquierda.filter((elemento) => !clavesDerecha.has(normalizar(elemento))),
  };
}

/**
 * Contrasta la configuración declarada contra la que está activa en producción.
 *
 * Un `faltante` es una entrada **activa que nadie declara**: es lo que la
 * herramienta de publicación ofrece borrar, y es el daño que persigue esta
 * historia (RN-6.1-J). Un `sobrante` es una entrada declarada que todavía no
 * existe: se creará al publicar, no destruye nada, pero también significa que lo
 * declarado y lo activo no coinciden.
 *
 * @param {{declarada: Object, activa: Object}} param0 Contenido ya parseado de los dos `firestore.indexes.json`.
 * @returns {{indices: Object, fieldOverrides: Object, alineado: boolean}} Diferencias por tipo y el veredicto conjunto.
 */
function compararConActivos({ declarada, activa }) {
  const indices = diferencia(
    declarada.indexes || [],
    activa.indexes || [],
    normalizarIndice,
  );

  const fieldOverrides = diferencia(
    declarada.fieldOverrides || [],
    activa.fieldOverrides || [],
    normalizarFieldOverride,
  );

  const alineado =
    indices.faltantes.length === 0 &&
    indices.sobrantes.length === 0 &&
    fieldOverrides.faltantes.length === 0 &&
    fieldOverrides.sobrantes.length === 0;

  return { indices, fieldOverrides, alineado };
}

/**
 * Contrasta el canónico contra su espejo (RN-6-G: una sola fuente).
 *
 * El espejo lo genera `sync` y nadie debería editarlo a mano; esto es lo que
 * detecta que alguien lo hizo.
 *
 * Índices y configuraciones de campo se devuelven **separados** porque cada uno
 * se describe de una forma distinta; mezclarlos obligaría al informe a adivinar
 * qué tiene entre manos.
 *
 * @param {{canonica: Object, espejo: Object}} param0 Contenido ya parseado de los dos ficheros.
 * @returns {{indices: Object, fieldOverrides: Object, identicos: boolean}} Qué declara cada uno que el otro no.
 */
function compararDeclaraciones({ canonica, espejo }) {
  const indices = diferencia(canonica.indexes || [], espejo.indexes || [], normalizarIndice);
  const overrides = diferencia(
    canonica.fieldOverrides || [],
    espejo.fieldOverrides || [],
    normalizarFieldOverride,
  );

  // `diferencia` nombra desde la izquierda: `faltantes` es lo que tiene el
  // espejo y no el canónico. Aquí se renombra por su dueño, que es lo que el
  // informe necesita decir.
  const porDueno = (par) => ({ soloEnCanonico: par.sobrantes, soloEnEspejo: par.faltantes });

  const resultado = { indices: porDueno(indices), fieldOverrides: porDueno(overrides) };

  resultado.identicos =
    resultado.indices.soloEnCanonico.length === 0 &&
    resultado.indices.soloEnEspejo.length === 0 &&
    resultado.fieldOverrides.soloEnCanonico.length === 0 &&
    resultado.fieldOverrides.soloEnEspejo.length === 0;

  return resultado;
}

/**
 * Por qué NO se puede publicar, si es que no se puede.
 *
 * Las dos direcciones de una divergencia no son la misma cosa, y tratarlas igual
 * era un defecto de la HU 6.1 que la 6.2 destapó al usarlo: una entrada
 * **activa que nadie declara** es el gatillo del daño —al publicar, Firebase
 * ofrece borrarla—, mientras que una entrada **declarada que todavía no está
 * activa** es exactamente lo que un despliegue viene a crear. Bloquear la
 * segunda hacía imposible añadir nada, que es justo el flujo que documenta
 * `FIRESTORE-INDEXES-FUENTE-UNICA.md`.
 *
 * @param {{comparacionActivos: Object, comparacionEspejo: Object|null, mismoTexto: boolean, hayEspejo: boolean}} param0 Lo que produjo la comprobación.
 * @returns {Array<string>} Motivos para no publicar; vacío si se puede.
 */
function motivosParaNoPublicar({ comparacionActivos, comparacionEspejo, mismoTexto, hayEspejo }) {
  const motivos = [];

  if (comparacionActivos.indices.faltantes.length > 0) {
    motivos.push(
      `Hay ${comparacionActivos.indices.faltantes.length} índice(s) activo(s) que nadie declara: ` +
        'publicar ofrecería borrarlos.',
    );
  }
  if (comparacionActivos.fieldOverrides.faltantes.length > 0) {
    motivos.push(
      `Hay ${comparacionActivos.fieldOverrides.faltantes.length} configuración(es) de campo activa(s) ` +
        'que nadie declara: publicar ofrecería borrarlas.',
    );
  }
  if (hayEspejo && (!comparacionEspejo.identicos || !mismoTexto)) {
    motivos.push('El espejo no es una copia exacta del canónico: regenéralo con `sync`.');
  }

  return motivos;
}

// ============================================================================
// INFORME
// ============================================================================

/**
 * Una sección del informe: título y, debajo, una línea por entrada.
 *
 * @param {string} titulo Encabezado de la sección.
 * @param {Array<Object>} entradas Lo que hay que listar.
 * @param {function(Object): string} describir Cómo se escribe cada entrada.
 * @returns {Array<string>} Líneas, o vacío si no hay nada que decir.
 */
function seccion(titulo, entradas, describir) {
  if (entradas.length === 0) return [];
  return [`${titulo} (${entradas.length}):`, ...entradas.map((e) => `    ${describir(e)}`), ''];
}

/**
 * El texto que se imprime cuando hay —o no— diferencias. Es el aviso del
 * Escenario 3: dice qué está desalineado **antes** de publicar nada.
 *
 * @param {{comparacionActivos: Object, comparacionEspejo: Object|null}} param0 Salidas de las dos comparaciones; `comparacionEspejo` es `null` si el repo espejo no está en el workspace.
 * @returns {string} Informe listo para `console.log`.
 */
function formatearInforme({ comparacionActivos, comparacionEspejo }) {
  const lineas = [];

  lineas.push(
    ...seccion(
      '  Activas que NADIE declara — publicar ofrecería BORRARLAS',
      comparacionActivos.indices.faltantes,
      describirIndice,
    ),
  );
  lineas.push(
    ...seccion(
      '  Declaradas que todavía no están activas — publicar las crearía',
      comparacionActivos.indices.sobrantes,
      describirIndice,
    ),
  );
  lineas.push(
    ...seccion(
      '  Configuraciones de campo activas que nadie declara',
      comparacionActivos.fieldOverrides.faltantes,
      describirFieldOverride,
    ),
  );
  lineas.push(
    ...seccion(
      '  Configuraciones de campo declaradas que no están activas',
      comparacionActivos.fieldOverrides.sobrantes,
      describirFieldOverride,
    ),
  );

  if (comparacionEspejo) {
    lineas.push(
      ...seccion(
        '  Sólo en el canónico — el espejo se quedó atrás',
        comparacionEspejo.indices.soloEnCanonico,
        describirIndice,
      ),
    );
    lineas.push(
      ...seccion(
        '  Sólo en el espejo — alguien lo editó a mano',
        comparacionEspejo.indices.soloEnEspejo,
        describirIndice,
      ),
    );
    lineas.push(
      ...seccion(
        '  Configuraciones de campo sólo en el canónico',
        comparacionEspejo.fieldOverrides.soloEnCanonico,
        describirFieldOverride,
      ),
    );
    lineas.push(
      ...seccion(
        '  Configuraciones de campo sólo en el espejo',
        comparacionEspejo.fieldOverrides.soloEnEspejo,
        describirFieldOverride,
      ),
    );
  }

  if (lineas.length === 0) {
    return '  Sin diferencias: lo declarado es exactamente lo que está activo.';
  }

  return lineas.join('\n').trimEnd();
}

module.exports = {
  CAMPO_IMPLICITO,
  QUERY_SCOPE_POR_DEFECTO,
  normalizarIndice,
  normalizarFieldOverride,
  describirIndice,
  describirFieldOverride,
  compararConActivos,
  compararDeclaraciones,
  motivosParaNoPublicar,
  formatearInforme,
};
