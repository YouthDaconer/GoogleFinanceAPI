/**
 * HU 6.2 — Comparar la medición del historial antes y después de quitarle el
 * material de búsqueda.
 *
 * **Por qué en un módulo propio.** La historia no se da por buena porque el
 * cambio «debería» funcionar: RN-6-B exige comparar antes de aplicar, RN-6.2-L
 * que el ahorro se demuestre con la medición de la propia plataforma, y RN-6-A
 * que ninguna cifra se mueva ni un céntimo. Quien decide si eso se cumple es
 * este módulo, y por eso es **puro** —sin red, sin disco, sin imprimir— y tiene
 * tests. El CLI (`scripts/historial-rendimientos.js`) solo le trae los datos.
 *
 * @module scripts/medicionHistorial
 * @see platform-docs/stories/6.2-dejar-pagar-busqueda-sin-uso/refinamiento.md (T4)
 */

/**
 * Tolerancia del proyecto para comparar montos ya presentados (estándar §4
 * regla 13). Es lo que «idénticas al céntimo» significa aquí.
 */
const EPSILON = 0.01;

/** El espacio tiene que bajar a menos de esta fracción del original (Escenario 1). */
const FRACCION_MAXIMA_ESPACIO = 1 / 3;

/** La latencia no puede empeorar más de esto en su valor típico (RN-6.2-N). */
const EMPEORAMIENTO_MAXIMO_LATENCIA = 0.1;

const BYTES_POR_GIB = 1024 * 1024 * 1024;

// ============================================================================
// ESTADÍSTICA
// ============================================================================

/**
 * Mediana de una muestra.
 *
 * RN-6.2-N habla del **valor típico**, no del promedio: una sola corrida lenta
 * —un arranque en frío, un pico de red— desplaza el promedio y no dice nada del
 * comportamiento habitual.
 *
 * @param {Array<number>} valores Muestras, en cualquier orden.
 * @returns {number|null} La mediana, o `null` si no hay muestras.
 */
function mediana(valores) {
  const limpios = (valores || []).filter((v) => typeof v === 'number' && Number.isFinite(v));
  if (limpios.length === 0) return null;

  const ordenados = [...limpios].sort((a, b) => a - b);
  const medio = Math.floor(ordenados.length / 2);

  return ordenados.length % 2 === 0
    ? (ordenados[medio - 1] + ordenados[medio]) / 2
    : ordenados[medio];
}

// ============================================================================
// ESCENARIO 1 — EL ESPACIO
// ============================================================================

/**
 * ¿Bajó el espacio a menos de la tercera parte? (Escenario 1, RN-6.2-L)
 *
 * @param {{bytesAntes: number, bytesDespues: number}} param0 Las dos mediciones, en bytes.
 * @returns {Object} `gibAntes`, `gibDespues`, `fraccion`, `reduccion` (0–1) y `cumple`.
 */
function compararEspacio({ bytesAntes, bytesDespues }) {
  const fraccion = bytesAntes > 0 ? bytesDespues / bytesAntes : null;

  return {
    bytesAntes,
    bytesDespues,
    gibAntes: bytesAntes / BYTES_POR_GIB,
    gibDespues: bytesDespues / BYTES_POR_GIB,
    fraccion,
    reduccion: fraccion === null ? null : 1 - fraccion,
    cumple: fraccion !== null && fraccion < FRACCION_MAXIMA_ESPACIO,
  };
}

// ============================================================================
// ESCENARIO 3 — LA LATENCIA
// ============================================================================

/**
 * ¿Siguen las consultas por fecha igual de rápidas? (Escenario 3, RN-6.2-N)
 *
 * Se compara **consulta por consulta**: un promedio de las tres escondería que
 * una empeoró mientras otra mejoró.
 *
 * @param {{antes: Object, despues: Object}} param0 Mapas `{nombreDeConsulta: [ms, ms, …]}`.
 * @returns {Object} `consultas[]` con su variación y veredicto, y `cumple` conjunto.
 */
function compararLatencia({ antes, despues }) {
  const nombres = [...new Set([...Object.keys(antes || {}), ...Object.keys(despues || {})])].sort();

  const consultas = nombres.map((nombre) => {
    const medianaAntes = mediana(antes?.[nombre]);
    const medianaDespues = mediana(despues?.[nombre]);

    // Sin una de las dos muestras no hay veredicto posible; decirlo es mejor que
    // dar por buena una consulta que nadie midió.
    if (medianaAntes === null || medianaDespues === null) {
      return { nombre, medianaAntes, medianaDespues, variacion: null, cumple: null, medida: false };
    }

    const variacion = medianaAntes > 0 ? (medianaDespues - medianaAntes) / medianaAntes : 0;

    return {
      nombre,
      medianaAntes,
      medianaDespues,
      variacion,
      cumple: variacion <= EMPEORAMIENTO_MAXIMO_LATENCIA,
      medida: true,
    };
  });

  const medidas = consultas.filter((c) => c.medida);

  return {
    consultas,
    cumple: medidas.length > 0 && medidas.every((c) => c.cumple),
    sinMedir: consultas.filter((c) => !c.medida).map((c) => c.nombre),
  };
}

// ============================================================================
// ESCENARIO 2 — LAS CIFRAS
// ============================================================================

/**
 * Recorre dos estructuras en paralelo y anota cada hoja que no coincide.
 *
 * @param {*} antes Valor de la medición previa.
 * @param {*} despues Valor de la medición posterior.
 * @param {string} ruta Ruta acumulada, para poder nombrar la diferencia.
 * @param {Object} acumulado `{diferencias, soloEnAntes, soloEnDespues}`.
 * @returns {void} Escribe en `acumulado`.
 */
function recorrer(antes, despues, ruta, acumulado) {
  const faltaAntes = antes === undefined;
  const faltaDespues = despues === undefined;

  if (faltaAntes && faltaDespues) return;
  if (faltaDespues) {
    acumulado.soloEnAntes.push(ruta);
    return;
  }
  if (faltaAntes) {
    acumulado.soloEnDespues.push(ruta);
    return;
  }

  const sonObjetos =
    antes !== null &&
    despues !== null &&
    typeof antes === 'object' &&
    typeof despues === 'object' &&
    !Array.isArray(antes) &&
    !Array.isArray(despues);

  if (sonObjetos) {
    const claves = [...new Set([...Object.keys(antes), ...Object.keys(despues)])].sort();
    claves.forEach((clave) => {
      recorrer(antes[clave], despues[clave], ruta ? `${ruta}.${clave}` : clave, acumulado);
    });
    return;
  }

  if (typeof antes === 'number' && typeof despues === 'number') {
    // NaN !== NaN: dos NaN son el mismo valor para esta comparación, y Firestore
    // sí los persiste en campos de tipo double.
    if (Number.isNaN(antes) && Number.isNaN(despues)) return;
    const delta = despues - antes;
    if (delta !== 0) {
      acumulado.diferencias.push({ ruta, antes, despues, delta });
    }
    return;
  }

  if (JSON.stringify(antes) !== JSON.stringify(despues)) {
    acumulado.diferencias.push({ ruta, antes, despues, delta: null });
  }
}

/**
 * ¿Cambió alguna cifra que ve el inversor? (Escenario 2, RN-6-A)
 *
 * El cambio de la HU no escribe ni transforma nada, así que lo esperado es que
 * las cifras sean **exactamente** iguales.
 *
 * **Lo que NO es una diferencia:** los días que aparecen entre una medición y
 * otra. Entre el «antes» y el «después» de los 7 días que pide el Escenario 1,
 * el cierre diario añade unos cinco días nuevos, y la ventana de muestra
 * —los últimos días, íntegros— se desplaza entera. Compararlas en crudo haría
 * fallar el Escenario 2 por razones que no tienen nada que ver con el cambio.
 * Por eso aquí se compara **lo que está en las dos tomas**, y lo que aparece se
 * informa aparte.
 *
 * **Lo que SÍ es una diferencia:** un día que estaba y ya no está. Eso es
 * pérdida de datos, y es justo lo que RN-6.2-J promete que no pasa.
 *
 * @param {{antes: Object, despues: Object}} param0 Los dos volcados `{hashes, muestra}`.
 * @returns {Object} Qué cambió, qué apareció, qué se perdió y los dos veredictos.
 */
function compararCifras({ antes, despues }) {
  const hashesAntes = antes?.hashes || {};
  const hashesDespues = despues?.hashes || {};

  const diasNuevos = Object.keys(hashesDespues).filter((d) => !(d in hashesAntes)).sort();
  const diasPerdidos = Object.keys(hashesAntes).filter((d) => !(d in hashesDespues)).sort();
  const diasComunes = Object.keys(hashesAntes).filter((d) => d in hashesDespues).sort();

  const hashesDistintos = diasComunes
    .filter((dia) => hashesAntes[dia] !== hashesDespues[dia])
    .map((dia) => ({ dia, antes: hashesAntes[dia], despues: hashesDespues[dia] }));

  // La muestra íntegra solo se compara en los días que las dos tomas traen: la
  // ventana se desplaza, y un día que salió de ella no cambió, solo dejó de
  // muestrearse.
  const acumulado = { diferencias: [], soloEnAntes: [], soloEnDespues: [] };
  const muestraAntes = antes?.muestra || {};
  const muestraDespues = despues?.muestra || {};

  Object.keys(muestraAntes)
    .filter((dia) => dia in muestraDespues)
    .sort()
    .forEach((dia) => {
      recorrer(muestraAntes[dia], muestraDespues[dia], dia, acumulado);
    });

  const masAllaDelCentimo = acumulado.diferencias.filter(
    (d) => d.delta === null || Math.abs(d.delta) >= EPSILON,
  );

  // Dentro de un día que SÍ está en las dos tomas, un campo que aparece o
  // desaparece es un cambio: una moneda que se esfuma es exactamente lo que ve
  // el inversor. Esto es distinto de un día entero nuevo, que es el cierre
  // diario haciendo su trabajo.
  const huecosEnDiasComunes = acumulado.soloEnAntes.length + acumulado.soloEnDespues.length;

  return {
    ...acumulado,
    diasComparados: diasComunes.length,
    diasNuevos,
    diasPerdidos,
    hashesDistintos,
    masAllaDelCentimo,
    identicasExactamente:
      hashesDistintos.length === 0 &&
      acumulado.diferencias.length === 0 &&
      diasPerdidos.length === 0 &&
      huecosEnDiasComunes === 0,
    cumple:
      hashesDistintos.length === 0 &&
      masAllaDelCentimo.length === 0 &&
      diasPerdidos.length === 0 &&
      huecosEnDiasComunes === 0,
  };
}

// ============================================================================
// VEREDICTO
// ============================================================================

/**
 * Un porcentaje con un decimal y su signo, para el informe.
 *
 * @param {number|null} fraccion Valor entre 0 y 1 (o negativo).
 * @returns {string} Por ejemplo `+3,4 %` o `sin dato`.
 */
function porcentaje(fraccion) {
  if (fraccion === null || fraccion === undefined) return 'sin dato';
  const signo = fraccion > 0 ? '+' : '';
  return `${signo}${(fraccion * 100).toFixed(1).replace('.', ',')} %`;
}

/**
 * El informe de los tres criterios, listo para mostrar.
 *
 * @param {{espacio: Object, cifras: Object, latencia: Object}} param0 Las tres comparaciones.
 * @returns {string} Informe en texto.
 */
function formatearVeredicto({ espacio, cifras, latencia }) {
  const marca = (cumple) => (cumple === null ? '•' : cumple ? '✅' : '❌');
  const lineas = [];

  lineas.push(`${marca(espacio.cumple)} Escenario 1 — el espacio baja a menos de la tercera parte`);
  lineas.push(
    `     ${espacio.gibAntes.toFixed(3)} GiB → ${espacio.gibDespues.toFixed(3)} GiB ` +
      `(${porcentaje(espacio.reduccion === null ? null : -espacio.reduccion)}, ` +
      `queda el ${espacio.fraccion === null ? 'sin dato' : (espacio.fraccion * 100).toFixed(1).replace('.', ',') + ' %'})`,
  );
  lineas.push('');

  lineas.push(`${marca(cifras.cumple)} Escenario 2 — ninguna cifra cambia`);
  lineas.push(
    cifras.identicasExactamente
      ? `     Las cifras de los ${cifras.diasComparados} días comunes son exactamente iguales.`
      : `     ${cifras.hashesDistintos.length} día(s) con contenido distinto y ` +
        `${cifras.masAllaDelCentimo.length} cifra(s) por encima del céntimo, sobre ` +
        `${cifras.diasComparados} días comparados.`,
  );
  if (cifras.soloEnAntes.length > 0 || cifras.soloEnDespues.length > 0) {
    lineas.push(
      `     ❌ ${cifras.soloEnAntes.length} campo(s) que estaban ya no están y ` +
        `${cifras.soloEnDespues.length} que no estaban aparecieron, dentro de días comunes.`,
    );
    cifras.soloEnAntes.slice(0, 5).forEach((ruta) => lineas.push(`       falta: ${ruta}`));
  }
  if (cifras.diasPerdidos.length > 0) {
    lineas.push(`     ❌ ${cifras.diasPerdidos.length} día(s) que estaban ya no están: ${cifras.diasPerdidos.slice(0, 5).join(', ')}`);
  }
  if (cifras.diasNuevos.length > 0) {
    lineas.push(`     ℹ️  ${cifras.diasNuevos.length} día(s) nuevo(s) desde el «antes» (cierre diario), no cuentan como diferencia.`);
  }
  cifras.hashesDistintos.slice(0, 5).forEach((d) => {
    lineas.push(`       día ${d.dia}: contenido distinto`);
  });
  cifras.masAllaDelCentimo.slice(0, 10).forEach((d) => {
    lineas.push(`       ${d.ruta}: ${d.antes} → ${d.despues}`);
  });
  lineas.push('');

  lineas.push(`${marca(latencia.cumple)} Escenario 3 — las consultas por fecha no empeoran más del 10 %`);
  latencia.consultas.forEach((c) => {
    lineas.push(
      c.medida
        ? `     ${marca(c.cumple)} ${c.nombre}: ${c.medianaAntes.toFixed(0)} ms → ${c.medianaDespues.toFixed(0)} ms (${porcentaje(c.variacion)})`
        : `     • ${c.nombre}: sin medición en una de las dos tomas`,
    );
  });

  return lineas.join('\n');
}

module.exports = {
  EPSILON,
  FRACCION_MAXIMA_ESPACIO,
  EMPEORAMIENTO_MAXIMO_LATENCIA,
  BYTES_POR_GIB,
  mediana,
  compararEspacio,
  compararLatencia,
  compararCifras,
  porcentaje,
  formatearVeredicto,
};
