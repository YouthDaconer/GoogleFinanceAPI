/**
 * El tope de análisis de **toda la plataforma** en un día.
 *
 * No es el cupo del usuario, que es otra cosa: aquel reparte un derecho entre
 * quienes pagan; éste protege el saldo. Cada análisis consume créditos de
 * Gemini y el saldo es prepago: sin un techo global, un pico de uso —o un fallo
 * que reintente en bucle— puede vaciarlo en una tarde.
 *
 * Se reserva **al lanzar**, no al completar. Un análisis que falla a mitad ya
 * gastó las búsquedas que hizo: para el cupo del usuario no cuenta (RN-5-C),
 * pero para el saldo sí, y este guardia habla de dinero.
 *
 * @module services/copilot/copilotSpendGuard
 */

const { DateTime } = require('luxon');
const admin = require('../firebaseAdmin');
const { ZONA_DEL_CUPO } = require('./copilotConstants');

/** Colección raíz del contador. El id del documento es el día. */
const COLECCION = 'copilotPlatformQuota';

/**
 * Cuántos análisis puede generar la plataforma entera en un día.
 *
 * Medido sobre analisis reales, no estimado: el del 13/10/2026 con 6 activos
 * consumio 41.481 tokens y 8 busquedas, ~800 COP (~$0.20). Tres al dia son
 * ~2.400 COP, que es lo que una recarga de 15.000 aguanta casi una semana.
 *
 * Empieza bajo a proposito. Subirlo es una linea; recuperar un saldo gastado,
 * no. El PO lo mueve segun lo que quiera comprometer.
 */
const ANALISIS_POR_DIA_PLATAFORMA = 3;

/**
 * El día al que se imputa un instante, en la zona del mercado.
 *
 * @param {Date} now
 * @returns {string} `YYYY-MM-DD`.
 */
function dia(now) {
  return DateTime.fromJSDate(now, { zone: ZONA_DEL_CUPO }).toISODate();
}

/**
 * Aparta un análisis del presupuesto de hoy.
 *
 * En una transacción porque dos lanzamientos simultáneos leerían el mismo
 * contador y lo dejarían en uno: justo el caso que el tope existe para evitar.
 *
 * **Si el contador no se puede leer, deja pasar.** Un guardia de gasto que
 * tumba el producto cuando falla Firestore cuesta más de lo que ahorra; el
 * cupo por usuario sigue acotando mientras tanto.
 *
 * @param {Date} [now]
 * @returns {Promise<{concedido: boolean, usados: number}>}
 */
async function reservar(now = new Date()) {
  try {
    const db = admin.firestore();
    const ref = db.collection(COLECCION).doc(dia(now));

    return await db.runTransaction(async (tx) => {
      const doc = await tx.get(ref);
      const usados = doc.exists ? (doc.data().used || 0) : 0;

      if (usados >= ANALISIS_POR_DIA_PLATAFORMA) {
        return { concedido: false, usados };
      }

      tx.set(ref, { used: usados + 1, at: now.toISOString() }, { merge: true });
      return { concedido: true, usados: usados + 1 };
    });
  } catch (error) {
    console.error('[copilotSpendGuard] No se pudo reservar presupuesto:', error.message);
    return { concedido: true, usados: 0 };
  }
}

/**
 * Cuántos análisis lleva hoy la plataforma, sin reservar ninguno.
 *
 * @param {Date} [now]
 * @returns {Promise<number>}
 */
async function usadosHoy(now = new Date()) {
  try {
    const doc = await admin.firestore().collection(COLECCION).doc(dia(now)).get();
    return doc.exists ? (doc.data().used || 0) : 0;
  } catch (error) {
    console.error('[copilotSpendGuard] No se pudo leer el consumo:', error.message);
    return 0;
  }
}

module.exports = {
  COLECCION,
  ANALISIS_POR_DIA_PLATAFORMA,
  dia,
  reservar,
  usadosHoy,
};
