/**
 * Worker de prueba para `ejecutorAislado.test.js` (HU 6.8.2): cada payload
 * dice qué hacer, para provocar cada forma de terminar.
 */
const { atenderTareas } = require('../../ejecutorAislado');

let atendidas = 0;

atenderTareas(async ({ accion, valor }) => {
  atendidas++;
  switch (accion) {
    case 'doblar':
      return { doble: valor * 2, atendidas };
    case 'agotarMemoria': {
      const montones = [];
      for (;;) montones.push(new Array(1e6).fill(Math.random()));
    }
    case 'colgarse':
      return new Promise(() => {});
    case 'fallar':
      throw new Error(`fallo pedido: ${valor}`);
    default:
      throw new Error(`acción desconocida: ${accion}`);
  }
});
