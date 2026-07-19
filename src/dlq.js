// Dead Letter Queue (DLQ) — red de seguridad para mensajes que fallan su procesamiento.
//
// Sin esto, un mensaje que lanzaba una excepción en su handler se registraba (log.error) y se
// PERDÍA: un bug transitorio, un JSON inesperado o una caída momentánea de Redis se tragaba el
// evento sin rastro. Con la DLQ, ese mensaje se republica en un topic aparte (`dlq`) junto con
// el error, el topic original y el correlationId → queda recuperable e inspeccionable, y se
// puede reprocesar o analizar en vez de desaparecer (patrón de "procesamiento asíncrono": los
// mensajes que fallan varias veces van a una cola especial, no se pierden).
import { producer } from './kafka.js';
import { log } from './logger.js';

const DLQ_TOPIC = process.env.DLQ_TOPIC ?? 'dlq';

// Republica un mensaje fallido a la DLQ. NUNCA relanza: si hasta Kafka está caído, deja el log
// y sigue — el bucle del consumer no debe romperse por un fallo al enrutar a la DLQ.
export async function enviarADLQ({ servicio, topicOriginal, key = null, raw, error, correlationId = null }) {
  try {
    await producer.send({
      topic: DLQ_TOPIC,
      messages: [{
        key,
        value: JSON.stringify({
          servicio,
          topicOriginal,
          error:         String(error?.message ?? error),
          stack:         String(error?.stack ?? '').split('\n').slice(0, 4),
          correlationId,
          fallidoEn:     Date.now(),
          original:      raw, // mensaje crudo original (string), listo para reprocesar
        }),
      }],
    });
    log.warn(`mensaje de ${topicOriginal} enviado a DLQ (${DLQ_TOPIC})`);
  } catch (err) {
    log.error(`no se pudo enviar a DLQ (${topicOriginal}) — ${err.message}`);
  }
}
