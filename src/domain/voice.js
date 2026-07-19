// Funciones puras del dominio de voz (sin Redis ni Kafka) — testeables sin mocks.
// Canales de voz, análogos al chat de texto:
//   - 'equipo'  → solo compañeros del mismo bando (default en 2v2).
//   - 'publico' → todos los jugadores humanos de la sala.
// En 1v1 todo canal se normaliza a 'publico' (no hay compañeros).
// En 1v1-bot no hay voz. Los bots NUNCA participan.

export function jugador(sala, playerId) {
  return sala?.jugadores?.find((j) => j.id === playerId) ?? null;
}

// ¿El modo de la sala admite voz? (1v1-bot no).
export function vozHabilitada(sala) {
  return !!sala && sala.modo !== '1v1-bot';
}

// Normaliza el canal pedido según el modo (en 1v1 no existen canales de equipo).
export function normalizarCanal(canal, modo) {
  if (modo !== '2v2') return 'publico';
  return canal === 'publico' ? 'publico' : 'equipo';
}

// ¿a y b se escuchan? Deben estar ambos en el MISMO canal; si es 'equipo',
// además deben ser compañeros de bando.
export function mismoCanal(sala, aId, bId, canales = {}) {
  if (!aId || !bId || aId === bId) return false;
  const a = jugador(sala, aId);
  const b = jugador(sala, bId);
  if (!a || !b || a.esBot || b.esBot) return false;
  if (!vozHabilitada(sala)) return false;
  const canalA = normalizarCanal(canales[aId], sala.modo);
  const canalB = normalizarCanal(canales[bId], sala.modo);
  if (canalA !== canalB) return false;
  if (canalA === 'equipo') return a.equipo === b.equipo;
  return true; // ambos en público
}

// De los miembros actuales del canal de voz ({ playerId: canal }), devuelve con
// cuáles debe conectar `playerId` (excluyéndose a sí mismo y a los bots).
export function peersParaJugador(sala, playerId, canales) {
  return Object.keys(canales ?? {}).filter((id) => mismoCanal(sala, playerId, id, canales));
}
