import 'dotenv/config';
import { createRedis } from './redis.js';
import { producer, createConsumer } from './kafka.js';
import { log } from './logger.js';
import { startObservability, conCorrelation, correlationActual, instrumentar , trackConsumer} from './observability.js';
import { enviarADLQ } from './dlq.js';
import { peersParaJugador, vozHabilitada, jugador, normalizarCanal } from './domain/voice.js';

const redis = createRedis();
await redis.connect();

startObservability({ port: process.env.OBS_PORT ?? 9100, redis });

await producer.connect();
log.info('kafka producer conectado');

const consumer = createConsumer('voice-group');
  trackConsumer(consumer); // salud del consumer -> kafka_consumer_up + /health
await consumer.connect();
await consumer.subscribe({ topics: ['cmd.voice', 'evt.room'], fromBeginning: false });
log.info('suscrito a cmd.voice, evt.room');

// TTL del canal de voz (se refresca en cada join): igual que el de la sala.
const VOICE_TTL_SEG = 60 * 60 * 6;
// HASH playerId → canal ('equipo'|'publico'). Antes era un SET; el HASH permite el
// switch equipo/público del 2v2 sin claves extra.
const claveVoz  = (codigo) => `sala:${codigo}:voice`;
const claveMute = (codigo) => `sala:${codigo}:voice:mute`; // HASH playerId → '1'|'0'

// Servidores ICE para los clientes WebRTC.
//
// STUN solo basta cuando los peers pueden hacer "hole punching" (misma red o NAT de cono).
// Entre REDES DISTINTAS —sobre todo si alguno está tras NAT SIMÉTRICO (móvil/4G, CGNAT,
// corporativo)— el hole punching FALLA y hace falta un TURN que RELEVE el audio. Sin TURN, la
// conexión queda "conectando"/"conectado" pero el audio nunca fluye (el bug reportado).
//
// Por eso el default ahora incluye un TURN público (OpenRelay). Es best-effort: los TURN
// públicos gratis son inestables → para juego cross-red CONFIABLE, define VOICE_ICE_SERVERS
// con tu propio TURN (metered.ca free API key o coturn). Ver GUIA-PRUEBAS.md §Voz/TURN.
function iceServers() {
  const raw = process.env.VOICE_ICE_SERVERS;
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length) return parsed;
    } catch {
      log.warn('VOICE_ICE_SERVERS inválido (no es JSON) — usando STUN+TURN por defecto');
    }
  }
  return [
    { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302',
             'stun:stun2.l.google.com:19302', 'stun:stun3.l.google.com:19302'] },
    // TURN público OpenRelay (relevo cuando el P2P directo no es posible). Varios transportes
    // (UDP/TCP/443) para atravesar firewalls restrictivos. Reemplázalo por el tuyo en prod.
    { urls: 'turn:openrelay.metered.ca:80',              username: 'openrelayproject', credential: 'openrelayproject' },
    { urls: 'turn:openrelay.metered.ca:443',             username: 'openrelayproject', credential: 'openrelayproject' },
    { urls: 'turn:openrelay.metered.ca:443?transport=tcp', username: 'openrelayproject', credential: 'openrelayproject' },
  ];
}

const HANDLERS = {
  'voice:join':  handleJoin,
  'voice:leave': handleLeave,
  'voice:mute':  handleMute,
  'PlayerDisconnectedFromRoom': handleLeave, // desconectarse de la sala = salir de voz
  'RoomDestroyed': handleRoomClosed,
};

await consumer.run({
  partitionsConsumedConcurrently: 6, // paraleliza particiones (salas distintas); cada sala sigue serial
  eachMessage: async ({ topic, message }) => {
    const raw = message.value.toString();
    const key = message.key?.toString() ?? null;
    let msg;
    try {
      msg = JSON.parse(raw); // poison message (JSON inválido) → DLQ, no cuelga el consumer
    } catch (err) {
      await enviarADLQ({ servicio: 'voice-channel', topicOriginal: topic ?? 'cmd.voice', key, raw, error: err });
      return;
    }
    await conCorrelation(msg.correlationId, async () => {
      try {
        const handler = HANDLERS[msg.type];
        if (handler) await instrumentar(msg.type, handler)(msg.data);
      } catch (err) {
        log.error(`mensaje no procesado — ${err.message} [cid=${correlationActual()}]`);
        await enviarADLQ({ servicio: 'voice-channel', topicOriginal: topic ?? 'cmd.voice', key, raw, error: err, correlationId: correlationActual() });
      }
    });
  },
});

// ── Handlers ─────────────────────────────────────────────────────────────────

// Entrar al canal de voz (o CAMBIAR de canal con el switch equipo/público del 2v2:
// el cliente re-emite voice:join con el canal nuevo).
async function handleJoin({ codigo, playerId, canal }) {
  if (!codigo || !playerId) return;
  const sala = await cargarSala(codigo);
  if (!vozHabilitada(sala) || !jugador(sala, playerId)) return;

  const canalNuevo = normalizarCanal(canal, sala.modo);
  const canales    = await redis.hgetall(claveVoz(codigo)) ?? {};
  const canalPrev  = canales[playerId] ?? null;

  // Cambio de canal: avisar a los peers del canal ANTERIOR que este jugador se fue
  // (cierran su conexión) antes de recalcular los nuevos.
  if (canalPrev && canalPrev !== canalNuevo) {
    const peersPrev = peersParaJugador(sala, playerId, canales);
    for (const p of peersPrev) await emitir(p, 'voice:peer-left', { peerId: playerId });
  }

  canales[playerId] = canalNuevo;
  await redis.hset(claveVoz(codigo), playerId, canalNuevo);
  await redis.expire(claveVoz(codigo), VOICE_TTL_SEG);

  const peers = peersParaJugador(sala, playerId, canales); // con quién debe conectar

  // Estado de mute conocido de esos peers (para que la UI del recién llegado lo muestre).
  const mutes = {};
  for (const p of peers) {
    const m = await redis.hget(claveMute(codigo), p);
    if (m != null) mutes[p] = m === '1';
  }

  // 1) Al recién llegado: la lista de peers (él INICIA las ofertas WebRTC) + ICE + mutes.
  await emitir(playerId, 'voice:peers', { peers, canal: canalNuevo, iceServers: iceServers(), mutes });

  // 2) A cada peer del canal: aviso de que alguien entró (para su UI y para esperar la oferta).
  const yoMute = (await redis.hget(claveMute(codigo), playerId)) === '1';
  for (const p of peers) {
    await emitir(p, 'voice:peer-joined', { peerId: playerId, muted: yoMute });
  }

  log.info(`sala ${codigo} — ${playerId} en voz (canal ${canalNuevo}, peers: ${peers.length})`);
}

async function handleLeave({ codigo, playerId }) {
  if (!codigo || !playerId) return;
  const canales = await redis.hgetall(claveVoz(codigo)) ?? {};
  if (!(playerId in canales)) return;

  const sala  = await cargarSala(codigo);
  const peers = sala ? peersParaJugador(sala, playerId, canales) : [];

  await redis.hdel(claveVoz(codigo), playerId);
  await redis.hdel(claveMute(codigo), playerId);

  for (const p of peers) {
    await emitir(p, 'voice:peer-left', { peerId: playerId });
  }
  log.info(`sala ${codigo} — ${playerId} salió de voz`);
}

async function handleMute({ codigo, playerId, muted }) {
  if (!codigo || !playerId) return;
  const canales = await redis.hgetall(claveVoz(codigo)) ?? {};
  if (!(playerId in canales)) return;

  await redis.hset(claveMute(codigo), playerId, muted ? '1' : '0');

  const sala  = await cargarSala(codigo);
  const peers = sala ? peersParaJugador(sala, playerId, canales) : [];
  for (const p of peers) {
    await emitir(p, 'voice:peer-mute', { peerId: playerId, muted: !!muted });
  }
}

async function handleRoomClosed({ codigo }) {
  if (!codigo) return;
  await redis.del(claveVoz(codigo));
  await redis.del(claveMute(codigo));
  log.info(`sala ${codigo} — canal de voz cerrado (sala destruida)`);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function cargarSala(codigo) {
  const raw = await redis.get(`sala:${codigo}`);
  return raw ? JSON.parse(raw) : null;
}

// Entrega un evento a un jugador concreto vía el gateway (fan-out por gw.broadcast;
// el gateway hace io.to(roomId).emit — cada socket se une al room con su playerId).
async function emitir(playerId, event, payload) {
  await producer.send({
    topic:    'gw.broadcast',
    messages: [{ key: playerId, value: JSON.stringify({ roomId: playerId, event, payload, correlationId: correlationActual() }) }],
  });
}
