# battlecaos-voice-channel

Microservicio de **chat de voz** de BattleCaos-Ship (WebRTC P2P en malla).

## Qué hace

El audio va **directo entre navegadores** (P2P/WebRTC) — este servicio NO transporta
audio. Solo **orquesta** el canal de voz por sala:

- Membresía del canal en Redis (`sala:{codigo}:voice`).
- **Routing por equipo**, igual que el chat de texto:
  - `2v2` → voz exclusiva del equipo (solo compañeros).
  - `1v1` → voz abierta entre los dos jugadores (se habla con el rival).
  - `1v1-bot` → sin voz.
- Al entrar un jugador, calcula con qué **peers** debe conectar y les avisa.
- Entrega la configuración de **servidores ICE** (STUN por defecto; TURN opcional por env).
- Cierra el canal al terminar/destruirse la sala.

La **señalización** WebRTC (SDP/ICE) NO pasa por aquí: es un relay efímero directo
en el gateway (`voice:signal`), igual que `colocacion:preview`.

## Comunicación

| Entrada (Kafka) | |
|---|---|
| `cmd.voice` | `voice:join`, `voice:leave`, `voice:mute` (desde el gateway) |
| `evt.room`  | `PlayerDisconnectedFromRoom`, `RoomDestroyed` |

| Salida (Kafka `gw.broadcast`, dirigida por playerId) | |
|---|---|
| `voice:peers`       | al recién llegado: lista de peers + `iceServers` + estado de mute |
| `voice:peer-joined` | a los peers: entró alguien nuevo (esperar su oferta) |
| `voice:peer-left`   | a los peers: alguien salió (cerrar su conexión) |
| `voice:peer-mute`   | a los peers: un peer cambió su mute (para la UI) |

## Variables de entorno

Ver `.env.example`. Destacadas:

- `REDIS_URL`, `KAFKA_BROKER`, `KAFKA_CLIENT_ID` — como el resto de servicios.
- `VOICE_ICE_SERVERS` (opcional) — JSON con los servidores ICE. Sin él, STUN público.
  Para NAT difíciles añade un TURN (p.ej. free tier de Metered.ca).

## Tests

```bash
npm test
```
