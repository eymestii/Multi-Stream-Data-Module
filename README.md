# streamchat

Unifica el chat y los eventos de **Twitch** y **Kick** en un único modelo (`ChatEvent`) y los expone vía **gRPC**.

```
Twitch EventSub (WS) ─┐                                                  ┌─► bot / backend
                      ├─► adaptadores ─► normalización ─► hub ─► gRPC ───┼─► dashboard
Kick webhooks (HTTP) ─┘                    (dedup, ULID)  (buffer)       └─► overlay (vía gRPC-Web)
```

## Estructura

| Ruta | Capa | Qué hace |
|---|---|---|
| `proto/streamchat.proto` | Contrato | Modelo común y servicio `ChatHub` |
| `src/adapters/twitch/` | Ingesta | WebSocket EventSub, suscripciones, reconexión, API Helix |
| `src/adapters/kick/` | Ingesta | Servidor de webhooks, verificación de firma RSA, API pública |
| `src/adapters/*/normalize.rs` | Normalización | Evento nativo → `ChatEvent` |
| `src/adapters/mock.rs` | Ingesta | Eventos falsos para desarrollo |
| `src/hub.rs` | Núcleo | Deduplicación, ids ULID, ring buffer, reparto a suscriptores |
| `src/outbound.rs` | Núcleo | Router de salida para `SendMessage` |
| `src/grpc.rs` | Exposición | Servicio gRPC, filtros y backpressure |
| `src/config.rs` | Transversal | Variables de entorno |
| `fixtures/` | Tests | Payloads de ejemplo de cada plataforma |

## Requisitos

- Rust 1.80 o superior (`rustup update`).
- No hace falta instalar `protoc`: viene embebido.

## Arranque rápido (sin cuentas)

```bash
cp .env.example .env          # MOCK_EVENTS=true viene activado
cargo run                     # terminal 1: servidor
cargo run --example cliente   # terminal 2: imprime los eventos
```

## Configuración

Todo se configura con variables de entorno (o un archivo `.env`). Ver `.env.example`, que documenta cada una.

### Twitch

1. Registra una app en <https://dev.twitch.tv/console> y obtén el Client ID.
2. Genera un *user access token* para la cuenta del bot con los scopes `user:read:chat` y `user:write:chat`.
3. Completa `TWITCH_CLIENT_ID`, `TWITCH_ACCESS_TOKEN`, `TWITCH_BOT_USER_ID` y `TWITCH_BROADCASTER_IDS` (ids numéricos).

Para probar sin estar en directo, el [Twitch CLI](https://dev.twitch.tv/docs/cli/) trae un servidor EventSub simulado.

### Kick

1. Crea una app en <https://kick.com/settings/developer> y configura la URL del webhook.
2. En desarrollo, expón tu puerto local con `ngrok http 8080` o `cloudflared tunnel --url http://localhost:8080`.
3. Completa `KICK_ACCESS_TOKEN` (scopes `chat:write` y `events:subscribe`) y `KICK_BROADCASTER_IDS`.
4. Con `KICK_AUTO_SUBSCRIBE=true` las suscripciones se crean al arrancar.

Para probar sin Kick, desactiva la verificación (`KICK_VERIFY_SIGNATURES=false`) y envía un fixture:

```bash
curl -X POST http://localhost:8080/kick/webhook \
  -H "Kick-Event-Message-Id: 01JC0000000000000000000001" \
  -H "Kick-Event-Message-Timestamp: 2026-11-04T18:21:35Z" \
  -H "Kick-Event-Type: chat.message.sent" \
  --data @fixtures/kick/chat.message.sent.json
```

## Probar el gRPC

El servidor tiene *reflection* habilitado, así que [grpcurl](https://github.com/fullstorydev/grpcurl), Postman o Bruno descubren el servicio solos:

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -d '{"channels":["twitch:micanal"],"event_types":["message"]}' \
  localhost:50051 streamchat.v1.ChatHub/StreamEvents
grpcurl -plaintext -d '{"platform":"PLATFORM_TWITCH","channel_id":"123","text":"hola"}' \
  localhost:50051 streamchat.v1.ChatHub/SendMessage
```

**Reanudar sin perder eventos:** guarda el `id` del último evento recibido y reconecta con `resume_after_id`. Si responde `OUT_OF_RANGE`, ese evento ya salió del buffer y hay que suscribirse de nuevo sin él.

**Clientes lentos:** si un cliente se atrasa más de `SUBSCRIBER_QUEUE_SIZE` eventos, se le cierra el stream con `RESOURCE_EXHAUSTED` para no frenar a los demás; puede reanudar con `resume_after_id`.

## Tests

```bash
cargo test
```

Cubren la normalización de ambas plataformas (con los fixtures), el parser de emotes de Kick, la verificación de firmas, los filtros gRPC, la deduplicación y el replay.

> Los fixtures son ejemplos armados a partir de la documentación. Cuando recibas eventos reales, guarda su JSON en `fixtures/` (puedes activar `INCLUDE_RAW_JSON=true`) y agrega tests con ellos.

## Próximos pasos

- **Refresh de tokens**: hoy se usan tokens fijos; ambos vencen. Agregar renovación automática con el refresh token.
- **Más eventos**: follows, subs, raids y bits de Twitch (`channel.follow`, `channel.chat.notification`, `channel.cheer`), bans en ambas plataformas.
- **Clientes web**: agregar `tonic-web` para que un overlay en el navegador pueda consumir el stream.
- **Persistencia** opcional del buffer (SQLite) para sobrevivir reinicios.
- **Observabilidad**: métricas (eventos por plataforma, clientes conectados, reconexiones).
