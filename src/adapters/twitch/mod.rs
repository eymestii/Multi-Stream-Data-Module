//! Adaptador de Twitch: mantiene una conexión a EventSub por WebSocket,
//! crea las suscripciones y publica los eventos normalizados en el hub.

pub mod api;
mod normalize;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::TwitchConfig;
use crate::hub::Hub;
use crate::util::sleep_or_cancel;
use api::TwitchApi;

const EVENTSUB_URL: &str = "wss://eventsub.wss.twitch.tv/ws";
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const WELCOME_TIMEOUT: Duration = Duration::from_secs(10);

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Deserialize)]
struct Envelope {
    metadata: Metadata,
    #[serde(default)]
    payload: Value,
}

#[derive(Deserialize)]
struct Metadata {
    message_id: String,
    message_type: String,
    #[serde(default)]
    message_timestamp: String,
    #[serde(default)]
    subscription_type: Option<String>,
}

struct Welcome {
    session_id: String,
    keepalive: Duration,
}

pub async fn run(
    cfg: TwitchConfig,
    api: TwitchApi,
    hub: Arc<Hub>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut backoff = INITIAL_BACKOFF;
    while !cancel.is_cancelled() {
        match session(&cfg, &api, &hub, &cancel, &mut backoff).await {
            Ok(()) => break, // cancelado
            Err(e) => {
                warn!(error = %format!("{e:#}"), "Twitch: sesión EventSub terminada; reintento en {backoff:?}");
                if !sleep_or_cancel(backoff, &cancel).await {
                    break;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
    info!("Twitch: adaptador detenido");
    Ok(())
}

/// Una sesión completa. Devuelve `Ok` solo si se canceló; cualquier otro final es un error
/// para que `run` reconecte (con suscripciones nuevas, porque se pierden al cerrar).
async fn session(
    cfg: &TwitchConfig,
    api: &TwitchApi,
    hub: &Hub,
    cancel: &CancellationToken,
    backoff: &mut Duration,
) -> Result<()> {
    let (mut ws, welcome) = connect(EVENTSUB_URL).await?;
    info!(session_id = %welcome.session_id, "Twitch: conectado a EventSub");
    subscribe_all(cfg, api, &welcome.session_id).await?;
    *backoff = INITIAL_BACKOFF;
    let mut keepalive = welcome.keepalive;

    loop {
        let message = tokio::select! {
            _ = cancel.cancelled() => {
                let _ = ws.close(None).await;
                return Ok(());
            }
            next = tokio::time::timeout(keepalive, ws.next()) => match next {
                Err(_) => bail!("no llegó ningún mensaje dentro del keepalive"),
                Ok(None) => bail!("Twitch cerró la conexión"),
                Ok(Some(Err(e))) => return Err(e).context("error de WebSocket"),
                Ok(Some(Ok(m))) => m,
            },
        };

        let text = match message {
            Message::Text(text) => text,
            Message::Close(frame) => bail!("Twitch cerró la conexión: {frame:?}"),
            _ => continue, // ping/pong los maneja tungstenite
        };

        let envelope: Envelope = match serde_json::from_str(&text) {
            Ok(env) => env,
            Err(e) => {
                warn!(error = %e, "Twitch: mensaje que no se pudo leer");
                continue;
            }
        };

        match envelope.metadata.message_type.as_str() {
            "session_keepalive" => {}
            "notification" => {
                let sub_type = envelope.metadata.subscription_type.as_deref().unwrap_or_default();
                match normalize::normalize(sub_type, &envelope.metadata, &envelope.payload["event"]) {
                    Some(event) => {
                        hub.publish(event);
                    }
                    None => debug!(sub_type, "Twitch: evento ignorado"),
                }
            }
            "session_reconnect" => {
                // Twitch pide migrar: conectamos a la URL nueva, esperamos su welcome y recién
                // entonces soltamos la vieja. Las suscripciones se mantienen solas.
                let url = envelope.payload["session"]["reconnect_url"]
                    .as_str()
                    .context("session_reconnect sin reconnect_url")?;
                info!("Twitch: migrando a una nueva conexión");
                let (new_ws, welcome) = connect(url).await?;
                let _ = ws.close(None).await;
                ws = new_ws;
                keepalive = welcome.keepalive;
            }
            "revocation" => {
                warn!(
                    subscription = %envelope.payload["subscription"],
                    "Twitch revocó una suscripción (¿token vencido o permisos retirados?)"
                );
            }
            other => debug!(message_type = other, "Twitch: tipo de mensaje no manejado"),
        }
    }
}

/// Lee frames hasta recibir el primer mensaje de texto, ignorando ping/pong.
/// Si Twitch cierra la conexión o envía otro tipo de frame, el error incluye el detalle.
async fn read_first_text(ws: &mut Ws) -> Result<String> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => return Ok(t.as_str().to_owned()),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(Message::Close(frame))) => {
                bail!("EventSub cerró la conexión antes del welcome: {frame:?}")
            }
            Some(Ok(other)) => bail!("primer mensaje inesperado de EventSub: {other:?}"),
            Some(Err(e)) => return Err(e).context("error de WebSocket antes del welcome"),
            None => bail!("conexión cerrada antes del welcome"),
        }
    }
}

async fn connect(url: &str) -> Result<(Ws, Welcome)> {
    let (mut ws, _) = connect_async(url)
        .await
        .context("no se pudo conectar a EventSub")?;

    let text = tokio::time::timeout(WELCOME_TIMEOUT, read_first_text(&mut ws))
        .await
        .context("timeout esperando session_welcome")??;

    let envelope: Envelope = serde_json::from_str(&text)
        .with_context(|| format!("welcome ilegible: {text}"))?;
    if envelope.metadata.message_type != "session_welcome" {
        bail!(
            "se esperaba session_welcome y llegó {}: {text}",
            envelope.metadata.message_type
        );
    }
    let session = &envelope.payload["session"];
    let session_id = session["id"]
        .as_str()
        .context("welcome sin session.id")?
        .to_string();
    let keepalive_secs = session["keepalive_timeout_seconds"].as_u64().unwrap_or(10);

    Ok((
        ws,
        Welcome {
            session_id,
            // Margen extra para no reconectar por un poco de latencia.
            keepalive: Duration::from_secs(keepalive_secs + 5),
        },
    ))
}

/// Suscripciones que se crean por cada canal configurado.
async fn subscribe_all(cfg: &TwitchConfig, api: &TwitchApi, session_id: &str) -> Result<()> {
    let (mut ok, mut total) = (0, 0);
    for broadcaster in &cfg.broadcaster_ids {
        let chat_condition = json!({ "broadcaster_user_id": broadcaster, "user_id": api.bot_user_id() });
        let channel_condition = json!({ "broadcaster_user_id": broadcaster });
        let subscriptions = [
            ("channel.chat.message", chat_condition.clone()),
            ("channel.chat.message_delete", chat_condition),
            ("stream.online", channel_condition.clone()),
            ("stream.offline", channel_condition),
        ];
        for (sub_type, condition) in subscriptions {
            total += 1;
            match api.create_subscription(session_id, sub_type, "1", condition).await {
                Ok(()) => ok += 1,
                Err(e) => warn!(%broadcaster, sub_type, error = %format!("{e:#}"), "Twitch: no se pudo suscribir"),
            }
        }
    }
    if ok == 0 {
        bail!("no se creó ninguna suscripción EventSub; revisa el token y sus scopes");
    }
    info!("Twitch: {ok}/{total} suscripciones activas");
    Ok(())
}
