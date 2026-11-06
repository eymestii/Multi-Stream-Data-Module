//! Configuración leída desde variables de entorno (y opcionalmente desde un archivo `.env`).

use std::{env, fmt::Display, net::SocketAddr, str::FromStr, time::Duration};

use anyhow::{anyhow, bail, Context, Result};

#[derive(Clone)]
pub struct Config {
    pub grpc_addr: SocketAddr,
    pub oauth_addr: String, // Dirección del servidor gRPC OAuth (ej. http://127.0.0.1:50052)
    pub event_buffer_size: usize,
    pub subscriber_queue_size: usize,
    pub include_raw_json: bool,
    pub mock: Option<MockConfig>,
    pub twitch: Option<TwitchConfig>,
    pub kick: Option<KickConfig>,
}

#[derive(Clone)]
pub struct MockConfig {
    pub interval: Duration,
}

#[derive(Clone)]
pub struct TwitchConfig {
    pub client_id: String,
    pub access_token: String,
    pub bot_user_id: String,
    pub broadcaster_ids: Vec<String>,
}

#[derive(Clone)]
pub struct KickConfig {
    pub webhook_addr: SocketAddr,
    pub webhook_path: String,
    pub client_id: String,
    pub access_token: Option<String>,
    pub broadcaster_ids: Vec<String>,
    pub public_key_pem: Option<String>,
    pub verify_signatures: bool,
    pub auto_subscribe: bool,
    pub send_as: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let event_buffer_size: usize = parse_or("EVENT_BUFFER_SIZE", 1000)?;
        let subscriber_queue_size: usize = parse_or("SUBSCRIBER_QUEUE_SIZE", 256)?;
        if event_buffer_size == 0 || subscriber_queue_size == 0 {
            bail!("EVENT_BUFFER_SIZE y SUBSCRIBER_QUEUE_SIZE deben ser mayores que 0");
        }

        Ok(Self {
            grpc_addr: parse_or("GRPC_ADDR", SocketAddr::from(([0, 0, 0, 0], 50051)))?,
            oauth_addr: var("OAUTH_GRPC_ADDR")
                .unwrap_or_else(|| "http://127.0.0.1:50052".to_string()),
            event_buffer_size,
            subscriber_queue_size,
            include_raw_json: flag("INCLUDE_RAW_JSON", false)?,
            mock: mock_config()?,
            twitch: twitch_config()?,
            kick: kick_config()?,
        })
    }
}

fn mock_config() -> Result<Option<MockConfig>> {
    if !flag("MOCK_EVENTS", false)? {
        return Ok(None);
    }
    let ms: u64 = parse_or("MOCK_EVENTS_INTERVAL_MS", 2000)?;
    Ok(Some(MockConfig {
        interval: Duration::from_millis(ms.max(10)),
    }))
}

fn twitch_config() -> Result<Option<TwitchConfig>> {
    if !flag("TWITCH_ENABLED", false)? {
        return Ok(None);
    }
    let broadcaster_ids = list("TWITCH_BROADCASTER_IDS");
    if broadcaster_ids.is_empty() {
        bail!("TWITCH_ENABLED=true pero TWITCH_BROADCASTER_IDS está vacío");
    }
    Ok(Some(TwitchConfig {
        client_id: required("TWITCH_CLIENT_ID")?,
        access_token: required("TWITCH_ACCESS_TOKEN")?
            .trim_start_matches("oauth:")
            .to_string(),
        bot_user_id: required("TWITCH_BOT_USER_ID")?,
        broadcaster_ids,
    }))
}

fn kick_config() -> Result<Option<KickConfig>> {
    if !flag("KICK_ENABLED", false)? {
        return Ok(None);
    }
    let webhook_path = var("KICK_WEBHOOK_PATH").unwrap_or_else(|| "/kick/webhook".into());
    if !webhook_path.starts_with('/') {
        bail!("KICK_WEBHOOK_PATH debe empezar con '/'");
    }
    let send_as = var("KICK_SEND_AS")
        .unwrap_or_else(|| "bot".into())
        .to_lowercase();
    if send_as != "bot" && send_as != "user" {
        bail!("KICK_SEND_AS debe ser 'bot' o 'user'");
    }
    let access_token = var("KICK_ACCESS_TOKEN");
    let broadcaster_ids = list("KICK_BROADCASTER_IDS");
    let auto_subscribe = flag("KICK_AUTO_SUBSCRIBE", false)?;
    if auto_subscribe && (access_token.is_none() || broadcaster_ids.is_empty()) {
        bail!("KICK_AUTO_SUBSCRIBE=true requiere KICK_ACCESS_TOKEN y KICK_BROADCASTER_IDS");
    }

    Ok(Some(KickConfig {
        webhook_addr: parse_or("KICK_WEBHOOK_ADDR", SocketAddr::from(([0, 0, 0, 0], 8080)))?,
        webhook_path,
        client_id: var("KICK_CLIENT_ID").unwrap_or_default(),
        access_token,
        broadcaster_ids,
        // Permite pegar la clave en una sola línea usando "\n".
        public_key_pem: var("KICK_PUBLIC_KEY_PEM").map(|p| p.replace("\\n", "\n")),
        verify_signatures: flag("KICK_VERIFY_SIGNATURES", true)?,
        auto_subscribe,
        send_as,
    }))
}

// ---------- helpers ----------

fn var(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn required(name: &str) -> Result<String> {
    var(name).with_context(|| format!("falta la variable de entorno {name}"))
}

fn parse_or<T>(name: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    match var(name) {
        None => Ok(default),
        Some(v) => v
            .parse()
            .map_err(|e| anyhow!("{name}={v:?} no es válido: {e}")),
    }
}

fn flag(name: &str, default: bool) -> Result<bool> {
    match var(name).map(|v| v.to_lowercase()) {
        None => Ok(default),
        Some(v) => match v.as_str() {
            "1" | "true" | "yes" | "si" | "sí" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => bail!("{name}={v:?} no es un booleano válido (usa true/false)"),
        },
    }
}

fn list(name: &str) -> Vec<String> {
    var(name)
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}
