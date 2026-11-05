//! Cliente mínimo de la API pública de Kick.
//!
//! Si se proporciona un `TokenManager`, se obtiene el access token fresco del servidor
//! OAuth gRPC antes de cada petición, permitiendo la renovación automática de tokens.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::config::KickConfig;
use crate::oauth_client::TokenManager;
use crate::proto::v1::SendMessageResponse;

const API: &str = "https://api.kick.com/public/v1";

#[derive(Clone)]
pub struct KickApi {
    http: Client,
    // access_token estático como fallback si no hay TokenManager.
    access_token: Option<String>,
    send_as: String,
    token_manager: Option<Arc<Mutex<TokenManager>>>,
}

impl KickApi {
    pub fn new(
        http: Client,
        cfg: &KickConfig,
        token_manager: Option<Arc<Mutex<TokenManager>>>,
    ) -> Self {
        Self {
            http,
            access_token: cfg.access_token.clone(),
            send_as: cfg.send_as.clone(),
            token_manager,
        }
    }

    pub fn http(&self) -> &Client {
        &self.http
    }

    /// Obtiene el access token fresco: del TokenManager si está disponible, o del valor estático.
    async fn access_token(&self) -> Result<String> {
        if let Some(tm) = &self.token_manager {
            let mut tm = tm.lock().await;
            tm.kick_access_token().await
        } else {
            self.access_token
                .as_deref()
                .map(|s| s.to_string())
                .context("KICK_ACCESS_TOKEN no está configurado")
        }
    }

    /// Suscribe un canal a eventos entregados por webhook. La URL de destino
    /// se configura en la app de Kick (kick.com/settings/developer).
    pub async fn subscribe_events(&self, broadcaster_id: &str, events: &[(&str, u32)]) -> Result<()> {
        let broadcaster_user_id: u64 = broadcaster_id
            .parse()
            .with_context(|| format!("el id de canal de Kick {broadcaster_id:?} debe ser numérico"))?;
        let events: Vec<Value> = events
            .iter()
            .map(|(name, version)| json!({ "name": name, "version": version }))
            .collect();

        let token = self.access_token().await?;
        let resp = self
            .http
            .post(format!("{API}/events/subscriptions"))
            .bearer_auth(&token)
            .json(&json!({
                "broadcaster_user_id": broadcaster_user_id,
                "events": events,
                "method": "webhook",
            }))
            .send()
            .await
            .context("no se pudo contactar a la API de Kick")?;

        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            bail!("Kick respondió 401 Unauthorized: token de Kick inválido o expirado");
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("Kick respondió {status}: {body}");
        }
        Ok(())
    }

    /// Envía un mensaje al chat. Con `KICK_SEND_AS=bot`, Kick lo publica en el canal
    /// asociado al token; con `user`, en el canal indicado por `broadcaster_id`.
    pub async fn send_chat_message(
        &self,
        broadcaster_id: &str,
        text: &str,
        reply_to: Option<&str>,
    ) -> Result<SendMessageResponse> {
        let broadcaster_user_id: u64 = broadcaster_id
            .parse()
            .context("channel_id de Kick debe ser numérico")?;
        let token = self.access_token().await?;
        let mut body = json!({
            "broadcaster_user_id": broadcaster_user_id,
            "content": text,
            "type": self.send_as,
        });
        if let Some(parent) = reply_to {
            body["reply_to_message_id"] = json!(parent);
        }

        let resp = self
            .http
            .post(format!("{API}/chat"))
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            bail!("Kick respondió 401 Unauthorized: token de Kick inválido o expirado");
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("Kick respondió {status}: {body}");
        }

        #[derive(Deserialize)]
        struct Envelope {
            data: Data,
        }
        #[derive(Deserialize)]
        struct Data {
            #[serde(default)]
            is_sent: bool,
            #[serde(default)]
            message_id: String,
        }

        let envelope: Envelope = resp.json().await.context("respuesta inesperada de Kick")?;
        Ok(SendMessageResponse {
            message_id: envelope.data.message_id,
            accepted: envelope.data.is_sent,
            drop_reason: String::new(),
        })
    }
}

/// Descarga la clave pública con la que Kick firma los webhooks.
pub async fn fetch_public_key(http: &Client) -> Result<String> {
    #[derive(Deserialize)]
    struct Envelope {
        data: Data,
    }
    #[derive(Deserialize)]
    struct Data {
        public_key: String,
    }

    let resp = http.get(format!("{API}/public-key")).send().await?;
    let status = resp.status();
    if !status.is_success() {
        bail!("Kick respondió {status} al pedir la clave pública");
    }
    let envelope: Envelope = resp.json().await.context("respuesta inesperada de Kick")?;
    Ok(envelope.data.public_key)
}
