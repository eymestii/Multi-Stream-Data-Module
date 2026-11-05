//! Cliente mínimo de la API Helix de Twitch.
//!
//! Si se proporciona un `TokenManager`, se obtiene el access token fresco del servidor
//! OAuth gRPC antes de cada petición, permitiendo la renovación automática de tokens.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::config::TwitchConfig;
use crate::oauth_client::TokenManager;
use crate::proto::v1::SendMessageResponse;

const HELIX: &str = "https://api.twitch.tv/helix";

#[derive(Clone)]
pub struct TwitchApi {
    http: Client,
    client_id: String,
    // access_token estático como fallback si no hay TokenManager.
    access_token: Option<String>,
    bot_user_id: String,
    token_manager: Option<Arc<Mutex<TokenManager>>>,
}

impl TwitchApi {
    pub fn new(
        http: Client,
        cfg: &TwitchConfig,
        token_manager: Option<Arc<Mutex<TokenManager>>>,
    ) -> Self {
        Self {
            http,
            client_id: cfg.client_id.clone(),
            access_token: Some(cfg.access_token.clone()),
            bot_user_id: cfg.bot_user_id.clone(),
            token_manager,
        }
    }

    pub fn bot_user_id(&self) -> &str {
        &self.bot_user_id
    }

    /// Obtiene el access token fresco: del TokenManager si está disponible, o del valor estático.
    async fn access_token(&self) -> Result<String> {
        if let Some(tm) = &self.token_manager {
            let mut tm = tm.lock().await;
            tm.twitch_access_token().await
        } else {
            self.access_token
                .as_deref()
                .map(|s| s.to_string())
                .context("TWITCH_ACCESS_TOKEN no está configurado")
        }
    }

    async fn post(&self, path: &str) -> Result<RequestBuilder> {
        let token = self.access_token().await?;
        Ok(self
            .http
            .post(format!("{HELIX}{path}"))
            .bearer_auth(&token)
            .header("Client-Id", &self.client_id))
    }

    /// Crea una suscripción EventSub ligada a la sesión de WebSocket.
    pub async fn create_subscription(
        &self,
        session_id: &str,
        sub_type: &str,
        version: &str,
        condition: Value,
    ) -> Result<()> {
        let resp = self
            .post("/eventsub/subscriptions")
            .await?
            .json(&json!({
                "type": sub_type,
                "version": version,
                "condition": condition,
                "transport": { "method": "websocket", "session_id": session_id },
            }))
            .send()
            .await
            .context("no se pudo contactar a Helix")?;

        match resp.status() {
            s if s.is_success() => Ok(()),
            // Ya existía: no es un error.
            StatusCode::CONFLICT => Ok(()),
            s if s == StatusCode::UNAUTHORIZED => {
                // 401: el token expiró o fue revocado. Invalidar para forzar refresh en la próxima.
                bail!("Helix respondió 401 Unauthorized: token de Twitch inválido o expirado")
            }
            s => {
                let body = resp.text().await.unwrap_or_default();
                bail!("Helix respondió {s}: {body}")
            }
        }
    }

    /// Envía un mensaje al chat como el usuario del bot (scope `user:write:chat`).
    pub async fn send_chat_message(
        &self,
        broadcaster_id: &str,
        text: &str,
        reply_to: Option<&str>,
    ) -> Result<SendMessageResponse> {
        let mut body = json!({
            "broadcaster_id": broadcaster_id,
            "sender_id": self.bot_user_id,
            "message": text,
        });
        if let Some(parent) = reply_to {
            body["reply_parent_message_id"] = json!(parent);
        }

        let resp = self.post("/chat/messages").await?.json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("Helix respondió {status}: {body}");
        }

        #[derive(Deserialize)]
        struct Envelope {
            data: Vec<Item>,
        }
        #[derive(Deserialize)]
        struct Item {
            #[serde(default)]
            message_id: String,
            #[serde(default)]
            is_sent: bool,
            drop_reason: Option<DropReason>,
        }
        #[derive(Deserialize)]
        struct DropReason {
            #[serde(default)]
            code: String,
            #[serde(default)]
            message: String,
        }

        let envelope: Envelope = resp.json().await.context("respuesta inesperada de Helix")?;
        let item = envelope.data.into_iter().next().context("Helix devolvió data vacío")?;
        Ok(SendMessageResponse {
            message_id: item.message_id,
            accepted: item.is_sent,
            drop_reason: item
                .drop_reason
                .map(|d| format!("{}: {}", d.code, d.message))
                .unwrap_or_default(),
        })
    }
}
