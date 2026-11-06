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
    client_id: String,
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
            client_id: cfg.client_id.clone(),
            send_as: cfg.send_as.clone(),
            token_manager,
        }
    }

    pub fn http(&self) -> &Client {
        &self.http
    }

    /// Obtiene el access token fresco: del TokenManager si está disponible,
    /// o del valor estático (`KICK_ACCESS_TOKEN`) como fallback cuando el
    /// servidor OAuth no responde o no tiene credenciales de Kick.
    async fn access_token(&self) -> Result<String> {
        if let Some(tm) = &self.token_manager {
            let mut tm = tm.lock().await;
            match tm.kick_access_token().await {
                Ok(token) => return Ok(token),
                Err(e) => {
                    tracing::warn!(
                        error = %format!("{e:#}"),
                        "OAuth: no se pudo obtener token de Kick, se usa token estático si existe"
                    );
                }
            }
        }
        self.access_token
            .as_deref()
            .map(|s| s.to_string())
            .context("no se obtuvo token de Kick del OAuth ni hay KICK_ACCESS_TOKEN configurado")
    }

    async fn post_chat(&self, token: &str, body: &Value) -> Result<reqwest::Response> {
        let mut request = self
            .http
            .post(format!("{API}/chat"))
            .bearer_auth(token)
            .json(body);
        if !self.client_id.is_empty() {
            request = request.header("Client-Id", &self.client_id);
        }
        request.send().await.context("no se pudo contactar a la API de Kick")
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

    /// Envía un mensaje al chat.
    ///
    /// Con `KICK_SEND_AS=bot`, Kick publica en el canal asociado al token
    /// (no se requiere `broadcaster_user_id`). Con `user`, se publica en el
    /// canal indicado por `broadcaster_id` (se requiere `broadcaster_user_id`).
    pub async fn send_chat_message(
        &self,
        broadcaster_id: &str,
        text: &str,
        reply_to: Option<&str>,
    ) -> Result<SendMessageResponse> {
        let token = self.access_token().await?;
        let body = chat_message_body(&self.send_as, broadcaster_id, text, reply_to)?;

        tracing::info!(
            send_as = %self.send_as,
            channel_id = %broadcaster_id,
            text_chars = text.chars().count(),
            is_reply = reply_to.is_some(),
            "Kick: enviando mensaje"
        );

        let mut resp = self.post_chat(&token, &body).await?;
        tracing::info!(status = %resp.status(), "Kick: respuesta al enviar mensaje");
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            // El token puede haber caducado antes de lo previsto: se renueva y se reintenta una vez.
            if let Some(tm) = &self.token_manager {
                let fresh = tm.lock().await.force_refresh_kick().await;
                match fresh {
                    Ok(fresh) => {
                        resp = self.post_chat(&fresh, &body).await?;
                        tracing::info!(
                            status = %resp.status(),
                            "Kick: respuesta tras renovar el token"
                        );
                    }
                    Err(e) => tracing::warn!(error = %format!("{e:#}"), "OAuth: no se pudo renovar el token de Kick tras 401"),
                }
            }
        }
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

fn chat_message_body(
    send_as: &str,
    broadcaster_id: &str,
    text: &str,
    reply_to: Option<&str>,
) -> Result<Value> {
    let mut body = json!({
        "content": text,
        "type": send_as,
    });

    // Kick exige el canal al enviar como usuario. Al enviar como bot ignora
    // este campo y usa siempre el canal asociado al token.
    if send_as == "user" {
        let broadcaster_user_id = broadcaster_id
            .trim()
            .parse::<u64>()
            .context("channel_id de Kick debe ser numérico cuando se envía como user")?;
        body["broadcaster_user_id"] = json!(broadcaster_user_id);
    }

    if let Some(parent) = reply_to.filter(|id| !id.is_empty()) {
        body["reply_to_message_id"] = json!(parent);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::chat_message_body;

    #[test]
    fn user_message_includes_selected_channel() {
        let body = chat_message_body("user", "123", "hola", None).unwrap();
        assert_eq!(body["broadcaster_user_id"], 123);
        assert_eq!(body["type"], "user");
    }

    #[test]
    fn bot_message_omits_channel_as_required_by_kick() {
        let body = chat_message_body("bot", "123", "hola", None).unwrap();
        assert!(body.get("broadcaster_user_id").is_none());
        assert_eq!(body["type"], "bot");
    }

    #[test]
    fn user_message_rejects_non_numeric_channel() {
        assert!(chat_message_body("user", "canal", "hola", None).is_err());
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
