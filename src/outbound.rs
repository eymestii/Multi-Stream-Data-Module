//! Router de salida: envía mensajes al chat de la plataforma que corresponda.

use tonic::Status;

use crate::adapters::{kick::api::KickApi, twitch::api::TwitchApi};
use crate::proto::v1::{Platform, SendMessageResponse};

/// Ambas plataformas limitan los mensajes a 500 caracteres.
const MAX_MESSAGE_LEN: usize = 500;

#[derive(Default)]
pub struct Outbound {
    pub twitch: Option<TwitchApi>,
    pub kick: Option<KickApi>,
}

impl Outbound {
    pub async fn send(
        &self,
        platform: Platform,
        channel_id: &str,
        text: &str,
        reply_to: Option<&str>,
    ) -> Result<SendMessageResponse, Status> {
        let text = text.trim();
        if text.is_empty() {
            return Err(Status::invalid_argument("el texto está vacío"));
        }
        if text.chars().count() > MAX_MESSAGE_LEN {
            return Err(Status::invalid_argument(format!(
                "el texto supera los {MAX_MESSAGE_LEN} caracteres"
            )));
        }
        if channel_id.trim().is_empty() {
            return Err(Status::invalid_argument("falta channel_id"));
        }

        let result = match platform {
            Platform::Twitch => {
                let api = self
                    .twitch
                    .as_ref()
                    .ok_or_else(|| Status::failed_precondition("Twitch no está habilitado"))?;
                api.send_chat_message(channel_id, text, reply_to).await
            }
            Platform::Kick => {
                let api = self
                    .kick
                    .as_ref()
                    .ok_or_else(|| Status::failed_precondition("Kick no está habilitado"))?;
                api.send_chat_message(channel_id, text, reply_to).await
            }
            Platform::Unspecified => return Err(Status::invalid_argument("falta la plataforma")),
        };

        result.map_err(|e| {
            tracing::warn!(error = %format!("{e:#}"), ?platform, "no se pudo enviar el mensaje");
            Status::unavailable(format!("error al enviar el mensaje: {e:#}"))
        })
    }
}
