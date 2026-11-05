//! Implementación del servicio gRPC `ChatHub`.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast::error::RecvError, mpsc};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::hub::{Hub, ReplayError};
use crate::outbound::Outbound;
use crate::proto::v1::chat_hub_server::ChatHub;
use crate::proto::v1::{
    ChatEvent, ListChannelsRequest, ListChannelsResponse, Platform, SendMessageRequest,
    SendMessageResponse, StreamEventsRequest,
};
use crate::proto::{event_type_name, platform_key};

/// Cola entre la tarea que lee del hub y la conexión gRPC de cada cliente.
const CLIENT_QUEUE: usize = 64;

pub struct ChatHubService {
    hub: Arc<Hub>,
    outbound: Arc<Outbound>,
}

impl ChatHubService {
    pub fn new(hub: Arc<Hub>, outbound: Arc<Outbound>) -> Self {
        Self { hub, outbound }
    }
}

#[tonic::async_trait]
impl ChatHub for ChatHubService {
    type StreamEventsStream = Pin<Box<dyn Stream<Item = Result<ChatEvent, Status>> + Send>>;

    async fn stream_events(
        &self,
        request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let req = request.into_inner();
        let filter = Filter::from_request(&req)?;
        let resume = (!req.resume_after_id.is_empty()).then_some(req.resume_after_id.as_str());

        let (backlog, mut live) = self.hub.subscribe(resume).map_err(|e| match e {
            ReplayError::InvalidId => Status::invalid_argument("resume_after_id no es un ULID válido"),
            ReplayError::NotInBuffer => Status::out_of_range(
                "resume_after_id ya no está en el buffer; vuelve a suscribirte sin él",
            ),
        })?;

        let (tx, rx) = mpsc::channel(CLIENT_QUEUE);
        tokio::spawn(async move {
            for event in backlog {
                if filter.matches(&event) && tx.send(Ok((*event).clone())).await.is_err() {
                    return;
                }
            }
            loop {
                tokio::select! {
                    // El cliente se desconectó.
                    _ = tx.closed() => return,
                    received = live.recv() => match received {
                        Ok(event) => {
                            if filter.matches(&event) && tx.send(Ok((*event).clone())).await.is_err() {
                                return;
                            }
                        }
                        // Backpressure: el cliente no consume lo bastante rápido. En lugar de
                        // frenar a los demás, lo desconectamos; puede reanudar con resume_after_id.
                        Err(RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "cliente gRPC demasiado lento, se desconecta");
                            let _ = tx
                                .send(Err(Status::resource_exhausted(format!(
                                    "cliente demasiado lento: se perdieron {skipped} eventos; \
                                     reconecta usando resume_after_id"
                                ))))
                                .await;
                            return;
                        }
                        Err(RecvError::Closed) => return,
                    },
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let req = request.into_inner();
        let platform = Platform::try_from(req.platform)
            .map_err(|_| Status::invalid_argument("plataforma desconocida"))?;
        let reply_to = (!req.reply_to_message_id.is_empty()).then_some(req.reply_to_message_id.as_str());
        let response = self
            .outbound
            .send(platform, &req.channel_id, &req.text, reply_to)
            .await?;
        Ok(Response::new(response))
    }

    async fn list_channels(
        &self,
        _request: Request<ListChannelsRequest>,
    ) -> Result<Response<ListChannelsResponse>, Status> {
        Ok(Response::new(ListChannelsResponse {
            channels: self.hub.channels(),
        }))
    }
}

/// Filtros que un cliente pide al suscribirse. Un filtro vacío deja pasar todo.
struct Filter {
    platforms: HashSet<i32>,
    channels: HashSet<String>,
    event_types: HashSet<String>,
}

impl Filter {
    fn from_request(req: &StreamEventsRequest) -> Result<Self, Status> {
        for &p in &req.platforms {
            match Platform::try_from(p) {
                Ok(Platform::Twitch | Platform::Kick) => {}
                _ => return Err(Status::invalid_argument(format!("plataforma inválida: {p}"))),
            }
        }
        let channels = req
            .channels
            .iter()
            .map(|c| {
                let c = c.trim().to_lowercase();
                match c.split_once(':') {
                    Some((p, rest)) if (p == "twitch" || p == "kick") && !rest.is_empty() => Ok(c),
                    _ => Err(Status::invalid_argument(format!(
                        "canal inválido {c:?}: usa el formato \"twitch:slug\" o \"kick:slug\""
                    ))),
                }
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            platforms: req.platforms.iter().copied().collect(),
            channels,
            event_types: req.event_types.iter().map(|t| t.trim().to_lowercase()).collect(),
        })
    }

    fn matches(&self, event: &ChatEvent) -> bool {
        if !self.platforms.is_empty() && !self.platforms.contains(&event.platform) {
            return false;
        }
        if !self.channels.is_empty() {
            let p = platform_key(event.platform);
            let matched = event.channel.as_ref().is_some_and(|c| {
                self.channels.contains(&format!("{p}:{}", c.slug.to_lowercase()))
                    || self.channels.contains(&format!("{p}:{}", c.id))
            });
            if !matched {
                return false;
            }
        }
        if !self.event_types.is_empty() && !self.event_types.contains(event_type_name(event)) {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::v1::{chat_event::Payload, Channel, ChatMessage, Follow};

    fn ev(platform: Platform, slug: &str, payload: Payload) -> ChatEvent {
        ChatEvent {
            platform: platform as i32,
            channel: Some(Channel {
                platform: platform as i32,
                id: "42".into(),
                slug: slug.into(),
                display_name: slug.into(),
            }),
            payload: Some(payload),
            ..Default::default()
        }
    }

    fn msg() -> Payload {
        Payload::Message(ChatMessage::default())
    }

    #[test]
    fn filtro_vacio_deja_pasar_todo() {
        let f = Filter::from_request(&StreamEventsRequest::default()).unwrap();
        assert!(f.matches(&ev(Platform::Kick, "x", msg())));
    }

    #[test]
    fn filtra_por_plataforma_canal_y_tipo() {
        let f = Filter::from_request(&StreamEventsRequest {
            platforms: vec![Platform::Twitch as i32],
            channels: vec!["Twitch:MiCanal".into()],
            event_types: vec!["message".into()],
            ..Default::default()
        })
        .unwrap();
        assert!(f.matches(&ev(Platform::Twitch, "micanal", msg())));
        assert!(!f.matches(&ev(Platform::Kick, "micanal", msg())));
        assert!(!f.matches(&ev(Platform::Twitch, "otro", msg())));
        assert!(!f.matches(&ev(
            Platform::Twitch,
            "micanal",
            Payload::Follow(Follow::default())
        )));
    }

    #[test]
    fn rechaza_canales_mal_formados() {
        let req = StreamEventsRequest {
            channels: vec!["micanal".into()],
            ..Default::default()
        };
        assert!(Filter::from_request(&req).is_err());
    }
}
