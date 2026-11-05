//! Generador de eventos falsos para desarrollar y probar clientes sin conectarse
//! a Twitch ni a Kick. Se activa con `MOCK_EVENTS=true`.

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::MockConfig;
use crate::hub::Hub;
use crate::proto::v1::{chat_event::Payload, Channel, ChatEvent, ChatMessage, Platform, User};
use crate::util::text_fragment;

pub async fn run(cfg: MockConfig, hub: Arc<Hub>, cancel: CancellationToken) -> Result<()> {
    info!(interval = ?cfg.interval, "Mock: generando eventos de prueba");
    let mut ticker = tokio::time::interval(cfg.interval);
    let mut n: u64 = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }
        n += 1;
        let (platform, slug) = if n % 2 == 0 {
            (Platform::Kick, "demo_kick")
        } else {
            (Platform::Twitch, "demo_twitch")
        };
        let text = format!("mensaje de prueba #{n}");

        hub.publish(ChatEvent {
            platform: platform as i32,
            platform_event_id: format!("mock-{n}"),
            channel: Some(Channel {
                platform: platform as i32,
                id: format!("mock-{slug}"),
                slug: slug.into(),
                display_name: slug.into(),
            }),
            occurred_at: Some(SystemTime::now().into()),
            payload: Some(Payload::Message(ChatMessage {
                message_id: format!("mock-{n}"),
                sender: Some(User {
                    id: "0".into(),
                    username: "bot_de_prueba".into(),
                    display_name: "BotDePrueba".into(),
                    ..Default::default()
                }),
                fragments: vec![text_fragment(text.clone())],
                text,
                reply: None,
            })),
            ..Default::default()
        });
    }
}
