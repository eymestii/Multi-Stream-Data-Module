//! Cliente de ejemplo: se suscribe a StreamEvents e imprime cada evento.
//!
//!   cargo run --example cliente
//!   cargo run --example cliente -- http://localhost:50051 twitch:micanal

#[allow(dead_code)]
mod proto {
    tonic::include_proto!("streamchat.v1");
}

use proto::{chat_event::Payload, chat_hub_client::ChatHubClient, Platform, StreamEventsRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "http://127.0.0.1:50051".into());
    let channels: Vec<String> = args.collect();

    let mut client = ChatHubClient::connect(addr.clone()).await?;
    println!("conectado a {addr}, esperando eventos (Ctrl+C para salir)…");

    let mut stream = client
        .stream_events(StreamEventsRequest { channels, ..Default::default() })
        .await?
        .into_inner();

    while let Some(event) = stream.message().await? {
        let platform = Platform::try_from(event.platform).unwrap_or(Platform::Unspecified);
        let channel = event.channel.map(|c| c.slug).unwrap_or_default();
        match event.payload {
            Some(Payload::Message(m)) => {
                let user = m.sender.map(|s| s.display_name).unwrap_or_default();
                println!("[{platform:?}] #{channel} {user}: {}", m.text);
            }
            Some(other) => println!("[{platform:?}] #{channel} {other:?}"),
            None => {}
        }
        // Guardar event.id permite reconectar con resume_after_id sin perder eventos.
    }
    Ok(())
}
