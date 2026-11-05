//! Adaptador de Kick: servidor HTTP que recibe los webhooks oficiales,
//! verifica la firma y publica los eventos normalizados en el hub.

pub mod api;
mod normalize;
mod signature;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Router,
};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::KickConfig;
use crate::hub::Hub;
use crate::proto::v1::{Channel, Platform};
use api::KickApi;
use signature::Verifier;

/// Eventos a los que se suscribe cada canal cuando `KICK_AUTO_SUBSCRIBE=true`.
const EVENTS: &[(&str, u32)] = &[
    ("chat.message.sent", 1),
    ("channel.followed", 1),
    ("channel.subscription.new", 1),
    ("channel.subscription.renewal", 1),
    ("channel.subscription.gifts", 1),
    ("livestream.status.updated", 1),
];

#[derive(Clone)]
struct AppState {
    hub: Arc<Hub>,
    verifier: Option<Arc<Verifier>>,
}

pub async fn run(cfg: KickConfig, api: KickApi, hub: Arc<Hub>, cancel: CancellationToken) -> Result<()> {
    let verifier = if cfg.verify_signatures {
        let pem = match &cfg.public_key_pem {
            Some(pem) => pem.clone(),
            None => api::fetch_public_key(api.http())
                .await
                .context("no se pudo obtener la clave pública de Kick (puedes fijarla con KICK_PUBLIC_KEY_PEM)")?,
        };
        Some(Arc::new(Verifier::from_pem(&pem)?))
    } else {
        warn!("KICK_VERIFY_SIGNATURES=false: se aceptan webhooks sin verificar. Úsalo solo en desarrollo");
        None
    };

    for id in &cfg.broadcaster_ids {
        hub.register_channel(Channel {
            platform: Platform::Kick as i32,
            id: id.clone(),
            ..Default::default()
        });
    }

    if cfg.auto_subscribe {
        for id in &cfg.broadcaster_ids {
            match api.subscribe_events(id, EVENTS).await {
                Ok(()) => info!(broadcaster = %id, "Kick: suscripción a eventos creada"),
                Err(e) => warn!(broadcaster = %id, error = %format!("{e:#}"), "Kick: no se pudo suscribir"),
            }
        }
    }

    let app = Router::new()
        .route(&cfg.webhook_path, post(webhook))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(AppState { hub, verifier });

    let listener = tokio::net::TcpListener::bind(cfg.webhook_addr)
        .await
        .with_context(|| format!("no se pudo escuchar en {}", cfg.webhook_addr))?;
    info!(addr = %cfg.webhook_addr, path = %cfg.webhook_path, "Kick: escuchando webhooks");

    axum::serve(listener, app)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await
        .context("el servidor de webhooks falló")?;
    info!("Kick: adaptador detenido");
    Ok(())
}

async fn webhook(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let message_id = header(&headers, "Kick-Event-Message-Id");
    let timestamp = header(&headers, "Kick-Event-Message-Timestamp");
    let signature = header(&headers, "Kick-Event-Signature");
    let event_type = header(&headers, "Kick-Event-Type");

    if message_id.is_empty() || event_type.is_empty() {
        return StatusCode::BAD_REQUEST;
    }
    if let Some(verifier) = &state.verifier {
        if !verifier.verify(message_id, timestamp, &body, signature) {
            warn!(%message_id, "Kick: firma inválida, webhook rechazado");
            return StatusCode::UNAUTHORIZED;
        }
    }

    // A partir de aquí respondemos 200 aunque no podamos procesarlo: reintentar no lo arreglaría.
    let value: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, event_type, "Kick: body ilegible");
            return StatusCode::OK;
        }
    };
    match normalize::normalize(event_type, message_id, &value) {
        Some(event) => {
            state.hub.publish(event);
        }
        None => debug!(event_type, "Kick: evento ignorado"),
    }
    StatusCode::OK
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
}
