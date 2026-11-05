mod adapters;
mod config;
mod grpc;
mod hub;
mod oauth_client;
mod oauth_proto;
mod outbound;
mod proto;
mod util;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use adapters::{kick::api::KickApi, twitch::api::TwitchApi};
use config::Config;
use hub::Hub;
use oauth_client::TokenManager;
use outbound::Outbound;
use proto::v1::{chat_hub_server::ChatHubServer, Channel, Platform};

#[tokio::main]
async fn main() -> Result<()> {
    // Carga `.env` si existe; las variables ya definidas en el entorno tienen prioridad.
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cfg = Config::from_env().context("configuración inválida")?;

    // ---- Cliente OAuth (servidor gRPC externo en OAUTH_GRPC_ADDR) ----
    // Se usa para obtener/refrescar los access_token de Twitch y Kick antes de cada petición.
    let token_manager = Arc::new(Mutex::new(
        TokenManager::new(&cfg.oauth_addr).await.context("no se pudo conectar al servidor OAuth")?,
    ));

    let hub = Hub::new(cfg.event_buffer_size, cfg.subscriber_queue_size, cfg.include_raw_json);
    let cancel = CancellationToken::new();
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    let http = reqwest::Client::builder()
        .user_agent(concat!("streamchat/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut outbound = Outbound::default();

    // ---- Capa 1: adaptadores ----
    if let Some(tc) = cfg.twitch.clone() {
        let api = TwitchApi::new(http.clone(), &tc, Some(token_manager.clone()));
        outbound.twitch = Some(api.clone());
        for id in &tc.broadcaster_ids {
            hub.register_channel(Channel {
                platform: Platform::Twitch as i32,
                id: id.clone(),
                ..Default::default()
            });
        }
        let tm = token_manager.clone();
        let hub_clone = hub.clone();
        let cancel_clone = cancel.clone();
        tasks.spawn(async move {
            let result = adapters::twitch::run(tc, api, hub_clone, cancel_clone).await;
            if let Err(e) = &result {
                // Si el error es 401, invalidar el token para forzar refresh en la próxima ejecución.
                if e.to_string().contains("401") {
                    let mut tm = tm.lock().await;
                    tm.invalidate_twitch();
                }
                error!(error = %format!("{e:#}"), "adaptador de Twitch terminó con error");
            }
            result
        });
    }
    if let Some(kc) = cfg.kick.clone() {
        let api = KickApi::new(http.clone(), &kc, Some(token_manager.clone()));
        outbound.kick = Some(api.clone());
        let tm = token_manager.clone();
        let hub_clone = hub.clone();
        let cancel_clone = cancel.clone();
        tasks.spawn(async move {
            let result = adapters::kick::run(kc, api, hub_clone, cancel_clone).await;
            if let Err(e) = &result {
                if e.to_string().contains("401") {
                    let mut tm = tm.lock().await;
                    tm.invalidate_kick();
                }
                error!(error = %format!("{e:#}"), "adaptador de Kick terminó con error");
            }
            result
        });
    }
    if let Some(mc) = cfg.mock.clone() {
        let hub_clone = hub.clone();
        let cancel_clone = cancel.clone();
        tasks.spawn(async move {
            adapters::mock::run(mc, hub_clone, cancel_clone).await
        });
    }
    if cfg.twitch.is_none() && cfg.kick.is_none() && cfg.mock.is_none() {
        warn!("no hay fuentes habilitadas: activa TWITCH_ENABLED, KICK_ENABLED o MOCK_EVENTS");
    }

    // ---- Capa 4: servidor gRPC ----
    let service = grpc::ChatHubService::new(hub.clone(), Arc::new(outbound));
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let grpc_addr = cfg.grpc_addr;
    let shutdown = cancel.clone();
    tasks.spawn(async move {
        info!(addr = %grpc_addr, "gRPC: servidor escuchando");
        tonic::transport::Server::builder()
            .add_service(reflection)
            .add_service(ChatHubServer::new(service))
            .serve_with_shutdown(grpc_addr, shutdown.cancelled_owned())
            .await
            .context("el servidor gRPC falló")
    });

    // Corre hasta Ctrl+C o hasta que alguna tarea termine (lo que indica un problema).
    tokio::select! {
        _ = shutdown_signal() => info!("señal de apagado recibida"),
        Some(result) = tasks.join_next() => match result {
            Ok(Ok(())) => warn!("una tarea terminó inesperadamente"),
            Ok(Err(e)) => error!(error = %format!("{e:#}"), "una tarea falló"),
            Err(e) => error!(error = %e, "una tarea entró en pánico"),
        },
    }

    cancel.cancel();
    while let Some(result) = tasks.join_next().await {
        if let Ok(Err(e)) = result {
            error!(error = %format!("{e:#}"), "error al detener una tarea");
        }
    }
    info!("apagado completo");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
