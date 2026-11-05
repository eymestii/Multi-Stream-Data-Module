//! Cliente gRPC para el servidor OAuth (módulo externo, ya existente).
//!
//! Se conecta al servidor gRPC OAuth (p. ej. localhost:50052) y expone operaciones:
//! - GetToken(platform): obtiene el access token actual.
//! - RefreshToken(platform): renueva el access_token usando el refresh_token guardado.
//! - GetCredentials(platform): devuelve todas las credenciales de una plataforma.
//! - RevokeToken(platform): revoca un token.
//!
//! La dirección del servidor se configura con la variable de entorno OAUTH_GRPC_ADDR
//! (por defecto http://127.0.0.1:50052).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::oauth_proto::oauth::{
    o_auth_service_client::OAuthServiceClient as GrpcOAuthClient, CredentialsRequest,
    CredentialsResponse, GetTokenRequest, RefreshRequest, RevokeRequest, RevokeResponse,
    TokenResponse,
};

/// Wrapper de un access token con su fecha de expiración para saber cuándo refrescar.
#[derive(Clone, Debug)]
pub struct CachedToken {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<SystemTime>,
    pub scope: Option<String>,
    pub platform: String,
}

impl CachedToken {
    pub fn is_expired_or_expiring_soon(&self, threshold_secs: u64) -> bool {
        match self.expires_at {
            None => true, // sin fecha de expiración, asumimos que hay que refrescar
            Some(expires_at) => {
                let now = SystemTime::now();
                match expires_at.duration_since(now) {
                    Ok(duration) => duration.as_secs() <= threshold_secs,
                    Err(_) => true, // ya expiró
                }
            }
        }
    }
}

/// Cliente gRPC para el servidor OAuth.
pub struct OAuthClient {
    client: GrpcOAuthClient<Channel>,
    addr: String,
}

impl OAuthClient {
    /// Crea un nuevo cliente conectando al servidor OAuth en `addr`.
    pub async fn connect(addr: &str) -> Result<Self> {
        let channel = Channel::from_shared(addr.to_string())
            .context("dirección de servidor OAuth inválida")?
            .connect_lazy();
        let client = GrpcOAuthClient::new(channel);
        info!(addr = %addr, "OAuth: cliente gRPC conectado");
        Ok(Self {
            client,
            addr: addr.to_string(),
        })
    }

    /// Llama a GetToken(platform) en el servidor OAuth.
    pub async fn get_token(&mut self, platform: &str) -> Result<TokenResponse> {
        let request = tonic::Request::new(GetTokenRequest {
            platform: platform.to_string(),
        });
        let response = self
            .client
            .get_token(request)
            .await
            .context("error llamando a GetToken en el servidor OAuth")?
            .into_inner();
        debug!(platform, success = response.success, error = %response.error, "OAuth: GetToken respuesta");
        Ok(response)
    }

    /// Llama a RefreshToken(platform) en el servidor OAuth.
    pub async fn refresh_token(&mut self, platform: &str) -> Result<TokenResponse> {
        let request = tonic::Request::new(RefreshRequest {
            platform: platform.to_string(),
        });
        let response = self
            .client
            .refresh_token(request)
            .await
            .context("error llamando a RefreshToken en el servidor OAuth")?
            .into_inner();
        debug!(platform, success = response.success, error = %response.error, "OAuth: RefreshToken respuesta");
        Ok(response)
    }

    /// Llama a GetCredentials(platform) en el servidor OAuth.
    pub async fn get_credentials(&mut self, platform: &str) -> Result<CredentialsResponse> {
        let request = tonic::Request::new(CredentialsRequest {
            platform: platform.to_string(),
        });
        let response = self
            .client
            .get_credentials(request)
            .await
            .context("error llamando a GetCredentials en el servidor OAuth")?
            .into_inner();
        debug!(platform, found = response.found, error = %response.error, "OAuth: GetCredentials respuesta");
        Ok(response)
    }

    /// Llama a RevokeToken(platform) en el servidor OAuth.
    pub async fn revoke_token(&mut self, platform: &str) -> Result<RevokeResponse> {
        let request = tonic::Request::new(RevokeRequest {
            platform: platform.to_string(),
        });
        let response = self
            .client
            .revoke_token(request)
            .await
            .context("error llamando a RevokeToken en el servidor OAuth")?
            .into_inner();
        debug!(platform, success = response.success, error = %response.error, "OAuth: RevokeToken respuesta");
        Ok(response)
    }
}

/// Gestor de tokens en memoria que se autofresca llamando al servidor OAuth cuando es necesario.
pub struct TokenManager {
    client: Arc<Mutex<OAuthClient>>,
    twitch_token: Option<CachedToken>,
    kick_token: Option<CachedToken>,
    refresh_threshold_secs: u64,
}

impl TokenManager {
    pub async fn new(oauth_addr: &str) -> Result<Self> {
        let client = OAuthClient::connect(oauth_addr).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            twitch_token: None,
            kick_token: None,
            refresh_threshold_secs: 60, // refrescar si faltan 60s o menos
        })
    }

    /// Devuelve el access_token de Twitch, refrescando si es necesario.
    pub async fn twitch_access_token(&mut self) -> Result<String> {
        let needs_refresh = match &self.twitch_token {
            None => true,
            Some(token) => token.is_expired_or_expiring_soon(self.refresh_threshold_secs),
        };
        if !needs_refresh {
            return Ok(self.twitch_token.as_ref().unwrap().access_token.clone());
        }

        info!(platform = "twitch", "OAuth: obteniendo credenciales de Twitch");
        let mut client = self.client.lock().await;
        let creds = client.get_credentials("twitch").await?;
        drop(client); // liberar el lock antes de mutar self.twitch_token

        if creds.found == "true" {
            // GetCredentials no devuelve expires_in; tratamos el token como sin fecha conocida.
            let scope = if creds.scope.is_empty() { None } else { Some(creds.scope.clone()) };
            let scope_log = scope.as_deref().map(|s| s.to_string());
            self.twitch_token = Some(CachedToken {
                access_token: creds.access_token,
                refresh_token: if creds.refresh_token.is_empty() {
                    None
                } else {
                    Some(creds.refresh_token)
                },
                expires_at: None,
                scope,
                platform: "twitch".to_string(),
            });
            info!(platform = "twitch", scope = ?scope_log, "OAuth: credenciales de Twitch obtenidas");
            return Ok(self.twitch_token.as_ref().unwrap().access_token.clone());
        }

        // No hay credenciales guardadas, intentar refresh directo
        self.refresh_platform("twitch").await
    }

    /// Devuelve el access_token de Kick, refrescando si es necesario.
    pub async fn kick_access_token(&mut self) -> Result<String> {
        let needs_refresh = match &self.kick_token {
            None => true,
            Some(token) => token.is_expired_or_expiring_soon(self.refresh_threshold_secs),
        };
        if !needs_refresh {
            return Ok(self.kick_token.as_ref().unwrap().access_token.clone());
        }

        info!(platform = "kick", "OAuth: obteniendo credenciales de Kick");
        let mut client = self.client.lock().await;
        let creds = client.get_credentials("kick").await?;
        drop(client);

        if creds.found == "true" {
            // GetCredentials no devuelve expires_in; tratamos el token como sin fecha conocida.
            let scope = if creds.scope.is_empty() { None } else { Some(creds.scope.clone()) };
            let scope_log = scope.as_deref().map(|s| s.to_string());
            self.kick_token = Some(CachedToken {
                access_token: creds.access_token,
                refresh_token: if creds.refresh_token.is_empty() {
                    None
                } else {
                    Some(creds.refresh_token)
                },
                expires_at: None,
                scope,
                platform: "kick".to_string(),
            });
            info!(platform = "kick", scope = ?scope_log, "OAuth: credenciales de Kick obtenidas");
            return Ok(self.kick_token.as_ref().unwrap().access_token.clone());
        }

        self.refresh_platform("kick").await
    }

    /// Marca el token de Twitch como inválido (401) para forzar refresco en la próxima petición.
    pub fn invalidate_twitch(&mut self) {
        self.twitch_token = None;
        warn!("OAuth: token de Twitch invalidado (401), se refrescará en la próxima petición");
    }

    /// Marca el token de Kick como inválido (401) para forzar refresco en la próxima petición.
    pub fn invalidate_kick(&mut self) {
        self.kick_token = None;
        warn!("OAuth: token de Kick invalidado (401), se refrescará en la próxima petición");
    }

    async fn refresh_platform(&mut self, platform: &str) -> Result<String> {
        let mut client = self.client.lock().await;
        let response = client.refresh_token(platform).await?;
        drop(client);

        if !response.success {
            anyhow::bail!("OAuth: RefreshToken falló para {}: {}", platform, response.error);
        }
        let expires_at = if response.expires_in > 0 {
            Some(SystemTime::now() + Duration::from_secs(response.expires_in as u64))
        } else {
            None
        };
        let token = CachedToken {
            access_token: response.access_token.clone(),
            refresh_token: if response.refresh_token.is_empty() {
                None
            } else {
                Some(response.refresh_token)
            },
            expires_at,
            scope: if response.scope.is_empty() { None } else { Some(response.scope) },
            platform: platform.to_string(),
        };
        if platform == "twitch" {
            self.twitch_token = Some(token.clone());
        } else {
            self.kick_token = Some(token.clone());
        }
        info!(platform, expires_in = response.expires_in, success = response.success, "OAuth: token refrescado");
        Ok(response.access_token)
    }
}
