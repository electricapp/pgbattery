//! Gateway TCP proxy implementation.
//!
//! The Gateway accepts client connections and proxies them to the
//! current `PostgreSQL` leader, handling failover transparently.

use parking_lot::RwLock;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::config::SslModeConfig;
use crate::config::constants::METRICS_SYNC_INTERVAL_MS;
use crate::error::{Error, Result};
use crate::governor::raft::FenceState;

use super::connection::{
    ConnectionRegistry, ConnectionState, ProxyMode, SharedConnectionState, SslMode,
};
use super::protocol::{is_gssenc_request, is_ssl_request};
use super::ssl::{MaybeTlsStream, TlsConfig, upgrade_to_tls};
use crate::gateway::handlers::ConnectionHandler;

/// Pause after a failed `accept()` before retrying.
///
/// Accept errors are usually resource exhaustion (EMFILE/ENFILE) or a
/// connection reset racing the accept; retrying immediately on fd
/// exhaustion turns the accept loop into a busy spin that starves the very
/// connections holding the fds.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Gateway configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Address to listen for client connections
    pub listen_addr: SocketAddr,

    /// Local management API address for leader discovery/probing.
    pub mgmt_addr: SocketAddr,

    /// SSL/TLS mode
    pub ssl_mode: SslModeConfig,

    /// Path to SSL certificate (for terminate mode)
    pub ssl_cert_path: Option<PathBuf>,

    /// Path to SSL private key (for terminate mode)
    pub ssl_key_path: Option<PathBuf>,

    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,

    /// Idle timeout in milliseconds
    pub idle_timeout_ms: u64,
}

/// The Gateway - `PostgreSQL`-aware TCP proxy.
pub struct Gateway {
    config: GatewayConfig,
    tls_config: Arc<TlsConfig>,
    registry: Arc<ConnectionRegistry>,
    leader_rx: watch::Receiver<Option<SocketAddr>>,
    fence_rx: watch::Receiver<FenceState>,
    lease: crate::governor::SharedLeaseState,
    shutdown_rx: watch::Receiver<bool>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("config", &self.config)
            .field("leader", &*self.leader_rx.borrow())
            .field("fence", &*self.fence_rx.borrow())
            .finish_non_exhaustive()
    }
}

impl Gateway {
    /// Create a new Gateway instance.
    ///
    /// # Errors
    /// Returns an error if `ssl_mode=passthrough` is requested (unsupported) or
    /// the TLS configuration cannot be built.
    pub fn new(
        config: GatewayConfig,
        leader_rx: watch::Receiver<Option<SocketAddr>>,
        fence_rx: watch::Receiver<FenceState>,
        lease: crate::governor::SharedLeaseState,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Self> {
        if config.ssl_mode == SslModeConfig::Passthrough {
            return Err(Error::Tls(
                "ssl_mode=passthrough is currently unsupported; use 'disable' or 'terminate'"
                    .to_string(),
            ));
        }

        let registry = Arc::new(ConnectionRegistry::new());

        // Create TLS configuration
        let tls_config = Arc::new(TlsConfig::new(
            config.ssl_mode,
            config.ssl_cert_path.as_deref(),
            config.ssl_key_path.as_deref(),
        )?);

        Ok(Self {
            config,
            tls_config,
            registry,
            leader_rx,
            fence_rx,
            lease,
            shutdown_rx,
        })
    }

    /// Run the Gateway, accepting client connections.
    ///
    /// # Errors
    /// Returns an error if the listener cannot bind to the configured address.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        let mut shutdown_rx = self.shutdown_rx.clone();

        tracing::info!(
            addr = %self.config.listen_addr,
            "Gateway listening for client connections"
        );

        // Spawn background task to sync metrics from atomic counters
        // Batches updates instead of syncing on every query completion
        let metrics_registry = self.registry.clone();
        let mut metrics_shutdown_rx = self.shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(METRICS_SYNC_INTERVAL_MS));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        metrics_registry.sync_metrics();
                    }
                    _ = metrics_shutdown_rx.changed() => {
                        if *metrics_shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        loop {
            let (socket, peer_addr) = tokio::select! {
                accept = listener.accept() => match accept {
                    Ok(conn) => conn,
                    Err(e) => {
                        // A per-connection accept error (fd exhaustion, a
                        // racing RST) must not kill the accept loop — the
                        // node would keep reporting healthy while its
                        // client port is dead. Only binding the listener
                        // is fatal.
                        tracing::warn!(error = %e, "Failed to accept client connection");
                        metrics::counter!("pgbattery_gateway_accept_errors").increment(1);
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                        continue;
                    }
                },
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Gateway shutting down");
                        break;
                    }
                    continue;
                }
            };
            if let Err(e) = socket.set_nodelay(true) {
                // Nagle stays on for this connection — higher latency, not
                // a reason to refuse service.
                tracing::debug!(peer = %peer_addr, error = %e, "set_nodelay failed");
            }

            // Fast connection ID generation - no crypto overhead
            let conn_id = self.registry.next_id();

            tracing::debug!(
                conn_id = conn_id,
                peer = %peer_addr,
                "Accepted new client connection"
            );

            metrics::counter!("pgbattery_connections_total").increment(1);

            // Create connection state
            let state = Arc::new(RwLock::new(ConnectionState::new(conn_id)));
            self.registry.register(state.clone());

            // Clone what we need for the handler task
            let registry = self.registry.clone();
            let leader_rx = self.leader_rx.clone();
            let fence_rx = self.fence_rx.clone();
            let config = self.config.clone();
            let tls_config = self.tls_config.clone();
            let lease = self.lease.clone();

            // Spawn handler task
            tokio::spawn(async move {
                // Handle SSL negotiation on raw socket first
                let (client, preread) =
                    match negotiate_ssl(socket, &tls_config, conn_id, &state).await {
                        Ok(stream) => stream,
                        Err(e) => {
                            tracing::debug!(conn_id = conn_id, error = %e, "SSL negotiation failed");
                            registry.unregister(conn_id);
                            return;
                        }
                    };

                let result = ConnectionHandler::new(
                    conn_id,
                    client,
                    state.clone(),
                    leader_rx,
                    fence_rx,
                    config,
                    registry.clone(),
                    lease,
                    preread,
                )
                .run()
                .await;

                match &result {
                    Ok(()) => {
                        tracing::debug!(conn_id = conn_id, "Connection closed normally");
                    }
                    Err(e) => {
                        tracing::debug!(conn_id = conn_id, error = %e, "Connection closed");
                    }
                }

                registry.unregister(conn_id);
            });
        }

        Ok(())
    }
}

/// Handle encryption negotiation on a raw TCP connection.
///
/// `PostgreSQL` clients may open with a `GSSENCRequest`, an `SSLRequest`,
/// or go straight to the startup/cancel message. The first two are 8-byte
/// requests answered with a single raw byte, and both must be answered
/// *here* — forwarding either to the backend elicits an unframed 1-byte
/// reply that desynchronizes message parsing for the rest of the session.
/// GSS encryption is unsupported, so a `GSSENCRequest` is refused with 'N'
/// and the client falls back (per protocol: at most one GSS and one SSL
/// attempt per connection).
///
/// The 8-byte header is read with `read_exact` rather than a single
/// `peek`, so a request split across TCP segments is reassembled instead
/// of being misclassified as a plaintext startup. When the bytes turn out
/// to begin a startup or cancel message they are returned alongside the
/// stream for the connection handler to consume.
async fn negotiate_ssl(
    mut socket: TcpStream,
    tls_config: &TlsConfig,
    conn_id: u64,
    state: &SharedConnectionState,
) -> Result<(MaybeTlsStream, Option<[u8; 8]>)> {
    let mut header = [0u8; 8];
    let mut gss_refused = false;
    loop {
        socket.read_exact(&mut header).await?;
        if is_gssenc_request(&header) {
            if gss_refused {
                return Err(Error::Protocol("Repeated GSSENCRequest".to_string()));
            }
            gss_refused = true;
            socket.write_all(b"N").await?;
            socket.flush().await?;
            tracing::debug!(
                conn_id = conn_id,
                "Refused GSSENCRequest (GSS encryption unsupported)"
            );
            continue;
        }
        if is_ssl_request(&header) {
            break;
        }
        // Startup or cancel message — hand the already-read bytes onward.
        return Ok((MaybeTlsStream::Plain(socket), Some(header)));
    }

    match tls_config.mode {
        SslModeConfig::Disable => {
            // Send 'N' - SSL not supported
            socket.write_all(b"N").await?;
            socket.flush().await?;
            tracing::debug!(
                conn_id = conn_id,
                "SSL disabled, sending plaintext connection"
            );
            Ok((MaybeTlsStream::Plain(socket), None))
        }
        SslModeConfig::Terminate => {
            // Send 'S' to indicate SSL is supported
            socket.write_all(b"S").await?;
            socket.flush().await?;

            // Upgrade to TLS using the ssl module helper
            if let Some(ref acceptor) = tls_config.acceptor {
                let tls_stream = upgrade_to_tls(socket, acceptor).await?;
                tracing::debug!(
                    conn_id = conn_id,
                    "SSL connection established (termination mode)"
                );
                {
                    let mut s = state.write();
                    s.ssl_mode = SslMode::Terminated;
                }
                Ok((MaybeTlsStream::ServerTls(Box::new(tls_stream)), None))
            } else {
                // This shouldn't happen if TlsConfig was created properly
                Err(Error::Tls("TLS acceptor not configured".to_string()))
            }
        }
        SslModeConfig::Passthrough => {
            // Send 'S' and mark as passthrough - client will negotiate SSL with backend
            socket.write_all(b"S").await?;
            socket.flush().await?;
            {
                let mut s = state.write();
                s.proxy_mode = ProxyMode::SslPassthrough;
                s.ssl_mode = SslMode::Passthrough;
            }
            tracing::debug!(conn_id = conn_id, "SSL passthrough mode enabled");
            Ok((MaybeTlsStream::Plain(socket), None))
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[test]
    fn test_gateway_config() {
        let config = GatewayConfig {
            listen_addr: "127.0.0.1:5432".parse().unwrap(),
            mgmt_addr: "127.0.0.1:9091".parse().unwrap(),
            ssl_mode: SslModeConfig::Disable,
            ssl_cert_path: None,
            ssl_key_path: None,
            connection_timeout_ms: 5000,
            idle_timeout_ms: 300_000,
        };

        assert_eq!(config.connection_timeout_ms, 5000);
    }

    #[test]
    fn test_gateway_new_rejects_passthrough_mode() {
        let config = GatewayConfig {
            listen_addr: "127.0.0.1:5432".parse().unwrap(),
            mgmt_addr: "127.0.0.1:9091".parse().unwrap(),
            ssl_mode: SslModeConfig::Passthrough,
            ssl_cert_path: None,
            ssl_key_path: None,
            connection_timeout_ms: 5000,
            idle_timeout_ms: 300_000,
        };
        let (_leader_tx, leader_rx) = watch::channel(None::<SocketAddr>);
        let (_fence_tx, fence_rx) = watch::channel(FenceState::unfenced());
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let lease = crate::governor::new_shared_lease();

        let result = Gateway::new(config, leader_rx, fence_rx, lease, shutdown_rx);
        assert!(result.is_err());
    }
}
