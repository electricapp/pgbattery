//! SSL/TLS handling for the Gateway.
//!
//! Supports SSL termination mode where the proxy terminates SSL
//! and connects to backend in plaintext.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream as ServerTlsStream;

use crate::config::SslModeConfig;
use crate::error::{Error, Result};

/// TLS configuration for the gateway.
pub struct TlsConfig {
    /// Server TLS acceptor (for SSL termination mode)
    pub acceptor: Option<TlsAcceptor>,
    /// SSL mode setting
    pub mode: SslModeConfig,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("mode", &self.mode)
            .field("acceptor_configured", &self.acceptor.is_some())
            .finish()
    }
}

impl TlsConfig {
    /// Create a new TLS configuration.
    ///
    /// - `mode`: SSL mode (Disable, Terminate, Passthrough)
    /// - `cert_path`: Path to PEM certificate file (for Terminate mode)
    /// - `key_path`: Path to PEM private key file (for Terminate mode)
    ///
    /// # Errors
    /// Returns an error if `Terminate` mode is selected without cert/key paths,
    /// the cert or key cannot be loaded, or the server config cannot be built.
    pub fn new(
        mode: SslModeConfig,
        cert_path: Option<&Path>,
        key_path: Option<&Path>,
    ) -> Result<Self> {
        let acceptor = match mode {
            SslModeConfig::Terminate => {
                let cert_path = cert_path.ok_or_else(|| {
                    Error::Tls("Certificate path required for SSL termination".to_string())
                })?;
                let key_path = key_path.ok_or_else(|| {
                    Error::Tls("Key path required for SSL termination".to_string())
                })?;

                let certs = load_certs(cert_path)?;
                let key = load_private_key(key_path)?;

                // Pin TLS 1.3 on the client-facing gateway port. Dropping TLS
                // 1.2 removes the legacy cipher suites (RSA key transport, CBC
                // modes, renegotiation) entirely. This rejects pre-1.3 clients,
                // which is acceptable here by design.
                let config =
                    ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                        .with_no_client_auth()
                        .with_single_cert(certs, key)
                        .map_err(|e| Error::Tls(format!("Failed to build server config: {e}")))?;

                Some(TlsAcceptor::from(Arc::new(config)))
            }
            _ => None,
        };

        Ok(Self { acceptor, mode })
    }
}

/// Upgrade a TCP stream to TLS.
///
/// # Errors
/// Returns an error if the TLS handshake fails.
pub async fn upgrade_to_tls(
    stream: TcpStream,
    acceptor: &TlsAcceptor,
) -> Result<ServerTlsStream<TcpStream>> {
    acceptor
        .accept(stream)
        .await
        .map_err(|e| Error::Tls(format!("TLS handshake failed: {e}")))
}

/// Load certificates from a PEM file.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file =
        File::open(path).map_err(|e| Error::Tls(format!("Failed to open cert file: {e}")))?;
    let mut reader = BufReader::new(file);

    let certs: Vec<CertificateDer<'static>> = certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Tls(format!("Failed to parse certificates: {e}")))?;

    if certs.is_empty() {
        return Err(Error::Tls("No certificates found in file".to_string()));
    }

    tracing::debug!(count = certs.len(), "Loaded TLS certificates");
    Ok(certs)
}

/// Load a private key from a PEM file.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|e| Error::Tls(format!("Failed to open key file: {e}")))?;
    let mut reader = BufReader::new(file);

    let key = private_key(&mut reader)
        .map_err(|e| Error::Tls(format!("Failed to parse private key: {e}")))?
        .ok_or_else(|| Error::Tls("No private key found in file".to_string()))?;

    tracing::debug!("Loaded TLS private key");
    Ok(key)
}

/// A wrapper for either a plain TCP stream or a TLS stream.
pub enum MaybeTlsStream {
    /// Plain TCP connection
    Plain(TcpStream),
    /// Server-side TLS connection (client -> proxy)
    ServerTls(Box<ServerTlsStream<TcpStream>>),
}

impl std::fmt::Debug for MaybeTlsStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(s) => f.debug_tuple("Plain").field(s).finish(),
            Self::ServerTls(_) => f.debug_tuple("ServerTls").finish(),
        }
    }
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::ServerTls(s) => std::pin::Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::ServerTls(s) => std::pin::Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::ServerTls(s) => std::pin::Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::ServerTls(s) => std::pin::Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_tls_config_disable_mode_no_acceptor() {
        let cfg = TlsConfig::new(SslModeConfig::Disable, None, None).unwrap();
        assert!(cfg.acceptor.is_none());
        assert_eq!(cfg.mode, SslModeConfig::Disable);
    }

    #[test]
    fn test_tls_config_passthrough_mode_no_acceptor() {
        let cfg = TlsConfig::new(SslModeConfig::Passthrough, None, None).unwrap();
        assert!(cfg.acceptor.is_none());
        assert_eq!(cfg.mode, SslModeConfig::Passthrough);
    }

    #[test]
    fn test_tls_config_terminate_requires_cert_path() {
        let err = TlsConfig::new(SslModeConfig::Terminate, None, None)
            .expect_err("expected error");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("Certificate path required"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_tls_config_terminate_requires_key_path() {
        let tmp = NamedTempFile::new().unwrap();
        let err = TlsConfig::new(SslModeConfig::Terminate, Some(tmp.path()), None)
            .expect_err("expected error");
        let msg = format!("{err:?}");
        assert!(msg.contains("Key path required"), "unexpected: {msg}");
    }

    #[test]
    fn test_load_certs_empty_file_returns_error() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"").unwrap();
        let err = load_certs(tmp.path()).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("No certificates found"), "unexpected: {msg}");
    }

    #[test]
    fn test_load_certs_garbage_content_returns_error() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"this is not a PEM file\n").unwrap();
        // rustls-pemfile silently ignores non-PEM blocks; the empty result triggers our guard
        let err = load_certs(tmp.path()).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("No certificates found"), "unexpected: {msg}");
    }

    #[test]
    fn test_load_private_key_empty_file_returns_error() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"").unwrap();
        let err = load_private_key(tmp.path()).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("No private key found"), "unexpected: {msg}");
    }
}
