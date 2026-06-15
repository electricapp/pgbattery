//! TLS support for Raft inter-node communication.
//!
//! Provides mutual TLS (mTLS) for secure Raft RPC transport.
//! Each node presents its certificate and verifies peer certificates.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pemfile::{certs, private_key};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::error::{Error, Result};

/// TLS configuration for Raft communication.
#[derive(Clone)]
pub struct RaftTlsConfig {
    /// TLS acceptor for incoming connections
    pub acceptor: TlsAcceptor,
    /// TLS connector for outgoing connections
    pub connector: TlsConnector,
}

impl std::fmt::Debug for RaftTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftTlsConfig")
            .field("acceptor", &"TlsAcceptor")
            .field("connector", &"TlsConnector")
            .finish()
    }
}

impl RaftTlsConfig {
    /// Create a new Raft TLS configuration from certificate paths.
    ///
    /// Requires mutual TLS (mTLS) - all three paths are mandatory.
    /// Each node must present a valid certificate signed by the CA.
    ///
    /// # Arguments
    /// * `cert_path` - Path to the node's certificate (PEM format)
    /// * `key_path` - Path to the node's private key (PEM format)
    /// * `ca_path` - Path to CA certificate for verifying peers (required for mTLS)
    ///
    /// # Errors
    /// Returns an error if any of the cert, key, or CA files cannot be opened
    /// or parsed, or the client/server TLS configs cannot be built.
    pub fn new(cert_path: &Path, key_path: &Path, ca_path: &Path) -> Result<Self> {
        // Load certificate chain
        let cert_file = File::open(cert_path).map_err(|e| {
            Error::Tls(format!(
                "Failed to open Raft TLS cert {}: {}",
                cert_path.display(),
                e
            ))
        })?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<CertificateDer<'static>> = certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Tls(format!("Failed to parse Raft TLS certs: {e}")))?;

        if certs.is_empty() {
            return Err(Error::Tls(
                "No certificates found in Raft TLS cert file".into(),
            ));
        }

        // Load private key
        let key_file = File::open(key_path).map_err(|e| {
            Error::Tls(format!(
                "Failed to open Raft TLS key {}: {}",
                key_path.display(),
                e
            ))
        })?;
        let mut key_reader = BufReader::new(key_file);
        let key = private_key(&mut key_reader)
            .map_err(|e| Error::Tls(format!("Failed to parse Raft TLS key: {e}")))?
            .ok_or_else(|| Error::Tls("No private key found in Raft TLS key file".into()))?;

        // Build root certificate store from CA
        let root_store = Self::build_root_store(ca_path)?;

        // Build server config with mTLS - verify client certificates against our CA
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store.clone()))
            .build()
            .map_err(|e| Error::Tls(format!("Failed to build client cert verifier: {e}")))?;

        // Pin TLS 1.3 — Raft RPC is an internal control-plane channel
        // between our own nodes, so we have no compatibility reason to
        // accept TLS 1.2 here. TLS 1.3 removes all the legacy ciphers
        // (RSA key transport, CBC modes, renegotiation) and is the only
        // version we want a wire-protocol attacker to be allowed to
        // negotiate against the cluster.
        let server_config =
            ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(certs.clone(), key.clone_key())
                .map_err(|e| Error::Tls(format!("Failed to build Raft TLS server config: {e}")))?;

        // Build client config (for connecting to peers) — same TLS 1.3 floor.
        let client_config =
            ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::Tls(format!("Failed to build Raft TLS client config: {e}")))?;

        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let connector = TlsConnector::from(Arc::new(client_config));

        tracing::info!(
            cert_path = %cert_path.display(),
            "Raft TLS configured"
        );

        Ok(Self {
            acceptor,
            connector,
        })
    }

    /// Build root certificate store from CA file.
    fn build_root_store(ca_path: &Path) -> Result<RootCertStore> {
        let mut root_store = RootCertStore::empty();

        let ca_file = File::open(ca_path).map_err(|e| {
            Error::Tls(format!(
                "Failed to open Raft TLS CA {}: {}",
                ca_path.display(),
                e
            ))
        })?;
        let mut ca_reader = BufReader::new(ca_file);
        let ca_certs: Vec<CertificateDer<'static>> = certs(&mut ca_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Tls(format!("Failed to parse Raft TLS CA certs: {e}")))?;

        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| Error::Tls(format!("Failed to add CA cert to root store: {e}")))?;
        }

        tracing::debug!(
            ca_path = %ca_path.display(),
            cert_count = root_store.len(),
            "Loaded CA certificates for Raft mTLS"
        );

        Ok(root_store)
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
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Self-signed test certificate and key for testing
    // Generated with: openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"
    const TEST_CERT: &str = r"-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUdAFZAvAqrgu/CuIYfsWtAnccAQYwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDEwNjAwMzAzMVoXDTI3MDEw
NjAwMzAzMVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAz20S0BPoOdp2+PRSwY0T3DNKu74Jhbj9h4MX2v8hgY5z
HAQmjFEhd2b8oEsoD70FUEjaH6PRHsaOXiXPW79IjbzxCT7SWYMARMx+/iRKH+2L
1O6xyx8S+93v4q/zps/cxBurQE31/xa1cBIRQGvIP2MzqpOYSaJaCqrdpsgXhawN
b+AH7ylSdi31/qrbqbQLnlDQ6CBalIpN1tK0iltRA6u5t0Wyhj8CEPfQaWmRNHw7
H5SCKlfV5Bbyi7Ls2wCYboK97RBF+y5M31vOSZ2XgASwAc7wWRa30pyjzbm8HzR4
8vnvUDn+FGcXRmkCio+SawuENbS4iYaJIWf+sOIJ6wIDAQABo1MwUTAdBgNVHQ4E
FgQUpXTRWg9sIC50BglPiz6TvQ1J3uQwHwYDVR0jBBgwFoAUpXTRWg9sIC50BglP
iz6TvQ1J3uQwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAIAGD
i5zzaFTgVZASbOQ4Hm2KccZ25yTPgM7ozLljCLYFD0dhc9ic55QUxg7TSb7AtTm8
hoUEc7XrXdXO3N57DBOTGmhx8cCviEtZbx5XSpJSLpUqgLmSp1Ka8wnGPpPtantf
TtwgidBPVlK5VNNN69B49eb2K2T/H/WdL3c+QtzLQjfWZp9EiRyvcoSkWU/FhkR8
Ftn+XOo01JjKVaOJhu17te8qJwe8Z9IsM6Rc5/8GDNRiu/mq2/H6Y/yA3cFdFvIo
Qj9WcnSszz6cozF1FmAuOkKi9dbMaYpwlk/BVsEfsI88qc3eIPA6YaN6/cUp0G2j
JHGGjw4mnUT0p+Cj7Q==
-----END CERTIFICATE-----";

    const TEST_KEY: &str = r"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDPbRLQE+g52nb4
9FLBjRPcM0q7vgmFuP2Hgxfa/yGBjnMcBCaMUSF3ZvygSygPvQVQSNofo9Eexo5e
Jc9bv0iNvPEJPtJZgwBEzH7+JEof7YvU7rHLHxL73e/ir/Omz9zEG6tATfX/FrVw
EhFAa8g/YzOqk5hJoloKqt2myBeFrA1v4AfvKVJ2LfX+qtuptAueUNDoIFqUik3W
0rSKW1EDq7m3RbKGPwIQ99BpaZE0fDsflIIqV9XkFvKLsuzbAJhugr3tEEX7Lkzf
W85JnZeABLABzvBZFrfSnKPNubwfNHjy+e9QOf4UZxdGaQKKj5JrC4Q1tLiJhokh
Z/6w4gnrAgMBAAECggEAH6Jn6xoOIbyixmcks+YbMEsWS7m45A8Jg4BHxBuC9apO
/efYJTA+ZWJshtnPe079n3fs5ERsoT/w+ITVsg1jOFKnhBxzojkrclKpz4KjV2k4
GTpqHStZDtaTPkRNaZRr+/CoVn9NVCEXfMcUoHmPqbMsbwhhgmFjUBstAMGlbLpB
a8KGVT11d/JCMKIoTco53LGOmyJiq1UrukzufO3dgdPtrJU4TzlsF3Wxr5sQ+TC5
xy287zFMMa8WTAiOyZvL5CJITu8mxPAv/VGRhCX/Gw5GfLtN2lnDf3IGwErZqWeS
7DcWcBpHUZWxmS50gSc0gouF5jDMzNMu4IUqAZiq0QKBgQDoiRH9I+/umlOCjQTW
+pcwEhi+8rdtdm8R9GKrN7wWjLsRYeGoZ9Ef81O54NAxEDNuhBobHqgqHFhNRIor
9b1xRDOgz/0TuYTu+awV9eXW12CCiwHX3tj4kjpvww+Hq5GcxXNbxmZeQoWaLRqZ
xR/ua5LMFPiuNO07yIH7qiCJQwKBgQDkW17AjGAs4iKs8oyCcbsaOW19pIzq0dXm
BLDRDhK0kZeDGuTSzzc3qmOKPpwtmeFmskkyiopebhe6OeU+I06j7VIMQTp+c8l2
I7tgyin0HRE/piOTOREBYN9x42+uSQk53/hHy+OsOE5dMEEFlTbxRzZRyVNi/GoU
auc6ThH+OQKBgBkRVcAdVKs7Nc94FlJ6lzvWZ5aGIeIKB3U/DDf6/SrNJwl7rNDz
yCaSm68JHkh7v5+lXA8aYfSQM7C4t9B/YFnKiWpHobezozID9loztQBRHZVVGPDF
lExPrz8HHzB3/W2SF5qIK9bzguWZASochxGzxRJ9HEXjbMOqHOEdeP5zAoGAGgO2
taTISBSy8pTnIO0n7YLhUFDwpMem4H9kTUyXIO79HbhwnPtyROsqT9N2I1PGc9aX
tCRIQx2zokl6LiwDh3U/xZmguksihkznycz+Hos5LdEVeG4l28xXaDgKvwYfAPLc
7AD0POhlNQSMQ8CN88qzC3ot/7bVtuG+2cuPDTECgYEA40s4p2um/HAlArCayU+4
bmuCLzhBYxwsFiGx2SbGc1tUuQUnnXX6Zvg1rRTPe9DPP2lq9ReWqbz7y03rIA/h
D7DPV1/+8972OAE6AP3wOhbvRaOdzJWe6fuefmZfBwR2Gq+YEo3A5jfJRxEYxV8O
9M28lYYoMbJsMmZwt9cJ9UM=
-----END PRIVATE KEY-----";

    #[test]
    fn test_mtls_config_creation_succeeds_with_valid_certs() {
        // Install default crypto provider for rustls
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok();

        let mut cert_file = NamedTempFile::new().unwrap();
        let mut key_file = NamedTempFile::new().unwrap();
        let mut ca_file = NamedTempFile::new().unwrap();

        cert_file.write_all(TEST_CERT.as_bytes()).unwrap();
        key_file.write_all(TEST_KEY.as_bytes()).unwrap();
        // Self-signed cert acts as its own CA
        ca_file.write_all(TEST_CERT.as_bytes()).unwrap();

        let result = RaftTlsConfig::new(cert_file.path(), key_file.path(), ca_file.path());

        assert!(
            result.is_ok(),
            "mTLS config should succeed with valid certs: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_mtls_config_fails_with_invalid_certs() {
        let mut cert_file = NamedTempFile::new().unwrap();
        let mut key_file = NamedTempFile::new().unwrap();
        let mut ca_file = NamedTempFile::new().unwrap();

        cert_file.write_all(b"not a valid cert").unwrap();
        key_file.write_all(b"not a valid key").unwrap();
        ca_file.write_all(b"not a valid ca").unwrap();

        let result = RaftTlsConfig::new(cert_file.path(), key_file.path(), ca_file.path());

        assert!(result.is_err());
    }

    #[test]
    fn test_mtls_config_fails_with_missing_files() {
        let result = RaftTlsConfig::new(
            Path::new("/nonexistent/cert.pem"),
            Path::new("/nonexistent/key.pem"),
            Path::new("/nonexistent/ca.pem"),
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to open"));
    }
}
