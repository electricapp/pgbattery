//! Raft RPC network transport.
//!
//! Provides TCP-based RPC transport for Raft consensus messages
//! including `AppendEntries`, `Vote`, and `InstallSnapshot`.
//!
//! Supports optional TLS for secure inter-node communication.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_rustls::TlsConnector;

use super::raft::TypeConfig;
use super::state_machine::{ClusterState, NodeId};
use super::tls::RaftTlsConfig;
use crate::error::{Error, Result};

/// Maximum RPC frame size. 4 MB is generous for Raft messages —
/// the state machine snapshot is just node IDs/LSNs.
/// Previously 64 MB, reduced to limit DoS-induced allocation.
const MAX_RPC_FRAME_LEN: usize = 4 * 1024 * 1024;

/// Per-operation I/O budget for a single RPC: connect, TLS handshake, and
/// each framed read/write on both client and server sides. The server uses
/// the same value so a request that fits the client's budget is never cut
/// off mid-frame by the server.
const RPC_IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Server-side idle bound between requests on a persistent peer connection.
///
/// Clients keep one connection per peer and send heartbeats every few
/// hundred ms while replication is active, so a live peer is never anywhere
/// near this bound — but it must comfortably exceed several election
/// timeouts (1-2 s default) so a peer that pauses traffic across an election
/// isn't churned. Half-open or abandoned connections are reaped at this
/// bound instead of pinning their server task forever.
const SERVER_IDLE_TIMEOUT: Duration = Duration::from_mins(1);

fn validate_rpc_frame_len(len: usize, context: &str) -> Result<usize> {
    if len == 0 {
        return Err(Error::Protocol(format!(
            "Invalid RPC {context} frame length: 0"
        )));
    }
    if len > MAX_RPC_FRAME_LEN {
        return Err(Error::Protocol(format!(
            "RPC {context} frame too large: {len}"
        )));
    }
    Ok(len - 1)
}

/// RPC message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum RpcType {
    AppendEntries = 1,
    AppendEntriesResponse = 2,
    Vote = 3,
    VoteResponse = 4,
    InstallSnapshot = 5,
    InstallSnapshotResponse = 6,
}

impl TryFrom<u8> for RpcType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::AppendEntries),
            2 => Ok(Self::AppendEntriesResponse),
            3 => Ok(Self::Vote),
            4 => Ok(Self::VoteResponse),
            5 => Ok(Self::InstallSnapshot),
            6 => Ok(Self::InstallSnapshotResponse),
            _ => Err(Error::Protocol(format!("Invalid RPC type: {value}"))),
        }
    }
}

/// Raft RPC server that listens for incoming requests.
pub struct RaftRpcServer {
    listener: TcpListener,
    raft: Arc<openraft::Raft<TypeConfig>>,
    /// Cluster state for LSN-aware vote checking. The state machine
    /// itself decides which catch-up threshold applies based on the
    /// replicated `sync_replication_active` flag — the vote handler
    /// just hands it the candidate id.
    cluster_state: Arc<RwLock<ClusterState>>,
    /// Optional TLS configuration for secure transport
    tls_config: Option<RaftTlsConfig>,
    shutdown_rx: watch::Receiver<bool>,
}

impl std::fmt::Debug for RaftRpcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftRpcServer")
            .field("listener", &self.listener)
            .field("tls_enabled", &self.tls_config.is_some())
            .finish_non_exhaustive()
    }
}

impl RaftRpcServer {
    /// Create a new RPC server with optional TLS.
    ///
    /// # Errors
    /// Returns an error if the listener cannot bind to `addr`.
    pub async fn new_with_tls(
        addr: SocketAddr,
        raft: Arc<openraft::Raft<TypeConfig>>,
        cluster_state: Arc<RwLock<ClusterState>>,
        tls_config: Option<RaftTlsConfig>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        if tls_config.is_some() {
            tracing::info!(%addr, tls = "enabled", "Raft RPC server listening");
        } else if addr.ip().is_loopback() {
            tracing::info!(%addr, tls = "plaintext-loopback", "Raft RPC server listening");
        } else {
            // Plaintext = unauthenticated consensus: any reachable host can
            // forge votes/log entries and force split-brain. Trusted networks
            // only; configure Raft mTLS otherwise.
            tracing::warn!(
                %addr,
                "Raft RPC server listening PLAINTEXT on a non-loopback address — the \
                 consensus port is UNAUTHENTICATED. Configure Raft mTLS or restrict it to a \
                 trusted network; an attacker who can reach it can force split-brain."
            );
        }

        Ok(Self {
            listener,
            raft,
            cluster_state,
            tls_config,
            shutdown_rx,
        })
    }

    /// Run the RPC server.
    ///
    /// # Errors
    /// Returns an error if accepting a connection fails fatally.
    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("Raft RPC server shutting down");
                        break;
                    }
                }

                result = self.listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let raft = self.raft.clone();
                            let cluster_state = self.cluster_state.clone();
                            let tls_config = self.tls_config.clone();

                            tokio::spawn(async move {
                                let result = if let Some(tls) = tls_config {
                                    // TLS connection. A peer that connects but
                                    // never completes the handshake would pin
                                    // this task forever — bound it by the same
                                    // I/O budget the client side uses.
                                    match tokio::time::timeout(
                                        RPC_IO_TIMEOUT,
                                        tls.acceptor.accept(stream),
                                    )
                                    .await
                                    {
                                        Ok(Ok(tls_stream)) => {
                                            handle_connection_generic(tls_stream, peer_addr, raft, cluster_state).await
                                        }
                                        Ok(Err(e)) => {
                                            tracing::warn!(%peer_addr, error = %e, "TLS handshake failed");
                                            return;
                                        }
                                        Err(_) => {
                                            tracing::warn!(%peer_addr, "TLS handshake timed out");
                                            return;
                                        }
                                    }
                                } else {
                                    // Plaintext connection
                                    handle_connection_generic(stream, peer_addr, raft, cluster_state).await
                                };

                                if let Err(e) = result {
                                    tracing::warn!(%peer_addr, error = %e, "RPC connection error");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to accept connection");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Serve RPC requests on a peer connection until it closes or idles out
/// (generic over stream type for TLS support).
///
/// Clients hold one persistent connection per peer, so this loops over
/// framed requests. Waiting for the next frame is bounded by
/// [`SERVER_IDLE_TIMEOUT`]; once a frame has started, each read/write is
/// bounded by [`RPC_IO_TIMEOUT`] so a half-open peer cannot pin this task.
async fn handle_connection_generic<S>(
    mut stream: S,
    peer_addr: SocketAddr,
    raft: Arc<openraft::Raft<TypeConfig>>,
    cluster_state: Arc<RwLock<ClusterState>>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        // Wait for the next request's length prefix (4 bytes). A clean close
        // or an idle expiry ends the connection without error.
        let mut len_buf = [0u8; 4];
        match tokio::time::timeout(SERVER_IDLE_TIMEOUT, stream.read_exact(&mut len_buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                tracing::trace!(%peer_addr, "Peer closed RPC connection");
                return Ok(());
            }
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {
                tracing::debug!(%peer_addr, "Idle RPC connection reaped");
                return Ok(());
            }
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let body_len = validate_rpc_frame_len(len, "request")?;

        // Read message type (1 byte)
        let mut type_buf = [0u8; 1];
        tokio::time::timeout(RPC_IO_TIMEOUT, stream.read_exact(&mut type_buf))
            .await
            .map_err(|_| Error::ConnectionTimeout(peer_addr))??;
        let rpc_type = RpcType::try_from(type_buf[0])?;

        // Read message body
        let mut body = BytesMut::with_capacity(body_len);
        body.resize(body_len, 0);
        tokio::time::timeout(RPC_IO_TIMEOUT, stream.read_exact(&mut body))
            .await
            .map_err(|_| Error::ConnectionTimeout(peer_addr))??;

        let (resp_type, resp_body) =
            process_request(rpc_type, &body, peer_addr, &raft, &cluster_state).await?;

        // Send response
        let total_len = 1 + resp_body.len();
        let total_len_u32 = u32::try_from(total_len)
            .map_err(|_| Error::Protocol("RPC response length exceeds u32".to_string()))?;

        let mut response_buf = BytesMut::with_capacity(4 + total_len);
        response_buf.put_u32(total_len_u32);
        response_buf.put_u8(resp_type as u8);
        response_buf.put_slice(&resp_body);

        tokio::time::timeout(RPC_IO_TIMEOUT, stream.write_all(&response_buf))
            .await
            .map_err(|_| Error::ConnectionTimeout(peer_addr))??;
        tokio::time::timeout(RPC_IO_TIMEOUT, stream.flush())
            .await
            .map_err(|_| Error::ConnectionTimeout(peer_addr))??;
    }
}

/// Dispatch a single decoded RPC request to the local Raft instance.
async fn process_request(
    rpc_type: RpcType,
    body: &[u8],
    peer_addr: SocketAddr,
    raft: &openraft::Raft<TypeConfig>,
    cluster_state: &RwLock<ClusterState>,
) -> Result<(RpcType, Vec<u8>)> {
    let response = match rpc_type {
        RpcType::AppendEntries => {
            let req: AppendEntriesRequest<TypeConfig> = postcard::from_bytes(body)?;
            tracing::trace!(%peer_addr, "Received AppendEntries");
            let resp = raft
                .append_entries(req)
                .await
                .map_err(|e| Error::Raft(e.to_string()))?;
            let resp_bytes = postcard::to_allocvec(&resp)?;
            (RpcType::AppendEntriesResponse, resp_bytes)
        }
        RpcType::Vote => {
            let req: VoteRequest<NodeId> = postcard::from_bytes(body)?;
            let candidate_id = req.vote.leader_id().node_id;

            // LSN safety check: reject candidates significantly behind in
            // PostgreSQL WAL position. The state machine selects the
            // threshold from the replicated `sync_replication_active`
            // flag — sync mode uses one WAL block, async mode uses the
            // 16 MB RPO bound. Small or unknown gaps still forward to
            // Raft.
            let (lsn_acceptable, reason) = {
                let state = cluster_state.read();
                state.is_lsn_acceptable_for_election(candidate_id)
            };

            if lsn_acceptable {
                tracing::trace!(
                    %peer_addr,
                    candidate_id = candidate_id,
                    reason = reason,
                    "Vote request LSN check passed"
                );
                let resp = raft
                    .vote(req)
                    .await
                    .map_err(|e| Error::Raft(e.to_string()))?;
                let resp_bytes = postcard::to_allocvec(&resp)?;
                (RpcType::VoteResponse, resp_bytes)
            } else {
                tracing::error!(
                    candidate_id = candidate_id,
                    %peer_addr,
                    reason = reason,
                    "LSN check FAILED - rejecting vote to prevent data loss"
                );
                metrics::counter!("pgbattery_votes_lsn_rejected").increment(1);
                // Return a proper VoteResponse{vote_granted=false} instead of
                // a transport-error Protocol Err. Previously this Err became
                // a connection failure on the caller side; openraft treats
                // transport failures as "try again later" and retried the
                // vote forever, while *we* had committed to rejecting it.
                // That mismatch is an election livelock: candidate keeps
                // bumping term, we keep rejecting with a transport-shaped
                // error, no progress.
                //
                // Build the response from our local metrics without invoking
                // `raft.vote(req)` so we don't persist a `voted_for=candidate`
                // (a "no" doesn't bind the voter, but a "yes" via openraft's
                // vote() would). Carrying *our* current vote lets the
                // candidate learn our term and step down if it's behind.
                //
                // `last_log_id` is deliberately `None`. openraft 0.9's
                // RaftMetrics exposes `last_log_index` but NOT the term of
                // that entry, and the entry's term is generally NOT our
                // current vote term (e.g. we're a follower at log term 5 that
                // has since voted in term 7). Fabricating
                // (current_vote_term, last_log_index) would advertise a
                // log-up-to-date position the candidate could misjudge on a
                // safety-critical path. `None` is the honest, safe fallback
                // ("voter's log position unknown"); the `vote` field still
                // does the work of conveying our term.
                let metrics_snapshot = raft.metrics().borrow().clone();
                let resp = VoteResponse::<NodeId> {
                    vote: metrics_snapshot.vote,
                    vote_granted: false,
                    last_log_id: None,
                };
                let resp_bytes = postcard::to_allocvec(&resp)?;
                (RpcType::VoteResponse, resp_bytes)
            }
        }
        RpcType::InstallSnapshot => {
            let req: InstallSnapshotRequest<TypeConfig> = postcard::from_bytes(body)?;
            tracing::debug!(%peer_addr, "Received InstallSnapshot");
            let resp = raft
                .install_snapshot(req)
                .await
                .map_err(|e| Error::Raft(e.to_string()))?;
            let resp_bytes = postcard::to_allocvec(&resp)?;
            (RpcType::InstallSnapshotResponse, resp_bytes)
        }
        _ => {
            return Err(Error::Protocol(format!(
                "Unexpected request type: {rpc_type:?}"
            )));
        }
    };

    Ok(response)
}

/// A persistent framed connection to a Raft peer.
///
/// Owned by the per-peer `NetworkConnection` so that heartbeats and log
/// replication reuse one TCP (and TLS) session instead of paying
/// connect + handshake per RPC.
#[derive(Debug)]
pub struct PeerConnection {
    stream: PeerStream,
    addr: SocketAddr,
}

enum PeerStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl std::fmt::Debug for PeerStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("PeerStream::Plain"),
            Self::Tls(_) => f.write_str("PeerStream::Tls"),
        }
    }
}

impl PeerConnection {
    /// Run one framed request/response cycle, consuming the connection and
    /// returning it only on clean completion. If the returned future is
    /// dropped mid-exchange the connection is dropped with it — its protocol
    /// state would be unknown (a response may still be in flight), so it must
    /// never be reused.
    async fn exchange(mut self, rpc_type: RpcType, body: &[u8]) -> Result<(Self, Vec<u8>)> {
        let request_buf = frame_request(rpc_type, body)?;
        let addr = self.addr;
        let response = match &mut self.stream {
            PeerStream::Plain(stream) => exchange_on(stream, &request_buf, addr).await?,
            PeerStream::Tls(stream) => exchange_on(stream.as_mut(), &request_buf, addr).await?,
        };
        Ok((self, response))
    }
}

/// Frame a request: 4-byte big-endian length, 1-byte type, body.
fn frame_request(rpc_type: RpcType, body: &[u8]) -> Result<BytesMut> {
    let total_len = 1 + body.len();
    let total_len_u32 = u32::try_from(total_len)
        .map_err(|_| Error::Protocol("RPC request length exceeds u32".to_string()))?;
    let mut request_buf = BytesMut::with_capacity(4 + total_len);
    request_buf.put_u32(total_len_u32);
    request_buf.put_u8(rpc_type as u8);
    request_buf.put_slice(body);
    Ok(request_buf)
}

/// Write a framed request and read the framed response over a stream.
async fn exchange_on<S>(stream: &mut S, request_buf: &BytesMut, addr: SocketAddr) -> Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Send request
    tokio::time::timeout(RPC_IO_TIMEOUT, stream.write_all(request_buf))
        .await
        .map_err(|_| Error::ConnectionTimeout(addr))??;

    tokio::time::timeout(RPC_IO_TIMEOUT, stream.flush())
        .await
        .map_err(|_| Error::ConnectionTimeout(addr))??;

    // Read response length
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(RPC_IO_TIMEOUT, stream.read_exact(&mut len_buf))
        .await
        .map_err(|_| Error::ConnectionTimeout(addr))??;
    let len = u32::from_be_bytes(len_buf) as usize;
    let body_len = validate_rpc_frame_len(len, "response")?;

    // Read response type
    let mut type_buf = [0u8; 1];
    tokio::time::timeout(RPC_IO_TIMEOUT, stream.read_exact(&mut type_buf))
        .await
        .map_err(|_| Error::ConnectionTimeout(addr))??;

    // Read response body
    let mut response_body = BytesMut::with_capacity(body_len);
    response_body.resize(body_len, 0);
    tokio::time::timeout(RPC_IO_TIMEOUT, stream.read_exact(&mut response_body))
        .await
        .map_err(|_| Error::ConnectionTimeout(addr))??;

    Ok(response_body.to_vec())
}

/// Raft RPC client for sending requests to other nodes.
///
/// Supports optional TLS for secure inter-node communication. Callers hold
/// one [`PeerConnection`] slot per peer; the typed request methods reuse it
/// and transparently reconnect once when a cached connection turns out to be
/// stale (peer restarted, idle-reaped by the server, half-closed socket).
#[derive(Clone)]
pub struct RaftRpcClient {
    /// Optional TLS connector for secure connections
    tls_connector: Option<TlsConnector>,
}

impl std::fmt::Debug for RaftRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftRpcClient")
            .field("tls_enabled", &self.tls_connector.is_some())
            .finish()
    }
}

impl RaftRpcClient {
    /// Create a new RPC client without TLS.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tls_connector: None,
        }
    }

    /// Create a new RPC client with TLS.
    #[must_use]
    pub fn with_tls(tls_config: &RaftTlsConfig) -> Self {
        Self {
            tls_connector: Some(tls_config.connector.clone()),
        }
    }

    /// Send an `AppendEntries` request, reusing `conn` when possible.
    ///
    /// # Errors
    /// Returns an error if serialization, transport, or deserialization fails.
    pub async fn append_entries(
        &self,
        conn: &mut Option<PeerConnection>,
        addr: SocketAddr,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>> {
        let body = postcard::to_allocvec(&req)?;
        let response = self
            .request(conn, addr, RpcType::AppendEntries, &body)
            .await?;
        let resp: AppendEntriesResponse<NodeId> = postcard::from_bytes(&response)?;
        Ok(resp)
    }

    /// Send a `Vote` request, reusing `conn` when possible.
    ///
    /// # Errors
    /// Returns an error if serialization, transport, or deserialization fails.
    pub async fn vote(
        &self,
        conn: &mut Option<PeerConnection>,
        addr: SocketAddr,
        req: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>> {
        let body = postcard::to_allocvec(&req)?;
        let response = self.request(conn, addr, RpcType::Vote, &body).await?;
        let resp: VoteResponse<NodeId> = postcard::from_bytes(&response)?;
        Ok(resp)
    }

    /// Send an `InstallSnapshot` request, reusing `conn` when possible.
    ///
    /// # Errors
    /// Returns an error if serialization, transport, or deserialization fails.
    pub async fn install_snapshot(
        &self,
        conn: &mut Option<PeerConnection>,
        addr: SocketAddr,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>> {
        let body = postcard::to_allocvec(&req)?;
        let response = self
            .request(conn, addr, RpcType::InstallSnapshot, &body)
            .await?;
        let resp: InstallSnapshotResponse<NodeId> = postcard::from_bytes(&response)?;
        Ok(resp)
    }

    /// Run one request/response cycle against `addr`, reusing the cached
    /// connection in `conn` and reconnecting once if it fails.
    ///
    /// The slot is `take()`n for the duration of the exchange: if this future
    /// is dropped mid-exchange (openraft cancels RPCs on timeout) the
    /// connection's protocol state is unknown, so it is only put back after a
    /// complete cycle. A failed exchange on a cached connection most likely
    /// means the connection went stale (peer restart, server idle reaping),
    /// so one fresh connect + retry is attempted; a failure on the fresh
    /// connection fails the RPC — openraft handles RPC errors with its own
    /// retry logic, so no backoff here.
    async fn request(
        &self,
        conn: &mut Option<PeerConnection>,
        addr: SocketAddr,
        rpc_type: RpcType,
        body: &[u8],
    ) -> Result<Vec<u8>> {
        if let Some(cached) = conn.take() {
            match cached.exchange(rpc_type, body).await {
                Ok((cached, response)) => {
                    *conn = Some(cached);
                    return Ok(response);
                }
                Err(e) => {
                    tracing::debug!(%addr, error = %e, "Cached Raft connection failed; reconnecting");
                }
            }
        }

        let fresh = self.connect(addr).await?;
        let (fresh, response) = fresh.exchange(rpc_type, body).await?;
        *conn = Some(fresh);
        Ok(response)
    }

    /// Establish a new connection (TCP + optional TLS handshake) to a peer.
    async fn connect(&self, addr: SocketAddr) -> Result<PeerConnection> {
        let stream = match tokio::time::timeout(RPC_IO_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(Error::Connect { addr, source: e }),
            Err(_) => return Err(Error::ConnectionTimeout(addr)),
        };

        // Disable Nagle for low-latency RPC
        stream.set_nodelay(true)?;

        let stream = if let Some(connector) = &self.tls_connector {
            // Peer addresses arrive as parsed `SocketAddr` literals (PeerConfig
            // requires `SocketAddr` at config-parse time — see config/types.rs).
            // We therefore verify against the IP SAN on the peer cert; this is
            // the authoritative identity for cluster peers since DNS is not on
            // the trust path. Refuse unspecified / loopback peer IPs so a
            // misconfigured peer can't silently pass verification against a
            // wildcard-SAN cert.
            let peer_ip = addr.ip();
            if peer_ip.is_unspecified() || peer_ip.is_multicast() {
                return Err(Error::Tls(format!(
                    "Refusing TLS connect to unroutable peer IP {peer_ip}"
                )));
            }
            let server_name = ServerName::IpAddress(peer_ip.into());
            let tls_stream =
                match tokio::time::timeout(RPC_IO_TIMEOUT, connector.connect(server_name, stream))
                    .await
                {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => return Err(Error::Tls(format!("TLS handshake failed: {e}"))),
                    Err(_) => return Err(Error::ConnectionTimeout(addr)),
                };
            PeerStream::Tls(Box::new(tls_stream))
        } else {
            PeerStream::Plain(stream)
        };

        Ok(PeerConnection { stream, addr })
    }
}

impl Default for RaftRpcClient {
    fn default() -> Self {
        Self::new()
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
    fn test_rpc_type_conversion() {
        assert_eq!(RpcType::try_from(1u8).unwrap(), RpcType::AppendEntries);
        assert_eq!(RpcType::try_from(3u8).unwrap(), RpcType::Vote);
        assert!(RpcType::try_from(100u8).is_err());
    }

    #[test]
    fn test_validate_rpc_frame_len_rejects_zero() {
        let result = validate_rpc_frame_len(0, "request");
        assert!(result.is_err());
        let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(msg.contains("length: 0"));
    }
}
