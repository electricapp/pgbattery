//! Raft RPC network transport.
//!
//! Provides TCP-based RPC transport for Raft consensus messages
//! including `AppendEntries`, `Vote`, and `InstallSnapshot`.
//!
//! Supports optional TLS for secure inter-node communication.
//!
//! # Wire format and cancellation safety
//!
//! Every frame is `len(4, big-endian) + type(1) + corr_id(8, big-endian) +
//! body`, where `len` counts everything after itself. A response echoes its
//! request's `corr_id`.
//!
//! openraft cancels an RPC by dropping its future, and a drop can land at
//! any await point — after half a frame was written, or with half a response
//! read. The client therefore keeps per-connection staging buffers that
//! survive cancellation (`FramedIo`): a torn outbound frame is completed
//! before the next request is sent (the peer never sees a byte-desynced
//! stream), and a partially-read inbound frame resumes exactly where it
//! stopped. The correlation ID then makes response matching structural: a
//! straggler response to an abandoned request is skipped by ID, an ID from
//! the future or a type mismatch is a protocol violation that drops the
//! connection. Desync is impossible by construction, not by the discipline
//! of remembering to drop connections after every cancellation — and a
//! cancelled RPC no longer costs a TCP+TLS reconnect.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
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

/// Bytes of a frame counted by its length prefix besides the body:
/// type (1) + correlation ID (8).
const FRAME_OVERHEAD_AFTER_LEN: usize = 1 + 8;

fn validate_rpc_frame_len(len: usize, context: &str) -> Result<usize> {
    if len < FRAME_OVERHEAD_AFTER_LEN {
        return Err(Error::Protocol(format!(
            "Invalid RPC {context} frame length: {len} (minimum {FRAME_OVERHEAD_AFTER_LEN})"
        )));
    }
    if len > MAX_RPC_FRAME_LEN {
        return Err(Error::Protocol(format!(
            "RPC {context} frame too large: {len}"
        )));
    }
    Ok(len - FRAME_OVERHEAD_AFTER_LEN)
}

/// Write a frame header (length placeholder, type byte, correlation ID);
/// returns the length field's offset for [`finish_frame`].
///
/// Together with `finish_frame` this is the wire format itself, written
/// once: the byte-body path (`append_frame_bytes`, which the fuzz seam also
/// exercises) and the serializing path (`append_frame_serialize`) both go
/// through here, so the round-trip property the fuzzer checks stays a
/// property of the *shipping* encoder.
fn start_frame(buf: &mut BytesMut, type_byte: u8, corr_id: u64) -> usize {
    let len_pos = buf.len();
    buf.put_u32(0);
    buf.put_u8(type_byte);
    buf.put_u64(corr_id);
    len_pos
}

/// Backfill the length prefix written by [`start_frame`]. On error the
/// frame is truncated away, leaving earlier staged bytes intact.
fn finish_frame(buf: &mut BytesMut, len_pos: usize) -> Result<()> {
    let total_len = buf.len().saturating_sub(len_pos + 4);
    let Ok(total_len_u32) = u32::try_from(total_len) else {
        buf.truncate(len_pos);
        return Err(Error::Protocol("RPC frame length exceeds u32".to_string()));
    };
    let Some(len_slot) = buf.get_mut(len_pos..len_pos + 4) else {
        buf.truncate(len_pos);
        return Err(Error::Protocol("RPC frame header missing".to_string()));
    };
    len_slot.copy_from_slice(&total_len_u32.to_be_bytes());
    Ok(())
}

fn append_frame_bytes(buf: &mut BytesMut, type_byte: u8, corr_id: u64, body: &[u8]) -> Result<()> {
    let len_pos = start_frame(buf, type_byte, corr_id);
    buf.put_slice(body);
    finish_frame(buf, len_pos)
}

/// Append one framed message, serializing `msg` directly into `buf` after
/// the header — no intermediate `Vec` per RPC. An encode error may lose
/// bytes staged before this frame; every caller treats it as
/// connection-fatal, so nothing is resumed from the buffer afterwards.
fn append_frame_serialize<T>(
    buf: &mut BytesMut,
    rpc_type: RpcType,
    corr_id: u64,
    msg: &T,
) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let len_pos = start_frame(buf, rpc_type as u8, corr_id);
    let staged = std::mem::take(buf);
    *buf = postcard::to_extend(msg, staged)?;
    finish_frame(buf, len_pos)
}

/// Capacity retained by a staging buffer once it is empty. A 4 MB
/// `InstallSnapshot` must not pin its high-water allocation for the
/// connection's remaining life.
const FRAME_BUF_RETAIN: usize = 64 * 1024;

/// Release an oversized staging buffer's capacity once drained; a buffer
/// still holding bytes (a torn frame) is left untouched.
fn shrink_frame_buf(buf: &mut BytesMut) {
    if buf.is_empty() && buf.capacity() > FRAME_BUF_RETAIN {
        *buf = BytesMut::with_capacity(4096);
    }
}

/// One parsed inbound frame.
struct Frame {
    type_byte: u8,
    corr_id: u64,
    body: BytesMut,
}

/// Fuzz/test seam over [`take_frame`] and [`append_frame_bytes`].
///
/// The frame codec only otherwise reachable through a live socket plus an
/// `openraft` dependency. `Ok(None)` must leave `buf` byte-identical (the
/// torn-write resume property), and a returned frame must re-encode to exactly
/// the bytes consumed.
#[doc(hidden)]
pub fn decode_rpc_frame_for_fuzz(buf: &mut BytesMut) -> Result<Option<(u8, u64, Vec<u8>)>> {
    Ok(take_frame(buf, "fuzz")?.map(|f| (f.type_byte, f.corr_id, f.body.to_vec())))
}

/// Re-encode a frame decoded by [`decode_rpc_frame_for_fuzz`].
///
/// Delegates to the same [`append_frame_bytes`] the transport writes through.
/// A separate copy of the four writes would make the fuzzer's round-trip
/// property self-referential: it would keep passing against a stale encoder
/// while the shipping one drifted.
#[doc(hidden)]
pub fn encode_rpc_frame_for_fuzz(
    buf: &mut BytesMut,
    type_byte: u8,
    corr_id: u64,
    body: &[u8],
) -> Result<()> {
    append_frame_bytes(buf, type_byte, corr_id, body)
}

/// Frame-length bounds, exposed for assertions in fuzz targets.
#[doc(hidden)]
pub const RPC_FRAME_LEN_BOUNDS: (usize, usize) = (FRAME_OVERHEAD_AFTER_LEN, MAX_RPC_FRAME_LEN);

/// Split one complete frame off the front of `buf`, if present.
///
/// `Ok(None)` means more bytes are needed; the partial frame stays in `buf`
/// untouched, which is what lets a cancelled read resume losslessly. A
/// length prefix outside the valid range is a protocol violation.
fn take_frame(buf: &mut BytesMut, context: &str) -> Result<Option<Frame>> {
    let Some(len_bytes) = buf.get(..4) else {
        return Ok(None);
    };
    let len = u32::from_be_bytes(<[u8; 4]>::try_from(len_bytes).unwrap_or_default()) as usize;
    let body_len = validate_rpc_frame_len(len, context)?;
    let total = 4 + len;
    if buf.len() < total {
        return Ok(None);
    }
    let mut frame = buf.split_to(total);
    frame.advance(4);
    let type_byte = frame.get_u8();
    let corr_id = frame.get_u64();
    debug_assert_eq!(frame.len(), body_len);
    Ok(Some(Frame {
        type_byte,
        corr_id,
        body: frame,
    }))
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
        /// Cap on concurrently-served RPC connections. A healthy cluster needs a
        /// handful (one persistent connection per peer); this is far above that
        /// but bounds an attacker/misbehaving peer that opens many connections,
        /// each pinning a task and buffering up to `MAX_RPC_FRAME_LEN` per
        /// in-flight request. Excess connections are dropped (peers reconnect).
        const MAX_CONCURRENT_RPC_CONNECTIONS: usize = 256;
        let conn_limiter = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_RPC_CONNECTIONS));

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
                            let Ok(permit) = Arc::clone(&conn_limiter).try_acquire_owned() else {
                                tracing::warn!(
                                    %peer_addr,
                                    cap = MAX_CONCURRENT_RPC_CONNECTIONS,
                                    "RPC connection cap reached; dropping connection"
                                );
                                metrics::counter!("pgbattery_rpc_connections_dropped").increment(1);
                                drop(stream);
                                continue;
                            };
                            let raft = self.raft.clone();
                            let cluster_state = self.cluster_state.clone();
                            let tls_config = self.tls_config.clone();

                            tokio::spawn(async move {
                                // Held for the connection's lifetime, freeing the
                                // slot when this task ends.
                                let _permit = permit;
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
    // Per-connection staging, retained across requests: inbound frames are
    // split off `read_buf` by the same `take_frame` the client side decodes
    // with, and responses serialize into `response_buf`.
    let mut read_buf = BytesMut::with_capacity(4096);
    let mut response_buf = BytesMut::with_capacity(4096);
    loop {
        response_buf.clear();
        shrink_frame_buf(&mut read_buf);
        shrink_frame_buf(&mut response_buf);

        let frame = loop {
            if let Some(frame) = take_frame(&mut read_buf, "request")? {
                break frame;
            }
            // Waiting between requests is idle time; once any bytes of a
            // frame are staged, each read gets the per-op budget instead so
            // a half-open peer cannot pin this task.
            let idle = read_buf.is_empty();
            let read_timeout = if idle {
                SERVER_IDLE_TIMEOUT
            } else {
                RPC_IO_TIMEOUT
            };
            // Once the length prefix is in, reserve the whole frame — one
            // growth step even at InstallSnapshot sizes. `read_buf` appends
            // into uninitialized capacity, so nothing is zero-filled.
            if let Some(len_bytes) = read_buf.get(..4) {
                let len = u32::from_be_bytes(<[u8; 4]>::try_from(len_bytes).unwrap_or_default());
                read_buf.reserve((4 + len as usize).saturating_sub(read_buf.len()));
            } else {
                read_buf.reserve(4096);
            }
            match tokio::time::timeout(read_timeout, stream.read_buf(&mut read_buf)).await {
                Ok(Ok(0)) => {
                    if read_buf.is_empty() {
                        tracing::trace!(%peer_addr, "Peer closed RPC connection");
                        return Ok(());
                    }
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) if idle => {
                    tracing::debug!(%peer_addr, "Idle RPC connection reaped");
                    return Ok(());
                }
                Err(_) => return Err(Error::ConnectionTimeout(peer_addr)),
            }
        };
        let rpc_type = RpcType::try_from(frame.type_byte)?;

        // The response echoes the request's correlation ID so the client can
        // match it — including skipping responses to abandoned requests.
        process_request(
            rpc_type,
            &frame.body,
            frame.corr_id,
            peer_addr,
            &raft,
            &cluster_state,
            &mut response_buf,
        )
        .await?;

        tokio::time::timeout(RPC_IO_TIMEOUT, stream.write_all(&response_buf))
            .await
            .map_err(|_| Error::ConnectionTimeout(peer_addr))??;
        tokio::time::timeout(RPC_IO_TIMEOUT, stream.flush())
            .await
            .map_err(|_| Error::ConnectionTimeout(peer_addr))??;
    }
}

/// Dispatch a single decoded RPC request to the local Raft instance,
/// framing the response (echoing `corr_id`) into `response_buf`.
async fn process_request(
    rpc_type: RpcType,
    body: &[u8],
    corr_id: u64,
    peer_addr: SocketAddr,
    raft: &openraft::Raft<TypeConfig>,
    cluster_state: &RwLock<ClusterState>,
    response_buf: &mut BytesMut,
) -> Result<()> {
    match rpc_type {
        RpcType::AppendEntries => {
            let req: AppendEntriesRequest<TypeConfig> = postcard::from_bytes(body)?;
            tracing::trace!(%peer_addr, "Received AppendEntries");
            let resp = raft
                .append_entries(req)
                .await
                .map_err(|e| Error::Raft(e.to_string()))?;
            append_frame_serialize(response_buf, RpcType::AppendEntriesResponse, corr_id, &resp)
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
                append_frame_serialize(response_buf, RpcType::VoteResponse, corr_id, &resp)
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
                //
                // Caveat: `RaftMetrics` is an eventually-consistent projection
                // that can lag our durably-persisted vote by a core-loop tick,
                // so the term echoed here may be slightly stale. That only
                // prolongs an election round — a rejection never binds the
                // voter and this path can never *grant* a vote — so it is a
                // liveness footnote, not a safety hazard.
                let vote = raft.metrics().borrow().vote;
                let resp = VoteResponse::<NodeId> {
                    vote,
                    vote_granted: false,
                    last_log_id: None,
                };
                append_frame_serialize(response_buf, RpcType::VoteResponse, corr_id, &resp)
            }
        }
        RpcType::InstallSnapshot => {
            let req: InstallSnapshotRequest<TypeConfig> = postcard::from_bytes(body)?;
            tracing::debug!(%peer_addr, "Received InstallSnapshot");
            let resp = raft
                .install_snapshot(req)
                .await
                .map_err(|e| Error::Raft(e.to_string()))?;
            append_frame_serialize(
                response_buf,
                RpcType::InstallSnapshotResponse,
                corr_id,
                &resp,
            )
        }
        _ => Err(Error::Protocol(format!(
            "Unexpected request type: {rpc_type:?}"
        ))),
    }
}

/// A persistent framed connection to a Raft peer.
///
/// Owned by the per-peer `NetworkConnection` so that heartbeats and log
/// replication reuse one TCP (and TLS) session instead of paying
/// connect + handshake per RPC. The connection survives a cancelled RPC:
/// `io` stages torn frames across drops (see the module docs) and
/// `next_corr_id` keeps request/response matching structural, so the slot
/// holding this connection is only cleared on a real error.
#[derive(Debug)]
pub struct PeerConnection {
    stream: PeerStream,
    addr: SocketAddr,
    /// Correlation ID assigned to the next request. Monotonic per
    /// connection; any inbound frame with a lower ID is a straggler response
    /// to an abandoned (cancelled) request and is skipped.
    next_corr_id: u64,
    io: FramedIo,
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
    /// Run one framed request/response cycle in place.
    ///
    /// Cancellation-safe by design: if this future is dropped at any await
    /// point, the staging buffers in `self.io` hold whatever was in flight
    /// (an unwritten frame tail, a partially-read response) and the next
    /// call resumes losslessly. An `Err` means the connection is genuinely
    /// broken (I/O error, EOF, no progress within `RPC_IO_TIMEOUT`, or a
    /// protocol violation) and must be discarded by the caller.
    async fn exchange<T>(&mut self, rpc_type: RpcType, msg: &T) -> Result<BytesMut>
    where
        T: Serialize + Sync + ?Sized,
    {
        // Each request type has exactly one valid response type; validating it
        // catches a desynced stream before postcard decodes the wrong frame.
        let expected = match rpc_type {
            RpcType::AppendEntries => RpcType::AppendEntriesResponse,
            RpcType::Vote => RpcType::VoteResponse,
            RpcType::InstallSnapshot => RpcType::InstallSnapshotResponse,
            other => {
                return Err(Error::Protocol(format!(
                    "not a request RPC type: {other:?}"
                )));
            }
        };
        let corr_id = self.next_corr_id;
        self.next_corr_id += 1;
        let addr = self.addr;
        match &mut self.stream {
            PeerStream::Plain(stream) => {
                self.io
                    .exchange_frames(stream, rpc_type, corr_id, msg, expected, addr)
                    .await
            }
            PeerStream::Tls(stream) => {
                self.io
                    .exchange_frames(stream.as_mut(), rpc_type, corr_id, msg, expected, addr)
                    .await
            }
        }
    }
}

/// Per-connection staging buffers that make a framed request/response
/// exchange safe to cancel at any await point.
///
/// `write_buf` holds bytes not yet accepted by the socket: `AsyncWriteExt::
/// write` is cancel-safe (a cancelled write wrote nothing), so after a drop
/// the buffer is exactly the unsent remainder, and the next exchange
/// finishes the torn frame before its own — the peer never observes a
/// byte-desynced stream. `read_buf` accumulates inbound bytes via the
/// equally cancel-safe `read_buf`; a partial frame simply stays staged until
/// the next exchange resumes reading.
#[derive(Debug, Default)]
struct FramedIo {
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl FramedIo {
    /// Stragglers a single exchange will skip before declaring the peer
    /// broken. Each abandoned request leaves at most one response, so a
    /// healthy connection can never accumulate more stale frames than it had
    /// cancelled RPCs since the last completed one; a peer spraying frames
    /// past this bound is misbehaving.
    const MAX_STALE_FRAMES_PER_EXCHANGE: usize = 64;

    async fn exchange_frames<S, T>(
        &mut self,
        stream: &mut S,
        rpc_type: RpcType,
        corr_id: u64,
        msg: &T,
        expected: RpcType,
        addr: SocketAddr,
    ) -> Result<BytesMut>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: Serialize + Sync + ?Sized,
    {
        // Release capacity a large InstallSnapshot left behind; a staged
        // torn frame keeps its buffer untouched.
        shrink_frame_buf(&mut self.write_buf);
        shrink_frame_buf(&mut self.read_buf);
        // Stage this request after any torn predecessor, then drain. The
        // timeout bounds total progress; on expiry the connection is dead to
        // the caller, so partial state doesn't matter.
        append_frame_serialize(&mut self.write_buf, rpc_type, corr_id, msg)?;
        tokio::time::timeout(RPC_IO_TIMEOUT, async {
            while !self.write_buf.is_empty() {
                let n = stream.write(&self.write_buf).await?;
                if n == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
                }
                self.write_buf.advance(n);
            }
            stream.flush().await
        })
        .await
        .map_err(|_| Error::ConnectionTimeout(addr))??;

        // Read until this request's response appears. Frames with a lower
        // correlation ID are responses to abandoned requests — skipped. A
        // higher ID, an unknown type byte, or a type that isn't this
        // request's response type means the stream cannot be trusted.
        let mut skipped = 0_usize;
        tokio::time::timeout(RPC_IO_TIMEOUT, async {
            loop {
                while let Some(frame) = take_frame(&mut self.read_buf, "response")? {
                    if frame.corr_id < corr_id {
                        skipped += 1;
                        if skipped > Self::MAX_STALE_FRAMES_PER_EXCHANGE {
                            return Err(Error::Protocol(format!(
                                "RPC stream from {addr} produced more than \
                                 {} stale frames in one exchange",
                                Self::MAX_STALE_FRAMES_PER_EXCHANGE
                            )));
                        }
                        tracing::debug!(
                            %addr,
                            stale_corr_id = frame.corr_id,
                            corr_id,
                            "Skipping straggler response to a cancelled RPC"
                        );
                        continue;
                    }
                    if frame.corr_id > corr_id {
                        return Err(Error::Protocol(format!(
                            "RPC correlation mismatch from {addr}: expected {corr_id}, \
                             got {} (from the future)",
                            frame.corr_id
                        )));
                    }
                    // postcard is positional/untagged, so a wrong-type frame
                    // could decode as a structurally-valid-but-wrong message
                    // (e.g. a fabricated VoteResponse) — a safety hazard on
                    // the election path. Correlation plus this check make
                    // that impossible.
                    let resp_type = RpcType::try_from(frame.type_byte)?;
                    if resp_type != expected {
                        return Err(Error::Protocol(format!(
                            "RPC response type mismatch from {addr}: \
                             expected {expected:?}, got {resp_type:?}"
                        )));
                    }
                    return Ok(frame.body);
                }
                // Need more bytes. Reserve so `read_buf` has room to append
                // (it reads nothing into a full buffer); the frame-length
                // validation in `take_frame` bounds how large the staged
                // buffer can be asked to grow.
                self.read_buf.reserve(4096);
                let n = stream.read_buf(&mut self.read_buf).await?;
                if n == 0 {
                    return Err(Error::Protocol(format!(
                        "RPC connection to {addr} closed mid-response"
                    )));
                }
            }
        })
        .await
        .map_err(|_| Error::ConnectionTimeout(addr))?
    }
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
        let response = self
            .request(conn, addr, RpcType::AppendEntries, &req)
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
        let response = self.request(conn, addr, RpcType::Vote, &req).await?;
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
        let response = self
            .request(conn, addr, RpcType::InstallSnapshot, &req)
            .await?;
        let resp: InstallSnapshotResponse<NodeId> = postcard::from_bytes(&response)?;
        Ok(resp)
    }

    /// Run one request/response cycle against `addr`, reusing the cached
    /// connection in `conn` and reconnecting once if it fails.
    ///
    /// The connection stays in the slot for the whole exchange: `exchange`
    /// is cancellation-safe (torn frames are staged in the connection and
    /// resumed; correlation IDs make straggler responses skippable), so a
    /// future dropped mid-exchange — openraft cancels RPCs on timeout —
    /// retains the connection for the next attempt instead of paying a
    /// TCP+TLS reconnect. Only an `Err` from `exchange` (I/O failure, EOF,
    /// stalled progress, protocol violation) clears the slot; that most
    /// likely means the connection went stale (peer restart, server idle
    /// reaping), so one fresh connect + retry is attempted. A failure on the
    /// fresh connection fails the RPC — openraft handles RPC errors with its
    /// own retry logic, so no backoff here.
    async fn request<T>(
        &self,
        conn: &mut Option<PeerConnection>,
        addr: SocketAddr,
        rpc_type: RpcType,
        msg: &T,
    ) -> Result<BytesMut>
    where
        T: Serialize + Sync + ?Sized,
    {
        if let Some(cached) = conn.as_mut() {
            match cached.exchange(rpc_type, msg).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    *conn = None;
                    tracing::debug!(%addr, error = %e, "Cached Raft connection failed; reconnecting");
                }
            }
        }

        // Slot-first so a cancellation mid-exchange keeps the fresh
        // connection too.
        *conn = Some(self.connect(addr).await?);
        let Some(fresh) = conn.as_mut() else {
            return Err(Error::Protocol(
                "connection slot unexpectedly empty".to_string(),
            ));
        };
        match fresh.exchange(rpc_type, msg).await {
            Ok(response) => Ok(response),
            Err(e) => {
                *conn = None;
                Err(e)
            }
        }
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

        Ok(PeerConnection {
            stream,
            addr,
            next_corr_id: 0,
            io: FramedIo::default(),
        })
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
    fn test_validate_rpc_frame_len_bounds() {
        // Below the fixed overhead (type + correlation ID) is malformed.
        assert!(validate_rpc_frame_len(0, "request").is_err());
        assert!(validate_rpc_frame_len(FRAME_OVERHEAD_AFTER_LEN - 1, "request").is_err());
        // Overhead-only = empty body, valid.
        assert_eq!(
            validate_rpc_frame_len(FRAME_OVERHEAD_AFTER_LEN, "request").unwrap(),
            0
        );
        assert!(validate_rpc_frame_len(MAX_RPC_FRAME_LEN, "request").is_ok());
        assert!(validate_rpc_frame_len(MAX_RPC_FRAME_LEN + 1, "request").is_err());
    }

    /// Test-side framing over the shipping [`append_frame_bytes`].
    fn append_frame(
        buf: &mut BytesMut,
        rpc_type: RpcType,
        corr_id: u64,
        body: &[u8],
    ) -> Result<()> {
        append_frame_bytes(buf, rpc_type as u8, corr_id, body)
    }

    /// Serializes as its raw bytes with no length prefix (postcard writes
    /// tuples unprefixed), so peer-side test readers see the body verbatim.
    struct RawBody<'a>(&'a [u8]);

    impl Serialize for RawBody<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            use serde::ser::SerializeTuple as _;
            let mut tuple = serializer.serialize_tuple(self.0.len())?;
            for b in self.0 {
                tuple.serialize_element(b)?;
            }
            tuple.end()
        }
    }

    #[derive(Serialize)]
    struct SampleMsg {
        term: u64,
        entries: Vec<u32>,
        tag: String,
    }

    fn sample_msg() -> SampleMsg {
        SampleMsg {
            term: 7,
            entries: vec![1, 2, 3],
            tag: "x".to_string(),
        }
    }

    /// The serializing encoder and the byte-body encoder must emit identical
    /// frames — the fuzz round-trip property covers the latter.
    #[test]
    fn append_frame_serialize_matches_byte_encoding() {
        let msg = sample_msg();
        let mut via_vec = BytesMut::new();
        append_frame(
            &mut via_vec,
            RpcType::Vote,
            9,
            &postcard::to_allocvec(&msg).unwrap(),
        )
        .unwrap();
        let mut direct = BytesMut::new();
        append_frame_serialize(&mut direct, RpcType::Vote, 9, &msg).unwrap();
        assert_eq!(via_vec, direct);
    }

    #[test]
    fn append_frame_serialize_reuses_a_warm_buffer() {
        let mut buf = BytesMut::with_capacity(4096);
        let msg = sample_msg();
        append_frame_serialize(&mut buf, RpcType::Vote, 1, &msg).unwrap();
        buf.clear();
        let (result, stats) = crate::alloc_meter::measure(|| {
            append_frame_serialize(&mut buf, RpcType::Vote, 2, &msg)
        });
        result.unwrap();
        assert_eq!(
            stats.count, 0,
            "warm-buffer encode must not allocate: {stats:?}"
        );
    }

    #[test]
    fn shrink_frame_buf_releases_only_oversized_empty_buffers() {
        let mut oversized = BytesMut::with_capacity(FRAME_BUF_RETAIN * 2);
        shrink_frame_buf(&mut oversized);
        assert!(oversized.capacity() <= FRAME_BUF_RETAIN);

        let mut torn = BytesMut::with_capacity(FRAME_BUF_RETAIN * 2);
        torn.put_u8(1);
        shrink_frame_buf(&mut torn);
        assert!(
            torn.capacity() > FRAME_BUF_RETAIN,
            "staged bytes pin the buffer"
        );

        let mut small = BytesMut::with_capacity(64);
        shrink_frame_buf(&mut small);
        assert!(small.capacity() <= FRAME_BUF_RETAIN);
    }

    const TEST_ADDR: &str = "127.0.0.1:9";

    fn addr() -> SocketAddr {
        TEST_ADDR.parse().unwrap()
    }

    /// Peer-side helper: read one full frame with plain `read_exact` (tests
    /// control cancellation explicitly, so the simple reader is fine here).
    async fn read_frame_from<S: AsyncRead + Unpin>(stream: &mut S) -> (u8, u64, Vec<u8>) {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut rest = vec![0u8; len];
        stream.read_exact(&mut rest).await.unwrap();
        let type_byte = rest[0];
        let corr_id = u64::from_be_bytes(<[u8; 8]>::try_from(&rest[1..9]).unwrap());
        (type_byte, corr_id, rest[9..].to_vec())
    }

    async fn write_frame_to<S: AsyncWrite + Unpin>(
        stream: &mut S,
        rpc_type: RpcType,
        corr_id: u64,
        body: &[u8],
    ) {
        let mut buf = BytesMut::new();
        append_frame(&mut buf, rpc_type, corr_id, body).unwrap();
        stream.write_all(&buf).await.unwrap();
    }

    #[tokio::test]
    async fn exchange_round_trips_and_echoes_correlation() {
        let (mut ours, mut theirs) = tokio::io::duplex(64 * 1024);
        let mut io = FramedIo::default();

        let peer = tokio::spawn(async move {
            let (type_byte, corr_id, body) = read_frame_from(&mut theirs).await;
            assert_eq!(type_byte, RpcType::Vote as u8);
            assert_eq!(corr_id, 5);
            assert_eq!(body, b"ballot");
            write_frame_to(&mut theirs, RpcType::VoteResponse, corr_id, b"granted").await;
            theirs
        });

        let response = io
            .exchange_frames(
                &mut ours,
                RpcType::Vote,
                5,
                b"ballot",
                RpcType::VoteResponse,
                addr(),
            )
            .await
            .unwrap();
        assert_eq!(&response[..], b"granted");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn straggler_responses_are_skipped_by_correlation() {
        let (mut ours, mut theirs) = tokio::io::duplex(64 * 1024);
        let mut io = FramedIo::default();

        // Responses to two abandoned requests sit in the stream ahead of the
        // one this exchange is waiting for.
        write_frame_to(&mut theirs, RpcType::VoteResponse, 0, b"stale-0").await;
        write_frame_to(&mut theirs, RpcType::VoteResponse, 1, b"stale-1").await;
        write_frame_to(&mut theirs, RpcType::VoteResponse, 2, b"fresh").await;

        let response = io
            .exchange_frames(
                &mut ours,
                RpcType::Vote,
                2,
                b"req",
                RpcType::VoteResponse,
                addr(),
            )
            .await
            .unwrap();
        assert_eq!(&response[..], b"fresh");
    }

    #[tokio::test]
    async fn cancelled_read_resumes_mid_frame_and_skips_the_straggler() {
        let (mut ours, mut theirs) = tokio::io::duplex(64 * 1024);
        let mut io = FramedIo::default();

        // The peer delivers only HALF of the response to request 0, then the
        // exchange is cancelled (external timeout, as openraft does). The
        // half frame must stay staged.
        let mut full = BytesMut::new();
        append_frame(&mut full, RpcType::VoteResponse, 0, b"abandoned-response").unwrap();
        let split_at = full.len() / 2;
        theirs.write_all(&full[..split_at]).await.unwrap();

        let cancelled = tokio::time::timeout(
            Duration::from_millis(50),
            io.exchange_frames(
                &mut ours,
                RpcType::Vote,
                0,
                b"req-0",
                RpcType::VoteResponse,
                addr(),
            ),
        )
        .await;
        assert!(cancelled.is_err(), "exchange must have been cancelled");
        assert!(!io.read_buf.is_empty(), "partial frame must stay staged");

        // The rest of the abandoned response arrives, followed by the next
        // request's response. The resumed reader must reassemble the torn
        // frame, skip it by correlation ID, and return the fresh one.
        theirs.write_all(&full[split_at..]).await.unwrap();
        write_frame_to(&mut theirs, RpcType::VoteResponse, 1, b"fresh").await;

        let response = io
            .exchange_frames(
                &mut ours,
                RpcType::Vote,
                1,
                b"req-1",
                RpcType::VoteResponse,
                addr(),
            )
            .await
            .unwrap();
        assert_eq!(&response[..], b"fresh");
        assert!(io.read_buf.is_empty());
    }

    #[tokio::test]
    async fn torn_write_is_completed_before_the_next_request() {
        // A duplex buffer far smaller than the request forces the write to
        // stall mid-frame; the external cancellation then leaves a torn
        // outbound frame staged in `write_buf`.
        let (mut ours, mut theirs) = tokio::io::duplex(64);
        let mut io = FramedIo::default();
        let big_body = [0xAB_u8; 1024];

        let cancelled = tokio::time::timeout(
            Duration::from_millis(50),
            io.exchange_frames(
                &mut ours,
                RpcType::AppendEntries,
                0,
                &RawBody(&big_body),
                RpcType::AppendEntriesResponse,
                addr(),
            ),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "write must have stalled and been cancelled"
        );
        assert!(!io.write_buf.is_empty(), "torn frame must stay staged");

        // The peer now reads: it must receive request 0 INTACT (the resumed
        // writer finishes the torn frame first) and then request 1, and it
        // answers both in order.
        let peer = tokio::spawn(async move {
            let (t0, c0, b0) = read_frame_from(&mut theirs).await;
            assert_eq!(t0, RpcType::AppendEntries as u8);
            assert_eq!(c0, 0);
            assert_eq!(b0, vec![0xAB_u8; 1024]);
            write_frame_to(&mut theirs, RpcType::AppendEntriesResponse, 0, b"resp-0").await;

            let (t1, c1, b1) = read_frame_from(&mut theirs).await;
            assert_eq!(t1, RpcType::AppendEntries as u8);
            assert_eq!(c1, 1);
            assert_eq!(b1, b"next");
            write_frame_to(&mut theirs, RpcType::AppendEntriesResponse, 1, b"resp-1").await;
            theirs
        });

        let response = io
            .exchange_frames(
                &mut ours,
                RpcType::AppendEntries,
                1,
                b"next",
                RpcType::AppendEntriesResponse,
                addr(),
            )
            .await
            .unwrap();
        assert_eq!(&response[..], b"resp-1");
        assert!(io.write_buf.is_empty());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn correlation_from_the_future_is_a_protocol_violation() {
        let (mut ours, mut theirs) = tokio::io::duplex(64 * 1024);
        let mut io = FramedIo::default();
        write_frame_to(&mut theirs, RpcType::VoteResponse, 7, b"who?").await;

        let err = io
            .exchange_frames(
                &mut ours,
                RpcType::Vote,
                1,
                b"req",
                RpcType::VoteResponse,
                addr(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("correlation mismatch"));
    }

    #[tokio::test]
    async fn response_type_mismatch_is_a_protocol_violation() {
        let (mut ours, mut theirs) = tokio::io::duplex(64 * 1024);
        let mut io = FramedIo::default();
        // Correct correlation ID, wrong frame type: without the type check a
        // positional postcard decode could accept this as a valid VoteResponse.
        write_frame_to(&mut theirs, RpcType::AppendEntriesResponse, 1, b"wrong").await;

        let err = io
            .exchange_frames(
                &mut ours,
                RpcType::Vote,
                1,
                b"req",
                RpcType::VoteResponse,
                addr(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("type mismatch"));
    }
}
