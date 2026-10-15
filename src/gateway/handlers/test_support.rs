//! Handler builders and wire-frame helpers shared by the handler test modules.

use bytes::{BufMut, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::config::UNKNOWN_SOCKET_ADDR;
use crate::gateway::connection::{ConnectionRegistry, SharedConnectionState};
use crate::gateway::proxy::GatewayConfig;
use crate::gateway::ssl::MaybeTlsStream;
use crate::governor::raft::FenceState;

use super::ConnectionHandler;

/// Build a `ConnectionHandler` over a connected socket pair, returning
/// the peer end so a test can read exactly what the handler forwarded to
/// its client. Tests that never touch the socket just drop the peer.
pub(super) async fn test_handler_with_peer(
    state: SharedConnectionState,
) -> (ConnectionHandler, TcpStream) {
    let (_leader_tx, leader_rx) = watch::channel(None::<SocketAddr>);
    let (_fence_tx, fence_rx) = watch::channel(FenceState::unfenced());
    test_handler_with_watches(state, leader_rx, fence_rx).await
}

/// As [`test_handler_with_peer`], on watches the test drives.
pub(super) async fn test_handler_with_watches(
    state: SharedConnectionState,
    leader_rx: watch::Receiver<Option<SocketAddr>>,
    fence_rx: watch::Receiver<FenceState>,
) -> (ConnectionHandler, TcpStream) {
    use crate::config::SslModeConfig;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    let client = client.unwrap();
    let (peer, _) = accepted.unwrap();
    let config = GatewayConfig {
        listen_addr: UNKNOWN_SOCKET_ADDR,
        ssl_mode: SslModeConfig::Disable,
        ssl_cert_path: None,
        ssl_key_path: None,
        connection_timeout_ms: 5_000,
        idle_timeout_ms: 300_000,
        upgrade_hold_timeout_ms: 60_000,
        max_connections: 5_000,
    };
    let handler = ConnectionHandler::new(
        0,
        MaybeTlsStream::Plain(client),
        state,
        leader_rx,
        fence_rx,
        Arc::new(config),
        Arc::new(ConnectionRegistry::new()),
        crate::governor::new_shared_lease(),
        None,
    );
    (handler, peer)
}

/// Build a `ConnectionHandler` over a throwaway connected socket. The
/// session-analysis paths under test never touch the socket; it only has
/// to exist so the handler can be constructed.
pub(super) async fn test_handler(state: SharedConnectionState) -> ConnectionHandler {
    test_handler_with_peer(state).await.0
}

/// Encode a Parse ('P') message. `parse_names` ignores the length field, so
/// a zero placeholder is fine.
pub(super) fn parse_message(stmt_name: &str, query: &str) -> BytesMut {
    let mut m = BytesMut::new();
    m.put_u8(b'P');
    m.put_u32(0);
    m.extend_from_slice(stmt_name.as_bytes());
    m.put_u8(0);
    m.extend_from_slice(query.as_bytes());
    m.put_u8(0);
    m.put_u16(0); // parameter type count
    m
}

/// A handler with a live backend socket, for driving whole client and
/// backend batches through the proxy paths. Returns the handler, its end
/// of the backend socket, and the client and backend peers, which must
/// stay open while the test writes through them.
pub(super) async fn proxied(
    state: SharedConnectionState,
) -> (ConnectionHandler, TcpStream, TcpStream, TcpStream) {
    let (handler, client_peer) = test_handler_with_peer(state).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (backend, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    (handler, backend.unwrap(), client_peer, accepted.unwrap().0)
}

pub(super) fn message(kind: u8, body: &[u8]) -> BytesMut {
    let mut m = BytesMut::new();
    m.put_u8(kind);
    m.put_u32(u32::try_from(4 + body.len()).unwrap());
    m.extend_from_slice(body);
    m
}

pub(super) fn framed_parse(stmt_name: &str, query: &str) -> BytesMut {
    let mut body = Vec::new();
    body.extend_from_slice(stmt_name.as_bytes());
    body.push(0);
    body.extend_from_slice(query.as_bytes());
    body.extend_from_slice(&[0, 0, 0]);
    message(b'P', &body)
}

pub(super) fn framed_query(sql: &str) -> BytesMut {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    message(b'Q', &body)
}

pub(super) fn error_response(sqlstate: &str) -> BytesMut {
    let mut body = vec![b'C'];
    body.extend_from_slice(sqlstate.as_bytes());
    body.extend_from_slice(&[0, 0]);
    message(b'E', &body)
}

pub(super) fn ready(status: u8) -> BytesMut {
    message(b'Z', &[status])
}

pub(super) async fn client_sends(
    handler: &mut ConnectionHandler,
    backend: &mut TcpStream,
    parts: &[BytesMut],
) {
    let mut buf = BytesMut::new();
    for part in parts {
        buf.extend_from_slice(part);
    }
    handler
        .process_client_data(&mut buf, backend)
        .await
        .unwrap();
}

pub(super) async fn backend_answers(handler: &mut ConnectionHandler, parts: &[BytesMut]) {
    let mut buf = BytesMut::new();
    for part in parts {
        buf.extend_from_slice(part);
    }
    handler.process_backend_data(&mut buf).await.unwrap();
}

pub(super) fn key_data(pid: i32, secret: &[u8]) -> BytesMut {
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(secret);
    message(b'K', &body)
}

pub(super) fn cancel_request(pid: i32, secret: &[u8]) -> Vec<u8> {
    let mut msg = u32::try_from(12 + secret.len())
        .unwrap()
        .to_be_bytes()
        .to_vec();
    msg.extend_from_slice(&crate::gateway::protocol::CANCEL_REQUEST_CODE);
    msg.extend_from_slice(&pid.to_be_bytes());
    msg.extend_from_slice(secret);
    msg
}
