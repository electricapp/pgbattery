//! `PostgreSQL` wire protocol parser.
//!
//! This module parses the minimal set of `PostgreSQL` protocol messages
//! needed for connection state tracking. We only parse packet headers
//! (5 bytes) and specific payload bytes for state tracking.

use bytes::{BufMut, BytesMut};

/// `PostgreSQL` message types we care about for state tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    // ===== Frontend (Client → Server) =====
    /// 'Q' - Simple query
    Query,
    /// 'P' - Extended query parse
    Parse,
    /// 'B' - Bind parameters
    Bind,
    /// 'S' - Sync (extended query boundary)
    Sync,
    /// 'X' - Client disconnect
    Terminate,
    /// 'd' - COPY data row
    CopyData,
    /// 'c' - COPY complete
    CopyDone,
    /// 'f' - COPY failed
    CopyFail,

    // ===== Backend (Server → Client) =====
    /// 'Z' - Transaction status indicator (`ReadyForQuery`)
    ReadyForQuery,
    /// 'G' - COPY IN started (client sends data)
    CopyInResponse,
    /// 'H' - COPY OUT started (server sends data)
    CopyOutResponse,
    /// 'W' - COPY BOTH (replication)
    CopyBothResponse,
    /// 'A' - Async notification (LISTEN/NOTIFY)
    NotificationResponse,
    /// 'D' - Data row (query results)
    DataRow,
    /// 'E' - Error response (backend → client)
    ErrorResponse,
    /// 'C' - Command complete
    CommandComplete,
    /// 'T' - Row description (column metadata)
    RowDescription,
    /// 'K' - Backend key data (PID and secret key for cancel requests)
    BackendKeyData,
    /// 'R' - Authentication request (code 0 = `AuthenticationOk`, anything
    /// else is a challenge: cleartext, md5, SCRAM, …)
    Authentication,

    /// Unknown or unhandled message type
    Unknown(u8),
}

impl MessageType {
    /// Parse a message type from a single byte.
    ///
    /// PERF: Inlined and optimized for the common case (Query, `ReadyForQuery`).
    /// The compiler will generate a jump table for the match.
    #[inline]
    #[must_use]
    pub const fn from_byte(b: u8) -> Self {
        match b {
            // Frontend messages
            b'Q' => Self::Query,
            b'P' => Self::Parse,
            b'B' => Self::Bind,
            b'S' => Self::Sync,
            b'X' => Self::Terminate,
            b'd' => Self::CopyData,
            b'c' => Self::CopyDone,
            b'f' => Self::CopyFail,

            // Backend messages
            b'Z' => Self::ReadyForQuery,
            b'G' => Self::CopyInResponse,
            b'H' => Self::CopyOutResponse,
            b'W' => Self::CopyBothResponse,
            b'A' => Self::NotificationResponse,
            b'D' => Self::DataRow,
            b'E' => Self::ErrorResponse, // Note: Context-dependent: Execute (frontend) or ErrorResponse (backend)
            b'C' => Self::CommandComplete,
            b'T' => Self::RowDescription,
            b'K' => Self::BackendKeyData,
            b'R' => Self::Authentication,

            _ => Self::Unknown(b),
        }
    }
}

/// Transaction state extracted from `ReadyForQuery` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionStatus {
    /// 'I' - Not in a transaction block
    #[default]
    Idle,
    /// 'T' - In a transaction block
    InTransaction,
    /// 'E' - In a failed transaction block
    Failed,
}

impl TransactionStatus {
    /// Parse transaction status from the status byte in `ReadyForQuery`.
    #[inline]
    #[must_use]
    pub const fn from_byte(b: u8) -> Self {
        match b {
            b'T' => Self::InTransaction,
            b'E' => Self::Failed,
            // 'I' or unknown, assume idle
            _ => Self::Idle,
        }
    }

    /// Check if the connection is safe to migrate during failover.
    ///
    /// Only idle connections can be safely migrated to a new backend.
    /// Connections in a transaction must be severed (data could be lost).
    #[inline]
    #[must_use]
    pub const fn is_migratable(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

/// Parsed `PostgreSQL` packet header.
#[derive(Debug, Clone, Copy)]
pub struct PacketHeader {
    /// Message type
    pub msg_type: MessageType,
    /// Message length (includes the 4-byte length field, excludes type byte)
    pub length: u32,
}

impl PacketHeader {
    /// Minimum header size (type byte + 4-byte length)
    pub const HEADER_SIZE: usize = 5;

    /// Parse a packet header from at least 5 bytes.
    ///
    /// Returns `None` if there aren't enough bytes.
    ///
    /// PERF: After the length check, we use `get()` with known-good indices.
    /// The compiler can often elide redundant bounds checks after the length guard.
    #[inline]
    #[must_use]
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::HEADER_SIZE {
            return None;
        }

        // We verified buf.len() >= 5, so indices 0..=4 are valid.
        let msg_type = MessageType::from_byte(*buf.first()?);
        let length = u32::from_be_bytes([*buf.get(1)?, *buf.get(2)?, *buf.get(3)?, *buf.get(4)?]);

        Some(Self { msg_type, length })
    }

    /// Total packet size including the type byte.
    ///
    /// Uses saturating arithmetic to prevent overflow attacks where
    /// a malicious client sends `length = u32::MAX`.
    #[inline]
    #[must_use]
    pub const fn total_length(&self) -> usize {
        1usize.saturating_add(self.length as usize)
    }

    /// Check if the packet length is within safe bounds.
    ///
    /// The length field includes itself (4 bytes), so any value below 4 is
    /// malformed. Accepting one would desynchronize the gateway's framing
    /// from the backend's — a `length = 0` frame advances our cursor by a
    /// single byte while `PostgreSQL` rejects the message outright, letting
    /// a peer steer the gateway's state tracking away from what the backend
    /// actually executes. Values above `max_size` (16MB) could cause
    /// unbounded buffering.
    #[inline]
    #[must_use]
    pub const fn is_length_valid(&self, max_size: usize) -> bool {
        (self.length as usize) >= 4 && (self.length as usize) <= max_size.saturating_sub(1)
    }
}

/// SSL request magic bytes (int32 of 80877103).
pub const SSL_REQUEST_CODE: [u8; 4] = [0x04, 0xD2, 0x16, 0x2F];

/// Cancel request magic bytes (int32 of 80877102).
pub const CANCEL_REQUEST_CODE: [u8; 4] = [0x04, 0xD2, 0x16, 0x2E];

/// GSSAPI encryption request magic bytes (int32 of 80877104).
pub const GSSENC_REQUEST_CODE: [u8; 4] = [0x04, 0xD2, 0x16, 0x30];

/// Check if a startup packet is an SSL request.
#[must_use]
pub fn is_ssl_request(buf: &[u8]) -> bool {
    buf.get(4..8).is_some_and(|code| code == SSL_REQUEST_CODE)
}

/// Check if a startup packet is a `GSSENCRequest` (8 bytes, like
/// `SSLRequest`).
///
/// The gateway does not support GSS encryption and must answer 'N' itself —
/// forwarding the request to the backend elicits a raw unframed 1-byte
/// reply that desynchronizes message parsing.
#[must_use]
pub fn is_gssenc_request(buf: &[u8]) -> bool {
    buf.get(4..8)
        .is_some_and(|code| code == GSSENC_REQUEST_CODE)
}

/// Check if a startup packet is a cancel request.
/// Cancel requests are length(4) + code(4) + pid(4) + secret. Protocol 3.0
/// secrets are 4 bytes (16 total); protocol 3.2 allows up to 256 bytes.
#[must_use]
pub fn is_cancel_request(buf: &[u8]) -> bool {
    buf.get(4..8)
        .is_some_and(|code| code == CANCEL_REQUEST_CODE)
}

/// Largest `CancelRequest` the gateway accepts:
/// length(4) + code(4) + pid(4) + secret(≤256, per protocol 3.2).
pub const MAX_CANCEL_REQUEST_LEN: usize = 12 + 256;

/// Extract `(pid, secret)` from a complete `CancelRequest` message.
///
/// The secret is returned as raw bytes of whatever length the client sent
/// (4 bytes under protocol 3.0, up to 256 under 3.2) — truncating it to a
/// fixed width would make 3.2 cancels never match the registry and be
/// silently dropped. Returns `None` for messages too short to carry a
/// pid + non-empty secret.
#[must_use]
pub fn cancel_request_key(buf: &[u8]) -> Option<(i32, &[u8])> {
    let pid = i32::from_be_bytes(<[u8; 4]>::try_from(buf.get(8..12)?).ok()?);
    let secret = buf.get(12..)?;
    if secret.is_empty() {
        return None;
    }
    Some((pid, secret))
}

/// Parse a `PostgreSQL` startup-message body for the `replication` parameter.
///
/// The startup-message body (per PG wire protocol — [Frontend/Backend
/// Protocol §55.2.1]) is byte-oriented, not UTF-8: after the 4-byte length
/// and 4-byte protocol version, it is a sequence of NUL-terminated byte
/// strings followed by a final NUL:
///
///   `name\0value\0name\0value\0…\0`
///
/// We never decode these as UTF-8 here — `eq_ignore_ascii_case(b"replication")`
/// works directly on bytes, so non-UTF-8 client junk cannot panic this
/// function. Returns `true` iff a key equal to `replication`
/// (case-insensitive, ASCII) appears with a non-empty value.
///
/// Used by the gateway to refuse streaming-replication / walsender
/// connections, which must talk directly to the node's internal PG port
/// rather than ride the failover-aware proxy.
#[must_use]
pub fn startup_has_replication_option(buf: &[u8]) -> bool {
    let Some(body) = buf.get(8..) else {
        return false;
    };
    let mut pairs = body.split(|&b| b == 0);
    while let (Some(name), Some(value)) = (pairs.next(), pairs.next()) {
        if name.is_empty() {
            // Final terminator reached (empty name after last value's NUL).
            return false;
        }
        if name.eq_ignore_ascii_case(b"replication") && !value.is_empty() {
            return true;
        }
    }
    false
}

/// Extract transaction status from a `ReadyForQuery` message payload.
#[must_use]
pub fn extract_transaction_status(payload: &[u8]) -> Option<TransactionStatus> {
    // ReadyForQuery payload is just 1 byte: the transaction status
    payload.first().map(|&b| TransactionStatus::from_byte(b))
}

/// SQLSTATE reserved for "`connection_failure`".
///
/// Signals the client driver that the session is transport-level dead and it
/// should apply its transport-retry logic rather than surface the failure as
/// a query error. Used whenever the gateway has to sever a client session
/// because of a leader change / backend disconnect and migrating the session
/// is unsafe (in-flight query, in-transaction, COPY in progress, etc.).
pub const SQLSTATE_CONNECTION_FAILURE: &str = "08006";

/// Soft cap on the `M` field (message text) of an `ErrorResponse` built by
/// [`build_failover_error_response`]. The PG wire format uses a u32 length
/// prefix, but long messages in a post-failover path are never useful — the
/// client just needs the SQLSTATE to trigger its retry logic.
const FAILOVER_ERROR_MESSAGE_MAX: usize = 512;

/// Build a `PostgreSQL` `ErrorResponse` with `SEVERITY=FATAL` and
/// `SQLSTATE=08006` (`connection_failure`).
///
/// Wire format: `'E' | length (u32 BE, includes itself) | fields... | '\0'`.
/// Each field is `field_byte | cstring`. We emit `S` (severity, localised),
/// `V` (severity, non-localised, PG 9.6+), `C` (SQLSTATE), `M` (message).
///
/// The returned buffer is a complete, ready-to-send protocol message — no
/// further framing required.
#[must_use]
pub fn build_failover_error_response(message: &str) -> BytesMut {
    const SEVERITY: &str = "FATAL";

    // Truncate at a character boundary so we don't emit a malformed cstring.
    let truncated_msg = if message.len() <= FAILOVER_ERROR_MESSAGE_MAX {
        message
    } else {
        // Find the longest prefix <= FAILOVER_ERROR_MESSAGE_MAX that ends on
        // a UTF-8 boundary.
        let mut end = FAILOVER_ERROR_MESSAGE_MAX;
        while end > 0 && !message.is_char_boundary(end) {
            end -= 1;
        }
        message.get(..end).unwrap_or("")
    };

    // Body layout (each \0-terminated):
    //   S<SEVERITY>\0  V<SEVERITY>\0  C<SQLSTATE>\0  M<msg>\0  \0
    let body_len = 2 * (1 + SEVERITY.len() + 1)                 // S + V
        + (1 + SQLSTATE_CONNECTION_FAILURE.len() + 1)           // C
        + (1 + truncated_msg.len() + 1)                         // M
        + 1; // final terminator

    // length field is u32 BE and includes itself (4 bytes) but not the type byte.
    let msg_len_field = 4 + body_len;
    // This can't legitimately overflow u32 given our 512-byte message cap.
    let msg_len_u32 = u32::try_from(msg_len_field).unwrap_or(u32::MAX);

    let mut buf = BytesMut::with_capacity(1 + msg_len_field);
    buf.put_u8(b'E');
    buf.put_u32(msg_len_u32);
    buf.put_u8(b'S');
    buf.put_slice(SEVERITY.as_bytes());
    buf.put_u8(0);
    buf.put_u8(b'V');
    buf.put_slice(SEVERITY.as_bytes());
    buf.put_u8(0);
    buf.put_u8(b'C');
    buf.put_slice(SQLSTATE_CONNECTION_FAILURE.as_bytes());
    buf.put_u8(0);
    buf.put_u8(b'M');
    buf.put_slice(truncated_msg.as_bytes());
    buf.put_u8(0);
    buf.put_u8(0);
    buf
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
    fn test_message_type_from_byte() {
        assert_eq!(MessageType::from_byte(b'Q'), MessageType::Query);
        assert_eq!(MessageType::from_byte(b'Z'), MessageType::ReadyForQuery);
        assert_eq!(MessageType::from_byte(b'X'), MessageType::Terminate);
        assert_eq!(MessageType::from_byte(b'G'), MessageType::CopyInResponse);
        assert!(matches!(
            MessageType::from_byte(0xFF),
            MessageType::Unknown(0xFF)
        ));
    }

    #[test]
    fn test_transaction_status_from_byte() {
        assert_eq!(TransactionStatus::from_byte(b'I'), TransactionStatus::Idle);
        assert_eq!(
            TransactionStatus::from_byte(b'T'),
            TransactionStatus::InTransaction
        );
        assert_eq!(
            TransactionStatus::from_byte(b'E'),
            TransactionStatus::Failed
        );
    }

    #[test]
    fn test_packet_header_parse() {
        // ReadyForQuery message: 'Z' + length(5) + status byte
        let buf = [b'Z', 0, 0, 0, 5, b'I'];

        let header = PacketHeader::parse(&buf).unwrap();
        assert_eq!(header.msg_type, MessageType::ReadyForQuery);
        assert_eq!(header.length, 5);
        assert_eq!(header.total_length(), 6);
    }

    #[test]
    fn test_packet_header_parse_insufficient_bytes() {
        let buf = [b'Z', 0, 0];
        assert!(PacketHeader::parse(&buf).is_none());
    }

    #[test]
    fn test_ssl_request_detection() {
        // SSL request: length(8) + protocol(80877103)
        let ssl_request = [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F];
        assert!(is_ssl_request(&ssl_request));

        // Regular startup message
        let startup = [0, 0, 0, 8, 0, 0, 0, 0];
        assert!(!is_ssl_request(&startup));
    }

    /// Helper: build a startup packet header (4-byte length + 4-byte version)
    /// followed by the given param pairs and the final terminating NUL.
    fn make_startup(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut body: Vec<u8> = Vec::new();
        for (k, v) in pairs {
            body.extend_from_slice(k);
            body.push(0);
            body.extend_from_slice(v);
            body.push(0);
        }
        body.push(0); // final terminator

        let total_len = 8 + body.len();
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&u32::try_from(total_len).unwrap().to_be_bytes());
        out.extend_from_slice(&0x00_03_00_00u32.to_be_bytes()); // protocol 3.0
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn test_startup_no_replication_option() {
        let buf = make_startup(&[(b"user", b"alice"), (b"database", b"postgres")]);
        assert!(!startup_has_replication_option(&buf));
    }

    #[test]
    fn test_startup_with_replication_database() {
        let buf = make_startup(&[
            (b"user", b"repl"),
            (b"replication", b"database"),
            (b"database", b"postgres"),
        ]);
        assert!(startup_has_replication_option(&buf));
    }

    #[test]
    fn test_startup_with_replication_physical_true() {
        let buf = make_startup(&[(b"user", b"repl"), (b"replication", b"true")]);
        assert!(startup_has_replication_option(&buf));
    }

    #[test]
    fn test_startup_replication_case_insensitive() {
        let buf = make_startup(&[(b"Replication", b"true")]);
        assert!(startup_has_replication_option(&buf));
        let buf = make_startup(&[(b"REPLICATION", b"on")]);
        assert!(startup_has_replication_option(&buf));
    }

    #[test]
    fn test_startup_replication_empty_value_ignored() {
        // Empty value means "no replication" — PG treats it as the default
        // (non-replication) connection mode.
        let buf = make_startup(&[(b"replication", b""), (b"user", b"alice")]);
        assert!(!startup_has_replication_option(&buf));
    }

    #[test]
    fn test_startup_truncated_buffer_no_panic() {
        // Empty / short buffers must not panic.
        assert!(!startup_has_replication_option(&[]));
        assert!(!startup_has_replication_option(&[0, 0, 0, 4]));
        assert!(!startup_has_replication_option(&[0, 0, 0, 8, 0, 3, 0, 0]));
    }

    #[test]
    fn test_startup_non_utf8_bytes_no_panic() {
        // Non-UTF-8 garbage in value position: must not panic, must not match.
        let buf = make_startup(&[(b"user", &[0xFF, 0xFE, 0x80])]);
        assert!(!startup_has_replication_option(&buf));
    }

    #[test]
    fn test_packet_length_validation() {
        let max_size = 16usize * 1024 * 1024; // 16MB
        let max_size_u32 = u32::try_from(max_size).unwrap_or(u32::MAX);

        // Normal packet - should be valid
        let normal = PacketHeader {
            msg_type: MessageType::Query,
            length: 100,
        };
        assert!(normal.is_length_valid(max_size));

        // Packet at limit - should be valid (length = max_size - 1 for type byte)
        let at_limit = PacketHeader {
            msg_type: MessageType::Query,
            length: max_size_u32 - 1,
        };
        assert!(at_limit.is_length_valid(max_size));

        // Packet over limit - should be invalid
        let over_limit = PacketHeader {
            msg_type: MessageType::Query,
            length: max_size_u32,
        };
        assert!(!over_limit.is_length_valid(max_size));

        // Malicious packet with u32::MAX length - should be invalid
        let malicious = PacketHeader {
            msg_type: MessageType::Query,
            length: u32::MAX,
        };
        assert!(!malicious.is_length_valid(max_size));

        // The length field includes itself: anything below 4 is malformed
        // and would desync framing against the backend.
        for length in 0..4u32 {
            let undersized = PacketHeader {
                msg_type: MessageType::Query,
                length,
            };
            assert!(
                !undersized.is_length_valid(max_size),
                "length {length} must be rejected"
            );
        }
        let minimal = PacketHeader {
            msg_type: MessageType::Sync,
            length: 4,
        };
        assert!(minimal.is_length_valid(max_size));
    }

    #[test]
    fn test_gssenc_request_detection() {
        // GSSENCRequest: length(8) + code(80877104)
        let gssenc = [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x30];
        assert!(is_gssenc_request(&gssenc));
        assert!(!is_ssl_request(&gssenc));
        assert!(!is_cancel_request(&gssenc));

        // SSL request must not be detected as GSSENC.
        let ssl = [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F];
        assert!(!is_gssenc_request(&ssl));

        // Truncated buffer must not match.
        assert!(!is_gssenc_request(&[0, 0, 0, 8]));
    }

    #[test]
    fn test_total_length_saturating() {
        // Normal case
        let normal = PacketHeader {
            msg_type: MessageType::Query,
            length: 100,
        };
        assert_eq!(normal.total_length(), 101);

        // Edge case: u32::MAX should saturate, not overflow
        let max_length = PacketHeader {
            msg_type: MessageType::Query,
            length: u32::MAX,
        };
        // Should saturate to usize::MAX, not wrap to 0
        let total = max_length.total_length();
        assert!(total > 0, "total_length should not overflow to 0");
        // On 64-bit systems, u32::MAX + 1 fits in usize
        #[cfg(target_pointer_width = "64")]
        assert_eq!(total, usize::try_from(u32::MAX).unwrap_or(usize::MAX) + 1);
    }

    #[test]
    fn test_cancel_request_detection() {
        // Cancel request: length(16) + protocol(80877102) + pid(4) + secret(4)
        let cancel = [0, 0, 0, 16, 0x04, 0xD2, 0x16, 0x2E, 0, 0, 0, 1, 0, 0, 0, 2];
        assert!(is_cancel_request(&cancel));

        // SSL request should not be detected as cancel
        let ssl = [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F];
        assert!(!is_cancel_request(&ssl));
    }

    #[test]
    fn test_cancel_request_key_protocol_30() {
        // 16-byte protocol 3.0 cancel: pid=7, secret=[0,0,0,42]
        let cancel = [0, 0, 0, 16, 0x04, 0xD2, 0x16, 0x2E, 0, 0, 0, 7, 0, 0, 0, 42];
        let (pid, secret) = cancel_request_key(&cancel).unwrap();
        assert_eq!(pid, 7);
        assert_eq!(secret, &[0, 0, 0, 42]);
    }

    #[test]
    fn test_cancel_request_key_variable_length_secret() {
        // Protocol 3.2 cancel with a 12-byte secret.
        let mut cancel = vec![0, 0, 0, 24, 0x04, 0xD2, 0x16, 0x2E, 0, 0, 1, 0];
        cancel.extend_from_slice(&[9; 12]);
        let (pid, secret) = cancel_request_key(&cancel).unwrap();
        assert_eq!(pid, 256);
        assert_eq!(secret, &[9; 12]);
    }

    #[test]
    fn test_cancel_request_key_rejects_short_messages() {
        // Too short to hold a pid.
        assert!(cancel_request_key(&[0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2E]).is_none());
        // pid present but no secret bytes at all.
        let no_secret = [0, 0, 0, 12, 0x04, 0xD2, 0x16, 0x2E, 0, 0, 0, 1];
        assert!(cancel_request_key(&no_secret).is_none());
    }

    #[test]
    fn test_failover_error_response_roundtrip() {
        let msg = build_failover_error_response("connection lost during failover");

        // First byte is the message type.
        assert_eq!(msg.first(), Some(&b'E'));

        // Bytes 1..5 are the length field (u32 BE, includes itself).
        let len_bytes = msg.get(1..5).unwrap();
        let declared_len =
            u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        // total_on_wire = 1 (type) + declared_len. Verify the buffer really
        // has that many bytes.
        assert_eq!(msg.len(), 1 + declared_len);

        // Body fields, zero-terminated cstrings. Make sure 08006 appears
        // under a 'C' field and that the buffer ends with \0\0 (final message
        // terminator preceded by the M-field terminator).
        let body = msg.get(5..).unwrap();
        assert!(body.windows(7).any(|w| w == b"C08006\0"));
        assert_eq!(body.last(), Some(&0));
    }

    #[test]
    fn test_failover_error_response_truncates_long_message() {
        let long = "x".repeat(2_000);
        let msg = build_failover_error_response(&long);
        // Length field is u32 — must have succeeded (cap is 512).
        assert!(msg.len() < 1_000);
    }

    #[test]
    fn test_failover_error_response_truncates_on_char_boundary() {
        // Build a message that, if truncated at byte 512, would split a
        // multi-byte UTF-8 codepoint. We just need to confirm the truncation
        // path produces a valid BytesMut and doesn't panic.
        let multibyte = "é".repeat(400); // each 'é' is 2 bytes => 800 bytes
        let msg = build_failover_error_response(&multibyte);
        assert!(msg.first() == Some(&b'E'));
    }

    #[test]
    fn test_extract_transaction_status() {
        assert_eq!(
            extract_transaction_status(b"I"),
            Some(TransactionStatus::Idle)
        );
        assert_eq!(
            extract_transaction_status(b"T"),
            Some(TransactionStatus::InTransaction)
        );
        assert_eq!(
            extract_transaction_status(b"E"),
            Some(TransactionStatus::Failed)
        );
        assert_eq!(extract_transaction_status(&[]), None);
    }
}
