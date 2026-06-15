//! Connection state management for the Gateway.
//!
//! Tracks per-connection state for failover decisions and provides
//! a registry for coordinating failovers across all connections.

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use super::protocol::TransactionStatus;

/// Cache-line padded atomic to prevent false sharing.
/// On `x86_64`, cache lines are 64 bytes. When multiple atomics share
/// a cache line, writes from one core invalidate the line for all cores.
#[derive(Debug)]
#[repr(align(64))]
struct CachePaddedAtomicUsize(AtomicUsize);

impl CachePaddedAtomicUsize {
    const fn new(val: usize) -> Self {
        Self(AtomicUsize::new(val))
    }

    #[inline]
    fn fetch_add(&self, val: usize, order: Ordering) -> usize {
        self.0.fetch_add(val, order)
    }

    #[inline]
    fn fetch_sub(&self, val: usize, order: Ordering) -> usize {
        self.0.fetch_sub(val, order)
    }

    #[inline]
    fn load(&self, order: Ordering) -> usize {
        self.0.load(order)
    }
}

#[derive(Debug)]
#[repr(align(64))]
struct CachePaddedAtomicU64(AtomicU64);

impl CachePaddedAtomicU64 {
    const fn new(val: u64) -> Self {
        Self(AtomicU64::new(val))
    }

    #[inline]
    fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
        self.0.fetch_add(val, order)
    }
}

/// Connection proxy mode - determines failover behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProxyMode {
    /// Normal query processing - can inspect and potentially migrate
    #[default]
    Normal,
    /// COPY IN/OUT active - streaming data, cannot migrate
    CopyStreaming,
    /// SSL passthrough - cannot inspect packets
    SslPassthrough,
}

impl ProxyMode {
    /// Check if the connection can be migrated during failover.
    #[must_use]
    pub const fn is_migratable(&self) -> bool {
        matches!(self, Self::Normal)
    }
}

/// SSL/TLS mode for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// No SSL - plaintext connection
    #[default]
    Disabled,
    /// SSL terminated at proxy - plaintext to backend
    Terminated,
    /// SSL passed through to backend - cannot inspect
    Passthrough,
}

/// LISTEN/NOTIFY channel subscriptions for a connection.
#[derive(Debug, Default, Clone)]
pub struct NotifySubscriptions {
    /// Channel names this connection is listening to
    pub channels: HashSet<String>,
}

/// Stateful session that must be replayed on a new backend during failover.
///
/// Captures named prepared statements so clients can keep issuing the same
/// queries after a leader change without a "prepared statement does not exist"
/// error. Session GUCs are NOT reconstructed — a connection that set one is
/// marked non-migratable instead (see `ConnectionState::not_migratable`).
#[derive(Debug, Default, Clone)]
pub struct SessionReplay {
    /// Named prepared statements: name → raw Parse message bytes ready to re-send.
    /// Unnamed statements (empty name) are not tracked — they're transient by
    /// definition and re-issued by clients on every use.
    pub prepared: HashMap<String, Bytes>,
}

/// State for tracking in-flight COMMIT for transaction status verification.
///
/// If a primary crashes after writing a COMMIT to WAL but before sending
/// acknowledgment to the proxy, we can query the new leader with
/// `SELECT txid_status($txid)` to verify if the transaction committed.
/// This prevents false negatives where the client sees an error but the
/// transaction actually succeeded.
#[derive(Debug, Default, Clone)]
pub struct CommitProbeState {
    /// Transaction ID from `txid_current()` - captured before COMMIT
    pub txid: Option<i64>,
    /// Whether we're waiting for a COMMIT response from backend
    pub pending_commit: bool,
    /// True only when the in-flight COMMIT was a standalone simple-query
    /// `COMMIT` (nothing else in the batch). The synthetic commit response
    /// emits exactly one `CommandComplete`+`ReadyForQuery`, which is only
    /// wire-correct for that case. For a multi-statement batch or the extended
    /// protocol the client is owed a different message sequence, so we sever
    /// (08006) instead of risking a desynced session.
    pub lone_commit: bool,
}

/// Per-connection state tracking.
///
/// PERF: Fields are ordered for cache efficiency.
/// Hot fields (accessed on every message) are grouped together
/// at the start to fit in a single cache line when possible.
#[derive(Debug)]
#[repr(C)] // Ensure predictable layout
pub struct ConnectionState {
    // ===== HOT PATH - First cache line (64 bytes) =====
    // These fields are read/written on every message
    /// Unique connection identifier (u64 for speed, not UUID)
    pub id: u64, // 8 bytes, offset 0

    /// Bytes sent to backend (updated every client->backend forward)
    pub bytes_sent: u64, // 8 bytes, offset 8

    /// Bytes received from backend (updated every backend->client forward)
    pub bytes_received: u64, // 8 bytes, offset 16

    /// Number of queries processed
    pub queries_processed: u64, // 8 bytes, offset 24

    /// Current transaction status (checked on every `ReadyForQuery`)
    pub tx_status: TransactionStatus, // 1 byte, offset 32

    /// Current proxy mode (checked on every message)
    pub proxy_mode: ProxyMode, // 1 byte, offset 33

    /// SSL mode (checked once per message loop iteration)
    pub ssl_mode: SslMode, // 1 byte, offset 34

    // 29 bytes padding to end of cache line

    // ===== COLD PATH - Second cache line =====
    // These fields are accessed rarely (init, failover, metrics)
    /// Backend server address (if connected) - set once at connect
    pub backend_addr: Option<SocketAddr>, // 24 bytes

    /// Connection creation time - set once, read for metrics
    pub created_at: Instant, // 8 bytes

    /// Last activity time - REMOVED: `Instant::now()` is expensive
    /// If needed, use coarse timestamp or update only on idle transitions

    /// LISTEN/NOTIFY subscriptions - rare feature
    pub subscriptions: NotifySubscriptions,

    /// Session state to replay on a new backend after failover.
    /// Populated as the client issues PARSE / SET messages.
    pub replay: SessionReplay,

    /// Commit probe state for transaction status verification on failover
    pub commit_probe: CommitProbeState,

    /// Backend key data (PID, secret) received from `PostgreSQL`.
    /// Used for routing cancel requests to the correct backend.
    pub backend_key: Option<BackendKey>,

    /// True when we have forwarded a client message to the backend and have
    /// not yet observed the follow-up `ReadyForQuery`.
    ///
    /// This is the "is a request in flight?" bit, distinct from `tx_status`.
    /// `tx_status` is the *transaction* status as of the last
    /// `ReadyForQuery` — between messages we can be `Idle` (last-seen status)
    /// *and* have a query in flight whose response hasn't arrived yet. On
    /// backend disconnect with this flag set, the outcome of the last
    /// request is unknown — silently migrating the session would leave the
    /// client waiting forever for a response that the new backend has no
    /// knowledge of. We emit an `ErrorResponse` (SQLSTATE `08006`) so the
    /// driver applies its transport-retry logic instead of hanging.
    pub awaiting_response: bool,

    /// Set when the session carries state we cannot reconstruct on a new
    /// backend — `LISTEN "*"` or any session-scoped `SET`. Such a connection is
    /// not migratable: the gateway severs it (SQLSTATE `08006`) on the next
    /// leader change so the client reconnects with a clean session rather than
    /// silently inheriting the new backend's default GUCs.
    pub not_migratable: bool,
}

impl ConnectionState {
    /// Create a new connection state.
    #[inline]
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self {
            // Hot path fields first
            id,
            bytes_sent: 0,
            bytes_received: 0,
            queries_processed: 0,
            tx_status: TransactionStatus::default(),
            proxy_mode: ProxyMode::default(),
            ssl_mode: SslMode::default(),
            // Cold path fields
            backend_addr: None,
            created_at: Instant::now(), // Only called once per connection
            subscriptions: NotifySubscriptions::default(),
            replay: SessionReplay::default(),
            commit_probe: CommitProbeState::default(),
            backend_key: None,
            awaiting_response: false,
            not_migratable: false,
        }
    }

    /// Check if this connection can be migrated during failover.
    ///
    /// A connection is migratable only if:
    /// - Transaction status is Idle (not in a transaction)
    /// - Proxy mode allows inspection (not in COPY or SSL passthrough)
    /// - No client request is in flight awaiting a backend response
    /// - The session does not depend on state we cannot reconstruct on a new
    ///   backend (`LISTEN "*"`, or any session-scoped `SET`)
    #[inline]
    #[must_use]
    pub const fn is_migratable(&self) -> bool {
        self.tx_status.is_migratable()
            && self.proxy_mode.is_migratable()
            && !self.awaiting_response
            && !self.not_migratable
    }

    // REMOVED: touch() - Instant::now() is a syscall, don't call it on every message
    // REMOVED: record_sent() - just update bytes_sent directly
    // REMOVED: record_received() - just update bytes_received directly
    // REMOVED: record_query() - just update queries_processed directly
}

/// Shared connection state wrapper.
pub type SharedConnectionState = Arc<RwLock<ConnectionState>>;

/// Backend key data from `PostgreSQL` (used for cancel request routing).
///
/// Protocol 3.0 secrets are exactly 4 bytes; protocol 3.2 (PG 18+) allows
/// variable-length secrets up to 256 bytes, so the secret is stored as the
/// raw bytes from `BackendKeyData` and compared in full — truncating to 4
/// bytes would silently drop cancels for 3.2 sessions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BackendKey {
    pub pid: i32,
    pub secret: Vec<u8>,
}

/// Registry for tracking all active connections.
///
/// This is used by the failover logic to coordinate migrations
/// and severing of connections.
///
/// PERF NOTES:
/// - Uses `DashMap` (sharded concurrent hashmap) instead of `RwLock<HashMap>`
///   to eliminate global lock contention. Each shard is independently locked.
/// - Atomics are cache-line padded to prevent false sharing between cores.
/// - Metrics are NOT updated in hot path - use `sync_metrics()` periodically.
#[derive(Debug)]
pub struct ConnectionRegistry {
    /// All active connections by ID.
    /// `DashMap` provides ~64 internal shards for concurrent access.
    /// Shard locks are held only during lookup, not during connection work.
    connections: DashMap<u64, SharedConnectionState>,

    /// Backend key to backend address mapping for cancel request routing.
    /// When a connection receives `BackendKeyData` from `PostgreSQL`, we store
    /// the mapping so cancel requests can be routed to the correct backend.
    backend_keys: DashMap<BackendKey, SocketAddr>,

    /// Atomic connection ID counter (replaces expensive UUID generation)
    /// Cache-padded: written by accept thread, read by all
    next_conn_id: CachePaddedAtomicU64,

    /// Atomic counters for connection states (avoids O(n) iteration).
    /// Each on its own cache line to prevent false sharing between cores.
    idle_count: CachePaddedAtomicUsize,
    in_transaction_count: CachePaddedAtomicUsize,
    copy_streaming_count: CachePaddedAtomicUsize,
}

impl ConnectionRegistry {
    /// Create a new connection registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            // DashMap with default capacity and shard count (~64 shards)
            connections: DashMap::with_capacity(1024),
            backend_keys: DashMap::with_capacity(1024),
            next_conn_id: CachePaddedAtomicU64::new(1),
            idle_count: CachePaddedAtomicUsize::new(0),
            in_transaction_count: CachePaddedAtomicUsize::new(0),
            copy_streaming_count: CachePaddedAtomicUsize::new(0),
        }
    }

    /// Register a backend key mapping for cancel request routing.
    pub fn register_backend_key(&self, key: BackendKey, backend_addr: SocketAddr) {
        self.backend_keys.insert(key, backend_addr);
    }

    /// Unregister a backend key when connection closes.
    pub fn unregister_backend_key(&self, key: &BackendKey) {
        self.backend_keys.remove(key);
    }

    /// Look up the backend address for a cancel request.
    pub fn lookup_backend_for_cancel(&self, pid: i32, secret: &[u8]) -> Option<SocketAddr> {
        let key = BackendKey {
            pid,
            secret: secret.to_vec(),
        };
        self.backend_keys.get(&key).map(|r| *r)
    }

    /// Generate a new connection ID (fast, no crypto).
    #[inline]
    pub fn next_id(&self) -> u64 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register a new connection.
    ///
    /// PERF: `DashMap` shard lock held only for insert (~20ns).
    /// Metrics updated here since register is not hot path.
    pub fn register(&self, state: SharedConnectionState) {
        let id = state.read().id;
        self.connections.insert(id, state);
        // New connections start idle
        self.idle_count.fetch_add(1, Ordering::Relaxed);

        // Metrics OK here - register is not hot path
        metrics::gauge!("pgbattery_connections_active").increment(1.0);
    }

    /// Unregister a connection.
    ///
    /// PERF: `DashMap` shard lock held only for remove (~20ns).
    /// Metrics updated here since unregister is not hot path.
    pub fn unregister(&self, id: u64) {
        if let Some((_, conn)) = self.connections.remove(&id) {
            // Decrement the appropriate counter based on final state
            let state = conn.read();
            match state.tx_status {
                TransactionStatus::Idle => {
                    self.idle_count.fetch_sub(1, Ordering::Relaxed);
                }
                TransactionStatus::InTransaction | TransactionStatus::Failed => {
                    self.in_transaction_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
            if state.proxy_mode == ProxyMode::CopyStreaming
                && self.copy_streaming_count.load(Ordering::Relaxed) > 0
            {
                self.copy_streaming_count.fetch_sub(1, Ordering::Relaxed);
            }
            drop(state);

            // Metrics OK here - unregister is not hot path
            metrics::gauge!("pgbattery_connections_active").decrement(1.0);
        }
    }

    /// Update transaction status through an already-held connection handle.
    ///
    /// PERF CRITICAL PATH - called on every query completion. The
    /// per-connection handler owns an `Arc` to its own state; routing the
    /// update through it skips the `DashMap` shard lookup + `Arc` clone the
    /// id-keyed variant pays. NO metrics updates here - use `sync_metrics()`
    /// periodically.
    #[inline]
    pub fn update_tx_status_on(&self, conn: &SharedConnectionState, new_status: TransactionStatus) {
        // Step 1: Update connection state (no registry lock held)
        let old_status = {
            let mut state = conn.write();
            let old = state.tx_status;
            state.tx_status = new_status;
            old
        };

        // Step 2: Update atomic counters (lock-free)
        if old_status != new_status {
            match old_status {
                TransactionStatus::Idle => {
                    self.idle_count.fetch_sub(1, Ordering::Relaxed);
                }
                TransactionStatus::InTransaction | TransactionStatus::Failed => {
                    self.in_transaction_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
            match new_status {
                TransactionStatus::Idle => {
                    self.idle_count.fetch_add(1, Ordering::Relaxed);
                }
                TransactionStatus::InTransaction | TransactionStatus::Failed => {
                    self.in_transaction_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            // NO metrics::gauge! here - that's the whole point
        }
    }

    /// Update transaction status by connection id.
    ///
    /// `DashMap` shard lock held only for the Arc clone (~20ns); the
    /// connection write lock is independent of the registry.
    #[inline]
    pub fn update_tx_status_fast(&self, id: u64, new_status: TransactionStatus) {
        let conn = match self.connections.get(&id) {
            Some(entry) => entry.value().clone(),
            None => return,
        };
        // Shard lock released here - we only hold Arc<RwLock<ConnectionState>>
        self.update_tx_status_on(&conn, new_status);
    }

    /// Update transaction status for a connection.
    #[inline]
    pub fn update_tx_status(&self, id: u64, status: TransactionStatus) {
        self.update_tx_status_fast(id, status);
    }

    /// Update proxy mode (and counters) through an already-held connection
    /// handle — no `DashMap` lookup on the hot path.
    #[inline]
    pub fn update_proxy_mode_on(&self, conn: &SharedConnectionState, new_mode: ProxyMode) {
        let old_mode = {
            let mut state = conn.write();
            let old = state.proxy_mode;
            state.proxy_mode = new_mode;
            old
        };

        if old_mode != new_mode {
            if old_mode == ProxyMode::CopyStreaming
                && self.copy_streaming_count.load(Ordering::Relaxed) > 0
            {
                self.copy_streaming_count.fetch_sub(1, Ordering::Relaxed);
            }
            if new_mode == ProxyMode::CopyStreaming {
                self.copy_streaming_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Update proxy mode by connection id.
    #[inline]
    pub fn update_proxy_mode_fast(&self, id: u64, new_mode: ProxyMode) {
        let conn = match self.connections.get(&id) {
            Some(entry) => entry.value().clone(),
            None => return,
        };
        self.update_proxy_mode_on(&conn, new_mode);
    }

    /// Get total number of active connections.
    #[inline]
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Sync atomic counters to Prometheus metrics.
    ///
    /// Call this from a background task every 100-500ms.
    /// NOT called from hot path.
    #[allow(
        clippy::cast_precision_loss,
        reason = "connection counts are small; precision loss is negligible"
    )]
    pub fn sync_metrics(&self) {
        metrics::gauge!("pgbattery_connections_idle")
            .set(self.idle_count.load(Ordering::Relaxed) as f64);
        metrics::gauge!("pgbattery_connections_in_transaction")
            .set(self.in_transaction_count.load(Ordering::Relaxed) as f64);
        metrics::gauge!("pgbattery_connections_copy_streaming")
            .set(self.copy_streaming_count.load(Ordering::Relaxed) as f64);
        metrics::gauge!("pgbattery_connections_active").set(self.connections.len() as f64);
    }
}

impl Default for ConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    unsafe_code,
    reason = "test code asserts on known-good values; unsafe is used for controlled test fixtures"
)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_new() {
        let id = 42u64;
        let state = ConnectionState::new(id);

        assert_eq!(state.id, id);
        assert_eq!(state.tx_status, TransactionStatus::Idle);
        assert_eq!(state.proxy_mode, ProxyMode::Normal);
        assert_eq!(state.bytes_sent, 0);
        assert_eq!(state.bytes_received, 0);
        assert!(state.is_migratable());
    }

    #[test]
    fn test_connection_state_migratable() {
        let mut state = ConnectionState::new(1);

        // Initially migratable (idle, normal mode, no in-flight request)
        assert!(state.is_migratable());

        // In transaction - not migratable
        state.tx_status = TransactionStatus::InTransaction;
        assert!(!state.is_migratable());

        // Back to idle but in COPY mode - not migratable
        state.tx_status = TransactionStatus::Idle;
        state.proxy_mode = ProxyMode::CopyStreaming;
        assert!(!state.is_migratable());

        // Back to normal - migratable again
        state.proxy_mode = ProxyMode::Normal;
        assert!(state.is_migratable());

        // Idle + normal, but a request is in flight — not migratable
        // (outcome of the in-flight query would be unknown on the new backend)
        state.awaiting_response = true;
        assert!(!state.is_migratable());

        state.awaiting_response = false;
        assert!(state.is_migratable());
    }

    #[test]
    fn test_connection_state_layout() {
        // Verify hot fields are at the start of the struct
        let state = ConnectionState::new(1);
        let ptr = (&raw const state).cast::<u8>();

        // id should be at offset 0
        let id_ptr = (&raw const state.id).cast::<u8>();
        assert_eq!(unsafe { id_ptr.offset_from(ptr) }, 0);

        // bytes_sent should be at offset 8
        let sent_ptr = (&raw const state.bytes_sent).cast::<u8>();
        assert_eq!(unsafe { sent_ptr.offset_from(ptr) }, 8);
    }

    #[test]
    fn test_notify_subscriptions() {
        let mut subs = NotifySubscriptions::default();

        assert!(subs.channels.is_empty());

        subs.channels.insert("channel1".to_string());
        subs.channels.insert("channel2".to_string());
        assert!(!subs.channels.is_empty());
        assert_eq!(subs.channels.len(), 2);

        subs.channels.remove("channel1");
        assert_eq!(subs.channels.len(), 1);

        subs.channels.clear();
        assert!(subs.channels.is_empty());
    }

    #[test]
    fn test_connection_registry() {
        let registry = ConnectionRegistry::new();

        // Generate connection IDs using the registry
        let id1 = registry.next_id();
        let id2 = registry.next_id();

        let state1 = Arc::new(RwLock::new(ConnectionState::new(id1)));
        let state2 = Arc::new(RwLock::new(ConnectionState::new(id2)));

        registry.register(state1.clone());
        registry.register(state2.clone());

        assert_eq!(registry.connection_count(), 2);

        // Both should be migratable initially
        assert!(state1.read().is_migratable());
        assert!(state2.read().is_migratable());

        // Make one non-migratable (in transaction)
        registry.update_tx_status(id1, TransactionStatus::InTransaction);
        assert!(!state1.read().is_migratable());
        assert!(state2.read().is_migratable());

        // Unregister
        registry.unregister(id1);
        assert_eq!(registry.connection_count(), 1);
    }

    #[test]
    fn test_copy_streaming_counter_tracks_mode_transitions() {
        let registry = ConnectionRegistry::new();
        let id = registry.next_id();
        let state = Arc::new(RwLock::new(ConnectionState::new(id)));
        registry.register(state);

        assert_eq!(registry.copy_streaming_count.load(Ordering::Relaxed), 0);

        registry.update_proxy_mode_fast(id, ProxyMode::CopyStreaming);
        assert_eq!(registry.copy_streaming_count.load(Ordering::Relaxed), 1);

        // Re-applying same mode must not double-count
        registry.update_proxy_mode_fast(id, ProxyMode::CopyStreaming);
        assert_eq!(registry.copy_streaming_count.load(Ordering::Relaxed), 1);

        registry.update_proxy_mode_fast(id, ProxyMode::Normal);
        assert_eq!(registry.copy_streaming_count.load(Ordering::Relaxed), 0);

        // Unregister after mode returned to Normal should not underflow
        registry.unregister(id);
        assert_eq!(registry.copy_streaming_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_tx_status_counters_track_transitions() {
        let registry = ConnectionRegistry::new();
        let id = registry.next_id();
        let state = Arc::new(RwLock::new(ConnectionState::new(id)));
        registry.register(state);

        assert_eq!(registry.idle_count.load(Ordering::Relaxed), 1);
        assert_eq!(registry.in_transaction_count.load(Ordering::Relaxed), 0);

        registry.update_tx_status_fast(id, TransactionStatus::InTransaction);
        assert_eq!(registry.idle_count.load(Ordering::Relaxed), 0);
        assert_eq!(registry.in_transaction_count.load(Ordering::Relaxed), 1);

        // Failed transactions are still counted in in_transaction bucket.
        registry.update_tx_status_fast(id, TransactionStatus::Failed);
        assert_eq!(registry.idle_count.load(Ordering::Relaxed), 0);
        assert_eq!(registry.in_transaction_count.load(Ordering::Relaxed), 1);

        registry.update_tx_status_fast(id, TransactionStatus::Idle);
        assert_eq!(registry.idle_count.load(Ordering::Relaxed), 1);
        assert_eq!(registry.in_transaction_count.load(Ordering::Relaxed), 0);

        registry.unregister(id);
        assert_eq!(registry.idle_count.load(Ordering::Relaxed), 0);
        assert_eq!(registry.in_transaction_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cache_padded_size() {
        // Verify cache padding is working
        assert!(size_of::<CachePaddedAtomicUsize>() >= 64);
        assert!(size_of::<CachePaddedAtomicU64>() >= 64);
    }
}
