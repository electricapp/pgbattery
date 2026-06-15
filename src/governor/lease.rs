//! Leader lease tracking for split-brain prevention.
//!
//! A leader's authority to accept writes is time-limited. This lease is
//! renewed with every successful Raft heartbeat/quorum acknowledgment.
//! If the lease expires, the node must stop accepting writes immediately.
//!
//! # Clock injection
//!
//! `LeaseState` reads time via [`pgbattery_core::Clock`] rather than
//! `Instant::now()` directly. Production wires [`SystemClock`]; tests
//! can supply a manually-advanced clock so lease-expiry behavior can be
//! exercised without `thread::sleep`. The `dyn Clock::now()` call is
//! dominated by the underlying syscall it forwards to.

use parking_lot::RwLock;
use pgbattery_core::{Clock, SystemClock};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default lease duration - conservative value
/// Leader must renew within this window or lose write authority
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(2);

/// How often to check lease validity in background tasks
pub const LEASE_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Leader lease state - shared between Governor, Gateway, and Supervisor
///
/// This is the source of truth for "can this node accept writes?"
/// - Governor updates it based on Raft leadership
/// - Gateway checks it before forwarding writes
/// - Supervisor uses it to fence `PostgreSQL`
pub struct LeaseState {
    /// Time source. Production: `Arc<SystemClock>`. Held as
    /// `Arc<dyn Clock>` so the struct stays non-generic (callers don't
    /// pay a type-parameter tax to thread the clock through every
    /// interface that holds `SharedLeaseState`).
    clock: Arc<dyn Clock>,

    /// When the current lease expires
    /// If `clock.now() > expires_at`, the node has NO write authority
    expires_at: Instant,

    /// How long each lease grant lasts
    duration: Duration,

    /// Is this node currently the Raft leader?
    /// Updated by Governor, used for metrics/logging
    is_leader: bool,
}

impl std::fmt::Debug for LeaseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseState")
            .field("expires_at", &self.expires_at)
            .field("duration", &self.duration)
            .field("is_leader", &self.is_leader)
            .field("clock", &"<dyn Clock>")
            .finish()
    }
}

impl LeaseState {
    /// Create a new lease state with the system clock (expired by default).
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    /// Create a lease state with a custom clock — useful for tests that
    /// need to drive lease expiry without `thread::sleep`.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        let now = clock.now();
        let expires_at = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);
        Self {
            clock,
            expires_at,
            duration: DEFAULT_LEASE_DURATION,
            is_leader: false,
        }
    }

    /// Create with custom duration (system clock).
    #[must_use]
    pub fn with_duration(duration: Duration) -> Self {
        Self::with_clock_and_duration(Arc::new(SystemClock), duration)
    }

    /// Create with both custom clock and duration.
    #[must_use]
    pub fn with_clock_and_duration(clock: Arc<dyn Clock>, duration: Duration) -> Self {
        let mut s = Self::with_clock(clock);
        s.duration = duration;
        s
    }

    /// Check if the lease is currently valid
    ///
    /// Returns true only if:
    /// 1. Node is Raft leader
    /// 2. Lease has not expired (now < `expires_at`)
    #[inline]
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.is_leader && self.clock.now() < self.expires_at
    }

    /// Renew the lease (called when Raft confirms leadership with quorum).
    ///
    /// `quorum_ack_age` is how long ago the most recent quorum acknowledgment
    /// actually arrived. The lease is anchored at that instant, NOT at `now` —
    /// otherwise a leader whose last quorum contact is already `QUORUM_TIMEOUT_MS`
    /// stale would be granted a *fresh full* `duration`, stacking the staleness
    /// budget on top of the lease and widening the stale-leader write window
    /// past `duration`. Anchoring on the ack instant caps worst-case authority
    /// at exactly `duration` from real quorum contact.
    pub fn renew(&mut self, quorum_ack_age: Duration) {
        let now = self.clock.now();
        let anchor = now.checked_sub(quorum_ack_age).unwrap_or(now);
        self.expires_at = anchor + self.duration;

        tracing::trace!(
            is_leader = self.is_leader,
            quorum_ack_age_ms = quorum_ack_age.as_millis(),
            expires_in_ms = self.remaining().as_millis(),
            "Lease renewed"
        );
    }

    /// Update lease state based on Raft metrics
    ///
    /// Called periodically by Governor when Raft state changes.
    /// The `pgbattery_has_lease` gauge tracks lease *validity*, not just
    /// leadership role — a node that is still `is_leader=true` per Raft
    /// but has lost quorum (and thus the lease) must show gauge=0, and
    /// gauge must return to 1 when quorum is restored without the role
    /// flipping. So we compare lease validity before/after and log only
    /// on the actual edge.
    pub fn update_from_raft(
        &mut self,
        is_leader: bool,
        has_quorum: bool,
        quorum_ack_age: Duration,
    ) {
        let was_valid = self.is_valid();
        self.is_leader = is_leader;

        if is_leader && has_quorum {
            self.renew(quorum_ack_age);
        } else {
            // Lost leadership or quorum - immediate expiry
            let now = self.clock.now();
            self.expires_at = now.checked_sub(Duration::from_nanos(1)).unwrap_or(now);
        }

        let is_valid_now = self.is_valid();
        if is_valid_now != was_valid {
            if is_valid_now {
                tracing::info!(
                    is_leader = is_leader,
                    has_quorum = has_quorum,
                    "Lease granted"
                );
            } else {
                tracing::warn!(
                    is_leader = is_leader,
                    has_quorum = has_quorum,
                    "Lease EXPIRED"
                );
            }
        }
        metrics::gauge!("pgbattery_has_lease").set(f64::from(u8::from(is_valid_now)));
    }

    /// Force lease expiry (for testing or emergency fence)
    pub fn expire(&mut self) {
        let now = self.clock.now();
        self.expires_at = now.checked_sub(Duration::from_nanos(1)).unwrap_or(now);
        tracing::warn!("Lease forcibly expired");
    }

    /// Get remaining lease time (for metrics/debugging)
    #[must_use]
    pub fn remaining(&self) -> Duration {
        let now = self.clock.now();
        if now < self.expires_at {
            self.expires_at - now
        } else {
            Duration::ZERO
        }
    }

    /// Check if this node thinks it's the Raft leader
    /// (Note: leadership without valid lease = no write authority)
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// The instant past which the lease is certainly invalid — an upper
    /// bound on this node's write authority, for callers (e.g. the gateway)
    /// that budget work against the lease deadline instead of polling
    /// [`Self::is_valid`]. Losing leadership or quorum rewinds this deadline
    /// into the past via [`Self::update_from_raft`], so the deadline never
    /// outlives authority; a snapshot of it is an upper bound, not a grant —
    /// re-check `is_valid` before any safety-relevant action.
    #[must_use]
    pub const fn valid_until(&self) -> Instant {
        self.expires_at
    }
}

impl Default for LeaseState {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared lease state (thread-safe)
pub type SharedLeaseState = Arc<RwLock<LeaseState>>;

/// Create a new shared lease state with the system clock.
#[must_use]
pub fn new_shared_lease() -> SharedLeaseState {
    Arc::new(RwLock::new(LeaseState::new()))
}

/// Create a shared lease with custom duration (system clock).
#[must_use]
pub fn new_shared_lease_with_duration(duration: Duration) -> SharedLeaseState {
    Arc::new(RwLock::new(LeaseState::with_duration(duration)))
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
    use parking_lot::Mutex;

    /// Manually-advanced clock used by lease tests. Anchored at
    /// `Instant::now()` once at construction; `advance` bumps the
    /// returned `Instant` forward. Interior-mutable so a test can
    /// share it with `LeaseState` (which owns an `Arc<dyn Clock>`)
    /// and still advance it.
    struct ManualClock {
        anchor: Instant,
        offset: Mutex<Duration>,
    }

    impl ManualClock {
        fn new() -> Self {
            Self {
                anchor: Instant::now(),
                offset: Mutex::new(Duration::ZERO),
            }
        }

        fn advance(&self, by: Duration) {
            *self.offset.lock() += by;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            self.anchor + *self.offset.lock()
        }
    }

    #[test]
    fn test_lease_starts_expired() {
        let lease = LeaseState::new();
        assert!(!lease.is_valid());
        assert_eq!(lease.remaining(), Duration::ZERO);
    }

    #[test]
    fn test_lease_renewal() {
        let mut lease = LeaseState::new();
        lease.update_from_raft(true, true, Duration::ZERO);
        assert!(lease.is_valid());
        assert!(lease.remaining() > Duration::from_millis(1900));
    }

    #[test]
    fn test_lease_expiry_deterministic() {
        let clock = Arc::new(ManualClock::new());
        let mut lease =
            LeaseState::with_clock_and_duration(clock.clone(), Duration::from_millis(100));

        lease.update_from_raft(true, true, Duration::ZERO);
        assert!(lease.is_valid());

        // Advance past expiry — no sleep, no flake.
        clock.advance(Duration::from_millis(150));
        assert!(!lease.is_valid());
    }

    /// A renewal must anchor the lease on the actual quorum-ack instant, not on
    /// `now`. Otherwise a leader whose last quorum contact is already stale gets
    /// a fresh full lease, and worst-case write authority becomes
    /// `quorum_staleness + duration` instead of just `duration` — the
    /// stale-leader / split-brain window the lease exists to bound.
    #[test]
    fn test_renew_anchors_on_quorum_ack_not_now() {
        let clock = Arc::new(ManualClock::new());
        let mut lease = LeaseState::with_clock_and_duration(clock.clone(), Duration::from_secs(2));

        // Quorum was last acknowledged 900ms ago.
        lease.update_from_raft(true, true, Duration::from_millis(900));
        assert!(lease.is_valid());

        // Authority must end ~1100ms (2000 - 900) from now, NOT a full 2000ms.
        assert!(
            lease.remaining() <= Duration::from_millis(1100),
            "lease must not extend past duration from the real quorum ack; got {:?}",
            lease.remaining()
        );

        // Past the anchored expiry the lease must be invalid.
        clock.advance(Duration::from_millis(1101));
        assert!(!lease.is_valid());
    }

    /// The lease outlives the Raft election timeout, so a crash failover can
    /// elect a new leader while the deposed leader's lease — anchored at its
    /// last quorum ack — is still valid. Winning an election is therefore
    /// never proof that the old lease has expired, which is why
    /// `App::promote_local_postgres` holds promotion down for one full
    /// `DEFAULT_LEASE_DURATION` from local leader-loss detection. If this
    /// ordering ever flips (lease ≤ election timeout), the hold-down becomes
    /// redundant rather than wrong — revisit it, don't just relax this test.
    #[test]
    fn test_lease_outlives_election_timeout() {
        let lease_ms = u64::try_from(DEFAULT_LEASE_DURATION.as_millis()).unwrap();
        let election_ms = crate::config::constants::DEFAULT_ELECTION_TIMEOUT_MS;
        assert!(
            lease_ms > election_ms,
            "lease ({lease_ms} ms) outlives the election timeout ({election_ms} ms); \
             the promotion lease hold-down is load-bearing"
        );
    }

    #[test]
    fn test_lose_quorum() {
        let mut lease = LeaseState::new();
        lease.update_from_raft(true, true, Duration::ZERO);
        assert!(lease.is_valid());

        lease.update_from_raft(true, false, Duration::ZERO);
        assert!(!lease.is_valid());
    }

    #[test]
    fn test_lose_leadership() {
        let mut lease = LeaseState::new();
        lease.update_from_raft(true, true, Duration::ZERO);
        assert!(lease.is_valid());

        lease.update_from_raft(false, true, Duration::ZERO);
        assert!(!lease.is_valid());
    }
}
