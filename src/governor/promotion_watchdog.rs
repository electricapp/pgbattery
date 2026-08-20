//! Watchdog for a leader whose `PostgreSQL` never becomes primary.
//!
//! Raft leadership and a serving data plane are separate facts, and this node
//! can hold the first without ever reaching the second: a standby that has not
//! reached a consistent recovery state refuses connections, so
//! `pg_is_in_recovery()` cannot be probed, `promote` fails closed, and the
//! reconcile loop retries it identically forever. Raft stays healthy the whole
//! time — the node heartbeats, `/api/v1/cluster/leader` publishes it, every
//! liveness check reads green — and only writes are gone.
//!
//! Nothing the node can read locally names the cause. `pg_controldata` reports
//! `in production` for a stuck standby, and the connection error is the same
//! one `PostgreSQL` gives for the second or two of any normal start, so a guard
//! keyed on either would either lie or fire on every restart (`HARDENING.md`
//! H-53 records both experiments). What is left is the weaker statement that
//! does not need a cause: **this node has led for longer than any legitimate
//! promotion could take and its `PostgreSQL` is still not primary.** A wedged
//! recovery, a crashed postmaster and a hung start all want the same remedy,
//! which is for a peer that can serve to lead instead.

use std::time::{Duration, Instant};

use pgbattery_core::types::NodeId;

/// What one reconcile pass saw of this node's leadership and its `PostgreSQL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipObservation {
    /// `RaftMetrics::current_leader == Some(self)`, the canonical truth source.
    pub is_raft_leader: bool,
    /// Whether `PostgreSQL` is primary; `None` when the probe itself failed.
    ///
    /// Kept distinct from `Some(false)` for the log line only. Both mean the
    /// data plane is not serving writes, and the wedge's own signature is a
    /// probe that cannot complete.
    pub pg_primary: Option<bool>,
}

impl LeadershipObservation {
    /// What a pass that found this node is not the leader reports.
    #[must_use]
    pub const fn not_leading() -> Self {
        Self {
            is_raft_leader: false,
            pg_primary: None,
        }
    }
}

/// What the watchdog wants done about the observation it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WedgeVerdict {
    /// Leadership and the data plane agree, or have not disagreed long enough.
    Hold,
    /// Led without a primary past the bound: hand leadership to a peer.
    Yield,
}

/// Whether a leader that is not serving has been that way long enough to yield.
///
/// Separate from [`PromotionWatchdog`] so the rule can be read and tested
/// without a clock. Every argument is a duration rather than an instant for the
/// same reason.
#[must_use]
pub fn wedge_verdict(
    leader_for: Option<Duration>,
    pg_primary: Option<bool>,
    bound: Duration,
    since_last_yield: Option<Duration>,
    cooldown: Duration,
) -> WedgeVerdict {
    // Not the leader: nothing to yield, and nothing has diverged.
    let Some(leader_for) = leader_for else {
        return WedgeVerdict::Hold;
    };
    // The only observation that clears the watchdog. An unreadable probe does
    // not, because that is what the wedge looks like from in here.
    if pg_primary == Some(true) {
        return WedgeVerdict::Hold;
    }
    if leader_for < bound {
        return WedgeVerdict::Hold;
    }
    // A yield costs the cluster a lease drain, and a target that could not take
    // over a moment ago usually still cannot. Waiting between attempts is what
    // keeps a cluster where nobody can serve from spending its whole time in
    // handoffs instead of recovering.
    if since_last_yield.is_some_and(|since| since < cooldown) {
        return WedgeVerdict::Hold;
    }
    WedgeVerdict::Yield
}

/// A peer considered as the destination for a yield.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YieldCandidate {
    pub node_id: NodeId,
    /// The peer's last reported WAL position.
    pub lsn: u64,
    /// `ClusterState::is_lsn_acceptable_for_promotion`, which fails closed on a
    /// peer with no fresh report.
    pub acceptable: bool,
}

/// The peer to hand leadership to: the acceptable one holding the most WAL.
///
/// `None` when no peer qualifies, which is a cluster where yielding would only
/// move the outage. The transfer path re-checks all of this on the way through
/// and can still refuse; this only decides who to ask.
#[must_use]
pub fn choose_yield_target(candidates: &[YieldCandidate]) -> Option<NodeId> {
    candidates
        .iter()
        .filter(|c| c.acceptable)
        // Ties broken by the lower node id so a cluster with two equally
        // caught-up peers asks the same one each round rather than alternating.
        .min_by_key(|c| (std::cmp::Reverse(c.lsn), c.node_id))
        .map(|c| c.node_id)
}

/// Tracks how long this node has been Raft leader without a primary.
///
/// `leader_since` is re-derived from `RaftMetrics` on every pass — set on the
/// not-leader to leader edge and cleared the moment leadership goes away —
/// rather than being written once at a transition and trusted afterwards. It is
/// the same discipline `ReplicationManager::leader_since` follows, and it is
/// what makes the anchor survive the restart that `failover_started_at` does
/// not: a node that comes back up and wins an election has a fresh edge, and
/// the wedge this exists to catch is reached exactly that way.
#[derive(Debug, Default)]
pub struct PromotionWatchdog {
    leader_since: Option<Instant>,
    last_yield: Option<Instant>,
}

impl PromotionWatchdog {
    /// Fold one reconcile pass into the watchdog and ask what to do.
    ///
    /// `now` comes from the lease clock, so a test that moves time moves this
    /// with it and a clock skew that the lease honours is honoured here too.
    pub fn observe(&mut self, obs: LeadershipObservation, now: Instant) -> WedgeVerdict {
        if !obs.is_raft_leader {
            self.leader_since = None;
            return WedgeVerdict::Hold;
        }
        let since = *self.leader_since.get_or_insert(now);
        let verdict = wedge_verdict(
            Some(now.saturating_duration_since(since)),
            obs.pg_primary,
            Duration::from_millis(crate::config::constants::LEADER_WITHOUT_PRIMARY_YIELD_MS),
            self.last_yield
                .map(|last| now.saturating_duration_since(last)),
            Duration::from_millis(crate::config::constants::LEADER_YIELD_COOLDOWN_MS),
        );
        if verdict == WedgeVerdict::Yield {
            self.last_yield = Some(now);
        }
        verdict
    }

    /// How long this node has been Raft leader, for the log line that reports a
    /// yield. `None` while not leader.
    #[must_use]
    pub fn leader_for(&self, now: Instant) -> Option<Duration> {
        self.leader_since
            .map(|since| now.saturating_duration_since(since))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOUND: Duration = Duration::from_mins(1);
    const A_MILLISECOND: Duration = Duration::from_millis(1);
    const COOLDOWN: Duration = Duration::from_secs(15);

    fn verdict(leader_for: Option<Duration>, pg_primary: Option<bool>) -> WedgeVerdict {
        wedge_verdict(leader_for, pg_primary, BOUND, None, COOLDOWN)
    }

    #[test]
    fn a_follower_never_yields() {
        assert_eq!(verdict(None, None), WedgeVerdict::Hold);
        assert_eq!(verdict(None, Some(false)), WedgeVerdict::Hold);
    }

    #[test]
    fn a_leader_serving_writes_never_yields() {
        assert_eq!(
            verdict(Some(Duration::from_hours(1)), Some(true)),
            WedgeVerdict::Hold
        );
    }

    #[test]
    fn a_promotion_in_progress_is_given_its_time() {
        assert_eq!(
            verdict(Some(BOUND.saturating_sub(A_MILLISECOND)), Some(false)),
            WedgeVerdict::Hold,
        );
    }

    #[test]
    fn a_leader_that_never_became_primary_yields() {
        assert_eq!(verdict(Some(BOUND), Some(false)), WedgeVerdict::Yield);
    }

    #[test]
    fn an_unreadable_probe_is_the_wedges_own_signature_and_yields() {
        assert_eq!(verdict(Some(BOUND), None), WedgeVerdict::Yield);
    }

    #[test]
    fn a_recent_yield_holds_until_the_cooldown_passes() {
        assert_eq!(
            wedge_verdict(
                Some(BOUND),
                None,
                BOUND,
                Some(COOLDOWN.saturating_sub(A_MILLISECOND)),
                COOLDOWN
            ),
            WedgeVerdict::Hold,
        );
        assert_eq!(
            wedge_verdict(Some(BOUND), None, BOUND, Some(COOLDOWN), COOLDOWN),
            WedgeVerdict::Yield,
        );
    }

    #[test]
    fn losing_leadership_restarts_the_clock() {
        let mut watchdog = PromotionWatchdog::default();
        let start = Instant::now();
        let leading = LeadershipObservation {
            is_raft_leader: true,
            pg_primary: Some(false),
        };
        assert_eq!(watchdog.observe(leading, start), WedgeVerdict::Hold);
        // Deposed, then leader again: the second reign is measured from itself,
        // so the first reign's age cannot make it yield immediately.
        assert_eq!(
            watchdog.observe(LeadershipObservation::not_leading(), start + BOUND),
            WedgeVerdict::Hold
        );
        assert_eq!(watchdog.observe(leading, start + BOUND), WedgeVerdict::Hold);
        assert_eq!(
            watchdog.observe(leading, start + BOUND + BOUND),
            WedgeVerdict::Yield
        );
    }

    #[test]
    fn becoming_primary_ends_it_without_a_yield() {
        let mut watchdog = PromotionWatchdog::default();
        let start = Instant::now();
        assert_eq!(
            watchdog.observe(
                LeadershipObservation {
                    is_raft_leader: true,
                    pg_primary: None,
                },
                start,
            ),
            WedgeVerdict::Hold
        );
        assert_eq!(
            watchdog.observe(
                LeadershipObservation {
                    is_raft_leader: true,
                    pg_primary: Some(true),
                },
                start + BOUND + BOUND,
            ),
            WedgeVerdict::Hold
        );
    }

    #[test]
    fn the_watchdog_waits_a_cooldown_between_yields() {
        let mut watchdog = PromotionWatchdog::default();
        let start = Instant::now();
        let wedged = LeadershipObservation {
            is_raft_leader: true,
            pg_primary: None,
        };
        let bound =
            Duration::from_millis(crate::config::constants::LEADER_WITHOUT_PRIMARY_YIELD_MS);
        let cooldown = Duration::from_millis(crate::config::constants::LEADER_YIELD_COOLDOWN_MS);
        // The first pass is the not-leader to leader edge, so it anchors the
        // reign rather than measuring one.
        assert_eq!(watchdog.observe(wedged, start), WedgeVerdict::Hold);
        assert_eq!(watchdog.observe(wedged, start + bound), WedgeVerdict::Yield);
        assert_eq!(
            watchdog.observe(
                wedged,
                start + bound + cooldown.saturating_sub(A_MILLISECOND)
            ),
            WedgeVerdict::Hold,
        );
        assert_eq!(
            watchdog.observe(wedged, start + bound + cooldown),
            WedgeVerdict::Yield,
        );
    }

    fn candidate(node_id: NodeId, lsn: u64, acceptable: bool) -> YieldCandidate {
        YieldCandidate {
            node_id,
            lsn,
            acceptable,
        }
    }

    #[test]
    fn no_acceptable_peer_is_nobody_to_yield_to() {
        assert_eq!(choose_yield_target(&[]), None);
        assert_eq!(
            choose_yield_target(&[candidate(2, 900, false), candidate(3, 800, false)]),
            None
        );
    }

    #[test]
    fn the_furthest_ahead_acceptable_peer_wins() {
        assert_eq!(
            choose_yield_target(&[candidate(2, 100, true), candidate(3, 900, true)]),
            Some(3)
        );
    }

    #[test]
    fn a_peer_behind_on_wal_is_not_chosen_for_being_ahead_of_nobody() {
        // The unacceptable peer holds more WAL, and is still not the target:
        // the promotion gate, not the raw position, is what decides.
        assert_eq!(
            choose_yield_target(&[candidate(2, 5_000, false), candidate(3, 10, true)]),
            Some(3)
        );
    }

    #[test]
    fn equally_caught_up_peers_are_asked_in_a_stable_order() {
        let peers = [candidate(3, 500, true), candidate(2, 500, true)];
        assert_eq!(choose_yield_target(&peers), Some(2));
        assert_eq!(choose_yield_target(&peers), Some(2));
    }
}
