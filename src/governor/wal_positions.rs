//! Every member's WAL position as this node last heard it: soft state, never
//! replicated.
//!
//! A node records its own position from the reconciler's probe; the leader
//! also records each follower's from its reports. Elections do not read this:
//! a vote carries the candidate's live position and the voter compares its
//! own (`network::vote_lsn_gate`). What reads it is the leader deciding who
//! may lead next — a transfer target, a learner's promotion, a yield — and
//! the lag endpoint. A new leader learns positions within a report interval.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use super::state_machine::NodeId;

/// A report older than this says nothing about where the node is now.
pub const POSITION_FRESHNESS: Duration = Duration::from_secs(10);

/// This node's book of WAL positions.
#[derive(Debug, Default)]
pub struct WalPositions {
    reports: RwLock<HashMap<NodeId, (u64, Instant)>>,
}

/// Shared handle.
pub type SharedWalPositions = std::sync::Arc<WalPositions>;

impl WalPositions {
    /// Record `node`'s position as of `at`.
    pub fn record(&self, node: NodeId, lsn: u64, at: Instant) {
        self.reports.write().insert(node, (lsn, at));
    }

    /// `node`'s position if reported within [`POSITION_FRESHNESS`] of `now`.
    #[must_use]
    pub fn fresh(&self, node: NodeId, now: Instant) -> Option<u64> {
        self.reports
            .read()
            .get(&node)
            .filter(|(_, at)| now.saturating_duration_since(*at) < POSITION_FRESHNESS)
            .map(|(lsn, _)| *lsn)
    }

    /// Whether `candidate` holds the WAL every fresh report says the cluster
    /// has, within `threshold`. Fails closed on a candidate with no fresh
    /// report: nothing proves it caught up.
    #[must_use]
    pub fn caught_up(
        &self,
        candidate: NodeId,
        threshold: u64,
        now: Instant,
    ) -> (bool, &'static str) {
        let Some(at) = self.fresh(candidate, now) else {
            return (false, "candidate has no fresh WAL position report");
        };
        let max = self
            .reports
            .read()
            .values()
            .filter(|(_, reported)| now.saturating_duration_since(*reported) < POSITION_FRESHNESS)
            .map(|(lsn, _)| *lsn)
            .max()
            .unwrap_or(at);
        if at.saturating_add(threshold) >= max {
            (true, "candidate within threshold of every fresh report")
        } else {
            (
                false,
                "candidate behind the cluster's WAL beyond the threshold",
            )
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "test code asserts on known-good values and panics are the failure signal"
)]
mod tests {
    use super::*;

    #[test]
    fn a_candidate_is_caught_up_within_the_threshold_of_every_fresh_report() {
        let book = WalPositions::default();
        let now = Instant::now();
        book.record(1, 10_000, now);
        book.record(2, 9_000, now);
        book.record(3, 5_000, now);
        assert!(book.caught_up(2, 1_000, now).0);
        assert!(!book.caught_up(3, 1_000, now).0);
    }

    #[test]
    fn stale_reports_neither_qualify_nor_raise_the_bar() {
        let book = WalPositions::default();
        let now = Instant::now();
        let old = now.checked_sub(POSITION_FRESHNESS * 2).unwrap();
        book.record(2, 1_000_000, old);
        book.record(3, 5_000, now);
        assert!(
            !book.caught_up(2, 0, now).0,
            "a stale report proves nothing"
        );
        assert!(
            book.caught_up(3, 0, now).0,
            "a stale high mark binds nobody"
        );
    }
}
