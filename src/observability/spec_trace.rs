//! Spec trace: the transitions `tla/lease_fencing.tla` and `tla/raft_lsn.tla`
//! model, recorded where they happen so `testing/trace_check.py` can check a
//! real run against the specs.
//!
//! Events go to the debug-event buffer under [`EVENT_TYPE`] and are read
//! through `GET /debug/events`. The buffer's sequence numbers are per process
//! and contiguous, so a reader can tell an evicted event from one never
//! recorded. Recording never blocks on anything but the buffer's own lock and
//! decides nothing.

use std::sync::OnceLock;

use serde::Serialize;

use super::debug_events::DebugEventBuffer;

/// The debug-event type every spec event is recorded under.
pub const EVENT_TYPE: &str = "spec";

/// One modeled transition, as this node saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpecEvent {
    /// This node's Raft term or the leader it sees changed (`RaftMetrics`).
    Raft { term: u64, leader: Option<u64> },
    /// The failover anchor was stamped (or re-stamped) or spent.
    Anchor { stamped: bool },
    /// The write gate admitted an open (`EnablePgWrites`), recorded before
    /// the open is applied and before it spends the anchor.
    Open,
    /// The write state a fence tick left: `PostgreSQL`'s client writability
    /// (`None` when the probe could not tell) and the lease it acted on.
    Writes {
        writable: Option<bool>,
        lease_valid: bool,
        lease_leader: bool,
    },
    /// A vote this node answered, with the inputs of its WAL gate.
    Vote {
        term: u64,
        candidate: u64,
        granted: bool,
        candidate_lsn: Option<u64>,
        voter_wal: VoterWal,
        threshold: u64,
    },
}

/// The voter's own WAL position as its gate read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VoterWal {
    At(u64),
    Empty,
    Unknown,
}

static SINK: OnceLock<(u64, DebugEventBuffer)> = OnceLock::new();

/// Route this process's spec events into `events`. The first call wins.
pub fn install(node_id: u64, events: DebugEventBuffer) {
    if SINK.set((node_id, events)).is_err() {
        tracing::debug!("spec trace already installed");
    }
}

/// Record `event` if a sink is installed.
pub fn record(event: SpecEvent) {
    if let Some((node_id, events)) = SINK.get()
        && let Ok(data) = serde_json::to_value(event)
    {
        events.record(*node_id, EVENT_TYPE, data);
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
    fn events_serialize_as_the_checker_parses_them() {
        let vote = serde_json::to_value(SpecEvent::Vote {
            term: 4,
            candidate: 2,
            granted: true,
            candidate_lsn: Some(100),
            voter_wal: VoterWal::At(90),
            threshold: 0,
        })
        .unwrap();
        assert_eq!(
            vote,
            serde_json::json!({
                "kind": "vote", "term": 4, "candidate": 2, "granted": true,
                "candidate_lsn": 100, "voter_wal": {"at": 90}, "threshold": 0,
            })
        );
        let writes = serde_json::to_value(SpecEvent::Writes {
            writable: None,
            lease_valid: false,
            lease_leader: true,
        })
        .unwrap();
        assert_eq!(
            writes,
            serde_json::json!({
                "kind": "writes", "writable": null, "lease_valid": false, "lease_leader": true,
            })
        );
        assert_eq!(
            serde_json::to_value(VoterWal::Empty).unwrap(),
            serde_json::json!("empty")
        );
    }
}
