//! Debug event buffer for state transitions.
//!
//! Stores the last N significant events for debugging and chaos testing.
//! Events are stored in a lock-free ring buffer and exposed via /debug/events.

use parking_lot::RwLock;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum number of events to retain.
const MAX_EVENTS: usize = 1_000;

/// A debug event representing a state transition.
#[derive(Debug, Clone, Serialize)]
pub struct DebugEvent {
    /// Monotonic sequence number.
    pub seq: u64,
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Event type (e.g., `leader_change`, `fence`, `membership`).
    pub event_type: String,
    /// Node ID where event occurred.
    pub node_id: u64,
    /// Event-specific data.
    pub data: serde_json::Value,
}

/// Thread-safe event buffer.
#[derive(Clone, Debug)]
pub struct DebugEventBuffer {
    inner: Arc<RwLock<EventBufferInner>>,
}

#[derive(Debug)]
struct EventBufferInner {
    events: VecDeque<DebugEvent>,
    next_seq: u64,
}

impl Default for DebugEventBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl DebugEventBuffer {
    /// Create a new event buffer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(EventBufferInner {
                events: VecDeque::with_capacity(MAX_EVENTS),
                next_seq: 1,
            })),
        }
    }

    /// Record a new event.
    pub fn record(&self, node_id: u64, event_type: impl Into<String>, data: serde_json::Value) {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

        let mut inner = self.inner.write();
        let seq = inner.next_seq;
        inner.next_seq = inner.next_seq.saturating_add(1);

        let event = DebugEvent {
            seq,
            timestamp_ms,
            event_type: event_type.into(),
            node_id,
            data,
        };

        // Evict oldest if at capacity
        if inner.events.len() >= MAX_EVENTS {
            inner.events.pop_front();
        }
        inner.events.push_back(event);
    }

    /// Get events since a given sequence number.
    #[must_use]
    pub fn get_since(&self, since_seq: u64) -> Vec<DebugEvent> {
        let inner = self.inner.read();
        inner
            .events
            .iter()
            .filter(|e| e.seq > since_seq)
            .cloned()
            .collect()
    }

    /// Get the last N events.
    #[must_use]
    pub fn get_last(&self, n: usize) -> Vec<DebugEvent> {
        let inner = self.inner.read();
        inner.events.iter().rev().take(n).cloned().collect()
    }

    /// Get current sequence number (for polling).
    #[must_use]
    pub fn current_seq(&self) -> u64 {
        self.inner.read().next_seq.saturating_sub(1)
    }
}

/// Helper macros for recording common events.
impl DebugEventBuffer {
    /// Record a leader change event.
    pub fn leader_change(&self, node_id: u64, old_leader: Option<u64>, new_leader: Option<u64>) {
        self.record(
            node_id,
            "leader_change",
            serde_json::json!({
                "old_leader": old_leader,
                "new_leader": new_leader,
            }),
        );
    }

    /// Record a fence state change.
    pub fn fence_change(&self, node_id: u64, fenced: bool, reason: &str) {
        self.record(
            node_id,
            "fence",
            serde_json::json!({
                "fenced": fenced,
                "reason": reason,
            }),
        );
    }

    /// Record a membership change.
    pub fn membership_change(&self, node_id: u64, voters: &[u64], learners: &[u64]) {
        self.record(
            node_id,
            "membership",
            serde_json::json!({
                "voters": voters,
                "learners": learners,
            }),
        );
    }

    /// Record sync replication state change.
    pub fn sync_state_change(&self, node_id: u64, sync_standbys: &str, has_quorum: bool) {
        self.record(
            node_id,
            "sync_state",
            serde_json::json!({
                "sync_standbys": sync_standbys,
                "has_quorum": has_quorum,
            }),
        );
    }

    /// Record a connection migration.
    pub fn connection_migrated(&self, node_id: u64, conn_id: u64, new_leader: &str) {
        self.record(
            node_id,
            "connection_migrated",
            serde_json::json!({
                "conn_id": conn_id,
                "new_leader": new_leader,
            }),
        );
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
    fn test_event_buffer_basic() {
        let buf = DebugEventBuffer::new();
        buf.leader_change(1, None, Some(1));
        buf.fence_change(1, false, "has_quorum");

        let events = buf.get_last(10);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "fence");
        assert_eq!(events[1].event_type, "leader_change");
    }

    #[test]
    fn test_event_buffer_since() {
        let buf = DebugEventBuffer::new();
        buf.leader_change(1, None, Some(1));
        let seq = buf.current_seq();
        buf.fence_change(1, false, "test");

        let events = buf.get_since(seq);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "fence");
    }

    #[test]
    fn test_event_buffer_capacity() {
        let buf = DebugEventBuffer::new();
        for i in 0..1500 {
            buf.record(1, "test", serde_json::json!({"i": i}));
        }

        let events = buf.get_last(2000);
        assert_eq!(events.len(), MAX_EVENTS);
    }
}
