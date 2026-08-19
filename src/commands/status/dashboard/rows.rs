//! The table body: one row per node, and the cells it is built from.

use std::fmt::Write as _;

use super::super::frame::Frame;
use super::super::model::{ClusterStatus, NodeStatus, RaftState, ReplicaSyncState};
use super::layout::{LEADER_MARK, col};
use crate::commands::render::{Palette, format_lag, format_lsn, write_field};

pub(super) fn render(frame: &mut Frame, p: &Palette, status: &ClusterStatus, addr_width: usize) {
    for (idx, node) in status.nodes.iter().enumerate() {
        let display_id = node.node_id.unwrap_or((idx as u64) + 1);
        let (role_color, role_text) = role(p, node);

        if node.reachable && node.state == RaftState::Leader {
            write!(frame, " {}{LEADER_MARK}{} ", p.green, p.reset).ok();
        } else {
            frame.push_str("   ");
        }
        write!(frame, "{display_id:>id$}  ", id = col::ID).ok();
        write_field(frame, &node.addr, addr_width).ok();
        write!(
            frame,
            "  {role_color}{role_text:<role$}{r}  ",
            r = p.reset,
            role = col::ROLE
        )
        .ok();
        write_sync(frame, p, status, node);
        frame.push_str("  ");
        write_lsn(frame, p, node);
        frame.push_str("  ");
        write_lag(frame, p, status, node);
        frame.push_str("  ");
        write_connections(frame, p, node);
        frame.endl();
    }
}

const fn role(p: &Palette, node: &NodeStatus) -> (&'static str, &'static str) {
    if !node.reachable {
        return (p.red, "UNREACHABLE");
    }
    match node.state {
        RaftState::Leader => (p.green, "LEADER"),
        RaftState::Follower => (p.cyan, "FOLLOWER"),
        RaftState::Learner => (p.yellow, "LEARNER"),
        RaftState::Candidate => (p.yellow, "CANDIDATE"),
        RaftState::Unknown => (p.red, "UNKNOWN"),
    }
}

fn write_sync(frame: &mut Frame, p: &Palette, status: &ClusterStatus, node: &NodeStatus) {
    let (color, text) = if !node.reachable || node.state == RaftState::Leader {
        (p.dim, "-")
    } else if let Some(actual_id) = node.node_id
        && let Some(&sync_state) = status.replica_sync_status.get(&actual_id)
    {
        match sync_state {
            ReplicaSyncState::Sync => (p.green, "SYNC"),
            ReplicaSyncState::Potential => (p.cyan, "READY"),
            ReplicaSyncState::Async => (p.red, "ASYNC"),
        }
    } else {
        (p.dim, "?")
    };
    write!(frame, "{color}{text:>w$}{}", p.reset, w = col::SYNC).ok();
}

fn write_lsn(frame: &mut Frame, p: &Palette, node: &NodeStatus) {
    if node.reachable && node.lsn_bytes > 0 {
        write!(frame, "{:>w$}", format_lsn(node.lsn_bytes), w = col::LSN).ok();
    } else {
        write!(frame, "{}{:>w$}{}", p.dim, "-", p.reset, w = col::LSN).ok();
    }
}

fn write_lag(frame: &mut Frame, p: &Palette, status: &ClusterStatus, node: &NodeStatus) {
    let w = col::LAG;
    if !node.reachable {
        write!(frame, "{}{:>w$}{}", p.dim, "-", p.reset).ok();
        return;
    }
    if node.state == RaftState::Leader {
        write!(frame, "{}{:>w$}{}", p.green, "HEAD", p.reset).ok();
        return;
    }

    // Primary source: leader's pg_stat_replication. Only includes replicas whose
    // walreceiver is currently streaming — a stuck or disconnected standby drops
    // out of this view and would otherwise silently show as "no lag info".
    let pg_stat_lag = node
        .node_id
        .and_then(|id| status.replica_lag_bytes.get(&id).copied());

    // Fallback: leader LSN minus the follower's self-reported LSN. Still works
    // when the follower has dropped out of pg_stat_replication, which is the
    // failure mode worth visualizing.
    let raft_lag = status
        .leader_lsn
        .checked_sub(node.lsn_bytes)
        .filter(|d| *d > 0);

    let Some(lag) = pg_stat_lag.or(raft_lag) else {
        write!(frame, "{}{:>w$}{}", p.dim, "-", p.reset).ok();
        return;
    };

    if lag == 0 {
        write!(frame, "{}{:>w$}{}", p.green, "0B", p.reset).ok();
        return;
    }
    let color = if lag > 1_000_000 {
        p.red
    } else if lag > 10_000 {
        p.yellow
    } else {
        p.white
    };
    write!(frame, "{color}{:>w$}{}", format_lag(lag), p.reset).ok();
}

fn write_connections(frame: &mut Frame, p: &Palette, node: &NodeStatus) {
    let w = col::CONNS;
    if !node.reachable {
        write!(frame, "{}{:>w$}{}", p.dim, "-", p.reset).ok();
    } else if node.connections_active > 0 {
        write!(frame, "{:>w$}", node.connections_active).ok();
    } else {
        write!(frame, "{}{:>w$}{}", p.dim, "0", p.reset).ok();
    }
}
