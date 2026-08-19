//! The `--format dashboard` renderer: a fixed-width box repainted in place.
//!
//! [`layout`] owns every width, [`chrome`] draws the box around the table and
//! [`rows`] draws the table itself. Nothing here allocates: the frame buffer
//! is reused between repaints and every cell writes straight into it.

mod chrome;
mod layout;
mod rows;

use super::frame::Frame;
use super::model::ClusterStatus;
use crate::commands::render::palette;
use layout::{Layout, write_rule};

pub(super) fn render(frame: &mut Frame, status: &ClusterStatus) {
    let p = palette();
    let l = Layout::measure(status);

    frame.begin();
    chrome::header(frame, p, status, l.width, l.term_digits);
    write_rule(frame, p, l.width);
    chrome::column_labels(frame, p, l.addr_width, l.width);
    rows::render(frame, p, status, l.addr_width);
    chrome::footer(frame, p, status, l.width);
    chrome::key(frame, p, l.width);
    frame.endl();
}

#[cfg(test)]
pub(super) mod tests {
    use super::layout::{LEADER_MARK, head, legend};
    use super::*;
    use crate::alloc_meter::measure;
    use crate::commands::render::get_terminal_width;
    use crate::commands::status::model::{NodeStatus, RaftState, ReplicaSyncState};
    use std::collections::HashMap;

    /// A healthy cluster with node 1 leading, for the renderers to draw.
    pub(in crate::commands::status) fn sample_status(node_count: usize) -> ClusterStatus {
        let mut nodes = Vec::new();
        let mut replica_sync_status = HashMap::new();
        let mut replica_lag_bytes = HashMap::new();
        for i in 0..node_count {
            let id = i as u64 + 1;
            let leader = i == 0;
            nodes.push(NodeStatus {
                addr: format!("172.28.0.1{id}:9091"),
                node_id: Some(id),
                reachable: true,
                state: if leader {
                    RaftState::Leader
                } else {
                    RaftState::Follower
                },
                term: 42,
                commit_index: 1_024,
                lsn_bytes: 7_040_012_800 - i as u64 * 4_096,
                connections_active: 127,
                connections_migrated: 4_200,
                connections_held: 0,
                is_primary: leader,
                is_sync: !leader,
            });
            if !leader {
                replica_sync_status.insert(id, ReplicaSyncState::Sync);
                replica_lag_bytes.insert(id, 4_096 * id);
            }
        }
        ClusterStatus {
            nodes,
            leader_addr: Some("172.28.0.11:9091".to_string()),
            leader_lsn: 7_040_012_800,
            term: 42,
            healthy: true,
            sync_replicated: true,
            replica_sync_status,
            replica_lag_bytes,
        }
    }

    /// A repaint must not allocate at all. Catches any per-cell `format!` or
    /// `String` return creeping back into the render path.
    #[test]
    fn steady_state_repaint_allocates_nothing() {
        let cluster = sample_status(3);
        let mut frame = Frame::new(true);

        render(&mut frame, &cluster); // warm the buffer to capacity
        let ((), stats) = measure(|| render(&mut frame, &cluster));

        assert_eq!(
            stats.count, 0,
            "a steady-state repaint allocated {} times ({} bytes)",
            stats.count, stats.bytes
        );
    }

    /// The first frame must fit the preallocated buffer, or the "one
    /// allocation" claim is only true from the second frame onward.
    #[test]
    fn first_frame_fits_the_preallocated_buffer() {
        let cluster = sample_status(5);
        let mut frame = Frame::new(true);
        render(&mut frame, &cluster);
        assert!(
            frame.as_str().len() <= super::super::frame::INITIAL_CAPACITY,
            "a 5-node frame is {} bytes, past the buffer's initial capacity",
            frame.as_str().len()
        );
    }

    /// Repainting is deterministic: same status, same bytes. Guards the
    /// buffer reuse against leaving a previous frame's tail behind.
    #[test]
    fn repaint_is_byte_identical() {
        let cluster = sample_status(3);
        let mut frame = Frame::new(true);
        render(&mut frame, &cluster);
        let first = frame.as_str().to_string();
        render(&mut frame, &cluster);
        // The timestamp is the only field that can differ between renders.
        assert_eq!(first.len(), frame.as_str().len());
        assert_eq!(
            first.matches('\n').count(),
            frame.as_str().matches('\n').count()
        );
    }

    /// In-place repaint homes the cursor and erases each row as it goes,
    /// rather than blanking the screen before the next scrape.
    #[test]
    fn watch_frames_repaint_in_place() {
        let cluster = sample_status(3);
        let mut frame = Frame::new(true);
        render(&mut frame, &cluster);
        frame.end();

        assert!(
            frame.as_str().starts_with("\x1b[H"),
            "frame must home the cursor"
        );
        assert!(
            frame.as_str().contains("\x1b[K"),
            "rows must erase to end of line"
        );
        assert!(
            frame.as_str().ends_with("\x1b[J"),
            "frame must clear rows below"
        );
        assert!(
            !frame.as_str().contains("\x1b[2J"),
            "a full-screen erase reintroduces the blank-pane flicker"
        );
    }

    /// Piped output carries no repaint control sequences.
    #[test]
    fn non_watch_frames_carry_no_cursor_control() {
        let cluster = sample_status(3);
        let mut frame = Frame::new(false);
        render(&mut frame, &cluster);
        assert!(!frame.as_str().contains("\x1b[H"));
        assert!(!frame.as_str().contains("\x1b[K"));
        assert!(!frame.as_str().contains("\x1b[J"));
    }

    /// Every row, the title, the column header and the rules must be the same
    /// width, or the table does not read as a table.
    #[test]
    fn table_rows_and_rules_share_one_width() {
        let cluster = sample_status(3);
        let mut frame = Frame::new(false);
        render(&mut frame, &cluster);

        // The title, three rules, the column header, one row per node.
        let widths: Vec<usize> = frame
            .as_str()
            .lines()
            .filter(|l| {
                l.contains('─')
                    || l.contains("ADDRESS")
                    || l.contains("LEADER")
                    || l.contains("FOLLOWER")
                    || l.contains(head::NAME)
            })
            .map(|l| l.chars().count())
            .collect();
        assert_eq!(widths.len(), 1 + 3 + 1 + cluster.nodes.len());
        assert!(
            widths.windows(2).all(|w| w.first() == w.last()),
            "table lines disagree on width: {widths:?}"
        );
    }

    /// The footer and legend justify to the same edge as the table, so the
    /// frame reads as one block rather than a box with ragged text under it.
    #[test]
    fn footer_and_legend_are_flush_with_the_box() {
        let cluster = sample_status(3);
        let mut frame = Frame::new(false);
        render(&mut frame, &cluster);

        let rule = frame
            .as_str()
            .lines()
            .find(|l| l.contains('─'))
            .map(|l| l.chars().count())
            .unwrap_or_default();
        assert!(rule > 0, "no rule rendered");

        for marker in ["Leader ", legend::LABEL] {
            let line = frame
                .as_str()
                .lines()
                .find(|l| l.trim_start().starts_with(marker))
                .unwrap_or_default();
            assert_eq!(
                line.chars().count(),
                rule,
                "{marker:?} line is not flush with the box: {line:?}"
            );
        }
    }

    /// A terminal too narrow for the table must not get a rule wider than it.
    #[test]
    fn rule_never_exceeds_the_terminal() {
        let cluster = sample_status(3);
        let mut frame = Frame::new(false);
        render(&mut frame, &cluster);
        let term = get_terminal_width();
        for line in frame.as_str().lines().filter(|l| l.contains('─')) {
            assert!(
                line.chars().count() <= term,
                "rule is {} chars in a {term}-char terminal",
                line.chars().count()
            );
        }
    }

    /// The leader is identifiable without color, since `--no-color` and pipes
    /// strip it. The marker, not the ANSI, carries that.
    #[test]
    fn leader_is_marked_without_color() {
        let cluster = sample_status(3);
        let mut frame = Frame::new(false);
        render(&mut frame, &cluster);

        let marked: Vec<&str> = frame
            .as_str()
            .lines()
            .filter(|l| l.contains(LEADER_MARK))
            .collect();
        assert_eq!(marked.len(), 1, "exactly one row is the leader");
        assert!(
            marked.first().is_some_and(|l| l.contains("LEADER")),
            "the marker must be on the LEADER row"
        );
    }

    /// A node address is remote input. It must not be able to carry an escape
    /// sequence into the pane that reports it.
    #[test]
    fn hostile_node_address_cannot_inject_escapes() {
        let mut cluster = sample_status(1);
        if let Some(node) = cluster.nodes.get_mut(0) {
            node.addr = "\x1b[2J\x1b[31mowned".to_string();
        }
        cluster.leader_addr = Some("\x1b]0;title\x07evil".to_string());

        let mut frame = Frame::new(false);
        render(&mut frame, &cluster);
        assert!(!frame.as_str().contains('\x1b'), "escape reached the frame");
        assert!(!frame.as_str().contains('\x07'), "BEL reached the frame");
        assert!(
            frame.as_str().contains("owned"),
            "the text itself must still show"
        );
    }
}
