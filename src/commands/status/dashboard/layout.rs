//! Column widths and the arithmetic that fits them to a terminal.
//!
//! Every width the dashboard uses is declared here and the rule is derived
//! from them, so a column cannot be widened without the box following. All of
//! it is measured in characters — [`char_width`] rather than `str::len`, since
//! a single `·` in a separator would otherwise skew a column by one.

use super::super::frame::Frame;
use super::super::model::ClusterStatus;
use crate::commands::render::{Palette, char_width, get_terminal_width};
use std::fmt::Write as _;

/// Column widths, in characters.
pub(super) mod col {
    /// Leader marker. One glyph, one meaning, and it survives `--no-color`.
    pub(in super::super) const GUTTER: usize = 1;
    pub(in super::super) const ID: usize = 2;
    pub(in super::super) const ROLE: usize = 11;
    pub(in super::super) const SYNC: usize = 5;
    /// Fits `FFFFF/FFFFFFFF` — five high-word hex digits is 4 EB of WAL.
    pub(in super::super) const LSN: usize = 14;
    pub(in super::super) const LAG: usize = 8;
    pub(in super::super) const CONNS: usize = 5;

    /// The address column breathes to fit the addresses actually present.
    pub(in super::super) const ADDR_MIN: usize = 12;
    pub(in super::super) const ADDR_MAX: usize = 32;

    /// Gap between columns.
    pub(in super::super) const GAP: usize = 2;
    /// Gap between groups in the header and footer, which are prose.
    pub(in super::super) const GROUP: &str = "   ";

    /// Everything but the address column, gaps included.
    pub(in super::super) const FIXED: usize =
        GUTTER + 1 + ID + GAP + GAP + ROLE + GAP + SYNC + GAP + LSN + GAP + LAG + GAP + CONNS;
}

/// The leader's row marker.
pub(super) const LEADER_MARK: char = '▸';

/// Header text, with the widths needed to justify it to the box edge.
pub(super) mod head {
    use super::char_width;

    pub(in super::super) const NAME: &str = "pgbattery";
    pub(in super::super) const VERSION: &str = concat!("v", env!("CARGO_PKG_VERSION"));
    pub(in super::super) const TERM: &str = "Term ";
    pub(in super::super) const TIER: &str = "COMMUNITY";
    /// Every health badge is padded to this width so the fields after it do
    /// not shift as the badge changes.
    pub(in super::super) const BADGE: usize = 10;
    /// `YYYY-MM-DD HH:MM:SS UTC`.
    pub(in super::super) const STAMP: usize = 23;

    /// Identity and health, which sit at the left edge. Measured after the
    /// line indent, so it is comparable with the table's content width.
    pub(in super::super) const LEFT: usize =
        char_width(NAME) + 1 + char_width(VERSION) + char_width(super::col::GROUP) + BADGE;

    /// Metadata, which is right-aligned to the box edge. `term_digits` is the
    /// only part that varies.
    pub(in super::super) const fn right(term_digits: usize) -> usize {
        char_width(TERM)
            + term_digits
            + char_width(super::col::GROUP)
            + STAMP
            + char_width(super::col::GROUP)
            + char_width(TIER)
    }
}

/// Legend text, with the widths needed to justify it to the box edge.
pub(super) mod legend {
    use super::char_width;

    pub(in super::super) const LABEL: &str = "RPO";
    pub(in super::super) const SEP: &str = " · ";
    pub(in super::super) const SYNC: &str = "SYNC zero data loss";
    pub(in super::super) const READY: &str = "READY sync-capable";
    pub(in super::super) const ASYNC: &str = "ASYNC data loss possible";

    pub(in super::super) const RIGHT: usize = char_width(SYNC)
        + char_width(SEP)
        + char_width(READY)
        + char_width(SEP)
        + char_width(ASYNC);

    /// Narrowest box the legend fits in, with one gap after the label.
    pub(in super::super) const MIN: usize = char_width(LABEL) + 2 + RIGHT;
}

/// A dash long enough for any separator; sliced rather than built per frame.
const RULE: &str = "────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────";

/// How wide the box is and how much of it the address column takes.
pub(super) struct Layout {
    /// Content width after the one-character line indent.
    pub(super) width: usize,
    pub(super) addr_width: usize,
    pub(super) term_digits: usize,
}

impl Layout {
    /// Size the box to enclose both the table and the header, clamped to the
    /// terminal. The address column then absorbs whatever slack the header
    /// forces, so the table's right edge stays flush with the box.
    pub(super) fn measure(status: &ClusterStatus) -> Self {
        let term_width = get_terminal_width();
        let term_digits = decimal_digits(status.term);
        let width = (col::FIXED + address_column_width(status, term_width))
            .max(head::LEFT + char_width(col::GROUP) + head::right(term_digits))
            .max(legend::MIN)
            .min(term_width.saturating_sub(2));
        Self {
            width,
            addr_width: width.saturating_sub(col::FIXED).max(col::ADDR_MIN),
            term_digits,
        }
    }
}

/// Fit the address column to the addresses on screen, within the terminal.
pub(super) fn address_column_width(status: &ClusterStatus, term_width: usize) -> usize {
    let widest = status
        .nodes
        .iter()
        .map(|n| n.addr.chars().filter(|c| !c.is_control()).count())
        .max()
        .unwrap_or(col::ADDR_MIN);
    let budget = term_width.saturating_sub(col::FIXED + 1).max(col::ADDR_MIN);
    widest.clamp(col::ADDR_MIN, col::ADDR_MAX.min(budget))
}

/// Decimal digits in `n`, for laying out a line before writing it.
pub(super) const fn decimal_digits(n: u64) -> usize {
    match n.checked_ilog10() {
        Some(log) => log as usize + 1,
        None => 1,
    }
}

/// Write `width` rule characters. `RULE` is char-indexed, so slice by chars.
pub(super) fn write_rule(frame: &mut Frame, p: &Palette, width: usize) {
    let chars = RULE.chars().count();
    let bytes = RULE
        .char_indices()
        .nth(width.min(chars))
        .map_or(RULE.len(), |(i, _)| i);
    write!(
        frame,
        " {}{}{}",
        p.dim,
        RULE.get(..bytes).unwrap_or(RULE),
        p.reset
    )
    .ok();
    frame.endl();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::status::dashboard::tests::sample_status;

    /// The address column fits its contents rather than a fixed guess.
    #[test]
    fn address_column_fits_its_contents() {
        let mut narrow = sample_status(2);
        for node in &mut narrow.nodes {
            node.addr = "10.0.0.1:1".to_string();
        }
        let mut wide = sample_status(2);
        for node in &mut wide.nodes {
            node.addr = "some-rather-long-host.internal:9091".to_string();
        }

        let narrow_w = address_column_width(&narrow, 200);
        let wide_w = address_column_width(&wide, 200);
        assert_eq!(narrow_w, col::ADDR_MIN, "short addresses hit the floor");
        assert_eq!(wide_w, col::ADDR_MAX, "long addresses hit the ceiling");
        assert!(narrow_w < wide_w);
    }

    /// Layout arithmetic is done on characters. A byte count would skew every
    /// width that crosses a non-ASCII glyph.
    #[test]
    fn layout_constants_are_measured_in_characters() {
        assert_eq!(char_width(legend::SEP), legend::SEP.chars().count());
        assert_eq!(char_width(legend::SEP), 3, "space, middot, space");
        assert_ne!(
            char_width(legend::SEP),
            legend::SEP.len(),
            "the middot is multi-byte, so this test would prove nothing if it were not"
        );
        assert_eq!(char_width(col::GROUP), col::GROUP.chars().count());
    }

    #[test]
    fn decimal_digits_counts_correctly() {
        assert_eq!(decimal_digits(0), 1);
        assert_eq!(decimal_digits(9), 1);
        assert_eq!(decimal_digits(10), 2);
        assert_eq!(decimal_digits(u64::MAX), 20);
    }

    /// The rule is the one line whose width we fully control; it must never be
    /// the thing that overflows a narrow terminal.
    #[test]
    fn box_never_exceeds_the_terminal() {
        let layout = Layout::measure(&sample_status(3));
        assert!(layout.width <= get_terminal_width().saturating_sub(2));
        assert!(layout.addr_width >= col::ADDR_MIN);
    }
}
