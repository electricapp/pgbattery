//! Terminal rendering primitives: the color palette, a heap-free string sink,
//! and the width-aware writers the dashboard lays its columns out with.
//!
//! Everything here is allocation-free by construction. The `status --watch`
//! dashboard repaints on a timer, so a `String` returned from a per-cell
//! helper becomes an allocation per cell per node per second.

use std::fmt;
use std::fmt::Write as _;

use super::common::{colors, stdout_color};
use terminal_size::{Width, terminal_size};

/// The escape sequences a renderer should emit, chosen once for the process.
///
/// All-empty when color is off, so a renderer emits the right bytes on the
/// first pass and never has to strip them back out.
#[derive(Debug, Clone, Copy)]
pub(super) struct Palette {
    pub(super) reset: &'static str,
    pub(super) bold: &'static str,
    pub(super) dim: &'static str,
    pub(super) green: &'static str,
    pub(super) yellow: &'static str,
    pub(super) red: &'static str,
    pub(super) cyan: &'static str,
    pub(super) white: &'static str,
    pub(super) bg_green: &'static str,
    pub(super) bg_red: &'static str,
    pub(super) bg_yellow: &'static str,
    pub(super) black: &'static str,
}

static COLOR_PALETTE: Palette = Palette {
    reset: colors::RESET,
    bold: colors::BOLD,
    dim: colors::DIM,
    green: colors::GREEN,
    yellow: colors::YELLOW,
    red: colors::RED,
    cyan: colors::CYAN,
    white: colors::WHITE,
    bg_green: colors::BG_GREEN,
    bg_red: colors::BG_RED,
    bg_yellow: colors::BG_YELLOW,
    black: colors::BLACK,
};

static MONO_PALETTE: Palette = Palette {
    reset: "",
    bold: "",
    dim: "",
    green: "",
    yellow: "",
    red: "",
    cyan: "",
    white: "",
    bg_green: "",
    bg_red: "",
    bg_yellow: "",
    black: "",
};

/// The palette matching this process's stdout color decision.
pub(super) fn palette() -> &'static Palette {
    if stdout_color() {
        &COLOR_PALETTE
    } else {
        &MONO_PALETTE
    }
}

/// A fixed-capacity [`fmt::Write`] sink that never touches the heap.
///
/// Size `N` above the longest output a caller can produce. Writes past
/// capacity are dropped at a char boundary rather than panicking.
pub(super) struct StackStr<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> StackStr<N> {
    pub(super) const fn new() -> Self {
        Self {
            buf: [0; N],
            len: 0,
        }
    }

    pub(super) fn as_str(&self) -> &str {
        // `write_str` only ever commits whole UTF-8 sequences, so the prefix
        // is always valid; the lossy path is unreachable but keeps this free
        // of `unsafe` and of the `unwrap` the workspace denies.
        std::str::from_utf8(self.buf.get(..self.len).unwrap_or_default()).unwrap_or_default()
    }
}

impl<const N: usize> fmt::Write for StackStr<N> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for ch in s.chars() {
            let mut encoded = [0u8; 4];
            let bytes = ch.encode_utf8(&mut encoded).as_bytes();
            let Some(slot) = self.buf.get_mut(self.len..self.len + bytes.len()) else {
                return Ok(()); // full: drop the tail, keep the buffer valid
            };
            slot.copy_from_slice(bytes);
            self.len += bytes.len();
        }
        Ok(())
    }
}

impl<const N: usize> fmt::Display for StackStr<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.as_str())
    }
}

impl<const N: usize> PartialEq<&str> for StackStr<N> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<const N: usize> fmt::Debug for StackStr<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

/// Widest field [`write_field`] can lay out in one column.
const FIELD_MAX: usize = 128;

/// Count the characters in `s` at compile time.
///
/// Layout arithmetic over literals needs a character count, and `str::len` is
/// bytes — a single `·` in a separator silently skews a column by one. Counts
/// scalar values, so it is correct for the box-drawing and punctuation glyphs
/// used here but not for double-width CJK.
pub(super) const fn char_width(s: &str) -> usize {
    let mut bytes = s.as_bytes();
    let mut count = 0;
    while let Some((first, rest)) = bytes.split_first() {
        // Continuation bytes (0b10xxxxxx) continue the previous character.
        if (*first & 0xC0) != 0x80 {
            count += 1;
        }
        bytes = rest;
    }
    count
}

/// Write `s` clipped to `max_chars` with an ellipsis, dropping control
/// characters.
///
/// Node addresses come from the discovery API and from peer `/metrics`, so
/// they are remote input painted into an operator's terminal. Clipping is by
/// character, not byte, so a non-ASCII address cannot skew the column.
pub(super) fn write_clipped(w: &mut impl fmt::Write, s: &str, max_chars: usize) -> fmt::Result {
    if max_chars == 0 {
        return Ok(());
    }
    let clean = || s.chars().filter(|c| !c.is_control());
    let len = clean().count();
    if len <= max_chars {
        for c in clean() {
            w.write_char(c)?;
        }
        return Ok(());
    }
    if max_chars <= 3 {
        for _ in 0..max_chars {
            w.write_char('.')?;
        }
        return Ok(());
    }
    for c in clean().take(max_chars - 3) {
        w.write_char(c)?;
    }
    w.write_str("...")
}

/// Left-align `s` in a `width`-character column via [`write_clipped`].
pub(super) fn write_field(w: &mut impl fmt::Write, s: &str, width: usize) -> fmt::Result {
    let mut cell: StackStr<FIELD_MAX> = StackStr::new();
    write_clipped(&mut cell, s, width.min(FIELD_MAX))?;
    write!(w, "{:<width$}", cell.as_str(), width = width)
}

/// Get terminal width, with sensible defaults.
pub(super) fn get_terminal_width() -> usize {
    terminal_size()
        .map_or(80, |(Width(w), _)| w as usize)
        .max(60) // minimum usable width
}

/// Capacity for the numeric cells below. Longest output is a 17-character LSN.
pub(super) const NUM_CELL: usize = 24;

/// A rendered numeric cell. Returned by value, formatted without allocating.
pub(super) type NumCell = StackStr<NUM_CELL>;

/// Format bytes as a `PostgreSQL` LSN (X/YYYYYYYY).
pub(super) fn format_lsn(bytes: u64) -> NumCell {
    let high = bytes >> 32;
    let low = bytes & 0xFFFF_FFFF;
    let mut out = NumCell::new();
    // Infallible: `StackStr` never errors, and 17 chars fits NUM_CELL.
    write!(&mut out, "{high:X}/{low:08X}").ok();
    out
}

/// Format large numbers with K/M suffixes.
pub(super) fn format_number(n: u64) -> NumCell {
    let mut out = NumCell::new();
    if n >= 1_000_000 {
        write!(&mut out, "{}.{}M", n / 1_000_000, (n % 1_000_000) / 100_000).ok();
    } else if n >= 1_000 {
        write!(&mut out, "{}.{}k", n / 1_000, (n % 1_000) / 100).ok();
    } else {
        write!(&mut out, "{n}").ok();
    }
    out
}

/// Format a byte count as a compact lag figure (`0B`, `4.2KB`, `1.3MB`).
pub(super) fn format_lag(bytes: u64) -> NumCell {
    let mut out = NumCell::new();
    if bytes > 1_000_000 {
        write!(
            &mut out,
            "{}.{}MB",
            bytes / 1_000_000,
            (bytes % 1_000_000) / 100_000
        )
        .ok();
    } else if bytes > 1_000 {
        write!(&mut out, "{}.{}KB", bytes / 1_000, (bytes % 1_000) / 100).ok();
    } else {
        write!(&mut out, "{bytes}B").ok();
    }
    out
}

/// Format bytes as human-readable size.
pub(super) fn format_size(bytes: u64) -> String {
    if bytes > 1_000_000_000 {
        format!(
            "{}.{} GB",
            bytes / 1_000_000_000,
            (bytes % 1_000_000_000) / 100_000_000
        )
    } else if bytes > 1_000_000 {
        format!("{}.{} MB", bytes / 1_000_000, (bytes % 1_000_000) / 100_000)
    } else if bytes > 1_000 {
        format!("{}.{} KB", bytes / 1_000, (bytes % 1_000) / 100)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Collect `write_clipped` into a `String` so the cases below read as
    /// plain equalities.
    fn clipped(s: &str, max: usize) -> String {
        let mut out = String::new();
        write_clipped(&mut out, s, max).ok();
        out
    }

    #[test]
    fn test_clip_short_string() {
        assert_eq!(clipped("hello", 10), "hello");
    }

    #[test]
    fn test_clip_long_string() {
        assert_eq!(clipped("hello world", 8), "hello...");
    }

    #[test]
    fn test_clip_exact_length() {
        assert_eq!(clipped("hello", 5), "hello");
    }

    #[test]
    fn test_clip_tiny_width() {
        assert_eq!(clipped("hello", 2), "..");
        assert_eq!(clipped("hello", 0), "");
    }

    #[test]
    fn test_clip_strips_control_characters() {
        // A node address arrives from a peer's /metrics endpoint. It must not
        // be able to carry an escape sequence into the operator's terminal.
        assert_eq!(clipped("\x1b[2J\x1b[31m10.0.0.1", 32), "[2J[31m10.0.0.1");
        assert_eq!(clipped("a\rb\nc\tdel\x07", 32), "abcdel");
    }

    #[test]
    fn test_clip_measures_width_in_characters() {
        // Nine characters, eighteen bytes. Clipping must not treat it as
        // over-width, or the column it sits in loses its alignment.
        let nine_wide = "ααααααααα";
        assert_eq!(nine_wide.len(), 18);
        assert_eq!(clipped(nine_wide, 12), nine_wide);
    }

    #[test]
    fn test_write_field_pads_to_width() {
        let mut out = String::new();
        write_field(&mut out, "1.2.3.4", 12).ok();
        assert_eq!(out, "1.2.3.4     ");
        assert_eq!(out.chars().count(), 12);
    }

    #[test]
    fn test_char_width_counts_characters_not_bytes() {
        assert_eq!(char_width(" · "), 3);
        assert_eq!(" · ".len(), 4, "the middot is multi-byte");
        assert_eq!(char_width("─"), 1);
        assert_eq!(char_width("plain"), 5);
        assert_eq!(char_width(""), 0);
    }

    #[test]
    fn test_stack_str_clips_instead_of_panicking() {
        let mut s: StackStr<4> = StackStr::new();
        write!(&mut s, "abcdefgh").ok();
        assert_eq!(s.as_str(), "abcd");
    }

    #[test]
    fn test_stack_str_clips_on_a_char_boundary() {
        // 'α' is two bytes: the second one must not be half-written.
        let mut s: StackStr<3> = StackStr::new();
        write!(&mut s, "αα").ok();
        assert_eq!(s.as_str(), "α");
    }

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0), "0/00000000");
        assert_eq!(format_lsn(0x1A4B_2C00), "0/1A4B2C00");
        assert_eq!(format_lsn(0x0001_0000_1A4B_2C00), "10000/1A4B2C00");
        assert_eq!(format_lsn((1u64 << 32) | 0x1234_5678), "1/12345678");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(1500), "1.5k");
        assert_eq!(format_number(1_500_000), "1.5M");
    }

    #[test]
    fn test_format_lag() {
        assert_eq!(format_lag(0), "0B");
        assert_eq!(format_lag(512), "512B");
        assert_eq!(format_lag(4200), "4.2KB");
        assert_eq!(format_lag(1_300_000), "1.3MB");
    }

    #[test]
    fn test_numeric_cells_never_overflow_their_buffer() {
        // NUM_CELL is sized against the worst case; prove it for the extremes
        // rather than trusting the arithmetic.
        assert_eq!(format_lsn(u64::MAX), "FFFFFFFF/FFFFFFFF");
        assert_eq!(format_lsn(u64::MAX).as_str().len(), 17);
        assert!(format_number(u64::MAX).as_str().len() < NUM_CELL);
        assert!(format_lag(u64::MAX).as_str().len() < NUM_CELL);
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1500), "1.5 KB");
        assert_eq!(format_size(1_500_000), "1.5 MB");
        assert_eq!(format_size(1_500_000_000), "1.5 GB");
    }

    #[test]
    fn test_get_terminal_width_has_minimum() {
        // Even without a real terminal, should return at least 60
        let width = get_terminal_width();
        assert!(width >= 60);
    }
}
