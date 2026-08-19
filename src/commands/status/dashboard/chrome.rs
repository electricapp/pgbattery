//! Everything around the table: title, column labels, footer, legend.
//!
//! All three text lines justify to the box edge, so the frame reads as one
//! block rather than a box with ragged prose under it.

use std::fmt::Write as _;

use super::super::frame::Frame;
use super::super::model::ClusterStatus;
use super::layout::{col, decimal_digits, head, legend, write_rule};
use crate::commands::render::{Palette, char_width, format_number, write_clipped};

pub(super) fn header(
    frame: &mut Frame,
    p: &Palette,
    status: &ClusterStatus,
    box_width: usize,
    term_digits: usize,
) {
    // Padded to head::BADGE, so the fields after it do not shift as it changes.
    let health = if !status.healthy {
        " DEGRADED "
    } else if !status.sync_replicated {
        // Available but no sync replica — leader loss = data loss.
        " RPO RISK "
    } else {
        " HEALTHY  "
    };
    let health_bg = if status.healthy && status.sync_replicated {
        p.bg_green
    } else {
        p.bg_red
    };
    let term = status.term;
    // chrono's strftime path parses the format string and allocates; the
    // fields go straight into the frame instead.
    let now = chrono::Utc::now();
    let (date, time) = (now.date_naive(), now.time());
    // Justify the metadata to the box's right edge.
    let gap = box_width.saturating_sub(head::LEFT + head::right(term_digits));

    frame.endl();
    write!(
        frame,
        " {b}{name}{r} {version}{gr}{hb}{k}{health}{r}",
        b = p.bold,
        r = p.reset,
        name = head::NAME,
        version = head::VERSION,
        gr = col::GROUP,
        hb = health_bg,
        k = p.black,
    )
    .ok();
    write!(
        frame,
        "{:gap$}{d}{term_label}{r}{term}{gr}{d}",
        "",
        d = p.dim,
        r = p.reset,
        term_label = head::TERM,
        gr = col::GROUP,
    )
    .ok();
    write_timestamp(frame, date, time);
    write!(
        frame,
        "{r}{gr}{d}{tier}{r}",
        r = p.reset,
        gr = col::GROUP,
        d = p.dim,
        tier = head::TIER,
    )
    .ok();
    frame.endl();
}

/// `YYYY-MM-DD HH:MM:SS UTC`, written field by field.
fn write_timestamp(frame: &mut Frame, date: chrono::NaiveDate, time: chrono::NaiveTime) {
    use chrono::{Datelike as _, Timelike as _};
    write!(
        frame,
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
        date.year(),
        date.month(),
        date.day(),
        time.hour(),
        time.minute(),
        time.second(),
    )
    .ok();
}

pub(super) fn column_labels(
    frame: &mut Frame,
    p: &Palette,
    addr_width: usize,
    separator_width: usize,
) {
    write!(
        frame,
        " {d}{:g$}{:>id$}  {:<aw$}  {:<role$}  {:>sync$}  {:>lsn$}  {:>lag$}  {:>conns$}{r}",
        "",
        "ID",
        "ADDRESS",
        "ROLE",
        "SYNC",
        "LSN",
        "LAG",
        "CONNS",
        d = p.dim,
        r = p.reset,
        g = col::GUTTER + 1,
        id = col::ID,
        aw = addr_width,
        role = col::ROLE,
        sync = col::SYNC,
        lsn = col::LSN,
        lag = col::LAG,
        conns = col::CONNS,
    )
    .ok();
    frame.endl();
    write_rule(frame, p, separator_width);
}

pub(super) fn footer(
    frame: &mut Frame,
    p: &Palette,
    status: &ClusterStatus,
    separator_width: usize,
) {
    write_rule(frame, p, separator_width);

    let total_conns: u64 = status.nodes.iter().map(|n| n.connections_active).sum();
    let total_migrated: u64 = status.nodes.iter().map(|n| n.connections_migrated).sum();
    let total_held: u64 = status.nodes.iter().map(|n| n.connections_held).sum();
    let leader_str = status.leader_addr.as_deref().unwrap_or("none");
    let migrated = format_number(total_migrated);

    // Justified like the header: the leader at the left edge, the connection
    // totals ending flush with the box.
    let left = char_width("Leader ")
        + leader_str
            .chars()
            .filter(|c| !c.is_control())
            .count()
            .min(col::ADDR_MAX);
    let right = char_width("Active ")
        + decimal_digits(total_conns)
        + char_width(col::GROUP)
        + char_width("Held ")
        + decimal_digits(total_held)
        + char_width(col::GROUP)
        + char_width("Migrated ")
        + migrated.as_str().chars().count();
    let gap = separator_width.saturating_sub(left + right);

    write!(frame, " {}Leader{} ", p.dim, p.reset).ok();
    write_clipped(frame, leader_str, col::ADDR_MAX).ok();
    write!(
        frame,
        "{:gap$}{d}Active{r} {total_conns}{gr}",
        "",
        gr = col::GROUP,
        d = p.dim,
        r = p.reset
    )
    .ok();
    if total_held > 0 {
        write!(
            frame,
            "{}{}Held {total_held}{}",
            p.bg_yellow, p.black, p.reset
        )
        .ok();
    } else {
        write!(frame, "{}Held{} {total_held}", p.dim, p.reset).ok();
    }
    write!(
        frame,
        "{gr}{d}Migrated{r} {migrated}",
        gr = col::GROUP,
        d = p.dim,
        r = p.reset
    )
    .ok();
    frame.endl();
}

pub(super) fn key(frame: &mut Frame, p: &Palette, box_width: usize) {
    let gap = box_width.saturating_sub(char_width(legend::LABEL) + legend::RIGHT);
    // Each key word is colored as it appears in the SYNC column; the gloss
    // after it stays dim.
    write!(
        frame,
        " {d}{label}{:gap$}{r}{g}SYNC{d} zero data loss{sep}{r}{c}READY{d} sync-capable{sep}{r}{e}ASYNC{d} data loss possible{r}",
        "",
        d = p.dim,
        r = p.reset,
        label = legend::LABEL,
        sep = legend::SEP,
        g = p.green,
        c = p.cyan,
        e = p.red,
    )
    .ok();
    frame.endl();
}
