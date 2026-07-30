//! Fuzz target for the gateway's query classifier and its cheap prefilters —
//! the path that turns client SQL text into failover decisions and is the only
//! place untrusted input reaches `libpg_query` (a C library).
//!
//! Reachable public entry points: `ConnectionHandler::contains_token_ci`,
//! `query_keyword_flags`, `is_commit_query`, `build_query_message`.
//! `is_commit_query` is the route into the C parser: when the commit prefilter
//! fires it calls the private `analyze_query`, which runs `pg_query::parse`
//! (and, on a parse error, `pg_query::split_with_scanner` plus a per-statement
//! reparse) and walks the resulting AST — including the session-state
//! classification arms. So this target does exercise the AST walk, but only
//! for queries that also carry a `commit`/`end` token. See the coverage note
//! at the bottom.
//!
//! Invariants checked:
//!   - `contains_token_ci` matches an independent word-boundary reference
//!     implementation, for needles and haystacks both taken from the fuzz
//!     input. This is the load-bearing prefilter: a false negative means a
//!     session-state or COMMIT statement is never looked at again.
//!   - `query_keyword_flags` is deterministic and invariant under ASCII case
//!     folding (it is documented case-insensitive, and ASCII folding changes
//!     neither byte length nor word-character-ness, so this is exact).
//!   - `query_keyword_flags` is monotone under append: appending text after a
//!     newline can only add flags, never clear one. A keyword must not become
//!     invisible because more SQL follows it.
//!   - `is_commit_query` is deterministic — called twice on the same text it
//!     must agree, which is a real check on `libpg_query` holding no state
//!     across calls.
//!   - `is_commit_query(q)` implies `query_keyword_flags(q).commit`, the
//!     documented three-tier structure (byte scan gates the C parse).
//!   - `build_query_message` emits a frame `PacketHeader::parse` agrees with:
//!     total length equals buffer length, payload is the SQL bytes plus a
//!     terminating NUL.
//!
//! Deliberately NOT asserted, and why:
//!   - The session-state contract ("a query containing a session-scoped
//!     construct must never be classified migratable") is the invariant worth
//!     the most here, and no fuzz target can carry it. Deciding whether a
//!     statement actually leaves session state requires a reference oracle,
//!     and the only real oracle is a live PostgreSQL session plus catalog
//!     snapshots — `pg_cursors`, `pg_locks WHERE locktype = 'advisory'`,
//!     `pg_listening_channels()`, `pg_class WHERE relpersistence = 't'`,
//!     `pg_settings WHERE source = 'session'`, `pg_prepared_statements`. That
//!     is the differential-classifier work in `HARDENING.md` Tier 2, not
//!     something a fuzz target can approximate. `analyze_query`,
//!     `QueryAnalysis`, and `SessionChange` therefore stay private on purpose:
//!     widening them would buy only a strictly weaker property (determinism
//!     and no-panic, both already covered here through the public prefilters)
//!     in exchange for permanent public API surface.
//!   - Any relationship between `query_keyword_flags` and `contains_token_ci`
//!     over the *specific* prefilter tokens. The doc comment states the two
//!     use identical word-boundary semantics, so `flags.commit ==
//!     contains_token_ci(q, "commit") | contains_token_ci(q, "end")` ought to
//!     hold — but the token list is private, and exposing it to assert a
//!     snapshot of itself is not worth the API surface either. The invariants
//!     below are all token-agnostic for that reason: they hold no matter which
//!     keywords the fused scan grows.
//!   - Whether a given query *should* be treated as a COMMIT. There is no
//!     oracle for that short of PostgreSQL itself.
//!
//! Run with: cargo fuzz run gateway_query_analysis
#![no_main]
use libfuzzer_sys::fuzz_target;
use pgbattery::gateway::handlers::ConnectionHandler;
use pgbattery::gateway::protocol::PacketHeader;

/// The prefilter tokens the gateway actually scans for. Used only as *inputs*
/// to the `contains_token_ci` differential, never to predict a flag value.
const PREFILTER_TOKENS: &[&str] = &[
    "commit",
    "end",
    "listen",
    "unlisten",
    "set_config",
    "pg_advisory_lock",
    "pg_advisory_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_lock_shared",
];

/// Straightforward word-boundary, case-insensitive token search. Written
/// independently of the implementation under test: explicit start-index range
/// and explicit boundary lookups rather than a `windows` scan.
fn reference_contains_token_ci(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.is_empty() || n.len() > h.len() {
        return false;
    }
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    (0..=h.len() - n.len()).any(|i| {
        h[i..i + n.len()].eq_ignore_ascii_case(n)
            && (i == 0 || !is_word(h[i - 1]))
            && (i + n.len() == h.len() || !is_word(h[i + n.len()]))
    })
}

fn check_token_scan(haystack: &str, needle: &str) {
    assert_eq!(
        ConnectionHandler::contains_token_ci(haystack, needle),
        reference_contains_token_ci(haystack, needle),
        "contains_token_ci disagrees with the reference for needle {needle:?}"
    );
}

fuzz_target!(|data: &[u8]| {
    // Split the input into a needle and the SQL text, so the differential
    // covers arbitrary needles as well as the real prefilter tokens.
    let Some((&needle_len, rest)) = data.split_first() else {
        return;
    };
    let split = usize::from(needle_len % 32).min(rest.len());
    let (needle_bytes, sql_bytes) = rest.split_at(split);

    let Ok(sql) = std::str::from_utf8(sql_bytes) else {
        return;
    };

    if let Ok(needle) = std::str::from_utf8(needle_bytes) {
        check_token_scan(sql, needle);
    }
    for token in PREFILTER_TOKENS {
        check_token_scan(sql, token);
    }

    // Fused single-pass scan: deterministic, ASCII-case-insensitive, monotone
    // under append.
    let flags = ConnectionHandler::query_keyword_flags(sql);
    assert_eq!(
        flags,
        ConnectionHandler::query_keyword_flags(sql),
        "query_keyword_flags is not deterministic"
    );
    assert_eq!(
        flags,
        ConnectionHandler::query_keyword_flags(&sql.to_ascii_uppercase()),
        "query_keyword_flags is not ASCII-case-insensitive (upper)"
    );
    assert_eq!(
        flags,
        ConnectionHandler::query_keyword_flags(&sql.to_ascii_lowercase()),
        "query_keyword_flags is not ASCII-case-insensitive (lower)"
    );

    // Monotonicity: a keyword found in either half must still be found in the
    // newline-joined whole. No prefilter token contains a newline, so the seam
    // cannot create or destroy a word boundary that matters.
    let mut mid = sql.len() / 2;
    while mid > 0 && !sql.is_char_boundary(mid) {
        mid -= 1;
    }
    let (head, tail) = sql.split_at(mid);
    let joined = format!("{head}\n{tail}");
    let joined_flags = ConnectionHandler::query_keyword_flags(&joined);
    for (name, part) in [("head", head), ("tail", tail)] {
        let part_flags = ConnectionHandler::query_keyword_flags(part);
        assert!(
            !part_flags.commit || joined_flags.commit,
            "commit flag lost from {name} after append"
        );
        assert!(
            !part_flags.subscription || joined_flags.subscription,
            "subscription flag lost from {name} after append"
        );
        assert!(
            !part_flags.function_state || joined_flags.function_state,
            "function_state flag lost from {name} after append"
        );
    }

    // Full detection pipeline. This is where libpg_query gets the bytes.
    let is_commit = ConnectionHandler::is_commit_query(sql);
    assert_eq!(
        is_commit,
        ConnectionHandler::is_commit_query(sql),
        "is_commit_query is not deterministic"
    );
    assert!(
        !is_commit || flags.commit,
        "is_commit_query bypassed its own commit prefilter"
    );

    // Query message builder must agree with the header parser. Skipped for SQL
    // large enough to overflow the u32 length field, where the builder
    // deliberately saturates instead of failing.
    if 5 + sql.len() > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
        return;
    }
    let msg = ConnectionHandler::build_query_message(sql);
    let header = PacketHeader::parse(&msg).expect("Query message shorter than a header");
    assert_eq!(
        header.total_length(),
        msg.len(),
        "Query message length field disagrees with buffer length"
    );
    assert_eq!(
        &msg[PacketHeader::HEADER_SIZE..msg.len() - 1],
        sql.as_bytes(),
        "Query message payload is not the SQL text"
    );
    assert_eq!(msg.last(), Some(&0), "Query message must end with NUL");
});
