//! SQL session-state analysis: keyword prefilters and the statement classifier.

use bytes::BytesMut;

use super::ConnectionHandler;

/// Keyword hits from the single-pass `query_keyword_flags` scan — one walk
/// over the query bytes answers every full-text prefilter the per-message
/// path needs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct QueryKeywordFlags {
    /// `listen` or `unlisten` token present (word-boundary, case-insensitive).
    pub subscription: bool,
    /// A session-state function call is present: `set_config` or the
    /// session-scoped advisory-lock family (`pg_advisory_lock`,
    /// `pg_advisory_lock_shared`, and their `pg_try_advisory_lock*`
    /// spellings). The `*_xact_*` variants never match — they release at
    /// transaction end and cannot survive to an Idle
    /// migration point. Conservative by design: a match inside a string
    /// literal also fires, and severing on failover is the safe direction.
    pub function_state: bool,
    /// A `temp`, `temporary`, or `pg_temp` token is present, so the statement
    /// may name an object in the session's temp schema. Unlike the flags
    /// above this one does not classify anything on its own — it routes the
    /// query to the analyzer (`select`-leading shapes such as
    /// `SELECT … INTO TEMP` are otherwise never parsed) and arms the
    /// unmodeled-statement fallback in `accumulate_query_analysis`. Word
    /// boundaries keep `temp_c` and `temp_buffers` from matching, so an
    /// ordinary column named `temp` costs one parse and stays migratable.
    pub temp_object: bool,
}

/// Leading keyword of the first statement in `query`: skips whitespace,
/// `--` line comments, and (nested) `/* */` block comments, then takes the
/// longest identifier run. Returns `None` when the query starts with
/// something else (punctuation, an unterminated comment, emptiness).
fn first_statement_keyword(query: &str) -> Option<&str> {
    let b = query.as_bytes();
    let mut i = 0usize;
    loop {
        while b.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if b.get(i) == Some(&b'-') && b.get(i + 1) == Some(&b'-') {
            while b.get(i).is_some_and(|&c| c != b'\n') {
                i += 1;
            }
            continue;
        }
        if b.get(i) == Some(&b'/') && b.get(i + 1) == Some(&b'*') {
            // PostgreSQL block comments nest.
            let mut depth = 1usize;
            i += 2;
            while depth > 0 {
                match (b.get(i), b.get(i + 1)) {
                    (Some(b'/'), Some(b'*')) => {
                        depth += 1;
                        i += 2;
                    }
                    (Some(b'*'), Some(b'/')) => {
                        depth -= 1;
                        i += 2;
                    }
                    (Some(_), _) => i += 1,
                    (None, _) => return None, // unterminated comment
                }
            }
            continue;
        }
        break;
    }
    let start = i;
    while b
        .get(i)
        .is_some_and(|&c| c.is_ascii_alphanumeric() || c == b'_')
    {
        i += 1;
    }
    (i > start).then(|| query.get(start..i)).flatten()
}

/// True when `s` contains a statement-separating `;` — one outside every
/// single-quoted literal (`''` doubling honored), quoted identifier
/// (`""` doubling honored), dollar-quoted string (`$tag$ … $tag$`), `--`
/// line comment, and (nested) `/* */` block comment.
///
/// One pass, no allocation. Errs only toward `true`, which costs a C parse:
/// a backslash in a literal answers `true` outright, since whether it escapes
/// the quote depends on an `E` prefix and `standard_conforming_strings`; and
/// a `$` continuing an identifier (`a$b`) never opens a dollar quote. An
/// unterminated construct swallows the rest of the text, which is a server
/// side syntax error anyway (the analyzer's parse would yield nothing).
fn has_top_level_semicolon(text: &str) -> bool {
    let continues_identifier =
        |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80;
    let raw = text.as_bytes();
    let len = raw.len();
    let mut pos = 0usize;
    while pos < len {
        match raw.get(pos) {
            Some(b';') => return true,
            // Single-quoted literal: scan to the closing quote; a doubled
            // quote ('') is an escaped quote, not a close.
            Some(b'\'') => {
                pos += 1;
                while pos < len {
                    match raw.get(pos) {
                        Some(b'\\') => return true,
                        Some(b'\'') if raw.get(pos + 1) == Some(&b'\'') => pos += 2,
                        Some(b'\'') => break,
                        _ => pos += 1,
                    }
                }
                pos += 1;
            }
            // Quoted identifier: same shape with "" doubling.
            Some(b'"') => {
                pos += 1;
                while pos < len {
                    if raw.get(pos) == Some(&b'"') {
                        if raw.get(pos + 1) == Some(&b'"') {
                            pos += 2;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                pos += 1;
            }
            // Dollar quoting: `$` + optional identifier (not digit-leading —
            // `$1` is a parameter) + `$`, closed by the identical tag. A `$`
            // continuing an identifier is part of its name.
            Some(b'$') if pos > 0 && raw.get(pos - 1).is_some_and(|&c| continues_identifier(c)) => {
                pos += 1;
            }
            Some(b'$') => {
                let tag_start = pos + 1;
                let mut tag_end = tag_start;
                while raw
                    .get(tag_end)
                    .is_some_and(|&c| c != b'$' && continues_identifier(c))
                {
                    tag_end += 1;
                }
                let tag_valid = raw.get(tag_end) == Some(&b'$')
                    && !raw.get(tag_start).is_some_and(u8::is_ascii_digit);
                if !tag_valid {
                    pos += 1;
                    continue;
                }
                let Some(tag) = raw.get(pos..=tag_end) else {
                    return false;
                };
                // Find the closing tag; none -> rest of text is the literal.
                let mut search = tag_end + 1;
                loop {
                    match raw.get(search..search + tag.len()) {
                        None => return false,
                        Some(w) if w == tag => {
                            pos = search + tag.len();
                            break;
                        }
                        Some(_) => search += 1,
                    }
                }
            }
            // Line comment: to end of line.
            Some(b'-') if raw.get(pos + 1) == Some(&b'-') => {
                while pos < len && raw.get(pos) != Some(&b'\n') {
                    pos += 1;
                }
            }
            // Block comment: nested, per PostgreSQL.
            Some(b'/') if raw.get(pos + 1) == Some(&b'*') => {
                let mut depth = 1usize;
                pos += 2;
                while pos < len && depth > 0 {
                    if raw.get(pos) == Some(&b'/') && raw.get(pos + 1) == Some(&b'*') {
                        depth += 1;
                        pos += 2;
                    } else if raw.get(pos) == Some(&b'*') && raw.get(pos + 1) == Some(&b'/') {
                        depth -= 1;
                        pos += 2;
                    } else {
                        pos += 1;
                    }
                }
            }
            _ => pos += 1,
        }
    }
    false
}

/// True when any top-level statement in `query` could begin with one of
/// `keywords` (case-insensitive).
///
/// Tier-1 prefilter for the session-state analyzer. SET / RESET /
/// DEALLOCATE / DISCARD are statement-leading keywords, so a token buried
/// inside another statement (`UPDATE t SET …`) must not pay for the C
/// parser. The first statement's keyword is computed exactly; multi-
/// statement strings — detected by a top-level `;` remaining after trailing
/// `;` and whitespace are trimmed (`has_top_level_semicolon` skips literals,
/// identifiers, dollar quotes, and comments) — fall back to a word-boundary
/// token scan, which can over-trigger (a keyword token inside a literal)
/// but never under-triggers: a non-first statement starting with a keyword
/// always leaves both a top-level `;` and the keyword token in the text.
fn leading_statement_keyword_matches(query: &str, keywords: &[&str]) -> bool {
    if let Some(first) = first_statement_keyword(query)
        && keywords.iter().any(|k| first.eq_ignore_ascii_case(k))
    {
        return true;
    }
    let trimmed = query.trim_end_matches(|c: char| c == ';' || c.is_ascii_whitespace());
    if !has_top_level_semicolon(trimmed) {
        return false;
    }
    keywords
        .iter()
        .any(|k| ConnectionHandler::contains_token_ci(query, k))
}

/// Query text of a Parse message: 'P', length, then the NUL-terminated
/// statement name and query.
pub(super) fn parse_message_query(msg: &[u8]) -> Option<&str> {
    let (_stmt_name, rest) = read_cstring(msg.get(5..)?)?;
    read_cstring(rest).map(|(query, _)| query)
}

fn read_cstring(data: &[u8]) -> Option<(&str, &[u8])> {
    let null_pos = data.iter().position(|&b| b == 0)?;
    let s = std::str::from_utf8(data.get(..null_pos)?).ok()?;
    Some((s, data.get(null_pos + 1..)?))
}

/// A statement that mutates session state the gateway tracks for failover:
/// LISTEN registrations, the prepared-statement replay set, and the
/// non-migratable flag for session GUCs.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) enum SessionChange {
    Listen(String),
    Unlisten(String),
    UnlistenAll,
    /// Session-scoped `SET`/`RESET` (not `SET LOCAL`) — the session carries
    /// GUCs we don't reconstruct on a new backend, so the connection is
    /// marked non-migratable and severed on failover.
    SetSessionVar,
    /// `DEALLOCATE name` — drop one statement from the replay set.
    Deallocate(String),
    /// `DEALLOCATE ALL` — drop every statement from the replay set.
    DeallocateAll,
    /// `DISCARD ALL` — server-side it deallocates every prepared statement
    /// and unlistens every channel, so the tracked state must follow.
    DiscardAll,
    /// A statement leaving backend-local session state the gateway cannot
    /// reconstruct on a migrated backend (temp table / view / sequence, SQL
    /// `PREPARE`, `WITH HOLD` cursor, session advisory lock,
    /// `set_config(..., false)`, a `DO` block whose body cannot be inspected).
    /// The `&'static str` is the reason, for logging. Marks the connection
    /// non-migratable so failover severs (08006) instead of silently losing
    /// the state.
    NonMigratable(&'static str),
}

/// How much the analyzer knows about one parsed statement.
///
/// The analyzer is a **deny-list**: it has arms for the statement types it has
/// been taught, and `PostgreSQL` has roughly two hundred more that it has not.
/// `Unmodeled` therefore means "not examined", never "leaves no session
/// state" — see `ConnectionHandler::classify_statement`.
#[derive(Debug, Clone, Eq, PartialEq)]
enum StatementClass {
    /// The node type has an arm and that arm reached a verdict: `Some` is the
    /// session state the statement leaves behind, `None` means the arm looked
    /// and found none (permanent DDL, `SET LOCAL`, plain DML, a non-HOLD
    /// cursor).
    Modeled(Option<SessionChange>),
    /// No arm for this node type; its effect on session state is unknown.
    Unmodeled,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub(super) struct QueryAnalysis {
    /// Session-state mutations in statement order. Order matters:
    /// `SET x=1; DISCARD ALL` and `DISCARD ALL; SET x=1` leave different
    /// session state behind.
    pub(super) session_changes: Vec<SessionChange>,
    /// The text begins or ends a transaction itself, so whether its tracked
    /// changes took effect is not decided by the `ReadyForQuery` alone.
    pub(super) controls_transaction: bool,
}

impl ConnectionHandler {
    /// Word-boundary, case-insensitive token search over raw bytes.
    ///
    /// Replaces the split-based implementation. `windows` iterates over the
    /// haystack once at the byte level — no UTF-8 char decoding, no intermediate
    /// string slices, no closure-per-char overhead. Word boundaries are checked
    /// inline on the rare match rather than for every character.
    #[inline]
    #[must_use]
    pub fn contains_token_ci(query: &str, token: &str) -> bool {
        let qb = query.as_bytes();
        let tb = token.as_bytes();
        if tb.is_empty() {
            return false;
        }
        qb.windows(tb.len()).enumerate().any(|(i, w)| {
            w.eq_ignore_ascii_case(tb)
                && i.checked_sub(1)
                    .and_then(|j| qb.get(j))
                    .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_')
                && qb
                    .get(i + tb.len())
                    .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_')
        })
    }

    /// Single-flag view of the fused scan; the hot paths call
    /// `query_keyword_flags` once instead and read all flags from it.
    #[cfg(test)]
    fn might_contain_subscription_command(query: &str) -> bool {
        Self::query_keyword_flags(query).subscription
    }

    /// Prefilter for statements that mutate tracked session state: session
    /// GUCs (`SET`/`RESET`), the prepared-statement replay set
    /// (`DEALLOCATE`/`DISCARD`/`PREPARE`), backend-local temp objects
    /// (`CREATE TEMP`), `WITH HOLD` cursors (`DECLARE`), anonymous blocks whose
    /// body the analyzer cannot see (`DO`), and session-local library loads
    /// (`LOAD`). All are statement-leading keywords, so the leading-keyword scan
    /// keeps `UPDATE t SET …` / `SELECT …` — the hot shapes — away from the C
    /// parser.
    ///
    /// This list gates whether the analyzer runs at all, so a `classify_statement`
    /// arm for a statement absent here is unreachable in production: the two must
    /// be changed together.
    ///
    /// Not exhaustive on its own: `select`-leading statements can still leave
    /// session state (`SELECT … INTO TEMP`), so the callers OR this with the
    /// `temp_object` and `subscription` token flags before deciding to parse.
    #[inline]
    pub(super) fn might_contain_session_state_command(query: &str) -> bool {
        leading_statement_keyword_matches(
            query,
            &[
                "set",
                "reset",
                "deallocate",
                "discard",
                "create",
                "prepare",
                "declare",
                "do",
                "load",
                "call",
            ],
        )
    }

    /// Single-flag view of the fused scan (see `QueryKeywordFlags`): the query
    /// carries a `temp` / `temporary` / `pg_temp` token, so it may name an
    /// object in the session's temp schema and must reach the analyzer even
    /// when its leading keyword is not a session-state keyword.
    #[cfg(test)]
    fn might_reference_temp_scoped_object(query: &str) -> bool {
        Self::query_keyword_flags(query).temp_object
    }

    /// Single-flag view of the fused scan (see `QueryKeywordFlags`): session
    /// state left by *function calls* rather than statement-leading keywords,
    /// which the leading-keyword scan cannot see. A match marks the
    /// connection non-migratable without a full parse.
    #[cfg(test)]
    fn might_leave_unmigratable_function_state(query: &str) -> bool {
        Self::query_keyword_flags(query).function_state
    }

    /// Single-pass scan for every keyword the per-query hot path needs:
    /// LISTEN/UNLISTEN, the session-state function calls
    /// (`set_config`, the `pg_advisory_lock` / `pg_try_advisory_lock`
    /// families), and the temp-schema tokens (`temp`, `temporary`, `pg_temp`).
    ///
    /// Scanning once per keyword group would walk the query bytes several
    /// times per message; this makes one pass and sets all flags.
    /// Word-boundary checking matches `contains_token_ci` semantics exactly.
    #[inline]
    #[must_use]
    pub fn query_keyword_flags(query: &str) -> QueryKeywordFlags {
        // Keywords as byte slices — checked longest-first at each position so
        // "unlisten" is not double-counted as "listen" and
        // "pg_advisory_lock_shared" wins over its "pg_advisory_lock" prefix.
        // The `pg_try_advisory_*` spellings need their own tokens: they share
        // no substring with `pg_advisory_lock` ("pg_" + "try_" splits it), so
        // the non-try tokens cannot match them.
        const PG_TRY_ADVISORY_LOCK_SHARED: &[u8] = b"pg_try_advisory_lock_shared"; // 27
        const PG_ADVISORY_LOCK_SHARED: &[u8] = b"pg_advisory_lock_shared"; // 23
        const PG_TRY_ADVISORY_LOCK: &[u8] = b"pg_try_advisory_lock"; // 20
        const PG_ADVISORY_LOCK: &[u8] = b"pg_advisory_lock"; // 16
        const SET_CONFIG: &[u8] = b"set_config"; // 10
        const TEMPORARY: &[u8] = b"temporary"; // 9
        const UNLISTEN: &[u8] = b"unlisten"; // 8
        const PG_TEMP: &[u8] = b"pg_temp"; // 7
        const LISTEN: &[u8] = b"listen"; // 6
        const TEMP: &[u8] = b"temp"; // 4

        let qb = query.as_bytes();
        let len = qb.len();
        let mut flags = QueryKeywordFlags::default();

        // Inline word-boundary check using safe slice indexing.
        let at_word_boundary = |start: usize, end: usize| -> bool {
            let pre_ok = start == 0
                || qb
                    .get(start - 1)
                    .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_');
            let post_ok = qb
                .get(end)
                .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_');
            pre_ok && post_ok
        };
        let matches_at = |i: usize, token: &[u8]| -> bool {
            qb.get(i..i + token.len())
                .is_some_and(|w| w.eq_ignore_ascii_case(token))
                && at_word_boundary(i, i + token.len())
        };

        let mut i = 0usize;
        while i < len {
            if matches_at(i, PG_TRY_ADVISORY_LOCK_SHARED) {
                flags.function_state = true;
                i += PG_TRY_ADVISORY_LOCK_SHARED.len();
            } else if matches_at(i, PG_ADVISORY_LOCK_SHARED) {
                flags.function_state = true;
                i += PG_ADVISORY_LOCK_SHARED.len();
            } else if matches_at(i, PG_TRY_ADVISORY_LOCK) {
                flags.function_state = true;
                i += PG_TRY_ADVISORY_LOCK.len();
            } else if matches_at(i, PG_ADVISORY_LOCK) {
                flags.function_state = true;
                i += PG_ADVISORY_LOCK.len();
            } else if matches_at(i, SET_CONFIG) {
                flags.function_state = true;
                i += SET_CONFIG.len();
            } else if matches_at(i, TEMPORARY) {
                flags.temp_object = true;
                i += TEMPORARY.len();
            } else if matches_at(i, UNLISTEN) {
                flags.subscription = true;
                i += UNLISTEN.len();
            } else if matches_at(i, PG_TEMP) {
                flags.temp_object = true;
                i += PG_TEMP.len();
            } else if matches_at(i, LISTEN) {
                flags.subscription = true;
                i += LISTEN.len();
            } else if matches_at(i, TEMP) {
                flags.temp_object = true;
                i += TEMP.len();
            } else {
                i += 1;
            }
        }
        flags
    }

    pub(super) fn analyze_query(query: &str) -> QueryAnalysis {
        // Derived here rather than taken from the caller's scan: the unmodeled
        // fallback in `accumulate_query_analysis` is only sound when this flag
        // describes the same text that was parsed.
        if let Ok(parsed) = pg_query::parse(query) {
            return Self::analyze_parse_result(
                &parsed.protobuf,
                Self::query_keyword_flags(query).temp_object,
            );
        }

        // The whole text failed to parse. Salvage per-statement analysis for
        // what the scanner can still separate (tracked changes stay
        // tracked), but fail closed
        // regardless: the session-state prefilter fired and the parser could
        // not fully understand the text — either a syntax error the server
        // will reject (severing is harmless) or grammar newer than the
        // embedded parser's PostgreSQL major, where the statement may leave
        // session state the analyzer cannot see and migrating would silently
        // lose it. The scanner can also swallow an unparseable tail entirely,
        // so the ratchet keys off the whole-text failure, not the salvage.
        let mut analysis = QueryAnalysis::default();
        if let Ok(statements) = pg_query::split_with_scanner(query) {
            for statement in statements {
                if let Ok(parsed) = pg_query::parse(statement) {
                    Self::accumulate_query_analysis(
                        &mut analysis,
                        &parsed.protobuf,
                        Self::query_keyword_flags(statement).temp_object,
                    );
                }
            }
        }
        metrics::counter!("pgbattery_gateway_unparsed_statements_total").increment(1);
        analysis.session_changes.push(SessionChange::NonMigratable(
            "statement the embedded SQL parser cannot parse",
        ));
        analysis
    }

    fn analyze_parse_result(
        result: &pg_query::protobuf::ParseResult,
        names_temp_object: bool,
    ) -> QueryAnalysis {
        let mut analysis = QueryAnalysis::default();
        Self::accumulate_query_analysis(&mut analysis, result, names_temp_object);
        analysis
    }

    /// `names_temp_object` is the temp-schema token flag for the text `result`
    /// was parsed from; it arms the unmodeled-statement fallback below.
    fn accumulate_query_analysis(
        analysis: &mut QueryAnalysis,
        result: &pg_query::protobuf::ParseResult,
        names_temp_object: bool,
    ) {
        for raw_stmt in &result.stmts {
            let Some(stmt_node) = raw_stmt.stmt.as_ref().and_then(|stmt| stmt.node.as_ref()) else {
                continue;
            };
            if matches!(
                stmt_node,
                pg_query::protobuf::node::Node::TransactionStmt(_)
            ) {
                analysis.controls_transaction = true;
            }

            match Self::classify_statement(stmt_node) {
                StatementClass::Modeled(Some(change)) => analysis.session_changes.push(change),
                // Narrow fail-safe: an unexamined statement only severs when
                // the text also names a temp-scoped object — what
                // `EXPLAIN ANALYZE CREATE TEMP TABLE …` and
                // `CREATE FUNCTION pg_temp.f()` have in common. The token flag
                // covers the whole parsed text, not the individual statement,
                // so one temp-naming statement arms the fallback for its
                // siblings in the same batch.
                StatementClass::Unmodeled if names_temp_object => {
                    Self::record_unmodeled_statement(stmt_node);
                    analysis.session_changes.push(SessionChange::NonMigratable(
                        "unmodeled statement naming a temp-scoped object",
                    ));
                }
                StatementClass::Unmodeled => Self::record_unmodeled_statement(stmt_node),
                StatementClass::Modeled(None) => {}
            }
        }
    }

    /// Count a statement the classifier has no arm for. The fallback for these
    /// is *migratable* — the unsafe direction — so the tail has to be measured
    /// before that polarity can be revisited.
    fn record_unmodeled_statement(stmt: &pg_query::protobuf::node::Node) {
        metrics::counter!(
            "pgbattery_gateway_unmodeled_statements_total",
            "node_type" => Self::node_type_name(stmt),
        )
        .increment(1);
    }

    /// Variant name of a parse node, for use as a metric label.
    ///
    /// Derived `Debug` writes the name then `(`, so erroring at the first `(`
    /// stops before the payload is formatted. Pinned by `node_type_name_*`.
    fn node_type_name(stmt: &pg_query::protobuf::node::Node) -> String {
        use std::fmt::Write as _;

        struct UpToOpenParen(String);
        impl std::fmt::Write for UpToOpenParen {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                if let Some(end) = s.find('(') {
                    self.0.push_str(&s[..end]);
                    return Err(std::fmt::Error);
                }
                self.0.push_str(s);
                Ok(())
            }
        }

        let mut sink = UpToOpenParen(String::new());
        let _ = write!(sink, "{stmt:?}");
        let name = sink.0.trim();
        // Nothing here comes from client text, so cardinality is bounded by the
        // enum; reject anything unexpected rather than emit it as a label.
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return "unknown".to_owned();
        }
        name.to_owned()
    }

    /// Classify one parsed statement's effect on session state.
    ///
    /// A deny-list, not a positive safety proof: only the node types with an
    /// arm below are examined, and `_ => Unmodeled` catches every other
    /// statement `PostgreSQL` can parse. An `Unmodeled` statement is migrated
    /// unless the query text also names a temp-scoped object (see
    /// `accumulate_query_analysis`), so any construct that leaves session
    /// state *without* naming `temp`/`temporary`/`pg_temp` needs its own arm
    /// here — that is the only way it can ever be severed.
    ///
    /// The plain DML and transaction-control node types are modeled
    /// explicitly, returning `Modeled(None)`, rather than being left to `_`:
    /// they are the hot shapes, and a column or alias named `temp` must not
    /// reach the fallback and sever them.
    fn classify_statement(stmt: &pg_query::protobuf::node::Node) -> StatementClass {
        match stmt {
            pg_query::protobuf::node::Node::ListenStmt(listen_stmt) => StatementClass::Modeled(
                Self::normalize_condition_name(&listen_stmt.conditionname)
                    .map(SessionChange::Listen),
            ),
            pg_query::protobuf::node::Node::UnlistenStmt(unlisten_stmt) => {
                let condition = unlisten_stmt.conditionname.trim();
                StatementClass::Modeled(Some(if condition.is_empty() || condition == "*" {
                    SessionChange::UnlistenAll
                } else {
                    SessionChange::Unlisten(condition.to_string())
                }))
            }
            // A session-scoped SET/RESET leaves GUCs we don't reconstruct on a
            // new backend. SET LOCAL is transaction-scoped and stays
            // migratable.
            pg_query::protobuf::node::Node::VariableSetStmt(set_stmt) => StatementClass::Modeled(
                (!set_stmt.is_local).then_some(SessionChange::SetSessionVar),
            ),
            pg_query::protobuf::node::Node::DeallocateStmt(dealloc) => {
                StatementClass::Modeled(if dealloc.isall {
                    Some(SessionChange::DeallocateAll)
                } else if dealloc.name.is_empty() {
                    None
                } else {
                    Some(SessionChange::Deallocate(dealloc.name.clone()))
                })
            }
            pg_query::protobuf::node::Node::DiscardStmt(discard) => {
                // Only DISCARD ALL touches state the gateway tracks; PLANS /
                // SEQUENCES / TEMP leave prepared statements and LISTEN
                // registrations intact, and none of them *create* state.
                StatementClass::Modeled(
                    matches!(
                        pg_query::protobuf::DiscardMode::try_from(discard.target),
                        Ok(pg_query::protobuf::DiscardMode::DiscardAll)
                    )
                    .then_some(SessionChange::DiscardAll),
                )
            }
            // CREATE TEMP/TEMPORARY TABLE leaves a backend-local temporary
            // relation. `relpersistence == "t"` is the temp marker ("p" =
            // permanent, "u" = unlogged, both of which survive a migrated
            // backend).
            pg_query::protobuf::node::Node::CreateStmt(create_stmt) => StatementClass::Modeled(
                create_stmt
                    .relation
                    .as_ref()
                    .is_some_and(|r| r.relpersistence == "t")
                    .then_some(SessionChange::NonMigratable("temp table")),
            ),
            // `CREATE TEMP TABLE … AS SELECT` parses to `CreateTableAsStmt`,
            // not `CreateStmt`; the temp relation it leaves behind is just as
            // unreconstructable on a migrated backend.
            pg_query::protobuf::node::Node::CreateTableAsStmt(create_as) => {
                StatementClass::Modeled(
                    create_as
                        .into
                        .as_ref()
                        .and_then(|into_clause| into_clause.rel.as_ref())
                        .is_some_and(|r| r.relpersistence == "t")
                        .then_some(SessionChange::NonMigratable("temp table (CREATE TABLE AS)")),
                )
            }
            // A temp view and a temp sequence live in the same session-local
            // `pg_temp` schema as a temp table and die with the backend, but
            // each carries its own `RangeVar`, so the temp-table arms above
            // cannot see them.
            pg_query::protobuf::node::Node::ViewStmt(view_stmt) => StatementClass::Modeled(
                view_stmt
                    .view
                    .as_ref()
                    .is_some_and(|r| r.relpersistence == "t")
                    .then_some(SessionChange::NonMigratable("temp view")),
            ),
            pg_query::protobuf::node::Node::CreateSeqStmt(seq_stmt) => StatementClass::Modeled(
                seq_stmt
                    .sequence
                    .as_ref()
                    .is_some_and(|r| r.relpersistence == "t")
                    .then_some(SessionChange::NonMigratable("temp sequence")),
            ),
            // Legacy `SELECT … INTO TEMP` leaves the same relation as
            // `CREATE TEMP TABLE … AS`, but in the RAW tree libpg_query
            // returns it stays a `SelectStmt` with an `into_clause` — the
            // `CreateTableAsStmt` rewrite happens in parse analysis, which
            // libpg_query does not run.
            pg_query::protobuf::node::Node::SelectStmt(select) => StatementClass::Modeled(
                select
                    .into_clause
                    .as_ref()
                    .and_then(|into_clause| into_clause.rel.as_ref())
                    .is_some_and(|r| r.relpersistence == "t")
                    .then_some(SessionChange::NonMigratable("temp table (SELECT INTO)")),
            ),
            // SQL-level PREPARE: only extended-protocol Parse statements are
            // captured for replay, so a migrated backend would answer a later
            // EXECUTE with "prepared statement does not exist".
            pg_query::protobuf::node::Node::PrepareStmt(_) => {
                StatementClass::Modeled(Some(SessionChange::NonMigratable("SQL PREPARE")))
            }
            // A `DO` block body is procedural-language source that libpg_query
            // does not parse — `DoStmt` carries it as one opaque string — so a
            // block that creates a temp table, takes a session advisory lock,
            // or sets a session GUC is indistinguishable here from one that
            // leaves nothing behind.
            pg_query::protobuf::node::Node::DoStmt(_) => StatementClass::Modeled(Some(
                SessionChange::NonMigratable("DO block (opaque body)"),
            )),
            // `CALL proc()` is opaque for the same reason: the procedure body
            // lives in the catalog, not in the parse tree, so a procedure that
            // creates a temp table or takes a session advisory lock is
            // indistinguishable here from one that leaves nothing behind. A
            // procedure can also COMMIT mid-body, which `DO` cannot.
            //
            // Severing is the documented polarity for anything the analyzer
            // cannot see into, and unlike `LOAD` it is not free: a
            // stored-procedure-heavy workload loses its connections on every
            // failover rather than migrating them. That cost is availability;
            // migrating would trade it for silent session-state loss. If it
            // proves too expensive, the narrower fix is to consult
            // `prokind`/`provolatile` for the target procedure rather than to
            // widen the fallback.
            pg_query::protobuf::node::Node::CallStmt(_) => StatementClass::Modeled(Some(
                SessionChange::NonMigratable("CALL (opaque procedure body)"),
            )),
            // `LOAD 'lib'` links a shared library into this backend for the rest
            // of the session. A migrated backend has not loaded it, so anything
            // the library provides — functions, hooks, custom GUCs — silently
            // stops resolving. Session-local by construction, and rare enough
            // that severing costs effectively nothing.
            pg_query::protobuf::node::Node::LoadStmt(_) => StatementClass::Modeled(Some(
                SessionChange::NonMigratable("LOAD (session-local library)"),
            )),
            // DECLARE ... CURSOR WITH HOLD persists past its transaction; a
            // migrated backend would not have it. Non-HOLD cursors die at
            // transaction end and can never reach an Idle migration point.
            pg_query::protobuf::node::Node::DeclareCursorStmt(decl) => {
                // `CURSOR_OPT_HOLD` bit (PostgreSQL `parsenodes.h`).
                const CURSOR_OPT_HOLD: i32 = 0x0020;
                StatementClass::Modeled(
                    (decl.options & CURSOR_OPT_HOLD != 0)
                        .then_some(SessionChange::NonMigratable("WITH HOLD cursor")),
                )
            }
            node if Self::leaves_no_session_state(node) => StatementClass::Modeled(None),
            _ => StatementClass::Unmodeled,
        }
    }

    /// Node types examined and found to leave nothing that survives to an Idle
    /// migration point.
    ///
    /// Distinct from `Unmodeled`, which means *unexamined*: these are a positive
    /// claim. Plain DML leaves no session state at all (a `set_config` or
    /// advisory-lock call inside one is caught by the token prefilter, which
    /// needs no parse); transaction control, cursor stepping and table locks are
    /// transaction-scoped; `SHOW` is read-only; and the SQL `PREPARE` an
    /// `EXECUTE` names already ratcheted the session when it was issued.
    const fn leaves_no_session_state(stmt: &pg_query::protobuf::node::Node) -> bool {
        matches!(
            stmt,
            pg_query::protobuf::node::Node::InsertStmt(_)
                | pg_query::protobuf::node::Node::UpdateStmt(_)
                | pg_query::protobuf::node::Node::DeleteStmt(_)
                | pg_query::protobuf::node::Node::MergeStmt(_)
                | pg_query::protobuf::node::Node::CopyStmt(_)
                | pg_query::protobuf::node::Node::TransactionStmt(_)
                | pg_query::protobuf::node::Node::FetchStmt(_)
                | pg_query::protobuf::node::Node::ClosePortalStmt(_)
                | pg_query::protobuf::node::Node::LockStmt(_)
                | pg_query::protobuf::node::Node::ExecuteStmt(_)
                | pg_query::protobuf::node::Node::VariableShowStmt(_)
        )
    }

    fn normalize_condition_name(name: &str) -> Option<String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    /// Whether the analyzer finds session state in `query`: a change the
    /// gateway tracks, or one that makes the session non-migratable. Runs the
    /// C parser, so async callers offload it to a blocking thread.
    #[must_use]
    pub fn analyzer_finds_session_state(query: &str) -> bool {
        !Self::analyze_query(query).session_changes.is_empty()
    }

    /// Build a `PostgreSQL` Query message from SQL text.
    #[must_use]
    pub fn build_query_message(sql: &str) -> BytesMut {
        use bytes::BufMut;
        let msg_len = u32::try_from(4 + sql.len() + 1).unwrap_or(u32::MAX);
        let mut msg = BytesMut::with_capacity(5 + sql.len() + 1);
        msg.put_u8(b'Q');
        msg.put_u32(msg_len);
        msg.put_slice(sql.as_bytes());
        msg.put_u8(0); // null terminator
        msg
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
    fn test_build_query_message() {
        let msg = ConnectionHandler::build_query_message("SELECT 1");

        // Check message format: 'Q' + length(4) + query + null terminator
        assert_eq!(msg[0], b'Q');

        // Length should be 4 (length field) + 8 (query) + 1 (null) = 13
        let length = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(length, 13);

        // Check query content
        assert_eq!(&msg[5..13], b"SELECT 1");

        // Check null terminator
        assert_eq!(msg[13], 0);
    }

    #[test]
    fn test_analyze_query_subscription_changes() {
        let analysis = ConnectionHandler::analyze_query(
            r#"LISTEN events; UNLISTEN jobs; UNLISTEN *; LISTEN "MyChannel";"#,
        );
        assert_eq!(
            analysis.session_changes,
            vec![
                SessionChange::Listen("events".to_string()),
                SessionChange::Unlisten("jobs".to_string()),
                SessionChange::UnlistenAll,
                SessionChange::Listen("MyChannel".to_string()),
            ]
        );
    }

    fn sets_session_var(query: &str) -> bool {
        ConnectionHandler::analyze_query(query)
            .session_changes
            .contains(&SessionChange::SetSessionVar)
    }

    #[test]
    fn test_analyze_query_detects_session_set() {
        // A session-scoped SET marks the connection non-migratable.
        assert!(sets_session_var("SET search_path = myschema"));
        assert!(sets_session_var("SET TIME ZONE 'UTC'"));
        assert!(sets_session_var("RESET search_path"));

        // SET LOCAL is transaction-scoped — it must NOT flag the connection.
        assert!(!sets_session_var("SET LOCAL work_mem = '64MB'"));

        // Non-SET queries leave the flag clear.
        assert!(!sets_session_var("SELECT 1"));
        assert!(!sets_session_var("INSERT INTO t VALUES (1)"));

        // A SET anywhere in a multi-statement query flags it.
        assert!(sets_session_var("SELECT 1; SET search_path = x"));
    }

    fn marks_non_migratable(query: &str) -> bool {
        ConnectionHandler::analyze_query(query)
            .session_changes
            .iter()
            .any(|c| matches!(c, SessionChange::NonMigratable(_)))
    }

    /// A statement the embedded parser rejects — a syntax error, or grammar
    /// newer than the parser's `PostgreSQL` major — must sever, not silently
    /// migrate: the analyzer only runs when a session-state prefilter fired,
    /// so an unparseable statement here may have left state on a newer
    /// server that the parser simply cannot see.
    #[test]
    fn test_unparseable_session_state_statement_severs() {
        // Trips the `set` leading-keyword prefilter, then fails to parse.
        assert!(marks_non_migratable("SET application_name TO"));
        // A parseable statement alongside an unparseable one: the tracked
        // change survives AND the connection is ratcheted non-migratable.
        let analysis = ConnectionHandler::analyze_query("LISTEN ch1; LISTEN ((");
        assert!(
            analysis
                .session_changes
                .iter()
                .any(|c| matches!(c, SessionChange::Listen(ch) if ch == "ch1")),
            "the parseable LISTEN must still be tracked: {analysis:?}"
        );
        assert!(
            analysis
                .session_changes
                .iter()
                .any(|c| matches!(c, SessionChange::NonMigratable(_))),
            "the unparseable statement must ratchet non-migratable: {analysis:?}"
        );
    }

    #[test]
    fn test_analyze_query_flags_unmigratable_session_state() {
        // Backend-local state that no replay reconstructs: a transparently
        // migrated client would find its temp table / prepared statement /
        // cursor gone on the new backend, so these must ratchet the session
        // non-migratable.
        assert!(marks_non_migratable("CREATE TEMP TABLE t (id int)"));
        assert!(marks_non_migratable("CREATE TEMPORARY TABLE t (id int)"));
        // CREATE TABLE AS parses to CreateTableAsStmt, a distinct node — the
        // temp form must still flag the connection.
        assert!(marks_non_migratable("CREATE TEMP TABLE t AS SELECT 1"));
        assert!(marks_non_migratable("CREATE TEMPORARY TABLE t AS SELECT 1"));
        assert!(marks_non_migratable("PREPARE p AS SELECT 1"));
        assert!(marks_non_migratable(
            "DECLARE c CURSOR WITH HOLD FOR SELECT 1"
        ));

        // A permanent table is fine — it survives failover on the shared
        // volume, so the connection stays migratable. Both the plain and the
        // `AS SELECT` forms must stay clear.
        assert!(!marks_non_migratable("CREATE TABLE t (id int)"));
        assert!(!marks_non_migratable("CREATE TABLE t AS SELECT 1"));
        // A non-HOLD cursor dies at transaction end and never reaches an Idle
        // migration point, so it must not flag the connection.
        assert!(!marks_non_migratable("DECLARE c CURSOR FOR SELECT 1"));
        assert!(!marks_non_migratable("SELECT 1"));
    }

    /// A temp table is not the only object that lives in the session's
    /// `pg_temp` schema and dies with the backend. A temp view and a temp
    /// sequence each parse to their own node type (`ViewStmt`,
    /// `CreateSeqStmt`), which the temp-*table* arms cannot see.
    #[test]
    fn test_analyze_query_flags_temp_views_and_sequences() {
        assert!(marks_non_migratable("CREATE TEMP VIEW v AS SELECT 1"));
        assert!(marks_non_migratable("CREATE TEMPORARY VIEW v AS SELECT 1"));
        assert!(marks_non_migratable("CREATE TEMP SEQUENCE s"));
        assert!(marks_non_migratable("CREATE TEMPORARY SEQUENCE s START 5"));

        // The permanent spellings are shared objects that exist on the new
        // leader, so they must stay migratable.
        assert!(!marks_non_migratable("CREATE VIEW v AS SELECT 1"));
        assert!(!marks_non_migratable("CREATE SEQUENCE s"));
        // UNLOGGED is a durability choice for a *shared* relation, not a
        // session-scoped one: the relation exists for every session on the new
        // leader, so it is not state the gateway would be hiding.
        assert!(!marks_non_migratable("CREATE UNLOGGED TABLE t (id int)"));
    }

    /// Legacy `SELECT … INTO TEMP` leaves the same backend-local relation as
    /// `CREATE TEMP TABLE … AS`. In the RAW parse tree it stays a `SelectStmt`
    /// carrying an `into_clause` — the `CreateTableAsStmt` rewrite happens in
    /// parse analysis, which `libpg_query` does not run — and `select` is
    /// deliberately absent from the leading-keyword prefilter, so this shape
    /// has to reach the parser by another route.
    #[test]
    fn test_analyze_query_flags_select_into_temp() {
        assert!(marks_non_migratable("SELECT id INTO TEMP t FROM src"));
        assert!(marks_non_migratable("SELECT id INTO TEMPORARY t FROM src"));
        assert!(marks_non_migratable("SELECT id INTO TEMP TABLE t FROM src"));
        assert!(marks_non_migratable(
            "WITH s AS (SELECT 1 AS id) SELECT id INTO TEMP t FROM s"
        ));

        // `SELECT … INTO` a permanent or unlogged table is shared DDL.
        assert!(!marks_non_migratable("SELECT id INTO t FROM src"));
        assert!(!marks_non_migratable("SELECT id INTO UNLOGGED t FROM src"));
        // A column named `temp` is not a temp object.
        assert!(!marks_non_migratable("SELECT temp FROM readings"));
        assert!(!marks_non_migratable(
            "UPDATE readings SET temp = 21.5 WHERE id = 1"
        ));
    }

    /// A `DO` block body is procedural-language source that `libpg_query` does
    /// not parse: `DoStmt` carries it as one opaque string. Any DO block can
    /// therefore create a temp table, take a session advisory lock, or set a
    /// session GUC with nothing visible in the parse tree, so the only sound
    /// classification is non-migratable.
    #[test]
    fn test_analyze_query_flags_do_blocks() {
        assert!(marks_non_migratable(
            "DO $$ BEGIN CREATE TEMP TABLE t (id int); END $$"
        ));
        assert!(marks_non_migratable(
            "DO $$ BEGIN PERFORM pg_advisory_lock(1); END $$"
        ));
        // A state-free body is indistinguishable from a stateful one here.
        assert!(marks_non_migratable("DO $$ BEGIN PERFORM 1; END $$"));
        assert!(marks_non_migratable(
            "DO LANGUAGE plpgsql $$ BEGIN PERFORM 1; END $$"
        ));
    }

    /// A `classify_statement` arm is only reachable if the prefilter routes the
    /// statement to the analyzer at all. `marks_non_migratable` calls
    /// `analyze_query` directly, so it cannot see a missing gate keyword — an arm
    /// added without one is dead code that still passes its own test.
    #[test]
    fn test_session_state_prefilter_admits_every_gated_statement() {
        for query in [
            "SET work_mem = '4MB'",
            "RESET ALL",
            "DEALLOCATE ALL",
            "DISCARD ALL",
            "CREATE TEMP TABLE t (id int)",
            "PREPARE p AS SELECT 1",
            "DECLARE c CURSOR WITH HOLD FOR SELECT 1",
            "DO $$ BEGIN PERFORM 1; END $$",
            "LOAD 'auto_explain'",
            "CALL refresh_totals()",
        ] {
            assert!(
                ConnectionHandler::might_contain_session_state_command(query),
                "prefilter drops {query:?}, so its analyzer arm never runs"
            );
        }
        // The hot shapes must stay off the C parser.
        for query in [
            "SELECT id FROM readings WHERE id = 1",
            "UPDATE readings SET temp = 21.5 WHERE id = 1",
            "INSERT INTO readings (temp) VALUES (1)",
        ] {
            assert!(
                !ConnectionHandler::might_contain_session_state_command(query),
                "prefilter sends {query:?} to the parser"
            );
        }
    }

    /// The metric label depends on how `Debug` is derived for the node enum. If
    /// that changes, these fail rather than the label silently becoming junk.
    #[test]
    fn node_type_name_is_the_variant_name() {
        for (sql, expected) in [
            ("EXPLAIN ANALYZE SELECT 1", "ExplainStmt"),
            ("VACUUM readings", "VacuumStmt"),
            ("ALTER TABLE readings ADD COLUMN c int", "AlterTableStmt"),
            ("GRANT SELECT ON readings TO alice", "GrantStmt"),
            ("LOAD 'auto_explain'", "LoadStmt"),
        ] {
            let parsed = pg_query::parse(sql).unwrap();
            let node = parsed.protobuf.stmts[0]
                .stmt
                .as_ref()
                .and_then(|s| s.node.as_ref())
                .unwrap();
            assert_eq!(ConnectionHandler::node_type_name(node), expected);
        }
    }

    /// A label must never carry parse-tree payload: unbounded cardinality, and
    /// it would put client text into a metric.
    #[test]
    fn node_type_name_excludes_the_payload() {
        let parsed = pg_query::parse("VACUUM some_table_named_here").unwrap();
        let node = parsed.protobuf.stmts[0]
            .stmt
            .as_ref()
            .and_then(|s| s.node.as_ref())
            .unwrap();
        let name = ConnectionHandler::node_type_name(node);
        assert!(!name.contains("some_table_named_here"), "{name}");
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric()), "{name}");
    }

    /// `LOAD` links a shared library into this backend only. A migrated backend
    /// has not loaded it, so the functions and hooks it provides stop resolving
    /// with nothing in the parse tree to hint why.
    #[test]
    fn test_analyze_query_flags_load() {
        assert!(marks_non_migratable("LOAD 'auto_explain'"));
        assert!(marks_non_migratable("load 'pg_stat_statements'"));
        // A LOAD anywhere in a multi-statement batch ratchets the session.
        assert!(marks_non_migratable("SELECT 1; LOAD 'auto_explain'"));
        // The token prefilter must not be what catches it: no bare mention of
        // the word in ordinary SQL may sever a session.
        assert!(!marks_non_migratable(
            "SELECT payload FROM jobs WHERE kind = 'load'"
        ));
        assert!(!marks_non_migratable(
            "INSERT INTO audit (action) VALUES ('LOAD')"
        ));
    }

    /// The `_ => Unmodeled` fallback stays migratable, and the argument rests
    /// entirely on this: a plain `SELECT` is classified by its own arm, so
    /// `SELECT my_func()` never reaches the fallback and flipping it could not
    /// close the function-body hole. If a refactor ever routes `SelectStmt`
    /// through the fallback, that argument silently stops holding.
    #[test]
    fn select_is_classified_by_its_own_arm_not_the_fallback() {
        for query in [
            "SELECT my_func()",
            "SELECT id FROM readings WHERE id = 1",
            "SELECT 1",
        ] {
            let parsed = pg_query::parse(query).unwrap();
            let protobuf = parsed.protobuf;
            let node = protobuf
                .stmts
                .first()
                .and_then(|s| s.stmt.as_ref())
                .and_then(|s| s.node.as_ref())
                .unwrap();
            assert!(
                !matches!(
                    ConnectionHandler::classify_statement(node),
                    StatementClass::Unmodeled
                ),
                "{query:?} reached the Unmodeled fallback; the fallback's reasoning no longer holds"
            );
        }
    }

    /// `CALL` runs a procedure body the analyzer cannot see: it lives in the
    /// catalog, not the parse tree. Unlike `LOAD`, severing has a real cost for
    /// stored-procedure-heavy workloads, so the polarity is deliberate.
    #[test]
    fn test_analyze_query_flags_call() {
        assert!(marks_non_migratable("CALL refresh_totals()"));
        assert!(marks_non_migratable("call refresh_totals(1, 'x')"));
        // A CALL anywhere in a multi-statement batch ratchets the session.
        assert!(marks_non_migratable("SELECT 1; CALL refresh_totals()"));
        // Naming the word must not sever: only a leading CALL is the statement.
        assert!(!marks_non_migratable(
            "SELECT payload FROM jobs WHERE kind = 'call'"
        ));
        assert!(!marks_non_migratable(
            "INSERT INTO audit (action) VALUES ('CALL')"
        ));
        // A function *call* in a SELECT is not a CALL statement and must stay
        // on the hot path — this is the shape that would make the arm costly if
        // it over-matched.
        assert!(!marks_non_migratable("SELECT refresh_totals()"));
    }

    /// The analyzer is a deny-list: a node type it has no arm for is
    /// *unexamined*, not proven state-free. When such a statement also names a
    /// temp-scoped object the fallback must ratchet rather than migrate.
    #[test]
    fn test_analyze_query_flags_unmodeled_statements_naming_temp_objects() {
        // EXPLAIN ANALYZE executes its inner statement, so this really does
        // leave a temp table behind; `ExplainStmt` has no arm.
        assert!(marks_non_migratable(
            "EXPLAIN ANALYZE CREATE TEMP TABLE t AS SELECT 1"
        ));
        // A function created in the session's temp schema dies with the
        // backend; `CreateFunctionStmt` has no arm.
        assert!(marks_non_migratable(
            "CREATE FUNCTION pg_temp.f() RETURNS int AS 'SELECT 1' LANGUAGE sql"
        ));

        // Ordinary DML and reads are modeled arms, so a column named `temp`
        // cannot reach the fallback and sever them.
        assert!(!marks_non_migratable("UPDATE readings SET temp = 1"));
        assert!(!marks_non_migratable(
            "INSERT INTO readings (temp) VALUES (1)"
        ));
        assert!(!marks_non_migratable(
            "DELETE FROM readings WHERE temp > 30"
        ));
        assert!(!marks_non_migratable("SELECT avg(temp) FROM readings"));
        assert!(!marks_non_migratable("SELECT temp AS temp FROM readings"));
        // `DISCARD TEMP` drops temp objects; it cannot create any.
        assert!(!marks_non_migratable("DISCARD TEMP"));
        // Transaction-scoped statements naming a temp relation stay migratable
        // (the CREATE that made the relation already ratcheted the session).
        assert!(!marks_non_migratable("LOCK TABLE temp IN SHARE MODE"));
    }

    /// Server-side large object descriptors are transaction-scoped, not
    /// session-scoped: `PostgreSQL` closes every descriptor still open at
    /// transaction end, so an Idle connection — the only state the gateway
    /// ever migrates from — holds none. The large object itself is a shared,
    /// WAL-logged relation. Opening or creating one must therefore NOT sever
    /// the session.
    #[test]
    fn test_large_object_calls_stay_migratable() {
        for query in [
            "SELECT lo_open(16384, 262144)",
            "SELECT lo_creat(-1)",
            "SELECT lo_create(0)",
            "SELECT lo_import('/srv/x.bin')",
            "SELECT lo_export(16384, '/srv/x.bin')",
            "SELECT lo_get(16384)",
            "SELECT loread(0, 32)",
        ] {
            assert!(
                !ConnectionHandler::might_leave_unmigratable_function_state(query),
                "`{query}` opens no session-scoped state and must stay migratable"
            );
            assert!(!marks_non_migratable(query));
        }
    }

    #[test]
    fn test_function_state_prefilter_detects_set_config_and_advisory_locks() {
        assert!(ConnectionHandler::might_leave_unmigratable_function_state(
            "SELECT set_config('search_path', 'x', false)"
        ));
        assert!(ConnectionHandler::might_leave_unmigratable_function_state(
            "SELECT pg_advisory_lock(42)"
        ));
        assert!(ConnectionHandler::might_leave_unmigratable_function_state(
            "SELECT pg_advisory_lock_shared(42)"
        ));
        // Transaction-scoped advisory locks release at commit — not session
        // state — and plain reads must stay migratable.
        assert!(!ConnectionHandler::might_leave_unmigratable_function_state(
            "SELECT pg_advisory_xact_lock(42)"
        ));
        assert!(!ConnectionHandler::might_leave_unmigratable_function_state(
            "SELECT id FROM t WHERE id = 1"
        ));
    }

    /// The `pg_try_advisory_lock` family is session-scoped exactly like
    /// `pg_advisory_lock` — it differs only in returning false instead of
    /// blocking. A caller that acquires one holds a lock the gateway cannot
    /// reconstruct on a migrated backend, so it must mark the session
    /// non-migratable. Only the `_xact_` variants release at commit.
    #[test]
    fn test_function_state_prefilter_detects_try_advisory_locks() {
        assert!(ConnectionHandler::might_leave_unmigratable_function_state(
            "SELECT pg_try_advisory_lock(42)"
        ));
        assert!(ConnectionHandler::might_leave_unmigratable_function_state(
            "SELECT pg_try_advisory_lock_shared(42)"
        ));
        // Transaction-scoped `try` variants still release at commit.
        assert!(!ConnectionHandler::might_leave_unmigratable_function_state(
            "SELECT pg_try_advisory_xact_lock(42)"
        ));
    }

    #[test]
    fn test_analyze_query_deallocate_and_discard() {
        assert_eq!(
            ConnectionHandler::analyze_query("DEALLOCATE stmt1").session_changes,
            vec![SessionChange::Deallocate("stmt1".to_string())]
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DEALLOCATE PREPARE stmt1").session_changes,
            vec![SessionChange::Deallocate("stmt1".to_string())]
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DEALLOCATE ALL").session_changes,
            vec![SessionChange::DeallocateAll]
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DISCARD ALL").session_changes,
            vec![SessionChange::DiscardAll]
        );
        // DISCARD PLANS / SEQUENCES / TEMP leave prepared statements and
        // LISTEN registrations alone.
        assert_eq!(
            ConnectionHandler::analyze_query("DISCARD PLANS").session_changes,
            Vec::<SessionChange>::new()
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DISCARD SEQUENCES").session_changes,
            Vec::<SessionChange>::new()
        );
    }

    #[test]
    fn test_analyze_query_preserves_statement_order() {
        // `SET x=1; DISCARD ALL` ends with clean session state while
        // `DISCARD ALL; SET x=1` does not — order must survive analysis.
        assert_eq!(
            ConnectionHandler::analyze_query("SET search_path = x; DISCARD ALL").session_changes,
            vec![SessionChange::SetSessionVar, SessionChange::DiscardAll]
        );
        assert_eq!(
            ConnectionHandler::analyze_query("DISCARD ALL; SET search_path = x").session_changes,
            vec![SessionChange::DiscardAll, SessionChange::SetSessionVar]
        );
    }

    #[test]
    fn test_first_statement_keyword() {
        assert_eq!(first_statement_keyword("SET x = 1"), Some("SET"));
        assert_eq!(first_statement_keyword("  \t\nset x"), Some("set"));
        assert_eq!(first_statement_keyword("/*c*/ SET x"), Some("SET"));
        assert_eq!(
            first_statement_keyword("-- line\n/* outer /* inner */ */ RESET x"),
            Some("RESET")
        );
        assert_eq!(first_statement_keyword("UPDATE t SET x=1"), Some("UPDATE"));
        // Unterminated comment / empty / punctuation starts resolve to None.
        assert_eq!(first_statement_keyword("/* never closed"), None);
        assert_eq!(first_statement_keyword(""), None);
        assert_eq!(first_statement_keyword("  ;"), None);
    }

    #[test]
    fn test_session_state_prefilter_leading_keyword_only() {
        // The hottest write shape must NOT pay for the C parser.
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET x=1"
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET x=1;"
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2"
        ));

        // Leading SET/RESET in various dressings must trigger.
        assert!(ConnectionHandler::might_contain_session_state_command(
            "set search_path = x"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            " /*c*/ SET x = 1"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            "RESET ALL"
        ));

        // Per-statement detection in multi-statement strings.
        assert!(ConnectionHandler::might_contain_session_state_command(
            "begin; set local lock_timeout = '1s'; select 1"
        ));

        // DEALLOCATE / DISCARD are session-state statements too.
        assert!(ConnectionHandler::might_contain_session_state_command(
            "DEALLOCATE stmt1"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            "DISCARD ALL"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            "select 1; deallocate all"
        ));

        // Identifier prefixes don't count as the keyword.
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "settings_lookup('a')"
        ));
    }

    #[test]
    fn test_session_state_prefilter_reaches_do_blocks_and_temp_targets() {
        // `DO` is statement-leading and its body is opaque to the parser, so
        // the statement itself must reach the analyzer.
        assert!(ConnectionHandler::might_contain_session_state_command(
            "DO $$ BEGIN PERFORM 1; END $$"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            "select 1; do $$ begin perform 1; end $$"
        ));

        // `select` / `with` stay off the leading-keyword list (the read hot
        // path must not pay for the C parser), so a temp-object token is what
        // routes `SELECT … INTO TEMP` to the analyzer.
        assert!(ConnectionHandler::might_reference_temp_scoped_object(
            "SELECT id INTO TEMP t FROM src"
        ));
        assert!(ConnectionHandler::might_reference_temp_scoped_object(
            "SELECT id INTO TEMPORARY TABLE t FROM src"
        ));
        assert!(ConnectionHandler::might_reference_temp_scoped_object(
            "CREATE FUNCTION pg_temp.f() RETURNS int AS 'SELECT 1' LANGUAGE sql"
        ));

        // Identifiers that merely embed the token do not match, so the read
        // hot path keeps away from the parser.
        assert!(!ConnectionHandler::might_reference_temp_scoped_object(
            "SELECT temp_c FROM readings"
        ));
        assert!(!ConnectionHandler::might_reference_temp_scoped_object(
            "SET LOCAL temp_buffers = '8MB'"
        ));
        assert!(!ConnectionHandler::might_reference_temp_scoped_object(
            "UPDATE t SET x = 1"
        ));
    }

    #[test]
    fn test_multi_statement_detection_ignores_quoted_semicolons() {
        // A `;` inside a literal, identifier, dollar-quoted string, or
        // comment is not a statement boundary — a plain OLTP UPDATE whose
        // literal happens to contain one (URLs, serialized blobs) must not
        // pay for the C parser.
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET note = 'a;b' WHERE id = 1"
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET url = 'https://x.test/?a=1;b=2'"
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            r#"UPDATE "weird;name" SET x = 1"#
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET body = $tag$a;b$tag$"
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET x = 1 -- set; trailing comment"
        ));
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t /* set; */ SET x = 1"
        ));
        // An escaped quote inside a literal must not end it early.
        assert!(!ConnectionHandler::might_contain_session_state_command(
            "UPDATE t SET note = 'it''s;fine'"
        ));

        // Real top-level semicolons still trigger the fallback.
        assert!(ConnectionHandler::might_contain_session_state_command(
            "select 1; set search_path = x"
        ));
        assert!(ConnectionHandler::might_contain_session_state_command(
            "update t set note = 'a;b'; discard all"
        ));
    }

    /// M16: the scanner read `E'a\'b'` as closing at `\'`, then took the real
    /// closing quote for an opening one and swallowed `; SET …`; and it read
    /// `a$b$c` as a dollar quote that never closes. Either way the session
    /// SET went unseen and a migration silently dropped it.
    #[test]
    fn a_top_level_semicolon_is_never_masked() {
        for masked in [
            r"SELECT E'a\'b'; SET search_path = x",
            r"SELECT E'\''; SET search_path = x",
            r"SELECT 'a\'; SET search_path = x",
            "SELECT a$b$c FROM t; SET search_path = x",
            "SELECT 1 AS x$y; PREPARE p AS SELECT 1",
        ] {
            assert!(has_top_level_semicolon(masked), "{masked}");
            assert!(
                ConnectionHandler::might_contain_session_state_command(masked),
                "{masked}"
            );
        }
        // Dollar quotes still hide their own semicolons, whatever the tag.
        assert!(!has_top_level_semicolon("SELECT $fn$a;b$fn$"));
        assert!(!has_top_level_semicolon("SELECT $é$a;b$é$"));
    }

    #[test]
    fn test_contains_token_ci_word_boundaries() {
        assert!(ConnectionHandler::contains_token_ci("COMMIT;", "commit"));
        assert!(ConnectionHandler::contains_token_ci("END", "end"));
        assert!(!ConnectionHandler::contains_token_ci("send_mail()", "end"));
        assert!(!ConnectionHandler::contains_token_ci("listener", "listen"));
        assert!(ConnectionHandler::contains_token_ci(
            "UNLISTEN channel",
            "unlisten"
        ));
    }

    #[test]
    fn test_query_prefilter_helpers() {
        assert!(ConnectionHandler::might_contain_subscription_command(
            "LISTEN events"
        ));
        assert!(ConnectionHandler::might_contain_subscription_command(
            "UNLISTEN *"
        ));
        assert!(!ConnectionHandler::might_contain_subscription_command(
            "SELECT listener_count FROM metrics"
        ));
    }
}

/// The differential classifier oracle.
///
/// The classifier is a deny-list over a language with roughly two hundred
/// statement types, so its correctness cannot be argued from its own arms. It
/// needs a reference, and the only true reference for "did this statement leave
/// session state behind" is a `PostgreSQL` session and its catalogs. This runs
/// candidate SQL in a real session, snapshots the six catalogs that expose
/// backend-local state either side of it, and asserts the one direction that
/// matters: **if session state changed, the gateway must have marked the
/// connection non-migratable.** The converse is not asserted — over-marking
/// severs a connection that could have moved, which is the safe direction and
/// the whole point of a deny-list.
///
/// Skipped unless `PGBATTERY_ORACLE_PSQL` names a command that runs `psql`
/// against a scratch database, because `cargo test` in CI has neither:
///
/// ```text
/// PGBATTERY_ORACLE_PSQL='docker compose exec -T node1 psql -U postgres -h 127.0.0.1 -p 5434' \
///     cargo test --lib session_state_oracle -- --nocapture
/// ```
#[cfg(test)]
mod session_state_oracle {
    use super::{ConnectionHandler, SessionChange};
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    const ORACLE_ENV: &str = "PGBATTERY_ORACLE_PSQL";

    /// Set wherever the oracle is meant to run, so that a missing or misspelled
    /// `PGBATTERY_ORACLE_PSQL` fails the job instead of skipping it. A test
    /// that silently declines to run reports exactly like one that ran and
    /// found nothing, which is the whole failure mode this file exists to
    /// close.
    const ORACLE_REQUIRED_ENV: &str = "PGBATTERY_ORACLE_REQUIRED";

    /// The six catalogs, each narrowed to this backend.
    ///
    /// Narrowing is what makes the reading a fact about the session rather than
    /// about the instance: `pg_locks` and `pg_class` are cluster-wide, so an
    /// unscoped count would move when anything else on the node took an
    /// advisory lock or made a temp table, and the oracle would report a
    /// violation belonging to another session.
    const FINGERPRINT: &str = "(SELECT count(*)::text FROM pg_prepared_statements) || '/' || \
        (SELECT count(*)::text FROM pg_cursors) || '/' || \
        (SELECT count(*)::text FROM pg_locks \
            WHERE locktype = 'advisory' AND pid = pg_backend_pid()) || '/' || \
        (SELECT count(*)::text FROM pg_listening_channels()) || '/' || \
        (SELECT count(*)::text FROM pg_class \
            WHERE relpersistence = 't' AND relnamespace = pg_my_temp_schema()) || '/' || \
        (SELECT count(*)::text FROM pg_settings WHERE source = 'session')";

    /// One candidate. `cleanup` runs after the second snapshot, so a sample may
    /// leave a permanent object behind without poisoning the next one.
    struct Sample {
        sql: &'static str,
        setup: Option<&'static str>,
        cleanup: Option<&'static str>,
    }

    const fn s(sql: &'static str) -> Sample {
        Sample {
            sql,
            setup: None,
            cleanup: None,
        }
    }

    const fn s_around(sql: &'static str, setup: &'static str, cleanup: &'static str) -> Sample {
        Sample {
            sql,
            setup: Some(setup),
            cleanup: Some(cleanup),
        }
    }

    /// Deliberately mixed. The statements that leave nothing behind are not
    /// padding: they are what stops this passing because every sample happens
    /// to be flagged, which a classifier that marked everything would achieve
    /// too.
    fn samples() -> Vec<Sample> {
        vec![
            s("SELECT 1"),
            s("SELECT pg_advisory_lock(4242)"),
            s("SELECT pg_try_advisory_lock(4243)"),
            s("SELECT pg_advisory_lock_shared(4244)"),
            s("SELECT set_config('search_path', 'public', false)"),
            s("CREATE TEMP TABLE oracle_tmp (id int)"),
            s("CREATE TEMP TABLE oracle_tmp_as AS SELECT 1 AS id"),
            s("PREPARE oracle_ps AS SELECT 1"),
            s("LISTEN oracle_channel"),
            s("SET search_path = pg_catalog, public"),
            s("BEGIN; DECLARE oracle_cur CURSOR WITH HOLD FOR SELECT 1; COMMIT"),
            s_around(
                "CREATE TABLE oracle_perm (id int)",
                "DROP TABLE IF EXISTS oracle_perm",
                "DROP TABLE IF EXISTS oracle_perm",
            ),
            s_around(
                "INSERT INTO oracle_perm2 VALUES (1)",
                "CREATE TABLE IF NOT EXISTS oracle_perm2 (id int)",
                "DROP TABLE IF EXISTS oracle_perm2",
            ),
            s("BEGIN; SET LOCAL search_path = public; COMMIT"),
        ]
    }

    /// What the gateway does about a statement's session state.
    ///
    /// Severing is not the only correct answer, and an oracle that demanded it
    /// would report `LISTEN` as a defect: the gateway tracks channels and
    /// replays them on the new backend, so that state survives migration by
    /// being reconstructed rather than by refusing to migrate. Only `Unnoticed`
    /// is a defect — state left behind that nothing recorded and nothing
    /// severed for, which is the case where a client silently loses it.
    #[derive(Debug, PartialEq, Eq)]
    enum Verdict {
        /// The connection is marked non-migratable and failover severs it.
        Severs,
        /// The change is tracked and replayed onto the migrated backend.
        Replays,
        /// The gateway saw nothing.
        Unnoticed,
    }

    /// What the gateway concludes, prefilters and analyzer together.
    ///
    /// The analyzer alone is not the verdict, and asking it alone would make
    /// this oracle disagree with the running gateway: `analyze_query` runs only
    /// when a prefilter recognises the statement, and function-call state
    /// (`set_config`, the advisory-lock family) never reaches it because that
    /// flag is authoritative on its own. Going through the gateway is what
    /// catches an arm sitting unreachable in production while its own test
    /// passes, the test having called the analyzer directly and so been unable
    /// to see a missing gate keyword.
    fn gateway_verdict(query: &str) -> Verdict {
        let flags = ConnectionHandler::query_keyword_flags(query);
        if flags.function_state {
            return Verdict::Severs;
        }
        if !(ConnectionHandler::might_contain_session_state_command(query)
            || flags.subscription
            || flags.temp_object)
        {
            return Verdict::Unnoticed;
        }
        let changes = ConnectionHandler::analyze_query(query).session_changes;
        if changes.is_empty() {
            return Verdict::Unnoticed;
        }
        // `SetSessionVar` and `LISTEN "*"` reach `not_migratable` through
        // `apply_session_changes` rather than by being `NonMigratable`, so a
        // predicate that only matched that variant would call both unnoticed.
        let severs = changes.iter().any(|c| match c {
            SessionChange::NonMigratable(_) | SessionChange::SetSessionVar => true,
            SessionChange::Listen(channel) => channel == "*",
            _ => false,
        });
        if severs {
            Verdict::Severs
        } else {
            Verdict::Replays
        }
    }

    /// Run `script` through the configured psql and return its stdout.
    fn psql(prefix: &str, script: &str) -> Result<String, String> {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(format!("{prefix} -tA -q -v ON_ERROR_STOP=1"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("could not spawn psql: {e}"))?;
        child
            .stdin
            .take()
            .ok_or_else(|| "psql stdin unavailable".to_string())?
            .write_all(script.as_bytes())
            .map_err(|e| format!("could not write to psql: {e}"))?;
        let out = child
            .wait_with_output()
            .map_err(|e| format!("psql did not complete: {e}"))?;
        if !out.status.success() {
            return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// The two fingerprints either side of `sample`, from one session.
    ///
    /// One session is the whole point: the state under test is backend-local,
    /// so a before and an after read on different connections would compare two
    /// unrelated backends and never see a change at all.
    fn fingerprints(prefix: &str, sample: &Sample) -> Result<(String, String), String> {
        let script = format!(
            "{};\nSELECT 'FP:' || {FINGERPRINT};\n{};\nSELECT 'FP:' || {FINGERPRINT};\n{};\n",
            sample.setup.unwrap_or("SELECT 1"),
            sample.sql,
            sample.cleanup.unwrap_or("SELECT 1"),
        );
        let out = psql(prefix, &script)?;
        let marks: Vec<&str> = out
            .lines()
            .filter_map(|l| l.trim().strip_prefix("FP:"))
            .collect();
        match marks.as_slice() {
            [before, after] => Ok(((*before).to_owned(), (*after).to_owned())),
            other => Err(format!(
                "expected two fingerprints, got {}: {out}",
                other.len()
            )),
        }
    }

    #[test]
    fn session_state_implies_the_gateway_severs() {
        let Ok(prefix) = std::env::var(ORACLE_ENV) else {
            assert!(
                std::env::var(ORACLE_REQUIRED_ENV).is_err(),
                "{ORACLE_REQUIRED_ENV} is set but {ORACLE_ENV} is not, so the oracle \
                 would have skipped in the one place built to run it"
            );
            eprintln!("{ORACLE_ENV} unset — skipping the differential oracle");
            return;
        };

        let mut changed = 0usize;
        let mut violations: Vec<String> = Vec::new();
        for sample in samples() {
            // A sample that cannot run is a broken sample, not a pass: the
            // corpus is ours, so every entry has to be valid SQL here.
            let (before, after) = match fingerprints(&prefix, &sample) {
                Ok(pair) => pair,
                Err(e) => {
                    violations.push(format!("sample {:?} did not run: {e}", sample.sql));
                    continue;
                }
            };
            let left_state = before != after;
            if left_state {
                changed += 1;
            }
            let verdict = gateway_verdict(sample.sql);
            if left_state && verdict == Verdict::Unnoticed {
                violations.push(format!(
                    "{:?} changed session state ({before} -> {after}) and the gateway \
                     neither severs nor replays it",
                    sample.sql
                ));
            }
            eprintln!("{:<64} state={left_state:<5} {verdict:?}", sample.sql);
        }

        // Without this the oracle passes on a corpus that touched nothing,
        // which is what a broken fingerprint or a psql that quietly did nothing
        // produces — and it would read as the classifier being correct.
        assert!(
            changed >= 5,
            "only {changed} samples changed session state; the fingerprint is not measuring"
        );
        assert!(violations.is_empty(), "{}", violations.join("\n"));
    }
}
