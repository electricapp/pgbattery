//! Benchmarks for gateway hot paths.
//!
//! Run with: `cargo bench`
//! Quick iteration: `cargo bench -- --warm-up-time 1 --measurement-time 3`
//! Flamegraph:      `cargo flamegraph --profile profiling --bench gateway`

use bytes::{BufMut, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pgbattery::gateway::handlers::ConnectionHandler;
use pgbattery::gateway::protocol::{PacketHeader, TransactionStatus, extract_transaction_status};
use std::hint::black_box;

// ---------------------------------------------------------------------------
// Protocol parsing — runs on EVERY message in both directions.
// These should be sub-nanosecond; any regression here is a proxy-wide hit.
// ---------------------------------------------------------------------------

fn bench_protocol_parsing(c: &mut Criterion) {
    let mut g = c.benchmark_group("protocol");

    // PacketHeader::parse is called inside the hot read loop for every message.
    // A 5-byte parse with a be_u32 decode — should be ~1 ns.
    g.bench_function("packet_header/data_row", |b| {
        // DataRow: 'D' + length(4) = 5 bytes header
        let buf = [b'D', 0, 0, 0, 50u8];
        b.iter(|| PacketHeader::parse(black_box(&buf)));
    });

    g.bench_function("packet_header/ready_for_query", |b| {
        // ReadyForQuery: 'Z' + length(5) + status byte
        let buf = [b'Z', 0, 0, 0, 5, b'I'];
        b.iter(|| PacketHeader::parse(black_box(&buf)));
    });

    g.bench_function("packet_header/too_short", |b| {
        // Incomplete header — common when reading partial TCP segments.
        let buf = [b'Z', 0, 0];
        b.iter(|| PacketHeader::parse(black_box(&buf)));
    });

    // extract_transaction_status is called on every ReadyForQuery message.
    // 1 byte payload → enum — should be ~0 ns (const fn).
    g.bench_function("extract_tx_status/idle", |b| {
        b.iter(|| extract_transaction_status(black_box(b"I")));
    });

    g.bench_function("extract_tx_status/in_transaction", |b| {
        b.iter(|| extract_transaction_status(black_box(b"T")));
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// Backend stream scan — simulates processing a real SELECT result.
//
// For every query the proxy scans the backend response looking for
// ReadyForQuery to extract transaction status. This is the main proxy
// overhead per round-trip: O(messages) header parses.
//
// Realistic message sequence for SELECT ... LIMIT N:
//   RowDescription(T) + N×DataRow(D) + CommandComplete(C) + ReadyForQuery(Z)
// ---------------------------------------------------------------------------

/// Build a synthetic backend response buffer: N data rows + bookend messages.
fn build_backend_response(n_rows: usize) -> BytesMut {
    // Fixed 32-byte payload per row — lengths are known constants.
    const ROW_PAYLOAD: &[u8] = b"fake-row-data-32-bytes-padding--"; // 32 bytes
    const ROW_PAYLOAD_LEN: u32 = 32;
    // DataRow length field: 4 (self) + 2 (field_count) + 4 (field_len) + 32 (data)
    const ROW_MSG_LEN: u32 = 4 + 2 + 4 + ROW_PAYLOAD_LEN;
    // CommandComplete tag: 'SELECT' + NUL (7 bytes)
    const TAG: &[u8] = b"SELECT\0";

    // Capacity: RowDescription(~20) + N×DataRow + CommandComplete(~15) + ReadyForQuery(6)
    let capacity = 20 + n_rows * (5 + ROW_MSG_LEN as usize) + 20;
    let mut buf = BytesMut::with_capacity(capacity);

    // RowDescription: 'T' + length + field_count(0) (simplified)
    buf.put_u8(b'T');
    buf.put_u32(6); // length: 4 (self) + 2 (field_count)
    buf.put_u16(0); // 0 fields for simplicity

    // DataRow messages
    for _ in 0..n_rows {
        buf.put_u8(b'D');
        buf.put_u32(ROW_MSG_LEN);
        buf.put_u16(1); // 1 field
        buf.put_i32(32i32); // ROW_PAYLOAD_LEN as i32 (field data length)
        buf.put_slice(ROW_PAYLOAD);
    }

    // CommandComplete: 'C' + length + tag + NUL
    buf.put_u8(b'C');
    buf.put_u32(4 + 7); // length: 4 (self) + 7 (tag)
    buf.put_slice(TAG);

    // ReadyForQuery: 'Z' + length(5) + status('I')
    buf.put_u8(b'Z');
    buf.put_u32(5);
    buf.put_u8(b'I');

    buf
}

/// Scan a backend buffer for `ReadyForQuery` — mirrors the proxy's inner loop.
/// Returns the transaction status if found.
fn scan_for_ready(buf: &[u8]) -> Option<TransactionStatus> {
    let mut pos = 0;
    while pos + PacketHeader::HEADER_SIZE <= buf.len() {
        let header = PacketHeader::parse(buf.get(pos..)?)?;
        let total = header.total_length();
        if pos + total > buf.len() {
            break;
        }
        if matches!(
            header.msg_type,
            pgbattery::gateway::protocol::MessageType::ReadyForQuery
        ) {
            let payload_start = pos + PacketHeader::HEADER_SIZE;
            return extract_transaction_status(buf.get(payload_start..pos + total)?);
        }
        pos += total;
    }
    None
}

fn bench_backend_stream(c: &mut Criterion) {
    let mut g = c.benchmark_group("backend_stream_scan");

    for n_rows in [1, 10, 100, 1000] {
        let buf = build_backend_response(n_rows);
        g.throughput(Throughput::Elements(n_rows as u64 + 3)); // rows + RowDesc + CC + RFQ
        g.bench_with_input(BenchmarkId::new("rows", n_rows), &buf, |b, buf| {
            b.iter(|| scan_for_ready(black_box(buf)));
        });
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// Query analysis pre-filter — called on every simple-protocol Query message
// to decide whether to invoke the C parser at all.
//
// In practice: `might_contain_commit_command` + `might_contain_subscription_command`
// are both evaluated on every Q message. The combined cost must stay tiny
// relative to the network round-trip.
// ---------------------------------------------------------------------------

fn bench_query_prefilter(c: &mut Criterion) {
    let mut g = c.benchmark_group("query_prefilter");

    // The hot path: most queries are plain DML with no keywords.
    // Both heuristics must short-circuit immediately.
    g.bench_function("select/both_miss", |b| {
        let q = "SELECT id, name, email FROM users WHERE id = $1";
        b.iter(|| {
            black_box(ConnectionHandler::contains_token_ci(q, "commit"))
                | black_box(ConnectionHandler::contains_token_ci(q, "end"))
                | black_box(ConnectionHandler::contains_token_ci(q, "listen"))
                | black_box(ConnectionHandler::contains_token_ci(q, "unlisten"))
        });
    });

    g.bench_function("insert/both_miss", |b| {
        let q = "INSERT INTO events (user_id, action, ts) VALUES ($1, $2, NOW())";
        b.iter(|| {
            black_box(ConnectionHandler::contains_token_ci(q, "commit"))
                | black_box(ConnectionHandler::contains_token_ci(q, "end"))
                | black_box(ConnectionHandler::contains_token_ci(q, "listen"))
                | black_box(ConnectionHandler::contains_token_ci(q, "unlisten"))
        });
    });

    // COMMIT — commit check fires, subscription check misses.
    g.bench_function("commit/commit_fires", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::contains_token_ci("COMMIT", "commit"))
                | black_box(ConnectionHandler::contains_token_ci("COMMIT", "end"))
                | black_box(ConnectionHandler::contains_token_ci("COMMIT", "listen"))
                | black_box(ConnectionHandler::contains_token_ci("COMMIT", "unlisten"))
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// contains_token_ci — word-boundary keyword heuristic, called 4× per query
// ---------------------------------------------------------------------------

fn bench_contains_token_ci(c: &mut Criterion) {
    let mut g = c.benchmark_group("contains_token_ci");

    g.bench_function("select/no_match", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::contains_token_ci(
                black_box("SELECT id, name FROM users WHERE id = $1"),
                black_box("commit"),
            ));
        });
    });

    g.bench_function("long_select/no_match", |b| {
        let q = "SELECT u.id, u.name, u.email, o.id AS order_id, o.total \
                 FROM users u JOIN orders o ON u.id = o.user_id \
                 WHERE u.active = true AND o.created_at > '2024-01-01' \
                 ORDER BY o.created_at DESC LIMIT 100";
        b.iter(|| ConnectionHandler::contains_token_ci(black_box(q), black_box("commit")));
    });

    g.bench_function("commit/match_start", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::contains_token_ci(
                black_box("COMMIT"),
                black_box("commit"),
            ));
        });
    });

    g.bench_function("committer/boundary_rejection", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::contains_token_ci(
                black_box("UPDATE committers SET name = $1 WHERE id = $2"),
                black_box("commit"),
            ));
        });
    });

    g.bench_function("listen/match", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::contains_token_ci(
                black_box("LISTEN events"),
                black_box("listen"),
            ));
        });
    });

    g.bench_function("listener/boundary_rejection", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::contains_token_ci(
                black_box("SELECT listener_id FROM subs"),
                black_box("listen"),
            ));
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// query_keyword_flags — single-pass scan replacing 4 × contains_token_ci.
//
// In the hot path each Query message runs:
//   might_contain_commit_command (2 scans) + might_contain_subscription_command (2 scans)
// The single-pass version should be ~4× faster than the combined prefilter above.
// ---------------------------------------------------------------------------

fn bench_query_keyword_flags(c: &mut Criterion) {
    let mut g = c.benchmark_group("query_keyword_flags");

    g.bench_function("select/all_miss", |b| {
        let q = "SELECT id, name, email FROM users WHERE id = $1";
        b.iter(|| ConnectionHandler::query_keyword_flags(black_box(q)));
    });

    g.bench_function("insert/all_miss", |b| {
        let q = "INSERT INTO events (user_id, action, ts) VALUES ($1, $2, NOW())";
        b.iter(|| ConnectionHandler::query_keyword_flags(black_box(q)));
    });

    g.bench_function("commit/commit_fires", |b| {
        b.iter(|| ConnectionHandler::query_keyword_flags(black_box("COMMIT")));
    });

    g.bench_function("listen/listen_fires", |b| {
        b.iter(|| ConnectionHandler::query_keyword_flags(black_box("LISTEN events")));
    });

    g.bench_function("unlisten/unlisten_fires", |b| {
        b.iter(|| ConnectionHandler::query_keyword_flags(black_box("UNLISTEN *")));
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// is_commit_query — full detection pipeline (heuristic + optional C parse)
// ---------------------------------------------------------------------------

fn bench_is_commit_query(c: &mut Criterion) {
    let mut g = c.benchmark_group("is_commit_query");

    g.bench_function("select/heuristic_short_circuit", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::is_commit_query(black_box(
                "SELECT id FROM users WHERE id = $1",
            )));
        });
    });

    g.bench_function("end_column/parse_eliminates", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::is_commit_query(black_box(
                "SELECT end_time FROM events WHERE id = $1",
            )));
        });
    });

    g.bench_function("commit/true_positive", |b| {
        b.iter(|| ConnectionHandler::is_commit_query(black_box("COMMIT")));
    });

    g.bench_function("end_transaction/true_positive", |b| {
        b.iter(|| ConnectionHandler::is_commit_query(black_box("END TRANSACTION")));
    });

    g.bench_function("multi_stmt_commit/true_positive", |b| {
        b.iter(|| {
            black_box(ConnectionHandler::is_commit_query(black_box(
                "INSERT INTO audit (msg) VALUES ($1); COMMIT;",
            )));
        });
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_protocol_parsing,
    bench_backend_stream,
    bench_query_prefilter,
    bench_contains_token_ci,
    bench_query_keyword_flags,
    bench_is_commit_query,
);
criterion_main!(benches);
