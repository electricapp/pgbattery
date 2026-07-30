#!/usr/bin/env -S uv run --project testing python
"""Dual-writability prober: a direct measurement of contract L1.

`docs/CONTRACTS.md` L1 (FATAL) states: "At most one node may be in
write-accepting state at any point in time."  This module measures exactly
that proposition and nothing else.

WHY THIS EXISTS — WHAT THE REST OF THE HARNESS ACTUALLY MEASURES
────────────────────────────────────────────────────────────────
`correctness_lite.py` invariant I4 samples the three management APIs every
0.5 s and asserts they agree on a single Raft leader id.  That is a
*control-plane agreement* check, not an L1 check.  The two propositions come
apart in the case that matters most:

  - A deposed leader whose fence has not landed yet is simultaneously
    Raft-consistent (everyone agrees node B is leader) and still
    PostgreSQL-writable (node A's `default_transaction_read_only` is still
    `off`).  I4 sees nothing.
  - The lease/fence enforcement loop in `src/app.rs` ticks at 100 ms with a
    1 s SQL budget, so a dual-write window can easily be shorter than I4's
    500 ms sampling period.  Such a window is *structurally* unobservable to
    I4 — it is not a matter of luck.

`ci_matrix.yaml`'s `stale-leader-fencing` case does probe writability
directly, but exactly once, at one point in time.  A window that opens and
closes between two matrix steps is invisible to it.

This prober closes that hole: it attempts a genuine write against all three
nodes' internal PostgreSQL ports *concurrently*, at a high fixed cadence
(50 ms by default), for the whole duration of a fault schedule, and asserts
that at most one attempt is ever confirmed accepted.

WHY THE PROBE MUST BE A REAL WRITE
──────────────────────────────────
pgbattery's fence is `ALTER SYSTEM SET default_transaction_read_only = 'on'`
plus `pg_terminate_backend` over client backends (see
`crates/pgbattery-supervisor/src/process.rs` and
`App::terminate_client_backends` in `src/app.rs`).  It does **not** demote the
postmaster, so a correctly fenced ex-primary still reports
`pg_is_in_recovery() = false`.  Any oracle keyed on recovery state would
report false positives forever.  The only sound signal is whether a normal
client's write is accepted.

For the same reason the probe never issues `SET transaction_read_only = off`,
`SET default_transaction_read_only = off`, or `BEGIN READ WRITE`: those are
precisely the overrides the fence deliberately tolerates for superuser
sessions, and using them would measure "can a privileged session bypass the
fence" instead of "is this node write-accepting".

THE PROBE WRITE
───────────────
A single autocommit UPSERT into a dedicated table:

    INSERT INTO pgb_dual_write_probe (node_id, round_seq, probe_ns)
    VALUES (n, seq, ns)
    ON CONFLICT (node_id) DO UPDATE SET round_seq = ..., probe_ns = ...
    RETURNING round_seq;

  - *Cannot corrupt the workload*: its own table, touched by nothing else.
    Not `jepsen`, `linreg`, `linappend`, `accounts`, or `pgbench_*`.
  - *Cannot grow unboundedly*: one row per node id, three rows forever.
  - *Independently attributable*: each node only ever writes the row keyed by
    its own node id, so three concurrent probes never contend on a row, never
    block on each other's locks, and never turn a real acceptance into a
    unique-violation rejection.
  - *Positively confirmed*: `RETURNING round_seq` must come back equal to the
    round sequence.  A returned row is proof the statement executed; in
    autocommit mode the server commits before it sends the result, so a
    returned row is proof of a committed write.
  - *Refused by a fenced node*: a read-only default turns the implicit
    transaction read-only, so the INSERT fails with SQLSTATE 25006
    (`read_only_sql_transaction`).  A hot standby refuses with the same code.

SOUNDNESS: ONLY CONFIRMED ACCEPTANCES COUNT
───────────────────────────────────────────
An indeterminate outcome is never acceptance.  Probes get killed *precisely*
during failover — that is when `pg_terminate_backend` fires — so counting a
dropped connection as a write would manufacture FATAL violations out of
correct behaviour.  Rules:

  - `ACCEPTED` requires a returned row matching the round sequence.  Nothing
    else can produce it.
  - A server-issued SQLSTATE on the definite-rejection list means the node
    provably did not take the write.
  - Everything else — connection refused, connect timeout, backend
    termination (57P01), statement cancellation (57014),
    `transaction_resolution_unknown` (08007), an unrecognised SQLSTATE, an
    unparseable client-side error string — is `INDETERMINATE`, and its
    verbatim error text is recorded.

Indeterminates are safe for the violation verdict but corrosive to the
*claim* a clean run makes, so they are counted and reported:

  - A round where fewer than three nodes answered is `reduced observability`.
  - A round where two or more nodes were indeterminate is
    `observability lost`: dual writability could not be excluded for that
    instant.  It is not a violation, and it is not a clean pass either.
  - `--max-indeterminate-rate` turns an unobservant run into exit code 3
    (INCONCLUSIVE) so it cannot masquerade as a pass.

WHY TWO ACCEPTANCES IN ONE ROUND IS FATAL EVEN IF THEY DID NOT OVERLAP
──────────────────────────────────────────────────────────────────────
`DEFAULT_LEASE_DURATION` is 2 s (`src/governor/lease.rs`).  A correct handover
requires the outgoing primary's lease to expire before the incoming primary
accepts writes, so two nodes cannot legitimately accept writes 50 ms apart in
any ordering.  The verdict therefore keys on "two confirmed acceptances in one
round", and per-node send/complete monotonic timestamps are reported alongside
so the overlap can be audited independently.

TRANSPORT
─────────
Probes must hit the *internal* PostgreSQL port (5434 inside each container),
never the published 5432/5433/5434 host ports — those are gateway ports that
proxy to whoever the gateway believes the leader is, which would collapse
three distinct nodes into one and make the whole measurement vacuous.

`docker-compose.yml` publishes only gateway, metrics, and management ports, so
there are two ways to reach internal PG from the host:

  - `direct`: connect to `172.28.0.1{1,2,3}:5434`.  Works wherever the compose
    bridge network is routable from the host, which includes Linux and the
    GitHub Actions runners.  Preferred: lowest latency, no helper processes.
  - `docker-exec`: a local TCP listener per node that relays bytes over
    `docker compose exec -T <svc> perl -e <relay>` into the container's
    127.0.0.1:5434.  Needed on Docker Desktop for macOS/Windows, where the
    bridge subnet is not routable from the host.  Measured overhead is
    ~0.4 ms per round trip, and psycopg speaks the real wire protocol through
    it, so classification logic is byte-for-byte the same on both transports.

`auto` (the default) tries `direct` and falls back to `docker-exec`.

USAGE
─────
Standalone, against a running compose cluster:

    ./testing/dual_writability_prober.py --duration 60
    ./testing/dual_writability_prober.py --duration 60 --round-ms 50 \
        --json testing/artifacts/dual-writability/result.json
    ./testing/dual_writability_prober.py --until-stopped     # Ctrl-C to finish

As a background oracle around an injected fault schedule:

    from dual_writability_prober import DualWritabilityProber

    with DualWritabilityProber() as prober:
        prober.start()
        inject_faults()
        report = prober.stop()
    if report.violations:
        ...

Exit codes:
    0 — PASS: no confirmed dual-writability window, observability adequate.
    1 — FAIL: at least one round with two or more confirmed acceptances (L1).
    2 — infrastructure error (cluster unreachable, probe table unusable).
    3 — INCONCLUSIVE: no violation found, but observability below threshold.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import subprocess
import threading
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Any, Final, Protocol

import psycopg
import typer
from rich.console import Console
from rich.table import Table

# ─────────────────────────────────────────────────────────────────────────────
# Topology
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
"""Repo root — `docker compose` is invoked with this as cwd."""

INTERNAL_PG_PORT: Final[int] = 5434
"""PostgreSQL port *inside* each container. Never the published gateway port."""


@dataclass(frozen=True)
class NodeTarget:
    """One cluster node's internal PostgreSQL endpoint."""

    node_id: int
    service: str
    """Docker Compose service name, for the `docker-exec` transport."""
    pg_ip: str
    """Internal bridge address, for the `direct` transport."""


NODES: Final[tuple[NodeTarget, ...]] = (
    NodeTarget(node_id=1, service="node1", pg_ip="172.28.0.11"),
    NodeTarget(node_id=2, service="node2", pg_ip="172.28.0.12"),
    NodeTarget(node_id=3, service="node3", pg_ip="172.28.0.13"),
)

PG_USER: Final[str] = "postgres"
PG_DBNAME: Final[str] = "postgres"

# ─────────────────────────────────────────────────────────────────────────────
# Timing defaults
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_ROUND_PERIOD_S: Final[float] = 0.05
"""Target cadence. Chosen below the 100 ms fence-enforcement tick in
`src/app.rs` so a single-tick dual-write window cannot fall between rounds."""

ATTEMPT_TIMEOUT_FRACTION: Final[float] = 0.6
"""Per-attempt deadline as a fraction of the round period. Bounded well under
the period so one hung node cannot stretch the round and desynchronise the
other two probes."""

MIN_ATTEMPT_TIMEOUT_S: Final[float] = 0.01

CONNECT_TIMEOUT_S: Final[float] = 2.0
"""Connect budget. Connects happen off the round critical path: the driver
records `indeterminate` for a worker that is still connecting and moves on."""

CONNECT_BACKOFF_S: Final[float] = 0.5
"""Delay before retrying a node whose last connect attempt failed. Keeps a
dead node's worker from consuming its connect budget every single round, which
would report that node as `probe_overrun` instead of naming the real failure."""

REQUIRE_TRANSPORT_ENV: Final[str] = "PGBATTERY_PROBER_REQUIRE_TRANSPORT"
"""Env var backing `--require-transport`, so CI can demand `direct` without every
matrix case having to pass a flag that would break local macOS runs."""

DEFAULT_MAX_INDETERMINATE_RATE: Final[float] = 0.40
"""Default observability gate. Above 1/3 so that one of three nodes being
unreachable for an entire run — chaos schedules kill nodes on purpose — is
still a usable result, but well below 2/3 so a run that was blind on two nodes
cannot pass. A healthy cluster measures ~0.0."""

# ─────────────────────────────────────────────────────────────────────────────
# Probe schema and statement
# ─────────────────────────────────────────────────────────────────────────────

PROBE_TABLE: Final[str] = "pgb_dual_write_probe"

CREATE_PROBE_TABLE_SQL: Final[str] = f"""
CREATE TABLE IF NOT EXISTS {PROBE_TABLE} (
    node_id   integer PRIMARY KEY,
    round_seq bigint  NOT NULL,
    probe_ns  bigint  NOT NULL
)
"""

PROBE_SQL: Final[str] = f"""
INSERT INTO {PROBE_TABLE} (node_id, round_seq, probe_ns)
VALUES (%s, %s, %s)
ON CONFLICT (node_id) DO UPDATE
    SET round_seq = EXCLUDED.round_seq, probe_ns = EXCLUDED.probe_ns
RETURNING round_seq
"""

READ_PROBE_TABLE_SQL: Final[str] = f"SELECT count(*) FROM {PROBE_TABLE}"

# ─────────────────────────────────────────────────────────────────────────────
# Outcome classification
# ─────────────────────────────────────────────────────────────────────────────


class Outcome(StrEnum):
    """Fate of one node's write attempt in one round."""

    ACCEPTED = "accepted"
    """The server returned the row we wrote. Proof of a committed write."""

    REJECTED = "definitely-rejected"
    """The server answered with an error that proves the write did not land."""

    INDETERMINATE = "indeterminate"
    """We do not know. Never counted as acceptance; always counted as blindness."""


DEFINITE_REJECTION_SQLSTATES: Final[dict[str, str]] = {
    "25006": "read_only_sql_transaction",
    "25P02": "in_failed_sql_transaction",
    "40001": "serialization_failure",
    "23505": "unique_violation",
    "42501": "insufficient_privilege",
    "42P01": "undefined_table",
    "3D000": "invalid_catalog_name",
    "57P03": "cannot_connect_now",
}
"""SQLSTATEs that prove the statement did not commit.

25006 is the fence itself (`default_transaction_read_only = on`) and is also
what a hot standby returns. 57P03 is a postmaster that is starting up or in
recovery and refusing connections. 42P01 means the probe table has not
replicated to this node yet — a definite non-acceptance, but also a hole in
the measurement, so it is surfaced separately in the report."""

INDETERMINATE_SQLSTATES: Final[dict[str, str]] = {
    "08000": "connection_exception",
    "08001": "sqlclient_unable_to_establish_sqlconnection",
    "08003": "connection_does_not_exist",
    "08004": "sqlserver_rejected_establishment_of_sqlconnection",
    "08006": "connection_failure",
    "08007": "transaction_resolution_unknown",
    "57014": "query_canceled",
    "57P01": "admin_shutdown",
    "57P02": "crash_shutdown",
}
"""SQLSTATEs where the write may or may not have committed.

57P01 is what `pg_terminate_backend` delivers, i.e. the second half of
pgbattery's own fence, and 08007 is the SQL standard's name for exactly this
situation. Counting any of these as acceptance would fabricate FATAL
violations during correct failovers."""

CONNECTION_ERROR_REASONS: Final[tuple[tuple[str, str], ...]] = (
    ("connection refused", "connect_refused"),
    ("timeout expired", "connect_timeout"),
    ("timed out", "connect_timeout"),
    ("no route to host", "host_unreachable"),
    ("network is unreachable", "network_unreachable"),
    ("server closed the connection unexpectedly", "conn_closed_by_server"),
    ("connection reset by peer", "conn_reset"),
    ("broken pipe", "broken_pipe"),
    ("unexpected eof", "unexpected_eof"),
    ("terminating connection", "backend_terminated"),
    ("the connection is closed", "conn_closed_locally"),
    ("consuming input failed", "conn_input_failed"),
)
"""Client-side error substrings, matched lowercase, in order. Every entry maps
to INDETERMINATE — the list exists to *name* the blindness in the report, not
to decide the verdict."""

UNCLASSIFIED_REASON: Final[str] = "unclassified_error"


def classify_failure(sqlstate: str | None, message: str) -> tuple[Outcome, str]:
    """Classify a failed probe attempt. Never returns `ACCEPTED`.

    Acceptance is established only by a returned row, so this function is
    structurally incapable of manufacturing one.

    Args:
        sqlstate: Server-issued SQLSTATE, or None for a client-side failure.
        message: Error text as reported by the driver.

    Returns:
        `(outcome, reason)` where `reason` is a short machine-readable tag.
        An unrecognised SQLSTATE yields `unclassified_sqlstate_<code>`; an
        unrecognised client-side message yields `unclassified_error`. Both are
        INDETERMINATE.
    """
    if sqlstate:
        code = sqlstate.upper()
        if code in DEFINITE_REJECTION_SQLSTATES:
            return Outcome.REJECTED, DEFINITE_REJECTION_SQLSTATES[code]
        if code in INDETERMINATE_SQLSTATES:
            return Outcome.INDETERMINATE, INDETERMINATE_SQLSTATES[code]
        return Outcome.INDETERMINATE, f"unclassified_sqlstate_{code}"

    lowered = message.lower()
    for needle, reason in CONNECTION_ERROR_REASONS:
        if needle in lowered:
            return Outcome.INDETERMINATE, reason
    return Outcome.INDETERMINATE, UNCLASSIFIED_REASON


# ─────────────────────────────────────────────────────────────────────────────
# Round data model
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class NodeProbe:
    """One node's write attempt in one round.

    Timestamps are `time.monotonic_ns()`, matching the harness-wide convention
    documented on `JepsenRecord` in `linearizability_register.py`. Wall-clock
    time is never used for ordering: it can step backwards, and the libfaketime
    shim in `docker-compose.yml` moves it on purpose.
    """

    node_id: int
    outcome: Outcome
    reason: str
    """Short machine-readable tag: `committed`, `read_only_sql_transaction`,
    `connect_refused`, `unclassified_sqlstate_XXXXX`, ..."""
    sqlstate: str | None = None
    error_text: str = ""
    """Verbatim driver error. Populated for every non-accepted outcome, which
    is what makes an `unclassified_*` reason actionable rather than a shrug."""
    sent_ns: int | None = None
    """Monotonic ns immediately before the write was put on the wire. None if
    no connection was available, i.e. the write never left the prober."""
    done_ns: int | None = None
    """Monotonic ns when the outcome became known."""

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "outcome": self.outcome.value,
            "reason": self.reason,
            "sqlstate": self.sqlstate,
            "error_text": self.error_text,
            "sent_ns": self.sent_ns,
            "done_ns": self.done_ns,
        }


@dataclass(frozen=True)
class ProbeRound:
    """One concurrent release of write attempts against all three nodes."""

    seq: int
    started_ns: int
    """Monotonic ns at which the round's probes were released together."""
    probes: tuple[NodeProbe, ...]

    def _with(self, outcome: Outcome) -> tuple[NodeProbe, ...]:
        return tuple(p for p in self.probes if p.outcome is outcome)

    @property
    def accepted(self) -> tuple[NodeProbe, ...]:
        return self._with(Outcome.ACCEPTED)

    @property
    def rejected(self) -> tuple[NodeProbe, ...]:
        return self._with(Outcome.REJECTED)

    @property
    def indeterminate(self) -> tuple[NodeProbe, ...]:
        return self._with(Outcome.INDETERMINATE)

    @property
    def acceptance_count(self) -> int:
        return len(self.accepted)

    @property
    def answered_count(self) -> int:
        """Nodes that gave a definite answer (accepted or provably rejected)."""
        return self.acceptance_count + len(self.rejected)

    @property
    def is_violation(self) -> bool:
        """Two or more *confirmed* acceptances in one round violates L1."""
        return self.acceptance_count >= 2

    @property
    def reduced_observability(self) -> bool:
        """Fewer than three definite answers: the round is not fully observed."""
        return self.answered_count < len(self.probes)

    @property
    def observability_lost(self) -> bool:
        """Two or more nodes indeterminate: dual writability cannot be excluded.

        Not a violation — we did not observe two acceptances — but not a clean
        observation either. Reported so a blind run cannot pass silently.
        """
        return len(self.indeterminate) >= 2

    @property
    def accepted_overlap_ns(self) -> int | None:
        """Overlap of the accepted probes' [sent_ns, done_ns] intervals.

        Positive means two acceptances were genuinely in flight at the same
        instant. Corroborating evidence only: the verdict does not depend on
        it, because a 2 s lease makes two acceptances 50 ms apart illegitimate
        in any ordering. None when fewer than two acceptances carry timestamps.
        """
        spans = [
            (p.sent_ns, p.done_ns)
            for p in self.accepted
            if p.sent_ns is not None and p.done_ns is not None
        ]
        if len(spans) < 2:
            return None
        latest_start = max(s for s, _ in spans)
        earliest_end = min(e for _, e in spans)
        return earliest_end - latest_start

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "started_ns": self.started_ns,
            "acceptance_count": self.acceptance_count,
            "answered_count": self.answered_count,
            "accepted_overlap_ns": self.accepted_overlap_ns,
            "probes": [p.to_dict() for p in self.probes],
        }


@dataclass(frozen=True)
class ViolationWindow:
    """A maximal run of consecutive violating rounds."""

    first_seq: int
    last_seq: int
    start_ns: int
    end_ns: int
    rounds: int

    @property
    def span_ns(self) -> int:
        """Observed span between the first and last violating round.

        A single-round window has span 0: dual writability was observed at one
        instant, and the true window is bounded only by the round period.
        """
        return self.end_ns - self.start_ns

    def to_dict(self) -> dict[str, Any]:
        return {
            "first_seq": self.first_seq,
            "last_seq": self.last_seq,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "rounds": self.rounds,
            "span_ns": self.span_ns,
        }


def violation_windows(rounds: Sequence[ProbeRound]) -> list[ViolationWindow]:
    """Group violating rounds into maximal consecutive-sequence windows.

    A non-violating round, or a gap in the sequence numbers, ends a window.
    Sequence gaps matter: a missing round is a round we did not observe, so we
    must not claim the violation persisted across it.
    """
    windows: list[ViolationWindow] = []
    run: list[ProbeRound] = []

    def flush() -> None:
        if not run:
            return
        windows.append(
            ViolationWindow(
                first_seq=run[0].seq,
                last_seq=run[-1].seq,
                start_ns=run[0].started_ns,
                end_ns=run[-1].started_ns,
                rounds=len(run),
            )
        )
        run.clear()

    for rnd in rounds:
        if not rnd.is_violation:
            flush()
            continue
        if run and rnd.seq != run[-1].seq + 1:
            flush()
        run.append(rnd)
    flush()
    return windows


# ─────────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────────


class Verdict(StrEnum):
    """Overall outcome. Maps 1:1 onto the process exit code."""

    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"


EXIT_PASS: Final[int] = 0
EXIT_FAIL: Final[int] = 1
EXIT_INFRA: Final[int] = 2
EXIT_INCONCLUSIVE: Final[int] = 3

VERDICT_EXIT_CODES: Final[dict[Verdict, int]] = {
    Verdict.PASS: EXIT_PASS,
    Verdict.FAIL: EXIT_FAIL,
    Verdict.INCONCLUSIVE: EXIT_INCONCLUSIVE,
}


@dataclass(frozen=True)
class ProbeReport:
    """Everything the run established, and everything it failed to establish."""

    total_rounds: int
    node_count: int
    """Nodes probed per round. The acceptance histogram spans 0..node_count."""
    rounds_by_acceptance_count: dict[int, int]
    """Histogram keyed 0..node_count — nodes confirmed writable per round."""
    violations: tuple[ProbeRound, ...]
    windows: tuple[ViolationWindow, ...]
    max_window_span_ns: int
    reduced_observability_rounds: int
    observability_lost_rounds: int
    total_probes: int
    indeterminate_probes: int
    reason_counts: dict[str, int]
    unclassified_errors: tuple[str, ...]
    """Distinct verbatim error texts that matched no known SQLSTATE or pattern.
    A non-empty list means the classifier needs a new entry."""
    schema_missing_probes: int
    """Probes that failed with 42P01. Each one is a node whose writability was
    not actually tested, so a nonzero count weakens the whole run."""
    wall_duration_s: float
    round_period_s: float
    transport: str
    max_indeterminate_rate: float
    min_single_acceptance_rate: float

    @property
    def indeterminate_rate(self) -> float:
        if self.total_probes == 0:
            return 1.0
        return self.indeterminate_probes / self.total_probes

    @property
    def single_acceptance_rate(self) -> float:
        """Fraction of rounds that saw exactly one writable node."""
        if self.total_rounds == 0:
            return 0.0
        return self.rounds_by_acceptance_count.get(1, 0) / self.total_rounds

    @property
    def observed_rounds_per_second(self) -> float:
        if self.wall_duration_s <= 0:
            return 0.0
        return self.total_rounds / self.wall_duration_s

    @property
    def inconclusive_reasons(self) -> tuple[str, ...]:
        """Why the run cannot claim to have observed L1, absent a violation."""
        reasons: list[str] = []
        if self.total_rounds == 0:
            reasons.append("no rounds completed")
        if self.indeterminate_rate > self.max_indeterminate_rate:
            reasons.append(
                f"indeterminate rate {self.indeterminate_rate:.1%} exceeds "
                f"limit {self.max_indeterminate_rate:.1%}"
            )
        if self.single_acceptance_rate < self.min_single_acceptance_rate:
            reasons.append(
                f"exactly-one-writable rate {self.single_acceptance_rate:.1%} "
                f"below required {self.min_single_acceptance_rate:.1%}"
            )
        return tuple(reasons)

    @property
    def verdict(self) -> Verdict:
        if self.violations:
            return Verdict.FAIL
        if self.inconclusive_reasons:
            return Verdict.INCONCLUSIVE
        return Verdict.PASS

    @property
    def exit_code(self) -> int:
        return VERDICT_EXIT_CODES[self.verdict]

    @property
    def headline(self) -> str:
        """One unambiguous line stating what was and was not established."""
        if self.verdict is Verdict.FAIL:
            return (
                f"RESULT: FAIL - contract L1 VIOLATED. "
                f"{len(self.violations)} of {self.total_rounds} rounds saw two or more "
                f"nodes confirm a write. Longest observed dual-write window: "
                f"{self.max_window_span_ns / 1_000_000:.1f} ms "
                f"across {max((w.rounds for w in self.windows), default=0)} consecutive rounds."
            )
        if self.verdict is Verdict.INCONCLUSIVE:
            return (
                f"RESULT: INCONCLUSIVE - no dual-writability violation observed in "
                f"{self.total_rounds} rounds, but the run did not observe enough to "
                f"claim L1 holds: {'; '.join(self.inconclusive_reasons)}."
            )
        return (
            f"RESULT: PASS - at most one node confirmed a write in every one of "
            f"{self.total_rounds} rounds "
            f"({self.observed_rounds_per_second:.1f} rounds/s, "
            f"indeterminate rate {self.indeterminate_rate:.2%}, "
            f"{self.observability_lost_rounds} rounds with observability lost)."
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict.value,
            "exit_code": self.exit_code,
            "headline": self.headline,
            "contract": "L1 single writable leader (docs/CONTRACTS.md)",
            "transport": self.transport,
            "round_period_s": self.round_period_s,
            "wall_duration_s": self.wall_duration_s,
            "total_rounds": self.total_rounds,
            "node_count": self.node_count,
            "observed_rounds_per_second": self.observed_rounds_per_second,
            "rounds_by_acceptance_count": {
                str(k): v for k, v in sorted(self.rounds_by_acceptance_count.items())
            },
            "violation_count": len(self.violations),
            "max_window_span_ns": self.max_window_span_ns,
            "windows": [w.to_dict() for w in self.windows],
            "violations": [r.to_dict() for r in self.violations],
            "observability": {
                "total_probes": self.total_probes,
                "indeterminate_probes": self.indeterminate_probes,
                "indeterminate_rate": self.indeterminate_rate,
                "reduced_observability_rounds": self.reduced_observability_rounds,
                "observability_lost_rounds": self.observability_lost_rounds,
                "schema_missing_probes": self.schema_missing_probes,
                "single_acceptance_rate": self.single_acceptance_rate,
                "reason_counts": dict(sorted(self.reason_counts.items())),
                "unclassified_errors": list(self.unclassified_errors),
            },
            "thresholds": {
                "max_indeterminate_rate": self.max_indeterminate_rate,
                "min_single_acceptance_rate": self.min_single_acceptance_rate,
            },
            "inconclusive_reasons": list(self.inconclusive_reasons),
        }


def analyze(
    rounds: Sequence[ProbeRound],
    *,
    wall_duration_s: float = 0.0,
    round_period_s: float = DEFAULT_ROUND_PERIOD_S,
    transport: str = "unknown",
    max_indeterminate_rate: float = DEFAULT_MAX_INDETERMINATE_RATE,
    min_single_acceptance_rate: float = 0.0,
) -> ProbeReport:
    """Reduce a round history to a verdict. Pure: no I/O, no clock reads.

    This is the whole oracle. Everything above it is plumbing that produces
    `ProbeRound`s; everything below it is presentation.
    """
    by_count: dict[int, int] = {}
    reason_counts: dict[str, int] = {}
    unclassified: list[str] = []
    seen_unclassified: set[str] = set()
    total_probes = 0
    indeterminate_probes = 0
    schema_missing = 0
    reduced = 0
    lost = 0

    for rnd in rounds:
        by_count[rnd.acceptance_count] = by_count.get(rnd.acceptance_count, 0) + 1
        if rnd.reduced_observability:
            reduced += 1
        if rnd.observability_lost:
            lost += 1
        for probe in rnd.probes:
            total_probes += 1
            reason_counts[probe.reason] = reason_counts.get(probe.reason, 0) + 1
            if probe.outcome is Outcome.INDETERMINATE:
                indeterminate_probes += 1
            if probe.reason == DEFINITE_REJECTION_SQLSTATES["42P01"]:
                schema_missing += 1
            if (
                probe.reason.startswith("unclassified")
                and probe.error_text
                and probe.error_text not in seen_unclassified
            ):
                seen_unclassified.add(probe.error_text)
                unclassified.append(probe.error_text)

    violations = tuple(r for r in rounds if r.is_violation)
    windows = tuple(violation_windows(rounds))
    node_count = max((len(r.probes) for r in rounds), default=len(NODES))
    return ProbeReport(
        total_rounds=len(rounds),
        node_count=node_count,
        rounds_by_acceptance_count=by_count,
        violations=violations,
        windows=windows,
        max_window_span_ns=max((w.span_ns for w in windows), default=0),
        reduced_observability_rounds=reduced,
        observability_lost_rounds=lost,
        total_probes=total_probes,
        indeterminate_probes=indeterminate_probes,
        reason_counts=reason_counts,
        unclassified_errors=tuple(unclassified),
        schema_missing_probes=schema_missing,
        wall_duration_s=wall_duration_s,
        round_period_s=round_period_s,
        transport=transport,
        max_indeterminate_rate=max_indeterminate_rate,
        min_single_acceptance_rate=min_single_acceptance_rate,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Transports
# ─────────────────────────────────────────────────────────────────────────────


class ProberError(RuntimeError):
    """Infrastructure failure — maps to exit code 2, never to a verdict."""


class Transport(Protocol):
    """Supplies a host-reachable TCP endpoint for a node's internal PG port."""

    name: str

    def endpoint(self, node: NodeTarget) -> tuple[str, int]: ...

    def close(self) -> None: ...


class DirectTransport:
    """Connect straight to the compose bridge address.

    Available wherever the host can route 172.28.0.0/16, which includes Linux
    and the GitHub Actions runners.
    """

    name = "direct"

    def endpoint(self, node: NodeTarget) -> tuple[str, int]:
        return node.pg_ip, INTERNAL_PG_PORT

    def close(self) -> None:
        return None


_RELAY_PERL: Final[str] = r"""
use strict; use IO::Socket::INET; use IO::Select;
my $s = IO::Socket::INET->new(PeerAddr=>'127.0.0.1', PeerPort=>PORT, Proto=>'tcp') or exit 1;
binmode STDIN; binmode STDOUT; binmode $s;
sub wr {
  my ($h,$b)=@_; my $o=0;
  while ($o < length($b)) {
    my $n = syswrite($h,$b,length($b)-$o,$o); return 0 unless defined $n; $o += $n;
  }
  1
}
my $sel = IO::Select->new(\*STDIN, $s);
OUTER: while (1) {
  for my $h ($sel->can_read()) {
    my $buf = ""; my $n = sysread($h, $buf, 65536);
    last OUTER if !defined($n) || $n == 0;
    if (fileno($h) == fileno($s)) { last OUTER unless wr(\*STDOUT, $buf) }
    else { last OUTER unless wr($s, $buf) }
  }
}
close($s);
"""
"""Bidirectional byte relay between the `docker exec` stdio pair and the
container's own 127.0.0.1:5434. Perl is the only scripting runtime in the
pgbattery image (no socat, nc, or python3), and `IO::Socket::INET`,
`IO::Select`, and `Socket` are all present."""


class DockerExecTransport:
    """Tunnel internal PG over `docker compose exec` stdio.

    For hosts where the compose bridge subnet is not routable — Docker Desktop
    on macOS and Windows. One local listener per node; each accepted connection
    spawns its own relay, so a connection killed by the fence simply reconnects
    on the next round.

    Deliberately *not* a psql-driving subprocess: psql block-buffers stdout
    when it is a pipe, so a request/response protocol over it deadlocks. Byte
    relaying keeps psycopg on the real wire protocol, which means error
    classification is identical on both transports and local runs exercise the
    same code CI does.
    """

    name = "docker-exec"

    def __init__(self, nodes: Sequence[NodeTarget], project_root: Path = PROJECT_ROOT) -> None:
        self._project_root = project_root
        self._env = _compose_env()
        self._listeners: dict[int, socket.socket] = {}
        self._ports: dict[int, int] = {}
        self._children: list[subprocess.Popen[bytes]] = []
        self._children_lock = threading.Lock()
        self._closed = threading.Event()
        for node in nodes:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(8)
            self._listeners[node.node_id] = listener
            self._ports[node.node_id] = int(listener.getsockname()[1])
            threading.Thread(
                target=self._accept_loop,
                args=(node, listener),
                name=f"dwp-tunnel-{node.service}",
                daemon=True,
            ).start()

    def endpoint(self, node: NodeTarget) -> tuple[str, int]:
        return "127.0.0.1", self._ports[node.node_id]

    def _accept_loop(self, node: NodeTarget, listener: socket.socket) -> None:
        while not self._closed.is_set():
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            self._start_relay(node, conn)

    def _start_relay(self, node: NodeTarget, conn: socket.socket) -> None:
        relay = _RELAY_PERL.replace("PORT", str(INTERNAL_PG_PORT))
        try:
            child = subprocess.Popen(
                ["docker", "compose", "exec", "-T", node.service, "perl", "-e", relay],
                cwd=self._project_root,
                env=self._env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            conn.close()
            return
        with self._children_lock:
            self._children.append(child)
        threading.Thread(target=self._pump_to_container, args=(conn, child), daemon=True).start()
        threading.Thread(target=self._pump_to_client, args=(conn, child), daemon=True).start()

    @staticmethod
    def _pump_to_container(conn: socket.socket, child: subprocess.Popen[bytes]) -> None:
        stdin = child.stdin
        assert stdin is not None
        try:
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                stdin.write(chunk)
                stdin.flush()
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                stdin.close()

    @staticmethod
    def _pump_to_client(conn: socket.socket, child: subprocess.Popen[bytes]) -> None:
        stdout = child.stdout
        assert stdout is not None
        # Raw `os.read` rather than a buffered read: the relay must forward
        # whatever bytes are available now, not wait for a full buffer, or the
        # PostgreSQL response would sit in the pipe until the next round.
        fd = stdout.fileno()
        try:
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                conn.sendall(chunk)
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                conn.close()

    def close(self) -> None:
        self._closed.set()
        for listener in self._listeners.values():
            with contextlib.suppress(OSError):
                listener.close()
        with self._children_lock:
            children = list(self._children)
            self._children.clear()
        for child in children:
            with contextlib.suppress(OSError):
                child.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                child.wait(timeout=2)


def _compose_env() -> dict[str, str]:
    """Environment for `docker compose exec`.

    `docker-compose.yml` interpolates `PGBATTERY_MANAGEMENT_API_TOKEN` with a
    `:?` guard, so compose refuses to parse the file without it — even for
    `exec`, which only attaches to an already-running container. A placeholder
    is therefore safe: it satisfies interpolation and never reaches a
    container. A real value in the environment is passed through untouched.
    """
    env = dict(os.environ)
    env.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "dual-writability-prober-placeholder")
    return env


def tcp_reachable(host: str, port: int, timeout_s: float = 1.0) -> bool:
    """True if a TCP connection to `host:port` completes within `timeout_s`."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout_s)
    try:
        sock.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def build_transport(
    kind: str,
    nodes: Sequence[NodeTarget] = NODES,
    project_root: Path = PROJECT_ROOT,
    *,
    require: str = "",
) -> Transport:
    """Create the requested transport.

    `auto` prefers `direct` and falls back to `docker-exec` when any node's
    internal port is not reachable from the host.

    `require` names the transport the caller insists on, and raises if `auto`
    resolved to anything else. Without it a fallback is invisible: the probe
    still runs and still reports, so a `docker-exec` run on a host that was
    supposed to prove host-to-bridge routability reads exactly like a direct one.
    An explicit `direct` is checked the same way rather than being allowed to
    fail later as a pile of connection errors.
    """
    if kind == "direct":
        _require_direct_reachable(nodes)
        return DirectTransport()
    if kind == "docker-exec":
        transport: Transport = DockerExecTransport(nodes, project_root)
    elif kind == "auto":
        if all(tcp_reachable(n.pg_ip, INTERNAL_PG_PORT) for n in nodes):
            transport = DirectTransport()
        else:
            transport = DockerExecTransport(nodes, project_root)
    else:
        raise ProberError(f"Unknown transport {kind!r}; expected auto|direct|docker-exec")
    if require and transport.name != require:
        raise ProberError(
            f"required transport {require!r} but resolved {transport.name!r}. "
            f"With --transport {kind}, the internal port was not reachable from "
            "this host; a silent fallback would report as though it had been."
        )
    return transport


def _require_direct_reachable(nodes: Sequence[NodeTarget]) -> None:
    """Fail before probing if any node's internal port is unreachable."""
    unreachable = [n.pg_ip for n in nodes if not tcp_reachable(n.pg_ip, INTERNAL_PG_PORT)]
    if unreachable:
        raise ProberError(
            f"--transport direct requested but {', '.join(unreachable)}:"
            f"{INTERNAL_PG_PORT} is not reachable from this host. Docker Desktop "
            "cannot route the compose subnet; use --transport auto there."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Per-node probe worker
# ─────────────────────────────────────────────────────────────────────────────


class _NodeWorker(threading.Thread):
    """Owns one node's connection and performs one write attempt per round.

    A thread per node is what makes the round concurrent. Sequential probes
    could never establish simultaneity, and simultaneity is the proposition
    under test.

    The worker never blocks the driver: `dispatch` returns False if the worker
    is still busy from a previous round (a slow reconnect, a hung socket), and
    the driver records `indeterminate` for that node instead of waiting.
    """

    def __init__(
        self,
        node: NodeTarget,
        transport: Transport,
        attempt_timeout_s: float,
        connect_timeout_s: float = CONNECT_TIMEOUT_S,
        connect_backoff_s: float = CONNECT_BACKOFF_S,
    ) -> None:
        super().__init__(name=f"dwp-probe-{node.service}", daemon=True)
        self.node = node
        self._transport = transport
        self._attempt_timeout_s = attempt_timeout_s
        self._connect_timeout_s = connect_timeout_s
        self._connect_backoff_ns = int(connect_backoff_s * 1_000_000_000)
        self._conn: psycopg.Connection[Any] | None = None
        self._go = threading.Event()
        self._done = threading.Event()
        self._shutdown = threading.Event()
        self._seq = 0
        self._result: NodeProbe | None = None
        self._busy = False

    # ── driver-side API ─────────────────────────────────────────────────────

    @property
    def busy(self) -> bool:
        return self._busy

    def dispatch(self, seq: int) -> bool:
        """Release this worker for round `seq`. False if it is still busy."""
        if self._busy:
            return False
        self._seq = seq
        self._result = None
        self._done.clear()
        self._busy = True
        self._go.set()
        return True

    def collect(self, timeout_s: float) -> NodeProbe | None:
        """Wait up to `timeout_s` for this round's result. None on overrun."""
        if self._done.wait(timeout_s):
            return self._result
        return None

    def shutdown(self) -> None:
        self._shutdown.set()
        self._go.set()

    # ── worker thread ───────────────────────────────────────────────────────

    def run(self) -> None:
        try:
            while not self._shutdown.is_set():
                if not self._go.wait(0.05):
                    continue
                self._go.clear()
                if self._shutdown.is_set():
                    break
                result = self._probe_once(self._seq)
                self._result = result
                # Order matters: publish the result, clear `busy`, then signal.
                # A `collect` that returns therefore always sees `busy` False.
                self._busy = False
                self._done.set()
        finally:
            self._close_conn()

    def _probe_once(self, seq: int) -> NodeProbe:
        conn = self._ensure_conn()
        if conn is None:
            return NodeProbe(
                node_id=self.node.node_id,
                outcome=Outcome.INDETERMINATE,
                reason=self._connect_reason,
                error_text=self._connect_error,
                sent_ns=None,
                done_ns=time.monotonic_ns(),
            )
        sent_ns = time.monotonic_ns()
        try:
            with conn.cursor() as cur:
                cur.execute(PROBE_SQL, (self.node.node_id, seq, sent_ns))
                row = cur.fetchone()
        except psycopg.Error as exc:
            done_ns = time.monotonic_ns()
            sqlstate = exc.sqlstate
            outcome, reason = classify_failure(sqlstate, str(exc))
            if outcome is Outcome.INDETERMINATE or _is_connection_dead(conn):
                self._close_conn()
            else:
                self._rollback(conn)
            return NodeProbe(
                node_id=self.node.node_id,
                outcome=outcome,
                reason=reason,
                sqlstate=sqlstate,
                error_text=str(exc).strip(),
                sent_ns=sent_ns,
                done_ns=done_ns,
            )
        except OSError as exc:
            done_ns = time.monotonic_ns()
            self._close_conn()
            outcome, reason = classify_failure(None, str(exc))
            return NodeProbe(
                node_id=self.node.node_id,
                outcome=outcome,
                reason=reason,
                error_text=str(exc).strip(),
                sent_ns=sent_ns,
                done_ns=done_ns,
            )
        done_ns = time.monotonic_ns()
        if row is not None and len(row) == 1 and int(row[0]) == seq:
            return NodeProbe(
                node_id=self.node.node_id,
                outcome=Outcome.ACCEPTED,
                reason="committed",
                sent_ns=sent_ns,
                done_ns=done_ns,
            )
        # The statement succeeded but did not return what we wrote. We will not
        # guess what happened, and we will certainly not call it acceptance.
        return NodeProbe(
            node_id=self.node.node_id,
            outcome=Outcome.INDETERMINATE,
            reason="unclassified_returning",
            error_text=f"RETURNING gave {row!r}, expected ({seq},)",
            sent_ns=sent_ns,
            done_ns=done_ns,
        )

    # ── connection management ───────────────────────────────────────────────

    _connect_reason: str = "connect_pending"
    _connect_error: str = ""
    _connect_retry_ns: int = 0
    """Monotonic ns before which reconnecting is not attempted.

    Rate-limits reconnects to an unreachable node. Without it a dead node's
    worker burns the whole connect budget every round, which turns into
    `probe_overrun` for that node and — on the `docker-exec` transport — a
    process spawn per round. It caches nothing about cluster state: every round
    still emits an explicit `indeterminate` probe naming the last failure, and
    the next attempt after the backoff re-probes from scratch."""

    def _ensure_conn(self) -> psycopg.Connection[Any] | None:
        if self._conn is not None and not self._conn.closed:
            return self._conn
        self._conn = None
        if time.monotonic_ns() < self._connect_retry_ns:
            return None
        host, port = self._transport.endpoint(self.node)
        try:
            conn: psycopg.Connection[Any] = psycopg.connect(
                host=host,
                port=port,
                user=PG_USER,
                dbname=PG_DBNAME,
                connect_timeout=max(1, round(self._connect_timeout_s)),
                autocommit=True,
            )
        except (psycopg.Error, OSError) as exc:
            sqlstate = exc.sqlstate if isinstance(exc, psycopg.Error) else None
            _, self._connect_reason = classify_failure(sqlstate, str(exc))
            self._connect_error = str(exc).strip()
            self._connect_retry_ns = time.monotonic_ns() + self._connect_backoff_ns
            return None
        try:
            timeout_ms = max(1, int(self._attempt_timeout_s * 1000))
            with conn.cursor() as cur:
                # Server-side deadline mirroring the round budget: a wedged
                # backend must abort rather than pin the worker. `SET` is not a
                # write, so a fenced node accepts it.
                cur.execute(f"SET statement_timeout = {timeout_ms}")
        except (psycopg.Error, OSError) as exc:
            self._connect_reason = "session_setup_failed"
            self._connect_error = str(exc).strip()
            self._connect_retry_ns = time.monotonic_ns() + self._connect_backoff_ns
            with contextlib.suppress(psycopg.Error, OSError):
                conn.close()
            return None
        self._connect_reason = "connect_pending"
        self._connect_error = ""
        self._connect_retry_ns = 0
        self._conn = conn
        return conn

    def _rollback(self, conn: psycopg.Connection[Any]) -> None:
        with contextlib.suppress(psycopg.Error, OSError):
            conn.rollback()

    def _close_conn(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            with contextlib.suppress(psycopg.Error, OSError):
                conn.close()


def _is_connection_dead(conn: psycopg.Connection[Any]) -> bool:
    if conn.closed:
        return True
    return bool(conn.broken)


# ─────────────────────────────────────────────────────────────────────────────
# Prober
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ProberConfig:
    """Knobs. Defaults are the ones the CI oracle should use."""

    round_period_s: float = DEFAULT_ROUND_PERIOD_S
    attempt_timeout_s: float | None = None
    """Per-attempt deadline. None derives it from the round period."""
    transport: str = "auto"
    require_transport: str = ""
    nodes: tuple[NodeTarget, ...] = NODES
    project_root: Path = PROJECT_ROOT
    max_indeterminate_rate: float = DEFAULT_MAX_INDETERMINATE_RATE
    min_single_acceptance_rate: float = 0.0
    setup_timeout_s: float = 60.0

    def effective_attempt_timeout_s(self) -> float:
        if self.attempt_timeout_s is not None:
            return self.attempt_timeout_s
        return max(MIN_ATTEMPT_TIMEOUT_S, self.round_period_s * ATTEMPT_TIMEOUT_FRACTION)


class DualWritabilityProber:
    """Continuous L1 oracle over the three internal PostgreSQL endpoints.

    Standalone:

        prober = DualWritabilityProber()
        prober.setup()
        report = prober.run_for(60.0)
        prober.close()

    Background oracle around a fault schedule:

        with DualWritabilityProber() as prober:
            prober.start()
            inject_faults()
            report = prober.stop()
    """

    def __init__(self, config: ProberConfig | None = None) -> None:
        self.config = config or ProberConfig()
        self._transport: Transport | None = None
        self._workers: list[_NodeWorker] = []
        self._rounds: list[ProbeRound] = []
        self._rounds_lock = threading.Lock()
        self._stop = threading.Event()
        self._loop_thread: threading.Thread | None = None
        self._started_ns = 0
        self._ended_ns = 0
        self._setup_notes: list[str] = []

    # ── lifecycle ───────────────────────────────────────────────────────────

    def __enter__(self) -> DualWritabilityProber:
        self.setup()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    @property
    def transport_name(self) -> str:
        return self._transport.name if self._transport is not None else self.config.transport

    @property
    def setup_notes(self) -> tuple[str, ...]:
        return tuple(self._setup_notes)

    def setup(self) -> None:
        """Pick a transport, create the probe table, and start the workers.

        Raises:
            ProberError: No node accepted the probe-table DDL, or the table
                never became visible on every node. Both are infrastructure
                failures, not verdicts: without a usable probe table the
                oracle would silently report rejections everywhere.
        """
        self._transport = build_transport(
            self.config.transport,
            self.config.nodes,
            self.config.project_root,
            require=self.config.require_transport,
        )
        self._setup_notes.append(f"transport={self._transport.name}")
        self._create_probe_table()
        self._await_probe_table()
        attempt_timeout = self.config.effective_attempt_timeout_s()
        for node in self.config.nodes:
            worker = _NodeWorker(node, self._transport, attempt_timeout)
            worker.start()
            self._workers.append(worker)

    def close(self) -> None:
        self._stop.set()
        if self._loop_thread is not None and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=5.0)
        for worker in self._workers:
            worker.shutdown()
        for worker in self._workers:
            worker.join(timeout=5.0)
        self._workers.clear()
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    # ── probe table ─────────────────────────────────────────────────────────

    def _connect_for_setup(self, node: NodeTarget) -> psycopg.Connection[Any] | None:
        assert self._transport is not None
        host, port = self._transport.endpoint(node)
        try:
            return psycopg.connect(
                host=host,
                port=port,
                user=PG_USER,
                dbname=PG_DBNAME,
                connect_timeout=5,
                autocommit=True,
            )
        except (psycopg.Error, OSError):
            return None

    def _create_probe_table(self) -> None:
        """Create the probe table on whichever node currently takes writes.

        Deliberately control-plane-free: the oracle must not learn who the
        leader is from the thing it is auditing. Trying all three and keeping
        whichever accepts the DDL is both simpler and independent.
        """
        deadline = time.monotonic() + self.config.setup_timeout_s
        errors: list[str] = []
        while time.monotonic() < deadline:
            errors.clear()
            for node in self.config.nodes:
                conn = self._connect_for_setup(node)
                if conn is None:
                    errors.append(f"node{node.node_id}: unreachable")
                    continue
                try:
                    with conn.cursor() as cur:
                        cur.execute(CREATE_PROBE_TABLE_SQL)
                    self._setup_notes.append(f"probe table ensured on node{node.node_id}")
                    return
                except (psycopg.Error, OSError) as exc:
                    errors.append(f"node{node.node_id}: {str(exc).strip()}")
                finally:
                    with contextlib.suppress(psycopg.Error, OSError):
                        conn.close()
            time.sleep(1.0)
        raise ProberError(
            "No node accepted the probe-table DDL within "
            f"{self.config.setup_timeout_s:.0f}s: {'; '.join(errors)}"
        )

    def _await_probe_table(self) -> None:
        """Block until every node can read the probe table.

        A node that has not replicated the table yet would answer 42P01 for
        every probe, which reads as a rejection and would hide a real
        violation on that node. Waiting here keeps the oracle honest; the wait
        is recorded in `setup_notes` either way.
        """
        deadline = time.monotonic() + self.config.setup_timeout_s
        pending = {n.node_id: n for n in self.config.nodes}
        while pending and time.monotonic() < deadline:
            for node in list(pending.values()):
                conn = self._connect_for_setup(node)
                if conn is None:
                    continue
                try:
                    with conn.cursor() as cur:
                        cur.execute(READ_PROBE_TABLE_SQL)
                        cur.fetchone()
                    pending.pop(node.node_id, None)
                except (psycopg.Error, OSError):
                    pass
                finally:
                    with contextlib.suppress(psycopg.Error, OSError):
                        conn.close()
            if pending:
                time.sleep(0.5)
        if pending:
            missing = ", ".join(f"node{i}" for i in sorted(pending))
            self._setup_notes.append(
                f"WARNING: probe table not readable on {missing} at start; "
                "their writability is untested until it replicates"
            )
        else:
            self._setup_notes.append("probe table readable on all nodes")

    # ── round loop ──────────────────────────────────────────────────────────

    def probe_round(self, seq: int) -> ProbeRound:
        """Release one concurrent write attempt per node and collect the results.

        All three workers are released before any result is collected, so the
        attempts genuinely overlap. Collection is bounded by the attempt
        timeout: a worker that overruns yields `indeterminate` and the round
        closes on schedule.
        """
        attempt_timeout = self.config.effective_attempt_timeout_s()
        started_ns = time.monotonic_ns()
        dispatched: list[_NodeWorker] = []
        overrun: list[_NodeWorker] = []
        for worker in self._workers:
            if worker.dispatch(seq):
                dispatched.append(worker)
            else:
                overrun.append(worker)

        probes: list[NodeProbe] = []
        for worker in overrun:
            probes.append(
                NodeProbe(
                    node_id=worker.node.node_id,
                    outcome=Outcome.INDETERMINATE,
                    reason="probe_overrun",
                    error_text="worker still busy from a previous round",
                    done_ns=time.monotonic_ns(),
                )
            )
        deadline = started_ns + int(attempt_timeout * 1_000_000_000)
        for worker in dispatched:
            remaining = (deadline - time.monotonic_ns()) / 1_000_000_000
            result = worker.collect(max(0.0, remaining))
            if result is None:
                probes.append(
                    NodeProbe(
                        node_id=worker.node.node_id,
                        outcome=Outcome.INDETERMINATE,
                        reason="attempt_timeout",
                        error_text=f"no result within {attempt_timeout * 1000:.0f} ms",
                        done_ns=time.monotonic_ns(),
                    )
                )
            else:
                probes.append(result)
        probes.sort(key=lambda p: p.node_id)
        return ProbeRound(seq=seq, started_ns=started_ns, probes=tuple(probes))

    def _run_loop(self) -> None:
        period = self.config.round_period_s
        seq = 0
        next_ns = time.monotonic_ns()
        while not self._stop.is_set():
            rnd = self.probe_round(seq)
            with self._rounds_lock:
                self._rounds.append(rnd)
            seq += 1
            next_ns += int(period * 1_000_000_000)
            sleep_s = (next_ns - time.monotonic_ns()) / 1_000_000_000
            if sleep_s > 0:
                self._stop.wait(sleep_s)
            else:
                # Behind schedule (slow round). Re-anchor rather than
                # accumulating debt and then busy-looping to catch up.
                next_ns = time.monotonic_ns()

    def start(self) -> None:
        """Begin probing in the background. Returns immediately."""
        if not self._workers:
            raise ProberError("setup() must run before start()")
        if self._loop_thread is not None:
            raise ProberError("prober already started")
        self._stop.clear()
        self._started_ns = time.monotonic_ns()
        self._loop_thread = threading.Thread(target=self._run_loop, name="dwp-driver", daemon=True)
        self._loop_thread.start()

    def stop(self) -> ProbeReport:
        """Stop probing and reduce the collected history to a verdict."""
        self._stop.set()
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5.0)
            self._loop_thread = None
        self._ended_ns = time.monotonic_ns()
        return self.report()

    def run_for(self, duration_s: float) -> ProbeReport:
        """Probe for a fixed duration, then stop and report."""
        self.start()
        self._stop.wait(duration_s)
        return self.stop()

    def snapshot(self) -> tuple[ProbeRound, ...]:
        """Rounds recorded so far. Safe to call while probing."""
        with self._rounds_lock:
            return tuple(self._rounds)

    def report(self) -> ProbeReport:
        rounds = self.snapshot()
        end_ns = self._ended_ns or time.monotonic_ns()
        wall = (end_ns - self._started_ns) / 1_000_000_000 if self._started_ns else 0.0
        return analyze(
            rounds,
            wall_duration_s=wall,
            round_period_s=self.config.round_period_s,
            transport=self.transport_name,
            max_indeterminate_rate=self.config.max_indeterminate_rate,
            min_single_acceptance_rate=self.config.min_single_acceptance_rate,
        )


@contextlib.contextmanager
def background_oracle(config: ProberConfig | None = None) -> Iterator[DualWritabilityProber]:
    """Run the prober for the duration of a `with` block.

    The report is available from `prober.stop()` inside the block, or from
    `prober.report()` after it.
    """
    prober = DualWritabilityProber(config)
    prober.setup()
    prober.start()
    try:
        yield prober
    finally:
        with contextlib.suppress(Exception):
            prober.stop()
        prober.close()


# ─────────────────────────────────────────────────────────────────────────────
# Presentation
# ─────────────────────────────────────────────────────────────────────────────

_VERDICT_STYLE: Final[dict[Verdict, str]] = {
    Verdict.PASS: "bold green",
    Verdict.FAIL: "bold red",
    Verdict.INCONCLUSIVE: "bold yellow",
}


def render_report(report: ProbeReport, console: Console, *, max_violations: int = 10) -> None:
    """Print the report: acceptance histogram, observability, violations."""
    console.rule("[bold]DUAL-WRITABILITY PROBER — CONTRACT L1")

    summary = Table(show_header=False, box=None, pad_edge=False)
    summary.add_column(style="cyan")
    summary.add_column()
    summary.add_row("transport", report.transport)
    summary.add_row("round period", f"{report.round_period_s * 1000:.0f} ms")
    summary.add_row("wall duration", f"{report.wall_duration_s:.1f} s")
    summary.add_row("rounds", f"{report.total_rounds} ({report.observed_rounds_per_second:.1f}/s)")
    console.print(summary)

    hist = Table(title="Confirmed writable nodes per round", title_justify="left")
    hist.add_column("acceptances", justify="right")
    hist.add_column("rounds", justify="right")
    hist.add_column("share", justify="right")
    hist.add_column("meaning")
    meanings = {
        0: "no node took a write (no leader, or all probes blind)",
        1: "healthy: exactly one writable node",
        2: "L1 VIOLATION: dual writability",
        3: "L1 VIOLATION: triple writability",
    }
    top = max([report.node_count, *report.rounds_by_acceptance_count])
    for count in range(top + 1):
        rounds = report.rounds_by_acceptance_count.get(count, 0)
        share = rounds / report.total_rounds if report.total_rounds else 0.0
        style = "red" if count >= 2 and rounds else None
        hist.add_row(
            str(count),
            str(rounds),
            f"{share:.1%}",
            meanings.get(count, ""),
            style=style,
        )
    console.print(hist)

    obs = Table(title="Observability", title_justify="left")
    obs.add_column("metric", style="cyan")
    obs.add_column("value", justify="right")
    obs.add_row("probes", str(report.total_probes))
    obs.add_row("indeterminate probes", str(report.indeterminate_probes))
    obs.add_row("indeterminate rate", f"{report.indeterminate_rate:.2%}")
    obs.add_row("rounds with < 3 answers", str(report.reduced_observability_rounds))
    obs.add_row("rounds with observability lost", str(report.observability_lost_rounds))
    obs.add_row("probe-table-missing probes", str(report.schema_missing_probes))
    obs.add_row("exactly-one-writable rate", f"{report.single_acceptance_rate:.2%}")
    console.print(obs)

    reasons = Table(title="Outcome reasons", title_justify="left")
    reasons.add_column("reason", style="cyan")
    reasons.add_column("count", justify="right")
    for reason, count in sorted(report.reason_counts.items(), key=lambda kv: -kv[1]):
        reasons.add_row(reason, str(count))
    console.print(reasons)

    if report.unclassified_errors:
        console.print("[bold yellow]Unclassified errors (treated as indeterminate):[/]")
        for text in report.unclassified_errors[:10]:
            console.print(f"  {text}")

    if report.violations:
        console.print(
            f"[bold red]L1 VIOLATIONS: {len(report.violations)} rounds in "
            f"{len(report.windows)} window(s)[/]"
        )
        wins = Table(title="Dual-write windows", title_justify="left")
        wins.add_column("rounds", justify="right")
        wins.add_column("seq range")
        wins.add_column("observed span")
        for window in report.windows:
            wins.add_row(
                str(window.rounds),
                f"{window.first_seq}..{window.last_seq}",
                f"{window.span_ns / 1_000_000:.1f} ms",
            )
        console.print(wins)
        for rnd in report.violations[:max_violations]:
            detail = Table(
                title=f"round {rnd.seq} @ {rnd.started_ns} ns "
                f"(accepted overlap: {_fmt_overlap(rnd.accepted_overlap_ns)})",
                title_justify="left",
            )
            detail.add_column("node", justify="right")
            detail.add_column("outcome")
            detail.add_column("reason")
            detail.add_column("sent_ns", justify="right")
            detail.add_column("done_ns", justify="right")
            detail.add_column("error")
            for probe in rnd.probes:
                detail.add_row(
                    str(probe.node_id),
                    probe.outcome.value,
                    probe.reason,
                    str(probe.sent_ns),
                    str(probe.done_ns),
                    probe.error_text[:60],
                    style="bold red" if probe.outcome is Outcome.ACCEPTED else None,
                )
            console.print(detail)
        if len(report.violations) > max_violations:
            console.print(
                f"[dim]... {len(report.violations) - max_violations} further "
                "violating rounds in the JSON artifact[/]"
            )

    console.print(f"[{_VERDICT_STYLE[report.verdict]}]{report.headline}[/]")


def _fmt_overlap(overlap_ns: int | None) -> str:
    if overlap_ns is None:
        return "n/a"
    if overlap_ns >= 0:
        return f"+{overlap_ns / 1_000_000:.3f} ms (concurrent)"
    return f"{overlap_ns / 1_000_000:.3f} ms (disjoint)"


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

app = typer.Typer(
    add_completion=False,
    help="Continuous dual-writability prober: direct measurement of contract L1.",
)
console = Console()


@app.command()
def run(
    duration: float = typer.Option(
        60.0,
        "--duration",
        help="Seconds to probe. Use --until-stopped to run until signalled.",
    ),
    until_stopped: bool = typer.Option(
        False,
        "--until-stopped",
        help="Probe until stopped instead of for --duration; the report is "
        "still printed and --json still written. Stop with Ctrl-C/SIGINT. "
        "SIGTERM also works, but only when the module is run through the "
        "interpreter directly: the uv shebang wrapper forwards SIGINT to the "
        "child and does not forward SIGTERM.",
    ),
    round_ms: float = typer.Option(
        DEFAULT_ROUND_PERIOD_S * 1000,
        "--round-ms",
        help="Round period in milliseconds. 50 ms sits under the 100 ms "
        "fence-enforcement tick in src/app.rs.",
    ),
    attempt_ms: float = typer.Option(
        0.0,
        "--attempt-ms",
        help="Per-attempt deadline in milliseconds. 0 derives it from --round-ms.",
    ),
    transport: str = typer.Option(
        "auto",
        "--transport",
        help="auto | direct | docker-exec. auto prefers direct 172.28.0.x:5434 "
        "and falls back to a docker-exec tunnel when the bridge is not routable.",
    ),
    require_transport: str = typer.Option(
        "",
        "--require-transport",
        envvar=REQUIRE_TRANSPORT_ENV,
        help="Fail unless the resolved transport is this one. Set it in CI, where "
        "the bridge is routable, so an unnoticed docker-exec fallback cannot "
        "report as a direct probe.",
    ),
    max_indeterminate_rate: float = typer.Option(
        DEFAULT_MAX_INDETERMINATE_RATE,
        "--max-indeterminate-rate",
        help="Exit 3 (INCONCLUSIVE) if more than this fraction of probes were "
        "indeterminate, so a blind run cannot pass.",
    ),
    min_single_acceptance_rate: float = typer.Option(
        0.0,
        "--min-single-acceptance-rate",
        help="Exit 3 (INCONCLUSIVE) if fewer than this fraction of rounds saw "
        "exactly one writable node. Use ~0.95 for a healthy-cluster sanity run; "
        "leave at 0 when faults are being injected.",
    ),
    json_out: str = typer.Option(
        "",
        "--json",
        help="Write the full report, including every violating round, to this path.",
    ),
) -> None:
    """Probe all three nodes concurrently and assert at most one accepts writes.

    Exit codes: 0 PASS, 1 L1 violated, 2 infrastructure error, 3 inconclusive.
    """
    config = ProberConfig(
        round_period_s=round_ms / 1000.0,
        attempt_timeout_s=(attempt_ms / 1000.0) if attempt_ms > 0 else None,
        transport=transport,
        require_transport=require_transport,
        max_indeterminate_rate=max_indeterminate_rate,
        min_single_acceptance_rate=min_single_acceptance_rate,
    )
    prober = DualWritabilityProber(config)
    try:
        prober.setup()
    except ProberError as exc:
        console.print(f"[bold red]INFRASTRUCTURE ERROR:[/] {exc}")
        prober.close()
        raise typer.Exit(code=EXIT_INFRA) from exc
    except KeyboardInterrupt as exc:
        # Setup can take seconds (transport handshake, probe-table replication).
        # An interrupt there aborts without a verdict rather than a traceback.
        console.print("[yellow]interrupted during setup; no rounds were probed[/]")
        prober.close()
        raise typer.Exit(code=EXIT_INFRA) from exc

    for note in prober.setup_notes:
        style = "yellow" if note.startswith("WARNING") else "dim"
        console.print(f"[{style}]{note}[/]")

    try:
        if until_stopped:
            console.print("[dim]probing until interrupted (SIGINT or SIGTERM)[/]")
            stop_requested = threading.Event()
            # SIGTERM as well as SIGINT: a supervising harness kills with
            # SIGTERM, and the report is the whole point of the run.
            previous = signal.signal(signal.SIGTERM, lambda *_: stop_requested.set())
            prober.start()
            try:
                while not stop_requested.wait(0.5):
                    pass
            except KeyboardInterrupt:
                pass
            finally:
                signal.signal(signal.SIGTERM, previous)
            console.print("[dim]stop requested[/]")
            report = prober.stop()
        else:
            console.print(f"[dim]probing for {duration:.0f}s[/]")
            report = prober.run_for(duration)
    finally:
        prober.close()

    render_report(report, console)

    if json_out:
        path = Path(json_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
        console.print(f"[dim]report written to {path}[/]")

    raise typer.Exit(code=report.exit_code)


__all__ = [
    "DEFAULT_ROUND_PERIOD_S",
    "DualWritabilityProber",
    "NodeProbe",
    "NodeTarget",
    "Outcome",
    "ProbeReport",
    "ProbeRound",
    "ProberConfig",
    "ProberError",
    "Verdict",
    "ViolationWindow",
    "analyze",
    "background_oracle",
    "classify_failure",
    "render_report",
    "violation_windows",
]


if __name__ == "__main__":
    app()
