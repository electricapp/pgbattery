#!/usr/bin/env -S uv run --project testing python
"""Correctness Lite: history-based durability + split-brain checker for pgbattery.

SCOPE — READ THIS FIRST
This is **NOT** Jepsen. It does not run a linearizability checker, does not
generate a concurrent operation DAG, and does not detect transactional
anomalies (read-skew, write-skew, lost-update, phantom-read). It is a
**custom-invariant durability and split-brain smoketest**. Real Jepsen-grade
correctness verification needs Elle / Knossos / Porcupine plus an adversarial
multi-object workload — see `testing/linearizability_register.py` for the
single-register linearizability test we run separately, and the skeletons
under `testing/{fault_primitives,five_node_suite}.py` for what's still missing.

What this file DOES verify:
- Acked writes survive every injected fault (durability — I1).
- Writes during quorum-loss windows are properly fenced (no ghost writes — I5).
- No two nodes claim leadership in the same 0.5 s poll round (no split-brain — I4).
- A bank-transfer workload conserves total balance (B1/B2 — single-object
  multi-write consistency under failover) and applies every uniquely-identified
  transfer at most once, exactly once if acked (B3/B4).
- Optional: concurrent same-row hammer (3 writers contending on 3 keys with a
  blind read-modify-write `UPDATE counters SET val = val + 1 WHERE id = k`,
  which PostgreSQL serialises per row; verifies "no lost update" — C1).
- Optional: monotonic-read session test (a value visible from a session must
  not regress after failover on that same logical session — M1).

What this file does NOT verify:
- Linearizability of concurrent reads + writes (see linearizability_register.py).
- Isolation across multi-statement transactions (serializable / snapshot).
- 2PC (prepared transaction) fate after coordinator failover.
- Causal+ consistency across sessions.

Every write attempt is logged with monotonic timestamps.  A background thread
continuously samples leader status across all three nodes in parallel.  Fault
windows are recorded precisely (open/close).  After the fault schedule the
invariants below are checked against the *complete* operation history — not
just a single point-in-time snapshot.

Outcome taxonomy (`classify_attempt`) is the foundation everything else rests
on.  "acked" requires psql exit status 0 and nothing else; a *recognised*
definite refusal is "rejected"; anything else — including an error string we
have never seen before — is "indeterminate".  Falling back to "rejected" for an
unrecognised error would let a write that really did commit be counted as a
definite non-commit, which turns I2 / C1 / B3 into false-positive generators.
Indeterminate merely widens the permitted bound, so unknown always maps there.

════════════════════════════════════════════════════════════════════════════════
LAYER 1 — POLLING-BASED INVARIANTS  I1-I7  (all FATAL)
Checked against the timestamped operation history and background leader polls.
Coverage: ~0.5s granularity for leader state; exact for write timestamps.
════════════════════════════════════════════════════════════════════════════════

I1  NO_LOST_ACKS
    ∀ v ∈ acked_set  →  v ∈ db_final
    An acknowledged write must survive every fault.  Violation = data loss.

I2  NO_PHANTOM_WRITES
    ∀ v ∈ db_final  →  v ∈ (acked_set | indeterminate_set)
    No value may appear in the DB unless we either acked it or lost track of it.
    Violation = split-brain ghost write or uncommitted-data replay.

I3  NO_DUPLICATES
    COUNT(*) = COUNT(DISTINCT id) in the jepsen table.
    The PRIMARY KEY constraint must hold under all replication paths.

I4  SINGLE_LEADER
    In every concurrent leader-poll round, all responding management nodes
    agree on a single leader (or all return "no leader").
    Two distinct non-None leader IDs in the same round = split-brain.

I5  NO_ACKS_DURING_QUORUM_LOSS
    No write whose entire lifespan [start_ts, end_ts] is strictly inside a
    recorded quorum-loss fault window was acked.
    Violation = lease-fencing mechanism failed to block writes under majority loss.
    This is the exact letter of contract L2 in docs/CONTRACTS.md, so strict
    containment stays the FATAL case.  An acked write that *overlaps* a window
    but crosses one of its boundaries cannot be judged against L2 — our fault
    open/close timestamps are wall-clock-adjacent to the real outage, not
    identical to it — so those are reported separately as I5-WARN observations
    rather than dropped on the floor.

I6  INTERMEDIATE_READ_CONSISTENCY
    Each post-recovery snapshot must contain every value that was in acked_set
    at the moment the snapshot was taken.
    Violation = transient data loss (write survived until that point, then vanished).

I7  CAUSAL_MONOTONICITY
    In db_final: if value N is present (N ∈ acked_set) and value M was fully
    acked *before* N was even attempted — ack_end_ts(M) < attempt_start_ts(N) —
    then M must also be in db_final.
    Violation = selective rollback: an older committed write was lost while a
    strictly later write that had not yet started survived.

════════════════════════════════════════════════════════════════════════════════
LAYER 2 — LOG GREP CHECKS  L0, L2-L3  (FATAL, defense in depth)
Checked against the collected container log file after the fault schedule.
Simple substring presence — no regex parsing, no format fragility.  Every
substring below is emitted verbatim by the Rust source (`src/app.rs`);
`testing/test_correctness_lite_invariants.py` greps the tree to prove they
still exist, so a reworded log line breaks the unit test instead of silently
turning a grep into a no-op.
════════════════════════════════════════════════════════════════════════════════

L0  LOG_CORPUS_VALID
    The collected log must be readable and must contain at least one known
    pgbattery startup marker.  L2/L3 are absence/conditional greps: over a
    missing, empty, or non-pgbattery log they match nothing and would read as
    a pass.  L0 makes that vacuity a loud FATAL instead.

L2  NO_EXPLICIT_SPLIT_BRAIN_SIGNALS
    Zero occurrences of "potential split-brain", "FAILED TO FENCE", or
    "Promotion safety check failed" in the collected log.
    These strings are emitted only when the code itself detects an unsafe
    state — their presence is an unconditional violation regardless of
    whether data loss occurred.

L3  FENCE_CONFIRMED_AFTER_EMERGENCY
    If any "EMERGENCY FENCE" line exists in the log, at least one
    "PostgreSQL fenced (read-only)" line must also exist.
    A fence that fires without a subsequent confirmation means writes may
    have been accepted on a node that had already lost quorum.
    If a quorum-loss window was injected but the log carries no fence trace at
    all, L3 had nothing to check: reported as an L3-WARN observation, because
    the lease may legitimately have been surrendered through the ordinary
    step-down path instead of the emergency fence.

════════════════════════════════════════════════════════════════════════════════
LAYER 3 — WORKLOAD INVARIANTS  B1-B4, C1, M1  (all FATAL)
Checked against the workload tables plus the in-memory attempt records.
════════════════════════════════════════════════════════════════════════════════

B1  BANK_TOTAL_CONSERVED           SUM(balance) == BANK_TOTAL.
B2  NO_NEGATIVE_BALANCE            MIN(balance) >= 0.
B3  TRANSFER_APPLIED_AT_MOST_ONCE  Every attempted transfer id appears at most
                                   once in the ledger, exactly once if acked,
                                   and never if definitely rejected.
B4  BANK_LEDGER_RECONCILED         Every balance equals the initial balance
                                   plus the credits minus the debits recorded
                                   in the ledger.  B1 is algebraically blind to
                                   double application (applying a transfer
                                   twice still conserves the sum); B4 is not.
C1  NO_LOST_UPDATE                 acked <= db_val <= acked + indeterminate.
M1  NO_READ_REGRESSION             observed MAX(val) never decreases.

════════════════════════════════════════════════════════════════════════════════

Findings are either FATAL (an invariant violation, drives the exit code) or
WARN (a labelled observation that is reported but does not fail the run).

Exit codes:
    0 — all invariants hold (PASS)
    1 — at least one invariant violated (FAIL)
    2 — infrastructure error (cluster unreachable, table creation failed, etc.)
"""

from __future__ import annotations

import contextlib
import json
import os
import random
import subprocess
import threading
import time
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import fault_primitives as fp

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

GATEWAY_PORTS: Final[list[int]] = [5432, 5433, 5434]
"""Gateway ports for node1/node2/node3 (each proxied to current leader)."""

BANK_ACCOUNTS: Final[int] = 10
"""Number of accounts in the bank transfer workload."""

BANK_INITIAL_BALANCE: Final[int] = 1000
"""Initial balance per account; total = BANK_ACCOUNTS * BANK_INITIAL_BALANCE."""

BANK_TOTAL: Final[int] = BANK_ACCOUNTS * BANK_INITIAL_BALANCE
"""Invariant total that must be conserved across all transfers."""

NODES: Final[list[str]] = list(fp.NODES)
"""Compose service names of the voters, derived from the compose file rather
than restated. Never container names: those carry the project prefix, which CI
overrides per run."""

MGMT_PORTS: Final[list[int]] = [fp.MGMT_PORTS[node] for node in NODES]
"""Host-published management API ports, in the same order as `NODES`."""

PSQL_TIMEOUT: Final[int] = 5
"""Seconds before a psql write attempt is classified as indeterminate."""

LEADER_POLL_INTERVAL: Final[float] = 0.5
"""Seconds between background leader-poll rounds."""

CONVERGENCE_BUDGET_S: Final[float] = 30.0
"""How long the nodes may name different leaders before it stops being a
failover in progress.

An election that openraft's own timers do not settle is driven by the
leaderless watchdog, whose last rank fires at 21 election timeouts (21 s at the
1 s default), plus the election window and the lease grant. A disagreement
shorter than that is a cluster converging; one longer is a cluster that has
stopped."""

BANK_LEDGER_TABLE: Final[str] = "bank_ledger"
"""Table recording one row per *applied* transfer, keyed by transfer id."""

REJECTION_PATTERNS: Final[list[str]] = [
    "read-only",
    "cannot execute",
    "connection refused",
    "not accept",
    "read_only",
]
"""Substrings proving the write never reached a committing backend.

These are routing-level refusals: the node we hit is a standby, is fenced, or
is not listening at all. Nothing was committed, and *another* gateway port may
still be able to serve the write, so the caller may move on to the next port.
"""

SERVER_REJECTION_PATTERNS: Final[list[str]] = [
    "violates check constraint",
    "violates unique constraint",
    "duplicate key value",
    "current transaction is aborted",
    "syntax error at or near",
]
"""Substrings proving the server parsed the statement and aborted the transaction.

Also a definite non-commit, but unlike REJECTION_PATTERNS it is a semantic
refusal: every other port would answer identically, so retrying is pointless.
"""

INDETERMINATE_PATTERNS: Final[list[str]] = [
    "connection",
    "server closed",
    "timeout",
    "reset by peer",
    "broken pipe",
    "unexpected eof",
    "eof detected",
    "ssl syscall error",
    "could not receive data",
    "terminating connection",
    "no connection to the server",
]
"""Output substrings that indicate the write fate is unknown.

Every entry here describes a connection that died at an unknown point in the
protocol: an in-flight COMMIT may or may not have been made durable. This list
is documentation of the *recognised* unknown-fate errors, not a gate — an
unrecognised error is also treated as indeterminate (see `classify_attempt`),
it is merely reported with reason "unclassified" so taxonomy gaps stay visible.
"""

LOG_LIVENESS_MARKERS: Final[tuple[str, ...]] = (
    "Starting pgbattery in DATA mode",
    "pgbattery DATA node is running (lease fencing enabled)",
    "Opened Raft storage",
    "This node is now the leader",
)
"""Log lines proving the collected corpus really contains pgbattery output."""

LOG_SPLIT_BRAIN_SIGNALS: Final[tuple[str, ...]] = (
    "potential split-brain",
    "FAILED TO FENCE",
    "Promotion safety check failed",
)
"""Self-reported unsafe-state markers; any occurrence is an L2 violation."""

LOG_FENCE_MARKERS: Final[tuple[str, ...]] = (
    "EMERGENCY FENCE",
    "Lease expired",
)
"""Markers proving the emergency-fence path was reached at least once."""

LOG_FENCE_CONFIRMED: Final[str] = "PostgreSQL fenced (read-only)"
"""Confirmation that a fired fence actually made PostgreSQL read-only."""

LOG_FENCE_MOOT: Final[str] = "Fence not applied: PostgreSQL is not answering"
"""The other way an emergency fence concludes safely.

A server that is not answering is serving no writes through the socket the
fence would have used, so there is nothing left to fence. Every run here kills
nodes, so without this L3 fires on all of them."""

ATTEMPT_ACKED: Final[str] = "acked"
"""psql exited 0: the write is committed and durable from the client's view."""

ATTEMPT_ROUTING_REJECTED: Final[str] = "routing_rejected"
"""Definite non-commit at this port; another port may still accept the write."""

ATTEMPT_REJECTED: Final[str] = "rejected"
"""Definite non-commit, refused by the server itself; retrying cannot help."""

ATTEMPT_INDETERMINATE: Final[str] = "indeterminate"
"""Fate unknown: the write may or may not have committed. Never retry these."""

SEVERITY_FATAL: Final[str] = "FATAL"
"""Finding that violates an invariant and fails the run."""

SEVERITY_WARN: Final[str] = "WARN"
"""Labelled observation: reported in full, but does not fail the run."""

ACK_CONTAINED: Final[str] = "contained"
"""Acked write whose whole lifespan lies inside a quorum-loss window."""

ACK_STRADDLING: Final[str] = "straddling"
"""Acked write overlapping a quorum-loss window but crossing a boundary."""

ACK_OUTSIDE: Final[str] = "outside"
"""Acked write with no overlap against any quorum-loss window."""


# ─────────────────────────────────────────────────────────────────────────────
# Outcome taxonomy
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AttemptClass:
    """Classification of one psql attempt, with the evidence that produced it."""

    outcome: str  # ATTEMPT_* constant
    reason: str  # matched pattern, "exit-0", or "unclassified"


def classify_attempt(returncode: int, output: str) -> AttemptClass:
    """Classify a psql invocation from its exit status and combined output.

    Order matters. "acked" is granted only on exit status 0. A recognised
    server-side abort or routing refusal is a *definite* non-commit. Everything
    else — including an error nobody has catalogued yet — is indeterminate,
    because the sound failure direction is to widen the permitted bound rather
    than to assert a non-commit we cannot prove.

    *output* must include stderr: `run_cmd` reports its own timeout as
    ``(-1, "", "timeout")``, so a caller that inspects stdout alone sees an
    empty string and would classify a timed-out COMMIT as unclassified.
    """
    if returncode == 0:
        return AttemptClass(ATTEMPT_ACKED, "exit-0")
    lower = output.lower()
    for pattern in SERVER_REJECTION_PATTERNS:
        if pattern in lower:
            return AttemptClass(ATTEMPT_REJECTED, pattern)
    for pattern in REJECTION_PATTERNS:
        if pattern in lower:
            return AttemptClass(ATTEMPT_ROUTING_REJECTED, pattern)
    for pattern in INDETERMINATE_PATTERNS:
        if pattern in lower:
            return AttemptClass(ATTEMPT_INDETERMINATE, pattern)
    return AttemptClass(ATTEMPT_INDETERMINATE, "unclassified")


# ─────────────────────────────────────────────────────────────────────────────
# History data model
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class OpRecord:
    """A single write attempt with precise timing."""

    seq: int
    value: int
    start_ts: float  # time.monotonic() when the attempt started
    end_ts: float  # time.monotonic() when the attempt completed
    wall_start: float  # time.time() at start (for human-readable logs)
    outcome: str  # "acked" | "errored" | "indeterminate"
    port: int  # gateway port that produced this outcome


@dataclass
class FaultWindow:
    """An open/closed fault injection interval (monotonic timestamps)."""

    kind: str  # "kill_leader" | "pause_node" | "network_partition" | "quorum_loss" | …
    start_ts: float  # monotonic — when the fault was injected
    end_ts: float  # monotonic — when the cluster was confirmed healthy again (set on close)
    detail: str = ""  # e.g. "killed node2"

    @property
    def is_quorum_loss(self) -> bool:
        """True for faults where a majority of nodes are unavailable."""
        return "quorum" in self.kind or "majority" in self.kind


@dataclass
class LeaderPollRound:
    """Results of one concurrent poll of all three management nodes."""

    ts: float  # monotonic time when the round was issued
    responses: dict[int, int | None]  # mgmt_port → leader_id (None = no response)

    @property
    def unique_leaders(self) -> set[int]:
        """Non-None leader IDs seen in this round."""
        return {v for v in self.responses.values() if v is not None}

    @property
    def leaders_disagree(self) -> bool:
        """The responding nodes named more than one leader this round.

        Not split-brain on its own. `RaftMetrics::current_leader` is a local
        belief and an isolated node cannot refresh it, so a deposed leader keeps
        naming itself until it hears otherwise — with its lease already expired
        and writes already refused. See docs/STATE_MACHINE.md section 1: the
        only reliable reading is agreement across a majority, and write
        authority is what L1 is about, which `dual_writability_prober.py`
        measures directly.
        """
        return len(self.unique_leaders) > 1

    @property
    def quorum_leader(self) -> int | None:
        """The leader a majority of *all* nodes named, or None without one."""
        counts = Counter(v for v in self.responses.values() if v is not None)
        if not counts:
            return None
        leader, votes = counts.most_common(1)[0]
        return leader if votes * 2 > len(self.responses) else None


@dataclass
class SnapshotRecord:
    """A point-in-time DB read taken immediately after a fault heals."""

    ts: float
    after_fault: str  # human label of the fault that just healed
    acked_before: set[int]  # copy of acked_set at snapshot time
    db_contents: set[int]  # values read from the DB


@dataclass
class TransferRecord:
    """A single bank-transfer attempt, identified by a unique transfer id.

    The id is what makes double application detectable: the transfer writes its
    own id into `bank_ledger` inside the same transaction that moves the money,
    so the checker can count applications instead of only conserving a sum.
    """

    transfer_id: int
    from_id: int
    to_id: int
    amount: int
    start_ts: float  # time.monotonic() when the attempt started
    end_ts: float  # time.monotonic() when the attempt completed
    outcome: str  # "acked" | "rejected" | "indeterminate"
    port: int  # gateway port that produced this outcome
    reason: str = ""  # AttemptClass.reason that produced the outcome


@dataclass(frozen=True)
class LedgerRow:
    """One applied transfer as read back from the `bank_ledger` table."""

    transfer_id: int
    from_id: int
    to_id: int
    amount: int


@dataclass
class Violation:
    """A finding produced by the checker.

    FATAL findings are invariant violations and drive the exit code. WARN
    findings are labelled observations: surfaced in the summary, the detail
    section and the artifact, but they do not fail the run.
    """

    invariant: str  # "I1"-"I7", "L0"/"L2"/"L3", "B1"-"B4", "C1", "M1", or an "-WARN" variant
    message: str
    evidence: object = None  # supporting detail (sorted lists, counts, etc.)
    severity: str = SEVERITY_FATAL


@dataclass
class ContentionRun:
    """What the contention burst (step 9) acked, per key.

    `skipped` is not the same as empty: a step that could not set its table up
    has nothing to say about the counters, while one that ran and acked nothing
    does.
    """

    skipped: bool = False
    acked: dict[int, int] = field(default_factory=dict)
    indeterminate: dict[int, int] = field(default_factory=dict)


@dataclass
class MonotonicRun:
    """The reads step 10 recorded, and the writes it got acks for."""

    skipped: bool = False
    observations: list[tuple[int, int]] = field(default_factory=list)
    acked: list[int] = field(default_factory=list)


@dataclass
class History:
    """Accumulated record of the entire test run, shared across threads."""

    ops: list[OpRecord] = field(default_factory=list)
    faults: list[FaultWindow] = field(default_factory=list)
    leader_polls: list[LeaderPollRound] = field(default_factory=list)
    snapshots: list[SnapshotRecord] = field(default_factory=list)
    transfers: list[TransferRecord] = field(default_factory=list)

    acked_set: set[int] = field(default_factory=set)
    errored_set: set[int] = field(default_factory=set)
    indeterminate_set: set[int] = field(default_factory=set)

    unclassified_attempts: int = 0
    """Insert / transfer attempts whose error matched no known pattern.

    A non-zero count is not a violation — those attempts were counted as
    indeterminate, which is sound — but it means the outcome taxonomy has a gap
    worth a new entry in INDETERMINATE_PATTERNS or SERVER_REJECTION_PATTERNS.
    """

    contention: ContentionRun | None = None
    """What step 9 observed, or `None` if it never ran."""

    monotonic: MonotonicRun | None = None
    """What step 10 observed, or `None` if it never ran."""

    _counter: int = field(default=0, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def next_seq(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter

    def record_op(self, op: OpRecord) -> None:
        with self._lock:
            self.ops.append(op)
            if op.outcome == "acked":
                self.acked_set.add(op.value)
            elif op.outcome == "errored":
                self.errored_set.add(op.value)
            else:
                self.indeterminate_set.add(op.value)

    def record_transfer(self, transfer: TransferRecord) -> None:
        with self._lock:
            self.transfers.append(transfer)

    def note_unclassified(self) -> None:
        with self._lock:
            self.unclassified_attempts += 1

    def record_poll(self, round_: LeaderPollRound) -> None:
        with self._lock:
            self.leader_polls.append(round_)

    def open_fault(self, kind: str, detail: str = "") -> FaultWindow:
        fw = FaultWindow(kind=kind, start_ts=time.monotonic(), end_ts=0.0, detail=detail)
        with self._lock:
            self.faults.append(fw)
        return fw

    def close_fault(self, fw: FaultWindow) -> None:
        fw.end_ts = time.monotonic()

    def add_snapshot(self, snap: SnapshotRecord) -> None:
        with self._lock:
            self.snapshots.append(snap)

    @property
    def total_attempted(self) -> int:
        return self._counter


_LAST_HISTORY: History | None = None
"""The History built by the current `run()`.

The post-recovery checkers (`check_bank_invariants`, `check_contention_invariant`,
`check_monotonic_read_invariant`) read the in-memory attempt records from here so
they can keep the same no-argument shape as the DB-reading helpers around them.
The pure comparison functions they delegate to take their inputs explicitly and
are unit-tested without a cluster.
"""


# ─────────────────────────────────────────────────────────────────────────────
# Background leader sampler
# ─────────────────────────────────────────────────────────────────────────────


class LeaderSampler:
    """Daemon thread: polls all three mgmt nodes concurrently every 0.5s.

    Each round issues three concurrent HTTP requests and records a
    LeaderPollRound.  Split-brain is detectable if two ports return
    different non-None leader IDs in the same round.
    """

    def __init__(self, history: History) -> None:
        self._history = history
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="leader-sampler")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _poll_one(self, port: int) -> int | None:
        try:
            r = subprocess.run(
                [
                    "curl",
                    "-sf",
                    "--max-time",
                    "1",
                    f"http://localhost:{port}/api/v1/cluster/leader",
                ],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if r.returncode == 0:
                result: int | None = json.loads(r.stdout).get("leader_id")
                return result
        except Exception:
            pass
        return None

    def _run(self) -> None:
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="lsamp") as ex:
            while not self._stop.is_set():
                ts = time.monotonic()
                futures = {ex.submit(self._poll_one, p): p for p in MGMT_PORTS}
                done, _ = futures_wait(futures, timeout=2.0)
                responses: dict[int, int | None] = {futures[f]: None for f in futures}
                for f in done:
                    with contextlib.suppress(Exception):
                        responses[futures[f]] = f.result()
                self._history.record_poll(LeaderPollRound(ts=ts, responses=responses))
                self._stop.wait(LEADER_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# Shell helpers
# ─────────────────────────────────────────────────────────────────────────────


def run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
    """Run a shell command; return (returncode, stdout, stderr).

    Returns (-1, "", "timeout") if the command exceeds *timeout* seconds.
    """
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def docker_compose(*args: str) -> tuple[int, str, str]:
    return run_cmd("docker compose " + " ".join(args), timeout=60)


def compose_project() -> str:
    """Active compose project name.

    ``docker-compose.yml`` sets ``name: pgbattery``, but ``COMPOSE_PROJECT_NAME``
    overrides it and every CI workflow sets a per-run value. Literal
    ``pgbattery-node1-1`` / ``pgbattery_raft_net`` names therefore do not exist
    in CI, which silently turned the partition fault into a no-op.
    """
    return os.environ.get("COMPOSE_PROJECT_NAME", "pgbattery")


def container_name(service: str) -> str:
    """Resolve a compose service to its container name.

    Asks docker rather than string-building, so a change to compose's naming
    convention cannot silently reintroduce an unresolvable name.
    """
    rc, out, err = docker_compose("ps", "-q", service)
    cid = out.strip().split("\n")[-1].strip() if rc == 0 else ""
    if not cid:
        raise RuntimeError(
            f"cannot resolve container for service {service!r} in project "
            f"{compose_project()!r}: {err.strip() or 'no container id'}"
        )
    return cid


def raft_network_name() -> str:
    """Resolve the cluster network name for the active project."""
    return f"{compose_project()}_raft_net"


def find_leader() -> tuple[str | None, int | None]:
    """Return (node_name, gateway_port) for the current leader, or (None, None)."""
    for port in MGMT_PORTS:
        try:
            r = subprocess.run(
                ["curl", "-sf", f"http://localhost:{port}/api/v1/cluster/leader"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if r.returncode == 0:
                data = json.loads(r.stdout)
                lid = data.get("leader_id")
                if lid is not None:
                    return NODES[lid - 1], GATEWAY_PORTS[lid - 1]
        except Exception:
            continue
    return None, None


def wait_cluster_healthy(timeout: int = 60) -> bool:
    """Poll until a leader is discoverable or *timeout* seconds pass."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader, _ = find_leader()
        if leader is not None:
            return True
        time.sleep(2)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Core: write attempt + snapshot read
# ─────────────────────────────────────────────────────────────────────────────


def try_insert(value: int, history: History) -> str:
    """Attempt INSERT of *value*, classify the outcome, and record to history.

    Moves on to the next gateway port only while the outcome is a *definite*
    non-commit at that port.  An indeterminate outcome ends the attempt
    immediately: the INSERT may already be durable, so re-issuing it would make
    the recorded history a lie about what was attempted once.

    Returns "acked", "errored", or "indeterminate" — and appends exactly one
    OpRecord with monotonic timestamps.
    """
    seq = history.next_seq()
    start_ts = time.monotonic()
    wall_start = time.time()
    sql = f"INSERT INTO jepsen(id) VALUES ({value})"

    def record(outcome: str, port: int) -> str:
        history.record_op(
            OpRecord(seq, value, start_ts, time.monotonic(), wall_start, outcome, port)
        )
        return outcome

    last_port = GATEWAY_PORTS[-1]
    for port in GATEWAY_PORTS:
        last_port = port
        cmd = f'psql -h localhost -p {port} -U postgres -c "{sql}" 2>&1'
        try:
            r = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=PSQL_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            return record("indeterminate", port)
        cls = classify_attempt(r.returncode, r.stdout + r.stderr)
        if cls.reason == "unclassified":
            history.note_unclassified()
        if cls.outcome == ATTEMPT_ACKED:
            return record("acked", port)
        if cls.outcome == ATTEMPT_INDETERMINATE:
            return record("indeterminate", port)
        if cls.outcome == ATTEMPT_REJECTED:
            return record("errored", port)  # the server itself refused; no port will differ

    return record("errored", last_port)


def do_inserts(n: int, history: History, console: Console) -> None:
    """Insert *n* integers sequentially, logging each to *history*."""
    for _ in range(n):
        value = history.total_attempted + 1
        result = try_insert(value, history)
        console.print(f"  {value:>4} → {result}", highlight=False)
        time.sleep(0.05)


def read_all_from_db() -> set[int] | None:
    """Read all IDs from the jepsen table via any available gateway port."""
    for port in GATEWAY_PORTS:
        rc, out, _ = run_cmd(
            f"psql -h localhost -p {port} -U postgres -t -A -c 'SELECT id FROM jepsen ORDER BY id'",
            timeout=10,
        )
        if rc == 0 and out.strip():
            ids: set[int] = set()
            for line in out.strip().splitlines():
                line = line.strip()
                if line.lstrip("-").isdigit():
                    ids.add(int(line))
            return ids
    return None


def check_duplicates() -> tuple[int | None, int | None]:
    """Return (total_count, distinct_count) from the jepsen table."""
    for port in GATEWAY_PORTS:
        rc, out, _ = run_cmd(
            f"psql -h localhost -p {port} -U postgres -t -A "
            f"-c 'SELECT COUNT(*), COUNT(DISTINCT id) FROM jepsen'",
            timeout=10,
        )
        if rc == 0 and out.strip():
            parts = out.strip().split("|")
            if len(parts) == 2:
                return int(parts[0]), int(parts[1])
    return None, None


def take_snapshot(history: History, after_fault: str, console: Console) -> None:
    """Read the DB and record a SnapshotRecord against the current acked_set."""
    console.print(f"  [dim]snapshot after '{after_fault}'…[/]")
    with history._lock:
        acked_copy = history.acked_set.copy()
    db = read_all_from_db()
    if db is None:
        console.print(
            f"  [yellow]WARNING:[/] snapshot after '{after_fault}' failed - no DB response"
        )
        return
    snap = SnapshotRecord(
        ts=time.monotonic(),
        after_fault=after_fault,
        acked_before=acked_copy,
        db_contents=db,
    )
    history.add_snapshot(snap)
    missing = acked_copy - db
    if missing:
        console.print(
            f"  [red]SNAPSHOT EARLY-WARNING:[/] {len(missing)} acked value(s) missing "
            f"post-'{after_fault}': {sorted(missing)[:10]}"
        )


def bank_transfer(transfer_id: int, from_id: int, to_id: int, amount: int) -> TransferRecord:
    """Attempt one uniquely-identified bank transfer; return what happened.

    The transaction stamps *transfer_id* into `bank_ledger` alongside the two
    balance updates, so a transfer that gets applied twice is visible to B3/B4
    instead of hiding behind B1's sum conservation. The ledger primary key also
    makes the transfer idempotent on the server side.

    Retry discipline: another gateway port is tried only after a *definite*
    non-commit that a different port might route past. A timeout — or any other
    unknown-fate error — returns "indeterminate" without re-running anything: a
    COMMIT that timed out may already be durable, and re-issuing it is exactly
    how a checker ends up double-applying a transfer it believes it applied once.

    Server-side CHECK (balance >= 0) enforcement rolls back an overdrawing
    transfer, which classifies as a definite rejection.
    """
    sql = (
        f"BEGIN; "
        f"INSERT INTO {BANK_LEDGER_TABLE}(transfer_id, from_id, to_id, amount) "
        f"VALUES ({transfer_id}, {from_id}, {to_id}, {amount}); "
        f"UPDATE bank_accounts SET balance = balance - {amount} WHERE id = {from_id}; "
        f"UPDATE bank_accounts SET balance = balance + {amount} WHERE id = {to_id}; "
        f"COMMIT;"
    )
    start_ts = time.monotonic()

    def record(outcome: str, port: int, reason: str) -> TransferRecord:
        return TransferRecord(
            transfer_id=transfer_id,
            from_id=from_id,
            to_id=to_id,
            amount=amount,
            start_ts=start_ts,
            end_ts=time.monotonic(),
            outcome=outcome,
            port=port,
            reason=reason,
        )

    last_port = GATEWAY_PORTS[-1]
    last_reason = "no-attempt"
    for port in GATEWAY_PORTS:
        last_port = port
        rc, out, err = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{sql}" 2>&1',
            timeout=PSQL_TIMEOUT,
        )
        # stderr matters: run_cmd reports its own timeout as (-1, "", "timeout"),
        # and an empty stdout matches no pattern at all.
        cls = classify_attempt(rc, out + err)
        last_reason = cls.reason
        if cls.outcome == ATTEMPT_ACKED:
            return record("acked", port, cls.reason)
        if cls.outcome == ATTEMPT_INDETERMINATE:
            return record("indeterminate", port, cls.reason)
        if cls.outcome == ATTEMPT_REJECTED:
            return record("rejected", port, cls.reason)
    return record("rejected", last_port, last_reason)


def read_bank_balances() -> dict[int, int] | None:
    """Read every account balance via any available gateway port."""
    for port in GATEWAY_PORTS:
        rc, out, _ = run_cmd(
            f"psql -h localhost -p {port} -U postgres -t -A "
            f"-c 'SELECT id, balance FROM bank_accounts ORDER BY id'",
            timeout=10,
        )
        if rc == 0 and out.strip():
            balances: dict[int, int] = {}
            for line in out.strip().splitlines():
                parts = line.strip().split("|")
                if len(parts) == 2 and parts[0].isdigit() and parts[1].lstrip("-").isdigit():
                    balances[int(parts[0])] = int(parts[1])
            if balances:
                return balances
    return None


def read_bank_ledger() -> list[LedgerRow] | None:
    """Read every applied transfer from `bank_ledger` via any gateway port."""
    for port in GATEWAY_PORTS:
        rc, out, _ = run_cmd(
            f"psql -h localhost -p {port} -U postgres -t -A "
            f"-c 'SELECT transfer_id, from_id, to_id, amount FROM {BANK_LEDGER_TABLE} "
            f"ORDER BY transfer_id'",
            timeout=10,
        )
        if rc == 0:
            rows: list[LedgerRow] = []
            for line in out.strip().splitlines():
                parts = line.strip().split("|")
                if len(parts) == 4 and all(p.strip().lstrip("-").isdigit() for p in parts):
                    rows.append(
                        LedgerRow(
                            transfer_id=int(parts[0]),
                            from_id=int(parts[1]),
                            to_id=int(parts[2]),
                            amount=int(parts[3]),
                        )
                    )
            return rows
    return None


def check_bank_invariants() -> list[Violation]:
    """B1-B4: read the bank tables, then compare them against the attempts."""
    balances = read_bank_balances()
    if balances is None:
        return [Violation("B1", "Could not read bank_accounts after recovery", None)]
    ledger = read_bank_ledger()
    transfers = list(_LAST_HISTORY.transfers) if _LAST_HISTORY is not None else []
    if ledger is None:
        if not transfers:
            # No transfer was ever attempted, so there is nothing the ledger
            # could bound. B1/B2 still apply to whatever the accounts hold.
            return _check_bank_against_db(balances, [], [])
        return [
            Violation(
                "B3",
                f"Could not read {BANK_LEDGER_TABLE} after recovery — "
                f"{len(transfers)} transfer attempt(s) are unbounded",
                {"attempted": len(transfers)},
            )
        ]
    return _check_bank_against_db(balances, ledger, transfers)


def _check_bank_against_db(
    balances: dict[int, int],
    ledger: list[LedgerRow],
    transfers: list[TransferRecord],
) -> list[Violation]:
    """Compare the bank tables against the recorded transfer attempts.

    B1  BANK_TOTAL_CONSERVED
        SUM(balance) must equal BANK_TOTAL after all transfers.
    B2  NO_NEGATIVE_BALANCE
        MIN(balance) must be >= 0 (enforced by a DB CHECK constraint; a
        violation here means the constraint was bypassed somehow).
    B3  TRANSFER_APPLIED_AT_MOST_ONCE
        Each attempted transfer id appears at most once in the ledger, exactly
        once if it was acked, and not at all if it was definitely rejected.
        No ledger row may carry an id we never attempted.
    B4  BANK_LEDGER_RECONCILED
        Every balance equals its initial balance plus ledger credits minus
        ledger debits. B1 cannot see a transfer applied twice — the sum is
        still conserved — whereas the per-account reconciliation can.
    """
    violations: list[Violation] = []

    total = sum(balances.values())
    if total != BANK_TOTAL:
        violations.append(
            Violation(
                "B1",
                f"Bank balance sum violated: expected {BANK_TOTAL}, got {total}",
                {"expected": BANK_TOTAL, "actual": total, "accounts": len(balances)},
            )
        )
    minimum = min(balances.values(), default=0)
    if minimum < 0:
        violations.append(
            Violation(
                "B2",
                f"Negative balance found (CHECK constraint bypassed): min={minimum}",
                {"min_balance": minimum},
            )
        )

    if not transfers:
        return violations  # the bank step recorded nothing; B3/B4 have no bound

    applied_count: dict[int, int] = {}
    for row in ledger:
        applied_count[row.transfer_id] = applied_count.get(row.transfer_id, 0) + 1

    attempted = {t.transfer_id: t for t in transfers}

    duplicated = {tid: n for tid, n in applied_count.items() if n > 1}
    if duplicated:
        violations.append(
            Violation(
                "B3",
                f"{len(duplicated)} transfer(s) applied more than once "
                f"(ledger holds duplicate transfer ids)",
                sorted(duplicated.items())[:10],
            )
        )

    missing_acked = sorted(
        tid for tid, t in attempted.items() if t.outcome == "acked" and tid not in applied_count
    )
    if missing_acked:
        violations.append(
            Violation(
                "B3",
                f"{len(missing_acked)} acked transfer(s) absent from the ledger (lost commit)",
                missing_acked[:10],
            )
        )

    applied_but_refused = sorted(
        tid for tid in applied_count if tid in attempted and attempted[tid].outcome == "rejected"
    )
    if applied_but_refused:
        violations.append(
            Violation(
                "B3",
                f"{len(applied_but_refused)} definitely-rejected transfer(s) present "
                f"in the ledger (phantom transfer)",
                applied_but_refused[:10],
            )
        )

    never_attempted = sorted(tid for tid in applied_count if tid not in attempted)
    if never_attempted:
        violations.append(
            Violation(
                "B3",
                f"{len(never_attempted)} ledger transfer id(s) were never attempted",
                never_attempted[:10],
            )
        )

    # Reconcile against distinct ledger ids so B4 is an independent statement
    # about the balances: duplicated ledger rows are B3's business.
    distinct_rows = {row.transfer_id: row for row in ledger}
    expected: dict[int, int] = dict.fromkeys(balances, BANK_INITIAL_BALANCE)
    for row in distinct_rows.values():
        if row.from_id in expected:
            expected[row.from_id] -= row.amount
        if row.to_id in expected:
            expected[row.to_id] += row.amount
    mismatched = [
        (acct, expected[acct], balances[acct])
        for acct in sorted(balances)
        if expected[acct] != balances[acct]
    ]
    if mismatched:
        violations.append(
            Violation(
                "B4",
                f"{len(mismatched)} account balance(s) do not match the ledger "
                f"(transfer applied a number of times the ledger does not record)",
                [{"account": a, "expected": e, "observed": o} for a, e, o in mismatched[:10]],
            )
        )

    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Reusable fault injectors (parity with testing/linearizability_register.py)
# ─────────────────────────────────────────────────────────────────────────────


def _kill_leader_now() -> str | None:
    leader, _ = find_leader()
    if leader is None:
        return None
    docker_compose("kill", leader)
    return leader


def _partition_leader_now(heal_after: float = 4.0) -> str | None:
    """Detach the leader now; heal in the background after `heal_after`.

    The window is opened synchronously so the caller's writes land inside it, and
    closed from a thread. Both halves are verified by the primitive, including
    the reattach — which restores the address read off the container rather than
    one derived from a node index, so it cannot put a node back somewhere else.
    """
    leader, _ = find_leader()
    if leader is None:
        return None
    # enter_context injects and verifies synchronously, raising if the detach did
    # not land: an empty fault window otherwise reads as "no violations during
    # the partition" rather than "the partition never happened".
    stack = contextlib.ExitStack()
    stack.enter_context(fp.network_detached(leader))

    def _heal() -> None:
        time.sleep(heal_after)
        stack.close()

    _spawn_fault_thread(_heal, f"heal-partition-{leader}")
    return leader


def _freeze_leader_now(hold: float = 3.0) -> str | None:
    leader, _ = find_leader()
    if leader is None:
        return None
    rc, out, _ = run_cmd(
        f"docker compose exec -T {leader} sh -c 'pgrep -x pgbattery | head -1'",
        timeout=5,
    )
    pid = out.strip().split("\n")[-1].strip() if rc == 0 else ""
    if not pid.isdigit():
        return None
    run_cmd(f"docker compose exec -T --user root {leader} kill -STOP {pid}", timeout=5)

    def _thaw() -> None:
        time.sleep(hold)
        run_cmd(f"docker compose exec -T --user root {leader} kill -CONT {pid}", timeout=5)

    threading.Thread(target=_thaw, daemon=True).start()
    return leader


def _transfer_leader_now() -> str | None:
    leader, _ = find_leader()
    if leader is None:
        return None
    idx = NODES.index(leader) + 1
    target = (idx % len(NODES)) + 1
    mgmt = MGMT_PORTS[idx - 1]
    _, tok, _ = run_cmd("grep PGBATTERY_MANAGEMENT_API_TOKEN .env | cut -d= -f2", timeout=5)
    token = tok.strip()
    run_cmd(
        f"curl -s -X POST --max-time 10 "
        f"-H 'x-pgbattery-token: {token}' "
        f"http://localhost:{mgmt}/api/v1/cluster/transfer-leadership/{target}",
        timeout=15,
    )
    return leader


def _cascade_kill_now(kills: int = 2, gap: float = 1.5) -> str | None:
    last: str | None = None
    for _ in range(kills):
        leader, _ = find_leader()
        if leader is None:
            time.sleep(gap)
            continue
        last = leader
        docker_compose("kill", leader)
        docker_compose("start", leader)
        time.sleep(gap)
    return last


def _quorum_loss_now(restore_after: float = 4.0) -> str | None:
    leader, _ = find_leader()
    if leader is None:
        return None
    others = [n for n in NODES if n != leader]
    for n in others:
        docker_compose("kill", n)

    def _restore() -> None:
        time.sleep(restore_after)
        docker_compose("start", others[0])

    _spawn_fault_thread(_restore, "restore-quorum")
    return leader


STORM_KINDS: Final[tuple[str, ...]] = ("kill", "partition", "freeze", "transfer")


def chaos_storm_plan(rng: random.Random, duration: float = 8.0) -> list[tuple[float, str]]:
    """The (offset, fault) schedule a storm will fire, drawn but not executed.

    Pure, so the same seed provably yields the same storm — which is what makes
    a failed run replayable — and so the plan can be asserted without a cluster.
    """
    n = rng.randint(2, 4)
    times = sorted(rng.uniform(0, duration) for _ in range(n))
    kinds = [rng.choice(STORM_KINDS) for _ in range(n)]
    return list(zip(times, kinds, strict=True))


def _chaos_storm_now(rng: random.Random, duration: float = 8.0) -> str | None:
    """Fire 2-4 random faults at random times within `duration` seconds.

    Mixes kill, partition, freeze, transfer. Returns the leader observed
    when the storm started.
    """
    leader, _ = find_leader()
    start = time.monotonic()
    for ft, kind in chaos_storm_plan(rng, duration):
        elapsed = time.monotonic() - start
        if ft > elapsed:
            time.sleep(ft - elapsed)
        _spawn_fault_thread(_FAULT_DISPATCH[kind], f"chaos-{kind}")
    return leader


_FAULT_THREAD_ERRORS: list[str] = []
"""Failures raised inside fault threads, for `main` to fail the run on.

Faults are fired from daemon threads, so an exception in one is printed to stderr
and then discarded: the run continues, finds no violation in a window where no
fault was ever injected, and reports PASS. The primitives raise rather than
no-op, which only helps if somebody reads the exception."""

_FAULT_THREAD_LOCK: Final[threading.Lock] = threading.Lock()


def _spawn_fault_thread(target: Callable[[], object], name: str) -> threading.Thread:
    """Run `target` in a daemon thread, recording any failure it raises."""

    def _wrapped() -> None:
        try:
            target()
        except BaseException as exc:
            with _FAULT_THREAD_LOCK:
                _FAULT_THREAD_ERRORS.append(f"{name}: {type(exc).__name__}: {exc}")

    thread = threading.Thread(target=_wrapped, name=name, daemon=True)
    thread.start()
    return thread


def _record_fault_error(where: str, detail: str) -> None:
    """Record a fault that could not be injected, for `main` to fail on."""
    with _FAULT_THREAD_LOCK:
        _FAULT_THREAD_ERRORS.append(f"{where}: {detail}")


def fault_thread_errors() -> list[str]:
    """Snapshot of failures raised inside fault threads."""
    with _FAULT_THREAD_LOCK:
        return list(_FAULT_THREAD_ERRORS)


_FAULT_DISPATCH: dict[str, Callable[[], str | None]] = {
    "kill": _kill_leader_now,
    "partition": _partition_leader_now,
    "freeze": _freeze_leader_now,
    "transfer": _transfer_leader_now,
    "cascade": _cascade_kill_now,
    "quorum_loss": _quorum_loss_now,
}

FAULT_KINDS: Final[tuple[str, ...]] = (*_FAULT_DISPATCH, "chaos_storm")


def fire_fault(kind: str, rng: random.Random) -> str | None:
    """Fire one named fault, returning the leader it aimed at.

    Only the storm draws, so the run's seed reaches exactly the fault that
    needs it and the others stay fixed sequences.
    """
    if kind == "chaos_storm":
        return _chaos_storm_now(rng)
    return _FAULT_DISPATCH[kind]()


def _restore_killed_nodes() -> None:
    """Bring back any nodes that an attack may have left down."""
    for n in NODES:
        docker_compose("start", n)


# ─────────────────────────────────────────────────────────────────────────────
# Fault injection steps
# ─────────────────────────────────────────────────────────────────────────────


def step_baseline(history: History, console: Console) -> None:
    """Step 1: 50 inserts with no faults (baseline)."""
    console.print("[bold]Step 1:[/] baseline — 50 inserts, no faults")
    do_inserts(50, history, console)


def step_kill_leader(history: History, console: Console) -> None:
    """Step 2: Kill the leader, insert 50 values during failover, recover."""
    console.print("[bold]Step 2:[/] kill leader")
    leader, _ = find_leader()
    if leader is None:
        console.print("  [yellow]WARNING:[/] no leader found — inserting without fault")
        do_inserts(50, history, console)
        return

    console.print(f"  killing {leader}")
    fw = history.open_fault("kill_leader", f"killed {leader}")
    docker_compose("kill", leader)
    do_inserts(50, history, console)
    docker_compose("start", leader)
    wait_cluster_healthy(timeout=60)
    history.close_fault(fw)
    take_snapshot(history, "kill_leader", console)


def step_pause_random(history: History, console: Console, rng: random.Random) -> None:
    """Step 3: Pause a random node for the duration of 50 inserts, then resume."""
    node = rng.choice(NODES)
    console.print(f"[bold]Step 3:[/] pause {node}")
    fw = history.open_fault("pause_node", f"paused {node}")
    docker_compose("pause", node)
    do_inserts(50, history, console)
    docker_compose("unpause", node)
    wait_cluster_healthy(timeout=60)
    history.close_fault(fw)
    take_snapshot(history, f"pause_{node}", console)


def step_network_partition_leader(history: History, console: Console) -> None:
    """Step 4: Detach the leader from the raft overlay network during 50 inserts.

    Previously this reattached with a bare ``docker network connect``, which
    assigns a fresh address instead of the compose-pinned one, leaving every
    later step addressing the node at an IP it no longer held. The primitive
    restores the address it read.

    A missing leader is recorded as a fault error rather than degrading to 50
    unfaulted inserts: those would enter the history labelled as a partition
    window and read as coverage of a fault that never happened.
    """
    console.print("[bold]Step 4:[/] network-disconnect leader")
    leader, _ = find_leader()
    if leader is None:
        _record_fault_error("step_network_partition_leader", "no leader to detach")
        return

    console.print(f"  detaching {leader} from {raft_network_name()}")
    fw = history.open_fault("network_partition", f"partitioned {leader}")
    try:
        with fp.network_detached(leader):
            do_inserts(50, history, console)
    except fp.FaultError as exc:
        _record_fault_error("step_network_partition_leader", str(exc))
        return
    finally:
        wait_cluster_healthy(timeout=60)
        history.close_fault(fw)
    take_snapshot(history, "network_partition", console)


def step_majority_loss(history: History, console: Console) -> None:
    """Step 5: Kill 2 of 3 nodes (quorum loss), insert 20 values, recover."""
    console.print("[bold]Step 5:[/] kill 2 of 3 nodes (majority loss)")
    leader, _ = find_leader()
    victims = [n for n in NODES if n != leader][:2] if leader else NODES[:2]
    console.print(f"  killing {victims}")
    fw = history.open_fault("quorum_loss", f"killed {victims}")
    for v in victims:
        docker_compose("kill", v)
    do_inserts(20, history, console)
    for v in victims:
        docker_compose("start", v)
    wait_cluster_healthy(timeout=90)
    history.close_fault(fw)
    take_snapshot(history, "majority_loss", console)


def step_full_restart(history: History, console: Console) -> None:
    """Step 6: Restart the entire cluster, wait for leader election, insert 50 values."""
    console.print("[bold]Step 6:[/] full cluster restart")
    fw = history.open_fault("full_restart", "docker compose restart")
    docker_compose("restart")
    wait_cluster_healthy(timeout=90)
    history.close_fault(fw)
    take_snapshot(history, "full_restart", console)
    do_inserts(50, history, console)


def step_final_steady(history: History, console: Console) -> None:
    """Step 7: 50 inserts in steady state (post-fault baseline)."""
    console.print("[bold]Step 7:[/] final steady-state — 50 inserts")
    do_inserts(50, history, console)


def bank_transfer_plan(rng: random.Random, count: int, accounts: int) -> list[tuple[int, int, int]]:
    """The (from, to, amount) each transfer attempt will use.

    Drawn up front and pure, so a run that finds a ledger violation can be
    replayed from its seed against the same sequence of transfers.
    """
    plan: list[tuple[int, int, int]] = []
    for _ in range(count):
        source, target = rng.sample(range(1, accounts + 1), 2)
        plan.append((source, target, rng.randint(1, 100)))
    return plan


def step_bank_transfer(
    history: History,
    console: Console,
    rng: random.Random,
    attack: str = "kill",
    num_transfers: int = 40,
) -> None:
    """Step 8: bank transfer workload — B1-B4.

    Creates BANK_ACCOUNTS accounts plus the transfer ledger, then runs
    `num_transfers` uniquely-identified transfer attempts while injecting the
    named `attack` mid-workload. Every attempt is recorded on the history with
    its outcome, so the post-recovery checks in run() can bound the ledger:
    conservation (B1), non-negativity (B2), at-most-once application (B3) and
    balance/ledger reconciliation (B4).

    `attack` is one of: kill, partition, freeze, transfer, cascade,
    quorum_loss, chaos_storm — see `_FAULT_DISPATCH`.
    """
    console.print(
        f"[bold]Step 8:[/] bank transfer workload (attack={attack}, "
        f"B1-B4, total must equal {BANK_TOTAL})"
    )

    setup_sql = (
        "DROP TABLE IF EXISTS bank_accounts; "
        f"DROP TABLE IF EXISTS {BANK_LEDGER_TABLE}; "
        "CREATE TABLE bank_accounts "
        "(id INTEGER PRIMARY KEY, balance INTEGER NOT NULL CHECK (balance >= 0)); "
        f"CREATE TABLE {BANK_LEDGER_TABLE} "
        f"(transfer_id INTEGER PRIMARY KEY, from_id INTEGER NOT NULL, "
        f"to_id INTEGER NOT NULL, amount INTEGER NOT NULL); "
        f"INSERT INTO bank_accounts "
        f"SELECT generate_series(1, {BANK_ACCOUNTS}), {BANK_INITIAL_BALANCE};"
    )
    setup_ok = False
    for port in GATEWAY_PORTS:
        rc, _, _ = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{setup_sql}"',
            timeout=15,
        )
        if rc == 0:
            setup_ok = True
            break
    if not setup_ok:
        console.print(
            f"  [yellow]WARNING:[/] could not create bank_accounts / {BANK_LEDGER_TABLE} "
            f"- skipping bank workload"
        )
        return

    if attack not in FAULT_KINDS:
        console.print(f"  [red]Unknown attack '{attack}', falling back to 'kill'[/]")
        attack = "kill"

    # Fire the attack about 25% into the workload so we have transfers
    # before and during the fault.
    kickoff_at = max(2, num_transfers // 4)
    plan = bank_transfer_plan(rng, num_transfers, BANK_ACCOUNTS)
    fw: FaultWindow | None = None
    tally: dict[str, int] = {"acked": 0, "rejected": 0, "indeterminate": 0}
    fired_leader: str | None = None
    for i in range(num_transfers):
        if i == kickoff_at:
            console.print(f"  injecting attack: {attack}")
            fw = history.open_fault(
                f"bank_attack_{attack}", f"injecting {attack} mid-bank-workload"
            )
            fired_leader = fire_fault(attack, rng)
        source, target, amount = plan[i]
        # Transfer ids are 1-based and unique per attempt: they are the key the
        # ledger and B3/B4 count applications by.
        record = bank_transfer(i + 1, source, target, amount)
        history.record_transfer(record)
        if record.reason == "unclassified":
            history.note_unclassified()
        tally[record.outcome] += 1
        time.sleep(0.05)

    # Restore any nodes that may still be down; the dispatch's auto-heal
    # threads cover network partitions and freezes, but kill / cascade /
    # quorum_loss leave containers dead.
    _restore_killed_nodes()
    wait_cluster_healthy(timeout=60)
    if fw is not None:
        history.close_fault(fw)

    console.print(
        f"  {tally['acked']}/{num_transfers} transfers acked, "
        f"{tally['rejected']} rejected, {tally['indeterminate']} indeterminate "
        f"(indeterminate transfers are never retried)"
        + (f" (attack fired against {fired_leader})" if fired_leader else "")
    )


# ─────────────────────────────────────────────────────────────────────────────
# Step 9 — concurrent same-row contention (C1: NO_LOST_UPDATE)
# ─────────────────────────────────────────────────────────────────────────────


CONTENTION_KEYS: Final[int] = 3
"""Number of shared rows for the concurrent-increment workload."""

CONTENTION_WORKERS: Final[int] = 3
"""Concurrent client threads hammering the shared rows."""

CONTENTION_INCREMENTS_PER_WORKER: Final[int] = 60
"""How many UPDATE-by-1 attempts each worker issues."""


@dataclass
class IncrementOp:
    """A single counter-increment attempt."""

    key: int
    outcome: str  # "acked" | "rejected" | "indeterminate"


def _try_increment(port: int, key: int) -> str:
    """UPDATE counters SET val = val + 1 WHERE id = key.

    Returns "acked" (exit status 0), "rejected" (a *recognised* definite
    refusal), or "indeterminate" (timeout, connection failure, or any error we
    cannot classify). An unrecognised error must land in "indeterminate":
    counting it as rejected would tighten C1's upper bound past what we can
    prove and turn a committed increment into a fake ghost-increment report.
    """
    cmd = (
        f"psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 "
        f'-c "UPDATE counters SET val = val + 1 WHERE id = {key}" 2>&1'
    )
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=PSQL_TIMEOUT)
    except subprocess.TimeoutExpired:
        return "indeterminate"
    cls = classify_attempt(r.returncode, r.stdout + r.stderr)
    if cls.outcome == ATTEMPT_ACKED:
        return "acked"
    if cls.outcome == ATTEMPT_INDETERMINATE:
        return "indeterminate"
    return "rejected"


def _contention_worker(
    worker_id: int,
    results: list[IncrementOp],
    results_lock: threading.Lock,
    rng: random.Random,
) -> None:
    """Hammer CONTENTION_KEYS rows for CONTENTION_INCREMENTS_PER_WORKER iterations.

    Rotates through gateway ports so each worker probes leader and follower
    routes during the failover window.
    """
    port_idx = worker_id
    for _ in range(CONTENTION_INCREMENTS_PER_WORKER):
        port = GATEWAY_PORTS[port_idx % len(GATEWAY_PORTS)]
        port_idx += 1
        key = rng.randrange(CONTENTION_KEYS)
        outcome = _try_increment(port, key)
        with results_lock:
            results.append(IncrementOp(key=key, outcome=outcome))


def step_concurrent_contention(history: History, console: Console) -> None:
    """Step 9: concurrent same-row contention — verify C1 (NO_LOST_UPDATE).

    Spawns CONTENTION_WORKERS threads hammering CONTENTION_KEYS shared rows
    with UPDATE counters SET val = val + 1 WHERE id = key — a blind
    read-modify-write, not a compare-and-set: there is no expected-value
    predicate, and correctness under contention comes from PostgreSQL taking a
    row lock for the duration of the statement. Kills the leader mid-flight.
    Verifies per-key that:

        acked_count[key] <= db_val[key] <= acked_count[key] + indeterminate_count[key]

    The lower bound asserts no committed increment was lost (no lost-update);
    the upper bound asserts no extra increment appeared from nowhere (no
    ghost-write under split-brain). Indeterminate ops are allowed to have
    either landed or not.
    """
    console.print(
        f"[bold]Step 9:[/] concurrent contention "
        f"({CONTENTION_WORKERS} workers x {CONTENTION_INCREMENTS_PER_WORKER} increments)"
    )
    setup_sql = (
        "DROP TABLE IF EXISTS counters; "
        "CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER NOT NULL); "
        f"INSERT INTO counters SELECT generate_series(0, {CONTENTION_KEYS - 1}), 0;"
    )
    setup_ok = False
    for port in GATEWAY_PORTS:
        rc, _, _ = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{setup_sql}"',
            timeout=15,
        )
        if rc == 0:
            setup_ok = True
            break
    if not setup_ok:
        console.print("  [yellow]WARNING:[/] could not create counters table — skipping step 9")
        history.contention = ContentionRun(skipped=True)
        return

    leader, _ = find_leader()
    fw = None
    if leader is not None:
        console.print(f"  killing {leader} during contention burst")
        fw = history.open_fault("kill_leader_contention", f"killed {leader} during contention")

    increments: list[IncrementOp] = []
    increments_lock = threading.Lock()
    workers = [
        threading.Thread(
            target=_contention_worker,
            args=(i, increments, increments_lock, random.Random(7919 + i)),
            name=f"contention-w{i}",
            daemon=True,
        )
        for i in range(CONTENTION_WORKERS)
    ]
    for t in workers:
        t.start()

    # Kill the leader ~25% through the workload — gives each worker enough
    # acks before the fault to make lost-update violations detectable.
    if leader is not None:
        time.sleep(0.5)
        docker_compose("kill", leader)

    for t in workers:
        t.join(timeout=60)

    if leader is not None:
        docker_compose("start", leader)
        wait_cluster_healthy(timeout=60)
        if fw is not None:
            history.close_fault(fw)

    # Persist the per-key acked/indeterminate counts for the checker.
    acked_per_key: dict[int, int] = {k: 0 for k in range(CONTENTION_KEYS)}
    indet_per_key: dict[int, int] = {k: 0 for k in range(CONTENTION_KEYS)}
    for op in increments:
        if op.outcome == "acked":
            acked_per_key[op.key] += 1
        elif op.outcome == "indeterminate":
            indet_per_key[op.key] += 1
    history.contention = ContentionRun(acked=acked_per_key, indeterminate=indet_per_key)

    summary = ", ".join(
        f"k{k}: {acked_per_key[k]}+{indet_per_key[k]}?" for k in range(CONTENTION_KEYS)
    )
    console.print(f"  per-key acked + indeterminate: {summary}")


def check_contention_invariant() -> list[Violation]:
    """C1: NO_LOST_UPDATE — db_val[key] in [acked, acked + indeterminate]."""
    violations: list[Violation] = []
    for port in GATEWAY_PORTS:
        rc, out, _ = run_cmd(
            f"psql -h localhost -p {port} -U postgres -t -A "
            f"-c 'SELECT id, val FROM counters ORDER BY id'",
            timeout=10,
        )
        if rc == 0 and out.strip():
            db_val: dict[int, int] = {}
            for line in out.strip().splitlines():
                parts = line.strip().split("|")
                if len(parts) == 2 and parts[0].isdigit() and parts[1].lstrip("-").isdigit():
                    db_val[int(parts[0])] = int(parts[1])
            return _check_contention_against_db(db_val)
    violations.append(Violation("C1", "Could not read counters after recovery", None))
    return violations


def _check_contention_against_db(db_val: dict[int, int]) -> list[Violation]:
    """Compare DB values against the in-memory acked / indeterminate counts."""
    violations: list[Violation] = []
    run = _LAST_HISTORY.contention if _LAST_HISTORY is not None else None
    if run is None or run.skipped:
        return violations  # step itself didn't run
    acked, indet = run.acked, run.indeterminate
    if not acked and not indet:
        return violations  # nothing to compare
    for key in range(CONTENTION_KEYS):
        lo = acked.get(key, 0)
        hi = lo + indet.get(key, 0)
        observed = db_val.get(key, 0)
        if observed < lo:
            violations.append(
                Violation(
                    "C1",
                    f"Lost update on key {key}: "
                    f"db={observed} < acked={lo} (indeterminate={indet.get(key, 0)})",
                    {"key": key, "db": observed, "acked": lo, "indeterminate": indet.get(key, 0)},
                )
            )
        elif observed > hi:
            violations.append(
                Violation(
                    "C1",
                    f"Ghost increment on key {key}: "
                    f"db={observed} > acked+indet={hi} (acked={lo}, indet={indet.get(key, 0)})",
                    {"key": key, "db": observed, "acked": lo, "indeterminate": indet.get(key, 0)},
                )
            )
    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Step 10 — monotonic-read session (M1: NO_READ_REGRESSION_ACROSS_FAILOVER)
# ─────────────────────────────────────────────────────────────────────────────

MONOTONIC_WRITES: Final[int] = 30
"""How many monotonic values to write across the failover window."""

MONOTONIC_KILL_AT: Final[int] = 15
"""Which iteration triggers the leader kill (mid-sequence)."""


def _try_write_monotonic(value: int) -> str:
    """Write `value` via any gateway. Returns "acked" | "rejected" | "indeterminate".

    Only a definite non-commit at the current port advances to the next one;
    an unknown fate ends the attempt, since the row may already be durable.
    """
    sql = f"INSERT INTO monotonic(val) VALUES ({value})"
    for port in GATEWAY_PORTS:
        cmd = f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{sql}" 2>&1'
        try:
            r = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=PSQL_TIMEOUT
            )
        except subprocess.TimeoutExpired:
            return "indeterminate"
        cls = classify_attempt(r.returncode, r.stdout + r.stderr)
        if cls.outcome == ATTEMPT_ACKED:
            return "acked"
        if cls.outcome == ATTEMPT_INDETERMINATE:
            return "indeterminate"
        if cls.outcome == ATTEMPT_REJECTED:
            return "rejected"
    return "rejected"


def _try_read_max_monotonic() -> int | None:
    """Read MAX(val) FROM monotonic via any available gateway. None on failure."""
    for port in GATEWAY_PORTS:
        rc, out, _ = run_cmd(
            f"psql -h localhost -p {port} -U postgres -t -A "
            f"-c 'SELECT COALESCE(MAX(val), 0) FROM monotonic' 2>&1",
            timeout=PSQL_TIMEOUT,
        )
        if rc == 0:
            for line in out.strip().splitlines():
                if line.strip().lstrip("-").isdigit():
                    return int(line.strip())
    return None


def step_monotonic_read_session(history: History, console: Console) -> None:
    """Step 10: monotonic-read session test — verify M1.

    Issues `MONOTONIC_WRITES` writes of strictly increasing values (1, 2, …)
    through the gateway. After each write, performs a read of MAX(val).
    Triggers a leader kill at `MONOTONIC_KILL_AT` to force a failover
    mid-sequence. Records every (read_index, observed_max) pair so the
    checker can verify that the observed-max sequence is non-decreasing
    — i.e. no read sees an older max than a prior read did.

    This is a weaker form of single-session monotonic-read (we use a fresh
    psql connection per op rather than a long-lived session), but the
    invariant is meaningful: if any read sees value N, every subsequent read
    must see ≥ N. A regression would indicate either a phantom rewind
    (split-brain accepting writes that get rolled back) or routing to a
    severely lagging follower.
    """
    console.print(
        f"[bold]Step 10:[/] monotonic-read session "
        f"({MONOTONIC_WRITES} writes, kill leader at iter {MONOTONIC_KILL_AT})"
    )
    setup_sql = "DROP TABLE IF EXISTS monotonic; CREATE TABLE monotonic (val INTEGER PRIMARY KEY);"
    setup_ok = False
    for port in GATEWAY_PORTS:
        rc, _, _ = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{setup_sql}"',
            timeout=15,
        )
        if rc == 0:
            setup_ok = True
            break
    if not setup_ok:
        console.print("  [yellow]WARNING:[/] could not create monotonic table — skipping step 10")
        history.monotonic = MonotonicRun(skipped=True)
        return

    observations: list[tuple[int, int]] = []  # (write_iter, observed_max_after_read)
    acked: list[int] = []
    leader_killed = False
    fw = None
    leader, _ = find_leader()

    for i in range(1, MONOTONIC_WRITES + 1):
        if i == MONOTONIC_KILL_AT and leader is not None:
            console.print(f"  killing {leader} at iter {i}")
            fw = history.open_fault("kill_leader_monotonic", f"killed {leader} at iter {i}")
            docker_compose("kill", leader)
            leader_killed = True

        outcome = _try_write_monotonic(i)
        if outcome == "acked":
            acked.append(i)
        observed_max = _try_read_max_monotonic()
        if observed_max is not None:
            observations.append((i, observed_max))
        time.sleep(0.05)

    if leader_killed and leader is not None:
        docker_compose("start", leader)
        wait_cluster_healthy(timeout=60)
        if fw is not None:
            history.close_fault(fw)

    # Persist for the checker.
    history.monotonic = MonotonicRun(observations=observations, acked=acked)
    console.print(f"  acked {len(acked)}/{MONOTONIC_WRITES}, recorded {len(observations)} reads")


def check_monotonic_read_invariant() -> list[Violation]:
    """M1: NO_READ_REGRESSION_ACROSS_FAILOVER.

    For every pair of recorded reads (i, max_i) and (j, max_j) with j > i,
    we require max_j >= max_i. A counter-example is a *regression*: a read
    that returns less than a previously-observed value.
    """
    violations: list[Violation] = []
    run = _LAST_HISTORY.monotonic if _LAST_HISTORY is not None else None
    if run is None or run.skipped:
        return violations
    obs = run.observations
    if not obs:
        return violations
    regressions: list[tuple[int, int, int, int]] = []  # (i, max_i, j, max_j)
    prev_iter, prev_max = obs[0]
    running_max = prev_max
    for cur_iter, cur_max in obs[1:]:
        if cur_max < running_max:
            regressions.append((prev_iter, running_max, cur_iter, cur_max))
        if cur_max > running_max:
            running_max = cur_max
        prev_iter, prev_max = cur_iter, cur_max
    if regressions:
        violations.append(
            Violation(
                "M1",
                f"{len(regressions)} read regression(s): observed_max decreased across reads",
                regressions[:10],
            )
        )
    # Also: the FINAL read should be >= every acked write.
    if obs:
        final_max = max(m for _, m in obs)
        acked = run.acked
        if acked and final_max < max(acked):
            violations.append(
                Violation(
                    "M1",
                    f"Final observed_max {final_max} < max acked {max(acked)} (durability loss)",
                    {"final_observed_max": final_max, "max_acked": max(acked)},
                )
            )
    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Invariant checker
# ─────────────────────────────────────────────────────────────────────────────


def classify_ack_vs_quorum_loss(
    op: OpRecord, quorum_windows: list[FaultWindow]
) -> tuple[str, FaultWindow | None]:
    """Position one acked write against the recorded quorum-loss windows.

    ACK_CONTAINED   [start_ts, end_ts] lies entirely inside a window. This is
                    the exact shape contract L2 forbids: FATAL.
    ACK_STRADDLING  Overlaps a window but crosses one of its boundaries. L2 has
                    nothing to say about it — the ack may have completed before
                    the majority actually went away, or after it came back, and
                    our window edges are only approximations of the real outage
                    — so it is surfaced as a labelled observation.
    ACK_OUTSIDE     No overlap with any window.

    Containment beats straddling: if any window contains the write, that is the
    verdict, no matter how many other windows it merely overlaps.
    """
    straddled: FaultWindow | None = None
    for fw in quorum_windows:
        if fw.start_ts <= op.start_ts and op.end_ts <= fw.end_ts:
            return ACK_CONTAINED, fw
        if op.start_ts < fw.end_ts and op.end_ts > fw.start_ts:
            straddled = straddled or fw
    if straddled is not None:
        return ACK_STRADDLING, straddled
    return ACK_OUTSIDE, None


def unconverged_leader_rounds(rounds: Sequence[LeaderPollRound]) -> list[LeaderPollRound]:
    """Rounds inside a disagreement that outlasted `CONVERGENCE_BUDGET_S`.

    Pure, so the self-test can hand it a failover-length disagreement and
    require silence, then a stuck one and require it back.

    Any single round of disagreement is expected: a deposed leader keeps naming
    itself until it learns better, and the poll is not synchronised with the
    cluster's own convergence. What is not expected is disagreement that never
    resolves — a cluster that has stopped electing rather than one mid-election.
    """
    runs: list[list[LeaderPollRound]] = []
    current: list[LeaderPollRound] = []
    for r in rounds:
        if r.leaders_disagree:
            current.append(r)
            continue
        runs.append(current)
        current = []
    runs.append(current)
    return [r for run in runs if _run_span_s(run) > CONVERGENCE_BUDGET_S for r in run]


def _run_span_s(run: Sequence[LeaderPollRound]) -> float:
    return 0.0 if len(run) < 2 else run[-1].ts - run[0].ts


def check_invariants(
    history: History,
    db_final: set[int],
    db_total: int | None,
    db_distinct: int | None,
) -> list[Violation]:
    """Check invariants I1-I7 against the complete history.

    Returns a (possibly empty) list of findings: FATAL violations plus any
    labelled WARN observations (currently I5-WARN).
    """
    violations: list[Violation] = []

    # I1: NO_LOST_ACKS
    lost = history.acked_set - db_final
    if lost:
        violations.append(
            Violation(
                "I1",
                f"{len(lost)} acknowledged write(s) are missing from the final DB read",
                sorted(lost),
            )
        )

    # I2: NO_PHANTOM_WRITES
    phantom = db_final - (history.acked_set | history.indeterminate_set)
    if phantom:
        violations.append(
            Violation(
                "I2",
                f"{len(phantom)} value(s) in DB were never attempted or clearly rejected",
                sorted(phantom),
            )
        )

    # I3: NO_DUPLICATES
    if db_total is not None and db_distinct is not None and db_total != db_distinct:
        violations.append(
            Violation(
                "I3",
                f"PRIMARY KEY violation: {db_total} rows, {db_distinct} distinct",
                {"total": db_total, "distinct": db_distinct},
            )
        )

    # I4: SINGLE_LEADER
    split_rounds = unconverged_leader_rounds(history.leader_polls)
    if split_rounds:
        example = split_rounds[0]
        violations.append(
            Violation(
                "I4",
                f"{len(split_rounds)} poll round(s) in a run of leader disagreement "
                f"longer than a failover can explain",
                {"example_ts": example.ts, "example_responses": example.responses},
            )
        )

    # I5: NO_ACKS_DURING_QUORUM_LOSS
    quorum_windows = [fw for fw in history.faults if fw.is_quorum_loss and fw.end_ts > 0]
    bad_acks: list[int] = []
    straddling_acks: list[dict[str, object]] = []
    for op in history.ops:
        if op.outcome != "acked":
            continue
        position, fw = classify_ack_vs_quorum_loss(op, quorum_windows)
        if position == ACK_CONTAINED:
            bad_acks.append(op.value)
        elif position == ACK_STRADDLING and fw is not None:
            straddling_acks.append(
                {
                    "value": op.value,
                    "fault": fw.kind,
                    "ack_span_s": round(op.end_ts - op.start_ts, 3),
                    # How far the ack reached outside the window on each side;
                    # 0.0 means that edge of the ack was inside the window.
                    "began_before_window_s": max(0.0, round(fw.start_ts - op.start_ts, 3)),
                    "ended_after_window_s": max(0.0, round(op.end_ts - fw.end_ts, 3)),
                }
            )
    if bad_acks:
        violations.append(
            Violation(
                "I5",
                f"{len(bad_acks)} write(s) acked while quorum was lost (fencing failure)",
                bad_acks,
            )
        )
    if straddling_acks:
        violations.append(
            Violation(
                "I5-WARN",
                f"{len(straddling_acks)} acked write(s) overlap a quorum-loss window without "
                f"being contained by it — outside contract L2's letter, reported for review",
                straddling_acks[:10],
                severity=SEVERITY_WARN,
            )
        )

    # I6: INTERMEDIATE_READ_CONSISTENCY
    for snap in history.snapshots:
        missing = snap.acked_before - snap.db_contents
        if missing:
            violations.append(
                Violation(
                    "I6",
                    f"Post-'{snap.after_fault}' snapshot missing "
                    f"{len(missing)} previously acked value(s)",
                    sorted(missing),
                )
            )

    # I7: CAUSAL_MONOTONICITY
    # Build map value → op for all acked ops
    acked_ops: dict[int, OpRecord] = {op.value: op for op in history.ops if op.outcome == "acked"}
    causal_violations: list[tuple[int, int]] = []
    for n, op_n in acked_ops.items():
        if n not in db_final:
            continue  # already captured by I1
        # For each M that was fully acked BEFORE N was even started:
        for m, op_m in acked_ops.items():
            if op_m.end_ts < op_n.start_ts and m not in db_final:
                causal_violations.append((m, n))
    if causal_violations:
        violations.append(
            Violation(
                "I7",
                f"{len(causal_violations)} causal ordering violation(s): "
                f"later write present, earlier write absent",
                causal_violations[:10],
            )
        )

    return violations


def _check_log_grep(logs_path: Path, quorum_loss_windows: int = 0) -> list[Violation]:
    """L0/L2-L3: substring presence checks on the collected container log file.

    No regex parsing — just plain ``in`` membership tests, so tracing format
    changes and docker compose log prefixes do not matter. What *does* matter is
    that the substrings still exist in the Rust source; they are held in the
    LOG_* constants and `testing/test_correctness_lite_invariants.py` greps the
    tree to prove a reworded log line breaks a test rather than silently
    disabling a grep here.

    L2 and L3 are absence/conditional greps, which means a missing or truncated
    log would satisfy both by matching nothing. L0 closes that: the corpus must
    be readable and must actually contain pgbattery output, or the whole layer
    is declared invalid rather than passed.
    """
    violations: list[Violation] = []
    try:
        log_text = logs_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return [
            Violation(
                "L0",
                f"Could not read collected log {logs_path} ({exc.__class__.__name__}) — "
                f"L2/L3 could not be evaluated",
                {"path": str(logs_path)},
            )
        ]

    # L0: LOG_CORPUS_VALID
    # Markers are info level, so this also fires when RUST_LOG is turned down
    # far enough to erase them — in which case L2/L3 really are vacuous.
    liveness = [m for m in LOG_LIVENESS_MARKERS if m in log_text]
    if not liveness:
        return [
            Violation(
                "L0",
                f"Collected log ({len(log_text)} bytes) contains none of the expected pgbattery "
                f"startup markers — L2/L3 would match nothing and read as a false pass",
                {"expected_any_of": list(LOG_LIVENESS_MARKERS), "bytes": len(log_text)},
            )
        ]

    # L2: NO_EXPLICIT_SPLIT_BRAIN_SIGNALS
    found = [s for s in LOG_SPLIT_BRAIN_SIGNALS if s in log_text]
    if found:
        violations.append(
            Violation(
                "L2",
                f"Explicit split-brain / fence-failure signal(s) present in logs: {found}",
                found,
            )
        )

    # L3: FENCE_CONFIRMED_AFTER_EMERGENCY
    fence_markers = [m for m in LOG_FENCE_MARKERS if m in log_text]
    fence_resolved = LOG_FENCE_CONFIRMED in log_text or LOG_FENCE_MOOT in log_text
    if "EMERGENCY FENCE" in log_text and not fence_resolved:
        violations.append(
            Violation(
                "L3",
                f"EMERGENCY FENCE fired but no '{LOG_FENCE_CONFIRMED}' confirmation in logs",
                None,
            )
        )
    elif quorum_loss_windows > 0 and not fence_markers and LOG_FENCE_CONFIRMED not in log_text:
        # The scenario removed a majority, yet the log carries no trace of the
        # fence path at all, so L3 checked nothing. That is not automatically a
        # violation — the lease can also be surrendered through the ordinary
        # step-down path — but it must not pass silently either.
        violations.append(
            Violation(
                "L3-WARN",
                f"{quorum_loss_windows} quorum-loss window(s) were injected but the log holds "
                f"no fence trace ({list(LOG_FENCE_MARKERS)}, '{LOG_FENCE_CONFIRMED}') — "
                f"L3 was vacuous",
                {"quorum_loss_windows": quorum_loss_windows},
                severity=SEVERITY_WARN,
            )
        )

    return violations


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

app = typer.Typer(
    add_completion=False,
    help="Correctness Lite: durability + split-brain checker for pgbattery.",
)
console = Console()


@app.command()
def run(
    artifact_dir: str = typer.Option(
        "testing/artifacts/correctness-lite",
        "--artifact-dir",
        envvar="ARTIFACT_DIR",
        help="Directory to write results.json and docker compose logs.",
    ),
    bank_only: bool = typer.Option(
        False,
        "--bank-only",
        help="Skip steps 1-7 and 9-10; run only the bank-transfer step "
        "(useful for sweeping attack modes against the B1-B4 invariants).",
    ),
    attack: str = typer.Option(
        "kill",
        "--attack",
        help="Fault to inject during the bank-transfer step. One of: "
        "kill, partition, freeze, transfer, cascade, quorum_loss, chaos_storm.",
    ),
    transfers: int = typer.Option(
        40,
        "--transfers",
        help="Number of bank-transfer attempts in step 8.",
    ),
    seed: int = typer.Option(
        0,
        "--seed",
        envvar="CORRECTNESS_LITE_SEED",
        help="Seed for every random choice this run makes: which node is paused, "
        "the storm's fault schedule, and the transfer sequence. 0 draws a fresh "
        "one, which is recorded in results.json and printed with the verdict so "
        "the run can be replayed from its artifact.",
    ),
) -> None:
    """Execute the fault schedule, record full history, check all invariants.

    Runs 10 fault injection steps (~360 write attempts, ~3-5 min wall clock).
    Layer 1 (I1-I7): checked against the timestamped operation history and
    background leader polls (0.5s granularity).
    Layer 2 (L0, L2-L3): substring presence checks on the collected container log.
    Layer 3 (B1-B4, C1, M1): workload invariants over the bank, counter and
    monotonic tables.

    Exits 1 if any FATAL finding was produced. WARN observations are printed
    and written to the artifact but do not change the exit code.
    """
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)

    # Drawn once, recorded, and printed. A run whose seed nobody can name is a
    # failure nobody can reproduce, which is the whole point of recording it.
    run_seed = seed or random.randrange(1, 2**31)
    rng = random.Random(run_seed)
    replay = f"CORRECTNESS_LITE_SEED={run_seed} ./testing/correctness_lite.py --attack {attack}"

    console.rule("[bold]CORRECTNESS LITE START")
    console.print(f"seed {run_seed} — replay this run with:\n  {replay}")

    if not wait_cluster_healthy(timeout=120):
        console.print("[bold red]FATAL:[/] cluster not healthy after 120s")
        raise typer.Exit(code=2)

    table_created = False
    for port in GATEWAY_PORTS:
        rc, _, _ = run_cmd(
            f"psql -h localhost -p {port} -U postgres -c "
            f"'CREATE TABLE IF NOT EXISTS jepsen (id INTEGER PRIMARY KEY)'",
            timeout=10,
        )
        if rc == 0:
            table_created = True
            break
    if not table_created:
        console.print("[bold red]FATAL:[/] could not create jepsen table")
        raise typer.Exit(code=2)

    history = History()
    global _LAST_HISTORY
    _LAST_HISTORY = history
    sampler = LeaderSampler(history)
    sampler.start()

    t0 = time.time()
    try:
        if bank_only:
            console.print(f"[bold yellow]--bank-only:[/] running step 8 with attack={attack} only")
            step_bank_transfer(history, console, rng, attack=attack, num_transfers=transfers)
        else:
            step_baseline(history, console)
            step_kill_leader(history, console)
            step_pause_random(history, console, rng)
            step_network_partition_leader(history, console)
            step_majority_loss(history, console)
            step_full_restart(history, console)
            step_final_steady(history, console)
            step_bank_transfer(history, console, rng, attack=attack, num_transfers=transfers)
            step_concurrent_contention(history, console)
            step_monotonic_read_session(history, console)
    finally:
        sampler.stop()

    elapsed = time.time() - t0

    console.print(f"\nFault schedule complete in {elapsed:.0f}s. Waiting for cluster recovery…")
    wait_cluster_healthy(timeout=60)
    time.sleep(3)

    if bank_only:
        # In bank-only mode the `jepsen` table is empty, so the regular
        # post-recovery read would falsely return None. We only care about
        # B1-B4, which query `bank_accounts` and the ledger directly. Provide
        # empty stubs for the jepsen-derived layers so the summary table still
        # renders cleanly and we skip irrelevant invariant checks.
        db_final: set[int] = set()
        db_total: int | None = 0
        db_distinct: int | None = 0
        findings: list[Violation] = []
        findings.extend(check_bank_invariants())
        logs_path = artifact_path / "docker-compose.log"
        run_cmd(f"docker compose logs --no-color > {logs_path} 2>&1", timeout=30)
        console.print(f"Logs written to {logs_path}")
    else:
        console.print("Reading final DB state…")
        maybe_db_final = read_all_from_db()
        if maybe_db_final is None:
            console.print("[bold red]FATAL:[/] could not read from database after recovery")
            log_path = artifact_path / "docker-compose.log"
            run_cmd(f"docker compose logs --no-color > {log_path} 2>&1", timeout=30)
            raise typer.Exit(code=2)
        db_final = maybe_db_final

        db_total, db_distinct = check_duplicates()

        findings = check_invariants(history, db_final, db_total, db_distinct)

        # ── Layer 2: log grep checks (L0, L2-L3) ────────────────────────────
        logs_path = artifact_path / "docker-compose.log"
        run_cmd(f"docker compose logs --no-color > {logs_path} 2>&1", timeout=30)
        console.print(f"Logs written to {logs_path}")
        findings.extend(
            _check_log_grep(
                logs_path,
                quorum_loss_windows=sum(1 for fw in history.faults if fw.is_quorum_loss),
            )
        )

        # ── Layer 3: bank transfer invariants (B1-B4) ───────────────────────
        findings.extend(check_bank_invariants())

        # ── Layer 4: concurrent same-row contention invariant (C1) ──────────
        findings.extend(check_contention_invariant())

        # ── Layer 5: monotonic-read session invariant (M1) ──────────────────
        findings.extend(check_monotonic_read_invariant())

    violations = [f for f in findings if f.severity == SEVERITY_FATAL]
    observations = [f for f in findings if f.severity == SEVERITY_WARN]

    # ── Summary table ────────────────────────────────────────────────────────
    console.print()
    t = Table(title="Correctness Lite Results", show_lines=False)
    t.add_column("Metric", style="bold")
    t.add_column("Value", justify="right")

    t.add_row("Attempted", str(history.total_attempted))
    t.add_row("Acked", str(len(history.acked_set)))
    t.add_row("Errored", str(len(history.errored_set)))
    t.add_row("Indeterminate", str(len(history.indeterminate_set)))
    t.add_row("In DB (final)", str(len(db_final)))
    t.add_row("Indeterminate→committed", str(len(db_final & history.indeterminate_set)))
    t.add_row("Unclassified errors (insert/transfer)", str(history.unclassified_attempts))
    t.add_row("Transfers attempted", str(len(history.transfers)))
    t.add_row(
        "Transfers acked / indeterminate",
        f"{sum(1 for tr in history.transfers if tr.outcome == 'acked')} / "
        f"{sum(1 for tr in history.transfers if tr.outcome == 'indeterminate')}",
    )
    t.add_row("Leader poll rounds", str(len(history.leader_polls)))
    t.add_row(
        "Leader-disagreement rounds",
        str(sum(1 for r in history.leader_polls if r.leaders_disagree)),
    )
    t.add_row("Fault windows", str(len(history.faults)))
    t.add_row("Intermediate snapshots", str(len(history.snapshots)))
    t.add_row("Wall clock", f"{elapsed:.0f}s")

    inv_ids: tuple[str, ...]
    warn_ids: tuple[str, ...]
    if bank_only:
        # Only the bank invariants were actually evaluated; anything else
        # would be a misleading green check.
        inv_ids = ("B1", "B2", "B3", "B4")
        warn_ids = ()
    else:
        inv_ids = (
            "I1",
            "I2",
            "I3",
            "I4",
            "I5",
            "I6",
            "I7",
            "L0",
            "L2",
            "L3",
            "B1",
            "B2",
            "B3",
            "B4",
            "C1",
            "M1",
        )
        warn_ids = ("I5-WARN", "L3-WARN")
    for inv_id in inv_ids:
        v = next((vv for vv in violations if vv.invariant == inv_id), None)
        if v is None:
            t.add_row(inv_id, "[green]PASS ✓[/]")
        else:
            t.add_row(inv_id, f"[red]FAIL ✗  {v.message}[/]")
    for warn_id in warn_ids:
        o = next((oo for oo in observations if oo.invariant == warn_id), None)
        if o is None:
            t.add_row(warn_id, "[green]none[/]")
        else:
            t.add_row(warn_id, f"[yellow]WARN  {o.message}[/]")

    verdict = "PASS" if not violations else "FAIL"
    verdict_style = "[bold green]PASS[/]" if not violations else "[bold red]FAIL[/]"
    t.add_row("Verdict", verdict_style)
    console.print(t)
    console.print()

    if violations:
        console.print("[bold red]INVARIANT VIOLATIONS DETAIL (FATAL):[/]")
        for v in violations:
            console.print(f"  [{v.invariant}] {v.message}")
            if v.evidence is not None:
                console.print(f"       evidence: {v.evidence}")

    if observations:
        console.print(
            "[bold yellow]OBSERVATIONS (WARN — reported, does not affect the verdict):[/]"
        )
        for o in observations:
            console.print(f"  [{o.invariant}] {o.message}")
            if o.evidence is not None:
                console.print(f"       evidence: {o.evidence}")

    # ── Artifact dump ────────────────────────────────────────────────────────
    results = {
        "verdict": verdict,
        "seed": run_seed,
        "attack": attack,
        "replay": replay,
        "attempted": history.total_attempted,
        "acked": len(history.acked_set),
        "errored": len(history.errored_set),
        "indeterminate": len(history.indeterminate_set),
        "in_db_final": len(db_final),
        "unclassified_errors": history.unclassified_attempts,
        "elapsed_seconds": round(elapsed, 1),
        "violations": [
            {"invariant": v.invariant, "message": v.message, "evidence": str(v.evidence)}
            for v in violations
        ],
        "observations": [
            {"invariant": o.invariant, "message": o.message, "evidence": str(o.evidence)}
            for o in observations
        ],
        "transfers": [
            {
                "transfer_id": tr.transfer_id,
                "from_id": tr.from_id,
                "to_id": tr.to_id,
                "amount": tr.amount,
                "outcome": tr.outcome,
                "reason": tr.reason,
                "port": tr.port,
            }
            for tr in history.transfers
        ],
        "leader_poll_rounds": len(history.leader_polls),
        "leader_disagreement_rounds": sum(1 for r in history.leader_polls if r.leaders_disagree),
        "unconverged_leader_rounds": len(unconverged_leader_rounds(history.leader_polls)),
        "fault_windows": [
            {
                "kind": fw.kind,
                "detail": fw.detail,
                "duration_s": round(fw.end_ts - fw.start_ts, 3) if fw.end_ts else None,
            }
            for fw in history.faults
        ],
        "intermediate_snapshots": [
            {
                "after": s.after_fault,
                "acked_before": len(s.acked_before),
                "in_db": len(s.db_contents),
                "missing": sorted(s.acked_before - s.db_contents),
            }
            for s in history.snapshots
        ],
        "acked_set": sorted(history.acked_set),
        "errored_set": sorted(history.errored_set),
        "indeterminate_set": sorted(history.indeterminate_set),
        "db_final": sorted(db_final),
    }
    results_path = artifact_path / "results.json"
    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    console.print(f"Results written to {results_path}")

    # A fault that failed to inject makes this run prove nothing: the invariants
    # would be checked against a window where nothing happened. Exit 2 (infra),
    # distinct from 1 (a real violation), and only after the artifacts are on
    # disk so the failure is debuggable.
    fault_errors = fault_thread_errors()
    if fault_errors:
        console.print(
            "\n[bold red]FATAL:[/] faults failed to inject, so the invariants "
            "above were checked against windows that may contain no fault:\n  "
            + "\n  ".join(fault_errors)
        )
        console.print(f"Replay this run with:\n  {replay}")
        raise typer.Exit(code=2)

    if violations:
        console.print(f"\nReplay this run with:\n  {replay}")
    raise typer.Exit(code=0 if not violations else 1)


if __name__ == "__main__":
    app()
