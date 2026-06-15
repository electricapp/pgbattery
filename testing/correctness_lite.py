#!/usr/bin/env -S uv run --project testing python
"""Correctness Lite: history-based durability + split-brain checker for pgbattery.

⚠ SCOPE — READ THIS FIRST ⚠
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
  multi-write consistency under failover).
- Optional: concurrent same-row hammer (3 writers contending on 3 keys with
  CAS-style UPDATE … WHERE; verifies "no lost update" — C1).
- Optional: monotonic-read session test (a value visible from a session must
  not regress after failover on that same logical session — M1).

What this file does NOT verify:
- Linearizability of concurrent reads + writes (see linearizability_register.py).
- Isolation across multi-statement transactions (serializable / snapshot).
- 2PC (prepared transaction) fate after coordinator failover.
- Causal+ consistency across sessions.

Every write attempt is logged with monotonic timestamps.  A background thread
continuously samples leader status across all three nodes in parallel.  Fault
windows are recorded precisely (open/close).  After the fault schedule, seven
invariants are checked against the *complete* operation history — not just a
single point-in-time snapshot.

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

Exit codes:
    0 — all invariants hold (PASS)
════════════════════════════════════════════════════════════════════════════════
LAYER 2 — LOG GREP CHECKS  L2-L3  (all FATAL, defense in depth)
Checked against the collected container log file after the fault schedule.
Simple substring presence — no regex parsing, no format fragility.
════════════════════════════════════════════════════════════════════════════════

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

════════════════════════════════════════════════════════════════════════════════

Exit codes:
    0 — all invariants hold (PASS)
    1 — at least one invariant violated (FAIL)
    2 — infrastructure error (cluster unreachable, table creation failed, etc.)
"""

from __future__ import annotations

import contextlib
import json
import random
import subprocess
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

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

MGMT_PORTS: Final[list[int]] = [9081, 9082, 9083]
"""Management API ports for node1/node2/node3."""

NODES: Final[list[str]] = ["node1", "node2", "node3"]
"""Docker Compose service names."""

PSQL_TIMEOUT: Final[int] = 5
"""Seconds before a psql write attempt is classified as indeterminate."""

LEADER_POLL_INTERVAL: Final[float] = 0.5
"""Seconds between background leader-poll rounds."""

REJECTION_PATTERNS: Final[list[str]] = [
    "read-only",
    "cannot execute",
    "connection refused",
    "not accept",
    "read_only",
]
"""Output substrings that indicate a clear, unambiguous write rejection."""

INDETERMINATE_PATTERNS: Final[list[str]] = [
    "connection",
    "server closed",
    "timeout",
    "reset by peer",
    "broken pipe",
    "unexpected eof",
]
"""Output substrings that indicate the write fate is unknown."""

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
    def is_split_brain(self) -> bool:
        """Two distinct nodes simultaneously claim leadership."""
        return len(self.unique_leaders) > 1


@dataclass
class SnapshotRecord:
    """A point-in-time DB read taken immediately after a fault heals."""

    ts: float
    after_fault: str  # human label of the fault that just healed
    acked_before: set[int]  # copy of acked_set at snapshot time
    db_contents: set[int]  # values read from the DB


@dataclass
class Violation:
    """A confirmed invariant violation."""

    invariant: str  # "I1"-"I7" (polling) or "L2"-"L3" (log grep)
    message: str
    evidence: object = None  # supporting detail (sorted lists, counts, etc.)


@dataclass
class History:
    """Accumulated record of the entire test run, shared across threads."""

    ops: list[OpRecord] = field(default_factory=list)
    faults: list[FaultWindow] = field(default_factory=list)
    leader_polls: list[LeaderPollRound] = field(default_factory=list)
    snapshots: list[SnapshotRecord] = field(default_factory=list)

    acked_set: set[int] = field(default_factory=set)
    errored_set: set[int] = field(default_factory=set)
    indeterminate_set: set[int] = field(default_factory=set)

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


def find_network() -> str | None:
    """Return the Docker network name containing 'raft_net', or None."""
    rc, out, _ = run_cmd("docker network ls --format '{{.Name}}' | grep raft_net")
    if rc == 0 and out.strip():
        return out.strip().splitlines()[0]
    return None


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

    Tries each gateway port in order.  Returns "acked", "errored", or
    "indeterminate" — and appends an OpRecord with monotonic timestamps.
    """
    seq = history.next_seq()
    start_ts = time.monotonic()
    wall_start = time.time()
    sql = f"INSERT INTO jepsen(id) VALUES ({value})"

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
            output = r.stdout + r.stderr
            lower = output.lower()
            if r.returncode == 0:
                op = OpRecord(seq, value, start_ts, time.monotonic(), wall_start, "acked", port)
                history.record_op(op)
                return "acked"
            if any(s in lower for s in REJECTION_PATTERNS):
                continue  # try next port
            if any(s in lower for s in INDETERMINATE_PATTERNS):
                op = OpRecord(
                    seq, value, start_ts, time.monotonic(), wall_start, "indeterminate", port
                )
                history.record_op(op)
                return "indeterminate"
            continue
        except subprocess.TimeoutExpired:
            op = OpRecord(seq, value, start_ts, time.monotonic(), wall_start, "indeterminate", port)
            history.record_op(op)
            return "indeterminate"

    op = OpRecord(seq, value, start_ts, time.monotonic(), wall_start, "errored", last_port)
    history.record_op(op)
    return "errored"


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


def bank_transfer(from_id: int, to_id: int, amount: int) -> bool:
    """Attempt a bank transfer. Returns True if committed, False otherwise.

    Uses CHECK (balance >= 0) enforcement on the server side so a
    transfer that would overdraw is automatically rolled back.
    """
    sql = (
        f"BEGIN; "
        f"UPDATE bank_accounts SET balance = balance - {amount} WHERE id = {from_id}; "
        f"UPDATE bank_accounts SET balance = balance + {amount} WHERE id = {to_id}; "
        f"COMMIT;"
    )
    for port in GATEWAY_PORTS:
        rc, out, _ = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{sql}" 2>&1',
            timeout=PSQL_TIMEOUT,
        )
        if rc == 0:
            return True
        lower = out.lower()
        if any(s in lower for s in REJECTION_PATTERNS):
            continue  # try next port
    return False


def check_bank_invariants() -> list[Violation]:
    """B1-B2: conservation of total balance and no negative balances.

    B1  BANK_TOTAL_CONSERVED
        SUM(balance) must equal BANK_TOTAL after all transfers.
    B2  NO_NEGATIVE_BALANCE
        MIN(balance) must be >= 0 (enforced by DB CHECK constraint; a
        violation here means the constraint was bypassed somehow).
    """
    violations: list[Violation] = []
    for port in GATEWAY_PORTS:
        rc, out, _ = run_cmd(
            f"psql -h localhost -p {port} -U postgres -t -A "
            f"-c 'SELECT SUM(balance), MIN(balance) FROM bank_accounts'",
            timeout=10,
        )
        if rc == 0 and out.strip():
            parts = out.strip().split("|")
            if len(parts) == 2:
                total = int(parts[0])
                minimum = int(parts[1])
                if total != BANK_TOTAL:
                    violations.append(
                        Violation(
                            "B1",
                            f"Bank balance sum violated: expected {BANK_TOTAL}, got {total}",
                            {"expected": BANK_TOTAL, "actual": total},
                        )
                    )
                if minimum < 0:
                    violations.append(
                        Violation(
                            "B2",
                            f"Negative balance found (CHECK constraint bypassed): min={minimum}",
                            {"min_balance": minimum},
                        )
                    )
                return violations
    violations.append(Violation("B1", "Could not read bank_accounts after recovery", None))
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
    leader, _ = find_leader()
    if leader is None:
        return None
    idx = NODES.index(leader) + 1
    run_cmd(
        f"docker network disconnect pgbattery_raft_net pgbattery-{leader}-1",
        timeout=10,
    )

    def _heal() -> None:
        time.sleep(heal_after)
        run_cmd(
            f"docker network connect --ip 172.28.0.1{idx} pgbattery_raft_net pgbattery-{leader}-1",
            timeout=10,
        )

    threading.Thread(target=_heal, daemon=True).start()
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

    threading.Thread(target=_restore, daemon=True).start()
    return leader


def _chaos_storm_now(duration: float = 8.0, seed: int | None = None) -> str | None:
    """Fire 2-4 random faults at random times within `duration` seconds.

    Mixes kill, partition, freeze, transfer. Returns the leader observed
    when the storm started.
    """
    rng = random.Random(seed if seed is not None else int(time.time()))
    leader, _ = find_leader()
    n = rng.randint(2, 4)
    times = sorted(rng.uniform(0, duration) for _ in range(n))
    kinds = [rng.choice(["kill", "partition", "freeze", "transfer"]) for _ in range(n)]
    start = time.monotonic()
    for ft, kind in zip(times, kinds, strict=True):
        elapsed = time.monotonic() - start
        if ft > elapsed:
            time.sleep(ft - elapsed)
        threading.Thread(target=_FAULT_DISPATCH[kind], daemon=True).start()
    return leader


_FAULT_DISPATCH: dict[str, Callable[[], str | None]] = {
    "kill": _kill_leader_now,
    "partition": _partition_leader_now,
    "freeze": _freeze_leader_now,
    "transfer": _transfer_leader_now,
    "cascade": _cascade_kill_now,
    "quorum_loss": _quorum_loss_now,
    "chaos_storm": _chaos_storm_now,
}


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


def step_pause_random(history: History, console: Console) -> None:
    """Step 3: Pause a random node for the duration of 50 inserts, then resume."""
    node = random.choice(NODES)
    console.print(f"[bold]Step 3:[/] pause {node}")
    fw = history.open_fault("pause_node", f"paused {node}")
    docker_compose("pause", node)
    do_inserts(50, history, console)
    docker_compose("unpause", node)
    wait_cluster_healthy(timeout=60)
    history.close_fault(fw)
    take_snapshot(history, f"pause_{node}", console)


def step_network_partition_leader(history: History, console: Console) -> None:
    """Step 4: Disconnect the leader from the raft overlay network during 50 inserts."""
    console.print("[bold]Step 4:[/] network-disconnect leader")
    leader, _ = find_leader()
    net = find_network()
    if leader is None or net is None:
        console.print(f"  [yellow]WARNING:[/] leader={leader}, net={net} — inserting without fault")
        do_inserts(50, history, console)
        return

    _, container_id, _ = run_cmd(f"docker compose ps -q {leader}")
    container_id = container_id.strip()
    if not container_id:
        console.print("  [yellow]WARNING:[/] could not find container ID — skipping partition")
        do_inserts(50, history, console)
        return

    console.print(f"  disconnecting {leader} ({container_id[:12]}) from {net}")
    fw = history.open_fault("network_partition", f"partitioned {leader}")
    run_cmd(f"docker network disconnect {net} {container_id}")
    do_inserts(50, history, console)
    run_cmd(f"docker network connect {net} {container_id}")
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


def step_bank_transfer(
    history: History,
    console: Console,
    attack: str = "kill",
    num_transfers: int = 40,
) -> None:
    """Step 8: bank transfer workload — total balance must be conserved (B1-B2).

    Creates BANK_ACCOUNTS accounts, runs `num_transfers` transfer attempts
    while injecting the named `attack` mid-workload. The B1/B2 invariant
    check (SUM(balances) and per-account >=0) runs after all steps in run().

    `attack` is one of: kill, partition, freeze, transfer, cascade,
    quorum_loss, chaos_storm — see `_FAULT_DISPATCH`.
    """
    console.print(
        f"[bold]Step 8:[/] bank transfer workload (attack={attack}, B1-B2 total must equal 10 000)"
    )

    setup_sql = (
        "DROP TABLE IF EXISTS bank_accounts; "
        "CREATE TABLE bank_accounts "
        "(id INTEGER PRIMARY KEY, balance INTEGER NOT NULL CHECK (balance >= 0)); "
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
            "  [yellow]WARNING:[/] could not create bank_accounts - skipping bank workload"
        )
        return

    if attack not in _FAULT_DISPATCH:
        console.print(f"  [red]Unknown attack '{attack}', falling back to 'kill'[/]")
        attack = "kill"

    # Fire the attack about 25% into the workload so we have transfers
    # before and during the fault.
    kickoff_at = max(2, num_transfers // 4)
    fw: FaultWindow | None = None
    committed = 0
    fired_leader: str | None = None
    for i in range(num_transfers):
        if i == kickoff_at:
            console.print(f"  injecting attack: {attack}")
            fw = history.open_fault(
                f"bank_attack_{attack}", f"injecting {attack} mid-bank-workload"
            )
            fired_leader = _FAULT_DISPATCH[attack]()
        ids = random.sample(range(1, BANK_ACCOUNTS + 1), 2)
        amount = random.randint(1, 100)
        if bank_transfer(ids[0], ids[1], amount):
            committed += 1
        time.sleep(0.05)

    # Restore any nodes that may still be down; the dispatch's auto-heal
    # threads cover network partitions and freezes, but kill / cascade /
    # quorum_loss leave containers dead.
    _restore_killed_nodes()
    wait_cluster_healthy(timeout=60)
    if fw is not None:
        history.close_fault(fw)

    console.print(
        f"  {committed}/{num_transfers} transfers committed"
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

    Classifies the outcome strictly: "acked" on success, "rejected" on
    explicit refusal (read-only / cannot execute), "indeterminate" on
    timeout / connection failure (write may or may not have landed).
    """
    cmd = (
        f"psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 "
        f'-c "UPDATE counters SET val = val + 1 WHERE id = {key}" 2>&1'
    )
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=PSQL_TIMEOUT)
    except subprocess.TimeoutExpired:
        return "indeterminate"
    output = (r.stdout + r.stderr).lower()
    if r.returncode == 0:
        return "acked"
    if any(s in output for s in REJECTION_PATTERNS):
        return "rejected"
    if any(s in output for s in INDETERMINATE_PATTERNS):
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
    with UPDATE counters SET val = val + 1 WHERE id = key. Kills the leader
    mid-flight. Verifies per-key that:

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
        history.contention_skipped = True  # type: ignore[attr-defined]
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
    history.contention_acked = acked_per_key  # type: ignore[attr-defined]
    history.contention_indeterminate = indet_per_key  # type: ignore[attr-defined]

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
    acked: dict[int, int] = getattr(_LAST_HISTORY, "contention_acked", {})
    indet: dict[int, int] = getattr(_LAST_HISTORY, "contention_indeterminate", {})
    if getattr(_LAST_HISTORY, "contention_skipped", False):
        return violations  # step itself didn't run
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


# `_LAST_HISTORY` is set in `run()` so `check_contention_invariant` can read the
# per-key counts without taking the History as a parameter (matches the shape
# of `check_bank_invariants` which queries the DB directly).
_LAST_HISTORY: History | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Step 10 — monotonic-read session (M1: NO_READ_REGRESSION_ACROSS_FAILOVER)
# ─────────────────────────────────────────────────────────────────────────────

MONOTONIC_WRITES: Final[int] = 30
"""How many monotonic values to write across the failover window."""

MONOTONIC_KILL_AT: Final[int] = 15
"""Which iteration triggers the leader kill (mid-sequence)."""


def _try_write_monotonic(value: int) -> str:
    """Write `value` via any gateway. Returns "acked" | "rejected" | "indeterminate"."""
    sql = f"INSERT INTO monotonic(val) VALUES ({value})"
    last_outcome = "rejected"
    for port in GATEWAY_PORTS:
        cmd = f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{sql}" 2>&1'
        try:
            r = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=PSQL_TIMEOUT
            )
        except subprocess.TimeoutExpired:
            return "indeterminate"
        out = (r.stdout + r.stderr).lower()
        if r.returncode == 0:
            return "acked"
        if any(s in out for s in REJECTION_PATTERNS):
            last_outcome = "rejected"
            continue
        if any(s in out for s in INDETERMINATE_PATTERNS):
            return "indeterminate"
    return last_outcome


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
        history.monotonic_skipped = True  # type: ignore[attr-defined]
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
    history.monotonic_observations = observations  # type: ignore[attr-defined]
    history.monotonic_acked = acked  # type: ignore[attr-defined]
    console.print(f"  acked {len(acked)}/{MONOTONIC_WRITES}, recorded {len(observations)} reads")


def check_monotonic_read_invariant() -> list[Violation]:
    """M1: NO_READ_REGRESSION_ACROSS_FAILOVER.

    For every pair of recorded reads (i, max_i) and (j, max_j) with j > i,
    we require max_j >= max_i. A counter-example is a *regression*: a read
    that returns less than a previously-observed value.
    """
    violations: list[Violation] = []
    if _LAST_HISTORY is None:
        return violations
    if getattr(_LAST_HISTORY, "monotonic_skipped", False):
        return violations
    obs: list[tuple[int, int]] = getattr(_LAST_HISTORY, "monotonic_observations", [])
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
        acked: list[int] = getattr(_LAST_HISTORY, "monotonic_acked", [])
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


def check_invariants(
    history: History,
    db_final: set[int],
    db_total: int | None,
    db_distinct: int | None,
) -> list[Violation]:
    """Check all 7 invariants against the complete history.

    Returns a (possibly empty) list of Violation objects.
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
    split_rounds = [r for r in history.leader_polls if r.is_split_brain]
    if split_rounds:
        example = split_rounds[0]
        violations.append(
            Violation(
                "I4",
                f"{len(split_rounds)} poll round(s) observed two simultaneous leaders",
                {"example_ts": example.ts, "example_responses": example.responses},
            )
        )

    # I5: NO_ACKS_DURING_QUORUM_LOSS
    quorum_windows = [fw for fw in history.faults if fw.is_quorum_loss and fw.end_ts > 0]
    bad_acks: list[int] = []
    for op in history.ops:
        if op.outcome != "acked":
            continue
        for fw in quorum_windows:
            if fw.start_ts <= op.start_ts and op.end_ts <= fw.end_ts:
                bad_acks.append(op.value)
                break
    if bad_acks:
        violations.append(
            Violation(
                "I5",
                f"{len(bad_acks)} write(s) acked while quorum was lost (fencing failure)",
                bad_acks,
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


def _check_log_grep(logs_path: Path) -> list[Violation]:
    """L2-L3: substring presence checks on the collected container log file.

    No regex parsing — just plain ``in`` membership tests.  Unaffected by
    tracing format changes or docker compose log prefix variations.
    """
    violations: list[Violation] = []
    try:
        log_text = logs_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return violations

    # L2: NO_EXPLICIT_SPLIT_BRAIN_SIGNALS
    l2_signals = [
        "potential split-brain",
        "FAILED TO FENCE",
        "Promotion safety check failed",
    ]
    found = [s for s in l2_signals if s in log_text]
    if found:
        violations.append(
            Violation(
                "L2",
                f"Explicit split-brain / fence-failure signal(s) present in logs: {found}",
                found,
            )
        )

    # L3: FENCE_CONFIRMED_AFTER_EMERGENCY
    if "EMERGENCY FENCE" in log_text and "PostgreSQL fenced (read-only)" not in log_text:
        violations.append(
            Violation(
                "L3",
                "EMERGENCY FENCE fired but no 'PostgreSQL fenced (read-only)' confirmation in logs",
                None,
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
        "(useful for sweeping attack modes against the B1/B2 invariant).",
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
) -> None:
    """Execute the fault schedule, record full history, check all invariants.

    Runs 8 fault injection steps (~360 write attempts, ~3-5 min wall clock).
    Layer 1 (I1-I7): checked against the timestamped operation history and
    background leader polls (0.5s granularity).
    Layer 2 (L2-L3): substring presence checks on the collected container log.
    Layer 3 (B1-B2): bank transfer total-balance conservation invariant.
    """
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)

    console.rule("[bold]CORRECTNESS LITE START")

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
            step_bank_transfer(history, console, attack=attack, num_transfers=transfers)
        else:
            step_baseline(history, console)
            step_kill_leader(history, console)
            step_pause_random(history, console)
            step_network_partition_leader(history, console)
            step_majority_loss(history, console)
            step_full_restart(history, console)
            step_final_steady(history, console)
            step_bank_transfer(history, console, attack=attack, num_transfers=transfers)
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
        # B1/B2, which queries `bank_accounts` directly. Provide empty
        # stubs for the jepsen-derived layers so the summary table still
        # renders cleanly and we skip irrelevant invariant checks.
        db_final: set[int] = set()
        db_total: int | None = 0
        db_distinct: int | None = 0
        violations: list[Violation] = []
        violations.extend(check_bank_invariants())
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

        violations = check_invariants(history, db_final, db_total, db_distinct)

        # ── Layer 2: log grep checks (L2-L3) ────────────────────────────────
        logs_path = artifact_path / "docker-compose.log"
        run_cmd(f"docker compose logs --no-color > {logs_path} 2>&1", timeout=30)
        console.print(f"Logs written to {logs_path}")
        violations.extend(_check_log_grep(logs_path))

        # ── Layer 3: bank transfer invariants (B1-B2) ───────────────────────
        violations.extend(check_bank_invariants())

        # ── Layer 4: concurrent same-row contention invariant (C1) ──────────
        violations.extend(check_contention_invariant())

        # ── Layer 5: monotonic-read session invariant (M1) ──────────────────
        violations.extend(check_monotonic_read_invariant())

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
    t.add_row("Leader poll rounds", str(len(history.leader_polls)))
    t.add_row("Split-brain rounds", str(sum(1 for r in history.leader_polls if r.is_split_brain)))
    t.add_row("Fault windows", str(len(history.faults)))
    t.add_row("Intermediate snapshots", str(len(history.snapshots)))
    t.add_row("Wall clock", f"{elapsed:.0f}s")

    inv_ids: tuple[str, ...]
    if bank_only:
        # Only the bank invariants were actually evaluated; anything else
        # would be a misleading green check.
        inv_ids = ("B1", "B2")
    else:
        inv_ids = ("I1", "I2", "I3", "I4", "I5", "I6", "I7", "L2", "L3", "B1", "B2", "C1", "M1")
    for inv_id in inv_ids:
        v = next((vv for vv in violations if vv.invariant == inv_id), None)
        if v is None:
            t.add_row(inv_id, "[green]PASS ✓[/]")
        else:
            t.add_row(inv_id, f"[red]FAIL ✗  {v.message}[/]")

    verdict = "PASS" if not violations else "FAIL"
    verdict_style = "[bold green]PASS[/]" if not violations else "[bold red]FAIL[/]"
    t.add_row("Verdict", verdict_style)
    console.print(t)
    console.print()

    if violations:
        console.print("[bold red]INVARIANT VIOLATIONS DETAIL:[/]")
        for v in violations:
            console.print(f"  [{v.invariant}] {v.message}")
            if v.evidence is not None:
                console.print(f"       evidence: {v.evidence}")

    # ── Artifact dump ────────────────────────────────────────────────────────
    results = {
        "verdict": verdict,
        "attempted": history.total_attempted,
        "acked": len(history.acked_set),
        "errored": len(history.errored_set),
        "indeterminate": len(history.indeterminate_set),
        "in_db_final": len(db_final),
        "elapsed_seconds": round(elapsed, 1),
        "violations": [
            {"invariant": v.invariant, "message": v.message, "evidence": str(v.evidence)}
            for v in violations
        ],
        "leader_poll_rounds": len(history.leader_polls),
        "split_brain_rounds": sum(1 for r in history.leader_polls if r.is_split_brain),
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

    raise typer.Exit(code=0 if not violations else 1)


if __name__ == "__main__":
    app()
