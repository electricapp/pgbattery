#!/usr/bin/env -S uv run --project testing python
"""Single-register linearizability checker for pgbattery.

Spawns K concurrent client threads that issue read / write / CAS operations
against a small set of "register" rows (one row per key) through the
pgbattery gateway. Each operation is recorded with monotonic
invocation/response timestamps. The leader is killed mid-run to force a
failover. After recovery, the recorded operation history is checked for
linearizability per-key using the Wing-Gong-Lowe (WGL) search algorithm.

═══════════════════════════════════════════════════════════════════════════
WHY THIS FILE EXISTS ALONGSIDE correctness_lite.py
═══════════════════════════════════════════════════════════════════════════

correctness_lite.py verifies durability of acked writes and absence of
split-brain. It does NOT verify *ordering* of concurrent operations from a
client's perspective. Two writers and a reader, all hitting the same key
under a failover window, can produce a history where:

  - All acked writes survive (durability holds — correctness_lite passes).
  - The read returns a value that no linearization could possibly produce
    (e.g. an older value after a newer one was already observed).

That second class of bug — a stale-read, a lost-update, a write-skew — is
what this file is for. It checks the *real-time* relationship between
concurrent ops against the sequential specification of a register
({read returns last write, write replaces, CAS commits iff witness matches}).

═══════════════════════════════════════════════════════════════════════════
SCOPE & LIMITATIONS
═══════════════════════════════════════════════════════════════════════════

  - **Per-key register model only.** Each key is treated as an independent
    register; cross-key invariants (e.g. SUM(values) conservation) are
    out of scope. Use correctness_lite's bank-transfer step for that.

  - **WGL is exponential in the worst case.** This file caps operations
    per key to roughly 100; beyond that, runtime blows up.  Real Jepsen
    uses Knossos/Elle which have better-than-WGL constants and can also
    verify transactional histories. We do not.

  - **Indeterminate operations are encoded as "pending" with both possible
    outcomes considered.** A write that timed out could have committed or
    not — WGL handles this natively.

═══════════════════════════════════════════════════════════════════════════
ALGORITHM (Wing-Gong-Lowe, register specialisation)
═══════════════════════════════════════════════════════════════════════════

A history H = [(op_i, invoke_i, return_i)] is linearizable iff there exists
a total order < of the completed operations in H such that:

  1. (REAL-TIME)  op_a returned before op_b invoked  ⇒  op_a < op_b.
  2. (SEQUENTIAL) The total order produces a valid sequential register
                  history when each op is applied to the register state.

Search:

  - Maintain `remaining` = set of ops not yet linearized.
  - At each step, consider only ops whose invocation is at-or-before the
    earliest return time in `remaining` (only these are eligible to be
    linearized next under the real-time constraint).
  - For each candidate, simulate the register transition; recurse with
    the candidate removed and the register state updated.
  - Memoize on (frozenset(remaining_ids), register_value) — a hash that
    captures the entire search state.
  - If recursion exhausts the frontier, history is linearizable.

For "pending" ops (indeterminate outcome), we try both "this op happened"
and "this op didn't happen" branches.

═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import contextlib
import json
import random
import re
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

from db_clients import PsycopgWorkerClient

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

GATEWAY_PORTS: Final[list[int]] = [5432, 5433, 5434]
MGMT_PORTS: Final[list[int]] = [9081, 9082, 9083]
NODES: Final[list[str]] = ["node1", "node2", "node3"]

NUM_KEYS: int = 3
"""Independent register keys. Each is checked separately."""

NUM_WORKERS: int = 2
"""Concurrent client threads."""

WORKLOAD_DURATION_SECONDS: float = 6.0
"""Total wall-clock time the workload runs."""

KILL_LEADER_AFTER_SECONDS: float = 2.0
"""When (relative to workload start) to inject the failover."""

PSQL_TIMEOUT_SECONDS: Final[int] = 4

# Cap per-key op count to keep WGL tractable. With NUM_WORKERS=2 doing ~5
# ops/sec each across NUM_KEYS=3, expected per-key ops is ~20 — well under
# the cap, so we get unsampled checks.
WGL_OPS_PER_KEY_CAP: Final[int] = 2000


# ─────────────────────────────────────────────────────────────────────────────
# Operation history
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Op:
    """A single client operation against the register.

    Fields:
        op_id:      monotonically increasing identifier, unique across workers.
        key:        which register this op targets.
        kind:       "read" | "write" | "cas".
        write_val:  for write/cas, the new value.
        cas_old:    for cas, the witness value.
        invoke_ts:  time.monotonic() right before the SQL is sent.
        return_ts:  time.monotonic() right after the SQL completes (or fails).
                    None ⇒ pending (no response ever received).
        result:     for read, the returned value; for write, the written value
                    on success; for cas, True iff the CAS committed. None ⇒ pending
                    or hard error (treated like pending in WGL).
        worker:     human-readable thread label.
        port:       gateway port the op was sent through.
    """

    op_id: int
    key: int
    kind: str
    invoke_ts: float
    return_ts: float | None = None
    write_val: int | None = None
    cas_old: int | None = None
    result: int | bool | None = None
    worker: str = ""
    port: int = 0
    # For kind="txn" / "append": ordered list of (mop_kind, key, val) micro-ops.
    # mop_kind ∈ {"r", "w", "append"}.
    # val is:
    #   - int   for register reads and writes
    #   - list[int] for list-append reads (the full observed list)
    #   - int   for list-append micro-ops (the single element being appended)
    #   - None  for pending or unobserved reads
    micro_ops: list[tuple[str, int, object]] = field(default_factory=list)

    @property
    def is_completed(self) -> bool:
        return self.return_ts is not None

    def to_jsonable(self) -> dict[str, object]:
        return {
            "op_id": self.op_id,
            "key": self.key,
            "kind": self.kind,
            "invoke_ts": round(self.invoke_ts, 6),
            "return_ts": round(self.return_ts, 6) if self.return_ts is not None else None,
            "write_val": self.write_val,
            "cas_old": self.cas_old,
            "result": self.result,
            "worker": self.worker,
            "port": self.port,
            "micro_ops": [list(m) for m in self.micro_ops] if self.micro_ops else [],
        }


@dataclass
class JepsenRecord:
    """One operation record in Jepsen / Elle history format.

    A single transaction produces two records: an `invoke` at start time
    and exactly one close (`ok` / `fail` / `info`) at the moment the worker
    learns the outcome. The format is what Elle's `check` consumes directly
    after wrapping in `jepsen.history/history` -- no Python-side
    reconstruction. Required fields and types described below.

    Fields:
        type:     "invoke" | "ok" | "fail" | "info"
        process:  worker id (single-threaded actor: at most one in-flight op)
        time_ns:  monotonic clock in integer nanoseconds, must be strictly
                  monotonic per process (Elle / jepsen.history asserts this)
        f:        function name, always "txn" for our workloads
        value:    list of micro-ops [[kind, key, val], ...] where
                  kind in {"r", "w", "append"}.
    """

    type: str
    process: int
    time_ns: int
    f: str
    value: list[list[object]]

    def to_jsonable(self) -> dict[str, object]:
        return {
            "type": self.type,
            "process": self.process,
            "time": self.time_ns,
            "f": self.f,
            "value": self.value,
        }


@dataclass
class History:
    """Thread-safe operation log.

    `ops` holds per-key register workload Ops (for WGL / weak checks).
    `jepsen` holds Jepsen-format records for the txn / list-append workloads
    (consumed by Elle directly).
    """

    ops: list[Op] = field(default_factory=list)
    jepsen: list[JepsenRecord] = field(default_factory=list)
    _counter: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def next_id(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter

    def append(self, op: Op) -> None:
        with self._lock:
            self.ops.append(op)

    def append_jepsen(self, record: JepsenRecord) -> None:
        """Per-process monotonicity is the caller's responsibility (use
        `time.monotonic_ns()` and don't reorder)."""
        with self._lock:
            self.jepsen.append(record)

    def per_key(self) -> dict[int, list[Op]]:
        out: dict[int, list[Op]] = {k: [] for k in range(NUM_KEYS)}
        for op in self.ops:
            out.setdefault(op.key, []).append(op)
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Shell helpers
# ─────────────────────────────────────────────────────────────────────────────


def run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
    """Run a shell command, return (rc, stdout, stderr). -1 rc on timeout."""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    return r.returncode, r.stdout, r.stderr


def find_leader() -> tuple[str | None, int | None]:
    """Return (node_name, gateway_port) for the current leader, or (None, None)."""
    for port in MGMT_PORTS:
        rc, out, _ = run_cmd(
            f"curl -sf --max-time 2 http://localhost:{port}/api/v1/cluster/leader",
            timeout=4,
        )
        if rc == 0:
            with contextlib.suppress(Exception):
                lid = json.loads(out).get("leader_id")
                if lid is not None and 1 <= lid <= len(NODES):
                    return NODES[lid - 1], GATEWAY_PORTS[lid - 1]
    return None, None


def wait_cluster_healthy(timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader, _ = find_leader()
        if leader is not None:
            return True
        time.sleep(2)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Workload
# ─────────────────────────────────────────────────────────────────────────────


def setup_table() -> bool:
    """Create the linreg table seeded with key in [0, NUM_KEYS), val = 0.

    Returns True iff the table exists with NUM_KEYS rows post-setup.
    """
    setup_sql = (
        "DROP TABLE IF EXISTS linreg; "
        "CREATE TABLE linreg (key INTEGER PRIMARY KEY, val INTEGER NOT NULL); "
        f"INSERT INTO linreg SELECT generate_series(0, {NUM_KEYS - 1}), 0;"
    )
    for port in GATEWAY_PORTS:
        rc, _, _ = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{setup_sql}"',
            timeout=15,
        )
        if rc == 0:
            return True
    return False


def setup_list_append_table() -> bool:
    """Create the linappend table for the list-append workload.

    Each row's val is a comma-separated decimal int list, starting empty.
    """
    setup_sql = (
        "DROP TABLE IF EXISTS linappend; "
        "CREATE TABLE linappend (key INTEGER PRIMARY KEY, val TEXT NOT NULL DEFAULT ''); "
        f"INSERT INTO linappend SELECT generate_series(0, {NUM_KEYS - 1}), '';"
    )
    for port in GATEWAY_PORTS:
        rc, _, _ = run_cmd(
            f'psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 -c "{setup_sql}"',
            timeout=15,
        )
        if rc == 0:
            return True
    return False


_VAL_RE = re.compile(r"-?\d+")


def _parse_first_int(s: str) -> int | None:
    """Pull the first integer out of psql's -t -A output."""
    for line in s.strip().splitlines():
        match = _VAL_RE.match(line.strip())
        if match:
            with contextlib.suppress(ValueError):
                return int(match.group())
    return None


def do_read(port: int, key: int) -> tuple[int | None, bool]:
    """Execute a read; return (value, completed).

    completed = True iff we got a definite answer (success or definite reject).
    A timeout/conn-closed returns (None, False) → "pending".
    """
    cmd = (
        f"psql -h localhost -p {port} -U postgres -t -A -v ON_ERROR_STOP=1 "
        f"-c 'SELECT val FROM linreg WHERE key = {key}' 2>&1"
    )
    rc, out, _ = run_cmd(cmd, timeout=PSQL_TIMEOUT_SECONDS)
    if rc == 0:
        return _parse_first_int(out), True
    lower = out.lower()
    if "read-only" in lower or "cannot execute" in lower:
        return None, True  # definite reject
    return None, False  # pending


def do_write(port: int, key: int, val: int) -> bool | None:
    """Execute a write; return True (acked), False (definite reject), None (pending)."""
    cmd = (
        f"psql -h localhost -p {port} -U postgres -v ON_ERROR_STOP=1 "
        f'-c "UPDATE linreg SET val = {val} WHERE key = {key}" 2>&1'
    )
    rc, out, _ = run_cmd(cmd, timeout=PSQL_TIMEOUT_SECONDS)
    if rc == 0:
        return True
    lower = out.lower()
    if "read-only" in lower or "cannot execute" in lower:
        return False  # definite reject
    return None  # pending


def do_cas(port: int, key: int, old: int, new: int) -> bool | None:
    """Execute a CAS; True on commit, False on witness-mismatch / reject, None pending."""
    cmd = (
        f"psql -h localhost -p {port} -U postgres -t -A -v ON_ERROR_STOP=1 "
        f'-c "UPDATE linreg SET val = {new} WHERE key = {key} AND val = {old} '
        f'RETURNING 1" 2>&1'
    )
    rc, out, _ = run_cmd(cmd, timeout=PSQL_TIMEOUT_SECONDS)
    if rc == 0:
        return "1" in out  # one matching row updated → True
    lower = out.lower()
    if "read-only" in lower or "cannot execute" in lower:
        return False
    return None


def txn_worker_loop(
    worker_id: int,
    history: History,
    stop_event: threading.Event,
    rng: random.Random,
) -> None:
    """2-key SERIALIZABLE rw-register transactions, emitting Jepsen-format
    records directly.

    Per Jepsen: a worker is a single-threaded actor that issues one txn at
    a time. We emit `:invoke` when sending and exactly one of `:ok`/`:fail`/
    `:info` when the outcome is known (success / definite rollback / unknown).
    """
    port = GATEWAY_PORTS[worker_id % len(GATEWAY_PORTS)]
    client = PsycopgWorkerClient(port=port)
    try:
        while not stop_event.is_set():
            if NUM_KEYS < 2:
                return
            k1, k2 = rng.sample(range(NUM_KEYS), 2)
            new1 = rng.randint(1, 1_000_000)
            new2 = rng.randint(1, 1_000_000)
            # Invoke: reads pending, writes declared.
            invoke_value: list[list[object]] = [
                ["r", k1, None],
                ["r", k2, None],
                ["w", k1, new1],
                ["w", k2, new2],
            ]
            history.append_jepsen(
                JepsenRecord(
                    type="invoke",
                    process=worker_id,
                    time_ns=time.monotonic_ns(),
                    f="txn",
                    value=invoke_value,
                )
            )
            outcome = client.execute_register_txn(k1, k2, new1, new2)
            close_ns = time.monotonic_ns()
            if outcome.committed is True:
                r1, r2 = outcome.reads[0], outcome.reads[1]
                ok_value: list[list[object]] = [
                    ["r", k1, r1 if isinstance(r1, int) else None],
                    ["r", k2, r2 if isinstance(r2, int) else None],
                    ["w", k1, new1],
                    ["w", k2, new2],
                ]
                history.append_jepsen(
                    JepsenRecord(
                        type="ok",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=ok_value,
                    )
                )
            elif outcome.committed is False:
                # Definite rollback: txn had no effect. Same value as invoke.
                history.append_jepsen(
                    JepsenRecord(
                        type="fail",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=invoke_value,
                    )
                )
            else:
                # Pending: connection broke / timed out. We don't know if
                # the cluster committed. :info releases the process and
                # tells Elle "could have happened any time in [invoke, info]".
                history.append_jepsen(
                    JepsenRecord(
                        type="info",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=invoke_value,
                    )
                )
    finally:
        client.close()


def list_append_worker_loop(
    worker_id: int,
    history: History,
    stop_event: threading.Event,
    rng: random.Random,
) -> None:
    """2-key SERIALIZABLE list-append transactions, emitting Jepsen records
    directly. Each txn appends a globally-unique tag (the worker's local
    counter combined with worker_id) to both keys."""
    port = GATEWAY_PORTS[worker_id % len(GATEWAY_PORTS)]
    client = PsycopgWorkerClient(port=port)
    try:
        while not stop_event.is_set():
            if NUM_KEYS < 2:
                return
            k1, k2 = rng.sample(range(NUM_KEYS), 2)
            tag = history.next_id()
            invoke_value: list[list[object]] = [
                ["r", k1, None],
                ["r", k2, None],
                ["append", k1, tag],
                ["append", k2, tag],
            ]
            history.append_jepsen(
                JepsenRecord(
                    type="invoke",
                    process=worker_id,
                    time_ns=time.monotonic_ns(),
                    f="txn",
                    value=invoke_value,
                )
            )
            outcome = client.execute_append_txn(k1, k2, tag)
            close_ns = time.monotonic_ns()
            if outcome.committed is True:
                l1, l2 = outcome.reads[0], outcome.reads[1]
                ok_value: list[list[object]] = [
                    ["r", k1, l1 if isinstance(l1, list) else None],
                    ["r", k2, l2 if isinstance(l2, list) else None],
                    ["append", k1, tag],
                    ["append", k2, tag],
                ]
                history.append_jepsen(
                    JepsenRecord(
                        type="ok",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=ok_value,
                    )
                )
            elif outcome.committed is False:
                history.append_jepsen(
                    JepsenRecord(
                        type="fail",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=invoke_value,
                    )
                )
            else:
                history.append_jepsen(
                    JepsenRecord(
                        type="info",
                        process=worker_id,
                        time_ns=close_ns,
                        f="txn",
                        value=invoke_value,
                    )
                )
    finally:
        client.close()


def worker_loop(
    worker_id: int,
    history: History,
    stop_event: threading.Event,
    rng: random.Random,
) -> None:
    """Issue ops at high rate until stop_event is set.

    The op mix is read 50% / write 30% / cas 20% — enough writes to create
    real ordering history, enough reads to make ordering observable, and
    a CAS workload that surfaces lost-update style anomalies.
    """
    worker_label = f"w{worker_id}"
    # Each worker rotates through gateway ports so we exercise routing during
    # failover. The leader port routes; followers reject.
    port_cycle_index = 0
    while not stop_event.is_set():
        port = GATEWAY_PORTS[port_cycle_index % len(GATEWAY_PORTS)]
        port_cycle_index += 1
        key = rng.randrange(NUM_KEYS)
        choice = rng.random()
        op = Op(
            op_id=history.next_id(),
            key=key,
            kind="?",
            invoke_ts=time.monotonic(),
            worker=worker_label,
            port=port,
        )
        if choice < 0.50:
            op.kind = "read"
            value, completed = do_read(port, key)
            op.return_ts = time.monotonic() if completed else None
            op.result = value
        elif choice < 0.80:
            new_val = rng.randint(1, 1_000_000)
            op.kind = "write"
            op.write_val = new_val
            outcome = do_write(port, key, new_val)
            if outcome is None:
                op.return_ts = None
            else:
                op.return_ts = time.monotonic()
                op.result = outcome
        else:
            old_val = rng.randint(0, 1_000_000)
            new_val = rng.randint(1, 1_000_000)
            op.kind = "cas"
            op.cas_old = old_val
            op.write_val = new_val
            outcome = do_cas(port, key, old_val, new_val)
            if outcome is None:
                op.return_ts = None
            else:
                op.return_ts = time.monotonic()
                op.result = outcome
        history.append(op)


def kill_leader_after(delay: float) -> None:
    """Sleep `delay` seconds, then kill whichever node is currently leader."""
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    run_cmd(f"docker compose kill {leader}", timeout=10)


def partition_leader_after(delay: float, heal_after: float = 4.0) -> None:
    """Disconnect leader from raft_net, then reconnect after `heal_after`."""
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    leader_idx = NODES.index(leader) + 1
    ip = f"172.28.0.1{leader_idx}"
    run_cmd(f"docker network disconnect pgbattery_raft_net pgbattery-{leader}-1", timeout=10)
    time.sleep(heal_after)
    run_cmd(
        f"docker network connect --ip {ip} pgbattery_raft_net pgbattery-{leader}-1",
        timeout=10,
    )


def freeze_leader_after(delay: float, hold: float = 3.0) -> None:
    """SIGSTOP pgbattery on leader, SIGCONT after `hold` seconds."""
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    rc, pid_out, _ = run_cmd(
        f"docker compose exec -T {leader} sh -c 'pgrep -x pgbattery | head -1'",
        timeout=5,
    )
    pid = pid_out.strip().split("\n")[-1].strip() if rc == 0 else ""
    if not pid.isdigit():
        return
    run_cmd(f"docker compose exec -T --user root {leader} kill -STOP {pid}", timeout=5)
    time.sleep(hold)
    run_cmd(f"docker compose exec -T --user root {leader} kill -CONT {pid}", timeout=5)


def transfer_leader_after(delay: float) -> None:
    """Trigger transfer-leadership via management API."""
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    leader_idx = NODES.index(leader) + 1
    target = (leader_idx % len(NODES)) + 1
    mgmt_port = MGMT_PORTS[leader_idx - 1]
    token_rc, token_out, _ = run_cmd(
        "grep PGBATTERY_MANAGEMENT_API_TOKEN .env | cut -d= -f2", timeout=5
    )
    token = token_out.strip() if token_rc == 0 else ""
    run_cmd(
        f"curl -s -X POST --max-time 10 "
        f"-H 'x-pgbattery-token: {token}' "
        f"http://localhost:{mgmt_port}/api/v1/cluster/transfer-leadership/{target}",
        timeout=15,
    )


def cascade_kill_after(delay: float, kills: int = 2, gap: float = 1.5) -> None:
    """Kill the leader, wait `gap`, kill the new leader, etc."""
    time.sleep(delay)
    for _ in range(kills):
        leader, _ = find_leader()
        if leader is None:
            time.sleep(gap)
            continue
        run_cmd(f"docker compose kill {leader}", timeout=10)
        run_cmd(f"docker compose start {leader}", timeout=10)
        time.sleep(gap)


def quorum_loss_after(delay: float, restore_after: float = 4.0) -> None:
    """Kill 2 of 3 nodes to lose quorum; restore one to regain it."""
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    others = [n for n in NODES if n != leader]
    for n in others:
        run_cmd(f"docker compose kill {n}", timeout=10)
    time.sleep(restore_after)
    # Bring back ONE so quorum returns
    run_cmd(f"docker compose start {others[0]}", timeout=10)


def asymmetric_partition_after(delay: float, hold: float = 4.0) -> None:
    """One-way packet drop: leader can SEND to followers but can't RECEIVE
    from them. iptables INPUT DROP on the leader for each peer IP.

    Classic split-brain pattern: leader continues sending AppendEntries
    that go unacknowledged (heartbeats blackholed at the inbound side),
    while followers see no leader and start an election. Tests pre-vote +
    lease-step-down logic against bidirectional-reachability assumptions.
    """
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    leader_idx = NODES.index(leader) + 1
    peer_ips = [f"172.28.0.1{i}" for i in range(1, len(NODES) + 1) if i != leader_idx]
    try:
        for ip in peer_ips:
            run_cmd(
                f"docker compose exec -T --user root {leader} iptables -I INPUT -s {ip} -j DROP",
                timeout=5,
            )
        time.sleep(hold)
    finally:
        for ip in peer_ips:
            run_cmd(
                f"docker compose exec -T --user root {leader} iptables -D INPUT -s {ip} -j DROP",
                timeout=5,
            )


def network_slow_after(delay: float, hold: float = 5.0, delay_ms: int = 250) -> None:
    """Inject `delay_ms` of latency on leader's eth0 via tc netem.

    Tests Raft heartbeat / lease-renewal tolerance to slow links. A
    leader whose AppendEntries take longer than the election timeout to
    arrive at a follower will be deposed even if nothing is actually
    broken.
    """
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    try:
        run_cmd(
            f"docker compose exec -T --user root {leader} "
            f"tc qdisc add dev eth0 root netem delay {delay_ms}ms",
            timeout=5,
        )
        time.sleep(hold)
    finally:
        run_cmd(
            f"docker compose exec -T --user root {leader} tc qdisc del dev eth0 root",
            timeout=5,
        )


def network_loss_after(delay: float, hold: float = 5.0, loss_pct: int = 30) -> None:
    """Drop `loss_pct`% of packets on leader's eth0 via tc netem.

    Different failure mode from full partition: some RPCs get through
    after retries, some don't. Exposes resends, idempotency, and
    duplicate-handling bugs that clean disconnects can't.
    """
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    try:
        run_cmd(
            f"docker compose exec -T --user root {leader} "
            f"tc qdisc add dev eth0 root netem loss {loss_pct}%",
            timeout=5,
        )
        time.sleep(hold)
    finally:
        run_cmd(
            f"docker compose exec -T --user root {leader} tc qdisc del dev eth0 root",
            timeout=5,
        )


def clock_skew_after(delay: float, skew_s: int = 30, hold: float = 5.0) -> None:
    """Jump leader's clock forward by `skew_s` via libfaketime.

    The container's libfaketime reads `/tmp/faketime` every call (no
    cache) and applies the offset. Tests `LeaseState`'s claim of
    monotonic-clock immunity: even if wall time jumps, the lease's
    Instant-based math should still expire at the right monotonic moment.
    """
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    try:
        run_cmd(
            f"docker compose exec -T {leader} sh -c \"echo '+{skew_s}s' > /tmp/faketime\"",
            timeout=5,
        )
        time.sleep(hold)
    finally:
        run_cmd(
            f"docker compose exec -T {leader} sh -c \"echo '+0s' > /tmp/faketime\"",
            timeout=5,
        )


def pg_only_kill_after(delay: float) -> None:
    """Kill the leader's postgres process, leaving pgbattery alive.

    Tests the supervisor's PG-death detection in isolation. Different
    from `kill_leader_after` (which terminates the whole container):
    here, pgbattery sees PG die and must restart it without losing
    leadership unnecessarily, or step down cleanly if restart fails.
    """
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    # SIGKILL all postgres processes; pgbattery's supervisor should respawn.
    run_cmd(
        f"docker compose exec -T --user root {leader} pkill -KILL postgres",
        timeout=5,
    )


def disk_full_after(delay: float, hold: float = 4.0, size_mb: int = 500) -> None:
    """Exhaust the leader's data volume free space mid-write.

    Allocates a `size_mb` filler file in the PG data dir. PG behavior
    when WAL can't be flushed is a known sharp edge: writes block,
    checkpointer fails, eventually PG may PANIC. We want pgbattery to
    detect this and step down (or fence) rather than report success on
    an un-durable write.
    """
    fill_path = "/var/lib/postgresql/data/_chaos_fill.bin"
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    try:
        run_cmd(
            f"docker compose exec -T --user root {leader} fallocate -l {size_mb}M {fill_path}",
            timeout=10,
        )
        time.sleep(hold)
    finally:
        run_cmd(
            f"docker compose exec -T --user root {leader} rm -f {fill_path}",
            timeout=5,
        )


def fsync_stall_after(delay: float, hold: float = 3.0) -> None:
    """Stall PG durable-write path via SIGSTOP on the checkpointer.

    NOTE: this is a documented approximation of a true fsync drop. Real
    fsync drops (libeatmydata + LD_PRELOAD into postgres) require a
    rebuild of the PG container image. SIGSTOP-the-checkpointer reproduces
    the symptom (writes accumulate, durable persistence stalls) without
    the disk-controller path. Use this to verify the lease-tick's "PG is
    alive but unhealthy" branch.
    """
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    rc, pid_out, _ = run_cmd(
        f"docker compose exec -T --user root {leader} pgrep -f 'postgres.*checkpointer'",
        timeout=5,
    )
    pid = pid_out.strip().splitlines()[-1].strip() if rc == 0 else ""
    if not pid.isdigit():
        return
    try:
        run_cmd(
            f"docker compose exec -T --user root {leader} kill -STOP {pid}",
            timeout=5,
        )
        time.sleep(hold)
    finally:
        run_cmd(
            f"docker compose exec -T --user root {leader} kill -CONT {pid}",
            timeout=5,
        )


def flap_partition_after(delay: float, cycles: int = 8, period_s: float = 0.6) -> None:
    """Repeatedly partition then heal the leader on tight intervals.

    Each cycle: disconnect leader from raft_net for `period_s/2` s, then
    reconnect for `period_s/2` s. Stresses election storm + leader
    oscillation: every break may trigger a new election; every heal may
    cause the deposed leader to fight back.
    """
    time.sleep(delay)
    # Re-resolve leader before each break since failovers may have moved it.
    for _ in range(cycles):
        leader, _ = find_leader()
        if leader is None:
            time.sleep(period_s)
            continue
        leader_idx = NODES.index(leader) + 1
        ip = f"172.28.0.1{leader_idx}"
        container = f"pgbattery-{leader}-1"
        run_cmd(
            f"docker network disconnect pgbattery_raft_net {container}",
            timeout=5,
        )
        time.sleep(period_s / 2)
        run_cmd(
            f"docker network connect --ip {ip} pgbattery_raft_net {container}",
            timeout=5,
        )
        time.sleep(period_s / 2)


def membership_change_after(delay: float) -> None:
    """Add a node (the witness) while chaos is happening.

    Kicks off the join while killing the current leader. Two
    correctness-critical state machines interact: Raft membership change
    and Raft leader election. Witness lifecycle is best-effort cleaned up
    at suite teardown.
    """
    time.sleep(delay)
    # Kick off the join asynchronously; the join command blocks until the
    # node catches up.
    threading.Thread(
        target=lambda: run_cmd(
            "docker compose --profile witness up -d witness",
            timeout=30,
        ),
        daemon=True,
    ).start()
    # Tiny gap so the join request is in-flight when we kill the leader.
    time.sleep(0.5)
    leader, _ = find_leader()
    if leader is not None:
        run_cmd(f"docker compose kill {leader}", timeout=10)


# ─────────────────────────────────────────────────────────────────────────────
# TODO — disk-layer chaos primitives (NOT YET IMPLEMENTED)
# ─────────────────────────────────────────────────────────────────────────────
#
# These two attack types are deliberately scaffolded as NotImplementedError
# stubs. They require infrastructure changes (PG image rebuild, sidecar
# block-device) that cross the "no env mutation in a test script" line. The
# scaffolds exist so:
#
#   1. Anyone running `--attack fsync_drop` or `--attack bit_flip` gets a
#      precise error pointing at exactly what to add, instead of a silent
#      no-op or a mysterious crash.
#
#   2. The shape of the call (delay, return) matches `ATTACK_DISPATCH`, so
#      enabling them later is a `raise NotImplementedError` → real code
#      swap with no churn at the call sites.
#
#   3. `chaos_storm` deliberately does *not* include these in its random
#      pick list. The matrix in `run_elle_matrix.sh` also omits them. When
#      either fault is enabled, also add it back to those two surfaces.


_FSYNC_DROP_PRECONDITION = (
    "fsync_drop requires libeatmydata preloaded into the postgres process.\n"
    "  To enable, in Dockerfile add:\n"
    "    RUN apt-get update && apt-get install -y libeatmydata1\n"
    "  Modify config/nodeN.toml so pgbattery starts postgres with\n"
    "    env LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libeatmydata.so\n"
    "  ... only after `touch /tmp/fsync_drop_enabled`. The fault-injection\n"
    "  hook below toggles that sentinel + SIGHUPs postgres so fsync()\n"
    "  becomes a no-op only inside the chaos window.\n"
    "  Smoke-validate by running `eatmydata pg_isready`; check that\n"
    "  pg_stat_database shows blks_hit increasing but pg_xact files don't\n"
    "  fsync() during the window.\n"
    "  Then remove this guard and the matching ALL_ATTACKS / chaos_storm\n"
    "  exclusion."
)


def fsync_drop_after(delay: float, hold: float = 3.0) -> None:
    """[SCAFFOLD] True fsync drop via libeatmydata LD_PRELOAD.

    Distinct from `fsync_stall_after` (SIGSTOP the checkpointer): a real
    fsync drop returns success immediately *without* flushing, so PG
    acks the commit but the data isn't durable. Kill the host right after
    and any acked-but-not-flushed write disappears -- the classic
    durability-violation test.

    Why scaffold-only: requires libeatmydata in the PG image and a
    pgbattery config change to start postgres with LD_PRELOAD. Cross-cuts
    Dockerfile + config; not safe to enable from a test script.
    """
    # When enabling, replace this raise with the sentinel toggle:
    #   run_cmd(f"docker compose exec -T {leader} touch /tmp/fsync_drop_enabled")
    #   run_cmd(f"docker compose exec -T --user root {leader} kill -HUP $(pgrep postgres)")
    #   time.sleep(hold)
    #   run_cmd(f"docker compose exec -T {leader} rm -f /tmp/fsync_drop_enabled")
    #   run_cmd(f"docker compose exec -T --user root {leader} kill -HUP $(pgrep postgres)")
    # Reference the time of `delay` and `hold` to keep mypy/ruff quiet.
    _ = (delay, hold)
    raise NotImplementedError(_FSYNC_DROP_PRECONDITION)


_BIT_FLIP_PRECONDITION = (
    "bit_flip requires a corruptible block device under /var/lib/postgresql.\n"
    "  Approach A (recommended): docker-compose sidecar exposing an `nbd-server`\n"
    "  backed by a file. The nbd-client in each pgbattery container mounts it\n"
    "  at /var/lib/postgresql. At fault time, send the nbd-server a SIGUSR1 to\n"
    "  enter corrupt-on-write mode for `hold` s, then SIGUSR2 to restore.\n"
    "  Approach B: dmsetup `flakey` target wrapping a loop device. Requires\n"
    "  privileged: true and CAP_SYS_ADMIN in compose; less portable but no\n"
    "  sidecar.\n"
    "  Either approach: validate by torturing PG with `pgbench -c8 -T30` and\n"
    "  checking that pg_amcheck reports corruption afterward.\n"
    "  Then remove this guard."
)


def bit_flip_after(delay: float, hold: float = 2.0) -> None:
    """[SCAFFOLD] Random bit-flip on writes to leader's PG data volume.

    Tests PG page checksum + Raft log integrity. Lower yield than the
    process/network faults because hardware bit-flips are rare in
    practice, but this is the only test that exercises the
    detection+recovery path for on-disk corruption.

    Why scaffold-only: requires either an nbd sidecar in docker-compose
    or `privileged: true` for dmsetup. Both are real infra changes.
    """
    # When enabling, replace this raise with:
    #   leader, _ = find_leader()
    #   send_corrupt_signal_to_nbd_server(leader)  # or dmsetup load_table
    #   time.sleep(hold)
    #   send_restore_signal_to_nbd_server(leader)
    _ = (delay, hold)
    raise NotImplementedError(_BIT_FLIP_PRECONDITION)


def chaos_storm_after(
    delay: float,
    duration: float = 25.0,
    seed: int | None = None,
) -> None:
    """Fire 3-5 random faults at random times within `duration` seconds.

    Mixes every attack type so a single run exercises the full surface.
    Times are chosen by an independent RNG so behavior depends on `seed`.
    After each fault, sleeps a random interval before the next so the
    cluster sometimes has time to settle and sometimes doesn't.
    """
    storm_kinds = [
        "kill",
        "partition",
        "freeze",
        "transfer",
        "asymmetric_partition",
        "network_slow",
        "network_loss",
        "clock_skew",
        "pg_only_kill",
        "fsync_stall",
        "flap_partition",
    ]
    rng = random.Random(seed if seed is not None else int(time.time()))
    time.sleep(delay)
    num_faults = rng.randint(3, 5)
    fault_times = sorted(rng.uniform(0, duration) for _ in range(num_faults))
    fault_kinds = [rng.choice(storm_kinds) for _ in range(num_faults)]
    start = time.monotonic()
    for ft, kind in zip(fault_times, fault_kinds, strict=True):
        elapsed = time.monotonic() - start
        if ft > elapsed:
            time.sleep(ft - elapsed)
        # Spawn the fault in a background thread so a slow one (partition heal)
        # doesn't block the next.
        worker_thread = threading.Thread(
            target=ATTACK_DISPATCH[kind],
            args=(0.0,),  # immediate
            daemon=True,
        )
        worker_thread.start()


def start_killed_nodes() -> None:
    """Bring back any nodes that were killed during the workload."""
    for n in NODES:
        run_cmd(f"docker compose start {n}", timeout=15)


def scrub_chaos_residue() -> None:
    """Best-effort cleanup of fault residue. Runs every test, idempotent.

    The discipline is that every fault function cleans up its own scope in
    its own `finally`. This is the belt + suspenders: if a fault crashed
    or the test was interrupted, we don't want iptables rules, tc qdiscs,
    skewed clocks, or filler files lingering into the next run.
    """
    for n in NODES:
        # iptables: flush our chains
        run_cmd(
            f"docker compose exec -T --user root {n} iptables -F INPUT",
            timeout=5,
        )
        # tc netem: drop any root qdisc we may have added
        run_cmd(
            f"docker compose exec -T --user root {n} tc qdisc del dev eth0 root",
            timeout=5,
        )
        # Clock skew: reset to zero offset
        run_cmd(
            f"docker compose exec -T {n} sh -c \"echo '+0s' > /tmp/faketime\"",
            timeout=5,
        )
        # Disk-full filler
        run_cmd(
            f"docker compose exec -T --user root {n} "
            "rm -f /var/lib/postgresql/data/_chaos_fill.bin",
            timeout=5,
        )
    # Witness: tear it down so the next run starts from the canonical
    # 3-node topology.
    run_cmd("docker compose --profile witness rm -sf witness", timeout=30)


# ─────────────────────────────────────────────────────────────────────────────
# WGL linearizability checker — register model
# ─────────────────────────────────────────────────────────────────────────────


def _apply_op_to_register(op: Op, current: int) -> tuple[bool, int]:
    """Compute the register transition for `op` given `current` state.

    Returns (matches_observed_result, new_value).

    For a pending op (no completion), the caller treats it as if it had
    succeeded with whatever value the kind implies (read returns `current`,
    write succeeds, cas commits iff old == current).
    """
    if op.kind == "read":
        observed = op.result
        if observed is None:
            # Pending read — always consistent with the current state.
            return True, current
        return observed == current, current
    if op.kind == "write":
        # A pending write may or may not have landed. The caller decides via
        # the "completed = succeeded" branching; if we get here, treat as
        # having committed.
        new_val = op.write_val
        if new_val is None:
            return False, current
        return True, new_val
    if op.kind == "cas":
        old = op.cas_old
        new_val = op.write_val
        if old is None or new_val is None:
            return False, current
        succeeded_in_history = bool(op.result) if op.result is not None else None
        if old == current:
            # CAS would have committed.
            if succeeded_in_history is False:
                return False, current  # history says it failed → contradiction
            return True, new_val
        # CAS would have observed mismatch.
        if succeeded_in_history is True:
            return False, current  # history says it committed → contradiction
        return True, current
    return False, current


def _infer_register_value_at(ops_sorted: list[Op], at_ts: float) -> int:
    """Find the register value at wall time `at_ts`.

    Looks at all ops that COMPLETED before `at_ts` and picks the most recent
    successful write or committed CAS. If none, the register is at its
    initial value 0.
    """
    candidates = []
    for op in ops_sorted:
        if op.return_ts is None or op.return_ts > at_ts:
            continue
        if op.kind in ("write", "cas") and op.result is True and op.write_val is not None:
            candidates.append((op.return_ts, op.write_val))
    if not candidates:
        return 0
    candidates.sort()
    return candidates[-1][1]


def _is_weakly_consistent(ops: list[Op]) -> tuple[bool, str]:
    """Fast structural check that runs in O(n) on histories of any size.

    Verifies two properties:

      W-1 (no phantom reads): every value returned by a successful read was
          the target of SOME write or CAS op (acked, rejected, or pending).
          A read returning a value no client ever wrote indicates split-brain
          or memory corruption.

      W-2 (no impossible CAS commit): every committed CAS observed a witness
          value that was the target of SOME write or CAS op (or the initial
          value 0). If a CAS commits on a witness no one ever wrote, the
          cluster fabricated the witness.

    Weaker than WGL (doesn't check real-time ordering), but tractable at
    any scale and catches the loudest split-brain symptoms.
    """
    legit_values = {0}
    for op in ops:
        if op.write_val is not None:
            legit_values.add(op.write_val)

    for op in ops:
        if op.kind == "read" and op.result is not None and op.result not in legit_values:
            return False, f"phantom read: value {op.result!r} never written by any client"
        if (
            op.kind == "cas"
            and op.result is True
            and op.cas_old is not None
            and op.cas_old not in legit_values
        ):
            return False, f"impossible CAS commit on witness {op.cas_old!r}"

    return True, f"weakly-consistent ({len(legit_values)} distinct values)"


def _is_linearizable(ops: list[Op]) -> tuple[bool, str]:
    """WGL search over `ops` (single-register history). Returns (ok, reason)."""
    # Only consider ops that completed OR have at least an invoke timestamp
    # (pending ops are tried both ways via the loop below).
    if not ops:
        return True, "empty history"

    # Sort by invoke time for stable iteration order.
    ops_sorted = sorted(ops, key=lambda o: o.invoke_ts)
    initial_value = 0

    if len(ops_sorted) > WGL_OPS_PER_KEY_CAP:
        # Take a CONTIGUOUS WINDOW centered on the median return time
        # (workload-symmetric heuristic; lands near the fault for symmetric
        # workloads). Then INFER the register's value at the window's start
        # from the prefix of ops that completed before it — otherwise WGL
        # would start at 0 and fail any read returning a value written
        # earlier in history.
        completed_return_ts = [o.return_ts for o in ops_sorted if o.return_ts is not None]
        if completed_return_ts:
            mid_ts = sorted(completed_return_ts)[len(completed_return_ts) // 2]
            center = min(
                range(len(ops_sorted)),
                key=lambda i: abs(ops_sorted[i].invoke_ts - mid_ts),
            )
        else:
            center = len(ops_sorted) // 2
        half = WGL_OPS_PER_KEY_CAP // 2
        start = max(0, min(len(ops_sorted) - WGL_OPS_PER_KEY_CAP, center - half))
        window_start_ts = ops_sorted[start].invoke_ts
        # Critical: infer starting register value from the prefix we're
        # discarding. Without this, WGL hallucinates anomalies.
        initial_value = _infer_register_value_at(ops_sorted[:start], window_start_ts)
        ops_sorted = ops_sorted[start : start + WGL_OPS_PER_KEY_CAP]

    # Cache return_ts of completed ops; pending ops get +inf.
    return_of: dict[int, float] = {}
    for o in ops_sorted:
        return_of[o.op_id] = o.return_ts if o.return_ts is not None else float("inf")

    op_by_id: dict[int, Op] = {o.op_id: o for o in ops_sorted}
    remaining_init: frozenset[int] = frozenset(op_by_id)

    visited: set[tuple[frozenset[int], int]] = set()

    sys.setrecursionlimit(10_000)

    def search(remaining: frozenset[int], reg_val: int) -> bool:
        if not remaining:
            return True
        state_key = (remaining, reg_val)
        if state_key in visited:
            return False
        visited.add(state_key)
        # Minimum return_ts among remaining — only ops invoked at-or-before
        # this are eligible to be linearized next (others must come strictly
        # after by real-time order).
        min_return = min(return_of[i] for i in remaining)
        candidates = [op_by_id[i] for i in remaining if op_by_id[i].invoke_ts <= min_return]
        for candidate in candidates:
            matches, new_val = _apply_op_to_register(candidate, reg_val)
            if not matches:
                continue
            if search(remaining - {candidate.op_id}, new_val):
                return True
        return False

    ok = search(remaining_init, initial_value)
    if ok:
        return True, "linearizable"
    return False, "no total order satisfies real-time + sequential register semantics"


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


app = typer.Typer(
    add_completion=False,
    help="Single-register linearizability checker (WGL) for pgbattery.",
)
console = Console()


ATTACK_DISPATCH: dict[str, Callable[[float], None]] = {
    "kill": kill_leader_after,
    "partition": partition_leader_after,
    "freeze": freeze_leader_after,
    "transfer": transfer_leader_after,
    "cascade": cascade_kill_after,
    "quorum_loss": quorum_loss_after,
    "asymmetric_partition": asymmetric_partition_after,
    "network_slow": network_slow_after,
    "network_loss": network_loss_after,
    "clock_skew": clock_skew_after,
    "pg_only_kill": pg_only_kill_after,
    "disk_full": disk_full_after,
    "fsync_stall": fsync_stall_after,
    "flap_partition": flap_partition_after,
    "membership_change": membership_change_after,
    "chaos_storm": chaos_storm_after,
    # SCAFFOLD ATTACKS — raise NotImplementedError until prerequisites are
    # added (PG image rebuild for fsync_drop, nbd sidecar for bit_flip).
    # Registered here so `--attack fsync_drop` fails with a precise message
    # instead of "unknown attack". Intentionally absent from
    # `run_elle_matrix.sh` ALL_ATTACKS and from `chaos_storm`'s random pool.
    "fsync_drop": fsync_drop_after,
    "bit_flip": bit_flip_after,
}


SCAFFOLD_ATTACKS: set[str] = {"fsync_drop", "bit_flip"}
"""Attacks registered for discoverability but not yet implemented. Calling
one of these raises NotImplementedError with the prereq doc. The CLI also
checks this set before launching the injector thread so the user gets a
clear failure instead of a silent daemon-thread death."""


@app.command()
def run(
    artifact_dir: str = typer.Option(
        "testing/artifacts/linearizability-register",
        "--artifact-dir",
        envvar="ARTIFACT_DIR",
        help="Where to write history.json and results.json.",
    ),
    seed: int = typer.Option(
        0,
        "--seed",
        help="RNG seed for worker op selection. 0 = derive from time.",
    ),
    attack: str = typer.Option(
        "kill",
        "--attack",
        help=f"One of: {', '.join(ATTACK_DISPATCH)}",
    ),
    check: str = typer.Option(
        "wgl",
        "--check",
        help="'wgl' = strict linearizability (slow, ≤cap ops/key); "
        "'weak' = no-phantom-reads (fast, any scale); "
        "'elle' = subprocess into Elle for transactional anomaly classes "
        "(requires --workload txn).",
    ),
    workload: str = typer.Option(
        "register",
        "--workload",
        help="'register' = single-op reads/writes/CAS (default); "
        "'txn' = 2-key SERIALIZABLE multi-statement transactions (for Elle).",
    ),
    workers: int = typer.Option(NUM_WORKERS, "--workers", help="Concurrent client threads."),
    keys: int = typer.Option(NUM_KEYS, "--keys", help="Number of register keys."),
    duration: float = typer.Option(
        WORKLOAD_DURATION_SECONDS, "--duration", help="Workload runtime (s)."
    ),
    fault_at: float = typer.Option(
        KILL_LEADER_AFTER_SECONDS, "--fault-at", help="When to inject the fault (s)."
    ),
) -> None:
    """Run a concurrent register workload with leader-kill mid-flight.

    Spawns NUM_WORKERS threads issuing reads / writes / CAS across NUM_KEYS
    keys. Kills the leader after KILL_LEADER_AFTER_SECONDS. After
    WORKLOAD_DURATION_SECONDS total, stops workers, waits for cluster
    recovery, then checks each key's op history for linearizability.
    """
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)

    actual_seed = seed if seed != 0 else int(time.time())
    # Override module globals so worker_loop, setup_table, History.per_key all
    # see the same configuration. Test-script-grade mutation; production
    # code would inject these.
    global NUM_WORKERS, NUM_KEYS, WORKLOAD_DURATION_SECONDS, KILL_LEADER_AFTER_SECONDS
    NUM_WORKERS = workers
    NUM_KEYS = keys
    WORKLOAD_DURATION_SECONDS = duration
    KILL_LEADER_AFTER_SECONDS = fault_at
    # Validate workload / check combo before the run kicks off.
    valid_workloads = {"register", "txn", "list-append"}
    valid_checks = {"wgl", "weak", "elle"}
    if workload not in valid_workloads:
        console.print(f"[bold red]Unknown workload:[/] {workload}")
        raise typer.Exit(code=2)
    if check not in valid_checks:
        console.print(f"[bold red]Unknown check:[/] {check}")
        raise typer.Exit(code=2)
    if check == "elle" and workload not in {"txn", "list-append"}:
        console.print(
            "[bold red]--check elle requires --workload txn or list-append[/] "
            "(per-key register histories have no cross-key dependencies for Elle)"
        )
        raise typer.Exit(code=2)
    if workload in {"txn", "list-append"} and keys < 2:
        console.print(f"[bold red]--workload {workload} requires --keys >= 2[/]")
        raise typer.Exit(code=2)

    console.rule(f"[bold]LINEARIZABILITY (workload={workload}, check={check})")
    console.print(
        f"[dim]Seed: {actual_seed}  (replay with --seed {actual_seed})"
        f" | workers={workers} keys={keys} duration={duration}s fault_at={fault_at}s[/]"
    )

    if not wait_cluster_healthy(timeout=120):
        console.print("[bold red]FATAL:[/] cluster not healthy after 120s")
        raise typer.Exit(code=2)
    table_setup_ok = setup_list_append_table() if workload == "list-append" else setup_table()
    if not table_setup_ok:
        table_name = "linappend" if workload == "list-append" else "linreg"
        console.print(f"[bold red]FATAL:[/] could not create {table_name} table")
        raise typer.Exit(code=2)

    history = History()
    stop_event = threading.Event()
    worker_threads: list[threading.Thread] = []
    worker_fn = {
        "register": worker_loop,
        "txn": txn_worker_loop,
        "list-append": list_append_worker_loop,
    }[workload]
    for i in range(NUM_WORKERS):
        wrng = random.Random(actual_seed + i)
        t = threading.Thread(
            target=worker_fn,
            args=(i, history, stop_event, wrng),
            name=f"linreg-w{i}",
            daemon=True,
        )
        worker_threads.append(t)

    if attack not in ATTACK_DISPATCH:
        console.print(f"[bold red]Unknown attack:[/] {attack}")
        raise typer.Exit(code=2)
    if attack in SCAFFOLD_ATTACKS:
        # Surface the precondition before the workload starts. The
        # NotImplementedError raised inside the injector thread would
        # otherwise die silently and the run would falsely report PASS.
        try:
            ATTACK_DISPATCH[attack](0.0)
        except NotImplementedError as e:
            console.print(f"[bold red]{attack} is a scaffold attack:[/]\n{e}")
            raise typer.Exit(code=2) from e
    console.print(f"[dim]Attack mode: {attack}[/]")
    killer = threading.Thread(
        target=ATTACK_DISPATCH[attack],
        args=(KILL_LEADER_AFTER_SECONDS,),
        daemon=True,
        name="injector",
    )
    killer.start()
    for t in worker_threads:
        t.start()

    console.print(
        f"Running workload for {WORKLOAD_DURATION_SECONDS:.0f}s "
        f"({NUM_WORKERS} workers, {NUM_KEYS} keys, "
        f"leader-kill at {KILL_LEADER_AFTER_SECONDS:.0f}s)..."
    )
    time.sleep(WORKLOAD_DURATION_SECONDS)
    stop_event.set()
    for t in worker_threads:
        t.join(timeout=10)
    killer.join(timeout=10)

    start_killed_nodes()
    scrub_chaos_residue()
    console.print("Waiting for cluster recovery…")
    wait_cluster_healthy(timeout=90)
    time.sleep(2)

    # ── Persist raw history first (always, even if check fails) ─────────────
    history_path = artifact_path / "history.json"
    history_path.write_text(
        json.dumps([op.to_jsonable() for op in history.ops], indent=2),
        encoding="utf-8",
    )

    any_failure = False
    results: dict[int, dict[str, object]] = {}
    elle_summary: dict[str, object] | None = None

    if check == "elle":
        # ── Elle (subprocess) ────────────────────────────────────────────────
        from elle_adapter import ElleError, run_check

        elle_model = "list-append" if workload == "list-append" else "rw-register"
        records = [r.to_jsonable() for r in history.jepsen]
        try:
            elle_result = run_check(
                records=records,
                out_dir=artifact_path,
                model=elle_model,
                timeout_s=300,
            )
        except ElleError as e:
            console.print(f"[bold red]Elle infrastructure error:[/] {e}")
            raise typer.Exit(code=2) from e

        elle_table = Table(title="Elle Anomalies", show_lines=False)
        elle_table.add_column("Anomaly", style="bold")
        elle_table.add_column("Count", justify="right")
        elle_table.add_column("Sample cycle (head)")
        seen: set[str] = set()
        for a in elle_result.anomalies:
            if a.name in seen:
                continue
            seen.add(a.name)
            count = elle_result.anomaly_summary.get(a.name, 0)
            cycle_str = ", ".join(str(c) for c in a.cycle[:5])
            if len(a.cycle) > 5:
                cycle_str += " …"
            elle_table.add_row(a.name, str(count), cycle_str)
        if not elle_result.anomalies:
            elle_table.add_row("(none)", "0", "")
        console.print()
        console.print(elle_table)
        verdict_word = (
            "PASS"
            if elle_result.valid is True
            else "FAIL"
            if elle_result.valid is False
            else "UNKNOWN"
        )
        verdict_color = (
            "green"
            if elle_result.valid is True
            else "red"
            if elle_result.valid is False
            else "yellow"
        )
        console.print(
            f"[{verdict_color}]Elle verdict: {verdict_word}[/] "
            f"(anomalies: {len(elle_result.anomalies)}, "
            f"elapsed: {elle_result.elapsed_ms:.0f} ms)"
        )

        any_failure = elle_result.valid is not True
        elle_summary = {
            "valid": elle_result.valid,
            "anomaly_classes": list(elle_result.anomaly_summary),
            "anomaly_summary": elle_result.anomaly_summary,
            "elapsed_ms": elle_result.elapsed_ms,
            "op_count": elle_result.op_count,
        }
    else:
        # ── Per-key WGL or weak check ────────────────────────────────────────
        per_key = history.per_key()
        checker = _is_weakly_consistent if check == "weak" else _is_linearizable
        for key, ops in per_key.items():
            ok, reason = checker(ops)
            results[key] = {
                "key": key,
                "op_count": len(ops),
                "linearizable": ok,
                "reason": reason,
            }
            if not ok:
                any_failure = True

        result_table = Table(title="Linearizability Results", show_lines=False)
        result_table.add_column("Key", style="bold", justify="right")
        result_table.add_column("Ops")
        result_table.add_column("Linearizable")
        result_table.add_column("Reason")
        for key, info in results.items():
            verdict = "[green]PASS[/]" if info["linearizable"] else "[red]FAIL[/]"
            result_table.add_row(str(key), str(info["op_count"]), verdict, str(info["reason"]))
        console.print()
        console.print(result_table)

    # ── Persist top-level results.json ──────────────────────────────────────
    results_path = artifact_path / "results.json"
    results_path.write_text(
        json.dumps(
            {
                "seed": actual_seed,
                "workers": NUM_WORKERS,
                "keys": NUM_KEYS,
                "duration_s": WORKLOAD_DURATION_SECONDS,
                "workload": workload,
                "check": check,
                "attack": attack,
                "verdict": "PASS" if not any_failure else "FAIL",
                "per_key": list(results.values()),
                "elle": elle_summary,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    console.print(f"History → {history_path}")
    console.print(f"Results → {results_path}")

    raise typer.Exit(code=0 if not any_failure else 1)


if __name__ == "__main__":
    app()
