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

  - **WGL is exponential in the worst case.** `WGL_OPS_PER_KEY_CAP` bounds
    the per-key op count actually searched. A longer history is reduced to
    one contiguous window of that size centred near the median return time,
    with the register's starting value inferred from the discarded prefix;
    ops outside the window are not checked. Real Jepsen uses Knossos/Elle
    which have better-than-WGL constants and can also verify transactional
    histories. We do not.

  - **Indeterminate operations are encoded as "pending"** (`return_ts is
    None`, `result is None`) and both possible outcomes are considered: a
    write that timed out could have committed or not. Distinct from a
    **definite rejection** (`result is False` — PG refused the statement),
    which provably never reached the register and is therefore modelled as
    a no-op; a later read of its value is an anomaly, not a normal write.

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

A pending op has no return time, so it is treated as returning at +inf: it
stays eligible at every step and can always be deferred to the tail of the
order, where nothing observes it. Applying it earlier is the "this op
happened" branch; deferring it to the tail is "this op didn't happen". The
search needs no separate branch for the two. A definitely-rejected op is an
identity transition, so placing it is always legal and constrains nothing.

═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import contextlib
import json
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import fault_primitives as fp
from linreg.checkers import (
    _is_linearizable,
    _is_weakly_consistent,
)
from linreg.cluster import (
    MGMT_PORTS,
    NODES,
    find_leader,
    run_cmd,
    wait_cluster_healthy,
)
from linreg.config import (
    CHAOS_STORM_DURATION,
    DEFAULT_KILL_LEADER_AFTER_SECONDS,
    DEFAULT_NUM_KEYS,
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKLOAD_DURATION_SECONDS,
    WorkloadConfig,
)
from linreg.records import History
from linreg.workload import (
    list_append_worker_loop,
    setup_list_append_table,
    setup_table,
    txn_worker_loop,
    worker_loop,
)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Topology constants live in `linreg.cluster` (imported above): one source of
# truth, so the two cannot drift.

# Workload configuration lives in `linreg.config`; re-exported below so the
# CLI signature and the tests keep one import site.

# ─────────────────────────────────────────────────────────────────────────────
# Operation history
# ─────────────────────────────────────────────────────────────────────────────
# Records live in `linreg.records`; re-exported here for the same reason.

# ─────────────────────────────────────────────────────────────────────────────
# Shell helpers
# ─────────────────────────────────────────────────────────────────────────────

# Cluster helpers live in `linreg.cluster`; re-exported below.

# ─────────────────────────────────────────────────────────────────────────────
# Workload
# ─────────────────────────────────────────────────────────────────────────────


# Table setup, op helpers, and the worker loops live in `linreg.workload`.


@dataclass
class InjectorOutcome:
    """Whether the fault injector finished, and why not if it did not.

    The injector is a daemon thread, so an exception raised inside it is printed
    to stderr and then discarded: the run continues, finds no anomaly in a
    workload that was never faulted, and reports PASS. The fault primitives raise
    rather than no-op, which only helps if somebody reads the exception, so
    `run()` checks this before computing a verdict.
    """

    error: BaseException | None = None
    finished: bool = False


def run_injector(
    fn: Callable[..., None],
    args: tuple[object, ...],
    outcome: InjectorOutcome,
) -> None:
    """Call `fn(*args)`, recording the outcome for `run()` to inspect."""
    try:
        fn(*args)
    except BaseException as exc:
        outcome.error = exc
    finally:
        outcome.finished = True


def kill_leader_after(delay: float) -> None:
    """Sleep `delay` seconds, then kill whichever node is currently leader."""
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    run_cmd(f"docker compose kill {leader}", timeout=10)


def partition_leader_after(delay: float, heal_after: float = 4.0) -> None:
    """Detach the leader from the cluster network, reattach after `heal_after`."""
    time.sleep(delay)
    leader, _ = find_leader()
    if leader is None:
        return
    with fp.network_detached(leader):
        time.sleep(heal_after)


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
    # One window per peer, each verified: the primitive confirms the rule is
    # installed *and* matched packets, so a rule that exists but drops nothing
    # is a failure rather than a quiet pass.
    with contextlib.ExitStack() as stack:
        for peer in (n for n in NODES if n != leader):
            stack.enter_context(
                fp.partition_asymmetric(peer, leader, direction=fp.Direction.INBOUND)
            )
        time.sleep(hold)


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
    with fp.partition_lossy(leader, drop_pct=0.0, latency_ms=delay_ms):
        time.sleep(hold)


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
    with fp.partition_lossy(leader, drop_pct=float(loss_pct), latency_ms=0):
        time.sleep(hold)


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
        with fp.network_detached(leader):
            time.sleep(period_s / 2)
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
    duration: float = CHAOS_STORM_DURATION,
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


def scrub_chaos_residue() -> list[str]:
    """Clear fault residue, returning whatever survived.

    Each fault heals its own scope in its own `finally`; this is the backstop for
    a fault that crashed or a run that was interrupted. The primitive layer
    clears more than this used to — notably it resumes any postgres process left
    in state ``T``, which otherwise survives the whole run and poisons every
    later case.

    Residue is returned rather than raised because the caller runs this before
    persisting the history, and losing that artifact costs more than the delay
    in reporting.
    """
    residue: list[str] = []
    try:
        residue.extend(fp.scrub().residue)
    except fp.FaultError as exc:
        residue.append(str(exc))
    # Witness: tear it down so the next run starts from the canonical
    # 3-node topology.
    run_cmd("docker compose --profile witness rm -sf witness", timeout=30)
    return residue


# ─────────────────────────────────────────────────────────────────────────────
# WGL linearizability checker — register model
# ─────────────────────────────────────────────────────────────────────────────
# Checkers live in `linreg.checkers`; re-exported here because the matrix,
# the tests, and the CLI all reach them through this module.


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


SEEDED_ATTACKS: Final[frozenset[str]] = frozenset({"chaos_storm"})
"""Attacks whose fault schedule comes from an RNG and so must receive the run's
seed to be replayable. The injector passes `(delay, duration, seed)` for these
and `(delay,)` for everything else."""


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
    workers: int = typer.Option(
        DEFAULT_NUM_WORKERS, "--workers", help="Concurrent client threads."
    ),
    keys: int = typer.Option(DEFAULT_NUM_KEYS, "--keys", help="Number of register keys."),
    duration: float = typer.Option(
        DEFAULT_WORKLOAD_DURATION_SECONDS, "--duration", help="Workload runtime (s)."
    ),
    fault_at: float = typer.Option(
        DEFAULT_KILL_LEADER_AFTER_SECONDS, "--fault-at", help="When to inject the fault (s)."
    ),
) -> None:
    """Run a concurrent register workload with leader-kill mid-flight.

    Spawns `--workers` threads issuing reads / writes / CAS across `--keys`
    keys. Kills the leader at `--fault-at`. After `--duration` total, stops
    workers, waits for cluster recovery, then checks each key's op history for
    linearizability.
    """
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)

    actual_seed = seed if seed != 0 else int(time.time())
    cfg = WorkloadConfig(workers=workers, keys=keys, duration_s=duration, fault_at=fault_at)
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
    table_setup_ok = (
        setup_list_append_table(cfg.keys) if workload == "list-append" else setup_table(cfg.keys)
    )
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
    for i in range(cfg.workers):
        wrng = random.Random(actual_seed + i)
        t = threading.Thread(
            target=worker_fn,
            args=(i, history, stop_event, wrng, cfg),
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
    # chaos_storm picks its faults, ordering, and offsets from an RNG. Without
    # the run's seed it falls back to wall-clock time, so replaying a failure
    # with the recorded seed would reproduce the workload but not the fault
    # schedule. Attacks that take no seed keep the plain (delay,) signature.
    injector_args: tuple[object, ...] = (cfg.fault_at,)
    if attack in SEEDED_ATTACKS:
        injector_args = (cfg.fault_at, CHAOS_STORM_DURATION, actual_seed)
    injector = InjectorOutcome()
    killer = threading.Thread(
        target=run_injector,
        args=(ATTACK_DISPATCH[attack], injector_args, injector),
        daemon=True,
        name="injector",
    )
    killer.start()
    for t in worker_threads:
        t.start()

    console.print(
        f"Running workload for {cfg.duration_s:.0f}s "
        f"({cfg.workers} workers, {cfg.keys} keys, "
        f"leader-kill at {cfg.fault_at:.0f}s)..."
    )
    time.sleep(cfg.duration_s)
    stop_event.set()
    for t in worker_threads:
        t.join(timeout=10)
    killer.join(timeout=10)

    start_killed_nodes()
    residue = scrub_chaos_residue()
    console.print("Waiting for cluster recovery…")
    wait_cluster_healthy(timeout=90)
    time.sleep(2)

    # ── Persist raw history first (always, even if check fails) ─────────────
    history_path = artifact_path / "history.json"
    history_path.write_text(
        json.dumps([op.to_jsonable() for op in history.ops], indent=2),
        encoding="utf-8",
    )

    # An injector that raised leaves a history with no fault in it. Checking that
    # history would find no anomaly and report PASS, so refuse a verdict instead.
    # Exit 2 (infra), not 1 (violation): the run proves nothing either way.
    if injector.error is not None:
        console.print(
            f"[bold red]FATAL:[/] the {attack} injector failed, so no fault was "
            f"injected for at least part of the run:\n  "
            f"{type(injector.error).__name__}: {injector.error}"
        )
        raise typer.Exit(code=2)
    if not injector.finished:
        console.print(
            f"[bold red]FATAL:[/] the {attack} injector did not finish within the "
            "join timeout; the fault window is unknown, so no verdict is possible."
        )
        raise typer.Exit(code=2)
    if residue:
        console.print(
            "[bold red]FATAL:[/] fault residue survived the scrub, so this run "
            "would poison the next one:\n  " + "\n  ".join(residue)
        )
        raise typer.Exit(code=2)

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
        per_key = history.per_key(cfg.keys)
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
                "workers": cfg.workers,
                "keys": cfg.keys,
                "duration_s": cfg.duration_s,
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
