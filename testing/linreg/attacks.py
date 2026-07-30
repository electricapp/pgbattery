"""Fault injectors and the attack table.

Every injector routes through `fault_primitives`, which verifies its own
effect and resolves docker names against the active compose project.
"""

from __future__ import annotations

import contextlib
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

import fault_primitives as fp
from linreg.cluster import MGMT_PORTS, NODES, find_leader, run_cmd
from linreg.config import CHAOS_STORM_DURATION


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
