#!/usr/bin/env -S uv run --project testing python
"""Torn-write oracle for the Raft store (H-25).

PROVES: that a write torn in half by a power loss cannot make a node serve
Raft state it never committed. The property is sharper here than for a heap
page. Lost data on a follower is recoverable from the leader; a corrupted
*vote* or *log* is not, because a node that forgets it voted can vote again in
the same term, and a node that forgets a committed entry can help elect a
leader that never had it.

HOW: ``lazyfs::torn-op`` splits the next write to ``raft.db`` and crashes the
Raft store's LazyFS instance, so the unwritten half dies in its userspace
cache. The Raft store has its own instance, separate from PGDATA's, so this
does not disturb PostgreSQL and the damage is attributable.

WHAT COUNTS AS PASSING: either outcome, provided it is not silent.

  - tolerated: redb rolls back to its last committed state and the node
    rejoins. Expected when the tear lands in an uncommitted page.
  - detected:  redb reports the store unreadable and pgbattery refuses to
    start rather than recreating it.

The violation is a node that comes back believing something the cluster never
committed, which shows up here as a lost acked write or a second leader.

WHY THE INVERSION IS NOT OPTIONAL: most tears land in uncommitted pages and are
correctly a non-event, so a green run is the *usual* result and says nothing by
itself about whether damage would ever be noticed. ``--prove-oracle`` therefore
mangles ``raft.db`` past repair first and requires the node to refuse to start,
which establishes that the observable a green is measured on is reachable at
all. Without it this suite would pass just as happily against a fault that
injected nothing.

Run:
    COMPOSE_FILE=docker-compose.lazyfs.yml docker compose up -d node1 node2 node3
    COMPOSE_FILE=docker-compose.lazyfs.yml uv run --project testing \
        python testing/torn_raft.py --prove-oracle
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Final

import psycopg

import fault_primitives as fp
import topology

RAFT_DB: Final[str] = f"{fp.LAZYFS_RAFT.root_dir}/{fp.RAFT_DB_FILE}"
TABLE: Final[str] = "torn_raft_probe"

CONVERGE_TIMEOUT_S: Final[float] = 180.0
REFUSAL_TIMEOUT_S: Final[float] = 90.0
# How long the LazyFS mounts may take to appear after the containers start.
# The entrypoint gives itself 30s per mount and there are two, so this covers
# a slow runner without ever masking a mount that is genuinely absent.
MOUNT_TIMEOUT_S: Final[float] = 90.0
CORRUPT_MARKER: Final[str] = "Raft DB corrupted"
"""Emitted by `src/governor/storage.rs` when redb returns an error opening the
store. Matched rather than a redb string so a change in redb's wording surfaces
as a failing test rather than a silent loss of the check."""

REDB_PANIC_MARKERS: Final[tuple[str, ...]] = (
    "entered unreachable code",
    "panicked at",
)
"""A damaged store does not always come back as an `Err`. redb 4.1 can panic on
an `unreachable!()` deep in its btree while reading one, which bypasses the
clean refusal path in `storage.rs` entirely.

Both count as refusing, because in both the process dies rather than serving
Raft state nobody vouched for -- which is the safety property. They are not
equally good, and the difference is recorded in HARDENING.md: a panic gives the
operator a backtrace where the handled path gives them the reason and the
recovery steps."""

STORAGE_ERROR_MARKER: Final[str] = "Failed to create database"
"""A third shape. redb can also report a damaged store through the open path
that `storage.rs` treats as creation, which produces a generic storage error
rather than the refusal written for this case."""

GATEWAY_PORT_BY_NODE: Final[dict[str, int]] = dict(
    zip(topology.NODES, topology.GATEWAY_PORTS, strict=True)
)


class RaftDurabilityViolation(RuntimeError):
    """A node came back with Raft state the cluster never committed."""


class OracleNotProven(RuntimeError):
    """The inversion did not go red, so a green result would mean nothing."""


@dataclass
class Outcome:
    tears: int = 0
    torn_bytes: list[int] = field(default_factory=list)
    acked: list[int] = field(default_factory=list)
    surviving: set[int] = field(default_factory=set)
    victim_healthy: bool = False
    victim_refused: bool = False
    leaders_after: list[str] = field(default_factory=list)
    refusal: str = ""
    torn_offsets: list[int] = field(default_factory=list)
    observed: list[dict[str, int]] = field(default_factory=list)
    attempts: int = 0
    victim: str = ""
    target: str = ""

    @property
    def lost(self) -> list[int]:
        return sorted(set(self.acked) - self.surviving)

    @property
    def silent(self) -> bool:
        """Neither tolerated cleanly nor loudly refused.

        A node that is neither healthy nor refusing is the state this suite
        exists to catch: still running, still voting, on a store nobody
        vouched for.
        """
        return not self.victim_healthy and not self.victim_refused

    def as_dict(self) -> dict[str, object]:
        return {
            "target": self.target,
            "victim": self.victim,
            "attempts": self.attempts,
            "tears": self.tears,
            "torn_bytes": self.torn_bytes,
            "torn_offsets": self.torn_offsets,
            "observed_writes": self.observed,
            "acked": len(self.acked),
            "surviving": len(self.surviving),
            "lost_acked": len(self.lost),
            "lost_keys": self.lost[:20],
            "victim_healthy": self.victim_healthy,
            "victim_refused": self.victim_refused,
            "refusal_shape": self.refusal,
            "leaders_after": self.leaders_after,
            "contracts": ["R2", "L3"],
        }


def mgmt(node: str, path: str) -> Any:
    port = topology.MGMT_PORTS[node]
    result = fp.run(f"curl -s --max-time 5 http://127.0.0.1:{port}/api/v1{path}")
    if not result.ok or not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def leaders() -> list[str]:
    """Every node that some reachable node currently calls the leader.

    Collected as a list rather than reduced to one, because more than one
    distinct answer is itself the finding.
    """
    seen: list[str] = []
    for node in topology.NODES:
        info = mgmt(node, "/cluster/leader")
        if not info or not info.get("leader_id"):
            continue
        name = f"node{info['leader_id']}"
        if name not in seen:
            seen.append(name)
    return seen


def await_leader(timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        found = leaders()
        if len(found) == 1:
            return found[0]
        time.sleep(2)
    raise fp.FaultPreconditionError(
        f"no single leader within {timeout_s:g}s (saw {leaders()}); the cluster "
        f"never converged, so nothing below would measure a torn write"
    )


def connect(node: str) -> psycopg.Connection[Any]:
    return psycopg.connect(
        host="127.0.0.1",
        port=GATEWAY_PORT_BY_NODE[node],
        user="postgres",
        dbname="postgres",
        connect_timeout=10,
        autocommit=True,
    )


def ensure_table(node: str) -> None:
    with connect(node) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} (k int PRIMARY KEY)")


def write_batch(node: str, keys: range) -> list[int]:
    """Acked writes only. An INSERT that raised was never promised to anyone."""
    acked: list[int] = []
    with connect(node) as conn, conn.cursor() as cur:
        for key in keys:
            try:
                cur.execute(f"INSERT INTO {TABLE} VALUES (%s)", (key,))
                acked.append(key)
            except psycopg.Error:
                continue
    return acked


def read_back(node: str) -> set[int]:
    with connect(node) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT k FROM {TABLE}")
        return {int(row[0]) for row in cur.fetchall()}


def container_healthy(service: str) -> bool:
    status = fp.run(f"docker compose ps --format '{{{{.Service}}}} {{{{.Status}}}}' {service}")
    return "healthy" in status.stdout and "Restarting" not in status.stdout


def refused_to_start(service: str) -> bool:
    """Whether the node is refusing to run rather than running on a damaged store.

    The signal is the container restart-looping, not a log string. A damaged
    Raft store surfaces in at least three shapes -- the handled
    `Raft DB corrupted` refusal, a redb `unreachable!()` panic, and a generic
    `Failed to create database: I/O error` -- and matching on text meant every
    shape not yet seen read as "still running fine". Dying repeatedly is what
    they have in common and is the thing the safety property actually cares
    about: the process is not up, so it is not voting.

    :func:`refusal_shape` reports which of them it was, because the difference
    matters to an operator even though it does not to this assertion.
    """
    status = fp.run(f"docker compose ps -a --format '{{{{.Status}}}}' {service}")
    return status.ok and "Restarting" in status.stdout


def refusal_shape(service: str) -> str:
    """How the node refused, for the report. Not an assertion."""
    logs = fp.run(f"docker compose logs --no-color --tail 2000 {service}")
    haystack = logs.output if logs.ok else ""
    if CORRUPT_MARKER in haystack:
        return "handled: refused with recovery instructions"
    if any(marker in haystack for marker in REDB_PANIC_MARKERS):
        return "redb panic: unreachable!() in the btree, bypassing the handled path"
    if STORAGE_ERROR_MARKER in haystack:
        return "generic storage error: no recovery instructions"
    return "unknown"


def await_victim_settled(service: str, timeout_s: float) -> tuple[bool, bool]:
    """Wait until the victim is either healthy or visibly refusing."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if refused_to_start(service):
            return False, True
        if container_healthy(service):
            return True, False
        time.sleep(3)
    return container_healthy(service), refused_to_start(service)


def tear_once(victim: str, index: int, outcome: Outcome) -> tuple[int, int] | None:
    """Arm one torn write on `victim`'s raft.db, drive traffic, report the tear.

    Returns ``(bytes, offset)``, or None when the tear did not fire -- a real
    possibility, because the fault is armed for the *next* write to that path
    and a node that has caught up may not write again promptly.

    The writes driven here go into `outcome.acked`. They are issued while a
    tear is armed on a live member of the cluster, which makes them the writes
    most exposed to it; counting only the ones from before the first tear would
    have measured the safest keys in the run.
    """
    fp.arm_torn_write(victim, RAFT_DB, parts=2, persist=(1,), mount=fp.LAZYFS_RAFT)
    lead = await_leader(CONVERGE_TIMEOUT_S)
    base = 1000 * (index + 1)
    outcome.acked.extend(write_batch(lead, range(base, base + 50)))
    time.sleep(3)

    log = fp.exec_in(victim, fp.lazyfs_log_cmd(log=fp.LAZYFS_RAFT.log))
    records = fp.parse_lazyfs_torn_records(log.stdout, RAFT_DB) if log.ok else []
    return records[-1] if records else None


def run_tears(*, tears: int, target: str, min_torn_bytes: int, max_attempts: int) -> Outcome:
    """Tear `target`'s Raft store `tears` times, retrying for a big enough tear.

    `min_torn_bytes` exists because redb's next write after arming is usually a
    short record. A 160-byte tear of a 320-byte append says little about how
    the store handles damage to a page it has committed, so an attempt that
    lands under the threshold is retried rather than counted. What each attempt
    actually tore is recorded either way, because "redb never writes anything
    larger in this workload" would itself be the answer.
    """
    outcome = Outcome()
    lead = await_leader(CONVERGE_TIMEOUT_S)
    ensure_table(lead)
    outcome.acked = write_batch(lead, range(200))

    # A follower keeps redb's behaviour separate from a promotion happening at
    # the same moment. The leader is the harder case: it is the node whose vote
    # and log the cluster is currently relying on.
    victim = lead if target == "leader" else next(n for n in topology.NODES if n != lead)
    outcome.victim = victim
    outcome.target = target

    attempts = 0
    while outcome.tears < tears and attempts < max_attempts:
        attempts += 1
        record = tear_once(victim, attempts, outcome)
        if record is not None:
            persisted, offset = record
            outcome.observed.append({"bytes": persisted, "offset": offset})
            if persisted >= min_torn_bytes:
                outcome.tears += 1
                outcome.torn_bytes.append(persisted)
                outcome.torn_offsets.append(offset)

        # The tear crashes the Raft store's LazyFS, leaving the mount stale.
        # Recreate rather than restart so the entrypoint remounts both
        # instances through its own tested path.
        fp.run(f"docker compose up -d --force-recreate {victim}")
        outcome.victim_healthy, outcome.victim_refused = await_victim_settled(
            victim, CONVERGE_TIMEOUT_S
        )
        if outcome.victim_refused:
            outcome.refusal = refusal_shape(victim)
            break
        if not outcome.victim_healthy:
            # Neither healthy nor restart-looping. Stop rather than arm the
            # next tear against a node that cannot be reached: the exec would
            # fail and report as a fault that did not fire.
            break
        if target == "leader":
            # Killing the leader's Raft store moves leadership. Re-resolve, or
            # every later tear would be aimed at a node that is now a follower
            # while the report still claimed the leader was targeted.
            lead = await_leader(CONVERGE_TIMEOUT_S)
            victim = lead
            outcome.victim = victim

    outcome.attempts = attempts

    outcome.leaders_after = leaders()
    survivor = await_leader(CONVERGE_TIMEOUT_S)
    outcome.surviving = read_back(survivor)
    return outcome


def await_fault_readiness() -> None:
    """Wait until every node can actually receive a fault.

    Two facts, neither implying the other: the Raft store must be on LazyFS,
    and LazyFS's fault worker must be consuming its control FIFO. Both carry
    the full timeout. A mount that is present says nothing about the container
    a moment later -- a node that restarts between the two checks passes the
    first and cannot be exec'd into for the second.
    """
    for node in topology.NODES:
        fp.verify_lazyfs_mounted(node, fp.LAZYFS_RAFT.mount_dir, timeout_s=MOUNT_TIMEOUT_S)
        fp.verify_lazyfs_fault_channel(node, mount=fp.LAZYFS_RAFT, timeout_s=MOUNT_TIMEOUT_S)


def reset_cluster() -> None:
    """Rebuild the cluster from nothing and wait for a leader.

    The inversion leaves a node that cannot open its Raft store, and there is
    no repairing it in place: `docker compose exec` will not attach to a
    container that is restart-looping, so the file cannot be put back from
    inside. Nor should it be -- pgbattery's own message says a damaged store
    means removing the node from membership and re-joining it as a learner.
    Recreating the cluster is that, done bluntly.
    """
    fp.run("docker compose down -v", timeout_s=180.0)
    started = fp.run("docker compose up -d node1 node2 node3", timeout_s=300.0)
    if not started.ok:
        raise fp.FaultPreconditionError(f"could not restart the cluster: {started.output}")
    await_leader(CONVERGE_TIMEOUT_S)
    # A leader says nothing about whether the other nodes can receive a fault
    # yet. Without this the real run reached a node still restarting its
    # LazyFS and failed writing to a control FIFO that did not exist.
    await_fault_readiness()


def prove_oracle() -> None:
    """Show that damage to raft.db is noticed at all.

    Overwrites the head of the store with random bytes -- past what any single
    torn write does -- and requires the node to stop rather than run on it.
    Most tears land in uncommitted pages and are correctly a non-event, so
    without this a green run below could not be told apart from a fault that
    injected nothing.
    """
    lead = await_leader(CONVERGE_TIMEOUT_S)
    victim = next(n for n in topology.NODES if n != lead)

    mangle = fp.exec_in(
        victim, f"dd if=/dev/urandom of={RAFT_DB} bs=4096 count=20 conv=notrunc 2>/dev/null"
    )
    if not mangle.ok:
        raise fp.FaultPreconditionError(f"{victim}: could not mangle raft.db: {mangle.output}")

    fp.run(f"docker compose up -d --force-recreate {victim}")
    healthy, refused = await_victim_settled(victim, REFUSAL_TIMEOUT_S)

    if not refused:
        raise OracleNotProven(
            f"{victim} kept running on a raft.db overwritten with random bytes "
            f"(healthy={healthy}). Either the Raft store is not on LazyFS, or nothing "
            f"checks the store before opening it. A green run below would then be "
            f"measuring an observable that damage never reaches."
        )
    print(f"oracle proven: {victim} would not run on a corrupted raft.db")
    reset_cluster()
    print("cluster rebuilt for the real run\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tears", type=int, default=3, help="torn writes to land in sequence")
    parser.add_argument(
        "--target",
        choices=("follower", "leader"),
        default="follower",
        help="whose Raft store to tear; leader is the harder case",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=0,
        help="cap on arming attempts; defaults to four times --tears. Raise it "
        "when hunting for a write shape that is rare.",
    )
    parser.add_argument(
        "--min-torn-bytes",
        type=int,
        default=0,
        help="retry an attempt whose tear was smaller than this, so the run can "
        "aim past redb's short appends at a page it has committed",
    )
    parser.add_argument(
        "--prove-oracle",
        action="store_true",
        help="require a mangled raft.db to be refused before believing a green run",
    )
    args = parser.parse_args()

    # The suite's own readiness gate: it waits for the facts it needs rather
    # than trusting the container to have finished by the time compose says
    # "Started".
    await_fault_readiness()

    if args.prove_oracle:
        prove_oracle()

    outcome = run_tears(
        tears=args.tears,
        target=args.target,
        min_torn_bytes=args.min_torn_bytes,
        max_attempts=args.attempts or args.tears * 4,
    )
    print(json.dumps(outcome.as_dict(), indent=2))

    if outcome.tears == 0:
        largest = max((w["bytes"] for w in outcome.observed), default=0)
        raise fp.FaultEffectNotObserved(
            f"no torn write met the bar in {outcome.attempts} attempts, so this run "
            f"asserts nothing about torn-write handling. Largest tear seen was "
            f"{largest} bytes against a --min-torn-bytes of {args.min_torn_bytes}. "
            f"Either the fault never fired -- it arms for the *next* write to "
            f"raft.db, and a caught-up node may not write again promptly -- or redb "
            f"writes nothing that large in this workload, which is worth recording "
            f"rather than working around."
        )
    if outcome.lost:
        raise RaftDurabilityViolation(
            f"{len(outcome.lost)} acked writes are gone after tearing the Raft "
            f"store: {outcome.lost[:20]}. A torn write cost the cluster committed data."
        )
    if len(outcome.leaders_after) > 1:
        raise RaftDurabilityViolation(
            f"nodes disagree about the leader after a torn Raft write: "
            f"{outcome.leaders_after}. A node came back with a vote or log the "
            f"cluster never committed."
        )
    if outcome.silent:
        raise RaftDurabilityViolation(
            "the torn node is neither healthy nor refusing to start. It is running "
            "and voting on a Raft store nothing has vouched for."
        )

    how = "refused to start" if outcome.victim_refused else "tolerated and rejoined"
    print(f"{outcome.tears} torn writes to raft.db; the node {how}; no acked write lost")
    if outcome.victim_refused:
        print(f"  refusal shape -- {outcome.refusal}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RaftDurabilityViolation, OracleNotProven, fp.FaultError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
