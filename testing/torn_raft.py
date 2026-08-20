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
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Final, TypedDict

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
MANGLE_TIMEOUT_S: Final[float] = 30.0
MANGLE_MARKER: Final[str] = "PGBATTERYMANGLE"
"""Written over raft.db's head by the inversion. Recognisable on read-back, so
the damage can be confirmed rather than assumed."""
MANGLE_BYTES: Final[int] = 81920

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

HANDLED_REFUSAL: Final[str] = "handled: refused with recovery instructions"
PANIC_REFUSAL: Final[str] = "redb panic: unreachable!() in the btree, bypassing the handled path"
STORAGE_ERROR_REFUSAL: Final[str] = "generic storage error: no recovery instructions"
UNKNOWN_REFUSAL: Final[str] = "unknown"
"""The node is down and none of the store-damage markers explain why, so the
refusal cannot be attributed to the tear."""

PAGE_TEAR_OCCURRENCE: Final[int] = 1200
"""Which write to `raft.db` the page tear aims at, counted from the restarted
LazyFS's mount.

An idle cluster writes the store at a steady 475-485 writes/min — control-plane
appends on their own cadence, with no startup burst to clear, since the first
minute after a mount measures the same 487. So this is about two and a half
minutes in: long enough that the victim has rejoined and a tear lands on a node
the cluster has admitted, short enough to leave most of the wait as margin.

Pages are 85% of redb's writes, so a given occurrence is far more often a page
than a header, and the run reports which it hit rather than assuming."""

PAGE_TEAR_TIMEOUT_S: Final[float] = 420.0
"""How long to wait for the baked fault to reach its occurrence.

Two and a half times the measured time to reach :data:`PAGE_TEAR_OCCURRENCE`,
so a runner writing at half the rate still lands the tear."""

PAGE_TEAR_BATCH: Final[int] = 100
"""Writes issued per attempt, so a run that needed several still reads back a
distinct key range per attempt rather than re-acking one."""

PAGE_TEAR_ATTEMPTS: Final[int] = 4
"""Consecutive occurrences to try before giving up on reaching a page.

Each costs a container recreate and a re-settle, so this is not free — but with
roughly five pages per header, four consecutive headers is not a run that got
unlucky, it is a run against a write pattern nobody measured."""

WRITABILITY_PROBE_KEY: Final[int] = -1
"""Key the writability probe deletes. Negative so no run ever writes it, which
is what makes the delete both a real write and a no-op."""

BASELINE_WRITES: Final[int] = 200
MIN_BASELINE_ACKED: Final[int] = 190
"""How much of the baseline batch must be acked before the first tear.

The cluster is healthy then, so a batch that mostly failed means there was
never a working cluster to damage. Checked because the durability verdict is
`acked - surviving`: an empty acked set makes "no acked write lost" hold
trivially, which is a green measuring nothing."""

LOG_READ_TIMEOUT_S: Final[float] = 45.0
"""How long to keep trying to read the LazyFS log after a tear. The tear kills
the Raft store's LazyFS, so the container holding the log is usually mid-restart
when the read is issued."""

GATEWAY_PORT_BY_NODE: Final[dict[str, int]] = dict(
    zip(topology.NODES, topology.GATEWAY_PORTS, strict=True)
)


class RaftDurabilityViolation(RuntimeError):
    """A node came back with Raft state the cluster never committed."""


class OracleNotProven(RuntimeError):
    """The inversion did not go red, so a green result would mean nothing."""


class ObservedTear(TypedDict):
    """One tear as LazyFS actually landed it, not as it was requested."""

    bytes: int
    offset: int


class TornRaftJson(TypedDict):
    """The run report. CI keeps this, so the keys are a contract with whatever
    reads it afterwards."""

    target: str
    victim: str
    attempts: int
    tears: int
    torn_bytes: list[int]
    torn_offsets: list[int]
    observed_writes: list[ObservedTear]
    acked: int
    surviving: int
    lost_acked: int
    lost_keys: list[int]
    victim_healthy: bool
    victim_refused: bool
    refusal_shape: str
    unreadable_reads: list[str]
    damage_established: bool
    leaders_after: list[str]
    contracts: list[str]


@dataclass(frozen=True, slots=True)
class TearReading:
    """What one attempt could establish about its tear.

    `record is None` with an empty `unreadable` means the log was consulted and
    held no tear. A non-empty `unreadable` means it could not be consulted at
    all, which is a different finding: collapsing the two reports a fault that
    fired as one that never did.
    """

    record: tuple[int, int] | None = None
    unreadable: str = ""


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
    observed: list[ObservedTear] = field(default_factory=list)
    attempts: int = 0
    victim: str = ""
    target: str = ""
    unreadable: list[str] = field(default_factory=list)

    @property
    def lost(self) -> list[int]:
        return sorted(set(self.acked) - self.surviving)

    @property
    def damage_established(self) -> bool:
        """Whether this run may assert anything about torn-write handling.

        A tear the LazyFS log sized past the bar establishes it. So does the
        node refusing to start on a store redb could not open: that is damage
        no byte threshold would have judged more strictly, and it is the usual
        way a big tear presents, because killing the Raft store's LazyFS also
        takes down the container whose log would have sized it.

        A refusal nobody can attribute does not count. A node that is down for
        reasons none of the store-damage markers explain is not evidence the
        tear reached the store.
        """
        return self.tears > 0 or (self.victim_refused and self.refusal != UNKNOWN_REFUSAL)

    @property
    def silent(self) -> bool:
        """Neither tolerated cleanly nor loudly refused.

        A node that is neither healthy nor refusing is the state this suite
        exists to catch: still running, still voting, on a store nobody
        vouched for.
        """
        return not self.victim_healthy and not self.victim_refused

    def as_json(self) -> TornRaftJson:
        return TornRaftJson(
            target=self.target,
            victim=self.victim,
            attempts=self.attempts,
            tears=self.tears,
            torn_bytes=self.torn_bytes,
            torn_offsets=self.torn_offsets,
            observed_writes=self.observed,
            acked=len(self.acked),
            surviving=len(self.surviving),
            lost_acked=len(self.lost),
            lost_keys=self.lost[:20],
            victim_healthy=self.victim_healthy,
            victim_refused=self.victim_refused,
            refusal_shape=self.refusal,
            unreadable_reads=self.unreadable,
            damage_established=self.damage_established,
            leaders_after=self.leaders_after,
            contracts=["R2", "L3"],
        )


def mgmt(node: str, path: str) -> Any:
    port = topology.MGMT_PORTS[node]
    result = fp.run(f"curl -s --max-time 5 http://127.0.0.1:{port}/api/v1{path}")
    if not result.ok or not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def mgmt_token() -> str:
    """The management API token, from the environment or `.env`.

    CI sets it as an environment variable and compose reads it from `.env`, so
    a harness that consults only one of the two works in exactly one place.
    """
    from_env = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "").strip()
    if from_env:
        return from_env
    found = fp.run("grep -m1 PGBATTERY_MANAGEMENT_API_TOKEN .env | cut -d= -f2")
    return found.stdout.strip() if found.ok else ""


def make_leader(node: str, timeout_s: float) -> None:
    """Move leadership to `node`, waiting until the cluster agrees it moved.

    Baking a fault into a config costs a restart, and restarting the leader
    hands leadership away — so a leader-targeted page tear would otherwise
    damage a follower's store and report it as the leader case. Transfer is a
    Raft operation that can be declined, so this waits on the result rather
    than on the request having been accepted.
    """
    deadline = time.monotonic() + timeout_s
    target = node.removeprefix("node")
    token = mgmt_token()
    while time.monotonic() < deadline:
        current = leaders()
        if current == [node]:
            return
        if current:
            port = topology.MGMT_PORTS[current[0]]
            fp.run(
                f"curl -s -X POST --max-time 10 -H 'x-pgbattery-token: {token}' "
                f"http://127.0.0.1:{port}/api/v1/cluster/transfer-leadership/{target}"
            )
        time.sleep(2)
    raise fp.FaultPreconditionError(
        f"leadership would not move to {node} within {timeout_s:g}s, so a leader-targeted "
        f"tear would have damaged a follower and reported it as the leader"
    )


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


def voter_views() -> dict[str, set[str]]:
    """Each reachable node's view of who the committed voters are.

    Kept per-node rather than reduced to one set, because a node that has not
    yet applied the membership entry the others have is exactly the state this
    is waiting out.
    """
    views: dict[str, set[str]] = {}
    for node in topology.NODES:
        info = mgmt(node, "/cluster/members")
        if not info:
            continue
        views[node] = {
            f"node{member['node_id']}"
            for member in info.get("members", [])
            if member.get("role") == "voter"
        }
    return views


def await_settled_cluster(timeout_s: float) -> str:
    """Wait until every node is a committed voter, and name the leader.

    A leader is not enough to damage a node's Raft store against, because
    `await_leader` returns the moment the bootstrap node calls itself leader --
    which it does at a voter set of `{1}`, while the others are still running
    their initial join.

    Damaging a node the cluster does not list is not a test of anything:
    `run_join_flow` in `src/app.rs` wipes the local store and joins fresh when
    the peer does not list this node, so the damage is deleted rather than
    opened and the node comes back healthy having never read it. Membership is
    the fact that branch turns on, so it is the fact to wait for here.

    Every node must answer, and every answer must name every node a voter. A
    view collected only from reachable nodes would let a node that cannot
    serve its own management API count as settled.
    """
    expected = set(topology.NODES)
    deadline = time.monotonic() + timeout_s
    detail = "nothing was read before the deadline"
    while True:
        found = leaders()
        views = voter_views()
        every_node_answered = set(views) == expected
        every_answer_is_full = all(view == expected for view in views.values())
        if len(found) == 1 and every_node_answered and every_answer_is_full:
            return found[0]
        detail = (
            f"leaders={found or 'none'}, voters="
            f"{ {node: sorted(v) for node, v in views.items()} or 'no node answered' }"
        )
        if time.monotonic() >= deadline:
            break
        time.sleep(2)
    # Name what stalled, not just that something did. A bare convergence
    # timeout reads as a slow cluster; the two occurrences in CI were a node
    # shut down on the fence threshold over a catalog it could not open, and a
    # join that met a populated data directory. Both say so in their own logs.
    stalled = [node for node in sorted(expected) if views.get(node) != expected]
    diagnosis = fp.cluster_stall_report(stalled) if stalled else "every node answered"
    raise fp.FaultPreconditionError(
        f"the cluster was not a full voter set within {timeout_s:g}s ({detail}); "
        f"tearing a node that is not a committed member measures nothing, because "
        f"pgbattery wipes its own store and rejoins rather than opening it. "
        f"What stalled: {diagnosis}"
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


def accepts_a_write(node: str) -> None:
    """Raise unless `node` will actually take a write right now.

    A `DELETE` of a key no run uses. `CREATE TABLE IF NOT EXISTS` against a
    table that already exists is not a write and succeeds on a fenced primary,
    which is how a gate built on it passed and handed the caller a leader that
    refused all 200 baseline rows a moment later. `PostgreSQL` refuses a
    read-only transaction by command type before it runs, so a `DELETE` that
    matches nothing is as good a probe as one that matches — and unlike the
    `INSERT` this first used, it is idempotent, which matters because
    `connect` is autocommit and there is no rollback to undo it with.
    """
    with connect(node) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} (k int PRIMARY KEY)")
        cur.execute(f"DELETE FROM {TABLE} WHERE k = {WRITABILITY_PROBE_KEY}")


def await_writable_leader(timeout_s: float) -> str:
    """A settled cluster whose leader actually accepts a write.

    A settled voter set is not a serving data plane. A node that has just
    rejoined leaves the leader fenced until the sync durability its list
    promises is being delivered again, and every write in that window is
    refused — so a run that waited only for membership got its whole baseline
    refused and reported "no working cluster to damage", which was true but two
    steps later than the fact that explains it.

    Leadership is re-read each round rather than held from the settle: the
    window this waits out is one in which it can still move.
    """
    deadline = time.monotonic() + timeout_s
    await_settled_cluster(timeout_s)
    refusal = "no write was attempted"
    while time.monotonic() < deadline:
        current = leaders()
        if len(current) == 1:
            try:
                accepts_a_write(current[0])
                return current[0]
            except (psycopg.Error, OSError) as e:
                refusal = str(e).strip()
        else:
            refusal = f"the cluster named {current or 'no leader'}"
        time.sleep(2)
    raise fp.FaultPreconditionError(
        f"no leader accepted a write within {timeout_s:g}s ({refusal}), so there was no "
        f"working cluster to damage"
    )


def ensure_table(node: str) -> None:
    with connect(node) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} (k int PRIMARY KEY)")


def write_batch(node: str, keys: range, *, under_fault: bool = False) -> list[int]:
    """Acked writes only. An INSERT that raised was never promised to anyone.

    `under_fault` also tolerates losing the connection itself. Writes driven
    with a tear armed on the leader race the fault by design: the tear kills
    that node's Raft store and the gateway drops the session mid-batch, which
    is the fault working. Without it the baseline batch would swallow the same
    failure and leave the run measuring an empty acked set.
    """
    acked: list[int] = []
    try:
        with connect(node) as conn, conn.cursor() as cur:
            for key in keys:
                try:
                    cur.execute(f"INSERT INTO {TABLE} VALUES (%s)", (key,))
                    acked.append(key)
                except psycopg.Error:
                    continue
    except (psycopg.Error, OSError):
        if not under_fault:
            raise
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
        return HANDLED_REFUSAL
    if any(marker in haystack for marker in REDB_PANIC_MARKERS):
        return PANIC_REFUSAL
    if STORAGE_ERROR_MARKER in haystack:
        return STORAGE_ERROR_REFUSAL
    return UNKNOWN_REFUSAL


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


def tear_once(victim: str, index: int, outcome: Outcome) -> TearReading:
    """Arm one torn write on `victim`'s raft.db, drive traffic, report the tear.

    A tear that did not fire is a real possibility, because the fault is armed
    for the *next* write to that path and a node that has caught up may not
    write again promptly. So is a log that cannot be read: the tear kills the
    Raft store's LazyFS, which takes the container holding the log down with
    it. The two are returned distinctly.

    The writes driven here go into `outcome.acked`. They are issued while a
    tear is armed on a live member of the cluster, which makes them the writes
    most exposed to it; counting only the ones from before the first tear would
    have measured the safest keys in the run.

    """
    fp.arm_torn_write(victim, RAFT_DB, parts=2, persist=(1,), mount=fp.LAZYFS_RAFT)
    lead = await_leader(CONVERGE_TIMEOUT_S)
    base = 1000 * (index + 1)
    outcome.acked.extend(write_batch(lead, range(base, base + 50), under_fault=True))
    time.sleep(3)

    log = fp.exec_when_deliverable(
        victim, fp.lazyfs_log_cmd(log=fp.LAZYFS_RAFT.log), timeout_s=LOG_READ_TIMEOUT_S
    )
    if not log.ok:
        return TearReading(unreadable=log.output.strip() or f"exited {log.rc} in silence")
    records = fp.parse_lazyfs_torn_records(log.stdout, RAFT_DB)
    return TearReading(record=records[-1] if records else None)


def tear_until_page(
    victim: str, *, occurrence: int, outcome: Outcome, target: str
) -> tuple[int, int]:
    """Arm at stepping occurrences until a tear lands off offset 0; `(bytes, offset)`.

    redb writes one header and roughly five pages per commit, so a header hit
    means the very next write is almost certainly a page — walking the
    occurrence forward converges in an attempt or two rather than re-rolling
    the same guess. Every attempt costs a recreate and a re-settle.

    Raises rather than returning a header: a run that only reached offset 0 has
    already been covered by the FIFO form and asserts nothing new.
    """
    for attempt in range(PAGE_TEAR_ATTEMPTS):
        at = occurrence + attempt
        outcome.attempts = attempt + 1
        if attempt:
            fp.run(f"docker compose up -d --force-recreate {victim}")

        fp.arm_config_torn_write(
            victim, RAFT_DB, occurrence=at, parts=2, persist=(1,), mount=fp.LAZYFS_RAFT
        )
        # Re-settle before waiting on the fault. A tear that beat this would
        # have hit a node the cluster had not admitted, which measures nothing.
        lead = await_writable_leader(CONVERGE_TIMEOUT_S)
        if target == "leader":
            make_leader(victim, CONVERGE_TIMEOUT_S)
            lead = victim
        first = BASELINE_WRITES + attempt * PAGE_TEAR_BATCH
        outcome.acked.extend(write_batch(lead, range(first, first + PAGE_TEAR_BATCH)))

        record = fp.await_torn_record(
            victim, RAFT_DB, mount=fp.LAZYFS_RAFT, timeout_s=PAGE_TEAR_TIMEOUT_S
        )
        if record is None:
            raise fp.FaultEffectNotObserved(
                f"no torn write to {RAFT_DB} within {PAGE_TEAR_TIMEOUT_S:g}s at occurrence "
                f"{at}. The store may not have reached that many writes; this run "
                f"asserts nothing about torn pages."
            )
        persisted, offset = record
        outcome.observed.append(ObservedTear(bytes=persisted, offset=offset))
        if offset != 0:
            return persisted, offset

    raise fp.FaultEffectNotObserved(
        f"{PAGE_TEAR_ATTEMPTS} consecutive occurrences from {occurrence} all landed "
        f"on the commit header at offset 0. redb writes about five pages per header, "
        f"so this is not the write pattern this suite was measured against — the "
        f"run asserts nothing about torn pages rather than claiming the header case."
    )


def tear_a_page(*, target: str, occurrence: int) -> Outcome:
    """Tear a committed btree page rather than the commit header.

    The FIFO form cannot: LazyFS pins it to occurrence 1, and redb opens every
    commit with its 320-byte header, so the next write after arming is always
    that header. Measured on a running cluster, pages are 85% of the writes to
    `raft.db` and go through FUSE like everything else — the header-only result
    was never about visibility.

    So the fault is baked into the victim's LazyFS config, which does honour
    `occurrence`, and the victim is restarted to load it. `occurrence` counts
    from that mount, so it has to clear what the node writes coming back up;
    the cluster is re-settled first and the tear is only then waited on, which
    means a fault that fired too early shows up as a settle failure rather than
    as a tear of something that was not yet a member.

    That restart is also why a leader-targeted run moves leadership back onto
    the victim before waiting: restarting the leader hands leadership away, and
    tearing a node that stopped being the leader is the follower case under
    another name.
    """
    outcome = Outcome()
    lead = await_writable_leader(CONVERGE_TIMEOUT_S)
    outcome.acked = write_batch(lead, range(BASELINE_WRITES))
    if len(outcome.acked) < MIN_BASELINE_ACKED:
        raise fp.FaultPreconditionError(
            f"only {len(outcome.acked)} of {BASELINE_WRITES} baseline writes were acked "
            f"before the tear, so there was no working cluster to damage"
        )

    victim = lead if target == "leader" else next(n for n in topology.NODES if n != lead)
    outcome.victim = victim
    outcome.target = target

    try:
        persisted, offset = tear_until_page(
            victim, occurrence=occurrence, outcome=outcome, target=target
        )
    finally:
        # One recreate both restarts the victim onto the damaged store and
        # disarms it: `/etc/lazyfs-raft.toml` is baked into the image, so a
        # fresh container carries no injection. It runs on the failure paths
        # too, because a fault left armed fires into whatever runs next.
        restarted = fp.run(f"docker compose up -d --force-recreate {victim}")
    if not restarted.ok:
        raise fp.FaultInjectionError(
            f"{victim}: could not restart onto the torn store: {restarted.output}"
        )

    outcome.tears = 1
    outcome.torn_bytes.append(persisted)
    outcome.torn_offsets.append(offset)
    outcome.victim_healthy, outcome.victim_refused = await_victim_settled(
        victim, CONVERGE_TIMEOUT_S
    )
    outcome.refusal = refusal_shape(victim) if outcome.victim_refused else ""
    lead = await_leader(CONVERGE_TIMEOUT_S)
    outcome.surviving = read_back(lead)
    outcome.leaders_after = leaders()
    return outcome


def run_tears(*, tears: int, target: str, min_torn_bytes: int, max_attempts: int) -> Outcome:
    """Tear `target`'s Raft store `tears` times, retrying for a big enough tear.

    `min_torn_bytes` is the floor below which a tear is too small to have
    damaged anything, and an attempt under it is retried rather than counted.
    What each attempt actually tore is recorded either way, because "redb never
    writes anything larger in this workload" is itself an answer — and it is
    the one this store gives. Every write LazyFS intercepts on `raft.db` is the
    320-byte header at offset 0, so a `parts=2` tear persists 160 bytes of it
    and nothing here reaches a btree page. That makes this a torn-header test,
    which is a real one — the header is what redb reads to find anything else.

    Why it is only the header is the arming window, not visibility: redb 4 has
    no mmap anywhere in its sources and writes through `pwrite`, so a data page
    reaches FUSE exactly as the header does. LazyFS pins a FIFO torn-op to
    occurrence 1, so it fires on the very next write to the path, and every
    redb commit opens with the header. :func:`tear_a_page` is the other
    structure, and it pays a restart to select a later occurrence.
    """
    outcome = Outcome()
    lead = await_writable_leader(CONVERGE_TIMEOUT_S)
    outcome.acked = write_batch(lead, range(BASELINE_WRITES))
    if len(outcome.acked) < MIN_BASELINE_ACKED:
        raise fp.FaultPreconditionError(
            f"only {len(outcome.acked)} of {BASELINE_WRITES} baseline writes were acked "
            f"before any tear, so there was no working cluster to damage. `lost` is "
            f"`acked - surviving`, so the run would report no acked write lost for want "
            f"of anything to lose."
        )

    # A follower keeps redb's behaviour separate from a promotion happening at
    # the same moment. The leader is the harder case: it is the node whose vote
    # and log the cluster is currently relying on.
    victim = lead if target == "leader" else next(n for n in topology.NODES if n != lead)
    outcome.victim = victim
    outcome.target = target

    attempts = 0
    while outcome.tears < tears and attempts < max_attempts:
        attempts += 1
        reading = tear_once(victim, attempts, outcome)
        if reading.unreadable:
            outcome.unreadable.append(reading.unreadable)
        elif reading.record is not None:
            persisted, offset = reading.record
            outcome.observed.append(ObservedTear(bytes=persisted, offset=offset))
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
    await_settled_cluster(CONVERGE_TIMEOUT_S)
    # A settled voter set says nothing about whether the nodes can receive a
    # fault yet. Without this the real run reached a node still restarting its
    # LazyFS and failed writing to a control FIFO that did not exist.
    await_fault_readiness()


def mangle_script(path: str) -> str:
    """Overwrite the head of `path`, printing its size and the marker count.

    Refuses a path that does not already hold a store. `dd of=` creates what it
    cannot find, so against a node whose join has not yet reached redb this
    would write the marker into a file it invented and read it straight back --
    reporting a damaged store where there was no store at all.

    A repeating marker rather than /dev/urandom, so the damage can be read
    back. Random bytes are indistinguishable from redb's own afterwards.

    Free of single quotes, because the caller passes this to `docker compose
    run` inside a single-quoted `-c` argument and one here would end that
    argument early.
    """
    return (
        f'if [ ! -s {path} ]; then echo "no raft.db to damage at {path}"; exit 1; fi; '
        f"wc -c < {path}; "
        f"yes {MANGLE_MARKER} | head -c {MANGLE_BYTES} | "
        f"dd of={path} bs={MANGLE_BYTES} count=1 conv=notrunc 2>/dev/null && "
        f"head -c {MANGLE_BYTES} {path} | grep -ac {MANGLE_MARKER}"
    )


def mangle_raft_db(victim: str) -> None:
    """Destroy the head of `victim`'s raft.db, with nothing left to undo it.

    Done with the node stopped, through a throwaway container on the same
    volume, because both things that hold the store put it back otherwise:
    redb rewrites its header on the next commit, and LazyFS flushes its own
    cached copy over whatever is written to the backing store behind it. A
    `dd` that exits 0 against a live node is not a damaged store.
    """
    stopped = fp.run(f"docker compose stop {victim}", MANGLE_TIMEOUT_S)
    if not stopped.ok:
        raise fp.FaultPreconditionError(f"could not stop {victim}: {stopped.output}")

    mangled = fp.run(
        f"docker compose run --rm --no-deps --entrypoint sh {victim} -c '{mangle_script(RAFT_DB)}'",
        MANGLE_TIMEOUT_S,
    )
    if not mangled.ok or mangled.stdout.strip().splitlines()[-1:] in ([], ["0"]):
        raise fp.FaultPreconditionError(
            f"{victim}: raft.db does not carry the overwrite after it was written with "
            f"the node stopped: {mangled.output.strip() or 'no output'}. The inversion "
            f"cannot run, because nothing would be measuring a damaged store."
        )


def describe_store(victim: str) -> str:
    """Say what became of the overwrite, so a surviving node can be explained.

    Two very different failures reach the same place. If the marker is gone the
    inversion is aimed at the wrong artifact -- something restores or bypasses
    the file this damaged, and no amount of overwriting it would ever be seen.
    If it is still there, the store really is damaged and pgbattery opened it
    anyway, which is the far more serious reading and not a harness bug at all.
    """
    marker = fp.exec_when_deliverable(
        victim, f"head -c {MANGLE_BYTES} {RAFT_DB} | grep -ac {MANGLE_MARKER} || true"
    ).stdout.strip()
    size = fp.exec_when_deliverable(
        victim, f"stat -c %s {RAFT_DB} 2>/dev/null || echo missing"
    ).stdout.strip()
    survived = marker not in ("", "0")
    reading = (
        "nothing checks the store before opening it"
        if survived
        else "the store is restored on startup, or the damage never reached the file "
        "pgbattery opens"
    )
    return (
        f"After the restart the overwrite is {'STILL PRESENT' if survived else 'GONE'} "
        f"({marker or 'no'} marker lines, file {size} bytes), so {reading}."
    )


def prove_oracle() -> None:
    """Show that damage to raft.db is noticed at all.

    Overwrites the head of the store -- past what any single torn write does --
    and requires the node to stop rather than run on it.
    Most tears land in uncommitted pages and are correctly a non-event, so
    without this a green run below could not be told apart from a fault that
    injected nothing.
    """
    lead = await_settled_cluster(CONVERGE_TIMEOUT_S)
    victim = next(n for n in topology.NODES if n != lead)

    mangle_raft_db(victim)

    fp.run(f"docker compose up -d --force-recreate {victim}")
    healthy, refused = await_victim_settled(victim, REFUSAL_TIMEOUT_S)

    if not refused:
        raise OracleNotProven(
            f"{victim} kept running on a raft.db this overwrote (healthy={healthy}). "
            f"{describe_store(victim)} A green run below would be measuring an "
            f"observable that damage never reaches."
        )
    print(f"oracle proven: {victim} would not run on a corrupted raft.db")
    reset_cluster()
    print("cluster rebuilt for the real run\n")


def unobserved_fault_message(outcome: Outcome, *, min_torn_bytes: int) -> str:
    """Why the run establishes nothing, distinguishing the two ways that happens."""
    largest = max((w["bytes"] for w in outcome.observed), default=0)
    head = (
        f"no torn write met the bar in {outcome.attempts} attempts, so this run "
        f"asserts nothing about torn-write handling. Largest tear seen was "
        f"{largest} bytes against a --min-torn-bytes of {min_torn_bytes}."
    )
    if outcome.unreadable:
        return (
            f"{head} On {len(outcome.unreadable)} of those attempts the LazyFS log "
            f"could not be read, so the run cannot say whether the fault fired: "
            f"{outcome.unreadable[-1]}"
        )
    return (
        f"{head} The fault never fired -- it arms for the *next* write to raft.db, "
        f"and a caught-up node may not write again promptly -- or redb writes "
        f"nothing that large in this workload, which is worth recording rather "
        f"than working around."
    )


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
        "--structure",
        choices=("header", "page"),
        default="header",
        help="which redb structure to tear. `header` arms over the FIFO, which "
        "fires on the next write and so always lands on the 320-byte commit "
        "header. `page` bakes the fault into LazyFS's config at an occurrence "
        "past the node's own restart, which is the only way to reach a btree "
        "page — see HARDENING H-48.",
    )
    parser.add_argument(
        "--occurrence",
        type=int,
        default=PAGE_TEAR_OCCURRENCE,
        help="for --structure page: which write to raft.db to tear, counted "
        "from the restarted LazyFS's mount",
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

    outcome = (
        tear_a_page(target=args.target, occurrence=args.occurrence)
        if args.structure == "page"
        else run_tears(
            tears=args.tears,
            target=args.target,
            min_torn_bytes=args.min_torn_bytes,
            max_attempts=args.attempts or args.tears * 4,
        )
    )
    print(json.dumps(outcome.as_json(), indent=2))

    if not outcome.damage_established:
        raise fp.FaultEffectNotObserved(
            unobserved_fault_message(outcome, min_torn_bytes=args.min_torn_bytes)
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
    landed = (
        f"{outcome.tears} torn writes to raft.db"
        if outcome.tears
        else "a torn write whose size died with the LazyFS that logged it"
    )
    print(f"{landed}; the node {how}; no acked write lost")
    if outcome.victim_refused:
        print(f"  refusal shape -- {outcome.refusal}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RaftDurabilityViolation, OracleNotProven, fp.FaultError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
