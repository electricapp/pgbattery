#!/usr/bin/env -S uv run --project testing python
"""ENOSPC at WAL-segment allocation — H-26, contract W1.

A blanket "fill the volume" test proves little: it exhausts the filesystem at
whatever point the filler happens to reach, which is usually somewhere dull.
The interesting failure is narrower — PostgreSQL rolling to a *new WAL
segment* and finding no room for it. That is where a database either fences
cleanly or starts acknowledging commits it cannot persist.

`fault_primitives.disk_full_during_wal` aims there: it drives free space down
to half a WAL segment, so ordinary small writes still succeed while segment
allocation cannot, and it proves the aim by attempting a segment-sized
allocation and requiring it to fail. This harness is what surrounds that fault
with a claim.

WHAT IS ASSERTED

    W1  Every write acknowledged before or during the full window is still
        readable afterwards. This is the whole point: a full disk is allowed to
        refuse writes, and is never allowed to lose one it accepted.

    The fault must have a visible effect. If a leader whose filesystem cannot
    allocate another WAL segment goes on acknowledging writes indefinitely with
    leadership unmoved, this run measured nothing and says so rather than
    passing. That is the difference between "the cluster tolerated the fault"
    and "the fault never reached anything".

    Recovery. Releasing the filler must return the cluster to accepting writes
    without restarting anything.

WHAT IS NOT ASSERTED

    Where leadership ends up. Whether a disk-full leader sheds leadership or
    holds it while refusing writes is recorded in the report, not required.
    Both are defensible; asserting one before measuring which pgbattery does
    would be writing the test around a guess.

    L1 comes from `dual_writability_prober.py`, running concurrently across all
    three internal PG ports for the whole fill-and-recovery window. Leader
    observations are still collected but never asserted on: two distinct leader
    names across the window are a leadership *transition*, not two leaders at
    once, and Raft does not promise every node learns of a new leader at the
    same instant. L1 is about two nodes being simultaneously *writable*, which
    only concurrent real writes answer. A violation fails the run; an oracle
    that saw too little to conclude leaves L1 out of the reported contracts
    rather than claiming it.

NO RESTARTS, DELIBERATELY

    This suite needs the bounded volume variant, which is a tmpfs. Docker
    instantiates a tmpfs volume's mount per container start, so restarting a
    node brings it back with an EMPTY /var/lib/postgresql — node1 would
    bootstrap a fresh cluster with a fresh Raft log and every assertion here
    would pass against a database that had thrown away the evidence. So the
    fault is released rather than crashed out of, and recovery is measured
    without a restart. Run it against a cluster started with
    PGBATTERY_STATE_SUFFIX=_bounded; the primitive refuses an unbounded
    filesystem rather than filling a host disk.

THE INVERSION

    `--prove-oracle` runs the identical sequence with no fill and requires the
    effect signal to be ABSENT. That is what makes the signal attributable: if
    writes fail and leadership moves in a cluster nobody touched, then observing
    the same thing under a full disk says nothing about disk exhaustion. A
    suite that cannot tell its fault apart from its own workload is not
    evidence, and this refuses to report green until it has shown it can.
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
from dual_writability_prober import (
    DualWritabilityProber,
    ProberConfig,
    ProbeReport,
    ProberError,
    Verdict,
)

PG_USER: Final[str] = "postgres"
PG_DBNAME: Final[str] = "postgres"

TABLE: Final[str] = "wal_enospc_ledger"

BOOTSTRAP_NODE: Final[str] = topology.NODES[0]
"""The node started with ``--bootstrap``.

Never the victim. Bounded volumes are tmpfs, so a node that restarts comes back
with an empty ``/var/lib/postgresql`` — and this one would then bootstrap a
brand-new cluster with a fresh Raft log rather than rejoin the old one. Every
assertion here would pass against a database that had discarded the evidence,
and the run would read as catastrophic data loss caused by disk exhaustion when
in fact the substrate destroyed itself. Any other node comes back as a follower,
re-clones from the leader, and rejoins, which is a legitimate recovery path.

Filling the *leader* is the point of the exercise — that is where WAL is
generated — so leadership is moved off this node first rather than the victim
being demoted to a follower."""

PAYLOAD_BYTES: Final[int] = 256 * 1024
"""Payload per row during the full window.

Large enough that a couple of hundred rows generate several segments' worth of
WAL, so the segment roll happens inside the window. Bare integer keys do not:
a hundred tiny inserts fit inside the *current* segment, PostgreSQL never has
to allocate the next one, and every write is acknowledged with the disk full
and the fault correctly aimed — a green that measured nothing.

Every row is still a tracked key, so the same writes that force the allocation
are the ones W1 is asserted over."""

CONVERGE_TIMEOUT_S: Final[float] = 180.0
"""Budget for a writable leader to appear. Covers an election plus promotion."""

RECOVERY_TIMEOUT_S: Final[float] = 180.0
"""Budget for writes to resume after the filler is released. Generous: if
leadership moved, this includes the new leader's promotion."""

SETTLE_S: Final[float] = 5.0
"""How long to keep observing after the fill before concluding nothing
happened. Election plus lease expiry is the slowest reaction being waited on;
shorter than that and 'no effect' would just mean 'asked too early'."""


PROBE_ROUND_PERIOD_S: Final[float] = 1.0
"""How often the L1 oracle races a write at all three nodes at once.

Fast enough to land several rounds inside the window between the leader's disk
filling and its successor being elected, which is the only interval in which
two nodes could plausibly both accept."""


class EnospcViolation(Exception):
    """An acknowledged write did not survive disk exhaustion. Contract W1."""


class DualWritability(Exception):
    """Two nodes accepted a write at once under ENOSPC. Contract L1."""


class VacuousRun(Exception):
    """The fault produced no observable effect, so nothing was measured."""


class OracleNotProven(Exception):
    """The inversion showed the same effect with no fault, so it is not
    attributable to disk exhaustion."""


class FillJson(TypedDict):
    """Serialised `FillDetail`."""

    container: str
    filled_kb: int
    wal_segment_bytes: int
    avail_kb_after: int


class ProbeJson(TypedDict):
    """Serialised `ProbeSummary`."""

    verdict: str
    rounds: int
    violations: int
    rounds_by_acceptance_count: dict[int, int]
    indeterminate_rate: float
    transport: str
    headline: str


class PhaseJson(TypedDict):
    """One write phase, counted rather than enumerated."""

    name: str
    acked: int
    unacked: int


class OutcomeJson(TypedDict):
    """The run report. This is the artifact CI keeps, so the keys are a
    contract with anything reading them after the fact."""

    acked: int
    surviving: int
    lost_acked: int
    lost_keys: list[int]
    writes_refused: int
    leader_before: str
    leaders_during: list[str]
    leader_after: str
    leadership_moved: bool
    recovered: bool
    phases: list[PhaseJson]
    fill: FillJson | None
    l1: ProbeJson
    contracts: list[str]


class RunJson(TypedDict, total=False):
    """What `main` prints. Every key is optional: the control run is absent
    without `--prove-oracle`, and a failure before the fault leaves both out."""

    control: OutcomeJson
    enospc: OutcomeJson
    error: str


@dataclass(slots=True)
class Phase:
    """Writes attempted in one phase of the run."""

    name: str
    acked: list[int] = field(default_factory=list)
    unacked: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def as_json(self) -> PhaseJson:
        return PhaseJson(name=self.name, acked=len(self.acked), unacked=len(self.unacked))


@dataclass(frozen=True, slots=True)
class FillDetail:
    """What the filler did, as measured rather than as requested."""

    container: str
    filled_kb: int
    wal_segment_bytes: int
    avail_kb_after: int

    def as_json(self) -> FillJson:
        return FillJson(
            container=self.container,
            filled_kb=self.filled_kb,
            wal_segment_bytes=self.wal_segment_bytes,
            avail_kb_after=self.avail_kb_after,
        )


@dataclass(frozen=True, slots=True)
class ProbeSummary:
    """The L1 oracle's finding, reduced to what the run report carries."""

    verdict: str
    rounds: int
    violations: int
    rounds_by_acceptance_count: dict[int, int]
    indeterminate_rate: float
    transport: str
    headline: str

    @classmethod
    def of(cls, report: ProbeReport | None) -> ProbeSummary:
        if report is None:
            return cls("NOT RUN", 0, 0, {}, 0.0, "", "the L1 oracle did not run")
        return cls(
            verdict=str(report.verdict),
            rounds=report.total_rounds,
            violations=len(report.violations),
            rounds_by_acceptance_count=report.rounds_by_acceptance_count,
            indeterminate_rate=round(report.indeterminate_rate, 4),
            transport=report.transport,
            headline=report.headline,
        )

    def as_json(self) -> ProbeJson:
        return ProbeJson(
            verdict=self.verdict,
            rounds=self.rounds,
            violations=self.violations,
            rounds_by_acceptance_count=self.rounds_by_acceptance_count,
            indeterminate_rate=self.indeterminate_rate,
            transport=self.transport,
            headline=self.headline,
        )


@dataclass(slots=True)
class Outcome:
    """Everything the run observed, whether or not it was asserted on."""

    phases: list[Phase] = field(default_factory=list)
    surviving: set[int] = field(default_factory=set)
    leader_before: str = ""
    leaders_during: list[str] = field(default_factory=list)
    leader_after: str = ""
    leadership_moved: bool = False
    writes_refused: int = 0
    recovered: bool = False
    fill_detail: FillDetail | None = None
    probe: ProbeReport | None = None

    @property
    def acked(self) -> list[int]:
        return sorted(k for phase in self.phases for k in phase.acked)

    @property
    def lost(self) -> list[int]:
        """Acked writes absent at the end. Non-empty is a W1 violation."""
        return sorted(k for k in self.acked if k not in self.surviving)

    @property
    def had_effect(self) -> bool:
        """Whether the fault changed anything observable."""
        return self.writes_refused > 0 or self.leadership_moved

    @property
    def l1_established(self) -> bool:
        """Whether the oracle saw enough to claim L1, not merely fail to break it."""
        return self.probe is not None and self.probe.verdict is Verdict.PASS

    def report(self) -> OutcomeJson:
        return OutcomeJson(
            acked=len(self.acked),
            surviving=len(self.surviving),
            lost_acked=len(self.lost),
            lost_keys=self.lost[:20],
            writes_refused=self.writes_refused,
            leader_before=self.leader_before,
            leaders_during=self.leaders_during,
            leader_after=self.leader_after,
            leadership_moved=self.leadership_moved,
            recovered=self.recovered,
            phases=[p.as_json() for p in self.phases],
            fill=self.fill_detail.as_json() if self.fill_detail else None,
            l1=ProbeSummary.of(self.probe).as_json(),
            contracts=["W1", "L1"] if self.l1_established else ["W1"],
        )


GATEWAY_PORT_BY_NODE: Final[dict[str, int]] = dict(
    zip(topology.NODES, topology.GATEWAY_PORTS, strict=True)
)
"""Gateway port per voter service, positional in `topology`. `strict=True` makes
a drift in either list an error rather than a short mapping that would leave a
node unaddressed and its writes uncounted."""


def connect_gateway(node: str) -> psycopg.Connection[Any]:
    """Client connection through `node`'s gateway, which routes to the leader."""
    conn: psycopg.Connection[Any] = psycopg.connect(
        host="127.0.0.1",
        port=GATEWAY_PORT_BY_NODE[node],
        user=PG_USER,
        dbname=PG_DBNAME,
        connect_timeout=10,
        autocommit=True,
    )
    return conn


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

    A list rather than one value: more than one distinct answer is the L1
    finding, so collapsing it here would discard exactly what is being checked.
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


def await_writable_leader(timeout_s: float, *, via: str) -> str:
    """Wait until a write succeeds through `via`, and return the leader's name.

    Writability, not the management API's opinion: a node can be elected before
    its PostgreSQL is promoted, and this suite only cares about the point at
    which the cluster actually takes writes.
    """
    deadline = time.monotonic() + timeout_s
    last = "no attempt made"
    while time.monotonic() < deadline:
        try:
            with connect_gateway(via) as conn, conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery()")
                row = cur.fetchone()
                if row is not None and row[0] is False:
                    # Out of recovery is not writable. A promoted primary stays
                    # read-only until the lease tick recovers writes, and every
                    # statement after this one is a write — so prove the write
                    # path rather than the role. Rolled back, and a temp table
                    # is session-local, so this leaves nothing behind.
                    cur.execute("CREATE TEMP TABLE wal_enospc_writable_probe(x int)")
                    conn.rollback()
                    found = leaders()
                    if len(found) == 1:
                        return found[0]
        except (psycopg.Error, OSError) as exc:
            last = str(exc).strip()
        time.sleep(2.0)
    # The last connection error says the gateway would not take a write and
    # nothing about which node could not serve one. A CI run that timed out here
    # left only that line, and no way to tell a node still restarting after the
    # disk filled from one that had stopped for good.
    raise TimeoutError(
        f"no writable leader within {timeout_s:g}s; last error: {last}. "
        f"Cluster: {fp.cluster_stall_report(list(topology.NODES))}"
    )


def ensure_table(via: str) -> None:
    with connect_gateway(via) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(
            f"CREATE TABLE {TABLE} (k int PRIMARY KEY, written_at timestamptz, payload text)"
        )
        # PostgreSQL would otherwise compress or move a large payload out of
        # line, and a TOASTed value generates a different (smaller) amount of
        # WAL than the bytes handed to it. The whole point of the payload is
        # its WAL volume, so keep it inline and uncompressed.
        cur.execute(f"ALTER TABLE {TABLE} ALTER COLUMN payload SET STORAGE EXTERNAL")


def is_synced(node: str) -> bool:
    """Whether the leader reports `node` caught up.

    A lagging node is not a candidate: openraft declines to hand leadership to
    one, and a suite that asks anyway waits out its whole timeout for a
    transfer that was never going to happen. This suite creates exactly that
    situation itself — a victim restarts onto an empty tmpfs and spends the
    next minutes re-cloning — so it has to check rather than assume.
    """
    info = mgmt(BOOTSTRAP_NODE, f"/cluster/node/{node.removeprefix('node')}/lag")
    return bool(info and info.get("is_synced"))


def request_transfer(target: str) -> tuple[bool, str]:
    """Ask the bootstrap node to hand leadership to `target`.

    Returns whether the API *accepted* it, with its message. curl's exit status
    answers "did the request go out", which is not the same question: a 200
    carrying ``success: false``, or any error status with a body, still exits 0.
    Reading only the exit status is how a transfer that never happened turns
    into a timeout blamed on the cluster.
    """
    port = topology.MGMT_PORTS[BOOTSTRAP_NODE]
    token = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")
    result = fp.run(
        f"curl -s -w '\\n%{{http_code}}' -X POST --max-time 10 "
        f"-H 'x-pgbattery-token: {token}' "
        f"http://127.0.0.1:{port}/api/v1/cluster/transfer-leadership/"
        f"{target.removeprefix('node')}"
    )
    if not result.ok:
        return False, f"curl failed: {result.output.strip()}"
    lines = result.stdout.strip().splitlines()
    status = lines[-1] if lines else ""
    body = "\n".join(lines[:-1])
    if status != "200":
        return False, f"HTTP {status}: {body.strip()[:200]}"
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, f"unparseable response: {body.strip()[:200]}"
    if not parsed.get("success"):
        return False, str(parsed.get("message", body))[:200]
    return True, str(parsed.get("message", ""))


def transfer_leadership_off_bootstrap(timeout_s: float) -> str:
    """Move leadership to a node that is safe to fill, and return its name.

    See :data:`BOOTSTRAP_NODE` for why the bootstrap node must never be the
    victim. If leadership is already elsewhere this is a no-op.
    """
    candidates = [n for n in topology.NODES if n != BOOTSTRAP_NODE]
    if not candidates:
        raise fp.FaultPreconditionError("need at least two nodes to run this suite")

    attempts: list[str] = []
    deadline = time.monotonic() + timeout_s
    while True:
        # Read the goal first, every round. A refused transfer is not a reason
        # to keep asking: the bootstrap node refuses with `421 Not the leader`
        # precisely when leadership has already moved, which is the state this
        # is waiting for. Re-reading only after an *accepted* transfer meant
        # that answer was discarded and the whole timeout burned.
        found = leaders()
        if len(found) == 1 and found[0] != BOOTSTRAP_NODE:
            return found[0]
        if time.monotonic() >= deadline:
            break
        for target in candidates:
            if not is_synced(target):
                attempts.append(f"{target}: not caught up")
                continue
            accepted, detail = request_transfer(target)
            attempts.append(f"{target}: {'accepted' if accepted else 'refused'} — {detail}")
            if accepted:
                break
        time.sleep(3.0)

    raise fp.FaultPreconditionError(
        f"leadership did not move off {BOOTSTRAP_NODE} within {timeout_s:g}s; filling it "
        f"would let it restart onto an empty tmpfs and bootstrap a fresh cluster. "
        f"Attempts: {'; '.join(attempts[-6:]) if attempts else 'none made before the deadline'}"
    )


def write_batch(via: str, keys: range, *, name: str, payload_bytes: int = 0) -> Phase:
    """Attempt one transaction per key, recording exactly what COMMIT returned.

    A key counts as acked only once COMMIT has returned. That is the promise W1
    is about and the only claim made on the cluster here. Connection setup is
    per batch rather than per key so a refused write is the server refusing,
    not the client failing to arrive.
    """
    phase = Phase(name=name)
    payload = "x" * payload_bytes if payload_bytes else ""
    try:
        conn = connect_gateway(via)
    except (psycopg.Error, OSError) as exc:
        phase.unacked.extend(keys)
        phase.errors.append(f"connect: {str(exc).strip()}")
        return phase
    try:
        for k in keys:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO {TABLE} (k, written_at, payload) VALUES (%s, now(), %s)",
                        (k, payload),
                    )
            except (psycopg.Error, OSError) as exc:
                phase.unacked.append(k)
                if len(phase.errors) < 5:
                    phase.errors.append(str(exc).strip()[:200])
                # A refused write usually means the connection is now unusable
                # (PostgreSQL panicking on ENOSPC takes the backend with it).
                # Reconnect so the remaining keys are measured against the
                # cluster rather than against a dead socket.
                try:
                    conn.close()
                    conn = connect_gateway(via)
                except (psycopg.Error, OSError):
                    phase.unacked.extend(k2 for k2 in keys if k2 > k)
                    break
                continue
            phase.acked.append(k)
    finally:
        conn.close()
    return phase


def read_surviving(via: str) -> set[int]:
    with connect_gateway(via) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT k FROM {TABLE}")
        return {int(row[0]) for row in cur.fetchall()}


def other_than(node: str) -> str:
    """A node that is not `node`, to use as the client's entry point.

    The client must not enter through the gateway of the node whose filesystem
    is being filled: that measures one node's liveness, where the contract is
    about the cluster's. Entering elsewhere lets routing follow leadership if
    it moves, which is the client's real experience.
    """
    for candidate in topology.NODES:
        if candidate != node:
            return candidate
    raise fp.FaultPreconditionError("need at least two nodes to run this suite")


def watch_leaders(outcome: Outcome, seconds: float) -> None:
    """Record every node any peer calls leader over `seconds`.

    Runs after the writes because an election the fill triggered may not have
    resolved by the time the last INSERT returned.
    """
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for name in leaders():
            if name not in outcome.leaders_during:
                outcome.leaders_during.append(name)
        time.sleep(1.0)


def run_case(*, writes: int, fill: bool) -> Outcome:
    """One pass. `fill=False` is the inversion: identical, minus the fault."""
    outcome = Outcome()

    await_writable_leader(CONVERGE_TIMEOUT_S, via=BOOTSTRAP_NODE)
    victim = transfer_leadership_off_bootstrap(CONVERGE_TIMEOUT_S)
    # The client must not enter through the victim's own gateway: that measures
    # one node's liveness where the contract is about the cluster's. Entering
    # elsewhere lets routing follow leadership if it moves.
    via = other_than(victim)
    await_writable_leader(CONVERGE_TIMEOUT_S, via=via)
    outcome.leader_before = victim

    ensure_table(via)
    outcome.phases.append(write_batch(via, range(1, writes + 1), name="before"))

    during = range(writes + 1, 2 * writes + 1)
    # The oracle spans the fill *and* the recovery. The victim rejoining after
    # space is freed is as good a chance for two writable nodes as the election
    # that replaced it, and stopping at the filler's release would miss it.
    with DualWritabilityProber(ProberConfig(round_period_s=PROBE_ROUND_PERIOD_S)) as prober:
        prober.start()
        if fill:
            with fp.disk_full_during_wal(victim) as handle:
                outcome.fill_detail = FillDetail(
                    container=handle.container,
                    filled_kb=handle.filled_kb,
                    wal_segment_bytes=handle.wal_segment_bytes,
                    avail_kb_after=handle.usage_after.avail_kb,
                )
                outcome.phases.append(
                    write_batch(via, during, name="during-fill", payload_bytes=PAYLOAD_BYTES)
                )
                watch_leaders(outcome, SETTLE_S)
        else:
            outcome.phases.append(
                write_batch(via, during, name="during-nofill", payload_bytes=PAYLOAD_BYTES)
            )
            watch_leaders(outcome, SETTLE_S)

        outcome.writes_refused = sum(len(p.unacked) for p in outcome.phases if p.name != "before")

        # The filler is released by here. Recovery is measured without restarting
        # anything: the victim may have restarted itself, which is a legitimate way
        # to fence, but nothing here restarts it deliberately.
        outcome.leader_after = await_writable_leader(RECOVERY_TIMEOUT_S, via=via)
        outcome.recovered = True
        outcome.probe = prober.stop()

    # Measured over the settle window *and* the end state. A victim that dies
    # under the fault is still nominally leader for as long as its lease runs,
    # so sampling only during the window reports "leadership never moved" for a
    # node that has in fact been replaced by the time writes resume.
    observed = [*outcome.leaders_during, outcome.leader_after]
    outcome.leadership_moved = any(name != victim for name in observed)

    outcome.surviving = read_surviving(via)
    return outcome


def assert_single_writability(outcome: Outcome, label: str) -> None:
    """L1, from concurrent real writes rather than from leader observations.

    Asserted whenever the oracle saw a violation, including on the control run:
    two nodes accepting at once is a violation wherever it happens.
    """
    probe = outcome.probe
    if probe is None or not probe.violations:
        return
    raise DualWritability(
        f"L1 ({label}): {len(probe.violations)} of {probe.total_rounds} probe "
        f"rounds saw two or more nodes accept a write. {probe.headline}"
    )


def assert_contracts(outcome: Outcome) -> None:
    """Everything that must hold when the disk filled."""
    assert_single_writability(outcome, "enospc")

    if outcome.lost:
        raise EnospcViolation(
            f"W1: {len(outcome.lost)} acknowledged write(s) did not survive disk "
            f"exhaustion: {outcome.lost[:20]}. A full disk may refuse a write; it "
            f"may never lose one it accepted."
        )

    if not outcome.had_effect:
        raise VacuousRun(
            "the leader's filesystem could not allocate another WAL segment, yet "
            "every write was still acknowledged and leadership never moved. Either "
            "the fill did not reach PostgreSQL's WAL, or writes are being "
            "acknowledged that cannot be persisted. Nothing was measured."
        )

    if not outcome.recovered:
        raise EnospcViolation("the cluster did not take writes again after space was freed")


def prove_oracle(writes: int) -> Outcome:
    """Run without the fault and require the effect signal to be absent."""
    control = run_case(writes=writes, fill=False)
    assert_single_writability(control, "control")
    if control.had_effect:
        raise OracleNotProven(
            f"a run with no fill also showed the effect this suite attributes to "
            f"disk exhaustion — {control.writes_refused} write(s) refused, "
            f"leadership_moved={control.leadership_moved}. The signal is therefore "
            f"not attributable to ENOSPC, and a green run would prove nothing. "
            f"Investigate the cluster before trusting this suite."
        )
    return control


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--writes",
        type=int,
        default=200,
        help="transactions per phase (default: 200)",
    )
    parser.add_argument(
        "--prove-oracle",
        action="store_true",
        help="require a no-fill run to show no effect before believing a green run",
    )
    args = parser.parse_args()

    report: RunJson = {}
    try:
        if args.prove_oracle:
            control = prove_oracle(args.writes)
            report["control"] = control.report()
        outcome = run_case(writes=args.writes, fill=True)
        report["enospc"] = outcome.report()
        assert_contracts(outcome)
    except (
        EnospcViolation,
        DualWritability,
        VacuousRun,
        OracleNotProven,
        ProberError,
        fp.FaultError,
        TimeoutError,
    ) as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(json.dumps(report, indent=2))
        print(f"\nFAIL: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
