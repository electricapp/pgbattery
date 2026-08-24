#!/usr/bin/env -S uv run --project testing python
"""RW-11: the replicated sync mode must not be looser than the live GUC.

Every voter's election gate picks its LSN catch-up threshold from the
replicated `ClusterState::sync_replication_active`, while the durability it is
supposed to describe is the leader's live `synchronous_standby_names`. When the
flag says async and the GUC says sync, the gate admits a candidate up to the
loose threshold behind — orders of magnitude further than the tight one — while
sync replication was acknowledging writes that candidate does not hold.

The disagreement is one-directional by design. `check_and_update_sync_standbys`
commits the flag from the GUC it just set rather than from PostgreSQL
confirming a standby is `sync`, because committing `active=true` only tightens
the gate. So:

  flag true, GUC empty      tighter than reality. Safe, and the deliberate
                            ordering; not a finding
  flag false, GUC non-empty looser than reality. This is RW-11
  flag None, either way     no transition observed, which selects the tight
                            threshold; safe

What bounds it is `REPLICA_CHECK_INTERVAL_MS`: the flag is re-committed on the
next reconcile after a transition, and `last_committed_sync_mode` resets to
`None` when a node stops leading, so a new leader re-commits from its own view
on its first tick. A window of one or two intervals is the design; a window
that does not close is the defect.

Two phases, because there are two ways in:

  heal      the leader falls back to async with its standbys' replication
            severed, then gets them back. The same leader must re-commit.
  failover  the fallback is still in force when leadership moves. The new
            leader arms a sync list at promotion and must re-commit from its
            own view rather than inheriting the deposed leader's.

`--prove-oracle` runs the checker over a series that holds the disagreement open
past the budget and requires it to report a violation, because a checker that
has only ever seen healthy input is a checker nobody has watched fail.
"""

from __future__ import annotations

import itertools
import json
import subprocess
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import api_models
import fault_primitives as fp
import topology

app = typer.Typer(add_completion=False, help="RW-11: replicated sync mode vs the live GUC.")
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent

SYNC_FIELD: Final[str] = "sync_replication_active"
"""The `/debug/state` key this whole suite decides on. Checked for presence
before anything is measured: `None` is a legitimate value, so a field that had
been removed would parse to the same thing and every sample would read as
'no transition observed' — a green that looked at nothing."""

INTERVALS_ALLOWED: Final[int] = 4
"""Reconcile intervals the flag may trail the GUC.

The re-commit happens on the next tick after the transition, so one interval is
the design. Four leaves room for a `client_write` round trip and a loaded
runner without leaving room to call an unbounded window slow."""

SAMPLE_PERIOD_S: Final[float] = 0.25
"""Fast enough to resolve a window measured in single reconcile intervals."""

FALLBACK_TIMEOUT_S: Final[float] = 90.0
"""Budget for the leader to fall back to async once replication is severed.
Covers `replica_disconnect_timeout_ms` plus a reconcile."""

CONVERGE_TIMEOUT_S: Final[float] = 120.0
"""Budget for a leader to exist and answer both reads."""


class Phase(StrEnum):
    HEAL = "heal"
    FAILOVER = "failover"


ALL_PHASES: Final[tuple[Phase, ...]] = tuple(Phase)
"""Both phases, which is what `--phase` runs when it is not given."""


def selected_phases(raw: str) -> tuple[Phase, ...]:
    """Parse `--phase`, refusing a name that is not a phase.

    An unrecognised name must not silently select nothing: a run that executes
    no phase reports no violation, which is the same output a clean run gives.
    """
    names = [part.strip() for part in raw.split(",") if part.strip()]
    if not names:
        return ALL_PHASES
    unknown = [n for n in names if n not in {str(p) for p in Phase}]
    if unknown:
        raise PreconditionError(
            f"unknown phase(s) {', '.join(unknown)}; known: {', '.join(str(p) for p in Phase)}"
        )
    return tuple(Phase(n) for n in names)


class SyncModeViolation(RuntimeError):
    """The election gate ran loose while sync replication was in force."""


class OracleNotProven(RuntimeError):
    """The checker did not go red on input built to make it red."""


class VacuousRun(RuntimeError):
    """The phase produced no transition, or watched too coarsely to see one."""


class PreconditionError(RuntimeError):
    """The run could not be carried out as specified."""


@dataclass(frozen=True, slots=True)
class Sample:
    """One paired reading of the two views of sync mode.

    `guc` is `None` when the node could not be asked, which is not the same as
    an empty list: an unreachable node is an absence of evidence, and counting
    it as "no sync standbys" would manufacture the very disagreement this
    looks for.
    """

    at_s: float
    leader: str | None
    replicated: bool | None
    guc: str | None

    @property
    def readable(self) -> bool:
        return self.leader is not None and self.guc is not None

    @property
    def disagrees(self) -> bool:
        """The flag is looser than the GUC — the RW-11 shape."""
        return self.readable and self.replicated is False and bool(self.guc)


@dataclass(frozen=True, slots=True)
class Window:
    """The longest run of consecutive disagreeing samples."""

    seconds: float
    samples: int
    started_at_s: float
    leader: str | None


def longest_disagreement(samples: Sequence[Sample]) -> Window:
    """The longest unbroken span in which the flag was looser than the GUC.

    An unreadable sample breaks the run rather than extending or ending it: the
    node was not observed, so neither the disagreement nor its repair was.
    Extending across the gap would charge the system for a window nobody saw.
    """
    best = Window(0.0, 0, 0.0, None)
    run_start: float | None = None
    run_count = 0
    run_leader: str | None = None
    for sample in samples:
        if not sample.readable:
            run_start, run_count, run_leader = None, 0, None
            continue
        if not sample.disagrees:
            run_start, run_count, run_leader = None, 0, None
            continue
        if run_start is None:
            run_start, run_leader = sample.at_s, sample.leader
        run_count += 1
        span = sample.at_s - run_start
        if span > best.seconds or (span == best.seconds and run_count > best.samples):
            best = Window(span, run_count, run_start, run_leader)
    return best


def budget_seconds(timings: fp.SystemTimings) -> float:
    """How long the flag may trail the GUC before it is a finding."""
    return INTERVALS_ALLOWED * timings.replica_check_interval_ms / 1_000


def _sh(cmd: str, timeout: float = 30.0) -> tuple[int, str]:
    proc = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=PROJECT_ROOT,
        check=False,
    )
    return proc.returncode, proc.stdout.strip()


def _mgmt_port(node: str) -> int:
    return topology.load().by_service(node).mgmt_port


def read_debug_state(node: str, token: str) -> api_models.DebugState | None:
    rc, out = _sh(
        f"curl -s -m 3 -H 'x-pgbattery-token: {token}' "
        f"http://127.0.0.1:{_mgmt_port(node)}/debug/state",
        timeout=10.0,
    )
    if rc != 0:
        return None
    return api_models.parse_or_none(api_models.DebugState, out)


def require_sync_field(node: str, token: str) -> None:
    """Refuse to measure until `/debug/state` actually carries the field."""
    rc, out = _sh(
        f"curl -s -m 5 -H 'x-pgbattery-token: {token}' "
        f"http://127.0.0.1:{_mgmt_port(node)}/debug/state",
        timeout=15.0,
    )
    if rc != 0:
        raise PreconditionError(f"{node}: /debug/state did not answer")
    try:
        payload = json.loads(out)
    except json.JSONDecodeError as exc:
        raise PreconditionError(f"{node}: /debug/state is not JSON: {exc}") from None
    if SYNC_FIELD not in payload:
        raise PreconditionError(
            f"{node}: /debug/state has no {SYNC_FIELD!r}. Every sample would read as "
            "'no transition observed' and the run would pass having measured nothing."
        )


WRITE_TABLE: Final[str] = "rw11_sync_mode_load"

PARTITION_SETTLE_S: Final[float] = 8.0
"""How long the replication rule may take to match its first packet.

The walreceiver reconnects with backoff, so an interval sized for a busy
channel would refuse a fault that is about to land. The load below is what
makes the channel busy in the first place."""


@contextmanager
def write_load(node: str) -> Iterator[None]:
    """Keep WAL flowing so the replication channel carries traffic.

    Without it there is nothing to stream, the `--dport` rule matches no packet,
    and `partition_channel` correctly refuses the window as uninjected. Writes
    are expected to block once the sync standbys are severed — that is the state
    being produced — so failures are ignored rather than raised.
    """
    stop = threading.Event()

    def loop() -> None:
        _sh(
            f"docker compose exec -T {node} psql -U postgres -h 127.0.0.1 "
            f"-p {fp.PG_INTERNAL_PORT} -d postgres -At "
            f'-c "CREATE TABLE IF NOT EXISTS {WRITE_TABLE}(id bigserial primary key, '
            f'payload text)"',
            timeout=30.0,
        )
        while not stop.is_set():
            try:
                _sh(
                    f"docker compose exec -T {node} psql -U postgres -h 127.0.0.1 "
                    f"-p {fp.PG_INTERNAL_PORT} -d postgres -At "
                    f'-c "INSERT INTO {WRITE_TABLE}(payload) '
                    f"SELECT repeat('x', 4096) FROM generate_series(1, 32)\"",
                    timeout=15.0,
                )
            except subprocess.TimeoutExpired:
                # A commit blocks for as long as the severed sync standbys owe
                # it an ack. That is the state this load exists to produce.
                continue

    worker = threading.Thread(target=loop, name="rw11-load", daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=30.0)


def read_sync_names(node: str) -> str | None:
    """The leader's live `synchronous_standby_names`, or `None` if unreachable."""
    rc, out = _sh(
        f"docker compose exec -T {node} psql -U postgres -h 127.0.0.1 "
        f"-p {fp.PG_INTERNAL_PORT} -d postgres -At "
        '-c "SHOW synchronous_standby_names"',
        timeout=15.0,
    )
    return None if rc != 0 else out.strip()


def sample_leader(token: str, started_at: float) -> Sample:
    """One paired reading, taken from whichever node currently leads."""
    leader = fp.find_raft_leader()
    at_s = time.monotonic() - started_at
    if leader is None:
        return Sample(at_s=at_s, leader=None, replicated=None, guc=None)
    state = read_debug_state(leader, token)
    guc = read_sync_names(leader)
    return Sample(
        at_s=at_s,
        leader=leader,
        replicated=None if state is None else state.sync_replication_active,
        guc=None if state is None else guc,
    )


def await_leader(token: str) -> str:
    deadline = time.monotonic() + CONVERGE_TIMEOUT_S
    while time.monotonic() < deadline:
        leader = fp.find_raft_leader()
        if leader is not None and read_sync_names(leader) is not None:
            return leader
        time.sleep(1.0)
    raise PreconditionError(f"no leader answering both reads within {CONVERGE_TIMEOUT_S:.0f}s")


def await_flag(token: str, want: bool, timeout_s: float) -> float:
    """Wait for the replicated flag to reach `want`, returning how long it took."""
    started = time.monotonic()
    deadline = started + timeout_s
    while time.monotonic() < deadline:
        leader = fp.find_raft_leader()
        if leader is not None:
            state = read_debug_state(leader, token)
            if state is not None and state.sync_replication_active is want:
                return time.monotonic() - started
        time.sleep(0.5)
    raise PreconditionError(
        f"replicated sync mode never reached {want} within {timeout_s:.0f}s; "
        "the fallback this phase depends on did not happen"
    )


@contextmanager
def sampling(token: str) -> Iterator[list[Sample]]:
    """Sample the leader for the whole of the block, on a background thread.

    Spanning the phase rather than starting after the heal: the repair and the
    re-commit are one reconcile apart, so a collector started afterwards can
    miss the transition entirely and report a window of zero having watched
    nothing happen.
    """
    samples: list[Sample] = []
    stop = threading.Event()
    started = time.monotonic()

    def loop() -> None:
        while not stop.is_set():
            samples.append(sample_leader(token, started))
            stop.wait(SAMPLE_PERIOD_S)

    worker = threading.Thread(target=loop, name="rw11-sampler", daemon=True)
    worker.start()
    try:
        yield samples
    finally:
        stop.set()
        worker.join(timeout=60.0)


def observed_repair(samples: Sequence[Sample]) -> bool:
    """Whether the series actually watched the sync list empty and refill.

    A run in which nothing ever changed reports a window of zero, which is the
    same answer a working system gives. Without this the phase could pass
    having produced no transition at all.
    """
    readable = [s for s in samples if s.readable]
    return any(not s.guc for s in readable) and any(s.guc for s in readable)


def median_gap_s(samples: Sequence[Sample]) -> float:
    """Typical spacing between samples.

    Each sample costs a leader lookup, an HTTP read and a `docker exec`, so the
    period is what those allow rather than `SAMPLE_PERIOD_S`. A series too
    coarse to resolve the budget cannot report a window inside it, and would
    pass by being unable to look.
    """
    gaps = sorted(b.at_s - a.at_s for a, b in itertools.pairwise(samples))
    if not gaps:
        return float("inf")
    middle = len(gaps) // 2
    if len(gaps) % 2:
        return gaps[middle]
    return (gaps[middle - 1] + gaps[middle]) / 2


def run_phase(
    phase: Phase, token: str, timings: fp.SystemTimings
) -> tuple[Window, float, list[str]]:
    """Drive one way into the window and measure how long it stays open."""
    notes: list[str] = []
    leader = await_leader(token)
    require_sync_field(leader, token)
    standbys = [n for n in fp.NODES if n != leader]
    notes.append(f"leader {leader}, standbys {', '.join(standbys)}")

    budget = budget_seconds(timings)
    # The sampler spans the whole phase, so the empty-then-refilled transition
    # is inside the series rather than either side of where it started.
    with sampling(token) as samples:
        # Sever replication at both standbys, leaving Raft up. One severed
        # standby is not enough: the other takes the sync slot and no fallback
        # happens. The load runs first, so the channel is carrying traffic when
        # the rules go in and the primitive can see them match.
        with (
            write_load(leader),
            fp.partition_channel(
                standbys[0], [leader], fp.Channel.REPLICATION, settle_s=PARTITION_SETTLE_S
            ),
            fp.partition_channel(
                standbys[1], [leader], fp.Channel.REPLICATION, settle_s=PARTITION_SETTLE_S
            ),
        ):
            took = await_flag(token, want=False, timeout_s=FALLBACK_TIMEOUT_S)
            notes.append(f"async fallback committed after {took:.0f}s")
            if phase is Phase.FAILOVER:
                # Move leadership while the fallback is still in force, so the
                # new leader arms its own sync list against an inherited flag.
                target = topology.load().by_service(standbys[0]).node_id
                rc, out = _sh(
                    f"curl -s -m 20 -X POST -H 'x-pgbattery-token: {token}' "
                    f"http://127.0.0.1:{_mgmt_port(leader)}"
                    f"/api/v1/cluster/transfer-leadership/{target}",
                    timeout=30.0,
                )
                notes.append(f"transfer to node {target} requested (rc={rc}): {out[:100]}")

        # Healed. From here the sync list refills and the flag must follow.
        time.sleep(budget * 3)

    readable = sum(1 for s in samples if s.readable)
    gap = median_gap_s(samples)
    notes.append(f"{len(samples)} samples, {readable} readable, median gap {gap:.2f}s")
    if not observed_repair(samples):
        raise VacuousRun(
            f"{phase}: the series never shows the sync list both empty and refilled, "
            "so a window of zero would mean the transition was never produced"
        )
    if gap > budget / 2:
        raise VacuousRun(
            f"{phase}: samples are {gap:.2f}s apart against a {budget:.1f}s budget, "
            "too coarse to resolve a window inside it"
        )
    return longest_disagreement(samples), gap, notes


def upper_bound_s(window: Window, gap_s: float) -> float:
    """The longest the window could have been, given when it was sampled.

    A run of `n` samples spans `n-1` periods, so a window seen once measures
    zero — which reads as "not seen at all" and is the wrong way to be wrong.
    It could have opened just after the previous sample and closed just before
    the next, so one period is added and the verdict is taken on that.
    """
    return 0.0 if window.samples == 0 else window.seconds + gap_s


def report(rows: list[tuple[str, Window, float, float, list[str]]]) -> bool:
    table = Table(title="Replicated sync mode vs the live GUC")
    table.add_column("Phase")
    table.add_column("Leader")
    table.add_column("Seen in", justify="right")
    table.add_column("At most", justify="right")
    table.add_column("Budget", justify="right")
    table.add_column("Verdict")
    ok = True
    for name, window, gap, budget, notes in rows:
        upper = upper_bound_s(window, gap)
        passed = upper <= budget
        ok = ok and passed
        table.add_row(
            name,
            window.leader or "-",
            f"{window.samples} sample(s)",
            f"{upper:.1f}s",
            f"{budget:.1f}s",
            "PASS" if passed else "VIOLATION",
        )
        for note in notes:
            console.print(f"  [dim]{name}: {note}[/]")
    console.print(table)
    return ok


@app.command()
def main(
    token: str = typer.Option("local-ci-token", "--token", help="Management API token."),
    phases: str = typer.Option(
        "", "--phase", help="Comma-separated phases to run; empty runs both."
    ),
    prove_oracle: bool = typer.Option(
        False, "--prove-oracle", help="Require the checker to reject a held-open window."
    ),
) -> None:
    """Measure how long the replicated flag may run looser than the live GUC."""
    timings = fp.read_system_timings()
    budget = budget_seconds(timings)
    console.print(
        f"[bold]RW-11 sync-mode agreement[/] — reconcile {timings.replica_check_interval_ms} ms, "
        f"disconnect {timings.replica_disconnect_timeout_ms} ms, budget {budget:.1f}s"
    )

    if prove_oracle:
        held = [
            Sample(
                at_s=i * SAMPLE_PERIOD_S, leader="node1", replicated=False, guc="FIRST 1 (node2)"
            )
            for i in range(int((budget + 5.0) / SAMPLE_PERIOD_S))
        ]
        if longest_disagreement(held).seconds <= budget:
            raise OracleNotProven(
                "a window held open past the budget did not register as a violation"
            )
        console.print("[green]oracle proven[/]: a held-open window is reported")

    rows: list[tuple[str, Window, float, float, list[str]]] = []
    try:
        for chosen in selected_phases(phases):
            window, gap, notes = run_phase(chosen, token, timings)
            rows.append((str(chosen), window, gap, budget, notes))
    except (PreconditionError, VacuousRun, fp.FaultError) as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(code=1) from None

    if not report(rows):
        console.print(
            "[red]RW-11 VIOLATED[/]: the election gate used the loose LSN threshold while "
            "sync replication was in force, for longer than a re-commit can explain."
        )
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
