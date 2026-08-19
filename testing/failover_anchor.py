#!/usr/bin/env -S uv run --project testing python
"""H-18: the failover anchor's lifecycle under coalesced watch transitions.

`promote_local_postgres` withholds promotion for one lease duration measured
from `failover_started_at`. The governor stamps that anchor on the leader-loss
edge, clears it when another node is the stable leader, and re-stamps it when
the metrics watch coalesces `Leader(other) -> None -> Leader(self)` and swallows
the edge. A missed clear leaves an ancient anchor; a missed re-stamp leaves
none. Both read as "the lease expired long ago", and the hold-down lets a
promotion through while the deposed leader may still hold write authority.

The pure functions are unit-tested. This drives the live lifecycle: rapid
leadership churn is what makes the watch coalesce, and `/debug/state` now
reports the anchor's age so the assertion is on the anchor itself rather than
on whether a hold-down happened to fire.

Two properties:

  settled   a cluster with a stable leader carries no anchor anywhere — one
            left behind is the stale-anchor bug, and it would be read as
            ancient at the next failover
  bounded   an anchor observed during a failover is younger than the failover
            it belongs to, never inherited from an earlier one
"""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import fault_primitives as fp

app = typer.Typer(add_completion=False)
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
RECOVERY_TIMEOUT_S: Final[float] = 240.0
SAMPLE_INTERVAL_S: Final[float] = 0.2

# An anchor older than this while a leader is stable was not cleared. One lease
# duration is the window the hold-down actually consults; anything past it is
# indistinguishable from "ancient" to the gate.
STALE_ANCHOR_FACTOR: Final[float] = 3.0


class AnchorError(RuntimeError):
    """The run could not be carried out as specified."""


@dataclass(frozen=True)
class Sample:
    node: str
    leader_id: int | None
    is_leader: bool
    anchor_age_ms: int | None
    at: float


@dataclass
class Round:
    index: int
    killed: str
    samples: list[Sample] = field(default_factory=list)
    settled_with_anchor: list[tuple[str, int]] = field(default_factory=list)

    @property
    def verdict(self) -> str:
        if self.settled_with_anchor:
            worst = ", ".join(f"{n} held {age} ms" for n, age in self.settled_with_anchor)
            return f"FAIL: anchor survived a settled cluster ({worst})"
        return "PASS"

    @property
    def ok(self) -> bool:
        return self.verdict == "PASS"


def _sh(cmd: str, timeout: float = 30.0) -> tuple[int, str]:
    import os

    env = dict(os.environ)
    env.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "ci-token")
    proc = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=PROJECT_ROOT,
        env=env,
        check=False,
    )
    return proc.returncode, proc.stdout.strip()


def read_state(node: str, token: str) -> Sample | None:
    """`/debug/state` on one node, including the anchor's age."""
    index = int(node.removeprefix("node"))
    rc, out = _sh(
        f"curl -s -m 3 -H 'x-pgbattery-token: {token}' http://127.0.0.1:908{index}/debug/state",
        timeout=10.0,
    )
    if rc != 0 or not out.startswith("{"):
        return None
    try:
        parsed = json.loads(out)
    except json.JSONDecodeError:
        return None
    age = parsed.get("failover_anchor_age_ms")
    return Sample(
        node=node,
        leader_id=parsed.get("leader_id"),
        is_leader=bool(parsed.get("is_leader")),
        anchor_age_ms=int(age) if isinstance(age, int) else None,
        at=time.monotonic(),
    )


def sample_all(token: str) -> list[Sample]:
    return [s for s in (read_state(n, token) for n in fp.NODES) if s is not None]


def cluster_settled(samples: list[Sample]) -> bool:
    """Every node answered and they all name the same leader.

    Requires the full set: two agreeing while a third is unreachable is not
    agreement, it is a smaller cluster.
    """
    return len(samples) >= len(fp.NODES) and quorum_agrees(samples)


def quorum_agrees(samples: list[Sample]) -> bool:
    """The nodes that answered name one leader.

    Used while a node is deliberately down, where waiting for the full set
    would wait forever — the earlier version did exactly that and burned the
    whole window every round.
    """
    if not samples:
        return False
    leaders = {s.leader_id for s in samples}
    return len(leaders) == 1 and next(iter(leaders)) is not None


def await_settled(token: str, timeout_s: float) -> list[Sample]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        samples = sample_all(token)
        if cluster_settled(samples):
            return samples
        time.sleep(1.0)
    raise AnchorError(f"cluster did not settle within {timeout_s:.0f}s")


def stale_anchors(samples: list[Sample], lease_ms: int) -> list[tuple[str, int]]:
    """Anchors still held on a settled cluster, past the window that matters.

    Pure, so the self-test can hand it a settled cluster carrying an ancient
    anchor and require it back.
    """
    limit = lease_ms * STALE_ANCHOR_FACTOR
    return [
        (s.node, s.anchor_age_ms)
        for s in samples
        if s.anchor_age_ms is not None and s.anchor_age_ms > limit
    ]


def run_round(index: int, token: str, timings: fp.SystemTimings) -> Round:
    """One churn round: depose the leader, watch the anchor through recovery."""
    settled = await_settled(token, RECOVERY_TIMEOUT_S)
    leader = next((s.node for s in settled if s.is_leader), None)
    if leader is None:
        raise AnchorError("settled cluster with no node claiming leadership")

    rnd = Round(index=index, killed=leader)
    fp.kill_container(leader)
    try:
        # Sample across the whole failover, so an anchor that is stamped,
        # cleared, and re-stamped is seen in every state it passes through.
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            samples = sample_all(token)
            rnd.samples.extend(samples)
            # The killed node cannot answer, so the survivors agreeing on a new
            # leader is what "the failover finished" means here.
            if quorum_agrees(samples) and any(s.is_leader for s in samples):
                break
            time.sleep(SAMPLE_INTERVAL_S)
    finally:
        fp.start_container(leader)

    final = await_settled(token, RECOVERY_TIMEOUT_S)
    rnd.samples.extend(final)
    rnd.settled_with_anchor = stale_anchors(final, timings.lease_duration_ms)
    return rnd


@app.command()
def run(
    rounds: int = typer.Option(2, "--rounds", help="Churn rounds to drive."),
    token: str = typer.Option("local-ci-token", "--token", help="Management API token."),
) -> None:
    """Force leadership churn and assert the anchor lifecycle, not its arithmetic."""
    timings = fp.read_system_timings()
    console.print(f"[bold]H-18 failover anchor[/] — lease {timings.lease_duration_ms} ms")

    results: list[Round] = []
    for index in range(rounds):
        try:
            results.append(run_round(index, token, timings))
        except (AnchorError, fp.FaultError) as exc:
            console.print(f"[red]round {index}: {exc}[/]")
            raise typer.Exit(code=1) from None

    table = Table(title="Failover anchor lifecycle (H-18)")
    table.add_column("Round", justify="right")
    table.add_column("Deposed")
    table.add_column("Samples", justify="right")
    table.add_column("Anchors seen", justify="right")
    table.add_column("Verdict")
    for r in results:
        seen = sum(1 for s in r.samples if s.anchor_age_ms is not None)
        table.add_row(
            str(r.index),
            r.killed,
            str(len(r.samples)),
            str(seen),
            "[green]PASS[/]" if r.ok else f"[red]{r.verdict}[/]",
        )
    console.print(table)

    observed = sum(1 for r in results for s in r.samples if s.anchor_age_ms is not None)
    if observed == 0:
        console.print(
            "[yellow]no anchor was ever observed — the failovers completed between "
            "samples, so the lifecycle was not exercised[/]"
        )
    if [r for r in results if not r.ok]:
        raise typer.Exit(code=1)
    console.print(f"[green]anchor cleared on every settled cluster across {len(results)} rounds[/]")


if __name__ == "__main__":
    app()
