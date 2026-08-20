#!/usr/bin/env -S uv run --project testing python
"""H-09: destroy a node's state, rejoin it under the same id, assert nobody voted twice.

A voter's persisted vote is what stops it voting twice in one term. Wipe the
store, bring the node back under the same id, and it has no memory of the vote
it cast — two candidates can then each collect a majority.

Variants: `both`, `raft-only` (still looks like a full replica, so nothing
downstream hints it forgot), `pgdata-only`.

Either outcome is acceptable — rejoin cleanly, or refuse to run. What is not is
a node that runs and votes on a store nothing vouched for, so the assertion is
on the term: no two nodes may claim leadership in the same one.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import fault_primitives as fp

app = typer.Typer(add_completion=False)
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
INTERNAL_PG_PORT: Final[int] = fp.PG_INTERNAL_PORT

RAFT_DIR: Final[str] = f"{fp.PG_STATE_DIR}/raft"
PGDATA_DIR: Final[str] = fp.PG_DATA_DIR

RECOVERY_TIMEOUT_S: Final[float] = 300.0

# A bound, not a wait: the loop ends when the outcome is observed, so reaching
# this is the "neither rejoined nor refused" failure.
OBSERVE_BOUND_S: Final[float] = 150.0

# Keep sampling past the outcome — a returning voter's vote only matters at the
# next election.
SETTLE_AFTER_OUTCOME_S: Final[float] = 15.0

# `restart: unless-stopped` means a refusing node crash-loops rather than
# staying exited, so the restart counter is what separates refusing from slow.
REFUSAL_RESTARTS: Final[int] = 2

SAMPLE_INTERVAL_S: Final[float] = 0.5

PROBE_TABLE: Final[str] = "wiped_rejoin_probe"


class Variant(StrEnum):
    BOTH = "both"
    RAFT_ONLY = "raft-only"
    PGDATA_ONLY = "pgdata-only"

    @property
    def paths(self) -> list[str]:
        if self is Variant.BOTH:
            return [RAFT_DIR, PGDATA_DIR]
        if self is Variant.RAFT_ONLY:
            return [RAFT_DIR]
        return [PGDATA_DIR]


class RejoinError(RuntimeError):
    """The run could not be carried out as specified."""


@dataclass(frozen=True)
class LeaderSample:
    node: str
    term: int


@dataclass
class VariantResult:
    variant: Variant
    target: str
    rejoined: bool
    refused: bool
    dual_leadership: list[tuple[int, tuple[str, ...]]] = field(default_factory=list)
    acked_rows_before: int = 0
    acked_rows_after: int | None = None
    detail: str = ""

    @property
    def verdict(self) -> str:
        # A run that could not be carried out reports why, not a verdict about
        # a cluster it never measured.
        if self.detail.startswith("FAIL:"):
            return self.detail
        if self.dual_leadership:
            terms = ", ".join(f"term {t}: {', '.join(n)}" for t, n in self.dual_leadership)
            return f"FAIL: two leaders in one term ({terms})"
        if self.acked_rows_after is not None and self.acked_rows_after < self.acked_rows_before:
            return (
                f"FAIL: acked rows lost ({self.acked_rows_before} before, "
                f"{self.acked_rows_after} after)"
            )
        if not (self.rejoined or self.refused):
            return "FAIL: neither rejoined nor refused — running on state nobody vouched for"
        return "PASS"

    @property
    def ok(self) -> bool:
        return self.verdict == "PASS"


def _sh(cmd: str, timeout: float = 60.0) -> tuple[int, str, str]:
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
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def sample_leaders() -> list[LeaderSample]:
    """Nodes claiming leadership right now, with their term.

    From each node's own `/metrics`: the management API serves the last leader a
    node *heard about*, so it agrees even when the nodes do not.
    """
    samples: list[LeaderSample] = []
    for node in fp.NODES:
        is_leader = fp.read_metric(node, "pgbattery_raft_is_leader")
        term = fp.read_metric(node, "pgbattery_raft_term")
        if is_leader is not None and is_leader >= 1.0 and term is not None:
            samples.append(LeaderSample(node=node, term=int(term)))
    return samples


def find_dual_leadership(history: list[list[LeaderSample]]) -> list[tuple[int, tuple[str, ...]]]:
    """Terms in which more than one node claimed leadership.

    Different terms is ordinary failover; the same term is the violation.
    """
    by_term: dict[int, set[str]] = {}
    for sample_set in history:
        for sample in sample_set:
            by_term.setdefault(sample.term, set()).add(sample.node)
    return [
        (term, tuple(sorted(nodes))) for term, nodes in sorted(by_term.items()) if len(nodes) > 1
    ]


def await_healthy_cluster(timeout_s: float) -> str:
    """One leader and every node's PostgreSQL answering; returns the leader."""
    deadline = time.monotonic() + timeout_s
    last = "never converged"
    while time.monotonic() < deadline:
        leader = fp.find_raft_leader()
        if leader is not None:
            answered = [n for n in fp.NODES if _pg_answers(n)]
            if len(answered) == len(fp.NODES):
                return leader
            last = f"leader {leader}, PostgreSQL answering on {len(answered)}/{len(fp.NODES)}"
        else:
            last = "no leader"
        time.sleep(2.0)
    raise RejoinError(f"cluster did not become healthy within {timeout_s:.0f}s ({last})")


def _pg_answers(node: str) -> bool:
    rc, out, _ = _sh(
        f"docker compose exec -T {node} psql -U postgres -h 127.0.0.1 "
        f"-p {INTERNAL_PG_PORT} -d postgres -At -c 'SELECT 1'",
        timeout=20.0,
    )
    return rc == 0 and out.strip() == "1"


def seed_acked_rows(leader: str, count: int) -> int:
    """Acknowledged rows written before the wipe — the durability half of the test."""
    rc, out, err = _sh(
        f"docker compose exec -T {leader} psql -U postgres -h 127.0.0.1 "
        f"-p {INTERNAL_PG_PORT} -d postgres -At -c "
        f'"CREATE TABLE IF NOT EXISTS {PROBE_TABLE}(id bigserial primary key); '
        f"INSERT INTO {PROBE_TABLE} SELECT FROM generate_series(1, {count}); "
        f'SELECT count(*) FROM {PROBE_TABLE}"',
        timeout=60.0,
    )
    if rc != 0:
        raise RejoinError(f"could not seed acknowledged rows on {leader}: {err or out}")
    total = out.strip().splitlines()[-1]
    if not total.isdigit():
        raise RejoinError(f"unexpected row count from {leader}: {out!r}")
    return int(total)


def count_rows(node: str) -> int | None:
    rc, out, _ = _sh(
        f"docker compose exec -T {node} psql -U postgres -h 127.0.0.1 "
        f"-p {INTERNAL_PG_PORT} -d postgres -At -c 'SELECT count(*) FROM {PROBE_TABLE}'",
        timeout=30.0,
    )
    if rc != 0 or not out.strip().isdigit():
        return None
    return int(out.strip())


def node_is_running(service: str) -> bool:
    state = fp.read_container_runstate(service)
    return state is not None and state.status == "running"


def await_service_running(service: str, timeout_s: float) -> None:
    """Wait until `service` is 'running' and stays there for two samples."""
    deadline = time.monotonic() + timeout_s
    stable = 0
    while time.monotonic() < deadline:
        if node_is_running(service):
            stable += 1
            if stable >= 2:
                return
        else:
            stable = 0
        time.sleep(2.0)
    raise RejoinError(f"{service} never settled into 'running' within {timeout_s:.0f}s")


def _restart_count(service: str) -> int:
    state = fp.read_container_runstate(service)
    return 0 if state is None else state.restart_count


def run_variant(variant: Variant) -> VariantResult:
    """Wipe one node's state and watch what the cluster does about it."""
    leader = await_healthy_cluster(RECOVERY_TIMEOUT_S)
    # Not the leader: that would conflate the wipe with an ordinary failover.
    target = next(n for n in fp.NODES if n != leader)
    # And not one still cycling from the previous variant — wiping a container
    # mid-restart fails the primitive's own precondition, which is a statement
    # about the harness rather than about the node.
    await_service_running(target, RECOVERY_TIMEOUT_S)
    rows_before = seed_acked_rows(leader, 50)

    console.print(f"[dim]{variant}: wiping {', '.join(variant.paths)} on {target}[/]")
    fp.wipe_node_state(target, variant.paths)

    history: list[list[LeaderSample]] = []
    restarts_at_start = _restart_count(target)
    fp.start_container(target)

    started = time.monotonic()
    deadline = started + OBSERVE_BOUND_S
    rejoined = False
    refused = False
    decided_at: float | None = None

    while time.monotonic() < deadline:
        history.append(sample_leaders())
        if decided_at is None:
            if (
                node_is_running(target)
                and _pg_answers(target)
                and fp.find_raft_leader() is not None
            ):
                rejoined = True
                decided_at = time.monotonic()
            elif (_restart_count(target) - restarts_at_start) >= REFUSAL_RESTARTS:
                refused = True
                decided_at = time.monotonic()
        elif time.monotonic() - decided_at >= SETTLE_AFTER_OUTCOME_S:
            break
        time.sleep(SAMPLE_INTERVAL_S)

    rows_after = count_rows(fp.find_raft_leader() or leader)
    elapsed = time.monotonic() - started

    return VariantResult(
        variant=variant,
        target=target,
        rejoined=rejoined,
        refused=refused,
        dual_leadership=find_dual_leadership(history),
        acked_rows_before=rows_before,
        acked_rows_after=rows_after,
        detail=f"{len(history)} samples over {elapsed:.0f}s",
    )


def restore_cluster(result: VariantResult) -> None:
    """Put the node back so the next variant starts from a converged cluster.

    A node that refused will not heal itself — that is the point of refusing —
    so waiting for it is a guaranteed timeout. Wiping both stores lets it take
    the fresh-join path, which is the operator's move too.
    """
    if result.target in fp.NODES and not result.rejoined:
        console.print(f"[dim]restoring {result.target} for the next variant[/]")
        try:
            fp.wipe_node_state(result.target, [RAFT_DIR, PGDATA_DIR])
            fp.start_container(result.target)
        except fp.FaultError as exc:
            console.print(f"[yellow]could not restore {result.target}: {exc}[/]")
    try:
        await_healthy_cluster(RECOVERY_TIMEOUT_S)
    except RejoinError as exc:
        console.print(f"[yellow]cluster did not fully recover: {exc}[/]")


@app.command()
def run(
    variant: str = typer.Option(
        "all",
        "--variant",
        help=f"Which wipe to drive: {[v.value for v in Variant]}, or 'all'.",
    ),
) -> None:
    """Destroy a node's state, rejoin it, and assert nobody voted twice."""
    selected = list(Variant) if variant == "all" else [Variant(variant)]

    results: list[VariantResult] = []
    for chosen in selected:
        try:
            results.append(run_variant(chosen))
        except (RejoinError, fp.FaultError) as exc:
            results.append(
                VariantResult(
                    variant=chosen,
                    target="-",
                    rejoined=False,
                    refused=False,
                    detail=f"FAIL: {exc}",
                )
            )
            console.print(f"[red]{chosen}: {exc}[/]")
            break
        restore_cluster(results[-1])

    table = Table(title="Wiped-node rejoin (H-09)")
    table.add_column("Variant", style="bold")
    table.add_column("Node")
    table.add_column("Rejoined")
    table.add_column("Refused")
    table.add_column("Rows before", justify="right")
    table.add_column("Rows after", justify="right")
    table.add_column("Verdict")
    for r in results:
        table.add_row(
            r.variant.value,
            r.target,
            "yes" if r.rejoined else "no",
            "yes" if r.refused else "no",
            str(r.acked_rows_before),
            "-" if r.acked_rows_after is None else str(r.acked_rows_after),
            "[green]PASS[/]" if r.ok else f"[red]{r.verdict}[/]",
        )
    console.print(table)

    if [r for r in results if not r.ok]:
        raise typer.Exit(code=1)
    console.print(f"[green]no node voted twice in a term across {len(results)} wipes[/]")


if __name__ == "__main__":
    app()
