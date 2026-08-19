#!/usr/bin/env -S uv run --project testing python
"""H-16: the join and rejoin edges, and what they leave behind.

Three cases, each an edge the happy path never visits:

  deposed-mid-copy    the source a joining node is cloning from is deposed
                      while `pg_basebackup` is running. The node must not end
                      up running on a half-copied data directory.
  orphan-slot         a replication slot nobody consumes pins WAL forever.
                      The reconciler is supposed to drop it; "supposed to" is
                      the part this measures.
  learner-crash       a node registers as a learner and dies before finishing.
                      Membership must not keep a phantom voter.
  bootstrap-wiped     the node that starts with `--bootstrap` loses its state
                      directory. It re-runs initdb, so it holds a data lineage
                      the cluster never wrote to, under an id the cluster still
                      lists — and it is the peer the others are configured to
                      join through. It must end up back on the cluster's
                      lineage, and no peer may discard its own Raft state on
                      what it said in the meantime.

Orphan-slot pinning is asserted *bounded*: the slot is dropped, and the WAL it
held is released, within a multiple of the reconciler's own interval. An
unbounded slot is an outage on a real disk, and nothing had ever watched one.
"""

from __future__ import annotations

import contextlib
import subprocess
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import api_models
import fault_primitives as fp

app = typer.Typer(add_completion=False)
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
INTERNAL_PG_PORT: Final[int] = 5434
RECOVERY_TIMEOUT_S: Final[float] = 300.0

RAFT_DIR: Final[str] = f"{fp.PG_STATE_DIR}/raft"
PGDATA_DIR: Final[str] = fp.PG_DATA_DIR

ORPHAN_NODE_ID: Final[int] = 99
"""A node id no cluster in this repo hands out, so its slot is orphaned the
moment it exists."""

FOREIGN_SLOT: Final[str] = "operator_keepme"
"""A slot pgbattery did not mint. Dropping one would destroy an operator's WAL
reservation, so the sweep must leave it exactly where it is."""

# The reconciler ensures slots every REPLICATION_SLOT_ENSURE_INTERVAL_SECS.
# Three intervals is generous for one drop and leaves no room to call an
# unbounded slot "just slow".
SLOT_INTERVALS_ALLOWED: Final[int] = 3


class Case(StrEnum):
    DEPOSED_MID_COPY = "deposed-mid-copy"
    ORPHAN_SLOT = "orphan-slot"
    LEARNER_CRASH = "learner-crash"
    BOOTSTRAP_WIPED = "bootstrap-wiped"


class EdgeError(RuntimeError):
    """The run could not be carried out as specified."""


@dataclass
class Result:
    case: Case
    detail: str
    ok: bool


def _sh(cmd: str, timeout: float = 60.0) -> tuple[int, str]:
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


def _psql(node: str, sql: str, timeout: float = 30.0) -> tuple[int, str]:
    return _sh(
        f"docker compose exec -T {node} psql -U postgres -h 127.0.0.1 "
        f'-p {INTERNAL_PG_PORT} -d postgres -At -c "{sql}"',
        timeout=timeout,
    )


def _pg_answers(node: str) -> bool:
    rc, out = _psql(node, "SELECT 1", timeout=20.0)
    return rc == 0 and out.strip() == "1"


def await_healthy(timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        leader = fp.find_raft_leader()
        if leader is not None and all(_pg_answers(n) for n in fp.NODES):
            return leader
        time.sleep(2.0)
    raise EdgeError(f"cluster did not become healthy within {timeout_s:.0f}s")


def slot_names(node: str) -> list[str]:
    rc, out = _psql(node, "SELECT slot_name FROM pg_replication_slots ORDER BY slot_name")
    if rc != 0:
        return []
    return [line.strip() for line in out.splitlines() if line.strip()]


def has_pg_version(node: str) -> bool:
    rc, _ = _sh(
        f"docker compose exec -T {node} test -f {PGDATA_DIR}/PG_VERSION",
        timeout=20.0,
    )
    return rc == 0


def member_ids(token: str) -> set[int]:
    """Node ids the cluster currently lists as members."""
    rc, out = _sh("curl -s -m 5 http://127.0.0.1:9081/api/v1/cluster/members", timeout=15.0)
    if rc != 0:
        return set()
    parsed = api_models.parse_or_none(api_models.Members, out)
    return set() if parsed is None else parsed.node_ids


def run_orphan_slot(timings: fp.SystemTimings) -> Result:
    """A slot nobody consumes must be dropped, and its WAL released.

    Two slots, because the reconciler has to tell them apart: one named the way
    this cluster mints them for a node that is not in membership, and one an
    operator could have created. The first must go; the second must not.
    """
    orphan = f"{fp.replication_slot_prefix()}{ORPHAN_NODE_ID}"
    leader = await_healthy(RECOVERY_TIMEOUT_S)
    for slot in (orphan, FOREIGN_SLOT):
        rc, out = _psql(
            leader,
            f"SELECT pg_create_physical_replication_slot('{slot}', true)",
            timeout=30.0,
        )
        if rc != 0:
            return Result(Case.ORPHAN_SLOT, f"could not create {slot}: {out}", ok=False)
    present = slot_names(leader)
    if orphan not in present or FOREIGN_SLOT not in present:
        return Result(Case.ORPHAN_SLOT, f"slots were never created: {present}", ok=False)

    budget = SLOT_INTERVALS_ALLOWED * timings.slot_ensure_interval_ms / 1_000
    deadline = time.monotonic() + budget
    dropped_after: float | None = None
    while time.monotonic() < deadline:
        if orphan not in slot_names(fp.find_raft_leader() or leader):
            dropped_after = budget - (deadline - time.monotonic())
            break
        time.sleep(2.0)

    survivors = slot_names(fp.find_raft_leader() or leader)
    # Leave nothing pinned behind, whatever the verdict.
    for slot in (orphan, FOREIGN_SLOT):
        if slot in survivors:
            _psql(leader, f"SELECT pg_drop_replication_slot('{slot}')", timeout=30.0)

    if dropped_after is None:
        return Result(
            Case.ORPHAN_SLOT,
            f"{orphan} still present after {budget:.0f}s — WAL pinning is unbounded",
            ok=False,
        )
    if FOREIGN_SLOT not in survivors:
        return Result(
            Case.ORPHAN_SLOT,
            f"{FOREIGN_SLOT} was dropped — the sweep destroyed a slot it does not own",
            ok=False,
        )
    return Result(
        Case.ORPHAN_SLOT,
        f"{orphan} dropped after {dropped_after:.0f}s (budget {budget:.0f}s), "
        f"{FOREIGN_SLOT} left alone",
        ok=True,
    )


def wipe_target(leader: str) -> str:
    """A node that can be wiped and expected back.

    Never the leader (the case deposes it) and never the bootstrap node, which
    answers an empty data directory with `initdb` rather than a join.
    """
    target = next((n for n in fp.JOINING_NODES if n != leader), None)
    if target is None:
        raise EdgeError(
            f"no joining node to wipe: leader {leader}, joining {list(fp.JOINING_NODES)}"
        )
    return target


def run_deposed_mid_copy() -> Result:
    """Depose the clone source while a joining node is copying from it."""
    leader = await_healthy(RECOVERY_TIMEOUT_S)
    target = wipe_target(leader)

    fp.wipe_node_state(target, [RAFT_DIR, PGDATA_DIR])
    fp.start_container(target)

    # Depose the source once the join has actually begun copying — waiting for
    # PGDATA to appear is the protocol state, not a sleep.
    deadline = time.monotonic() + 90.0
    copying = False
    while time.monotonic() < deadline:
        if has_pg_version(target):
            copying = True
            break
        time.sleep(0.5)

    fp.kill_container(leader)
    try:
        if not copying:
            return Result(
                Case.DEPOSED_MID_COPY,
                "the join never began copying, so the source was deposed against nothing",
                ok=False,
            )
        # The joining node must reach a decided state: serving, or down. What it
        # must never do is run on a half-copied directory.
        settle = time.monotonic() + 180.0
        while time.monotonic() < settle:
            if _pg_answers(target):
                return Result(
                    Case.DEPOSED_MID_COPY,
                    f"{target} completed its join after the source was deposed",
                    ok=True,
                )
            state = fp.read_container_runstate(target)
            if state is not None and state.status == "exited":
                return Result(
                    Case.DEPOSED_MID_COPY,
                    f"{target} refused to run rather than serve a half-copied directory",
                    ok=True,
                )
            time.sleep(2.0)
        return Result(
            Case.DEPOSED_MID_COPY,
            f"{target} neither served nor stopped within 180s",
            ok=False,
        )
    finally:
        fp.start_container(leader)


def node_lineage(node: str) -> int | None:
    """The PostgreSQL lineage a node reports for its own data directory."""
    port = fp.MGMT_PORTS[node]
    rc, out = _sh(f"curl -s -m 5 http://127.0.0.1:{port}/api/v1/cluster/identity", timeout=15.0)
    if rc != 0:
        return None
    identity = api_models.parse_or_none(api_models.ClusterIdentity, out)
    return None if identity is None else identity.cluster_lineage


def run_bootstrap_wiped(token: str) -> Result:
    """Wipe the bootstrap node and require the cluster to absorb it.

    The bootstrap node answers an empty state directory with `initdb`, so it
    comes back holding a lineage this cluster never wrote to, under an id the
    cluster still lists, on the address its peers join through. Two things must
    hold afterwards: it ends up on the cluster's lineage rather than its own,
    and no peer discarded its own Raft state on the strength of a membership
    answer from a cluster of one.
    """
    leader = await_healthy(RECOVERY_TIMEOUT_S)
    target = next((n for n in fp.NODES if n not in fp.JOINING_NODES), None)
    if target is None:
        return Result(Case.BOOTSTRAP_WIPED, "no node starts with --bootstrap", ok=False)
    before = member_ids(token)
    lineage_before = node_lineage(leader if leader != target else _other(target))
    if lineage_before is None:
        return Result(Case.BOOTSTRAP_WIPED, "the cluster reports no lineage to compare", ok=False)

    fp.wipe_node_state(target, [RAFT_DIR, PGDATA_DIR])
    fp.start_container(target)

    deadline = time.monotonic() + RECOVERY_TIMEOUT_S
    while time.monotonic() < deadline:
        if node_lineage(target) == lineage_before and _pg_answers(target):
            break
        time.sleep(2.0)
    else:
        return Result(
            Case.BOOTSTRAP_WIPED,
            f"{target} never returned to the cluster's lineage {lineage_before} "
            f"(it reports {node_lineage(target)})",
            ok=False,
        )

    after = member_ids(token)
    if after != before:
        return Result(
            Case.BOOTSTRAP_WIPED,
            f"membership changed across the wipe: {sorted(before)} -> {sorted(after)}",
            ok=False,
        )
    survivors = [n for n in fp.NODES if n != target]
    kept_state = [n for n in survivors if _pg_answers(n)]
    if len(kept_state) != len(survivors):
        return Result(
            Case.BOOTSTRAP_WIPED,
            f"only {kept_state} of {survivors} still serve; a peer was taken down with it",
            ok=False,
        )
    return Result(
        Case.BOOTSTRAP_WIPED,
        f"{target} re-provisioned onto lineage {lineage_before}, membership and peers intact",
        ok=True,
    )


def _other(node: str) -> str:
    return next(n for n in fp.NODES if n != node)


def run_learner_crash(token: str) -> Result:
    """A node that dies mid-join must not leave a phantom member behind."""
    leader = await_healthy(RECOVERY_TIMEOUT_S)
    target = wipe_target(leader)
    before = member_ids(token)

    fp.wipe_node_state(target, [RAFT_DIR, PGDATA_DIR])
    fp.start_container(target)
    # Kill it the moment the clone starts — the join has registered by then.
    deadline = time.monotonic() + 90.0
    while time.monotonic() < deadline and not has_pg_version(target):
        time.sleep(0.2)
    fp.kill_container(target)

    after = member_ids(token)
    fp.start_container(target)
    with contextlib.suppress(EdgeError):
        await_healthy(RECOVERY_TIMEOUT_S)
    final = member_ids(token)

    if final - before:
        return Result(
            Case.LEARNER_CRASH,
            f"membership gained {sorted(final - before)} that was never there before",
            ok=False,
        )
    return Result(
        Case.LEARNER_CRASH,
        f"membership unchanged across the crash ({sorted(before)} -> {sorted(final)}, "
        f"mid-crash {sorted(after)})",
        ok=True,
    )


@app.command()
def run(
    case: str = typer.Option("all", "--case", help=f"{[c.value for c in Case]}, or 'all'."),
    token: str = typer.Option("local-ci-token", "--token", help="Management API token."),
) -> None:
    """Drive the join and rejoin edges."""
    timings = fp.read_system_timings()
    selected = list(Case) if case == "all" else [Case(case)]
    results: list[Result] = []
    for chosen in selected:
        console.print(f"[dim]{chosen}[/]")
        try:
            if chosen is Case.ORPHAN_SLOT:
                results.append(run_orphan_slot(timings))
            elif chosen is Case.DEPOSED_MID_COPY:
                results.append(run_deposed_mid_copy())
            elif chosen is Case.BOOTSTRAP_WIPED:
                results.append(run_bootstrap_wiped(token))
            else:
                results.append(run_learner_crash(token))
        except (EdgeError, fp.FaultError) as exc:
            results.append(Result(chosen, f"could not be driven: {exc}", ok=False))
        try:
            await_healthy(RECOVERY_TIMEOUT_S)
        except EdgeError as exc:
            console.print(f"[yellow]cluster did not fully recover after {chosen}: {exc}[/]")

    table = Table(title="Join and rejoin edges (H-16)")
    table.add_column("Case", style="bold")
    table.add_column("Result")
    table.add_column("Detail")
    for r in results:
        table.add_row(r.case.value, "[green]PASS[/]" if r.ok else "[red]FAIL[/]", r.detail)
    console.print(table)

    if [r for r in results if not r.ok]:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
