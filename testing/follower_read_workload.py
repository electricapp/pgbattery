#!/usr/bin/env -S uv run --project testing python
"""H-10: drive follower reads and predicate reads against a live cluster.

Writes go through the gateway to the leader and record their commit LSN. Reads
are served by each standby and record that standby's replay position, which is
what makes them checkable rather than merely stale: lagging is legal, but a
standby that has replayed past a commit and still serves the older value has a
visibility bug.

Predicate reads (`WHERE val > n`) run alongside, because a point read cannot
express a phantom.

Checkers live in `linreg/follower_reads.py` and are pure; `test_follower_reads.py`
shows each one rejecting the history it is meant to catch.
"""

from __future__ import annotations

import random
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import fault_primitives as fp
from linreg.follower_reads import (
    FollowerRead,
    LeaderWrite,
    PredicateRead,
    no_invented_values,
    no_long_fork,
    predicate_reads_gain_only_written_keys,
    reads_are_monotonic,
    replayed_writes_are_visible,
)

app = typer.Typer(add_completion=False)
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
INTERNAL_PG_PORT: Final[int] = 5434
TABLE: Final[str] = "follower_read_probe"
NUM_KEYS: Final[int] = 8
PREDICATE_THRESHOLD: Final[int] = 500


class WorkloadError(RuntimeError):
    """The run could not be carried out as specified."""


@dataclass
class Recorded:
    writes: list[LeaderWrite]
    reads: list[FollowerRead]
    predicates: list[PredicateRead]


def _psql(node: str, sql: str, timeout: float = 30.0) -> tuple[int, str]:
    import os

    env = dict(os.environ)
    env.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "ci-token")
    proc = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            node,
            "psql",
            "-U",
            "postgres",
            "-h",
            "127.0.0.1",
            "-p",
            str(INTERNAL_PG_PORT),
            "-d",
            "postgres",
            "-At",
            "-F",
            "|",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=PROJECT_ROOT,
        env=env,
        check=False,
    )
    return proc.returncode, proc.stdout.strip()


def setup(leader: str) -> None:
    rc, out = _psql(
        leader,
        f"CREATE TABLE IF NOT EXISTS {TABLE}(key int primary key, val int not null); "
        f"INSERT INTO {TABLE} SELECT g, 0 FROM generate_series(1, {NUM_KEYS}) g "
        "ON CONFLICT (key) DO UPDATE SET val = 0",
        timeout=60.0,
    )
    if rc != 0:
        raise WorkloadError(f"could not create the probe table on {leader}: {out}")


def write_once(leader: str, key: int, value: int) -> LeaderWrite | None:
    """Write through the leader and return a position at or past its commit.

    The position is read in a *second* statement. Asking for it in the same one
    puts both in a single implicit transaction, so the value comes back from
    before the commit record — and a standby replayed to exactly there has not
    applied the write, which reads as a visibility bug that did not happen.
    """
    rc, _ = _psql(leader, f"UPDATE {TABLE} SET val = {value} WHERE key = {key}", timeout=30.0)
    if rc != 0:
        return None
    rc, out = _psql(leader, "SELECT pg_current_wal_lsn()", timeout=30.0)
    if rc != 0 or not out:
        return None
    lsn = out.splitlines()[-1].strip()
    if "/" not in lsn:
        return None
    return LeaderWrite(key=key, value=value, commit_lsn=lsn, at=time.monotonic())


def read_from_follower(node: str, key: int) -> FollowerRead | None:
    """Read a key on a standby, with the replay position at that instant.

    One statement, so the value and the position describe the same moment; two
    round trips could let replay advance in between and make a stale read look
    like a visibility bug.
    """
    rc, out = _psql(
        node,
        f"SELECT val, pg_last_wal_replay_lsn() FROM {TABLE} WHERE key = {key}",
        timeout=30.0,
    )
    if rc != 0 or not out:
        return None
    parts = out.splitlines()[-1].split("|")
    if len(parts) != 2 or not parts[0].strip().isdigit() or "/" not in parts[1]:
        return None
    return FollowerRead(
        node=node,
        key=key,
        value=int(parts[0]),
        replay_lsn=parts[1].strip(),
        at=time.monotonic(),
    )


def predicate_read(node: str, threshold: int) -> PredicateRead | None:
    rc, out = _psql(node, f"SELECT key FROM {TABLE} WHERE val > {threshold} ORDER BY key")
    if rc != 0:
        return None
    keys = [int(line) for line in out.splitlines() if line.strip().isdigit()]
    return PredicateRead(node=node, threshold=threshold, keys=keys, at=time.monotonic())


def run_workload(leader: str, standbys: list[str], rounds: int) -> Recorded:
    recorded = Recorded(writes=[], reads=[], predicates=[])
    rng = random.Random(20_260_819)

    for round_index in range(rounds):
        key = rng.randint(1, NUM_KEYS)
        # Monotonically increasing per key, so a regression is unambiguous.
        value = (round_index + 1) * 10
        written = write_once(leader, key, value)
        if written is not None:
            recorded.writes.append(written)

        for node in standbys:
            got = read_from_follower(node, key)
            if got is not None:
                recorded.reads.append(got)

        if round_index % 3 == 0:
            for node in standbys:
                seen = predicate_read(node, PREDICATE_THRESHOLD)
                if seen is not None:
                    recorded.predicates.append(seen)

    return recorded


@app.command()
def run(
    rounds: int = typer.Option(40, "--rounds", help="Write/read rounds to drive."),
) -> None:
    """Drive follower and predicate reads, then check what they observed."""
    leader = fp.find_raft_leader()
    if leader is None:
        console.print("[red]no leader[/]")
        raise typer.Exit(code=2)
    standbys = [n for n in fp.NODES if n != leader]
    console.print(f"[bold]H-10 follower reads[/] — leader {leader}, standbys {', '.join(standbys)}")

    setup(leader)
    recorded = run_workload(leader, standbys, rounds)

    if not recorded.reads:
        console.print("[red]no follower read completed — nothing was checked[/]")
        raise typer.Exit(code=2)

    checks = [
        ("no invented values", *no_invented_values(recorded.reads, recorded.writes)),
        ("monotonic follower reads", *reads_are_monotonic(recorded.reads)),
        (
            "replayed writes are visible",
            *replayed_writes_are_visible(recorded.reads, recorded.writes),
        ),
        ("no long fork", *no_long_fork(recorded.reads)),
        (
            "predicate matches are explained",
            *predicate_reads_gain_only_written_keys(recorded.predicates, recorded.writes),
        ),
    ]

    table = Table(title="Follower and predicate reads (H-10)")
    table.add_column("Property", style="bold")
    table.add_column("Result")
    table.add_column("Detail")
    for name, ok, detail in checks:
        table.add_row(name, "[green]held[/]" if ok else "[red]VIOLATED[/]", detail)
    console.print(table)
    console.print(
        f"[dim]{len(recorded.writes)} writes, {len(recorded.reads)} follower reads, "
        f"{len(recorded.predicates)} predicate reads[/]"
    )

    if [c for c in checks if not c[1]]:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
