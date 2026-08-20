#!/usr/bin/env -S uv run --project testing python
"""H-14: how much acknowledged WAL does pg_rewind actually destroy?

The rewind gate tolerates divergence up to `PG_REWIND_DIVERGENCE_THRESHOLD_BYTES`
(16 MiB) and its justification is that synchronous replication is holding the
acked writes elsewhere. During the async fallback that assumption is exactly
false: the replication manager has deliberately emptied
`synchronous_standby_names`, so the leader acknowledges writes it alone holds.
Depose it there and the survivors elect a leader without them; the old leader
then rewinds, and the diverged WAL — those acknowledged writes — is discarded.

The fallback was reachable. Nothing measured the loss. This does:

  1. sever streaming replication, leaving Raft healthy, until the leader
     reports the async fallback
  2. write acknowledged rows in that window
  3. depose the leader and let the survivors elect
  4. heal, let the old leader rewind, and count how many of those rows survive

The number it prints is the RPO of the fallback window, in acknowledged writes.
"""

from __future__ import annotations

import subprocess
import threading
import time
from dataclasses import dataclass
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
TABLE: Final[str] = "rewind_loss_probe"
LOAD_TABLE: Final[str] = "rewind_loss_load"
RECOVERY_TIMEOUT_S: Final[float] = 300.0

# `pgbattery_replication_sync` is 1.0 while a sync list is configured and 0.0
# once the replication manager has fallen back to async. That edge is the
# window this measures, so it is waited for rather than slept past.
SYNC_METRIC: Final[str] = "pgbattery_replication_sync"


class RewindError(RuntimeError):
    """The run could not be carried out as specified."""


@dataclass
class Measurement:
    acked: int
    survived: int
    fallback_observed: bool
    old_leader: str
    new_leader: str | None

    @property
    def destroyed(self) -> int:
        return max(0, self.acked - self.survived)

    @property
    def verdict(self) -> str:
        if not self.fallback_observed:
            return "SKIP: the async fallback never engaged, so nothing was measured"
        if self.new_leader is None or self.new_leader == self.old_leader:
            return "SKIP: leadership never moved, so no divergence was created"
        if self.acked == 0:
            return "SKIP: no write was acknowledged in the fallback window"
        return f"MEASURED: {self.destroyed} of {self.acked} acknowledged writes destroyed"


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


def await_healthy(timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        leader = fp.find_raft_leader()
        if leader is not None and all(_pg_answers(n) for n in fp.NODES):
            return leader
        time.sleep(2.0)
    raise RewindError(f"cluster did not become healthy within {timeout_s:.0f}s")


def _pg_answers(node: str) -> bool:
    rc, out = _psql(node, "SELECT 1", timeout=20.0)
    return rc == 0 and out.strip() == "1"


def await_async_fallback(leader: str, timeout_s: float) -> bool:
    """Wait until the replication manager has emptied the sync list."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = fp.read_metric(leader, SYNC_METRIC)
        if value is not None and value < 1.0:
            return True
        time.sleep(1.0)
    return False


def write_acked_rows(leader: str, count: int) -> int:
    """Rows the leader acknowledged. Each is its own transaction, so a block
    part-way through still leaves the earlier ones acknowledged."""
    acked = 0
    for index in range(count):
        rc, _ = _psql(
            leader,
            f"INSERT INTO {TABLE}(tag) VALUES ('fallback-{index}')",
            timeout=20.0,
        )
        if rc == 0:
            acked += 1
    return acked


def count_rows(node: str) -> int | None:
    rc, out = _psql(node, f"SELECT count(*) FROM {TABLE} WHERE tag LIKE 'fallback-%'")
    if rc != 0 or not out.strip().isdigit():
        return None
    return int(out.strip())


@app.command()
def run(
    writes: int = typer.Option(30, "--writes", help="Acked rows to attempt in the window."),
    fallback_timeout: float = typer.Option(
        90.0, "--fallback-timeout", help="Seconds to wait for the async fallback."
    ),
) -> None:
    """Force the async fallback, depose the leader, and count what rewind ate."""
    leader = await_healthy(RECOVERY_TIMEOUT_S)
    standbys = [n for n in fp.NODES if n != leader]
    console.print(f"[bold]H-14 rewind loss[/] — leader {leader}, standbys {', '.join(standbys)}")

    rc, out = _psql(
        leader,
        f"CREATE TABLE IF NOT EXISTS {TABLE}(tag text primary key); "
        f"DELETE FROM {TABLE} WHERE tag LIKE 'fallback-%'; "
        f"CREATE TABLE IF NOT EXISTS {LOAD_TABLE}(payload text); "
        f"TRUNCATE {LOAD_TABLE}",
        timeout=60.0,
    )
    if rc != 0:
        raise RewindError(f"could not prepare {TABLE}: {out}")

    # Replication traffic has to be in flight or the DROP rules match nothing
    # and the primitive refuses — an idle cluster streams no WAL to sever.
    stop_load = threading.Event()

    def keep_writing() -> None:
        # Bulk, not row-at-a-time: one INSERT per `docker exec` is about one a
        # second and produces too little WAL for the DROP rule to see anything
        # inside its settle window.
        while not stop_load.is_set():
            try:
                _psql(
                    leader,
                    f"INSERT INTO {LOAD_TABLE}(payload) "
                    "SELECT repeat('x', 200) FROM generate_series(1, 20000)",
                    timeout=60.0,
                )
            except subprocess.TimeoutExpired:
                # Expected: once the sync list still names an unreachable
                # standby the write blocks. The load has already done its job
                # by then — it exists to make the partition observable.
                return

    loader = threading.Thread(target=keep_writing, name="rewind-loss-load", daemon=True)
    loader.start()

    acked = 0
    fallback = False
    # Sever streaming replication on each standby, which is where the rule has
    # to live: the leader streams continuously while a standby answers only
    # every wal_receiver_status_interval, so a rule on the leader matches
    # nothing and partitions nothing.
    with (
        fp.partition_channel(standbys[0], [leader], fp.Channel.REPLICATION, settle_s=15.0),
        fp.partition_channel(standbys[1], [leader], fp.Channel.REPLICATION, settle_s=15.0),
    ):
        fallback = await_async_fallback(leader, fallback_timeout)
        stop_load.set()
        loader.join(timeout=30.0)
        if fallback:
            acked = write_acked_rows(leader, writes)
            console.print(f"[dim]{acked} rows acknowledged with the sync list empty[/]")
        fp.kill_container(leader)
        # The survivors have to elect before the partition heals, or the old
        # leader's WAL never diverges and there is nothing for rewind to eat.
        deadline = time.monotonic() + 120.0
        new_leader: str | None = None
        while time.monotonic() < deadline and new_leader is None:
            candidate = fp.find_raft_leader(standbys)
            if candidate is not None and candidate != leader:
                new_leader = candidate
            else:
                time.sleep(1.0)

    fp.start_container(leader)
    try:
        await_healthy(RECOVERY_TIMEOUT_S)
    except RewindError as exc:
        console.print(f"[yellow]cluster did not fully recover: {exc}[/]")

    survived = count_rows(new_leader or standbys[0])
    measurement = Measurement(
        acked=acked,
        survived=survived or 0,
        fallback_observed=fallback,
        old_leader=leader,
        new_leader=new_leader,
    )

    table = Table(title="What pg_rewind discarded (H-14)")
    table.add_column("Acked in fallback", justify="right")
    table.add_column("Survived", justify="right")
    table.add_column("Destroyed", justify="right")
    table.add_column("Old leader")
    table.add_column("New leader")
    table.add_row(
        str(measurement.acked),
        "-" if survived is None else str(survived),
        str(measurement.destroyed),
        measurement.old_leader,
        str(measurement.new_leader),
    )
    console.print(table)
    console.print(measurement.verdict)

    if measurement.verdict.startswith("SKIP"):
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
