#!/usr/bin/env -S uv run --project testing python
"""Track B: gateway hold under continuous writes + freeze/unfreeze (step 5/6).

Exercises the upgrade freeze without needing a second PG major:
  freeze -> writers stall -> unfreeze -> writers resume, no lost acks.

The freeze is a committed Raft entry, so one call covers the cluster and the
state outlives any node. Every path here therefore unfreezes on the way out:
a harness that left the cluster frozen would wedge every case after it.

Chaos modes:
  --chaos mid-freeze    kill the leader while frozen
  --chaos mid-unfreeze  kill during the unfreeze window
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Final

import httpx
import psycopg
import typer
from rich.console import Console

import topology
from topology import Node, Topology, load
from waiting import WaitTimeout, await_until

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
TOKEN: Final[str] = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")

console = Console()
app = typer.Typer(add_completion=False)


class ContractFailure(RuntimeError):
    """The hold did not behave as Track B requires."""


def _headers() -> dict[str, str]:
    if not TOKEN:
        raise SystemExit("PGBATTERY_MANAGEMENT_API_TOKEN required")
    return {"x-pgbattery-token": TOKEN}


def _leader_mgmt(topo: Topology) -> str:
    for n in topo.nodes:
        try:
            r = httpx.get(
                f"http://127.0.0.1:{n.mgmt_port}/api/v1/cluster/leader",
                timeout=2.0,
            )
            if r.status_code == 200 and r.json().get("leader_mgmt_addr"):
                return f"http://{r.json()['leader_mgmt_addr']}"
        except (httpx.HTTPError, ValueError):
            continue
    raise SystemExit("no leader")


def _set_freeze(topo: Topology, frozen: bool, timeout: float = 120.0) -> None:
    """Commit the freeze. Any node can propose it once a leader exists.

    Retried across the whole window because the unfreeze after a chaos kill
    has to wait out an election, and leaving the cluster frozen would wedge
    every case that runs after this one.
    """
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        for n in topo.nodes:
            try:
                r = httpx.post(
                    f"http://127.0.0.1:{n.mgmt_port}/api/v1/admin/postgres-upgrade/freeze",
                    headers=_headers(),
                    json={"frozen": frozen},
                    timeout=30.0,
                )
                if r.status_code == 200:
                    return
                last = r.text
            except httpx.HTTPError as exc:
                last = str(exc)
        time.sleep(2.0)
    raise ContractFailure(f"could not set frozen={frozen} on any node in {timeout}s: {last}")


def _assert_acked_survived(topo: Topology, acked: int) -> None:
    """Every row the server acked is still there afterwards (W1)."""
    if acked == 0:
        raise ContractFailure("W1 check is vacuous: the writer acked nothing")
    r = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            topo.nodes[0].service,
            "psql",
            "-h",
            "127.0.0.1",
            "-p",
            str(topology.PG_INTERNAL_PORT),
            "-U",
            "postgres",
            "-At",
            "-c",
            "SELECT count(*) FROM hold_probe;",
        ],
        check=False,
        text=True,
        capture_output=True,
        env={**os.environ},
    )
    if r.returncode != 0:
        raise ContractFailure(f"could not count hold_probe: {r.stderr or r.stdout}")
    present = int(r.stdout.strip())
    if present < acked:
        raise ContractFailure(f"W1 violated: {acked} writes acked, {present} present")
    console.print(f"W1 OK: all {acked} acked writes survived the hold")


class Writer(threading.Thread):
    """Continuous gateway writes, counting only what the server acked."""

    def __init__(self, gateway_port: int) -> None:
        super().__init__(name="hold-writer", daemon=True)
        self._port = gateway_port
        # Not `_stop`: that is a Thread internal on CPython 3.12, and
        # rebinding it makes join() raise at teardown.
        self._stop_event = threading.Event()
        self.acked = 0
        self.errors = 0

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                with psycopg.connect(
                    host="127.0.0.1",
                    port=self._port,
                    user="postgres",
                    dbname="postgres",
                    connect_timeout=3,
                ) as conn:
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute(
                            "CREATE TABLE IF NOT EXISTS hold_probe("
                            "id bigserial PRIMARY KEY, t timestamptz default now())"
                        )
                        while not self._stop_event.is_set():
                            cur.execute("INSERT INTO hold_probe DEFAULT VALUES")
                            self.acked += 1
                            time.sleep(0.05)
            except (psycopg.Error, OSError):
                self.errors += 1
                time.sleep(0.2)

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=10)


def _held_connections(node: Node) -> int | None:
    """Connections parked for an upgrade hold. `None` is "no reading", not zero."""
    try:
        r = httpx.get(f"http://127.0.0.1:{node.metrics_port}/metrics", timeout=5.0)
    except httpx.HTTPError:
        return None
    if r.status_code != 200:
        return None
    for line in r.text.splitlines():
        if line.startswith("#"):
            continue
        series, _, value = line.rpartition(" ")
        name = series.partition("{")[0].strip()
        if name in (
            "pgbattery_connections_held_during_upgrade",
            "pgbattery_connections_held_during_upgrade_total",
        ):
            return int(float(value))
    return 0


def _await_gateway_holding(node: Node, before: int, timeout: float = 30.0) -> None:
    """The freeze commits before gateways observe it; the counter says when."""

    def holding() -> bool | None:
        held = _held_connections(node)
        return True if held is not None and held > before else None

    try:
        await_until(
            holding,
            budget=timeout,
            describe=f"{node.service} to hold a connection",
            interval=0.25,
        )
    except WaitTimeout as exc:
        raise ContractFailure(f"freeze committed but {exc}") from exc


@app.command()
def main(
    chaos: str | None = typer.Option(None, help="mid-freeze | mid-unfreeze"),
    hold_sec: float = typer.Option(5.0, "--hold-sec"),
) -> None:
    if chaos not in (None, "mid-freeze", "mid-unfreeze"):
        raise SystemExit(f"unknown --chaos {chaos!r}")
    topo = load()
    _leader_mgmt(topo)

    writer = Writer(topo.nodes[0].gateway_port)
    writer.start()
    time.sleep(1.0)
    before = writer.acked
    if before == 0:
        raise ContractFailure("writer acked nothing before the freeze; the run proves nothing")

    held_before = _held_connections(topo.nodes[0]) or 0
    try:
        _set_freeze(topo, True)
        _await_gateway_holding(topo.nodes[0], held_before)
        # Baseline taken after the gateway is demonstrably holding, so any
        # later ack is a write that got through the hold, not through the
        # window before it took effect.
        held_from = writer.acked

        if chaos == "mid-freeze":
            subprocess.run(
                ["docker", "compose", "kill", "node1"],
                cwd=PROJECT_ROOT,
                check=False,
            )
            time.sleep(3)
            during = writer.acked - held_from
            if during > 0:
                raise ContractFailure(
                    f"L1/V1: {during} writes acked while frozen with the leader killed"
                )
            console.print("chaos mid-freeze OK: no writes acked while frozen")
            return

        time.sleep(hold_sec)
        during = writer.acked - held_from
        # The whole point of the freeze is that clients stop making progress.
        # Without this the case passes just as happily on a freeze that did
        # nothing at all.
        if during > 0:
            raise ContractFailure(
                f"hold did not hold: {during} writes acked during a {hold_sec}s freeze"
            )
        console.print(f"hold OK: 0 writes acked across {hold_sec}s")
    finally:
        _set_freeze(topo, False)

    if chaos == "mid-unfreeze":
        subprocess.run(
            ["docker", "compose", "kill", "node1"],
            cwd=PROJECT_ROOT,
            check=False,
        )

    deadline = time.monotonic() + 60
    resumed = writer.acked
    while time.monotonic() < deadline and writer.acked <= resumed:
        time.sleep(0.5)
    after = writer.acked
    writer.stop()

    # W1: the hold stalls clients, it does not lose their completed work.
    if chaos is None:
        _assert_acked_survived(topo, after)

    console.print(
        f"acked before={before} during_hold=0 "
        f"after_unfreeze={after - resumed} errors={writer.errors}"
    )
    if after <= resumed and chaos is None:
        raise ContractFailure("writers never resumed after unfreeze")
    console.print("pg_upgrade hold chaos OK")


if __name__ == "__main__":
    app()
