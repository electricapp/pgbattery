#!/usr/bin/env -S uv run --project testing python
"""Track B: gateway hold under continuous writes + freeze/unfreeze (step 5/6).

Exercises the upgrade freeze without needing a second PG major:
  freeze -> writers stall -> unfreeze -> writers resume, no lost acks.

The freeze is a committed Raft entry, so one call covers the cluster and the
state outlives any node. Every path here therefore unfreezes on the way out:
a harness that left the cluster frozen would wedge every case after it.

Writers run against the gateways of the two nodes that are NOT the leader, so
killing the leader cannot silence them: an ack during the freeze reaches the
harness through a surviving gateway and fails the case. A writer pinned to
the leader's own gateway dies with the leader, and its silence proves
nothing.

Chaos modes:
  --chaos mid-freeze    kill the leader while frozen; the freeze is committed
                        state and must hold through the failover
  --chaos mid-unfreeze  kill the leader right after the unfreeze commits;
                        writers must resume and the W1 ledger stay intact
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
# Bounds any single docker call; the cluster is already up when this runs.
COMPOSE_TIMEOUT_SEC: Final[float] = 300.0

console = Console()
app = typer.Typer(add_completion=False)


class ContractFailure(RuntimeError):
    """The hold did not behave as Track B requires."""


def _headers() -> dict[str, str]:
    if not TOKEN:
        raise SystemExit("PGBATTERY_MANAGEMENT_API_TOKEN required")
    return {"x-pgbattery-token": TOKEN}


def _compose(
    *args: str, check: bool = False, timeout: float = COMPOSE_TIMEOUT_SEC
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["docker", "compose", *args],
            cwd=PROJECT_ROOT,
            check=check,
            text=True,
            capture_output=True,
            env={**os.environ},
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"docker compose {' '.join(args)} hung for {timeout:g}s") from exc


def _leader_node(topo: Topology, timeout: float = 120.0) -> Node:
    """The actual leader, from the management API — never an assumed node."""
    last = ""

    def leader() -> Node | None:
        nonlocal last
        for n in topo.nodes:
            try:
                r = httpx.get(
                    f"http://127.0.0.1:{n.mgmt_port}/api/v1/cluster/leader",
                    timeout=2.0,
                )
            except httpx.HTTPError as exc:
                last = str(exc)
                continue
            if r.status_code != 200:
                last = r.text
                continue
            lid = r.json().get("leader_id")
            if lid is None:
                last = r.text
                continue
            for cand in topo.nodes:
                if cand.node_id == int(lid):
                    return cand
        return None

    try:
        return await_until(leader, budget=timeout, describe="a leader via the management API")
    except WaitTimeout as exc:
        raise SystemExit(f"no leader: {last} ({exc})") from exc


def _kill_and_await_down(node: Node, budget: float = 30.0) -> None:
    """Kill a container and confirm the fault's effect: it is not running."""
    _compose("kill", node.service, check=True)

    def down() -> bool | None:
        state = _compose("ps", "-a", "--format", "{{.Service}} {{.State}}").stdout
        for line in state.splitlines():
            service, _, status = line.partition(" ")
            if service == node.service and status.strip().startswith("running"):
                return None
        return True

    try:
        await_until(down, budget=budget, describe=f"{node.service} to be down after the kill")
    except WaitTimeout as exc:
        raise ContractFailure(f"the kill did not land: {exc}") from exc


def _await_new_leader(survivors: list[Node], dead: Node, budget: float = 90.0) -> Node:
    """The survivors' own view has moved past the killed leader."""

    def elected() -> Node | None:
        for n in survivors:
            try:
                r = httpx.get(
                    f"http://127.0.0.1:{n.mgmt_port}/api/v1/cluster/leader",
                    timeout=2.0,
                )
            except httpx.HTTPError:
                continue
            if r.status_code != 200:
                continue
            lid = r.json().get("leader_id")
            if lid is None or int(lid) == dead.node_id:
                continue
            for cand in survivors:
                if cand.node_id == int(lid):
                    return cand
        return None

    try:
        return await_until(
            elected, budget=budget, describe=f"a new leader after {dead.service} died"
        )
    except WaitTimeout as exc:
        raise ContractFailure(
            f"no survivor took over after {dead.service} was killed: {exc}"
        ) from exc


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


def _present_keys(node: Node) -> set[str]:
    """Keys actually committed, read from one node's own PostgreSQL."""
    proc = _compose(
        "exec",
        "-T",
        node.service,
        "psql",
        "-h",
        "127.0.0.1",
        "-p",
        str(topology.PG_INTERNAL_PORT),
        "-U",
        "postgres",
        "-At",
        "-c",
        "SELECT k FROM hold_probe WHERE k LIKE 'w-%';",
    )
    if proc.returncode != 0:
        raise ContractFailure(f"could not read hold_probe keys: {proc.stderr or proc.stdout}")
    return set(proc.stdout.split())


def _assert_acked_survived(node: Node, acked: set[str]) -> None:
    """W1 by key identity, not count: a committed retry the writer never
    counted as acked can offset a lost acked row in any count comparison."""
    if not acked:
        raise ContractFailure("W1 check is vacuous: the writers acked nothing")
    missing = sorted(acked - _present_keys(node))
    if missing:
        raise ContractFailure(f"W1 violated: {len(missing)} acked keys missing, e.g. {missing[:5]}")
    console.print(f"W1 OK: all {len(acked)} acked keys survived")


class Writer(threading.Thread):
    """Continuous keyed gateway writes, recording only keys the server acked."""

    def __init__(self, gateway_port: int, prefix: str) -> None:
        super().__init__(name=f"hold-writer-{prefix}", daemon=True)
        self._port = gateway_port
        self._prefix = prefix
        # Not `_stop`: that is a Thread internal on CPython 3.12, and
        # rebinding it makes join() raise at teardown.
        self._stop_event = threading.Event()
        self.acked: list[str] = []
        self.errors = 0

    def run(self) -> None:
        i = 0
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
                        while not self._stop_event.is_set():
                            key = f"{self._prefix}-{i}"
                            cur.execute(
                                "INSERT INTO hold_probe(k) VALUES (%s) ON CONFLICT (k) DO NOTHING",
                                (key,),
                            )
                            self.acked.append(key)
                            i += 1
                            time.sleep(0.05)
            except (psycopg.Error, OSError):
                self.errors += 1
                time.sleep(0.2)

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=10)


def _acked_total(writers: list[Writer]) -> int:
    return sum(len(w.acked) for w in writers)


def _seed(gateway_port: int) -> None:
    """The probe table, created once so concurrent writers never race DDL."""
    with psycopg.connect(
        host="127.0.0.1",
        port=gateway_port,
        user="postgres",
        dbname="postgres",
        connect_timeout=5,
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS hold_probe(k text PRIMARY KEY)")


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
    _headers()
    topo = load()
    leader = _leader_node(topo)
    survivors = [n for n in topo.voters if n.node_id != leader.node_id]
    if len(survivors) < 2:
        raise SystemExit(f"need two non-leader gateways, got {[n.service for n in survivors]}")
    console.print(f"leader={leader.service}; writers on {[n.service for n in survivors]}")

    _seed(survivors[0].gateway_port)
    writers = [Writer(n.gateway_port, f"w-{n.service}") for n in survivors]
    for w in writers:
        w.start()

    def every_writer_acked() -> bool | None:
        return True if all(w.acked for w in writers) else None

    try:
        await_until(every_writer_acked, budget=30.0, describe="each writer's first acked key")
    except WaitTimeout as exc:
        raise ContractFailure(
            f"writers acked nothing before the freeze; the run proves nothing: {exc}"
        ) from exc

    held_before = _held_connections(survivors[0]) or 0
    killed: Node | None = None
    try:
        _set_freeze(topo, True)
        _await_gateway_holding(survivors[0], held_before)
        # Baseline taken after a surviving gateway is demonstrably holding, so
        # any later ack is a write that got through the hold, not through the
        # window before it took effect.
        held_from = _acked_total(writers)

        if chaos == "mid-freeze":
            killed = leader
            _kill_and_await_down(leader)
            new_leader = _await_new_leader(survivors, leader)
            console.print(
                f"{leader.service} confirmed down; {new_leader.service} leads while frozen"
            )
            # Elapsed time is the point: the freeze is committed state and must
            # keep holding across the whole window the failover sits inside.
            time.sleep(hold_sec)
            during = _acked_total(writers) - held_from
            if during > 0:
                raise ContractFailure(
                    f"L1/V1 violated: {during} writes acked through surviving gateways "
                    f"while frozen, spanning the failover to {new_leader.service}"
                )
            console.print(
                f"mid-freeze OK: 0 acks through surviving gateways across {hold_sec:g}s "
                "spanning a leader kill and re-election"
            )
        else:
            time.sleep(hold_sec)
            during = _acked_total(writers) - held_from
            # The whole point of the freeze is that clients stop making
            # progress; a freeze that did nothing passes without this.
            if during > 0:
                raise ContractFailure(
                    f"hold did not hold: {during} writes acked during a {hold_sec:g}s freeze"
                )
            console.print(f"hold OK: 0 writes acked across {hold_sec:g}s")

        if chaos == "mid-unfreeze":
            # The kill lands inside the unfreeze window: after the unfreeze
            # commits, before writers have observably resumed.
            _set_freeze(topo, False)
            killed = leader
            _kill_and_await_down(leader)
            new_leader = _await_new_leader(survivors, leader)
            console.print(
                f"{leader.service} confirmed down mid-unfreeze; {new_leader.service} leads"
            )
    finally:
        if killed is not None:
            _compose("start", killed.service)
        _set_freeze(topo, False)

    resumed_from = _acked_total(writers)

    def resumed() -> bool | None:
        return True if _acked_total(writers) > resumed_from else None

    try:
        await_until(resumed, budget=90.0, describe="writers to resume after unfreeze")
    except WaitTimeout as exc:
        raise ContractFailure(f"writers never resumed after unfreeze: {exc}") from exc

    for w in writers:
        w.stop()
    acked: set[str] = set()
    for w in writers:
        acked |= set(w.acked)
    errors = sum(w.errors for w in writers)

    # W1 reads from the current leader: a standby can lag acks it has not yet
    # applied, and a key missing there is replication lag, not loss.
    _assert_acked_survived(_leader_node(topo), acked)
    console.print(
        f"acked total={len(acked)} during_hold=0 "
        f"after_unfreeze={_acked_total(writers) - resumed_from} errors={errors}"
    )
    console.print("pg_upgrade hold chaos OK")


if __name__ == "__main__":
    app()
