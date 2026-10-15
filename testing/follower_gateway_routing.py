#!/usr/bin/env -S uv run --project testing python
"""RW-6: a follower's gateway must not route a write to a deposed primary.

The leader's Raft port is cut in both directions while its PostgreSQL stays
reachable, so the cluster elects past it and a follower's gateway can still
reach the old primary for the whole detection interval. A writer runs through
that follower's gateway the entire time, and every row records which server
accepted it (`inet_server_addr()`) and when (`clock_timestamp()`; the
containers share the host's clock).

Oracles, all data-plane:

  W1    every key the writer was acked is on the final leader
  RW-6  no write landed on the deposed primary once the new one was taking
        them: in the writer's own ack order, no ack from the deposed node
        follows one from the new node, and in the final table the latest
        deposed-side commit precedes the earliest new-side commit

Preconditions, so a green cannot come from looking away: every partition rule
dropped Raft packets, leadership moved off the partitioned node, and the
writer was acked by both the old primary and the new one, through a gateway
on a node that was never the old leader.
"""

from __future__ import annotations

import contextlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Final

import httpx
import psycopg
import typer
from rich.console import Console
from rich.table import Table

import api_models
import fault_primitives as fp
import settle
from topology import Node, load

app = typer.Typer(add_completion=False)
console = Console()

TABLE: Final[str] = "rw6_writes"
# Long enough for an election past the partitioned leader and its lease, and
# for the new primary to open; the window ends as soon as both are seen.
PARTITION_BUDGET_S: Final[float] = 90.0
# Acks the writer must collect from the new primary before the window closes,
# so the change of route is observed, not inferred.
ACKS_ON_NEW_PRIMARY: Final[int] = 10
WRITE_INTERVAL_S: Final[float] = 0.02
TAIL_S: Final[float] = 3.0


class RoutingError(RuntimeError):
    """The run could not be carried out as specified."""


@dataclass(frozen=True)
class Ack:
    """One acknowledged write, in the order the writer saw it."""

    key: str
    server: str


@dataclass(frozen=True)
class Row:
    """One committed write, as the final leader holds it."""

    key: str
    server: str
    at_us: int


@dataclass
class Verdict:
    problems: list[str] = field(default_factory=list)

    def extend(self, found: list[str]) -> None:
        self.problems.extend(found)


def routing_violations(acks: list[Ack], deposed: str, new: str) -> list[str]:
    """Acks from the deposed node that follow one from the new node.

    The writer is sequential, so its ack order is the order the gateway
    routed in: once the new primary has answered, a deposed-side ack means
    the gateway went back to the node that lost leadership.
    """
    seen_new = False
    violations = []
    for ack in acks:
        if ack.server == new:
            seen_new = True
        elif ack.server == deposed and seen_new:
            violations.append(f"{ack.key} was acked by deposed {deposed} after {new} took writes")
    return violations


def overlap_violations(rows: list[Row], deposed: str, new: str) -> list[str]:
    """The deposed node committing after the new one began committing."""
    on_deposed = [r.at_us for r in rows if r.server == deposed]
    on_new = [r.at_us for r in rows if r.server == new]
    if not on_deposed or not on_new:
        return []
    last_deposed, first_new = max(on_deposed), min(on_new)
    if last_deposed > first_new:
        return [
            f"deposed {deposed} committed {last_deposed - first_new} us after "
            f"{new} committed its first write"
        ]
    return []


def missing_acked(acks: list[Ack], rows: list[Row]) -> list[str]:
    """Acked keys absent from the final leader (W1)."""
    present = {r.key for r in rows}
    return sorted(a.key for a in acks if a.key not in present)


def coverage_problems(acks: list[Ack], deposed: str, new: str) -> list[str]:
    """What makes this run unable to say anything about RW-6."""
    servers = {a.server for a in acks}
    problems = []
    if deposed not in servers:
        problems.append(f"no write was acked by the old primary {deposed}")
    if new not in servers:
        problems.append(f"no write was acked by the new primary {new}")
    return problems


class GatewayWriter(threading.Thread):
    """Writes through one gateway as fast as it answers, recording each ack."""

    def __init__(self, gateway_port: int, prefix: str) -> None:
        super().__init__(daemon=True)
        self.gateway_port = gateway_port
        self.prefix = prefix
        self.acks: list[Ack] = []
        self.errors = 0
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def snapshot(self) -> list[Ack]:
        with self._lock:
            return list(self.acks)

    def run(self) -> None:
        conn: psycopg.Connection[tuple[object, ...]] | None = None
        n = 0
        while not self._stop_event.is_set():
            try:
                if conn is None:
                    conn = psycopg.connect(
                        host="127.0.0.1",
                        port=self.gateway_port,
                        user="postgres",
                        dbname="postgres",
                        connect_timeout=2,
                        autocommit=True,
                    )
                key = f"{self.prefix}-{n}"
                row = conn.execute(
                    f"INSERT INTO {TABLE} (k, server, at) "
                    "VALUES (%s, inet_server_addr(), clock_timestamp()) "
                    "RETURNING host(server)",
                    (key,),
                ).fetchone()
                n += 1
                if row is not None:
                    with self._lock:
                        self.acks.append(Ack(key=key, server=str(row[0])))
            except psycopg.Error:
                self.errors += 1
                if conn is not None:
                    with contextlib.suppress(psycopg.Error):
                        conn.close()
                conn = None
                n += 1
                time.sleep(0.05)
                continue
            time.sleep(WRITE_INTERVAL_S)
        if conn is not None:
            with contextlib.suppress(psycopg.Error):
                conn.close()

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=10)


def leader_seen_by(node: Node) -> int | None:
    try:
        r = httpx.get(f"http://127.0.0.1:{node.mgmt_port}/api/v1/cluster/leader", timeout=2.0)
    except httpx.HTTPError:
        return None
    if r.status_code != 200:
        return None
    info = api_models.parse_or_none(api_models.LeaderInfo, r.text)
    return None if info is None else info.leader_id


def acks_from(writer: GatewayWriter, server: str) -> int:
    return sum(1 for a in writer.snapshot() if a.server == server)


def await_writer(writer: GatewayWriter, server: str, count: int, budget: float, what: str) -> None:
    deadline = time.monotonic() + budget
    while acks_from(writer, server) < count:
        if time.monotonic() >= deadline:
            raise RoutingError(
                f"{what}: {acks_from(writer, server)} of {count} acks from {server} "
                f"in {budget:g}s ({writer.errors} write errors)"
            )
        time.sleep(0.2)


def read_rows(node: Node, prefix: str) -> list[Row]:
    with psycopg.connect(
        host="127.0.0.1",
        port=node.gateway_port,
        user="postgres",
        dbname="postgres",
        connect_timeout=5,
        autocommit=True,
    ) as conn:
        fetched = conn.execute(
            f"SELECT k, host(server), (extract(epoch FROM at) * 1000000)::bigint "
            f"FROM {TABLE} WHERE k LIKE %s",
            (f"{prefix}-%",),
        ).fetchall()
    return [Row(key=str(k), server=str(s), at_us=int(str(t))) for k, s, t in fetched]


def create_table(node: Node) -> None:
    with psycopg.connect(
        host="127.0.0.1",
        port=node.gateway_port,
        user="postgres",
        dbname="postgres",
        connect_timeout=5,
        autocommit=True,
    ) as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {TABLE} ("
            "k text PRIMARY KEY, server inet NOT NULL, at timestamptz NOT NULL)"
        )


@app.command()
def main() -> None:
    topo = load()
    nodes = [n for n in topo.nodes if n.is_voter]
    leader = settle.await_settled(nodes)
    peers = [n for n in nodes if n.node_id != leader.node_id]
    console.print(
        f"leader {leader.service} ({leader.ip}); writing through "
        f"{', '.join(p.service for p in peers)}"
    )

    create_table(leader)
    run = uuid.uuid4().hex[:8]
    # One writer per follower gateway: whichever follower wins, another one
    # keeps routing to the deposed primary until it learns of the change.
    writers = {p.service: GatewayWriter(p.gateway_port, f"rw6-{run}-{p.service}") for p in peers}
    for writer in writers.values():
        writer.start()
    new_leader: Node | None = None
    try:
        for service, writer in writers.items():
            await_writer(writer, leader.ip, 20, 30.0, f"through {service} before the partition")
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                fp.partition_channel(leader.service, [p.service for p in peers], fp.Channel.RAFT)
            )
            for p in peers:
                stack.enter_context(
                    fp.partition_channel(p.service, [leader.service], fp.Channel.RAFT)
                )
            console.print(f"Raft cut both ways around {leader.service}; its PostgreSQL stays up")
            deadline = time.monotonic() + PARTITION_BUDGET_S
            while new_leader is None:
                if time.monotonic() >= deadline:
                    raise RoutingError(
                        f"no leader other than {leader.service} within {PARTITION_BUDGET_S:g}s"
                    )
                views = {leader_seen_by(p) for p in peers}
                new_leader = next(
                    (n for n in peers if views == {n.node_id}),
                    None,
                )
                time.sleep(0.2)
            console.print(f"leadership moved to {new_leader.service} ({new_leader.ip})")
            for service, writer in writers.items():
                await_writer(
                    writer,
                    new_leader.ip,
                    ACKS_ON_NEW_PRIMARY,
                    PARTITION_BUDGET_S,
                    f"through {service} after the change",
                )
        final = settle.await_settled(nodes)
        time.sleep(TAIL_S)
    finally:
        for writer in writers.values():
            writer.stop()
    if new_leader is None:
        raise RoutingError("the partition closed without a new leader")

    rows = read_rows(final, f"rw6-{run}")
    verdict = Verdict()
    verdict.extend(overlap_violations(rows, leader.ip, new_leader.ip))
    table = Table(title="RW-6: follower gateways across a deposed primary")
    for column in ("gateway", "acks", "by deposed", "by new", "errors"):
        table.add_column(column)
    for service, writer in writers.items():
        acks = writer.snapshot()
        verdict.extend(
            [f"{service}: {p}" for p in coverage_problems(acks, leader.ip, new_leader.ip)]
        )
        verdict.extend(
            [f"W1: {service} acked {k}, not on the final leader" for k in missing_acked(acks, rows)]
        )
        verdict.extend(
            [f"{service}: {v}" for v in routing_violations(acks, leader.ip, new_leader.ip)]
        )
        table.add_row(
            service,
            str(len(acks)),
            str(acks_from(writer, leader.ip)),
            str(acks_from(writer, new_leader.ip)),
            str(writer.errors),
        )
    console.print(table)
    console.print(f"rows on the final leader: {len(rows)}")
    if verdict.problems:
        for problem in verdict.problems:
            console.print(f"[red]FAIL[/] {problem}")
        raise typer.Exit(code=1)
    console.print("[green]RW-6 OK[/]: no follower gateway wrote to the deposed primary")


if __name__ == "__main__":
    app()
