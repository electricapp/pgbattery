#!/usr/bin/env -S uv run --project testing python
"""Hold a real failover's trace to `lease_fencing` and `raft_lsn`.

Every node records the transitions the specs model (`SpecEvent`). This case
reads them from each node's `/debug/events` for the whole run, cuts the
leader's Raft port both ways so the cluster elects past a deposed leader that
is still running, and checks the merged trace with `trace_check`:

  - no rule is violated
  - every rule decided at least one instance, so none passed by looking away
  - every drift in `trace_check.DRIFTS`, written into a copy of this trace,
    trips the rule it targets

A rule that would need an evicted event reports it unverifiable, never
violated. The raw events land in `testing/artifacts/trace-validation/`.
"""

from __future__ import annotations

import contextlib
import json
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Final

import httpx
import typer
from rich.console import Console
from rich.table import Table

import api_models
import fault_primitives as fp
import settle
import trace_check as tc
from topology import Node, load

app = typer.Typer(add_completion=False)
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
ARTIFACT_DIR: Final[Path] = PROJECT_ROOT / "testing" / "artifacts" / "trace-validation"
POLL_INTERVAL_S: Final[float] = 0.3
# The ring holds 1_000 events; one read returns them all.
EVENTS_LIMIT: Final[int] = 10_000
# An election past the partitioned leader, its hold-down, and the open.
PARTITION_BUDGET_S: Final[float] = 90.0
TAIL_S: Final[float] = 3.0


class TraceValidationError(RuntimeError):
    """The run could not be carried out as specified."""


class EventReader(threading.Thread):
    """Reads every node's debug events until stopped, keeping all of them."""

    def __init__(self, nodes: list[Node], token: str) -> None:
        super().__init__(daemon=True)
        self.nodes = nodes
        self.headers = {"x-pgbattery-token": token}
        self.events: defaultdict[int, dict[int, api_models.DebugEvent]] = defaultdict(dict)
        self.current: dict[int, int] = {}
        self.failure: str | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def read_once(self) -> None:
        for node in self.nodes:
            since = self.current.get(node.node_id, 0)
            try:
                r = httpx.get(
                    f"http://127.0.0.1:{node.mgmt_port}/debug/events",
                    params={"since": since, "limit": EVENTS_LIMIT},
                    headers=self.headers,
                    timeout=2.0,
                )
            except httpx.HTTPError:
                continue
            if r.status_code in (401, 403):
                with self._lock:
                    self.failure = f"{node.service} refused the management token"
                return
            if r.status_code != 200:
                continue
            page = api_models.parse(api_models.DebugEvents, r.text)
            with self._lock:
                if page.current_seq < since:
                    self.failure = f"{node.service} restarted: seq {page.current_seq} after {since}"
                    return
                for event in page.events:
                    self.events[node.node_id][event.seq] = event
                self.current[node.node_id] = page.current_seq

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.read_once()
            time.sleep(POLL_INTERVAL_S)

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=10)
        self.read_once()

    def seq(self, node: Node) -> int:
        with self._lock:
            if self.failure:
                raise TraceValidationError(self.failure)
            return self.current.get(node.node_id, 0)

    def opened_after(self, node: Node, seq: int) -> bool:
        """Whether `node` recorded a write-gate open after `seq`."""
        with self._lock:
            if self.failure:
                raise TraceValidationError(self.failure)
            events = list(self.events[node.node_id].values())
        return any(
            e.seq > seq and e.event_type == tc.SPEC_EVENT_TYPE and e.data.get("kind") == "open"
            for e in events
        )

    def trace(self) -> tc.Trace:
        with self._lock:
            if self.failure:
                raise TraceValidationError(self.failure)
            missing = sorted(n.service for n in self.nodes if n.node_id not in self.current)
            if missing:
                raise TraceValidationError(f"never read the events of {missing}")
            return tc.Trace(
                {
                    node: tc.node_trace(node, events.values(), self.current[node])
                    for node, events in self.events.items()
                }
            )

    def dump(self, path: Path) -> None:
        with self._lock:
            body = {
                str(node): {
                    "current_seq": self.current.get(node, 0),
                    "events": [e.model_dump() for _, e in sorted(events.items())],
                }
                for node, events in self.events.items()
            }
        path.write_text(json.dumps(body, indent=1), encoding="utf-8")


def leader_seen_by(node: Node) -> int | None:
    try:
        r = httpx.get(f"http://127.0.0.1:{node.mgmt_port}/api/v1/cluster/leader", timeout=2.0)
    except httpx.HTTPError:
        return None
    if r.status_code != 200:
        return None
    info = api_models.parse_or_none(api_models.LeaderInfo, r.text)
    return None if info is None else info.leader_id


def failover(reader: EventReader, nodes: list[Node]) -> Node:
    """Cut the leader's Raft port both ways until another node leads and opens."""
    leader = settle.await_settled(nodes)
    peers = [n for n in nodes if n.node_id != leader.node_id]
    if unread := [n.service for n in nodes if reader.seq(n) == 0]:
        raise TraceValidationError(f"no events read from {unread} before the fault")
    marks = {p.node_id: reader.seq(p) for p in peers}
    console.print(f"leader {leader.service}; cutting its Raft port both ways")
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            fp.partition_channel(leader.service, [p.service for p in peers], fp.Channel.RAFT)
        )
        for p in peers:
            stack.enter_context(fp.partition_channel(p.service, [leader.service], fp.Channel.RAFT))
        deadline = time.monotonic() + PARTITION_BUDGET_S
        new_leader: Node | None = None
        while new_leader is None:
            if time.monotonic() >= deadline:
                raise TraceValidationError(
                    f"no leader other than {leader.service} within {PARTITION_BUDGET_S:g}s"
                )
            views = {leader_seen_by(p) for p in peers}
            new_leader = next((n for n in peers if views == {n.node_id}), None)
            time.sleep(0.2)
        mark = marks[new_leader.node_id]
        console.print(f"{new_leader.service} leads; waiting for its write gate to open")
        while not reader.opened_after(new_leader, mark):
            if time.monotonic() >= deadline:
                raise TraceValidationError(
                    f"{new_leader.service} recorded no open after seq {mark}"
                )
            time.sleep(0.2)
    settle.await_settled(nodes)
    time.sleep(TAIL_S)
    return new_leader


def report_table(report: tc.Report) -> Table:
    table = Table(title="Trace against lease_fencing and raft_lsn")
    for column in ("rule", "cites", "checked", "violated", "unverifiable"):
        table.add_column(column)
    for rule in tc.RULES:
        table.add_row(
            rule.name,
            ", ".join(rule.refs),
            str(report.checked[rule.name]),
            str(len(report.violated(rule))),
            str(sum(1 for f in report.unverifiable if f.rule == rule.name)),
        )
    return table


def drift_problems(trace: tc.Trace, bounds: tc.Bounds) -> list[str]:
    """Drifts this run's trace could not carry, or that the checker missed."""
    problems = []
    for drift in tc.DRIFTS:
        try:
            drifted = drift.apply(trace, bounds)
        except tc.DriftNotApplicable as exc:
            problems.append(f"drift '{drift.name}' does not apply to this run: {exc}")
            continue
        if not tc.check(drifted, bounds).violated(drift.trips):
            problems.append(f"drift '{drift.name}' did not trip {drift.trips.name}")
    return problems


@app.command()
def main(
    token: str = typer.Option(
        ...,
        envvar="PGBATTERY_MANAGEMENT_API_TOKEN",
        help="Management API token; /debug/events is gated.",
    ),
) -> None:
    topo = load()
    nodes = [n for n in topo.nodes if n.is_voter]
    timings = fp.read_system_timings()
    bounds = tc.Bounds(hold_down_ms=timings.promotion_holddown_ms, voters=len(nodes))
    reader = EventReader(nodes, token)
    reader.read_once()
    reader.start()
    try:
        new_leader = failover(reader, nodes)
    finally:
        reader.stop()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    reader.dump(ARTIFACT_DIR / "events.json")

    trace = reader.trace()
    report = tc.check(trace, bounds)
    console.print(report_table(report))
    console.print(f"new leader {new_leader.service}; events in {ARTIFACT_DIR / 'events.json'}")
    problems = [f"{f.rule} on node {f.node}: {f.detail}" for f in report.violations]
    problems += [
        f"{r.name} decided no instance: the run did not exercise it"
        for r in tc.RULES
        if report.checked[r.name] == 0
    ]
    problems += drift_problems(trace, bounds)
    for f in report.unverifiable:
        console.print(f"[yellow]unverifiable[/] {f.rule} on node {f.node}: {f.detail}")
    if problems:
        for problem in problems:
            console.print(f"[red]FAIL[/] {problem}")
        raise typer.Exit(code=1)
    console.print(
        f"[green]TRACE OK[/]: {sum(report.checked.values())} instances checked, "
        f"{len(tc.DRIFTS)} drifts caught"
    )


if __name__ == "__main__":
    app()
