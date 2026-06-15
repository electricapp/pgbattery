#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "psycopg[binary]>=3.2",
#   "rich>=13",
#   "httpx>=0.27",
# ]
# ///
"""Reproducible pgbattery benchmark.

Runs against the local 3-node demo cluster (docker-compose). Produces:
  - Baseline pgbench TPS (no fault)
  - Heartbeat gap during SIGKILL of the leader (client-observed unavailability)
  - Resource snapshot (docker stats during steady-state)
  - JSON results at demo/bench-results.json
  - Markdown table on stdout for paste into README

Assumes:
  - 3 nodes up: pgbattery-node{1,2,3}-1, healthy
  - Gateway: 127.0.0.1:5432; mgmt API: 127.0.0.1:9081
  - PGBATTERY_MANAGEMENT_API_TOKEN exported (only needed if mutating; we don't)
"""

from __future__ import annotations

import json
import re
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import psycopg
from rich.console import Console
from rich.table import Table

GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 5432
MGMT_BASE = "http://127.0.0.1:9081"
DSN = f"postgresql://postgres@{GATEWAY_HOST}:{GATEWAY_PORT}/postgres"

PGBENCH_CLIENTS = 4
BASELINE_DURATION_S = 30
HEARTBEAT_DURATION_S = 30
HEARTBEAT_INTERVAL_S = 0.05
KILL_AT_S = 10

console = Console()


@dataclass
class BenchResults:
    baseline_tps: float = 0.0
    baseline_latency_ms: float = 0.0
    heartbeat_max_gap_ms: float = 0.0
    heartbeat_p99_gap_ms: float = 0.0
    heartbeat_recovery_ms: float = 0.0
    leader_before: int = 0
    leader_after: int = 0
    cluster_recovery_s: float = 0.0
    docker_stats: list[dict[str, str]] = field(default_factory=list)


def get_leader() -> dict[str, object]:
    r = httpx.get(f"{MGMT_BASE}/api/v1/cluster/leader", timeout=5)
    r.raise_for_status()
    return r.json()


def container_for_leader(leader_id: int) -> str:
    return f"pgbattery-node{leader_id}-1"


def run_pgbench_init() -> None:
    console.print("[dim]Initializing pgbench schema (scale=1)...[/]")
    subprocess.run(
        [
            "pgbench",
            "-i",
            "-q",
            "-s",
            "1",
            "-h",
            GATEWAY_HOST,
            "-p",
            str(GATEWAY_PORT),
            "-U",
            "postgres",
            "postgres",
        ],
        check=True,
        capture_output=True,
    )


def run_pgbench_baseline(duration_s: int) -> tuple[float, float]:
    console.print(f"[bold]Baseline pgbench[/]: {PGBENCH_CLIENTS} clients × {duration_s}s")
    result = subprocess.run(
        [
            "pgbench",
            "-c",
            str(PGBENCH_CLIENTS),
            "-j",
            str(PGBENCH_CLIENTS),
            "-T",
            str(duration_s),
            "-r",
            "-h",
            GATEWAY_HOST,
            "-p",
            str(GATEWAY_PORT),
            "-U",
            "postgres",
            "postgres",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    tps_match = re.search(r"tps = ([\d.]+) ", result.stdout)
    lat_match = re.search(r"latency average = ([\d.]+) ms", result.stdout)
    if not tps_match or not lat_match:
        console.print("[red]could not parse pgbench output:[/]")
        console.print(result.stdout)
        sys.exit(1)
    return float(tps_match.group(1)), float(lat_match.group(1))


def heartbeat_worker(stop_event: threading.Event, gaps_ms: list[float], errors: list[float]) -> None:
    """Open one persistent connection and SELECT every 50 ms; record per-tick gap."""
    prev = time.monotonic()
    while not stop_event.is_set():
        try:
            with psycopg.connect(DSN, connect_timeout=2, autocommit=True) as conn:
                with conn.cursor() as cur:
                    while not stop_event.is_set():
                        now = time.monotonic()
                        gap_ms = (now - prev) * 1000.0
                        gaps_ms.append(gap_ms)
                        prev = now
                        try:
                            cur.execute("SELECT 1")
                            cur.fetchone()
                        except Exception:
                            errors.append(now)
                            break
                        time.sleep(HEARTBEAT_INTERVAL_S)
        except Exception:
            errors.append(time.monotonic())
            time.sleep(0.1)


def measure_failover() -> tuple[float, float, float, float, float]:
    """Run heartbeat loop, kill the leader at KILL_AT_S, measure observed gap.

    Returns:
      (max_gap_ms, p99_gap_ms, recovery_ms, cluster_recovery_s, leader_after)
    """
    leader = get_leader()
    leader_id_before = int(leader["leader_id"])  # type: ignore[arg-type]
    container = container_for_leader(leader_id_before)

    console.print(f"[bold]Failover heartbeat[/]: kill {container} at T={KILL_AT_S}s")

    stop = threading.Event()
    gaps: list[float] = []
    errors: list[float] = []
    t = threading.Thread(target=heartbeat_worker, args=(stop, gaps, errors), daemon=True)
    t.start()

    t0 = time.monotonic()
    # Quiesce for KILL_AT_S, then kill the leader.
    time.sleep(KILL_AT_S)
    kill_t = time.monotonic()
    subprocess.run(["docker", "kill", "-s", "SIGKILL", container], check=True, capture_output=True)
    console.print(f"  [yellow]→ SIGKILL sent to {container}[/]")

    # Watch heartbeat until first long gap (the failover window).
    deadline = t0 + HEARTBEAT_DURATION_S
    recovery_t: float | None = None
    while time.monotonic() < deadline:
        time.sleep(0.5)
        # consider recovered once we've had ≥ 2s of clean ticks past the kill
        recent_gaps = [g for g in gaps[-40:] if g > 200]
        if not recent_gaps and time.monotonic() - kill_t > 2.0:
            recovery_t = time.monotonic()
            break

    stop.set()
    t.join(timeout=2)

    # Bring node back so subsequent runs work.
    subprocess.run(["docker", "start", container], check=True, capture_output=True)
    console.print(f"  [green]→ restarted {container}[/]")

    if not gaps:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    sorted_gaps = sorted(gaps)
    p99 = sorted_gaps[int(len(sorted_gaps) * 0.99)]
    max_gap = max(gaps)

    # Wait for cluster to fully reconverge to a 3-node leader-quorum, then read new leader.
    converge_start = time.monotonic()
    new_leader_id = leader_id_before
    while time.monotonic() - converge_start < 60:
        try:
            new_leader = get_leader()
            new_leader_id = int(new_leader["leader_id"])  # type: ignore[arg-type]
            break
        except Exception:
            time.sleep(0.5)
    cluster_recovery_s = time.monotonic() - kill_t

    recovery_ms = (recovery_t - kill_t) * 1000.0 if recovery_t else max_gap

    return max_gap, p99, recovery_ms, cluster_recovery_s, float(new_leader_id)


def docker_stats_snapshot() -> list[dict[str, str]]:
    """One-shot `docker stats` for the three demo nodes."""
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    rows: list[dict[str, str]] = []
    for line in out.strip().splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        name, cpu, mem = parts
        if name.startswith("pgbattery-node"):
            rows.append({"container": name, "cpu": cpu, "mem": mem})
    return rows


def wait_cluster_ready(timeout_s: int = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            get_leader()
            with psycopg.connect(DSN, connect_timeout=2) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("cluster did not become ready in time")


def main() -> int:
    wait_cluster_ready()
    results = BenchResults()
    leader = get_leader()
    results.leader_before = int(leader["leader_id"])  # type: ignore[arg-type]

    run_pgbench_init()
    results.baseline_tps, results.baseline_latency_ms = run_pgbench_baseline(BASELINE_DURATION_S)

    results.docker_stats = docker_stats_snapshot()

    (
        results.heartbeat_max_gap_ms,
        results.heartbeat_p99_gap_ms,
        results.heartbeat_recovery_ms,
        results.cluster_recovery_s,
        new_leader,
    ) = measure_failover()
    results.leader_after = int(new_leader)

    wait_cluster_ready()

    out_path = Path(__file__).parent / "bench-results.json"
    out_path.write_text(json.dumps(results.__dict__, indent=2))

    t = Table(title="pgbattery benchmark results", show_lines=False)
    t.add_column("Metric", style="bold")
    t.add_column("Value", justify="right")
    t.add_row("Baseline TPS (4 clients, 30s)", f"{results.baseline_tps:,.0f}")
    t.add_row("Baseline latency (avg)", f"{results.baseline_latency_ms:.2f} ms")
    t.add_row("Failover recovery (client-observed)", f"{results.heartbeat_recovery_ms:.0f} ms")
    t.add_row("Heartbeat max gap during kill", f"{results.heartbeat_max_gap_ms:.0f} ms")
    t.add_row("Heartbeat p99 gap", f"{results.heartbeat_p99_gap_ms:.0f} ms")
    t.add_row("Cluster reconvergence", f"{results.cluster_recovery_s:.1f} s")
    t.add_row("Leader before → after", f"node{results.leader_before} → node{results.leader_after}")
    console.print(t)

    console.print("\n[bold]Resource snapshot (steady-state):[/]")
    rt = Table(show_lines=False)
    rt.add_column("Container")
    rt.add_column("CPU", justify="right")
    rt.add_column("Memory", justify="right")
    for row in results.docker_stats:
        rt.add_row(row["container"], row["cpu"], row["mem"])
    console.print(rt)

    console.print(f"\n[dim]→ wrote {out_path}[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
