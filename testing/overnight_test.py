#!/usr/bin/env -S uv run --project testing python
"""Overnight chaos testing for pgbattery PostgreSQL HA cluster.

This module runs various chaos scenarios against a PostgreSQL HA cluster
to verify resilience, failover behavior, and data integrity under failure conditions.
"""

from __future__ import annotations

import random
import subprocess
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

import typer
from pydantic import BaseModel
from rich.console import Console
from rich.live import Live
from rich.table import Table

console = Console()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class TestResult(BaseModel):
    name: str
    success: bool
    duration: float
    timestamp: str
    recovery_wait: float | None = None
    status_output: str | None = None
    error: str | None = None


class TestRunSummary(BaseModel):
    duration_hours: float
    nodes: int
    seed: int | None
    start_time: str
    end_time: str


class TestReportStats(BaseModel):
    total: int
    successes: int
    failures: int
    success_rate: float


class TestReport(BaseModel):
    test_run: TestRunSummary
    summary: TestReportStats
    results: list[TestResult]


# ---------------------------------------------------------------------------
# Chaos test runner
# ---------------------------------------------------------------------------


class ChaosTest:
    """Chaos testing framework for pgbattery cluster.

    Runs randomized failure scenarios against a PostgreSQL HA cluster and
    verifies cluster health and recovery after each test.
    """

    def __init__(
        self,
        duration_hours: float = 8,
        nodes: int = 3,
        verbose: bool = False,
        seed: int | None = None,
    ) -> None:
        self.seed = seed if seed is not None else random.randrange(2**32)
        random.seed(self.seed)
        self.end_time = datetime.now() + timedelta(hours=duration_hours)
        self.start_time = datetime.now()
        self.duration_hours = duration_hours
        self.nodes = nodes
        self.verbose = verbose
        self.results: list[TestResult] = []
        self.written_values: list[str] = []
        self.project_root = Path(__file__).parent.parent
        # Monotonic write-sequence tracking: every chaos-time insert gets the
        # next seq#. At the end of the run we query the DB and assert no
        # acked seq# is missing (durability) and no unacked seq# appears
        # (ghost write). Lets us detect lost-during-chaos writes that the
        # original "did the cluster come back?" check would miss.
        self._next_seq = 0
        self.acked_seqs: set[int] = set()
        self.indeterminate_seqs: set[int] = set()
        self.failure_artifacts_dir = (
            self.project_root
            / "testing"
            / "artifacts"
            / f"overnight-{self.start_time.strftime('%Y%m%d-%H%M%S')}"
        )
        # Tell the operator what's happening as soon as possible — seed is the
        # only knob that makes a stochastic chaos run reproducible.
        console.print(f"[dim]Seed: [cyan]{self.seed}[/]  (replay with --seed {self.seed})[/]")

    def run_command(self, cmd: str, cwd: Path | None = None) -> tuple[str, str, int]:
        """Execute shell command and return output."""
        if cwd is None:
            cwd = self.project_root

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
        return result.stdout, result.stderr, result.returncode

    def get_cluster_status(self) -> tuple[bool, str]:
        """Check if cluster is healthy."""
        nodes_arg = ",".join([f"localhost:909{i}" for i in range(1, self.nodes + 1)])
        stdout, stderr, rc = self.run_command(
            f"./target/release/pgbattery status --nodes {nodes_arg}"
        )

        if rc != 0:
            return False, f"Command failed: {stderr}"

        healthy = "HEALTHY" in stdout
        leader_count = stdout.count("LEADER")

        return healthy and leader_count == 1, stdout

    def wait_for_healthy(self, timeout: int = 60) -> tuple[bool, str, float]:
        """Poll cluster status until healthy or timeout."""
        start = time.time()
        last_status = "unknown"
        while time.time() - start < timeout:
            healthy, status = self.get_cluster_status()
            last_status = status
            if healthy:
                return True, status, time.time() - start
            time.sleep(3)
        return False, last_status, time.time() - start

    def get_leader_node(self) -> int | None:
        """Find which node is currently the leader."""
        for i in range(1, self.nodes + 1):
            stdout, _, rc = self.run_command(
                f"./target/release/pgbattery status --nodes localhost:909{i}"
            )
            if rc == 0 and "LEADER" in stdout:
                return i
        return None

    @staticmethod
    def _extract_iter_number(name: str) -> int:
        """Pull the iteration index out of a "Iter N: …" scenario name."""
        if name.startswith("Iter "):
            tail = name[5:]
            num_str = tail.split(":", 1)[0].strip()
            if num_str.isdigit():
                return int(num_str)
        return 0

    def test_scenario(self, name: str, action: Callable[[], None]) -> None:
        """Run a test scenario and record results."""
        console.print(f"[bold blue]Running:[/] {name}")
        start = time.time()

        try:
            action()

            healthy, status, wait_elapsed = self.wait_for_healthy(timeout=60)
            duration = time.time() - start

            self.results.append(
                TestResult(
                    name=name,
                    success=healthy,
                    duration=duration,
                    recovery_wait=wait_elapsed,
                    timestamp=datetime.now().isoformat(),
                    status_output=status[:200] if status else None,
                )
            )

            if healthy:
                console.print(
                    f"  [bold green]PASS[/] {name} "
                    f"({duration:.1f}s, healthy after {wait_elapsed:.1f}s)"
                )
            else:
                console.print(
                    f"  [bold red]FAIL[/] {name} ({duration:.1f}s) "
                    f"- Cluster unhealthy after {wait_elapsed:.1f}s"
                )
                if self.verbose:
                    console.print(f"  [dim]Status: {status[:200]}[/]")
                # Iteration index extracted from "Iter N: …" prefix if present.
                iter_num = self._extract_iter_number(name)
                self._dump_failure_artifacts(iter_num, name)

        except Exception as e:
            duration = time.time() - start
            console.print(f"  [bold red]ERROR[/] {name}: {e}")
            console.print_exception()
            iter_num = self._extract_iter_number(name)
            self._dump_failure_artifacts(iter_num, name)
            self.results.append(
                TestResult(
                    name=name,
                    success=False,
                    duration=duration,
                    error=str(e),
                    timestamp=datetime.now().isoformat(),
                )
            )

    def kill_random_node(self) -> None:
        """Kill a random node and restart it."""
        node = random.randint(1, self.nodes)
        if self.verbose:
            console.print(f"  [dim]Killing random node {node}[/]")
        self.run_command(f"docker kill pgbattery-node{node}-1")
        time.sleep(2)
        self.run_command(f"docker start pgbattery-node{node}-1")

    def kill_leader(self) -> None:
        """Kill the current leader node and restart it."""
        leader = self.get_leader_node()
        if leader:
            if self.verbose:
                console.print(f"  [dim]Killing leader (node {leader})[/]")
            self.run_command(f"docker kill pgbattery-node{leader}-1")
            time.sleep(3)
            self.run_command(f"docker start pgbattery-node{leader}-1")
        else:
            console.print("  [yellow]No leader found, skipping[/]")

    def restart_all_nodes(self) -> None:
        """Restart all nodes to simulate cluster-wide restart."""
        if self.verbose:
            console.print(f"  [dim]Restarting all {self.nodes} nodes[/]")
        for i in range(1, self.nodes + 1):
            self.run_command(f"docker restart pgbattery-node{i}-1")
        time.sleep(5)

    def network_partition(self) -> None:
        """Simulate network partition by pausing a random node."""
        node = random.randint(1, self.nodes)
        if self.verbose:
            console.print(f"  [dim]Pausing node {node} for 15 seconds[/]")
        self.run_command(f"docker pause pgbattery-node{node}-1")
        time.sleep(15)
        self.run_command(f"docker unpause pgbattery-node{node}-1")

    def _ensure_seq_table(self, leader: int) -> None:
        """Create chaos_seq if absent. Cheap — DDL is idempotent."""
        self.run_command(
            f"docker exec pgbattery-node{leader}-1 psql -U postgres -c "
            f'"CREATE TABLE IF NOT EXISTS chaos_seq '
            f'(seq BIGINT PRIMARY KEY, ts TIMESTAMP DEFAULT NOW());"'
        )

    def _next_seq_value(self) -> int:
        self._next_seq += 1
        return self._next_seq

    def write_next_seq(self, leader: int) -> tuple[int, str]:
        """Insert the next seq# via the leader. Classifies outcome strictly.

        Returns (seq, outcome) where outcome is "acked" | "indeterminate" |
        "rejected". Acks land in `self.acked_seqs`; indeterminate (timeout,
        conn closed, broken pipe) lands in `self.indeterminate_seqs`. A
        rejection is NOT tracked — those are real "didn't happen" outcomes.
        """
        seq = self._next_seq_value()
        cmd = (
            f"docker exec pgbattery-node{leader}-1 psql -U postgres "
            f"-v ON_ERROR_STOP=1 "
            f'-c "INSERT INTO chaos_seq(seq) VALUES ({seq});" 2>&1'
        )
        stdout, stderr, rc = self.run_command(cmd)
        combined = (stdout + stderr).lower()
        if rc == 0:
            self.acked_seqs.add(seq)
            return seq, "acked"
        # Indeterminate: the write may or may not have landed. Track as
        # "could be in DB, could not be" — at verification time both
        # acked AND indeterminate-but-present count as legal.
        indeterminate_signals = (
            "connection",
            "server closed",
            "timeout",
            "reset by peer",
            "broken pipe",
            "unexpected eof",
        )
        if any(s in combined for s in indeterminate_signals):
            self.indeterminate_seqs.add(seq)
            return seq, "indeterminate"
        return seq, "rejected"

    def write_and_verify_data(self) -> None:
        """Write a small seq# batch via the leader, kill it, verify on a follower.

        Records every acked write into `self.acked_seqs` so the end-of-run
        durability check can verify it survived the cumulative chaos —
        not just this single scenario. Lost writes that surface only
        hours later (e.g. a slow corruption that the per-scenario healthy
        check misses) are caught by `_verify_seq_durability` at exit.
        """
        leader = self.get_leader_node()
        if leader is None:
            raise Exception("No leader found, cannot write data")

        followers = [i for i in range(1, self.nodes + 1) if i != leader]
        read_node = random.choice(followers) if followers else leader

        self._ensure_seq_table(leader)

        # Burst a small batch so failover catches us mid-flight more often.
        batch_size = random.randint(3, 8)
        batch: list[tuple[int, str]] = []
        for _ in range(batch_size):
            batch.append(self.write_next_seq(leader))

        rejected_or_empty = [s for s, o in batch if o == "rejected"]
        if len(rejected_or_empty) == batch_size:
            raise Exception(f"All {batch_size} writes rejected before failover")

        acked_in_batch = [s for s, o in batch if o == "acked"]
        if self.verbose:
            console.print(
                f"  [dim]Wrote seq batch via node{leader}: "
                f"{len(acked_in_batch)} acked, batch={[s for s, _ in batch]}[/]"
            )

        time.sleep(1)
        self.kill_leader()
        time.sleep(3)

        # Verify the acked writes are visible from a follower post-failover.
        if not acked_in_batch:
            return  # No acks → nothing to verify here; full check at end-of-run.
        seq_list = ",".join(str(s) for s in acked_in_batch)
        stdout, _, rc = self.run_command(
            f"docker exec pgbattery-node{read_node}-1 psql -U postgres -t -A -c "
            f'"SELECT seq FROM chaos_seq WHERE seq IN ({seq_list}) ORDER BY seq;"'
        )
        if rc != 0:
            # Don't raise — read might fail during failover. The end-of-run
            # check is authoritative; this is just an early signal.
            return
        present = {int(line) for line in stdout.strip().splitlines() if line.strip().isdigit()}
        missing = set(acked_in_batch) - present
        if missing:
            raise Exception(
                f"Acked seq# lost immediately after failover: {sorted(missing)} "
                f"(read from node{read_node})"
            )

    def verify_all_data(self) -> tuple[bool, list[str]]:
        """Verify all previously written test values still exist."""
        if not self.written_values:
            return True, []

        leader = self.get_leader_node()
        node = leader if leader else 1

        missing: list[str] = []
        for value in self.written_values:
            stdout, _, rc = self.run_command(
                f"docker exec pgbattery-node{node}-1 psql -U postgres -t -c "
                f"\"SELECT value FROM chaos_test WHERE value = '{value}';\""
            )
            if rc != 0 or value not in stdout:
                missing.append(value)

        return len(missing) == 0, missing

    def verify_seq_durability(self) -> tuple[bool, set[int], set[int]]:
        """End-of-run durability sweep against the chaos_seq table.

        Returns (ok, lost, ghost) where
            lost  = acked_seqs - db_seqs   (durability violation)
            ghost = db_seqs - acked_seqs - indeterminate_seqs
                                          (write appeared with no attempt → split-brain)
        Both sets must be empty for `ok = True`.
        """
        if not self.acked_seqs and not self.indeterminate_seqs:
            return True, set(), set()

        node = self.get_leader_node() or 1
        stdout, _, rc = self.run_command(
            f"docker exec pgbattery-node{node}-1 psql -U postgres -t -A -c "
            f'"SELECT seq FROM chaos_seq ORDER BY seq;"'
        )
        if rc != 0:
            return False, set(self.acked_seqs), set()
        db_seqs = {int(line) for line in stdout.strip().splitlines() if line.strip().isdigit()}
        lost = self.acked_seqs - db_seqs
        ghost = db_seqs - self.acked_seqs - self.indeterminate_seqs
        return (not lost and not ghost), lost, ghost

    def _dump_failure_artifacts(self, iteration: int, scenario: str) -> Path | None:
        """Dump per-node pg_controldata + raft dir listing + tail of logs.

        Called on case failure. The goal is to land enough state in one place
        that a developer reading the artifacts directory can answer: "what
        was each node thinking when this failed?" — without having to
        `docker exec` into already-removed containers.
        """
        try:
            self.failure_artifacts_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            console.print(f"  [yellow]Could not create artifacts dir: {exc}[/]")
            return None
        safe_scenario = scenario.replace(" ", "_").replace("/", "_")
        out_path = self.failure_artifacts_dir / f"iter-{iteration:04d}-{safe_scenario}.txt"
        sections: list[str] = [
            "# overnight_test failure dump",
            f"# iteration={iteration}  scenario={scenario}",
            f"# seed={self.seed}",
            f"# timestamp={datetime.now().isoformat()}",
            "",
        ]
        for i in range(1, self.nodes + 1):
            sections.append(f"\n══════════ node{i} ══════════")
            for label, cmd in (
                (
                    "pg_controldata",
                    f"docker exec pgbattery-node{i}-1 pg_controldata /var/lib/postgresql/data",
                ),
                (
                    "raft data dir (proxy for redb state)",
                    f"docker exec pgbattery-node{i}-1 ls -la /var/lib/postgresql/raft",
                ),
                (
                    "pgbattery status from node mgmt API",
                    f"curl -sf --max-time 3 http://localhost:908{i}/api/v1/cluster/leader",
                ),
                (
                    "last 200 lines of container log",
                    f"docker logs --tail 200 pgbattery-node{i}-1",
                ),
            ):
                sections.append(f"\n--- {label} ---")
                stdout, stderr, _ = self.run_command(cmd)
                sections.append(stdout.strip() or stderr.strip() or "(empty)")
        try:
            out_path.write_text("\n".join(sections), encoding="utf-8")
            console.print(f"  [dim]Failure artifacts → {out_path}[/]")
            return out_path
        except Exception as exc:
            console.print(f"  [yellow]Could not write artifacts: {exc}[/]")
            return None

    def cascading_failures(self) -> None:
        """Kill multiple nodes in quick succession to test resilience."""
        nodes_to_kill = min(2, self.nodes - 1)
        if self.verbose:
            console.print(f"  [dim]Killing {nodes_to_kill} nodes in succession[/]")

        killed = random.sample(range(1, self.nodes + 1), nodes_to_kill)
        for node in killed:
            self.run_command(f"docker kill pgbattery-node{node}-1")
            time.sleep(2)

        time.sleep(5)

        for i in range(1, self.nodes + 1):
            self.run_command(f"docker start pgbattery-node{i}-1")

    def fill_disk_and_recover(self) -> None:
        """Fill a node's data disk and ensure it recovers once space is freed."""
        node = random.randint(1, self.nodes)
        if self.verbose:
            console.print(f"  [dim]Filling disk on node {node}[/]")
        filler = "/var/lib/postgresql/data/.pgbattery_fill"
        self.run_command(
            f"docker exec pgbattery-node{node}-1 bash -c "
            f'"fallocate -l 2G {filler} || fallocate -l 1G {filler}"'
        )
        time.sleep(5)
        self.run_command(f"docker exec pgbattery-node{node}-1 rm -f {filler}")

    def raft_dir_readonly(self) -> None:
        """Make raft directory read-only to simulate storage fault."""
        node = random.randint(1, self.nodes)
        if self.verbose:
            console.print(f"  [dim]Setting raft dir read-only on node {node}[/]")
        self.run_command(
            f'docker exec pgbattery-node{node}-1 bash -c "chmod -R 500 /var/lib/postgresql/raft"'
        )
        time.sleep(5)
        self.run_command(
            f'docker exec pgbattery-node{node}-1 bash -c "chmod -R 700 /var/lib/postgresql/raft"'
        )

    def rotate_tls_cert(self) -> None:
        """Replace TLS cert on a node and restart it."""
        node = random.randint(1, self.nodes)
        if self.verbose:
            console.print(f"  [dim]Rotating TLS cert on node {node}[/]")
        self.run_command(
            f"docker exec pgbattery-node{node}-1 bash -c "
            '"openssl req -x509 -nodes -newkey rsa:2048 '
            "-keyout /var/lib/postgresql/tls/server.key "
            "-out /var/lib/postgresql/tls/server.crt "
            "-subj '/CN=node' -days 1\""
        )
        self.run_command(f"docker restart pgbattery-node{node}-1")
        time.sleep(3)

    def restore_corrupt_backup(self) -> None:
        """Create a backup, corrupt it, and ensure restore fails cleanly."""
        if self.verbose:
            console.print("  [dim]Creating backup for corruption test[/]")
        stdout, stderr, rc = self.run_command(
            "./target/release/pgbattery backup create --node localhost:9091"
        )
        if rc != 0:
            console.print(f"  [yellow]Backup creation failed: {stderr}[/]")
            return
        path = None
        for line in stdout.splitlines():
            if "Path:" in line:
                path = line.split("Path:")[1].strip()
        if not path:
            console.print("  [yellow]Could not find backup path[/]")
            return
        if self.verbose:
            console.print(f"  [dim]Corrupting backup {path}[/]")
        self.run_command(f"truncate -s 1M {path}")
        if self.verbose:
            console.print("  [dim]Attempting restore (expected to fail)[/]")
        stdout, stderr, rc = self.run_command(
            "./target/release/pgbattery backup restore "
            f"--filename {Path(path).name} --node localhost:9092"
        )
        if rc == 0:
            raise Exception("Corrupt backup restore unexpectedly succeeded")

    def _sleep_with_spinner(self, seconds: int) -> None:
        """Sleep with a visible countdown spinner."""
        with console.status(f"Sleeping {seconds}s before next test...") as status:
            remaining = seconds
            while remaining > 0:
                status.update(f"Sleeping {remaining}s before next test...")
                time.sleep(1)
                remaining -= 1

    def _build_progress_table(self, iteration: int, scenario_name: str) -> Table:
        """Build a rich table showing current test progress."""
        elapsed = datetime.now() - self.start_time
        remaining = self.end_time - datetime.now()
        if remaining.total_seconds() < 0:
            remaining = timedelta(0)

        passes = sum(1 for r in self.results if r.success)
        fails = sum(1 for r in self.results if not r.success)

        table = Table(
            title="Chaos Test Progress",
            show_header=False,
            box=None,
            padding=(0, 2),
        )
        table.add_column("Key", style="bold")
        table.add_column("Value")
        table.add_row("Iteration", str(iteration))
        table.add_row(
            "Elapsed / Remaining",
            f"{str(elapsed).split('.')[0]} / {str(remaining).split('.')[0]}",
        )
        table.add_row(
            "Pass / Fail",
            f"[green]{passes}[/] / [red]{fails}[/]",
        )
        table.add_row("Current scenario", scenario_name)
        return table

    def run(self) -> None:
        """Run chaos tests until duration expires."""
        console.rule("[bold]CHAOS TEST")
        console.print(f"Duration: [cyan]{self.duration_hours}[/] hours")
        console.print(f"Nodes: [cyan]{self.nodes}[/]")
        console.print(f"End time: [cyan]{self.end_time.strftime('%Y-%m-%d %H:%M:%S')}[/]")
        console.rule()

        scenarios = [
            ("Kill random node", self.kill_random_node),
            ("Kill leader", self.kill_leader),
            ("Network partition (pause node)", self.network_partition),
            ("Write and verify data", self.write_and_verify_data),
            ("Cascading failures", self.cascading_failures),
            ("Restart all nodes", self.restart_all_nodes),
            ("Fill disk and recover", self.fill_disk_and_recover),
            ("Make raft dir read-only", self.raft_dir_readonly),
            ("TLS certificate rotation", self.rotate_tls_cert),
            ("Corrupt backup restore", self.restore_corrupt_backup),
        ]

        iteration = 0
        try:
            while datetime.now() < self.end_time:
                iteration += 1
                scenario_name, scenario_func = random.choice(scenarios)
                full_name = f"Iter {iteration}: {scenario_name}"

                # Show progress dashboard briefly before each scenario
                with Live(
                    self._build_progress_table(iteration, scenario_name),
                    console=console,
                    refresh_per_second=1,
                    transient=True,
                ):
                    time.sleep(1)

                # Wait for healthy cluster before starting scenario
                healthy, status, elapsed = self.wait_for_healthy(timeout=60)
                if not healthy:
                    console.print(
                        f"  [bold yellow]WARNING[/] Cluster not healthy before "
                        f"{full_name} (waited {elapsed:.1f}s), skipping"
                    )
                    self.results.append(
                        TestResult(
                            name=full_name,
                            success=False,
                            duration=elapsed,
                            error="Cluster unhealthy before scenario start",
                            timestamp=datetime.now().isoformat(),
                            status_output=status[:200] if status else None,
                        )
                    )
                    # Try to let the cluster settle a bit more
                    self._sleep_with_spinner(15)
                    continue

                self.test_scenario(full_name, scenario_func)

                sleep_time = random.randint(10, 60)
                self._sleep_with_spinner(sleep_time)

        except KeyboardInterrupt:
            console.print("\n[yellow]Chaos test interrupted by user[/]")

        # Cumulative data integrity check
        if self.written_values:
            console.print("\n[bold]Verifying cumulative data integrity...[/]")
            all_ok, missing = self.verify_all_data()
            if all_ok:
                console.print(
                    f"  [bold green]All {len(self.written_values)} written values verified[/]"
                )
            else:
                console.print(
                    f"  [bold red]MISSING {len(missing)}/{len(self.written_values)} values:[/]"
                )
                for v in missing:
                    console.print(f"    [red]- {v}[/]")

        # Seq# durability sweep — catches writes that survived the per-scenario
        # check but were lost across a later fault, or ghost writes that
        # appeared with no attempt (split-brain).
        if self.acked_seqs or self.indeterminate_seqs:
            console.print("\n[bold]Verifying seq# durability and absence of ghost writes...[/]")
            self.wait_for_healthy(timeout=60)
            ok, lost, ghost = self.verify_seq_durability()
            if ok:
                console.print(
                    f"  [bold green]Durability OK[/]: "
                    f"{len(self.acked_seqs)} acked seq# all present "
                    f"({len(self.indeterminate_seqs)} indeterminate)"
                )
            else:
                if lost:
                    console.print(
                        f"  [bold red]DURABILITY VIOLATION[/]: {len(lost)} acked seq# missing: "
                        f"{sorted(lost)[:20]}{'…' if len(lost) > 20 else ''}"
                    )
                if ghost:
                    console.print(
                        f"  [bold red]GHOST WRITE[/]: {len(ghost)} seq# in DB never attempted: "
                        f"{sorted(ghost)[:20]}{'…' if len(ghost) > 20 else ''}"
                    )
                self.results.append(
                    TestResult(
                        name="end-of-run seq# durability",
                        success=False,
                        duration=0.0,
                        error=f"lost={sorted(lost)[:50]} ghost={sorted(ghost)[:50]}",
                        timestamp=datetime.now().isoformat(),
                    )
                )

        self.print_report()

    def print_report(self) -> None:
        """Print final test report and save results to JSON."""
        successes = sum(1 for r in self.results if r.success)
        failures = sum(1 for r in self.results if not r.success)
        total = len(self.results)

        console.print()
        console.rule("[bold]CHAOS TEST REPORT")

        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Key", style="bold")
        table.add_column("Value")
        table.add_row("Duration", f"{self.duration_hours} hours")
        table.add_row("Total iterations", str(total))
        table.add_row("Successes", f"[green]{successes}[/]")
        table.add_row("Failures", f"[red]{failures}[/]" if failures else "0")

        if total > 0:
            success_rate = 100 * successes / total
            color = "green" if success_rate == 100 else "yellow" if success_rate >= 80 else "red"
            table.add_row("Success rate", f"[{color}]{success_rate:.1f}%[/]")

            recovery_times = [r.duration for r in self.results if r.success]
            if recovery_times:
                avg_recovery = sum(recovery_times) / len(recovery_times)
                table.add_row("Avg recovery time", f"{avg_recovery:.1f}s")

        if self.written_values:
            table.add_row("Data values written", str(len(self.written_values)))

        if self.acked_seqs or self.indeterminate_seqs:
            table.add_row("Seq# acked", str(len(self.acked_seqs)))
            table.add_row("Seq# indeterminate", str(len(self.indeterminate_seqs)))

        console.print(table)

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        report_file = self.project_root / "testing" / f"chaos-report-{timestamp}.json"

        report = TestReport(
            test_run=TestRunSummary(
                duration_hours=self.duration_hours,
                nodes=self.nodes,
                seed=self.seed,
                start_time=(datetime.now() - timedelta(hours=self.duration_hours)).isoformat(),
                end_time=datetime.now().isoformat(),
            ),
            summary=TestReportStats(
                total=total,
                successes=successes,
                failures=failures,
                success_rate=(100 * successes / total) if total > 0 else 0,
            ),
            results=self.results,
        )

        with open(report_file, "w") as f:
            f.write(report.model_dump_json(indent=2))

        console.print(f"\nReport saved to: [cyan]{report_file}[/]")
        console.print(f"Seed: [cyan]{self.seed}[/]  (replay with --seed {self.seed})")
        console.rule()

        if failures > 0:
            console.print("\n[bold red]Failed Tests:[/]")
            for r in self.results:
                if not r.success:
                    error = r.error or "Unknown error"
                    console.print(f"  [red]- {r.name}: {error}[/]")


app = typer.Typer(help="Run chaos tests on pgbattery PostgreSQL HA cluster")


@app.command()
def run(
    hours: float = typer.Option(8.0, "--hours", "-h", help="Test duration in hours"),
    nodes: int = typer.Option(3, "--nodes", "-n", help="Number of nodes in cluster"),
    quick: bool = typer.Option(False, "--quick", "-q", help="Quick test mode (5 minutes)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    seed: int | None = typer.Option(None, "--seed", help="RNG seed for reproducible runs"),
) -> None:
    """Run overnight chaos testing on pgbattery cluster."""
    duration = 5 / 60 if quick else hours

    test = ChaosTest(duration_hours=duration, nodes=nodes, verbose=verbose, seed=seed)
    test.run()

    failures = sum(1 for r in test.results if not r.success)
    if failures > 0:
        console.print(f"\n[bold red]Test completed with {failures} failures[/]")
        raise typer.Exit(code=1)
    else:
        console.print("\n[bold green]All tests passed![/]")
        raise typer.Exit(code=0)


if __name__ == "__main__":
    app()
