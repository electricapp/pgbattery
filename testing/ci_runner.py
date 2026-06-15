#!/usr/bin/env -S uv run --project testing python
"""Deterministic HA CI runner for pgbattery.

Executes scenario suites defined in ``testing/ci_matrix.yaml`` against a Docker
Compose cluster.  Each suite is a list of test cases; each case has action,
assertion, and cleanup phases composed of typed steps (see ``StepType``).

The matrix file uses a ``.yaml`` extension but is intentionally valid JSON so
the runner can parse it with stdlib ``json`` alone — no PyYAML dependency in CI.

Exit codes:
    0: All cases passed.
    1: One or more cases failed, or a runner-level error occurred.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import typer
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from rich.console import Console
from rich.table import Table

# ---------------------------------------------------------------------------
# Step types
# ---------------------------------------------------------------------------


class StepType(StrEnum):
    """Discriminator for steps in the scenario matrix.

    Each value maps 1-to-1 to a handler in ``CIRunner._execute_step``.

    Attributes:
        CMD: Run an arbitrary shell command with optional exit-code and
            stdout/stderr assertions.
        SLEEP: Pause execution for a fixed number of seconds.
        WAIT_CLUSTER: Poll the management API until the cluster reaches the
            expected node count and leader count (or timeout).
        RECORD_LEADER: Snapshot the current leader ID into a context variable
            for later comparison.
        CLUSTER_TOPOLOGY: Assert exact node and leader counts right now
            (no polling).
        LEADER_NOT: Assert the current leader differs from a previously
            recorded variable.
        LEADER_EQUALS: Assert the current leader matches a literal node ID.
        LEADER_EQUALS_VAR: Assert the current leader matches a context
            variable.
        METRIC_EXISTS: Assert a Prometheus metric is present on a given node.
        METRIC_EQUALS: Assert a Prometheus metric equals an expected value
            within tolerance.
        METRIC_LEADER_COUNT: Assert exactly N nodes report ``is_leader=1`` in
            Prometheus.
        WAIT_METRIC: Poll a Prometheus metric on a node until it appears (or
            timeout). Use as an action when a metric may lag cluster
            convergence by a few ticks.
        HTTP: Perform an HTTP request with status, body, and JSON-field
            assertions.
        TRANSFER_LEADERSHIP: POST to the management API to move leadership to
            a target node.
        BASENAME: Extract the filename component of a context variable (used
            for backup paths).
        SQL: Pipe a ``.sql`` file from ``testing/sql/`` through ``psql`` on a
            cluster node via stdin — zero shell escaping required.
        ASYMMETRIC_PARTITION: Drop inbound traffic to a node from a specific
            peer using iptables (requires ``NET_ADMIN`` capability).
        ASYMMETRIC_HEAL: Remove the iptables DROP rule added by
            ``ASYMMETRIC_PARTITION``.
        CLOCK_SKEW: Write a libfaketime offset to ``/tmp/faketime`` on a node,
            shifting its apparent clock by ``seconds`` (requires
            ``LD_PRELOAD=libfaketime.so.1`` and ``FAKETIME_TIMESTAMP_FILE``
            set in the container environment).
        CLOCK_HEAL: Restore a node's faketime offset to ``+0s`` (real time).
        WAIT_SYNC: Poll ``/api/v1/cluster/node/{id}/lag`` on all follower nodes
            until ``lag_bytes == 0`` and ``is_synced == true``, or timeout.
            Optional ``nodes`` parameter (list of int IDs) to restrict which
            nodes are checked; defaults to all nodes minus the current leader.
        NETWORK_DELAY: Add ``tc netem delay`` to a node's ``eth0`` interface
            (requires ``NET_ADMIN`` capability and ``iproute2`` in the image).
            Parameters: ``node`` (int), ``delay_ms`` (int, default 200),
            ``jitter_ms`` (int, default 50).
        NETWORK_HEAL: Remove the ``tc netem`` rule added by ``NETWORK_DELAY``.
            Parameter: ``node`` (int).
        PGBENCH: Run pgbench against a node's internal PostgreSQL port.
            Initialises the pgbench schema (``pgbench -i``) then runs the
            default read-write workload for ``duration_sec`` seconds and
            asserts the measured TPS is at least ``min_tps``.
            Parameters: ``node`` (int, default 1), ``scale`` (int, default 1),
            ``clients`` (int, default 4), ``threads`` (int, default 2),
            ``duration_sec`` (int, default 10), ``min_tps`` (float, default
            100.0), ``capture_tps`` (str, optional context variable name).
    """

    CMD = "cmd"
    SLEEP = "sleep"
    WAIT_CLUSTER = "wait_cluster"
    RECORD_LEADER = "record_leader"
    CLUSTER_TOPOLOGY = "cluster_topology"
    LEADER_NOT = "leader_not"
    LEADER_EQUALS = "leader_equals"
    LEADER_EQUALS_VAR = "leader_equals_var"
    METRIC_EXISTS = "metric_exists"
    METRIC_EQUALS = "metric_equals"
    METRIC_LEADER_COUNT = "metric_leader_count"
    WAIT_METRIC = "wait_metric"
    HTTP = "http"
    TRANSFER_LEADERSHIP = "transfer_leadership"
    BASENAME = "basename"
    SQL = "sql"
    ASYMMETRIC_PARTITION = "asymmetric_partition"
    ASYMMETRIC_HEAL = "asymmetric_heal"
    CLOCK_SKEW = "clock_skew"
    CLOCK_HEAL = "clock_heal"
    WAIT_SYNC = "wait_sync"
    NETWORK_DELAY = "network_delay"
    NETWORK_HEAL = "network_heal"
    PGBENCH = "pgbench"


# Static IP addresses for each node on the raft_net bridge network.
# Used by asymmetric_partition / asymmetric_heal to build iptables rules.
_NODE_IPS: Final[dict[int, str]] = {
    1: "172.28.0.11",
    2: "172.28.0.12",
    3: "172.28.0.13",
}


# ---------------------------------------------------------------------------
# Pydantic models — matrix config & API responses
# ---------------------------------------------------------------------------


class ClusterNodeConfig(BaseModel):
    """Static configuration for a single cluster node.

    Attributes:
        id: Raft node ID (1-based).
        name: Docker Compose service name (e.g. ``node1``).
        mgmt_url: Base URL for the management API (e.g. ``http://localhost:9081``).
        metrics_url: Full URL for the Prometheus metrics endpoint.
    """

    id: int
    name: str
    mgmt_url: str
    metrics_url: str


class ClusterConfig(BaseModel):
    """Cluster-wide defaults from the matrix header.

    Attributes:
        expected_nodes: Default node count used by ``wait_cluster`` steps when
            the step omits ``nodes``.
        nodes: Static list of node configurations.
    """

    expected_nodes: int
    nodes: list[ClusterNodeConfig]


class SuiteConfig(BaseModel):
    """Configuration for a named suite of test cases.

    Attributes:
        description: Human-readable purpose of the suite.
        reuse_cluster: If ``True``, bring the cluster up once and run all
            cases sequentially; otherwise stand up / tear down per case.
        max_wait_cluster_seconds: Optional convergence budget applied to every
            ``wait_cluster`` step in this suite.
        cases: Ordered list of case IDs to execute.
    """

    description: str = ""
    reuse_cluster: bool = False
    max_wait_cluster_seconds: int | None = None
    cases: list[str]


class CaseConfig(BaseModel):
    """A single test case: actions → assertions → cleanup.

    Attributes:
        id: Unique identifier referenced by suites.
        description: Human-readable summary.
        tests_md_ref: Cross-reference to the ``TESTS.md`` section.
        actions: Steps that mutate cluster state (faults, writes, etc.).
        assertions: Steps that verify invariants after actions complete.
        cleanup: Steps that restore the cluster for the next case; always
            executed even if actions/assertions fail.
    """

    id: str
    description: str = ""
    tests_md_ref: str = ""
    actions: list[dict[str, Any]] = []
    assertions: list[dict[str, Any]] = []
    cleanup: list[dict[str, Any]] = []


class MatrixConfig(BaseModel):
    """Top-level schema for ``ci_matrix.yaml``.

    Attributes:
        version: Schema version (currently ``1``).
        compose_file: Path to ``docker-compose.yml`` relative to project root.
        cluster: Cluster-wide configuration.
        suites: Named suites mapping to ordered case lists.
        cases: All case definitions (referenced by suites).
    """

    version: int
    compose_file: str
    cluster: ClusterConfig
    suites: dict[str, SuiteConfig]
    cases: list[CaseConfig]


class ClusterNodeState(BaseModel):
    """Runtime state of a single node as returned by ``/api/v1/cluster/nodes``.

    Attributes:
        node_id: Raft node ID.
        is_leader: Whether this node currently holds the leader lease.
    """

    node_id: int
    is_leader: bool


class ClusterNodesResponse(BaseModel):
    """Response from ``GET /api/v1/cluster/nodes``."""

    nodes: list[ClusterNodeState]


class LeaderResponse(BaseModel):
    """Response from ``GET /api/v1/cluster/leader``."""

    leader_id: int | None = None


class TransferLeadershipResponse(BaseModel):
    """Response from ``POST /api/v1/cluster/transfer-leadership/{id}``."""

    success: bool


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------


@dataclass
class CaseSummary:
    """Outcome of a single test case, used to build the final table.

    Attributes:
        case_id: The case identifier from the matrix.
        passed: ``True`` if all actions and assertions succeeded.
        detail: Elapsed time on success, or the error message on failure.
    """

    case_id: str
    passed: bool
    detail: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class RunnerError(Exception):
    """Raised for deterministic scenario failures.

    Caught at the case level to collect failure artifacts before propagating.
    """


def utc_timestamp() -> str:
    """Return a filesystem-safe UTC timestamp (``YYYYMMDDTHHMMSSz``)."""
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def safe_name(value: str) -> str:
    """Convert free-form text to a filesystem-safe token.

    Args:
        value: Arbitrary string (e.g. a case ID or step type).

    Returns:
        Lowercased string with non-alphanumeric runs replaced by ``-``.
    """
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-")


def parse_matrix(path: Path) -> MatrixConfig:
    """Load and validate the scenario matrix.

    Attempts stdlib JSON first, falling back to PyYAML if installed.

    Args:
        path: Absolute path to the matrix file.

    Returns:
        Validated ``MatrixConfig``.

    Raises:
        RunnerError: If the file cannot be parsed or fails validation.
    """
    text = path.read_text(encoding="utf-8")

    raw: Any = None
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml
        except ImportError as exc:
            raise RunnerError(
                f"Failed to parse {path}. "
                "File must be valid JSON syntax unless PyYAML is installed."
            ) from exc

        raw = yaml.safe_load(text)
        if not isinstance(raw, dict):
            raise RunnerError(f"Matrix {path} must contain a top-level object.") from None

    try:
        return MatrixConfig.model_validate(raw)
    except ValidationError as exc:
        raise RunnerError(f"Invalid matrix config: {exc}") from exc


def get_json_path(data: Any, path: str) -> Any:
    """Traverse a parsed JSON object using a dotted path.

    Supports dict keys and integer list indices (e.g. ``nodes.0.id``).

    Args:
        data: Parsed JSON value (dict, list, or scalar).
        path: Dot-separated key/index path.

    Returns:
        The value at the given path.

    Raises:
        RunnerError: If any segment is missing or the structure is unexpected.
    """
    current = data
    for token in path.split("."):
        if isinstance(current, dict):
            if token not in current:
                raise RunnerError(f"JSON path '{path}' missing key '{token}'.")
            current = current[token]
            continue
        if isinstance(current, list):
            if not token.isdigit():
                raise RunnerError(f"JSON path '{path}' expected list index, got '{token}'.")
            index = int(token)
            if index < 0 or index >= len(current):
                raise RunnerError(
                    f"JSON path '{path}' index {index} out of bounds for length {len(current)}."
                )
            current = current[index]
            continue
        raise RunnerError(f"JSON path '{path}' is not traversable at '{token}'.")
    return current


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class CIRunner:
    """Executes scenario suites against a Docker Compose pgbattery cluster.

    Lifecycle:
        1. Parse the matrix and resolve the requested suite/case.
        2. For each case: start cluster (if needed) → actions → assertions →
           cleanup → stop cluster (if needed).
        3. Collect snapshots (management API state, metrics, ``docker compose
           ps``) before, after, and on failure for each case.
        4. Write per-step logs, per-case results, and a summary table.

    Template variables:
        Steps may contain ``{{ var }}`` placeholders that are resolved against
        ``self.context`` at execution time.  Variables are set by
        ``record_leader``, ``capture_stdout``, ``capture_json``, and
        ``basename`` steps.
    """

    TEMPLATE_PATTERN: re.Pattern[str] = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}")

    def __init__(
        self,
        project_root: Path,
        matrix_path: Path,
        suite: str,
        case_filter: str | None,
        artifact_dir: Path,
        build_images: bool,
        keep_cluster_on_failure: bool,
        console: Console,
    ) -> None:
        """Initialise the runner, validate the suite, and create artifact dirs.

        Args:
            project_root: Repository root (parent of ``testing/``).
            matrix_path: Absolute path to ``ci_matrix.yaml``.
            suite: Name of the suite to execute.
            case_filter: If set, run only this single case from the suite.
            artifact_dir: Base directory for all output artifacts.
            build_images: Whether to pass ``--build`` to ``docker compose up``.
            keep_cluster_on_failure: Skip ``docker compose down`` on failure
                so the user can inspect container state.
            console: Rich console for terminal output.

        Raises:
            RunnerError: If the suite name is unknown or the case filter
                doesn't belong to the suite.
        """
        self.project_root: Path = project_root
        self.matrix_path: Path = matrix_path
        self.matrix: MatrixConfig = parse_matrix(matrix_path)
        self.suite_name: str = suite
        self.case_filter: str | None = case_filter
        self.artifact_dir: Path = artifact_dir
        self.build_images: bool = build_images
        self.keep_cluster_on_failure: bool = keep_cluster_on_failure
        self.console: Console = console

        self.context: dict[str, Any] = {}
        self.summary: list[CaseSummary] = []
        self.failed: bool = False

        if self.suite_name not in self.matrix.suites:
            available = ", ".join(sorted(self.matrix.suites.keys()))
            raise RunnerError(f"Unknown suite '{self.suite_name}'. Available: {available}")

        self.case_map: dict[str, CaseConfig] = {case.id: case for case in self.matrix.cases}
        self.suite_config: SuiteConfig = self.matrix.suites[self.suite_name]
        self.selected_case_ids: list[str] = self._select_cases()

        self.node_map: dict[int, ClusterNodeConfig] = {
            node.id: node for node in self.matrix.cluster.nodes
        }

        # Load <project_root>/.env so the runner picks up the same secrets
        # docker-compose feeds the containers (notably
        # PGBATTERY_MANAGEMENT_API_TOKEN). override=False keeps shell-exported
        # values authoritative.
        load_dotenv(self.project_root / ".env", override=False)
        self.env: dict[str, str] = os.environ.copy()
        self.env["COMPOSE_FILE"] = str((self.project_root / self.matrix.compose_file).resolve())
        self.mgmt_token: str = self.env.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")

        self.system_dir: Path = self.artifact_dir / "system"
        self.case_dir_root: Path = self.artifact_dir / "cases"
        self.snapshot_dir: Path = self.artifact_dir / "snapshots"
        for directory in [
            self.artifact_dir,
            self.system_dir,
            self.case_dir_root,
            self.snapshot_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)

    # -- Suite / case selection ----------------------------------------------

    def _select_cases(self) -> list[str]:
        """Return the list of case IDs to run, respecting ``case_filter``.

        Raises:
            RunnerError: If ``case_filter`` is set but not found in the suite.
        """
        case_ids = list(self.suite_config.cases)
        if self.case_filter is None:
            return case_ids
        if self.case_filter not in case_ids:
            raise RunnerError(f"Case '{self.case_filter}' is not in suite '{self.suite_name}'.")
        return [self.case_filter]

    # -- Output helpers ------------------------------------------------------

    def log(self, message: str) -> None:
        """Print a message to the terminal without Rich markup interpretation."""
        self.console.print(message, markup=False, highlight=False)

    def _write_text(self, path: Path, text: str) -> None:
        """Write ``text`` to ``path``, creating parent directories as needed."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    # -- Template rendering --------------------------------------------------

    def _render_template(self, text: str) -> str:
        """Replace ``{{ var }}`` placeholders with values from ``self.context``.

        Args:
            text: String potentially containing ``{{ variable }}`` tokens.

        Returns:
            Rendered string with all placeholders substituted.

        Raises:
            RunnerError: If a referenced variable is not in context.
        """

        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in self.context:
                raise RunnerError(f"Template variable '{key}' is not defined.")
            return str(self.context[key])

        return self.TEMPLATE_PATTERN.sub(repl, text)

    # -- Shell execution -----------------------------------------------------

    def _run_shell(
        self,
        command: str,
        log_path: Path,
        expect_exit: int | list[int] | None = 0,
    ) -> subprocess.CompletedProcess[str]:
        """Run a shell command, log output, and optionally assert exit code.

        Args:
            command: Shell command string (may contain ``{{ var }}`` templates).
            log_path: File to write the command, exit code, stdout, and stderr.
            expect_exit: Expected exit code(s).  ``None`` accepts any code.

        Returns:
            The completed process.

        Raises:
            RunnerError: If the actual exit code is not in ``expect_exit``.
        """
        rendered = self._render_template(command)
        proc = subprocess.run(
            rendered,
            shell=True,
            cwd=self.project_root,
            env=self.env,
            capture_output=True,
            text=True,
        )

        log_text = [
            f"$ {rendered}",
            f"exit_code: {proc.returncode}",
            "",
            "--- stdout ---",
            proc.stdout,
            "--- stderr ---",
            proc.stderr,
        ]
        self._write_text(log_path, "\n".join(log_text))

        if expect_exit is None:
            return proc

        expected = [expect_exit] if isinstance(expect_exit, int) else list(expect_exit)

        if proc.returncode not in expected:
            raise RunnerError(
                f"Command failed with exit code {proc.returncode}, expected {expected}: {rendered}"
            )
        return proc

    # -- HTTP helpers --------------------------------------------------------

    def _http_request(
        self,
        method: str,
        url: str,
        timeout_sec: int = 10,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, str]:
        """Perform an HTTP request and return ``(status_code, body)``.

        Args:
            method: HTTP method (GET, POST, etc.).
            url: Fully qualified URL.
            timeout_sec: Socket-level timeout.
            headers: Optional extra headers to include.

        Returns:
            Tuple of ``(status_code, response_body)``.

        Raises:
            RunnerError: On connection-level failures (not HTTP error codes).
        """
        req = urllib.request.Request(url=url, method=method.upper())
        for key, val in (headers or {}).items():
            req.add_header(key, val)
        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as response:
                body = response.read().decode("utf-8", errors="replace")
                return response.getcode(), body
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            return exc.code, body
        except urllib.error.URLError as exc:
            raise RunnerError(f"HTTP request failed for {url}: {exc}") from exc
        except (ConnectionResetError, ConnectionRefusedError, TimeoutError, OSError) as exc:
            # Connection-level failures during the read phase aren't wrapped in URLError
            raise RunnerError(f"HTTP connection error for {url}: {exc}") from exc

    def _parse_json(self, body: str, context: str) -> Any:
        """Parse a JSON string, wrapping decode errors in ``RunnerError``.

        Args:
            body: Raw JSON string.
            context: Descriptive label for error messages (e.g. the URL).
        """
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise RunnerError(f"Invalid JSON from {context}: {exc}") from exc

    # -- Cluster state queries -----------------------------------------------

    def _get_cluster_nodes(self) -> list[ClusterNodeState]:
        """Query ``/api/v1/cluster/nodes`` from each node until one responds.

        Tries nodes in ID order; returns the first successful response.

        Raises:
            RunnerError: If no node returns a valid response.
        """
        errors: list[str] = []
        for node_id in sorted(self.node_map):
            node = self.node_map[node_id]
            url = f"{node.mgmt_url}/api/v1/cluster/nodes"
            try:
                status, body = self._http_request("GET", url, timeout_sec=5)
                if status != 200:
                    errors.append(f"{url} returned {status}")
                    continue
                parsed = self._parse_json(body, url)
                try:
                    response = ClusterNodesResponse.model_validate(parsed)
                except ValidationError as exc:
                    errors.append(f"{url} invalid nodes response: {exc}")
                    continue
                return response.nodes
            except RunnerError as exc:
                errors.append(str(exc))
        raise RunnerError("Unable to fetch cluster nodes from management API: " + "; ".join(errors))

    def _get_leader_id(self) -> int | None:
        """Query ``/api/v1/cluster/leader`` from each node until one responds.

        Returns:
            The current leader's node ID, or ``None`` if the cluster reports
            no leader.

        Raises:
            RunnerError: If no node returns a valid response.
        """
        errors: list[str] = []
        for node_id in sorted(self.node_map):
            node = self.node_map[node_id]
            url = f"{node.mgmt_url}/api/v1/cluster/leader"
            try:
                status, body = self._http_request("GET", url, timeout_sec=5)
                if status != 200:
                    errors.append(f"{url} returned {status}")
                    continue
                parsed = self._parse_json(body, url)
                try:
                    response = LeaderResponse.model_validate(parsed)
                except ValidationError as exc:
                    errors.append(f"{url} invalid leader response: {exc}")
                    continue
                return response.leader_id
            except RunnerError as exc:
                errors.append(str(exc))
        raise RunnerError("Unable to discover leader from management API: " + "; ".join(errors))

    def _get_leader_views(self) -> dict[int, int | None]:
        """Query every node's ``/api/v1/cluster/leader`` independently.

        Unlike :meth:`_get_leader_id` (which returns the first successful
        response), this collects each node's own view of who the leader is.
        Used to detect partition and split-brain: the test runner reaches
        every node directly (partitions are between nodes, not between the
        runner and any node), so cross-node disagreement here is a true
        partition signal — independent of the leader's
        ``disconnect_timeout`` metric grace window.

        Returns:
            ``{node_id: leader_id_seen}``. Unreachable or malformed nodes
            are omitted entirely so the caller can distinguish them from
            "leader is None".
        """
        views: dict[int, int | None] = {}
        for node_id, node in self.node_map.items():
            url = f"{node.mgmt_url}/api/v1/cluster/leader"
            try:
                status, body = self._http_request("GET", url, timeout_sec=3)
                if status != 200:
                    continue
                parsed = self._parse_json(body, url)
                response = LeaderResponse.model_validate(parsed)
                views[node_id] = response.leader_id
            except (RunnerError, ValidationError):
                continue
        return views

    def _resolve_node_ref(self, ref: Any) -> int:
        """Resolve a step's ``node_id`` reference to an int.

        Accepts either an integer (used verbatim) or the string ``"leader"``,
        which is resolved to the current leader at assertion time.  This lets
        test cases assert on leader-only metrics (``pgbattery_replication_sync``,
        ``pgbattery_sync_replicas``, etc.) without hard-coding a node number
        that may not actually be the leader after prior failovers — the root
        cause of the flaky ``sync-replica-failure`` assertion.

        Args:
            ref: Raw value from the step dict.  Usually an ``int`` but may be
                the string ``"leader"``.

        Returns:
            The concrete node ID.

        Raises:
            RunnerError: If ``ref`` is ``"leader"`` but no leader is elected,
                or if the value is neither an int nor the literal string.
        """
        if isinstance(ref, bool):
            # isinstance(True, int) is True, so catch this before the int branch.
            raise RunnerError(f"Invalid node reference: {ref!r}")
        if isinstance(ref, int):
            return ref
        if isinstance(ref, str):
            if ref == "leader":
                leader = self._get_leader_id()
                if leader is None:
                    raise RunnerError(
                        "node_id='leader' cannot be resolved: no leader is currently elected."
                    )
                return leader
            try:
                return int(ref)
            except ValueError as exc:
                raise RunnerError(
                    f"Invalid node reference {ref!r}: expected int or 'leader'"
                ) from exc
        raise RunnerError(f"Invalid node reference type {type(ref).__name__}: {ref!r}")

    def _get_voter_count(self) -> int:
        """Query /api/v1/cluster/members and count voters. Returns 0 on error."""
        for node_id in sorted(self.node_map):
            node = self.node_map[node_id]
            url = f"{node.mgmt_url}/api/v1/cluster/members"
            try:
                status, body = self._http_request("GET", url, timeout_sec=5)
                if status != 200:
                    continue
                data = json.loads(body)
                return sum(1 for m in data.get("members", []) if m.get("role") == "voter")
            except (RunnerError, json.JSONDecodeError, KeyError):
                continue
        return 0

    def _wait_for_cluster(
        self,
        expected_nodes: int,
        expected_leaders: int,
        timeout_sec: int,
        leader_not: int | None = None,
        leader_equals: int | None = None,
        require_all_voters: bool = False,
        require_replication_health: bool = False,
        min_healthy_replicas: int = 1,
        live_nodes: int | None = None,
    ) -> None:
        """Poll until the cluster reaches the expected topology or timeout.

        Args:
            expected_nodes: Required number of nodes in the ``/nodes`` response.
            expected_leaders: Required number of nodes with ``is_leader=True``.
            timeout_sec: Maximum seconds to wait.
            leader_not: If set, also require the current leader's id != this value
                (used after killing the leader to wait for actual failover).
            leader_equals: If set, also require the current leader's id == this value
                (used after a leadership transfer to confirm the target is leading).
            require_all_voters: If True, also require all expected_nodes to be voters
                (used at startup to ensure auto-promotion completed before tests).
            require_replication_health: If True, enforce the exact replica shape
                derived from ``live_nodes``: 3-live → 1 Sync + 1 Potential,
                2-live → 1 Sync, 1-live → no replicas. ``async_count`` must be 0
                in every case. Uses ``pgbattery_replica_is_sync`` (2=Sync,
                1=Potential, 0=Async).
            live_nodes: How many of the ``expected_nodes`` voters are expected
                to be reachable/streaming at this wait point. Defaults to
                ``expected_nodes`` (true full-health wait). Set this lower for
                steps that intentionally have a downed node (e.g. ``live_nodes:
                2`` immediately after killing one of three). Drives the strict
                replica-shape and leader-views checks; ``expected_nodes`` still
                governs the raft membership topology check.

        Raises:
            RunnerError: If the cluster does not converge in time, or if an
                illegal state is observed (split-brain, more sync replicas than
                ``FIRST 1`` semantics permit) that cannot transiently become
                healthy.
        """
        live = live_nodes if live_nodes is not None else expected_nodes
        if live > expected_nodes or live < 0:
            raise RunnerError(f"Invalid live_nodes={live} (must be 0..{expected_nodes})")
        expected_sync = 1 if live >= 2 else 0
        expected_potential = 1 if live >= 3 else 0
        allowed_missing_views = expected_nodes - live
        deadline = time.time() + timeout_sec
        last_error = "cluster did not converge"
        while time.time() < deadline:
            try:
                nodes = self._get_cluster_nodes()
                node_count = len(nodes)
                leaders = [node for node in nodes if node.is_leader]
                leader_count = len(leaders)
                # Split-brain is never a transient legitimate state.
                if leader_count > 1:
                    raise RunnerError(
                        f"Illegal cluster state: {leader_count} concurrent leaders "
                        f"(split brain): {[n.node_id for n in leaders]}"
                    )
                topology_ok = node_count == expected_nodes and leader_count == expected_leaders
                leader_changed = leader_not is None or (
                    leader_count == 1 and leaders[0].node_id != leader_not
                )
                leader_eq_ok = leader_equals is None or (
                    leader_count == 1 and leaders[0].node_id == leader_equals
                )
                voters_ok = True
                voter_count = expected_nodes
                if require_all_voters:
                    voter_count = self._get_voter_count()
                    voters_ok = voter_count == expected_nodes
                repl_ok = True
                repl_detail = ""
                # Cross-node leader-view check: every node must independently
                # agree on the same leader. This catches partition without
                # relying on the leader's replica-metric grace window (which
                # can stale-report a partitioned follower as Sync for up to
                # ``disconnect_timeout`` after the partition starts).
                views_ok = True
                views_detail = ""
                if require_replication_health and topology_ok and leader_count == 1:
                    elected_leader = leaders[0].node_id
                    views = self._get_leader_views()
                    self_claimed_leaders = {nid for nid, seen in views.items() if seen == nid}
                    # Split-brain: two distinct nodes each claim themselves as
                    # leader. Raft forbids this for a given term; surface it
                    # immediately rather than waiting for the timeout.
                    if len(self_claimed_leaders) > 1:
                        raise RunnerError(
                            f"Illegal cluster state: multiple self-claimed leaders "
                            f"{sorted(self_claimed_leaders)}"
                        )
                    missing = sorted(set(self.node_map) - set(views))
                    disagreeing = sorted(
                        nid for nid, seen in views.items() if seen != elected_leader
                    )
                    # Allow up to `allowed_missing_views` nodes to be
                    # unreachable when the caller declared fewer live nodes
                    # than configured voters. All responding nodes must still
                    # agree on the elected leader.
                    views_ok = len(missing) <= allowed_missing_views and not disagreeing
                    views_detail = (
                        f", leader_views agree_on={elected_leader}"
                        f" missing={missing}/{allowed_missing_views}"
                        f" disagreeing={disagreeing}"
                    )
                if require_replication_health and topology_ok and leader_count == 1:
                    leader_id = leaders[0].node_id
                    fetch_failed = False
                    try:
                        per_replica = self._fetch_metric_values(
                            leader_id, "pgbattery_replica_is_sync"
                        )
                        healthy_vals = self._fetch_metric_values(
                            leader_id, "pgbattery_healthy_replicas"
                        )
                    except RunnerError:
                        fetch_failed = True
                        per_replica, healthy_vals = [], []
                    if fetch_failed:
                        repl_ok = False
                        repl_detail = ", repl metrics unavailable"
                    else:
                        sync_count = sum(1 for v in per_replica if v >= 1.5)
                        potential_count = sum(1 for v in per_replica if 0.5 <= v < 1.5)
                        async_count = sum(1 for v in per_replica if v < 0.5)
                        observed_replicas = len(per_replica)
                        expected_replicas = max(0, live - 1)
                        healthy_count = int(healthy_vals[0]) if healthy_vals else 0
                        # Fail-fast on invariants that cannot transiently become healthy:
                        # - FIRST 1 (...) sync standby semantics: >expected_sync in
                        #   Sync state is a topology violation, not a wait.
                        # - More replicas than configured nodes: stale/duplicate state.
                        if sync_count > expected_sync:
                            raise RunnerError(
                                f"Illegal cluster state: sync_replicas={sync_count} > "
                                f"expected={expected_sync} for {expected_nodes}-node cluster"
                                f" (FIRST 1 sync standby invariant violated)"
                            )
                        # Fail-fast bound is the configured voter ceiling
                        # (``expected_nodes - 1``), not ``expected_replicas``.
                        # During recovery a node may rejoin sooner than the
                        # caller's ``live_nodes`` hint anticipated; that just
                        # makes the strict success check temporarily false
                        # until it converges, not an illegal state.
                        if observed_replicas > expected_nodes - 1:
                            raise RunnerError(
                                f"Illegal cluster state: observed_replicas={observed_replicas} > "
                                f"max={expected_nodes - 1} for {expected_nodes}-node cluster"
                            )
                        # Success requires the leader to enumerate ALL expected
                        # replicas. A partitioned follower dropped from the leader's
                        # status map shows up here as observed < expected — keep
                        # waiting rather than declaring healthy on a partial view.
                        repl_ok = (
                            observed_replicas == expected_replicas
                            and sync_count == expected_sync
                            and potential_count == expected_potential
                            and async_count == 0
                            and healthy_count >= max(0, min_healthy_replicas)
                        )
                        repl_detail = (
                            f", repl observed={observed_replicas}/{expected_replicas} "
                            f"sync={sync_count}/{expected_sync} "
                            f"potential={potential_count}/{expected_potential} "
                            f"async={async_count}/0 "
                            f"healthy={healthy_count}/{min_healthy_replicas}"
                        )
                if (
                    topology_ok
                    and leader_changed
                    and leader_eq_ok
                    and voters_ok
                    and views_ok
                    and repl_ok
                ):
                    return
                last_error = (
                    f"observed nodes={node_count}, leaders={leader_count}, voters={voter_count}, "
                    f"expected nodes={expected_nodes}, leaders={expected_leaders}"
                )
                if require_all_voters:
                    last_error += f", expected_voters={expected_nodes}"
                if leader_not is not None and leader_count == 1:
                    last_error += (
                        f", current_leader={leaders[0].node_id}"
                        f" (waiting for change from {leader_not})"
                    )
                if leader_equals is not None and leader_count == 1:
                    last_error += (
                        f", current_leader={leaders[0].node_id}"
                        f" (waiting for leader={leader_equals})"
                    )
                if require_replication_health:
                    last_error += views_detail
                    last_error += repl_detail
            except RunnerError as exc:
                # Illegal-state errors are permanent — don't swallow them into
                # the polling loop. Transient HTTP/metric errors are retried.
                if str(exc).startswith("Illegal cluster state"):
                    raise
                last_error = str(exc)
            time.sleep(2)
        raise RunnerError(f"Timed out waiting for cluster convergence: {last_error}")

    # -- Metrics helpers -----------------------------------------------------

    def _fetch_metric_values(self, node_id: int, metric_name: str) -> list[float]:
        """Scrape Prometheus metrics for a node and extract values by name.

        Parses the text exposition format, matching lines like
        ``metric_name{labels} 42.0`` or ``metric_name 42.0``.

        Args:
            node_id: Node to scrape.
            metric_name: Exact metric name (no regex).

        Returns:
            List of float values (empty if the metric is not found).

        Raises:
            RunnerError: If the metrics endpoint is unreachable or returns
                a non-200 status.
        """
        node = self.node_map.get(node_id)
        if not node:
            raise RunnerError(f"Unknown node_id {node_id} for metric lookup.")
        status, body = self._http_request("GET", node.metrics_url, timeout_sec=5)
        if status != 200:
            raise RunnerError(f"Metrics endpoint {node.metrics_url} returned status {status}.")
        pattern = re.compile(
            rf"^{re.escape(metric_name)}(?:\{{[^}}]*\}})?\s+"
            r"(-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)$"
        )
        values: list[float] = []
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            match = pattern.match(line)
            if match:
                values.append(float(match.group(1)))
        return values

    def _poll_metric_values(
        self, node_id: int, metric_name: str, timeout_sec: int = 10
    ) -> list[float]:
        """Poll until a metric appears on a node, then return its values.

        Metrics can lag cluster convergence by one or more tick intervals
        (typically 1s). This helper retries at 1s intervals so callers don't
        need explicit ``wait_metric`` actions before every metric assertion.

        Args:
            node_id: Node to scrape.
            metric_name: Exact metric name (no regex).
            timeout_sec: How long to wait before giving up.

        Returns:
            Non-empty list of float values.

        Raises:
            RunnerError: If the metric does not appear within ``timeout_sec``.
        """
        deadline = time.time() + timeout_sec
        while True:
            values = self._fetch_metric_values(node_id=node_id, metric_name=metric_name)
            if values:
                return values
            if time.time() >= deadline:
                raise RunnerError(
                    f"Metric '{metric_name}' missing on node {node_id} after {timeout_sec}s."
                )
            time.sleep(1)

    # -- Artifact collection -------------------------------------------------

    def _case_dir(self, case_id: str) -> Path:
        """Return (and create) the artifact directory for a case."""
        path = self.case_dir_root / safe_name(case_id)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _collect_snapshot(self, label: str) -> None:
        """Capture a point-in-time snapshot of the cluster for debugging.

        Collects ``docker compose ps``, management API responses (``/leader``
        and ``/nodes``), and Prometheus metrics from every node.  Each snapshot
        is written to a timestamped subdirectory under ``snapshots/``.

        Args:
            label: Human-readable label embedded in the directory name.
        """
        snap = self.snapshot_dir / f"{utc_timestamp()}-{safe_name(label)}"
        snap.mkdir(parents=True, exist_ok=True)

        compose_ps = self._run_shell(
            "docker compose ps",
            snap / "compose-ps.txt",
            expect_exit=None,
        )
        if compose_ps.returncode != 0:
            self._write_text(
                snap / "compose-ps.error.txt",
                f"docker compose ps failed with {compose_ps.returncode}",
            )

        for node_id in sorted(self.node_map):
            node = self.node_map[node_id]
            for endpoint in ["leader", "nodes"]:
                url = f"{node.mgmt_url}/api/v1/cluster/{endpoint}"
                path = snap / f"node{node_id}-{endpoint}.json"
                try:
                    status, body = self._http_request("GET", url, timeout_sec=5)
                    self._write_text(path, body)
                    self._write_text(path.with_suffix(".status.txt"), str(status))
                except RunnerError as exc:
                    self._write_text(path.with_suffix(".error.txt"), str(exc))

        for node_id in sorted(self.node_map):
            node = self.node_map[node_id]
            path = snap / f"node{node_id}-metrics.prom"
            try:
                status, body = self._http_request("GET", node.metrics_url, timeout_sec=5)
                self._write_text(path, body)
                self._write_text(path.with_suffix(".status.txt"), str(status))
            except RunnerError as exc:
                self._write_text(path.with_suffix(".error.txt"), str(exc))

    def _collect_failure_logs(self, case_id: str) -> None:
        """Dump full Docker Compose logs and container state on case failure.

        Args:
            case_id: Failed case identifier (used for directory naming).
        """
        failure_dir = self._case_dir(case_id) / "failure-logs"
        failure_dir.mkdir(parents=True, exist_ok=True)
        self._run_shell(
            "docker compose logs --no-color",
            failure_dir / "docker-compose.logs.txt",
            expect_exit=None,
        )
        self._run_shell(
            "docker compose ps -a",
            failure_dir / "docker-compose.ps-a.txt",
            expect_exit=None,
        )

    # -- Cluster lifecycle ---------------------------------------------------

    def _start_cluster(self, label: str) -> None:
        """Bring up the Docker Compose cluster and wait for convergence.

        Tears down any pre-existing cluster first, then runs
        ``docker compose up -d`` (with ``--build`` if ``build_images`` is set),
        and waits up to 180 s for the expected topology.

        Args:
            label: Descriptive label for log files and snapshots.
        """
        self.log(f"[cluster] up ({label})")
        self._run_shell(
            "docker compose down -v --remove-orphans",
            self.system_dir / f"{utc_timestamp()}-{safe_name(label)}-down-before.log",
            expect_exit=None,
        )

        up_cmd = "docker compose up -d --remove-orphans"
        if self.build_images:
            up_cmd += " --build"
        self._run_shell(
            up_cmd,
            self.system_dir / f"{utc_timestamp()}-{safe_name(label)}-up.log",
            expect_exit=0,
        )

        self._wait_for_cluster(
            expected_nodes=self.matrix.cluster.expected_nodes,
            expected_leaders=1,
            timeout_sec=180,
            require_all_voters=True,
        )
        self._collect_snapshot(f"{label}-cluster-started")

    def _stop_cluster(self, label: str) -> None:
        """Tear down the Docker Compose cluster and remove volumes.

        Args:
            label: Descriptive label for the log file.
        """
        self.log(f"[cluster] down ({label})")
        self._run_shell(
            "docker compose down -v --remove-orphans",
            self.system_dir / f"{utc_timestamp()}-{safe_name(label)}-down.log",
            expect_exit=None,
        )

    # -- Step handlers -------------------------------------------------------

    def _execute_http_step(self, step: dict[str, Any], step_log: Path) -> None:
        """Execute an ``http`` step: request, status check, body/JSON assertions.

        Supports ``expect_status``, ``body_contains``, ``json_fields`` (existence
        check), and ``capture_json`` (store a JSON path value into context).

        Args:
            step: Step dict from the matrix.
            step_log: File to write the request/response log.
        """
        method = str(step.get("method", "GET")).upper()
        url = self._render_template(str(step["url"]))
        expect_status = step.get("expect_status", 200)
        if isinstance(expect_status, int):
            expected = [expect_status]
        else:
            expected = [int(item) for item in expect_status]

        auth_headers = (
            {"x-pgbattery-token": self.mgmt_token} if method == "POST" and self.mgmt_token else {}
        )
        status, body = self._http_request(
            method, url, timeout_sec=int(step.get("timeout_sec", 10)), headers=auth_headers
        )
        log_payload = {
            "method": method,
            "url": url,
            "status": status,
            "expected_status": expected,
            "body": body,
        }
        self._write_text(step_log, json.dumps(log_payload, indent=2))

        if status not in expected:
            raise RunnerError(f"HTTP {method} {url} returned {status}, expected {expected}")

        if "body_contains" in step:
            needles = step["body_contains"]
            if isinstance(needles, str):
                needles = [needles]
            for needle in needles:
                if needle not in body:
                    raise RunnerError(f"HTTP {method} {url} body missing '{needle}'")

        json_fields = step.get("json_fields", [])
        capture_json = step.get("capture_json", {})
        if json_fields or capture_json:
            parsed = self._parse_json(body, f"{method} {url}")
            for json_field in json_fields:
                _ = get_json_path(parsed, str(json_field))
            for variable, json_field in capture_json.items():
                self.context[str(variable)] = get_json_path(parsed, str(json_field))

    def _execute_transfer_leadership(self, step: dict[str, Any], step_log: Path) -> None:
        """Execute a ``transfer_leadership`` step.

        POSTs to ``/api/v1/cluster/transfer-leadership/{target}`` on the
        current leader's management API.  No-ops if the leader is already the
        target.

        Args:
            step: Step dict containing ``target_node_id`` and optional
                ``timeout_sec``.
            step_log: File to write the request/response log.

        Raises:
            RunnerError: If no leader exists, the API returns an error, or the
                response indicates failure.
        """
        target_node_id = int(step["target_node_id"])
        leader_id = self._get_leader_id()
        if leader_id is None:
            raise RunnerError("Cannot transfer leadership when no leader is elected.")
        if leader_id == target_node_id:
            self._write_text(step_log, f"Leader already on target node {target_node_id}; no-op.")
            return
        if leader_id not in self.node_map:
            raise RunnerError(f"Current leader {leader_id} not present in cluster node map.")

        url = (
            f"{self.node_map[leader_id].mgmt_url}"
            f"/api/v1/cluster/transfer-leadership/{target_node_id}"
        )
        status, body = self._http_request(
            "POST",
            url,
            timeout_sec=int(step.get("timeout_sec", 15)),
            headers={"x-pgbattery-token": self.mgmt_token} if self.mgmt_token else {},
        )
        self._write_text(
            step_log,
            json.dumps(
                {
                    "url": url,
                    "status": status,
                    "body": body,
                    "leader_before": leader_id,
                },
                indent=2,
            ),
        )
        if status != 200:
            raise RunnerError(f"Leadership transfer request failed with status {status}: {body}")

        # Leadership transfer is async — the API initiates the request and may
        # return success=false with "attempted" before the new leader is confirmed.
        # The subsequent wait_cluster step is the authoritative verification.
        self._parse_json(body, url)  # validate it is parseable JSON

    def _execute_pgbench_step(self, step: dict[str, Any], step_log: Path) -> None:
        """Execute a ``pgbench`` step: initialise schema and run a TPS benchmark.

        Runs ``pgbench -i`` to create the standard pgbench tables, then runs
        the default read-write workload for ``duration_sec`` seconds.  Parses
        the ``tps = X`` line from pgbench output and asserts it is at least
        ``min_tps``.

        Args:
            step: Step dict with optional ``node`` (default 1), ``scale``
                (default 1), ``clients`` (default 4), ``threads`` (default 2),
                ``duration_sec`` (default 10), ``min_tps`` (default 100.0),
                ``capture_tps`` (optional context variable name).
            step_log: File to write pgbench stdout/stderr and measured TPS.

        Raises:
            RunnerError: If the node is unknown, pgbench fails to run, or the
                measured TPS is below ``min_tps``.
        """
        import re as _re

        node_id = int(step.get("node", 1))
        node = self.node_map.get(node_id)
        if not node:
            raise RunnerError(f"Unknown node {node_id} for pgbench step.")

        scale = int(step.get("scale", 1))
        clients = int(step.get("clients", 4))
        threads = int(step.get("threads", 2))
        duration_sec = int(step.get("duration_sec", 10))
        min_tps = float(step.get("min_tps", 100.0))
        pg_bin = "/usr/lib/postgresql/18/bin/pgbench"
        pg_conn = "-U postgres -h localhost -p 5434 -d postgres"

        # Initialise pgbench schema (idempotent: -i drops and recreates tables).
        init_cmd = f"docker compose exec -T {node.name} {pg_bin} -i -s {scale} {pg_conn}"
        self._run_shell(init_cmd, step_log)

        # Run benchmark.
        bench_cmd = (
            f"docker compose exec -T {node.name} "
            f"{pg_bin} -c {clients} -j {threads} -T {duration_sec} {pg_conn}"
        )
        result = subprocess.run(
            bench_cmd,
            shell=True,
            cwd=self.project_root,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=duration_sec + 30,
        )

        log_text = [
            f"$ {bench_cmd}",
            f"exit_code: {result.returncode}",
            "--- stdout ---",
            result.stdout,
            "--- stderr ---",
            result.stderr,
        ]
        self._write_text(step_log, "\n".join(log_text))

        if result.returncode != 0:
            raise RunnerError(f"pgbench failed with exit code {result.returncode}")

        # pgbench prints two TPS lines; use the one excluding connection overhead.
        # e.g. "tps = 1234.567890 (without initial connection time)"
        matches = _re.findall(
            r"tps\s*=\s*([\d.]+)\s*\(without initial connection time\)",
            result.stdout + result.stderr,
        )
        if not matches:
            raise RunnerError("Could not parse TPS from pgbench output")

        measured_tps = float(matches[-1])
        self.log(f"    pgbench TPS={measured_tps:.0f} (min={min_tps:.0f})")
        self._write_text(step_log, f"\nmeasured_tps={measured_tps:.2f}\nmin_tps={min_tps:.2f}")

        if "capture_tps" in step:
            self.context[str(step["capture_tps"])] = str(measured_tps)

        if measured_tps < min_tps:
            raise RunnerError(f"pgbench TPS {measured_tps:.0f} below minimum {min_tps:.0f}")

    def _execute_sql_step(self, step: dict[str, Any], step_log: Path) -> None:
        """Execute a ``sql`` step: pipe a ``.sql`` file through psql via stdin.

        Reads the file from ``testing/sql/{file}``, sends it as stdin to
        ``psql`` inside the target container.  This avoids all shell escaping
        issues that plague inline SQL in ``cmd`` steps.

        Args:
            step: Step dict with required ``file`` and optional ``node``
                (default 1), ``direct`` (connect to internal port 5434 instead
                of gateway 5432), ``on_error_stop`` (default ``True``), and
                ``expect_exit`` (default 0).
            step_log: File to write the SQL content, stdout, stderr, and exit
                code.

        Raises:
            RunnerError: If the SQL file is missing, the node is unknown, or
                the exit code is unexpected.
        """
        sql_file = self.project_root / "testing" / "sql" / str(step["file"])
        if not sql_file.exists():
            raise RunnerError(f"SQL file not found: {sql_file}")
        sql_content = sql_file.read_text(encoding="utf-8")

        raw_node = step.get("node", 1)
        if raw_node == "leader":
            leader_id = self._get_leader_id()
            if not leader_id:
                raise RunnerError("Cannot run SQL on leader: no leader elected")
            node_id = leader_id
        else:
            node_id = int(raw_node)
        node = self.node_map.get(node_id)
        if not node:
            raise RunnerError(f"Unknown node {node_id} for sql step.")

        port = 5434 if step.get("direct") else 5432
        on_error_stop = "1" if step.get("on_error_stop", True) else "0"
        expect_exit = step.get("expect_exit", 0)

        cmd = (
            f"docker compose exec -T {node.name} "
            f"psql -U postgres -h localhost -p {port} -d postgres "
            f"-v ON_ERROR_STOP={on_error_stop}"
        )
        proc = subprocess.run(
            cmd,
            shell=True,
            input=sql_content,
            cwd=self.project_root,
            env=self.env,
            capture_output=True,
            text=True,
        )

        log_text = [
            f"$ {cmd} < {sql_file.relative_to(self.project_root)}",
            f"exit_code: {proc.returncode}",
            "",
            "--- sql ---",
            sql_content,
            "--- stdout ---",
            proc.stdout,
            "--- stderr ---",
            proc.stderr,
        ]
        self._write_text(step_log, "\n".join(log_text))

        expected = [expect_exit] if isinstance(expect_exit, int) else [int(e) for e in expect_exit]

        if proc.returncode not in expected:
            raise RunnerError(
                f"SQL file {step['file']} failed with exit code {proc.returncode}, "
                f"expected {expected}"
            )

    # -- Step dispatcher -----------------------------------------------------

    def _execute_step(
        self,
        step: dict[str, Any],
        case_id: str,
        phase: str,
        index: int,
    ) -> None:
        """Dispatch a single step to the appropriate handler.

        Args:
            step: Step dict from the matrix (must contain ``type``).
            case_id: Owning case ID (for logging and artifact paths).
            phase: One of ``action``, ``assert``, or ``cleanup``.
            index: Zero-based step index within the phase.

        Raises:
            RunnerError: If the step type is missing or unknown, or if the
                handler raises.
        """
        raw_type = str(step.get("type", "")).strip()
        if not raw_type:
            raise RunnerError(f"{case_id} {phase} step #{index} is missing 'type'.")

        try:
            step_type = StepType(raw_type)
        except ValueError as exc:
            raise RunnerError(f"Unsupported step type '{raw_type}'.") from exc

        case_dir = self._case_dir(case_id)
        step_log = case_dir / f"{phase}-{index:02d}-{safe_name(step_type)}.log"
        self.log(f"  [{phase}:{index:02d}] {step_type}")

        match step_type:
            case StepType.CMD:
                command = str(step["cmd"])
                expect_exit = step.get("expect_exit", 0)
                result = self._run_shell(command, step_log, expect_exit=expect_exit)

                stdout_contains = step.get("stdout_contains")
                stderr_contains = step.get("stderr_contains")

                if stdout_contains:
                    needles = (
                        [stdout_contains] if isinstance(stdout_contains, str) else stdout_contains
                    )
                    for needle in needles:
                        if str(needle) not in result.stdout:
                            raise RunnerError(f"stdout missing expected token '{needle}'")
                if stderr_contains:
                    needles = (
                        [stderr_contains] if isinstance(stderr_contains, str) else stderr_contains
                    )
                    for needle in needles:
                        if str(needle) not in result.stderr:
                            raise RunnerError(f"stderr missing expected token '{needle}'")

                if "capture_stdout" in step:
                    self.context[str(step["capture_stdout"])] = result.stdout.strip()
                if "capture_stderr" in step:
                    self.context[str(step["capture_stderr"])] = result.stderr.strip()

            case StepType.SLEEP:
                seconds = int(step["seconds"])
                self._write_text(step_log, f"sleep {seconds}s")
                time.sleep(seconds)

            case StepType.WAIT_CLUSTER:
                expected_nodes = int(step.get("nodes", self.matrix.cluster.expected_nodes))
                expected_leaders = int(step.get("leaders", 1))
                timeout_sec = int(step.get("timeout_sec", 120))
                leader_not = None
                if "leader_not_var" in step:
                    var_name = str(step["leader_not_var"])
                    raw = self.context.get(var_name)
                    if raw is not None:
                        leader_not = int(raw)
                leader_equals = int(step["leader_equals"]) if "leader_equals" in step else None
                started = time.time()
                # Default: require full replication health when waiting for
                # a complete 3-node/1-leader topology.  Tests that intentionally
                # wait for a degraded state (nodes<3) skip this automatically.
                full_topology = (
                    expected_nodes == self.matrix.cluster.expected_nodes and expected_leaders == 1
                )
                require_repl = bool(
                    step.get(
                        "require_replication_health",
                        full_topology,
                    )
                )
                # Default `min_healthy_replicas=1` matches historical
                # behaviour. End-of-case waits that need to leave the
                # cluster fully replicated for the next case (especially
                # after restart / partition heal) should explicitly pass
                # `min_healthy_replicas: 2` so the next test doesn't
                # transfer leadership to a not-yet-replicated follower.
                min_healthy = int(step.get("min_healthy_replicas", 1))
                live_nodes = int(step["live_nodes"]) if "live_nodes" in step else None
                self._wait_for_cluster(
                    expected_nodes=expected_nodes,
                    expected_leaders=expected_leaders,
                    timeout_sec=timeout_sec,
                    leader_not=leader_not,
                    leader_equals=leader_equals,
                    require_replication_health=require_repl,
                    min_healthy_replicas=min_healthy,
                    live_nodes=live_nodes,
                )
                elapsed = time.time() - started
                max_converge_sec = step.get("max_converge_sec")
                if max_converge_sec is None:
                    max_converge_sec = self.suite_config.max_wait_cluster_seconds
                if max_converge_sec is not None and elapsed > float(max_converge_sec):
                    raise RunnerError(
                        f"Cluster convergence exceeded budget: {elapsed:.1f}s > {max_converge_sec}s"
                    )
                self._write_text(
                    step_log,
                    "cluster converged: "
                    f"nodes={expected_nodes}, "
                    f"leaders={expected_leaders}, "
                    f"elapsed_sec={elapsed:.1f}, "
                    f"timeout_sec={timeout_sec}, "
                    f"budget_sec={max_converge_sec}",
                )

            case StepType.RECORD_LEADER:
                variable = str(step["var"])
                self.context[variable] = self._get_leader_id()
                self._write_text(step_log, f"{variable}={self.context[variable]}")

            case StepType.CLUSTER_TOPOLOGY:
                expected_nodes = int(step["nodes"])
                expected_leaders = int(step["leaders"])
                nodes = self._get_cluster_nodes()
                node_count = len(nodes)
                leader_count = sum(1 for node in nodes if node.is_leader)
                self._write_text(
                    step_log,
                    json.dumps(
                        {
                            "observed_nodes": node_count,
                            "observed_leaders": leader_count,
                            "expected_nodes": expected_nodes,
                            "expected_leaders": expected_leaders,
                            "nodes": [node.model_dump(mode="json") for node in nodes],
                        },
                        indent=2,
                    ),
                )
                if node_count != expected_nodes or leader_count != expected_leaders:
                    raise RunnerError(
                        "Unexpected cluster topology: "
                        f"nodes={node_count}/{expected_nodes}, "
                        f"leaders={leader_count}/{expected_leaders}"
                    )

            case StepType.LEADER_NOT:
                variable = str(step["var"])
                if variable not in self.context:
                    raise RunnerError(f"leader_not references undefined variable '{variable}'.")
                previous = self.context[variable]
                current = self._get_leader_id()
                self._write_text(
                    step_log,
                    json.dumps(
                        {"previous_leader": previous, "current_leader": current},
                        indent=2,
                    ),
                )
                if current == previous:
                    raise RunnerError(f"Leader did not change (still {current}).")

            case StepType.LEADER_EQUALS:
                expected_leader = int(step["value"])
                current = self._get_leader_id()
                self._write_text(
                    step_log,
                    json.dumps(
                        {"expected_leader": expected_leader, "current_leader": current},
                        indent=2,
                    ),
                )
                if current != expected_leader:
                    raise RunnerError(
                        f"Leader mismatch: expected {expected_leader}, got {current}."
                    )

            case StepType.LEADER_EQUALS_VAR:
                variable = str(step["var"])
                if variable not in self.context:
                    raise RunnerError(
                        f"leader_equals_var references undefined variable '{variable}'."
                    )
                expected_leader = self.context[variable]
                current = self._get_leader_id()
                self._write_text(
                    step_log,
                    json.dumps(
                        {"expected_leader": expected_leader, "current_leader": current},
                        indent=2,
                    ),
                )
                if current != expected_leader:
                    raise RunnerError(
                        f"Leader mismatch: expected {expected_leader}, got {current}."
                    )

            case StepType.METRIC_EXISTS:
                node_id = self._resolve_node_ref(step["node_id"])
                metric = str(step["metric"])
                values = self._poll_metric_values(node_id=node_id, metric_name=metric)
                self._write_text(
                    step_log,
                    json.dumps({"metric": metric, "node_id": node_id, "values": values}, indent=2),
                )

            case StepType.METRIC_EQUALS:
                node_id = self._resolve_node_ref(step["node_id"])
                metric = str(step["metric"])
                expected_val = float(step["value"])
                tolerance = float(step.get("tolerance", 0.0001))
                values = self._poll_metric_values(node_id=node_id, metric_name=metric)
                actual = values[0]
                self._write_text(
                    step_log,
                    json.dumps(
                        {
                            "metric": metric,
                            "values": values,
                            "expected": expected_val,
                            "tolerance": tolerance,
                        },
                        indent=2,
                    ),
                )
                if abs(actual - expected_val) > tolerance:
                    raise RunnerError(
                        f"Metric '{metric}' on node {node_id} "
                        f"expected {expected_val}, got {actual}."
                    )

            case StepType.METRIC_LEADER_COUNT:
                expected_count = int(step["expected"])
                leader_metric = "pgbattery_raft_is_leader"
                observed: dict[int, float] = {}
                count = 0
                for node_id in sorted(self.node_map):
                    values = self._poll_metric_values(node_id=node_id, metric_name=leader_metric)
                    observed[node_id] = values[0]
                    if values[0] > 0.5:
                        count += 1
                self._write_text(
                    step_log,
                    json.dumps(
                        {
                            "expected_leader_count": expected_count,
                            "observed_leader_values": observed,
                        },
                        indent=2,
                    ),
                )
                if count != expected_count:
                    raise RunnerError(
                        f"Expected {expected_count} leader metric=1 nodes, got {count}."
                    )

            case StepType.WAIT_METRIC:
                node_id = int(step["node_id"])
                metric = str(step["metric"])
                timeout_sec = int(step.get("timeout_sec", 30))
                deadline = time.time() + timeout_sec
                while True:
                    values = self._fetch_metric_values(node_id=node_id, metric_name=metric)
                    if values:
                        self._write_text(
                            step_log,
                            json.dumps({"metric": metric, "values": values}, indent=2),
                        )
                        break
                    if time.time() >= deadline:
                        raise RunnerError(
                            f"Timed out waiting for metric '{metric}' on node {node_id} "
                            f"after {timeout_sec}s."
                        )
                    time.sleep(1)

            case StepType.HTTP:
                self._execute_http_step(step, step_log)

            case StepType.TRANSFER_LEADERSHIP:
                self._execute_transfer_leadership(step, step_log)

            case StepType.BASENAME:
                source_var = str(step["source_var"])
                target_var = str(step["var"])
                if source_var not in self.context:
                    raise RunnerError(
                        f"basename source_var '{source_var}' is not defined in context."
                    )
                self.context[target_var] = Path(str(self.context[source_var])).name
                self._write_text(
                    step_log,
                    f"{target_var}={self.context[target_var]} (from {source_var})",
                )

            case StepType.SQL:
                self._execute_sql_step(step, step_log)

            case StepType.ASYMMETRIC_PARTITION:
                node_id = int(step["node"])
                from_id = int(step["from_node"])
                node_name = f"node{node_id}"
                src_ip = _NODE_IPS.get(from_id)
                if not src_ip:
                    raise RunnerError(f"Unknown from_node {from_id} for asymmetric_partition.")
                cmd = f"docker compose exec -T {node_name} iptables -A INPUT -s {src_ip} -j DROP"
                self._run_shell(cmd, step_log)

            case StepType.ASYMMETRIC_HEAL:
                node_id = int(step["node"])
                from_id = int(step["from_node"])
                node_name = f"node{node_id}"
                src_ip = _NODE_IPS.get(from_id)
                if not src_ip:
                    raise RunnerError(f"Unknown from_node {from_id} for asymmetric_heal.")
                cmd = f"docker compose exec -T {node_name} iptables -D INPUT -s {src_ip} -j DROP"
                self._run_shell(cmd, step_log)

            case StepType.CLOCK_SKEW:
                node_id = int(step["node"])
                offset_seconds = int(step.get("seconds", 300))
                node_name = f"node{node_id}"
                cmd = (
                    f"docker compose exec -T {node_name} "
                    f"sh -c \"echo '+{offset_seconds}s' > /tmp/faketime\""
                )
                self._run_shell(cmd, step_log)

            case StepType.CLOCK_HEAL:
                node_id = int(step["node"])
                node_name = f"node{node_id}"
                cmd = f"docker compose exec -T {node_name} sh -c \"echo '+0s' > /tmp/faketime\""
                self._run_shell(cmd, step_log)

            case StepType.WAIT_SYNC:
                check_nodes = step.get("nodes")
                if check_nodes is None:
                    check_nodes = list(self.node_map.keys())
                elif isinstance(check_nodes, int):
                    check_nodes = [int(check_nodes)]
                else:
                    check_nodes = [int(n) for n in check_nodes]
                timeout_sec = int(step.get("timeout_sec", 60))
                try:
                    leader_id = self._get_leader_id()
                except RunnerError:
                    leader_id = None
                follower_ids = [nid for nid in check_nodes if nid != leader_id]
                deadline = time.time() + timeout_sec
                last_status: dict[int, Any] = {}
                while True:
                    all_synced = True
                    for nid in follower_ids:
                        node = self.node_map.get(nid)
                        if not node:
                            raise RunnerError(f"Unknown node_id {nid} for wait_sync.")
                        url = f"{node.mgmt_url}/api/v1/cluster/node/{nid}/lag"
                        try:
                            http_status, body = self._http_request("GET", url, timeout_sec=5)
                            if http_status == 200:
                                parsed = self._parse_json(body, url)
                                lag = int(parsed.get("lag_bytes", 999999))
                                is_synced = bool(parsed.get("is_synced", False))
                                last_status[nid] = {"lag_bytes": lag, "is_synced": is_synced}
                                if not is_synced or lag > 0:
                                    all_synced = False
                            else:
                                all_synced = False
                                last_status[nid] = f"HTTP {http_status}"
                        except RunnerError as exc:
                            all_synced = False
                            last_status[nid] = str(exc)
                    if all_synced:
                        self._write_text(step_log, json.dumps(last_status, indent=2))
                        break
                    if time.time() >= deadline:
                        raise RunnerError(
                            f"Timed out waiting for replication sync on {follower_ids} "
                            f"after {timeout_sec}s: {last_status}"
                        )
                    time.sleep(1)

            case StepType.NETWORK_DELAY:
                node_id = int(step["node"])
                delay_ms = int(step.get("delay_ms", 200))
                jitter_ms = int(step.get("jitter_ms", 50))
                node_name = f"node{node_id}"
                inner = (
                    f"tc qdisc del dev eth0 root 2>/dev/null; "
                    f"tc qdisc add dev eth0 root netem delay {delay_ms}ms {jitter_ms}ms"
                )
                cmd = f'docker compose exec -T {node_name} sh -c "{inner}"'
                self._run_shell(cmd, step_log)

            case StepType.NETWORK_HEAL:
                node_id = int(step["node"])
                node_name = f"node{node_id}"
                inner = "tc qdisc del dev eth0 root 2>/dev/null; true"
                cmd = f'docker compose exec -T {node_name} sh -c "{inner}"'
                self._run_shell(cmd, step_log)

            case StepType.PGBENCH:
                self._execute_pgbench_step(step, step_log)

    # -- Phase & case execution ----------------------------------------------

    def _execute_step_list(
        self,
        case_id: str,
        phase: str,
        steps: list[dict[str, Any]],
        continue_on_error: bool = False,
    ) -> None:
        """Execute a list of steps sequentially.

        Args:
            case_id: Owning case ID.
            phase: Phase label (``action``, ``assert``, or ``cleanup``).
            steps: Ordered list of step dicts.
            continue_on_error: If ``True``, collect all failures and raise a
                combined error at the end instead of failing fast.

        Raises:
            RunnerError: On step failure (immediately if ``continue_on_error``
                is ``False``, or aggregated at the end).
        """
        failures: list[str] = []
        for index, step in enumerate(steps):
            try:
                self._execute_step(step, case_id, phase, index)
            except Exception as exc:
                if continue_on_error:
                    failures.append(f"{phase} step {index}: {exc}")
                    continue
                raise
        if failures:
            joined = "\n".join(failures)
            raise RunnerError(f"{phase} encountered failures:\n{joined}")

    def _run_case(self, case_id: str) -> bool:
        """Execute a single test case: actions → assertions → cleanup.

        Collects snapshots before, after, and on failure.  Cleanup runs in the
        ``finally`` block so it executes regardless of action/assertion outcome.

        Args:
            case_id: Case identifier from the matrix.

        Returns:
            ``True`` if the case passed, ``False`` otherwise.
        """
        case = self.case_map[case_id]
        self.context = {}
        self.log(f"[case] {case_id}: {case.description}")
        case_dir = self._case_dir(case_id)
        self._collect_snapshot(f"{case_id}-before")
        started = time.time()

        try:
            self._execute_step_list(case_id, "action", case.actions)
            self._execute_step_list(case_id, "assert", case.assertions)
            elapsed = time.time() - started
            self._write_text(case_dir / "result.txt", f"PASS ({elapsed:.1f}s)\n")
            self.log(f"[pass] {case_id} ({elapsed:.1f}s)")
            self.summary.append(CaseSummary(case_id=case_id, passed=True, detail=f"{elapsed:.1f}s"))
            self._collect_snapshot(f"{case_id}-after")
            return True
        except Exception as exc:
            elapsed = time.time() - started
            self.failed = True
            error_text = (
                f"FAIL ({elapsed:.1f}s)\n\n{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}"
            )
            self._write_text(case_dir / "result.txt", error_text)
            self._collect_snapshot(f"{case_id}-failed")
            self._collect_failure_logs(case_id)
            self.log(f"[fail] {case_id}: {exc}")
            self.summary.append(CaseSummary(case_id=case_id, passed=False, detail=str(exc)))
            return False
        finally:
            if case.cleanup:
                try:
                    self._execute_step_list(
                        case_id=case_id,
                        phase="cleanup",
                        steps=case.cleanup,
                        continue_on_error=False,
                    )
                except Exception as cleanup_exc:
                    self.failed = True
                    self.log(f"[warn] cleanup failed for {case_id}: {cleanup_exc}")
                    self._write_text(
                        case_dir / "cleanup-error.txt",
                        f"{cleanup_exc}\n\n{traceback.format_exc()}",
                    )

    # -- Summary & top-level execution ---------------------------------------

    def _print_summary(self) -> None:
        """Print a Rich table summarising pass/fail status for all cases."""
        self.log("")
        table = Table(title="Scenario Summary", show_lines=False)
        table.add_column("Case")
        table.add_column("Status")
        table.add_column("Detail")
        for entry in self.summary:
            status = "[green]PASS[/]" if entry.passed else "[red]FAIL[/]"
            table.add_row(entry.case_id, status, entry.detail)
        self.console.print(table)

    def run(self) -> int:
        """Execute the selected suite and return an exit code.

        Handles both ``reuse_cluster`` (single cluster, sequential cases with
        early abort on failure) and per-case cluster lifecycle modes.

        Returns:
            0 if all cases passed, 1 if any failed.
        """
        reuse_cluster = self.suite_config.reuse_cluster

        if reuse_cluster:
            cluster_started = False
            try:
                self._start_cluster(self.suite_name)
                cluster_started = True
                for case_id in self.selected_case_ids:
                    if not self._run_case(case_id):
                        break
            finally:
                if cluster_started:
                    if self.failed and self.keep_cluster_on_failure:
                        self.log(
                            "[cluster] preserving cluster for debugging (--keep-cluster-on-failure)"
                        )
                    else:
                        self._stop_cluster(self.suite_name)
        else:
            for case_id in self.selected_case_ids:
                cluster_started = False
                try:
                    self._start_cluster(case_id)
                    cluster_started = True
                    self._run_case(case_id)
                finally:
                    if cluster_started:
                        if self.failed and self.keep_cluster_on_failure:
                            self.log(
                                "[cluster] preserving cluster for debugging "
                                "(--keep-cluster-on-failure)"
                            )
                        else:
                            self._stop_cluster(case_id)

        self._print_summary()
        self._save_run_summary()
        return 1 if self.failed else 0

    def _save_run_summary(self) -> None:
        """Write run_summary.json to the artifact directory.

        Records suite name, cases executed, pass/fail status, and the full
        case definitions so the exact run can be replayed or analysed offline.
        """
        import datetime

        summary_data = {
            "suite": self.suite_name,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "passed": not self.failed,
            "cases": [
                {"case_id": s.case_id, "passed": s.passed, "detail": s.detail} for s in self.summary
            ],
            "case_definitions": [
                self.case_map[s.case_id].model_dump(mode="json")
                for s in self.summary
                if s.case_id in self.case_map
            ],
        }
        path = self.artifact_dir / "run_summary.json"
        self._write_text(path, json.dumps(summary_data, indent=2))


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Run deterministic pgbattery HA scenarios from testing/ci_matrix.yaml.",
)
console = Console()


@app.command()
def run(
    suite: str = typer.Option(
        ...,
        "--suite",
        help="Suite name from testing/ci_matrix.yaml (e.g. ha-sequential, ha-parallel).",
    ),
    case: str | None = typer.Option(
        None,
        "--case",
        help="Optional single case id to run (must belong to --suite).",
    ),
    matrix: str = typer.Option(
        "testing/ci_matrix.yaml",
        "--matrix",
        help="Path to scenario matrix file.",
    ),
    artifact_dir: str = typer.Option(
        f"testing/artifacts/{utc_timestamp()}",
        "--artifact-dir",
        help="Directory to write logs, snapshots, and command output.",
    ),
    no_build: bool = typer.Option(
        False,
        "--no-build",
        help="Skip docker compose --build when bringing up the cluster.",
    ),
    keep_cluster_on_failure: bool = typer.Option(
        False,
        "--keep-cluster-on-failure",
        help="Do not tear down docker compose when a failure occurs (debug only).",
    ),
) -> None:
    """Run a scenario suite against a Docker Compose pgbattery cluster."""
    project_root = Path(__file__).resolve().parent.parent
    matrix_path = (project_root / matrix).resolve()
    resolved_artifact_dir = (project_root / artifact_dir).resolve()

    try:
        runner = CIRunner(
            project_root=project_root,
            matrix_path=matrix_path,
            suite=suite,
            case_filter=case,
            artifact_dir=resolved_artifact_dir,
            build_images=not no_build,
            keep_cluster_on_failure=keep_cluster_on_failure,
            console=console,
        )
        code = runner.run()
    except RunnerError as exc:
        console.print(f"[red]Runner error:[/] {exc}")
        raise typer.Exit(code=1) from exc

    raise typer.Exit(code=code)


if __name__ == "__main__":
    app()
