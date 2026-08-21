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
import shlex
import subprocess
import time
import traceback
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import typer
from dotenv import load_dotenv
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationError,
    field_validator,
    model_validator,
)
from rich.console import Console
from rich.table import Table

import fault_primitives as fp

# ---------------------------------------------------------------------------
# Step types
# ---------------------------------------------------------------------------


class StepType(StrEnum):
    """Discriminator for steps in the scenario matrix.

    Each value maps 1-to-1 to a handler in ``CIRunner._execute_step``.

    Every step that spawns a shell command is bounded by
    ``DEFAULT_SHELL_TIMEOUT_SEC``.  Individual steps override that budget with
    ``shell_timeout_sec`` (``timeout_sec`` is accepted as an alias on steps that
    do not already use it for polling).  Exceeding the budget kills the command
    and fails the step.

    Fault-injecting steps verify that the fault actually landed before the step
    is allowed to succeed; see :meth:`CIRunner._verify_fault_intent` and the
    ``_verify_*`` helpers.

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
        CHANNEL_PARTITION: Sever ONE protocol port between a node and a peer,
            leaving the others up — Raft without replication, or replication
            without Raft. Whole-peer partitions cannot express this, and it is
            the shape RW-6 and RW-11 live in. Installs both ``--dport`` and
            ``--sport``: only one side of a TCP channel listens on the service
            port, so matching one direction alone silently catches nothing.
        CHANNEL_HEAL: Remove the rules added by ``CHANNEL_PARTITION`` and
            assert none survives.
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
    CHANNEL_PARTITION = "channel_partition"
    CHANNEL_HEAL = "channel_heal"
    CLOCK_SKEW = "clock_skew"
    CLOCK_HEAL = "clock_heal"
    WAIT_SYNC = "wait_sync"
    NETWORK_DELAY = "network_delay"
    NETWORK_HEAL = "network_heal"
    PGBENCH = "pgbench"


# Static IP addresses for each node on the raft_net bridge network, keyed by
# node id. Re-keyed from the primitive layer rather than restated: these are
# declared once in docker-compose.yml, and two copies drift.
# `witness` is skipped: the matrix addresses nodes by Raft id, and the witness
# has no id to key on.
_NODE_IPS: Final[dict[int, str]] = {
    int(service.removeprefix("node")): ip
    for service, ip in fp.NODE_IPS.items()
    if service.removeprefix("node").isdigit()
}

# The raft_net subnet prefix; a container attached to raft_net always holds an
# address in it, so its presence/absence is the observable effect of
# ``docker network connect`` / ``docker network disconnect``.
_RAFT_SUBNET_PREFIX: Final[str] = fp.CLUSTER_SUBNET_PREFIX


# ---------------------------------------------------------------------------
# Timeout budgets
# ---------------------------------------------------------------------------

# Every shell command the runner spawns is bounded.  Without this a single hung
# `docker compose exec` (e.g. into a container that a previous step paused)
# blocks the whole suite until the CI job-level timeout kills it, losing all
# artifacts.
DEFAULT_SHELL_TIMEOUT_SEC: Final[int] = 600
# `docker compose up --build` compiles the Rust binary on a cold cargo cache.
CLUSTER_LIFECYCLE_TIMEOUT_SEC: Final[int] = 1800
# Snapshots and log dumps are best-effort diagnostics: keep them on a short
# leash so a wedged docker daemon cannot consume the whole job budget.
DIAGNOSTIC_TIMEOUT_SEC: Final[int] = 120
# Fault-effect probes are single short `docker inspect` / `docker exec` calls.
FAULT_PROBE_TIMEOUT_SEC: Final[int] = 60
# How long a writing SQL step waits for its path to take writes. Long enough to
# cover a promotion plus the lease tick's write recovery, short enough that a
# node which is never going to accept writes says so rather than eating the
# case's own timeout.
SQL_WRITABLE_TIMEOUT_SEC: Final[int] = 60

# Lines a single node may log across one cluster's lifetime before the run is
# called a hot loop.
#
# A node that cannot reach a peer used to retry the connection with no backoff,
# and one CI run collected 2.4 million lines for four minutes of cluster — a
# burnt core and a starved data plane on a two-vCPU runner, invisible to every
# layer because the cases it broke failed for other-looking reasons. Healthy
# runs sit three orders of magnitude below this; see `_check_log_budget`.
LOG_LINES_PER_SERVICE_BUDGET: Final[int] = 250_000

# Statement-leading verbs that need a path accepting writes. Matched at the
# start of a line so a verb inside a string or a comment does not count; the
# cost of a false positive is one wait that returns immediately, and of a false
# negative the flake this exists to remove.
_SQL_WRITE_VERB: Final[re.Pattern[str]] = re.compile(
    r"^\s*(insert|update|delete|truncate|create|drop|alter|grant|revoke|do)\b",
    re.IGNORECASE | re.MULTILINE,
)
# Per-stream character budget for the partial output reported on a timeout.
TIMEOUT_OUTPUT_CHARS: Final[int] = 4000
# How long to wait for the pipes of a killed command to drain.
TIMEOUT_DRAIN_SEC: Final[int] = 5
# Slack allowed between the clock shift a libfaketime offset should produce and
# the shift actually observed across two `docker exec` round-trips.
FAKETIME_SHIFT_TOLERANCE_SEC: Final[int] = 10
# Step keys that override the shell timeout, in precedence order.
SHELL_TIMEOUT_KEYS: Final[tuple[str, ...]] = ("shell_timeout_sec", "timeout_sec")

# User that privileged in-container operations exec as.  See
# :func:`container_exec_prefix` for why the image's default user cannot be used.
PRIVILEGED_EXEC_USER: Final[str] = "root"

# Shape of a correctness contract ID as defined by the ``### <ID> — <title>``
# headings in ``docs/CONTRACTS.md`` (W1, W2, L3, R1, V2, S1, ...).
CONTRACT_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Z]{1,3}[0-9]{1,2}")


def sql_step_needs_a_writable_path(step: dict[str, Any], sql_content: str) -> bool:
    """Whether this SQL step should wait for a path that accepts writes.

    Three things have to hold. It has to go through a gateway — a `direct` step
    addresses one node's own PostgreSQL, which is legitimately a read-only
    standby. It has to expect success — `stale-leader-fencing` and
    `majority-loss` assert a write is *refused*, and waiting for writability
    there would wait out the clock on the state under test. And it has to
    actually write, so an assertion that only reads is not held up by a cluster
    that is deliberately fenced.
    """
    if step.get("direct"):
        return False
    expect = step.get("expect_exit", 0)
    if expect != 0:
        return False
    return bool(_SQL_WRITE_VERB.search(sql_content))


def captured_text(captured: Any) -> str | None:
    """What a killed command had written, as text.

    `TimeoutExpired` carries `str` or `bytes` depending on how the command was
    run and types both as `Any`. Decoding here keeps a bytes capture in the
    failure message instead of dropping it, which is the only place that
    output survives at all.
    """
    match captured:
        case None:
            return None
        case bytes():
            return captured.decode(errors="replace")
        case _:
            return str(captured)


def as_strings(value: Any) -> list[str]:
    """A matrix field that accepts one string or a list of them, as a list.

    Both spellings appear across the matrix (`body_contains`, `stdout_contains`,
    ...). Normalised once here so no step handler has to ask which it got, and
    so a bare string is never iterated character by character.
    """
    match value:
        case None:
            return []
        case str():
            return [value]
        case _:
            return [str(item) for item in value]


def as_ints(value: Any) -> list[int]:
    """A matrix field that accepts one integer or a list of them, as a list.

    `expect_exit`, `expect_status` and `nodes` are all written both ways.
    """
    match value:
        case None:
            return []
        case int():
            return [value]
        case _:
            return [int(item) for item in value]


def validate_timeout_value(raw: Any, key: str) -> int:
    """Coerce a matrix-supplied timeout to a positive whole number of seconds.

    Args:
        raw: Raw value from the step dict.
        key: Step key the value came from (used in the error message).

    Returns:
        The timeout in seconds.

    Raises:
        ValueError: If the value is not a positive integer (bools and
            fractional floats are rejected).
    """
    if isinstance(raw, bool):
        raise ValueError(f"'{key}' must be a positive integer, got {raw!r}")
    if isinstance(raw, float):
        if not raw.is_integer():
            raise ValueError(f"'{key}' must be a whole number of seconds, got {raw!r}")
        raw = int(raw)
    if not isinstance(raw, int):
        raise ValueError(f"'{key}' must be a positive integer, got {raw!r}")
    if raw <= 0:
        raise ValueError(f"'{key}' must be > 0, got {raw!r}")
    return raw


def resolve_shell_timeout(
    step: Mapping[str, Any],
    default: int = DEFAULT_SHELL_TIMEOUT_SEC,
) -> int:
    """Return the shell timeout for a step, honouring per-step overrides.

    Args:
        step: Step dict from the matrix.
        default: Budget applied when the step declares no override.

    Returns:
        Timeout in seconds.

    Raises:
        RunnerError: If an override is present but is not a positive integer.
    """
    for key in SHELL_TIMEOUT_KEYS:
        if step.get(key) is not None:
            try:
                return validate_timeout_value(step[key], key)
            except ValueError as exc:
                raise RunnerError(str(exc)) from exc
    return default


def tail_text(text: str | None, limit: int = TIMEOUT_OUTPUT_CHARS) -> str:
    """Return at most ``limit`` trailing characters of ``text``.

    Args:
        text: Captured stream contents (``None`` when the stream was empty).
        limit: Maximum characters to keep.

    Returns:
        The tail of ``text``, prefixed with a truncation marker when clipped,
        or ``"<empty>"`` when there is nothing to show.
    """
    if not text:
        return "<empty>"
    if len(text) <= limit:
        return text
    return f"...<truncated {len(text) - limit} chars>...{text[-limit:]}"


def format_timeout_failure(
    command: str,
    timeout_sec: float,
    stdout: str | None,
    stderr: str | None,
) -> str:
    """Build the loud failure message for a command that exceeded its budget.

    The message is deliberately verbose: a timeout means the cluster (or the
    docker daemon) is wedged, and the partial output is usually the only
    evidence of where it wedged.

    Args:
        command: The command that was killed.
        timeout_sec: Budget it exceeded.
        stdout: Partial stdout captured before the kill, if any.
        stderr: Partial stderr captured before the kill, if any.

    Returns:
        Multi-line message beginning with the ``STEP TIMEOUT`` label.
    """
    return "\n".join(
        [
            f"STEP TIMEOUT: command exceeded {timeout_sec:g}s and was killed",
            f"$ {command}",
            "--- partial stdout ---",
            tail_text(stdout),
            "--- partial stderr ---",
            tail_text(stderr),
        ]
    )


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
        description: Human-readable summary, and the case's own specification —
            what it asserts and why that is the right thing to assert. There is
            no other document to defer to.
        contracts: Correctness contract IDs from ``docs/CONTRACTS.md`` that this
            case exercises (e.g. ``["W1", "R2"]``).  Optional for the runner so
            a case without it still parses and executes; ``lint_matrix.py``
            enforces the "every case declares at least one real contract"
            policy that ``docs/CONTRACTS.md`` states.
        actions: Steps that mutate cluster state (faults, writes, etc.).
        assertions: Steps that verify invariants after actions complete.
        cleanup: Heal steps that restore the cluster for the next case.  Every
            cleanup step is attempted regardless of the outcome of earlier
            phases and regardless of earlier cleanup-step failures; failures are
            aggregated and reported (see :meth:`CIRunner._run_cleanup`).
        ci_excluded_reason: Why no workflow runs this case. Empty means it runs.
            ``--emit-cases`` omits any case carrying one, so a case is either
            executed by CI or says in the matrix why it is not; `lint_matrix.py`
            rejects a blank reason. Running the case by ``--case`` still works —
            this governs the derived CI matrix, not the runner.
    """

    id: str
    description: str = ""
    ci_excluded_reason: str = ""
    contracts: list[str] = []
    actions: list[dict[str, Any]] = []
    assertions: list[dict[str, Any]] = []
    cleanup: list[dict[str, Any]] = []

    @field_validator("contracts")
    @classmethod
    def _validate_contracts(cls, value: list[str]) -> list[str]:
        """Reject malformed contract IDs and duplicates.

        Existence of each ID in ``docs/CONTRACTS.md`` is checked by
        ``lint_matrix.py``, which owns the doc-parsing side of the policy.
        """
        seen: set[str] = set()
        for raw in value:
            if not CONTRACT_ID_PATTERN.fullmatch(raw):
                raise ValueError(
                    f"malformed contract ID {raw!r}: expected letters followed by digits "
                    f"(e.g. 'W1', 'L2', 'R1')"
                )
            if raw in seen:
                raise ValueError(f"duplicate contract ID {raw!r}")
            seen.add(raw)
        return value

    @field_validator("actions", "assertions", "cleanup")
    @classmethod
    def _validate_steps(cls, value: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate the step envelope shared by every step type.

        Checks ``type`` is a non-empty string and that any shell-timeout
        override is a positive integer, so a typo fails at matrix-parse time
        rather than mid-suite.
        """
        for index, step in enumerate(value):
            raw_type = step.get("type")
            if not isinstance(raw_type, str) or not raw_type.strip():
                raise ValueError(f"step #{index} is missing a non-empty 'type'")
            for key in SHELL_TIMEOUT_KEYS:
                if step.get(key) is not None:
                    validate_timeout_value(step[key], key)
        return value


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
        cleanup_failures: One entry per cleanup step that failed.  A case whose
            assertions passed but whose cleanup failed is reported as ``ERROR``,
            never ``PASS``: the residue it leaves behind (a surviving iptables
            rule, a still-paused container) corrupts every later case in a
            ``reuse_cluster`` suite.
    """

    case_id: str
    passed: bool
    detail: str
    cleanup_failures: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        """Return ``PASS``, ``FAIL``, or ``ERROR`` (assertions passed, cleanup did not)."""
        if not self.passed:
            return "FAIL"
        return "ERROR" if self.cleanup_failures else "PASS"


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
# Fault-effect verification
#
# A fault that silently fails to inject turns a scenario into vacuous green:
# the cluster is never disturbed, every assertion holds, the case passes.  The
# helpers below parse the output of read-only probes so each fault-injecting
# step can prove its fault actually landed (and each heal can prove it is
# actually gone).  They are pure functions of command output so they can be
# unit-tested without a cluster.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of a read-only verification probe.

    Attributes:
        exit_code: Probe process exit status.
        stdout: Captured stdout.
        stderr: Captured stderr.
    """

    exit_code: int
    stdout: str
    stderr: str

    @property
    def combined(self) -> str:
        """stdout and stderr joined, for probes whose tools log to either."""
        return f"{self.stdout}\n{self.stderr}" if self.stderr else self.stdout


@dataclass(frozen=True)
class ContainerRunState:
    """Container liveness identity used to prove a kill/restart happened.

    Attributes:
        status: Docker state string (``running``, ``paused``, ``exited``, ...).
        started_at: RFC3339 start timestamp of the current incarnation.
        restart_count: Number of restart-policy restarts docker has performed.
    """

    status: str
    started_at: str
    restart_count: int


def parse_container_runstate(output: str) -> ContainerRunState | None:
    """Parse ``docker inspect --format '{{.State.Status}} {{.State.StartedAt}} {{.RestartCount}}'``.

    Args:
        output: Probe stdout.

    Returns:
        The parsed state, or ``None`` if the probe produced no usable line
        (missing container, docker error, empty output).
    """
    for line in output.splitlines():
        parts = line.split()
        if len(parts) != 3 or not parts[2].isdigit():
            continue
        return ContainerRunState(status=parts[0], started_at=parts[1], restart_count=int(parts[2]))
    return None


_SERVICE_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def parse_compose_services(output: str) -> set[str]:
    """Parse ``docker compose ps --services`` output into a set of service names.

    Args:
        output: Probe stdout (one service name per line, possibly with
            interleaved warning lines).

    Returns:
        Set of service names.
    """
    return {
        line.strip() for line in output.splitlines() if _SERVICE_NAME_PATTERN.match(line.strip())
    }


# Rule and qdisc parsing lives in `fault_primitives`; this module used to carry
# its own copies. The netem contract differs slightly and the primitive one is
# the keeper: `parse_netem` returns a state with `delay_ms == 0.0` for a qdisc
# installed without a delay clause, where the old local parser returned None and
# so could not tell that apart from no qdisc at all.


def parse_ps_stat_comm(output: str) -> list[tuple[str, str]]:
    """Parse ``ps -eo stat=,comm=`` output into ``(stat, comm)`` pairs.

    Args:
        output: Probe stdout.

    Returns:
        List of ``(process state code, command name)`` pairs.
    """
    pairs: list[tuple[str, str]] = []
    for line in output.splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        pairs.append((parts[0].strip(), parts[1].strip()))
    return pairs


def matching_processes(pairs: Sequence[tuple[str, str]], pattern: str) -> list[tuple[str, str]]:
    """Select the ``(stat, comm)`` pairs whose command name matches ``pattern``.

    ``pkill`` treats its pattern as an extended regular expression over the
    (15-character-truncated) command name; an unparseable pattern falls back to
    a substring match.

    Args:
        pairs: Parsed ``ps`` output.
        pattern: The pkill pattern.

    Returns:
        Matching pairs.
    """
    try:
        compiled = re.compile(pattern)
    except re.error:
        return [pair for pair in pairs if pattern in pair[1]]
    return [pair for pair in pairs if compiled.search(pair[1])]


def stopped_processes(pairs: Sequence[tuple[str, str]], pattern: str) -> list[tuple[str, str]]:
    """Select matching processes that are in the stopped state (``T``).

    Args:
        pairs: Parsed ``ps`` output.
        pattern: The pkill pattern.

    Returns:
        Matching pairs whose state code starts with ``T``.
    """
    return [pair for pair in matching_processes(pairs, pattern) if pair[0].startswith("T")]


def parse_faketime_probe(output: str) -> tuple[str, int] | None:
    """Parse the combined ``cat /tmp/faketime; date +%s`` probe output.

    Args:
        output: Probe stdout — the libfaketime offset on one line, the
            container's current epoch seconds on the next.

    Returns:
        ``(offset_text, epoch_seconds)``, or ``None`` if the output is not
        parseable.
    """
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) < 2 or not lines[-1].isdigit():
        return None
    return lines[-2], int(lines[-1])


_FAKETIME_OFFSET_PATTERN: Final[re.Pattern[str]] = re.compile(r"^([+-])(\d+)([smhd])$")
_OFFSET_UNIT_TO_SEC: Final[dict[str, int]] = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_faketime_offset_seconds(text: str) -> int | None:
    """Convert a libfaketime relative offset (e.g. ``+300s``) to seconds.

    Args:
        text: Offset text as written to ``FAKETIME_TIMESTAMP_FILE``.

    Returns:
        Signed seconds, or ``None`` for absolute/unsupported offset formats.
    """
    match = _FAKETIME_OFFSET_PATTERN.match(text.strip())
    if not match:
        return None
    sign = -1 if match.group(1) == "-" else 1
    return sign * int(match.group(2)) * _OFFSET_UNIT_TO_SEC[match.group(3)]


class DockerNetworkAttachment(BaseModel):
    """One entry of ``docker inspect .NetworkSettings.Networks``."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    ip_address: str = Field(default="", alias="IPAddress")


class DockerNetworks(RootModel[dict[str, DockerNetworkAttachment]]):
    """The whole Networks blob. ``null`` — a container attached to none — is an
    empty mapping, which is what every caller means by "no addresses"."""

    root: dict[str, DockerNetworkAttachment] = {}

    @model_validator(mode="before")
    @classmethod
    def _null_is_empty(cls, value: object) -> object:
        return {} if value is None else value


def container_subnet_addresses(json_output: str, prefix: str) -> list[str]:
    """Extract container IPs matching ``prefix`` from a Networks JSON blob.

    Args:
        json_output: Output of
            ``docker inspect --format '{{json .NetworkSettings.Networks}}'``.
        prefix: Address prefix to select (e.g. ``172.28.``).

    Returns:
        Matching IP addresses (empty when the container is attached to no
        matching network, or the blob is ``null`` / unparseable).
    """
    try:
        attachments = DockerNetworks.model_validate_json(json_output.strip() or "null").root
    except ValidationError:
        return []
    return [a.ip_address for a in attachments.values() if a.ip_address.startswith(prefix)]


# -- Fault-shaped shell command classification -------------------------------

# Shell control operators.  A command containing any of these is not a single
# invocation we can reason about, so it is never classified.
_SHELL_METACHARACTERS: Final[tuple[str, ...]] = (";", "&", "|", "`", "$(", ">", "<", "\n")

# Commands that inject a fault.  Used only to decide whether an unclassifiable
# command deserves an explicit "unverified fault" warning, so that a dynamic
# one-liner never looks like a verified fault.
_FAULT_INJECTION_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\bdocker\s+(compose\s+)?pause\b"),
    re.compile(r"\bdocker\s+compose\s+(kill|stop)\b"),
    re.compile(r"\bdocker\s+kill\b"),
    re.compile(r"\bdocker\s+network\s+disconnect\b"),
    re.compile(r"\bpkill\b[^|;]*\s-(9|KILL|SIGKILL|STOP|SIGSTOP)\b"),
    re.compile(r"\bkill\s+-(9|KILL|SIGKILL|STOP|SIGSTOP)\b"),
    re.compile(r"\biptables\s+-[AI]\b"),
    re.compile(r"\btc\s+qdisc\s+add\b"),
    re.compile(r"\bpg_ctl\s+promote\b"),
    re.compile(r"\bdd\s+if=/dev/urandom\b"),
    re.compile(r"\bfallocate\b"),
)

# `docker compose` global flags that consume the following token.
_COMPOSE_VALUE_FLAGS: Final[frozenset[str]] = frozenset(
    {"-f", "--file", "-p", "--project-name", "--profile", "--project-directory", "--env-file"}
)
# `docker compose exec` flags that consume the following token.
_EXEC_VALUE_FLAGS: Final[frozenset[str]] = frozenset(
    {"-e", "--env", "-u", "--user", "-w", "--workdir", "--index"}
)

_PKILL_STOP_SIGNALS: Final[frozenset[str]] = frozenset({"-STOP", "-SIGSTOP", "-19"})
_PKILL_CONT_SIGNALS: Final[frozenset[str]] = frozenset({"-CONT", "-SIGCONT", "-18"})
_PKILL_KILL_SIGNALS: Final[frozenset[str]] = frozenset({"-9", "-KILL", "-SIGKILL"})


@dataclass(frozen=True)
class FaultIntent:
    """What a fault-shaped shell command claims to do, so it can be verified.

    Attributes:
        kind: Verification strategy key (see
            :meth:`CIRunner._verify_fault_intent`).
        target_kind: ``"service"`` for compose service names, ``"container"``
            for raw container names.
        targets: Targets the command names.
        injects: ``True`` for fault injection (verification failure fails the
            step), ``False`` for a heal (a target that no longer exists is
            treated as already healed).
        pattern: ``pkill`` process pattern, empty for other kinds.
        needs_prestate: ``True`` when verification compares container liveness
            identity before and after the command.
    """

    kind: str
    target_kind: str
    targets: tuple[str, ...]
    injects: bool
    pattern: str = ""
    needs_prestate: bool = False


def container_exec_prefix(service: str, privileged: bool = False) -> str:
    """Build the ``docker compose exec`` prefix for a command inside a node.

    The runtime image ends with ``USER postgres`` (``Dockerfile``), and
    ``docker-compose.yml`` sets no ``user:`` override, so an exec defaults to
    the unprivileged ``postgres`` user.  ``cap_add: [NET_ADMIN]`` grants the
    capability to the *container*, but an unprivileged process does not carry it:
    every ``tc`` / ``iptables`` mutation, and every read of the iptables chain,
    fails with ``Operation not permitted`` unless the exec asks for root.

    Args:
        service: Compose service name (e.g. ``node1``).
        privileged: ``True`` for operations that need ``CAP_NET_ADMIN`` (network
            stack mutation and inspection).  Everything else — psql, pgbench,
            ``ps``, the libfaketime offset file — must stay unprivileged so it
            runs as the same user PostgreSQL and pgbattery run as.

    Returns:
        The command prefix, ending with the service name.
    """
    user = f" --user {PRIVILEGED_EXEC_USER}" if privileged else ""
    return f"docker compose exec -T{user} {service}"


def looks_like_fault_injection(command: str) -> bool:
    """Report whether ``command`` appears to inject a fault.

    Args:
        command: Rendered shell command.

    Returns:
        ``True`` if any known fault-injection verb appears.
    """
    return any(pattern.search(command) for pattern in _FAULT_INJECTION_PATTERNS)


def is_simple_command(command: str) -> bool:
    """Report whether ``command`` is one invocation with no shell operators.

    Args:
        command: Rendered shell command.

    Returns:
        ``True`` if the command contains no shell control metacharacters.
    """
    return not any(meta in command for meta in _SHELL_METACHARACTERS)


def _strip_flags(tokens: Sequence[str], value_flags: frozenset[str]) -> list[str]:
    """Drop leading option tokens, consuming values of known value-flags.

    Args:
        tokens: Argument tokens.
        value_flags: Flags whose following token is a value, not a positional.

    Returns:
        The remaining positional tokens.
    """
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if not token.startswith("-"):
            break
        if token in value_flags and "=" not in token:
            index += 2
            continue
        index += 1
    return list(tokens[index:])


def _classify_pkill(service: str, args: Sequence[str]) -> FaultIntent | None:
    """Classify a ``pkill`` invocation inside a container.

    Args:
        service: Compose service the pkill runs in.
        args: Tokens after the ``pkill`` executable.

    Returns:
        The intent, or ``None`` when the signal or pattern is not one we can
        verify (notably ``-f``, whose pattern matches the full command line
        rather than the process name reported by ``ps -o comm=``).
    """
    if "-f" in args:
        return None
    signal_tokens = [token for token in args if token.startswith("-")]
    positional = [token for token in args if not token.startswith("-")]
    if len(positional) != 1:
        return None
    pattern = positional[0]
    signals = set(signal_tokens)
    if signals & _PKILL_STOP_SIGNALS:
        kind, injects, prestate = "pkill_stop", True, False
    elif signals & _PKILL_CONT_SIGNALS:
        kind, injects, prestate = "pkill_cont", False, False
    elif signals & _PKILL_KILL_SIGNALS:
        kind, injects, prestate = "pkill_kill", True, True
    else:
        return None
    return FaultIntent(
        kind=kind,
        target_kind="service",
        targets=(service,),
        injects=injects,
        pattern=pattern,
        needs_prestate=prestate,
    )


def _classify_compose(tokens: Sequence[str]) -> FaultIntent | None:
    """Classify a ``docker compose ...`` invocation.

    Args:
        tokens: Tokens after ``docker compose``.

    Returns:
        The intent, or ``None`` when the subcommand is not a fault or heal we
        can verify.
    """
    positional = _strip_flags(tokens, _COMPOSE_VALUE_FLAGS)
    if not positional:
        return None
    subcommand, rest = positional[0], positional[1:]
    if subcommand == "exec":
        exec_positional = _strip_flags(rest, _EXEC_VALUE_FLAGS)
        if len(exec_positional) < 2:
            return None
        service, executable, args = exec_positional[0], exec_positional[1], exec_positional[2:]
        if executable == "pkill":
            return _classify_pkill(service, args)
        return None
    services = tuple(_strip_flags(rest, frozenset({"-t", "--timeout", "-s", "--signal"})))
    if not services:
        return None
    match subcommand:
        case "stop":
            return FaultIntent("compose_stop", "service", services, injects=True)
        case "kill":
            return FaultIntent(
                "compose_kill", "service", services, injects=True, needs_prestate=True
            )
        case "start":
            return FaultIntent("compose_start", "service", services, injects=False)
        case "restart":
            return FaultIntent("compose_restart", "service", services, injects=False)
        case _:
            return None


def _classify_docker(tokens: Sequence[str]) -> FaultIntent | None:
    """Classify a plain ``docker ...`` invocation.

    Args:
        tokens: Tokens after ``docker``.

    Returns:
        The intent, or ``None`` when the subcommand is not a fault or heal we
        can verify.
    """
    if not tokens:
        return None
    subcommand, rest = tokens[0], tokens[1:]
    if subcommand in {"pause", "unpause"}:
        containers = tuple(_strip_flags(rest, frozenset()))
        if not containers:
            return None
        return FaultIntent(
            kind=subcommand,
            target_kind="container",
            targets=containers,
            injects=subcommand == "pause",
        )
    if subcommand == "network" and len(rest) >= 3 and rest[0] in {"connect", "disconnect"}:
        action = rest[0]
        operands = _strip_flags(rest[1:], frozenset({"--ip", "--ip6", "--alias", "--link"}))
        if len(operands) != 2:
            return None
        return FaultIntent(
            kind=f"network_{action}",
            target_kind="container",
            targets=(operands[1],),
            injects=action == "disconnect",
        )
    return None


def classify_fault_command(command: str) -> FaultIntent | None:
    """Derive a verifiable intent from a fault-shaped shell command.

    Only single, statically analysable invocations are classified.  Compound
    or dynamic commands (``LEADER=$(...); docker compose kill node$LEADER``)
    return ``None``; callers pair that with :func:`looks_like_fault_injection`
    to emit an explicit unverified-fault warning instead of staying silent.

    Args:
        command: Rendered shell command.

    Returns:
        The intent, or ``None`` when the command is not a classifiable fault.
    """
    if not is_simple_command(command):
        return None
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    if not tokens or tokens[0] != "docker":
        return None
    if len(tokens) > 1 and tokens[1] == "compose":
        return _classify_compose(tokens[2:])
    return _classify_docker(tokens[1:])


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

    # Fault-verification retry seams.  Post-conditions can take a moment to
    # appear (docker restarting a killed container, a service reaching
    # ``running``), so verification polls.  Unit tests drive canned probe output
    # that never changes and set both to 0 to make verification single-shot.
    verify_retry_scale: float = 1.0
    verify_poll_interval_sec: float = 1.0

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
        # Fault-shaped commands whose effect the runner could not confirm.
        # Surfaced in the summary so "no verification" is never mistaken for
        # "verified".
        self.unverified_faults: list[str] = []

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

        A whole-suite run skips cases carrying a ``ci_excluded_reason`` and says
        so, matching what CI runs — a local run that went red on a case no
        workflow executes would disagree with CI about the same suite. Naming
        one with ``--case`` still runs it: the exclusion governs what runs by
        default, not what may be run.

        Raises:
            RunnerError: If ``case_filter`` is set but not found in the suite.
        """
        case_ids = list(self.suite_config.cases)
        if self.case_filter is not None:
            if self.case_filter not in case_ids:
                raise RunnerError(f"Case '{self.case_filter}' is not in suite '{self.suite_name}'.")
            return [self.case_filter]
        selected: list[str] = []
        for case_id in case_ids:
            case = next((c for c in self.matrix.cases if c.id == case_id), None)
            reason = case.ci_excluded_reason if case is not None else ""
            if reason:
                self.log(f"[skip] {case_id}: {reason}")
                continue
            selected.append(case_id)
        return selected

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
        timeout_sec: int = DEFAULT_SHELL_TIMEOUT_SEC,
        render: bool = True,
        stdin_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a shell command under a timeout, log output, and check exit code.

        Args:
            command: Shell command string (may contain ``{{ var }}`` templates).
            log_path: File to write the command, exit code, stdout, and stderr.
            expect_exit: Expected exit code(s).  ``None`` accepts any code.
            timeout_sec: Wall-clock budget.  Exceeding it kills the command and
                raises, whatever ``expect_exit`` says — a hang is never a pass.
            render: Set ``False`` for internally generated commands (probes)
                that contain Go template braces the ``{{ var }}`` renderer would
                otherwise try to resolve.
            stdin_text: Text to feed the command on stdin (used to pipe ``.sql``
                files into ``psql`` without any shell escaping).

        Returns:
            The completed process.

        Raises:
            RunnerError: If the command exceeds ``timeout_sec``, or the actual
                exit code is not in ``expect_exit``.
        """
        rendered = self._render_template(command) if render else command
        # Popen rather than subprocess.run: on POSIX, run() discards the output
        # buffered before a timeout, and that partial output is usually the only
        # evidence of where the command wedged.
        with subprocess.Popen(
            rendered,
            shell=True,
            cwd=self.project_root,
            env=self.env,
            stdin=subprocess.PIPE if stdin_text is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as handle:
            try:
                stdout, stderr = handle.communicate(input=stdin_text, timeout=timeout_sec)
            except subprocess.TimeoutExpired as exc:
                handle.kill()
                try:
                    # Drain whatever the killed command already wrote.  A
                    # grandchild that inherited the pipe can hold it open, so
                    # the drain itself is bounded.
                    stdout, stderr = handle.communicate(timeout=TIMEOUT_DRAIN_SEC)
                except subprocess.TimeoutExpired:
                    stdout, stderr = None, "<partial output unavailable: pipe still held open>"
                message = format_timeout_failure(rendered, timeout_sec, stdout, stderr)
                self._write_text(log_path, message)
                self.log(f"    {message.splitlines()[0]}")
                raise RunnerError(message) from exc
            returncode = handle.returncode

        proc = subprocess.CompletedProcess(
            args=rendered, returncode=returncode, stdout=stdout, stderr=stderr
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

        expected = as_ints(expect_exit)

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

    def _get_self_claimed_leaders(self) -> dict[int, int | None]:
        """Every reachable node's answer to "who is the leader", by node id.

        Thin wrapper over :meth:`_get_leader_views` that exists to name the
        distinction the convergence check depends on: a *self*-claim (node N
        says the leader is N) is the only claim a partitioned node can make
        without hearing from anyone, so two distinct self-claims are the
        observable signature of split brain. A node naming *someone else* is
        reporting hearsay and cannot manufacture a second leader.
        """
        return self._get_leader_views()

    def _holds_valid_lease(self, node_id: int) -> bool:
        """Whether `node_id` reports a currently valid write lease.

        Unreachable or metric-less counts as "no lease": this only ever gates
        escalating a suspicion to a split-brain failure, so the conservative
        answer is the one that does not invent a second leader out of a scrape
        error.
        """
        try:
            values = self._fetch_metric_values(node_id, "pgbattery_lease_valid")
        except RunnerError:
            return False
        return any(value >= 0.5 for value in values)

    def _quorum_leader(self, views: Mapping[int, int | None]) -> tuple[int | None, bool]:
        """Reduce per-node leader views to ``(leader_id, agreed)``.

        A leader is established only when a strict majority of the *configured*
        cluster — not of the nodes that happened to answer — names the same one.
        Sizing the majority on the configured count is what stops a minority
        partition from certifying its own view: two reachable nodes out of three
        that disagree cannot produce a leader, and one reachable node can never
        produce one on its own.
        """
        quorum = len(self.node_map) // 2 + 1
        tally: dict[int, int] = {}
        for seen in views.values():
            if seen is not None:
                tally[seen] = tally.get(seen, 0) + 1
        for leader_id, count in tally.items():
            if count >= quorum:
                return leader_id, True
        return None, False

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
        that may not actually be the leader after prior failovers.

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
        stable_for_sec: float = 0.0,
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
        # When stable_for_sec > 0, the full success condition must hold across
        # consecutive polls for that long before we return — this asserts the
        # cluster has *settled* on one leader, not merely flickered through the
        # target state for a single poll. Critical after a disruption that
        # provokes a brief, legitimate re-election (e.g. a partitioned former
        # leader rejoining): a one-shot observation can catch the momentary
        # leaders==1 and return right before the re-election drops it to 0.
        stable_since: float | None = None
        while time.time() < deadline:
            try:
                nodes = self._get_cluster_nodes()
                node_count = len(nodes)
                # Leadership is resolved across every node, not from the single
                # node that answered `/cluster/nodes` first. That response marks
                # `is_leader` from one `Option<node_id>`, so exactly one entry
                # can ever be true in it: counting leaders there can neither see
                # split brain nor notice that the node answering is an isolated
                # ex-leader still naming itself.
                views = self._get_self_claimed_leaders()
                self_claimants = sorted(
                    node_id for node_id, seen in views.items() if seen == node_id
                )
                # Two self-claims is a suspicion, not a verdict. An isolated
                # ex-leader legitimately keeps naming itself until it can hear
                # someone again — it has no way to learn otherwise — while its
                # lease expires and it fences itself. Split brain is two nodes
                # with *write authority*, so confirm against the lease before
                # calling it: that, not the belief, is what L1 constrains.
                if len(self_claimants) > 1:
                    leased = [nid for nid in self_claimants if self._holds_valid_lease(nid)]
                    if len(leased) > 1:
                        raise RunnerError(
                            f"Illegal cluster state: {len(leased)} concurrent leaders "
                            f"holding valid leases (split brain): {leased}"
                        )
                quorum_leader, quorum_agreed = self._quorum_leader(views)
                leader_count = 1 if quorum_agreed else 0
                topology_ok = node_count == expected_nodes and leader_count == expected_leaders
                leader_changed = leader_not is None or (
                    quorum_agreed and quorum_leader != leader_not
                )
                leader_eq_ok = leader_equals is None or (
                    quorum_agreed and quorum_leader == leader_equals
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
                if require_replication_health and topology_ok and quorum_leader is not None:
                    elected_leader = quorum_leader
                    # `views` and the split-brain check above already ran, and
                    # ran unconditionally — this branch is off whenever a case
                    # partitions the cluster, which is the one time two
                    # self-claims are actually reachable.
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
                if require_replication_health and topology_ok and quorum_leader is not None:
                    leader_id = quorum_leader
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
                converged = (
                    topology_ok
                    and leader_changed
                    and leader_eq_ok
                    and voters_ok
                    and views_ok
                    and repl_ok
                )
                if converged:
                    if stable_since is None:
                        stable_since = time.time()
                    if stable_for_sec <= 0 or (time.time() - stable_since) >= stable_for_sec:
                        return
                else:
                    # Any regression resets the stability window.
                    stable_since = None
                last_error = (
                    f"observed nodes={node_count}, leaders={leader_count}, voters={voter_count}, "
                    f"expected nodes={expected_nodes}, leaders={expected_leaders}"
                )
                if require_all_voters:
                    last_error += f", expected_voters={expected_nodes}"
                if leader_not is not None and leader_count == 1:
                    last_error += (
                        f", current_leader={quorum_leader} (waiting for change from {leader_not})"
                    )
                if leader_equals is not None and leader_count == 1:
                    last_error += (
                        f", current_leader={quorum_leader} (waiting for leader={leader_equals})"
                    )
                if require_replication_health:
                    last_error += views_detail
                    last_error += repl_detail
                if converged and stable_for_sec > 0 and stable_since is not None:
                    last_error += (
                        f", converged but awaiting stability "
                        f"({time.time() - stable_since:.0f}/{stable_for_sec:.0f}s held)"
                    )
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
            rf"^{re.escape(metric_name)}(\{{[^}}]*\}})?\s+"
            r"(-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)$"
        )
        values: list[float] = []
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            match = pattern.match(line)
            if match and self._series_belongs_to_cluster(match.group(1)):
                values.append(float(match.group(2)))
        return values

    def _series_belongs_to_cluster(self, labels: str | None) -> bool:
        """Whether a labelled series describes one of this cluster's own nodes.

        Prometheus series are never removed once created: a node that leaves
        keeps its `node="4"` series at 0.0 for the life of the leader process.
        Counting those made `observed_replicas` permanently wrong for any
        cluster that had ever had a fourth node — `witness-topology` adds one,
        so every case after it saw three replicas on a three-node cluster and
        the suite stopped there.

        Only the `node` label is filtered, and only when it names a node the
        matrix does not have. Series with no labels, or labelled by something
        else, are counted as before.
        """
        if not labels:
            return True
        match = re.search(r'node="(\d+)"', labels)
        return match is None or int(match.group(1)) in self.node_map

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

        # Snapshots are diagnostics, not assertions: a docker daemon that is too
        # wedged to answer must not abort the case (or mask the real failure),
        # so a timeout here is recorded and swallowed.
        try:
            compose_ps = self._run_shell(
                "docker compose ps",
                snap / "compose-ps.txt",
                expect_exit=None,
                timeout_sec=DIAGNOSTIC_TIMEOUT_SEC,
            )
            if compose_ps.returncode != 0:
                self._write_text(
                    snap / "compose-ps.error.txt",
                    f"docker compose ps failed with {compose_ps.returncode}",
                )
        except RunnerError as exc:
            self._write_text(snap / "compose-ps.error.txt", str(exc))
            self.log(f"    [warn] snapshot 'docker compose ps' did not complete: {exc}")

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
        for command, filename in [
            ("docker compose logs --no-color", "docker-compose.logs.txt"),
            ("docker compose ps -a", "docker-compose.ps-a.txt"),
        ]:
            # Best-effort: never let log collection replace the failure that
            # triggered it.
            try:
                self._run_shell(
                    command,
                    failure_dir / filename,
                    expect_exit=None,
                    timeout_sec=DIAGNOSTIC_TIMEOUT_SEC,
                )
            except RunnerError as exc:
                self.log(f"    [warn] failure-log collection '{command}' did not complete: {exc}")

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
            timeout_sec=CLUSTER_LIFECYCLE_TIMEOUT_SEC,
        )

        up_cmd = "docker compose up -d --remove-orphans"
        if self.build_images:
            up_cmd += " --build"
        self._run_shell(
            up_cmd,
            self.system_dir / f"{utc_timestamp()}-{safe_name(label)}-up.log",
            expect_exit=0,
            timeout_sec=CLUSTER_LIFECYCLE_TIMEOUT_SEC,
        )

        # Replication health, not just topology: a leader whose sync list is
        # not yet being honoured refuses writes, so a case's first SQL step
        # fails on a cluster that is merely up. The between-cases barrier has
        # always required this; a freshly started one is no different.
        self._wait_for_cluster(
            expected_nodes=self.matrix.cluster.expected_nodes,
            expected_leaders=1,
            timeout_sec=180,
            require_all_voters=True,
            require_replication_health=True,
        )
        self._collect_snapshot(f"{label}-cluster-started")

    def _record_cases_not_reached(self, stopped_at: str) -> None:
        """Put the cases a suite abort skipped into the summary.

        Args:
            stopped_at: Case id whose failure ended the suite.
        """
        remaining = self.selected_case_ids[self.selected_case_ids.index(stopped_at) + 1 :]
        if not remaining:
            return
        self.log(f"[not-run] {len(remaining)} case(s) after {stopped_at}: {', '.join(remaining)}")
        for case_id in remaining:
            self.summary.append(
                CaseSummary(
                    case_id=case_id,
                    passed=False,
                    detail=f"NOT RUN — suite stopped at {stopped_at}",
                )
            )

    def _check_log_budget(self, label: str) -> None:
        """Fail the run when a node logged like a hot loop.

        Counts lines per service while the containers still exist, and keeps
        the offender's log so the repeated line is one ``sort | uniq -c`` away.
        A loop that retries something thousands of times a second starves the
        data plane it shares a machine with, and every case it breaks fails
        looking like something else.

        Args:
            label: Descriptive label for log files.
        """
        for node in self.matrix.cluster.nodes:
            result = self._run_shell(
                f"docker compose logs --no-color {node.name} | wc -l",
                self.system_dir / f"{utc_timestamp()}-{safe_name(label)}-logvol-{node.name}.log",
                expect_exit=None,
                timeout_sec=DIAGNOSTIC_TIMEOUT_SEC,
            )
            lines = int(result.stdout.strip() or 0)
            if lines <= LOG_LINES_PER_SERVICE_BUDGET:
                self.log(f"    [log-volume] {node.name}: {lines} lines")
                continue
            self.failed = True
            detail = (
                f"logged {lines} lines this run, over the {LOG_LINES_PER_SERVICE_BUDGET} "
                f"budget — something is retrying in a tight loop"
            )
            self.log(f"[fail] log volume: {node.name} {detail}")
            self.summary.append(
                CaseSummary(case_id=f"log-volume:{node.name}", passed=False, detail=detail)
            )
            self._run_shell(
                f"docker compose logs --no-color {node.name}",
                self.system_dir / f"{utc_timestamp()}-{safe_name(label)}-hotloop-{node.name}.log",
                expect_exit=None,
                timeout_sec=DIAGNOSTIC_TIMEOUT_SEC,
            )

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
            timeout_sec=CLUSTER_LIFECYCLE_TIMEOUT_SEC,
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
        expected = as_ints(step.get("expect_status", 200))

        # POSTs are mutations and always carry the token. GETs default to
        # unauthenticated so the public discovery contract stays tested;
        # token-gated GETs (e.g. /api/v1/backup/list) opt in with `auth: true`.
        send_auth = method == "POST" or bool(step.get("auth", False))
        auth_headers = (
            {"x-pgbattery-token": self.mgmt_token} if send_auth and self.mgmt_token else {}
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
            for needle in as_strings(step["body_contains"]):
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
        # 409 means "not now, retry after replication converges" (e.g. the
        # target standby is still catching up on WAL right after a preceding
        # case restarted it — reuse_cluster suites hit this). The server's
        # refusal is the safety gate working; honor its retry semantics with
        # a bounded window instead of failing the case on the first attempt.
        retry_deadline = time.monotonic() + float(step.get("retry_sec", 60))
        attempts = []
        while True:
            status, body = self._http_request(
                "POST",
                url,
                timeout_sec=int(step.get("timeout_sec", 15)),
                headers={"x-pgbattery-token": self.mgmt_token} if self.mgmt_token else {},
            )
            attempts.append({"status": status, "body": body})
            if status != 409 or time.monotonic() >= retry_deadline:
                break
            time.sleep(2.0)
        self._write_text(
            step_log,
            json.dumps(
                {
                    "url": url,
                    "attempts": attempts,
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
                ``capture_tps`` (optional context variable name),
                ``shell_timeout_sec`` (default ``duration_sec`` + 60).
            step_log: File to write pgbench stdout/stderr and measured TPS.

        Raises:
            RunnerError: If the node is unknown, pgbench fails to run or exceeds
                its timeout, or the measured TPS is below ``min_tps``.
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
        timeout_sec = resolve_shell_timeout(step, default=duration_sec + 60)

        # Initialise pgbench schema (idempotent: -i drops and recreates tables).
        exec_prefix = container_exec_prefix(node.name)
        init_cmd = f"{exec_prefix} {pg_bin} -i -s {scale} {pg_conn}"
        self._run_shell(init_cmd, step_log, timeout_sec=timeout_sec)

        # Run benchmark.
        bench_cmd = f"{exec_prefix} {pg_bin} -c {clients} -j {threads} -T {duration_sec} {pg_conn}"
        try:
            result = subprocess.run(
                bench_cmd,
                shell=True,
                cwd=self.project_root,
                env=self.env,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
        except subprocess.TimeoutExpired as exc:
            message = format_timeout_failure(
                bench_cmd,
                timeout_sec,
                captured_text(exc.stdout),
                captured_text(exc.stderr),
            )
            self._write_text(step_log, message)
            raise RunnerError(message) from exc

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

    def _await_node_accepts_writes(self, node: ClusterNodeConfig, step_log: Path) -> None:
        """Block until `node` runs a write, or raise saying it never did.

        The probe is a rolled-back temp table: refused in a read-only
        transaction the same way the case's own INSERT would be, and
        session-local, so a passing probe leaves nothing behind.
        """
        deadline = time.time() + SQL_WRITABLE_TIMEOUT_SEC
        last = "no attempt made"
        probe = (
            f"{container_exec_prefix(node.name)} "
            "psql -U postgres -h localhost -p 5432 -d postgres -v ON_ERROR_STOP=1 "
            '-c "CREATE TEMP TABLE ci_runner_writable_probe(x int)"'
        )
        while time.time() < deadline:
            proc = self._run_shell(
                probe,
                step_log.with_suffix(".writable-probe.log"),
                expect_exit=None,
                timeout_sec=15,
                render=False,
            )
            if proc.returncode == 0:
                return
            last = (proc.stderr or proc.stdout).strip()
            time.sleep(1.0)
        raise RunnerError(
            f"{node.name} never accepted a write within {SQL_WRITABLE_TIMEOUT_SEC}s of being "
            f"resolved as the leader; last psql error: {last}"
        )

    def _execute_sql_step(self, step: dict[str, Any], step_log: Path) -> None:
        """Execute a ``sql`` step: pipe a ``.sql`` file through psql via stdin.

        Reads the file from ``testing/sql/{file}``, sends it as stdin to
        ``psql`` inside the target container.  This avoids all shell escaping
        issues that plague inline SQL in ``cmd`` steps.

        Args:
            step: Step dict with required ``file`` and optional ``node``
                (default 1), ``direct`` (connect to internal port 5434 instead
                of gateway 5432), ``on_error_stop`` (default ``True``),
                ``expect_exit`` (default 0), and ``shell_timeout_sec``.
            step_log: File to write the SQL content, stdout, stderr, and exit
                code.

        Raises:
            RunnerError: If the SQL file is missing, the node is unknown, psql
                exceeds its timeout, or the exit code is unexpected.
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

        # Winning the election is not the same as taking writes: a freshly
        # promoted primary stays read-only until the lease tick recovers
        # writes. Waiting turns "cannot execute INSERT in a read-only
        # transaction" — which reads like the case's own subject failing —
        # back into what it is, a step that started too early.
        if sql_step_needs_a_writable_path(step, sql_content):
            self._await_node_accepts_writes(node, step_log)

        port = 5434 if step.get("direct") else 5432
        on_error_stop = "1" if step.get("on_error_stop", True) else "0"
        expect_exit = step.get("expect_exit", 0)

        cmd = (
            f"{container_exec_prefix(node.name)} "
            f"psql -U postgres -h localhost -p {port} -d postgres "
            f"-v ON_ERROR_STOP={on_error_stop}"
        )
        # expect_exit=None: the step's own expectation is checked below, after
        # the fuller log (including the SQL text) has been written.
        proc = self._run_shell(
            cmd,
            step_log,
            expect_exit=None,
            timeout_sec=resolve_shell_timeout(step),
            render=False,
            stdin_text=sql_content,
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

        expected = as_ints(expect_exit)

        if proc.returncode not in expected:
            raise RunnerError(
                f"SQL file {step['file']} failed with exit code {proc.returncode}, "
                f"expected {expected}"
            )

    # -- Fault-effect verification -------------------------------------------

    def _verify_log(self, step_log: Path, tag: str) -> Path:
        """Return the log path for a verification probe belonging to a step."""
        return step_log.with_name(f"{step_log.stem}.{safe_name(tag)}.log")

    def _probe(self, command: str, log_path: Path) -> ProbeResult:
        """Run a read-only verification probe.

        Args:
            command: Probe command (never template-rendered: probes embed Go
                template braces).
            log_path: File to write the probe transcript to.

        Returns:
            The probe outcome; a non-zero exit is data for the caller's
            predicate, not an error.

        Raises:
            RunnerError: If the probe itself times out.
        """
        proc = self._run_shell(
            command,
            log_path,
            expect_exit=None,
            timeout_sec=FAULT_PROBE_TIMEOUT_SEC,
            render=False,
        )
        return ProbeResult(exit_code=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)

    def _await_probe(
        self,
        probe: str,
        check: Callable[[ProbeResult], str | None],
        log_path: Path,
        label: str,
        timeout_sec: float = 15.0,
    ) -> None:
        """Poll a probe until its predicate is satisfied, else fail loudly.

        Args:
            probe: Probe command.
            check: Predicate returning ``None`` when satisfied, or a
                human-readable reason why it is not.
            log_path: File to write the probe transcript to.
            label: Failure headline (``FAULT NOT VERIFIED`` / ``HEAL NOT
                VERIFIED``).
            timeout_sec: How long the post-condition is given to appear.

        Raises:
            RunnerError: If the predicate is still unsatisfied at the deadline.
        """
        deadline = time.monotonic() + timeout_sec * self.verify_retry_scale
        while True:
            reason = check(self._probe(probe, log_path))
            if reason is None:
                return
            if time.monotonic() >= deadline:
                raise RunnerError(f"{label}: {reason}\n$ {probe}")
            time.sleep(self.verify_poll_interval_sec)

    def _container_state_probe(self, target: str, target_kind: str) -> str:
        """Build a probe that prints ``status startedAt restartCount`` for a target."""
        fmt = "{{.State.Status}} {{.State.StartedAt}} {{.RestartCount}}"
        if target_kind == "service":
            return f'docker inspect --format "{fmt}" "$(docker compose ps -aq {target})"'
        return f'docker inspect --format "{fmt}" "{target}"'

    def _capture_container_states(
        self, intent: FaultIntent, step_log: Path
    ) -> dict[str, ContainerRunState | None]:
        """Snapshot container liveness identity before a kill-style command.

        A killed container is restarted by the compose ``unless-stopped``
        policy, so "is it running?" cannot prove the kill landed.  The
        before/after pair of ``StartedAt`` / ``RestartCount`` can.

        Args:
            intent: Classified command intent.
            step_log: Owning step's log path (probe logs derive from it).

        Returns:
            ``{target: state or None}``.
        """
        states: dict[str, ContainerRunState | None] = {}
        for target in intent.targets:
            result = self._probe(
                self._container_state_probe(target, intent.target_kind),
                self._verify_log(step_log, f"prestate-{target}"),
            )
            states[target] = parse_container_runstate(result.stdout)
        return states

    def _note_unverified_fault(self, command: str, step_log: Path, reason: str) -> None:
        """Record that a fault-shaped command's effect could not be verified.

        Args:
            command: The rendered command.
            step_log: Owning step's log path.
            reason: Why verification was not possible.
        """
        entry = f"{reason}: {command}"
        self.unverified_faults.append(entry)
        self.log(f"    [unverified-fault] {entry}")
        self._write_text(
            self._verify_log(step_log, "unverified"),
            f"UNVERIFIED FAULT\nreason: {reason}\n$ {command}\n",
        )

    def _require_container_binary(self, service: str, binary: str, step_log: Path) -> None:
        """Fail loudly when a fault depends on a tool the image does not ship.

        The lookup runs as the same user the fault itself runs as, so it
        resolves against the same ``PATH``: ``tc`` and ``iptables`` live in
        ``/usr/sbin``, and a lookup performed under a different user would not
        prove the fault can find them.

        Args:
            service: Compose service the fault targets.
            binary: Executable the fault needs (``iptables``, ``tc``).
            step_log: Owning step's log path.

        Raises:
            RunnerError: If the binary is absent from the container.
        """
        prefix = container_exec_prefix(service, privileged=True)
        probe = f'{prefix} sh -c "command -v {binary}"'
        result = self._probe(probe, self._verify_log(step_log, f"which-{binary}"))
        if result.exit_code != 0 or not result.stdout.strip():
            raise RunnerError(
                f"FAULT NOT INJECTABLE: '{binary}' is not available in {service}, so this fault "
                f"would be a no-op. Install it in the runtime image (Dockerfile) or drop the step."
            )

    def _verify_iptables_drop(
        self, service: str, src_ip: str, present: bool, step_log: Path
    ) -> None:
        """Assert an INPUT DROP rule for ``src_ip`` is (or is no longer) installed.

        Reading the chain needs ``CAP_NET_ADMIN`` just like writing it, so the
        probe execs as root; as the image's default unprivileged user every read
        fails and a successfully injected rule would be misreported as absent.

        Args:
            service: Compose service whose chain is inspected.
            src_ip: Source address of the rule.
            present: Expected presence of the rule.
            step_log: Owning step's log path.

        Raises:
            RunnerError: If the chain does not match the expectation.
        """
        prefix = container_exec_prefix(service, privileged=True)
        probe = f'{prefix} sh -c "{fp.iptables_rules_cmd()}"'

        def check(result: ProbeResult) -> str | None:
            if result.exit_code != 0 and not result.stdout.strip():
                return f"could not read the INPUT chain on {service}: {result.combined.strip()}"
            found = fp.parse_peer_drop_rule(result.stdout, src_ip)
            if found is present:
                return None
            if present:
                return f"no INPUT DROP rule for {src_ip} on {service} after injection"
            return f"INPUT DROP rule for {src_ip} still present on {service} after heal"

        self._await_probe(
            probe,
            check,
            self._verify_log(step_log, f"iptables-{src_ip}"),
            "FAULT NOT VERIFIED" if present else "HEAL NOT VERIFIED",
            timeout_sec=5.0,
        )

    def _verify_channel_drop(
        self,
        service: str,
        src_ip: str,
        channel: fp.Channel,
        present: bool,
        step_log: Path,
    ) -> None:
        """Assert both direction rules for `channel` are (or are no longer) there.

        On injection this also requires the rules to have *matched packets*. A
        rule that exists but has caught nothing is indistinguishable from no
        partition at all, and the two ways to get there are both real: the
        replication channel needs the rule on the standby with write load in
        flight, and an idle cluster generates no WAL to sever. Failing here with
        that hint is the point — the tempting fix is to stop checking.
        """
        prefix = container_exec_prefix(service, privileged=True)
        probe = f'{prefix} sh -c "{fp.iptables_rules_cmd()}"'

        def check(result: ProbeResult) -> str | None:
            if result.exit_code != 0 and not result.stdout.strip():
                return f"could not read the INPUT chain on {service}: {result.combined.strip()}"
            for from_listener in (False, True):
                found = fp.parse_port_drop_rule(
                    result.stdout, src_ip, channel.port, from_listener=from_listener
                )
                if found is not present:
                    direction = "sport" if from_listener else "dport"
                    verb = "missing after injection" if present else "still present after heal"
                    return (
                        f"{channel} DROP ({direction} {channel.port}) for {src_ip} "
                        f"on {service} {verb}"
                    )
            if not present:
                return None
            matched = fp.parse_port_drop_packets(result.stdout, src_ip, channel.port)
            if matched == 0:
                return (
                    f"{channel} rules on {service} matched 0 packets: the partition "
                    f"exists but severed nothing. {fp.channel_side_hint(channel)}"
                )
            return None

        self._await_probe(
            probe,
            check,
            self._verify_log(step_log, f"channel-{channel}-{src_ip}"),
            "FAULT NOT VERIFIED" if present else "HEAL NOT VERIFIED",
            timeout_sec=20.0,
        )

    def _verify_netem(self, service: str, expected_delay_ms: int | None, step_log: Path) -> None:
        """Assert the eth0 netem qdisc matches expectation.

        Args:
            service: Compose service whose qdisc is inspected.
            expected_delay_ms: Delay the qdisc must report, or ``None`` to
                require that no netem qdisc exists.
            step_log: Owning step's log path.

        Raises:
            RunnerError: If the qdisc does not match.
        """
        # Reading qdiscs succeeds unprivileged, but this execs as root to match
        # the user that installs and removes them: one privilege context for the
        # whole tc lifecycle, so a verification result can never disagree with
        # the injection for a permission reason.
        prefix = container_exec_prefix(service, privileged=True)
        probe = f"{prefix} {fp.read_qdiscs_cmd()}"

        def check(result: ProbeResult) -> str | None:
            state = fp.parse_netem(result.stdout)
            if expected_delay_ms is None:
                if state is not None:
                    return f"netem qdisc still installed on {service}: {result.stdout.strip()}"
                return None
            if state is None:
                return (
                    f"no netem qdisc on {service} after injection: "
                    f"{result.combined.strip() or '<no output>'}"
                )
            tolerance = max(1.0, expected_delay_ms * 0.05)
            if abs(state.delay_ms - expected_delay_ms) > tolerance:
                return (
                    f"netem delay on {service} is {state.delay_ms:g}ms, "
                    f"expected {expected_delay_ms}ms (±{tolerance:g}ms)"
                )
            return None

        self._await_probe(
            probe,
            check,
            self._verify_log(step_log, "tc-qdisc"),
            "FAULT NOT VERIFIED" if expected_delay_ms is not None else "HEAL NOT VERIFIED",
            timeout_sec=5.0,
        )

    def _apply_faketime(self, node_id: int, offset_seconds: int, step_log: Path) -> None:
        """Write a libfaketime offset to a node and verify its clock actually moved.

        Reads the node's current offset and clock, writes the new offset, then
        re-reads both: the observed clock jump must match the change in offset.
        This proves the LD_PRELOAD machinery is live, not just that the file was
        written.

        Args:
            node_id: Target node.
            offset_seconds: New offset in seconds (0 restores real time).
            step_log: Owning step's log path.

        Raises:
            RunnerError: If the file does not hold the new offset, or the node's
                clock did not shift by the expected amount.
        """
        service = f"node{node_id}"
        offset_text = f"{offset_seconds:+d}s"
        exec_prefix = container_exec_prefix(service)
        probe = f'{exec_prefix} sh -c "cat /tmp/faketime; date +%s"'
        before = parse_faketime_probe(
            self._probe(probe, self._verify_log(step_log, "faketime-before")).stdout
        )

        write_cmd = f"{exec_prefix} sh -c \"echo '{offset_text}' > /tmp/faketime\""
        self._run_shell(write_cmd, step_log, timeout_sec=FAULT_PROBE_TIMEOUT_SEC)

        after = parse_faketime_probe(
            self._probe(probe, self._verify_log(step_log, "faketime-after")).stdout
        )
        if after is None:
            raise RunnerError(
                f"FAULT NOT VERIFIED: could not read /tmp/faketime and clock on {service}"
            )
        if after[0] != offset_text:
            raise RunnerError(
                f"FAULT NOT VERIFIED: /tmp/faketime on {service} holds {after[0]!r}, "
                f"expected {offset_text!r}"
            )
        if before is None:
            self._note_unverified_fault(
                write_cmd, step_log, "clock offset before the write was unreadable"
            )
            return
        previous_offset = parse_faketime_offset_seconds(before[0])
        if previous_offset is None:
            self._note_unverified_fault(
                write_cmd,
                step_log,
                f"previous offset {before[0]!r} is not a relative offset, clock shift not checked",
            )
            return
        expected_shift = offset_seconds - previous_offset
        observed_shift = after[1] - before[1]
        # The two probes are separate `docker exec` calls, so real time passes
        # between them; FAKETIME_NO_CACHE=1 makes the offset itself immediate.
        if abs(observed_shift - expected_shift) > FAKETIME_SHIFT_TOLERANCE_SEC:
            raise RunnerError(
                f"FAULT NOT VERIFIED: clock on {service} shifted {observed_shift}s, expected "
                f"{expected_shift}s (±{FAKETIME_SHIFT_TOLERANCE_SEC}s) after writing {offset_text}"
            )

    def _verify_fault_intent(
        self,
        intent: FaultIntent,
        prestate: dict[str, ContainerRunState | None],
        step_log: Path,
    ) -> None:
        """Verify every target of a classified fault (or heal) command.

        Args:
            intent: Classified command intent.
            prestate: Container states captured before the command, empty when
                the kind does not need them.
            step_log: Owning step's log path.

        Raises:
            RunnerError: If any target's post-condition does not hold.
        """
        for target in intent.targets:
            self._verify_fault_target(intent, target, prestate.get(target), step_log)

    def _verify_fault_target(
        self,
        intent: FaultIntent,
        target: str,
        prestate: ContainerRunState | None,
        step_log: Path,
    ) -> None:
        """Verify one target's post-condition for a classified command.

        Args:
            intent: Classified command intent.
            target: Compose service or container name.
            prestate: This target's pre-command state, if captured.
            step_log: Owning step's log path.

        Raises:
            RunnerError: If the post-condition does not hold.
        """
        label = "FAULT NOT VERIFIED" if intent.injects else "HEAL NOT VERIFIED"
        state_probe = self._container_state_probe(target, intent.target_kind)
        log_path = self._verify_log(step_log, f"{intent.kind}-{target}")

        match intent.kind:
            case "pause" | "unpause":
                expected = "paused" if intent.kind == "pause" else "running"

                def check_status(result: ProbeResult) -> str | None:
                    state = parse_container_runstate(result.stdout)
                    if state is None:
                        if not intent.injects:
                            # Nothing to unpause: treat a missing container as
                            # already healed rather than blocking later heals.
                            return None
                        return f"container '{target}' state unreadable: {result.combined.strip()}"
                    if state.status != expected:
                        return f"container '{target}' status is '{state.status}', want '{expected}'"
                    return None

                self._await_probe(state_probe, check_status, log_path, label, timeout_sec=10.0)

            case "compose_stop" | "compose_start" | "compose_restart":
                want_running = intent.kind != "compose_stop"
                probe = "docker compose ps --services --filter status=running"

                def check_running(result: ProbeResult) -> str | None:
                    running = parse_compose_services(result.stdout)
                    if (target in running) is want_running:
                        return None
                    if want_running:
                        return f"service '{target}' is not running after {intent.kind}"
                    return f"service '{target}' is still running after {intent.kind}"

                self._await_probe(probe, check_running, log_path, label, timeout_sec=20.0)

            case "compose_kill" | "pkill_kill":

                def check_restarted(result: ProbeResult) -> str | None:
                    state = parse_container_runstate(result.stdout)
                    if state is None:
                        # The container is gone entirely: the process it ran
                        # certainly is too.
                        return None
                    if state.status != "running":
                        return None
                    if prestate is None:
                        return (
                            f"'{target}' is running and its pre-kill state was unreadable, "
                            f"so the kill cannot be confirmed"
                        )
                    if (
                        state.started_at != prestate.started_at
                        or state.restart_count != prestate.restart_count
                    ):
                        return None
                    return (
                        f"'{target}' never went down: still running since {state.started_at} "
                        f"with restart_count={state.restart_count}"
                    )

                self._await_probe(state_probe, check_restarted, log_path, label, timeout_sec=30.0)

            case "pkill_stop" | "pkill_cont":
                want_stopped = intent.kind == "pkill_stop"
                probe = f"{container_exec_prefix(target)} ps -eo stat=,comm="

                def check_stopped(result: ProbeResult) -> str | None:
                    pairs = parse_ps_stat_comm(result.stdout)
                    if not pairs:
                        return f"could not list processes in '{target}': {result.combined.strip()}"
                    matched = matching_processes(pairs, intent.pattern)
                    stopped = stopped_processes(pairs, intent.pattern)
                    if want_stopped:
                        if stopped:
                            return None
                        return (
                            f"no process matching '{intent.pattern}' in '{target}' is stopped "
                            f"(state T); matched={matched or 'nothing'}"
                        )
                    if stopped:
                        return (
                            f"process matching '{intent.pattern}' in '{target}' is still "
                            f"stopped: {stopped}"
                        )
                    return None

                self._await_probe(probe, check_stopped, log_path, label, timeout_sec=10.0)

            case "network_disconnect" | "network_connect":
                want_attached = intent.kind == "network_connect"
                probe = (
                    f'docker inspect --format "{{{{json .NetworkSettings.Networks}}}}" "{target}"'
                )

                def check_attached(result: ProbeResult) -> str | None:
                    addresses = container_subnet_addresses(result.stdout, _RAFT_SUBNET_PREFIX)
                    if bool(addresses) is want_attached:
                        return None
                    if want_attached:
                        return f"'{target}' has no {_RAFT_SUBNET_PREFIX}x address after reconnect"
                    return f"'{target}' still holds {_RAFT_SUBNET_PREFIX}x address(es) {addresses}"

                self._await_probe(probe, check_attached, log_path, label, timeout_sec=10.0)

            case _:
                self._note_unverified_fault(
                    intent.kind, step_log, "no verification strategy for this fault kind"
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
                # Classify before running: kill-style faults need the target's
                # pre-command liveness identity to prove anything afterwards.
                command = self._render_template(str(step["cmd"]))
                expect_exit = step.get("expect_exit", 0)
                intent = classify_fault_command(command)
                prestate: dict[str, ContainerRunState | None] = {}
                if intent is not None and intent.needs_prestate:
                    prestate = self._capture_container_states(intent, step_log)
                result = self._run_shell(
                    command,
                    step_log,
                    expect_exit=expect_exit,
                    timeout_sec=resolve_shell_timeout(step),
                    render=False,
                )
                if intent is not None:
                    if result.returncode == 0:
                        self._verify_fault_intent(intent, prestate, step_log)
                    else:
                        # The command itself did not run to completion (the case
                        # declared a non-zero expect_exit), so there is no
                        # landed effect to verify.
                        self._note_unverified_fault(
                            command,
                            step_log,
                            f"command exited {result.returncode}, effect not asserted",
                        )
                elif looks_like_fault_injection(command):
                    self._note_unverified_fault(
                        command,
                        step_log,
                        "fault-shaped command is not statically analysable",
                    )

                stdout_contains = step.get("stdout_contains")
                stderr_contains = step.get("stderr_contains")

                for needle in as_strings(stdout_contains):
                    if needle not in result.stdout:
                        raise RunnerError(f"stdout missing expected token '{needle}'")
                for needle in as_strings(stderr_contains):
                    if needle not in result.stderr:
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
                stable_for = float(step.get("stable_for_sec", 0))
                self._wait_for_cluster(
                    expected_nodes=expected_nodes,
                    expected_leaders=expected_leaders,
                    timeout_sec=timeout_sec,
                    leader_not=leader_not,
                    leader_equals=leader_equals,
                    require_replication_health=require_repl,
                    min_healthy_replicas=min_healthy,
                    live_nodes=live_nodes,
                    stable_for_sec=stable_for,
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
                # A node whose metrics port refuses the connection has no
                # process listening, so it is serving no Raft role either and
                # counts as not-leader. Cases here kill nodes on purpose, and
                # treating that as a runner error made the assertion unusable
                # in exactly the scenarios it exists for. Anything else — a
                # timeout, a non-200 — stays fatal: a node that is up but not
                # answering could be a second leader, and this assertion must
                # not pass while blind to one.
                refused: list[int] = []
                count = 0
                for node_id in sorted(self.node_map):
                    try:
                        values = self._poll_metric_values(
                            node_id=node_id, metric_name=leader_metric
                        )
                    except RunnerError as exc:
                        if "refused" not in str(exc).lower():
                            raise
                        refused.append(node_id)
                        continue
                    if not values:
                        raise RunnerError(
                            f"node{node_id} answered without {leader_metric}; the metric "
                            "moved or the exporter is broken, so the leader count is unknown"
                        )
                    observed[node_id] = values[0]
                    if values[0] > 0.5:
                        count += 1
                self._write_text(
                    step_log,
                    json.dumps(
                        {
                            "expected_leader_count": expected_count,
                            "observed_leader_values": observed,
                            "refused_nodes": refused,
                        },
                        indent=2,
                    ),
                )
                if count != expected_count:
                    raise RunnerError(
                        f"Expected {expected_count} leader metric=1 nodes, got {count} "
                        f"(nodes refusing their metrics port: {refused or 'none'})."
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
                self._require_container_binary(node_name, "iptables", step_log)
                prefix = container_exec_prefix(node_name, privileged=True)
                cmd = f"{prefix} {fp.iptables_peer_drop_cmd(src_ip, insert=True)}"
                self._run_shell(cmd, step_log, timeout_sec=resolve_shell_timeout(step))
                self._verify_iptables_drop(node_name, src_ip, present=True, step_log=step_log)

            case StepType.ASYMMETRIC_HEAL:
                node_id = int(step["node"])
                from_id = int(step["from_node"])
                node_name = f"node{node_id}"
                src_ip = _NODE_IPS.get(from_id)
                if not src_ip:
                    raise RunnerError(f"Unknown from_node {from_id} for asymmetric_heal.")
                prefix = container_exec_prefix(node_name, privileged=True)
                cmd = f"{prefix} {fp.iptables_peer_drop_cmd(src_ip, insert=False)}"
                self._run_shell(cmd, step_log, timeout_sec=resolve_shell_timeout(step))
                self._verify_iptables_drop(node_name, src_ip, present=False, step_log=step_log)

            case StepType.CHANNEL_PARTITION | StepType.CHANNEL_HEAL:
                inject = step["type"] == StepType.CHANNEL_PARTITION
                node_id = int(step["node"])
                from_id = int(step["from_node"])
                node_name = f"node{node_id}"
                src_ip = _NODE_IPS.get(from_id)
                if not src_ip:
                    raise RunnerError(f"Unknown from_node {from_id} for {step['type']}.")
                channel = fp.Channel(str(step["channel"]))
                if inject:
                    self._require_container_binary(node_name, "iptables", step_log)
                prefix = container_exec_prefix(node_name, privileged=True)
                # Both directions. Only one side of a TCP channel listens on the
                # service port: requests arrive at the listener with --dport P,
                # replies at the initiator with --sport P and an ephemeral
                # destination. Matching one alone catches nothing whenever the
                # traffic happens to flow the other way.
                for from_listener in (False, True):
                    rule = fp.iptables_port_drop_cmd(
                        src_ip, channel.port, insert=inject, from_listener=from_listener
                    )
                    # Strict add, idempotent delete — the same asymmetry netem
                    # uses. `iptables -D` exits 1 on a rule that is not there,
                    # but "the rule is gone" is the state a heal asks for, and a
                    # cleanup heal after an in-case heal is the normal path. The
                    # verification below still asserts absence, so tolerating the
                    # exit code costs no strictness.
                    shell = (
                        f"{prefix} {rule}"
                        if inject
                        else f'{prefix} sh -c "{rule} 2>/dev/null; true"'
                    )
                    self._run_shell(shell, step_log, timeout_sec=resolve_shell_timeout(step))
                self._verify_channel_drop(
                    node_name, src_ip, channel, present=inject, step_log=step_log
                )

            case StepType.CLOCK_SKEW:
                self._apply_faketime(
                    node_id=int(step["node"]),
                    offset_seconds=int(step.get("seconds", 300)),
                    step_log=step_log,
                )

            case StepType.CLOCK_HEAL:
                self._apply_faketime(
                    node_id=int(step["node"]),
                    offset_seconds=0,
                    step_log=step_log,
                )

            case StepType.WAIT_SYNC:
                check_nodes = as_ints(step.get("nodes")) or list(self.node_map.keys())
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
                self._require_container_binary(node_name, "tc", step_log)
                # No `qdisc del` first: an interface that already carries a root
                # qdisc fails the add with "Exclusivity flag on", which is how a
                # case learns it inherited residue instead of quietly
                # overwriting whatever the previous case left behind.
                prefix = container_exec_prefix(node_name, privileged=True)
                netem = fp.netem_add_cmd(delay_ms=delay_ms, jitter_ms=jitter_ms, loss_pct=0)
                cmd = f"{prefix} {netem}"
                self._run_shell(cmd, step_log, timeout_sec=resolve_shell_timeout(step))
                self._verify_netem(node_name, expected_delay_ms=delay_ms, step_log=step_log)

            case StepType.NETWORK_HEAL:
                node_id = int(step["node"])
                node_name = f"node{node_id}"
                # Healing stays tolerant of an already-clean interface: removing
                # a qdisc that is not there is the state the step asks for.
                inner = f"{fp.netem_del_cmd()} 2>/dev/null; true"
                prefix = container_exec_prefix(node_name, privileged=True)
                cmd = f'{prefix} sh -c "{inner}"'
                self._run_shell(cmd, step_log, timeout_sec=resolve_shell_timeout(step))
                self._verify_netem(node_name, expected_delay_ms=None, step_log=step_log)

            case StepType.PGBENCH:
                self._execute_pgbench_step(step, step_log)

    # -- Phase & case execution ----------------------------------------------

    def _execute_step_list(
        self,
        case_id: str,
        phase: str,
        steps: list[dict[str, Any]],
    ) -> None:
        """Execute a list of steps sequentially, failing fast.

        Used for the ``action`` and ``assert`` phases, where a failed step
        invalidates everything after it.  Cleanup uses :meth:`_run_cleanup`,
        which must not stop at the first failure.

        Args:
            case_id: Owning case ID.
            phase: Phase label (``action`` or ``assert``).
            steps: Ordered list of step dicts.

        Raises:
            RunnerError: Propagated from the first failing step.
        """
        for index, step in enumerate(steps):
            self._execute_step(step, case_id, phase, index)

    def _run_cleanup(self, case_id: str, steps: list[dict[str, Any]]) -> list[str]:
        """Attempt every cleanup step, collecting rather than propagating failures.

        Cleanup steps are independent heals (drop an iptables rule, restore the
        clock, unpause a container, restart a service).  A single failing heal —
        ``iptables -D`` against a container that restarted and lost the rule, for
        instance — must not skip the heals that follow it, or the residue leaks
        into every later case of a ``reuse_cluster`` suite.

        Args:
            case_id: Owning case ID.
            steps: Ordered list of cleanup step dicts.

        Returns:
            One message per failed cleanup step (empty when all succeeded).
        """
        failures: list[str] = []
        for index, step in enumerate(steps):
            try:
                self._execute_step(step, case_id, "cleanup", index)
            except Exception as exc:
                step_type = str(step.get("type", "?"))
                failures.append(f"cleanup step {index:02d} ({step_type}): {exc}")
                self.log(f"  [cleanup-fail:{index:02d}] {step_type}: {exc}")
        return failures

    def _run_case(self, case_id: str) -> bool:
        """Execute a single test case: actions → assertions → cleanup.

        Collects snapshots before, after, and on failure.  Cleanup is attempted
        in full regardless of the action/assertion outcome; a case whose
        assertions passed but whose cleanup failed is recorded as ``ERROR``.

        Args:
            case_id: Case identifier from the matrix.

        Returns:
            ``True`` only if actions, assertions, and cleanup all succeeded.
        """
        case = self.case_map[case_id]
        self.context = {}
        self.log(f"[case] {case_id}: {case.description}")
        case_dir = self._case_dir(case_id)
        self._collect_snapshot(f"{case_id}-before")
        started = time.time()

        passed = False
        detail = ""
        try:
            self._execute_step_list(case_id, "action", case.actions)
            self._execute_step_list(case_id, "assert", case.assertions)
            elapsed = time.time() - started
            passed = True
            detail = f"{elapsed:.1f}s"
            self._write_text(case_dir / "result.txt", f"PASS ({elapsed:.1f}s)\n")
            self.log(f"[pass] {case_id} ({elapsed:.1f}s)")
            self._collect_snapshot(f"{case_id}-after")
        except Exception as exc:
            elapsed = time.time() - started
            self.failed = True
            detail = str(exc)
            error_text = (
                f"FAIL ({elapsed:.1f}s)\n\n{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}"
            )
            self._write_text(case_dir / "result.txt", error_text)
            self._collect_snapshot(f"{case_id}-failed")
            self._collect_failure_logs(case_id)
            self.log(f"[fail] {case_id}: {exc}")
        finally:
            cleanup_failures = self._run_cleanup(case_id, case.cleanup)

        if cleanup_failures:
            # A half-cleaned cluster poisons every later case, so the case is
            # reported as errored and the suite exits non-zero — never a pass.
            self.failed = True
            joined = "\n".join(cleanup_failures)
            self._write_text(case_dir / "cleanup-error.txt", f"{joined}\n")
            self.log(
                f"[cleanup-error] {case_id}: {len(cleanup_failures)} cleanup step(s) failed; "
                f"cluster may hold residue"
            )
        self.summary.append(
            CaseSummary(
                case_id=case_id,
                passed=passed,
                detail=detail,
                cleanup_failures=cleanup_failures,
            )
        )
        return passed and not cleanup_failures

    # -- Summary & top-level execution ---------------------------------------

    def _print_summary(self) -> None:
        """Print a Rich table of case outcomes plus any unverified-fault warnings."""
        self.log("")
        table = Table(title="Scenario Summary", show_lines=False)
        table.add_column("Case")
        table.add_column("Status")
        table.add_column("Detail")
        status_markup = {
            "PASS": "[green]PASS[/]",
            "FAIL": "[red]FAIL[/]",
            "ERROR": "[yellow]ERROR[/]",
        }
        for entry in self.summary:
            detail = entry.detail
            if entry.cleanup_failures:
                joined = "; ".join(entry.cleanup_failures)
                detail = f"{detail} | CLEANUP FAILED: {joined}" if detail else joined
            table.add_row(entry.case_id, status_markup[entry.status], detail)
        self.console.print(table)

        if self.unverified_faults:
            self.console.print(
                f"[yellow bold]{len(self.unverified_faults)} fault(s) ran without effect "
                f"verification:[/]"
            )
            for warning in self.unverified_faults:
                self.log(f"  - {warning}")

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
                for index, case_id in enumerate(self.selected_case_ids):
                    if index > 0:
                        # Convergence barrier: every case starts from a settled
                        # cluster (one leader, all voters, healthy replica
                        # shape), not from whatever churn the previous case's
                        # cleanup left behind — a just-restarted standby can
                        # still be catching up on WAL, and the next case's
                        # first step would race that convergence.
                        self._wait_for_cluster(
                            expected_nodes=self.matrix.cluster.expected_nodes,
                            expected_leaders=1,
                            timeout_sec=self.suite_config.max_wait_cluster_seconds or 90,
                            require_all_voters=True,
                            require_replication_health=True,
                        )
                    if not self._run_case(case_id):
                        self._record_cases_not_reached(case_id)
                        # The cluster is no longer in a state later cases can
                        # start from, so the suite stops — but it says which
                        # cases that cost, because a summary listing only what
                        # ran reads as coverage of the whole suite. This one
                        # hid half the nightly matrix for weeks.
                        break
            finally:
                if cluster_started:
                    # Before teardown: the containers have to still exist for
                    # `docker compose logs` to have anything to count.
                    self._check_log_budget(self.suite_name)
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
                        self._check_log_budget(case_id)
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

        Records suite name, cases executed, per-case status (including cleanup
        failures), unverified faults, and the full case definitions so the exact
        run can be replayed or analysed offline.
        """
        import datetime

        summary_data = {
            "suite": self.suite_name,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "passed": not self.failed,
            "unverified_faults": list(self.unverified_faults),
            "cases": [
                {
                    "case_id": s.case_id,
                    "passed": s.passed,
                    "status": s.status,
                    "detail": s.detail,
                    "cleanup_failures": s.cleanup_failures,
                }
                for s in self.summary
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


def runnable_case_ids(matrix: MatrixConfig, suite: str) -> list[str]:
    """The case ids in `suite` that CI should run, in matrix order.

    Cases carrying a `ci_excluded_reason` are left out. This is what a CI job
    matrix is built from, so the workflow cannot list a different set of cases
    from the suite — the drift that left twelve of seventeen `ha-parallel`
    cases executed by nothing at all.

    Raises:
        RunnerError: If `suite` is not in the matrix, or if nothing in it is
            runnable. `fromJSON([])` gives GitHub Actions zero matrix jobs and
            a green check, so an empty answer must be an error rather than a
            value — the same reason `topology.py` refuses to return no nodes.
    """
    suite_config = matrix.suites.get(suite)
    if suite_config is None:
        known = ", ".join(sorted(matrix.suites))
        raise RunnerError(f"Unknown suite {suite!r}. Known suites: {known}")
    excluded = {case.id for case in matrix.cases if case.ci_excluded_reason}
    ids = [case_id for case_id in suite_config.cases if case_id not in excluded]
    if not ids:
        raise RunnerError(
            f"Suite {suite!r} has no runnable cases ({len(suite_config.cases)} declared, "
            f"all carrying a ci_excluded_reason). An empty case list would run nothing "
            f"and report success."
        )
    return ids


def _print_matrix_listing(matrix_path: Path, console: Console) -> None:
    """Print every suite in a matrix with its cases (no cluster required).

    Args:
        matrix_path: Absolute path to the matrix file.
        console: Rich console for output.

    Raises:
        RunnerError: If the matrix cannot be parsed or fails validation.
    """
    matrix = parse_matrix(matrix_path)
    table = Table(title=f"Suites in {matrix_path.name}", show_lines=False)
    table.add_column("Suite")
    table.add_column("Cases", justify="right")
    table.add_column("reuse_cluster")
    table.add_column("Description")
    for name in sorted(matrix.suites):
        suite_config = matrix.suites[name]
        table.add_row(
            name,
            str(len(suite_config.cases)),
            "yes" if suite_config.reuse_cluster else "no",
            suite_config.description,
        )
    console.print(table)
    console.print(f"{len(matrix.cases)} case definitions loaded")
    for name in sorted(matrix.suites):
        console.print(f"\n[bold]{name}[/]")
        for case_id in matrix.suites[name].cases:
            case = next((c for c in matrix.cases if c.id == case_id), None)
            contracts = ",".join(case.contracts) if case and case.contracts else "-"
            console.print(f"  {case_id} [dim]contracts={contracts}[/]")


@app.command()
def run(
    suite: str = typer.Option(
        "",
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
    list_suites: bool = typer.Option(
        False,
        "--list",
        help="List suites, cases, and declared contracts from the matrix, then exit.",
    ),
    emit_cases: bool = typer.Option(
        False,
        "--emit-cases",
        help="Print --suite's runnable case ids as a JSON array, then exit. "
        "Feeds a CI job matrix so the two lists cannot drift.",
    ),
) -> None:
    """Run a scenario suite against a Docker Compose pgbattery cluster."""
    project_root = Path(__file__).resolve().parent.parent
    matrix_path = (project_root / matrix).resolve()
    resolved_artifact_dir = (project_root / artifact_dir).resolve()

    if emit_cases:
        if not suite:
            console.print("[red]Runner error:[/] --emit-cases requires --suite.")
            raise typer.Exit(code=2)
        try:
            ids = runnable_case_ids(parse_matrix(matrix_path), suite)
        except RunnerError as exc:
            console.print(f"[red]Runner error:[/] {exc}")
            raise typer.Exit(code=1) from exc
        print(json.dumps(ids))
        raise typer.Exit(code=0)

    if list_suites:
        try:
            _print_matrix_listing(matrix_path, console)
        except RunnerError as exc:
            console.print(f"[red]Runner error:[/] {exc}")
            raise typer.Exit(code=1) from exc
        raise typer.Exit(code=0)

    if not suite:
        console.print("[red]Runner error:[/] --suite is required (or pass --list).")
        raise typer.Exit(code=2)

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
