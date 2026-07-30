#!/usr/bin/env -S uv run --project testing python
"""Unit tests for the pgbattery CI runner and matrix linter.

Covers the parts of ``ci_runner.py`` and ``lint_matrix.py`` that must hold
without a cluster: shell-timeout resolution and reporting, cleanup-failure
aggregation, fault-effect verification predicates and command classification,
and the contract-reference policy.

Every test is a module-level ``test_*`` function using plain ``assert``, so this
file runs standalone (``./testing/test_ci_runner_units.py``) and under pytest if
it is available.  No docker, no cluster, no network.

Exit codes:
    0: All tests passed.
    1: One or more tests failed.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ci_runner
import lint_matrix

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MATRIX_PATH = PROJECT_ROOT / "testing" / "ci_matrix.yaml"
CONTRACTS_PATH = PROJECT_ROOT / "docs" / "CONTRACTS.md"


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class StubRunner(ci_runner.CIRunner):
    """A ``CIRunner`` whose docker-touching operations are recorded, not run.

    Attributes:
        executed: ``(phase, index, step_type)`` for every dispatched step.
        fail_steps: ``(phase, index)`` pairs that should raise when dispatched.
        snapshots: Labels passed to :meth:`_collect_snapshot`.
        probe_handler: Optional canned-probe function keyed on the probe
            command; when unset, probing raises to catch unexpected probes.
    """

    # Canned probe output never changes, so retrying it only burns wall clock.
    verify_retry_scale = 0.0
    verify_poll_interval_sec = 0.0

    def __init__(self, artifact_dir: Path) -> None:
        super().__init__(
            project_root=PROJECT_ROOT,
            matrix_path=MATRIX_PATH,
            suite="ha-parallel",
            case_filter=None,
            artifact_dir=artifact_dir,
            build_images=False,
            keep_cluster_on_failure=False,
            console=Console(quiet=True),
        )
        self.executed: list[tuple[str, int, str]] = []
        self.fail_steps: set[tuple[str, int]] = set()
        self.snapshots: list[str] = []
        self.probe_handler: Callable[[str], ci_runner.ProbeResult] | None = None

    def _execute_step(self, step: dict[str, Any], case_id: str, phase: str, index: int) -> None:
        self.executed.append((phase, index, str(step.get("type", "?"))))
        if (phase, index) in self.fail_steps:
            raise ci_runner.RunnerError(f"stub failure in {phase} step {index}")

    def _collect_snapshot(self, label: str) -> None:
        self.snapshots.append(label)

    def _collect_failure_logs(self, case_id: str) -> None:
        return None

    def _probe(self, command: str, log_path: Path) -> ci_runner.ProbeResult:
        if self.probe_handler is None:
            raise AssertionError(f"unexpected probe: {command}")
        return self.probe_handler(command)


class DispatchRunner(StubRunner):
    """Stub that dispatches steps for real while spawning no shell at all.

    Attributes:
        shell_commands: ``(command, timeout_sec)`` for every shell invocation.
        verified: ``(intent, prestate)`` for every fault verification triggered.
    """

    def __init__(self, artifact_dir: Path) -> None:
        super().__init__(artifact_dir)
        self.shell_commands: list[tuple[str, int]] = []
        self.verified: list[tuple[ci_runner.FaultIntent, dict[str, Any]]] = []

    def _execute_step(self, step: dict[str, Any], case_id: str, phase: str, index: int) -> None:
        ci_runner.CIRunner._execute_step(self, step, case_id, phase, index)

    def _run_shell(
        self,
        command: str,
        log_path: Path,
        expect_exit: int | list[int] | None = 0,
        timeout_sec: int = ci_runner.DEFAULT_SHELL_TIMEOUT_SEC,
        render: bool = True,
        stdin_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.shell_commands.append((command, timeout_sec))
        self._write_text(log_path, command)
        return subprocess.CompletedProcess(args=command, returncode=0, stdout="", stderr="")

    def _verify_fault_intent(
        self,
        intent: ci_runner.FaultIntent,
        prestate: dict[str, ci_runner.ContainerRunState | None],
        step_log: Path,
    ) -> None:
        self.verified.append((intent, dict(prestate)))


def make_runner() -> StubRunner:
    """Build a stub runner writing artifacts to a throwaway directory."""
    return StubRunner(Path(tempfile.mkdtemp(prefix="ci-runner-units-")))


def make_dispatch_runner() -> DispatchRunner:
    """Build a step-dispatching stub runner writing to a throwaway directory."""
    return DispatchRunner(Path(tempfile.mkdtemp(prefix="ci-runner-dispatch-")))


def probe(stdout: str = "", exit_code: int = 0, stderr: str = "") -> ci_runner.ProbeResult:
    """Build a canned probe result."""
    return ci_runner.ProbeResult(exit_code=exit_code, stdout=stdout, stderr=stderr)


def assert_raises(
    exc_type: type[BaseException], fn: Callable[[], object], needle: str = ""
) -> BaseException:
    """Assert ``fn`` raises ``exc_type`` whose message contains ``needle``."""
    try:
        fn()
    except exc_type as exc:
        assert needle in str(exc), f"expected {needle!r} in {str(exc)!r}"
        return exc
    raise AssertionError(f"expected {exc_type.__name__} (needle={needle!r}) but nothing was raised")


# ---------------------------------------------------------------------------
# Defect 1 — shell timeouts
# ---------------------------------------------------------------------------


def test_default_shell_timeout_is_finite_and_generous() -> None:
    assert 60 <= ci_runner.DEFAULT_SHELL_TIMEOUT_SEC <= 1800
    assert ci_runner.CLUSTER_LIFECYCLE_TIMEOUT_SEC >= ci_runner.DEFAULT_SHELL_TIMEOUT_SEC


def test_resolve_shell_timeout_uses_default_when_unset() -> None:
    assert ci_runner.resolve_shell_timeout({"type": "cmd"}) == ci_runner.DEFAULT_SHELL_TIMEOUT_SEC
    assert ci_runner.resolve_shell_timeout({"type": "cmd"}, default=42) == 42


def test_resolve_shell_timeout_honours_per_step_override() -> None:
    assert ci_runner.resolve_shell_timeout({"shell_timeout_sec": 30}) == 30
    # `timeout_sec` is the accepted alias for steps that do not use it for polling.
    assert ci_runner.resolve_shell_timeout({"timeout_sec": 45}) == 45
    # The canonical key wins when both are present.
    assert ci_runner.resolve_shell_timeout({"shell_timeout_sec": 5, "timeout_sec": 900}) == 5
    # JSON numbers arrive as floats.
    assert ci_runner.resolve_shell_timeout({"shell_timeout_sec": 90.0}) == 90


def test_resolve_shell_timeout_rejects_nonsense() -> None:
    def resolving(value: Any) -> Callable[[], int]:
        return lambda: ci_runner.resolve_shell_timeout({"shell_timeout_sec": value})

    for bad in [0, -1, True, "30", 1.5, object()]:
        assert_raises(ci_runner.RunnerError, resolving(bad), "shell_timeout_sec")


def test_case_config_rejects_bad_step_timeout_at_parse_time() -> None:
    assert_raises(
        ValidationError,
        lambda: ci_runner.CaseConfig.model_validate(
            {"id": "c", "actions": [{"type": "cmd", "cmd": "true", "shell_timeout_sec": -5}]}
        ),
        "must be > 0",
    )
    good = ci_runner.CaseConfig.model_validate(
        {"id": "c", "actions": [{"type": "cmd", "cmd": "true", "shell_timeout_sec": 30}]}
    )
    assert good.actions[0]["shell_timeout_sec"] == 30


def test_case_config_rejects_step_without_type() -> None:
    assert_raises(
        ValidationError,
        lambda: ci_runner.CaseConfig.model_validate({"id": "c", "actions": [{"cmd": "true"}]}),
        "missing a non-empty 'type'",
    )


def test_format_timeout_failure_is_loud_and_keeps_partial_output() -> None:
    message = ci_runner.format_timeout_failure(
        "docker compose exec -T node1 psql", 600, "partial-stdout", None
    )
    assert message.startswith("STEP TIMEOUT: command exceeded 600s and was killed")
    assert "docker compose exec -T node1 psql" in message
    assert "partial-stdout" in message
    assert "--- partial stderr ---" in message
    assert "<empty>" in message


def test_tail_text_truncates_but_keeps_the_end() -> None:
    assert ci_runner.tail_text(None) == "<empty>"
    assert ci_runner.tail_text("short", limit=10) == "short"
    tailed = ci_runner.tail_text("a" * 50 + "END", limit=5)
    assert tailed.endswith("aaEND")
    assert "truncated 48 chars" in tailed


def test_run_shell_timeout_raises_even_when_exit_code_is_ignored() -> None:
    runner = make_runner()
    log_path = runner.artifact_dir / "timeout.log"
    # expect_exit=None means "any exit code is fine" — a hang must still fail.
    exc = assert_raises(
        ci_runner.RunnerError,
        lambda: runner._run_shell("sleep 30", log_path, expect_exit=None, timeout_sec=1),
        "STEP TIMEOUT",
    )
    assert "sleep 30" in str(exc)
    assert "STEP TIMEOUT" in log_path.read_text(encoding="utf-8")


def test_run_shell_timeout_reports_partial_output() -> None:
    runner = make_runner()
    # `exec` makes the sleeping process the one we kill, so its inherited pipe
    # closes and the already-written output can be drained.
    exc = assert_raises(
        ci_runner.RunnerError,
        lambda: runner._run_shell(
            "printf 'wrote-this-before-hanging\\n'; exec sleep 30",
            runner.artifact_dir / "timeout-partial.log",
            expect_exit=None,
            timeout_sec=1,
        ),
        "STEP TIMEOUT",
    )
    message = str(exc)
    partial_section = message.split("--- partial stdout ---", 1)[1]
    assert "wrote-this-before-hanging" in partial_section


def test_run_shell_still_returns_normally_within_budget() -> None:
    runner = make_runner()
    result = runner._run_shell(
        "echo ok", runner.artifact_dir / "ok.log", expect_exit=0, timeout_sec=30
    )
    assert result.stdout.strip() == "ok"


def test_run_shell_skips_template_rendering_for_probes() -> None:
    runner = make_runner()
    # Go template braces must not be mistaken for {{ var }} placeholders.
    result = runner._run_shell(
        'echo "{{end}}"', runner.artifact_dir / "probe.log", timeout_sec=30, render=False
    )
    assert result.stdout.strip() == "{{end}}"
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._run_shell(
            'echo "{{end}}"', runner.artifact_dir / "probe2.log", timeout_sec=30
        ),
        "Template variable 'end' is not defined",
    )


def test_stdin_command_delivers_input_and_still_times_out() -> None:
    # The sql step pipes a .sql file through psql on stdin; that path is the
    # same _run_shell, so it inherits the timeout instead of hanging forever.
    runner = make_runner()
    result = runner._run_shell(
        "cat", runner.artifact_dir / "stdin.log", timeout_sec=30, stdin_text="SELECT 1;\n"
    )
    assert result.stdout == "SELECT 1;\n"
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._run_shell(
            "cat > /dev/null; exec sleep 30",
            runner.artifact_dir / "stdin-timeout.log",
            expect_exit=None,
            timeout_sec=1,
            stdin_text="SELECT pg_sleep(600);\n",
        ),
        "STEP TIMEOUT",
    )


# ---------------------------------------------------------------------------
# Defect 2 — cleanup must attempt every step
# ---------------------------------------------------------------------------


def _cleanup_steps() -> list[dict[str, Any]]:
    return [
        {"type": "cmd", "cmd": "iptables -D INPUT ..."},
        {"type": "clock_heal", "node": 1},
        {"type": "network_heal", "node": 2},
        {"type": "cmd", "cmd": "docker unpause pgbattery-node1-1"},
    ]


def test_run_cleanup_attempts_every_step_after_a_failure() -> None:
    runner = make_runner()
    runner.fail_steps = {("cleanup", 0)}
    failures = runner._run_cleanup("case-x", _cleanup_steps())
    attempted = [index for phase, index, _ in runner.executed if phase == "cleanup"]
    assert attempted == [0, 1, 2, 3], f"heal steps were skipped: {attempted}"
    assert len(failures) == 1
    assert "cleanup step 00 (cmd)" in failures[0]


def test_run_cleanup_aggregates_multiple_failures() -> None:
    runner = make_runner()
    runner.fail_steps = {("cleanup", 0), ("cleanup", 2)}
    failures = runner._run_cleanup("case-x", _cleanup_steps())
    assert len(runner.executed) == 4
    assert len(failures) == 2
    assert "cleanup step 02 (network_heal)" in failures[1]


def test_run_cleanup_returns_empty_on_success() -> None:
    runner = make_runner()
    assert runner._run_cleanup("case-x", _cleanup_steps()) == []


def test_case_with_failing_cleanup_is_errored_not_passed() -> None:
    runner = make_runner()
    case = ci_runner.CaseConfig.model_validate(
        {
            "id": "cleanup-residue",
            "actions": [{"type": "cmd", "cmd": "true"}],
            "assertions": [{"type": "cmd", "cmd": "true"}],
            "cleanup": _cleanup_steps(),
        }
    )
    runner.case_map[case.id] = case
    runner.fail_steps = {("cleanup", 0)}

    passed = runner._run_case(case.id)

    assert passed is False, "a case whose cleanup failed must not report success"
    assert runner.failed is True, "a cleanup failure must make the suite exit non-zero"
    entry = runner.summary[-1]
    assert entry.status == "ERROR"
    assert entry.passed is True, "assertions did pass; the cleanup is what failed"
    assert len(entry.cleanup_failures) == 1
    cleanup_error = runner._case_dir(case.id) / "cleanup-error.txt"
    assert "cleanup step 00" in cleanup_error.read_text(encoding="utf-8")


def test_case_with_clean_cleanup_passes() -> None:
    runner = make_runner()
    case = ci_runner.CaseConfig.model_validate(
        {
            "id": "cleanup-ok",
            "assertions": [{"type": "cmd", "cmd": "true"}],
            "cleanup": _cleanup_steps(),
        }
    )
    runner.case_map[case.id] = case
    assert runner._run_case(case.id) is True
    assert runner.summary[-1].status == "PASS"
    assert runner.failed is False


def test_failing_assertion_still_runs_all_cleanup() -> None:
    runner = make_runner()
    case = ci_runner.CaseConfig.model_validate(
        {
            "id": "assert-fails",
            "assertions": [{"type": "cmd", "cmd": "false"}],
            "cleanup": _cleanup_steps(),
        }
    )
    runner.case_map[case.id] = case
    runner.fail_steps = {("assert", 0)}
    assert runner._run_case(case.id) is False
    cleanup_indices = [index for phase, index, _ in runner.executed if phase == "cleanup"]
    assert cleanup_indices == [0, 1, 2, 3]
    assert runner.summary[-1].status == "FAIL"


# ---------------------------------------------------------------------------
# Defect 3 — fault-effect verification
# ---------------------------------------------------------------------------

IPTABLES_WITH_RULE = """-P INPUT ACCEPT
-A INPUT -s 172.28.0.12/32 -j DROP
--- counters ---
Chain INPUT (policy ACCEPT 12 packets, 900 bytes)
 pkts bytes target     prot opt in     out     source               destination
   17  1020 DROP       all  --  *      *       172.28.0.12          0.0.0.0/0
"""

IPTABLES_EMPTY = """-P INPUT ACCEPT
--- counters ---
Chain INPUT (policy ACCEPT 3 packets, 180 bytes)
 pkts bytes target     prot opt in     out     source               destination
"""


def test_iptables_drop_rule_detection() -> None:
    assert ci_runner.iptables_drop_rule_present(IPTABLES_WITH_RULE, "172.28.0.12") is True
    assert ci_runner.iptables_drop_rule_present(IPTABLES_WITH_RULE, "172.28.0.13") is False
    assert ci_runner.iptables_drop_rule_present(IPTABLES_EMPTY, "172.28.0.12") is False
    # Bare address without /32 (as the runner writes it) must also match.
    assert (
        ci_runner.iptables_drop_rule_present("-A INPUT -s 172.28.0.13 -j DROP", "172.28.0.13")
        is True
    )
    # An ACCEPT rule for the same source is not a partition.
    assert (
        ci_runner.iptables_drop_rule_present("-A INPUT -s 172.28.0.13 -j ACCEPT", "172.28.0.13")
        is False
    )


def test_verify_iptables_drop_fails_when_rule_absent() -> None:
    runner = make_runner()
    runner.probe_handler = lambda _command: probe(IPTABLES_EMPTY)
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_iptables_drop(
            "node1", "172.28.0.12", present=True, step_log=runner.artifact_dir / "s.log"
        ),
        "FAULT NOT VERIFIED",
    )
    runner.probe_handler = lambda _command: probe(IPTABLES_WITH_RULE)
    runner._verify_iptables_drop(
        "node1", "172.28.0.12", present=True, step_log=runner.artifact_dir / "s.log"
    )


def test_verify_iptables_heal_fails_when_rule_survives() -> None:
    runner = make_runner()
    runner.probe_handler = lambda _command: probe(IPTABLES_WITH_RULE)
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_iptables_drop(
            "node1", "172.28.0.12", present=False, step_log=runner.artifact_dir / "s.log"
        ),
        "HEAL NOT VERIFIED",
    )


def test_require_container_binary_names_the_missing_tool() -> None:
    runner = make_runner()
    runner.probe_handler = lambda _command: probe("", exit_code=127, stderr="not found")
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._require_container_binary(
            "node1", "iptables", runner.artifact_dir / "s.log"
        ),
        "FAULT NOT INJECTABLE: 'iptables' is not available in node1",
    )
    runner.probe_handler = lambda _command: probe("/usr/sbin/iptables\n")
    runner._require_container_binary("node1", "iptables", runner.artifact_dir / "s.log")


NETEM_SHOW = "qdisc netem 8001: root refcnt 2 limit 1000 delay 200ms  50ms\n"
NETEM_CLEAN = "qdisc noqueue 0: root refcnt 2 \n"


def test_netem_parsing() -> None:
    assert ci_runner.netem_present(NETEM_SHOW) is True
    assert ci_runner.netem_present(NETEM_CLEAN) is False
    assert ci_runner.parse_netem_delay_ms(NETEM_SHOW) == 200.0
    assert ci_runner.parse_netem_delay_ms(NETEM_CLEAN) is None
    # Fractional and non-ms units.
    assert ci_runner.parse_netem_delay_ms("qdisc netem 1: root delay 149.9ms 40ms") == 149.9
    assert ci_runner.parse_netem_delay_ms("qdisc netem 1: root delay 1s") == 1000.0
    assert ci_runner.parse_netem_delay_ms("qdisc netem 1: root delay 500us") == 0.5
    # netem installed but with loss only: no delay to report.
    assert ci_runner.parse_netem_delay_ms("qdisc netem 1: root loss 30%") is None


def test_verify_netem_requires_the_expected_delay() -> None:
    runner = make_runner()
    runner.probe_handler = lambda _command: probe(NETEM_CLEAN)
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_netem("node2", 150, runner.artifact_dir / "s.log"),
        "no netem delay on node2 after injection",
    )
    runner.probe_handler = lambda _command: probe(NETEM_SHOW)
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_netem("node2", 150, runner.artifact_dir / "s.log"),
        "netem delay on node2 is 200ms, expected 150ms",
    )
    runner._verify_netem("node2", 200, runner.artifact_dir / "s.log")


def test_verify_netem_heal_requires_the_qdisc_to_be_gone() -> None:
    runner = make_runner()
    runner.probe_handler = lambda _command: probe(NETEM_SHOW)
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_netem("node2", None, runner.artifact_dir / "s.log"),
        "netem qdisc still installed on node2",
    )
    runner.probe_handler = lambda _command: probe(NETEM_CLEAN)
    runner._verify_netem("node2", None, runner.artifact_dir / "s.log")


PS_OUTPUT_RUNNING = """Ss   tini
Sl   pgbattery
Ss   postgres
S    psql
"""
PS_OUTPUT_STOPPED = """Ss   tini
Tl   pgbattery
Ss   postgres
"""


def test_ps_stat_comm_parsing_and_stopped_detection() -> None:
    pairs = ci_runner.parse_ps_stat_comm(PS_OUTPUT_RUNNING)
    assert ("Sl", "pgbattery") in pairs
    assert len(pairs) == 4
    assert ci_runner.stopped_processes(pairs, "pgbattery") == []
    stopped = ci_runner.stopped_processes(
        ci_runner.parse_ps_stat_comm(PS_OUTPUT_STOPPED), "pgbattery"
    )
    assert stopped == [("Tl", "pgbattery")]
    # A pattern that no longer matches after a rename finds nothing at all.
    assert ci_runner.matching_processes(pairs, "pgbattery-server") == []
    # Invalid regex degrades to substring matching instead of blowing up.
    assert ci_runner.matching_processes(pairs, "pgbattery[") == []


def test_verify_sigstop_requires_a_stopped_process() -> None:
    runner = make_runner()
    intent = ci_runner.classify_fault_command("docker compose exec -T node1 pkill -STOP pgbattery")
    assert intent is not None
    runner.probe_handler = lambda _command: probe(PS_OUTPUT_RUNNING)
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_fault_target(intent, "node1", None, runner.artifact_dir / "s.log"),
        "no process matching 'pgbattery' in 'node1' is stopped",
    )
    runner.probe_handler = lambda _command: probe(PS_OUTPUT_STOPPED)
    runner._verify_fault_target(intent, "node1", None, runner.artifact_dir / "s.log")


def test_verify_sigcont_requires_no_stopped_process() -> None:
    runner = make_runner()
    intent = ci_runner.classify_fault_command("docker compose exec -T node1 pkill -CONT pgbattery")
    assert intent is not None
    runner.probe_handler = lambda _command: probe(PS_OUTPUT_STOPPED)
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_fault_target(intent, "node1", None, runner.artifact_dir / "s.log"),
        "is still stopped",
    )
    runner.probe_handler = lambda _command: probe(PS_OUTPUT_RUNNING)
    runner._verify_fault_target(intent, "node1", None, runner.artifact_dir / "s.log")


def test_container_runstate_parsing() -> None:
    state = ci_runner.parse_container_runstate("running 2026-07-29T22:58:46.034460092Z 0\n")
    assert state == ci_runner.ContainerRunState("running", "2026-07-29T22:58:46.034460092Z", 0)
    assert ci_runner.parse_container_runstate("") is None
    assert ci_runner.parse_container_runstate("Error: No such object: node9\n") is None


def test_verify_pause_requires_paused_status() -> None:
    runner = make_runner()
    intent = ci_runner.classify_fault_command("docker pause pgbattery-node2-1")
    assert intent is not None
    assert intent.kind == "pause"
    runner.probe_handler = lambda _command: probe("running 2026-07-29T22:58:46Z 0")
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_fault_target(
            intent, "pgbattery-node2-1", None, runner.artifact_dir / "s.log"
        ),
        "status is 'running', want 'paused'",
    )
    runner.probe_handler = lambda _command: probe("paused 2026-07-29T22:58:46Z 0")
    runner._verify_fault_target(intent, "pgbattery-node2-1", None, runner.artifact_dir / "s.log")


def test_verify_pause_fails_when_container_is_missing() -> None:
    runner = make_runner()
    intent = ci_runner.classify_fault_command("docker pause pgbattery-node2-1")
    assert intent is not None
    runner.probe_handler = lambda _command: probe("", exit_code=1, stderr="No such object")
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_fault_target(
            intent, "pgbattery-node2-1", None, runner.artifact_dir / "s.log"
        ),
        "state unreadable",
    )


def test_verify_unpause_tolerates_a_missing_container() -> None:
    runner = make_runner()
    intent = ci_runner.classify_fault_command("docker unpause pgbattery-node2-1")
    assert intent is not None
    assert intent.injects is False
    runner.probe_handler = lambda _command: probe("", exit_code=1, stderr="No such object")
    # Nothing to unpause is already healed; blocking here would skip later heals.
    runner._verify_fault_target(intent, "pgbattery-node2-1", None, runner.artifact_dir / "s.log")


def test_verify_kill_detects_a_no_op_kill() -> None:
    runner = make_runner()
    intent = ci_runner.classify_fault_command("docker compose kill node1")
    assert intent is not None
    assert intent.needs_prestate is True
    prestate = ci_runner.ContainerRunState("running", "2026-07-29T22:58:46Z", 0)
    # Same incarnation, same restart count: the container never went down.
    runner.probe_handler = lambda _command: probe("running 2026-07-29T22:58:46Z 0")
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_fault_target(
            intent, "node1", prestate, runner.artifact_dir / "s.log"
        ),
        "never went down",
    )
    # Restarted by the compose restart policy: the kill landed.
    runner.probe_handler = lambda _command: probe("running 2026-07-29T23:04:10Z 1")
    runner._verify_fault_target(intent, "node1", prestate, runner.artifact_dir / "s.log")
    # Still down: also proof the kill landed.
    runner.probe_handler = lambda _command: probe("exited 2026-07-29T22:58:46Z 0")
    runner._verify_fault_target(intent, "node1", prestate, runner.artifact_dir / "s.log")


def test_verify_compose_stop_and_start() -> None:
    runner = make_runner()
    stop = ci_runner.classify_fault_command("docker compose stop node3")
    start = ci_runner.classify_fault_command("docker compose start node3")
    assert stop is not None and start is not None
    runner.probe_handler = lambda _command: probe("node1\nnode2\nnode3\n")
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_fault_target(stop, "node3", None, runner.artifact_dir / "s.log"),
        "still running after compose_stop",
    )
    runner._verify_fault_target(start, "node3", None, runner.artifact_dir / "s.log")
    runner.probe_handler = lambda _command: probe("node1\nnode2\n")
    runner._verify_fault_target(stop, "node3", None, runner.artifact_dir / "s.log")
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_fault_target(start, "node3", None, runner.artifact_dir / "s.log"),
        "not running after compose_start",
    )


def test_compose_services_parsing_ignores_noise() -> None:
    assert ci_runner.parse_compose_services("node1\nnode2\n\n") == {"node1", "node2"}
    assert ci_runner.parse_compose_services('time=... msg="a warning"\nnode1\n') == {"node1"}
    assert ci_runner.parse_compose_services("") == set()


NETWORKS_ATTACHED = '{"pgbattery_raft_net":{"IPAddress":"172.28.0.11","Gateway":"172.28.0.1"}}'
NETWORKS_DETACHED = '{"bridge":{"IPAddress":"10.0.0.5"}}'


def test_container_subnet_addresses() -> None:
    assert ci_runner.container_subnet_addresses(NETWORKS_ATTACHED, "172.28.") == ["172.28.0.11"]
    assert ci_runner.container_subnet_addresses(NETWORKS_DETACHED, "172.28.") == []
    assert ci_runner.container_subnet_addresses("null", "172.28.") == []
    assert ci_runner.container_subnet_addresses("not json", "172.28.") == []


def test_verify_network_disconnect() -> None:
    runner = make_runner()
    intent = ci_runner.classify_fault_command(
        'docker network disconnect "pgbattery_raft_net" "pgbattery-node1-1"'
    )
    assert intent is not None
    assert intent.kind == "network_disconnect"
    assert intent.targets == ("pgbattery-node1-1",)
    runner.probe_handler = lambda _command: probe(NETWORKS_ATTACHED)
    assert_raises(
        ci_runner.RunnerError,
        lambda: runner._verify_fault_target(
            intent, "pgbattery-node1-1", None, runner.artifact_dir / "s.log"
        ),
        "still holds 172.28.x address",
    )
    runner.probe_handler = lambda _command: probe(NETWORKS_DETACHED)
    runner._verify_fault_target(intent, "pgbattery-node1-1", None, runner.artifact_dir / "s.log")


def test_faketime_probe_parsing() -> None:
    assert ci_runner.parse_faketime_probe("+300s\n1785000000\n") == ("+300s", 1785000000)
    assert ci_runner.parse_faketime_probe("+0s\n") is None
    assert ci_runner.parse_faketime_probe("") is None
    assert ci_runner.parse_faketime_offset_seconds("+300s") == 300
    assert ci_runner.parse_faketime_offset_seconds("-45s") == -45
    assert ci_runner.parse_faketime_offset_seconds("+0s") == 0
    assert ci_runner.parse_faketime_offset_seconds("+2m") == 120
    assert ci_runner.parse_faketime_offset_seconds("@2020-01-01 00:00:00") is None


def test_classify_fault_command_recognises_simple_faults() -> None:
    cases: list[tuple[str, str, tuple[str, ...], bool]] = [
        ("docker pause pgbattery-node2-1", "pause", ("pgbattery-node2-1",), True),
        (
            "docker unpause pgbattery-node1-1 pgbattery-node2-1",
            "unpause",
            ("pgbattery-node1-1", "pgbattery-node2-1"),
            False,
        ),
        ("docker compose stop node2 node3", "compose_stop", ("node2", "node3"), True),
        ("docker compose kill node1", "compose_kill", ("node1",), True),
        ("docker compose start node3", "compose_start", ("node3",), False),
        ("docker compose restart node2 node3", "compose_restart", ("node2", "node3"), False),
        (
            "docker compose exec -T node1 pkill -9 pgbattery",
            "pkill_kill",
            ("node1",),
            True,
        ),
        (
            "docker compose exec -T node1 pkill -STOP pgbattery",
            "pkill_stop",
            ("node1",),
            True,
        ),
        (
            "docker compose exec -T node1 pkill -CONT pgbattery",
            "pkill_cont",
            ("node1",),
            False,
        ),
    ]
    for command, kind, targets, injects in cases:
        intent = ci_runner.classify_fault_command(command)
        assert intent is not None, f"unclassified: {command}"
        assert intent.kind == kind, f"{command} -> {intent.kind}"
        assert intent.targets == targets, f"{command} -> {intent.targets}"
        assert intent.injects is injects, f"{command} -> injects={intent.injects}"


def test_classify_fault_command_skips_what_it_cannot_reason_about() -> None:
    unclassifiable = [
        # Dynamic target: only the shell knows which node this kills.
        "L=$(curl -s http://localhost:9081/api/v1/cluster/leader); docker compose kill node$L",
        # Compound heal with error suppression.
        "docker compose exec -T node1 iptables -D INPUT -s 172.28.0.12 -j DROP 2>/dev/null || true",
        # Detached exec: nothing to observe synchronously.
        'docker compose exec -T -d node1 sh -c "pkill -9 pgbattery"',
        # -f matches the full command line, not the comm we can read from ps.
        "docker compose exec -T node1 pkill -9 -f pgbattery",
        # Not a docker command at all.
        "psql -c 'SELECT 1'",
        # Unrelated docker subcommand.
        "docker compose ps",
    ]
    for command in unclassifiable:
        assert ci_runner.classify_fault_command(command) is None, f"classified: {command}"


def test_unclassifiable_fault_commands_are_flagged_as_faults() -> None:
    flagged = [
        "L=$(curl -s http://localhost:9081/api/v1/cluster/leader); docker compose kill node$L",
        'docker compose exec -T -d node1 sh -c "pkill -9 pgbattery"',
        "docker compose exec -T node1 pkill -9 -f '^pgbattery'",
        "docker compose exec -T node2 /usr/lib/postgresql/18/bin/pg_ctl promote -D /data",
        "docker compose run --rm node3 sh -c 'dd if=/dev/urandom of=/data/pg_control bs=1'",
    ]
    for command in flagged:
        assert ci_runner.looks_like_fault_injection(command) is True, f"not flagged: {command}"
    # Heals and reads are not fault injection, so they must not warn.
    for command in [
        "docker compose start node1 2>/dev/null || true",
        "docker compose exec -T node1 iptables -D INPUT -s 172.28.0.12 -j DROP || true",
        "docker unpause pgbattery-node1-1 2>/dev/null || true",
        "docker compose exec -T node1 tc qdisc del dev eth0 root",
    ]:
        assert ci_runner.looks_like_fault_injection(command) is False, f"false positive: {command}"


def test_cmd_step_renders_before_classifying_and_bounds_the_timeout() -> None:
    runner = make_dispatch_runner()
    runner.context["second_leader"] = 2
    runner._execute_step(
        {
            "type": "cmd",
            "cmd": "docker pause pgbattery-node{{ second_leader }}-1",
            "shell_timeout_sec": 42,
        },
        "case",
        "action",
        0,
    )
    assert runner.shell_commands == [("docker pause pgbattery-node2-1", 42)]
    intent, prestate = runner.verified[0]
    assert intent.kind == "pause"
    assert intent.targets == ("pgbattery-node2-1",)
    assert prestate == {}


def test_cmd_step_uses_the_default_timeout_when_unset() -> None:
    runner = make_dispatch_runner()
    runner._execute_step({"type": "cmd", "cmd": "echo hello"}, "case", "action", 0)
    assert runner.shell_commands == [("echo hello", ci_runner.DEFAULT_SHELL_TIMEOUT_SEC)]
    assert runner.verified == []
    assert runner.unverified_faults == []


def test_cmd_step_captures_prestate_for_kill_style_faults() -> None:
    runner = make_dispatch_runner()
    runner.probe_handler = lambda _command: probe("running 2026-07-29T22:58:46Z 0")
    runner._execute_step({"type": "cmd", "cmd": "docker compose kill node1"}, "case", "action", 0)
    intent, prestate = runner.verified[0]
    assert intent.kind == "compose_kill"
    assert prestate["node1"] == ci_runner.ContainerRunState("running", "2026-07-29T22:58:46Z", 0)


def test_cmd_step_warns_when_a_fault_cannot_be_verified() -> None:
    runner = make_dispatch_runner()
    runner._execute_step(
        {
            "type": "cmd",
            "cmd": "L=$(curl -s http://localhost:9081/leader); docker compose kill node$L",
        },
        "case",
        "action",
        0,
    )
    assert runner.verified == []
    assert len(runner.unverified_faults) == 1
    assert "not statically analysable" in runner.unverified_faults[0]


def test_note_unverified_fault_records_and_surfaces() -> None:
    runner = make_runner()
    runner._note_unverified_fault(
        "docker compose kill node$L", runner.artifact_dir / "s.log", "not statically analysable"
    )
    assert len(runner.unverified_faults) == 1
    assert "not statically analysable" in runner.unverified_faults[0]


# ---------------------------------------------------------------------------
# Defect 4 — contract-to-test policy
# ---------------------------------------------------------------------------

CONTRACTS_DOC_SAMPLE = """# Contracts

### W1 — ACKed Write Durability (FATAL)

Text.

### L2 — Lease-Fenced Write Rejection (FATAL)

Text.

## Contract-to-Test Index

| Contract                          | Severity |
| --------------------------------- | -------- |
| W1                                | FATAL    |
| Linearizability (single-register) | FATAL    |
"""


def test_extract_contract_ids_reads_headings_only() -> None:
    assert lint_matrix.extract_contract_ids(CONTRACTS_DOC_SAMPLE) == {"W1", "L2"}


def test_extract_contract_ids_from_the_real_doc() -> None:
    ids = lint_matrix.extract_contract_ids(CONTRACTS_PATH.read_text(encoding="utf-8"))
    assert {"W1", "W2", "W3", "L1", "L2", "L3", "V1", "V2", "S1", "S2", "R1", "R2"} <= ids


def test_contract_violations_flag_missing_declarations() -> None:
    violations = lint_matrix.collect_contract_violations(
        [{"id": "a", "contracts": ["W1"]}, {"id": "b"}, {"id": "c", "contracts": []}],
        {"W1", "L2"},
    )
    assert len(violations) == 1
    assert "2 case(s) declare no contracts" in violations[0]
    assert "b, c" in violations[0]
    assert "testing/ci_matrix.yaml" in violations[0]


def test_contract_violations_flag_unknown_ids() -> None:
    violations = lint_matrix.collect_contract_violations(
        [{"id": "a", "contracts": ["W1", "ZZ9"]}], {"W1", "L2"}
    )
    assert len(violations) == 1
    assert "a -> ['ZZ9']" in violations[0]
    assert "Defined IDs: ['L2', 'W1']" in violations[0]


def test_contract_violations_flag_malformed_field() -> None:
    violations = lint_matrix.collect_contract_violations([{"id": "a", "contracts": "W1"}], {"W1"})
    assert len(violations) == 1
    assert "non-list-of-strings" in violations[0]


def test_contract_violations_silent_when_policy_holds() -> None:
    assert (
        lint_matrix.collect_contract_violations(
            [{"id": "a", "contracts": ["W1"]}, {"id": "b", "contracts": ["L2", "W1"]}],
            {"W1", "L2"},
        )
        == []
    )


def test_case_config_accepts_and_validates_contracts() -> None:
    case = ci_runner.CaseConfig.model_validate({"id": "c", "contracts": ["W1", "R2"]})
    assert case.contracts == ["W1", "R2"]
    # Absent field still parses so the matrix runs before the IDs land.
    assert ci_runner.CaseConfig.model_validate({"id": "c"}).contracts == []
    assert_raises(
        ValidationError,
        lambda: ci_runner.CaseConfig.model_validate({"id": "c", "contracts": ["w1"]}),
        "malformed contract ID",
    )
    assert_raises(
        ValidationError,
        lambda: ci_runner.CaseConfig.model_validate({"id": "c", "contracts": ["W1", "W1"]}),
        "duplicate contract ID",
    )


def test_real_matrix_still_parses() -> None:
    matrix = ci_runner.parse_matrix(MATRIX_PATH)
    assert matrix.suites
    assert matrix.cases
    for suite in matrix.suites.values():
        assert suite.cases


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def main() -> int:
    """Run every ``test_*`` function and print a summary table.

    Returns:
        0 when all tests pass, 1 otherwise.
    """
    console = Console()
    tests = [
        (name, fn)
        for name, fn in sorted(globals().items())
        if name.startswith("test_") and callable(fn)
    ]
    failures: list[tuple[str, str]] = []
    for name, fn in tests:
        try:
            fn()
        except Exception as exc:
            failures.append((name, f"{type(exc).__name__}: {exc}"))

    table = Table(title="ci_runner unit tests", show_lines=False)
    table.add_column("Test")
    table.add_column("Status")
    table.add_column("Detail")
    failed_names = dict(failures)
    for name, _ in tests:
        if name in failed_names:
            table.add_row(name, "[red]FAIL[/]", failed_names[name])
        else:
            table.add_row(name, "[green]PASS[/]", "")
    console.print(table)
    console.print()
    if failures:
        console.print(f"[red bold]{len(failures)} of {len(tests)} test(s) failed[/]")
        return 1
    console.print(f"[green bold]All {len(tests)} tests passed[/]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
