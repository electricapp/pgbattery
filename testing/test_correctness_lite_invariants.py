#!/usr/bin/env -S uv run --project testing python
"""Unit tests for the pure invariant logic in correctness_lite.

No cluster, no docker, no psql: every test here feeds a synthetic history (or a
synthetic psql outcome) to a pure function and asserts on the classification.
Stdlib `unittest`, so the test harness pyproject doesn't grow a pytest
dependency.

Covered:
- Error taxonomy (`classify_attempt`): an unrecognized psql failure must be
  INDETERMINATE, never a definite non-commit, or I2/C1 fire false positives.
- Retry discipline (`bank_transfer`): an indeterminate transfer must never be
  re-executed, because a timed-out COMMIT may have committed.
- I5 classification: strict containment in a quorum-loss window is FATAL,
  boundary-straddling is a labelled warning.
- Bank ledger invariants B3/B4: a transfer applied twice must be detectable
  even though SUM(balance) conservation (B1) is blind to it.
- Log-grep layer L0/L2/L3, including that the marker substrings the greps
  depend on still exist in the Rust source.

Run with:
    uv run --project testing python testing/test_correctness_lite_invariants.py
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import correctness_lite as cl

REPO_ROOT: Path = Path(__file__).resolve().parent.parent


# ─────────────────────────────────────────────────────────────────────────────
# Builders
# ─────────────────────────────────────────────────────────────────────────────


def _op(value: int, start: float, end: float, outcome: str = "acked") -> cl.OpRecord:
    return cl.OpRecord(
        seq=value,
        value=value,
        start_ts=start,
        end_ts=end,
        wall_start=0.0,
        outcome=outcome,
        port=5432,
    )


def _window(start: float, end: float, kind: str = "quorum_loss") -> cl.FaultWindow:
    return cl.FaultWindow(kind=kind, start_ts=start, end_ts=end, detail="synthetic")


def _transfer(
    transfer_id: int, outcome: str, frm: int = 1, to: int = 2, amount: int = 10
) -> cl.TransferRecord:
    return cl.TransferRecord(
        transfer_id=transfer_id,
        from_id=frm,
        to_id=to,
        amount=amount,
        start_ts=0.0,
        end_ts=0.1,
        outcome=outcome,
        port=5432,
    )


def _ledger(transfer_id: int, frm: int = 1, to: int = 2, amount: int = 10) -> cl.LedgerRow:
    return cl.LedgerRow(transfer_id=transfer_id, from_id=frm, to_id=to, amount=amount)


def _flat_balances() -> dict[int, int]:
    return {i: cl.BANK_INITIAL_BALANCE for i in range(1, cl.BANK_ACCOUNTS + 1)}


def _ids(findings: list[cl.Violation]) -> list[str]:
    return sorted(f.invariant for f in findings)


def _fatal_ids(findings: list[cl.Violation]) -> list[str]:
    return sorted(f.invariant for f in findings if f.severity == cl.SEVERITY_FATAL)


def _warn_ids(findings: list[cl.Violation]) -> list[str]:
    return sorted(f.invariant for f in findings if f.severity == cl.SEVERITY_WARN)


# ─────────────────────────────────────────────────────────────────────────────
# Error taxonomy
# ─────────────────────────────────────────────────────────────────────────────


class ClassifyAttemptTests(unittest.TestCase):
    def test_returncode_zero_is_acked(self) -> None:
        # "acked" requires rc == 0 and nothing else may produce it.
        cls = cl.classify_attempt(0, "INSERT 0 1")
        self.assertEqual(cls.outcome, cl.ATTEMPT_ACKED)

    def test_alarming_output_with_rc_zero_is_still_acked(self) -> None:
        cls = cl.classify_attempt(0, "NOTICE: read-only hint that did not fail")
        self.assertEqual(cls.outcome, cl.ATTEMPT_ACKED)

    def test_timeout_marker_is_indeterminate(self) -> None:
        # run_cmd maps subprocess.TimeoutExpired to (-1, "", "timeout").
        cls = cl.classify_attempt(-1, "timeout")
        self.assertEqual(cls.outcome, cl.ATTEMPT_INDETERMINATE)

    def test_ssl_syscall_eof_is_indeterminate(self) -> None:
        cls = cl.classify_attempt(
            2, "psql: error: SSL SYSCALL error: EOF detected\nconnection to server lost"
        )
        self.assertEqual(cls.outcome, cl.ATTEMPT_INDETERMINATE)

    def test_admin_termination_is_indeterminate(self) -> None:
        cls = cl.classify_attempt(2, "FATAL:  terminating connection due to administrator command")
        self.assertEqual(cls.outcome, cl.ATTEMPT_INDETERMINATE)

    def test_unrecognized_error_is_indeterminate(self) -> None:
        # The sound direction: unknown fate must weaken the bound, never
        # manufacture a definite non-commit (which would make I2/C1 fire).
        cls = cl.classify_attempt(1, "psql: error: something nobody has ever seen")
        self.assertEqual(cls.outcome, cl.ATTEMPT_INDETERMINATE)
        self.assertEqual(cls.reason, "unclassified")

    def test_read_only_is_routing_rejection(self) -> None:
        cls = cl.classify_attempt(1, "ERROR:  cannot execute INSERT in a read-only transaction")
        self.assertEqual(cls.outcome, cl.ATTEMPT_ROUTING_REJECTED)

    def test_connection_refused_is_routing_rejection(self) -> None:
        cls = cl.classify_attempt(2, "psql: error: connection refused")
        self.assertEqual(cls.outcome, cl.ATTEMPT_ROUTING_REJECTED)

    def test_check_constraint_is_definite_rejection(self) -> None:
        cls = cl.classify_attempt(
            1, 'ERROR:  new row for relation "bank_accounts" violates check constraint'
        )
        self.assertEqual(cls.outcome, cl.ATTEMPT_REJECTED)

    def test_duplicate_key_is_definite_rejection(self) -> None:
        cls = cl.classify_attempt(
            1, 'ERROR:  duplicate key value violates unique constraint "bank_ledger_pkey"'
        )
        self.assertEqual(cls.outcome, cl.ATTEMPT_REJECTED)


class TryInsertTaxonomyTests(unittest.TestCase):
    """`try_insert` must not record an unknown-fate write as definitely errored."""

    def test_unrecognized_error_records_indeterminate(self) -> None:
        history = cl.History()
        completed = mock.Mock()
        completed.returncode = 1
        completed.stdout = "psql: error: SSL SYSCALL error: EOF detected"
        completed.stderr = ""
        with mock.patch.object(subprocess, "run", return_value=completed):
            outcome = cl.try_insert(7, history)
        self.assertEqual(outcome, "indeterminate")
        self.assertEqual(history.indeterminate_set, {7})
        self.assertEqual(history.errored_set, set())

    def test_routing_rejection_on_all_ports_is_errored(self) -> None:
        history = cl.History()
        completed = mock.Mock()
        completed.returncode = 1
        completed.stdout = "ERROR:  cannot execute INSERT in a read-only transaction"
        completed.stderr = ""
        with mock.patch.object(subprocess, "run", return_value=completed) as run:
            outcome = cl.try_insert(9, history)
        self.assertEqual(outcome, "errored")
        self.assertEqual(run.call_count, len(cl.GATEWAY_PORTS))


class IncrementTaxonomyTests(unittest.TestCase):
    def test_unrecognized_error_is_indeterminate(self) -> None:
        completed = mock.Mock()
        completed.returncode = 1
        completed.stdout = "psql: error: SSL SYSCALL error: EOF detected"
        completed.stderr = ""
        with mock.patch.object(subprocess, "run", return_value=completed):
            self.assertEqual(cl._try_increment(5432, 0), "indeterminate")

    def test_monotonic_unrecognized_error_is_indeterminate(self) -> None:
        completed = mock.Mock()
        completed.returncode = 1
        completed.stdout = "psql: error: SSL SYSCALL error: EOF detected"
        completed.stderr = ""
        with mock.patch.object(subprocess, "run", return_value=completed):
            self.assertEqual(cl._try_write_monotonic(1), "indeterminate")


# ─────────────────────────────────────────────────────────────────────────────
# Retry discipline for bank transfers
# ─────────────────────────────────────────────────────────────────────────────


class BankTransferRetryTests(unittest.TestCase):
    def test_timeout_is_recorded_indeterminate_and_never_retried(self) -> None:
        calls: list[str] = []

        def fake_run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
            calls.append(cmd)
            return -1, "", "timeout"

        with mock.patch.object(cl, "run_cmd", fake_run_cmd):
            rec = cl.bank_transfer(11, 1, 2, 25)

        self.assertEqual(rec.outcome, "indeterminate")
        self.assertEqual(len(calls), 1, "a possibly-committed transfer must not be re-executed")

    def test_routing_rejection_tries_next_port(self) -> None:
        calls: list[str] = []

        def fake_run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
            calls.append(cmd)
            return 1, "ERROR:  cannot execute UPDATE in a read-only transaction", ""

        with mock.patch.object(cl, "run_cmd", fake_run_cmd):
            rec = cl.bank_transfer(12, 1, 2, 25)

        self.assertEqual(rec.outcome, "rejected")
        self.assertEqual(len(calls), len(cl.GATEWAY_PORTS))

    def test_server_rejection_is_not_retried(self) -> None:
        calls: list[str] = []

        def fake_run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
            calls.append(cmd)
            return 1, 'ERROR:  new row violates check constraint "bank_accounts_balance_check"', ""

        with mock.patch.object(cl, "run_cmd", fake_run_cmd):
            rec = cl.bank_transfer(13, 1, 2, 25)

        self.assertEqual(rec.outcome, "rejected")
        self.assertEqual(len(calls), 1)

    def test_unclassified_error_is_indeterminate_and_not_retried(self) -> None:
        calls: list[str] = []

        def fake_run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
            calls.append(cmd)
            return 1, "psql: error: a failure mode nobody has catalogued", ""

        with mock.patch.object(cl, "run_cmd", fake_run_cmd):
            rec = cl.bank_transfer(15, 1, 2, 25)

        self.assertEqual(rec.outcome, "indeterminate")
        self.assertEqual(rec.reason, "unclassified")
        self.assertEqual(len(calls), 1)

    def test_transfer_writes_its_id_to_the_ledger_in_the_same_transaction(self) -> None:
        calls: list[str] = []

        def fake_run_cmd(cmd: str, timeout: int = 30) -> tuple[int, str, str]:
            calls.append(cmd)
            return 0, "COMMIT", ""

        with mock.patch.object(cl, "run_cmd", fake_run_cmd):
            rec = cl.bank_transfer(14, 3, 4, 25)

        self.assertEqual(rec.outcome, "acked")
        self.assertEqual(len(calls), 1)
        sql = calls[0]
        self.assertIn("BEGIN", sql)
        self.assertIn(cl.BANK_LEDGER_TABLE, sql)
        self.assertIn("14", sql)
        self.assertLess(sql.index(cl.BANK_LEDGER_TABLE), sql.index("COMMIT"))


# ─────────────────────────────────────────────────────────────────────────────
# I5 — acked writes vs quorum-loss windows
# ─────────────────────────────────────────────────────────────────────────────


class QuorumLossClassificationTests(unittest.TestCase):
    def test_strictly_contained(self) -> None:
        pos, fw = cl.classify_ack_vs_quorum_loss(_op(1, 12.0, 13.0), [_window(10.0, 20.0)])
        self.assertEqual(pos, cl.ACK_CONTAINED)
        self.assertIsNotNone(fw)

    def test_straddles_window_start(self) -> None:
        pos, _ = cl.classify_ack_vs_quorum_loss(_op(1, 9.5, 12.0), [_window(10.0, 20.0)])
        self.assertEqual(pos, cl.ACK_STRADDLING)

    def test_straddles_window_end(self) -> None:
        pos, _ = cl.classify_ack_vs_quorum_loss(_op(1, 19.5, 21.0), [_window(10.0, 20.0)])
        self.assertEqual(pos, cl.ACK_STRADDLING)

    def test_outside_window(self) -> None:
        pos, fw = cl.classify_ack_vs_quorum_loss(_op(1, 21.0, 22.0), [_window(10.0, 20.0)])
        self.assertEqual(pos, cl.ACK_OUTSIDE)
        self.assertIsNone(fw)

    def test_containment_wins_over_straddle(self) -> None:
        windows = [_window(10.0, 11.0), _window(9.0, 20.0)]
        pos, _ = cl.classify_ack_vs_quorum_loss(_op(1, 10.5, 12.0), windows)
        self.assertEqual(pos, cl.ACK_CONTAINED)


class CheckInvariantsI5Tests(unittest.TestCase):
    def _history(self, op: cl.OpRecord, fw: cl.FaultWindow) -> cl.History:
        history = cl.History()
        history.ops.append(op)
        if op.outcome == "acked":
            history.acked_set.add(op.value)
        history.faults.append(fw)
        return history

    def test_contained_ack_is_fatal_i5(self) -> None:
        history = self._history(_op(1, 12.0, 13.0), _window(10.0, 20.0))
        findings = cl.check_invariants(history, {1}, 1, 1)
        self.assertIn("I5", _fatal_ids(findings))

    def test_straddling_ack_is_warning_not_fatal(self) -> None:
        history = self._history(_op(1, 9.5, 12.0), _window(10.0, 20.0))
        findings = cl.check_invariants(history, {1}, 1, 1)
        self.assertNotIn("I5", _fatal_ids(findings))
        self.assertIn("I5-WARN", _warn_ids(findings))

    def test_straddling_end_ack_is_warning_not_fatal(self) -> None:
        history = self._history(_op(1, 19.9, 25.0), _window(10.0, 20.0))
        findings = cl.check_invariants(history, {1}, 1, 1)
        self.assertNotIn("I5", _fatal_ids(findings))
        self.assertIn("I5-WARN", _warn_ids(findings))

    def test_ack_outside_window_is_silent(self) -> None:
        history = self._history(_op(1, 21.0, 22.0), _window(10.0, 20.0))
        findings = cl.check_invariants(history, {1}, 1, 1)
        self.assertEqual(_ids(findings), [])

    def test_unclosed_window_is_ignored(self) -> None:
        history = self._history(_op(1, 12.0, 13.0), _window(10.0, 0.0))
        findings = cl.check_invariants(history, {1}, 1, 1)
        self.assertEqual(_ids(findings), [])

    def test_non_quorum_window_is_ignored(self) -> None:
        history = self._history(_op(1, 12.0, 13.0), _window(10.0, 20.0, kind="kill_leader"))
        findings = cl.check_invariants(history, {1}, 1, 1)
        self.assertEqual(_ids(findings), [])


# ─────────────────────────────────────────────────────────────────────────────
# Bank invariants B1-B4
# ─────────────────────────────────────────────────────────────────────────────


class BankLedgerInvariantTests(unittest.TestCase):
    def test_clean_single_application_passes(self) -> None:
        balances = _flat_balances()
        balances[1] -= 10
        balances[2] += 10
        findings = cl._check_bank_against_db(balances, [_ledger(1)], [_transfer(1, "acked")])
        self.assertEqual(_ids(findings), [])

    def test_b1_is_blind_to_double_application(self) -> None:
        # The audited hole: applying the same transfer twice still conserves
        # SUM(balance), so B1 alone cannot see it.
        balances = _flat_balances()
        balances[1] -= 20
        balances[2] += 20
        self.assertEqual(sum(balances.values()), cl.BANK_TOTAL)
        findings = cl._check_bank_against_db(balances, [_ledger(1)], [_transfer(1, "acked")])
        self.assertNotIn("B1", _ids(findings))
        self.assertIn("B4", _fatal_ids(findings))

    def test_duplicate_ledger_row_flags_b3(self) -> None:
        balances = _flat_balances()
        balances[1] -= 20
        balances[2] += 20
        findings = cl._check_bank_against_db(
            balances, [_ledger(1), _ledger(1)], [_transfer(1, "acked")]
        )
        self.assertIn("B3", _fatal_ids(findings))

    def test_acked_transfer_missing_from_ledger_flags_b3(self) -> None:
        findings = cl._check_bank_against_db(_flat_balances(), [], [_transfer(1, "acked")])
        self.assertIn("B3", _fatal_ids(findings))

    def test_phantom_ledger_row_flags_b3(self) -> None:
        balances = _flat_balances()
        balances[1] -= 10
        balances[2] += 10
        findings = cl._check_bank_against_db(balances, [_ledger(1)], [_transfer(1, "rejected")])
        self.assertIn("B3", _fatal_ids(findings))

    def test_never_attempted_ledger_row_flags_b3(self) -> None:
        balances = _flat_balances()
        balances[1] -= 10
        balances[2] += 10
        findings = cl._check_bank_against_db(balances, [_ledger(99)], [_transfer(1, "rejected")])
        self.assertIn("B3", _fatal_ids(findings))

    def test_indeterminate_transfer_may_be_applied_once(self) -> None:
        balances = _flat_balances()
        balances[1] -= 10
        balances[2] += 10
        findings = cl._check_bank_against_db(
            balances, [_ledger(1)], [_transfer(1, "indeterminate")]
        )
        self.assertEqual(_ids(findings), [])

    def test_indeterminate_transfer_may_be_absent(self) -> None:
        findings = cl._check_bank_against_db(_flat_balances(), [], [_transfer(1, "indeterminate")])
        self.assertEqual(_ids(findings), [])

    def test_negative_balance_flags_b2(self) -> None:
        balances = _flat_balances()
        balances[1] = -5
        balances[2] += cl.BANK_INITIAL_BALANCE + 5
        findings = cl._check_bank_against_db(balances, [], [])
        self.assertIn("B2", _fatal_ids(findings))

    def test_lost_money_flags_b1(self) -> None:
        balances = _flat_balances()
        balances[1] -= 10
        findings = cl._check_bank_against_db(balances, [], [])
        self.assertIn("B1", _fatal_ids(findings))

    def test_no_transfers_recorded_skips_b3_b4(self) -> None:
        findings = cl._check_bank_against_db(_flat_balances(), [], [])
        self.assertEqual(_ids(findings), [])


# ─────────────────────────────────────────────────────────────────────────────
# Log-grep layer L0/L2/L3
# ─────────────────────────────────────────────────────────────────────────────


HEALTHY_LOG = (
    "node1-1  | INFO Starting pgbattery in DATA mode node_id=1\n"
    "node1-1  | INFO pgbattery DATA node is running (lease fencing enabled)\n"
    "node1-1  | INFO This node is now the leader\n"
)


class LogGrepTests(unittest.TestCase):
    def _check(self, text: str | None, quorum_windows: int = 0) -> list[cl.Violation]:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "docker-compose.log"
            if text is not None:
                path.write_text(text, encoding="utf-8")
            return cl._check_log_grep(path, quorum_loss_windows=quorum_windows)

    def test_missing_log_file_is_fatal(self) -> None:
        # A grep over a log that does not exist must never read as a pass.
        findings = self._check(None)
        self.assertIn("L0", _fatal_ids(findings))

    def test_log_without_pgbattery_markers_is_fatal(self) -> None:
        findings = self._check("some unrelated text with no pgbattery output at all\n")
        self.assertIn("L0", _fatal_ids(findings))

    def test_healthy_log_passes(self) -> None:
        self.assertEqual(_ids(self._check(HEALTHY_LOG)), [])

    def test_split_brain_signal_flags_l2(self) -> None:
        findings = self._check(
            HEALTHY_LOG + "node2-1  | ERROR Promotion safety check failed - potential split-brain\n"
        )
        self.assertIn("L2", _fatal_ids(findings))

    def test_fence_failure_signal_flags_l2(self) -> None:
        findings = self._check(
            HEALTHY_LOG + "node2-1  | ERROR FAILED TO FENCE - will shut down if this persists\n"
        )
        self.assertIn("L2", _fatal_ids(findings))

    def test_emergency_fence_without_confirmation_flags_l3(self) -> None:
        findings = self._check(
            HEALTHY_LOG + "node1-1  | ERROR EMERGENCY FENCE: Lease expired, forcing read-only\n"
        )
        self.assertIn("L3", _fatal_ids(findings))

    def test_emergency_fence_with_confirmation_passes(self) -> None:
        findings = self._check(
            HEALTHY_LOG
            + "node1-1  | ERROR EMERGENCY FENCE: Lease expired, forcing read-only\n"
            + "node1-1  | INFO PostgreSQL fenced (read-only)\n",
            quorum_windows=1,
        )
        self.assertEqual(_ids(findings), [])

    def test_quorum_loss_without_any_fence_trace_warns(self) -> None:
        # A silent grep over a scenario that should have produced fence
        # activity must be visible, not an implicit pass.
        findings = self._check(HEALTHY_LOG, quorum_windows=1)
        self.assertEqual(_fatal_ids(findings), [])
        self.assertIn("L3-WARN", _warn_ids(findings))


class RustMarkerStringTests(unittest.TestCase):
    """The log greps are only as good as the substrings the Rust code emits."""

    def test_marker_substrings_exist_in_rust_source(self) -> None:
        roots = [REPO_ROOT / "src", REPO_ROOT / "crates"]
        roots = [r for r in roots if r.is_dir()]
        if not roots:
            self.skipTest("Rust source tree not available")
        corpus = "\n".join(
            p.read_text(encoding="utf-8", errors="replace")
            for root in roots
            for p in root.rglob("*.rs")
        )
        expected = (
            list(cl.LOG_SPLIT_BRAIN_SIGNALS)
            + list(cl.LOG_FENCE_MARKERS)
            + list(cl.LOG_LIVENESS_MARKERS)
            + [cl.LOG_FENCE_CONFIRMED]
        )
        missing = [s for s in expected if s not in corpus]
        self.assertEqual(
            missing, [], f"log markers no longer emitted by the Rust source: {missing}"
        )


class ComposeNameResolutionTests(unittest.TestCase):
    """Docker object names must follow the active compose project.

    `docker-compose.yml` sets `name: pgbattery`, so literal names work locally
    and only break under the per-run `COMPOSE_PROJECT_NAME` every CI workflow
    sets — which is why the partition fault no-opped in CI undetected.
    """

    def test_project_defaults_to_compose_file_name(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("COMPOSE_PROJECT_NAME", None)
            self.assertEqual(cl.compose_project(), "pgbattery")

    def test_project_honours_environment_override(self) -> None:
        with mock.patch.dict(os.environ, {"COMPOSE_PROJECT_NAME": "pgbha_correctness_42_1"}):
            self.assertEqual(cl.compose_project(), "pgbha_correctness_42_1")

    def test_network_name_tracks_the_project(self) -> None:
        with mock.patch.dict(os.environ, {"COMPOSE_PROJECT_NAME": "pgbha_correctness_42_1"}):
            self.assertEqual(cl.raft_network_name(), "pgbha_correctness_42_1_raft_net")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("COMPOSE_PROJECT_NAME", None)
            self.assertEqual(cl.raft_network_name(), "pgbattery_raft_net")

    def test_container_name_raises_when_service_cannot_be_resolved(self) -> None:
        with (
            mock.patch.object(cl, "docker_compose", return_value=(0, "\n", "")),
            self.assertRaises(RuntimeError) as caught,
        ):
            cl.container_name("node1")
        self.assertIn("cannot resolve container", str(caught.exception))

    def test_container_name_returns_resolved_id(self) -> None:
        with mock.patch.object(cl, "docker_compose", return_value=(0, "abc123\n", "")):
            self.assertEqual(cl.container_name("node1"), "abc123")


if __name__ == "__main__":
    unittest.main()
