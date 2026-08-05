#!/usr/bin/env -S uv run --project testing python
"""Unit tests for the ENOSPC suite's verdict logic.

No docker. These cover what the suite concludes from an L1 oracle report, which
is the part that decides whether a contract may be claimed.

Run with:
    uv run --project testing python testing/test_wal_enospc.py
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

import wal_enospc as we
from dual_writability_prober import ProbeReport, ProbeRound, Verdict


def probe_report(
    *,
    violations: tuple[ProbeRound, ...] = (),
    total_rounds: int = 60,
    indeterminate_probes: int = 0,
    total_probes: int = 180,
) -> ProbeReport:
    """A report with the fields this suite reads, defaults chosen to PASS."""
    return ProbeReport(
        total_rounds=total_rounds,
        node_count=3,
        rounds_by_acceptance_count={0: 4, 1: total_rounds - 4},
        violations=violations,
        windows=(),
        max_window_span_ns=0,
        reduced_observability_rounds=0,
        observability_lost_rounds=0,
        total_probes=total_probes,
        indeterminate_probes=indeterminate_probes,
        reason_counts={},
        unclassified_errors=(),
        schema_missing_probes=0,
        wall_duration_s=60.0,
        round_period_s=1.0,
        transport="docker-exec",
        max_indeterminate_rate=0.2,
        min_single_acceptance_rate=0.0,
    )


class SingleWritabilityTests(unittest.TestCase):
    def test_a_violation_fails_the_run(self) -> None:
        outcome = we.Outcome(probe=probe_report(violations=(object(),)))  # type: ignore[arg-type]
        with self.assertRaises(we.DualWritability):
            we.assert_single_writability(outcome, "enospc")

    def test_a_clean_oracle_does_not_fail_the_run(self) -> None:
        we.assert_single_writability(we.Outcome(probe=probe_report()), "enospc")

    def test_no_oracle_does_not_fail_the_run(self) -> None:
        """Absent an oracle there is nothing to assert on — the run still has
        to fail for its own reasons, not for L1's silence."""
        we.assert_single_writability(we.Outcome(probe=None), "enospc")


class ClaimedContractsTests(unittest.TestCase):
    """L1 is claimed only when the oracle established it. Treating "saw no
    violation" as "holds" is how a blind run reports a contract it never
    tested."""

    def test_a_passing_oracle_claims_l1(self) -> None:
        outcome = we.Outcome(probe=probe_report())
        self.assertIs(outcome.probe.verdict, Verdict.PASS)  # type: ignore[union-attr]
        self.assertTrue(outcome.l1_established)
        self.assertIn("L1", outcome.report()["contracts"])

    def test_an_inconclusive_oracle_does_not_claim_l1(self) -> None:
        outcome = we.Outcome(probe=probe_report(indeterminate_probes=120))
        self.assertIs(outcome.probe.verdict, Verdict.INCONCLUSIVE)  # type: ignore[union-attr]
        self.assertFalse(outcome.l1_established)
        self.assertNotIn("L1", outcome.report()["contracts"])
        self.assertIn("W1", outcome.report()["contracts"])

    def test_a_run_with_no_rounds_does_not_claim_l1(self) -> None:
        outcome = we.Outcome(probe=probe_report(total_rounds=0, total_probes=0))
        self.assertFalse(outcome.l1_established)
        self.assertNotIn("L1", outcome.report()["contracts"])

    def test_no_oracle_does_not_claim_l1(self) -> None:
        outcome = we.Outcome(probe=None)
        self.assertFalse(outcome.l1_established)
        self.assertNotIn("L1", outcome.report()["contracts"])
        self.assertEqual(outcome.report()["l1"]["verdict"], "NOT RUN")


class ReportShapeTests(unittest.TestCase):
    def test_fill_detail_survives_serialisation(self) -> None:
        outcome = we.Outcome(
            fill_detail=we.FillDetail(
                container="node2",
                filled_kb=1024,
                wal_segment_bytes=16 * 1024 * 1024,
                avail_kb_after=8192,
            )
        )
        fill = outcome.report()["fill"]
        assert fill is not None
        self.assertEqual(fill["container"], "node2")
        self.assertEqual(fill["avail_kb_after"], 8192)

    def test_an_absent_fill_serialises_null(self) -> None:
        """Null rather than {}: a control run has no fill, and an empty object
        would read as a fill that measured nothing."""
        self.assertIsNone(we.Outcome().report()["fill"])


class MainReportsItsOwnFailuresTests(unittest.TestCase):
    """An exception the suite defines but `main` does not catch escapes as a
    traceback with no JSON report, so CI sees a crash rather than a verdict."""

    @staticmethod
    def handled_names() -> set[str]:
        tree = ast.parse(Path(we.__file__).read_text(encoding="utf-8"))
        names: set[str] = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler) or node.type is None:
                continue
            caught = node.type.elts if isinstance(node.type, ast.Tuple) else [node.type]
            names.update(ast.unparse(item) for item in caught)
        return names

    def test_every_exception_this_module_defines_is_caught(self) -> None:
        declared = {
            name
            for name, obj in vars(we).items()
            if isinstance(obj, type)
            and issubclass(obj, Exception)
            and obj.__module__ == we.__name__
        }
        self.assertTrue(declared, "no suite exceptions found — the check would be vacuous")
        self.assertEqual(
            declared - self.handled_names(),
            set(),
            "defined but never caught, so it would escape as a traceback",
        )


if __name__ == "__main__":
    unittest.main()
