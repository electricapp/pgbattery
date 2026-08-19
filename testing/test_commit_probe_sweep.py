#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the H-17 commit-probe verdict.

Both wrong answers have to be rejected. `committed` with no row is a phantom
commit — the client believes a write that never happened. `aborted` with the row
present is a duplicate retry — the client re-issues a write that did.
"""

from __future__ import annotations

import unittest

import commit_probe_sweep as cps


def trial(probe: str | None, row_present: bool | None, txid: int | None = 42) -> cps.Trial:
    return cps.Trial(offset_ms=5, txid=txid, probe=probe, row_present=row_present)


class VerdictTests(unittest.TestCase):
    def test_committed_without_the_row_is_a_phantom_commit(self) -> None:
        result = trial("committed", False)
        self.assertFalse(result.ok)
        self.assertIn("phantom commit", result.verdict)

    def test_aborted_with_the_row_is_a_duplicate_retry(self) -> None:
        result = trial("aborted", True)
        self.assertFalse(result.ok)
        self.assertIn("duplicate retry", result.verdict)

    def test_definite_answers_that_match_the_data_pass(self) -> None:
        self.assertTrue(trial("committed", True).ok)
        self.assertTrue(trial("aborted", False).ok)

    def test_an_honest_unknown_constrains_nothing(self) -> None:
        """`in progress` and no answer are honest; they cannot be wrong."""
        self.assertTrue(trial(None, True).ok)
        self.assertTrue(trial(None, False).ok)
        self.assertTrue(trial("in progress", True).ok)
        self.assertTrue(trial("in progress", False).ok)

    def test_a_trial_that_never_got_an_id_is_skipped_not_passed(self) -> None:
        result = trial("committed", False, txid=None)
        self.assertTrue(result.verdict.startswith("SKIP"))

    def test_an_unreadable_row_is_skipped_not_convicted(self) -> None:
        result = trial("committed", None)
        self.assertTrue(result.verdict.startswith("SKIP"))


if __name__ == "__main__":
    unittest.main()
