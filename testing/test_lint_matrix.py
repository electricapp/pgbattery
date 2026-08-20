#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the harness lint's documentation checks.

A check that cannot fail is worse than no check, because it reads as coverage.
These drive each documentation gate to red on a case it is supposed to catch,
so that a green lint means the gate looked and found nothing rather than that
it never looked.

Run with:
    uv run --project testing python testing/test_lint_matrix.py
"""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

import lint_matrix as lm


class ProseFileReferenceTest(unittest.TestCase):
    """Paths in the documentation must name files that exist."""

    def test_the_documents_as_committed_pass(self) -> None:
        lm.check_prose_file_references_resolve()

    def test_a_path_naming_nothing_is_caught(self) -> None:
        with (
            mock.patch.object(lm, "PROSE_DOCS", ("CLAUDE.md",)),
            mock.patch.object(Path, "read_text", return_value="see `docs/NO_SUCH_FILE.md`"),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_prose_file_references_resolve()
        self.assertIn("NO_SUCH_FILE.md", str(caught.exception))

    def test_a_basename_is_enough_to_resolve(self) -> None:
        # Documentation says `torn_raft.py`, not `testing/torn_raft.py`, and
        # demanding the full path would push authors toward naming nothing.
        with (
            mock.patch.object(lm, "PROSE_DOCS", ("CLAUDE.md",)),
            mock.patch.object(Path, "read_text", return_value="see `torn_raft.py`"),
        ):
            lm.check_prose_file_references_resolve()


class ProseExemptionTest(unittest.TestCase):
    """An exemption that has outlived its reason is a hole held open."""

    def test_the_exemptions_as_committed_still_apply(self) -> None:
        lm.check_prose_exemptions_are_still_needed()

    def test_an_exemption_for_a_file_that_exists_is_caught(self) -> None:
        with (
            mock.patch.object(
                lm, "PROSE_REFERENCE_EXEMPT", {"torn_raft.py": "it exists, so this is stale"}
            ),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_prose_exemptions_are_still_needed()
        self.assertIn("torn_raft.py", str(caught.exception))

    def test_an_exemption_nothing_cites_any_more_is_caught(self) -> None:
        with (
            mock.patch.object(
                lm, "PROSE_REFERENCE_EXEMPT", {"gone/from/the/prose.md": "nothing says this"}
            ),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_prose_exemptions_are_still_needed()
        self.assertIn("prose.md", str(caught.exception))


if __name__ == "__main__":
    unittest.main(verbosity=2)
