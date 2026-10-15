#!/usr/bin/env -S uv run --project testing python
"""Proof that the proposal lint can fail.

`lint_propose.py` keeps every Raft proposal behind the feature-level admission
in `governor::propose`. A lint that has only ever passed is indistinguishable
from one that cannot fail, so this proves it reports a direct `client_write`,
ignores one that is only mentioned in a comment, and that the module it
exempts still exists.

Run with:
    uv run --project testing python testing/test_lint_propose.py
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Final

from lint_propose import DOOR, proposals_outside_the_door

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent


class CatchesDirectProposals(unittest.TestCase):
    def test_a_direct_client_write_is_reported(self) -> None:
        source = "let r = state.raft.client_write(ClusterRequest { command }).await;"
        self.assertEqual([n for n, _ in proposals_outside_the_door(source)], [1])

    def test_a_method_chain_split_across_lines_is_reported(self) -> None:
        source = "state\n    .raft\n    .client_write(request)\n    .await"
        self.assertEqual([n for n, _ in proposals_outside_the_door(source)], [3])

    def test_a_comment_is_not_a_proposal(self) -> None:
        source = "// callers never use client_write( directly\n/// see client_write(\n"
        self.assertEqual(proposals_outside_the_door(source), [])

    def test_a_trailing_comment_does_not_hide_the_call(self) -> None:
        source = "raft.client_write(req).await; // allowed?"
        self.assertEqual([n for n, _ in proposals_outside_the_door(source)], [1])


class TheDoorExists(unittest.TestCase):
    def test_the_exempt_module_is_real(self) -> None:
        """Otherwise every proposal would be reported, or none would be exempt
        for the wrong reason."""
        door = REPO_ROOT / DOOR
        self.assertTrue(door.exists(), f"{DOOR} is gone")
        self.assertTrue(proposals_outside_the_door(door.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
