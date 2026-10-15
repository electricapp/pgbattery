#!/usr/bin/env -S uv run --project testing python
"""Inversions for the RW-6 oracles in `follower_gateway_routing.py`.

Each test drives one oracle to its failing answer: an oracle never seen
failing is indistinguishable from one that cannot fail.

Run with:
    uv run --project testing python testing/test_follower_gateway_routing.py
"""

from __future__ import annotations

import unittest

import follower_gateway_routing as rw6
from follower_gateway_routing import Ack, Row

OLD = "172.28.0.11"
NEW = "172.28.0.12"


class RoutingOrderTest(unittest.TestCase):
    """The writer's own ack order: never back to the deposed node."""

    def test_a_deposed_ack_after_the_new_primary_is_caught(self) -> None:
        acks = [Ack("k1", OLD), Ack("k2", NEW), Ack("k3", OLD)]
        found = rw6.routing_violations(acks, OLD, NEW)
        self.assertEqual(len(found), 1)
        self.assertIn("k3", found[0])

    def test_a_clean_handover_passes(self) -> None:
        acks = [Ack("k1", OLD), Ack("k2", OLD), Ack("k3", NEW), Ack("k4", NEW)]
        self.assertEqual(rw6.routing_violations(acks, OLD, NEW), [])


class CommitOverlapTest(unittest.TestCase):
    """The final table: the deposed node's commits all precede the new one's."""

    def test_overlapping_commits_are_caught(self) -> None:
        rows = [Row("a", OLD, 100), Row("b", NEW, 200), Row("c", OLD, 300)]
        self.assertEqual(len(rw6.overlap_violations(rows, OLD, NEW)), 1)

    def test_disjoint_commits_pass(self) -> None:
        rows = [Row("a", OLD, 100), Row("b", OLD, 150), Row("c", NEW, 200)]
        self.assertEqual(rw6.overlap_violations(rows, OLD, NEW), [])


class DurabilityTest(unittest.TestCase):
    """W1: an acked key missing from the final leader is named."""

    def test_a_lost_acked_key_is_caught(self) -> None:
        acks = [Ack("a", OLD), Ack("b", OLD)]
        rows = [Row("a", NEW, 1)]
        self.assertEqual(rw6.missing_acked(acks, rows), ["b"])


class CoverageTest(unittest.TestCase):
    """A run that never crossed the change says nothing about RW-6."""

    def test_a_run_without_acks_on_both_sides_is_refused(self) -> None:
        self.assertEqual(len(rw6.coverage_problems([Ack("a", OLD)], OLD, NEW)), 1)
        self.assertEqual(len(rw6.coverage_problems([], OLD, NEW)), 2)
        self.assertEqual(rw6.coverage_problems([Ack("a", OLD), Ack("b", NEW)], OLD, NEW), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
