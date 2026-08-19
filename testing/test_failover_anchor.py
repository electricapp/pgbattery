#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the H-18 anchor checker."""

from __future__ import annotations

import unittest

import failover_anchor as fa
import fault_primitives as fp


def sample(node: str, leader: int | None, age: int | None, is_leader: bool = False) -> fa.Sample:
    return fa.Sample(node=node, leader_id=leader, is_leader=is_leader, anchor_age_ms=age, at=0.0)


class StaleAnchorTests(unittest.TestCase):
    def test_an_ancient_anchor_on_a_settled_cluster_is_rejected(self) -> None:
        """The missed-clear bug: the hold-down would read this as long expired."""
        found = fa.stale_anchors([sample("node1", 1, 90_000, is_leader=True)], lease_ms=2_000)
        self.assertEqual(found, [("node1", 90_000)])

    def test_no_anchor_is_the_settled_state(self) -> None:
        self.assertEqual(fa.stale_anchors([sample("node1", 1, None)], lease_ms=2_000), [])

    def test_a_fresh_anchor_is_not_stale(self) -> None:
        """A failover just finished; the anchor is allowed to still be there."""
        self.assertEqual(fa.stale_anchors([sample("node1", 1, 500)], lease_ms=2_000), [])

    def test_every_offending_node_is_reported(self) -> None:
        found = fa.stale_anchors(
            [sample("node1", 1, 90_000), sample("node2", 1, None), sample("node3", 1, 70_000)],
            lease_ms=2_000,
        )
        self.assertEqual(sorted(found), [("node1", 90_000), ("node3", 70_000)])


class SettledTests(unittest.TestCase):
    def test_agreement_on_one_leader_is_settled(self) -> None:
        samples = [sample(n, 2, None) for n in fp.NODES]
        self.assertTrue(fa.cluster_settled(samples))

    def test_disagreement_is_not_settled(self) -> None:
        samples = [sample("node1", 1, None), sample("node2", 2, None), sample("node3", 2, None)]
        self.assertFalse(fa.cluster_settled(samples))

    def test_leaderless_is_not_settled(self) -> None:
        samples = [sample(n, None, None) for n in fp.NODES]
        self.assertFalse(fa.cluster_settled(samples))

    def test_a_missing_node_is_not_settled(self) -> None:
        """Two nodes agreeing while a third is unreachable is not agreement."""
        self.assertFalse(fa.cluster_settled([sample("node1", 1, None), sample("node2", 1, None)]))


class RoundVerdictTests(unittest.TestCase):
    def test_a_surviving_anchor_fails_the_round(self) -> None:
        rnd = fa.Round(index=0, killed="node1", settled_with_anchor=[("node2", 90_000)])
        self.assertFalse(rnd.ok)
        self.assertIn("survived a settled cluster", rnd.verdict)

    def test_a_cleared_anchor_passes(self) -> None:
        self.assertTrue(fa.Round(index=0, killed="node1").ok)


if __name__ == "__main__":
    unittest.main()
