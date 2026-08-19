#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the follower-read and predicate-read checkers (H-10).

Each checker gets a history that violates it and must reject; a checker that
only ever passes is worse than none, because it reads as coverage.
"""

from __future__ import annotations

import unittest

from linreg.follower_reads import (
    FollowerRead,
    LeaderWrite,
    PredicateRead,
    lsn_to_int,
    no_invented_values,
    no_long_fork,
    predicate_reads_gain_only_written_keys,
    reads_are_monotonic,
    replayed_writes_are_visible,
)


def read(node: str, key: int, value: int | None, lsn: str, at: float) -> FollowerRead:
    return FollowerRead(node=node, key=key, value=value, replay_lsn=lsn, at=at)


def write(key: int, value: int, lsn: str, at: float = 0.0) -> LeaderWrite:
    return LeaderWrite(key=key, value=value, commit_lsn=lsn, at=at)


class LsnTests(unittest.TestCase):
    def test_lsn_orders_across_the_segment_boundary(self) -> None:
        self.assertLess(lsn_to_int("0/FFFFFFFF"), lsn_to_int("1/00000000"))
        self.assertLess(lsn_to_int("0/16B3748"), lsn_to_int("0/16B3749"))

    def test_a_malformed_lsn_raises(self) -> None:
        for bad in ("", "16B3748", "nonsense"):
            with self.assertRaises(ValueError):
                lsn_to_int(bad)


class InventedValueTests(unittest.TestCase):
    def test_a_value_nobody_wrote_is_rejected(self) -> None:
        ok, detail = no_invented_values(
            [read("node2", 1, 99, "0/10", 1.0)],
            [write(1, 5, "0/8")],
        )
        self.assertFalse(ok, detail)
        self.assertIn("99", detail)

    def test_written_values_and_the_initial_zero_pass(self) -> None:
        ok, _ = no_invented_values(
            [read("node2", 1, 5, "0/10", 1.0), read("node3", 1, 0, "0/1", 0.5)],
            [write(1, 5, "0/8")],
        )
        self.assertTrue(ok)


class MonotonicReadTests(unittest.TestCase):
    def test_a_regressing_view_is_rejected(self) -> None:
        ok, detail = reads_are_monotonic(
            [
                read("node2", 1, 7, "0/20", 1.0),
                read("node2", 1, 3, "0/30", 2.0),
            ]
        )
        self.assertFalse(ok, detail)
        self.assertIn("went backwards", detail)

    def test_a_regression_while_replay_rewound_is_not_flagged(self) -> None:
        """Only a regression at an equal-or-later replay position is a fault."""
        ok, _ = reads_are_monotonic(
            [
                read("node2", 1, 7, "0/30", 1.0),
                read("node2", 1, 3, "0/20", 2.0),
            ]
        )
        self.assertTrue(ok)

    def test_different_nodes_do_not_constrain_each_other(self) -> None:
        ok, _ = reads_are_monotonic(
            [
                read("node2", 1, 7, "0/30", 1.0),
                read("node3", 1, 3, "0/10", 2.0),
            ]
        )
        self.assertTrue(ok)


class ReplayVisibilityTests(unittest.TestCase):
    def test_a_replayed_write_that_is_not_visible_is_rejected(self) -> None:
        """The injected stale read: replayed past the commit, still serving old."""
        ok, detail = replayed_writes_are_visible(
            [read("node2", 1, 3, "0/50", 2.0)],
            [write(1, 3, "0/20"), write(1, 9, "0/40")],
        )
        self.assertFalse(ok, detail)
        self.assertIn("but served", detail)

    def test_lagging_behind_the_write_is_allowed(self) -> None:
        """Staleness is legal; only serving stale data you have replayed is not."""
        ok, _ = replayed_writes_are_visible(
            [read("node2", 1, 3, "0/30", 2.0)],
            [write(1, 3, "0/20"), write(1, 9, "0/40")],
        )
        self.assertTrue(ok)

    def test_seeing_the_replayed_write_passes(self) -> None:
        ok, _ = replayed_writes_are_visible(
            [read("node2", 1, 9, "0/50", 2.0)],
            [write(1, 3, "0/20"), write(1, 9, "0/40")],
        )
        self.assertTrue(ok)


class LongForkTests(unittest.TestCase):
    def test_two_standbys_ordering_a_key_differently_is_rejected(self) -> None:
        ok, detail = no_long_fork(
            [
                read("node2", 1, 4, "0/10", 1.0),
                read("node3", 1, 7, "0/10", 1.1),
                read("node2", 1, 7, "0/20", 2.0),
                read("node3", 1, 4, "0/20", 2.1),
            ]
        )
        self.assertFalse(ok, detail)
        self.assertIn("long fork", detail)

    def test_the_same_order_at_different_speeds_passes(self) -> None:
        """One standby lagging is not a fork — the order is the same."""
        ok, _ = no_long_fork(
            [
                read("node2", 1, 4, "0/10", 1.0),
                read("node2", 1, 7, "0/20", 2.0),
                read("node3", 1, 4, "0/10", 3.0),
                read("node3", 1, 7, "0/20", 4.0),
            ]
        )
        self.assertTrue(ok)

    def test_disjoint_observations_are_not_a_fork(self) -> None:
        ok, _ = no_long_fork(
            [
                read("node2", 1, 4, "0/10", 1.0),
                read("node3", 1, 7, "0/20", 2.0),
            ]
        )
        self.assertTrue(ok)


class PredicateReadTests(unittest.TestCase):
    def test_a_phantom_key_is_rejected(self) -> None:
        ok, detail = predicate_reads_gain_only_written_keys(
            [PredicateRead(node="node2", threshold=10, keys=[1, 2], at=5.0)],
            [write(1, 50, "0/10", at=1.0)],
        )
        self.assertFalse(ok, detail)
        self.assertIn("[2]", detail)

    def test_keys_a_write_qualified_pass(self) -> None:
        ok, _ = predicate_reads_gain_only_written_keys(
            [PredicateRead(node="node2", threshold=10, keys=[1, 2], at=5.0)],
            [write(1, 50, "0/10", at=1.0), write(2, 11, "0/20", at=2.0)],
        )
        self.assertTrue(ok)

    def test_a_write_after_the_read_cannot_explain_the_match(self) -> None:
        """A key matched before anything pushed it over the threshold."""
        ok, detail = predicate_reads_gain_only_written_keys(
            [PredicateRead(node="node2", threshold=10, keys=[1], at=1.0)],
            [write(1, 50, "0/10", at=9.0)],
        )
        self.assertFalse(ok, detail)


if __name__ == "__main__":
    unittest.main()
