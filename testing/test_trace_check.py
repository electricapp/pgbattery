#!/usr/bin/env -S uv run --project testing python
"""Inversions for `trace_check.py`.

A synthetic three-node failover goes through the same parse boundary the live
trace does. Every rule is driven to its failing answer, every drift is shown to
trip the rule it targets, and a hole in the trace is shown to make an instance
unverifiable rather than violated.

Run with:
    uv run --project testing python testing/test_trace_check.py
"""

from __future__ import annotations

import unittest
from collections import defaultdict
from typing import Final

import api_models
import trace_check as tc

BOUNDS: Final[tc.Bounds] = tc.Bounds(hold_down_ms=2_000, voters=3)


class Run:
    """Per-node event streams, sequence-numbered in the order added."""

    def __init__(self) -> None:
        self.events: defaultdict[int, list[api_models.DebugEvent]] = defaultdict(list)

    def add(self, node: int, at: int, event_type: str = "spec", **data: object) -> int:
        seq = len(self.events[node]) + 1
        self.events[node].append(
            api_models.DebugEvent(
                seq=seq, timestamp_ms=at, event_type=event_type, node_id=node, data=data
            )
        )
        return seq

    def raft(self, node: int, at: int, term: int, leader: int | None) -> int:
        return self.add(node, at, kind="raft", term=term, leader=leader)

    def anchor(self, node: int, at: int, stamped: bool) -> int:
        return self.add(node, at, kind="anchor", stamped=stamped)

    def open(self, node: int, at: int) -> int:
        return self.add(node, at, kind="open")

    def writes(
        self, node: int, at: int, writable: bool | None, lease_valid: bool, lease_leader: bool
    ) -> int:
        return self.add(
            node,
            at,
            kind="writes",
            writable=writable,
            lease_valid=lease_valid,
            lease_leader=lease_leader,
        )

    def vote(
        self,
        node: int,
        at: int,
        term: int,
        candidate: int,
        granted: bool = True,
        candidate_lsn: int | None = 500,
        voter_wal: object = None,
        threshold: int = 0,
    ) -> int:
        return self.add(
            node,
            at,
            kind="vote",
            term=term,
            candidate=candidate,
            granted=granted,
            candidate_lsn=candidate_lsn,
            voter_wal={"at": 500} if voter_wal is None else voter_wal,
            threshold=threshold,
        )

    def trace(self, evicted: frozenset[tuple[int, int]] = frozenset()) -> tc.Trace:
        return tc.Trace(
            {
                node: tc.node_trace(
                    node,
                    [e for e in events if (node, e.seq) not in evicted],
                    current_seq=len(events),
                )
                for node, events in self.events.items()
            }
        )


def bootstrap(run: Run) -> None:
    """node1 bootstraps and opens; node2 and node3 join as standbys."""
    run.raft(1, 0, term=1, leader=1)
    run.writes(1, 10, writable=False, lease_valid=True, lease_leader=True)
    run.open(1, 50)
    run.writes(1, 100, writable=True, lease_valid=True, lease_leader=True)
    for node in (2, 3):
        run.raft(node, 20, term=1, leader=1)
        run.writes(node, 30, writable=False, lease_valid=False, lease_leader=False)


def failover(run: Run, open_at: int = 4_100) -> dict[str, int]:
    """node1 is cut off; node2 wins term 2 on node3's vote and opens."""
    bootstrap(run)
    seqs = {"fence": run.writes(1, 2_000, writable=False, lease_valid=False, lease_leader=True)}
    seqs["campaign"] = run.raft(2, 2_100, term=2, leader=None)
    run.anchor(2, 2_100, stamped=True)
    seqs["vote"] = run.vote(3, 2_110, term=2, candidate=2)
    run.raft(3, 2_111, term=2, leader=None)
    run.raft(2, 2_150, term=2, leader=2)
    run.raft(3, 2_200, term=2, leader=2)
    seqs["open"] = run.open(2, open_at)
    run.anchor(2, open_at + 1, stamped=False)
    run.writes(2, open_at + 50, writable=True, lease_valid=True, lease_leader=True)
    run.raft(1, 6_000, term=2, leader=2)
    run.writes(1, 6_001, writable=False, lease_valid=False, lease_leader=False)
    return seqs


def checked(run: Run) -> tc.Report:
    return tc.check(run.trace(), BOUNDS)


class CleanRunTest(unittest.TestCase):
    def test_a_clean_failover_violates_nothing_and_checks_every_rule(self) -> None:
        run = Run()
        failover(run)
        report = checked(run)
        self.assertEqual(report.violations, [])
        unchecked = [r.name for r in tc.RULES if report.checked[r.name] == 0]
        self.assertEqual(unchecked, [], "a rule the synthetic run never exercised")

    def test_the_bootstrap_open_owes_no_hold_down(self) -> None:
        # No earlier leader exists to have been deposed.
        run = Run()
        bootstrap(run)
        report = checked(run)
        self.assertEqual(report.violated(tc.HOLD_DOWN_FROM_TERM), [])
        self.assertEqual(report.checked[tc.HOLD_DOWN_FROM_TERM.name], 0)


class DriftTest(unittest.TestCase):
    """Each drift, applied to the clean run, trips the rule it targets."""

    def test_every_drift_trips_its_rule(self) -> None:
        run = Run()
        failover(run)
        clean = run.trace()
        before = tc.check(clean, BOUNDS)
        for drift in tc.DRIFTS:
            with self.subTest(drift=drift.name):
                self.assertEqual(before.violated(drift.trips), [])
                after = tc.check(drift.apply(clean, BOUNDS), BOUNDS)
                self.assertNotEqual(after.violated(drift.trips), [], drift.name)

    def test_a_drift_without_its_shape_says_so(self) -> None:
        run = Run()
        bootstrap(run)
        with self.assertRaises(tc.DriftNotApplicable):
            tc.drift_open_inside_hold_down(run.trace(), BOUNDS)


class HoldDownTest(unittest.TestCase):
    def test_an_open_inside_the_hold_down_is_caught_both_ways(self) -> None:
        run = Run()
        failover(run, open_at=3_000)
        report = checked(run)
        self.assertNotEqual(report.violated(tc.HOLD_DOWN_FROM_TERM), [])
        self.assertNotEqual(report.violated(tc.OPEN_GATED), [], "the anchor was 900 ms old")

    def test_an_open_at_the_hold_down_passes(self) -> None:
        run = Run()
        failover(run, open_at=2_100 + 2_000)
        self.assertEqual(checked(run).violations, [])

    def test_a_reelected_primary_that_opens_at_once_is_caught(self) -> None:
        # node1 hears term 2, wins term 3 over the primary it never demoted,
        # and opens with no promotion to hold it.
        run = Run()
        bootstrap(run)
        run.raft(1, 1_000, term=2, leader=None)
        run.writes(1, 1_050, writable=False, lease_valid=False, lease_leader=False)
        run.raft(1, 1_500, term=3, leader=1)
        run.open(1, 1_800)
        run.writes(1, 1_850, writable=True, lease_valid=True, lease_leader=True)
        self.assertNotEqual(checked(run).violated(tc.HOLD_DOWN_FROM_TERM), [])

    def test_a_hole_where_the_term_began_is_unverifiable_not_violated(self) -> None:
        run = Run()
        seqs = failover(run, open_at=3_000)
        report = tc.check(run.trace(evicted=frozenset({(2, seqs["campaign"])})), BOUNDS)
        self.assertEqual(report.violated(tc.HOLD_DOWN_FROM_TERM), [])
        self.assertIn(tc.HOLD_DOWN_FROM_TERM.name, {f.rule for f in report.unverifiable})

    def test_an_open_believing_another_leader_is_caught(self) -> None:
        run = Run()
        bootstrap(run)
        run.open(2, 500)
        self.assertNotEqual(checked(run).violated(tc.OPEN_GATED), [])


class WriteStateTest(unittest.TestCase):
    def test_writable_without_an_open_is_caught(self) -> None:
        run = Run()
        run.raft(1, 0, term=1, leader=1)
        run.writes(1, 10, writable=False, lease_valid=True, lease_leader=True)
        run.writes(1, 100, writable=True, lease_valid=True, lease_leader=True)
        self.assertNotEqual(checked(run).violated(tc.WRITABLE_VIA_OPEN), [])

    def test_writable_when_the_trace_starts_is_unverifiable(self) -> None:
        run = Run()
        run.raft(1, 0, term=1, leader=1)
        run.writes(1, 100, writable=True, lease_valid=True, lease_leader=True)
        report = tc.check(run.trace(evicted=frozenset({(1, 1)})), BOUNDS)
        self.assertEqual(report.violated(tc.WRITABLE_VIA_OPEN), [])
        self.assertIn(tc.WRITABLE_VIA_OPEN.name, {f.rule for f in report.unverifiable})

    def test_an_unread_writability_under_an_invalid_lease_is_unverifiable(self) -> None:
        run = Run()
        bootstrap(run)
        run.writes(1, 2_000, writable=None, lease_valid=False, lease_leader=True)
        report = checked(run)
        self.assertEqual(report.violated(tc.FENCE_LANDS), [])
        self.assertIn(tc.FENCE_LANDS.name, {f.rule for f in report.unverifiable})

    def test_overlap_with_an_unread_writability_is_unverifiable(self) -> None:
        run = Run()
        bootstrap(run)
        run.raft(2, 500, term=2, leader=2)
        run.writes(2, 600, writable=None, lease_valid=True, lease_leader=True)
        report = checked(run)
        self.assertEqual(report.violated(tc.ONE_WRITE_AUTHORITY), [])
        self.assertIn(tc.ONE_WRITE_AUTHORITY.name, {f.rule for f in report.unverifiable})

    def test_spans_end_at_the_step_that_gives_authority_up(self) -> None:
        run = Run()
        failover(run)
        spans = tc.authority_spans(run.trace().nodes[1])
        self.assertEqual(spans, [tc.Span(1, 100, 2_000, certain=True)])


class VoteTest(unittest.TestCase):
    def test_an_empty_voter_may_grant_anyone(self) -> None:
        # IsLSNAcceptable against a position of zero.
        run = Run()
        run.raft(3, 0, term=1, leader=None)
        run.vote(3, 10, term=2, candidate=2, candidate_lsn=None, voter_wal="empty")
        self.assertEqual(checked(run).violated(tc.VOTE_LSN_ACCEPTABLE), [])

    def test_a_grant_over_an_unread_wal_is_caught(self) -> None:
        run = Run()
        run.vote(3, 10, term=2, candidate=2, voter_wal="unknown")
        self.assertNotEqual(checked(run).violated(tc.VOTE_LSN_ACCEPTABLE), [])

    def test_the_threshold_admits_a_candidate_behind_by_it(self) -> None:
        run = Run()
        run.vote(3, 10, term=2, candidate=2, candidate_lsn=400, threshold=100)
        self.assertEqual(checked(run).violated(tc.VOTE_LSN_ACCEPTABLE), [])
        run.vote(3, 20, term=3, candidate=2, candidate_lsn=399, threshold=100)
        self.assertNotEqual(checked(run).violated(tc.VOTE_LSN_ACCEPTABLE), [])

    def test_a_grant_below_the_voters_own_term_is_caught(self) -> None:
        run = Run()
        run.raft(3, 0, term=5, leader=None)
        run.vote(3, 10, term=4, candidate=2)
        self.assertNotEqual(checked(run).violated(tc.ONE_VOTE_PER_TERM), [])

    def test_a_leader_unseen_by_absent_voters_is_unverifiable(self) -> None:
        # node2 and node3 joined after node1 led term 1.
        run = Run()
        bootstrap(run)
        report = checked(run)
        self.assertEqual(report.violated(tc.LEADER_HAS_QUORUM), [])
        self.assertIn(tc.LEADER_HAS_QUORUM.name, {f.rule for f in report.unverifiable})


class BoundaryTest(unittest.TestCase):
    def test_holes_are_every_sequence_number_never_read(self) -> None:
        run = Run()
        run.add(1, 0, event_type="leader_change")
        run.raft(1, 1, term=1, leader=1)
        run.add(1, 2, event_type="fence")
        trace = run.trace(evicted=frozenset({(1, 1)}))
        node = trace.nodes[1]
        self.assertEqual(node.missing, (1,))
        self.assertEqual(len(node.raft), 1, "only spec events become steps")
        self.assertFalse(node.known_before(2))
        self.assertTrue(node.gap_free(1, 3))

    def test_an_event_of_another_node_is_refused(self) -> None:
        event = api_models.DebugEvent(
            seq=1, timestamp_ms=0, event_type="spec", node_id=2, data={"kind": "open"}
        )
        with self.assertRaises(tc.TraceParseError):
            tc.node_trace(1, [event], current_seq=1)

    def test_a_malformed_spec_event_is_refused(self) -> None:
        event = api_models.DebugEvent(
            seq=1, timestamp_ms=0, event_type="spec", node_id=1, data={"kind": "raft"}
        )
        with self.assertRaises(ValueError):
            tc.node_trace(1, [event], current_seq=1)


if __name__ == "__main__":
    unittest.main()
