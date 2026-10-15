#!/usr/bin/env -S uv run --project testing python
"""Proof that the settle barrier refuses an unsettled cluster and admits a settled one.

Three upgrade proofs started their phases on "a leader exists" while two nodes
were still learners mid-join: one refused on `doctor --strict`, one measured a
one-voter blue cluster. The barrier is the fix, so it has to be able to say no
to exactly those shapes, and a barrier that always says yes would pass every
case below that expects a settled answer.

Pure: observations are built by hand and the poller is handed a fake observer,
so nothing here needs docker.

Run with:
    uv run --project testing python testing/test_settle.py
"""

from __future__ import annotations

import unittest
from collections.abc import Sequence
from typing import Final

import api_models
import settle
from settle import ClusterNotSettled, SettleObservation, settle_problems
from topology import REPO_ROOT, Node

EXPECTED: Final[frozenset[int]] = frozenset({1, 2, 3})


def node(node_id: int) -> Node:
    return Node(
        service=f"node{node_id}",
        node_id=node_id,
        ip=f"172.28.0.1{node_id}",
        gateway_port=5_431 + node_id,
        metrics_port=9_090 + node_id,
        mgmt_port=9_080 + node_id,
        is_voter=True,
        is_bootstrap=node_id == 1,
    )


NODES: Final[tuple[Node, ...]] = (node(1), node(2), node(3))


def members(roles: dict[int, str]) -> api_models.Members:
    return api_models.Members(
        success=True,
        members=[
            api_models.Member(node_id=i, addr=f"172.28.0.1{i}:5433", role=r)
            for i, r in sorted(roles.items())
        ],
    )


def observation(
    *,
    views: dict[int, int | None] | None = None,
    roles: dict[int, str] | None = None,
    confirmed: float | None = 1.0,
    required: float | None = 1.0,
) -> SettleObservation:
    """A settled three-voter cluster unless a field says otherwise."""
    return SettleObservation(
        expected_voters=EXPECTED,
        leader_views={1: 1, 2: 1, 3: 1} if views is None else views,
        members=members({1: "voter", 2: "voter", 3: "voter"} if roles is None else roles),
        sync_confirmed=confirmed,
        sync_required=required,
    )


class SettledClusterPasses(unittest.TestCase):
    """Without these, every refusal below would also pass against a barrier
    that refused everything."""

    def test_full_voter_set_agreed_leader_and_sync(self) -> None:
        self.assertEqual(settle_problems(observation()), [])

    def test_nothing_required_is_settled_without_a_sync_standby(self) -> None:
        self.assertEqual(settle_problems(observation(confirmed=0.0, required=0.0)), [])


class UnsettledMembershipFails(unittest.TestCase):
    def test_learners_mid_join(self) -> None:
        """The shape the binary-upgrade proof started its roll against."""
        problems = settle_problems(
            observation(roles={1: "voter", 2: "learner", 3: "learner"}, confirmed=0.0)
        )
        joined = " ".join(problems)
        self.assertIn("learners [2, 3]", joined)
        self.assertIn("voters [1]", joined)
        self.assertIn("0 of 1", joined)

    def test_one_voter_and_nothing_else(self) -> None:
        """'blue has 1 voters': the joins had not registered at all yet."""
        problems = settle_problems(observation(roles={1: "voter"}))
        self.assertIn("voters [1], expected exactly [1, 2, 3]", " ".join(problems))

    def test_a_voter_the_topology_does_not_know(self) -> None:
        roles = {1: "voter", 2: "voter", 3: "voter", 9: "voter"}
        self.assertTrue(settle_problems(observation(roles=roles)))

    def test_unknown_role(self) -> None:
        roles = {1: "voter", 2: "voter", 3: "unknown"}
        self.assertIn("unknown roles", " ".join(settle_problems(observation(roles=roles))))

    def test_unread_membership(self) -> None:
        unread = SettleObservation(
            expected_voters=EXPECTED,
            leader_views={1: 1, 2: 1, 3: 1},
            members=None,
            sync_confirmed=1.0,
            sync_required=1.0,
        )
        self.assertTrue(settle_problems(unread))


class UnsettledLeadershipFails(unittest.TestCase):
    def test_disagreeing_views(self) -> None:
        self.assertTrue(settle_problems(observation(views={1: 1, 2: 1, 3: 2})))

    def test_an_unanswering_node_is_not_agreement(self) -> None:
        self.assertTrue(settle_problems(observation(views={1: 1, 2: 1, 3: None})))

    def test_no_node_names_a_leader(self) -> None:
        self.assertTrue(settle_problems(observation(views={1: None, 2: None, 3: None})))

    def test_agreed_leader_outside_the_voter_set(self) -> None:
        self.assertTrue(settle_problems(observation(views={1: 7, 2: 7, 3: 7})))


class UnsettledSyncFails(unittest.TestCase):
    """The same rule as doctor's `replication:sync`."""

    def test_no_standby_sync_yet(self) -> None:
        self.assertTrue(settle_problems(observation(confirmed=0.0)))

    def test_fewer_than_required(self) -> None:
        self.assertTrue(settle_problems(observation(confirmed=1.0, required=2.0)))

    def test_leader_publishes_no_evidence(self) -> None:
        self.assertTrue(settle_problems(observation(confirmed=None, required=None)))
        self.assertTrue(settle_problems(observation(confirmed=None, required=1.0)))


class BarrierPolls(unittest.TestCase):
    """The poller around the decision: bounded, loud, and returns the leader."""

    def test_unsettled_cluster_raises_with_what_it_saw(self) -> None:
        unsettled = observation(roles={1: "voter", 2: "learner", 3: "learner"}, confirmed=0.0)

        def observe(_nodes: Sequence[Node]) -> SettleObservation:
            return unsettled

        with self.assertRaises(ClusterNotSettled) as raised:
            settle.await_settled(NODES, budget=0.05, interval=0.01, observe_round=observe)
        message = str(raised.exception)
        self.assertIn("learners [2, 3]", message)
        self.assertIn("'node3': 1", message)
        self.assertIn("(2, 'learner')", message)

    def test_settles_once_the_cluster_does(self) -> None:
        rounds = [
            observation(roles={1: "voter", 2: "learner", 3: "learner"}, confirmed=0.0),
            observation(views={1: 2, 2: 2, 3: 2}),
        ]

        def observe(_nodes: Sequence[Node]) -> SettleObservation:
            return rounds.pop(0) if len(rounds) > 1 else rounds[0]

        leader = settle.await_settled(NODES, budget=5.0, interval=0.01, observe_round=observe)
        self.assertEqual(leader.service, "node2")

    def test_no_voters_refuses_before_polling(self) -> None:
        learner_only = (Node("w", 4, "172.28.0.14", 5_436, 9_095, 9_085, False, False),)
        with self.assertRaises(ClusterNotSettled):
            settle.await_settled(learner_only, budget=0.05, interval=0.01)


class EvidenceNamesMatchTheBinary(unittest.TestCase):
    """The gauges the barrier reads must be the ones the leader publishes."""

    def test_metric_names_appear_in_the_rust_source(self) -> None:
        source = (REPO_ROOT / "src/observability/metrics.rs").read_text(encoding="utf-8")
        for name in (settle.SYNC_CONFIRMED_METRIC, settle.SYNC_REQUIRED_METRIC):
            self.assertIn(f'"{name}"', source)


if __name__ == "__main__":
    unittest.main()
