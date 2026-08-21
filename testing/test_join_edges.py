#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the H-16 join-edge harness.

The live cases each drive one edge and read one observable back. What is tested
here is the reading: the target choice that keeps a case pointed at the edge it
was written for, and the parsing that turns an answer into a verdict. A checker
that cannot fail is worse than no checker, so every case below has its
inversion.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

import api_models
import fault_primitives as fp
import join_edges as je

REPO_ROOT = Path(__file__).resolve().parent.parent


class WipeTargetTests(unittest.TestCase):
    """Wiping the bootstrap node exercises re-provisioning, not a rejoin.

    It answers an empty data directory with `initdb`, so a case that means to
    watch a join would instead be watching a node mint a new lineage.
    """

    def test_the_bootstrap_node_is_never_the_wipe_target(self) -> None:
        for leader in fp.NODES:
            self.assertIn(je.wipe_target(leader), fp.JOINING_NODES)

    def test_the_leader_is_never_the_wipe_target(self) -> None:
        """The case deposes the leader; wiping it too tests nothing."""
        for leader in fp.NODES:
            self.assertNotEqual(je.wipe_target(leader), leader)

    def test_no_target_is_an_error_rather_than_a_silent_skip(self) -> None:
        """A topology with one joining node that is also the leader leaves
        nothing to wipe. Raising says so; returning the leader would wipe the
        cluster's only source."""
        joining = fp.JOINING_NODES
        try:
            fp.JOINING_NODES = (joining[0],)  # type: ignore[misc]
            with self.assertRaises(je.EdgeError):
                je.wipe_target(joining[0])
        finally:
            fp.JOINING_NODES = joining  # type: ignore[misc]


class LineageTests(unittest.TestCase):
    """The bootstrap-wiped case turns on one number: does the returning node
    report the cluster's lineage, or one it minted itself?"""

    def test_a_reported_lineage_is_read(self) -> None:
        parsed = api_models.parse(
            api_models.ClusterIdentity,
            '{"node_id": 1, "cluster_lineage": 7675741024400711708}',
        )
        self.assertEqual(parsed.cluster_lineage, 7_675_741_024_400_711_708)

    def test_a_node_with_no_data_directory_reports_no_lineage(self) -> None:
        """A witness has nothing to compare, which is not a mismatch."""
        parsed = api_models.parse(
            api_models.ClusterIdentity, '{"node_id": 4, "cluster_lineage": null}'
        )
        self.assertIsNone(parsed.cluster_lineage)

    def test_an_unreadable_body_is_not_a_lineage(self) -> None:
        """An unreachable node must not read as agreement with anything."""
        self.assertIsNone(api_models.parse_or_none(api_models.ClusterIdentity, "not json"))
        self.assertIsNone(api_models.parse_or_none(api_models.ClusterIdentity, ""))


class MemberIdTests(unittest.TestCase):
    def test_members_are_read_as_ids(self) -> None:
        body = (
            '{"success": true, "message": "3 members in cluster", "members": ['
            '{"node_id": 1, "addr": "172.28.0.11:5433", "role": "Voter"},'
            '{"node_id": 2, "addr": "172.28.0.12:5433", "role": "Voter"},'
            '{"node_id": 3, "addr": "172.28.0.13:5433", "role": "Voter"}]}'
        )
        self.assertEqual(api_models.parse(api_models.Members, body).node_ids, {1, 2, 3})

    def test_an_unreadable_membership_is_empty_rather_than_wrong(self) -> None:
        """`run_learner_crash` compares membership before and after. An
        unreadable answer must not look like a set that happens to match."""
        self.assertIsNone(api_models.parse_or_none(api_models.Members, "<html>502</html>"))


class SlotBudgetTests(unittest.TestCase):
    def test_the_orphan_slot_budget_comes_from_the_reconciler(self) -> None:
        """Restating the interval here would let the two drift and turn a
        bounded assertion into whatever number was typed."""
        constants = (REPO_ROOT / "src" / "config" / "constants.rs").read_text(encoding="utf-8")
        expected_ms = (
            fp.parse_rust_u64_const(constants, "REPLICATION_SLOT_ENSURE_INTERVAL_SECS") * 1_000
        )
        self.assertEqual(fp.read_system_timings().slot_ensure_interval_ms, expected_ms)

    def test_the_slot_prefix_comes_from_the_rust_source(self) -> None:
        """A slot named anything else is a foreign slot the reconciler
        deliberately never drops, so a harness that spelled it itself would
        assert against a name nothing owns — which is how this case first
        reported a failure that was its own."""
        self.assertEqual(fp.replication_slot_prefix(), "replica_")


class ResultTests(unittest.TestCase):
    def test_a_failing_case_is_not_ok(self) -> None:
        self.assertFalse(je.Result(je.Case.ORPHAN_SLOT, "still present", ok=False).ok)

    def test_every_case_is_dispatched(self) -> None:
        """A case in the enum with no arm in `run` would silently take the
        final else and drive the wrong edge."""
        self.assertEqual(
            {c.value for c in je.Case},
            {
                "deposed-mid-copy",
                "clone-interrupted",
                "orphan-slot",
                "learner-crash",
                "bootstrap-wiped",
            },
        )


class CloneInterruptedTests(unittest.TestCase):
    """The verdict turns on reading a clone failure out of one node's log. A
    node that failed a clone in an earlier case would otherwise satisfy this one
    without the fault having landed at all, so the read is scoped to the
    incarnation the case started."""

    def test_the_basebackup_message_is_evidence_the_clone_failed(self) -> None:
        log = 'pg_basebackup: error: could not send replication command "START_REPLICATION"'
        with mock.patch.object(je, "_sh", return_value=(0, log)) as shell:
            self.assertTrue(je.clone_failed_since("node3", "2026-08-21T18:00:00Z"))
        self.assertIn("--since 2026-08-21T18:00:00Z", shell.call_args.args[0])

    def test_pgbatterys_own_retry_line_is_evidence(self) -> None:
        log = "WARN pgbattery::app: Clone from the leader failed; discarding what it left"
        with mock.patch.object(je, "_sh", return_value=(0, log)):
            self.assertTrue(je.clone_failed_since("node3", "2026-08-21T18:00:00Z"))

    def test_a_clean_log_is_not_evidence(self) -> None:
        with mock.patch.object(je, "_sh", return_value=(0, "pg_basebackup: completed")):
            self.assertFalse(je.clone_failed_since("node3", "2026-08-21T18:00:00Z"))

    def test_an_unreadable_log_is_not_evidence(self) -> None:
        with mock.patch.object(je, "_sh", return_value=(1, "no such service")):
            self.assertFalse(je.clone_failed_since("node3", "2026-08-21T18:00:00Z"))


if __name__ == "__main__":
    unittest.main()
