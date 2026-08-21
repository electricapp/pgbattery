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


class HardeningCitationTest(unittest.TestCase):
    """The register is cited as the place the reasoning lives. A citation of an
    item that was renumbered sends a reader looking for an argument that is not
    there, in a document long enough that nobody would notice."""

    def test_the_repository_as_committed_cites_only_real_items(self) -> None:
        lm.check_hardening_citations_resolve()

    def test_a_citation_of_a_missing_item_is_caught(self) -> None:
        self.assertEqual(
            lm.unresolved_hardening_citations("see H-99 for the argument", {"H-01"}),
            {"H-99"},
        )

    def test_a_citation_of_a_real_item_is_not(self) -> None:
        self.assertEqual(lm.unresolved_hardening_citations("see H-01", {"H-01"}), set())


class DerivedConstantTest(unittest.TestCase):
    """Fault timings are read from the Rust source rather than restated. A
    renamed constant otherwise surfaces minutes into a docker run, as a
    precondition failure that reads like a broken cluster."""

    def test_every_constant_the_harness_derives_is_found(self) -> None:
        lm.check_derived_rust_constants_resolve()


class ClusterPortTest(unittest.TestCase):
    """A harness that spells a published port itself reaches nothing when the
    port moves, and an unreachable node is recorded `indeterminate` — which the
    L1 verdict reads as no acceptance observed. It passes while blind."""

    PORTS = (5432, 5433, 5434)

    def test_the_harness_as_committed_derives_every_port(self) -> None:
        lm.check_harness_derives_cluster_ports()

    def test_a_restated_port_is_caught(self) -> None:
        found = lm.restated_cluster_ports("PORTS = [5432, 5433, 5434]\n", self.PORTS)
        self.assertEqual(len(found), 3, found)

    def test_a_derived_port_is_not(self) -> None:
        source = "PORTS = [topology.GATEWAY_PORT_BY_NODE[n] for n in NODES]\n"
        self.assertEqual(lm.restated_cluster_ports(source, self.PORTS), [])

    def test_a_literal_inside_a_function_is_left_alone(self) -> None:
        # Fixture data in a test asserts a value rather than reaching a node,
        # and flagging it is what a line-by-line scan does wrong.
        source = "def test_x():\n    assert node.addr == '172.28.0.12:5433'\n    return 5434\n"
        self.assertEqual(lm.restated_cluster_ports(source, self.PORTS), [])


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


class MembershipWaitTest(unittest.TestCase):
    """A wait for fewer members than the cluster has can only time out."""

    def test_the_matrix_as_committed_passes(self) -> None:
        lm.check_waits_do_not_ask_membership_to_shrink()

    def test_a_wait_for_one_fewer_member_after_a_kill_is_caught(self) -> None:
        """Two nightly cases were written this way and neither could ever
        pass; both sat behind an earlier failure where nothing ran them."""
        cases = [
            {
                "id": "kills-then-waits-for-two",
                "actions": [
                    {"type": "cmd", "cmd": "docker compose kill node1"},
                    {"type": "wait_cluster", "nodes": 2, "leaders": 1},
                ],
            }
        ]
        problems = lm.waits_that_ask_membership_to_shrink(cases, 3)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("live_nodes=2", problems[0])

    def test_saying_live_nodes_instead_is_accepted(self) -> None:
        cases = [
            {
                "id": "says-which-are-live",
                "actions": [{"type": "wait_cluster", "nodes": 3, "live_nodes": 2, "leaders": 1}],
            }
        ]
        self.assertEqual(lm.waits_that_ask_membership_to_shrink(cases, 3), [])

    def test_a_topology_that_adds_a_member_is_left_alone(self) -> None:
        """`witness-topology` joins a fourth node and waits for four."""
        cases = [
            {
                "id": "adds-a-witness",
                "actions": [{"type": "wait_cluster", "nodes": 4, "leaders": 1}],
            }
        ]
        self.assertEqual(lm.waits_that_ask_membership_to_shrink(cases, 3), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
