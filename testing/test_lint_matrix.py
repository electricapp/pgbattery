#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the harness lint's documentation checks.

Run with:
    uv run --project testing python testing/test_lint_matrix.py
"""

from __future__ import annotations

import dataclasses
import unittest
from pathlib import Path
from unittest import mock

import fault_primitives as fp
import lint_matrix as lm


class ProseFileReferenceTest(unittest.TestCase):
    """Markdown links in documentation must name files that exist."""

    def test_the_documents_as_committed_pass(self) -> None:
        lm.check_prose_file_references_resolve()

    def test_a_path_naming_nothing_is_caught(self) -> None:
        with (
            mock.patch.object(lm, "PROSE_DOCS", ("CLAUDE.md",)),
            mock.patch.object(Path, "read_text", return_value="see [gone](docs/NO_SUCH_FILE.md)"),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_prose_file_references_resolve()
        self.assertIn("NO_SUCH_FILE.md", str(caught.exception))

    def test_a_basename_link_is_enough_to_resolve(self) -> None:
        with (
            mock.patch.object(lm, "PROSE_DOCS", ("CLAUDE.md",)),
            mock.patch.object(Path, "read_text", return_value="see [x](torn_raft.py)"),
        ):
            lm.check_prose_file_references_resolve()

    def test_a_backtick_path_is_not_scanned(self) -> None:
        """Freeform backticks must not freeze essay wording."""
        with (
            mock.patch.object(lm, "PROSE_DOCS", ("CLAUDE.md",)),
            mock.patch.object(Path, "read_text", return_value="see `docs/NO_SUCH_FILE.md`"),
        ):
            lm.check_prose_file_references_resolve()


class DerivedConstantTest(unittest.TestCase):
    def test_every_constant_the_harness_derives_is_found(self) -> None:
        lm.check_derived_rust_constants_resolve()


class ClusterPortTest(unittest.TestCase):
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
        source = "def test_x():\n    assert node.addr == '172.28.0.12:5433'\n    return 5434\n"
        self.assertEqual(lm.restated_cluster_ports(source, self.PORTS), [])


class FinalTypeTest(unittest.TestCase):
    def test_the_python_as_committed_types_every_final(self) -> None:
        lm.check_finals_state_their_type()

    def test_a_bare_final_is_caught(self) -> None:
        source = "A: Final[int] = 1\nB: Final = 2\nC:Final=3\nD: Final[re.Pattern[str]] = x\n"
        self.assertEqual(lm.bare_finals(source), [2, 3])


class ProseExemptionTest(unittest.TestCase):
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
    def test_the_matrix_as_committed_passes(self) -> None:
        lm.check_waits_do_not_ask_membership_to_shrink()

    def test_a_wait_for_one_fewer_member_after_a_kill_is_caught(self) -> None:
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
        cases = [
            {
                "id": "adds-a-witness",
                "actions": [{"type": "wait_cluster", "nodes": 4, "leaders": 1}],
            }
        ]
        self.assertEqual(lm.waits_that_ask_membership_to_shrink(cases, 3), [])


class StateMachineInventoryTest(unittest.TestCase):
    """Inventory TOML is pinned to the code — not markdown backticks."""

    def test_the_inventory_as_committed_passes(self) -> None:
        lm.check_state_machine_names_resolve()
        lm.check_state_machine_describes_whole_enums()
        lm.check_state_machine_foreign_names_are_still_needed()
        lm.check_no_cluster_state_leader_projection()

    def test_a_type_the_workspace_no_longer_defines_is_caught(self) -> None:
        inv = {
            "enums": {},
            "types": ["TheRenamedThing"],
            "foreign": {},
        }
        with (
            mock.patch.object(lm, "load_state_machine_inventory", return_value=inv),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_state_machine_names_resolve()
        self.assertIn("TheRenamedThing", str(caught.exception))

    def test_an_acronym_is_not_mistaken_for_a_type(self) -> None:
        self.assertFalse(lm.is_rust_type_name("WAL"))
        self.assertFalse(lm.is_rust_type_name("WHERE"))
        self.assertTrue(lm.is_rust_type_name("StandbyAction"))

    def test_a_variant_the_inventory_omits_is_caught(self) -> None:
        inv = {
            "enums": {"StandbyAction": ["NoOp", "Defer"]},
            "types": [],
            "foreign": {},
        }
        index = {"StandbyAction": {"NoOp", "Defer", "TheNewOne"}}
        with (
            mock.patch.object(lm, "load_state_machine_inventory", return_value=inv),
            mock.patch.object(lm, "enum_variant_index", return_value=index),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_state_machine_describes_whole_enums()
        self.assertIn("TheNewOne", str(caught.exception))

    def test_a_variant_after_an_attribute_holding_braces_is_still_seen(self) -> None:
        source = (
            "pub enum Error {\n"
            '    #[error("Connection idle timeout (id={conn_id}, limit={idle_ms}ms)")]\n'
            "    IdleTimeout { conn_id: u64, idle_ms: u64 },\n"
            '    #[error("later: {0}")]\n'
            "    ArrivesAfterTheBraces(String),\n"
            "}\n"
        )
        with mock.patch.object(Path, "read_text", return_value=source):
            index = lm.enum_variant_index([Path("error.rs")])
        self.assertEqual(index["Error"], {"IdleTimeout", "ArrivesAfterTheBraces"})

    def test_a_type_under_any_visibility_is_seen(self) -> None:
        source = (
            "pub(super) enum Restricted {\n    Seen,\n}\n"
            "pub(in crate::process) struct Scoped;\n"
            "enum Private {\n    Also,\n}\n"
        )
        with mock.patch.object(Path, "read_text", return_value=source):
            declared = lm.declared_types([Path("follow.rs")])
            index = lm.enum_variant_index([Path("follow.rs")])
        self.assertLessEqual({"Restricted", "Scoped", "Private"}, declared.keys())
        self.assertEqual(index["Restricted"], {"Seen"})

    def test_a_foreign_name_the_workspace_now_defines_is_caught(self) -> None:
        with (
            mock.patch.object(
                lm, "STATE_MACHINE_FOREIGN_TYPES", {"StandbyAction": "claimed to be foreign"}
            ),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_state_machine_foreign_names_are_still_needed()
        self.assertIn("StandbyAction", str(caught.exception))


class SpecCodeReferenceTest(unittest.TestCase):
    """tla/code_map.toml must resolve to real workspace code."""

    def test_the_map_as_committed_resolves(self) -> None:
        lm.check_spec_code_references_resolve()

    def test_a_prose_word_beside_a_path_is_not_read_as_a_symbol(self) -> None:
        self.assertEqual(lm.code_identifiers_named("CODE: app.rs bootstrap path."), set())

    def test_a_call_with_arguments_names_only_its_receiver(self) -> None:
        self.assertEqual(
            lm.code_identifiers_named("lsn[n] -> state_machine.rs: node_lsns.get(&n)"),
            {"node_lsns"},
        )

    def test_empty_parentheses_mark_a_name_with_no_underscore(self) -> None:
        self.assertEqual(lm.code_identifiers_named("CODE: governor/lease.rs renew()."), {"renew"})

    def test_a_reference_resolves_by_path_suffix_not_basename(self) -> None:
        sources = lm.rust_sources()
        resolved = lm.resolve_rust_path("gateway/handlers/mod.rs", sources)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertTrue(str(resolved).endswith("src/gateway/handlers/mod.rs"), resolved)

    def test_a_source_the_map_names_that_is_gone_is_caught(self) -> None:
        with (
            mock.patch.object(
                lm,
                "load_tla_code_map",
                return_value={"lease_fencing": {"governor/departed.rs": ["still_here"]}},
            ),
            mock.patch.object(lm, "tla_specs", return_value=[Path("tla/lease_fencing.tla")]),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_spec_code_references_resolve()
        self.assertIn("departed.rs", str(caught.exception))

    def test_a_function_renamed_out_from_under_the_map_is_caught(self) -> None:
        with (
            mock.patch.object(
                lm,
                "load_tla_code_map",
                return_value={"lease_fencing": {"governor/lease.rs": ["renew_the_lease"]}},
            ),
            mock.patch.object(lm, "tla_specs", return_value=[Path("tla/lease_fencing.tla")]),
            self.assertRaises(AssertionError) as caught,
        ):
            lm.check_spec_code_references_resolve()
        self.assertIn("renew_the_lease", str(caught.exception))


class LeaseFencingTimingTest(unittest.TestCase):
    def test_the_models_as_committed_state_the_shipped_timings(self) -> None:
        lm.check_lease_fencing_model_matches_the_shipped_timings()

    def test_a_vote_delay_inside_the_safe_region_is_caught(self) -> None:
        timings = fp.read_system_timings()
        problems = lm.lease_fencing_tick_problems(
            {"QuorumTimeout": 2, "LeaseDuration": 4, "HoldDown": 4, "VoteDelay": 1}, timings
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("VoteDelay", problems[0])

    def test_a_retuned_quorum_timeout_is_caught(self) -> None:
        timings = dataclasses.replace(fp.read_system_timings(), quorum_timeout_ms=1_500)
        problems = lm.lease_fencing_tick_problems(
            {"QuorumTimeout": 2, "LeaseDuration": 4, "HoldDown": 4, "VoteDelay": 2}, timings
        )
        self.assertEqual(len(problems), 2, problems)
        self.assertIn("QuorumTimeout", " ".join(problems))

    def test_a_config_that_assigns_nothing_is_caught(self) -> None:
        problems = lm.lease_fencing_tick_problems({}, fp.read_system_timings())
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("assigns no", problems[0])

    def test_a_commented_ratio_is_not_read_as_an_assignment(self) -> None:
        text = "\\* HoldDown = 99 is what the comment says\nHoldDown = 4\n"
        self.assertEqual(lm.tla_config_constants(text), {"HoldDown": 4})


if __name__ == "__main__":
    unittest.main(verbosity=2)
