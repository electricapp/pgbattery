#!/usr/bin/env -S uv run --project testing python
"""Inversions for the Track C contracts U1-U5.

Each test drives one oracle in `bluegreen_cutover.py` to its failing answer.
A contract whose inversion has never gone red is a contract whose oracle has
never been shown to work.

Run with:
    uv run --project testing python testing/test_bluegreen_oracles.py
"""

from __future__ import annotations

import os
import unittest
from typing import Any
from unittest import mock

os.environ.setdefault("COMPOSE_FILE", "docker-compose.bluegreen.yml")

import bluegreen_cutover as bg
from topology import Node


def _node(service: str = "blue1", ip: str = "10.0.0.1") -> Node:
    """A real `Node` so the oracles are exercised through their real types."""
    return Node(
        service=service,
        node_id=1,
        ip=ip,
        gateway_port=5432,
        metrics_port=9091,
        mgmt_port=9081,
        is_voter=True,
        is_bootstrap=service.endswith("1"),
    )


class FenceProbeClassificationTest(unittest.TestCase):
    """The probe tells the fence's own denial apart from a probe that is broken."""

    def test_an_acked_write_reads_as_allowed(self) -> None:
        self.assertTrue(bg._classify_write_probe(0, "INSERT 0 1", ""))

    def test_the_revoke_fence_denial_reads_as_fenced(self) -> None:
        self.assertFalse(
            bg._classify_write_probe(1, "", "ERROR:  permission denied for table bg_kv")
        )

    def test_the_read_only_fence_denial_reads_as_fenced(self) -> None:
        self.assertFalse(
            bg._classify_write_probe(
                1, "", "ERROR:  cannot execute INSERT in a read-only transaction"
            )
        )

    def test_a_missing_role_is_a_broken_probe_not_a_fence(self) -> None:
        """The classifier this replaced read every non-zero exit as fenced, so a
        green with no bg_app role passed the boot-fence assertion unimplemented."""
        err = 'psql: error: FATAL:  role "bg_app" does not exist'
        with self.assertRaises(RuntimeError):
            bg._classify_write_probe(2, "", err)

    def test_a_missing_table_is_a_broken_probe_not_a_fence(self) -> None:
        with self.assertRaises(RuntimeError):
            bg._classify_write_probe(1, "", 'ERROR:  relation "bg_kv" does not exist')


class BootFenceTest(unittest.TestCase):
    """U1: the boot-fence assertion passes only on an explicit denial."""

    def test_a_green_that_keeps_accepting_writes_is_refused(self) -> None:
        with (
            mock.patch.object(bg, "_psql_allowed", return_value=True),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._assert_green_boot_fenced(_node("green1"), budget=0.2)
        self.assertIn("U1", str(caught.exception))

    def test_an_explicit_denial_passes_and_removes_the_probe_row(self) -> None:
        deletes: list[str] = []

        def record_delete(_node: Any, sql: str, **_kw: Any) -> str:
            deletes.append(sql)
            return ""

        with (
            mock.patch.object(bg, "_psql_allowed", return_value=False),
            mock.patch.object(bg, "_psql", side_effect=record_delete),
        ):
            bg._assert_green_boot_fenced(_node("green1"), budget=0.2)
        self.assertTrue(any("DELETE" in sql for sql in deletes))

    def test_a_broken_probe_raises_instead_of_reading_as_fenced(self) -> None:
        with (
            mock.patch.object(bg, "_psql_allowed", side_effect=RuntimeError("role missing")),
            self.assertRaises(RuntimeError),
        ):
            bg._assert_green_boot_fenced(_node("green1"), budget=0.2)


class DrainCompletenessTest(unittest.TestCase):
    """U2: every write blue acked before SEAL is present on green."""

    def test_a_drain_short_of_the_final_lsn_is_refused(self) -> None:
        with (
            mock.patch.object(bg, "_slot_confirmed_lsn", return_value=100),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._await_slot_confirms(_node(), target_lsn=500, timeout=0.2)
        self.assertIn("U2", str(caught.exception))
        self.assertIn("500", str(caught.exception))

    def test_a_confirmed_slot_ends_the_wait(self) -> None:
        with mock.patch.object(bg, "_slot_confirmed_lsn", return_value=600):
            self.assertEqual(bg._await_slot_confirms(_node(), 500, timeout=5.0), 600)

    def test_matching_row_counts_do_not_prove_a_drain(self) -> None:
        """The oracle this replaced compared counts, which an UPDATE moves past."""
        acked = {"pre-0", "pre-1", "pre-2"}
        # Green holds three rows, so any count check passes — but one of them
        # is not a key blue acked.
        present = "pre-0\npre-1\nother-9"
        with (
            mock.patch.object(bg, "_psql", return_value=present),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._assert_drain_complete(_node(), acked)
        self.assertIn("pre-2", str(caught.exception))

    def test_a_run_with_no_acked_writes_is_refused(self) -> None:
        with self.assertRaises(bg.ContractFailure) as caught:
            bg._assert_drain_complete(_node(), set())
        self.assertIn("vacuous", str(caught.exception))


class SequenceNonCollisionTest(unittest.TestCase):
    """U3: green must not issue a value blue already used."""

    def test_an_unadvanced_sequence_is_caught(self) -> None:
        with (
            mock.patch.object(bg, "_psql", return_value="7"),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._assert_no_sequence_collision(_node(), blue_max=42)
        self.assertIn("U3 violated", str(caught.exception))

    def test_an_advanced_sequence_passes(self) -> None:
        with mock.patch.object(bg, "_psql", return_value="1042"):
            bg._assert_no_sequence_collision(_node(), blue_max=42)

    def test_a_sequence_that_setval_cannot_reach_is_refused(self) -> None:
        """A plain bigint column owns no sequence, so setval is a silent no-op."""
        calls: list[str] = []

        def fake_psql(_node: Any, sql: str, **_kw: Any) -> str:
            calls.append(sql)
            return "" if "pg_get_serial_sequence" in sql else "5"

        with (
            mock.patch.object(bg, "_psql", side_effect=fake_psql),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._advance_sequences(_node("green1"), _node("blue1"))
        self.assertIn("no-op", str(caught.exception))
        self.assertFalse(
            any("setval" in c for c in calls),
            "must refuse before issuing a setval that would do nothing",
        )


class SingleAuthorityTest(unittest.TestCase):
    """U1/U4: exactly one cluster reports authority, and it fails closed."""

    def test_a_cluster_without_authority_never_reports_holding_it(self) -> None:
        statuses = {
            "blue1": {"holds_client_write_authority": True},
            "green1": {"holds_client_write_authority": True, "epoch": 1},
        }
        with (
            mock.patch.object(bg, "_cutover_status", side_effect=lambda n: statuses[n.service]),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._assert_authority_is_green_only(
                [_node("blue1")], _node("green1", "10.0.0.4"), epoch=1
            )
        self.assertIn("U1 violated", str(caught.exception))

    def test_a_recipient_that_never_took_authority_is_caught(self) -> None:
        statuses = {
            "blue1": {"holds_client_write_authority": False},
            "green1": {"holds_client_write_authority": False, "epoch": 1},
        }
        with (
            mock.patch.object(bg, "_cutover_status", side_effect=lambda n: statuses[n.service]),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._assert_authority_is_green_only(
                [_node("blue1")], _node("green1", "10.0.0.4"), epoch=1
            )
        self.assertIn("did not take authority", str(caught.exception))

    def test_an_epoch_mismatch_is_caught(self) -> None:
        statuses = {
            "blue1": {"holds_client_write_authority": False},
            "green1": {"holds_client_write_authority": True, "epoch": 5},
        }
        with (
            mock.patch.object(bg, "_cutover_status", side_effect=lambda n: statuses[n.service]),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._assert_authority_is_green_only(
                [_node("blue1")], _node("green1", "10.0.0.4"), epoch=1
            )
        self.assertIn("epoch", str(caught.exception))

    def test_the_happy_shape_passes(self) -> None:
        statuses = {
            "blue1": {"holds_client_write_authority": False},
            "green1": {"holds_client_write_authority": True, "epoch": 1},
        }
        with mock.patch.object(bg, "_cutover_status", side_effect=lambda n: statuses[n.service]):
            bg._assert_authority_is_green_only(
                [_node("blue1")], _node("green1", "10.0.0.4"), epoch=1
            )


class FenceCoverageTest(unittest.TestCase):
    """U1: the fence must hold on the internal port, not just the gateway."""

    def test_a_fence_that_covers_nothing_is_refused(self) -> None:
        """SEAL reports success but the app role still writes to blue's PG."""
        posts: list[str] = []

        def fake_post(_node: Any, path: str, body: Any = None) -> Any:
            posts.append(path)
            return mock.Mock(status_code=200, raise_for_status=lambda: None)

        with (
            mock.patch.object(bg, "_post", side_effect=fake_post),
            mock.patch.object(
                bg,
                "_cutover_status",
                return_value={
                    "sealed_final_lsn": 1_000,
                    "holds_client_write_authority": False,
                },
            ),
            # The fence did not hold: the app role still writes.
            mock.patch.object(bg, "_psql_allowed", return_value=True),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._cutover(_node("blue1"), _node("green1", "10.0.0.4"), [_node("blue1")])
        self.assertIn("internal port", str(caught.exception))

    def test_a_seal_that_records_no_lsn_is_refused(self) -> None:
        with (
            mock.patch.object(
                bg,
                "_post",
                return_value=mock.Mock(status_code=200, raise_for_status=lambda: None),
            ),
            mock.patch.object(bg, "_cutover_status", return_value={"phase": "sealed"}),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._cutover(_node("blue1"), _node("green1", "10.0.0.4"), [_node("blue1")])
        self.assertIn("final LSN", str(caught.exception))

    def test_an_unconfirmable_authority_holds_rather_than_guessing(self) -> None:
        """Blue still claiming authority after SEAL is refused, not tolerated."""
        with (
            mock.patch.object(
                bg,
                "_post",
                return_value=mock.Mock(status_code=200, raise_for_status=lambda: None),
            ),
            mock.patch.object(
                bg,
                "_cutover_status",
                return_value={
                    "sealed_final_lsn": 10,
                    "holds_client_write_authority": True,
                },
            ),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._cutover(_node("blue1"), _node("green1", "10.0.0.4"), [_node("blue1")])
        self.assertIn("still reports write authority", str(caught.exception))


class ReverseDrainTest(unittest.TestCase):
    """U5: while rollback is reported available, green's acks are on blue."""

    def test_a_write_missing_from_the_reverse_stream_is_caught(self) -> None:
        posts: list[tuple[str, Any]] = []

        def fake_post(_node: Any, path: str, body: Any = None) -> Any:
            posts.append((path, body))
            return mock.Mock(status_code=200, raise_for_status=lambda: None)

        def fake_psql(_node: Any, sql: str, **_kw: Any) -> str:
            # Blue received only two of the three writes green acked.
            return "rb-0\nrb-1" if "rb-%" in sql else ""

        with (
            mock.patch.object(bg, "_write_burst", return_value={"rb-0", "rb-1", "rb-2"}),
            mock.patch.object(bg, "_post", side_effect=fake_post),
            mock.patch.object(
                bg, "_cutover_status", return_value={"sealed_final_lsn": 9, "epoch": 2}
            ),
            mock.patch.object(bg, "_await_slot_confirms", return_value=9),
            mock.patch.object(bg, "_receive_authority"),
            mock.patch.object(bg, "_psql", side_effect=fake_psql),
            self.assertRaises(bg.ContractFailure) as caught,
        ):
            bg._rollback_drill(
                _node("blue1"),
                _node("green1", "10.0.0.4"),
                [_node("blue1")],
                [_node("green1", "10.0.0.4")],
            )
        self.assertIn("U5 violated", str(caught.exception))
        self.assertIn("rb-2", str(caught.exception))

    def test_a_transfer_without_a_reverse_slot_is_refused(self) -> None:
        """The slot must predate the recipient's first accepted write."""
        sent: list[Any] = []

        def fake_post(_node: Any, path: str, body: Any = None) -> Any:
            if path == "transfer":
                sent.append(body)
            return mock.Mock(status_code=200, raise_for_status=lambda: None)

        # Post-rollback: blue holds authority again, green does not.
        def fake_status(node: Node) -> dict[str, Any]:
            return {
                "sealed_final_lsn": 9,
                "epoch": 2,
                "holds_client_write_authority": node.service.startswith("blue"),
            }

        with (
            mock.patch.object(bg, "_post", side_effect=fake_post),
            mock.patch.object(bg, "_cutover_status", side_effect=fake_status),
            mock.patch.object(bg, "_await_slot_confirms", return_value=9),
            mock.patch.object(bg, "_receive_authority"),
            mock.patch.object(bg, "_psql", return_value="rb-0"),
            mock.patch.object(bg, "_write_burst", return_value={"rb-0"}),
        ):
            bg._rollback_drill(
                _node("blue1"),
                _node("green1", "10.0.0.4"),
                [_node("blue1")],
                [_node("green1", "10.0.0.4")],
            )
        self.assertTrue(sent, "the drill must actually run a transfer")
        self.assertTrue(
            all(b["reverse_slot_ready"] for b in sent),
            "the harness must never ask for a transfer without the reverse slot",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
