#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the continuous writers in the Track A/B upgrade harnesses.

The writers are `threading.Thread` subclasses. An instance attribute whose
name `Thread` uses internally replaces that internal for the object: binding
`_stop` to an Event makes `join()` raise `TypeError: 'Event' object is not
callable` at teardown, after every assertion in the proof has already passed.

`Thread._stop` is a method on CPython 3.12 and gone on 3.13, so the crash
reproduces on CI's interpreter and not on a newer local one. The forbidden
names are therefore listed rather than read off the running interpreter, and
only the attributes the subclass itself binds are checked.

Run with:
    uv run --project testing python testing/test_upgrade_writers.py
"""

from __future__ import annotations

import os
import sys
import threading
import unittest
from pathlib import Path
from typing import Any, Final
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

os.environ.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "self-test-token")

import cluster_binary_upgrade_proof
import pg_major_upgrade_proof
import pg_upgrade_hold_chaos
from topology import Node


def _node(service: str = "node1") -> Node:
    """A real `Node` so the oracles are exercised through their real types."""
    return Node(
        service=service,
        node_id=1,
        ip="10.0.0.1",
        gateway_port=5432,
        metrics_port=9091,
        mgmt_port=9081,
        is_voter=True,
        is_bootstrap=True,
    )


# One lost acked key (w-1) offset by one committed-but-unacked extra (w-99):
# the world the count-based W1 ledger could not see.
ACKED_KEYS: Final[list[str]] = ["w-0", "w-1", "w-2"]
PRESENT_RAW: Final[str] = "w-0\nw-2\nw-99\n"

# Names `threading.Thread` has relied on across the versions this repo
# supports (3.10 through 3.13). Rebinding any of them breaks start/join.
THREAD_INTERNALS: Final[frozenset[str]] = frozenset(
    {
        "_bootstrap",
        "_delete",
        "_handle",
        "_is_stopped",
        "_reset_internal_locks",
        "_set_ident",
        "_set_tstate_lock",
        "_started",
        "_stop",
        "_tstate_lock",
        "_wait_for_tstate_lock",
    }
)


def _subclass_attributes(writer: threading.Thread) -> set[str]:
    """Attributes the subclass bound, excluding `Thread.__init__`'s own."""
    baseline = set(vars(threading.Thread(target=lambda: None)))
    return set(vars(writer)) - baseline


class WriterThreadHygieneTest(unittest.TestCase):
    """A writer must not take a name `threading.Thread` relies on."""

    def _assert_no_collision(self, writer: threading.Thread) -> None:
        collisions = sorted(_subclass_attributes(writer) & THREAD_INTERNALS)
        self.assertEqual(
            collisions,
            [],
            f"{type(writer).__name__} rebinds Thread internals {collisions}; "
            "join() uses these and fails at teardown on CPython 3.12",
        )

    def test_the_hold_chaos_writer_leaves_thread_internals_alone(self) -> None:
        self._assert_no_collision(pg_upgrade_hold_chaos.Writer(gateway_port=1, prefix="w-test"))

    def test_the_major_upgrade_writer_leaves_thread_internals_alone(self) -> None:
        self._assert_no_collision(pg_major_upgrade_proof.Writer(gateway_port=1))

    def test_the_binary_upgrade_writer_leaves_thread_internals_alone(self) -> None:
        self._assert_no_collision(cluster_binary_upgrade_proof.Writer(gateway_port=1))


class ProofEntrypointTest(unittest.TestCase):
    """Running the script runs the proof, not a helper.

    `@app.command()` sat on the `_unfreeze` helper instead of `main`, so the
    whole pg-major-upgrade-proof case unfroze a healthy cluster and exited 0
    without proving anything.
    """

    def test_the_major_upgrade_app_registers_main_alone(self) -> None:
        commands = [
            info.callback.__name__
            for info in pg_major_upgrade_proof.app.registered_commands
            if info.callback is not None
        ]
        self.assertEqual(commands, ["main"])


class AckedKeySetTest(unittest.TestCase):
    """W1 compares acked keys, not counts, against what survived."""

    def test_the_count_logic_this_replaced_passed_the_offset_world(self) -> None:
        present_count = len(PRESENT_RAW.split())
        self.assertGreaterEqual(
            present_count,
            len(ACKED_KEYS),
            "the offset world no longer documents the count defect",
        )

    def test_the_major_upgrade_oracle_names_the_lost_key(self) -> None:
        with (
            mock.patch.object(pg_major_upgrade_proof, "_exec", return_value=PRESENT_RAW),
            self.assertRaises(pg_major_upgrade_proof.ContractFailure) as caught,
        ):
            pg_major_upgrade_proof._assert_acked_survived(_node(), list(ACKED_KEYS))
        self.assertIn("w-1", str(caught.exception))
        self.assertNotIn("w-99", str(caught.exception))

    def test_every_acked_key_present_passes_despite_extras(self) -> None:
        with mock.patch.object(
            pg_major_upgrade_proof, "_exec", return_value="w-0\nw-1\nw-2\nw-99\n"
        ):
            pg_major_upgrade_proof._assert_acked_survived(_node(), list(ACKED_KEYS))

    def test_no_acked_keys_is_vacuous(self) -> None:
        with self.assertRaises(pg_major_upgrade_proof.ContractFailure) as caught:
            pg_major_upgrade_proof._assert_acked_survived(_node(), [])
        self.assertIn("vacuous", str(caught.exception))

    def test_the_hold_chaos_oracle_names_the_lost_key(self) -> None:
        with (
            mock.patch.object(
                pg_upgrade_hold_chaos, "_present_keys", return_value=set(PRESENT_RAW.split())
            ),
            self.assertRaises(pg_upgrade_hold_chaos.ContractFailure) as caught,
        ):
            pg_upgrade_hold_chaos._assert_acked_survived(_node(), set(ACKED_KEYS))
        self.assertIn("w-1", str(caught.exception))
        self.assertNotIn("w-99", str(caught.exception))

    def test_the_hold_chaos_oracle_passes_on_full_survival(self) -> None:
        with mock.patch.object(
            pg_upgrade_hold_chaos,
            "_present_keys",
            return_value={"w-0", "w-1", "w-2", "w-99"},
        ):
            pg_upgrade_hold_chaos._assert_acked_survived(_node(), set(ACKED_KEYS))

    def test_the_binary_upgrade_oracle_names_the_lost_key(self) -> None:
        with (
            mock.patch.object(
                cluster_binary_upgrade_proof,
                "_present_keys",
                return_value=set(PRESENT_RAW.split()),
            ),
            self.assertRaises(cluster_binary_upgrade_proof.ContractFailure) as caught,
        ):
            cluster_binary_upgrade_proof._assert_acked_survived("node1", set(ACKED_KEYS))
        self.assertIn("w-1", str(caught.exception))

    def test_the_binary_upgrade_oracle_refuses_an_empty_ledger(self) -> None:
        with self.assertRaises(cluster_binary_upgrade_proof.ContractFailure) as caught:
            cluster_binary_upgrade_proof._assert_acked_survived("node1", set())
        self.assertIn("vacuous", str(caught.exception))


class HoldChaosKillEffectTest(unittest.TestCase):
    """The mid-freeze kill asserts its effect, never that it was requested."""

    def test_a_kill_that_did_not_land_is_refused(self) -> None:
        def fake_compose(*args: str, **_kw: Any) -> Any:
            if args[0] == "kill":
                return mock.Mock(returncode=0, stdout="", stderr="")
            return mock.Mock(stdout="node1 running (healthy)\nnode2 running\n")

        with (
            mock.patch.object(pg_upgrade_hold_chaos, "_compose", side_effect=fake_compose),
            self.assertRaises(pg_upgrade_hold_chaos.ContractFailure) as caught,
        ):
            pg_upgrade_hold_chaos._kill_and_await_down(_node("node1"), budget=0.2)
        self.assertIn("did not land", str(caught.exception))

    def test_a_confirmed_dead_container_passes(self) -> None:
        def fake_compose(*args: str, **_kw: Any) -> Any:
            if args[0] == "kill":
                return mock.Mock(returncode=0, stdout="", stderr="")
            return mock.Mock(stdout="node1 exited (137)\nnode2 running\n")

        with mock.patch.object(pg_upgrade_hold_chaos, "_compose", side_effect=fake_compose):
            pg_upgrade_hold_chaos._kill_and_await_down(_node("node1"), budget=1.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
