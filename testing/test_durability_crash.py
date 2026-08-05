#!/usr/bin/env -S uv run --project testing python
"""Unit tests for the durability_crash harness.

No docker. Container reads are answered by a scripted command runner installed
with ``set_command_runner``, so the readiness gate can be exercised against the
container states that actually occur during a dirty crash.

Run with:
    uv run --project testing python testing/test_durability_crash.py
"""

from __future__ import annotations

import unittest
from collections.abc import Sequence
from unittest import mock

import psycopg

import durability_crash as dc
import fault_primitives as fp
from fault_primitives import CommandResult, set_command_runner

RESTARTING = (
    "Error response from daemon: Container 8823f52780b2 is restarting, "
    "wait until the container is running"
)

POSTMASTER_PS = (
    "  PID USER     STAT COMMAND\n"
    "    1 postgres S    /usr/local/bin/pgbattery --config /etc/pgbattery/config.toml\n"
    "   42 postgres S    postgres: checkpointer\n"
    "   43 postgres S    postgres: background writer\n"
)
NO_POSTMASTER_PS = (
    "  PID USER     STAT COMMAND\n"
    "    1 postgres S    /usr/local/bin/pgbattery --config /etc/pgbattery/config.toml\n"
)


class ReplayRunner:
    """Answers each call from a queue, repeating the final entry forever."""

    def __init__(self, results: Sequence[CommandResult]) -> None:
        self.results = list(results)
        self.calls = 0

    def __call__(self, cmd: str, timeout_s: float) -> CommandResult:
        index = min(self.calls, len(self.results) - 1)
        self.calls += 1
        return self.results[index]


class AwaitPostgresRunningTests(unittest.TestCase):
    """`cluster-crash` kills every node at once, so the wait for PostgreSQL has
    to survive containers still restarting rather than abort on one."""

    def install(self, runner: ReplayRunner) -> ReplayRunner:
        previous = set_command_runner(runner)
        self.addCleanup(set_command_runner, previous)
        return runner

    def test_a_restarting_node_is_waited_through(self) -> None:
        runner = self.install(
            ReplayRunner(
                [
                    CommandResult(1, "", RESTARTING),
                    CommandResult(1, "", RESTARTING),
                    CommandResult(0, POSTMASTER_PS, ""),
                ]
            )
        )
        with mock.patch("time.sleep"):
            dc.await_postgres_running(["node3"], 30.0)
        self.assertEqual(runner.calls, 3)

    def test_a_node_that_never_runs_carries_the_daemons_reason(self) -> None:
        """Otherwise it blames a missing postmaster in a container that never
        held one."""
        self.install(ReplayRunner([CommandResult(1, "", RESTARTING)]))
        with mock.patch("time.sleep"), self.assertRaises(fp.FaultPreconditionError) as caught:
            dc.await_postgres_running(["node3"], 2.0)
        message = str(caught.exception)
        self.assertIn("is restarting", message)
        self.assertIn("no PostgreSQL running within 2s", message)
        self.assertNotIsInstance(caught.exception, fp.ContainerNotRunning)

    def test_a_running_node_without_a_postmaster_still_fails(self) -> None:
        self.install(ReplayRunner([CommandResult(0, NO_POSTMASTER_PS, "")]))
        with mock.patch("time.sleep"), self.assertRaises(fp.FaultPreconditionError) as caught:
            dc.await_postgres_running(["node3"], 2.0)
        message = str(caught.exception)
        self.assertIn("no PostgreSQL running", message)
        self.assertNotIn("is restarting", message)

    def test_a_ready_node_returns_without_sleeping(self) -> None:
        runner = self.install(ReplayRunner([CommandResult(0, POSTMASTER_PS, "")]))
        with mock.patch("time.sleep") as slept:
            dc.await_postgres_running(["node3"], 30.0)
        self.assertEqual(runner.calls, 1)
        slept.assert_not_called()


class EnsureTableTests(unittest.TestCase):
    """The gateway closes client connections while it re-resolves the leader.

    Common right after a whole-cluster crash-restart, which is exactly where
    the real run starts when `--prove-oracle` has just run the inversion.
    """

    def test_a_closed_setup_connection_is_retried_on_the_new_leader(self) -> None:
        attempts: list[str] = []

        def connect(node: str, *, weaken: bool) -> mock.MagicMock:
            attempts.append(node)
            if len(attempts) == 1:
                raise psycopg.OperationalError("server closed the connection unexpectedly")
            return mock.MagicMock()

        with (
            mock.patch.object(dc, "connect_gateway", side_effect=connect),
            mock.patch.object(dc, "await_writable_leader", return_value="node2"),
            mock.patch("time.sleep"),
        ):
            self.assertEqual(dc.ensure_table("node1"), "node2")
        self.assertEqual(attempts, ["node1", "node2"])

    def test_a_setup_that_never_lands_fails_loudly(self) -> None:
        with (
            mock.patch.object(
                dc,
                "connect_gateway",
                side_effect=psycopg.OperationalError("server closed the connection"),
            ),
            mock.patch.object(dc, "await_writable_leader", return_value="node1"),
            mock.patch("time.sleep"),
            self.assertRaises(fp.FaultPreconditionError) as caught,
        ):
            dc.ensure_table("node1", timeout_s=0.0)
        self.assertIn("server closed the connection", str(caught.exception))

    def test_the_node_that_took_the_ddl_is_the_one_returned(self) -> None:
        with mock.patch.object(dc, "connect_gateway", return_value=mock.MagicMock()):
            self.assertEqual(dc.ensure_table("node3"), "node3")


if __name__ == "__main__":
    unittest.main()
