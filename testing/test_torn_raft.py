#!/usr/bin/env -S uv run --project testing python
"""Unit tests for the Raft torn-write suite's verdict logic.

No docker. These cover what the suite concludes from an attempt it could not
fully observe, which is the part that decides whether a contract may be claimed.

Run with:
    uv run --project testing python testing/test_torn_raft.py
"""

from __future__ import annotations

import unittest
from unittest import mock

import fault_primitives as fp
import torn_raft as tr


def outcome(**kwargs: object) -> tr.Outcome:
    """An Outcome with the fields the verdict reads, defaults chosen to be silent."""
    result = tr.Outcome()
    for key, value in kwargs.items():
        setattr(result, key, value)
    return result


class TearReadingTest(unittest.TestCase):
    """One attempt must not report a log it could not read as a log that was empty."""

    def read(self, result: fp.CommandResult) -> tr.TearReading:
        with (
            mock.patch.object(fp, "arm_torn_write"),
            mock.patch.object(tr, "await_leader", return_value="node1"),
            mock.patch.object(tr, "write_batch", return_value=[]),
            mock.patch("time.sleep"),
            mock.patch.object(fp, "exec_when_deliverable", return_value=result),
        ):
            return tr.tear_once("node2", 0, tr.Outcome())

    def test_unreadable_log_is_not_an_absent_tear(self) -> None:
        reading = self.read(fp.CommandResult(rc=1, stdout="", stderr="is not running"))
        self.assertTrue(
            reading.unreadable,
            "a log the harness could not read must be recorded as unread; reporting "
            "it as 'no tear' turns a fault that fired into one that never did",
        )
        self.assertIn("is not running", reading.unreadable)

    def test_readable_log_with_no_record_is_an_absent_tear(self) -> None:
        reading = self.read(fp.CommandResult(rc=0, stdout="nothing of interest\n", stderr=""))
        self.assertEqual(reading.unreadable, "")
        self.assertIsNone(reading.record)

    def test_readable_log_reports_the_last_tear(self) -> None:
        line = f"Write to path {tr.RAFT_DB}: will persist 1024 bytes from offset 8192"
        reading = self.read(fp.CommandResult(rc=0, stdout=f"{line}\n", stderr=""))
        self.assertEqual(reading.record, (1024, 8192))
        self.assertEqual(reading.unreadable, "")


class DamageEstablishedTest(unittest.TestCase):
    """What entitles a run to assert anything about torn-write handling."""

    def test_a_sized_tear_establishes_damage(self) -> None:
        self.assertTrue(outcome(tears=1, victim_healthy=True).damage_established)

    def test_a_refusal_on_a_corrupt_store_establishes_damage(self) -> None:
        self.assertTrue(
            outcome(tears=0, victim_refused=True, refusal=tr.HANDLED_REFUSAL).damage_established,
            "a node that will not open the store it was just serving is damage no "
            "byte threshold would have judged more strictly",
        )

    def test_a_refusal_nobody_can_attribute_establishes_nothing(self) -> None:
        self.assertFalse(
            outcome(tears=0, victim_refused=True, refusal=tr.UNKNOWN_REFUSAL).damage_established,
            "a node down for reasons the harness cannot name is not evidence the "
            "tear reached the store",
        )

    def test_a_healthy_node_with_no_sized_tear_establishes_nothing(self) -> None:
        self.assertFalse(outcome(tears=0, victim_healthy=True).damage_established)


class UnobservedFaultMessageTest(unittest.TestCase):
    """The message must say which of the two happened, because they differ."""

    def test_unreadable_attempts_are_named_rather_than_denied(self) -> None:
        message = tr.unobserved_fault_message(
            outcome(attempts=3, unreadable=["node2: is not running"]), min_torn_bytes=512
        )
        self.assertIn("could not be read", message)
        self.assertNotIn("the fault never fired", message)

    def test_a_readable_empty_log_says_the_fault_never_fired(self) -> None:
        message = tr.unobserved_fault_message(outcome(attempts=3), min_torn_bytes=512)
        self.assertIn("never fired", message)


if __name__ == "__main__":
    unittest.main(verbosity=2)
