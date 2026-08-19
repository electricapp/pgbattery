#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the H-14 RPO measurement.

`rewind_loss.py` reports a number — how many acknowledged writes the async
fallback destroyed — and a number is only worth reporting if the harness
refuses to invent one. Three preconditions have to hold before a run has
measured anything at all: the fallback engaged, leadership actually moved, and
something was acknowledged in the window. Each is asserted here, in both
directions, because a SKIP that reads as a pass is the failure mode this repo
is built around.
"""

from __future__ import annotations

import unittest

from rewind_loss import Measurement


def measurement(
    *,
    acked: int = 20,
    survived: int = 0,
    fallback_observed: bool = True,
    old_leader: str = "node1",
    new_leader: str | None = "node2",
) -> Measurement:
    """A measurement whose preconditions all hold, unless overridden."""
    return Measurement(
        acked=acked,
        survived=survived,
        fallback_observed=fallback_observed,
        old_leader=old_leader,
        new_leader=new_leader,
    )


class MeasuredVerdictTests(unittest.TestCase):
    def test_the_published_rpo_bound(self) -> None:
        """The number HARDENING.md quotes for RW-3: every write acknowledged
        while `synchronous_standby_names` is empty can be lost."""
        m = measurement(acked=20, survived=0)
        self.assertEqual(m.destroyed, 20)
        self.assertEqual(m.verdict, "MEASURED: 20 of 20 acknowledged writes destroyed")

    def test_partial_loss_is_counted_not_rounded(self) -> None:
        m = measurement(acked=20, survived=7)
        self.assertEqual(m.destroyed, 13)
        self.assertIn("13 of 20", m.verdict)

    def test_nothing_destroyed_is_still_a_measurement(self) -> None:
        """A rewind that discarded nothing is a real result, not a skip: it
        says the window existed and cost nothing, which is what a tighter
        fallback would look like."""
        m = measurement(acked=20, survived=20)
        self.assertEqual(m.destroyed, 0)
        self.assertIn("0 of 20", m.verdict)
        self.assertTrue(m.verdict.startswith("MEASURED"))

    def test_more_survivors_than_acks_cannot_report_negative_loss(self) -> None:
        """The survivor count comes from a row count on the new leader, which
        also carries writes this run never acknowledged. Loss is a floor at
        zero, never a negative number dressed up as a gain."""
        self.assertEqual(measurement(acked=5, survived=9).destroyed, 0)


class RefusesToMeasureNothingTests(unittest.TestCase):
    """Each precondition, inverted. Without these the harness would print a
    confident 0 for a run in which nothing happened."""

    def test_no_fallback_is_a_skip(self) -> None:
        m = measurement(fallback_observed=False)
        self.assertTrue(m.verdict.startswith("SKIP"))
        self.assertIn("async fallback never engaged", m.verdict)

    def test_leadership_that_never_moved_is_a_skip(self) -> None:
        """No new leader means no diverged timeline, so no rewind, so nothing
        for the fallback to have cost."""
        self.assertTrue(measurement(new_leader=None).verdict.startswith("SKIP"))
        self.assertIn("never moved", measurement(new_leader="node1").verdict)

    def test_an_empty_window_is_a_skip(self) -> None:
        """Zero acknowledged writes and zero survivors is arithmetically a
        clean run, and means nothing at all."""
        m = measurement(acked=0, survived=0)
        self.assertEqual(m.destroyed, 0)
        self.assertTrue(m.verdict.startswith("SKIP"))
        self.assertIn("no write was acknowledged", m.verdict)

    def test_a_skip_never_reads_as_a_measurement(self) -> None:
        for skipped in (
            measurement(fallback_observed=False),
            measurement(new_leader=None),
            measurement(new_leader="node1"),
            measurement(acked=0, survived=0),
        ):
            with self.subTest(verdict=skipped.verdict):
                self.assertNotIn("MEASURED", skipped.verdict)


if __name__ == "__main__":
    unittest.main()
