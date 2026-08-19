#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the H-08 clock-skew sweep.

The sweep's own logic is the plan: which steps get applied. Everything else is
the fault primitive and the prober, both tested elsewhere. What matters here is
that the plan actually straddles the boundaries it claims to, in both
directions — a sweep that quietly went forward-only would read as coverage of
exactly the case it was written to add.
"""

from __future__ import annotations

import unittest

import clock_skew_sweep as css
import fault_primitives as fp


def timings(*, lease_duration_ms: int = 2_000) -> fp.SystemTimings:
    """A `SystemTimings` with only the fields this sweep reasons about set."""
    return fp.SystemTimings(
        lease_duration_ms=lease_duration_ms,
        lease_check_interval_ms=100,
        election_timeout_ms=1_000,
        heartbeat_interval_ms=250,
        quorum_timeout_ms=1_000,
        metrics_watchdog_timeout_ms=1_500,
        lsn_staleness_threshold_ms=30_000,
        leadership_transfer_lease_safety_ms=500,
        slot_ensure_interval_ms=30_000,
        election_timeout_source="constants.rs",
    )


TIMINGS = timings()


class SkewPlanTests(unittest.TestCase):
    def test_every_magnitude_is_applied_both_ways(self) -> None:
        """Backward steps are the point of this task; forward-only is the bug."""
        plan = css.skew_plan(TIMINGS, quick=False)
        for step in plan:
            self.assertIn(-step, plan, f"{step:+d} ms has no mirror")
        self.assertTrue(any(s < 0 for s in plan), "no backward steps at all")
        self.assertTrue(any(s > 0 for s in plan), "no forward steps at all")

    def test_the_plan_straddles_the_lease_boundary(self) -> None:
        """A sweep that only lands past the boundary never tests the boundary."""
        lease = TIMINGS.lease_duration_ms
        plan = css.skew_plan(TIMINGS, quick=False)
        self.assertTrue(
            any(0 < s < lease for s in plan),
            f"nothing below the {lease} ms lease boundary: {plan}",
        )
        self.assertIn(lease, plan, "the boundary itself is not sampled")
        self.assertTrue(any(s > lease for s in plan), "nothing past the lease boundary")

    def test_the_plan_reaches_sub_second_steps(self) -> None:
        """The interesting regime is a few hundred ms, not the +30 s this replaces."""
        plan = css.skew_plan(TIMINGS, quick=False)
        small = [s for s in plan if 0 < s <= 500]
        self.assertTrue(small, f"no sub-second forward steps: {plan}")
        self.assertTrue([s for s in plan if -500 <= s < 0], "no sub-second backward steps")

    def test_the_plan_is_anchored_to_the_systems_constants(self) -> None:
        """Retuning a constant must retune the sweep, not leave it stranded."""
        other = timings(lease_duration_ms=5_000)
        self.assertIn(5_000, css.skew_plan(other, quick=False))
        self.assertNotIn(5_000, css.skew_plan(TIMINGS, quick=False))

    def test_quick_still_covers_both_directions(self) -> None:
        """The short form is for local runs; it must not drop a direction."""
        plan = css.skew_plan(TIMINGS, quick=True)
        self.assertTrue(any(s < 0 for s in plan), f"quick plan is forward-only: {plan}")
        self.assertTrue(any(s > 0 for s in plan), f"quick plan is backward-only: {plan}")
        self.assertLess(len(plan), len(css.skew_plan(TIMINGS, quick=False)))

    def test_no_zero_step(self) -> None:
        """A zero step injects nothing and would pass vacuously."""
        for quick in (True, False):
            self.assertNotIn(0, css.skew_plan(TIMINGS, quick=quick))


class StepResultTests(unittest.TestCase):
    def test_only_pass_counts_as_ok(self) -> None:
        def result(verdict: str) -> css.StepResult:
            return css.StepResult(
                skew_ms=-2_000,
                aim=fp.Aim.HOLDDOWN_START,
                observed_skew_ms=-2_000.0,
                offset_into_window_ms=10.0,
                releases_holddown_early=False,
                holddown_engaged=True,
                future_skew_delta=3.0,
                verdict=verdict,
            )

        self.assertTrue(result("PASS").ok)
        self.assertFalse(result("FAIL: L1 VIOLATED").ok)
        self.assertFalse(result("FAIL: skew landed outside the window").ok)


if __name__ == "__main__":
    unittest.main()
