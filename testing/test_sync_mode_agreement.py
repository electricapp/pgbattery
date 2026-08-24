#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the RW-11 sync-mode checker.

The checker decides whether the election gate ran looser than the durability
actually in force. Every rule it applies is driven to both answers here, because
one that has only ever seen healthy input is one nobody has watched fail.

Run with:
    uv run --project testing python testing/test_sync_mode_agreement.py
"""

from __future__ import annotations

import unittest

import fault_primitives as fp
import sync_mode_agreement as sm

SYNC_LIST = "FIRST 1 (node2, node3)"


def series(*rows: tuple[bool | None, str | None]) -> list[sm.Sample]:
    """Samples one period apart, from `(replicated, guc)` pairs.

    `guc=None` is an unreachable leader; the leader name goes with it, since a
    node that cannot answer is not one we know led.
    """
    return [
        sm.Sample(
            at_s=index * sm.SAMPLE_PERIOD_S,
            leader=None if guc is None else "node1",
            replicated=replicated,
            guc=guc,
        )
        for index, (replicated, guc) in enumerate(rows)
    ]


class DisagreementTest(unittest.TestCase):
    """Only a flag looser than the GUC is a finding. The other direction is the
    deliberate ordering: committing `active=true` early only tightens the gate."""

    def test_loose_flag_against_a_sync_list_is_the_finding(self) -> None:
        self.assertTrue(series((False, SYNC_LIST))[0].disagrees)

    def test_a_tight_flag_against_an_empty_list_is_not(self) -> None:
        self.assertFalse(series((True, ""))[0].disagrees)

    def test_agreement_in_either_direction_is_not(self) -> None:
        self.assertFalse(series((True, SYNC_LIST))[0].disagrees)
        self.assertFalse(series((False, ""))[0].disagrees)

    def test_no_transition_observed_is_not(self) -> None:
        """`None` selects the tight threshold, so it can never be too loose."""
        self.assertFalse(series((None, SYNC_LIST))[0].disagrees)

    def test_an_unreachable_leader_is_not_a_finding(self) -> None:
        """Absence of evidence. Reading it as an empty sync list would
        manufacture the disagreement the suite exists to look for."""
        self.assertFalse(series((False, None))[0].disagrees)


class WindowTest(unittest.TestCase):
    """The verdict is how long the disagreement stayed open, against a budget
    derived from the reconcile interval that closes it."""

    def test_a_healthy_series_has_no_window(self) -> None:
        window = sm.longest_disagreement(series((True, SYNC_LIST), (True, SYNC_LIST)))
        self.assertEqual(window.samples, 0)
        self.assertEqual(window.seconds, 0.0)

    def test_the_longest_run_is_the_one_reported(self) -> None:
        window = sm.longest_disagreement(
            series(
                (False, SYNC_LIST),
                (True, SYNC_LIST),
                (False, SYNC_LIST),
                (False, SYNC_LIST),
                (False, SYNC_LIST),
            )
        )
        self.assertEqual(window.samples, 3)
        self.assertAlmostEqual(window.seconds, 2 * sm.SAMPLE_PERIOD_S)

    def test_a_repair_ends_the_window(self) -> None:
        window = sm.longest_disagreement(
            series((False, SYNC_LIST), (False, SYNC_LIST), (True, SYNC_LIST))
        )
        self.assertEqual(window.samples, 2)

    def test_an_unreadable_sample_breaks_the_run_rather_than_spanning_it(self) -> None:
        """Two short windows either side of a gap are not one long one: nobody
        observed the middle, so nothing was seen to stay open across it."""
        window = sm.longest_disagreement(
            series(
                (False, SYNC_LIST),
                (False, SYNC_LIST),
                (False, None),
                (False, SYNC_LIST),
            )
        )
        self.assertEqual(window.samples, 2)

    def test_the_window_names_the_leader_it_opened_on(self) -> None:
        window = sm.longest_disagreement(series((False, SYNC_LIST), (False, SYNC_LIST)))
        self.assertEqual(window.leader, "node1")


class VacuityTest(unittest.TestCase):
    """A window of zero is what a working system reports and also what a run
    that produced no transition reports. The phase has to tell them apart."""

    def test_a_series_that_never_changed_is_not_a_repair(self) -> None:
        self.assertFalse(sm.observed_repair(series((True, SYNC_LIST), (True, SYNC_LIST))))

    def test_a_series_holding_the_list_empty_throughout_is_not_either(self) -> None:
        self.assertFalse(sm.observed_repair(series((False, ""), (False, ""))))

    def test_empty_then_refilled_is_a_repair(self) -> None:
        self.assertTrue(sm.observed_repair(series((False, ""), (True, SYNC_LIST))))

    def test_an_unreadable_sample_cannot_supply_either_half(self) -> None:
        """`guc=None` is 'not observed', so it can neither be the empty list nor
        the refilled one."""
        self.assertFalse(sm.observed_repair(series((False, None), (True, SYNC_LIST))))

    def test_the_median_gap_is_what_the_samples_actually_cost(self) -> None:
        samples = [
            sm.Sample(at_s=0.0, leader="node1", replicated=True, guc=SYNC_LIST),
            sm.Sample(at_s=1.0, leader="node1", replicated=True, guc=SYNC_LIST),
            sm.Sample(at_s=2.0, leader="node1", replicated=True, guc=SYNC_LIST),
        ]
        self.assertAlmostEqual(sm.median_gap_s(samples), 1.0)

    def test_a_single_sample_has_no_resolution_at_all(self) -> None:
        one = series((True, SYNC_LIST))
        self.assertEqual(sm.median_gap_s(one), float("inf"))


class PhaseSelectionTest(unittest.TestCase):
    """A run that executes no phase reports no violation, which is what a clean
    run reports too. Selecting nothing has to be impossible rather than quiet."""

    def test_no_argument_runs_every_phase(self) -> None:
        self.assertEqual(sm.selected_phases(""), sm.ALL_PHASES)

    def test_whitespace_only_runs_every_phase(self) -> None:
        self.assertEqual(sm.selected_phases("  ,  "), sm.ALL_PHASES)

    def test_one_name_runs_that_phase(self) -> None:
        self.assertEqual(sm.selected_phases("failover"), (sm.Phase.FAILOVER,))

    def test_both_names_run_in_the_order_given(self) -> None:
        self.assertEqual(sm.selected_phases("failover, heal"), (sm.Phase.FAILOVER, sm.Phase.HEAL))

    def test_an_unknown_name_is_refused_rather_than_dropped(self) -> None:
        with self.assertRaises(sm.PreconditionError) as caught:
            sm.selected_phases("heal,typo")
        self.assertIn("typo", str(caught.exception))


class UpperBoundTest(unittest.TestCase):
    """A window seen once spans zero periods, which reads as never seen. The
    verdict is taken on how long it could have been, not how long it measured."""

    def test_a_window_never_seen_is_zero(self) -> None:
        self.assertEqual(sm.upper_bound_s(sm.Window(0.0, 0, 0.0, None), 0.4), 0.0)

    def test_a_window_seen_once_is_at_most_one_period(self) -> None:
        self.assertAlmostEqual(sm.upper_bound_s(sm.Window(0.0, 1, 0.0, "node2"), 0.4), 0.4)

    def test_a_longer_window_carries_the_same_slack(self) -> None:
        self.assertAlmostEqual(sm.upper_bound_s(sm.Window(1.2, 4, 0.0, "node2"), 0.4), 1.6)


class BudgetTest(unittest.TestCase):
    """The budget is derived from the Rust constant that closes the window, not
    written down here — a retuned reconcile interval must move it."""

    def test_the_budget_follows_the_reconcile_interval(self) -> None:
        timings = fp.read_system_timings()
        self.assertAlmostEqual(
            sm.budget_seconds(timings),
            sm.INTERVALS_ALLOWED * timings.replica_check_interval_ms / 1_000,
        )

    def test_a_window_past_the_budget_is_a_violation(self) -> None:
        timings = fp.read_system_timings()
        budget = sm.budget_seconds(timings)
        held = series(*[(False, SYNC_LIST)] * int((budget + 5.0) / sm.SAMPLE_PERIOD_S))
        self.assertGreater(sm.longest_disagreement(held).seconds, budget)

    def test_a_window_inside_the_budget_is_not(self) -> None:
        timings = fp.read_system_timings()
        budget = sm.budget_seconds(timings)
        brief = series(*[(False, SYNC_LIST)] * 2)
        self.assertLessEqual(sm.longest_disagreement(brief).seconds, budget)


if __name__ == "__main__":
    unittest.main(verbosity=2)
