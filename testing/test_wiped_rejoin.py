#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the H-09 wiped-rejoin checker.

The safety question is decided by two pure functions: `find_dual_leadership`,
which reads a sampled history, and `VariantResult.verdict`, which turns one
run's observations into a pass or a failure. Both are tested here, and the
inversions matter most — a checker that calls split brain a pass, or that
calls an ordinary failover split brain, is worse than no checker.
"""

from __future__ import annotations

import unittest

import wiped_rejoin as wr


def sample(*pairs: tuple[str, int]) -> list[wr.LeaderSample]:
    return [wr.LeaderSample(node=node, term=term) for node, term in pairs]


class DualLeadershipTests(unittest.TestCase):
    def test_two_leaders_in_one_term_is_reported(self) -> None:
        """The inversion: this is exactly what a lost persisted vote produces."""
        history = [
            sample(("node1", 7)),
            sample(("node1", 7), ("node3", 7)),
            sample(("node3", 7)),
        ]
        found = wr.find_dual_leadership(history)
        self.assertEqual(found, [(7, ("node1", "node3"))])

    def test_leaders_in_different_terms_are_ordinary_failover(self) -> None:
        """A term change is how leadership is *supposed* to move."""
        history = [
            sample(("node1", 7)),
            sample(),
            sample(("node3", 8)),
            sample(("node3", 8)),
        ]
        self.assertEqual(wr.find_dual_leadership(history), [])

    def test_the_same_leader_reported_repeatedly_is_not_a_violation(self) -> None:
        history = [sample(("node2", 4)) for _ in range(20)]
        self.assertEqual(wr.find_dual_leadership(history), [])

    def test_a_leaderless_history_is_not_a_violation(self) -> None:
        """No leader is a liveness problem, not a safety one."""
        self.assertEqual(wr.find_dual_leadership([sample(), sample()]), [])

    def test_every_offending_term_is_reported(self) -> None:
        history = [
            sample(("node1", 2), ("node2", 2)),
            sample(("node2", 5), ("node3", 5)),
            sample(("node3", 6)),
        ]
        self.assertEqual(
            wr.find_dual_leadership(history),
            [(2, ("node1", "node2")), (5, ("node2", "node3"))],
        )


class VerdictTests(unittest.TestCase):
    def result(self, **kwargs: object) -> wr.VariantResult:
        base: dict[str, object] = {
            "variant": wr.Variant.RAFT_ONLY,
            "target": "node2",
            "rejoined": True,
            "refused": False,
            "acked_rows_before": 50,
            "acked_rows_after": 50,
        }
        base.update(kwargs)
        return wr.VariantResult(**base)  # type: ignore[arg-type]

    def test_a_clean_rejoin_passes(self) -> None:
        self.assertTrue(self.result().ok)

    def test_a_refusal_passes(self) -> None:
        """Refusing to run is an acceptable answer to a destroyed store."""
        self.assertTrue(self.result(rejoined=False, refused=True).ok)

    def test_split_brain_fails_even_when_the_node_rejoined_cleanly(self) -> None:
        """The rejoin looking healthy is exactly how this would be missed."""
        verdict = self.result(dual_leadership=[(7, ("node1", "node3"))])
        self.assertFalse(verdict.ok)
        self.assertIn("two leaders in one term", verdict.verdict)

    def test_lost_acked_rows_fail(self) -> None:
        verdict = self.result(acked_rows_after=42)
        self.assertFalse(verdict.ok)
        self.assertIn("acked rows lost", verdict.verdict)

    def test_running_without_rejoining_or_refusing_fails(self) -> None:
        """A node that is up but never rejoined is voting on nothing vouched for."""
        verdict = self.result(rejoined=False, refused=False)
        self.assertFalse(verdict.ok)
        self.assertIn("nobody vouched for", verdict.verdict)

    def test_an_unreadable_row_count_does_not_invent_a_loss(self) -> None:
        """A count that could not be read is not evidence of lost rows."""
        self.assertTrue(self.result(acked_rows_after=None).ok)

    def test_extra_rows_are_not_a_loss(self) -> None:
        """Only a shortfall is a durability failure."""
        self.assertTrue(self.result(acked_rows_after=51).ok)


class VariantTests(unittest.TestCase):
    def test_each_variant_wipes_what_it_names(self) -> None:
        self.assertEqual(wr.Variant.RAFT_ONLY.paths, [wr.RAFT_DIR])
        self.assertEqual(wr.Variant.PGDATA_ONLY.paths, [wr.PGDATA_DIR])
        self.assertEqual(sorted(wr.Variant.BOTH.paths), sorted([wr.RAFT_DIR, wr.PGDATA_DIR]))

    def test_the_paths_come_from_the_shared_constants(self) -> None:
        """Restating a state path here is how a wipe quietly targets nothing."""
        import fault_primitives as fp

        self.assertTrue(wr.RAFT_DIR.startswith(fp.PG_STATE_DIR))
        self.assertEqual(wr.PGDATA_DIR, fp.PG_DATA_DIR)


if __name__ == "__main__":
    unittest.main()
