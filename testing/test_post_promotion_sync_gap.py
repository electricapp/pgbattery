#!/usr/bin/env -S uv run --project testing python
"""Self-tests for the RW-2 checker.

The point of these is the inversion: `classify` must call the *unfixed*
cluster's behaviour a violation. A checker that returns a pass for every input
is worse than no checker, because it reads as coverage.

The probe text here is verbatim output captured from a live three-node
cluster — the violating case from the build before `Supervisor::promote`
installed the voter set's sync list, and the safe cases from after.
"""

from __future__ import annotations

import unittest

import post_promotion_sync_gap as ppsg

# Captured from the unfixed build: promoted under the previous term's inherited
# list with no standby connected, then acknowledged 63 ms later with the list
# already cleared to empty and nothing holding the commit.
UNFIXED_PROBE = """Timing is off.
Output format is unaligned.
Field separator is "|".
PROMOTED|2026-08-19 00:17:44.61205+00|FIRST 1 (pgbattery_node_1, pgbattery_node_2)|0
SET
ACKED|2026-08-19 00:17:44.675566+00|0/412EEE0|
SYNCNOW|0
SYNCACK|0
"""

# A standby that was never promoted: its marker says so, and it must never be
# read as a verdict.
NOT_PROMOTED_PROBE = """Timing is off.
NOTPROMOTED|2026-08-19 00:17:44.61205+00||0
ERROR:  cannot execute INSERT in a read-only transaction
"""

# The commit refused to acknowledge: promoted under a real sync list with no
# standby yet connected, so it waited until statement_timeout cancelled it.
BLOCKED_PROBE = """PROMOTED|2026-08-19 01:02:03.1+00|FIRST 1 (pgbattery_node_1, pgbattery_node_2)|0
SET
ERROR:  canceling statement due to statement timeout
"""

# The primary refused the write outright: promoted, but fenced read-only until
# its synchronous configuration is in force.
REFUSED_PROBE = """PROMOTED|2026-08-19 01:02:03.1+00|FIRST 1 (pgbattery_node_2, pgbattery_node_3)|0
SET
ERROR:  cannot execute INSERT in a read-only transaction
"""

# The commit acknowledged and a synchronous standby held it.
BACKED_PROBE = """PROMOTED|2026-08-19 01:02:03.1+00|FIRST 1 (pgbattery_node_1, pgbattery_node_3)|1
SET
ACKED|2026-08-19 01:02:03.2+00|0/5000000|FIRST 1 (pgbattery_node_1, pgbattery_node_3)
SYNCNOW|1
SYNCACK|1
"""

# A standby was designated sync and the commit returned — so PostgreSQL waited
# for it — but the position read back after the commit has already drifted past
# what that standby has flushed. Convicting on the LSN comparison alone would
# report a violation that did not happen.
BACKED_BUT_LSN_DRIFTED_PROBE = (
    "PROMOTED|2026-08-19 01:02:03.1+00|FIRST 1 (pgbattery_node_1, pgbattery_node_3)|1|off\n"
    "SET\n"
    "ACKED|2026-08-19 01:02:03.2+00|0/5000100|"
    "FIRST 1 (pgbattery_node_1, pgbattery_node_3)|off\n"
    "SYNCNOW|1\n"
    "SYNCACK|0\n"
)


class ClassifierTests(unittest.TestCase):
    """The verdict function, including that it can return a failure."""

    def test_unfixed_behaviour_is_a_violation(self) -> None:
        """The inversion. This input is what the cluster did before the fix."""
        results = [
            ppsg.parse_probe("node3", UNFIXED_PROBE),
            ppsg.parse_probe("node1", NOT_PROMOTED_PROBE),
        ]
        verdict, detail = ppsg.classify(results)
        self.assertIs(verdict, ppsg.Verdict.UNBACKED, detail)
        self.assertFalse(verdict.is_pass)
        self.assertIn("no standby held", detail)

    def test_a_blocked_commit_passes(self) -> None:
        results = [
            ppsg.parse_probe("node3", BLOCKED_PROBE),
            ppsg.parse_probe("node1", NOT_PROMOTED_PROBE),
        ]
        verdict, detail = ppsg.classify(results)
        self.assertIs(verdict, ppsg.Verdict.BLOCKED, detail)
        self.assertTrue(verdict.is_pass)

    def test_a_read_only_refusal_passes(self) -> None:
        """A refusal is the strongest safe outcome, not an incomplete run.

        The commit never acknowledged and never hung: the node was primary but
        fenced until its sync configuration was in force, so the client got an
        immediate error instead of a promise nothing was holding.
        """
        results = [
            ppsg.parse_probe("node3", REFUSED_PROBE),
            ppsg.parse_probe("node1", NOT_PROMOTED_PROBE),
        ]
        verdict, detail = ppsg.classify(results)
        self.assertIs(verdict, ppsg.Verdict.REFUSED, detail)
        self.assertTrue(verdict.is_pass)

    def test_a_standby_refusing_is_not_a_verdict(self) -> None:
        """Only the promoted node's outcome counts.

        Every standby refuses writes, so a run where nothing was promoted must
        stay indeterminate rather than borrowing a standby's refusal as a pass.
        """
        results = [
            ppsg.parse_probe("node1", NOT_PROMOTED_PROBE),
            ppsg.parse_probe("node3", NOT_PROMOTED_PROBE),
        ]
        verdict, detail = ppsg.classify(results)
        self.assertIs(verdict, ppsg.Verdict.INDETERMINATE, detail)
        self.assertFalse(verdict.is_pass)

    def test_a_standby_backed_ack_passes(self) -> None:
        verdict, detail = ppsg.classify([ppsg.parse_probe("node3", BACKED_PROBE)])
        self.assertIs(verdict, ppsg.Verdict.SYNC_ACK, detail)
        self.assertTrue(verdict.is_pass)

    def test_a_drifted_commit_position_is_not_a_violation(self) -> None:
        """The checker must not convict on the LSN comparison alone.

        `commit_lsn` is read after the commit, so it sits past the commit
        record; any WAL written in between leaves a standby that genuinely
        flushed the commit reporting a smaller flush_lsn. PostgreSQL having
        designated a sync standby is what makes the acknowledgement backed.
        """
        verdict, detail = ppsg.classify([ppsg.parse_probe("node3", BACKED_BUT_LSN_DRIFTED_PROBE)])
        self.assertIs(verdict, ppsg.Verdict.SYNC_ACK, detail)
        self.assertTrue(verdict.is_pass)

    def test_a_refused_write_never_reads_as_an_acknowledgement(self) -> None:
        """The phantom-ack regression.

        An earlier probe read the commit position in a statement of its own,
        after the INSERT. A read-only transaction runs SELECTs perfectly well,
        so a refused write still produced a commit position and the probe
        announced an acknowledgement that had not happened — reported as
        UNBACKED, a violation invented by the harness. The marker now comes
        back from the INSERT's own RETURNING, so a refusal prints no ACKED
        line at all.
        """
        probe = ppsg.parse_probe("node3", REFUSED_PROBE)
        self.assertFalse(probe.acked, "a refused write must not read as acked")
        self.assertTrue(probe.refused_read_only)
        verdict, detail = ppsg.classify([probe])
        self.assertIs(verdict, ppsg.Verdict.REFUSED, detail)

    def test_no_promotion_is_never_a_pass(self) -> None:
        """A fault that produced no failover tested nothing about the window."""
        results = [
            ppsg.parse_probe("node1", NOT_PROMOTED_PROBE),
            ppsg.parse_probe("node3", NOT_PROMOTED_PROBE),
        ]
        verdict, detail = ppsg.classify(results)
        self.assertIs(verdict, ppsg.Verdict.INDETERMINATE, detail)
        self.assertFalse(verdict.is_pass)

    def test_two_promoted_nodes_are_not_reported_as_a_sync_gap(self) -> None:
        """Split brain is a bigger finding and must not be filed under RW-2."""
        results = [
            ppsg.parse_probe("node1", BACKED_PROBE),
            ppsg.parse_probe("node3", BACKED_PROBE),
        ]
        verdict, detail = ppsg.classify(results)
        self.assertIs(verdict, ppsg.Verdict.INDETERMINATE, detail)
        self.assertIn("split brain", detail)

    def test_an_ack_without_a_standby_count_is_indeterminate(self) -> None:
        """A truncated probe must not be read as a backed ack."""
        truncated = "PROMOTED|t|FIRST 1 (a, b)|0\nACKED|t|0/1|\n"
        verdict, detail = ppsg.classify([ppsg.parse_probe("node3", truncated)])
        self.assertIs(verdict, ppsg.Verdict.INDETERMINATE, detail)
        self.assertFalse(verdict.is_pass)


class ParserTests(unittest.TestCase):
    """The markers must mean exactly what they say."""

    def test_parser_reads_the_empty_window(self) -> None:
        probe = ppsg.parse_probe("node3", UNFIXED_PROBE)
        self.assertTrue(probe.promoted)
        self.assertEqual(
            probe.sync_list_at_promotion,
            "FIRST 1 (pgbattery_node_1, pgbattery_node_2)",
        )
        self.assertEqual(probe.standbys_at_promotion, 0)
        self.assertEqual(probe.sync_list_at_ack, "")
        self.assertEqual(probe.sync_acks, 0)
        self.assertTrue(probe.entered_empty_window)

    def test_a_standby_is_not_an_empty_window(self) -> None:
        """A node still in recovery has an empty list and is not the window."""
        probe = ppsg.parse_probe("node1", NOT_PROMOTED_PROBE)
        self.assertFalse(probe.promoted)
        self.assertFalse(probe.entered_empty_window)

    def test_a_timed_out_commit_is_recorded(self) -> None:
        probe = ppsg.parse_probe("node3", BLOCKED_PROBE)
        self.assertTrue(probe.promoted)
        self.assertFalse(probe.acked)
        self.assertTrue(probe.timed_out)


if __name__ == "__main__":
    unittest.main()
