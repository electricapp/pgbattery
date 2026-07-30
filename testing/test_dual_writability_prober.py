#!/usr/bin/env -S uv run --project testing python
"""Unit tests for the dual-writability prober's pure logic.

No cluster, no docker, no PostgreSQL. Every test builds synthetic
`ProbeRound`s and drives `classify_failure`, `ProbeRound`'s predicates,
`violation_windows`, and `analyze`. Stdlib `unittest`, matching
`test_elle_adapter.py`, so the harness pyproject does not grow a pytest
dependency.

An oracle that cannot fail is worse than no oracle, so this file is written
in matched pairs: for every "clean input passes" test there is a
"contaminated input fails" test on the same code path. `CheckerCanFailTests`
exists specifically to prove the FAIL verdict is reachable — if someone breaks
`is_violation` so it always returns False, those tests go red, not green.

Run with:
    uv run --project testing python testing/test_dual_writability_prober.py
"""

from __future__ import annotations

import unittest
from unittest import mock

import dual_writability_prober as dwp
from dual_writability_prober import (
    NodeProbe,
    Outcome,
    ProberError,
    ProbeRound,
    Verdict,
    analyze,
    build_transport,
    classify_failure,
    violation_windows,
)

MS: int = 1_000_000
"""Nanoseconds per millisecond — round timestamps are monotonic ns."""

ROUND_PERIOD_NS: int = 50 * MS


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic round builders
# ─────────────────────────────────────────────────────────────────────────────


def accepted(node_id: int, sent_ns: int = 0, done_ns: int = 2 * MS) -> NodeProbe:
    """A confirmed committed write. The only thing that counts toward L1."""
    return NodeProbe(
        node_id=node_id,
        outcome=Outcome.ACCEPTED,
        reason="committed",
        sent_ns=sent_ns,
        done_ns=done_ns,
    )


def rejected(node_id: int, sent_ns: int = 0, done_ns: int = 1 * MS) -> NodeProbe:
    """A fenced node or hot standby refusing the write (SQLSTATE 25006)."""
    return NodeProbe(
        node_id=node_id,
        outcome=Outcome.REJECTED,
        reason="read_only_sql_transaction",
        sqlstate="25006",
        error_text="cannot execute INSERT in a read-only transaction",
        sent_ns=sent_ns,
        done_ns=done_ns,
    )


def indeterminate(
    node_id: int,
    reason: str = "backend_terminated",
    sqlstate: str | None = "57P01",
    error_text: str = "terminating connection due to administrator command",
    sent_ns: int | None = 0,
    done_ns: int = 3 * MS,
) -> NodeProbe:
    """An unknown fate — the state a probe lands in during an actual fence."""
    return NodeProbe(
        node_id=node_id,
        outcome=Outcome.INDETERMINATE,
        reason=reason,
        sqlstate=sqlstate,
        error_text=error_text,
        sent_ns=sent_ns,
        done_ns=done_ns,
    )


def make_round(seq: int, *probes: NodeProbe, started_ns: int | None = None) -> ProbeRound:
    base = seq * ROUND_PERIOD_NS if started_ns is None else started_ns
    shifted = tuple(
        NodeProbe(
            node_id=p.node_id,
            outcome=p.outcome,
            reason=p.reason,
            sqlstate=p.sqlstate,
            error_text=p.error_text,
            sent_ns=None if p.sent_ns is None else base + p.sent_ns,
            done_ns=None if p.done_ns is None else base + p.done_ns,
        )
        for p in probes
    )
    return ProbeRound(seq=seq, started_ns=base, probes=shifted)


# ─────────────────────────────────────────────────────────────────────────────
# The four canonical round shapes
# ─────────────────────────────────────────────────────────────────────────────


class RoundVerdictTests(unittest.TestCase):
    def test_two_confirmed_acceptances_is_a_fatal_violation(self) -> None:
        rnd = make_round(0, accepted(1), accepted(2), rejected(3))
        self.assertTrue(rnd.is_violation)
        self.assertEqual(rnd.acceptance_count, 2)
        report = analyze([rnd])
        self.assertEqual(report.verdict, Verdict.FAIL)
        self.assertEqual(report.exit_code, 1)
        self.assertEqual(len(report.violations), 1)
        self.assertIn("FAIL", report.headline)
        self.assertIn("L1 VIOLATED", report.headline)

    def test_three_confirmed_acceptances_is_a_violation(self) -> None:
        rnd = make_round(0, accepted(1), accepted(2), accepted(3))
        self.assertTrue(rnd.is_violation)
        self.assertEqual(analyze([rnd]).verdict, Verdict.FAIL)

    def test_one_acceptance_two_rejections_is_clean(self) -> None:
        rnd = make_round(0, accepted(1), rejected(2), rejected(3))
        self.assertFalse(rnd.is_violation)
        self.assertFalse(rnd.reduced_observability)
        self.assertFalse(rnd.observability_lost)
        report = analyze([rnd])
        self.assertEqual(report.verdict, Verdict.PASS)
        self.assertEqual(report.exit_code, 0)
        self.assertEqual(report.indeterminate_probes, 0)
        self.assertEqual(report.reduced_observability_rounds, 0)

    def test_one_acceptance_two_indeterminates_is_clean_but_flagged(self) -> None:
        rnd = make_round(0, accepted(1), indeterminate(2), indeterminate(3))
        self.assertFalse(rnd.is_violation)
        self.assertTrue(rnd.reduced_observability)
        self.assertTrue(rnd.observability_lost)
        self.assertEqual(rnd.answered_count, 1)
        report = analyze([rnd], max_indeterminate_rate=1.0)
        self.assertEqual(report.violations, ())
        self.assertEqual(report.reduced_observability_rounds, 1)
        self.assertEqual(report.observability_lost_rounds, 1)
        self.assertEqual(report.indeterminate_probes, 2)
        self.assertAlmostEqual(report.indeterminate_rate, 2 / 3)

    def test_two_indeterminates_zero_acceptances_is_not_a_violation(self) -> None:
        rnd = make_round(0, rejected(1), indeterminate(2), indeterminate(3))
        self.assertFalse(rnd.is_violation)
        self.assertEqual(rnd.acceptance_count, 0)
        self.assertTrue(rnd.observability_lost)
        report = analyze([rnd], max_indeterminate_rate=1.0)
        self.assertEqual(report.violations, ())
        self.assertNotEqual(report.verdict, Verdict.FAIL)
        self.assertEqual(report.rounds_by_acceptance_count, {0: 1})

    def test_all_three_indeterminate_is_not_a_violation(self) -> None:
        rnd = make_round(0, indeterminate(1), indeterminate(2), indeterminate(3))
        self.assertFalse(rnd.is_violation)
        self.assertTrue(rnd.observability_lost)
        self.assertEqual(analyze([rnd], max_indeterminate_rate=1.0).violations, ())


# ─────────────────────────────────────────────────────────────────────────────
# The classifier can never manufacture an acceptance
# ─────────────────────────────────────────────────────────────────────────────


class ClassifyFailureTests(unittest.TestCase):
    def test_read_only_transaction_is_definite_rejection(self) -> None:
        outcome, reason = classify_failure(
            "25006", "cannot execute INSERT in a read-only transaction"
        )
        self.assertIs(outcome, Outcome.REJECTED)
        self.assertEqual(reason, "read_only_sql_transaction")

    def test_lowercase_sqlstate_still_matches(self) -> None:
        outcome, _ = classify_failure("25p02", "current transaction is aborted")
        self.assertIs(outcome, Outcome.REJECTED)

    def test_backend_termination_is_indeterminate(self) -> None:
        # This is the second half of pgbattery's own fence
        # (App::terminate_client_backends). Calling it acceptance would
        # fabricate FATAL violations on every correct failover.
        outcome, reason = classify_failure(
            "57P01", "terminating connection due to administrator command"
        )
        self.assertIs(outcome, Outcome.INDETERMINATE)
        self.assertEqual(reason, "admin_shutdown")

    def test_transaction_resolution_unknown_is_indeterminate(self) -> None:
        outcome, _ = classify_failure("08007", "transaction resolution unknown")
        self.assertIs(outcome, Outcome.INDETERMINATE)

    def test_statement_timeout_is_indeterminate(self) -> None:
        outcome, _ = classify_failure("57014", "canceling statement due to statement timeout")
        self.assertIs(outcome, Outcome.INDETERMINATE)

    def test_unknown_sqlstate_is_indeterminate_and_named(self) -> None:
        outcome, reason = classify_failure("XX999", "some brand new server error")
        self.assertIs(outcome, Outcome.INDETERMINATE)
        self.assertEqual(reason, "unclassified_sqlstate_XX999")

    def test_unclassified_error_string_is_indeterminate(self) -> None:
        outcome, reason = classify_failure(None, "something nobody has ever seen before")
        self.assertIs(outcome, Outcome.INDETERMINATE)
        self.assertEqual(reason, "unclassified_error")

    def test_empty_error_string_is_indeterminate(self) -> None:
        outcome, _ = classify_failure(None, "")
        self.assertIs(outcome, Outcome.INDETERMINATE)

    def test_connection_failures_are_indeterminate_not_rejections(self) -> None:
        # An unreachable node may still be happily accepting writes from other
        # clients — think of a partition injected with iptables DROP. Calling
        # this a rejection would hide exactly the violation we are hunting.
        for message, expected_reason in (
            ("connection refused", "connect_refused"),
            ("connection timeout expired", "connect_timeout"),
            ("server closed the connection unexpectedly", "conn_closed_by_server"),
            ("connection reset by peer", "conn_reset"),
            ("no route to host", "host_unreachable"),
        ):
            with self.subTest(message=message):
                outcome, reason = classify_failure(None, message)
                self.assertIs(outcome, Outcome.INDETERMINATE)
                self.assertEqual(reason, expected_reason)

    def test_classifier_never_returns_accepted(self) -> None:
        # Exhaustive over every SQLSTATE the classifier knows plus a fuzz of
        # unknown inputs: no failure path can produce an acceptance, because
        # acceptance requires a returned row and lives outside this function.
        candidates: list[tuple[str | None, str]] = [
            (None, ""),
            (None, "committed"),
            (None, "INSERT 0 1"),
            ("00000", "successful completion"),
            ("25006", "read-only"),
            ("57P01", "terminating"),
            ("ZZZZZ", "nonsense"),
        ]
        for sqlstate, message in candidates:
            with self.subTest(sqlstate=sqlstate, message=message):
                outcome, _ = classify_failure(sqlstate, message)
                self.assertIsNot(outcome, Outcome.ACCEPTED)


class UnclassifiedErrorReportingTests(unittest.TestCase):
    def test_unclassified_error_text_is_surfaced_once(self) -> None:
        rounds = [
            make_round(
                seq,
                accepted(1),
                rejected(2),
                indeterminate(
                    3,
                    reason="unclassified_error",
                    sqlstate=None,
                    error_text="gremlins in the wire",
                ),
            )
            for seq in range(3)
        ]
        report = analyze(rounds, max_indeterminate_rate=1.0)
        self.assertEqual(report.unclassified_errors, ("gremlins in the wire",))
        self.assertEqual(report.reason_counts["unclassified_error"], 3)

    def test_missing_probe_table_is_counted_as_a_measurement_hole(self) -> None:
        rnd = make_round(
            0,
            accepted(1),
            rejected(2),
            NodeProbe(
                node_id=3,
                outcome=Outcome.REJECTED,
                reason="undefined_table",
                sqlstate="42P01",
                error_text='relation "pgb_dual_write_probe" does not exist',
                sent_ns=0,
                done_ns=MS,
            ),
        )
        report = analyze([rnd])
        self.assertEqual(report.schema_missing_probes, 1)


# ─────────────────────────────────────────────────────────────────────────────
# Window duration math
# ─────────────────────────────────────────────────────────────────────────────


class ViolationWindowTests(unittest.TestCase):
    def test_single_violating_round_has_zero_observed_span(self) -> None:
        rounds = [
            make_round(0, accepted(1), rejected(2), rejected(3)),
            make_round(1, accepted(1), accepted(2), rejected(3)),
            make_round(2, accepted(2), rejected(1), rejected(3)),
        ]
        windows = violation_windows(rounds)
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].rounds, 1)
        self.assertEqual(windows[0].first_seq, 1)
        self.assertEqual(windows[0].last_seq, 1)
        self.assertEqual(windows[0].span_ns, 0)

    def test_consecutive_violating_rounds_span_first_to_last(self) -> None:
        # Rounds 2..5 violate: four rounds, three 50 ms gaps = 150 ms span.
        rounds = [make_round(0, accepted(1), rejected(2), rejected(3))]
        rounds += [make_round(seq, accepted(1), accepted(2), rejected(3)) for seq in range(2, 6)]
        rounds.append(make_round(6, accepted(2), rejected(1), rejected(3)))
        windows = violation_windows(rounds)
        self.assertEqual(len(windows), 1)
        window = windows[0]
        self.assertEqual((window.first_seq, window.last_seq), (2, 5))
        self.assertEqual(window.rounds, 4)
        self.assertEqual(window.span_ns, 3 * ROUND_PERIOD_NS)
        self.assertEqual(window.span_ns, 150 * MS)

    def test_clean_round_splits_two_windows(self) -> None:
        rounds = [
            make_round(0, accepted(1), accepted(2), rejected(3)),
            make_round(1, accepted(1), accepted(2), rejected(3)),
            make_round(2, accepted(1), rejected(2), rejected(3)),
            make_round(3, accepted(1), accepted(3), rejected(2)),
        ]
        windows = violation_windows(rounds)
        self.assertEqual([w.rounds for w in windows], [2, 1])
        self.assertEqual([w.span_ns for w in windows], [ROUND_PERIOD_NS, 0])

    def test_sequence_gap_splits_a_window(self) -> None:
        # A missing round is a round we did not observe, so the window must not
        # be claimed to span it.
        rounds = [
            make_round(0, accepted(1), accepted(2), rejected(3)),
            make_round(1, accepted(1), accepted(2), rejected(3)),
            make_round(9, accepted(1), accepted(2), rejected(3)),
        ]
        windows = violation_windows(rounds)
        self.assertEqual([(w.first_seq, w.last_seq) for w in windows], [(0, 1), (9, 9)])
        self.assertEqual([w.rounds for w in windows], [2, 1])

    def test_max_window_span_is_the_longest_window(self) -> None:
        rounds = [make_round(seq, accepted(1), accepted(2), rejected(3)) for seq in range(0, 3)]
        rounds.append(make_round(3, accepted(1), rejected(2), rejected(3)))
        rounds += [make_round(seq, accepted(1), accepted(2), rejected(3)) for seq in range(4, 10)]
        report = analyze(rounds)
        self.assertEqual([w.rounds for w in report.windows], [3, 6])
        self.assertEqual(report.max_window_span_ns, 5 * ROUND_PERIOD_NS)

    def test_span_uses_real_timestamps_not_sequence_arithmetic(self) -> None:
        # Rounds do not always land on the nominal period; the span must come
        # from the recorded monotonic clock, not from seq * period.
        rounds = [
            make_round(0, accepted(1), accepted(2), rejected(3), started_ns=1_000),
            make_round(1, accepted(1), accepted(2), rejected(3), started_ns=1_000 + 137 * MS),
        ]
        windows = violation_windows(rounds)
        self.assertEqual(windows[0].span_ns, 137 * MS)


class AcceptedOverlapTests(unittest.TestCase):
    def test_overlapping_acceptances_report_positive_overlap(self) -> None:
        rnd = make_round(
            0,
            accepted(1, sent_ns=0, done_ns=3 * MS),
            accepted(2, sent_ns=1 * MS, done_ns=4 * MS),
            rejected(3),
        )
        overlap = rnd.accepted_overlap_ns
        assert overlap is not None
        self.assertEqual(overlap, 2 * MS)

    def test_disjoint_acceptances_report_negative_overlap_but_still_violate(self) -> None:
        rnd = make_round(
            0,
            accepted(1, sent_ns=0, done_ns=1 * MS),
            accepted(2, sent_ns=10 * MS, done_ns=11 * MS),
            rejected(3),
        )
        overlap = rnd.accepted_overlap_ns
        assert overlap is not None
        self.assertEqual(overlap, -9 * MS)
        # The 2 s lease in src/governor/lease.rs makes two acceptances 10 ms
        # apart illegitimate in any ordering, so the verdict does not soften.
        self.assertTrue(rnd.is_violation)
        self.assertEqual(analyze([rnd]).verdict, Verdict.FAIL)

    def test_overlap_is_none_with_fewer_than_two_acceptances(self) -> None:
        rnd = make_round(0, accepted(1), rejected(2), rejected(3))
        self.assertIsNone(rnd.accepted_overlap_ns)


# ─────────────────────────────────────────────────────────────────────────────
# Observability gating: a blind run must not pass
# ─────────────────────────────────────────────────────────────────────────────


class ObservabilityGateTests(unittest.TestCase):
    def _mostly_blind_rounds(self, count: int = 20) -> list[ProbeRound]:
        return [
            make_round(seq, accepted(1), indeterminate(2), indeterminate(3)) for seq in range(count)
        ]

    def test_blind_run_is_inconclusive_not_pass(self) -> None:
        report = analyze(self._mostly_blind_rounds(), max_indeterminate_rate=0.4)
        self.assertEqual(report.violations, ())
        self.assertEqual(report.verdict, Verdict.INCONCLUSIVE)
        self.assertEqual(report.exit_code, 3)
        self.assertIn("INCONCLUSIVE", report.headline)
        self.assertTrue(any("indeterminate rate" in r for r in report.inconclusive_reasons))

    def test_blind_run_passes_when_the_gate_is_relaxed(self) -> None:
        report = analyze(self._mostly_blind_rounds(), max_indeterminate_rate=1.0)
        self.assertEqual(report.verdict, Verdict.PASS)

    def test_violation_outranks_inconclusive(self) -> None:
        rounds = self._mostly_blind_rounds()
        rounds.append(make_round(len(rounds), accepted(1), accepted(2), indeterminate(3)))
        report = analyze(rounds, max_indeterminate_rate=0.4)
        self.assertEqual(report.verdict, Verdict.FAIL)
        self.assertEqual(report.exit_code, 1)

    def test_zero_rounds_is_inconclusive(self) -> None:
        report = analyze([])
        self.assertEqual(report.verdict, Verdict.INCONCLUSIVE)
        self.assertEqual(report.total_rounds, 0)
        self.assertEqual(report.indeterminate_rate, 1.0)

    def test_no_writable_node_fails_the_single_acceptance_gate(self) -> None:
        # A healthy-cluster sanity run asserts exactly one writable node. A run
        # where nobody ever took a write must not be reported as a clean pass
        # just because it never saw two.
        rounds = [make_round(seq, rejected(1), rejected(2), rejected(3)) for seq in range(10)]
        report = analyze(rounds, min_single_acceptance_rate=0.95)
        self.assertEqual(report.violations, ())
        self.assertEqual(report.single_acceptance_rate, 0.0)
        self.assertEqual(report.verdict, Verdict.INCONCLUSIVE)
        self.assertTrue(
            any("exactly-one-writable" in r for r in report.inconclusive_reasons),
            report.inconclusive_reasons,
        )

    def test_healthy_run_satisfies_the_single_acceptance_gate(self) -> None:
        rounds = [make_round(seq, accepted(1), rejected(2), rejected(3)) for seq in range(10)]
        report = analyze(rounds, min_single_acceptance_rate=0.95)
        self.assertEqual(report.single_acceptance_rate, 1.0)
        self.assertEqual(report.verdict, Verdict.PASS)


# ─────────────────────────────────────────────────────────────────────────────
# Proof the checker can fail
# ─────────────────────────────────────────────────────────────────────────────


class CheckerCanFailTests(unittest.TestCase):
    """An oracle that cannot fail is worse than no oracle.

    These tests are the counterweight to the clean-input tests above: they
    assert the FAIL verdict, exit code 1, and the violation detail are actually
    reachable. Weakening `is_violation`, `analyze`, or `ProbeReport.verdict`
    into something that always reports PASS turns this class red.
    """

    def test_a_single_bad_round_in_a_long_clean_run_still_fails(self) -> None:
        rounds = [make_round(seq, accepted(1), rejected(2), rejected(3)) for seq in range(500)]
        rounds[250] = make_round(250, accepted(1), accepted(2), rejected(3))
        report = analyze(rounds)
        self.assertEqual(report.verdict, Verdict.FAIL)
        self.assertEqual(report.exit_code, 1)
        self.assertEqual(len(report.violations), 1)
        self.assertEqual(report.violations[0].seq, 250)

    def test_the_same_run_without_that_round_passes(self) -> None:
        rounds = [make_round(seq, accepted(1), rejected(2), rejected(3)) for seq in range(500)]
        report = analyze(rounds)
        self.assertEqual(report.verdict, Verdict.PASS)
        self.assertEqual(report.exit_code, 0)

    def test_violation_detail_identifies_which_nodes_accepted(self) -> None:
        rnd = make_round(7, accepted(1), rejected(2), accepted(3))
        report = analyze([rnd])
        self.assertEqual(report.verdict, Verdict.FAIL)
        accepted_ids = sorted(p.node_id for p in report.violations[0].accepted)
        self.assertEqual(accepted_ids, [1, 3])
        payload = report.to_dict()
        self.assertEqual(payload["verdict"], "FAIL")
        self.assertEqual(payload["exit_code"], 1)
        self.assertEqual(payload["violation_count"], 1)
        self.assertEqual(payload["violations"][0]["seq"], 7)
        self.assertEqual(payload["violations"][0]["acceptance_count"], 2)

    def test_indeterminates_alone_can_never_produce_a_failure(self) -> None:
        # The inverse guard: no quantity of blindness may be promoted into a
        # FATAL verdict, at any round count.
        for count in (1, 10, 1000):
            with self.subTest(count=count):
                rounds = [
                    make_round(seq, indeterminate(1), indeterminate(2), indeterminate(3))
                    for seq in range(count)
                ]
                report = analyze(rounds, max_indeterminate_rate=1.0)
                self.assertEqual(report.verdict, Verdict.PASS)
                self.assertEqual(report.violations, ())

    def test_exit_codes_are_distinct_per_verdict(self) -> None:
        fail = analyze([make_round(0, accepted(1), accepted(2), rejected(3))])
        inconclusive = analyze([])
        clean = analyze([make_round(0, accepted(1), rejected(2), rejected(3))])
        self.assertEqual(
            (clean.exit_code, fail.exit_code, inconclusive.exit_code),
            (0, 1, 3),
        )


class ReportShapeTests(unittest.TestCase):
    def test_histogram_and_probe_counts_add_up(self) -> None:
        rounds = [
            make_round(0, accepted(1), rejected(2), rejected(3)),
            make_round(1, accepted(1), accepted(2), rejected(3)),
            make_round(2, rejected(1), rejected(2), indeterminate(3)),
        ]
        report = analyze(rounds, max_indeterminate_rate=1.0)
        self.assertEqual(report.total_rounds, 3)
        self.assertEqual(report.node_count, 3)
        self.assertEqual(report.rounds_by_acceptance_count, {0: 1, 1: 1, 2: 1})
        self.assertEqual(sum(report.rounds_by_acceptance_count.values()), 3)
        self.assertEqual(report.total_probes, 9)
        self.assertEqual(sum(report.reason_counts.values()), 9)
        self.assertEqual(report.indeterminate_probes, 1)

    def test_report_dict_is_json_serialisable(self) -> None:
        import json

        rounds = [
            make_round(0, accepted(1), rejected(2), rejected(3)),
            make_round(1, accepted(1), accepted(2), indeterminate(3)),
        ]
        payload = analyze(rounds, max_indeterminate_rate=1.0).to_dict()
        restored = json.loads(json.dumps(payload))
        self.assertEqual(restored["verdict"], "FAIL")
        self.assertEqual(restored["observability"]["total_probes"], 6)
        self.assertEqual(restored["violations"][0]["probes"][2]["outcome"], "indeterminate")

    def test_probes_with_no_send_timestamp_are_indeterminate_only(self) -> None:
        # A probe that never left the prober (no connection) has sent_ns None.
        # It must not contribute to acceptance or to overlap arithmetic.
        rnd = make_round(
            0,
            accepted(1),
            indeterminate(2, reason="connect_refused", sqlstate=None, sent_ns=None),
            indeterminate(3, reason="connect_refused", sqlstate=None, sent_ns=None),
        )
        self.assertFalse(rnd.is_violation)
        self.assertIsNone(rnd.accepted_overlap_ns)
        self.assertEqual(rnd.answered_count, 1)


class TransportSelectionTests(unittest.TestCase):
    """A silent `docker-exec` fallback reports exactly like a direct probe.

    The two transports classify identically by construction, which is what makes
    local runs useful — and also what makes a fallback invisible. On Linux CI the
    bridge is routable, so a fallback there means the routability claim went
    untested while still reporting PASS.
    """

    def reachable(self, value: bool) -> None:
        patcher = mock.patch.object(dwp, "tcp_reachable", lambda ip, port: value)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_auto_prefers_direct_when_reachable(self) -> None:
        self.reachable(True)
        self.assertEqual(build_transport("auto").name, "direct")

    def test_auto_falls_back_when_unreachable(self) -> None:
        self.reachable(False)
        self.assertEqual(build_transport("auto").name, "docker-exec")

    def test_require_direct_rejects_a_fallback(self) -> None:
        self.reachable(False)
        with self.assertRaises(ProberError) as ctx:
            build_transport("auto", require="direct")
        self.assertIn("docker-exec", str(ctx.exception))

    def test_require_direct_accepts_a_direct_resolution(self) -> None:
        self.reachable(True)
        self.assertEqual(build_transport("auto", require="direct").name, "direct")

    def test_explicit_direct_fails_fast_when_unreachable(self) -> None:
        """Otherwise it fails later as a pile of connection errors, which the
        indeterminate-rate gate reports as inconclusive rather than as a
        misconfigured transport."""
        self.reachable(False)
        with self.assertRaises(ProberError) as ctx:
            build_transport("direct")
        self.assertIn("not reachable", str(ctx.exception))

    def test_require_docker_exec_rejects_direct(self) -> None:
        self.reachable(True)
        with self.assertRaises(ProberError):
            build_transport("auto", require="docker-exec")

    def test_unknown_transport_is_rejected(self) -> None:
        self.reachable(True)
        with self.assertRaises(ProberError):
            build_transport("carrier-pigeon")

    def test_no_requirement_permits_either(self) -> None:
        for value, expected in ((True, "direct"), (False, "docker-exec")):
            with self.subTest(reachable=value):
                self.reachable(value)
                self.assertEqual(build_transport("auto", require="").name, expected)


if __name__ == "__main__":
    unittest.main()
