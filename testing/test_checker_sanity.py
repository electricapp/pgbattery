#!/usr/bin/env -S uv run --project testing python
"""Known-bad-history sanity tests for the in-tree consistency checkers.

`ha-assert-sanity` in ci_matrix.yaml proves the SQL oracles can fail. This
file is the equivalent proof for the checkers in
`linearizability_register.py`: it feeds hand-built histories with a known
anomaly to `_is_linearizable` (WGL) and `_is_weakly_consistent` and asserts
each one is reported as a violation. A checker that silently passes
everything is worse than no checker, because the whole matrix then reports
PASS on a broken cluster.

Every history here is a literal, so these tests need no cluster, no Docker,
and no Elle uberjar. Stdlib `unittest`, matching test_elle_adapter.py.

Histories are single-key: both checkers take one key's op list.

Run with:
    uv run --project testing python testing/test_checker_sanity.py
"""

from __future__ import annotations

import unittest
from collections.abc import Callable
from inspect import signature
from typing import Final

from linreg.attacks import ATTACK_DISPATCH, SEEDED_ATTACKS
from linreg.checkers import _is_linearizable, _is_weakly_consistent
from linreg.cluster import GATEWAY_PORTS
from linreg.records import History, Op
from linreg.workload import _parse_first_int, cas_committed, next_gateway_port

Checker = Callable[[list[Op]], tuple[bool | None, str]]
"""`None` is the WGL "state budget spent, key unchecked" verdict. These cases
are all tiny, so None here would mean the budget stopped binding correctly."""

KEY: Final[int] = 0
"""All histories target one register; the checkers are per-key."""


# ─────────────────────────────────────────────────────────────────────────────
# History builders
# ─────────────────────────────────────────────────────────────────────────────


def _read(op_id: int, invoke: float, ret: float | None, value: int | None) -> Op:
    """A read. `value` None means no answer was observed (pending / reject)."""
    return Op(
        op_id=op_id,
        key=KEY,
        kind="read",
        invoke_ts=invoke,
        return_ts=ret,
        result=value,
    )


def _write(
    op_id: int,
    invoke: float,
    ret: float | None,
    value: int,
    result: bool | None = True,
) -> Op:
    """A write. `result`: True acked, False definitely rejected, None pending."""
    return Op(
        op_id=op_id,
        key=KEY,
        kind="write",
        invoke_ts=invoke,
        return_ts=ret,
        write_val=value,
        result=result,
    )


def _cas(
    op_id: int,
    invoke: float,
    ret: float | None,
    old: int,
    new: int,
    result: bool | None = True,
) -> Op:
    """A CAS. `result`: True committed, False did not commit, None pending."""
    return Op(
        op_id=op_id,
        key=KEY,
        kind="cas",
        invoke_ts=invoke,
        return_ts=ret,
        cas_old=old,
        write_val=new,
        result=result,
    )


class CheckerCase(unittest.TestCase):
    """Base class carrying verdict assertions that report the checker reason."""

    def assert_flagged(self, checker: Checker, ops: list[Op], label: str) -> None:
        ok, reason = checker(ops)
        # `is False`, not falsiness: None means the search gave up, and a
        # giving-up checker would otherwise satisfy every flagged case here.
        self.assertIs(ok, False, f"{label}: expected FAIL, got {ok!r}, reason={reason!r}")

    def assert_accepted(self, checker: Checker, ops: list[Op], label: str) -> None:
        ok, reason = checker(ops)
        self.assertIs(ok, True, f"{label}: expected PASS, got {ok!r}, reason={reason!r}")


# ─────────────────────────────────────────────────────────────────────────────
# Rejected-write soundness (the hole this suite exists to pin down)
# ─────────────────────────────────────────────────────────────────────────────


class RejectedWriteTests(CheckerCase):
    """A write that came back as a definite rejection provably never reached
    the register. If its value is later readable, a fenced node applied it
    anyway -- exactly the split-brain class pgbattery exists to prevent, so
    the checker must not linearize it as an ordinary committed write."""

    def test_rejected_write_value_becomes_visible_is_flagged(self) -> None:
        ops = [
            _write(1, 1.0, 2.0, 99, result=False),
            _read(2, 3.0, 4.0, 99),
        ]
        self.assert_flagged(_is_linearizable, ops, "ghost value from rejected write")

    def test_rejected_write_visible_after_acked_write_is_flagged(self) -> None:
        ops = [
            _write(1, 1.0, 2.0, 5),
            _write(2, 3.0, 4.0, 99, result=False),
            _read(3, 5.0, 6.0, 99),
        ]
        self.assert_flagged(_is_linearizable, ops, "ghost value overwrote acked write")

    def test_weak_check_flags_rejected_only_value(self) -> None:
        ops = [
            _write(1, 1.0, 2.0, 99, result=False),
            _read(2, 3.0, 4.0, 99),
        ]
        self.assert_flagged(_is_weakly_consistent, ops, "ghost value, weak check")

    def test_rejected_write_leaves_register_alone(self) -> None:
        """No false positive: the read sees the pre-rejection value."""
        ops = [
            _write(1, 1.0, 2.0, 99, result=False),
            _read(2, 3.0, 4.0, 0),
        ]
        self.assert_accepted(_is_linearizable, ops, "rejected write is a no-op")

    def test_rejected_value_also_written_by_another_client_is_accepted(self) -> None:
        """Value collision between a rejected and an acked write is not an
        anomaly: the acked write explains the read."""
        ops = [
            _write(1, 1.0, 2.0, 99, result=False),
            _write(2, 2.1, 2.2, 99),
            _read(3, 3.0, 4.0, 99),
        ]
        self.assert_accepted(_is_linearizable, ops, "collision, WGL")
        self.assert_accepted(_is_weakly_consistent, ops, "collision, weak check")

    def test_pending_write_value_may_be_observed(self) -> None:
        """A pending write may have landed, so reading its value is legal."""
        ops = [
            _write(1, 1.0, None, 77, result=None),
            _read(2, 3.0, 4.0, 77),
        ]
        self.assert_accepted(_is_linearizable, ops, "pending write observed, WGL")
        self.assert_accepted(_is_weakly_consistent, ops, "pending write observed, weak")

    def test_pending_write_value_need_not_be_observed(self) -> None:
        """The same pending write may equally have been lost."""
        ops = [
            _write(1, 1.0, None, 77, result=None),
            _read(2, 3.0, 4.0, 0),
        ]
        self.assert_accepted(_is_linearizable, ops, "pending write not observed")


# ─────────────────────────────────────────────────────────────────────────────
# Classic single-register anomalies
# ─────────────────────────────────────────────────────────────────────────────


class LostWriteTests(CheckerCase):
    def test_lost_acked_write_is_flagged(self) -> None:
        """Write of 7 is acked, then the register reads back as the prior
        value with no other write in the history to explain it."""
        ops = [
            _write(1, 1.0, 2.0, 7),
            _read(2, 3.0, 4.0, 0),
        ]
        self.assert_flagged(_is_linearizable, ops, "lost acked write")

    def test_lost_acked_write_under_concurrency_is_flagged(self) -> None:
        """Two acked writes, then a read of the initial value. Both writes
        ack before the read is invoked and nothing writes 0, so no
        linearization puts the register back at 0."""
        ops = [
            _write(1, 1.0, 2.0, 7),
            _write(2, 2.5, 3.0, 8),
            _read(3, 4.0, 5.0, 0),
        ]
        self.assert_flagged(_is_linearizable, ops, "lost writes under concurrency")


class PhantomReadTests(CheckerCase):
    def test_phantom_read_is_flagged_by_wgl(self) -> None:
        """42 was never the target of any write or CAS."""
        ops = [
            _write(1, 1.0, 2.0, 5),
            _read(2, 3.0, 4.0, 42),
        ]
        self.assert_flagged(_is_linearizable, ops, "phantom read")

    def test_phantom_read_is_flagged_by_weak_check(self) -> None:
        ops = [
            _write(1, 1.0, 2.0, 5),
            _read(2, 3.0, 4.0, 42),
        ]
        self.assert_flagged(_is_weakly_consistent, ops, "phantom read, weak check")


class ImpossibleCasTests(CheckerCase):
    def test_cas_committing_on_unwritten_witness_is_flagged_by_wgl(self) -> None:
        """CAS reports it swapped 99 -> 7, but 99 was never in the register."""
        ops = [
            _cas(1, 1.0, 2.0, 99, 7),
        ]
        self.assert_flagged(_is_linearizable, ops, "impossible CAS witness")

    def test_cas_committing_on_unwritten_witness_is_flagged_by_weak_check(self) -> None:
        ops = [
            _cas(1, 1.0, 2.0, 99, 7),
        ]
        self.assert_flagged(_is_weakly_consistent, ops, "impossible CAS witness, weak")

    def test_cas_committing_on_stale_witness_is_flagged(self) -> None:
        """Witness 5 was real, but an acked write moved the register to 6
        before the CAS was invoked, so the CAS cannot have matched."""
        ops = [
            _write(1, 1.0, 2.0, 5),
            _write(2, 3.0, 4.0, 6),
            _cas(3, 5.0, 6.0, 5, 7),
        ]
        self.assert_flagged(_is_linearizable, ops, "CAS on stale witness")


class RealTimeOrderTests(CheckerCase):
    def test_stale_read_after_ack_is_flagged(self) -> None:
        """The read is invoked strictly after the ack of write(2), so no
        linearization lets it return the pre-write value."""
        ops = [
            _write(1, 1.0, 2.0, 1),
            _write(2, 3.0, 4.0, 2),
            _read(3, 5.0, 6.0, 1),
        ]
        self.assert_flagged(_is_linearizable, ops, "stale read breaks real-time order")

    def test_stale_read_is_legal_while_the_write_is_still_in_flight(self) -> None:
        """Same values, but the read now overlaps the write, so ordering the
        read first is a valid linearization."""
        ops = [
            _write(1, 1.0, 2.0, 1),
            _write(2, 3.0, 6.0, 2),
            _read(3, 4.0, 5.0, 1),
        ]
        self.assert_accepted(_is_linearizable, ops, "concurrent stale read")


# ─────────────────────────────────────────────────────────────────────────────
# Valid histories: guard against a checker that always fails
# ─────────────────────────────────────────────────────────────────────────────


class ValidHistoryTests(CheckerCase):
    def test_empty_history(self) -> None:
        self.assert_accepted(_is_linearizable, [], "empty history")
        self.assert_accepted(_is_weakly_consistent, [], "empty history, weak check")

    def test_sequential_history(self) -> None:
        ops = [
            _read(1, 1.0, 2.0, 0),
            _write(2, 3.0, 4.0, 5),
            _read(3, 5.0, 6.0, 5),
            _cas(4, 7.0, 8.0, 5, 9),
            _read(5, 9.0, 10.0, 9),
        ]
        self.assert_accepted(_is_linearizable, ops, "sequential history")
        self.assert_accepted(_is_weakly_consistent, ops, "sequential history, weak")

    def test_concurrent_read_may_see_either_side_of_a_write(self) -> None:
        ops = [
            _write(1, 1.0, 3.0, 5),
            _read(2, 1.5, 2.0, 0),
            _read(3, 4.0, 5.0, 5),
        ]
        self.assert_accepted(_is_linearizable, ops, "read inside the write window")

    def test_failed_cas_on_mismatched_witness(self) -> None:
        ops = [
            _write(1, 1.0, 2.0, 5),
            _cas(2, 3.0, 4.0, 4, 7, result=False),
            _read(3, 5.0, 6.0, 5),
        ]
        self.assert_accepted(_is_linearizable, ops, "CAS witness mismatch")

    def test_unanswered_read_constrains_nothing(self) -> None:
        """A read that came back as a definite reject carries no value."""
        ops = [
            _write(1, 1.0, 2.0, 5),
            _read(2, 3.0, 4.0, None),
            _read(3, 5.0, 6.0, 5),
        ]
        self.assert_accepted(_is_linearizable, ops, "rejected read")


class GatewayRotationTests(unittest.TestCase):
    """A worker pinned to a dead gateway must move.

    In one CI run, workers pinned to the killed leader's gateway produced 56,600
    of 56,665 `:info` records while two surviving workers committed every real
    transaction, so the history was 98 percent connection noise.
    """

    def test_rotation_leaves_the_current_port(self) -> None:
        for port in GATEWAY_PORTS:
            self.assertNotEqual(next_gateway_port(port), port)

    def test_rotation_stays_within_the_known_gateways(self) -> None:
        for port in GATEWAY_PORTS:
            self.assertIn(next_gateway_port(port), GATEWAY_PORTS)

    def test_rotation_visits_every_gateway(self) -> None:
        seen = {GATEWAY_PORTS[0]}
        port = GATEWAY_PORTS[0]
        for _ in range(len(GATEWAY_PORTS)):
            port = next_gateway_port(port)
            seen.add(port)
        self.assertEqual(seen, set(GATEWAY_PORTS))

    def test_unknown_port_recovers_to_a_known_gateway(self) -> None:
        self.assertIn(next_gateway_port(1), GATEWAY_PORTS)

    def test_gateway_switches_are_counted(self) -> None:
        history = History()
        self.assertEqual(history.gateway_switches, 0)
        history.record_gateway_switch()
        history.record_gateway_switch()
        self.assertEqual(history.gateway_switches, 2)


class SeededAttackTests(unittest.TestCase):
    """chaos_storm draws its fault schedule from an RNG, so replaying a failure
    by seed only reproduces it if the seed reaches the injector."""

    def test_chaos_storm_is_declared_seeded(self) -> None:
        self.assertIn("chaos_storm", SEEDED_ATTACKS)

    def test_deterministic_attacks_are_not_declared_seeded(self) -> None:
        for attack in ("kill", "partition", "freeze", "transfer"):
            self.assertNotIn(attack, SEEDED_ATTACKS)

    def test_seeded_attacks_accept_the_injector_argument_shape(self) -> None:
        """The injector runs in a daemon thread, where a signature mismatch dies
        silently and the run still reports PASS. Bind the call here instead."""
        for attack in SEEDED_ATTACKS:
            signature(ATTACK_DISPATCH[attack]).bind(2.0, 25.0, 12345)

    def test_unseeded_attacks_accept_a_lone_delay(self) -> None:
        for attack, fn in ATTACK_DISPATCH.items():
            if attack not in SEEDED_ATTACKS:
                signature(fn).bind(2.0)


class StateBudgetTests(unittest.TestCase):
    """WGL is exponential in concurrency, and `WGL_OPS_PER_KEY_CAP` does not
    bound it -- it caps op count, which is the wrong variable. A 45 s / 6-worker
    / 8-key kill run put ~1,450 ops on each key, well under the 2,000 cap, and
    5 of 8 keys were still searching after 30 s. Unbounded, that is a hang; CI
    kills it and the run reads as an infra flake rather than as an unchecked
    history.
    """

    @staticmethod
    def _wide_concurrent_history(n: int) -> list[Op]:
        """`n` acked concurrent writes of distinct values, plus one read that
        returns a value nobody ever wrote.

        Mutually concurrent, so real-time order prunes nothing and every
        interleaving is reachable. Unsatisfiable, so the search cannot stop
        early on a lucky branch -- it has to visit the whole memoized state
        space before it can answer, which is precisely the shape that made the
        real run hang.
        """
        ops: list[Op] = [_write(op_id=i, invoke=0.0, ret=100.0, value=i + 1) for i in range(n)]
        ops.append(_read(op_id=n, invoke=0.0, ret=100.0, value=n + 1))
        return ops

    def test_budget_stops_a_search_that_would_not_terminate(self) -> None:
        ops = self._wide_concurrent_history(10)
        ok, reason = _is_linearizable(ops, max_states=500)
        self.assertIsNone(ok, f"expected UNCHECKED, got {ok!r} ({reason})")
        self.assertIn("UNCHECKED", reason)

    def test_the_same_history_is_decidable_with_a_large_budget(self) -> None:
        """Without this, the test above would also pass if the checker had
        simply become unable to decide anything."""
        ops = self._wide_concurrent_history(10)
        ok, _ = _is_linearizable(ops, max_states=10_000_000)
        self.assertIsNotNone(ok, "budget exhaustion is not what made this UNCHECKED")

    def test_small_histories_never_hit_the_default_budget(self) -> None:
        """The default must not turn the ordinary cases above into UNCHECKED."""
        ok, reason = _is_linearizable(self._wide_concurrent_history(3))
        self.assertIsNotNone(ok, f"default budget too small: {reason}")

    def test_verdict_is_a_function_of_the_history_alone(self) -> None:
        """A wall-clock deadline would make coverage depend on machine load, so
        the same seed could pass on CI and go unchecked on a laptop."""
        ops = self._wide_concurrent_history(10)
        verdicts = {_is_linearizable(ops, max_states=500)[0] for _ in range(3)}
        self.assertEqual(len(verdicts), 1, f"same history gave different verdicts: {verdicts}")


class CasOutcomeTests(unittest.TestCase):
    """The CAS outcome classifier is the first oracle in the chain.

    Everything above checks whether a history is linearizable. This checks
    whether the history is *true*: `do_cas` turns one psql transcript into
    `:ok` / `:fail` / `:info`, and a wrong answer here hands the checkers a
    history the cluster never produced. A fabricated `:ok` is the dangerous
    direction — it invents a compare-and-swap that succeeded, and the checker
    then reports a FATAL violation against a correct cluster.
    """

    def test_a_returned_row_is_a_commit(self) -> None:
        self.assertTrue(cas_committed("1\n"))
        self.assertTrue(cas_committed("1"))

    def test_no_returned_row_is_a_witness_mismatch(self) -> None:
        self.assertFalse(cas_committed(""))
        self.assertFalse(cas_committed("\n"))

    def test_a_diagnostic_mentioning_one_is_not_a_commit(self) -> None:
        """The command merges stderr into stdout to classify rejects, so
        anything `PostgreSQL` prints on a successful run shares the buffer with
        the result set. Substring-matching that buffer for `1` promoted every
        such line to a committed CAS."""
        for noise in (
            "psql:1: WARNING:  there is no transaction in progress",
            "NOTICE:  identifier will be truncated to 1 character",
            "UPDATE 1",
        ):
            with self.subTest(noise=noise):
                self.assertFalse(
                    cas_committed(noise),
                    f"a diagnostic was read as a committed CAS: {noise!r}",
                )

    def test_a_row_alongside_a_diagnostic_is_still_a_commit(self) -> None:
        """The fix must not go the other way and drop real commits."""
        self.assertTrue(cas_committed("WARNING:  something\n1\n"))


class ReadParseTests(unittest.TestCase):
    """`-t -A` renders an integer column as nothing but its digits, so a line
    carrying anything else is a diagnostic sharing the buffer, not a value.
    Reading one as the register's value puts a state in the history that the
    register never held."""

    def test_a_bare_integer_is_the_value(self) -> None:
        self.assertEqual(_parse_first_int("42\n"), 42)
        self.assertEqual(_parse_first_int("-7"), -7)

    def test_no_row_is_nil(self) -> None:
        self.assertIsNone(_parse_first_int(""))

    def test_a_line_with_trailing_text_is_not_a_value(self) -> None:
        for noise in ("1 row", "0 rows returned", "42 is the answer"):
            with self.subTest(noise=noise):
                self.assertIsNone(
                    _parse_first_int(noise),
                    f"a diagnostic was read as the register's value: {noise!r}",
                )


if __name__ == "__main__":
    unittest.main()
