"""Per-key consistency checkers: strict WGL linearizability and the weak
no-phantom-read check."""

from __future__ import annotations

import sys
from typing import Final

from linreg.records import Op

# Bound on the per-key op count WGL searches; a longer per-key history is
# reduced to one contiguous window of this size (see `_is_linearizable`).
#
# This caps memory and sort cost. It is NOT a bound on runtime: WGL's cost is
# driven by concurrency, not op count. Measured on a 45 s / 6-worker / 8-key
# kill run (~1,450 ops per key, 22% of them pending), 5 of 8 keys were still
# searching after 30 s while another finished in 0.3 s -- all far below this
# cap. `WGL_MAX_EXPLORED_STATES` is the bound that actually binds.
WGL_OPS_PER_KEY_CAP: Final[int] = 2000

WGL_MAX_EXPLORED_STATES: Final[int] = 250_000
"""Search states WGL may explore per key before giving up and saying so.

Counted states rather than wall-clock so a verdict is a function of the
history alone: the same `--seed` replays to the same answer on a loaded
laptop and on CI. A wall-clock deadline would make coverage depend on
machine speed, which is how an unchecked key starts reading as a pass.

Calibrated from the run above: keys that were decidable at all landed at
8 k, 71 k, and 127 k states, so this leaves roughly 2x headroom over the
worst decidable key while capping an undecidable one near 40 s.
"""


class _SearchExhausted(Exception):
    """Raised deep in the WGL recursion once the state budget is spent."""


def _apply_op_to_register(op: Op, current: int) -> tuple[bool, int]:
    """Compute the register transition for `op` given `current` state.

    Returns (matches_observed_result, new_value).

    `op.result` carries the outcome the client actually observed and drives
    the transition for every kind:

      - `True`  — acked. The op definitely took effect: a write installs
        `write_val`, a CAS installs it iff the witness matches `current`.
      - `False` — definitely rejected (PG refused the statement on a
        read-only / recovering node, or the CAS witness did not match). The
        op provably never reached the register, so it is an identity
        transition: placing it in the total order is always legal and never
        changes what a later read may observe. Modelling a rejection as a
        committed write would let a value that only a fenced node ever wrote
        be explained away as an ordinary write, which is the one anomaly
        class this checker must never miss.
      - `None`  — pending (timed out, connection died) or, for a read, an
        answer we never saw. A pending write may or may not have landed, so
        it is applied as if it committed; because pending ops carry
        `return_ts = None` (treated as +inf) the search can equally defer
        them to the end of the order, where nothing observes them. Those two
        placements are the "it happened" / "it didn't happen" branches.

    Known imprecision: `do_cas` reports both a witness mismatch and a
    fencing rejection as `False`. When such a CAS's witness happens to equal
    the register value at that point, the CAS branch below calls it a
    contradiction. That is a false positive, not a masked anomaly, and it
    needs a random witness to collide with the live value to occur.
    """
    if op.kind == "read":
        observed = op.result
        if observed is None:
            # No value observed (pending, or a definite reject that carries
            # no value) — consistent with any current state.
            return True, current
        return observed == current, current
    if op.kind == "write":
        new_val = op.write_val
        if new_val is None:
            return False, current
        if op.result is False:
            # Definitely rejected: no-op transition.
            return True, current
        return True, new_val
    if op.kind == "cas":
        old = op.cas_old
        new_val = op.write_val
        if old is None or new_val is None:
            return False, current
        succeeded_in_history = bool(op.result) if op.result is not None else None
        if old == current:
            # CAS would have committed.
            if succeeded_in_history is False:
                return False, current  # history says it failed → contradiction
            return True, new_val
        # CAS would have observed mismatch.
        if succeeded_in_history is True:
            return False, current  # history says it committed → contradiction
        return True, current
    return False, current


def _infer_register_value_at(ops_sorted: list[Op], at_ts: float) -> int:
    """Find the register value at wall time `at_ts`.

    Looks at all ops that COMPLETED before `at_ts` and picks the most recent
    successful write or committed CAS. If none, the register is at its
    initial value 0.
    """
    candidates = []
    for op in ops_sorted:
        if op.return_ts is None or op.return_ts > at_ts:
            continue
        if op.kind in ("write", "cas") and op.result is True and op.write_val is not None:
            candidates.append((op.return_ts, op.write_val))
    if not candidates:
        return 0
    candidates.sort()
    return candidates[-1][1]


def _is_weakly_consistent(ops: list[Op]) -> tuple[bool, str]:
    """Fast structural check that runs in O(n) on histories of any size.

    A value is *installable* if it is the initial 0 or the target of a write
    or CAS that could have reached the register: acked (`result is True`) or
    pending (`result is None` — timed out or the connection died, so it may
    have landed). A definitely-rejected op (`result is False`) is excluded:
    PG refused the statement, or the CAS witness did not match, so that value
    never entered the register through any legitimate path.

    Excluding rejected values cannot produce a false positive from value
    collisions: the installable set is a union, so a value written by both a
    rejected and an acked/pending op stays in it. It only drops out when NO
    op that could have taken effect ever wrote it.

    Verifies two properties:

      W-1 (no phantom reads): every value returned by a successful read is
          installable. A read returning a value no client could have written
          indicates split-brain, a fenced node still serving writes, or
          memory corruption.

      W-2 (no impossible CAS commit): every committed CAS observed an
          installable witness. If a CAS commits on a witness nothing could
          have installed, the cluster fabricated the witness.

    Weaker than WGL (doesn't check real-time ordering), but tractable at
    any scale and catches the loudest split-brain symptoms.
    """
    legit_values = {0}
    for op in ops:
        if op.write_val is not None and op.result is not False:
            legit_values.add(op.write_val)

    for op in ops:
        if op.kind == "read" and op.result is not None and op.result not in legit_values:
            return False, (
                f"phantom read: value {op.result!r} is not installable "
                "(no acked or pending write ever wrote it)"
            )
        if (
            op.kind == "cas"
            and op.result is True
            and op.cas_old is not None
            and op.cas_old not in legit_values
        ):
            return False, f"impossible CAS commit on witness {op.cas_old!r} (not installable)"

    return True, f"weakly-consistent ({len(legit_values)} installable values)"


def _is_linearizable(
    ops: list[Op], *, max_states: int = WGL_MAX_EXPLORED_STATES
) -> tuple[bool | None, str]:
    """WGL search over `ops` (single-register history). Returns (ok, reason).

    `ok` is True (linearizable), False (no valid total order exists), or None
    (the search hit `max_states` and the key is simply unchecked). None is not
    a pass: callers must surface it, because a history nobody could check looks
    exactly like a history with nothing wrong in it.
    """
    # Only consider ops that completed OR have at least an invoke timestamp
    # (pending ops are tried both ways via the loop below).
    if not ops:
        return True, "empty history"

    # Sort by invoke time for stable iteration order.
    ops_sorted = sorted(ops, key=lambda o: o.invoke_ts)
    initial_value = 0

    if len(ops_sorted) > WGL_OPS_PER_KEY_CAP:
        # Take a CONTIGUOUS WINDOW centered on the median return time
        # (workload-symmetric heuristic; lands near the fault for symmetric
        # workloads). Then INFER the register's value at the window's start
        # from the prefix of ops that completed before it — otherwise WGL
        # would start at 0 and fail any read returning a value written
        # earlier in history.
        completed_return_ts = [o.return_ts for o in ops_sorted if o.return_ts is not None]
        if completed_return_ts:
            mid_ts = sorted(completed_return_ts)[len(completed_return_ts) // 2]
            center = min(
                range(len(ops_sorted)),
                key=lambda i: abs(ops_sorted[i].invoke_ts - mid_ts),
            )
        else:
            center = len(ops_sorted) // 2
        half = WGL_OPS_PER_KEY_CAP // 2
        start = max(0, min(len(ops_sorted) - WGL_OPS_PER_KEY_CAP, center - half))
        window_start_ts = ops_sorted[start].invoke_ts
        # Critical: infer starting register value from the prefix we're
        # discarding. Without this, WGL hallucinates anomalies.
        initial_value = _infer_register_value_at(ops_sorted[:start], window_start_ts)
        ops_sorted = ops_sorted[start : start + WGL_OPS_PER_KEY_CAP]

    # Cache return_ts of completed ops; pending ops get +inf.
    return_of: dict[int, float] = {}
    for o in ops_sorted:
        return_of[o.op_id] = o.return_ts if o.return_ts is not None else float("inf")

    op_by_id: dict[int, Op] = {o.op_id: o for o in ops_sorted}
    remaining_init: frozenset[int] = frozenset(op_by_id)

    visited: set[tuple[frozenset[int], int]] = set()
    explored = 0

    sys.setrecursionlimit(10_000)

    def search(remaining: frozenset[int], reg_val: int) -> bool:
        nonlocal explored
        if not remaining:
            return True
        state_key = (remaining, reg_val)
        if state_key in visited:
            return False
        visited.add(state_key)
        explored += 1
        if explored > max_states:
            raise _SearchExhausted
        # Minimum return_ts among remaining — only ops invoked at-or-before
        # this are eligible to be linearized next (others must come strictly
        # after by real-time order).
        min_return = min(return_of[i] for i in remaining)
        candidates = [op_by_id[i] for i in remaining if op_by_id[i].invoke_ts <= min_return]
        for candidate in candidates:
            matches, new_val = _apply_op_to_register(candidate, reg_val)
            if not matches:
                continue
            if search(remaining - {candidate.op_id}, new_val):
                return True
        return False

    try:
        ok = search(remaining_init, initial_value)
    except _SearchExhausted:
        return None, (
            f"UNCHECKED: search exceeded {max_states:,} states on {len(ops_sorted)} ops "
            f"({sum(1 for o in ops_sorted if o.return_ts is None)} pending). "
            "Shorten --duration, lower --workers, or use --check weak."
        )
    if ok:
        return True, "linearizable"
    return False, "no total order satisfies real-time + sequential register semantics"
