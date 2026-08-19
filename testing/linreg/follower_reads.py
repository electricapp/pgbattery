"""H-10: follower reads and predicate reads, and the properties they can break.

A read served by a standby is not the same read the gateway serves. It can lag,
it can disagree with another standby, and — if replication or visibility is
wrong — it can show something no client ever wrote. None of that was in the test
universe: every operation went through the gateway to the leader.

Reads here carry the standby's `pg_last_wal_replay_lsn()` at the moment they ran,
which is what makes them checkable rather than merely stale. A follower that has
replayed past a commit must show it; one that has not is allowed to lag.

Predicate reads (`WHERE val > n`) are the other half: a point read cannot express
a phantom, because a phantom is a row entering a *set* that a repeated predicate
should not have gained.

Every checker is pure and returns `(ok, detail)`, so `test_follower_reads.py` can
hand each one a history that violates it and require a rejection back.
"""

from __future__ import annotations

from dataclasses import dataclass, field


def lsn_to_int(lsn: str) -> int:
    """`0/16B3748` as a comparable integer. Raises on anything else."""
    hi, _, lo = lsn.strip().partition("/")
    if not lo:
        raise ValueError(f"not a PostgreSQL LSN: {lsn!r}")
    return (int(hi, 16) << 32) | int(lo, 16)


@dataclass
class FollowerRead:
    """One read served by a standby.

    `replay_lsn` is the standby's replay position at the moment of the read, so
    the check is "did it show what it had replayed", not "was it fresh".
    """

    node: str
    key: int
    value: int | None
    replay_lsn: str
    at: float


@dataclass
class LeaderWrite:
    """One acknowledged write, with the position it committed at."""

    key: int
    value: int
    commit_lsn: str
    at: float


@dataclass
class PredicateRead:
    """A set-returning read, which is where phantoms live."""

    node: str
    threshold: int
    keys: list[int] = field(default_factory=list)
    at: float = 0.0


def no_invented_values(reads: list[FollowerRead], writes: list[LeaderWrite]) -> tuple[bool, str]:
    """Every value a standby returned was written by somebody.

    A standby showing a value no client ever wrote is the loudest possible
    symptom: a fork, a fabricated record, or corruption.
    """
    written = {0} | {w.value for w in writes}
    for read in reads:
        if read.value is not None and read.value not in written:
            return False, (
                f"{read.node} returned {read.value!r} for key {read.key}, which no write produced"
            )
    return True, f"every observed value came from a write ({len(written)} distinct)"


def reads_are_monotonic(reads: list[FollowerRead]) -> tuple[bool, str]:
    """A standby's view of a key never goes backwards.

    Replication only moves forward, so a value regressing on one node means it
    replayed backwards or served from two different states — a fork inside a
    single node.
    """
    seen: dict[tuple[str, int], tuple[int, str]] = {}
    for read in sorted(reads, key=lambda r: r.at):
        if read.value is None:
            continue
        prior = seen.get((read.node, read.key))
        if prior is not None:
            prior_value, prior_lsn = prior
            if lsn_to_int(read.replay_lsn) >= lsn_to_int(prior_lsn) and read.value < prior_value:
                return False, (
                    f"{read.node} key {read.key} went backwards: saw {prior_value} at "
                    f"{prior_lsn}, then {read.value} at {read.replay_lsn}"
                )
        seen[(read.node, read.key)] = (read.value, read.replay_lsn)
    return True, f"no standby view regressed across {len(reads)} reads"


def replayed_writes_are_visible(
    reads: list[FollowerRead], writes: list[LeaderWrite]
) -> tuple[bool, str]:
    """A standby that has replayed past a commit shows it.

    This is the property that makes a follower read checkable at all. Lagging is
    allowed; having replayed the WAL and still serving the older value is not —
    that is a visibility bug, not staleness.
    """
    by_key: dict[int, list[LeaderWrite]] = {}
    for write in writes:
        by_key.setdefault(write.key, []).append(write)
    for values in by_key.values():
        values.sort(key=lambda w: lsn_to_int(w.commit_lsn))

    for read in reads:
        if read.value is None:
            continue
        replayed = lsn_to_int(read.replay_lsn)
        # The newest write this standby has definitely replayed.
        expected: LeaderWrite | None = None
        for write in by_key.get(read.key, []):
            if lsn_to_int(write.commit_lsn) <= replayed:
                expected = write
            else:
                break
        if expected is not None and read.value < expected.value:
            return False, (
                f"{read.node} replayed to {read.replay_lsn} — past the write of "
                f"{expected.value} at {expected.commit_lsn} — but served "
                f"{read.value} for key {read.key}"
            )
    return True, f"every replayed write was visible across {len(reads)} reads"


def no_long_fork(reads: list[FollowerRead]) -> tuple[bool, str]:
    """Two standbys never order the same key's writes differently.

    A long fork shows as one node seeing a→b while another sees b→a. Both are
    replaying the same WAL, so any disagreement in order is a fork.
    """
    order: dict[int, dict[str, list[int]]] = {}
    for read in sorted(reads, key=lambda r: r.at):
        if read.value is None:
            continue
        per_node = order.setdefault(read.key, {})
        seq = per_node.setdefault(read.node, [])
        if not seq or seq[-1] != read.value:
            seq.append(read.value)

    for key, per_node in order.items():
        pairs = list(per_node.items())
        for i, (node_a, seq_a) in enumerate(pairs):
            for node_b, seq_b in pairs[i + 1 :]:
                conflict = _conflicting_order(seq_a, seq_b)
                if conflict is not None:
                    first, second = conflict
                    return False, (
                        f"long fork on key {key}: {node_a} saw {first} before {second}, "
                        f"{node_b} saw the reverse"
                    )
    return True, f"no two standbys disagreed on order across {len(order)} keys"


def _conflicting_order(seq_a: list[int], seq_b: list[int]) -> tuple[int, int] | None:
    """Two values one sequence orders one way and the other orders the other."""
    index_a = {value: i for i, value in enumerate(seq_a)}
    index_b = {value: i for i, value in enumerate(seq_b)}
    shared = sorted(set(index_a) & set(index_b))
    for i, first in enumerate(shared):
        for second in shared[i + 1 :]:
            a_order = index_a[first] < index_a[second]
            b_order = index_b[first] < index_b[second]
            if a_order != b_order:
                return (first, second) if a_order else (second, first)
    return None


def predicate_reads_gain_only_written_keys(
    reads: list[PredicateRead], writes: list[LeaderWrite]
) -> tuple[bool, str]:
    """A predicate's result set only ever gains keys some write qualified.

    The phantom shape: a repeated `WHERE val > n` returning a key that no write
    ever pushed above `n`. A point read cannot express this, which is why the
    workload needs a set-returning one.
    """
    for read in reads:
        qualifying = {w.key for w in writes if w.value > read.threshold and w.at <= read.at}
        unexplained = [k for k in read.keys if k not in qualifying]
        if unexplained:
            return False, (
                f"{read.node} matched keys {sorted(unexplained)} for val > {read.threshold}, "
                "which no write qualified"
            )
    return True, f"every predicate match was explained by a write ({len(reads)} reads)"
