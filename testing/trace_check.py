"""Check a recorded run against `tla/lease_fencing.tla` and `tla/raft_lsn.tla`.

The binary records the transitions the specs model (`SpecEvent`,
`src/observability/spec_trace.rs`) into each node's debug-event buffer. This
module parses those events once, at the boundary, and checks every rule in
`RULES` against them. Each rule names the spec definitions it refines, and
`lint_matrix` holds those names to the specs.

Sequence numbers are contiguous per process, so an event the ring evicted
before it was read is a known hole, never a silent one. A rule that would need
a hole's contents reports the instance as unverifiable, never as a violation.
"""

from __future__ import annotations

import bisect
import sys
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Annotated, Final, Literal, TypeVar

from pydantic import BaseModel, Field, TypeAdapter

import api_models

SPEC_EVENT_TYPE: Final[str] = "spec"
# Both ends of a hold-down are millisecond stamps, each truncated once.
CLOCK_SLACK_MS: Final[int] = 1
# An authority span still open when the trace was last read.
OPEN_END_MS: Final[int] = sys.maxsize


# ---------------------------------------------------------------------------
# Boundary
# ---------------------------------------------------------------------------


class _VoterWalAt(BaseModel):
    at: int


class _Raft(BaseModel):
    kind: Literal["raft"]
    term: int
    leader: int | None


class _Anchor(BaseModel):
    kind: Literal["anchor"]
    stamped: bool


class _Open(BaseModel):
    kind: Literal["open"]


class _Writes(BaseModel):
    kind: Literal["writes"]
    writable: bool | None
    lease_valid: bool
    lease_leader: bool


class _Vote(BaseModel):
    kind: Literal["vote"]
    term: int
    candidate: int
    granted: bool
    candidate_lsn: int | None
    voter_wal: Literal["empty", "unknown"] | _VoterWalAt
    threshold: int


_SPEC_DATA: Final[TypeAdapter[_Raft | _Anchor | _Open | _Writes | _Vote]] = TypeAdapter(
    Annotated[_Raft | _Anchor | _Open | _Writes | _Vote, Field(discriminator="kind")]
)


class TraceParseError(RuntimeError):
    """The events read from a node do not form a trace."""


@dataclass(frozen=True)
class RaftStep:
    node: int
    seq: int
    at_ms: int
    term: int
    leader: int | None


@dataclass(frozen=True)
class AnchorStep:
    node: int
    seq: int
    at_ms: int
    stamped: bool


@dataclass(frozen=True)
class OpenStep:
    node: int
    seq: int
    at_ms: int


@dataclass(frozen=True)
class WriteStep:
    node: int
    seq: int
    at_ms: int
    writable: bool | None
    lease_valid: bool
    lease_leader: bool

    @property
    def authority(self) -> bool | None:
        """`HasWriteAuthority`: a valid lease over a writable primary. `None`
        when the lease was valid and the probe could not read writability."""
        return self.writable if self.lease_valid else False


@dataclass(frozen=True)
class VoteStep:
    node: int
    seq: int
    at_ms: int
    term: int
    candidate: int
    granted: bool
    candidate_lsn: int | None
    voter_lsn: int | None
    voter_empty: bool
    threshold: int


@dataclass(frozen=True)
class NodeTrace:
    """One process's spec events in sequence order, and the holes in them."""

    node: int
    last_seq: int
    missing: tuple[int, ...]
    raft: tuple[RaftStep, ...] = ()
    anchors: tuple[AnchorStep, ...] = ()
    opens: tuple[OpenStep, ...] = ()
    writes: tuple[WriteStep, ...] = ()
    votes: tuple[VoteStep, ...] = ()

    def gap_free(self, after: int, before: int) -> bool:
        """No event this process recorded with `after < seq < before` is missing."""
        start = bisect.bisect_right(self.missing, after)
        return start == len(self.missing) or self.missing[start] >= before

    def known_before(self, seq: int) -> bool:
        return self.gap_free(0, seq)


@dataclass(frozen=True)
class Trace:
    nodes: Mapping[int, NodeTrace]

    def all_raft(self) -> Iterable[RaftStep]:
        for trace in self.nodes.values():
            yield from trace.raft


def _voter_wal(wal: Literal["empty", "unknown"] | _VoterWalAt) -> tuple[int | None, bool]:
    """`(lsn, empty)` from the serialized `VoterWal`."""
    match wal:
        case "empty":
            return None, True
        case "unknown":
            return None, False
        case _VoterWalAt(at=lsn):
            return lsn, False


def node_trace(node: int, events: Iterable[api_models.DebugEvent], current_seq: int) -> NodeTrace:
    """Parse one process's events, read through `current_seq`.

    Every event type counts toward the sequence; only `spec` events become
    steps. A sequence number never read, from 1 through `current_seq`, is a
    hole.
    """
    seen: set[int] = set()
    raft: list[RaftStep] = []
    anchors: list[AnchorStep] = []
    opens: list[OpenStep] = []
    writes: list[WriteStep] = []
    votes: list[VoteStep] = []
    for event in sorted(events, key=lambda e: e.seq):
        if event.node_id != node:
            raise TraceParseError(f"node {node} returned an event of node {event.node_id}")
        if event.seq in seen:
            continue
        seen.add(event.seq)
        if event.event_type != SPEC_EVENT_TYPE:
            continue
        at = (node, event.seq, event.timestamp_ms)
        match _SPEC_DATA.validate_python(event.data):
            case _Raft(term=term, leader=leader):
                raft.append(RaftStep(*at, term=term, leader=leader))
            case _Anchor(stamped=stamped):
                anchors.append(AnchorStep(*at, stamped=stamped))
            case _Open():
                opens.append(OpenStep(*at))
            case _Writes(writable=writable, lease_valid=valid, lease_leader=leader):
                writes.append(WriteStep(*at, writable, valid, leader))
            case _Vote() as vote:
                lsn, empty = _voter_wal(vote.voter_wal)
                votes.append(
                    VoteStep(
                        *at,
                        term=vote.term,
                        candidate=vote.candidate,
                        granted=vote.granted,
                        candidate_lsn=vote.candidate_lsn,
                        voter_lsn=lsn,
                        voter_empty=empty,
                        threshold=vote.threshold,
                    )
                )
    if seen and max(seen) > current_seq:
        raise TraceParseError(f"node {node} returned seq {max(seen)} past current {current_seq}")
    return NodeTrace(
        node=node,
        last_seq=current_seq,
        missing=tuple(s for s in range(1, current_seq + 1) if s not in seen),
        raft=tuple(raft),
        anchors=tuple(anchors),
        opens=tuple(opens),
        writes=tuple(writes),
        votes=tuple(votes),
    )


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Rule:
    name: str
    refs: tuple[str, ...]
    """`spec.Definition` names in `tla/`; lint resolves each one."""
    claim: str


@dataclass(frozen=True)
class Bounds:
    hold_down_ms: int
    voters: int


@dataclass(frozen=True)
class Finding:
    rule: str
    node: int
    detail: str


@dataclass
class Report:
    violations: list[Finding] = field(default_factory=list)
    unverifiable: list[Finding] = field(default_factory=list)
    checked: Counter[str] = field(default_factory=Counter)

    def violated(self, rule: Rule) -> list[Finding]:
        return [f for f in self.violations if f.rule == rule.name]


TERM_MONOTONIC: Final[Rule] = Rule(
    "term-monotonic", ("lease_fencing.Next",), "a node's term never decreases"
)
ONE_LEADER_PER_TERM: Final[Rule] = Rule(
    "one-leader-per-term",
    ("lease_fencing.OneLeaderPerTerm", "raft_lsn.ElectionSafety"),
    "at most one node believes itself leader of a term",
)
WRITABLE_IMPLIES_LEASE_LEADER: Final[Rule] = Rule(
    "writable-implies-lease-leader",
    ("lease_fencing.WritableImpliesLeaseLeader",),
    "a writable primary sits under a lease that believes it leads",
)
FENCE_LANDS: Final[Rule] = Rule(
    "fence-lands",
    ("lease_fencing.Fence", "lease_fencing.LeaseIsValid"),
    "no fence tick leaves a writable primary under an invalid lease",
)
OPEN_GATED: Final[Rule] = Rule(
    "open-gated",
    ("lease_fencing.EnablePgWrites", "lease_fencing.HoldDownSatisfied"),
    "writes open only on a believed leader whose anchor, if stamped, is a hold-down old",
)
HOLD_DOWN_FROM_TERM: Final[Rule] = Rule(
    "hold-down-from-term",
    ("lease_fencing.StartCampaign", "lease_fencing.WinElection"),
    "an open in a term with an earlier leader waits a hold-down after the node first saw the term",
)
WRITABLE_VIA_OPEN: Final[Rule] = Rule(
    "writable-via-open",
    ("lease_fencing.EnablePgWrites",),
    "a primary becomes writable only through the write gate",
)
ONE_WRITE_AUTHORITY: Final[Rule] = Rule(
    "one-write-authority",
    ("lease_fencing.AtMostOneWriteAuthority", "lease_fencing.HasWriteAuthority"),
    "no two nodes hold write authority at once",
)
VOTE_LSN_ACCEPTABLE: Final[Rule] = Rule(
    "vote-lsn-acceptable",
    ("raft_lsn.IsLSNAcceptable",),
    "a granted vote's candidate is within the threshold of the voter's WAL",
)
ONE_VOTE_PER_TERM: Final[Rule] = Rule(
    "one-vote-per-term",
    ("raft_lsn.CanVoteFor",),
    "a voter grants at most one candidate per term, and none below its own term",
)
LEADER_HAS_QUORUM: Final[Rule] = Rule(
    "leader-has-quorum",
    ("raft_lsn.BecomeLeader", "raft_lsn.HasQuorum"),
    "a leader of a term holds grants from a quorum, its own included",
)

RULES: Final[tuple[Rule, ...]] = (
    TERM_MONOTONIC,
    ONE_LEADER_PER_TERM,
    WRITABLE_IMPLIES_LEASE_LEADER,
    FENCE_LANDS,
    OPEN_GATED,
    HOLD_DOWN_FROM_TERM,
    WRITABLE_VIA_OPEN,
    ONE_WRITE_AUTHORITY,
    VOTE_LSN_ACCEPTABLE,
    ONE_VOTE_PER_TERM,
    LEADER_HAS_QUORUM,
)


StepT = TypeVar("StepT", RaftStep, AnchorStep, WriteStep)


def _latest_before(steps: Sequence[StepT], seq: int) -> StepT | None:
    index = bisect.bisect_left([s.seq for s in steps], seq)
    return steps[index - 1] if index else None


def _first_seen(trace: NodeTrace, term: int) -> RaftStep | None:
    """The step at which `trace` first recorded `term`, if nothing before it
    could have recorded it first."""
    first = next((s for s in trace.raft if s.term >= term), None)
    if first is None or first.term != term:
        return None
    earlier = [s for s in trace.raft if s.seq < first.seq]
    after = earlier[-1].seq if earlier else 0
    return first if trace.gap_free(after, first.seq) else None


def _check_terms(trace: Trace, report: Report) -> None:
    for node in trace.nodes.values():
        for prev, step in zip(node.raft, node.raft[1:], strict=False):
            report.checked[TERM_MONOTONIC.name] += 1
            if step.term < prev.term:
                report.violations.append(
                    Finding(
                        TERM_MONOTONIC.name,
                        node.node,
                        f"term {prev.term} then {step.term} (seq {step.seq})",
                    )
                )


def _check_leaders(trace: Trace, report: Report) -> None:
    claims: defaultdict[int, set[int]] = defaultdict(set)
    for step in trace.all_raft():
        if step.leader == step.node:
            claims[step.term].add(step.node)
    for term, nodes in sorted(claims.items()):
        report.checked[ONE_LEADER_PER_TERM.name] += 1
        if len(nodes) > 1:
            report.violations.append(
                Finding(ONE_LEADER_PER_TERM.name, min(nodes), f"term {term} led by {sorted(nodes)}")
            )


def _check_write_steps(trace: Trace, report: Report) -> None:
    for node in trace.nodes.values():
        for step in node.writes:
            if step.writable:
                report.checked[WRITABLE_IMPLIES_LEASE_LEADER.name] += 1
                if not step.lease_leader:
                    report.violations.append(
                        Finding(
                            WRITABLE_IMPLIES_LEASE_LEADER.name,
                            node.node,
                            f"writable with the lease not leading (seq {step.seq})",
                        )
                    )
            if step.lease_valid:
                continue
            report.checked[FENCE_LANDS.name] += 1
            if step.writable:
                report.violations.append(
                    Finding(
                        FENCE_LANDS.name,
                        node.node,
                        f"left writable under an invalid lease (seq {step.seq})",
                    )
                )
            elif step.writable is None:
                report.unverifiable.append(
                    Finding(FENCE_LANDS.name, node.node, f"writability unread (seq {step.seq})")
                )


def _check_opens(trace: Trace, bounds: Bounds, report: Report) -> None:
    complete = all(n.known_before(n.last_seq + 1) for n in trace.nodes.values())
    for node in trace.nodes.values():
        for open_ in node.opens:
            raft = _latest_before(node.raft, open_.seq)
            if raft is None or raft.leader != node.node:
                if raft is None and not node.known_before(open_.seq):
                    report.unverifiable.append(
                        Finding(OPEN_GATED.name, node.node, f"no leadership seen (seq {open_.seq})")
                    )
                else:
                    report.checked[OPEN_GATED.name] += 1
                    report.violations.append(
                        Finding(
                            OPEN_GATED.name,
                            node.node,
                            f"opened believing {raft.leader if raft else None} leads "
                            f"(seq {open_.seq})",
                        )
                    )
                continue
            _check_open_anchor(node, open_, bounds, report)
            _check_open_term(trace, node, open_, raft.term, bounds, complete, report)


def _check_open_anchor(node: NodeTrace, open_: OpenStep, bounds: Bounds, report: Report) -> None:
    anchor = _latest_before(node.anchors, open_.seq)
    if anchor is None and not node.known_before(open_.seq):
        report.unverifiable.append(
            Finding(OPEN_GATED.name, node.node, f"anchor history unread (seq {open_.seq})")
        )
        return
    report.checked[OPEN_GATED.name] += 1
    if anchor is None or not anchor.stamped:
        return
    held = open_.at_ms - anchor.at_ms
    if held < bounds.hold_down_ms - CLOCK_SLACK_MS:
        report.violations.append(
            Finding(
                OPEN_GATED.name,
                node.node,
                f"opened {held} ms after its anchor, inside the {bounds.hold_down_ms} ms "
                f"hold-down (seq {open_.seq})",
            )
        )


def _check_open_term(
    trace: Trace,
    node: NodeTrace,
    open_: OpenStep,
    term: int,
    bounds: Bounds,
    complete: bool,
    report: Report,
) -> None:
    earlier_leader = any(s.leader is not None and s.term < term for s in trace.all_raft())
    if not earlier_leader:
        if not complete:
            report.unverifiable.append(
                Finding(
                    HOLD_DOWN_FROM_TERM.name,
                    node.node,
                    f"term {term}: earlier leaders unread (seq {open_.seq})",
                )
            )
        return
    first = _first_seen(node, term)
    if first is None:
        report.unverifiable.append(
            Finding(
                HOLD_DOWN_FROM_TERM.name,
                node.node,
                f"term {term}: first sighting unread (seq {open_.seq})",
            )
        )
        return
    report.checked[HOLD_DOWN_FROM_TERM.name] += 1
    held = open_.at_ms - first.at_ms
    if held < bounds.hold_down_ms - CLOCK_SLACK_MS:
        report.violations.append(
            Finding(
                HOLD_DOWN_FROM_TERM.name,
                node.node,
                f"opened {held} ms into term {term}, inside the {bounds.hold_down_ms} ms "
                f"hold-down (seq {open_.seq})",
            )
        )


def _check_writable_edges(trace: Trace, report: Report) -> None:
    for node in trace.nodes.values():
        # The seq at which writable last turned not writable. A closed reading
        # without a writable one before it is the primary not yet open, which
        # an open admitted just before may still be applying.
        closed_at = 0
        was_writable = False
        prev: WriteStep | None = None
        for step in node.writes:
            edge = step.writable is True and (prev is None or prev.writable is not True)
            if step.writable is True:
                was_writable = True
            elif step.writable is False and was_writable:
                closed_at = step.seq
                was_writable = False
            if edge:
                _check_edge(node, step, prev, closed_at, report)
            prev = step


def _check_edge(
    node: NodeTrace, step: WriteStep, prev: WriteStep | None, closed_at: int, report: Report
) -> None:
    if prev is None and not node.known_before(step.seq):
        report.unverifiable.append(
            Finding(WRITABLE_VIA_OPEN.name, node.node, f"state before seq {step.seq} unread")
        )
        return
    opened = any(closed_at < o.seq < step.seq for o in node.opens)
    if opened:
        report.checked[WRITABLE_VIA_OPEN.name] += 1
    elif not node.gap_free(closed_at, step.seq):
        report.unverifiable.append(
            Finding(WRITABLE_VIA_OPEN.name, node.node, f"a hole before seq {step.seq}")
        )
    else:
        report.checked[WRITABLE_VIA_OPEN.name] += 1
        report.violations.append(
            Finding(
                WRITABLE_VIA_OPEN.name,
                node.node,
                f"writable at seq {step.seq} with no open since seq {closed_at}",
            )
        )


@dataclass(frozen=True)
class Span:
    node: int
    start_ms: int
    end_ms: int
    certain: bool


def authority_spans(node: NodeTrace) -> list[Span]:
    """Maximal runs of steps holding (or possibly holding) write authority."""
    spans: list[Span] = []
    start: WriteStep | None = None
    certain = True
    prev: WriteStep | None = None
    for step in node.writes:
        holds = step.authority
        if start is not None and prev is not None and not node.gap_free(prev.seq, step.seq):
            certain = False
        if holds is False:
            if start is not None:
                spans.append(Span(node.node, start.at_ms, step.at_ms, certain))
                start = None
        else:
            if start is None:
                start = step
                certain = True
            if holds is None:
                certain = False
        prev = step
    if start is not None and prev is not None:
        tail_known = node.gap_free(prev.seq, node.last_seq + 1)
        spans.append(Span(node.node, start.at_ms, OPEN_END_MS, certain and tail_known))
    return spans


def _check_authority(trace: Trace, report: Report) -> None:
    spans = [s for n in trace.nodes.values() for s in authority_spans(n)]
    report.checked[ONE_WRITE_AUTHORITY.name] += len(spans)
    for i, a in enumerate(spans):
        for b in spans[i + 1 :]:
            if a.node == b.node or a.start_ms >= b.end_ms or b.start_ms >= a.end_ms:
                continue
            finding = Finding(
                ONE_WRITE_AUTHORITY.name,
                a.node,
                f"node {a.node} [{a.start_ms}, {a.end_ms}) and node {b.node} "
                f"[{b.start_ms}, {b.end_ms}) both held write authority",
            )
            (report.violations if a.certain and b.certain else report.unverifiable).append(finding)


def _check_votes(trace: Trace, report: Report) -> None:
    for node in trace.nodes.values():
        backed: defaultdict[int, set[int]] = defaultdict(set)
        for vote in node.votes:
            if not vote.granted:
                continue
            report.checked[VOTE_LSN_ACCEPTABLE.name] += 1
            report.checked[ONE_VOTE_PER_TERM.name] += 1
            if problem := _lsn_problem(vote):
                report.violations.append(
                    Finding(VOTE_LSN_ACCEPTABLE.name, node.node, f"{problem} (seq {vote.seq})")
                )
            backed[vote.term].add(vote.candidate)
            own = _latest_before(node.raft, vote.seq)
            if own is not None and own.term > vote.term:
                report.violations.append(
                    Finding(
                        ONE_VOTE_PER_TERM.name,
                        node.node,
                        f"granted term {vote.term} while at term {own.term} (seq {vote.seq})",
                    )
                )
        for term, candidates in sorted(backed.items()):
            if len(candidates) > 1:
                report.violations.append(
                    Finding(
                        ONE_VOTE_PER_TERM.name,
                        node.node,
                        f"granted term {term} to {sorted(candidates)}",
                    )
                )


def _lsn_problem(vote: VoteStep) -> str | None:
    if vote.voter_empty:
        return None
    if vote.voter_lsn is None:
        return "granted with its own WAL unread"
    if vote.candidate_lsn is None:
        return "granted a candidate that sent no WAL position"
    if vote.candidate_lsn + vote.threshold < vote.voter_lsn:
        return (
            f"granted candidate {vote.candidate} at {vote.candidate_lsn} + {vote.threshold} "
            f"below its own {vote.voter_lsn}"
        )
    return None


def _witnessed(voter: NodeTrace, term: int) -> bool:
    """Whether `voter`'s every answer in `term`'s election was read: it was
    present before the term and nothing it recorded until it moved past the
    term is missing."""
    before = [s for s in voter.raft if s.term < term]
    if not before:
        return False
    past = next((s.seq for s in voter.raft if s.term > term), voter.last_seq + 1)
    return voter.gap_free(before[-1].seq, past)


def _check_quorums(trace: Trace, bounds: Bounds, report: Report) -> None:
    need = bounds.voters // 2  # a quorum less the candidate's own vote
    claims = sorted({(s.node, s.term) for s in trace.all_raft() if s.leader == s.node})
    for leader, term in claims:
        others = [n for n in trace.nodes.values() if n.node != leader]
        grants = {
            n.node
            for n in others
            for v in n.votes
            if v.granted and v.candidate == leader and v.term == term
        }
        if len(grants) >= need:
            report.checked[LEADER_HAS_QUORUM.name] += 1
            continue
        verifiable = len(trace.nodes) == bounds.voters and all(_witnessed(n, term) for n in others)
        finding = Finding(
            LEADER_HAS_QUORUM.name,
            leader,
            f"led term {term} with grants from {sorted(grants)}, needing {need}",
        )
        if verifiable:
            report.checked[LEADER_HAS_QUORUM.name] += 1
            report.violations.append(finding)
        else:
            report.unverifiable.append(finding)


def check(trace: Trace, bounds: Bounds) -> Report:
    report = Report()
    _check_terms(trace, report)
    _check_leaders(trace, report)
    _check_write_steps(trace, report)
    _check_opens(trace, bounds, report)
    _check_writable_edges(trace, report)
    _check_authority(trace, report)
    _check_votes(trace, report)
    _check_quorums(trace, bounds, report)
    return report


# ---------------------------------------------------------------------------
# Drifts: what a regression would leave in a real trace
# ---------------------------------------------------------------------------


class DriftNotApplicable(RuntimeError):
    """The trace lacks the shape this drift rewrites."""


def _with(trace: Trace, node: NodeTrace) -> Trace:
    return Trace({**trace.nodes, node.node: node})


def _failover_open(trace: Trace) -> tuple[NodeTrace, OpenStep, RaftStep]:
    """The last open in a term some earlier term's leader preceded, with the
    step at which its node first saw that term."""
    found: list[tuple[NodeTrace, OpenStep, RaftStep]] = []
    for node in trace.nodes.values():
        for open_ in node.opens:
            raft = _latest_before(node.raft, open_.seq)
            if raft is None or raft.leader != node.node:
                continue
            earlier = any(s.leader is not None and s.term < raft.term for s in trace.all_raft())
            first = _first_seen(node, raft.term)
            if earlier and first is not None:
                found.append((node, open_, first))
    if not found:
        raise DriftNotApplicable("no open after a failover")
    return max(found, key=lambda f: f[1].at_ms)


def drift_open_inside_hold_down(trace: Trace, bounds: Bounds) -> Trace:
    """The winner opens half a hold-down into its term."""
    node, open_, first = _failover_open(trace)
    early = replace(open_, at_ms=first.at_ms + bounds.hold_down_ms // 2)
    opens = tuple(early if o == open_ else o for o in node.opens)
    return _with(trace, replace(node, opens=opens))


def drift_deposed_never_fenced(trace: Trace, bounds: Bounds) -> Trace:
    """The leader deposed before the failover open keeps its authority."""
    node, _, _ = _failover_open(trace)
    for other in trace.nodes.values():
        if other.node == node.node:
            continue
        ended = [s for s in authority_spans(other) if s.end_ms != OPEN_END_MS]
        if not ended:
            continue
        last_end = max(s.end_ms for s in ended)
        writes = tuple(
            replace(w, writable=True, lease_valid=True, lease_leader=True)
            if w.at_ms >= last_end
            else w
            for w in other.writes
        )
        return _with(trace, replace(other, writes=writes))
    raise DriftNotApplicable("no other node gave up write authority")


def _granted(trace: Trace) -> tuple[NodeTrace, VoteStep]:
    for node in trace.nodes.values():
        for vote in node.votes:
            if vote.granted:
                return node, vote
    raise DriftNotApplicable("no vote was granted")


def drift_double_vote(trace: Trace, bounds: Bounds) -> Trace:
    """A voter also backs a second candidate in a term it already granted."""
    node, vote = _granted(trace)
    rival = next((n for n in trace.nodes if n not in (node.node, vote.candidate)), None)
    if rival is None:
        raise DriftNotApplicable("no third node to back")
    seq = node.last_seq + 1
    second = replace(vote, seq=seq, candidate=rival)
    return _with(trace, replace(node, last_seq=seq, votes=(*node.votes, second)))


def drift_vote_past_wal_gate(trace: Trace, bounds: Bounds) -> Trace:
    """A voter grants a candidate further behind than the threshold allows."""
    node, vote = _granted(trace)
    candidate = vote.candidate_lsn or 0
    behind = replace(
        vote,
        candidate_lsn=candidate,
        voter_lsn=candidate + vote.threshold + 1,
        voter_empty=False,
    )
    votes = tuple(behind if v == vote else v for v in node.votes)
    return _with(trace, replace(node, votes=votes))


def drift_two_leaders(trace: Trace, bounds: Bounds) -> Trace:
    """A second node believes itself leader of the failover's term."""
    winner, _, first = _failover_open(trace)
    other = next((n for n in trace.nodes.values() if n.node != winner.node), None)
    if other is None:
        raise DriftNotApplicable("no second node")
    seq = other.last_seq + 1
    claim = RaftStep(other.node, seq, first.at_ms, term=first.term, leader=other.node)
    return _with(trace, replace(other, last_seq=seq, raft=(*other.raft, claim)))


def drift_term_regresses(trace: Trace, bounds: Bounds) -> Trace:
    """A node records a term below one it already recorded."""
    node = next((n for n in trace.nodes.values() if n.raft and n.raft[-1].term > 0), None)
    if node is None:
        raise DriftNotApplicable("no node recorded a term")
    last = node.raft[-1]
    seq = node.last_seq + 1
    back = RaftStep(node.node, seq, last.at_ms, term=last.term - 1, leader=None)
    return _with(trace, replace(node, last_seq=seq, raft=(*node.raft, back)))


def _first_writable(trace: Trace) -> tuple[NodeTrace, WriteStep]:
    for node in trace.nodes.values():
        for step in node.writes:
            if step.writable:
                return node, step
    raise DriftNotApplicable("no writable primary was recorded")


def drift_writable_without_lease_leader(trace: Trace, bounds: Bounds) -> Trace:
    """A writable primary whose lease does not believe it leads."""
    node, step = _first_writable(trace)
    writes = tuple(replace(w, lease_leader=False) if w == step else w for w in node.writes)
    return _with(trace, replace(node, writes=writes))


def drift_fence_did_not_land(trace: Trace, bounds: Bounds) -> Trace:
    """A tick that should have fenced leaves the primary writable."""
    for node in trace.nodes.values():
        for prev, step in zip(node.writes, node.writes[1:], strict=False):
            if prev.writable and not step.lease_valid and step.writable is False:
                writes = tuple(replace(w, writable=True) if w == step else w for w in node.writes)
                return _with(trace, replace(node, writes=writes))
    raise DriftNotApplicable("no fence closed a writable primary")


def drift_leader_without_quorum(trace: Trace, bounds: Bounds) -> Trace:
    """The failover's winner leads with every grant it got withdrawn."""
    winner, _, first = _failover_open(trace)
    nodes = dict(trace.nodes)
    for node in trace.nodes.values():
        votes = tuple(
            replace(v, granted=False) if v.candidate == winner.node and v.term == first.term else v
            for v in node.votes
        )
        nodes[node.node] = replace(node, votes=votes)
    return Trace(nodes)


def drift_writes_open_outside_gate(trace: Trace, bounds: Bounds) -> Trace:
    """A primary turns writable with no open recorded: the gate was bypassed."""
    node, open_, _ = _failover_open(trace)
    spent = AnchorStep(node.node, open_.seq, open_.at_ms, stamped=False)
    anchors = tuple(sorted((*node.anchors, spent), key=lambda a: a.seq))
    opens = tuple(o for o in node.opens if o != open_)
    return _with(trace, replace(node, opens=opens, anchors=anchors))


@dataclass(frozen=True)
class Drift:
    name: str
    trips: Rule
    apply: Callable[[Trace, Bounds], Trace]


DRIFTS: Final[tuple[Drift, ...]] = (
    Drift("open inside the hold-down", HOLD_DOWN_FROM_TERM, drift_open_inside_hold_down),
    Drift("deposed leader never fenced", ONE_WRITE_AUTHORITY, drift_deposed_never_fenced),
    Drift("a voter backs two candidates", ONE_VOTE_PER_TERM, drift_double_vote),
    Drift("a vote past the WAL gate", VOTE_LSN_ACCEPTABLE, drift_vote_past_wal_gate),
    Drift("two leaders in one term", ONE_LEADER_PER_TERM, drift_two_leaders),
    Drift("a term goes backwards", TERM_MONOTONIC, drift_term_regresses),
    Drift(
        "writable without the lease leading",
        WRITABLE_IMPLIES_LEASE_LEADER,
        drift_writable_without_lease_leader,
    ),
    Drift("a fence that did not land", FENCE_LANDS, drift_fence_did_not_land),
    Drift("a leader without a quorum", LEADER_HAS_QUORUM, drift_leader_without_quorum),
    Drift("writes opened outside the gate", WRITABLE_VIA_OPEN, drift_writes_open_outside_gate),
)
