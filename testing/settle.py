"""A bounded, loud barrier: one agreed leader, the full voter set, sync in force.

`up -d` followed by "a leader exists" races the joins. The joining nodes are
still learners mid-basebackup, the leader counts one voter, and no standby is
`sync` yet. A phase started there fails for a reason the product did not cause
(`doctor --strict` refusing on `replication:sync`), or passes against a smaller
cluster than the one it claims to test ("blue has 1 voters").

Settled means all three at once, read from the cluster rather than assumed:

- every node's `/api/v1/cluster/leader` names the same leader;
- that leader's `/api/v1/cluster/members` lists exactly the expected voters and
  no learners;
- that leader's write-gate evidence meets the requirement, by the same rule as
  `doctor`'s `replication:sync`: zero required passes, otherwise confirmed
  standbys must reach the required count.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final

import httpx

import api_models
from fault_primitives import parse_prometheus_metric
from topology import Node
from waiting import WaitTimeout, await_until

SYNC_CONFIRMED_METRIC: Final[str] = "pgbattery_sync_standbys_confirmed"
"""Standbys PostgreSQL reports `sync`, from the leader's lease-tick probe."""

SYNC_REQUIRED_METRIC: Final[str] = "pgbattery_sync_standbys_required"
"""Sync standbys the leader's voter set requires."""

VOTER_ROLE: Final[str] = "voter"
LEARNER_ROLE: Final[str] = "learner"

DEFAULT_BUDGET_SEC: Final[float] = 240.0
"""Covers a join's basebackup plus its learner-to-voter promotion, which polls
lag for up to two minutes before giving up."""

PROBE_TIMEOUT_SEC: Final[float] = 3.0


class ClusterNotSettled(RuntimeError):
    """The cluster did not settle within the budget. Carries what it said."""


@dataclass(frozen=True)
class SettleObservation:
    """One round of what the cluster reports about itself."""

    expected_voters: frozenset[int]
    leader_views: Mapping[int, int | None]
    """Observing node id to the leader it names. `None` when it named none or
    did not answer: an unreachable node is an absence of evidence."""
    members: api_models.Members | None
    """The agreed leader's membership; `None` when there was no agreed leader
    or it did not answer."""
    sync_confirmed: float | None
    sync_required: float | None

    @property
    def agreed_leader(self) -> int | None:
        named = set(self.leader_views.values())
        if len(named) != 1:
            return None
        (leader,) = named
        return leader


def sync_problems(confirmed: float | None, required: float | None) -> list[str]:
    """`doctor`'s `replication:sync` rule, as reasons the leader fails it."""
    if required == 0:
        return []
    if confirmed is None or required is None:
        return [
            f"leader publishes no {SYNC_CONFIRMED_METRIC}/{SYNC_REQUIRED_METRIC} "
            f"(confirmed={confirmed}, required={required})"
        ]
    if confirmed < required:
        return [f"{confirmed:g} of {required:g} required standbys report sync"]
    return []


def membership_problems(
    members: api_models.Members | None, expected_voters: frozenset[int]
) -> list[str]:
    """Reasons the leader's membership is not exactly the expected voter set."""
    if members is None:
        return ["no membership read from an agreed leader"]
    voters = {m.node_id for m in members.members if m.role == VOTER_ROLE}
    learners = {m.node_id for m in members.members if m.role == LEARNER_ROLE}
    unknown = sorted(
        (m.node_id, m.role) for m in members.members if m.role not in (VOTER_ROLE, LEARNER_ROLE)
    )
    problems: list[str] = []
    if voters != expected_voters:
        problems.append(f"voters {sorted(voters)}, expected exactly {sorted(expected_voters)}")
    if learners:
        problems.append(f"learners {sorted(learners)} have not been promoted")
    if unknown:
        problems.append(f"members with unknown roles {unknown}")
    return problems


def settle_problems(observation: SettleObservation) -> list[str]:
    """Every reason `observation` is not settled; empty means settled."""
    problems: list[str] = []
    leader = observation.agreed_leader
    if leader is None:
        problems.append(
            f"nodes do not agree on one leader: {dict(sorted(observation.leader_views.items()))}"
        )
    elif leader not in observation.expected_voters:
        problems.append(
            f"agreed leader {leader} is not an expected voter {sorted(observation.expected_voters)}"
        )
    problems.extend(membership_problems(observation.members, observation.expected_voters))
    problems.extend(sync_problems(observation.sync_confirmed, observation.sync_required))
    return problems


def _get(url: str) -> str | None:
    try:
        response = httpx.get(url, timeout=PROBE_TIMEOUT_SEC)
    except httpx.HTTPError:
        return None
    return response.text if response.status_code == 200 else None


def _leader_view(node: Node) -> int | None:
    body = _get(f"http://127.0.0.1:{node.mgmt_port}/api/v1/cluster/leader")
    info = None if body is None else api_models.parse_or_none(api_models.LeaderInfo, body)
    return None if info is None else info.leader_id


def observe(nodes: Sequence[Node]) -> SettleObservation:
    """Read one round from the cluster: every node's leader, then the leader's facts."""
    views = {n.node_id: _leader_view(n) for n in nodes}
    expected = frozenset(n.node_id for n in nodes if n.is_voter)
    named = set(views.values())
    by_id = {n.node_id: n for n in nodes}
    members: api_models.Members | None = None
    confirmed: float | None = None
    required: float | None = None
    if len(named) == 1 and (leader_id := next(iter(named))) in by_id:
        leader = by_id[leader_id]
        body = _get(f"http://127.0.0.1:{leader.mgmt_port}/api/v1/cluster/members")
        members = None if body is None else api_models.parse_or_none(api_models.Members, body)
        metrics = _get(f"http://127.0.0.1:{leader.metrics_port}/metrics")
        if metrics is not None:
            confirmed = parse_prometheus_metric(metrics, SYNC_CONFIRMED_METRIC)
            required = parse_prometheus_metric(metrics, SYNC_REQUIRED_METRIC)
    return SettleObservation(
        expected_voters=expected,
        leader_views=views,
        members=members,
        sync_confirmed=confirmed,
        sync_required=required,
    )


def describe(observation: SettleObservation | None, nodes: Sequence[Node]) -> str:
    """What the cluster said, by service name, for a failure message."""
    if observation is None:
        return "nothing was observed"
    services = {n.node_id: n.service for n in nodes}
    views = {services.get(k, str(k)): v for k, v in sorted(observation.leader_views.items())}
    members = (
        "unread"
        if observation.members is None
        else [(m.node_id, m.role) for m in observation.members.members]
    )
    return (
        f"problems={settle_problems(observation)}; leader views={views}; "
        f"leader membership={members}; sync confirmed={observation.sync_confirmed} "
        f"required={observation.sync_required}"
    )


def await_settled(
    nodes: Sequence[Node],
    *,
    budget: float = DEFAULT_BUDGET_SEC,
    interval: float = 1.0,
    observe_round: Callable[[Sequence[Node]], SettleObservation] = observe,
) -> Node:
    """Block until `nodes` form one settled cluster and return its leader.

    Raises `ClusterNotSettled` with the last observation when the budget runs
    out. `nodes` is one cluster: pass each cluster of a blue/green pair alone.
    """
    if not any(n.is_voter for n in nodes):
        raise ClusterNotSettled(
            f"no voters among {[n.service for n in nodes]}; settling them proves nothing"
        )
    by_id = {n.node_id: n for n in nodes}
    last: SettleObservation | None = None

    def settled() -> Node | None:
        nonlocal last
        last = observe_round(nodes)
        if settle_problems(last):
            return None
        leader = last.agreed_leader
        return None if leader is None else by_id.get(leader)

    try:
        return await_until(
            settled,
            budget=budget,
            interval=interval,
            describe=f"{[n.service for n in nodes]} to settle",
        )
    except WaitTimeout as exc:
        raise ClusterNotSettled(f"{exc}: {describe(last, nodes)}") from exc
