"""Typed models for the management API, parsed once at the boundary.

Every harness used to reach into `json.loads(...)` results with `.get()` and
`isinstance` checks. That is a parse spread across call sites: a field that
changes shape surfaces as a `None` somewhere downstream instead of an error
where the body was read, and the checkers then reason about a dict nobody
validated.

These models are the boundary. Parse here, raise here, and everything after
holds a typed object.
"""

from __future__ import annotations

import json
from typing import TypeVar

from pydantic import BaseModel, ValidationError

ModelT = TypeVar("ModelT", bound=BaseModel)


class ApiParseError(RuntimeError):
    """A response body did not match the shape the API promises."""


class LeaderInfo(BaseModel):
    """`GET /api/v1/cluster/leader`."""

    leader_id: int | None = None
    leader_addr: str | None = None
    leader_pg_addr: str | None = None
    leader_mgmt_addr: str | None = None


class Member(BaseModel):
    """One entry of `GET /api/v1/cluster/members`."""

    node_id: int
    addr: str
    role: str


class Members(BaseModel):
    """`GET /api/v1/cluster/members`."""

    success: bool
    message: str = ""
    members: list[Member] = []

    @property
    def node_ids(self) -> set[int]:
        return {m.node_id for m in self.members}


class ClusterIdentity(BaseModel):
    """`GET /api/v1/cluster/identity`.

    `cluster_lineage` is the PostgreSQL system identifier of this node's data
    directory. Two nodes share it exactly when they share a data history, so a
    node that re-provisioned correctly reports the leader's, and one that
    initdb'd a new lineage reports its own.
    """

    node_id: int
    cluster_lineage: int | None = None


class DebugState(BaseModel):
    """`GET /debug/state`."""

    node_id: int
    leader_id: int | None = None
    is_leader: bool = False
    voters: list[int] = []
    learners: list[int] = []
    node_count: int = 0
    failover_anchor_age_ms: int | None = None


class TxidStatus(BaseModel):
    """`GET /api/v1/cluster/txid-status/{txid}`.

    `status` is `None` for a transaction the server will not answer for — a
    txid from the future, or one too old to be in the commit log. That is an
    honest unknown and callers must not read it as either outcome.
    """

    txid: int
    status: str | None = None


def parse(model: type[ModelT], body: str) -> ModelT:
    """Parse `body` into `model`, or raise `ApiParseError`.

    One place where a malformed body becomes an error, so no caller has to ask
    what shape it got.
    """
    try:
        return model.model_validate(json.loads(body))
    except (json.JSONDecodeError, ValidationError) as exc:
        raise ApiParseError(f"not a valid {model.__name__}: {exc}") from exc


def parse_or_none(model: type[ModelT], body: str) -> ModelT | None:
    """`parse`, but `None` when the body could not be read.

    For pollers, where an unreachable node and a malformed body mean the same
    thing to the caller: nothing was observed this round.
    """
    try:
        return parse(model, body)
    except ApiParseError:
        return None
