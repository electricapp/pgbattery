"""Cluster topology, read from the compose file that defines it.

Service names, static `raft_net` addresses, and published host ports were
written out independently in `fault_primitives.py`, `linreg/cluster.py`,
`correctness_lite.py`, and `dual_writability_prober.py` — four copies of a fact
that `docker-compose.yml` already states, and that a fifth file (the 5-node
compose) states differently. A harness addressing a docker object that is not
there is the failure mode this repo has been bitten by repeatedly: nothing
raises, the fault lands nowhere, and the run reads as coverage.

So nothing declares the topology. It is derived from the compose file, which is
what actually creates the containers, and a mismatch is impossible rather than
linted. Reading it costs a few milliseconds at import.

Which file is read follows `COMPOSE_FILE`, the same variable `docker compose`
itself honours, so a 5-node run gets the 5-node topology without a flag. Only
the first entry is read: these harnesses drive one cluster definition, and an
override file would name the same services.

Anything malformed raises at import. There is no fallback to a hardcoded
default — that would be the duplication this module exists to remove, and it
would be reached exactly when the real answer had become unavailable.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import yaml
from pydantic import BaseModel, ValidationError, field_validator, model_validator

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent

DEFAULT_COMPOSE_FILE: Final[str] = "docker-compose.yml"

CONFIG_MOUNT: Final[str] = ":/app/pgbattery.toml:"
"""The volume that gives a service its identity. `node_id` is read from the
config pgbattery itself reads, not from the service name: `witness` has no
digit in its name and is node 4, and a service renamed without its mount
changing would otherwise be silently misidentified."""

# Container-side ports, from the image's own configuration. These are the
# other half of every published mapping, and are how a mapping is identified.
PG_CONTAINER_PORT: Final[int] = 5432
METRICS_CONTAINER_PORT: Final[int] = 9090
MGMT_CONTAINER_PORT: Final[int] = 9091

PG_INTERNAL_PORT: Final[int] = 5434
"""Where `PostgreSQL` itself listens inside the container, behind the gateway
on :data:`PG_CONTAINER_PORT`. A harness reaches this to ask one node about its
own state, rather than whichever node the gateway is routing to."""

RAFT_CONTAINER_PORT: Final[int] = 5433
"""Raft consensus traffic between peers, container-side."""


class TopologyError(RuntimeError):
    """The compose file does not describe a cluster this harness can drive."""


@dataclass(frozen=True)
class Node:
    """One service in the compose deployment."""

    service: str
    """Compose service name — what `docker compose exec` takes. Never a
    container name: those carry the project prefix, which CI overrides per
    run."""

    node_id: int
    """Raft node id, from the service name. `node2` is 2."""

    ip: str
    """Static address on the cluster network, for peer-level faults."""

    gateway_port: int
    """Host port proxying to whichever node is leader."""

    metrics_port: int
    """Host port serving Prometheus. The truth source for per-node lease
    state: `pgbattery_has_lease` cannot lie about a node other than itself,
    while the management API serves the last leader it heard about."""

    mgmt_port: int
    """Host port serving the management API."""

    is_voter: bool
    """Whether this node joins the Raft voter set, from the command that starts
    it: `--bootstrap` or `--voter`. A witness joins as a learner and must not be
    counted when a test computes a quorum — counting learners as voters made
    the first 5-node run's quorum assertions vacuous."""

    is_bootstrap: bool
    """Whether this node starts with `--bootstrap`, which means "initdb a new
    cluster if there is no data here". Wiping such a node does not exercise a
    rejoin: it mints a fresh PostgreSQL lineage under an id the cluster still
    lists — see H-16 in HARDENING.md — so a harness testing joins must target a
    node that actually joins."""


@dataclass(frozen=True)
class Topology:
    """Every service in the deployment, in node-id order."""

    nodes: tuple[Node, ...]
    network: str
    compose_file: Path

    @property
    def voters(self) -> tuple[Node, ...]:
        return tuple(n for n in self.nodes if n.is_voter)

    @property
    def voter_services(self) -> tuple[str, ...]:
        return tuple(n.service for n in self.voters)

    @property
    def joining_services(self) -> tuple[str, ...]:
        """Voters that reach the cluster by joining it.

        The target set for anything that wipes a node and expects it back: the
        bootstrap node answers an empty state directory with `initdb`, not with
        a join.
        """
        return tuple(n.service for n in self.voters if not n.is_bootstrap)

    def by_service(self, service: str) -> Node:
        for node in self.nodes:
            if node.service == service:
                return node
        raise TopologyError(
            f"{service!r} is not in {self.compose_file.name}; "
            f"known services: {[n.service for n in self.nodes]}"
        )


class PortMapping(BaseModel):
    """One published port.

    Compose's short `"HOST:CONTAINER"` spelling is normalised into the long
    form as it is read, so nothing downstream has to ask which spelling the
    file used.
    """

    target: int
    published: int

    @model_validator(mode="before")
    @classmethod
    def _accept_short_form(cls, value: object) -> object:
        match value:
            case str():
                parts = value.split(":")
                if len(parts) < 2:
                    raise ValueError(f"cannot read port mapping {value!r}")
                return {"published": parts[-2], "target": parts[-1].split("/")[0]}
            case _:
                return value


class NetworkAttachment(BaseModel):
    """A service's attachment to one network.

    `ipv4_address` is required: peer-level faults address nodes by IP, and a
    DHCP address would move between runs.
    """

    ipv4_address: str


class BuildSpec(BaseModel):
    """A service's build block.

    Compose's short form is a bare context path, which names no stage — the
    case `lint_matrix` exists to catch, so it is normalised rather than
    rejected.
    """

    context: str = "."
    target: str = ""

    @model_validator(mode="before")
    @classmethod
    def _accept_short_form(cls, value: object) -> object:
        match value:
            case str():
                return {"context": value}
            case _:
                return value


class ComposeService(BaseModel):
    """One service, in the fields this module derives topology from."""

    ports: list[PortMapping] = []
    volumes: list[str] = []
    networks: dict[str, NetworkAttachment] = {}
    command: str = ""
    build: BuildSpec | None = None

    @field_validator("command", mode="before")
    @classmethod
    def _join_argv(cls, value: object) -> object:
        match value:
            case list():
                return " ".join(str(part) for part in value)
            case _:
                return value

    @property
    def published(self) -> dict[int, int]:
        """Container port to host port."""
        return {p.target: p.published for p in self.ports}

    @property
    def is_voter(self) -> bool:
        return self.is_bootstrap or "--voter" in self.command

    @property
    def is_bootstrap(self) -> bool:
        return "--bootstrap" in self.command

    def config_path(self, service: str) -> Path:
        """The config file this service mounts as its `pgbattery.toml`."""
        for volume in self.volumes:
            if CONFIG_MOUNT in volume:
                return REPO_ROOT / volume.split(":", 1)[0]
        raise TopologyError(f"{service}: no {CONFIG_MOUNT} mount, so it has no declared identity")

    def node_id(self, service: str) -> int:
        config = self.config_path(service)
        if not config.exists():
            raise TopologyError(f"{service}: mounts {config}, which does not exist")
        try:
            return NodeConfig.model_validate(
                tomllib.loads(config.read_text(encoding="utf-8"))
            ).node_id
        except ValidationError as exc:
            raise TopologyError(
                f"{service}: {config.name} declares no integer node_id: {exc}"
            ) from exc

    def static_ip(self, service: str, network: str) -> str:
        attachment = self.networks.get(network)
        if attachment is None:
            raise TopologyError(
                f"{service}: no static ipv4_address on {network}. Peer-level faults "
                f"address nodes by IP, and a DHCP address would move between runs."
            )
        return attachment.ipv4_address


class NodeConfig(BaseModel):
    """The one field topology needs from a node's `pgbattery.toml`.

    Strict, so a `node_id` written as a string is a config error here rather
    than a coerced integer nothing ever questions.
    """

    model_config = {"strict": True, "extra": "ignore"}

    node_id: int


class ComposeDocument(BaseModel):
    """The compose file itself."""

    services: dict[str, ComposeService] = {}
    networks: dict[str, object] = {}


def load(compose_file: Path | None = None) -> Topology:
    """Derive the topology from a compose file."""
    path = compose_file or active_compose_file()
    if not path.exists():
        raise TopologyError(f"{path} not found; nothing declares the cluster topology")

    try:
        document = ComposeDocument.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    except ValidationError as exc:
        raise TopologyError(
            f"{path.name} is not a compose file this harness can read: {exc}"
        ) from exc

    networks = list(document.networks)
    if len(networks) != 1:
        raise TopologyError(f"{path.name} declares {len(networks)} networks; expected exactly one")
    network = networks[0]

    nodes: list[Node] = []
    for service, spec in document.services.items():
        ports = spec.published
        missing = {
            name: port
            for name, port in (
                ("gateway", PG_CONTAINER_PORT),
                ("metrics", METRICS_CONTAINER_PORT),
                ("management", MGMT_CONTAINER_PORT),
            )
            if port not in ports
        }
        if missing:
            # Not a pgbattery node: a workload container, a sidecar. Skip it
            # rather than fail, but only when it publishes none of them —
            # a node missing one mapping is a broken deployment.
            if len(missing) == 3:
                continue
            raise TopologyError(
                f"{service} publishes some pgbattery ports but not {sorted(missing)}; "
                f"the harness would address a port that is not there"
            )
        nodes.append(
            Node(
                service=service,
                node_id=spec.node_id(service),
                ip=spec.static_ip(service, network),
                gateway_port=ports[PG_CONTAINER_PORT],
                metrics_port=ports[METRICS_CONTAINER_PORT],
                mgmt_port=ports[MGMT_CONTAINER_PORT],
                is_voter=spec.is_voter,
                is_bootstrap=spec.is_bootstrap,
            )
        )

    if not nodes:
        raise TopologyError(
            f"{path.name} declares no pgbattery nodes. Every derived name would be "
            f"empty and every loop over them would pass without doing anything."
        )
    return Topology(
        nodes=tuple(sorted(nodes, key=lambda n: n.node_id)),
        network=network,
        compose_file=path,
    )


def active_compose_file() -> Path:
    """The compose file `docker compose` would read, as this harness sees it."""
    raw = os.environ.get("COMPOSE_FILE", "").strip()
    if not raw:
        return REPO_ROOT / DEFAULT_COMPOSE_FILE
    separator = os.environ.get("COMPOSE_PATH_SEPARATOR", os.pathsep)
    first = raw.split(separator)[0].strip()
    candidate = Path(first)
    return candidate if candidate.is_absolute() else REPO_ROOT / candidate


TOPOLOGY: Final[Topology] = load()
"""The cluster this process is driving."""

NODES: Final[tuple[str, ...]] = TOPOLOGY.voter_services
JOINING_NODES: Final[tuple[str, ...]] = TOPOLOGY.joining_services
NODE_IPS: Final[dict[str, str]] = {n.service: n.ip for n in TOPOLOGY.nodes}
GATEWAY_PORTS: Final[tuple[int, ...]] = tuple(n.gateway_port for n in TOPOLOGY.voters)
GATEWAY_PORT_BY_NODE: Final[dict[str, int]] = {n.service: n.gateway_port for n in TOPOLOGY.voters}
"""The same ports keyed by service, so a harness that has a node name does not
have to know its position in `NODES` to reach it. Two harnesses derived this
mapping themselves before it lived here."""
MGMT_PORTS: Final[dict[str, int]] = {n.service: n.mgmt_port for n in TOPOLOGY.nodes}
METRICS_PORTS: Final[dict[str, int]] = {n.service: n.metrics_port for n in TOPOLOGY.nodes}
