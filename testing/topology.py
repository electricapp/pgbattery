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

    def by_service(self, service: str) -> Node:
        for node in self.nodes:
            if node.service == service:
                return node
        raise TopologyError(
            f"{service!r} is not in {self.compose_file.name}; "
            f"known services: {[n.service for n in self.nodes]}"
        )


def _published_ports(service: str, spec: dict[str, object]) -> dict[int, int]:
    """Map container port to host port for one service.

    Handles both compose spellings. The short form is `"HOST:CONTAINER"`; the
    long form is a mapping with `published` and `target`. A form this does not
    understand raises rather than silently yielding no ports, which would leave
    every derived port lookup failing later and further from the cause.
    """
    published: dict[int, int] = {}
    for entry in spec.get("ports", []) or []:
        if isinstance(entry, str):
            parts = entry.split(":")
            if len(parts) < 2:
                raise TopologyError(f"{service}: cannot read port mapping {entry!r}")
            host, container = parts[-2], parts[-1]
            published[int(container.split("/")[0])] = int(host)
        elif isinstance(entry, dict):
            published[int(entry["target"])] = int(entry["published"])
        else:
            raise TopologyError(f"{service}: unrecognised port entry {entry!r}")
    return published


def _config_path(service: str, spec: dict[str, object]) -> Path:
    """The config file this service mounts as its `pgbattery.toml`."""
    for volume in spec.get("volumes", []) or []:
        if isinstance(volume, str) and CONFIG_MOUNT in volume:
            return REPO_ROOT / volume.split(":", 1)[0]
    raise TopologyError(f"{service}: no {CONFIG_MOUNT} mount, so it has no declared identity")


def _node_id(service: str, spec: dict[str, object]) -> int:
    config = _config_path(service, spec)
    if not config.exists():
        raise TopologyError(f"{service}: mounts {config}, which does not exist")
    parsed = tomllib.loads(config.read_text(encoding="utf-8"))
    node_id = parsed.get("node_id")
    if not isinstance(node_id, int):
        raise TopologyError(f"{service}: {config.name} declares no integer node_id")
    return node_id


def _command_text(spec: dict[str, object]) -> str:
    command = spec.get("command")
    if isinstance(command, str):
        return command
    if isinstance(command, list):
        return " ".join(str(part) for part in command)
    return ""


def _is_voter(spec: dict[str, object]) -> bool:
    command = _command_text(spec)
    return "--bootstrap" in command or "--voter" in command


def _static_ip(service: str, spec: dict[str, object], network: str) -> str:
    networks = spec.get("networks")
    if not isinstance(networks, dict):
        raise TopologyError(f"{service}: no network assignment; expected a static address")
    attachment = networks.get(network)
    if not isinstance(attachment, dict) or "ipv4_address" not in attachment:
        raise TopologyError(
            f"{service}: no static ipv4_address on {network}. Peer-level faults "
            f"address nodes by IP, and a DHCP address would move between runs."
        )
    return str(attachment["ipv4_address"])


def load(compose_file: Path | None = None) -> Topology:
    """Derive the topology from a compose file."""
    path = compose_file or active_compose_file()
    if not path.exists():
        raise TopologyError(f"{path} not found; nothing declares the cluster topology")

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = document.get("services") or {}
    networks = list((document.get("networks") or {}).keys())
    if len(networks) != 1:
        raise TopologyError(f"{path.name} declares {len(networks)} networks; expected exactly one")
    network = networks[0]

    nodes: list[Node] = []
    for service, spec in services.items():
        if not isinstance(spec, dict):
            continue
        ports = _published_ports(service, spec)
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
                node_id=_node_id(service, spec),
                ip=_static_ip(service, spec, network),
                gateway_port=ports[PG_CONTAINER_PORT],
                metrics_port=ports[METRICS_CONTAINER_PORT],
                mgmt_port=ports[MGMT_CONTAINER_PORT],
                is_voter=_is_voter(spec),
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
NODE_IPS: Final[dict[str, str]] = {n.service: n.ip for n in TOPOLOGY.nodes}
GATEWAY_PORTS: Final[tuple[int, ...]] = tuple(n.gateway_port for n in TOPOLOGY.voters)
MGMT_PORTS: Final[dict[str, int]] = {n.service: n.mgmt_port for n in TOPOLOGY.nodes}
METRICS_PORTS: Final[dict[str, int]] = {n.service: n.metrics_port for n in TOPOLOGY.nodes}
