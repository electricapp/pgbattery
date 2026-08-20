#!/usr/bin/env -S uv run --project testing python
"""Fault-injection primitives — the shared fault vocabulary for pgbattery harnesses.

Every primitive here does three things, in this order, and none of them are
optional:

  1. **Inject** the fault.
  2. **Verify the effect landed**, positively and from outside: an iptables/tc
     rule that is present *and* counting packets, a process actually in state
     ``T``, a qdisc that reports the delay/loss we asked for, a clock offset
     observable via ``date`` in the container, a filesystem that genuinely
     cannot allocate another WAL segment. If the effect cannot be observed the
     primitive raises :class:`FaultEffectNotObserved`.
  3. **Heal idempotently and verify the removal.**

The reason step 2 exists is `disk_full`: the pre-existing attack allocated a
500 MB filler inside an *unbounded* named volume, so on any host with GBs free
it could never produce ENOSPC. It passed while testing nothing, which is worse
than not existing because it reads as coverage. A fault that cannot fail is
worse than no test — so a primitive that cannot verify its own effect raises a
loud, actionable error instead of proceeding.

═══════════════════════════════════════════════════════════════════════════
WHAT EACH PRIMITIVE PROVES
═══════════════════════════════════════════════════════════════════════════

``partition_lossy(container, drop_pct, latency_ms)``
    PROVES: Raft heartbeat / lease-renewal behaviour on a link that is
    degraded rather than cut. Some RPCs arrive late, some never — this is the
    regime that exposes resend, idempotency and duplicate-handling bugs that a
    clean disconnect cannot, and it is the regime that decides whether a leader
    with a slow-but-alive link gets deposed.
    DOES NOT PROVE: anything about asymmetric reachability (use
    ``partition_asymmetric``); netem is applied to egress only, so the peer's
    view of *its own* send path is unchanged.

``partition_asymmetric(from_container, to_container, direction)``
    PROVES: behaviour when reachability is one-directional — the classic
    "leader still believes it holds quorum while followers see no leader"
    split. Packets in one direction are destroyed while packets in the other
    direction are delivered, and both halves of that claim are verified with
    tc packet counters.
    DOES NOT PROVE: that a *connection* survives in the surviving direction.
    Destroying one direction breaks request/response both ways at the
    application layer, because responses carry the blocked address as their
    source. The asymmetry is at the packet layer, which is where Raft's
    reachability assumptions live.

``fsync_stall(container, duration_s)``
    PROVES: behaviour when a node cannot advance its durable WAL position while
    its process supervisor, Raft loop and lease tick all keep running — the
    "PG is alive but cannot make progress durable" branch. On a sync standby it
    additionally blocks the leader's commit path, because commits wait for the
    standby's ack; that is the closest signal-only analogue of a hung fsync().
    DOES NOT PROVE: that a hung ``fsync()`` *syscall* is handled. Commit-time
    fsyncs happen inside the backend process, which is deliberately left
    running (stopping backends would hang the client, not the durability path).
    No kernel or disk-controller path is exercised, fsync error (EIO) handling
    is untested, and this cannot produce the "fsync returned success without
    flushing" durability violation. :func:`crash_losing_unsynced_writes` can,
    and does — run it from ``durability_crash.py`` against
    ``docker-compose.lazyfs.yml``.

``clock_skew_at_lease_boundary(container, skew_ms, window_ms)``
    PROVES: whether any decision that a lease guards is actually taken on the
    monotonic clock. The audited hazard is concrete: ``App::promote_local_postgres``
    gates promotion behind ``promotion_lease_holddown(failover_started_ms,
    unix_now_ms(), lease_ms)`` (``src/app.rs:1051``, ``src/app.rs:2341``), which
    compares *wall clock* — ``unix_now_ms()`` is ``SystemTime::now()``
    (``src/app.rs:2354``), and ``LD_PRELOAD``ed libfaketime intercepts it. The
    lease it stands in for is monotonic (``LeaseState`` reads
    ``pgbattery_core::Clock``, ``src/governor/lease.rs:106``). So a forward step
    of at least one lease duration, applied to the election winner *inside* the
    hold-down window, releases the hold-down while the deposed leader's lease is
    still valid — two writable primaries. This primitive aims the skew at that
    live window instead of at an arbitrary offset.
    DOES NOT PROVE: anything about skew between *nodes* at rest (that is the
    existing ``clock_skew`` step), and nothing at all if the skew lands outside
    the window — which is why landing outside it raises rather than passes.

``sigstop_checkpointer(container, duration_s)``
    PROVES: the "PG accepts writes but cannot checkpoint" branch. Checkpoints
    stop, ``pg_wal`` grows without recycling, and the lease tick must classify
    the node as alive-but-unhealthy rather than dead.
    DOES NOT PROVE: that commits stall. With ``synchronous_commit=on`` a backend
    fsyncs WAL itself, so commits keep completing with the checkpointer stopped.
    Use ``fsync_stall`` for the durability-path stall.

``disk_full_during_wal(container)``
    PROVES: the ENOSPC class specifically at WAL segment allocation. The
    filesystem holding ``pg_wal`` is filled until strictly less than one WAL
    segment of space remains, and the primitive proves it by *failing* to
    allocate one more segment. PG can still write catalog/stat pages; what it
    cannot do is roll to the next WAL file.
    DOES NOT PROVE: anything unless the node state volume is size-bounded.
    Requires ``PGBATTERY_STATE_SUFFIX=_bounded`` (see ``docker-compose.yml``);
    against the unbounded default it refuses to run rather than fill a host
    disk.

═══════════════════════════════════════════════════════════════════════════
DERIVED TIMINGS, NOT HAND-TUNED SLEEPS
═══════════════════════════════════════════════════════════════════════════

Fault durations that need to straddle a system boundary read that boundary from
the Rust source instead of hard-coding it: :func:`read_system_timings` parses
``src/config/constants.rs`` and ``src/governor/lease.rs`` (and lets
``config/node1.toml`` override the Raft knobs it pins, because that is what the
running cluster uses). :func:`sweep_around` turns a boundary into a set of
durations either side of it, so a caller sweeps 0.5x / 0.9x / 1.0x / 1.1x / 2.0x
of the lease rather than guessing. A rename in the Rust source makes
:func:`read_system_timings` raise, so the harness cannot silently keep using a
stale constant.

═══════════════════════════════════════════════════════════════════════════
SUBSTRATE FACTS THIS MODULE RELIES ON
═══════════════════════════════════════════════════════════════════════════

  - **``iptables`` is not installed in the image.** The Dockerfile installs
    ``tini libfaketime iproute2 procps curl`` on top of ``postgres:18``; there
    is no iptables binary. Directional drops here are built from ``tc``, which
    is present: an ``ingress`` qdisc plus a ``u32``/``gact`` filter is the exact
    equivalent of ``iptables -I INPUT -s <ip> -j DROP``, and a ``prio`` band fed
    by ``netem loss 100%`` is the egress equivalent.
  - **``tc`` needs ``--user root``.** ``cap_add: [NET_ADMIN]`` grants the
    capability to the container, but the image runs as ``postgres``; a non-root
    exec has no effective NET_ADMIN.
  - **libfaketime offsets must be written in fractional seconds.** ``+250ms`` is
    parsed as *250 minutes* (``m`` is the minute suffix, the trailing ``s`` is
    ignored). :func:`faketime_offset_literal` only ever emits seconds with a
    decimal fraction, and refuses to emit ``ms``.
  - **``ping`` is not installed either.** Reachability probes use ``curl``
    against a peer's management port: exit 7 means the SYN reached a closed or
    refused socket, exit 28 means it was blackholed.

═══════════════════════════════════════════════════════════════════════════
RELATIONSHIP TO THE OTHER TWO FAULT VOCABULARIES
═══════════════════════════════════════════════════════════════════════════

Three fault vocabularies exist in this tree and they have drifted apart:

  - ``ci_runner.py``'s ``StepType`` enum — 24 step types, of which
    ``asymmetric_partition`` / ``asymmetric_heal`` / ``clock_skew`` /
    ``clock_heal`` / ``network_delay`` / ``network_heal`` are faults.
  - ``linearizability_register.py``'s ``ATTACK_DISPATCH`` — 16 live attacks plus
    2 scaffolds (``fsync_drop``, ``bit_flip``).
  - this module.

They overlap but do not agree: the CI runner's ``network_delay`` has delay and
jitter but no loss at all, its fault steps exec without ``--user root``, both of
the others build asymmetric partitions from an ``iptables`` binary the image
does not contain, and ``ATTACK_DISPATCH``'s cleanup never SIGCONTs a process
that a crashed test left stopped. This module is the vocabulary intended to be
shared; :func:`scrub` deliberately cleans up residue from all three (including
the legacy ``_chaos_fill.bin`` filler path) so nothing leaks between runs.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
import tomllib
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Final

import pydantic
import typer
from rich.console import Console

import api_models
import topology

# ─────────────────────────────────────────────────────────────────────────────
# Topology & substrate constants
# ─────────────────────────────────────────────────────────────────────────────

# Derived from the compose file that creates the containers, never declared
# here. Four modules used to carry their own copy of this, and a harness that
# addresses a docker object which is not there injects nothing while reading as
# coverage — the failure mode this repo has been bitten by repeatedly. See
# `topology.py`.
NODES: Final[tuple[str, ...]] = topology.NODES
"""Voter services, in Raft node-id order. Learners (the witness) are excluded:
counting one as a voter makes every quorum assertion vacuous."""

JOINING_NODES: Final[tuple[str, ...]] = topology.JOINING_NODES
"""Voters that reach the cluster by joining it — the only valid targets for a
wipe-and-rejoin. Wiping the bootstrap node makes it `initdb` a new lineage
under an id the cluster still lists — see H-16 in HARDENING.md."""

NODE_IPS: Final[dict[str, str]] = topology.NODE_IPS
"""Static addresses on the cluster network, for peer-level faults."""

MGMT_PORTS: Final[dict[str, int]] = topology.MGMT_PORTS
"""Host-published management API ports."""

METRICS_PORTS: Final[dict[str, int]] = topology.METRICS_PORTS
"""Host-published Prometheus ports. These, not the management API, are the truth
source for lease state: ``GET /api/v1/cluster/leader`` serves the last leader it
knows about, so after a leader dies its peers keep naming it for the whole
failover (measured: 2.5 s of ``leader_id: 3`` while node3 was down and
unreachable). ``pgbattery_has_lease`` is per-node and cannot lie about a node
other than itself."""

MGMT_INTERNAL_PORT: Final[int] = 9091

RAFT_PORT: Final[int] = 5433
"""Raft consensus traffic between peers."""

GATEWAY_PORT: Final[int] = 5432
"""Client-facing gateway port."""
"""In-container management API port — the probe target for reachability."""

LEASE_METRIC: Final[str] = "pgbattery_has_lease"
"""1 iff that node currently holds valid write authority."""

RAFT_LEADER_METRIC: Final[str] = "pgbattery_raft_is_leader"
"""1 iff that node holds Raft leadership, which it can do before its PG is
promoted — that gap is the promotion hold-down window."""

PROMOTION_HOLDDOWN_METRIC: Final[str] = "pgbattery_promotion_lease_holddowns"
"""Counter incremented every reconcile pass that refuses to promote because the
deposed leader's lease may still be valid. A rise in this counter is direct
evidence that ``promotion_lease_holddown`` engaged."""

NET_DEVICE: Final[str] = "eth0"
"""The container-side interface on ``raft_net``."""

PG_DATA_DIR: Final[str] = "/var/lib/postgresql/data"
PG_STATE_DIR: Final[str] = "/var/lib/postgresql"
PG_INTERNAL_PORT: Final[int] = 5434

FAKETIME_FILE: Final[str] = "/tmp/faketime"
"""Read on every clock call because the image sets ``FAKETIME_NO_CACHE=1``."""


@dataclass(frozen=True)
class LazyfsMount:
    """One LazyFS instance: where it mounts, what backs it, how it is driven.

    These four paths are a set, not four independent settings. A harness that
    took the FIFO of one instance and the backing root of another would arm a
    fault on one filesystem and then look for its effect on the other, and
    find nothing -- which reads as a fault that did not fire. Keeping them in
    one object means a caller names an instance rather than assembling one.
    """

    name: str
    mount_dir: str
    root_dir: str
    fifo: str
    log: str
    config: str

    def holds(self, path: str) -> bool:
        """Whether `path` is a backing-store path belonging to this instance."""
        return path.startswith(f"{self.root_dir}/")


LAZYFS_DATA: Final[LazyfsMount] = LazyfsMount(
    name="pgdata",
    mount_dir=PG_DATA_DIR,
    root_dir=f"{PG_STATE_DIR}/pgdata-root",
    fifo="/tmp/lazyfs.fifo",
    log="/tmp/lazyfs.log",
    config="/etc/lazyfs.toml",
)
"""PGDATA. Matches ``testing/lazyfs/lazyfs.toml``, baked in at
``/etc/lazyfs.toml`` by the ``runtime-lazyfs`` image stage. The FIFO and log
sit outside the mount on purpose: a control channel inside the filesystem being
crashed vanishes exactly when it is needed."""

LAZYFS_RAFT: Final[LazyfsMount] = LazyfsMount(
    name="raft",
    mount_dir=f"{PG_STATE_DIR}/raft",
    root_dir=f"{PG_STATE_DIR}/raft-root",
    fifo="/tmp/lazyfs-raft.fifo",
    log="/tmp/lazyfs-raft.log",
    config="/etc/lazyfs-raft.toml",
)
"""The Raft store, holding redb's ``raft.db``. A separate instance from
PGDATA's so a fault aimed at one cannot crash the filesystem holding the other,
which is what makes it possible to say which store the damage was aimed at.
pgbattery derives this directory as a sibling of ``pg_data_dir``."""

LAZYFS_FIFO: Final[str] = LAZYFS_DATA.fifo
LAZYFS_LOG: Final[str] = LAZYFS_DATA.log
LAZYFS_ROOT_DIR: Final[str] = LAZYFS_DATA.root_dir
"""PGDATA's paths, named directly for the callers that only ever mean PGDATA.
The durability suite is the whole of that set: it crashes the node, so which
instance it addresses is not a choice it makes."""

RAFT_DB_FILE: Final[str] = "raft.db"
"""redb's database, in ``LAZYFS_RAFT``. Named in ``src/app.rs``."""

FILLER_PATH: Final[str] = f"{PG_STATE_DIR}/_fault_fill.bin"
"""Disk-fill target. Deliberately beside PGDATA rather than inside it: same
filesystem, so ENOSPC is identical, without leaving an unexpected file in the
data directory for PG to complain about."""

LEGACY_FILLER_PATH: Final[str] = f"{PG_DATA_DIR}/_chaos_fill.bin"
"""``linearizability_register.py``'s filler. Scrubbed here too."""

WAL_DURABILITY_PROCESSES: Final[tuple[str, ...]] = (
    "checkpointer",
    "walwriter",
    "background writer",
    "walreceiver",
)
"""Auxiliary processes on the path from "WAL written" to "WAL durable and
replayed". ``walreceiver`` exists only on a standby."""

REQUIRED_WAL_PROCESSES: Final[tuple[str, ...]] = ("checkpointer",)
"""Present on every healthy postmaster, primary or standby."""

REQUIRED_WAL_PATH_PROCESSES: Final[tuple[str, ...]] = ("walwriter", "walreceiver")
"""At least one must be present, and which one says what the node is: a primary
has ``walwriter`` and no ``walreceiver``, a standby in recovery has
``walreceiver`` and no ``walwriter``. Requiring both would make
:func:`fsync_stall` refuse to run on every follower."""

LAZYFS_CONFIG_RELOAD_TIMEOUT_S: Final[float] = 90.0
"""How long to wait for a restarted LazyFS to reprint its config.

Long enough for a node to come back with two mounts on a loaded runner, and
short enough that a container which failed to restart is reported as such
rather than waited on until the suite's own budget runs out."""

DEFAULT_TIMEOUT_S: Final[float] = 15.0

MAX_BOUNDED_FS_BYTES: Final[int] = 8 * 1024**3
"""A node state filesystem larger than this is treated as unbounded, and
:func:`disk_full_during_wal` refuses to fill it. The bounded compose variant
defaults to 4 GiB; this leaves room for a hand-raised
``PGBATTERY_BOUNDED_STATE_SIZE`` while still rejecting a host disk."""

MIN_VERIFIABLE_LOSS_PCT: Final[float] = 10.0
"""Below this, a short probe can legitimately drop zero packets, so the netem
drop counter is not usable as proof."""

MIN_VERIFIABLE_LATENCY_MS: Final[int] = 25
"""Below this, added latency is inside the noise of a container-to-container
HTTP probe and cannot be distinguished from scheduling jitter."""

CURL_EXIT_TIMEOUT: Final[int] = 28
CURL_EXIT_CONNECT_REFUSED: Final[int] = 7

_BOUNDED_STATE_REMEDIATION: Final[str] = (
    "The node state volume is not size-bounded, so a fill can never reach\n"
    "ENOSPC and this primitive would pass while testing nothing.\n"
    "  Recreate the cluster on the bounded volume variant:\n"
    "    docker compose down\n"
    "    PGBATTERY_STATE_SUFFIX=_bounded docker compose up -d\n"
    "  Optionally size it with PGBATTERY_BOUNDED_STATE_SIZE (default 4g).\n"
    "  Bounded volumes are tmpfs and therefore NOT restart-persistent — see\n"
    "  the volumes: block in docker-compose.yml before using them in a case\n"
    "  that restarts a node."
)


# ─────────────────────────────────────────────────────────────────────────────
# Errors
# ─────────────────────────────────────────────────────────────────────────────


class FaultError(Exception):
    """Base class for every failure raised by this module."""


class FaultPreconditionError(FaultError):
    """The substrate cannot support the fault, so nothing was injected.

    Carries the exact remediation, because the alternative to a fault that
    cannot be injected is a test that passes vacuously.
    """


class ContainerNotRunning(FaultPreconditionError):
    """The container held no process to exec in, so nothing was observed.

    Not the same as observing an absence. A caller waiting for the substrate to
    settle retries this; one that required a running container fails on it.
    """


class FaultInjectionError(FaultError):
    """The injection command itself failed."""


class FaultEffectNotObserved(FaultError):
    """Injection reported success but the effect could not be observed.

    Always a hard failure: an unobservable fault proves nothing.
    """


# ─────────────────────────────────────────────────────────────────────────────
# Command execution — indirected so tests can drive the pure logic
# ─────────────────────────────────────────────────────────────────────────────


TIMED_OUT_RC: Final[int] = -1
"""Return code for a command the runner killed at its deadline. No real exit
status is negative, so this cannot collide with one the command produced."""


@dataclass(frozen=True)
class CommandResult:
    """Outcome of one shell command. ``rc == TIMED_OUT_RC`` means it timed out."""

    rc: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.rc == 0

    @property
    def output(self) -> str:
        """stdout with stderr appended — tc and ps write diagnostics to both."""
        return self.stdout if not self.stderr else f"{self.stdout}{self.stderr}"


CommandRunner = Callable[[str, float], CommandResult]


def _subprocess_runner(cmd: str, timeout_s: float) -> CommandResult:
    try:
        proc = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return CommandResult(TIMED_OUT_RC, "", f"timeout after {timeout_s}s")
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


_runner: CommandRunner = _subprocess_runner


def set_command_runner(runner: CommandRunner) -> CommandRunner:
    """Swap the shell executor, returning the previous one.

    The only reason this exists is testability: the parsers and verifiers are
    pure, and swapping the runner lets a unit test drive a whole primitive
    against recorded ``tc`` / ``ps`` / ``df`` output with no docker.
    """
    global _runner
    previous = _runner
    _runner = runner
    return previous


def run(cmd: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> CommandResult:
    """Execute `cmd` through the current runner."""
    return _runner(cmd, timeout_s)


def compose_exec(container: str, script: str, *, as_root: bool = True) -> str:
    """Build a ``docker compose exec`` command running `script` under ``sh -c``.

    `as_root` is the default because ``tc`` and ``kill`` need the container's
    NET_ADMIN / signal privileges, which the image's ``postgres`` user does not
    have even though the capability is granted to the container.
    """
    user = "--user root " if as_root else ""
    escaped = script.replace("\\", "\\\\").replace('"', '\\"')
    return f'docker compose exec -T {user}{container} sh -c "{escaped}"'


def exec_in(
    container: str,
    script: str,
    *,
    as_root: bool = True,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> CommandResult:
    """Run `script` inside `container`."""
    return run(compose_exec(container, script, as_root=as_root), timeout_s)


# ─────────────────────────────────────────────────────────────────────────────
# Compose object naming
# ─────────────────────────────────────────────────────────────────────────────
#
# Every primitive above reaches containers as compose *services*, which compose
# resolves inside whatever project is active. Some faults cannot be expressed
# that way — detaching a container from a network needs the real container and
# network names — and those are the ones that silently no-op when a literal name
# is wrong. Resolve them here and nowhere else.

DEFAULT_COMPOSE_PROJECT: Final[str] = "pgbattery"
"""What compose uses when the environment does not override it, from
``docker-compose.yml``'s own ``name:``."""

RAFT_NETWORK_SUFFIX: Final[str] = "raft_net"
"""Compose prefixes network names with the project, so the full name is
``<project>_raft_net``."""

CLUSTER_SUBNET_PREFIX: Final[str] = "172.28."
"""Cluster subnet from ``docker-compose.yml``. Attachment is confirmed by an
address in this range rather than by a command's exit code."""


def compose_project() -> str:
    """Active compose project name.

    ``COMPOSE_PROJECT_NAME`` overrides the compose file's ``name:``, and every CI
    workflow but one sets a per-run value. A literal ``pgbattery_raft_net``
    therefore does not exist in CI, which is what turned two partition attacks
    into no-ops that still reported PASS.
    """
    return os.environ.get("COMPOSE_PROJECT_NAME") or DEFAULT_COMPOSE_PROJECT


def cluster_network(suffix: str = RAFT_NETWORK_SUFFIX) -> str:
    """Full name of a compose-created network in the active project."""
    return f"{compose_project()}_{suffix}"


def container_networks_cmd(container: str) -> str:
    """Build a command listing ``network=ip`` pairs for `container`."""
    fmt = "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}={{$v.IPAddress}} {{end}}"
    return f"docker inspect -f '{fmt}' {container}"


def parse_container_networks(text: str) -> dict[str, str]:
    """Parse `container_networks_cmd` output into network name → IP.

    A network attached with no address yet maps to the empty string, which is
    deliberately distinct from being absent.
    """
    out: dict[str, str] = {}
    for token in text.split():
        name, _, ip = token.partition("=")
        if name:
            out[name] = ip
    return out


def container_id(service: str) -> str:
    """Resolve a compose service to its container id, running or not.

    Asks compose rather than string-building a name, so a change to its naming
    convention surfaces as a precondition failure instead of an unresolvable
    name that the fault then shrugs off.

    `-a` is load-bearing. Without it compose reports nothing for a container
    that is not running, which is the state every healing verb is called in:
    `start_container` after `kill_container` could never resolve its own
    target. It also restores the already-dead guard in `kill_container` — with
    the state unreadable that check was skipped, so the one thing it exists to
    catch (killing a container that was already down, a silent no-op fault)
    reached it as an unresolvable name instead.
    """
    result = run(f"docker compose ps -aq {service}")
    cid = result.stdout.strip().split("\n")[-1].strip() if result.ok else ""
    if not cid:
        raise FaultPreconditionError(
            f"cannot resolve a container for compose service {service!r} in "
            f"project {compose_project()!r}: "
            f"{result.stderr.strip() or 'no container id returned'}. "
            "Check the cluster is up and COMPOSE_PROJECT_NAME matches it."
        )
    return cid


@dataclass(frozen=True)
class ContainerRunState:
    """Container liveness identity, enough to prove an incarnation changed.

    `started_at` is what distinguishes a restart from a no-op: a container that
    was never killed keeps its timestamp, so comparing the triple before and
    after is the difference between observing a fault and assuming one.
    """

    status: str
    started_at: str
    restart_count: int


def container_runstate_cmd(container: str) -> str:
    """Inspect the fields `ContainerRunState` carries, space-separated."""
    fmt = "{{.State.Status}} {{.State.StartedAt}} {{.RestartCount}}"
    return f'docker inspect --format "{fmt}" {container}'


def parse_container_runstate(text: str) -> ContainerRunState | None:
    """Parse `container_runstate_cmd` output; None if nothing usable.

    None covers a missing container, a docker error, and empty output alike:
    all three mean the state could not be read, which callers must not confuse
    with a container that is simply stopped.
    """
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 3 or not parts[2].isdigit():
            continue
        return ContainerRunState(status=parts[0], started_at=parts[1], restart_count=int(parts[2]))
    return None


def read_container_runstate(service: str) -> ContainerRunState | None:
    """Current `ContainerRunState` for `service`, or None if unreadable."""
    return parse_container_runstate(run(container_runstate_cmd(container_id(service))).stdout)


def verify_incarnation_changed(
    before: ContainerRunState | None,
    after: ContainerRunState | None,
    *,
    target: str,
    action: str,
) -> None:
    """Assert `action` actually replaced the container's incarnation.

    A `docker kill` against an already-dead container exits 0 and changes
    nothing, which is precisely the silent no-op that reads as coverage.
    """
    if after is None:
        raise FaultEffectNotObserved(f"{action}: cannot read {target} state afterwards")
    if before is not None and before.started_at == after.started_at:
        raise FaultEffectNotObserved(
            f"{action}: {target} still on the same incarnation "
            f"(started_at={after.started_at}); the container was not replaced"
        )


def verify_status(state: ContainerRunState | None, *, target: str, expected: str) -> None:
    """Assert the container reports `expected` docker status."""
    if state is None:
        raise FaultEffectNotObserved(f"cannot read {target} state; expected {expected!r}")
    if state.status != expected:
        raise FaultEffectNotObserved(f"{target} is {state.status!r}, expected {expected!r}")


def read_container_networks(service: str) -> dict[str, str]:
    """Network name → IP for `service`'s container."""
    result = run(container_networks_cmd(container_id(service)))
    if not result.ok:
        raise FaultInjectionError(
            f"could not inspect networks for {service}: {result.stderr.strip()}"
        )
    return parse_container_networks(result.stdout)


STALL_SIGNATURES: Final[tuple[str, ...]] = (
    "contains unexpected zero page",
    "is not empty",
    "Raft DB corrupted",
    "Failed to create database",
    "PANIC:",
    "FATAL:",
    "Shutting down after",
    "database system is shut down",
)
"""Log lines that explain why a node is not a serving member of the cluster.

A harness that gives up waiting reports what it was waiting for and almost never
why it never arrived, so a node shut down by the fence threshold over a catalog
it cannot open reads exactly like a slow start. Both shapes seen in CI are here:
the corrupt index (`unexpected zero page`) and the join that met a populated
data directory (`is not empty`)."""


def stall_reason(log_text: str) -> str | None:
    """The last line in `log_text` that explains why a node is not serving.

    Last rather than first: a node that restart-loops repeats its complaint, and
    the most recent one is the state it is in now. None when nothing matches,
    which is itself worth reporting — the node is stalled for a reason no
    harness has seen before.
    """
    for line in reversed(log_text.splitlines()):
        if any(signature in line for signature in STALL_SIGNATURES):
            return line.strip()
    return None


def node_stall_report(service: str) -> str:
    """One line on why `service` is not serving, for a harness that gave up.

    Diagnostic only, and best-effort: a node that cannot be inspected is
    reported as such rather than raising, because this runs on a path that is
    already failing and must not replace one error with another.
    """
    try:
        state = read_container_runstate(service)
        where = (
            f"status={state.status} restarts={state.restart_count}"
            if state is not None
            else "container state unreadable"
        )
        logs = run(f"docker compose logs --no-color --tail 2000 {service}")
        reason = stall_reason(logs.output) if logs.ok else None
    except Exception as exc:
        return f"{service}: could not be inspected ({exc})"
    return f"{service}: {where}; {reason or 'no known stall signature in its log'}"


def cluster_stall_report(services: Sequence[str]) -> str:
    """`node_stall_report` for each of `services`, joined for one message."""
    return "; ".join(node_stall_report(service) for service in services) or "no nodes to inspect"


def verify_detached(networks: dict[str, str], *, target: str, network: str) -> None:
    """Assert `network` is gone. A disconnect that no-ops leaves an empty fault
    window, and an empty window reads as "no violations during the partition"."""
    if network in networks:
        raise FaultEffectNotObserved(
            f"{target} is still attached to {network} after disconnect "
            f"(attached: {sorted(networks)})"
        )


def verify_attached(networks: dict[str, str], *, target: str, network: str) -> None:
    """Assert `network` is back, with a cluster address. Leaving a node detached
    poisons every later case in the run."""
    ip = networks.get(network)
    if ip is None:
        raise FaultEffectNotObserved(
            f"{target} was not reattached to {network} (attached: {sorted(networks)})"
        )
    if not ip.startswith(CLUSTER_SUBNET_PREFIX):
        raise FaultEffectNotObserved(
            f"{target} reattached to {network} with address {ip!r}, which is "
            f"outside the cluster subnet {CLUSTER_SUBNET_PREFIX}x"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Structured fault-window logging (invariant 4: observable from outside)
# ─────────────────────────────────────────────────────────────────────────────

EventSink = Callable[[dict[str, object]], None]


def _stderr_sink(event: dict[str, object]) -> None:
    sys.stderr.write(json.dumps(event, sort_keys=True) + "\n")
    sys.stderr.flush()


_sink: EventSink = _stderr_sink


def set_event_sink(sink: EventSink) -> EventSink:
    """Redirect fault-window events, returning the previous sink.

    Schema is stable: ``ts`` (unix seconds), ``event`` (``fault.open`` /
    ``fault.close`` / ``fault.scrub``), ``primitive``, ``target``, and a
    primitive-specific ``detail`` mapping. A downstream trace correlator can
    splice fault windows into an operation history on these alone.
    """
    global _sink
    previous = _sink
    _sink = sink
    return previous


def _emit(event: str, primitive: str, target: str, detail: dict[str, object]) -> None:
    _sink({"ts": time.time(), "event": event, "primitive": primitive, "target": target, **detail})


# ─────────────────────────────────────────────────────────────────────────────
# System timings, derived from the Rust source
# ─────────────────────────────────────────────────────────────────────────────


class PinnedTimings(pydantic.BaseModel):
    """The timing keys a node config may override.

    Everything else in the file is ignored on purpose — this model exists to
    give the two values that matter a declared type at the point they are read.
    """

    model_config = pydantic.ConfigDict(extra="ignore")

    election_timeout_ms: int | None = None
    heartbeat_interval_ms: int | None = None


@dataclass(frozen=True)
class SystemTimings:
    """The pgbattery timing constants a fault duration may need to straddle.

    Read from the Rust source by :func:`read_system_timings` rather than
    duplicated here, so a constant that is renamed or retuned breaks the
    harness loudly instead of leaving it sweeping the wrong boundary.
    """

    lease_duration_ms: int
    lease_check_interval_ms: int
    election_timeout_ms: int
    heartbeat_interval_ms: int
    quorum_timeout_ms: int
    metrics_watchdog_timeout_ms: int
    lsn_staleness_threshold_ms: int
    leadership_transfer_lease_safety_ms: int
    slot_ensure_interval_ms: int
    election_timeout_source: str
    """``constants.rs`` or the config file that overrides it."""

    @property
    def promotion_holddown_ms(self) -> int:
        """How long ``promote_local_postgres`` withholds promotion after the
        locally-observed leaderless edge. Exactly one lease duration — that is
        the window ``clock_skew_at_lease_boundary`` aims at."""
        return self.lease_duration_ms

    @property
    def worst_case_election_ms(self) -> int:
        """openraft picks its election timer from ``[timeout, 2 * timeout]``."""
        return 2 * self.election_timeout_ms

    @property
    def max_stale_write_window_ms(self) -> int:
        """Upper bound on a deposed leader's write authority: the lease, which
        :meth:`LeaseState::renew` anchors on the quorum ack rather than on now,
        so quorum staleness does not stack on top of it."""
        return self.lease_duration_ms

    def as_dict(self) -> dict[str, object]:
        return {
            "lease_duration_ms": self.lease_duration_ms,
            "lease_check_interval_ms": self.lease_check_interval_ms,
            "election_timeout_ms": self.election_timeout_ms,
            "election_timeout_source": self.election_timeout_source,
            "heartbeat_interval_ms": self.heartbeat_interval_ms,
            "quorum_timeout_ms": self.quorum_timeout_ms,
            "metrics_watchdog_timeout_ms": self.metrics_watchdog_timeout_ms,
            "lsn_staleness_threshold_ms": self.lsn_staleness_threshold_ms,
            "leadership_transfer_lease_safety_ms": self.leadership_transfer_lease_safety_ms,
            "promotion_holddown_ms": self.promotion_holddown_ms,
            "worst_case_election_ms": self.worst_case_election_ms,
        }


def _repo_root(explicit: Path | None = None) -> Path:
    return explicit if explicit is not None else Path(__file__).resolve().parent.parent


def parse_rust_u64_const(source: str, name: str) -> int:
    """Extract ``pub const <name>: u64 = <int>;``, tolerating ``1_000``."""
    match = re.search(rf"const\s+{re.escape(name)}\s*:\s*u64\s*=\s*([0-9_]+)", source)
    if match is None:
        raise FaultPreconditionError(
            f"Rust constant {name} not found. The harness derives fault timings from "
            "the Rust source; a renamed constant must be fixed here rather than "
            "silently replaced by a stale default."
        )
    return int(match.group(1).replace("_", ""))


def parse_rust_str_const(source: str, name: str) -> str:
    """Extract ``const <name>: &'static str = "<value>";``."""
    match = re.search(rf"const\s+{re.escape(name)}\s*:\s*&'static\s+str\s*=\s*\"([^\"]*)\"", source)
    if match is None:
        raise FaultPreconditionError(
            f"Rust string constant {name} not found. A harness that minted names by "
            "its own spelling would exercise a name the cluster never owns."
        )
    return match.group(1)


def replication_slot_prefix(repo_root: Path | None = None) -> str:
    """The prefix of every slot name this cluster mints.

    Read from `ReplicationSlot::PREFIX`, because a slot named anything else is
    a foreign slot the reconciler deliberately never touches — a harness that
    spelled it itself would be asserting against a name nothing owns.
    """
    path = _repo_root(repo_root) / "crates" / "pgbattery-core" / "src" / "types.rs"
    if not path.is_file():
        raise FaultPreconditionError(f"{path} not found — cannot derive the slot name format.")
    return parse_rust_str_const(path.read_text(encoding="utf-8"), "PREFIX")


def parse_rust_duration_const_ms(source: str, name: str) -> int:
    """Extract ``pub const <name>: Duration = Duration::from_{secs,millis}(N);``."""
    match = re.search(
        rf"const\s+{re.escape(name)}\s*:\s*Duration\s*=\s*"
        rf"Duration::from_(secs|millis)\(\s*([0-9_]+)\s*\)",
        source,
    )
    if match is None:
        raise FaultPreconditionError(
            f"Rust Duration constant {name} not found. The harness derives fault "
            "timings from the Rust source; a renamed constant must be fixed here "
            "rather than silently replaced by a stale default."
        )
    value = int(match.group(2).replace("_", ""))
    return value * 1000 if match.group(1) == "secs" else value


def read_system_timings(repo_root: Path | None = None) -> SystemTimings:
    """Read the live timing constants from the Rust source and node config.

    ``src/governor/lease.rs`` owns the lease; ``src/config/constants.rs`` owns
    the Raft and watchdog defaults. ``config/node1.toml`` pins
    ``election_timeout_ms`` / ``heartbeat_interval_ms`` for the compose cluster,
    and those are what actually run, so they win when present.
    """
    root = _repo_root(repo_root)
    constants_path = root / "src" / "config" / "constants.rs"
    lease_path = root / "src" / "governor" / "lease.rs"
    for path in (constants_path, lease_path):
        if not path.is_file():
            raise FaultPreconditionError(
                f"{path} not found — pass repo_root= pointing at the pgbattery checkout."
            )
    constants = constants_path.read_text(encoding="utf-8")
    lease = lease_path.read_text(encoding="utf-8")

    election_ms = parse_rust_u64_const(constants, "DEFAULT_ELECTION_TIMEOUT_MS")
    heartbeat_ms = parse_rust_u64_const(constants, "DEFAULT_HEARTBEAT_INTERVAL_MS")
    election_source = "src/config/constants.rs"

    node_config = root / "config" / "node1.toml"
    if node_config.is_file():
        # Parsed into a model, so a key that changes type is an error here
        # rather than a silently ignored override that leaves every sweep
        # straddling the wrong boundary.
        pinned = PinnedTimings.model_validate(
            tomllib.loads(node_config.read_text(encoding="utf-8"))
        )
        if pinned.election_timeout_ms is not None:
            election_ms = pinned.election_timeout_ms
            election_source = "config/node1.toml"
        if pinned.heartbeat_interval_ms is not None:
            heartbeat_ms = pinned.heartbeat_interval_ms

    return SystemTimings(
        lease_duration_ms=parse_rust_duration_const_ms(lease, "DEFAULT_LEASE_DURATION"),
        lease_check_interval_ms=parse_rust_duration_const_ms(lease, "LEASE_CHECK_INTERVAL"),
        election_timeout_ms=election_ms,
        heartbeat_interval_ms=heartbeat_ms,
        quorum_timeout_ms=parse_rust_u64_const(constants, "QUORUM_TIMEOUT_MS"),
        metrics_watchdog_timeout_ms=parse_rust_u64_const(constants, "METRICS_WATCHDOG_TIMEOUT_MS"),
        lsn_staleness_threshold_ms=parse_rust_u64_const(constants, "LSN_STALENESS_THRESHOLD_SECS")
        * 1000,
        leadership_transfer_lease_safety_ms=parse_rust_u64_const(
            constants, "LEADERSHIP_TRANSFER_LEASE_SAFETY_MS"
        ),
        slot_ensure_interval_ms=parse_rust_u64_const(
            constants, "REPLICATION_SLOT_ENSURE_INTERVAL_SECS"
        )
        * 1_000,
        election_timeout_source=election_source,
    )


DEFAULT_SWEEP_FACTORS: Final[tuple[float, ...]] = (0.5, 0.9, 1.0, 1.1, 2.0)
"""Straddles a boundary: two values that should be tolerated, the boundary
itself, and two that should trip it."""


def sweep_around(
    boundary_ms: int,
    factors: Sequence[float] = DEFAULT_SWEEP_FACTORS,
) -> list[int]:
    """Fault durations either side of `boundary_ms`, deduped and sorted.

    Callers sweep a system boundary — lease duration, election timeout, quorum
    timeout — instead of hand-tuning a magic number that silently stops
    straddling anything the moment the constant is retuned.
    """
    if boundary_ms <= 0:
        raise ValueError(f"boundary_ms must be positive, got {boundary_ms}")
    return sorted({max(1, round(boundary_ms * factor)) for factor in factors})


# ─────────────────────────────────────────────────────────────────────────────
# Pure command construction
# ─────────────────────────────────────────────────────────────────────────────


def ip_to_u32_hex(ip: str) -> str:
    """Render a dotted-quad as the 8 hex digits ``tc filter show`` prints.

    ``tc`` echoes u32 matches as ``match ac1c000c/ffffffff at 12``, so this is
    how a rule is located in the filter dump.
    """
    octets = ip.split(".")
    if len(octets) != 4:
        raise ValueError(f"not an IPv4 dotted-quad: {ip!r}")
    values = []
    for octet in octets:
        if not octet.isdigit() or not 0 <= int(octet) <= 255:
            raise ValueError(f"not an IPv4 dotted-quad: {ip!r}")
        values.append(int(octet))
    return "".join(f"{value:02x}" for value in values)


def _last_octet(ip: str) -> int:
    return int(ip.rsplit(".", maxsplit=1)[1])


def drop_prio_for(ip: str) -> int:
    """Filter priority reserved for the DROP rule against `ip`.

    Derived from the address so repeated installs are idempotent in placement
    and a heal can delete exactly its own rule instead of flushing the chain.
    """
    return 100 + _last_octet(ip)


def count_prio_for(ip: str) -> int:
    """Filter priority reserved for the pass-through COUNTING rule for `ip`."""
    return 200 + _last_octet(ip)


def netem_add_cmd(
    *,
    delay_ms: int,
    jitter_ms: int,
    loss_pct: float,
    dev: str = NET_DEVICE,
) -> str:
    """``tc netem`` root qdisc combining latency, jitter and packet loss.

    Deliberately not prefixed with a ``qdisc del``: if a root qdisc is already
    installed, the add fails with "Exclusivity flag on" and the caller learns
    that another primitive owns the interface, instead of silently clobbering
    it.
    """
    parts = [f"tc qdisc add dev {dev} root netem"]
    if delay_ms > 0:
        parts.append(f"delay {delay_ms}ms")
        if jitter_ms > 0:
            parts.append(f"{jitter_ms}ms")
    if loss_pct > 0:
        parts.append(f"loss {loss_pct:g}%")
    if len(parts) == 1:
        raise ValueError("netem needs at least one of delay_ms / loss_pct")
    return " ".join(parts)


_ROOT_QDISC_LOCKS: Final[dict[tuple[str, str], threading.Lock]] = {}
_ROOT_QDISC_REGISTRY_LOCK: Final[threading.Lock] = threading.Lock()

ROOT_QDISC_WAIT_S: Final[float] = 60.0
"""How long a netem fault waits for the device. Longer than any window the
attacks hold (the longest is ~10 s), so queuing behind a real one succeeds and
only a nested or leaked window reaches the bound."""


def root_qdisc_lock(container: str, dev: str = NET_DEVICE) -> threading.Lock:
    """The lock that stands in for tc's one-root-qdisc-per-device rule.

    tc permits one root qdisc per device, so two netem faults on one container
    cannot overlap — the second `add` fails with "Exclusivity flag on". A
    concurrent caller (`chaos_storm` runs each fault in its own thread) waits
    for the device instead, which is the only faithful reading of "both at
    once" when the device says no. Keyed per (container, dev), so faults on
    different nodes still overlap.
    """
    key = (container, dev)
    with _ROOT_QDISC_REGISTRY_LOCK:
        return _ROOT_QDISC_LOCKS.setdefault(key, threading.Lock())


def netem_del_cmd(dev: str = NET_DEVICE) -> str:
    return f"tc qdisc del dev {dev} root"


def ingress_qdisc_add_cmd(dev: str = NET_DEVICE) -> str:
    return f"tc qdisc add dev {dev} handle ffff: ingress"


def ingress_qdisc_del_cmd(dev: str = NET_DEVICE) -> str:
    return f"tc qdisc del dev {dev} ingress"


def ingress_filter_add_cmd(
    src_ip: str,
    *,
    prio: int,
    action: str,
    dev: str = NET_DEVICE,
) -> str:
    """Ingress ``u32`` filter on source address.

    ``action drop`` is the exact equivalent of ``iptables -I INPUT -s <ip> -j
    DROP``; ``action pass`` leaves delivery alone and only counts, which is how
    the surviving direction of an asymmetric partition is proven.
    """
    if action not in {"drop", "pass"}:
        raise ValueError(f"action must be drop or pass, got {action!r}")
    return (
        f"tc filter add dev {dev} parent ffff: protocol ip prio {prio} "
        f"u32 match ip src {src_ip}/32 action {action}"
    )


def ingress_filter_del_cmd(*, prio: int, dev: str = NET_DEVICE) -> str:
    return f"tc filter del dev {dev} parent ffff: protocol ip prio {prio} u32"


def egress_drop_cmds(dst_ip: str, *, dev: str = NET_DEVICE) -> list[str]:
    """Egress-side directional drop: ``prio`` band 1:1 fed by 100 % netem loss.

    A plain ``action drop`` on egress needs the ``act_gact`` classifier action
    on the root qdisc path; routing the matched flow into a ``netem loss 100%``
    band reuses the same scheduler the other primitives already depend on and
    gives a drop counter for free.
    """
    return [
        f"tc qdisc add dev {dev} root handle 1: prio bands 3",
        f"tc qdisc add dev {dev} parent 1:1 handle 10: netem loss 100%",
        f"tc filter add dev {dev} protocol ip parent 1: prio 1 "
        f"u32 match ip dst {dst_ip}/32 flowid 1:1",
    ]


class Channel(StrEnum):
    """A destination port class, so a partition can sever one protocol.

    Every existing partition is per-peer-IP, which takes down all five ports at
    once. The interesting failures are the gray splits: Raft healthy while
    streaming replication is dead (the leader keeps its lease and its quorum but
    cannot ship WAL), or the inverse (replication flowing while consensus is
    blind).
    """

    RAFT = "raft"
    """Either side works: peers exchange RPCs continuously in both directions."""

    REPLICATION = "replication"
    """Install on the **standby**, with write load in flight.

    Measured on a live cluster: the leader streams and the standby only answers
    every `wal_receiver_status_interval` (10 s by default), so a rule on the
    leader matches nothing for many seconds. And an idle cluster generates no
    WAL at all — the same rule on the standby saw 0 packets idle and 23 under
    load. Both failures look identical to a fault that did not land, which is
    the point: do not reach for `require_traffic=False` to silence them."""

    GATEWAY = "gateway"
    """Needs client traffic in flight; there is no steady background chatter."""

    MANAGEMENT = "management"
    """Polled by the harness rather than by peers, so also traffic-dependent."""

    @property
    def port(self) -> int:
        return {
            Channel.RAFT: RAFT_PORT,
            Channel.REPLICATION: PG_INTERNAL_PORT,
            Channel.GATEWAY: GATEWAY_PORT,
            Channel.MANAGEMENT: MGMT_INTERNAL_PORT,
        }[self]


_CHANNEL_SIDE_HINT: Final[dict[Channel, str]] = {
    Channel.REPLICATION: "Install this on the standby, with write load in flight: "
    "the leader streams (so the standby sees the traffic), and an idle cluster "
    "generates no WAL to stream at all.",
    Channel.GATEWAY: "This channel is idle without client traffic; run a workload "
    "or pass require_traffic=False.",
    Channel.MANAGEMENT: "This channel is idle unless the harness is polling it; "
    "pass require_traffic=False.",
}


def iptables_port_drop_cmd(peer_ip: str, port: int, *, insert: bool, from_listener: bool) -> str:
    """Add or remove an INPUT DROP for `peer_ip` traffic on `port`.

    `from_listener` selects `--sport` instead of `--dport`. Both are needed
    because only one side of a TCP channel listens on the service port: requests
    arrive at the listener with `--dport P`, replies arrive at the initiator with
    `--sport P` and an ephemeral destination. Matching one direction alone
    silently catches nothing whenever the traffic happens to flow the other way.
    """
    action = "-I" if insert else "-D"
    match = f"--sport {port}" if from_listener else f"--dport {port}"
    return f"iptables {action} INPUT -p tcp -s {peer_ip} {match} -j DROP"


def channel_side_hint(channel: Channel) -> str:
    """Why this channel might match zero packets, if there is a known reason.

    Shared with `ci_runner` so the matrix steps fail with the same guidance the
    context-manager primitive gives, rather than a second copy that drifts.
    """
    return _CHANNEL_SIDE_HINT.get(channel, "")


def iptables_peer_drop_cmd(peer_ip: str, *, insert: bool, chain: str = "INPUT") -> str:
    """Add or remove a DROP for every packet from `peer_ip`, any port.

    The whole-peer counterpart to `iptables_port_drop_cmd`. Because it matches
    on source alone it needs no `--sport`/`--dport` pair: one rule already
    covers both directions of every channel.
    """
    action = "-A" if insert else "-D"
    return f"iptables {action} {chain} -s {peer_ip} -j DROP"


def parse_peer_drop_rule(text: str, peer_ip: str, *, chain: str = "INPUT") -> bool:
    """Whether `iptables -S` output carries a whole-peer DROP for `peer_ip`.

    Tolerates the ``/32`` iptables appends when printing rules back, and the
    counter dump appended by `iptables_rules_cmd`.
    """
    pattern = re.compile(
        rf"^-A\s+{re.escape(chain)}\s+.*-s\s+{re.escape(peer_ip)}(?:/\d+)?\s+.*-j\s+DROP\b"
    )
    return any(pattern.match(line.strip()) for line in text.splitlines())


def iptables_rules_cmd() -> str:
    """Dump INPUT rules plus their packet counters in one exec."""
    return "iptables -S INPUT; echo '--- counters ---'; iptables -L INPUT -n -v"


def parse_port_drop_rule(text: str, peer_ip: str, port: int, *, from_listener: bool) -> bool:
    """Whether `iptables -S` output carries the DROP for `peer_ip` on `port`.

    Tolerates the ``/32`` iptables appends when printing rules back, and the
    counter dump appended by `iptables_rules_cmd`.
    """
    direction = "sport" if from_listener else "dport"
    pattern = re.compile(
        rf"^-A\s+INPUT\s+.*-s\s+{re.escape(peer_ip)}(?:/\d+)?\s+.*"
        rf"--{direction}\s+{port}\b.*-j\s+DROP\b"
    )
    return any(pattern.match(line.strip()) for line in text.splitlines())


def parse_port_drop_packets(text: str, peer_ip: str, port: int) -> int:
    """Packets matched by the port DROP rules, from ``iptables -L -n -v`` output.

    Sums both directions. Zero means the rules exist but nothing hit them, which
    is indistinguishable from no partition at all for anything downstream.
    """
    total = 0
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 2 or "DROP" not in line or peer_ip not in line:
            continue
        if f"dpt:{port}" not in line and f"spt:{port}" not in line:
            continue
        with suppress(ValueError):
            total += int(fields[0])
    return total


def faketime_offset_literal(offset_ms: int) -> str:
    """Render a libfaketime offset for ``/tmp/faketime``.

    Always fractional seconds. libfaketime reads ``m`` as *minutes*, so a
    ``+250ms`` literal shifts the clock by 250 minutes — measured, not
    theoretical. Emitting only ``s`` makes that class of misfire impossible.
    """
    sign = "-" if offset_ms < 0 else "+"
    magnitude = abs(offset_ms)
    return f"{sign}{magnitude // 1000}.{magnitude % 1000:03d}s"


def faketime_write_cmd(offset_ms: int) -> str:
    return f"echo '{faketime_offset_literal(offset_ms)}' > {FAKETIME_FILE}"


def container_unix_ms_cmd() -> str:
    """Read the container's wall clock, which libfaketime intercepts."""
    return "date +%s%3N"


def ps_cmd() -> str:
    """Headerless ``pid stat args`` for every process in the container."""
    return "ps -eo pid=,stat=,args="


def df_cmd(path: str = PG_STATE_DIR) -> str:
    """POSIX single-line ``df`` in 1 KiB blocks."""
    return f"df -k -P {path}"


def fallocate_cmd(path: str, size_kb: int) -> str:
    return f"fallocate -l {size_kb}K {path}"


def dd_fill_cmd(path: str, size_mb: int) -> str:
    """Fallback filler for filesystems whose ``fallocate`` is unsupported."""
    return f"dd if=/dev/zero of={path} bs=1M count={size_mb} 2>&1"


def curl_probe_cmd(ip: str, *, port: int = MGMT_INTERNAL_PORT, timeout_s: float = 2.0) -> str:
    """Time a request to a peer's management API, printing seconds on stdout."""
    return (
        f"curl -s -o /dev/null -w '%{{time_total}}' --max-time {timeout_s:g} "
        f"http://{ip}:{port}/api/v1/cluster/leader"
    )


def lazyfs_control_cmd(command: str, *, fifo: str = LAZYFS_FIFO) -> str:
    """Write one ``lazyfs::`` control word to the LazyFS fault FIFO.

    The write must be newline-terminated or LazyFS never sees a complete
    command, and a command it never sees is a fault that never injects.
    """
    if not command.startswith("lazyfs::"):
        raise ValueError(f"not a lazyfs control command: {command!r}")
    return f"printf '{command}\\n' > {fifo}"


def lazyfs_probe_command(nonce: str) -> str:
    """A control word LazyFS is guaranteed not to implement.

    The fault worker logs every command it fails to recognise, so an unknown
    word is a round trip with no side effect: writing it proves nothing, but
    seeing it echoed in the log proves the worker is consuming the FIFO. The
    nonce keeps one probe's echo from being mistaken for an earlier one's.
    """
    if not nonce or not nonce.isalnum():
        raise ValueError(f"nonce must be non-empty alphanumeric: {nonce!r}")
    return f"lazyfs::pgbattery-probe-{nonce}"


def lazyfs_torn_op_cmd(
    path: str, *, parts: int, persist: Sequence[int], mount: LazyfsMount = LAZYFS_DATA
) -> str:
    """Arm a torn write: split the next write to `path` into `parts` equal
    pieces and persist only those in `persist`.

    `path` must be the path in the LazyFS **root** directory, not the mount.
    LazyFS keys its fault table by the path its FUSE callbacks receive, and the
    subdir module has already rewritten those to the backing root by then; it
    then `open()`s that path directly to write the surviving pieces. A mount
    path silently matches nothing.

    The FIFO form cannot select a later write. LazyFS hardcodes the occurrence
    to 1 when a torn-op arrives over the FIFO -- an `occurrence=` attribute is
    accepted by the parser and then ignored -- so the fault fires on the very
    next write to `path` and the caller must arm it immediately before the
    write it means to tear.

    LazyFS crashes itself once the surviving pieces are down, which is the
    point: the pieces it did not write are still only in its userspace cache,
    and they die with it.
    """
    if parts < 2:
        raise ValueError(f"a torn write needs at least 2 parts, got {parts}")
    if not persist:
        raise ValueError("persist must name at least one part to keep")
    if any(p < 1 or p > parts for p in persist):
        raise ValueError(f"persist parts must be within 1..{parts}: {list(persist)}")
    if len(set(persist)) != len(persist):
        raise ValueError(f"persist must not repeat a part: {list(persist)}")
    if len(persist) == parts:
        raise ValueError(
            f"persisting all {parts} parts is a whole write, not a torn one; "
            f"the fault would report success and change nothing"
        )
    if not mount.holds(path):
        raise ValueError(
            f"torn-op needs a path in the {mount.name} LazyFS root, not the mount "
            f"path: {path!r} does not start with {mount.root_dir}/"
        )
    kept = ",".join(str(p) for p in persist)
    return f"lazyfs::torn-op::file={path}::persist={kept}::parts={parts}"


def parse_lazyfs_torn_records(log_text: str, path: str) -> list[tuple[int, int]]:
    """Every ``(bytes, offset)`` LazyFS reports persisting for a tear on `path`.

    The offset is what says *which* structure was torn. A store's header or
    root lives at a low, fixed offset and a data page does not, so a suite that
    only counts bytes cannot tell a tear that exercised checksum validation
    from one that clipped an append nobody had committed to yet.
    """
    pattern = rf"Write to path {re.escape(path)}: will persist (\d+) bytes from offset (\d+)"
    return [(int(size), int(offset)) for size, offset in re.findall(pattern, log_text)]


def parse_lazyfs_torn_bytes(log_text: str, path: str) -> int | None:
    """Bytes LazyFS reports persisting for a torn write to `path`, else None.

    LazyFS logs one line per surviving piece. Their sum is how much of the
    write reached the backing store, and a `None` return means the fault was
    configured and never fired -- the case that would otherwise read as a
    clean run.
    """
    pattern = rf"Write to path {re.escape(path)}: will persist (\d+) bytes"
    sizes = [int(m) for m in re.findall(pattern, log_text)]
    return sum(sizes) if sizes else None


def lazyfs_log_cmd(*, log: str = LAZYFS_LOG) -> str:
    """Read the LazyFS log, tolerating its absence.

    A missing log must not look like a read failure: LazyFS truncates the file
    at startup, so there is a window where it does not exist yet, and the
    caller is polling for a line to appear in it anyway.
    """
    return f"cat {log} 2>/dev/null || true"


def count_lazyfs_received(log_text: str, command: str) -> int:
    """How many times the fault worker has logged receiving `command`.

    A count rather than a boolean because the caller compares before against
    after. Testing for presence would match the previous injection's line and
    pass without the current command having been read at all.
    """
    if not command.startswith("lazyfs::"):
        raise ValueError(f"not a lazyfs control command: {command!r}")
    return log_text.count(f"received '{command}'")


def parse_lazyfs_consumed(log_text: str, nonce: str) -> bool:
    """Whether the fault worker echoed the probe carrying `nonce`.

    Matches the worker's own "command unknown" line. Anything weaker — the
    nonce appearing anywhere in the log — would also match the command being
    logged on the way in rather than on the way out, which is the distinction
    the probe exists to draw.
    """
    return f"command unknown '{lazyfs_probe_command(nonce)}'" in log_text


def lazyfs_mounts_cmd() -> str:
    """Read the container's mount table.

    ``/proc/mounts`` rather than ``mount(8)``: the latter is not installed in
    the postgres image, and a missing binary would make the mount check exit
    127, which reads as "not mounted" and would disable the suite silently.
    """
    return "cat /proc/mounts"


# ─────────────────────────────────────────────────────────────────────────────
# Pure output parsing
# ─────────────────────────────────────────────────────────────────────────────

_TIME_UNITS_MS: Final[dict[str, float]] = {"us": 0.001, "ms": 1.0, "s": 1000.0}


@dataclass(frozen=True)
class NetemState:
    """What ``tc qdisc show`` reports for an installed netem qdisc."""

    delay_ms: float
    jitter_ms: float
    loss_pct: float
    dropped_packets: int = 0


def parse_netem(text: str) -> NetemState | None:
    """Parse a netem qdisc out of ``tc [-s] qdisc show``; None if absent."""
    if "netem" not in text:
        return None
    delay_ms = 0.0
    jitter_ms = 0.0
    delay = re.search(r"delay\s+([\d.]+)(us|ms|s)(?:\s+([\d.]+)(us|ms|s))?", text)
    if delay is not None:
        delay_ms = float(delay.group(1)) * _TIME_UNITS_MS[delay.group(2)]
        if delay.group(3) is not None and delay.group(4) is not None:
            jitter_ms = float(delay.group(3)) * _TIME_UNITS_MS[delay.group(4)]
    loss_pct = 0.0
    loss = re.search(r"loss\s+(?:random\s+)?([\d.]+)%", text)
    if loss is not None:
        loss_pct = float(loss.group(1))
    dropped = 0
    drop = re.search(r"\(dropped\s+(\d+)", text)
    if drop is not None:
        dropped = int(drop.group(1))
    return NetemState(
        delay_ms=delay_ms, jitter_ms=jitter_ms, loss_pct=loss_pct, dropped_packets=dropped
    )


@dataclass(frozen=True)
class FilterMatch:
    """One ``u32`` match in a ``tc -s filter show`` dump.

    ``rule_hits`` counts filter-chain consultations; ``match_success`` counts
    packets that actually matched the address, which is the number that proves
    the fault is doing work rather than merely existing.
    """

    hex_key: str
    offset: int
    prio: int
    rule_hits: int
    match_success: int


def parse_tc_filters(text: str) -> list[FilterMatch]:
    """Parse every u32 address match, with its counters, out of a filter dump."""
    matches: list[FilterMatch] = []
    prio = 0
    rule_hits = 0
    for line in text.splitlines():
        pref = re.search(r"\bpref\s+(\d+)\b", line)
        if pref is not None:
            prio = int(pref.group(1))
        hits = re.search(r"\(rule hit\s+(\d+)\s+success\s+(\d+)\)", line)
        if hits is not None:
            rule_hits = int(hits.group(1))
        key = re.search(
            r"\bmatch\s+([0-9a-f]{8})/[0-9a-f]{8}\s+at\s+(\d+)(?:\s*\(success\s+(\d+)\s*\))?",
            line,
        )
        if key is not None:
            matches.append(
                FilterMatch(
                    hex_key=key.group(1),
                    offset=int(key.group(2)),
                    prio=prio,
                    rule_hits=rule_hits,
                    match_success=int(key.group(3)) if key.group(3) is not None else 0,
                )
            )
    return matches


@dataclass(frozen=True)
class ProcessInfo:
    """One row of ``ps -eo pid=,stat=,args=``."""

    pid: int
    state: str
    """First character of the ``stat`` field. ``T`` means stopped by a signal."""
    args: str

    @property
    def is_stopped(self) -> bool:
        return self.state == "T"

    @property
    def pg_title(self) -> str | None:
        """The part after ``postgres: `` for a PG auxiliary/backend process."""
        if not self.args.startswith("postgres: "):
            return None
        return self.args.removeprefix("postgres: ").strip()


def parse_ps(text: str) -> list[ProcessInfo]:
    """Parse a headerless ``ps`` dump; unparseable lines are skipped."""
    processes: list[ProcessInfo] = []
    for line in text.splitlines():
        row = re.match(r"^\s*(\d+)\s+(\S+)\s+(.*)$", line)
        if row is None:
            continue
        processes.append(
            ProcessInfo(pid=int(row.group(1)), state=row.group(2)[0], args=row.group(3))
        )
    return processes


def select_pg_processes(
    processes: Sequence[ProcessInfo],
    titles: Sequence[str],
) -> list[ProcessInfo]:
    """PG auxiliary processes whose title starts with one of `titles`.

    Title prefixes rather than equality because PG appends state to several of
    them (``walreceiver streaming 0/4051230``, ``startup recovering ...``).
    """
    selected: list[ProcessInfo] = []
    for process in processes:
        title = process.pg_title
        if title is None:
            continue
        if any(title.startswith(name) for name in titles):
            selected.append(process)
    return selected


@dataclass(frozen=True)
class DiskUsage:
    """One filesystem row of ``df -k -P``."""

    filesystem: str
    total_kb: int
    used_kb: int
    avail_kb: int
    mount: str

    @property
    def total_bytes(self) -> int:
        return self.total_kb * 1024

    @property
    def avail_bytes(self) -> int:
        return self.avail_kb * 1024


def parse_lazyfs_mounted(proc_mounts: str, mount_dir: str) -> bool:
    """Whether `mount_dir` appears in ``/proc/mounts`` as a FUSE filesystem.

    The filesystem type is checked, not just the path. A container whose
    entrypoint failed to mount still has the directory — it is PGDATA on the
    ordinary filesystem — and treating "the path exists" as "LazyFS is mounted"
    is how the durability suite would come to assert fsync semantics against a
    filesystem that cannot lose an un-fsynced write.
    """
    for line in proc_mounts.splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[1] == mount_dir and fields[2].startswith("fuse"):
            return True
    return False


def parse_df(text: str) -> DiskUsage:
    """Parse the filesystem row out of ``df -k -P`` output."""
    for line in reversed(text.strip().splitlines()):
        row = line.split()
        if len(row) < 6 or not row[1].isdigit():
            continue
        return DiskUsage(
            filesystem=row[0],
            total_kb=int(row[1]),
            used_kb=int(row[2]),
            avail_kb=int(row[3]),
            mount=row[5],
        )
    raise FaultEffectNotObserved(f"could not parse df output: {text!r}")


_SIZE_UNITS: Final[dict[str, int]] = {
    "b": 1,
    "kb": 1024,
    "mb": 1024**2,
    "gb": 1024**3,
    "tb": 1024**4,
}


def parse_size_literal(text: str) -> int:
    """Parse a PG size setting (``16MB``, ``8kB``, ``1024``) into bytes."""
    stripped = text.strip()
    match = re.fullmatch(r"([\d.]+)\s*([a-zA-Z]*)", stripped)
    if match is None:
        raise FaultEffectNotObserved(f"could not parse a size out of {text!r}")
    unit = match.group(2).lower() or "b"
    if unit not in _SIZE_UNITS:
        raise FaultEffectNotObserved(f"unknown size unit in {text!r}")
    return int(float(match.group(1)) * _SIZE_UNITS[unit])


def parse_curl_seconds(text: str) -> float:
    """Parse curl's ``%{time_total}``; 0.0 when curl printed nothing usable."""
    match = re.search(r"([\d.]+)", text.strip())
    return float(match.group(1)) if match is not None else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Pure verifiers — each raises FaultEffectNotObserved rather than returning bool
# ─────────────────────────────────────────────────────────────────────────────


def verify_netem_applied(
    text: str,
    *,
    target: str,
    expect_delay_ms: int,
    expect_jitter_ms: int,
    expect_loss_pct: float,
) -> NetemState:
    """Assert the qdisc dump reports the latency/jitter/loss we asked for."""
    state = parse_netem(text)
    if state is None:
        raise FaultEffectNotObserved(
            f"{target}: no netem qdisc present after injection. tc reported:\n{text}"
        )
    problems: list[str] = []
    if abs(state.delay_ms - expect_delay_ms) > 1.0:
        problems.append(f"delay {state.delay_ms}ms != requested {expect_delay_ms}ms")
    if abs(state.jitter_ms - expect_jitter_ms) > 1.0:
        problems.append(f"jitter {state.jitter_ms}ms != requested {expect_jitter_ms}ms")
    if abs(state.loss_pct - expect_loss_pct) > 0.5:
        problems.append(f"loss {state.loss_pct}% != requested {expect_loss_pct}%")
    if problems:
        raise FaultEffectNotObserved(f"{target}: netem qdisc mismatch: {'; '.join(problems)}")
    return state


def verify_netem_absent(text: str, *, target: str) -> None:
    """Assert no netem qdisc survives the heal."""
    state = parse_netem(text)
    if state is not None:
        raise FaultEffectNotObserved(
            f"{target}: netem qdisc still installed after heal: {state}. tc reported:\n{text}"
        )


def verify_filter_present(text: str, *, target: str, ip: str, prio: int) -> FilterMatch:
    """Assert a u32 filter for `ip` is installed at `prio`."""
    wanted = ip_to_u32_hex(ip)
    for match in parse_tc_filters(text):
        if match.hex_key == wanted and match.prio == prio:
            return match
    raise FaultEffectNotObserved(
        f"{target}: no tc filter matching {ip} ({wanted}) at prio {prio}. "
        f"tc reported:\n{text or '<empty>'}"
    )


def verify_filter_absent(text: str, *, target: str, ip: str) -> None:
    """Assert no u32 filter for `ip` survives the heal."""
    wanted = ip_to_u32_hex(ip)
    for match in parse_tc_filters(text):
        if match.hex_key == wanted:
            raise FaultEffectNotObserved(
                f"{target}: tc filter for {ip} still installed after heal at prio {match.prio}"
            )


def verify_packets_matched(
    before: FilterMatch,
    after: FilterMatch,
    *,
    target: str,
    what: str,
) -> int:
    """Assert a filter's match counter advanced, i.e. it is doing work.

    A rule that exists but never matches is indistinguishable from no rule, so
    this is the difference between "configured" and "in effect".
    """
    delta = after.match_success - before.match_success
    if delta <= 0:
        raise FaultEffectNotObserved(
            f"{target}: {what} matched 0 packets during the probe window "
            f"(before={before.match_success}, after={after.match_success}); "
            "the rule is installed but not in effect"
        )
    return delta


def verify_processes_stopped(
    processes: Sequence[ProcessInfo],
    *,
    target: str,
    expected_pids: Sequence[int],
) -> list[ProcessInfo]:
    """Assert every expected pid is present and in state ``T``."""
    by_pid = {process.pid: process for process in processes}
    missing = [pid for pid in expected_pids if pid not in by_pid]
    if missing:
        raise FaultEffectNotObserved(
            f"{target}: process(es) {missing} vanished instead of stopping — "
            "PG likely restarted, so the stall window is not what it appears"
        )
    running = [by_pid[pid] for pid in expected_pids if not by_pid[pid].is_stopped]
    if running:
        detail = ", ".join(f"pid {p.pid} state {p.state} ({p.args.strip()})" for p in running)
        raise FaultEffectNotObserved(f"{target}: SIGSTOP did not take effect: {detail}")
    return [by_pid[pid] for pid in expected_pids]


def verify_processes_resumed(
    processes: Sequence[ProcessInfo],
    *,
    target: str,
    expected_pids: Sequence[int],
) -> None:
    """Assert no expected pid is still stopped (a vanished pid is fine here:
    PG may have restarted the auxiliary process, which is itself recovery)."""
    by_pid = {process.pid: process for process in processes}
    still_stopped = [pid for pid in expected_pids if pid in by_pid and by_pid[pid].is_stopped]
    if still_stopped:
        raise FaultEffectNotObserved(
            f"{target}: process(es) {still_stopped} still stopped after SIGCONT"
        )


def verify_scope_unaffected(
    processes: Sequence[ProcessInfo],
    *,
    target: str,
    stopped_pids: Sequence[int],
) -> None:
    """Assert the stall is bounded: something in the container still runs.

    Invariant 3 in miniature — a primitive that targets the checkpointer must
    not have stopped the postmaster.
    """
    stopped = set(stopped_pids)
    others_running = [
        process
        for process in processes
        if process.pid not in stopped and "postgres" in process.args and not process.is_stopped
    ]
    if not others_running:
        raise FaultEffectNotObserved(
            f"{target}: every postgres process is stopped — the fault escaped its scope"
        )


def verify_out_of_space(usage: DiskUsage, *, target: str, need_bytes: int) -> None:
    """Assert less than `need_bytes` remains free."""
    if usage.avail_bytes >= need_bytes:
        raise FaultEffectNotObserved(
            f"{target}: {usage.avail_bytes} bytes still free on {usage.mount}, "
            f"which is enough for another {need_bytes}-byte allocation — the fill "
            "did not exhaust the filesystem"
        )


def verify_space_restored(usage: DiskUsage, *, target: str, need_bytes: int) -> None:
    """Assert the heal freed enough space for another WAL segment."""
    if usage.avail_bytes < need_bytes:
        raise FaultEffectNotObserved(
            f"{target}: only {usage.avail_bytes} bytes free on {usage.mount} after heal, "
            f"below the {need_bytes} bytes one WAL segment needs"
        )


def verify_bounded_filesystem(usage: DiskUsage, *, target: str, max_bytes: int) -> None:
    """Assert the filesystem is small enough that a fill is safe and meaningful."""
    if usage.total_bytes > max_bytes:
        raise FaultPreconditionError(
            f"{target}: {usage.mount} is {usage.total_bytes / 1024**3:.1f} GiB "
            f"(limit {max_bytes / 1024**3:.1f} GiB).\n{_BOUNDED_STATE_REMEDIATION}"
        )


def verify_clock_offset(
    *,
    target: str,
    observed_ms: float,
    expected_ms: int,
    tolerance_ms: float,
) -> None:
    """Assert the container's wall clock actually moved by `expected_ms`.

    This doubles as the libfaketime capability check: an image whose
    ``LD_PRELOAD`` failed to resolve reports an offset of zero, and a literal
    with the wrong unit reports a wildly wrong one.
    """
    if abs(observed_ms - expected_ms) > tolerance_ms:
        raise FaultEffectNotObserved(
            f"{target}: clock moved {observed_ms:.0f}ms, expected {expected_ms}ms "
            f"(tolerance {tolerance_ms:.0f}ms). Either libfaketime is not active "
            f"({FAKETIME_FILE} / LD_PRELOAD) or the offset literal was misparsed."
        )


def verify_probe_blackholed(*, target: str, rc: int, peer: str) -> None:
    """Assert a probe was dropped rather than refused or answered.

    ``curl`` exit 28 is a timeout, which is what a blackhole looks like; exit 7
    means the SYN reached a socket that refused it, i.e. packets are flowing.
    """
    if rc == CURL_EXIT_TIMEOUT:
        return
    reason = "connection refused (packets are flowing)" if rc == CURL_EXIT_CONNECT_REFUSED else "OK"
    raise FaultEffectNotObserved(
        f"{target}: probe to {peer} exited {rc} — {reason}; expected "
        f"{CURL_EXIT_TIMEOUT} (blackholed)"
    )


def verify_probe_reachable(*, target: str, rc: int, peer: str) -> None:
    """Assert a probe succeeded — used for scope and heal checks."""
    if rc != 0:
        raise FaultEffectNotObserved(
            f"{target}: probe to {peer} exited {rc}, expected 0. Either the fault "
            "escaped its declared scope or the heal did not restore reachability."
        )


def verify_added_latency(
    *,
    target: str,
    baseline_s: float,
    observed_s: float,
    expected_delay_ms: int,
) -> float:
    """Assert an end-to-end probe actually got slower by roughly the delay.

    netem shapes egress, so a request out of the shaped container carries the
    delay once. Half the requested delay is the floor: enough to separate the
    effect from container-scheduling noise without being brittle.
    """
    added_ms = (observed_s - baseline_s) * 1000.0
    floor_ms = expected_delay_ms / 2.0
    if added_ms < floor_ms:
        raise FaultEffectNotObserved(
            f"{target}: probe latency rose by only {added_ms:.0f}ms "
            f"(baseline {baseline_s * 1000:.0f}ms, observed {observed_s * 1000:.0f}ms); "
            f"expected at least {floor_ms:.0f}ms from a {expected_delay_ms}ms netem delay"
        )
    return added_ms


# ─────────────────────────────────────────────────────────────────────────────
# Container-facing helpers
# ─────────────────────────────────────────────────────────────────────────────


def _resolve_ip(container: str) -> str:
    ip = NODE_IPS.get(container)
    if ip is None:
        raise FaultPreconditionError(
            f"unknown container {container!r}; known: {sorted(NODE_IPS)}. "
            "Static addresses are declared in docker-compose.yml."
        )
    return ip


EXEC_UNAVAILABLE_MARKERS: Final[tuple[str, ...]] = (
    "is restarting, wait until the container is running",
    "is not running",
    "error executing setns process",
    "cannot exec in a stopped container",
)
"""Failures that mean the command never reached the container.

The first two are the daemon refusing a stopped container; the third is runc
unable to enter its namespaces, which happens while a container is coming up.
The fourth is the same refusal in the OCI runtime's own words, which is what
the daemon passes through on a container that has not started yet — missing it
made every wait-for-startup path fail instead of waiting.

``No such container`` is excluded: a name resolving to nothing is topology
drift, which must fail rather than be waited on.
"""


def exec_undelivered(result: CommandResult) -> bool:
    """Whether `result` is a failure that says nothing about the command.

    Three shapes, all meaning the command never ran to an answer: the daemon
    refusing a container that is not running, runc unable to enter its
    namespaces, and a non-zero exit carrying no diagnostic on either stream --
    which a real command failure never does, because the shell reports its own
    errors. Timeouts count: no answer arrived.
    """
    if result.ok:
        return False
    if result.rc == TIMED_OUT_RC:
        return True
    detail = result.output.strip()
    return not detail or any(marker in detail for marker in EXEC_UNAVAILABLE_MARKERS)


def read_failure(container: str, what: str, result: CommandResult) -> FaultPreconditionError:
    """Classify a failed container read as indeterminate or genuinely bad."""
    detail = result.stderr.strip() or result.stdout.strip() or f"exited {result.rc} in silence"
    if exec_undelivered(result):
        return ContainerNotRunning(f"{container}: {what} could not run: {detail}")
    return FaultPreconditionError(f"{container}: {what} failed: {detail}")


def exec_when_deliverable(
    container: str, script: str, *, as_root: bool = True, timeout_s: float = 60.0
) -> CommandResult:
    """Run `script` in `container`, waiting out execs that never land.

    For the paths that address a container which may be mid-restart. Returns
    the first result that actually ran, so a command that ran and failed stays
    the caller's to interpret; an undeliverable one at the deadline is returned
    as it is, carrying whatever the daemon last said.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        result = exec_in(container, script, as_root=as_root)
        if not exec_undelivered(result) or time.monotonic() >= deadline:
            return result
        time.sleep(0.5)


def wipe_node_state(service: str, paths: Sequence[str], *, timeout_s: float = 90.0) -> None:
    """Destroy `paths` inside `service`'s state volume, with the node stopped.

    Through a throwaway container on the same volume rather than an exec, for
    the reason the Raft-store inversion had to learn: a live node puts back
    what is taken from underneath it, and `docker exec` needs the very
    container this is trying to empty.

    The removal is read back. A wipe that silently did nothing would leave the
    node rejoining with all its state intact, which is a green run measuring a
    fault that never happened.
    """
    _emit("fault.begin", "wipe_node_state", service, {"paths": list(paths)})
    stopped = run(f"docker compose stop {service}", timeout_s)
    if not stopped.ok:
        raise FaultInjectionError(f"could not stop {service} to wipe it: {stopped.output}")

    targets = " ".join(f"{p}/* {p}/.[!.]*" for p in paths)
    # One `ls -A` per directory. Given several at once it prints a `dir:` header
    # before each listing, so the read-back was never empty and a multi-path
    # wipe could not be verified at all.
    checks = "; ".join(f'ls -A "{p}" 2>/dev/null' for p in paths)
    script = f"rm -rf {targets} 2>/dev/null; {{ {checks}; }} | head -5"
    wiped = run(
        f"docker compose run --rm --no-deps --entrypoint sh {service} -c '{script}'", timeout_s
    )
    if not wiped.ok:
        raise FaultInjectionError(f"{service}: could not wipe {paths}: {wiped.output}")
    leftover = wiped.stdout.strip()
    if leftover:
        raise FaultEffectNotObserved(
            f"{service}: {paths} still hold entries after being wiped: {leftover!r}. "
            f"The node would rejoin with the state this meant to destroy."
        )
    _emit("fault.injected", "wipe_node_state", service, {"paths": list(paths)})


def read_processes(container: str) -> list[ProcessInfo]:
    """Snapshot every process in `container`."""
    result = exec_in(container, ps_cmd())
    if not result.ok:
        raise read_failure(container, "ps", result)
    return parse_ps(result.output)


def read_disk_usage(container: str, path: str = PG_STATE_DIR) -> DiskUsage:
    """Read the filesystem `path` lives on inside `container`."""
    result = exec_in(container, df_cmd(path))
    if not result.ok:
        raise read_failure(container, "df", result)
    return parse_df(result.output)


def read_wal_segment_bytes(container: str) -> int:
    """Ask PG for its WAL segment size — the allocation ENOSPC must break."""
    result = exec_in(
        container,
        f"psql -h 127.0.0.1 -p {PG_INTERNAL_PORT} -U postgres -tAc 'SHOW wal_segment_size'",
        as_root=False,
    )
    if not result.ok or not result.stdout.strip():
        raise FaultPreconditionError(
            f"{container}: could not read wal_segment_size from PG "
            f"({result.stderr.strip() or 'empty response'}). The fill must be sized "
            "against the real segment size, so this is fatal rather than defaulted."
        )
    return parse_size_literal(result.stdout)


def read_container_unix_ms(container: str) -> int:
    """The container's own view of wall-clock time, in milliseconds."""
    result = exec_in(container, container_unix_ms_cmd(), as_root=False)
    if not result.ok or not result.stdout.strip().isdigit():
        raise FaultPreconditionError(
            f"{container}: could not read the container clock: {result.stderr.strip()}"
        )
    return int(result.stdout.strip())


def probe_peer(
    container: str,
    peer: str,
    *,
    timeout_s: float = 2.0,
) -> tuple[int, float]:
    """Time an HTTP probe from `container` to `peer`'s management API.

    Returns ``(curl_exit_code, seconds)``. ``ping`` is not in the image, so this
    is the reachability oracle: 0 answered, 7 refused (packets flowing), 28
    timed out (blackholed).
    """
    peer_ip = _resolve_ip(peer)
    result = exec_in(
        container,
        curl_probe_cmd(peer_ip, timeout_s=timeout_s),
        as_root=False,
        timeout_s=timeout_s + 8.0,
    )
    return result.rc, parse_curl_seconds(result.stdout)


def _generate_probe_traffic(container: str, peer: str, *, attempts: int = 3) -> int:
    """Send probes so packet counters have something to count. Returns last rc."""
    rc = 0
    for _ in range(attempts):
        rc, _ = probe_peer(container, peer, timeout_s=1.0)
    return rc


def read_ingress_filters(container: str, *, dev: str = NET_DEVICE) -> str:
    """``tc -s filter show`` for the ingress qdisc, counters included."""
    return exec_in(container, f"tc -s filter show dev {dev} parent ffff:").output


def read_qdiscs_cmd(dev: str = NET_DEVICE) -> str:
    """``tc -s qdisc show``, counters included.

    Split from `read_qdiscs` so a caller that runs its own exec (ci_runner
    drives probes through its retry/logging layer) shares the command instead
    of restating it.
    """
    return f"tc -s qdisc show dev {dev}"


def read_qdiscs(container: str, *, dev: str = NET_DEVICE) -> str:
    """``tc -s qdisc show``, counters included."""
    return exec_in(container, read_qdiscs_cmd(dev)).output


def drop_ingress_qdisc_if_empty(container: str, *, dev: str = NET_DEVICE) -> bool:
    """Remove the ingress qdisc once it holds no filters. Returns whether it went.

    Keeps invariant 2 (no zombie qdiscs) without stepping on a concurrent
    partition that still has filters parked under the same qdisc.
    """
    if parse_tc_filters(read_ingress_filters(container, dev=dev)):
        return False
    return exec_in(container, ingress_qdisc_del_cmd(dev)).ok


def _heal_steps(primitive: str, target: str, steps: Sequence[tuple[str, str]]) -> list[str]:
    """Run cleanup commands, collecting failures instead of raising.

    Invariant 1: a test that crashed mid-fault must still get its cleanup, so
    heal never raises from a ``finally``. Failures come back to the caller,
    which logs them into the close event and leaves them for :func:`scrub`.
    """
    failures: list[str] = []
    for container, script in steps:
        result = exec_in(container, script)
        if not result.ok:
            failures.append(f"{container}: `{script}` rc={result.rc} {result.stderr.strip()}")
    if failures:
        console.print(f"[yellow]{primitive}({target}): cleanup incomplete[/]")
        for failure in failures:
            console.print(f"  [yellow]{failure}[/]")
    return failures


# ─────────────────────────────────────────────────────────────────────────────
# Primitive handles
# ─────────────────────────────────────────────────────────────────────────────


class Direction(StrEnum):
    """Which side of the wire destroys the packets.

    Both produce the same packet-level asymmetry (``from -> to`` destroyed,
    ``to -> from`` delivered); they differ in where the loss happens, which is
    what a stack can and cannot notice.
    """

    INBOUND = "inbound"
    """Drop on arrival at `to`. `from`'s stack believes every send succeeded —
    the classic "leader still thinks it has quorum" shape, and the exact
    behaviour of ``iptables -I INPUT -s <from> -j DROP``."""

    OUTBOUND = "outbound"
    """Drop on departure from `from`. Closer to a broken uplink: the loss is on
    the sender's side of the wire."""


class Aim(StrEnum):
    """Where inside the live failover window a clock skew is placed."""

    HOLDDOWN_START = "holddown_start"
    """As soon as the leaderless edge is observed — maximum exposure, because
    the entire remaining hold-down is skipped if the guard is wall-clock."""

    LEASE_EXPIRY = "lease_expiry"
    """At ``leaderless + lease_duration``, i.e. the boundary itself, within
    ``window_ms / 2``. Distinguishes "releases early" from "releases at the
    boundary anyway"."""


@dataclass(frozen=True)
class ChannelPartitionHandle:
    """Live state of a :func:`partition_channel` window."""

    container: str
    peers: tuple[str, ...]
    channel: Channel
    port: int
    dropped_packets: int


@dataclass(frozen=True)
class NetworkDetachHandle:
    """Live state of a :func:`network_detached` window."""

    service: str
    container: str
    network: str
    restore_ip: str


@dataclass
class NetemHandle:
    """Live state of a :func:`partition_lossy` window."""

    container: str
    drop_pct: float
    latency_ms: int
    jitter_ms: int
    state: NetemState
    added_latency_ms: float | None = None
    dropped_packets: int | None = None


@dataclass
class AsymmetricHandle:
    """Live state of a :func:`partition_asymmetric` window."""

    from_container: str
    to_container: str
    direction: Direction
    drop_side: str
    dropped_packets: int
    delivered_packets: int


@dataclass
class StoppedProcessHandle:
    """Live state of a signal-stall window."""

    container: str
    processes: list[ProcessInfo] = field(default_factory=list)

    @property
    def pids(self) -> list[int]:
        return [process.pid for process in self.processes]

    @property
    def titles(self) -> list[str]:
        return [process.pg_title or process.args for process in self.processes]


@dataclass
class LeaseBoundarySkew:
    """Where, relative to the live failover window, the skew actually landed."""

    container: str
    aim: Aim
    skew_ms: int
    observed_skew_ms: float
    lease_ms: int
    leaderless_at: float
    """Monotonic instant at which no node held a valid lease any more."""
    injected_at: float
    promotion_holddowns_before: float | None = None
    promotion_holddowns_after: float | None = None
    """``pgbattery_promotion_lease_holddowns`` on the skewed node, sampled either
    side of the window. Filled in on close."""

    @property
    def offset_into_window_ms(self) -> float:
        """Milliseconds from the lease edge to the injection."""
        return (self.injected_at - self.leaderless_at) * 1000.0

    @property
    def releases_holddown_early(self) -> bool:
        """Whether this skew is large enough to satisfy a *wall-clock*
        hold-down that the monotonic lease has not actually outlived."""
        return self.observed_skew_ms + self.offset_into_window_ms >= self.lease_ms

    @property
    def holddown_engaged(self) -> bool | None:
        """Whether the hold-down actually refused a promotion in this window.

        ``None`` when the counter could not be read on both sides. A window in
        which it never rose means the skew arrived after promotion had already
        happened, so the experiment says nothing about the guard.
        """
        if self.promotion_holddowns_before is None or self.promotion_holddowns_after is None:
            return None
        return self.promotion_holddowns_after > self.promotion_holddowns_before


@dataclass
class DiskFullHandle:
    """Live state of a :func:`disk_full_during_wal` window."""

    container: str
    filler_path: str
    filled_kb: int
    wal_segment_bytes: int
    usage_before: DiskUsage
    usage_after: DiskUsage


# ─────────────────────────────────────────────────────────────────────────────
# Primitives
# ─────────────────────────────────────────────────────────────────────────────


@contextmanager
def partition_channel(
    container: str,
    peers: Sequence[str],
    channel: Channel,
    *,
    require_traffic: bool = True,
    settle_s: float = 2.0,
) -> Iterator[ChannelPartitionHandle]:
    """Drop inbound traffic to one protocol port from `peers`, leaving the rest.

    Expresses the gray split that per-peer-IP partitions cannot: kill streaming
    replication while Raft stays healthy, or the reverse.

    `require_traffic` fails the window if no packet ever hit the rule. A rule
    that exists but matched nothing partitions nothing, and the case around it
    would still pass.
    """
    peer_ips = [_resolve_ip(p) for p in peers]
    port = channel.port
    _emit("inject", "partition_channel", container, {"channel": str(channel), "peers": peers})

    installed: list[tuple[str, bool]] = []
    try:
        for ip in peer_ips:
            for from_listener in (False, True):
                result = exec_in(
                    container,
                    iptables_port_drop_cmd(ip, port, insert=True, from_listener=from_listener),
                )
                if not result.ok:
                    raise FaultInjectionError(
                        f"could not drop {channel} traffic from {ip} at {container}: "
                        f"{result.output.strip()}"
                    )
                installed.append((ip, from_listener))

        rules = exec_in(container, iptables_rules_cmd()).output
        for ip, from_listener in installed:
            if not parse_port_drop_rule(rules, ip, port, from_listener=from_listener):
                raise FaultEffectNotObserved(
                    f"{container}: no DROP rule for {ip} port {port} "
                    f"({'sport' if from_listener else 'dport'}) after insert"
                )

        # Raft heartbeats and WAL streaming are continuous, so a live rule starts
        # counting within a tick or two; no synthetic traffic needed.
        matched = 0
        if require_traffic:
            time.sleep(settle_s)
            counters = exec_in(container, iptables_rules_cmd()).output
            matched = sum(parse_port_drop_packets(counters, ip, port) for ip in peer_ips)
            if matched == 0:
                raise FaultEffectNotObserved(
                    f"{container}: {channel} DROP rules matched no packets in "
                    f"{settle_s}s; nothing was partitioned. "
                    f"{_CHANNEL_SIDE_HINT.get(channel, '')}".strip()
                )

        yield ChannelPartitionHandle(
            container=container,
            peers=tuple(peers),
            channel=channel,
            port=port,
            dropped_packets=matched,
        )
    finally:
        heal_failures: list[str] = []
        for ip, from_listener in installed:
            result = exec_in(
                container,
                iptables_port_drop_cmd(ip, port, insert=False, from_listener=from_listener),
            )
            if not result.ok:
                heal_failures.append(f"{ip}: {result.output.strip()}")
        residue = exec_in(container, iptables_rules_cmd()).output
        for ip, from_listener in installed:
            if parse_port_drop_rule(residue, ip, port, from_listener=from_listener):
                heal_failures.append(f"{ip}: DROP rule survived removal")
        _emit("heal", "partition_channel", container, {"channel": str(channel)})
        if heal_failures:
            raise FaultEffectNotObserved(
                f"{container}: {channel} partition did not heal — " + "; ".join(heal_failures)
            )


@contextmanager
def network_detached(
    service: str,
    *,
    network: str | None = None,
) -> Iterator[NetworkDetachHandle]:
    """Detach `service` from the cluster network for the duration of the block.

    A total partition, as distinct from :func:`partition_lossy` (which degrades a
    link) and :func:`partition_asymmetric` (which drops one direction). The node
    keeps running and keeps its data; it simply cannot be reached.

    The restore address is read from the container beforehand rather than derived
    from a node index, so reattaching cannot put a node back on a different
    address than it had. Both the detach and the reattach are verified, and a
    failed reattach raises: leaving a node off the network would poison every
    later case in the run.
    """
    net = network if network is not None else cluster_network()
    cid = container_id(service)
    before = read_container_networks(service)
    restore_ip = before.get(net, "")
    if not restore_ip:
        raise FaultPreconditionError(
            f"{service} is not attached to {net} (attached: {sorted(before)}), so "
            "detaching it would inject nothing. Check the cluster is up and "
            f"COMPOSE_PROJECT_NAME ({compose_project()!r}) matches it."
        )

    _emit("inject", "network_detached", service, {"network": net, "ip": restore_ip})
    detach = run(f"docker network disconnect {net} {cid}")
    if not detach.ok:
        raise FaultInjectionError(f"could not detach {service} from {net}: {detach.stderr.strip()}")
    verify_detached(read_container_networks(service), target=service, network=net)

    handle = NetworkDetachHandle(service=service, container=cid, network=net, restore_ip=restore_ip)
    try:
        yield handle
    finally:
        reattach = run(f"docker network connect --ip {restore_ip} {net} {cid}")
        if not reattach.ok:
            raise FaultInjectionError(
                f"could not reattach {service} to {net} at {restore_ip}: {reattach.stderr.strip()}"
            )
        verify_attached(read_container_networks(service), target=service, network=net)
        _emit("heal", "network_detached", service, {"network": net, "ip": restore_ip})


@contextmanager
def partition_lossy(
    container: str,
    drop_pct: float,
    latency_ms: int,
    *,
    jitter_ms: int = 0,
    dev: str = NET_DEVICE,
    probe_peer_name: str | None = None,
) -> Iterator[NetemHandle]:
    """Degrade `container`'s link with combined packet loss and latency.

    PROVES: Raft heartbeat and lease-renewal behaviour on a link that is bad
    rather than broken — retries partially succeed, timing budgets partially
    blow. This is the regime that separates "leader is unreachable" from
    "leader is slow", and it is what the CI matrix's delay-and-jitter-only
    ``network_delay`` step cannot reach.

    Both effects are verified dynamically, not just read back from the qdisc:
    latency by timing a real probe, loss by the netem drop counter after probe
    traffic. Parameters too small to verify either way are rejected, because a
    fault nobody can observe proves nothing.

    Raises:
        FaultPreconditionError: parameters below the verifiable floors, or
            another primitive already owns ``dev``'s root qdisc.
        FaultEffectNotObserved: qdisc, latency or drop counter disagree.
    """
    if drop_pct < MIN_VERIFIABLE_LOSS_PCT and latency_ms < MIN_VERIFIABLE_LATENCY_MS:
        raise FaultPreconditionError(
            f"partition_lossy({container}): drop_pct={drop_pct} and "
            f"latency_ms={latency_ms} are both below the verifiable floors "
            f"({MIN_VERIFIABLE_LOSS_PCT}% / {MIN_VERIFIABLE_LATENCY_MS}ms). A short "
            "probe can legitimately drop zero packets and sub-25ms latency is inside "
            "container scheduling noise, so neither effect could be confirmed."
        )
    peer = probe_peer_name or next(node for node in NODES if node != container)
    _resolve_ip(container)
    _resolve_ip(peer)

    # Held for the whole window, not just the add: the device is occupied until
    # the heal removes the qdisc. See `root_qdisc_lock`. Bounded, because the
    # lock is not reentrant: nesting two windows on one container on one thread
    # would otherwise hang instead of failing the way tc used to.
    device = root_qdisc_lock(container, dev)
    if not device.acquire(timeout=ROOT_QDISC_WAIT_S):
        raise FaultPreconditionError(
            f"partition_lossy({container}): dev {dev}'s root qdisc was still held after "
            f"{ROOT_QDISC_WAIT_S:g}s. Another netem window on this container has not "
            "closed — nested windows on one thread cannot both hold the device."
        )
    try:
        yield from _partition_lossy_locked(
            container,
            drop_pct,
            latency_ms,
            jitter_ms=jitter_ms,
            dev=dev,
            peer=peer,
        )
    finally:
        device.release()


def _partition_lossy_locked(
    container: str,
    drop_pct: float,
    latency_ms: int,
    *,
    jitter_ms: int,
    dev: str,
    peer: str,
) -> Iterator[NetemHandle]:
    """`partition_lossy`'s body, run with the device's root qdisc held."""
    baseline_rc, baseline_s = probe_peer(container, peer, timeout_s=2.0)
    verify_probe_reachable(target=container, rc=baseline_rc, peer=peer)

    add = exec_in(
        container,
        netem_add_cmd(delay_ms=latency_ms, jitter_ms=jitter_ms, loss_pct=drop_pct, dev=dev),
    )
    if not add.ok:
        raise FaultInjectionError(
            f"partition_lossy({container}): tc rejected the qdisc: "
            f"{add.stderr.strip() or add.stdout.strip()}. 'Exclusivity flag on' means "
            f"another primitive already owns dev {dev} root — heal it or call scrub()."
        )

    handle = NetemHandle(
        container=container,
        drop_pct=drop_pct,
        latency_ms=latency_ms,
        jitter_ms=jitter_ms,
        state=verify_netem_applied(
            read_qdiscs(container, dev=dev),
            target=container,
            expect_delay_ms=latency_ms,
            expect_jitter_ms=jitter_ms,
            expect_loss_pct=drop_pct,
        ),
    )

    if latency_ms >= MIN_VERIFIABLE_LATENCY_MS:
        probe_timeout = max(2.0, (latency_ms * 4) / 1000.0 + 2.0)
        _, observed_s = probe_peer(container, peer, timeout_s=probe_timeout)
        handle.added_latency_ms = verify_added_latency(
            target=container,
            baseline_s=baseline_s,
            observed_s=observed_s,
            expected_delay_ms=latency_ms,
        )
    if drop_pct >= MIN_VERIFIABLE_LOSS_PCT:
        _generate_probe_traffic(container, peer, attempts=4)
        state = parse_netem(read_qdiscs(container, dev=dev))
        if state is None or state.dropped_packets <= 0:
            raise FaultEffectNotObserved(
                f"partition_lossy({container}): netem dropped 0 packets at "
                f"{drop_pct}% loss after probe traffic; the qdisc is installed but "
                "not dropping"
            )
        handle.dropped_packets = state.dropped_packets

    opened_at = time.monotonic()
    _emit(
        "fault.open",
        "partition_lossy",
        container,
        {
            "detail": {
                "drop_pct": drop_pct,
                "latency_ms": latency_ms,
                "jitter_ms": jitter_ms,
                "added_latency_ms": handle.added_latency_ms,
                "dropped_packets": handle.dropped_packets,
            }
        },
    )
    try:
        yield handle
    finally:
        failures = _heal_steps("partition_lossy", container, [(container, netem_del_cmd(dev))])
        residue: list[str] = list(failures)
        with suppress(FaultEffectNotObserved):
            verify_netem_absent(read_qdiscs(container, dev=dev), target=container)
        state_after = parse_netem(read_qdiscs(container, dev=dev))
        if state_after is not None:
            residue.append(f"netem still installed on {container}: {state_after}")
        _emit(
            "fault.close",
            "partition_lossy",
            container,
            {"held_ms": round((time.monotonic() - opened_at) * 1000), "residue": residue},
        )


@contextmanager
def partition_asymmetric(
    from_container: str,
    to_container: str,
    *,
    direction: Direction = Direction.INBOUND,
    dev: str = NET_DEVICE,
) -> Iterator[AsymmetricHandle]:
    """Destroy ``from -> to`` packets while ``to -> from`` packets keep arriving.

    PROVES: behaviour under one-directional reachability — a leader whose
    AppendEntries land but whose acks never come back, so it believes it holds
    quorum while its followers hold an election. `direction` selects which side
    of the wire destroys the traffic (see :class:`Direction`).

    Both halves of the asymmetry are proven mechanically, with tc counters:
    the drop rule's match counter must advance (packets really are being
    destroyed) and a pass-through counting rule on the surviving direction must
    also advance (packets really are still delivered). A third node is probed to
    confirm the fault stayed inside its declared scope.

    Note that only the *packet* direction is asymmetric. Request/response in the
    surviving direction still fails at the application layer, because a response
    carries the blocked address as its source — identical to what
    ``iptables -I INPUT -s <ip> -j DROP`` does, and the level at which Raft's
    reachability assumptions live.

    Raises:
        FaultInjectionError: tc rejected the rule.
        FaultEffectNotObserved: the rule is installed but matched no packets,
            the surviving direction is not delivering, or an unrelated peer
            became unreachable.
    """
    from_ip = _resolve_ip(from_container)
    to_ip = _resolve_ip(to_container)
    inbound = direction is Direction.INBOUND
    drop_side = to_container if inbound else from_container
    drop_key_ip = from_ip if inbound else to_ip
    # Ingress filters carry the address-derived priority; egress filters live
    # under the prio qdisc's own numbering, where band 1:1 is prio 1.
    drop_prio = drop_prio_for(drop_key_ip) if inbound else 1
    drop_parent = "ffff:" if inbound else "1:"
    count_prio = count_prio_for(to_ip)

    def read_drop_filters() -> str:
        return exec_in(drop_side, f"tc -s filter show dev {dev} parent {drop_parent}").output

    if inbound:
        # `to` becomes deaf to `from`: ingress u32 + gact drop, the tc spelling
        # of iptables INPUT -s DROP. The ingress qdisc lives on handle ffff:,
        # so it does not contend with a netem root qdisc.
        exec_in(to_container, ingress_qdisc_add_cmd(dev))
        install = exec_in(
            to_container,
            ingress_filter_add_cmd(from_ip, prio=drop_prio, action="drop", dev=dev),
        )
    else:
        # `from` stops emitting toward `to`: prio band fed by netem loss 100%.
        install = CommandResult(0, "", "")
        for script in egress_drop_cmds(to_ip, dev=dev):
            install = exec_in(from_container, script)
            if not install.ok:
                break

    if not install.ok:
        _heal_steps(
            "partition_asymmetric",
            drop_side,
            [(drop_side, ingress_qdisc_del_cmd(dev)), (drop_side, netem_del_cmd(dev))],
        )
        raise FaultInjectionError(
            f"partition_asymmetric({from_container} -> {to_container}, {direction.value}): "
            f"tc rejected the rule: {install.stderr.strip() or install.stdout.strip()}"
        )

    # Counting rule on the surviving direction (`to -> from`): counts without
    # dropping, so "the other direction still arrives" is a measurement.
    exec_in(from_container, ingress_qdisc_add_cmd(dev))
    exec_in(
        from_container,
        ingress_filter_add_cmd(to_ip, prio=count_prio, action="pass", dev=dev),
    )

    drop_before = verify_filter_present(
        read_drop_filters(), target=drop_side, ip=drop_key_ip, prio=drop_prio
    )
    count_before = verify_filter_present(
        read_ingress_filters(from_container, dev=dev),
        target=from_container,
        ip=to_ip,
        prio=count_prio,
    )

    # Drive traffic through both directions so the counters have something to
    # count, then require both to have advanced.
    blocked_rc = _generate_probe_traffic(from_container, to_container, attempts=2)
    _generate_probe_traffic(to_container, from_container, attempts=2)
    verify_probe_blackholed(target=from_container, rc=blocked_rc, peer=to_container)

    dropped = verify_packets_matched(
        drop_before,
        verify_filter_present(
            read_drop_filters(), target=drop_side, ip=drop_key_ip, prio=drop_prio
        ),
        target=drop_side,
        what=f"{direction.value} DROP rule for {drop_key_ip}",
    )
    delivered = verify_packets_matched(
        count_before,
        verify_filter_present(
            read_ingress_filters(from_container, dev=dev),
            target=from_container,
            ip=to_ip,
            prio=count_prio,
        ),
        target=from_container,
        what=f"counting rule for {to_ip} (surviving direction)",
    )

    scope_peer = next(
        (node for node in NODES if node not in {from_container, to_container}),
        None,
    )
    if scope_peer is not None:
        scope_rc, _ = probe_peer(from_container, scope_peer, timeout_s=2.0)
        verify_probe_reachable(target=from_container, rc=scope_rc, peer=scope_peer)

    handle = AsymmetricHandle(
        from_container=from_container,
        to_container=to_container,
        direction=direction,
        drop_side=drop_side,
        dropped_packets=dropped,
        delivered_packets=delivered,
    )
    opened_at = time.monotonic()
    _emit(
        "fault.open",
        "partition_asymmetric",
        f"{from_container}->{to_container}",
        {
            "detail": {
                "direction": direction.value,
                "drop_side": drop_side,
                "dropped_packets": dropped,
                "delivered_packets": delivered,
                "scope_peer_reachable": scope_peer,
            }
        },
    )
    try:
        yield handle
    finally:
        steps: list[tuple[str, str]] = [
            (from_container, ingress_filter_del_cmd(prio=count_prio, dev=dev)),
        ]
        if inbound:
            steps.append((to_container, ingress_filter_del_cmd(prio=drop_prio, dev=dev)))
        else:
            steps.append((from_container, netem_del_cmd(dev)))
        failures = _heal_steps("partition_asymmetric", drop_side, steps)
        # Leave no empty ingress qdisc behind, but only once our own filters are
        # gone and nothing else is using it — another concurrent partition on the
        # same container keeps its filters under the same qdisc.
        for container in {from_container, drop_side}:
            drop_ingress_qdisc_if_empty(container, dev=dev)
        residue = list(failures)
        try:
            verify_filter_absent(read_drop_filters(), target=drop_side, ip=drop_key_ip)
            restored_rc, _ = probe_peer(from_container, to_container, timeout_s=3.0)
            verify_probe_reachable(target=from_container, rc=restored_rc, peer=to_container)
        except FaultEffectNotObserved as exc:
            residue.append(str(exc))
        _emit(
            "fault.close",
            "partition_asymmetric",
            f"{from_container}->{to_container}",
            {"held_ms": round((time.monotonic() - opened_at) * 1000), "residue": residue},
        )


@contextmanager
def _stop_pg_processes(
    container: str,
    titles: Sequence[str],
    required_all: Sequence[str],
    required_any: Sequence[str],
    duration_s: float,
    primitive: str,
) -> Iterator[StoppedProcessHandle]:
    """Shared machinery for the two signal-stall primitives.

    Selects PG auxiliary processes by title, SIGSTOPs them, proves every one
    reached state ``T`` while something else in the container kept running,
    holds for at least `duration_s`, then SIGCONTs and proves none is stopped.

    `required_all` must all be present; `required_any`, when non-empty, needs at
    least one — that is how a primary (``walwriter``) and a standby
    (``walreceiver``) both qualify without either being demanded of the other.
    """
    processes = read_processes(container)
    targets = select_pg_processes(processes, titles)
    found = {process.pg_title or "" for process in targets}
    missing = [name for name in required_all if not any(t.startswith(name) for t in found)]
    if missing:
        raise FaultPreconditionError(
            f"{primitive}({container}): PG auxiliary process(es) {missing} not running. "
            "Every healthy postmaster has them, so PG is down or still starting; "
            "stalling nothing would look like a passing test."
        )
    if required_any and not any(t.startswith(name) for name in required_any for t in found):
        raise FaultPreconditionError(
            f"{primitive}({container}): none of {list(required_any)} is running, so this "
            "node has no live WAL path to stall. PG is down, still starting, or wedged "
            "before recovery — stalling nothing would look like a passing test."
        )
    pids = [process.pid for process in targets]

    stop = exec_in(container, f"kill -STOP {' '.join(str(pid) for pid in pids)}")
    if not stop.ok:
        raise FaultInjectionError(
            f"{primitive}({container}): kill -STOP failed: {stop.stderr.strip()}"
        )

    after = read_processes(container)
    stopped = verify_processes_stopped(after, target=container, expected_pids=pids)
    verify_scope_unaffected(after, target=container, stopped_pids=pids)

    handle = StoppedProcessHandle(container=container, processes=stopped)
    opened_at = time.monotonic()
    _emit(
        "fault.open",
        primitive,
        container,
        {"detail": {"pids": pids, "titles": handle.titles, "duration_s": duration_s}},
    )
    try:
        yield handle
        remaining = duration_s - (time.monotonic() - opened_at)
        if remaining > 0:
            time.sleep(remaining)
    finally:
        failures = _heal_steps(
            primitive,
            container,
            [(container, f"kill -CONT {' '.join(str(pid) for pid in pids)}")],
        )
        residue = list(failures)
        try:
            verify_processes_resumed(
                read_processes(container), target=container, expected_pids=pids
            )
        except FaultEffectNotObserved as exc:
            residue.append(str(exc))
        _emit(
            "fault.close",
            primitive,
            container,
            {"held_ms": round((time.monotonic() - opened_at) * 1000), "residue": residue},
        )


@contextmanager
def fsync_stall(container: str, duration_s: float) -> Iterator[StoppedProcessHandle]:
    """Stall `container`'s durable-WAL path for at least `duration_s` seconds.

    PROVES: what happens when a node cannot advance its durable WAL position
    while its supervisor, Raft loop and lease tick keep ticking. The whole
    WAL-durability auxiliary set is stopped — checkpointer, walwriter,
    background writer, and walreceiver when the node is a standby. On the
    cluster's *sync* standby this also blocks the leader's commit path, since
    commits wait for that standby's ack, which is the closest a signal can get
    to a hung ``fsync()``.

    DOES NOT PROVE: that a hung ``fsync()`` syscall is survivable. Commit-time
    fsyncs run inside the backend process, which stays running on purpose —
    stopping backends would hang the client rather than the durability path.
    Nothing here touches the kernel or disk-controller path, fsync error (EIO)
    handling is untested, and this cannot produce the "fsync returned success
    without flushing" violation — :func:`crash_losing_unsynced_writes` is the
    primitive that can.

    Distinct from :func:`sigstop_checkpointer`, which stops the checkpointer
    alone and leaves the WAL write path intact.
    """
    with _stop_pg_processes(
        container,
        WAL_DURABILITY_PROCESSES,
        REQUIRED_WAL_PROCESSES,
        REQUIRED_WAL_PATH_PROCESSES,
        duration_s,
        "fsync_stall",
    ) as handle:
        yield handle


@contextmanager
def sigstop_checkpointer(container: str, duration_s: float) -> Iterator[StoppedProcessHandle]:
    """SIGSTOP `container`'s checkpointer for at least `duration_s` seconds.

    PROVES: the "PG is alive but unhealthy" branch of the lease tick.
    Checkpoints stop, so ``pg_wal`` grows without recycling and restart
    recovery lengthens, while the postmaster keeps answering.

    DOES NOT PROVE: that commits stall. With ``synchronous_commit=on`` each
    backend fsyncs WAL itself, so transactions keep committing with the
    checkpointer stopped — use :func:`fsync_stall` for the durability path.
    """
    with _stop_pg_processes(
        container,
        ("checkpointer",),
        ("checkpointer",),
        (),
        duration_s,
        "sigstop_checkpointer",
    ) as handle:
        yield handle


def read_lazyfs_mounted(container: str, mount_dir: str = PG_DATA_DIR) -> bool:
    """Whether `container` has PGDATA on LazyFS right now."""
    result = exec_in(container, lazyfs_mounts_cmd())
    if not result.ok:
        raise read_failure(container, "reading /proc/mounts", result)
    return parse_lazyfs_mounted(result.stdout, mount_dir)


def verify_lazyfs_mounted(
    container: str, mount_dir: str = PG_DATA_DIR, *, timeout_s: float = 0.0
) -> None:
    """Assert `container` runs PGDATA on LazyFS, with the remediation attached.

    Called before every durability fault. Without LazyFS the crash below is an
    ordinary SIGKILL against a filesystem whose un-fsynced writes are held in
    the *host* page cache, which killing a container does not discard — so the
    assertion "every acked write survived" would hold no matter what
    PostgreSQL's durability settings were, and would read as evidence.

    `timeout_s` waits for the mount to appear rather than demanding it already
    has. The entrypoint makes these mounts during startup, so a suite that
    checks the instant the container reports "Started" reads a container that
    is merely early as one that is misconfigured. Waiting weakens nothing: a
    mount that never appears still fails, with the same message.

    A container that cannot be exec'd into is waited through too, and reported
    as such if the wait expires — a restarting node says nothing about mounts.

    The wait belongs here rather than in `docker compose up --wait`. That gate
    depends on compose's health aggregation across services, which reports a
    node still in `health: starting` as unhealthy once a sibling has gone
    healthy, and aborts. This waits on the fact the fault actually needs.
    """
    deadline = time.monotonic() + timeout_s
    unreachable: ContainerNotRunning | None = None
    while True:
        try:
            if read_lazyfs_mounted(container, mount_dir):
                return
            unreachable = None
        except ContainerNotRunning as exc:
            unreachable = exc
        if time.monotonic() >= deadline:
            break
        time.sleep(1.0)
    if unreachable is not None:
        raise FaultPreconditionError(
            f"{container}: never stayed running long enough to read its mounts"
            f"{f' within {timeout_s:g}s' if timeout_s else ''}: {unreachable}.\n"
            f"  The node is restart-looping, so this says nothing about LazyFS. "
            f"Read its logs before treating it as a mount problem:\n"
            f"    docker compose logs {container}"
        ) from unreachable
    raise FaultPreconditionError(
        f"{container}: {mount_dir} is not a LazyFS mount"
        f"{f' after {timeout_s:g}s' if timeout_s else ''}, so un-fsynced writes "
        f"cannot be lost and no durability claim can be tested here.\n"
        f"  Run this suite against docker-compose.lazyfs.yml:\n"
        f"    COMPOSE_FILE=docker-compose.lazyfs.yml docker compose up -d\n"
        f"  That file targets the `runtime-lazyfs` image stage and starts each "
        f"node through the entrypoint that mounts LazyFS at {mount_dir}."
    )


ECHO_WAIT_S: Final[float] = 3.0
"""How long one probe waits for its echo before a fresh one is sent."""


def verify_lazyfs_fault_channel(
    container: str, *, mount: LazyfsMount = LAZYFS_DATA, timeout_s: float = 60.0
) -> None:
    """Assert LazyFS is *consuming* the fault FIFO, not merely accepting writes.

    Writing to the FIFO succeeds whenever LazyFS holds it open, which it does
    from the moment it creates it — before, and independently of, the worker
    thread that reads it. If that thread stalls, every ``lazyfs::`` command
    lands in the pipe buffer and is never executed, the shell write reports
    success, and the durability suite proceeds believing it discarded a cache
    it never touched.

    This is not hypothetical. Setting ``fifo_path_completed`` in the LazyFS
    config parks the worker in ``open(O_WRONLY)`` on a FIFO no process ever
    opens for reading, which is exactly this failure and produced exactly this
    silence.

    So: send a command LazyFS cannot recognise and wait for it to say so.
    """
    deadline = time.monotonic() + timeout_s
    undeliverable = ""
    while True:
        # A fresh nonce per attempt: a container that restarted between the
        # write and the read took the log with it, so the old nonce can never
        # appear and waiting for it would report a parked worker.
        nonce = uuid.uuid4().hex
        probe = exec_in(container, lazyfs_control_cmd(lazyfs_probe_command(nonce), fifo=mount.fifo))
        if probe.ok:
            undeliverable = ""
            echo_deadline = min(deadline, time.monotonic() + ECHO_WAIT_S)
            while time.monotonic() < echo_deadline:
                log = exec_in(container, lazyfs_log_cmd(log=mount.log))
                if log.ok and parse_lazyfs_consumed(log.stdout, nonce):
                    return
                time.sleep(0.2)
        else:
            undeliverable = probe.output.strip() or f"exited {probe.rc} in silence"
            if not exec_undelivered(probe):
                raise FaultPreconditionError(
                    f"{container}: could not write to {mount.fifo}: {probe.output}"
                )
        if time.monotonic() >= deadline:
            break
        time.sleep(0.5)

    if undeliverable:
        raise FaultPreconditionError(
            f"{container}: never became reachable enough to probe {mount.fifo} "
            f"within {timeout_s:g}s: {undeliverable}"
        )

    raise FaultPreconditionError(
        f"{container}: the {mount.name} LazyFS accepted a write to {mount.fifo} but "
        f"never echoed it within {timeout_s:g}s, so its fault worker is not reading "
        f"the FIFO. Every fault command would report success and inject nothing.\n"
        f"  Check {mount.log} in the container for how far startup got, and check "
        f"that `fifo_path_completed` is unset in its config."
    )


def arm_torn_write(
    container: str,
    path: str,
    *,
    parts: int = 2,
    persist: Sequence[int] = (1,),
    mount: LazyfsMount = LAZYFS_DATA,
) -> None:
    """Arm a torn write on `path` and confirm LazyFS accepted the fault.

    Confirms configuration, not injection: the tear fires on the next write to
    `path`, which has not happened yet. Use :func:`verify_torn_write_injected`
    after driving that write.
    """
    verify_lazyfs_fault_channel(container, mount=mount)
    command = lazyfs_torn_op_cmd(path, parts=parts, persist=persist, mount=mount)

    before = _lazyfs_received_count(container, command, mount=mount)
    armed = exec_in(container, lazyfs_control_cmd(command, fifo=mount.fifo))
    if not armed.ok:
        raise FaultInjectionError(f"{container}: could not arm torn-op: {armed.output}")
    _await_lazyfs_received(container, command, before, mount=mount)

    _emit(
        "fault.armed",
        "torn_write",
        container,
        {"path": path, "parts": parts, "mount": mount.name},
    )


def config_torn_op_block(path: str, *, occurrence: int, parts: int, persist: Sequence[int]) -> str:
    """A `[[injection]]` block LazyFS reads at startup, unlike the FIFO form.

    The FIFO pins a torn-op to occurrence 1, so it tears the first write to the
    path after arming and nothing else. For redb that is always the 320-byte
    commit header, because every commit opens with one — which is why the Raft
    torn-write suite had never reached a btree page and why it is worth paying
    a container restart to select a later write.
    """
    persisted = ", ".join(str(part) for part in persist)
    return (
        "\n[[injection]]\n"
        'type = "torn-op"\n'
        f'file = "{path}"\n'
        f"occurrence = {occurrence}\n"
        f"parts = {parts}\n"
        f"persist = [{persisted}]\n"
    )


def arm_config_torn_write(
    container: str,
    path: str,
    *,
    occurrence: int,
    parts: int = 2,
    persist: Sequence[int] = (1,),
    mount: LazyfsMount = LAZYFS_DATA,
) -> None:
    """Bake a torn-op into `container`'s LazyFS config and restart it to load it.

    The restart is the cost of occurrence selection: LazyFS reads injections
    once, at mount. `occurrence` counts writes to `path` from that moment, so it
    has to clear whatever the node writes coming back up — the caller waits for
    the cluster to re-settle before treating the fault as pending.

    Confirms the config was re-read, not that the fault fired: that is
    :func:`await_torn_record`'s job, and the distinction is the same one the
    FIFO form draws between a command accepted and a command executed.
    """
    block = config_torn_op_block(path, occurrence=occurrence, parts=parts, persist=persist)
    quoted = shlex.quote(block)
    appended = exec_in(container, f"printf %s {quoted} >> {mount.config}", as_root=True)
    if not appended.ok:
        raise FaultInjectionError(f"{container}: could not write {mount.config}: {appended.output}")

    restarted = run(f"docker compose restart {container}")
    if not restarted.ok:
        raise FaultInjectionError(f"{container}: could not restart to load the fault")

    # LazyFS reprints its config on every mount, so its absence means the
    # process did not come back and the fault is not armed at all.
    deadline = time.monotonic() + LAZYFS_CONFIG_RELOAD_TIMEOUT_S
    while time.monotonic() < deadline:
        log = exec_in(container, lazyfs_log_cmd(log=mount.log))
        if log.ok and "using a custom config" in log.stdout:
            _emit(
                "fault.armed",
                "config_torn_write",
                container,
                {"path": path, "occurrence": occurrence, "mount": mount.name},
            )
            return
        time.sleep(2)
    raise FaultInjectionError(
        f"{container}: LazyFS did not re-read {mount.config} within "
        f"{LAZYFS_CONFIG_RELOAD_TIMEOUT_S:g}s, so the torn-op is not armed"
    )


def await_torn_record(
    container: str,
    path: str,
    *,
    mount: LazyfsMount = LAZYFS_DATA,
    timeout_s: float,
) -> tuple[int, int] | None:
    """Wait for LazyFS to report tearing a write to `path`; `(bytes, offset)`.

    None on timeout, which the caller must not read as "nothing was torn"
    without saying so: a fault that never fired and a fault whose report could
    not be read are different findings.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        log = exec_in(container, lazyfs_log_cmd(log=mount.log))
        if log.ok:
            records = parse_lazyfs_torn_records(log.stdout, path)
            if records:
                return records[-1]
        time.sleep(3)
    return None


def verify_torn_write_injected(
    container: str,
    path: str,
    *,
    expected_bytes: int | None = None,
    mount: LazyfsMount = LAZYFS_DATA,
) -> int:
    """Assert LazyFS actually tore a write to `path`, and say how much survived.

    A torn-op that was configured but never fired leaves the file whole, which
    is indistinguishable from a passing test unless this is checked. LazyFS
    logs each surviving piece, so the log is the truth source; the file's size
    is not, because a torn write does not change it.
    """
    log = exec_in(container, lazyfs_log_cmd(log=mount.log))
    persisted = parse_lazyfs_torn_bytes(log.stdout, path) if log.ok else None
    if persisted is None:
        raise FaultEffectNotObserved(
            f"{container}: LazyFS never tore a write to {path}. The fault was armed "
            f"and no write to that path followed, so nothing was torn and any "
            f"assertion about torn-write handling below is vacuous."
        )
    if expected_bytes is not None and persisted != expected_bytes:
        raise FaultEffectNotObserved(
            f"{container}: torn write to {path} persisted {persisted} bytes, "
            f"expected {expected_bytes}"
        )
    return persisted


def _lazyfs_received_count(
    container: str, command: str, *, mount: LazyfsMount = LAZYFS_DATA
) -> int:
    """How many times `container`'s LazyFS has logged receiving `command`."""
    log = exec_in(container, lazyfs_log_cmd(log=mount.log))
    return count_lazyfs_received(log.stdout, command) if log.ok else 0


def _await_lazyfs_received(
    container: str,
    command: str,
    before: int,
    *,
    mount: LazyfsMount = LAZYFS_DATA,
    timeout_s: float = 10.0,
) -> None:
    """Block until LazyFS logs receiving `command` more often than `before`."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _lazyfs_received_count(container, command, mount=mount) > before:
            return
        time.sleep(0.2)
    raise FaultEffectNotObserved(
        f"{container}: LazyFS never logged receiving {command!r} within {timeout_s:g}s, "
        f"so the command was written to the FIFO but not executed."
    )


def lazyfs_unsynced_bytes(container: str, path: str) -> int:
    """Size of `path` as the *backing store* has it, in bytes.

    Reads through ``LAZYFS_ROOT_DIR`` rather than the mount, and the
    distinction is the whole point: the mount serves un-fsynced data out of
    LazyFS's cache and would report it as present. Only the backing store
    knows what a power loss would leave behind.
    """
    rel = path[len(PG_DATA_DIR) :].lstrip("/") if path.startswith(PG_DATA_DIR) else path
    result = exec_in(container, f"stat -c %s {LAZYFS_ROOT_DIR}/{rel} 2>/dev/null || echo -1")
    return int(result.stdout.strip() or -1)


@contextmanager
def crash_losing_unsynced_writes(
    containers: Sequence[str], *, mount_dir: str = PG_DATA_DIR
) -> Iterator[None]:
    """SIGKILL every named container after discarding writes it never fsynced.

    PROVES: whether an acknowledged write was durable at the moment it was
    acknowledged. LazyFS holds un-fsynced writes in its own userspace cache;
    ``clear-cache`` discards exactly those, and the SIGKILL that follows takes
    the process with the rest of the cache. What remains in the backing store
    is precisely what PostgreSQL flushed. This is the only fault in this module
    that can produce the "fsync returned success without flushing" violation,
    and therefore the only one that can turn W1 and R2 from asserted into
    demonstrated.

    The Raft store is included. It has its own LazyFS instance, and both
    caches are discarded before anything is killed, so redb's un-fsynced writes
    die here exactly as PostgreSQL's do. That was not true while the Raft store
    sat on an ordinary filesystem: it kept its un-fsynced writes across this
    fault, and openraft's storage conformance suite carried the durability
    pins alone.

    DOES NOT PROVE: torn-write handling. Pages here are lost whole, never
    half-written. That is :func:`arm_torn_write`'s job, and H-25's.

    Every named container is discarded before any is killed. Killing them one
    at a time would let the survivors keep accepting writes and re-replicating
    through the gap, so a whole-cluster crash would quietly degrade into a
    rolling restart — which the repo already covers and which proves nothing
    about fsync.

    The containers are left dead. The caller restarts them, because what a
    restart recovers is the measurement.

    Everything between the caller's last write and the SIGKILL is inside the
    window the fault is trying to measure, so this does the least it can. In
    particular it does not run the nonce round-trip of
    :func:`verify_lazyfs_fault_channel`: waiting for LazyFS to log receiving
    the `clear-cache` proves the same thing more directly, by confirming the
    command that matters rather than a proxy for it. The round-trips that
    check bought several seconds of `docker exec` latency, and with
    `synchronous_commit=off` a few seconds is long enough for a backend to
    flush WAL under buffer pressure and make writes durable that the inversion
    needs to lose.
    """
    targets = list(containers)
    if not targets:
        raise FaultPreconditionError("crash_losing_unsynced_writes needs at least one container")

    for target in targets:
        verify_lazyfs_mounted(target, mount_dir)

    # Resolved once, before anything dies. `container_id` goes through
    # `docker compose ps -q`, which returns nothing while a container is
    # between incarnations — so re-resolving after the kill races the restart
    # policy and fails on a fault that landed perfectly.
    ids = {target: container_id(target) for target in targets}

    joined = ",".join(targets)
    _emit("fault.begin", "crash_losing_unsynced_writes", joined, {"mount_dir": mount_dir})

    # The compose files set `restart: unless-stopped`, which is right for
    # production and wrong here: Docker would restart the first victim while
    # the last was still being killed, so the cluster would never actually be
    # simultaneously dead and a whole-cluster crash would degrade into a
    # rolling restart. Suspended for the window, restored in `finally`.
    for target, cid in ids.items():
        policy = run(f"docker update --restart=no {cid}")
        if not policy.ok:
            raise FaultInjectionError(
                f"{target}: could not suspend restart policy: {policy.output}"
            )

    try:
        for target in targets:
            # Both instances. A power loss does not discard PGDATA's un-fsynced
            # writes and spare the Raft store's, and clearing only one would
            # leave redb's cache to be destroyed by the SIGKILL alone -- true
            # here, but true by accident rather than by the fault.
            for mount in (LAZYFS_DATA, LAZYFS_RAFT):
                before = _lazyfs_received_count(target, "lazyfs::clear-cache", mount=mount)
                discard = exec_in(
                    target, lazyfs_control_cmd("lazyfs::clear-cache", fifo=mount.fifo)
                )
                if not discard.ok:
                    raise FaultInjectionError(
                        f"{target}: could not write clear-cache to {mount.fifo}: {discard.output}"
                    )
                _await_lazyfs_received(target, "lazyfs::clear-cache", before, mount=mount)

        # SIGKILL, never `docker stop`. A graceful stop gives LazyFS a chance
        # to unmount, and unmounting flushes — which would quietly write out
        # the very data the fault exists to destroy.
        for target, cid in ids.items():
            kill = run(f"docker kill --signal=KILL {cid}")
            if not kill.ok:
                raise FaultInjectionError(f"{target}: SIGKILL failed: {kill.output}")

        for target, cid in ids.items():
            _await_container_id_status(target, cid, "exited")

        _emit("fault.injected", "crash_losing_unsynced_writes", joined, {"mount_dir": mount_dir})
        yield
    finally:
        for cid in ids.values():
            run(f"docker update --restart=unless-stopped {cid}")
        _emit("fault.end", "crash_losing_unsynced_writes", joined, {"mount_dir": mount_dir})


def _await_container_id_status(
    service: str, container: str, expected: str, *, timeout_s: float = 60.0
) -> None:
    """Poll a container *by id* until it reports `expected`.

    By id rather than by compose service, because compose cannot name a
    container that is between incarnations, and this is called precisely when
    that is true.
    """
    deadline = time.monotonic() + timeout_s
    status = "unknown"
    while time.monotonic() < deadline:
        probe = run(f"docker inspect -f '{{{{.State.Status}}}}' {container}")
        status = probe.stdout.strip()
        if status == expected:
            return
        time.sleep(0.5)
    raise FaultEffectNotObserved(
        f"{service} ({container}) reported {status!r}, expected {expected!r} within {timeout_s:g}s"
    )


def start_containers_by_id(ids: Mapping[str, str], *, timeout_s: float = 60.0) -> None:
    """Start each stopped container by id, and prove each one came back.

    By id, not by compose service: `docker compose ps -q` reports nothing for a
    container that is not running, which is exactly the state every container
    handed to this is in. The ids must therefore have been resolved before the
    fault, which is what :func:`crash_losing_unsynced_writes` returns them for.

    The wait is the point. `docker start` returns as soon as the daemon has
    accepted the request, so a caller that treats exit 0 as "the node is back"
    goes on to measure recovery against a container that has not started —
    reading a timeout as lost data rather than as a harness that did not wait.
    """
    if not ids:
        raise FaultPreconditionError("start_containers_by_id needs at least one container")

    joined = ",".join(ids)
    _emit("fault.begin", "start_containers_by_id", joined, {})
    for service, container in ids.items():
        started = run(f"docker start {container}")
        if not started.ok:
            raise FaultInjectionError(f"{service}: restart failed: {started.output}")
    for service, container in ids.items():
        _await_container_id_status(service, container, "running", timeout_s=timeout_s)
    _emit("fault.injected", "start_containers_by_id", joined, {})


def _lifecycle(service: str, verb: str) -> CommandResult:
    """Run a docker lifecycle verb against `service`'s resolved container."""
    result = run(f"docker {verb} {container_id(service)}")
    if not result.ok:
        raise FaultInjectionError(f"docker {verb} {service} failed: {result.stderr.strip()}")
    return result


def kill_container(service: str) -> None:
    """SIGKILL `service`, and prove the incarnation was replaced.

    Note this is a *clean* fault: `docker kill` leaves the host page cache
    intact, so it says nothing about whether fsync was honoured.
    """
    before = read_container_runstate(service)
    _lifecycle(service, "kill")
    _emit("fault.inject", "kill_container", service, {})
    after = _wait_for_status(service, "exited")
    if before is not None and after is not None and before.status != "running":
        raise FaultPreconditionError(
            f"kill_container: {service} was {before.status!r}, not running; "
            "killing an already-dead container is a silent no-op"
        )


def start_container(service: str) -> None:
    """Start `service` and wait until docker reports it running."""
    _lifecycle(service, "start")
    _emit("fault.heal", "start_container", service, {})
    _wait_for_status(service, "running")


def restart_container(service: str) -> None:
    """Restart `service`, proving it came back as a new incarnation."""
    before = read_container_runstate(service)
    _lifecycle(service, "restart")
    _emit("fault.inject", "restart_container", service, {})
    after = _wait_for_status(service, "running")
    verify_incarnation_changed(before, after, target=service, action="restart_container")


def pause_container(service: str) -> None:
    """SIGSTOP every process in `service` via the freezer cgroup."""
    _lifecycle(service, "pause")
    _emit("fault.inject", "pause_container", service, {})
    _wait_for_status(service, "paused")


def unpause_container(service: str) -> None:
    """Thaw a paused `service`."""
    _lifecycle(service, "unpause")
    _emit("fault.heal", "unpause_container", service, {})
    _wait_for_status(service, "running")


def _wait_for_status(
    service: str, expected: str, *, timeout_s: float = 30.0
) -> ContainerRunState | None:
    """Poll until `service` reports `expected`, then return that state.

    Docker's lifecycle verbs return before the state settles, so asserting
    immediately races the daemon and fails on a fault that did land.
    """
    deadline = time.monotonic() + timeout_s
    state = read_container_runstate(service)
    while time.monotonic() < deadline:
        if state is not None and state.status == expected:
            return state
        time.sleep(0.25)
        state = read_container_runstate(service)
    verify_status(state, target=service, expected=expected)
    return state


def find_raft_leader(nodes: Sequence[str] = NODES) -> str | None:
    """Whom the management API names as leader, or ``None`` if nobody answers.

    Useful for "who should I attack", but NOT a leaderless oracle: the endpoint
    serves the last leader the node knows about, so peers keep naming a dead
    leader for the whole failover. Use :func:`find_lease_holder` for that.
    """
    for node in nodes:
        port = MGMT_PORTS.get(node)
        if port is None:
            continue
        result = run(
            f"curl -sf --max-time 2 http://localhost:{port}/api/v1/cluster/leader",
            timeout_s=5.0,
        )
        if not result.ok:
            continue
        info = api_models.parse_or_none(api_models.LeaderInfo, result.stdout)
        if info is not None and info.leader_id is not None and 1 <= info.leader_id <= len(NODES):
            return NODES[info.leader_id - 1]
    return None


def parse_prometheus_metric(text: str, name: str) -> float | None:
    """Read a single unlabelled sample out of a Prometheus exposition body."""
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        head, _, value = line.partition(" ")
        if head == name or head.startswith(f"{name}{{"):
            with suppress(ValueError):
                return float(value.strip())
    return None


def read_metric(node: str, name: str) -> float | None:
    """Scrape one metric from `node`; ``None`` if the node or metric is absent."""
    port = METRICS_PORTS.get(node)
    if port is None:
        return None
    result = run(f"curl -sf --max-time 2 http://localhost:{port}/metrics", timeout_s=5.0)
    if not result.ok:
        return None
    return parse_prometheus_metric(result.stdout, name)


def find_lease_holder(nodes: Sequence[str] = NODES) -> str | None:
    """The node that currently holds valid write authority, if any.

    ``None`` means the cluster is inside a failover window: no node may accept
    writes. This is the edge :func:`clock_skew_at_lease_boundary` aims at, and
    it is read from each node's own ``pgbattery_has_lease`` gauge rather than
    from the management API's cached leader.
    """
    for node in nodes:
        if read_metric(node, LEASE_METRIC) == 1.0:
            return node
    return None


def find_raft_leader_by_metric(nodes: Sequence[str] = NODES) -> str | None:
    """The node reporting Raft leadership about itself.

    A node can hold Raft leadership while its PostgreSQL is still un-promoted;
    that gap is the promotion hold-down, so this is how the winner is identified
    while the window is still open.
    """
    for node in nodes:
        if read_metric(node, RAFT_LEADER_METRIC) == 1.0:
            return node
    return None


def _wait_for(
    predicate: Callable[[], str | None],
    *,
    timeout_s: float,
    poll_interval_s: float,
) -> tuple[str | None, float]:
    """Poll `predicate` until it reports a value, returning it and the instant."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = predicate()
        if value is not None:
            return value, time.monotonic()
        time.sleep(poll_interval_s)
    return None, time.monotonic()


@contextmanager
def clock_skew_at_lease_boundary(
    container: str | None = None,
    skew_ms: int | None = None,
    window_ms: int = 100,
    *,
    aim: Aim = Aim.HOLDDOWN_START,
    trigger: Callable[[], None] | None = None,
    timings: SystemTimings | None = None,
    settle_ms: int | None = None,
    timeout_s: float = 30.0,
    repo_root: Path | None = None,
) -> Iterator[LeaseBoundarySkew]:
    """Step a node's wall clock inside the live promotion hold-down window.

    PROVES: whether a decision the lease guards is taken on the monotonic clock
    or on the wall clock. ``App::promote_local_postgres`` withholds promotion
    until one full lease duration has passed since the locally-observed
    leaderless edge. That hold-down now measures with ``Instant`` and
    ``checked_duration_since(...).unwrap_or(ZERO)``
    (``promotion_lease_holddown``, ``src/app.rs``), and ``failover_started_at``
    is an ``Instant`` too — so a wall-clock step of either sign cannot shorten
    it. It was not always so: the guard once compared ``unix_now_ms()`` against
    a wall-clock anchor while the lease it stood in for was monotonic, and a
    forward step of one lease duration applied to the election winner inside
    that window satisfied the guard while the deposed leader's lease was still
    genuinely valid — two writable primaries. This primitive is what keeps that
    conversion honest, so it is a regression guard rather than a live exploit.

    libfaketime is ``LD_PRELOAD``ed into pgbattery, so the step reaches every
    wall-clock read the process makes. The path that still reads the wall clock
    is the LSN staleness filter (``unix_now_secs`` in ``state_machine.rs``): a
    backward step makes every recorded LSN timestamp look future-dated, and the
    filter caps those at age 0 rather than dropping them, which keeps the
    election gate strict instead of collapsing it to the bootstrap-permissive
    fallback. ``pgbattery_lsn_future_skew_total`` is how that path reports it
    was reached.

    ``skew_ms`` defaults to exactly one lease duration, the magnitude that used
    to be sufficient; sweep it — both signs — with
    ``sweep_around(timings.lease_duration_ms)``. ``clock_skew_sweep.py`` drives
    that sweep with the dual-writability prober attached.

    Timing comes from the system's own constants, not from sleeps: the window is
    ``[lease_edge, lease_edge + lease_duration]`` with ``lease_duration`` read via
    :func:`read_system_timings`, and polling runs at ``LEASE_CHECK_INTERVAL``.
    The edge is taken from ``pgbattery_has_lease`` going to zero cluster-wide,
    not from the management API's leader endpoint — that endpoint serves the last
    leader it knew about, so it never reports a leaderless cluster (measured: it
    named a killed node as leader for the whole 2.5 s failover).

    The winner is identified by its own ``pgbattery_raft_is_leader`` gauge, which
    goes to 1 while its PostgreSQL is still un-promoted; that gap *is* the
    hold-down. The winner's ``pgbattery_promotion_lease_holddowns`` counter is
    sampled either side of the window, so a caller can assert whether the
    hold-down engaged at all or was skipped.

    Args:
        container: node to skew. ``None`` waits for the winner to identify
            itself, which only lands inside the window on a cluster whose
            election completes in under one lease duration; when it does not,
            the primitive raises rather than pretending. Naming a candidate up
            front is the reliable mode for hitting the window — the skew applies
            to the node that runs the hold-down if that candidate wins, and
            ``holddown_engaged`` on the handle reports whether it did, so a
            caller can retry or skew every candidate rather than read an
            inconclusive run as a pass.
        skew_ms: forward step in milliseconds; defaults to one lease duration.
        window_ms: targeting tolerance around the aim point.
        aim: see :class:`Aim`.
        trigger: optional failover trigger, run in a daemon thread on entry so
            one call performs the whole experiment.
        settle_ms: delay from the lease edge before skewing a named container,
            defaulting to one election timeout. Load-bearing: the guard compares
            ``unix_now_ms()`` against ``failover_started_at_unix_ms``, which the
            governor writes on its own leader-loss edge. A skew applied *before*
            that write is baked into the recorded value too and cancels out, so
            the injection has to follow it. One election timeout is the derived
            upper bound on how long a voter takes to observe leader loss, and it
            still leaves most of the lease-length window.
        timeout_s: how long to wait for the failover window to open.

    Raises:
        FaultPreconditionError: no failover window appeared, or no node took
            leadership inside it.
        FaultEffectNotObserved: the container clock did not move as requested
            (libfaketime inactive or literal misparsed), or the skew landed
            outside the window and therefore proves nothing about the boundary.
    """
    resolved = timings or read_system_timings(repo_root)
    lease_ms = resolved.lease_duration_ms
    skew = lease_ms if skew_ms is None else skew_ms
    poll_s = resolved.lease_check_interval_ms / 1000.0

    if trigger is not None:
        threading.Thread(target=trigger, daemon=True).start()

    # The edge is the *observation* that nobody holds write authority. Re-polling
    # after the wait would race the next election and lose the edge, so the
    # verdict comes from what the wait itself saw.
    edge, edge_at = _wait_for(
        lambda: "no-lease-holder" if find_lease_holder() is None else None,
        timeout_s=timeout_s,
        poll_interval_s=poll_s,
    )
    if edge is None:
        raise FaultPreconditionError(
            f"clock_skew_at_lease_boundary: no node lost its lease within {timeout_s}s "
            f"({LEASE_METRIC} never went to zero cluster-wide). The window this "
            "primitive aims at only exists during a failover — pass trigger= or arm "
            "one concurrently."
        )

    target = container
    if target is None:
        # Wait for the winner, bounded by the whole failover rather than by the
        # hold-down: openraft's election can take up to two election timeouts on
        # top of the leaderless watchdog, and the hold-down only starts once the
        # winner's reconcile loop runs.
        winner, _ = _wait_for(
            find_raft_leader_by_metric,
            timeout_s=timeout_s,
            poll_interval_s=poll_s,
        )
        if winner is None:
            raise FaultPreconditionError(
                f"clock_skew_at_lease_boundary: no node reported {RAFT_LEADER_METRIC}=1 "
                f"within {timeout_s}s, so there was nothing to skew. Pass container= to "
                "target a node unconditionally."
            )
        target = winner
    else:
        # Let the target's governor record failover_started_at_unix_ms first;
        # skewing before that write cancels out. See `settle_ms`.
        settle_s = (resolved.election_timeout_ms if settle_ms is None else settle_ms) / 1000.0
        pause = edge_at + settle_s - time.monotonic()
        if pause > 0:
            time.sleep(pause)
    _resolve_ip(target)
    holddowns_before = read_metric(target, PROMOTION_HOLDDOWN_METRIC)

    if aim is Aim.LEASE_EXPIRY:
        aim_at = edge_at + (lease_ms - window_ms / 2.0) / 1000.0
        pause = aim_at - time.monotonic()
        if pause > 0:
            time.sleep(pause)

    # Difference-of-differences against the host clock: cancels any constant
    # host/VM clock disagreement, so what is left is the injected step.
    local_before_ms = time.time() * 1000.0
    container_before_ms = read_container_unix_ms(target)
    local_mid_ms = time.time() * 1000.0

    write = exec_in(target, faketime_write_cmd(skew), as_root=False)
    if not write.ok:
        raise FaultInjectionError(
            f"clock_skew_at_lease_boundary({target}): could not write {FAKETIME_FILE}: "
            f"{write.stderr.strip()}"
        )
    injected_at = time.monotonic()

    local_after_ms = time.time() * 1000.0
    container_after_ms = read_container_unix_ms(target)
    local_end_ms = time.time() * 1000.0

    host_elapsed_ms = (local_after_ms + local_end_ms) / 2.0 - (local_before_ms + local_mid_ms) / 2.0
    observed_skew_ms = (container_after_ms - container_before_ms) - host_elapsed_ms
    read_jitter_ms = (local_mid_ms - local_before_ms) + (local_end_ms - local_after_ms)
    verify_clock_offset(
        target=target,
        observed_ms=observed_skew_ms,
        expected_ms=skew,
        tolerance_ms=max(window_ms, 250.0) + read_jitter_ms,
    )

    handle = LeaseBoundarySkew(
        container=target,
        aim=aim,
        skew_ms=skew,
        observed_skew_ms=observed_skew_ms,
        lease_ms=lease_ms,
        leaderless_at=edge_at,
        injected_at=injected_at,
        promotion_holddowns_before=holddowns_before,
    )
    offset_ms = handle.offset_into_window_ms
    if aim is Aim.HOLDDOWN_START and offset_ms > lease_ms:
        raise FaultEffectNotObserved(
            f"clock_skew_at_lease_boundary({target}): skew landed {offset_ms:.0f}ms after "
            f"the lease edge, past the {lease_ms}ms hold-down window. The hold-down had "
            "already released on its own, so this skew proves nothing about the boundary. "
            "Election latency on this cluster exceeded the window — aim at a node "
            "explicitly, or widen the workload so the window is reachable."
        )
    if aim is Aim.LEASE_EXPIRY and abs(offset_ms - lease_ms) > window_ms / 2.0 + read_jitter_ms:
        raise FaultEffectNotObserved(
            f"clock_skew_at_lease_boundary({target}): skew landed {offset_ms:.0f}ms after "
            f"the lease edge, outside the {lease_ms}ms +/- {window_ms / 2:.0f}ms target "
            "window; a skew off the boundary proves nothing about it."
        )

    _emit(
        "fault.open",
        "clock_skew_at_lease_boundary",
        target,
        {
            "detail": {
                "aim": aim.value,
                "skew_ms": skew,
                "observed_skew_ms": round(observed_skew_ms),
                "lease_ms": lease_ms,
                "offset_into_window_ms": round(offset_ms),
                "releases_holddown_early": handle.releases_holddown_early,
                "promotion_holddowns_before": holddowns_before,
            }
        },
    )
    try:
        yield handle
    finally:
        handle.promotion_holddowns_after = read_metric(target, PROMOTION_HOLDDOWN_METRIC)
        failures = _heal_steps(
            "clock_skew_at_lease_boundary", target, [(target, faketime_write_cmd(0))]
        )
        residue = list(failures)
        try:
            healed_local_ms = time.time() * 1000.0
            healed_container_ms = read_container_unix_ms(target)
            verify_clock_offset(
                target=target,
                observed_ms=healed_container_ms - healed_local_ms,
                expected_ms=0,
                tolerance_ms=max(window_ms, 1000.0) + abs(observed_skew_ms) * 0.0,
            )
        except FaultError as exc:
            residue.append(str(exc))
        _emit(
            "fault.close",
            "clock_skew_at_lease_boundary",
            target,
            {
                "held_ms": round((time.monotonic() - injected_at) * 1000),
                "residue": residue,
                "promotion_holddowns_after": handle.promotion_holddowns_after,
                "holddown_engaged": handle.holddown_engaged,
            },
        )


@contextmanager
def disk_full_during_wal(
    container: str,
    *,
    filler_path: str = FILLER_PATH,
    max_fs_bytes: int = MAX_BOUNDED_FS_BYTES,
) -> Iterator[DiskFullHandle]:
    """Exhaust `container`'s state filesystem down to less than one WAL segment.

    PROVES: the ENOSPC class at exactly the interesting point — WAL segment
    allocation. The fill is measured, not blanket: free space is driven to half
    a WAL segment, so ordinary small writes still succeed but rolling to the
    next WAL file cannot. The effect is proven by *attempting* a
    segment-sized allocation and requiring it to fail, not by assuming the fill
    was big enough.

    DEPENDS ON the bounded volume variant. Docker's ``local`` volume driver has
    no quota, so the default named volumes are as large as the host filesystem
    and a fill could never reach ENOSPC there — that is exactly the vacuous
    ``fallocate 500M`` test this replaces. Against an unbounded filesystem the
    primitive refuses rather than filling a host disk; recreate the cluster with
    ``PGBATTERY_STATE_SUFFIX=_bounded`` (see ``docker-compose.yml``).

    Raises:
        FaultPreconditionError: the filesystem is unbounded, PG cannot report
            its segment size, or the filesystem is already nearly full.
        FaultInjectionError: neither ``fallocate`` nor ``dd`` could allocate.
        FaultEffectNotObserved: space remains for another WAL segment.
    """
    usage_before = read_disk_usage(container)
    verify_bounded_filesystem(usage_before, target=container, max_bytes=max_fs_bytes)

    segment_bytes = read_wal_segment_bytes(container)
    segment_kb = segment_bytes // 1024
    residual_kb = segment_kb // 2
    fill_kb = usage_before.avail_kb - residual_kb
    if fill_kb <= 0:
        raise FaultPreconditionError(
            f"disk_full_during_wal({container}): only {usage_before.avail_kb} KiB free on "
            f"{usage_before.mount}, already below the {residual_kb} KiB target residual. "
            "Nothing to inject — clean up the filesystem first."
        )

    fill = exec_in(container, fallocate_cmd(filler_path, fill_kb), timeout_s=120.0)
    if not fill.ok:
        exec_in(container, f"rm -f {filler_path}")
        fill = exec_in(container, dd_fill_cmd(filler_path, fill_kb // 1024), timeout_s=300.0)
        if not fill.ok and "No space left" not in fill.output:
            raise FaultInjectionError(
                f"disk_full_during_wal({container}): could not allocate the filler: "
                f"{fill.output.strip()}"
            )

    usage_after = read_disk_usage(container)
    verify_out_of_space(usage_after, target=container, need_bytes=segment_bytes)

    # The load-bearing assertion: one more WAL segment must not fit.
    probe_path = f"{filler_path}.segment_probe"
    probe = exec_in(container, fallocate_cmd(probe_path, segment_kb))
    exec_in(container, f"rm -f {probe_path}")
    if probe.ok:
        exec_in(container, f"rm -f {filler_path}")
        raise FaultEffectNotObserved(
            f"disk_full_during_wal({container}): a {segment_kb} KiB WAL segment still "
            f"allocated successfully on {usage_after.mount} with "
            f"{usage_after.avail_kb} KiB reported free — the filesystem is not out of "
            "space and the fault would have passed vacuously."
        )

    handle = DiskFullHandle(
        container=container,
        filler_path=filler_path,
        filled_kb=fill_kb,
        wal_segment_bytes=segment_bytes,
        usage_before=usage_before,
        usage_after=usage_after,
    )
    opened_at = time.monotonic()
    _emit(
        "fault.open",
        "disk_full_during_wal",
        container,
        {
            "detail": {
                "mount": usage_after.mount,
                "fs_total_kb": usage_after.total_kb,
                "filled_kb": fill_kb,
                "avail_kb_after": usage_after.avail_kb,
                "wal_segment_bytes": segment_bytes,
                "segment_alloc_rc": probe.rc,
            }
        },
    )
    try:
        yield handle
    finally:
        failures = _heal_steps(
            "disk_full_during_wal",
            container,
            [(container, f"rm -f {filler_path} {probe_path}")],
        )
        residue = list(failures)
        try:
            verify_space_restored(
                read_disk_usage(container), target=container, need_bytes=segment_bytes
            )
        except FaultError as exc:
            residue.append(str(exc))
        _emit(
            "fault.close",
            "disk_full_during_wal",
            container,
            {"held_ms": round((time.monotonic() - opened_at) * 1000), "residue": residue},
        )


# ─────────────────────────────────────────────────────────────────────────────
# Whole-environment scrub
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ScrubReport:
    """What :func:`scrub` removed and what it could not."""

    containers: list[str] = field(default_factory=list)
    residue: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.residue


def scrub(containers: Sequence[str] = NODES, *, verify: bool = True) -> ScrubReport:
    """Remove every fault this module (or its two predecessors) can leave behind.

    Idempotent and safe on a healthy cluster: netem root qdisc, ingress qdisc
    (which takes all its filters with it), libfaketime offset, both filler
    paths, and — unlike ``linearizability_register.scrub_chaos_residue`` — any
    postgres process a crashed test left in state ``T``. A stopped checkpointer
    otherwise survives the whole run and silently poisons every later case.

    Args:
        containers: nodes to clean.
        verify: re-read each node afterwards and raise if residue remains.

    Raises:
        FaultEffectNotObserved: `verify` is set and something survived.
    """
    report = ScrubReport(containers=list(containers))
    for container in containers:
        if container not in NODE_IPS:
            report.residue.append(f"unknown container {container!r}")
            continue
        exec_in(container, netem_del_cmd())
        exec_in(container, ingress_qdisc_del_cmd())
        exec_in(container, faketime_write_cmd(0), as_root=False)
        exec_in(container, f"rm -f {FILLER_PATH} {FILLER_PATH}.segment_probe {LEGACY_FILLER_PATH}")
        with suppress(FaultError):
            stopped = [p.pid for p in read_processes(container) if p.is_stopped]
            if stopped:
                exec_in(container, f"kill -CONT {' '.join(str(pid) for pid in stopped)}")

    if verify:
        for container in containers:
            if container not in NODE_IPS:
                continue
            report.residue.extend(_residue_for(container))
        if report.residue:
            _emit("fault.scrub", "scrub", ",".join(containers), {"residue": report.residue})
            raise FaultEffectNotObserved(
                "scrub left residue behind:\n  " + "\n  ".join(report.residue)
            )
    _emit("fault.scrub", "scrub", ",".join(containers), {"residue": report.residue})
    return report


def _residue_for(container: str) -> list[str]:
    """Anything fault-shaped still present on `container`."""
    residue: list[str] = []
    qdiscs = read_qdiscs(container)
    if parse_netem(qdiscs) is not None:
        residue.append(f"{container}: netem qdisc still installed")
    filters = read_ingress_filters(container)
    if parse_tc_filters(filters):
        residue.append(f"{container}: ingress filters still installed")
    offset = exec_in(container, f"cat {FAKETIME_FILE}", as_root=False)
    if offset.ok and offset.stdout.strip() not in {"+0s", "+0.000s", ""}:
        residue.append(f"{container}: faketime offset is {offset.stdout.strip()}")
    for path in (FILLER_PATH, LEGACY_FILLER_PATH):
        if exec_in(container, f"test -e {path}").ok:
            residue.append(f"{container}: filler {path} still present")
    with suppress(FaultError):
        stopped = [p.args.strip() for p in read_processes(container) if p.is_stopped]
        if stopped:
            residue.append(f"{container}: processes still stopped: {stopped}")
    return residue


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

app = typer.Typer(
    add_completion=False,
    help="Fault-injection primitives: inject, verify the effect landed, heal.",
)
console = Console()

_PRIMITIVE_SUMMARY: Final[tuple[tuple[str, str], ...]] = (
    ("fsync_stall", "stop the WAL-durability aux set; durable LSN cannot advance"),
    ("partition_lossy", "tc netem combined packet loss + latency (+ jitter)"),
    ("partition_asymmetric", "one-directional packet drop, inbound or outbound"),
    ("clock_skew_at_lease_boundary", "wall-clock step inside the live promotion hold-down"),
    ("sigstop_checkpointer", "SIGSTOP the checkpointer; PG alive but not checkpointing"),
    ("disk_full_during_wal", "ENOSPC at the next WAL segment (needs a bounded volume)"),
)


@app.command()
def list_primitives() -> None:
    """Print the primitive surface and what each one proves."""
    for name, summary in _PRIMITIVE_SUMMARY:
        console.print(f"  [bold]{name}[/] — {summary}")
    console.print("\nSee the module docstring for the full proves / does-not-prove list.")


@app.command()
def timings() -> None:
    """Print the system timing constants fault durations are derived from."""
    resolved = read_system_timings()
    for key, value in resolved.as_dict().items():
        console.print(f"  {key} = {value}")
    console.print(
        f"\n  lease sweep (ms): {sweep_around(resolved.lease_duration_ms)}"
        f"\n  election sweep (ms): {sweep_around(resolved.election_timeout_ms)}"
    )


@app.command(name="scrub")
def scrub_command(
    nodes: str = typer.Option(",".join(NODES), help="Comma-separated container names."),
) -> None:
    """Remove fault residue from every node, then verify none survived."""
    targets = [node.strip() for node in nodes.split(",") if node.strip()]
    try:
        report = scrub(targets)
    except FaultEffectNotObserved as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(code=1) from exc
    console.print(f"[green]scrubbed {', '.join(report.containers)}; no residue[/]")


@app.command()
def preflight(
    nodes: str = typer.Option(",".join(NODES), help="Comma-separated container names."),
) -> None:
    """Report which primitives the current substrate can actually support."""
    targets = [node.strip() for node in nodes.split(",") if node.strip()]
    failures = 0
    for container in targets:
        console.print(f"[bold]{container}[/]")
        tc_ok = exec_in(container, "command -v tc").ok
        faketime_ok = exec_in(container, f"test -e {FAKETIME_FILE}", as_root=False).ok
        usage = read_disk_usage(container)
        bounded = usage.total_bytes <= MAX_BOUNDED_FS_BYTES
        console.print(f"  tc (NET_ADMIN as root): {'yes' if tc_ok else 'NO'}")
        console.print(f"  libfaketime file present: {'yes' if faketime_ok else 'NO'}")
        console.print(
            f"  state fs {usage.mount}: {usage.total_bytes / 1024**3:.1f} GiB "
            f"({'bounded' if bounded else 'UNBOUNDED - disk_full_during_wal will refuse'})"
        )
        if not (tc_ok and faketime_ok):
            failures += 1
    if failures:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
