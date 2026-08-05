#!/usr/bin/env -S uv run --project testing python
"""Dirty-crash durability oracle — H-24, contracts W1 and R2.

Every other fault in this repo is a *clean* crash. ``docker kill`` sends
SIGKILL, the container dies, and every byte PostgreSQL wrote but never flushed
is still sitting in the *host* page cache, which nothing here discards. Restart
the node and the data is all there — whatever its durability settings were. So
the existing suites cannot distinguish a cluster that honours fsync from one
that lies about it, and W1 ("an acknowledged write survives every supported
fault") and R2 ("no ack before the write is flushed to the standby's WAL") have
rested on how the code is built rather than on evidence.

This harness closes that. It runs against ``docker-compose.lazyfs.yml``, where
PGDATA is a LazyFS mount: un-fsynced writes live in LazyFS's own userspace
cache, and both ``lazyfs::clear-cache`` and the SIGKILL that follows destroy
them for real. What survives in the backing store is exactly what PostgreSQL
flushed before it acknowledged.

Two modes, because they prove different contracts:

``leader-crash``
    Crash the leader alone. The cluster must not lose an acked write, because
    sync replication should have put it on a standby's disk before the ack.
    This is W1 under a dirty crash. It does not isolate the leader's own fsync
    behaviour: a surviving standby can cover for a leader that lied.

``cluster-crash``
    Crash all three nodes at once. Nothing can cover for anything. This is the
    only configuration in the repo where a *standby* that acknowledges a flush
    it did not perform becomes observable, which makes it the R2 test. It is
    also the harsher W1 test, and the one a Jepsen analysis would reach for.

The oracle is proven able to fail before it is trusted. ``--weaken-durability``
runs the writer with ``synchronous_commit = off``, which acknowledges commits
before flushing anything; under ``cluster-crash`` that must lose acked writes.
A run of this harness that cannot produce that red result is not evidence of
durability, it is evidence the fault stopped injecting — so the inversion is
not an optional extra mode, it is the thing that licenses the green result.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Final

import psycopg

import fault_primitives as fp
import topology

PG_USER: Final[str] = "postgres"
PG_DBNAME: Final[str] = "postgres"

TABLE: Final[str] = "durability_crash_ledger"

CONVERGE_TIMEOUT_S: Final[float] = 300.0
"""Budget for the cluster to come back after a crash. Generous because a
cluster-wide crash means every node replays WAL from its last checkpoint, and
it does so over FUSE, which is slower than the filesystems the other suites
measured their budgets against."""

WAL_FLUSH_PROCESSES: Final[tuple[str, ...]] = ("walwriter", "checkpointer", "background writer")
"""Processes that flush WAL behind the backend's back.

They have to be stopped for the inversion to mean anything. With
``synchronous_commit = off`` the backend acknowledges without fsyncing, but the
walwriter fsyncs anyway every ``wal_writer_delay`` — 200 ms by default. Any gap
between the last ack and the crash wider than that and everything is durable,
the inversion goes green, and the harness reports that it cannot detect
weakened durability when in fact it never gave itself the chance. Measured:
with a 1 s settle, all 300 acked writes survived ``synchronous_commit = off``.

Stopping them does not weaken the *green* run, which never touches them: it
widens the un-fsynced window for the red one so the fault has something to
destroy."""


class DurabilityViolation(Exception):
    """An acknowledged write did not survive the crash. Contract W1 or R2."""


class OracleNotProven(Exception):
    """The inversion did not go red, so a green result would mean nothing."""


@dataclass
class WriteLog:
    """Acks collected from the writer, and what the cluster held afterwards."""

    acked: list[int] = field(default_factory=list)
    unacked: list[int] = field(default_factory=list)
    surviving: set[int] = field(default_factory=set)

    @property
    def lost(self) -> list[int]:
        """Acked writes absent after recovery. Non-empty is a W1/R2 violation."""
        return sorted(k for k in self.acked if k not in self.surviving)

    @property
    def phantom(self) -> list[int]:
        """Writes that were never acked but survived anyway.

        Not a violation on its own — an ack lost in transit is indistinguishable
        from one never sent — but reported because a large phantom count next to
        a large lost count usually means the harness mislabelled acks rather
        than that the cluster misbehaved.
        """
        return sorted(k for k in self.unacked if k in self.surviving)


GATEWAY_PORT_BY_NODE: Final[dict[str, int]] = dict(
    zip(topology.NODES, topology.GATEWAY_PORTS, strict=True)
)
"""Gateway port per voter service. ``topology.GATEWAY_PORTS`` is positional and
parallel to ``NODES``; ``strict=True`` makes a drift between the two lengths an
error here rather than a silently shortened mapping that would leave some node
unaddressed and its writes uncounted."""


def gateway_endpoint(node: str) -> tuple[str, int]:
    """Host-published gateway address for `node`, derived from the compose file."""
    return "127.0.0.1", GATEWAY_PORT_BY_NODE[node]


def connect_gateway(node: str, *, weaken: bool) -> psycopg.Connection[Any]:
    host, port = gateway_endpoint(node)
    conn: psycopg.Connection[Any] = psycopg.connect(
        host=host,
        port=port,
        user=PG_USER,
        dbname=PG_DBNAME,
        connect_timeout=10,
        autocommit=True,
    )
    if weaken:
        # Session-scoped rather than ALTER SYSTEM: the supervisor owns
        # postgresql.conf and would contend with a persistent change, and a
        # weakening that outlives the run is a trap for the next suite.
        # `off` acknowledges the commit before the WAL is flushed anywhere,
        # locally or on a standby, which is precisely the lie being tested for.
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit = off")
    return conn


def ensure_table(node: str) -> None:
    with connect_gateway(node, weaken=False) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(f"CREATE TABLE {TABLE} (k int PRIMARY KEY, written_at timestamptz)")


def write_until(node: str, count: int, *, weaken: bool) -> WriteLog:
    """Write `count` single-row transactions, recording exactly what was acked.

    A row is recorded as acked only after COMMIT returns. That is the promise
    W1 is about, and it is the only claim this harness holds the cluster to.
    """
    log = WriteLog()
    with connect_gateway(node, weaken=weaken) as conn:
        for k in range(1, count + 1):
            try:
                with conn.cursor() as cur:
                    cur.execute(f"INSERT INTO {TABLE} (k, written_at) VALUES (%s, now())", (k,))
            except (psycopg.Error, OSError):
                log.unacked.append(k)
                continue
            log.acked.append(k)
    return log


def read_surviving(node: str) -> set[int]:
    with connect_gateway(node, weaken=False) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT k FROM {TABLE}")
        return {int(row[0]) for row in cur.fetchall()}


def await_writable_leader(timeout_s: float) -> str:
    """Block until some node's gateway accepts a write, and name it.

    Asks PostgreSQL rather than the management API. A node can report Raft
    leadership before its PG is promoted, and the recovery this measures is
    "the database takes writes again", not "a leader exists".
    """
    deadline = time.monotonic() + timeout_s
    last: str = "no attempt made"
    while time.monotonic() < deadline:
        for node in topology.NODES:
            try:
                with connect_gateway(node, weaken=False) as conn, conn.cursor() as cur:
                    cur.execute("SELECT pg_is_in_recovery()")
                    row = cur.fetchone()
                    if row is not None and row[0] is False:
                        return node
            except (psycopg.Error, OSError) as exc:
                last = f"{node}: {str(exc).strip()}"
        time.sleep(2.0)
    raise TimeoutError(f"no writable leader within {timeout_s:g}s; last error: {last}")


def restart(ids: dict[str, str]) -> None:
    """Start each crashed container by id, and wait for each to be running."""
    fp.start_containers_by_id(ids)


def freeze_wal_flush(nodes: list[str]) -> None:
    """SIGSTOP the background WAL-flush processes on every named node.

    Only ever called for the inversion. The processes are never resumed: the
    crash that follows kills them, and the node comes back with fresh ones.
    """
    for node in nodes:
        # PIDs are selected first and signalled by number. `pkill -f` cannot be
        # used here: the pattern would have to name these processes, and the
        # shell running pkill carries that pattern on its own command line, so
        # pkill SIGSTOPs its own caller and the exec hangs until it times out.
        running = fp.read_processes(node)
        pids = [p.pid for p in running if any(name in p.args for name in WAL_FLUSH_PROCESSES)]
        if not pids:
            raise fp.FaultPreconditionError(
                f"{node}: none of {WAL_FLUSH_PROCESSES} are running, so there is "
                f"nothing to freeze and the un-fsynced window cannot be widened"
            )
        joined = " ".join(str(pid) for pid in pids)
        stopped = fp.exec_in(node, f"kill -STOP {joined}")
        if not stopped.ok:
            raise fp.FaultInjectionError(f"{node}: could not stop WAL flush: {stopped.output}")
        frozen = [
            p for p in fp.read_processes(node) if p.pid in set(pids) and p.state.startswith("T")
        ]
        if not frozen:
            raise fp.FaultEffectNotObserved(
                f"{node}: asked to stop {WAL_FLUSH_PROCESSES} but no process is in state T; "
                f"the un-fsynced window was never widened and the inversion would "
                f"go green for the wrong reason"
            )


def await_postgres_running(nodes: list[str], timeout_s: float) -> None:
    """Wait until every named node is running a PostgreSQL of its own.

    A writable leader is not evidence that the followers have one. A follower's
    postmaster only starts once its basebackup finishes, which lags the leader
    becoming writable by however long the clone takes — so a run that begins the
    moment a leader answers can reach a follower that has no postmaster at all.

    For the inversion that surfaced as a precondition failure; for the real run
    it would have been worse, because crashing a node whose PostgreSQL never
    started measures nothing and still reports a green.

    `checkpointer` and `background writer` run in recovery, so this holds on a
    standby; `walwriter` does not, which is why any one of them suffices.

    A node still restarting counts as pending, not as an error: `cluster-crash`
    kills all three at once and recovery can outlast a supervisor restart. The
    last refusal is carried into the timeout message to keep the two distinct.
    """
    deadline = time.monotonic() + timeout_s
    pending = list(nodes)
    unreachable: dict[str, str] = {}
    while pending and time.monotonic() < deadline:
        still: list[str] = []
        for node in pending:
            try:
                running = fp.read_processes(node)
            except fp.ContainerNotRunning as exc:
                unreachable[node] = str(exc)
                still.append(node)
                continue
            unreachable.pop(node, None)
            if not any(name in p.args for p in running for name in WAL_FLUSH_PROCESSES):
                still.append(node)
        pending = still
        if pending:
            time.sleep(1.0)
    if pending:
        detail = (
            f"; last seen: {'; '.join(f'{n}: {m}' for n, m in sorted(unreachable.items()))}"
            if unreachable
            else ""
        )
        raise fp.FaultPreconditionError(
            f"{', '.join(pending)}: no PostgreSQL running within {timeout_s:g}s "
            f"(looked for {WAL_FLUSH_PROCESSES}); the run would crash a node that "
            f"never started and report the result as durability{detail}"
        )


def run_case(*, mode: str, writes: int, weaken: bool) -> WriteLog:
    leader = await_writable_leader(CONVERGE_TIMEOUT_S)
    ensure_table(leader)

    victims = [leader] if mode == "leader-crash" else list(topology.NODES)
    await_postgres_running(victims, CONVERGE_TIMEOUT_S)

    if weaken:
        freeze_wal_flush(victims)

    log = write_until(leader, writes, weaken=weaken)
    if not log.acked:
        raise DurabilityViolation(
            "no write was acknowledged, so the run measures nothing; "
            "the cluster was not accepting writes"
        )

    # Resolved before the crash, for the same reason the primitive does it:
    # compose cannot name a container it is not currently running.
    victim_ids = {node: fp.container_id(node) for node in victims}
    with fp.crash_losing_unsynced_writes(victims):
        restart(victim_ids)

    survivor = await_writable_leader(CONVERGE_TIMEOUT_S)
    log.surviving = read_surviving(survivor)
    return log


def verdict(log: WriteLog, *, mode: str, weaken: bool) -> dict[str, object]:
    return {
        "mode": mode,
        "weakened_durability": weaken,
        "acked": len(log.acked),
        "unacked": len(log.unacked),
        "surviving": len(log.surviving),
        "lost_acked": len(log.lost),
        "lost_keys": log.lost[:20],
        "phantom": len(log.phantom),
        "contracts": ["W1", "R2"] if mode == "cluster-crash" else ["W1"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("leader-crash", "cluster-crash"),
        default="cluster-crash",
        help="cluster-crash is the R2 test; leader-crash tests W1 with a survivor",
    )
    parser.add_argument("--writes", type=int, default=500)
    parser.add_argument(
        "--weaken-durability",
        action="store_true",
        help="run the writer with synchronous_commit=off; the run then EXPECTS "
        "lost acked writes and fails if none are lost",
    )
    parser.add_argument(
        "--prove-oracle",
        action="store_true",
        help="run the inversion and then the real assertion, and require the "
        "first to go red before believing the second",
    )
    args = parser.parse_args()

    if args.prove_oracle and args.mode == "leader-crash":
        # `synchronous_commit=off` is not an inversion for this mode. Only the
        # leader's WAL flushers are frozen, because only the leader is killed,
        # so the standbys flush the streamed WAL normally and a promoted
        # standby still has every acked write. The inversion can therefore only
        # go red by killing the leader before replication has made the data
        # durable anywhere -- a race, not a property.
        #
        # It did go red for a while, which was worse than failing: it read as
        # the fault being proven when what had been proven was that the writer
        # outran the WAL sender that run. Slowing the write path with a second
        # LazyFS mount was enough to flip it.
        #
        # The primitive's evidence comes from cluster-crash, which shares it
        # and whose inversion is sound: with every node dead there is no
        # survivor to have flushed anything.
        raise OracleNotProven(
            "--prove-oracle is not meaningful for leader-crash. Weakening "
            "synchronous_commit cannot lose an acked write while a standby "
            "survives and flushes the streamed WAL, so the inversion would only "
            "go red by winning a race against replication.\n"
            "  Prove the fault with cluster-crash, which shares the same "
            "primitive and has no survivor:\n"
            "    testing/durability_crash.py --mode cluster-crash --prove-oracle"
        )

    if args.prove_oracle:
        red = run_case(mode=args.mode, writes=args.writes, weaken=True)
        print(json.dumps(verdict(red, mode=args.mode, weaken=True), indent=2))
        if not red.lost:
            raise OracleNotProven(
                "synchronous_commit=off lost no acked write across a dirty crash. "
                "Either the fault stopped injecting (check the LazyFS mount and "
                "the control FIFO) or PGDATA is not on LazyFS. Until this run is "
                "red, a green result from the real assertion proves nothing."
            )
        print(f"oracle proven: {len(red.lost)} acked writes lost with durability weakened\n")

    log = run_case(mode=args.mode, writes=args.writes, weaken=args.weaken_durability)
    report = verdict(log, mode=args.mode, weaken=args.weaken_durability)
    print(json.dumps(report, indent=2))

    if args.weaken_durability and not args.prove_oracle:
        if not log.lost:
            raise OracleNotProven(
                "durability was weakened and nothing was lost; the fault did not inject"
            )
        print(f"expected loss observed: {len(log.lost)} acked writes")
        return 0

    if log.lost:
        raise DurabilityViolation(
            f"{len(log.lost)} acknowledged writes did not survive a dirty crash "
            f"({args.mode}). First lost keys: {log.lost[:20]}. "
            f"This is a {'W1 and R2' if args.mode == 'cluster-crash' else 'W1'} violation."
        )

    print(f"all {len(log.acked)} acknowledged writes survived the dirty crash")
    return 0


if __name__ == "__main__":
    sys.exit(main())
