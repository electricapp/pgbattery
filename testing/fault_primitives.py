#!/usr/bin/env -S uv run --project testing python
"""Fault-injection primitives — SKELETON.

╔══════════════════════════════════════════════════════════════════════════╗
║ STATUS: SKELETON — NOT YET IMPLEMENTED                                   ║
║ Owner: TBD                                                               ║
║ Tracking: BI2 in the audit-4 backlog                                     ║
╚══════════════════════════════════════════════════════════════════════════╝

═══════════════════════════════════════════════════════════════════════════
WHY THIS FILE EXISTS
═══════════════════════════════════════════════════════════════════════════

The chaos surface today is essentially {kill, pause, network-disconnect,
truncate-backup, fill-disk, libfaketime clock-shift}. That's a useful but
narrow vocabulary. Real production faults that we *do not* simulate:

  - **fsync stall.** A drive controller hangs an fsync() for 30 s. PG's
    commit path blocks; pgbattery's lease tick keeps running. Does the
    lease loop trip the fence threshold before fsync returns? If yes,
    is the in-flight write durable? If no, did we lose it?

  - **Asymmetric / lossy network.** Today's partition is binary
    (connected | disconnected). Real production sees 10-60 % packet
    drop, asymmetric routing (A→B works, B→A doesn't), and 500 ms+
    RTT spikes. Each one trips Raft's heartbeat timing differently.

  - **Lease-boundary clock skew.** Current clock-skew tests jump by
    ±5 min. The interesting case is a skew of ±100 ms applied *exactly*
    at the lease-expiry boundary, which can wedge openraft 0.9's
    no-PreVote election logic without producing any operator-visible
    error.

  - **WAL corruption / partial fsync.** A torn page on the leader's WAL
    after a power-cut: replay must detect it. Today's "truncate backup"
    test corrupts a backup file post-hoc, not a live WAL page.

═══════════════════════════════════════════════════════════════════════════
SCOPE — PRIMITIVES TO IMPLEMENT
═══════════════════════════════════════════════════════════════════════════

Each primitive should expose:

    open()  → state-handle  (set up the fault, returning a context for cleanup)
    close(handle)           (tear the fault down cleanly)

So callers compose: `with fsync_stall("node2", duration_s=10): kill_leader()`.

  [ ] fsync_stall(container, duration_s)
        Approach option A: bind-mount a FUSE filesystem in front of PGDATA
            that intercepts fsync() and delays it. Heavyweight but precise.
        Approach option B: SIGSTOP the postmaster's *checkpointer* worker.
            Cheaper, less realistic — does not exercise the disk-controller
            path. Document the limitation.

  [ ] partition_lossy(container, drop_pct, latency_ms)
        Use `docker exec` + `tc qdisc add dev eth0 root netem loss 30% delay 200ms`.
        Requires the container to have NET_ADMIN. The 3-node docker-compose
        already grants this; verify before using.

  [ ] partition_asymmetric(from_container, to_container)
        iptables -A OUTPUT -d <to_ip> -j DROP on `from`. Inbound still
        works because the reverse direction is unfiltered. Produces the
        classic "leader thinks it's alive, followers see it as dead"
        split. Cleanup MUST iptables -F or the rule survives the test.

  [ ] clock_skew_at_lease_boundary(container, skew_ms, window_ms)
        Use libfaketime (already deployed) but compute the trigger
        timestamp from the live lease expiry returned by the mgmt API,
        so the skew lands *exactly* in the [expiry - window/2,
        expiry + window/2] interval. Today's tests pick a random time.

  [ ] sigstop_checkpointer(container, duration_s)
        SIGSTOP the postmaster's checkpointer process by name. PG keeps
        accepting writes but cannot durably flush them. Verifies the
        lease-tick's "PG is alive but unhealthy" branch.

  [ ] disk_full_during_wal(container)
        Better than the current `fallocate 2G` blanket fill — bind-mount
        a tmpfs sized at exactly current_wal_size + 1 MB so the *next*
        WAL segment write hits ENOSPC. Verifies the supervisor's
        crash-restart path under specifically the ENOSPC class.

═══════════════════════════════════════════════════════════════════════════
INVARIANTS EACH PRIMITIVE MUST PRESERVE
═══════════════════════════════════════════════════════════════════════════

  1. **Cleanup is idempotent and best-effort.** A test crash mid-fault must
     not leave the container in an unrecoverable state. `close()` runs
     in a `finally`; if it can't tear down, log loudly but do not raise.

  2. **No state survives the test process.** No iptables rules, no
     bind-mounts, no zombie traffic-control qdiscs. Every primitive
     documents its cleanup recipe; the suite end-of-run also runs a
     "scrub" pass that nukes anything named with our test prefix.

  3. **Fault scope is bounded.** A primitive that targets `node2` must
     never affect `node1` or `node3`. Verified by spot-checks in the
     primitive's own integration test.

  4. **Observable from outside.** Each primitive emits a structured log
     line at open() and close() with a stable schema, so a downstream
     trace correlator (BI3 / request-id) can splice fault windows into
     the operation history.

═══════════════════════════════════════════════════════════════════════════
HOW THIS HOOKS INTO EXISTING TESTS
═══════════════════════════════════════════════════════════════════════════

  - `correctness_lite.py` and `overnight_test.py` get new "step" /
    scenario callables that call these primitives. Existing steps stay.

  - `ci_matrix.yaml` gets new step types (e.g. `fsync_stall`,
    `partition_lossy`) registered in `ci_runner.py`'s `StepType` enum.
    Each matrix step references the primitive by name; the runner does
    the open/close in a try/finally.

═══════════════════════════════════════════════════════════════════════════
TODO — file is a STUB. Adding the real implementation is BI2.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import typer
from rich.console import Console

app = typer.Typer(
    add_completion=False,
    help="Fault-injection primitives (SKELETON — not yet implemented).",
)
console = Console()


# ─────────────────────────────────────────────────────────────────────────────
# Primitive signatures — NotImplementedError stubs until BI2 lands.
# ─────────────────────────────────────────────────────────────────────────────


@contextmanager
def fsync_stall(container: str, duration_s: float) -> Iterator[None]:
    """Stall fsync() on `container` for `duration_s` seconds.

    See module docstring for the two implementation options. Cleanup
    MUST happen even if the body raises.
    """
    raise NotImplementedError("BI2: fsync_stall not yet implemented")
    yield  # pragma: no cover


@contextmanager
def partition_lossy(container: str, drop_pct: float, latency_ms: int) -> Iterator[None]:
    """Apply `tc netem loss / delay` to `container`'s primary interface."""
    raise NotImplementedError("BI2: partition_lossy not yet implemented")
    yield  # pragma: no cover


@contextmanager
def partition_asymmetric(from_container: str, to_container: str) -> Iterator[None]:
    """Drop all `from → to` packets; `to → from` still flows."""
    raise NotImplementedError("BI2: partition_asymmetric not yet implemented")
    yield  # pragma: no cover


@contextmanager
def clock_skew_at_lease_boundary(
    container: str, skew_ms: int, window_ms: int = 100
) -> Iterator[None]:
    """Apply a `skew_ms` libfaketime offset timed to hit the live lease boundary."""
    raise NotImplementedError("BI2: clock_skew_at_lease_boundary not yet implemented")
    yield  # pragma: no cover


@contextmanager
def sigstop_checkpointer(container: str, duration_s: float) -> Iterator[None]:
    """SIGSTOP the checkpointer; SIGCONT it after `duration_s`."""
    raise NotImplementedError("BI2: sigstop_checkpointer not yet implemented")
    yield  # pragma: no cover


@contextmanager
def disk_full_during_wal(container: str) -> Iterator[None]:
    """Fill the WAL volume so the next WAL segment write hits ENOSPC."""
    raise NotImplementedError("BI2: disk_full_during_wal not yet implemented")
    yield  # pragma: no cover


@app.command()
def list_primitives() -> None:
    """Print the planned primitive surface."""
    console.print("[bold yellow]fault_primitives.py is a SKELETON[/]")
    console.print("Planned primitives:")
    for name in (
        "fsync_stall",
        "partition_lossy",
        "partition_asymmetric",
        "clock_skew_at_lease_boundary",
        "sigstop_checkpointer",
        "disk_full_during_wal",
    ):
        console.print(f"  - {name}")
    console.print("\nTracking ticket: BI2. See module docstring for details.")


if __name__ == "__main__":
    app()
