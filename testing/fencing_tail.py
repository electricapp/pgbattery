#!/usr/bin/env -S uv run --project testing python
"""RW-4: what happens when fencing itself cannot complete.

Fencing is SQL-level. `default_transaction_read_only` plus
`pg_terminate_backend` requires PostgreSQL to still answer its supervisor, and
the accepted-risk entry says so plainly: when it does not, the escalation path
is process exit and container restart, with the container runtime as the real
backstop.

"The escalation path is process exit" was construction, not evidence. Nothing
had ever driven a node into a state where the fence *cannot* run and then
watched what the write path does meanwhile. That is this script.

Each mode makes the leader's PostgreSQL unable to service the fence, in a way a
clean SIGKILL cannot reproduce:

  sigstop-postmaster
      SIGSTOP the postmaster. It holds its listening socket, so the port stays
      open and connections queue rather than being refused — the supervisor's
      probe blocks instead of failing fast, which is the wedged case the fence
      has no answer for.

  exhaust-connections
      Fill every connection slot. Measured result: this does *not* reach the
      failure tail. `superuser_reserved_connections` is 3 and pgbattery
      connects as a superuser, so PostgreSQL keeps slots available for exactly
      this kind of administrative work and the fence still completes. Kept as a
      case because "connection exhaustion cannot starve the fence" is worth
      holding true, not because it breaks anything.

Breaking PostgreSQL is not by itself enough: pgbattery keeps running, keeps its
Raft leadership and a valid lease, and a node that still *holds* write authority
has nothing to fence. Measured — 210 s wedged with no escalation, correctly. So
each mode also isolates the node from its peers, costing it quorum, so the lease
expires and the enforcement loop must act through a PostgreSQL that may not be
able to answer.

Two things are asserted, and the first is the one that matters:

  1. **L1 holds.** The dual-writability prober races real writes at all three
     internal PostgreSQL ports on a 50 ms period for the whole window. A node
     that has lost its lease while the cluster elects a new leader is precisely
     where two nodes could both accept writes.
  2. **The node stops being a write authority.** Either it fences itself
     read-only, or it escalates to process exit and container restart when it
     cannot. Which one fired is reported; requiring a specific one would fail a
     node that fenced cleanly, which is what exhaustion does.

Run with:
    ./testing/fencing_tail.py --mode sigstop-postmaster
"""

from __future__ import annotations

import contextlib
import subprocess
import time
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import fault_primitives as fp
from dual_writability_prober import CREATE_PROBE_TABLE_SQL, PROBE_TABLE

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
ARTIFACT_DIR: Final[Path] = PROJECT_ROOT / "testing" / "artifacts" / "fencing-tail"

MODES: Final[tuple[str, ...]] = ("sigstop-postmaster", "exhaust-connections")

WINDOW_S: Final[float] = 90.0
"""How long the failure mode is held.

Long enough to cover the lease expiry plus the supervisor's consecutive-failure
budget before it signals shutdown, which is what the escalation assertion is
waiting for."""

console = Console()
app = typer.Typer(add_completion=False, help="RW-4: fencing-failure tail.")


class TailError(RuntimeError):
    """A precondition failed, or an assertion this script exists to make."""


def sh(cmd: str, timeout: float = 60.0) -> tuple[int, str, str]:
    try:
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout, cwd=PROJECT_ROOT
        )
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    return r.returncode, r.stdout, r.stderr


def leader_service() -> str:
    """The node holding write authority, by lease rather than by Raft role."""
    holder = fp.find_lease_holder()
    if holder is None:
        raise TailError("no node holds a valid lease; cluster is mid-failover or down")
    return holder


def wedge_postmaster(service: str) -> list[int]:
    """SIGSTOP every postgres process, returning the pids stopped.

    All of them, not just the postmaster: stopping the parent alone leaves the
    already-connected backends running, and the fence's `pg_terminate_backend`
    would still be delivered by them. The wedge has to cover the whole engine
    for the fence to actually have no path.
    """
    processes = fp.read_processes(service)
    postgres = [p for p in processes if "postgres" in p.args]
    if not postgres:
        raise TailError(f"no postgres processes found on {service}")
    pids = [p.pid for p in postgres]
    fp.exec_in(service, f"kill -STOP {' '.join(str(p) for p in pids)}", as_root=True)
    return pids


def unwedge_postmaster(service: str, pids: list[int]) -> None:
    fp.exec_in(service, f"kill -CONT {' '.join(str(p) for p in pids)}", as_root=True)


def exhaust_connections(service: str) -> None:
    """Occupy every remaining connection slot with idle sessions.

    Backgrounded inside the container so the sessions outlive this call; they
    die with the postmaster when the escalation restarts it, which is also the
    cleanup.
    """
    script = (
        'MAX=$(psql -h 127.0.0.1 -p 5434 -U postgres -tAc "SHOW max_connections"); '
        'for i in $(seq 1 "$MAX"); do '
        "  psql -h 127.0.0.1 -p 5434 -U postgres -c 'SELECT pg_sleep(600)' "
        "    >/dev/null 2>&1 & "
        "done; true"
    )
    fp.exec_in(service, script)


def start_prober(duration_s: float) -> subprocess.Popen[str]:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        [
            "./testing/dual_writability_prober.py",
            "--duration",
            str(duration_s),
            "--json",
            str(ARTIFACT_DIR / "result.json"),
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def await_prober_running(service: str, proc: subprocess.Popen[str]) -> None:
    """Block until the prober has landed at least one probe.

    The failure modes below leave no writable node, and the prober's startup
    DDL needs one — so it has to be past setup before the mode is applied.
    A row in the probe table is that proof: it means a round completed against
    a real PostgreSQL. Polling for it rather than sleeping keeps the ordering a
    fact instead of an assumption.
    """
    deadline = time.time() + 90.0
    while time.time() < deadline:
        if proc.poll() is not None:
            out, _ = proc.communicate()
            raise TailError(f"prober exited before probing (rc={proc.returncode}):\n{out[-800:]}")
        rc, out, _ = sh(
            f"docker compose exec -T {service} psql -h 127.0.0.1 -p 5434 -U postgres "
            f'-tAc "SELECT count(*) FROM {PROBE_TABLE}"',
            timeout=20,
        )
        if rc == 0 and out.strip().isdigit() and int(out.strip()) > 0:
            return
        time.sleep(2)
    raise TailError("prober never landed a probe; refusing to apply the failure mode blind")


def finish_prober(proc: subprocess.Popen[str]) -> None:
    """Interpret the prober's verdict. Only exit 0 is a pass."""
    out, _ = proc.communicate(timeout=WINDOW_S + 120)
    if proc.returncode == 1:
        raise TailError(f"L1 VIOLATED during the fencing-failure window:\n{out[-1500:]}")
    if proc.returncode == 3:
        raise TailError(
            "prober returned INCONCLUSIVE — too many indeterminate probes to assert "
            f"L1 over this window:\n{out[-1000:]}"
        )
    if proc.returncode != 0:
        raise TailError(f"prober failed to run (rc={proc.returncode}):\n{out[-1000:]}")


def node_writable(service: str) -> bool:
    """Whether `service` still accepts a write, asked of PostgreSQL directly."""
    rc, _, _ = sh(
        f"docker compose exec -T {service} psql -h 127.0.0.1 -p 5434 -U postgres "
        f'-v ON_ERROR_STOP=1 -tAc "CREATE TABLE IF NOT EXISTS pgb_tail_probe(id int); '
        f'INSERT INTO pgb_tail_probe VALUES (1)"',
        timeout=25,
    )
    return rc == 0


def await_not_writable(service: str, before: fp.ContainerRunState | None, timeout_s: float) -> str:
    """Wait until the node stops accepting writes, and report how it got there.

    This is the RW-4 assertion. A node that has lost its lease must stop being
    a write authority; whether it fences itself read-only or escalates to
    process exit and container restart is a mechanism, not the property.
    Demanding a restart specifically would fail a node that fenced cleanly —
    which is what connection exhaustion does, because superuser-reserved slots
    keep the fence's own session available.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        after = fp.read_container_runstate(service)
        if after is not None and before is not None and after.started_at != before.started_at:
            return (
                f"escalated to process exit and restart ({before.started_at} → {after.started_at})"
            )
        if not node_writable(service):
            return "fenced read-only without needing to restart"
        time.sleep(3)
    raise TailError(
        f"{service} was still accepting writes after {timeout_s:.0f}s without a lease. "
        "It neither fenced nor escalated — a writable zombie primary, which is "
        "exactly the RW-4 tail."
    )


def run_mode(mode: str) -> None:
    service = leader_service()
    console.print(f"[bold]{mode}[/] against the lease holder, {service}")
    before = fp.read_container_runstate(service)
    if before is None:
        raise TailError(f"could not read {service} container state")

    # The prober creates its probe table on first use. Doing that *after* the
    # failure mode is applied cannot work: the wedged leader is unreachable and
    # every standby is read-only, so the prober aborts as an infrastructure
    # error before it ever probes anything. Create it while a writer still
    # exists; the prober's own DDL is then a no-op.
    rc, out, err = sh(
        f"docker compose exec -T {service} psql -h 127.0.0.1 -p 5434 -U postgres "
        f'-v ON_ERROR_STOP=1 -c "{CREATE_PROBE_TABLE_SQL.strip()}"'
    )
    if rc != 0:
        raise TailError(f"could not pre-create the probe table on {service}: {err or out}")

    proc = start_prober(WINDOW_S)
    await_prober_running(service, proc)
    stopped_pids: list[int] = []
    isolated_from: list[str] = []
    try:
        if mode == "sigstop-postmaster":
            stopped_pids = wedge_postmaster(service)
            console.print(f"  SIGSTOPped {len(stopped_pids)} postgres process(es)")
        else:
            exhaust_connections(service)
            console.print("  filled every connection slot")

        # Breaking PostgreSQL alone does not exercise RW-4. pgbattery keeps
        # running, keeps its Raft leadership and a valid lease, and a node that
        # still *holds* write authority has nothing to fence — so no escalation
        # is expected and none happens. Measured: 210 s wedged with no restart,
        # which is correct behaviour for that state, not the failure tail.
        #
        # The tail needs the node to *lose* authority while unable to act on it.
        # Isolating it costs quorum, so the lease expires and the enforcement
        # loop must fence — through a PostgreSQL that cannot answer.
        peers = [n for n in fp.NODES if n != service]
        for peer in peers:
            fp.exec_in(
                service,
                fp.iptables_peer_drop_cmd(fp.NODE_IPS[peer], insert=True),
                as_root=True,
            )
        isolated_from = list(peers)
        console.print(f"  isolated {service} from {peers}: lease must now expire")

        how = await_not_writable(service, before, WINDOW_S + 120)
        console.print(f"  {service} stopped accepting writes — {how}")
    finally:
        for peer in isolated_from:
            # Idempotent: -D on an absent rule exits 1, and the node may
            # have restarted with a clean chain underneath us.
            with contextlib.suppress(fp.FaultError):
                heal = fp.iptables_peer_drop_cmd(fp.NODE_IPS[peer], insert=False)
                fp.exec_in(service, f"{heal} 2>/dev/null; true", as_root=True)
        if stopped_pids:
            # The restart replaces the postmaster, so these pids are usually
            # gone. CONT them anyway: if the escalation did NOT fire, leaving a
            # stopped postmaster behind would poison every later case.
            with contextlib.suppress(fp.FaultError):
                unwedge_postmaster(service, stopped_pids)
        finish_prober(proc)

    console.print("  L1 held for the whole window")


@app.command()
def run(
    mode: str = typer.Option(
        "sigstop-postmaster",
        "--mode",
        help=f"Which fencing-failure mode to drive. One of: {', '.join(MODES)}, or 'all'.",
    ),
) -> None:
    """Drive a fencing-failure mode and assert L1 holds through the escalation."""
    selected = MODES if mode == "all" else (mode,)
    for m in selected:
        if m not in MODES:
            console.print(f"[red]unknown mode {m!r}[/]; expected one of {MODES}")
            raise typer.Exit(code=2)

    results: list[tuple[str, str]] = []
    failed = False
    for m in selected:
        try:
            run_mode(m)
            results.append((m, "PASS"))
        except (TailError, fp.FaultError) as exc:
            console.print(f"[red]FAIL[/] {m}: {exc}")
            results.append((m, f"FAIL: {exc}"))
            failed = True
        # Let the cluster settle before the next mode: the previous one just
        # restarted a node, and driving the next against a half-rejoined
        # cluster measures the churn rather than the fence.
        if not fp.find_lease_holder():
            time.sleep(20)

    table = Table(title="Fencing-failure tail (RW-4)")
    table.add_column("Mode", style="bold")
    table.add_column("Result")
    for name, verdict in results:
        table.add_row(name, verdict)
    console.print(table)
    if failed:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
