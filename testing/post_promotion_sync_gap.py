#!/usr/bin/env -S uv run --project testing python
"""RW-2: the post-promotion window, entered on protocol state rather than a sleep.

`Supervisor::promote` rewrites `synchronous_standby_names` as part of becoming
primary. Between `pg_ctl promote` returning and the replication manager's next
reconcile tick, whatever value sits in that GUC is the durability contract the
new primary is running under. If it is empty, the primary acknowledges commits
that no standby holds, and a crash in that window loses an acknowledged write —
contracts W1 and R2.

The window is short. Measured on a live three-node cluster it was ~120 ms, which
is why a `sleep` cannot address it: by the time a fixed delay expires the window
has closed, and the test reports on steady state while believing it tested the
transition. So the probe waits on the protocol state itself — a session opened
on each standby *before* the failover parks on `pg_is_in_recovery()` and issues
its commit the instant that flips. The commit costs no connect round trip
because the session already exists.

The verdict is on the commit, per the contract this closes:

  BLOCKED   the commit refused to acknowledge without a standby. Safe: the
            primary is running under a non-empty sync list and no standby has
            connected yet, so it waits rather than promising durability it
            cannot deliver.
  SYNC_ACK  the commit acknowledged and a synchronous standby's flush_lsn
            covered it. Safe: the ack was backed by a second copy.
  UNBACKED  the commit acknowledged and no standby held it. This is RW-2 open.

A run that never sees a promotion is `INDETERMINATE`, never a pass: the fault is
required to have had its effect before any verdict means anything. That rule is
the whole reason this file exists — an earlier draft bounded the wait loop with
`statement_timeout`, so the cancelled loop fell through to a marker that claimed
a promotion which had not happened.

`classify` is pure and separately tested in `test_post_promotion_sync_gap.py`,
including that it calls the unfixed behaviour a violation. A checker that cannot
fail is worse than no checker.
"""

from __future__ import annotations

import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import fault_primitives as fp

app = typer.Typer(add_completion=False)
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
PROBE_SQL: Final[Path] = (
    Path(__file__).resolve().parent / "sql" / "post-promotion-sync-gap-probe.sql"
)

INTERNAL_PG_PORT: Final[int] = 5434

# The probe parks on pg_is_in_recovery() with its own 90 s deadline and then
# bounds only the commit at 20 s, so a probe process is done well inside this.
PROBE_TIMEOUT_S: Final[float] = 150.0

# A promotion this cluster cannot complete inside is a failed run, not a slow
# one: the observed failover on an idle three-node cluster is under ten seconds.
PROMOTION_TIMEOUT_S: Final[float] = 120.0

RECOVERY_TIMEOUT_S: Final[float] = 180.0

TIMEOUT_MARKER: Final[str] = "canceling statement due to statement timeout"

# The primary refused the write outright rather than acknowledging it. This is
# the strongest safe outcome: a node that is primary but not yet honouring its
# synchronous configuration is fenced read-only, so a client gets an immediate
# error instead of an acknowledgement no standby holds — or an unbounded wait,
# which is what an ungated commit turns into once the wait engages.
READ_ONLY_MARKER: Final[str] = "read-only transaction"


class GapError(RuntimeError):
    """The run could not be carried out as specified."""


class Verdict(StrEnum):
    BLOCKED = "BLOCKED"
    REFUSED = "REFUSED"
    SYNC_ACK = "SYNC_ACK"
    UNBACKED = "UNBACKED"
    INDETERMINATE = "INDETERMINATE"

    @property
    def is_pass(self) -> bool:
        return self in (Verdict.BLOCKED, Verdict.REFUSED, Verdict.SYNC_ACK)


@dataclass(frozen=True)
class ProbeResult:
    """What one standby's probe session observed."""

    service: str
    promoted: bool
    sync_list_at_promotion: str | None
    standbys_at_promotion: int | None
    acked: bool
    sync_list_at_ack: str | None
    sync_acks: int | None
    timed_out: bool
    refused_read_only: bool
    sync_standbys_at_ack: int | None = None
    read_only_at_promotion: str | None = None
    read_only_at_ack: str | None = None
    raw: str = field(repr=False, default="")

    @property
    def entered_empty_window(self) -> bool:
        """Whether this node was writable under no synchronous requirement.

        True when the node is out of recovery and its sync list is empty at
        either observation point. That is RW-2's precondition; the verdict
        still rests on what the commit did, because an empty list with no
        client write in flight harms nobody.
        """
        if not self.promoted:
            return False
        return self.sync_list_at_promotion == "" or self.sync_list_at_ack == ""


def parse_probe(service: str, text: str) -> ProbeResult:
    """Read one probe session's markers.

    Tolerant of the surrounding psql chatter and of a session that errored:
    every field defaults to "not observed" rather than to a value that would
    read as evidence.
    """
    promoted = False
    sync_list_at_promotion: str | None = None
    standbys_at_promotion: int | None = None
    read_only_at_promotion: str | None = None
    acked = False
    sync_list_at_ack: str | None = None
    read_only_at_ack: str | None = None
    sync_standbys_at_ack: int | None = None
    sync_acks: int | None = None

    for line in text.splitlines():
        parts = line.split("|")
        head = parts[0].strip()
        if head == "PROMOTED" and len(parts) >= 4:
            promoted = True
            sync_list_at_promotion = parts[2]
            standbys_at_promotion = _maybe_int(parts[3])
            if len(parts) >= 5:
                read_only_at_promotion = parts[4].strip()
        elif head == "ACKED" and len(parts) >= 4:
            acked = True
            sync_list_at_ack = parts[3]
            if len(parts) >= 5:
                read_only_at_ack = parts[4].strip()
        elif head == "SYNCNOW" and len(parts) >= 2:
            sync_standbys_at_ack = _maybe_int(parts[1])
        elif head == "SYNCACK" and len(parts) >= 2:
            sync_acks = _maybe_int(parts[1])

    return ProbeResult(
        service=service,
        promoted=promoted,
        sync_list_at_promotion=sync_list_at_promotion,
        standbys_at_promotion=standbys_at_promotion,
        read_only_at_promotion=read_only_at_promotion,
        acked=acked,
        sync_list_at_ack=sync_list_at_ack,
        read_only_at_ack=read_only_at_ack,
        sync_standbys_at_ack=sync_standbys_at_ack,
        sync_acks=sync_acks,
        timed_out=TIMEOUT_MARKER in text,
        refused_read_only=READ_ONLY_MARKER in text,
        raw=text,
    )


def _tristate(value: bool | None) -> str:
    """Render an observation that may not have been made at all."""
    if value is None:
        return "-"
    return "yes" if value else "no"


def _acked_cell(probe: ProbeResult) -> str:
    """How the commit ended, for the summary table."""
    if probe.acked:
        return "yes"
    if probe.refused_read_only:
        return "refused (read-only)"
    if probe.timed_out:
        return "timed out"
    return "no"


def _maybe_int(text: str) -> int | None:
    try:
        return int(text.strip())
    except ValueError:
        return None


def classify(results: list[ProbeResult]) -> tuple[Verdict, str]:
    """The verdict for a run, from every probe's observations.

    Pure, so the inversion tests can hand it the unfixed cluster's behaviour and
    require a violation back.
    """
    promoted = [r for r in results if r.promoted]
    if not promoted:
        return (
            Verdict.INDETERMINATE,
            "no probe observed a promotion — the fault produced no failover, "
            "so nothing about the window was tested",
        )
    if len(promoted) > 1:
        # Two primaries at once is a far bigger finding than RW-2, and it must
        # not be reported as a sync-gap pass.
        names = ", ".join(sorted(r.service for r in promoted))
        return (
            Verdict.INDETERMINATE,
            f"more than one node reported itself primary ({names}) — "
            "investigate as split brain, not as a sync gap",
        )

    probe = promoted[0]
    if not probe.acked:
        if probe.refused_read_only:
            return (
                Verdict.REFUSED,
                f"{probe.service} refused the write outright — primary but "
                "fenced read-only until its synchronous configuration is in "
                f"force (sync list at promotion: {probe.sync_list_at_promotion!r}, "
                f"{probe.standbys_at_promotion} standbys connected)",
            )
        if probe.timed_out:
            return (
                Verdict.BLOCKED,
                f"{probe.service} refused to acknowledge the commit "
                f"(sync list at promotion: {probe.sync_list_at_promotion!r}, "
                f"{probe.standbys_at_promotion} standbys connected)",
            )
        return (
            Verdict.INDETERMINATE,
            f"{probe.service} was promoted but the commit neither acknowledged, "
            "timed out, nor was refused; the probe session did not run to "
            "completion",
        )

    if probe.sync_standbys_at_ack is None and probe.sync_acks is None:
        return (
            Verdict.INDETERMINATE,
            f"{probe.service} acknowledged the commit but the standby-ack count "
            "was never read, so the ack cannot be shown to be backed",
        )
    # PostgreSQL's own contract carries the verdict: with synchronous_commit on
    # and a standby designated `sync`, a commit does not return until that
    # standby has flushed it. The LSN-covering count corroborates but cannot
    # convict on its own — `commit_lsn` is read after the commit and drifts
    # past the commit record, so a standby that genuinely holds this write can
    # report a smaller flush_lsn.
    if (probe.sync_standbys_at_ack or 0) > 0 or (probe.sync_acks or 0) > 0:
        return (
            Verdict.SYNC_ACK,
            f"{probe.service} acknowledged the commit with "
            f"{probe.sync_standbys_at_ack} standby(s) designated sync "
            f"({probe.sync_acks} of them already past the commit position)",
        )
    return (
        Verdict.UNBACKED,
        f"{probe.service} acknowledged a commit that no standby held "
        f"(sync list at promotion: {probe.sync_list_at_promotion!r}, "
        f"at ack: {probe.sync_list_at_ack!r}) — RW-2 is open: a crash here "
        "loses an acknowledged write",
    )


def _compose_env() -> dict[str, str]:
    import os

    env = dict(os.environ)
    env.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "ci-token")
    return env


def arm_probe(service: str, sink: dict[str, str]) -> threading.Thread:
    """Open a probe session on `service` and let it park on protocol state.

    Returns immediately; the thread collects the session's output. Sessions are
    opened before the fault so the commit under test costs no connect round
    trip — a connect inside a ~120 ms window would be measuring the connect.
    """

    def _run() -> None:
        try:
            proc = subprocess.run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    service,
                    "psql",
                    "-U",
                    "postgres",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    str(INTERNAL_PG_PORT),
                    "-d",
                    "postgres",
                ],
                stdin=PROBE_SQL.open("rb"),
                capture_output=True,
                text=True,
                timeout=PROBE_TIMEOUT_S,
                cwd=PROJECT_ROOT,
                env=_compose_env(),
                check=False,
            )
            sink[service] = proc.stdout + proc.stderr
        except subprocess.TimeoutExpired:
            sink[service] = "probe session exceeded its timeout"

    thread = threading.Thread(target=_run, name=f"rw2-probe-{service}", daemon=True)
    thread.start()
    return thread


def ensure_probe_table(service: str) -> None:
    """Create the probe table on the leader so it replicates before the fault."""
    sql = (
        "CREATE TABLE IF NOT EXISTS rw2_probe("
        "id bigserial primary key, tag text, at timestamptz default clock_timestamp())"
    )
    rc, out, err = _sh(
        f"docker compose exec -T {service} psql -U postgres -h 127.0.0.1 "
        f'-p {INTERNAL_PG_PORT} -d postgres -At -c "{sql}"'
    )
    if rc != 0:
        raise GapError(f"could not create the probe table on {service}: {err or out}")


def _sh(cmd: str, timeout: float = 60.0) -> tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=PROJECT_ROOT,
        env=_compose_env(),
        check=False,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _await(predicate: Callable[[], bool], *, timeout_s: float, what: str) -> None:
    """Poll until `predicate` holds, or raise.

    A precondition that never holds has to end the run: the alternative is a
    test that proceeds against a cluster it never confirmed, which is how a
    harness reports coverage it does not have.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise GapError(f"timed out after {timeout_s:.0f}s waiting for {what}")


def await_healthy_cluster(timeout_s: float) -> None:
    """Every voter answering, one leader, before the run means anything."""

    def _ready() -> bool:
        leader = fp.find_raft_leader()
        if leader is None:
            return False
        for node in fp.NODES:
            rc, out, _ = _sh(
                f"docker compose exec -T {node} psql -U postgres -h 127.0.0.1 "
                f"-p {INTERNAL_PG_PORT} -d postgres -At -c 'SELECT 1'",
                timeout=20.0,
            )
            if rc != 0 or out.strip() != "1":
                return False
        return True

    _await(_ready, timeout_s=timeout_s, what="a healthy cluster")


@app.command()
def run(
    restore: bool = typer.Option(
        True,
        "--restore/--no-restore",
        help="Restart the killed leader and wait for the cluster to recover.",
    ),
) -> None:
    """Enter the post-promotion window on protocol state and judge the commit."""
    console.print("[bold]RW-2 — post-promotion sync gap[/]")

    await_healthy_cluster(RECOVERY_TIMEOUT_S)
    leader = fp.find_raft_leader()
    if leader is None:
        raise typer.Exit(code=2)
    console.print(f"leader: [bold]{leader}[/]")

    ensure_probe_table(leader)

    standbys = [n for n in fp.NODES if n != leader]
    sink: dict[str, str] = {}
    threads = [arm_probe(s, sink) for s in standbys]
    # The sessions must be parked on pg_is_in_recovery() before the fault, or a
    # fast failover promotes a node whose probe has not connected yet and the
    # run is indeterminate for a reason that has nothing to do with the window.
    _await_probes_parked(standbys)
    console.print(f"probes armed on: {', '.join(standbys)}")

    console.print(f"killing the leader ({leader})")
    fp.kill_container(leader)

    for thread in threads:
        thread.join(timeout=PROBE_TIMEOUT_S)

    results = [parse_probe(s, sink.get(s, "")) for s in standbys]
    verdict, detail = classify(results)

    table = Table(title="Post-promotion window (RW-2)")
    table.add_column("Node", style="bold")
    table.add_column("Promoted")
    table.add_column("Sync list at promotion")
    table.add_column("Standbys")
    table.add_column("Fenced at promotion")
    table.add_column("Fenced at ack")
    table.add_column("Acked")
    table.add_column("Sync standbys at ack")
    for r in results:
        table.add_row(
            r.service,
            "yes" if r.promoted else "no",
            "(empty)" if r.sync_list_at_promotion == "" else str(r.sync_list_at_promotion),
            "-" if r.standbys_at_promotion is None else str(r.standbys_at_promotion),
            r.read_only_at_promotion or "-",
            r.read_only_at_ack or "-",
            _acked_cell(r),
            "-" if r.sync_standbys_at_ack is None else str(r.sync_standbys_at_ack),
        )
    console.print(table)

    if restore:
        console.print(f"restoring {leader}")
        fp.start_container(leader)
        try:
            await_healthy_cluster(RECOVERY_TIMEOUT_S)
        except (GapError, fp.FaultError) as exc:
            console.print(f"[yellow]cluster did not fully recover: {exc}[/]")

    style = "green" if verdict.is_pass else "red"
    console.print(f"[{style}]{verdict}[/]: {detail}")
    if not verdict.is_pass:
        raise typer.Exit(code=1)


def _await_probes_parked(services: list[str]) -> None:
    """Wait until every probe session is visible in `pg_stat_activity`.

    Asserting the session exists, rather than sleeping and hoping, is the same
    discipline the fault primitives use: the probe is a precondition of the
    test, so its absence must fail loudly rather than silently weaken the run.
    """

    def _parked() -> bool:
        for service in services:
            rc, out, _ = _sh(
                f"docker compose exec -T {service} psql -U postgres -h 127.0.0.1 "
                f"-p {INTERNAL_PG_PORT} -d postgres -At -c "
                "\"SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%pg_is_in_recovery%' "
                'AND pid <> pg_backend_pid()"',
                timeout=20.0,
            )
            if rc != 0 or _maybe_int(out) in (None, 0):
                return False
        return True

    _await(_parked, timeout_s=60.0, what="the probe sessions to park")


if __name__ == "__main__":
    app()
