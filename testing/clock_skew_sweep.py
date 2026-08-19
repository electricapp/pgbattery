#!/usr/bin/env -S uv run --project testing python
"""H-08: clock steps across the failover boundary, in both directions.

The skew this repo shipped with was forward-only and coarse (+30 s / +300 s),
so it never landed near a boundary and never went backwards — the harder
direction, since a timestamp recorded before the step reads as future-dated
afterwards.

Two wall-clock-sensitive paths should be immune: the promotion hold-down (now
`Instant` with `checked_duration_since`), and the LSN staleness filter, which
caps future-dated entries at age 0 rather than dropping them and weakening the
election gate. `pgbattery_lsn_future_skew_total` is how that path reports it
was reached.

Each step runs inside a live failover with `dual_writability_prober` attached
as the L1 oracle. A step that lands outside the window raises rather than
passing.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

import fault_primitives as fp
from dual_writability_prober import PROBE_TABLE

app = typer.Typer(add_completion=False)
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
ARTIFACT_DIR: Final[Path] = PROJECT_ROOT / "testing" / "artifacts" / "clock-skew-sweep"
INTERNAL_PG_PORT: Final[int] = 5434

# Must outlive the whole step — edge, election, skew, promotion — or the
# interesting part goes unobserved rather than failing.
WINDOW_S: Final[float] = 150.0
RECOVERY_TIMEOUT_S: Final[float] = 240.0

# Small enough to land inside a boundary; the rest come from `sweep_around`, so
# retuning a constant retunes the sweep.
SUB_SECOND_MS: Final[tuple[int, ...]] = (100, 250, 500)


class SweepError(RuntimeError):
    """The sweep could not be carried out as specified."""


@dataclass(frozen=True)
class StepResult:
    """What one skew step did to a live failover."""

    skew_ms: int
    aim: fp.Aim
    observed_skew_ms: float
    offset_into_window_ms: float
    releases_holddown_early: bool
    holddown_engaged: bool | None
    future_skew_delta: float | None
    verdict: str

    @property
    def ok(self) -> bool:
        return self.verdict == "PASS"


def _sh(cmd: str, timeout: float = 60.0) -> tuple[int, str, str]:
    import os

    env = dict(os.environ)
    env.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "ci-token")
    proc = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=PROJECT_ROOT,
        env=env,
        check=False,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def skew_plan(timings: fp.SystemTimings, *, quick: bool) -> list[int]:
    """Every step, both signs, sign-interleaved so a cut-short run still covers both."""
    magnitudes: set[int] = set(SUB_SECOND_MS)
    magnitudes.update(fp.sweep_around(timings.lease_duration_ms))
    magnitudes.update(fp.sweep_around(timings.election_timeout_ms))
    if quick:
        # One below the lease boundary, one at it, one above, each way. Enough
        # to exercise the arithmetic in both directions without a 20-minute run.
        lease = timings.lease_duration_ms
        magnitudes = {max(1, lease // 2), lease, lease * 2}

    plan: list[int] = []
    for magnitude in sorted(magnitudes):
        plan.append(-magnitude)
        plan.append(magnitude)
    return plan


def await_healthy_cluster(timeout_s: float) -> None:
    """One leader, and every node's PostgreSQL answering.

    PostgreSQL too, not just the management API: a stranded node answers the
    latter while refusing connections, and would count as healthy.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if fp.find_raft_leader() is not None:
            answered = 0
            for node in fp.NODES:
                rc, out, _ = _sh(
                    f"docker compose exec -T {node} psql -U postgres -h 127.0.0.1 "
                    f"-p {INTERNAL_PG_PORT} -d postgres -At -c 'SELECT 1'",
                    timeout=20.0,
                )
                if rc == 0 and out.strip() == "1":
                    answered += 1
            if answered == len(fp.NODES):
                return
        time.sleep(2.0)
    raise SweepError(f"cluster did not become healthy within {timeout_s:.0f}s")


def start_prober(duration_s: float, label: str) -> subprocess.Popen[str]:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        [
            "./testing/dual_writability_prober.py",
            "--duration",
            str(duration_s),
            "--json",
            str(ARTIFACT_DIR / f"{label}.json"),
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def await_prober_running(proc: subprocess.Popen[str]) -> None:
    """Block until the prober has landed a probe — a skew applied before that is unwatched."""
    deadline = time.monotonic() + 90.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out, _ = proc.communicate()
            raise SweepError(f"prober exited before probing (rc={proc.returncode}):\n{out[-800:]}")
        for node in fp.NODES:
            rc, out, _ = _sh(
                f"docker compose exec -T {node} psql -h 127.0.0.1 -p {INTERNAL_PG_PORT} "
                f'-U postgres -tAc "SELECT count(*) FROM {PROBE_TABLE}"',
                timeout=20.0,
            )
            if rc == 0 and out.strip().isdigit() and int(out.strip()) > 0:
                return
        time.sleep(2.0)
    raise SweepError("prober never landed a probe; refusing to skew a clock nobody is watching")


def finish_prober(proc: subprocess.Popen[str]) -> None:
    """Interpret the prober's verdict. Only exit 0 is a pass."""
    out, _ = proc.communicate(timeout=WINDOW_S + 180)
    if proc.returncode == 1:
        raise SweepError(f"L1 VIOLATED — two nodes accepted writes:\n{out[-1_500:]}")
    if proc.returncode == 3:
        raise SweepError(
            "prober returned INCONCLUSIVE — too many indeterminate probes to assert "
            f"L1 over this window:\n{out[-1_000:]}"
        )
    if proc.returncode != 0:
        raise SweepError(f"prober failed to run (rc={proc.returncode}):\n{out[-1_000:]}")


def _future_skew_total() -> float:
    """`pgbattery_lsn_future_skew_total` across the cluster — proof a backward step
    reached the staleness filter. Summed, since the step lands on the election winner."""
    total = 0.0
    for node in fp.NODES:
        value = fp.read_metric(node, "pgbattery_lsn_future_skew_total")
        if value is not None:
            total += value
    return total


def run_step(skew_ms: int, aim: fp.Aim, timings: fp.SystemTimings) -> StepResult:
    """One skew step, applied inside a live failover with the oracle attached."""
    await_healthy_cluster(RECOVERY_TIMEOUT_S)
    leader = fp.find_raft_leader()
    if leader is None:
        raise SweepError("no leader to depose")

    label = f"skew{skew_ms:+d}ms-{aim.value}"
    before = _future_skew_total()
    proc = start_prober(WINDOW_S, label)
    try:
        await_prober_running(proc)
        with fp.clock_skew_at_lease_boundary(
            skew_ms=skew_ms,
            aim=aim,
            trigger=lambda: fp.kill_container(leader),
            timings=timings,
        ) as handle:
            # Hold across the rest of the hold-down and the promotion after it.
            time.sleep(timings.lease_duration_ms / 1_000.0 + 5.0)
        finish_prober(proc)
        verdict = "PASS"
    except (SweepError, fp.FaultError) as exc:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()
        return StepResult(
            skew_ms=skew_ms,
            aim=aim,
            observed_skew_ms=0.0,
            offset_into_window_ms=0.0,
            releases_holddown_early=False,
            holddown_engaged=None,
            future_skew_delta=None,
            verdict=f"FAIL: {exc}",
        )
    finally:
        fp.start_container(leader)

    return StepResult(
        skew_ms=skew_ms,
        aim=aim,
        observed_skew_ms=handle.observed_skew_ms,
        offset_into_window_ms=handle.offset_into_window_ms,
        releases_holddown_early=handle.releases_holddown_early,
        holddown_engaged=handle.holddown_engaged,
        future_skew_delta=_future_skew_total() - before,
        verdict=verdict,
    )


@app.command()
def run(
    quick: bool = typer.Option(
        False,
        "--quick",
        help="Three magnitudes each way instead of the full boundary neighbourhood.",
    ),
    aim: str = typer.Option(
        fp.Aim.HOLDDOWN_START.value,
        "--aim",
        help=f"Where in the window to step the clock: {[a.value for a in fp.Aim]}.",
    ),
) -> None:
    """Step the clock both ways across the failover boundary, and assert L1."""
    try:
        target_aim = fp.Aim(aim)
    except ValueError:
        console.print(f"[red]unknown aim {aim!r}[/]; expected one of {[a.value for a in fp.Aim]}")
        raise typer.Exit(code=2) from None

    timings = fp.read_system_timings()
    plan = skew_plan(timings, quick=quick)
    console.print(
        f"[bold]H-08 clock-skew sweep[/] — lease {timings.lease_duration_ms} ms, "
        f"election {timings.election_timeout_ms} ms, {len(plan)} steps, aim={target_aim.value}"
    )

    results: list[StepResult] = []
    for skew in plan:
        console.print(f"[dim]step {skew:+d} ms[/]")
        results.append(run_step(skew, target_aim, timings))
        if not results[-1].ok:
            console.print(f"[red]{results[-1].verdict}[/]")
            break

    table = Table(title="Clock steps across the failover boundary (H-08)")
    table.add_column("Skew", justify="right")
    table.add_column("Observed", justify="right")
    table.add_column("Into window", justify="right")
    table.add_column("Would release early")
    table.add_column("Hold-down engaged")
    table.add_column("Future-skew entries", justify="right")
    table.add_column("L1")
    for r in results:
        table.add_row(
            f"{r.skew_ms:+d} ms",
            f"{r.observed_skew_ms:+.0f} ms",
            f"{r.offset_into_window_ms:.0f} ms",
            "yes" if r.releases_holddown_early else "no",
            "-" if r.holddown_engaged is None else ("yes" if r.holddown_engaged else "no"),
            "-" if r.future_skew_delta is None else f"{r.future_skew_delta:.0f}",
            "[green]held[/]" if r.ok else "[red]VIOLATED[/]",
        )
    console.print(table)

    failed = [r for r in results if not r.ok]
    if failed:
        for r in failed:
            console.print(f"[red]{r.skew_ms:+d} ms: {r.verdict}[/]")
        raise typer.Exit(code=1)
    console.print(
        f"[green]L1 held across {len(results)} clock steps[/] "
        f"({min(r.skew_ms for r in results):+d} ms to {max(r.skew_ms for r in results):+d} ms)"
    )


if __name__ == "__main__":
    app()
