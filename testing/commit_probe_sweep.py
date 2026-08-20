#!/usr/bin/env -S uv run --project testing python
"""H-17: is the commit probe right at every offset around COMMIT?

A client whose connection dies mid-COMMIT asks the probe whether its
transaction landed. A wrong answer is not a wrong status line — it manufactures
a phantom commit (client believes a write that never happened) or a duplicate
retry (client re-issues a write that did). One fixed timing was tested; the
neighbourhood of the commit record was not.

Each trial holds a transaction open, severs the backend a chosen number of
milliseconds after COMMIT is issued, then compares two things that must agree:

  probe   `/api/v1/cluster/txid-status`, which is what a client would ask
  truth   whether the row is actually there

`committed` must mean the row is present, `aborted` must mean it is absent.
`in progress` and unknown constrain nothing — they are honest — but a definite
answer that disagrees with the data is the bug this sweeps for.
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

app = typer.Typer(add_completion=False)
console = Console()

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
INTERNAL_PG_PORT: Final[int] = fp.PG_INTERNAL_PORT
TABLE: Final[str] = "commit_probe_trials"
APP_NAME: Final[str] = "commit_probe_trial"

# Tags are unique per run as well as per trial, so a row from an earlier sweep
# can never answer "did this transaction land".
RUN_NONCE: Final[int] = int(time.monotonic_ns() % 1_000_000)

# The transaction is held open this long before COMMIT, giving the sweep a
# fixed origin to measure offsets from.
HOLD_S: Final[float] = 2.0

# Offsets past the moment COMMIT is issued. Zero is "sever as COMMIT starts";
# the tail runs past a normal commit so the sweep brackets the record rather
# than sitting on one side of it.
DEFAULT_OFFSETS_MS: Final[tuple[int, ...]] = (0, 1, 2, 3, 5, 8, 12, 20, 35, 60, 100, 250)


class SweepError(RuntimeError):
    """The sweep could not be carried out as specified."""


@dataclass(frozen=True)
class Trial:
    offset_ms: int
    txid: int | None
    probe: str | None
    row_present: bool | None

    @property
    def verdict(self) -> str:
        if self.txid is None:
            return "SKIP: transaction never got an id"
        if self.row_present is None:
            return "SKIP: could not read the row back"
        if self.probe == "committed" and not self.row_present:
            return "FAIL: probe said committed, the row is absent (phantom commit)"
        if self.probe == "aborted" and self.row_present:
            return "FAIL: probe said aborted, the row is present (duplicate retry)"
        return "PASS"

    @property
    def ok(self) -> bool:
        return not self.verdict.startswith("FAIL")


def _psql(node: str, sql: str, timeout: float = 30.0) -> tuple[int, str]:
    import os

    env = dict(os.environ)
    env.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "ci-token")
    proc = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            node,
            "psql",
            "-U",
            "postgres",
            "-h",
            "127.0.0.1",
            "-p",
            str(INTERNAL_PG_PORT),
            "-d",
            "postgres",
            "-At",
            "-F",
            "|",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=PROJECT_ROOT,
        env=env,
        check=False,
    )
    return proc.returncode, proc.stdout.strip()


def setup(leader: str) -> None:
    # Truncated, and every tag carries a per-run nonce. Tags repeated across
    # runs otherwise, so a row left by a previous sweep answered `row_present`
    # for a transaction that had just aborted — the harness reporting a
    # phantom disagreement of its own making.
    rc, out = _psql(
        leader,
        f"CREATE TABLE IF NOT EXISTS {TABLE}(tag text primary key); TRUNCATE {TABLE}",
        timeout=60.0,
    )
    if rc != 0:
        raise SweepError(f"could not create {TABLE} on {leader}: {out}")


def start_trial(node: str) -> subprocess.Popen[str]:
    """A psql fed statement by statement, so COMMIT can be seen starting.

    Sent as one `-c` string the statements arrive as a single query, and the
    sweep could only time the sever by wall-clock arithmetic — which includes
    `docker exec` startup and put every sever well past the commit record.
    """
    import os

    env = dict(os.environ)
    env.setdefault("PGBATTERY_MANAGEMENT_API_TOKEN", "ci-token")
    return subprocess.Popen(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "-e",
            f"PGAPPNAME={APP_NAME}",
            node,
            "psql",
            "-U",
            "postgres",
            "-h",
            "127.0.0.1",
            "-p",
            str(INTERNAL_PG_PORT),
            "-d",
            "postgres",
            "-At",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
    )


def feed(proc: subprocess.Popen[str], sql: str, *, last: bool = False) -> None:
    """Send statements; `last` closes stdin so psql sees EOF and exits."""
    if proc.stdin is None:
        return
    proc.stdin.write(sql)
    proc.stdin.flush()
    if last:
        proc.stdin.close()


def await_commit_started(node: str, timeout_s: float) -> bool:
    """Wait until the trial backend is actually executing COMMIT.

    The offset is measured from this, not from wall-clock arithmetic: `docker
    exec` startup alone put every sever well past the commit record.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        rc, out = _psql(
            node,
            "SELECT count(*) FROM pg_stat_activity "
            f"WHERE application_name = '{APP_NAME}' AND state = 'active' "
            "AND query ILIKE 'COMMIT%'",
        )
        if rc == 0 and out.strip().isdigit() and int(out.strip()) > 0:
            return True
        time.sleep(0.002)
    return False


def find_backend(node: str) -> tuple[int, int] | None:
    """The trial backend's pid and its assigned transaction id.

    `pg_stat_activity.backend_xid` is how the transaction's identity is read
    from outside — the session that owns it cannot report it after being cut.
    """
    rc, out = _psql(
        node,
        "SELECT pid, backend_xid FROM pg_stat_activity "
        f"WHERE application_name = '{APP_NAME}' AND backend_xid IS NOT NULL LIMIT 1",
    )
    if rc != 0 or not out:
        return None
    parts = out.splitlines()[-1].split("|")
    if len(parts) != 2 or not parts[0].strip().isdigit() or not parts[1].strip().isdigit():
        return None
    return int(parts[0]), int(parts[1])


def sever(node: str, pid: int) -> None:
    _psql(node, f"SELECT pg_terminate_backend({pid})")


def ask_probe(node_index: int, txid: int) -> str | None:
    """What the management API tells a client about this transaction."""
    port = 9080 + node_index
    rc, out, _ = _sh(f"curl -s -m 5 'http://127.0.0.1:{port}/api/v1/cluster/txid-status/{txid}'")
    if rc != 0 or not out:
        return None
    # {"status":"committed"} or {"status":null}
    for key in ("committed", "aborted", "in progress"):
        if f'"{key}"' in out:
            return key
    return None


def _sh(cmd: str, timeout: float = 30.0) -> tuple[int, str, str]:
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


def row_present(node: str, tag: str) -> bool | None:
    rc, out = _psql(node, f"SELECT count(*) FROM {TABLE} WHERE tag = '{tag}'")
    if rc != 0 or not out.strip().isdigit():
        return None
    return int(out.strip()) > 0


def run_trial(leader: str, node_index: int, offset_ms: int, index: int) -> Trial:
    tag = f"trial-{RUN_NONCE}-{index}-{offset_ms}"
    proc = start_trial(leader)
    try:
        # Open the transaction and hold it, so the id can be read from outside.
        feed(proc, f"BEGIN;\nINSERT INTO {TABLE}(tag) VALUES ('{tag}');\n")
        found: tuple[int, int] | None = None
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline and found is None:
            found = find_backend(leader)
            if found is None:
                time.sleep(0.02)
        if found is None:
            feed(proc, "COMMIT;\n", last=True)
            proc.wait(timeout=30)
            return Trial(offset_ms=offset_ms, txid=None, probe=None, row_present=None)

        pid, txid = found
        feed(proc, "COMMIT;\n", last=True)
        if not await_commit_started(leader, timeout_s=10.0):
            # COMMIT finished before it could be seen — nothing to sever at
            # this offset, and no claim to make about it.
            proc.wait(timeout=30)
            return Trial(
                offset_ms=offset_ms,
                txid=txid,
                probe=ask_probe(node_index, txid),
                row_present=row_present(leader, tag),
            )
        time.sleep(offset_ms / 1_000.0)
        sever(leader, pid)
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        # Not `communicate()`: it flushes stdin, which `feed(last=True)` has
        # already closed, and raises on the closed handle.
        if proc.stdout is not None:
            proc.stdout.read()

    probe = ask_probe(node_index, txid)
    return Trial(
        offset_ms=offset_ms,
        txid=txid,
        probe=probe,
        row_present=row_present(leader, tag),
    )


@app.command()
def run(
    trials: int = typer.Option(1, "--trials", help="Repeats per offset."),
) -> None:
    """Sever a commit at each offset and check the probe against the data."""
    leader = fp.find_raft_leader()
    if leader is None:
        console.print("[red]no leader[/]")
        raise typer.Exit(code=2)
    node_index = int(leader.removeprefix("node"))
    console.print(f"[bold]H-17 commit-probe sweep[/] — leader {leader}")
    setup(leader)

    results: list[Trial] = []
    for repeat in range(trials):
        for offset in DEFAULT_OFFSETS_MS:
            results.append(run_trial(leader, node_index, offset, repeat))

    table = Table(title="Commit probe around COMMIT (H-17)")
    table.add_column("Offset", justify="right")
    table.add_column("Probe")
    table.add_column("Row")
    table.add_column("Verdict")
    for r in results:
        table.add_row(
            f"{r.offset_ms} ms",
            str(r.probe),
            "-" if r.row_present is None else ("present" if r.row_present else "absent"),
            "[green]PASS[/]" if r.ok else f"[red]{r.verdict}[/]",
        )
    console.print(table)

    definite = [r for r in results if r.probe in ("committed", "aborted")]
    console.print(f"[dim]{len(results)} trials, {len(definite)} with a definite probe answer[/]")
    if not definite:
        console.print("[red]no trial produced a definite answer — nothing was checked[/]")
        raise typer.Exit(code=2)
    if [r for r in results if not r.ok]:
        raise typer.Exit(code=1)
    console.print("[green]every definite probe answer matched the data[/]")


if __name__ == "__main__":
    app()
