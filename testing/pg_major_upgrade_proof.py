#!/usr/bin/env -S uv run --project testing python
"""Track B: prove a live PostgreSQL major upgrade 17 -> 18 via pg_upgrade.

Uses ``docker-compose.pgupgrade.yml`` (cluster boots on major 17 with 18
binaries present). Runs the real CLI: ``pgbattery upgrade postgres --to 18
--experimental``, then ``--confirm``.

Anti-vacuity: refuses if any node still reports major 17 after the apply, and
refuses if the writer produced no acked writes to check.

``--rollback-drill`` stops before ``--confirm`` and exercises the rollback
path instead: back to 17, old cluster starts and accepts a write, and the
write-loss watermark refusal fires without ``--accept-write-loss``.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Final

import httpx
import psycopg
import typer
from rich.console import Console

os.environ.setdefault("COMPOSE_FILE", "docker-compose.pgupgrade.yml")

import topology
from topology import Node, load

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
COMPOSE_FILE: Final[str] = "docker-compose.pgupgrade.yml"
# Bounds any single docker call; the image is built by the job beforehand.
COMPOSE_TIMEOUT_SEC: Final[float] = 300.0
TOKEN_ENV: Final[str] = "PGBATTERY_MANAGEMENT_API_TOKEN"
FROM_MAJOR: Final[int] = 17
TO_MAJOR: Final[int] = 18

console = Console()
err_console = Console(stderr=True)
app = typer.Typer(add_completion=False)


class ContractFailure(RuntimeError):
    """A Track B contract did not hold."""


def _token() -> str:
    t = os.environ.get(TOKEN_ENV, "").strip()
    if not t:
        raise SystemExit(f"{TOKEN_ENV} required")
    return t


def _compose(
    *args: str, check: bool = True, timeout: float = COMPOSE_TIMEOUT_SEC
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["docker", "compose", *args],
            cwd=PROJECT_ROOT,
            check=check,
            text=True,
            capture_output=True,
            env={**os.environ, "COMPOSE_FILE": COMPOSE_FILE},
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        # Naming the wedged command beats letting the step budget kill the
        # harness with nothing said about which docker call hung.
        raise SystemExit(f"docker compose {' '.join(args)} hung for {timeout:g}s") from exc


def _exec(service: str, *cmd: str) -> str:
    proc = _compose("exec", "-T", service, *cmd, check=False)
    if proc.returncode != 0:
        raise SystemExit(
            f"exec {service} failed rc={proc.returncode}: {proc.stderr or proc.stdout}"
        )
    return proc.stdout


def _pgbattery(service: str, args: str) -> subprocess.CompletedProcess[str]:
    """Run the shipped CLI inside a container, as an operator would."""
    cmd = f"PGBATTERY_MANAGEMENT_API_TOKEN={_token()} pgbattery {args}"
    proc = _compose("exec", "-T", service, "sh", "-c", cmd, check=False)
    console.print(proc.stdout)
    if proc.stderr:
        err_console.print(proc.stderr)
    return proc


def _entry_addr() -> str:
    """Cluster entry point, container-side, derived from the compose file."""
    node = load().nodes[0]
    return f"{node.ip}:{topology.MGMT_CONTAINER_PORT}"


def _wait_leader(timeout: float = 180.0) -> None:
    topo = load()
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        for n in topo.nodes:
            try:
                r = httpx.get(
                    f"http://127.0.0.1:{n.mgmt_port}/api/v1/cluster/leader",
                    timeout=2.0,
                )
                body = r.json()
                if (
                    r.status_code == 200
                    and isinstance(body, dict)
                    and body.get("leader_id") is not None
                ):
                    return
                last = r.text
            except (httpx.HTTPError, ValueError) as exc:
                last = str(exc)
        time.sleep(2.0)
    raise SystemExit(f"no leader: {last}")


def _pg_major(node: Node) -> int:
    out = _exec(
        node.service,
        "psql",
        "-h",
        "127.0.0.1",
        "-p",
        str(topology.PG_INTERNAL_PORT),
        "-U",
        "postgres",
        "-At",
        "-c",
        "SHOW server_version_num;",
    ).strip()
    return int(out) // 10_000


def _assert_majors(expected: int) -> None:
    for n in load().nodes:
        maj = _pg_major(n)
        if maj != expected:
            raise ContractFailure(f"anti-vacuity: {n.service} reports major {maj}, want {expected}")
    console.print(f"all nodes on PostgreSQL major {expected}")


class Writer(threading.Thread):
    """Continuous gateway writes, recording only what the server acked.

    The hold is supposed to stall clients, not break them, so a write that
    times out inside the hold window is not a lost ack — only a key the
    server confirmed and then lost is a W1 violation.
    """

    def __init__(self, gateway_port: int) -> None:
        super().__init__(name="pg-upgrade-writer", daemon=True)
        self._port = gateway_port
        self._stop = threading.Event()
        self.acked: list[str] = []
        self.errors: list[str] = []

    def run(self) -> None:
        i = 0
        while not self._stop.is_set():
            try:
                with psycopg.connect(
                    host="127.0.0.1",
                    port=self._port,
                    user="postgres",
                    dbname="postgres",
                    connect_timeout=5,
                ) as conn:
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        while not self._stop.is_set():
                            key = f"w-{i}"
                            cur.execute(
                                "INSERT INTO major_upgrade_probe VALUES (%s, %s) "
                                "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                                (key, str(i)),
                            )
                            self.acked.append(key)
                            i += 1
                            time.sleep(0.05)
            except (psycopg.Error, OSError) as exc:
                self.errors.append(str(exc))
                time.sleep(0.5)

    def stop(self) -> None:
        self._stop.set()
        self.join(timeout=15)


def _assert_acked_survived(node: Node, acked: list[str]) -> None:
    if not acked:
        raise ContractFailure("W1 check is vacuous: the writer acked nothing")
    present = int(
        _exec(
            node.service,
            "psql",
            "-h",
            "127.0.0.1",
            "-p",
            str(topology.PG_INTERNAL_PORT),
            "-U",
            "postgres",
            "-At",
            "-c",
            "SELECT count(*) FROM major_upgrade_probe WHERE k LIKE 'w-%';",
        ).strip()
    )
    if present < len(acked):
        raise ContractFailure(
            f"W1 violated: {len(acked)} writes acked, {present} present after upgrade"
        )
    console.print(f"W1 OK: all {len(acked)} acked writes survived the upgrade")


def _seed(gateway_port: int) -> None:
    with psycopg.connect(
        host="127.0.0.1",
        port=gateway_port,
        user="postgres",
        dbname="postgres",
        connect_timeout=5,
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS major_upgrade_probe(k text PRIMARY KEY, v text)"
            )
            cur.execute(
                "INSERT INTO major_upgrade_probe VALUES ('pre', '17') "
                "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
            )


def _require_dual_majors() -> None:
    topo = load()
    body = httpx.get(
        f"http://127.0.0.1:{topo.nodes[0].mgmt_port}/api/v1/postgres/bins",
        timeout=10.0,
    ).json()
    console.print("postgres-bins:", body)
    bin_dirs = body.get("bin_dirs") or {}
    majors = set(bin_dirs) if isinstance(bin_dirs, dict) else set()
    if not majors and "directories" in body:
        majors = {str(d.get("major")) for d in body["directories"]}
    missing = {str(m) for m in (FROM_MAJOR, TO_MAJOR)} - majors
    if missing:
        raise ContractFailure(
            f"image does not publish both majors (missing {sorted(missing)}); "
            "every major-upgrade case would pass while blind"
        )


@app.command()
def main(
    no_up: bool = typer.Option(False, "--no-up", help="Skip docker compose up"),
    rollback_drill: bool = typer.Option(
        False, "--rollback-drill", help="Roll back instead of confirming"
    ),
) -> None:
    _token()
    os.environ["COMPOSE_FILE"] = COMPOSE_FILE
    if not no_up:
        _compose("down", "-v", check=False)
        _compose("up", "-d", "--remove-orphans")

    _wait_leader()
    topo = load()
    console.print(f"topology: {[n.service for n in topo.nodes]}")
    _require_dual_majors()
    _assert_majors(FROM_MAJOR)

    gateway = topo.nodes[0].gateway_port
    _seed(gateway)

    writer = Writer(gateway)
    writer.start()
    time.sleep(2)

    entry = _entry_addr()
    # No --allow-no-backup: the fresh-backup, restore-into-a-scratch-cluster
    # preflight is the abort path this whole track leans on, and skipping it
    # here would leave it unexercised.
    proc = _pgbattery(
        topo.nodes[0].service,
        f"upgrade postgres --to {TO_MAJOR} --experimental --node {entry}",
    )
    writer.stop()
    if proc.returncode != 0:
        raise ContractFailure(f"postgres upgrade apply failed rc={proc.returncode}")

    _assert_majors(TO_MAJOR)
    _assert_acked_survived(topo.nodes[0], writer.acked)

    if rollback_drill:
        refused = _pgbattery(
            topo.nodes[0].service,
            f"upgrade postgres --to {TO_MAJOR} --rollback --node {entry}",
        )
        if refused.returncode == 0:
            raise ContractFailure(
                "rollback without --accept-write-loss must refuse: writes landed "
                "past the unfreeze watermark"
            )
        console.print("write-loss refusal fired")

        rolled = _pgbattery(
            topo.nodes[0].service,
            f"upgrade postgres --to {TO_MAJOR} --rollback --accept-write-loss --node {entry}",
        )
        if rolled.returncode != 0:
            raise ContractFailure(f"rollback failed rc={rolled.returncode}")
        _assert_majors(FROM_MAJOR)
        _seed(gateway)
        console.print(f"rollback drill OK ({TO_MAJOR} -> {FROM_MAJOR}, old cluster accepts writes)")
        return

    confirmed = _pgbattery(
        topo.nodes[0].service,
        f"upgrade postgres --to {TO_MAJOR} --confirm --node {entry}",
    )
    if confirmed.returncode != 0:
        raise ContractFailure(f"postgres upgrade confirm failed rc={confirmed.returncode}")

    with psycopg.connect(
        host="127.0.0.1",
        port=gateway,
        user="postgres",
        dbname="postgres",
        connect_timeout=5,
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM major_upgrade_probe WHERE k = 'pre'")
            row = cur.fetchone()
            if not row or row[0] != "17":
                raise ContractFailure(f"seed row missing after upgrade: {row}")

    console.print(f"pg-major-upgrade-proof OK ({FROM_MAJOR} -> {TO_MAJOR})")


if __name__ == "__main__":
    app()
