#!/usr/bin/env -S uv run --project testing python
"""Track B: prove live PostgreSQL major upgrade 17→18 via pg_upgrade.

Uses ``docker-compose.pgupgrade.yml`` (cluster boots on major 17 with 18
binaries present). Runs ``pgbattery upgrade postgres --to 18 --experimental``,
then ``--confirm``. Anti-vacuity: refuses if any node still reports major 17
after apply.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import typer

os.environ.setdefault("COMPOSE_FILE", "docker-compose.pgupgrade.yml")

from topology import load

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TOKEN = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")
FROM_MAJOR = 17
TO_MAJOR = 18

app = typer.Typer(add_completion=False)


def _compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=PROJECT_ROOT,
        check=check,
        text=True,
        capture_output=True,
        env={**os.environ, "COMPOSE_FILE": "docker-compose.pgupgrade.yml"},
    )


def _exec(service: str, *cmd: str) -> str:
    proc = _compose("exec", "-T", service, *cmd, check=False)
    if proc.returncode != 0:
        raise SystemExit(
            f"exec {service} failed rc={proc.returncode}: {proc.stderr or proc.stdout}"
        )
    return proc.stdout


def _wait_leader(timeout: float = 180.0) -> dict[str, object]:
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
                    return {str(k): v for k, v in body.items()}
                last = r.text
            except Exception as exc:
                last = str(exc)
        time.sleep(2.0)
    raise SystemExit(f"no leader: {last}")


def _pg_major(service: str) -> int:
    out = _exec(
        service,
        "psql",
        "-h",
        "127.0.0.1",
        "-p",
        "5434",
        "-U",
        "postgres",
        "-At",
        "-c",
        "SHOW server_version_num;",
    ).strip()
    # server_version_num is e.g. 170011 → major 17
    num = int(out)
    return num // 10000


def _assert_majors(expected: int) -> None:
    topo = load()
    for n in topo.nodes:
        maj = _pg_major(n.service)
        if maj != expected:
            raise SystemExit(f"anti-vacuity: {n.service} reports major {maj}, expected {expected}")
    print(f"all nodes on PostgreSQL major {expected}")


@app.command()
def main(
    no_up: bool = typer.Option(False, "--no-up", help="Skip docker compose up"),
) -> None:
    if not TOKEN:
        raise SystemExit("PGBATTERY_MANAGEMENT_API_TOKEN required")

    os.environ["COMPOSE_FILE"] = "docker-compose.pgupgrade.yml"
    if not no_up:
        _compose("down", "-v", check=False)
        _compose("up", "-d", "--remove-orphans", check=True)
        time.sleep(10)

    _wait_leader()
    topo = load()
    print(f"topology: {[n.service for n in topo.nodes]}")

    # Dual-bin inventory must be present before we claim a major upgrade path.
    bins = httpx.get(
        f"http://127.0.0.1:{topo.nodes[0].mgmt_port}/api/v1/postgres/bins",
        timeout=10.0,
    )
    bins.raise_for_status()
    body = bins.json()
    print("postgres-bins:", body)
    bin_dirs = body.get("bin_dirs") or {}
    majors = set(bin_dirs.keys()) if isinstance(bin_dirs, dict) else set()
    if not majors and "directories" in body:
        majors = {str(d.get("major")) for d in body["directories"]}
    if str(FROM_MAJOR) not in majors or str(TO_MAJOR) not in majors:
        for maj in (FROM_MAJOR, TO_MAJOR):
            _exec(
                "node1",
                "test",
                "-x",
                f"/usr/lib/postgresql/{maj}/bin/postgres",
            )

    _assert_majors(FROM_MAJOR)

    # Seed a durable row so we can prove it survived pg_upgrade.
    gw = topo.nodes[0].gateway_port
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=gw,
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

    entry = "172.31.0.11:9091"
    upgrade = (
        f"PGBATTERY_MANAGEMENT_API_TOKEN={TOKEN} "
        f"pgbattery upgrade postgres --to {TO_MAJOR} --experimental "
        f"--node {entry} --allow-no-backup"
    )
    proc = _compose("exec", "-T", "node1", "sh", "-c", upgrade, check=False)
    print(proc.stdout)
    if proc.stderr:
        print(proc.stderr, file=sys.stderr)
    if proc.returncode != 0:
        raise SystemExit(f"postgres upgrade apply failed rc={proc.returncode}")

    _assert_majors(TO_MAJOR)

    confirm = (
        f"PGBATTERY_MANAGEMENT_API_TOKEN={TOKEN} "
        f"pgbattery upgrade postgres --to {TO_MAJOR} --confirm --node {entry}"
    )
    proc = _compose("exec", "-T", "node1", "sh", "-c", confirm, check=False)
    print(proc.stdout)
    if proc.returncode != 0:
        raise SystemExit(f"postgres upgrade confirm failed rc={proc.returncode}")

    with psycopg.connect(
        host="127.0.0.1",
        port=gw,
        user="postgres",
        dbname="postgres",
        connect_timeout=5,
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM major_upgrade_probe WHERE k = 'pre'")
            row = cur.fetchone()
            if not row or row[0] != "17":
                raise SystemExit(f"seed row missing after upgrade: {row}")

    print(f"pg-major-upgrade-proof OK ({FROM_MAJOR} → {TO_MAJOR})")


if __name__ == "__main__":
    app()
