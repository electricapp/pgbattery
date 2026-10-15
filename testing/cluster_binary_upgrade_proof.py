#!/usr/bin/env -S uv run --project testing python
"""Track A: prove a real package-version rolling upgrade (not same-binary restart).

Requires the `runtime` image that ships:
  - running binary at workspace version (e.g. 0.1.0)
  - `/usr/local/share/pgbattery-releases/pgbattery-next` at workspace+patch (e.g. 0.1.1)

Serves a miniature release layout from the entry node, then runs
`pgbattery upgrade --cluster --yes` against it under a continuous keyed
writer. Anti-vacuity: refuses if current == next, if next is missing, if any
node stays on the old version, if the writer acked nothing (W1 needs a
ledger), or if SYNC replication does not recover after the roll.
"""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Final

import httpx
import psycopg
import typer
from rich.console import Console

os.environ.setdefault("COMPOSE_FILE", "docker-compose.yml")

import settle
import topology
from topology import Node, Topology, load
from waiting import WaitTimeout, await_until

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Bounds any single docker call. `up` is the slow one; the image is built by
# the job before the case runs.
COMPOSE_TIMEOUT_SEC: Final[float] = 300.0
TOKEN = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")
RELEASE_PORT = 8765

console = Console()
err_console = Console(stderr=True)
app = typer.Typer(add_completion=False)


class ContractFailure(RuntimeError):
    """A Track A contract did not hold. Never an infrastructure problem."""


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
            env={**os.environ},
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        # Naming the wedged command beats letting the step budget kill the
        # harness with nothing said about which docker call hung.
        raise SystemExit(f"docker compose {' '.join(args)} hung for {timeout:g}s") from exc


def _exec(service: str, *cmd: str, user: str | None = None) -> str:
    args = ["exec", "-T"]
    if user:
        args.extend(["--user", user])
    args.append(service)
    args.extend(cmd)
    proc = _compose(*args, check=False)
    if proc.returncode != 0:
        raise SystemExit(
            f"compose exec {service} failed rc={proc.returncode}: {proc.stderr or proc.stdout}"
        )
    return proc.stdout


def _version(mgmt_port: int) -> str:
    r = httpx.get(f"http://127.0.0.1:{mgmt_port}/api/v1/version", timeout=5.0)
    r.raise_for_status()
    return str(r.json()["version"])


def _wait_versions(expected: str, timeout: float = 300.0) -> None:
    topo = load()
    # Profiled services (witness) are in the compose file but not started.
    nodes = topo.voters
    deadline = time.monotonic() + timeout
    got: list[tuple[str, str]] = []
    while time.monotonic() < deadline:
        got = []
        ok = True
        for n in nodes:
            try:
                v = _version(n.mgmt_port)
                got.append((n.service, v))
                if v != expected:
                    ok = False
            except Exception as exc:
                got.append((n.service, f"err:{exc}"))
                ok = False
        if ok:
            console.print(f"all voters at {expected}: {got}")
            return
        time.sleep(2.0)
    raise SystemExit(f"timeout waiting for version {expected}: {got}")


def _leader_node(topo: Topology, timeout: float = 180.0) -> Node:
    """The actual leader, from the management API — never an assumed node."""
    last = ""

    def leader() -> Node | None:
        nonlocal last
        for n in topo.nodes:
            try:
                r = httpx.get(
                    f"http://127.0.0.1:{n.mgmt_port}/api/v1/cluster/leader",
                    timeout=2.0,
                )
            except httpx.HTTPError as exc:
                last = str(exc)
                continue
            if r.status_code != 200:
                last = r.text
                continue
            lid = r.json().get("leader_id")
            if lid is None:
                last = r.text
                continue
            for cand in topo.nodes:
                if cand.node_id == int(lid):
                    return cand
        return None

    try:
        return await_until(leader, budget=timeout, describe="a leader via the management API")
    except WaitTimeout as exc:
        raise SystemExit(f"no leader: {last} ({exc})") from exc


class Writer(threading.Thread):
    """Continuous keyed gateway writes across the roll; records only acks."""

    def __init__(self, gateway_port: int) -> None:
        super().__init__(name="binary-upgrade-writer", daemon=True)
        self._port = gateway_port
        # Not `_stop`: that is a Thread internal on CPython 3.12, and
        # rebinding it makes join() raise at teardown.
        self._stop_event = threading.Event()
        self.acked: list[str] = []
        self.errors = 0

    def run(self) -> None:
        i = 0
        while not self._stop_event.is_set():
            try:
                with psycopg.connect(
                    host="127.0.0.1",
                    port=self._port,
                    user="postgres",
                    dbname="postgres",
                    connect_timeout=3,
                ) as conn:
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        while not self._stop_event.is_set():
                            key = f"w-{i}"
                            cur.execute(
                                "INSERT INTO binary_upgrade_probe(k) VALUES (%s) "
                                "ON CONFLICT (k) DO NOTHING",
                                (key,),
                            )
                            self.acked.append(key)
                            i += 1
                            time.sleep(0.05)
            except (psycopg.Error, OSError):
                self.errors += 1
                time.sleep(0.5)

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=15)


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
            cur.execute("CREATE TABLE IF NOT EXISTS binary_upgrade_probe(k text PRIMARY KEY)")


def _present_keys(service: str) -> set[str]:
    """Keys actually committed, read from one node's own PostgreSQL."""
    out = _exec(
        service,
        "psql",
        "-h",
        "127.0.0.1",
        "-p",
        str(topology.PG_INTERNAL_PORT),
        "-U",
        "postgres",
        "-At",
        "-c",
        "SELECT k FROM binary_upgrade_probe WHERE k LIKE 'w-%';",
    )
    return set(out.split())


def _assert_acked_survived(service: str, acked: set[str]) -> None:
    """W1 by key identity, not count: a committed retry the writer never
    counted as acked can offset a lost acked row in any count comparison."""
    if not acked:
        raise ContractFailure("W1 check is vacuous: the writer acked nothing across the roll")
    missing = sorted(acked - _present_keys(service))
    if missing:
        raise ContractFailure(
            f"W1 violated: {len(missing)} acked keys missing after the roll, e.g. {missing[:5]}"
        )
    console.print(f"W1 OK: all {len(acked)} acked keys survived the roll")


def _await_sync_recovered(topo: Topology, timeout: float = 180.0) -> None:
    """Every follower reports is_synced after the roll, per its own lag API."""
    leader = _leader_node(topo)
    followers = [n for n in topo.voters if n.node_id != leader.node_id]
    last: dict[str, object] = {}

    def synced() -> bool | None:
        for n in followers:
            try:
                r = httpx.get(
                    f"http://127.0.0.1:{n.mgmt_port}/api/v1/cluster/node/{n.node_id}/lag",
                    timeout=5.0,
                )
            except httpx.HTTPError as exc:
                last[n.service] = str(exc)
                return None
            if r.status_code != 200:
                last[n.service] = f"HTTP {r.status_code}"
                return None
            body = r.json()
            last[n.service] = body
            if not bool(body.get("is_synced", False)):
                return None
        return True

    try:
        await_until(synced, budget=timeout, describe="followers to report is_synced")
    except WaitTimeout as exc:
        raise ContractFailure(f"SYNC did not recover after the roll: {last} ({exc})") from exc
    console.print(f"SYNC recovered: {[n.service for n in followers]} report is_synced")


@app.command()
def main(
    no_up: bool = typer.Option(False, "--no-up", help="Skip docker compose up"),
) -> None:
    if not TOKEN:
        raise SystemExit("PGBATTERY_MANAGEMENT_API_TOKEN required")

    if not no_up:
        _compose("down", "-v", "--remove-orphans", check=False)
        _compose("up", "-d", "--remove-orphans", check=True)

    topo = load()
    entry = topo.voters[0]
    # The roll's preflight is `doctor --strict`: starting it while the joins
    # are still learners refuses on replication:sync for a reason the roll
    # did not cause.
    leader = settle.await_settled(topo.voters)
    console.print(f"settled: {len(topo.voters)} voters, leader {leader.service}")

    # Probe current + next binaries inside the image.
    cur = _exec(entry.service, "pgbattery", "--version").strip().splitlines()[0]
    next_out = (
        _exec(
            entry.service,
            "sh",
            "-c",
            "/usr/local/share/pgbattery-releases/pgbattery-next --version",
        )
        .strip()
        .splitlines()[0]
    )

    # clap --version is typically "pgbattery 0.1.0" or similar
    def parse_ver(line: str) -> str:
        m = re.search(r"(\d+\.\d+\.\d+)", line)
        if not m:
            raise SystemExit(f"cannot parse version from: {line!r}")
        return m.group(1)

    cur_v = parse_ver(cur)
    next_v = parse_ver(next_out)
    if cur_v == next_v:
        raise SystemExit(
            f"anti-vacuity: running and next binaries both report {cur_v}; "
            "rebuild the runtime image with a dual-version Dockerfile stage"
        )
    live = _version(entry.mgmt_port)
    if live != cur_v:
        raise SystemExit(f"management API reports {live}, binary --version {cur_v}")

    console.print(f"proving Track A upgrade {cur_v} → {next_v}")

    # Platform binary name matches Rust std::env::consts::{OS,ARCH}.
    uname = _exec(entry.service, "uname", "-m").strip()
    arch = {"aarch64": "aarch64", "arm64": "aarch64", "x86_64": "x86_64"}.get(uname)
    if not arch:
        raise SystemExit(f"unknown arch {uname!r}")
    binary_name = f"pgbattery-linux-{arch}"

    setup = f"""
set -euo pipefail
ROOT=/tmp/pgbattery-releases
rm -rf "$ROOT"
mkdir -p "$ROOT/v{next_v}"
cp /usr/local/share/pgbattery-releases/pgbattery-next "$ROOT/v{next_v}/{binary_name}"
(cd "$ROOT/v{next_v}" && sha256sum "{binary_name}" > "{binary_name}.sha256")
echo "{next_v}" > "$ROOT/latest"
# Tear down a prior release server by pid file (do not pkill -f: that can
# match this shell's own argv and SIGTERM the setup script).
if [ -f /tmp/pgbattery-release-http.pid ]; then
  kill "$(cat /tmp/pgbattery-release-http.pid)" 2>/dev/null || true
  rm -f /tmp/pgbattery-release-http.pid
fi
cd "$ROOT"
python3 -m http.server {RELEASE_PORT} --bind 0.0.0.0 >/tmp/release-http.log 2>&1 &
echo $! > /tmp/pgbattery-release-http.pid
sleep 0.5
curl -sf "http://127.0.0.1:{RELEASE_PORT}/latest" | grep -qx "{next_v}"
"""
    _exec(entry.service, "sh", "-c", setup, user="root")

    release_base = f"http://{entry.ip}:{RELEASE_PORT}"
    entry_addr = f"{entry.ip}:{topology.MGMT_CONTAINER_PORT}"
    # Orchestrate from a *separate* container on the compose network. Running
    # `upgrade --cluster` inside a node dies with the process when the former
    # leader replaces its own binary and restarts (exit 137).
    container = _compose("ps", "-q", entry.service, check=False).stdout.strip()
    if not container:
        # Guessing a container name would re-hardcode what topology derives.
        raise SystemExit(f"{entry.service} has no running container to inspect")
    # Resolve the raft network name from the running entry container.
    inspect = subprocess.run(
        [
            "docker",
            "inspect",
            "-f",
            "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
            container,
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    network = inspect.stdout.strip().splitlines()[0]
    image = subprocess.run(
        ["docker", "inspect", "-f", "{{.Config.Image}}", container],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    upgrade_cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "-e",
        f"PGBATTERY_MANAGEMENT_API_TOKEN={TOKEN}",
        image,
        "pgbattery",
        "upgrade",
        "--cluster",
        "--yes",
        "--node",
        entry_addr,
        "--version",
        next_v,
        "--url",
        release_base,
        "--allow-insecure-http",
        "--insecure-no-verify",
        "--allow-no-backup",
    ]
    # W1 needs a ledger: keyed writes run through the whole roll, and every
    # key the server acked must be present at the end.
    _seed(entry.gateway_port)
    writer = Writer(entry.gateway_port)
    writer.start()
    try:
        await_until(
            lambda: True if writer.acked else None,
            budget=30.0,
            describe="the writer's first acked key before the roll",
        )
        proc = subprocess.run(
            upgrade_cmd,
            cwd=PROJECT_ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        console.print(proc.stdout)
        if proc.stderr:
            err_console.print(proc.stderr)
        if proc.returncode != 0:
            raise SystemExit(f"cluster upgrade failed rc={proc.returncode}")

        _wait_versions(next_v)
    finally:
        writer.stop()

    # Read the ledger from the current leader: a standby can lag acks it has
    # not yet applied, and a key missing there is replication lag, not loss.
    _assert_acked_survived(_leader_node(topo).service, set(writer.acked))
    _await_sync_recovered(topo)
    console.print(f"cluster-binary-upgrade-proof OK ({cur_v} → {next_v})")


if __name__ == "__main__":
    app()
