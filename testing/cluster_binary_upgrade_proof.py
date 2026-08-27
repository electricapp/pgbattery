#!/usr/bin/env -S uv run --project testing python
"""Track A: prove a real package-version rolling upgrade (not same-binary restart).

Requires the `runtime` image that ships:
  - running binary at workspace version (e.g. 0.1.0)
  - `/usr/local/share/pgbattery-releases/pgbattery-next` at workspace+patch (e.g. 0.1.1)

Serves a miniature release layout from node1, then runs
`pgbattery upgrade --cluster --yes` against it. Anti-vacuity: refuses if
current == next, if next is missing, or if any node stays on the old version.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path
from typing import Final

import httpx
import typer
from rich.console import Console

os.environ.setdefault("COMPOSE_FILE", "docker-compose.yml")

import topology
from topology import load

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Bounds any single docker call. `up` is the slow one; the image is built by
# the job before the case runs.
COMPOSE_TIMEOUT_SEC: Final[float] = 300.0
TOKEN = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")
RELEASE_PORT = 8765

console = Console()
err_console = Console(stderr=True)
app = typer.Typer(add_completion=False)


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
                if r.status_code == 200 and r.json().get("leader_id") is not None:
                    return
                last = r.text
            except Exception as exc:
                last = str(exc)
        time.sleep(2.0)
    raise SystemExit(f"no leader: {last}")


@app.command()
def main(
    no_up: bool = typer.Option(False, "--no-up", help="Skip docker compose up"),
) -> None:
    if not TOKEN:
        raise SystemExit("PGBATTERY_MANAGEMENT_API_TOKEN required")

    if not no_up:
        _compose("down", "-v", "--remove-orphans", check=False)
        _compose("up", "-d", "--remove-orphans", check=True)
        time.sleep(8)

    _wait_leader()
    topo = load()
    node1 = next(n for n in topo.nodes if n.service == "node1")

    # Probe current + next binaries inside the image.
    cur = _exec("node1", "pgbattery", "--version").strip().splitlines()[0]
    next_out = (
        _exec(
            "node1",
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
    live = _version(node1.mgmt_port)
    if live != cur_v:
        raise SystemExit(f"management API reports {live}, binary --version {cur_v}")

    console.print(f"proving Track A upgrade {cur_v} → {next_v}")

    # Platform binary name matches Rust std::env::consts::{OS,ARCH}.
    uname = _exec("node1", "uname", "-m").strip()
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
    _exec("node1", "sh", "-c", setup, user="root")

    release_base = f"http://{node1.ip}:{RELEASE_PORT}"
    entry = f"{node1.ip}:{topology.MGMT_CONTAINER_PORT}"
    # Orchestrate from a *separate* container on the compose network. Running
    # `upgrade --cluster` inside node1 dies with the process when the former
    # leader replaces its own binary and restarts (exit 137).
    net = _compose("ps", "-q", "node1", check=False).stdout.strip()
    # Resolve the raft network name from the running node1 container.
    inspect = subprocess.run(
        [
            "docker",
            "inspect",
            "-f",
            "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
            net or "pgbattery-node1-1",
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    network = inspect.stdout.strip().splitlines()[0]
    image = subprocess.run(
        ["docker", "inspect", "-f", "{{.Config.Image}}", net or "pgbattery-node1-1"],
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
        entry,
        "--version",
        next_v,
        "--url",
        release_base,
        "--allow-insecure-http",
        "--insecure-no-verify",
        "--allow-no-backup",
    ]
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
    console.print(f"cluster-binary-upgrade-proof OK ({cur_v} → {next_v})")


if __name__ == "__main__":
    app()
