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

``--chaos mid-upgrade`` layers ``docker-compose.pgupgrade-shim.yml``, whose
PostgreSQL 18 bindir holds the apply's own ``pg_upgrade`` until released
(``testing/pg_upgrade_shim/``). That process, on the derived leader, is
SIGKILLed once it has written into the target; the case asserts the failure's
effect, re-runs the apply to prove the leftover target is discarded, and ends
with the same verification as the plain proof. The writer runs throughout, so
W1 covers writes acked on 17 between the failed apply and the resume.
"""

from __future__ import annotations

import os
import re
import subprocess
import threading
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final

import httpx
import psycopg
import typer
import yaml
from pydantic import BaseModel
from rich.console import Console

os.environ.setdefault("COMPOSE_FILE", "docker-compose.pgupgrade.yml")

import api_models
import settle
import topology
from fault_primitives import parse_prometheus_metric
from topology import Node, load
from waiting import WaitTimeout, await_until

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
COMPOSE_FILE: Final[str] = "docker-compose.pgupgrade.yml"
SHIM_COMPOSE_FILE: Final[str] = "docker-compose.pgupgrade-shim.yml"
"""Layered on `COMPOSE_FILE` by the mid-upgrade chaos only."""
MID_UPGRADE: Final[str] = "mid-upgrade"
# Bounds any single docker call; the image is built by the job beforehand.
COMPOSE_TIMEOUT_SEC: Final[float] = 300.0
TOKEN_ENV: Final[str] = "PGBATTERY_MANAGEMENT_API_TOKEN"
FROM_MAJOR: Final[int] = 17
TO_MAJOR: Final[int] = 18
# backup.backup_dir from config/pgupgrade/*.toml, container-side: where the
# apply's fresh-backup preflight leaves its artifact.
BACKUP_DIR: Final[str] = "/var/lib/postgresql/backups"
START_MARKER: Final[str] = "/tmp/pgb-harness-start"

SHIM_MOUNT: Final[str] = "/opt/pg_upgrade_shim"
"""Where `SHIM_COMPOSE_FILE` mounts `testing/pg_upgrade_shim/` in every node."""
SHIM_HOLD_DIR: Final[str] = "/tmp/pg_upgrade_shim/hold"
"""The wrapper's default hold directory: it writes `held.pid` and waits for `release`."""
LEFTOVER_MARKER: Final[str] = ".harness_leftover_target"
"""Dropped into the killed apply's target; only a discard can remove it."""
HELD_METRIC: Final[str] = "pgbattery_connections_held_during_upgrade"
"""Counts sessions the gateway parked on the committed upgrade freeze."""
FREEZE_PROBE_SEC: Final[float] = 5.0
APPLY_BUDGET_SEC: Final[float] = 600.0

KILL_HELD_PG_UPGRADE: Final[str] = r"""
set -eu
hold="$1" target="$2" old="$3" real="$(readlink -f "$4/pg_upgrade.real")"
pid="$(cat "$hold/held.pid")"
alive() {
  [ -r "/proc/$pid/status" ] && ! grep -Eq '^State:[[:space:]]+[ZX]' "/proc/$pid/status"
}
args="$(tr '\0' ' ' <"/proc/$pid/cmdline")"
case " $args " in
  *" --check "*) echo "held pid $pid is a --check run: $args"; exit 3 ;;
esac
touch "$hold/release"
n=0
until [ "$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)" = "$real" ]; do
  if ! alive; then echo "pid $pid exited before becoming $real"; exit 4; fi
  n=$((n + 1))
  if [ "$n" -gt 1000 ]; then echo "pid $pid never became $real"; exit 4; fi
  sleep 0.01
done
n=0
until [ -d "$target/pg_upgrade_output.d" ]; do
  if ! alive; then echo "pg_upgrade $pid exited before writing into $target"; exit 5; fi
  n=$((n + 1))
  if [ "$n" -gt 1000 ]; then echo "pg_upgrade $pid wrote nothing into $target"; exit 5; fi
  sleep 0.01
done
kill -STOP "$pid"
if ! alive; then echo "pg_upgrade $pid exited before the kill"; exit 6; fi
servers=""
for dir in "$old" "$target"; do
  if [ -e "$dir/postmaster.pid" ]; then servers="$servers $dir"; fi
done
kill -KILL "$pid"
n=0
while alive; do
  n=$((n + 1))
  if [ "$n" -gt 500 ]; then echo "pg_upgrade $pid survived SIGKILL"; exit 7; fi
  sleep 0.01
done
echo "killed pid=$pid exe=$real servers=${servers:-none}"
"""
"""Release the held apply, wait until it is the real pg_upgrade writing into the
target, stop it to read whether it had started a server, then SIGKILL it and
confirm it is gone. Positional args: hold dir, target, old datadir, shim bindir."""

console = Console()
err_console = Console(stderr=True)
app = typer.Typer(add_completion=False)


class ContractFailure(RuntimeError):
    """A Track B contract did not hold."""


class NodePaths(BaseModel):
    """The PostgreSQL paths a node's own config file gives it."""

    model_config = {"extra": "ignore"}

    pg_data_dir: str
    pg_bin_dirs: dict[str, str]


@dataclass(frozen=True)
class KillReport:
    """What the in-container kill observed at the moment it fired."""

    pid: int
    exe: str
    servers: str
    """`none`, or the datadirs that had a postmaster when pg_upgrade was stopped."""


def parse_kill_report(output: str) -> KillReport:
    """The kill script's last line, or `ContractFailure` naming what it said instead."""
    lines = output.strip().splitlines()
    match = re.fullmatch(r"killed pid=(\d+) exe=(\S+) servers=(.+)", lines[-1] if lines else "")
    if match is None:
        raise ContractFailure(f"the kill did not report landing: {output.strip()!r}")
    return KillReport(pid=int(match[1]), exe=match[2], servers=match[3].strip())


def freeze_verdict(
    answered: bool, error: str | None, held_before: float, held_after: float
) -> bool:
    """Whether one idle query through a gateway shows the committed upgrade freeze.

    Frozen is a query left unanswered while the gateway's held counter moved;
    thawed is a prompt answer with the counter still. Anything else is not an
    answer either way and raises.
    """
    held = held_after > held_before
    if held and not answered:
        return True
    if answered and error is None and not held:
        return False
    raise ContractFailure(
        f"freeze probe is ambiguous: answered={answered} error={error!r} "
        f"held counter {held_before:g} -> {held_after:g}"
    )


def target_datadir(pg_data_dir: str, major: int) -> str:
    """The apply's target: `<base>_<major>` beside PGDATA, as the product names it."""
    path = PurePosixPath(pg_data_dir)
    base = re.sub(r"_\d+$", "", path.name)
    return str(path.parent / f"{base}_{major}")


def compose_files(chaos: str | None) -> list[str]:
    """The base file, plus the shim override for the case that holds pg_upgrade."""
    return [COMPOSE_FILE, SHIM_COMPOSE_FILE] if chaos == MID_UPGRADE else [COMPOSE_FILE]


def node_paths(service: str, files: Sequence[str]) -> NodePaths:
    """The config `service` runs with: compose merges mounts by target, so the
    last file mounting one wins."""
    config: Path | None = None
    for name in files:
        document = topology.ComposeDocument.model_validate(
            yaml.safe_load((PROJECT_ROOT / name).read_text(encoding="utf-8"))
        )
        spec = document.services.get(service)
        if spec is not None and any(topology.CONFIG_MOUNT in v for v in spec.volumes):
            config = spec.config_path(service)
    if config is None:
        raise SystemExit(f"{service} mounts no config in {list(files)}")
    return NodePaths.model_validate(tomllib.loads(config.read_text(encoding="utf-8")))


def _active_files() -> list[str]:
    separator = os.environ.get("COMPOSE_PATH_SEPARATOR", os.pathsep)
    return [f for f in os.environ.get("COMPOSE_FILE", COMPOSE_FILE).split(separator) if f]


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
            env={**os.environ},
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


def _probe(service: str, script: str, *args: str) -> str:
    """Run a shell probe that must print one of its own answers.

    `docker compose exec` exits 1 for a stopped container too, so a probe's
    answer is what it printed, never its exit code.
    """
    proc = _compose("exec", "-T", service, "sh", "-c", script, "sh", *args, check=False)
    answer = proc.stdout.strip()
    if proc.returncode != 0 or not answer:
        raise ContractFailure(
            f"probe on {service} gave no answer (rc={proc.returncode}): "
            f"{(proc.stderr or proc.stdout).strip()}"
        )
    return answer


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


def _pg_major(node: Node, timeout: float = 180.0) -> int:
    """Poll the query itself: pg_isready can pass and the postmaster still bounce."""
    last = ""

    def major() -> int | None:
        nonlocal last
        proc = _compose(
            "exec",
            "-T",
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
            check=False,
        )
        if proc.returncode != 0:
            last = (proc.stderr or proc.stdout).strip()
            return None
        return int(proc.stdout.strip()) // 10_000

    try:
        return await_until(
            major, budget=timeout, describe=f"{node.service} to report its major", interval=2.0
        )
    except WaitTimeout as exc:
        raise ContractFailure(f"{exc}: {last}") from exc


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
        # Not `_stop`: that is a Thread internal on CPython 3.12, and
        # rebinding it makes join() raise at teardown.
        self._stop_event = threading.Event()
        self.acked: list[str] = []
        self.errors: list[str] = []

    def run(self) -> None:
        i = 0
        while not self._stop_event.is_set():
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
                        while not self._stop_event.is_set():
                            key = f"w-{i}"
                            cur.execute(
                                "INSERT INTO major_upgrade_probe VALUES (%s, %s) "
                                "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                                (key, str(i)),
                            )
                            self.acked.append(key)
                            i += 1
                            self._stop_event.wait(0.05)
            except (psycopg.Error, OSError) as exc:
                self.errors.append(str(exc))
                self._stop_event.wait(0.5)

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=15)


def _assert_acked_survived(node: Node, acked: list[str]) -> None:
    """W1 by key identity, not count: the writer's own error path can commit a
    row it never counted as acked, and that extra row offsets a lost one."""
    if not acked:
        raise ContractFailure("W1 check is vacuous: the writer acked nothing")
    present = set(
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
            "SELECT k FROM major_upgrade_probe WHERE k LIKE 'w-%';",
        ).split()
    )
    missing = sorted(set(acked) - present)
    if missing:
        raise ContractFailure(
            f"W1 violated: {len(missing)} acked keys missing after the upgrade, e.g. {missing[:5]}"
        )
    console.print(f"W1 OK: all {len(set(acked))} acked keys survived the upgrade")


def _assert_preflight_backup_fresh(service: str) -> None:
    """The apply's no-skip preflight really took a backup on this run.

    The apply runs without --allow-no-backup, so a fresh artifact must exist
    on the primary; an old artifact from an earlier run satisfies an
    existence check while the preflight silently skipped.
    """
    out = _exec(
        service,
        "sh",
        "-c",
        f"find {BACKUP_DIR} -mindepth 1 -newer {START_MARKER} 2>/dev/null | head -5; true",
    )
    if not out.strip():
        raise ContractFailure(
            f"preflight backup missing: nothing under {BACKUP_DIR} on {service} "
            "is newer than the harness start marker"
        )
    console.print(f"preflight backup fresh on {service}: {out.strip().splitlines()[0]}")


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


def _postgres_bins(node: Node) -> api_models.PostgresBins:
    # Token-gated: an auth error body must not parse as "no majors".
    r = httpx.get(
        f"http://127.0.0.1:{node.mgmt_port}/api/v1/postgres/bins",
        headers={"x-pgbattery-token": _token()},
        timeout=10.0,
    )
    r.raise_for_status()
    return api_models.parse(api_models.PostgresBins, r.text)


def _require_dual_majors(nodes: Sequence[Node]) -> None:
    """Every node publishes both majors, at the bindir its own config names."""
    for node in nodes:
        bins = _postgres_bins(node)
        expected = node_paths(node.service, _active_files()).pg_bin_dirs
        for major in (FROM_MAJOR, TO_MAJOR):
            probed = bins.bin_dirs.get(str(major))
            if probed is None or not probed.postgres_executable or probed.reported_major != major:
                raise ContractFailure(
                    f"{node.service} does not publish a working major {major} ({probed}); "
                    "every major-upgrade case would pass while blind"
                )
            if probed.path != expected[str(major)]:
                raise ContractFailure(
                    f"{node.service} probes major {major} at {probed.path}, "
                    f"but its config names {expected[str(major)]}"
                )
    console.print(f"majors {FROM_MAJOR} and {TO_MAJOR} published on every node")


def _install_shim(nodes: Sequence[Node]) -> None:
    """Build the holding bindir on every node and prove the product will use it.

    Every node, not just the leader: standbys rejoin with the 18 bindir too.
    """
    for node in nodes:
        paths = node_paths(node.service, _active_files())
        real = node_paths(node.service, [COMPOSE_FILE]).pg_bin_dirs[str(TO_MAJOR)]
        shim = paths.pg_bin_dirs[str(TO_MAJOR)]
        if shim == real:
            raise ContractFailure(
                f"{node.service}'s config still names the real {real}; the shim would hold nothing"
            )
        _exec(node.service, "sh", f"{SHIM_MOUNT}/install.sh", real, shim)
        wrapper = _probe(node.service, 'readlink "$1/pg_upgrade"', shim)
        if wrapper != f"{SHIM_MOUNT}/pg_upgrade":
            raise ContractFailure(f"{node.service}: {shim}/pg_upgrade resolves to {wrapper}")
    console.print(f"pg_upgrade shim installed on {[n.service for n in nodes]}")


def _metric(node: Node, name: str) -> float:
    r = httpx.get(f"http://127.0.0.1:{node.metrics_port}/metrics", timeout=5.0)
    r.raise_for_status()
    # A counter nothing has incremented is not exported yet.
    return parse_prometheus_metric(r.text, name) or 0.0


def _cluster_frozen(node: Node) -> bool:
    """Whether `node`'s gateway parks an idle query: the committed freeze's effect."""
    before = _metric(node, HELD_METRIC)
    done = threading.Event()
    outcome: dict[str, str] = {}

    def query() -> None:
        try:
            with psycopg.connect(
                host="127.0.0.1",
                port=node.gateway_port,
                user="postgres",
                dbname="postgres",
                connect_timeout=5,
            ) as conn:
                conn.execute("SELECT 1")
        except (psycopg.Error, OSError) as exc:
            outcome["error"] = str(exc)
        finally:
            done.set()

    threading.Thread(target=query, name="freeze-probe", daemon=True).start()
    answered = done.wait(FREEZE_PROBE_SEC)
    return freeze_verdict(answered, outcome.get("error"), before, _metric(node, HELD_METRIC))


def _await_held(leader: Node, worker: threading.Thread, result: dict[str, str]) -> None:
    """Wait for the apply's pg_upgrade to reach the shim on the derived leader."""

    def held() -> bool | None:
        if not worker.is_alive():
            raise ContractFailure(
                f"the apply ended before its pg_upgrade ran, so nothing was killed: "
                f"{result.get('output', 'no output')}"
            )
        answer = _probe(
            leader.service,
            'if [ -s "$1/held.pid" ]; then echo held; else echo no; fi',
            SHIM_HOLD_DIR,
        )
        return True if answer == "held" else None

    try:
        await_until(
            held, budget=APPLY_BUDGET_SEC, describe="the apply's pg_upgrade to reach the shim"
        )
    except WaitTimeout as exc:
        raise ContractFailure(f"{exc} on {leader.service}") from exc


def _assert_serves_old_major(leader: Node) -> None:
    major = _pg_major(leader)
    in_recovery = _exec(
        leader.service,
        "psql",
        "-h",
        "127.0.0.1",
        "-p",
        str(topology.PG_INTERNAL_PORT),
        "-U",
        "postgres",
        "-At",
        "-c",
        "SELECT pg_is_in_recovery();",
    ).strip()
    if major != FROM_MAJOR or in_recovery != "f":
        raise ContractFailure(
            f"after the killed pg_upgrade {leader.service} serves major {major} "
            f"(in recovery: {in_recovery}), not a {FROM_MAJOR} primary"
        )


def _assert_old_control_intact(leader: Node, paths: NodePaths) -> None:
    """The old datadir's control file parses and says the old cluster is running."""
    controldata = f"{paths.pg_bin_dirs[str(FROM_MAJOR)]}/pg_controldata"
    out = _exec(leader.service, controldata, "-D", paths.pg_data_dir)
    state = next((ln for ln in out.splitlines() if ln.startswith("Database cluster state:")), "")
    if not state.endswith("in production"):
        raise ContractFailure(f"old control file on {leader.service} reads {state!r}")


def _mark_leftover(leader: Node, target: str) -> None:
    """The killed apply left its target behind; mark it so only a discard clears it."""
    version = _probe(leader.service, 'cat "$1/PG_VERSION" 2>/dev/null || echo absent', target)
    if version != str(TO_MAJOR):
        raise ContractFailure(
            f"no leftover {TO_MAJOR} target at {target} on {leader.service} ({version}); "
            "discard-on-resume would not be exercised"
        )
    _exec(leader.service, "touch", f"{target}/{LEFTOVER_MARKER}")


def _assert_leftover_discarded(leader: Node, target: str) -> None:
    answer = _probe(
        leader.service,
        'if [ -e "$1" ]; then echo reused; else echo discarded; fi',
        f"{target}/{LEFTOVER_MARKER}",
    )
    if answer != "discarded":
        raise ContractFailure(f"the resumed apply kept the leftover target {target} ({answer})")
    console.print(f"discard-on-resume OK: the leftover {target} was replaced, not reused")


def _kill_mid_apply(leader: Node, cli: Node, entry: str) -> None:
    """SIGKILL the apply's own pg_upgrade on `leader` and assert the effect."""
    paths = node_paths(leader.service, _active_files())
    target = target_datadir(paths.pg_data_dir, TO_MAJOR)
    _exec(leader.service, "rm", "-rf", SHIM_HOLD_DIR)

    result: dict[str, str] = {}

    def apply() -> None:
        proc = _pgbattery(
            cli.service, f"upgrade postgres --to {TO_MAJOR} --experimental --node {entry}"
        )
        result["rc"] = str(proc.returncode)
        result["output"] = (proc.stdout + proc.stderr).strip()

    worker = threading.Thread(target=apply, name="pg-upgrade-apply", daemon=True)
    worker.start()
    _await_held(leader, worker, result)

    shim = paths.pg_bin_dirs[str(TO_MAJOR)]
    proc = _compose(
        "exec",
        "-T",
        leader.service,
        "sh",
        "-c",
        KILL_HELD_PG_UPGRADE,
        "sh",
        SHIM_HOLD_DIR,
        target,
        paths.pg_data_dir,
        shim,
        check=False,
    )
    if proc.returncode != 0:
        raise ContractFailure(
            f"the kill did not land (rc={proc.returncode}): {(proc.stdout + proc.stderr).strip()}"
        )
    report = parse_kill_report(proc.stdout)
    if report.servers != "none":
        raise ContractFailure(
            f"the kill landed after pg_upgrade started a server in {report.servers}; "
            "this case's kill point is before any"
        )
    console.print(f"SIGKILLed the apply's pg_upgrade on {leader.service}: {report}")

    worker.join(timeout=APPLY_BUDGET_SEC)
    if worker.is_alive():
        raise ContractFailure("the apply never returned after its pg_upgrade was killed")
    if result.get("rc") == "0":
        raise ContractFailure("apply reported success after its pg_upgrade was killed")

    _assert_serves_old_major(leader)
    _assert_old_control_intact(leader, paths)
    # A failed apply that recovered the old cluster returns it to service: held
    # clients are released into the primary that answers, not kept waiting on
    # an upgrade that is no longer running.
    if _cluster_frozen(leader):
        raise ContractFailure(
            f"after a failed apply {leader.service} serves {FROM_MAJOR}, yet the freeze still "
            "holds clients"
        )
    _mark_leftover(leader, target)
    console.print(
        f"chaos mid-upgrade: apply failed, {leader.service} serves {FROM_MAJOR} from an intact "
        f"control file to released clients, leftover target {target} marked"
    )


@app.command()
def main(
    no_up: bool = typer.Option(False, "--no-up", help="Skip docker compose up"),
    rollback_drill: bool = typer.Option(
        False, "--rollback-drill", help="Roll back instead of confirming"
    ),
    chaos: str | None = typer.Option(None, help=MID_UPGRADE),
) -> None:
    _token()
    if chaos not in (None, MID_UPGRADE):
        raise SystemExit(f"unknown --chaos {chaos!r}")
    files = compose_files(chaos)
    os.environ["COMPOSE_FILE"] = os.pathsep.join(files)
    os.environ["COMPOSE_PATH_SEPARATOR"] = os.pathsep
    if not no_up:
        _compose("down", "-v", check=False)
        _compose("up", "-d", "--remove-orphans")

    topo = load()
    # The apply's preflight is `doctor --strict`, which refuses a cluster
    # whose joins are still learners or whose standbys are not yet sync.
    leader = settle.await_settled(topo.nodes)
    console.print(f"topology: {[n.service for n in topo.nodes]}; leader={leader.service}")
    if chaos == MID_UPGRADE:
        _install_shim(topo.nodes)
    _require_dual_majors(topo.nodes)
    _assert_majors(FROM_MAJOR)

    cli = topo.nodes[0]
    gateway = cli.gateway_port
    entry = _entry_addr()
    _seed(gateway)

    writer = Writer(gateway)
    writer.start()
    try:
        await_until(
            lambda: True if writer.acked else None,
            budget=30.0,
            describe="the writer's first acked key",
        )
        if chaos == MID_UPGRADE:
            _kill_mid_apply(leader, cli, entry)
            # The resume's preflight is `doctor --strict` again: the standbys
            # the failed apply stopped must be back and sync first.
            resumed = settle.await_settled(topo.nodes)
            if resumed.service != leader.service:
                raise ContractFailure(
                    f"leadership moved {leader.service} -> {resumed.service} before the resume, "
                    "so the leftover target is not where the resumed apply runs"
                )
        # Freshness baseline for the preflight backup, on the primary taking it.
        _exec(leader.service, "touch", START_MARKER)
        # No --allow-no-backup: the fresh-backup, restore-into-a-scratch-cluster
        # preflight is the abort path this whole track leans on, and skipping it
        # here would leave it unexercised.
        proc = _pgbattery(
            cli.service, f"upgrade postgres --to {TO_MAJOR} --experimental --node {entry}"
        )
    finally:
        writer.stop()
    if proc.returncode != 0:
        raise ContractFailure(f"postgres upgrade apply failed rc={proc.returncode}")
    if chaos == MID_UPGRADE:
        paths = node_paths(leader.service, _active_files())
        _assert_leftover_discarded(leader, target_datadir(paths.pg_data_dir, TO_MAJOR))
    _assert_preflight_backup_fresh(leader.service)

    _assert_majors(TO_MAJOR)
    _assert_acked_survived(cli, writer.acked)

    if rollback_drill:
        refused = _pgbattery(
            cli.service,
            f"upgrade postgres --to {TO_MAJOR} --rollback --node {entry}",
        )
        if refused.returncode == 0:
            raise ContractFailure(
                "rollback without --accept-write-loss must refuse: writes landed "
                "past the unfreeze watermark"
            )
        console.print("write-loss refusal fired")

        rolled = _pgbattery(
            cli.service,
            f"upgrade postgres --to {TO_MAJOR} --rollback --accept-write-loss --node {entry}",
        )
        if rolled.returncode != 0:
            raise ContractFailure(f"rollback failed rc={rolled.returncode}")
        _assert_majors(FROM_MAJOR)
        _seed(gateway)
        console.print(f"rollback drill OK ({TO_MAJOR} -> {FROM_MAJOR}, old cluster accepts writes)")
        return

    confirmed = _pgbattery(
        cli.service,
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
