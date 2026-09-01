#!/usr/bin/env -S uv run --project testing python
"""PITR restore-and-start proof (UPGRADE_CHECKLIST step 3).

Stand up is assumed (compose already running). This script:

1. Writes a marker row on the leader gateway
2. Forces a WAL switch and waits for an archived segment
3. Takes a full basebackup via the management API
4. Writes a *post-backup* marker (only recoverable via WAL archive)
5. Restores the basebackup into a scratch data dir with ``restore_command``
   pointing at the live WAL archive, starts a throwaway postmaster, and
   asserts both markers are present — then accepts a new write

Inversion: if the restore path is skipped or the archive is empty, the
post-backup marker is missing and this exits non-zero. ``pg_verifybackup``
alone is not enough.

RPO note (also in UPGRADE_CHECKLIST): without WAL archiving, abort/restore
rewinds to the last full basebackup (hours of loss under light write load).
With archiving + this proof, abort RPO is bounded by ``archive_timeout``
(60s) plus apply lag.
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Any, Final, NoReturn

import httpx
import typer
from rich.console import Console

import topology
from topology import Node, load

TOKEN: Final[str] = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")
WAL_ARCHIVE: Final[str] = "/var/lib/postgresql/wal_archive"
PG_USER: Final[str] = "postgres"
SCRATCH_PORT: Final[int] = 55_432

console = Console()
err_console = Console(stderr=True)
app = typer.Typer(add_completion=False)


def die(msg: str) -> NoReturn:
    err_console.print(f"[red]pitr_restore_proof: FAIL:[/] {msg}")
    raise SystemExit(1)


def api(node: Node, method: str, path: str) -> dict[str, Any]:
    url = f"http://127.0.0.1:{node.mgmt_port}{path}"
    try:
        r = httpx.request(
            method,
            url,
            headers={"x-pgbattery-token": TOKEN, "Content-Type": "application/json"},
            timeout=600.0,
        )
        r.raise_for_status()
        parsed = r.json()
    except httpx.HTTPError as e:
        die(f"{method} {path}: {e}")
    except ValueError as e:
        die(f"{method} {path}: response was not JSON: {e}")
    if not isinstance(parsed, dict):
        die(f"{method} {path}: expected a JSON object")
    return {str(k): v for k, v in parsed.items()}


def compose_exec(node: Node, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "exec", "-T", node.service, *args]
    return subprocess.run(cmd, check=check, text=True, capture_output=True, env={**os.environ})


def psql_gateway(node: Node, sql: str) -> str:
    r = compose_exec(
        node,
        "psql",
        "-h",
        "127.0.0.1",
        "-p",
        str(topology.PG_CONTAINER_PORT),
        "-U",
        PG_USER,
        "-v",
        "ON_ERROR_STOP=1",
        "-t",
        "-A",
        "-c",
        sql,
        check=False,
    )
    if r.returncode != 0:
        die(f"psql failed: {r.stderr or r.stdout}")
    return r.stdout.strip()


def _pg_bin(node: Node) -> str:
    """Active bindir, from the node itself rather than a pinned major."""
    body = api(node, "GET", "/api/v1/postgres/status")
    bindir = str(body.get("active_bin_dir", "")).strip()
    if not bindir:
        die(f"{node.service} reports no active PostgreSQL bindir")
    return bindir


@app.command()
def main() -> None:
    if not TOKEN:
        die("PGBATTERY_MANAGEMENT_API_TOKEN is required")
    node = load().nodes[0]
    pg_bin = _pg_bin(node)

    # Prove archive_mode is on in the managed conf.
    conf = compose_exec(node, "cat", "/var/lib/postgresql/data/postgresql.conf").stdout
    if "archive_mode = on" not in conf:
        die("postgresql.conf does not have archive_mode = on — set backup.wal_archive_dir")

    compose_exec(node, "mkdir", "-p", WAL_ARCHIVE)

    marker = f"pitr-{int(time.time())}"
    psql_gateway(
        node,
        "CREATE TABLE IF NOT EXISTS pitr_proof("
        "id text PRIMARY KEY, when_ts timestamptz DEFAULT now());",
    )
    psql_gateway(
        node,
        f"INSERT INTO pitr_proof(id) VALUES ('pre-{marker}') ON CONFLICT DO NOTHING;",
    )
    psql_gateway(node, "SELECT pg_switch_wal();")

    # Wait for at least one archived segment.
    deadline = time.time() + 90
    while time.time() < deadline:
        listing = compose_exec(
            node, "sh", "-c", f"ls -1 {WAL_ARCHIVE} 2>/dev/null | wc -l"
        ).stdout.strip()
        if listing.isdigit() and int(listing) > 0:
            break
        time.sleep(2)
    else:
        die(f"no WAL segments appeared under {WAL_ARCHIVE} within 90s")

    created = api(node, "POST", "/api/v1/backup/create?type=full")
    backup_path = created.get("path")
    if not created.get("success") or not backup_path:
        die(f"backup create failed: {created}")

    psql_gateway(node, f"INSERT INTO pitr_proof(id) VALUES ('post-{marker}');")
    # The segment holding the post-backup marker, named before the switch.
    # Segments archive in order, so this one reaching the archive proves the
    # marker is recoverable — a fixed pause proves nothing either way.
    marker_segment = psql_gateway(node, "SELECT pg_walfile_name(pg_current_wal_insert_lsn());")
    psql_gateway(node, "SELECT pg_switch_wal();")
    deadline = time.time() + 90
    while time.time() < deadline:
        probe = compose_exec(node, "test", "-f", f"{WAL_ARCHIVE}/{marker_segment}", check=False)
        if probe.returncode == 0:
            break
        time.sleep(2)
    else:
        die(
            f"segment {marker_segment} holding the post-backup marker never "
            f"reached {WAL_ARCHIVE} within 90s"
        )

    scratch_in = f"/tmp/pitr-proof-{marker}"
    compose_exec(node, "rm", "-rf", scratch_in)
    compose_exec(node, "mkdir", "-p", scratch_in)

    # Materialize an uncompressed datadir from the backup path inside the node.
    r = compose_exec(
        node,
        "sh",
        "-c",
        f"""
set -e
if [ -d "{backup_path}" ]; then
  if [ -f "{backup_path}/base.tar.gz" ]; then
    mkdir -p {scratch_in}/data
    tar -xzf {backup_path}/base.tar.gz -C {scratch_in}/data
  else
    cp -a {backup_path}/. {scratch_in}/data/
  fi
else
  echo "backup path is not a directory: {backup_path}" >&2
  exit 2
fi
test -d {scratch_in}/data
# mkdir/tar leave the directory at the shell's umask; the postmaster refuses
# to start on anything looser than 0750.
chmod 0700 {scratch_in}/data
cat > {scratch_in}/data/postgresql.auto.conf <<EOF
restore_command = 'cp {WAL_ARCHIVE}/%f %p'
recovery_target_action = 'promote'
port = {SCRATCH_PORT}
EOF
touch {scratch_in}/data/recovery.signal
rm -f {scratch_in}/data/standby.signal
""",
    )
    if r.returncode != 0:
        die(f"prepare scratch datadir failed: {r.stderr}")

    # Refuse to claim coverage if we never actually started recovery.
    compose_exec(
        node,
        "sh",
        "-c",
        f"test -f {scratch_in}/data/recovery.signal",
    )

    # Start throwaway postmaster; wait for ready; assert markers; write; stop.
    # `pg_ctl -l` sends the reason it refused to start to pg.log, so a failure
    # here must report that file rather than "could not start server".
    start = compose_exec(
        node,
        "sh",
        "-c",
        f"""
set -e
{pg_bin}/pg_ctl -D {scratch_in}/data -l {scratch_in}/pg.log start
for i in $(seq 1 60); do
  {pg_bin}/pg_isready -h 127.0.0.1 -p {SCRATCH_PORT} -U {PG_USER} && break
  sleep 1
done
{pg_bin}/pg_isready -h 127.0.0.1 -p {SCRATCH_PORT} -U {PG_USER}
""",
        check=False,
    )
    if start.returncode != 0:
        log = compose_exec(node, "cat", f"{scratch_in}/pg.log", check=False).stdout
        die(
            f"restored cluster did not start under {pg_bin} "
            f"(rc={start.returncode}): {start.stderr or start.stdout}\n"
            f"--- pg.log ---\n{log}"
        )

    def scratch_psql(sql: str) -> str:
        r = compose_exec(
            node,
            "psql",
            "-h",
            "127.0.0.1",
            "-p",
            str(SCRATCH_PORT),
            "-U",
            PG_USER,
            "-v",
            "ON_ERROR_STOP=1",
            "-t",
            "-A",
            "-c",
            sql,
            check=False,
        )
        if r.returncode != 0:
            log = compose_exec(node, "cat", f"{scratch_in}/pg.log", check=False).stdout
            die(f"scratch psql failed: {r.stderr}\n--- pg.log ---\n{log}")
        return r.stdout.strip()

    try:
        pre = scratch_psql(f"SELECT count(*) FROM pitr_proof WHERE id = 'pre-{marker}';")
        post = scratch_psql(f"SELECT count(*) FROM pitr_proof WHERE id = 'post-{marker}';")
        if pre != "1":
            die(f"pre-backup marker missing after PITR restore (got {pre!r})")
        if post != "1":
            die(
                f"post-backup marker missing after PITR restore (got {post!r}) — "
                "WAL archive did not apply; restore-and-start proof failed"
            )
        scratch_psql(f"INSERT INTO pitr_proof(id) VALUES ('write-{marker}');")
        wrote = scratch_psql(f"SELECT count(*) FROM pitr_proof WHERE id = 'write-{marker}';")
        if wrote != "1":
            die("restored cluster did not accept a write")
    finally:
        compose_exec(
            node,
            "sh",
            "-c",
            f"{pg_bin}/pg_ctl -D {scratch_in}/data stop -m fast || true; rm -rf {scratch_in}",
            check=False,
        )

    console.print("pitr_restore_proof: PASS")


if __name__ == "__main__":
    app()
