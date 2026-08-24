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

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

TOKEN = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")
MGMT = os.environ.get("PGBATTERY_PITR_MGMT", "http://127.0.0.1:9081")
GATEWAY = os.environ.get("PGBATTERY_PITR_GATEWAY", "127.0.0.1")
GATEWAY_PORT = int(os.environ.get("PGBATTERY_PITR_GATEWAY_PORT", "5432"))
COMPOSE = os.environ.get("COMPOSE_FILE", "docker-compose.yml")
NODE = os.environ.get("PGBATTERY_PITR_NODE", "node1")
WAL_ARCHIVE = "/var/lib/postgresql/wal_archive"
PG_BIN = "/usr/lib/postgresql/18/bin"
PG_USER = "postgres"


def die(msg: str) -> None:
    print(f"pitr_restore_proof: FAIL: {msg}", file=sys.stderr)
    raise SystemExit(1)


def api(method: str, path: str, body: dict[str, object] | None = None) -> dict[str, object]:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"{MGMT}{path}",
        data=data,
        method=method,
        headers={
            "x-pgbattery-token": TOKEN,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            raw = resp.read().decode()
    except urllib.error.HTTPError as e:
        die(f"{method} {path}: HTTP {e.code}: {e.read().decode()}")
    except Exception as e:
        die(f"{method} {path}: {e}")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        die(f"{method} {path}: expected JSON object, got {type(parsed).__name__}")
    out: dict[str, object] = {str(k): v for k, v in parsed.items()}
    return out


def compose_exec(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", COMPOSE, "exec", "-T", NODE, *args]
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def psql_gateway(sql: str) -> str:
    cmd = [
        "docker",
        "compose",
        "-f",
        COMPOSE,
        "exec",
        "-T",
        NODE,
        "psql",
        "-h",
        "127.0.0.1",
        "-p",
        "5432",
        "-U",
        PG_USER,
        "-v",
        "ON_ERROR_STOP=1",
        "-t",
        "-A",
        "-c",
        sql,
    ]
    r = subprocess.run(cmd, check=False, text=True, capture_output=True)
    if r.returncode != 0:
        die(f"psql failed: {r.stderr or r.stdout}")
    return r.stdout.strip()


def main() -> None:
    if not TOKEN:
        die("PGBATTERY_MANAGEMENT_API_TOKEN is required")

    # Prove archive_mode is on in the managed conf.
    conf = compose_exec("cat", "/var/lib/postgresql/data/postgresql.conf").stdout
    if "archive_mode = on" not in conf:
        die("postgresql.conf does not have archive_mode = on — set backup.wal_archive_dir")

    compose_exec("mkdir", "-p", WAL_ARCHIVE)

    marker = f"pitr-{int(time.time())}"
    psql_gateway(
        "CREATE TABLE IF NOT EXISTS pitr_proof("
        "id text PRIMARY KEY, when_ts timestamptz DEFAULT now());"
    )
    psql_gateway(f"INSERT INTO pitr_proof(id) VALUES ('pre-{marker}') ON CONFLICT DO NOTHING;")
    psql_gateway("SELECT pg_switch_wal();")

    # Wait for at least one archived segment.
    deadline = time.time() + 90
    while time.time() < deadline:
        listing = compose_exec(
            "sh", "-c", f"ls -1 {WAL_ARCHIVE} 2>/dev/null | wc -l"
        ).stdout.strip()
        if listing.isdigit() and int(listing) > 0:
            break
        time.sleep(2)
    else:
        die(f"no WAL segments appeared under {WAL_ARCHIVE} within 90s")

    created = api("POST", "/api/v1/backup/create?type=full")
    backup_path = created.get("path")
    if not created.get("success") or not backup_path:
        die(f"backup create failed: {created}")

    psql_gateway(f"INSERT INTO pitr_proof(id) VALUES ('post-{marker}');")
    psql_gateway("SELECT pg_switch_wal();")
    time.sleep(3)  # allow archive_command to copy the post-backup segment

    scratch_in = f"/tmp/pitr-proof-{marker}"
    compose_exec("rm", "-rf", scratch_in)
    compose_exec("mkdir", "-p", scratch_in)

    # Materialize an uncompressed datadir from the backup path inside the node.
    r = compose_exec(
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
cat > {scratch_in}/data/postgresql.auto.conf <<EOF
restore_command = 'cp {WAL_ARCHIVE}/%f %p'
recovery_target_action = 'promote'
port = 55432
EOF
touch {scratch_in}/data/recovery.signal
rm -f {scratch_in}/data/standby.signal
""",
    )
    if r.returncode != 0:
        die(f"prepare scratch datadir failed: {r.stderr}")

    # Refuse to claim coverage if we never actually started recovery.
    compose_exec(
        "sh",
        "-c",
        f"test -f {scratch_in}/data/recovery.signal",
    )

    # Start throwaway postmaster; wait for ready; assert markers; write; stop.
    compose_exec(
        "sh",
        "-c",
        f"""
set -e
{PG_BIN}/pg_ctl -D {scratch_in}/data -l {scratch_in}/pg.log start
for i in $(seq 1 60); do
  {PG_BIN}/pg_isready -h 127.0.0.1 -p 55432 -U {PG_USER} && break
  sleep 1
done
{PG_BIN}/pg_isready -h 127.0.0.1 -p 55432 -U {PG_USER}
""",
    )

    def scratch_psql(sql: str) -> str:
        r = compose_exec(
            "psql",
            "-h",
            "127.0.0.1",
            "-p",
            "55432",
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
            log = compose_exec("cat", f"{scratch_in}/pg.log", check=False).stdout
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
            "sh",
            "-c",
            f"{PG_BIN}/pg_ctl -D {scratch_in}/data stop -m fast || true; rm -rf {scratch_in}",
            check=False,
        )

    print("pitr_restore_proof: PASS")


if __name__ == "__main__":
    main()
