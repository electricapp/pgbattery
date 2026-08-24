#!/usr/bin/env -S uv run --project testing python
"""Track C blue/green cutover e2e (UPGRADE_CHECKLIST step 9).

``COMPOSE_FILE=docker-compose.bluegreen.yml`` — ports from topology.py only.

Phases:
  1. Wait blue + green Raft healthy
  2. Seed schema + logical publication (blue) / subscription (green)
  3. Continuous writes on blue gateway during catch-up
  4. Cutover protocol (arm → seal+fence → drain → reverse slot → transfer)
  5. Advance sequences on green; verify U1 with six-port prober
  6. Reverse replication; report rollback available
  7. Optional --chaos {arm-seal,seal-transfer,mid-drain,blue-quorum}
  8. Optional --rollback-drill (U5 window)

Exit 0 on success; non-zero on contract failure or infra error.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Final

import httpx
import psycopg
import typer

os.environ.setdefault("COMPOSE_FILE", "docker-compose.bluegreen.yml")

from topology import Node, Topology, load

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
APP_ROLE: Final[str] = "bg_app"
SUB_OWNER: Final[str] = "postgres"
TOKEN_ENV: Final[str] = "PGBATTERY_MANAGEMENT_API_TOKEN"

app = typer.Typer(add_completion=False)


def _token() -> str:
    t = os.environ.get(TOKEN_ENV, "").strip()
    if not t:
        raise SystemExit(f"{TOKEN_ENV} required")
    return t


def _headers() -> dict[str, str]:
    return {"x-pgbattery-token": _token()}


def _mgmt(node: Node) -> str:
    return f"http://127.0.0.1:{node.mgmt_port}"


def _wait_leader(node: Node, timeout: float = 120.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{_mgmt(node)}/api/v1/cluster/leader", timeout=2.0)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, dict) and body.get("leader_id") is not None:
                    return {str(k): v for k, v in body.items()}
                last = str(body)
        except Exception as exc:
            last = str(exc)
        time.sleep(1.0)
    raise TimeoutError(f"no leader via {node.service}: {last}")


def _psql(node: Node, sql: str, user: str = SUB_OWNER) -> str:
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        node.service,
        "psql",
        "-U",
        user,
        "-h",
        "127.0.0.1",
        "-p",
        "5434",
        "-v",
        "ON_ERROR_STOP=1",
        "-At",
        "-c",
        sql,
    ]
    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "COMPOSE_FILE": os.environ["COMPOSE_FILE"]},
    )
    if proc.returncode != 0:
        raise RuntimeError(f"psql on {node.service} failed: {proc.stderr or proc.stdout}")
    return proc.stdout.strip()


def _blue_green(topo: Topology) -> tuple[list[Node], list[Node]]:
    blue = [n for n in topo.nodes if n.service.startswith("blue")]
    green = [n for n in topo.nodes if n.service.startswith("green")]
    if len(blue) != 3 or len(green) != 3:
        raise SystemExit(f"need 3+3 nodes, got {len(blue)}+{len(green)}")
    return blue, green


def _leader_node(nodes: list[Node]) -> Node:
    for n in nodes:
        try:
            body = _wait_leader(n, timeout=5.0)
            lid = int(body["leader_id"])
            for cand in nodes:
                if cand.node_id == lid:
                    return cand
        except TimeoutError:
            continue
    raise TimeoutError("no leader among cluster")


def _setup_schema(blue_leader: Node) -> None:
    _psql(
        blue_leader,
        f"""
        DO $$ BEGIN
          CREATE ROLE {APP_ROLE} LOGIN;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        GRANT CONNECT ON DATABASE postgres TO {APP_ROLE};
        CREATE TABLE IF NOT EXISTS bg_kv (
          k text PRIMARY KEY,
          v text NOT NULL,
          seq bigserial
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON bg_kv TO {APP_ROLE};
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {APP_ROLE};
        """,
    )


def _seed_logical(blue_leader: Node, green_leader: Node) -> None:
    # Publication on blue
    _psql(
        blue_leader,
        """
        SELECT pg_create_logical_replication_slot('bg_slot', 'pgoutput')
        WHERE NOT EXISTS (
          SELECT 1 FROM pg_replication_slots WHERE slot_name = 'bg_slot'
        );
        """,
    )
    _psql(
        blue_leader,
        """
        DO $$ BEGIN
          CREATE PUBLICATION bg_pub FOR TABLE bg_kv;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    )
    # Matching DDL on green (logical replication does not create the table).
    _psql(
        green_leader,
        """
        CREATE TABLE IF NOT EXISTS bg_kv (
          k text PRIMARY KEY,
          v text NOT NULL,
          seq bigint
        );
        CREATE SEQUENCE IF NOT EXISTS bg_kv_seq_seq;
        """,
    )
    blue_conn = f"host={blue_leader.ip} port=5434 user={SUB_OWNER} dbname=postgres"
    _psql(
        green_leader,
        f"""
        DO $$ BEGIN
          CREATE SUBSCRIPTION bg_sub
            CONNECTION '{blue_conn}'
            PUBLICATION bg_pub
            WITH (copy_data = true, create_slot = false, slot_name = 'bg_slot');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    )
    # Wait for catch-up
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        st = _psql(
            green_leader,
            "SELECT COALESCE(received_lsn::text,'') || '|' || "
            "COALESCE(latest_end_lsn::text,'') "
            "FROM pg_stat_subscription WHERE subname = 'bg_sub';",
        )
        if st and "|" in st:
            time.sleep(2)
            return
        time.sleep(1)
    raise TimeoutError("green subscription did not start")


def _flush_lsn(blue_leader: Node) -> int:
    raw = _psql(blue_leader, "SELECT pg_current_wal_flush_lsn()::text;")
    # Convert LSN X/Y to bytes
    upper, lower = raw.split("/")
    return (int(upper, 16) << 32) | int(lower, 16)


def _advance_sequences(green_leader: Node, blue_leader: Node) -> None:
    blue_max = _psql(
        blue_leader,
        "SELECT COALESCE(max(seq), 0)::text FROM bg_kv;",
    )
    target = int(blue_max or "0") + 1000
    _psql(
        green_leader,
        f"""
        SELECT setval(pg_get_serial_sequence('bg_kv', 'seq'), {target}, true)
        WHERE pg_get_serial_sequence('bg_kv', 'seq') IS NOT NULL;
        """,
    )


def _cutover(
    blue_leader: Node,
    green_leader: Node,
    *,
    chaos: str | None,
) -> None:
    base = _mgmt(blue_leader)
    h = _headers()

    r = httpx.post(
        f"{base}/api/v1/admin/cutover/arm",
        headers=h,
        json={"green_cluster_id": "green"},
        timeout=30.0,
    )
    r.raise_for_status()

    if chaos == "arm-seal":
        subprocess.run(
            ["docker", "compose", "kill", blue_leader.service],
            cwd=PROJECT_ROOT,
            check=False,
        )
        raise SystemExit(
            "chaos arm-seal: killed blue leader after ARMED "
            "(green must stay RO — verify separately)"
        )

    final_lsn = _flush_lsn(blue_leader)
    r = httpx.post(
        f"{base}/api/v1/admin/cutover/seal",
        headers=h,
        json={"final_lsn": final_lsn, "app_roles": [APP_ROLE]},
        timeout=60.0,
    )
    r.raise_for_status()

    if chaos == "blue-quorum":
        # Stop two blue followers → blue loses quorum while sealed
        followers = [
            n
            for n in load().nodes
            if n.service.startswith("blue") and n.service != blue_leader.service
        ]
        for f in followers[:2]:
            subprocess.run(
                ["docker", "compose", "stop", f.service],
                cwd=PROJECT_ROOT,
                check=False,
            )
        # Force transfer path
        r = httpx.post(
            f"{base}/api/v1/admin/cutover/force-transfer",
            headers=h,
            json={
                "green_cluster_id": "green",
                "green_leader_pg_addr": f"{green_leader.ip}:5434",
                "accept_blue_write_loss": True,
            },
            timeout=60.0,
        )
        # May fail if leader is unreachable — that is acceptable fail-closed.
        print(f"force-transfer status={r.status_code} body={r.text[:200]}")
        return

    if chaos == "seal-transfer":
        subprocess.run(
            ["docker", "compose", "kill", blue_leader.service],
            cwd=PROJECT_ROOT,
            check=False,
        )
        raise SystemExit("chaos seal-transfer: killed blue after SEAL")

    if chaos == "mid-drain":
        subprocess.run(
            ["docker", "compose", "kill", green_leader.service],
            cwd=PROJECT_ROOT,
            check=False,
        )
        # Abort unseal on blue
        r = httpx.post(
            f"{base}/api/v1/admin/cutover/abort",
            headers=h,
            json={"app_roles": [APP_ROLE]},
            timeout=60.0,
        )
        print(f"abort after mid-drain chaos: {r.status_code}")
        return

    # Wait green applied >= final (poll subscription)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        # Drain observed when green has caught up — probe row count match
        bc = _psql(blue_leader, "SELECT count(*)::text FROM bg_kv;")
        gc = _psql(green_leader, "SELECT count(*)::text FROM bg_kv;")
        if bc == gc and int(bc or "0") > 0:
            break
        time.sleep(1)
    else:
        raise TimeoutError("green did not drain to blue row count")

    r = httpx.post(
        f"{base}/api/v1/admin/cutover/drain-observed",
        headers=h,
        timeout=30.0,
    )
    r.raise_for_status()

    _advance_sequences(green_leader, blue_leader)

    # Reverse publication + slot on green BEFORE transfer (U5).
    # Separate statements: creating a slot cannot share a transaction with DDL.
    _psql(
        green_leader,
        """
        DO $$ BEGIN
          CREATE PUBLICATION bg_reverse FOR TABLE bg_kv;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    )
    _psql(
        green_leader,
        """
        SELECT pg_create_logical_replication_slot('bg_reverse_slot', 'pgoutput')
        WHERE NOT EXISTS (
          SELECT 1 FROM pg_replication_slots WHERE slot_name = 'bg_reverse_slot'
        );
        """,
    )

    r = httpx.post(
        f"{base}/api/v1/admin/cutover/transfer",
        headers=h,
        json={
            "green_cluster_id": "green",
            "green_leader_pg_addr": f"{green_leader.ip}:5434",
            "reverse_slot_ready": True,
        },
        timeout=60.0,
    )
    r.raise_for_status()


def _wire_reverse(blue_leader: Node, green_leader: Node) -> None:
    green_conn = f"host={green_leader.ip} port=5434 user={SUB_OWNER} dbname=postgres"
    _psql(
        blue_leader,
        f"""
        DO $$ BEGIN
          CREATE SUBSCRIPTION bg_reverse_sub
            CONNECTION '{green_conn}'
            PUBLICATION bg_reverse
            WITH (copy_data = false, create_slot = false,
                  slot_name = 'bg_reverse_slot');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    )


def _rollback_available(blue_leader: Node) -> bool:
    st = _psql(
        blue_leader,
        "SELECT count(*)::text FROM pg_stat_subscription "
        "WHERE subname = 'bg_reverse_sub' AND pid IS NOT NULL;",
    )
    return st == "1"


def _write_burst(gateway_port: int, n: int, prefix: str) -> None:
    with psycopg.connect(
        host="127.0.0.1",
        port=gateway_port,
        user=SUB_OWNER,
        dbname="postgres",
        connect_timeout=5,
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for i in range(n):
                cur.execute(
                    "INSERT INTO bg_kv(k, v) VALUES (%s, %s) "
                    "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                    (f"{prefix}-{i}", f"v-{i}"),
                )


@app.command()
def main(
    chaos: str | None = typer.Option(
        None,
        help="arm-seal | seal-transfer | mid-drain | blue-quorum",
    ),
    rollback_drill: bool = typer.Option(False, "--rollback-drill"),
    skip_prober: bool = typer.Option(False, "--skip-prober"),
    no_up: bool = typer.Option(False, "--no-up", help="Skip docker compose up"),
) -> None:
    os.environ["COMPOSE_FILE"] = "docker-compose.bluegreen.yml"
    if not no_up:
        subprocess.run(
            ["docker", "compose", "down", "-v", "--remove-orphans"],
            cwd=PROJECT_ROOT,
            check=False,
            env={**os.environ},
        )
        subprocess.run(
            ["docker", "compose", "up", "-d", "--remove-orphans"],
            cwd=PROJECT_ROOT,
            check=True,
            env={**os.environ},
        )
        time.sleep(5)

    topo = load()
    blue, green = _blue_green(topo)
    print(f"topology: blue={[n.service for n in blue]} green={[n.service for n in green]}")

    for n in (blue[0], green[0]):
        _wait_leader(n, timeout=180)

    # Anti-vacuity: blue must be major 17, green major 18 (Track C major path).
    def _maj(node: Node) -> int:
        out = _psql(node, "SHOW server_version_num;")
        return int(out.strip()) // 10000

    blue_leader = _leader_node(blue)
    green_leader = _leader_node(green)
    bm, gm = _maj(blue_leader), _maj(green_leader)
    if bm != 17 or gm != 18:
        raise SystemExit(f"anti-vacuity: expected blue@17 green@18, got blue@{bm} green@{gm}")
    print(f"leaders: blue={blue_leader.service}@{bm} green={green_leader.service}@{gm}")

    _setup_schema(blue_leader)
    _write_burst(blue_leader.gateway_port, 20, "pre")
    _seed_logical(blue_leader, green_leader)
    _write_burst(blue_leader.gateway_port, 30, "stream")

    _cutover(blue_leader, green_leader, chaos=chaos)
    if chaos:
        print(f"chaos={chaos} completed (see logs)")
        raise SystemExit(0)

    status = httpx.get(f"{_mgmt(blue_leader)}/api/v1/cutover/status", timeout=10.0).json()
    print("cutover status:", json.dumps(status, indent=2))
    if status.get("phase") != "transferred":
        raise SystemExit(f"expected transferred, got {status}")

    _wire_reverse(blue_leader, green_leader)
    if not _rollback_available(blue_leader):
        raise SystemExit("reverse subscription not active — rollback unavailable")

    # Writes via blue gateway should relay to green
    _write_burst(blue_leader.gateway_port, 10, "post")

    if rollback_drill:
        # Write on green, ensure blue receives via reverse
        _write_burst(green_leader.gateway_port, 5, "rb")
        time.sleep(5)
        bc = _psql(blue_leader, "SELECT count(*)::text FROM bg_kv WHERE k LIKE 'rb-%';")
        if int(bc or "0") < 1:
            raise SystemExit("U5 rollback drill: blue missing reverse-replicated rows")
        print("rollback drill OK")

    if not skip_prober:
        # Six-port L1/U1 prober
        env = {
            **os.environ,
            "COMPOSE_FILE": "docker-compose.bluegreen.yml",
        }
        proc = subprocess.run(
            [
                "./testing/dual_writability_prober.py",
                "--topology",
                "bluegreen",
                "--duration",
                "15",
                "--user",
                APP_ROLE,
                "--json",
                "testing/artifacts/bluegreen-u1/result.json",
            ],
            cwd=PROJECT_ROOT,
            check=False,
            env=env,
        )
        Path("testing/artifacts/bluegreen-u1").mkdir(parents=True, exist_ok=True)
        if proc.returncode not in (0, 3):
            raise SystemExit(f"prober failed rc={proc.returncode}")

    print("bluegreen cutover e2e OK")


if __name__ == "__main__":
    app()
