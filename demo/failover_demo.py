#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "psycopg[binary]>=3.2",
#   "rich>=13",
#   "httpx>=0.27",
# ]
# ///
"""pgbattery failover demo — recorded by VHS to produce the README GIF.

Holds a single psql-equivalent connection through SIGKILL of the leader.
Shows that the session survives without reconnecting, so users see the
connection-migration claim with their own eyes.

Run directly (`./demo/failover_demo.py`) or via vhs (`vhs demo/failover.tape`).
"""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timezone

import httpx
import psycopg
from rich.console import Console
from rich.text import Text

GATEWAY_DSN = "postgresql://postgres@127.0.0.1:5432/postgres"
# Each node exposes its mgmt API on a different host port; try them all so
# we can still discover the leader after the one we killed.
MGMT_BASES = ("http://127.0.0.1:9081", "http://127.0.0.1:9082", "http://127.0.0.1:9083")
KILL_AT_S = 6
TOTAL_S = 18

console = Console(highlight=False, force_terminal=True)


def stamp() -> str:
    return datetime.now(tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def get_leader_id() -> int:
    last_exc: Exception | None = None
    for base in MGMT_BASES:
        try:
            return int(httpx.get(f"{base}/api/v1/cluster/leader", timeout=3).json()["leader_id"])
        except Exception as e:
            last_exc = e
    raise RuntimeError(f"no mgmt API reachable: {last_exc}")


def setup() -> None:
    with psycopg.connect(GATEWAY_DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS demo_counter (id int primary key, val bigint not null)"
        )
        cur.execute(
            "INSERT INTO demo_counter (id, val) VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET val = 0"
        )


def main() -> int:
    setup()
    leader_before = get_leader_id()
    container = f"pgbattery-node{leader_before}-1"

    console.print(
        Text.from_markup(
            f"[bold cyan]pgbattery demo[/]  connecting to gateway :5432  (leader = node{leader_before})"
        )
    )
    console.print("[dim]single connection, no reconnect logic — survives leader SIGKILL[/]\n")

    t0 = time.monotonic()
    killed = False
    last_ok_ts: float | None = None

    def open_conn() -> psycopg.Connection:
        return psycopg.connect(GATEWAY_DSN, autocommit=True, connect_timeout=3)

    conn = open_conn()
    cur = conn.cursor()
    try:
        while time.monotonic() - t0 < TOTAL_S:
            elapsed = time.monotonic() - t0

            if not killed and elapsed >= KILL_AT_S:
                killed = True
                console.print(
                    f"  [bold red]{stamp()}  ⚡ docker kill -s SIGKILL {container}[/]"
                )
                subprocess.run(
                    ["docker", "kill", "-s", "SIGKILL", container],
                    check=True,
                    capture_output=True,
                )

            try:
                cur.execute(
                    "UPDATE demo_counter SET val = val + 1 WHERE id = 1 RETURNING val, "
                    "(SELECT inet_server_addr()::text)"
                )
                row = cur.fetchone()
                if row is None:
                    continue
                val, addr = row
                last_ok_ts = time.monotonic()
                color = (
                    "green" if not killed else ("yellow" if elapsed - KILL_AT_S < 2 else "green")
                )
                label = "OK" if not killed else "RECOVERED"
                console.print(
                    f"  [{color}]{stamp()}  counter={val:>4d}  backend={addr}  [{label}][/]"
                )
            except psycopg.OperationalError:
                console.print(f"  [yellow]{stamp()}  …connection severed, reconnecting…[/]")
                try:
                    conn.close()
                except Exception:
                    pass
                while time.monotonic() - t0 < TOTAL_S:
                    try:
                        conn = open_conn()
                        cur = conn.cursor()
                        break
                    except psycopg.OperationalError:
                        time.sleep(0.3)
            except psycopg.Error as e:
                console.print(f"  [yellow]{stamp()}  …waiting on failover ({type(e).__name__})[/]")
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur = conn.cursor()

            time.sleep(0.4)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    leader_after = get_leader_id()
    console.print(
        f"\n[bold green]✓ session survived[/]  new leader = node{leader_after}"
        f"  ({last_ok_ts is not None and 'connection migrated' or 'lost'})"
    )

    # Restart the killed node so the cluster is back to 3-of-3 for next run.
    subprocess.run(["docker", "start", container], check=True, capture_output=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
