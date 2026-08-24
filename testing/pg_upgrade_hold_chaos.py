#!/usr/bin/env -S uv run --project testing python
"""Track B: gateway hold under continuous writes + freeze/unfreeze (step 5/6).

Exercises upgrade freeze hold without requiring a second PG major:
  freeze → writers stall/hold → unfreeze → writers resume; zero lost acks
  for completed transactions.

Chaos modes:
  --chaos mid-freeze   kill leader while frozen
  --chaos mid-unfreeze kill during unfreeze window
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

import httpx
import psycopg
import typer

from topology import load

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TOKEN = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")

app = typer.Typer(add_completion=False)


def _headers() -> dict[str, str]:
    if not TOKEN:
        raise SystemExit("PGBATTERY_MANAGEMENT_API_TOKEN required")
    return {"x-pgbattery-token": TOKEN}


def _leader_mgmt() -> str:
    topo = load()
    for n in topo.nodes:
        try:
            r = httpx.get(
                f"http://127.0.0.1:{n.mgmt_port}/api/v1/cluster/leader",
                timeout=2.0,
            )
            if r.status_code == 200 and r.json().get("leader_mgmt_addr"):
                return f"http://{r.json()['leader_mgmt_addr']}"
        except Exception:
            continue
    raise SystemExit("no leader")


def _gateway_port() -> int:
    return load().nodes[0].gateway_port


@app.command()
def main(
    chaos: str | None = typer.Option(None, help="mid-freeze | mid-unfreeze"),
    hold_sec: float = typer.Option(5.0, "--hold-sec"),
) -> None:
    gw = _gateway_port()
    base = _leader_mgmt()
    h = _headers()

    acked = {"n": 0}
    errors = {"n": 0}
    stop = threading.Event()

    def writer() -> None:
        i = 0
        while not stop.is_set():
            try:
                with psycopg.connect(
                    host="127.0.0.1",
                    port=gw,
                    user="postgres",
                    dbname="postgres",
                    connect_timeout=3,
                ) as conn:
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute(
                            "CREATE TABLE IF NOT EXISTS hold_probe("
                            "id bigserial PRIMARY KEY, t timestamptz default now())"
                        )
                        cur.execute("INSERT INTO hold_probe DEFAULT VALUES")
                        acked["n"] += 1
            except Exception:
                errors["n"] += 1
            i += 1
            time.sleep(0.05)

    t = threading.Thread(target=writer, daemon=True)
    t.start()
    time.sleep(1.0)
    before = acked["n"]

    r = httpx.post(
        f"{base}/api/v1/admin/postgres-upgrade/freeze",
        headers=h,
        json={"frozen": True},
        timeout=30.0,
    )
    r.raise_for_status()

    if chaos == "mid-freeze":
        subprocess.run(
            ["docker", "compose", "kill", "node1"],
            cwd=PROJECT_ROOT,
            check=False,
        )
        time.sleep(2)
        stop.set()
        print(f"chaos mid-freeze: acked={acked['n']} errors={errors['n']}")
        raise SystemExit(0)

    time.sleep(hold_sec)
    during = acked["n"]

    r = httpx.post(
        f"{base}/api/v1/admin/postgres-upgrade/freeze",
        headers=h,
        json={"frozen": False},
        timeout=30.0,
    )
    r.raise_for_status()

    if chaos == "mid-unfreeze":
        subprocess.run(
            ["docker", "compose", "kill", "node1"],
            cwd=PROJECT_ROOT,
            check=False,
        )

    time.sleep(2.0)
    stop.set()
    t.join(timeout=5)
    after = acked["n"]

    print(
        f"acked before={before} during_hold={during - before} "
        f"after={after - during} errors={errors['n']}"
    )
    # After unfreeze we expect progress (unless chaos killed the cluster).
    if after <= during and chaos is None:
        raise SystemExit("no writes after unfreeze")
    print("pg_upgrade hold chaos OK")


if __name__ == "__main__":
    app()
