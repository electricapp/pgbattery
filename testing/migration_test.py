#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "typer>=0.12",
#   "rich>=13",
#   "psycopg[binary]>=3.2",
# ]
# ///
"""Hold a single gateway connection and write/read in a loop.

Prints a timestamped record for every query.  If connection migration
works, this should keep going through a leader transfer; if it doesn't,
we'll see an error mid-stream.
"""

from __future__ import annotations

import time
from typing import Annotated

import psycopg
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False, pretty_exceptions_show_locals=False)
console = Console()


@app.command()
def run(
    host: Annotated[str, typer.Option(help="Gateway host")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Gateway port")] = 5432,
    user: Annotated[str, typer.Option(help="Postgres user")] = "postgres",
    dbname: Annotated[str, typer.Option(help="Database name")] = "postgres",
    duration: Annotated[int, typer.Option(help="Test duration (seconds)")] = 30,
    interval: Annotated[float, typer.Option(help="Seconds between queries")] = 1.0,
) -> None:
    """Write to the gateway every --interval seconds for --duration seconds."""
    conn_str = f"host={host} port={port} user={user} dbname={dbname}"
    console.print(f"[dim]connecting: {conn_str}[/dim]")

    conn = psycopg.connect(conn_str, autocommit=True)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS migration_test ("
        "id BIGSERIAL PRIMARY KEY, "
        "ts TIMESTAMPTZ NOT NULL DEFAULT now(), "
        "note TEXT)"
    )
    console.print("[green]connected[/green], starting write loop\n")

    started = time.time()
    query_num = 0
    errors = 0
    first_pid: int | None = None

    while time.time() - started < duration:
        query_num += 1
        elapsed = time.time() - started
        t0 = time.time()
        try:
            cur = conn.execute(
                "INSERT INTO migration_test (note) VALUES (%s) "
                "RETURNING id, pg_backend_pid(), inet_server_addr()",
                (f"q{query_num}",),
            )
            row = cur.fetchone()
            assert row is not None
            row_id, pid, server_addr = row
            dt_ms = (time.time() - t0) * 1000

            tag = ""
            if first_pid is None:
                first_pid = pid
            elif pid != first_pid:
                tag = " [yellow bold]PID CHANGED (backend migrated)[/yellow bold]"
                first_pid = pid

            console.print(
                f"[cyan]{elapsed:6.2f}s[/cyan] q{query_num:03d} "
                f"[green]OK[/green] id={row_id} pid={pid} "
                f"server={server_addr} [dim]{dt_ms:.1f}ms[/dim]{tag}"
            )
        except Exception as e:
            errors += 1
            dt_ms = (time.time() - t0) * 1000
            console.print(
                f"[cyan]{elapsed:6.2f}s[/cyan] q{query_num:03d} "
                f"[red]ERR[/red] [dim]{dt_ms:.1f}ms[/dim] "
                f"{type(e).__name__}: {e}"
            )
            if conn.closed:
                console.print("  [yellow]connection closed — reconnecting...[/yellow]")
                try:
                    conn = psycopg.connect(conn_str, autocommit=True)
                    console.print("  [green]reconnected[/green]")
                    first_pid = None
                except Exception as e2:
                    console.print(f"  [red]reconnect failed:[/red] {e2}")
                    raise typer.Exit(1) from e2

        time.sleep(interval)

    # Summary table
    table = Table(title="Migration Test Summary", show_header=True)
    table.add_column("Metric", style="bold")
    table.add_column("Value")
    table.add_row("Duration", f"{duration}s")
    table.add_row("Queries", str(query_num))
    table.add_row("Errors", str(errors))
    table.add_row(
        "Result",
        "[green]PASS[/green]" if errors == 0 else f"[red]FAIL ({errors} errors)[/red]",
    )
    console.print()
    console.print(table)

    raise typer.Exit(0 if errors == 0 else 2)


if __name__ == "__main__":
    app()
