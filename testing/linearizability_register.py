#!/usr/bin/env -S uv run --project testing python
"""Single-register linearizability checker for pgbattery.

Spawns K concurrent client threads that issue read / write / CAS operations
against a small set of "register" rows (one row per key) through the
pgbattery gateway. Each operation is recorded with monotonic
invocation/response timestamps. The leader is killed mid-run to force a
failover. After recovery, the recorded operation history is checked for
linearizability per-key using the Wing-Gong-Lowe (WGL) search algorithm.

═══════════════════════════════════════════════════════════════════════════
WHY THIS FILE EXISTS ALONGSIDE correctness_lite.py
═══════════════════════════════════════════════════════════════════════════

correctness_lite.py verifies durability of acked writes and absence of
split-brain. It does NOT verify *ordering* of concurrent operations from a
client's perspective. Two writers and a reader, all hitting the same key
under a failover window, can produce a history where:

  - All acked writes survive (durability holds — correctness_lite passes).
  - The read returns a value that no linearization could possibly produce
    (e.g. an older value after a newer one was already observed).

That second class of bug — a stale-read, a lost-update, a write-skew — is
what this file is for. It checks the *real-time* relationship between
concurrent ops against the sequential specification of a register
({read returns last write, write replaces, CAS commits iff witness matches}).

═══════════════════════════════════════════════════════════════════════════
SCOPE & LIMITATIONS
═══════════════════════════════════════════════════════════════════════════

  - **Per-key register model only.** Each key is treated as an independent
    register; cross-key invariants (e.g. SUM(values) conservation) are
    out of scope. Use correctness_lite's bank-transfer step for that.

  - **WGL is exponential in concurrency, and two separate bounds apply.**
    `WGL_OPS_PER_KEY_CAP` bounds the per-key op count: a longer history is
    reduced to one contiguous window of that size centred near the median
    return time, with the register's starting value inferred from the
    discarded prefix, and ops outside the window are not checked. That caps
    memory, not time — cost tracks how many ops overlap, not how many there
    are. `WGL_MAX_EXPLORED_STATES` is the bound on the search itself.

    A key that spends its state budget is reported `UNCHECKED` and the run
    exits 2 (`INCONCLUSIVE`), never 0. Widening the workload past the point
    WGL can decide it is a real risk: at 6 workers over 45 s, five of eight
    keys were undecidable. Shrink the workload or use `--check weak`.

    Real Jepsen uses Knossos/Elle which have better-than-WGL constants and
    can also verify transactional histories. We do not.

  - **Indeterminate operations are encoded as "pending"** (`return_ts is
    None`, `result is None`) and both possible outcomes are considered: a
    write that timed out could have committed or not. Distinct from a
    **definite rejection** (`result is False` — PG refused the statement),
    which provably never reached the register and is therefore modelled as
    a no-op; a later read of its value is an anomaly, not a normal write.

═══════════════════════════════════════════════════════════════════════════
ALGORITHM (Wing-Gong-Lowe, register specialisation)
═══════════════════════════════════════════════════════════════════════════

A history H = [(op_i, invoke_i, return_i)] is linearizable iff there exists
a total order < of the completed operations in H such that:

  1. (REAL-TIME)  op_a returned before op_b invoked  ⇒  op_a < op_b.
  2. (SEQUENTIAL) The total order produces a valid sequential register
                  history when each op is applied to the register state.

Search:

  - Maintain `remaining` = set of ops not yet linearized.
  - At each step, consider only ops whose invocation is at-or-before the
    earliest return time in `remaining` (only these are eligible to be
    linearized next under the real-time constraint).
  - For each candidate, simulate the register transition; recurse with
    the candidate removed and the register state updated.
  - Memoize on (frozenset(remaining_ids), register_value) — a hash that
    captures the entire search state.
  - If recursion exhausts the frontier, history is linearizable.

A pending op has no return time, so it is treated as returning at +inf: it
stays eligible at every step and can always be deferred to the tail of the
order, where nothing observes it. Applying it earlier is the "this op
happened" branch; deferring it to the tail is "this op didn't happen". The
search needs no separate branch for the two. A definitely-rejected op is an
identity transition, so placing it is always legal and constrains nothing.

═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import random
import threading
import time
from collections.abc import Callable
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from linreg.attacks import (
    ATTACK_DISPATCH,
    SCAFFOLD_ATTACKS,
    SEEDED_ATTACKS,
    InjectorOutcome,
    run_injector,
    scrub_chaos_residue,
    start_killed_nodes,
)
from linreg.checkers import (
    _is_linearizable,
    _is_weakly_consistent,
)

# `find_leader` is re-exported, not used here: the Elle matrix's fault-wave
# driver reaches it through this module. `lint_matrix.py` pins that.
from linreg.cluster import find_leader as find_leader
from linreg.cluster import wait_cluster_healthy
from linreg.config import (
    CHAOS_STORM_DURATION,
    DEFAULT_KILL_LEADER_AFTER_SECONDS,
    DEFAULT_NUM_KEYS,
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKLOAD_DURATION_SECONDS,
    WorkloadConfig,
)
from linreg.records import History, Op
from linreg.workload import (
    list_append_worker_loop,
    setup_list_append_table,
    setup_table,
    txn_worker_loop,
    worker_loop,
)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Topology constants live in `linreg.cluster` (imported above): one source of
# truth, so the two cannot drift.

# Workload configuration lives in `linreg.config`; re-exported below so the
# CLI signature and the tests keep one import site.

# ─────────────────────────────────────────────────────────────────────────────
# Operation history
# ─────────────────────────────────────────────────────────────────────────────
# Records live in `linreg.records`; re-exported here for the same reason.

# ─────────────────────────────────────────────────────────────────────────────
# Shell helpers
# ─────────────────────────────────────────────────────────────────────────────

# Cluster helpers live in `linreg.cluster`; re-exported below.

# ─────────────────────────────────────────────────────────────────────────────
# Workload
# ─────────────────────────────────────────────────────────────────────────────


# Table setup, op helpers, and the worker loops live in `linreg.workload`.


# ─────────────────────────────────────────────────────────────────────────────
# WGL linearizability checker — register model
# ─────────────────────────────────────────────────────────────────────────────
# Checkers live in `linreg.checkers`; re-exported here because the matrix,
# the tests, and the CLI all reach them through this module.


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


app = typer.Typer(
    add_completion=False,
    help="Single-register linearizability checker (WGL) for pgbattery.",
)
console = Console()


@app.command()
def run(
    artifact_dir: str = typer.Option(
        "testing/artifacts/linearizability-register",
        "--artifact-dir",
        envvar="ARTIFACT_DIR",
        help="Where to write history.json and results.json.",
    ),
    seed: int = typer.Option(
        0,
        "--seed",
        help="RNG seed for worker op selection. 0 = derive from time.",
    ),
    attack: str = typer.Option(
        "kill",
        "--attack",
        help=f"One of: {', '.join(ATTACK_DISPATCH)}",
    ),
    check: str = typer.Option(
        "wgl",
        "--check",
        help="'wgl' = strict linearizability (slow, ≤cap ops/key); "
        "'weak' = no-phantom-reads (fast, any scale); "
        "'elle' = subprocess into Elle for transactional anomaly classes "
        "(requires --workload txn).",
    ),
    workload: str = typer.Option(
        "register",
        "--workload",
        help="'register' = single-op reads/writes/CAS (default); "
        "'txn' = 2-key SERIALIZABLE multi-statement transactions (for Elle).",
    ),
    workers: int = typer.Option(
        DEFAULT_NUM_WORKERS, "--workers", help="Concurrent client threads."
    ),
    keys: int = typer.Option(DEFAULT_NUM_KEYS, "--keys", help="Number of register keys."),
    duration: float = typer.Option(
        DEFAULT_WORKLOAD_DURATION_SECONDS, "--duration", help="Workload runtime (s)."
    ),
    fault_at: float = typer.Option(
        DEFAULT_KILL_LEADER_AFTER_SECONDS, "--fault-at", help="When to inject the fault (s)."
    ),
) -> None:
    """Run a concurrent register workload with leader-kill mid-flight.

    Spawns `--workers` threads issuing reads / writes / CAS across `--keys`
    keys. Kills the leader at `--fault-at`. After `--duration` total, stops
    workers, waits for cluster recovery, then checks each key's op history for
    linearizability.
    """
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)

    actual_seed = seed if seed != 0 else int(time.time())
    cfg = WorkloadConfig(workers=workers, keys=keys, duration_s=duration, fault_at=fault_at)
    # Validate workload / check combo before the run kicks off.
    valid_workloads = {"register", "txn", "list-append"}
    valid_checks = {"wgl", "weak", "elle"}
    if workload not in valid_workloads:
        console.print(f"[bold red]Unknown workload:[/] {workload}")
        raise typer.Exit(code=2)
    if check not in valid_checks:
        console.print(f"[bold red]Unknown check:[/] {check}")
        raise typer.Exit(code=2)
    if check == "elle" and workload not in {"txn", "list-append"}:
        console.print(
            "[bold red]--check elle requires --workload txn or list-append[/] "
            "(per-key register histories have no cross-key dependencies for Elle)"
        )
        raise typer.Exit(code=2)
    if workload in {"txn", "list-append"} and keys < 2:
        console.print(f"[bold red]--workload {workload} requires --keys >= 2[/]")
        raise typer.Exit(code=2)

    console.rule(f"[bold]LINEARIZABILITY (workload={workload}, check={check})")
    console.print(
        f"[dim]Seed: {actual_seed}  (replay with --seed {actual_seed})"
        f" | workers={workers} keys={keys} duration={duration}s fault_at={fault_at}s[/]"
    )

    if not wait_cluster_healthy(timeout=120):
        console.print("[bold red]FATAL:[/] cluster not healthy after 120s")
        raise typer.Exit(code=2)
    table_setup_ok = (
        setup_list_append_table(cfg.keys) if workload == "list-append" else setup_table(cfg.keys)
    )
    if not table_setup_ok:
        table_name = "linappend" if workload == "list-append" else "linreg"
        console.print(f"[bold red]FATAL:[/] could not create {table_name} table")
        raise typer.Exit(code=2)

    history = History()
    stop_event = threading.Event()
    worker_threads: list[threading.Thread] = []
    worker_fn = {
        "register": worker_loop,
        "txn": txn_worker_loop,
        "list-append": list_append_worker_loop,
    }[workload]
    for i in range(cfg.workers):
        wrng = random.Random(actual_seed + i)
        t = threading.Thread(
            target=worker_fn,
            args=(i, history, stop_event, wrng, cfg),
            name=f"linreg-w{i}",
            daemon=True,
        )
        worker_threads.append(t)

    if attack not in ATTACK_DISPATCH:
        console.print(f"[bold red]Unknown attack:[/] {attack}")
        raise typer.Exit(code=2)
    if attack in SCAFFOLD_ATTACKS:
        # Surface the precondition before the workload starts. The
        # NotImplementedError raised inside the injector thread would
        # otherwise die silently and the run would falsely report PASS.
        try:
            ATTACK_DISPATCH[attack](0.0)
        except NotImplementedError as e:
            console.print(f"[bold red]{attack} is a scaffold attack:[/]\n{e}")
            raise typer.Exit(code=2) from e
    console.print(f"[dim]Attack mode: {attack}[/]")
    # chaos_storm picks its faults, ordering, and offsets from an RNG. Without
    # the run's seed it falls back to wall-clock time, so replaying a failure
    # with the recorded seed would reproduce the workload but not the fault
    # schedule. Attacks that take no seed keep the plain (delay,) signature.
    injector_args: tuple[object, ...] = (cfg.fault_at,)
    if attack in SEEDED_ATTACKS:
        injector_args = (cfg.fault_at, CHAOS_STORM_DURATION, actual_seed)
    injector = InjectorOutcome()
    killer = threading.Thread(
        target=run_injector,
        args=(ATTACK_DISPATCH[attack], injector_args, injector),
        daemon=True,
        name="injector",
    )
    killer.start()
    for t in worker_threads:
        t.start()

    console.print(
        f"Running workload for {cfg.duration_s:.0f}s "
        f"({cfg.workers} workers, {cfg.keys} keys, "
        f"leader-kill at {cfg.fault_at:.0f}s)..."
    )
    time.sleep(cfg.duration_s)
    stop_event.set()
    for t in worker_threads:
        t.join(timeout=10)
    killer.join(timeout=10)

    start_killed_nodes()
    residue = scrub_chaos_residue()
    console.print("Waiting for cluster recovery…")
    wait_cluster_healthy(timeout=90)
    time.sleep(2)

    # ── Persist raw history first (always, even if check fails) ─────────────
    history_path = artifact_path / "history.json"
    history_path.write_text(
        json.dumps([op.to_jsonable() for op in history.ops], indent=2),
        encoding="utf-8",
    )

    # An injector that raised leaves a history with no fault in it. Checking that
    # history would find no anomaly and report PASS, so refuse a verdict instead.
    # Exit 2 (infra), not 1 (violation): the run proves nothing either way.
    if injector.error is not None:
        console.print(
            f"[bold red]FATAL:[/] the {attack} injector failed, so no fault was "
            f"injected for at least part of the run:\n  "
            f"{type(injector.error).__name__}: {injector.error}"
        )
        raise typer.Exit(code=2)
    if not injector.finished:
        console.print(
            f"[bold red]FATAL:[/] the {attack} injector did not finish within the "
            "join timeout; the fault window is unknown, so no verdict is possible."
        )
        raise typer.Exit(code=2)
    if residue:
        console.print(
            "[bold red]FATAL:[/] fault residue survived the scrub, so this run "
            "would poison the next one:\n  " + "\n  ".join(residue)
        )
        raise typer.Exit(code=2)

    any_failure = False
    unchecked_keys: list[int] = []
    results: dict[int, dict[str, object]] = {}
    elle_summary: dict[str, object] | None = None

    if check == "elle":
        # ── Elle (subprocess) ────────────────────────────────────────────────
        from elle_adapter import ElleError, run_check

        elle_model = "list-append" if workload == "list-append" else "rw-register"
        records = [r.to_jsonable() for r in history.jepsen]
        try:
            elle_result = run_check(
                records=records,
                out_dir=artifact_path,
                model=elle_model,
                timeout_s=300,
            )
        except ElleError as e:
            console.print(f"[bold red]Elle infrastructure error:[/] {e}")
            raise typer.Exit(code=2) from e

        elle_table = Table(title="Elle Anomalies", show_lines=False)
        elle_table.add_column("Anomaly", style="bold")
        elle_table.add_column("Count", justify="right")
        elle_table.add_column("Sample cycle (head)")
        seen: set[str] = set()
        for a in elle_result.anomalies:
            if a.name in seen:
                continue
            seen.add(a.name)
            count = elle_result.anomaly_summary.get(a.name, 0)
            cycle_str = ", ".join(str(c) for c in a.cycle[:5])
            if len(a.cycle) > 5:
                cycle_str += " …"
            elle_table.add_row(a.name, str(count), cycle_str)
        if not elle_result.anomalies:
            elle_table.add_row("(none)", "0", "")
        console.print()
        console.print(elle_table)
        verdict_word = (
            "PASS"
            if elle_result.valid is True
            else "FAIL"
            if elle_result.valid is False
            else "UNKNOWN"
        )
        verdict_color = (
            "green"
            if elle_result.valid is True
            else "red"
            if elle_result.valid is False
            else "yellow"
        )
        console.print(
            f"[{verdict_color}]Elle verdict: {verdict_word}[/] "
            f"(anomalies: {len(elle_result.anomalies)}, "
            f"elapsed: {elle_result.elapsed_ms:.0f} ms)"
        )

        any_failure = elle_result.valid is not True
        elle_summary = {
            "valid": elle_result.valid,
            "anomaly_classes": list(elle_result.anomaly_summary),
            "anomaly_summary": elle_result.anomaly_summary,
            "elapsed_ms": elle_result.elapsed_ms,
            "op_count": elle_result.op_count,
        }
    else:
        # ── Per-key WGL or weak check ────────────────────────────────────────
        per_key = history.per_key(cfg.keys)
        checker: Callable[[list[Op]], tuple[bool | None, str]] = (
            _is_weakly_consistent if check == "weak" else _is_linearizable
        )
        for key, ops in per_key.items():
            ok, reason = checker(ops)
            results[key] = {
                "key": key,
                "op_count": len(ops),
                "linearizable": ok,
                "reason": reason,
            }
            if ok is None:
                unchecked_keys.append(key)
            elif not ok:
                any_failure = True

        result_table = Table(title="Linearizability Results", show_lines=False)
        result_table.add_column("Key", style="bold", justify="right")
        result_table.add_column("Ops")
        result_table.add_column("Linearizable")
        result_table.add_column("Reason")
        for key, info in results.items():
            if info["linearizable"] is None:
                verdict = "[yellow]UNCHECKED[/]"
            elif info["linearizable"]:
                verdict = "[green]PASS[/]"
            else:
                verdict = "[red]FAIL[/]"
            result_table.add_row(str(key), str(info["op_count"]), verdict, str(info["reason"]))
        console.print()
        console.print(result_table)

    # A key nobody could check is reported as its own verdict, never folded
    # into PASS: exit 1 is "we found a violation", exit 2 is "we did not look".
    if any_failure:
        verdict_word, exit_code = "FAIL", 1
    elif unchecked_keys:
        verdict_word, exit_code = "INCONCLUSIVE", 2
    else:
        verdict_word, exit_code = "PASS", 0

    # ── Persist top-level results.json ──────────────────────────────────────
    results_path = artifact_path / "results.json"
    results_path.write_text(
        json.dumps(
            {
                "seed": actual_seed,
                "workers": cfg.workers,
                "keys": cfg.keys,
                "duration_s": cfg.duration_s,
                "workload": workload,
                "check": check,
                "attack": attack,
                "verdict": verdict_word,
                "unchecked_keys": unchecked_keys,
                "per_key": list(results.values()),
                "elle": elle_summary,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    console.print(f"History → {history_path}")
    console.print(f"Results → {results_path}")

    if unchecked_keys and not any_failure:
        console.print(
            f"[yellow]INCONCLUSIVE[/]: keys {unchecked_keys} exceeded the WGL state "
            "budget and were never checked. This is not a pass."
        )
    raise typer.Exit(code=exit_code)


if __name__ == "__main__":
    app()
