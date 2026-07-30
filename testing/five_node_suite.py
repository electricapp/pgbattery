#!/usr/bin/env -S uv run --project testing python
"""5-node topology correctness suite.

Every other test in this project runs against the 3-node compose cluster.
Three nodes is the minimum for Raft and the most common deployment, but it
cannot distinguish `>= N/2` from `> N/2`: at N=3 both give quorum 2. At N=5
they give 3 and 4, and only one of those survives two failures. That is the
arithmetic this suite exists to pin down.

Phase 1 is implemented — bootstrap, the two-failure survival case, and the
three-failure quorum-loss case, with a W1 durability oracle across all of it.
Later phases (membership chaos, 2-sync/2-async replication, Elle at five nodes)
are scoped in HARDENING.md and are not here yet; the runner says so and exits
non-zero rather than reporting a pass it did not earn.

Topology lives in `docker-compose.5node.yml`: its own project name, subnet, and
host ports, so it coexists with the 3-node cluster and neither one's fault
injection can reach the other. Quorum is 3.

Run with:
    ./testing/five_node_suite.py run
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from typing import Final

import typer
from rich.console import Console
from rich.table import Table

COMPOSE_FILE: Final[str] = "docker-compose.5node.yml"
PROJECT: Final[str] = "pgbattery5"
NODES: Final[tuple[int, ...]] = (1, 2, 3, 4, 5)
QUORUM: Final[int] = 3
"""`floor(5/2) + 1`. Stated rather than computed so a wrong constant is a
visible edit rather than a silently-agreeing derivation."""

MGMT_PORT: Final[dict[int, int]] = {n: 9180 + n for n in NODES}
GATEWAY_PORT: Final[dict[int, int]] = {n: 5441 + n for n in NODES}

BOOTSTRAP_TIMEOUT_S: Final[float] = 180.0
CONVERGE_TIMEOUT_S: Final[float] = 90.0

console = Console()
app = typer.Typer(add_completion=False, help="5-node topology correctness suite.")


class SuiteError(RuntimeError):
    """A precondition failed, or an assertion the suite exists to make."""


def compose_env() -> dict[str, str]:
    env = dict(os.environ)
    env["COMPOSE_FILE"] = COMPOSE_FILE
    env["COMPOSE_PROJECT_NAME"] = PROJECT
    return env


def run(cmd: str, timeout: float = 120.0) -> tuple[int, str, str]:
    try:
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout, env=compose_env()
        )
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    return r.returncode, r.stdout, r.stderr


def psql(node: int, sql: str, timeout: float = 20.0) -> tuple[int, str]:
    """Run `sql` against `node`'s internal PostgreSQL, bypassing the gateway.

    Direct rather than through the gateway because these cases ask which
    *specific* node accepts a write. A gateway would route the question away
    from the node under test.
    """
    rc, out, err = run(
        f"docker compose exec -T node{node} psql -h 127.0.0.1 -p 5434 -U postgres "
        f'-v ON_ERROR_STOP=1 -tAc "{sql}"',
        timeout=timeout,
    )
    return rc, (out or err).strip()


def leader_views() -> dict[int, int | None]:
    """Each reachable node's answer to "who leads", by node id.

    Same discipline as `ci_runner._quorum_leader`: one node's answer is a
    belief, not a fact, and an isolated ex-leader keeps naming itself.
    """
    views: dict[int, int | None] = {}
    for n in NODES:
        rc, out, _ = run(
            f"curl -sf --max-time 3 http://localhost:{MGMT_PORT[n]}/api/v1/cluster/leader",
            timeout=6,
        )
        if rc != 0:
            continue
        try:
            views[n] = json.loads(out).get("leader_id")
        except (json.JSONDecodeError, AttributeError):
            continue
    return views


def quorum_leader() -> int | None:
    """The leader a strict majority of the *configured* five agree on."""
    tally: dict[int, int] = {}
    for seen in leader_views().values():
        if seen is not None:
            tally[seen] = tally.get(seen, 0) + 1
    for leader, count in tally.items():
        if count >= QUORUM:
            return leader
    return None


def await_leader(timeout_s: float) -> int:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        leader = quorum_leader()
        if leader is not None:
            return leader
        time.sleep(2)
    raise SuiteError(
        f"no leader agreed by {QUORUM} of {len(NODES)} nodes within {timeout_s:.0f}s; "
        f"views={leader_views()}"
    )


def members() -> dict[int, str]:
    """`node_id -> role` from the leader's view of Raft membership."""
    for n in NODES:
        rc, out, _ = run(
            f"curl -sf --max-time 3 http://localhost:{MGMT_PORT[n]}/api/v1/cluster/members",
            timeout=6,
        )
        if rc != 0:
            continue
        try:
            payload = json.loads(out)
        except json.JSONDecodeError:
            continue
        return {int(m["node_id"]): str(m["role"]) for m in payload.get("members", [])}
    return {}


def ensure_all_voters(timeout_s: float = 240.0) -> None:
    """Promote every learner to voter, one at a time, and verify it took.

    Compose starts all five joins at once, and openraft 0.9 will not run
    multi-step joint consensus in parallel: the losers of that race stay
    learners. A learner is not a voter, so killing one costs no quorum — which
    silently turns "kill 2 of 5" into "kill 2 bystanders" and makes every
    quorum assertion below meaningless while still passing.

    That is exactly how the first run of this suite reported a bogus L1
    violation: the voter set was {1,4,5}, so two dead learners plus one dead
    voter still left 2 of 3 voters and the leader legitimately held quorum.
    """
    token = os.environ.get("PGBATTERY_MANAGEMENT_API_TOKEN", "")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        roles = members()
        if not roles:
            time.sleep(3)
            continue
        learners = sorted(n for n, role in roles.items() if role != "voter")
        if not learners and len(roles) == len(NODES):
            console.print(f"  voter set = {sorted(roles)} ({len(roles)} of {len(NODES)})")
            return
        target = learners[0]
        run(
            f"curl -sf -X POST --max-time 10 "
            f'-H "x-pgbattery-token: {token}" '
            f"http://localhost:{MGMT_PORT[1]}/api/v1/cluster/promote/{target}",
            timeout=15,
        )
        # One at a time: issuing the next promotion before this one commits is
        # the same parallel-joint-consensus mistake that created the learners.
        time.sleep(4)
    raise SuiteError(
        f"not all {len(NODES)} nodes reached voter status within {timeout_s:.0f}s; "
        f"roles={members()}. Quorum arithmetic below would be meaningless."
    )


def voters() -> list[int]:
    return sorted(n for n, role in members().items() if role == "voter")


def writable(node: int) -> bool:
    """Whether `node` accepts a real write. The question W1 and L1 both ask."""
    rc, _ = psql(node, "INSERT INTO five_node_w1(note) VALUES ('probe')")
    return rc == 0


def kill(node: int) -> None:
    rc, _, err = run(f"docker compose kill node{node}")
    if rc != 0:
        raise SuiteError(f"could not kill node{node}: {err}")


def start(node: int) -> None:
    rc, _, err = run(f"docker compose start node{node}", timeout=180)
    if rc != 0:
        raise SuiteError(f"could not start node{node}: {err}")


def phase_bootstrap() -> int:
    console.print("[bold]Phase 1a[/] bootstrap and quorum sanity")
    rc, _, err = run("docker compose up -d", timeout=900)
    if rc != 0:
        raise SuiteError(f"compose up failed: {err}")
    leader = await_leader(BOOTSTRAP_TIMEOUT_S)
    console.print(f"  leader = node{leader}, agreed by >= {QUORUM} of {len(NODES)}")
    ensure_all_voters()

    rc, out = psql(
        leader,
        "CREATE TABLE IF NOT EXISTS five_node_w1(id bigserial primary key, note text)",
    )
    if rc != 0:
        raise SuiteError(f"could not create the W1 table on the leader: {out}")
    return leader


def phase_survives_two_failures(leader: int) -> int:
    """Quorum is 3 of 5, so two dead voters still leave a writable cluster.

    This is the case a 3-node cluster cannot express at all: there, two
    failures *is* quorum loss.
    """
    console.print("[bold]Phase 1b[/] two simultaneous failures — must stay writable")
    # Voters only. Killing a learner costs no quorum, so a learner victim
    # would make this case pass without testing anything.
    victims = [n for n in voters() if n != leader][:2]
    if len(victims) != 2:
        raise SuiteError(f"need 2 non-leader voters to kill, have {victims}")
    for n in victims:
        kill(n)
    console.print(f"  killed node{victims[0]}, node{victims[1]}")

    leader_after = await_leader(CONVERGE_TIMEOUT_S)
    if not writable(leader_after):
        raise SuiteError(
            f"node{leader_after} leads a 3-of-5 quorum but refuses writes; "
            "two failures must not cost availability at N=5"
        )
    console.print(f"  node{leader_after} still accepting writes")

    survivors = [n for n in NODES if n not in victims]
    accepting = [n for n in survivors if writable(n)]
    if accepting != [leader_after]:
        raise SuiteError(f"L1: expected only node{leader_after} writable, got {accepting}")
    console.print(f"  L1 holds: exactly one writable node ({accepting})")
    return leader_after


def phase_loses_quorum_at_three(leader: int) -> None:
    """A third failure drops the cluster to 2 of 5 — below quorum.

    The assertion is that it goes read-only rather than electing on a
    minority. `writable` asks PostgreSQL, not the control plane, so a node
    that merely *believes* it leads does not satisfy it.
    """
    console.print("[bold]Phase 1c[/] third failure — must lose quorum")
    alive_voters = [n for n in voters() if _running(n)]
    victim = next((n for n in alive_voters if n != leader), None)
    if victim is None:
        raise SuiteError(f"no live non-leader voter to kill; alive={alive_voters}")
    kill(victim)
    console.print(f"  killed node{victim}; 2 of 5 remain")

    deadline = time.time() + CONVERGE_TIMEOUT_S
    while time.time() < deadline:
        remaining = [n for n in NODES if _running(n)]
        if not any(writable(n) for n in remaining):
            console.print("  no node accepts writes — quorum correctly lost")
            return
        time.sleep(3)
    still = [n for n in NODES if _running(n) and writable(n)]
    raise SuiteError(
        f"nodes {still} still accept writes with only 2 of 5 alive; "
        "a minority must never be writable"
    )


def phase_recovers(expected_rows: int) -> None:
    console.print("[bold]Phase 1d[/] restart — must recover with data intact")
    for n in NODES:
        if not _running(n):
            start(n)
    leader = await_leader(BOOTSTRAP_TIMEOUT_S)
    rc, out = psql(leader, "SELECT count(*) FROM five_node_w1")
    if rc != 0:
        raise SuiteError(f"could not count rows after recovery: {out}")
    if int(out) < expected_rows:
        raise SuiteError(
            f"W1: {out} rows after recovery, expected at least {expected_rows} — "
            "an acknowledged write was lost"
        )
    console.print(f"  leader = node{leader}, {out} rows survived (>= {expected_rows} acked)")


def _running(node: int) -> bool:
    rc, out, _ = run(f"docker compose ps -q node{node}", timeout=30)
    if rc != 0 or not out.strip():
        return False
    cid = out.strip().split("\n")[-1]
    rc, state, _ = run(f'docker inspect -f "{{{{.State.Status}}}}" {cid}', timeout=30)
    return rc == 0 and state.strip() == "running"


@app.command()
def run_suite() -> None:
    """Run Phase 1. Exits non-zero on any failed assertion."""
    console.rule("[bold]5-NODE SUITE — Phase 1")
    results: list[tuple[str, str]] = []
    try:
        leader = phase_bootstrap()
        acked = 0
        if writable(leader):
            acked += 1
        results.append(("bootstrap + single leader", "PASS"))

        leader = phase_survives_two_failures(leader)
        acked += 1  # the write `phase_survives_two_failures` made
        results.append(("survives 2 simultaneous failures", "PASS"))

        phase_loses_quorum_at_three(leader)
        results.append(("loses quorum at 3 failures", "PASS"))

        phase_recovers(acked)
        results.append(("recovers with acked writes intact", "PASS"))
    except SuiteError as exc:
        console.print(f"[red]FAIL[/] {exc}")
        results.append(("suite", f"FAIL: {exc}"))
        _report(results)
        raise typer.Exit(code=1) from exc

    _report(results)
    console.print(
        "[yellow]Phases 2-4 (membership chaos, 2-sync/2-async, Elle at five nodes) "
        "are not implemented.[/] See HARDENING.md."
    )


def _report(results: list[tuple[str, str]]) -> None:
    table = Table(title="5-Node Suite — Phase 1")
    table.add_column("Case", style="bold")
    table.add_column("Result")
    for name, verdict in results:
        table.add_row(name, verdict)
    console.print(table)


@app.command()
def down() -> None:
    """Tear the 5-node cluster down, volumes included."""
    rc, _, err = run("docker compose down -v", timeout=300)
    if rc != 0:
        console.print(f"[red]teardown failed[/] {err.strip()}")
        raise typer.Exit(code=1)
    console.print("5-node cluster removed")


if __name__ == "__main__":
    app()
