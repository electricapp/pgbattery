#!/usr/bin/env -S uv run --project testing python
"""5-node topology correctness suite.

Every other test in this project runs against the 3-node compose cluster.
Three nodes is the minimum for Raft and the most common deployment, but it
cannot distinguish `>= N/2` from `> N/2`: at N=3 both give quorum 2. At N=5
they give 3 and 4, and only one of those survives two failures. That is the
arithmetic this suite exists to pin down.

Phase 1 — bootstrap, surviving two failures, losing quorum at three, and
recovering with acked writes intact.

Phase 2 — partition shapes a 3-node cluster cannot express: a 3/2 split with
the leader stranded on the minority side, a leader left with exactly one
follower (seeing *a* peer is not seeing a quorum), and a three-way split where
no group reaches 3 and nothing anywhere may write.

Membership chaos, 2-sync/2-async replication, and Elle at five nodes are scoped
in HARDENING.md and are not here yet; the runner says so rather than implying
coverage.

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

REFENCE_CONVERGE_TIMEOUT_S: Final[float] = 240.0
"""Budget for shapes that strand nodes without quorum.

Fencing escalates to process exit when a node cannot hold its lease, and
`restart: unless-stopped` then brings the container back — so convergence here
includes a full pgbattery restart and rejoin on the stranded side, not just an
election. Measured: the minority containers came back at 13 s and 28 s uptime
after a partition that had run well under a minute, and the majority's new
primary was not writable until after that churn settled.

Separate constant rather than raising `CONVERGE_TIMEOUT_S` everywhere: the
phases that do not strand anyone should stay fast, and a shape that suddenly
needs this long is worth noticing."""

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


def await_all_healthy(timeout_s: float = REFENCE_CONVERGE_TIMEOUT_S) -> int:
    """Wait until all five nodes answer *and* are voters, then return the leader.

    `await_leader` is not enough as a precondition for the partition shapes.
    A node stranded by the previous shape self-fences to process exit and comes
    back via `restart: unless-stopped`, and while it is restarting the other
    four can still agree on a leader — so `await_leader` returns happily with a
    node still down. The next shape then puts that node on the side it is
    counting, and "3 of 5" is really 2, which cannot elect.

    This is the same mistake as the learner race, in a different costume:
    asserting on a topology without first establishing it.
    """
    deadline = time.time() + timeout_s
    last = ""
    while time.time() < deadline:
        views = leader_views()
        roles = members()
        healthy = sorted(views)
        all_voters = sorted(n for n, r in roles.items() if r == "voter")
        if len(healthy) == len(NODES) and len(all_voters) == len(NODES):
            leader = quorum_leader()
            if leader is not None:
                return leader
        last = f"responding={healthy} voters={all_voters}"
        time.sleep(3)
    raise SuiteError(
        f"cluster did not return to all-{len(NODES)}-healthy within {timeout_s:.0f}s ({last}); "
        "a partition shape run now would be counting a node that is still down"
    )


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


NODE_IP: Final[dict[int, str]] = {n: f"172.29.0.{10 + n}" for n in NODES}


def _iptables(node: int, peer: int, *, insert: bool) -> None:
    """Add or remove a whole-peer DROP on `node` for traffic from `peer`."""
    action = "-A" if insert else "-D"
    tail = "" if insert else " 2>/dev/null; true"
    rc, _, err = run(
        f"docker compose exec -T --user root node{node} sh -c "
        f'"iptables {action} INPUT -s {NODE_IP[peer]} -j DROP{tail}"',
        timeout=30,
    )
    if insert and rc != 0:
        raise SuiteError(f"could not partition node{node} from node{peer}: {err}")


def partition_groups(groups: list[list[int]], *, heal: bool = False) -> None:
    """Sever every cross-group link, leaving intra-group links up.

    Whole-peer rather than port-granular: these shapes are about who can reach
    whom at all, so a channel-level cut would leave consensus flowing through
    the very links the shape is meant to remove.

    Installed on both endpoints of each pair. A one-sided DROP still lets the
    other direction through, and Raft only needs one direction to keep a
    follower believing in a leader it cannot answer.
    """
    for i, group in enumerate(groups):
        for other in groups[i + 1 :]:
            for a in group:
                for b in other:
                    _iptables(a, b, insert=not heal)
                    _iptables(b, a, insert=not heal)


def any_writable() -> list[int]:
    """Which running nodes accept a real write, asked of PostgreSQL directly."""
    return [n for n in NODES if _running(n) and writable(n)]


def await_no_writer(timeout_s: float) -> None:
    """Wait until no node accepts writes, or fail naming the ones that do."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if not any_writable():
            return
        time.sleep(3)
    raise SuiteError(f"nodes {any_writable()} still accept writes without a quorum")


def _running(node: int) -> bool:
    rc, out, _ = run(f"docker compose ps -q node{node}", timeout=30)
    if rc != 0 or not out.strip():
        return False
    cid = out.strip().split("\n")[-1]
    rc, state, _ = run(f'docker inspect -f "{{{{.State.Status}}}}" {cid}', timeout=30)
    return rc == 0 and state.strip() == "running"


def phase_majority_isolation(leader: int) -> None:
    """Split 3/2 with the leader on the minority side.

    The majority must elect and accept writes; the minority must not, however
    confident its members are. Asked of PostgreSQL on each side rather than of
    the control plane, so a node that merely *believes* it leads does not count.
    """
    console.print("[bold]Phase 2a[/] 3/2 split, leader on the minority side")
    minority = [leader, next(n for n in NODES if n != leader)]
    majority = [n for n in NODES if n not in minority]
    partition_groups([minority, majority])
    console.print(f"  minority={minority} majority={majority}")
    try:
        deadline = time.time() + REFENCE_CONVERGE_TIMEOUT_S
        while time.time() < deadline:
            accepting = [n for n in majority if writable(n)]
            if len(accepting) == 1:
                break
            time.sleep(3)
        else:
            raise SuiteError(
                f"majority {majority} produced no single writer within {CONVERGE_TIMEOUT_S:.0f}s"
            )
        stuck = [n for n in minority if writable(n)]
        if stuck:
            raise SuiteError(f"minority {stuck} accepted writes — L1 violated")
        console.print(f"  majority writer = node{accepting[0]}, minority silent")
    finally:
        partition_groups([minority, majority], heal=True)


def phase_three_way_split() -> None:
    """No group holds 3 of 5, so nothing anywhere may accept a write."""
    console.print("[bold]Phase 2b[/] three-way split — nobody may write")
    groups = [[1, 2], [3, 4], [5]]
    partition_groups(groups)
    console.print(f"  groups={groups}, largest={max(len(g) for g in groups)} of {QUORUM} needed")
    try:
        await_no_writer(REFENCE_CONVERGE_TIMEOUT_S)
        console.print("  no node accepts writes")
    finally:
        partition_groups(groups, heal=True)


def phase_follower_bridge(leader: int) -> None:
    """The leader keeps exactly one follower and must still step down.

    Seeing *a* follower is not seeing a quorum, and this is the shape where
    that distinction is easy to get wrong: the leader has a live peer, an open
    replication stream, and no obvious signal that anything is missing. Only
    the quorum count says otherwise.
    """
    console.print("[bold]Phase 2c[/] leader keeps one follower — must still step down")
    companion = next(n for n in NODES if n != leader)
    isolated = [leader, companion]
    rest = [n for n in NODES if n not in isolated]
    partition_groups([isolated, rest])
    console.print(f"  leader node{leader} sees only node{companion}; rest={rest}")
    try:
        deadline = time.time() + REFENCE_CONVERGE_TIMEOUT_S
        while time.time() < deadline:
            if not writable(leader):
                break
            time.sleep(3)
        else:
            raise SuiteError(
                f"node{leader} kept accepting writes while seeing only node{companion} "
                f"({len(isolated)} of {QUORUM} needed)"
            )
        console.print(f"  node{leader} stepped down despite a live follower")

        # Poll rather than sample once: the majority has to elect *and* clear
        # the promotion hold-down, which is a full lease duration, so a single
        # check right after the old leader steps down is always too early.
        accepting: list[int] = []
        deadline = time.time() + REFENCE_CONVERGE_TIMEOUT_S
        while time.time() < deadline:
            accepting = [n for n in rest if writable(n)]
            if len(accepting) == 1:
                break
            time.sleep(3)
        if len(accepting) != 1:
            raise SuiteError(f"expected exactly one writer among {rest}, got {accepting}")
        console.print(f"  majority side elected node{accepting[0]}")
    finally:
        partition_groups([isolated, rest], heal=True)


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

        # Phase 2 needs a healthy cluster, so it runs after recovery.
        leader = await_all_healthy()
        phase_majority_isolation(leader)
        results.append(("3/2 split: only the majority writes", "PASS"))

        leader = await_all_healthy()
        phase_follower_bridge(leader)
        results.append(("leader with one follower steps down", "PASS"))

        await_all_healthy()
        phase_three_way_split()
        results.append(("three-way split: nobody writes", "PASS"))
    except SuiteError as exc:
        console.print(f"[red]FAIL[/] {exc}")
        results.append(("suite", f"FAIL: {exc}"))
        _report(results)
        raise typer.Exit(code=1) from exc

    _report(results)
    console.print(
        "[yellow]Not implemented: membership chaos, 2-sync/2-async replication, "
        "Elle at five nodes.[/] See HARDENING.md."
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
