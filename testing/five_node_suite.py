#!/usr/bin/env -S uv run --project testing python
"""5-node topology correctness suite — SKELETON.

╔══════════════════════════════════════════════════════════════════════════╗
║ STATUS: SKELETON — NOT YET IMPLEMENTED                                   ║
║ Owner: TBD                                                               ║
║ Tracking: BI1 in the audit-4 backlog                                     ║
╚══════════════════════════════════════════════════════════════════════════╝

═══════════════════════════════════════════════════════════════════════════
WHY THIS SUITE EXISTS
═══════════════════════════════════════════════════════════════════════════

Every other test in this project — `ci_runner.py`, `correctness_lite.py`,
`overnight_test.py`, `linearizability_register.py`, the GitHub workflows —
runs against the docker-compose 3-node cluster. Three nodes is the minimum
for Raft (quorum = 2/3) and exercises the *most common* operator
deployment, but it hides entire classes of bugs that only surface with
larger or asymmetric memberships:

  - **Quorum arithmetic at N ≥ 5.** A 5-node cluster has quorum = 3, so it
    survives 2 simultaneous failures. The code paths that subtract a node
    from the voter set (`/remove/{id}`, `change_membership`) and the LSN-
    safety gate (`is_lsn_acceptable_for_election`) are exercised
    differently when there are *spare* voters. Bugs that round to the
    wrong half (e.g. `>= N/2` vs `> N/2`) ship today undetected.

  - **Witness / non-voting learner promotion.** The witness-topology case
    in `ci_matrix.yaml` covers the 2+1 happy path on 3 nodes. It does NOT
    cover witness promotion races at 4+1 or 6+1.

  - **Cascading replication.** A leader → 2 sync + 2 async replicas
    surfaces sync-name-mask bugs in the replication manager that 3-node
    `synchronous_standby_names = 'ANY 1 (…)'` cannot.

  - **Membership convergence under partition.** In a 5-node cluster with
    a 3/2 split, the minority must NEVER elect; in a 4/1 split the
    minority is even smaller. Joint-consensus transitions under any of
    these splits are unchecked today.

═══════════════════════════════════════════════════════════════════════════
SCOPE (initial)
═══════════════════════════════════════════════════════════════════════════

Phase 1 — bootstrap and quorum sanity
    [ ] Start a 5-node cluster via a parallel docker-compose file (no port
        collisions with the existing 3-node compose); confirm a single
        leader within 30 s.
    [ ] Kill 2 voters; confirm the cluster remains writable (quorum = 3).
    [ ] Kill 3 voters; confirm the cluster goes read-only (no quorum) and
        recovers on restart.
    [ ] Run `correctness_lite.py` against the 5-node cluster end-to-end.

Phase 2 — membership chaos
    [ ] Add a 6th node as learner, promote to voter; verify quorum is now
        4 (majority of 6 = 4, openraft convention) and the cluster
        survives 2 failures but not 3.
    [ ] Remove 2 voters back-to-back; confirm guard refuses the 2nd if
        the resulting voter set would be < 3.
    [ ] Run `add → kill candidate → promote during partition` race
        scenarios to surface joint-consensus regressions.

Phase 3 — replication topology
    [ ] Configure 2 sync + 2 async replicas (k=2 in
        `synchronous_standby_names = 'ANY 2 (…)'`); verify acked-write
        durability under leader + 1 sync replica failure.
    [ ] Kill 1 sync replica during write load; verify the second sync
        replica is promoted into the sync set and writes do not stall.

Phase 4 — Jepsen-grade chaos
    [ ] Wire `linearizability_register.py` against 5 nodes; verify the
        WGL register check still passes under 5-node failover.
    [ ] Compose with `fault_primitives.py` once it lands (BI2).

═══════════════════════════════════════════════════════════════════════════
IMPLEMENTATION NOTES
═══════════════════════════════════════════════════════════════════════════

  - Use a separate compose file (e.g. `docker-compose.5node.yml`) with
    ports offset by +10 to avoid collisions during local dev. Mark the
    5-node compose as `profiles: [five-node]` so `docker compose up` on
    the default file isn't slowed by 5 extra containers.

  - Bootstrap order is identical to 3-node: node1 bootstraps with
    `--bootstrap`, the rest join via `POST /api/v1/cluster/join` against
    node1's mgmt API. Each subsequent join must wait for the prior to
    reach voter status before promoting — openraft 0.9 does not allow
    multi-step joint consensus in parallel.

  - Quorum arithmetic in openraft 0.9: `majority = floor(N/2) + 1`. So
    N=3 ⇒ quorum 2; N=5 ⇒ quorum 3; N=6 ⇒ quorum 4. The remove-node
    guard in `src/observability/management_api/cluster.rs` already
    refuses to drop below 2 — extend it (and document in the test) for
    the N=6 case.

  - The CI matrix entry should NOT live in `ha-controlplane-pr` (too
    slow for a per-PR gate). Add to `ha-controlplane-nightly` or to a
    new dedicated `five-node-suite` workflow.

═══════════════════════════════════════════════════════════════════════════
TODO — file is a STUB. Adding the real implementation is BI1.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import typer
from rich.console import Console

app = typer.Typer(
    add_completion=False,
    help="5-node topology correctness suite (SKELETON — not yet implemented).",
)
console = Console()


@app.command()
def run() -> None:
    """Run the 5-node suite. STUB — currently exits 0 with a TODO banner."""
    console.print("[bold yellow]five_node_suite.py is a SKELETON[/]")
    console.print(
        "Tracking ticket: BI1. See the module docstring for scope, phases, "
        "and implementation notes."
    )
    raise typer.Exit(code=0)


if __name__ == "__main__":
    app()
