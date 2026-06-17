# TLA+ Specifications

Formal models of pgbattery's core safety properties, **machine-checked** with TLC.

> **Validation.** `make check` (below) runs all four specs through TLC and fails
> on any violation. The model checker (`tla2tools.jar`) is pinned by version +
> SHA-256 in the `Makefile`, so every run — local or CI — uses an identical,
> verified binary. CI runs the same target: `.github/workflows/tla.yml`.

## Run

```bash
cd tla/
make check                      # download (pinned) + check ALL specs
make check-lease_fencing        # one spec, full TLC output
make tools                      # just fetch + verify the jar
make clean                      # remove downloaded jar + TLC state
```

Requires a JDK ≥ 11. The `Makefile` auto-detects a working `java`, else Homebrew
`openjdk@21`. On macOS install it with **`brew install openjdk@21`** (a formula —
no `sudo`; the `--cask`/Toolbox needs root and isn't required).

## What each spec checks

| Spec                        | Verified property                                                          |
| --------------------------- | -------------------------------------------------------------------------- |
| `lease_fencing.tla`         | At most one node holds **write authority** across leadership transfer      |
| `raft_lsn.tla`              | Election safety + the LSN gate never deadlocks elections                   |
| `commit_probing.tla`        | In-doubt COMMIT probing is correct; acknowledged commits survive failover  |
| `timeline_verification.tla` | PostgreSQL timelines stay bounded and never decrease across promotions     |

## Properties (checked by each `.cfg`)

**`lease_fencing.tla`** — passes (14,974 distinct states)

- `AtMostOneWriteAuthority` — at most one node has a valid lease **and** writable
  PG at any instant. **Non-vacuous**: leadership transfers in the model, so a
  deposed and a new leader coexist; only the promotion hold-down plus the
  quorum-ack-anchored lease keep their write windows apart. Removing
  `ASSUME HoldDown >= LeaseDuration` and setting `HoldDown = 0` makes TLC produce
  the two-writer (split-brain) counterexample — confirming the invariant has teeth.
- `SelfFenceOnQuorumLoss` — a leader that stops getting acks loses write authority
  within `QuorumTimeout` of its last ack.
- Time is modeled **relatively** (bounded countdowns advanced by one global tick),
  not as an absolute clock, so the state space stays small and finite.
- Scope: the hold-down is modeled at the election instant; the implementation
  anchors at the new leader's local leader-loss observation, which coincides only
  for fast failover. The partial-partition case and its sync-replication backstop
  are documented in the spec header.

**`raft_lsn.tla`** — passes (33,957 distinct states)

- `ElectionSafety` (≤1 leader), `NoLSNDeadlock` / `SomeNodeCanWin` (the LSN gate
  never wedges elections), `TypeOK`.
- **TLC actively DISPROVES** `LeaderHasAcceptableLSN` and `LeaderLSNNotBelowVoters`
  (defined in the spec, deliberately not in the cfg): a candidate self-votes past
  the gate — the self-vote is not LSN-checked — and reaches quorum via an
  under-informed voter, so a node can lead while behind on LSN. The LSN gate is
  **advisory**; Raft log-matching (abstracted here) is the real safety net, exactly
  as the implementation comments state.

**`commit_probing.tla`** — passes (47,932 distinct states)

- `ProbeCommittedImpliesVisible` / `ProbeAbortedImpliesNotWritten` — no false commit
  or false abort.
- `AckedSuccessIsDurable` — any success the client saw (normal or synthetic) is
  still visible on the current leader after failover (RPO=0).

**`timeline_verification.tla`** — passes (3,208 distinct states)

- `TypeOK` + `TimelineMonotonic` — timelines stay bounded and a promotion only ever
  advances a node's timeline.
- Does **not** claim "no two primaries share a timeline" — false in the partition
  model. Single-primary safety lives in `lease_fencing.tla` (Raft); the `pg_rewind`
  data-loss gate is covered by Rust unit tests for `rewind_divergence_decision`.

## Code mapping

Each spec's header maps its TLA+ variables and actions to the Rust that implements
them, by file and function (not line number, which rots). The supervisor lives in
the `crates/pgbattery-supervisor/` workspace crate
(`crates/pgbattery-supervisor/src/process.rs`), not `src/supervisor/` (a re-export shim).
