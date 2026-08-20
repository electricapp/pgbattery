# TLA+ Specifications

Formal models of pgbattery's core safety properties, **machine-checked** with TLC.

> **Validation.** `make check` (below) runs all four specs through TLC and fails
> on any violation, then runs the counterexample models and fails if any of them
> _passes_. The model checker (`tla2tools.jar`) is pinned by version + SHA-256 in
> the `Makefile`, so every run — local or CI — uses an identical, verified
> binary. CI runs the same target: `.github/workflows/tla.yml`.

## Run

```bash
cd tla/
make check                      # download (pinned) + check ALL specs + counterexamples
make check-large                # the nightly models (more nodes, wider bounds)
make check-counterexamples      # the models that must fail, and how
make check-lease_fencing        # one spec, full TLC output
make tools                      # just fetch + verify the jar
make clean                      # remove downloaded jar + TLC state
```

Requires a JDK ≥ 11. The `Makefile` auto-detects a working `java`, else Homebrew
`openjdk@21`. On macOS install it with **`brew install openjdk@21`** (a formula —
no `sudo`; the `--cask`/Toolbox needs root and isn't required). `check-large`
also needs `timeout(1)` — macOS ships none, so `brew install coreutils`.

Each spec has two models. `<spec>.cfg` is what every PR runs: the smallest model
that can exhibit the property at all. `<spec>.large.cfg` is the nightly one, and
its header says which axis it widens, why that axis rather than another, and the
measurement behind the choice. Node count is not always the right axis —
`raft_lsn` carries a term and an LSN per node and becomes intractable at five,
where a wider `MaxLSN` buys more. `check-large` fails rather than skips when a
spec has no large config.

A third kind of model is a **counterexample**: `<spec>.inv-<name>.cfg` disables
one mechanism the safety argument rests on and names, in an `EXPECT-VIOLATION:`
header, the invariant TLC then has to report. A run that succeeds is the failure,
and so is one that fails for a different reason — a parse error or a violated
`ASSUME` is not a counterexample. This is the same rule the Python harnesses live
under: an assertion nobody has watched fail is an assertion whose passing means
nothing. `lease_fencing` has one; the other three do not yet (H-47 in
`HARDENING.md`), and `make check-counterexamples` says so by only checking what
exists rather than claiming per-spec coverage.

## What each spec checks

| Spec                        | Verified property                                                         |
| --------------------------- | ------------------------------------------------------------------------- |
| `lease_fencing.tla`         | At most one node holds **write authority** across leadership transfer     |
| `raft_lsn.tla`              | Election safety + the LSN gate never deadlocks elections                  |
| `commit_probing.tla`        | In-doubt COMMIT probing is correct; acknowledged commits survive failover |
| `timeline_verification.tla` | PostgreSQL timelines stay bounded and never decrease across promotions    |

## Properties (checked by each `.cfg`)

**`lease_fencing.tla`** — passes (492,689 distinct states)

- `AtMostOneWriteAuthority` — at most one node has a valid lease **and** writable
  PG at any instant. **Non-vacuous, and not on the honour system**: leadership
  transfers in the model, so a deposed and a new leader coexist, and
  `lease_fencing.inv-anchor-not-restamped.cfg` is a checked config in which TLC
  produces the two-writer counterexample.
- `SelfFenceOnQuorumLoss` — a leader that stops getting acks loses write authority
  within `QuorumTimeout` of its last ack. This is the bound the hold-down is sized
  against, and the one that actually ends a deposed leader's authority, since
  `QuorumTimeout < LeaseDuration`.
- `OneLeaderPerTerm` — Raft's election safety, here **derived** rather than
  assumed: elections and acks both name the majority that took part, and voters
  grant only strictly higher terms.
- Quorums are node sets, not an ack counter, which is what makes the RW-1 shape —
  a deposed leader holding a quorum that excludes the winner — a reachable state,
  and what makes quorum intersection a consequence instead of a premise.
- Time is modeled **relatively** (bounded countdowns advanced by one global tick),
  not as an absolute clock, so the state space stays small and finite.
- Scope: the Raft log is abstracted away (that is `raft_lsn.tla`), and message
  loss appears only as a candidacy that did not win in time. `VoteDelay` is the
  network bound the argument cannot escape — a vote slower than
  `LeaseDuration - QuorumTimeout` breaks the inequality, and no lease scheme
  bounds that without assuming it.

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
