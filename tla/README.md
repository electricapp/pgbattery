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

`<spec>.alt-<name>.cfg` is a third kind, and the distinction from `large` is the
point: it is the same spec at another setting **the binary can be running**, not
a wider bound on the same one. `raft_lsn.alt-loose-lsn-gate.cfg` exists because
`lsn_catchup_threshold_bytes()` picks between two thresholds by replication mode
and a single `LSNLagThreshold` stands for whichever is active, so one model
checks one of the two gates that ship. These run in `check-specs` beside
`<spec>.cfg`, so a PR passes both; they are optional, because most specs have a
single interesting point in their constant space.

A third kind of model is a **counterexample**: `<spec>.inv-<name>.cfg` disables
one mechanism the safety argument rests on and names, in an `EXPECT-VIOLATION:`
header, the invariant TLC then has to report. A run that succeeds is the failure,
and so is one that fails for a different reason — a parse error or a violated
`ASSUME` is not a counterexample. This is the same rule the Python harnesses live
under: an assertion nobody has watched fail is an assertion whose passing means
nothing. All four specs have one, and `make check-counterexamples` fails when a
spec does not — a target that silently checked three of four would read as
coverage the next time somebody added a spec.

| Spec                    | Mechanism disabled                                    | Invariant that must then break |
| ----------------------- | ----------------------------------------------------- | ------------------------------ |
| `lease_fencing`         | hold-down anchored at the campaign it wins            | `AtMostOneWriteAuthority`      |
| `raft_lsn`              | one vote per term                                     | `ElectionSafety`               |
| `commit_probing`        | LSN election gate (leader holds every replicated txn) | `AckedSuccessIsDurable`        |
| `timeline_verification` | pg_rewind's divergence gate                           | `TimelineMonotonic`            |

Each switch is a `CONSTANT` defaulting to the safe value in every other model,
so the mechanism is disabled only where a counterexample is the expected result.

## What each spec checks

| Spec                        | Verified property                                                         |
| --------------------------- | ------------------------------------------------------------------------- |
| `lease_fencing.tla`         | At most one node holds **write authority** across leadership transfer     |
| `raft_lsn.tla`              | Election safety + the LSN gate never deadlocks elections                  |
| `commit_probing.tla`        | In-doubt COMMIT probing is correct; acknowledged commits survive failover |
| `timeline_verification.tla` | PostgreSQL timelines stay bounded and never decrease across promotions    |

## Properties (checked by each `.cfg`)

**`lease_fencing.tla`** — passes (533,517 distinct states)

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
  not as an absolute clock, so the state space stays small and finite. The tick's
  length is the binary's, not the model's: `lint_matrix.py` derives it from
  `DEFAULT_LEASE_DURATION` and requires `QuorumTimeout` and `HoldDown` to land on
  the shipped milliseconds, and `VoteDelay` to sit at the boundary they admit
  (`HoldDown - QuorumTimeout`). Without that, retuning a constant in Rust leaves
  every model passing about a system nobody runs.
- Scope: the Raft log is abstracted away (that is `raft_lsn.tla`), and message
  loss appears only as a candidacy that did not win in time. `VoteDelay` is the
  network bound the argument cannot escape — a vote slower than
  `LeaseDuration - QuorumTimeout` breaks the inequality, and no lease scheme
  bounds that without assuming it.

**`raft_lsn.tla`** — passes (33,957 distinct states tight, 112,053 loose)

- `ElectionSafety` (≤1 leader), `NoLSNDeadlock` / `SomeNodeCanWin` (the LSN gate
  never wedges elections), `TypeOK`. `raft_lsn.inv-double-vote-per-term.cfg` is
  the checked config in which `ElectionSafety` breaks, which matters here more
  than elsewhere: the gate below is advisory, so this is the property safety
  actually rests on.
- Checked at **both** ends of the gate. `raft_lsn.cfg` is the tight threshold,
  where `NoLSNDeadlock` and `SomeNodeCanWin` bite because a narrow gate admits
  fewer candidates; `raft_lsn.alt-loose-lsn-gate.cfg` is the loose one, which
  reaches leaders further behind than the tight gate would ever have admitted
  and where `ElectionSafety` still has to hold.
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
  `commit_probing.inv-election-ignores-lsn.cfg` drops the requirement that a new
  leader hold every replicated transaction, and TLC produces the lost ack.

**`timeline_verification.tla`** — passes (3,208 distinct states)

- `TypeOK` + `TimelineMonotonic` — timelines stay bounded and a promotion only ever
  advances a node's timeline. No action in the base model can lower one, so
  `timeline_verification.inv-rewind-lowers-timeline.cfg` adds the one that can —
  a pg_rewind onto a source behind the node being rewound — and TLC catches the
  backwards step.
- Does **not** claim "no two primaries share a timeline" — false in the partition
  model. Single-primary safety lives in `lease_fencing.tla` (Raft); the `pg_rewind`
  data-loss gate is covered by Rust unit tests for `rewind_divergence_decision`.

## Code mapping

Each spec's header maps its TLA+ variables and actions to the Rust that implements
them, by file and function (not line number, which rots). The supervisor lives in
the `crates/pgbattery-supervisor/` workspace crate
(`crates/pgbattery-supervisor/src/process.rs`), not `src/supervisor/` (a re-export shim).

The mapping is a lint, not a comment. `lint_matrix.py`'s "TLA specs name real
code" resolves every `.rs` path a spec names against the workspace, and requires
the identifiers cited alongside it — anything with an underscore, or written with
empty parentheses — to be in that file. An entry naming one source claims every
line indented past it, so a citation that wraps is still read against the file it
named. A line naming several sources claims nothing, since there is no saying
which of them a name on it belongs to. A rename that leaves a spec behind fails
the lint rather than leaving a passing model checker describing a program that is
gone.
