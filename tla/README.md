# TLA+ Specifications

Formal models of pgbattery's core safety properties.

> **Validation status.** These specs are **not run in CI** and are **not
> machine-checked in this repository** — TLC is not installed here. Each spec's
> invariants were verified by hand against its actions (a proof sketch). Run TLC
> (below) before relying on any `THEOREM`/`INVARIANT`. Treat the `.tla` files as
> the source of truth for _what is claimed_; only a TLC run establishes that the
> claims _hold_.

## Specs

| Spec                        | What it checks                                                              |
| --------------------------- | -------------------------------------------------------------------------- |
| `lease_fencing.tla`         | At most one node holds **write authority** across leadership transfer      |
| `raft_lsn.tla`              | LSN-aware voting never elects a leader a voter deemed too far behind        |
| `commit_probing.tla`        | In-doubt COMMIT probing is correct; acknowledged commits survive failover  |
| `timeline_verification.tla` | PostgreSQL timelines stay bounded and never decrease across promotions      |

## Run

```bash
# Install TLC (one-time)
brew install --cask tla-plus-toolbox

# Run each model (artifacts go into artifacts/)
cd tla/
tlc lease_fencing.tla         -config lease_fencing.cfg         -metadir artifacts
tlc raft_lsn.tla              -config raft_lsn.cfg              -metadir artifacts
tlc commit_probing.tla        -config commit_probing.cfg        -metadir artifacts
tlc timeline_verification.tla -config timeline_verification.cfg -metadir artifacts
```

## Properties checked (by `.cfg`)

**`lease_fencing.tla`**

- `AtMostOneWriteAuthority` — at most one node has a valid lease **and** a
  writable PG at any instant. Non-vacuous: leadership transfers in the model, so
  a deposed leader and a new leader coexist; the promotion hold-down plus the
  quorum-ack-anchored lease keep their write windows disjoint. Set `HoldDown = 0`
  in the cfg and TLC produces the two-writer (split-brain) counterexample.
- `SelfFenceOnQuorumLoss` — a leader that stops getting quorum acks loses write
  authority within `QuorumTimeout` of its last ack.
- Scope note: the hold-down is modeled anchored at the election instant. The
  implementation anchors at the new leader's _local_ leader-loss observation,
  which coincides only for fast failover. The partial-partition case and its
  sync-replication backstop are documented in the spec header.

**`raft_lsn.tla`**

- `ElectionSafety` — at most one leader.
- `LeaderHasAcceptableLSN` / `LeaderLSNNotBelowVoters` — every node that voted
  for the leader found its LSN acceptable (the LSN-safety property).
- `NoLSNDeadlock` / `SomeNodeCanWin` — the LSN gate never wedges elections.
- Scope note: a single abstract threshold stands in for the code's dual
  sync/async threshold; the staleness window and the election-vs-promotion
  fail-open/closed split are documented, not modeled (no clock in this spec).

**`commit_probing.tla`**

- `ProbeCommittedImpliesVisible` / `ProbeAbortedImpliesNotWritten` — the probe
  never reports a false commit or a false abort.
- `AckedSuccessIsDurable` — any success the client saw (normal or synthetic)
  denotes a transaction still visible on the current leader after failover (RPO=0).

**`timeline_verification.tla`**

- `TypeOK` + `TimelineMonotonic` — timelines stay in bounds and a promotion only
  ever advances a node's timeline.
- Does **not** claim "no two primaries share a timeline" — that is false in the
  partition model. Single-primary safety lives in `lease_fencing.tla` (Raft); the
  `pg_rewind` data-loss gate is covered by Rust unit tests for
  `rewind_divergence_decision`.

## Code mapping

Each spec's header maps its TLA+ variables and actions to the Rust that
implements them, by file and function (not line number, which rots). Example
from `lease_fencing.tla`:

```
LeaseIsValid(n)          →  governor/lease.rs            is_valid()
renew(quorum_ack_age)    →  governor/lease.rs            renew()  (anchor = last ack + duration)
BelievesHasQuorum (1s)   →  governor/raft.rs             millis_since_quorum_ack < QUORUM_TIMEOUT_MS
HoldDown gate            →  app.rs                       promotion_lease_holddown()
```

The supervisor lives in the `crates/pgbattery-supervisor/` workspace crate
(`crates/pgbattery-supervisor/src/process.rs`), not `src/supervisor/` (which is
a re-export shim).
