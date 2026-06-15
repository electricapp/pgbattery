# TLA+ Specifications

Formal verification of pgbattery's core safety properties.

## Specs

| Spec                        | Verifies                                        |
| --------------------------- | ----------------------------------------------- |
| `raft_lsn.tla`              | LSN-aware voting prevents stale leader election |
| `lease_fencing.tla`         | Quorum timing ensures no split-brain            |
| `commit_probing.tla`        | In-doubt transaction probing is correct         |
| `timeline_verification.tla` | Timeline divergence detected before promotion   |

## Run

```bash
# Install TLC
brew install --cask tla-plus-toolbox

# Run model checker (artifacts go into artifacts/)
cd tla/
tlc raft_lsn.tla -config raft_lsn.cfg -metadir artifacts
tlc lease_fencing.tla -config lease_fencing.cfg -metadir artifacts
tlc commit_probing.tla -config commit_probing.cfg -metadir artifacts
tlc timeline_verification.tla -config timeline_verification.cfg -metadir artifacts
```

## Key Properties Verified

**raft_lsn.tla:**

- `ElectionSafety` - At most one leader per term
- `LeaderHasAcceptableLSN` - Leader not stale
- `NoElectionDeadlock` - LSN constraints don't cause permanent deadlock

**lease_fencing.tla:**

- `NoSplitBrain` - At most one valid lease at any time
- `PartitionedNodeLeaseMustExpire` - 1s quorum timeout < 2s lease duration

**commit_probing.tla:**

- `ProbeCommittedImpliesVisible` - If probe says "committed", data exists on new leader

## Code Mapping

Each spec documents the mapping to Rust code. Example from `lease_fencing.tla`:

```
LeaseIsValid(n) in TLA+  →  lease.rs:63 is_valid()
QuorumTimeout (1000ms)   →  raft.rs:220 millis_since_quorum_ack < 1000
```
