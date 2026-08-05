# pgbattery Correctness Contracts

This document formally defines the correctness contracts for pgbattery. Every
CI test case must reference at least one contract ID. A violation of any
FATAL contract is a release-blocking bug.

---

## Write Contracts

### W1 — ACKed Write Durability (FATAL)

An acknowledged write (connection received success response) must survive every
supported fault (leader crash, network partition, rolling restart) and appear
exactly once in the final database state.

**Violation**: data loss or duplicate write after failover.

**Tests**: `acked-write-durability`, `failover-commit-boundary`, I1/I2/I3 in
`correctness-lite-invariants`. Under faults the clean matrix cannot produce:
`testing/durability_crash.py` (un-fsynced writes discarded with the process),
`testing/torn_write.py` (a half-written WAL segment or control file), and
`testing/wal_enospc.py` (the leader unable to allocate its next WAL segment).

---

### W2 — At-Most-Once Write Delivery (FATAL)

No write may be committed more than once. The PRIMARY KEY constraint on the
cluster must hold under all replication paths including split-brain recovery.

**Violation**: duplicate rows after failover, replication slot replay, or rogue
promotion.

**Tests**: `concurrent-writes-failover`, `rogue-pg-promote`, I3 / B3 / B4 in
`correctness-lite-invariants`.

B3 and B4 are the load-bearing oracles here: a sum-conservation check cannot
see a transfer applied twice, so B3 asserts each attempted transfer id appears
at most once (exactly once if acked) in `bank_ledger`, and B4 reconciles every
balance against the distinct ledger entries.

---

### W3 — DDL Atomicity (FATAL)

CREATE TABLE, CREATE INDEX, and other DDL must be fully committed or fully
absent after failover. Partial schema state (table without its PRIMARY KEY,
orphaned index without its table) must never persist.

**Violation**: catalog inconsistency after failover mid-DDL.

**Tests**: `ddl-failover`.

---

## Leadership Contracts

### L1 — Single Writable Leader (FATAL)

At most one node may be in write-accepting state at any point in time.
Two concurrent nodes with valid leases constitute a split-brain violation.

**Violation**: two nodes both accepting writes concurrently.

**Tests**: `stale-leader-fencing`, `rogue-pg-promote`, I4 in
`correctness-lite-invariants`. Directly, by racing concurrent writes at all
three internal PG ports: `testing/dual_writability_prober.py`, run standalone
and again inside `testing/wal_enospc.py`'s full-disk window.

---

### L2 — Lease-Fenced Write Rejection (FATAL)

During quorum loss, no write whose entire lifespan falls inside the quorum-loss
window may be acknowledged. The lease mechanism must block writes when the
leader cannot confirm quorum.

**Violation**: acked write during majority loss window.

**Tests**: `majority-loss`, `async-degraded-durability`, I5 in
`correctness-lite-invariants`.

An ack whose lifespan only _overlaps_ the quorum-loss window is outside this
contract's letter and is reported as the non-fatal `I5-WARN` observation rather
than silently dropped from the analysis.

---

### L3 — LSN-Safe Election (FATAL)

A candidate significantly behind the cluster's maximum known LSN (>16 MB by
default) must not be elected without operator intervention.

**Violation**: new leader elected with stale WAL, causing unrecoverable
data loss on promotion.

**Tests**: unit tests in `governor/state_machine.rs`
(`test_lsn_election_threshold_boundary`, `test_lsn_acceptable_for_election`).
`testing/torn_raft.py` covers the storage side of the same property: a node
whose `raft.db` was torn must tolerate the damage cleanly or refuse to start,
never come back voting on a store nothing vouched for.

---

## Liveness Contracts

### V1 — Bounded Failover Recovery (non-fatal / SLO)

After a supported fault (leader crash, single-node failure), a writable leader
must be elected within 30 seconds on a healthy 3-node cluster.

**Violation**: cluster stuck with no leader for > 30 seconds after a supported
fault.

**Tests**: `wait_cluster` timeout budgets in `ha-sequential`,
`ha-controlplane-pr` (`max_wait_cluster_seconds: 90`).

---

### V2 — Follower Resync (non-fatal / SLO)

A node that falls behind or whose data directory is corrupted must
automatically resync from the leader (via pg_basebackup or pg_rewind) and
rejoin the cluster without operator intervention.

**Violation**: node never rejoins after corruption or extended partition.

**Tests**: `diverged-node-rejoin`, `wal-hole-resync`, `storage-fault-recovery`.

---

## Session Contracts

### S1 — In-Transaction Failover Behavior (FATAL)

An open transaction on the old leader must be terminated (connection closed or
error returned to client) during failover. It must never be silently committed
on the new leader.

**Violation**: phantom commit of an in-flight transaction on the new leader.

**Tests**: `failover-commit-boundary`, `prepared-transaction-semantics`.

---

### S2 — Session Continuity Post-Failover (non-fatal / SLO)

Idle client sessions connected to the gateway must reconnect to the new leader
within the gateway's probe interval. Long-lived idle sessions may be
disconnected; the client is responsible for reconnection.

**Tests**: `gateway-connection-survival`, `session-semantics-contract`.

---

## Replication Contracts

### R1 — No Replication Slot Leak (FATAL)

Physical replication slots for departed or crashed nodes must not accumulate
indefinitely. Orphaned slots that block WAL recycling must be cleaned up
automatically by the replication manager.

**Violation**: pg_replication_slots contains inactive physical slots after
a node restarts or is removed.

**Tests**: `replication-slot-no-leak`.

---

### R2 — Synchronous Replica Acknowledgment (FATAL)

While at least one synchronous standby is present, the leader must not serve a
write ACK until the write has been flushed to the standby's WAL.

**Violation**: acked write not on standby disk before leader crash.

**Tests**: `acked-write-durability` (sync replication path). The standby's own
flush is only observable when nothing can cover for it, which is
`testing/durability_crash.py` in `cluster-crash` mode; `testing/torn_write.py`
and `testing/torn_raft.py` cover what survives when a write reaches the disk
only in part.

---

## Contract-to-Test Index

Every FATAL contract carries an **inversion**: a case or test that feeds the
oracle data it must reject, proving the oracle can fail. An oracle never
observed failing is indistinguishable from one that cannot, and a suite full of
those reports PASS on a broken cluster. `lint_matrix.py` enforces that this
column is non-empty for every FATAL row.

| Contract                          | Severity | Primary Tests                                                                                                                                                                            | Inversion (proves the oracle can fail)                                                                                                                                                                                                                  |
| --------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| W1                                | FATAL    | `acked-write-durability`, `failover-commit-boundary`, `correctness-lite-invariants` I1/I2, `testing/durability_crash.py`, `testing/torn_write.py` (WAL/control), `testing/wal_enospc.py` | `assert-sanity-acked`, `assert-sanity-acked-dup`, `assert-sanity-chaos-oracle{,-post,-full}`, plus each durability suite's own inverted mode: `testing/durability_crash.py` weakened, `testing/torn_write.py` mangled, `testing/wal_enospc.py` unfilled |
| W2                                | FATAL    | `concurrent-writes-failover`, `rogue-pg-promote`, `correctness-lite-invariants` I3/B3/B4                                                                                                 | `assert-sanity-concurrent`, `assert-sanity-cascade-atomicity`, unit: `test_duplicate_ledger_row_flags_b3`                                                                                                                                               |
| W3                                | FATAL    | `ddl-failover`                                                                                                                                                                           | `assert-sanity-ddl`                                                                                                                                                                                                                                     |
| L1                                | FATAL    | `stale-leader-fencing`, `rogue-pg-promote`, `correctness-lite-invariants` I4, `dual-writability-prober`, `testing/wal_enospc.py` (prober across the full-disk window)                    | unit: `test_two_confirmed_acceptances_is_a_fatal_violation`, `test_split_brain_signal_flags_l2`                                                                                                                                                         |
| L2                                | FATAL    | `majority-loss`, `async-degraded-durability`, `correctness-lite-invariants` I5                                                                                                           | unit: `test_contained_ack_is_fatal_i5`, `test_fence_failure_signal_flags_l2`                                                                                                                                                                            |
| L3                                | FATAL    | unit: `test_lsn_election_threshold_boundary`, `testing/torn_raft.py` — a torn `raft.db` must tolerate or refuse, never vote on a store nothing vouched for                               | unit: `test_emergency_fence_without_confirmation_flags_l3`, LSN-gate proptest, `testing/torn_raft.py` run with the store mangled past repair                                                                                                            |
| Linearizability (single-register) | FATAL    | `linearizability-register`                                                                                                                                                               | `testing/test_checker_sanity.py` — every `assert_flagged` case                                                                                                                                                                                          |
| V1                                | SLO      | `ha-sequential` wait budgets, `ha-controlplane-pr`                                                                                                                                       | —                                                                                                                                                                                                                                                       |
| V2                                | SLO      | `diverged-node-rejoin`, `wal-hole-resync`, `storage-fault-recovery`                                                                                                                      | `assert-sanity-diverged`                                                                                                                                                                                                                                |
| S1                                | FATAL    | `failover-commit-boundary`, `prepared-transaction-semantics`                                                                                                                             | `assert-sanity-commit-boundary`                                                                                                                                                                                                                         |
| S2                                | SLO      | `gateway-connection-survival`, `session-semantics-contract`                                                                                                                              | `assert-sanity-gateway-migration`                                                                                                                                                                                                                       |
| R1                                | FATAL    | `replication-slot-no-leak`                                                                                                                                                               | `assert-sanity-slot-leak`                                                                                                                                                                                                                               |
| R2                                | FATAL    | `acked-write-durability` (sync path), `testing/durability_crash.py` cluster-crash, `testing/torn_write.py`, `testing/torn_raft.py`                                                       | `assert-sanity-acked-dup`, `testing/durability_crash.py` weakened under cluster-crash, `testing/torn_write.py` with full_page_writes off, `testing/torn_raft.py` mangled                                                                                |
