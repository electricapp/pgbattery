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
`correctness-lite-invariants`.

---

### W2 — At-Most-Once Write Delivery (FATAL)

No write may be committed more than once. The PRIMARY KEY constraint on the
cluster must hold under all replication paths including split-brain recovery.

**Violation**: duplicate rows after failover, replication slot replay, or rogue
promotion.

**Tests**: `concurrent-writes-failover`, `rogue-pg-promote`, I3 in
`correctness-lite-invariants`.

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
`correctness-lite-invariants`.

---

### L2 — Lease-Fenced Write Rejection (FATAL)

During quorum loss, no write whose entire lifespan falls inside the quorum-loss
window may be acknowledged. The lease mechanism must block writes when the
leader cannot confirm quorum.

**Violation**: acked write during majority loss window.

**Tests**: `majority-loss`, `async-degraded-durability`, I5 in
`correctness-lite-invariants`.

---

### L3 — LSN-Safe Election (FATAL)

A candidate significantly behind the cluster's maximum known LSN (>16 MB by
default) must not be elected without operator intervention.

**Violation**: new leader elected with stale WAL, causing unrecoverable
data loss on promotion.

**Tests**: unit tests in `governor/state_machine.rs`
(`test_lsn_election_threshold_boundary`, `test_lsn_acceptable_for_election`).

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

**Tests**: `acked-write-durability` (sync replication path).

---

## Contract-to-Test Index

| Contract                          | Severity | Primary Tests                                                                             |
| --------------------------------- | -------- | ----------------------------------------------------------------------------------------- |
| W1                                | FATAL    | `acked-write-durability`, `failover-commit-boundary`, `correctness-lite-invariants` I1/I2 |
| W2                                | FATAL    | `concurrent-writes-failover`, `rogue-pg-promote`, `correctness-lite-invariants` I3        |
| W3                                | FATAL    | `ddl-failover`                                                                            |
| L1                                | FATAL    | `stale-leader-fencing`, `rogue-pg-promote`, `correctness-lite-invariants` I4              |
| L2                                | FATAL    | `majority-loss`, `async-degraded-durability`, `correctness-lite-invariants` I5            |
| L3                                | FATAL    | unit: `test_lsn_election_threshold_boundary`                                              |
| Linearizability (single-register) | FATAL    | `linearizability-register`                                                                |
| V1                                | SLO      | `ha-sequential` wait budgets, `ha-controlplane-pr`                                        |
| V2                                | SLO      | `diverged-node-rejoin`, `wal-hole-resync`, `storage-fault-recovery`                       |
| S1                                | FATAL    | `failover-commit-boundary`, `prepared-transaction-semantics`                              |
| S2                                | SLO      | `gateway-connection-survival`, `session-semantics-contract`                               |
| R1                                | FATAL    | `replication-slot-no-leak`                                                                |
| R2                                | FATAL    | `acked-write-durability` (sync path)                                                      |
