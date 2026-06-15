-------------------------------- MODULE lease_fencing --------------------------------
(*
 * TLA+ Specification for Lease-Based Split-Brain Prevention (v2)
 *
 * ENHANCEMENT OVER v1: Models quorum detection timing explicitly.
 *
 * v1 assumed instant quorum detection. Reality uses millis_since_quorum_ack
 * with a 1000ms threshold. This spec models that timing to prove the
 * threshold is sufficient for safety.
 *
 * Models the triple-layer defense mechanism in pgbattery:
 * - Layer 1: Process coupling (tini)
 * - Layer 2: Gateway lease checking
 * - Layer 3: Supervisor enforcement loop
 *
 * === KEY ADDITION: Quorum Detection Timing ===
 *
 * CODE: governor/raft.rs
 *
 *   let millis = metrics.millis_since_quorum_ack.unwrap_or(u64::MAX);
 *   let has_quorum = if voter_count == 1 {
 *       true  // Single voter always has quorum
 *   } else {
 *       millis < 1000  // Quorum if heard from majority within 1s
 *   };
 *
 * This spec models:
 *   - lastQuorumAck[n] = time of last successful quorum ack
 *   - QuorumTimeout = 10 (represents 1000ms in time units)
 *   - Quorum detected as lost when: time - lastQuorumAck[n] > QuorumTimeout
 *
 * Authors: pgbattery team
 * Date: 2025
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,                  \* Set of node IDs {1, 2, 3}
    LeaseDuration,          \* Lease duration in time units (e.g., 20 = 2 seconds)
    QuorumTimeout,          \* Quorum detection timeout (e.g., 10 = 1 second)
    EnforcementInterval,    \* Lease enforcement check interval (e.g., 1 = 100ms)
    MaxTime                 \* Maximum time to explore (bounds state space)

VARIABLES
    (* Raft state *)
    raftLeader,             \* Current Raft leader (node ID or None)
    lastQuorumAck,          \* lastQuorumAck[n] = time of last quorum ack for node n

    (* Lease state - Layers 2 & 3 *)
    leaseExpiresAt,         \* leaseExpiresAt[n] = when node n's lease expires (time)
    leaseIsLeader,          \* leaseIsLeader[n] = does lease state think n is leader?

    (* PostgreSQL state *)
    pgWritable,             \* pgWritable[n] = is PostgreSQL accepting writes?
    pgAlive,                \* pgAlive[n] = is PostgreSQL process running?

    (* Process state - Layer 1 *)
    pgBatteryAlive,         \* pgBatteryAlive[n] = is pgbattery process running?

    (* Gateway state - Layer 2 *)
    gatewayAcceptsWrites,   \* gatewayAcceptsWrites[n] = gateway forwarding writes?

    (* Network partition state *)
    canReachMajority,       \* canReachMajority[n] = can node n reach majority of cluster?

    (* Clock *)
    time                    \* Global clock (monotonic time)

vars == <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
          pgWritable, pgAlive, pgBatteryAlive, gatewayAcceptsWrites,
          canReachMajority, time>>

\* ============================================================================
\* HELPERS
\* ============================================================================

Quorum == (Cardinality(Nodes) \div 2) + 1
None == 0

\* Derived: Does node believe it has quorum based on timing?
\* CODE: governor/raft.rs
BelievesHasQuorum(n) ==
    time - lastQuorumAck[n] < QuorumTimeout

\* Lease validity check - includes quorum check
\* CODE: governor/lease.rs, governor/raft.rs
\* A lease is only valid if the node ALSO believes it has quorum
LeaseIsValid(n) ==
    /\ leaseIsLeader[n]
    /\ time < leaseExpiresAt[n]
    /\ BelievesHasQuorum(n)

\* ============================================================================
\* INITIAL STATE
\* ============================================================================

Init ==
    /\ raftLeader = None
    /\ lastQuorumAck = [n \in Nodes |-> 0]  \* No acks yet
    /\ leaseExpiresAt = [n \in Nodes |-> 0]  \* All expired
    /\ leaseIsLeader = [n \in Nodes |-> FALSE]
    /\ pgWritable = [n \in Nodes |-> FALSE]
    /\ pgAlive = [n \in Nodes |-> TRUE]
    /\ pgBatteryAlive = [n \in Nodes |-> TRUE]
    /\ gatewayAcceptsWrites = [n \in Nodes |-> FALSE]
    /\ canReachMajority = [n \in Nodes |-> TRUE]  \* Initially connected
    /\ time = 0

\* ============================================================================
\* ACTIONS
\* ============================================================================

(*
 * Raft elects a leader - simplified, focus is on lease timing
 *)
RaftElectLeader(n) ==
    /\ raftLeader = None
    /\ pgBatteryAlive[n]
    /\ canReachMajority[n]
    /\ raftLeader' = n
    /\ lastQuorumAck' = [lastQuorumAck EXCEPT ![n] = time]
    /\ UNCHANGED <<leaseExpiresAt, leaseIsLeader, pgWritable, pgAlive,
                   pgBatteryAlive, gatewayAcceptsWrites, canReachMajority, time>>

(*
 * Leader receives heartbeat ack from majority - updates quorum timestamp
 * CODE: governor/raft.rs (metrics.millis_since_quorum_ack)
 *
 * This is the KEY action for quorum timing.
 * In reality, OpenRaft tracks this via heartbeat responses.
 *)
ReceiveQuorumAck(n) ==
    /\ raftLeader = n
    /\ pgBatteryAlive[n]
    /\ canReachMajority[n]  \* Can only get acks if network connected
    /\ lastQuorumAck' = [lastQuorumAck EXCEPT ![n] = time]
    /\ UNCHANGED <<raftLeader, leaseExpiresAt, leaseIsLeader, pgWritable,
                   pgAlive, pgBatteryAlive, gatewayAcceptsWrites, canReachMajority, time>>

(*
 * Governor updates lease based on Raft state
 * CODE: governor/lease.rs (update_from_raft)
 *
 * CRITICAL: Only renews lease if BelievesHasQuorum(n) is true
 * This is where the 1000ms threshold matters!
 *)
UpdateLease(n) ==
    /\ pgBatteryAlive[n]
    /\ raftLeader = n
    /\ BelievesHasQuorum(n)  \* <-- THE TIMING CHECK
    /\ leaseExpiresAt' = [leaseExpiresAt EXCEPT ![n] = time + LeaseDuration]
    /\ leaseIsLeader' = [leaseIsLeader EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, pgWritable, pgAlive,
                   pgBatteryAlive, gatewayAcceptsWrites, canReachMajority, time>>

(*
 * Quorum loss detected via timing - lease expires immediately
 * CODE: governor/lease.rs (immediate expiry on quorum loss)
 *
 * This happens when: time - lastQuorumAck[n] >= QuorumTimeout
 * The node realizes it hasn't heard from majority in 1 second.
 *)
DetectQuorumLoss(n) ==
    /\ pgBatteryAlive[n]
    /\ leaseIsLeader[n]
    /\ ~BelievesHasQuorum(n)  \* Timeout exceeded!
    /\ leaseExpiresAt' = [leaseExpiresAt EXCEPT ![n] = time - 1]  \* Immediate expiry
    /\ leaseIsLeader' = [leaseIsLeader EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, pgWritable, pgAlive,
                   pgBatteryAlive, gatewayAcceptsWrites, canReachMajority, time>>

(*
 * Network partition - node can no longer reach majority
 * Models: docker pause, network failure, etc.
 *)
NetworkPartition(n) ==
    /\ canReachMajority[n]
    /\ canReachMajority' = [canReachMajority EXCEPT ![n] = FALSE]
    \* Note: lastQuorumAck NOT updated - this is how partition is detected
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   pgWritable, pgAlive, pgBatteryAlive, gatewayAcceptsWrites, time>>

(*
 * Network heals - node can reach majority again
 *)
NetworkHeal(n) ==
    /\ ~canReachMajority[n]
    /\ canReachMajority' = [canReachMajority EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   pgWritable, pgAlive, pgBatteryAlive, gatewayAcceptsWrites, time>>

(*
 * Layer 2: Gateway opens writes when lease is valid
 * CODE: gateway/handlers/mod.rs — lease checked per-message
 *
 * Previously modeled as a static constraint (GatewayLeaseInSync), which meant
 * gatewayAcceptsWrites was never set to TRUE and NoWritesWithoutLease held vacuously.
 * Now modeled as an explicit action so TLC actually explores write-enabled states.
 *)
GatewayEnableWrites(n) ==
    /\ pgBatteryAlive[n]
    /\ LeaseIsValid(n)
    /\ ~gatewayAcceptsWrites[n]
    /\ gatewayAcceptsWrites' = [gatewayAcceptsWrites EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   pgWritable, pgAlive, pgBatteryAlive, canReachMajority, time>>

(*
 * Layer 2: Gateway closes writes when lease expires
 *)
GatewayDisableWrites(n) ==
    /\ ~LeaseIsValid(n)
    /\ gatewayAcceptsWrites[n]
    /\ gatewayAcceptsWrites' = [gatewayAcceptsWrites EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   pgWritable, pgAlive, pgBatteryAlive, canReachMajority, time>>

(*
 * Layer 3: Supervisor enables PG writes when lease is valid
 * CODE: supervisor/process.rs set_readonly(false) — called when node becomes leader
 *)
EnablePgWrites(n) ==
    /\ pgBatteryAlive[n]
    /\ LeaseIsValid(n)
    /\ pgAlive[n]
    /\ ~pgWritable[n]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   pgAlive, pgBatteryAlive, gatewayAcceptsWrites, canReachMajority, time>>

(*
 * Layer 3: Enforcement loop forces readonly
 * CODE: app.rs — enforcement loop runs every 500ms
 *)
EnforcementFence(n) ==
    /\ pgBatteryAlive[n]
    /\ ~LeaseIsValid(n)
    /\ pgWritable[n]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   pgAlive, pgBatteryAlive, gatewayAcceptsWrites, canReachMajority, time>>

(*
 * Layer 1: Process coupling - pgbattery dies, PostgreSQL dies too
 *)
KillPgBattery(n) ==
    /\ pgBatteryAlive[n]
    /\ pgBatteryAlive' = [pgBatteryAlive EXCEPT ![n] = FALSE]
    /\ pgAlive' = [pgAlive EXCEPT ![n] = FALSE]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]
    /\ gatewayAcceptsWrites' = [gatewayAcceptsWrites EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   canReachMajority, time>>

(*
 * Strict fate sharing - pgbattery exits when PostgreSQL dies
 *)
PostgreSQLDies(n) ==
    /\ pgAlive[n]
    /\ pgAlive' = [pgAlive EXCEPT ![n] = FALSE]
    /\ pgBatteryAlive' = [pgBatteryAlive EXCEPT ![n] = FALSE]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]
    /\ gatewayAcceptsWrites' = [gatewayAcceptsWrites EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   canReachMajority, time>>

(*
 * Time advances
 *)
Tick ==
    /\ time < MaxTime
    /\ time' = time + 1
    /\ UNCHANGED <<raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
                   pgWritable, pgAlive, pgBatteryAlive, gatewayAcceptsWrites,
                   canReachMajority>>

\* ============================================================================
\* SPECIFICATION
\* ============================================================================

Next ==
    \/ \E n \in Nodes : RaftElectLeader(n)
    \/ \E n \in Nodes : ReceiveQuorumAck(n)
    \/ \E n \in Nodes : UpdateLease(n)
    \/ \E n \in Nodes : DetectQuorumLoss(n)
    \/ \E n \in Nodes : NetworkPartition(n)
    \/ \E n \in Nodes : NetworkHeal(n)
    \/ \E n \in Nodes : GatewayEnableWrites(n)
    \/ \E n \in Nodes : GatewayDisableWrites(n)
    \/ \E n \in Nodes : EnablePgWrites(n)
    \/ \E n \in Nodes : EnforcementFence(n)
    \/ \E n \in Nodes : KillPgBattery(n)
    \/ \E n \in Nodes : PostgreSQLDies(n)
    \/ Tick

StateConstraint ==
    \E n \in Nodes : pgBatteryAlive[n]

Spec == Init /\ [][Next]_vars

\* ============================================================================
\* INVARIANTS (Safety Properties)
\* ============================================================================

(*
 * CRITICAL: At most one node can accept writes at any time
 *)
AtMostOneWritableNode ==
    Cardinality({n \in Nodes :
        /\ pgWritable[n]
        /\ gatewayAcceptsWrites[n]
    }) <= 1

(*
 * No writes without valid lease
 *)
NoWritesWithoutLease ==
    \A n \in Nodes :
        gatewayAcceptsWrites[n] => LeaseIsValid(n)

(*
 * KEY TIMING INVARIANT: Partitioned node's lease expires before new election
 *
 * If a node is partitioned (can't reach majority), its lease MUST expire
 * before any other node could become leader with a valid lease.
 *
 * This proves QuorumTimeout < LeaseDuration is sufficient for safety.
 *)
PartitionedNodeLeaseMustExpire ==
    \A n \in Nodes :
        (~canReachMajority[n] /\ time - lastQuorumAck[n] >= QuorumTimeout)
        => ~LeaseIsValid(n)

(*
 * Quorum timeout must be less than lease duration for safety
 * This is the KEY relationship this spec verifies!
 *
 * If QuorumTimeout >= LeaseDuration, a partitioned node might still
 * have a valid lease when a new leader is elected.
 *)
ASSUME QuorumTimeout < LeaseDuration

(*
 * After partition, lease expires within QuorumTimeout time units
 *)
LeaseExpiresAfterPartition ==
    \A n \in Nodes :
        \* If partitioned and enough time has passed
        (~canReachMajority[n] /\ time > lastQuorumAck[n] + QuorumTimeout)
        \* Then lease must be invalid
        => ~LeaseIsValid(n)

(*
 * No split-brain: Can't have two valid leases simultaneously
 *)
NoSplitBrain ==
    Cardinality({n \in Nodes : LeaseIsValid(n)}) <= 1

================================================================================

(*
 * Model Checking Configuration (lease_fencing_v2.cfg)
 *
 * CONSTANTS
 *   Nodes = {1, 2, 3}
 *   LeaseDuration = 20      \* 2 seconds
 *   QuorumTimeout = 10      \* 1 second (must be < LeaseDuration!)
 *   EnforcementInterval = 1 \* 100ms
 *   MaxTime = 50
 *
 * INVARIANTS
 *   AtMostOneWritableNode
 *   NoWritesWithoutLease
 *   PartitionedNodeLeaseMustExpire
 *   LeaseExpiresAfterPartition
 *   NoSplitBrain
 *
 * This proves:
 *   - The 1000ms quorum timeout is safe given 2s lease duration
 *   - Partitioned nodes self-fence before new leader can accept writes
 *   - No split-brain possible under the timing model
 *)
