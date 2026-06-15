-------------------------------- MODULE commit_probing --------------------------------
(*
 * TLA+ Specification for Commit Probing (In-Doubt Transaction Recovery)
 *
 * Models the gateway's ability to recover transaction status after leader
 * failure during COMMIT. This is the "in-doubt transaction" problem.
 *
 * Scenario:
 *   1. Client sends COMMIT through gateway
 *   2. Gateway captures txid_current() before forwarding
 *   3. Leader crashes after WAL write but before response
 *   4. Gateway detects disconnect, probes new leader with txid_status()
 *   5. Returns synthetic success/failure to client
 *
 * === MAPPING TO IMPLEMENTATION ===
 *
 * TLA+ Variable → Rust Code:
 *   txidCaptured[c]     → gateway/handlers/mod.rs: txid captured before COMMIT forwarded
 *   commitSent[c]       → gateway/handlers/mod.rs: after Query("COMMIT") forwarded
 *   walWritten[txid]    → PostgreSQL: transaction in WAL
 *   replicated[txid]    → PostgreSQL: replicated to sync replica
 *   probeResult[c]      → gateway/handlers/mod.rs: probe_transaction_status()
 *
 * CODE REFERENCES:
 *   gateway/handlers/mod.rs - Commit detection, txid capture, probe logic, synthetic responses
 *
 * SAFETY PROPERTY:
 *   If probe returns "committed", the transaction MUST exist on new leader.
 *   If probe returns "aborted", the transaction MUST NOT exist.
 *
 * Authors: pgbattery team
 * Date: 2025
 *)

EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
    Connections,        \* Set of client connections {c1, c2, ...}
    Nodes,              \* Set of database nodes {1, 2, 3}
    MaxTxid             \* Maximum transaction ID to explore

VARIABLES
    (* Leader state *)
    leader,             \* Current leader node (or None)

    (* Transaction state - per connection *)
    txidCaptured,       \* txidCaptured[c] = txid captured before COMMIT (or 0)
    commitSent,         \* commitSent[c] = has COMMIT been sent to backend?
    leaderWhenSent,     \* leaderWhenSent[c] = which leader the commit was sent to
    responseReceived,   \* responseReceived[c] = did client get response?
    probeResult,        \* probeResult[c] = result of probe ("none", "committed", "aborted", "unknown")

    (* Database state - per txid *)
    walWritten,         \* walWritten[txid] = is txid in leader's WAL?
    replicated,         \* replicated[txid] = is txid on sync replica?

    (* Per-node transaction visibility *)
    visibleOn           \* visibleOn[txid] = set of nodes where txid is visible

vars == <<leader, txidCaptured, commitSent, leaderWhenSent, responseReceived, probeResult,
          walWritten, replicated, visibleOn>>

\* ============================================================================
\* HELPERS
\* ============================================================================

None == 0
Quorum == (Cardinality(Nodes) \div 2) + 1

\* A transaction is committed if it's in WAL and replicated
IsCommitted(txid) == walWritten[txid] /\ replicated[txid]

\* A transaction is visible on a node
IsVisibleOn(txid, node) == node \in visibleOn[txid]

\* ============================================================================
\* INITIAL STATE
\* ============================================================================

Init ==
    /\ leader = 1  \* Node 1 starts as leader
    /\ txidCaptured = [c \in Connections |-> 0]
    /\ commitSent = [c \in Connections |-> FALSE]
    /\ leaderWhenSent = [c \in Connections |-> 0]  \* 0 = not sent yet
    /\ responseReceived = [c \in Connections |-> FALSE]
    /\ probeResult = [c \in Connections |-> "none"]
    /\ walWritten = [t \in 1..MaxTxid |-> FALSE]
    /\ replicated = [t \in 1..MaxTxid |-> FALSE]
    /\ visibleOn = [t \in 1..MaxTxid |-> {}]

\* ============================================================================
\* ACTIONS
\* ============================================================================

(*
 * Client starts transaction and gateway captures txid
 * Each txid is unique - no two connections can have the same txid
 *)
BeginTransaction(c, txid) ==
    /\ txidCaptured[c] = 0  \* No active transaction
    /\ txid > 0
    /\ leader /= None
    \* Txid must be unique - not already used by another connection
    /\ \A other \in Connections : txidCaptured[other] /= txid
    /\ txidCaptured' = [txidCaptured EXCEPT ![c] = txid]
    /\ UNCHANGED <<leader, commitSent, leaderWhenSent, responseReceived, probeResult,
                   walWritten, replicated, visibleOn>>

(*
 * Gateway forwards COMMIT to backend
 *)
SendCommit(c) ==
    /\ txidCaptured[c] /= 0  \* Has active transaction
    /\ ~commitSent[c]        \* Haven't sent COMMIT yet
    /\ leader /= None
    /\ commitSent' = [commitSent EXCEPT ![c] = TRUE]
    /\ leaderWhenSent' = [leaderWhenSent EXCEPT ![c] = leader]  \* Record which leader
    /\ UNCHANGED <<leader, txidCaptured, responseReceived, probeResult,
                   walWritten, replicated, visibleOn>>

(*
 * Leader writes transaction to WAL (but not yet replicated)
 * This models the window where crash = data on leader only.
 *
 * IMPORTANT: Once a connection has probed (after failover), no more WAL writes
 * can happen for that transaction - the probe captured the final state.
 *)
WriteToWAL(c) ==
    /\ commitSent[c]
    /\ probeResult[c] = "none"    \* No WAL write after probe - probe captured final state
    /\ leader /= None
    /\ leader = leaderWhenSent[c]  \* WAL write only happens on original leader
    /\ LET txid == txidCaptured[c]
       IN /\ ~walWritten[txid]
          /\ walWritten' = [walWritten EXCEPT ![txid] = TRUE]
          /\ visibleOn' = [visibleOn EXCEPT ![txid] = visibleOn[txid] \cup {leader}]
    /\ UNCHANGED <<leader, txidCaptured, commitSent, leaderWhenSent, responseReceived,
                   probeResult, replicated>>

(*
 * Transaction replicates to sync replica
 * After this, transaction survives leader failure.
 * NOTE: Replication requires data to be on the current leader.
 *)
ReplicateToSync(txid) ==
    /\ walWritten[txid]
    /\ ~replicated[txid]
    /\ leader /= None
    /\ IsVisibleOn(txid, leader)  \* Data must be on current leader to replicate
    /\ \E replica \in Nodes \ {leader} :
        /\ replicated' = [replicated EXCEPT ![txid] = TRUE]
        /\ visibleOn' = [visibleOn EXCEPT ![txid] = visibleOn[txid] \cup {replica}]
    /\ UNCHANGED <<leader, txidCaptured, commitSent, leaderWhenSent, responseReceived,
                   probeResult, walWritten>>

(*
 * Client receives successful COMMIT response (normal path)
 * Only happens if transaction is replicated (sync commit)
 *)
ReceiveCommitResponse(c) ==
    /\ commitSent[c]
    /\ ~responseReceived[c]
    /\ leader /= None
    /\ leader = leaderWhenSent[c]  \* Response comes from original leader
    /\ LET txid == txidCaptured[c]
       IN replicated[txid]  \* Sync commit - must be replicated
    /\ responseReceived' = [responseReceived EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<leader, txidCaptured, commitSent, leaderWhenSent, probeResult,
                   walWritten, replicated, visibleOn>>

(*
 * Leader crashes - connection lost before response
 * Gateway detects backend disconnect and enters probe path
 *)
LeaderCrashes ==
    /\ leader /= None
    /\ leader' = None
    /\ UNCHANGED <<txidCaptured, commitSent, leaderWhenSent, responseReceived, probeResult,
                   walWritten, replicated, visibleOn>>

(*
 * New leader elected from nodes that have replicated transactions
 * In sync-commit mode, the new leader must have all replicated data.
 * This models proper failover behavior: sync replica becomes leader.
 *)
ElectNewLeader(n) ==
    /\ leader = None
    /\ n \in Nodes
    \* New leader must have all replicated transactions
    \* (i.e., must be a node that was a sync replica)
    /\ \A txid \in 1..MaxTxid : replicated[txid] => IsVisibleOn(txid, n)
    /\ leader' = n
    /\ UNCHANGED <<txidCaptured, commitSent, leaderWhenSent, responseReceived, probeResult,
                   walWritten, replicated, visibleOn>>

(*
 * Gateway probes new leader for transaction status
 * Uses txid_status() to determine final outcome:
 *
 * txid_status() returns:
 *   - "committed" if transaction committed and visible
 *   - "aborted" if transaction aborted
 *   - "in progress" / NULL if unknown (we treat as "unknown")
 *
 * IMPORTANT: Probe only happens after failover (leader changed)
 *)
ProbeNewLeader(c) ==
    /\ commitSent[c]
    /\ ~responseReceived[c]
    /\ probeResult[c] = "none"
    /\ leader /= None             \* New leader must be elected
    /\ leader /= leaderWhenSent[c] \* Must be a DIFFERENT leader (failover occurred)
    /\ LET txid == txidCaptured[c]
       IN probeResult' = [probeResult EXCEPT ![c] =
            IF IsVisibleOn(txid, leader)
            THEN "committed"
            ELSE IF ~walWritten[txid]
                 THEN "aborted"
                 ELSE "unknown"]  \* WAL written but not on new leader
    /\ UNCHANGED <<leader, txidCaptured, commitSent, leaderWhenSent, responseReceived,
                   walWritten, replicated, visibleOn>>

(*
 * Gateway returns synthetic response based on probe result
 *)
ReturnSyntheticResponse(c) ==
    /\ probeResult[c] \in {"committed", "aborted"}
    /\ ~responseReceived[c]
    /\ responseReceived' = [responseReceived EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<leader, txidCaptured, commitSent, leaderWhenSent, probeResult,
                   walWritten, replicated, visibleOn>>

(*
 * Connection gives up on unknown probe result
 * Client must retry and check manually
 *)
GiveUpOnUnknown(c) ==
    /\ probeResult[c] = "unknown"
    /\ ~responseReceived[c]
    /\ responseReceived' = [responseReceived EXCEPT ![c] = TRUE]  \* Error returned
    /\ UNCHANGED <<leader, txidCaptured, commitSent, leaderWhenSent, probeResult,
                   walWritten, replicated, visibleOn>>

\* ============================================================================
\* SPECIFICATION
\* ============================================================================

Next ==
    \/ \E c \in Connections, t \in 1..MaxTxid : BeginTransaction(c, t)
    \/ \E c \in Connections : SendCommit(c)
    \/ \E c \in Connections : WriteToWAL(c)
    \/ \E t \in 1..MaxTxid : ReplicateToSync(t)
    \/ \E c \in Connections : ReceiveCommitResponse(c)
    \/ LeaderCrashes
    \/ \E n \in Nodes : ElectNewLeader(n)
    \/ \E c \in Connections : ProbeNewLeader(c)
    \/ \E c \in Connections : ReturnSyntheticResponse(c)
    \/ \E c \in Connections : GiveUpOnUnknown(c)

Spec == Init /\ [][Next]_vars

\* ============================================================================
\* SAFETY INVARIANTS
\* ============================================================================

(*
 * CRITICAL: If probe says "committed", transaction MUST be on new leader
 * This is the core safety property of commit probing
 *)
ProbeCommittedImpliesVisible ==
    \A c \in Connections :
        (probeResult[c] = "committed" /\ leader /= None)
        => IsVisibleOn(txidCaptured[c], leader)

(*
 * If probe says "aborted", transaction was never written to WAL
 *)
ProbeAbortedImpliesNotWritten ==
    \A c \in Connections :
        probeResult[c] = "aborted"
        => ~walWritten[txidCaptured[c]]

(*
 * If client got response (normal or synthetic), state is consistent
 *)
ResponseImpliesConsistentState ==
    \A c \in Connections :
        responseReceived[c] =>
            \/ probeResult[c] = "none"      \* Normal path - got response before crash
            \/ probeResult[c] = "committed" \* Probe confirmed commit
            \/ probeResult[c] = "aborted"   \* Probe confirmed abort
            \/ probeResult[c] = "unknown"   \* Error returned to client

(*
 * No data loss: If transaction was replicated, probe must find it
 * Guard: Only check when connection has an active transaction (txid /= 0)
 *)
ReplicatedImpliesProbeFindsIt ==
    \A c \in Connections :
        (txidCaptured[c] /= 0 /\ replicated[txidCaptured[c]] /\ probeResult[c] /= "none" /\ leader /= None)
        => (probeResult[c] = "committed" \/ IsVisibleOn(txidCaptured[c], leader))

\* ============================================================================
\* WHAT THIS PROVES
\* ============================================================================

(*
 * 1. ProbeCommittedImpliesVisible: No false positives
 *    - If we tell client "committed", the data is there
 *
 * 2. ProbeAbortedImpliesNotWritten: No false negatives for clean aborts
 *    - If we tell client "aborted", no data was written
 *
 * 3. ReplicatedImpliesProbeFindsIt: Sync replication guarantee
 *    - If sync replica had the data, new leader has it
 *
 * LIMITATION: "unknown" case requires client retry
 * This happens when: WAL written on old leader, not replicated, old leader lost
 * This is unavoidable - the data may or may not exist
 *)

================================================================================
