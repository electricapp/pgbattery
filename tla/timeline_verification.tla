---------------------------- MODULE timeline_verification ----------------------------
(*
 * TLA+ Specification for Timeline Transitions
 *
 * Machine-checked: `make -C tla check` (CI: .github/workflows/tla.yml).
 *
 * Models PostgreSQL timeline transitions during promotion and partition. The
 * implementation deliberately does NOT block promotion on timeline divergence
 * (pg_rewind reconciles it), so this spec checks the transition invariants that
 * DO hold, not a divergence-blocking property.
 *
 * Based on: crates/pgbattery-supervisor/src/process.rs verify_promotion_safe()
 *
 * Key Safety Property:
 *   A node cannot promote to primary if its timeline has diverged from
 *   the cluster's active timeline (detected via WAL receiver).
 *
 * === MAPPING TO IMPLEMENTATION ===
 *
 * TLA+ Variable → Rust Code:
 *   timelineID[n]         → pg_controldata: "Latest checkpoint's TimeLineID"
 *   walReceiverTimeline   → pg_wal_receiver_status(): timeline
 *   inRecovery[n]         → SELECT pg_is_in_recovery()
 *   isPrimary[n]          → NOT in_recovery
 *
 * Safety Check (observability, NOT a hard block — see note at end of header):
 *   TLA: receiverTimeline > localTimeline → logged, promotion proceeds
 *   Code: crates/pgbattery-supervisor/src/process.rs verify_promotion_safe()
 *         returns Err ONLY on operational failure (pg_controldata timeout /
 *         missing TimeLineID / parse error), which aborts promotion. Timeline
 *         divergence itself does NOT return Err and does NOT block — pg_rewind
 *         plus Raft authority handle it.
 *
 * SIMPLIFICATIONS:
 *
 * 1. TIMELINE QUERY:
 *    Spec: Instant read of timelineID
 *    Reality: Shell out to pg_controldata + parsing
 *    Gap: Could hang on NFS stale handles (no timeout originally)
 *    Fix: Added 10s timeout wrapper in crates/pgbattery-supervisor/src/process.rs
 *    CODE: crates/pgbattery-supervisor/src/process.rs
 *
 * 2. WAL RECEIVER QUERY:
 *    Spec: Instant read of walReceiverTimeline
 *    Reality: SQL query SELECT * FROM pg_wal_receiver_status()
 *    Gap: Could hang if PostgreSQL deadlocked
 *    Mitigation: PostgreSQL query timeout (statement_timeout)
 *    CODE: crates/pgbattery-supervisor/src/process.rs
 *
 * 3. PROMOTION ATOMICITY:
 *    Spec: Promotion is single atomic action
 *    Reality: verify → promote → wait_for_promotion (multi-step)
 *    Gap: Could crash between verify and promote
 *    Mitigation: Timeline verified on EVERY ExistingPrimary startup
 *    CODE: crates/pgbattery-supervisor/src/process.rs
 *
 * 4. TIMELINE INCREMENT:
 *    Spec: timeline++ on promotion
 *    Reality: PostgreSQL manages timelines, we read via pg_controldata
 *    Assumption: PostgreSQL timeline logic is correct
 *    Justification: PostgreSQL timeline handling is well-tested (20+ years)
 *
 * 5. NETWORK PARTITION MODELING:
 *    Spec: NetworkPartitionDiverge creates instant divergence
 *    Reality: Nodes must both promote in separate partitions
 *    Gap: Doesn't model full Raft election in each partition
 *    Justification: This spec focuses on detection, not how divergence occurs
 *
 * === SPEC vs IMPLEMENTATION NOTE ===
 *
 * This spec does NOT block promotion when walReceiverTimeline > localTimeline.
 *
 * WHY NOT BLOCK:
 *   After pg_rewind, a standby has its local timeline at the divergence point
 *   while receiving WAL from a higher timeline. This is EXPECTED and SAFE.
 *   Blocking would prevent recovery from ever completing.
 *
 * CODE: crates/pgbattery-supervisor/src/process.rs
 *   if receiver_info > timeline_id {
 *       tracing::info!("Timeline difference detected (expected after pg_rewind)");
 *       // Does NOT block - proceeds with promotion
 *   }
 *
 * SAFETY COMES FROM:
 *   1. pg_rewind synchronizes data before promotion
 *   2. Raft is authoritative for leadership decisions
 *   3. Synchronous replication prevents data loss
 *
 * WHAT THIS SPEC CHECKS: timelines stay in bounds (TypeOK) and never decrease
 *   (TimelineMonotonic) — a promotion only ever advances a node's timeline.
 * WHAT IT DOES NOT CLAIM: "no two primaries share a timeline" (false in the
 *   partition model — Raft provides single-primary, not the timeline number;
 *   see lease_fencing.tla) and the pg_rewind data-loss gate (pure logic,
 *   covered by the Rust unit tests for rewind_divergence_decision).
 *
 * Authors: pgbattery team
 * Date: 2026
 *)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,                  \* Set of node IDs
    MaxTimeline            \* Maximum timeline to explore (e.g., 5)

VARIABLES
    (* PostgreSQL state *)
    timelineID,            \* timelineID[n] = current timeline of node n
    inRecovery,            \* inRecovery[n] = is node in recovery (standby) mode?
    walReceiverTimeline,   \* walReceiverTimeline[n] = timeline from WAL stream (if standby)

    (* Promotion state *)
    isPrimary,             \* isPrimary[n] = is node currently primary?
    promotionAttempted     \* promotionAttempted[n] = did node try to promote?

vars == <<timelineID, inRecovery, walReceiverTimeline,
          isPrimary, promotionAttempted>>

\* ============================================================================
\* TYPE INVARIANTS
\* ============================================================================

TypeOK ==
    /\ timelineID \in [Nodes -> 0..MaxTimeline]
    /\ inRecovery \in [Nodes -> BOOLEAN]
    /\ walReceiverTimeline \in [Nodes -> 0..MaxTimeline]
    /\ isPrimary \in [Nodes -> BOOLEAN]
    /\ promotionAttempted \in [Nodes -> BOOLEAN]

\* ============================================================================
\* INITIAL STATE
\* ============================================================================

Init ==
    /\ timelineID = [n \in Nodes |-> 1]  \* All start on timeline 1
    /\ inRecovery = [n \in Nodes |-> n /= 1]  \* Node 1 is primary initially
    /\ walReceiverTimeline = [n \in Nodes |-> IF n = 1 THEN 0 ELSE 1]
    /\ isPrimary = [n \in Nodes |-> n = 1]
    /\ promotionAttempted = [n \in Nodes |-> FALSE]

\* ============================================================================
\* ACTIONS
\* ============================================================================

(* Node promotes from standby to primary *)
AttemptPromotion(n) ==
    /\ inRecovery[n]  \* Must be standby
    /\ ~isPrimary[n]  \* Not already primary
    /\ promotionAttempted' = [promotionAttempted EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<timelineID, inRecovery, walReceiverTimeline, isPrimary>>

(* Timeline verification check
 *
 * IMPORTANT: This check is for OBSERVABILITY, not blocking.
 * After pg_rewind, local timeline is at divergence point while receiving
 * from higher timeline - this is EXPECTED and SAFE.
 *
 * The real safety comes from:
 * 1. pg_rewind synchronizing data before promotion
 * 2. Raft being authoritative for leadership decisions
 *
 * We log timeline differences but proceed with promotion.
 *)
VerifyTimelineSafety(n) ==
    /\ promotionAttempted[n]
    /\ inRecovery[n]  \* Must still be standby (not yet promoted)
    /\ timelineID[n] < MaxTimeline  \* bound so TypeOK holds and is checkable
    /\ LET localTimeline == timelineID[n]
           receiverTimeline == walReceiverTimeline[n]
       IN
           \* NOTE: We do NOT block on receiverTimeline > localTimeline
           \* This is expected after pg_rewind. Blocking would break recovery.
           \* Timeline safe - allow promotion, demote any existing primary
           /\ inRecovery' = [m \in Nodes |->
                IF m = n THEN FALSE
                ELSE IF isPrimary[m] THEN TRUE  \* Demote old primary
                ELSE inRecovery[m]]
           /\ isPrimary' = [m \in Nodes |->
                IF m = n THEN TRUE
                ELSE FALSE]  \* Only new node is primary
           /\ timelineID' = [timelineID EXCEPT ![n] = localTimeline + 1]
           \* Clear promotionAttempted after successful promotion
           /\ promotionAttempted' = [promotionAttempted EXCEPT ![n] = FALSE]
           /\ UNCHANGED <<walReceiverTimeline>>

(* Network partition causes timeline divergence
 * Even in a partition, a standby can only promote if its timeline is not stale
 * relative to what it knows (walReceiverTimeline).
 *)
NetworkPartitionDiverge(n1, n2) ==
    /\ isPrimary[n1]
    /\ inRecovery[n2]
    /\ walReceiverTimeline[n2] = timelineID[n1]  \* n2 was following n1
    /\ walReceiverTimeline[n2] <= timelineID[n2]  \* Timeline safety check
    /\ timelineID[n2] < MaxTimeline  \* bound so TypeOK holds and is checkable
    /\ inRecovery' = [inRecovery EXCEPT ![n2] = FALSE]  \* n2 promoted in partition
    /\ isPrimary' = [isPrimary EXCEPT ![n2] = TRUE]
    /\ timelineID' = [timelineID EXCEPT ![n2] = timelineID[n2] + 1]
    /\ UNCHANGED <<walReceiverTimeline, promotionAttempted>>

(*
 * After partition heals, standbys reconnect and see the current primary's timeline.
 * This is how timeline divergence gets detected - the standby's WAL receiver
 * reports a timeline higher than what the standby has locally.
 *
 * NOTE: walReceiverTimeline is monotonic - once we see a higher timeline,
 * we can't "unsee" it. This models that a standby remembers timeline history.
 *)
PropagateTimeline(primary, standby) ==
    /\ isPrimary[primary]
    /\ inRecovery[standby]
    /\ standby /= primary
    /\ timelineID[primary] > walReceiverTimeline[standby]  \* Only update if higher
    \* Standby reconnects to primary and sees its timeline
    /\ walReceiverTimeline' = [walReceiverTimeline EXCEPT ![standby] = timelineID[primary]]
    /\ UNCHANGED <<timelineID, inRecovery, isPrimary, promotionAttempted>>

\* ============================================================================
\* SPECIFICATION
\* ============================================================================

(* Termination: Allow stuttering when no actions are possible *)
Terminating ==
    /\ \A n \in Nodes : ~inRecovery[n]  \* All nodes promoted
    /\ UNCHANGED vars

Next ==
    \/ \E n \in Nodes : AttemptPromotion(n)
    \/ \E n \in Nodes : VerifyTimelineSafety(n)
    \/ \E n1, n2 \in Nodes : n1 /= n2 /\ NetworkPartitionDiverge(n1, n2)
    \/ \E p, s \in Nodes : p /= s /\ PropagateTimeline(p, s)
    \/ Terminating

Spec == Init /\ [][Next]_vars

\* State constraint to bound state space
StateConstraint ==
    /\ \A n \in Nodes : timelineID[n] <= MaxTimeline

\* ============================================================================
\* PROPERTIES
\* ============================================================================

(* A node's PostgreSQL timeline never decreases — every promotion (normal or
 * in-partition) strictly increments it, and nothing lowers it. This is the
 * real, checkable guarantee of the timeline model. *)
TimelineMonotonic ==
    [][ \A n \in Nodes : timelineID'[n] >= timelineID[n] ]_vars

(* NOT an invariant — kept to document why timeline numbers do NOT provide
 * single-primary safety. Two nodes promoting in separate partitions can reach
 * the same timeline number on different forks, so this predicate is FALSE in
 * the partition model and is left unchecked. Single-primary safety is Raft's
 * (see lease_fencing.tla); the pg_rewind data-loss gate is pure logic covered
 * by the Rust unit tests for rewind_divergence_decision. *)
NoDualPrimariesSameTimeline ==
    ~\E n1, n2 \in Nodes :
        /\ n1 /= n2
        /\ isPrimary[n1]
        /\ isPrimary[n2]
        /\ timelineID[n1] = timelineID[n2]

================================================================================
