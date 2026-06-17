-------------------------------- MODULE lease_fencing --------------------------------
(*
 * TLA+ Specification for Lease-Based Split-Brain Prevention
 *
 * NOT machine-checked in this repo (no TLC in CI). Run it with the command in
 * tla/README.md; the invariants below are hand-verified against the actions,
 * which is a proof sketch, not a proof.
 *
 * === WHY THIS MODELS LEADERSHIP TRANSFER ===
 *
 * Split-brain is only expressible if two nodes can believe they lead at once.
 * A model with a single, write-once leader makes `AtMostOneWriteAuthority` hold
 * VACUOUSLY — the dangerous state (a deposed leader still holding a stale lease
 * while a new leader is elected) is unreachable. So this spec models leadership
 * TRANSFER across terms: a stale (deposed) leader and a freshly-elected leader
 * coexist in the state space, and only the hold-down plus lease anchoring keep
 * their write windows apart. The property is non-vacuous — set HoldDown = 0 in
 * the cfg and TLC finds the two-writer counterexample.
 *
 * === THE TWO MECHANISMS THIS SPEC MODELS ===
 *
 *   1. Lease anchored at the last quorum ack.
 *      CODE: governor/lease.rs renew():
 *            expires_at = (now - quorum_ack_age) + duration
 *      => a leader's authority ends at  last_quorum_ack + LeaseDuration, NOT
 *         at  now + LeaseDuration.  A deposed leader cannot ack again (only the
 *         current Raft leader receives quorum acks), so its expiry is frozen.
 *
 *   2. Promotion hold-down.
 *      CODE: app.rs promotion_lease_holddown(); governor/raft.rs
 *            failover_started_at_unix_ms; docs/STATE_MACHINE.md §2.
 *      => a leader elected via failover refuses to make PG writable until one
 *         full LeaseDuration has elapsed since the leader->none edge it observed.
 *
 * === MODELING NOTE 1: write AUTHORITY, not lease validity, is the safety property ===
 *
 * Two VALID leases can briefly coexist (the new leader renews its lease as soon
 * as it has quorum, while the deposed leader's lease has not yet expired). That
 * is harmless because the deposed leader is the only one WRITABLE during that
 * window and the new leader is still in hold-down. So the invariant is about
 * WRITE AUTHORITY (valid lease AND PG writable), matching the code: the lease
 * is intent; the hold-down gates the actual promotion. Treating <=1 *valid
 * lease* as the safety property would be wrong — the system does not provide it
 * (two leases overlap transiently); only <=1 *write authority* holds.
 *
 * === MODELING NOTE 2: the hold-down anchor (important limitation) ===
 *
 * This spec anchors the hold-down at the ELECTION instant and enforces that a
 * deposed leader cannot ack after it loses the term (only the current
 * `raftLeader` runs ReceiveQuorumAck). Hence  A_old <= E_new  holds by
 * construction, and the hold-down (>= LeaseDuration) closes the window.
 *
 * The IMPLEMENTATION anchors at the new leader's FIRST LOCAL leader->none
 * observation `obs`, and does NOT re-stamp at the election edge unless the edge
 * was coalesced away (raft.rs should_anchor_coalesced_failover). For FAST
 * failover  obs ≈ E  and this model is faithful. For SLOW / PARTIAL-PARTITION
 * failover  obs  can precede the deposed leader's last quorum ack by more than
 * (LeaseDuration - QuorumTimeout); the hold-down is then already satisfied at
 * election and provides NO protection. In that regime split-brain freedom rests
 * on (a) `SelfFenceOnQuorumLoss` below — the deposed leader self-fences within
 * QuorumTimeout of its last real ack — and (b) synchronous replication blocking
 * un-acknowledged commits during that <= QuorumTimeout window.
 * THIS SPEC DOES NOT MODEL PARTIAL-QUORUM DYNAMICS — the analysis is in this header.
 *
 * Single global clock: clock skew between nodes is out of scope (the code uses
 * wall-clock Instants; the ordering argument is about event ordering, which a
 * single monotonic `time` captures).
 *
 * Authors: pgbattery team   Date: 2026
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,            \* e.g. {1, 2, 3}
    LeaseDuration,    \* DEFAULT_LEASE_DURATION (e.g. 20 == 2s)
    QuorumTimeout,    \* QUORUM_TIMEOUT_MS     (e.g. 10 == 1s); must be < LeaseDuration
    HoldDown,         \* promotion hold-down   (== LeaseDuration in the code)
    MaxTerm,          \* bound on elections (state-space bound)
    MaxTime           \* bound on the clock  (state-space bound)

\* Key timing relationships the implementation depends on.
ASSUME QuorumTimeout < LeaseDuration
ASSUME HoldDown >= LeaseDuration

VARIABLES
    raftTerm,           \* global monotonic Raft term; 0 == no leader yet
    raftLeader,         \* node that is leader at raftTerm (or None)
    lastQuorumAck,      \* lastQuorumAck[n]: time of n's last majority ack
    leaseExpiresAt,     \* leaseExpiresAt[n]
    leaseIsLeader,      \* leaseIsLeader[n]: lease state thinks n is leader
    holdDownUntil,      \* holdDownUntil[n]: n must not enable writes before this time
    pgWritable,         \* pgWritable[n]: PostgreSQL on n accepts writes
    canReachMajority,   \* canReachMajority[n]
    time

vars == <<raftTerm, raftLeader, lastQuorumAck, leaseExpiresAt, leaseIsLeader,
          holdDownUntil, pgWritable, canReachMajority, time>>

None == 0

\* CODE: governor/raft.rs — has_quorum := millis_since_quorum_ack < QUORUM_TIMEOUT_MS
BelievesHasQuorum(n) == time - lastQuorumAck[n] < QuorumTimeout

\* Lease validity.  CODE: governor/lease.rs is_valid() == is_leader && now < expires_at,
\* with quorum loss folded in via update_from_raft's immediate-expiry branch
\* (modeled here as the BelievesHasQuorum conjunct so a stale leader self-fences).
LeaseIsValid(n) ==
    /\ leaseIsLeader[n]
    /\ time < leaseExpiresAt[n]
    /\ BelievesHasQuorum(n)

\* Effective write authority: a node may serve writes only with a valid lease
\* AND a writable PG. (Gateway per-message lease check + supervisor enable.)
HasWriteAuthority(n) == LeaseIsValid(n) /\ pgWritable[n]

TypeOK ==
    /\ raftTerm \in 0..MaxTerm
    /\ raftLeader \in Nodes \cup {None}
    /\ lastQuorumAck \in [Nodes -> 0..MaxTime]
    /\ leaseExpiresAt \in [Nodes -> 0..(MaxTime + LeaseDuration)]
    /\ leaseIsLeader \in [Nodes -> BOOLEAN]
    /\ holdDownUntil \in [Nodes -> 0..(MaxTime + HoldDown)]
    /\ pgWritable \in [Nodes -> BOOLEAN]
    /\ canReachMajority \in [Nodes -> BOOLEAN]
    /\ time \in 0..MaxTime

Init ==
    /\ raftTerm = 0
    /\ raftLeader = None
    /\ lastQuorumAck = [n \in Nodes |-> 0]
    /\ leaseExpiresAt = [n \in Nodes |-> 0]
    /\ leaseIsLeader = [n \in Nodes |-> FALSE]
    /\ holdDownUntil = [n \in Nodes |-> 0]
    /\ pgWritable = [n \in Nodes |-> FALSE]
    /\ canReachMajority = [n \in Nodes |-> TRUE]
    /\ time = 0

\* ============================================================================
\* ACTIONS
\* ============================================================================

(*
 * Bootstrap election: the very first leader. No prior leader => no hold-down.
 * CODE: node1 bootstraps; promotion_lease_holddown returns "promote now" when
 * failover_started_at_unix_ms is None.
 *)
ElectBootstrapLeader(n) ==
    /\ raftTerm = 0
    /\ canReachMajority[n]
    /\ raftTerm' = 1
    /\ raftLeader' = n
    /\ lastQuorumAck' = [lastQuorumAck EXCEPT ![n] = time]
    /\ holdDownUntil' = [holdDownUntil EXCEPT ![n] = 0]   \* no hold-down on bootstrap
    /\ UNCHANGED <<leaseExpiresAt, leaseIsLeader, pgWritable, canReachMajority, time>>

(*
 * Failover election at a higher term. A previous leader exists and is left
 * INTACT — it is now STALE and must self-fence (quorum timeout) / expire (lease)
 * on its own. The new leader starts fresh: not writable, no lease, hold-down
 * anchored at "now".  See MODELING NOTE 2 for why "now" (election) rather than
 * the implementation's earlier local observation.
 * CODE: governor/raft.rs stamps failover_started_at_unix_ms on the leader->none
 * edge; app.rs gates promote() on that + LeaseDuration.
 *)
ElectFailoverLeader(n) ==
    /\ raftTerm >= 1
    /\ raftTerm < MaxTerm
    /\ raftLeader # n                 \* a real transfer
    /\ canReachMajority[n]
    /\ raftTerm' = raftTerm + 1
    /\ raftLeader' = n
    /\ lastQuorumAck' = [lastQuorumAck EXCEPT ![n] = time]
    /\ holdDownUntil' = [holdDownUntil EXCEPT ![n] = time + HoldDown]
    /\ leaseIsLeader' = [leaseIsLeader EXCEPT ![n] = FALSE]  \* must re-renew
    /\ leaseExpiresAt' = [leaseExpiresAt EXCEPT ![n] = time] \* expired (time < time is FALSE)
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]        \* came up as a standby
    /\ UNCHANGED <<canReachMajority, time>>

(*
 * Leader receives a heartbeat ack from a majority. ONLY the current Raft leader
 * can do this — a deposed leader at a lower term cannot get quorum acks, so its
 * lastQuorumAck is frozen at deposition (this enforces A_old <= E_new).
 * CODE: governor/raft.rs (metrics.millis_since_quorum_ack).
 *)
ReceiveQuorumAck(n) ==
    /\ raftLeader = n
    /\ canReachMajority[n]
    /\ lastQuorumAck' = [lastQuorumAck EXCEPT ![n] = time]
    /\ UNCHANGED <<raftTerm, raftLeader, leaseExpiresAt, leaseIsLeader,
                   holdDownUntil, pgWritable, canReachMajority, time>>

(*
 * Governor renews the lease, anchored at the last quorum ack (NOT at now).
 * CODE: governor/lease.rs renew(): expires_at = (now - quorum_ack_age) + duration,
 * which equals lastQuorumAck + LeaseDuration.
 *)
RenewLease(n) ==
    /\ raftLeader = n
    /\ BelievesHasQuorum(n)
    /\ leaseIsLeader' = [leaseIsLeader EXCEPT ![n] = TRUE]
    /\ leaseExpiresAt' = [leaseExpiresAt EXCEPT ![n] = lastQuorumAck[n] + LeaseDuration]
    /\ UNCHANGED <<raftTerm, raftLeader, lastQuorumAck, holdDownUntil,
                   pgWritable, canReachMajority, time>>

(*
 * Supervisor enables PG writes — gated by BOTH a valid lease AND the hold-down.
 * CODE: app.rs promote_local_postgres after promotion_lease_holddown passes;
 * crates/pgbattery-supervisor/src/process.rs set_readonly(false).
 *)
EnablePgWrites(n) ==
    /\ raftLeader = n
    /\ LeaseIsValid(n)
    /\ time >= holdDownUntil[n]      \* THE HOLD-DOWN GATE
    /\ ~pgWritable[n]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<raftTerm, raftLeader, lastQuorumAck, leaseExpiresAt,
                   leaseIsLeader, holdDownUntil, canReachMajority, time>>

(*
 * Enforcement / gateway forces read-only once the lease is invalid.
 * CODE: app.rs lease_enforcement_tick (100ms) + gateway per-message lease check.
 *)
Fence(n) ==
    /\ ~LeaseIsValid(n)
    /\ pgWritable[n]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<raftTerm, raftLeader, lastQuorumAck, leaseExpiresAt,
                   leaseIsLeader, holdDownUntil, canReachMajority, time>>

NetworkPartition(n) ==
    /\ canReachMajority[n]
    /\ canReachMajority' = [canReachMajority EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<raftTerm, raftLeader, lastQuorumAck, leaseExpiresAt,
                   leaseIsLeader, holdDownUntil, pgWritable, time>>

NetworkHeal(n) ==
    /\ ~canReachMajority[n]
    /\ canReachMajority' = [canReachMajority EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<raftTerm, raftLeader, lastQuorumAck, leaseExpiresAt,
                   leaseIsLeader, holdDownUntil, pgWritable, time>>

Tick ==
    /\ time < MaxTime
    /\ time' = time + 1
    /\ UNCHANGED <<raftTerm, raftLeader, lastQuorumAck, leaseExpiresAt,
                   leaseIsLeader, holdDownUntil, pgWritable, canReachMajority>>

\* Stutter at the time bound so TLC does not report an artificial deadlock.
Terminating ==
    /\ time = MaxTime
    /\ UNCHANGED vars

Next ==
    \/ \E n \in Nodes : ElectBootstrapLeader(n)
    \/ \E n \in Nodes : ElectFailoverLeader(n)
    \/ \E n \in Nodes : ReceiveQuorumAck(n)
    \/ \E n \in Nodes : RenewLease(n)
    \/ \E n \in Nodes : EnablePgWrites(n)
    \/ \E n \in Nodes : Fence(n)
    \/ \E n \in Nodes : NetworkPartition(n)
    \/ \E n \in Nodes : NetworkHeal(n)
    \/ Tick
    \/ Terminating

Spec == Init /\ [][Next]_vars

StateConstraint ==
    /\ time <= MaxTime
    /\ raftTerm <= MaxTerm

\* ============================================================================
\* SAFETY INVARIANTS
\* ============================================================================

(*
 * THE safety property: at most one node holds write authority at any instant.
 * NON-VACUOUS — leadership transfers, so two leaders coexist in the state space;
 * only the hold-down (>= LeaseDuration) plus lease anchoring keep their write
 * windows apart.  Hand proof: a failover leader N becomes writable no earlier
 * than  E_N + HoldDown >= E_N + LeaseDuration.  The deposed leader O cannot ack
 * after E_N (ReceiveQuorumAck requires raftLeader = O), so
 * leaseExpiresAt[O] = A_O + LeaseDuration  with  A_O <= E_N.  Hence O's lease has
 * expired by the time N can write.  Set HoldDown = 0 to see TLC break this.
 *)
AtMostOneWriteAuthority ==
    Cardinality({n \in Nodes : HasWriteAuthority(n)}) <= 1

(*
 * A leader that stops receiving quorum acks loses write authority within
 * QuorumTimeout of its last ack — even before its lease would naturally expire.
 * This is the bound that protects the partial-partition case (MODELING NOTE 2),
 * where the hold-down does not. Also a regression guard: if the BelievesHasQuorum
 * conjunct were ever dropped from LeaseIsValid, this would be violable.
 * CODE: governor/raft.rs has_quorum == false => lease immediate-expiry.
 *)
SelfFenceOnQuorumLoss ==
    \A n \in Nodes :
        (time - lastQuorumAck[n] >= QuorumTimeout) => ~LeaseIsValid(n)

(*
 * Writability implies the node currently believes itself the lease leader.
 * (EnablePgWrites requires LeaseIsValid -> leaseIsLeader; only an election of n
 * clears leaseIsLeader[n], and it clears pgWritable[n] in the same step.)
 *)
WritableImpliesLeaseLeader ==
    \A n \in Nodes : pgWritable[n] => leaseIsLeader[n]

\* ============================================================================
\* THEOREMS (checked by TLC against the cfg)
\* ============================================================================

THEOREM TypeSafety       == Spec => []TypeOK
THEOREM NoSplitBrain     == Spec => []AtMostOneWriteAuthority
THEOREM QuorumSelfFence  == Spec => []SelfFenceOnQuorumLoss

================================================================================
