-------------------------------- MODULE lease_fencing --------------------------------
(*
 * TLA+ Specification for Lease-Based Split-Brain Prevention
 *
 * Machine-checked: `make -C tla check` (CI: .github/workflows/tla.yml).
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
 * === RELATIVE TIME (no global clock — this is what keeps TLC tractable) ===
 *
 * Modeling an absolute clock plus absolute timestamps makes the state space
 * explode: the clock grows without bound and every timestamp variable multiplies
 * it. Instead, time is RELATIVE — each node tracks bounded countdowns/ages that
 * a single global `Tick` advances, each capped by a small constant:
 *   quorumAckAge[n]      0..QuorumTimeout  ticks since n's last quorum ack (capped)
 *   leaseRemaining[n]    0..LeaseDuration  lease time left (0 = expired)
 *   holdDownRemaining[n] 0..HoldDown       promotion hold-down left (0 = cleared)
 * Every variable is bounded, so the reachable state space is small and finite
 * regardless of how long a scenario runs.
 *
 * === THE TWO MECHANISMS THIS SPEC MODELS ===
 *
 *   1. Lease anchored at the last quorum ack.
 *      CODE: governor/lease.rs renew(): expires_at = (now - quorum_ack_age) + duration.
 *      Here: RenewLease sets leaseRemaining = LeaseDuration - quorumAckAge, so a
 *      leader whose last ack is already stale gets LESS than a full duration — its
 *      authority still ends LeaseDuration after the real ack, not after `now`. A
 *      deposed leader cannot ack or renew, so its leaseRemaining only counts down.
 *
 *   2. Promotion hold-down.
 *      CODE: app.rs promotion_lease_holddown(); governor/raft.rs
 *      failover_started_at_unix_ms; docs/STATE_MACHINE.md §2.
 *      Here: ElectFailoverLeader sets holdDownRemaining = HoldDown, and
 *      EnablePgWrites is gated on holdDownRemaining = 0. With HoldDown >=
 *      LeaseDuration, the new leader cannot become writable until any prior
 *      leader's lease (<= LeaseDuration remaining at deposition) has counted out.
 *
 * === MODELING NOTE 1: write AUTHORITY, not lease validity, is the safety property ===
 *
 * Two VALID leases can briefly coexist (the new leader renews as soon as it has
 * quorum, while the deposed leader's lease has not yet expired). That is harmless
 * because the deposed leader is the only one WRITABLE during that window and the
 * new leader is still in hold-down. So the invariant is about WRITE AUTHORITY
 * (valid lease AND PG writable), matching the code: the lease is intent; the
 * hold-down gates the actual promotion. Treating <=1 *valid lease* as the safety
 * property would be wrong — the system does not provide it (two leases overlap
 * transiently); only <=1 *write authority* holds.
 *
 * === MODELING NOTE 2: the hold-down anchor (important limitation) ===
 *
 * This spec gives every failover leader a full HoldDown countdown and lets a
 * deposed leader keep at most LeaseDuration of lease, so the hold-down always
 * outlasts the stale lease. The IMPLEMENTATION anchors the hold-down at the new
 * leader's FIRST LOCAL leader->none observation, not at election. For FAST
 * failover these coincide. For SLOW / PARTIAL-PARTITION failover the local
 * observation can precede the deposed leader's last quorum ack by more than
 * (LeaseDuration - QuorumTimeout); the hold-down is then already satisfied at
 * election and adds no protection. Split-brain freedom there rests on (a)
 * SelfFenceOnQuorumLoss below — the deposed leader self-fences within
 * QuorumTimeout of its last real ack — and (b) synchronous replication blocking
 * un-acknowledged commits during that <= QuorumTimeout window. THIS SPEC DOES
 * NOT MODEL PARTIAL-QUORUM DYNAMICS — the analysis is in this header.
 *
 * Authors: pgbattery team   Date: 2026
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,            \* e.g. {1, 2, 3}
    LeaseDuration,    \* DEFAULT_LEASE_DURATION, in ticks
    QuorumTimeout,    \* QUORUM_TIMEOUT_MS, in ticks; must be < LeaseDuration
    HoldDown,         \* promotion hold-down, in ticks; == LeaseDuration in the code
    MaxTerm           \* bound on elections (state-space bound)

\* Key timing relationships the implementation depends on.
ASSUME QuorumTimeout < LeaseDuration
ASSUME HoldDown >= LeaseDuration

VARIABLES
    raftTerm,           \* global monotonic Raft term; 0 = no leader yet
    raftLeader,         \* node that is leader at raftTerm (or None)
    quorumAckAge,       \* quorumAckAge[n]: ticks since n's last majority ack (capped)
    leaseRemaining,     \* leaseRemaining[n]: lease ticks left (0 = expired)
    holdDownRemaining,  \* holdDownRemaining[n]: hold-down ticks left (0 = cleared)
    leaseIsLeader,      \* leaseIsLeader[n]: lease state thinks n is leader
    pgWritable          \* pgWritable[n]: PostgreSQL on n accepts writes

vars == <<raftTerm, raftLeader, quorumAckAge, leaseRemaining, holdDownRemaining,
          leaseIsLeader, pgWritable>>

None == 0

Dec(x) == IF x > 0 THEN x - 1 ELSE 0

\* CODE: governor/raft.rs — has_quorum := millis_since_quorum_ack < QUORUM_TIMEOUT_MS
BelievesHasQuorum(n) == quorumAckAge[n] < QuorumTimeout

\* Lease validity. CODE: governor/lease.rs is_valid() plus the quorum-loss
\* immediate-expiry branch (modeled as the BelievesHasQuorum conjunct).
LeaseIsValid(n) ==
    /\ leaseIsLeader[n]
    /\ leaseRemaining[n] > 0
    /\ BelievesHasQuorum(n)

\* A node may serve writes only with a valid lease AND a writable PG.
HasWriteAuthority(n) == LeaseIsValid(n) /\ pgWritable[n]

TypeOK ==
    /\ raftTerm \in 0..MaxTerm
    /\ raftLeader \in Nodes \cup {None}
    /\ quorumAckAge \in [Nodes -> 0..QuorumTimeout]
    /\ leaseRemaining \in [Nodes -> 0..LeaseDuration]
    /\ holdDownRemaining \in [Nodes -> 0..HoldDown]
    /\ leaseIsLeader \in [Nodes -> BOOLEAN]
    /\ pgWritable \in [Nodes -> BOOLEAN]

Init ==
    /\ raftTerm = 0
    /\ raftLeader = None
    /\ quorumAckAge = [n \in Nodes |-> QuorumTimeout]   \* no acks yet => no quorum
    /\ leaseRemaining = [n \in Nodes |-> 0]
    /\ holdDownRemaining = [n \in Nodes |-> 0]
    /\ leaseIsLeader = [n \in Nodes |-> FALSE]
    /\ pgWritable = [n \in Nodes |-> FALSE]

\* ============================================================================
\* ACTIONS
\* ============================================================================

(* One global tick of logical time: ages advance, countdowns count down. Always
 * enabled (so the model never deadlocks); a tick that changes nothing is an
 * inert self-loop TLC simply does not re-expand. *)
Tick ==
    /\ quorumAckAge'      = [n \in Nodes |-> IF quorumAckAge[n] < QuorumTimeout
                                             THEN quorumAckAge[n] + 1 ELSE QuorumTimeout]
    /\ leaseRemaining'    = [n \in Nodes |-> Dec(leaseRemaining[n])]
    /\ holdDownRemaining' = [n \in Nodes |-> Dec(holdDownRemaining[n])]
    /\ UNCHANGED <<raftTerm, raftLeader, leaseIsLeader, pgWritable>>

(* Bootstrap election: the first leader; no prior leader => no hold-down. *)
ElectBootstrapLeader(n) ==
    /\ raftTerm = 0
    /\ raftTerm' = 1
    /\ raftLeader' = n
    /\ quorumAckAge' = [quorumAckAge EXCEPT ![n] = 0]
    /\ holdDownRemaining' = [holdDownRemaining EXCEPT ![n] = 0]
    /\ UNCHANGED <<leaseRemaining, leaseIsLeader, pgWritable>>

(* Failover election at a higher term. The previous leader is left INTACT — now
 * STALE — and must self-fence (quorum) / expire (lease) as its countdowns drain.
 * The new leader starts fresh: no lease, not writable, full hold-down. *)
ElectFailoverLeader(n) ==
    /\ raftTerm >= 1
    /\ raftTerm < MaxTerm
    /\ raftLeader /= n
    /\ raftTerm' = raftTerm + 1
    /\ raftLeader' = n
    /\ quorumAckAge' = [quorumAckAge EXCEPT ![n] = 0]
    /\ holdDownRemaining' = [holdDownRemaining EXCEPT ![n] = HoldDown]
    /\ leaseRemaining' = [leaseRemaining EXCEPT ![n] = 0]
    /\ leaseIsLeader' = [leaseIsLeader EXCEPT ![n] = FALSE]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]

(* Leader receives a majority heartbeat ack. ONLY the current Raft leader can —
 * a deposed leader cannot, so its quorumAckAge only grows (enforces that a
 * deposed leader self-fences). Optional each round; not taking it for
 * QuorumTimeout ticks models a partition. *)
ReceiveQuorumAck(n) ==
    /\ raftLeader = n
    /\ quorumAckAge' = [quorumAckAge EXCEPT ![n] = 0]
    /\ UNCHANGED <<raftTerm, raftLeader, leaseRemaining, holdDownRemaining,
                   leaseIsLeader, pgWritable>>

(* Renew the lease, anchored at the last quorum ack: remaining = duration minus
 * how stale that ack already is. CODE: governor/lease.rs renew(). *)
RenewLease(n) ==
    /\ raftLeader = n
    /\ BelievesHasQuorum(n)
    /\ leaseIsLeader' = [leaseIsLeader EXCEPT ![n] = TRUE]
    /\ leaseRemaining' = [leaseRemaining EXCEPT ![n] = LeaseDuration - quorumAckAge[n]]
    /\ UNCHANGED <<raftTerm, raftLeader, quorumAckAge, holdDownRemaining, pgWritable>>

(* Supervisor enables PG writes — gated by BOTH a valid lease AND the hold-down. *)
EnablePgWrites(n) ==
    /\ raftLeader = n
    /\ LeaseIsValid(n)
    /\ holdDownRemaining[n] = 0
    /\ ~pgWritable[n]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<raftTerm, raftLeader, quorumAckAge, leaseRemaining,
                   holdDownRemaining, leaseIsLeader>>

(* Enforcement / gateway forces read-only once the lease is invalid. *)
Fence(n) ==
    /\ ~LeaseIsValid(n)
    /\ pgWritable[n]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<raftTerm, raftLeader, quorumAckAge, leaseRemaining,
                   holdDownRemaining, leaseIsLeader>>

Next ==
    \/ \E n \in Nodes : ElectBootstrapLeader(n)
    \/ \E n \in Nodes : ElectFailoverLeader(n)
    \/ \E n \in Nodes : ReceiveQuorumAck(n)
    \/ \E n \in Nodes : RenewLease(n)
    \/ \E n \in Nodes : EnablePgWrites(n)
    \/ \E n \in Nodes : Fence(n)
    \/ Tick

Spec == Init /\ [][Next]_vars

\* ============================================================================
\* SAFETY INVARIANTS
\* ============================================================================

(*
 * THE safety property: at most one node holds write authority at any instant.
 * NON-VACUOUS — leadership transfers, so two leaders coexist in the state space;
 * only the hold-down (>= LeaseDuration) plus lease anchoring keep their write
 * windows apart. Proof sketch: a failover leader N can write only after HoldDown
 * ticks (holdDownRemaining counts HoldDown -> 0). A deposed leader O cannot ack
 * or renew, so at deposition leaseRemaining[O] <= LeaseDuration and only counts
 * down; after HoldDown >= LeaseDuration ticks it is 0, so O's lease is invalid
 * before N can write. Set HoldDown = 0 in the cfg to watch TLC break this.
 *)
AtMostOneWriteAuthority ==
    Cardinality({n \in Nodes : HasWriteAuthority(n)}) <= 1

(*
 * A leader that stops receiving quorum acks loses write authority within
 * QuorumTimeout of its last ack — even before its lease counts out. This is the
 * bound that protects the partial-partition case (MODELING NOTE 2). Also a
 * regression guard on LeaseIsValid keeping the BelievesHasQuorum conjunct.
 *)
SelfFenceOnQuorumLoss ==
    \A n \in Nodes : (quorumAckAge[n] >= QuorumTimeout) => ~LeaseIsValid(n)

(* Writability implies the node currently believes itself the lease leader. *)
WritableImpliesLeaseLeader ==
    \A n \in Nodes : pgWritable[n] => leaseIsLeader[n]

\* ============================================================================
\* THEOREMS (checked by TLC against the cfg)
\* ============================================================================

THEOREM TypeSafety      == Spec => []TypeOK
THEOREM NoSplitBrain    == Spec => []AtMostOneWriteAuthority
THEOREM QuorumSelfFence == Spec => []SelfFenceOnQuorumLoss

================================================================================
