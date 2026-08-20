-------------------------------- MODULE lease_fencing --------------------------------
(*
 * TLA+ Specification for Lease-Based Split-Brain Prevention
 *
 * Machine-checked: `make -C tla check` (CI: .github/workflows/tla.yml).
 * The counterexample this spec exists to produce is checked too, by
 * `make -C tla check-lease_fencing-counterexample`.
 *
 * === WHY THIS MODELS LEADERSHIP TRANSFER ===
 *
 * Split-brain is only expressible if two nodes can believe they lead at once.
 * A model with a single, write-once leader makes `AtMostOneWriteAuthority` hold
 * VACUOUSLY — the dangerous state (a deposed leader still holding a stale lease
 * while a new leader is elected) is unreachable. So this spec models leadership
 * TRANSFER across terms: a stale (deposed) leader and a freshly-elected leader
 * coexist in the state space, and only the hold-down plus lease anchoring keep
 * their write windows apart.
 *
 * === QUORUM IS A SET OF NODES, NOT A COUNTER ===
 *
 * Elections and heartbeat acks both name the majority `Q` that participated,
 * and a node's term is what decides whether it can be in one. That makes the
 * regime of RW-1 — a deposed leader holding a quorum that excludes the election
 * winner — a reachable state rather than an assumption, and makes quorum
 * intersection a DERIVED property: after `WinElection(N, Q)` every node in `Q`
 * sits at N's term, so any majority the old leader could still ack contains one
 * of them and is refused. Election safety (one leader per term) falls out the
 * same way, from voters granting only strictly-higher terms.
 *
 * The old leader is left running and unaware. It is not in the winner's quorum,
 * nothing tells it the term moved, and it keeps acking whatever majority it can
 * still reach until the winner's votes land. That window is the whole subject
 * of this spec.
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
 *   campaignAge[n]       0..VoteDelay+1    ticks n's current candidacy has run
 *
 * campaignAge caps one PAST VoteDelay on purpose: a counter capped AT its bound
 * makes "the votes arrived in time" unfalsifiable, since the age can never
 * exceed what the guard compares it to. VoteDelay+1 is the state that means
 * "too late", and it is what forces a stalled candidacy to be abandoned and
 * retried at a higher term.
 * Every variable is bounded, so the reachable state space is small and finite
 * regardless of how long a scenario runs.
 *
 * === THE THREE MECHANISMS THIS SPEC MODELS ===
 *
 *   1. Lease anchored at the last quorum ack.
 *      CODE: governor/lease.rs renew(): expires_at = (now - quorum_ack_age) + duration.
 *      Here: RenewLease sets leaseRemaining = LeaseDuration - quorumAckAge, so a
 *      leader whose last ack is already stale gets LESS than a full duration — its
 *      authority still ends LeaseDuration after the real ack, not after `now`.
 *
 *   2. Quorum-loss self-fence.
 *      CODE: governor/raft.rs has_quorum := millis_since_quorum_ack < QUORUM_TIMEOUT_MS,
 *      feeding LeaseState::update_from_raft.
 *      Here: LeaseIsValid conjoins BelievesHasQuorum. Since QuorumTimeout <
 *      LeaseDuration this is the tighter of the two bounds, and it is the one
 *      that actually ends a deposed leader's authority.
 *
 *   3. Promotion hold-down, anchored on the newest term this node has seen.
 *      CODE: app.rs promotion_lease_holddown(); governor/raft.rs
 *      should_anchor_term_advance and failover_started_at; docs/STATE_MACHINE.md §2.
 *      Here: StartCampaign re-stamps holdDownRemaining = HoldDown, and
 *      EnablePgWrites is gated on holdDownRemaining = 0 (or on there being no
 *      anchor at all, which is what `promotion_lease_holddown(None, ..)` means).
 *
 * === WHY THE ANCHOR MOVES WITH THE TERM (mechanism 3, in full) ===
 *
 * The bound a hold-down needs is "when could the deposed leader last have had
 * quorum", and that instant is the one a majority moved past its term — the
 * vote the winner is campaigning for. It is NOT the instant the winner first
 * noticed the leader gone: an election that splits, is dropped, or is retried by
 * the leaderless watchdog can win arbitrarily later, and an anchor stamped at
 * the first observation has already counted out by then. The winner would
 * promote the moment it won, while the old leader's last ack was one message
 * flight ago and its self-fence is still QuorumTimeout away. That is a real L1
 * violation, not a modeling artifact, and it is what
 * `lease_fencing.inv-anchor-not-restamped.cfg` makes TLC produce.
 *
 * Re-stamping on every term advance costs nothing in a prompt failover, where
 * the candidacy and the leader→none edge are the same instant. It costs one
 * hold-down per extra election round in exactly the case that needs it.
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
 * === MODELING NOTE 2: what a voter learns, it learns at once ===
 *
 * A node that hears a higher term adopts it, stops believing it leads, drops its
 * lease and goes read-only in the same step. The implementation does all four
 * from one metrics handler in that documented order, but PostgreSQL's read-only
 * GUC lands a moment later, so the model is optimistic by that moment. It is not
 * optimistic where it matters, for two reasons. The dangerous expiries are the
 * ones nobody drives — a lease timing out, a quorum ack ageing past
 * QuorumTimeout — and those still leave PG writable until the separate `Fence`
 * action fires. And the deposed leader OUTSIDE the winner's quorum hears nothing
 * at all until it happens to (StepDown), which is the node the whole safety
 * argument turns on.
 *
 * === MODELING NOTE 3: what is still abstracted ===
 *
 * The Raft log is not modeled, so nothing here says the winner's data is
 * current — that is raft_lsn.tla's subject. Message loss is modeled only as
 * "the candidacy did not win in time" (AbandonCampaign), not per-message.
 * VoteDelay is the network bound the safety argument needs: a vote that takes
 * longer than LeaseDuration - QuorumTimeout to reach a majority breaks the
 * inequality below, and no lease scheme can bound that without bounding
 * message delay.
 *
 * Authors: pgbattery team   Date: 2026
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,            \* e.g. {1, 2, 3}
    LeaseDuration,    \* DEFAULT_LEASE_DURATION, in ticks
    QuorumTimeout,    \* QUORUM_TIMEOUT_MS, in ticks; must be < LeaseDuration
    HoldDown,         \* promotion hold-down, in ticks; == LeaseDuration in the code
    VoteDelay,        \* max ticks from a candidacy starting to a majority granting it
    MaxTerm,          \* bound on elections (state-space bound)
    RestampAnchorOnCampaign  \* models should_anchor_term_advance; FALSE is the defect

\* Key timing relationships the implementation depends on.
ASSUME QuorumTimeout < LeaseDuration
ASSUME HoldDown >= LeaseDuration

\* THE load-bearing inequality. The winner is writable HoldDown after its
\* winning candidacy started; the deposed leader's last possible ack is
\* VoteDelay after that same instant, and it self-fences QuorumTimeout later.
\* Guarded on the re-stamp because without it the anchor is not the candidacy
\* instant at all, and the counterexample config would be rejected before TLC
\* could produce the counterexample.
ASSUME RestampAnchorOnCampaign => (HoldDown >= VoteDelay + QuorumTimeout)

VARIABLES
    nodeTerm,           \* nodeTerm[n]: the term n is in (0 = never in one)
    believesLeader,     \* believesLeader[n]: n's own RaftMetrics say n leads
    campaigning,        \* campaigning[n]: n has an outstanding candidacy
    campaignAge,        \* campaignAge[n]: ticks that candidacy has run
    bootstrapped,       \* the cluster has been initialized (Raft::initialize)
    quorumAckAge,       \* quorumAckAge[n]: ticks since n's last majority ack (capped)
    leaseRemaining,     \* leaseRemaining[n]: lease ticks left (0 = expired)
    holdDownRemaining,  \* holdDownRemaining[n]: hold-down ticks left (0 = cleared)
    anchored,           \* anchored[n]: failover_started_at is stamped
    leaseIsLeader,      \* leaseIsLeader[n]: lease state thinks n is leader
    pgWritable          \* pgWritable[n]: PostgreSQL on n accepts writes

vars == <<nodeTerm, believesLeader, campaigning, campaignAge, bootstrapped,
          quorumAckAge, leaseRemaining, holdDownRemaining, anchored,
          leaseIsLeader, pgWritable>>

Dec(x) == IF x > 0 THEN x - 1 ELSE 0

Majorities == {Q \in SUBSET Nodes : Cardinality(Q) * 2 > Cardinality(Nodes)}

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

\* CODE: app.rs promotion_lease_holddown() — `None` (no anchor) means no
\* hold-down at all, which is why the anchor's presence is half of this gate.
HoldDownSatisfied(n) == ~anchored[n] \/ holdDownRemaining[n] = 0

TypeOK ==
    /\ nodeTerm \in [Nodes -> 0..MaxTerm]
    /\ believesLeader \in [Nodes -> BOOLEAN]
    /\ campaigning \in [Nodes -> BOOLEAN]
    /\ campaignAge \in [Nodes -> 0..VoteDelay + 1]
    /\ bootstrapped \in BOOLEAN
    /\ quorumAckAge \in [Nodes -> 0..QuorumTimeout]
    /\ leaseRemaining \in [Nodes -> 0..LeaseDuration]
    /\ holdDownRemaining \in [Nodes -> 0..HoldDown]
    /\ anchored \in [Nodes -> BOOLEAN]
    /\ leaseIsLeader \in [Nodes -> BOOLEAN]
    /\ pgWritable \in [Nodes -> BOOLEAN]

Init ==
    /\ nodeTerm = [n \in Nodes |-> 0]
    /\ believesLeader = [n \in Nodes |-> FALSE]
    /\ campaigning = [n \in Nodes |-> FALSE]
    /\ campaignAge = [n \in Nodes |-> 0]
    /\ bootstrapped = FALSE
    /\ quorumAckAge = [n \in Nodes |-> QuorumTimeout]   \* no acks yet => no quorum
    /\ leaseRemaining = [n \in Nodes |-> 0]
    /\ holdDownRemaining = [n \in Nodes |-> 0]
    /\ anchored = [n \in Nodes |-> FALSE]
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
    /\ campaignAge'       = [n \in Nodes |-> IF campaigning[n] /\ campaignAge[n] <= VoteDelay
                                             THEN campaignAge[n] + 1 ELSE campaignAge[n]]
    /\ UNCHANGED <<nodeTerm, believesLeader, campaigning, bootstrapped, anchored,
                   leaseIsLeader, pgWritable>>

(* Cluster creation: `Raft::initialize`, not an election — no campaign, no prior
 * term, so nothing to hold down against. CODE: app.rs bootstrap path. *)
Bootstrap(n, Q) ==
    /\ ~bootstrapped
    /\ Q \in Majorities
    /\ n \in Q
    /\ \A m \in Q : nodeTerm[m] = 0
    /\ bootstrapped' = TRUE
    /\ nodeTerm' = [m \in Nodes |-> IF m \in Q THEN 1 ELSE nodeTerm[m]]
    /\ believesLeader' = [believesLeader EXCEPT ![n] = TRUE]
    /\ quorumAckAge' = [quorumAckAge EXCEPT ![n] = 0]
    /\ UNCHANGED <<campaigning, campaignAge, leaseRemaining, holdDownRemaining,
                   anchored, leaseIsLeader, pgWritable>>

(* n's election timer fires: it bumps its term and solicits votes. openraft
 * clears `current_leader` on the vote change, so this IS the leader→none edge
 * the anchor is stamped on — and every later retry is another one of these.
 * CODE: governor/raft.rs should_anchor_term_advance. *)
StartCampaign(n) ==
    /\ bootstrapped
    /\ ~believesLeader[n]
    /\ ~campaigning[n]
    /\ nodeTerm[n] < MaxTerm
    /\ nodeTerm' = [nodeTerm EXCEPT ![n] = nodeTerm[n] + 1]
    /\ campaigning' = [campaigning EXCEPT ![n] = TRUE]
    /\ campaignAge' = [campaignAge EXCEPT ![n] = 0]
    /\ anchored' = [anchored EXCEPT ![n] = TRUE]
    /\ holdDownRemaining' = [holdDownRemaining EXCEPT ![n] =
           IF RestampAnchorOnCampaign \/ ~anchored[n] THEN HoldDown ELSE holdDownRemaining[n]]
    /\ UNCHANGED <<believesLeader, bootstrapped, quorumAckAge, leaseRemaining,
                   leaseIsLeader, pgWritable>>

(* The candidacy did not reach a majority in time — a split vote, a dropped
 * RequestVote, or the LSN vote-gate refusing it. n is free to campaign again at
 * a higher term. *)
AbandonCampaign(n) ==
    /\ campaigning[n]
    /\ campaignAge[n] > VoteDelay
    /\ campaigning' = [campaigning EXCEPT ![n] = FALSE]
    /\ campaignAge' = [campaignAge EXCEPT ![n] = 0]
    /\ UNCHANGED <<nodeTerm, believesLeader, bootstrapped, quorumAckAge,
                   leaseRemaining, holdDownRemaining, anchored, leaseIsLeader,
                   pgWritable>>

(* A majority grants the candidacy. Voters grant only a STRICTLY higher term,
 * which is what makes at most one leader per term derivable rather than
 * assumed. Any leader among them steps down. A prior leader OUTSIDE Q is left
 * intact and unaware — it must self-fence as its own countdowns drain.
 *
 * The winner keeps the anchor it stamped when it began this candidacy; a winner
 * with no anchor (its leader→none edge was coalesced away) stamps one now.
 * CODE: governor/raft.rs should_anchor_coalesced_failover. *)
WinElection(n, Q) ==
    /\ campaigning[n]
    /\ campaignAge[n] <= VoteDelay
    /\ Q \in Majorities
    /\ n \in Q
    /\ \A m \in Q \ {n} : nodeTerm[m] < nodeTerm[n]
    /\ nodeTerm' = [m \in Nodes |-> IF m \in Q THEN nodeTerm[n] ELSE nodeTerm[m]]
    /\ believesLeader' = [m \in Nodes |-> IF m = n THEN TRUE
                                          ELSE IF m \in Q THEN FALSE
                                          ELSE believesLeader[m]]
    /\ campaigning' = [m \in Nodes |-> IF m \in Q THEN FALSE ELSE campaigning[m]]
    /\ campaignAge' = [m \in Nodes |-> IF m \in Q THEN 0 ELSE campaignAge[m]]
    /\ quorumAckAge' = [quorumAckAge EXCEPT ![n] = 0]
    /\ leaseRemaining' = [leaseRemaining EXCEPT ![n] = 0]
    /\ leaseIsLeader' = [m \in Nodes |-> IF m \in Q THEN FALSE ELSE leaseIsLeader[m]]
    /\ pgWritable' = [m \in Nodes |-> IF m \in Q THEN FALSE ELSE pgWritable[m]]
    /\ holdDownRemaining' = [holdDownRemaining EXCEPT ![n] =
           IF anchored[n] THEN holdDownRemaining[n] ELSE HoldDown]
    /\ anchored' = [anchored EXCEPT ![n] = TRUE]
    /\ UNCHANGED bootstrapped

(* A leader heartbeats a majority it can still reach. Only nodes that have not
 * moved past its term will answer — that single conjunct is the whole of quorum
 * intersection, and it is what a deposed leader eventually falls foul of. Until
 * then it keeps its quorum, which is the RW-1 regime.
 *
 * Followers in Q learn who leads: they stop campaigning and drop the stale
 * anchor they stamped for a failover somebody else won.
 * CODE: governor/raft.rs should_clear_stale_failover_anchor. *)
ReceiveQuorumAck(n, Q) ==
    /\ believesLeader[n]
    /\ Q \in Majorities
    /\ n \in Q
    /\ \A m \in Q : nodeTerm[m] <= nodeTerm[n]
    /\ nodeTerm' = [m \in Nodes |-> IF m \in Q THEN nodeTerm[n] ELSE nodeTerm[m]]
    /\ quorumAckAge' = [quorumAckAge EXCEPT ![n] = 0]
    /\ believesLeader' = [m \in Nodes |-> IF m \in Q /\ m /= n THEN FALSE
                                          ELSE believesLeader[m]]
    /\ campaigning' = [m \in Nodes |-> IF m \in Q /\ m /= n THEN FALSE ELSE campaigning[m]]
    /\ campaignAge' = [m \in Nodes |-> IF m \in Q /\ m /= n THEN 0 ELSE campaignAge[m]]
    /\ anchored' = [m \in Nodes |-> IF m \in Q /\ m /= n THEN FALSE ELSE anchored[m]]
    /\ holdDownRemaining' = [m \in Nodes |-> IF m \in Q /\ m /= n THEN 0
                                             ELSE holdDownRemaining[m]]
    /\ leaseIsLeader' = [m \in Nodes |-> IF m \in Q /\ m /= n THEN FALSE
                                         ELSE leaseIsLeader[m]]
    /\ pgWritable' = [m \in Nodes |-> IF m \in Q /\ m /= n THEN FALSE ELSE pgWritable[m]]
    /\ UNCHANGED <<bootstrapped, leaseRemaining>>

(* The deposed leader finally hears a higher term and steps down. Optional and
 * unscheduled on purpose: safety must not depend on when it happens. *)
StepDown(n) ==
    /\ believesLeader[n]
    /\ \E m \in Nodes : nodeTerm[m] > nodeTerm[n]
    /\ believesLeader' = [believesLeader EXCEPT ![n] = FALSE]
    /\ leaseIsLeader' = [leaseIsLeader EXCEPT ![n] = FALSE]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<nodeTerm, campaigning, campaignAge, bootstrapped, quorumAckAge,
                   leaseRemaining, holdDownRemaining, anchored>>

(* Renew the lease, anchored at the last quorum ack: remaining = duration minus
 * how stale that ack already is. CODE: governor/lease.rs renew(). *)
RenewLease(n) ==
    /\ believesLeader[n]
    /\ BelievesHasQuorum(n)
    /\ leaseIsLeader' = [leaseIsLeader EXCEPT ![n] = TRUE]
    /\ leaseRemaining' = [leaseRemaining EXCEPT ![n] = LeaseDuration - quorumAckAge[n]]
    /\ UNCHANGED <<nodeTerm, believesLeader, campaigning, campaignAge, bootstrapped,
                   quorumAckAge, holdDownRemaining, anchored, pgWritable>>

(* Supervisor enables PG writes — gated by BOTH a valid lease AND the hold-down.
 * Promotion consumes the anchor, matching `promote_local_postgres`: from here on
 * this node is a primary and the gate that governs it is the lease, not the
 * hold-down. *)
EnablePgWrites(n) ==
    /\ believesLeader[n]
    /\ LeaseIsValid(n)
    /\ HoldDownSatisfied(n)
    /\ ~pgWritable[n]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = TRUE]
    /\ anchored' = [anchored EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<nodeTerm, believesLeader, campaigning, campaignAge, bootstrapped,
                   quorumAckAge, leaseRemaining, holdDownRemaining, leaseIsLeader>>

(* Enforcement / gateway forces read-only once the lease is invalid. *)
Fence(n) ==
    /\ ~LeaseIsValid(n)
    /\ pgWritable[n]
    /\ pgWritable' = [pgWritable EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<nodeTerm, believesLeader, campaigning, campaignAge, bootstrapped,
                   quorumAckAge, leaseRemaining, holdDownRemaining, anchored,
                   leaseIsLeader>>

Next ==
    \/ \E n \in Nodes, Q \in Majorities : Bootstrap(n, Q)
    \/ \E n \in Nodes : StartCampaign(n)
    \/ \E n \in Nodes : AbandonCampaign(n)
    \/ \E n \in Nodes, Q \in Majorities : WinElection(n, Q)
    \/ \E n \in Nodes, Q \in Majorities : ReceiveQuorumAck(n, Q)
    \/ \E n \in Nodes : StepDown(n)
    \/ \E n \in Nodes : RenewLease(n)
    \/ \E n \in Nodes : EnablePgWrites(n)
    \/ \E n \in Nodes : Fence(n)
    \/ Tick

Spec == Init /\ [][Next]_vars

\* ============================================================================
\* SAFETY INVARIANTS
\* ============================================================================

(*
 * THE safety property (contract L1): at most one node holds write authority at
 * any instant. NON-VACUOUS — leadership transfers and a deposed leader keeps
 * acking a quorum of its own until the winner's votes land.
 *
 * Proof sketch: a failover leader N re-stamps its anchor when it begins the
 * candidacy it goes on to win, so it cannot write until HoldDown ticks after
 * that instant. A deposed leader O is not in N's quorum, so its last ack is no
 * later than the win, which is at most VoteDelay after that same instant; from
 * there BelievesHasQuorum(O) fails within QuorumTimeout. HoldDown >= VoteDelay
 * + QuorumTimeout closes the gap. After the win, quorum intersection stops O
 * from acking at all: every majority contains a node at N's term.
 *
 * Set RestampAnchorOnCampaign = FALSE to watch TLC break this — that config is
 * checked, see `make -C tla check-lease_fencing-counterexample`.
 *)
AtMostOneWriteAuthority ==
    Cardinality({n \in Nodes : HasWriteAuthority(n)}) <= 1

(*
 * A leader that stops receiving quorum acks loses write authority within
 * QuorumTimeout of its last ack — even before its lease counts out. This is the
 * bound the hold-down is sized against, and a regression guard on LeaseIsValid
 * keeping the BelievesHasQuorum conjunct.
 *)
SelfFenceOnQuorumLoss ==
    \A n \in Nodes : (quorumAckAge[n] >= QuorumTimeout) => ~LeaseIsValid(n)

(* Writability implies the node currently believes itself the lease leader. *)
WritableImpliesLeaseLeader ==
    \A n \in Nodes : pgWritable[n] => leaseIsLeader[n]

(*
 * Raft's own election safety, here a consequence of voters granting only
 * strictly higher terms rather than an assumption. If this ever fails the
 * quorum-intersection argument above has lost its premise, and every other
 * invariant in this file is resting on nothing.
 *)
OneLeaderPerTerm ==
    \A n, m \in Nodes :
        (n /= m /\ believesLeader[n] /\ believesLeader[m]) => nodeTerm[n] /= nodeTerm[m]

\* ============================================================================
\* THEOREMS (checked by TLC against the cfg)
\* ============================================================================

THEOREM TypeSafety      == Spec => []TypeOK
THEOREM NoSplitBrain    == Spec => []AtMostOneWriteAuthority
THEOREM QuorumSelfFence == Spec => []SelfFenceOnQuorumLoss
THEOREM ElectionSafety  == Spec => []OneLeaderPerTerm

================================================================================
