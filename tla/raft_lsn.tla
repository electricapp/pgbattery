-------------------------------- MODULE raft_lsn --------------------------------
(*
 * TLA+ Specification for Raft with LSN-Aware Elections
 *
 * NOT machine-checked in this repo (no TLC in CI). Run it with the command in
 * tla/README.md before relying on the THEOREMs at the foot of this file.
 *
 * This spec extends standard Raft voting to include PostgreSQL LSN (Log Sequence
 * Number) as an additional constraint on vote acceptance. This is the "Kukushkin
 * Safety" feature that prevents electing a leader with stale data.
 *
 * The key modification:
 *   A follower rejects a vote if the Candidate's LSN is unacceptably behind
 *   the follower's known max cluster LSN.
 *
 * We verify:
 *   1. Safety: Only candidates with acceptable LSN can become leader
 *   2. Liveness: The cluster can eventually elect a leader (no permanent deadlock)
 *
 * === MAPPING TO IMPLEMENTATION ===
 *
 * TLA+ Variable → Rust Code:
 *   lsn[n]         → state_machine.rs: node_lsns.get(&n)
 *   maxKnownLSN[n] → state_machine.rs: max_cluster_lsn
 *   IsLSNAcceptable → state_machine.rs: is_lsn_acceptable_for_election()
 *   RequestVote    → governor/network.rs: vote RPC handler
 *
 * Vote Rejection:
 *   TLA: CanVoteFor = FALSE → vote not granted
 *   Code: governor/network.rs → vote_granted: false
 *
 * SIMPLIFICATIONS:
 *
 * 1. LSN UPDATE MECHANISM:
 *    Spec: lsn values change non-deterministically
 *    Reality: Supervisor reports LSN every 1s via UpdateLsn command
 *    Justification: How LSN changes doesn't affect election safety
 *    CODE: app.rs (LSN reporting loop)
 *
 * 2. MAX CLUSTER LSN PROPAGATION:
 *    Spec: maxKnownLSN updated in RequestVote action
 *    Reality: ClusterState updates when UpdateLsn applied
 *    Justification: Both propagate max LSN to voters
 *    CODE: state_machine.rs (update max_cluster_lsn)
 *
 * 3. LSN LAG THRESHOLD (single, abstracted):
 *    Spec: one LSNLagThreshold constant.
 *    Reality: state_machine.rs lsn_catchup_threshold_bytes() picks a DUAL
 *      threshold by replication mode — tight SYNC_LAG_THRESHOLD_BYTES (1 MB,
 *      also the fail-safe default for unknown mode) vs loose
 *      MAX_REPLICATION_LAG_BYTES (16 MB) only when async is positively known.
 *    Run this model twice — a tight and a loose LSNLagThreshold — to cover
 *      both; the single constant abstracts whichever mode is active.
 *
 * 4. VOTING LOGIC (core comparison only):
 *    Spec: candidateLSN + threshold >= voterMaxLSN  (i.e. max - candidate <=
 *      threshold), matching the "too far behind" branch of evaluate_lsn_acceptable.
 *    Abstracted away (no clock in this spec, so documented not checked):
 *      the staleness window (LSN_STALENESS_THRESHOLD_SECS = 30 s) that recomputes
 *      a *fresh* max and rejects a candidate whose own LSN heartbeat is stale;
 *      and the fail-open vs fail-closed split — is_lsn_acceptable_for_election
 *      accepts a candidate with no LSN report, is_lsn_acceptable_for_promotion
 *      rejects it when the cluster has fresh data.
 *
 * WHAT THIS PROVES: LSN-aware voting cannot elect a leader a voter considered
 *   too far behind, and the threshold never deadlocks elections.
 * WHAT THIS DOESN'T PROVE: LSN reporting is timely/accurate; the staleness and
 *   fail-closed-promotion paths above; that the dual threshold is well-chosen.
 *
 * === IMPLEMENTATION vs SPEC NOTE ===
 *
 * The spec models LSN voting as MANDATORY (blocking) - a candidate with
 * unacceptable LSN cannot become leader. The implementation matches:
 * governor/network.rs rejects the vote RPC with an error when the candidate's
 * LSN is too far behind the known cluster max. The spec and implementation
 * are aligned.
 *
 * Authors: pgbattery team
 * Date: 2024
 *)

EXTENDS Naturals, FiniteSets, Sequences, TLC

\* ============================================================================
\* CONSTANTS
\* ============================================================================

CONSTANTS
    Nodes,              \* Set of node IDs (e.g., {1, 2, 3})
    MaxTerm,            \* Maximum term to explore (bounds state space)
    MaxLSN,             \* Maximum LSN value to explore
    LSNLagThreshold     \* Maximum acceptable LSN lag (e.g., 16MB = 16777216)

\* ============================================================================
\* VARIABLES
\* ============================================================================

VARIABLES
    currentTerm,        \* currentTerm[n] = current term of node n
    votedFor,           \* votedFor[n] = node that n voted for in current term (or None)
    state,              \* state[n] \in {Follower, Candidate, Leader}
    lsn,                \* lsn[n] = PostgreSQL LSN of node n
    maxKnownLSN,        \* maxKnownLSN[n] = max LSN node n has observed in cluster
    votesGranted,       \* votesGranted[n] = set of nodes that granted vote to n
    leader              \* leader = current leader node (or None)

vars == <<currentTerm, votedFor, state, lsn, maxKnownLSN, votesGranted, leader>>

\* ============================================================================
\* TYPE INVARIANTS
\* ============================================================================

None == 0  \* Represents "no vote" or "no leader"

TypeOK ==
    /\ currentTerm \in [Nodes -> 0..MaxTerm]
    /\ votedFor \in [Nodes -> Nodes \cup {None}]
    /\ state \in [Nodes -> {"Follower", "Candidate", "Leader"}]
    /\ lsn \in [Nodes -> 0..MaxLSN]
    /\ maxKnownLSN \in [Nodes -> 0..MaxLSN]
    /\ votesGranted \in [Nodes -> SUBSET Nodes]
    /\ leader \in Nodes \cup {None}

\* ============================================================================
\* HELPER PREDICATES
\* ============================================================================

\* Quorum: majority of nodes
Quorum == (Cardinality(Nodes) \div 2) + 1

HasQuorum(votes) == Cardinality(votes) >= Quorum

\* Standard Raft log comparison (simplified - we focus on LSN)
\* In full Raft, this compares lastLogTerm and lastLogIndex
\* Here we abstract to: candidate's log is at least as up-to-date
LogIsUpToDate(candidate, voter) ==
    \* For this spec, we assume all nodes have equivalent Raft logs
    \* The differentiation is in the PostgreSQL LSN
    TRUE

\* LSN acceptability check - THE KEY MODIFICATION
\* A candidate's LSN is acceptable if it's not too far behind the voter's
\* known max cluster LSN
IsLSNAcceptable(candidate, voter) ==
    LET candidateLSN == lsn[candidate]
        voterMaxLSN == maxKnownLSN[voter]
    IN
        \* If voter hasn't observed any LSN, accept (bootstrap case)
        \/ voterMaxLSN = 0
        \* If candidate is within threshold of max known LSN, accept
        \/ candidateLSN + LSNLagThreshold >= voterMaxLSN

\* Combined vote acceptance logic
CanVoteFor(voter, candidate) ==
    /\ currentTerm[voter] <= currentTerm[candidate]
    /\ (votedFor[voter] = None \/ votedFor[voter] = candidate)
    /\ LogIsUpToDate(candidate, voter)
    /\ IsLSNAcceptable(candidate, voter)  \* <-- THE LSN CHECK

\* ============================================================================
\* INITIAL STATE
\* ============================================================================

Init ==
    /\ currentTerm = [n \in Nodes |-> 1]
    /\ votedFor = [n \in Nodes |-> None]
    /\ state = [n \in Nodes |-> "Follower"]
    \* LSN values are chosen non-deterministically to model different scenarios
    /\ lsn \in [Nodes -> 0..MaxLSN]
    \* Initially, each node only knows its own LSN
    /\ maxKnownLSN = [n \in Nodes |-> lsn[n]]
    /\ votesGranted = [n \in Nodes |-> {}]
    /\ leader = None

\* ============================================================================
\* ACTIONS
\* ============================================================================

(*
 * StartElection: A follower times out and becomes a candidate
 * A candidate with quorum doesn't restart - it becomes leader instead.
 *)
StartElection(n) ==
    /\ state[n] \in {"Follower", "Candidate"}
    /\ currentTerm[n] < MaxTerm  \* Bound state space
    \* Don't restart election if we already have quorum (should BecomeLeader instead)
    /\ ~(state[n] = "Candidate" /\ HasQuorum(votesGranted[n]))
    /\ currentTerm' = [currentTerm EXCEPT ![n] = currentTerm[n] + 1]
    /\ votedFor' = [votedFor EXCEPT ![n] = n]  \* Vote for self
    /\ state' = [state EXCEPT ![n] = "Candidate"]
    /\ votesGranted' = [votesGranted EXCEPT ![n] = {n}]  \* Self-vote
    /\ UNCHANGED <<lsn, maxKnownLSN, leader>>

(*
 * RequestVote: Candidate requests vote from another node
 * This includes the LSN check!
 *)
RequestVote(candidate, voter) ==
    /\ state[candidate] = "Candidate"
    /\ voter /= candidate
    /\ CanVoteFor(voter, candidate)
    \* Grant vote
    /\ votedFor' = [votedFor EXCEPT ![voter] = candidate]
    /\ currentTerm' = [currentTerm EXCEPT ![voter] = currentTerm[candidate]]
    /\ votesGranted' = [votesGranted EXCEPT ![candidate] = votesGranted[candidate] \cup {voter}]
    \* Update voter's known max LSN (candidate shares its LSN in vote request)
    /\ maxKnownLSN' = [maxKnownLSN EXCEPT ![voter] =
                       IF lsn[candidate] > maxKnownLSN[voter]
                       THEN lsn[candidate]
                       ELSE maxKnownLSN[voter]]
    /\ UNCHANGED <<state, lsn, leader>>

(*
 * BecomeLeader: Candidate with quorum becomes leader
 * Also steps down any existing leader with lower term (they would discover
 * the higher term via RPC and step down in real Raft).
 *)
BecomeLeader(n) ==
    /\ state[n] = "Candidate"
    /\ HasQuorum(votesGranted[n])
    \* In Raft, a candidate would discover higher terms via RPC and step down
    \* We model this by not becoming leader if any node has higher term
    /\ \A m \in Nodes : currentTerm[m] <= currentTerm[n]
    \* Step down any existing leader with lower term
    /\ state' = [m \in Nodes |->
                    IF m = n THEN "Leader"
                    ELSE IF state[m] = "Leader" /\ currentTerm[m] < currentTerm[n]
                         THEN "Follower"
                         ELSE state[m]]
    /\ leader' = n
    \* Clear votes for stepped-down leaders
    /\ votesGranted' = [m \in Nodes |->
                          IF state[m] = "Leader" /\ currentTerm[m] < currentTerm[n] /\ m /= n
                          THEN {}
                          ELSE votesGranted[m]]
    /\ UNCHANGED <<currentTerm, votedFor, lsn, maxKnownLSN>>

(*
 * StepDown: A node discovers a higher term and steps down
 * This is crucial for liveness - prevents term deadlock
 *)
StepDown(n, higherTermNode) ==
    /\ currentTerm[higherTermNode] > currentTerm[n]
    /\ state[n] \in {"Candidate", "Leader"}
    /\ currentTerm' = [currentTerm EXCEPT ![n] = currentTerm[higherTermNode]]
    /\ state' = [state EXCEPT ![n] = "Follower"]
    /\ votedFor' = [votedFor EXCEPT ![n] = None]
    /\ votesGranted' = [votesGranted EXCEPT ![n] = {}]
    /\ leader' = IF leader = n THEN None ELSE leader
    /\ UNCHANGED <<lsn, maxKnownLSN>>

(*
 * UpdateLSN: Leader's LSN increases (PostgreSQL writes WAL)
 * In reality, only the leader writes - followers replicate.
 * We model this by only allowing leader to increase LSN.
 *)
UpdateLSN(n) ==
    /\ state[n] = "Leader"  \* Only leader can write
    /\ lsn[n] < MaxLSN
    /\ lsn' = [lsn EXCEPT ![n] = lsn[n] + 1]
    /\ maxKnownLSN' = [maxKnownLSN EXCEPT ![n] =
                       IF lsn[n] + 1 > maxKnownLSN[n]
                       THEN lsn[n] + 1
                       ELSE maxKnownLSN[n]]
    /\ UNCHANGED <<currentTerm, votedFor, state, votesGranted, leader>>

(*
 * ReplicateLSN: Follower's LSN catches up to leader via replication
 * This models PostgreSQL streaming replication.
 *)
ReplicateLSN(leaderNode, follower) ==
    /\ state[leaderNode] = "Leader"
    /\ follower /= leaderNode
    /\ lsn[follower] < lsn[leaderNode]  \* Follower is behind
    /\ lsn' = [lsn EXCEPT ![follower] = lsn[leaderNode]]
    /\ maxKnownLSN' = [maxKnownLSN EXCEPT ![follower] =
                       IF lsn[leaderNode] > maxKnownLSN[follower]
                       THEN lsn[leaderNode]
                       ELSE maxKnownLSN[follower]]
    /\ UNCHANGED <<currentTerm, votedFor, state, votesGranted, leader>>

(*
 * PropagateMaxLSN: Leader propagates max LSN to follower (via AppendEntries)
 * This is how followers learn about the cluster's max LSN
 *)
PropagateMaxLSN(leaderNode, follower) ==
    /\ state[leaderNode] = "Leader"
    /\ follower /= leaderNode
    /\ maxKnownLSN' = [maxKnownLSN EXCEPT ![follower] =
                       IF maxKnownLSN[leaderNode] > maxKnownLSN[follower]
                       THEN maxKnownLSN[leaderNode]
                       ELSE maxKnownLSN[follower]]
    /\ UNCHANGED <<currentTerm, votedFor, state, lsn, votesGranted, leader>>

(*
 * LeaderFails: Current leader crashes/becomes unavailable
 * Triggers new election
 *)
LeaderFails(n) ==
    /\ state[n] = "Leader"
    /\ state' = [state EXCEPT ![n] = "Follower"]
    /\ leader' = None
    /\ votesGranted' = [votesGranted EXCEPT ![n] = {}]
    /\ UNCHANGED <<currentTerm, votedFor, lsn, maxKnownLSN>>

(*
 * ElectionTimeoutBreaksTie: Models Raft's random election timeout
 *
 * In real Raft, split-votes resolve because nodes have random timeouts.
 * One node's timeout fires first, it starts a new election and gets votes
 * before others timeout.
 *
 * We model this by: when we're in a split-vote (multiple candidates, no quorum),
 * one candidate can "win the timeout race" and reset others to Follower state
 * with cleared votes, allowing a fresh election round.
 *
 * This is the key action that enables liveness in bounded models.
 *)
ElectionTimeoutBreaksTie(winner) ==
    /\ state[winner] = "Candidate"
    /\ ~HasQuorum(votesGranted[winner])
    \* There's at least one other candidate (it's a split-vote situation)
    /\ \E other \in Nodes : other /= winner /\ state[other] = "Candidate"
    \* Winner has highest term among candidates (would timeout and restart first)
    /\ \A other \in Nodes :
        state[other] = "Candidate" => currentTerm[other] <= currentTerm[winner]
    \* Reset other candidates to Follower (they'll see the higher term)
    /\ state' = [n \in Nodes |->
                    IF n = winner THEN "Candidate"
                    ELSE IF state[n] = "Candidate" THEN "Follower"
                    ELSE state[n]]
    \* Clear their votes and reset votedFor (new election round)
    /\ votesGranted' = [n \in Nodes |->
                          IF n = winner THEN votesGranted[n]
                          ELSE IF state[n] = "Candidate" THEN {}
                          ELSE votesGranted[n]]
    /\ votedFor' = [n \in Nodes |->
                      IF state[n] = "Candidate" /\ n /= winner THEN None
                      ELSE votedFor[n]]
    /\ UNCHANGED <<currentTerm, lsn, maxKnownLSN, leader>>

\* ============================================================================
\* NEXT STATE RELATION
\* ============================================================================

(*
 * Terminating: Allow graceful termination when state space is exhausted
 * This prevents TLC from reporting artificial "deadlock" when MaxTerm
 * prevents further elections. In a real system, terms are unbounded.
 *)
Terminating ==
    /\ \A n \in Nodes : currentTerm[n] >= MaxTerm
    /\ UNCHANGED vars

Next ==
    \/ \E n \in Nodes : StartElection(n)
    \/ \E c, v \in Nodes : RequestVote(c, v)
    \/ \E n \in Nodes : BecomeLeader(n)
    \/ \E n, m \in Nodes : StepDown(n, m)
    \/ \E n \in Nodes : UpdateLSN(n)
    \/ \E l, f \in Nodes : ReplicateLSN(l, f)
    \/ \E l, f \in Nodes : PropagateMaxLSN(l, f)
    \/ \E n \in Nodes : LeaderFails(n)
    \/ \E n \in Nodes : ElectionTimeoutBreaksTie(n)  \* Breaks split-votes
    \/ Terminating

Spec == Init /\ [][Next]_vars

\* ============================================================================
\* SAFETY INVARIANTS
\* ============================================================================

(*
 * ElectionSafety: At most one leader per term
 * (Standard Raft safety property)
 *)
ElectionSafety ==
    \A n, m \in Nodes :
        (state[n] = "Leader" /\ state[m] = "Leader") => n = m

(*
 * LeaderCompleteness: A leader's LSN was acceptable to its voters
 * (The LSN safety property we want to verify)
 *
 * Note: We only check against nodes that VOTED for the leader.
 * Nodes that didn't participate (e.g., partitioned) don't affect this.
 * This matches Raft's quorum-based safety model.
 *)
LeaderHasAcceptableLSN ==
    leader /= None =>
        \A voter \in votesGranted[leader] :
            \* Each voter must have found the leader's LSN acceptable
            \* at the time they voted (based on their maxKnownLSN)
            IsLSNAcceptable(leader, voter)

(*
 * StrongerLSNSafety: No leader can have LSN below any voter's actual LSN
 * This is what we REALLY want - the leader should have data at least as
 * fresh as any node that voted for it.
 *)
LeaderLSNNotBelowVoters ==
    leader /= None =>
        \A voter \in votesGranted[leader] :
            lsn[leader] + LSNLagThreshold >= lsn[voter]

\* ============================================================================
\* LIVENESS PROPERTIES
\* ============================================================================

(*
 * LSNDeadlock: Checks for TRUE deadlock where NO node can EVER become leader
 *
 * THE KEY LIVENESS PROPERTY we need to verify!
 *
 * It's FINE for LSN to block a stale candidate - that's the point!
 * What we care about is: can SOME node eventually become leader?
 *
 * A TRUE LSN deadlock would require:
 * - Every node in the cluster is blocked by LSN when trying to get votes
 * - No node can ever win regardless of term escalation
 *
 * This is the "high term low LSN vs low term high LSN" scenario where
 * BOTH nodes are blocked.
 *)

\* Check if a node COULD become leader (ignoring current state, just LSN)
CouldEventuallyWin(candidate) ==
    \* Can get quorum of votes based purely on LSN acceptability
    \* (Ignoring current term/votedFor - those can change via elections)
    LET potentialVoters == {v \in Nodes : IsLSNAcceptable(candidate, v)}
    IN HasQuorum(potentialVoters)

\* True LSN deadlock: NO node can ever win due to LSN constraints
TrueLSNDeadlock ==
    /\ leader = None
    \* No node in the cluster can get enough LSN-acceptable votes
    /\ \A n \in Nodes : ~CouldEventuallyWin(n)

\* We want to verify that true LSN deadlock is NOT reachable
NoLSNDeadlock == ~TrueLSNDeadlock

(*
 * AlternativeCheck: At least one node can eventually become leader
 * This is equivalent to NoLSNDeadlock but stated positively.
 *)
SomeNodeCanWin ==
    leader /= None \/ \E n \in Nodes : CouldEventuallyWin(n)

(*
 * Legacy invariant name for compatibility
 *)
NoElectionDeadlock == NoLSNDeadlock

(*
 * EventuallyLeader: Eventually a leader is elected
 * (Temporal property - requires fairness)
 *)
EventuallyLeader == <>(leader /= None)

(*
 * LivenessInvariant: When no leader exists and election is possible,
 * there must be a path forward (some node can potentially win).
 *
 * This is a SAFETY property that implies liveness:
 * If there's always a node that can win, the system cannot be stuck.
 *
 * Combined with fairness (which we can't model-check with bounded terms),
 * this guarantees eventual leader election in the real system.
 *)
LivenessPreserved ==
    (leader = None /\ \E n \in Nodes : currentTerm[n] < MaxTerm)
    => SomeNodeCanWin

(*
 * No LSN Starvation: If a node has high LSN, it should be able to get votes.
 * This checks that LSN constraints don't permanently block election.
 *)
HighLSNNodeCanWin ==
    LET maxLSN == CHOOSE m \in 0..MaxLSN :
                    /\ \E n \in Nodes : lsn[n] = m
                    /\ \A n \in Nodes : lsn[n] <= m
        nodesWithMaxLSN == {n \in Nodes : lsn[n] = maxLSN}
    IN \E n \in nodesWithMaxLSN : CouldEventuallyWin(n)

\* ============================================================================
\* FAIRNESS
\* ============================================================================

\* Fairness assumptions for liveness
\* WF = Weak Fairness: if continuously enabled, eventually happens
\* SF = Strong Fairness: if infinitely often enabled, eventually happens
Fairness ==
    /\ \A n \in Nodes : WF_vars(StartElection(n))
    /\ \A c, v \in Nodes : WF_vars(RequestVote(c, v))
    \* STRONG fairness for BecomeLeader - critical!
    \* A candidate with quorum should become leader even if other nodes
    \* keep starting elections (which would disable BecomeLeader temporarily)
    /\ \A n \in Nodes : SF_vars(BecomeLeader(n))
    /\ \A n \in Nodes : WF_vars(UpdateLSN(n))
    /\ \A l, f \in Nodes : WF_vars(ReplicateLSN(l, f))
    /\ \A l, f \in Nodes : WF_vars(PropagateMaxLSN(l, f))
    \* Split-votes eventually resolve via timeout
    /\ \A n \in Nodes : WF_vars(ElectionTimeoutBreaksTie(n))

FairSpec == Spec /\ Fairness

\* ============================================================================
\* MODEL CHECKING CONSTRAINTS
\* ============================================================================

\* State constraint to bound the state space
StateConstraint ==
    /\ \A n \in Nodes : currentTerm[n] <= MaxTerm
    /\ \A n \in Nodes : lsn[n] <= MaxLSN

\* ============================================================================
\* THEOREMS TO VERIFY
\* ============================================================================

\* These are checked by TLC:

\* 1. Type safety
THEOREM TypeSafety == Spec => []TypeOK

\* 2. Election safety (at most one leader)
THEOREM ElectionSafetyTheorem == Spec => []ElectionSafety

\* 3. Leader has acceptable LSN
THEOREM LeaderLSNTheorem == Spec => []LeaderHasAcceptableLSN

\* 4. No election deadlock (the key property for LSN-aware voting)
THEOREM NoDeadlockTheorem == Spec => []NoElectionDeadlock

\* 5. Eventually a leader (with fairness)
THEOREM LivenessTheorem == FairSpec => EventuallyLeader

================================================================================
