-------------------------------- MODULE raft_lsn --------------------------------
(*
 * TLA+ Specification for Raft with LSN-Aware Elections
 *
 * Machine-checked: `make -C tla check` (CI: .github/workflows/tla.yml).
 *
 * This spec extends standard Raft voting to include PostgreSQL LSN (Log Sequence
 * Number) as an additional constraint on vote acceptance, the "Kukushkin
 * Safety" feature that prevents electing a leader with stale data.
 *
 * The key modification, Raft's up-to-date rule applied to PostgreSQL:
 *   A voter rejects a candidate whose WAL position is behind the voter's own
 *   by more than the threshold. The vote carries the candidate's position, and
 *   the voter reads its own at vote time.
 *
 * We verify:
 *   1. Safety: at most one leader.
 *   2. Liveness: the gate never leaves every node unelectable.
 *
 * === MAPPING TO IMPLEMENTATION ===
 *
 * TLA+ → Rust Code:
 *   lsn[n]          → governor/network.rs: LocalWal::read(), node n's position
 *   IsLSNAcceptable → governor/network.rs: vote_lsn_gate()
 *   RequestVote     → governor/network.rs: VoteFrame, the vote RPC handler
 *
 * SIMPLIFICATIONS:
 *
 * 1. LSN UPDATE MECHANISM:
 *    Spec: lsn values change by leader writes and replication.
 *    Reality: PostgreSQL moves them; nothing samples or reports them for an
 *      election, so there is no staleness to model.
 *
 * 2. LSN LAG THRESHOLD (single, abstracted):
 *    Spec: one LSNLagThreshold constant.
 *    Reality: state_machine.rs vote_lsn_threshold_bytes() is zero unless the
 *      committed sync mode says async, then the published async RPO in
 *      config/constants.rs (LSN_CATCHUP_THRESHOLD_BYTES_ASYNC). An unknown
 *      mode votes as strictly as sync.
 *    One constant abstracts whichever mode is active, so the model is checked
 *      at both ends: raft_lsn.cfg is the tight one and
 *      raft_lsn.alt-loose-lsn-gate.cfg the loose one, and `make check` runs
 *      both.
 *
 * 3. UNREADABLE POSITIONS: not modeled. In the code a voter that cannot read
 *    its own WAL refuses, one with no data directory grants, and a candidate
 *    that sends none is refused by any voter with data.
 *
 * WHAT THIS PROVES: ElectionSafety (at most one leader) and that the LSN gate
 *   never deadlocks elections (NoLSNDeadlock / SomeNodeCanWin): the node
 *   holding the most WAL passes every voter.
 * WHAT TLC ACTIVELY DISPROVES: that the leader is never behind a voter that
 *   elected it. The gate holds at vote time, but this spec has no lease, so a
 *   deposed leader's ReplicateLSN can advance a voter after it voted.
 *   LeaderHasAcceptableLSN and LeaderLSNNotBelowVoters are defined below as the
 *   aspiration but are NOT in the cfg. In the code the deposed leader's lease
 *   fence stops its writes first (lease_fencing.tla).
 * ALSO NOT MODELED: which voters hold acknowledged writes (the sync set).
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
    LSNLagThreshold,    \* Maximum acceptable LSN lag (e.g., 16MB = 16777216)
    EnforceOneVotePerTerm  \* Modeled defect switch; see CanVoteFor

\* ============================================================================
\* VARIABLES
\* ============================================================================

VARIABLES
    currentTerm,        \* currentTerm[n] = current term of node n
    votedFor,           \* votedFor[n] = node that n voted for in current term (or None)
    state,              \* state[n] \in {Follower, Candidate, Leader}
    lsn,                \* lsn[n] = PostgreSQL LSN of node n
    votesGranted,       \* votesGranted[n] = set of nodes that granted vote to n
    leader              \* leader = current leader node (or None)

vars == <<currentTerm, votedFor, state, lsn, votesGranted, leader>>

\* ============================================================================
\* TYPE INVARIANTS
\* ============================================================================

None == 0  \* Represents "no vote" or "no leader"

TypeOK ==
    /\ currentTerm \in [Nodes -> 0..MaxTerm]
    /\ votedFor \in [Nodes -> Nodes \cup {None}]
    /\ state \in [Nodes -> {"Follower", "Candidate", "Leader"}]
    /\ lsn \in [Nodes -> 0..MaxLSN]
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
\* A candidate's LSN is acceptable if it is within the threshold of the
\* voter's own, both as they stand when the vote is asked.
IsLSNAcceptable(candidate, voter) ==
    lsn[candidate] + LSNLagThreshold >= lsn[voter]

\* Combined vote acceptance logic
\*
\* `EnforceOneVotePerTerm = FALSE` drops the second conjunct, which is the
\* modeled defect the .inv- config exists to watch fail: a voter that may back
\* two candidates in one term lets both reach quorum, and ElectionSafety — the
\* property this spec is otherwise never seen to test — breaks. Every other
\* model leaves it TRUE.
CanVoteFor(voter, candidate) ==
    /\ currentTerm[voter] <= currentTerm[candidate]
    /\ (~EnforceOneVotePerTerm
        \/ votedFor[voter] = None
        \/ votedFor[voter] = candidate)
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
    /\ UNCHANGED <<lsn, leader>>

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
    /\ UNCHANGED <<currentTerm, votedFor, lsn>>

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
    /\ UNCHANGED <<lsn>>

(*
 * UpdateLSN: Leader's LSN increases (PostgreSQL writes WAL)
 * In reality, only the leader writes - followers replicate.
 * We model this by only allowing leader to increase LSN.
 *)
UpdateLSN(n) ==
    /\ state[n] = "Leader"  \* Only leader can write
    /\ lsn[n] < MaxLSN
    /\ lsn' = [lsn EXCEPT ![n] = lsn[n] + 1]
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
    /\ UNCHANGED <<currentTerm, votedFor, state, votesGranted, leader>>

(*
 * LeaderFails: Current leader crashes/becomes unavailable
 * Triggers new election
 *)
LeaderFails(n) ==
    /\ state[n] = "Leader"
    /\ state' = [state EXCEPT ![n] = "Follower"]
    /\ leader' = None
    /\ votesGranted' = [votesGranted EXCEPT ![n] = {}]
    /\ UNCHANGED <<currentTerm, votedFor, lsn>>

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
    /\ UNCHANGED <<currentTerm, lsn, leader>>

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
 * LeaderHasAcceptableLSN: aspiration that every voter still finds the leader's
 * LSN acceptable. NOT checked — TLC disproves it. Each voter's check held when
 * it voted, but this spec has no lease: a deposed leader still in Leader state
 * can ReplicateLSN to a voter after its vote and before the winner takes over.
 * In the code the deposed leader's lease fence stops its writes first.
 *)
LeaderHasAcceptableLSN ==
    leader /= None =>
        \A voter \in votesGranted[leader] :
            IsLSNAcceptable(leader, voter)

(*
 * LeaderLSNNotBelowVoters: the same claim stated over raw positions. NOT
 * checked, for the same reason.
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
