-------------------------------- MODULE cutover_authority --------------------------------
(*
 * TLA+ Specification for Track C blue/green cutover write authority (U1 / U4).
 *
 * Machine-checked: `make -C tla check` (CI: .github/workflows/tla.yml).
 * Counterexample: `make -C tla check-counterexamples` via
 * `cutover_authority.inv-green-self-elect.cfg`.
 *
 * === WHAT THIS MODELS ===
 *
 * Two clusters with SEPARATE Raft logs. Blue commits the cutover protocol;
 * Green cannot read blue's log and learns of a handoff only through an
 * attestation it records in its own log. That gap is the point of the spec:
 * modelling one shared phase variable would assume away the only thing that
 * makes cross-cluster handoff hard.
 *
 * Blue's log (bluePhase, blueEpoch) drives:
 *   1. CUTOVER_ARMED
 *   2. WRITE_AUTHORITY_SEALED   — blue stops client writes FIRST, then records
 *                                 its final LSN, so the two are one step
 *   3. Green drains (blue observes the subscriber slot; green still read-only)
 *   4. AUTHORITY_TRANSFERRED    — blue relinquishes at epoch+1
 *
 * Green's log (greenEpoch, greenWritable) advances only on AcceptAuthority,
 * which requires an attestation for a strictly greater epoch. The handoff is
 * therefore NOT atomic: between BlueTransfer and AcceptAuthority nobody is
 * writable, and TLC explores every interleaving of that window with network
 * loss, blue abort, and green acceptance.
 *
 * === SAFETY (checked) ===
 *
 *   AtMostOneWriteAuthority — <=1 cluster accepts client writes (U1).
 *   GreenWritesOnlyAfterTransfer — green writable => blue relinquished that
 *                                  epoch, and green's epoch matches it.
 *   FailClosedWhenBlueUnreachable — green cannot start writing while blue is
 *                                   unreachable mid-cutover (U4).
 *   NoStaleAttestationReplay — green's epoch never exceeds blue's.
 *
 * === PROPERTIES (action formulas — a state invariant cannot express these) ===
 *
 *   EpochsNeverDecrease — blueEpoch' >= blueEpoch /\ greenEpoch' >= greenEpoch.
 *   AuthorityNeverReturnsSilently — green stops being writable only through a
 *                                   step that raises blue's epoch.
 *
 * === DELIBERATELY NOT MODELLED ===
 *
 * Sequence advancement (U3) and reverse-slot ordering (U5) are catalog
 * properties with their own oracles in testing/bluegreen_cutover.py; they are
 * not reachable from this state space. Role-grant internals likewise.
 *
 * CODE: the ClusterCommand cutover variants in governor/state_machine.rs, the
 * fence projection in governor/raft.rs, and the recipient endpoint in
 * observability/management_api/cutover.rs.
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    MaxEpoch,                 \* bound epochs for TLC
    AllowGreenSelfElect       \* FALSE in safety models; TRUE in inv- config

ASSUME MaxEpoch \in Nat /\ MaxEpoch >= 2
ASSUME AllowGreenSelfElect \in BOOLEAN

Clusters == {"blue", "green"}

\* Phase of the cutover protocol as committed in BLUE's log.
Phases == {"idle", "armed", "sealed", "draining", "transferred", "aborted"}

VARIABLES
    blueEpoch,        \* Nat — epoch in blue's committed log
    bluePhase,        \* Phases
    blueWritable,     \* BOOLEAN — blue accepts client writes
    greenEpoch,       \* Nat — epoch in GREEN's own committed log
    greenWritable,    \* BOOLEAN — green accepts client writes
    greenDrained,     \* BOOLEAN — blue observed green's slot reach final LSN
    blueReachable,    \* BOOLEAN — green can reach blue (fail closed when FALSE)
    reverseSlotReady, \* BOOLEAN — reverse pub/slot exists before green lift
    attested          \* Nat — highest epoch an attestation has been delivered
                      \* for; 0 = none. Delivery is a separate step from
                      \* blue's commit, which is what makes the handoff
                      \* non-atomic.

vars == <<blueEpoch, bluePhase, blueWritable, greenEpoch, greenWritable,
          greenDrained, blueReachable, reverseSlotReady, attested>>

TypeOK ==
    /\ blueEpoch \in 0..MaxEpoch
    /\ bluePhase \in Phases
    /\ blueWritable \in BOOLEAN
    /\ greenEpoch \in 0..MaxEpoch
    /\ greenWritable \in BOOLEAN
    /\ greenDrained \in BOOLEAN
    /\ blueReachable \in BOOLEAN
    /\ reverseSlotReady \in BOOLEAN
    /\ attested \in 0..MaxEpoch

-----------------------------------------------------------------------------
\* Initial: blue holds epoch 0 write authority; green is fenced read-only by
\* config (awaits_write_authority_from) before any entry exists.

Init ==
    /\ blueEpoch = 0
    /\ bluePhase = "idle"
    /\ blueWritable = TRUE
    /\ greenEpoch = 0
    /\ greenWritable = FALSE
    /\ greenDrained = FALSE
    /\ blueReachable = TRUE
    /\ reverseSlotReady = FALSE
    /\ attested = 0

-----------------------------------------------------------------------------
\* Blue-side actions: commits in blue's log.

Arm ==
    /\ bluePhase \in {"idle", "aborted"}
    /\ blueEpoch < MaxEpoch
    /\ bluePhase' = "armed"
    /\ UNCHANGED <<blueEpoch, blueWritable, greenEpoch, greenWritable,
                   greenDrained, blueReachable, reverseSlotReady, attested>>

\* Fence and LSN in one step: the implementation revokes client DML, then
\* reads pg_current_wal_flush_lsn, then commits. Splitting them here would
\* model a window the code does not have.
Seal ==
    /\ bluePhase = "armed"
    /\ bluePhase' = "sealed"
    /\ blueWritable' = FALSE
    /\ greenDrained' = FALSE
    /\ reverseSlotReady' = FALSE
    /\ UNCHANGED <<blueEpoch, greenEpoch, greenWritable, blueReachable, attested>>

\* Blue observes the subscriber slot has confirmed past its final LSN.
ObserveDrain ==
    /\ bluePhase = "sealed"
    /\ ~greenDrained
    /\ greenDrained' = TRUE
    /\ bluePhase' = "draining"
    /\ UNCHANGED <<blueEpoch, blueWritable, greenEpoch, greenWritable,
                   blueReachable, reverseSlotReady, attested>>

PrepareReverseSlot ==
    /\ bluePhase = "draining"
    /\ greenDrained
    /\ ~reverseSlotReady
    /\ reverseSlotReady' = TRUE
    /\ UNCHANGED <<blueEpoch, bluePhase, blueWritable, greenEpoch,
                   greenWritable, greenDrained, blueReachable, attested>>

\* Blue relinquishes. Green is NOT writable yet — it has not heard.
BlueTransfer ==
    /\ bluePhase = "draining"
    /\ greenDrained
    /\ reverseSlotReady
    /\ blueEpoch < MaxEpoch
    /\ blueEpoch' = blueEpoch + 1
    /\ bluePhase' = "transferred"
    /\ blueWritable' = FALSE
    /\ UNCHANGED <<greenEpoch, greenWritable, greenDrained, blueReachable,
                   reverseSlotReady, attested>>

\* Break glass: forced transfer with accepted write loss, no drain proof.
\* Still a commit to blue's log, so it needs blue reachable.
ForceTransfer ==
    /\ bluePhase \in {"sealed", "draining"}
    /\ blueReachable
    /\ blueEpoch < MaxEpoch
    /\ blueEpoch' = blueEpoch + 1
    /\ bluePhase' = "transferred"
    /\ blueWritable' = FALSE
    /\ UNCHANGED <<greenEpoch, greenWritable, greenDrained, blueReachable,
                   reverseSlotReady, attested>>

\* Green failed to drain: unseal blue into a NEW epoch. Safe only because
\* green never wrote — which AtMostOneWriteAuthority is checking.
AbortUnseal ==
    /\ bluePhase \in {"sealed", "draining"}
    /\ ~greenWritable
    /\ blueEpoch < MaxEpoch
    /\ blueEpoch' = blueEpoch + 1
    /\ bluePhase' = "aborted"
    /\ blueWritable' = TRUE
    /\ greenDrained' = FALSE
    /\ reverseSlotReady' = FALSE
    /\ UNCHANGED <<greenEpoch, greenWritable, blueReachable, attested>>

ResetAfterAbort ==
    /\ bluePhase = "aborted"
    /\ bluePhase' = "idle"
    /\ UNCHANGED <<blueEpoch, blueWritable, greenEpoch, greenWritable,
                   greenDrained, blueReachable, reverseSlotReady, attested>>

-----------------------------------------------------------------------------
\* The gap between the logs.

\* An orchestrator delivers blue's TRANSFERRED entry to green. Separate from
\* BlueTransfer because the two Raft groups share nothing: this may be
\* arbitrarily delayed, and never happens while blue is unreachable.
DeliverAttestation ==
    /\ bluePhase = "transferred"
    /\ blueReachable
    /\ attested < blueEpoch
    /\ attested' = blueEpoch
    /\ UNCHANGED <<blueEpoch, bluePhase, blueWritable, greenEpoch,
                   greenWritable, greenDrained, blueReachable, reverseSlotReady>>

\* Green records the handoff in its OWN log. Monotonic: a replayed or stale
\* attestation cannot lift the fence twice.
AcceptAuthority ==
    /\ attested > greenEpoch
    /\ greenEpoch' = attested
    /\ greenWritable' = TRUE
    /\ UNCHANGED <<blueEpoch, bluePhase, blueWritable, greenDrained,
                   blueReachable, reverseSlotReady, attested>>

-----------------------------------------------------------------------------
\* Network.

LoseBlue ==
    /\ blueReachable
    /\ blueReachable' = FALSE
    /\ UNCHANGED <<blueEpoch, bluePhase, blueWritable, greenEpoch,
                   greenWritable, greenDrained, reverseSlotReady, attested>>

RestoreBlue ==
    /\ ~blueReachable
    /\ blueReachable' = TRUE
    /\ UNCHANGED <<blueEpoch, bluePhase, blueWritable, greenEpoch,
                   greenWritable, greenDrained, reverseSlotReady, attested>>

-----------------------------------------------------------------------------
\* Defect action for the counterexample model: AcceptAuthority with its one
\* guard removed. Green lifts its fence on a local timeout after waiting on an
\* unreachable blue, rather than on a delivered attestation — which is the
\* timer the design refuses, and the shape a config-only fence would have.
\*
\* Confined to sealed/draining so blue is already not writable: the point is
\* U4, and a trace where both clusters write would report U1 first and say
\* nothing about fail-closed.

GreenSelfElect ==
    /\ AllowGreenSelfElect
    /\ ~blueReachable
    /\ bluePhase \in {"sealed", "draining"}
    /\ ~blueWritable
    /\ greenEpoch < MaxEpoch
    /\ greenEpoch' = greenEpoch + 1
    /\ greenWritable' = TRUE
    /\ UNCHANGED <<blueEpoch, bluePhase, blueWritable, greenDrained,
                   blueReachable, reverseSlotReady, attested>>

Next ==
    \/ Arm
    \/ Seal
    \/ ObserveDrain
    \/ PrepareReverseSlot
    \/ BlueTransfer
    \/ ForceTransfer
    \/ AbortUnseal
    \/ ResetAfterAbort
    \/ DeliverAttestation
    \/ AcceptAuthority
    \/ LoseBlue
    \/ RestoreBlue
    \/ GreenSelfElect

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Invariants

AtMostOneWriteAuthority ==
    Cardinality({c \in Clusters :
                   (c = "blue" /\ blueWritable) \/ (c = "green" /\ greenWritable)})
        <= 1

\* Green writes only for an epoch blue actually relinquished.
GreenWritesOnlyAfterTransfer ==
    greenWritable => (greenEpoch > 0 /\ greenEpoch <= blueEpoch)

\* Green never runs ahead of blue's log: an attestation it invented, or one
\* replayed for an epoch blue has moved past, cannot make it writable.
NoStaleAttestationReplay ==
    greenEpoch <= blueEpoch

\* Blue unreachable mid-cutover: green stays read-only (U4).
FailClosedWhenBlueUnreachable ==
    (~blueReachable /\ bluePhase \in {"armed", "sealed", "draining"})
        => ~greenWritable

-----------------------------------------------------------------------------
\* Action properties. Monotonicity relates a state to its successor, so it
\* cannot be written as a state invariant — one over a single state can only
\* restate the type bound.

EpochsNeverDecrease ==
    [][blueEpoch' >= blueEpoch /\ greenEpoch' >= greenEpoch]_vars

\* Green stops being writable only in a step that raises blue's epoch — i.e.
\* through a committed relinquish, never by silently losing the flag.
AuthorityNeverReturnsSilently ==
    [][(greenWritable /\ ~greenWritable') => blueEpoch' > blueEpoch]_vars

=============================================================================
