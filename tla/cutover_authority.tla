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
 * Two clusters (Blue, Green). Write authority is an `(epoch, cluster)` pair.
 * Epochs advance ONLY through Blue's committed Raft log. Green never elects
 * itself into authority; it receives it when Blue commits AUTHORITY_TRANSFERRED.
 *
 * Cutover steps (blue log entries), matching UPGRADE.md:
 *   1. CUTOVER_ARMED
 *   2. WRITE_AUTHORITY_SEALED   — blue stops client writes
 *   3. Green drains (observed; still read-only)
 *   4. AUTHORITY_TRANSFERRED   — only after drain proof; then green may write
 *
 * Fail-closed abort: if green cannot drain, Blue commits UNSEAL into a new
 * epoch and green never wrote.
 *
 * === SAFETY (checked) ===
 *
 *   AtMostOneWriteAuthority — ≤1 cluster accepts client writes (U1).
 *   EpochMonotonic          — blue log epoch never decreases.
 *   GreenWritesOnlyAfterTransfer — green writable ⇒ TRANSFERRED committed.
 *   NoGreenSelfElect        — green cannot become writable without blue log
 *                             (disabled in the inv- config to produce U4).
 *
 * === DELIBERATELY NOT MODELLED ===
 *
 * Sequence advancement (U3), reverse slot ordering (U5), gateway epoch cache,
 * role-based fence internals — those are separate oracles / later specs.
 * This spec owns cross-cluster L1 (U1) and fail-closed cutover (U4).
 *
 * CODE (after TLA lands): blue Raft log entries + green drain probe;
 * today only catalog preflight exists (`commands/upgrade/blue_green.rs`).
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    MaxEpoch,                 \* bound epochs for TLC
    AllowGreenSelfElect       \* FALSE in safety models; TRUE in inv- config

ASSUME MaxEpoch \in Nat /\ MaxEpoch >= 2
ASSUME AllowGreenSelfElect \in BOOLEAN

Clusters == {"blue", "green"}

\* Phase of the cutover protocol as committed on blue (plus idle/aborted).
Phases == {"idle", "armed", "sealed", "draining", "transferred", "aborted"}

VARIABLES
    epoch,            \* Nat — current authority epoch (from blue log)
    authority,        \* cluster that holds write authority for `epoch`, or "none"
    phase,            \* Phases
    blueWritable,     \* BOOLEAN — blue accepts client writes
    greenWritable,    \* BOOLEAN — green accepts client writes
    greenDrained,     \* BOOLEAN — green origin LSN caught up (observed)
    blueReachable,    \* BOOLEAN — green can see blue (fail-closed when FALSE)
    reverseSlotReady  \* BOOLEAN — reverse pub/slot exists before green lift

vars == <<epoch, authority, phase, blueWritable, greenWritable,
          greenDrained, blueReachable, reverseSlotReady>>

TypeOK ==
    /\ epoch \in 0..MaxEpoch
    /\ authority \in Clusters \union {"none"}
    /\ phase \in Phases
    /\ blueWritable \in BOOLEAN
    /\ greenWritable \in BOOLEAN
    /\ greenDrained \in BOOLEAN
    /\ blueReachable \in BOOLEAN
    /\ reverseSlotReady \in BOOLEAN

-----------------------------------------------------------------------------
\* Initial: blue holds epoch 0 write authority; green is read-only.

Init ==
    /\ epoch = 0
    /\ authority = "blue"
    /\ phase = "idle"
    /\ blueWritable = TRUE
    /\ greenWritable = FALSE
    /\ greenDrained = FALSE
    /\ blueReachable = TRUE
    /\ reverseSlotReady = FALSE

-----------------------------------------------------------------------------
\* Actions — blue log commits drive the protocol.

Arm ==
    /\ phase = "idle"
    /\ blueReachable
    /\ epoch < MaxEpoch
    /\ phase' = "armed"
    /\ UNCHANGED <<epoch, authority, blueWritable, greenWritable,
                   greenDrained, blueReachable, reverseSlotReady>>

Seal ==
    /\ phase = "armed"
    /\ blueReachable
    /\ phase' = "sealed"
    /\ blueWritable' = FALSE          \* reuse fence path: stop client writes
    /\ greenDrained' = FALSE
    /\ reverseSlotReady' = FALSE
    /\ UNCHANGED <<epoch, authority, greenWritable, blueReachable>>

\* Green observes drain progress (catalog probe). Still not writable.
ObserveDrain ==
    /\ phase = "sealed"
    /\ blueReachable
    /\ ~greenDrained
    /\ greenDrained' = TRUE
    /\ phase' = "draining"
    /\ UNCHANGED <<epoch, authority, blueWritable, greenWritable,
                   blueReachable, reverseSlotReady>>

\* Slot before fence lift (load-bearing for U5; modelled as a gate here).
PrepareReverseSlot ==
    /\ phase = "draining"
    /\ greenDrained
    /\ blueReachable
    /\ ~reverseSlotReady
    /\ reverseSlotReady' = TRUE
    /\ UNCHANGED <<epoch, authority, phase, blueWritable, greenWritable,
                   greenDrained, blueReachable>>

Transfer ==
    /\ phase = "draining"
    /\ greenDrained
    /\ reverseSlotReady
    /\ blueReachable
    /\ epoch < MaxEpoch
    /\ epoch' = epoch + 1
    /\ authority' = "green"
    /\ phase' = "transferred"
    /\ greenWritable' = TRUE
    /\ blueWritable' = FALSE
    /\ UNCHANGED <<greenDrained, blueReachable, reverseSlotReady>>

\* Green failed to drain: unseal blue into a NEW epoch; green never wrote.
AbortUnseal ==
    /\ phase \in {"sealed", "draining"}
    /\ blueReachable
    /\ epoch < MaxEpoch
    /\ epoch' = epoch + 1
    /\ authority' = "blue"
    /\ phase' = "aborted"
    /\ blueWritable' = TRUE
    /\ greenWritable' = FALSE
    /\ greenDrained' = FALSE
    /\ reverseSlotReady' = FALSE
    /\ UNCHANGED blueReachable

\* After abort, operator may re-arm (new cutover attempt).
ResetAfterAbort ==
    /\ phase = "aborted"
    /\ phase' = "idle"
    /\ UNCHANGED <<epoch, authority, blueWritable, greenWritable,
                   greenDrained, blueReachable, reverseSlotReady>>

\* Network: blue becomes unreachable (or recovers).
LoseBlue ==
    /\ blueReachable
    /\ blueReachable' = FALSE
    /\ UNCHANGED <<epoch, authority, phase, blueWritable, greenWritable,
                   greenDrained, reverseSlotReady>>

RestoreBlue ==
    /\ ~blueReachable
    /\ blueReachable' = TRUE
    /\ UNCHANGED <<epoch, authority, phase, blueWritable, greenWritable,
                   greenDrained, reverseSlotReady>>

\* Defect action: green takes authority without blue's TRANSFERRED entry.
\* Enabled only when AllowGreenSelfElect (inv- config). Produces U4 violation
\* while blue is already sealed (so U1 still holds — only green writes).
GreenSelfElect ==
    /\ AllowGreenSelfElect
    /\ ~blueReachable
    /\ phase \in {"sealed", "draining"}
    /\ ~blueWritable
    /\ greenWritable' = TRUE
    /\ authority' = "green"
    /\ UNCHANGED <<epoch, phase, blueWritable, greenDrained,
                   blueReachable, reverseSlotReady>>

Next ==
    \/ Arm
    \/ Seal
    \/ ObserveDrain
    \/ PrepareReverseSlot
    \/ Transfer
    \/ AbortUnseal
    \/ ResetAfterAbort
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

EpochMonotonic ==
    epoch \in 0..MaxEpoch

GreenWritesOnlyAfterTransfer ==
    greenWritable => (phase = "transferred" /\ authority = "green")

\* When blue is unreachable mid-cutover, green must stay read-only (U4).
FailClosedWhenBlueUnreachable ==
    (~blueReachable /\ phase \in {"armed", "sealed", "draining"})
        => ~greenWritable

WritableMatchesAuthority ==
    /\ (blueWritable => authority = "blue")
    /\ (greenWritable => authority = "green")

=============================================================================
