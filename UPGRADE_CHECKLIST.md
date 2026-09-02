# Upgrade implementation checklist

Thin, shippable checklist for the work specified in [`UPGRADE.md`](UPGRADE.md).
Design narrative stays there; **implement from this file**. When a step lands,
tick it and add any new contract IDs to [`docs/contracts.yaml`](docs/contracts.yaml)
in the same change (narrative in `docs/CONTRACTS.md` if `prose: true`).

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[-]` wontfix / deferred

Nothing here is implemented unless marked done. Re-derive rollout state from
the cluster (versions, doctor, lag, epoch entries) — do not cache a rollout
journal (`docs/STATE_MACHINE.md` discipline).

---

## Sequencing (do in order)

Shared spine before Track B/C is honest:

| #   | Step                                                       | Track    | Status |
| --- | ---------------------------------------------------------- | -------- | ------ |
| 1   | Raft frame version byte + redb store schema key            | A compat | [x]    |
| 2   | Track A orchestration (`upgrade --cluster`)                | A        | [x]    |
| 3   | WAL archiving + PITR with **restore-and-start proof**      | Shared   | [x]    |
| 4   | Track B preflight (`upgrade postgres --check`)             | B        | [x]    |
| 5   | Track B full flow (`--experimental`), refuse-during-freeze | B        | [x]    |
| 6   | Track B gateway hold mode                                  | B        | [x]    |
| 7   | Track C preflight (`--blue-green --check`)                 | C        | [x]    |
| 8   | Track C cutover protocol — **TLA+ before code**            | C        | [x]    |
| 9   | Track C e2e + reverse replication + post-cutover rollback  | C        | [~]    |

Step 9 is `[~]`, not `[x]`. Every piece is written, and after the audit pass
every audited defect in it is fixed — the arm guard, the drain gate on donor
slot confirmation, backend termination at SEAL, PUBLIC/inherited fence
coverage, real sequence advancement, atomic seeding with the publication and
ACLs, tokens on every call. What is still missing is live evidence: none of
the Track C paths have run against a real cluster, and the Track C matrix
cases have not passed since the rework. `[x]` below means written and
unit/shape-tested, not live-proven.

Do **not** parallelize 5, 6, and 8–9 in one branch. Do **not** ship wire-format
and PG-major upgrade in the same release if avoidable.

---

## Exists today (do not rebuild)

- [x] `pgbattery upgrade` — local binary from release URL, cosign keyless, semver anti-rollback, `--check`
- [x] `POST /api/v1/cluster/transfer-leadership/{id}`
- [x] Supervisor PG lifecycle: initdb, basebackup, promote/demote, pg_rewind, `verify_promotion_safe`
- [x] Join/rejoin via basebackup
- [x] `pgbattery doctor --strict`
- [x] Backup/restore + management API
- [x] Gateway routes to leader and survives failover

---

## Track A — rolling pgbattery self-upgrades

### Compatibility (real work — step 1)

- [x] Inventory current Raft frame header (no always-zero byte to steal — greenfield)
- [x] Protocol version byte: emit N; accept `N-1..=N`; typed “version not accepted”
- [x] Frame: `version(1) + type(1) + corr_id(8)` (`RAFT_RPC_FRAME_VERSION = 0`)
- [x] redb schema-version key at create; missing key = schema 0
- [x] Open: equal → proceed; older → forward migrate before Raft join; newer → refuse (no downgrade)
- [x] Schema 1: `ClusterState` grew `cutover`/`upgrade_frozen`, so v0 snapshots
      are rewritten on open (`migrate_snapshot_v0_to_v1`); a store the new
      binary touches is stamped 1 and refuses the old binary
- [x] Policy: patch/minor must not change frame/schema (CI compat fixtures).
      The schema is the postcard record encodings, not the constant: the
      committed `.bin` pins and populated schema-0/schema-1 golden stores turn
      red on any stored-type field change
- [x] Major may change either within N−1→N only
- [x] Format fixtures: populated prior-schema + current-schema stores and
      encoding pins; HEAD must decode/open/migrate them (release-tagged
      fixtures still need a first tagged release — see the `[-]` below)

### Orchestration (step 2)

- [x] Preflight: `doctor --strict`; all nodes same current version; SYNC; backup or `--allow-no-backup`
- [x] Followers in lag order; never bounce sole sync (`safe_to_bounce` fails
      closed on probe errors, an unknown leader, or an empty sync set);
      transfer, trying each upgraded follower; upgrade former leader;
      postflight; re-running after a mid-roll abort resumes (the preflight
      accepts exactly the {starting, target} version mix)

### Tests

- [x] Matrix: rolling upgrade under continuous writes — `rolling-upgrade` (W1/V1), on PR via `ha-parallel`
- [x] Restart/transfer orientation soak — `leadership-orientation-soak`
- [x] Frame/store compat in CI — `governor::compat_fixtures`, goldens committed

### Proof gaps

- [x] Image ships two distinct package versions (`0.1.0` running + `0.1.1` release artifact)
- [x] Live N→N+1 rolling upgrade across a version boundary (`testing/cluster_binary_upgrade_proof.py`, matrix `cluster-binary-upgrade-proof`)
- [x] Suite `upgrade-proof` runs that case in CI
- [-] Cross-version **wire/store** proof. Both binaries are the same source, so
  frame format and redb schema are identical by construction and the roll
  cannot exercise them. `governor::compat_fixtures` covers that property
  against committed goldens; a real cross-release roll needs two tagged
  releases and is not reachable until one exists.

---

## Shared — WAL archive + PITR proof (step 3)

- [x] `archive_mode` / WAL archiving on
- [x] Harness: `testing/pitr_restore_proof.py`
- [x] Matrix case `pitr-restore-proof`
- [x] Document RPO of abort path

---

## Track B — PostgreSQL major via `pg_upgrade`

### Prerequisites / config / transfer mode / preflight / full flow / hold / docs

- [x] All items in steps 4–6 (see prior commits): `pg_bin_dirs`, transfer modes, preflight, `--experimental` apply, confirm/rollback, gateway hold, STATE_MACHINE
- [x] Retarget is durable: apply, standby rejoin, and rollback rewrite
      `pg_bin_dir` / `pg_data_dir` in the node's own config file (atomic
      `toml_edit` rewrite), so the active-PG pointer survives a pgbattery
      restart; a failed apply restarts the old cluster before reporting

### Tests

- [x] Happy path under continuous writes; bound pause — `pg-upgrade-hold-writes` + `testing/pg_upgrade_hold_chaos.py`
- [x] Chaos: crash mid-freeze — `pg-upgrade-hold-chaos-mid-freeze`, on PR via
      `ha-parallel`: kills the derived leader (kill asserted landed), writers
      on the surviving gateways, zero acks across the failover while frozen;
      `mid-unfreeze` variant runs nightly
- [x] Rollback before `--confirm` — live drill `pg-major-upgrade-rollback-drill`; probes from disk, not from a marker
- [x] Write-loss watermark recorded as the last step before unfreeze; rollback
      refuses without `--accept-write-loss` when WAL advanced past it (client
      writes may be among that WAL — background activity advances it too, so
      the gate is conservative, and the refusal says so)
- [x] `--link` / `--accept-no-rollback` refuses `--allow-no-backup` — unit `link_forbids_allow_no_backup_at_cli`
- [x] Extension/collation preflight abort cases by name — units `missing_extension_control_named`, `collation_mismatch_abort_message_names_count`
- [x] Freeze is a committed log entry, not a per-node latched bit
- [x] `--confirm` re-runs the verification gate before deleting anything

### Proof gaps

- [x] Runtime image installs **two** PostgreSQL majors (`17` + `18`); `pg_bin_dirs` maps both; refuse green if either `postgres` missing
- [x] Compose that boots the cluster on major N (`config/pgupgrade/` + `docker-compose.pgupgrade.yml`)
- [x] Live `--to N+1 --experimental` apply + standby re-basebackup + `--confirm` (`testing/pg_major_upgrade_proof.py`, matrix `pg-major-upgrade-proof`)
- [x] Continuous writer with an acked-key ledger across the freeze (W1) — the
      check is a key-set difference naming missing keys, never a row count
- [x] Backup + verified-restore preflight actually runs (no `--allow-no-backup` in the proof)
- [x] Post-upgrade `PG_VERSION` proves N+1 on every node (anti-vacuity: refuse if still on N)
- [x] Chaos mid-`pg_upgrade` itself — `pg-major-upgrade-chaos-mid-upgrade`. Waits
      for `pg_upgrade` to actually be running before the SIGKILL, so a run that
      killed nothing fails rather than passing. **Never executed.**

---

## Track C — logical blue/green major upgrades

### Contracts

- [x] Register U1–U5 with inversions that **can go red** — `testing/test_bluegreen_oracles.py` drives every U1–U5 oracle to its failing answer
- [x] `lint_matrix` refuses an inversion that names an implementation file
- [x] Extend `dual_writability_prober` to six internal ports (`--topology bluegreen`) + `--user` app-role path; the role is recorded in the result

### Authority model (step 8)

- [x] TLA+ cutover spec + two counterexamples: `inv-green-self-elect` (U4,
      timeout self-election) and `inv-green-self-arm` (U1, arming without the
      holds-authority guard). Two clusters, **separate logs**, a delivery step
      between them, and monotonicity as an action property
- [x] `(epoch, cluster_id)` identity from `Config::cluster_id`; no blue/green magic strings
- [x] `Governor::cutover_authority_projection` is the single read (raw log
      state plus the `awaits_write_authority_from` boot fence); authority in
      transit belongs to nobody, and every handler — status included — reads
      through it
- [x] Arming requires held authority in every phase: arming records authority
      for the local cluster, so a boot-fenced recipient's arm is refused (U1)
- [x] SEAL fences, terminates every fenced-role backend and waits them out,
      then reads its own final LSN, then commits (U2); a seal whose commit
      fails lifts the fence it applied
- [x] Out-of-phase commands report `OutOfOrder`, not `success: true`
- [x] Gateways relay to the committed recipient address; no fallback to the local leader
- [x] Fail closed: no confirmed address means hold
- [x] Force transfer records `forced: true` permanently; requires `accept_write_loss`; no timeout
- [x] Abort unseal into a new epoch — from ARMED too, so a wrong-target arm
      exits without fencing clients first

### Client-vs-apply fence

- [x] Role-based revoke covering tables **and sequences** (`nextval` is a
      write), plus default-ACL grants for every granting owner; coverage
      refuses a seal that PUBLIC grants or an unfenced parent role would
      write through
- [x] Lift restores exactly the recorded grants — relation and default-ACL
      alike, filtered to this cluster's own snapshot rows — skipping loudly
      over a relation dropped mid-fence instead of failing wholesale
- [x] Fence coverage re-checked at SEAL, not only in a separate `--check` run
- [x] Reconciler drives the fence from committed state (read fresh, not the
      tick-lagged projection), retries probe failures, and shares the fence
      lock with the seal/abort handlers
- [x] Recipient fenced from boot via `awaits_write_authority_from` + `client_fence_roles` (role names validated at load)

### Preflight (step 7)

- [x] All catalog aborts (sequences, RI, LO, unlogged, extensions, fence)

### Orchestration / rollback (step 9)

- [x] Seed via logical; catch up on a real LSN, not a sleep — `testing/bluegreen_cutover.py`
- [x] Drain gate is the donor's slot `confirmed_flush_lsn` (min across logical
      slots, NULL fail-closed) vs the sealed final LSN — satisfiable across
      nonpublishable WAL — plus every subscribed table `srsubstate` ready on
      ≥1 enabled subscription; never row counts, never the plateauing origin
- [x] U2 ledger: every acked pre-seal key asserted present on green by
      set-difference, with a writer racing ARM and SEAL and the boundary
      snapshot taken at the seal response
- [x] U3: the product reads donor sequence values and advances the recipient's
      by endpoint (a missing sequence errors by name); the harness drives
      those endpoints and asserts non-collision independently
- [x] Relay assertion probed before reverse wiring: writes through the donor's
      gateway land on the recipient and grow the donor by exactly zero
- [x] Post-cutover rollback drill runs the **reverse protocol**, not just a write check
- [x] Six-node compose via `topology.py` — `docker-compose.bluegreen.yml`
- [x] Chaos: arm-seal, seal-transfer, mid-drain, blue-quorum — each asserts the
      kill actually landed and an outcome, none use `expect_exit: null`, all
      four run in CI; plus the live arm-guard inversion (arming virgin green
      must be refused)

### Orchestration command (step 9)

- [x] `pgbattery upgrade postgres --to N --blue-green --experimental --green-node <addr> --green-cluster-id <id> --app-role <role>` runs the handoff; `--green-token` (env `PGBATTERY_GREEN_MANAGEMENT_API_TOKEN`) for a recipient with its own token; `--accept-security-definer <name>` for named SECURITY DEFINER acceptance
- [x] Refuses a recipient that already claims authority, or whose `cluster_id` is not the one named
- [x] Drain waits on the donor's slot confirmation vs the sealed LSN and the recipient's table-sync readiness, and refuses a recipient with no enabled subscriptions; three consecutive retention-read failures abort while the donor can still unseal
- [x] Postflight refuses unless every recipient node claims authority for the recipient at the transfer epoch and every donor node reports not holding — identity, not a count, with bounded retry for lagging followers (U1)

### Verified provenance (step 8)

- [x] The recipient confirms the handoff by reading the donor's committed state from a **strict majority of the donor's own nodes** — silence counts against, polled concurrently
- [x] The denominator is bounded by the donor's own reported voter count, so an honest caller listing one node of a three-node donor is refused rather than granted a 1-of-1 "majority"
- [x] `accept_unverified_provenance` narrowed to the unreachable-donor break-glass; it cannot be combined with `donor_mgmt_addrs`
- [x] Unit inversions: minority, all-silent, contradicting, empty-set, 5-node quorum, and short-denominator
- [x] Live inversion: `bluegreen-cutover` asserts a single donor node cannot hand over

### Written, live-unproven (Track C)

- [x] Green provisioning and logical seeding — `POST /api/v1/admin/cutover/seed`
      creates the donor publication over every user schema (pgbattery's own
      excluded), dumps the donor schema with its ACLs, restores it in one
      psql transaction (a failure rolls back and re-seed stays possible),
      refuses a recipient that already has tables or is missing a required
      role, and subscribes with copy. Table-copy state
      (`pg_subscription_rel.srsubstate`) is enforced by the drain gate, where
      no subscribed tables is not completion. **Never live-executed.**
- [x] DDL freeze event trigger and schema digest (ordinal columns, not raw
      `attnum`, so a donor that dropped a column can match a fresh recipient).
      The trigger rides the committed cutover phase and the reconciler probes
      it instead of churning it — one left installed would block every DDL
      forever, and the abort drops it before lifting the fence. Frozen SEALED
      through DRAINING only: a recipient fenced from boot is IDLE, and
      freezing it there would block the very DDL seeding needs.
      **Never live-executed.**
- [x] WAL-retention / slot-lag monitor — the drain wait reads donor retention
      each round, fails closed on a broken read (three consecutive failures
      abort), and aborts while the donor can still be unsealed.
      **Never live-executed.**
- [x] Reverse cutover. What blocked it was the arm guard, not a missing command:
      a recipient sat in TRANSFERRED and `CutoverArmed` took only IDLE/ABORTED.
      With that fixed the existing command drives it by swapping `--node` and
      `--green-node`, since neither the plan nor the protocol is
      direction-specific. `bluegreen-rollback-drill` runs it.
- [ ] Operator-facing naming for that reverse run. Rolling back means passing
      `--green-node`/`--green-cluster-id` for the cluster being rolled back
      _to_, which reads backwards. Sugar, not capability.

Matrix case IDs: `bluegreen-cutover`, `bluegreen-rollback-drill`, `bluegreen-chaos-*`.

---

## Doc / lint hygiene (every landing PR)

- [x] `docs/STATE_MACHINE.md` if freeze, hold, epoch, or dual-bin-dir SoT changes
- [x] `docs/contracts.yaml` (+ `CONTRACTS.md` headings) for new FATAL IDs
- [x] `docs/state_machine_inventory.toml` / `tla/code_map.toml` if new types or specs
- [x] Matrix cases cite contract IDs; FATAL rows have inversions that can go red
- [x] No new `ClusterState` leadership projections (`lint_matrix` forbids them)

---

## Explicit refusals

- Timer that deletes rollback anchors or forces authority transfer
- Second Raft group that elects itself into write authority
- Gateway-only fence as the sole cross-cluster L1 mechanism
- rsync-based standby upgrade in Track B
- `--swap` transfer mode
- Caching rollout progress instead of re-deriving from cluster state
