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

Step 9 is `[~]`, not `[x]`. The cutover protocol, both fences, the epoch
projection, provenance verification, the operator command, and the proofs are
in. What is missing is everything _before_ the handoff: green provisioning and
logical seeding, the DDL freeze, and the schema digest. See "Not implemented"
under Track C.

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
- [x] Policy: patch/minor must not change frame/schema (CI compat fixtures)
- [x] Major may change either within N−1→N only
- [x] Release-tag fixtures: prior release frames + store; HEAD must decode/open them

### Orchestration (step 2)

- [x] Preflight: `doctor --strict`; all nodes same current version; SYNC; backup or `--allow-no-backup`
- [x] Followers in lag order; never bounce sole sync; transfer; upgrade former leader; postflight; idempotent

### Tests

- [x] Matrix: rolling upgrade under continuous writes — `rolling-upgrade` (W1/V1)
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

### Tests

- [x] Happy path under continuous writes; bound pause — `pg-upgrade-hold-writes` + `testing/pg_upgrade_hold_chaos.py`
- [x] Chaos: crash mid-freeze — `pg-upgrade-hold-chaos-mid-freeze`
- [x] Rollback before `--confirm` — live drill `pg-major-upgrade-rollback-drill`; probes from disk, not from a marker
- [x] Write-loss watermark recorded at unfreeze; rollback refuses without `--accept-write-loss` when writes landed past it
- [x] `--link` / `--accept-no-rollback` refuses `--allow-no-backup` — unit `link_forbids_allow_no_backup_at_cli`
- [x] Extension/collation preflight abort cases by name — units `missing_extension_control_named`, `collation_mismatch_abort_message_names_count`
- [x] Freeze is a committed log entry, not a per-node latched bit
- [x] `--confirm` re-runs the verification gate before deleting anything

### Proof gaps

- [x] Runtime image installs **two** PostgreSQL majors (`17` + `18`); `pg_bin_dirs` maps both; refuse green if either `postgres` missing
- [x] Compose that boots the cluster on major N (`config/pgupgrade/` + `docker-compose.pgupgrade.yml`)
- [x] Live `--to N+1 --experimental` apply + standby re-basebackup + `--confirm` (`testing/pg_major_upgrade_proof.py`, matrix `pg-major-upgrade-proof`)
- [x] Continuous writer with an acked-key ledger across the freeze (W1)
- [x] Backup + verified-restore preflight actually runs (no `--allow-no-backup` in the proof)
- [x] Post-upgrade `PG_VERSION` proves N+1 on every node (anti-vacuity: refuse if still on N)
- [ ] Chaos mid-`pg_upgrade` itself (kill the process during file transfer). The
      budget/`kill_on_drop` path and the discard-on-resume rule are unit-covered;
      a live case still needs writing.

---

## Track C — logical blue/green major upgrades

### Contracts

- [x] Register U1–U5 with inversions that **can go red** — `testing/test_bluegreen_oracles.py` (16 tests, each driving one oracle to its failing answer)
- [x] `lint_matrix` refuses an inversion that names an implementation file
- [x] Extend `dual_writability_prober` to six internal ports (`--topology bluegreen`) + `--user` app-role path; the role is recorded in the result

### Authority model (step 8)

- [x] TLA+ cutover spec + counterexample. Two clusters, **separate logs**, a delivery step between them, and monotonicity as an action property
- [x] `(epoch, cluster_id)` identity from `Config::cluster_id`; no blue/green magic strings
- [x] `holds_client_write_authority` is the single read; authority in transit belongs to nobody
- [x] SEAL fences first, then reads its own final LSN, then commits (U2)
- [x] Out-of-phase commands report `OutOfOrder`, not `success: true`
- [x] Gateways relay to the committed recipient address; no fallback to the local leader
- [x] Fail closed: no confirmed address means hold
- [x] Force transfer records `forced: true` permanently; requires `accept_write_loss`; no timeout
- [x] Abort unseal into a new epoch

### Client-vs-apply fence

- [x] Role-based revoke covering tables **and sequences** (`nextval` is a write)
- [x] Lift restores exactly the recorded grants, so abort is a true inverse
- [x] Fence coverage re-checked at SEAL, not only in a separate `--check` run
- [x] Reconciler drives the fence from committed state, so a node promoted mid-cutover applies it
- [x] Recipient fenced from boot via `awaits_write_authority_from` + `client_fence_roles`

### Preflight (step 7)

- [x] All catalog aborts (sequences, RI, LO, unlogged, extensions, fence)

### Orchestration / rollback (step 9)

- [x] Seed via logical; catch up on a real LSN, not a sleep — `testing/bluegreen_cutover.py`
- [x] Drain gate is subscriber-confirmed LSN vs the sealed final LSN, not row counts
- [x] U2 ledger: every acked pre-seal key asserted present on green
- [x] U3: green's sequence is a real identity column, advanced past blue, and the non-collision asserted
- [x] Relay assertion: writes through the donor's gateway land on the recipient and not on the donor
- [x] Post-cutover rollback drill runs the **reverse protocol**, not just a write check
- [x] Six-node compose via `topology.py` — `docker-compose.bluegreen.yml`
- [x] Chaos: arm-seal, seal-transfer, mid-drain, blue-quorum — each asserts an outcome, none use `expect_exit: null`, all four run in CI

### Orchestration command (step 9)

- [x] `pgbattery upgrade postgres --to N --blue-green --experimental --green-node <addr> --green-cluster-id <id> --app-role <role>` runs the handoff
- [x] Refuses a recipient that already claims authority, or whose `cluster_id` is not the one named
- [x] Drain waits on the recipient's applied origin LSN vs the donor's sealed LSN, and refuses a recipient with no subscriptions
- [x] Postflight refuses unless exactly the recipient's nodes claim authority (U1)

### Verified provenance (step 8)

- [x] The recipient confirms the handoff by reading the donor's committed state from a **strict majority of the donor's own nodes** — silence counts against
- [x] `accept_unverified_provenance` narrowed to the unreachable-donor break-glass; it cannot be combined with `donor_mgmt_addrs`
- [x] Unit inversions: minority, all-silent, contradicting, empty-set, and 5-node quorum
- [x] Live inversion: `bluegreen-cutover` asserts a single donor node cannot hand over

### Not implemented (Track C)

- [ ] Product-side green provisioning and logical seeding (`pg_createsubscriber`
      or dump + `CREATE SUBSCRIPTION`). The cutover command requires green to be
      already streaming and says so rather than pretending; the e2e harness does
      the seeding.
- [ ] DDL freeze event trigger and schema digest. `UPGRADE.md` calls these
      "enforced, not requested"; preflight only reports the gap.
- [ ] WAL-retention / slot-lag monitor that aborts before the donor is at risk.
- [ ] Reverse-cutover as its own command. The state machine now lets the cluster
      holding authority arm one, and `bluegreen-rollback-drill` runs the protocol
      with the roles exchanged, but there is no `--rollback` flag that drives it
      for an operator.

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
