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
| 9   | Track C e2e + reverse replication + post-cutover rollback  | C        | [x]    |

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
- [x] Mixed-version soak both orientations — `mixed-version-orientation` (+ frame/store compat fixtures)
- [x] Frame/store compat in CI

### Proof gaps (must go red-then-green)

- [x] Image ships two distinct package versions (`0.1.0` running + `0.1.1` release artifact)
- [x] Live N→N+1 binary rolling upgrade (`testing/cluster_binary_upgrade_proof.py`, matrix `cluster-binary-upgrade-proof`) — not same-binary restart
- [x] Suite `upgrade-proof` runs that case in CI (or documents a dedicated workflow)

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
- [x] Chaos: crash mid-freeze — `pg-upgrade-hold-chaos-mid-freeze` (hold/freeze path; mid-`pg_upgrade` chaos needs dual majors — see proof gaps)
- [x] Rollback before `--confirm` — `rollback_local` refuses `--link`; unit + apply path
- [x] `--link` / `--accept-no-rollback` refuses `--allow-no-backup` — unit `link_forbids_allow_no_backup_at_cli`
- [x] Extension/collation preflight abort cases by name — units `missing_extension_control_named`, `collation_mismatch_abort_message_names_count`

### Proof gaps (must go red-then-green)

- [x] Runtime image installs **two** PostgreSQL majors (`17` + `18`); `pg_bin_dirs` maps both; refuse green if either `postgres` missing
- [x] Compose that boots the cluster on major N (`config/pgupgrade/` + `docker-compose.pgupgrade.yml`)
- [x] Live `--to N+1 --experimental` apply + standby re-basebackup + `--confirm` (`testing/pg_major_upgrade_proof.py`, matrix `pg-major-upgrade-proof`)
- [x] Post-upgrade `SELECT version()` / `PG_VERSION` prove N+1 on every node (anti-vacuity: refuse if still on N)

---

## Track C — logical blue/green major upgrades

### Contracts

- [x] Register U1–U5 with real inversion names
- [x] Extend `dual_writability_prober` to six internal ports (`--topology bluegreen`) + `--user` app-role path

### Authority model (step 8)

- [x] TLA+ cutover spec + counterexample
- [x] Raft SoT + gateway hold via `cutover_client_hold`
- [x] Gateways route to highest epoch / green PG from committed `green_leader_pg_addr` (`FenceState.authority_pg_addr`)
- [x] Fail closed (TLA U4 + hold without green addr)
- [x] Force transfer with `--accept-blue-write-loss` (no timeout)
- [x] Abort unseal into new epoch

### Client-vs-apply fence

- [x] Role-based revoke for app roles (`client_fence.rs`; SEAL API `app_roles`)
- [x] Preflight proves fence coverage (`--blue-green --check --app-role`)
- [x] Prober connected as app role (`--user`)

### Preflight (step 7)

- [x] All catalog aborts (sequences, RI, LO, unlogged, extensions, fence)

### Orchestration / rollback (step 9)

- [x] Provision green; seed via logical; catch up — `testing/bluegreen_cutover.py`
- [x] Run cutover protocol under continuous gateway writes
- [x] Reverse replication; report rollback available
- [x] Post-cutover rollback drill — `--rollback-drill`
- [x] Six-node compose via `topology.py` — `docker-compose.bluegreen.yml`
- [x] Chaos: arm-seal, seal-transfer, mid-drain, blue-quorum — `--chaos …`

### Proof gaps (must go red-then-green)

- [x] Blue cluster boots on major N, green on N+1 (not same-major logical cutover only)
- [x] Live six-node cutover green without `ci_excluded_reason` (`bluegreen-cutover` in suite `upgrade-proof`)
- [x] Rollback-drill case similarly CI-runnable
- [x] Prober U1 across six ports after cutover (not `--skip-prober` as the only path)

Matrix case IDs: `bluegreen-cutover`, `bluegreen-rollback-drill`, `bluegreen-chaos-*`
(chaos cases may stay excluded until the happy path is green in CI).

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
