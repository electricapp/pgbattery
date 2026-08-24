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
| 1   | Raft frame version byte + redb store schema key            | A compat | [ ]    |
| 2   | Track A orchestration (`upgrade --cluster`)                | A        | [ ]    |
| 3   | WAL archiving + PITR with **restore-and-start proof**      | Shared   | [ ]    |
| 4   | Track B preflight (`upgrade postgres --check`)             | B        | [ ]    |
| 5   | Track B full flow (`--experimental`), refuse-during-freeze | B        | [ ]    |
| 6   | Track B gateway hold mode                                  | B        | [ ]    |
| 7   | Track C preflight (`--blue-green --check`)                 | C        | [ ]    |
| 8   | Track C cutover protocol — **TLA+ before code**            | C        | [ ]    |
| 9   | Track C e2e + reverse replication + post-cutover rollback  | C        | [ ]    |

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

**Goal:** `pgbattery upgrade --cluster` → all nodes on target release; write
unavailability bounded to one leadership transfer; refuse jumps outside the
supported compatibility window.

**Non-goals:** N→N+2 in one step; unattended auto-upgrade.

### Compatibility (real work — step 1)

- [ ] Inventory current Raft frame header (always-zero byte to steal?)
- [ ] Protocol version byte: emit/accept N and N−1; typed “version too new”
- [ ] Bootstrap: accept today’s unversioned frames as v0; later release flips emit default
- [ ] redb schema-version key at create; missing key = schema 0
- [ ] Open: equal → proceed; older → forward migrate before Raft join; newer → refuse (no downgrade)
- [ ] Policy: patch/minor must not change frame/schema (CI compat fixtures)
- [ ] Major may change either within N−1→N only
- [ ] Release-tag fixtures: prior release frames + store; HEAD must decode/open them

**SoT:** installed binary version (reported), store schema key, frame version on the wire.  
**Abort:** mixed starting versions; doctor red; jump outside window; sole sync standby bounce.  
**Inversion:** HEAD against fixture from “next” schema/frame must refuse loudly.

### Orchestration (step 2)

- [ ] Preflight: `doctor --strict`; all nodes same current version; SYNC; backup or `--allow-no-backup`
- [ ] Followers in lag order (most caught-up last): upgrade binary → restart → rejoin Raft → streaming (`/lag`)
- [ ] Never restart the only sync candidate until another standby is sync again
- [ ] `transfer-leadership` to upgraded follower
- [ ] Upgrade former leader
- [ ] Postflight: doctor; all nodes report target version
- [ ] Idempotent / resumable: skip nodes already on target (cluster is state)

**Failure:** mid-upgrade node left down (atomic binary replace); loop stops; quorum on remaining two.  
**Rollback:** `--force-downgrade` only; minors when store schema unchanged; majors → restore.

### Tests

- [ ] Matrix: rolling upgrade under continuous writes — zero lost acks, exactly one transfer
- [ ] Mixed-version soak both orientations (leader N−1 / followers N; leader N / follower N−1)
- [ ] Frame/store compat in CI

**Estimate:** days (compat 1–2d, loop 1–2d, fixtures/matrix 1–2d).

---

## Shared — WAL archive + PITR proof (step 3)

Load-bearing for B abort and C abort once reverse replication is forfeited.

- [ ] `archive_mode` / WAL archiving on (today: often off; full basebackup only)
- [ ] Harness: stand up cluster → backup → restore into scratch → start → write → tear down
- [ ] Nightly or matrix case that **fails** if restore never runs (not only `pg_verifybackup`)
- [ ] Document RPO of abort path (hours without archive vs seconds with PITR)

**SoT:** restored cluster accepts a write at expected version.  
**Inversion:** corrupted/incomplete archive must fail the proof red.

---

## Track B — PostgreSQL major via `pg_upgrade`

**Goal:** `pgbattery upgrade postgres --to <major>` — one gateway-held pause
(target &lt;1 min for catalog-sized `--clone`); no connection-string change;
rollback until `--confirm`.

**Non-goals:** near-zero-downtime cutover (that is C); downgrade after `--confirm`.

### Prerequisites / config

- [ ] `pg_bin_dirs = { "18" = "...", "19" = "..." }`; `pg_bin_dir` = active pointer
- [ ] Old+new datadirs same filesystem (except plain `--copy`)
- [ ] Doctor reports disk headroom for selected transfer mode
- [ ] WAL archiving on (step 3) — else `--accept-write-loss` covers hours

### Transfer mode

| Mode      | Select when                                           | Rollback after new start |
| --------- | ----------------------------------------------------- | ------------------------ |
| `--clone` | default if reflink works                              | yes                      |
| `--copy`  | fallback                                              | yes                      |
| `--link`  | only with `--accept-no-rollback` (+ mandatory backup) | no once new starts       |
| `--swap`  | never                                                 | n/a                      |

- [ ] Doctor probes reflink with a real clone attempt (not FS name)
- [ ] `--link` forbids `--allow-no-backup`

### Preflight (step 4) — ships alone

- [ ] `doctor --strict`, SYNC, quorum
- [ ] New binaries present on every node
- [ ] `pg_upgrade --check` on primary
- [ ] Extension audit (control files under new sharedir); post-upgrade load check later
- [ ] Collation version audit (`datcollversion` / `pg_collation.collversion`)
- [ ] initdb params from old cluster (checksums, WAL seg size, encoding, locale)
- [ ] Transfer mode resolved + reflink probe
- [ ] Fresh backup **restored and started** unless `--allow-no-backup`

### Full flow (step 5) — experimental first

1. [ ] Freeze: refuse new queries (hold mode is step 6); stop standbys cleanly
2. [ ] Primary: clean shutdown → `pg_upgrade --clone` → start new → post-upgrade scripts
3. [ ] Standbys: existing join/rejoin basebackup only (no rsync tricks); restore SYNC before second standby
4. [ ] Unfreeze when new primary accepts writes (may be briefly ASYNC — status must say so)
5. [ ] Verification: SYNC both, doctor, `version()` agreement, analyze-in-stages (gating rules per target major)
6. [ ] `--confirm` only by explicit operator action — **no timer** deletes rollback anchor

**SoT:** active `pg_bin_dir` + control-file major; freeze = gateway/lease fence paths already trusted.  
**Abort before confirm:** start old datadir under old binaries; verify write + version; standbys re-basebackup.  
**Abort after confirm / under `--link`:** restore from preflight backup only.

### Gateway hold mode (step 6)

- [ ] Hold queries at protocol level up to `upgrade_hold_timeout` (default 60s), then clean error
- [ ] Existing sessions stall, not disconnect (same contract as leadership transfer)
- [ ] Land **after** refuse-during-freeze flow is chaos-proven

### State machine / docs

- [ ] Update `docs/STATE_MACHINE.md` for freeze/hold and dual-bin-dir active pointer
- [ ] No new leadership projection on `ClusterState`

### Tests

- [ ] Happy path under continuous writes; bound pause; zero lost acks
- [ ] Chaos: crash mid-freeze, mid-`pg_upgrade`, mid-standby re-basebackup
- [ ] Rollback before `--confirm` starts old major and accepts a write
- [ ] `--link` / `--accept-no-rollback` path refuses `--allow-no-backup`
- [ ] Extension/collation preflight abort cases by name

**Estimate:** weeks (preflight valuable alone; full flow + chaos larger).

---

## Track C — logical blue/green major upgrades

**Goal:** two clusters, logical replication, cutover in seconds, rollback available
after cutover until forfeited; **exactly one** write authority across both.

**Non-goals:** in-place upgrade; databases that fail preflight (point at B);
skipping Track B shared preflight/dual-bin-dir.

### Contracts (add to `docs/contracts.yaml` when oracles exist)

| ID  | Severity | Claim                                                            | Inversion (sketch)                              |
| --- | -------- | ---------------------------------------------------------------- | ----------------------------------------------- |
| U1  | FATAL    | At most one `(epoch, cluster)` accepts client writes             | Acceptances on both clusters                    |
| U2  | FATAL    | Every blue ack before SEAL present on green after TRANSFER       | Missing pre-seal ack on green                   |
| U3  | FATAL    | Green sequences never reissue blue values                        | Sequences not advanced                          |
| U4  | FATAL    | Unconfirmed epoch → no writes                                    | Green takes authority with blue unreachable     |
| U5  | FATAL    | While rollback available, every green ack on blue / reverse slot | Fence lifted before reverse slot + write in gap |

- [ ] Register U1–U5 with real inversion names before claiming coverage
- [ ] Extend `dual_writability_prober` to six internal ports + app-role / SECURITY DEFINER paths for the client-vs-apply fence

### Authority model (step 8 — specify + TLA first)

Identity: `(epoch, cluster_id, node_id)`. Epoch advances **only** via **blue’s** Raft log.

| Step | Entry (blue log)                        | Effect                                                                       |
| ---- | --------------------------------------- | ---------------------------------------------------------------------------- |
| 1    | `CUTOVER_ARMED{green}`                  | Pending handoff; clients unchanged                                           |
| 2    | `WRITE_AUTHORITY_SEALED{final_lsn}`     | Blue stops client writes (reuse fence path)                                  |
| 3    | (green drain)                           | Wait origin LSN ≥ final; advance sequences; equality checks; still read-only |
| 4    | `AUTHORITY_TRANSFERRED{green, epoch+1}` | After drain proof; reverse publication + **slot before** lifting green fence |

- [ ] TLA+ cutover spec + counterexample models (fail-closed cases)
- [ ] Gateways route to highest epoch from a **committed** source; never guess/cache
- [ ] Fail closed: blue unreachable → green does not take authority
- [ ] Blue quorum loss after SEAL → green stays RO until explicit `--force-authority-transfer --accept-blue-write-loss` (no timeout)
- [ ] Green drain fail → unseal blue into **new** epoch (committed), green never wrote

### Client-vs-apply fence (new primitive)

Blue cannot be a standby post-cutover (needs logical apply). Need fence that
blocks **client** writes but allows **apply** writes on a primary.

- [ ] Role-based revoke for app roles; subscription owner keeps write
- [ ] Preflight proves fence covers this DB: app roles own no written tables; no app superuser; SECURITY DEFINER enumerated
- [ ] Prober connected as app role through those paths (not plain INSERT only)

### Preflight (step 7) — ships alone

Abort by name for each gap:

- [ ] Sequences not replicated → must advance on green before cutover
- [ ] Replica identity / PK requirements
- [ ] Non-empty `pg_largeobject` → Track B
- [ ] Unlogged tables, DDL-during-freeze policy
- [ ] Everything from Track B version-pair / extension / dual-bin-dir for green
- [ ] Fence-feasibility (above)

### Orchestration / rollback (step 9)

- [ ] Provision green; seed via logical; catch up
- [ ] Run cutover protocol under continuous gateway writes
- [ ] Reverse replication; report rollback available until forfeited (e.g. DDL on green)
- [ ] Post-cutover rollback drill as first-class case (U5 window)
- [ ] Six-node compose via `topology.py` (no hardcoded ports)
- [ ] Chaos: kill/partition at ARMED↔SEAL, SEAL↔TRANSFER, green mid-drain, blue quorum loss after SEAL

**SoT:** blue Raft log for epoch; green catalogs for drain progress (observed).  
**Estimate:** months if protocol+chaos are real; weeks is happy-path only. Step 7 alone is still valuable.

---

## Doc / lint hygiene (every landing PR)

- [ ] `docs/STATE_MACHINE.md` if freeze, hold, epoch, or dual-bin-dir SoT changes
- [ ] `docs/contracts.yaml` (+ `CONTRACTS.md` headings) for new FATAL IDs
- [ ] `docs/state_machine_inventory.toml` / `tla/code_map.toml` if new types or specs
- [ ] Matrix cases cite contract IDs; FATAL rows have inversions that can go red
- [ ] No new `ClusterState` leadership projections (`lint_matrix` forbids them)

---

## Explicit refusals

- Timer that deletes rollback anchors or forces authority transfer
- Second Raft group that elects itself into write authority
- Gateway-only fence as the sole cross-cluster L1 mechanism
- rsync-based standby upgrade in Track B
- `--swap` transfer mode
- Caching rollout progress instead of re-deriving from cluster state
