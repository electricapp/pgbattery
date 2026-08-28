# pgbattery State Machine — Canonical

States, transitions, and the **definitive source of truth** for each piece of state.

> **Maintenance.** Same-commit updates when consensus / supervisor / lease / replication / fencing / gateway-routing changes add, remove, or rename a state, transition, or truth source — or introduce a cache, timer, or polling loop. Bug fixes that do not change the model stay in the git log. If this doc and the code disagree, **fix the code**.
>
> Put new transitions in the section's state/transition lists, not only in prose underneath. (`Reprovision` once lived a week in code without appearing in any list.) `lint_matrix.py` checks: every type named here still exists; a bare enum name requires every variant to appear; foreign-name exemptions stay minimal.

---

## Philosophy

Transitions are driven by a definitive source of truth — not by a timer, sleep, or polling-as-event.

| Concern                                 | Definitive source of truth                                                                                        |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Who is Raft leader                      | `openraft::RaftMetrics::current_leader` (via `metrics_watch`)                                                     |
| Whether _we_ are Raft leader            | `RaftMetrics::current_leader == Some(self.node_id)`                                                               |
| Raft membership / quorum                | `RaftMetrics::membership_config`                                                                                  |
| Raft committed log                      | `RaftMetrics::last_applied`                                                                                       |
| Whether PG is primary                   | `pg_is_in_recovery()`                                                                                             |
| Whether PG is read-only                 | `SELECT setting FROM pg_settings WHERE name = 'default_transaction_read_only'`                                    |
| Which leader PG is configured to follow | `SELECT setting FROM pg_settings WHERE name = 'primary_conninfo'`                                                 |
| PG sync replication state               | `pg_stat_replication.sync_state`                                                                                  |
| Whether sync durability is in force     | `pg_stat_replication` rows with `sync_state = 'sync'`, vs required voter count — not the GUC text alone           |
| `synchronous_standby_names`             | `SHOW synchronous_standby_names`                                                                                  |
| Standby intent on disk                  | presence of `standby.signal`                                                                                      |
| Current WAL position                    | `pg_current_wal_lsn()` / `pg_last_wal_replay_lsn()` (gates); reportable LSN also uses `pg_last_wal_receive_lsn()` |
| Timeline ID                             | `pg_walfile_name(pg_current_wal_lsn())` parsed                                                                    |
| PG process alive                        | `PostmasterState` — `Child::try_wait()` plus exit code (consumed on read; §3)                                     |
| Lease validity                          | `is_leader && has_quorum && now < expires_at` from `RaftMetrics`                                                  |
| Maximum cluster LSN                     | replicated `node_lsns` — fresh entries when any exist; **aged as tiebreak when none do**                          |

### Discipline

1. **No duplicate caches of a re-derivable truth.** Re-derive unless (a) every truth-changing event invalidates the cache and (b) safety decisions verify against the truth. Allowed today: memoized sync peer-name list (checked against the live GUC before apply); stored `max_cluster_lsn` (advisory — safety paths recompute from `node_lsns`).
2. **Idempotency lives with the writer.** Callers stay stateless; "ensure PG follows X" is the supervisor's job.
3. **Timers are fallbacks only.** Same idempotent path as the event (e.g. 2 s reconcile backs `leader_rx.changed()`). Timers never drive a transition alone.
4. **Cross-process probes ≠ introspection.** `pg_is_in_recovery()` is required; reading back state we just wrote in-process is not.

---

## State machines

### 1. Raft role (governor)

- **States**: `Leader(self) | Follower(of: NodeId) | Candidate | Learner | NoLeader`.
- **Source of truth**: `RaftMetrics::current_leader`.
- **Trigger**: `metrics_watch.changed()`.
- **Code**: `src/governor/raft.rs` — `process_metrics_update`, `log_leadership_changes`.
- **Local projections**: none on `ClusterState` for leadership. Decisions use `RaftMetrics::current_leader`. Address fan-out and wakeup use `leader_rx` (written each metrics tick in `process_metrics_update`). `failover_started_at` remains a local `serde(skip)` Instant on `ClusterState` for the promotion hold-down.
- **Discovery gap**: an isolated ex-leader still reports itself via `/api/v1/cluster/leader` and `/api/v1/cluster/nodes` (both read local `RaftMetrics`). Write authority is unaffected — lease/fence refuse writes. Treat one node's answer as belief, not quorum; `ci_runner`'s `_quorum_leader` requires majority agreement.
- **Election LSN gate**: `evaluate_lsn_acceptable` uses the max **fresh** report when any exist; otherwise the max **aged** report; permissive only if `node_lsns` is empty. Aged ≠ absent (WAL does not go backwards) — blocks a node restored from an old basebackup with an intact Raft dir (RW-7). The holder of the aged max always compares gap-zero against itself, so the gate cannot livelock.
- **Leaderless watchdog**: openraft 0.9 has no pre-vote; voters force elections after `(BASE + rank * STAGGER) * election_timeout` (rank = 0-based among sorted voter ids). Cooldown is one full schedule pass (`voters * STAGGER * election_timeout`, `Governor::leaderless_cooldown`), not a constant — a fixed cooldown collided rank-0's second attempt with rank-2's first (`leaderless-wedge-recovery`). Invariant is tested over ranks × rounds.

### 2. Lease (governor → fencing)

- **States**: `valid | expired` = `is_leader && has_quorum && now < expires_at`.
- **Source of truth**: `RaftMetrics` + monotonic `pgbattery_core::Clock` (`Instant`). Never wall clock for lease expiry or promotion hold-down.
- **One clock for write-authority**: `LeaseState::now()` for fence, leaderless recovery, async-fallback grace, promotion hold-down. No `Instant::now()` / `.elapsed()` on those paths (`testing/lint_clock_injection.py`; `// clock-lint: allow` for non-safety reads). `state_machine.rs` exempt (LSN staleness is replicated; `Instant` is not meaningful across nodes).
- **Trigger**: each metrics update → `LeaseState::update_from_raft`.
- **Promotion hold-down**: refuse `promote()` until `DEFAULT_LEASE_DURATION` after the newest observed Raft term (`failover_started_at`, lease clock). Anchor the **term advance** (when the deposed leader could last hold quorum), not "when we noticed leader gone" — otherwise a late election can promote while the deposed leader's self-fence (`≤ QUORUM_TIMEOUT_MS`) has not fired (L1). Re-stamp via `should_anchor_term_advance` (skip while another node leads; skip term 0 / `Raft::initialize`). Clear on promote and when another node becomes stable leader (`should_clear_stale_failover_anchor`) so a coalesced `other→none→self` win cannot treat an ancient anchor as elapsed (`should_anchor_coalesced_failover`). Modeled as `RestampAnchorOnCampaign` in `tla/lease_fencing.tla`; `lease_fencing.inv-anchor-not-restamped.cfg` must keep producing the counterexample. Does not bound message delay past `DEFAULT_LEASE_DURATION - QUORUM_TIMEOUT_MS`. Retry loop re-checks — no extra timer.
- **Code**: `src/governor/lease.rs`, `App::promote_local_postgres`.

### 3. PostgreSQL process role (supervisor)

- **States** (model, not a Rust enum): `Stopped | Starting | Primary | Standby | Recovering`. No postmaster ⇒ `Stopped`. Distinctions among never-started / stopped / reaped come from `PostmasterState` at the exit edge.
- **Source of truth**:
  - **Role**: `pg_is_in_recovery()`.
  - **On-disk intent**: `standby.signal`.
  - **Liveness**: `PostmasterState` from `Child::try_wait()` + exit code → `Running` | `NotRunning` | `Exited { refused }` (`refused` = nonzero exit; signal death is not refusal).
  - **Consumed on read**: `postmaster_state()` clears the child; the `refused` bit is one-shot → `Error::PostgresRefusedToOpen`.
- **Triggers** (`App::ensure_follows`):
  - `start()` — startup; refusal non-fatal (reconcile repairs once a leader address exists).
  - `promote()` — `current_leader == Some(self.node_id)`; idempotent if not in recovery.
  - `demote(addr)` — `current_leader == Some(other)`; idempotent if already following `addr` on a matching timeline.
  - `stop()` — shutdown.
- **`demote(addr)` → one `StandbyAction`** (`decide_standby_action` / `repoint_standby`):

  | Action        | Meaning                                          | Effect                          |
  | ------------- | ------------------------------------------------ | ------------------------------- |
  | `NoOp`        | Already streaming this leader, matching timeline | nothing                         |
  | `Defer`       | Probe unanswered, or PG will recover alone       | nothing; reconcile retries      |
  | `RestartOnly` | Config stale, timelines compatible               | rewrite conninfo, restart       |
  | `Rewind`      | Local WAL ahead of leader                        | `pg_rewind`, restart            |
  | `Reprovision` | Cannot follow in place                           | discard directory, clone leader |

- Unanswered probes → `Defer`. Never stop/rewind/discard on an unanswered question.
- Both demote paths `Defer` on `TimelineCheck::Unknown` (unreachable leader). Node is already fenced; staying up can still serve WAL the leader needs.
- **`Reprovision` doors** (no restart repairs these):
  1. Directory `PostgreSQL` will not open → clone (`restart_or_repair_standby`); skip `pg_rewind` (needs a running local WAL position).
  2. WAL hole leader cannot fill → `streaming_gap_action` / `replay_can_continue`.
  3. Failed rewind / foreign lineage → `Error::RewindFailed::target_intact`, `Error::ForeignDataDirectory` (intact → restart+retry; compromised/foreign → clone).
- Discard is safe: W1 + V1 ⇒ a same-lineage leader holds every acked write; foreign/half-rewound directories hold nothing this cluster promised. Related divergence hits the divergence gate (refuse), not discard.
- These must be doors, not holds: PG dies on every start in these states → permanent restart loop.
- Wrong prediction → act on refusal: `standby_action_for_mismatch` may skip rewind when replay ≤ leader; `repoint_standby` takes rewind when the checkpoint-based fork refuses.
- **No `Supervisor::role` field.** `pgbattery_pg_is_primary` is set from the `pg_is_in_recovery()` result at the writer.

### 3a. Cluster lineage (identity)

- **SoT**: control-file `Database system identifier` via `pg_controldata` (`read_system_identifier`). Shared iff shared data history. `None` on witness / unprovisioned.
- **Never cached.** Re-provision changes it.
- **Published**: `GET /api/v1/cluster/identity` (no auth), from control file (either end may not be serving yet).
- **Gates**:
  - Resume with local Raft: ask peer if still in membership; wipe own Raft state if not. Attribute the answer with `peer_speaks_for_lineage` (unknown ≠ mismatch).
  - Join discarding a complete data dir: `join_data_dir_disposition` / `clone_supersedes_local_data` — empty OK; interrupted clone cleared; complete dir cleared only when both identifiers known and equal; else keep and refuse. Unknown resolves opposite to membership (destroying data requires proof).
- Mid-clone failure: retry in-process up to `JOIN_CLONE_ATTEMPTS` (idempotent `register_as_learner`); exhausted budget rolls registration back. Do not hand first failure to the container runtime (fresh `initdb` + backoff per attempt).
- Needed because a join with no Raft membership can still hold a complete `PGDATA` the cluster no longer accounts for.

### 4. Sync replication membership (replication manager)

- **Per replica**: `ReplicaHealth` = `Healthy | Lagging | Unhealthy`. Disconnect timing via `ReplicaStatus.last_seen` (`Instant`); flip to `Unhealthy` / `Async` when `now - last_seen >= disconnect_timeout`.
- **Aggregate**: `synchronous_standby_names` on the primary.
- **SoT**: `pg_stat_replication` + live GUC.
- **Trigger**: leader-only ticker `REPLICA_CHECK_INTERVAL_MS` (PG has no event hook).
- **Leadership grace**: suppress async fallback / `SetSyncMode{active:false}` until `disconnect_timeout` after not-leader→leader (`leader_since` from `RaftMetrics`). Avoids every failover dropping to async while followers re-point.
- **Memoization**: peer-name list may be cached; `Supervisor::set_sync_standby_names` still reads the live GUC and short-circuits on match.

### 5. Cluster Raft state machine (replicated)

- **Shape**: `ClusterState` — replicated: members, `node_lsns`, `max_cluster_lsn`, sync-mode flag, etc. **Local only** (`serde(skip)`): `failover_started_at`. Leadership is not stored on `ClusterState`.
- **SoT (replicated fields)**: Raft log via `ClusterCommand` `apply()`. Leadership: `RaftMetrics` (§1); address wakeup: `leader_rx`.
- **Commands** (`ClusterCommand`):
  - `AddNode` — register in `nodes` (always with `NodeRole::Follower`; the role field is dead wire metadata).
  - `RemoveNode` — drop record and `node_lsns` entry.
  - `UpdateNode` — change recorded `NodeRole` (`Leader` | `Follower`). **Never issued in production**; kept for postcard layout / fuzz only. Not live leadership.
  - `UpdateLsn` — WAL position stamped with leader commit time (reporter clock ignored).
  - `SetSyncMode` — whether sync replication is in force.
- **Trigger**: openraft applying a committed entry.
- **Membership ≠ this map**: `change_membership` does not touch `nodes`/`node_lsns`; also commit `ClusterCommand::RemoveNode` (`forget_removed_node`) or a removed peer's LSN keeps inflating `max_cluster_lsn` / `evaluate_lsn_acceptable`. Best-effort after membership commits.
- **Code**: `src/governor/state_machine.rs`, `management_api::cluster::remove_node`.

### 6. Gateway leader routing

- **State**: current primary `SocketAddr`.
- **SoT / trigger**: `leader_rx: watch::Receiver<Option<SocketAddr>>`, fed by the governor metrics handler from `RaftMetrics::current_leader` + the membership address map.
- **In-flight**: 08006 on failover severance (`src/gateway/connection.rs`).
- **Migratability**: `ConnectionState::is_migratable` is false when session state cannot be reconstructed → sever (08006). Sets `not_migratable`: `LISTEN "*"`, session `SET`/`RESET`, and via `SessionChange::NonMigratable` — temp table/view/sequence, `PREPARE`, `WITH HOLD` cursor, `DO`, `CALL`, `LOAD`, session `set_config(..., false)` / session advisory locks (`pg_advisory_lock*` / `pg_try_advisory_lock*`; `*_xact_*` stay migratable). Permanent DDL, plain DML, `SET LOCAL`, `CREATE UNLOGGED`, txn-scoped cursors/locks, large-object descriptors (closed at txn end) stay migratable.
- **Deny-list, not a proof**: `classify_statement` models ~two dozen of ~200 node types; `_ => StatementClass::Unmodeled` is **migrated** (permissive). Fail-safe: `Unmodeled` + `temp`/`temporary`/`pg_temp` token → `not_migratable` (e.g. `EXPLAIN ANALYZE CREATE TEMP…` as `ExplainStmt`, `CREATE FUNCTION pg_temp.f()`). Unmodeled session state that neither names temp nor leads with a gated keyword remains — e.g. a function body creating temp state behind `SELECT f()` (`SelectStmt`). Prefilter match + parse failure → `not_migratable` (fail-closed; analyzer grammar may lag the server).
- **Parse gates**: analyzer runs only for leading keywords (`set`, `reset`, `deallocate`, `discard`, `create`, `prepare`, `declare`, `do`, `load`, `call`), `listen`/`unlisten`, or temp tokens. Gate list and `classify_statement` arms change together (`test_session_state_prefilter_admits_every_gated_statement`). `select` omitted from keywords so the read path stays off `libpg_query`; temp tokens route select/with shapes in.
- **Protocol**: simple query **applies** session changes (LISTEN/UNLISTEN, `DEALLOCATE`/`DISCARD ALL`); extended Parse (`observe_parse_message`) only prepares — never mutates tracked sets; execution that would change session state → `not_migratable`.

### 7. App orchestration — leader-follow loop

- **Driven by**: `(RaftMetrics::current_leader, pg_is_in_recovery())`.
- **Trigger**: `leader_rx.changed()` + 2 s reconcile → `App::ensure_follows` (`src/app.rs`).
- **Snapshot coherence**: promote/demote and follow-target from one `RaftMetrics::current_leader` read + `nodes` map. `leader_rx` is wakeup only — never the address (mixing snapshots demoted a just-deposed leader toward its own stale addr). `demote(addr)` refuses `addr == self`.
- **No role cache.** `promote()` / `demote(addr)` unconditional; both idempotent.
- **Observation**: `LeadershipObservation` — Raft-leader bool + PG primary as `Some(true)` / `Some(false)` / `None` (probe failed; distinct from `Some(false)`). `promote_local_postgres` fails closed on `None`.

### 7a. Leadership / data-plane divergence watchdog

- **Purpose**: Raft leader without a serving primary → yield leadership (do not touch data).
- **States**: `not leading | leading (measuring) | leading (yielded, in cooldown)` in `PromotionWatchdog` (`src/governor/promotion_watchdog.rs`).
- **SoT**: `leader_since` from `RaftMetrics` each pass (same pattern as `ReplicationManager::leader_since`). Not `failover_started_at` (cleared across restart).
- **Clock**: lease clock (`SharedLeaseState::now`).
- **Trigger**: same paths as §7; no extra probe.
- **Rule**: yield after `LEADER_WITHOUT_PRIMARY_YIELD_MS` as Raft leader while PG is not primary. Bounds a **healthy standby stuck on promotion gates** (LSN / `verify_promotion_safe` / hold-down) — health tick still passes. A silent PG fails health (~9 s lose leadership / ~11 s restart) and never reaches this bound.
- **Action**: `POST /api/v1/cluster/transfer-leadership/{target}` on self (§9). Client timeout pinned above API timeout.
- **Target**: most-WAL peer passing `ClusterState::is_lsn_acceptable_for_promotion`; none → no yield.
- **Flapping**: success clears `leader_since`; refusal retries after `LEADER_YIELD_COOLDOWN_MS`.

### 8. App orchestration — lease enforcement / fencing

- **Purpose**: invalid lease → force read-only; valid lease + primary + sync durability delivered → allow writes.
- **SoT each tick**:
  - `lease.is_valid()` immediately before fence/recover (after supervisor-lock wait / probes — not a stale pre-lock snapshot).
  - `Supervisor::probe_role_and_readonly()` — recovery, read-only GUC, empty sync list?, sync standby count — one round trip.
- **Write gate** (`sync_durability_in_force`): evidence is a standby with `sync_state = 'sync'`, not GUC text. Unreadable probe → not in force. Empty list → in force (no ack required / deliberate async). Else need `≥ required_sync_standbys(peers)` sync. Gate on recovery only; never delays a fence.
- **Availability**: with a non-empty list and no sync standby, commits block in PG anyway; the gate turns that into read-only refusal. `Supervisor::promote` arms fence settings **before** `pg_ctl promote` so the primary exists fenced.
- **Trigger**: 100 ms (`LEASE_CHECK_INTERVAL`). SQL budget `LEASE_TICK_SQL_BUDGET = 1 s` → fail-closed fence on overrun.
- **Fence escalation**: `default_transaction_read_only` alone is bypassable (`BEGIN READ WRITE`, existing sessions). After GUC apply, terminate client backends (`pg_terminate_backend`, `backend_type = 'client backend'`) — **only on verified fence**. `set_readonly` polls `pg_settings` up to `READONLY_RELOAD_SETTLE` (300 ms). Still-false after settle → real `FAILED TO FENCE`. Non-answering PG is not split-brain risk (`fence_failure_is_split_brain_risk`) → `Fence not applied`.
- **Fail-closed**: every tick re-queries. After `FENCE_FAILURE_SHUTDOWN_THRESHOLD` (= 5) consecutive failures → process shutdown (`restart: on-failure`).

### 8b. Track B upgrade freeze (gateway)

- **States**: `thawed | frozen` on `ClusterState.upgrade_frozen`.
- **Source of truth**: the **committed Raft log** — `ClusterCommand::SetUpgradeFreeze`, proposed by `POST /api/v1/admin/postgres-upgrade/freeze`. One call covers every node, the state survives a restart, and it clears only through another committed entry. `FenceState.upgrade_frozen` is a re-derived projection, refreshed every metrics tick, never latched.
- **Gateway behaviour**: `FenceState::cutover_client_hold()` or `upgrade_frozen` → **hold** idle sessions up to `upgrade_hold_timeout_ms`, then a clean failover-shaped error. A session with a transaction open is errored immediately rather than held: a datadir swap cannot preserve it.
- **Active PG major pointer**: `pg_bin_dir` (config) plus `PG_VERSION` / `postgres --version` probes; dual map in `pg_bin_dirs`. No leadership projection.
- **Refused**: a per-node latched freeze bit. An orchestrator that died between freeze and unfreeze would leave every gateway holding with nothing able to clear it, and a node that restarted would silently thaw into a cluster mid-`pg_upgrade`.
- **Code**: `FenceState` in `governor/raft.rs`; hold in `gateway/handlers`; freeze command in `governor/state_machine.rs`.

### 8c. Track B upgrade position (probed, never marked)

- **Source of truth**: the data directories themselves. `UpgradeSite::probe` reads `PG_VERSION` from every directory beside PGDATA to find the old and new clusters, and reads `global/pg_control` to decide whether an in-place rollback exists — `pg_upgrade --link` renames that file, so its absence _is_ link mode.
- **Rollback available**: an older data directory is present **and** its control file is intact.
- **Resume after a crash**: `pg_upgrade` has no restart point, so a target directory that no postmaster has ever started is wreckage and is discarded; one that has been started is a live cluster and the upgrade refuses to touch it.
- **Write-loss watermark**: `.pgbattery_unfreeze_watermark` beside PGDATA records the flush LSN clients resumed writing at. It is the LSN of a moment that has passed, not upgrade state — once the new postmaster stops, nothing else can answer whether a rollback discards anything.
- **Refused**: a marker file describing the flow's own intent. Every other fact here is on disk already.
- **Code**: `commands/upgrade/postgres.rs`.

### 8d. Track C cutover authority (epoch)

- **Identity**: `(epoch, cluster_id)`. `cluster_id` is `Config::cluster_id`; every node in a cluster agrees, and two clusters in a cutover differ.
- **Source of truth**: each cluster's **own committed log**. The donor advances the epoch through `CutoverArmed` → `CutoverSeal` → `CutoverDrainObserved` → `CutoverTransfer` (or `CutoverAbortUnseal` into a new epoch). The recipient records `CutoverAuthorityReceived` in its own log.
- **The one read**: `CutoverAuthorityState::holds_client_write_authority(local_cluster_id)`. Authority in transit (SEALED/DRAINING) belongs to nobody; an authority naming another cluster excludes this one whatever its own Raft group thinks.
- **Arming from TRANSFERRED**: allowed only for the cluster that `holds_client_write_authority`. Donor and recipient are both TRANSFERRED after a cutover, so phase alone cannot tell them apart; gating on the authority read lets the recipient arm the reverse cutover while refusing the donor an unearned path back into authority. Arming is otherwise IDLE or ABORTED only.
- **SEAL ordering**: revoke client DML, then read `pg_current_wal_flush_lsn()`, then commit. Committing first, or taking the LSN from the caller, leaves a window where an acked client write carries an LSN past `final_lsn` and the recipient's drain gate passes without it (U2).
- **Two enforcement points**: the gateway holds or relays; the supervisor's reconciler revokes DML from `fenced_app_roles` on the local primary. A gateway-only fence is insufficient by this project's own standard — `dual_writability_prober.py` writes straight to the internal PG port.
- **Recipient before cutover**: `awaits_write_authority_from` plus `client_fence_roles` fence it from boot. Config speaks only until the recipient's own log says something; after that the committed record is the answer.
- **Provenance**: the two Raft groups share no log, so the recipient confirms the donor's commit by reading `/api/v1/cutover/status` from a **strict majority of the donor's nodes** (`donor_quorum_confirms`). Each must report TRANSFERRED, to this recipient, at exactly this epoch, with the donor no longer holding authority; silence counts against. Epoch monotonicity is enforced underneath regardless. `accept_unverified_provenance` is the break-glass for an unreachable donor and cannot be combined with the node list.
- **Drain**: `pg_replication_origin_progress` on the recipient, the minimum across its subscriptions, against the donor's sealed LSN. Applied position, not `received_lsn`, and a recipient with no subscriptions is refused rather than waited out.
- **Forced transfer**: `CutoverTransfer { forced: true }` is a distinct committed record, so the log says permanently that writes acked before SEAL may be missing. It still needs donor quorum; a donor that can commit nothing is write-unavailable until quorum returns, which is the trade U4 makes. There is no timer.
- **Invariant**: `AtMostOneWriteAuthority` across both clusters (U1). Model: `tla/cutover_authority.tla`, which gives the two clusters separate logs and a delivery step between them.
- **Code**: `governor/state_machine.rs`, the projection in `governor/raft.rs`, `observability/management_api/cutover.rs`, `commands/upgrade/client_fence.rs`.

### 8e. Track C blue/green catalog preflight

- **Purpose**: prove logical replication and the client-vs-apply fence can cover this database before a cutover starts.
- **Source of truth**: live PG catalogs on the donor primary (probed SQL); the dual-bin map for the recipient major's extension control files.
- **Also a gate at SEAL**: `require_fence_covers_roles` runs again there. A table owner keeps implicit write privilege through `REVOKE`, so fencing such a role reports U1 held while blind, and a preflight run in a separate invocation cannot prevent that.
- **Not implemented**: the DDL-freeze event trigger and schema digest `UPGRADE.md` specifies, and green provisioning / logical seeding. `run_blue_green_cutover` requires the recipient to be already streaming and refuses one with no subscriptions, rather than assuming it.
- **Code**: `commands/upgrade/blue_green.rs`; `POST /api/v1/admin/postgres-upgrade/blue-green/preflight`.

### 9. Leadership transfer — lame-duck window

- **Purpose**: hand leadership to a chosen target without a third node winning the term.
- **States**: `heartbeating → silent (draining) → silent (handing off) → heartbeating`. `HeartbeatGuard`'s `Drop` restores the last edge on every exit.
- **SoT**: `RaftMetrics::current_leader`, re-read after drain.
- **Code**: `src/observability/management_api/cluster.rs` — `transfer_leadership`, `elect_readiness`, `trigger_elect`.
- **Drain**: openraft refuses votes while follower leases for the current leader are live. After expiry every follower may elect — post-drain work is a race.
- **Budget**: `lame_duck_budget_after_drain_ms()` < `DEFAULT_ELECTION_TIMEOUT_MS` (constants test). Longer work must finish **before** heartbeats stop.
- **Readiness outside the window**: `elect_readiness` waits out target demote lock while still heartbeating; refuses if target cannot serve. `trigger_elect` re-checks under a short bound inside the window.
