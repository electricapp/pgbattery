# pgbattery State Machine — Canonical

Canonical reference for every state machine in pgbattery: states, transitions, and the **definitive source of truth** for each piece of state.

> **Maintenance rule.** Changes to consensus, supervisor, lease, replication, fencing, or gateway-routing logic update this document _in the same commit_ — but only when the change adds, removes, or renames a state, transition, or truth source, or introduces a new cache, timer, or polling loop. Bug fixes that don't alter the state model belong in the git log, not here. If the discipline below ever drifts from the code, the _code_ is wrong — fix the code, don't update the doc to match.

---

## Philosophy

Every state transition is driven by a **definitive source of truth** — never by a timer, sleep, or polling-as-substitute-for-event.

| Concern                                 | Definitive source of truth                                                     |
| --------------------------------------- | ------------------------------------------------------------------------------ |
| Who is Raft leader                      | `openraft::RaftMetrics::current_leader` (via `metrics_watch`)                  |
| Whether _we_ are Raft leader            | `RaftMetrics::current_leader == Some(self.node_id)`                            |
| Raft membership / quorum                | `RaftMetrics::membership_config`                                               |
| Raft committed log                      | `RaftMetrics::last_applied`                                                    |
| Whether PG is primary                   | `pg_is_in_recovery()`                                                          |
| Whether PG is read-only                 | `SELECT setting FROM pg_settings WHERE name = 'default_transaction_read_only'` |
| Which leader PG is configured to follow | `SELECT setting FROM pg_settings WHERE name = 'primary_conninfo'`              |
| PG sync replication state               | `pg_stat_replication.sync_state`                                               |
| `synchronous_standby_names`             | `SHOW synchronous_standby_names`                                               |
| Standby intent on disk                  | presence of `standby.signal`                                                   |
| Current WAL position                    | `pg_current_wal_lsn()` / `pg_last_wal_replay_lsn()` (local safety gates); the reportable LSN adds `pg_last_wal_receive_lsn()` — what the node holds on disk |
| Timeline ID                             | `pg_walfile_name(pg_current_wal_lsn())` parsed                                 |
| PG process alive                        | `Child::try_wait()` on the postmaster                                          |
| Lease validity                          | `is_leader && has_quorum && now < expires_at` derived from `RaftMetrics`       |
| Maximum cluster LSN                     | Raft state machine `node_lsns` map (replicated, durable)                       |

### Discipline

1. **No state caches.** If a value can be re-derived from a truth source, re-derive it. A cache is permitted only if (a) every event that changes the truth invalidates it, and (b) it is verified against the truth before any safety-relevant decision. We currently keep none.
2. **Idempotency lives with the writer.** "Ensure PG follows leader X" is the supervisor's job, not the caller's. The truth query happens once, where it has to anyway; callers stay stateless.
3. **Timers are safety fallbacks only.** A timer is allowed only as a backup for a missed event (e.g. the 2 s reconcile loop is a fallback for `leader_rx.changed()`). The fallback path runs the _same_ idempotent code as the event path; timers never drive a transition alone.
4. **Cross-process probes are not introspection.** Querying `pg_is_in_recovery()` is a protocol probe across a process boundary — necessary, because PG's state can diverge from what we told it. Asking our _own_ process for state we already wrote is the antipattern.

---

## State machines

### 1. Raft role (governor)

- **States**: `Leader(self) | Follower(of: NodeId) | Candidate | Learner | NoLeader`.
- **Source of truth**: `RaftMetrics::current_leader`.
- **Transition trigger**: `metrics_watch.changed()`.
- **Code**: `src/governor/raft.rs` — `process_metrics_update`, `log_leadership_changes`.
- **Local projection**: `ClusterState::leader_id` / `leader_addr` are a per-node derivation of `RaftMetrics::current_leader`, re-written every metrics tick by `process_metrics_update` — NOT replicated through the Raft log, and `serde(skip)` so they never ride in a snapshot. Read `RaftMetrics::current_leader` for the truth; the projection only exists to expose the leader address to in-process consumers (replication manager, discovery API).

### 2. Lease (governor → fencing)

- **States**: `valid | expired`. Computed: `is_leader && has_quorum && now < expires_at`.
- **Source of truth**: `RaftMetrics` + wall clock.
- **Transition trigger**: every Raft metrics update calls `LeaseState::update_from_raft`, which renews (anchored on the quorum-ack instant) or expires the lease.
- **Promotion hold-down**: a newly-elected leader refuses `promote()` until one full `DEFAULT_LEASE_DURATION` has elapsed since the locally-observed leader→none edge (`failover_started_at_unix_ms`). The old leader's lease anchors at its last quorum ack, which cannot be later than the instant followers stopped hearing from it — so winning an election (election timeout < lease duration) is _not_ proof the old lease has expired; waiting one lease duration from local detection closes it — but only for a prompt failover. When a deposed leader keeps a quorum that excludes the eventual winner, the winner's local detection can precede the deposed leader's last quorum ack, the hold-down is already satisfied at election, and split-brain freedom instead rests on the quorum-loss self-fence (≤ `QUORUM_TIMEOUT_MS`) and synchronous replication refusing un-acked commits (see `tla/lease_fencing.tla`). The lease is the time-based truth source here; the promotion retry loop re-checks the gate — no new timer.
- **Code**: `src/governor/lease.rs`, `App::promote_local_postgres`.

### 3. PostgreSQL process role (supervisor)

- **States**: `Stopped | Starting | Primary | Standby | Recovering`.
- **Source of truth**:
  - **Authoritative**: `pg_is_in_recovery()`.
  - **On-disk intent**: presence/absence of `standby.signal`.
  - **Process liveness**: `Child::try_wait()`.
- **Transition triggers** (initiated by `App::ensure_follows`):
  - `start()` — at app startup.
  - `promote()` — when `current_leader == Some(self.node_id)`. **Idempotent**: early-return if `pg_is_in_recovery() == false`.
  - `demote(addr)` — when `current_leader == Some(other)`. **Idempotent**: early-return if already in recovery, configured for `addr`, and timeline matches the leader.
  - `stop()` — on shutdown.
- **Cache**: none. There is no `Supervisor::role` field. The `pgbattery_pg_is_primary` metric is set from the actual `pg_is_in_recovery()` result by the writer that just performed the role change.

### 4. Sync replication membership (replication manager)

- **States per replica**: `Healthy | Lagging | Unhealthy(disconnected_since: Instant)`.
- **Aggregate state**: `synchronous_standby_names` GUC value on the primary.
- **Source of truth**: `pg_stat_replication` + the live GUC.
- **Transition trigger**: leader-only ticker on `REPLICA_CHECK_INTERVAL_MS` (polling forced by PG's lack of an event hook on `pg_stat_replication`).
- **Leadership-acquisition grace**: the async fallback (and the `SetSyncMode{active:false}` Raft commit) is suppressed until `disconnect_timeout` has elapsed since the not-leader→leader edge (`leader_since`, re-derived from `RaftMetrics` each tick, cleared when not leader). A freshly-promoted leader sees zero replicas in `pg_stat_replication` for the seconds it takes followers to re-point; without the grace, every failover would silently drop to async — the same hysteresis individual replicas already get via `disconnect_timeout`.
- **Cache**: none. `Supervisor::set_sync_standby_names` is itself idempotent — it reads the live GUC and short-circuits if it already matches. The manager calls it every tick without tracking last-applied state.

### 5. Cluster Raft state machine (replicated)

- **Shape**: single `ClusterState` struct holding `leader_id`, `leader_addr`, members, `node_lsns`, `max_cluster_lsn`, `failover_started_at_unix_ms`.
- **Source of truth**: the Raft log itself (every change is a `ClusterCommand` applied via `apply()`).
- **Transition trigger**: openraft applying a committed log entry.
- **Code**: `src/governor/state_machine.rs`.

### 6. Gateway leader routing

- **States**: which `SocketAddr` is the current primary.
- **Source of truth**: `ClusterState::leader_addr`.
- **Transition trigger**: `leader_rx: watch::Receiver<Option<SocketAddr>>`, fed by the governor's metrics handler.
- **In-flight handling**: 08006 emitted on failover-induced severance (`src/gateway/connection.rs`).
- **Connection migratability**: `ConnectionState::is_migratable` returns false if `NotifySubscriptions::unreplayable_listen` is set (e.g. a `LISTEN "*"` was issued), so the connection is severed on failover rather than silently losing subscription state.

### 7. App orchestration — leader-follow loop

- **Purpose**: react to leader changes by promoting / demoting local PG.
- **States**: implicit — driven entirely by `(RaftMetrics::current_leader, pg_is_in_recovery())`.
- **Transition trigger**: `leader_rx.changed()` (event) + 2-second reconcile (safety fallback). Both paths call `App::ensure_follows` (`src/app.rs`).
- **Snapshot coherence**: the promote-vs-demote decision _and_ the follow-target address derive from one `RaftMetrics::current_leader` read plus the `nodes` membership map; `leader_rx` is a wakeup signal only, never the address source (the watch is populated after the metrics update, so mixing the two snapshots let a just-deposed leader demote toward its own stale address). `demote(addr)` refuses `addr == self`.
- **Cache**: none. `ensure_follows` calls `promote()` or `demote(addr)` unconditionally; both are idempotent in the supervisor.

### 8. App orchestration — lease enforcement / fencing

- **Purpose**: if lease invalid, force PG read-only; if lease valid and we are primary, allow writes.
- **Source of truth, every tick**:
  - `lease.is_valid()` for the lease side — read _after_ the supervisor-lock wait and PG probes, immediately before the fence-or-recover decision (a snapshot taken before an unbounded lock wait can be arbitrarily stale).
  - `Supervisor::probe_role_and_readonly()` for the PG side — `pg_is_in_recovery()` + `default_transaction_read_only` in one round trip.
- **Transition trigger**: 100 ms timer (`LEASE_CHECK_INTERVAL`).
- **SQL budget**: every probe inside `lease_enforcement_tick` is wrapped in `tokio::time::timeout(LEASE_TICK_SQL_BUDGET = 1 s)`; overruns are treated as failed probes and fail-closed (fence).
- **Fence escalation**: `default_transaction_read_only = 'on'` only changes the _default_ — existing sessions and `BEGIN READ WRITE` bypass it. After the GUC applies, the emergency fence terminates client backends (`pg_terminate_backend` over `backend_type = 'client backend'`), so in-flight sessions on a deposed primary cannot keep committing writes that a later rewind destroys.
- **Cache**: none. Every tick re-queries PG. Failed probes are treated as "PG might be writable" (fail-closed). After `FENCE_FAILURE_SHUTDOWN_THRESHOLD` (= 5) consecutive fence failures, the loop signals process shutdown so Docker's `restart: on-failure` brings us back with a clean slate.
