# pgbattery State Machine — Canonical

Canonical reference for every state machine in pgbattery: states, transitions, and the **definitive source of truth** for each piece of state.

> **Maintenance rule.** Changes to consensus, supervisor, lease, replication, fencing, or gateway-routing logic update this document _in the same commit_ — but only when the change adds, removes, or renames a state, transition, or truth source, or introduces a new cache, timer, or polling loop. Bug fixes that don't alter the state model belong in the git log, not here. If the discipline below ever drifts from the code, the _code_ is wrong — fix the code, don't update the doc to match.

---

## Philosophy

Every state transition is driven by a **definitive source of truth** — never by a timer, sleep, or polling-as-substitute-for-event.

| Concern                                 | Definitive source of truth                                                                                                                                  |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Who is Raft leader                      | `openraft::RaftMetrics::current_leader` (via `metrics_watch`)                                                                                               |
| Whether _we_ are Raft leader            | `RaftMetrics::current_leader == Some(self.node_id)`                                                                                                         |
| Raft membership / quorum                | `RaftMetrics::membership_config`                                                                                                                            |
| Raft committed log                      | `RaftMetrics::last_applied`                                                                                                                                 |
| Whether PG is primary                   | `pg_is_in_recovery()`                                                                                                                                       |
| Whether PG is read-only                 | `SELECT setting FROM pg_settings WHERE name = 'default_transaction_read_only'`                                                                              |
| Which leader PG is configured to follow | `SELECT setting FROM pg_settings WHERE name = 'primary_conninfo'`                                                                                           |
| PG sync replication state               | `pg_stat_replication.sync_state`                                                                                                                            |
| `synchronous_standby_names`             | `SHOW synchronous_standby_names`                                                                                                                            |
| Standby intent on disk                  | presence of `standby.signal`                                                                                                                                |
| Current WAL position                    | `pg_current_wal_lsn()` / `pg_last_wal_replay_lsn()` (local safety gates); the reportable LSN adds `pg_last_wal_receive_lsn()` — what the node holds on disk |
| Timeline ID                             | `pg_walfile_name(pg_current_wal_lsn())` parsed                                                                                                              |
| PG process alive                        | `Child::try_wait()` on the postmaster                                                                                                                       |
| Lease validity                          | `is_leader && has_quorum && now < expires_at` derived from `RaftMetrics`                                                                                    |
| Maximum cluster LSN                     | Raft state machine `node_lsns` map (replicated, durable). Fresh entries when any exist; **aged entries as a tiebreak when none do** — see below             |

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
- **`current_leader` is a local belief, not a cluster fact, and an isolated node cannot refresh it.** An ex-leader that can no longer hear its followers keeps reporting itself: `/api/v1/cluster/leader` answers `leader_id = self` and `/api/v1/cluster/nodes` marks itself `is_leader`, because both read `RaftMetrics::current_leader` and nothing else. Measured under an inbound-only partition of node1: node2 and node3 elect node3 within ~10 s while node1 still names itself for as long as the partition holds, then steps down within ~5 s of heal. **Write authority is unaffected** — that is gated on the lease, and the same node concurrently reports `pgbattery_lease_valid 0` / `pgbattery_emergency_fence 1` and refuses writes with `cannot execute ... in a read-only transaction`. So this is a discovery-accuracy gap, not a split-brain one: L1 holds, but a client that asks the isolated node where to write is sent to a node that will refuse it. Consumers that need "who can serve writes" rather than "who does Raft think leads" must not treat a single node's answer as authoritative — agreement across a majority is the only reliable reading, which is what `ci_runner`'s `_quorum_leader` does. Deriving `is_leader` from one `Option<node_id>` also means at most one node per `/cluster/nodes` response is ever the leader, so that response can never exhibit split brain no matter how broken the cluster is.

- **The election LSN gate falls back to aged reports, not to permissive.** `evaluate_lsn_acceptable` compares against the maximum _fresh_ report when one exists. When nothing is fresh — a leaderless window past `LSN_STALENESS_THRESHOLD_SECS` — it compares against the maximum _aged_ report instead, and only goes permissive when `node_lsns` is empty (a cluster that has genuinely never reported). Aged is not absent: WAL positions do not go backwards, so a peer that once reported 100 MB has at least that much. This is what stops a node restored from an old basebackup **with its Raft directory intact** from winning: log matching sees a current log and has no view of how old the PostgreSQL data underneath it is (RW-7). It cannot livelock — the node holding the aged maximum compares against itself with a gap of zero, so some candidate is always electable no matter how stale the data is, which is the liveness property the earlier blanket-permissive rule existed to protect.

### 2. Lease (governor → fencing)

- **States**: `valid | expired`. Computed: `is_leader && has_quorum && now < expires_at`.
- **Source of truth**: `RaftMetrics` + the monotonic clock (`pgbattery_core::Clock`, `Instant`-based; injectable in tests). Never the wall clock: lease expiry and the promotion hold-down must be immune to NTP steps, so every time comparison in this state machine is between `Instant`s.
- **One clock for every write-authority decision.** The lease's `Clock` is the only one: `LeaseState::now()` is read by the metrics-watchdog fence and leaderless-recovery gates (`raft.rs`), the async-fallback grace (`replication_manager.rs`), and the promotion hold-down (`app.rs`). Nothing in those paths calls `Instant::now()` or `.elapsed()` — the latter is implicitly `Instant::now() - self`, so a value stamped from an injected clock and then read with `.elapsed()` silently falls back to the real one. `testing/lint_clock_injection.py` enforces this; non-safety reads in those modules carry an inline `// clock-lint: allow` with a reason. `state_machine.rs` is exempt by design (LSN staleness rides in replicated state, where an `Instant` is meaningless across processes).
- **Transition trigger**: every Raft metrics update calls `LeaseState::update_from_raft`, which renews (anchored on the quorum-ack instant) or expires the lease.
- **Promotion hold-down**: a newly-elected leader refuses `promote()` until one full `DEFAULT_LEASE_DURATION` has elapsed since the locally-observed leader→none edge (`failover_started_at`, a monotonic `Instant` — same clock the lease expires on, so no wall-clock adjustment can release the gate while the deposed leader's lease is still valid). The old leader's lease anchors at its last quorum ack, which cannot be later than the instant followers stopped hearing from it — so winning an election (election timeout < lease duration) is _not_ proof the old lease has expired; waiting one lease duration from local detection closes it — but only for a prompt failover. When a deposed leader keeps a quorum that excludes the eventual winner, the winner's local detection can precede the deposed leader's last quorum ack, the hold-down is already satisfied at election, and split-brain freedom instead rests on the quorum-loss self-fence (≤ `QUORUM_TIMEOUT_MS`) and synchronous replication refusing un-acked commits (see `tla/lease_fencing.tla`). The lease is the time-based truth source here; the promotion retry loop re-checks the gate — no new timer. The anchor describes only the failover this node might complete: it is cleared when this node promotes (consuming it) **and** on the edge where a _different_ node becomes the stable leader (`should_clear_stale_failover_anchor`). Without the latter clear, a node that witnessed a failover it did not win would carry the anchor forever, and on a _later_ election it does win via a coalesced `other→none→self` transition the re-anchor would be suppressed (`should_anchor_coalesced_failover` refuses when already anchored) — making the hold-down read the ancient timestamp as long-elapsed and promote immediately.
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

- **States per replica**: `Healthy | Lagging | Unhealthy` (a fieldless `ReplicaHealth`). The disconnect instant is not carried on the variant; it is tracked separately as `ReplicaStatus.last_seen` (a monotonic `Instant`), and `disconnect_timeout` is measured against it. A replica is flipped to `Unhealthy` (and its `sync_state` to `Async`) once `now - last_seen >= disconnect_timeout`.
- **Aggregate state**: `synchronous_standby_names` GUC value on the primary.
- **Source of truth**: `pg_stat_replication` + the live GUC.
- **Transition trigger**: leader-only ticker on `REPLICA_CHECK_INTERVAL_MS` (polling forced by PG's lack of an event hook on `pg_stat_replication`).
- **Leadership-acquisition grace**: the async fallback (and the `SetSyncMode{active:false}` Raft commit) is suppressed until `disconnect_timeout` has elapsed since the not-leader→leader edge (`leader_since`, re-derived from `RaftMetrics` each tick, cleared when not leader). A freshly-promoted leader sees zero replicas in `pg_stat_replication` for the seconds it takes followers to re-point; without the grace, every failover would silently drop to async — the same hysteresis individual replicas already get via `disconnect_timeout`.
- **Cache**: none. `Supervisor::set_sync_standby_names` is itself idempotent — it reads the live GUC and short-circuits if it already matches. The manager calls it every tick without tracking last-applied state.

### 5. Cluster Raft state machine (replicated)

- **Shape**: single `ClusterState` struct holding `leader_id`, `leader_addr`, members, `node_lsns`, `max_cluster_lsn`, `failover_started_at`.
- **Source of truth**: the Raft log itself (every change is a `ClusterCommand` applied via `apply()`).
- **Transition trigger**: openraft applying a committed log entry.
- **Code**: `src/governor/state_machine.rs`.

### 6. Gateway leader routing

- **States**: which `SocketAddr` is the current primary.
- **Source of truth**: `ClusterState::leader_addr`.
- **Transition trigger**: `leader_rx: watch::Receiver<Option<SocketAddr>>`, fed by the governor's metrics handler.
- **In-flight handling**: 08006 emitted on failover-induced severance (`src/gateway/connection.rs`).
- **Connection migratability**: `ConnectionState::is_migratable` returns false when the session carries backend-local state the gateway cannot reconstruct on a migrated backend, so the connection is severed (08006) on failover rather than silently losing it. Triggers set `not_migratable`: `LISTEN "*"` (unreplayable subscription), session `SET`/`RESET`, and — via `SessionChange::NonMigratable` — a temp table (`CREATE TEMP`, `CREATE TEMP TABLE ... AS`, `SELECT ... INTO TEMP`), a temp view, a temp sequence, SQL `PREPARE`, a `WITH HOLD` cursor, any `DO` block (its body is procedural-language source `libpg_query` does not parse, so a block taking a session lock is indistinguishable from one leaving nothing behind), a `CALL` (the procedure body lives in the catalog rather than the parse tree, so it is opaque for the same reason, and a procedure may additionally `COMMIT` mid-body, which `DO` cannot), a `LOAD` (the shared library is linked into this backend only, so the functions and hooks it provides stop resolving on a migrated one), and (cheap-token prefilter, no parse) `set_config(..., false)` / session-scoped advisory locks (both the `pg_advisory_lock*` and `pg_try_advisory_lock*` spellings; the `*_xact_*` variants release at commit and stay migratable). Permanent DDL, plain DML, `SET LOCAL`, `CREATE UNLOGGED`, transaction-scoped cursors/locks, and large-object descriptors (PG closes every descriptor open at transaction end, so an Idle session holds none) stay migratable.
- **The analyzer is a deny-list, not a safety proof**: `classify_statement` has arms for roughly two dozen of PostgreSQL's ~200 statement node types; `_ => StatementClass::Unmodeled` covers the rest and is **migrated** — the permissive direction. "Migratable" therefore does not mean "examined and found state-free". One narrow fail-safe contains the worst of it: an `Unmodeled` statement whose parsed text also carries a `temp` / `temporary` / `pg_temp` token ratchets `not_migratable`, catching `EXPLAIN ANALYZE CREATE TEMP TABLE ...` (an `ExplainStmt` that executes its inner statement) and `CREATE FUNCTION pg_temp.f()`. Plain DML and transaction-control types are modeled explicitly as state-free so an ordinary `UPDATE readings SET temp = ...` cannot reach that fallback. `LOAD` and `CALL` now have their own arms. What remains unmodeled is session state left by a construct that neither names the temp schema nor leads with a gated keyword — most concretely a _function_ body (as opposed to a procedure) that creates a temp table or takes a session advisory lock, since `SELECT f()` is a plain `SelectStmt` and deliberately stays off the parser.
- **Parse gates**: the analyzer runs only when a query looks relevant — a statement-leading keyword (`set`, `reset`, `deallocate`, `discard`, `create`, `prepare`, `declare`, `do`, `load`, `call`), a `listen`/`unlisten` token, or a `temp`/`temporary`/`pg_temp` token. This list decides whether the analyzer runs at all, so a `classify_statement` arm whose statement is not gated here is unreachable in production; the two are changed together, and `test_session_state_prefilter_admits_every_gated_statement` is what enforces it (the analyzer-level tests call `analyze_query` directly and cannot see a missing gate keyword). The temp token is what routes `select`- and `with`-leading shapes to the parser, since `select` is deliberately absent from the keyword list to keep the read hot path off `libpg_query`. Word boundaries mean `temp_c` and `temp_buffers` do not match.
- **Protocol asymmetry, by design**: on the simple-query path the analyzer's session changes are _applied_ (LISTEN/UNLISTEN update the tracked subscription set; `DEALLOCATE`/`DISCARD ALL` prune the replay set) because a simple Query executes on receipt. On the extended-protocol path (`observe_parse_message`) a Parse only _prepares_ — execution is unknowable (an error before Sync discards queued Executes) — so the gateway never mutates its tracked sets there; any statement whose execution would change session state instead ratchets `not_migratable`. Severing is the safe direction; silently migrating a maybe-wrong subscription/replay set is not.

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

### 9. Leadership transfer — the lame-duck window

- **Purpose**: hand leadership to a chosen target without the cluster electing someone else.
- **States**: `heartbeating → silent (draining) → silent (handing off) → heartbeating`. `HeartbeatGuard`'s `Drop` restores the last edge on every exit, including a cancelled handler future or a panic.
- **Source of truth**: `RaftMetrics::current_leader`, re-read after the drain — leadership can move while this node is silent, and triggering an election then would bump a term against a cluster that already has a leader.
- **Code**: `src/observability/management_api/cluster.rs` — `transfer_leadership`, `elect_readiness`, `trigger_elect`.
- **The drain is what removes the protection.** openraft refuses to grant a vote while a follower's lease for the current leader is live, so no follower can elect during the drain itself. The moment it expires every follower is eligible and its election timer starts. Everything the leader does after the drain is therefore a race it can lose, and losing it is a third node taking the term mid-transfer.
- **Budget**: `lame_duck_budget_after_drain_ms()` sums every post-drain cost — the lease safety overshoot, the trigger-elect client timeout, and the handover observation. A constants test pins it below `DEFAULT_ELECTION_TIMEOUT_MS`. Anything that can block for longer than that budget must happen **before** heartbeats stop, or it is not a transfer, it is an outage.
- **Readiness is asked outside the window.** A target mid-demote holds its supervisor lock for seconds; `elect_readiness` waits that out while the leader is still heartbeating and refuses the transfer outright if the target cannot serve. `trigger_elect` re-checks the same condition inside the window under a short bound, because only "did this change during the drain" is still open by then.
- **Cache**: none. Readiness is re-derived on both calls; the second is not trusted to the first.
