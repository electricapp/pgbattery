# UPGRADE.md — Upgrade Design Spec

Status: proposal. Nothing in this document is implemented unless explicitly
marked "exists today". Two tracks, independently shippable:

- Track A: rolling pgbattery self-upgrades (minor and major) with no cluster
  downtime.
- Track B: PostgreSQL major version upgrades orchestrated end to end
  (`pgbattery upgrade postgres --to <major>`).

Track A is a precondition for Track B being pleasant (the orchestrator should
be current before it drives a PG upgrade), but neither blocks the other.

## What exists today

- `pgbattery upgrade` replaces the local binary from a release URL with
  cosign keyless verification (Fulcio identity + Rekor inclusion), semver
  anti-rollback, and `--check` dry-run mode.
- `POST /api/v1/cluster/transfer-leadership/{id}` moves leadership on demand.
- The supervisor owns the full PG process lifecycle: initdb, basebackup,
  promote/demote, pg_rewind, verify_promotion_safe().
- The join/rejoin path re-provisions a standby from the leader via basebackup.
- `pgbattery doctor` runs cluster-wide preflight checks with `--strict`.
- Backup/restore with pg_verifybackup, retention, and a management API.
- The gateway routes clients to the current leader and survives failover.

## Track A: rolling pgbattery self-upgrades

### Goal

`pgbattery upgrade --cluster` upgrades every node of a 3-node cluster to a
target release with zero write-unavailability beyond one leadership transfer,
and refuses to start when the version jump is outside the supported
compatibility window.

### Non-goals

- Skipping major versions in one step (N to N+2). The operator runs the
  ladder; the tool enforces it.
- Automatic unattended upgrades. Every rollout is operator-initiated.

### Orchestration

Thin loop over existing APIs, run from any operator machine:

1. Preflight: `doctor --strict` green; all nodes report the same current
   version (mixed starting versions abort); replication SYNC; recent backup
   exists or `--allow-no-backup`.
2. For each follower, in lag order (most caught-up last):
   a. Upgrade the binary on that node (existing single-node `upgrade` path).
   b. Restart pgbattery; wait until the node rejoins Raft and its standby is
      streaming again (`/cluster/node/{id}/lag` below threshold).
3. `transfer-leadership` to an already-upgraded follower.
4. Upgrade the former leader (now a follower) the same way.
5. Postflight: `doctor --strict`; all nodes report the target version.

Each step is idempotent and the loop is resumable: a re-run detects
already-upgraded nodes by version and skips them. No state is stored about
the rollout itself — the cluster is the state (same discipline as
docs/STATE_MACHINE.md: derive, do not cache).

### Compatibility guarantees (the real work)

Rolling upgrades mean mixed versions on the wire and on disk. Two contracts
must become explicit and tested:

1. Raft RPC framing. The correlation-ID framed transport gets a protocol
   version byte in the frame header. Rule: version N speaks the frame format
   of N and accepts N-1; a node receiving a newer major frame version replies
   with a typed "version too new" error instead of a decode failure. This
   bounds the supported window to adjacent releases and makes violations loud.
2. redb store schema. The store gets a schema-version key written at create
   time. On open: equal version proceeds; older version runs forward
   migrations (idempotent, applied before the node joins Raft); newer version
   refuses to start with an explicit "downgrade not supported" error. The
   binary's semver anti-rollback already refuses downgrades at install time;
   the store check is defense in depth for hand-copied binaries.

Policy: patch/minor releases must not change frame format or store schema
(CI-enforced by a compat test that runs current HEAD against the previous
release's serialized frames and store fixtures). Major releases may change
either, with migrations, and support exactly the N-1 to N window.

### Failure handling

- A node that fails mid-upgrade is left down, not half-up: the upgrade
  replaces the binary atomically (existing rename dance) and the restart
  either rejoins or the loop stops with that node's doctor output. The
  cluster keeps quorum with the remaining two nodes.
- The loop never proceeds past a node whose standby has not re-attained
  streaming replication.
- Rollback of a follower = install previous release (allowed while store
  schema is unchanged, i.e. always within a minor line; documented as
  unsupported across majors — restore from backup instead).

### Testing

- ci_matrix case: rolling upgrade under continuous writes; assert zero lost
  acked writes and exactly one leadership transfer.
- Mixed-version soak: leader on N-1, followers on N, run the parallel HA
  suite unchanged.
- Frame/store compat fixtures generated at release-tag time and checked
  against HEAD in CI.

### Estimate

Days. Orchestration loop 1-2 days; frame version byte + store schema key
1-2 days; compat fixtures + matrix cases 1-2 days.

## Track B: PostgreSQL major version upgrades

### Goal

`pgbattery upgrade postgres --to 19` takes a healthy 3-node cluster from PG
major N to major 19 with application-visible impact bounded to one
gateway-held pause (target: under a minute for catalog-sized upgrades via
`pg_upgrade --link`), no connection-string changes, and a verified rollback
path until the operator confirms success.

### Non-goals

- Logical-replication blue-green (near-zero-downtime) upgrades. That is a
  future track; this spec is the pg_upgrade path.
- Downgrades after confirmation. Rollback exists only until `--confirm`.

### Prerequisites

- Both PG major binary dirs present on every node. Config grows
  `pg_bin_dirs = { "18" = "/usr/lib/postgresql/18/bin", "19" = ... }`;
  the existing `pg_bin_dir` remains as the active pointer.
- Disk headroom check: `--link` needs catalog-scale space only, but the
  rollback copy policy (below) may require more; doctor computes and reports.

### Orchestration

1. Preflight (`--check`, also run implicitly before the real thing):
   - `doctor --strict` green, SYNC replication, quorum healthy.
   - New-version binaries present and executable on every node.
   - `pg_upgrade --check` against the primary's data dir with old+new bins.
   - Extension audit: every installed extension has a control file under the
     new version's sharedir; unknown extensions abort with a named list.
   - Fresh full backup (pg_verifybackup-clean) unless `--allow-no-backup`.
2. Freeze: gateway enters hold mode (below). Demote nothing; stop standbys
   cleanly (their data dirs are about to be replaced anyway).
3. Primary upgrade, on the leader node, driven by its supervisor:
   - Clean shutdown of the primary postmaster.
   - `pg_upgrade --link` old-datadir to new-datadir (new initdb'd datadir on
     the same filesystem). The old datadir is preserved untouched except for
     hard-linked relation files — this is the rollback anchor.
   - Start the new-version postmaster; run the pg_upgrade-emitted
     post-upgrade scripts (analyze-in-stages runs after unfreeze, throttled).
4. Standby re-provisioning: each standby runs the existing join/rejoin
   basebackup path against the upgraded primary (pg_upgrade invalidates
   standby data dirs; rsync tricks are explicitly rejected as too fragile).
   Standbys come back one at a time; SYNC is restored before the second one
   starts to keep the window with a single sync candidate short.
5. Unfreeze: gateway releases held connections once the new primary accepts
   writes (before standby re-provisioning completes — durability is briefly
   ASYNC and the status output says so).
6. Verification gate: replication SYNC on both standbys, doctor green,
   `SELECT version()` agreement, analyze-in-stages complete.
7. `--confirm` (explicit operator action, or `--auto-confirm-after <dur>`):
   deletes the old data dirs and flips `pg_bin_dir` permanently. Until then,
   rollback is available.

### Gateway hold mode

During step 2-5 the gateway does not refuse or reset client connections; it
holds new queries at the protocol level (accepting connections, delaying
query responses) up to `upgrade_hold_timeout` (default 60s, after which
clients get the same clean error as an ordinary failover). Existing sessions
see a stall, not a disconnect — the same contract as leadership transfer.
This is what makes the upgrade look like a long failover instead of an
outage.

### Rollback

Available any time before `--confirm`:

- Stop new-version postmaster on the primary; the old data dir (preserved by
  `--link` semantics: never started under the new version) starts again under
  the old binaries. Standbys re-basebackup from it.
- If the old primary datadir is unusable (should not happen; `--link` does
  not modify it once the new cluster has been started we treat it as
  tainted and refuse automatic rollback), restore the preflight backup.
- Writes accepted on the new version after unfreeze are lost on rollback.
  The command prints the exact LSN watermark at unfreeze and requires
  `--rollback --accept-write-loss` once any write has landed.

### State machine impact

This adds an explicit supervisor mode (UPGRADING with sub-states FREEZE,
PRIMARY_UPGRADE, STANDBY_REPROVISION, VERIFY) and a gateway routing state
(HOLD). Per the mandatory rule, docs/STATE_MACHINE.md gains these states,
their truth sources (pg_upgrade exit status, postmaster version probe,
replication state — all probed, never cached), and the abort transitions,
in the same commit as the implementation.

### Testing

- ci_matrix suite `pg-major-upgrade`: full 18-to-19 upgrade under continuous
  gateway writes; assert zero lost acked pre-freeze writes, bounded hold
  duration, SYNC restored, version agreement.
- Chaos variants: kill a standby mid-reprovision; kill pgbattery on the
  primary mid-pg_upgrade (must resume or roll back cleanly on restart);
  disk-full during basebackup.
- Rollback drill as a first-class CI case, including the write-loss
  watermark refusal.
- Extension-abort case: install a dummy extension with no new-version
  control file; preflight must name it and abort.

### Estimate

2-4 weeks. Gateway hold mode ~3-4 days; supervisor orchestration + dual
bin-dir config ~1 week; rollback semantics + confirm flow ~3-4 days; chaos
test suite ~1 week. The risk concentrates in hold-mode edge cases and
mid-upgrade crash recovery, not in the pg_upgrade mechanics.

## Sequencing recommendation

1. Track A compatibility contracts (frame version byte, store schema key) —
   small, and they de-risk every future release immediately.
2. Track A orchestration loop.
3. Track B preflight (`upgrade postgres --check`) — independently useful as
   a "can I upgrade" doctor extension, ships value before the full flow.
4. Track B full flow behind an explicit experimental flag, chaos-tested.
5. Track B gateway hold mode last: the flow works with plain
   refuse-during-window semantics first, hold mode upgrades the experience.
