# UPGRADE.md — Upgrade Design Spec

Status: proposal. Nothing in this document is implemented unless explicitly
marked "exists today". Three tracks, independently shippable:

- Track A: rolling pgbattery self-upgrades (minor and major) with no cluster
  downtime.
- Track B: PostgreSQL major version upgrades orchestrated end to end via
  `pg_upgrade` (`pgbattery upgrade postgres --to <major>`). One gateway-held
  pause, measured in seconds to minutes.
- Track C: PostgreSQL major version upgrades via logical replication between
  two clusters, with a cutover measured in seconds and a rollback that stays
  available after the cutover.

Track A is a precondition for Track B being pleasant (the orchestrator should
be current before it drives a PG upgrade), but neither blocks the other.

Track B and Track C are not alternatives to pick between once. They suit
different databases: B's pause scales with catalog size and is the right answer
for most clusters, C's does not scale with data size at all and is the only
answer for a database where even a minute of held writes is unacceptable. B is
also a prerequisite in practice — C's green cluster needs the dual-bin-dir
config, the extension audit, and the version-pair preflight that B builds.

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
`pg_upgrade --clone`), no connection-string changes, and a verified rollback
path until the operator confirms success.

### Non-goals

- Near-zero-downtime cutover. The pause here is bounded but real, and it grows
  with catalog size. Track C is the path for databases that cannot take it.
- Downgrades after confirmation. Rollback exists only until `--confirm`.

### Prerequisites

- Both PG major binary dirs present on every node. Config grows
  `pg_bin_dirs = { "18" = "/usr/lib/postgresql/18/bin", "19" = ... }`;
  the existing `pg_bin_dir` remains as the active pointer.
- Old and new data directories on the same filesystem, which every transfer
  mode except plain copy requires.
- Disk headroom, computed and reported by doctor against the selected transfer
  mode (see below): clone and link need catalog-scale free space, copy needs a
  second full copy of the database.

### Transfer mode

`pg_upgrade` transfers relation files in one of four modes, and the choice is
the difference between having a rollback path and not having one. pgbattery
selects `--clone`, falls back to `--copy`, and accepts `--link` only when the
operator has explicitly given up rollback.

| Mode      | Speed                       | Space           | Old cluster after the new one starts |
| --------- | --------------------------- | --------------- | ------------------------------------ |
| `--clone` | near-instant                | catalog-scale   | usable                               |
| `--copy`  | scales with data            | second full set | usable                               |
| `--link`  | near-instant                | catalog-scale   | unusable                             |
| `--swap`  | fastest with many relations | none            | destroyed during transfer            |

Clone mode uses filesystem copy-on-write (reflinks), so the new cluster's
writes allocate new blocks instead of mutating the old cluster's. The upstream
documentation is explicit that clone "provides the same speed and disk space
advantages but does not cause the old cluster to be unusable once the new
cluster is started." It requires reflink support: Linux with Btrfs, or XFS
created with `reflink=1`; macOS with APFS. Doctor probes for it rather than
inferring it from the filesystem name, because XFS without `reflink=1` looks
identical until the clone fails.

Link mode hard-links relation files, so the two clusters share inodes and every
write the new cluster makes is a write to the old cluster's data. This is not a
subtlety to work around — it is why `pg_upgrade` renames the old cluster's
`global/pg_control` to `pg_control.old` when linking starts, and why the
documentation says "you will not be able to access your old cluster once you
start the new cluster after the upgrade." A datadir upgraded with `--link` is
a rollback anchor only in the window before the new postmaster has started,
which is a window this flow deliberately leaves. Selecting link mode therefore
requires `--accept-no-rollback`, which turns the preflight backup from
skippable into mandatory (`--allow-no-backup` is refused alongside it) and
makes restore-from-backup the only abort path past step 3.

Swap mode is never selected. It is destructive from the first moment of file
transfer, which is strictly worse than link mode for this flow's purposes.

### Orchestration

1. Preflight (`--check`, also run implicitly before the real thing):
   - `doctor --strict` green, SYNC replication, quorum healthy.
   - New-version binaries present and executable on every node.
   - `pg_upgrade --check` against the primary's data dir with old+new bins.
   - Extension audit: every installed extension has a control file under the
     new version's sharedir; unknown extensions abort with a named list. A
     control file proves the extension was packaged for the new major, not
     that its `.so` is ABI-compatible, so the audit is a floor and the
     post-upgrade verification gate re-checks that each extension loads.
   - Transfer mode resolved and probed: reflink support confirmed by an actual
     clone attempt in the target directory, not by filesystem type.
   - Fresh full backup (pg_verifybackup-clean) unless `--allow-no-backup`,
     which `--accept-no-rollback` forbids.
2. Freeze: gateway enters hold mode (below). Demote nothing; stop standbys
   cleanly (their data dirs are about to be replaced anyway). The cluster now
   has no redundancy, not merely no availability: from here until a standby is
   back in step 4, a disk failure on the leader is recoverable only from the
   preflight backup. This is the window the flow is trying to keep short, and
   it is the reason step 4 restores SYNC before doing anything else.
3. Primary upgrade, on the leader node, driven by its supervisor:
   - Clean shutdown of the primary postmaster.
   - `pg_upgrade --clone` old-datadir to new-datadir (new initdb'd datadir on
     the same filesystem). Copy-on-write leaves the old datadir a complete,
     startable PG N cluster — this is the rollback anchor.
   - Start the new-version postmaster; run the pg_upgrade-emitted
     post-upgrade scripts.
   - Planner statistics: PG 18 and later transfer most optimizer statistics
     during pg_upgrade. Earlier targets transfer none, and even on 18+ the
     transfer excludes `CREATE STATISTICS` objects, extension-supplied
     statistics, and the cumulative statistics system. The flow runs
     analyze-in-stages after unfreeze in both cases, but only treats it as
     release-gating for targets below 18, where arriving on an unanalyzed
     cluster means plan regressions that are an effective outage well after
     the hold window has closed. Which case applies is probed from the target
     binary's version, not assumed from the config.
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

Available any time before `--confirm`, and only because the transfer mode was
chosen to make it so.

Under `--clone` or `--copy`:

- Stop the new-version postmaster on the primary. The old data dir is a
  complete PG N cluster whose blocks the new cluster never wrote to, so it
  starts again under the old binaries. Standbys re-basebackup from it.
- Rollback is verified, not assumed: the old cluster is started and probed
  (accepts a write, reports the expected version, catalogs consistent) before
  the gateway is pointed back at it. A rollback path that has never been
  exercised is the same failure mode as an oracle that has never failed.

Under `--link` (only reachable via `--accept-no-rollback`):

- There is no in-place rollback once the new postmaster has started. The
  clusters share inodes, so the new version has already written through the
  hard links into the old cluster's relation files, and `pg_upgrade` has
  renamed `global/pg_control` to stop anyone from starting it anyway. The only
  abort path is restore-from-backup, and the flow refuses to pretend
  otherwise: `--rollback` reports the mode, names the backup it will restore,
  and requires `--accept-write-loss` covering everything since the backup, not
  just since unfreeze.

In every mode:

- Writes accepted on the new version after unfreeze are lost on rollback. The
  command prints the exact LSN watermark at unfreeze and requires `--rollback
--accept-write-loss` once any write has landed past it.
- Crash mid-`pg_upgrade` does not resume. `pg_upgrade` has no restart point;
  a new-version datadir left behind by an interrupted run is discarded, not
  repaired. On restart the supervisor either re-runs the upgrade from the
  preserved old datadir or rolls back, and it decides by probing which
  datadirs exist and which postmaster has ever started — never by a marker it
  wrote about its own intent.

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
  primary mid-pg_upgrade (must restart the upgrade or roll back cleanly, and
  must never present a half-transferred datadir as usable); disk-full during
  basebackup.
- Rollback drill as a first-class CI case: clone-mode rollback must start the
  old cluster and accept a write, and the write-loss watermark refusal must
  fire.
- Transfer-mode cases: a filesystem without reflink support must fall back to
  copy rather than fail; `--link` without `--accept-no-rollback` must refuse;
  `--link --accept-no-rollback` must refuse `--allow-no-backup`; and a
  link-mode rollback attempt must report no in-place path rather than
  attempting one.
- Extension-abort case: install a dummy extension with no new-version
  control file; preflight must name it and abort.

### Estimate

4-6 weeks. Gateway hold mode ~3-4 days; supervisor orchestration + dual
bin-dir config ~1 week; transfer-mode selection, probing, and the rollback
and confirm flows ~1 week; chaos and rollback test suite ~2 weeks. The risk
concentrates in hold-mode edge cases and mid-upgrade crash recovery, not in
the pg_upgrade mechanics. The earlier 2-4 week figure assumed rollback was
free because `--link` preserved the old datadir; it does not, and building a
rollback path that is actually exercised is most of the difference.

## Track C: logical blue/green major upgrades

### Goal

`pgbattery upgrade postgres --to 19 --blue-green` stands up a second three-node
pgbattery cluster at the new major, streams the live database into it by
logical replication, and cuts write authority over in a window measured in
seconds regardless of database size — with the old cluster left running, fully
caught up in reverse, and available as a rollback target after the cutover.

### Why this is the differentiator

Physical replication cannot cross a major version. WAL is a physical log of
page-level changes, tied to the on-disk page layout and the catalog version;
a standby checks the new WAL's version against its own and refuses. That is
why every HA tool's answer to "upgrade my major version" ends in downtime.
Patroni does not orchestrate major upgrades at all. Track B narrows the
downtime to a bounded pause but cannot remove it, because the pause is where
`pg_upgrade` runs.

Logical replication does cross versions, because logical decoding reconstructs
row-level changes from WAL and emits them in a version-independent protocol.
So a blue/green upgrade is possible for anyone. What is hard, and what almost
nobody gets right, is the cutover: for a window spanning two independent
database clusters, something has to guarantee that exactly one of them accepts
writes. Get it wrong in the safe direction and you have an outage; get it wrong
in the unsafe direction and you have a split-brain across two clusters, with
divergent data and no shared log to reconcile it from.

That guarantee is the thing pgbattery already builds. Contract L1 — at most one
node in write-accepting state — is enforced by a lease over a Raft log, with a
fence that escalates to process exit, an oracle (`dual_writability_prober.py`)
that writes concurrently to every node to try to catch two acceptances, and an
inversion proving that oracle can fail. The gateway routes from the same truth
source, so clients follow authority instead of racing it.

Owning both layers is what makes the cutover tractable. A tool that owns only
the control plane has to ask the operator to move the connection string, and
cannot bound the moment the old cluster stops being written to. A tool that
owns only the proxy has no committed log to order the handoff in. pgbattery has
both, so the handoff can be an entry in a replicated log that the gateway reads
— which turns "did the old cluster really stop?" from a question about timing
into a question about a committed fact.

The comparison users will actually make is RDS Blue/Green. That switches over
in about a minute and, once switched, the green cluster is the database; the
blue one is frozen at the cutover point, so rolling back means losing
everything written since. Reverse replication after cutover is the feature
worth building, and it falls out of the same machinery: authority handoff is
symmetric, so rolling back is the same protocol run the other way.

### Non-goals

- Upgrading in place. Track C needs the hardware for two clusters at once.
- Cross-version upgrades of databases the restrictions below rule out. The
  preflight rejects them by name and points at Track B, rather than degrading
  into a partial migration discovered at cutover.
- Skipping Track B. The version-pair preflight, dual-bin-dir config, and
  extension audit are shared, and Track C needs them for the green cluster.

### The dual-cluster write authority problem

Two pgbattery clusters means two Raft groups. Each will independently elect a
leader and grant a lease, and nothing orders events between them. Running both
with the current design is not "L1 at a larger granularity" — it is a direct
L1 violation with no mechanism that could detect it, because each group's log
is internally consistent and neither can see the other.

The fix is to refuse to create a second authority. There is one authority, it
is versioned by an epoch, and the epoch advances only through entries committed
in **blue's** Raft log, which is a total order. Green never elects itself into
write authority; it receives authority, and the record of receiving it lives in
the log of the cluster that gave it up.

Concretely, the authority identity widens from `node_id` to
`(epoch, cluster_id, node_id)`. Cutover is four committed steps:

1. `CUTOVER_ARMED{green_cluster_id}` — committed in blue's log. Nothing changes
   for clients. Green's supervisor learns which cluster it is chasing, and
   blue's gateways learn a handoff is pending.
2. `WRITE_AUTHORITY_SEALED{final_lsn}` — blue's leader stops accepting client
   writes and records its final flush LSN. This reuses the existing fence path,
   so it inherits L2's testing rather than introducing a parallel mechanism.
   The entry commits before any write can land on green.
3. Green drains. Its supervisor waits until the subscription's applied origin
   LSN has reached `final_lsn`, then advances every sequence past blue's
   current values, then runs the equality checks. Green is still read-only.
   Drain progress is polled from green's catalogs — observed, never assumed.
4. `AUTHORITY_TRANSFERRED{green_cluster_id, epoch+1}` — committed in blue's log
   on green's proof of drain. Only now does green's supervisor lift its fence.
   From here green's own Raft group owns the lease for the new epoch, and
   blue's log holds the immutable record that it relinquished the old one.

Gateways on both sides route to the highest epoch they can confirm from a
committed source. The invariant generalizes cleanly: **write authority exists
for exactly one `(epoch, cluster)` pair, and epochs advance only through blue's
log.**

The failure cases are where this earns its keep, and all of them fail closed:

- **Blue unreachable mid-cutover.** Green does not take authority; no entry
  names it. The database is write-unavailable until blue is reachable. This
  trades availability for L1, which is the same trade quorum-loss fencing
  already makes.
- **Blue loses quorum between SEAL and TRANSFER.** Blue can commit nothing,
  green waits. Recovery is an explicit, logged, single-shot operator action
  (`--force-authority-transfer --accept-blue-write-loss`). It is deliberately
  not a timeout: a timer here is not merely the repo's refused anti-pattern for
  state transitions, it is the exact mechanism by which this design would
  produce a two-cluster split-brain.
- **Green fails to drain.** Blue is still sealed. The abort is to unseal blue
  into a _new_ epoch — safe precisely because green never wrote — and it is
  itself a committed entry, so the epoch stays monotonic and no gateway can
  confuse a pre-seal view with a post-abort one.
- **A gateway that cannot confirm the current epoch** holds or refuses. It
  never guesses, and it never falls back to a cached epoch.

One piece here is genuinely new rather than a generalization, and the design
should not pretend otherwise. Today's fence works by demoting PG to a standby.
Blue cannot be a standby after cutover, because it has to run a logical
replication apply worker to receive the reverse stream — a cluster in recovery
cannot. So Track C needs a fence that blocks _client_ writes while permitting
_apply_ writes on a primary. Gateway-level blocking is not sufficient by the
project's own standard: `dual_writability_prober.py` writes directly to the
internal PG ports, and it is right to. The mechanism should be role-based —
revoke write privileges from application roles, leave them with the
subscription owner — because it is durable across restarts, independent of the
gateway, and draws the line exactly where it needs to be drawn. It is a new
fencing primitive with its own verification burden, and the prober has to be
extended to prove it holds.

### Prerequisites and the restrictions that decide feasibility

Logical replication does not carry everything, and each gap is a way for a
migration to look healthy and be wrong. Every one of these is a preflight
check that aborts by name, not a runtime surprise:

- **Sequences are not replicated.** Serial and identity column _values_ arrive
  inside the replicated rows, but the sequence objects on green still sit at
  their start value. Cut over without advancing them and green issues primary
  keys that blue already used — duplicate-key errors, or worse, silent
  collisions on tables without the constraint. This is the archetype of the
  failure this repo cares about: it passes while blind, and surfaces later as
  data corruption. Step 3 advances every sequence past blue's value with a
  safety margin, and the verification gate checks it.
- **DDL is not replicated.** Blue must be under a schema freeze for the whole
  migration window. Enforced, not requested: an event trigger on blue that
  rejects DDL from application roles for the duration, plus a schema digest
  compared between the clusters as part of the drain proof.
- **Large objects are not replicated at all.** Preflight aborts if
  `pg_largeobject` is non-empty and points at Track B.
- **Views, materialized views, and foreign tables cannot be members of a
  publication.** They come across with the schema dump; materialized view
  contents need refreshing on green.
- **Unlogged tables** produce no WAL and so replicate nothing. Preflight names
  them; the operator decides whether empty-on-green is acceptable.
- **Replica identity.** Tables without a primary key need `REPLICA IDENTITY
FULL` or a suitable unique index, and even with FULL, `UPDATE` and `DELETE`
  cannot be applied for tables with columns whose types lack a default btree
  or hash operator class. Preflight enumerates every publishable table and
  checks it can actually carry updates.
- **TRUNCATE** replicates, but fails if a truncated table has foreign-key
  links to tables outside the subscription. Publish whole schemas, not table
  subsets.
- **Constraints and triggers on green are live during apply.** Foreign keys
  and check constraints are enforced against arriving rows and can stall the
  subscription; row triggers do not fire unless `ENABLE REPLICA`. Both
  directions of that are surprises, so preflight reports them.
- **WAL retention on blue.** A stalled subscriber holds blue's slot back and
  can fill its disk. The supervisor monitors slot lag as a first-class health
  signal and aborts the migration long before blue is at risk, rather than
  letting a green-side problem become a blue-side outage.

Two upstream features make this much cheaper and are worth depending on where
available. `pg_createsubscriber` (PG 17+) converts a physical standby into a
logical subscriber, skipping the initial data copy entirely — which is the
expensive part for a large database. It requires the standby to be at the same
major version as its source, so the flow is: build a physical standby of blue,
convert it, then upgrade that now-independent cluster to the new major. And
`pg_upgrade` from PG 17 or later preserves logical slots and subscription state
across the major upgrade, so the stream survives the step that creates green.
Below PG 17 both are unavailable and green is built by schema dump plus a full
initial copy; the flow supports that path and says which one it took.

### Orchestration

1. Preflight: everything above, plus Track B's version-pair and extension
   checks against the green binaries, plus capacity for a second cluster.
2. Build green: a normal three-node pgbattery cluster at the new major,
   bootstrapped and healthy on its own Raft group, fenced read-only from
   client roles from the moment it starts. It is never a candidate for write
   authority except through the cutover protocol.
3. Seed and stream: `pg_createsubscriber` from a physical standby of blue where
   available, otherwise schema dump plus `CREATE SUBSCRIPTION`. Green's
   internal replication is ordinary same-version physical replication, so
   everything below the primary is unchanged pgbattery.
4. Steady state: green tracks blue, lag is exported alongside the existing
   replication metrics, and the schema freeze is enforced. This state is
   indefinitely holdable; the operator picks the cutover moment.
5. Cutover: the four-step protocol above.
6. Reverse: green becomes publisher, blue becomes subscriber, immediately on
   transfer. Blue stays a live PG N cluster tracking green.
7. `--confirm`: tears down reverse replication and decommissions blue. Until
   then, rollback is the same protocol with green's log as the ordering point.

### Rollback

Before cutover, rollback is free — green has never held authority, so tearing
it down changes nothing about blue.

After cutover it is the differentiating capability, and it works because of
step 6: blue is not frozen at the cutover LSN, it is a live cluster continuously
receiving green's writes. Rolling back runs the same four steps with the roles
exchanged and green's log as the ordering point. The window is bounded by
honesty about what reverse replication cannot carry: the moment green's schema
diverges from blue's — a new-major-only type, an index blue cannot express, any
DDL at all — the reverse stream is no longer a viable rollback target. The
supervisor tracks that and reports rollback as available or forfeited with the
reason, rather than presenting a path that would fail when used.

### State machine impact

Substantial, and per the mandatory rule `docs/STATE_MACHINE.md` gains all of it
in the same commit as the implementation:

- Authority identity widens from `node_id` to `(epoch, cluster_id, node_id)`,
  with blue's Raft log as the sole truth source for the epoch.
- A cutover state machine (ARMED, SEALED, DRAINING, TRANSFERRED, ABORTED) whose
  transitions are committed log entries, not in-process state.
- A new fence mode: client-write-fenced primary, distinct from the existing
  demote-to-standby fence, with role grants as its truth source. Probed from
  the catalog, never cached.
- Gateway routing gains an epoch dimension, and the fail-closed rule when the
  current epoch cannot be confirmed.
- Subscription health (applied origin LSN, slot lag, apply errors) becomes a
  probed input to the cutover gate. It is PG state, so it is probed, not
  assumed — the same discipline the doc already applies to replication state.

### Contracts

Track C needs its own contract IDs in `docs/CONTRACTS.md`, each with the
inversion that `lint_matrix.py` requires for FATAL rows:

- **U1 — Cross-Cluster Single Write Authority (FATAL).** At most one
  `(epoch, cluster)` accepts client writes at any point. The generalization of
  L1, and the reason the epoch lives in a single log. Inversion: feed the
  oracle a history with confirmed acceptances on both clusters.
- **U2 — Cutover Drain Completeness (FATAL).** Every write acked by blue before
  SEAL is present on green after TRANSFER. W1 across the cutover boundary.
  Inversion: a history missing a pre-seal ack.
- **U3 — Sequence Non-Collision (FATAL).** After cutover, no sequence on green
  issues a value blue already issued. Inversion: a green cluster whose
  sequences were deliberately not advanced.
- **U4 — Fail-Closed Cutover (FATAL).** When the current epoch cannot be
  confirmed from a committed source, no node accepts writes. Inversion: an
  unreachable blue with a green that took authority anyway.

### Testing

- `dual_writability_prober.py` generalizes directly and is the primary U1
  oracle: it already writes concurrently to every internal PG port and asserts
  at most one acceptance. Point it at all six ports across both clusters and it
  covers the cross-cluster case with no change to its logic — including its
  existing discipline that an unreachable node is `indeterminate`, not
  "no acceptance".
- A six-node compose file, derived through `topology.py` like every other
  topology, so no harness restates addresses or ports.
- ci_matrix suite `pg-blue-green`: full cutover under continuous gateway
  writes; assert zero lost acked writes across the boundary, bounded cutover
  duration, sequences advanced, green SYNC restored.
- Chaos at every step of the protocol, since each is a distinct split-brain
  opportunity: kill blue's leader between ARMED and SEALED; partition the
  clusters between SEALED and TRANSFERRED; kill green's primary mid-drain;
  lose blue's quorum after SEAL and assert green stays read-only until the
  explicit break-glass.
- Post-cutover rollback drill as a first-class case, and a forfeited-rollback
  case where DDL on green must flip the reported status.
- Preflight abort cases, one per restriction: a table with no replica identity,
  a non-empty `pg_largeobject`, an unlogged table, a DDL attempt during the
  freeze.
- A TLA+ spec for the cutover protocol. `lease_fencing` already proves single
  write authority within a cluster; the cross-cluster epoch handoff is a new
  protocol with new failure interleavings, and it is exactly the kind of thing
  that is cheap to model and expensive to debug in a cluster.

### Estimate

3-6 months, and the range is wide because the protocol is the risk, not the
plumbing. Green cluster provisioning and the logical seeding reuse existing
paths and are weeks. The cutover protocol, the new fence mode, the epoch-aware
gateway, the TLA+ spec, and a chaos suite that actually exercises every
interleaving are the bulk of it, and this project's standard is that a fault
which cannot fail is worse than no test.

Anyone quoting weeks for this is quoting the happy path.

## Sequencing recommendation

1. Track A compatibility contracts (frame version byte, store schema key) —
   small, and they de-risk every future release immediately.
2. Track A orchestration loop.
3. Track B preflight (`upgrade postgres --check`) — independently useful as
   a "can I upgrade" doctor extension, ships value before the full flow.
4. Track B full flow behind an explicit experimental flag, chaos-tested.
5. Track B gateway hold mode last: the flow works with plain
   refuse-during-window semantics first, hold mode upgrades the experience.
6. Track C preflight, which is most of the feasibility answer and ships on its
   own: `upgrade postgres --to N --blue-green --check` tells an operator
   whether their database can take the logical path and names every blocker.
   Useful even to someone who then does the migration by hand.
7. Track C cutover protocol, specified and model-checked in TLA+ before it is
   implemented. The protocol is the product here; the provisioning around it
   is ordinary work.
8. Track C end to end, then reverse replication and post-cutover rollback.

Track C's value does not depend on finishing it. Step 6 is a week or two and
answers the question people actually ask, and the epoch-widened authority
identity from step 7 makes the invariant that matters explicit inside a single
cluster too.
