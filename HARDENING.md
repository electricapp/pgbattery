# Hardening Roadmap

Verification work not yet done, ordered by how much confidence it buys. The
prose sections explain each gap and why it matters; the "Execution plan" at the
end is the same content as tracked tasks, and the document is complete when every
box there is checked.

Existing verification is inventoried in `CLAUDE.md`; correctness contracts live
in `docs/CONTRACTS.md`; state-machine truth sources in `docs/STATE_MACHINE.md`.
This document is about what those layers **cannot** currently catch.

## The bar

The goal is that an adversarial distributed-systems reviewer — the Jepsen kind —
finds nothing we did not already know and document. That splits into two
different problems, and only the first is a testing problem:

1. **Anomalies our harness cannot find.** Addressed by the tiers below.
2. **Design properties no amount of green testing answers.** Fencing is
   SQL-level, the internal PostgreSQL port trusts the cluster network, and a
   deposed leader holding a quorum that excludes the election winner is a
   documented open window. These need either a design change or an explicit
   published stance. They are tracked in "Accepted risks" and in the README's
   Known Limits, because a reviewer will otherwise write that section for us.

## Why a bug would escape today

Every gap below is one of three kinds. Sorting by kind is more useful than
sorting by component, because each kind needs a different investment.

### Class A — the fault cannot be produced

There are two versions of this. The second is worse, and it is not theoretical.

**A1: the fault silently fails to inject, and the test passes anyway.** Five
confirmed instances, all found by adding effect verification rather than by any
test going red:

| What                                                                                                                                                        | Consequence                                                                                                           |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `iptables` was never installed in the runtime image                                                                                                         | all three nightly asymmetric-partition cases injected nothing; their `iptables -D ... \|\| true` cleanup no-opped too |
| every fault `exec` ran as unprivileged `postgres` (the image ends `USER postgres`; `NET_ADMIN` is granted to the container, not to an unprivileged process) | `tc netem` could never inject latency — `RTNETLINK answers: Operation not permitted`                                  |
| `elle.yml` set a per-run `COMPOSE_PROJECT_NAME` while the harness addressed docker objects literally                                                        | `partition` and `flap_partition` were no-ops in CI                                                                    |
| `correctness_lite.py` had the same literal names under `correctness-lite.yml`'s per-run project                                                             | the partition fault window was empty, making I5 vacuous for it                                                        |
| `transfer_leader_after` read the API token from a `.env` CI never wrote                                                                                     | `transfer` posted unauthenticated and transferred nothing                                                             |

All five are fixed. The lesson is the durable part: **a fault must verify its own
effect and fail loudly, and the harness must not assume its environment.** Every
one of these passed for months. `docker-compose.yml` sets `name: pgbattery`, so
literal names work locally and only break under CI's per-run project — the worst
possible failure shape.

The related power bug: workers pinned to a killed leader's gateway spun on
connection-refused at roughly 2800 attempts/second, producing 56,600 of 56,665
`:info` records in one CI run while two surviving workers committed every real
transaction. A nightly Elle run advertised ~10^5 operations and delivered ~385
real ones.

**A2: the fault class does not exist.** Every fault the harness injects is a
_clean_ fault: SIGKILL, container stop, network disconnect, SIGSTOP.
`docker kill` leaves the host page cache intact and the kernel still flushes
dirty pages, so:

**Nothing proves `fsync` is honored.** W1 (ACKed Write Durability) and R2
(Synchronous Replica Acknowledgment) are the two most important FATAL contracts
and they currently rest on careful construction — `redb::Durability::Immediate`
on every write path, synchronous replication with a write set that intersects
every Raft majority — rather than on evidence. No test kills a node between
commit-ack and fsync.

Also absent: torn WAL pages, real power loss, resource exhaustion (OOM, CPU
starvation, fd limits, `max_connections` saturation), slow-disk as distinct from
full-disk, and any 5-node topology at all, so quorum arithmetic beyond 3 nodes
and dual-fault tolerance are unexercised.

### Class B — the violation cannot be observed

The silent kind, and the one worth worrying about most.

Split-brain detection in `correctness_lite.py` is _control-plane_: it samples
what nodes **say** about Raft leadership. A deposed leader whose fence has not
landed yet is Raft-consistent and PostgreSQL-writable at the same time, which no
leader-ID-agreement check can see, and a detector sampling at 500 ms cannot
resolve a sub-500 ms window even in principle.

`dual_writability_prober.py` closes that hole for L1 specifically, and only for
L1. It is data-plane: it races real writes at all three internal PostgreSQL
ports on a 50 ms round period and asks the database, not the control plane, which
node accepted. Nine matrix cases invoke it. Its own floor is that round period —
a dual-write window shorter than 50 ms is still invisible — and it measures one
property, so every other contract remains control-plane-observed.

Beyond that: no workload reads through a follower, issues a read-only
transaction, or uses a predicate — so stale reads, long fork, and phantoms are
outside the test universe. Fault timing is hand-tuned sleeps rather than values
derived from the lease and election constants, so the boundary regime where
openraft-0.9-without-pre-vote pathologies live is never swept deliberately.

### Class C — the schedule cannot be explored

There is no deterministic simulation and no way to replay a failure.

The consequence is visible in where tests are _not_: `src/app.rs` holds
`ensure_follows`, `lease_enforcement_tick`, and `promote_local_postgres` — the
split-brain-prevention core — and is the least-tested large file in the repo,
because reaching it requires Docker and a live PostgreSQL. The most
safety-critical logic gets the slowest, least reproducible verification. Each
integration case takes minutes and explores exactly one OS-chosen interleaving,
and a failure found in CI is not replayable.

## Risk-window register

Ranked by expected severity. "Reachable" means the current harness can actually
produce the conditions, not that a test asserts the right thing.

| #     | Window                                                                                                                                                                                                     | Contract   | Reachable today                                                                              |
| ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | -------------------------------------------------------------------------------------------- |
| RW-1  | Deposed leader retains a quorum excluding the election winner; the promotion hold-down is vacuous and safety rests on the quorum-loss self-fence plus sync replication refusing acks                       | L1         | No — needs 5-node asymmetric partition shapes                                                |
| RW-2  | Post-promotion window where `synchronous_standby_names` is cleared before the replication manager reinstates it: commits ack with zero standby acks                                                        | W1, R2     | No — needs injection triggered by protocol state, not a sleep                                |
| RW-3  | Async fallback then `pg_rewind` discarding up to the divergence threshold of genuinely acked WAL. The threshold's justification assumes sync replication is active, which is exactly false during fallback | W1         | Partially — the fallback is reachable, but nothing measures what rewind destroys             |
| RW-4  | Fencing failure tail: wedged postmaster, exhausted connection slots, or a backend in uninterruptible I/O surviving `pg_terminate_backend`                                                                  | L1, L2     | Partially — SIGSTOP of the postmaster exists; the write path during it is unmeasured         |
| RW-5  | Direct writers on the internal PostgreSQL port bypassing the gateway's lease check entirely (`trust` auth on the cluster network)                                                                          | L1         | Yes — `dual_writability_prober` writes all three internal ports at 50 ms resolution          |
| RW-6  | Follower gateway routing writes to a deposed primary during the Raft detection interval, stopped only by the old leader's own lease                                                                        | W1         | Partially — never driven through a _follower_ gateway specifically                           |
| RW-7  | After a long leaderless window every LSN report ages past the staleness threshold and both election and promotion gates fall back to bootstrap-permissive; a node restored from an old backup can win      | L3         | No — the existing case asserts election _succeeds_ (liveness), not that a stale node _loses_ |
| RW-8  | Failover-anchor lifecycle under coalesced watch transitions: a missed clear or missed re-stamp makes the hold-down read an ancient anchor and promote immediately                                          | L1         | Partially — pure functions are unit-tested; the live coalescing race is not                  |
| RW-9  | `demote()` holds the supervisor mutex across stop, rewind, and recovery — the 100 ms lease tick, health watchdog, and LSN reporting all stall behind it                                                    | L1         | No                                                                                           |
| RW-10 | Join and rejoin edges: basebackup against a leader that gets deposed mid-copy, orphan slots pinning WAL, a learner registration surviving a mid-join crash                                                 | R1, V2     | Partially                                                                                    |
| RW-11 | `SetSyncMode` replicated state disagreeing with the live GUC across a leader change, so the election gate uses the loose async threshold while sync is actually active                                     | W1, L3     | No                                                                                           |
| RW-12 | Commit-probe correctness at every byte offset around COMMIT: a wrong answer manufactures a phantom commit or a duplicate retry                                                                             | W1, W2, S1 | Partially — one fixed timing, no sweep                                                       |

The pattern worth noting: the harness is densest exactly where the design is
already strongest, and thinnest where the design documents its own residual
risk.

## Tier 1 — Close the oracles

No new infrastructure. Highest ratio of confidence to effort.

- **Port-granular partitions.** All current partition rules are per-peer-IP, so
  the classic gray split — Raft heartbeats healthy while PostgreSQL streaming is
  dead, or the inverse — is not expressible. Partition by destination port
  (Raft/management vs replication vs gateway) to reach RW-11 and RW-6.
- **Partition shapes beyond node-vs-rest.** Majority-side isolation with the
  observer on the minority side, three-way splits, and a _follower_ as bridge
  (the leader must step down though it still sees one follower).
- **Faults during protocol windows, not at wall-clock offsets.** Trigger
  injection off observed state — mid-rewind, mid-basebackup, inside the
  promotion hold-down, inside the post-promotion sync gap (RW-2) — instead of
  `sleep 4`.
- **Backward clock jumps and sub-second skews.** Current skew is forward-only
  and coarse (+30 s / +300 s). Backward steps are the harder case for lease
  arithmetic, and the interesting regime is near the timeout boundaries.
- **Wiped-node rejoin.** Destroy one node's volume and rejoin under the same
  node id — the canonical Raft persisted-vote violation. Also the asymmetric
  cases: Raft store lost with PostgreSQL data intact, and the reverse.
- **Follower reads and read-only transactions** in the Elle workload, so
  staleness and long-fork anomalies enter the test universe at all.
- **RW-7 as a safety test.** Restore a node from a stale basebackup, keep its
  Raft directory, start the cluster after the staleness threshold has elapsed,
  and assert the stale node cannot win.
- **Consolidate the three fault vocabularies.** `ci_runner.py`'s step handlers,
  `linearizability_register.py`'s attacks, and `fault_primitives.py` are three
  implementations of "partition" / "pause" / "clock skew" with different flags
  and heal paths. The primitive layer now exists and carries effect
  verification, privilege handling, and project-aware name resolution; the other
  two should route through it so a Class A1 bug can only be fixed once.
- **`overnight_test.py` hardening.** It still addresses docker objects
  literally (about 30 sites), so it inherits the Class A1 bug the moment it runs
  under a non-default compose project. It is also local-only, injects 9 of its
  10 faults with no client load in flight, and its health oracle is a substring
  match on CLI output. Wire it to the primitive layer and give it a real oracle.
- **Split `linearizability_register.py`** (about 1,750 lines) along its natural
  seams: history and records, the WGL and weak checkers, the workload loops, and
  the attack table. The prerequisite that made this non-mechanical is done: the
  workload configuration is now a frozen `WorkloadConfig` passed explicitly, so
  a consumer moved to another module can no longer read a stale default and
  silently ignore the CLI. `test_workload_config.py` is the guard, and it fails
  if any of the four names becomes a module global again. Still validate with a
  real Elle run rather than unit tests alone, because a weakened workload
  produces no visible symptom.

## Tier 2 — Differential classifier testing

About a week. This is an _oracle_, not an enumeration, and it is the durable fix
for a bug class that has already produced one real defect (a session advisory
lock held through `pg_try_advisory_lock` was classified migratable, because the
token prefilter matched neither `pg_advisory_lock` nor `pg_advisory_lock_shared`
as substrings — so the gateway silently moved the session to a backend that did
not hold the lock while the client believed it did).

Generate SQL, execute it against a real PostgreSQL session, then snapshot ground
truth from the catalogs:

`pg_prepared_statements`, `pg_cursors`, `pg_locks WHERE locktype = 'advisory'`,
`pg_listening_channels()`, `pg_class WHERE relpersistence = 't'`,
`pg_settings WHERE source = 'session'`.

Assert: **if session state changed, the classifier said non-migratable.** This
finds the whole family automatically instead of one member at a time. It also
guards the opposite error: catalog truth is what shows that large-object
descriptors are _transaction_-scoped, so severing on `lo_open` would over-sever
ordinary reads for no safety gain.

The structural issue it backstops: the classifier is a deny-list with arms for
roughly two dozen of PostgreSQL's ~200 statement node types, and the fallback is
_migratable_ — the unsafe direction. A narrow fail-safe now catches unmodeled
statements that name the temp schema, but the tail is real.

Do **not** flip the fallback wholesale. The ratchet is one-way for the
connection's lifetime and poolers hold connections for hours, so a single
`ALTER TABLE` or `VACUUM` early in a pooled session would permanently disqualify
it — severing every schema-migration, maintenance, monitoring, and `EXPLAIN`
session. It also would not close the deepest hole: `SELECT my_func()` is a
`SelectStmt` and a function body can create a temp table or take a session lock,
so no positive-classification story is sound while function bodies are opaque.

Ordered instead:

1. Add arms for `LoadStmt` (`LOAD 'lib'` is session-local by construction, near
   zero traffic cost) and `CallStmt` (`CALL proc()` is opaque exactly like `DO`,
   but has real cost for stored-procedure-heavy workloads — needs sign-off).
2. Emit a metric labelled by node type whenever the unmodeled arm is reached, so
   the production tail is measured before any policy change.
3. Revisit the wholesale flip only with that data in hand.

## Tier 3 — Prove durability

About a week, and it touches the container image. This is the single most likely
place to find a genuine RPO violation, and exactly what a Jepsen analysis would
reach for.

- **Lost-unfsynced-writes on crash.** LazyFS (what Jepsen uses for this) or
  libeatmydata. The test that matters: open the fsync-drop window, collect acked
  writes, SIGKILL the container, restart, assert every acked write is present.
  That converts W1 and R2 from asserted to demonstrated.
- **Torn writes** via dm-flakey, which also exercises PostgreSQL page checksums
  and redb crash recovery.
- **ENOSPC at the next WAL segment**, as distinct from a blanket volume fill.
  The data volumes are now tmpfs-bounded so a fill can genuinely exhaust them,
  which is the precondition; targeting the exhaustion at WAL-segment allocation
  is the remaining work.

## Tier 3.5 — Concurrency gaps the current tests structurally cannot reach

Cheap to state, and each needs a purpose-built test rather than more coverage of
the same kind.

- **The `snapshot_consistency` race.** openraft's storage conformance suite now
  runs against the production adapters, but it drives `build_snapshot` and
  `install_snapshot` sequentially, so the mutex is taken and never contended.
  The interleaving it exists to prevent — a builder observing the installer's
  redb write before its state swap, emitting a snapshot whose meta is ahead of
  its data — is not covered. Needs a concurrency test, not a conformance suite.
- **Real power loss below redb.** The crash-recovery tests simulate process
  death via pre-write MVCC snapshots and reopen-from-file, which pins our
  transaction boundaries. They do not cover loss between redb's fsync and the
  platter, or a torn write inside redb's two-slot commit protocol. That is
  Tier 3 territory.
- **Host-to-bridge routability for the prober's `direct` transport** is unproven
  on macOS (Docker Desktop cannot route the compose subnet), so it auto-falls
  back to `docker-exec` locally. The Linux CI path rests on standard bridge
  behaviour rather than a measurement.

## Tier 4 — Deterministic simulation

Multi-week, and the actual next level. It is what buys reproducibility and
schedule coverage that Docker structurally cannot.

The work is the seams, not the simulator, and two of three already exist:
`LeaseState` reads time through an injectable `Clock`, and
`src/governor/network.rs` is the transport seam. What is missing is a `PgProbe`
trait over `probe_role_and_readonly` / `promote` / `demote` /
`pg_stat_replication`, plus clock injection everywhere rather than in the lease
alone.

Then run the governor and `app.rs` orchestration against a model PostgreSQL and
a seeded network controlling delay, reorder, drop, and clock advance. `madsim`
fits better than `turmoil` because time control is required, not just network
control.

Payoff: thousands of failover schedules per second, seed-reproducible failures,
and interleavings the OS will never pick — aimed directly at `app.rs`, the
least-tested and most dangerous file in the repo (Class C).

## Tier 5 — Connect the specs to the binary

The four TLA+ specs are good and genuinely non-vacuous (removing the hold-down
assumption produces the split-brain counterexample; `raft_lsn.cfg` documents
which invariants TLC deliberately disproves). But the spec-to-code mapping is
_comments_, so nothing detects drift.

- **Trace validation.** Emit structured state-transition events (lease
  renew/expire, anchor stamp/clear, promote/demote, fence escalation) and check
  real harness runs against the specs. This turns "we have specs" into "the
  specs describe this binary", and directly closes the known delta where the
  model anchors the hold-down at the election instant while the implementation
  anchors at local leader-loss observation.
- **Model partial-quorum dynamics** in `lease_fencing.tla`, which currently
  states plainly that it does not — the regime of RW-1.
- **Larger models nightly**: 5 nodes, higher term bounds. Current configs are 3
  nodes with small bounds, and `raft_lsn.tla` abstracts the Raft log away
  entirely, so its `ElectionSafety` is the textbook Raft theorem rather than a
  statement about openraft.

## Tier 6 — Real Jepsen, or autonomous exploration

A real Jepsen run is a moderate lift rather than a rewrite: the histories are
already in Jepsen format and Elle is already the checker. What is missing is a
`jepsen.db` setup/teardown harness, 5 nodes on LXC or VMs (which is also what
unlocks the RW-1 partition shapes), and Jepsen's own nemesis library.

Antithesis-style autonomous exploration under a deterministic hypervisor is the
commercial shortcut to the same place.

Full FoundationDB-style simulation — all IO behind swappable traits from the
start — is the one thing not worth retrofitting at this stage; Tier 4 captures
most of its value.

## Accepted risks

Deliberate, documented, and not scheduled. Revisit if the deployment model
changes.

- **Fencing is SQL-level.** `default_transaction_read_only` plus client-backend
  termination requires PostgreSQL to still answer its supervisor. A wedged
  postmaster escalates to process exit and container restart, so the container
  runtime is the real backstop. Outside a restart-policy environment, a writable
  zombie primary can persist.
- **`trust` auth on the internal port.** The fencing perimeter assumes only the
  gateway and cluster peers reach PostgreSQL directly. This is a deployment
  requirement, not a guarantee the binary enforces.
- **Async fallback trades RPO for availability.** With no healthy standby for the
  disconnect timeout, the leader drops to asynchronous replication and the
  published RPO becomes non-zero.
- **LSN staleness uses the wall clock** because those timestamps ride in
  replicated Raft state and snapshots, where a monotonic `Instant` would be
  meaningless. Future skew is capped and the failure direction is permissive.
  The promotion hold-down, by contrast, is monotonic precisely because it is a
  local safety decision.
- **openraft 0.9 has no pre-vote**, so a flapping partitioned node inflates terms
  and can disrupt a healthy leader. `CheckQuorum` is the only guard. The
  flap-partition nemesis exists but is checked for convergence, not for
  disruption cost.

## Exit criteria

The roadmap is done when:

1. Every FATAL contract has an oracle that measures the contract rather than a
   proxy for it, and a paired inversion test proving that oracle can fail.
2. Durability claims survive a dirty crash, not just a clean one.
3. Any failure found in CI can be replayed from a seed.
4. Every risk window above is either covered by a test, closed by design, or
   listed in Accepted risks with a rationale.
5. The specs are checked against traces from the running binary.

## Execution plan

Everything above, restated as independently committable tasks. When every box is
checked, this document describes no remaining gap and the exit criteria are met.

Rules that apply to every task, so they are not repeated per line:

- **A task is not done until its oracle has been shown to fail.** Build the
  inversion first — inject the violation the task exists to catch, watch the new
  check go red, then fix or heal and watch it go green. An oracle never observed
  failing is indistinguishable from one that cannot fail, which is the defect
  this whole document is about.
- **Faults verify their own effect and fail loudly.** No task may add a fault
  that can silently no-op. Derive container and network names from the active
  compose project; never hardcode.
- **Update `docs/STATE_MACHINE.md` in the same commit** as any change to
  consensus, supervisor, lease, replication, fencing, or gateway-routing state.
- **Update this file in the same commit**: check the box, and if the task
  retires or changes a risk window, edit the register rather than leaving the
  prose stale.

Effort: **S** hours · **M** one to two days · **L** about a week · **XL**
multi-week.

### Wave 0 — Foundations

Nothing else in Wave 1 is safe or cheap until these land.

- [x] **H-01 — Inject `linearizability_register.py` config instead of rebinding
      globals.** Done. The four module globals are now `DEFAULT_*` constants and
      a frozen `WorkloadConfig` is threaded to the worker loops, both table
      setups, and `History.per_key`, which no longer has a default key count.
      `test_workload_config.py` asserts the four retired names are not module
      attributes, that no `global` statement remains, that the thread arg tuple
      binds to all three worker loops, and that `--keys` measurably governs both
      the workers and the setup SQL. Verified non-vacuous against four injected
      regressions: a worker reading `DEFAULT_NUM_KEYS`, a reintroduced global, a
      defaulted `per_key`, and a loop missing its `cfg` parameter.
      _Blocks_ H-03 · _Effort_ S

- [ ] **H-02 — Route every fault through the primitive layer.** Partly done.
      `linearizability_register.py` and `correctness_lite.py` now inject nothing
      directly: their partition, latency, loss, asymmetric-partition, and scrub
      paths call `fault_primitives.py`, which verifies its own effect and resolves
      docker names against the active compose project. The primitive layer gained
      that name resolution plus a `network_detached` total-partition primitive,
      which is the one fault that cannot be expressed as a compose service.

      Three defects fell out and are fixed: the two partition attacks used literal
      docker names, which `elle.yml` had been working around by pinning
      `COMPOSE_PROJECT_NAME` (now un-pinned, so Elle has per-run isolation like
      every other workflow); both harnesses fired faults from daemon threads whose
      exceptions were discarded, so a fault that failed to inject still produced a
      PASS; and `correctness_lite`'s reattach used a bare `docker network connect`,
      which assigns a fresh address rather than the compose-pinned one, leaving
      later steps addressing a node at an IP it no longer held.

      `lint_matrix.py` now checks which modules inject directly, not how many
      times — it stops the spread to new modules and forces a migrated file off
      the pending list. It is explicitly not a correctness check: matching source
      text cannot tell a command from prose about one, and a verb assembled at
      runtime is invisible to it.

      **Remaining:** `ci_runner.py` still drives asymmetric partition with
      `iptables` and latency with `tc` behind its own verifiers, and its netem
      parser has a different contract from the primitive layer's (`None` versus
      `0.0` for a delay-less qdisc), so merging them needs care rather than a
      rename. Then the real gate: a live Elle smoke and `ha-sequential`, neither
      of which has run against these changes.
      _Closes_ Class A1 structurally · _Blocks_ H-05…H-09, H-12 · _Effort_ M

- [ ] **H-03 — Split `linearizability_register.py`** (about 1,750 lines) along its
      natural seams: history and records, the WGL and weak checkers, the workload
      loops, the attack table.
      **Done when** a real Elle run passes post-split, not just unit tests — the
      failure mode is a silently weakened workload, which unit tests do not see.
      `test_workload_config.py` must still pass unchanged, since it is the guard
      that the split cannot reintroduce a stale-default read.
      _Blocked by_ H-01 (done) · _Effort_ M

- [ ] **H-04 — Build the 5-node topology.** `five_node_suite.py` is a skeleton
      that raises; no 5-node topology is tested anywhere, so quorum arithmetic
      beyond three nodes and dual-fault tolerance are entirely unexercised.
      **Done when** a 5-node cluster runs the existing L1 and W1 oracles, and one
      case survives two simultaneous node failures while another confirms three
      failures correctly loses quorum.
      _Closes_ BI1 · _Blocks_ H-06 (5-node shapes), H-11, H-29 · _Effort_ L

### Wave 1 — Close the oracles (Tier 1)

No new infrastructure. Highest confidence per unit of effort.

- [ ] **H-05 — Port-granular partitions.** All current rules are per-peer-IP, so
      the classic gray split — Raft heartbeats healthy while PostgreSQL streaming
      is dead, or the inverse — cannot be expressed. Partition by destination
      port: Raft/management vs replication vs gateway.
      **Done when** a case severs replication while leaving Raft intact and
      asserts the leader notices, plus the inverse.
      _Closes_ RW-6, RW-11 reachability · _Blocked by_ H-02 · _Effort_ M

- [ ] **H-06 — Partition shapes beyond node-vs-rest.** Majority-side isolation
      with the observer on the minority side, three-way splits, and a _follower_
      as bridge, where the leader must step down even though it still sees one
      follower.
      **Done when** each shape has a case asserting the safety outcome, and the
      5-node asymmetric shape that reaches RW-1 exists.
      _Closes_ RW-1 · _Blocked by_ H-02, H-04 · _Effort_ M

- [ ] **H-07 — Trigger faults on protocol state, not wall-clock offsets.** Replace
      `sleep 4` with injection keyed off observed state: mid-rewind,
      mid-basebackup, inside the promotion hold-down, and inside the
      post-promotion window where `synchronous_standby_names` is cleared before
      the replication manager reinstates it.
      **Done when** the post-promotion sync gap is entered deliberately and a
      commit during it is proven either to block or to carry a standby ack.
      _Closes_ RW-2 · _Blocked by_ H-02 · _Effort_ M

- [ ] **H-08 — Backward clock jumps and sub-second skew.** Current skew is
      forward-only and coarse (+30 s / +300 s). Backward steps are the harder
      case for lease arithmetic, and the interesting regime is within a few
      hundred milliseconds of the lease and election boundaries.
      **Done when** a sweep across the boundary neighbourhood runs with the
      prober attached and no dual-write window appears.
      _Blocked by_ H-02 · _Effort_ M

- [ ] **H-09 — Wiped-node rejoin.** Destroy one node's volume and rejoin under
      the same node id — the canonical Raft persisted-vote violation. Plus the
      asymmetric cases: Raft store lost with PostgreSQL data intact, and the
      reverse.
      **Done when** all three variants either rejoin cleanly or refuse, and
      neither votes twice in one term.
      _Blocked by_ H-02 · _Effort_ M

- [ ] **H-10 — Follower reads and read-only transactions in the Elle workload**,
      so staleness, long fork, and phantoms enter the test universe at all.
      **Done when** Elle checks a history containing follower reads and predicate
      reads, and the checker is shown to reject an injected stale read.
      _Closes_ part of Class B · _Effort_ M

- [ ] **H-11 — RW-7 as a safety test.** The existing case asserts election
      _succeeds_ (liveness). Restore a node from a stale basebackup, keep its Raft
      directory, start the cluster after the LSN staleness threshold has elapsed,
      and assert the stale node cannot win.
      **Done when** the assertion is that a stale node _loses_, and it is shown to
      fail if the staleness gate is disabled.
      _Closes_ RW-7 · _Effort_ M

- [ ] **H-12 — Harden `overnight_test.py`.** It addresses docker objects literally
      at about 30 sites, so it inherits the Class A1 bug the moment it runs under
      a non-default compose project. It is local-only, injects 9 of its 10 faults
      with no client load in flight, and its health oracle is a substring match on
      CLI output.
      **Done when** it routes through the primitive layer, runs load during every
      fault, and its oracle queries state instead of grepping text.
      _Blocked by_ H-02 · _Effort_ M

- [ ] **H-13 — Measure the fencing failure tail.** Wedged postmaster, exhausted
      connection slots, and a backend in uninterruptible I/O surviving
      `pg_terminate_backend`. SIGSTOP of the postmaster exists; the write path
      during it is unmeasured.
      **Done when** the prober runs concurrently with each fencing-failure mode
      and the escalation to process exit and container restart is observed.
      _Closes_ RW-4 · _Effort_ M

- [ ] **H-14 — Measure what `pg_rewind` discards.** Async fallback then rewind can
      discard up to the divergence threshold of genuinely acked WAL, and the
      threshold's justification assumes sync replication is active — exactly false
      during fallback. The fallback is reachable but nothing measures the loss.
      **Done when** a case records acked writes, forces async fallback and rewind,
      and reports how many acked writes were destroyed. That number either is zero
      or becomes a documented RPO bound.
      _Closes_ RW-3 · _Effort_ M

- [ ] **H-15 — Unblock the supervisor mutex in `demote()`.** It is held across
      stop, rewind, and recovery, so the 100 ms lease tick, the health watchdog,
      and LSN reporting all stall behind it.
      **Done when** the stall is measured first, then removed, and a test asserts
      the lease tick keeps its period during a demote.
      _Closes_ RW-9 · _Effort_ M

- [ ] **H-16 — Join and rejoin edges.** Basebackup against a leader that gets
      deposed mid-copy, orphan slots pinning WAL, and a learner registration
      surviving a mid-join crash.
      **Done when** each edge has a case, and orphan-slot WAL pinning is asserted
      bounded rather than assumed.
      _Closes_ RW-10 · _Effort_ M

- [ ] **H-17 — Sweep commit-probe correctness around COMMIT.** A wrong answer
      manufactures a phantom commit or a duplicate retry. One fixed timing is
      tested; the byte offsets around the commit record are not.
      **Done when** the probe is exercised at every offset in the neighbourhood of
      COMMIT and each answer is checked against ground truth from
      `txid_status()`.
      _Closes_ RW-12 · _Effort_ M

- [ ] **H-18 — Drive the failover-anchor coalescing race live.** The pure
      functions are unit-tested; a missed clear or missed re-stamp under coalesced
      watch transitions would make the hold-down read an ancient anchor and
      promote immediately.
      **Done when** coalesced transitions are forced against a live cluster and
      the anchor lifecycle is asserted, not just its arithmetic.
      _Closes_ RW-8 · _Effort_ M

### Wave 2 — Differential classifier testing (Tier 2)

The durable fix for a bug class that has already produced one real defect.

- [ ] **H-19 — Build the differential classifier oracle.** Generate SQL, execute
      it against a real PostgreSQL session, snapshot ground truth from
      `pg_prepared_statements`, `pg_cursors`,
      `pg_locks WHERE locktype = 'advisory'`, `pg_listening_channels()`,
      `pg_class WHERE relpersistence = 't'`, and
      `pg_settings WHERE source = 'session'`.
      **Done when** the oracle asserts _if session state changed, the classifier
      said non-migratable_, and it reproduces the known advisory-lock defect when
      the fix is reverted.
      _Effort_ L

- [x] **H-20 — Add a `LoadStmt` arm.** Done. `LOAD 'lib'` links a shared library
      into one backend, so a migrated session silently stops resolving whatever it
      provided.

      The arm alone was not enough, and the reason generalises: the analyzer only
      runs when a prefilter recognises the statement, and `load` was not in that
      keyword list. The arm was therefore unreachable in production while its own
      test passed, because `marks_non_migratable` calls `analyze_query` directly
      and cannot see a missing gate keyword. `load` is now gated, and
      `test_session_state_prefilter_admits_every_gated_statement` checks the gate
      layer for all nine keyword shapes — verified to fail when `load` is removed
      from the list.

      Extracting `leaves_no_session_state` also gave the state-free arms a name;
      they were previously indistinguishable in shape from the `Unmodeled`
      fallback, though they mean the opposite.
      _Effort_ S

- [ ] **H-21 — Emit a metric labelled by statement node type whenever the
      unmodeled arm is reached**, so the production tail is measured before any
      policy change. The classifier is a deny-list covering roughly two dozen of
      PostgreSQL's ~200 node types and the fallback is _migratable_ — the unsafe
      direction.
      **Done when** the metric is exported and documented as the input to H-23.
      _Blocks_ H-23 · _Effort_ S

- [ ] **H-22 — Decide on a `CallStmt` arm.** `CALL proc()` is opaque exactly like
      `DO`, but severing it has real cost for stored-procedure-heavy workloads.
      **Needs sign-off before implementation** — this is a policy call, not a bug
      fix.
      Note from H-20: `call` is not a prefilter keyword either, so the arm and the
      gate have to land together or the arm is dead code that still passes its own
      test. Add the shape to
      `test_session_state_prefilter_admits_every_gated_statement`.
      _Effort_ S after decision

- [ ] **H-23 — Revisit the fallback polarity with production data in hand.** Do
      not flip it wholesale: the ratchet is one-way for the connection's lifetime
      and poolers hold connections for hours, so one early `ALTER TABLE` or
      `VACUUM` would permanently disqualify a pooled session. It also would not
      close the deepest hole — `SELECT my_func()` is a `SelectStmt` and a function
      body can create a temp table or take a session lock.
      **Gated on** H-21 data. Outcome may legitimately be "no change, documented".
      _Blocked by_ H-21 · _Effort_ M

### Wave 3 — Prove durability (Tier 3)

Touches the container image. The single most likely place to find a genuine RPO
violation, and the first thing a Jepsen analysis would reach for.

- [ ] **H-24 — Lost-unfsynced-writes on crash.** LazyFS or libeatmydata. Open the
      fsync-drop window, collect acked writes, SIGKILL the container, restart,
      assert every acked write is present.
      **Done when** W1 and R2 are demonstrated rather than asserted, and the
      harness is shown to detect a deliberately weakened durability setting.
      _Closes_ Class A2 for fsync · _Effort_ L

- [ ] **H-25 — Torn writes via dm-flakey**, which also exercises PostgreSQL page
      checksums and redb crash recovery.
      **Done when** a torn write is injected and detected rather than silently
      accepted.
      _Effort_ L

- [ ] **H-26 — ENOSPC at the next WAL segment**, as distinct from a blanket volume
      fill. The data volumes are tmpfs-bounded now, so a fill can genuinely
      exhaust them; targeting exhaustion at WAL-segment allocation is what
      remains.
      **Done when** the failure lands at segment allocation specifically and the
      node fences rather than corrupting.
      _Effort_ M

### Wave 4 — Concurrency gaps (Tier 3.5)

- [ ] **H-27 — Test the `snapshot_consistency` race.** The conformance suite drives
      `build_snapshot` and `install_snapshot` sequentially, so the mutex is taken
      and never contended. The interleaving it exists to prevent — a builder
      observing the installer's redb write before its state swap, emitting a
      snapshot whose meta is ahead of its data — is uncovered.
      **Done when** a concurrency test contends the mutex and fails with the lock
      removed.
      _Effort_ M

- [ ] **H-28 — Measure host-to-bridge routability for the prober's `direct`
      transport.** It is unproven on macOS, where Docker Desktop cannot route the
      compose subnet, so it auto-falls back to `docker-exec`. The Linux CI path
      rests on standard bridge behaviour rather than a measurement.
      **Done when** CI asserts which transport it selected, so a silent
      fallback cannot masquerade as a direct probe.
      _Effort_ S

### Wave 5 — Deterministic simulation (Tier 4)

The actual next level: reproducibility and schedule coverage Docker structurally
cannot provide. Aimed at `app.rs`, the least-tested and most dangerous file in
the repo.

- [ ] **H-29 — Add a `PgProbe` seam** over `probe_role_and_readonly`, `promote`,
      `demote`, and `pg_stat_replication`. Two of the three seams already exist:
      `LeaseState` reads time through an injectable `Clock`, and
      `src/governor/network.rs` is the transport seam.
      **Done when** the governor compiles against a model PostgreSQL with no
      behaviour change to the real path.
      _Blocks_ H-31 · _Effort_ L

- [ ] **H-30 — Inject the clock everywhere**, not in the lease alone.
      **Done when** no safety decision reads `Instant::now()` or `SystemTime::now()`
      directly, enforced by a lint.
      _Blocks_ H-31 · _Effort_ M

- [ ] **H-31 — Run the governor and `app.rs` orchestration under `madsim`** with a
      seeded network controlling delay, reorder, drop, and clock advance. `madsim`
      over `turmoil` because time control is required, not just network control.
      **Done when** thousands of failover schedules run per second and the suite
      reproduces a known-bad schedule from a seed.
      _Blocked by_ H-29, H-30 · _Effort_ XL

- [ ] **H-32 — Make CI failures replayable from a seed.** Emit the seed on every
      failure and accept it as input.
      **Done when** a CI failure can be reproduced locally from its artifact
      alone.
      _Closes_ Class C and exit criterion 3 · _Blocked by_ H-31 · _Effort_ M

### Wave 6 — Connect the specs to the binary (Tier 5)

The four specs are non-vacuous, but the spec-to-code mapping is _comments_, so
nothing detects drift.

- [ ] **H-33 — Emit structured state-transition events**: lease renew and expire,
      anchor stamp and clear, promote and demote, fence escalation.
      **Done when** the event stream is complete enough to reconstruct a failover
      without reading logs.
      _Blocks_ H-34 · _Effort_ M

- [ ] **H-34 — Validate real traces against the specs.** This turns "we have
      specs" into "the specs describe this binary", and directly closes the known
      delta where the model anchors the hold-down at the election instant while
      the implementation anchors at local leader-loss observation.
      **Done when** a harness run's trace is checked against `lease_fencing` and
      `raft_lsn`, and an injected drift is caught.
      _Closes_ exit criterion 5 · _Blocked by_ H-33 · _Effort_ L

- [ ] **H-35 — Model partial-quorum dynamics in `lease_fencing.tla`**, which
      currently states plainly that it does not — the regime of RW-1.
      **Done when** the spec covers a deposed leader holding a quorum that
      excludes the winner, and TLC either proves safety or produces the
      counterexample.
      _Effort_ L

- [ ] **H-36 — Larger models nightly**: 5 nodes and higher term bounds. Current
      configs are 3 nodes with small bounds, and `raft_lsn.tla` abstracts the Raft
      log away entirely, so its `ElectionSafety` is the textbook Raft theorem
      rather than a statement about openraft.
      **Done when** the nightly job checks the larger configs within its budget.
      _Effort_ M

### Wave 7 — Real Jepsen (Tier 6)

A moderate lift rather than a rewrite: the histories are already in Jepsen
format and Elle is already the checker.

- [ ] **H-37 — Write a `jepsen.db` setup and teardown harness**, run on 5 nodes on
      LXC or VMs, and adopt Jepsen's own nemesis library.
      **Done when** a real Jepsen run reproduces our existing L1 and W1 results
      independently.
      _Blocked by_ H-04 · _Effort_ XL

Full FoundationDB-style simulation — all IO behind swappable traits from the
start — remains deliberately out of scope; H-29 through H-32 capture most of its
value without the retrofit.

### Wave 8 — Completion audit

These close the exit criteria and cannot be done early.

- [ ] **H-38 — Pair every FATAL contract oracle with an inversion test** proving
      that oracle can fail. Partly done: the `assert-sanity-*-bad.sql` cases and
      the prober's self-falsification are this pattern already.
      **Done when** every FATAL contract in `docs/CONTRACTS.md` names both its
      oracle and its inversion case, enforced by `lint_matrix.py`.
      _Closes_ exit criterion 1 · _Effort_ M

- [ ] **H-39 — Audit the risk-window register.** Every RW-1…RW-12 must be covered
      by a test, closed by design, or listed in Accepted risks with a rationale —
      and the reachability column must be re-derived from the code, not trusted.
      **Done when** no row reads "No" or "Partially" without a task ID or an
      accepted-risk entry beside it.
      _Closes_ exit criterion 4 · _Effort_ S

- [ ] **H-40 — Re-verify the whole document.** Re-read the escape classes and the
      register against the implemented state and delete what is no longer true.
      **Done when** the prose describes the system as it then is, with no gap left
      unclaimed.
      _Effort_ S
