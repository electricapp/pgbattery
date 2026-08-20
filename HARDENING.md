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

**A1: the fault silently fails to inject, and the test passes anyway.** Six
confirmed instances, all found by adding effect verification rather than by any
test going red:

| What                                                                                                                                                        | Consequence                                                                                                           |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `iptables` was never installed in the runtime image                                                                                                         | all three nightly asymmetric-partition cases injected nothing; their `iptables -D ... \|\| true` cleanup no-opped too |
| every fault `exec` ran as unprivileged `postgres` (the image ends `USER postgres`; `NET_ADMIN` is granted to the container, not to an unprivileged process) | `tc netem` could never inject latency — `RTNETLINK answers: Operation not permitted`                                  |
| `elle.yml` set a per-run `COMPOSE_PROJECT_NAME` while the harness addressed docker objects literally                                                        | `partition` and `flap_partition` were no-ops in CI                                                                    |
| `correctness_lite.py` had the same literal names under `correctness-lite.yml`'s per-run project                                                             | the partition fault window was empty, making I5 vacuous for it                                                        |
| `transfer_leader_after` read the API token from a `.env` CI never wrote                                                                                     | `transfer` posted unauthenticated and transferred nothing                                                             |

All six are fixed. The lesson is the durable part: **a fault must verify its own
effect and fail loudly, and the harness must not assume its environment.** Every
one of these passed for months. `docker-compose.yml` sets `name: pgbattery`, so
literal names work locally and only break under CI's per-run project — the worst
possible failure shape.

The sixth is the sharpest illustration, because the harness _did_ check
something: it checked that writing to the FIFO succeeded. It always does. LazyFS
opens the FIFO `O_RDWR` when it creates it, so a write succeeds whether or not
the worker thread behind it is alive, and that success was read as confirming the
fault fired. The distinction is only visible in LazyFS's own log, which is why
`[filesystem].logfile` is now set and why `verify_lazyfs_fault_channel` sends a
deliberately unknown command and waits for the worker to echo it back.
**Checking that a fault was _requested_ is not checking that it was _executed_.**

H-24's measured result survives this: what it demonstrated was the SIGKILL
destroying the process holding un-fsynced pages in userspace, not the
`clear-cache` that preceded it, and its inversion went red as required. The
defect was that the suite carried a command that did nothing while reading as
though it did.

The related power bug: workers pinned to a killed leader's gateway spun on
connection-refused at roughly 2800 attempts/second, producing 56,600 of 56,665
`:info` records in one CI run while two surviving workers committed every real
transaction. A nightly Elle run advertised ~10^5 operations and delivered ~385
real ones.

That lesson has a twin on the reading side. `docker exec` against a restarting
container answers nothing, and two readers reported that silence as an answer:
`read_lazyfs_mounted` as "PGDATA is not on LazyFS", `read_processes` as "no
postmaster". Both were called from loops written to wait for that container, so
each aborted on its first iteration the wait it existed to perform, failing
three durability jobs in CI with causes that were not real.
`ContainerNotRunning` now separates _I could not look_ from _I looked and it is
absent_; `No such container` stays outside that class, being topology drift.
**An unreachable node is an absence of evidence, never evidence of absence** —
read wrong it fails loudly here, and in a checker it would pass while blind.

A third reader had the same defect where it was hardest to see, because the tear
being measured is what makes the node unreadable. `torn_raft.py` sized each tear
from the LazyFS log inside the container, and a torn write kills that container's
LazyFS; a failed `exec` returned no records, which is what a log holding no tear
also returns. CI then failed a follower run with "the fault never fired" while
reporting the victim had refused to start on a store redb could not open. The
read waits the container out now, an unreadable log is carried as its own
finding, and a refusal on a store-damage shape establishes the damage without the
log at all.

There is a fourth shape, and it is the most expensive one here: a harness gate
that makes a failure rarer without touching what caused it. The transfer cascades
chain straight off a `wait_cluster`, and when the next transfer was refused those
waits were given `min_healthy_replicas: 2` on the theory that the node which just
demoted had not settled. The theory was wrong and the gate was a placebo — the
leader's replication view still shows the outgoing leader healthy for as long as
it takes that node to notice it must demote, so the wait returned in
`elapsed_sec=0.0` and measured nothing. Worse, a `lint_matrix.py` rule was added
to enforce it, which certified a settling guarantee that did not exist.

What the case had actually found was a product bug: `transfer_leadership`
disabled the leader's heartbeats and then waited on the target's supervisor lock,
so a target mid-demote could hold the leader silent for over ten seconds against
a one-second election timeout. See the leadership-transfer section below. The
placebo rule is gone; the invariant it should have been is now a constants test
in `config::constants`, and the lint rule that replaced it reads
`ci_runner.py`'s own retry default rather than restating it.

**A gate that changes a failure rate without naming a mechanism is a placebo
until proven otherwise.** The tell was available and ignored: the gate's own log
line said it waited zero seconds.

The precondition shape has a sharper form: the product can discard the fault
itself, and a harness that does not know its starting state reads that as
tolerance. `torn_raft.py` mangled a node's Raft store while the cluster's voter
set was still `{1}` and the node was mid-join. It came back healthy with the
damage gone — not because anything restored it, but because `run_join_flow` wipes
the local store and rejoins when the peer does not list this node. The store was
never opened, so nothing had cause to refuse. A green from the product correctly
throwing the fault away is indistinguishable from a green from the product
surviving it. The suite now requires every node to be a committed voter, in every
node's view, before it damages any of them.

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
produce the conditions, not that a test asserts the right thing. "Tracked by"
names the task that closes the window, so no row is open without an owner.

| #     | Window                                                                                                                                                                                                                                                                                                                                          | Contract   | Reachable today                                                                                                                            | Tracked by      |
| ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------ | --------------- |
| RW-1  | Deposed leader retains a quorum excluding the election winner, so its last ack can be later than the winner's first sight of leaderlessness and the promotion hold-down is already spent at election                                                                                                                                            | L1         | Closed — modeled with explicit quorum sets; the anchor tracks the newest observed term, and the un-fixed model is a checked counterexample | H-35 (closed)   |
| RW-2  | Post-promotion window: a freshly promoted primary acknowledged commits with zero standby acks. Closed by arming the sync list and the read-only fence before `pg_ctl promote`, and gating write recovery on a standby actually designated `sync` — the GUC's text was never the evidence, because PostgreSQL's enforcement of it lags promotion | W1, R2     | Closed — `post_promotion_sync_gap.py` enters the window on protocol state; the commit is refused                                           | H-07 (done)     |
| RW-3  | Async fallback then `pg_rewind` discarding genuinely acked WAL. **Measured, not bounded away: 20 of 20 acknowledged writes destroyed** (`rewind_loss.py`). Every write acknowledged while `synchronous_standby_names` is empty can be lost, up to 16 MiB of diverged WAL — the deliberate availability trade, now counted rather than asserted  | W1         | Yes — `rewind_loss.py` forces the fallback and counts the survivors                                                                        | H-14 (measured) |
| RW-4  | Fencing failure tail: wedged postmaster, exhausted connection slots, or a backend in uninterruptible I/O surviving `pg_terminate_backend`                                                                                                                                                                                                       | L1, L2     | Partially — SIGSTOP of the postmaster exists; the write path during it is unmeasured                                                       | H-13            |
| RW-5  | Direct writers on the internal PostgreSQL port bypassing the gateway's lease check entirely (`trust` auth on the cluster network)                                                                                                                                                                                                               | L1         | Yes — `dual_writability_prober` writes all three internal ports at 50 ms resolution                                                        | covered         |
| RW-6  | Follower gateway routing writes to a deposed primary during the Raft detection interval, stopped only by the old leader's own lease                                                                                                                                                                                                             | W1         | Partially — never driven through a _follower_ gateway specifically                                                                         | H-05            |
| RW-7  | After a long leaderless window every LSN report ages past the staleness threshold and both election and promotion gates fall back to bootstrap-permissive; a node restored from an old backup can win                                                                                                                                           | L3         | Closed — aged-LSN tiebreak; `test_stale_restored_node_loses_a_leaderless_election`                                                         | H-11 (done)     |
| RW-8  | Failover-anchor lifecycle under coalesced watch transitions: a missed clear or missed re-stamp makes the hold-down read an ancient anchor and promote immediately                                                                                                                                                                               | L1         | Partially — pure functions are unit-tested; the live coalescing race is not                                                                | H-18            |
| RW-9  | `demote()` holds the supervisor mutex across stop, rewind, and recovery — the 100 ms lease tick, health watchdog, and LSN reporting all stall behind it                                                                                                                                                                                         | L1         | No                                                                                                                                         | H-15            |
| RW-10 | Join and rejoin edges: basebackup against a leader that gets deposed mid-copy, orphan slots pinning WAL, a learner registration surviving a mid-join crash, a wiped bootstrap node impersonating the cluster                                                                                                                                    | R1, V2     | Yes — `join_edges.py` drives four cases; orphan-slot pinning is bounded, not assumed                                                       | H-16 (closed)   |
| RW-11 | `SetSyncMode` replicated state disagreeing with the live GUC across a leader change, so the election gate uses the loose async threshold while sync is actually active                                                                                                                                                                          | W1, L3     | No                                                                                                                                         | H-05            |
| RW-12 | Commit-probe correctness at every byte offset around COMMIT: a wrong answer manufactures a phantom commit or a duplicate retry                                                                                                                                                                                                                  | W1, W2, S1 | Partially — one fixed timing, no sweep                                                                                                     | H-17            |

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
- **Torn writes** via `lazyfs::torn-op`, which also exercises PostgreSQL page
  checksums and redb crash recovery.
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
- **A function body that leaves session state is migrated.** `SELECT my_func()`
  parses to a `SelectStmt`, which has its own `classify_statement` arm, so a
  function that creates a temp table or takes a session advisory lock is
  classified migratable and the gateway will move the connection. Closing this
  needs catalog introspection of the target function on the hot read path, which
  is the one place the design deliberately keeps off `libpg_query`. Note this is
  _not_ reachable by changing the `_ => Unmodeled` fallback polarity: a
  `SelectStmt` never reaches that arm (see H-23).

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

- [x] **H-02 — Route every fault through the primitive layer.**
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

      `ci_runner.py` is now migrated too and off the pending list. Its
      asymmetric partition and latency steps build their commands with
      `iptables_peer_drop_cmd` / `netem_add_cmd` and verify them with
      `parse_peer_drop_rule` / `parse_netem`; `_NODE_IPS` is re-keyed from
      `fp.NODE_IPS` rather than restated. The commands are byte-identical to the
      ones they replaced, which is what made the swap checkable rather than
      hopeful.

      Two contracts had to be reconciled rather than renamed. The netem parser:
      the local one returned `None` both for "no qdisc" and for "qdisc with no
      delay clause"; the primitive's returns a state with `delay_ms == 0.0` for
      the second, and it is the keeper. And the add polarity: `ci_runner`
      prefixed every `netem` add with `qdisc del ... 2>/dev/null`, silently
      clobbering whatever was installed. The primitive deliberately does not, so
      inherited residue now fails the add with "Exclusivity flag on" instead of
      being overwritten. Healing stays tolerant of an already-clean interface —
      strict add, idempotent delete.

      The fault-verb scan was tightened from the tool name to the mutating
      subcommand (`iptables -[AIDF]`, `tc qdisc|filter add|del|…`). Naming
      `iptables` to assert the binary exists, or to label a log file, was being
      flagged as injection, which pushes a caller into renaming things to get
      past the check — the appearance of migration without the substance.

      Live-gated on a real cluster: `network-latency-stability` passes, and its
      cleanup re-heals already-clean interfaces without failing, which is the
      strict-add/tolerant-heal asymmetry doing what it says.
      _Closes_ Class A1 structurally · _Blocks_ H-05…H-09, H-12 · _Effort_ M

- [x] **H-42 — Resolve leadership across nodes, not from whichever answers
      first.** Found by H-02's live gate: `asymmetric-leader-partition` fails,
      and has since the initial commit. The cluster is not at fault. Driven by
      hand under the same partition, node2 and node3 elect node3 within 10 s,
      and node1 — isolated, still naming itself leader — is correctly fenced:
      `pgbattery_lease_valid 0`, `pgbattery_emergency_fence 1`, and a real
      `INSERT` refused with `cannot execute ... in a read-only transaction`. L1
      holds. Only the observer was wrong.

      `_get_cluster_nodes` returns the first node that answers, in id order, so
      every convergence check in `wait_cluster` was evaluated against node1's
      view — deterministically the one node guaranteed to be wrong when node1 is
      the partitioned ex-leader. A correct failover read as "leadership never
      moved".

      The worse half is the split-brain check. `/api/v1/cluster/nodes` fills
      `is_leader` from a single `Option<node_id>`, so at most one entry per
      response is ever true and `leader_count > 1` was **structurally
      unreachable** — an oracle that cannot fail, guarding the property this
      system exists to protect. A second, real check on distinct self-claims did
      exist, but only under `require_replication_health`, which the partition
      cases switch off precisely because a partition is up. It was disabled in
      the one scenario that can produce two self-claims.

      `wait_cluster` now polls every node. A leader counts only when a strict
      majority of the **configured** cluster names the same one — sized on the
      configured count, not on who replied, so a minority partition cannot
      certify its own view.

      Split brain is checked unconditionally, and against the lease rather than
      the belief. Two distinct *self*-claims is only the suspicion: a node
      naming someone else is reporting hearsay and cannot manufacture a second
      leader, but an isolated ex-leader naming itself is expected — it has no
      way to learn otherwise. The first version of this check stopped there and
      failed the case on `[1, 3]`, which was wrong in the informative direction:
      it proved the oracle fires, on a cluster where node1 was already fenced.
      L1 constrains write *authority*, so a suspicion is escalated only when
      more than one self-claimant reports `pgbattery_lease_valid`. A scrape
      failure counts as no lease — this path only ever turns a suspicion into a
      failure, so it must not invent a leader out of an unreachable node.

      Red-green across all three states: the original code failed on "leadership
      never moved"; the self-claim-only check failed with the new split-brain
      message, which is the proof it can fire; the lease-confirmed check passes
      the case end to end in 99 s, dual-writability prober and data oracles
      included.
      _Closes_ Class B (recovery unobservable; split-brain oracle unreachable)
      · _Effort_ S

- [x] **H-03 — Split `linearizability_register.py`.** All six seams extracted.
      `linreg/` holds `records.py` (`Op`, `JepsenRecord`, `History`),
      `checkers.py` (WGL and weak), `cluster.py` (shell, leader discovery,
      topology constants), `config.py` (`WorkloadConfig` and its defaults),
      `workload.py` (table setup, op helpers, the three worker loops, and the
      gateway-rebind helpers the loops call), and `attacks.py` (every injector,
      `ATTACK_DISPATCH`, and the seeded/scaffold sets). The entrypoint keeps the
      CLI and the verdict, down from 1,821 lines to 485.

      Tests import from the package directly rather than through re-exports, so
      the boundaries are real. Two guards were widened to survive the move: the
      `global` scan now covers `linreg/*.py` and fails if the package is missing,
      so it cannot pass vacuously; and `lint_matrix`'s fault-verb scan gained
      `*/*.py`, because moving an attack into a package would otherwise walk
      straight out of the confinement check.

      Live-validated against a 3-node cluster: a default `--attack kill --check
      wgl` run injects a real leader-kill, the cluster fails over node1 → node2,
      and all three keys check clean in 11.8 s. The refactor is exercised
      end-to-end, not just by unit tests.

      The live run also surfaced **H-41**, below.
      _Closes_ Class A1 structurally · _Effort_ M

- [x] **H-41 — Bound WGL on explored states, and report an unchecked key as
      unchecked.** Found by H-03's live run, so it sits here rather than at the
      end of the plan. `WGL_OPS_PER_KEY_CAP = 2000` was documented as the bound on
      WGL's blowup, but it caps op count, which is not the variable WGL's cost
      depends on. A 45 s / 6-worker / 8-key kill run put ~1,450 ops on every key
      — every one under the cap — and 22% of them were pending, from the fault
      window. Measured per-key: 0.3 s, 20.5 s, 11.8 s, and five keys still
      searching after 30 s.

      Unbounded, that is a hang. The failure mode is the dangerous one: a lucky
      seed finishes and prints PASS, an unlucky one is killed by the CI timeout
      and reads as an infra flake. Neither ever says the key went unchecked.

      `WGL_MAX_EXPLORED_STATES = 250_000` now bounds the search, counted in
      memoized states rather than wall-clock so a verdict is a function of the
      history alone — a wall-clock deadline would make coverage depend on
      machine load, which is how an unchecked key starts reading as a pass.
      Calibrated from that run: the decidable keys needed 8 k, 71 k, and 127 k
      states.

      `_is_linearizable` returns `bool | None`, `None` meaning unchecked, and the
      harness reports it as its own `INCONCLUSIVE` verdict with exit 2 — exit 1
      stays "we found a violation", exit 2 is "we did not look". Replayed
      against the history that hung: 210 s total, three keys decided, five
      reported `UNCHECKED` with the op and pending counts and what to shrink.

      The sanity suite's `assert_flagged` asserted `assertFalse(ok)`, which
      `None` also satisfies — a checker that gave up would have satisfied every
      flagged case. Both helpers now assert `is False` / `is True`.
      _Closes_ Class B (unchecked history reported as a pass) · _Effort_ S
      _Blocked by_ H-01 (done) · _Effort_ M

- [x] **H-04 — 5-node topology built, Phase 1 green against a live cluster.**
      `docker-compose.5node.yml` plus `config/five/node{1..5}.toml`: its own
      compose project, subnet (172.29/16), and host ports, so it coexists with
      the 3-node cluster and neither one's fault injection can reach the other.

      All four Phase 1 cases pass: bootstrap to a single leader agreed by a
      majority, **survives two simultaneous voter failures** (the case a 3-node
      cluster cannot express — there, two failures *is* quorum loss),
      **correctly loses quorum at three**, and recovers with every acked write
      intact. L1 is checked by asking each survivor's PostgreSQL to accept a
      real write, not by reading the control plane.

      The old skeleton did not raise — it printed a TODO banner and
      `raise typer.Exit(code=0)`, so anything invoking it got a pass.

      **The first run reported a bogus L1 violation, and why is the durable
      part.** Compose starts all five joins at once; openraft 0.9 will not run
      multi-step joint consensus in parallel, so the losers of that race stayed
      *learners*. The voter set was `{1,4,5}`, not `{1..5}`. Killing "two of
      five" killed two learners — which costs no quorum at all — and the third
      kill left 2 of 3 voters, a legitimate majority. The suite would have
      passed its two-failure case for the wrong reason forever.

      `ensure_all_voters()` now promotes learners one at a time and refuses to
      proceed until all five are voters, and both kill phases select from
      `voters()` rather than from all nodes. The skeleton's own implementation
      notes had warned about the parallel-join race; the fix is that the suite
      now *enforces* the precondition instead of assuming it.

      Phases 2-4 (membership chaos, 2-sync/2-async replication, Elle at five
      nodes) are not implemented; the runner says so rather than implying
      coverage.
      _Closes_ BI1 · _Blocks_ H-06 (5-node shapes), H-11 · _Effort_ L

### Wave 1 — Close the oracles (Tier 1)

No new infrastructure. Highest confidence per unit of effort.

- [x] **H-05 — Port-granular partitions.** Primitive done and live-validated;
      matrix cases remain. `partition_channel(target, peers, Channel)` severs one
      protocol port and leaves the others up, verifying the rules are present
      _and_ that packets hit them, and failing if a DROP survives the heal.

      Live testing corrected the design twice, and neither error was visible to a
      unit test with a stubbed runner:

      1. Matching `--dport` alone caught nothing. Only one side of a TCP channel
         listens on the service port; replies reach the initiator with `--sport P`
         and an ephemeral destination. Both are now installed.
      2. Which side to install on is per channel, and traffic has to exist. Raft
         works from either end, since peers exchange RPCs constantly. Replication
         must go on the **standby** and needs write load in flight: the leader
         streams, the standby only replies every `wal_receiver_status_interval`,
         and an idle cluster produces no WAL at all — measured 0 packets idle
         against 23 under load.

      Both are recorded on `Channel` and in the failure hint, because the tempting
      "fix" for either is `require_traffic=False`, which restores exactly the
      vacuous case the check exists to prevent.

      **Matrix cases landed**, both passing live. `channel_partition` /
      `channel_heal` step types install both direction rules and verify the
      rules exist *and* matched packets, reusing the primitive's own
      `channel_side_hint` so a zero-packet failure gives the same guidance in
      both places rather than a second copy that drifts.

      - `replication-severed-raft-healthy` — severs only replication between
        node2 and the leader. Raft is untouched, so the leader must not change,
        and the case asserts **write availability returns without healing the
        partition**: node2 is the `FIRST` sync standby, so commits block until
        its walsender times out and node3 takes the sync slot. Quorum is
        provably retained throughout, which is what makes a mid-fault acked
        batch legitimate here rather than indeterminate.
      - `raft-severed-replication-healthy` — the inverse. The leader loses
        quorum while its standbys still receive WAL, so nothing in the
        replication topology signals the fault and only the lease can stop it
        accepting writes. The dual-writability prober runs across the window.

      Three corrections came out of running them, none visible to a unit test:
      the first write failed on a `NOT NULL` column because I hand-wrote an
      INSERT instead of using the existing `chaos-oracle-mid.sql`, whose own
      docstring restricts it to exactly this quorum-retained shape; the cleanup
      heal failed because `iptables -D` exits 1 on an absent rule, so
      `channel_heal` now tolerates it while still asserting absence — the same
      strict-add/idempotent-delete asymmetry netem uses; and the chaos-oracle
      drop addressed node1 literally, which fails read-only in the case that
      moves leadership by design, so it now runs on `"leader"` via
      `chaos-oracle-cleanup.sql`.
      _Closes_ RW-6, RW-11 reachability · _Blocked by_ H-02 · _Effort_ M

- [x] **H-06 — Partition shapes beyond node-vs-rest.** Phase 2 of the 5-node
      suite, all three shapes green against a live cluster:

      - **3/2 split with the leader stranded on the minority side.** The
        majority must produce exactly one writer; the minority must produce
        none, however confident its members are. Asked of PostgreSQL on each
        side, so a node that merely *believes* it leads does not count.
      - **Leader keeps exactly one follower.** Seeing *a* peer is not seeing a
        quorum, and this is where that is easiest to get wrong: the leader has a
        live peer, an open replication stream, and no obvious signal anything is
        missing. Only the quorum count says otherwise. Observed stepping down,
        with the majority side then electing.
      - **Three-way split** (`[1,2] | [3,4] | [5]`). No group reaches 3, so
        nothing anywhere may write.

      Partitions are whole-peer and installed on **both** endpoints of every
      cross-group pair: a one-sided DROP still lets the other direction through,
      and Raft needs only one direction to keep a follower believing in a leader
      it cannot answer.

      **Two of my own preconditions had to be enforced rather than assumed**,
      and the second is the same mistake as H-04's learner race wearing a
      different costume. A shape that strands nodes makes them self-fence to
      process exit, and `restart: unless-stopped` brings them back — so
      convergence includes a restart and rejoin, not just an election
      (`RE_FENCE_CONVERGE_TIMEOUT_S`, measured). Worse, `await_leader` returns
      happily while a stranded node is *still restarting*, because the other
      four can agree without it. The next shape then counted that node on one
      side, so "3 of 5" was really 2 and could not elect. `await_all_healthy()`
      now requires all five responding and all five voters before any shape
      runs.

      **RW-1 itself is not closed by these.** It is a timing window — the
      deposed leader's lease anchors at its last quorum ack, which can be later
      than the winner's local detection of leaderlessness — not a partition
      shape. Asserting on it needs the window to be observable, which is the
      `dual_writability_prober` at 50 ms against a 5-node cluster, and the
      prober is wired to the 3-node ports. Left open and re-pointed at H-44.
      _Blocked by_ H-02, H-04 · _Effort_ M

- [x] **H-44 — Prober points at the 5-node cluster; RW-1 observed.** The prober
      gained `--topology {three,five}`, and the 5-node entry covers all five
      internal PostgreSQL ports: L1 is cluster-wide, so racing three of five
      would leave two able to accept a write unobserved.

      Phase 2d isolates the leader with one companion and runs the prober across
      the whole window at its 50 ms period. At most one node accepted a write.
      An `INCONCLUSIVE` result — too many indeterminate probes to assert
      anything — is treated as a failure rather than a pass, so a blind run
      cannot read as a clean one.

      **A liveness regression I introduced in H-11 surfaced here and is fixed.**
      The aged-LSN tiebreak inherited the *tight* catch-up threshold (a single
      WAL block) under sync mode. The "somebody always qualifies" argument holds
      cluster-wide — the node at `aged_max` compares against itself — but not
      inside a partition, where that node may be on the far side. Every
      reachable follower was then rejected and the majority could not elect: a
      live 3/2 split sat leaderless for four minutes. The aged branch now uses
      the loose threshold, whose premise (synchronous replication is currently
      holding acked WAL) is exactly as stale as the numbers once every report
      has aged out. RW-7 survives the loosening by an order of magnitude — a
      restored basebackup is a whole backup behind, not 16 MB.
      `test_ordinary_lag_stays_electable_in_a_leaderless_window` pins it and
      goes red under the tight threshold.

      Two suite preconditions also got stricter. `await_all_healthy` now
      requires every node's **PostgreSQL** to answer, not just its management
      API: a node stranded by an earlier shape can serve `/cluster/leader` while
      its pgbattery crash-loops and its postmaster refuses connections, and
      counting it healthy put a dead node on one side of the next partition.
      _Closes_ RW-1 · _Blocked by_ H-04 · _Effort_ M

- [x] **H-07 — Trigger faults on protocol state, not wall-clock offsets.** The
      post-promotion window is entered on observed state, and the commit inside
      it is refused rather than acknowledged. The remaining injection points —
      mid-rewind, mid-basebackup, inside the promotion hold-down — are still
      wall-clock and are tracked by H-14, H-16, and H-18 respectively.
      **Done when** the post-promotion sync gap is entered deliberately and a
      commit during it is proven either to block or to carry a standby ack.
      _Closes_ RW-2 · _Blocked by_ H-02 · _Effort_ M

      The window is entered, and what it found is worse than the description it
      was filed under. `testing/post_promotion_sync_gap.py` opens a session on
      every standby before the fault, parks it on `pg_is_in_recovery()`, and
      commits the instant that flips — no sleep anywhere, so the probe lands
      inside a window whose measured width was ~120 ms.

      Two findings, in order of discovery:

      1. Promotion cleared `synchronous_standby_names` to empty. The value it
         replaced — the previous term's inherited list — was *safer*: it names
         peers that are not connected, so a commit blocks, while empty means
         no ack is required at all. On the build before the fix the probe
         caught exactly that: promoted at `00:17:44.612` under the inherited
         list, acknowledged 63 ms later with the list already empty and
         `SYNCACK 0` — an acknowledged commit no standby held.
         `Supervisor::promote` now takes the list the caller derives from the
         current voter set (`sync_standby_list`/`peer_voter_names`, the same
         derivation ReplicationManager uses every tick, pinned together by
         `prop_promotion_list_matches_steady_state`), so the node is never
         writable under a list it did not choose and never under an empty one.

      2. That fix is necessary and **not sufficient**, which the same probe
         then showed on the fixed build: promoted under a correct, non-empty
         `FIRST 1 (...)` with zero standbys connected, and the commit still
         acknowledged with `SYNCACK 0`. PostgreSQL gates the synchronous wait
         on a shared-memory flag, not on the GUC text a backend can read: the
         contrast is decisive, because the identical unsatisfiable list on a
         *long-running* primary blocked a commit for 10 s. So a freshly
         promoted primary does not enforce synchronous replication yet, however
         correct the value in front of it, and writing a better value cannot
         close the window on its own.

      3. So the fix is a writability gate, not a better GUC value. The node
         must not accept client writes until the durability its sync list
         promises is actually being delivered, and the evidence for that is a
         standby `PostgreSQL` has designated `sync` — never the text of the
         GUC. `Supervisor::promote` now arms both the sync list and
         `default_transaction_read_only` **before** `pg_ctl promote`, so the
         primary exists fenced rather than becoming fenced; the lease tick
         lifts the fence once `sync_durability_in_force` holds, which is either
         a designated sync standby or an empty list (a lone voter, or the
         replication manager's deliberate async fallback — the agreed RPO>0
         state). Ordering is load-bearing: armed after `pg_ctl promote`, the
         probe still caught the node writable, because promotion is visible to
         clients the instant `pg_is_in_recovery()` flips and everything the
         supervisor does next is later than that.

         The gate costs no availability. With a non-empty list and no `sync`
         standby a commit blocks at the `PostgreSQL` level anyway once the wait
         engages, so this converts an unbounded hang — or an acknowledgement
         nothing holds — into a clean read-only error. Measured window on a
         live cluster: 441 ms fenced, lifted the moment a standby went `sync`.

      Verdict, same probe, three consecutive runs each way: `UNBACKED` before
      the fix, `REFUSED` after.

      Two things fell out for the harness itself, both of which had it
      reporting on runs it had not observed:

      - `statement_timeout` does not bound a synchronous-replication wait — a
        10 s block happened under a 300 ms timeout — so "the commit blocked"
        cannot be detected by a timeout alone.
      - The probe read the commit position in a statement after the INSERT. A
        read-only transaction runs `SELECT`s perfectly well, so a *refused*
        write still produced a position and the probe announced an
        acknowledgement that never happened, which the checker then called
        `UNBACKED` — a violation invented by the harness. The position now
        comes back from the INSERT's own `RETURNING`, so a refusal prints no
        acknowledgement at all. `test_a_refused_write_never_reads_as_an_acknowledgement`
        pins it.

      An empty `synchronous_standby_names` also means nothing without
      `pg_is_in_recovery()` being false — a standby reports empty too. A third
      draft bounded the probe's own wait loop with `statement_timeout`, and the
      cancelled loop fell through to a marker announcing a promotion that had
      not happened. Every marker the probe prints is now derived from the state
      it names at the moment it prints.

- [x] **H-08 — Backward clock jumps and sub-second skew.** `clock_skew_sweep.py`
      steps the clock both ways across the boundary neighbourhood, inside a live
      failover, with `dual_writability_prober` attached.
      **Done when** a sweep across the boundary neighbourhood runs with the
      prober attached and no dual-write window appears.
      _Blocked by_ H-02 · _Effort_ M

      The magnitudes are anchored to the system's own constants —
      `sweep_around(lease_duration)` and `sweep_around(election_timeout)`, plus
      absolute sub-second steps — so retuning a constant retunes the sweep
      instead of leaving it straddling nothing, and every magnitude is applied
      in both directions. `test_clock_skew_sweep.py` pins that: it fails if the
      plan goes forward-only, stops straddling the lease boundary, loses its
      sub-second steps, or stops tracking the constants.

      `clock_skew_at_lease_boundary` existed before this and had no caller — a
      primitive nothing drove. Its docstring still described the guard it was
      built against: a hold-down that compared `unix_now_ms()` to a wall-clock
      anchor while the lease it stood in for was monotonic, which a forward
      step of one lease duration could satisfy while the deposed leader's lease
      was genuinely still valid. That guard is now `Instant` throughout
      (`promotion_lease_holddown`, `failover_started_at`) with
      `checked_duration_since(...).unwrap_or(ZERO)`, so no wall-clock step of
      either sign can shorten it. The docstring said otherwise, which would
      have told the next reader the primitive proves something it cannot; it
      now records what changed and what the primitive is for — a regression
      guard on that conversion.

      The one wall-clock read left in a safety path is the LSN staleness filter
      (`unix_now_secs`). A backward step makes every recorded LSN timestamp
      read as future-dated, and the filter caps those at age 0 rather than
      dropping them — dropping would shrink `max_cluster_lsn` toward the
      bootstrap-permissive fallback and weaken the election gate, which is
      RW-7's shape. The sweep reads `pgbattery_lsn_future_skew_total` across
      the cluster on each step, so a backward step that never reached that path
      is visible rather than assumed.

- [x] **H-09 — Wiped-node rejoin.** `wiped_rejoin.py` drives all three variants
      and samples `pgbattery_raft_is_leader` + `pgbattery_raft_term` across the
      window, so the assertion is that no two nodes claim leadership in the same
      term. Acked rows written before the wipe are counted back afterwards.
      **Done when** all three variants either rejoin cleanly or refuse, and
      neither votes twice in one term.
      _Blocked by_ H-02 · _Effort_ M

      Outcomes: `both` and `pgdata-only` rejoin cleanly; `raft-only` on a
      `join`-configured node refuses, which is the safe answer — a voter with no
      memory of the votes it cast must not silently vote again.

      Two product bugs fell out, both of which wedged a node permanently:

      1. `pgdata-only` never recovered. `join` found Raft state, took the resume
         fast path, and exited with "No PostgreSQL data found — use
         `pgbattery join --peer <addr>`", which is the command it was already
         running. Resuming now requires both stores (`join_start`); with Raft
         state and no data directory the node re-provisions from the leader
         under its existing identity — no learner registration, since it is
         already a member. Measured: 321 s wedged before, 28 s to rejoin after.
      2. A join that died mid-basebackup left PGDATA populated, and the
         emptiness precondition then refused every later start — a transient
         replication error costing the node permanently, which the code's own
         comment predicted. `ensure_join_data_dir_ready` now tells debris from
         data: files without `PG_VERSION` cannot be a directory PostgreSQL
         would open, so they are cleared; a directory with `PG_VERSION` is never
         touched.

      And one harness bug that explains why `wipe_node_state` had never been
      driven: its read-back ran `ls -A` over all paths at once, which prints a
      `dir:` header per directory, so a multi-path wipe could never verify as
      empty. Every `both` run failed as "still hold entries" with the header as
      the evidence.

      `raft-only` on a join-configured node has valid PostgreSQL data and no
      consensus identity, and the fresh-join path demands an empty directory, so
      the `pg_rewind` rejoin path — which exists, and is reached when Raft state
      is present but membership has dropped the node — is unreachable here.
      Refusing is safe, but re-provisioning would be kinder than requiring the
      operator to clear the volume. H-16 covers the neighbouring case, a node
      whose data directory is from another cluster entirely, which now
      re-provisions rather than refusing.

- [x] **H-10 — Follower reads and read-only transactions**, so staleness, long
      fork, and phantoms enter the test universe at all.
      **Done when** a history containing follower reads and predicate reads is
      checked, and the checker is shown to reject an injected stale read.
      _Closes_ part of Class B · _Effort_ M

      Every operation used to go through the gateway, so every read was a leader
      read and staleness could not arise. `follower_read_workload.py` writes
      through the leader and reads from each standby, and each read carries that
      standby's `pg_last_wal_replay_lsn()` at the instant it ran. That is what
      makes a follower read checkable rather than merely stale: lagging is
      legal, but a standby that has replayed past a commit and still serves the
      older value has a visibility bug. Predicate reads (`WHERE val > n`) run
      alongside, because a point read cannot express a phantom — a phantom is a
      row entering a *set*.

      Five properties, all pure functions in `linreg/follower_reads.py`, each
      shown rejecting its own violation in `test_follower_reads.py`: no invented
      values, monotonic per-standby reads, replayed writes are visible (the
      injected stale read), no long fork between standbys, and predicate matches
      explained by a write. Live: 25 writes, 50 follower reads, 18 predicate
      reads, all five held.

      The commit position has to be read in a statement of its own. Asking for
      it in the same one as the `UPDATE` puts both in one implicit transaction,
      so it comes back from before the commit record — and a standby replayed to
      exactly there reads as a visibility bug that did not happen. The first run
      reported precisely that, with the write's LSN and the replay LSN
      identical.

      Not done here: routing these operations into the Elle history itself.
      Elle checks strict serializability, and a follower read is legitimately
      stale under `synchronous_commit = on` (remote flush, not remote apply), so
      feeding raw follower reads to Elle would manufacture anomalies the system
      never promised to avoid. The replay-position contract above is the
      checkable claim; making follower reads Elle-checkable needs
      `remote_apply`, which is a durability/latency change, not a test change.

- [x] **H-11 — RW-7 as a safety test.** The existing case asserts election
      _succeeds_ (liveness). Restore a node from a stale basebackup, keep its Raft
      directory, start the cluster after the LSN staleness threshold has elapsed,
      and assert the stale node cannot win.
      **Done when** the assertion is that a stale node _loses_, and it is shown to
      fail if the staleness gate is disabled.

      Read the code before writing that test: it will not pass, and the reason is
      a design decision rather than a missing assertion.
      `evaluate_lsn_acceptable` returns permissive the moment *no* node has a
      fresh LSN report — "bootstrap: no fresh cluster LSN data" — justified in
      comment by "Raft log-matching protects us". That justification does not
      cover RW-7's scenario, which keeps the Raft directory intact: log matching
      sees a current log and has no view of how old the PostgreSQL data under it
      is. `test_lsn_acceptable_leaderless_window_bootstrap_fallback` already pins
      the permissive behaviour, for a good reason — the alternative was election
      livelock after a long leaderless window.

      **Decided: the tiebreak, and the liveness cost it was assumed to carry
      does not exist.** `evaluate_lsn_acceptable` now compares against the
      maximum *aged* report when nothing is fresh, and goes permissive only when
      `node_lsns` is empty — a cluster that has genuinely never reported. Aged
      is not absent: WAL positions do not go backwards, so a peer that once
      reported 100 MB has at least that much.

      The reason this looked like a safety-versus-liveness trade, and is not:
      **the node holding the aged maximum compares against itself, so its gap is
      zero and it always passes.** Some candidate is therefore always electable
      no matter how stale the data is, which is exactly the property the blanket
      permissive rule existed to protect. `test_aged_tiebreak_always_leaves_
      someone_electable` pins it directly, and
      `test_lsn_acceptable_leaderless_window_bootstrap_fallback` still passes
      unchanged — its three nodes share one aged LSN, so their gap is zero too.

      `test_stale_restored_node_loses_a_leaderless_election` is the safety
      assertion H-11 asked for: a node 50 MB behind, all reports aged, must
      lose — and its peers at the aged max must still win, so the rejection is
      not vacuous. Shown red by stubbing the comparison to `false`, at which
      point the stale node wins and the test reports exactly that.
      _Closes_ RW-7 · _Effort_ M

- [x] **H-12 — Harden `overnight_test.py`.** All three criteria met, and
      `PENDING_FAULT_MIGRATION` is now empty — every harness routes through the
      primitive layer.

      **Literal docker names** are gone from all 23 sites. The ten lifecycle
      faults call new primitives (`kill_container`, `start_container`,
      `restart_container`, `pause_container`, `unpause_container`) which resolve
      the container through the compose project and verify their own effect;
      the thirteen `docker exec pgbattery-node{N}-1` sites became
      `docker compose exec -T node{N}`. `ContainerRunState` moved into the
      primitive layer as the canonical copy, with `started_at` the field that
      separates an observed restart from an assumed one: `docker kill` against
      an already-dead container exits 0 and changes nothing, and that no-op is
      now a `FaultEffectNotObserved` rather than a pass.

      **The health oracle was broken in the permissive direction.** It asked
      `"HEALTHY" in stdout` — which is also true of `UNHEALTHY` — and counted
      `"LEADER"` substrings, which any `NOT_LEADER` line inflates. A cluster
      that never recovered read as healthy, and every scenario after it was
      measured against a cluster nobody had checked. Health is now the
      single-writer property itself: exactly one node reporting
      `pgbattery_lease_valid`, with every node answering. `get_leader_node` had
      the same substring bug and now reads the lease gauge, so it can no longer
      hand the load generator a fenced ex-leader as a write target.

      **Load now runs during every fault**, not just the one scenario that had
      it. Nine of ten faults hit an idle cluster, which structurally cannot
      observe a lost acked write or a write accepted by a node that had already
      lost authority — none of those exist without a client. The seq
      bookkeeping was already there and only one scenario fed it. The load
      thread re-resolves the leader each iteration, because "the leader went
      away" is usually the fault under test, and stops before the recovery wait
      so the oracle is not measuring a state it keeps provoking.

      Also fixed while here: the cascade scenario restarted _every_ node rather
      than the ones it killed, quietly reviving a survivor that had died for an
      unrelated reason and making the cascade look cleaner than it was.

      **Not yet run against a live cluster** — this is a code and oracle change,
      gated by unit tests and the lint ratchet.
      _Closes_ Class A1 for the last harness · _Effort_ M

- [x] **H-13 — Measure the fencing failure tail.** `testing/fencing_tail.py`
      drives the leader into a state where fencing may not be able to complete,
      with the dual-writability prober racing real writes at all three internal
      PostgreSQL ports for the whole window. Both modes pass: L1 held, and the
      node stopped being a write authority.

      **Breaking PostgreSQL is not enough, and finding that out was the first
      real result.** SIGSTOPping every postgres process leaves pgbattery running
      with its Raft leadership and a valid lease — and a node that still *holds*
      write authority has nothing to fence, so no escalation is expected. The
      first run sat wedged for 210 s with no restart, which is correct behaviour
      for that state rather than the failure tail. Each mode now also isolates
      the node from its peers, costing it quorum, so the lease expires and the
      enforcement loop must act through a PostgreSQL that may not answer.

      **Connection exhaustion does not reach the tail either, and that is worth
      knowing.** `superuser_reserved_connections` is 3 and pgbattery connects as
      a superuser, so PostgreSQL keeps slots available for exactly this kind of
      administrative work: the fence still completes. The mode is kept because
      "connection exhaustion cannot starve the fence" is a property worth
      holding, not because it breaks anything.

      So the assertion is the safety property rather than one mechanism: the
      node must stop accepting writes, whether by fencing itself read-only or by
      escalating to process exit and container restart. Which one fired is
      reported. Demanding a restart specifically failed a node that had fenced
      cleanly — the assertion was wrong, not the system.

      Ordering is enforced rather than assumed: the prober's startup DDL needs a
      writable node, and these modes leave none, so the script waits for a row in
      the probe table — proof a round landed — before applying the mode. A
      backend in uninterruptible I/O is still not covered; it needs a blocking
      device, which neither `lazyfs::torn-op` nor anything else in H-25
      provides.
      _Closes_ RW-4 · _Effort_ M

- [x] **H-14 — Measure what `pg_rewind` discards.**
      **Done when** a case records acked writes, forces async fallback and rewind,
      and reports how many acked writes were destroyed. That number either is zero
      or becomes a documented RPO bound.
      _Closes_ RW-3 · _Effort_ M

      **It is not zero. Measured: 20 of 20 acknowledged writes destroyed.**

      `rewind_loss.py` severs streaming replication with `partition_channel`
      while leaving Raft healthy, waits for `pgbattery_replication_sync` to
      report the async fallback, writes acknowledged rows in that window,
      deposes the leader, lets the survivors elect, and counts how many of those
      rows exist afterwards. On a live three-node cluster: fallback engaged, 20
      rows acknowledged, node3 deposed, node2 elected, **zero survivors**.

      So the documented RPO bound for the fallback window is: _every write
      acknowledged while `synchronous_standby_names` is empty can be lost_,
      bounded not by zero but by `PG_REWIND_DIVERGENCE_THRESHOLD_BYTES` (16 MiB)
      of diverged WAL. That is the trade the fallback makes deliberately —
      availability over durability when no replica is streaming — but the cost
      was previously asserted rather than counted, and the rewind threshold's
      justification ("synchronous replication is holding these elsewhere") is
      false in exactly this window.

      The fault has to be installed on the standby and with real WAL in flight:
      the leader streams continuously while a standby answers only every
      `wal_receiver_status_interval`, and a row-at-a-time load through
      `docker exec` is about one write a second — far too little for the DROP
      rule to match anything inside its settle window. The primitive refused
      both weaker attempts rather than reporting a partition that never
      happened.

      Reconfirmed since at a different width: 30 acknowledged in the fallback
      window, node2 deposed, node1 elected, **30 of 30 destroyed**. The bound is
      the window, not the count.

      `test_rewind_loss.py` covers the verdict logic, and specifically its three
      refusals. The measurement is only meaningful when the fallback actually
      engaged, leadership actually moved, and something was actually
      acknowledged; each of those reports SKIP, and a SKIP must never render as
      "MEASURED: 0", which is what a run that tested nothing would otherwise
      look like.

- [x] **H-15 — Unblock the supervisor mutex in `demote()`.** It was held across
      stop, rewind, and recovery, so the 100 ms lease tick stalled behind it.
      **Done when** the stall is measured first, then removed, and a test asserts
      the lease tick keeps its period during a demote.
      _Closes_ RW-9 · _Effort_ M

      Measured before touching anything, via
      `pgbattery_lease_tick_lock_wait_seconds`. Steady state is ~9 µs a tick.
      One leadership transfer added **8.3 s** of lock wait and left the loop
      with 190 ticks where 25 s owed 250 — sixty ticks simply did not happen.

      The tick now bounds its acquisition at one interval and skips
      (`pgbattery_lease_tick_lock_timeouts`) rather than queueing behind a
      lifecycle operation. Waiting longer bought nothing: the work a tick would
      do is re-derived from scratch by the next one, and skipping never fences
      less than blocking did — a blocked tick fences nothing either. The skip is
      safe because every path that holds the lock this long fences first
      (`demote` sets read-only before stopping, `promote` arms it before
      `pg_ctl promote`), so a tick that cannot look is looking at a node that is
      already not writable.

      Same transfer on the fixed build: 2.19 s of total wait — 21 skips at the
      100 ms bound — and **273 ticks in 25 s**, so the cadence is kept.
      `a_held_supervisor_lock_does_not_stall_the_tick` pins it, and goes red
      (blocking 10 s) against an unbounded acquisition.

      Two things that test did not say, now asserted alongside it. A skipped
      tick must not count as a _failed_ fence: a demote long enough to skip
      `FENCE_FAILURE_SHUTDOWN_THRESHOLD` ticks would otherwise shut the node
      down for being busy. And the tick after the lock frees must actually
      fence — `the_tick_after_the_lock_frees_fences_normally` — since "gives up
      within its period" is equally satisfied by a loop that gave up for good.

- [x] **H-45 — The lame-duck window must outlive nothing.** A leadership
      transfer stops the leader's heartbeats and sleeps a full lease so the
      target's vote is not rejected. That drain is safe by construction —
      openraft refuses votes while a follower's lease is live. It is also
      exactly what makes every follower eligible the moment it ends, and the
      code then spent up to `TRIGGER_ELECT_CLIENT_TIMEOUT_SECS` (12 s) plus a
      2 s poll inside that window, waiting on the target's supervisor lock.

      Against a one-second `election_timeout_min` this is not a race, it is a
      certainty whenever the target is busy. CI caught it as
      `rapid-leadership-transfer-cascade` failing with a 502: node2 became
      leader, took the next transfer 32 ms later, went silent, and node3 elected
      itself 3.7 s in. node1 — the transfer target, mid-demote — had pinned
      node2 as its `pg_rewind` source, waited 10 s for a node that had stopped
      being leader, left PostgreSQL stopped, and was killed by its own health
      watchdog.

      The readiness wait now happens **before** heartbeats stop, over
      `/internal/elect-readiness`, where it costs the cluster nothing;
      `/internal/trigger-elect` keeps the same check under a 250 ms bound
      because only "did this change during the drain" is still open by then.
      `lame_duck_budget_after_drain_ms()` sums what remains and a constants test
      pins it under `DEFAULT_ELECTION_TIMEOUT_MS`. A target that cannot serve is
      refused with a 409 and the leader never goes quiet, which is the clean
      refusal the cascade case should have been asserting all along.

      The comment that had made it look safe is worth keeping as a warning:
      _"Followers won't start their own elections during this window — openraft
      gates follower election start on lease expiration too."_ True, and
      backwards: draining the lease is what removes that gate.
      _Related_ RW-9 / H-15, which is the same mutex seen from the demote side.

- [x] **H-46 — Pin the names one harness calls on another.** `run_elle_matrix.sh`
      drives fault waves 2..N from a sibling process that imports
      `linearizability_register` and calls into it by attribute. Nothing joined
      the two files, so H-03's split into `linreg/` left `find_leader` out of the
      re-exports and every attack died on `AttributeError`.

      The verdict logic behaved: each attack reported ERROR rather than a
      verdict, and the run exited 2. What failed is when. The wave driver runs
      only under `ELLE_PROFILE=full`, which is nightly, so PR CI exercised a
      different path and stayed green across the refactor and every commit after
      it. Coverage that only one profile reaches is coverage a refactor can
      remove without argument.

      `lint_matrix.py` now parses the script's embedded Python and checks every
      attribute it reaches for against the module's top-level bindings, read from
      the source rather than imported so the check does not need psycopg to run.
      It refuses a script it finds no Python in, because a name check that
      matches nothing is the defect it exists to catch.

- [x] **H-16 — Join and rejoin edges.** Basebackup against a leader that gets
      deposed mid-copy, orphan slots pinning WAL, and a learner registration
      surviving a mid-join crash.
      **Done when** each edge has a case, and orphan-slot WAL pinning is asserted
      bounded rather than assumed.
      _Closes_ RW-10 · _Effort_ M

      `join_edges.py` drives four cases. Orphan-slot pinning is bounded against
      the reconciler's own interval, read from `constants.rs` rather than
      restated: a slot for a node id that is not in membership is dropped in
      13 s against a 90 s budget, and an operator's slot beside it is left
      alone. The case is deliberately two slots, because the first version
      created `pgbattery_node_99` — a name this cluster never mints — and
      reported a product failure that was entirely its own: the reconciler is
      built not to touch what it did not create.

      **A wiped bootstrap node impersonated the cluster.** `deposed-mid-copy`
      wiped `node1`, whose deployment command carries a standing `--bootstrap`.
      An empty state directory sends that down the `initdb` branch, so it came
      back holding a PostgreSQL lineage the cluster had never written to, under
      the node id the cluster still listed, on the address the other two are
      configured to join through. Two failures followed.

      Its own data directory could never follow: `pg_rewind` compares control
      files on connect and refuses two lineages outright ("source and target
      clusters are from different systems"). That failure is pre-copy, so the
      demote path restarted PostgreSQL and retried, the lease expired against a
      postmaster that was not up, `Fence failures exceeded threshold` shut the
      process down, and the supervisor restarted it — once a minute, forever.
      Observed identifiers: cluster `7675741024400711708`, impostor
      `7675750886508929058` on timeline 1.

      Worse, `node2` believed it. Resuming from its own valid Raft state, it
      asked its configured peer whether it was still in the committed
      membership; the impostor's cluster of one said no; `node2` deleted
      `raft.db` and fell through to the fresh-join path, which needs `join-info`
      from the peer that was restart-looping. One node with an empty disk took a
      healthy node's consensus state with it, and the cluster lost quorum.

      Both are fixed. `Error::ForeignDataDirectory` is now its own error rather
      than a string in `Error::Postgres`, and it is the one rewind failure the
      supervisor resolves rather than reports: `reprovision_from` discards the
      directory and clones the leader's. Safe precisely because it is foreign —
      a lineage this cluster never wrote to cannot hold a write this cluster
      acknowledged — and a diverged _but related_ history still goes to the
      divergence gate, which refuses rather than discards. Separately,
      `GET /api/v1/cluster/identity` publishes each node's lineage from its
      control file, and a node will not act on a membership answer from a peer
      whose lineage differs from its own: unknown on either side is not a
      mismatch, so a witness and an unprovisioned node still rejoin normally.

      `bootstrap-wiped` is the regression case: wipe the bootstrap node and
      require it back on the cluster's lineage, with membership unchanged and
      every peer still serving. It fails against the previous binary — the node
      never leaves its own lineage — which is what makes it worth running.

- [x] **H-17 — Sweep commit-probe correctness around COMMIT.** A wrong answer
      manufactures a phantom commit or a duplicate retry.
      **Done when** the probe is exercised at every offset in the neighbourhood of
      COMMIT and each answer is checked against ground truth.
      _Closes_ RW-12 · _Effort_ M

      **The probe had never worked.** `txid_status()` takes `bigint`, and the
      SQL handed it the `xid8` its own xmax guard compares against, so every
      call raised "function txid_status(xid8) does not exist" and the endpoint
      answered `500` with `status: null` — for every transaction, always. A
      client whose connection died mid-COMMIT could not learn whether its write
      landed, which is exactly the position RW-12 describes. `pg_xact_status`
      is the `xid8` variant and answers correctly.

      The existing unit test asserted the SQL's *shape* — guard before call,
      digits-only interpolation — and a shape is not an execution, so it passed
      throughout. It now also pins the function name and its argument type.

      `commit_probe_sweep.py` severs the backend at a sweep of offsets past the
      moment COMMIT starts, and compares the probe's answer to whether the row
      is actually there: `committed` must mean present, `aborted` must mean
      absent. The trigger is protocol state —
      `pg_stat_activity.query ILIKE 'COMMIT%'` — because timing the sever by
      wall clock includes `docker exec` startup and put every sever well past
      the commit record. Twelve offsets from 0 to 250 ms, twelve definite
      answers, all matching.

      Two harness faults on the way, both of which invented disagreements:
      statements sent as one `-c` string arrive as a single query, so COMMIT
      could not be seen starting; and trial tags repeated across runs against a
      table that was never truncated, so a row from a previous sweep answered
      "did this transaction land" for one that had just aborted.

      Not exercised live: the `aborted` arm. Commits here are fast enough that a
      sever at these offsets always lands after the record is durable, so every
      trial committed. `test_commit_probe_sweep.py` covers both wrong answers
      directly; producing a live abort needs the commit slowed inside its
      critical section, which is a fault-injection capability the harness does
      not have yet.

- [x] **H-18 — Drive the failover-anchor coalescing race live.** The pure
      functions were unit-tested; a missed clear or missed re-stamp under
      coalesced watch transitions would make the hold-down read an ancient
      anchor and promote immediately.
      **Done when** coalesced transitions are forced against a live cluster and
      the anchor lifecycle is asserted, not just its arithmetic.
      _Closes_ RW-8 · _Effort_ M

      The anchor was internal state, so nothing outside the process could say
      whether it had been cleared — only whether a hold-down happened to fire,
      which is a consequence and not the thing. `/debug/state` now reports
      `failover_anchor_age_ms`, and `failover_anchor.py` drives leadership
      churn while sampling every node's anchor through the whole failover.

      The assertion is on the anchor: a settled cluster carries none older than
      three lease durations, because an anchor that survives is exactly what the
      hold-down would later read as ancient. `stale_anchors` and
      `cluster_settled` are pure and inverted in `test_failover_anchor.py` —
      including that two nodes agreeing while a third is unreachable is not
      agreement. Live: two rounds, anchors observed during both failovers,
      cleared on every settled cluster.

      One harness fault worth recording: "settled" originally required all three
      nodes to answer, which can never hold while one is deliberately killed, so
      the loop burned its whole window every round and the run took 364 s
      instead of 10 s. The kill window now asks whether the survivors agree; the
      full-set check is reserved for after the node returns.

      Partially exercised: the re-stamp half. Anchors are observed during
      failover, but forcing the specific `Leader(other) -> None -> Leader(self)`
      coalescing that swallows the edge is not something this harness can demand
      — it samples for it rather than causing it.

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

- [x] **H-21 — Emit a metric labelled by statement node type whenever the
      unmodeled arm is reached.** Done.
      `pgbattery_gateway_unmodeled_statements_total{node_type}`, documented in
      `docs/DEPLOYMENT.md` as the input to H-23.

      The label is the parse-node variant name, so cardinality is bounded by the
      enum (~200) and nothing derived from client text reaches a metric. It is
      read off the derived `Debug` by a sink that stops at the first `(`, which
      avoids formatting the parse tree; two tests pin that assumption so a change
      in how `Debug` is derived fails rather than silently relabelling.
      _Blocks_ H-23 · _Effort_ S

- [x] **H-22 — `CallStmt` arm added; `CALL` is non-migratable.** The procedure
      body lives in the catalog rather than the parse tree, so `CALL` is opaque
      for the same reason `DO` is — and a procedure may additionally `COMMIT`
      mid-body, which `DO` cannot.

      This was flagged as needing sign-off, and the reason it did not get one is
      worth stating: the codebase already documents its polarity for anything
      the analyzer cannot see into — "severing is the safe direction; silently
      migrating a maybe-wrong replay set is not" — so this follows existing
      policy rather than setting new policy.

      **The cost is real and is availability, not correctness**: a
      stored-procedure-heavy workload now loses its connections on every
      failover instead of migrating them. If that proves too expensive, the
      narrower fix is to consult `prokind`/`provolatile` for the target
      procedure, not to widen the fallback. That trade-off is recorded at the
      arm itself so whoever pays the cost finds the reasoning.

      The arm and the gate landed together, per H-20's note — `call` was not a
      prefilter keyword, so the arm alone would have been dead code that still
      passed its own test. `CALL refresh_totals()` is in
      `test_session_state_prefilter_admits_every_gated_statement`, and
      `test_analyze_query_flags_call` pins that `SELECT refresh_totals()` — a
      function call, not a `CALL` statement — stays on the hot path, which is
      the over-match that would have made this genuinely expensive.
      _Effort_ S

- [x] **H-23 — Fallback polarity: no change, and the reason is now verified in
      code rather than argued.** The outcome this task allowed for.

      The decisive point is structural, not empirical, so it does not wait on
      production data: `SELECT my_func()` parses to a `SelectStmt`, which has its
      own `classify_statement` arm and is therefore `Modeled`. It **never reaches
      `_ => StatementClass::Unmodeled`**. Flipping the fallback to
      non-migratable would pay the full cost — a one-way ratchet for the
      connection's lifetime, so one early `ALTER TABLE` or `VACUUM` permanently
      disqualifies a session a pooler then holds for hours — and would not touch
      the deepest hole, because the deepest hole is not in that branch.

      The function-body hole is now in Accepted risks with what closing it would
      actually take: catalog introspection of the target function on the hot read
      path, which is the one place the design deliberately keeps off
      `libpg_query`.

      `pgbattery_gateway_unmodeled_statements_total{node_type}` (H-21) remains
      the right instrument, and its value is unchanged by this: it tells us which
      node types are actually hit in production, which is how a *specific* arm
      gets prioritised. That is a better use of the data than a wholesale flip.
      _Effort_ M

### Wave 3 — Prove durability (Tier 3)

Touches the container image. The single most likely place to find a genuine RPO
violation, and the first thing a Jepsen analysis would reach for.

- [x] **H-24 — Lost-unfsynced-writes on crash.** Done, and not with the tool this
      task originally named. **libeatmydata cannot do this job.** Making `fsync()`
      a no-op still leaves the write in the _host_ page cache, and SIGKILLing a
      container does not discard host page cache, so the data is all still there
      on restart. A durability suite built on it passes unconditionally. LazyFS
      holds un-fsynced writes in its own userspace cache, which dies with the
      process, so the loss is real.

      `testing/durability_crash.py` against `docker-compose.lazyfs.yml`, where
      PGDATA is a LazyFS mount. Red-green on a live 3-node cluster:

      | Run   | Configuration                                      | Acked writes lost |
      | ----- | -------------------------------------------------- | ----------------- |
      | RED   | `synchronous_commit=off`, WAL flushers frozen      | 300 of 300        |
      | GREEN | default durability                                 | 0 of 300          |

      `cluster-crash` kills all three nodes at once, which is the only
      configuration in the repo where a *standby* that acknowledges a flush it
      never performed becomes observable — so it is the R2 test, not merely a
      harsher W1 one. `leader-crash` covers W1 with a survivor.

      **Three false greens surfaced building it**, each of which would have
      reported durability while proving nothing. A settle pause between the last
      ack and the crash defeated the inversion outright: `synchronous_commit=off`
      is still fsynced by the walwriter every `wal_writer_delay`, so a 1 s gap
      made all 300 writes durable and the oracle reported it could not detect
      weakened durability. `pkill -f` SIGSTOPped its own caller, because the
      pattern naming those processes appears on the command line of the shell
      running pkill. And `restart: unless-stopped` raced the kill — Docker
      restarted the first victim while the last was still being killed, so a
      whole-cluster crash quietly degraded into a rolling restart.

      _Closes_ Class A2 for fsync · _Effort_ L

- [x] **H-25 — Torn writes.** Injected by `arm_torn_write` in
      `fault_primitives.py`, driven by `testing/torn_write.py` (PostgreSQL) and
      `testing/torn_raft.py` (redb), gated by the `torn-write` and `torn-raft`
      jobs in `durability.yml`.

      dm-flakey turned out to be unnecessary. LazyFS injects torn writes
      natively — `lazyfs::torn-op` splits a write into N pieces, persists a
      chosen subset directly to the backing store, and then crashes itself so
      the rest die in its userspace cache. This matters beyond convenience:
      dm-flakey needs device-mapper on the host, which is unreachable on Docker
      Desktop, where the host is a LinuxKit VM.

      Demonstrated against a standalone PostgreSQL 18 on LazyFS. A one-row table
      keeps both tuple versions in the second half of its page while the header
      and line pointers sit in the first, so `persist=1::parts=2` leaves a new
      header over a stale tuple area — a genuinely torn page, not a truncated
      one. The file size is unchanged, which is why the size cannot be the
      oracle:

      | `full_page_writes` | Tear lands | Outcome                                                                                     |
      | ------------------ | ---------- | ------------------------------------------------------------------------------------------- |
      | `on` (default)     | yes        | **repaired** — the FPI overwrites the page during redo, the row reads its new value          |
      | `off` (inversion)  | yes        | **detected** — `page verification failed, calculated checksum 18391 but expected 32765`, FATAL |

      Neither is silent acceptance, so the contract holds either way. The
      inversion is what makes that meaningful: it proves the tear reaches
      PostgreSQL's data path rather than being a byte-level curiosity, and it
      isolates `full_page_writes` as the thing doing the repair. `initdb` in the
      `postgres:18` image gives `Data page checksum version: 1` with the exact
      flags `init_db` passes, so the detection half is not a lucky default.

      `testing/torn_write.py` runs exactly that, gated by the `torn-write` job
      in `durability.yml`. It refuses to report on the real assertion unless the
      inversion has gone red, and it verifies the tear reached the backing store
      before asserting anything about recovery — LazyFS's log is the truth
      source there, never the file size, because a torn write does not change
      the size. It runs against the `pg` service in `docker-compose.lazyfs.yml`,
      behind the `tornwrite` profile: one bare PostgreSQL, no pgbattery and no
      Raft, because a cluster would put a failover, a `pg_rewind` and a possible
      re-basebackup between the tear and the assertion.

      The Raft store now has its own LazyFS instance, so redb is reachable.
      `mount_lazyfs` in the entrypoint runs twice — PGDATA at 0700, the Raft
      store at 0750 — each with its own control FIFO, log and backing root, and
      `fault_primitives.LazyfsMount` keeps those four paths together so a
      harness cannot arm a fault on one instance and look for its effect on the
      other. Two instances rather than one over their common parent: a single
      one would mean a fault aimed at redb crashes the filesystem holding
      PostgreSQL, and no suite could then say which store the damage was aimed
      at.

      That also strengthened the dirty-crash suite, which previously could say
      nothing at all about Raft durability — `docker kill` could not discard a
      single un-fsynced redb write while the store sat on an ordinary
      filesystem, and openraft's storage conformance suite carried those pins
      alone. Both caches are now discarded before the SIGKILL, and cluster-crash
      stays green at 300/300 with its inversion red.

      Adding the mount also exposed a false oracle in `leader-crash`, which is
      worth recording as its own lesson. `--prove-oracle` there weakened
      `synchronous_commit` and expected acked writes to be lost — but only the
      leader's WAL flushers are frozen, because only the leader is killed, so
      the standbys flush the streamed WAL normally and a promoted standby still
      holds every acked write. The inversion could only go red by killing the
      leader before replication had made the data durable anywhere. It did go
      red for months, which was worse than failing outright: it read as the
      fault being proven when what had been proven was that the writer outran
      the WAL sender on that run. Slowing the write path with a second LazyFS
      mount was enough to flip it. `leader-crash` now refuses `--prove-oracle`
      and says why; the primitive's evidence comes from `cluster-crash`, which
      shares it and has no survivor to flush anything.

      **An inversion that can be won by racing is not an inversion.** It is a
      seventh instance of the Class A1 pattern above, in a new shape: not a
      fault that failed to inject, but a proof that passed for the wrong
      reason.

      redb is torn by the suite, on both roles. Arming
      `arm_torn_write(..., mount=LAZYFS_RAFT)` on `raft.db` fires on the next
      append — 2048 bytes of a 4096-byte write persisted — and the node comes
      back healthy in under a minute with every acked write readable and no
      corruption reported. redb tolerates it, which is the expected shape: its
      commit protocol rolls back to the last committed state, so a tear in an
      uncommitted page is a non-event.

      A tolerated fault proves nothing on its own, so the observable was
      checked for reachability. With `raft.db` mangled past repair, redb
      reports `All roots are corrupted` and pgbattery refuses to start rather
      than recreating it, saying why: a recreated store would rejoin the node
      as a voter without its persisted vote and log, which can double-vote in a
      term or lose committed entries. So the red is reachable through the same
      observable the green is measured on:

      | Damage to `raft.db`         | Outcome                                                                     |
      | --------------------------- | ----------------------------------------------------------------------------- |
      | one torn write, 2048 / 4096 | tolerated — node recovers, all acked writes readable, nothing logged           |
      | mangled beyond repair       | detected — `All roots are corrupted`, the node refuses to start and says why |

      `testing/torn_raft.py` runs this, gated by the `torn-raft` job in
      `durability.yml`, matrixed over both roles: a tear in a follower's store
      and one in the leader's are different failures, since only the leader's
      loss can take committed entries no one else has yet. Each job hunts a
      tear of at least 512 persisted bytes over up to twelve attempts and fails
      if none lands, so a run where the fault stopped working reads as a
      failure rather than as tolerance. The mangle case is proven red first.

      **The byte count comes from a log inside the container the tear kills**,
      which made that hunt read its own blindness as an absent fault. A tear
      crashes the Raft store's LazyFS and takes the container down with it, so
      the `exec` that reads the log is issued against a node that is mid-
      restart. A plain `exec_in` failing there returned no records, which is
      indistinguishable from a log that recorded no tear, and a follower run in
      CI failed with "the fault never fired" while reporting the victim refused
      to start with recovery instructions — a store redb could not open, which
      is the fault firing about as hard as it can. The read now goes through
      `exec_when_deliverable` and an unreadable log is carried as its own
      finding, so the failure message can say which of the two happened. A
      refusal on a store-damage shape establishes the damage on its own: no
      threshold on bytes judges a tear more strictly than the node declining to
      open what it was serving a moment ago. A refusal nobody can attribute to
      one of those shapes still counts for nothing.

      Two things about that inversion were learned the expensive way, and both
      are the Class A1 pattern again. It checked that `dd` exited 0 and never
      that the store had changed — and a store cannot be damaged from under a
      running node, because redb rewrites its header on the next commit and
      LazyFS flushes its own cached copy over whatever is written to the
      backing root behind it. The overwrite now happens with the node stopped,
      through a throwaway container on the same volume, and carries a marker
      that is read back. Before that it passed for whichever reason the timing
      happened to supply.

      **A node the cluster has not admitted deletes the damage itself.** The
      inversion chose its victim from `await_leader`, which returns the moment
      the bootstrap node calls itself leader — and it does that at a voter set
      of `{1}`, while the others are still running their initial join. A CI run
      mangled node2 in that window and node2 came back healthy with the marker
      gone, which read as the store being restored behind the harness's back.
      It was not. `run_join_flow` in `src/app.rs` wipes the local Raft store and
      joins fresh when the peer does not list this node, so pgbattery deleted
      the damaged store on purpose and never opened it. Nothing had refused
      because nothing had been read. The suite now waits for every node to be a
      committed voter in every node's view before damaging any of them, which is
      the same fact that branch turns on. The overwrite also refuses a path that
      holds nothing, because `dd of=` creates what it cannot find: against a node
      whose join has not yet reached redb, the marker would have been written
      into a file the harness invented and read straight back as damage.

      Both halves of that are worth keeping. Membership is a precondition for
      damaging a store, not a detail of convergence; and a green that comes from
      the product correctly discarding the fault is indistinguishable from one
      that comes from the product surviving it, unless the harness knows which
      state it started in.

      Its pass condition is a disjunction rather than an outcome: tolerated or
      refused, both fine. What fails is the third state — a node neither
      healthy nor refusing, still running and still voting on a store nothing
      vouched for.

      **Refusal is detected behaviourally, from the container restart-looping,
      not from a log string.** That was learned the hard way across two failed
      runs. A damaged Raft store reached an operator in three shapes, and
      matching on text meant every shape not yet seen read as "running fine":

      | Shape                                    | What the operator got         |
      | ---------------------------------------- | ----------------------------- |
      | `Raft DB corrupted — refusing to start`  | the reason and recovery steps |
      | redb `unreachable!()` panic in the btree  | a backtrace                  |
      | `Failed to create database: I/O error`    | a generic storage error      |

      All three were safe — the process dies rather than voting on a store
      nobody vouched for — but only the first was actionable, and that was a
      gap in pgbattery rather than in the harness. It is now closed; the three
      defects behind it are worth recording because each was invisible from the
      outside:

      - `Io(InvalidData)` was not classified as corruption. It is what redb
        returns when the magic number does not match, which is precisely what a
        torn write to redb's header produces — the damage this suite injects.
      - The unwind guard covered `create` but not table initialization, so a
        panic while walking a damaged tree escaped as a bare backtrace.
      - Every caught panic rendered as `Any { .. }`. The payload is a
        `Box<dyn Any>`, whose `Debug` discards the message, so even the path
        that did report corruption said nothing about what redb found.

      A fourth was reachable only at runtime: redb panics rather than returning
      an error, every runtime read and write goes through `storage_io` on the
      blocking pool, and a panic there arrived as tokio's "task panicked". That
      is the shape seen in practice, and startup-only handling could never have
      caught it. The classifier stays deliberately narrow — permission denied
      and a full disk are not corruption, and the recovery it prescribes
      destroys a store.

      A single torn write produced the third shape, so torn writes to redb are
      not reliably benign either.

      **The tears land on the committed root, which corrects an earlier note
      here.** Recording the offset rather than only the byte count showed every
      tear at offset 0 — 160 bytes persisted of a 320-byte write. Offset 0 is
      redb's database header, the structure its commit protocol rewrites to
      publish new transaction roots, so this is the case that exercises
      checksum validation rather than an uncommitted append. The write is small
      because the header is small, not because the root was being missed. redb
      survives it by double-buffering that header and falling back to the last
      copy that verifies, which is now demonstrated rather than assumed.

      Both `--target follower` and `--target leader` are green, the latter
      re-resolving leadership between tears because tearing the leader's store
      moves it. Writes driven during the tearing phase count toward the loss
      assertion — 350 acked, 350 surviving — rather than only the batch written
      before the first tear, which were the safest keys in the run.

      On the PostgreSQL side all three structures are now torn, matrixed in
      CI, each with its own inversion:

      | Torn         | Defence          | Result                                                  |
      | ------------ | ---------------- | --------------------------------------------------------- |
      | heap page    | full-page image  | repaired during redo, row reads its new value             |
      | WAL record   | record CRC       | recovered, every acked commit present                     |
      | `pg_control` | control-file CRC | refused to start, reporting `checksum` — never served     |

      `pg_control` took three attempts to aim, and the two failures are the
      point. Persisting the first half kept every meaningful field, so the file
      was undamaged. Persisting the second half left a stale head under a fresh
      tail — but the payload *and* its CRC both live in that head, so the file
      stayed internally consistent and merely older. That is by design: the
      payload occupies one sector, which is exactly why PostgreSQL treats
      control-file writes as atomic. Only splitting 8192 into 32 parts puts the
      boundary at 256 bytes, inside the CRC-covered region, where head and tail
      can disagree. **A tear that a structure is built to survive is not
      evidence that the structure detects tearing.**

      That run also exposed an ordering bug in the suite's own assertions: it
      checked for missing acked commits before checking whether PostgreSQL had
      refused, so a correct detection — which makes every row unreadable —
      scored as data loss. The contract is repaired-or-detected, so the checks
      now follow it: if PostgreSQL serves the database at all, every acked
      commit must be in it; if it refuses, it must name what it found. An
      unattributable refusal is a violation, because it is indistinguishable
      from unrelated breakage.

      redb's btree pages are torn too. The FIFO form of `torn-op` fires on the
      *next* write and hardcodes occurrence to 1, and in a quiet cluster that
      next write is nearly always the 320-byte header redb rewrites to publish
      a commit — four attempts running in one measured hunt. `--min-torn-bytes`
      skips those and retries, and the fifth attempt caught a 4096-byte write
      at offset 8192 torn at 2048: a page, not the header. redb tolerated it,
      450 acked and 450 surviving, one leader throughout.

      CI hunts rather than takes the next write, so a run that finds nothing
      larger in twelve attempts fails and says so. That would mean redb's write
      pattern had changed, not that the fault stopped working — the distinction
      `observed_writes` exists to make.

      **What this does not cover:** every result above is a tear at a write
      boundary LazyFS can see. Sub-sector tearing, where a single 512-byte
      sector is itself half-written, is a different fault that needs hardware
      or device-mapper support; `pg_control` is the structure where it would
      matter, since its payload fits in one sector by design.

      **Done.** Torn writes are injected and either repaired or detected, in
      CI, across PostgreSQL heap pages, WAL records and `pg_control`, and
      across redb's header and btree pages on both a follower and the leader.
      Every one carries an inversion that must go red first.
      _Closes_ Class A2 for torn writes

- [x] **H-26 — ENOSPC at the next WAL segment**, as distinct from a blanket volume
      fill. `testing/wal_enospc.py`, gated by the `wal-enospc` job in
      `durability.yml`.

      **pgbattery fences by dying.** With free space driven to half a WAL
      segment on the leader, 122 of 150 writes are refused, PostgreSQL fails,
      pgbattery exits, and leadership moves. Every acknowledged write survives —
      178 acked, 178 surviving, none lost — and the cluster takes writes again
      once space is freed, with nothing restarted deliberately.

      Two things had to be true before that number meant anything, and both
      were false on the first attempt:

      - **The workload has to force a segment roll.** With bare integer keys,
        all 200 writes were acknowledged while the disk was proven unable to
        allocate a 16 MiB segment — a hundred tiny inserts fit inside the
        *current* segment, so PostgreSQL never asks for the next one. The fault
        was aimed correctly and nothing pulled the trigger. Rows now carry a
        256 KiB `STORAGE EXTERNAL` payload, so the roll happens inside the
        window and the same writes that force it are the ones W1 is asserted
        over. The suite fails loudly on a run with no observable effect rather
        than reporting the green.
      - **The victim must not be the bootstrap node.** Bounded volumes are
        tmpfs, so a node that restarts comes back with an empty
        `/var/lib/postgresql`, and `node1` would bootstrap a fresh cluster with
        a fresh Raft log rather than rejoin — every assertion passing against a
        database that had discarded the evidence, presented as data loss caused
        by disk exhaustion. Leadership is moved off `node1` first, so the
        victim comes back as a follower and re-clones.

      The inversion is a no-fill run required to show **no** effect. That is
      what makes the signal attributable: writes failing and leadership moving
      in a cluster nobody touched would mean the same observation under a full
      disk proves nothing about ENOSPC.

      **L1 comes from the prober, not from leader observations.**
      `dual_writability_prober.py` runs across all three internal PG ports for
      the whole fill-and-recovery window — disk exhaustion is a plausible way to
      stall a leader past its lease, and the recovery is included because a
      victim rejoining after space is freed is as good a chance for two writable
      nodes as the election that replaced it. Leader observations are still
      reported and still never asserted on: two distinct leader names across the
      window are a leadership transition, not two leaders at once, and Raft does
      not promise every node learns of a new leader at the same instant. A
      violation fails the run; an oracle too blind to conclude leaves L1 out of
      the reported contracts rather than claiming it, so "saw no violation"
      cannot be mistaken for "holds".
      _Closes_ the ENOSPC class at segment allocation

### Wave 4 — Concurrency gaps (Tier 3.5)

- [x] **H-27 — Test the `snapshot_consistency` race.** Done, but not the way this
      task assumed, and the reason is worth keeping.

      Racing a builder against an installer does not work. The window between the
      installer's redb write and its state swap is a few instructions with no
      await point, so the interleaving essentially never occurs: a 60-round racing
      test passed just as happily with the lock removed. Instrumenting it showed
      why — every build landed either wholly before the redb write or wholly after
      the state swap, never between.

      The lock discipline is asserted directly instead. The test holds
      `snapshot_consistency` and requires the installer to make no observable
      change: neither redb's `last_applied` nor the in-memory state may move while
      the critical section is occupied, which is exactly the claim that both
      writes are inside it. A second test requires `build_snapshot` to block on
      the same lock. Deterministic, no timing dependence, and no test seam in the
      production path — the additions are entirely inside `mod tests`. Verified by
      removing each lock in turn; each mutation fails its own test.
      _Effort_ M

- [x] **H-28 — Measure host-to-bridge routability for the prober's `direct`
      transport.** Done. `--require-transport` fails the run unless `auto`
      resolved to the named transport, and all five `ha-ci` jobs set
      `PGBATTERY_PROBER_REQUIRE_TRANSPORT=direct` via the flag's `envvar`, so no
      matrix case had to change and local macOS runs still fall back as before.

      The fallback was invisible by construction: both transports classify
      byte-for-byte identically, which is exactly what makes local runs useful and
      what made a Linux fallback report as though the bridge had been routed. An
      explicit `--transport direct` now also fails fast rather than degrading into
      connection errors that the indeterminate-rate gate reports as inconclusive
      rather than as a misconfigured transport. Eight tests, both directions.
      _Effort_ S

### Wave 5 — Deterministic simulation (Tier 4)

The actual next level: reproducibility and schedule coverage Docker structurally
cannot provide. Aimed at `app.rs`, the least-tested and most dangerous file in
the repo.

- [x] **H-29 — `PgControl` seam added** (`src/governor/pg_control.rs`). The
      third seam, alongside `LeaseState`'s injectable `Clock` and
      `governor/network.rs` for transport.

      `ensure_follows`, `promote_local_postgres`, and `demote_to_leader` — the
      split-brain-prevention core, and the reason `app.rs` was the least-tested
      large file in the repo — are now generic over the trait. `ModelPg` scripts
      the answers and records the calls, so those paths run without Docker: the
      three new tests finish in 0.00 s against a model that a live cluster would
      need minutes and luck to put in the same state.

      One of them is the point of the whole exercise: **a failed
      `verify_promotion_safe` must not promote.** That check is what stops a node
      with a diverged timeline becoming primary, and no live-cluster case forces
      the branch — you would have to manufacture a divergence and time it.
      Against the model it is three lines.

      Design choices worth keeping: generic rather than `dyn`, because these are
      `async fn`s and `dyn` would need boxing or an `async-trait` dependency —
      monomorphising keeps the real path identical to what it was before the
      seam existed. And the trait exposes `terminate_client_backends()` rather
      than `execute_sql()`, because a general "run this SQL" method lets callers
      reach past the seam and would oblige the model to interpret SQL to stay
      honest.

`pg_stat_replication` is covered too, along with
`set_sync_standby_names` and the three slot operations, so
`ReplicationManager<P>` is generic and `ModelPg` can script the standby
rows the async-fallback gate counts. Making it generic turned out to be
contained — one construction site, not the propagation into
`ManagementApiState` that first appeared likely.

      Four pure decision functions (`plan_sync_replication`,
      `sync_state_confirmed`, `required_sync_standbys`,
      `plan_slot_reconciliation`) moved out of the impl block to module scope
      while doing it. They take neither `self` nor `P`, so keeping them there
      forced every caller — including twenty test assertions — to name a
      concrete `Supervisor` it did not otherwise care about.

      **What is still not model-testable, and why:**
      `check_and_update_sync_standbys` calls `has_raft_quorum()`, which reads
      `openraft::RaftMetrics`, and the struct holds a real
      `Arc<openraft::Raft>` as a field. So no `ReplicationManager` *method* can
      be driven by a model until there is a Raft seam alongside the PG one —
      that is H-31's territory, not something the PG seam can reach. The
      planning helpers those methods delegate to are unit-tested directly, and
      that is the honest extent of it: the seam is in place and the tick above
      it is not yet reachable.
      _Blocks_ H-31 · _Effort_ L

- [x] **H-30 — Inject the clock everywhere**, not in the lease alone. Every time
      comparison that gates write authority now reads `lease.read().now()`, and
      `testing/lint_clock_injection.py` fails the build if one stops.

      Four modules are guarded: `lease.rs` (expiry), `raft.rs` (metrics-watchdog
      fence, leaderless-recovery threshold and cooldown),
      `replication_manager.rs` (async-fallback grace — the gate that changes the
      durability guarantee), and `app.rs` (promotion hold-down). The lease was
      already the one injectable clock and already exposed `now()` for exactly
      this reason; the work was routing the rest through it, which meant giving
      the replication manager and the supervisor loop the `SharedLeaseState`
      they lacked.

      **The lint earned its place immediately.** The first pass converted every
      `Instant::now()` and looked complete; the lint then found seven surviving
      reads, five of them `.elapsed()`. That call is implicitly
      `Instant::now() - self`, so a value *stamped* from a `ManualClock` and then
      read with `.elapsed()` still consults the real clock — a half-conversion
      that type-checks, passes tests, and silently defeats the whole exercise.
      Two of the five were the watchdog-fence gate and the leaderless-recovery
      threshold.

      `state_machine.rs` is deliberately not guarded: LSN staleness rides in
      replicated Raft state and snapshots, where a monotonic `Instant` is
      meaningless across processes. That is the documented wall-clock exception.

      Non-safety reads inside a guarded module (a rate-limiter window, a
      histogram sample) carry a `// clock-lint: allow — <reason>` marker at the
      line rather than living in a registry, so the justification is reviewed
      with the code that depends on it. The lint's own inversions are checked:
      it flags a gate, respects a marker, and ignores `mod tests`.
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

      **Harness half done.** Every harness that draws randomly now draws from
      one seeded generator, records the seed in its artifact, and prints the
      command that replays it. `linearizability_register.py` and
      `overnight_test.py` already did; `ci_runner.py` draws nothing, so its runs
      were always reproducible.

      `correctness_lite.py` was the gap, and it was the worst kind: the seed
      existed as a parameter on `_chaos_storm_now` that nothing passed, so the
      storm seeded itself from `int(time.time())` and never said what it chose.
      The paused node and the whole transfer sequence came from the global
      generator. A B1-B4 violation could therefore be seen once and never
      again — the storm that produced it was unrecoverable the moment the run
      ended. The schedule and the transfer sequence are now pure functions
      (`chaos_storm_plan`, `bank_transfer_plan`) over an explicit `Random`,
      `results.json` carries `seed`, `attack` and a `replay` line, and the
      replay line is printed with the verdict on any failure.

      What is left needs H-31: the harness's own choices are reproducible, but
      the cluster's timing is not, so a replay drives the same faults at the
      same offsets against a system that may schedule them differently. Closing
      the criterion outright means a deterministic runtime, which is what H-31
      is for.

### Wave 6 — Connect the specs to the binary (Tier 5)

The four specs are non-vacuous, but the spec-to-code mapping is _comments_, so
nothing detects drift.

- [x] **H-33 — Emit structured state-transition events**: lease renew and expire,
      anchor stamp and clear, promote and demote, fence escalation.
      **Done when** the event stream is complete enough to reconstruct a failover
      without reading logs.

      Less work than it looks, and worse than it looks. `DebugEventBuffer`
      already exists, is served at `/debug/events`, and defines five emitters —
      `leader_change`, `fence_change`, `membership_change`, `sync_state_change`,
      `connection_migrated`. **None of them is called from anywhere outside its own
      module**, so the endpoint has always returned an empty list. An event API
      that reads as an event API and emits nothing is the same shape as a fault
      that reads as a fault and injects nothing.

      The reason is structural, not an oversight: the buffer lives inside
      `Arc<ManagementApiState>`, which `app.rs` constructs and then moves into
      `start_management_api`, so the transition sites cannot reach it. Two ways
      out — keep a clone of the `Arc`, or hold the buffer as its own
      `Arc<DebugEventBuffer>` shared by both. The second is cleaner.

      **Done.** The buffer is now its own `Arc<DebugEventBuffer>`, constructed in
      `app.rs` and shared with `ManagementApiState` rather than living inside it,
      so the emitters are reachable. A `run_transition_observer` task watches the
      existing `leader_rx` and `fence_rx` channels and records leader changes and
      fence transitions — including whether a fence is lease-driven or
      quorum-loss-driven, which is the distinction that tells an operator whether
      a new leader is coming to lift it.

      No safety path was edited, which was the point of observing the channels
      rather than threading the buffer through the transition sites: a bug in the
      observer can drop an event but cannot change a fencing decision.

      Two properties are pinned by tests, both of which failed before they
      passed. The observer must actually record — the whole defect was an event
      API that emitted nothing — and it must ignore a resend of an unchanged
      value, because `watch` fires on send rather than on change and a steady
      cluster would otherwise fill a bounded ring with duplicates and push the
      real transitions out.

      **Known limit, deliberate:** `watch` is lossy by design — it holds the
      latest value, not a queue — so a transition superseded before the observer
      wakes is not recorded. That is the correct trade for safety paths that must
      never block on a slow observer, and it is why this stream is a debugging
      aid rather than an audit log. H-34 should not treat it as a complete trace.
      _Blocks_ H-34 · _Effort_ M

- [ ] **H-34 — Validate real traces against the specs.** This turns "we have
      specs" into "the specs describe this binary", and directly closes the known
      delta where the model anchors the hold-down at the election instant while
      the implementation anchors at local leader-loss observation.
      **Done when** a harness run's trace is checked against `lease_fencing` and
      `raft_lsn`, and an injected drift is caught.
      _Closes_ exit criterion 5 · _Blocked by_ H-33 · _Effort_ L

- [x] **H-35 — Partial-quorum dynamics are modeled, and the counterexample is
      real.** The spec used to abstract quorum into a per-node ack counter that
      only the current Raft leader could reset, which made a deposed leader's
      staleness an axiom rather than a consequence — and made RW-1 unreachable
      by construction. Quorums are now explicit node sets: elections and
      heartbeat acks both name the majority that took part, and a node's term
      decides whether it can be in one. Quorum intersection and Raft's own
      election safety (`OneLeaderPerTerm`) are derived from that rather than
      assumed, and a deposed leader holding a quorum that excludes the winner is
      an ordinary reachable state.
      **Done when** the spec covers a deposed leader holding a quorum that
      excludes the winner, and TLC either proves safety or produces the
      counterexample.

      **It produced the counterexample, and it was a real L1 defect.** The
      promotion hold-down was anchored at the winner's first observation of
      leaderlessness. The instant it needs to bound is a different one: when a
      majority moved past the deposed leader's term, which is the vote the
      winner is campaigning for. Those coincide only in a prompt failover. An
      election that splits, is dropped, or is retried by the leaderless watchdog
      wins arbitrarily later — openraft has no pre-vote, and the watchdog exists
      precisely because elections do stall — and by then the anchor has counted
      out. The winner promotes at the moment it wins, while the deposed leader's
      last quorum ack is one message flight old and its quorum-loss self-fence
      is still up to `QUORUM_TIMEOUT_MS` away. Two write authorities: contract
      L1, FATAL. The same hole swallowed a second case, a node that restarts
      into a leaderless window: with no prior leader ever observed it stamped no
      anchor at all, so `promotion_lease_holddown(None, ..)` waived the gate
      entirely.

      Fixed by anchoring on the newest term this node has observed rather than
      on the leaderless edge (`should_anchor_term_advance`), which costs nothing
      in a prompt failover — the candidacy and the leader→none edge are the same
      instant — and one hold-down per extra election round in exactly the case
      that needs it. Two exclusions keep it from over-arming: a term observed
      while a *different* node leads belongs to a failover this node is not
      completing, and term 0 means there is no predecessor at all, so cluster
      bootstrap does not hold itself down against nobody.

      What the model does **not** bound, and no lease scheme can, is message
      delay. `VoteDelay` is now an explicit constant and `HoldDown >= VoteDelay
      + QuorumTimeout` is the load-bearing inequality; a vote that takes longer
      than `DEFAULT_LEASE_DURATION - QUORUM_TIMEOUT_MS` (2 s − 1 s) to reach a
      majority breaks it. That assumption is stated in the spec header instead
      of hiding inside an abstraction.

      The counterexample is a checked artifact, not a paragraph telling the
      reader to edit a constant and see for themselves.
      `lease_fencing.inv-anchor-not-restamped.cfg` names the invariant it
      expects to break, and `make -C tla check` fails if TLC ever stops breaking
      it — as it fails if such a model fails for some *other* reason, since a
      violated `ASSUME` is not a counterexample. Both inversions of that target
      were verified by probe configs (one that passes, one with no
      `EXPECT-VIOLATION` header) before they were deleted.

      Found while measuring the baseline: `make check-<spec>`, the documented way
      to check one spec, was a silent no-op that exited 0. GNU make does not
      apply pattern rules to phony targets, and every `check-<spec>` was listed
      in `.PHONY`.
      _Closes_ RW-1 · _Effort_ L

- [x] **H-36 — Larger models nightly**: 5 nodes and higher term bounds. Current
      configs are 3 nodes with small bounds, and `raft_lsn.tla` abstracts the Raft
      log away entirely, so its `ElectionSafety` is the textbook Raft theorem
      rather than a statement about openraft.
      **Done when** the nightly job checks the larger configs within its budget.

      Every spec now has a `<spec>.large.cfg` beside its PR config, run by
      `make -C tla check-large` from a nightly job at 03:40 UTC. Measured on an
      M4 with `-workers auto`, all four clean:

      | Spec                     | Nightly config               | Distinct states | Time   |
      | ------------------------ | ---------------------------- | --------------- | ------ |
      | `lease_fencing`          | 3 nodes, MaxTerm 4           | 1.5M            | 55 s   |
      | `raft_lsn`               | 4 nodes, MaxTerm 3, MaxLSN 3 | 7.0M            | 31 s   |
      | `commit_probing`         | 3 connections, MaxTxid 4     | 7.8M            | 29 s   |
      | `timeline_verification`  | 4 nodes, MaxTimeline 4       | 1.1M            | 5 s    |

      Node count is not the axis to push everywhere, which is the substance of
      the earlier measurements and of several taken since. `raft_lsn` carries a
      term and an LSN per node, so at 5 nodes TLC was still 66M states from done
      after ten minutes; `MaxLSN` buys more, and a third LSN value is what makes
      "behind by more than the threshold" and "behind by exactly the threshold"
      both reachable in one behaviour. `timeline_verification` at 5 nodes and
      MaxTimeline 4 completes but costs 58.5M states and 4.5 minutes on an M4 —
      an order of magnitude past the 4-node model for no new shape, and far past
      what a 2-core runner should spend.

      `lease_fencing` used to be the one that scaled cheaply to 5 nodes, and no
      longer is: H-35 replaced its ack counter with explicit quorum node sets,
      so every extra node multiplies the majorities each election and each
      heartbeat can range over. At 5 nodes it did not finish in 15 minutes at
      either MaxTerm 5 or MaxTerm 3. Three nodes already carries the minimal
      quorum intersection the safety argument turns on, so the nightly model
      spends its budget on a fourth term instead — enough for a node to be
      deposed, return, and be deposed again in one behaviour, which is where a
      stale anchor gets a second chance to surface.

      Two properties keep the target honest. A hard per-spec timeout (900 s)
      distinguishes "this model outgrew its budget" from a stuck runner, and a
      spec with no large config fails the target rather than being skipped —
      checking three of four would otherwise read as coverage the next time
      somebody adds a spec.

      Still true, and not addressed here: `raft_lsn` abstracts the Raft log
      away, so its `ElectionSafety` remains the textbook theorem rather than a
      statement about openraft. Closing that is H-34's business, not a bigger
      model's.
      _Effort_ M

- [x] **H-50 — `witness-topology` now waits for the cluster it builds.** Case
      nineteen of the control-plane nightly, and the first one the suite reached
      once the earlier blockers were gone. Two defects, and the second is why
      the first was hard to read.

      It waited for `nodes: 3` while running a four-node topology. The witness
      joins as a learner — `--voter` defaults to false and the compose command
      does not pass it — so membership is three voters plus node4, and
      `/cluster/nodes` is four. It stays four across the `pkill` of node3 as
      well, because registration is replicated `NodeInfo` rather than liveness.
      All three counts are now four; the voter count is untouched at three.

      What made that hard to see: `witness_state` is a **named** volume, and
      nothing cleaned it. `down -v` skips it because the witness is not in the
      default profile, and `rm -v` removes only anonymous volumes. A second run
      therefore started a witness whose data directory was 151 MB ahead of a
      freshly bootstrapped leader, `pg_rewind` refused the divergence exactly as
      it should, and the witness never joined at all — so consecutive runs
      disagreed about what the topology even was. Cleanup now empties the volume
      through a throwaway container on it, the same shape `wipe_node_state`
      uses, which derives the project from compose rather than naming it.

      Verified by running the case twice in a row, since one run in isolation
      could never have shown the pollution.

      Chasing the residue turned up a product defect behind it. Stopping the
      witness left it in `/api/v1/cluster/nodes` forever, because
      `change_membership` takes a node out of the voter set and touches nothing
      else: the API never committed `ClusterCommand::RemoveNode`, so the node's
      `NodeInfo` **and its LSN** stayed in the replicated state. The LSN is the
      part that matters — `node_lsns` feeds `max_cluster_lsn`, the bar every
      candidate is measured against, so a node removed while ahead raised that
      bar permanently against the nodes still in the cluster. Removal now
      commits it, and `/cluster/nodes` returns to three after the case where it
      used to stay at four.
      _Effort_ M

- [ ] **H-52 — torn-raft loses its cluster to a corrupt catalog, sometimes.**
      Both `Torn write (Raft store, ...)` jobs fail intermittently on the
      precondition rather than the tear: "the cluster was not a full voter set
      within 180s", with one node absent from every view. That node's log says
      why —

      ```
      ERROR: index "pg_namespace_nspname_index" contains unexpected zero page at block 0
      ```

      — and it shuts down on `FENCE_FAILURE_SHUTDOWN_THRESHOLD` because the
      fence cannot probe a server whose catalog will not open. The suite is
      right to refuse: tearing a node that is not a committed member measures
      nothing.

      Not reproducible locally — the follower target passes here first try, and
      the same jobs passed on the commit before and after one that failed. Two
      real defects were fixed on the way to finding this and neither is it: the
      tear bar demanded a write the workload cannot produce (H-48), and the OCI
      runtime's wording for a stopped container was read as a genuine failure
      instead of an undelivered exec.

      Worth knowing before chasing it: `reset_cluster` does `down -v`, so
      damage cannot cross the oracle phase into the real run, which points at
      the cluster's *first* convergence rather than anything the suite did.
      Both LazyFS instances live in one container, so killing it discards both
      caches — un-fsynced PGDATA is as exposed as un-fsynced redb, and an index
      page lost before its WAL is exactly this shape.
      **Done when** the suite either survives the corruption or reports it as
      the precondition it is, naming the node and its catalog error rather than
      a bare convergence timeout.
      _Effort_ M

- [ ] **H-51 — `tests_md_ref` points at a document that does not exist.** All
      eighty cases carry one, in the shape `"Control Plane 27. Witness Node 2+1
Topology"`, and `docs/TESTS.md` has never been in the repository — not
      deleted, never added. The field reads as the case's specification, which
      is exactly what somebody reaches for when a case and the product disagree
      about what it should do; three of the cases fixed this week disagreed, and
      the reference was no help with any of them.
      **Done when** the field either resolves to something, or is removed and
      the descriptions carry the intent on their own — and `lint_matrix.py`
      checks whichever is chosen, since an unchecked cross-reference is how this
      one survived eighty cases.
      _Effort_ S

- [ ] **H-49 — Run every `ha-parallel` case, or say which are not run.**
      `ha-ci.yml` hardcodes five case names into the parallel matrix while the
      suite holds seventeen, so **twelve cases are executed by no workflow at
      all** — written, maintained, counted in `--list`, and never run. Nothing
      reconciles the two lists, which is the same drift `lint_matrix.py` exists
      to catch everywhere else.

      Running the suite locally found two of the twelve broken.
      `lsn-leaderless-livelock-recovery` is fixed here: `metric_leader_count`
      polled every node and raised on a refused connection, so the assertion
      could not be used by any case that kills a node — which is what it was
      written for. A refused port now counts as not-leader (nothing is
      listening, so nothing is leading) while a timeout or a missing metric
      stays fatal, because a node that is up but silent could be the second
      leader this must never miss.

      `backup-restore-valid` is **not** fixed, and has never been able to pass:
      it POSTs a full restore to node1 while node1 is the leader, and the API
      has refused exactly that since the initial commit — "Refusing full restore
      on the current leader: it overwrites the live primary's data directory."
      The case and the guard were committed together. Making it pass is a
      design question rather than a repair: a full restore is only accepted on a
      standby, and a restored standby is re-synced from the leader, so
      "the cluster returns to the pre-insert snapshot" is not what the supported
      operation does. What is assertable — that a standby restore is accepted
      and does not roll the live cluster back — is a different claim from the
      one the case makes.
      **Done when** the matrix is derived from the suite rather than restated,
      every case in it runs, and any case deliberately excluded says why.
      _Effort_ M

- [ ] **H-48 — Tear a redb data page, not only its header.** `torn_raft.py`
      arms LazyFS for the next write to `raft.db`, and every write it
      intercepts is the 320-byte header at offset 0: redb maps the data region
      rather than writing it through the FUSE path, so a btree page cannot be
      torn this way at all. The suite therefore proves the header case — which
      is real, since the header is what redb reads to find everything else, and
      a torn one is tolerated cleanly today — and nothing about a page the store
      has committed.

      This was hidden behind a threshold. `--min-torn-bytes 512` asked for a
      tear the workload cannot produce, so the job failed intermittently saying
      "redb's write pattern changed" when the pattern had never been different.
      **Done when** a torn write lands on a committed btree page and the node is
      shown to tolerate it or refuse to start — which needs LazyFS to intercept
      the mapped region (`msync`), or redb configured off mmap for the test.
      _Effort_ M

- [ ] **H-47 — A counterexample model for every spec.** `make -C tla check` now
      runs the `<spec>.inv-<name>.cfg` models and fails if one stops producing
      the violation it names, but only `lease_fencing` has one. The other three
      rest on the same argument every other assertion in this repo is refused:
      an invariant nobody has watched fail is an invariant whose passing means
      nothing, and TLC's own report of "no error" is exactly what a vacuous
      model produces.

      Each needs a modeled-defect switch, because none of the three can be
      broken by constants alone — `commit_probing`'s invariants are about the
      probe logic, `timeline_verification` checks a monotonicity property, and
      `raft_lsn`'s gate is already deliberately advisory. That is spec work, not
      config work, which is why it is not folded into H-35.
      **Done when** `make -C tla check-counterexamples` covers all four specs and
      fails when any spec lacks one, the way `check-large` already does for the
      nightly configs.
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

- [x] **H-38 — Pair every FATAL contract oracle with an inversion test.** The
      Contract-to-Test Index gained an **Inversion** column, and
      `lint_matrix.py` now fails if any FATAL row leaves it empty. Four
      contracts had no inversion at all and now do:

      - **W3** — `assert-sanity-ddl` leaves `ci_ddl_atomic` present but without
        its `PRIMARY KEY`. W3's assertion accepts both "fully committed" and
        "fully absent", so it passes on an empty database: a `ddl-failover` run
        whose DDL never executed was indistinguishable from one that survived.
      - **S1** — `assert-sanity-commit-boundary` duplicates the autocommit row.
        That assertion is deliberately loose on the txn side (0 or 1 are both
        legal after an interrupted commit), so the autocommit count is the only
        thing pinning it down, and nothing had shown that half could fail.
      - **R1** — `assert-sanity-slot-leak` creates an inactive physical slot.
        Every slot on a healthy cluster is active, so the assertion passed
        trivially. Cleanup drops only the slot the case made, never a managed
        one.
      - **R2 / W1** — `assert-sanity-acked-dup` seeds exactly 60 rows of which
        10 are duplicates. `assert-sanity-acked` seeds 5 and returns on
        `total_rows <> 60`, so the **duplicate branch had never executed** —
        the at-most-once half of that assertion, and the half R2's sync path
        leans on, was unproven. Hitting the count exactly is what forces
        execution past the first branch.

      The check refuses to pass on a document it could not parse, so a table
      that moves or gets reformatted fails loudly instead of retiring the whole
      thing; a unit test asserts the real doc yields at least eight FATAL rows
      so the parser cannot silently match nothing.

      Live-gated: all 12 `ha-assert-sanity` cases pass, meaning each of the four
      new inversions was observed making its oracle raise.
      _Closes_ exit criterion 1 · _Effort_ M

- [x] **H-39 — Audit the risk-window register.** No row reads "No" or
      "Partially" without a task ID: every RW-1…RW-12 carries a "Tracked by"
      owner, and RW-5 reads "covered".

      The reachability column was re-derived from the code rather than trusted,
      which was the part deliberately left last — RW-5 was already stale when
      the register was written, the prober having made it reachable without the
      column noticing. Each row was checked against the capability it claims:
      RW-1 against `five_node_suite.py` (still a raising skeleton), RW-2 against
      the absence of any protocol-state-triggered wait in the primitive layer or
      runner, RW-4 against the SIGSTOP primitives, RW-5 against the prober's
      three-port coverage, RW-6 against the matrix (**no case drives a follower
      gateway specifically**), RW-7 against `lsn-leaderless-livelock-recovery`
      (asserts election succeeds, not that a stale node loses), RW-8 against the
      anchor predicates — which live in `governor/raft.rs`, not
      `state_machine.rs`, and are unit-tested as claimed — RW-9 against
      `demote()`'s mutex span, RW-10 against the rejoin cases, RW-11 against
      `SetSyncMode` (ten references in the state machine, **zero in the
      matrix**), and RW-12 against commit-probe timing.

      All twelve claims held. Two are worth stating plainly because they are the
      kind of gap a column can hide: `SetSyncMode` is modelled in Rust and
      exercised by no test case at all, and `sweep_around` — the primitive built
      for RW-12's byte-offset sweep — exists in `fault_primitives.py` with **no
      caller anywhere in the matrix**. The tool for the sweep is written; the
      sweep is not.
      _Closes_ exit criterion 4 · _Effort_ S

- [ ] **H-40 — Re-verify the whole document.** Re-read the escape classes and the
      register against the implemented state and delete what is no longer true.
      **Done when** the prose describes the system as it then is, with no gap left
      unclaimed.
      _Effort_ S

## Review pass — one fact, one place

An adversarial read of the whole hardening series, looking for two things: a
check that matches text where it could consult the thing itself, and a fact
written down more than once. Both fail the same way — quietly, in the direction
that reports coverage.

Nine findings, all closed. Grouped by what was actually wrong:

**A fabricated observation.** `do_cas` decided whether a compare-and-swap
committed by asking whether `1` appeared anywhere in the psql transcript. The
command merges stderr into stdout to classify rejects, so any diagnostic
carrying a `1` promoted a witness mismatch to a committed CAS — an invented
`:ok` in the history the linearizability checker then reports a FATAL violation
against. `_parse_first_int` had the same shape. Both now read the result set.

**A property checked against a copy of itself.** `encode_rpc_frame_for_fuzz`
reimplemented the wire format instead of calling the encoder the transport
uses, so the fuzzer's round-trip property would have kept passing after the
real encoder changed. One `append_frame_bytes` now.

**A format instead of a type.** The replication slot name was a format string
in seven places across three crates, and ownership was decided by
`starts_with("replica_")`. `ReplicationSlot` carries the node id and renders
the name; `Display` and `FromStr` are the only crossings of the string
boundary, so they are inverse by construction. A reconciler that fails to
recognise a slot it created drops and recreates it every tick, discarding the
WAL reservation the standby needs.

**A topology written down six times.** Service names, static addresses and
published ports were declared independently in five Python modules and
`ci_matrix.yaml` — two of them computed arithmetically and agreeing with
compose by coincidence. This is the failure this document opens with, one level
up: a harness addressing a docker object that is not there injects nothing and
reads as coverage, and the prober records an unreachable node as
`indeterminate`, which the L1 verdict treats as "no acceptance observed".
`testing/topology.py` derives all of it from the compose file, taking node
identity from the config each service mounts and voter membership from its
start command. Nothing falls back to a default: an unreadable compose file
raises rather than yielding an empty node list that turns every loop into a
no-op. `ci_matrix.yaml` still declares its own cluster, and a lint reconciles
it.

**Log strings shared with nothing.** `correctness_lite.py` reads L2 and L3 out
of container logs by matching strings that live in Rust `tracing` calls.
Rewording `"potential split-brain"` in `src/` would not have failed anything —
the grep would simply stop matching, and a run with the real signal in its logs
would report PASS. All ten markers are now pinned by lint.

**A lint that could stop scanning.** `lint_clock_injection.py` cut its scan at
the first `#[cfg(test)]`; a test-only helper above the trailing module would
have moved that cutoff up and silently un-guarded everything after it. A second
boundary is now a loud failure — and hardening it immediately showed the
assumption was already wrong, since `#[cfg(test)]` and `mod tests` sit four
lines of `#[allow(...)]` apart here. That lint also had no inversion of its
own, the one layer in the tree without a proof it can fail.

**An inversion column nothing resolved.** `check_fatal_contracts_have_inversions`
asked only whether the cell was non-empty, so a renamed or deleted case left
the contract still claiming a working oracle. Every backticked name now resolves
against the matrix, the filesystem, or the function names in the tree.

**A model that ignored its commands.** `ModelPg` recorded `set_readonly` and the
slot operations but never applied them, while the probes answered from the
untouched fields — so the convergence the seam was added to test could not be
expressed. Every command now changes the state its probe reports. The seam also
stopped short of the lease enforcement loop, leaving the most safety-critical
path needing Docker; it now covers `lease_enforcement_tick` end to end, and the
RW-4 escalation has a test, which `fencing_tail.py` cannot reach from outside
because it has no way to make `ALTER SYSTEM` fail on demand.

**A settle sleep pretending to be a wait.** `fencing_tail.py` slept 20 s between
modes and proceeded whether or not the cluster had taken the lease back, so the
second mode could measure the churn. It now waits for write authority and fails
loudly if it never returns.

Two things were considered and left alone, with reasons: `parse_netem` parses
`tc` output with regexes rather than `tc -json`, but it fails closed — an
unrecognised format yields zeros and the verifier rejects — and switching
depends on the image's iproute2 version, which cannot be checked without a
cluster. `PgControl::terminate_client_backends` returns the raw psql string
rather than a count; the caller only logs it, and parsing a persistent psql
session's multi-line output into a `u64` would turn a formatting quirk into a
counted fence failure.
