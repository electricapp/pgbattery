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

| #     | Window                                                                                                                                                                                                     | Contract   | Reachable today                                                                      | Tracked by        |
| ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ------------------------------------------------------------------------------------ | ----------------- |
| RW-1  | Deposed leader retains a quorum excluding the election winner; the promotion hold-down is vacuous and safety rests on the quorum-loss self-fence plus sync replication refusing acks                       | L1         | No — needs 5-node asymmetric partition shapes                                        | H-06 (needs H-04) |
| RW-2  | Post-promotion window where `synchronous_standby_names` is cleared before the replication manager reinstates it: commits ack with zero standby acks                                                        | W1, R2     | No — needs injection triggered by protocol state, not a sleep                        | H-07              |
| RW-3  | Async fallback then `pg_rewind` discarding up to the divergence threshold of genuinely acked WAL. The threshold's justification assumes sync replication is active, which is exactly false during fallback | W1         | Partially — the fallback is reachable, but nothing measures what rewind destroys     | H-14              |
| RW-4  | Fencing failure tail: wedged postmaster, exhausted connection slots, or a backend in uninterruptible I/O surviving `pg_terminate_backend`                                                                  | L1, L2     | Partially — SIGSTOP of the postmaster exists; the write path during it is unmeasured | H-13              |
| RW-5  | Direct writers on the internal PostgreSQL port bypassing the gateway's lease check entirely (`trust` auth on the cluster network)                                                                          | L1         | Yes — `dual_writability_prober` writes all three internal ports at 50 ms resolution  | covered           |
| RW-6  | Follower gateway routing writes to a deposed primary during the Raft detection interval, stopped only by the old leader's own lease                                                                        | W1         | Partially — never driven through a _follower_ gateway specifically                   | H-05              |
| RW-7  | After a long leaderless window every LSN report ages past the staleness threshold and both election and promotion gates fall back to bootstrap-permissive; a node restored from an old backup can win      | L3         | Closed — aged-LSN tiebreak; `test_stale_restored_node_loses_a_leaderless_election`   | H-11 (done)       |
| RW-8  | Failover-anchor lifecycle under coalesced watch transitions: a missed clear or missed re-stamp makes the hold-down read an ancient anchor and promote immediately                                          | L1         | Partially — pure functions are unit-tested; the live coalescing race is not          | H-18              |
| RW-9  | `demote()` holds the supervisor mutex across stop, rewind, and recovery — the 100 ms lease tick, health watchdog, and LSN reporting all stall behind it                                                    | L1         | No                                                                                   | H-15              |
| RW-10 | Join and rejoin edges: basebackup against a leader that gets deposed mid-copy, orphan slots pinning WAL, a learner registration surviving a mid-join crash                                                 | R1, V2     | Partially                                                                            | H-16              |
| RW-11 | `SetSyncMode` replicated state disagreeing with the live GUC across a leader change, so the election gate uses the loose async threshold while sync is actually active                                     | W1, L3     | No                                                                                   | H-05              |
| RW-12 | Commit-probe correctness at every byte offset around COMMIT: a wrong answer manufactures a phantom commit or a duplicate retry                                                                             | W1, W2, S1 | Partially — one fixed timing, no sweep                                               | H-17              |

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

      Deferred, but the tractability question is answered — measured locally
      (M4, `-workers auto`) so nobody has to rediscover it:

      | Spec             | Config                     | Distinct states | Time    |
      | ---------------- | -------------------------- | --------------- | ------- |
      | `lease_fencing`  | 5 nodes, MaxTerm 5         | 4.8M            | 13 s    |
      | `lease_fencing`  | 5 nodes, MaxTerm 4         | 1.1M            | 3 s     |
      | `raft_lsn`       | 3 nodes, MaxTerm 5         | 283 k           | 1 s     |
      | `raft_lsn`       | 4 nodes, MaxTerm 3         | 1.6M            | 6 s     |
      | `raft_lsn`       | 4 nodes, MaxTerm 3, LSN 3  | 7.0M            | 28 s    |
      | `raft_lsn`       | 5 nodes, MaxTerm 3         | intractable     | >180 s  |
      | `raft_lsn`       | 5 nodes, MaxTerm 4         | intractable     | >10 min |

      `lease_fencing` scales to 5 nodes cheaply, which is the config that matters
      for RW-1. `raft_lsn` does not: it carries a per-node LSN and term, so the
      state space multiplies on several dimensions at once and the queue grows
      faster than TLC drains it — at 5 nodes it was still 66M states from done
      after ten minutes. Its realistic ceiling is 4 nodes, and raising `MaxLSN`
      buys more than raising the node count. Any future attempt should move one
      axis at a time and run under a hard timeout.
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
