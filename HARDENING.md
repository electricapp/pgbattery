# Hardening Roadmap

Verification work not yet done, ordered by how much confidence it buys. The
prose sections explain each gap and why it matters; the "Execution plan" at the
end is the same content as tracked tasks, and the document is complete when it
holds no tasks.

This file describes only what is still open. A task that lands is deleted from
it — the account of what was wrong and how it was fixed belongs in the commit
that fixed it, and a register that keeps them is a changelog with a checkbox
column.

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

**A1: the fault silently fails to inject, and the test passes anyway.** Every
confirmed instance below was found by adding effect verification (or by
auditing the oracle), never by the test itself going red:

| What                                                                                                                                                        | Consequence                                                                                                              |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `iptables` was never installed in the runtime image                                                                                                         | all three nightly asymmetric-partition cases injected nothing; their `iptables -D ... \|\| true` cleanup no-opped too    |
| every fault `exec` ran as unprivileged `postgres` (the image ends `USER postgres`; `NET_ADMIN` is granted to the container, not to an unprivileged process) | `tc netem` could never inject latency — `RTNETLINK answers: Operation not permitted`                                     |
| `elle.yml` set a per-run `COMPOSE_PROJECT_NAME` while the harness addressed docker objects literally                                                        | `partition` and `flap_partition` were no-ops in CI                                                                       |
| `correctness_lite.py` had the same literal names under `correctness-lite.yml`'s per-run project                                                             | the partition fault window was empty, making I5 vacuous for it                                                           |
| `transfer_leader_after` read the API token from a `.env` CI never wrote                                                                                     | `transfer` posted unauthenticated and transferred nothing                                                                |
| a LazyFS fault worker was parked before its read loop, and writing to the control FIFO succeeds whether or not anything is reading                          | every `lazyfs::` command was accepted and never executed                                                                 |
| libfaketime moves `CLOCK_MONOTONIC` with `CLOCK_REALTIME` on amd64, and not on arm64                                                                        | the lease clock moved with the injected wall-clock skew, so every clock-skew case on CI tested the opposite of its claim |
| `*.bin` in `.gitignore` swallowed the Raft frame goldens                                                                                                    | two always-on frame-compat tests panicked on a clean checkout; they had only ever run against the author's working tree  |
| the four Track C chaos cases carried `expect_exit: null` and printed instead of asserting                                                                   | each accepted any outcome, including a harness crash, while the checklist ticked them as done                            |
| a Track C sequence check called `setval(pg_get_serial_sequence(...))` on a plain `bigint` column                                                            | the function returned NULL, the `WHERE ... IS NOT NULL` guard made it a no-op, and U3 passed having advanced nothing     |
| the bluegreen boot-fence probe ran before the app role existed on green, and read every psql failure as "fenced"                                            | "green is fenced read-only from boot" printed on `role does not exist`; the headline U1 boot check could not fail        |
| the mid-freeze chaos case killed hardcoded `node1` — the container hosting the writer's only gateway — and asserted zero acks                               | zero acks was guaranteed by the writer's own gateway dying; a cluster dual-accepting through the survivors passed        |
| two Track B W1 ledgers compared row COUNTS against the acked count                                                                                          | a write committed-but-unacked on the writer's own error path offset a genuinely lost acked key                           |
| `@app.command()` decorated the `_unfreeze` helper in the pg-major proof, not `main`                                                                         | invoking the proof performed a silent unfreeze and exited 0; the proof body had never run                                |

The lesson is the durable part: **a fault must verify its own effect and fail
loudly, and the harness must not assume its environment.** Every one of these
passed for months. `docker-compose.yml` sets `name: pgbattery`, so literal names
work locally and only break under CI's per-run project — the worst possible
failure shape. The libfaketime one adds a second lesson, which is to run a case
everywhere it is meant to run: arm64 does not fake the monotonic clock, so the
oracle passed on a developer machine and had never once run in CI.

The LazyFS one is the sharpest illustration, because the harness _did_ check
something: it checked that writing to the FIFO succeeded. It always does. LazyFS
opens the FIFO `O_RDWR` when it creates it, so a write succeeds whether or not
the worker thread behind it is alive, and that success was read as confirming the
fault fired. The distinction is only visible in LazyFS's own log, which is why
`[filesystem].logfile` is set and why `verify_lazyfs_fault_channel` sends a
deliberately unknown command and waits for the worker to echo it back.
**Checking that a fault was _requested_ is not checking that it was _executed_.**

The three Track C rows add a fourth shape: **a gate that checks a name rather
than a behaviour.** `lint_matrix` required every FATAL contract to declare an
inversion and checked that the name resolved somewhere in the tree — so
`docs/CONTRACTS.md` and `src/commands/upgrade/blue_green.rs` both satisfied it,
and U1–U5 shipped with nothing that could turn red. The gate now requires a
matrix case, a test function, a `testing/` harness, or an `inv-` model config,
and `test_an_inversion_that_cannot_fail_is_refused` holds it there.

The related power bug: workers pinned to a killed leader's gateway spun on
connection-refused at roughly 2800 attempts/second, producing 56,600 of 56,665
`:info` records in one CI run while two surviving workers committed every real
transaction. A nightly Elle run advertised ~10^5 operations and delivered ~385
real ones.

That lesson has a twin on the reading side. `docker exec` against a restarting
container answers nothing, and silence is not an answer: a reader that treats it
as one aborts on its first iteration the wait it exists to perform.
`ContainerNotRunning` separates _I could not look_ from _I looked and it is
absent_; `No such container` stays outside that class, being topology drift.
**An unreachable node is an absence of evidence, never evidence of absence** —
read wrong it fails loudly in a harness, and in a checker it would pass while
blind. The hardest place to see this is where the fault being measured is what
makes the node unreadable, as in `torn_raft.py`: a failed `exec` returns no
records, which is exactly what a log holding no tear returns.

There is a fourth shape, and it is the most expensive one here: a harness gate
that makes a failure rarer without touching what caused it. **A gate that changes
a failure rate without naming a mechanism is a placebo until proven otherwise**,
and the tell is usually in its own log — a wait that returns in `elapsed_sec=0.0`
measured nothing. Worse than the placebo is a lint added to enforce it, which
certifies a guarantee that does not exist.

The precondition shape has a sharper form: the product can discard the fault
itself, and a harness that does not know its starting state reads that as
tolerance. Damage a node's Raft store while it is mid-join and the store is never
opened, so nothing has cause to refuse it; `run_join_flow` wipes and rejoins, and
the damage is simply gone. A green from the product correctly throwing the fault
away is indistinguishable from a green from the product surviving it. Suites that
damage a node therefore require it to be a committed voter, in every node's view,
first.

**A2: the fault class does not exist.** Every fault in the default matrix is a
_clean_ fault: SIGKILL, container stop, network disconnect, SIGSTOP.
`docker kill` leaves the host page cache intact and the kernel still flushes
dirty pages, so none of them can discard an un-fsynced write. The dirty-crash and
torn-write suites are the separate answer to that, and they run against
`docker-compose.lazyfs.yml`.

### Class B — the violation cannot be observed

The silent kind, and the one worth worrying about most.

Split-brain detection in `correctness_lite.py` is _control-plane_: it samples
what nodes **say** about Raft leadership. A deposed leader whose fence has not
landed yet is Raft-consistent and PostgreSQL-writable at the same time, which no
leader-ID-agreement check can see, and a detector sampling at 500 ms cannot
resolve a sub-500 ms window even in principle.

`dual_writability_prober.py` closes that hole for L1 specifically, and only for
L1. It is data-plane: it races real writes at every internal PostgreSQL port —
three or five, by `--topology` — on a 50 ms round period and asks the database,
not the control plane, which node accepted. Its own floor is that round period —
a dual-write window shorter than 50 ms is still invisible — and it measures one
property, so **every other contract remains control-plane-observed.**

A sampler has two ways to report nothing, and they look identical. It can watch
a healthy system, or it can fail to watch: sample too coarsely to resolve the
window it is measuring, or run over a stretch in which the transition never
happened at all. Both give a clean zero. **A sampling harness has to assert its
own resolution and its own preconditions** — the series must be fine-grained
enough to fit inside the budget it judges against, and must contain the
transition it exists to observe — or a green from it says only that it looked
away. The corollary is that a window seen in a single sample spans zero seconds
by construction, so the verdict belongs on the upper bound rather than the
measured span.

What remains here is timing: fault timing in the older suites is hand-tuned
sleeps rather than values derived from the lease and election constants, so the
boundary regime where openraft-0.9-without-pre-vote pathologies live is never
swept deliberately.

### Class C — the schedule cannot be explored

There is no deterministic simulation, and a failure is only half replayable.

Each integration case takes minutes and explores exactly one OS-chosen
interleaving, chosen for it rather than by it — the unit tests fix the ordering
they assert, so nothing sweeps the orderings nobody thought to write down. Replay
is half solved: every harness that draws randomly records its seed and prints the
command that repeats it, so the harness's own choices come back, while the
cluster's timing does not — the same faults land at the same offsets against a
system free to schedule them differently.

## Risk-window register

The windows this design has, and what watches each one. These are named in the
code and the harnesses by their `RW-N` id, so this is the definition list for
those names rather than a record of work — a row leaves only when the window
itself stops existing.

| #     | Window                                                                                                                                                                 | Contract | Watched by                                                                                                                                                                                                                                                                                             |
| ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| RW-1  | Deposed leader retains a quorum excluding the election winner, so its last ack can be later than the winner's first sight of leaderlessness                            | L1       | `lease_fencing.tla` with explicit quorum sets, and its counterexample model; live in `five_node_suite.py` Phase 2d with the prober running                                                                                                                                                             |
| RW-2  | Post-promotion window: a freshly promoted primary acknowledging commits with zero standby acks                                                                         | W1, R2   | `post_promotion_sync_gap.py`, which enters the window on protocol state rather than a sleep; the commit is refused                                                                                                                                                                                     |
| RW-3  | Async fallback then `pg_rewind` discarding genuinely acked WAL                                                                                                         | W1       | `rewind_loss.py` forces the fallback and counts the survivors. The loss is real and accepted — see Accepted risks                                                                                                                                                                                      |
| RW-4  | Fencing failure tail: wedged postmaster, exhausted connection slots, or a backend in uninterruptible I/O surviving `pg_terminate_backend`                              | L1, L2   | `fencing_tail.py` wedges PostgreSQL, costs the node quorum, and races the prober across the whole window                                                                                                                                                                                               |
| RW-5  | Direct writers on the internal PostgreSQL port bypassing the gateway's lease check entirely                                                                            | L1       | `dual_writability_prober.py` writes every internal port at 50 ms resolution. The `trust` perimeter itself is an accepted risk                                                                                                                                                                          |
| RW-6  | Follower gateway routing writes to a deposed primary during the Raft detection interval, stopped only by the old leader's own lease                                    | W1       | `follower_gateway_routing.py` (`follower-gateway-deposed-primary`) cuts the leader's Raft port both ways with its PostgreSQL reachable, writes through every follower gateway, and asserts from each row's accepting server and commit time that no deposed-side write follows the new primary's first |
| RW-7  | A node restored from an old backup, Raft directory intact, standing for election after a long leaderless window                                                        | L3       | Every voter compares the candidate's WAL with its own, both read at vote time (`network::vote_lsn_gate`); nothing ages. Unit and property tests in `network.rs`                                                                                                                                        |
| RW-8  | Failover-anchor lifecycle under coalesced watch transitions: a missed clear or missed re-stamp makes the hold-down read an ancient anchor                              | L1       | `failover_anchor.py` drives the live lifecycle and asserts on the anchor's age from `/debug/state`; `trace-validation` checks every write-gate open against its anchor and against the instant its node first saw the term                                                                             |
| RW-9  | `demote()` holding the supervisor mutex across stop, rewind and recovery, stalling the lease tick, health watchdog and LSN reporting behind it                         | L1       | The lease fence runs on its own PostgreSQL session; `governor::fence::spawn` is handed that session and the lease, never the supervisor mutex                                                                                                                                                          |
| RW-10 | Join and rejoin edges: a clone source deposed mid-copy, orphan slots pinning WAL, a learner dying mid-join, a wiped bootstrap node impersonating the cluster           | R1, V2   | `join_edges.py`, five cases, run in CI                                                                                                                                                                                                                                                                 |
| RW-11 | `SetSyncMode` replicated state disagreeing with the live GUC across a leader change, so the election gate uses the loose async threshold while sync is actually active | W1, L3   | `sync_mode_agreement.py` measures how long the replicated flag runs looser than the leader's live GUC, against a budget derived from the reconcile that closes it                                                                                                                                      |

## Tier 4 — Deterministic simulation

Multi-week, and the actual next level. It is what buys reproducibility and
schedule coverage that Docker structurally cannot.

The seams exist: `LeaseState` reads time through an injectable `Clock`,
`src/governor/network.rs` is the transport seam, and `PgControl` makes
the node reconciler's pass and the lease fence drivable against a model
PostgreSQL with no database at all.

What is missing is running the governor and `app.rs` orchestration against a
seeded network controlling delay, reorder, drop, and clock advance. `madsim`
fits better than `turmoil` because time control is required, not just network
control.

Payoff: thousands of failover schedules per second, seed-reproducible failures,
and interleavings the OS will never pick. What no seam gives is the orderings:
every test fixes the one it asserts (Class C).

## Tier 5 — Connect the specs to the binary

The four TLA+ specs are non-vacuous, and both halves of their mapping onto the
binary are enforced. Statically, `lint_matrix.py` resolves the code each spec
names and holds `lease_fencing`'s ticks to the milliseconds the binary ships.
Dynamically, the binary records the transitions `lease_fencing` and `raft_lsn`
model (`SpecEvent`: term and leader, anchor stamp and spend, write-gate open,
the write state each fence tick leaves, each vote with its WAL gate inputs),
and `trace-validation` holds a real failover's trace to them with
`trace_check.py`. Each rule cites the spec definitions it refines, which lint
resolves, and each drift in `trace_check.DRIFTS`, written into the real trace,
must trip its rule.

What the traces do not reach: `timeline_verification` and `cutover_authority`
record no transitions, and one run sees one scenario (a Raft-only partition of
the leader). Write state is observed at the fence's 100 ms tick, so a writable
interval shorter than a tick can pass unrecorded.

## Tier 6 — Real Jepsen, or autonomous exploration

A real Jepsen run is a moderate lift rather than a rewrite: the histories are
already in Jepsen format and Elle is already the checker. What is missing is a
`jepsen.db` setup/teardown harness, 5 nodes on LXC or VMs, and Jepsen's own
nemesis library.

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
  published RPO becomes non-zero. Measured rather than asserted: `rewind_loss.py`
  forces the fallback and counts the survivors.
- **A stopped standby's WAL reads as its replay position.** With no server
  running, a node reads where its WAL ends from the control file, which for a
  standby is the replay position: WAL it received and had not replayed lies
  past it, so as a voter it can grant a candidate missing that WAL. Only a
  clean stop reaches this; a crashed or hung server reads as unknown and
  refuses. Without it, a Track B freeze, which stops every standby, could not
  elect the leader that moves the upgrade record on.
- **A voter whose PostgreSQL neither answers nor stopped cleanly refuses every
  vote**, since it may hold the acknowledged writes. Boot starts PostgreSQL
  before Raft, so this is a hung or crashed server, and a cluster that has lost
  a leader and has such a voter in every majority waits for it.
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
  `SelectStmt` never reaches that arm.

## Exit criteria

The roadmap is done when:

1. Every FATAL contract has an oracle that measures the contract rather than a
   proxy for it, and a paired inversion test proving that oracle can fail.
   **Met, and mechanically.** `lint_matrix.py` fails when a FATAL row in the
   Contract-to-Test Index leaves its Inversion column empty, so this one cannot
   drift back out without going red.
2. Durability claims survive a dirty crash, not just a clean one. **Met.**
   `durability_crash.py` (W1 and R2 against un-fsynced writes on LazyFS),
   `torn_write.py`, `torn_raft.py` and `wal_enospc.py` all run against
   `docker-compose.lazyfs.yml`, and each refuses a green until its inversion has
   gone red.
3. Any failure found in CI can be replayed from a seed. **Half met.** Every
   harness that draws randomly draws from one seeded generator and records the
   seed; what is not reproducible is the cluster's own timing, so a replay drives
   the same faults against a system that may schedule them differently. Closing
   it outright needs a deterministic runtime.
4. Every risk window is either covered by a test, closed by design, or listed in
   Accepted risks with a rationale. **Met.**
5. The specs are checked against traces from the running binary. **Met.**
   `trace-validation` checks a failover's trace against `lease_fencing` and
   `raft_lsn`, and fails unless every rule decided an instance and every
   injected drift was caught. An event the ring evicted makes an instance
   unverifiable, never violated.

So the distance to zero is criterion 3: a deterministic runtime.

## Execution plan

Everything above, restated as independently committable tasks.

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
- **Delete the task from this file in the same commit** that lands it, and edit
  the register if it retires or changes a risk window. The commit message is
  where the account of the work goes.

Effort: **S** hours · **M** one to two days · **L** about a week · **XL**
multi-week.

- [ ] **Run the governor and `app.rs` orchestration under `madsim`** with a
      seeded network and clock: delay, reorder, drop, partition, clock advance.
      **Done when** a seed reproduces a failover schedule end to end and the
      suite runs thousands per minute in CI.
      _Effort_ XL

- [ ] **Make CI failures replayable from a seed.** Emit the seed on every run and
      accept it as input, so a red CI run replays locally.
      **Done when** a failing CI run prints a command that reproduces it.
      _Closes_ exit criterion 3 · _Blocked by_ the `madsim` runtime · _Effort_ M

- [ ] **Write a `jepsen.db` setup and teardown harness**, run on 5 nodes on LXC
      or VMs, and adopt Jepsen's own nemesis library.
      **Done when** a real Jepsen run reproduces our existing L1 and W1 results
      independently.
      _Effort_ XL
