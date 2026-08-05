# pgbattery

Raft-based HA manager for PostgreSQL. Rust binary (`pgbattery`) that manages a 3-node cluster with automatic failover, synchronous replication, and a TCP gateway that routes clients to the leader.

## Correctness is Paramount

This is a distributed database system. If you observe inconsistent state (split-brain, lost writes, stuck replication, missing slots), **STOP** and investigate to root cause. Once the root cause is clear, prefer fixing it immediately over writing it up. `BUGS.md` (create it if absent) is only for deferred fixes and unclear root causes — not a changelog of bugs that were found and fixed in the same session.

## State machines: `docs/STATE_MACHINE.md` is canonical

`docs/STATE_MACHINE.md` is the **canonical source of truth** for every state machine in pgbattery — Raft role, lease, PG process role, sync replication, gateway routing, app orchestration, lease enforcement. It documents the _definitive source of truth_ for each piece of state and the discipline that keeps caches and timers from creeping back in.

**Mandatory rule for any change to consensus / supervisor / lease / replication / fencing / gateway-routing logic:**

1. Read `docs/STATE_MACHINE.md` before editing.
2. If the change adds/removes/renames a state, transition, or truth source — or introduces a new cache, timer, or polling loop — update `docs/STATE_MACHINE.md` _in the same commit_ as the code.
3. Anti-patterns to refuse, even if asked: in-process state caches that duplicate a re-derivable truth source; `sleep`/timer-based gates on state transitions; introspection of our own process for state we just wrote. Caching across the PG process boundary is fine when needed; treat PG state as something to be _probed_, not assumed.
4. If you find yourself reaching for a cache, prefer making the writer idempotent so callers can stay stateless.

## Architecture

Cargo **workspace** with three members (`Cargo.toml` `[workspace]`): the root
binary crate (`.`) plus two leaf crates with a strict compile-time boundary —
neither leaf depends on the root, so they cannot pull in Raft/gateway/etc.

```
src/                           — root crate (the `pgbattery` binary)
  cli.rs, main.rs, app.rs      — entrypoint, CLI parsing, orchestration
  cluster/                     — Raft consensus, membership, replication management
  governor/                    — leader/follower state machines, failover logic
  gateway/                     — TCP proxy that routes clients to current leader
  supervisor/mod.rs            — RE-EXPORT SHIM only; impl lives in the crate below
  observability/               — Prometheus metrics + management HTTP API
  config/                      — TOML config parsing
  commands/                    — backup/restore

crates/
  pgbattery-core/src/          — shared primitives: clock, constants, error, types
  pgbattery-supervisor/src/    — REAL PostgreSQL process mgmt (process.rs, backup.rs)
                                 e.g. verify_promotion_safe(), promote/demote, pg_rewind
```

Note: `crate::supervisor::*` paths still resolve via the shim, but the source is
under `crates/pgbattery-supervisor/src/` — grep there, not `src/supervisor/`.

## Docker Compose (3-node cluster)

Network `raft_net` (172.28.0.0/16). node1 bootstraps, node2/node3 join.

| Node  | Gateway | Internal PG      | Metrics | Mgmt API |
| ----- | ------- | ---------------- | ------- | -------- |
| node1 | :5432   | 172.28.0.11:5434 | :9091   | :9081    |
| node2 | :5433   | 172.28.0.12:5434 | :9092   | :9082    |
| node3 | :5434   | 172.28.0.13:5434 | :9093   | :9083    |

**Gateway ports proxy to leader** — don't use them to check individual node state.
Check node state directly: `docker compose exec node1 psql -h 127.0.0.1 -p 5434 -U postgres -c "SELECT pg_is_in_recovery();"`

Requires `PGBATTERY_MANAGEMENT_API_TOKEN` env var (set in `.env` or shell).

## Management API

All on port 9091 internally (mapped to 9081/9082/9083).

**Discovery (no auth):**

- `GET /api/v1/cluster/leader` → `{leader_id, leader_addr, leader_pg_addr, leader_mgmt_addr}`
- `GET /api/v1/cluster/nodes` → list of node states
- `GET /api/v1/cluster/members` → Raft membership
- `GET /api/v1/cluster/node/{id}/lag` → `{lag_bytes, is_synced}`

**Token-required (`x-pgbattery-token` header):**

- `POST /api/v1/cluster/transfer-leadership/{target_id}`
- `POST /api/v1/cluster/join`, `/promote/{id}`, `/remove/{id}`
- `POST /api/v1/backup/create`, `/restore?filename=...`
- `GET /api/v1/backup/list` — read-only but gated: it leaks filesystem paths, sizes, and the backup schedule, and drives a stat walk of every backup tree

## Verification layers

Independent layers. Know which one covers a change before adding another. For current counts, ask the tools (`./testing/ci_runner.py --list`, `cargo test --workspace`) rather than trusting a number written here.

| Layer                  | Where                                        | What it checks                                                                                                                               |
| ---------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Rust unit tests        | inline `mod tests`                           | Pure decision functions. Densest in `process.rs`, `gateway/handlers`, `state_machine.rs`.                                                    |
| Property tests         | `proptest!` blocks in the modules they cover | LSN election/promotion gate, `rewind_divergence_decision`, sync-standby quorum intersection over all voter-set sizes.                        |
| Storage conformance    | `openraft::testing::Suite` in `storage.rs`   | The real `LogStorageAdapter` / `StateMachineStore` against openraft's own suite, plus crash-recovery and durability-pin tests.               |
| TLA+ model checking    | `tla/` (`make check`)                        | `lease_fencing` (one write authority), `raft_lsn` (election safety, LSN gate can't deadlock), `commit_probing`, `timeline_verification`.     |
| Fuzzing                | `fuzz/` (`cargo fuzz`)                       | PG wire protocol (startup/framing/extended), query analysis into libpg_query, Raft RPC frames, snapshot decode, LSN parsing.                 |
| Docker HA matrix       | `testing/ci_runner.py` + `ci_matrix.yaml`    | Real 3-node cluster, real faults, effect-verified. Every case declares its contract IDs; `lint_matrix.py` enforces it.                       |
| Transactional anomaly  | `testing/linearizability_register.py` + Elle | Elle (list-append / rw-register) asserting strict serializability; plus an in-tree WGL per-key linearizability checker.                      |
| Durability/split-brain | `testing/correctness_lite.py`                | History invariants (lost acks, phantom writes, dual leadership, quorum-loss fencing, bank ledger) + log-grep checks.                         |
| Single-writer oracle   | `testing/dual_writability_prober.py`         | Contract L1 directly: concurrent real writes to all three internal PG ports, at most one may be accepted.                                    |
| Dirty crash            | `testing/durability_crash.py`                | W1 and R2 when un-fsynced writes die with the process, on LazyFS. `cluster-crash` is the R2 test; `leader-crash` refuses `--prove-oracle`.   |
| Torn writes (PG)       | `testing/torn_write.py`                      | A half-written heap page, WAL segment or control file is repaired by the full-page image or rejected by checksums, never served silently.    |
| Torn writes (redb)     | `testing/torn_raft.py`                       | A torn `raft.db` leaves the node tolerating cleanly or refusing to start — never running and voting on a store nothing vouched for.          |
| Disk exhaustion        | `testing/wal_enospc.py`                      | W1 when the leader cannot allocate the next WAL segment, plus L1 from the prober across the window. Refuses a run whose fault had no effect. |
| Harness self-tests     | `testing/test_*.py`                          | The checkers, oracles, and fault primitives themselves — including that each can actually fail.                                              |

Contracts live in `docs/CONTRACTS.md` (W1-W3, L1-L3, V1-V2, S1-S2, R1-R2). FATAL contract violations are release-blocking.

All Python test scripts use a uv shebang (`#!/usr/bin/env -S uv run --project testing python`); deps in `testing/pyproject.toml`.

### CI gating

| Workflow                                               | On PR / push                                                                                                   | Nightly                                                        |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `ha-ci.yml`                                            | harness lint + self-tests, `ha-sequential`, `ha-parallel` (matrixed), `ha-controlplane-pr`, `ha-assert-sanity` | `ha-controlplane-nightly` at 04:15 UTC                         |
| `elle.yml`                                             | `kill` attack only (~90 s smoke)                                                                               | full attack matrix, sharded, random seed per run, at 03:00 UTC |
| `tla.yml`                                              | all 4 specs                                                                                                    | —                                                              |
| `correctness-lite.yml`, `fuzz.yml`, `supply-chain.yml` | see workflow                                                                                                   | —                                                              |

Run locally: `./testing/ci_runner.py --suite <suite> [--case <id>]`, `testing/run_elle_matrix.sh [attack...]`, `cd tla && make check`.

### Known-incomplete harnesses

Deliberate skeletons that raise rather than silently pass. Do not treat their absence as coverage:

- `testing/five_node_suite.py` — Phase 1 is implemented and green (bootstrap, survives 2 voter failures, loses quorum at 3, recovers with acked writes intact) against `docker-compose.5node.yml`. Phases 2-4 — membership chaos, 2-sync/2-async replication, Elle at five nodes — are not, and the runner says so. Note the suite promotes learners to voters before asserting anything: compose starts all five joins at once and openraft 0.9 leaves the losers of that race as learners, which would make every quorum assertion vacuous.
- `fsync_drop` / `bit_flip` attacks in `linearizability_register.py` — need image changes (LazyFS or libeatmydata; dm-flakey); excluded from `ALL_ATTACKS` and `chaos_storm`.

Faults in the default matrix are all _clean_ (SIGKILL, container stop, network disconnect, SIGSTOP): `docker kill` leaves the host page cache intact, so none of them can discard an un-fsynced write. The dirty-crash and torn-write paths are separate, all against `docker-compose.lazyfs.yml`, which mounts **two** LazyFS instances per node — PGDATA and the Raft store — each with its own control FIFO, log and backing root. Two rather than one over their common parent, so a fault aimed at redb cannot crash the filesystem holding PostgreSQL and leave no way to say which store the damage reached. Every one of these suites refuses to report a green until its inversion has gone red. See `HARDENING.md`.

### Faults must verify their own effect

A fault that silently fails to inject is worse than no test, because it reads as coverage. This has bitten six times: `iptables` missing from the image; every fault `exec` running as unprivileged `postgres` (the image ends `USER postgres`, so `NET_ADMIN` on the container does not reach the process — privileged operations need `--user root`); three cases addressing docker objects by literal name while CI sets a per-run `COMPOSE_PROJECT_NAME`; and a LazyFS fault worker parked before its read loop, so every `lazyfs::` command was accepted by the FIFO and never executed. Derive container and network names from the active compose project, never hardcode them, and assert the fault landed. `HARDENING.md` has the full list.

Assert the fault's _effect_, never the fact that you asked for it. The LazyFS case is the cautionary one: writing to its control FIFO succeeds whether or not anything is reading, so "the write succeeded" was mistaken for "the fault fired" for as long as the code existed. Where a fault goes through an external process, find the observable that process publishes and wait for it.

Service names, static addresses and published ports come from `testing/topology.py`, which reads the compose file named by `COMPOSE_FILE`. Do not restate them in a harness: an unreachable node is recorded `indeterminate`, which the L1 verdict treats as "no acceptance observed", so a harness addressing ports that moved passes while blind. `topology.py` raises rather than returning an empty node list — a list nothing iterates makes every loop over it a silent no-op. `lint_matrix.py` reconciles `ci_matrix.yaml`'s own cluster block against it, and pins the log strings `correctness_lite.py` greps for against the Rust sources that emit them.

## Testing Philosophy

- Verify correctness, not just "it didn't crash"
- Check replication state (SYNC/ASYNC) after failover
- Verify data integrity after leadership changes
- Investigate failures — don't restart to "fix" them
- A fault that cannot fail is worse than no test: prefer a loud `NotImplementedError` over an assertion that passes vacuously

## Key Docs

- `docs/STATE_MACHINE.md` — canonical state machines and truth sources
- `docs/CONTRACTS.md` — correctness contracts (referenced by test cases)
- `docs/MEMBERSHIP.md` — cluster membership operations
- `docs/RUNBOOK.md`, `docs/DEPLOYMENT.md`, `docs/RELEASING.md` — operations
- `tla/README.md` — what each spec proves, and what it deliberately does not
- `HARDENING.md` — what the verification layers cannot catch, and the scoped work to close it
