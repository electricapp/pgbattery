# pgbattery

Raft-based HA manager for PostgreSQL. Rust binary (`pgbattery`) that manages a 3-node cluster with automatic failover, synchronous replication, and a TCP gateway that routes clients to the leader.

## Correctness is Paramount

If you observe inconsistent state (split-brain, lost writes, stuck replication, missing slots), **STOP**, root-cause, and fix it in the same session. There is no bug list: an understood-but-unfixed defect is a defect the next run will hit. Write it up only to explain a fix you are landing.

## State machines: `docs/STATE_MACHINE.md` is canonical

Canonical reference for Raft role, lease, PG process role, sync replication, gateway routing, app orchestration, and lease enforcement — including each definitive source of truth.

For consensus / supervisor / lease / replication / fencing / gateway-routing changes:

1. Read `docs/STATE_MACHINE.md` before editing.
2. Same-commit update when you add/remove/rename a state, transition, or truth source — or introduce a cache, timer, or polling loop.
3. Refuse: in-process caches of re-derivable truth; timer-only transition gates; introspection of state we just wrote. Probe PG across the process boundary; prefer idempotent writers so callers stay stateless.

## Architecture

Cargo **workspace** with three members (`Cargo.toml` `[workspace]`): the root binary crate (`.`) plus two leaf crates. Neither leaf depends on the root (no Raft/gateway pull-in).

```
src/                           — root crate (`pgbattery` binary)
  cli.rs, main.rs, app.rs      — entrypoint, CLI, orchestration
  cluster/                     — Raft consensus, membership, replication
  governor/                    — leader/follower state machines, failover
  gateway/                     — TCP proxy to current leader
  supervisor/mod.rs            — RE-EXPORT SHIM only
  observability/               — Prometheus + management HTTP API
  config/                      — TOML config
  commands/                    — backup/restore

crates/
  pgbattery-core/src/          — clock, constants, error, types
  pgbattery-supervisor/src/    — PostgreSQL process mgmt (process/, session.rs, backup.rs)
```

`crate::supervisor::*` resolves via the shim; real source is under `crates/pgbattery-supervisor/src/`.

## Docker Compose (3-node cluster)

Network `raft_net` (172.28.0.0/16). node1 bootstraps; node2/node3 join.

| Node  | Gateway | Internal PG      | Metrics | Mgmt API |
| ----- | ------- | ---------------- | ------- | -------- |
| node1 | :5432   | 172.28.0.11:5434 | :9091   | :9081    |
| node2 | :5433   | 172.28.0.12:5434 | :9092   | :9082    |
| node3 | :5434   | 172.28.0.13:5434 | :9093   | :9083    |

Gateway ports proxy to the leader — do not use them for per-node checks:

```
docker compose exec node1 psql -h 127.0.0.1 -p 5434 -U postgres -c "SELECT pg_is_in_recovery();"
```

Requires `PGBATTERY_MANAGEMENT_API_TOKEN` (`.env` or shell).

## Management API

Internal port 9091 (host 9081/9082/9083).

**Discovery (no auth):**

- `GET /api/v1/version` → `{version, raft_rpc_frame_version, store_schema_version, node_id}`
- `GET /api/v1/cluster/leader` → `{leader_id, leader_addr, leader_pg_addr, leader_mgmt_addr}`
- `GET /api/v1/cluster/nodes` → node states
- `GET /api/v1/cluster/members` → Raft membership
- `GET /api/v1/cluster/node/{id}/lag` → `{lag_bytes, is_synced}`

**Token-required (`x-pgbattery-token`):**

- `POST /api/v1/admin/self-upgrade` — download/verify/replace binary + graceful restart
- `POST /api/v1/cluster/transfer-leadership/{target_id}`
- `POST /api/v1/cluster/join`, `/promote/{id}`, `/remove/{id}`
- `POST /api/v1/backup/create`, `/restore?filename=...`
- `GET /api/v1/backup/list` — gated (paths, sizes, schedule; walks backup trees)

## The local loop

`scripts/preflight.sh` runs local CI stand-ins (fmt, clippy, fuzz clippy, machete, typos, prettier, `cargo audit`, `cargo deny`, harness lint/self-tests, `cargo test`, `cargo doc`) in ~1 minute. `.githooks/pre-push` runs it (`git config core.hooksPath .githooks` once; `SKIP_PREFLIGHT=1 git push` to bypass). Each gate names its CI job; `lint_matrix.py` fails if CI adds a lint without a preflight mirror.

Images use the `ci` cargo profile (release opts without whole-program LTO). `release.yml` still ships `release`.

Matrix/SQL/Python-only changes usually hit the compose content-hash cache. Rust changes invalidate via `COPY . .` mtimes — use host cross-compile then `--no-build`:

```
scripts/dev-image.sh && ./testing/ci_runner.py --suite ha-parallel --case <id> --no-build
```

## Verification layers

Know which layer covers a change before adding another. For counts: `./testing/ci_runner.py --list`, `cargo test --workspace`.

| Layer                   | Where                                      | What it checks                                                                 |
| ----------------------- | ------------------------------------------ | ------------------------------------------------------------------------------ |
| Rust unit tests         | inline `mod tests`                         | Pure decision functions (`process/`, gateway handlers, `state_machine.rs`)     |
| Property tests          | `proptest!`                                | LSN election/promotion gate, `rewind_divergence_decision`, sync-standby quorum |
| Storage conformance     | `openraft::testing::Suite` in `storage.rs` | Log/SM stores + crash-recovery / durability-pin                                |
| TLA+                    | `tla/` (`make check`)                      | `lease_fencing`, `raft_lsn`, `timeline_verification`, `cutover_authority`      |
| Fuzzing                 | `fuzz/`                                    | PG wire, query analysis, Raft RPC, snapshot decode, LSN parse                  |
| Docker HA matrix        | `testing/ci_runner.py` + `ci_matrix.yaml`  | Real 3-node faults; cases cite contract IDs                                    |
| Transactional anomaly   | `linearizability_register.py` + Elle       | Strict serializability + in-tree WGL                                           |
| Durability/split-brain  | `correctness_lite.py`                      | History invariants + log-grep                                                  |
| Single-writer oracle    | `dual_writability_prober.py`               | L1 via concurrent writes to all internal PG ports                              |
| Dirty crash             | `durability_crash.py`                      | W1/R2 on LazyFS; `cluster-crash` is the R2 test                                |
| Torn writes (PG / redb) | `torn_write.py` / `torn_raft.py`           | Half-written pages/segments/`raft.db` never silent                             |
| Disk exhaustion         | `wal_enospc.py`                            | W1 at WAL segment alloc + concurrent L1                                        |
| Join/rejoin edges       | `join_edges.py`                            | Clone/slot/learner/wiped-bootstrap edges                                       |
| Harness self-tests      | `testing/test_*.py`                        | Checkers/oracles/faults can fail                                               |

Contracts: `docs/CONTRACTS.md` (W1–W3, L1–L3, V1–V2, S1–S2, R1–R2). FATAL = release-blocking.

Python harnesses use `#!/usr/bin/env -S uv run --project testing python` (`testing/pyproject.toml`).

### CI gating

| Workflow                                               | On PR / push                                                                                                      | Nightly                             |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `ha-ci.yml`                                            | harness lint + self-tests, `ha-sequential`, `ha-parallel`, `ha-controlplane-pr`, `ha-assert-sanity`, `join-edges` | `ha-controlplane-nightly` 04:15 UTC |
| `elle.yml`                                             | `kill` smoke (~90 s)                                                                                              | full attack matrix, 03:00 UTC       |
| `tla.yml`                                              | all 4 specs                                                                                                       | larger bounds, 03:40 UTC            |
| `correctness-lite.yml`, `fuzz.yml`, `supply-chain.yml` | see workflow                                                                                                      | —                                   |

Local: `./testing/ci_runner.py --suite <suite> [--case <id>]`, `testing/run_elle_matrix.sh [attack...]`, `cd tla && make check` (`make check-large` for nightly models).

### Known-incomplete harnesses

Skeletons that raise rather than silently pass:

- `testing/five_node_suite.py` — phases 1–2 green on `docker-compose.5node.yml` (quorum, partition shapes, RW-1 asymmetric). Not implemented: membership chaos, 2-sync/2-async, Elle at five nodes. Promotes learners before asserts (openraft 0.9 can leave join-race losers as learners).
- `fsync_drop` / `bit_flip` in `linearizability_register.py` — need LazyFS / dm-flakey image work; excluded from `ALL_ATTACKS` / `chaos_storm`.

Default-matrix faults are **clean** (SIGKILL, stop, disconnect, SIGSTOP) — host page cache intact. Dirty-crash / torn-write suites use `docker-compose.lazyfs.yml` (separate LazyFS for PGDATA and Raft). Details and fault-effect rules: `HARDENING.md`.

### Harness rules (summary)

Full incident list and exit criteria live in `HARDENING.md`. Short version:

- Assert the fault's **effect**, never that you requested it.
- Derive container/network names from the compose project (`testing/topology.py` + `COMPOSE_FILE`); never hardcode.
- Prefer a loud `NotImplementedError` over a vacuous pass.

## Testing Philosophy

- Verify correctness, not just "it didn't crash"
- Check SYNC/ASYNC and data integrity after leadership changes
- Investigate failures — don't restart to "fix" them
- A fault that cannot fail is worse than no test

## Key Docs

- `docs/STATE_MACHINE.md` — canonical state machines and truth sources
- `docs/contracts.yaml` — contract ID / severity / inversion registry
- `docs/CONTRACTS.md` — contract narrative (synced to the registry)
- `docs/state_machine_inventory.toml` — types/enums owned by STATE_MACHINE
- `tla/code_map.toml` — TLA spec → Rust path/symbol map
- `docs/MEMBERSHIP.md` — membership operations
- `docs/RUNBOOK.md`, `docs/DEPLOYMENT.md`, `docs/RELEASING.md` — operations
- `tla/README.md` — what each spec proves / does not
- `HARDENING.md` — verification gaps and fault-effect discipline
