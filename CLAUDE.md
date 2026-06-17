# pgbattery

Raft-based HA manager for PostgreSQL. Rust binary (`pgbattery`) that manages a 3-node cluster with automatic failover, synchronous replication, and a TCP gateway that routes clients to the leader.

## Correctness is Paramount

This is a distributed database system. If you observe inconsistent state (split-brain, lost writes, stuck replication, missing slots), **STOP**, investigate, and document in `BUGS.md` before continuing.

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

**Mutations (require `x-pgbattery-token` header):**

- `POST /api/v1/cluster/transfer-leadership/{target_id}`
- `POST /api/v1/cluster/join`, `/promote/{id}`, `/remove/{id}`
- `POST /api/v1/backup/create`, `/restore?filename=...`

## Testing

All test scripts use uv shebang (`#!/usr/bin/env -S uv run --project testing python`). Dependencies in `testing/pyproject.toml`.

| File                        | Purpose                                                        |
| --------------------------- | -------------------------------------------------------------- |
| `testing/ci_runner.py`      | YAML-driven CI test orchestrator (Pydantic + Rich)             |
| `testing/ci_matrix.yaml`    | 25 step types, 20 test cases across 4 suites                   |
| `testing/overnight_test.py` | 8hr randomized chaos (10 scenarios, Rich UI)                   |
| `testing/jepsen_lite.py`    | Simple Jepsen-style correctness test (~250 lines, stdlib only) |

CI workflows in `.github/workflows/`:

- `ha-ci.yml` — sequential + parallel HA suites (push/PR/nightly)
- `jepsen-lite.yml` — weekly correctness chaos test

## Testing Philosophy

- Verify correctness, not just "it didn't crash"
- Check replication state (SYNC/ASYNC) after failover
- Verify data integrity after leadership changes
- Investigate failures — don't restart to "fix" them

## Key Docs

- `BUGS.md` — bug/anomaly tracker
- `TESTS.md` — test results
- `MEMBERSHIP.md` — cluster membership operations
