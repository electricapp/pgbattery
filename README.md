<div align="center">

![TLA+ Verified](https://img.shields.io/badge/Verified-TLA%2B-blue)
![License](https://img.shields.io/badge/License-AGPLv3-green)
![Status](https://img.shields.io/badge/Status-Alpha-orange)
![Rust](https://img.shields.io/badge/Rust-1.96%2B-orange)
![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20macOS-lightgrey)

</div>

```
██████╗  ██████╗     ██████╗  █████╗ ████████╗████████╗███████╗██████╗ ██╗   ██╗
██╔══██╗██╔════╝     ██╔══██╗██╔══██╗╚══██╔══╝╚══██╔══╝██╔════╝██╔══██╗╚██╗ ██╔╝
██████╔╝██║  ███╗    ██████╔╝███████║   ██║      ██║   █████╗  ██████╔╝ ╚████╔╝
██╔═══╝ ██║   ██║    ██╔══██╗██╔══██║   ██║      ██║   ██╔══╝  ██╔══██╗  ╚██╔╝
██║     ╚██████╔╝    ██████╔╝██║  ██║   ██║      ██║   ███████╗██║  ██║   ██║
╚═╝      ╚═════╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚══════╝╚═╝  ╚═╝   ╚═╝
```

**Patroni + etcd + HAProxy = 847 config lines. pgbattery = 1 binary (~14 MB).**

MongoDB-style failover for PostgreSQL. Idle connections migrate in <100 ms; in-flight writes resume on the new leader within a few seconds — no client reconnect.

![pgbattery failover demo: SIGKILL the leader, the session keeps writing on the new leader without reconnecting](docs/assets/failover-demo.gif)

[Kyle Kingsbury](https://www.youtube.com/watch?v=FdfZxN-IkpA), Jepsen author and distributed-systems correctness researcher:

> **Q:** What's your favorite relational database?
>
> **A:** Postgres. Fantastic. Love it. Wish it had a good replication story.

From [HN discussion](https://news.ycombinator.com/item?id=46336947) and [PostgreSQL mailing list](https://www.postgresql.org/message-id/0e01fb4d-f8ea-4ca9-8c9b-79264ce11993%40postgrespro.ru):

> "I still don't get how folks can hype Postgres with every second post on HN, yet there is no simple batteries-included way to run a HA Postgres cluster with automatic failover like you can do with MongoDB."
>
> — heipei

> "In the SQL world, people are used to accepting the absence of real HA (resilience to failure, where transactions continue without interruption) and instead rely on fast DR (stop the service, recover, check for data loss, start the service). Yet they still call it HA because there's nothing else."
>
> — Franck Pachot (Developer Advocate, Crunchy Data)

> "There is no way to guarantee correctness with just two replicas. And many stories of lost transactions with Patroni/Stolon already confirms this thesis... I really dream PostgreSQL will be as reliable as MongoDB without need of external services."
>
> — [Yura Sokolov](https://www.postgresql.org/message-id/0e01fb4d-f8ea-4ca9-8c9b-79264ce11993%40postgrespro.ru), Postgres Professional (pgsql-hackers mailing list)

From [Bruce Momjian](https://momjian.us/main/blogs/pgblog/2017.html) (PostgreSQL Core Team founding member):

> "On the server side, high availability means having the ability to quickly failover to standby hardware, hopefully with no data loss. Failover behavior on the client side is more nuanced... For clients using a connection pooler, things are even more complicated."

From [Crunchy Data](https://www.crunchydata.com/blog/patroni-etcd-in-high-availability-environments) (major PostgreSQL contributor):

> "When communication between [Patroni and etcd] breaks down, it creates instability in the environment resulting in failover, cluster restart, and even the loss of a primary database."

From [PgCon 2012 Cluster Summit](https://wiki.postgresql.org/wiki/PgCon2012CanadaClusterSummit) (official PostgreSQL conference):

> "Currently we have to detect faults -- system down -- by polling. This takes much longer than the actual failover takes."

Batteries-included HA for PostgreSQL. Single binary. No external coordination service.

## Quick Start

Boot the 3-node demo cluster, write to it, kill the leader, and watch the session stay alive — under five minutes:

```bash
git clone https://github.com/electricapp/pgbattery
cd pgbattery
cp .env.example .env                                  # supplies PGBATTERY_MANAGEMENT_API_TOKEN
docker compose up -d                                  # bring up 3-node cluster
psql postgres://postgres@localhost:5432/postgres      # gateway always routes to the leader

# In another shell, drop the current leader — the psql session keeps working:
docker kill -s SIGKILL pgbattery-node1-1
```

Then inspect the cluster:

```bash
cargo run --release -- status --discover localhost:9081   # live dashboard
cargo run --release -- doctor --discover localhost:9081   # pre-deploy health gate
```

- Gateway listens on `5432` and always routes to the current leader.
- Internal PG port per container is `5434` (reach via `docker compose exec`).
- Management API on host `9081/9082/9083` (container `9091`); Prometheus metrics on host `9091/9092/9093` (container `9090`).
- Use TCP (`-h localhost`); Unix sockets aren’t exposed in the demo.
- Set `PGBATTERY_MANAGEMENT_API_TOKEN` (any random string) **before** `docker compose up` — the Compose file fails fast if it is unset. `cp .env.example .env` is the easy path.
- Release binary (~14 MB) lives at `target/release/pgbattery`; copy to each node.

## What pgbattery Does

pgbattery is a single binary for PostgreSQL HA: leader election, fencing, commit verification, backups, metrics, and TLS, without etcd or a separate load balancer.

Idle connections migrate transparently (<100 ms blip in the local 3-node cluster), in-flight writes resume on the new leader within a few seconds, and the gateway can answer uncertain COMMIT outcomes by probing the new leader. It also includes a CLI, REST API, Prometheus metrics, `pg_basebackup`/`pg_dump` automation, and a built-in upgrade workflow. Safety work includes a TLA+ election spec, chaos tooling, LSN-aware promotions, and lease-based fencing for stale primaries.

## Architecture

```
Gateway (5432) → Governor (Raft) → Supervisor → PostgreSQL
```

- Gateway: parses PostgreSQL protocol, enforces lease, migrates idle connections.
- Governor: OpenRaft-based consensus with LSN-aware elections.
- Supervisor: manages PostgreSQL (initdb, promote/demote, backup/restore).

Commit probing: Gateway captures `txid_current()` before COMMIT, and if the backend dies, asks the new leader `SELECT txid_status(...)`. Clients get a definitive answer instead of “maybe committed.”

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for the deep dive.

## Comparison

| Feature                       | pgbattery                  | Patroni             | CloudNativePG      | AWS RDS Multi-AZ     |
| ----------------------------- | -------------------------- | ------------------- | ------------------ | -------------------- |
| **Client errors on failover** | None (migration)           | Reconnect required  | Reconnect required | Reconnect required   |
| **Failover time**             | <100 ms idle / ~5 s writes | 15-30s              | 20-40s             | 60-120s (DNS)        |
| **Connection migration**      | Yes                        | No                  | No                 | No                   |
| **In-flight COMMIT recovery** | Yes (probe+verify)         | No                  | No                 | No                   |
| **Dependencies**              | None                       | etcd/Consul/ZK      | Kubernetes         | AWS infrastructure   |
| **Deployment**                | Single binary              | Multiple components | K8s operator       | Managed service      |
| **LSN-aware elections**       | Yes                        | Partial             | No                 | N/A (managed)        |
| **Cost**                      | Self-hosted                | Self-hosted         | Self-hosted        | $$$ + vendor lock-in |

## Benchmarks

Reproduce with `./demo/bench.py` — a uv script that drives `pgbench` and a
heartbeat probe against the 3-node `docker compose` cluster. Raw numbers
land in `demo/bench-results.json`.

**Environment.** macOS / Docker Desktop, 3 containers on one host (not
a tuned production deployment). Treat the numbers as a sanity floor, not a
ceiling.

### Throughput (steady state)

| Workload                                  | Result    |
| ----------------------------------------- | --------- |
| `pgbench` TPC-B-like, 4 clients × 30 s    | 1,085 TPS |
| Average write latency through the gateway | 3.7 ms    |

### Failover (SIGKILL of the leader)

| Observation                              | Value           |
| ---------------------------------------- | --------------- |
| Idle / read-only connection blip         | 68 ms (max gap) |
| Active writing connection unavailability | ~5 s            |
| Cluster reconvergence (Raft re-election) | 2.7 s           |
| Leader before → after                    | node3 → node1   |

Idle and read-only connections migrate transparently — the heartbeat loop
in `bench.py` saw a 68 ms worst-case gap. Connections in the middle of a
write transaction see a brief `ReadOnlySqlTransaction` window (visible in
the demo above) until the new leader is fully promoted; the gateway then
routes new statements to it without the client reconnecting.

### Footprint (per node, steady state)

| Container | CPU   | Memory  |
| --------- | ----- | ------- |
| node1     | 4.1 % | 145 MiB |
| node2     | 3.2 % | 116 MiB |
| node3     | 5.2 % | 159 MiB |

## Monitoring

Prometheus metrics live inside each container on `:9090/metrics` (host ports `9091/9092/9093` in the demo). Every emitted metric carries a `# HELP` line — run `curl -s localhost:9091/metrics | grep '^# HELP pgbattery_'` for the full enumerated list.

```
# Raft / cluster
pgbattery_raft_state                 # 0 follower, 1 candidate, 2 leader
pgbattery_raft_term
pgbattery_raft_commit_index
pgbattery_leader_elections

# Replication (per-replica, labelled by node)
pgbattery_sync_replicas
pgbattery_sync_quorum                # 1 if leader has a sync quorum
pgbattery_replica_lag_bytes{node="2"}
pgbattery_replica_lag_seconds{node="2"}
pgbattery_replica_health{node="2"}   # 1.0 healthy, 0.5 lagging, 0.0 unhealthy
pgbattery_replica_is_sync{node="2"}  # 2.0 sync, 1.0 potential, 0.0 async

# Connections
pgbattery_connections_active
pgbattery_connections_migrated
pgbattery_connections_severed

# Safety / fencing (CONTRACTS L1–L3)
pgbattery_emergency_fence
pgbattery_queries_rejected_lease_expired
pgbattery_local_lsn_bytes
pgbattery_lsn_future_skew_total
```

- Default flush interval is 250 ms to limit CPU overhead; crank it down for chaos runs if you need millisecond-level insight.
- Structured debug logs (enable with `RUST_LOG=pgbattery=debug`) include leader elections, fencing decisions, and LSN deltas.

## Testing

Test scripts run under [uv](https://docs.astral.sh/uv/), a Python project/runtime manager:

```bash
brew install uv                                       # or: pipx install uv
./testing/ci_runner.py --list                         # discover suites
./testing/ci_runner.py --suite ha-controlplane-pr     # ~3 min smoke
./testing/ci_runner.py --suite ha-sequential          # full sequential suite
```

- Matrix lives in `testing/ci_matrix.yaml` (25 step types, ~20 cases across 4 suites).
- `testing/jepsen_lite.py` — stdlib-only Jepsen-style register linearisability check.
- `testing/repro_two_sync*.sh` reproduce the OPEN metric-staleness anomaly tracked in `BUGS.md`.
- CI workflows in `.github/workflows/` — `ha-ci.yml` (push/PR/nightly) and `jepsen-lite.yml` (weekly).

## Reliability Status

**Alpha release.** Correctness work ongoing — see [CONTRACTS.md](docs/CONTRACTS.md) for the formal contracts.

### What Works

- Leader failure: idle connections see a <100 ms blip; in-flight writes resume on the new leader within a few seconds
- Network partition: multi-layer fencing prevents split-brain
- Connection migration: idle transactions survive failover
- Data integrity: synchronous replication, LSN-aware elections, and timeline verification

### Manual Recovery Required

- Node crash recovery: operator must confirm disk integrity before rejoin (prevents data corruption)

This is intentional: fully automatic recovery in this scenario risks silent data loss.

## Documentation

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — system design and component details
- [STATE_MACHINE.md](docs/STATE_MACHINE.md) — canonical state-machine truth sources (Raft, lease, replication, gateway routing)
- [CONTRACTS.md](docs/CONTRACTS.md) — correctness contracts (W1–W3, L1–L3, S1, R1–R2)
- [DEPLOYMENT.md](docs/DEPLOYMENT.md) — bootstrap, join, TLS, Prometheus alerts
- [RUNBOOK.md](docs/RUNBOOK.md) — incident response checklists
- [MEMBERSHIP.md](docs/MEMBERSHIP.md) — voter/learner topology operations
