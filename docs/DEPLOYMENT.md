# Deployment Quickstart

## Prerequisites

- Three or more nodes (odd count keeps elections simple).
- PostgreSQL 16 binaries installed locally.
- Ports open between nodes: `5432` (clients), `5433` (Raft), `5434`
  (internal PG), `9090` (metrics), `9091` (management).
- Set a shared management API token on every node:
  `PGBATTERY_MANAGEMENT_API_TOKEN=<long-random-secret>`

## Bootstrap the First Node

```bash
pgbattery init --output node1.toml --node-id 1
pgbattery --config node1.toml run --bootstrap
```

`pgbattery init` writes a starter config with sane defaults; edit advertised
addresses if needed.

## Add More Nodes

```bash
# Generate config without touching the cluster
pgbattery join --peer <leader-mgmt> --write-config node2.toml --node-id 2
# Review node2.toml, set listen/raft/metrics addresses for the new host
pgbattery --config node2.toml run
```

Repeat for node3, node4, etc. The `join` workflow runs `pg_basebackup`, puts
the node in learner mode, and auto-promotes once LSN lag drops below the
threshold.

## Operational Commands

| Task            | Command / Endpoint                                  |
| --------------- | --------------------------------------------------- |
| View status     | `pgbattery status --discover <mgmt>`                |
| Find leader     | `pgbattery cluster leader --json`                   |
| Promote learner | `pgbattery cluster promote <node-id> --leader <id>` |
| Remove node     | `pgbattery cluster remove <node-id> --leader <id>`  |
| Snapshot backup | `pgbattery backup create --node <id>`               |
| Restore backup  | `pgbattery backup restore <file> --node <id>`       |

All commands accept `--config <path>` if the default `pgbattery.toml` is not
present.

For mutating commands (`cluster promote/remove`, `backup restore`, etc.), set:

```bash
export PGBATTERY_MANAGEMENT_API_TOKEN=<long-random-secret>
```

## TLS (Optional)

Set the following keys in the config:

```toml
[tls]
enabled = true
cert_file = "/etc/pgbattery/server.crt"
key_file  = "/etc/pgbattery/server.key"
ca_file   = "/etc/pgbattery/ca.crt"
```

Use certificates that include every advertised hostname/IP and ensure the key
file is readable only by the pgbattery user.

## Verification Checklist

1. `pgbattery status` shows one leader and the expected followers.
2. `psql -h <gateway> -p 5432` can read/write.
3. `curl http://<mgmt>:9091/api/v1/cluster/members` lists each node.
4. Prometheus sees `pgbattery_raft_is_leader` / `pgbattery_connections_active`
   metrics.

Once those checks pass, start your workloads.

# Monitoring Essentials

Every node exposes Prometheus metrics at `http://<node>:9090/metrics`. Scrape
them and build alerts around the handful of signals that actually matter.

## Core Metrics

| Metric                                           | Meaning                                       | Action                                                                                     |
| ------------------------------------------------ | --------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `pgbattery_raft_is_leader`                       | 1 if this node is leader                      | Sum across cluster; if nobody reports 1, the cluster is leaderless.                        |
| `pgbattery_raft_has_quorum`                      | 1 when the leader has quorum                  | Alert if the leader has quorum=0 for >30s.                                                 |
| `pgbattery_lease_valid`                          | 1 when writes are allowed                     | If the leader’s lease expires, clients are already fenced. Investigate network partitions. |
| `pgbattery_replica_lag_bytes{node}`              | Lag per replica                               | Alert when lag exceeds your RPO threshold (e.g., 16MB).                                    |
| `pgbattery_connections_active`                   | Active client connections                     | Track trends; sudden drops often coincide with failovers.                                  |
| `pgbattery_connections_migrated` / `..._severed` | Counters for migration vs. forced disconnects | Spikes of “severed” connections indicate transactions were cut mid-flight.                 |

## Minimal Prometheus Scrape

```yaml
scrape_configs:
  - job_name: pgbattery
    static_configs:
      - targets: ["node1:9090", "node2:9090", "node3:9090"]
```

## Suggested Alerts

```yaml
- alert: PgBatteryNoLeader
  expr: sum(pgbattery_raft_is_leader) == 0
  for: 30s
  labels: { severity: critical }
  annotations:
    description: "No node is leader; writes are blocked."

- alert: PgBatteryLeaderNoQuorum
  expr: pgbattery_raft_is_leader == 1 and pgbattery_raft_has_quorum == 0
  for: 30s
  labels: { severity: critical }
  annotations:
    description: "Leader lost quorum. Expect fencing/failover."

- alert: PgBatteryReplicaLag
  expr: max(pgbattery_replica_lag_bytes) > 16*1024*1024
  for: 2m
  labels: { severity: warning }
  annotations:
    description: "Replica lag exceeds 16MB."
```
