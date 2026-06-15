# Architecture

This document explains the pieces that make up a pgbattery node and how they
interact during normal operation and failover. The goal is clarity, not an
exhaustive treatise.

## System Overview

Each node runs a single process with four major subsystems:

| Component         | Purpose                                                                                                                                                 |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Gateway**       | Listens on the PostgreSQL port, parses protocol messages, fences writes if the lease expires, and transparently reconnects idle clients after failover. |
| **Governor**      | Embeds the OpenRaft instance, manages membership, drives leader election, and exposes cluster state to the rest of the process.                         |
| **Supervisor**    | Owns the local PostgreSQL instance (initdb, start/stop, promotion, demotion, backups).                                                                  |
| **Observability** | Exposes the management API and Prometheus metrics so operators and tooling can inspect or change cluster state.                                         |

All components run inside the same async runtime and exchange information via
watch channels (`leader_rx`, `fence_rx`, `shutdown_rx`) or Arc-protected state.

## Data and Control Flow

1. Clients connect to the **Gateway** on the public port.
2. The gateway consults the current leader address shared by the **Governor**.
3. If this node is leader, the gateway forwards traffic to the local PostgreSQL
   instance; otherwise it proxies to the leader’s internal address.
4. PostgreSQL streams WAL to replicas via standard streaming replication.
5. Followers periodically report their LSN to the Governor (`report-lsn`) so
   elections can avoid choosing stale nodes.

```
Clients ─► Gateway ─► PostgreSQL (leader)
             │
             └─► Governor (Raft) ──► other nodes
```

## Failover Sequence

1. Leader dies or loses quorum → OpenRaft marks leader as `None`.
2. **Governor** immediately fences the node and expires the lease.
3. **Gateway** refuses new writes and severs in-flight transactions.
4. Remaining nodes elect a new leader based on Raft log state.
5. New leader runs `pg_promote` via the **Supervisor**, reenables writes once
   the lease is valid, and recreates replication slots for other voters.
6. Gateways on followers notice the leader address change and reconnect idle
   clients to the new primary.

## Storage Layout

- PostgreSQL data lives under `pg_data_dir`.
- Raft state (`raft.db`) lives in a sibling `raft/` directory to avoid being
  copied by `pg_basebackup`.
- Backups (if enabled) are stored under `backup_dir` per node.

Keeping Raft storage separate is critical: new nodes always start with a fresh
log, preventing cloned vote history from corrupting elections.

## Why One Binary?

Running everything in a single process keeps deployment simple: no etcd,
Consul, or sidecar daemons. The trade-off is strict layering inside the
binary. We preserve clarity through:

- explicit channels for leadership and fencing state,
- clear ownership (Gateway never touches PostgreSQL directly, Supervisor never
  talks to the network), and
- small management APIs that expose only the commands operators need.

This structure makes it possible to reason about the safety story without
reading thousands of lines of glue code. Keep future additions just as small
and explicit.
