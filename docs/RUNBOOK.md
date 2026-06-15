# Runbook

To use during incidents. Each section is a short checklist; expand only when
you need to.

## 1. No Leader / Writes Blocked

Symptoms: `pgbattery status` shows no leader or clients see `FENCED`.

1. Count running nodes. You need majority.
2. Check Raft port reachability (`nc -z <peer> 5433`).
3. View metrics: `pgbattery_raft_has_quorum` should be 1 on the leader.
4. Restore connectivity; the system elects a new leader automatically.

If the cluster is still leaderless after quorum is restored, restart one follower
at a time. Avoid restarting all nodes simultaneously.

## 2. Add or Replace a Node

```bash
pgbattery join --peer <leader-mgmt> --write-config nodeX.toml --node-id X
pgbattery --config nodeX.toml run
```

A restarted node with intact data will rejoin automatically. If its raft state
is corrupt, wipe the `raft/` directory but keep PostgreSQL data.

## 3. Manual Failover

Use only when you want to force a new leader despite the old one being healthy.

```bash
pgbattery cluster leader           # find current leader ID
pgbattery cluster promote <node> --leader <current-leader>
pgbattery cluster remove --self --leader <new-leader>  # optional drain
```

Promoting a follower forces Raft to elect it. Clients will reconnect once the
gateway sees the new leader address.

## 4. Backup & Restore

Create backup:

```bash
pgbattery backup create --node <leader>
```

Restore (stops the target node):

```bash
pgbattery backup restore --filename <name> --node <target>
```

After restore, restart the node and allow it to catch up.

## 5. Replica Lag

Check metrics:

```bash
curl -s http://<leader>:9090/metrics | grep pgbattery_replica_lag_bytes
```

If lag keeps growing:

1. Ensure network bandwidth between leader/follower is healthy.
2. Check disk I/O on the follower.
3. If lag stays high, demote to learner and investigate before promoting again.

## 6. Management API Hints

- `GET /api/v1/cluster/leader` – who’s leader right now?
- `GET /api/v1/cluster/members` – list of voters/learners.
- `POST /api/v1/cluster/remove/{id}` – drop node from membership.
- `POST /api/v1/cluster/report-lsn` – sent automatically by followers; if missing, check follower logs.

Mutating management endpoints require a token when `management_api_token` is
configured (recommended for any non-loopback binding).

For CLI mutating commands, export:

```bash
export PGBATTERY_MANAGEMENT_API_TOKEN=<your-token>
```

Token header accepted by API: `x-pgbattery-token` (or `Authorization: Bearer ...`).

## 7. Log Files Worth Checking

| Component   | Location / Command                                    |
| ----------- | ----------------------------------------------------- |
| pgbattery   | `journalctl -u pgbattery -f` or container logs        |
| PostgreSQL  | `pg_log/postgresql.log` inside `pg_data_dir`          |
| Metrics/API | same process; search for `Management API` or `backup` |

Always capture logs before restarting nodes in an incident; once the process
exits, the context is gone.

## 8. Debug Endpoints

For chaos testing and deep troubleshooting, two debug endpoints are available:

### `/debug/events` – State Transition History

Returns the last N state transitions (leader changes, fencing, membership changes):

```bash
# Get recent events
curl -s http://<node>:9091/debug/events | jq .

# Poll for new events (use since parameter)
curl -s "http://<node>:9091/debug/events?since=42&limit=50" | jq .
```

Response:

```json
{
  "current_seq": 57,
  "events": [
    {
      "seq": 56,
      "timestamp_ms": 1705250400000,
      "event_type": "leader_change",
      "node_id": 1,
      "data": { "old_leader": null, "new_leader": 1 }
    }
  ]
}
```

Event types: `leader_change`, `fence`, `membership`, `sync_state`, `connection_migrated`.

### `/debug/state` – Current Cluster Snapshot

Quick view of this node's perspective on cluster state:

```bash
curl -s http://<node>:9091/debug/state | jq .
```

Response:

```json
{
  "node_id": 1,
  "leader_id": 1,
  "is_leader": true,
  "voters": [1, 2, 3],
  "learners": [],
  "node_count": 3
}
```

## 9. Verbose Logging

By default, INFO-level logs show meaningful events (leader changes, failovers,
client migrations). For deeper debugging, enable DEBUG logs for specific modules:

```bash
# All debug logs (very verbose)
RUST_LOG=debug pgbattery run --config node.toml

# Raft internals only
RUST_LOG=pgbattery::governor::raft=debug pgbattery run --config node.toml

# Multiple modules
RUST_LOG=pgbattery::governor=debug,pgbattery::gateway=debug pgbattery run --config node.toml
```

Common modules for debugging:

- `pgbattery::governor::raft` – Raft log append/apply operations
- `pgbattery::governor` – Leader election, state machine updates
- `pgbattery::gateway` – Client connection routing
- `pgbattery::supervisor` – PostgreSQL process management
