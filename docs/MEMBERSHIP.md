# Membership Cheatsheet

Three relevant operations: **bootstrap**, **join/promote**, and **remove**.

## Terms

- **Voter** – full data node that participates in Raft elections.
- **Learner** – data node receiving WAL but not yet a voter (safe staging area).
- **Witness** – vote-only node (no PostgreSQL). Optional; most clusters run data nodes only.

## Bootstrap

```bash
pgbattery init --output node1.toml --node-id 1
pgbattery --config node1.toml run --bootstrap
```

This initializes PostgreSQL (if needed), creates a single-member Raft cluster,
and starts the management API.

## Join and Promote

```bash
# Prepare config from existing cluster
pgbattery join --peer <leader-mgmt> --write-config node2.toml --node-id 2
# Edit node2.toml as needed, then start the process
pgbattery --config node2.toml run

# Optional: force promotion once caught up
pgbattery cluster promote 2 --leader <leader-id-or-address>
```

Under the hood:

1. The new node registers as a learner via `/api/v1/cluster/join`.
2. It runs `pg_basebackup` and streams WAL until within the catch-up threshold.
3. The leader auto-promotes it to voter if `--voter` was passed or the operator
   runs `cluster promote`.

## Removing a Node

```bash
pgbattery cluster remove <node-id> --leader <leader>
# or, to self-remove the current node:
pgbattery cluster remove --self --leader <leader>
```

Removal steps:

- Leader updates Raft membership.
- Management API drops the node’s replication slot (to avoid WAL bloat).
- Operator stops the process at their convenience.

Never remove so many voters that the cluster loses majority (N/2+1).

## Bringing a Removed Node Back

A removed node does not rejoin by restarting. `join` clones from the leader and
refuses a data directory that already holds a cluster:

```
Error: Data directory /var/lib/postgresql/data is not empty.
```

That refusal is deliberate — the node's data is from a membership it is no
longer in, and nothing has reconciled it against the cluster's history — so the
operator clears the state and lets the node join as a new one:

```bash
# with the node stopped
rm -rf /var/lib/postgresql/data/* /var/lib/postgresql/raft/*
pgbattery join --peer <any-member> --voter
```

The cost is a full basebackup rather than catch-up from the WAL the node
already holds. Reattaching that WAL would need the node's data proven to be
this cluster's (see `docs/STATE_MACHINE.md` §3a) and no further ahead than the
leader; until that exists, the clone is the supported path.

## API Reference

| Endpoint                                 | Purpose                                    |
| ---------------------------------------- | ------------------------------------------ |
| `GET /api/v1/cluster/leader`             | Discover current leader & advertised addrs |
| `GET /api/v1/cluster/members`            | List voters/learners (source for CLI)      |
| `POST /api/v1/cluster/join`              | Register learner (body = `JoinRequest`)    |
| `POST /api/v1/cluster/promote/{node_id}` | Promote learner → voter                    |
| `POST /api/v1/cluster/remove/{node_id}`  | Remove node from membership                |
| `POST /api/v1/cluster/report-lsn`        | Followers report LSN for leader elections  |

`JoinRequest` carries all advertised addresses (`raft`, `pg`, `mgmt`,
`metrics`) so that every node has an accurate view without port guessing.

## Gotchas

- Always ensure the `node_id` in the config matches the one used in CLI/API.
- Witness nodes are optional; do not mix them unless you truly need a quorum
  vote without extra storage.
- If you see `Node ID <n> not found`, run `pgbattery cluster members --node <mgmt>`
  to list the IDs you can target.
- Removing the last voter is disallowed by design.

Keep your config files in version control so you can rebuild nodes quickly, and
prefer `pgbattery join --write-config` when adding hardware—it captures the
current peer list automatically.
