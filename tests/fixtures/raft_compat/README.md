# Raft frame + store compatibility fixtures

CI gate for `UPGRADE_CHECKLIST.md` step 1 / `UPGRADE.md` Track A compat.

## Layout

| Path                                 | Role                                                                             |
| ------------------------------------ | -------------------------------------------------------------------------------- |
| `manifest`                           | Pins `frame_version` and `store_schema_version` to the binary constants          |
| `supported/frame_v0_vote.bin`        | Current frame bytes; HEAD must decode and re-emit them byte-identically          |
| `supported/store_schema0.raft.db`    | Populated pre-versioning store; HEAD must open it and run the 0→1 migration      |
| `supported/store_schema1.raft.db`    | Populated current-schema store; HEAD must open it and decode every record        |
| `supported/snapshot_state_schema1.bin` | Postcard bytes of a populated `ClusterState`; HEAD must re-encode identically  |
| `supported/log_entries_schema1.bin`  | Postcard bytes of every `ClusterCommand` variant; HEAD must re-encode identically |
| `unsupported_newer/`                 | Inversion artifacts HEAD **must** refuse                                         |

## Policy

- Patch/minor releases must not change frame format or store schema. The
  schema is the **record encodings**, not just the constant: postcard is
  positional, so any field change in a stored type (`ClusterState`,
  `ClusterCommand`, `LogEntry`, …) changes the bytes and trips the `.bin`
  pins. If `RAFT_RPC_FRAME_VERSION` or `STORE_SCHEMA_VERSION` drifts from
  `manifest`, or an encoding drifts from its pin, without regenerating
  fixtures, `governor::compat_fixtures` fails in CI. An intentional encoding
  change is a schema bump and needs a forward migration
  (`RedbLogStorage::ensure_schema_version`).
- Majors may bump either version by one; the accept window stays `N-1..=N`
  (`frame_accept_window_is_exactly_n_minus_one_through_n`).
- When a real prior release exists, add its frame/store under `supported/`
  and keep HEAD able to open them (N−1). Do not delete older supported
  fixtures until they fall outside the window.

## Regenerate

After an intentional frame or schema bump:

```bash
cargo test -p pgbattery --lib governor::compat_fixtures::regenerate_raft_compat_fixtures -- --ignored --nocapture
```

Then commit the updated `manifest`, `supported/`, and `unsupported_newer/`.
