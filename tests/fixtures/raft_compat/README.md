# Raft frame + store compatibility fixtures

CI gate for `UPGRADE_CHECKLIST.md` step 1 / `UPGRADE.md` Track A compat.

## Layout

| Path                 | Role                                                                        |
| -------------------- | --------------------------------------------------------------------------- |
| `manifest`           | Pins `frame_version` and `store_schema_version` to the binary constants     |
| `supported/`         | Artifacts HEAD **must** decode / open (today: current v0 only — greenfield) |
| `unsupported_newer/` | Inversion artifacts HEAD **must** refuse                                    |

## Policy

- Patch/minor releases must not change frame format or store schema. If
  `RAFT_RPC_FRAME_VERSION` or `STORE_SCHEMA_VERSION` drifts from `manifest`
  without regenerating fixtures, `governor::compat_fixtures` fails in CI.
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
