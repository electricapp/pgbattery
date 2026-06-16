# Contributing to pgbattery

Thanks for your interest in pgbattery. This is a distributed database system, so
**correctness is paramount** — please read this guide before opening a pull
request.

## Contributor License Agreement (required)

pgbattery is **dual-licensed**: the open-source distribution is under
**AGPL-3.0-only**, and the Maintainer also offers the software under separate
**commercial license** terms. To keep that possible, every external contributor
must sign a Contributor License Agreement (CLA) granting the Maintainer the
right to distribute their contribution under **both** open-source and commercial
terms.

- **Individuals** sign the [Individual CLA](docs/legal/ICLA.md) (ICLA).
- **Contributing on behalf of an employer?** Your company signs the
  [Corporate CLA](docs/legal/CCLA.md) (CCLA) and lists you in its Schedule A.

> **Why a CLA and not just a DCO?** A Developer Certificate of Origin only
> certifies you have the right to submit code under the project's license; it
> does **not** grant the right to relicense it. Because pgbattery is offered
> under both AGPL and commercial terms, a CLA (with a relicensing grant) is
> required — a DCO alone would prevent us from including your contribution in
> the commercial distribution.

### How signing works

When you open your first pull request, an automated assistant checks whether you
have a CLA on file. If not, it comments asking you to sign. To sign, reply on
the PR with exactly:

```
I have read the CLA Document and I hereby sign the CLA
```

That records your signature (GitHub username + PR + commit) and unblocks the
PR. You only sign once; future PRs are recognized automatically. If a check goes
stale, comment `recheck`.

You retain all rights to your own contributions — the CLA is a license grant,
not a transfer of ownership.

## Before you open a PR

1. **Read the design docs.** For any change to consensus, supervisor, lease,
   replication, fencing, or gateway-routing logic, `docs/STATE_MACHINE.md` is
   the canonical source of truth — read it first, and update it **in the same
   commit** if you add/remove/rename a state, transition, or truth source. See
   also `docs/ARCHITECTURE.md`.
2. **Match the safety bar.** Clippy runs with `pedantic` + `nursery` and denies
   `unwrap`/`expect`/`panic`/`indexing_slicing`. `unsafe` is denied
   workspace-wide. Don't work around these — restructure.
3. **Verify correctness, not just "it didn't crash."** If your change touches
   failover or replication, exercise the relevant cases in `testing/` and check
   replication state and data integrity after leadership changes.

## Local checks (mirror CI)

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo doc --no-deps --all-features          # RUSTDOCFLAGS="-D warnings"

# Test-harness lints (Python):
uv run --project testing ruff check testing/
uv run --project testing ruff format --check testing/
uv run --project testing mypy testing/ --strict
uv run --python 3.14 --script testing/lint_matrix.py
```

The fuzz crate lives outside the workspace; if you touch it, also run
`cd fuzz && cargo clippy --all-targets -- -D warnings` (CI gates on this too).

## Commit and PR conventions

- Keep commits focused; write commit messages that explain the **why**.
- Reference any bug/anomaly you fix or discover in `BUGS.md`.
- CI must be green. If a chaos/HA case fails, investigate it — don't re-run to
  "fix" it.

By submitting a contribution you confirm it is your original work (or properly
attributed) and that it may be distributed under the terms described above.
