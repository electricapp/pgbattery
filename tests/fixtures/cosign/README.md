# cosign / Rekor test fixtures

These back the offline unit tests in `src/commands/upgrade/cosign.rs`. All are
optional: tests skip (rather than fail) when a fixture is absent, so `cargo test`
stays green on a minimal checkout with no network or openssl.

## Detached `.sig`/`.pem` path (synthetic, fully reproducible)

- `root.pem` — a self-issued CA standing in for a Fulcio root.
- `leaf.pem` — a leaf chaining to `root.pem`, carrying the Fulcio OIDC-issuer
  extension (`https://token.actions.githubusercontent.com`) and a
  URI SAN for `electricapp/pgbattery`'s release workflow @ `v9.9.9`.
- `blob.bin` — the signed payload.
- `blob.sig` — base64 signature of `blob.bin` under `leaf.pem`'s key.

These exercise signature-over-blob + Fulcio-chain (at `notBefore`) + issuer + SAN
identity entirely offline.

## Bundle / Rekor transparency-log path

A Rekor **Signed Entry Timestamp (SET)** is signed by Rekor's _private_ key,
which we cannot reproduce. A fully-synthetic, offline Rekor bundle is therefore
impossible — the SET would never verify against a real Rekor public key. We
handle this in two layers:

- `rekor_pub.pem` + `rekor_key_id.txt` — the **production Rekor public key** and
  its hex `logID`, captured from upstream sigstore-rs test vectors.
- `rekor_valid.bundle` — a **real, public** cosign bundle (upstream sigstore-rs
  vector, Rekor `logIndex` 7810348) whose SET is signed by that Rekor key. The
  test `rekor_set_inclusion_is_actually_verified` proves the SET check is real:
  the bundle verifies with the correct Rekor key and is _rejected_ with a wrong
  or absent key. (Its cert is a different identity that does not chain to our
  fixture Fulcio root and signs a blob we do not ship, so only the SET — not the
  full release policy — is asserted against this bundle.)

### `release.bundle` (full end-to-end, not checked in)

`verifies_full_release_bundle_when_fixture_present` runs the _entire_ bundle
policy (Rekor SET + signature + Fulcio chain at integrated time + issuer + SAN)
offline, but only if a self-consistent `release.bundle` is present whose:

- cert is `leaf.pem` (chains to `root.pem`, correct issuer + SAN), and
- SET verifies against `rekor_pub.pem`, and
- signature is over `blob.bin`.

Producing such a bundle requires forging a Rekor SET, which is infeasible without
Rekor's private key, so this fixture is intentionally **absent** and the test
skips. The real end-to-end guarantee is exercised in CI by `release.yml`, which
runs `cosign verify-blob --bundle …` against the live Sigstore trust root for
every published artifact. The `rekor_valid.bundle` test above is the offline
proof that our client wiring of the SET check is correct.
