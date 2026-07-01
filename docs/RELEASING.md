# Releasing pgbattery

`pgbattery upgrade` downloads a binary, verifies a **SHA-256 checksum**
(integrity) and a **Sigstore cosign keyless signature** (authenticity). This
document is the publisher-side half: how artifacts are signed and laid out so
the upgrade client can verify them.

Cosign **keyless** signing needs **no signing key and no repository secret**.
In CI, GitHub Actions presents its OIDC identity to Sigstore's Fulcio CA, which
issues a short-lived (~10 min) signing certificate whose Subject Alternative
Name encodes the exact workflow + tag that ran. The signature and that
certificate are published next to the binary; the upgrade client verifies them
against a baked-in trust anchor (issuer + identity), so there is nothing secret
to rotate or leak.

## Trust anchor (what the client enforces)

`pgbattery upgrade` prefers the **Sigstore bundle** (`<binary>.bundle`), which
carries the signature, the Fulcio certificate, **and** a Rekor transparency-log
inclusion proof. It accepts a binary only if **all** of these hold (see
`src/commands/upgrade/cosign.rs`, `CosignVerifier::verify_bundle`):

1. The bundle's **Rekor inclusion proof** (Signed Entry Timestamp / SET)
   verifies against the trust root's **Rekor public key**
   (`SignedArtifactBundle::new_verified`), proving the signing event was recorded
   in the public transparency log. This also yields the log's _integrated time_.
2. The signature verifies over the binary under the certificate's key.
3. The certificate chains to a **Fulcio CA** from the Sigstore trust root
   (fetched via TUF), checked **at the Rekor integrated time** — the
   transparency log's attestation of when the signature existed (strictly better
   than the certificate's `notBefore`).
4. The certificate's **OIDC issuer** is exactly
   `https://token.actions.githubusercontent.com`.
5. The certificate's **SAN identity** matches
   `^https://github.com/electricapp/pgbattery/.github/workflows/release.yml@refs/tags/v.*$`.

Issuer + identity are baked into the binary as constants; operators can override
them with `--identity <regex>` / `PGBATTERY_RELEASE_IDENTITY_REGEX` and
`PGBATTERY_RELEASE_OIDC_ISSUER` (e.g. for a fork).

**Rekor transparency-log inclusion is now verified** (previously a known gap).
When a release publishes no `.bundle` — older releases, or a publisher that
opted out — the client falls back to the detached `.sig` + `.pem`
(`CosignVerifier::verify_blob`): it runs checks 2, 4, and 5, anchors the chain at
the certificate's `notBefore`, and skips the Rekor check (the detached artifacts
carry no SET, and sigstore-rs 0.14 exposes Rekor verification only via the bundle
path). The fallback logs a warning that Rekor inclusion was not verified, but
still refuses an unverifiable binary.

## Artifact layout

For a release `vX.Y.Z`, publish under the release base URL
(`https://pgbattery.io/releases` by default, overridable with `--url`):

```
<base>/latest                              # plain text: "X.Y.Z"
<base>/vX.Y.Z/pgbattery-<os>-<arch>        # the binary (e.g. pgbattery-linux-x86_64)
<base>/vX.Y.Z/pgbattery-<os>-<arch>.sha256 # hex SHA-256 of the binary
<base>/vX.Y.Z/pgbattery-<os>-<arch>.bundle # Sigstore bundle: sig + cert + Rekor inclusion proof (primary)
<base>/vX.Y.Z/pgbattery-<os>-<arch>.sig    # cosign signature (base64; no-Rekor fallback)
<base>/vX.Y.Z/pgbattery-<os>-<arch>.pem    # Fulcio signing certificate (PEM; no-Rekor fallback)
```

The client fetches the `.bundle` first and verifies Rekor inclusion; it only
fetches `.sig`/`.pem` when the `.bundle` is absent (HTTP 404).

`<os>`/`<arch>` come from Rust's `std::env::consts` (`linux`/`macos`,
`x86_64`/`aarch64`) — see `platform_binary_name()` in
`src/commands/upgrade/mod.rs`.

## Signing in CI

`.github/workflows/release.yml` builds the matrix, then on a `v*` tag:

1. installs cosign (SHA-pinned + checksum-verified),
2. runs `cosign sign-blob --yes --bundle … --output-signature … --output-certificate …`
   for each `pgbattery-*` artifact (the `build` job has `id-token: write` so it
   can request the GitHub Actions OIDC token). The `--bundle` carries the Rekor
   transparency-log inclusion proof; the `.sig`/`.pem` are the no-Rekor fallback,
3. immediately runs `cosign verify-blob` (both the detached pair and the
   `--bundle`) against this repo's issuer + workflow identity (a broken signature
   or Rekor proof fails the release here, not at upgrade time),
4. uploads the binary + `.sha256` + `.bundle` + `.sig` + `.pem` to the release.

There is **no `MINISIGN_SECRET_KEY` (or any signing secret) to configure** — the
only requirement is the `id-token: write` permission, already granted to the
`build` job.

## Signing locally / manually

```bash
scripts/sign-release.sh dist/   # signs every pgbattery-* binary in dist/
```

cosign will open a browser for the OIDC sign-in. A local signon uses a personal
identity (not the GitHub Actions workflow identity), so a binary signed this way
will **not** pass the client's default identity check — local signing is for
testing the flow, not for producing client-verifiable releases. To verify a
locally signed blob against a custom identity, set `OIDC_ISSUER` and
`IDENTITY_REGEX` before running the script.

## Verifying manually

Verify the bundle (signature + cert + **Rekor inclusion**):

```bash
cosign verify-blob \
  --bundle pgbattery-linux-x86_64.bundle \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/electricapp/pgbattery/\.github/workflows/release\.yml@refs/tags/v.*$' \
  pgbattery-linux-x86_64
```

Or, using the detached fallback (no Rekor inclusion check):

```bash
cosign verify-blob \
  --signature   pgbattery-linux-x86_64.sig \
  --certificate pgbattery-linux-x86_64.pem \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/electricapp/pgbattery/\.github/workflows/release\.yml@refs/tags/v.*$' \
  pgbattery-linux-x86_64

sha256sum -c <(echo "$(cat pgbattery-linux-x86_64.sha256)  pgbattery-linux-x86_64")
```

## Changing the trust anchor

Because there is no key, "rotation" means changing the _identity policy_, not a
key:

1. If the release workflow path or repo changes, update the identity regex
   constant in `src/commands/upgrade/cosign.rs` (and the `verify-blob` step in
   `release.yml`) and ship a client release.
2. The issuer (`https://token.actions.githubusercontent.com`) is fixed by GitHub
   Actions and should not need to change.
